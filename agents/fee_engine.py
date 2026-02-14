#!/usr/bin/env python3
"""Fee Engine — Performance and management fee calculation for NetTrace platform.

Pricing model:
  - Performance fee: % of gains above High Water Mark (HWM)
  - Management fee: 2% annual on AUM (0.167%/month)
  - Minimum AUM for perf fee: $10K (below = subscription only)
  - Monthly invoicing via Stripe Invoice API

Fee tiers:
  Free:           $0/mo, no perf fee, paper only
  Pro:            $249/mo, 5% perf fee above HWM
  Enterprise:     $2,499/mo, 10% perf fee above HWM
  Enterprise Pro: $50,000/mo, 20% perf fee above HWM (2% mgmt fee included)
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger("fee_engine")

DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent.parent / "traceroute.db"))

# Performance fee rates by tier
PERF_FEE_RATES = {
    "free": 0.0,
    "pro": 0.05,             # 5% of gains above HWM
    "enterprise": 0.10,       # 10%
    "enterprise_pro": 0.20,   # 20%
    "government": 0.15,       # 15% (custom)
}

# Management fee: 2% annual on AUM for Enterprise Pro
MGMT_FEE_ANNUAL_RATE = 0.02  # 2%

# Minimum AUM to charge performance fee
MIN_AUM_FOR_PERF_FEE = 10_000  # $10K

# Subscription base fees (monthly)
SUBSCRIPTION_FEES = {
    "free": 0,
    "pro": 249,
    "enterprise": 2499,
    "enterprise_pro": 50000,
    "government": 500000,
}


class FeeEngine:
    """Calculate and invoice performance + management fees per org."""

    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH

    def _db(self):
        db = sqlite3.connect(self.db_path)
        db.row_factory = sqlite3.Row
        return db

    def snapshot_aum(self, org_id):
        """Take a daily AUM snapshot and update the high water mark.

        Should be called once daily by the scheduler.
        """
        db = self._db()
        try:
            # Get current portfolio value from latest trading snapshot
            snap = db.execute(
                "SELECT total_value_usd FROM trading_snapshots "
                "WHERE org_id = ? ORDER BY recorded_at DESC LIMIT 1",
                (org_id,)
            ).fetchone()

            current_aum = snap["total_value_usd"] if snap else 0.0

            # Get previous HWM
            prev = db.execute(
                "SELECT high_water_mark_usd FROM org_aum_snapshots "
                "WHERE org_id = ? ORDER BY recorded_at DESC LIMIT 1",
                (org_id,)
            ).fetchone()

            prev_hwm = prev["high_water_mark_usd"] if prev else 0.0
            new_hwm = max(current_aum, prev_hwm)
            gains_above_hwm = max(current_aum - prev_hwm, 0)

            db.execute(
                "INSERT INTO org_aum_snapshots (org_id, total_aum_usd, high_water_mark_usd, "
                "gains_above_hwm) VALUES (?, ?, ?, ?)",
                (org_id, current_aum, new_hwm, gains_above_hwm)
            )
            db.commit()

            logger.info("AUM snapshot org=%d: $%.2f (HWM=$%.2f, gains=$%.2f)",
                        org_id, current_aum, new_hwm, gains_above_hwm)

            return {
                "org_id": org_id,
                "aum": round(current_aum, 2),
                "hwm": round(new_hwm, 2),
                "gains_above_hwm": round(gains_above_hwm, 2),
            }
        finally:
            db.close()

    def calculate_performance_fee(self, org_id, period_start, period_end):
        """Calculate performance fee for a period based on gains above HWM.

        Returns (fee_amount, calculation_details).
        """
        db = self._db()
        try:
            org = db.execute(
                "SELECT tier FROM organizations WHERE id = ?", (org_id,)
            ).fetchone()
            if not org:
                return 0.0, {"error": "Org not found"}

            tier = org["tier"]
            rate = PERF_FEE_RATES.get(tier, 0)
            if rate == 0:
                return 0.0, {"reason": "No performance fee for this tier", "tier": tier}

            # Sum gains above HWM over the period
            rows = db.execute("""
                SELECT SUM(gains_above_hwm) as total_gains,
                       MAX(total_aum_usd) as max_aum,
                       MIN(total_aum_usd) as min_aum,
                       COUNT(*) as snapshots
                FROM org_aum_snapshots
                WHERE org_id = ? AND recorded_at >= ? AND recorded_at < ?
            """, (org_id, period_start, period_end)).fetchone()

            total_gains = rows["total_gains"] or 0.0
            max_aum = rows["max_aum"] or 0.0

            if max_aum < MIN_AUM_FOR_PERF_FEE:
                return 0.0, {
                    "reason": f"AUM ${max_aum:.2f} below minimum ${MIN_AUM_FOR_PERF_FEE}",
                    "threshold": MIN_AUM_FOR_PERF_FEE,
                }

            fee = total_gains * rate

            calculation = {
                "tier": tier,
                "rate": rate,
                "total_gains_above_hwm": round(total_gains, 2),
                "fee": round(fee, 2),
                "period_start": period_start,
                "period_end": period_end,
                "snapshots": rows["snapshots"],
                "max_aum": round(max_aum, 2),
                "min_aum": round(rows["min_aum"] or 0, 2),
            }

            return round(fee, 2), calculation
        finally:
            db.close()

    def calculate_management_fee(self, org_id, period_start, period_end):
        """Calculate management fee (2% annual on average AUM).

        Only charged on Enterprise Pro tier.
        Returns (fee_amount, calculation_details).
        """
        db = self._db()
        try:
            org = db.execute(
                "SELECT tier FROM organizations WHERE id = ?", (org_id,)
            ).fetchone()
            if not org:
                return 0.0, {"error": "Org not found"}

            if org["tier"] != "enterprise_pro":
                return 0.0, {"reason": "Management fee only for Enterprise Pro", "tier": org["tier"]}

            # Average AUM over the period
            row = db.execute("""
                SELECT AVG(total_aum_usd) as avg_aum, COUNT(*) as days
                FROM org_aum_snapshots
                WHERE org_id = ? AND recorded_at >= ? AND recorded_at < ?
            """, (org_id, period_start, period_end)).fetchone()

            avg_aum = row["avg_aum"] or 0.0
            days = row["days"] or 0

            if days == 0:
                return 0.0, {"reason": "No AUM data for period"}

            # Pro-rate the annual fee to the period length
            try:
                start_dt = datetime.fromisoformat(period_start)
                end_dt = datetime.fromisoformat(period_end)
                period_days = (end_dt - start_dt).days
            except (ValueError, TypeError):
                period_days = 30

            annual_fee = avg_aum * MGMT_FEE_ANNUAL_RATE
            period_fee = annual_fee * (period_days / 365)

            calculation = {
                "tier": org["tier"],
                "annual_rate": MGMT_FEE_ANNUAL_RATE,
                "avg_aum": round(avg_aum, 2),
                "period_days": period_days,
                "annual_fee": round(annual_fee, 2),
                "period_fee": round(period_fee, 2),
            }

            return round(period_fee, 2), calculation
        finally:
            db.close()

    def generate_monthly_invoice(self, org_id, year=None, month=None):
        """Generate a combined monthly invoice (subscription + performance + management fees).

        Returns the invoice record.
        """
        now = datetime.now(timezone.utc)
        if year is None:
            year = now.year
        if month is None:
            month = now.month - 1 or 12
            if month == 12:
                year -= 1

        period_start = datetime(year, month, 1, tzinfo=timezone.utc).isoformat()
        next_month = month + 1 if month < 12 else 1
        next_year = year if month < 12 else year + 1
        period_end = datetime(next_year, next_month, 1, tzinfo=timezone.utc).isoformat()

        db = self._db()
        try:
            org = db.execute(
                "SELECT * FROM organizations WHERE id = ?", (org_id,)
            ).fetchone()
            if not org:
                return {"error": "Org not found"}

            tier = org["tier"]

            # Check for existing invoice
            existing = db.execute(
                "SELECT id FROM fee_invoices WHERE org_id = ? AND period_start = ? "
                "AND period_end = ? AND status != 'void'",
                (org_id, period_start, period_end)
            ).fetchone()
            if existing:
                return {"error": "Invoice already exists for this period", "invoice_id": existing["id"]}

            # Calculate fees
            subscription = SUBSCRIPTION_FEES.get(tier, 0)
            perf_fee, perf_calc = self.calculate_performance_fee(org_id, period_start, period_end)
            mgmt_fee, mgmt_calc = self.calculate_management_fee(org_id, period_start, period_end)

            total = subscription + perf_fee + mgmt_fee

            calculation = {
                "subscription": subscription,
                "performance_fee": perf_fee,
                "performance_details": perf_calc,
                "management_fee": mgmt_fee,
                "management_details": mgmt_calc,
                "total": round(total, 2),
                "tier": tier,
            }

            cur = db.execute(
                "INSERT INTO fee_invoices (org_id, invoice_type, period_start, period_end, "
                "amount_usd, calculation_json, status) VALUES (?, 'combined', ?, ?, ?, ?, 'draft')",
                (org_id, period_start, period_end, total, json.dumps(calculation))
            )
            db.commit()

            invoice_id = cur.lastrowid
            logger.info("Generated invoice #%d for org %d: $%.2f (%s)",
                        invoice_id, org_id, total, tier)

            return {
                "invoice_id": invoice_id,
                "org_id": org_id,
                "period": f"{year}-{month:02d}",
                "amount_usd": round(total, 2),
                "breakdown": calculation,
                "status": "draft",
            }
        finally:
            db.close()

    def preview_fee(self, org_id):
        """Preview the next fee calculation without creating an invoice."""
        now = datetime.now(timezone.utc)
        period_start = datetime(now.year, now.month, 1, tzinfo=timezone.utc).isoformat()

        db = self._db()
        try:
            org = db.execute(
                "SELECT tier FROM organizations WHERE id = ?", (org_id,)
            ).fetchone()
            if not org:
                return {"error": "Org not found"}

            tier = org["tier"]
            subscription = SUBSCRIPTION_FEES.get(tier, 0)
            perf_fee, perf_calc = self.calculate_performance_fee(
                org_id, period_start, now.isoformat()
            )
            mgmt_fee, mgmt_calc = self.calculate_management_fee(
                org_id, period_start, now.isoformat()
            )

            return {
                "org_id": org_id,
                "tier": tier,
                "period_to_date": f"{now.year}-{now.month:02d}",
                "subscription": subscription,
                "performance_fee_to_date": perf_fee,
                "management_fee_to_date": mgmt_fee,
                "total_to_date": round(subscription + perf_fee + mgmt_fee, 2),
                "performance_details": perf_calc,
                "management_details": mgmt_calc,
            }
        finally:
            db.close()

    def get_aum_history(self, org_id, days=30):
        """Get AUM + HWM history for an org."""
        db = self._db()
        try:
            rows = db.execute("""
                SELECT total_aum_usd, high_water_mark_usd, gains_above_hwm, recorded_at
                FROM org_aum_snapshots
                WHERE org_id = ? AND recorded_at > datetime('now', ?)
                ORDER BY recorded_at
            """, (org_id, f"-{days} days")).fetchall()

            return [{
                "aum": r["total_aum_usd"],
                "hwm": r["high_water_mark_usd"],
                "gains": r["gains_above_hwm"],
                "recorded_at": r["recorded_at"],
            } for r in rows]
        finally:
            db.close()

    def get_invoices(self, org_id, limit=12):
        """Get invoice history for an org."""
        db = self._db()
        try:
            rows = db.execute("""
                SELECT id, invoice_type, period_start, period_end,
                       amount_usd, status, created_at, paid_at
                FROM fee_invoices
                WHERE org_id = ?
                ORDER BY created_at DESC LIMIT ?
            """, (org_id, limit)).fetchall()

            return [{
                "id": r["id"],
                "type": r["invoice_type"],
                "period_start": r["period_start"],
                "period_end": r["period_end"],
                "amount_usd": r["amount_usd"],
                "status": r["status"],
                "created_at": r["created_at"],
                "paid_at": r["paid_at"],
            } for r in rows]
        finally:
            db.close()
