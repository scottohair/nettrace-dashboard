#!/usr/bin/env python3
"""One-time backfill: populate capital_ledger from existing trade DBs.

Sources:
  - agents/trader.db   → agent_trades  (248 trades — sniper, live_trader)
  - agents/sniper.db   → sniper_trades (612 trades — sniper-specific)
  - agents/trader.db   → live_trades   (live_trader executions)
  - traceroute.db      → trading_snapshots (portfolio snapshots for gap inference)

Run:  python3 ~/src/quant/agents/backfill_ledger.py [--dry-run]
"""

import json
import logging
import os
import sqlite3
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

from ledger_writer import LedgerWriter  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [backfill] %(levelname)s %(message)s",
)
logger = logging.getLogger("backfill")

TRADER_DB = str(BASE / "trader.db")
SNIPER_DB = str(BASE / "sniper.db")
MAIN_DB = os.environ.get(
    "DB_PATH",
    str(BASE.parent / "traceroute.db"),
)


def _safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _connect(path):
    if not Path(path).exists():
        logger.warning("DB not found: %s — skipping", path)
        return None
    conn = sqlite3.connect(path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def backfill_agent_trades(ledger: LedgerWriter, dry_run: bool = False) -> int:
    """Backfill from agent_trades table (trader.db)."""
    conn = _connect(TRADER_DB)
    if not conn:
        return 0

    try:
        rows = conn.execute(
            "SELECT * FROM agent_trades ORDER BY created_at ASC"
        ).fetchall()
    except sqlite3.OperationalError as e:
        logger.warning("agent_trades not found: %s", e)
        return 0
    finally:
        conn.close()

    count = 0
    for t in rows:
        side = str(t["side"] or "").upper()
        pair = str(t["pair"] or "")
        asset = pair.split("-")[0] if "-" in pair else pair
        price = _safe_float(t["price"])
        qty = _safe_float(t["quantity"])
        total_usd = _safe_float(t["total_usd"])
        status = str(t["status"] or "").lower()

        # Only backfill filled trades (pending/cancelled don't move capital)
        if status not in ("filled", "closed", "executed", "settled"):
            continue

        if dry_run:
            logger.info(
                "[DRY RUN] agent_trade id=%d %s %s $%.2f @ $%.4f",
                t["id"], side, pair, total_usd, price,
            )
            count += 1
            continue

        # Estimate maker fee (0.4%)
        fees = total_usd * 0.004

        row_id = ledger.record_trade_fill(
            asset=asset,
            venue="coinbase",
            amount=qty if side == "BUY" else -qty,
            value_usd=total_usd if side == "BUY" else -total_usd,
            pair=pair,
            side=side,
            price=price,
            order_id=str(t["order_id"] or ""),
            agent=str(t["agent"] or "unknown"),
            fees_usd=fees,
            realized_pnl_usd=_safe_float(t["pnl"]) if t["pnl"] is not None else None,
            trade_id=int(t["id"]),
            timestamp=str(t["created_at"] or ""),
            metadata={"source": "backfill_agent_trades", "status": status},
        )
        if row_id:
            count += 1

    logger.info("Backfilled %d/%d agent_trades", count, len(rows))
    return count


def backfill_sniper_trades(ledger: LedgerWriter, dry_run: bool = False) -> int:
    """Backfill from sniper_trades table (sniper.db).

    Note: sniper_trades may overlap with agent_trades.  We use the dedup
    index (event_type, trade_id, timestamp) to prevent duplicates — but
    sniper_trades uses its own id space so we prefix trade_id with a large
    offset to avoid collisions.
    """
    conn = _connect(SNIPER_DB)
    if not conn:
        return 0

    try:
        rows = conn.execute(
            "SELECT * FROM sniper_trades ORDER BY created_at ASC"
        ).fetchall()
    except sqlite3.OperationalError as e:
        logger.warning("sniper_trades not found: %s", e)
        return 0
    finally:
        conn.close()

    count = 0
    for t in rows:
        direction = str(t["direction"] or "").upper()
        pair = str(t["pair"] or "")
        asset = pair.split("-")[0] if "-" in pair else pair
        amount_usd = _safe_float(t["amount_usd"])
        entry_price = _safe_float(t["entry_price"])
        status = str(t["status"] or "").lower()
        pnl = _safe_float(t["pnl"]) if t["pnl"] is not None else None

        if status not in ("filled", "closed", "executed", "settled"):
            continue

        qty = amount_usd / entry_price if entry_price > 0 else 0

        if dry_run:
            logger.info(
                "[DRY RUN] sniper_trade id=%d %s %s $%.2f @ $%.4f",
                t["id"], direction, pair, amount_usd, entry_price,
            )
            count += 1
            continue

        fees = amount_usd * 0.004

        # Use offset to avoid trade_id collision with agent_trades
        sniper_trade_id = 1_000_000 + int(t["id"])

        row_id = ledger.record_trade_fill(
            asset=asset,
            venue="coinbase",
            amount=qty if direction == "BUY" else -qty,
            value_usd=amount_usd if direction == "BUY" else -amount_usd,
            pair=pair,
            side=direction,
            price=entry_price,
            order_id=str(t["order_id"]) if "order_id" in t.keys() else None,
            agent="sniper",
            strategy_name="sniper_scan",
            fees_usd=fees,
            realized_pnl_usd=pnl,
            trade_id=sniper_trade_id,
            trigger=f"confidence={_safe_float(t['composite_confidence']):.2f}",
            timestamp=str(t["created_at"] or ""),
            metadata={
                "source": "backfill_sniper_trades",
                "composite_confidence": _safe_float(t["composite_confidence"]),
                "status": status,
            },
        )
        if row_id:
            count += 1

    logger.info("Backfilled %d/%d sniper_trades", count, len(rows))
    return count


def backfill_live_trades(ledger: LedgerWriter, dry_run: bool = False) -> int:
    """Backfill from live_trades table (trader.db)."""
    conn = _connect(TRADER_DB)
    if not conn:
        return 0

    try:
        rows = conn.execute(
            "SELECT * FROM live_trades ORDER BY created_at ASC"
        ).fetchall()
    except sqlite3.OperationalError as e:
        logger.warning("live_trades not found: %s", e)
        return 0
    finally:
        conn.close()

    count = 0
    for t in rows:
        side = str(t["side"] or "").upper()
        pair = str(t["pair"] or "")
        asset = pair.split("-")[0] if "-" in pair else pair
        price = _safe_float(t["price"])
        qty = _safe_float(t["quantity"])
        total_usd = _safe_float(t["total_usd"])
        status = str(t["status"] or "").lower()

        if status not in ("filled", "closed", "executed", "settled", "success"):
            continue

        if dry_run:
            logger.info(
                "[DRY RUN] live_trade id=%d %s %s $%.2f @ $%.4f",
                t["id"], side, pair, total_usd, price,
            )
            count += 1
            continue

        fees = total_usd * 0.004

        # Use offset to avoid collision
        live_trade_id = 2_000_000 + int(t["id"])

        row_id = ledger.record_trade_fill(
            asset=asset,
            venue="coinbase",
            amount=qty if side == "BUY" else -qty,
            value_usd=total_usd if side == "BUY" else -total_usd,
            pair=pair,
            side=side,
            price=price,
            order_id=str(t["coinbase_order_id"] or "") if "coinbase_order_id" in t.keys() else None,
            agent="live_trader",
            fees_usd=fees,
            realized_pnl_usd=_safe_float(t["pnl"]) if t["pnl"] is not None else None,
            trade_id=live_trade_id,
            trigger=str(t["signal_type"] or ""),
            timestamp=str(t["created_at"] or ""),
            metadata={
                "source": "backfill_live_trades",
                "signal_type": str(t["signal_type"] or ""),
                "signal_confidence": _safe_float(t["signal_confidence"]),
                "status": status,
            },
        )
        if row_id:
            count += 1

    logger.info("Backfilled %d/%d live_trades", count, len(rows))
    return count


def infer_initial_deposit(ledger: LedgerWriter, dry_run: bool = False) -> int:
    """Infer initial deposit from earliest snapshot or known starting capital.

    We know the portfolio started around $290 USDC (from memory notes).
    """
    conn = _connect(MAIN_DB)
    if not conn:
        return 0

    try:
        first_snap = conn.execute(
            "SELECT total_value_usd, recorded_at FROM trading_snapshots ORDER BY recorded_at ASC LIMIT 1"
        ).fetchone()
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()

    if not first_snap:
        return 0

    initial_value = float(first_snap["total_value_usd"] or 0)
    timestamp = str(first_snap["recorded_at"] or "")

    if initial_value <= 0:
        return 0

    if dry_run:
        logger.info("[DRY RUN] Initial deposit: $%.2f at %s", initial_value, timestamp)
        return 1

    row_id = ledger.record_deposit(
        asset="USDC",
        venue="coinbase",
        amount=initial_value,
        value_usd=initial_value,
        source="initial_deposit_inferred",
        timestamp=timestamp,
    )

    if row_id:
        logger.info("Recorded initial deposit: $%.2f", initial_value)
        return 1
    return 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Backfill capital_ledger from existing trade DBs")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--skip-sniper", action="store_true", help="Skip sniper_trades (may overlap agent_trades)")
    args = parser.parse_args()

    ledger = LedgerWriter()
    total = 0

    logger.info("=" * 60)
    logger.info("Capital Ledger Backfill %s", "(DRY RUN)" if args.dry_run else "")
    logger.info("=" * 60)

    # 1. Infer initial deposit
    total += infer_initial_deposit(ledger, dry_run=args.dry_run)

    # 2. Backfill from agent_trades (primary trade log)
    total += backfill_agent_trades(ledger, dry_run=args.dry_run)

    # 3. Backfill from sniper_trades (may overlap, dedup handles it)
    if not args.skip_sniper:
        total += backfill_sniper_trades(ledger, dry_run=args.dry_run)

    # 4. Backfill from live_trades
    total += backfill_live_trades(ledger, dry_run=args.dry_run)

    logger.info("=" * 60)
    logger.info("Total events backfilled: %d", total)
    logger.info("=" * 60)

    if not args.dry_run:
        # Show ledger summary
        ledger_total = ledger.get_ledger_total()
        logger.info("Ledger total value: $%.2f", ledger_total)

        # Run reconciliation
        logger.info("Running reconciliation against snapshots...")
        from ledger_reconciler import LedgerReconciler
        reconciler = LedgerReconciler()
        results = reconciler.reconcile_all_unreconciled(limit=100)
        gaps = [r for r in results if abs(r.get("delta", 0)) > 0.50]
        if gaps:
            logger.info("Found %d reconciliation gaps:", len(gaps))
            for g in gaps[:10]:
                logger.info(
                    "  snapshot=%d snap=$%.2f ledger=$%.2f delta=$%.2f → %s",
                    g["snapshot_id"], g["snapshot_total"], g["ledger_total"],
                    g["delta"], g.get("event_type", "?"),
                )
        else:
            logger.info("All snapshots balanced (no gaps > $0.50)")


if __name__ == "__main__":
    main()
