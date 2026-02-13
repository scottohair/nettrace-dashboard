#!/usr/bin/env python3
"""KPI Tracker — daily revenue targets and strategy scorecard.

Central performance hub that:
  1. Tracks daily/weekly/monthly P&L against escalating targets
  2. Scores each strategy (HF vs LF) on win rate, Sharpe, throughput
  3. Feeds evolutionary decisions (fire/promote/clone via agent_goals.py)
  4. Exposes /api/v1/kpi endpoint for dashboard

Target ramp: $1K/day → $10K → $100K → $1M as capital and edges scale.
Current phase: seed capital (~$290), target $1-5/day.

KPI Philosophy:
  - Every transaction or series of transactions = gains
  - Intermediately use any token; end of day settle to USD
  - HF strategies: high throughput, small edge, volume game
  - LF strategies: big edge, low frequency, conviction plays
  - Both contribute; score independently, allocate capital proportionally
"""

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger("kpi_tracker")

BASE = Path(__file__).parent
DB_PATH = os.environ.get("KPI_DB_PATH", str(BASE / "kpi_tracker.db"))

# Target ramp — scales with portfolio size
TARGET_TIERS = [
    {"min_portfolio": 0,       "daily_target": 1.0,     "label": "Seed"},
    {"min_portfolio": 1000,    "daily_target": 10.0,    "label": "Growth"},
    {"min_portfolio": 10000,   "daily_target": 100.0,   "label": "Scale"},
    {"min_portfolio": 100000,  "daily_target": 1000.0,  "label": "Velocity"},
    {"min_portfolio": 1000000, "daily_target": 10000.0, "label": "Acceleration"},
    {"min_portfolio": 10000000,"daily_target": 100000.0,"label": "Orbit"},
]


def _init_db(db_path=None):
    """Initialize KPI tracking tables."""
    p = db_path or DB_PATH
    conn = sqlite3.connect(p)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_pnl (
            date TEXT PRIMARY KEY,
            starting_value REAL,
            ending_value REAL,
            realized_pnl REAL DEFAULT 0,
            unrealized_pnl REAL DEFAULT 0,
            total_pnl REAL DEFAULT 0,
            target REAL DEFAULT 1.0,
            target_met INTEGER DEFAULT 0,
            trades_count INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            fees_paid REAL DEFAULT 0,
            tier TEXT DEFAULT 'Seed',
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS strategy_scorecard (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            strategy_name TEXT,
            strategy_type TEXT DEFAULT 'LF',
            trades INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            gross_pnl REAL DEFAULT 0,
            fees REAL DEFAULT 0,
            net_pnl REAL DEFAULT 0,
            avg_hold_seconds REAL DEFAULT 0,
            sharpe REAL DEFAULT 0,
            max_drawdown REAL DEFAULT 0,
            capital_allocated REAL DEFAULT 0,
            roi_pct REAL DEFAULT 0,
            score REAL DEFAULT 0,
            updated_at TEXT,
            UNIQUE(date, strategy_name)
        );

        CREATE TABLE IF NOT EXISTS kpi_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            event_type TEXT,
            strategy TEXT,
            details TEXT
        );
    """)
    conn.close()


class KPITracker:
    """Central performance scorecard for all trading strategies."""

    def __init__(self, db_path=None):
        self._db_path = db_path or DB_PATH
        _init_db(self._db_path)
        self._today = None
        self._refresh_date()

    def _db(self):
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _refresh_date(self):
        self._today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _current_tier(self, portfolio_value):
        """Determine current target tier based on portfolio size."""
        tier = TARGET_TIERS[0]
        for t in TARGET_TIERS:
            if portfolio_value >= t["min_portfolio"]:
                tier = t
        return tier

    # ── Recording ──────────────────────────────────────────────────────

    def record_trade(self, strategy_name, pair, direction, amount_usd,
                     pnl, fees, hold_seconds=0, strategy_type="LF", won=True):
        """Record a completed trade for KPI tracking.

        Called by sniper, grid_trader, momentum_scalper, etc. after each fill.
        """
        self._refresh_date()
        now = datetime.now(timezone.utc).isoformat()

        db = self._db()
        try:
            # Update strategy scorecard
            db.execute("""
                INSERT INTO strategy_scorecard
                    (date, strategy_name, strategy_type, trades, wins, losses,
                     gross_pnl, fees, net_pnl, avg_hold_seconds, updated_at)
                VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, strategy_name) DO UPDATE SET
                    trades = trades + 1,
                    wins = wins + ?,
                    losses = losses + ?,
                    gross_pnl = gross_pnl + ?,
                    fees = fees + ?,
                    net_pnl = net_pnl + ?,
                    avg_hold_seconds = (avg_hold_seconds * trades + ?) / (trades + 1),
                    updated_at = ?
            """, (
                self._today, strategy_name, strategy_type,
                1 if won else 0, 0 if won else 1,
                pnl + fees, fees, pnl, hold_seconds, now,
                # ON CONFLICT values:
                1 if won else 0, 0 if won else 1,
                pnl + fees, fees, pnl, hold_seconds, now,
            ))

            # Update daily P&L
            db.execute("""
                INSERT INTO daily_pnl (date, realized_pnl, trades_count, wins, losses, fees_paid, updated_at)
                VALUES (?, ?, 1, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    realized_pnl = realized_pnl + ?,
                    total_pnl = realized_pnl + unrealized_pnl,
                    trades_count = trades_count + 1,
                    wins = wins + ?,
                    losses = losses + ?,
                    fees_paid = fees_paid + ?,
                    updated_at = ?
            """, (
                self._today, pnl, 1 if won else 0, 0 if won else 1, fees, now,
                pnl, 1 if won else 0, 0 if won else 1, fees, now,
            ))

            # Log event
            db.execute("""
                INSERT INTO kpi_events (timestamp, event_type, strategy, details)
                VALUES (?, 'trade', ?, ?)
            """, (now, strategy_name, json.dumps({
                "pair": pair, "direction": direction, "amount_usd": amount_usd,
                "pnl": pnl, "fees": fees, "won": won,
            })))

            db.commit()
        except Exception as e:
            logger.error("KPI record_trade error: %s", e)
        finally:
            db.close()

    def update_portfolio_snapshot(self, portfolio_value, unrealized_pnl=0):
        """Update end-of-period portfolio value for daily P&L calc."""
        self._refresh_date()
        now = datetime.now(timezone.utc).isoformat()
        tier = self._current_tier(portfolio_value)

        db = self._db()
        try:
            db.execute("""
                INSERT INTO daily_pnl (date, ending_value, unrealized_pnl,
                                       target, tier, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    ending_value = ?,
                    unrealized_pnl = ?,
                    total_pnl = realized_pnl + ?,
                    target = ?,
                    target_met = CASE WHEN (realized_pnl + ?) >= ? THEN 1 ELSE 0 END,
                    tier = ?,
                    updated_at = ?
            """, (
                self._today, portfolio_value, unrealized_pnl,
                tier["daily_target"], tier["label"], now,
                portfolio_value, unrealized_pnl, unrealized_pnl,
                tier["daily_target"], unrealized_pnl, tier["daily_target"],
                tier["label"], now,
            ))
            db.commit()
        except Exception as e:
            logger.error("KPI snapshot error: %s", e)
        finally:
            db.close()

    # ── Querying ──────────────────────────────────────────────────────

    def get_daily_summary(self, date=None):
        """Get P&L summary for a date (default today)."""
        self._refresh_date()
        d = date or self._today
        db = self._db()
        try:
            row = db.execute("SELECT * FROM daily_pnl WHERE date = ?", (d,)).fetchone()
            if row:
                return dict(row)
            return {"date": d, "total_pnl": 0, "target": 1.0, "target_met": False}
        finally:
            db.close()

    def get_strategy_scorecards(self, date=None):
        """Get all strategy scores for a date."""
        self._refresh_date()
        d = date or self._today
        db = self._db()
        try:
            rows = db.execute("""
                SELECT * FROM strategy_scorecard
                WHERE date = ?
                ORDER BY net_pnl DESC
            """, (d,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            db.close()

    def get_weekly_summary(self):
        """Get P&L for last 7 days."""
        db = self._db()
        try:
            rows = db.execute("""
                SELECT date, total_pnl, target, target_met, trades_count, tier
                FROM daily_pnl
                WHERE date >= date('now', '-7 days')
                ORDER BY date
            """).fetchall()
            days = [dict(r) for r in rows]
            total_pnl = sum(d.get("total_pnl", 0) or 0 for d in days)
            targets_met = sum(1 for d in days if d.get("target_met"))
            return {
                "days": days,
                "total_pnl": total_pnl,
                "targets_met": targets_met,
                "total_days": len(days),
                "hit_rate": targets_met / max(len(days), 1),
            }
        finally:
            db.close()

    def get_strategy_rankings(self):
        """Rank strategies by cumulative performance for evolutionary decisions."""
        db = self._db()
        try:
            rows = db.execute("""
                SELECT strategy_name, strategy_type,
                       SUM(trades) as total_trades,
                       SUM(wins) as total_wins,
                       SUM(net_pnl) as total_pnl,
                       AVG(avg_hold_seconds) as avg_hold,
                       CASE WHEN SUM(trades) > 0
                            THEN CAST(SUM(wins) AS REAL) / SUM(trades)
                            ELSE 0 END as win_rate
                FROM strategy_scorecard
                GROUP BY strategy_name
                ORDER BY total_pnl DESC
            """).fetchall()
            return [dict(r) for r in rows]
        finally:
            db.close()

    def get_evolutionary_decisions(self):
        """Use agent_goals to determine fire/promote/clone decisions."""
        try:
            from agent_goals import GoalValidator
        except ImportError:
            return []

        rankings = self.get_strategy_rankings()
        decisions = []
        for s in rankings:
            name = s["strategy_name"]
            trades = s.get("total_trades", 0) or 0
            win_rate = s.get("win_rate", 0) or 0
            total_pnl = s.get("total_pnl", 0) or 0

            # Approximate Sharpe from win_rate and pnl
            sharpe = (win_rate - 0.5) * 4 if trades > 10 else 0
            drawdown = max(0, -total_pnl / max(abs(total_pnl), 1)) if total_pnl < 0 else 0

            fire = GoalValidator.should_fire_agent(sharpe, trades, win_rate, drawdown)
            promote = GoalValidator.should_promote_agent(sharpe, trades, win_rate)

            decisions.append({
                "strategy": name,
                "trades": trades,
                "win_rate": round(win_rate, 3),
                "pnl": round(total_pnl, 4),
                "sharpe_est": round(sharpe, 2),
                "fire": fire,
                "promote": promote,
            })
        return decisions

    def full_report(self, portfolio_value=0):
        """Complete KPI report for dashboard/API."""
        self._refresh_date()
        tier = self._current_tier(portfolio_value)
        return {
            "date": self._today,
            "portfolio_value": portfolio_value,
            "tier": tier,
            "daily": self.get_daily_summary(),
            "weekly": self.get_weekly_summary(),
            "strategies": self.get_strategy_scorecards(),
            "rankings": self.get_strategy_rankings(),
            "evolutionary": self.get_evolutionary_decisions(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


# Singleton
_tracker = None

def get_kpi_tracker():
    global _tracker
    if _tracker is None:
        _tracker = KPITracker()
    return _tracker
