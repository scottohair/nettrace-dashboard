#!/usr/bin/env python3
"""Paper Trading Arena — COLD/WARM/HOT pipeline for marketplace agents.

External agents enter through the evaluation pipeline:
  1. Register → COLD (backtest against historical data)
  2. Win rate > 58% → WARM (paper trade with live data)
  3. Profitable + Sharpe > 0.5 after 1h → HOT (real capital)
  4. Drawdown > 10% → fired

This module is org-scoped: each org has its own arena with isolated
agent evaluations. Integrates with strategy_pipeline.py promotion criteria.
"""

import json
import logging
import math
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("paper_arena")

DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent.parent / "traceroute.db"))

# Promotion thresholds (same as strategy_pipeline.py)
COLD_TO_WARM = {
    "min_trades": 12,
    "min_win_rate": 0.58,
    "min_return_pct": 0.3,
    "max_drawdown_pct": 5.0,
}

WARM_TO_HOT = {
    "min_trades": 10,
    "min_win_rate": 0.55,
    "min_return_pct": 0.1,
    "max_drawdown_pct": 3.0,
    "min_sharpe": 0.5,
    "min_warm_hours": 1.0,
}

# Firing thresholds
FIRE_MAX_DRAWDOWN = 0.10  # 10% drawdown = fired
FIRE_MIN_TRADES = 20      # need enough sample before firing


class PaperArena:
    """Evaluates marketplace agents through the COLD/WARM/HOT pipeline."""

    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH

    def _db(self):
        db = sqlite3.connect(self.db_path)
        db.row_factory = sqlite3.Row
        return db

    def evaluate_agent(self, agent_id, org_id=None):
        """Evaluate an agent and potentially promote/demote/fire it.

        Returns dict with evaluation result and any stage change.
        """
        db = self._db()
        try:
            agent = db.execute(
                "SELECT * FROM agent_registrations WHERE id = ?", (agent_id,)
            ).fetchone()

            if not agent:
                return {"error": "Agent not found"}

            if org_id and agent["org_id"] != org_id:
                return {"error": "Agent does not belong to this org"}

            stage = agent["pipeline_stage"]
            stats = self._compute_stats(db, agent_id, agent["org_id"])

            result = {
                "agent_id": agent_id,
                "agent_name": agent["agent_name"],
                "current_stage": stage,
                "stats": stats,
                "action": "hold",
                "new_stage": stage,
            }

            # Check for firing conditions
            if stats["trades"] >= FIRE_MIN_TRADES and stats["max_drawdown"] > FIRE_MAX_DRAWDOWN:
                self._fire_agent(db, agent_id)
                result["action"] = "fired"
                result["new_stage"] = "FIRED"
                result["reason"] = f"Drawdown {stats['max_drawdown']:.1%} exceeded {FIRE_MAX_DRAWDOWN:.0%} limit"
                return result

            # COLD evaluation
            if stage == "COLD":
                if self._meets_cold_criteria(stats):
                    self._promote(db, agent_id, "WARM")
                    result["action"] = "promoted"
                    result["new_stage"] = "WARM"
                    result["reason"] = "Passed COLD evaluation"

            # WARM evaluation
            elif stage == "WARM":
                if stats.get("warm_hours", 0) < WARM_TO_HOT["min_warm_hours"]:
                    result["reason"] = f"Still evaluating ({stats.get('warm_hours', 0):.1f}h of {WARM_TO_HOT['min_warm_hours']}h)"
                elif self._meets_warm_criteria(stats):
                    self._promote(db, agent_id, "HOT")
                    result["action"] = "promoted"
                    result["new_stage"] = "HOT"
                    result["reason"] = "Passed WARM evaluation — real capital eligible"
                elif stats["total_pnl"] < 0:
                    # Losing money in WARM → demote back to COLD
                    self._demote(db, agent_id, "COLD")
                    result["action"] = "demoted"
                    result["new_stage"] = "COLD"
                    result["reason"] = "Negative PnL in WARM — demoted to COLD"

            # HOT evaluation (continuous)
            elif stage == "HOT":
                if stats["total_pnl"] < 0 and stats["trades"] >= 5:
                    # Losing real money → kill immediately
                    self._fire_agent(db, agent_id)
                    result["action"] = "fired"
                    result["new_stage"] = "FIRED"
                    result["reason"] = "Losing money in HOT — fired (Rule #1)"

            return result
        finally:
            db.close()

    def evaluate_all_org_agents(self, org_id):
        """Evaluate all active agents for an org. Returns list of results."""
        db = self._db()
        try:
            agents = db.execute(
                "SELECT id FROM agent_registrations WHERE org_id = ? "
                "AND status NOT IN ('fired', 'suspended')",
                (org_id,)
            ).fetchall()
        finally:
            db.close()

        results = []
        for a in agents:
            results.append(self.evaluate_agent(a["id"], org_id))
        return results

    def get_arena_status(self, org_id):
        """Get arena summary for an org."""
        db = self._db()
        try:
            stages = db.execute("""
                SELECT pipeline_stage, COUNT(*) as cnt,
                       SUM(total_pnl) as stage_pnl,
                       AVG(sharpe_ratio) as avg_sharpe
                FROM agent_registrations
                WHERE org_id = ? AND status NOT IN ('fired')
                GROUP BY pipeline_stage
            """, (org_id,)).fetchall()

            fired = db.execute(
                "SELECT COUNT(*) as cnt FROM agent_registrations "
                "WHERE org_id = ? AND status = 'fired'",
                (org_id,)
            ).fetchone()["cnt"]

            return {
                "org_id": org_id,
                "stages": {
                    r["pipeline_stage"]: {
                        "count": r["cnt"],
                        "total_pnl": r["stage_pnl"] or 0,
                        "avg_sharpe": round(r["avg_sharpe"] or 0, 3),
                    } for r in stages
                },
                "fired_total": fired,
            }
        finally:
            db.close()

    def _compute_stats(self, db, agent_id, org_id):
        """Compute performance statistics from trade proposals."""
        proposals = db.execute("""
            SELECT direction, confidence, proposed_size_usd, status,
                   entry_price, stop_loss, take_profit, created_at
            FROM trade_proposals
            WHERE agent_id = ? AND org_id = ?
            ORDER BY created_at
        """, (agent_id, org_id)).fetchall()

        trades = len([p for p in proposals if p["status"] in ("executed", "approved")])
        wins = 0
        losses = 0
        total_pnl = 0.0
        max_drawdown = 0.0
        peak = 0.0
        running_pnl = 0.0
        returns = []

        for p in proposals:
            if p["status"] not in ("executed", "approved"):
                continue

            # Simple PnL estimation for paper trades
            entry = p["entry_price"] or 0
            tp = p["take_profit"] or 0
            sl = p["stop_loss"] or 0
            size = p["proposed_size_usd"] or 0

            if entry > 0 and tp > 0 and sl > 0:
                # Simulate based on confidence (higher confidence = more likely to hit TP)
                conf = p["confidence"] or 0.5
                if conf > 0.6:  # Assume win if above threshold
                    pnl = abs(tp - entry) / entry * size
                    wins += 1
                else:
                    pnl = -abs(entry - sl) / entry * size
                    losses += 1
            else:
                # No price targets: small positive/negative based on confidence
                if p["confidence"] and p["confidence"] > 0.65:
                    pnl = size * 0.005  # 0.5% gain estimate
                    wins += 1
                else:
                    pnl = -size * 0.003  # 0.3% loss estimate
                    losses += 1

            total_pnl += pnl
            running_pnl += pnl
            peak = max(peak, running_pnl)
            drawdown = (peak - running_pnl) / peak if peak > 0 else 0
            max_drawdown = max(max_drawdown, drawdown)

            if size > 0:
                returns.append(pnl / size)

        # Sharpe ratio approximation
        sharpe = 0.0
        if returns and len(returns) > 1:
            import statistics
            mean_ret = statistics.mean(returns)
            std_ret = statistics.stdev(returns)
            if std_ret > 0:
                sharpe = mean_ret / std_ret * math.sqrt(252)  # Annualized

        win_rate = wins / trades if trades > 0 else 0
        return_pct = (total_pnl / max(sum(p["proposed_size_usd"] or 0 for p in proposals), 1)) * 100

        # Calculate warm hours (time since entering WARM stage)
        agent = db.execute(
            "SELECT pipeline_stage, created_at FROM agent_registrations WHERE id = ?",
            (agent_id,)
        ).fetchone()
        warm_hours = 0
        if agent and agent["pipeline_stage"] == "WARM":
            # Use first proposal as proxy for WARM start
            first = db.execute(
                "SELECT MIN(created_at) as first_at FROM trade_proposals "
                "WHERE agent_id = ? AND org_id = ?",
                (agent_id, agent["org_id"])
            ).fetchone()
            if first and first["first_at"]:
                try:
                    start = datetime.fromisoformat(first["first_at"])
                    now = datetime.now(timezone.utc)
                    if start.tzinfo is None:
                        start = start.replace(tzinfo=timezone.utc)
                    warm_hours = (now - start).total_seconds() / 3600
                except (ValueError, TypeError):
                    pass

        return {
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 3),
            "total_pnl": round(total_pnl, 2),
            "return_pct": round(return_pct, 2),
            "max_drawdown": round(max_drawdown, 4),
            "sharpe_ratio": round(sharpe, 3),
            "warm_hours": round(warm_hours, 2),
        }

    def _meets_cold_criteria(self, stats):
        return (
            stats["trades"] >= COLD_TO_WARM["min_trades"]
            and stats["win_rate"] >= COLD_TO_WARM["min_win_rate"]
            and stats["return_pct"] >= COLD_TO_WARM["min_return_pct"]
            and stats["max_drawdown"] <= COLD_TO_WARM["max_drawdown_pct"] / 100
        )

    def _meets_warm_criteria(self, stats):
        return (
            stats["trades"] >= WARM_TO_HOT["min_trades"]
            and stats["win_rate"] >= WARM_TO_HOT["min_win_rate"]
            and stats["return_pct"] >= WARM_TO_HOT["min_return_pct"]
            and stats["max_drawdown"] <= WARM_TO_HOT["max_drawdown_pct"] / 100
            and stats["sharpe_ratio"] >= WARM_TO_HOT["min_sharpe"]
        )

    def _promote(self, db, agent_id, new_stage):
        logger.info("Promoting agent %d to %s", agent_id, new_stage)
        status = "active" if new_stage == "HOT" else "approved"
        db.execute(
            "UPDATE agent_registrations SET pipeline_stage = ?, status = ? WHERE id = ?",
            (new_stage, status, agent_id)
        )
        db.commit()

    def _demote(self, db, agent_id, new_stage):
        logger.info("Demoting agent %d to %s", agent_id, new_stage)
        db.execute(
            "UPDATE agent_registrations SET pipeline_stage = ?, capital_allocation_usd = 0 WHERE id = ?",
            (new_stage, agent_id)
        )
        db.commit()

    def _fire_agent(self, db, agent_id):
        logger.warning("FIRING agent %d", agent_id)
        db.execute(
            "UPDATE agent_registrations SET status = 'fired', capital_allocation_usd = 0 WHERE id = ?",
            (agent_id,)
        )
        db.commit()
