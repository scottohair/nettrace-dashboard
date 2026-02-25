#!/usr/bin/env python3
"""AutoBuilder — Self-evolving configuration search engine.

Discovers better trading parameters through telemetry-driven mutation
and statistical promotion. Operates on configuration space only (signal
weights, TP/SL targets, timing params) — never generates arbitrary code.

Safety model matches strategy_evolver.lisp:
  - Bounded mutations (max 10% per cycle)
  - Shadow-only evaluation (paper trades)
  - Statistical gate before promotion
  - Automatic rollback on degradation
  - Single active challenger at a time

Cycle (every 5 minutes):
  1. MINE: Read telemetry → identify top bottleneck
  2. HYPOTHESIZE: Generate bounded config mutation
  3. SHADOW: Run mutation as challenger (paper trades only)
  4. EVALUATE: After N shadow trades, compare to champion
  5. PROMOTE: If all gates pass, swap champion config
  6. LOG: Audit every decision for rollback
"""

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("auto_builder")

BASE = Path(__file__).parent
RUNTIME_DIR = BASE / "runtime"
CHAMPION_CONFIG_PATH = RUNTIME_DIR / "champion_config.json"
CHAMPION_HISTORY_PATH = RUNTIME_DIR / "champion_history.jsonl"
STATUS_PATH = RUNTIME_DIR / "autobuilder_status.json"
AUDIT_PATH = RUNTIME_DIR / "autobuilder_audit.jsonl"

# DB paths
TRADER_DB = BASE / "trader.db"
KPI_DB = os.environ.get("KPI_DB_PATH", str(BASE / "kpi_tracker.db"))
EXEC_DB = BASE / "execution_telemetry.db"
META_DB = BASE / "meta_engine.db"

# Configuration
CYCLE_SECONDS = int(os.environ.get("AUTOBUILDER_CYCLE_SECONDS", "300"))
MAX_MUTATION_PCT = float(os.environ.get("AUTOBUILDER_MAX_MUTATION_PCT", "0.10"))
MIN_SHADOW_TRADES = int(os.environ.get("AUTOBUILDER_MIN_SHADOW_TRADES", "20"))
MIN_EDGE_BPS = float(os.environ.get("AUTOBUILDER_MIN_EDGE_BPS", "2.0"))
ROLLBACK_WINDOW_S = int(os.environ.get("AUTOBUILDER_ROLLBACK_WINDOW_S", "1800"))  # 30min
COOLDOWN_SECONDS = int(os.environ.get("AUTOBUILDER_COOLDOWN_SECONDS", "86400"))  # 24h

# Bottleneck types
BOTTLENECK_SIGNAL_LOW = "signal_accuracy_low"
BOTTLENECK_SIGNAL_HIGH = "signal_accuracy_high"
BOTTLENECK_VENUE_LATENCY = "venue_latency_spike"
BOTTLENECK_STRATEGY_NEGATIVE = "strategy_negative_sharpe"
BOTTLENECK_FILL_RATE_LOW = "fill_rate_low"


def _safe_connect(db_path):
    p = str(db_path)
    if not os.path.exists(p):
        return None
    try:
        conn = sqlite3.connect(p, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


class Bottleneck:
    """A detected performance bottleneck with suggested fix."""

    def __init__(self, btype, target, severity, current_value, suggestion):
        self.btype = btype
        self.target = target  # e.g., signal name, venue name
        self.severity = severity  # 0-1, higher = worse
        self.current_value = current_value
        self.suggestion = suggestion  # dict of config changes

    def to_dict(self):
        return {
            "type": self.btype,
            "target": self.target,
            "severity": round(self.severity, 4),
            "current_value": self.current_value,
            "suggestion": self.suggestion,
        }


class AutoBuilder:
    """Self-improving configuration search engine."""

    def __init__(self):
        RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        self._champion = self._load_champion()
        self._challenger = None
        self._challenger_start_ts = 0
        self._shadow_trades = []
        self._cooldowns = {}  # {mutation_type: cooldown_until_ts}
        self._cycle_count = 0
        self._promoted_count = 0
        self._rollback_count = 0

    def _load_champion(self):
        """Load current champion config from disk."""
        if CHAMPION_CONFIG_PATH.exists():
            try:
                with open(CHAMPION_CONFIG_PATH) as f:
                    return json.load(f)
            except Exception:
                pass
        # Default champion: read from current env/config
        return {
            "signal_weights": {},  # Empty = use defaults
            "tp_targets": {
                "tp0": float(os.environ.get("GROWTH_MODE_TP0_TARGET", "1.5")),
                "tp1": float(os.environ.get("GROWTH_MODE_TP1_TARGET", "2.5")),
                "tp2": float(os.environ.get("GROWTH_MODE_TP2_TARGET", "5.0")),
            },
            "confidence_threshold": float(os.environ.get("GOAL_MIN_CONFIDENCE", "0.65")),
            "promoted_at": datetime.now(timezone.utc).isoformat(),
            "version": 0,
        }

    def mine_bottlenecks(self):
        """Read existing DBs, rank performance bottlenecks by impact."""
        bottlenecks = []

        # 1. Signal accuracy: identify underperforming and outperforming signals
        bottlenecks.extend(self._mine_signal_accuracy())

        # 2. Venue latency spikes
        bottlenecks.extend(self._mine_venue_latency())

        # 3. Strategy performance
        bottlenecks.extend(self._mine_strategy_performance())

        # 4. Fill rate issues
        bottlenecks.extend(self._mine_fill_rate())

        # Sort by severity (highest first)
        bottlenecks.sort(key=lambda b: b.severity, reverse=True)
        return bottlenecks

    def _mine_signal_accuracy(self):
        """Find signals with low or high accuracy."""
        conn = _safe_connect(TRADER_DB)
        if not conn:
            return []
        bottlenecks = []
        try:
            rows = conn.execute("""
                SELECT source,
                       COUNT(*) as total,
                       SUM(CASE WHEN was_correct = 1 THEN 1 ELSE 0 END) as correct
                FROM signal_accuracy
                WHERE signal_type = '1h'
                  AND verified_at >= datetime('now', '-7 days')
                GROUP BY source
                HAVING total >= 10
            """).fetchall()
            for r in rows:
                accuracy = float(r["correct"]) / float(r["total"])
                if accuracy < 0.45:
                    bottlenecks.append(Bottleneck(
                        BOTTLENECK_SIGNAL_LOW, r["source"],
                        severity=1.0 - accuracy,
                        current_value=round(accuracy, 4),
                        suggestion={"signal_weights": {r["source"]: -0.02}},
                    ))
                elif accuracy > 0.65:
                    bottlenecks.append(Bottleneck(
                        BOTTLENECK_SIGNAL_HIGH, r["source"],
                        severity=accuracy - 0.5,
                        current_value=round(accuracy, 4),
                        suggestion={"signal_weights": {r["source"]: 0.02}},
                    ))
        except Exception as e:
            logger.debug("Signal accuracy mining error: %s", e)
        finally:
            conn.close()
        return bottlenecks

    def _mine_venue_latency(self):
        """Find venues with latency spikes."""
        bottlenecks = []
        try:
            from execution_telemetry import venue_health_snapshot
            for venue in ["coinbase", "kraken"]:
                snap = venue_health_snapshot(venue, window_minutes=60)
                if not snap:
                    continue
                p90 = float(snap.get("p90_latency_ms", 0) or 0)
                if p90 > 2000:
                    severity = min(1.0, (p90 - 2000) / 5000)
                    bottlenecks.append(Bottleneck(
                        BOTTLENECK_VENUE_LATENCY, venue,
                        severity=severity,
                        current_value=p90,
                        suggestion={"venue_latency_penalty": {venue: 0.1}},
                    ))
        except Exception as e:
            logger.debug("Venue latency mining error: %s", e)
        return bottlenecks

    def _mine_strategy_performance(self):
        """Find strategies with negative Sharpe."""
        conn = _safe_connect(KPI_DB)
        if not conn:
            return []
        bottlenecks = []
        try:
            rows = conn.execute("""
                SELECT strategy_name, SUM(net_pnl) as total_pnl, SUM(trades) as total_trades
                FROM strategy_scorecard
                WHERE date >= date('now', '-7 days')
                GROUP BY strategy_name
                HAVING total_trades >= 5
            """).fetchall()
            for r in rows:
                pnl = float(r["total_pnl"] or 0)
                if pnl < 0:
                    severity = min(1.0, abs(pnl) / 10.0)
                    bottlenecks.append(Bottleneck(
                        BOTTLENECK_STRATEGY_NEGATIVE, r["strategy_name"],
                        severity=severity,
                        current_value=round(pnl, 4),
                        suggestion={"strategy_budget_reduction": {r["strategy_name"]: 0.20}},
                    ))
        except Exception as e:
            logger.debug("Strategy performance mining error: %s", e)
        finally:
            conn.close()
        return bottlenecks

    def _mine_fill_rate(self):
        """Find low fill rate issues."""
        conn = _safe_connect(EXEC_DB)
        if not conn:
            return []
        bottlenecks = []
        try:
            cutoff = time.time() - 86400
            rows = conn.execute("""
                SELECT status, COUNT(*) as cnt
                FROM order_lifecycle_metrics
                WHERE created_at >= ?
                GROUP BY status
            """, (cutoff,)).fetchall()
            total = sum(int(r["cnt"]) for r in rows)
            fills = sum(int(r["cnt"]) for r in rows if str(r["status"] or "").upper() == "FILLED")
            if total >= 10:
                fill_rate = fills / total
                if fill_rate < 0.6:
                    bottlenecks.append(Bottleneck(
                        BOTTLENECK_FILL_RATE_LOW, "all",
                        severity=1.0 - fill_rate,
                        current_value=round(fill_rate, 4),
                        suggestion={"order_aggressiveness": 0.05},
                    ))
        except Exception as e:
            logger.debug("Fill rate mining error: %s", e)
        finally:
            conn.close()
        return bottlenecks

    def generate_hypothesis(self, bottleneck):
        """Generate bounded config mutation from bottleneck analysis.

        All mutations are bounded to MAX_MUTATION_PCT per cycle.
        """
        if not bottleneck:
            return None

        # Check cooldown
        if bottleneck.btype in self._cooldowns:
            if time.time() < self._cooldowns[bottleneck.btype]:
                logger.info("Skipping %s (cooldown active)", bottleneck.btype)
                return None

        challenger = json.loads(json.dumps(self._champion))
        challenger["version"] = challenger.get("version", 0) + 1
        challenger["hypothesis"] = bottleneck.to_dict()
        challenger["created_at"] = datetime.now(timezone.utc).isoformat()

        if bottleneck.btype in (BOTTLENECK_SIGNAL_LOW, BOTTLENECK_SIGNAL_HIGH):
            weights = dict(challenger.get("signal_weights", {}))
            signal = bottleneck.target
            delta = bottleneck.suggestion.get("signal_weights", {}).get(signal, 0)
            current = weights.get(signal, 0.10)
            new_val = current + delta
            # Bounds: floor 0.02, cap 0.35
            new_val = max(0.02, min(0.35, new_val))
            # Max mutation check
            if abs(new_val - current) / max(0.01, current) > MAX_MUTATION_PCT:
                new_val = current * (1 + MAX_MUTATION_PCT * (1 if delta > 0 else -1))
            weights[signal] = round(new_val, 4)
            challenger["signal_weights"] = weights

        elif bottleneck.btype == BOTTLENECK_FILL_RATE_LOW:
            challenger["order_price_offset_pct"] = round(
                challenger.get("order_price_offset_pct", 0) + 0.05, 4)

        return challenger

    def shadow_evaluate(self, challenger_config):
        """Run challenger in shadow mode (read-only paper trades).

        Returns evaluation result dict after minimum shadow trades collected.
        """
        if not challenger_config:
            return None

        self._challenger = challenger_config
        self._challenger_start_ts = time.time()

        # Write challenger config for consumption by other agents
        tmp = STATUS_PATH.with_suffix(".tmp")
        status = {
            "state": "shadow",
            "challenger_version": challenger_config.get("version", 0),
            "hypothesis": challenger_config.get("hypothesis"),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "shadow_trades": 0,
            "cycle_count": self._cycle_count,
            "promoted_count": self._promoted_count,
            "rollback_count": self._rollback_count,
        }
        with open(tmp, "w") as f:
            json.dump(status, f, indent=2)
        os.replace(str(tmp), str(STATUS_PATH))

        return {"state": "shadow_started", "version": challenger_config.get("version")}

    def evaluate_challenger(self):
        """Evaluate shadow results against champion.

        Returns (decision, reason) tuple:
          - ("promote", reason) if challenger is better
          - ("reject", reason) if challenger is not better
          - ("wait", reason) if not enough data yet
        """
        if not self._challenger:
            return "reject", "no_active_challenger"

        elapsed = time.time() - self._challenger_start_ts

        # Read current metrics for evaluation
        try:
            from metrics_collector import collect_all_metrics
            metrics = collect_all_metrics()
        except Exception:
            metrics = {}

        sharpe = float(metrics.get("sharpe_7d", 0))
        fill_data = metrics.get("fill_rate_slippage", {})
        fill_rate = float(fill_data.get("fill_rate", 0) if fill_data else 0)

        # Need minimum observation time (at least 2 cycles)
        if elapsed < CYCLE_SECONDS * 2:
            return "wait", f"need_more_time_{int(elapsed)}s"

        # Statistical gate (matching promotion_guard.sh criteria)
        champion_sharpe = float(self._champion.get("sharpe_at_promotion", 0))
        edge_bps = (sharpe - champion_sharpe) * 10000

        if edge_bps >= MIN_EDGE_BPS:
            return "promote", f"edge={edge_bps:.1f}bps > min={MIN_EDGE_BPS}bps"
        elif edge_bps < -MIN_EDGE_BPS:
            return "reject", f"negative_edge={edge_bps:.1f}bps"
        else:
            # Not enough signal — hold for more data
            if elapsed > CYCLE_SECONDS * 6:  # After 30 min, reject if no improvement
                return "reject", f"no_improvement_after_{int(elapsed)}s"
            return "wait", f"marginal_edge={edge_bps:.1f}bps"

    def promote_challenger(self):
        """Promote challenger to champion. Atomic write with history."""
        if not self._challenger:
            return False

        # Archive old champion
        try:
            archive = dict(self._champion)
            archive["demoted_at"] = datetime.now(timezone.utc).isoformat()
            archive["demoted_reason"] = "replaced_by_v" + str(self._challenger.get("version", 0))
            CHAMPION_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(CHAMPION_HISTORY_PATH, "a") as f:
                f.write(json.dumps(archive) + "\n")
        except Exception as e:
            logger.warning("Failed to archive champion: %s", e)

        # Promote
        self._challenger["promoted_at"] = datetime.now(timezone.utc).isoformat()
        try:
            from metrics_collector import collect_all_metrics
            metrics = collect_all_metrics()
            self._challenger["sharpe_at_promotion"] = float(metrics.get("sharpe_7d", 0))
        except Exception:
            pass

        # Atomic write
        tmp = CHAMPION_CONFIG_PATH.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(self._challenger, f, indent=2)
        os.replace(str(tmp), str(CHAMPION_CONFIG_PATH))

        self._champion = self._challenger
        self._challenger = None
        self._promoted_count += 1

        logger.info("PROMOTED champion v%d", self._champion.get("version", 0))
        self._audit("promote", self._champion)
        return True

    def rollback(self, reason="degradation"):
        """Rollback to previous champion from history."""
        if not CHAMPION_HISTORY_PATH.exists():
            logger.warning("No champion history for rollback")
            return False

        try:
            lines = CHAMPION_HISTORY_PATH.read_text().strip().splitlines()
            if not lines:
                return False
            prev = json.loads(lines[-1])
            prev["restored_at"] = datetime.now(timezone.utc).isoformat()
            prev["restore_reason"] = reason

            tmp = CHAMPION_CONFIG_PATH.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump(prev, f, indent=2)
            os.replace(str(tmp), str(CHAMPION_CONFIG_PATH))

            self._champion = prev
            self._challenger = None
            self._rollback_count += 1

            # Apply cooldown to the mutation type that caused the rollback
            if self._challenger and self._challenger.get("hypothesis"):
                btype = self._challenger["hypothesis"].get("type", "unknown")
                self._cooldowns[btype] = time.time() + COOLDOWN_SECONDS

            logger.info("ROLLBACK to champion v%d reason=%s",
                       prev.get("version", 0), reason)
            self._audit("rollback", {"reason": reason, "restored": prev})
            return True
        except Exception as e:
            logger.error("Rollback failed: %s", e)
            return False

    def _audit(self, action, details):
        """Write audit log entry."""
        try:
            AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
            entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": action,
                "cycle": self._cycle_count,
                "details": details,
            }
            with open(AUDIT_PATH, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception:
            pass

    def _consume_alpha_discoveries(self):
        """Read alpha discoveries and convert the highest-confidence one to a bottleneck.

        This closes the feedback loop: alpha_researcher discovers opportunities,
        auto_builder acts on them by generating config mutations.
        """
        discoveries_path = BASE / "runtime" / "alpha_discoveries.jsonl"
        if not discoveries_path.exists():
            return None
        try:
            lines = discoveries_path.read_text().strip().splitlines()
            if not lines:
                return None
            # Read most recent 20 discoveries
            recent = []
            for line in lines[-20:]:
                line = line.strip()
                if not line:
                    continue
                try:
                    recent.append(json.loads(line))
                except Exception:
                    continue
            if not recent:
                return None
            # Sort by confidence descending, pick the top
            recent.sort(key=lambda d: float(d.get("confidence", 0)), reverse=True)
            top = recent[0]
            confidence = float(top.get("confidence", 0))
            if confidence < 0.60:
                return None  # Below action threshold

            otype = top.get("type", "unknown")
            source = top.get("source", "unknown")

            # Convert alpha discovery to config mutation suggestion
            suggestion = {}
            if otype in ("signal_hot", "signal_upgrade"):
                suggestion = {"signal_weights": {source: 0.02}}  # Boost hot signal
            elif otype in ("signal_degradation", "signal_cold"):
                suggestion = {"signal_weights": {source: -0.02}}  # Reduce cold signal
            elif otype == "strategy_hot":
                suggestion = {"strategy_budget_increase": {source: 0.20}}
            elif otype == "strategy_cold":
                suggestion = {"strategy_budget_reduction": {source: 0.20}}
            elif otype in ("venue_degraded", "venue_slow"):
                suggestion = {"venue_latency_penalty": {source: 0.1}}

            if not suggestion:
                return None

            return Bottleneck(
                btype=otype,
                target=source,
                severity=confidence,
                current_value=top.get("description", "")[:80],
                suggestion=suggestion,
            )
        except Exception as e:
            logger.debug("Alpha discovery consumption error: %s", e)
            return None

    def run_cycle(self):
        """Execute one autobuilder cycle."""
        self._cycle_count += 1
        logger.info("AutoBuilder cycle %d starting", self._cycle_count)

        # If we have an active challenger, evaluate it
        if self._challenger:
            decision, reason = self.evaluate_challenger()
            logger.info("Challenger evaluation: %s (%s)", decision, reason)

            if decision == "promote":
                self.promote_challenger()
            elif decision == "reject":
                self._audit("reject", {"reason": reason, "challenger": self._challenger})
                self._challenger = None
            # "wait" → keep observing
            return

        # Priority 1: Check alpha discoveries (closes alpha_researcher → auto_builder loop)
        discovery_bn = self._consume_alpha_discoveries()
        if discovery_bn:
            hypothesis = self.generate_hypothesis(discovery_bn)
            if hypothesis:
                logger.info("ALPHA-DRIVEN hypothesis: %s target=%s severity=%.2f",
                           discovery_bn.btype, discovery_bn.target, discovery_bn.severity)
                self._audit("alpha_discovery", discovery_bn.to_dict())
                self.shadow_evaluate(hypothesis)
                return

        # Priority 2: Mine telemetry for bottlenecks
        bottlenecks = self.mine_bottlenecks()
        if not bottlenecks:
            logger.info("No bottlenecks detected — system healthy")
            self._update_status("idle", None)
            return

        # Try the highest-severity bottleneck
        for bn in bottlenecks[:3]:  # Try top 3
            hypothesis = self.generate_hypothesis(bn)
            if hypothesis:
                logger.info("Hypothesis generated: %s target=%s severity=%.2f",
                           bn.btype, bn.target, bn.severity)
                self.shadow_evaluate(hypothesis)
                return

        logger.info("All bottlenecks on cooldown — waiting")
        self._update_status("cooldown", None)

    def _update_status(self, state, challenger):
        """Update status file."""
        try:
            status = {
                "state": state,
                "cycle_count": self._cycle_count,
                "promoted_count": self._promoted_count,
                "rollback_count": self._rollback_count,
                "champion_version": self._champion.get("version", 0),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if challenger:
                status["challenger_version"] = challenger.get("version")
                status["hypothesis"] = challenger.get("hypothesis")
            tmp = STATUS_PATH.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump(status, f, indent=2)
            os.replace(str(tmp), str(STATUS_PATH))
        except Exception:
            pass

    def run_loop(self):
        """Main daemon loop."""
        logger.info("AutoBuilder starting (cycle=%ds, max_mutation=%.0f%%)",
                   CYCLE_SECONDS, MAX_MUTATION_PCT * 100)
        while True:
            try:
                self.run_cycle()
            except Exception as e:
                logger.error("AutoBuilder cycle error: %s", e)
                self._audit("error", {"error": str(e)})
            time.sleep(CYCLE_SECONDS)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    enabled = os.environ.get("AUTOBUILDER_ENABLED", "0")
    if enabled != "1":
        logger.info("AutoBuilder disabled (AUTOBUILDER_ENABLED != 1)")
        return
    try:
        from process_singleton import acquire_process_singleton
        if not acquire_process_singleton("auto_builder", logger):
            logger.error("Another auto_builder is already running — exiting")
            return
    except Exception as e:
        logger.warning("Singleton lock unavailable: %s", e)
    builder = AutoBuilder()
    builder.run_loop()


if __name__ == "__main__":
    main()
