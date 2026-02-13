#!/usr/bin/env python3
"""AlgorithmOptimizerAgent — tunes strategy heuristics from live telemetry.

Inputs:
  - research_memo (from ResearchAgent)
  - strategy_proposal (from StrategyAgent)
  - learning_insights (from LearningAgent)

Outputs:
  - algorithm_tuning (broadcast to all agents)
  - algorithm_tuning.json (persisted artifact for observability)

Purpose:
  - tighten/relax confidence thresholds based on performance
  - apply pair-level confidence bias from win-rate edge
  - nudge factor weights toward currently effective signals
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_AGENTS_DIR = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _AGENTS_DIR)

_env_path = Path(_AGENTS_DIR) / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

from advanced_team.message_bus import MessageBus

logger = logging.getLogger("algorithm_optimizer_agent")

TUNING_FILE = str(Path(__file__).parent / "algorithm_tuning.json")
MIN_PAIR_TRADES = 3

BASE_WEIGHTS = {
    "price_momentum": 0.20,
    "volatility_regime": 0.15,
    "fear_greed": 0.10,
    "nettrace_latency": 0.25,
    "cross_exchange_spread": 0.15,
    "volume_trend": 0.15,
}


class AlgorithmOptimizerAgent:
    """Generates algorithm-tuning suggestions from recent cycle outcomes."""

    NAME = "algorithm_optimizer"

    def __init__(self):
        self.bus = MessageBus()
        self.state = {
            "cycle_count": 0,
            "last_msg_id": 0,
            "tuning_events": 0,
        }

    @staticmethod
    def _clamp(value, lo, hi):
        return max(lo, min(hi, value))

    def _read_latest_payload(self, recipient, msg_type, count=5):
        msgs = self.bus.read_latest(recipient, msg_type=msg_type, count=count)
        if not msgs:
            return {}
        return msgs[-1].get("payload", {})

    def _compute_pair_bias(self, learning):
        """Translate pair win-rate edge into confidence bias."""
        pair_perf = learning.get("pair_performance", {})
        pair_bias = {}
        notes = []

        for pair, stats in pair_perf.items():
            trades = int(stats.get("trade_count", 0))
            if trades < MIN_PAIR_TRADES:
                continue

            win_rate = float(stats.get("win_rate", 0.5))
            edge = win_rate - 0.5
            bias = round(self._clamp(edge * 0.30, -0.08, 0.08), 4)
            if abs(bias) < 0.01:
                continue

            pair_bias[pair] = bias
            # Map USDC pair stats back to research USD pairs.
            if pair.endswith("-USDC"):
                pair_bias[pair.replace("-USDC", "-USD")] = bias

            notes.append(f"{pair}: win_rate={win_rate:.0%} -> bias {bias:+.3f}")

        return pair_bias, notes

    def _derive_min_confidence(self, learning, research):
        """Dynamic confidence floor based on risk and regime."""
        threshold = 0.55
        sharpe = float(learning.get("sharpe_ratio", 0))
        drawdown = float(learning.get("max_drawdown_pct", 0))
        fear = int(research.get("fear_greed", {}).get("value", 50))

        if sharpe < -0.5:
            threshold += 0.06
        elif sharpe > 1.5:
            threshold -= 0.03

        if drawdown > 8:
            threshold += 0.05
        elif drawdown < 3:
            threshold -= 0.01

        if fear >= 75:
            threshold += 0.04
        elif fear <= 25:
            threshold -= 0.02

        return round(self._clamp(threshold, 0.50, 0.75), 4)

    def _derive_size_multiplier(self, learning):
        """Portfolio-wide sizing pressure from drawdown + Sharpe."""
        mult = 1.0
        sharpe = float(learning.get("sharpe_ratio", 0))
        drawdown = float(learning.get("max_drawdown_pct", 0))

        if drawdown > 10:
            mult *= 0.75
        elif drawdown > 6:
            mult *= 0.85
        elif drawdown < 3 and sharpe > 1.0:
            mult *= 1.08

        if sharpe < 0:
            mult *= 0.9
        elif sharpe > 2:
            mult *= 1.05

        return round(self._clamp(mult, 0.60, 1.20), 4)

    def _derive_weight_overrides(self, strategy_msgs):
        """Slightly rebalance factor weights from recent proposal scores."""
        factor_values = {k: [] for k in BASE_WEIGHTS}

        for msg in strategy_msgs:
            payload = msg.get("payload", {})
            scores = payload.get("score_breakdown", {})
            if not scores:
                continue
            for factor in BASE_WEIGHTS:
                if factor in scores:
                    try:
                        factor_values[factor].append(float(scores[factor]))
                    except Exception:
                        pass

        adjusted = dict(BASE_WEIGHTS)
        for factor, values in factor_values.items():
            if not values:
                continue
            avg_score = sum(values) / len(values)
            if avg_score >= 0.65:
                adjusted[factor] += 0.03
            elif avg_score <= 0.45:
                adjusted[factor] -= 0.03

        # Normalize to sum=1.0 and keep bounded for stability.
        for factor in adjusted:
            adjusted[factor] = self._clamp(adjusted[factor], 0.05, 0.35)
        total = sum(adjusted.values()) or 1.0
        normalized = {k: round(v / total, 4) for k, v in adjusted.items()}
        return normalized

    def run(self, cycle):
        logger.info("AlgorithmOptimizerAgent cycle %d starting", cycle)
        self.state["cycle_count"] = cycle

        research = self._read_latest_payload("strategy", "research_memo", count=3)
        learning = self._read_latest_payload("research", "learning_insights", count=3)
        strategy_msgs = self.bus.read_latest("risk", msg_type="strategy_proposal", count=20)

        pair_bias, bias_notes = self._compute_pair_bias(learning)
        min_confidence = self._derive_min_confidence(learning, research)
        size_multiplier = self._derive_size_multiplier(learning)
        weight_overrides = self._derive_weight_overrides(strategy_msgs)

        recommendations = []
        recommendations.extend(bias_notes[:4])
        recommendations.append(f"Global min confidence -> {min_confidence:.2f}")
        recommendations.append(f"Global size multiplier -> {size_multiplier:.2f}x")

        tuning = {
            "cycle": cycle,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "min_confidence": min_confidence,
            "size_multiplier": size_multiplier,
            "pair_bias": pair_bias,
            "factor_weight_overrides": weight_overrides,
            "blocked_pairs": [],
            "recommendations": recommendations,
            "source": self.NAME,
        }

        try:
            with open(TUNING_FILE, "w") as f:
                json.dump(tuning, f, indent=2)
        except Exception as e:
            logger.warning("Could not persist %s: %s", TUNING_FILE, e)

        msg_id = self.bus.publish(
            sender=self.NAME,
            recipient="broadcast",
            msg_type="algorithm_tuning",
            payload=tuning,
            cycle=cycle,
        )
        self.state["last_msg_id"] = msg_id
        self.state["tuning_events"] += 1

        logger.info(
            "Algorithm tuning published (msg_id=%d): min_conf=%.2f size_mult=%.2fx pair_biases=%d",
            msg_id, min_confidence, size_multiplier, len(pair_bias),
        )

        return tuning
