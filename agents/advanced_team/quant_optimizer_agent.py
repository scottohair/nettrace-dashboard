#!/usr/bin/env python3
"""QuantOptimizerAgent — converts market telemetry into quant/risk overrides.

Inputs:
  - research_memo (market + volatility + cross-exchange)
  - learning_insights (performance feedback)
  - algorithm_tuning (heuristic tuning from AlgorithmOptimizerAgent)

Outputs:
  - quant_optimization (broadcast; strategy can consume pair alpha)
  - quant_risk_overrides (broadcast; risk can consume hard overrides)
  - quant_optimization.json (persisted artifact)
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

logger = logging.getLogger("quant_optimizer_agent")

OUTPUT_FILE = str(Path(__file__).parent / "quant_optimization.json")


class QuantOptimizerAgent:
    """Generates pair alpha and risk posture from latest quant signals."""

    NAME = "quant_optimizer"

    def __init__(self):
        self.bus = MessageBus()
        self.state = {
            "cycle_count": 0,
            "last_msg_id": 0,
        }

    @staticmethod
    def _clamp(value, lo, hi):
        return max(lo, min(hi, value))

    def _read_latest_payload(self, recipient, msg_type, count=5):
        msgs = self.bus.read_latest(recipient, msg_type=msg_type, count=count)
        if not msgs:
            return {}
        return msgs[-1].get("payload", {})

    def _compute_pair_alpha(self, memo):
        """Per-pair alpha in [-0.08, +0.08] from momentum and volatility."""
        pair_alpha = {}
        ranking = []

        coingecko = memo.get("coingecko", {})
        volatility = memo.get("volatility", {})

        for pair, vol_data in volatility.items():
            ticker = pair.split("-")[0]
            cg = coingecko.get(ticker, {})
            change_24h = float(cg.get("change_24h_pct", 0))
            range_pct = float(vol_data.get("range_pct", 0))

            momentum_score = self._clamp(change_24h / 8.0, -1.0, 1.0)
            noise_penalty = self._clamp((range_pct - 8.0) / 8.0, 0.0, 1.0)
            score = momentum_score - (noise_penalty * 0.7)
            alpha = round(self._clamp(score * 0.06, -0.08, 0.08), 4)

            pair_alpha[pair] = alpha
            if pair.endswith("-USD"):
                pair_alpha[pair.replace("-USD", "-USDC")] = alpha

            ranking.append({
                "pair": pair,
                "alpha": alpha,
                "change_24h_pct": round(change_24h, 2),
                "range_pct": round(range_pct, 2),
            })

        ranking.sort(key=lambda r: r["alpha"], reverse=True)
        return pair_alpha, ranking

    def _derive_risk_posture(self, memo, learning):
        fear = int(memo.get("fear_greed", {}).get("value", 50))
        sharpe = float(learning.get("sharpe_ratio", 0))
        drawdown = float(learning.get("max_drawdown_pct", 0))

        if drawdown >= 10 or fear >= 80 or sharpe < -0.5:
            return "DEFENSIVE"
        if drawdown <= 4 and fear <= 65 and sharpe > 0.5:
            return "OFFENSIVE"
        return "BALANCED"

    def _derive_risk_overrides(self, posture, algorithm_tuning, ranking):
        base_min_conf = float(algorithm_tuning.get("min_confidence", 0.55))

        min_confidence = base_min_conf
        max_trade_usd = 5.0
        max_trades_per_cycle = 2
        size_multiplier = 1.0

        if posture == "DEFENSIVE":
            min_confidence += 0.07
            max_trade_usd = 3.5
            max_trades_per_cycle = 1
            size_multiplier = 0.75
        elif posture == "OFFENSIVE":
            min_confidence -= 0.02
            max_trade_usd = 5.0
            max_trades_per_cycle = 3
            size_multiplier = 1.10

        blocked_pairs = []
        # Hard block deep-negative alpha pairs.
        for row in ranking:
            if row["alpha"] <= -0.06:
                blocked_pairs.append(row["pair"])
                if row["pair"].endswith("-USD"):
                    blocked_pairs.append(row["pair"].replace("-USD", "-USDC"))

        min_confidence = round(self._clamp(min_confidence, 0.62, 0.85), 4)
        max_trade_usd = round(self._clamp(max_trade_usd, 1.0, 5.0), 2)
        max_trades_per_cycle = int(self._clamp(max_trades_per_cycle, 1, 4))
        size_multiplier = round(self._clamp(size_multiplier, 0.6, 1.2), 4)

        return {
            "risk_posture": posture,
            "min_confidence": min_confidence,
            "max_trade_usd": max_trade_usd,
            "max_trades_per_cycle": max_trades_per_cycle,
            "blocked_pairs": sorted(set(blocked_pairs)),
            "size_multiplier": size_multiplier,
        }

    def run(self, cycle):
        logger.info("QuantOptimizerAgent cycle %d starting", cycle)
        self.state["cycle_count"] = cycle

        memo = self._read_latest_payload("strategy", "research_memo", count=3)
        learning = self._read_latest_payload("research", "learning_insights", count=3)
        algorithm_tuning = self._read_latest_payload("quant_optimizer", "algorithm_tuning", count=3)

        pair_alpha, ranking = self._compute_pair_alpha(memo)
        posture = self._derive_risk_posture(memo, learning)
        risk_overrides = self._derive_risk_overrides(posture, algorithm_tuning, ranking)

        recommendations = []
        if ranking:
            top = ranking[0]
            recommendations.append(
                f"Top alpha: {top['pair']} ({top['alpha']:+.3f}) 24h={top['change_24h_pct']:+.1f}%"
            )
        if risk_overrides["blocked_pairs"]:
            recommendations.append(
                f"Blocked high-risk pairs: {', '.join(risk_overrides['blocked_pairs'][:4])}"
            )
        recommendations.append(
            f"Posture={posture} min_conf={risk_overrides['min_confidence']:.2f} "
            f"max_trade=${risk_overrides['max_trade_usd']:.2f}"
        )

        payload = {
            "cycle": cycle,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pair_alpha": pair_alpha,
            "ranking": ranking[:10],
            "risk_overrides": risk_overrides,
            "size_multiplier": risk_overrides["size_multiplier"],
            "recommendations": recommendations,
            "source": self.NAME,
        }

        try:
            with open(OUTPUT_FILE, "w") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.warning("Could not persist %s: %s", OUTPUT_FILE, e)

        msg_opt = self.bus.publish(
            sender=self.NAME,
            recipient="broadcast",
            msg_type="quant_optimization",
            payload=payload,
            cycle=cycle,
        )
        msg_risk = self.bus.publish(
            sender=self.NAME,
            recipient="broadcast",
            msg_type="quant_risk_overrides",
            payload=risk_overrides,
            cycle=cycle,
        )
        self.state["last_msg_id"] = msg_risk

        logger.info(
            "Quant optimization published (msg_ids=%d,%d): posture=%s top_pairs=%d blocked=%d",
            msg_opt, msg_risk, posture, len(ranking), len(risk_overrides["blocked_pairs"]),
        )

        return payload
