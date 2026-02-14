#!/usr/bin/env python3
"""StrategyAgent — generates trade signals and strategy proposals.

Takes research memos from ResearchAgent, analyzes them, and produces
strategy proposals with:
  - Trade direction (BUY/SELL/HOLD)
  - Asset and pair
  - Confidence score (composite of multiple signals)
  - Reasoning chain
  - Suggested size (USD)
  - Entry/exit levels
  - Strategy type tag

Game Theory principles:
  - Nash Equilibrium: avoid crowded strategies
  - Information asymmetry: use NetTrace latency data as private signal
  - Maker preference: limit orders over market orders
  - Multi-factor: require 2+ confirming signals

Input:  research_memo from ResearchAgent
Output: strategy_proposal -> RiskAgent
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_AGENTS_DIR = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _AGENTS_DIR)

# Load .env from parent agents/ dir
_env_path = Path(_AGENTS_DIR) / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

from advanced_team.message_bus import MessageBus

logger = logging.getLogger("strategy_agent")

# Strategy weights for composite confidence
SIGNAL_WEIGHTS = {
    "price_momentum": 0.20,
    "volatility_regime": 0.15,
    "fear_greed": 0.10,
    "nettrace_latency": 0.25,
    "cross_exchange_spread": 0.15,
    "volume_trend": 0.15,
}

# Minimum composite confidence to generate a proposal
MIN_CONFIDENCE = 0.55

# Pairs we trade (USDC pairs on Coinbase)
TRADEABLE_PAIRS = {
    "BTC-USD": "BTC-USDC",
    "ETH-USD": "ETH-USDC",
    "SOL-USD": "SOL-USDC",
    "AVAX-USD": "AVAX-USDC",
    "LINK-USD": "LINK-USDC",
    "DOGE-USD": "DOGE-USDC",
}


class StrategyAgent:
    """Analyzes research memos and generates strategy proposals."""

    NAME = "strategy"

    def __init__(self):
        self.bus = MessageBus()
        self.state = {
            "last_msg_id": 0,
            "proposals_generated": 0,
            "cycle_count": 0,
        }

    @staticmethod
    def _clamp(value, lo, hi):
        return max(lo, min(hi, value))

    def _read_latest_payload(self, recipient, msg_type, count=5):
        msgs = self.bus.read_latest(recipient, msg_type=msg_type, count=count)
        if not msgs:
            return {}
        return msgs[-1].get("payload", {})

    def _merge_signal_weights(self, overrides):
        """Merge dynamic factor overrides while preserving stable normalization."""
        if not isinstance(overrides, dict) or not overrides:
            return SIGNAL_WEIGHTS

        merged = dict(SIGNAL_WEIGHTS)
        for factor, value in overrides.items():
            if factor in merged:
                try:
                    merged[factor] = float(value)
                except Exception:
                    pass

        # Keep weights bounded and renormalized.
        for factor in merged:
            merged[factor] = self._clamp(merged[factor], 0.05, 0.35)
        total = sum(merged.values()) or 1.0
        return {k: v / total for k, v in merged.items()}

    def _get_optimization_context(self):
        """Read latest algorithm/quant optimization hints from previous cycles."""
        algo = self._read_latest_payload("strategy", "algorithm_tuning", count=5)
        quant = self._read_latest_payload("strategy", "quant_optimization", count=5)

        pair_bias = {}
        pair_bias.update(algo.get("pair_bias", {}))
        pair_bias.update(quant.get("pair_alpha", {}))

        blocked = set(algo.get("blocked_pairs", []) or [])
        blocked.update((quant.get("risk_overrides", {}) or {}).get("blocked_pairs", []) or [])

        return {
            "weights": self._merge_signal_weights(algo.get("factor_weight_overrides", {})),
            "min_confidence": float(algo.get("min_confidence", MIN_CONFIDENCE)),
            "size_multiplier": float(algo.get("size_multiplier", 1.0)) * float(quant.get("size_multiplier", 1.0)),
            "pair_bias": pair_bias,
            "blocked_pairs": blocked,
            "disabled_families": [f for f in (algo.get("disabled_signal_families", []) or []) if f],
        }

    def _score_price_momentum(self, memo, pair):
        """Score based on 24h price change and candle data."""
        base = pair.split("-")[0]

        # CoinGecko 24h change
        cg = memo.get("coingecko", {}).get(base, {})
        change_24h = cg.get("change_24h_pct", 0)

        # Candle volatility
        vol_data = memo.get("volatility", {}).get(pair, {})
        range_pct = vol_data.get("range_pct", 0)

        # Bullish momentum: positive 24h change, moderate volatility
        if change_24h > 2.0 and range_pct < 8.0:
            return 0.8, "BUY", f"Strong momentum: +{change_24h:.1f}% 24h, range {range_pct:.1f}%"
        elif change_24h > 0.5:
            return 0.6, "BUY", f"Mild bullish: +{change_24h:.1f}% 24h"
        elif change_24h < -3.0:
            # Potential dip buy if not a crash
            if range_pct < 10.0:
                return 0.5, "BUY", f"Dip buy opportunity: {change_24h:.1f}% 24h"
            else:
                return 0.3, "HOLD", f"High vol decline: {change_24h:.1f}% 24h, range {range_pct:.1f}%"
        elif change_24h < -1.0:
            return 0.4, "HOLD", f"Declining: {change_24h:.1f}% 24h"
        else:
            return 0.5, "HOLD", f"Flat: {change_24h:.1f}% 24h"

    def _score_volatility_regime(self, memo, pair):
        """Score based on volatility regime — prefer moderate vol."""
        vol_data = memo.get("volatility", {}).get(pair, {})
        if not vol_data:
            return 0.5, "HOLD", "No volatility data"

        avg_vol = vol_data.get("avg_hourly_volatility", 0)
        atr = vol_data.get("atr_24h", 0)
        volume = vol_data.get("volume_24h", 0)

        # Moderate volatility is ideal for trading
        if 0.3 < avg_vol < 1.5:
            return 0.7, "BUY", f"Optimal vol regime: {avg_vol:.2f}% avg hourly"
        elif avg_vol >= 1.5:
            return 0.4, "HOLD", f"High vol: {avg_vol:.2f}% avg hourly — risky"
        else:
            return 0.4, "HOLD", f"Low vol: {avg_vol:.2f}% avg hourly — no edge"

    def _score_fear_greed(self, memo):
        """Score based on Fear & Greed Index — contrarian signal."""
        fg = memo.get("fear_greed", {})
        value = fg.get("value", 50)
        classification = fg.get("classification", "Neutral")

        # Contrarian: buy on extreme fear, hold/caution on extreme greed
        if value <= 20:
            return 0.85, "BUY", f"Extreme Fear ({value}) — contrarian BUY signal"
        elif value <= 35:
            return 0.7, "BUY", f"Fear ({value}) — good entry zone"
        elif value >= 80:
            return 0.3, "HOLD", f"Extreme Greed ({value}) — caution"
        elif value >= 65:
            return 0.5, "HOLD", f"Greed ({value}) — momentum but risky"
        else:
            return 0.5, "HOLD", f"Neutral ({value})"

    def _score_nettrace_signals(self, memo, pair):
        """Score based on NetTrace latency signals — our private edge."""
        signals = memo.get("nettrace_signals", {})
        signal_count = signals.get("signal_count", 0)
        by_type = signals.get("by_type", {})

        if signal_count == 0:
            return 0.5, "HOLD", "No latency signals"

        # Count high-confidence signals targeting crypto exchanges
        crypto_signals = 0
        total_confidence = 0
        for sig_type, entries in by_type.items():
            for entry in entries:
                host = entry.get("host", "")
                conf = entry.get("confidence", 0)
                if any(ex in host for ex in ["coinbase", "binance", "kraken", "bybit", "okx", "gemini"]):
                    crypto_signals += 1
                    total_confidence += conf

        if crypto_signals >= 3:
            avg_conf = total_confidence / crypto_signals
            return min(0.9, avg_conf), "BUY", f"{crypto_signals} latency anomalies (avg conf: {avg_conf:.2f})"
        elif crypto_signals >= 1:
            avg_conf = total_confidence / crypto_signals
            return avg_conf * 0.8, "BUY", f"{crypto_signals} latency anomaly (conf: {avg_conf:.2f})"
        else:
            return 0.4, "HOLD", f"{signal_count} signals but none on crypto exchanges"

    def _score_cross_exchange(self, memo, pair):
        """Score based on cross-exchange spread — arb opportunity."""
        cx = memo.get("cross_exchange", {})
        base = pair.split("-")[0]
        spreads = cx.get("spreads", {}).get(base, {})

        if not spreads:
            return 0.5, "HOLD", "No cross-exchange data"

        spread_pct = spreads.get("spread_pct", 0)
        cheapest = spreads.get("cheapest", "unknown")

        # Significant spread = arb or movement incoming
        if spread_pct >= 0.5:
            if cheapest == "coinbase":
                return 0.8, "BUY", f"Coinbase cheapest, spread {spread_pct:.3f}% — buy here"
            else:
                return 0.6, "HOLD", f"Spread {spread_pct:.3f}% but {cheapest} is cheapest"
        elif spread_pct >= 0.2:
            return 0.55, "HOLD", f"Moderate spread {spread_pct:.3f}%"
        else:
            return 0.5, "HOLD", f"Tight spread {spread_pct:.3f}% — efficient market"

    def _score_volume(self, memo, pair):
        """Score based on volume trend."""
        vol_data = memo.get("volatility", {}).get(pair, {})
        cg_base = pair.split("-")[0]
        cg = memo.get("coingecko", {}).get(cg_base, {})
        vol_24h = cg.get("volume_24h", 0)
        market_cap = cg.get("market_cap", 1)

        if market_cap <= 0:
            return 0.5, "HOLD", "No market data"

        # Volume-to-market-cap ratio — high = active trading
        vol_ratio = vol_24h / market_cap if market_cap > 0 else 0

        if vol_ratio > 0.15:
            return 0.7, "BUY", f"High volume ratio: {vol_ratio:.3f}"
        elif vol_ratio > 0.05:
            return 0.55, "HOLD", f"Normal volume ratio: {vol_ratio:.3f}"
        else:
            return 0.4, "HOLD", f"Low volume ratio: {vol_ratio:.3f}"

    def _apply_learning_adjustments(self, memo, pair, composite_conf, direction):
        """Adjust scores based on learning agent feedback."""
        insights = memo.get("learning_insights", {})
        if not insights:
            return composite_conf, direction

        # Adjust based on pair-specific performance
        pair_stats = insights.get("pair_performance", {}).get(pair, {})
        win_rate = pair_stats.get("win_rate", 0.5)

        # If this pair has been losing, reduce confidence
        if win_rate < 0.3 and pair_stats.get("trade_count", 0) >= 3:
            composite_conf *= 0.7
            logger.info("Reduced confidence for %s (win_rate=%.2f)", pair, win_rate)
        elif win_rate > 0.7 and pair_stats.get("trade_count", 0) >= 3:
            composite_conf *= 1.15
            composite_conf = min(composite_conf, 0.95)
            logger.info("Boosted confidence for %s (win_rate=%.2f)", pair, win_rate)

        # Overall strategy adjustment
        overall_sharpe = insights.get("sharpe_ratio", 0)
        if overall_sharpe < -1.0:
            composite_conf *= 0.8
            logger.info("Overall Sharpe negative (%.2f), reducing all signals", overall_sharpe)

        return composite_conf, direction

    def generate_proposals(self, memo):
        """Analyze research memo and generate strategy proposals."""
        proposals = []
        cycle = memo.get("cycle", 0)
        opt = self._get_optimization_context()
        min_conf = self._clamp(opt["min_confidence"], 0.50, 0.80)
        size_mult = self._clamp(opt["size_multiplier"], 0.6, 1.2)
        disabled_families = set(opt.get("disabled_families", []))
        strategy_data_quality = {
            "data_quality_mode": memo.get("data_quality_mode", "unknown"),
            "data_fidelity": memo.get("data_fidelity", {}),
            "source_health": memo.get("source_health", {}),
        }

        for research_pair, trade_pair in TRADEABLE_PAIRS.items():
            if research_pair in opt["blocked_pairs"] or trade_pair in opt["blocked_pairs"]:
                logger.info("Skipping %s due to quant blocklist", trade_pair)
                continue

            scores = []
            reasons = []

            # 1. Price momentum
            if "price_momentum" not in disabled_families:
                score, direction, reason = self._score_price_momentum(memo, research_pair)
                scores.append(("price_momentum", score, direction))
                reasons.append(f"[momentum] {reason}")
            else:
                reasons.append("[momentum] DISABLED by optimizer guidance")

            # 2. Volatility regime
            if "volatility_regime" not in disabled_families:
                score, direction, reason = self._score_volatility_regime(memo, research_pair)
                scores.append(("volatility_regime", score, direction))
                reasons.append(f"[volatility] {reason}")
            else:
                reasons.append("[volatility] DISABLED by optimizer guidance")

            # 3. Fear & Greed (same for all pairs)
            if "fear_greed" not in disabled_families:
                score, direction, reason = self._score_fear_greed(memo)
                scores.append(("fear_greed", score, direction))
                reasons.append(f"[fear_greed] {reason}")
            else:
                reasons.append("[fear_greed] DISABLED by optimizer guidance")

            # 4. NetTrace latency signals
            if "nettrace_latency" not in disabled_families:
                score, direction, reason = self._score_nettrace_signals(memo, research_pair)
                scores.append(("nettrace_latency", score, direction))
                reasons.append(f"[nettrace] {reason}")
            else:
                reasons.append("[nettrace] DISABLED by optimizer guidance")

            # 5. Cross-exchange spread
            if "cross_exchange_spread" not in disabled_families:
                score, direction, reason = self._score_cross_exchange(memo, research_pair)
                scores.append(("cross_exchange_spread", score, direction))
                reasons.append(f"[spread] {reason}")
            else:
                reasons.append("[spread] DISABLED by optimizer guidance")

            # 6. Volume trend
            if "volume_trend" not in disabled_families:
                score, direction, reason = self._score_volume(memo, research_pair)
                scores.append(("volume_trend", score, direction))
                reasons.append(f"[volume] {reason}")
            else:
                reasons.append("[volume] DISABLED by optimizer guidance")

            if not scores:
                logger.info(
                    "Skipping %s due to all signal families disabled", trade_pair
                )
                continue

            # Compute weighted composite confidence
            composite = 0.0
            for factor_name, score, _ in scores:
                weight = opt["weights"].get(factor_name, SIGNAL_WEIGHTS.get(factor_name, 0.1))
                composite += score * weight

            # Pair-level alpha/bias from optimizer agents.
            pair_bias = float(
                opt["pair_bias"].get(research_pair, opt["pair_bias"].get(trade_pair, 0.0))
            )
            if pair_bias:
                composite = self._clamp(composite + pair_bias, 0.0, 0.99)
                reasons.append(f"[optimizer] Pair bias applied: {pair_bias:+.3f}")

            # Count confirming BUY signals
            buy_count = sum(1 for _, _, d in scores if d == "BUY")

            # Determine direction: majority vote
            if buy_count >= 3:
                direction = "BUY"
            elif buy_count >= 2 and composite > 0.6:
                direction = "BUY"
            else:
                direction = "HOLD"

            # Apply learning adjustments
            composite, direction = self._apply_learning_adjustments(
                memo, research_pair, composite, direction
            )

            # Only propose if above minimum confidence AND direction is BUY
            if composite >= min_conf and direction == "BUY":
                # Determine trade size based on confidence
                # Higher confidence = larger position (up to $5 max)
                suggested_size = round(min(5.0, max(1.0, composite * 7.0 * size_mult)), 2)

                # Get current price for entry/exit levels
                price_data = memo.get("prices", {}).get(research_pair, {})
                current_price = price_data.get("price", 0)

                proposal = {
                    "cycle": cycle,
                    "pair": trade_pair,
                    "research_pair": research_pair,
                    "direction": direction,
                    "confidence": round(composite, 4),
                    "confirming_signals": buy_count,
                    "suggested_size_usd": suggested_size,
                    "entry_price": current_price,
                    "stop_loss_pct": 2.0,  # 2% stop loss
                    "take_profit_pct": 3.0,  # 3% take profit (1.5:1 R:R)
                    "order_type": "limit" if composite < 0.8 else "market",
                    "strategy_type": "multi_factor_momentum",
                    "reasons": reasons,
                    "score_breakdown": {n: round(s, 3) for n, s, _ in scores},
                    "optimizer_context": {
                        "dynamic_min_confidence": round(min_conf, 4),
                        "size_multiplier": round(size_mult, 4),
                        "pair_bias": round(pair_bias, 4),
                    },
                    "data_quality_mode": strategy_data_quality["data_quality_mode"],
                    "data_fidelity": strategy_data_quality["data_fidelity"],
                    "source_health": strategy_data_quality["source_health"],
                }
                proposals.append(proposal)

        # Sort by confidence, highest first
        proposals.sort(key=lambda p: -p["confidence"])

        return proposals

    def run(self, cycle):
        """Process latest research memo and generate proposals."""
        logger.info("StrategyAgent cycle %d starting", cycle)
        self.state["cycle_count"] = cycle

        # Read latest research memo
        msgs = self.bus.read_latest("strategy", msg_type="research_memo", count=1)
        if not msgs:
            logger.warning("No research memo found for cycle %d", cycle)
            return []

        memo = msgs[-1].get("payload", {})
        self.state["last_msg_id"] = msgs[-1].get("id", 0)

        # Generate proposals
        proposals = self.generate_proposals(memo)

        # Publish each proposal to RiskAgent
        for proposal in proposals:
            msg_id = self.bus.publish(
                sender=self.NAME,
                recipient="risk",
                msg_type="strategy_proposal",
                payload=proposal,
                cycle=cycle,
            )
            logger.info("Proposal: %s %s conf=%.2f size=$%.2f (msg_id=%d)",
                         proposal["direction"], proposal["pair"],
                         proposal["confidence"], proposal["suggested_size_usd"], msg_id)

        self.state["proposals_generated"] += len(proposals)

        if not proposals:
            opt = self._get_optimization_context()
            # Still publish a HOLD summary so the DFA continues
            self.bus.publish(
                sender=self.NAME,
                recipient="risk",
                msg_type="strategy_proposal",
                payload={
                    "cycle": cycle,
                    "direction": "HOLD",
                    "pair": "NONE",
                    "confidence": 0.0,
                    "data_quality_mode": memo.get("data_quality_mode", "unknown"),
                    "data_fidelity": memo.get("data_fidelity", {}),
                    "source_health": memo.get("source_health", {}),
                    "reasons": [
                        f"No signals met dynamic confidence threshold ({opt['min_confidence']:.2f})"
                    ],
                },
                cycle=cycle,
            )
            logger.info("No proposals generated — all signals below threshold")

        return proposals
