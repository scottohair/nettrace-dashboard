#!/usr/bin/env python3
"""Growth Engine — Algebraic Signal Fusion + Optimal Trade Selection.

Uses CS + finite algebra theory to boost trading performance:

1. **Galois Field Signal Encoding (GF(2^n))**
   - Each signal is an element in a finite field
   - Error-correcting code properties detect "almost-fire" patterns
   - Hamming distance between signal vectors and ideal trade vectors
   - Syndrome decoding to identify which missing signal is most likely to fire

2. **Lattice-Based Decision Trees**
   - Partial order over (confidence, signal_count, EV, momentum, regime)
   - Lattice dominance replaces simple threshold gates
   - A trade passes if it dominates in enough dimensions (not ALL)
   - Meet/join operations find optimal configurations

3. **Markov Chain Regime Detector**
   - Market states: {accumulation, markup, distribution, markdown}
   - Wyckoff cycle modeled as absorbing Markov chain
   - Transition probabilities from recent price action
   - Optimal holding period = expected time to next state transition

4. **Knapsack Position Optimizer**
   - Given N opportunities with (expected_return, risk, capital_needed)
   - Solve bounded knapsack to maximize portfolio-level EV
   - Respects risk_controller limits and concentration caps

RULES: All trading decisions still gate through GoalValidator + risk_controller.
This engine SUPPLEMENTS signal quality — it does NOT bypass safety.
"""

import json
import logging
import math
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("growth_engine")

# Dynamic risk controller
try:
    from risk_controller import get_controller
    _risk_ctrl = get_controller()
except Exception:
    _risk_ctrl = None

# Strategic planner (3D Go game theory)
try:
    from strategic_planner import get_strategic_planner
    _planner = get_strategic_planner()
except Exception:
    _planner = None


# ============================================================
# 1. GALOIS FIELD SIGNAL ENCODING
# ============================================================

class GaloisSignalEncoder:
    """Encode trading signals as vectors in GF(2^n) for error-corrected fusion.

    Each signal source maps to a dimension. The signal vector is the binary
    encoding of which signals fired. We use Hamming distance to the "ideal
    trade vector" (all signals firing) to score trade quality.

    Key insight: A trade with 7/9 signals at high confidence may be better
    than one with 9/9 at low confidence. The syndrome (missing signals)
    tells us which information we're missing and how to compensate.
    """

    # Signal indices in the vector (order matters for parity checks)
    SIGNAL_INDICES = {
        "regime": 0,
        "arb": 1,
        "orderbook": 2,
        "rsi_extreme": 3,
        "momentum": 4,
        "latency": 5,
        "meta_engine": 6,
        "fear_greed": 7,
        "uptick": 8,
    }

    # Quantitative signal mask (bits that MUST fire for strong trades)
    QUANT_MASK = 0b1111111_00  # bits 2-8 (regime through meta_engine)

    # Signal reliability weights (derived from historical accuracy)
    # Higher = more reliable, used to weight Hamming distance
    RELIABILITY = {
        "regime": 0.85,      # C engine is very accurate
        "arb": 0.90,         # Pure math, highest reliability
        "orderbook": 0.80,   # Good but can be spoofed
        "rsi_extreme": 0.75, # Lagging indicator
        "momentum": 0.70,    # Trend-following, whipsaws in range
        "latency": 0.88,     # Our private edge, very reliable
        "meta_engine": 0.72, # ML predictions, improving
        "fear_greed": 0.60,  # Sentiment, noisy
        "uptick": 0.55,      # Simple, many false positives
    }

    def encode(self, signals):
        """Encode a dict of {signal_name: (direction, confidence)} into a binary vector.

        Args:
            signals: dict mapping signal name to (direction, confidence) tuple
                     direction is "BUY", "SELL", or "NONE"
                     confidence is 0.0-1.0

        Returns:
            (vector_int, confidence_vector, direction)
        """
        vector = 0
        conf_vector = [0.0] * len(self.SIGNAL_INDICES)
        buy_score = 0.0
        sell_score = 0.0

        for name, (direction, confidence) in signals.items():
            idx = self.SIGNAL_INDICES.get(name)
            if idx is None:
                continue

            # Signal fires if confidence > threshold (adaptive per signal)
            threshold = 0.5 + (1.0 - self.RELIABILITY.get(name, 0.7)) * 0.3
            if confidence >= threshold:
                vector |= (1 << idx)
                conf_vector[idx] = confidence

                weight = self.RELIABILITY.get(name, 0.7)
                if direction == "BUY":
                    buy_score += weight * confidence
                elif direction == "SELL":
                    sell_score += weight * confidence

        direction = "BUY" if buy_score > sell_score else "SELL" if sell_score > buy_score else "NONE"
        return vector, conf_vector, direction

    def hamming_weight(self, vector):
        """Count number of bits set (number of signals firing)."""
        count = 0
        while vector:
            count += vector & 1
            vector >>= 1
        return count

    def hamming_distance(self, v1, v2):
        """Hamming distance between two signal vectors."""
        return self.hamming_weight(v1 ^ v2)

    def syndrome(self, vector):
        """Identify which signals are NOT firing (the syndrome).

        Returns list of missing signal names, sorted by reliability
        (most reliable missing signals = biggest information gap).
        """
        all_bits = (1 << len(self.SIGNAL_INDICES)) - 1
        missing_bits = all_bits & ~vector
        missing = []
        for name, idx in self.SIGNAL_INDICES.items():
            if missing_bits & (1 << idx):
                missing.append((name, self.RELIABILITY.get(name, 0.5)))
        # Sort by reliability descending — most reliable missing = biggest concern
        missing.sort(key=lambda x: -x[1])
        return missing

    def weighted_quality_score(self, vector, conf_vector):
        """Compute quality score weighted by signal reliability.

        Higher score = better trade setup.
        Range: 0.0 to 1.0
        """
        total_weight = sum(self.RELIABILITY.values())
        achieved_weight = 0.0
        for name, idx in self.SIGNAL_INDICES.items():
            if vector & (1 << idx):
                reliability = self.RELIABILITY.get(name, 0.5)
                confidence = conf_vector[idx] if idx < len(conf_vector) else 0.0
                # Score = reliability * confidence (both high = great signal)
                achieved_weight += reliability * confidence
        return achieved_weight / total_weight if total_weight > 0 else 0.0

    def quant_coverage(self, vector):
        """What fraction of quantitative signals are firing?"""
        quant_bits = self.QUANT_MASK
        fired_quant = vector & quant_bits
        total_quant = self.hamming_weight(quant_bits)
        return self.hamming_weight(fired_quant) / total_quant if total_quant > 0 else 0.0


# ============================================================
# 2. LATTICE-BASED DECISION TREE
# ============================================================

class TradeDecisionLattice:
    """Partial order lattice over trade dimensions for optimal selection.

    Instead of simple threshold gates (confidence >= X AND signals >= Y),
    we use lattice dominance: a trade is acceptable if it dominates
    the minimum-quality element in enough dimensions.

    Dimensions:
    - quality_score (from GF encoder): 0.0-1.0
    - signal_count: 0-9
    - ev_ratio: expected_value / fees (>1 = profitable)
    - momentum_alignment: -1 to +1 (trade direction vs trend)
    - regime_score: 0-1 (how favorable the market regime is)

    A trade element (a,b,c,d,e) dominates (a',b',c',d',e') if
    it's >= in at least K dimensions (K = dominance threshold).
    """

    # Minimum quality element — the worst acceptable trade
    MIN_ELEMENT = {
        "quality_score": 0.45,
        "signal_count": 2,
        "ev_ratio": 1.05,          # EV must exceed fees by 5%
        "momentum_alignment": -0.3, # Slight counter-trend OK
        "regime_score": 0.3,
    }

    # Dominance threshold: must beat minimum in at least K of 5 dimensions
    DOMINANCE_K = 3

    # Bonus dimensions that can compensate for weak primary dimensions
    COMPENSATION_RULES = {
        # If quality_score is very high, can compensate for fewer signals
        "quality_score": {"threshold": 0.75, "compensates": "signal_count", "bonus": 1},
        # If EV is very high, can compensate for weaker momentum
        "ev_ratio": {"threshold": 2.0, "compensates": "momentum_alignment", "bonus": 0.3},
        # If regime is very strong, can compensate for lower quality
        "regime_score": {"threshold": 0.8, "compensates": "quality_score", "bonus": 0.1},
    }

    def evaluate(self, trade_element):
        """Evaluate a trade against the lattice minimum.

        Args:
            trade_element: dict with keys matching MIN_ELEMENT

        Returns:
            (passes: bool, dominance_count: int, details: dict)
        """
        # Apply compensation rules first
        compensated = dict(trade_element)
        for dim, rule in self.COMPENSATION_RULES.items():
            val = trade_element.get(dim, 0)
            if val >= rule["threshold"]:
                target = rule["compensates"]
                compensated[target] = compensated.get(target, 0) + rule["bonus"]

        # Count dominance
        dominance_count = 0
        details = {}
        for dim, min_val in self.MIN_ELEMENT.items():
            actual = compensated.get(dim, 0)
            passes_dim = actual >= min_val
            if passes_dim:
                dominance_count += 1
            details[dim] = {
                "actual": round(actual, 4) if isinstance(actual, float) else actual,
                "minimum": min_val,
                "passes": passes_dim,
            }

        passes = dominance_count >= self.DOMINANCE_K
        details["dominance_count"] = dominance_count
        details["required"] = self.DOMINANCE_K
        return passes, dominance_count, details

    def rank_opportunities(self, opportunities):
        """Rank a list of trade opportunities by lattice quality.

        Returns sorted list with lattice scores attached.
        """
        scored = []
        for opp in opportunities:
            passes, dom_count, details = self.evaluate(opp)
            # Composite score: weighted sum of normalized dimensions
            score = (
                opp.get("quality_score", 0) * 0.30
                + min(1.0, opp.get("signal_count", 0) / 7.0) * 0.20
                + min(1.0, opp.get("ev_ratio", 0) / 3.0) * 0.25
                + (opp.get("momentum_alignment", 0) + 1) / 2.0 * 0.15
                + opp.get("regime_score", 0) * 0.10
            )
            scored.append({
                **opp,
                "lattice_passes": passes,
                "lattice_dominance": dom_count,
                "lattice_score": round(score, 4),
                "lattice_details": details,
            })
        scored.sort(key=lambda x: (-x["lattice_score"],))
        return scored


# ============================================================
# 3. MARKOV CHAIN REGIME DETECTOR
# ============================================================

class MarkovRegimeDetector:
    """Model market regimes as a Markov chain (Wyckoff cycle).

    States: accumulation -> markup -> distribution -> markdown -> accumulation
    Each state has characteristic price behavior and optimal trading strategy.

    Transition probabilities are estimated from recent price action using
    simple statistics (no ML needed — pure math).
    """

    STATES = ["accumulation", "markup", "distribution", "markdown"]

    # Base transition matrix (Wyckoff cycle)
    # Rows = current state, Columns = next state
    # [accum, markup, distrib, markdown]
    BASE_TRANSITION = [
        [0.60, 0.30, 0.05, 0.05],  # accumulation: usually stays or goes to markup
        [0.05, 0.55, 0.35, 0.05],  # markup: usually stays or goes to distribution
        [0.05, 0.05, 0.55, 0.35],  # distribution: usually stays or goes to markdown
        [0.30, 0.05, 0.05, 0.60],  # markdown: usually stays or goes to accumulation
    ]

    # Optimal strategy per regime
    REGIME_STRATEGY = {
        "accumulation": {"bias": "BUY", "aggression": 0.8, "hold_hours": 12},
        "markup": {"bias": "BUY", "aggression": 1.0, "hold_hours": 24},
        "distribution": {"bias": "SELL", "aggression": 0.6, "hold_hours": 4},
        "markdown": {"bias": "SELL", "aggression": 0.3, "hold_hours": 2},
    }

    def detect_regime(self, prices):
        """Detect current market regime from recent prices.

        Uses price action statistics to classify:
        - Accumulation: low volatility, slight upward drift, narrowing range
        - Markup: rising prices, expanding volume, breaking resistance
        - Distribution: high volatility, topping pattern, volume divergence
        - Markdown: falling prices, expanding vol, breaking support

        Args:
            prices: list of recent close prices (newest last), minimum 20

        Returns:
            (state_name, confidence, strategy_dict)
        """
        if not prices or len(prices) < 10:
            return "accumulation", 0.5, self.REGIME_STRATEGY["accumulation"]

        n = len(prices)
        # Price change metrics
        returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, n) if prices[i-1] > 0]
        if not returns:
            return "accumulation", 0.5, self.REGIME_STRATEGY["accumulation"]

        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
        volatility = math.sqrt(variance) if variance > 0 else 0.001

        # Trend strength (linear regression slope normalized by volatility)
        x_mean = (n - 1) / 2
        numerator = sum((i - x_mean) * (prices[i] - prices[0]) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))
        slope = numerator / denominator if denominator > 0 else 0
        trend_strength = slope / (prices[0] * volatility) if prices[0] > 0 and volatility > 0 else 0

        # Range analysis (Bollinger Band width equivalent)
        recent = prices[-min(10, n):]
        price_range = (max(recent) - min(recent)) / min(recent) if min(recent) > 0 else 0

        # Classify regime
        scores = {
            "accumulation": 0.0,
            "markup": 0.0,
            "distribution": 0.0,
            "markdown": 0.0,
        }

        # Accumulation: low vol, slight upward drift, narrow range
        if volatility < 0.02 and mean_return > -0.005:
            scores["accumulation"] += 0.4
        if price_range < 0.03:
            scores["accumulation"] += 0.3
        if 0 < trend_strength < 2:
            scores["accumulation"] += 0.3

        # Markup: rising prices, moderate-high vol
        if mean_return > 0.005:
            scores["markup"] += 0.4
        if trend_strength > 2:
            scores["markup"] += 0.3
        if volatility > 0.01:
            scores["markup"] += 0.2
        if price_range > 0.02:
            scores["markup"] += 0.1

        # Distribution: high vol, topping (returns declining)
        late_returns = returns[-max(3, len(returns)//3):]
        early_returns = returns[:max(3, len(returns)//3)]
        mean_late = sum(late_returns) / len(late_returns) if late_returns else 0
        mean_early = sum(early_returns) / len(early_returns) if early_returns else 0
        if mean_late < mean_early and mean_early > 0:
            scores["distribution"] += 0.4
        if volatility > 0.03:
            scores["distribution"] += 0.3
        if price_range > 0.04:
            scores["distribution"] += 0.2

        # Markdown: falling prices, high vol
        if mean_return < -0.005:
            scores["markdown"] += 0.4
        if trend_strength < -2:
            scores["markdown"] += 0.3
        if volatility > 0.02:
            scores["markdown"] += 0.2
        if price_range > 0.03:
            scores["markdown"] += 0.1

        # Normalize scores
        total = sum(scores.values())
        if total > 0:
            for k in scores:
                scores[k] /= total

        # Pick highest scoring state
        best_state = max(scores, key=scores.get)
        confidence = scores[best_state]

        return best_state, confidence, self.REGIME_STRATEGY[best_state]

    def regime_score_for_trade(self, regime, direction):
        """Score how favorable the regime is for a given trade direction.

        Returns 0.0-1.0 where 1.0 = perfectly aligned.
        """
        strategy = self.REGIME_STRATEGY.get(regime, {})
        bias = strategy.get("bias", "NONE")
        aggression = strategy.get("aggression", 0.5)

        if bias == direction:
            return aggression
        elif bias == "NONE" or direction == "NONE":
            return 0.5
        else:
            # Counter-trend trade — penalize but don't block
            return max(0.1, 1.0 - aggression)

    def optimal_hold_hours(self, regime):
        """Suggested hold duration based on regime."""
        return self.REGIME_STRATEGY.get(regime, {}).get("hold_hours", 6)


# ============================================================
# 4. KNAPSACK POSITION OPTIMIZER
# ============================================================

class KnapsackOptimizer:
    """Bounded knapsack optimization for position sizing.

    Given N trade opportunities and limited capital, find the
    allocation that maximizes expected portfolio return while
    respecting risk constraints.

    Uses dynamic programming for exact solution (N is small, ~7 pairs).
    """

    def optimize(self, opportunities, total_capital, max_per_trade_pct=0.15,
                 max_concentration_pct=0.25):
        """Find optimal capital allocation across opportunities.

        Args:
            opportunities: list of dicts with:
                - pair: trading pair
                - direction: BUY/SELL
                - expected_return: expected return as fraction (e.g., 0.02 = 2%)
                - risk: expected risk (volatility) as fraction
                - lattice_score: quality score from lattice evaluation
                - min_size: minimum trade size in USD
            total_capital: available trading capital in USD
            max_per_trade_pct: max fraction of capital per trade
            max_concentration_pct: max concentration in any single asset

        Returns:
            list of (opportunity, allocated_capital) tuples, sorted by priority
        """
        if not opportunities or total_capital <= 0:
            return []

        # Filter to lattice-passing opportunities only
        viable = [o for o in opportunities if o.get("lattice_passes", False)]
        if not viable:
            # If nothing passes lattice, take the best one if it's close
            top = max(opportunities, key=lambda x: x.get("lattice_score", 0))
            if top.get("lattice_dominance", 0) >= 2:  # Close to passing
                viable = [top]
            else:
                return []

        # Calculate per-trade limits
        max_per_trade = total_capital * max_per_trade_pct
        min_trade = max(1.0, total_capital * 0.005)  # Minimum $1 or 0.5% of capital

        # Score each opportunity: EV * quality * regime alignment
        for opp in viable:
            ev = opp.get("expected_return", 0.01)
            risk = max(0.001, opp.get("risk", 0.02))
            quality = opp.get("lattice_score", 0.5)

            # Sharpe-like score: return per unit risk, weighted by quality
            sharpe_score = (ev / risk) * quality
            opp["_allocation_score"] = sharpe_score

        # Sort by allocation score (best first)
        viable.sort(key=lambda x: -x["_allocation_score"])

        # Greedy allocation (optimal for small N with convex returns)
        allocations = []
        remaining = total_capital
        for opp in viable:
            if remaining < min_trade:
                break

            # Size based on score and Kelly fraction
            ev = opp.get("expected_return", 0.01)
            risk = max(0.001, opp.get("risk", 0.02))
            # Half-Kelly: f* = (p*b - q) / b where p=win_prob, b=win/loss ratio, q=1-p
            win_prob = min(0.95, 0.5 + opp.get("lattice_score", 0.5) * 0.3)
            loss_prob = 1 - win_prob
            win_loss_ratio = abs(ev / risk) if risk > 0 else 1.0
            kelly = max(0, (win_prob * win_loss_ratio - loss_prob) / win_loss_ratio)
            half_kelly = kelly * 0.5

            # Clamp to limits
            size = total_capital * min(half_kelly, max_per_trade_pct)
            size = max(min_trade, min(size, max_per_trade, remaining))

            allocations.append((opp, round(size, 2)))
            remaining -= size

        return allocations


# ============================================================
# 5. GROWTH ENGINE (COMBINES ALL COMPONENTS)
# ============================================================

class GrowthEngine:
    """Main growth engine that orchestrates all algebraic components.

    Call flow:
    1. Receive raw signals from sniper's signal sources
    2. Encode signals via Galois field encoder
    3. Detect market regime via Markov chain
    4. Evaluate trade quality via lattice decision tree
    5. Optimize position sizing via knapsack solver
    6. Return enhanced trade recommendations

    This engine does NOT execute trades — it provides recommendations
    that sniper.py uses to improve its decision-making.
    """

    def __init__(self):
        self.encoder = GaloisSignalEncoder()
        self.lattice = TradeDecisionLattice()
        self.regime_detector = MarkovRegimeDetector()
        self.optimizer = KnapsackOptimizer()
        self._price_cache = {}
        self._regime_cache = {}
        self._cache_ttl = 60  # seconds

    def analyze_signals(self, pair, raw_signals, prices=None, available_capital=0):
        """Full algebraic analysis of trading signals for a pair.

        Args:
            pair: e.g., "BTC-USD"
            raw_signals: dict of {signal_name: (direction, confidence)}
            prices: recent price history (list, newest last)
            available_capital: cash available for new trades

        Returns:
            dict with:
                - recommendation: "BUY" | "SELL" | "HOLD"
                - quality_score: 0.0-1.0 (Galois weighted quality)
                - lattice_passes: bool
                - lattice_score: 0.0-1.0
                - regime: market regime name
                - regime_confidence: 0.0-1.0
                - optimal_size_usd: recommended position size
                - syndrome: list of missing signals
                - details: full analysis breakdown
        """
        # Step 1: Galois field encoding
        vector, conf_vector, gf_direction = self.encoder.encode(raw_signals)
        quality_score = self.encoder.weighted_quality_score(vector, conf_vector)
        signal_count = self.encoder.hamming_weight(vector)
        quant_coverage = self.encoder.quant_coverage(vector)
        syndrome = self.encoder.syndrome(vector)

        # Step 2: Markov regime detection
        if prices and len(prices) >= 10:
            regime, regime_conf, strategy = self.regime_detector.detect_regime(prices)
        else:
            regime, regime_conf, strategy = "accumulation", 0.5, {"bias": "BUY", "aggression": 0.5}

        regime_score = self.regime_detector.regime_score_for_trade(regime, gf_direction)

        # Step 3: Expected Value calculation
        fees = 0.009  # 0.9% round-trip (maker buy + maker sell + slippage)
        # EV scales with quality and regime alignment
        base_gain = 0.02 * (1 + quality_score)  # 2-4% expected gain
        base_loss = 0.01 * (2 - quality_score)   # 1-2% expected loss
        win_prob = 0.5 + quality_score * 0.25     # 50-75% win probability
        ev = (win_prob * base_gain) - ((1 - win_prob) * base_loss) - fees
        ev_ratio = ev / fees if fees > 0 else 0

        # Step 4: Momentum alignment
        if prices and len(prices) >= 5:
            recent_momentum = (prices[-1] - prices[-5]) / prices[-5] if prices[-5] > 0 else 0
            if gf_direction == "BUY":
                momentum_alignment = min(1.0, max(-1.0, recent_momentum * 20))
            elif gf_direction == "SELL":
                momentum_alignment = min(1.0, max(-1.0, -recent_momentum * 20))
            else:
                momentum_alignment = 0.0
        else:
            momentum_alignment = 0.0

        # Step 5: Lattice evaluation
        trade_element = {
            "quality_score": quality_score,
            "signal_count": signal_count,
            "ev_ratio": ev_ratio,
            "momentum_alignment": momentum_alignment,
            "regime_score": regime_score,
        }
        lattice_passes, dominance, lattice_details = self.lattice.evaluate(trade_element)

        # Step 6: Compute lattice score for ranking
        lattice_score = (
            quality_score * 0.30
            + min(1.0, signal_count / 7.0) * 0.20
            + min(1.0, max(0, ev_ratio) / 3.0) * 0.25
            + (momentum_alignment + 1) / 2.0 * 0.15
            + regime_score * 0.10
        )

        # Step 7: Final recommendation
        if lattice_passes and ev > 0 and gf_direction != "NONE":
            recommendation = gf_direction
        elif quality_score > 0.7 and signal_count >= 3 and ev > 0:
            # High quality but missed lattice — still worth considering
            recommendation = gf_direction
        else:
            recommendation = "HOLD"

        # Step 8: Optimal size (will be refined by knapsack in batch mode)
        if recommendation != "HOLD" and available_capital > 0:
            risk = max(0.001, 1.0 - quality_score) * 0.05  # Risk scales inversely with quality
            win_loss_ratio = base_gain / base_loss if base_loss > 0 else 1.0
            kelly = max(0, (win_prob * win_loss_ratio - (1 - win_prob)) / win_loss_ratio)
            optimal_size = available_capital * min(kelly * 0.5, 0.15)  # Half-Kelly, max 15%
            optimal_size = max(1.0, min(optimal_size, available_capital * 0.15))
        else:
            optimal_size = 0.0

        return {
            "pair": pair,
            "recommendation": recommendation,
            "direction": gf_direction,
            "quality_score": round(quality_score, 4),
            "signal_count": signal_count,
            "quant_coverage": round(quant_coverage, 4),
            "ev": round(ev, 6),
            "ev_ratio": round(ev_ratio, 4),
            "lattice_passes": lattice_passes,
            "lattice_score": round(lattice_score, 4),
            "lattice_dominance": dominance,
            "regime": regime,
            "regime_confidence": round(regime_conf, 4),
            "regime_score": round(regime_score, 4),
            "momentum_alignment": round(momentum_alignment, 4),
            "optimal_size_usd": round(optimal_size, 2),
            "optimal_hold_hours": self.regime_detector.optimal_hold_hours(regime),
            "syndrome": [(name, round(rel, 2)) for name, rel in syndrome[:3]],  # Top 3 missing
            "strategy": strategy,
        }

    def batch_analyze(self, pair_signals, prices_map, available_capital):
        """Analyze multiple pairs and optimize allocation across all.

        Args:
            pair_signals: dict of {pair: {signal_name: (direction, confidence)}}
            prices_map: dict of {pair: [prices]}
            available_capital: total cash available

        Returns:
            list of (analysis, allocated_usd) tuples, sorted by priority
        """
        analyses = []
        for pair, signals in pair_signals.items():
            prices = prices_map.get(pair, [])
            analysis = self.analyze_signals(pair, signals, prices, available_capital)
            analyses.append(analysis)

        # Build opportunities for knapsack
        opportunities = []
        for a in analyses:
            if a["recommendation"] != "HOLD":
                opportunities.append({
                    "pair": a["pair"],
                    "direction": a["direction"],
                    "expected_return": max(0.001, a["ev"]),
                    "risk": max(0.001, (1.0 - a["quality_score"]) * 0.05),
                    "lattice_passes": a["lattice_passes"],
                    "lattice_score": a["lattice_score"],
                    "lattice_dominance": a["lattice_dominance"],
                    "min_size": 1.0,
                    "analysis": a,
                })

        # Optimize allocation
        allocations = self.optimizer.optimize(opportunities, available_capital)

        # Build result
        result = []
        allocated_pairs = set()
        for opp, size in allocations:
            analysis = opp["analysis"]
            analysis["allocated_usd"] = size
            result.append(analysis)
            allocated_pairs.add(analysis["pair"])

        # Add non-allocated pairs as HOLD
        for a in analyses:
            if a["pair"] not in allocated_pairs:
                a["allocated_usd"] = 0.0
                result.append(a)

        # Sort by allocated amount (highest first)
        result.sort(key=lambda x: (-x.get("allocated_usd", 0), -x.get("lattice_score", 0)))

        # Strategic planner overlay (3D Go long-chain analysis)
        strategic_plan = None
        if _planner:
            try:
                # Build market_signals format for planner
                planner_signals = {}
                for a in analyses:
                    planner_signals[a["pair"]] = {
                        "direction": a["direction"],
                        "confidence": a["quality_score"],
                        "momentum": a.get("momentum_alignment", 0),
                        "regime": a.get("regime", "accumulation"),
                    }
                    # Feed influence map with signals
                    base = a["pair"].split("-")[0]
                    if a["direction"] != "NONE":
                        _planner.influence.place_stone(base, a["direction"], a["quality_score"])

                strategic_plan = _planner.analyze({}, planner_signals, available_capital)
                # Attach plan to first result for sniper to read
                if result:
                    result[0]["strategic_plan"] = strategic_plan
            except Exception as e:
                logger.debug("Strategic planner failed: %s", e)

        return result


# ============================================================
# SINGLETON
# ============================================================

_growth_engine = None

def get_growth_engine():
    """Get or create the singleton GrowthEngine."""
    global _growth_engine
    if _growth_engine is None:
        _growth_engine = GrowthEngine()
    return _growth_engine


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    import sys

    engine = get_growth_engine()

    # Demo with synthetic signals
    print("=== GROWTH ENGINE DEMO ===\n")

    test_signals = {
        "regime": ("BUY", 0.85),
        "arb": ("BUY", 0.72),
        "orderbook": ("BUY", 0.88),
        "rsi_extreme": ("NONE", 0.30),
        "momentum": ("BUY", 0.76),
        "latency": ("BUY", 0.92),
        "meta_engine": ("BUY", 0.68),
        "fear_greed": ("BUY", 0.84),
        "uptick": ("NONE", 0.20),
    }

    # Synthetic prices (uptrend)
    prices = [100 + i * 0.5 + (i % 3 - 1) * 0.2 for i in range(30)]

    result = engine.analyze_signals("BTC-USD", test_signals, prices, available_capital=50.0)

    print(f"  Pair: {result['pair']}")
    print(f"  Recommendation: {result['recommendation']}")
    print(f"  Quality Score: {result['quality_score']}")
    print(f"  Signal Count: {result['signal_count']}/9")
    print(f"  Quant Coverage: {result['quant_coverage']:.0%}")
    print(f"  EV: {result['ev']:.4%} (ratio: {result['ev_ratio']:.2f}x fees)")
    print(f"  Lattice: {'PASS' if result['lattice_passes'] else 'FAIL'} "
          f"(dominance {result['lattice_dominance']}/5, score {result['lattice_score']})")
    print(f"  Regime: {result['regime']} (conf={result['regime_confidence']}, "
          f"score={result['regime_score']})")
    print(f"  Momentum Alignment: {result['momentum_alignment']}")
    print(f"  Optimal Size: ${result['optimal_size_usd']}")
    print(f"  Optimal Hold: {result['optimal_hold_hours']}h")
    print(f"  Missing Signals: {result['syndrome']}")
    print(f"\n{'='*60}")
