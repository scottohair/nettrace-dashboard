#!/usr/bin/env python3
"""Agent Goal Training Module — single source of truth for all decision-making.

Every trading agent imports this module. It encodes the three immutable rules,
game theory principles, and quantitative gates that govern all trades.

RULES (NEVER VIOLATE):
  1. NEVER lose money — capital preservation above all
  2. Always make money — every decision must have positive expected value
  3. Grow money faster — compound gains, reinvest 20-35% of profits

GAME THEORY:
  - BE A MAKER not a taker (limit orders: 0.4% fee vs 0.6%)
  - Information asymmetry: NetTrace latency data is our private edge
  - Evolutionary: fire losers, promote/clone winners
  - Nash Equilibrium: avoid crowded strategies, find non-equilibrium edges
  - Zero-sum awareness: our gain = someone else's loss, exploit info advantage
"""

import logging
import math
import os
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger("agent_goals")

# ──────────────────────────────────────────────────────────────────────────────
# The Three Rules — every agent decision must satisfy ALL three
# ──────────────────────────────────────────────────────────────────────────────

RULES = {
    1: "NEVER lose money — capital preservation above all",
    2: "Always make money — every decision must have positive expected value",
    3: "Grow money faster — compound gains, reinvest 20-35% of profits",
}

# ──────────────────────────────────────────────────────────────────────────────
# Trade execution preferences (game theory)
# ──────────────────────────────────────────────────────────────────────────────

MAKER_FEE = float(os.environ.get("MAKER_FEE_PCT", "0.006"))   # 0.6% at <$1K tier
TAKER_FEE = float(os.environ.get("TAKER_FEE_PCT", "0.012"))  # 1.2% at <$1K tier
PREFERRED_ORDER_TYPE = "limit"  # BE A MAKER

# Derivatives fee schedule (perpetual futures)
PERP_MAKER_FEE = 0.0000    # 0.00% promotional
PERP_TAKER_FEE = 0.0003    # 0.03% promotional
PREFERRED_PERP_ORDER_TYPE = "limit"  # 0% maker = free trades

# Leverage rules (conservative start)
MAX_LEVERAGE = 3.0
MIN_MARGIN_HEALTH = 2.0
LIQUIDATION_BUFFER_PCT = 50

# Minimum thresholds
MIN_CONFIDENCE = float(os.environ.get("GOAL_MIN_CONFIDENCE", "0.75"))
MIN_CONFIDENCE_PERP = float(os.environ.get("GOAL_MIN_CONFIDENCE_PERP", "0.60"))
MIN_CONFIRMING_SIGNALS = int(os.environ.get("GOAL_MIN_CONFIRMING_SIGNALS", "2"))
MIN_CONFIRMING_SIGNALS_PERP = int(os.environ.get("GOAL_MIN_CONFIRMING_SIGNALS_PERP", "1"))
DOWNTREND_BLOCKED = True     # Rule 1: no buying in downtrends

# Agent performance thresholds (evolutionary)
FIRE_MIN_TRADES = 50         # enough sample size before judging
FIRE_MAX_SHARPE = 0.5        # below this = underperforming
FIRE_MAX_DRAWDOWN = 0.10     # 10% drawdown = fired
PROMOTE_MIN_SHARPE = 1.0     # good enough to promote
CLONE_MIN_SHARPE = 2.0       # exceptional = clone it
PROMOTE_MIN_WIN_RATE = 0.55  # must win more than lose


class GoalValidator:
    """Gate every agent decision through the encoded goals."""

    @staticmethod
    def should_trade(confidence, confirming_signals, direction, market_regime):
        """Determine if a trade should be executed.

        Args:
            confidence: float 0-1, composite signal confidence
            confirming_signals: int, number of independent signals agreeing
            direction: str, "BUY" or "SELL"
            market_regime: str, one of "uptrend", "neutral", "downtrend", "volatile"

        Returns:
            bool: True if trade passes all goal checks
        """
        try:
            conf = float(confidence)
        except (TypeError, ValueError):
            logger.warning("BLOCKED: non-numeric confidence=%r", confidence)
            return False

        if not math.isfinite(conf):
            logger.warning("BLOCKED: non-finite confidence=%r", confidence)
            return False

        try:
            signals = int(confirming_signals)
        except (TypeError, ValueError):
            logger.warning("BLOCKED: invalid confirming_signals=%r", confirming_signals)
            return False

        direction_u = str(direction or "").strip().upper()
        if direction_u not in {"BUY", "SELL"}:
            logger.warning("BLOCKED: invalid direction=%r", direction)
            return False

        regime_n = str(market_regime or "").strip().lower()
        regime_tags = {
            "downtrend",
            "markdown",
            "bearish",
            "down",
        }

        # Rule 1: minimum confidence + confirmation
        if conf < MIN_CONFIDENCE:
            logger.debug("BLOCKED: confidence %.2f < %.2f minimum",
                         conf, MIN_CONFIDENCE)
            return False

        if signals < MIN_CONFIRMING_SIGNALS:
            logger.debug("BLOCKED: %d confirming signals < %d minimum",
                         signals, MIN_CONFIRMING_SIGNALS)
            return False

        # Rule 1: no buying in downtrends
        if DOWNTREND_BLOCKED and direction_u == "BUY" and regime_n in regime_tags:
            logger.debug("BLOCKED: BUY in downtrend regime (Rule #1)")
            return False

        # Rule 2: positive expected value check
        # EV = (confidence * avg_win) - ((1 - confidence) * avg_loss)
        # With maker fees, need confidence > fee_drag to be positive EV
        fee_drag = MAKER_FEE  # best case (limit order)
        if conf <= fee_drag:
            logger.debug("BLOCKED: confidence %.2f <= fee drag %.3f (negative EV)",
                         conf, fee_drag)
            return False

        return True

    @staticmethod
    def should_trade_perp(confidence, confirming_signals, direction, market_regime,
                          leverage=1.0, margin_health=None):
        """Determine if a perpetual futures trade should be executed.

        Wraps should_trade() with additional leverage and margin checks.
        Higher leverage requires higher confidence (scaled linearly).

        Args:
            confidence: float 0-1, composite signal confidence
            confirming_signals: int, number of independent signals agreeing
            direction: str, "BUY" or "SELL"
            market_regime: str, one of "uptrend", "neutral", "downtrend", "volatile"
            leverage: float, requested leverage (1.0-MAX_LEVERAGE)
            margin_health: float or None, current margin health ratio

        Returns:
            bool: True if trade passes all goal checks including perp-specific gates
        """
        # Leverage cap
        try:
            lev = float(leverage)
        except (TypeError, ValueError):
            lev = 1.0
        if lev > MAX_LEVERAGE:
            logger.debug("BLOCKED: leverage %.1fx > %.1fx cap", lev, MAX_LEVERAGE)
            return False

        # Margin health gate
        if margin_health is not None:
            try:
                mh = float(margin_health)
            except (TypeError, ValueError):
                logger.debug("BLOCKED: invalid margin_health=%r", margin_health)
                return False
            if mh < MIN_MARGIN_HEALTH:
                logger.debug("BLOCKED: margin health %.2f < %.2f minimum", mh, MIN_MARGIN_HEALTH)
                return False

        # Scaled confidence: higher leverage demands higher confidence
        # At 1x: perp confidence threshold. At 3x: confidence must be ~25% higher.
        try:
            conf = float(confidence)
        except (TypeError, ValueError):
            return False
        leverage_confidence_penalty = (lev - 1.0) * 0.10  # 10% per 1x above 1
        adjusted_min = MIN_CONFIDENCE_PERP + leverage_confidence_penalty
        if conf < adjusted_min:
            logger.debug("BLOCKED: perp confidence %.2f < %.2f (leverage-adjusted minimum)",
                         conf, adjusted_min)
            return False

        # Perp-specific validation (uses perp fee floor, not spot MIN_CONFIDENCE)
        try:
            signals = int(confirming_signals)
        except (TypeError, ValueError):
            return False

        if signals < MIN_CONFIRMING_SIGNALS_PERP:
            logger.debug("BLOCKED: perp %d confirming signals < %d minimum",
                         signals, MIN_CONFIRMING_SIGNALS_PERP)
            return False

        direction_u = str(direction or "").strip().upper()
        if direction_u not in {"BUY", "SELL"}:
            return False

        regime_n = str(market_regime or "").strip().lower()
        if DOWNTREND_BLOCKED and direction_u == "BUY" and regime_n in {"downtrend", "markdown", "bearish", "down"}:
            logger.debug("BLOCKED: perp BUY in downtrend regime")
            return False

        # EV check with perp fee (0% maker vs 0.6% spot)
        fee_drag = PERP_MAKER_FEE
        if conf <= fee_drag:
            logger.debug("BLOCKED: perp confidence %.2f <= perp fee drag %.4f", conf, fee_drag)
            return False

        return True

    @staticmethod
    def optimal_venue(product_has_perp):
        """Determine optimal venue (perp vs spot) based on fee savings.

        Args:
            product_has_perp: bool, whether a perp product exists for this asset

        Returns:
            dict with venue ("perp" or "spot"), maker_fee, taker_fee, reason
        """
        if product_has_perp:
            return {
                "venue": "perp",
                "maker_fee": PERP_MAKER_FEE,
                "taker_fee": PERP_TAKER_FEE,
                "reason": f"Perp preferred: {PERP_MAKER_FEE:.2%} maker vs {MAKER_FEE:.2%} spot",
            }
        return {
            "venue": "spot",
            "maker_fee": MAKER_FEE,
            "taker_fee": TAKER_FEE,
            "reason": "No perp available, using spot",
        }

    @staticmethod
    def expected_value(confidence, potential_gain, potential_loss):
        """Calculate expected value of a trade.

        Args:
            confidence: probability of winning (0-1)
            potential_gain: USD gain if trade wins (before fees)
            potential_loss: USD loss if trade loses (before fees)

        Returns:
            float: expected value in USD (negative = don't trade)
        """
        gain_after_fees = potential_gain * (1 - MAKER_FEE)
        loss_with_fees = potential_loss + (potential_loss * MAKER_FEE)
        ev = (confidence * gain_after_fees) - ((1 - confidence) * loss_with_fees)
        return round(ev, 4)

    @staticmethod
    def kelly_fraction(confidence, win_loss_ratio=1.5):
        """Calculate Kelly Criterion optimal bet fraction.

        Rule 3: size proportional to edge, never overbet.

        Args:
            confidence: win probability (0-1)
            win_loss_ratio: average win / average loss

        Returns:
            float: fraction of bankroll to risk (0.0 to 0.25 capped)
        """
        if confidence <= 0 or win_loss_ratio <= 0:
            return 0.0

        # Kelly formula: f* = (bp - q) / b
        # b = win/loss ratio, p = win probability, q = loss probability
        b = win_loss_ratio
        p = confidence
        q = 1 - p

        kelly = (b * p - q) / b
        if kelly <= 0:
            return 0.0

        # Half-Kelly for safety (Rule 1), cap at 25%
        half_kelly = kelly / 2
        return round(min(half_kelly, 0.25), 4)

    @staticmethod
    def optimal_order_type(urgency="normal"):
        """BE A MAKER: always prefer limit orders.

        Args:
            urgency: "normal" (limit), "high" (limit aggressive), "critical" (market)

        Returns:
            dict with order_type and fee
        """
        if urgency == "critical":
            return {"order_type": "market", "fee": TAKER_FEE,
                    "reason": "Critical urgency overrides maker preference"}

        return {"order_type": "limit", "fee": MAKER_FEE,
                "reason": "BE A MAKER — 0.4% fee vs 0.6% taker"}

    @staticmethod
    def should_fire_agent(sharpe, trades, win_rate, drawdown):
        """Evolutionary: determine if an agent should be fired.

        Args:
            sharpe: Sharpe ratio of the agent
            trades: total number of trades completed
            win_rate: fraction of winning trades (0-1)
            drawdown: maximum drawdown fraction (0-1)

        Returns:
            dict with fired (bool), reason (str)
        """
        if trades < FIRE_MIN_TRADES:
            return {"fired": False, "reason": f"Insufficient trades ({trades}/{FIRE_MIN_TRADES})"}

        if drawdown > FIRE_MAX_DRAWDOWN:
            return {"fired": True,
                    "reason": f"Drawdown {drawdown:.1%} > {FIRE_MAX_DRAWDOWN:.1%} limit"}

        if sharpe < FIRE_MAX_SHARPE:
            return {"fired": True,
                    "reason": f"Sharpe {sharpe:.2f} < {FIRE_MAX_SHARPE:.2f} minimum"}

        if win_rate < 0.40 and trades >= FIRE_MIN_TRADES:
            return {"fired": True,
                    "reason": f"Win rate {win_rate:.1%} < 40% with {trades} trades"}

        return {"fired": False, "reason": "Performance acceptable"}

    @staticmethod
    def should_promote_agent(sharpe, trades, win_rate):
        """Determine if an agent deserves more capital allocation.

        Returns:
            dict with promoted (bool), clone (bool), reason (str)
        """
        if trades < 10:
            return {"promoted": False, "clone": False,
                    "reason": f"Insufficient trades ({trades})"}

        if sharpe >= CLONE_MIN_SHARPE and win_rate >= PROMOTE_MIN_WIN_RATE:
            return {"promoted": True, "clone": True,
                    "reason": f"Exceptional: Sharpe {sharpe:.2f}, WR {win_rate:.1%} — CLONE"}

        if sharpe >= PROMOTE_MIN_SHARPE and win_rate >= PROMOTE_MIN_WIN_RATE:
            return {"promoted": True, "clone": False,
                    "reason": f"Solid: Sharpe {sharpe:.2f}, WR {win_rate:.1%}"}

        return {"promoted": False, "clone": False, "reason": "Not yet meeting promotion bar"}

    @staticmethod
    def allocation_weight(agent_performances):
        """Multi-agent capital allocation proportional to performance.

        Game theory: agents compete for capital. Better agents get more.

        Args:
            agent_performances: list of dicts with keys:
                name, sharpe, win_rate, pnl, trades

        Returns:
            dict of agent_name -> weight (0-1, sums to 1.0)
        """
        if not agent_performances:
            return {}

        # Score = Sharpe * win_rate * log(1 + trades)
        # This rewards consistency (Sharpe), accuracy (win_rate), and experience (trades)
        scores = {}
        for ap in agent_performances:
            name = ap["name"]
            sharpe = max(ap.get("sharpe", 0), 0)  # floor at 0
            wr = max(ap.get("win_rate", 0.5), 0.1)
            trades = max(ap.get("trades", 0), 1)

            scores[name] = sharpe * wr * math.log(1 + trades)

        total = sum(scores.values())
        if total <= 0:
            # Equal allocation if no one has proven themselves
            n = len(agent_performances)
            return {ap["name"]: round(1.0 / n, 4) for ap in agent_performances}

        return {name: round(score / total, 4) for name, score in scores.items()}

    @staticmethod
    def reinvestment_rate(portfolio_value, recent_pnl):
        """Rule 3: how much of recent profits to reinvest (20-35%).

        Higher reinvestment when portfolio is small (growth phase),
        lower when large (preservation phase).

        Args:
            portfolio_value: current total portfolio USD
            recent_pnl: recent period P&L in USD

        Returns:
            float: reinvestment fraction (0.20 to 0.35)
        """
        if recent_pnl <= 0:
            return 0.0  # don't reinvest losses

        # Scale: small portfolio = 35%, large = 20%
        # Transition around $10k-$100k
        if portfolio_value < 1000:
            rate = 0.35
        elif portfolio_value < 10000:
            rate = 0.30
        elif portfolio_value < 100000:
            rate = 0.25
        else:
            rate = 0.20

        return rate

    # NYSE holidays for 2026 (month, day) — update annually
    NYSE_HOLIDAYS_2026 = [
        (1, 1),    # New Year's Day
        (1, 19),   # MLK Day
        (2, 16),   # Presidents' Day
        (4, 3),    # Good Friday
        (5, 25),   # Memorial Day
        (6, 19),   # Juneteenth
        (7, 3),    # Independence Day (observed)
        (9, 7),    # Labor Day
        (11, 26),  # Thanksgiving
        (12, 25),  # Christmas
    ]

    @staticmethod
    def is_equity_market_open():
        """Check if US equity markets are currently open.

        NYSE/NASDAQ hours: 9:30 AM - 4:00 PM Eastern, Mon-Fri.
        Excludes NYSE holidays.

        Returns:
            bool: True if market is open now.
        """
        try:
            et_now = datetime.now(ZoneInfo("America/New_York"))
        except Exception:
            logger.warning("Could not determine Eastern time, assuming market closed")
            return False

        # Weekend check
        if et_now.weekday() >= 5:
            return False

        # Holiday check
        if (et_now.month, et_now.day) in GoalValidator.NYSE_HOLIDAYS_2026:
            return False

        # Market hours: 9:30 AM - 4:00 PM ET
        market_open = et_now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = et_now.replace(hour=16, minute=0, second=0, microsecond=0)

        return market_open <= et_now <= market_close

    @staticmethod
    def should_trade_equity(confidence, confirming_signals, direction, market_regime):
        """Determine if an equity trade should be executed.

        Wraps should_trade() with equity market hours gate.
        Equities are commission-free on E*Trade so fee drag is 0,
        but we still use the standard confidence/signal gates.

        Args:
            confidence: float 0-1, composite signal confidence
            confirming_signals: int, number of independent signals agreeing
            direction: str, "BUY" or "SELL"
            market_regime: str, one of "uptrend", "neutral", "downtrend", "volatile"

        Returns:
            bool: True if trade passes all goal checks including market hours
        """
        if not GoalValidator.is_equity_market_open():
            logger.debug("BLOCKED: equity market closed")
            return False

        return GoalValidator.should_trade(confidence, confirming_signals, direction, market_regime)
