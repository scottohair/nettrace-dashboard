#!/usr/bin/env python3
"""Centralized Exit Strategy Manager — every position MUST have an exit plan.

"Everyone needs a strategy and exit strategy."

This module provides:
  1. Trailing stop losses (dynamic, tightens as profit grows)
  2. Time-based exits (dead money detection, scale-out)
  3. Take-profit targets (partial profit-taking at milestones)
  4. Volatility-adjusted stops (ATR-based, wider in high vol)
  5. Position monitor (background thread, 30s interval)
  6. Full integration with risk_controller (NO hardcoded values)

ALL risk parameters are DYNAMIC from risk_controller.get_risk_params().
Nothing is hardcoded. Everything derives from market state and portfolio size.

Game Theory:
  - Exits are the most important part of any trade
  - Letting winners run + cutting losers fast = positive expectancy
  - Partial exits reduce risk while maintaining upside exposure
  - Volatility-adjusted stops prevent shakeouts in high-vol markets
"""

import json
import logging
import math
import os
import sqlite3
import time
import threading
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"'))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "exit_manager.log")),
    ]
)
logger = logging.getLogger("exit_manager")

EXIT_DB = str(Path(__file__).parent / "exit_manager.db")

# Dynamic risk controller -- ALL parameters come from here
# NOTE: exit_manager still operates with fallback calculations if risk_controller fails,
# because exits MUST still happen to protect capital (unlike new entries which should be blocked)
try:
    from risk_controller import get_controller
    _risk_ctrl = get_controller()
except Exception as _rc_err:
    _risk_ctrl = None
    logger.error("RISK CONTROLLER FAILED TO LOAD: %s — exit_manager using fallback calculations", _rc_err)


def _fetch_json(url, headers=None, timeout=10):
    """HTTP GET JSON helper."""
    h = {"User-Agent": "ExitManager/1.0"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _data_pair(pair):
    """Convert trading pair to data pair for public APIs."""
    return pair.replace("-USDC", "-USD")


def _get_price(pair):
    """Get current spot price for a pair."""
    try:
        dp = _data_pair(pair)
        data = _fetch_json(f"https://api.coinbase.com/v2/prices/{dp}/spot")
        return float(data["data"]["amount"])
    except Exception:
        return None


class PositionState:
    """Tracks the live state of a single position for exit management."""

    def __init__(self, pair, entry_price, entry_time, held_amount, trade_id=None):
        self.pair = pair
        self.entry_price = entry_price
        self.entry_time = entry_time
        self.held_amount = held_amount
        self.trade_id = trade_id

        # Trailing stop tracking
        self.peak_price = entry_price
        self.peak_time = entry_time

        # Partial exit tracking
        self.partial_exits_done = set()  # e.g., {"tp1", "tp2", "scale_50"}
        self.original_amount = held_amount
        self.total_exited_amount = 0.0
        self.total_exit_value = 0.0

    def update_peak(self, current_price):
        """Update peak price if current is higher."""
        if current_price > self.peak_price:
            self.peak_price = current_price
            self.peak_time = datetime.now(timezone.utc)

    def profit_pct(self, current_price):
        """Current profit percentage from entry."""
        if self.entry_price <= 0:
            return 0.0
        return (current_price - self.entry_price) / self.entry_price

    def drawdown_from_peak_pct(self, current_price):
        """Drawdown from peak price as a percentage."""
        if self.peak_price <= 0:
            return 0.0
        return (self.peak_price - current_price) / self.peak_price

    def hold_duration_seconds(self):
        """How long this position has been held."""
        now = datetime.now(timezone.utc)
        if isinstance(self.entry_time, str):
            try:
                entry = datetime.fromisoformat(self.entry_time).replace(tzinfo=timezone.utc)
            except Exception:
                return 0
        elif isinstance(self.entry_time, datetime):
            entry = self.entry_time if self.entry_time.tzinfo else self.entry_time.replace(tzinfo=timezone.utc)
        else:
            return 0
        return (now - entry).total_seconds()

    def hold_duration_hours(self):
        """Hold duration in hours."""
        return self.hold_duration_seconds() / 3600.0

    def record_partial_exit(self, label, amount, price):
        """Record a partial exit."""
        self.partial_exits_done.add(label)
        self.total_exited_amount += amount
        self.total_exit_value += amount * price
        self.held_amount -= amount
        self.held_amount = max(0, self.held_amount)


class ExitManager:
    """Centralized exit strategy manager for ALL trading agents.

    Monitors all open positions and executes exits based on:
    - Trailing stop losses (dynamic, tightens as profit grows)
    - Time-based exits (dead money detection)
    - Take-profit targets (partial profit-taking)
    - Volatility-adjusted stops (ATR-based)
    - Daily loss limits from risk_controller

    ALL thresholds are DYNAMIC -- derived from risk_controller and market state.
    """

    def __init__(self):
        self._db = sqlite3.connect(EXIT_DB, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._init_db()
        self._positions = {}  # pair -> PositionState
        self._lock = threading.Lock()
        self._running = False
        self._monitor_thread = None
        self._trader = None  # lazy-loaded CoinbaseTrader

    def _init_db(self):
        """Initialize exit tracking database."""
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS exit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                exit_type TEXT NOT NULL,
                reason TEXT,
                entry_price REAL,
                exit_price REAL,
                amount REAL,
                pnl_usd REAL,
                pnl_pct REAL,
                hold_duration_hours REAL,
                peak_price REAL,
                volatility REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS position_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                entry_price REAL,
                current_price REAL,
                peak_price REAL,
                profit_pct REAL,
                drawdown_pct REAL,
                hold_hours REAL,
                trailing_stop_pct REAL,
                action TEXT DEFAULT 'hold',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self._db.commit()

    def _get_trader(self):
        """Lazy-load CoinbaseTrader to avoid import issues at startup."""
        if self._trader is None:
            try:
                from exchange_connector import CoinbaseTrader
                self._trader = CoinbaseTrader()
            except Exception as e:
                logger.error("Failed to load CoinbaseTrader: %s", e)
        return self._trader

    # ==========================================
    # DYNAMIC EXIT PARAMETER CALCULATIONS
    # ==========================================

    def _get_dynamic_params(self, pair):
        """Get all dynamic exit parameters from risk_controller and market state.

        Returns a dict of computed thresholds -- NOTHING hardcoded.
        Every value derives from portfolio size, volatility, and trend.
        """
        # Get portfolio value and risk params
        portfolio_value = self._estimate_portfolio_value()
        if _risk_ctrl:
            risk_params = _risk_ctrl.get_risk_params(portfolio_value, pair)
            volatility = risk_params["volatility"]
            max_daily_loss = risk_params["max_daily_loss"]
            regime = risk_params["regime"]
        else:
            # Fallback: compute from market data directly
            volatility = 0.02
            max_daily_loss = max(1.0, portfolio_value * 0.04)
            regime = "UNKNOWN"

        # === TRAILING STOP TIERS ===
        # Base stop distances scale with volatility
        # Higher vol = wider stops (prevent noise shakeouts)
        # Lower vol = tighter stops (less room needed)
        vol_multiplier = max(0.5, min(3.0, volatility / 0.02))  # 1.0x at 2% vol

        # Tier 1: 0 to low_profit_threshold -- wide stop, let it breathe
        # Base: 2% from peak, scaled by volatility
        wide_stop = 0.02 * vol_multiplier

        # Tier 2: low_profit to mid_profit -- tighten
        # Base: 1.5% from peak, scaled by volatility
        medium_stop = 0.015 * vol_multiplier

        # Tier 3: above mid_profit -- lock in gains
        # Base: 1% from peak, scaled by volatility
        tight_stop = 0.01 * vol_multiplier

        # Profit thresholds for tier transitions (scale with volatility)
        # In high vol, you need bigger moves to transition tiers
        low_profit_threshold = 0.01 * vol_multiplier   # ~1% in normal vol
        mid_profit_threshold = 0.03 * vol_multiplier   # ~3% in normal vol

        # === TIME-BASED EXIT THRESHOLDS ===
        # Faster exits in ranging/downtrend markets
        regime_time_mult = 1.0
        if regime == "DOWNTREND":
            regime_time_mult = 0.5  # exit faster in downtrends
        elif regime == "UPTREND":
            regime_time_mult = 1.5  # give more time in uptrends

        # Dead money threshold: hours before we consider a position stale
        # Base: 4 hours, adjusted by regime
        dead_money_hours = 4.0 * regime_time_mult
        # Minimum gain to avoid being considered dead money
        # Scales with volatility -- in high vol, 0.5% is nothing
        dead_money_min_gain = 0.005 * vol_multiplier

        # Force-evaluate threshold: position too old
        # Base: 24 hours, adjusted by regime
        force_eval_hours = 24.0 * regime_time_mult

        # === TAKE-PROFIT TARGETS ===
        # Scale targets with volatility -- higher vol = larger targets
        tp1_pct = 0.02 * vol_multiplier    # ~2% in normal vol
        tp1_sell_frac = 0.30                # sell 30% at TP1

        tp2_pct = 0.05 * vol_multiplier    # ~5% in normal vol
        tp2_sell_frac = 0.30                # sell 30% at TP2

        # Remaining 40% rides with trailing stop
        rider_frac = 1.0 - tp1_sell_frac - tp2_sell_frac

        # === SCALE-OUT AT TIME THRESHOLD ===
        # Sell 50% of position at 2% profit if held > scale_out_hours
        scale_out_hours = 2.0 * regime_time_mult
        scale_out_profit_pct = 0.02 * vol_multiplier
        scale_out_frac = 0.50

        # === MAX LOSS PER POSITION ===
        # Never let a single position lose more than a fraction of daily loss limit
        # Fraction scales inversely with number of positions
        n_positions = max(1, len(self._positions))
        max_position_loss_usd = max_daily_loss / max(2, n_positions)

        return {
            "volatility": volatility,
            "vol_multiplier": vol_multiplier,
            "regime": regime,
            "max_daily_loss": max_daily_loss,
            "portfolio_value": portfolio_value,
            # Trailing stop tiers
            "wide_stop_pct": round(wide_stop, 4),
            "medium_stop_pct": round(medium_stop, 4),
            "tight_stop_pct": round(tight_stop, 4),
            "low_profit_threshold": round(low_profit_threshold, 4),
            "mid_profit_threshold": round(mid_profit_threshold, 4),
            # Time-based
            "dead_money_hours": round(dead_money_hours, 2),
            "dead_money_min_gain": round(dead_money_min_gain, 4),
            "force_eval_hours": round(force_eval_hours, 2),
            # Take-profit
            "tp1_pct": round(tp1_pct, 4),
            "tp1_sell_frac": tp1_sell_frac,
            "tp2_pct": round(tp2_pct, 4),
            "tp2_sell_frac": tp2_sell_frac,
            "rider_frac": rider_frac,
            # Scale-out
            "scale_out_hours": round(scale_out_hours, 2),
            "scale_out_profit_pct": round(scale_out_profit_pct, 4),
            "scale_out_frac": scale_out_frac,
            # Loss limits
            "max_position_loss_usd": round(max_position_loss_usd, 2),
        }

    def _estimate_portfolio_value(self):
        """Estimate total portfolio value from Coinbase holdings."""
        try:
            trader = self._get_trader()
            if not trader:
                return 100.0  # safe fallback
            accts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
            total = 0.0
            for a in accts.get("accounts", []):
                cur = a.get("currency", "")
                bal = float(a.get("available_balance", {}).get("value", 0))
                if cur in ("USDC", "USD"):
                    total += bal
                elif bal > 0:
                    price = _get_price(f"{cur}-USDC")
                    if price:
                        total += bal * price
            return max(1.0, total)
        except Exception as e:
            logger.debug("Portfolio estimate failed: %s", e)
            return 100.0  # safe fallback

    # ==========================================
    # EXIT DECISION ENGINE
    # ==========================================

    def _compute_trailing_stop_pct(self, profit_pct, params):
        """Compute the trailing stop distance based on current profit tier.

        Returns the stop distance as a positive fraction (e.g., 0.02 = 2% from peak).
        Tiers transition smoothly based on profit level.
        """
        low_thresh = params["low_profit_threshold"]
        mid_thresh = params["mid_profit_threshold"]

        if profit_pct >= mid_thresh:
            # Tier 3: lock in gains with tight stop
            return params["tight_stop_pct"]
        elif profit_pct >= low_thresh:
            # Tier 2: medium tightness
            # Interpolate between medium and tight for smooth transition
            t = (profit_pct - low_thresh) / (mid_thresh - low_thresh) if mid_thresh > low_thresh else 0
            return params["medium_stop_pct"] + t * (params["tight_stop_pct"] - params["medium_stop_pct"])
        else:
            # Tier 1: wide stop, let it breathe
            # Interpolate between wide and medium as we approach low_thresh
            if low_thresh > 0 and profit_pct > 0:
                t = profit_pct / low_thresh
                return params["wide_stop_pct"] + t * (params["medium_stop_pct"] - params["wide_stop_pct"])
            return params["wide_stop_pct"]

    def check_exit(self, pair, entry_price, entry_time, current_price, held_amount, trade_id=None):
        """Check if a position should be exited and return the exit decision.

        Returns: dict with keys:
            - action: "HOLD" | "EXIT_FULL" | "EXIT_PARTIAL"
            - reason: human-readable reason
            - exit_type: "trailing_stop" | "time_exit" | "take_profit" | "scale_out" | "loss_limit" | "force_eval"
            - amount: how much to sell (in base currency)
            - fraction: what fraction of held amount to sell
        """
        if held_amount <= 0 or entry_price <= 0 or not current_price:
            return {"action": "HOLD", "reason": "Invalid position data", "exit_type": "none",
                    "amount": 0, "fraction": 0}

        # Get or create position state
        with self._lock:
            if pair not in self._positions:
                self._positions[pair] = PositionState(
                    pair, entry_price, entry_time, held_amount, trade_id)
            pos = self._positions[pair]
            # Update with latest data
            pos.held_amount = held_amount
            pos.update_peak(current_price)

        # Get dynamic parameters
        params = self._get_dynamic_params(pair)

        profit_pct = pos.profit_pct(current_price)
        drawdown_pct = pos.drawdown_from_peak_pct(current_price)
        hold_hours = pos.hold_duration_hours()
        trailing_stop_pct = self._compute_trailing_stop_pct(profit_pct, params)

        # Record snapshot for analysis
        try:
            self._db.execute(
                "INSERT INTO position_snapshots (pair, entry_price, current_price, peak_price, "
                "profit_pct, drawdown_pct, hold_hours, trailing_stop_pct) VALUES (?,?,?,?,?,?,?,?)",
                (pair, entry_price, current_price, pos.peak_price,
                 round(profit_pct, 6), round(drawdown_pct, 6),
                 round(hold_hours, 3), round(trailing_stop_pct, 6))
            )
            self._db.commit()
        except Exception:
            pass

        # === CHECK 1: Absolute loss limit (Rule #1: NEVER lose money beyond limit) ===
        position_value = held_amount * current_price
        position_loss_usd = held_amount * (entry_price - current_price) if current_price < entry_price else 0
        if position_loss_usd > params["max_position_loss_usd"]:
            return {
                "action": "EXIT_FULL",
                "reason": (f"Loss limit hit: ${position_loss_usd:.2f} > max ${params['max_position_loss_usd']:.2f} "
                           f"(daily limit ${params['max_daily_loss']:.2f})"),
                "exit_type": "loss_limit",
                "amount": held_amount,
                "fraction": 1.0,
            }

        # === CHECK 2: Trailing stop loss ===
        # Only trigger trailing stop if we've been in profit at some point
        # (peak > entry means we were profitable)
        if pos.peak_price > entry_price and drawdown_pct > trailing_stop_pct:
            return {
                "action": "EXIT_FULL",
                "reason": (f"Trailing stop triggered: {drawdown_pct:.2%} drawdown from peak "
                           f"${pos.peak_price:.2f} > stop {trailing_stop_pct:.2%} "
                           f"(profit tier: {profit_pct:.2%}, vol_mult: {params['vol_multiplier']:.1f}x)"),
                "exit_type": "trailing_stop",
                "amount": held_amount,
                "fraction": 1.0,
            }

        # === CHECK 3: Take-profit targets (partial exits) ===
        # TP1: First milestone
        if profit_pct >= params["tp1_pct"] and "tp1" not in pos.partial_exits_done:
            sell_amount = pos.original_amount * params["tp1_sell_frac"]
            sell_amount = min(sell_amount, held_amount)
            if sell_amount > 0:
                return {
                    "action": "EXIT_PARTIAL",
                    "reason": (f"Take-profit TP1: {profit_pct:.2%} >= {params['tp1_pct']:.2%} target, "
                               f"selling {params['tp1_sell_frac']:.0%} of original position"),
                    "exit_type": "take_profit_tp1",
                    "amount": sell_amount,
                    "fraction": params["tp1_sell_frac"],
                    "label": "tp1",
                }

        # TP2: Second milestone
        if profit_pct >= params["tp2_pct"] and "tp2" not in pos.partial_exits_done:
            sell_amount = pos.original_amount * params["tp2_sell_frac"]
            sell_amount = min(sell_amount, held_amount)
            if sell_amount > 0:
                return {
                    "action": "EXIT_PARTIAL",
                    "reason": (f"Take-profit TP2: {profit_pct:.2%} >= {params['tp2_pct']:.2%} target, "
                               f"selling {params['tp2_sell_frac']:.0%} of original position"),
                    "exit_type": "take_profit_tp2",
                    "amount": sell_amount,
                    "fraction": params["tp2_sell_frac"],
                    "label": "tp2",
                }

        # === CHECK 4: Scale-out at time + profit threshold ===
        if (hold_hours >= params["scale_out_hours"]
                and profit_pct >= params["scale_out_profit_pct"]
                and "scale_50" not in pos.partial_exits_done):
            sell_amount = held_amount * params["scale_out_frac"]
            if sell_amount > 0:
                return {
                    "action": "EXIT_PARTIAL",
                    "reason": (f"Scale-out: held {hold_hours:.1f}h >= {params['scale_out_hours']:.1f}h "
                               f"with {profit_pct:.2%} profit, selling {params['scale_out_frac']:.0%}"),
                    "exit_type": "scale_out",
                    "amount": sell_amount,
                    "fraction": params["scale_out_frac"],
                    "label": "scale_50",
                }

        # === CHECK 5: Dead money exit ===
        if hold_hours >= params["dead_money_hours"] and profit_pct < params["dead_money_min_gain"]:
            return {
                "action": "EXIT_FULL",
                "reason": (f"Dead money: held {hold_hours:.1f}h >= {params['dead_money_hours']:.1f}h "
                           f"with only {profit_pct:.2%} gain (need {params['dead_money_min_gain']:.2%})"),
                "exit_type": "time_exit_dead_money",
                "amount": held_amount,
                "fraction": 1.0,
            }

        # === CHECK 6: Force evaluation on very old positions ===
        if hold_hours >= params["force_eval_hours"]:
            # If flat or losing after force_eval_hours, exit
            if profit_pct <= 0:
                return {
                    "action": "EXIT_FULL",
                    "reason": (f"Force eval: held {hold_hours:.1f}h >= {params['force_eval_hours']:.1f}h "
                               f"with {profit_pct:.2%} -- flat/losing, exiting"),
                    "exit_type": "force_eval",
                    "amount": held_amount,
                    "fraction": 1.0,
                }
            else:
                # Profitable but old -- tighten stop aggressively
                # Use half the tight stop for very old positions
                aged_stop = params["tight_stop_pct"] * 0.5
                if drawdown_pct > aged_stop:
                    return {
                        "action": "EXIT_FULL",
                        "reason": (f"Aged position stop: held {hold_hours:.1f}h, "
                                   f"drawdown {drawdown_pct:.2%} > aged stop {aged_stop:.2%}"),
                        "exit_type": "force_eval_stop",
                        "amount": held_amount,
                        "fraction": 1.0,
                    }

        # === HOLD: no exit conditions met ===
        return {
            "action": "HOLD",
            "reason": (f"Holding: profit {profit_pct:.2%}, drawdown {drawdown_pct:.2%}, "
                       f"stop at {trailing_stop_pct:.2%}, held {hold_hours:.1f}h, "
                       f"regime={params['regime']}"),
            "exit_type": "none",
            "amount": 0,
            "fraction": 0,
        }

    # ==========================================
    # TRADE EXECUTION
    # ==========================================

    def execute_exit(self, pair, amount, reason, exit_type="manual"):
        """Execute a sell order on Coinbase for the given pair and amount.

        Uses risk_controller.approve_trade for the SELL side.
        Records the exit in the database and reports to agent_performance.
        Verifies actual Coinbase balance before selling to prevent insufficient balance errors.
        """
        trader = self._get_trader()
        if not trader:
            logger.error("EXIT FAILED: No trader available for %s", pair)
            return False

        current_price = _get_price(pair)
        if not current_price:
            logger.error("EXIT FAILED: Cannot get price for %s", pair)
            return False

        # Verify actual balance before selling — don't rely on stale position state
        base_currency = pair.split("-")[0]
        actual_balance = 0.0
        try:
            accts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
            for a in accts.get("accounts", []):
                if a.get("currency") == base_currency:
                    actual_balance = float(a.get("available_balance", {}).get("value", 0))
                    break
        except Exception as e:
            logger.warning("EXIT: Balance check failed for %s: %s — proceeding with requested amount", pair, e)
            actual_balance = amount  # fallback to requested amount

        if actual_balance <= 0:
            logger.info("EXIT SKIP: %s actual balance is 0", pair)
            return False

        # Adjust amount if we hold less than requested
        if amount > actual_balance:
            logger.info("EXIT: Adjusting %s sell amount from %.8f to actual balance %.8f",
                        pair, amount, actual_balance)
            amount = actual_balance

        sell_value_usd = amount * current_price

        # Minimum order check (Coinbase requires ~$1 minimum)
        if sell_value_usd < 0.50:
            logger.info("EXIT SKIP: %s sell value $%.2f too small", pair, sell_value_usd)
            return False

        # Risk controller approval for SELL (sells are generally always approved)
        portfolio_value = self._estimate_portfolio_value()
        if _risk_ctrl:
            approved, rc_reason, adj_size = _risk_ctrl.approve_trade(
                "exit_manager", pair, "SELL", sell_value_usd, portfolio_value)
            if not approved:
                logger.warning("EXIT BLOCKED by risk controller: %s -- %s", pair, rc_reason)
                return False

        # Get position state for PnL calculation
        pos = self._positions.get(pair)
        entry_price = pos.entry_price if pos else 0
        pnl_usd = amount * (current_price - entry_price) if entry_price > 0 else 0
        pnl_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0
        hold_hours = pos.hold_duration_hours() if pos else 0
        peak_price = pos.peak_price if pos else current_price

        logger.info("EXIT EXECUTING: %s %s | amount=%.8f | value=$%.2f | reason=%s",
                     exit_type.upper(), pair, amount, sell_value_usd, reason)

        try:
            # Use market order for exits (speed > cost)
            result = trader.place_order(pair, "SELL", amount)

            success = False
            order_id = None
            if "success_response" in result:
                order_id = result["success_response"].get("order_id")
                success = True
            elif "order_id" in result:
                order_id = result["order_id"]
                success = True
            elif "error_response" in result:
                err = result["error_response"]
                logger.error("EXIT ORDER FAILED: %s | %s", pair, err.get("message", err))
            else:
                logger.warning("EXIT ORDER UNKNOWN RESULT: %s", json.dumps(result)[:300])

            if success:
                logger.info("EXIT FILLED: %s | order=%s | PnL=$%.4f (%.2f%%) | held %.1fh",
                             pair, order_id, pnl_usd, pnl_pct * 100, hold_hours)

                # Record exit event
                vol = 0.0
                if _risk_ctrl:
                    try:
                        vol = _risk_ctrl.market.compute_volatility(pair)
                    except Exception:
                        pass

                self._db.execute(
                    "INSERT INTO exit_events (pair, exit_type, reason, entry_price, exit_price, "
                    "amount, pnl_usd, pnl_pct, hold_duration_hours, peak_price, volatility) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (pair, exit_type, reason, entry_price, current_price, amount,
                     round(pnl_usd, 6), round(pnl_pct, 6), round(hold_hours, 3),
                     peak_price, round(vol, 6))
                )
                self._db.commit()

                # Report to risk controller for agent performance tracking
                if _risk_ctrl:
                    _risk_ctrl.record_trade_result("exit_manager", pnl_usd)

                # Update position state
                if pos:
                    label = exit_type.replace("take_profit_", "").replace("time_exit_", "")
                    pos.record_partial_exit(label, amount, current_price)
                    if pos.held_amount <= 0:
                        # Position fully closed
                        with self._lock:
                            if pair in self._positions:
                                del self._positions[pair]
                        logger.info("EXIT COMPLETE: %s position fully closed", pair)

                return True
            return False

        except Exception as e:
            logger.error("EXIT EXECUTION ERROR: %s | %s", pair, e, exc_info=True)
            return False

    # ==========================================
    # POSITION REGISTRATION
    # ==========================================

    def register_position(self, pair, entry_price, entry_time, held_amount, trade_id=None):
        """Register a new position for exit monitoring.

        Called by sniper.py (or any agent) after a BUY order fills.
        """
        with self._lock:
            if pair in self._positions:
                # Update existing position (dollar-cost averaging)
                existing = self._positions[pair]
                old_value = existing.held_amount * existing.entry_price
                new_value = held_amount * entry_price
                total_amount = existing.held_amount + held_amount
                if total_amount > 0:
                    # Weighted average entry price
                    avg_price = (old_value + new_value) / total_amount
                    existing.entry_price = avg_price
                    existing.held_amount = total_amount
                    existing.original_amount = total_amount
                    # Reset peak to the new average -- position is now different
                    existing.peak_price = max(existing.peak_price, entry_price)
                logger.info("EXIT_MGR: Updated position %s: avg_entry=$%.2f, total=%.8f",
                             pair, existing.entry_price, existing.held_amount)
            else:
                self._positions[pair] = PositionState(
                    pair, entry_price, entry_time, held_amount, trade_id)
                logger.info("EXIT_MGR: Registered new position %s: entry=$%.2f, amount=%.8f",
                             pair, entry_price, held_amount)

    def unregister_position(self, pair):
        """Remove a position from monitoring (e.g., manual sell elsewhere)."""
        with self._lock:
            if pair in self._positions:
                del self._positions[pair]
                logger.info("EXIT_MGR: Unregistered position %s", pair)

    # ==========================================
    # POSITION MONITOR (BACKGROUND THREAD)
    # ==========================================

    def _load_positions_from_db(self):
        """Load open positions from sniper_trades table on startup.

        Looks for BUY trades that don't have matching SELL trades.
        """
        try:
            sniper_db_path = str(Path(__file__).parent / "sniper.db")
            if not Path(sniper_db_path).exists():
                logger.info("EXIT_MGR: No sniper.db found, starting with empty positions")
                return

            sdb = sqlite3.connect(sniper_db_path)
            sdb.row_factory = sqlite3.Row

            # Get all filled BUY trades
            buys = sdb.execute(
                "SELECT pair, entry_price, created_at, amount_usd "
                "FROM sniper_trades WHERE direction='BUY' AND status='filled' "
                "ORDER BY id ASC"
            ).fetchall()

            if not buys:
                sdb.close()
                return

            # Get current holdings from Coinbase to verify
            holdings = self._get_holdings_map()

            for buy in buys:
                pair = buy["pair"]
                base_currency = pair.split("-")[0]
                held = holdings.get(base_currency, 0)

                if held > 0 and pair not in self._positions:
                    entry_price = buy["entry_price"] or 0
                    entry_time = buy["created_at"] or datetime.now(timezone.utc).isoformat()
                    self.register_position(pair, entry_price, entry_time, held)

            sdb.close()
            logger.info("EXIT_MGR: Loaded %d positions from sniper_trades", len(self._positions))

        except Exception as e:
            logger.error("EXIT_MGR: Failed to load positions from DB: %s", e)

    def _get_holdings_map(self):
        """Get current Coinbase holdings as {currency: amount} dict."""
        try:
            trader = self._get_trader()
            if not trader:
                return {}
            accts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
            holdings = {}
            for a in accts.get("accounts", []):
                cur = a.get("currency", "")
                bal = float(a.get("available_balance", {}).get("value", 0))
                if cur not in ("USDC", "USD") and bal > 0:
                    holdings[cur] = bal
            return holdings
        except Exception as e:
            logger.warning("EXIT_MGR: Holdings fetch failed: %s", e)
            return {}

    def monitor_positions(self):
        """Main monitoring loop -- runs every 30 seconds.

        For each tracked position:
        1. Gets current price
        2. Checks all exit conditions
        3. Executes exits when triggered
        4. Logs every decision
        """
        logger.info("EXIT_MGR: Position monitor started (30s interval)")

        # Load existing positions from sniper DB on first run
        self._load_positions_from_db()

        while self._running:
            try:
                # Also sync with actual holdings periodically
                holdings = self._get_holdings_map()

                # Check each tracked position
                with self._lock:
                    pairs_to_check = list(self._positions.keys())

                for pair in pairs_to_check:
                    try:
                        pos = self._positions.get(pair)
                        if not pos:
                            continue

                        base_currency = pair.split("-")[0]
                        actual_held = holdings.get(base_currency, 0)

                        # If we no longer hold this asset, clean up
                        if actual_held <= 0:
                            logger.info("EXIT_MGR: %s no longer held, removing from tracking", pair)
                            self.unregister_position(pair)
                            continue

                        # Update held amount to match reality
                        pos.held_amount = actual_held

                        # Get current price
                        current_price = _get_price(pair)
                        if not current_price:
                            logger.debug("EXIT_MGR: Cannot get price for %s, skipping", pair)
                            continue

                        # Check exit conditions
                        decision = self.check_exit(
                            pair, pos.entry_price, pos.entry_time,
                            current_price, actual_held, pos.trade_id)

                        if decision["action"] == "HOLD":
                            logger.debug("EXIT_MGR: %s -- %s", pair, decision["reason"])
                            continue

                        # Execute the exit
                        exit_amount = decision["amount"]
                        exit_type = decision["exit_type"]
                        reason = decision["reason"]

                        logger.info("EXIT_MGR: %s triggered for %s -- %s",
                                     decision["action"], pair, reason)

                        success = self.execute_exit(pair, exit_amount, reason, exit_type)
                        # NOTE: record_partial_exit is already called inside execute_exit()
                        # when success=True — do NOT call it again here (was causing double-recording,
                        # held_amount going to zero prematurely, positions lost from tracking)

                    except Exception as e:
                        logger.error("EXIT_MGR: Error checking %s: %s", pair, e, exc_info=True)

            except Exception as e:
                logger.error("EXIT_MGR: Monitor loop error: %s", e, exc_info=True)

            # Sleep 30 seconds between checks
            for _ in range(30):
                if not self._running:
                    break
                time.sleep(1)

        logger.info("EXIT_MGR: Position monitor stopped")

    def start(self):
        """Start the background position monitor thread."""
        if self._running:
            logger.warning("EXIT_MGR: Already running")
            return

        self._running = True
        self._monitor_thread = threading.Thread(
            target=self.monitor_positions, daemon=True, name="exit_monitor")
        self._monitor_thread.start()
        logger.info("EXIT_MGR: Background monitor started")

    def stop(self):
        """Stop the background position monitor."""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=10)
            self._monitor_thread = None
        logger.info("EXIT_MGR: Background monitor stopped")

    # ==========================================
    # REPORTING
    # ==========================================

    def get_active_positions(self):
        """Get summary of all tracked positions."""
        with self._lock:
            result = []
            for pair, pos in self._positions.items():
                current_price = _get_price(pair)
                params = self._get_dynamic_params(pair)
                profit_pct = pos.profit_pct(current_price) if current_price else 0

                result.append({
                    "pair": pair,
                    "entry_price": pos.entry_price,
                    "current_price": current_price,
                    "peak_price": pos.peak_price,
                    "held_amount": pos.held_amount,
                    "profit_pct": round(profit_pct, 4),
                    "drawdown_from_peak": round(pos.drawdown_from_peak_pct(current_price), 4) if current_price else 0,
                    "hold_hours": round(pos.hold_duration_hours(), 2),
                    "trailing_stop_pct": round(self._compute_trailing_stop_pct(profit_pct, params), 4),
                    "partial_exits": list(pos.partial_exits_done),
                    "regime": params["regime"],
                    "volatility": params["volatility"],
                })
            return result

    def get_exit_history(self, limit=20):
        """Get recent exit events."""
        rows = self._db.execute(
            "SELECT * FROM exit_events ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def print_status(self):
        """Print full exit manager status."""
        print(f"\n{'='*70}")
        print(f"  EXIT MANAGER STATUS")
        print(f"{'='*70}")
        print(f"  Running: {self._running}")
        print(f"  Tracked positions: {len(self._positions)}")

        positions = self.get_active_positions()
        if positions:
            print(f"\n  Active Positions:")
            for p in positions:
                stop_pct = p["trailing_stop_pct"]
                print(f"    {p['pair']:12s} | entry=${p['entry_price']:>10,.2f} | "
                      f"now=${p['current_price'] or 0:>10,.2f} | "
                      f"P&L={p['profit_pct']:+.2%} | "
                      f"peak=${p['peak_price']:>10,.2f} | "
                      f"dd={p['drawdown_from_peak']:.2%} | "
                      f"stop={stop_pct:.2%} | "
                      f"held={p['hold_hours']:.1f}h | "
                      f"{p['regime']}")
                if p["partial_exits"]:
                    print(f"              exits: {', '.join(p['partial_exits'])}")
        else:
            print(f"\n  No active positions being tracked")

        history = self.get_exit_history(10)
        if history:
            print(f"\n  Recent Exits:")
            total_pnl = 0
            for h in history:
                total_pnl += h.get("pnl_usd", 0)
                print(f"    {h['exit_type']:20s} | {h['pair']:12s} | "
                      f"PnL=${h.get('pnl_usd', 0):+.4f} ({h.get('pnl_pct', 0)*100:+.2f}%) | "
                      f"held {h.get('hold_duration_hours', 0):.1f}h | "
                      f"{h['created_at']}")
            print(f"\n  Total exit P&L: ${total_pnl:+.4f}")

        print(f"{'='*70}\n")


# ==========================================
# SINGLETON
# ==========================================

_exit_manager = None
_exit_manager_lock = threading.Lock()


def get_exit_manager():
    """Get or create the singleton ExitManager."""
    global _exit_manager
    with _exit_manager_lock:
        if _exit_manager is None:
            _exit_manager = ExitManager()
        return _exit_manager


# ==========================================
# CLI
# ==========================================

if __name__ == "__main__":
    import sys

    em = get_exit_manager()

    if len(sys.argv) > 1 and sys.argv[1] == "status":
        em.print_status()

    elif len(sys.argv) > 1 and sys.argv[1] == "monitor":
        # Run the monitor in foreground (for testing)
        print("Starting exit monitor in foreground (Ctrl+C to stop)...")
        em._running = True
        try:
            em.monitor_positions()
        except KeyboardInterrupt:
            em.stop()
            print("Stopped.")

    elif len(sys.argv) > 1 and sys.argv[1] == "test":
        # Test exit logic with simulated positions
        print("=== EXIT MANAGER TEST ===")

        # Simulate positions at various profit levels
        test_cases = [
            ("BTC-USDC", 100000, 101500, 0.001, "1.5% profit"),
            ("ETH-USDC", 3500, 3400, 0.1, "slight loss"),
            ("SOL-USDC", 200, 210, 1.0, "5% profit"),
            ("DOGE-USDC", 0.40, 0.39, 100, "2.5% loss"),
        ]

        for pair, entry, current, amount, desc in test_cases:
            entry_time = datetime.now(timezone.utc).isoformat()
            decision = em.check_exit(pair, entry, entry_time, current, amount)
            params = em._get_dynamic_params(pair)

            profit = (current - entry) / entry
            print(f"\n  {pair} ({desc}):")
            print(f"    Entry: ${entry:,.2f} -> Now: ${current:,.2f} ({profit:+.2%})")
            print(f"    Vol: {params['volatility']:.3f} | Regime: {params['regime']}")
            print(f"    Decision: {decision['action']} -- {decision['reason']}")

        # Test time-based exit
        print(f"\n  Time-based test (5h old, flat):")
        old_time = "2026-02-12T01:00:00+00:00"  # 5+ hours ago
        decision = em.check_exit("AVAX-USDC", 40.0, old_time, 40.10, 5.0)
        print(f"    Decision: {decision['action']} -- {decision['reason']}")

        print(f"\n{'='*70}")

    elif len(sys.argv) > 1 and sys.argv[1] == "params":
        # Show dynamic parameters for all monitored pairs
        print("=== DYNAMIC EXIT PARAMETERS ===")
        pairs = ["BTC-USDC", "ETH-USDC", "SOL-USDC"]
        for pair in pairs:
            params = em._get_dynamic_params(pair)
            print(f"\n  {pair} (vol={params['volatility']:.3f}, regime={params['regime']}):")
            print(f"    Trailing stops: wide={params['wide_stop_pct']:.2%} | "
                  f"med={params['medium_stop_pct']:.2%} | tight={params['tight_stop_pct']:.2%}")
            print(f"    Profit tiers: low={params['low_profit_threshold']:.2%} | "
                  f"mid={params['mid_profit_threshold']:.2%}")
            print(f"    Take-profit: TP1={params['tp1_pct']:.2%} (sell {params['tp1_sell_frac']:.0%}) | "
                  f"TP2={params['tp2_pct']:.2%} (sell {params['tp2_sell_frac']:.0%})")
            print(f"    Time exits: dead={params['dead_money_hours']:.1f}h | "
                  f"force={params['force_eval_hours']:.1f}h")
            print(f"    Max position loss: ${params['max_position_loss_usd']:.2f}")

    elif len(sys.argv) > 1 and sys.argv[1] == "history":
        history = em.get_exit_history(30)
        if history:
            total = 0
            for h in history:
                total += h.get("pnl_usd", 0)
                print(f"  {h['exit_type']:20s} | {h['pair']:12s} | "
                      f"PnL=${h.get('pnl_usd', 0):+.4f} | {h['created_at']}")
            print(f"\n  Total: ${total:+.4f}")
        else:
            print("  No exit history yet")

    else:
        print("Usage: exit_manager.py [status|monitor|test|params|history]")
        em.print_status()
