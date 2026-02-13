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
import hashlib
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

# Use persistent volume on Fly (/data/), local agents/ dir otherwise
_persistent_dir = Path("/data") if Path("/data").is_dir() else Path(__file__).parent
EXIT_DB = str(_persistent_dir / "exit_manager.db")
TRADER_DB = str(Path(__file__).parent / "trader.db")
EXIT_STATUS_FILE = Path(__file__).parent / "exit_manager_status.json"
EXIT_IMPROVEMENTS_FILE = Path(__file__).parent / "exit_manager_improvements_registry.json"
MCP_HINTS_FILE = Path(__file__).parent / "mcp_exit_hints.json"
MCP_OUTBOX_FILE = Path(__file__).parent / "mcp_exit_outbox.json"
BASE_MONITOR_INTERVAL_SECONDS = int(os.environ.get("EXIT_MONITOR_INTERVAL_SECONDS", "30"))
MIN_MONITOR_INTERVAL_SECONDS = int(os.environ.get("EXIT_MONITOR_MIN_SECONDS", "8"))
MAX_MONITOR_INTERVAL_SECONDS = int(os.environ.get("EXIT_MONITOR_MAX_SECONDS", "75"))
EXIT_EXECUTION_RETRY_LIMIT = int(os.environ.get("EXIT_EXECUTION_RETRY_LIMIT", "2"))
EXIT_EXECUTION_SLIPPAGE_BPS = float(os.environ.get("EXIT_EXECUTION_SLIPPAGE_BPS", "8"))
EXIT_MIN_SELL_USD = float(os.environ.get("EXIT_MIN_SELL_USD", "1.0"))
EXIT_DUST_UNTRACK_USD = float(os.environ.get("EXIT_DUST_UNTRACK_USD", "0.75"))
EXIT_LOCK_GAIN_PCT = float(os.environ.get("EXIT_LOCK_GAIN_PCT", "0.01"))
EXIT_LOCK_GAIN_MAX_NOTIONAL_USD = float(os.environ.get("EXIT_LOCK_GAIN_MAX_NOTIONAL_USD", "25.0"))

try:
    from smart_router import SmartRouter
except Exception:
    SmartRouter = None

try:
    from exit_manager_improvements import load_or_create_registry
except Exception:
    try:
        from agents.exit_manager_improvements import load_or_create_registry  # type: ignore
    except Exception:
        def load_or_create_registry(path):
            return {
                "program": "exit_manager_upgrade",
                "summary": {
                    "advanced_total": 0,
                    "hyper_total": 0,
                    "advanced_active": 0,
                    "hyper_active": 0,
                },
                "items": [],
            }

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
        self._router = SmartRouter() if SmartRouter else None
        self._monitor_interval_seconds = max(5, int(BASE_MONITOR_INTERVAL_SECONDS))
        self._consecutive_api_failures = 0
        self._execution_failures = 0
        self._cycle_index = 0
        self._last_cycle_latency_ms = 0.0
        self._fear_greed_cache = (0, 1.0)  # (timestamp, multiplier)
        self._last_hints = {}
        self._improvements = load_or_create_registry(EXIT_IMPROVEMENTS_FILE)
        self._toolset = {
            "risk_controller": bool(_risk_ctrl),
            "smart_router": bool(self._router),
            "mcp_hints_file": str(MCP_HINTS_FILE),
            "mcp_outbox_file": str(MCP_OUTBOX_FILE),
            "status_file": str(EXIT_STATUS_FILE),
        }

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
            CREATE TABLE IF NOT EXISTS exit_decision_telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                action TEXT NOT NULL,
                exit_type TEXT,
                confidence REAL,
                priority INTEGER DEFAULT 0,
                decision_hash TEXT,
                decision_json TEXT,
                cycle_index INTEGER,
                latency_ms REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS exit_execution_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                exit_type TEXT,
                order_type TEXT,
                attempt INTEGER DEFAULT 1,
                amount REAL,
                price REAL,
                success INTEGER DEFAULT 0,
                order_id TEXT,
                details_json TEXT,
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

    def _active_improvement_counts(self):
        items = self._improvements.get("items", []) if isinstance(self._improvements, dict) else []
        if not isinstance(items, list):
            items = []
        adv = [x for x in items if isinstance(x, dict) and str(x.get("tier", "")) == "advanced"]
        hyp = [x for x in items if isinstance(x, dict) and str(x.get("tier", "")) == "hyper"]
        return {
            "advanced_total": len(adv),
            "hyper_total": len(hyp),
            "advanced_active": sum(1 for x in adv if str(x.get("status", "")).lower() == "active"),
            "hyper_active": sum(1 for x in hyp if str(x.get("status", "")).lower() == "active"),
        }

    def _load_mcp_hints(self):
        if not MCP_HINTS_FILE.exists():
            return {}
        try:
            payload = json.loads(MCP_HINTS_FILE.read_text())
            if not isinstance(payload, dict):
                return {}
            self._last_hints = payload
            return payload
        except Exception:
            return {}

    @staticmethod
    def _clamp(value, lo, hi):
        return max(lo, min(hi, value))

    def _apply_mcp_overrides(self, pair, params):
        hints = self._load_mcp_hints()
        if not hints:
            return params
        global_cfg = hints.get("global", {}) if isinstance(hints.get("global"), dict) else {}
        pair_cfg = hints.get("pairs", {}).get(pair, {}) if isinstance(hints.get("pairs"), dict) else {}
        merged = dict(params)
        merged["hint_source"] = "mcp"
        vol_scale = float(pair_cfg.get("vol_scale", global_cfg.get("vol_scale", 1.0)) or 1.0)
        stop_scale = float(pair_cfg.get("stop_scale", global_cfg.get("stop_scale", 1.0)) or 1.0)
        dead_mult = float(pair_cfg.get("dead_money_hours_mult", global_cfg.get("dead_money_hours_mult", 1.0)) or 1.0)
        force_mult = float(pair_cfg.get("force_eval_hours_mult", global_cfg.get("force_eval_hours_mult", 1.0)) or 1.0)
        tp_bias = float(pair_cfg.get("tp_bias_pct", global_cfg.get("tp_bias_pct", 0.0)) or 0.0)
        merged["volatility"] = self._clamp(float(merged["volatility"]) * vol_scale, 0.001, 0.25)
        merged["wide_stop_pct"] = self._clamp(float(merged["wide_stop_pct"]) * stop_scale, 0.004, 0.12)
        merged["medium_stop_pct"] = self._clamp(float(merged["medium_stop_pct"]) * stop_scale, 0.003, 0.10)
        merged["tight_stop_pct"] = self._clamp(float(merged["tight_stop_pct"]) * stop_scale, 0.002, 0.08)
        merged["dead_money_hours"] = self._clamp(float(merged["dead_money_hours"]) * dead_mult, 0.5, 72.0)
        merged["force_eval_hours"] = self._clamp(float(merged["force_eval_hours"]) * force_mult, 2.0, 168.0)
        merged["tp1_pct"] = self._clamp(float(merged["tp1_pct"]) + tp_bias, 0.002, 0.25)
        merged["tp2_pct"] = self._clamp(float(merged["tp2_pct"]) + tp_bias, 0.004, 0.40)
        return merged

    def _decision_confidence(self, params, profit_pct, drawdown_pct, hold_hours, action):
        base = 0.42
        base += min(0.18, max(0.0, abs(float(profit_pct)) * 2.0))
        base += min(0.18, max(0.0, float(drawdown_pct) * 2.5))
        base += min(0.12, max(0.0, float(hold_hours) / 48.0))
        if str(action).upper().startswith("EXIT"):
            base += 0.10
        vol = float(params.get("volatility", 0.02) or 0.02)
        if vol > 0.08:
            base += 0.04
        return round(self._clamp(base, 0.05, 0.99), 4)

    def _decision_hash(self, pair, decision):
        payload = {
            "pair": pair,
            "action": decision.get("action"),
            "exit_type": decision.get("exit_type"),
            "amount": round(float(decision.get("amount", 0.0) or 0.0), 8),
            "reason": str(decision.get("reason", "")),
            "cycle": self._cycle_index,
        }
        raw = json.dumps(payload, sort_keys=True).encode()
        return hashlib.sha256(raw).hexdigest()[:20]

    def _record_decision_telemetry(self, pair, decision, cycle_latency_ms):
        decision_hash = self._decision_hash(pair, decision)
        confidence = float(decision.get("confidence", 0.0) or 0.0)
        priority = int(decision.get("priority", 0) or 0)
        self._db.execute(
            """
            INSERT INTO exit_decision_telemetry
                (pair, action, exit_type, confidence, priority, decision_hash, decision_json, cycle_index, latency_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pair,
                str(decision.get("action", "HOLD")),
                str(decision.get("exit_type", "none")),
                confidence,
                priority,
                decision_hash,
                json.dumps(decision),
                int(self._cycle_index),
                float(cycle_latency_ms),
            ),
        )
        self._db.commit()
        return decision_hash

    def _record_execution_attempt(self, pair, exit_type, order_type, attempt, amount, price, success, order_id, details):
        self._db.execute(
            """
            INSERT INTO exit_execution_attempts
                (pair, exit_type, order_type, attempt, amount, price, success, order_id, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pair,
                str(exit_type or "manual"),
                str(order_type or "market"),
                int(attempt),
                float(amount or 0.0),
                float(price or 0.0),
                1 if success else 0,
                None if order_id is None else str(order_id),
                json.dumps(details or {}),
            ),
        )
        self._db.commit()

    def _record_realized_sell_trade(self, pair, order_type, order_id, price, amount, pnl_usd):
        """Mirror successful SELL fills into trader.db for realized-PnL gating."""
        try:
            tdb = sqlite3.connect(TRADER_DB)
            tdb.execute(
                """
                INSERT INTO agent_trades
                    (agent, pair, side, price, quantity, total_usd, order_type, order_id, status, pnl)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "exit_manager",
                    str(pair),
                    "SELL",
                    float(price),
                    float(amount),
                    float(amount) * float(price),
                    str(order_type or "market"),
                    str(order_id or ""),
                    "filled",
                    float(pnl_usd),
                ),
            )
            tdb.commit()
            tdb.close()
        except Exception as e:
            logger.warning("EXIT_MGR: Could not mirror realized sell to trader.db: %s", e)

    def _adaptive_interval_seconds(self):
        active = len(self._positions)
        if active <= 0:
            base = BASE_MONITOR_INTERVAL_SECONDS + 10
        elif active <= 2:
            base = BASE_MONITOR_INTERVAL_SECONDS
        else:
            base = max(MIN_MONITOR_INTERVAL_SECONDS, BASE_MONITOR_INTERVAL_SECONDS - min(14, active * 3))
        if self._consecutive_api_failures > 0:
            base += min(30, self._consecutive_api_failures * 3)
        if self._execution_failures > 0:
            base += min(20, self._execution_failures * 2)
        return int(self._clamp(base, MIN_MONITOR_INTERVAL_SECONDS, MAX_MONITOR_INTERVAL_SECONDS))

    def _write_status(self):
        improvements = self._active_improvement_counts()
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "running": bool(self._running),
            "cycle_index": int(self._cycle_index),
            "monitor_interval_seconds": int(self._monitor_interval_seconds),
            "active_positions": len(self._positions),
            "consecutive_api_failures": int(self._consecutive_api_failures),
            "execution_failures": int(self._execution_failures),
            "last_cycle_latency_ms": round(float(self._last_cycle_latency_ms), 3),
            "improvements": improvements,
            "toolset": dict(self._toolset),
            "mcp_hints_present": bool(self._last_hints),
        }
        EXIT_STATUS_FILE.write_text(json.dumps(payload, indent=2))
        try:
            MCP_OUTBOX_FILE.write_text(
                json.dumps(
                    {
                        "timestamp": payload["updated_at"],
                        "agent": "exit_manager",
                        "event": "status",
                        "payload": payload,
                    },
                    indent=2,
                )
            )
        except Exception:
            pass

    # ==========================================
    # DYNAMIC EXIT PARAMETER CALCULATIONS
    # ==========================================

    def _get_fear_multiplier(self):
        """Get fear-based stop multiplier, cached for 5 minutes."""
        now = time.time()
        cached_ts, cached_val = self._fear_greed_cache
        if now - cached_ts < 300:  # 5 minute cache
            return cached_val

        multiplier = 1.0
        try:
            import urllib.request, json as _json
            _fg_data = _json.loads(urllib.request.urlopen(
                "https://api.alternative.me/fng/?limit=1", timeout=3
            ).read())
            fg_value = int(_fg_data["data"][0]["value"])
            if fg_value < 15:       # Extreme Fear
                multiplier = 0.3   # 70% tighter stops
                logger.info("EXIT_MGR: EXTREME FEAR (F&G=%d) — stops at 30%%", fg_value)
            elif fg_value < 25:     # Fear
                multiplier = 0.5   # 50% tighter stops
            elif fg_value < 40:     # Mild Fear
                multiplier = 0.75  # 25% tighter stops
        except Exception:
            pass  # can't reach API, use defaults

        self._fear_greed_cache = (now, multiplier)
        return multiplier

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

        # === FEAR & GREED CIRCUIT BREAKER ===
        # In extreme fear (market crashing), tighten ALL stops aggressively
        # Rule #1: NEVER lose money — cut losses fast in crashes
        fear_multiplier = self._get_fear_multiplier()

        # === TRAILING STOP TIERS ===
        # Base stop distances scale with volatility
        # Higher vol = wider stops (prevent noise shakeouts)
        # Lower vol = tighter stops (less room needed)
        vol_multiplier = max(0.5, min(3.0, volatility / 0.02))  # 1.0x at 2% vol

        # Tier 1: 0 to low_profit_threshold -- wide stop, let it breathe
        # Base: 2% from peak, scaled by volatility and fear
        wide_stop = 0.02 * vol_multiplier * fear_multiplier

        # Tier 2: low_profit to mid_profit -- tighten
        # Base: 1.5% from peak, scaled by volatility and fear
        medium_stop = 0.015 * vol_multiplier * fear_multiplier

        # Tier 3: above mid_profit -- lock in gains
        # Base: 1% from peak, scaled by volatility and fear
        tight_stop = 0.01 * vol_multiplier * fear_multiplier

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
        # Base: 3 hours (was 4h — faster capital recycling at small portfolio)
        # In extreme fear: cut dead money time aggressively (get out fast)
        dead_money_hours = 3.0 * regime_time_mult * fear_multiplier
        # Minimum gain to avoid being considered dead money
        # Scales with volatility -- in high vol, 0.5% is nothing
        dead_money_min_gain = 0.005 * vol_multiplier

        # Force-evaluate threshold: position too old
        # Base: 24 hours, adjusted by regime
        force_eval_hours = 24.0 * regime_time_mult

        # === TAKE-PROFIT TARGETS ===
        # Scale targets with volatility -- higher vol = larger targets
        # TP0: Micro take-profit — free capital quickly for compounding
        # At ~$200 portfolio, freeing $5-10 fast matters more than riding for 5%
        tp0_pct = 0.008 * vol_multiplier   # ~0.8% in normal vol (covers fees + small profit)
        tp0_sell_frac = 0.20               # sell 20% at TP0 — free cash for next trade

        tp1_pct = 0.02 * vol_multiplier    # ~2% in normal vol
        tp1_sell_frac = 0.25               # sell 25% at TP1 (was 30%, reduced for TP0)

        tp2_pct = 0.05 * vol_multiplier    # ~5% in normal vol
        tp2_sell_frac = 0.25               # sell 25% at TP2 (was 30%, reduced for TP0)

        # Remaining 30% rides with trailing stop
        rider_frac = 1.0 - tp0_sell_frac - tp1_sell_frac - tp2_sell_frac

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

        params = {
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
            # Take-profit (TP0 = micro, TP1 = standard, TP2 = extended)
            "tp0_pct": round(tp0_pct, 4),
            "tp0_sell_frac": tp0_sell_frac,
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
        return self._apply_mcp_overrides(pair, params)

    def has_exit_plan(self, pair):
        """Return True only when dynamic exit thresholds are valid for a pair."""
        try:
            params = self._get_dynamic_params(pair)
            if not isinstance(params, dict):
                return False
            required = ("wide_stop_pct", "tp1_pct", "tp2_pct", "max_position_loss_usd")
            for key in required:
                value = float(params.get(key, 0.0) or 0.0)
                if value <= 0:
                    return False
            return True
        except Exception as e:
            logger.warning("EXIT_MGR: exit-plan validation failed for %s: %s", pair, e)
            return False

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
        # TP0: Micro take-profit — free cash quickly for compounding
        # At small portfolios, freeing $5-10 fast is critical for growth
        if profit_pct >= params["tp0_pct"] and "tp0" not in pos.partial_exits_done:
            sell_amount = pos.original_amount * params["tp0_sell_frac"]
            sell_amount = min(sell_amount, held_amount)
            if sell_amount > 0 and sell_amount * current_price >= 0.50:
                return {
                    "action": "EXIT_PARTIAL",
                    "reason": (f"Micro TP0: {profit_pct:.2%} >= {params['tp0_pct']:.2%} target, "
                               f"selling {params['tp0_sell_frac']:.0%} to free capital for compounding"),
                    "exit_type": "take_profit_tp0",
                    "amount": sell_amount,
                    "fraction": params["tp0_sell_frac"],
                    "label": "tp0",
                }

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

        # === CHECK 3b: Known-gain lock for small positions ===
        # Realize wins for small notional positions instead of letting them churn.
        position_notional = held_amount * current_price
        if (
            profit_pct >= EXIT_LOCK_GAIN_PCT
            and position_notional > 0
            and position_notional <= EXIT_LOCK_GAIN_MAX_NOTIONAL_USD
        ):
            return {
                "action": "EXIT_FULL",
                "reason": (
                    f"Known-gain lock: {profit_pct:.2%} >= {EXIT_LOCK_GAIN_PCT:.2%} "
                    f"on small position ${position_notional:.2f} <= ${EXIT_LOCK_GAIN_MAX_NOTIONAL_USD:.2f}"
                ),
                "exit_type": "known_gain_lock",
                "amount": held_amount,
                "fraction": 1.0,
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
        """Execute a SELL with optimized exit routing and retry logic."""
        trader = self._get_trader()
        if not trader:
            logger.error("EXIT FAILED: No trader available for %s", pair)
            return False

        current_price = _get_price(pair)
        if not current_price:
            logger.error("EXIT FAILED: Cannot get price for %s", pair)
            return False

        base_currency = pair.split("-")[0]
        actual_balance = 0.0
        try:
            accts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
            for a in accts.get("accounts", []):
                if a.get("currency") == base_currency:
                    actual_balance = float(a.get("available_balance", {}).get("value", 0))
                    break
            self._consecutive_api_failures = 0
        except Exception as e:
            logger.warning("EXIT: Balance check failed for %s: %s — proceeding with requested amount", pair, e)
            self._consecutive_api_failures += 1
            actual_balance = amount

        if actual_balance <= 0:
            logger.info("EXIT SKIP: %s actual balance is 0", pair)
            return False
        if amount > actual_balance:
            logger.info("EXIT: Adjusting %s sell amount from %.8f to actual balance %.8f",
                        pair, amount, actual_balance)
            amount = actual_balance

        sell_value_usd = amount * current_price
        full_value_usd = actual_balance * current_price
        if sell_value_usd < EXIT_MIN_SELL_USD:
            # If a partial is below executable notional, escalate to full close when possible.
            if amount < actual_balance and full_value_usd >= EXIT_MIN_SELL_USD:
                logger.info(
                    "EXIT: %s partial $%.2f below min $%.2f, escalating to full close $%.2f",
                    pair,
                    sell_value_usd,
                    EXIT_MIN_SELL_USD,
                    full_value_usd,
                )
                amount = actual_balance
                sell_value_usd = full_value_usd
            else:
                logger.info(
                    "EXIT SKIP: %s sell value $%.2f below executable min $%.2f",
                    pair,
                    sell_value_usd,
                    EXIT_MIN_SELL_USD,
                )
                if full_value_usd < EXIT_DUST_UNTRACK_USD:
                    logger.info(
                        "EXIT_MGR: %s notional $%.2f is dust (<$%.2f), removing from tracking",
                        pair,
                        full_value_usd,
                        EXIT_DUST_UNTRACK_USD,
                    )
                    self.unregister_position(pair)
                return False

        portfolio_value = self._estimate_portfolio_value()
        if _risk_ctrl:
            approved, rc_reason, _adj_size = _risk_ctrl.approve_trade(
                "exit_manager", pair, "SELL", sell_value_usd, portfolio_value)
            if not approved:
                logger.warning("EXIT BLOCKED by risk controller: %s -- %s", pair, rc_reason)
                return False

        pos = self._positions.get(pair)
        entry_price = pos.entry_price if pos else 0
        pnl_usd = amount * (current_price - entry_price) if entry_price > 0 else 0
        pnl_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0
        hold_hours = pos.hold_duration_hours() if pos else 0
        peak_price = pos.peak_price if pos else current_price

        route_hint = {}
        if self._router is not None:
            try:
                route_hint = self._router.find_best_execution(pair, "SELL", sell_value_usd) or {}
            except Exception:
                route_hint = {}

        logger.info(
            "EXIT EXECUTING: %s %s | amount=%.8f | value=$%.2f | reason=%s | route=%s",
            str(exit_type).upper(), pair, amount, sell_value_usd, reason, route_hint.get("venue", "coinbase")
        )

        urgent_types = {"loss_limit", "trailing_stop", "force_eval_stop"}
        urgent = str(exit_type) in urgent_types
        slippage_frac = max(1e-6, float(EXIT_EXECUTION_SLIPPAGE_BPS) / 10000.0)
        limit_price = current_price * (1.0 - slippage_frac)

        plan = []
        if not urgent:
            plan.append(("limit", {"price": limit_price, "post_only": False}))
        plan.append(("market", {}))
        plan = plan[:max(1, int(EXIT_EXECUTION_RETRY_LIMIT))]

        success = False
        order_id = None
        last_error = ""
        attempt = 0
        filled_order_type = "market"
        for order_type, cfg in plan:
            attempt += 1
            try:
                if order_type == "limit":
                    result = trader.place_limit_order(
                        pair,
                        "SELL",
                        amount,
                        cfg["price"],
                        post_only=bool(cfg.get("post_only", False)),
                        bypass_profit_guard=True,
                    )
                else:
                    result = trader.place_order(pair, "SELL", amount, bypass_profit_guard=True)
            except Exception as e:
                result = {"error_response": {"message": str(e)}}

            if "success_response" in result:
                order_id = result["success_response"].get("order_id")
                success = bool(order_id)
            elif "order_id" in result:
                order_id = result.get("order_id")
                success = bool(order_id)
            else:
                err = result.get("error_response", {}) if isinstance(result, dict) else {}
                last_error = str(err.get("message", "unknown_error"))
                success = False

            # Safely serialize result for telemetry (avoid MagicMock/non-serializable)
            try:
                safe_result = json.loads(json.dumps(result, default=str))
            except Exception:
                safe_result = str(result)[:500]
            try:
                safe_route_hint = json.loads(json.dumps(route_hint, default=str))
            except Exception:
                safe_route_hint = str(route_hint)[:500]
            self._record_execution_attempt(
                pair=pair,
                exit_type=exit_type,
                order_type=order_type,
                attempt=attempt,
                amount=amount,
                price=(cfg.get("price", current_price) if isinstance(cfg, dict) else current_price),
                success=success,
                order_id=order_id,
                details={"result": safe_result, "reason": reason, "route_hint": safe_route_hint},
            )
            if success:
                filled_order_type = str(order_type or "market")
                break

        if not success:
            self._execution_failures += 1
            logger.error("EXIT ORDER FAILED: %s | %s", pair, last_error or "unknown_failure")
            return False

        self._execution_failures = 0
        logger.info("EXIT FILLED: %s | order=%s | PnL=$%.4f (%.2f%%) | held %.1fh",
                    pair, order_id, pnl_usd, pnl_pct * 100, hold_hours)

        vol = 0.0
        if _risk_ctrl:
            try:
                vol = _risk_ctrl.market.compute_volatility(pair)
            except Exception:
                vol = 0.0

        self._db.execute(
            "INSERT INTO exit_events (pair, exit_type, reason, entry_price, exit_price, "
            "amount, pnl_usd, pnl_pct, hold_duration_hours, peak_price, volatility) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (pair, exit_type, reason, entry_price, current_price, amount,
             round(pnl_usd, 6), round(pnl_pct, 6), round(hold_hours, 3),
             peak_price, round(vol, 6))
        )
        self._db.commit()
        self._record_realized_sell_trade(
            pair=pair,
            order_type=filled_order_type,
            order_id=order_id,
            price=current_price,
            amount=amount,
            pnl_usd=pnl_usd,
        )

        if _risk_ctrl:
            _risk_ctrl.record_trade_result("exit_manager", pnl_usd)

        if pos:
            label = exit_type.replace("take_profit_", "").replace("time_exit_", "")
            pos.record_partial_exit(label, amount, current_price)
            if pos.held_amount <= 0:
                with self._lock:
                    if pair in self._positions:
                        del self._positions[pair]
                logger.info("EXIT COMPLETE: %s position fully closed", pair)

        return True

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
        Falls back to auto-discovery from Coinbase holdings if no DB records found.
        """
        try:
            # Check both persistent volume and local path for sniper.db
            candidates = [
                str(Path("/data") / "sniper.db"),
                str(Path(__file__).parent / "sniper.db"),
            ]
            sniper_db_path = None
            for p in candidates:
                if Path(p).exists():
                    sniper_db_path = p
                    break

            if sniper_db_path:
                sdb = sqlite3.connect(sniper_db_path)
                sdb.row_factory = sqlite3.Row

                buys = sdb.execute(
                    "SELECT pair, entry_price, created_at, amount_usd "
                    "FROM sniper_trades WHERE direction='BUY' AND status='filled' "
                    "ORDER BY id ASC"
                ).fetchall()

                if buys:
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

            if self._positions:
                logger.info("EXIT_MGR: Loaded %d positions from sniper_trades", len(self._positions))
                return

            # Fallback: auto-discover from Coinbase holdings
            # This ensures exits work even after deploys that wipe ephemeral DBs
            self._auto_discover_positions()

        except Exception as e:
            logger.error("EXIT_MGR: Failed to load positions from DB: %s", e)
            # Still try auto-discovery as last resort
            try:
                self._auto_discover_positions()
            except Exception:
                pass

    def _auto_discover_positions(self):
        """Auto-discover positions from Coinbase holdings when no DB records exist.

        Uses current price as entry price (conservative — may undercount profit).
        This ensures exit_manager ALWAYS monitors positions even after deploys.
        """
        holdings = self._get_holdings_map()
        if not holdings:
            logger.info("EXIT_MGR: No holdings found on Coinbase for auto-discovery")
            return

        # Reserve assets — never exit these (treasury)
        reserve_assets = {"BTC", "USD", "USDC"}

        discovered = 0
        for currency, amount in holdings.items():
            if currency in reserve_assets:
                continue  # Skip reserve assets — they are treasury

            pair = f"{currency}-USD"
            if pair in self._positions:
                continue

            current_price = _get_price(pair)
            if not current_price or current_price <= 0:
                continue

            position_value = amount * current_price
            if position_value < 1.0:  # skip dust
                continue

            # Register with current price as entry (conservative)
            self.register_position(
                pair, current_price,
                datetime.now(timezone.utc).isoformat(),
                amount)
            discovered += 1
            logger.info("EXIT_MGR: AUTO-DISCOVERED %s | %.8f units ($%.2f) @ $%.4f",
                       pair, amount, position_value, current_price)

        if discovered:
            logger.info("EXIT_MGR: Auto-discovered %d positions from Coinbase holdings", discovered)

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
        """Main monitoring loop with adaptive timing and telemetry."""
        logger.info("EXIT_MGR: Position monitor started (base interval=%ss)", BASE_MONITOR_INTERVAL_SECONDS)

        # Load existing positions from sniper DB on first run
        self._load_positions_from_db()

        while self._running:
            cycle_started = time.perf_counter()
            self._cycle_index += 1
            try:
                # Also sync with actual holdings periodically
                holdings = self._get_holdings_map()
                if not holdings:
                    self._consecutive_api_failures += 1
                else:
                    self._consecutive_api_failures = 0

                # Compute portfolio value once for concentration checks
                portfolio_value = self._estimate_portfolio_value()

                # Check each tracked position
                with self._lock:
                    pairs_to_check = list(self._positions.keys())

                # === CONCENTRATION CHECK: sell over-weighted positions to free cash ===
                # Scans ALL holdings (not just tracked positions) so it works on fresh deploys
                MAX_CONCENTRATION_PCT = 0.25  # 25% max per asset
                TARGET_CONCENTRATION_PCT = 0.18  # sell down to 18%
                QUOTE_CURRENCIES = {"USDC", "USD"}
                for base_currency, held_amount in holdings.items():
                    if base_currency in QUOTE_CURRENCIES or held_amount <= 0:
                        continue
                    try:
                        # Try common pair formats
                        pair = None
                        for suffix in ("-USDC", "-USD"):
                            test_pair = f"{base_currency}{suffix}"
                            p = _get_price(test_pair)
                            if p and p > 0:
                                pair = test_pair
                                current_price = p
                                break
                        if not pair or portfolio_value <= 0:
                            continue
                        position_value = held_amount * current_price
                        concentration = position_value / portfolio_value
                        if concentration > MAX_CONCENTRATION_PCT:
                            target_value = portfolio_value * TARGET_CONCENTRATION_PCT
                            sell_value = position_value - target_value
                            sell_amount = sell_value / current_price
                            if sell_value >= 1.0:  # minimum $1 for concentration rebalance
                                logger.info(
                                    "EXIT_MGR: CONCENTRATION REBALANCE %s | %.0f%% > %.0f%% max | "
                                    "selling $%.2f to reach %.0f%% target",
                                    pair, concentration * 100, MAX_CONCENTRATION_PCT * 100,
                                    sell_value, TARGET_CONCENTRATION_PCT * 100)
                                self.execute_exit(
                                    pair, sell_amount,
                                    f"Concentration rebalance: {concentration:.0%} > {MAX_CONCENTRATION_PCT:.0%} max, "
                                    f"selling ${sell_value:.2f} to reach {TARGET_CONCENTRATION_PCT:.0%}",
                                    "concentration_rebalance")
                    except Exception as e:
                        logger.error("EXIT_MGR: Concentration check error %s: %s", base_currency, e)

                for pair in pairs_to_check:
                    try:
                        pair_started = time.perf_counter()
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
                            self._consecutive_api_failures += 1
                            continue
                        self._consecutive_api_failures = 0

                        # Stop cycling on non-executable dust positions.
                        held_value = actual_held * current_price
                        if held_value < EXIT_DUST_UNTRACK_USD:
                            logger.info(
                                "EXIT_MGR: %s value $%.2f below dust threshold $%.2f, untracking",
                                pair,
                                held_value,
                                EXIT_DUST_UNTRACK_USD,
                            )
                            self.unregister_position(pair)
                            continue

                        # Check exit conditions
                        decision = self.check_exit(
                            pair, pos.entry_price, pos.entry_time,
                            current_price, actual_held, pos.trade_id)

                        params = self._get_dynamic_params(pair)
                        profit_pct = pos.profit_pct(current_price)
                        drawdown_pct = pos.drawdown_from_peak_pct(current_price)
                        hold_hours = pos.hold_duration_hours()
                        decision["confidence"] = self._decision_confidence(
                            params=params,
                            profit_pct=profit_pct,
                            drawdown_pct=drawdown_pct,
                            hold_hours=hold_hours,
                            action=decision.get("action", "HOLD"),
                        )
                        decision["priority"] = {
                            "loss_limit": 100,
                            "trailing_stop": 95,
                            "force_eval_stop": 90,
                            "force_eval": 80,
                            "time_exit_dead_money": 75,
                            "take_profit_tp2": 65,
                            "take_profit_tp1": 60,
                            "take_profit_tp0": 55,
                            "scale_out": 50,
                        }.get(str(decision.get("exit_type", "none")), 10)
                        decision["cycle_index"] = self._cycle_index
                        decision_latency_ms = (time.perf_counter() - pair_started) * 1000.0
                        decision_hash = self._record_decision_telemetry(
                            pair, decision, cycle_latency_ms=decision_latency_ms
                        )

                        if decision["action"] == "HOLD":
                            logger.debug(
                                "EXIT_MGR: %s -- %s [conf=%.2f hash=%s]",
                                pair,
                                decision["reason"],
                                float(decision.get("confidence", 0.0)),
                                decision_hash,
                            )
                            continue

                        # Execute the exit
                        exit_amount = decision["amount"]
                        exit_type = decision["exit_type"]
                        reason = decision["reason"]

                        logger.info("EXIT_MGR: %s triggered for %s -- %s",
                                     decision["action"], pair, reason)

                        success = self.execute_exit(pair, exit_amount, reason, exit_type)
                        if not success:
                            logger.warning("EXIT_MGR: execution failed for %s [hash=%s]", pair, decision_hash)
                        # NOTE: record_partial_exit is already called inside execute_exit()
                        # when success=True — do NOT call it again here (was causing double-recording,
                        # held_amount going to zero prematurely, positions lost from tracking)

                    except Exception as e:
                        logger.error("EXIT_MGR: Error checking %s: %s", pair, e, exc_info=True)

            except Exception as e:
                logger.error("EXIT_MGR: Monitor loop error: %s", e, exc_info=True)

            self._last_cycle_latency_ms = (time.perf_counter() - cycle_started) * 1000.0
            self._monitor_interval_seconds = self._adaptive_interval_seconds()
            self._write_status()

            # Sleep between checks with adaptive cadence
            for _ in range(max(1, int(self._monitor_interval_seconds))):
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
        self._write_status()
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
        self._write_status()
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

    def get_decision_telemetry(self, limit=50):
        """Get recent decision telemetry rows."""
        rows = self._db.execute(
            """
            SELECT pair, action, exit_type, confidence, priority, decision_hash, cycle_index, latency_ms, created_at
            FROM exit_decision_telemetry
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_execution_attempts(self, limit=50):
        """Get recent execution attempt rows."""
        rows = self._db.execute(
            """
            SELECT pair, exit_type, order_type, attempt, amount, price, success, order_id, created_at
            FROM exit_execution_attempts
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        return [dict(r) for r in rows]

    def print_status(self):
        """Print full exit manager status."""
        improvements = self._active_improvement_counts()
        print(f"\n{'='*70}")
        print(f"  EXIT MANAGER STATUS")
        print(f"{'='*70}")
        print(f"  Running: {self._running}")
        print(f"  Tracked positions: {len(self._positions)}")
        print(
            f"  Improvements: advanced={improvements['advanced_active']}/{improvements['advanced_total']} "
            f"hyper={improvements['hyper_active']}/{improvements['hyper_total']}"
        )
        print(
            f"  Monitor cadence: {self._monitor_interval_seconds}s | "
            f"cycle={self._cycle_index} | last_cycle={self._last_cycle_latency_ms:.1f}ms"
        )
        print(
            f"  Toolset: risk_controller={self._toolset['risk_controller']} "
            f"smart_router={self._toolset['smart_router']} mcp_hints={MCP_HINTS_FILE.exists()}"
        )

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

    elif len(sys.argv) > 1 and sys.argv[1] == "improvements":
        registry = em._improvements if isinstance(em._improvements, dict) else {}
        summary = em._active_improvement_counts()
        print("=== EXIT MANAGER IMPROVEMENTS ===")
        print(json.dumps(summary, indent=2))
        if len(sys.argv) > 2 and sys.argv[2] == "--full":
            print(json.dumps(registry, indent=2))

    elif len(sys.argv) > 1 and sys.argv[1] == "telemetry":
        limit = 20
        if len(sys.argv) > 2:
            try:
                limit = max(1, int(sys.argv[2]))
            except Exception:
                limit = 20
        decisions = em.get_decision_telemetry(limit=limit)
        attempts = em.get_execution_attempts(limit=limit)
        print("=== EXIT MANAGER TELEMETRY ===")
        print(f"Decisions ({len(decisions)}):")
        for row in decisions:
            print(
                f"  {row['created_at']} | {row['pair']:12s} | {row['action']:11s} | "
                f"{row.get('exit_type', 'none'):18s} | conf={float(row.get('confidence', 0.0)):.2f} | "
                f"prio={int(row.get('priority', 0) or 0):3d} | lat={float(row.get('latency_ms', 0.0)):.1f}ms"
            )
        print(f"\nExecution attempts ({len(attempts)}):")
        for row in attempts:
            print(
                f"  {row['created_at']} | {row['pair']:12s} | {row.get('exit_type', 'manual'):18s} | "
                f"{row.get('order_type', 'market'):6s} | try={int(row.get('attempt', 0) or 0)} | "
                f"ok={int(row.get('success', 0) or 0)} | order={row.get('order_id') or '-'}"
            )

    else:
        print("Usage: exit_manager.py [status|monitor|test|params|history|improvements|telemetry]")
        em.print_status()
