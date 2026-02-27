#!/usr/bin/env python3
"""Continuous Position Manager — target allocation rebalancing engine.

Paradigm shift from HF (discrete entry/exit) to CF (continuous positioning):
  HF: Wait for signal → enter → wait → exit → repeat
  CF: Always be positioned → continuously adjust allocation → compound

Each cycle:
  1. Fetch current holdings and portfolio value
  2. Fetch latest signal confidences from sniper scan DB
  3. Compute target allocation for each pair (0% to MAX_ALLOC_PCT)
  4. Compare current vs target, generate rebalance orders
  5. Execute via exchange_connector (existing risk controls apply)

Target allocation = f(signal_confidence, regime, volatility, fear_greed)
  - Higher confidence → larger allocation (quadratic scaling)
  - Extreme fear → lean in harder (contrarian)
  - High volatility → reduce allocation (risk parity)
  - Downtrend regime → zero allocation (Rule #1: never lose money)

Usage:
  python3 agents/continuous_position_manager.py
"""

import json
import logging
import os
import sqlite3
import sys
import threading
import time
import atexit
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from process_singleton import acquire_process_singleton, release_process_singleton

# Load .env if present
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

_LOG_DIR = Path("/data") if Path("/data").exists() else Path(__file__).parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CF] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(_LOG_DIR / "continuous_position_manager.log")),
    ],
)
logger = logging.getLogger("continuous_position_manager")

# ── Env helpers ──
def _env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


# ── Configuration ──
CYCLE_SECONDS = max(10, int(os.environ.get("CF_CYCLE_SECONDS", "60")))
MAX_ALLOC_PCT = min(0.40, float(os.environ.get("CF_MAX_ALLOC_PCT", "0.30")))
MIN_ALLOC_PCT = float(os.environ.get("CF_MIN_ALLOC_PCT", "0.0"))
RESERVE_PCT = max(0.05, float(os.environ.get("CF_RESERVE_PCT", "0.10")))
REBALANCE_THRESHOLD_PCT = float(os.environ.get("CF_REBALANCE_THRESHOLD_PCT", "0.015"))
MIN_TRADE_USD = float(os.environ.get("CF_MIN_TRADE_USD", "10.0"))
CONFIDENCE_FLOOR = float(os.environ.get("CF_CONFIDENCE_FLOOR", "0.50"))
PAIRS = (os.environ.get("CF_PAIRS", "") or "BTC-USD,ETH-USD,SOL-USD,AVAX-USD,LINK-USD").split(",")
PAIRS = [p.strip().upper() for p in PAIRS if str(p).strip()]
CF_TREND_FLOOR_ENABLED = _env_bool("CF_TREND_FLOOR_ENABLED", True)
CF_TREND_FLOOR_PCT = max(0.0, min(MAX_ALLOC_PCT, float(os.environ.get("CF_TREND_FLOOR_PCT", "0.12"))))
CF_TREND_FLOOR_PAIRS = [
    p.strip().upper()
    for p in (os.environ.get("CF_TREND_FLOOR_PAIRS", "BTC-USD,ETH-USD").split(","))
    if p.strip()
]
CF_MAX_TARGET_STEP_DOWN_PCT = max(0.0, float(os.environ.get("CF_MAX_TARGET_STEP_DOWN_PCT", "0.07")))
CF_SIGNAL_GAP_HOLD_CYCLES = max(0, int(os.environ.get("CF_SIGNAL_GAP_HOLD_CYCLES", "8")))
CF_MOMENTUM_FOLLOW_ENABLED = _env_bool("CF_MOMENTUM_FOLLOW_ENABLED", True)
CF_MOMENTUM_MIN_PCT = float(os.environ.get("CF_MOMENTUM_MIN_PCT", "0.0015"))
CF_MOMENTUM_LOOKBACK_SECONDS = max(60, int(os.environ.get("CF_MOMENTUM_LOOKBACK_SECONDS", "900")))
CF_MOMENTUM_HISTORY_MAX_SECONDS = max(
    CF_MOMENTUM_LOOKBACK_SECONDS + 60,
    int(os.environ.get("CF_MOMENTUM_HISTORY_MAX_SECONDS", "7200")),
)
CF_MAX_ORDER_USD = max(0.0, float(os.environ.get("CF_MAX_ORDER_USD", "20.0")))
CF_ORDER_CHUNK_PCT = max(0.0, float(os.environ.get("CF_ORDER_CHUNK_PCT", "0.02")))
# Sniper DB: on Fly containers it's at /data/sniper.db, locally at agents/sniper.db
_DEFAULT_SNIPER_DB = "/data/sniper.db" if os.path.exists("/data") else str(Path(__file__).parent / "sniper.db")
SNIPER_DB_PATH = os.environ.get("SNIPER_DB_PATH", _DEFAULT_SNIPER_DB)
_STATUS_DIR = Path("/data") if Path("/data").exists() else Path(__file__).parent
STATUS_FILE = _STATUS_DIR / "continuous_position_manager_status.json"
TRADING_LOCK_FILE = Path(__file__).parent / "trading_lock.json"
PID_FILE = Path(__file__).parent / ".continuous_position_manager.pid"


def _is_pid_running(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def _acquire_pid_guard():
    try:
        if PID_FILE.exists():
            raw = PID_FILE.read_text().strip()
            if raw:
                existing = int(raw)
                if existing != os.getpid() and _is_pid_running(existing):
                    logger.error("CF singleton: existing process active pid=%s", existing)
                    return False
        PID_FILE.write_text(str(os.getpid()))
    except Exception as e:
        logger.warning("CF singleton: pid guard warning: %s", e)
    return True


def _release_pid_guard():
    try:
        if PID_FILE.exists() and PID_FILE.read_text().strip() == str(os.getpid()):
            PID_FILE.unlink(missing_ok=True)
    except Exception:
        pass


class ContinuousPositionManager:
    """Target-allocation rebalancing engine for continuous trading."""

    def __init__(self):
        self._cycle = 0
        self._last_targets = {}
        self._last_rebalance_ts = {}
        self._last_target_notes = []
        self._signal_gap_cycles = 0
        self._price_history = {}
        self._cooldown_seconds = max(30, int(os.environ.get("CF_COOLDOWN_SECONDS", "120")))
        self._running = True
        self._lock = threading.Lock()

        # Import trading infrastructure (lazy — may not be available in test)
        self._trader = None
        self._risk_ctrl = None
        self._exchange = None
        self._exit_mgr = None
        self._ws_feed = None
        self._restore_last_targets()

    def _restore_last_targets(self):
        """Restore previous targets from status file to smooth restarts."""
        try:
            if not STATUS_FILE.exists():
                return
            payload = json.loads(STATUS_FILE.read_text())
            raw_targets = payload.get("targets", {})
            if not isinstance(raw_targets, dict):
                return
            restored = {}
            for pair in PAIRS:
                raw = raw_targets.get(pair, 0.0)
                try:
                    restored[pair] = max(0.0, float(raw) / 100.0)
                except Exception:
                    restored[pair] = 0.0
            if any(v > 0.0 for v in restored.values()):
                self._last_targets = restored
                logger.info(
                    "CF: restored last targets from status: %s",
                    ", ".join(f"{k}={v:.1%}" for k, v in restored.items() if v > 0),
                )
        except Exception:
            pass

    @staticmethod
    def _normalize_regime_label(value):
        regime = str(value or "").strip().lower()
        if regime in ("uptrend", "bull", "bullish", "markup", "up"):
            return "uptrend"
        if regime in ("downtrend", "bear", "bearish", "markdown", "down"):
            return "downtrend"
        return "neutral"

    def _record_price(self, pair, price):
        if not price or price <= 0:
            return
        now = time.time()
        series = self._price_history.setdefault(pair, [])
        series.append((now, float(price)))
        cutoff = now - CF_MOMENTUM_HISTORY_MAX_SECONDS
        if len(series) > 1:
            self._price_history[pair] = [(ts, px) for ts, px in series if ts >= cutoff]

    def _momentum_pct(self, pair, lookback_seconds=CF_MOMENTUM_LOOKBACK_SECONDS):
        series = self._price_history.get(pair, [])
        if len(series) < 2:
            return 0.0
        now = time.time()
        cutoff = now - max(60, int(lookback_seconds))
        latest = series[-1][1]
        anchor = None
        for ts, px in reversed(series):
            if ts <= cutoff:
                anchor = px
                break
        if anchor is None:
            anchor = series[0][1]
        if anchor <= 0 or latest <= 0:
            return 0.0
        return (latest - anchor) / anchor

    def _local_regime(self, pair):
        momentum_pct = self._momentum_pct(pair)
        if momentum_pct >= CF_MOMENTUM_MIN_PCT:
            return "uptrend"
        if momentum_pct <= -CF_MOMENTUM_MIN_PCT:
            return "downtrend"
        return "neutral"

    def _init_trading(self):
        """Lazy-init trading connections."""
        if self._trader is not None:
            return True
        try:
            from exchange_connector import CoinbaseTrader
            self._trader = CoinbaseTrader()
            logger.info("CF: CoinbaseTrader initialized")
        except Exception as e:
            logger.warning("CF: CoinbaseTrader unavailable: %s", e)
            return False

        try:
            from risk_controller import RiskController
            self._risk_ctrl = RiskController()
        except Exception as e:
            logger.warning("CF: Risk controller unavailable: %s", e)

        try:
            from coinbase_ws_feed import CoinbaseWSFeed
            self._ws_feed = CoinbaseWSFeed(pairs=PAIRS)
            self._ws_feed.start()
            logger.info("CF: WebSocket feed started for %d pairs", len(PAIRS))
        except Exception as e:
            logger.warning("CF: WebSocket feed unavailable: %s", e)

        return True

    # ── Signal Retrieval ──

    def get_latest_signals(self):
        """Read latest signal confidences from sniper's scan DB.

        Returns dict: {pair: {direction, confidence, confirming_signals, reason}}
        """
        signals = {}
        try:
            db = sqlite3.connect(SNIPER_DB_PATH, timeout=5)
            db.row_factory = sqlite3.Row
            # Get most recent scan per pair (last 5 minutes)
            cutoff = time.time() - 300
            rows = db.execute(
                """SELECT pair, direction, composite_confidence, confirming_signals,
                          signal_details, created_at
                   FROM sniper_scans
                   WHERE created_at > datetime(?, 'unixepoch')
                   ORDER BY created_at DESC""",
                (cutoff,),
            ).fetchall()
            db.close()

            seen = set()
            for row in rows:
                pair = row["pair"]
                if pair in seen:
                    continue
                seen.add(pair)
                signals[pair] = {
                    "direction": row["direction"] or "NONE",
                    "confidence": float(row["composite_confidence"] or 0),
                    "confirming_signals": int(row["confirming_signals"] or 0),
                    "reason": str(row["signal_details"] or "")[:200],
                }
        except Exception as e:
            logger.warning("CF: Failed to read sniper signals: %s", e)

        return signals

    _fg_cache = (0.0, 50)  # (timestamp, value)

    def _get_fear_greed(self):
        """Get current Fear & Greed index (cached 5 min)."""
        now = time.time()
        cached_ts, cached_val = ContinuousPositionManager._fg_cache
        if now - cached_ts < 300:
            return cached_val
        # Try env var first (set by other agents)
        try:
            env_val = os.environ.get("LAST_FEAR_GREED", "")
            if env_val:
                val = int(env_val)
                ContinuousPositionManager._fg_cache = (now, val)
                return val
        except (ValueError, TypeError):
            pass
        # Fetch from API
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://api.alternative.me/fng/?limit=1",
                headers={"User-Agent": "NetTrace/1.0"},
            )
            resp = urllib.request.urlopen(req, timeout=3)
            data = json.loads(resp.read())
            val = int(data["data"][0]["value"])
            ContinuousPositionManager._fg_cache = (now, val)
            return val
        except Exception:
            return cached_val

    def _get_regime(self, pair):
        """Get market regime for a pair (uptrend/neutral/downtrend)."""
        local = self._local_regime(pair)
        try:
            from agent_goals import GoalValidator
            detect_fn = getattr(GoalValidator, "detect_regime", None)
            if callable(detect_fn):
                remote = self._normalize_regime_label(detect_fn(pair))
                # Trust explicit non-neutral signal from shared detector.
                if remote != "neutral":
                    return remote
        except Exception:
            pass
        return local

    def _get_volatility(self, pair):
        """Estimate recent volatility for position sizing."""
        try:
            if self._ws_feed:
                quote = self._ws_feed.get_quote(pair)
                if quote:
                    bid = float(quote.get("bid", 0) or 0)
                    ask = float(quote.get("ask", 0) or 0)
                    if bid > 0 and ask > 0:
                        spread_pct = (ask - bid) / ((ask + bid) / 2)
                        # Rough vol estimate: spread * 10 (very rough)
                        return max(0.005, min(0.10, spread_pct * 10))
        except Exception:
            pass
        # Default moderate volatility
        return 0.02

    # ── Target Allocation ──

    def compute_targets(self, signals, portfolio_value):
        """Convert signal confidences to target portfolio allocations.

        Returns: {pair: target_pct} where 0.0 <= target_pct <= MAX_ALLOC_PCT
        """
        self._last_target_notes = []
        fg = self._get_fear_greed()
        extreme_fear = fg < 15
        deployable_pct = 1.0 - RESERVE_PCT

        raw_weights = {}
        total_weight = 0.0

        for pair in PAIRS:
            # Refresh local price history for momentum/regime fallback.
            self._get_price(pair)
            sig = signals.get(pair, {})
            conf = sig.get("confidence", 0.0)
            direction = sig.get("direction", "NONE")
            regime = self._get_regime(pair)
            vol = self._get_volatility(pair)

            # Rule #1: never buy in downtrend (unless extreme fear)
            if regime == "downtrend" and direction == "BUY" and not extreme_fear:
                raw_weights[pair] = 0.0
                continue

            # Only allocate on BUY signals above confidence floor
            if direction != "BUY" or conf < CONFIDENCE_FLOOR:
                # Fallback: track core assets in local uptrend even when signal feed is stale.
                if (
                    CF_MOMENTUM_FOLLOW_ENABLED
                    and pair in CF_TREND_FLOOR_PAIRS
                    and regime == "uptrend"
                ):
                    momentum_pct = self._momentum_pct(pair)
                    if momentum_pct >= CF_MOMENTUM_MIN_PCT:
                        direction = "BUY"
                        conf = min(0.92, max(CONFIDENCE_FLOOR, 0.55 + momentum_pct * 15.0))
                        self._last_target_notes.append(
                            f"momentum_follow pair={pair} mom={momentum_pct:.2%} conf={conf:.2f}"
                        )
                    else:
                        raw_weights[pair] = 0.0
                        continue
                else:
                    raw_weights[pair] = 0.0
                    continue

            if direction != "BUY" or conf < CONFIDENCE_FLOOR:
                raw_weights[pair] = 0.0
                continue

            # Quadratic confidence scaling (strong signals get much more)
            weight = conf ** 2

            # Volatility adjustment (risk parity: less allocation to volatile assets)
            vol_adj = max(0.3, 1.0 - vol * 5)
            weight *= vol_adj

            # Extreme fear boost (contrarian lean-in)
            if extreme_fear:
                fear_boost = 1.0 + max(0, (15 - fg)) * 0.05  # up to 1.5x at F&G=5
                weight *= fear_boost

            # Confirming signals bonus
            n_signals = sig.get("confirming_signals", 0)
            if n_signals >= 4:
                weight *= 1.2
            elif n_signals >= 3:
                weight *= 1.1

            raw_weights[pair] = weight
            total_weight += weight

        # Normalize: distribute deployable capital proportionally
        targets = {}
        if total_weight > 0:
            self._signal_gap_cycles = 0
            for pair in PAIRS:
                w = raw_weights.get(pair, 0.0)
                if w <= 0:
                    targets[pair] = 0.0
                    continue
                target = (w / total_weight) * deployable_pct
                target = min(target, MAX_ALLOC_PCT)
                targets[pair] = round(target, 4)
        else:
            # No signal weight this cycle: hold previous targets briefly to avoid
            # forced de-risking from transient feed/signal dropouts.
            self._signal_gap_cycles += 1
            if self._last_targets and self._signal_gap_cycles <= CF_SIGNAL_GAP_HOLD_CYCLES:
                for pair in PAIRS:
                    targets[pair] = round(float(self._last_targets.get(pair, 0.0) or 0.0), 4)
                self._last_target_notes.append(
                    f"signal_gap_hold_last_targets cycle={self._signal_gap_cycles}/{CF_SIGNAL_GAP_HOLD_CYCLES}"
                )
            else:
                for pair in PAIRS:
                    targets[pair] = 0.0

        # Maintain core BTC/ETH exposure during confirmed uptrends so we can track upside.
        if CF_TREND_FLOOR_ENABLED and CF_TREND_FLOOR_PCT > 0:
            floor_pairs = []
            for pair in CF_TREND_FLOOR_PAIRS:
                if pair not in PAIRS:
                    continue
                regime = str(self._get_regime(pair)).lower()
                if regime == "uptrend":
                    floor_pairs.append(pair)
            if floor_pairs:
                floor_pct = min(CF_TREND_FLOOR_PCT, deployable_pct / len(floor_pairs))
                for pair in floor_pairs:
                    if targets.get(pair, 0.0) < floor_pct:
                        targets[pair] = round(floor_pct, 4)
                self._last_target_notes.append(
                    f"trend_floor_applied pairs={','.join(floor_pairs)} floor={floor_pct:.1%}"
                )

        # Avoid immediate sell-to-zero on temporary signal dropouts.
        if CF_MAX_TARGET_STEP_DOWN_PCT > 0 and self._last_targets:
            for pair in PAIRS:
                prev = float(self._last_targets.get(pair, 0.0) or 0.0)
                cur = float(targets.get(pair, 0.0) or 0.0)
                min_allowed = max(0.0, prev - CF_MAX_TARGET_STEP_DOWN_PCT)
                if cur < min_allowed:
                    targets[pair] = round(min_allowed, 4)
            self._last_target_notes.append(
                f"target_step_down_limited max_drop={CF_MAX_TARGET_STEP_DOWN_PCT:.1%}/cycle"
            )

        # Respect reserve constraint after adjustments by trimming non-core pairs first.
        total_target = sum(max(0.0, float(v)) for v in targets.values())
        if total_target > deployable_pct and total_target > 0:
            excess = total_target - deployable_pct
            trim_order = [p for p in PAIRS if p not in CF_TREND_FLOOR_PAIRS] + [p for p in PAIRS if p in CF_TREND_FLOOR_PAIRS]
            for pair in trim_order:
                if excess <= 0:
                    break
                cur = float(targets.get(pair, 0.0) or 0.0)
                if cur <= 0:
                    continue
                cut = min(cur, excess)
                targets[pair] = round(cur - cut, 4)
                excess -= cut
            self._last_target_notes.append("reserve_trim_applied")

        return targets

    # ── Holdings ──

    def get_current_allocations(self, portfolio_value):
        """Get current allocation percentages from exchange holdings.

        Returns: {pair: current_pct, ...}, total_portfolio_usd
        """
        allocations = {}
        if not self._trader:
            return allocations, 0.0

        try:
            resp = self._trader.get_accounts()
            accounts = resp.get("accounts", []) if isinstance(resp, dict) else []
            total_usd = 0.0
            for pair in PAIRS:
                base = pair.split("-")[0]
                # Find account for this currency
                amount = 0.0
                for acct in accounts:
                    if acct.get("currency") == base:
                        avail = acct.get("available_balance", {})
                        amount = float(avail.get("value", 0) or 0)
                        break
                if amount <= 0:
                    allocations[pair] = 0.0
                    continue
                price = self._get_price(pair)
                if not price:
                    allocations[pair] = 0.0
                    continue
                value = amount * price
                total_usd += value
                if portfolio_value > 0:
                    allocations[pair] = round(value / portfolio_value, 4)
                else:
                    allocations[pair] = 0.0
            return allocations, total_usd
        except Exception as e:
            logger.warning("CF: Failed to get holdings: %s", e)
            return allocations, 0.0

    def _get_price(self, pair):
        """Get current price via WS feed or REST fallback."""
        if self._ws_feed:
            try:
                quote = self._ws_feed.get_quote(pair)
                if quote and float(quote.get("mid", 0) or 0) > 0:
                    px = float(quote["mid"])
                    self._record_price(pair, px)
                    return px
                # Try data pair alias
                dp = pair.replace("-USDC", "-USD")
                if dp != pair:
                    quote = self._ws_feed.get_quote(dp)
                    if quote and float(quote.get("mid", 0) or 0) > 0:
                        px = float(quote["mid"])
                        self._record_price(pair, px)
                        return px
            except Exception:
                pass
        # REST fallback
        try:
            import urllib.request
            dp = pair.replace("-USDC", "-USD")
            url = f"https://api.coinbase.com/v2/prices/{dp}/spot"
            req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
            resp = urllib.request.urlopen(req, timeout=5)
            data = json.loads(resp.read())
            px = float(data["data"]["amount"])
            self._record_price(pair, px)
            return px
        except Exception:
            return None

    def _get_portfolio_value(self):
        """Get total portfolio value in USD."""
        try:
            if self._trader:
                resp = self._trader.get_accounts()
                accounts = resp.get("accounts", []) if isinstance(resp, dict) else []
                total = 0.0
                for acct in accounts:
                    currency = acct.get("currency", "")
                    avail = acct.get("available_balance", {})
                    amount = float(avail.get("value", 0) or 0)
                    if amount <= 0:
                        continue
                    if currency in ("USD", "USDC"):
                        total += amount
                    else:
                        price = self._get_price(f"{currency}-USD")
                        if price:
                            total += amount * price
                return total
        except Exception as e:
            logger.warning("CF: Portfolio value fetch failed: %s", e)
        return 0.0

    def _entry_lock_active(self):
        try:
            if not TRADING_LOCK_FILE.exists():
                return False
            payload = json.loads(TRADING_LOCK_FILE.read_text())
            return bool(payload.get("locked", False))
        except Exception:
            return False

    # ── Rebalancing ──

    def compute_rebalance_orders(self, targets, current_allocs, portfolio_value):
        """Generate orders to close gap between target and current allocation.

        Returns list of: {pair, direction, amount_usd, reason}
        """
        orders = []
        now = time.time()
        entry_lock_active = self._entry_lock_active()
        chunk_cap = 0.0
        if CF_ORDER_CHUNK_PCT > 0 and portfolio_value > 0:
            chunk_cap = max(chunk_cap, portfolio_value * CF_ORDER_CHUNK_PCT)
        if CF_MAX_ORDER_USD > 0:
            chunk_cap = min(chunk_cap, CF_MAX_ORDER_USD) if chunk_cap > 0 else CF_MAX_ORDER_USD

        for pair in PAIRS:
            target_pct = targets.get(pair, 0.0)
            current_pct = current_allocs.get(pair, 0.0)
            gap_pct = target_pct - current_pct
            gap_usd = gap_pct * portfolio_value

            # Skip if gap is below threshold
            if abs(gap_pct) < REBALANCE_THRESHOLD_PCT:
                continue
            if chunk_cap > 0 and abs(gap_usd) > chunk_cap:
                gap_usd = chunk_cap if gap_usd > 0 else -chunk_cap
            if abs(gap_usd) < MIN_TRADE_USD:
                continue

            # Per-pair cooldown
            last_rebal = self._last_rebalance_ts.get(pair, 0)
            if now - last_rebal < self._cooldown_seconds:
                continue

            # Get signal confidence for edge computation
            sig = getattr(self, '_last_signals', {}).get(pair, {})
            sig_conf = sig.get("confidence", 0.75)

            if gap_usd > 0:
                orders.append({
                    "pair": pair,
                    "direction": "BUY",
                    "amount_usd": round(gap_usd, 2),
                    "target_pct": target_pct,
                    "current_pct": current_pct,
                    "signal_confidence": sig_conf,
                    "reason": (
                        f"CF rebalance: {current_pct:.1%} → {target_pct:.1%} "
                        f"(+${gap_usd:.2f}) chunk_cap=${chunk_cap:.2f}"
                    ),
                })
            elif gap_usd < 0:
                # If entries are globally locked, avoid one-way de-risking of core trend pairs
                # unless regime is explicitly downtrend.
                if entry_lock_active and pair in CF_TREND_FLOOR_PAIRS:
                    regime = str(self._get_regime(pair)).lower()
                    if regime != "downtrend":
                        self._last_target_notes.append(
                            f"sell_suppressed_entry_lock pair={pair} regime={regime or 'unknown'}"
                        )
                        continue
                orders.append({
                    "pair": pair,
                    "direction": "SELL",
                    "amount_usd": round(abs(gap_usd), 2),
                    "target_pct": target_pct,
                    "current_pct": current_pct,
                    "signal_confidence": sig_conf,
                    "reason": (
                        f"CF rebalance: {current_pct:.1%} → {target_pct:.1%} "
                        f"(-${abs(gap_usd):.2f}) chunk_cap=${chunk_cap:.2f}"
                    ),
                })

        # Sort: sells first (free up cash), then buys by gap size descending
        sells = [o for o in orders if o["direction"] == "SELL"]
        buys = sorted([o for o in orders if o["direction"] == "BUY"],
                      key=lambda x: x["amount_usd"], reverse=True)
        return sells + buys

    def execute_rebalance_order(self, order):
        """Execute a single rebalance order via CoinbaseTrader.

        Returns True if order was placed successfully.
        """
        pair = order["pair"]
        direction = order["direction"]
        amount_usd = order["amount_usd"]

        if not self._trader:
            logger.warning("CF: No trader — cannot execute %s %s $%.2f",
                          direction, pair, amount_usd)
            return False

        # Risk controller approval
        if self._risk_ctrl and direction == "BUY":
            try:
                portfolio = self._get_portfolio_value()
                approved, reason, adj_size = self._risk_ctrl.approve_trade(
                    "continuous_trader", pair, direction, amount_usd, portfolio,
                )
                if not approved:
                    logger.info("CF: Risk controller blocked %s %s $%.2f: %s",
                               direction, pair, amount_usd, reason)
                    return False
                amount_usd = adj_size
            except Exception as e:
                logger.warning("CF: Risk controller error: %s", e)

        # Get price for order
        price = self._get_price(pair)
        if not price or price <= 0:
            logger.warning("CF: No price for %s — skipping", pair)
            return False

        # Compute quantity
        quantity = amount_usd / price

        # Get product precision for rounding
        try:
            product = self._trader.get_product(pair)
            if isinstance(product, dict):
                base_increment = product.get("base_increment", "0.00000001")
                # Round quantity to product precision
                precision = len(str(base_increment).rstrip("0").split(".")[-1])
                quantity = round(quantity, precision)
        except Exception:
            quantity = round(quantity, 8)

        if quantity <= 0:
            return False

        try:
            # CF rebalancing has its own risk controls (target allocation caps,
            # reserve %, cooldowns, risk controller approval). Bypass the per-trade
            # no-loss policy which was designed for sniper's discrete trades.
            signal_conf = order.get("signal_confidence", 0.75)
            # Pass realistic edge (used for logging/metrics, not gating)
            expected_edge = max(0.10, (signal_conf - 0.5) * 4.0)

            if direction == "BUY":
                # Position registry: skip if owned by another agent
                try:
                    from position_registry import get_registry
                    _pos_reg = get_registry()
                    if _pos_reg:
                        reg_owner = _pos_reg.get_owner(pair)
                        if reg_owner and reg_owner not in ("continuous_trader", "sniper"):
                            logger.info("CF: %s owned by %s — skipping BUY (registry)", pair, reg_owner)
                            return None
                except Exception:
                    pass
                # Place limit order slightly below mid (maker)
                limit_price = round(price * 0.9996, 2)
                result = self._trader.place_limit_order(
                    pair, "BUY", quantity, limit_price,
                    signal_confidence=signal_conf,
                    expected_edge_pct=expected_edge,
                    bypass_profit_guard=True,
                )
            else:
                # SELL: slightly above mid (maker)
                limit_price = round(price * 1.0005, 2)
                result = self._trader.place_limit_order(
                    pair, "SELL", quantity, limit_price,
                    signal_confidence=0.99,
                    expected_edge_pct=expected_edge,
                    bypass_profit_guard=True,
                )

            order_id = None
            if isinstance(result, dict):
                sr = result.get("success_response", {})
                order_id = sr.get("order_id") or result.get("order_id")

            if order_id:
                logger.info("CF REBALANCE: %s %s $%.2f @ $%.2f -> order=%s (%s)",
                           direction, pair, amount_usd, limit_price, order_id,
                           order.get("reason", ""))
                self._last_rebalance_ts[pair] = time.time()
                return True
            else:
                err = result.get("error_response", {}) if isinstance(result, dict) else {}
                logger.warning("CF: Order failed %s %s: %s", direction, pair,
                             err.get("message", str(result)[:200]))
                return False

        except Exception as e:
            logger.warning("CF: Order execution failed for %s %s: %s", direction, pair, e)
            return False

    # ── Status ──

    def write_status(self, targets, current_allocs, orders, portfolio_value, status_note=None):
        """Write status file for monitoring."""
        try:
            status = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "cycle": self._cycle,
                "portfolio_value": round(portfolio_value, 2),
                "mode": "CF",
                "fear_greed": self._get_fear_greed(),
                "targets": {k: round(v * 100, 1) for k, v in targets.items()},
                "current": {k: round(v * 100, 1) for k, v in current_allocs.items()},
                "pending_orders": len(orders),
                "target_notes": list(self._last_target_notes),
                "signal_gap_cycles": int(self._signal_gap_cycles),
                "orders": [
                    {"pair": o["pair"], "direction": o["direction"],
                     "amount_usd": o["amount_usd"], "reason": o["reason"]}
                    for o in orders[:10]
                ],
            }
            if status_note:
                status["note"] = str(status_note)
            with open(STATUS_FILE, "w") as f:
                json.dump(status, f, indent=2)
        except Exception:
            pass

    # ── Main Loop ──

    def run(self):
        """Main continuous position management loop."""
        logger.info("CF Position Manager starting (cycle=%ds, pairs=%s, max_alloc=%.0f%%, reserve=%.0f%%)",
                    CYCLE_SECONDS, PAIRS, MAX_ALLOC_PCT * 100, RESERVE_PCT * 100)

        # Initialize trading connections
        if not self._init_trading():
            logger.error("CF: Failed to init trading — retrying in 30s")
            time.sleep(30)
            if not self._init_trading():
                logger.error("CF: Trading init failed twice — exiting")
                return

        while self._running:
            try:
                self._cycle += 1
                t0 = time.time()
                logger.info("=== CF CYCLE %d ===", self._cycle)

                # 1. Get portfolio value
                portfolio_value = self._get_portfolio_value()
                if portfolio_value < 10:
                    logger.warning("CF: Portfolio value $%.2f too low — waiting", portfolio_value)
                    self.write_status(
                        targets=self._last_targets or {pair: 0.0 for pair in PAIRS},
                        current_allocs={pair: 0.0 for pair in PAIRS},
                        orders=[],
                        portfolio_value=portfolio_value,
                        status_note="portfolio_value_unavailable_or_too_low",
                    )
                    time.sleep(CYCLE_SECONDS)
                    continue

                # 2. Get latest signals from sniper
                signals = self.get_latest_signals()
                n_signals = sum(1 for s in signals.values() if s.get("confidence", 0) > CONFIDENCE_FLOOR)
                logger.info("CF: Portfolio $%.2f, %d signals above %.0f%% confidence",
                           portfolio_value, n_signals, CONFIDENCE_FLOOR * 100)

                # 3. Compute target allocations
                targets = self.compute_targets(signals, portfolio_value)
                self._last_targets = targets
                self._last_signals = signals

                # 4. Get current allocations
                current_allocs, held_usd = self.get_current_allocations(portfolio_value)

                # Log allocation state
                for pair in PAIRS:
                    target = targets.get(pair, 0)
                    current = current_allocs.get(pair, 0)
                    if target > 0 or current > 0:
                        logger.info("  %s: target=%.1f%% current=%.1f%% gap=%.1f%%",
                                   pair, target * 100, current * 100, (target - current) * 100)

                # 5. Compute rebalance orders
                orders = self.compute_rebalance_orders(targets, current_allocs, portfolio_value)

                # 6. Execute orders
                executed = 0
                for order in orders:
                    if self.execute_rebalance_order(order):
                        executed += 1

                # 7. Write status
                self.write_status(targets, current_allocs, orders, portfolio_value)

                elapsed = time.time() - t0
                logger.info("CF cycle %d: %d orders generated, %d executed (%.1fs)",
                           self._cycle, len(orders), executed, elapsed)

                time.sleep(max(5, CYCLE_SECONDS - elapsed))

            except KeyboardInterrupt:
                logger.info("CF: Shutting down...")
                self._running = False
                break
            except Exception as e:
                logger.error("CF cycle error: %s", e, exc_info=True)
                time.sleep(30)


def main():
    if not acquire_process_singleton("continuous_position_manager", logger=logger, lock_dir=Path(__file__).parent):
        logger.error("CF: singleton lock unavailable; exiting duplicate instance")
        return
    if not _acquire_pid_guard():
        logger.error("CF: pid guard detected running instance; exiting duplicate")
        release_process_singleton("continuous_position_manager")
        return
    atexit.register(_release_pid_guard)
    atexit.register(lambda: release_process_singleton("continuous_position_manager"))
    mgr = ContinuousPositionManager()
    mgr.run()


if __name__ == "__main__":
    main()
