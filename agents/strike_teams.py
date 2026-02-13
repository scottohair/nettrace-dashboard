#!/usr/bin/env python3
"""Financial Strike Teams — autonomous rapid-gain trading squads.

Each strike team is a self-contained unit with:
  - Scout: finds opportunities (market scan, anomaly detection, cross-venue)
  - Analyst: validates signals (multi-timeframe, correlation, risk/reward)
  - Executor: places and manages orders (routing, sizing, execution quality)

Teams compete for capital allocation based on performance (evolutionary).
Top performers get more capital. Underperformers get fired and replaced.

Strike Team Types:
  1. MomentumStrike — catches fast moves (1-5 min, volume/EMA breakouts)
  2. ArbitrageStrike — cross-venue price gaps (CEX-CEX, CEX-DEX)
  3. MeanReversionStrike — statistical deviation plays (z-score > 2)
  4. BreakoutStrike — support/resistance breaks with volume confirmation
  5. CorrelationStrike — inter-asset correlation breakdown trades

Design: stateless, ephemeral, no persistent DB per team. All results feed
back to KPI tracker and agent_goals for evolutionary management.
"""

import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from threading import Thread, Event

sys.path.insert(0, str(Path(__file__).parent))

logger = logging.getLogger("strike_teams")

MAKER_FEE_RATE = float(os.environ.get("STRIKE_MAKER_FEE_RATE", "0.004") or 0.004)
TAKER_FEE_RATE = float(os.environ.get("STRIKE_TAKER_FEE_RATE", "0.006") or 0.006)
EXIT_BUFFER_RATE = float(os.environ.get("STRIKE_EXIT_BUFFER_RATE", "0.0025") or 0.0025)
MIN_SCAN_INTERVAL_SECONDS = int(os.environ.get("STRIKE_MIN_SCAN_INTERVAL_SECONDS", "90") or 90)
DEFAULT_HF_INTERVAL_SECONDS = int(os.environ.get("STRIKE_DEFAULT_HF_INTERVAL_SECONDS", "120") or 120)
DEFAULT_LF_INTERVAL_SECONDS = int(os.environ.get("STRIKE_DEFAULT_LF_INTERVAL_SECONDS", "240") or 240)
SELL_CLOSE_TARGET_RATE = float(os.environ.get("STRIKE_SELL_CLOSE_TARGET_RATE", "0.70") or 0.70)
SELL_CLOSE_MIN_OBS = int(os.environ.get("STRIKE_SELL_CLOSE_MIN_OBS", "4") or 4)
SELL_CLOSE_WINDOW = int(os.environ.get("STRIKE_SELL_CLOSE_WINDOW", "24") or 24)
BUY_THROTTLE_ON_SELL_GAP = os.environ.get("STRIKE_BUY_THROTTLE_ON_SELL_GAP", "1").lower() not in ("0", "false", "no")
EXECUTION_HEALTH_GATE = os.environ.get("STRIKE_EXECUTION_HEALTH_GATE", "1").lower() not in ("0", "false", "no")
EXECUTION_HEALTH_BUY_ONLY = os.environ.get("STRIKE_EXECUTION_HEALTH_BUY_ONLY", "1").lower() not in ("0", "false", "no")
EXECUTION_HEALTH_CACHE_SECONDS = int(os.environ.get("STRIKE_EXECUTION_HEALTH_CACHE_SECONDS", "45") or 45)

# Core imports
try:
    from agent_goals import GoalValidator
    _goals = GoalValidator()
except ImportError:
    _goals = None

try:
    from kpi_tracker import get_kpi_tracker
    _kpi = get_kpi_tracker()
except Exception:
    _kpi = None

try:
    from risk_controller import get_controller
    _risk = get_controller()
except Exception:
    _risk = None

try:
    from execution_health import evaluate_execution_health
except Exception:
    try:
        from agents.execution_health import evaluate_execution_health  # type: ignore
    except Exception:
        evaluate_execution_health = None  # type: ignore


_EXECUTION_HEALTH_CACHE = {"ts": 0.0, "payload": {"green": False, "reason": "uninitialized"}}


def _fetch_price(pair):
    """Get spot price from Coinbase."""
    try:
        dp = pair.replace("-USDC", "-USD")
        url = f"https://api.coinbase.com/v2/prices/{dp}/spot"
        req = urllib.request.Request(url, headers={"User-Agent": "StrikeTeam/1.0"})
        resp = urllib.request.urlopen(req, timeout=5)
        return float(json.loads(resp.read())["data"]["amount"])
    except Exception:
        return None


def _fetch_candles(pair, granularity=60, limit=30):
    """Get recent 1-min candles."""
    try:
        dp = pair.replace("-USDC", "-USD")
        url = f"https://api.exchange.coinbase.com/products/{dp}/candles?granularity={granularity}"
        req = urllib.request.Request(url, headers={"User-Agent": "StrikeTeam/1.0"})
        resp = urllib.request.urlopen(req, timeout=5)
        candles = json.loads(resp.read())
        return candles[:limit]  # [time, low, high, open, close, volume]
    except Exception:
        return []


def _execution_health_status(force_refresh=False):
    if not EXECUTION_HEALTH_GATE:
        return {"green": True, "reason": "gate_disabled"}
    fn = evaluate_execution_health
    if fn is None:
        return {"green": False, "reason": "execution_health_module_unavailable"}
    now = time.time()
    ttl = max(5, int(EXECUTION_HEALTH_CACHE_SECONDS))
    if not force_refresh and (now - float(_EXECUTION_HEALTH_CACHE.get("ts", 0.0) or 0.0)) <= ttl:
        cached = _EXECUTION_HEALTH_CACHE.get("payload", {})
        if isinstance(cached, dict) and cached:
            return cached
    try:
        payload = fn(refresh=True, probe_http=False, write_status=True)
    except Exception as e:
        payload = {"green": False, "reason": f"execution_health_exception:{e}"}
    if not isinstance(payload, dict):
        payload = {"green": False, "reason": "execution_health_invalid_payload"}
    _EXECUTION_HEALTH_CACHE["ts"] = now
    _EXECUTION_HEALTH_CACHE["payload"] = payload
    return payload


class StrikeTeam:
    """Base class for all strike teams."""

    name = "base"
    team_type = "LF"
    pairs = ["BTC-USD", "ETH-USD", "SOL-USD"]

    def __init__(self):
        self.running = False
        self._stop = Event()
        self.scan_count = 0
        self.signals_generated = 0
        self.trades_executed = 0
        self.total_pnl = 0.0
        self.buy_throttled = 0
        self.exec_health_blocked = 0
        self.sell_attempted = 0
        self.sell_completed = 0
        self.sell_failed = 0
        self._sell_close_recent = []

    def _sell_completion_rate(self):
        if not self._sell_close_recent:
            return 1.0
        wins = sum(1 for x in self._sell_close_recent if bool(x))
        return float(wins) / float(len(self._sell_close_recent))

    def _record_sell_completion(self, completed):
        ok = bool(completed)
        self.sell_attempted += 1
        if ok:
            self.sell_completed += 1
        else:
            self.sell_failed += 1
        self._sell_close_recent.append(ok)
        max_window = max(1, int(SELL_CLOSE_WINDOW))
        if len(self._sell_close_recent) > max_window:
            self._sell_close_recent = self._sell_close_recent[-max_window:]

    def _buy_throttle_active(self):
        if not BUY_THROTTLE_ON_SELL_GAP:
            return False
        obs = len(self._sell_close_recent)
        if obs < max(1, int(SELL_CLOSE_MIN_OBS)):
            return False
        return self._sell_completion_rate() < float(SELL_CLOSE_TARGET_RATE)

    def scout(self, pair):
        """Find opportunities. Override in subclass.
        Returns: {"signal": True/False, "direction": "BUY"/"SELL",
                  "confidence": 0-1, "reason": "..."}
        """
        return {"signal": False}

    def analyze(self, pair, scout_result):
        """Validate and refine a scout signal. Override in subclass.
        Returns: {"approved": True/False, "size_usd": float,
                  "entry_price": float, "stop_loss": float, "take_profit": float}
        """
        return {"approved": False}

    def execute(self, pair, analysis):
        """Execute a validated trade. Uses exchange_connector."""
        if not analysis.get("approved"):
            return None

        direction = analysis.get("direction", "BUY")
        size_usd = analysis.get("size_usd", 0)
        price = analysis.get("entry_price", 0)

        if size_usd < 1.0 or price <= 0:
            return None
        if EXECUTION_HEALTH_GATE:
            require_gate = True
            if EXECUTION_HEALTH_BUY_ONLY and str(direction).upper() != "BUY":
                require_gate = False
            if require_gate:
                exec_health = _execution_health_status()
                if not bool(exec_health.get("green", False)):
                    self.exec_health_blocked += 1
                    logger.info(
                        "STRIKE %s: blocked %s %s due to execution health gate (%s)",
                        self.name,
                        direction,
                        pair,
                        str(exec_health.get("reason", "unknown")),
                    )
                    return None
        if str(direction).upper() == "BUY" and self._buy_throttle_active():
            self.buy_throttled += 1
            logger.info(
                "STRIKE %s: BUY throttled on %s (sell_close_rate=%.2f%% target=%.2f%% obs=%d)",
                self.name,
                pair,
                self._sell_completion_rate() * 100.0,
                float(SELL_CLOSE_TARGET_RATE) * 100.0,
                len(self._sell_close_recent),
            )
            return None

        exit_ok, exit_reason = self.validate_profitable_exit(
            pair,
            entry_price=price,
            direction=direction,
            analysis=analysis,
        )
        if not exit_ok:
            logger.info(
                "STRIKE %s: blocked %s %s before execution (exit validation: %s)",
                self.name,
                direction,
                pair,
                exit_reason,
            )
            return None

        # Gate through GoalValidator
        confidence = analysis.get("confidence", 0)
        confirming = analysis.get("confirming_signals", 1)
        regime = analysis.get("regime", "neutral")

        if _goals and not _goals.should_trade(confidence, confirming, direction, regime):
            logger.debug("%s: GoalValidator blocked %s %s (conf=%.2f)",
                         self.name, direction, pair, confidence)
            return None

        # Gate through risk controller
        if _risk:
            # Get FULL portfolio value (cash + positions) for proper risk sizing
            try:
                import urllib.request as _ur
                from exchange_connector import CoinbaseTrader
                _t = CoinbaseTrader()
                accts = _t._request("GET", "/api/v3/brokerage/accounts?limit=250")
                portfolio = 0
                for a in accts.get("accounts", []):
                    cur = a.get("currency", "")
                    bal = float(a.get("available_balance", {}).get("value", 0))
                    if cur in ("USD", "USDC"):
                        portfolio += bal
                    elif bal > 0:
                        try:
                            _url = f"https://api.coinbase.com/v2/prices/{cur}-USD/spot"
                            _req = _ur.Request(_url, headers={"User-Agent": "Strike/1.0"})
                            _resp = _ur.urlopen(_req, timeout=3)
                            _price = float(json.loads(_resp.read())["data"]["amount"])
                            portfolio += bal * _price
                        except Exception:
                            pass
            except Exception:
                portfolio = 250.0  # conservative fallback

            approved, reason, adj_size = _risk.approve_trade(
                self.name, pair, direction, size_usd, portfolio
            )
            if not approved:
                logger.debug("%s: Risk blocked %s %s: %s", self.name, direction, pair, reason)
                return None
            size_usd = adj_size

        # Execute via exchange connector
        try:
            from exchange_connector import CoinbaseTrader
            trader = CoinbaseTrader()
            amount = size_usd / price
            result = trader.place_order(pair, direction, amount)
            order_id = None
            if isinstance(result, dict):
                if isinstance(result.get("success_response"), dict):
                    order_id = result["success_response"].get("order_id")
                elif result.get("order_id"):
                    order_id = result.get("order_id")

            if str(direction).upper() == "SELL":
                filled = False
                if order_id:
                    try:
                        fill = trader.get_order_fill(order_id, max_wait=2, poll_interval=0.4)
                    except Exception:
                        fill = None
                    status_u = str((fill or {}).get("status", "")).upper()
                    filled_sz = float((fill or {}).get("filled_size", 0.0) or 0.0)
                    filled = status_u == "FILLED" or filled_sz > 0.0
                self._record_sell_completion(filled)

            if result and "success_response" in result:
                self.trades_executed += 1
                logger.info("STRIKE %s: %s %s $%.2f FILLED", self.name, direction, pair, size_usd)

                # Record to KPI
                if _kpi:
                    _kpi.record_trade(
                        strategy_name=f"strike_{self.name}", pair=pair,
                        direction=direction, amount_usd=size_usd,
                        pnl=0, fees=size_usd * 0.004,
                        strategy_type=self.team_type, won=True,
                    )

                # Resolve allocation
                if _risk:
                    _risk.resolve_allocation(self.name, pair)

                return result
            else:
                if _risk:
                    _risk.resolve_allocation(self.name, pair)
                return None

        except Exception as e:
            logger.error("STRIKE %s execute error: %s", self.name, e)
            if _risk:
                _risk.resolve_allocation(self.name, pair)
            return None

    def _required_exit_price(self, entry_price):
        """Return minimum profitable exit price including fees and safety buffer."""
        gross_cost = (2.0 * TAKER_FEE_RATE) + EXIT_BUFFER_RATE
        return float(entry_price) * (1.0 + gross_cost)

    def validate_profitable_exit(self, pair, entry_price, direction, analysis=None):
        """Hard gate for BUY orders: exit must clear costs with margin."""
        side = str(direction or "").upper()
        if side != "BUY":
            return True, "sell_or_non_buy_direction"
        if not entry_price or float(entry_price) <= 0:
            return False, "invalid_entry_price"

        payload = analysis if isinstance(analysis, dict) else {}
        required_exit = self._required_exit_price(float(entry_price))
        explicit_target = 0.0
        for key in ("take_profit", "target_exit_price", "expected_exit_price"):
            val = payload.get(key)
            try:
                explicit_target = max(explicit_target, float(val or 0.0))
            except Exception:
                continue
        if explicit_target > 0.0:
            if explicit_target > required_exit:
                return True, f"explicit_target_ok:{explicit_target:.8f}>{required_exit:.8f}"
            return False, f"explicit_target_below_required:{explicit_target:.8f}<={required_exit:.8f}"

        # No explicit target: infer from microstructure + support/momentum.
        candles = _fetch_candles(pair, granularity=300, limit=30)
        if len(candles) < 12:
            return False, "insufficient_candles_for_exit_validation"

        highs = [float(c[2]) for c in candles[1:16]]
        lows = [float(c[1]) for c in candles[1:16]]
        closes = [float(c[4]) for c in candles[:8]]
        support = min(lows) if lows else 0.0
        inferred_exit = max(highs) if highs else 0.0
        current = float(entry_price)

        near_support = support > 0.0 and ((current - support) / support) <= 0.015
        momentum_up = len(closes) >= 4 and closes[0] > closes[1] > closes[2]
        if inferred_exit <= required_exit:
            return False, f"inferred_exit_below_required:{inferred_exit:.8f}<={required_exit:.8f}"
        if not (near_support or momentum_up):
            return False, "no_support_or_momentum_confirmation"
        return True, f"inferred_exit_ok:{inferred_exit:.8f}>{required_exit:.8f}"

    def has_exit_path(self, pair, entry_price, direction, analysis=None):
        """Backward-compatible bool wrapper around strict exit validation."""
        ok, _reason = self.validate_profitable_exit(pair, entry_price, direction, analysis=analysis)
        return bool(ok)

    def run(self, interval=60):
        """Main loop: scout → analyze → exit-validate → execute.

        Interval floor prevents over-trading and cash drain.
        """
        self.running = True
        interval = max(int(MIN_SCAN_INTERVAL_SECONDS), int(interval))
        logger.info("Strike team '%s' starting (type=%s, pairs=%s, interval=%ds)",
                     self.name, self.team_type, self.pairs, interval)

        while self.running and not self._stop.is_set():
            for pair in self.pairs:
                try:
                    self.scan_count += 1

                    # Scout
                    scout_result = self.scout(pair)
                    if not scout_result.get("signal"):
                        continue

                    self.signals_generated += 1
                    logger.info("STRIKE %s: signal on %s — %s (conf=%.2f)",
                                self.name, pair, scout_result.get("reason", ""),
                                scout_result.get("confidence", 0))

                    # Analyze
                    analysis = self.analyze(pair, scout_result)
                    if not analysis.get("approved"):
                        continue

                    # Exit path validation: ONLY buy if profitable exit exists
                    direction = analysis.get("direction", "BUY")
                    entry_price = analysis.get("entry_price", 0)
                    exit_ok, exit_reason = self.validate_profitable_exit(
                        pair, entry_price, direction, analysis=analysis
                    )
                    if not exit_ok:
                        logger.info(
                            "STRIKE %s: no profitable exit path for %s %s — SKIP (%s)",
                            self.name,
                            direction,
                            pair,
                            exit_reason,
                        )
                        continue

                    # Execute
                    self.execute(pair, analysis)

                except Exception as e:
                    logger.error("Strike %s scan error on %s: %s", self.name, pair, e)

            self._stop.wait(interval)

    def stop(self):
        self.running = False
        self._stop.set()

    def status(self):
        return {
            "name": self.name,
            "type": self.team_type,
            "scans": self.scan_count,
            "signals": self.signals_generated,
            "trades": self.trades_executed,
            "pnl": round(self.total_pnl, 4),
            "buy_throttled": int(self.buy_throttled),
            "exec_health_blocked": int(self.exec_health_blocked),
            "sell_attempted": int(self.sell_attempted),
            "sell_completed": int(self.sell_completed),
            "sell_failed": int(self.sell_failed),
            "sell_close_completion_rate": round(self._sell_completion_rate(), 4),
        }


class MomentumStrike(StrikeTeam):
    """Catches fast momentum moves using price velocity and volume spikes."""

    name = "momentum"
    team_type = "HF"
    pairs = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD"]

    def scout(self, pair):
        candles = _fetch_candles(pair, granularity=60, limit=15)
        if len(candles) < 10:
            return {"signal": False}

        # Candles: [time, low, high, open, close, volume]
        closes = [c[4] for c in candles]
        volumes = [c[5] for c in candles]

        # Price velocity: last 3 candles vs previous 7
        recent = closes[:3]
        baseline = closes[3:10]

        if not recent or not baseline:
            return {"signal": False}

        recent_avg = sum(recent) / len(recent)
        baseline_avg = sum(baseline) / len(baseline)
        velocity = (recent_avg - baseline_avg) / baseline_avg if baseline_avg else 0

        # Volume spike: recent vs baseline
        recent_vol = sum(volumes[:3])
        baseline_vol = sum(volumes[3:10]) / max(len(volumes[3:10]), 1) * 3
        vol_spike = recent_vol / baseline_vol if baseline_vol > 0 else 1

        # Signal: price moving + volume confirming
        if abs(velocity) > 0.002 and vol_spike > 1.5:
            direction = "BUY" if velocity > 0 else "SELL"
            confidence = min(0.95, 0.60 + abs(velocity) * 10 + (vol_spike - 1) * 0.1)
            return {
                "signal": True,
                "direction": direction,
                "confidence": confidence,
                "reason": f"momentum v={velocity:.4f} vol_spike={vol_spike:.1f}x",
                "velocity": velocity,
                "vol_spike": vol_spike,
            }

        return {"signal": False}

    def analyze(self, pair, scout_result):
        price = _fetch_price(pair)
        if not price:
            return {"approved": False}

        confidence = scout_result.get("confidence", 0)
        direction = scout_result.get("direction", "BUY")

        # Only BUY on upward momentum, only SELL on downward
        # Size dynamically
        size = 3.0

        return {
            "approved": confidence >= 0.72,
            "direction": direction,
            "size_usd": size,
            "entry_price": price,
            "confidence": confidence,
            "confirming_signals": 2,
            "regime": "neutral",
        }


class ArbitrageStrike(StrikeTeam):
    """Cross-venue price gap detection."""

    name = "arbitrage"
    team_type = "HF"
    pairs = ["BTC-USD", "ETH-USD"]

    def scout(self, pair):
        # Get Coinbase price
        cb_price = _fetch_price(pair)
        if not cb_price:
            return {"signal": False}

        # Compare with other exchanges via CoinGecko
        try:
            asset = pair.split("-")[0].lower()
            cg_map = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana"}
            cg_id = cg_map.get(asset)
            if not cg_id:
                return {"signal": False}

            url = f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd"
            req = urllib.request.Request(url, headers={"User-Agent": "StrikeTeam/1.0"})
            resp = urllib.request.urlopen(req, timeout=5)
            data = json.loads(resp.read())
            cg_price = data.get(cg_id, {}).get("usd")

            if cg_price and cg_price > 0:
                spread = (cb_price - cg_price) / cg_price
                # Need > 0.8% spread to cover 2x maker fees
                if abs(spread) > 0.008:
                    direction = "SELL" if spread > 0 else "BUY"
                    confidence = min(0.95, 0.70 + abs(spread) * 5)
                    return {
                        "signal": True,
                        "direction": direction,
                        "confidence": confidence,
                        "reason": f"arb spread={spread:.4f} cb={cb_price:.2f} mkt={cg_price:.2f}",
                    }
        except Exception:
            pass

        return {"signal": False}

    def analyze(self, pair, scout_result):
        price = _fetch_price(pair)
        if not price:
            return {"approved": False}

        return {
            "approved": scout_result.get("confidence", 0) >= 0.75,
            "direction": scout_result.get("direction", "BUY"),
            "size_usd": 3.0,
            "entry_price": price,
            "confidence": scout_result.get("confidence", 0),
            "confirming_signals": 2,
            "regime": "neutral",
        }


class MeanReversionStrike(StrikeTeam):
    """Statistical mean reversion on z-score deviation."""

    name = "mean_reversion"
    team_type = "LF"
    pairs = ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD"]

    def scout(self, pair):
        candles = _fetch_candles(pair, granularity=300, limit=50)
        if len(candles) < 20:
            return {"signal": False}

        closes = [c[4] for c in candles]
        mean = sum(closes) / len(closes)
        std = (sum((c - mean) ** 2 for c in closes) / len(closes)) ** 0.5

        if std <= 0:
            return {"signal": False}

        current = closes[0]
        z_score = (current - mean) / std

        # z > 2: overbought (sell), z < -2: oversold (buy)
        if abs(z_score) > 2.0:
            direction = "BUY" if z_score < -2 else "SELL"
            confidence = min(0.95, 0.65 + (abs(z_score) - 2) * 0.15)
            return {
                "signal": True,
                "direction": direction,
                "confidence": confidence,
                "reason": f"z-score={z_score:.2f} mean={mean:.2f} current={current:.2f}",
            }

        return {"signal": False}

    def analyze(self, pair, scout_result):
        price = _fetch_price(pair)
        if not price:
            return {"approved": False}

        return {
            "approved": scout_result.get("confidence", 0) >= 0.72,
            "direction": scout_result.get("direction", "BUY"),
            "size_usd": 3.0,
            "entry_price": price,
            "confidence": scout_result.get("confidence", 0),
            "confirming_signals": 2,
            "regime": "neutral",
        }


class BreakoutStrike(StrikeTeam):
    """Support/resistance breakout with volume confirmation."""

    name = "breakout"
    team_type = "LF"
    pairs = ["BTC-USD", "ETH-USD", "SOL-USD"]

    def scout(self, pair):
        candles = _fetch_candles(pair, granularity=300, limit=30)
        if len(candles) < 20:
            return {"signal": False}

        # Find support/resistance from recent highs/lows
        highs = [c[2] for c in candles[1:]]  # skip most recent
        lows = [c[1] for c in candles[1:]]
        current_close = candles[0][4]
        current_vol = candles[0][5]

        resistance = max(highs)
        support = min(lows)
        avg_vol = sum(c[5] for c in candles[1:]) / len(candles[1:])

        # Breakout above resistance with volume
        if current_close > resistance and current_vol > avg_vol * 1.5:
            pct_above = (current_close - resistance) / resistance
            confidence = min(0.90, 0.70 + pct_above * 10)
            return {
                "signal": True,
                "direction": "BUY",
                "confidence": confidence,
                "reason": f"breakout above R={resistance:.2f}, vol={current_vol/avg_vol:.1f}x",
            }

        # Breakdown below support with volume
        if current_close < support and current_vol > avg_vol * 1.5:
            pct_below = (support - current_close) / support
            confidence = min(0.90, 0.70 + pct_below * 10)
            return {
                "signal": True,
                "direction": "SELL",
                "confidence": confidence,
                "reason": f"breakdown below S={support:.2f}, vol={current_vol/avg_vol:.1f}x",
            }

        return {"signal": False}

    def analyze(self, pair, scout_result):
        price = _fetch_price(pair)
        if not price:
            return {"approved": False}

        return {
            "approved": scout_result.get("confidence", 0) >= 0.72,
            "direction": scout_result.get("direction", "BUY"),
            "size_usd": 3.0,
            "entry_price": price,
            "confidence": scout_result.get("confidence", 0),
            "confirming_signals": 2,
            "regime": "neutral",
        }


class CorrelationStrike(StrikeTeam):
    """Inter-asset correlation breakdown trades.

    When historically correlated assets diverge, trade the convergence.
    """

    name = "correlation"
    team_type = "LF"
    pairs = ["ETH-USD", "SOL-USD"]

    def scout(self, pair):
        # Compare with BTC as the base
        btc_candles = _fetch_candles("BTC-USD", granularity=300, limit=20)
        pair_candles = _fetch_candles(pair, granularity=300, limit=20)

        if len(btc_candles) < 15 or len(pair_candles) < 15:
            return {"signal": False}

        # Calculate returns
        btc_returns = [(btc_candles[i][4] - btc_candles[i+1][4]) / btc_candles[i+1][4]
                       for i in range(min(len(btc_candles), len(pair_candles)) - 1)]
        pair_returns = [(pair_candles[i][4] - pair_candles[i+1][4]) / pair_candles[i+1][4]
                        for i in range(min(len(btc_candles), len(pair_candles)) - 1)]

        if len(btc_returns) < 5:
            return {"signal": False}

        # Simple correlation
        n = len(btc_returns)
        mean_b = sum(btc_returns) / n
        mean_p = sum(pair_returns) / n
        cov = sum((btc_returns[i] - mean_b) * (pair_returns[i] - mean_p) for i in range(n)) / n
        std_b = (sum((r - mean_b) ** 2 for r in btc_returns) / n) ** 0.5
        std_p = (sum((r - mean_p) ** 2 for r in pair_returns) / n) ** 0.5

        if std_b <= 0 or std_p <= 0:
            return {"signal": False}

        correlation = cov / (std_b * std_p)

        # Recent divergence: last 3 returns diverge from correlation
        recent_btc = sum(btc_returns[:3]) / 3
        recent_pair = sum(pair_returns[:3]) / 3

        # If BTC up but pair lagging (and normally correlated), buy the pair
        if correlation > 0.5 and recent_btc > 0.003 and recent_pair < 0:
            confidence = min(0.88, 0.65 + correlation * 0.2)
            return {
                "signal": True,
                "direction": "BUY",
                "confidence": confidence,
                "reason": f"corr={correlation:.2f} BTC↑{recent_btc:.4f} {pair}↓{recent_pair:.4f}",
            }

        # If BTC down but pair still up (and normally correlated), sell the pair
        if correlation > 0.5 and recent_btc < -0.003 and recent_pair > 0:
            confidence = min(0.88, 0.65 + correlation * 0.2)
            return {
                "signal": True,
                "direction": "SELL",
                "confidence": confidence,
                "reason": f"corr={correlation:.2f} BTC↓{recent_btc:.4f} {pair}↑{recent_pair:.4f}",
            }

        return {"signal": False}

    def analyze(self, pair, scout_result):
        price = _fetch_price(pair)
        if not price:
            return {"approved": False}

        return {
            "approved": scout_result.get("confidence", 0) >= 0.72,
            "direction": scout_result.get("direction", "BUY"),
            "size_usd": 3.0,
            "entry_price": price,
            "confidence": scout_result.get("confidence", 0),
            "confirming_signals": 2,
            "regime": "neutral",
        }


# ── Strike Team Manager ────────────────────────────────────────────────────

ALL_TEAMS = [
    MomentumStrike,
    ArbitrageStrike,
    MeanReversionStrike,
    BreakoutStrike,
    CorrelationStrike,
]


class StrikeTeamManager:
    """Manages all strike teams as threads with evolutionary selection."""

    def __init__(self):
        self.teams = {}
        self.threads = {}

    def deploy_all(self):
        """Launch all strike teams as daemon threads."""
        for team_cls in ALL_TEAMS:
            team = team_cls()
            self.teams[team.name] = team
            base_interval = (
                int(DEFAULT_HF_INTERVAL_SECONDS)
                if str(team.team_type).upper() == "HF"
                else int(DEFAULT_LF_INTERVAL_SECONDS)
            )
            team_interval = max(int(MIN_SCAN_INTERVAL_SECONDS), int(base_interval))
            t = Thread(target=team.run, args=(team_interval,), daemon=True, name=f"strike-{team.name}")
            t.start()
            self.threads[team.name] = t
            logger.info("Deployed strike team: %s (type=%s, interval=%ds)", team.name, team.team_type, team_interval)

        logger.info("All %d strike teams deployed", len(self.teams))

    def stop_all(self):
        """Stop all strike teams."""
        for name, team in self.teams.items():
            team.stop()
        self.teams.clear()
        self.threads.clear()

    def status(self):
        """Get status of all teams."""
        return {
            name: {
                **team.status(),
                "alive": self.threads.get(name, None) is not None and self.threads[name].is_alive(),
            }
            for name, team in self.teams.items()
        }

    def rankings(self):
        """Rank teams by performance for capital allocation."""
        stats = []
        for name, team in self.teams.items():
            s = team.status()
            # Score: trades * win_implied * pnl_direction
            score = s["trades"] * max(s["pnl"], 0.01) if s["trades"] > 0 else 0
            stats.append({**s, "score": score})
        return sorted(stats, key=lambda x: x["score"], reverse=True)


# Singleton
_manager = None

def get_strike_manager():
    global _manager
    if _manager is None:
        _manager = StrikeTeamManager()
    return _manager
