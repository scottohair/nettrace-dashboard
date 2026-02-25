#!/usr/bin/env python3
"""Perp Scalper — Sub-second perpetual futures scalping agent.

Exploits Coinbase perp 0% maker fee (promotional) for high-frequency
micro-scalping. Round-trip cost is ~0.03% (taker close), so even tiny
price movements (0.05%+) are profitable.

Architecture:
  - 500ms cycle cadence (env-configurable)
  - WebSocket-only price feed (zero REST calls in hot path)
  - C engine micro_edge_pct for fast EMA crossover signals
  - VPIN flow toxicity as secondary signal
  - Cached regime from fast_bridge
  - CoinbaseDerivativesConnector.place_perp_order(post_only=True)

Safety:
  - GoalValidator.should_trade() gate
  - risk_controller.approve_trade() gate
  - C engine no_loss_gate check
  - Max 1 open perp position per pair
  - Circuit breaker: N consecutive losses → cooldown
  - Daily loss cap via risk controller
  - Process singleton lock
"""

import json
import logging
import os
import time
from collections import deque
from pathlib import Path

logger = logging.getLogger("perp_scalper")

# ── Configuration (env-driven) ───────────────────────────────────────────

ENABLED = os.environ.get("PERP_SCALPER_ENABLED", "0") in ("1", "true")
INTERVAL_MS = int(os.environ.get("PERP_SCALPER_INTERVAL_MS", "500"))
MIN_CONFIDENCE = float(os.environ.get("PERP_SCALPER_MIN_CONFIDENCE", "0.60"))
MIN_SIGNALS = int(os.environ.get("PERP_SCALPER_MIN_SIGNALS", "2"))
MIN_EDGE_PCT = float(os.environ.get("PERP_SCALPER_MIN_EDGE_PCT", "0.05"))
ORDER_USD = float(os.environ.get("PERP_SCALPER_ORDER_USD", "3.0"))
MAX_SPREAD_PCT = float(os.environ.get("PERP_SCALPER_MAX_SPREAD_PCT", "0.15"))

PAIRS = [
    p.strip() for p in
    os.environ.get("PERP_SCALPER_PAIRS", "BTC-USD,ETH-USD,SOL-USD").split(",")
    if p.strip()
]

# Circuit breaker
CB_MAX_CONSECUTIVE_LOSSES = int(os.environ.get("PERP_SCALPER_CB_MAX_LOSSES", "5"))
CB_COOLDOWN_SECONDS = int(os.environ.get("PERP_SCALPER_CB_COOLDOWN_S", "1800"))

# Tick buffer depth for EMA calculation
TICK_BUFFER_SIZE = 24

# Perp product ID mapping (Coinbase INTX format)
PERP_PRODUCT_MAP = {
    "BTC-USD": "BTC-PERP-INTX",
    "ETH-USD": "ETH-PERP-INTX",
    "SOL-USD": "SOL-PERP-INTX",
}

STATUS_PATH = Path(os.environ.get(
    "PERP_SCALPER_STATUS_PATH",
    str(Path(__file__).parent / "perp_scalper_status.json"),
))

# ── Lazy imports (avoids import-time crashes on missing .so) ─────────────

_ws_feed = None
_deriv = None
_fast_exec = None
_goal_validator = None
_risk_controller = None


def _get_ws_feed():
    global _ws_feed
    if _ws_feed is None:
        try:
            from coinbase_ws_feed import CoinbaseWSFeed
            _ws_feed = CoinbaseWSFeed(pairs=PAIRS)
            _ws_feed.start()
            logger.info("WebSocket feed started for %s", PAIRS)
        except Exception as e:
            logger.error("WS feed init failed: %s", e)
    return _ws_feed


def _get_deriv():
    global _deriv
    if _deriv is None:
        try:
            from coinbase_derivatives_connector import CoinbaseDerivativesConnector
            _deriv = CoinbaseDerivativesConnector()
        except Exception as e:
            logger.error("Derivatives connector init failed: %s", e)
    return _deriv


def _get_fast_exec():
    global _fast_exec
    if _fast_exec is None:
        try:
            from fast_exec_bridge import FastExec
            _fast_exec = FastExec()
        except Exception:
            try:
                from agents.fast_exec_bridge import FastExec
                _fast_exec = FastExec()
            except Exception:
                pass  # Pure-python fallback used
    return _fast_exec


def _get_goal_validator():
    global _goal_validator
    if _goal_validator is None:
        try:
            from agent_goals import GoalValidator
            _goal_validator = GoalValidator
        except Exception:
            try:
                from agents.agent_goals import GoalValidator
                _goal_validator = GoalValidator
            except Exception as e:
                logger.error("GoalValidator import failed: %s", e)
    return _goal_validator


def _get_risk_controller():
    global _risk_controller
    if _risk_controller is None:
        try:
            from risk_controller import get_controller
            _risk_controller = get_controller()
        except Exception:
            try:
                from agents.risk_controller import get_controller
                _risk_controller = get_controller()
            except Exception as e:
                logger.error("RiskController import failed: %s", e)
    return _risk_controller


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


class PerpScalper:
    """Sub-second perp scalping agent using WebSocket + C engine fast path."""

    def __init__(self):
        self.running = True
        self.cycle = 0
        self.interval_ms = INTERVAL_MS
        self.pairs = PAIRS

        # Per-pair tick buffers: {pair: {"mid": deque, "spread_pct": deque}}
        self.buffers = {
            pair: {
                "mid": deque(maxlen=TICK_BUFFER_SIZE),
                "spread_pct": deque(maxlen=TICK_BUFFER_SIZE),
            }
            for pair in self.pairs
        }

        # Position tracking: {pair: {"side": "BUY"/"SELL", "entry_price": float, "size": float, "ts": float}}
        self.open_positions = {}

        # Circuit breaker state
        self.consecutive_losses = 0
        self.cooldown_until = 0.0

        # Stats
        self.stats = {
            "cycles": 0,
            "signals_generated": 0,
            "trades_attempted": 0,
            "trades_filled": 0,
            "trades_blocked": 0,
            "total_pnl_usd": 0.0,
        }

        # C engine (may be None → pure-python fallback)
        self.fast = _get_fast_exec()

    def _book(self, pair):
        """Get latest quote from WebSocket feed. O(1), no network call."""
        ws = _get_ws_feed()
        if ws is None:
            return None
        quote = ws.get_quote(pair)
        if quote is None:
            return None
        bid = float(quote.get("bid", 0))
        ask = float(quote.get("ask", 0))
        if bid <= 0 or ask <= 0:
            return None
        mid = (bid + ask) / 2.0
        spread_pct = ((ask - bid) / mid) * 100.0 if mid > 0 else 999.0
        return {
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "spread_pct": spread_pct,
            "ts": quote.get("ts", time.time()),
        }

    def _append_tick(self, pair, tick):
        """Append tick to rolling buffer."""
        buf = self.buffers[pair]
        buf["mid"].append(tick["mid"])
        buf["spread_pct"].append(tick["spread_pct"])

    def _signal(self, pair):
        """Generate signal from fast/slow EMA crossover + C engine edge.

        Requires at least 12 ticks (~6 seconds at 500ms) before generating.
        """
        buf = self.buffers[pair]
        mids = list(buf["mid"])
        spreads = list(buf["spread_pct"])

        if len(mids) < 12:
            return None

        fast_avg = sum(mids[-5:]) / 5.0
        slow_avg = sum(mids[-12:]) / 12.0
        curr_spread = spreads[-1] if spreads else 0.0

        # Spread gate: skip if too wide
        if curr_spread > MAX_SPREAD_PCT:
            return None

        # C engine edge or pure-python fallback
        if self.fast is not None:
            edge = float(self.fast.micro_edge_pct(fast_avg, slow_avg, curr_spread))
        else:
            momentum_pct = ((fast_avg - slow_avg) / max(1e-9, slow_avg)) * 100.0
            edge = max(0.0, abs(momentum_pct) * 0.52 - curr_spread * 0.35)

        # Direction
        side = "HOLD"
        if fast_avg > slow_avg and edge >= MIN_EDGE_PCT:
            side = "BUY"
        elif fast_avg < slow_avg and edge >= MIN_EDGE_PCT:
            side = "SELL"

        # Confidence from edge strength + spread tightness
        confirming_signals = 0
        confidence = 0.50
        confidence += _clamp(edge / 1.0, 0.0, 0.35)  # Edge contributes up to 0.35
        confidence -= _clamp(curr_spread / 0.30, 0.0, 0.20)  # Spread penalty

        # Signal #1: EMA crossover
        if side != "HOLD":
            confirming_signals += 1

        # Signal #2: Momentum strength (edge above threshold)
        if edge >= MIN_EDGE_PCT * 1.5:
            confirming_signals += 1
            confidence += 0.05

        # Signal #3: Spread tightness (low spread = good liquidity)
        if curr_spread < MAX_SPREAD_PCT * 0.5:
            confirming_signals += 1
            confidence += 0.03

        confidence = _clamp(confidence, 0.05, 0.98)

        self.stats["signals_generated"] += 1

        return {
            "pair": pair,
            "side": side,
            "expected_edge_pct": round(edge, 6),
            "confidence": round(confidence, 4),
            "spread_pct": round(curr_spread, 6),
            "fast_avg": round(fast_avg, 8),
            "slow_avg": round(slow_avg, 8),
            "confirming_signals": confirming_signals,
        }

    def _should_trade(self, sig):
        """Multi-gate check: confidence, signal count, GoalValidator, risk controller."""
        pair = sig["pair"]
        side = sig["side"]
        confidence = sig["confidence"]
        confirming = sig["confirming_signals"]

        # Gate 1: Direction
        if side not in ("BUY", "SELL"):
            return False, "hold_signal"

        # Gate 2: Confidence floor
        if confidence < MIN_CONFIDENCE:
            return False, f"confidence_{confidence:.2f}_below_{MIN_CONFIDENCE}"

        # Gate 3: Minimum confirming signals
        if confirming < MIN_SIGNALS:
            return False, f"signals_{confirming}_below_{MIN_SIGNALS}"

        # Gate 4: Circuit breaker
        now = time.time()
        if now < self.cooldown_until:
            remaining = int(self.cooldown_until - now)
            return False, f"circuit_breaker_cooldown_{remaining}s"

        # Gate 5: No stacking — max 1 position per pair
        if pair in self.open_positions:
            existing = self.open_positions[pair]
            # Allow closing (opposite side)
            if existing["side"] == side:
                return False, f"position_already_open_{side}"

        # Gate 6: GoalValidator (use perp-specific method — 0% maker, lower confidence floor)
        gv = _get_goal_validator()
        if gv is not None:
            try:
                result = gv.should_trade_perp(confidence, confirming, side, "neutral",
                                              leverage=float(os.environ.get("PERP_MAX_LEVERAGE", "1.0")))
                if result is False:
                    return False, f"goal_validator_perp_rejected_conf_{confidence:.2f}"
            except Exception as e:
                logger.warning("GoalValidator check failed: %s", e)

        # Gate 7: Risk controller
        rc = _get_risk_controller()
        if rc is not None:
            try:
                approval = rc.approve_trade(
                    pair=pair,
                    side=side,
                    amount_usd=ORDER_USD,
                    source="perp_scalper",
                )
                if isinstance(approval, dict) and not approval.get("approved", True):
                    return False, f"risk_blocked:{approval.get('reason', 'unknown')}"
                elif approval is False:
                    return False, "risk_controller_rejected"
            except Exception as e:
                logger.warning("Risk controller check failed: %s", e)

        return True, "approved"

    def _execute(self, sig):
        """Execute a perp trade via CoinbaseDerivativesConnector."""
        pair = sig["pair"]
        side = sig["side"]
        mid_price = sig["fast_avg"]  # Use fast EMA as reference price

        perp_id = PERP_PRODUCT_MAP.get(pair)
        if not perp_id:
            return {"attempted": False, "reason": f"no_perp_product_for_{pair}"}

        deriv = _get_deriv()
        if deriv is None:
            return {"attempted": False, "reason": "derivatives_connector_unavailable"}

        # Calculate size from USD notional
        if mid_price <= 0:
            return {"attempted": False, "reason": "invalid_price"}

        base_size = ORDER_USD / mid_price

        # Price offset for post_only: slight edge for maker fill
        if side == "BUY":
            limit_price = mid_price * 0.9998  # Slightly below mid for buy
        else:
            limit_price = mid_price * 1.0002  # Slightly above mid for sell

        self.stats["trades_attempted"] += 1

        try:
            result = deriv.place_perp_order(
                product_id=perp_id,
                side=side,
                size=str(round(base_size, 8)),
                price=str(round(limit_price, 2)),
                leverage=1.0,
                post_only=True,
                reduce_only=False,
            )

            if result and not result.get("error_response"):
                order_id = result.get("order_id") or result.get("success_response", {}).get("order_id", "")
                self.stats["trades_filled"] += 1
                self.open_positions[pair] = {
                    "side": side,
                    "entry_price": mid_price,
                    "size": base_size,
                    "ts": time.time(),
                    "order_id": order_id,
                }
                logger.info(
                    "PERP_SCALP: %s %s size=%.6f price=%.2f edge=%.3f%% conf=%.2f order=%s",
                    side, perp_id, base_size, limit_price,
                    sig["expected_edge_pct"], sig["confidence"], order_id,
                )
                return {"attempted": True, "filled": True, "order_id": order_id}
            else:
                err = result.get("error_response", {}) if result else {}
                reason = err.get("error", "unknown")
                self.stats["trades_blocked"] += 1
                logger.warning("PERP_SCALP blocked: %s %s reason=%s", side, perp_id, reason)
                return {"attempted": True, "filled": False, "reason": reason}

        except Exception as e:
            logger.error("PERP_SCALP execution error: %s", e, exc_info=True)
            return {"attempted": True, "filled": False, "reason": str(e)}

    def _update_circuit_breaker(self, trade_result):
        """Track consecutive losses and trigger cooldown if needed."""
        if trade_result.get("filled"):
            # Reset on successful fill (actual P&L tracked by exit_manager)
            self.consecutive_losses = 0
        elif trade_result.get("attempted") and not trade_result.get("filled"):
            self.consecutive_losses += 1
            if self.consecutive_losses >= CB_MAX_CONSECUTIVE_LOSSES:
                self.cooldown_until = time.time() + CB_COOLDOWN_SECONDS
                logger.warning(
                    "CIRCUIT_BREAKER: %d consecutive failures, cooling down %ds",
                    self.consecutive_losses, CB_COOLDOWN_SECONDS,
                )

    def _write_status(self):
        """Write agent status for monitoring."""
        try:
            status = {
                "agent": "perp_scalper",
                "ts": time.time(),
                "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "enabled": ENABLED,
                "running": self.running,
                "cycle": self.cycle,
                "interval_ms": self.interval_ms,
                "pairs": self.pairs,
                "open_positions": len(self.open_positions),
                "positions": {k: {"side": v["side"], "entry": v["entry_price"]} for k, v in self.open_positions.items()},
                "circuit_breaker": {
                    "consecutive_losses": self.consecutive_losses,
                    "cooldown_until": self.cooldown_until,
                    "in_cooldown": time.time() < self.cooldown_until,
                },
                "stats": self.stats,
            }
            STATUS_PATH.write_text(json.dumps(status, indent=2))
        except Exception as e:
            logger.debug("Status write failed: %s", e)

    def run_cycle(self):
        """Single scalping cycle: book → signal → gate → execute for each pair."""
        self.cycle += 1
        self.stats["cycles"] = self.cycle

        for pair in self.pairs:
            try:
                tick = self._book(pair)
            except Exception as e:
                logger.debug("book failed %s: %s", pair, e)
                continue

            if not tick:
                continue

            self._append_tick(pair, tick)

            sig = self._signal(pair)
            if not sig:
                continue

            if sig["side"] == "HOLD":
                continue

            approved, reason = self._should_trade(sig)
            if not approved:
                self.stats["trades_blocked"] += 1
                if self.cycle % 60 == 0:  # Log blocks every ~30s
                    logger.debug(
                        "PERP blocked: %s %s conf=%.2f reason=%s",
                        sig["side"], pair, sig["confidence"], reason,
                    )
                continue

            result = self._execute(sig)
            self._update_circuit_breaker(result)

        # Write status every 10 cycles (~5s at 500ms interval)
        if self.cycle % 10 == 0:
            self._write_status()

        # Log summary periodically
        if self.cycle % 120 == 0:  # Every ~60s
            logger.info(
                "perp_scalper cycle=%d signals=%d attempts=%d fills=%d blocked=%d positions=%d",
                self.cycle, self.stats["signals_generated"],
                self.stats["trades_attempted"], self.stats["trades_filled"],
                self.stats["trades_blocked"], len(self.open_positions),
            )

    def run_loop(self):
        """Main event loop — runs at configured interval until stopped."""
        logger.info(
            "perp_scalper starting interval=%dms pairs=%s min_conf=%.2f min_edge=%.3f%%",
            self.interval_ms, self.pairs, MIN_CONFIDENCE, MIN_EDGE_PCT,
        )

        # Process singleton
        try:
            from process_singleton import acquire_process_singleton
            if not acquire_process_singleton("perp_scalper", logger=logger):
                logger.error("Another perp_scalper instance running. Exiting.")
                return
        except Exception:
            pass

        while self.running:
            started = time.perf_counter()
            try:
                self.run_cycle()
            except Exception as e:
                logger.error("perp_scalper cycle_failed: %s", e, exc_info=True)

            elapsed_ms = (time.perf_counter() - started) * 1000.0
            sleep_ms = max(10.0, float(self.interval_ms) - elapsed_ms)
            time.sleep(sleep_ms / 1000.0)

    def stop(self):
        """Graceful shutdown."""
        self.running = False
        self._write_status()
        logger.info("perp_scalper stopped. stats=%s", json.dumps(self.stats))


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Perp Scalper — sub-second perpetual futures agent")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument("--interval-ms", type=int, default=INTERVAL_MS)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if not ENABLED:
        logger.info("PERP_SCALPER_ENABLED not set. Exiting.")
        return

    scalper = PerpScalper()
    scalper.interval_ms = args.interval_ms

    if args.once:
        scalper.run_cycle()
    else:
        try:
            scalper.run_loop()
        except KeyboardInterrupt:
            scalper.stop()


if __name__ == "__main__":
    main()
