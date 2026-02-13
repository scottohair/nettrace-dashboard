#!/usr/bin/env python3
"""HF execution lane (sub-second scanner + native fast-path scoring).

This is a low-latency signal/execution lane, not exchange-colocated HFT.
It improves cadence and decision speed while preserving strict no-loss gating.

Default mode is paper execution. Live trading requires:
  HF_EXECUTE_LIVE=1 and valid Coinbase credentials.
"""

import argparse
import collections
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

try:
    from fast_exec_bridge import FastExec
except Exception:
    try:
        from agents.fast_exec_bridge import FastExec  # type: ignore
    except Exception:
        FastExec = None  # type: ignore

try:
    from no_loss_policy import evaluate_trade as evaluate_no_loss_trade, log_decision as log_no_loss_decision
except Exception:
    try:
        from agents.no_loss_policy import evaluate_trade as evaluate_no_loss_trade, log_decision as log_no_loss_decision  # type: ignore
    except Exception:
        def evaluate_no_loss_trade(**kwargs):
            payload = dict(kwargs)
            payload["approved"] = True
            payload["reason"] = "no_loss_policy_unavailable"
            return payload

        def log_no_loss_decision(*_args, **_kwargs):
            return None

try:
    from exchange_connector import CoinbaseTrader
except Exception:
    try:
        from agents.exchange_connector import CoinbaseTrader  # type: ignore
    except Exception:
        CoinbaseTrader = None  # type: ignore

try:
    from low_latency_connector import LowLatencyConnector
except Exception:
    try:
        from agents.low_latency_connector import LowLatencyConnector  # type: ignore
    except Exception:
        LowLatencyConnector = None  # type: ignore

BASE = Path(__file__).parent
STATELESS_MODE = os.environ.get("HF_STATELESS_MODE", "1").lower() not in {"0", "false", "no"}
RUNTIME_DIR = Path(os.environ.get("HF_RUNTIME_DIR", "/tmp/quant_runtime"))
if STATELESS_MODE:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = None if STATELESS_MODE else (BASE / "hf_execution.db")
STATUS_PATH = (RUNTIME_DIR / "hf_execution_status.json") if STATELESS_MODE else (BASE / "hf_execution_status.json")

DEFAULT_INTERVAL_MS = int(os.environ.get("HF_INTERVAL_MS", "900"))
DEFAULT_EXECUTE_LIVE = os.environ.get("HF_EXECUTE_LIVE", "0").lower() in {"1", "true", "yes"}
DEFAULT_ORDER_USD = float(os.environ.get("HF_ORDER_USD", "3.0"))
DEFAULT_MIN_EDGE_PCT = float(os.environ.get("HF_MIN_EDGE_PCT", "0.22"))
DEFAULT_MIN_CONFIDENCE = float(os.environ.get("HF_MIN_CONFIDENCE", "0.72"))
DEFAULT_MAX_SPREAD_PCT = float(os.environ.get("HF_MAX_SPREAD_PCT", "0.18"))
DEFAULT_LOOKBACK = int(os.environ.get("HF_LOOKBACK", "48"))
DEFAULT_MIN_COINBASE_SUCCESS_RATE = float(os.environ.get("HF_MIN_COINBASE_SUCCESS_RATE", "0.30"))
DEFAULT_MAX_COINBASE_P90_MS = float(os.environ.get("HF_MAX_COINBASE_P90_MS", "1200"))
DEFAULT_PAIRS = [p.strip().upper() for p in os.environ.get("HF_PAIRS", "BTC-USD,ETH-USD,SOL-USD").split(",") if p.strip()]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [hf_execution] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE / "hf_execution.log")),
    ],
)
logger = logging.getLogger("hf_execution")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


class HFExecutionAgent:
    def __init__(self, pairs=None, interval_ms=DEFAULT_INTERVAL_MS, execute_live=DEFAULT_EXECUTE_LIVE):
        self.pairs = list(pairs or DEFAULT_PAIRS)
        self.interval_ms = max(250, int(interval_ms))
        self.execute_live = bool(execute_live)
        self.running = True
        self.cycle = 0
        self.buffers = {
            pair: {
                "mid": collections.deque(maxlen=DEFAULT_LOOKBACK),
                "spread_pct": collections.deque(maxlen=DEFAULT_LOOKBACK),
            }
            for pair in self.pairs
        }
        self.fast = None
        if FastExec is not None:
            try:
                self.fast = FastExec()
            except Exception:
                self.fast = None
        self._last_live_readiness = {
            "ready": False,
            "route": "paper",
            "reason": "init",
            "checked_at": _now_iso(),
            "details": {},
        }

        self.connector = None
        if LowLatencyConnector is not None:
            try:
                self.connector = LowLatencyConnector(
                    prefer="coinbase",
                    allow_ibkr=self.execute_live,
                    allow_fix_stub=True,
                )
            except Exception:
                self.connector = None

        self.trader = None
        if self.connector is None and self.execute_live and CoinbaseTrader is not None:
            try:
                self.trader = CoinbaseTrader()
            except Exception:
                self.trader = None

        db_target = ":memory:" if DB_PATH is None else str(DB_PATH)
        self.db = sqlite3.connect(db_target, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self._init_db()

    def _live_readiness(self):
        if not self.execute_live:
            return {
                "ready": False,
                "route": "paper",
                "reason": "execute_live_disabled",
                "checked_at": _now_iso(),
                "details": {},
            }

        details = {}
        if self.connector is None and self.trader is None:
            return {
                "ready": False,
                "route": "paper",
                "reason": "no_live_connector",
                "checked_at": _now_iso(),
                "details": details,
            }

        if self.connector is None:
            return {
                "ready": bool(self.trader is not None),
                "route": "coinbase",
                "reason": "legacy_coinbase_trader" if self.trader is not None else "legacy_trader_missing",
                "checked_at": _now_iso(),
                "details": details,
            }

        try:
            health = self.connector.health()
        except Exception as e:
            return {
                "ready": False,
                "route": "paper",
                "reason": f"health_probe_failed:{e}",
                "checked_at": _now_iso(),
                "details": details,
            }

        details["connector_health"] = health
        cb = ((health.get("telemetry", {}) or {}).get("coinbase", {}) or {})
        cb_samples = int(cb.get("samples", 0) or 0)
        cb_success = float(cb.get("success_rate", 0.0) or 0.0)
        cb_p90 = float(cb.get("p90_latency_ms", 0.0) or 0.0)

        cb_dns_ok = bool(health.get("coinbase_dns_ok", False))
        cb_auth = bool(health.get("coinbase_auth_present", False))
        cb_trade_ready = bool(health.get("coinbase_trade_ready", False))
        cb_healthy = bool(cb_samples < 10 or cb_success >= DEFAULT_MIN_COINBASE_SUCCESS_RATE)
        cb_latency_ok = bool(cb_p90 <= 0 or cb_p90 <= DEFAULT_MAX_COINBASE_P90_MS)
        fix_ready = bool(health.get("fix_ready", False))

        details["coinbase_gate"] = {
            "dns_ok": cb_dns_ok,
            "auth_present": cb_auth,
            "trade_ready": cb_trade_ready,
            "telemetry_ok": cb_healthy,
            "latency_ok": cb_latency_ok,
            "samples": cb_samples,
            "success_rate": round(cb_success, 4),
            "p90_latency_ms": round(cb_p90, 3),
            "required_success_rate": DEFAULT_MIN_COINBASE_SUCCESS_RATE,
            "max_p90_latency_ms": DEFAULT_MAX_COINBASE_P90_MS,
        }

        if cb_dns_ok and cb_auth and cb_trade_ready and cb_healthy and cb_latency_ok:
            return {
                "ready": True,
                "route": "coinbase",
                "reason": "coinbase_ready",
                "checked_at": _now_iso(),
                "details": details,
            }

        if fix_ready:
            return {
                "ready": True,
                "route": "fix",
                "reason": "coinbase_gate_failed_using_fix",
                "checked_at": _now_iso(),
                "details": details,
            }

        blockers = []
        if not cb_dns_ok:
            blockers.append("dns")
        if not cb_auth:
            blockers.append("coinbase_auth")
        if not cb_trade_ready:
            blockers.append("coinbase_trade")
        if not cb_healthy:
            blockers.append("coinbase_success_rate")
        if not cb_latency_ok:
            blockers.append("coinbase_latency")
        if not fix_ready:
            blockers.append("fix_unavailable")

        return {
            "ready": False,
            "route": "paper",
            "reason": "gate_failed:" + ",".join(blockers),
            "checked_at": _now_iso(),
            "details": details,
        }

    def _init_db(self):
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS hf_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                bid REAL,
                ask REAL,
                mid REAL,
                spread_pct REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS hf_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                side TEXT NOT NULL,
                expected_edge_pct REAL,
                confidence REAL,
                spread_pct REAL,
                policy_approved INTEGER,
                policy_reason TEXT,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS hf_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                side TEXT NOT NULL,
                mode TEXT,
                amount_usd REAL,
                success INTEGER,
                order_id TEXT,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self.db.commit()

    def _book(self, pair):
        if self.connector is not None:
            try:
                q = self.connector.quote_crypto(pair)
            except Exception:
                q = None
            if isinstance(q, dict) and "error" not in q:
                bid = float(q.get("bid", 0.0) or 0.0)
                ask = float(q.get("ask", 0.0) or 0.0)
                mid = float(q.get("mid", 0.0) or 0.0)
                spread_pct = float(q.get("spread_pct", 0.0) or 0.0)
                if bid > 0 and ask > 0 and mid > 0:
                    return {"bid": bid, "ask": ask, "mid": mid, "spread_pct": spread_pct}

            # Last-known-good fallback: preserves cadence during short API/DNS blips.
            hist = self.buffers.get(pair, {})
            mids = list(hist.get("mid", []))
            spreads = list(hist.get("spread_pct", []))
            if mids:
                mid = float(mids[-1])
                spread_pct = float(spreads[-1] if spreads else DEFAULT_MAX_SPREAD_PCT)
                half = max(0.00005, (spread_pct / 100.0) / 2.0)
                bid = mid * (1.0 - half)
                ask = mid * (1.0 + half)
                return {"bid": bid, "ask": ask, "mid": mid, "spread_pct": spread_pct, "source": "stale_fallback"}
            return None
        return None

    def _append_tick(self, pair, tick):
        b = self.buffers[pair]
        b["mid"].append(float(tick["mid"]))
        b["spread_pct"].append(float(tick["spread_pct"]))
        self.db.execute(
            "INSERT INTO hf_ticks (pair, bid, ask, mid, spread_pct) VALUES (?, ?, ?, ?, ?)",
            (
                pair,
                float(tick["bid"]),
                float(tick["ask"]),
                float(tick["mid"]),
                float(tick["spread_pct"]),
            ),
        )
        self.db.commit()

    def _signal(self, pair):
        buf = self.buffers[pair]
        mids = list(buf["mid"])
        spreads = list(buf["spread_pct"])
        if len(mids) < 12:
            return None

        fast_avg = sum(mids[-5:]) / 5.0
        slow_avg = sum(mids[-12:]) / 12.0
        curr_spread = spreads[-1] if spreads else 0.0

        if self.fast is not None:
            edge = float(self.fast.micro_edge_pct(fast_avg, slow_avg, curr_spread))
        else:
            # Pure-python fallback edge model.
            momentum_pct = ((fast_avg - slow_avg) / max(1e-9, slow_avg)) * 100.0
            edge = max(0.0, momentum_pct * 0.52 - curr_spread * 0.35)

        side = "HOLD"
        if fast_avg > slow_avg and edge >= DEFAULT_MIN_EDGE_PCT:
            side = "BUY"
        elif fast_avg < slow_avg and edge >= DEFAULT_MIN_EDGE_PCT:
            side = "SELL"

        confidence = 0.50
        confidence += _clamp(edge / 1.5, 0.0, 0.35)
        confidence -= _clamp(curr_spread / 0.40, 0.0, 0.20)
        confidence = _clamp(confidence, 0.05, 0.98)

        return {
            "pair": pair,
            "side": side,
            "expected_edge_pct": round(edge, 6),
            "confidence": round(confidence, 6),
            "spread_pct": round(curr_spread, 6),
            "fast_avg": round(fast_avg, 8),
            "slow_avg": round(slow_avg, 8),
        }

    def _policy(self, sig):
        decision = evaluate_no_loss_trade(
            pair=sig["pair"],
            side=sig["side"],
            expected_edge_pct=float(sig.get("expected_edge_pct", 0.0) or 0.0),
            total_cost_pct=0.14,
            spread_pct=float(sig.get("spread_pct", 0.0) or 0.0),
            venue_latency_ms=float(self.interval_ms),
            venue_failure_rate=0.02,
            signal_confidence=float(sig.get("confidence", 0.0) or 0.0),
            market_regime="HF_MICRO",
        )
        try:
            log_no_loss_decision(decision, details={"agent": "hf_execution", "signal": sig})
        except Exception:
            pass
        return decision

    def _record_signal(self, sig, policy):
        self.db.execute(
            """
            INSERT INTO hf_signals (pair, side, expected_edge_pct, confidence, spread_pct, policy_approved, policy_reason, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sig["pair"],
                sig["side"],
                float(sig.get("expected_edge_pct", 0.0) or 0.0),
                float(sig.get("confidence", 0.0) or 0.0),
                float(sig.get("spread_pct", 0.0) or 0.0),
                1 if bool(policy.get("approved", False)) else 0,
                str(policy.get("reason", "")),
                json.dumps({"signal": sig, "policy": policy}),
            ),
        )
        self.db.commit()

    def _execute(self, sig, policy):
        side = sig["side"]
        if side not in {"BUY", "SELL"}:
            return {"attempted": False, "mode": "none", "reason": "hold_signal"}
        if float(sig.get("confidence", 0.0) or 0.0) < DEFAULT_MIN_CONFIDENCE:
            return {"attempted": False, "mode": "blocked", "reason": "confidence_below_threshold"}
        if float(sig.get("spread_pct", 0.0) or 0.0) > DEFAULT_MAX_SPREAD_PCT:
            return {"attempted": False, "mode": "blocked", "reason": "spread_above_threshold"}
        if not bool(policy.get("approved", False)):
            return {"attempted": False, "mode": "blocked", "reason": str(policy.get("reason", "policy_blocked"))}

        amount_usd = DEFAULT_ORDER_USD
        mode = "paper"
        success = True
        order_id = f"paper-{int(time.time() * 1000)}"
        details = {"signal": sig, "policy": policy, "amount_usd": amount_usd}

        if self.execute_live:
            readiness = self._live_readiness()
            self._last_live_readiness = readiness
            details["live_readiness"] = readiness
            if not bool(readiness.get("ready", False)):
                return {
                    "attempted": False,
                    "mode": "blocked",
                    "reason": str(readiness.get("reason", "live_readiness_failed")),
                    "live_readiness": readiness,
                }
            mode = "live_coinbase"
            try:
                mid = (float(sig.get("fast_avg", 0.0)) + float(sig.get("slow_avg", 0.0))) / 2.0
                if mid <= 0:
                    raise RuntimeError("invalid_mid")
                if self.connector is not None:
                    route = str(readiness.get("route", "coinbase"))
                    if route == "fix":
                        mode = "live_fix"
                        fix_result = self.connector.place_order_fix(sig["pair"], side, amount_usd, order_type="MARKET")
                        success = bool(fix_result.get("success", False))
                        order_id = str(fix_result.get("order_id", ""))
                        details["fix_result"] = fix_result
                        if not success:
                            details["error"] = fix_result.get("error", "fix_failed")
                    else:
                        mode = "live_coinbase"
                        result = self.connector.place_order_crypto(
                            pair=sig["pair"],
                            side=side,
                            amount_usd=amount_usd,
                            mid_price=mid,
                            post_only=True,
                        )
                        success = bool(result.get("success", False))
                        order_id = str(result.get("order_id", ""))
                        details["result"] = result
                        if not success and result.get("error") and self.connector.allow_fix_stub:
                            mode = "live_fix_fallback"
                            fix_result = self.connector.place_order_fix(sig["pair"], side, amount_usd, order_type="MARKET")
                            details["fix_result"] = fix_result
                            success = bool(fix_result.get("success", False))
                            order_id = str(fix_result.get("order_id", "") or order_id)
                            if not success and "error" not in details:
                                details["error"] = fix_result.get("error", "fix_fallback_failed")
                elif self.trader is not None:
                    size = max(0.00001, amount_usd / mid)
                    limit_price = mid * (0.9995 if side == "BUY" else 1.0005)
                    result = self.trader.place_limit_order(
                        sig["pair"],
                        side,
                        size,
                        limit_price,
                        post_only=True,
                        bypass_profit_guard=True,
                    )
                    order_id = str(result.get("success_response", {}).get("order_id", ""))
                    success = bool(order_id)
                    details["result"] = result
                else:
                    success = False
                    order_id = ""
                    details["error"] = "no_live_connector"
            except Exception as e:
                success = False
                order_id = ""
                details["error"] = str(e)

        self.db.execute(
            """
            INSERT INTO hf_orders (pair, side, mode, amount_usd, success, order_id, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sig["pair"],
                side,
                mode,
                float(amount_usd),
                1 if success else 0,
                order_id,
                json.dumps(details),
            ),
        )
        self.db.commit()
        return {
            "attempted": True,
            "mode": mode,
            "success": bool(success),
            "order_id": order_id,
            "amount_usd": amount_usd,
        }

    def _status(self):
        rows = self.db.execute(
            """
            SELECT pair, side, expected_edge_pct, confidence, spread_pct, policy_approved, created_at
            FROM hf_signals ORDER BY id DESC LIMIT 20
            """
        ).fetchall()
        recent = [dict(r) for r in rows]
        connector_health = {}
        if self.connector is not None:
            try:
                connector_health = self.connector.health()
            except Exception:
                connector_health = {"error": "health_probe_failed"}
        live_readiness = self._live_readiness() if self.execute_live else {
            "ready": False,
            "route": "paper",
            "reason": "execute_live_disabled",
            "checked_at": _now_iso(),
            "details": {},
        }
        self._last_live_readiness = live_readiness
        return {
            "updated_at": _now_iso(),
            "running": bool(self.running),
            "cycle": int(self.cycle),
            "interval_ms": int(self.interval_ms),
            "execute_live": bool(self.execute_live),
            "stateless_mode": bool(STATELESS_MODE),
            "pairs": self.pairs,
            "connector_health": connector_health,
            "live_readiness": live_readiness,
            "recent_signals": recent,
        }

    def run_cycle(self):
        self.cycle += 1
        last_exec = None
        for pair in self.pairs:
            try:
                tick = self._book(pair)
            except Exception as e:
                logger.debug("book fetch failed %s: %s", pair, e)
                continue
            if not tick:
                continue
            self._append_tick(pair, tick)
            sig = self._signal(pair)
            if not sig:
                continue
            policy = self._policy(sig)
            self._record_signal(sig, policy)
            if sig["side"] in {"BUY", "SELL"}:
                last_exec = self._execute(sig, policy)

        payload = self._status()
        if last_exec:
            payload["last_execution"] = last_exec
        STATUS_PATH.write_text(json.dumps(payload, indent=2))

        if payload.get("recent_signals"):
            top = payload["recent_signals"][0]
            logger.info(
                "cycle=%d pair=%s side=%s edge=%.3f%% conf=%.2f spread=%.3f%%",
                self.cycle,
                top.get("pair"),
                top.get("side"),
                float(top.get("expected_edge_pct", 0.0) or 0.0),
                float(top.get("confidence", 0.0) or 0.0),
                float(top.get("spread_pct", 0.0) or 0.0),
            )
        else:
            logger.info("cycle=%d no_signals", self.cycle)

    def run_loop(self):
        logger.info(
            "starting hf_execution loop interval=%dms execute_live=%s stateless=%s",
            self.interval_ms,
            self.execute_live,
            STATELESS_MODE,
        )
        while self.running:
            started = time.perf_counter()
            try:
                self.run_cycle()
            except Exception as e:
                logger.error("cycle_failed: %s", e, exc_info=True)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            sleep_ms = max(30.0, float(self.interval_ms) - elapsed_ms)
            time.sleep(sleep_ms / 1000.0)


def main():
    parser = argparse.ArgumentParser(description="HF execution lane agent")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-ms", type=int, default=DEFAULT_INTERVAL_MS)
    parser.add_argument("--execute-live", action="store_true")
    parser.add_argument("--pairs", default=",".join(DEFAULT_PAIRS))
    args = parser.parse_args()

    pairs = [p.strip().upper() for p in str(args.pairs).split(",") if p.strip()]
    agent = HFExecutionAgent(
        pairs=pairs,
        interval_ms=args.interval_ms,
        execute_live=bool(args.execute_live or DEFAULT_EXECUTE_LIVE),
    )

    if args.once:
        agent.run_cycle()
        print(json.dumps(json.loads(STATUS_PATH.read_text()), indent=2))
        return

    try:
        agent.run_loop()
    except KeyboardInterrupt:
        logger.info("stopped by keyboard interrupt")


if __name__ == "__main__":
    main()
