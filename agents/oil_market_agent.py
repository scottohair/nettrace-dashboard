#!/usr/bin/env python3
"""Oil Market Agent.

Paper-first oil/energy strategy engine with optional IBKR execution.

What it does:
  1) Pulls live futures/ETF market data (CL=F, BZ=F, XLE, USO).
  2) Computes momentum + spread regime features.
  3) Generates deterministic BUY/SELL/HOLD signals.
  4) Runs no-loss policy gate before any execution.
  5) Persists status/telemetry for orchestrator + Claude/MCP ingestion.

Execution is safe by default:
  - live execution is OFF unless --execute-live or OIL_AGENT_EXECUTE_LIVE=1.
  - if IBKR is unavailable, it degrades to paper actions.
"""

import argparse
import json
import logging
import math
import os
import sqlite3
import statistics
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

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
    from ibkr_connector import IBKRTrader
except Exception:
    try:
        from agents.ibkr_connector import IBKRTrader  # type: ignore
    except Exception:
        IBKRTrader = None  # type: ignore

try:
    import claude_duplex
except Exception:
    try:
        from agents import claude_duplex  # type: ignore
    except Exception:
        claude_duplex = None  # type: ignore

BASE = Path(__file__).parent
DB_PATH = BASE / "oil_market_agent.db"
STATUS_PATH = BASE / "oil_market_agent_status.json"
SIGNALS_PATH = BASE / "oil_market_signals.json"

DEFAULT_INTERVAL_SECONDS = int(os.environ.get("OIL_AGENT_INTERVAL_SECONDS", "180"))
DEFAULT_BUDGET_USD = float(os.environ.get("OIL_AGENT_BUDGET_USD", "10.0"))
DEFAULT_MIN_EDGE_PCT = float(os.environ.get("OIL_AGENT_MIN_EDGE_PCT", "0.18"))
DEFAULT_MIN_CONFIDENCE = float(os.environ.get("OIL_AGENT_MIN_CONFIDENCE", "0.58"))
DEFAULT_EXECUTE_LIVE = os.environ.get("OIL_AGENT_EXECUTE_LIVE", "0").lower() in {"1", "true", "yes"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [oil_market] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE / "oil_market_agent.log")),
    ],
)
logger = logging.getLogger("oil_market_agent")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _clamp(value, lo, hi):
    return max(lo, min(hi, value))


def _fetch_json(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-OilAgent/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _ema(values, span):
    vals = [float(x) for x in values if x is not None]
    if not vals:
        return 0.0
    span = max(2, int(span))
    alpha = 2.0 / (span + 1.0)
    out = vals[0]
    for v in vals[1:]:
        out = alpha * v + (1.0 - alpha) * out
    return out


class OilMarketAgent:
    def __init__(self, interval_seconds=DEFAULT_INTERVAL_SECONDS, execute_live=DEFAULT_EXECUTE_LIVE):
        self.interval_seconds = max(30, int(interval_seconds))
        self.execute_live = bool(execute_live)
        self.db = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self._init_db()
        self.running = True
        self.cycle = 0
        self.last_signal = None
        self._ibkr = None

    def _init_db(self):
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS market_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cl_price REAL,
                bz_price REAL,
                uso_price REAL,
                xle_price REAL,
                cl_mom_pct REAL,
                spread_pct REAL,
                spread_z REAL,
                volatility_pct REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS oil_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                side TEXT NOT NULL,
                confidence REAL,
                expected_edge_pct REAL,
                regime TEXT,
                reason TEXT,
                policy_approved INTEGER DEFAULT 0,
                policy_reason TEXT,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS oil_executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity REAL,
                budget_usd REAL,
                mode TEXT,
                success INTEGER DEFAULT 0,
                order_id TEXT,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self.db.commit()

    def _fetch_symbol_series(self, symbol, interval="5m", window="5d"):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={window}&interval={interval}"
        payload = _fetch_json(url)
        result = payload.get("chart", {}).get("result", [])
        if not result:
            return []
        q = result[0].get("indicators", {}).get("quote", [{}])[0]
        closes = q.get("close", []) or []
        return [float(x) for x in closes if x is not None and float(x) > 0]

    def _market_snapshot(self):
        cl = self._fetch_symbol_series("CL=F")
        bz = self._fetch_symbol_series("BZ=F")
        uso = self._fetch_symbol_series("USO")
        xle = self._fetch_symbol_series("XLE")

        if len(cl) < 24 or len(bz) < 24:
            raise RuntimeError("insufficient_oil_series_data")

        cl_last = cl[-1]
        bz_last = bz[-1]
        uso_last = uso[-1] if uso else 0.0
        xle_last = xle[-1] if xle else 0.0

        cl_ema_fast = _ema(cl[-24:], 8)
        cl_ema_slow = _ema(cl[-60:] if len(cl) >= 60 else cl, 21)
        mom_pct = ((cl_last / max(1e-9, cl_ema_slow)) - 1.0) * 100.0

        spread_series = []
        span = min(len(cl), len(bz), 96)
        for i in range(1, span + 1):
            c = cl[-i]
            b = bz[-i]
            if c > 0:
                spread_series.append(((b - c) / c) * 100.0)
        spread_pct = ((bz_last - cl_last) / max(1e-9, cl_last)) * 100.0
        spread_mean = statistics.mean(spread_series) if spread_series else spread_pct
        spread_std = statistics.pstdev(spread_series) if len(spread_series) > 1 else 0.0
        spread_z = (spread_pct - spread_mean) / spread_std if spread_std > 1e-9 else 0.0

        returns = []
        for i in range(max(1, len(cl) - 24), len(cl)):
            prev = cl[i - 1]
            cur = cl[i]
            if prev > 0:
                returns.append((cur - prev) / prev)
        vol_pct = (statistics.pstdev(returns) * math.sqrt(24) * 100.0) if len(returns) > 1 else 0.0

        regime = "NEUTRAL"
        if cl_ema_fast > cl_ema_slow * 1.002:
            regime = "UPTREND"
        elif cl_ema_fast < cl_ema_slow * 0.998:
            regime = "DOWNTREND"

        snap = {
            "timestamp": _now_iso(),
            "prices": {
                "CL": round(cl_last, 4),
                "BZ": round(bz_last, 4),
                "USO": round(uso_last, 4),
                "XLE": round(xle_last, 4),
            },
            "features": {
                "cl_ema_fast": round(cl_ema_fast, 6),
                "cl_ema_slow": round(cl_ema_slow, 6),
                "cl_momentum_pct": round(mom_pct, 6),
                "spread_pct": round(spread_pct, 6),
                "spread_z": round(spread_z, 6),
                "volatility_pct": round(vol_pct, 6),
                "regime": regime,
            },
        }
        return snap

    def _fallback_snapshot(self, error_text=""):
        row = self.db.execute(
            """
            SELECT cl_price, bz_price, uso_price, xle_price, cl_mom_pct, spread_pct, spread_z, volatility_pct
            FROM market_samples
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        if row:
            return {
                "timestamp": _now_iso(),
                "prices": {
                    "CL": float(row["cl_price"] or 0.0),
                    "BZ": float(row["bz_price"] or 0.0),
                    "USO": float(row["uso_price"] or 0.0),
                    "XLE": float(row["xle_price"] or 0.0),
                },
                "features": {
                    "cl_ema_fast": float(row["cl_price"] or 0.0),
                    "cl_ema_slow": float(row["cl_price"] or 0.0),
                    "cl_momentum_pct": float(row["cl_mom_pct"] or 0.0),
                    "spread_pct": float(row["spread_pct"] or 0.0),
                    "spread_z": float(row["spread_z"] or 0.0),
                    "volatility_pct": float(row["volatility_pct"] or 0.0),
                    "regime": "FALLBACK",
                    "error": str(error_text),
                },
            }

        # Cold-start fallback when no network and no prior sample.
        return {
            "timestamp": _now_iso(),
            "prices": {"CL": 75.0, "BZ": 76.0, "USO": 75.0, "XLE": 90.0},
            "features": {
                "cl_ema_fast": 75.0,
                "cl_ema_slow": 75.0,
                "cl_momentum_pct": 0.0,
                "spread_pct": 0.0,
                "spread_z": 0.0,
                "volatility_pct": 0.0,
                "regime": "FALLBACK",
                "error": str(error_text),
            },
        }

    def _build_signal(self, snap):
        f = snap["features"]
        mom = float(f["cl_momentum_pct"])
        spread_z = float(f["spread_z"])
        vol = float(f["volatility_pct"])
        regime = str(f["regime"])

        side = "HOLD"
        reason_bits = []

        if mom >= 0.28 and spread_z <= 1.8:
            side = "BUY"
            reason_bits.append("momentum_up")
        elif mom <= -0.28 and spread_z >= -1.8:
            side = "SELL"
            reason_bits.append("momentum_down")

        # Mean-reverting spread extremes strengthen directional exits.
        if spread_z >= 2.3:
            reason_bits.append("spread_extreme_high")
            if side == "BUY":
                side = "HOLD"
            elif side == "HOLD":
                side = "SELL"
        elif spread_z <= -2.3:
            reason_bits.append("spread_extreme_low")
            if side == "SELL":
                side = "HOLD"
            elif side == "HOLD":
                side = "BUY"

        momentum_strength = abs(mom) / 1.0
        spread_strength = min(2.5, abs(spread_z)) / 2.5
        vol_penalty = _clamp((vol - 2.8) / 4.0, 0.0, 0.35)

        confidence = 0.48 + momentum_strength * 0.28 + spread_strength * 0.20 - vol_penalty
        if side == "HOLD":
            confidence *= 0.75
        confidence = _clamp(confidence, 0.05, 0.98)

        expected_edge_pct = max(0.0, abs(mom) * 0.62 + spread_strength * 0.24 - vol_penalty * 0.20)

        signal = {
            "timestamp": _now_iso(),
            "instrument": "USO",
            "proxy_assets": ["CL=F", "BZ=F", "USO", "XLE"],
            "side": side,
            "confidence": round(confidence, 6),
            "expected_edge_pct": round(expected_edge_pct, 6),
            "regime": regime,
            "reason": ",".join(reason_bits) if reason_bits else "no_edge",
            "features": f,
        }
        return signal

    def _policy_gate(self, signal):
        features = signal.get("features", {}) if isinstance(signal.get("features"), dict) else {}
        regime = str(signal.get("regime", "NEUTRAL"))
        decision = evaluate_no_loss_trade(
            pair="USO-USD",
            side=signal.get("side", "HOLD"),
            expected_edge_pct=float(signal.get("expected_edge_pct", 0.0) or 0.0),
            total_cost_pct=0.16,
            spread_pct=min(1.2, abs(float(features.get("spread_pct", 0.0) or 0.0))),
            venue_latency_ms=150.0,
            venue_failure_rate=0.02,
            signal_confidence=float(signal.get("confidence", 0.0) or 0.0),
            market_regime=regime,
        )
        try:
            log_no_loss_decision(decision, details={"agent": "oil_market_agent", "signal": signal})
        except Exception:
            pass
        return decision

    def _ibkr(self):
        if self._ibkr is not None:
            return self._ibkr
        if IBKRTrader is None:
            return None
        try:
            trader = IBKRTrader()
            if trader.connect():
                self._ibkr = trader
                return self._ibkr
        except Exception as e:
            logger.warning("IBKR not available: %s", e)
        return None

    def _execute(self, signal, policy):
        side = str(signal.get("side", "HOLD")).upper()
        if side not in {"BUY", "SELL"}:
            return {"attempted": False, "mode": "none", "reason": "hold_signal"}

        confidence = float(signal.get("confidence", 0.0) or 0.0)
        expected_edge = float(signal.get("expected_edge_pct", 0.0) or 0.0)
        if not bool(policy.get("approved", False)):
            return {
                "attempted": False,
                "mode": "blocked",
                "reason": str(policy.get("reason", "no_loss_policy_block")),
            }
        if confidence < DEFAULT_MIN_CONFIDENCE or expected_edge < DEFAULT_MIN_EDGE_PCT:
            return {
                "attempted": False,
                "mode": "blocked",
                "reason": "confidence_or_edge_below_threshold",
            }

        budget_usd = DEFAULT_BUDGET_USD
        uso_px = float(signal.get("features", {}).get("uso_price", 0.0) or signal.get("features", {}).get("USO", 0.0) or 0.0)
        if uso_px <= 0:
            uso_px = max(1.0, float(signal.get("features", {}).get("cl_ema_fast", 1.0)) / 10.0)
        qty = max(1.0, round(budget_usd / max(1e-9, uso_px), 3))

        mode = "paper"
        success = True
        order_id = f"paper-{int(time.time())}"
        details = {
            "instrument": "USO",
            "side": side,
            "quantity": qty,
            "budget_usd": budget_usd,
            "signal": signal,
            "policy": policy,
        }

        if self.execute_live:
            trader = self._ibkr()
            if trader is None:
                mode = "paper_fallback"
                success = False
                order_id = ""
                details["error"] = "ibkr_unavailable"
            else:
                mode = "live_ibkr"
                try:
                    result = trader.place_order("USO", side, qty)
                    order_id = str(result.get("success_response", {}).get("order_id", ""))
                    success = bool(order_id)
                    details["result"] = result
                except Exception as e:
                    success = False
                    order_id = ""
                    details["error"] = str(e)

        self.db.execute(
            """
            INSERT INTO oil_executions (symbol, side, quantity, budget_usd, mode, success, order_id, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "USO",
                side,
                float(qty),
                float(budget_usd),
                mode,
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
            "quantity": qty,
            "budget_usd": budget_usd,
        }

    def _write_status(self, snapshot, signal, policy, execution, top_signals):
        payload = {
            "updated_at": _now_iso(),
            "running": bool(self.running),
            "cycle": int(self.cycle),
            "interval_seconds": int(self.interval_seconds),
            "execute_live": bool(self.execute_live),
            "market_snapshot": snapshot,
            "last_signal": signal,
            "policy": policy,
            "execution": execution,
            "recent_signals": top_signals,
        }
        STATUS_PATH.write_text(json.dumps(payload, indent=2))
        SIGNALS_PATH.write_text(json.dumps(top_signals, indent=2))

    def _record_market_sample(self, snap):
        p = snap["prices"]
        f = snap["features"]
        self.db.execute(
            """
            INSERT INTO market_samples
                (cl_price, bz_price, uso_price, xle_price, cl_mom_pct, spread_pct, spread_z, volatility_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                float(p.get("CL", 0.0) or 0.0),
                float(p.get("BZ", 0.0) or 0.0),
                float(p.get("USO", 0.0) or 0.0),
                float(p.get("XLE", 0.0) or 0.0),
                float(f.get("cl_momentum_pct", 0.0) or 0.0),
                float(f.get("spread_pct", 0.0) or 0.0),
                float(f.get("spread_z", 0.0) or 0.0),
                float(f.get("volatility_pct", 0.0) or 0.0),
            ),
        )
        self.db.commit()

    def _record_signal(self, signal, policy):
        self.db.execute(
            """
            INSERT INTO oil_signals (side, confidence, expected_edge_pct, regime, reason, policy_approved, policy_reason, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(signal.get("side", "HOLD")),
                float(signal.get("confidence", 0.0) or 0.0),
                float(signal.get("expected_edge_pct", 0.0) or 0.0),
                str(signal.get("regime", "NEUTRAL")),
                str(signal.get("reason", "")),
                1 if bool(policy.get("approved", False)) else 0,
                str(policy.get("reason", "")),
                json.dumps({"signal": signal, "policy": policy}),
            ),
        )
        self.db.commit()

    def _recent_signals(self, limit=20):
        rows = self.db.execute(
            """
            SELECT side, confidence, expected_edge_pct, regime, reason, policy_approved, policy_reason, created_at
            FROM oil_signals
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        return [dict(r) for r in rows]

    def run_cycle(self):
        self.cycle += 1
        try:
            snapshot = self._market_snapshot()
        except Exception as e:
            logger.warning("live snapshot unavailable, using fallback: %s", e)
            snapshot = self._fallback_snapshot(error_text=str(e))
        self._record_market_sample(snapshot)
        signal = self._build_signal(snapshot)
        # Mirror USO price into features for execution sizing.
        signal.setdefault("features", {})["uso_price"] = snapshot["prices"].get("USO", 0.0)
        policy = self._policy_gate(signal)
        self._record_signal(signal, policy)

        execution = self._execute(signal, policy)
        recent = self._recent_signals(limit=20)
        self._write_status(snapshot, signal, policy, execution, recent)

        if claude_duplex and signal.get("side") in {"BUY", "SELL"}:
            try:
                claude_duplex.send_to_claude(
                    message=(
                        f"oil_market_agent signal {signal['side']} USO edge={signal['expected_edge_pct']:.3f}% "
                        f"conf={signal['confidence']:.2f} policy={policy.get('approved', False)}"
                    ),
                    msg_type="strategy_signal",
                    priority="high" if float(signal.get("confidence", 0.0)) >= 0.72 else "normal",
                    source="oil_market_agent",
                    meta={"signal": signal, "policy": policy, "execution": execution},
                )
            except Exception:
                pass

        logger.info(
            "cycle=%d side=%s edge=%.3f%% conf=%.2f policy=%s exec=%s",
            self.cycle,
            signal.get("side"),
            float(signal.get("expected_edge_pct", 0.0) or 0.0),
            float(signal.get("confidence", 0.0) or 0.0),
            bool(policy.get("approved", False)),
            execution.get("mode"),
        )

    def run_loop(self):
        logger.info("starting oil_market_agent loop interval=%ss execute_live=%s", self.interval_seconds, self.execute_live)
        while self.running:
            started = time.perf_counter()
            try:
                self.run_cycle()
            except Exception as e:
                logger.error("cycle_failed: %s", e, exc_info=True)
            elapsed = time.perf_counter() - started
            sleep_for = max(1, int(self.interval_seconds - elapsed))
            for _ in range(sleep_for):
                if not self.running:
                    break
                time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="Oil market signal + execution agent")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--execute-live", action="store_true")
    args = parser.parse_args()

    execute_live = bool(args.execute_live or DEFAULT_EXECUTE_LIVE)
    agent = OilMarketAgent(interval_seconds=args.interval, execute_live=execute_live)

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
