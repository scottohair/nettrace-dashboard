#!/usr/bin/env python3
"""Multi-hop arbitrage engine.

Evaluates deterministic USDC -> asset -> asset -> USDC loops using
cross-exchange quotes and fee/slippage models.

Default mode is paper/signal only. Live execution remains opt-in.
"""

import argparse
import itertools
import json
import logging
import math
import os
import sqlite3
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

try:
    from exchange_connector import MultiExchangeFeed
except Exception:
    from agents.exchange_connector import MultiExchangeFeed  # type: ignore

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
    import claude_duplex
except Exception:
    try:
        from agents import claude_duplex  # type: ignore
    except Exception:
        claude_duplex = None  # type: ignore

BASE = Path(__file__).parent
DB_PATH = BASE / "multi_hop_arb.db"
STATUS_PATH = BASE / "multi_hop_arb_status.json"
MCP_FEED_PATH = BASE / "multi_hop_arb_opportunities.json"

DEFAULT_INTERVAL_SECONDS = int(os.environ.get("MULTI_HOP_ARB_INTERVAL_SECONDS", "120"))
DEFAULT_MIN_EDGE_PCT = float(os.environ.get("MULTI_HOP_ARB_MIN_EDGE_PCT", "0.20"))
DEFAULT_MAX_CANDIDATES = int(os.environ.get("MULTI_HOP_ARB_MAX_CANDIDATES", "30"))
DEFAULT_EXECUTE_LIVE = os.environ.get("MULTI_HOP_ARB_EXECUTE_LIVE", "0").lower() in {"1", "true", "yes"}

ASSETS = ["BTC", "ETH", "SOL", "AVAX", "LINK", "DOGE"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [multi_hop_arb] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE / "multi_hop_arb.log")),
    ],
)
logger = logging.getLogger("multi_hop_arb")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _clamp(value, lo, hi):
    return max(lo, min(hi, value))


class MultiHopArbEngine:
    def __init__(self, interval_seconds=DEFAULT_INTERVAL_SECONDS, execute_live=DEFAULT_EXECUTE_LIVE):
        self.interval_seconds = max(20, int(interval_seconds))
        self.execute_live = bool(execute_live)
        self.running = True
        self.cycle = 0
        self.db = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self._init_db()

    def _init_db(self):
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS quote_books (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT NOT NULL,
                buy_price REAL,
                sell_price REAL,
                mid_price REAL,
                spread_pct REAL,
                sources INTEGER,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS route_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                route TEXT NOT NULL,
                hop_count INTEGER,
                gross_return_pct REAL,
                total_cost_pct REAL,
                net_edge_pct REAL,
                avg_spread_pct REAL,
                confidence REAL,
                policy_approved INTEGER,
                policy_reason TEXT,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS route_executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                route TEXT NOT NULL,
                mode TEXT,
                success INTEGER,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self.db.commit()

    def _quote_book(self):
        book = {}
        for asset in ASSETS:
            prices = MultiExchangeFeed.get_all_prices(asset, quote="USD")
            vals = [float(v) for v in prices.values() if float(v) > 0]
            if len(vals) < 2:
                continue
            buy = min(vals)   # optimistic best ask
            sell = max(vals)  # optimistic best bid
            mid = statistics.median(vals)
            spread_pct = ((sell - buy) / max(1e-9, mid)) * 100.0
            entry = {
                "asset": asset,
                "buy": buy,
                "sell": sell,
                "mid": mid,
                "spread_pct": spread_pct,
                "sources": len(vals),
                "prices": prices,
            }
            book[asset] = entry

            self.db.execute(
                """
                INSERT INTO quote_books (asset, buy_price, sell_price, mid_price, spread_pct, sources, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset,
                    float(buy),
                    float(sell),
                    float(mid),
                    float(spread_pct),
                    int(len(vals)),
                    json.dumps(prices),
                ),
            )
        self.db.commit()
        return book

    def _fallback_quote_book(self):
        rows = self.db.execute(
            """
            SELECT q1.asset, q1.buy_price, q1.sell_price, q1.mid_price, q1.spread_pct, q1.sources, q1.details_json
            FROM quote_books q1
            JOIN (
                SELECT asset, MAX(id) AS max_id
                FROM quote_books
                GROUP BY asset
            ) q2 ON q1.asset = q2.asset AND q1.id = q2.max_id
            """
        ).fetchall()
        out = {}
        for r in rows:
            prices = {}
            try:
                prices = json.loads(r["details_json"] or "{}")
                if not isinstance(prices, dict):
                    prices = {}
            except Exception:
                prices = {}
            out[str(r["asset"])] = {
                "asset": str(r["asset"]),
                "buy": float(r["buy_price"] or 0.0),
                "sell": float(r["sell_price"] or 0.0),
                "mid": float(r["mid_price"] or 0.0),
                "spread_pct": float(r["spread_pct"] or 0.0),
                "sources": int(r["sources"] or 0),
                "prices": prices,
            }
        return out

    def _route_iter(self):
        for a, b in itertools.permutations(ASSETS, 2):
            yield ["USDC", a, b, "USDC"]

    def _simulate_route(self, route, book):
        qty = 1.0  # 1 USDC starting notional
        gross_qty = qty
        total_cost_pct = 0.0
        spread_vals = []
        hops = []

        for i in range(len(route) - 1):
            src = route[i]
            dst = route[i + 1]

            if src == "USDC" and dst in book:
                buy_px = book[dst]["buy"]
                if buy_px <= 0:
                    return None
                before = qty
                qty = qty / buy_px
                gross_qty = gross_qty / buy_px
                spread_vals.append(book[dst]["spread_pct"])
                hop_spread = book[dst]["spread_pct"]
            elif dst == "USDC" and src in book:
                sell_px = book[src]["sell"]
                if sell_px <= 0:
                    return None
                before = qty
                qty = qty * sell_px
                gross_qty = gross_qty * sell_px
                spread_vals.append(book[src]["spread_pct"])
                hop_spread = book[src]["spread_pct"]
            elif src in book and dst in book:
                sell_px = book[src]["sell"]
                buy_px = book[dst]["buy"]
                if sell_px <= 0 or buy_px <= 0:
                    return None
                before = qty
                qty = qty * (sell_px / buy_px)
                gross_qty = gross_qty * (sell_px / buy_px)
                hop_spread = (book[src]["spread_pct"] + book[dst]["spread_pct"]) / 2.0
                spread_vals.append(hop_spread)
            else:
                return None

            # Cost model: base fee + spread friction + latency reserve.
            base_fee_pct = 0.12
            spread_friction_pct = _clamp(hop_spread * 0.08, 0.02, 0.35)
            latency_reserve_pct = 0.03
            hop_cost_pct = base_fee_pct + spread_friction_pct + latency_reserve_pct
            total_cost_pct += hop_cost_pct
            qty *= (1.0 - hop_cost_pct / 100.0)

            hops.append(
                {
                    "src": src,
                    "dst": dst,
                    "qty_before": round(before, 10),
                    "qty_after": round(qty, 10),
                    "hop_cost_pct": round(hop_cost_pct, 6),
                }
            )

        gross_return_pct = (gross_qty - 1.0) * 100.0
        net_edge_pct = (qty - 1.0) * 100.0
        avg_spread = statistics.mean(spread_vals) if spread_vals else 0.0
        confidence = 0.45 + _clamp(net_edge_pct / 1.5, 0.0, 0.40) + _clamp(len(spread_vals) * 0.03, 0.0, 0.12)
        confidence -= _clamp(avg_spread / 8.0, 0.0, 0.15)
        confidence = _clamp(confidence, 0.05, 0.99)

        return {
            "route": route,
            "hop_count": len(route) - 1,
            "gross_return_pct": round(gross_return_pct, 6),
            "total_cost_pct": round(total_cost_pct, 6),
            "net_edge_pct": round(net_edge_pct, 6),
            "avg_spread_pct": round(avg_spread, 6),
            "confidence": round(confidence, 6),
            "hops": hops,
        }

    def _policy_gate(self, candidate):
        decision = evaluate_no_loss_trade(
            pair="->".join(candidate["route"]),
            side="BUY",
            expected_edge_pct=float(candidate.get("net_edge_pct", 0.0) or 0.0),
            total_cost_pct=float(candidate.get("total_cost_pct", 0.0) or 0.0),
            spread_pct=float(candidate.get("avg_spread_pct", 0.0) or 0.0),
            venue_latency_ms=190.0,
            venue_failure_rate=0.03,
            signal_confidence=float(candidate.get("confidence", 0.0) or 0.0),
            market_regime="ARBITRAGE",
        )
        try:
            log_no_loss_decision(decision, details={"agent": "multi_hop_arb", "candidate": candidate})
        except Exception:
            pass
        return decision

    def _record_candidate(self, candidate, policy):
        self.db.execute(
            """
            INSERT INTO route_candidates
                (route, hop_count, gross_return_pct, total_cost_pct, net_edge_pct, avg_spread_pct,
                 confidence, policy_approved, policy_reason, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "->".join(candidate["route"]),
                int(candidate.get("hop_count", 0) or 0),
                float(candidate.get("gross_return_pct", 0.0) or 0.0),
                float(candidate.get("total_cost_pct", 0.0) or 0.0),
                float(candidate.get("net_edge_pct", 0.0) or 0.0),
                float(candidate.get("avg_spread_pct", 0.0) or 0.0),
                float(candidate.get("confidence", 0.0) or 0.0),
                1 if bool(policy.get("approved", False)) else 0,
                str(policy.get("reason", "")),
                json.dumps({"candidate": candidate, "policy": policy}),
            ),
        )
        self.db.commit()

    def _maybe_execute(self, candidate, policy):
        route_str = "->".join(candidate["route"])
        if not self.execute_live:
            mode = "paper"
            success = bool(policy.get("approved", False))
            details = {"reason": "live_execution_disabled", "candidate": candidate, "policy": policy}
        elif not bool(policy.get("approved", False)):
            mode = "blocked"
            success = False
            details = {"reason": "no_loss_policy_blocked", "candidate": candidate, "policy": policy}
        else:
            # Live multi-hop execution is intentionally conservative and disabled by default.
            mode = "simulated_live_plan"
            success = True
            details = {
                "reason": "execution_stub",
                "note": "Route planning complete; wire venue-specific executors before live deployment.",
                "candidate": candidate,
                "policy": policy,
            }

        self.db.execute(
            "INSERT INTO route_executions (route, mode, success, details_json) VALUES (?, ?, ?, ?)",
            (route_str, mode, 1 if success else 0, json.dumps(details)),
        )
        self.db.commit()
        return {"mode": mode, "success": success}

    def run_cycle(self):
        self.cycle += 1
        quote_book = self._quote_book()
        if len(quote_book) < 3:
            cached = self._fallback_quote_book()
            if len(cached) >= 3:
                logger.warning("live quote book sparse; using cached quote book")
                quote_book = cached
            else:
                payload = {
                    "updated_at": _now_iso(),
                    "running": bool(self.running),
                    "cycle": int(self.cycle),
                    "interval_seconds": int(self.interval_seconds),
                    "execute_live": bool(self.execute_live),
                    "quote_assets": sorted(quote_book.keys()),
                    "top_candidates": [],
                    "candidate_count": 0,
                    "last_execution": None,
                    "warning": "insufficient_quote_book",
                }
                STATUS_PATH.write_text(json.dumps(payload, indent=2))
                MCP_FEED_PATH.write_text(json.dumps([], indent=2))
                logger.warning("cycle=%d skipped: insufficient quote book", self.cycle)
                return

        candidates = []
        for route in self._route_iter():
            c = self._simulate_route(route, quote_book)
            if not c:
                continue
            if float(c.get("net_edge_pct", 0.0) or 0.0) < DEFAULT_MIN_EDGE_PCT:
                continue
            policy = self._policy_gate(c)
            c["policy"] = policy
            self._record_candidate(c, policy)
            if bool(policy.get("approved", False)):
                candidates.append(c)

        candidates.sort(key=lambda x: (float(x.get("net_edge_pct", 0.0) or 0.0), float(x.get("confidence", 0.0) or 0.0)), reverse=True)
        candidates = candidates[: DEFAULT_MAX_CANDIDATES]

        execution = None
        if candidates:
            execution = self._maybe_execute(candidates[0], candidates[0]["policy"])

        payload = {
            "updated_at": _now_iso(),
            "running": bool(self.running),
            "cycle": int(self.cycle),
            "interval_seconds": int(self.interval_seconds),
            "execute_live": bool(self.execute_live),
            "quote_assets": sorted(quote_book.keys()),
            "top_candidates": candidates[:10],
            "candidate_count": len(candidates),
            "last_execution": execution,
        }
        STATUS_PATH.write_text(json.dumps(payload, indent=2))
        MCP_FEED_PATH.write_text(json.dumps(candidates[:25], indent=2))

        if claude_duplex and candidates:
            best = candidates[0]
            try:
                claude_duplex.send_to_claude(
                    message=(
                        f"multi_hop_arb top route {'->'.join(best['route'])} edge={best['net_edge_pct']:.3f}% "
                        f"conf={best['confidence']:.2f}"
                    ),
                    msg_type="arb_signal",
                    priority="high" if float(best.get("confidence", 0.0)) >= 0.72 else "normal",
                    source="multi_hop_arb_engine",
                    meta={"candidate": best},
                )
            except Exception:
                pass

        logger.info(
            "cycle=%d assets=%d approved_candidates=%d best_edge=%s",
            self.cycle,
            len(quote_book),
            len(candidates),
            (f"{candidates[0]['net_edge_pct']:.3f}%" if candidates else "n/a"),
        )

    def run_loop(self):
        logger.info("starting multi_hop_arb loop interval=%ss execute_live=%s", self.interval_seconds, self.execute_live)
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
    parser = argparse.ArgumentParser(description="Multi-hop arbitrage engine")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--execute-live", action="store_true")
    args = parser.parse_args()

    engine = MultiHopArbEngine(
        interval_seconds=args.interval,
        execute_live=bool(args.execute_live or DEFAULT_EXECUTE_LIVE),
    )
    if args.once:
        engine.run_cycle()
        print(json.dumps(json.loads(STATUS_PATH.read_text()), indent=2))
        return

    try:
        engine.run_loop()
    except KeyboardInterrupt:
        logger.info("stopped by keyboard interrupt")


if __name__ == "__main__":
    main()
