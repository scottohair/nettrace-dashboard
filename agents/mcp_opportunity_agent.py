#!/usr/bin/env python3
"""MCP opportunity agent.

Continuously collects opportunities from internal agents + live web market endpoints,
ranks them, and publishes:
  - mcp_opportunities.json       (ranked opportunity feed)
  - mcp_exit_hints.json          (exit-manager override hints)
  - mcp_opportunity_status.json  (agent status)

Also pushes concise updates onto Claude duplex for staged ingestion.
"""

import argparse
import json
import logging
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
    from market_connector_hub import write_hub_snapshot
except Exception:
    try:
        from agents.market_connector_hub import write_hub_snapshot  # type: ignore
    except Exception:
        write_hub_snapshot = None

try:
    import claude_duplex
except Exception:
    try:
        from agents import claude_duplex  # type: ignore
    except Exception:
        claude_duplex = None  # type: ignore

try:
    from claude_staging import stage_operator_message
except Exception:
    try:
        from agents.claude_staging import stage_operator_message  # type: ignore
    except Exception:
        def stage_operator_message(*_args, **_kwargs):
            return None

BASE = Path(__file__).parent
DB_PATH = BASE / "mcp_opportunity_agent.db"
STATUS_PATH = BASE / "mcp_opportunity_status.json"
OPPORTUNITY_PATH = BASE / "mcp_opportunities.json"
EXIT_HINTS_PATH = BASE / "mcp_exit_hints.json"
OUTBOX_PATH = BASE / "mcp_opportunity_outbox.json"

MULTI_HOP_PATH = BASE / "multi_hop_arb_opportunities.json"
OIL_STATUS_PATH = BASE / "oil_market_agent_status.json"
QUANT_RESULTS_PATH = BASE / "quant_100_results.json"
GROWTH_REPORT_PATH = BASE / "growth_go_no_go_report.json"

DEFAULT_INTERVAL_SECONDS = int(os.environ.get("MCP_OPP_INTERVAL_SECONDS", "150"))
DEFAULT_MIN_SCORE = float(os.environ.get("MCP_OPP_MIN_SCORE", "0.35"))
DEFAULT_TOP_N = int(os.environ.get("MCP_OPP_TOP_N", "40"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [mcp_opportunity] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE / "mcp_opportunity_agent.log")),
    ],
)
logger = logging.getLogger("mcp_opportunity_agent")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _load_json(path, default):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except Exception:
        return default


def _clamp(value, lo, hi):
    return max(lo, min(hi, value))


def _pair_to_usdc(pair):
    p = str(pair or "").upper().strip()
    if p.endswith("-USD"):
        return p.replace("-USD", "-USDC")
    return p


class MCPOpportunityAgent:
    def __init__(self, interval_seconds=DEFAULT_INTERVAL_SECONDS):
        self.interval_seconds = max(30, int(interval_seconds))
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
            CREATE TABLE IF NOT EXISTS opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                symbol TEXT,
                action TEXT,
                expected_edge_pct REAL,
                confidence REAL,
                latency_ms REAL,
                score REAL,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS hint_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                global_json TEXT,
                pairs_json TEXT,
                opportunity_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self.db.commit()

    def _collect_dashboard(self):
        if not write_hub_snapshot:
            return {}
        try:
            return write_hub_snapshot()
        except Exception as e:
            logger.debug("hub snapshot failed: %s", e)
            return {}

    def _collect_market_arb(self):
        pairs = ["BTC", "ETH", "SOL", "AVAX", "LINK", "DOGE"]
        raw = MultiExchangeFeed.find_arb_opportunities(pairs=pairs, min_spread_pct=0.0025)
        out = []
        for row in raw[:30]:
            spread = float(row.get("spread_pct", 0.0) or 0.0)
            edge = max(0.0, spread - 0.28)
            confidence = _clamp(0.50 + (edge / 1.6), 0.05, 0.97)
            out.append(
                {
                    "source": "cross_exchange_arb",
                    "symbol": _pair_to_usdc(row.get("pair", "")),
                    "action": "BUY" if str(row.get("buy_exchange", "")).lower() == "coinbase" else "SELL",
                    "expected_edge_pct": round(edge, 6),
                    "confidence": round(confidence, 6),
                    "latency_ms": 160.0,
                    "details": row,
                }
            )
        return out

    def _collect_multi_hop(self):
        rows = _load_json(MULTI_HOP_PATH, [])
        out = []
        if not isinstance(rows, list):
            return out
        for row in rows[:25]:
            if not isinstance(row, dict):
                continue
            route = row.get("route", [])
            if not isinstance(route, list) or len(route) < 2:
                continue
            anchor = str(route[1]) if len(route) > 1 else "BTC"
            out.append(
                {
                    "source": "multi_hop_arb",
                    "symbol": f"{anchor}-USDC",
                    "action": "BUY",
                    "expected_edge_pct": float(row.get("net_edge_pct", 0.0) or 0.0),
                    "confidence": float(row.get("confidence", 0.0) or 0.0),
                    "latency_ms": 210.0,
                    "details": row,
                }
            )
        return out

    def _collect_oil(self):
        payload = _load_json(OIL_STATUS_PATH, {})
        signal = payload.get("last_signal", {}) if isinstance(payload, dict) else {}
        if not isinstance(signal, dict) or not signal:
            return []
        side = str(signal.get("side", "HOLD")).upper()
        if side not in {"BUY", "SELL"}:
            return []
        edge = float(signal.get("expected_edge_pct", 0.0) or 0.0)
        conf = float(signal.get("confidence", 0.0) or 0.0)
        return [
            {
                "source": "oil_regime_proxy",
                "symbol": "ETH-USDC",
                "action": "BUY" if side == "BUY" else "SELL",
                "expected_edge_pct": round(max(0.0, edge * 0.60), 6),
                "confidence": round(_clamp(conf * 0.85, 0.05, 0.95), 6),
                "latency_ms": 200.0,
                "details": {"oil_signal": signal},
            }
        ]

    def _collect_quant_bias(self):
        quant = _load_json(QUANT_RESULTS_PATH, {})
        summary = quant.get("summary", {}) if isinstance(quant, dict) else {}
        if isinstance(summary, dict):
            top = summary.get("top_actionable", []) or summary.get("top_candidates", [])
        else:
            top = []
        out = []
        if not isinstance(top, list):
            return out
        for row in top[:12]:
            if not isinstance(row, dict):
                continue
            pair = _pair_to_usdc(row.get("pair", ""))
            if not pair:
                continue
            ret = float(row.get("total_return_pct", row.get("return_pct", 0.0)) or 0.0)
            conf = _clamp(0.48 + max(0.0, ret) / 3.0, 0.05, 0.92)
            out.append(
                {
                    "source": "quant_bias",
                    "symbol": pair,
                    "action": "BUY",
                    "expected_edge_pct": round(max(0.0, ret * 0.45), 6),
                    "confidence": round(conf, 6),
                    "latency_ms": 140.0,
                    "details": row,
                }
            )
        return out

    def _score(self, opp, dashboard_latencies, go_live):
        edge = max(0.0, float(opp.get("expected_edge_pct", 0.0) or 0.0))
        conf = _clamp(float(opp.get("confidence", 0.0) or 0.0), 0.0, 1.0)
        latency_ms = float(opp.get("latency_ms", 200.0) or 200.0)
        if dashboard_latencies:
            latency_ms = max(latency_ms, statistics.median(dashboard_latencies))

        edge_score = _clamp(edge / 1.2, 0.0, 1.0)
        conf_score = conf
        latency_penalty = _clamp(latency_ms / 1200.0, 0.0, 0.65)
        governance = 1.0 if go_live else 0.85

        score = (edge_score * 0.52 + conf_score * 0.43) * governance - latency_penalty * 0.25
        return round(_clamp(score, 0.0, 1.0), 6)

    def _build_exit_hints(self, opportunities, dashboard_latencies):
        lat = statistics.median(dashboard_latencies) if dashboard_latencies else 180.0
        global_cfg = {
            "vol_scale": 1.0,
            "stop_scale": 1.0,
            "dead_money_hours_mult": 1.0,
            "force_eval_hours_mult": 1.0,
            "tp_bias_pct": 0.0,
        }
        if lat > 500:
            global_cfg["stop_scale"] = 0.92
            global_cfg["dead_money_hours_mult"] = 0.90
        elif lat < 180:
            global_cfg["stop_scale"] = 1.02

        pair_cfg = {}
        for opp in opportunities[:25]:
            pair = _pair_to_usdc(opp.get("symbol", ""))
            if not pair or "-" not in pair:
                continue
            action = str(opp.get("action", "HOLD")).upper()
            edge = float(opp.get("expected_edge_pct", 0.0) or 0.0)
            conf = float(opp.get("confidence", 0.0) or 0.0)
            strength = _clamp((edge / 1.5) + (conf - 0.5), 0.0, 1.25)
            cfg = pair_cfg.get(
                pair,
                {
                    "vol_scale": 1.0,
                    "stop_scale": 1.0,
                    "dead_money_hours_mult": 1.0,
                    "force_eval_hours_mult": 1.0,
                    "tp_bias_pct": 0.0,
                },
            )
            if action == "SELL":
                cfg["stop_scale"] = min(cfg["stop_scale"], 1.0 - 0.20 * strength)
                cfg["dead_money_hours_mult"] = min(cfg["dead_money_hours_mult"], 1.0 - 0.25 * strength)
                cfg["force_eval_hours_mult"] = min(cfg["force_eval_hours_mult"], 1.0 - 0.20 * strength)
                cfg["tp_bias_pct"] = max(cfg["tp_bias_pct"], 0.0)
            elif action == "BUY":
                cfg["stop_scale"] = max(cfg["stop_scale"], 1.0 + 0.08 * strength)
                cfg["dead_money_hours_mult"] = max(cfg["dead_money_hours_mult"], 1.0 + 0.12 * strength)
                cfg["force_eval_hours_mult"] = max(cfg["force_eval_hours_mult"], 1.0 + 0.10 * strength)
                cfg["tp_bias_pct"] = max(cfg["tp_bias_pct"], 0.01 * strength)

            # Clamp pair overrides
            cfg["vol_scale"] = _clamp(cfg.get("vol_scale", 1.0), 0.70, 1.35)
            cfg["stop_scale"] = _clamp(cfg.get("stop_scale", 1.0), 0.72, 1.28)
            cfg["dead_money_hours_mult"] = _clamp(cfg.get("dead_money_hours_mult", 1.0), 0.60, 1.60)
            cfg["force_eval_hours_mult"] = _clamp(cfg.get("force_eval_hours_mult", 1.0), 0.70, 1.70)
            cfg["tp_bias_pct"] = _clamp(cfg.get("tp_bias_pct", 0.0), -0.04, 0.06)
            pair_cfg[pair] = cfg

        hints = {
            "updated_at": _now_iso(),
            "source": "mcp_opportunity_agent",
            "global": global_cfg,
            "pairs": pair_cfg,
        }
        return hints

    def run_cycle(self):
        self.cycle += 1
        dashboard = self._collect_dashboard()
        dashboard_endpoints = dashboard.get("dashboard_endpoints", []) if isinstance(dashboard, dict) else []
        dashboard_latencies = [float(x.get("latency_ms", 0.0) or 0.0) for x in dashboard_endpoints if bool(x.get("ok", False))]

        growth = _load_json(GROWTH_REPORT_PATH, {})
        go_live = bool((growth.get("decision", {}) if isinstance(growth, dict) else {}).get("go_live", False))

        all_rows = []
        all_rows.extend(self._collect_market_arb())
        all_rows.extend(self._collect_multi_hop())
        all_rows.extend(self._collect_oil())
        all_rows.extend(self._collect_quant_bias())

        ranked = []
        for row in all_rows:
            score = self._score(row, dashboard_latencies, go_live)
            row = dict(row)
            row["score"] = score
            if score < DEFAULT_MIN_SCORE:
                continue
            ranked.append(row)

            self.db.execute(
                """
                INSERT INTO opportunities
                    (source, symbol, action, expected_edge_pct, confidence, latency_ms, score, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(row.get("source", "")),
                    str(row.get("symbol", "")),
                    str(row.get("action", "HOLD")),
                    float(row.get("expected_edge_pct", 0.0) or 0.0),
                    float(row.get("confidence", 0.0) or 0.0),
                    float(row.get("latency_ms", 0.0) or 0.0),
                    float(score),
                    json.dumps(row.get("details", {})),
                ),
            )

        self.db.commit()
        ranked.sort(key=lambda x: (float(x.get("score", 0.0) or 0.0), float(x.get("expected_edge_pct", 0.0) or 0.0)), reverse=True)
        ranked = ranked[:DEFAULT_TOP_N]

        hints = self._build_exit_hints(ranked, dashboard_latencies)
        EXIT_HINTS_PATH.write_text(json.dumps(hints, indent=2))
        OPPORTUNITY_PATH.write_text(json.dumps(ranked, indent=2))

        self.db.execute(
            "INSERT INTO hint_updates (global_json, pairs_json, opportunity_count) VALUES (?, ?, ?)",
            (
                json.dumps(hints.get("global", {})),
                json.dumps(hints.get("pairs", {})),
                int(len(ranked)),
            ),
        )
        self.db.commit()

        top3 = ranked[:3]
        duplex_delivery = {}
        outbox = {
            "updated_at": _now_iso(),
            "cycle": int(self.cycle),
            "go_live": bool(go_live),
            "opportunity_count": len(ranked),
            "top_opportunities": top3,
            "dashboard_latency_ms": {
                "median": round(statistics.median(dashboard_latencies), 3) if dashboard_latencies else 0.0,
                "samples": len(dashboard_latencies),
            },
        }
        status = {
            "updated_at": _now_iso(),
            "running": bool(self.running),
            "cycle": int(self.cycle),
            "interval_seconds": int(self.interval_seconds),
            "go_live": bool(go_live),
            "opportunity_count": len(ranked),
            "top_sources": sorted({x.get("source", "") for x in ranked}),
            "files": {
                "opportunities": str(OPPORTUNITY_PATH),
                "exit_hints": str(EXIT_HINTS_PATH),
                "outbox": str(OUTBOX_PATH),
            },
        }

        if top3:
            msg = "; ".join(
                f"{x.get('source')}:{x.get('symbol')}:{x.get('action')} edge={float(x.get('expected_edge_pct',0.0)):.3f}% score={float(x.get('score',0.0)):.2f}"
                for x in top3
            )
            stage_operator_message(
                f"MCP opportunity cycle {self.cycle}: {msg}",
                category="mcp_opportunity",
                priority="high" if go_live else "normal",
                sender="mcp_opportunity_agent",
            )
            if claude_duplex:
                try:
                    payload = claude_duplex.send_to_claude(
                        message=f"mcp_opportunity top3: {msg}",
                        msg_type="opportunity_digest",
                        priority="high" if go_live else "normal",
                        source="mcp_opportunity_agent",
                        meta={"top": top3, "go_live": go_live},
                    )
                    duplex_delivery = {
                        "sent": True,
                        "channel": "to_claude",
                        "message_id": int(payload.get("id", 0)) if isinstance(payload, dict) else 0,
                        "trace_id": str(payload.get("trace_id", "")) if isinstance(payload, dict) else "",
                        "status": str(payload.get("status", "")) if isinstance(payload, dict) else "",
                    }
                except Exception as e:
                    duplex_delivery = {
                        "sent": False,
                        "error": str(e),
                    }

        if duplex_delivery:
            outbox["claude_duplex_delivery"] = duplex_delivery
            status["claude_duplex_delivery"] = duplex_delivery

        OUTBOX_PATH.write_text(json.dumps(outbox, indent=2))
        STATUS_PATH.write_text(json.dumps(status, indent=2))

        logger.info(
            "cycle=%d go_live=%s opportunities=%d top=%s",
            self.cycle,
            go_live,
            len(ranked),
            (f"{top3[0].get('source')}:{top3[0].get('symbol')}" if top3 else "n/a"),
        )

    def run_loop(self):
        logger.info("starting mcp_opportunity_agent loop interval=%ss", self.interval_seconds)
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
    parser = argparse.ArgumentParser(description="MCP opportunity aggregation agent")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    agent = MCPOpportunityAgent(interval_seconds=args.interval)
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
