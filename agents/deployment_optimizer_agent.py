#!/usr/bin/env python3
"""Deployment optimizer agent.

Builds a latency + credential + DNS aware deployment plan for the HF lane and
multi-market agent topology. This closes the loop on:
  - "sub-second logic but not true low-latency live readiness"
  - missing venue credentials
  - region/datacenter migration prioritization
"""

import argparse
import json
import logging
import os
import socket
import sqlite3
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent))

try:
    from execution_telemetry import venue_health_snapshot
except Exception:
    try:
        from agents.execution_telemetry import venue_health_snapshot  # type: ignore
    except Exception:
        def venue_health_snapshot(*_args, **_kwargs):
            return {}

try:
    from fly_deployer import DEPLOYMENT_MANIFEST, get_deployment_plan, get_region
except Exception:
    try:
        from agents.fly_deployer import DEPLOYMENT_MANIFEST, get_deployment_plan, get_region  # type: ignore
    except Exception:
        DEPLOYMENT_MANIFEST = {}

        def get_deployment_plan():
            return {}

        def get_region():
            return "local"

try:
    import claude_duplex
except Exception:
    try:
        from agents import claude_duplex  # type: ignore
    except Exception:
        claude_duplex = None  # type: ignore

BASE = Path(__file__).parent
STATUS_PATH = BASE / "deployment_optimizer_status.json"
PLAN_PATH = BASE / "deployment_optimizer_plan.json"
ROADMAP_MD_PATH = BASE / "deployment_optimizer_roadmap.md"
TELEMETRY_DB = BASE / "execution_telemetry.db"
TRACEROUTE_DB_CANDIDATES = [BASE / "traceroute.db", BASE.parent / "traceroute.db"]

DEFAULT_INTERVAL_SECONDS = int(os.environ.get("DEPLOYMENT_OPTIMIZER_INTERVAL_SECONDS", "300"))
TELEMETRY_WINDOW_MINUTES = int(os.environ.get("DEPLOYMENT_OPTIMIZER_TELEMETRY_WINDOW_MINUTES", "60"))
MIN_COINBASE_SUCCESS_RATE = float(os.environ.get("DEPLOYMENT_OPTIMIZER_MIN_CB_SUCCESS", "0.30"))
MAX_COINBASE_P90_MS = float(os.environ.get("DEPLOYMENT_OPTIMIZER_MAX_CB_P90_MS", "1200"))
MIN_FIX_TIMEOUT_MS = int(os.environ.get("DEPLOYMENT_OPTIMIZER_FIX_TIMEOUT_MS", "1200"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [deployment_optimizer] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE / "deployment_optimizer_agent.log")),
    ],
)
logger = logging.getLogger("deployment_optimizer")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _safe_bool_env(name):
    return bool(os.environ.get(name, "").strip())


def _extract_host(url):
    u = str(url or "").strip()
    if not u:
        return ""
    try:
        return (urlparse(u).hostname or "").strip()
    except Exception:
        return ""


def _dns_probe(host):
    host = str(host or "").strip()
    if not host:
        return {"host": host, "ok": False, "latency_ms": 0.0, "error": "empty_host"}
    t0 = time.perf_counter()
    try:
        socket.getaddrinfo(host, None)
        dt = (time.perf_counter() - t0) * 1000.0
        return {"host": host, "ok": True, "latency_ms": round(dt, 3), "error": ""}
    except Exception as e:
        dt = (time.perf_counter() - t0) * 1000.0
        return {"host": host, "ok": False, "latency_ms": round(dt, 3), "error": str(e)}


def _render_roadmap(plan):
    summary = plan.get("summary", {})
    readiness = plan.get("venue_readiness", {})
    regions = plan.get("region_ranking", [])
    actions = plan.get("priority_actions", [])

    lines = [
        "# Deployment Optimizer Roadmap",
        "",
        f"Updated: {plan.get('updated_at', '')}",
        "",
        "## Summary",
        f"- Runtime region: {summary.get('runtime_region', 'unknown')}",
        f"- Deployment score: {float(summary.get('deployment_score', 0.0) or 0.0):.2f}",
        f"- Live HF ready: {bool(summary.get('hf_live_ready', False))}",
        "",
        "## Venue Readiness",
    ]

    for venue, row in readiness.items():
        if not isinstance(row, dict):
            continue
        lines.append(
            f"- {venue}: live_ready={bool(row.get('live_ready', False))} "
            f"dns_ok={bool(row.get('dns_ok', False))} creds={bool(row.get('credentials_present', False))} "
            f"reason={row.get('reason', '')}"
        )

    lines += ["", "## Region Ranking"]
    for row in regions[:8]:
        lines.append(
            f"- {row.get('region')}: score={float(row.get('score', 0.0) or 0.0):.3f} "
            f"role={row.get('role', '')}"
        )

    lines += ["", "## Priority Actions"]
    for a in actions[:20]:
        lines.append(f"- {a}")

    return "\n".join(lines) + "\n"


class DeploymentOptimizerAgent:
    def __init__(self, interval_seconds=DEFAULT_INTERVAL_SECONDS):
        self.interval_seconds = max(30, int(interval_seconds))
        self.running = True
        self.cycle = 0

    def _credentials(self):
        fix_url = os.environ.get("FIX_GATEWAY_URL", "").strip()
        fix_timeout_ms = int(os.environ.get("FIX_TIMEOUT_MS", "0") or 0)
        fix_host = _extract_host(fix_url)
        return {
            "coinbase_api": _safe_bool_env("COINBASE_API_KEY_ID") and _safe_bool_env("COINBASE_API_KEY_SECRET"),
            "fix_gateway": bool(fix_url),
            "fix_api_key": _safe_bool_env("FIX_API_KEY"),
            "fix_gateway_url": fix_url,
            "fix_gateway_host": fix_host,
            "fix_timeout_ms": fix_timeout_ms,
            "ibkr_api": _safe_bool_env("IBKR_HOST") and _safe_bool_env("IBKR_PORT"),
            "etrade_api": _safe_bool_env("ETRADE_CONSUMER_KEY") and _safe_bool_env("ETRADE_CONSUMER_SECRET"),
        }

    def _telemetry(self):
        venues = ["coinbase", "ibkr", "etrade", "fix"]
        return {v: venue_health_snapshot(v, window_minutes=TELEMETRY_WINDOW_MINUTES) for v in venues}

    def _latest_api_errors(self, venue, limit=20):
        if not TELEMETRY_DB.exists():
            return []
        try:
            db = sqlite3.connect(str(TELEMETRY_DB))
            db.row_factory = sqlite3.Row
            rows = db.execute(
                """
                SELECT error_text
                FROM api_call_metrics
                WHERE venue=? AND ok=0
                ORDER BY id DESC
                LIMIT ?
                """,
                (str(venue), int(limit)),
            ).fetchall()
            db.close()
            out = [str(r["error_text"] or "").strip() for r in rows if str(r["error_text"] or "").strip()]
            return out
        except Exception:
            return []

    def _dns(self, credentials):
        hosts = {
            "api.coinbase.com",
            "api.exchange.coinbase.com",
        }
        if credentials.get("fix_gateway_host"):
            hosts.add(credentials["fix_gateway_host"])
        return {h: _dns_probe(h) for h in sorted(hosts)}

    def _venue_readiness(self, credentials, telemetry, dns):
        cb = telemetry.get("coinbase", {}) if isinstance(telemetry, dict) else {}
        cb_success = float(cb.get("success_rate", 0.0) or 0.0)
        cb_p90 = float(cb.get("p90_latency_ms", 0.0) or 0.0)
        cb_samples = int(cb.get("samples", 0) or 0)
        cb_dns_ok = bool(dns.get("api.coinbase.com", {}).get("ok", False) or dns.get("api.exchange.coinbase.com", {}).get("ok", False))
        cb_healthy = bool(cb_samples < 10 or cb_success >= MIN_COINBASE_SUCCESS_RATE)
        cb_latency_ok = bool(cb_p90 <= 0 or cb_p90 <= MAX_COINBASE_P90_MS)
        cb_creds = bool(credentials.get("coinbase_api", False))
        cb_live = bool(cb_dns_ok and cb_healthy and cb_latency_ok and cb_creds)

        fix_dns_ok = True
        if credentials.get("fix_gateway_host"):
            fix_dns_ok = bool(dns.get(credentials.get("fix_gateway_host"), {}).get("ok", False))
        fix_creds = bool(credentials.get("fix_gateway", False))
        fix_timeout_ok = int(credentials.get("fix_timeout_ms", 0) or 0) > 0
        fix_live = bool(fix_creds and fix_dns_ok and fix_timeout_ok)

        ibkr_creds = bool(credentials.get("ibkr_api", False))
        ibkr_samples = int((telemetry.get("ibkr", {}) or {}).get("samples", 0) or 0)
        ibkr_success = float((telemetry.get("ibkr", {}) or {}).get("success_rate", 0.0) or 0.0)
        ibkr_live = bool(ibkr_creds and (ibkr_samples < 5 or ibkr_success >= 0.25))

        return {
            "coinbase": {
                "live_ready": cb_live,
                "dns_ok": cb_dns_ok,
                "credentials_present": cb_creds,
                "success_rate": round(cb_success, 4),
                "samples": cb_samples,
                "p90_latency_ms": round(cb_p90, 3),
                "reason": (
                    "ready"
                    if cb_live
                    else "requires_dns+credentials+healthy_telemetry"
                ),
            },
            "fix": {
                "live_ready": fix_live,
                "dns_ok": fix_dns_ok,
                "credentials_present": fix_creds,
                "timeout_ms": int(credentials.get("fix_timeout_ms", 0) or 0),
                "reason": "ready" if fix_live else "requires_gateway_url+dns+timeout",
            },
            "ibkr": {
                "live_ready": ibkr_live,
                "dns_ok": True,
                "credentials_present": ibkr_creds,
                "success_rate": round(ibkr_success, 4),
                "samples": ibkr_samples,
                "reason": "ready" if ibkr_live else "requires_host_port+health",
            },
        }

    def _traceroute_summary(self):
        chosen = None
        for p in TRACEROUTE_DB_CANDIDATES:
            if p.exists():
                chosen = p
                break
        if chosen is None:
            return {"db_path": "", "rows": [], "available": False}

        try:
            db = sqlite3.connect(str(chosen))
            db.row_factory = sqlite3.Row
            rows = db.execute(
                """
                SELECT target_host, category, COUNT(*) AS samples,
                       AVG(total_rtt) AS avg_rtt_ms,
                       MIN(total_rtt) AS min_rtt_ms,
                       MAX(total_rtt) AS max_rtt_ms
                FROM scan_metrics
                GROUP BY target_host, category
                ORDER BY samples DESC
                LIMIT 120
                """
            ).fetchall()
            db.close()
            out = []
            for r in rows:
                out.append(
                    {
                        "target_host": str(r["target_host"] or ""),
                        "category": str(r["category"] or ""),
                        "samples": int(r["samples"] or 0),
                        "avg_rtt_ms": round(float(r["avg_rtt_ms"] or 0.0), 3),
                        "min_rtt_ms": round(float(r["min_rtt_ms"] or 0.0), 3),
                        "max_rtt_ms": round(float(r["max_rtt_ms"] or 0.0), 3),
                    }
                )
            return {"db_path": str(chosen), "rows": out, "available": True}
        except Exception:
            return {"db_path": str(chosen), "rows": [], "available": False}

    def _region_ranking(self, readiness, telemetry):
        plan = get_deployment_plan() if callable(get_deployment_plan) else {}
        runtime_region = get_region() if callable(get_region) else "local"
        rows = []

        cb = readiness.get("coinbase", {}) if isinstance(readiness, dict) else {}
        cb_live = bool(cb.get("live_ready", False))
        cb_success = float((telemetry.get("coinbase", {}) or {}).get("success_rate", 0.0) or 0.0)
        cb_p90 = float((telemetry.get("coinbase", {}) or {}).get("p90_latency_ms", 0.0) or 0.0)
        ib_live = bool((readiness.get("ibkr", {}) or {}).get("live_ready", False))

        for region, info in plan.items():
            exchanges = [str(x).lower() for x in info.get("exchanges", [])]
            score = 0.22
            reasons = []

            if region == runtime_region:
                score += 0.05
                reasons.append("runtime_region")

            if "coinbase" in exchanges:
                score += 0.24
                score += 0.12 if cb_live else 0.02
                reasons.append("coinbase_primary")
            else:
                # Current HF lane is crypto-spot first; non-coinbase regions are secondary for now.
                score -= 0.06

            if any(x in exchanges for x in ("cme", "nymex", "cbot", "cboe")):
                score += 0.13 if ib_live else 0.05
                reasons.append("futures_proximity")

            if any(x in exchanges for x in ("bitflyer", "liquid", "gmo_coin", "sgx", "bybit", "okx", "binance")):
                score += 0.08
                reasons.append("asia_liquidity")

            if cb_success >= MIN_COINBASE_SUCCESS_RATE:
                score += 0.10
            else:
                score -= 0.08
                reasons.append("coinbase_success_degraded")

            if cb_p90 > 0:
                if cb_p90 <= MAX_COINBASE_P90_MS:
                    score += 0.07
                elif cb_p90 >= 2 * MAX_COINBASE_P90_MS:
                    score -= 0.10
                    reasons.append("coinbase_latency_high")

            rows.append(
                {
                    "region": region,
                    "score": round(max(0.0, min(1.0, score)), 4),
                    "role": info.get("role", ""),
                    "vm_recommendation": info.get("vm_recommendation", "shared-cpu-1x"),
                    "agent_count": int(info.get("agent_count", 0) or 0),
                    "reasons": reasons,
                }
            )

        rows.sort(key=lambda x: x["score"], reverse=True)
        return rows

    def _priority_actions(self, credentials, readiness, dns, telemetry, traceroute):
        actions = []
        cb = readiness.get("coinbase", {})
        fix = readiness.get("fix", {})
        ib = readiness.get("ibkr", {})

        if not cb.get("credentials_present", False):
            actions.append("Set COINBASE_API_KEY_ID and COINBASE_API_KEY_SECRET for direct venue execution.")
        if not cb.get("dns_ok", False):
            actions.append("Fix DNS resolution for api.coinbase.com/api.exchange.coinbase.com in execution runtime.")
        if cb.get("success_rate", 0.0) < MIN_COINBASE_SUCCESS_RATE:
            actions.append("Stabilize Coinbase API health before live budget escalation (improve retries + region routing).")

        if not fix.get("credentials_present", False):
            actions.append("Deploy FIX gateway and set FIX_GATEWAY_URL as live fallback route.")
        if fix.get("credentials_present", False) and int(fix.get("timeout_ms", 0) or 0) < MIN_FIX_TIMEOUT_MS:
            actions.append(f"Raise FIX_TIMEOUT_MS to at least {MIN_FIX_TIMEOUT_MS} for stable routing.")

        if not ib.get("credentials_present", False):
            actions.append("Set IBKR_HOST/IBKR_PORT (and gateway process) for futures/equity routing.")

        dns_fail = [h for h, row in dns.items() if not bool((row or {}).get("ok", False))]
        if dns_fail:
            actions.append("Add redundant DNS resolvers and egress checks; unresolved hosts: " + ", ".join(sorted(dns_fail)))

        cb_errors = self._latest_api_errors("coinbase", limit=30)
        if cb_errors:
            top = []
            counts = {}
            for err in cb_errors:
                key = err[:120]
                counts[key] = counts.get(key, 0) + 1
            for key, cnt in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:3]:
                top.append(f"{cnt}x {key}")
            actions.append("Top recent Coinbase failures: " + " | ".join(top))

        if not traceroute.get("available", False) or not traceroute.get("rows"):
            actions.append("Start continuous traceroute sampling to venue hosts for region-level routing evidence.")

        cb_p90 = float((telemetry.get("coinbase", {}) or {}).get("p90_latency_ms", 0.0) or 0.0)
        if cb_p90 > MAX_COINBASE_P90_MS:
            actions.append(
                f"Coinbase p90 latency {cb_p90:.1f}ms is high; migrate HF execution to ewr/ord and compare against nrt/sin backups."
            )

        if not actions:
            actions.append("No blockers detected; continue phased budget escalation with realized-PnL evidence.")
        return actions

    def run_cycle(self):
        self.cycle += 1
        credentials = self._credentials()
        telemetry = self._telemetry()
        dns = self._dns(credentials)
        readiness = self._venue_readiness(credentials, telemetry, dns)
        traceroute = self._traceroute_summary()
        regions = self._region_ranking(readiness, telemetry)
        actions = self._priority_actions(credentials, readiness, dns, telemetry, traceroute)

        top_scores = [float(r.get("score", 0.0) or 0.0) for r in regions[:3]]
        deployment_score = statistics.mean(top_scores) if top_scores else 0.0
        hf_live_ready = bool((readiness.get("coinbase", {}) or {}).get("live_ready", False) or (readiness.get("fix", {}) or {}).get("live_ready", False))

        plan = {
            "updated_at": _now_iso(),
            "cycle": int(self.cycle),
            "summary": {
                "runtime_region": str(get_region() if callable(get_region) else "local"),
                "deployment_score": round(float(deployment_score), 4),
                "hf_live_ready": bool(hf_live_ready),
                "top_regions": [r.get("region") for r in regions[:3]],
            },
            "credential_matrix": credentials,
            "dns_probes": dns,
            "telemetry": telemetry,
            "venue_readiness": readiness,
            "region_ranking": regions,
            "traceroute_summary": traceroute,
            "priority_actions": actions,
            "source_manifest_regions": sorted(list(DEPLOYMENT_MANIFEST.keys())) if isinstance(DEPLOYMENT_MANIFEST, dict) else [],
        }

        PLAN_PATH.write_text(json.dumps(plan, indent=2))
        ROADMAP_MD_PATH.write_text(_render_roadmap(plan))

        status = {
            "updated_at": _now_iso(),
            "running": bool(self.running),
            "cycle": int(self.cycle),
            "interval_seconds": int(self.interval_seconds),
            "runtime_region": plan["summary"]["runtime_region"],
            "deployment_score": plan["summary"]["deployment_score"],
            "hf_live_ready": plan["summary"]["hf_live_ready"],
            "top_regions": plan["summary"]["top_regions"],
            "priority_action_count": len(actions),
            "files": {
                "plan": str(PLAN_PATH),
                "roadmap": str(ROADMAP_MD_PATH),
                "status": str(STATUS_PATH),
            },
        }
        STATUS_PATH.write_text(json.dumps(status, indent=2))

        if claude_duplex:
            try:
                claude_duplex.send_to_claude(
                    message=(
                        f"deployment_optimizer cycle={self.cycle} score={plan['summary']['deployment_score']:.2f} "
                        f"hf_live_ready={plan['summary']['hf_live_ready']} top_regions={','.join(plan['summary']['top_regions'])}"
                    ),
                    msg_type="deployment_optimizer",
                    priority="high" if not hf_live_ready else "normal",
                    source="deployment_optimizer_agent",
                    meta={
                        "summary": plan["summary"],
                        "venue_readiness": readiness,
                        "priority_actions": actions[:12],
                    },
                )
            except Exception:
                pass

        logger.info(
            "cycle=%d score=%.2f hf_live_ready=%s top_regions=%s",
            self.cycle,
            float(plan["summary"]["deployment_score"]),
            bool(plan["summary"]["hf_live_ready"]),
            ",".join(plan["summary"]["top_regions"]),
        )

    def run_loop(self):
        logger.info("starting deployment_optimizer loop interval=%ss", self.interval_seconds)
        while self.running:
            t0 = time.perf_counter()
            try:
                self.run_cycle()
            except Exception as e:
                logger.error("cycle_failed: %s", e, exc_info=True)
            elapsed = time.perf_counter() - t0
            sleep_for = max(1, int(self.interval_seconds - elapsed))
            for _ in range(sleep_for):
                if not self.running:
                    break
                time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="Deployment optimizer agent")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    agent = DeploymentOptimizerAgent(interval_seconds=args.interval)
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
