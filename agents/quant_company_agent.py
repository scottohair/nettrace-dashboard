#!/usr/bin/env python3
"""Quant company control-plane agent.

Converts the trading stack into an operator-ready company layer:
  - Migration strategy (infrastructure + connectors + regions)
  - Market strategy (pairs, venues, budgets, reserves)
  - Go-to-market readiness gates and launch checklist

Writes:
  - quant_company_master_plan.json
  - quant_company_status.json
  - quant_company_roadmap.md
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
    import claude_duplex
except Exception:
    try:
        from agents import claude_duplex  # type: ignore
    except Exception:
        claude_duplex = None  # type: ignore

BASE = Path(__file__).parent
MASTER_PLAN_PATH = BASE / "quant_company_master_plan.json"
STATUS_PATH = BASE / "quant_company_status.json"
ROADMAP_MD_PATH = BASE / "quant_company_roadmap.md"

GROWTH_REPORT = BASE / "growth_go_no_go_report.json"
QUANT_RESULTS = BASE / "quant_100_results.json"
MCP_OPPS = BASE / "mcp_opportunities.json"
TREASURY_REGISTRY = BASE / "treasury_registry.json"
MARKET_HUB_STATUS = BASE / "market_connector_hub_status.json"
FLYWHEEL_STATUS = BASE / "flywheel_status.json"
DEPLOYMENT_OPT_STATUS = BASE / "deployment_optimizer_status.json"
DEPLOYMENT_OPT_PLAN = BASE / "deployment_optimizer_plan.json"
ORCHESTRATOR_DB = BASE / "orchestrator.db"
TRADER_DB = BASE / "trader.db"

WIN_OBJECTIVE_TEXT = (
    "WIN = maximize mathematically validated, risk-governed realized gains "
    "with resource-efficient multi-path execution and treasury capture in USD/USDC."
)
WIN_TREASURY_ASSETS = ["USD", "USDC"]


def _parse_target_list(raw, fallback):
    text = str(raw or "").strip()
    if not text:
        return list(fallback)
    out = []
    for tok in text.split(","):
        t = tok.strip()
        if not t:
            continue
        try:
            v = float(t.replace("_", ""))
        except Exception:
            continue
        if v > 0:
            out.append(v)
    if not out:
        return list(fallback)
    return sorted(set(out))


DAILY_GAIN_TARGETS_USD = _parse_target_list(
    os.environ.get("QUANT_COMPANY_DAILY_TARGETS_USD", ""),
    [
        1_000.0,
        2_500.0,
        9_888.0,
        15_600.0,
        1_000_000.0,
        9_999_999.0,
    ],
)

DEFAULT_INTERVAL_SECONDS = 360
REALIZED_LOOKBACK_HOURS = int(os.environ.get("QUANT_COMPANY_REALIZED_LOOKBACK_HOURS", "48"))
REALIZED_MIN_POSITIVE_WINDOWS = int(os.environ.get("QUANT_COMPANY_REALIZED_MIN_POS_WINDOWS", "3"))
REALIZED_WINDOW_HOURS = int(os.environ.get("QUANT_COMPANY_REALIZED_WINDOW_HOURS", "4"))
REALIZED_MIN_CLOSES_PER_WINDOW = int(os.environ.get("QUANT_COMPANY_REALIZED_MIN_CLOSES_PER_WINDOW", "1"))
REALIZED_MIN_NET_PNL_USD = float(os.environ.get("QUANT_COMPANY_REALIZED_MIN_NET_PNL_USD", "0.01"))
EXECUTION_HEALTH_ESCALATION_GATE = os.environ.get(
    "EXECUTION_HEALTH_ESCALATION_GATE", "1"
).lower() not in ("0", "false", "no")

try:
    from execution_health import evaluate_execution_health
except Exception:
    try:
        from agents.execution_health import evaluate_execution_health  # type: ignore
    except Exception:
        evaluate_execution_health = None  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [quant_company] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE / "quant_company_agent.log")),
    ],
)
logger = logging.getLogger("quant_company_agent")


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


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


def _latest_portfolio_metrics():
    if not ORCHESTRATOR_DB.exists():
        return {"daily_pnl_usd": 0.0, "total_value_usd": 0.0, "drawdown_pct": 0.0}
    try:
        conn = sqlite3.connect(str(ORCHESTRATOR_DB))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT daily_pnl, total_value_usd, drawdown_pct
            FROM portfolio_history
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        conn.close()
        if not row:
            return {"daily_pnl_usd": 0.0, "total_value_usd": 0.0, "drawdown_pct": 0.0}
        return {
            "daily_pnl_usd": float(row["daily_pnl"] or 0.0),
            "total_value_usd": float(row["total_value_usd"] or 0.0),
            "drawdown_pct": float(row["drawdown_pct"] or 0.0),
        }
    except Exception:
        return {"daily_pnl_usd": 0.0, "total_value_usd": 0.0, "drawdown_pct": 0.0}


def _realized_close_evidence():
    """Aggregate realized close performance from trader ledger for escalation gates."""
    if not TRADER_DB.exists():
        return {
            "passed": False,
            "reason": "trader_db_missing",
            "lookback_hours": int(REALIZED_LOOKBACK_HOURS),
            "window_hours": int(REALIZED_WINDOW_HOURS),
            "positive_windows": 0,
            "required_positive_windows": int(REALIZED_MIN_POSITIVE_WINDOWS),
            "total_closes": 0,
            "total_net_pnl_usd": 0.0,
            "windows": [],
        }

    try:
        conn = sqlite3.connect(str(TRADER_DB))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                CAST((strftime('%s', created_at) / (? * 3600)) AS INTEGER) AS bucket_id,
                COUNT(CASE WHEN pnl IS NOT NULL THEN 1 END) AS closes,
                SUM(COALESCE(pnl, 0)) AS net_pnl
            FROM agent_trades
            WHERE side='SELL'
              AND LOWER(COALESCE(status, '')) IN ('filled', 'closed', 'executed', 'partial_filled', 'partially_filled', 'settled')
              AND created_at >= datetime('now', ?)
            GROUP BY bucket_id
            ORDER BY bucket_id DESC
            """,
            (max(1, int(REALIZED_WINDOW_HOURS)), f"-{max(1, int(REALIZED_LOOKBACK_HOURS))} hours"),
        ).fetchall()
        conn.close()
    except Exception:
        rows = []

    windows = []
    total_closes = 0
    total_pnl = 0.0
    positive_windows = 0
    for r in rows:
        closes = int(r["closes"] or 0)
        net_pnl = float(r["net_pnl"] or 0.0)
        total_closes += closes
        total_pnl += net_pnl
        is_positive = closes >= REALIZED_MIN_CLOSES_PER_WINDOW and net_pnl > 0
        if is_positive:
            positive_windows += 1
        windows.append(
            {
                "bucket_id": int(r["bucket_id"] or 0),
                "closes": closes,
                "net_pnl_usd": round(net_pnl, 6),
                "positive_window": bool(is_positive),
            }
        )

    passed = (
        positive_windows >= REALIZED_MIN_POSITIVE_WINDOWS
        and total_pnl >= REALIZED_MIN_NET_PNL_USD
        and total_closes >= REALIZED_MIN_CLOSES_PER_WINDOW
    )
    reason = "passed"
    if not passed:
        if total_closes < REALIZED_MIN_CLOSES_PER_WINDOW:
            reason = "insufficient_realized_closes"
        elif total_pnl < REALIZED_MIN_NET_PNL_USD:
            reason = "realized_pnl_below_threshold"
        elif positive_windows < REALIZED_MIN_POSITIVE_WINDOWS:
            reason = "insufficient_positive_realized_windows"

    return {
        "passed": bool(passed),
        "reason": reason,
        "lookback_hours": int(REALIZED_LOOKBACK_HOURS),
        "window_hours": int(REALIZED_WINDOW_HOURS),
        "positive_windows": int(positive_windows),
        "required_positive_windows": int(REALIZED_MIN_POSITIVE_WINDOWS),
        "total_closes": int(total_closes),
        "total_net_pnl_usd": round(float(total_pnl), 6),
        "windows": windows[:24],
    }


def _target_tracker(metrics):
    pnl = float(metrics.get("daily_pnl_usd", 0.0) or 0.0)
    completed = [t for t in DAILY_GAIN_TARGETS_USD if pnl >= t]
    next_target = next((t for t in DAILY_GAIN_TARGETS_USD if pnl < t), DAILY_GAIN_TARGETS_USD[-1])
    achieved_pct = (pnl / next_target) if next_target > 0 else 0.0
    shortfall = max(0.0, next_target - pnl)
    now = datetime.now(timezone.utc)
    elapsed_seconds = now.hour * 3600 + now.minute * 60 + now.second
    remaining_seconds = max(1, 86400 - elapsed_seconds)
    required_hourly_run_rate = shortfall / (remaining_seconds / 3600.0)
    return {
        "daily_targets_usd": DAILY_GAIN_TARGETS_USD,
        "completed_targets": completed,
        "next_target_usd": next_target,
        "daily_pnl_usd": round(pnl, 2),
        "achievement_pct_to_next": round(_clamp(achieved_pct, -5.0, 10.0), 4),
        "shortfall_usd": round(shortfall, 2),
        "day_elapsed_pct_utc": round(elapsed_seconds / 86400.0, 4),
        "required_hourly_run_rate_usd": round(required_hourly_run_rate, 2),
    }


def _execution_health_gate_status():
    payload = {
        "enabled": bool(EXECUTION_HEALTH_ESCALATION_GATE),
        "green": True,
        "reason": "gate_disabled",
        "updated_at": "",
    }
    if not EXECUTION_HEALTH_ESCALATION_GATE:
        return payload
    if evaluate_execution_health is None:
        payload["green"] = False
        payload["reason"] = "execution_health_module_unavailable"
        return payload
    try:
        health = evaluate_execution_health(refresh=False, probe_http=None, write_status=True)
    except Exception as e:
        payload["green"] = False
        payload["reason"] = f"execution_health_check_failed:{e}"
        return payload
    if not isinstance(health, dict):
        payload["green"] = False
        payload["reason"] = "execution_health_invalid_payload"
        return payload
    payload["green"] = bool(health.get("green", False))
    payload["reason"] = str(health.get("reason", "unknown"))
    payload["updated_at"] = str(health.get("updated_at", ""))
    return payload


def _budget_escalator(progress, metrics, target, realized, execution_health=None):
    pnl = float(metrics.get("daily_pnl_usd", 0.0) or 0.0)
    dd = float(metrics.get("drawdown_pct", 0.0) or 0.0)
    go_live = bool(progress.get("go_live", False))
    alpha = float(progress.get("alpha_score", 0.0) or 0.0)
    realized_passed = bool((realized or {}).get("passed", False))
    execution_green = bool((execution_health or {}).get("green", False))

    action = "hold"
    factor = 1.0
    reason = "neutral"

    if dd >= 8.0 or pnl < 0:
        action = "de_escalate"
        factor = 0.80
        reason = "drawdown_or_negative_pnl"
    elif not realized_passed:
        action = "de_escalate"
        factor = 0.88
        reason = f"realized_gate_failed:{(realized or {}).get('reason', 'unknown')}"
    elif EXECUTION_HEALTH_ESCALATION_GATE and not execution_green:
        action = "de_escalate"
        factor = 0.90
        reason = f"execution_health_gate_failed:{(execution_health or {}).get('reason', 'unknown')}"
    elif go_live and alpha >= 0.70 and pnl >= 0:
        action = "escalate"
        gap_ratio = _clamp(float(target.get("achievement_pct_to_next", 0.0) or 0.0), 0.0, 1.5)
        factor = 1.05 + min(0.20, (1.0 - min(1.0, gap_ratio)) * 0.20)
        reason = "positive_alpha_and_go_live"
    elif alpha < 0.50:
        action = "de_escalate"
        factor = 0.90
        reason = "alpha_below_floor"

    return {
        "action": action,
        "factor": round(float(factor), 4),
        "reason": reason,
        "constraints": {
            "requires_positive_realized_pnl": True,
            "requires_positive_realized_windows": int(REALIZED_MIN_POSITIVE_WINDOWS),
            "max_daily_drawdown_pct_for_escalation": 8.0,
            "requires_go_live": True,
            "requires_execution_health_green": bool(EXECUTION_HEALTH_ESCALATION_GATE),
        },
        "realized_evidence": {
            "passed": bool(realized_passed),
            "reason": str((realized or {}).get("reason", "")),
            "positive_windows": int((realized or {}).get("positive_windows", 0) or 0),
            "required_positive_windows": int((realized or {}).get("required_positive_windows", REALIZED_MIN_POSITIVE_WINDOWS) or REALIZED_MIN_POSITIVE_WINDOWS),
            "total_net_pnl_usd": float((realized or {}).get("total_net_pnl_usd", 0.0) or 0.0),
            "total_closes": int((realized or {}).get("total_closes", 0) or 0),
        },
        "execution_health": {
            "green": bool(execution_green),
            "reason": str((execution_health or {}).get("reason", "")),
            "updated_at": str((execution_health or {}).get("updated_at", "")),
        },
    }


def _score_progress(growth, quant, mcp, treasury, hub, deploy):
    decision = growth.get("decision", {}) if isinstance(growth, dict) else {}
    go_live = bool(decision.get("go_live", False))
    reasons = decision.get("reasons", []) if isinstance(decision.get("reasons"), list) else []

    q_summary = quant.get("summary", {}) if isinstance(quant, dict) else {}
    promoted = int(q_summary.get("promoted_warm", 0) or 0)
    retained_hot = int(q_summary.get("retained_hot", 0) or 0)
    no_data = int(q_summary.get("no_data", 0) or 0)

    opp_count = len(mcp) if isinstance(mcp, list) else 0
    top_scores = [float(x.get("score", 0.0) or 0.0) for x in (mcp or []) if isinstance(x, dict)]
    avg_score = statistics.mean(top_scores) if top_scores else 0.0

    retr = (treasury.get("retrievability", {}) if isinstance(treasury, dict) else {})
    retr_score = float(retr.get("total_score", 0.0) or 0.0)

    endpoints = hub.get("dashboard_endpoints", []) if isinstance(hub, dict) else []
    ok_endpoints = sum(1 for e in endpoints if bool((e or {}).get("ok", False)))
    endpoint_total = len(endpoints)

    deploy_score = float(((deploy or {}).get("summary", {}) or {}).get("deployment_score", 0.0) or 0.0)
    deploy_hf_ready = bool(((deploy or {}).get("summary", {}) or {}).get("hf_live_ready", False))
    deploy_regions = ((deploy or {}).get("summary", {}) or {}).get("top_regions", []) or []

    alpha_score = 0.25
    alpha_score += _clamp(promoted / 40.0, 0.0, 0.35)
    alpha_score += _clamp(retained_hot / 12.0, 0.0, 0.20)
    alpha_score += _clamp(avg_score, 0.0, 1.0) * 0.20
    alpha_score -= _clamp(no_data / 120.0, 0.0, 0.18)

    migration_score = 0.20
    migration_score += _clamp(retr_score, 0.0, 1.0) * 0.45
    migration_score += _clamp(ok_endpoints / max(1, endpoint_total), 0.0, 1.0) * 0.25
    migration_score += 0.10 if endpoint_total > 0 else 0.0
    migration_score += _clamp(deploy_score, 0.0, 1.0) * 0.20

    gtm_score = 0.20
    gtm_score += 0.30 if go_live else 0.05
    gtm_score += _clamp(opp_count / 50.0, 0.0, 0.20)
    gtm_score += _clamp((1.0 - len(reasons) / 8.0), 0.0, 0.25)

    return {
        "alpha_score": round(_clamp(alpha_score, 0.0, 1.0), 4),
        "migration_score": round(_clamp(migration_score, 0.0, 1.0), 4),
        "gtm_score": round(_clamp(gtm_score, 0.0, 1.0), 4),
        "go_live": go_live,
        "blockers": reasons,
        "opportunity_count": opp_count,
        "deployment_score": round(_clamp(deploy_score, 0.0, 1.0), 4),
        "hf_live_ready": bool(deploy_hf_ready),
        "top_regions": [str(r) for r in deploy_regions[:3]],
    }


def _build_market_strategy(mcp, quant):
    top_opps = [x for x in (mcp or []) if isinstance(x, dict)][:20]
    by_symbol = {}
    for row in top_opps:
        sym = str(row.get("symbol", "")).upper()
        if not sym:
            continue
        bucket = by_symbol.setdefault(sym, {"count": 0, "avg_score": 0.0, "avg_edge": 0.0, "sources": set(), "actions": set()})
        bucket["count"] += 1
        bucket["avg_score"] += float(row.get("score", 0.0) or 0.0)
        bucket["avg_edge"] += float(row.get("expected_edge_pct", 0.0) or 0.0)
        bucket["sources"].add(str(row.get("source", "")))
        bucket["actions"].add(str(row.get("action", "HOLD")))

    ranked_pairs = []
    for sym, agg in by_symbol.items():
        n = max(1, agg["count"])
        ranked_pairs.append(
            {
                "pair": sym,
                "signal_count": n,
                "avg_score": round(agg["avg_score"] / n, 4),
                "avg_edge_pct": round(agg["avg_edge"] / n, 4),
                "sources": sorted(agg["sources"]),
                "actions": sorted(agg["actions"]),
            }
        )
    ranked_pairs.sort(key=lambda x: (x["avg_score"], x["avg_edge_pct"]), reverse=True)

    q_top = []
    q_summary = quant.get("summary", {}) if isinstance(quant, dict) else {}
    for row in (q_summary.get("top_candidates", []) or [])[:12]:
        if not isinstance(row, dict):
            continue
        q_top.append(
            {
                "pair": str(row.get("pair", "")).upper(),
                "return_pct": float(row.get("total_return_pct", row.get("return_pct", 0.0)) or 0.0),
                "win_rate": float(row.get("win_rate", 0.0) or 0.0),
                "trades": int(row.get("total_trades", row.get("trades", 0)) or 0),
            }
        )

    return {
        "ranked_pairs": ranked_pairs[:12],
        "quant_priority_pairs": q_top,
        "market_tracks": [
            {
                "name": "crypto_spot_core",
                "objective": "scale validated BTC/ETH/SOL alpha with strict risk governor",
                "channel": "coinbase + smart_router",
                "launch_gate": "GO + realized close-profit evidence",
            },
            {
                "name": "multi_hop_arb",
                "objective": "deploy deterministic cross-venue loops with hard no-loss gate",
                "channel": "multi_hop_arb_engine",
                "launch_gate": "policy-approved net edge > cost floor",
            },
            {
                "name": "energy_proxy",
                "objective": "oil regime signals via futures/ETF proxies and IBKR onboarding",
                "channel": "oil_market_agent + ibkr_connector",
                "launch_gate": "connector readiness + positive paper expectancy",
            },
        ],
    }


def _build_migration_strategy(hub, treasury, flywheel, deploy):
    connectors = treasury.get("connectors", []) if isinstance(treasury, dict) else []
    retr = treasury.get("retrievability", {}) if isinstance(treasury, dict) else {}
    fly = flywheel if isinstance(flywheel, dict) else {}
    deploy_plan = deploy if isinstance(deploy, dict) else {}
    deploy_summary = (deploy_plan.get("summary", {}) if isinstance(deploy_plan, dict) else {}) or {}
    deploy_actions = deploy_plan.get("priority_actions", []) if isinstance(deploy_plan, dict) else []
    deploy_regions = deploy_plan.get("region_ranking", []) if isinstance(deploy_plan, dict) else []

    endpoint_rows = hub.get("dashboard_endpoints", []) if isinstance(hub, dict) else []
    endpoints_ok = sum(1 for r in endpoint_rows if bool((r or {}).get("ok", False)))

    connector_ready = sum(1 for c in connectors if bool((c or {}).get("ready", False)))
    connector_total = len(connectors)

    phases = [
        {
            "phase": 1,
            "name": "platform_hardening",
            "goals": [
                "stabilize agent uptime and restart loops",
                "enforce no-loss + Monte Carlo promotion gates",
                "establish treasury retrievability score >= 0.80",
            ],
            "kpi": {
                "retrievability_score_target": 0.80,
                "connector_ready_ratio_target": 0.60,
            },
            "status": "in_progress" if float(retr.get("total_score", 0.0) or 0.0) < 0.80 else "complete",
        },
        {
            "phase": 2,
            "name": "market_connector_migration",
            "goals": [
                "promote market connector hub to primary routing layer",
                "enable IBKR/E*Trade where credentials + infra are ready",
                "introduce region-aware execution based on latency health",
                "enforce HF live gate with DNS + credential + telemetry checks",
            ],
            "kpi": {
                "dashboard_endpoint_health_target": 1.0,
                "connector_ready_ratio_target": 0.75,
                "deployment_score_target": 0.70,
            },
            "status": "in_progress" if endpoints_ok < max(1, len(endpoint_rows)) else "ready",
        },
        {
            "phase": 3,
            "name": "go_to_market_rollout",
            "goals": [
                "launch alpha-backed strategy offerings with auditable metrics",
                "expose dashboard KPI surfaces for operators/investors",
                "publish reserve and risk disclosures for trust",
            ],
            "kpi": {
                "go_live_required": True,
                "positive_realized_pnl_windows": 3,
                "hf_live_ready_required": True,
            },
            "status": "blocked",
        },
    ]

    if connector_total > 0 and connector_ready / connector_total >= 0.75 and endpoints_ok >= max(1, len(endpoint_rows)):
        phases[1]["status"] = "ready"

    return {
        "phases": phases,
        "current_connector_ready": {"ready": connector_ready, "total": connector_total},
        "dashboard_endpoint_health": {"ok": endpoints_ok, "total": len(endpoint_rows)},
        "flywheel_cycle": fly.get("cycle", 0),
        "deployment_optimizer": {
            "score": float(deploy_summary.get("deployment_score", 0.0) or 0.0),
            "hf_live_ready": bool(deploy_summary.get("hf_live_ready", False)),
            "top_regions": [str(x) for x in (deploy_summary.get("top_regions", []) or [])[:5]],
            "priority_actions": [str(x) for x in (deploy_actions or [])[:12]],
            "region_ranking": [
                {
                    "region": str(r.get("region", "")),
                    "score": float(r.get("score", 0.0) or 0.0),
                    "role": str(r.get("role", "")),
                }
                for r in (deploy_regions or [])[:8]
                if isinstance(r, dict)
            ],
        },
    }


def _build_gtm_strategy(progress, market, migration):
    blockers = list(progress.get("blockers", []))
    hf_live_ready = bool(progress.get("hf_live_ready", False))
    launch_ready = (
        bool(progress.get("go_live", False))
        and float(progress.get("alpha_score", 0.0) or 0.0) >= 0.60
        and hf_live_ready
    )
    if not hf_live_ready:
        blockers.append("hf_live_not_ready")

    stages = [
        {
            "stage": "private_alpha",
            "audience": "internal capital only",
            "offer": "risk-capped automated strategy basket",
            "success_metric": "3 consecutive positive realized-PnL windows",
            "status": "ready" if launch_ready else "in_progress",
        },
        {
            "stage": "partner_beta",
            "audience": "small external allocator cohort",
            "offer": "dashboard + execution transparency + reserve reporting",
            "success_metric": "positive risk-adjusted returns with <5% drawdown band",
            "status": "blocked" if blockers else "planned",
        },
        {
            "stage": "public_launch",
            "audience": "broader market",
            "offer": "multi-market quant platform + treasury custody controls",
            "success_metric": "stable alpha + audited controls + compliant onboarding",
            "status": "planned",
        },
    ]

    return {
        "launch_ready": launch_ready,
        "blockers": blockers,
        "stages": stages,
        "top_market_targets": market.get("ranked_pairs", [])[:6],
        "migration_prereqs": migration.get("phases", []),
    }


def _profit_task_queue(progress, metrics, realized, deploy, market, targets, execution_health=None):
    tasks = []
    pnl = float(metrics.get("daily_pnl_usd", 0.0) or 0.0)
    req_rate = float(targets.get("required_hourly_run_rate_usd", 0.0) or 0.0)
    realized_reason = str(realized.get("reason", "unknown"))
    deploy_actions = (deploy.get("priority_actions", []) if isinstance(deploy, dict) else []) or []
    top_regions = (progress.get("top_regions", []) if isinstance(progress, dict) else []) or []
    ranked_pairs = market.get("ranked_pairs", []) if isinstance(market, dict) else []
    exec_green = bool((execution_health or {}).get("green", False))
    exec_reason = str((execution_health or {}).get("reason", "unknown"))

    if pnl <= 0:
        tasks.append("Raise realized close frequency: prioritize strategies with deterministic exits and net-positive close expectancy.")
    if not bool(realized.get("passed", False)):
        tasks.append(
            f"Resolve realized gate failure ({realized_reason}): require >= {REALIZED_MIN_POSITIVE_WINDOWS} positive close windows before budget escalation."
        )
    if not bool(progress.get("hf_live_ready", False)):
        tasks.append("HF live gate is blocked; keep HF lane in paper mode and fix DNS + venue credentials before live budget.")
        for action in deploy_actions[:3]:
            tasks.append(str(action))
    if EXECUTION_HEALTH_ESCALATION_GATE and not exec_green:
        tasks.append(f"Execution-health gate failed ({exec_reason}); block budget escalations until DNS/API/reconcile checks are green.")
    if req_rate > 0:
        tasks.append(f"Current run-rate gap: need ${req_rate:,.2f}/hour to hit next daily target ${float(targets.get('next_target_usd', 0.0) or 0.0):,.2f}.")
    if top_regions:
        tasks.append("Deploy primary execution to region order: " + " -> ".join(str(r) for r in top_regions[:3]) + ".")
    if ranked_pairs:
        top_pairs = [str(r.get("pair", "")) for r in ranked_pairs[:3] if str(r.get("pair", ""))]
        if top_pairs:
            tasks.append("Focus quant sweeps + walk-forward + Monte Carlo on top pairs: " + ", ".join(top_pairs) + ".")
    tasks.append("Run base-10 and hexadecimal radix feature experiments on microstructure deltas; promote only if out-of-sample realized PnL improves.")
    tasks.append("Apply network-stack tuning (DNS resilience, timeout policy, socket path efficiency) to reduce execution latency variance.")
    tasks.append("Continuously harvest realized gains into treasury assets: USD and USDC.")
    tasks.append("Prefer strategies with fast, repeatable close cycles that improve realized USD/USDC run-rate.")

    tasks.append("Send Claude a high-priority directive each cycle with blockers, required run-rate, and top migration actions.")
    tasks.append("Promote only strategies with positive realized PnL evidence; de-escalate automatically on drawdown or failed close windows.")

    # Deduplicate while preserving order.
    out = []
    seen = set()
    for t in tasks:
        key = t.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out[:20]


def _render_roadmap(plan):
    progress = plan.get("progress", {})
    realized = plan.get("realized_close_evidence", {})
    targets = plan.get("profit_target_tracker", {})
    escalator = plan.get("budget_escalator", {})
    migration = plan.get("migration_strategy", {})
    market = plan.get("market_strategy", {})
    gtm = plan.get("go_to_market_strategy", {})
    win_objective = (plan.get("win_objective", {}) if isinstance(plan, dict) else {}) or {}

    lines = [
        "# Quant Company Roadmap",
        "",
        f"Updated: {plan.get('updated_at', '')}",
        "",
        "## WIN Objective",
        f"- {win_objective.get('definition', WIN_OBJECTIVE_TEXT)}",
        f"- Treasury capture assets: {', '.join(win_objective.get('treasury_capture_assets', WIN_TREASURY_ASSETS))}",
        "",
        "## Scores",
        f"- Alpha score: {progress.get('alpha_score', 0.0):.2f}",
        f"- Migration score: {progress.get('migration_score', 0.0):.2f}",
        f"- GTM score: {progress.get('gtm_score', 0.0):.2f}",
        f"- Deployment score: {progress.get('deployment_score', 0.0):.2f}",
        f"- GO live: {progress.get('go_live', False)}",
        f"- HF live ready: {progress.get('hf_live_ready', False)}",
        "",
        "## Profit Targets",
        f"- Daily PnL: ${float(targets.get('daily_pnl_usd', 0.0) or 0.0):,.2f}",
        f"- Next target: ${float(targets.get('next_target_usd', DAILY_GAIN_TARGETS_USD[-1]) or DAILY_GAIN_TARGETS_USD[-1]):,.2f}",
        f"- Target progress: {float(targets.get('achievement_pct_to_next', 0.0) or 0.0):.2%}",
        f"- Required run-rate: ${float(targets.get('required_hourly_run_rate_usd', 0.0) or 0.0):,.2f}/hour",
        f"- Budget escalator: {escalator.get('action', 'hold')} x{float(escalator.get('factor', 1.0) or 1.0):.2f}",
        f"- Realized close gate: passed={bool(realized.get('passed', False))} reason={realized.get('reason', '')}",
        "",
        "## Migration Phases",
    ]
    for ph in migration.get("phases", []):
        lines.append(f"- Phase {ph.get('phase')}: {ph.get('name')} [{ph.get('status')}]")

    dep = migration.get("deployment_optimizer", {}) if isinstance(migration, dict) else {}
    if dep:
        lines += ["", "## Region Targets"]
        for row in dep.get("region_ranking", [])[:6]:
            lines.append(
                f"- {row.get('region')}: score={float(row.get('score', 0.0) or 0.0):.2f} role={row.get('role', '')}"
            )

    lines += ["", "## Market Priorities"]
    for row in market.get("ranked_pairs", [])[:8]:
        lines.append(
            f"- {row.get('pair')}: score={row.get('avg_score', 0.0):.2f}, edge={row.get('avg_edge_pct', 0.0):.3f}%"
        )

    lines += ["", "## GTM Stages"]
    for st in gtm.get("stages", []):
        lines.append(f"- {st.get('stage')}: {st.get('status')} ({st.get('offer')})")

    if gtm.get("blockers"):
        lines += ["", "## Blockers"]
        for b in gtm.get("blockers", []):
            lines.append(f"- {b}")

    tasks = plan.get("profit_task_queue", [])
    if tasks:
        lines += ["", "## Profit Task Queue"]
        for t in tasks[:12]:
            lines.append(f"- {t}")

    return "\n".join(lines) + "\n"


class QuantCompanyAgent:
    def __init__(self, interval_seconds=DEFAULT_INTERVAL_SECONDS):
        self.interval_seconds = max(30, int(interval_seconds))
        self.running = True
        self.cycle = 0

    def run_cycle(self):
        self.cycle += 1

        growth = _load_json(GROWTH_REPORT, {})
        quant = _load_json(QUANT_RESULTS, {})
        mcp = _load_json(MCP_OPPS, [])
        treasury = _load_json(TREASURY_REGISTRY, {})
        hub = _load_json(MARKET_HUB_STATUS, {})
        fly = _load_json(FLYWHEEL_STATUS, {})
        deploy_status = _load_json(DEPLOYMENT_OPT_STATUS, {})
        deploy_plan = _load_json(DEPLOYMENT_OPT_PLAN, {})
        deploy = deploy_plan if isinstance(deploy_plan, dict) and deploy_plan else {"summary": deploy_status}
        metrics = _latest_portfolio_metrics()
        realized = _realized_close_evidence()
        execution_health = _execution_health_gate_status()

        progress = _score_progress(growth, quant, mcp, treasury, hub, deploy)
        market = _build_market_strategy(mcp, quant)
        migration = _build_migration_strategy(hub, treasury, fly, deploy)
        gtm = _build_gtm_strategy(progress, market, migration)
        targets = _target_tracker(metrics)
        escalator = _budget_escalator(progress, metrics, targets, realized, execution_health=execution_health)
        profit_tasks = _profit_task_queue(
            progress,
            metrics,
            realized,
            deploy,
            market,
            targets,
            execution_health=execution_health,
        )

        plan = {
            "updated_at": _now_iso(),
            "cycle": int(self.cycle),
            "win_objective": {
                "definition": WIN_OBJECTIVE_TEXT,
                "treasury_capture_assets": WIN_TREASURY_ASSETS,
            },
            "progress": progress,
            "portfolio_metrics": metrics,
            "realized_close_evidence": realized,
            "profit_target_tracker": targets,
            "budget_escalator": escalator,
            "execution_health": execution_health,
            "migration_strategy": migration,
            "market_strategy": market,
            "go_to_market_strategy": gtm,
            "profit_task_queue": profit_tasks,
            "inputs": {
                "growth_report": str(GROWTH_REPORT),
                "quant_results": str(QUANT_RESULTS),
                "mcp_opportunities": str(MCP_OPPS),
                "treasury_registry": str(TREASURY_REGISTRY),
                "market_hub_status": str(MARKET_HUB_STATUS),
                "flywheel_status": str(FLYWHEEL_STATUS),
                "deployment_optimizer_status": str(DEPLOYMENT_OPT_STATUS),
                "deployment_optimizer_plan": str(DEPLOYMENT_OPT_PLAN),
            },
        }

        MASTER_PLAN_PATH.write_text(json.dumps(plan, indent=2))
        ROADMAP_MD_PATH.write_text(_render_roadmap(plan))

        status = {
            "updated_at": _now_iso(),
            "running": bool(self.running),
            "cycle": int(self.cycle),
            "interval_seconds": int(self.interval_seconds),
            "alpha_score": progress.get("alpha_score", 0.0),
            "migration_score": progress.get("migration_score", 0.0),
            "gtm_score": progress.get("gtm_score", 0.0),
            "deployment_score": progress.get("deployment_score", 0.0),
            "hf_live_ready": progress.get("hf_live_ready", False),
            "top_regions": progress.get("top_regions", []),
            "go_live": progress.get("go_live", False),
            "launch_ready": gtm.get("launch_ready", False),
            "win_objective": WIN_OBJECTIVE_TEXT,
            "daily_pnl_usd": metrics.get("daily_pnl_usd", 0.0),
            "next_daily_target_usd": targets.get("next_target_usd", DAILY_GAIN_TARGETS_USD[-1]),
            "required_hourly_run_rate_usd": targets.get("required_hourly_run_rate_usd", 0.0),
            "budget_escalator_action": escalator.get("action", "hold"),
            "realized_gate_passed": bool(realized.get("passed", False)),
            "realized_gate_reason": str(realized.get("reason", "")),
            "execution_health_green": bool(execution_health.get("green", False)),
            "execution_health_reason": str(execution_health.get("reason", "")),
            "blockers": progress.get("blockers", []),
            "profit_task_queue": profit_tasks[:10],
            "files": {
                "master_plan": str(MASTER_PLAN_PATH),
                "status": str(STATUS_PATH),
                "roadmap": str(ROADMAP_MD_PATH),
            },
        }
        STATUS_PATH.write_text(json.dumps(status, indent=2))

        if claude_duplex:
            try:
                claude_duplex.send_to_claude(
                    message=(
                        f"quant_company cycle={self.cycle} alpha={progress['alpha_score']:.2f} "
                        f"migration={progress['migration_score']:.2f} gtm={progress['gtm_score']:.2f} "
                        f"launch_ready={gtm.get('launch_ready', False)} "
                        f"hf_live_ready={progress.get('hf_live_ready', False)} "
                        f"top_regions={','.join(progress.get('top_regions', [])[:3]) or 'n/a'} "
                        f"daily_pnl=${metrics.get('daily_pnl_usd', 0.0):.2f} "
                        f"next_target=${targets.get('next_target_usd', DAILY_GAIN_TARGETS_USD[-1]):,.2f} "
                        f"run_rate=${targets.get('required_hourly_run_rate_usd', 0.0):,.2f}/hr "
                        f"budget={escalator.get('action', 'hold')}x{escalator.get('factor', 1.0):.2f}"
                    ),
                    msg_type="company_control_plane",
                    priority="high" if not gtm.get("launch_ready", False) else "normal",
                    source="quant_company_agent",
                    meta={
                        "progress": progress,
                        "launch_ready": gtm.get("launch_ready", False),
                        "portfolio_metrics": metrics,
                        "realized_close_evidence": realized,
                        "target_tracker": targets,
                        "budget_escalator": escalator,
                        "profit_task_queue": profit_tasks[:12],
                        "win_objective": {
                            "definition": WIN_OBJECTIVE_TEXT,
                            "treasury_capture_assets": WIN_TREASURY_ASSETS,
                            "operator_directive": "Best-in-class: reuse proven building blocks; ship paradigm shifts only if they beat baselines.",
                        },
                    },
                )
            except Exception:
                pass

        logger.info(
            "cycle=%d alpha=%.2f migration=%.2f gtm=%.2f deploy=%.2f hf_live=%s daily_pnl=$%.2f next_target=$%.2f budget=%s",
            self.cycle,
            float(progress.get("alpha_score", 0.0) or 0.0),
            float(progress.get("migration_score", 0.0) or 0.0),
            float(progress.get("gtm_score", 0.0) or 0.0),
            float(progress.get("deployment_score", 0.0) or 0.0),
            bool(progress.get("hf_live_ready", False)),
            float(metrics.get("daily_pnl_usd", 0.0) or 0.0),
            float(targets.get("next_target_usd", DAILY_GAIN_TARGETS_USD[-1]) or DAILY_GAIN_TARGETS_USD[-1]),
            str(escalator.get("action", "hold")),
        )

    def run_loop(self):
        logger.info("starting quant_company_agent loop interval=%ss", self.interval_seconds)
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
    parser = argparse.ArgumentParser(description="Quant company control-plane agent")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    agent = QuantCompanyAgent(interval_seconds=args.interval)
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
