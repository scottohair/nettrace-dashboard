#!/usr/bin/env python3
"""WIN-1000 task backlog generator, prioritizer, and triage runner.

Creates a 1000-item task system, prioritizes against current runtime state,
works through every item (triage), and optionally executes top automation actions.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).parent
TASKS_PATH = BASE / "win_1000_tasks.json"
STATUS_PATH = BASE / "win_1000_status.json"
DEPLOYMENT_OPT_PLAN = BASE / "deployment_optimizer_plan.json"
QUANT_COMPANY_STATUS = BASE / "quant_company_status.json"
GROWTH_REPORT = BASE / "growth_go_no_go_report.json"
FLYWHEEL_STATUS = BASE / "flywheel_status.json"


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


def _save_json(path, payload):
    Path(path).write_text(json.dumps(payload, indent=2))


def _state_snapshot():
    deploy = _load_json(DEPLOYMENT_OPT_PLAN, {})
    quant = _load_json(QUANT_COMPANY_STATUS, {})
    growth = _load_json(GROWTH_REPORT, {})
    fly = _load_json(FLYWHEEL_STATUS, {})
    growth_decision = growth.get("decision", {}) if isinstance(growth, dict) else {}
    growth_reasons = growth_decision.get("reasons", []) if isinstance(growth_decision.get("reasons"), list) else []
    deploy_readiness = deploy.get("venue_readiness", {}) if isinstance(deploy, dict) else {}
    coinbase_ready = bool((deploy_readiness.get("coinbase", {}) or {}).get("live_ready", False))
    fix_ready = bool((deploy_readiness.get("fix", {}) or {}).get("live_ready", False))
    creds_coinbase = bool(os.environ.get("COINBASE_API_KEY_ID", "").strip() and os.environ.get("COINBASE_API_KEY_SECRET", "").strip())
    creds_fix = bool(os.environ.get("FIX_GATEWAY_URL", "").strip())
    dns_coinbase_ok = bool((deploy_readiness.get("coinbase", {}) or {}).get("dns_ok", False))
    realized_ok = bool(quant.get("realized_gate_passed", False))
    run_rate = float(quant.get("required_hourly_run_rate_usd", 0.0) or 0.0)
    go_live = bool(growth_decision.get("go_live", False))
    return {
        "go_live": go_live,
        "growth_reasons": growth_reasons,
        "coinbase_ready": coinbase_ready,
        "fix_ready": fix_ready,
        "creds_coinbase": creds_coinbase,
        "creds_fix": creds_fix,
        "dns_coinbase_ok": dns_coinbase_ok,
        "realized_ok": realized_ok,
        "run_rate_usd_per_hour": run_rate,
        "top_regions": quant.get("top_regions", []),
        "hf_live_ready": bool(quant.get("hf_live_ready", False)),
        "quant_status": quant,
        "growth_status": growth_decision,
        "flywheel_status": fly,
    }


def _tier_from_score(score):
    s = float(score)
    if s >= 90:
        return "P0"
    if s >= 75:
        return "P1"
    if s >= 60:
        return "P2"
    if s >= 45:
        return "P3"
    return "P4"


def _task_title(category, idx, pair, region):
    if category == "infra_dns_credentials":
        return f"Stabilize venue readiness {idx}: creds+dns gate for {pair} in {region}"
    if category == "execution_realized_close":
        return f"Increase realized close velocity {idx}: deterministic exits on {pair}"
    if category == "strategy_validation":
        return f"Validate strategy variant {idx} with OOS + Monte Carlo on {pair}"
    if category == "treasury_usd_usdc":
        return f"USD/USDC treasury capture optimization {idx} for {pair}"
    if category == "deployment_regions":
        return f"Region routing benchmark {idx}: compare latency and fill quality in {region}"
    if category == "claude_collab":
        return f"Claude sync task {idx}: request baseline vs paradigm-shift uplift on {pair}"
    return f"WIN task {idx}: improve {pair} execution in {region}"


def _generate_tasks():
    pairs = [
        "BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD",
        "DOGE-USD", "XRP-USD", "ADA-USD", "LTC-USD", "BCH-USD",
        "DOT-USD", "ATOM-USD", "UNI-USD", "MATIC-USD", "ARB-USD",
        "OP-USD", "AAVE-USD", "MKR-USD", "NEAR-USD", "SUI-USD",
        "PEPE-USD", "FET-USD", "INJ-USD", "ETC-USD", "XLM-USD",
    ]
    regions = ["ewr", "ord", "lhr", "fra", "nrt", "sin", "bom", "iad", "sjc", "ams"]

    # Exactly 1000 tasks.
    category_plan = [
        ("infra_dns_credentials", 180, 96),
        ("execution_realized_close", 220, 92),
        ("strategy_validation", 260, 80),
        ("treasury_usd_usdc", 140, 74),
        ("deployment_regions", 120, 68),
        ("claude_collab", 80, 62),
    ]

    out = []
    counter = 0
    for category, count, base_score in category_plan:
        for i in range(count):
            counter += 1
            pair = pairs[i % len(pairs)]
            region = regions[i % len(regions)]
            score = float(base_score - (i % 11))
            out.append(
                {
                    "id": f"WIN-{counter:04d}",
                    "title": _task_title(category, i + 1, pair, region),
                    "category": category,
                    "pair": pair,
                    "region": region,
                    "priority_score": round(score, 3),
                    "priority_tier": _tier_from_score(score),
                    "status": "pending",
                    "worked_through": False,
                    "blocked_reason": "",
                    "acceptance": "Positive OOS + Monte Carlo + realized close-profit evidence where applicable.",
                    "last_reviewed_at": "",
                    "notes": [],
                }
            )

    if len(out) != 1000:
        raise RuntimeError(f"task count mismatch: {len(out)} != 1000")
    return out


def _reprioritize(tasks, state):
    for t in tasks:
        score = float(t.get("priority_score", 50.0) or 50.0)
        cat = str(t.get("category", ""))
        if cat == "infra_dns_credentials":
            if not state["creds_coinbase"]:
                score += 8
            if not state["dns_coinbase_ok"]:
                score += 7
            if not state["coinbase_ready"]:
                score += 6
        if cat == "execution_realized_close" and not state["realized_ok"]:
            score += 9
        if cat == "treasury_usd_usdc" and float(state["run_rate_usd_per_hour"]) > 0:
            score += 4
        if cat == "deployment_regions" and not state["hf_live_ready"]:
            score += 3
        if "funding_concentration_above_cap" in state["growth_reasons"] and cat in {"strategy_validation", "execution_realized_close"}:
            score += 4
        t["priority_score"] = round(min(100.0, max(0.0, score)), 3)
        t["priority_tier"] = _tier_from_score(t["priority_score"])

    tasks.sort(key=lambda x: (float(x.get("priority_score", 0.0)), x.get("id", "")), reverse=True)
    for rank, task in enumerate(tasks, start=1):
        task["priority_rank"] = rank
    return tasks


def _triage_status(task, state):
    cat = str(task.get("category", ""))
    if cat == "infra_dns_credentials":
        if state["coinbase_ready"] and state["creds_coinbase"] and state["dns_coinbase_ok"]:
            return "completed", ""
        return "in_progress", "venue_readiness_incomplete"
    if cat == "execution_realized_close":
        if state["realized_ok"]:
            return "completed", ""
        return "in_progress", "realized_gate_pending"
    if cat == "strategy_validation":
        if state["go_live"]:
            return "in_progress", ""
        return "pending", ""
    if cat == "treasury_usd_usdc":
        if float(state["run_rate_usd_per_hour"]) <= 0:
            return "completed", ""
        return "in_progress", "run_rate_gap"
    if cat == "deployment_regions":
        if state["hf_live_ready"]:
            return "completed", ""
        return "in_progress", "hf_live_not_ready"
    if cat == "claude_collab":
        return "in_progress", ""
    return "pending", ""


def _run_cmd(cmd, env_overrides=None, timeout_seconds=180):
    env = os.environ.copy()
    if isinstance(env_overrides, dict):
        for k, v in env_overrides.items():
            env[str(k)] = str(v)
    started = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(BASE),
        capture_output=True,
        text=True,
        timeout=max(1, int(timeout_seconds)),
        env=env,
    )
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "elapsed_seconds": round(time.time() - started, 3),
        "stdout_tail": "\n".join((proc.stdout or "").splitlines()[-20:]),
        "stderr_tail": "\n".join((proc.stderr or "").splitlines()[-20:]),
    }


def _execute_actions(limit):
    actions = [
        [sys.executable, str(BASE / "rebalance_funded_budgets.py")],
        [sys.executable, str(BASE / "growth_supervisor.py"), "--collector-interval-seconds", "300", "--quant-run"],
        [sys.executable, str(BASE / "deployment_optimizer_agent.py"), "--once"],
        [sys.executable, str(BASE / "quant_company_agent.py"), "--once"],
        [sys.executable, str(BASE / "flywheel_controller.py"), "--once", "--no-claude-updates"],
        [sys.executable, str(BASE / "hf_execution_agent.py"), "--once", "--interval-ms", "400", "--pairs", "BTC-USDC,ETH-USDC,SOL-USDC"],
    ]
    if int(limit) <= 0:
        return []
    out = []
    for cmd in actions[: max(0, int(limit))]:
        out.append(_run_cmd(cmd))
    return out


def run(generate=False, work_through_all=False, execute_top=0):
    if generate or (not TASKS_PATH.exists()):
        tasks = _generate_tasks()
    else:
        tasks = _load_json(TASKS_PATH, [])
        if not isinstance(tasks, list) or len(tasks) != 1000:
            tasks = _generate_tasks()

    state = _state_snapshot()
    tasks = _reprioritize(tasks, state)

    triaged = 0
    if work_through_all:
        for task in tasks:
            status, blocked = _triage_status(task, state)
            task["status"] = status
            task["worked_through"] = True
            task["blocked_reason"] = blocked
            task["last_reviewed_at"] = _now_iso()
            triaged += 1

    action_results = _execute_actions(execute_top)

    summary = {
        "updated_at": _now_iso(),
        "tasks_total": len(tasks),
        "worked_through_count": sum(1 for t in tasks if bool(t.get("worked_through", False))),
        "status_counts": {
            "completed": sum(1 for t in tasks if t.get("status") == "completed"),
            "in_progress": sum(1 for t in tasks if t.get("status") == "in_progress"),
            "pending": sum(1 for t in tasks if t.get("status") == "pending"),
            "blocked": sum(1 for t in tasks if t.get("status") == "blocked"),
        },
        "priority_counts": {
            "P0": sum(1 for t in tasks if t.get("priority_tier") == "P0"),
            "P1": sum(1 for t in tasks if t.get("priority_tier") == "P1"),
            "P2": sum(1 for t in tasks if t.get("priority_tier") == "P2"),
            "P3": sum(1 for t in tasks if t.get("priority_tier") == "P3"),
            "P4": sum(1 for t in tasks if t.get("priority_tier") == "P4"),
        },
        "triaged_this_run": int(triaged),
        "state_snapshot": {
            "go_live": state["go_live"],
            "realized_ok": state["realized_ok"],
            "hf_live_ready": state["hf_live_ready"],
            "coinbase_ready": state["coinbase_ready"],
            "dns_coinbase_ok": state["dns_coinbase_ok"],
            "creds_coinbase": state["creds_coinbase"],
            "run_rate_usd_per_hour": state["run_rate_usd_per_hour"],
            "top_regions": state["top_regions"],
        },
        "executed_actions": action_results,
        "top_tasks": tasks[:25],
    }

    _save_json(TASKS_PATH, tasks)
    _save_json(STATUS_PATH, summary)

    print(json.dumps(summary, indent=2))
    return summary


def main():
    parser = argparse.ArgumentParser(description="WIN-1000 task backlog runner")
    parser.add_argument("--generate", action="store_true", help="Regenerate task list")
    parser.add_argument("--work-through-all", action="store_true", help="Triage all 1000 tasks")
    parser.add_argument("--execute-top", type=int, default=0, help="Execute top automation actions (0-6)")
    args = parser.parse_args()
    run(
        generate=bool(args.generate),
        work_through_all=bool(args.work_through_all),
        execute_top=max(0, int(args.execute_top)),
    )


if __name__ == "__main__":
    main()
