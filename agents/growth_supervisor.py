#!/usr/bin/env python3
"""Run strict growth-mode supervision cycles and emit go/no-go reports."""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).parent
REGISTRY_PATH = BASE / "growth_mode_100_improvements.json"
LOG_PATH = BASE / "growth_mode_program_log.jsonl"
AUDIT_PATH = BASE / "profit_safety_audit.json"
WARM_COLLECTOR_PATH = BASE / "warm_runtime_collector_report.json"
WARM_PROMOTION_PATH = BASE / "warm_promotion_report.json"
REPORT_PATH = BASE / "growth_go_no_go_report.json"
QUANT_RESULTS_PATH = BASE / "quant_100_results.json"


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _load_json(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _save_json(path, payload):
    path.write_text(json.dumps(payload, indent=2))


def _append_log(event):
    with LOG_PATH.open("a") as f:
        f.write(json.dumps(event) + "\n")


def _run_py(script_name, *args):
    cmd = [sys.executable, str(BASE / script_name), *list(args)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout_tail": "\n".join((proc.stdout or "").strip().splitlines()[-20:]),
        "stderr_tail": "\n".join((proc.stderr or "").strip().splitlines()[-20:]),
    }


def _activate_creative_batch(batch_id, owner="codex"):
    payload = _load_json(REGISTRY_PATH, {})
    items = payload.get("items", []) if isinstance(payload, dict) else []
    touched = []
    for item in items:
        iid = str(item.get("id", ""))
        if not iid.startswith("IMPR-"):
            continue
        try:
            numeric = int(iid.split("-", 1)[1])
        except Exception:
            continue
        if numeric < 101 or numeric > 120:
            continue
        status = str(item.get("status", "planned")).lower()
        if status in {"validated", "blocked"}:
            continue
        item["status"] = "running"
        item["owner"] = owner
        item["batch_id"] = batch_id
        if not item.get("started_at"):
            item["started_at"] = _now_iso()
        item["last_updated"] = _now_iso()
        touched.append(iid)

    payload["updated_at"] = _now_iso()
    _save_json(REGISTRY_PATH, payload)
    _append_log(
        {
            "timestamp": _now_iso(),
            "event": "creative_batch_activated",
            "batch_id": batch_id,
            "owner": owner,
            "size": len(touched),
            "item_ids": touched,
        }
    )
    return {"batch_id": batch_id, "activated_ids": touched, "size": len(touched)}


def _build_decision(audit, warm_collector, warm_promotion):
    checks = audit.get("checks", []) if isinstance(audit, dict) else []
    check_map = {str(c.get("name", "")): c for c in checks}
    summary = audit.get("summary", {}) if isinstance(audit, dict) else {}
    pipe = audit.get("metrics", {}).get("pipeline", {}) if isinstance(audit, dict) else {}

    reasons = []
    if int(summary.get("critical_failures", 0) or 0) > 0:
        reasons.append("critical_audit_failures_present")
    if int(pipe.get("promoted_hot_events", 0) or 0) <= 0:
        reasons.append("no_hot_promotions")
    if int(pipe.get("killed_events", 0) or 0) > 0:
        reasons.append("killed_events_detected")
    total_funded_budget = float(pipe.get("total_funded_budget", 0.0) or 0.0)
    funded_strategy_count = int(pipe.get("funded_strategy_count", 0) or 0)
    enforce_concentration_cap = total_funded_budget >= 5.0 and funded_strategy_count >= 3
    if enforce_concentration_cap and float(pipe.get("max_pair_share", 0.0) or 0.0) > 0.70:
        reasons.append("funding_concentration_above_cap")

    oos_check = check_map.get("Funded OOS Trade Evidence", {})
    if not bool(oos_check.get("passed", False)):
        reasons.append("insufficient_funded_oos_evidence")

    collector_summary = warm_collector.get("summary", {}) if isinstance(warm_collector, dict) else {}
    if int(collector_summary.get("promoted_hot", 0) or 0) <= 0 and int(pipe.get("promoted_hot_events", 0) or 0) <= 0:
        reasons.append("warm_runtime_not_hot_eligible")

    promotion_summary = warm_promotion.get("summary", {}) if isinstance(warm_promotion, dict) else {}
    if int(promotion_summary.get("promoted_hot", 0) or 0) <= 0 and int(pipe.get("promoted_hot_events", 0) or 0) <= 0:
        reasons.append("warm_promotion_runner_no_hot")

    go_live = len(reasons) == 0
    decision = "GO" if go_live else "NO_GO"
    return {
        "decision": decision,
        "go_live": go_live,
        "reasons": sorted(set(reasons)),
        "audit_summary": summary,
        "pipeline_metrics": pipe,
        "warm_collector_summary": collector_summary,
        "warm_promotion_summary": promotion_summary,
    }


def run_cycle(
    activate_creative=False,
    batch_id="BATCH-CREATIVE-20",
    quant_run=True,
    collector_interval_seconds=300,
):
    cycle = {
        "generated_at": _now_iso(),
        "activation": None,
        "commands": [],
        "artifacts": {},
        "decision": {},
    }

    if activate_creative:
        cycle["activation"] = _activate_creative_batch(batch_id=batch_id, owner="codex")

    if quant_run:
        cycle["commands"].append(_run_py("quant_100_runner.py", "run"))

    cycle["commands"].append(
        _run_py(
            "warm_runtime_collector.py",
            "--hours",
            "168",
            "--granularity",
            "5min",
            "--interval-seconds",
            str(int(collector_interval_seconds)),
            "--promote",
        )
    )
    cycle["commands"].append(_run_py("warm_promotion_runner.py", "--hours", "168", "--granularity", "5min", "--promote"))
    cycle["commands"].append(_run_py("rebalance_funded_budgets.py", "--db", str(BASE / "pipeline.db"), "--commit"))
    cycle["commands"].append(
        _run_py(
            "profit_safety_audit.py",
            "--db",
            str(BASE / "pipeline.db"),
            "--quant",
            str(QUANT_RESULTS_PATH),
            "--output",
            str(AUDIT_PATH),
        )
    )

    audit = _load_json(AUDIT_PATH, {})
    warm_collector = _load_json(WARM_COLLECTOR_PATH, {})
    warm_promotion = _load_json(WARM_PROMOTION_PATH, {})
    quant = _load_json(QUANT_RESULTS_PATH, {})
    cycle["artifacts"] = {
        "audit": audit.get("summary", {}),
        "warm_collector": warm_collector.get("summary", {}),
        "warm_promotion": warm_promotion.get("summary", {}),
        "quant": (quant.get("summary", {}) if isinstance(quant, dict) else {}),
    }
    cycle["decision"] = _build_decision(audit, warm_collector, warm_promotion)
    _save_json(REPORT_PATH, cycle)
    print(str(REPORT_PATH))
    print(json.dumps(cycle["decision"], indent=2))
    return cycle


def _loop(args):
    while True:
        run_cycle(
            activate_creative=args.activate_creative,
            batch_id=args.batch_id,
            quant_run=args.quant_run,
            collector_interval_seconds=args.collector_interval_seconds,
        )
        time.sleep(max(15, int(args.sleep_seconds)))


def main():
    parser = argparse.ArgumentParser(description="Run growth supervision and produce go/no-go decisions.")
    parser.add_argument("--activate-creative", action="store_true")
    parser.add_argument("--batch-id", default=f"BATCH-CREATIVE-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}")
    parser.add_argument("--quant-run", action="store_true")
    parser.add_argument("--collector-interval-seconds", type=int, default=300)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--sleep-seconds", type=int, default=300)
    args = parser.parse_args()

    if args.loop:
        _loop(args)
        return
    run_cycle(
        activate_creative=args.activate_creative,
        batch_id=args.batch_id,
        quant_run=args.quant_run,
        collector_interval_seconds=args.collector_interval_seconds,
    )


if __name__ == "__main__":
    main()
