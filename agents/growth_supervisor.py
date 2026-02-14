#!/usr/bin/env python3
"""Run strict growth-mode supervision cycles and emit go/no-go reports."""

import argparse
import json
import os
import sqlite3
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
QUANT_COMPANY_STATUS_PATH = BASE / "quant_company_status.json"
EXECUTION_HEALTH_STATUS_PATH = BASE / "execution_health_status.json"
RECONCILE_STATUS_PATH = BASE / "reconcile_agent_trades_status.json"
GROWTH_MAX_PAIR_SHARE_CAP = float(os.environ.get("GROWTH_MAX_PAIR_SHARE_CAP", "0.70"))
STRICT_REALIZED_GO_LIVE_REQUIRED = os.environ.get(
    "STRICT_REALIZED_GO_LIVE_REQUIRED", "1"
).lower() not in ("0", "false", "no")
STRICT_REALIZED_BOOTSTRAP_ALLOW = os.environ.get(
    "STRICT_REALIZED_BOOTSTRAP_ALLOW", "1"
).lower() not in ("0", "false", "no")
STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_BUDGET = float(
    os.environ.get("STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_BUDGET", "2.0")
)
STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_STRATEGIES = int(
    os.environ.get("STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_STRATEGIES", "8")
)
CLOSE_FLOW_GO_LIVE_REQUIRED = os.environ.get(
    "CLOSE_FLOW_GO_LIVE_REQUIRED", "1"
).lower() not in ("0", "false", "no")
CLOSE_FLOW_LOOKBACK_HOURS = int(os.environ.get("CLOSE_FLOW_LOOKBACK_HOURS", "6"))
CLOSE_FLOW_MAX_BUY_SELL_RATIO = float(
    os.environ.get("CLOSE_FLOW_MAX_BUY_SELL_RATIO", "2.50")
)
CLOSE_FLOW_MIN_SELL_COMPLETIONS = int(
    os.environ.get("CLOSE_FLOW_MIN_SELL_COMPLETIONS", "1")
)
CLOSE_FLOW_MIN_CLOSE_ATTEMPTS = int(
    os.environ.get("CLOSE_FLOW_MIN_CLOSE_ATTEMPTS", "2")
)
CLOSE_FLOW_MIN_CLOSE_COMPLETION_RATE = float(
    os.environ.get("CLOSE_FLOW_MIN_CLOSE_COMPLETION_RATE", "0.40")
)
EXECUTION_HEALTH_GO_LIVE_REQUIRED = os.environ.get(
    "EXECUTION_HEALTH_GO_LIVE_REQUIRED", "1"
).lower() not in ("0", "false", "no")
TRADER_DB_PATH = BASE / "trader.db"
EXIT_MANAGER_DB_PATH = BASE / "exit_manager.db"
COMPLETED_TRADE_STATUSES = (
    "filled",
    "closed",
    "executed",
    "partial_filled",
    "partially_filled",
    "settled",
)

try:
    from execution_health import evaluate_execution_health
except Exception:
    try:
        from agents.execution_health import evaluate_execution_health  # type: ignore
    except Exception:
        evaluate_execution_health = None  # type: ignore


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


def _trade_flow_metrics(lookback_hours=CLOSE_FLOW_LOOKBACK_HOURS):
    lookback = max(1, int(lookback_hours))
    metrics = {
        "lookback_hours": int(lookback),
        "buy_fills": 0,
        "sell_fills": 0,
        "sell_close_events": 0,
        "sell_close_attempts": 0,
        "sell_close_completions": 0,
        "sell_close_completion_rate": 1.0,
        "reconcile_gate_passed": True,
        "reconcile_gate_reason": "not_available",
        "effective_sell_completions": 0,
        "buy_sell_ratio": 0.0,
    }

    if TRADER_DB_PATH.exists():
        conn = None
        try:
            conn = sqlite3.connect(str(TRADER_DB_PATH))
            conn.row_factory = sqlite3.Row
            statuses = ",".join("'" + s + "'" for s in COMPLETED_TRADE_STATUSES)
            rows = conn.execute(
                f"""
                SELECT UPPER(COALESCE(side, '')) AS side, COUNT(*) AS n
                FROM agent_trades
                WHERE created_at >= datetime('now', ?)
                  AND LOWER(COALESCE(status, '')) IN ({statuses})
                GROUP BY UPPER(COALESCE(side, ''))
                """,
                (f"-{lookback} hours",),
            ).fetchall()
            for row in rows:
                side = str(row["side"] or "").upper()
                n = int(row["n"] or 0)
                if side == "BUY":
                    metrics["buy_fills"] = n
                elif side == "SELL":
                    metrics["sell_fills"] = n
        except Exception:
            pass
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    if EXIT_MANAGER_DB_PATH.exists():
        conn = None
        try:
            conn = sqlite3.connect(str(EXIT_MANAGER_DB_PATH))
            row = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM exit_events
                WHERE created_at >= datetime('now', ?)
                """,
                (f"-{lookback} hours",),
            ).fetchone()
            metrics["sell_close_events"] = int((row[0] if row else 0) or 0)
        except Exception:
            pass
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    reconcile = _load_json(RECONCILE_STATUS_PATH, {})
    if isinstance(reconcile, dict) and reconcile:
        close = reconcile.get("close_reconciliation", {})
        summary = reconcile.get("summary", {})
        if not isinstance(close, dict):
            close = {}
        if not isinstance(summary, dict):
            summary = {}
        attempts = int(close.get("attempts", summary.get("close_attempts", 0)) or 0)
        completions = int(close.get("completions", summary.get("close_completions", 0)) or 0)
        if attempts > 0:
            metrics["sell_close_attempts"] = attempts
            metrics["sell_close_completions"] = completions
            metrics["sell_close_completion_rate"] = round(
                float(completions) / float(max(1, attempts)),
                6,
            )
        gate_passed = close.get("gate_passed", summary.get("close_gate_passed"))
        gate_reason = str(close.get("gate_reason", summary.get("close_gate_reason", "")) or "").strip()
        if isinstance(gate_passed, bool):
            metrics["reconcile_gate_passed"] = bool(gate_passed)
        if gate_reason:
            metrics["reconcile_gate_reason"] = gate_reason

    effective = max(
        int(metrics.get("sell_fills", 0) or 0),
        int(metrics.get("sell_close_events", 0) or 0),
    )
    buys = int(metrics.get("buy_fills", 0) or 0)
    ratio = float(buys) / float(max(1, effective))
    metrics["effective_sell_completions"] = int(effective)
    metrics["buy_sell_ratio"] = round(ratio, 4)
    return metrics


def _evaluate_close_flow_gate(trade_flow):
    flow = trade_flow if isinstance(trade_flow, dict) else {}
    gate = {
        "enabled": bool(CLOSE_FLOW_GO_LIVE_REQUIRED),
        "passed": True,
        "reason": "gate_disabled",
        "lookback_hours": int(max(1, int(flow.get("lookback_hours", CLOSE_FLOW_LOOKBACK_HOURS) or CLOSE_FLOW_LOOKBACK_HOURS))),
        "buy_fills": int(flow.get("buy_fills", 0) or 0),
        "sell_fills": int(flow.get("sell_fills", 0) or 0),
        "sell_close_events": int(flow.get("sell_close_events", 0) or 0),
        "effective_sell_completions": int(flow.get("effective_sell_completions", 0) or 0),
        "buy_sell_ratio": float(flow.get("buy_sell_ratio", 0.0) or 0.0),
        "max_buy_sell_ratio": float(CLOSE_FLOW_MAX_BUY_SELL_RATIO),
        "min_sell_completions": int(max(0, int(CLOSE_FLOW_MIN_SELL_COMPLETIONS))),
        "sell_close_attempts": int(flow.get("sell_close_attempts", 0) or 0),
        "sell_close_completions": int(flow.get("sell_close_completions", 0) or 0),
        "sell_close_completion_rate": float(flow.get("sell_close_completion_rate", 1.0) or 0.0),
        "min_close_attempts": int(max(1, int(CLOSE_FLOW_MIN_CLOSE_ATTEMPTS))),
        "min_close_completion_rate": float(max(0.0, min(1.0, float(CLOSE_FLOW_MIN_CLOSE_COMPLETION_RATE)))),
        "reconcile_gate_passed": bool(flow.get("reconcile_gate_passed", True)),
        "reconcile_gate_reason": str(flow.get("reconcile_gate_reason", "")),
    }
    if not CLOSE_FLOW_GO_LIVE_REQUIRED:
        return gate

    buys = int(gate["buy_fills"])
    effective_sells = int(gate["effective_sell_completions"])
    ratio = float(gate["buy_sell_ratio"])
    min_sell_completions = int(gate["min_sell_completions"])
    max_ratio = float(gate["max_buy_sell_ratio"])
    attempts = int(gate["sell_close_attempts"])
    completion_rate = float(gate["sell_close_completion_rate"])
    min_close_attempts = int(gate["min_close_attempts"])
    min_close_rate = float(gate["min_close_completion_rate"])
    reconcile_gate_passed = bool(gate["reconcile_gate_passed"])
    reconcile_gate_reason = str(gate.get("reconcile_gate_reason", "") or "").strip()

    if buys <= 0 and effective_sells <= 0:
        gate["reason"] = "no_recent_completed_trades"
        return gate

    if not reconcile_gate_passed and attempts > 0:
        gate["passed"] = False
        gate["reason"] = f"reconcile_close_gate_failed:{reconcile_gate_reason or 'unknown'}"
        return gate

    if buys > 0 and effective_sells < min_sell_completions:
        gate["passed"] = False
        gate["reason"] = (
            f"insufficient_sell_close_completions:{effective_sells}<{min_sell_completions}"
        )
        return gate

    if attempts >= min_close_attempts and completion_rate < min_close_rate:
        gate["passed"] = False
        gate["reason"] = (
            f"close_completion_rate_low:{completion_rate:.3f}<{min_close_rate:.3f}"
            f" attempts={attempts} completions={int(gate['sell_close_completions'])}"
        )
        return gate

    if buys > 0 and ratio > max_ratio:
        gate["passed"] = False
        gate["reason"] = f"buy_sell_ratio_above_cap:{ratio:.2f}>{max_ratio:.2f}"
        return gate

    gate["reason"] = "balanced_close_flow"
    return gate


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


def _build_decision(
    audit,
    warm_collector,
    warm_promotion,
    quant_company_status=None,
    execution_health=None,
    trade_flow_metrics=None,
):
    checks = audit.get("checks", []) if isinstance(audit, dict) else []
    check_map = {str(c.get("name", "")): c for c in checks}
    summary = audit.get("summary", {}) if isinstance(audit, dict) else {}
    pipe = audit.get("metrics", {}).get("pipeline", {}) if isinstance(audit, dict) else {}

    reasons = []
    warnings = []
    q_status = quant_company_status if isinstance(quant_company_status, dict) else {}
    realized_gate_passed = bool(q_status.get("realized_gate_passed", False))
    realized_gate_reason = str(q_status.get("realized_gate_reason", "unknown"))
    realized_positive_windows = int(q_status.get("realized_positive_windows", 0) or 0)
    realized_required_windows = int(
        q_status.get("realized_required_windows", 3) or 3
    )
    realized_total_closes = int(q_status.get("realized_total_closes", 0) or 0)
    realized_total_net_pnl = float(q_status.get("realized_total_net_pnl_usd", 0.0) or 0.0)
    total_funded_budget = float(pipe.get("total_funded_budget", 0.0) or 0.0)
    funded_strategy_count = int(pipe.get("funded_strategy_count", 0) or 0)
    realized_bootstrap_override = False
    realized_bootstrap_reason = ""
    if STRICT_REALIZED_GO_LIVE_REQUIRED:
        if not q_status:
            reasons.append("strict_realized_gate_status_missing")
        elif not realized_gate_passed:
            bootstrap_reason_codes = {"insufficient_realized_closes", "insufficient_positive_realized_windows"}
            near_window_pass = realized_positive_windows >= max(1, realized_required_windows - 1)
            can_bootstrap = (
                STRICT_REALIZED_BOOTSTRAP_ALLOW
                and realized_gate_reason in bootstrap_reason_codes
                and total_funded_budget <= float(STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_BUDGET)
                and funded_strategy_count <= int(STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_STRATEGIES)
                and realized_total_net_pnl > 0.0
                and realized_total_closes >= 3
                and near_window_pass
            )
            if can_bootstrap:
                realized_bootstrap_override = True
                realized_bootstrap_reason = (
                    f"bootstrap_realized_override:budget={total_funded_budget:.4f}"
                    f"_strategies={funded_strategy_count}"
                    f"_net_pnl={realized_total_net_pnl:.4f}"
                    f"_windows={realized_positive_windows}/{realized_required_windows}"
                )
                warnings.append(realized_bootstrap_reason)
            else:
                reasons.append(f"strict_realized_gate_failed:{realized_gate_reason}")
    exec_health = execution_health if isinstance(execution_health, dict) else {}
    exec_green = bool(exec_health.get("green", False))
    exec_reason = str(exec_health.get("reason", "missing"))
    if EXECUTION_HEALTH_GO_LIVE_REQUIRED:
        if not exec_health:
            reasons.append("execution_health_status_missing")
        elif not exec_green:
            reasons.append(f"execution_health_not_green:{exec_reason}")
    elif exec_health and not exec_green:
        warnings.append(f"execution_health_not_green:{exec_reason}")
    close_flow_gate = _evaluate_close_flow_gate(trade_flow_metrics)
    if close_flow_gate.get("enabled", False) and not close_flow_gate.get("passed", False):
        reasons.append(f"close_flow_gate_failed:{close_flow_gate.get('reason', 'unknown')}")

    if int(summary.get("critical_failures", 0) or 0) > 0:
        reasons.append("critical_audit_failures_present")
    if int(pipe.get("promoted_hot_events", 0) or 0) <= 0:
        reasons.append("no_hot_promotions")
    if int(pipe.get("killed_events", 0) or 0) > 0:
        reasons.append("killed_events_detected")
    enforce_concentration_cap = total_funded_budget >= 5.0 and funded_strategy_count >= 3
    cap = max(0.05, min(0.95, float(GROWTH_MAX_PAIR_SHARE_CAP)))
    if enforce_concentration_cap and float(pipe.get("max_pair_share", 0.0) or 0.0) > cap:
        reasons.append("funding_concentration_above_cap")

    oos_check = check_map.get("Funded OOS Trade Evidence", {})
    oos_check_passed = bool(oos_check.get("passed", False))
    if not oos_check_passed:
        oos_check_severity = str(oos_check.get("severity", "medium")).lower()
        # Bootstrap phase: treat medium OOS evidence gaps as warnings while funded
        # capital is still small. Escalate to hard block once deployment scales up.
        strict_oos_block = (
            oos_check_severity in {"high", "critical"}
            or (total_funded_budget >= 20.0 and funded_strategy_count >= 8)
        )
        if strict_oos_block:
            reasons.append("insufficient_funded_oos_evidence")
        else:
            warnings.append("insufficient_funded_oos_evidence_bootstrap")

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
        "warnings": sorted(set(warnings)),
        "limits": {
            "max_pair_share_cap": round(cap, 4),
            "concentration_cap_enforced": bool(enforce_concentration_cap),
        },
        "strict_realized_gate": {
            "enabled": bool(STRICT_REALIZED_GO_LIVE_REQUIRED),
            "status_available": bool(q_status),
            "passed": bool(realized_gate_passed),
            "reason": realized_gate_reason,
            "bootstrap_override": bool(realized_bootstrap_override),
            "bootstrap_reason": realized_bootstrap_reason,
            "bootstrap_limits": {
                "enabled": bool(STRICT_REALIZED_BOOTSTRAP_ALLOW),
                "max_funded_budget": float(STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_BUDGET),
                "max_funded_strategies": int(STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_STRATEGIES),
            },
        },
        "execution_health_gate": {
            "enabled": bool(EXECUTION_HEALTH_GO_LIVE_REQUIRED),
            "status_available": bool(exec_health),
            "green": bool(exec_green),
            "reason": exec_reason,
            "updated_at": str(exec_health.get("updated_at", "")),
        },
        "close_flow_gate": close_flow_gate,
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
    quant_company_status = _load_json(QUANT_COMPANY_STATUS_PATH, {})
    trade_flow = _trade_flow_metrics()
    execution_health = {}
    if evaluate_execution_health is not None:
        try:
            execution_health = evaluate_execution_health(refresh=True, probe_http=None, write_status=True)
        except Exception:
            execution_health = {}
    if not isinstance(execution_health, dict) or not execution_health:
        execution_health = _load_json(EXECUTION_HEALTH_STATUS_PATH, {})
    cycle["artifacts"] = {
        "audit": audit.get("summary", {}),
        "warm_collector": warm_collector.get("summary", {}),
        "warm_promotion": warm_promotion.get("summary", {}),
        "quant": (quant.get("summary", {}) if isinstance(quant, dict) else {}),
        "quant_company_status": {
            "go_live": bool((quant_company_status or {}).get("go_live", False)),
            "realized_gate_passed": bool((quant_company_status or {}).get("realized_gate_passed", False)),
            "realized_gate_reason": str((quant_company_status or {}).get("realized_gate_reason", "")),
            "updated_at": str((quant_company_status or {}).get("updated_at", "")),
        },
        "execution_health": {
            "green": bool((execution_health or {}).get("green", False)),
            "reason": str((execution_health or {}).get("reason", "")),
            "updated_at": str((execution_health or {}).get("updated_at", "")),
        },
        "trade_flow": trade_flow,
    }
    cycle["decision"] = _build_decision(
        audit,
        warm_collector,
        warm_promotion,
        quant_company_status=quant_company_status,
        execution_health=execution_health,
        trade_flow_metrics=trade_flow,
    )
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
