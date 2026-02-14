#!/usr/bin/env python3
"""Run a safety + profitability audit for the quant pipeline."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).parent
QUANT_RESULTS = BASE / "quant_100_results.json"
PIPELINE_DB = BASE / "pipeline.db"
OUT = BASE / "profit_safety_audit.json"


def _load_json(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _safe_json(text):
    try:
        payload = json.loads(text or "{}")
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {}


def _quant_metrics():
    payload = _load_json(QUANT_RESULTS, {})
    summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
    results = payload.get("results", []) if isinstance(payload, dict) else []

    promoted = [r for r in results if r.get("status") == "promoted_warm"]
    promoted_positive_ret = [r for r in promoted if float(r.get("metrics", {}).get("return_pct", 0) or 0) > 0]
    funded = [r for r in promoted if float(r.get("budget", {}).get("starter_budget_usd", 0) or 0) > 0]
    funded_with_oos_trades = [
        r for r in funded
        if int(
            (r.get("walkforward", {}).get("out_of_sample", {}).get("total_trades", 0) or 0)
        ) > 0
    ]

    return {
        "summary": summary,
        "total": int(summary.get("total", 0) or 0),
        "promoted_warm": int(summary.get("promoted_warm", 0) or 0),
        "retained_warm_passed": int(summary.get("retained_warm_passed", 0) or 0),
        "retained_hot_passed": int(summary.get("retained_hot_passed", 0) or 0),
        "implementable_count": int(summary.get("implementable_count", 0) or 0),
        "rejected_cold": int(summary.get("rejected_cold", 0) or 0),
        "no_data": int(summary.get("no_data", 0) or 0),
        "promoted_positive_return": len(promoted_positive_ret),
        "funded_promoted": len(funded),
        "funded_with_oos_trades": len(funded_with_oos_trades),
        "results": results,
    }


def _pipeline_metrics():
    if not PIPELINE_DB.exists():
        return {
            "db_exists": False,
            "stage_counts": {},
            "promoted_hot_events": 0,
            "promoted_hot_events_recent_24h": 0,
            "killed_events": 0,
            "funded_per_pair": {},
            "total_funded_budget": 0.0,
            "max_pair_share": 0.0,
            "funded_strategy_count": 0,
            "funded_hot_strategy_count": 0,
            "funded_warm_strategy_count": 0,
            "hot_stage_count": 0,
            "warm_stage_count": 0,
            "cold_stage_count": 0,
            "funded_with_oos_trades": 0,
        }

    conn = sqlite3.connect(str(PIPELINE_DB))
    conn.row_factory = sqlite3.Row
    try:
        stage_counts = {
            str(r["stage"]): int(r["n"])
            for r in conn.execute("SELECT stage, COUNT(*) AS n FROM strategy_registry GROUP BY stage")
        }
        promoted_hot = int(
            conn.execute("SELECT COUNT(*) FROM pipeline_events WHERE event_type='promoted_to_HOT'").fetchone()[0]
        )
        promoted_hot_recent = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM pipeline_events
                WHERE event_type='promoted_to_HOT'
                  AND created_at >= datetime('now', '-24 hours')
                """
            ).fetchone()[0]
        )
        killed = int(conn.execute("SELECT COUNT(*) FROM pipeline_events WHERE event_type='KILLED'").fetchone()[0])

        rows = conn.execute(
            """
            SELECT r.pair, COUNT(*) AS n, COALESCE(SUM(b.current_budget_usd), 0) AS budget
            FROM strategy_budgets b
            JOIN strategy_registry r
              ON r.name = b.strategy_name AND r.pair = b.pair
            WHERE r.stage IN ('WARM', 'HOT')
              AND COALESCE(b.current_budget_usd, 0) > 0
            GROUP BY r.pair
            """
        ).fetchall()
        funded_rows = conn.execute(
            """
            SELECT r.backtest_results_json
            FROM strategy_budgets b
            JOIN strategy_registry r
              ON r.name = b.strategy_name AND r.pair = b.pair
            WHERE r.stage IN ('WARM', 'HOT')
              AND COALESCE(b.current_budget_usd, 0) > 0
            """
        ).fetchall()
        funded_hot_count = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM strategy_budgets b
                JOIN strategy_registry r
                  ON r.name = b.strategy_name AND r.pair = b.pair
                WHERE r.stage='HOT'
                  AND COALESCE(b.current_budget_usd, 0) > 0
                """
            ).fetchone()[0]
        )
        funded_warm_count = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM strategy_budgets b
                JOIN strategy_registry r
                  ON r.name = b.strategy_name AND r.pair = b.pair
                WHERE r.stage='WARM'
                  AND COALESCE(b.current_budget_usd, 0) > 0
                """
            ).fetchone()[0]
        )

        per_pair = {}
        total_budget = 0.0
        for row in rows:
            pair = str(row["pair"])
            budget = float(row["budget"] or 0.0)
            per_pair[pair] = {
                "count": int(row["n"] or 0),
                "budget_usd": round(budget, 2),
            }
            total_budget += budget

        max_pair_share = 0.0
        if total_budget > 0:
            max_pair_share = max(v["budget_usd"] for v in per_pair.values()) / total_budget
        funded_with_oos_trades = 0
        for row in funded_rows:
            bt = _safe_json(row["backtest_results_json"])
            wf = bt.get("walkforward") if isinstance(bt.get("walkforward"), dict) else {}
            oos = wf.get("out_of_sample") if isinstance(wf.get("out_of_sample"), dict) else {}
            oos_trades = int(oos.get("total_trades", 0) or 0)
            if oos_trades > 0:
                funded_with_oos_trades += 1

        return {
            "db_exists": True,
            "stage_counts": stage_counts,
            "promoted_hot_events": promoted_hot,
            "promoted_hot_events_recent_24h": promoted_hot_recent,
            "killed_events": killed,
            "funded_per_pair": per_pair,
            "total_funded_budget": round(total_budget, 2),
            "max_pair_share": round(max_pair_share, 4),
            "funded_strategy_count": len(funded_rows),
            "funded_hot_strategy_count": funded_hot_count,
            "funded_warm_strategy_count": funded_warm_count,
            "funded_with_oos_trades": funded_with_oos_trades,
            "hot_stage_count": int(stage_counts.get("HOT", 0) or 0),
            "warm_stage_count": int(stage_counts.get("WARM", 0) or 0),
            "cold_stage_count": int(stage_counts.get("COLD", 0) or 0),
        }
    finally:
        conn.close()


def _check(name, passed, severity, detail, remediation):
    return {
        "name": name,
        "passed": bool(passed),
        "severity": severity,
        "detail": detail,
        "remediation": remediation,
    }


def run_audit():
    quant = _quant_metrics()
    pipe = _pipeline_metrics()

    checks = []
    strict_profit_mode = bool((quant.get("summary", {}) if isinstance(quant, dict) else {}).get("strict_profit_only", False))
    bounded_loss_controls = strict_profit_mode and pipe.get("killed_events", 0) == 0
    checks.append(
        _check(
            "Bounded-Loss Controls",
            bounded_loss_controls,
            "high",
            (
                "Absolute no-loss guarantees are impossible in live markets; enforce strict bounded-loss controls. "
                f"strict_profit_only={strict_profit_mode}, KILLED events={pipe.get('killed_events', 0)}"
            ),
            "Keep strict profit-only gating, kill-switches, and funded budget caps active at all times.",
        )
    )

    checks.append(
        _check(
            "Capital Protection (Kill Events)",
            pipe.get("killed_events", 0) == 0,
            "high",
            f"KILLED events: {pipe.get('killed_events', 0)}",
            "If >0, freeze budget escalation and root-cause failed HOT strategies.",
        )
    )

    active_promotions = max(
        int(quant.get("promoted_warm", 0) or 0),
        int(quant.get("retained_hot_passed", 0) or 0),
        int(quant.get("retained_warm_passed", 0) or 0),
        int(pipe.get("promoted_hot_events", 0) or 0),
        int(pipe.get("promoted_hot_events_recent_24h", 0) or 0),
    )
    active_funded = max(
        int(quant.get("funded_promoted", 0) or 0),
        int(pipe.get("funded_strategy_count", 0) or 0),
        int(pipe.get("funded_hot_strategy_count", 0) or 0),
    )
    checks.append(
        _check(
            "Active Profit Pipeline",
            active_promotions > 0 and active_funded > 0,
            "high",
            (
                f"Quant promoted WARM={quant.get('promoted_warm', 0)}, "
                f"Quant retained HOT passed={quant.get('retained_hot_passed', 0)}, "
                f"Quant retained WARM passed={quant.get('retained_warm_passed', 0)}, "
                f"Quant funded promoted={quant.get('funded_promoted', 0)}, "
                f"Pipeline promoted HOT={pipe.get('promoted_hot_events', 0)}, "
                f"Pipeline promoted HOT (24h)={pipe.get('promoted_hot_events_recent_24h', 0)}, "
                f"Pipeline funded strategies={pipe.get('funded_strategy_count', 0)}"
            ),
            "Increase positive OOS candidates and keep only funded strategies with robust gate metrics.",
        )
    )

    live_profit_evidence = bool(
        int(pipe.get("promoted_hot_events_recent_24h", 0) or 0) > 0
        or int(pipe.get("promoted_hot_events", 0) or 0) > 0
        or (
            int(pipe.get("funded_hot_strategy_count", 0) or 0) > 0
            and int(quant.get("retained_hot_passed", 0) or 0) > 0
        )
    )
    checks.append(
        _check(
            "Live Profit Evidence",
            live_profit_evidence,
            "critical",
            (
                f"Promoted HOT events(all-time)={pipe.get('promoted_hot_events', 0)}, "
                f"promoted HOT events(24h)={pipe.get('promoted_hot_events_recent_24h', 0)}, "
                f"funded HOT strategies={pipe.get('funded_hot_strategy_count', 0)}, "
                f"retained HOT passed={quant.get('retained_hot_passed', 0)}"
            ),
            "Run WARM strategies through runtime gate and promote only if paper PnL remains positive over required window.",
        )
    )

    checks.append(
        _check(
            "WARM Promotion Feed",
            int(pipe.get("warm_stage_count", 0) or 0) > 0,
            "high",
            (
                f"WARM stage count={pipe.get('warm_stage_count', 0)}, "
                f"HOT stage count={pipe.get('hot_stage_count', 0)}, "
                f"Quant implementable={quant.get('implementable_count', 0)}"
            ),
            "Keep WARM candidate feed non-empty by refreshing/rotating quant-validated candidates.",
        )
    )

    funded_strategy_count = int(pipe.get("funded_strategy_count", 0) or 0)
    total_funded_budget = float(pipe.get("total_funded_budget", 0.0) or 0.0)
    diversification_bootstrap = funded_strategy_count < 3 or total_funded_budget < 5.0
    checks.append(
        _check(
            "Funded Diversification",
            pipe.get("max_pair_share", 0.0) <= 0.70 or diversification_bootstrap,
            "high",
            (
                f"Max funded pair budget share={pipe.get('max_pair_share', 0.0):.2%}, "
                f"funded_per_pair={pipe.get('funded_per_pair', {})}, "
                f"bootstrap_exception={diversification_bootstrap}"
            ),
            "Enforce per-pair funded cap and total funded budget cap before granting new starter budget.",
        )
    )

    pipeline_funded = int(pipe.get("funded_strategy_count", 0) or 0)
    if pipeline_funded > 0:
        oos_evidence_ratio = int(pipe.get("funded_with_oos_trades", 0) or 0) / pipeline_funded
    else:
        oos_evidence_ratio = 1.0
    checks.append(
        _check(
            "Funded OOS Trade Evidence",
            oos_evidence_ratio >= 0.30,
            "medium",
            (
                f"Funded with OOS trades={pipe.get('funded_with_oos_trades', 0)} / "
                f"{pipe.get('funded_strategy_count', 0)} ({oos_evidence_ratio:.2%})"
            ),
            "Throttle sparse-OOS funding and prefer strategies with observed OOS trade activity.",
        )
    )

    failing = [c for c in checks if not c["passed"]]
    overall_pass = not any(c for c in checks if c["severity"] in {"critical", "high"} and not c["passed"])

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_pass": overall_pass,
        "checks": checks,
        "summary": {
            "total_checks": len(checks),
            "failed_checks": len(failing),
            "critical_failures": len([c for c in failing if c["severity"] == "critical"]),
            "high_failures": len([c for c in failing if c["severity"] == "high"]),
        },
        "metrics": {
            "quant": {
                "promoted_warm": quant.get("promoted_warm", 0),
                "retained_warm_passed": quant.get("retained_warm_passed", 0),
                "retained_hot_passed": quant.get("retained_hot_passed", 0),
                "implementable_count": quant.get("implementable_count", 0),
                "rejected_cold": quant.get("rejected_cold", 0),
                "no_data": quant.get("no_data", 0),
                "funded_promoted": quant.get("funded_promoted", 0),
                "funded_with_oos_trades": quant.get("funded_with_oos_trades", 0),
            },
            "pipeline": {
                "stage_counts": pipe.get("stage_counts", {}),
                "promoted_hot_events": pipe.get("promoted_hot_events", 0),
                "promoted_hot_events_recent_24h": pipe.get("promoted_hot_events_recent_24h", 0),
                "killed_events": pipe.get("killed_events", 0),
                "max_pair_share": pipe.get("max_pair_share", 0.0),
                "total_funded_budget": pipe.get("total_funded_budget", 0.0),
                "funded_strategy_count": pipe.get("funded_strategy_count", 0),
                "funded_hot_strategy_count": pipe.get("funded_hot_strategy_count", 0),
                "funded_warm_strategy_count": pipe.get("funded_warm_strategy_count", 0),
                "hot_stage_count": pipe.get("hot_stage_count", 0),
                "warm_stage_count": pipe.get("warm_stage_count", 0),
                "funded_with_oos_trades": pipe.get("funded_with_oos_trades", 0),
            },
        },
    }

    OUT.write_text(json.dumps(payload, indent=2))
    print(str(OUT))
    print(json.dumps(payload["summary"], indent=2))


if __name__ == "__main__":
    run_audit()
