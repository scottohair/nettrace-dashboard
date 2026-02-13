#!/usr/bin/env python3
"""Build a deterministic Codex -> Claude collaboration handoff bundle."""

import argparse
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def _load_json(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _pipeline_summary(db_path):
    if not db_path.exists():
        return {
            "db_exists": False,
            "stage_counts": {},
            "failed_cold_reasons_top": [],
            "recent_promotions": 0,
            "recent_failures": 0,
        }

    stage_counts = {}
    reason_counter = Counter()
    recent_promotions = 0
    recent_failures = 0

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        for row in conn.execute(
            "SELECT stage, COUNT(*) AS n FROM strategy_registry GROUP BY stage"
        ):
            stage_counts[str(row["stage"])] = int(row["n"])

        for row in conn.execute(
            """
            SELECT event_type, details_json
            FROM pipeline_events
            ORDER BY id DESC
            LIMIT 500
            """
        ):
            event_type = str(row["event_type"])
            if event_type == "promoted_to_WARM":
                recent_promotions += 1
            if event_type == "failed_COLD":
                recent_failures += 1
                details = {}
                try:
                    details = json.loads(row["details_json"] or "{}")
                except Exception:
                    details = {}
                reasons = details.get("reasons", [])
                if isinstance(reasons, list):
                    for reason in reasons:
                        reason_counter[str(reason)] += 1
    finally:
        conn.close()

    return {
        "db_exists": True,
        "stage_counts": stage_counts,
        "failed_cold_reasons_top": reason_counter.most_common(10),
        "recent_promotions": recent_promotions,
        "recent_failures": recent_failures,
    }


def _build_asks(quant_summary, pipeline):
    asks = []

    promoted = int(quant_summary.get("promoted_warm", 0) or 0)
    rejected = int(quant_summary.get("rejected_cold", 0) or 0)
    no_data = int(quant_summary.get("no_data", 0) or 0)

    if promoted == 0:
        asks.append(
            {
                "objective": "Increase promoted_warm while preserving strict risk gates",
                "focus": [
                    "signal activity calibration",
                    "walk-forward out-of-sample robustness",
                    "trade frequency adequacy",
                ],
                "priority": "high",
            }
        )

    if no_data > 0:
        asks.append(
            {
                "objective": "Eliminate no_data outcomes via market-data coverage improvements",
                "focus": [
                    "pair universe selection",
                    "fallback data quality",
                    "minimum-candle guarantees",
                ],
                "priority": "high",
            }
        )

    if rejected > 0:
        asks.append(
            {
                "objective": "Reduce deterministic rejection causes without relaxing capital protection",
                "focus": [
                    "reason-cluster analysis",
                    "parameter retuning",
                    "risk-aware promotion calibration",
                ],
                "priority": "medium",
            }
        )

    if not asks:
        asks.append(
            {
                "objective": "Improve capital efficiency for currently promoted strategies",
                "focus": ["budget scaling", "portfolio covariance control", "execution quality"],
                "priority": "medium",
            }
        )

    acceptance = [
        "Every proposed change includes deterministic pass/fail thresholds.",
        "No recommendation bypasses walk-forward and risk checks.",
        "Strategies with negative expected value remain blocked.",
        "Output includes code-level changes and validation commands.",
    ]

    pipeline_focus = {
        "stage_counts": pipeline.get("stage_counts", {}),
        "failed_cold_reasons_top": pipeline.get("failed_cold_reasons_top", []),
    }

    return asks, acceptance, pipeline_focus


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".", help="Repository root path")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    agents_dir = repo_root / "agents"

    quant_results = _load_json(agents_dir / "quant_100_results.json", {})
    quant_summary = quant_results.get("summary", {}) if isinstance(quant_results, dict) else {}
    quant_top = quant_summary.get("top_candidates", []) if isinstance(quant_summary, dict) else []
    quant_reasons = quant_summary.get("rejection_reasons_top", []) if isinstance(quant_summary, dict) else []
    improvements = _load_json(agents_dir / "growth_mode_100_improvements.json", {})
    program_status = _load_json(agents_dir / "growth_mode_program_status.json", {})
    batch_config = _load_json(agents_dir / "growth_batch_config.json", {})
    improvement_items = improvements.get("items", []) if isinstance(improvements, dict) else []
    planned_items = [i for i in improvement_items if str(i.get("status", "")).lower() == "planned"]
    running_items = [i for i in improvement_items if str(i.get("status", "")).lower() == "running"]

    pipeline = _pipeline_summary(agents_dir / "pipeline.db")

    asks_for_claude, acceptance_criteria, pipeline_focus = _build_asks(quant_summary, pipeline)

    handoff = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "codex",
        "objective": "Increase robust promoted strategies with strict capital protection",
        "quant100": {
            "summary": quant_summary,
            "top_candidates": quant_top[:10],
            "rejection_reasons_top": quant_reasons[:10],
        },
        "pipeline": pipeline_focus,
        "improvements_program": {
            "total_items": int(improvements.get("total_items", len(improvement_items)) or 0),
            "planned_items": len(planned_items),
            "running_items": len(running_items),
            "sample_next": planned_items[:15],
            "status_snapshot": {
                "counts": program_status.get("counts", {}),
                "active_batches": program_status.get("active_batches", [])[:5],
                "quant_snapshot": program_status.get("quant_snapshot", {}),
            },
            "batch_config": batch_config,
        },
        "asks_for_claude": asks_for_claude,
        "acceptance_criteria": acceptance_criteria,
    }

    staging_dir = agents_dir / "claude_staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    handoff_path = staging_dir / "codex_to_claude_handoff.json"
    handoff_path.write_text(json.dumps(handoff, indent=2))

    duplex_path = staging_dir / "duplex_to_claude.jsonl"
    envelope = {
        "timestamp": handoff["generated_at"],
        "channel": "to_claude",
        "msg_type": "codex_handoff",
        "source": "codex",
        "message": "Deterministic handoff ready",
        "meta": {
            "path": str(handoff_path.relative_to(repo_root)),
            "objective": handoff["objective"],
            "asks": len(asks_for_claude),
        },
    }
    with duplex_path.open("a") as f:
        f.write(json.dumps(envelope) + "\n")

    print(str(handoff_path))


if __name__ == "__main__":
    main()
