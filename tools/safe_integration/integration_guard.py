#!/usr/bin/env python3
"""Safety guard for integrating quant/agent upgrades into the platform.

Checks local status artifacts and code-level guardrails before rollout.
Default mode is non-destructive and read-only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


@dataclass
class CheckResult:
    name: str
    passed: bool
    required: bool
    details: str


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _tail_jsonl(path: Path, max_lines: int = 100) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    lines = path.read_text().splitlines()[-max_lines:]
    rows: list[dict[str, Any]] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
        except Exception:
            continue
    return rows


def _is_exit_manager_status_stale_lock(trading_lock: dict[str, Any]) -> bool:
    if not isinstance(trading_lock, dict):
        return False
    reason = str(trading_lock.get("reason", "")).lower()
    return "exit_manager_status_stale" in reason


def _read_trading_lock_with_recovery(agents_dir: Path, raw_state: dict[str, Any]) -> dict[str, Any]:
    """Use trading_guard recovery only for production root reads."""
    try:
        repo_root = agents_dir.parent
        if repo_root != Path(__file__).resolve().parents[2]:
            return raw_state
        sys.path.insert(0, str(agents_dir))
        try:
            import trading_guard  # type: ignore

            return trading_guard.read_trading_lock()
        except Exception:
            return raw_state
        finally:
            try:
                sys.path.remove(str(agents_dir))
            except ValueError:
                pass
    except Exception:
        return raw_state


def _refresh_execution_health(agents_dir: Path) -> dict[str, Any]:
    try:
        sys.path.insert(0, str(agents_dir))
        import execution_health  # type: ignore

        payload = execution_health.evaluate_execution_health(refresh=True, probe_http=None, write_status=True)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def run_guard(repo_root: Path) -> dict[str, Any]:
    agents_dir = repo_root / "agents"
    checks: list[CheckResult] = []
    execution_health_required = (
        str(os.environ.get("INTEGRATION_GUARD_EXECUTION_HEALTH_REQUIRED", "0")).strip().lower()
        in {"1", "true", "yes"}
    )

    execution_health = _refresh_execution_health(agents_dir)
    if not execution_health:
        execution_health = _read_json(agents_dir / "execution_health_status.json")
    execution_green = bool(execution_health.get("green", False))
    checks.append(
        CheckResult(
            name="execution_health_green",
            passed=execution_green,
            required=bool(execution_health_required),
            details=f"green={execution_green} reason={execution_health.get('reason', '')}",
        )
    )

    trading_lock = _read_json(agents_dir / "trading_lock.json")
    trading_lock = _read_trading_lock_with_recovery(agents_dir, trading_lock)
    locked = bool(trading_lock.get("locked", False))
    allow_stale_exit_manager_lock = (
        str(os.environ.get("INTEGRATION_GUARD_ALLOW_STALE_EXIT_MANAGER_LOCK", "0")).strip().lower()
        in {"1", "true", "yes"}
    )
    if locked and allow_stale_exit_manager_lock and _is_exit_manager_status_stale_lock(trading_lock):
        locked = False
    checks.append(
        CheckResult(
            name="trading_lock_allows_rollout",
            passed=not locked,
            required=True,
            details=f"locked={locked} reason={trading_lock.get('reason', '')}",
        )
    )

    quant = _read_json(agents_dir / "quant_100_results.json")
    summary = quant.get("summary", {}) if isinstance(quant.get("summary"), dict) else {}
    promoted = int(summary.get("promoted_warm", 0) or 0)
    rejected = int(summary.get("rejected_cold", 0) or 0)
    no_data = int(summary.get("no_data", 0) or 0)
    checks.append(
        CheckResult(
            name="quant_validation_has_signal",
            passed=(promoted + rejected) > 0 and no_data < 100,
            required=True,
            details=f"promoted={promoted} rejected={rejected} no_data={no_data}",
        )
    )

    bundle = _read_json(agents_dir / "claude_staging" / "claude_ingest_bundle.json")
    metadata = bundle.get("metadata", {}) if isinstance(bundle.get("metadata"), dict) else {}
    bundle_ok = bool(metadata.get("bundle_id")) and bool(metadata.get("bundle_hash"))
    checks.append(
        CheckResult(
            name="claude_bundle_metadata_present",
            passed=bundle_ok,
            required=True,
            details=f"bundle_id={metadata.get('bundle_id', '')} sequence={metadata.get('bundle_sequence', 0)}",
        )
    )

    to_rows: list[dict[str, Any]] = []
    from_rows: list[dict[str, Any]] = []
    try:
        sys.path.insert(0, str(agents_dir))
        import claude_duplex as duplex  # type: ignore

        to_rows = duplex.read_to_claude(limit=80)
        from_rows = duplex.read_from_claude(limit=80)
    except Exception:
        to_rows = _tail_jsonl(agents_dir / "claude_staging" / "duplex_to_claude.jsonl", max_lines=80)
        from_rows = _tail_jsonl(agents_dir / "claude_staging" / "duplex_from_claude.jsonl", max_lines=80)
    checked = 0
    traceable_count = 0
    for row in (to_rows + from_rows)[-40:]:
        checked += 1
        meta = row.get("meta", {}) if isinstance(row.get("meta"), dict) else {}
        trace_id = str(row.get("trace_id") or meta.get("trace_id") or "").strip()
        if trace_id:
            traceable_count += 1
    coverage = (traceable_count / checked) if checked else 0.0
    checks.append(
        CheckResult(
            name="claude_duplex_traceable",
            passed=checked > 0 and coverage >= 0.95,
            required=True,
            details=f"checked_messages={checked} trace_coverage={coverage:.2%}",
        )
    )

    sniper_source = (agents_dir / "sniper.py").read_text() if (agents_dir / "sniper.py").exists() else ""
    sniper_gate_ok = ("has_exit_plan(" in sniper_source) and ("ev_positive" in sniper_source)
    checks.append(
        CheckResult(
            name="sniper_buy_exit_gates_present",
            passed=sniper_gate_ok,
            required=True,
            details="expects has_exit_plan + ev_positive checks in BUY path",
        )
    )

    required_failures = [c for c in checks if c.required and not c.passed]
    ready = len(required_failures) == 0

    next_steps: list[str] = []
    if ready:
        next_steps.append("Run staged rollout in paper mode first (no budget escalation).")
        next_steps.append("Require realized close PnL evidence before any HOT budget increase.")
        next_steps.append("Promote only strategies with trace-linked Claude directive lineage.")
    else:
        for c in required_failures:
            next_steps.append(f"Fix {c.name}: {c.details}")

    return {
        "repo_root": str(repo_root),
        "ready_for_staged_rollout": ready,
        "required_failures": [c.name for c in required_failures],
        "checks": [asdict(c) for c in checks],
        "next_steps": next_steps,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Quant platform safe integration guard")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="Repository root (default: auto-detected from script path)",
    )
    args = parser.parse_args()

    report = run_guard(args.root)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
