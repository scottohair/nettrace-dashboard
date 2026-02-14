#!/usr/bin/env python3
"""Run and persist execution-health checks."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from execution_health import evaluate_execution_health

BASE = Path(__file__).parent
SELF_HEAL_NON_BLOCKING_REASON_PREFIXES = tuple(
    part.strip().lower()
    for part in str(
        os.environ.get(
            "EXEC_HEALTH_SELF_HEAL_NON_BLOCKING_REASONS",
            "candle_feed_",
        )
        or "candle_feed_"
    ).split(",")
    if part.strip()
)


def _tail_text(value, limit=400):
    text = str(value or "").strip()
    if len(text) <= int(limit):
        return text
    return text[-int(limit):]


def _iso_age_seconds(ts_text):
    text = str(ts_text or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())
    except Exception:
        return None


def _reason_matches_prefix(reason, prefixes):
    text = str(reason or "").strip().lower()
    if not text:
        return False
    for prefix in prefixes:
        pref = str(prefix or "").strip().lower()
        if pref and text.startswith(pref):
            return True
    return False


def _is_non_blocking_health_reason(payload):
    prefixes = SELF_HEAL_NON_BLOCKING_REASON_PREFIXES
    if not prefixes:
        return False
    if _reason_matches_prefix(payload.get("reason", ""), prefixes):
        return True
    reasons = payload.get("reasons")
    if isinstance(reasons, list):
        return any(_reason_matches_prefix(item, prefixes) for item in reasons)
    return False


def _run_reconcile_once(max_orders, lookback_hours, timeout_seconds):
    cmd = [
        sys.executable,
        str(BASE / "reconcile_agent_trades.py"),
        "--max-orders",
        str(max(1, int(max_orders))),
        "--lookback-hours",
        str(max(1, int(lookback_hours))),
    ]
    started = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=max(10.0, float(timeout_seconds)),
            check=False,
        )
        return {
            "ok": proc.returncode == 0,
            "returncode": int(proc.returncode),
            "elapsed_seconds": round((time.perf_counter() - started), 3),
            "stdout_tail": _tail_text(proc.stdout, 800),
            "stderr_tail": _tail_text(proc.stderr, 800),
            "cmd": cmd,
        }
    except Exception as exc:
        return {
            "ok": False,
            "returncode": -1,
            "elapsed_seconds": round((time.perf_counter() - started), 3),
            "error": str(exc),
            "cmd": cmd,
        }


def self_heal_block_reason(payload, require_fresh_within_seconds=0.0, lock_on_not_green=True):
    """Return blocking reason for fail-safe lock mode, or empty string when healthy."""
    if not isinstance(payload, dict) or not payload:
        return "execution_health_status_missing"

    freshness_limit = float(require_fresh_within_seconds or 0.0)
    if freshness_limit > 0.0:
        age = _iso_age_seconds(payload.get("updated_at"))
        max_age = max(30.0, freshness_limit)
        if age is None:
            return "execution_health_updated_at_missing"
        if age > max_age:
            return f"execution_health_stale:{int(age)}s>{int(max_age)}s"

    if bool(payload.get("egress_blocked", False)):
        payload_reason = str(payload.get("reason", "") or "")
        return f"egress_blocked:{payload_reason or 'unknown'}"

    if _is_non_blocking_health_reason(payload):
        return ""

    if bool(lock_on_not_green) and not bool(payload.get("green", False)):
        payload_reason = str(payload.get("reason", "") or "")
        return f"execution_health_not_green:{payload_reason or 'unknown'}"

    return ""


def apply_fail_safe_trading_lock(
    payload,
    lock_source="execution_health_probe.self_heal",
    require_fresh_within_seconds=0.0,
    lock_on_not_green=True,
    clear_on_recovery=True,
):
    """Apply fail-safe trading lock based on execution-health payload."""
    reason = self_heal_block_reason(
        payload,
        require_fresh_within_seconds=require_fresh_within_seconds,
        lock_on_not_green=bool(lock_on_not_green),
    )
    report = {
        "blocked": bool(reason),
        "reason": reason or "execution_health_green",
        "lock_source": str(lock_source or "execution_health_probe.self_heal"),
        "lock_action": "none",
    }
    try:
        from trading_guard import clear_trading_lock, read_trading_lock, set_trading_lock
    except Exception:
        report["lock_action"] = "unavailable"
        return report

    if reason:
        try:
            set_trading_lock(
                reason=f"Execution health runtime guard blocked: {reason}",
                source=report["lock_source"],
                metadata={
                    "event": "EXECUTION_HEALTH_SELF_HEAL_BLOCK",
                    "payload_reason": str(payload.get("reason", "") if isinstance(payload, dict) else ""),
                    "payload_green": bool(payload.get("green", False)) if isinstance(payload, dict) else False,
                    "payload_updated_at": str(payload.get("updated_at", "") if isinstance(payload, dict) else ""),
                },
            )
            report["lock_action"] = "set"
        except Exception:
            report["lock_action"] = "set_failed"
        return report

    if not clear_on_recovery:
        return report

    try:
        current = read_trading_lock()
    except Exception:
        current = {}
    if isinstance(current, dict) and current.get("locked") and str(current.get("source", "")) == report["lock_source"]:
        try:
            clear_trading_lock(
                source=report["lock_source"],
                note="execution_health_recovered",
            )
            report["lock_action"] = "cleared"
        except Exception:
            report["lock_action"] = "clear_failed"
    return report


def run_probe_cycle(
    *,
    refresh=True,
    probe_http=None,
    run_reconcile=False,
    reconcile_max_orders=120,
    reconcile_lookback_hours=96,
    reconcile_timeout_seconds=120.0,
):
    """Execute one probe cycle (optional reconcile refresh + execution health refresh)."""
    reconcile_refresh = None
    if run_reconcile:
        reconcile_refresh = _run_reconcile_once(
            max_orders=reconcile_max_orders,
            lookback_hours=reconcile_lookback_hours,
            timeout_seconds=reconcile_timeout_seconds,
        )

    payload = evaluate_execution_health(
        refresh=bool(refresh),
        probe_http=probe_http,
        write_status=True,
    )
    if reconcile_refresh is not None:
        payload = dict(payload)
        payload["reconcile_refresh"] = reconcile_refresh
    return payload


def main():
    parser = argparse.ArgumentParser(description="Probe venue execution health (DNS/API/reconciliation).")
    parser.add_argument("--refresh", action="store_true", help="force fresh probes instead of cache")
    parser.add_argument(
        "--no-http-probe",
        action="store_true",
        help="skip outbound HTTP API probes and rely on DNS/telemetry/reconcile status",
    )
    parser.add_argument(
        "--loop-seconds",
        type=float,
        default=0.0,
        help="Run continuously with this sleep between health checks (0 = once).",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Optional loop cap when --loop-seconds > 0 (0 = unbounded).",
    )
    parser.add_argument(
        "--run-reconcile",
        action="store_true",
        help="Run reconcile_agent_trades before scheduled health checks.",
    )
    parser.add_argument("--reconcile-max-orders", type=int, default=120)
    parser.add_argument("--reconcile-lookback-hours", type=int, default=96)
    parser.add_argument(
        "--reconcile-interval-seconds",
        type=float,
        default=90.0,
        help="Minimum seconds between reconcile refresh cycles while looping.",
    )
    parser.add_argument("--reconcile-timeout-seconds", type=float, default=120.0)
    parser.add_argument(
        "--fail-safe-lock",
        action="store_true",
        help="Apply fail-safe trading lock when runtime execution health is blocked.",
    )
    parser.add_argument(
        "--no-fail-safe-lock",
        action="store_true",
        help="Disable fail-safe trading lock even in loop mode.",
    )
    parser.add_argument(
        "--status-max-age-seconds",
        type=float,
        default=0.0,
        help="When fail-safe lock is enabled, block if status is older than this many seconds (0 disables age gate).",
    )
    parser.add_argument(
        "--lock-on-not-green",
        action="store_true",
        help="When fail-safe lock is enabled, block on any non-green status (default on in loop mode).",
    )
    args = parser.parse_args()

    loop_seconds = max(0.0, float(args.loop_seconds))
    reconcile_every = max(5.0, float(args.reconcile_interval_seconds))
    fail_safe_lock_enabled = bool(args.fail_safe_lock)
    if (not fail_safe_lock_enabled) and loop_seconds > 0.0 and (not bool(args.no_fail_safe_lock)):
        fail_safe_lock_enabled = True
    lock_on_not_green = bool(args.lock_on_not_green or loop_seconds > 0.0)
    status_max_age = max(0.0, float(args.status_max_age_seconds))
    max_cycles = max(0, int(args.max_cycles))
    cycle = 0
    next_reconcile_at = 0.0

    while True:
        now_mono = time.monotonic()
        run_reconcile = bool(args.run_reconcile) and (cycle == 0 or now_mono >= next_reconcile_at)
        if run_reconcile:
            next_reconcile_at = now_mono + reconcile_every

        payload = run_probe_cycle(
            refresh=(True if loop_seconds > 0 else bool(args.refresh)),
            probe_http=(False if args.no_http_probe else None),
            run_reconcile=run_reconcile,
            reconcile_max_orders=args.reconcile_max_orders,
            reconcile_lookback_hours=args.reconcile_lookback_hours,
            reconcile_timeout_seconds=args.reconcile_timeout_seconds,
        )
        if fail_safe_lock_enabled:
            guard = apply_fail_safe_trading_lock(
                payload,
                lock_source="execution_health_probe.self_heal",
                require_fresh_within_seconds=status_max_age,
                lock_on_not_green=lock_on_not_green,
                clear_on_recovery=True,
            )
            payload = dict(payload)
            payload["self_heal_guard"] = guard
        print(json.dumps(payload), flush=True)

        cycle += 1
        if loop_seconds <= 0.0:
            break
        if max_cycles > 0 and cycle >= max_cycles:
            break
        time.sleep(loop_seconds)


if __name__ == "__main__":
    main()
