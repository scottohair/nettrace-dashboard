#!/usr/bin/env python3
"""Long-running strike team loop runner.

Keeps all strike teams deployed in daemon threads and writes a compact
status heartbeat so runtime monitors can confirm activity.
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from strike_teams import StrikeTeamManager
try:
    from execution_health_probe import apply_fail_safe_trading_lock, run_probe_cycle
except Exception:
    apply_fail_safe_trading_lock = None
    run_probe_cycle = None


BASE_DIR = Path(__file__).resolve().parent
STATUS_PATH = BASE_DIR / "strike_loop_status.json"
HEARTBEAT_SECONDS = max(5, int(os.environ.get("STRIKE_LOOP_HEARTBEAT_SECONDS", "15") or 15))
SELF_HEAL_ENABLED = os.environ.get("STRIKE_LOOP_EXEC_HEALTH_SELF_HEAL_ENABLED", "1").lower() not in (
    "0",
    "false",
    "no",
)
SELF_HEAL_INTERVAL_SECONDS = max(
    5,
    int(os.environ.get("STRIKE_LOOP_EXEC_HEALTH_SELF_HEAL_INTERVAL_SECONDS", "30") or 30),
)
SELF_HEAL_RECONCILE_INTERVAL_SECONDS = max(
    30,
    int(os.environ.get("STRIKE_LOOP_EXEC_HEALTH_RECONCILE_INTERVAL_SECONDS", "120") or 120),
)
SELF_HEAL_RECONCILE_MAX_ORDERS = max(
    1,
    int(os.environ.get("STRIKE_LOOP_EXEC_HEALTH_RECONCILE_MAX_ORDERS", "120") or 120),
)
SELF_HEAL_RECONCILE_LOOKBACK_HOURS = max(
    1,
    int(os.environ.get("STRIKE_LOOP_EXEC_HEALTH_RECONCILE_LOOKBACK_HOURS", "96") or 96),
)
SELF_HEAL_RECONCILE_TIMEOUT_SECONDS = max(
    10.0,
    float(os.environ.get("STRIKE_LOOP_EXEC_HEALTH_RECONCILE_TIMEOUT_SECONDS", "120") or 120.0),
)
SELF_HEAL_STATUS_MAX_AGE_SECONDS = max(
    30,
    int(os.environ.get("STRIKE_LOOP_EXEC_HEALTH_STATUS_MAX_AGE_SECONDS", "180") or 180),
)
SELF_HEAL_LOCK_SOURCE = str(
    os.environ.get(
        "STRIKE_LOOP_EXEC_HEALTH_LOCK_SOURCE",
        "strike_loop.execution_health_guard",
    )
    or "strike_loop.execution_health_guard"
).strip()


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _load_env():
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))


def main():
    _load_env()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [strike_loop] %(levelname)s %(message)s",
    )
    logger = logging.getLogger("strike_loop")
    manager = StrikeTeamManager()

    stopping = {"flag": False}

    def _stop(_signum=None, _frame=None):
        stopping["flag"] = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    manager.deploy_all()
    logger.info("strike loop started with %d teams", len(manager.teams))
    self_heal_state = {
        "enabled": bool(SELF_HEAL_ENABLED and callable(run_probe_cycle)),
        "checked_at": "",
        "blocked": False,
        "reason": "self_heal_not_checked",
        "lock_action": "none",
        "last_error": "",
        "reconcile_refresh": None,
    }
    next_self_heal_at = 0.0
    next_reconcile_at = 0.0

    try:
        while not stopping["flag"]:
            now_mono = time.monotonic()
            if self_heal_state["enabled"] and now_mono >= next_self_heal_at:
                run_reconcile = now_mono >= next_reconcile_at
                try:
                    payload = run_probe_cycle(
                        refresh=True,
                        probe_http=None,
                        run_reconcile=run_reconcile,
                        reconcile_max_orders=SELF_HEAL_RECONCILE_MAX_ORDERS,
                        reconcile_lookback_hours=SELF_HEAL_RECONCILE_LOOKBACK_HOURS,
                        reconcile_timeout_seconds=SELF_HEAL_RECONCILE_TIMEOUT_SECONDS,
                    )
                    guard = (
                        apply_fail_safe_trading_lock(
                            payload,
                            lock_source=SELF_HEAL_LOCK_SOURCE,
                            require_fresh_within_seconds=SELF_HEAL_STATUS_MAX_AGE_SECONDS,
                            lock_on_not_green=True,
                            clear_on_recovery=True,
                        )
                        if callable(apply_fail_safe_trading_lock)
                        else {
                            "blocked": False,
                            "reason": "guard_unavailable",
                            "lock_action": "unavailable",
                        }
                    )
                    self_heal_state.update(
                        {
                            "checked_at": _utc_now(),
                            "blocked": bool(guard.get("blocked", False)),
                            "reason": str(guard.get("reason", "")),
                            "lock_action": str(guard.get("lock_action", "")),
                            "last_error": "",
                            "reconcile_refresh": payload.get("reconcile_refresh")
                            if isinstance(payload, dict)
                            else None,
                        }
                    )
                    if run_reconcile:
                        next_reconcile_at = now_mono + float(SELF_HEAL_RECONCILE_INTERVAL_SECONDS)
                except Exception as exc:
                    self_heal_state.update(
                        {
                            "checked_at": _utc_now(),
                            "blocked": True,
                            "reason": "self_heal_cycle_exception",
                            "lock_action": "error",
                            "last_error": str(exc),
                            "reconcile_refresh": None,
                        }
                    )
                next_self_heal_at = now_mono + float(SELF_HEAL_INTERVAL_SECONDS)

            status = manager.status()
            payload = {
                "updated_at": _utc_now(),
                "running": True,
                "team_count": len(status),
                "teams": status,
                "execution_health_self_heal": self_heal_state,
            }
            try:
                STATUS_PATH.write_text(json.dumps(payload, indent=2))
            except Exception:
                pass
            time.sleep(HEARTBEAT_SECONDS)
    finally:
        manager.stop_all()
        payload = {
            "updated_at": _utc_now(),
            "running": False,
            "team_count": 0,
            "teams": {},
        }
        try:
            STATUS_PATH.write_text(json.dumps(payload, indent=2))
        except Exception:
            pass
        logger.info("strike loop stopped")


if __name__ == "__main__":
    main()
