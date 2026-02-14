#!/usr/bin/env python3
"""Tests for orchestrator safe-integration guard wiring."""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import orchestrator_v2 as orch  # noqa: E402


def _make_orchestrator(monkeypatch, tmp_path):
    monkeypatch.setattr(orch, "ORCH_DB", str(tmp_path / "orchestrator.db"))
    monkeypatch.setattr(orch, "PENDING_BRIDGES_FILE", str(tmp_path / "pending_bridges.json"))
    monkeypatch.setattr(orch, "INTEGRATION_GUARD_STATUS_FILE", tmp_path / "integration_guard_status.json")
    monkeypatch.setattr(orch, "STARTUP_PREFLIGHT_STATUS_FILE", tmp_path / "orchestrator_startup_preflight.json")
    monkeypatch.setattr(orch.OrchestratorV2, "_get_starting_capital", lambda self: 100.0)
    return orch.OrchestratorV2()


def test_start_agent_blocks_guarded_growth_when_guard_not_ready(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    instance.guard_enabled = True
    instance.guarded_growth_agents = {"flywheel_controller"}
    instance.guard_status = {
        "ready_for_staged_rollout": False,
        "required_failures": ["execution_health_green", "trading_lock_allows_rollout"],
    }

    result = instance.start_agent(
        {"name": "flywheel_controller", "script": "flywheel_controller.py", "args": []}
    )
    assert result is False
    assert "flywheel_controller" not in instance.agents

    row = instance.db.execute(
        "SELECT status, last_error FROM agent_status ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row["status"] == "guard_blocked"
    assert "integration_guard_blocked" in (row["last_error"] or "")


def test_start_agent_allows_non_guarded_agent_when_guard_not_ready(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    instance.guard_enabled = True
    instance.guarded_growth_agents = {"flywheel_controller"}
    instance.guard_status = {
        "ready_for_staged_rollout": False,
        "required_failures": ["execution_health_green"],
    }

    class _DummyProc:
        pid = 4242
        returncode = None

        def poll(self):
            return None

        def terminate(self):
            return None

        def wait(self, timeout=None):  # noqa: ARG002
            return 0

    monkeypatch.setattr(orch.subprocess, "Popen", lambda *args, **kwargs: _DummyProc())
    monkeypatch.setattr(instance, "_discover_script_pids", lambda _script_relpath: [])
    monkeypatch.setattr(instance, "_known_agent_pids", lambda: set())

    result = instance.start_agent({"name": "sniper", "script": "sniper.py", "args": []})
    assert result is True
    assert "sniper" in instance.agents
    assert instance.agents["sniper"]["process"].pid == 4242


def test_run_integration_guard_uses_runner_and_persists_status(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    status_path = Path(orch.INTEGRATION_GUARD_STATUS_FILE)
    assert not status_path.exists()

    instance.guard_enabled = True
    instance.guard_fail_open = False
    instance._guard_runner = lambda _repo_root: {
        "ready_for_staged_rollout": True,
        "required_failures": [],
        "checks": [{"name": "dummy_check", "passed": True}],
    }

    report = instance.run_integration_guard(force=True)
    assert report["ready_for_staged_rollout"] is True
    assert report["required_failures"] == []
    assert "checked_at" in report
    assert status_path.exists()


def test_startup_preflight_passes_when_execution_health_green(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_ENABLED", True)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_FAIL_OPEN", False)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_REQUIRE_GREEN", True)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_MAX_AGE_SECONDS", 300)
    now_iso = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(
        instance,
        "_fetch_execution_health_payload",
        lambda refresh=True: {"updated_at": now_iso, "green": True, "reason": "passed", "reasons": []},
    )

    report = instance.run_startup_preflight(force_refresh=True)
    assert report["passed"] is True
    assert report["reason"] == "execution_health_green"
    assert report["status_available"] is True
    assert (tmp_path / "orchestrator_startup_preflight.json").exists()


def test_startup_preflight_blocks_when_execution_health_not_green(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_ENABLED", True)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_FAIL_OPEN", False)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_REQUIRE_GREEN", True)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_MAX_AGE_SECONDS", 300)
    now_iso = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(
        instance,
        "_fetch_execution_health_payload",
        lambda refresh=True: {"updated_at": now_iso, "green": False, "reason": "egress_blocked", "reasons": ["egress_blocked"]},
    )

    report = instance.run_startup_preflight(force_refresh=True)
    assert report["passed"] is False
    assert "execution_health_not_green:egress_blocked" in report["reason"]


def test_startup_preflight_overrides_with_ignorable_reasons(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_ENABLED", True)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_FAIL_OPEN", False)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_REQUIRE_GREEN", True)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_MAX_AGE_SECONDS", 300)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_IGNORE_REASONS", ("egress_blocked", "api_probe_failed", "telemetry_success_rate_low"))
    now_iso = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(
        instance,
        "_fetch_execution_health_payload",
        lambda refresh=True: {
            "updated_at": now_iso,
            "green": False,
            "reason": "egress_blocked",
            "reasons": [
                "egress_blocked",
                "api_probe_failed",
                "telemetry_success_rate_low:0.10<0.55",
            ],
        },
    )

    report = instance.run_startup_preflight(force_refresh=True)
    assert report["passed"] is True
    assert report["reason"] == "execution_health_warnings_ignored"
    assert "egress_blocked" in report["ignore_reasons"]
    assert "api_probe_failed" in report["ignore_reasons"]


def test_startup_preflight_still_blocks_mixed_with_unignored_reasons(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_ENABLED", True)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_FAIL_OPEN", False)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_REQUIRE_GREEN", True)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_MAX_AGE_SECONDS", 300)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_IGNORE_REASONS", ("egress_blocked", "api_probe_failed"))
    now_iso = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(
        instance,
        "_fetch_execution_health_payload",
        lambda refresh=True: {
            "updated_at": now_iso,
            "green": False,
            "reason": "reconcile_status_stale",
            "reasons": ["egress_blocked", "reconcile_status_stale"],
        },
    )

    report = instance.run_startup_preflight(force_refresh=True)
    assert report["passed"] is False
    assert report["reason"] == "execution_health_not_green:reconcile_status_stale"


def test_startup_preflight_fail_open_overrides_block(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_ENABLED", True)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_FAIL_OPEN", True)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_REQUIRE_GREEN", True)
    monkeypatch.setattr(orch, "ORCH_STARTUP_PREFLIGHT_MAX_AGE_SECONDS", 300)
    now_iso = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(
        instance,
        "_fetch_execution_health_payload",
        lambda refresh=True: {"updated_at": now_iso, "green": False, "reason": "egress_blocked", "reasons": ["egress_blocked"]},
    )

    report = instance.run_startup_preflight(force_refresh=True)
    assert report["passed"] is True
    assert report["reason"] == "startup_preflight_fail_open"
    assert report["pass_override_reason"].startswith("execution_health_not_green:")


def test_execution_health_self_heal_sets_lock_on_egress(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_ENABLED", True)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_INTERVAL_SECONDS", 5)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_RECONCILE_INTERVAL_SECONDS", 120)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_MAX_AGE_SECONDS", 180)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_LOCK_ON_NOT_GREEN", True)

    now_iso = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(
        instance,
        "_refresh_execution_health_payload",
        lambda: {
            "updated_at": now_iso,
            "green": False,
            "reason": "egress_blocked",
            "egress_blocked": True,
        },
    )
    monkeypatch.setattr(instance, "_run_reconcile_refresh_once", lambda: {"ok": True})
    lock_capture = {}

    def _set_lock(reason, payload):
        lock_capture["reason"] = reason
        lock_capture["payload"] = payload
        return True

    monkeypatch.setattr(instance, "_set_execution_health_self_heal_lock", _set_lock)
    monkeypatch.setattr(instance, "_clear_execution_health_self_heal_lock", lambda note="": False)

    report = instance.run_execution_health_self_heal(force=True)
    assert report["blocked"] is True
    assert report["reason"].startswith("egress_blocked:")
    assert report["lock_action"] == "set"
    assert lock_capture["reason"].startswith("egress_blocked:")


def test_execution_health_self_heal_blocks_stale_status(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_ENABLED", True)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_INTERVAL_SECONDS", 5)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_RECONCILE_INTERVAL_SECONDS", 120)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_MAX_AGE_SECONDS", 120)
    stale_iso = (datetime.now(timezone.utc) - timedelta(seconds=900)).isoformat()
    monkeypatch.setattr(
        instance,
        "_refresh_execution_health_payload",
        lambda: {
            "updated_at": stale_iso,
            "green": True,
            "reason": "passed",
            "egress_blocked": False,
        },
    )
    monkeypatch.setattr(instance, "_run_reconcile_refresh_once", lambda: {"ok": True})
    monkeypatch.setattr(instance, "_set_execution_health_self_heal_lock", lambda reason, payload: True)
    monkeypatch.setattr(instance, "_clear_execution_health_self_heal_lock", lambda note="": False)

    report = instance.run_execution_health_self_heal(force=True)
    assert report["blocked"] is True
    assert report["reason"].startswith("execution_health_stale:")
    assert report["lock_action"] == "set"


def test_execution_health_self_heal_ignores_non_blocking_reason(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_ENABLED", True)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_INTERVAL_SECONDS", 5)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_RECONCILE_INTERVAL_SECONDS", 120)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_MAX_AGE_SECONDS", 180)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_LOCK_ON_NOT_GREEN", True)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_NON_BLOCKING_REASONS", ("candle_feed_",))
    now_iso = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(
        instance,
        "_refresh_execution_health_payload",
        lambda: {
            "updated_at": now_iso,
            "green": False,
            "reason": "candle_feed_stale:900s>360s",
            "reasons": ["candle_feed_stale:900s>360s"],
            "egress_blocked": False,
        },
    )
    monkeypatch.setattr(instance, "_run_reconcile_refresh_once", lambda: {"ok": True})
    monkeypatch.setattr(instance, "_set_execution_health_self_heal_lock", lambda reason, payload: True)
    monkeypatch.setattr(instance, "_clear_execution_health_self_heal_lock", lambda note="": False)

    report = instance.run_execution_health_self_heal(force=True)
    assert report["blocked"] is False
    assert report["reason"] == "execution_health_green"
    assert report["lock_action"] == "none"


def test_execution_health_self_heal_reconcile_refresh_is_interval_driven(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_ENABLED", True)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_INTERVAL_SECONDS", 5)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_RECONCILE_INTERVAL_SECONDS", 60)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_MAX_AGE_SECONDS", 180)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_LOCK_ON_NOT_GREEN", True)
    clock = {"now": 1000.0}
    monkeypatch.setattr(orch.time, "monotonic", lambda: clock["now"])
    now_iso = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(
        instance,
        "_refresh_execution_health_payload",
        lambda: {
            "updated_at": now_iso,
            "green": True,
            "reason": "passed",
            "egress_blocked": False,
        },
    )
    reconcile_calls = []
    monkeypatch.setattr(
        instance,
        "_run_reconcile_refresh_once",
        lambda: reconcile_calls.append(clock["now"]) or {"ok": True},
    )
    monkeypatch.setattr(instance, "_set_execution_health_self_heal_lock", lambda reason, payload: False)
    clear_calls = []
    monkeypatch.setattr(
        instance,
        "_clear_execution_health_self_heal_lock",
        lambda note="execution_health_runtime_recovered": clear_calls.append(note) or True,
    )

    instance.execution_health_self_heal = {"enabled": True, "blocked": True, "reason": "egress_blocked:test"}
    first = instance.run_execution_health_self_heal(force=False)
    assert first["blocked"] is False
    assert first["lock_action"] == "cleared"
    assert len(reconcile_calls) == 1

    clock["now"] = 1006.0
    second = instance.run_execution_health_self_heal(force=False)
    assert second["blocked"] is False
    assert len(reconcile_calls) == 1
    assert len(clear_calls) >= 1


def test_execution_health_self_heal_clears_stale_owned_lock_on_healthy_restart(monkeypatch, tmp_path):
    instance = _make_orchestrator(monkeypatch, tmp_path)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_ENABLED", True)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_INTERVAL_SECONDS", 5)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_RECONCILE_INTERVAL_SECONDS", 120)
    monkeypatch.setattr(orch, "ORCH_EXEC_HEALTH_SELF_HEAL_MAX_AGE_SECONDS", 180)
    now_iso = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(
        instance,
        "_refresh_execution_health_payload",
        lambda: {
            "updated_at": now_iso,
            "green": True,
            "reason": "passed",
            "egress_blocked": False,
        },
    )
    monkeypatch.setattr(instance, "_run_reconcile_refresh_once", lambda: {"ok": True})
    monkeypatch.setattr(instance, "_set_execution_health_self_heal_lock", lambda reason, payload: False)
    clear_calls = []
    monkeypatch.setattr(
        instance,
        "_clear_execution_health_self_heal_lock",
        lambda note="execution_health_runtime_recovered": clear_calls.append(note) or True,
    )

    instance.execution_health_self_heal = {"enabled": True, "blocked": False, "reason": "self_heal_not_checked"}
    report = instance.run_execution_health_self_heal(force=True)
    assert report["blocked"] is False
    assert report["lock_action"] == "cleared"
    assert len(clear_calls) == 1
