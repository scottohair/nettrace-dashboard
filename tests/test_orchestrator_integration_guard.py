#!/usr/bin/env python3
"""Tests for orchestrator safe-integration guard wiring."""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

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
