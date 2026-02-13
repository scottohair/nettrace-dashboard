#!/usr/bin/env python3
"""Tests for orchestrator safe-integration guard wiring."""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import orchestrator_v2 as orch  # noqa: E402


def _make_orchestrator(monkeypatch, tmp_path):
    monkeypatch.setattr(orch, "ORCH_DB", str(tmp_path / "orchestrator.db"))
    monkeypatch.setattr(orch, "PENDING_BRIDGES_FILE", str(tmp_path / "pending_bridges.json"))
    monkeypatch.setattr(orch, "INTEGRATION_GUARD_STATUS_FILE", tmp_path / "integration_guard_status.json")
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
