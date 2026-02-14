#!/usr/bin/env python3
"""Tests flywheel integration with autonomous work manager."""

import json

import agents.flywheel_controller as fc


def test_flywheel_runs_autonomous_work_and_includes_status(monkeypatch, tmp_path):
    monkeypatch.setattr(fc, "STATUS_FILE", tmp_path / "flywheel_status.json")
    monkeypatch.setattr(fc, "CYCLE_LOG", tmp_path / "flywheel_cycles.jsonl")
    monkeypatch.setattr(fc, "AUTONOMOUS_WORK_ENABLED", True)
    auto_status = tmp_path / "autonomous_work_status.json"
    auto_status.write_text(
        json.dumps(
            {
                "queue_size": 11,
                "counts": {"queued": 4, "dispatched": 5, "acknowledged": 1, "completed": 1},
                "dispatch": {"dispatched": 2},
                "feedback": {"acked": 1, "completed": 1},
                "updated_at": "2026-02-13T00:00:00+00:00",
            }
        )
    )
    monkeypatch.setattr(fc, "AUTONOMOUS_WORK_STATUS_FILE", auto_status)

    controller = fc.FlywheelController(enable_claude_updates=False, enable_win_tasks=False)
    scripts = []

    def _fake_run_py(script_name, *args, **kwargs):
        scripts.append(script_name)
        return {
            "cmd": [script_name, *list(args)],
            "returncode": 0,
            "elapsed_seconds": 0.001,
            "stdout_tail": "",
            "stderr_tail": "",
            "env_overrides": dict(kwargs.get("env_overrides") or {}),
        }

    monkeypatch.setattr(controller, "_run_py", _fake_run_py)
    monkeypatch.setattr(controller, "_read_growth_decision", lambda: {"decision": "NO_GO", "go_live": False, "reasons": []})
    monkeypatch.setattr(controller, "_sync_trading_lock", lambda _decision: {"locked": True, "source": "test"})
    monkeypatch.setattr(
        controller,
        "_get_portfolio_snapshot",
        lambda: {"total_usd": 100.0, "available_cash": 50.0, "held_in_orders": 0.0, "source": "test"},
    )
    monkeypatch.setattr(controller, "_reserve_targets_snapshot", lambda _portfolio: {"targets": []})
    monkeypatch.setattr(controller, "_daily_realized_pnl", lambda: 0.0)
    monkeypatch.setattr(controller, "_target_progress", lambda _pnl: [])
    monkeypatch.setattr(controller, "_metal_runtime_snapshot", lambda: {})
    monkeypatch.setattr(controller, "_quant_blockers", lambda: [])
    monkeypatch.setattr(
        controller,
        "_run_claude_collaboration",
        lambda _payload: {"enabled": False, "team_loop_enabled": False, "sent": {"sent_count": 0}, "received": {"received_count_total": 0}},
    )

    payload = controller.run_cycle(force_quant=False)
    assert "autonomous_work_manager.py" in scripts
    assert payload["autonomous_work"]["queue_size"] == 11
    assert payload["autonomous_work"]["counts"]["dispatched"] == 5
