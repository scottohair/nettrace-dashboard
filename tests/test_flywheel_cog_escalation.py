#!/usr/bin/env python3
"""Tests for flywheel cog-tier progression and safety de-risking."""

import json
import agents.flywheel_controller as fc


def test_cog_control_escalates_with_consecutive_go_cycles(monkeypatch, tmp_path):
    monkeypatch.setattr(fc, "STATUS_FILE", tmp_path / "flywheel_status.json")
    monkeypatch.setattr(fc, "CYCLE_LOG", tmp_path / "flywheel_cycles.jsonl")
    monkeypatch.setattr(fc, "FLYWHEEL_COG_STATE_FILE", tmp_path / "flywheel_cog_state.json")
    monkeypatch.setattr(fc, "FLYWHEEL_COG_MAX_ACTIVE_PER_CYCLE", 20)

    controller = fc.FlywheelController(enable_claude_updates=False, enable_win_tasks=False)
    controller._update_cog_state_from_cycle({"decision": "GO", "go_live": True}, cycle_had_failures=False)
    controller._update_cog_state_from_cycle({"decision": "GO", "go_live": True}, cycle_had_failures=False)
    controller._update_cog_state_from_cycle({"decision": "GO", "go_live": True}, cycle_had_failures=False)
    controller._update_cog_state_from_cycle({"decision": "GO", "go_live": True}, cycle_had_failures=False)

    cogs = controller._select_flywheel_cogs(quant_run=True, bench_run=True, allow_escalation=True)
    scripts = {c[0] for c in cogs}
    assert "claude_stager_agent.py" in scripts
    assert "autonomous_work_manager.py" in scripts
    assert "bench_fast_exec.py" in scripts
    assert "win_1000_runner.py" in scripts
    assert ("execution_health_probe.py" in scripts), cogs


def test_cog_control_keeps_only_base_cogs_on_no_go(monkeypatch, tmp_path):
    monkeypatch.setattr(fc, "STATUS_FILE", tmp_path / "flywheel_status.json")
    monkeypatch.setattr(fc, "CYCLE_LOG", tmp_path / "flywheel_cycles.jsonl")
    monkeypatch.setattr(fc, "FLYWHEEL_COG_STATE_FILE", tmp_path / "flywheel_cog_state.json")
    monkeypatch.setattr(fc, "FLYWHEEL_COG_FAILURE_DEESCALATE_THRESHOLD", 1)

    controller = fc.FlywheelController(enable_claude_updates=False, enable_win_tasks=False)
    controller.cog_state.update({"active_tier": 4, "go_streak": 9, "failure_streak": 0})
    controller._update_cog_state_from_cycle({"decision": "NO_GO", "go_live": False}, cycle_had_failures=True)

    cogs = controller._select_flywheel_cogs(quant_run=True, bench_run=True, allow_escalation=False)
    scripts = {c[0] for c in cogs}
    assert scripts <= {
        "claude_stager_agent.py",
        "autonomous_work_manager.py",
        "quant_company_agent.py",
        "mcp_opportunity_agent.py",
    }


def test_close_flow_no_go_triggers_warm_pilot_escalation(monkeypatch, tmp_path):
    monkeypatch.setattr(fc, "STATUS_FILE", tmp_path / "flywheel_status.json")
    monkeypatch.setattr(fc, "CYCLE_LOG", tmp_path / "flywheel_cycles.jsonl")
    monkeypatch.setattr(fc, "FLYWHEEL_COG_STATE_FILE", tmp_path / "flywheel_cog_state.json")
    monkeypatch.setattr(fc, "FLYWHEEL_WARM_PILOT_ENABLED", True)
    monkeypatch.setattr(fc, "FLYWHEEL_WARM_PILOT_MAX_ACTIVE_PER_CYCLE", 20)
    monkeypatch.setattr(fc, "FLYWHEEL_COG_MAX_ACTIVE_PER_CYCLE", 20)

    controller = fc.FlywheelController(enable_claude_updates=False, enable_win_tasks=False)
    controller.cog_state.update({"active_tier": 3})

    decision = {
        "go_live": False,
        "decision": "NO_GO",
        "reasons": ["close_flow_gate_failed:close_completion_rate_low:0.33<0.4 attempts=3 completions=1"],
    }
    cogs = controller._select_flywheel_cogs(
        quant_run=True,
        bench_run=True,
        allow_escalation=controller._compute_allow_escalation(decision),
        pilot_mode=controller._decision_supports_pilot(decision),
    )
    scripts = {c[0] for c in cogs}
    assert "execution_health.py" in scripts
    assert "win_1000_runner.py" in scripts


def test_cog_control_limits_active_cogs(monkeypatch, tmp_path):
    monkeypatch.setattr(fc, "STATUS_FILE", tmp_path / "flywheel_status.json")
    monkeypatch.setattr(fc, "CYCLE_LOG", tmp_path / "flywheel_cycles.jsonl")
    monkeypatch.setattr(fc, "FLYWHEEL_COG_STATE_FILE", tmp_path / "flywheel_cog_state.json")
    monkeypatch.setattr(fc, "FLYWHEEL_COG_MAX_ACTIVE_PER_CYCLE", 2)

    controller = fc.FlywheelController(enable_claude_updates=False, enable_win_tasks=False)
    controller.cog_state.update({"active_tier": 4})
    cogs = controller._select_flywheel_cogs(quant_run=True, bench_run=True, allow_escalation=True)
    assert len(cogs) <= 2


def test_run_cycle_includes_cog_control_and_records_transition(monkeypatch, tmp_path):
    monkeypatch.setattr(fc, "STATUS_FILE", tmp_path / "flywheel_status.json")
    monkeypatch.setattr(fc, "CYCLE_LOG", tmp_path / "flywheel_cycles.jsonl")
    monkeypatch.setattr(fc, "RESERVE_STATUS_FILE", tmp_path / "reserve_targets_status.json")
    monkeypatch.setattr(fc, "RECONCILE_STATUS_FILE", tmp_path / "reconcile_agent_trades_status.json")
    monkeypatch.setattr(fc, "FLYWHEEL_COG_STATE_FILE", tmp_path / "flywheel_cog_state.json")
    monkeypatch.setattr(fc, "RECONCILE_AGENT_TRADES_ENABLED", True)
    monkeypatch.setattr(fc, "WIN_TASKS_EXECUTE_TOP", 0)
    monkeypatch.setattr(fc, "WIN_TASKS_ENABLED", True)

    (tmp_path / "reconcile_agent_trades_status.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-02-13T00:00:00+00:00",
                "summary": {"close_gate_passed": True},
            }
        )
    )

    controller = fc.FlywheelController(enable_claude_updates=False, enable_win_tasks=False)

    def _fake_run_py(script_name, *args, **kwargs):
        return {
            "cmd": [script_name, *list(args)],
            "returncode": 0,
            "elapsed_seconds": 0.001,
            "stdout_tail": "",
            "stderr_tail": "",
            "env_overrides": dict(kwargs.get("env_overrides") or {}),
        }

    monkeypatch.setattr(controller, "_run_py", _fake_run_py)
    monkeypatch.setattr(controller, "_sync_trading_lock", lambda _decision: {"locked": True, "source": "test"})
    monkeypatch.setattr(
        controller,
        "_get_portfolio_snapshot",
        lambda: {"total_usd": 0.0, "available_cash": 0.0, "held_in_orders": 0.0, "source": "test"},
    )
    monkeypatch.setattr(
        controller,
        "_reserve_targets_snapshot",
        lambda _portfolio: {"updated_at": "t", "portfolio_total_usd": 0.0, "targets": []},
    )
    monkeypatch.setattr(controller, "_daily_realized_pnl", lambda: 0.0)
    monkeypatch.setattr(controller, "_target_progress", lambda _pnl: [])
    monkeypatch.setattr(controller, "_metal_runtime_snapshot", lambda: {})
    monkeypatch.setattr(controller, "_quant_blockers", lambda: [])
    monkeypatch.setattr(controller, "_read_growth_decision", lambda: {"decision": "GO", "go_live": True, "reasons": []})
    monkeypatch.setattr(
        controller,
        "_run_claude_collaboration",
        lambda _payload: {"enabled": False, "team_loop_enabled": False, "sent": {"sent_count": 0}, "received": {"received_count_total": 0}},
    )

    payload = controller.run_cycle(force_quant=False)
    assert payload["cog_control"]["tier"] >= fc.FLYWHEEL_COG_BASE_TIER
    assert isinstance(payload["cog_control"]["active_transitions"], list)


def test_failed_cogs_enter_pause_window(monkeypatch, tmp_path):
    monkeypatch.setattr(fc, "FLYWHEEL_COG_MIN_SECONDS_BETWEEN_RUNS", 0)
    monkeypatch.setattr(fc, "FLYWHEEL_COG_FAILURE_COOLDOWN_BASE_SECONDS", 30)
    monkeypatch.setattr(fc, "FLYWHEEL_COG_FAILURE_COOLDOWN_MAX_SECONDS", 600)
    monkeypatch.setattr(fc, "FLYWHEEL_COG_FAILURE_STREAK_FOR_EVIDENCE", 1)
    monkeypatch.setattr(fc, "FLYWHEEL_COG_MAX_ACTIVE_PER_CYCLE", 10)
    monkeypatch.setattr(fc, "STATUS_FILE", tmp_path / "flywheel_status.json")
    monkeypatch.setattr(fc, "CYCLE_LOG", tmp_path / "flywheel_cycles.jsonl")
    monkeypatch.setattr(fc, "FLYWHEEL_COG_STATE_FILE", tmp_path / "flywheel_cog_state.json")

    controller = fc.FlywheelController(enable_claude_updates=False, enable_win_tasks=False)
    controller.cog_state.update({"active_tier": 3})
    controller._record_cog_command_result(
        "execution_health.py", (), {"returncode": 1, "timed_out": False}
    )
    assert controller.cog_state["cog_failures"]["execution_health.py"] == 1
    assert controller.cog_state["cog_pause_until"]["execution_health.py"] > 0
    cogs = controller._select_flywheel_cogs(quant_run=True, bench_run=True, allow_escalation=True)
    scripts = {c[0] for c in cogs}
    assert "execution_health.py" not in scripts
