#!/usr/bin/env python3
"""Tests for flywheel Claude Opus/Sonnet collaboration loop."""

import json

import agents.flywheel_controller as fc


class _DummyDuplex:
    def __init__(self):
        self.sent = []
        self.incoming = []

    def send_to_claude(self, message, msg_type="directive", priority="normal", source="operator", meta=None):
        payload = {
            "id": 1000 + len(self.sent) + 1,
            "timestamp": "2026-02-13T00:00:00Z",
            "channel": "to_claude",
            "msg_type": msg_type,
            "priority": priority,
            "source": source,
            "message": message,
            "meta": meta or {},
        }
        self.sent.append(payload)
        return payload

    def read_from_claude(self, since_id=0, limit=200):
        rows = [r for r in self.incoming if int(r.get("id", 0)) > int(since_id or 0)]
        if limit and limit > 0:
            rows = rows[-limit:]
        return rows


def test_flywheel_sends_role_directives_and_ingests_role_updates(monkeypatch, tmp_path):
    dummy = _DummyDuplex()
    dummy.incoming = [
        {"id": 201, "source": "claude_opus", "msg_type": "role_update", "message": "opus", "timestamp": "t1"},
        {"id": 202, "source": "claude_sonnet", "msg_type": "role_update", "message": "sonnet", "timestamp": "t2"},
        {"id": 203, "source": "claude_team", "msg_type": "team_consensus", "message": "team", "timestamp": "t3"},
        {"id": 204, "source": "claude_quant", "msg_type": "cycle_report", "message": "ignored", "timestamp": "t4"},
    ]
    monkeypatch.setattr(fc, "claude_duplex", dummy)
    monkeypatch.setattr(fc, "STATUS_FILE", tmp_path / "flywheel_status.json")

    controller = fc.FlywheelController(enable_claude_updates=True, enable_win_tasks=False)
    controller.cycle = 7
    payload = {
        "growth_decision": {"decision": "NO_GO", "go_live": False, "reasons": ["realized_pnl_below_threshold"]},
        "target_progress": [{"required_hourly_run_rate_usd": 42.5}],
        "quant_blockers": [{"reason": "no_data", "count": 100}],
    }

    result = controller._run_claude_collaboration(payload)
    assert result["enabled"] is True
    assert result["sent"]["sent_count"] == 3
    assert len(dummy.sent) == 3
    target_roles = {str(r.get("meta", {}).get("target_role")) for r in dummy.sent}
    assert target_roles == {"opus", "sonnet", "team"}
    assert result["received"]["received_count_total"] == 4
    assert result["received"]["received_role_count"] == 3
    assert result["received"]["sources"]["claude_opus"] == 1
    assert result["received"]["sources"]["claude_sonnet"] == 1
    assert result["received"]["sources"]["claude_team"] == 1
    assert result["last_from_claude_id"] == 204


def test_flywheel_collab_disabled_without_duplex(monkeypatch, tmp_path):
    monkeypatch.setattr(fc, "claude_duplex", None)
    monkeypatch.setattr(fc, "STATUS_FILE", tmp_path / "flywheel_status.json")

    controller = fc.FlywheelController(enable_claude_updates=True, enable_win_tasks=False)
    result = controller._run_claude_collaboration({})

    assert result["enabled"] is False
    assert result["team_loop_enabled"] is False
    assert result["sent"]["sent_count"] == 0
    assert result["received"]["received_count_total"] == 0


def test_flywheel_blocks_growth_when_close_reconcile_gate_fails(monkeypatch, tmp_path):
    reconcile_status = tmp_path / "reconcile_agent_trades_status.json"
    reconcile_status.write_text(
        json.dumps(
            {
                "updated_at": "2026-02-13T00:00:00+00:00",
                "summary": {
                    "close_attempts": 2,
                    "close_completions": 0,
                    "close_failures": 2,
                    "close_failure_reasons": {"snapshot_missing": 2},
                    "close_gate_passed": False,
                    "close_gate_reason": "sell_close_completion_missing:snapshot_missing",
                },
                "close_reconciliation": {
                    "attempts": 2,
                    "completions": 0,
                    "failures": 2,
                    "failure_reasons": {"snapshot_missing": 2},
                    "gate_passed": False,
                    "gate_reason": "sell_close_completion_missing:snapshot_missing",
                },
            }
        )
    )
    monkeypatch.setattr(fc, "claude_duplex", None)
    monkeypatch.setattr(fc, "STATUS_FILE", tmp_path / "flywheel_status.json")
    monkeypatch.setattr(fc, "CYCLE_LOG", tmp_path / "flywheel_cycles.jsonl")
    monkeypatch.setattr(fc, "RESERVE_STATUS_FILE", tmp_path / "reserve_targets_status.json")
    monkeypatch.setattr(fc, "RECONCILE_STATUS_FILE", reconcile_status)
    monkeypatch.setattr(fc, "RECONCILE_AGENT_TRADES_ENABLED", True)
    monkeypatch.setattr(fc, "CLOSE_FIRST_RECONCILE_GROWTH_GATE_ENABLED", True)

    controller = fc.FlywheelController(enable_claude_updates=False, enable_win_tasks=False)
    invoked_scripts = []

    def _fake_run_py(script_name, *args, **kwargs):
        invoked_scripts.append(script_name)
        return {
            "cmd": [script_name, *list(args)],
            "returncode": 0,
            "elapsed_seconds": 0.001,
            "stdout_tail": "",
            "stderr_tail": "",
            "env_overrides": dict(kwargs.get("env_overrides") or {}),
        }

    monkeypatch.setattr(controller, "_run_py", _fake_run_py)
    monkeypatch.setattr(controller, "_sync_trading_lock", lambda _decision: {"locked": True, "reason": "test", "source": "test"})
    monkeypatch.setattr(
        controller,
        "_get_portfolio_snapshot",
        lambda: {"total_usd": 0.0, "available_cash": 0.0, "held_in_orders": 0.0, "holdings": {}, "source": "test"},
    )
    monkeypatch.setattr(controller, "_reserve_targets_snapshot", lambda _portfolio: {"updated_at": "t", "portfolio_total_usd": 0.0, "targets": []})
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

    assert payload["reconcile_close_gate"]["passed"] is False
    assert payload["growth_decision"]["go_live"] is False
    assert any("sell_close_reconcile_gate_failed:" in r for r in payload["growth_decision"]["reasons"])
    growth_cmd = next(c for c in payload["commands"] if "growth_supervisor.py" in " ".join(c.get("cmd", [])))
    assert growth_cmd["skipped"] is True
    assert "growth_supervisor.py" not in invoked_scripts
    assert "reconcile_agent_trades.py" in invoked_scripts
