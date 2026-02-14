#!/usr/bin/env python3
"""Tests for warm evidence adaptation when growth is blocked.

These tests validate that warm evidence subprocesses receive adaptive env overrides
based on the previous NO_GO decision context.
"""

import json
from pathlib import Path
import sys
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "agents"))

import growth_supervisor as gs  # noqa: E402


def _write(path, payload):
    Path(path).write_text(json.dumps(payload))


def _report(decision, generated_at=None):
    if generated_at is None:
        generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return {"generated_at": generated_at, "decision": decision}


def test_warm_adaptation_disabled_without_no_go(tmp_path, monkeypatch):
    monkeypatch.setattr(gs, "REPORT_PATH", Path(tmp_path) / "growth_go_no_go_report.json")
    _write(gs.REPORT_PATH, _report({"go_live": True, "reasons": []}, "2026-02-14T00:00:00+00:00"))
    _write(Path(tmp_path) / "profit_safety_audit.json", {"metrics": {}})
    monkeypatch.setattr(gs, "AUDIT_PATH", Path(tmp_path) / "profit_safety_audit.json")
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_MODE", True)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_DATA_MODE", "candle")
    monkeypatch.setattr(
        gs,
        "evaluate_execution_health",
        lambda refresh=True, probe_http=None, write_status=True: {"green": True, "reason": "ok"},
    )

    invocations = []

    def fake_run_py(script_name, *args, **kwargs):
        invocations.append((script_name, kwargs.get("env_overrides", {})))
        return {
            "cmd": [],
            "returncode": 0,
            "stdout_tail": "",
            "stderr_tail": "",
        }

    monkeypatch.setattr(gs, "WARM_OVERRIDE_PATH", Path(tmp_path) / ".env.warm_override")
    monkeypatch.setattr(gs, "ENV_PATH", Path(tmp_path) / ".env")
    monkeypatch.setattr(gs, "_run_py", fake_run_py)
    gs.run_cycle(
        quant_run=False,
        collector_interval_seconds=10,
        warm_hours=6,
        warm_granularity="5min",
    )

    warm_calls = [call for call in invocations if call[0] == "warm_runtime_collector.py"]
    assert len(warm_calls) == 1
    env_overrides = warm_calls[0][1]
    assert env_overrides.get("WARM_EVIDENCE_DATA_MODE") == "candle"


def test_warm_adaptation_enabled_on_no_hot_no_go(tmp_path, monkeypatch):
    monkeypatch.setattr(gs, "REPORT_PATH", Path(tmp_path) / "growth_go_no_go_report.json")
    _write(
        gs.REPORT_PATH,
        _report({"go_live": False, "reasons": ["no_hot_promotions", "critical_audit_failures_present"]}),
    )
    _write(
        Path(tmp_path) / "profit_safety_audit.json",
        {
            "metrics": {
                "pipeline": {
                    "total_funded_budget": 1.0,
                    "funded_strategy_count": 1,
                }
            }
        },
    )
    monkeypatch.setattr(gs, "AUDIT_PATH", Path(tmp_path) / "profit_safety_audit.json")
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_MODE", True)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_NO_GO_DATA_MODE", "non_candle")
    monkeypatch.setattr(gs, "WARM_EVIDENCE_NO_GO_MIN_BARS", 4)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_NO_GO_MIN_CANDLES", 9)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_BUDGET_CAP", 3.0)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_STRATEGY_CAP", 2)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_MAX_AGE_SECONDS", 60 * 60)
    monkeypatch.setattr(
        gs,
        "evaluate_execution_health",
        lambda refresh=True, probe_http=None, write_status=True: {"green": True, "reason": "ok"},
    )

    invocations = []

    def fake_run_py(script_name, *args, **kwargs):
        invocations.append((script_name, tuple(str(a) for a in args), kwargs.get("env_overrides", {})))
        return {
            "cmd": [],
            "returncode": 0,
            "stdout_tail": "",
            "stderr_tail": "",
        }

    monkeypatch.setattr(gs, "_run_py", fake_run_py)
    gs.run_cycle(
        quant_run=False,
        collector_interval_seconds=10,
        warm_hours=12,
        warm_granularity="5min",
    )

    runtime_calls = [call for call in invocations if call[0] == "warm_runtime_collector.py"]
    assert len(runtime_calls) == 1
    args, overrides = runtime_calls[0][1], runtime_calls[0][2]
    assert overrides.get("WARM_EVIDENCE_DATA_MODE") == "non_candle"
    assert overrides.get("WARM_EVIDENCE_NON_CANDLE_STRICT_MODE") == "0"
    assert overrides.get("WARM_MIN_EVIDENCE_CANDLES") == "9"
    assert overrides.get("REALIZED_CLOSE_REQUIRED_FOR_HOT_PROMOTION") == "0"
    assert overrides.get("EXECUTION_HEALTH_PROMOTION_BOOTSTRAP_ALLOW_EGRESS") == "1"
    assert overrides.get("EXECUTION_HEALTH_PROMOTION_BOOTSTRAP_ALLOW_TELEMETRY") == "1"
    assert "EXECUTION_HEALTH_PROMOTION_BOOTSTRAP_MAX_BUDGET_USD" in overrides
    assert "EXECUTION_HEALTH_PROMOTION_BOOTSTRAP_MAX_STRATEGIES" in overrides
    assert "--hours" in args
    assert "1" in args  # adaptive short horizon for micro budget/no-go


def test_warm_adaptation_enabled_on_execution_health_no_go(tmp_path, monkeypatch):
    monkeypatch.setattr(gs, "REPORT_PATH", Path(tmp_path) / "growth_go_no_go_report.json")
    _write(
        gs.REPORT_PATH,
        _report(
            {
                "go_live": False,
                "reasons": ["execution_health_not_green:egress_blocked", "warm_runtime_not_hot_eligible"],
            }
        ),
    )
    _write(
        Path(tmp_path) / "profit_safety_audit.json",
        {
            "metrics": {
                "pipeline": {
                    "total_funded_budget": 1.0,
                    "funded_strategy_count": 1,
                }
            }
        },
    )
    monkeypatch.setattr(gs, "AUDIT_PATH", Path(tmp_path) / "profit_safety_audit.json")
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_MODE", True)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_NO_GO_DATA_MODE", "non_candle")
    monkeypatch.setattr(gs, "WARM_EVIDENCE_NO_GO_MIN_BARS", 4)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_NO_GO_MIN_CANDLES", 9)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_BUDGET_CAP", 3.0)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_STRATEGY_CAP", 2)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_MAX_AGE_SECONDS", 60 * 60)
    monkeypatch.setattr(
        gs,
        "evaluate_execution_health",
        lambda refresh=True, probe_http=None, write_status=True: {"green": True, "reason": "ok"},
    )

    invocations = []

    def fake_run_py(script_name, *args, **kwargs):
        invocations.append((script_name, kwargs.get("env_overrides", {})))
        return {
            "cmd": [],
            "returncode": 0,
            "stdout_tail": "",
            "stderr_tail": "",
        }

    monkeypatch.setattr(gs, "_run_py", fake_run_py)
    gs.run_cycle(
        quant_run=False,
        collector_interval_seconds=10,
        warm_hours=12,
        warm_granularity="5min",
    )

    runtime_calls = [call for call in invocations if call[0] == "warm_runtime_collector.py"]
    assert len(runtime_calls) == 1
    overrides = runtime_calls[0][1]
    assert overrides.get("REALIZED_CLOSE_REQUIRED_FOR_HOT_PROMOTION") == "0"
    assert overrides.get("EXECUTION_HEALTH_PROMOTION_BOOTSTRAP_ALLOW_EGRESS") == "1"


def test_warm_adaptation_accepts_legacy_report_schema(tmp_path, monkeypatch):
    report_payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "decision": {
            "decision": "NO_GO",
            "go_live": False,
            "reasons": ["no_hot_promotions", "critical_audit_failures_present"],
        },
    }
    Path(tmp_path / "growth_go_no_go_report.json").write_text(json.dumps(report_payload))
    monkeypatch.setattr(
        gs,
        "REPORT_PATH",
        Path(tmp_path) / "growth_go_no_go_report.json",
    )
    monkeypatch.setattr(
        gs,
        "AUDIT_PATH",
        Path(tmp_path) / "profit_safety_audit.json",
    )
    (Path(tmp_path) / "profit_safety_audit.json").write_text(
        json.dumps(
            {
                "metrics": {
                    "pipeline": {
                        "total_funded_budget": 1.0,
                        "funded_strategy_count": 1,
                    }
                }
            }
        )
    )
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_MODE", True)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_NO_GO_DATA_MODE", "non_candle")
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_BUDGET_CAP", 3.0)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_MAX_AGE_SECONDS", 60 * 60)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_STRATEGY_CAP", 2)
    monkeypatch.setattr(
        gs,
        "evaluate_execution_health",
        lambda refresh=True, probe_http=None, write_status=True: {"green": True, "reason": "ok"},
    )

    invocations = []

    def fake_run_py(script_name, *args, **kwargs):
        invocations.append((script_name, tuple(str(a) for a in args)))
        return {
            "cmd": [],
            "returncode": 0,
            "stdout_tail": "",
            "stderr_tail": "",
        }

    monkeypatch.setattr(gs, "_run_py", fake_run_py)
    gs.run_cycle(
        quant_run=False,
        collector_interval_seconds=10,
        warm_hours=12,
        warm_granularity="5min",
    )

    runtime_calls = [call for call in invocations if call[0] == "warm_runtime_collector.py"]
    assert len(runtime_calls) == 1
    args = runtime_calls[0][1]
    assert "--hours" in args
    assert "1" in args


def test_warm_adaptation_skips_when_no_go_is_stale(tmp_path, monkeypatch):
    from datetime import timedelta

    stale_payload = {
        "generated_at": (datetime.now(timezone.utc) - timedelta(seconds=120))
        .replace(microsecond=0)
        .isoformat(),
        "decision": {
            "decision": "NO_GO",
            "go_live": False,
            "reasons": ["no_hot_promotions"],
        },
    }
    report_path = Path(tmp_path) / "growth_go_no_go_report.json"
    report_path.write_text(json.dumps(stale_payload))
    monkeypatch.setattr(gs, "REPORT_PATH", report_path)
    monkeypatch.setattr(gs, "AUDIT_PATH", Path(tmp_path) / "profit_safety_audit.json")
    (Path(tmp_path) / "profit_safety_audit.json").write_text(
        json.dumps({"metrics": {"pipeline": {"total_funded_budget": 1.0, "funded_strategy_count": 1}}})
    )
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_MODE", True)
    monkeypatch.setattr(gs, "WARM_EVIDENCE_AGGRESSIVE_MAX_AGE_SECONDS", 1)
    monkeypatch.setattr(
        gs,
        "evaluate_execution_health",
        lambda refresh=True, probe_http=None, write_status=True: {"green": True, "reason": "ok"},
    )

    invocations = []

    def fake_run_py(script_name, *args, **kwargs):
        invocations.append((script_name, tuple(str(a) for a in args), kwargs.get("env_overrides", {})))
        return {
            "cmd": [],
            "returncode": 0,
            "stdout_tail": "",
            "stderr_tail": "",
        }

    monkeypatch.setattr(gs, "_run_py", fake_run_py)
    cycle = gs.run_cycle(
        quant_run=False,
        collector_interval_seconds=10,
        warm_hours=12,
        warm_granularity="5min",
    )

    runtime_calls = [call for call in invocations if call[0] == "warm_runtime_collector.py"]
    assert len(runtime_calls) == 1
    args = runtime_calls[0][1]
    assert "--hours" in args
    assert args[args.index("--hours") + 1] == "12"
    assert int(cycle["artifacts"]["warm_adaptation"]["applied_hours"]) == 12
    assert cycle["artifacts"]["warm_adaptation"]["active"] is False
