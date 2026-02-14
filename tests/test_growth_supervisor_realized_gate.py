#!/usr/bin/env python3
"""Tests for strict realized-go-live gating in growth_supervisor."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import growth_supervisor as gs  # noqa: E402


def _base_audit():
    return {
        "summary": {"critical_failures": 0},
        "metrics": {
            "pipeline": {
                "promoted_hot_events": 2,
                "killed_events": 0,
                "total_funded_budget": 10.0,
                "funded_strategy_count": 4,
                "max_pair_share": 0.50,
            }
        },
        "checks": [
            {
                "name": "Funded OOS Trade Evidence",
                "passed": True,
                "severity": "medium",
            }
        ],
    }


def _base_warm():
    return {"summary": {"promoted_hot": 1}}


def _green_execution_health():
    return {
        "green": True,
        "reason": "ok",
        "updated_at": "2026-02-14T00:00:00+00:00",
    }


def test_strict_realized_gate_blocks_go_when_failed(monkeypatch):
    monkeypatch.setattr(gs, "STRICT_REALIZED_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", True)
    decision = gs._build_decision(
        _base_audit(),
        _base_warm(),
        _base_warm(),
        quant_company_status={"realized_gate_passed": False, "realized_gate_reason": "realized_pnl_below_threshold"},
        execution_health=_green_execution_health(),
    )
    assert decision["go_live"] is False
    assert any("strict_realized_gate_failed" in r for r in decision["reasons"])


def test_strict_realized_gate_allows_go_when_passed(monkeypatch):
    monkeypatch.setattr(gs, "STRICT_REALIZED_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", True)
    decision = gs._build_decision(
        _base_audit(),
        _base_warm(),
        _base_warm(),
        quant_company_status={"realized_gate_passed": True, "realized_gate_reason": "passed"},
        execution_health=_green_execution_health(),
    )
    assert decision["go_live"] is True
    assert not any("strict_realized_gate_failed" in r for r in decision["reasons"])


def test_strict_realized_gate_bootstrap_override_allows_go_for_low_budget(monkeypatch):
    monkeypatch.setattr(gs, "STRICT_REALIZED_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "STRICT_REALIZED_BOOTSTRAP_ALLOW", True)
    monkeypatch.setattr(gs, "STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_BUDGET", 2.0)
    monkeypatch.setattr(gs, "STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_STRATEGIES", 8)

    audit = _base_audit()
    audit["metrics"]["pipeline"]["total_funded_budget"] = 1.0
    audit["metrics"]["pipeline"]["funded_strategy_count"] = 2
    decision = gs._build_decision(
        audit,
        _base_warm(),
        _base_warm(),
        quant_company_status={
            "realized_gate_passed": False,
            "realized_gate_reason": "insufficient_realized_closes",
            "realized_positive_windows": 2,
            "realized_required_windows": 3,
            "realized_total_closes": 6,
            "realized_total_net_pnl_usd": 0.42,
        },
        execution_health=_green_execution_health(),
    )

    assert decision["go_live"] is True
    assert not any("strict_realized_gate_failed" in r for r in decision["reasons"])
    assert any("bootstrap_realized_override" in w for w in decision["warnings"])
    gate = decision.get("strict_realized_gate", {})
    assert gate.get("bootstrap_override") is True


def test_strict_realized_gate_bootstrap_allows_near_pass_positive_windows(monkeypatch):
    monkeypatch.setattr(gs, "STRICT_REALIZED_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "STRICT_REALIZED_BOOTSTRAP_ALLOW", True)
    monkeypatch.setattr(gs, "STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_BUDGET", 2.0)
    monkeypatch.setattr(gs, "STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_STRATEGIES", 8)

    audit = _base_audit()
    audit["metrics"]["pipeline"]["total_funded_budget"] = 1.5
    audit["metrics"]["pipeline"]["funded_strategy_count"] = 7
    decision = gs._build_decision(
        audit,
        _base_warm(),
        _base_warm(),
        quant_company_status={
            "realized_gate_passed": False,
            "realized_gate_reason": "insufficient_positive_realized_windows",
            "realized_positive_windows": 2,
            "realized_required_windows": 3,
            "realized_total_closes": 11,
            "realized_total_net_pnl_usd": 3.17,
        },
        execution_health=_green_execution_health(),
    )

    assert decision["go_live"] is True
    assert not any("strict_realized_gate_failed" in r for r in decision["reasons"])
    assert any("bootstrap_realized_override" in w for w in decision["warnings"])


def test_close_flow_gate_blocks_buy_heavy_no_sell_closes(monkeypatch):
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "CLOSE_FLOW_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "CLOSE_FLOW_MIN_SELL_COMPLETIONS", 1)
    monkeypatch.setattr(gs, "CLOSE_FLOW_MAX_BUY_SELL_RATIO", 2.5)

    decision = gs._build_decision(
        _base_audit(),
        _base_warm(),
        _base_warm(),
        quant_company_status={"realized_gate_passed": True, "realized_gate_reason": "passed"},
        execution_health=_green_execution_health(),
        trade_flow_metrics={
            "lookback_hours": 6,
            "buy_fills": 8,
            "sell_fills": 0,
            "sell_close_events": 0,
            "effective_sell_completions": 0,
            "buy_sell_ratio": 8.0,
        },
    )

    assert decision["go_live"] is False
    assert any("close_flow_gate_failed:insufficient_sell_close_completions" in r for r in decision["reasons"])
    gate = decision.get("close_flow_gate", {})
    assert gate.get("passed") is False


def test_close_flow_gate_allows_balanced_flow(monkeypatch):
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "CLOSE_FLOW_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "CLOSE_FLOW_MIN_SELL_COMPLETIONS", 1)
    monkeypatch.setattr(gs, "CLOSE_FLOW_MAX_BUY_SELL_RATIO", 2.5)

    decision = gs._build_decision(
        _base_audit(),
        _base_warm(),
        _base_warm(),
        quant_company_status={"realized_gate_passed": True, "realized_gate_reason": "passed"},
        execution_health=_green_execution_health(),
        trade_flow_metrics={
            "lookback_hours": 6,
            "buy_fills": 4,
            "sell_fills": 2,
            "sell_close_events": 3,
            "effective_sell_completions": 3,
            "buy_sell_ratio": 1.3333,
        },
    )

    assert decision["go_live"] is True
    assert not any("close_flow_gate_failed" in r for r in decision["reasons"])
    gate = decision.get("close_flow_gate", {})
    assert gate.get("passed") is True


def test_close_flow_gate_blocks_low_close_completion_rate(monkeypatch):
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "CLOSE_FLOW_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "CLOSE_FLOW_MIN_SELL_COMPLETIONS", 1)
    monkeypatch.setattr(gs, "CLOSE_FLOW_MAX_BUY_SELL_RATIO", 2.5)
    monkeypatch.setattr(gs, "CLOSE_FLOW_MIN_CLOSE_ATTEMPTS", 2)
    monkeypatch.setattr(gs, "CLOSE_FLOW_MIN_CLOSE_COMPLETION_RATE", 0.60)

    decision = gs._build_decision(
        _base_audit(),
        _base_warm(),
        _base_warm(),
        quant_company_status={"realized_gate_passed": True, "realized_gate_reason": "passed"},
        execution_health=_green_execution_health(),
        trade_flow_metrics={
            "lookback_hours": 6,
            "buy_fills": 2,
            "sell_fills": 2,
            "sell_close_events": 2,
            "effective_sell_completions": 2,
            "buy_sell_ratio": 1.0,
            "sell_close_attempts": 4,
            "sell_close_completions": 1,
            "sell_close_completion_rate": 0.25,
            "reconcile_gate_passed": True,
            "reconcile_gate_reason": "sell_close_completion_missing:not_completed_open",
        },
    )

    assert decision["go_live"] is False
    assert any("close_flow_gate_failed:close_completion_rate_low:" in r for r in decision["reasons"])


def test_execution_health_gate_blocks_go_when_status_missing(monkeypatch):
    monkeypatch.setattr(gs, "STRICT_REALIZED_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", True)

    decision = gs._build_decision(
        _base_audit(),
        _base_warm(),
        _base_warm(),
        quant_company_status={"realized_gate_passed": True, "realized_gate_reason": "passed"},
        execution_health={},
    )

    assert decision["go_live"] is False
    assert "execution_health_status_missing" in decision["reasons"]


def test_execution_health_gate_blocks_go_when_not_green(monkeypatch):
    monkeypatch.setattr(gs, "STRICT_REALIZED_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", True)

    decision = gs._build_decision(
        _base_audit(),
        _base_warm(),
        _base_warm(),
        quant_company_status={"realized_gate_passed": True, "realized_gate_reason": "passed"},
        execution_health={"green": False, "reason": "dns_unhealthy", "updated_at": "2026-02-14T00:00:00+00:00"},
    )

    assert decision["go_live"] is False
    assert "execution_health_not_green:dns_unhealthy" in decision["reasons"]


def test_execution_health_bootstrap_allows_go_when_egress_blocked_in_reasons(monkeypatch):
    monkeypatch.setattr(gs, "STRICT_REALIZED_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "GROWTH_STRICT_HOT_PROMOTION_REQUIRED", False)
    monkeypatch.setattr(gs, "WARM_MICROLANE_ALLOW", True)
    monkeypatch.setattr(gs, "WARM_MICROLANE_MAX_FUNDED_BUDGET", 2.0)
    monkeypatch.setattr(gs, "WARM_MICROLANE_MAX_FUNDED_STRATEGIES", 2)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_WARM_BOOTSTRAP_ENABLED", True)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_WARM_BOOTSTRAP_ALLOW_TELEMETRY", True)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_WARM_BOOTSTRAP_ALLOW_EGRESS", True)

    audit = _base_audit()
    audit["metrics"]["pipeline"]["total_funded_budget"] = 1.0
    audit["metrics"]["pipeline"]["funded_strategy_count"] = 1

    warm_summary = {
        "promoted_hot": 1,
        "promoted_warm": 0,
        "warm_checked": 12,
        "eligible_hot": 4,
        "not_ready": 1,
    }

    decision = gs._build_decision(
        audit,
        {"summary": warm_summary},
        {"summary": warm_summary},
        quant_company_status={"realized_gate_passed": True, "realized_gate_reason": "passed"},
        execution_health={
            "green": False,
            "reasons": ["dns_unhealthy", "egress_blocked", "telemetry_samples_low:0<3"],
            "reason": "dns_unhealthy",
            "updated_at": "2026-02-14T00:00:00+00:00",
        },
    )

    assert decision["go_live"] is True
    assert any("execution_health_bootstrap_override:reason=egress_blocked" in w for w in decision["warnings"])
    assert not any("execution_health_not_green" in r for r in decision["reasons"])


def _no_promoted_warm():
    return {"summary": {"promoted_hot": 0}}


def test_hot_evidence_bootstrap_guard_keeps_no_go_in_strict_mode(monkeypatch):
    monkeypatch.setattr(gs, "GROWTH_STRICT_HOT_PROMOTION_REQUIRED", True)
    monkeypatch.setattr(gs, "WARM_MICROLANE_ALLOW", True)
    monkeypatch.setattr(gs, "WARM_MICROLANE_MAX_FUNDED_BUDGET", 2.0)
    monkeypatch.setattr(gs, "WARM_MICROLANE_MAX_FUNDED_STRATEGIES", 2)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", False)
    monkeypatch.setattr(gs, "CLOSE_FLOW_GO_LIVE_REQUIRED", False)

    audit = _base_audit()
    audit["metrics"]["pipeline"]["promoted_hot_events"] = 0
    audit["metrics"]["pipeline"]["total_funded_budget"] = 1.0
    audit["metrics"]["pipeline"]["funded_strategy_count"] = 1
    decision = gs._build_decision(
        audit,
        _no_promoted_warm(),
        _no_promoted_warm(),
        quant_company_status={"realized_gate_passed": True, "realized_gate_reason": "passed"},
        execution_health={},
    )

    assert decision["go_live"] is False
    assert "no_hot_promotions" in decision["reasons"]
    assert "warm_runtime_not_hot_eligible" in decision["reasons"]
    assert "warm_promotion_runner_no_hot" in decision["reasons"]
    assert decision["hot_evidence_bootstrap"]["active"] is False


def test_hot_evidence_bootstrap_allows_go_when_enabled_for_micro_budget(monkeypatch):
    monkeypatch.setattr(gs, "GROWTH_STRICT_HOT_PROMOTION_REQUIRED", False)
    monkeypatch.setattr(gs, "WARM_MICROLANE_ALLOW", True)
    monkeypatch.setattr(gs, "WARM_MICROLANE_MAX_FUNDED_BUDGET", 2.0)
    monkeypatch.setattr(gs, "WARM_MICROLANE_MAX_FUNDED_STRATEGIES", 2)
    monkeypatch.setattr(gs, "WARM_MICROLANE_REQUIRE_REALIZED_PROOF", True)
    monkeypatch.setattr(gs, "EXECUTION_HEALTH_GO_LIVE_REQUIRED", False)
    monkeypatch.setattr(gs, "CLOSE_FLOW_GO_LIVE_REQUIRED", False)

    audit = _base_audit()
    audit["metrics"]["pipeline"]["promoted_hot_events"] = 0
    audit["metrics"]["pipeline"]["total_funded_budget"] = 1.0
    audit["metrics"]["pipeline"]["funded_strategy_count"] = 1
    decision = gs._build_decision(
        audit,
        _no_promoted_warm(),
        _no_promoted_warm(),
        quant_company_status={"realized_gate_passed": True, "realized_gate_reason": "passed"},
        execution_health={"green": True, "reason": "ok"},
    )

    assert decision["go_live"] is True
    assert "no_hot_promotions" not in decision["reasons"]
    assert "warm_runtime_not_hot_eligible" not in decision["reasons"]
    assert "warm_promotion_runner_no_hot" not in decision["reasons"]
    assert decision["hot_evidence_bootstrap"]["active"] is True
    assert any("hot_evidence_bootstrap" in w for w in decision["warnings"])
