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


def test_strict_realized_gate_blocks_go_when_failed(monkeypatch):
    monkeypatch.setattr(gs, "STRICT_REALIZED_GO_LIVE_REQUIRED", True)
    decision = gs._build_decision(
        _base_audit(),
        _base_warm(),
        _base_warm(),
        quant_company_status={"realized_gate_passed": False, "realized_gate_reason": "realized_pnl_below_threshold"},
    )
    assert decision["go_live"] is False
    assert any("strict_realized_gate_failed" in r for r in decision["reasons"])


def test_strict_realized_gate_allows_go_when_passed(monkeypatch):
    monkeypatch.setattr(gs, "STRICT_REALIZED_GO_LIVE_REQUIRED", True)
    decision = gs._build_decision(
        _base_audit(),
        _base_warm(),
        _base_warm(),
        quant_company_status={"realized_gate_passed": True, "realized_gate_reason": "passed"},
    )
    assert decision["go_live"] is True
    assert not any("strict_realized_gate_failed" in r for r in decision["reasons"])


def test_strict_realized_gate_bootstrap_override_allows_go_for_low_budget(monkeypatch):
    monkeypatch.setattr(gs, "STRICT_REALIZED_GO_LIVE_REQUIRED", True)
    monkeypatch.setattr(gs, "STRICT_REALIZED_BOOTSTRAP_ALLOW", True)
    monkeypatch.setattr(gs, "STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_BUDGET", 2.0)
    monkeypatch.setattr(gs, "STRICT_REALIZED_BOOTSTRAP_MAX_FUNDED_STRATEGIES", 4)

    audit = _base_audit()
    audit["metrics"]["pipeline"]["total_funded_budget"] = 1.0
    audit["metrics"]["pipeline"]["funded_strategy_count"] = 2
    decision = gs._build_decision(
        audit,
        _base_warm(),
        _base_warm(),
        quant_company_status={"realized_gate_passed": False, "realized_gate_reason": "insufficient_realized_closes"},
    )

    assert decision["go_live"] is True
    assert not any("strict_realized_gate_failed" in r for r in decision["reasons"])
    assert any("bootstrap_realized_override" in w for w in decision["warnings"])
    gate = decision.get("strict_realized_gate", {})
    assert gate.get("bootstrap_override") is True
