#!/usr/bin/env python3
"""Tests for quant_company budget escalator advanced controls."""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import quant_company_agent as qca  # noqa: E402


def _base_progress():
    return {"go_live": True, "alpha_score": 0.82}


def _base_metrics():
    return {"daily_pnl_usd": 1.5, "drawdown_pct": 1.0}


def _base_target():
    return {"achievement_pct_to_next": 0.2}


def _base_realized():
    return {
        "passed": True,
        "reason": "passed",
        "positive_windows": 3,
        "required_positive_windows": 3,
        "total_net_pnl_usd": 1.25,
        "total_closes": 8,
        "windows": [{"net_pnl_usd": 0.42}],
    }


def test_budget_escalator_requires_health_streak(monkeypatch):
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_ESCALATION_GATE", True)
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_GREEN_STREAK_REQUIRED", 3)
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_MIN_GREEN_RATIO", 0.5)
    result = qca._budget_escalator(
        _base_progress(),
        _base_metrics(),
        _base_target(),
        _base_realized(),
        execution_health={"green": True, "reason": "passed", "green_streak": 1, "recent_green_ratio": 1.0},
        trade_flow={"close_balance_ok": True, "buy_sell_ratio": 1.0},
        prev_status={},
    )
    assert result["action"] == "de_escalate"
    assert "execution_health_streak_low" in result["reason"]


def test_budget_escalator_blocks_on_buy_sell_imbalance(monkeypatch):
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_ESCALATION_GATE", True)
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_GREEN_STREAK_REQUIRED", 1)
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_MIN_GREEN_RATIO", 0.5)
    monkeypatch.setattr(qca, "TRADE_FLOW_MAX_BUY_SELL_RATIO", 2.0)
    result = qca._budget_escalator(
        _base_progress(),
        _base_metrics(),
        _base_target(),
        _base_realized(),
        execution_health={"green": True, "reason": "passed", "green_streak": 4, "recent_green_ratio": 0.9},
        trade_flow={"close_balance_ok": False, "buy_sell_ratio": 4.8, "buy_fills": 12, "sell_fills": 1},
        prev_status={},
    )
    assert result["action"] == "de_escalate"
    assert "buy_sell_imbalance" in result["reason"]


def test_budget_escalator_enforces_escalation_cooldown(monkeypatch):
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_ESCALATION_GATE", True)
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_GREEN_STREAK_REQUIRED", 1)
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_MIN_GREEN_RATIO", 0.5)
    monkeypatch.setattr(qca, "BUDGET_ESCALATE_COOLDOWN_SECONDS", 3600)
    prev = {
        "budget_escalator_action": "escalate",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    result = qca._budget_escalator(
        _base_progress(),
        _base_metrics(),
        _base_target(),
        _base_realized(),
        execution_health={"green": True, "reason": "passed", "green_streak": 4, "recent_green_ratio": 0.9},
        trade_flow={"close_balance_ok": True, "buy_sell_ratio": 1.0, "buy_fills": 2, "sell_fills": 2},
        prev_status=prev,
    )
    assert result["action"] == "hold"
    assert "escalation_cooldown_active" in result["reason"]


def test_budget_escalator_blocks_on_close_completion_rate_deficit(monkeypatch):
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_ESCALATION_GATE", True)
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_GREEN_STREAK_REQUIRED", 1)
    monkeypatch.setattr(qca, "EXECUTION_HEALTH_MIN_GREEN_RATIO", 0.5)
    result = qca._budget_escalator(
        _base_progress(),
        _base_metrics(),
        _base_target(),
        _base_realized(),
        execution_health={"green": True, "reason": "passed", "green_streak": 5, "recent_green_ratio": 0.9},
        trade_flow={
            "close_balance_ok": False,
            "close_balance_reason": "close_completion_rate_low:0.250<0.600 attempts=4 completions=1",
            "buy_sell_ratio": 1.0,
            "buy_fills": 2,
            "sell_fills": 2,
        },
        prev_status={},
    )
    assert result["action"] == "de_escalate"
    assert "close_completion_rate_low" in result["reason"]
