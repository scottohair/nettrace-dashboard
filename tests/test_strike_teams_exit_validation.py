#!/usr/bin/env python3
"""Tests for strict profitable-exit gating in strike teams."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import strike_teams as st  # noqa: E402


def test_validate_profitable_exit_rejects_unprofitable_target():
    team = st.StrikeTeam()
    ok, reason = team.validate_profitable_exit(
        "BTC-USD",
        entry_price=100.0,
        direction="BUY",
        analysis={"take_profit": 101.0},
    )
    assert ok is False
    assert "below_required" in reason


def test_validate_profitable_exit_accepts_profitable_target():
    team = st.StrikeTeam()
    ok, reason = team.validate_profitable_exit(
        "BTC-USD",
        entry_price=100.0,
        direction="BUY",
        analysis={"take_profit": 102.0},
    )
    assert ok is True
    assert "target_ok" in reason


def test_execute_blocks_buy_without_profitable_exit():
    team = st.StrikeTeam()
    result = team.execute(
        "BTC-USD",
        {
            "approved": True,
            "direction": "BUY",
            "size_usd": 5.0,
            "entry_price": 100.0,
            "take_profit": 100.5,
            "confidence": 0.9,
            "confirming_signals": 3,
            "regime": "neutral",
        },
    )
    assert result is None
    assert team.trades_executed == 0


def test_buy_is_throttled_when_sell_close_completion_is_low(monkeypatch):
    monkeypatch.setattr(st, "EXECUTION_HEALTH_GATE", False)
    monkeypatch.setattr(st, "BUY_THROTTLE_ON_SELL_GAP", True)
    monkeypatch.setattr(st, "SELL_CLOSE_MIN_OBS", 4)
    monkeypatch.setattr(st, "SELL_CLOSE_TARGET_RATE", 0.75)

    team = st.StrikeTeam()
    team._record_sell_completion(False)
    team._record_sell_completion(False)
    team._record_sell_completion(True)
    team._record_sell_completion(False)

    result = team.execute(
        "BTC-USD",
        {
            "approved": True,
            "direction": "BUY",
            "size_usd": 5.0,
            "entry_price": 100.0,
            "take_profit": 102.0,
            "confidence": 0.9,
            "confirming_signals": 3,
            "regime": "neutral",
        },
    )
    assert result is None
    assert team.buy_throttled == 1


def test_status_reports_sell_close_completion_metrics():
    team = st.StrikeTeam()
    team._record_sell_completion(True)
    team._record_sell_completion(False)

    status = team.status()
    assert status["sell_attempted"] == 2
    assert status["sell_completed"] == 1
    assert status["sell_failed"] == 1
    assert abs(status["sell_close_completion_rate"] - 0.5) < 1e-9


def test_execute_blocks_buy_when_execution_health_unhealthy(monkeypatch):
    monkeypatch.setattr(st, "EXECUTION_HEALTH_GATE", True)
    monkeypatch.setattr(st, "EXECUTION_HEALTH_BUY_ONLY", True)
    monkeypatch.setattr(st, "_execution_health_status", lambda *_args, **_kwargs: {"green": False, "reason": "dns_unhealthy"})

    team = st.StrikeTeam()
    result = team.execute(
        "BTC-USD",
        {
            "approved": True,
            "direction": "BUY",
            "size_usd": 5.0,
            "entry_price": 100.0,
            "take_profit": 102.0,
            "confidence": 0.9,
            "confirming_signals": 3,
            "regime": "neutral",
        },
    )
    assert result is None
    assert team.exec_health_blocked == 1
