#!/usr/bin/env python3
"""Tests for Sniper end-to-end quant exit-plan gating."""

import os
import sys
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import sniper as sn  # noqa: E402


class _ExitMgr:
    def __init__(self, ready):
        self.ready = bool(ready)

    def has_exit_plan(self, _pair):
        return self.ready


def _buy_signal(pair="BTC-USD", ev_positive=True):
    return {
        "pair": pair,
        "direction": "BUY",
        "composite_confidence": 0.91,
        "confirming_signals": 4,
        "quant_signals": 3,
        "qual_signals": 1,
        "expected_value": 0.01 if ev_positive else -0.01,
        "ev_positive": ev_positive,
        "regime": "neutral",
        "details": {},
    }


def test_scan_all_blocks_buy_when_exit_manager_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    monkeypatch.setitem(sn.CONFIG, "pairs", ["BTC-USD"])
    monkeypatch.setattr(sn.Sniper, "scan_pair", lambda self, _pair: _buy_signal())
    monkeypatch.setattr(sn, "_exit_mgr", None)

    sniper = sn.Sniper()
    actionable = sniper.scan_all()
    assert actionable == []


def test_scan_all_blocks_buy_when_exit_plan_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    monkeypatch.setitem(sn.CONFIG, "pairs", ["BTC-USD"])
    monkeypatch.setattr(sn.Sniper, "scan_pair", lambda self, _pair: _buy_signal())
    monkeypatch.setattr(sn, "_exit_mgr", _ExitMgr(ready=False))

    sniper = sn.Sniper()
    actionable = sniper.scan_all()
    assert actionable == []


def test_execute_trade_blocks_buy_before_price_fetch_when_ev_negative(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    sniper = sn.Sniper()
    monkeypatch.setattr(sniper, "_execution_health_allows_buy", lambda: (True, "ok"))
    monkeypatch.setattr(sniper, "_exit_manager_status_allows_buy", lambda: (True, "ok"))

    called = {"price": 0}

    def _fake_price(_pair):
        called["price"] += 1
        return 100.0

    monkeypatch.setattr(sniper, "_get_price", _fake_price)
    result = sniper.execute_trade(_buy_signal(ev_positive=False))
    assert result is False
    assert called["price"] == 0


def test_execute_trade_blocks_buy_before_price_fetch_when_exit_plan_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    monkeypatch.setattr(sn, "_exit_mgr", _ExitMgr(ready=False))
    sniper = sn.Sniper()
    monkeypatch.setattr(sniper, "_execution_health_allows_buy", lambda: (True, "ok"))
    monkeypatch.setattr(sniper, "_exit_manager_status_allows_buy", lambda: (True, "ok"))

    called = {"price": 0}

    def _fake_price(_pair):
        called["price"] += 1
        return 100.0

    monkeypatch.setattr(sniper, "_get_price", _fake_price)
    result = sniper.execute_trade(_buy_signal(ev_positive=True))
    assert result is False
    assert called["price"] == 0


def test_execute_trade_blocks_buy_before_price_fetch_when_execution_health_bad(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    monkeypatch.setattr(sn, "_exit_mgr", _ExitMgr(ready=True))
    sniper = sn.Sniper()
    monkeypatch.setattr(sniper, "_execution_health_allows_buy", lambda: (False, "dns_unhealthy"))

    called = {"price": 0}

    def _fake_price(_pair):
        called["price"] += 1
        return 100.0

    monkeypatch.setattr(sniper, "_get_price", _fake_price)
    result = sniper.execute_trade(_buy_signal(ev_positive=True))
    assert result is False
    assert called["price"] == 0


def test_execution_health_allows_buy_with_degraded_telemetry(monkeypatch, tmp_path):
    path = tmp_path / "execution_health_status.json"
    monkeypatch.setattr(sn, "EXECUTION_HEALTH_STATUS_PATH", path)
    path.write_text(
        json.dumps(
            {
                "green": False,
                "reason": "telemetry_success_rate_low:0.1200<0.5500",
                "reasons": ["telemetry_success_rate_low:0.1200<0.5500"],
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "egress_blocked": False,
                "components": {"telemetry": {"green": False}},
            }
        )
    )

    sniper = sn.Sniper()
    ok, reason = sniper._execution_health_allows_buy()
    assert ok is True
    assert str(reason).startswith("execution_health_degraded:")


def test_execution_health_blocks_buy_with_dns_failure(monkeypatch, tmp_path):
    path = tmp_path / "execution_health_status.json"
    monkeypatch.setattr(sn, "EXECUTION_HEALTH_STATUS_PATH", path)
    path.write_text(
        json.dumps(
            {
                "green": False,
                "reason": "dns_unhealthy",
                "reasons": ["dns_unhealthy"],
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "egress_blocked": False,
            }
        )
    )

    sniper = sn.Sniper()
    ok, reason = sniper._execution_health_allows_buy()
    assert ok is False
    assert "execution_health_not_green" in str(reason)


def test_execute_trade_blocks_buy_when_exit_manager_status_bad(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    monkeypatch.setattr(sn, "_exit_mgr", _ExitMgr(ready=True))
    sniper = sn.Sniper()
    monkeypatch.setattr(sniper, "_execution_health_allows_buy", lambda: (True, "ok"))
    monkeypatch.setattr(sniper, "_exit_manager_status_allows_buy", lambda: (False, "stale"))

    called = {"price": 0}

    def _fake_price(_pair):
        called["price"] += 1
        return 100.0

    monkeypatch.setattr(sniper, "_get_price", _fake_price)
    result = sniper.execute_trade(_buy_signal(ev_positive=True))
    assert result is False
    assert called["price"] == 0


def test_execute_trade_blocks_buy_when_pair_cooldown_active(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    monkeypatch.setattr(sn, "_exit_mgr", _ExitMgr(ready=True))
    sniper = sn.Sniper()
    monkeypatch.setattr(sniper, "_execution_health_allows_buy", lambda: (True, "ok"))
    monkeypatch.setattr(sniper, "_exit_manager_status_allows_buy", lambda: (True, "ok"))
    monkeypatch.setattr(sniper, "_pair_buy_cooldown_active", lambda _pair: (True, 42))

    called = {"price": 0}

    def _fake_price(_pair):
        called["price"] += 1
        return 100.0

    monkeypatch.setattr(sniper, "_get_price", _fake_price)
    result = sniper.execute_trade(_buy_signal(ev_positive=True))
    assert result is False
    assert called["price"] == 0


def test_execute_trade_blocks_buy_when_close_flow_unhealthy(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    monkeypatch.setattr(sn, "_exit_mgr", _ExitMgr(ready=True))
    status_path = tmp_path / "reconcile_agent_trades_status.json"
    status_path.write_text(
        json.dumps(
            {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "summary": {
                    "close_attempts": 5,
                    "close_completions": 1,
                    "close_gate_passed": False,
                    "close_gate_reason": "sell_close_completion_missing",
                },
                "close_reconciliation": {
                    "attempts": 5,
                    "completions": 1,
                    "completion_rate": 0.2,
                    "gate_passed": False,
                    "gate_reason": "sell_close_completion_missing",
                },
            }
        )
    )

    sniper = sn.Sniper()
    monkeypatch.setitem(sn.CONFIG, "require_close_flow_for_buy", True)
    monkeypatch.setitem(sn.CONFIG, "close_flow_status_max_age_seconds", 3600)
    monkeypatch.setattr(sn, "RECONCILE_STATUS_PATH", status_path)
    monkeypatch.setattr(sniper, "_execution_health_allows_buy", lambda: (True, "ok"))
    monkeypatch.setattr(sniper, "_exit_manager_status_allows_buy", lambda: (True, "ok"))

    called = {"price": 0}

    def _fake_price(_pair):
        called["price"] += 1
        return 100.0

    monkeypatch.setattr(sniper, "_get_price", _fake_price)
    result = sniper.execute_trade(_buy_signal(ev_positive=True))
    assert result is False
    assert called["price"] == 0


def test_balanced_growth_state_blocks_buy_when_buy_sell_ratio_too_high(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    sniper = sn.Sniper()
    monkeypatch.setitem(sn.CONFIG, "balance_growth_mode", True)
    monkeypatch.setitem(sn.CONFIG, "balance_cache_seconds", 5)
    monkeypatch.setitem(sn.CONFIG, "balance_max_buy_sell_ratio", 1.20)
    monkeypatch.setattr(
        sniper,
        "_trade_flow_metrics_for_balance",
        lambda lookback_hours=24: {
            "lookback_hours": int(lookback_hours),
            "buy_fills": 9,
            "sell_fills": 2,
            "sell_close_attempts": 6,
            "sell_close_completions": 2,
            "sell_close_completion_rate": 0.8,
            "effective_sell_completions": 2,
            "buy_sell_ratio": 4.5,
            "realized_sell_closes": 4,
            "realized_sell_net_pnl_usd": 1.1,
            "reconcile_gate_passed": True,
            "reconcile_gate_reason": "ok",
        },
    )
    state = sniper._balanced_growth_state(force_refresh=True)
    assert state["allow_buy"] is False
    assert state["mode"] == "sell_recovery"
    assert "balance_buy_sell_ratio_high" in str(state["reason"])


def test_execute_trade_blocks_buy_when_balanced_growth_gate_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    monkeypatch.setattr(sn, "_exit_mgr", _ExitMgr(ready=True))
    sniper = sn.Sniper()
    monkeypatch.setattr(sniper, "_execution_health_allows_buy", lambda: (True, "ok"))
    monkeypatch.setattr(sniper, "_exit_manager_status_allows_buy", lambda: (True, "ok"))
    monkeypatch.setattr(sniper, "_close_flow_allows_buy", lambda: (True, "close_flow_healthy"))
    monkeypatch.setattr(
        sniper,
        "_balanced_growth_state",
        lambda force_refresh=False: {
            "allow_buy": False,
            "mode": "sell_recovery",
            "reason": "balance_buy_sell_ratio_high:3.0>1.2",
            "buy_size_factor": 1.0,
        },
    )

    called = {"price": 0}

    def _fake_price(_pair):
        called["price"] += 1
        return 100.0

    monkeypatch.setattr(sniper, "_get_price", _fake_price)
    result = sniper.execute_trade(_buy_signal(ev_positive=True))
    assert result is False
    assert called["price"] == 0
