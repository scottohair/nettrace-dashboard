#!/usr/bin/env python3
"""Tests for Sniper end-to-end quant exit-plan gating."""

import os
import sys

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

    called = {"price": 0}

    def _fake_price(_pair):
        called["price"] += 1
        return 100.0

    monkeypatch.setattr(sniper, "_get_price", _fake_price)
    result = sniper.execute_trade(_buy_signal(ev_positive=True))
    assert result is False
    assert called["price"] == 0
