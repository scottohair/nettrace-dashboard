#!/usr/bin/env python3
"""Tests for sniper long-chain game-theory gate."""

import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import sniper as sn  # noqa: E402


class _ExitMgr:
    def has_exit_plan(self, _pair):
        return True


def _buy_signal(pair="BTC-USDC"):
    return {
        "pair": pair,
        "direction": "BUY",
        "composite_confidence": 0.9,
        "confirming_signals": 4,
        "quant_signals": 3,
        "qual_signals": 1,
        "expected_value": 0.01,
        "ev_positive": True,
        "regime": "markup",
        "momentum": 0.5,
        "details": {},
        "growth_engine": {},
    }


class _PlannerStub:
    def __init__(self, validation):
        self._validation = dict(validation)
        self.ko = types.SimpleNamespace(
            is_banned=lambda _pair: (False, "", 0),
            detect_cycle=lambda _pair: False,
        )
        self.influence = types.SimpleNamespace(
            place_stone=lambda *_args, **_kwargs: None,
            get_influence=lambda *_args, **_kwargs: ("NONE", 0.0, []),
        )
        self.chain_planner = types.SimpleNamespace(
            evaluate_entry_chain=lambda *_args, **_kwargs: dict(self._validation),
        )

    def analyze(self, _portfolio_state, _market_signals, _cash):
        return {
            "territory": {"score": 0.5},
            "chain_length": 2,
            "entry_validations": {"BTC-USDC": dict(self._validation)},
        }


def test_scan_all_blocks_buy_when_chain_invalid(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    monkeypatch.setitem(sn.CONFIG, "pairs", ["BTC-USDC"])
    monkeypatch.setattr(sn.Sniper, "scan_pair", lambda self, _pair: _buy_signal())
    monkeypatch.setattr(sn, "_exit_mgr", _ExitMgr())
    monkeypatch.setattr(sn, "_goals", None)
    monkeypatch.setattr(sn, "_planner", _PlannerStub({
        "pair": "BTC-USDC",
        "viable": False,
        "reason": "blocked: edge below floor",
        "net_edge": 0.001,
        "worst_case_edge": -0.01,
        "steps": 2,
        "has_exit": True,
        "path": [],
    }))

    sniper = sn.Sniper()
    monkeypatch.setattr(sniper, "_get_holdings", lambda: ({}, 100.0))
    actionable = sniper.scan_all()
    assert actionable == []


def test_scan_all_allows_buy_when_chain_valid(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    monkeypatch.setitem(sn.CONFIG, "pairs", ["BTC-USDC"])
    monkeypatch.setattr(sn.Sniper, "scan_pair", lambda self, _pair: _buy_signal())
    monkeypatch.setattr(sn, "_exit_mgr", _ExitMgr())
    monkeypatch.setattr(sn, "_goals", None)
    monkeypatch.setattr(sn, "_planner", _PlannerStub({
        "pair": "BTC-USDC",
        "viable": True,
        "reason": "chain edge 2.4%",
        "net_edge": 0.024,
        "worst_case_edge": 0.004,
        "steps": 3,
        "has_exit": True,
        "path": [{"step": 1, "action": "BUY"}, {"step": 2, "action": "EXIT"}],
    }))

    sniper = sn.Sniper()
    monkeypatch.setattr(sniper, "_get_holdings", lambda: ({}, 100.0))
    actionable = sniper.scan_all()
    assert len(actionable) == 1
    assert actionable[0]["pair"] == "BTC-USDC"
    assert actionable[0]["direction"] == "BUY"
    assert actionable[0]["strategic_chain"]["viable"] is True


def test_resolve_buy_pair_routes_to_usd_when_usdc_insufficient(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    sniper = sn.Sniper()
    monkeypatch.setattr(sniper, "_get_quote_balances", lambda: {"USD": 110.0, "USDC": 4.7})
    routed = sniper._resolve_buy_pair_for_balance("BTC-USDC", min_quote_needed=10.0)
    assert routed == "BTC-USD"


def test_resolve_buy_pair_keeps_usdc_when_funded(monkeypatch, tmp_path):
    monkeypatch.setattr(sn, "SNIPER_DB", str(tmp_path / "sniper.db"))
    sniper = sn.Sniper()
    monkeypatch.setattr(sniper, "_get_quote_balances", lambda: {"USD": 5.0, "USDC": 20.0})
    routed = sniper._resolve_buy_pair_for_balance("BTC-USDC", min_quote_needed=10.0)
    assert routed == "BTC-USDC"
