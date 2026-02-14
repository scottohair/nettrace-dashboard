#!/usr/bin/env python3
"""Tests for long-chain entry validation in strategic planner."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

from strategic_planner import ChainMovePlanner  # noqa: E402


def test_entry_chain_viable_with_strong_multi_asset_signals(monkeypatch):
    monkeypatch.setenv("LONG_CHAIN_USE_C_GATE", "0")
    planner = ChainMovePlanner()
    signals = {
        "BTC-USDC": {"direction": "BUY", "confidence": 0.91, "momentum": 0.6, "regime": "markup"},
        "ETH-USDC": {"direction": "BUY", "confidence": 0.88, "momentum": 0.5, "regime": "markup"},
        "SOL-USDC": {"direction": "BUY", "confidence": 0.86, "momentum": 0.45, "regime": "accumulation"},
    }
    result = planner.evaluate_entry_chain(
        "BTC-USDC",
        signals,
        max_depth=4,
        min_net_edge=0.002,
        min_worst_case_edge=-0.01,  # relaxed for unit test determinism
    )
    assert isinstance(result, dict)
    assert result["has_exit"] is True
    assert result["steps"] >= 2
    assert any(step.get("action") == "EXIT" for step in result.get("path", []))
    assert result["viable"] is True


def test_entry_chain_rejects_weak_single_node_signal(monkeypatch):
    monkeypatch.setenv("LONG_CHAIN_USE_C_GATE", "0")
    planner = ChainMovePlanner()
    signals = {
        "BTC-USDC": {"direction": "BUY", "confidence": 0.54, "momentum": -0.2, "regime": "markdown"},
    }
    result = planner.evaluate_entry_chain(
        "BTC-USDC",
        signals,
        max_depth=3,
        min_net_edge=0.005,
        min_worst_case_edge=0.0,
    )
    assert isinstance(result, dict)
    assert result["has_exit"] is True  # planner always models an exit
    assert result["viable"] is False
