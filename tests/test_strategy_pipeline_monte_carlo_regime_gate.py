#!/usr/bin/env python3
"""Tests for regime-conditioned Monte Carlo thresholding in growth mode."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import strategy_pipeline as sp  # noqa: E402


def _criteria():
    return {
        "min_return_pct": 0.10,
        "max_drawdown_pct": 3.00,
    }


def _results_stub():
    return {
        "pair": "BTC-USD",
        "candle_count": 168,
        "total_return_pct": 0.12,
        "win_rate": 0.65,
        "max_drawdown_pct": 1.10,
        "sharpe_ratio": 0.70,
        "trades": [
            {"side": "SELL", "pnl": 0.01},
            {"side": "SELL", "pnl": 0.01},
            {"side": "SELL", "pnl": 0.01},
            {"side": "SELL", "pnl": 0.01},
        ],
    }


def test_regime_conditioned_threshold_blocks_stressed_profile(monkeypatch):
    monkeypatch.setattr(sp, "MONTE_CARLO_GATE_ENABLED", True)
    monkeypatch.setattr(sp, "MONTE_CARLO_MIN_P50_RETURN_PCT", -0.50)
    monkeypatch.setattr(sp, "MONTE_CARLO_MIN_P05_RETURN_PCT", -0.50)
    monkeypatch.setattr(sp, "MONTE_CARLO_STRESSED_P50_BONUS_PCT", 0.80)
    monkeypatch.setattr(sp, "MONTE_CARLO_STRESSED_P05_BONUS_PCT", 0.00)
    monkeypatch.setattr(sp, "MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_ENABLED", False)

    ctl = sp.GrowthModeController()
    monkeypatch.setattr(
        sp.GrowthModeController,
        "_trade_returns",
        staticmethod(lambda _results: [0.0, 0.0, 0.0, 0.0]),
    )
    monkeypatch.setattr(
        sp.GrowthModeController,
        "_probabilistic_assessment",
        lambda self, *_args, **_kwargs: {
            "regime": "stressed",
            "var95_loss_pct": 0.0,
            "es97_5_loss_pct": 0.0,
            "calibrated_p05_return_pct": 0.0,
            "passed": True,
            "reasons": [],
        },
    )

    report = ctl.evaluate_monte_carlo("s", "BTC-USD", _results_stub(), _criteria())
    assert report["passed"] is False
    assert any("regime(stressed) p50" in str(r) for r in report["reasons"])


def test_tail_penalty_reduces_effective_drawdown_threshold(monkeypatch):
    monkeypatch.setattr(sp, "MONTE_CARLO_GATE_ENABLED", True)
    monkeypatch.setattr(sp, "MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_ENABLED", True)
    monkeypatch.setattr(sp, "MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_SCALE", 0.80)
    monkeypatch.setattr(sp, "MONTE_CARLO_VAR95_PENALTY_WEIGHT", 0.60)
    monkeypatch.setattr(sp, "MONTE_CARLO_ES97_5_PENALTY_WEIGHT", 0.40)

    ctl = sp.GrowthModeController()
    gate = ctl._regime_conditioned_monte_carlo_thresholds(
        _criteria(),
        {
            "regime": "stressed",
            "var95_loss_pct": 1.60,
            "es97_5_loss_pct": 2.80,
        },
    )

    assert gate["tail_penalty_pct"] > 0
    assert gate["effective_max_p95_drawdown_pct"] < gate["base_max_p95_drawdown_pct"]


def test_stressed_tail_penalty_can_change_gate_outcome(monkeypatch):
    monkeypatch.setattr(sp, "MONTE_CARLO_GATE_ENABLED", True)
    monkeypatch.setattr(sp, "MONTE_CARLO_MIN_P50_RETURN_PCT", -0.50)
    monkeypatch.setattr(sp, "MONTE_CARLO_MIN_P05_RETURN_PCT", -0.50)
    monkeypatch.setattr(sp, "MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_ENABLED", True)
    monkeypatch.setattr(sp, "MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_SCALE", 0.80)
    monkeypatch.setattr(sp, "MONTE_CARLO_TAIL_PENALTY_REGIME_MULT_STRESSED", 2.0)
    monkeypatch.setattr(
        sp, "MONTE_CARLO_VAR95_PENALTY_WEIGHT", 0.60
    )
    monkeypatch.setattr(
        sp, "MONTE_CARLO_ES97_5_PENALTY_WEIGHT", 0.40
    )

    ctl = sp.GrowthModeController()

    monkeypatch.setattr(
        sp.GrowthModeController,
        "_trade_returns",
        staticmethod(lambda _results: [0.005, -0.004, 0.002, -0.001]),
    )
    monkeypatch.setattr(
        sp.GrowthModeController,
        "_probabilistic_assessment",
        lambda self, *_args, **_kwargs: {
            "regime": "stressed",
            "var95_loss_pct": 1.6,
            "es97_5_loss_pct": 2.8,
            "calibrated_p05_return_pct": 0.0,
            "passed": True,
            "reasons": [],
        },
    )
    # Force the sampled p95 drawdown to exceed the stressed effective threshold.
    def _percentile(values, pct):
        if pct == 95:
            return 3.5
        if pct == 5:
            return 0.02
        return 0.10

    monkeypatch.setattr(
        sp.GrowthModeController,
        "_percentile",
        staticmethod(_percentile),
    )

    report = ctl.evaluate_monte_carlo("s", "BTC-USD", _results_stub(), _criteria())
    assert report["passed"] is False
    assert any("tail_penalty" in str(reason) and "stressed" in str(reason) for reason in report["reasons"])
