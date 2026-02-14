#!/usr/bin/env python3
"""Tests for walk-forward leakage safety gates in strategy promotion."""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import strategy_pipeline as sp  # noqa: E402


def _base_metrics():
    return {
        "total_trades": 15,
        "win_rate": 0.80,
        "total_return_pct": 0.45,
        "max_drawdown_pct": 1.2,
        "runtime_seconds": 7200,
        "sharpe_ratio": 1.10,
        "losses": 0,
    }


def _base_backtest_results(pass_leakage=True):
    return {
        "total_trades": 24,
        "win_rate": 0.67,
        "total_return_pct": 1.25,
        "max_drawdown_pct": 1.4,
        "candle_count": 200,
        "walkforward": {
            "enabled": True,
            "available": True,
            "selection_method": "median_oos_return",
            "in_sample": {
                "total_return_pct": 0.90,
                "total_trades": 15,
            },
            "out_of_sample": {
                "total_trades": 4,
                "win_rate": 0.65,
                "total_return_pct": 0.35,
                "max_drawdown_pct": 0.75,
                "losses": 0,
            },
            "out_of_sample_candles": 120,
            "windows_tested": 3,
            "leakage_diagnostics": {
                "passed": bool(pass_leakage),
                "split_index": 80,
                "oos_start_index": 82,
                "overlap_count": 0 if pass_leakage else 2,
                "duplicate_count_in_sample": 0,
                "duplicate_count_out_of_sample": 0,
                "non_monotonic_in_sample": 0,
                "non_monotonic_out_of_sample": 0,
                "boundary_gap_seconds": 120,
            },
        },
    }


def _setup_validator_with_backtest(tmp_path, monkeypatch, backtest_results):
    monkeypatch.setattr(sp, "PIPELINE_DB", str(tmp_path / "pipeline.db"))
    monkeypatch.setattr(sp, "TRADER_DB", str(tmp_path / "trader.db"))
    monkeypatch.setattr(sp, "GROWTH_MODE_ENABLED", False)
    monkeypatch.setattr(sp, "REALIZED_ESCALATION_GATE_ENABLED", False)
    monkeypatch.setattr(sp, "EXECUTION_HEALTH_ESCALATION_GATE", False)

    validator = sp.StrategyValidator()
    strategy = sp.MomentumStrategy()
    pair = "BTC-USD"
    validator.register_strategy(strategy, pair)
    validator.db.execute(
        "UPDATE strategy_registry SET stage='WARM', backtest_results_json=? WHERE name=? AND pair=?",
        (json.dumps(backtest_results), strategy.name, pair),
    )
    validator.db.commit()
    validator._set_budget_profile(
        strategy.name,
        pair,
        {
            "tier": "standard",
            "current_budget_usd": 1.0,
            "starter_budget_usd": 1.0,
            "max_budget_usd": 5.0,
        },
    )
    return validator, strategy, pair


def test_check_warm_promotion_blocks_when_walkforward_leakage_detected(monkeypatch, tmp_path):
    backtest_results = _base_backtest_results(pass_leakage=False)
    validator, strategy, pair = _setup_validator_with_backtest(tmp_path, monkeypatch, backtest_results)
    promoted, message = validator.check_warm_promotion(
        strategy.name, pair, _base_metrics()
    )
    validator.db.close()

    assert promoted is False
    assert "walkforward_leakage_gate_failed:walkforward_leakage_overlap_count=2" in message


def test_submit_backtest_blocks_cold_promotion_when_backtest_walkforward_is_leaked(tmp_path, monkeypatch):
    monkeypatch.setattr(sp, "PIPELINE_DB", str(tmp_path / "pipeline.db"))
    monkeypatch.setattr(sp, "TRADER_DB", str(tmp_path / "trader.db"))
    monkeypatch.setattr(sp, "GROWTH_MODE_ENABLED", False)
    backtest_results = _base_backtest_results(pass_leakage=False)

    strategy = sp.MomentumStrategy()
    validator = sp.StrategyValidator()
    validator.register_strategy(strategy, "BTC-USD")
    passed, message = validator.submit_backtest(
        strategy.name,
        "BTC-USD",
        {
            "total_trades": 24,
            "win_rate": 0.67,
            "total_return_pct": 1.25,
            "max_drawdown_pct": 1.4,
            "candle_count": 200,
            "final_open_position_blocked": False,
            "losses": 0,
            "walkforward": backtest_results["walkforward"],
        },
    )
    validator.db.close()

    assert passed is False
    assert "walkforward_leakage_gate_failed:walkforward_leakage_overlap_count=2" in message
