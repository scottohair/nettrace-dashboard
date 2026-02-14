#!/usr/bin/env python3
"""Tests for sparse walk-forward out-of-sample fallback behavior."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import strategy_pipeline as sp  # noqa: E402


def _setup_validator(tmp_path, monkeypatch):
    monkeypatch.setattr(sp, "PIPELINE_DB", str(tmp_path / "pipeline.db"))
    monkeypatch.setattr(sp, "TRADER_DB", str(tmp_path / "trader.db"))
    monkeypatch.setattr(sp, "GROWTH_MODE_ENABLED", False)
    monkeypatch.setattr(sp, "REALIZED_ESCALATION_GATE_ENABLED", False)
    monkeypatch.setattr(sp, "EXECUTION_HEALTH_ESCALATION_GATE", False)
    monkeypatch.setattr(sp, "WARM_WALKFORWARD_LEAKAGE_ENFORCE", False)
    monkeypatch.setattr(sp, "WALKFORWARD_SPARSE_MAX_OOS_CANDLES", 72)
    monkeypatch.setattr(sp, "WALKFORWARD_SPARSE_OOS_MIN_WIN_RATE", 0.75)
    monkeypatch.setattr(sp, "WALKFORWARD_SPARSE_OOS_MIN_RETURN_PCT", 0.30)
    monkeypatch.setattr(sp, "WALKFORWARD_SPARSE_OOS_MAX_DRAWDOWN_PCT", 2.00)

    validator = sp.StrategyValidator()
    strategy = sp.RSIStrategy()
    pair = "BTC-USD"
    validator.register_strategy(strategy, pair)
    return validator, strategy, pair


def test_submit_backtest_allows_strong_sparse_zero_oos_trades(tmp_path, monkeypatch):
    validator, strategy, pair = _setup_validator(tmp_path, monkeypatch)
    passed, message = validator.submit_backtest(
        strategy.name,
        pair,
        {
            "total_trades": 4,
            "win_rate": 1.0,
            "total_return_pct": 0.48,
            "max_drawdown_pct": 0.30,
            "candle_count": 180,
            "losses": 0,
            "final_open_position_blocked": False,
            "walkforward": {
                "enabled": True,
                "available": True,
                "selection_method": "median_oos_return",
                "in_sample": {"total_return_pct": 0.9, "total_trades": 8},
                "out_of_sample": {
                    "total_trades": 0,
                    "win_rate": 0.0,
                    "total_return_pct": 0.0,
                    "max_drawdown_pct": 0.0,
                    "losses": 0,
                },
                "out_of_sample_candles": 58,
                "windows_tested": 3,
                "leakage_diagnostics": {"passed": True, "overlap_count": 0},
                "leakage_summary": {
                    "overlap_count": 0,
                    "duplicate_count_in_sample": 0,
                    "duplicate_count_out_of_sample": 0,
                    "non_monotonic_in_sample": 0,
                    "non_monotonic_out_of_sample": 0,
                    "boundary_gap_seconds": 120,
                },
            },
        },
    )
    validator.db.close()

    assert passed is True
    assert "Promoted to WARM" in message


def test_submit_backtest_blocks_sparse_zero_oos_when_in_sample_evidence_weak(tmp_path, monkeypatch):
    validator, strategy, pair = _setup_validator(tmp_path, monkeypatch)
    passed, message = validator.submit_backtest(
        strategy.name,
        pair,
        {
            "total_trades": 4,
            "win_rate": 0.50,
            "total_return_pct": 0.12,
            "max_drawdown_pct": 0.40,
            "candle_count": 180,
            "losses": 0,
            "final_open_position_blocked": False,
            "walkforward": {
                "enabled": True,
                "available": True,
                "selection_method": "median_oos_return",
                "in_sample": {"total_return_pct": 0.12, "total_trades": 8},
                "out_of_sample": {
                    "total_trades": 0,
                    "win_rate": 0.0,
                    "total_return_pct": 0.0,
                    "max_drawdown_pct": 0.0,
                    "losses": 0,
                },
                "out_of_sample_candles": 58,
                "windows_tested": 3,
                "leakage_diagnostics": {"passed": True, "overlap_count": 0},
                "leakage_summary": {
                    "overlap_count": 0,
                    "duplicate_count_in_sample": 0,
                    "duplicate_count_out_of_sample": 0,
                    "non_monotonic_in_sample": 0,
                    "non_monotonic_out_of_sample": 0,
                    "boundary_gap_seconds": 120,
                },
            },
        },
    )
    validator.db.close()

    assert passed is False
    assert "walkforward sparse oos window lacks strong in-sample evidence" in message
