#!/usr/bin/env python3
"""Tests for adaptive COLD sparse gating in strategy_pipeline."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import strategy_pipeline as sp  # noqa: E402


def test_cold_criteria_relaxes_for_sparse_high_quality_window(monkeypatch, tmp_path):
    monkeypatch.setattr(sp, "PIPELINE_DB", str(tmp_path / "pipeline.db"))
    monkeypatch.setattr(sp, "ADAPTIVE_COLD_GATING", True)

    validator = sp.StrategyValidator()
    criteria = validator._cold_criteria(
        {
            "candle_count": 150,
            "total_return_pct": 1.25,
            "win_rate": 0.90,
            "losses": 0,
            "max_drawdown_pct": 1.2,
        }
    )
    validator.db.close()

    assert int(criteria["min_trades"]) == 2
    assert float(criteria["min_win_rate"]) >= 0.65
    assert float(criteria["min_return_pct"]) >= 0.75


def test_cold_criteria_keeps_baseline_when_sparse_quality_is_not_strong(monkeypatch, tmp_path):
    monkeypatch.setattr(sp, "PIPELINE_DB", str(tmp_path / "pipeline.db"))
    monkeypatch.setattr(sp, "ADAPTIVE_COLD_GATING", True)

    validator = sp.StrategyValidator()
    criteria = validator._cold_criteria(
        {
            "candle_count": 150,
            "total_return_pct": 0.40,
            "win_rate": 0.62,
            "losses": 1,
            "max_drawdown_pct": 3.5,
        }
    )
    validator.db.close()

    assert int(criteria["min_trades"]) == 6
    assert float(criteria["min_win_rate"]) == 0.58
    assert float(criteria["min_return_pct"]) == 0.25
