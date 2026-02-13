#!/usr/bin/env python3
"""Tests for sparse-evidence warm gating behavior."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import strategy_pipeline as sp  # noqa: E402


def _base_metrics():
    return {
        "total_trades": 12,
        "win_rate": 0.70,
        "total_return_pct": 0.20,
        "max_drawdown_pct": 1.20,
        "runtime_seconds": 4800,
        "sharpe_ratio": 0.80,
        "losses": 0,
        "es97_5_loss_pct": 0.0,
        "evidence": {
            "window_count": 2,
            "windows": [30, 42],
            "source": "multi_window_replay",
        },
    }


def test_sparse_window_history_relaxes_criteria_without_runtime_trade_shortfall(monkeypatch, tmp_path):
    monkeypatch.setattr(sp, "PIPELINE_DB", str(tmp_path / "pipeline.db"))
    monkeypatch.setattr(sp, "TRADER_DB", str(tmp_path / "trader.db"))
    monkeypatch.setattr(sp, "WARM_ADAPTIVE_GATING", True)
    monkeypatch.setattr(sp, "WARM_SPARSE_MAX_WINDOW_CANDLES", 120)
    monkeypatch.setattr(sp, "WARM_SPARSE_MIN_RETURN_PCT", 0.03)
    monkeypatch.setattr(sp, "WARM_SPARSE_MIN_SHARPE", 0.0)

    validator = sp.StrategyValidator()
    metrics = _base_metrics()
    metrics.update(
        {
            "total_trades": 18,
            "runtime_seconds": 7200,
            "win_rate": 0.82,
            "total_return_pct": 0.04,
            "max_drawdown_pct": 2.20,
            "sharpe_ratio": 0.10,
            "evidence": {
                "window_count": 5,
                "windows": [30, 42, 54, 78, 96],
                "source": "multi_window_replay",
            },
        }
    )

    assessment = validator.evaluate_warm_metrics_detail(metrics)
    assert assessment["criteria_mode"] == "adaptive_sparse"
    assert assessment["criteria"]["min_return_pct"] == pytest.approx(0.03, abs=1e-12)
    assert assessment["passed"] is True
    validator.db.close()


def test_sparse_mature_performance_failure_is_rejected(monkeypatch, tmp_path):
    monkeypatch.setattr(sp, "PIPELINE_DB", str(tmp_path / "pipeline.db"))
    monkeypatch.setattr(sp, "TRADER_DB", str(tmp_path / "trader.db"))
    monkeypatch.setattr(sp, "WARM_ADAPTIVE_GATING", True)
    monkeypatch.setattr(sp, "WARM_SPARSE_MAX_WINDOW_CANDLES", 120)
    monkeypatch.setattr(sp, "WARM_MATURE_EVIDENCE_WINDOWS", 4)
    monkeypatch.setattr(sp, "WARM_MATURE_EVIDENCE_MIN_CANDLES", 72)

    validator = sp.StrategyValidator()
    metrics = _base_metrics()
    metrics.update(
        {
            "total_trades": 40,
            "runtime_seconds": 18000,
            "win_rate": 0.90,
            "total_return_pct": -0.01,
            "max_drawdown_pct": 1.50,
            "sharpe_ratio": -0.10,
            "evidence": {
                "window_count": 7,
                "windows": [30, 42, 54, 66, 78, 96, 99],
                "source": "multi_window_replay",
            },
        }
    )

    assessment = validator.evaluate_warm_metrics_detail(metrics)
    assert assessment["passed"] is False
    assert assessment["decision"] == "rejected"
    assert "return_below_min" in assessment["reason_codes"]
    assert "sharpe_below_min" in assessment["reason_codes"]
    validator.db.close()


def test_insufficient_trade_runtime_evidence_stays_collecting(monkeypatch, tmp_path):
    monkeypatch.setattr(sp, "PIPELINE_DB", str(tmp_path / "pipeline.db"))
    monkeypatch.setattr(sp, "TRADER_DB", str(tmp_path / "trader.db"))
    monkeypatch.setattr(sp, "WARM_ADAPTIVE_GATING", True)

    validator = sp.StrategyValidator()
    metrics = _base_metrics()
    metrics.update(
        {
            "total_trades": 1,
            "runtime_seconds": 300,
            "win_rate": 1.0,
            "total_return_pct": 0.50,
            "max_drawdown_pct": 0.60,
            "sharpe_ratio": 1.20,
            "evidence": {
                "window_count": 1,
                "windows": [30],
                "source": "multi_window_replay",
            },
        }
    )

    assessment = validator.evaluate_warm_metrics_detail(metrics)
    assert assessment["passed"] is False
    assert assessment["decision"] == "collecting"
    assert set(assessment["reason_codes"]) == {"trades_below_min", "runtime_below_min"}
    validator.db.close()
