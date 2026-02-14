#!/usr/bin/env python3
"""Tests for randomized walk-forward splits and leakage diagnostics."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import quant_100_runner as q100  # noqa: E402


def _candles(n, start=1_700_000_000, step=300):
    out = []
    ts = start
    for _ in range(n):
        out.append(
            {
                "time": ts,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
            }
        )
        ts += step
    return out


def test_walkforward_candidate_splits_are_deterministic_and_viable(monkeypatch):
    monkeypatch.setattr(q100, "WALKFORWARD_MIN_TOTAL_CANDLES", 60)
    monkeypatch.setattr(q100, "WALKFORWARD_MIN_OOS_CANDLES", 24)
    monkeypatch.setattr(q100, "WALKFORWARD_EMBARGO_CANDLES", 2)
    monkeypatch.setattr(q100, "WALKFORWARD_SPLIT_JITTER_PCT", 0.10)
    monkeypatch.setattr(q100, "WALKFORWARD_RANDOM_SPLITS", 5)

    splits_a = q100._walkforward_candidate_splits(200, seed_material="x|BTC-USD")
    splits_b = q100._walkforward_candidate_splits(200, seed_material="x|BTC-USD")

    assert splits_a == splits_b
    assert len(splits_a) >= 3
    for split in splits_a:
        assert split >= 30
        oos_count = 200 - (split + q100.WALKFORWARD_EMBARGO_CANDLES)
        assert oos_count >= q100.WALKFORWARD_MIN_OOS_CANDLES


def test_walkforward_leakage_diagnostics_detects_overlap_and_duplicates():
    candles_is = [
        {"time": 10},
        {"time": 20},
        {"time": 30},
    ]
    candles_oos = [
        {"time": 30},  # overlap
        {"time": 40},
        {"time": 40},  # duplicate
    ]
    diag = q100._walkforward_leakage_diagnostics(candles_is, candles_oos, split_idx=3, oos_start_idx=3)
    assert diag["passed"] is False
    assert diag["overlap_count"] >= 1
    assert diag["duplicate_count_out_of_sample"] >= 1


def test_build_walkforward_returns_selected_window(monkeypatch):
    monkeypatch.setattr(q100, "WALKFORWARD_MIN_TOTAL_CANDLES", 60)
    monkeypatch.setattr(q100, "WALKFORWARD_MIN_OOS_CANDLES", 24)
    monkeypatch.setattr(q100, "WALKFORWARD_EMBARGO_CANDLES", 2)
    monkeypatch.setattr(q100, "WALKFORWARD_RANDOM_SPLITS", 3)

    class DummyBacktester:
        @staticmethod
        def run(_strategy, candles, _pair):
            n = len(candles)
            return {
                "candle_count": n,
                "total_return_pct": float((n % 17) / 10.0),
                "total_trades": max(1, n // 20),
                "win_rate": 0.6,
                "losses": 0,
                "max_drawdown_pct": 1.0,
                "sharpe_ratio": 0.8,
                "final_open_position_blocked": False,
            }

    candles = _candles(180)
    exp = {"strategy_base": "momentum", "strategy_name": "wf_test_strategy"}
    params = {"short_window": 5, "long_window": 20, "volume_mult": 1.5}

    wf = q100._build_walkforward(DummyBacktester(), exp, params, candles, "BTC-USD")
    assert wf["enabled"] is True
    assert wf["available"] is True
    assert wf["windows_tested"] >= 1
    assert isinstance(wf.get("split_candidates"), list)
    assert isinstance(wf.get("leakage_diagnostics"), dict)
