#!/usr/bin/env python3
"""Tests for execution-cost aware gain scoring in quant_100_runner."""

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import quant_100_runner as q100  # noqa: E402


class TestQuant100GainScoring(unittest.TestCase):

    def test_score_gain_potential_penalizes_execution_shortfall(self):
        base_metrics = {
            "return_pct": 1.2,
            "win_rate": 0.62,
            "trades": 12,
            "drawdown_pct": 1.0,
            "sharpe": 0.8,
            "losses": 0,
            "execution_shortfall_bps": 0.0,
        }
        walkforward = {
            "available": True,
            "out_of_sample": {"total_return_pct": 0.2, "total_trades": 4},
        }

        low_shortfall = dict(base_metrics)
        low_shortfall["execution_shortfall_bps"] = 2.0
        high_shortfall = dict(base_metrics)
        high_shortfall["execution_shortfall_bps"] = 20.0

        with patch.object(
            q100, "QUANT100_EXECUTION_SHORTFALL_PENALTY_PER_BPS", 0.03, create=True
        ):
            low_score = q100._score_gain_potential(low_shortfall, walkforward)
            high_score = q100._score_gain_potential(high_shortfall, walkforward)

        self.assertGreater(low_score, high_score)
        self.assertEqual(low_score - high_score, 0.54)

    def test_ranked_ideas_includes_execution_shortfall_bps(self):
        rows = [
            {
                "id": "Q001",
                "strategy_name": "mkt_001",
                "strategy_base": "momentum",
                "pair": "BTC-USD",
                "target_region": "ewr",
                "status": "promoted_warm",
                "metrics": {
                    "return_pct": 1.2,
                    "execution_shortfall_bps": 11.0,
                    "trades": 10,
                    "sharpe": 1.0,
                    "win_rate": 0.6,
                },
                "walkforward": {"available": False, "out_of_sample": {}},
                "gain_gate": {"passed": True},
            }
        ]

        ranked = q100._ranked_ideas(rows)
        self.assertEqual(ranked[0]["execution_shortfall_bps"], 11.0)

    def test_ranked_ideas_includes_walkforward_stdev(self):
        rows = [
            {
                "id": "Q002",
                "strategy_name": "mkt_002",
                "strategy_base": "momentum",
                "pair": "ETH-USD",
                "target_region": "ewr",
                "status": "promoted_warm",
                "metrics": {
                    "return_pct": 1.2,
                    "execution_shortfall_bps": 9.0,
                    "trades": 10,
                    "sharpe": 1.0,
                    "win_rate": 0.6,
                },
                "walkforward": {
                    "available": True,
                    "out_of_sample": {"total_return_pct": 0.2, "total_trades": 4},
                    "oos_return_distribution_pct": {"stdev": 2.5},
                },
                "gain_gate": {"passed": True},
            }
        ]

        ranked = q100._ranked_ideas(rows)
        self.assertEqual(ranked[0]["walkforward_oos_return_stdev_pct"], 2.5)

    def test_score_gain_potential_penalizes_walkforward_return_instability(self):
        base_metrics = {
            "return_pct": 1.2,
            "win_rate": 0.62,
            "trades": 12,
            "drawdown_pct": 1.0,
            "sharpe": 0.8,
            "losses": 0,
            "execution_shortfall_bps": 0.0,
        }
        stable = {
            "available": True,
            "out_of_sample": {"total_return_pct": 0.2, "total_trades": 4},
            "oos_return_distribution_pct": {"stdev": 0.5},
        }
        unstable = {
            "available": True,
            "out_of_sample": {"total_return_pct": 0.2, "total_trades": 4},
            "oos_return_distribution_pct": {"stdev": 2.5},
        }

        with patch.object(
            q100, "QUANT100_WFO_RETURN_STD_PENALTY_PER_PCT", 0.5, create=True
        ):
            stable_score = q100._score_gain_potential(base_metrics, stable)
            unstable_score = q100._score_gain_potential(base_metrics, unstable)

        self.assertGreater(stable_score, unstable_score)
        self.assertEqual(stable_score - unstable_score, 1.0)

    def test_score_gain_potential_penalizes_low_walkforward_trade_count(self):
        base_metrics = {
            "return_pct": 1.2,
            "win_rate": 0.62,
            "trades": 12,
            "drawdown_pct": 1.0,
            "sharpe": 0.8,
            "losses": 0,
            "execution_shortfall_bps": 0.0,
        }
        low_trades_wf = {
            "available": True,
            "out_of_sample": {"total_return_pct": 0.2, "total_trades": 2},
            "oos_return_distribution_pct": {"stdev": 0.5},
        }
        high_trades_wf = {
            "available": True,
            "out_of_sample": {"total_return_pct": 0.2, "total_trades": 10},
            "oos_return_distribution_pct": {"stdev": 0.5},
        }

        with patch.object(
            q100,
            "QUANT100_MIN_WFO_OOS_TRADES_FOR_GAIN",
            8,
            create=True,
        ), patch.object(
            q100, "QUANT100_MIN_OOS_TRADES_PENALTY_PER_TRADE", 0.45, create=True
        ):
            low_score = q100._score_gain_potential(base_metrics, low_trades_wf)
            high_score = q100._score_gain_potential(base_metrics, high_trades_wf)

        self.assertGreater(high_score, low_score)
        self.assertAlmostEqual(high_score - low_score, 3.1, places=9)

    def test_gain_gate_rejects_low_walkforward_trade_count(self):
        bt = {
            "total_trades": 3,
            "total_return_pct": 0.12,
            "max_drawdown_pct": 1.0,
            "sharpe_ratio": 0.10,
            "execution_intelligence": {"shortfall_bps": 10.0},
        }
        walkforward = {
            "available": True,
            "out_of_sample": {"total_return_pct": 0.05, "total_trades": 4},
        }

        with patch.object(
            q100, "QUANT100_MIN_WFO_OOS_TRADES_FOR_GAIN", 8, create=True
        ):
            passed, reasons = q100._gain_gate(bt, walkforward)

        self.assertFalse(passed)
        self.assertTrue(any("walkforward_oos_trades" in str(r) for r in reasons))

    def test_gain_gate_rejects_excessive_execution_shortfall(self):
        bt = {
            "total_trades": 3,
            "total_return_pct": 0.12,
            "max_drawdown_pct": 1.0,
            "sharpe_ratio": 0.10,
            "execution_intelligence": {"shortfall_bps": 60.0},
        }
        walkforward = {
            "available": True,
            "out_of_sample": {"total_return_pct": 0.05, "total_trades": 8},
        }

        with patch.object(
            q100, "QUANT100_MAX_EXECUTION_SHORTFALL_BPS", 30.0, create=True
        ), patch.object(
            q100, "QUANT100_MIN_WFO_OOS_TRADES_FOR_GAIN", 8, create=True
        ):
            passed, reasons = q100._gain_gate(bt, walkforward)

        self.assertFalse(passed)
        self.assertTrue(any("execution_shortfall_bps" in str(r) for r in reasons))

    def test_gain_gate_allows_tight_execution_shortfall(self):
        bt = {
            "total_trades": 3,
            "total_return_pct": 0.12,
            "max_drawdown_pct": 1.0,
            "sharpe_ratio": 0.10,
            "execution_intelligence": {"shortfall_bps": 10.0},
        }
        walkforward = {
            "available": True,
            "out_of_sample": {"total_return_pct": 0.05, "total_trades": 8},
        }

        with patch.object(
            q100, "QUANT100_MAX_EXECUTION_SHORTFALL_BPS", 30.0, create=True
        ), patch.object(
            q100, "QUANT100_MIN_WFO_OOS_TRADES_FOR_GAIN", 8, create=True
        ):
            passed, reasons = q100._gain_gate(bt, walkforward)

        self.assertTrue(passed, f"gain gate failed unexpectedly: {reasons}")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
