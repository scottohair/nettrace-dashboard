"""Advanced team tests for signal-family decomposition and pruning."""

import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main
from unittest.mock import patch

from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "agents"))

from advanced_team.algorithm_optimizer_agent import AlgorithmOptimizerAgent  # noqa: E402
from advanced_team.learning_agent import LearningAgent  # noqa: E402
from advanced_team.strategy_agent import SIGNAL_WEIGHTS, StrategyAgent  # noqa: E402


class TestAdvancedTeamSignalFamilyFix(TestCase):
    """End-to-end tests for signal-family tracking and pruning flow."""

    def test_learning_generates_signal_family_recommendations(self):
        """Weak families should emit disable/deprioritize recommendations."""
        with TemporaryDirectory() as td:
            with patch(
                "advanced_team.learning_agent.PERF_FILE", os.path.join(td, "performance.json")
            ):
                agent = LearningAgent()
                agent.performance = {
                    "trades": [],
                    "daily_returns": [],
                    "pair_stats": {},
                    "strategy_stats": {},
                    "signal_family_stats": {},
                    "total_pnl": 0.0,
                    "peak_portfolio": 0.0,
                    "max_drawdown_pct": 0.0,
                    "last_portfolio_value": 0.0,
                }

                losing_trade = {
                    "trade": {
                        "score_breakdown": {
                            "price_momentum": 0.80,
                            "nettrace_latency": 0.20,
                        },
                        "size_usd": 100.0,
                        "confidence": 0.30,
                        "pnl_usd": -0.80,
                    }
                }

                for _ in range(4):
                    agent._update_signal_family_stats(losing_trade)

                insights = agent._generate_insights({"results": []})
                family_perf = insights["signal_family_performance"]
                recommendations = insights["signal_family_recommendations"]

                self.assertEqual(family_perf["price_momentum"]["sample_count"], 4)
                self.assertEqual(family_perf["price_momentum"]["avg_pnl_usd"], -0.64)
                self.assertEqual(family_perf["nettrace_latency"]["sample_count"], 4)
                self.assertEqual(family_perf["nettrace_latency"]["avg_pnl_usd"], -0.16)
                self.assertTrue(
                    any("DISABLE_FAMILY: price_momentum" in item for item in recommendations)
                )
                self.assertTrue(
                    any("DISABLE_FAMILY: nettrace_latency" in item for item in recommendations)
                )

    def test_algorithm_optimizer_disables_weak_families_and_downweights(self):
        """Low-PnL families should be flagged and down-weighted in tuning."""
        agent = AlgorithmOptimizerAgent()

        learning = {
            "signal_family_performance": {
                "price_momentum": {"sample_count": 4, "avg_pnl_usd": -0.12},
                "nettrace_latency": {"sample_count": 2, "avg_pnl_usd": -0.20},
            }
        }

        disabled, details = agent._derive_disabled_families(learning)
        self.assertEqual(disabled, ["price_momentum"])
        self.assertEqual(details["price_momentum"]["sample_count"], 4)
        self.assertEqual(details["price_momentum"]["avg_pnl_usd"], -0.12)

        overrides = agent._derive_weight_overrides([], learning=learning)
        self.assertLess(overrides["price_momentum"], 0.20)
        self.assertAlmostEqual(sum(overrides.values()), 1.0, delta=1e-3)

    def _build_active_strategy_context(self, disabled_families):
        return {
            "weights": dict(SIGNAL_WEIGHTS),
            "min_confidence": 0.55,
            "size_multiplier": 1.0,
            "pair_bias": {},
            "blocked_pairs": [],
            "disabled_families": disabled_families,
        }

    def test_strategy_agent_skips_all_families_when_disabled(self):
        """No proposals if every family is optimizer-disabled."""
        agent = StrategyAgent()
        disabled_families = [
            "price_momentum",
            "volatility_regime",
            "fear_greed",
            "nettrace_latency",
            "cross_exchange_spread",
            "volume_trend",
        ]

        with patch("advanced_team.strategy_agent.TRADEABLE_PAIRS", {"BTC-USD": "BTC-USDC"}):
            with patch.object(
                agent,
                "_get_optimization_context",
                return_value=self._build_active_strategy_context(disabled_families),
            ):
                proposals = agent.generate_proposals({"cycle": 1})

        self.assertEqual(proposals, [])

    def test_strategy_agent_omits_disabled_families_from_breakdown(self):
        """Disabled factors should be removed from proposal score breakdown."""
        agent = StrategyAgent()
        disabled_families = ["price_momentum", "nettrace_latency"]
        with patch("advanced_team.strategy_agent.TRADEABLE_PAIRS", {"BTC-USD": "BTC-USDC"}):
            with patch.object(
                agent,
                "_score_volatility_regime",
                Mock(return_value=(1.0, "BUY", "ok")),
            ):
                with patch.object(
                    agent,
                    "_score_fear_greed",
                    Mock(return_value=(1.0, "BUY", "ok")),
                ):
                    with patch.object(
                    agent,
                    "_score_cross_exchange",
                    Mock(return_value=(1.0, "BUY", "ok")),
                    ):
                        with patch.object(
                            agent,
                            "_score_volume",
                            Mock(return_value=(1.0, "BUY", "ok")),
                        ):
                            with patch.object(
                                agent,
                                "_get_optimization_context",
                                return_value=self._build_active_strategy_context(
                                    disabled_families
                                ),
                            ):
                                proposals = agent.generate_proposals({"cycle": 1})

        self.assertEqual(len(proposals), 1)
        self.assertNotIn("price_momentum", proposals[0]["score_breakdown"])
        self.assertNotIn("nettrace_latency", proposals[0]["score_breakdown"])


if __name__ == "__main__":
    main()
