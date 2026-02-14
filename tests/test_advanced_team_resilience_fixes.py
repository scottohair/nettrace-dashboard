#!/usr/bin/env python3
"""Resilience tests for advanced-team fallback and risk gates."""

import os
import sys
import sqlite3
import unittest
import tempfile
import shutil
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

from advanced_team.quant_optimizer_agent import QuantOptimizerAgent  # noqa: E402
from advanced_team.research_agent import ResearchAgent  # noqa: E402
from advanced_team.risk_agent import RiskAgent  # noqa: E402
from advanced_team.strategy_agent import StrategyAgent  # noqa: E402


class TestAdvancedTeamResilienceFixes(unittest.TestCase):
    """Validate fallback path reliability and conservative gating under degraded data."""

    def _build_db(self, path):
        con = sqlite3.connect(path)
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS price_feeds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                asset TEXT NOT NULL,
                price_usd REAL NOT NULL,
                change_pct REAL DEFAULT 0.0,
                metadata TEXT DEFAULT '{}',
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        con.execute(
            """
            INSERT INTO price_feeds (source, asset, price_usd, change_pct, metadata, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("coinbase", "btc", 70000.0, 0.0123, '{"cached_at": 1771010000}', "2026-02-13 12:00:00"),
        )
        con.commit()
        con.close()

    def test_research_agent_falls_back_to_local_prices_and_volatility(self):
        """Research memo should use local db data when public APIs return empty."""
        agent = ResearchAgent()
        agent.bus = type("DummyBus", (), {
            "publish": lambda *a, **k: 1,
            "read_latest": lambda *a, **k: [],
        })()
        tmpdir = tempfile.mkdtemp()
        db_path = os.path.join(tmpdir, "exchange_scanner.db")
        try:
            with patch.object(sys.modules["advanced_team.research_agent"], "EXCHANGE_SCANNER_DB", new=db_path):
                self._build_db(db_path)
                with patch.object(agent, "fetch_coinbase_prices", return_value={}):
                    with patch.object(agent, "fetch_coinbase_candles", return_value=None):
                        with patch.object(agent, "fetch_nettrace_signals", return_value={"signal_count": 0, "by_type": {}, "raw": []}):
                            with patch.object(agent, "fetch_fear_greed", return_value={"value": 50, "classification": "Neutral", "timestamp": ""}):
                                with patch.object(agent, "fetch_coingecko_market", return_value={}):
                                    with patch.object(agent, "fetch_cross_exchange_spreads", return_value={"spreads": {}, "arb_opportunities": []}):
                                        memo = agent.run(1)

            self.assertEqual(memo["prices"]["BTC-USD"]["source"], "exchange_scanner_local")
            self.assertEqual(memo["coingecko"]["BTC"]["change_24h_pct"], 1.23)
            self.assertTrue(memo["fallback_used"]["prices"])
            self.assertNotIn("volatility", memo["data_fidelity"]["missing_sources"])
            self.assertGreaterEqual(memo["data_fidelity"]["source_health_score"], 0.5)
        finally:
            shutil.rmtree(tmpdir)

    def test_quant_optimizer_relaxes_confidence_for_degraded_data(self):
        """Degraded data inputs should lower min_conf and trade size constraints."""
        agent = QuantOptimizerAgent()
        memo = {
            "data_fidelity": {
                "source_health_score": 0.55,
                "missing_sources": ["nettrace_signals", "coingecko", "volatility"],
                "fallback_used": {"prices": True},
            }
        }
        overrides = agent._derive_risk_overrides("DEFENSIVE", {"min_confidence": 0.66}, [], memo)

        self.assertEqual(overrides["data_quality_mode"], "degraded")
        self.assertLess(overrides["min_confidence"], 0.73)
        self.assertEqual(overrides["max_trade_usd"], 2.8)
        self.assertEqual(overrides["max_trades_per_cycle"], 1)
        self.assertLessEqual(overrides["size_multiplier"], 0.9)

    def test_risk_agent_applies_degraded_source_cap(self):
        """Risk should constrain sizes when proposal is sourced from degraded data."""
        risk = RiskAgent()
        proposal = {
            "pair": "BTC-USDC",
            "direction": "BUY",
            "confidence": 0.6,
            "confirming_signals": 2,
            "suggested_size_usd": 5.0,
            "data_quality_mode": "degraded",
            "data_fidelity": {"missing_sources": ["coingecko"]},
        }
        overrides = {
            "min_confidence": 0.48,
            "max_trade_usd": 2.8,
            "max_trades_per_cycle": 1,
            "data_quality_penalty": 0.20,
            "data_quality_mode": "degraded",
            "blocked_pairs": [],
        }

        verdict, adjusted, reason = risk.evaluate_proposal(proposal, portfolio=None, overrides=overrides)

        self.assertEqual(verdict, "APPROVED")
        self.assertLessEqual(adjusted["approved_size_usd"], 2.38)
        self.assertIn("Degraded-source cap", reason)

    def test_strategy_propagates_data_fidelity_to_proposals(self):
        """Strategy proposals should carry data-fidelity metadata for downstream risk gating."""
        agent = StrategyAgent()
        memo = {
            "cycle": 1,
            "data_quality_mode": "degraded",
            "data_fidelity": {"source_health_score": 0.4},
            "source_health": {"prices_api": False},
            "learning_insights": {},
            "prices": {"BTC-USD": {"price": 10000.0}},
            "volatility": {"BTC-USD": {"range_pct": 1.0}, "ETH-USD": {"range_pct": 1.0}, "SOL-USD": {"range_pct": 1.0}},
            "coingecko": {"BTC": {"change_24h_pct": 1.2}},
            "nettrace_signals": {"signal_count": 0, "by_type": {}, "raw": []},
            "fear_greed": {"value": 50, "classification": "Neutral", "timestamp": ""},
            "cross_exchange": {"spreads": {}},
        }
        with patch("advanced_team.strategy_agent.TRADEABLE_PAIRS", {"BTC-USD": "BTC-USDC"}):
            with patch.object(agent, "_get_optimization_context", return_value={
                "weights": {"price_momentum": 0.2, "volatility_regime": 0.15, "fear_greed": 0.1, "nettrace_latency": 0.25, "cross_exchange_spread": 0.15, "volume_trend": 0.15},
                "min_confidence": 0.25,
                "size_multiplier": 1.0,
                "pair_bias": {},
                "blocked_pairs": [],
                "disabled_families": [],
            }):
                with patch.object(agent, "_score_price_momentum", return_value=(0.95, "BUY", "ok")):
                    with patch.object(agent, "_score_volatility_regime", return_value=(0.95, "BUY", "ok")):
                        with patch.object(agent, "_score_fear_greed", return_value=(0.95, "BUY", "ok")):
                            with patch.object(agent, "_score_nettrace_signals", return_value=(0.95, "BUY", "ok")):
                                with patch.object(agent, "_score_cross_exchange", return_value=(0.95, "BUY", "ok")):
                                    with patch.object(agent, "_score_volume", return_value=(0.95, "BUY", "ok")):
                                        proposals = agent.generate_proposals(memo)

        self.assertEqual(len(proposals), 1)
        self.assertEqual(proposals[0]["data_quality_mode"], "degraded")
        self.assertEqual(proposals[0]["data_fidelity"]["source_health_score"], 0.4)


if __name__ == "__main__":
    unittest.main()
