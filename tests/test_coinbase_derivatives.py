#!/usr/bin/env python3
"""Tests for Coinbase derivatives integration.

Covers:
  - Perp product mapping
  - Margin health gates
  - Liquidation distance calculations
  - Close always allowed (never blocked)
  - Leverage cap enforcement
  - Perp risk params (tighter than spot)
  - GoalValidator.should_trade_perp (leverage-scaled confidence)
  - GoalValidator.optimal_venue (prefers perp when available)
"""

import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock, PropertyMock

# Ensure agents dir is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))


class TestGoalValidatorPerp(unittest.TestCase):
    """Test GoalValidator perp-specific methods."""

    def setUp(self):
        import agent_goals
        self.GoalValidator = agent_goals.GoalValidator

    def test_should_trade_perp_basic_pass(self):
        """Standard perp trade at 1x leverage should pass."""
        result = self.GoalValidator.should_trade_perp(
            confidence=0.8, confirming_signals=3,
            direction="BUY", market_regime="neutral",
            leverage=1.0, margin_health=3.0,
        )
        self.assertTrue(result)

    def test_should_trade_perp_higher_confidence_for_leverage(self):
        """Higher leverage requires higher confidence — 2x leverage adds 10% penalty."""
        # At 2x leverage: min_confidence = 0.65 + 0.10 = 0.75
        # 0.74 should fail
        result = self.GoalValidator.should_trade_perp(
            confidence=0.74, confirming_signals=3,
            direction="BUY", market_regime="neutral",
            leverage=2.0, margin_health=3.0,
        )
        self.assertFalse(result)

        # 0.76 should pass
        result = self.GoalValidator.should_trade_perp(
            confidence=0.76, confirming_signals=3,
            direction="BUY", market_regime="neutral",
            leverage=2.0, margin_health=3.0,
        )
        self.assertTrue(result)

    def test_leverage_cap_enforced(self):
        """Leverage above MAX_LEVERAGE (3.0) should be blocked."""
        result = self.GoalValidator.should_trade_perp(
            confidence=0.95, confirming_signals=5,
            direction="BUY", market_regime="uptrend",
            leverage=4.0, margin_health=5.0,
        )
        self.assertFalse(result)

    def test_margin_health_blocks_unhealthy(self):
        """Low margin health should block trades."""
        result = self.GoalValidator.should_trade_perp(
            confidence=0.85, confirming_signals=3,
            direction="BUY", market_regime="neutral",
            leverage=1.0, margin_health=1.5,  # below MIN_MARGIN_HEALTH=2.0
        )
        self.assertFalse(result)

    def test_margin_health_none_allowed(self):
        """None margin_health should not block (for backward compat)."""
        result = self.GoalValidator.should_trade_perp(
            confidence=0.8, confirming_signals=3,
            direction="BUY", market_regime="neutral",
            leverage=1.0, margin_health=None,
        )
        self.assertTrue(result)

    def test_optimal_venue_prefers_perp(self):
        """When perp is available, venue should be 'perp' with lower fees."""
        result = self.GoalValidator.optimal_venue(product_has_perp=True)
        self.assertEqual(result["venue"], "perp")
        self.assertEqual(result["maker_fee"], 0.0000)

    def test_optimal_venue_falls_back_to_spot(self):
        """When no perp available, venue should be 'spot'."""
        result = self.GoalValidator.optimal_venue(product_has_perp=False)
        self.assertEqual(result["venue"], "spot")
        self.assertEqual(result["maker_fee"], 0.004)


class TestPerpMapping(unittest.TestCase):
    """Test perp product discovery and mapping."""

    def test_perp_mapping_builds_correctly(self):
        from coinbase_derivatives_connector import CoinbaseDerivativesConnector

        mock_trader = MagicMock()
        mock_trader.list_perpetual_products.return_value = [
            {"product_id": "BTC-PERP-INTX", "base_currency_id": "BTC", "quote_currency_id": "USD"},
            {"product_id": "ETH-PERP-INTX", "base_currency_id": "ETH", "quote_currency_id": "USDC"},
            {"product_id": "SOL-PERP-INTX", "base_currency_id": "SOL", "quote_currency_id": "USD"},
        ]

        conn = CoinbaseDerivativesConnector(trader=mock_trader)
        mapping = conn.build_perp_mapping()

        self.assertEqual(mapping["BTC"], "BTC-PERP-INTX")
        self.assertEqual(mapping["ETH"], "ETH-PERP-INTX")
        self.assertEqual(mapping["SOL"], "SOL-PERP-INTX")

    def test_perp_for_spot_pair(self):
        from coinbase_derivatives_connector import CoinbaseDerivativesConnector

        mock_trader = MagicMock()
        mock_trader.list_perpetual_products.return_value = [
            {"product_id": "BTC-PERP-INTX", "base_currency_id": "BTC", "quote_currency_id": "USD"},
        ]

        conn = CoinbaseDerivativesConnector(trader=mock_trader)
        self.assertEqual(conn.perp_for_spot_pair("BTC-USD"), "BTC-PERP-INTX")
        self.assertEqual(conn.perp_for_spot_pair("BTC-USDC"), "BTC-PERP-INTX")
        self.assertIsNone(conn.perp_for_spot_pair("DOGE-USD"))
        self.assertIsNone(conn.perp_for_spot_pair(None))


class TestMarginHealth(unittest.TestCase):
    """Test margin health computation."""

    def _make_connector(self, summary, positions=None):
        from coinbase_derivatives_connector import CoinbaseDerivativesConnector

        mock_trader = MagicMock()
        mock_trader._request.side_effect = lambda method, path, *a, **kw: (
            summary if "balance_summary" in path
            else {"positions": positions or []} if "positions" in path
            else {}
        )
        mock_trader.list_perpetual_products.return_value = []
        return CoinbaseDerivativesConnector(trader=mock_trader)

    def test_healthy_margin(self):
        conn = self._make_connector({
            "portfolio_value": "10000",
            "margin_used": "2000",
            "margin_available": "8000",
            "liquidation_threshold": "1000",
        })
        health = conn.margin_health()
        self.assertTrue(health["healthy"])
        self.assertAlmostEqual(health["margin_ratio"], 5.0)
        self.assertTrue(health["can_open_new"])

    def test_unhealthy_margin_low_ratio(self):
        conn = self._make_connector({
            "portfolio_value": "1000",
            "margin_used": "800",
            "margin_available": "200",
            "liquidation_threshold": "100",
        })
        health = conn.margin_health()
        self.assertFalse(health["healthy"])
        self.assertLess(health["margin_ratio"], 2.0)

    def test_no_positions_is_healthy(self):
        conn = self._make_connector({
            "portfolio_value": "5000",
            "margin_used": "0",
            "margin_available": "5000",
            "liquidation_threshold": "0",
        })
        health = conn.margin_health()
        self.assertTrue(health["healthy"])
        self.assertTrue(health["can_open_new"])


class TestLiquidationDistance(unittest.TestCase):
    """Test liquidation distance computation."""

    def _make_connector_with_position(self, position):
        from coinbase_derivatives_connector import CoinbaseDerivativesConnector

        mock_trader = MagicMock()
        mock_trader._request.side_effect = lambda method, path, *a, **kw: (
            {"positions": [position]} if "positions" in path else {}
        )
        mock_trader.list_perpetual_products.return_value = []
        return CoinbaseDerivativesConnector(trader=mock_trader)

    def test_liquidation_distance_safe(self):
        conn = self._make_connector_with_position({
            "product_id": "BTC-PERP-INTX",
            "side": "LONG",
            "number_of_contracts": "0.01",
            "mark_price": "50000",
            "liquidation_price": "10000",
        })
        dist = conn.liquidation_distance("BTC-PERP-INTX")
        self.assertIsNotNone(dist)
        self.assertGreater(dist, 50.0)  # 80% away from liquidation

    def test_liquidation_distance_unsafe(self):
        conn = self._make_connector_with_position({
            "product_id": "BTC-PERP-INTX",
            "side": "LONG",
            "number_of_contracts": "0.01",
            "mark_price": "50000",
            "liquidation_price": "48000",
        })
        dist = conn.liquidation_distance("BTC-PERP-INTX")
        self.assertIsNotNone(dist)
        self.assertLess(dist, 10.0)  # only 4% away from liquidation


class TestCloseAlwaysAllowed(unittest.TestCase):
    """Close (reduce-only) must never be blocked by trading lock or env gate."""

    @patch.dict(os.environ, {"PERP_TRADING_ENABLED": "0"})
    def test_close_allowed_when_trading_disabled(self):
        from coinbase_derivatives_connector import CoinbaseDerivativesConnector

        mock_trader = MagicMock()
        mock_trader._request.side_effect = lambda method, path, *a, **kw: (
            {"positions": [{
                "product_id": "BTC-PERP-INTX",
                "side": "LONG",
                "number_of_contracts": "0.01",
                "mark_price": "50000",
                "liquidation_price": "30000",
            }]} if "positions" in path else {}
        )
        mock_trader.place_limit_order.return_value = {"success_response": {"order_id": "test123"}}
        mock_trader.list_perpetual_products.return_value = []

        conn = CoinbaseDerivativesConnector(trader=mock_trader)
        result = conn.close_position("BTC-PERP-INTX")
        self.assertIsNotNone(result)
        # Verify place_limit_order was called
        mock_trader.place_limit_order.assert_called_once()


class TestPerpRiskParams(unittest.TestCase):
    """Test perp risk params are tighter than spot."""

    def setUp(self):
        import risk_controller
        _test_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._test_db_path = _test_db.name
        _test_db.close()
        risk_controller.DB_PATH = self._test_db_path
        self.rc = risk_controller.RiskController()

    def test_perp_risk_params_smaller_than_spot(self):
        """Perp daily loss limit should be tighter than spot."""
        spot_params = self.rc.get_risk_params(10000, "BTC-USD")
        perp_params = self.rc.get_perp_risk_params(10000, "BTC-USD", leverage=1.0)

        self.assertLess(
            perp_params["perp_daily_loss_limit"],
            spot_params["max_daily_loss"],
            "Perp daily loss limit should be tighter than spot",
        )

    def test_perp_trade_size_scales_with_leverage(self):
        """Higher leverage should reduce max trade size."""
        params_1x = self.rc.get_perp_risk_params(10000, "BTC-USD", leverage=1.0)
        params_2x = self.rc.get_perp_risk_params(10000, "BTC-USD", leverage=2.0)

        self.assertGreater(
            params_1x["max_perp_trade_usd"],
            params_2x["max_perp_trade_usd"],
            "2x leverage should halve max trade size",
        )


if __name__ == "__main__":
    unittest.main()
