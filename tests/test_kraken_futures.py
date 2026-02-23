#!/usr/bin/env python3
"""Tests for Kraken Futures/Derivatives connector.

Covers:
  1. TestFuturesAuth -- signing, private request
  2. TestFuturesProducts -- list products, perp mapping, perp_for_spot_pair
  3. TestFuturesPositions -- get positions, get specific, close position
  4. TestFuturesMargin -- portfolio summary, margin health, liquidation distance
  5. TestFuturesFundingRates -- get rates, funding opportunities
  6. TestFuturesOrderPlacement (4-layer safety gate):
     - test_layer1_env_gate
     - test_layer2_margin_health
     - test_layer3_liquidation_distance
     - test_layer4_leverage_cap
     - test_goalvalidator_gate
     - test_max_positions
     - test_successful_order
     - test_reduce_only_skips_checks
     - test_post_only_default
  7. TestFuturesBasis -- scan_basis_opportunities
  8. TestFuturesTradeDB -- trade recording

All API calls mocked with unittest.mock.patch on _futures_request.
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Ensure agents dir is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))


# ---- Helper: patch environment before import ----
_FUTURES_ENV = {
    "KRAKEN_FUTURES_ENABLED": "1",
    "KRAKEN_FUTURES_API_KEY": "test_api_key",
    "KRAKEN_FUTURES_PRIVATE_KEY": "dGVzdF9wcml2YXRlX2tleQ==",  # base64("test_private_key")
    "KRAKEN_FUTURES_MAX_LEVERAGE": "3.0",
    "KRAKEN_FUTURES_MIN_MARGIN_HEALTH": "2.0",
    "KRAKEN_FUTURES_MIN_LIQ_BUFFER": "50.0",
    "KRAKEN_FUTURES_MAX_POSITIONS": "5",
}

_FUTURES_DISABLED_ENV = {
    "KRAKEN_FUTURES_ENABLED": "0",
    "KRAKEN_FUTURES_API_KEY": "test_api_key",
    "KRAKEN_FUTURES_PRIVATE_KEY": "dGVzdF9wcml2YXRlX2tleQ==",
}


def _import_connector():
    """Import the connector module (re-import to pick up env changes)."""
    import importlib
    if "kraken_futures_connector" in sys.modules:
        mod = importlib.reload(sys.modules["kraken_futures_connector"])
    else:
        mod = importlib.import_module("kraken_futures_connector")
    return mod


class TestFuturesAuth(unittest.TestCase):
    """Test Kraken Futures API authentication."""

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_futures_sign_returns_string(self):
        """_futures_sign should return a base64-encoded string."""
        mod = _import_connector()
        # Need to set module-level vars after reload
        mod.FUTURES_PRIVATE_KEY = "dGVzdF9wcml2YXRlX2tleQ=="
        result = mod._futures_sign("/test", "data=1", "12345")
        self.assertIsInstance(result, str)
        # Should be valid base64
        import base64
        decoded = base64.b64decode(result)
        self.assertGreater(len(decoded), 0)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_futures_request_missing_keys(self):
        """Request with empty keys should return error."""
        mod = _import_connector()
        mod.FUTURES_API_KEY = ""
        mod.FUTURES_PRIVATE_KEY = ""
        result = mod._futures_request("GET", "/test")
        self.assertIn("error", result)
        self.assertIn("keys not configured", result["error"])


class TestFuturesProducts(unittest.TestCase):
    """Test futures product discovery and mapping."""

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_list_futures_products(self):
        """list_futures_products should return instruments from API."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "instruments": [
                    {"symbol": "PF_XBTUSD", "type": "futures_inverse"},
                    {"symbol": "PF_ETHUSD", "type": "futures_inverse"},
                    {"symbol": "FI_XBTUSD_250328", "type": "futures_inverse"},
                ]
            }
            products = mod.KrakenFuturesConnector.list_futures_products()
            self.assertEqual(len(products), 3)
            mock_req.assert_called_once_with("GET", "/instruments")

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_list_futures_products_empty(self):
        """list_futures_products should return [] on API failure."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {"error": "API unavailable"}
            products = mod.KrakenFuturesConnector.list_futures_products()
            self.assertEqual(products, [])

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_build_perp_mapping_from_api(self):
        """build_perp_mapping should map base currencies to PF_ products."""
        mod = _import_connector()
        conn = mod.KrakenFuturesConnector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "instruments": [
                    {"symbol": "PF_XBTUSD", "type": "futures_inverse"},
                    {"symbol": "PF_ETHUSD", "type": "futures_inverse"},
                    {"symbol": "PF_SOLUSD", "type": "futures_inverse"},
                    {"symbol": "PF_XDGUSD", "type": "futures_inverse"},
                ]
            }
            mapping = conn.build_perp_mapping()
            self.assertEqual(mapping["BTC"], "PF_XBTUSD")
            self.assertEqual(mapping["ETH"], "PF_ETHUSD")
            self.assertEqual(mapping["SOL"], "PF_SOLUSD")
            self.assertEqual(mapping["DOGE"], "PF_XDGUSD")

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_build_perp_mapping_fallback_to_static(self):
        """build_perp_mapping should fall back to PERP_PRODUCTS on API failure."""
        mod = _import_connector()
        conn = mod.KrakenFuturesConnector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {"error": "timeout"}
            mapping = conn.build_perp_mapping()
            self.assertEqual(mapping["BTC"], "PF_XBTUSD")
            self.assertIn("ETH", mapping)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_perp_for_spot_pair(self):
        """perp_for_spot_pair should map spot pairs to PF_ products."""
        mod = _import_connector()
        self.assertEqual(mod.KrakenFuturesConnector.perp_for_spot_pair("BTC-USD"), "PF_XBTUSD")
        self.assertEqual(mod.KrakenFuturesConnector.perp_for_spot_pair("ETH-USDC"), "PF_ETHUSD")
        self.assertEqual(mod.KrakenFuturesConnector.perp_for_spot_pair("DOGE-USD"), "PF_XDGUSD")
        self.assertIsNone(mod.KrakenFuturesConnector.perp_for_spot_pair("UNKNOWN-USD"))
        self.assertIsNone(mod.KrakenFuturesConnector.perp_for_spot_pair(None))
        self.assertIsNone(mod.KrakenFuturesConnector.perp_for_spot_pair(""))


class TestFuturesPositions(unittest.TestCase):
    """Test position management."""

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_get_positions(self):
        """get_positions should return list of open positions."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "openPositions": [
                    {"symbol": "PF_XBTUSD", "side": "long", "size": 100, "price": 50000},
                    {"symbol": "PF_ETHUSD", "side": "short", "size": 50, "price": 3000},
                ]
            }
            positions = mod.KrakenFuturesConnector.get_positions()
            self.assertEqual(len(positions), 2)
            self.assertEqual(positions[0]["symbol"], "PF_XBTUSD")

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_get_positions_empty(self):
        """get_positions should return [] when no positions."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {"openPositions": []}
            positions = mod.KrakenFuturesConnector.get_positions()
            self.assertEqual(positions, [])

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_get_position_specific(self):
        """get_position should return specific position by product_id."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "openPositions": [
                    {"symbol": "PF_XBTUSD", "side": "long", "size": 100, "price": 50000},
                    {"symbol": "PF_ETHUSD", "side": "short", "size": 50, "price": 3000},
                ]
            }
            pos = mod.KrakenFuturesConnector.get_position("PF_ETHUSD")
            self.assertIsNotNone(pos)
            self.assertEqual(pos["symbol"], "PF_ETHUSD")

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_get_position_not_found(self):
        """get_position should return None when product not in positions."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {"openPositions": []}
            pos = mod.KrakenFuturesConnector.get_position("PF_XBTUSD")
            self.assertIsNone(pos)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_close_position(self):
        """close_position should send market reduce-only order."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            # First call: get positions, second call: send order
            mock_req.side_effect = [
                {"openPositions": [
                    {"symbol": "PF_XBTUSD", "side": "long", "size": 100, "price": 50000},
                ]},
                {"sendStatus": {"status": "placed", "order_id": "close123"}},
            ]
            result = mod.KrakenFuturesConnector.close_position("PF_XBTUSD")
            self.assertIn("sendStatus", result)
            # Verify the close order was a sell (opposite of long)
            call_args = mock_req.call_args_list[1]
            self.assertEqual(call_args[0][0], "POST")
            self.assertEqual(call_args[0][1], "/sendorder")
            order_data = call_args[0][2]
            self.assertEqual(order_data["side"], "sell")
            self.assertEqual(order_data["reduceOnly"], "true")

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_close_position_no_position(self):
        """close_position should return error when no position exists."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {"openPositions": []}
            result = mod.KrakenFuturesConnector.close_position("PF_XBTUSD")
            self.assertIn("error", result)


class TestFuturesMargin(unittest.TestCase):
    """Test margin health and portfolio summary."""

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_get_portfolio_summary(self):
        """get_portfolio_summary should parse account data."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "accounts": {
                    "flex": {
                        "type": "multiCollateralMargin",
                        "pv": 10000,
                        "am": 8000,
                        "im": 1500,
                        "mm": 1000,
                        "pnl": 500,
                    }
                }
            }
            summary = mod.KrakenFuturesConnector.get_portfolio_summary()
            self.assertEqual(summary["portfolio_value"], 10000)
            self.assertEqual(summary["available_margin"], 8000)
            self.assertEqual(summary["initial_margin"], 1500)
            self.assertEqual(summary["maintenance_margin"], 1000)
            self.assertEqual(summary["unrealized_pnl"], 500)
            self.assertEqual(summary["currency"], "flex")

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_get_portfolio_summary_error(self):
        """get_portfolio_summary should return error on API failure."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {"error": "timeout"}
            summary = mod.KrakenFuturesConnector.get_portfolio_summary()
            self.assertIn("error", summary)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_margin_health_healthy(self):
        """margin_health should return healthy when ratio > minimum."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.side_effect = [
                # get_portfolio_summary
                {"accounts": {"flex": {"type": "multiCollateralMargin",
                                       "pv": 10000, "am": 8000, "im": 1500, "mm": 1000, "pnl": 0}}},
                # get_positions (for can_open_new check)
                {"openPositions": [{"symbol": "PF_XBTUSD", "side": "long", "size": 100}]},
            ]
            health = mod.KrakenFuturesConnector.margin_health()
            self.assertTrue(health["healthy"])
            self.assertEqual(health["ratio"], 10.0)  # 10000/1000 = 10
            self.assertTrue(health["can_open_new"])

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_margin_health_unhealthy(self):
        """margin_health should return unhealthy when ratio < minimum."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.side_effect = [
                {"accounts": {"flex": {"type": "multiCollateralMargin",
                                       "pv": 1500, "am": 100, "im": 1000, "mm": 1000, "pnl": 0}}},
                {"openPositions": []},
            ]
            health = mod.KrakenFuturesConnector.margin_health()
            self.assertFalse(health["healthy"])
            self.assertEqual(health["ratio"], 1.5)  # 1500/1000 = 1.5 < 2.0

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_margin_health_no_positions(self):
        """margin_health with no margin used should be healthy."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.side_effect = [
                {"accounts": {"flex": {"type": "multiCollateralMargin",
                                       "pv": 5000, "am": 5000, "im": 0, "mm": 0, "pnl": 0}}},
                {"openPositions": []},
            ]
            health = mod.KrakenFuturesConnector.margin_health()
            self.assertTrue(health["healthy"])
            self.assertTrue(health["can_open_new"])
            self.assertEqual(health["ratio"], 999.0)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_liquidation_distance_safe(self):
        """liquidation_distance should be safe when far from liquidation."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "openPositions": [{
                    "symbol": "PF_XBTUSD",
                    "side": "long",
                    "size": 100,
                    "price": 50000,
                    "markPrice": 50000,
                    "liquidationThreshold": 10000,
                }]
            }
            liq = mod.KrakenFuturesConnector.liquidation_distance("PF_XBTUSD")
            self.assertTrue(liq["safe"])
            self.assertGreater(liq["distance_pct"], 50)  # 80% away
            self.assertEqual(liq["liq_price"], 10000)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_liquidation_distance_unsafe(self):
        """liquidation_distance should be unsafe when close to liquidation."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "openPositions": [{
                    "symbol": "PF_XBTUSD",
                    "side": "long",
                    "size": 100,
                    "price": 50000,
                    "markPrice": 50000,
                    "liquidationThreshold": 40000,
                }]
            }
            liq = mod.KrakenFuturesConnector.liquidation_distance("PF_XBTUSD")
            self.assertFalse(liq["safe"])
            self.assertLess(liq["distance_pct"], 50)  # only 20% away

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_liquidation_distance_short_position(self):
        """liquidation_distance for short position calculated correctly."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "openPositions": [{
                    "symbol": "PF_XBTUSD",
                    "side": "short",
                    "size": 100,
                    "price": 50000,
                    "markPrice": 50000,
                    "liquidationThreshold": 100000,
                }]
            }
            liq = mod.KrakenFuturesConnector.liquidation_distance("PF_XBTUSD")
            # Short: distance = (liq - mark) / mark = (100000 - 50000) / 50000 = 100%
            self.assertTrue(liq["safe"])
            self.assertGreater(liq["distance_pct"], 50)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_liquidation_distance_no_position(self):
        """liquidation_distance should return safe when no position."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {"openPositions": []}
            liq = mod.KrakenFuturesConnector.liquidation_distance("PF_XBTUSD")
            self.assertTrue(liq["safe"])
            self.assertEqual(liq["distance_pct"], 100)


class TestFuturesFundingRates(unittest.TestCase):
    """Test funding rate retrieval and opportunities."""

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_get_funding_rate_all(self):
        """get_funding_rate without product_id returns all perp rates."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "tickers": [
                    {"symbol": "PF_XBTUSD", "fundingRate": 0.0003, "fundingRatePrediction": 0.0004,
                     "last": 50000, "markPrice": 50100, "vol24h": 1000, "openInterest": 5000},
                    {"symbol": "PF_ETHUSD", "fundingRate": -0.0001, "fundingRatePrediction": -0.0002,
                     "last": 3000, "markPrice": 3010, "vol24h": 500, "openInterest": 2000},
                    {"symbol": "FI_XBTUSD_250328", "last": 51000},  # Not a perp
                ]
            }
            rates = mod.KrakenFuturesConnector.get_funding_rate()
            self.assertIn("PF_XBTUSD", rates)
            self.assertIn("PF_ETHUSD", rates)
            self.assertNotIn("FI_XBTUSD_250328", rates)
            self.assertAlmostEqual(rates["PF_XBTUSD"]["funding_rate"], 0.0003)
            self.assertAlmostEqual(rates["PF_ETHUSD"]["funding_rate"], -0.0001)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_get_funding_rate_specific(self):
        """get_funding_rate with product_id returns single rate."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "tickers": [
                    {"symbol": "PF_XBTUSD", "fundingRate": 0.0003, "last": 50000, "markPrice": 50100,
                     "vol24h": 1000, "openInterest": 5000, "fundingRatePrediction": 0.0004},
                ]
            }
            rate = mod.KrakenFuturesConnector.get_funding_rate("PF_XBTUSD")
            self.assertAlmostEqual(rate["funding_rate"], 0.0003)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_get_funding_rate_not_found(self):
        """get_funding_rate for non-existent product returns error."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {"tickers": []}
            rate = mod.KrakenFuturesConnector.get_funding_rate("PF_XBTUSD")
            self.assertIn("error", rate)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_funding_opportunities(self):
        """funding_opportunities should find high-funding products."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "tickers": [
                    {"symbol": "PF_XBTUSD", "fundingRate": 0.05, "fundingRatePrediction": 0.06,
                     "last": 50000, "markPrice": 50100, "vol24h": 1000, "openInterest": 5000},
                    {"symbol": "PF_ETHUSD", "fundingRate": 0.005, "fundingRatePrediction": 0.005,
                     "last": 3000, "markPrice": 3010, "vol24h": 500, "openInterest": 2000},
                    {"symbol": "PF_SOLUSD", "fundingRate": -0.03, "fundingRatePrediction": -0.03,
                     "last": 100, "markPrice": 101, "vol24h": 200, "openInterest": 800},
                ]
            }
            # Threshold 0.01 = 1%
            opps = mod.KrakenFuturesConnector.funding_opportunities(threshold=0.01)
            self.assertEqual(len(opps), 2)  # BTC (0.05) and SOL (-0.03) above 0.01
            # BTC should be first (highest |rate|)
            self.assertEqual(opps[0]["product_id"], "PF_XBTUSD")
            self.assertEqual(opps[0]["direction"], "SHORT")  # positive funding -> short to collect
            self.assertEqual(opps[1]["product_id"], "PF_SOLUSD")
            self.assertEqual(opps[1]["direction"], "LONG")  # negative funding -> long to collect


class TestFuturesOrderPlacement(unittest.TestCase):
    """Test 4-layer safety gate for order placement."""

    def _mock_healthy_state(self, mod):
        """Return side_effect for _futures_request that simulates healthy state."""
        def side_effect(method, endpoint, data=None):
            if "/accounts" in endpoint:
                return {"accounts": {"flex": {"type": "multiCollateralMargin",
                                              "pv": 10000, "am": 8000, "im": 1500, "mm": 1000, "pnl": 0}}}
            if "/openpositions" in endpoint:
                return {"openPositions": []}
            if "/sendorder" in endpoint:
                return {"sendStatus": {"status": "placed", "order_id": "test_order_123"}}
            return {}
        return side_effect

    def _mock_with_position(self, mod):
        """Return side_effect that simulates existing position close to liq."""
        def side_effect(method, endpoint, data=None):
            if "/accounts" in endpoint:
                return {"accounts": {"flex": {"type": "multiCollateralMargin",
                                              "pv": 10000, "am": 8000, "im": 1500, "mm": 1000, "pnl": 0}}}
            if "/openpositions" in endpoint:
                return {"openPositions": [{
                    "symbol": "PF_XBTUSD",
                    "side": "long",
                    "size": 100,
                    "price": 50000,
                    "markPrice": 50000,
                    "liquidationThreshold": 45000,  # only 10% away -> unsafe
                }]}
            if "/sendorder" in endpoint:
                return {"sendStatus": {"status": "placed", "order_id": "test_order_456"}}
            return {}
        return side_effect

    @patch.dict(os.environ, _FUTURES_DISABLED_ENV)
    def test_layer1_env_gate(self):
        """Layer 1: Order blocked when KRAKEN_FUTURES_ENABLED=0."""
        mod = _import_connector()
        # Force module-level var to disabled
        mod.FUTURES_ENABLED = False
        result = mod.KrakenFuturesConnector.place_futures_order(
            "PF_XBTUSD", "buy", 1, price=50000, confidence=0.85,
        )
        self.assertIn("error", result)
        self.assertIn("not enabled", result["error"])

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_layer2_margin_health(self):
        """Layer 2: Order blocked when margin health too low."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = "test"
        mod.FUTURES_PRIVATE_KEY = "dGVzdA=="
        with patch.object(mod, "_futures_request") as mock_req:
            def side_effect(method, endpoint, data=None):
                if "/accounts" in endpoint:
                    return {"accounts": {"flex": {"type": "multiCollateralMargin",
                                                  "pv": 1200, "am": 100, "im": 800, "mm": 1000, "pnl": 0}}}
                if "/openpositions" in endpoint:
                    return {"openPositions": []}
                return {}
            mock_req.side_effect = side_effect
            result = mod.KrakenFuturesConnector.place_futures_order(
                "PF_XBTUSD", "buy", 1, price=50000, confidence=0.85,
            )
            self.assertIn("error", result)
            self.assertIn("Margin health", result["error"])

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_layer3_liquidation_distance(self):
        """Layer 3: Order blocked when existing position too close to liquidation."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = "test"
        mod.FUTURES_PRIVATE_KEY = "dGVzdA=="
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.side_effect = self._mock_with_position(mod)
            result = mod.KrakenFuturesConnector.place_futures_order(
                "PF_XBTUSD", "buy", 1, price=50000, confidence=0.85,
            )
            self.assertIn("error", result)
            self.assertIn("liquidation", result["error"].lower())

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_layer4_leverage_cap(self):
        """Layer 4: Order blocked when leverage exceeds max."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = "test"
        mod.FUTURES_PRIVATE_KEY = "dGVzdA=="
        mod.MAX_LEVERAGE = 3.0
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.side_effect = self._mock_healthy_state(mod)
            result = mod.KrakenFuturesConnector.place_futures_order(
                "PF_XBTUSD", "buy", 1, price=50000, leverage=5.0, confidence=0.85,
            )
            self.assertIn("error", result)
            self.assertIn("Leverage", result["error"])
            self.assertIn("exceeds", result["error"])

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_goalvalidator_gate(self):
        """GoalValidator blocks order when confidence below threshold."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = "test"
        mod.FUTURES_PRIVATE_KEY = "dGVzdA=="
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.side_effect = self._mock_healthy_state(mod)
            # Confidence 0.30 is below 0.65 threshold
            result = mod.KrakenFuturesConnector.place_futures_order(
                "PF_XBTUSD", "buy", 1, price=50000, confidence=0.30,
            )
            self.assertIn("error", result)
            self.assertIn("GoalValidator blocked", result["error"])

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_max_positions(self):
        """Order blocked when at max positions (caught by margin health gate)."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = "test"
        mod.FUTURES_PRIVATE_KEY = "dGVzdA=="
        mod.MAX_POSITIONS = 2
        with patch.object(mod, "_futures_request") as mock_req:
            def side_effect(method, endpoint, data=None):
                if "/accounts" in endpoint:
                    return {"accounts": {"flex": {"type": "multiCollateralMargin",
                                                  "pv": 10000, "am": 8000, "im": 1500, "mm": 1000, "pnl": 0}}}
                if "/openpositions" in endpoint:
                    return {"openPositions": [
                        {"symbol": "PF_XBTUSD", "side": "long", "size": 100},
                        {"symbol": "PF_ETHUSD", "side": "short", "size": 50},
                    ]}
                return {}
            mock_req.side_effect = side_effect
            result = mod.KrakenFuturesConnector.place_futures_order(
                "PF_SOLUSD", "buy", 1, price=100, confidence=0.85,
            )
            self.assertIn("error", result)
            # Max positions is enforced by margin_health gate (can_open_new=False)
            # The error may come from margin health or explicit max positions check
            error_msg = result["error"].lower()
            self.assertTrue(
                "max positions" in error_msg or "margin health" in error_msg,
                f"Expected position limit error, got: {result['error']}"
            )

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_successful_order(self):
        """All gates pass -> order placed successfully."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = "test"
        mod.FUTURES_PRIVATE_KEY = "dGVzdA=="
        mod.MAX_POSITIONS = 5
        # Use temp DB to avoid pollution
        mod.FUTURES_TRADE_DB = Path(tempfile.mktemp(suffix=".db"))
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.side_effect = self._mock_healthy_state(mod)
            result = mod.KrakenFuturesConnector.place_futures_order(
                "PF_XBTUSD", "buy", 1, price=50000, leverage=2.0, confidence=0.85,
            )
            self.assertIn("sendStatus", result)
            self.assertEqual(result["sendStatus"]["status"], "placed")
            # Verify sendorder was called
            send_calls = [c for c in mock_req.call_args_list if "/sendorder" in str(c)]
            self.assertEqual(len(send_calls), 1)
        # Cleanup
        try:
            mod.FUTURES_TRADE_DB.unlink(missing_ok=True)
        except Exception:
            pass

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_reduce_only_skips_checks(self):
        """Reduce-only orders skip margin, liq, and max position checks."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = "test"
        mod.FUTURES_PRIVATE_KEY = "dGVzdA=="
        mod.FUTURES_TRADE_DB = Path(tempfile.mktemp(suffix=".db"))
        with patch.object(mod, "_futures_request") as mock_req:
            # Even with terrible margin, reduce-only should work
            def side_effect(method, endpoint, data=None):
                if "/sendorder" in endpoint:
                    return {"sendStatus": {"status": "placed", "order_id": "reduce123"}}
                return {}
            mock_req.side_effect = side_effect
            result = mod.KrakenFuturesConnector.place_futures_order(
                "PF_XBTUSD", "sell", 1, price=50000,
                reduce_only=True, confidence=0.85,
            )
            self.assertIn("sendStatus", result)
            # Verify no margin/position calls were made (only sendorder)
            endpoints_called = [str(c) for c in mock_req.call_args_list]
            for ep in endpoints_called:
                self.assertNotIn("/accounts", ep)
                self.assertNotIn("/openpositions", ep)
        try:
            mod.FUTURES_TRADE_DB.unlink(missing_ok=True)
        except Exception:
            pass

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_post_only_default(self):
        """BE A MAKER: post_only flag set by default for limit orders."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = "test"
        mod.FUTURES_PRIVATE_KEY = "dGVzdA=="
        mod.FUTURES_TRADE_DB = Path(tempfile.mktemp(suffix=".db"))
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.side_effect = self._mock_healthy_state(mod)
            mod.KrakenFuturesConnector.place_futures_order(
                "PF_XBTUSD", "buy", 1, price=50000, confidence=0.85,
            )
            # Find the sendorder call and verify postOnly
            for call in mock_req.call_args_list:
                args = call[0]
                if len(args) >= 3 and "/sendorder" in str(args[1]):
                    order_data = args[2]
                    self.assertEqual(order_data.get("postOnly"), "true")
                    self.assertEqual(order_data.get("orderType"), "lmt")
                    break
            else:
                self.fail("sendorder call not found")
        try:
            mod.FUTURES_TRADE_DB.unlink(missing_ok=True)
        except Exception:
            pass

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_market_order_no_post_only(self):
        """Market order (no price) should not have postOnly flag."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = "test"
        mod.FUTURES_PRIVATE_KEY = "dGVzdA=="
        mod.FUTURES_TRADE_DB = Path(tempfile.mktemp(suffix=".db"))
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.side_effect = self._mock_healthy_state(mod)
            mod.KrakenFuturesConnector.place_futures_order(
                "PF_XBTUSD", "buy", 1, price=None, confidence=0.85,
            )
            for call in mock_req.call_args_list:
                args = call[0]
                if len(args) >= 3 and "/sendorder" in str(args[1]):
                    order_data = args[2]
                    self.assertNotIn("postOnly", order_data)
                    self.assertEqual(order_data.get("orderType"), "mkt")
                    break
        try:
            mod.FUTURES_TRADE_DB.unlink(missing_ok=True)
        except Exception:
            pass


class TestFuturesBasis(unittest.TestCase):
    """Test basis opportunity scanning."""

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_scan_basis_opportunities_with_spot_prices(self):
        """scan_basis_opportunities should find basis when premium exists."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "tickers": [
                    {"symbol": "PF_XBTUSD", "fundingRate": 0.001, "fundingRatePrediction": 0.001,
                     "last": 50500, "markPrice": 50500, "vol24h": 1000, "openInterest": 5000},
                    {"symbol": "PF_ETHUSD", "fundingRate": 0.0005, "fundingRatePrediction": 0.0005,
                     "last": 3000, "markPrice": 3000, "vol24h": 500, "openInterest": 2000},
                ]
            }
            # BTC spot = 50000, futures = 50500 -> 1% basis
            # ETH spot = 2990, futures = 3000 -> 0.33% basis
            spot_prices = {"BTC": 50000, "ETH": 2990}
            opps = mod.KrakenFuturesConnector.scan_basis_opportunities(spot_prices=spot_prices)
            self.assertGreater(len(opps), 0)
            # BTC should have higher basis
            btc_opp = next((o for o in opps if o["base"] == "BTC"), None)
            self.assertIsNotNone(btc_opp)
            self.assertAlmostEqual(btc_opp["basis_pct"], 1.0, places=1)
            self.assertEqual(btc_opp["direction"], "short_futures")
            self.assertEqual(btc_opp["venue"], "kraken")

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_scan_basis_no_premium(self):
        """scan_basis_opportunities should return empty when no premium."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "tickers": [
                    {"symbol": "PF_XBTUSD", "fundingRate": 0, "fundingRatePrediction": 0,
                     "last": 50000, "markPrice": 50000, "vol24h": 1000, "openInterest": 5000},
                ]
            }
            spot_prices = {"BTC": 50000}  # Same as futures -> no basis
            opps = mod.KrakenFuturesConnector.scan_basis_opportunities(spot_prices=spot_prices)
            self.assertEqual(len(opps), 0)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_scan_basis_discount(self):
        """scan_basis_opportunities should detect futures discount."""
        mod = _import_connector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.return_value = {
                "tickers": [
                    {"symbol": "PF_XBTUSD", "fundingRate": -0.001, "fundingRatePrediction": -0.001,
                     "last": 49000, "markPrice": 49000, "vol24h": 1000, "openInterest": 5000},
                ]
            }
            spot_prices = {"BTC": 50000}  # Futures at discount
            opps = mod.KrakenFuturesConnector.scan_basis_opportunities(spot_prices=spot_prices)
            self.assertGreater(len(opps), 0)
            self.assertEqual(opps[0]["direction"], "long_futures")  # discount -> go long futures


class TestFuturesTradeDB(unittest.TestCase):
    """Test trade database recording."""

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_init_trade_db(self):
        """Trade DB should initialize with correct schema."""
        mod = _import_connector()
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            mod.FUTURES_TRADE_DB = Path(f.name)

        try:
            db = mod._init_futures_trade_db()
            # Verify table exists
            cursor = db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='kraken_futures_trades'"
            )
            tables = cursor.fetchall()
            self.assertEqual(len(tables), 1)
            db.close()
        finally:
            Path(f.name).unlink(missing_ok=True)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_record_trade_success(self):
        """_record_futures_trade should insert row on successful order."""
        mod = _import_connector()
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            mod.FUTURES_TRADE_DB = Path(f.name)

        try:
            result = {"sendStatus": {"status": "placed", "order_id": "abc123"}}
            mod._record_futures_trade(
                "PF_XBTUSD", "buy", 1.0, 50000.0, result, 0.85, 2.0
            )

            db = sqlite3.connect(str(mod.FUTURES_TRADE_DB))
            rows = db.execute("SELECT * FROM kraken_futures_trades").fetchall()
            self.assertEqual(len(rows), 1)
            # Check columns: id, product_id, side, size, price, leverage, order_id, status, confidence, created_at
            self.assertEqual(rows[0][1], "PF_XBTUSD")  # product_id
            self.assertEqual(rows[0][2], "buy")  # side
            self.assertEqual(rows[0][3], 1.0)  # size
            self.assertEqual(rows[0][4], 50000.0)  # price
            self.assertEqual(rows[0][5], 2.0)  # leverage
            self.assertEqual(rows[0][6], "abc123")  # order_id
            self.assertEqual(rows[0][7], "placed")  # status
            self.assertEqual(rows[0][8], 0.85)  # confidence
            db.close()
        finally:
            Path(f.name).unlink(missing_ok=True)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_record_trade_error(self):
        """_record_futures_trade should record error status on failure."""
        mod = _import_connector()
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            mod.FUTURES_TRADE_DB = Path(f.name)

        try:
            result = {"error": "Connection timeout"}
            mod._record_futures_trade(
                "PF_XBTUSD", "sell", 2.0, 49000.0, result, 0.75, 1.0
            )

            db = sqlite3.connect(str(mod.FUTURES_TRADE_DB))
            rows = db.execute("SELECT * FROM kraken_futures_trades").fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][7], "error")  # status
            db.close()
        finally:
            Path(f.name).unlink(missing_ok=True)


class TestConnectorEnabled(unittest.TestCase):
    """Test the enabled property."""

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_enabled_with_all_set(self):
        """Connector should be enabled with env var and keys."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = "test"
        mod.FUTURES_PRIVATE_KEY = "test"
        conn = mod.KrakenFuturesConnector()
        self.assertTrue(conn.enabled)

    @patch.dict(os.environ, _FUTURES_DISABLED_ENV)
    def test_disabled_without_env(self):
        """Connector should be disabled when KRAKEN_FUTURES_ENABLED=0."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = False
        conn = mod.KrakenFuturesConnector()
        self.assertFalse(conn.enabled)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_disabled_without_keys(self):
        """Connector should be disabled without API keys."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = ""
        mod.FUTURES_PRIVATE_KEY = ""
        conn = mod.KrakenFuturesConnector()
        self.assertFalse(conn.enabled)


class TestStatusSummary(unittest.TestCase):
    """Test status summary method."""

    @patch.dict(os.environ, _FUTURES_DISABLED_ENV)
    def test_status_when_disabled(self):
        """Status should show disabled reason."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = False
        conn = mod.KrakenFuturesConnector()
        status = conn.status_summary()
        self.assertFalse(status["enabled"])
        self.assertIn("reason", status)

    @patch.dict(os.environ, _FUTURES_ENV)
    def test_status_when_enabled(self):
        """Status should include margin health and positions."""
        mod = _import_connector()
        mod.FUTURES_ENABLED = True
        mod.FUTURES_API_KEY = "test"
        mod.FUTURES_PRIVATE_KEY = "test"
        conn = mod.KrakenFuturesConnector()
        with patch.object(mod, "_futures_request") as mock_req:
            mock_req.side_effect = [
                # margin_health -> get_portfolio_summary
                {"accounts": {"flex": {"type": "multiCollateralMargin",
                                       "pv": 5000, "am": 4000, "im": 500, "mm": 300, "pnl": 0}}},
                # margin_health -> get_positions
                {"openPositions": []},
                # status_summary -> get_positions
                {"openPositions": []},
                # status_summary -> get_open_orders
                {"openOrders": []},
            ]
            status = conn.status_summary()
            self.assertTrue(status["enabled"])
            self.assertIn("margin_health", status)
            self.assertEqual(status["positions"], 0)
            self.assertEqual(status["open_orders"], 0)


if __name__ == "__main__":
    unittest.main()
