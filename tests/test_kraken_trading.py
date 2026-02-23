#!/usr/bin/env python3
"""Tests for Kraken trading connector and signal agent.

Tests:
  1. Private request signing
  2. GoalValidator trade gating
  3. Limit order defaults (BE A MAKER)
  4. Cancel order
  5. Account balance
  6. Orderbook imbalance signal
  7. Volume anomaly signal
  8. Cross-venue divergence signal
  9. Trade DB creation
  10. No keys = no trading
"""

import os
import sys
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock

# Add agents/ to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))


class TestKrakenPrivateRequest(unittest.TestCase):
    """Test _private_request signs and sends correctly."""

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")  # base64("test_private_key")
    @patch("kraken_connector.urllib.request.urlopen")
    def test_private_request_signs_correctly(self, mock_urlopen):
        """Verify _sign_request is called with correct endpoint and data."""
        from kraken_connector import KrakenConnector, _sign_request

        # Mock response
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"error":[],"result":{"balance":"1000"}}'
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        with patch("kraken_connector._sign_request", wraps=_sign_request) as mock_sign:
            result = KrakenConnector._private_request("Balance", {"asset": "ZUSD"})

            # _sign_request should have been called
            mock_sign.assert_called_once()
            call_args = mock_sign.call_args
            # First arg is the URL path
            self.assertEqual(call_args[0][0], "/0/private/Balance")
            # Data dict should include nonce and our param
            data = call_args[0][1]
            self.assertIn("nonce", data)
            self.assertEqual(data["asset"], "ZUSD")

        # Should have made a POST request
        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.method, "POST")
        self.assertIn("api.kraken.com", req.full_url)


class TestKrakenPlaceOrder(unittest.TestCase):
    """Test place_order with GoalValidator gating."""

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    def test_place_order_goal_gate(self):
        """GoalValidator blocks low-confidence orders."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = False

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="BTC-USD", side="buy", volume=0.001,
                order_type="limit", price=50000.0, confidence=0.50,
            )

        self.assertIn("error", result)
        self.assertIn("GoalValidator blocked", result["error"])
        mock_gv.should_trade.assert_called_once_with(0.50, 2, "BUY", "neutral")

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    @patch("kraken_connector._init_trade_db")
    def test_place_order_limit_default(self, mock_db, mock_private):
        """Default order type is limit (maker), not market."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = True

        mock_private.return_value = {"error": [], "result": {"txid": ["OABCDE-FGHIJ-KLMNOP"]}}
        mock_conn = MagicMock()
        mock_db.return_value = mock_conn

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="ETH-USD", side="buy", volume=1.0,
                price=3000.0, confidence=0.85,
            )

        # Should call AddOrder with ordertype=limit
        mock_private.assert_called_once()
        call_data = mock_private.call_args[0][1]
        self.assertEqual(call_data["ordertype"], "limit")
        self.assertEqual(call_data["price"], "3000.0")
        self.assertEqual(call_data["type"], "buy")

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    def test_place_order_limit_requires_price(self):
        """Limit order without price returns error."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = True

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="BTC-USD", side="buy", volume=0.01,
                order_type="limit", price=None, confidence=0.80,
            )

        self.assertIn("error", result)
        self.assertIn("price", result["error"])


class TestKrakenCancelOrder(unittest.TestCase):
    """Test cancel_order calls CancelOrder endpoint."""

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    @patch("kraken_connector._init_trade_db")
    def test_cancel_order(self, mock_db, mock_private):
        """cancel_order calls CancelOrder with correct txid."""
        from kraken_connector import KrakenConnector

        mock_private.return_value = {"error": [], "result": {"count": 1}}
        mock_conn = MagicMock()
        mock_db.return_value = mock_conn

        result = KrakenConnector.cancel_order("OABCDE-FGHIJ-KLMNOP")

        mock_private.assert_called_once_with("CancelOrder", {"txid": "OABCDE-FGHIJ-KLMNOP"})
        self.assertEqual(result["result"]["count"], 1)


class TestKrakenAccountBalance(unittest.TestCase):
    """Test get_account_balance calls Balance endpoint."""

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    def test_get_account_balance(self, mock_private):
        """get_account_balance calls Balance endpoint."""
        from kraken_connector import KrakenConnector

        mock_private.return_value = {
            "error": [],
            "result": {"ZUSD": "10000.00", "XXBT": "0.5"}
        }

        result = KrakenConnector.get_account_balance()

        mock_private.assert_called_once_with("Balance")
        self.assertEqual(result["result"]["ZUSD"], "10000.00")


class TestOrderbookImbalanceSignal(unittest.TestCase):
    """Test orderbook imbalance signal generation."""

    def test_orderbook_imbalance_buy_signal(self):
        """2:1 bid/ask ratio emits BUY signal."""
        from kraken_signal_agent import KrakenSignalAgent

        agent = KrakenSignalAgent()

        # Mock orderbook with 2:1 bid/ask ratio
        mock_book = {
            "pair": "BTC-USD",
            "timestamp": "2026-02-23T00:00:00Z",
            "bids": [["50000", "2.0", "1234567890"]] * 10,  # 20.0 total
            "asks": [["50100", "1.0", "1234567890"]] * 10,  # 10.0 total
        }

        with patch("kraken_signal_agent.KrakenConnector") as mock_kc:
            mock_kc.get_orderbook.return_value = mock_book
            signal = agent.scan_orderbook_imbalance("BTC-USD")

        self.assertIsNotNone(signal)
        self.assertEqual(signal["direction"], "BUY")
        self.assertGreaterEqual(signal["confidence"], 0.70)
        self.assertIn("imbalance", signal["reasoning"].lower())

    def test_orderbook_imbalance_sell_signal(self):
        """1:3 bid/ask ratio emits SELL signal."""
        from kraken_signal_agent import KrakenSignalAgent

        agent = KrakenSignalAgent()

        # Mock orderbook with 1:3 bid/ask ratio (0.33)
        mock_book = {
            "pair": "ETH-USD",
            "timestamp": "2026-02-23T00:00:00Z",
            "bids": [["3000", "1.0", "1234567890"]] * 10,   # 10.0 total
            "asks": [["3010", "3.0", "1234567890"]] * 10,   # 30.0 total
        }

        with patch("kraken_signal_agent.KrakenConnector") as mock_kc:
            mock_kc.get_orderbook.return_value = mock_book
            signal = agent.scan_orderbook_imbalance("ETH-USD")

        self.assertIsNotNone(signal)
        self.assertEqual(signal["direction"], "SELL")
        self.assertGreaterEqual(signal["confidence"], 0.70)


class TestVolumeAnomalySignal(unittest.TestCase):
    """Test volume anomaly signal generation."""

    def test_volume_anomaly_signal(self):
        """3x volume emits momentum signal."""
        from kraken_signal_agent import KrakenSignalAgent, VOLUME_7D_AVERAGES

        agent = KrakenSignalAgent()

        avg = VOLUME_7D_AVERAGES["BTC-USD"]

        mock_vol = {
            "pair": "BTC-USD",
            "timestamp": "2026-02-23T00:00:00Z",
            "volume_24h": avg * 3.0,  # 3x average
            "price_24h_high": 51000.0,
            "price_24h_low": 49000.0,
            "last_price": 50500.0,  # Above mid (50000) = BUY
        }

        with patch("kraken_signal_agent.KrakenConnector") as mock_kc:
            mock_kc.get_24h_volume.return_value = mock_vol
            signal = agent.scan_volume_anomaly("BTC-USD")

        self.assertIsNotNone(signal)
        self.assertEqual(signal["direction"], "BUY")
        self.assertGreaterEqual(signal["confidence"], 0.70)
        self.assertIn("3.0x", signal["reasoning"])

    def test_volume_normal_no_signal(self):
        """Normal volume (1x average) emits no signal."""
        from kraken_signal_agent import KrakenSignalAgent, VOLUME_7D_AVERAGES

        agent = KrakenSignalAgent()
        avg = VOLUME_7D_AVERAGES["BTC-USD"]

        mock_vol = {
            "pair": "BTC-USD",
            "timestamp": "2026-02-23T00:00:00Z",
            "volume_24h": avg * 1.0,
            "price_24h_high": 51000.0,
            "price_24h_low": 49000.0,
            "last_price": 50000.0,
        }

        with patch("kraken_signal_agent.KrakenConnector") as mock_kc:
            mock_kc.get_24h_volume.return_value = mock_vol
            signal = agent.scan_volume_anomaly("BTC-USD")

        self.assertIsNone(signal)


class TestCrossVenueDivergence(unittest.TestCase):
    """Test cross-venue divergence signal generation."""

    def test_cross_venue_divergence_signal(self):
        """0.5% price gap (after fees) emits arb signal."""
        from kraken_signal_agent import KrakenSignalAgent

        agent = KrakenSignalAgent()

        # Kraken price $50000, Coinbase $51000 = ~2% raw gap
        # After fees (0.26% + 0.80% = 1.06%), net gap = ~0.94% > 0.3%
        mock_vol = {
            "pair": "BTC-USD",
            "timestamp": "2026-02-23T00:00:00Z",
            "volume_24h": 5000.0,
            "price_24h_high": 51000.0,
            "price_24h_low": 49000.0,
            "last_price": 50000.0,  # Kraken price
        }

        with patch("kraken_signal_agent.KrakenConnector") as mock_kc, \
             patch("kraken_signal_agent._fetch_coinbase_price") as mock_cb:
            mock_kc.get_24h_volume.return_value = mock_vol
            mock_cb.return_value = 51000.0  # Coinbase price

            signal = agent.scan_cross_venue_divergence("BTC-USD")

        self.assertIsNotNone(signal)
        self.assertEqual(signal["direction"], "BUY")  # Buy on Kraken (cheaper)
        self.assertEqual(signal["venue_hint"], "kraken")
        self.assertGreaterEqual(signal["confidence"], 0.75)
        self.assertIn("divergence", signal["reasoning"].lower())

    def test_cross_venue_no_divergence(self):
        """Prices within fee threshold emit no signal."""
        from kraken_signal_agent import KrakenSignalAgent

        agent = KrakenSignalAgent()

        # Kraken $50000, Coinbase $50050 = 0.1% gap, below fee threshold
        mock_vol = {
            "pair": "BTC-USD",
            "timestamp": "2026-02-23T00:00:00Z",
            "volume_24h": 5000.0,
            "price_24h_high": 51000.0,
            "price_24h_low": 49000.0,
            "last_price": 50000.0,
        }

        with patch("kraken_signal_agent.KrakenConnector") as mock_kc, \
             patch("kraken_signal_agent._fetch_coinbase_price") as mock_cb:
            mock_kc.get_24h_volume.return_value = mock_vol
            mock_cb.return_value = 50050.0  # Only 0.1% gap

            signal = agent.scan_cross_venue_divergence("BTC-USD")

        self.assertIsNone(signal)


class TestTradeDBCreation(unittest.TestCase):
    """Test trade database initialization."""

    def test_trade_db_creation(self):
        """DB and kraken_trades table are created."""
        from kraken_connector import _init_trade_db

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            with patch("kraken_connector.KRAKEN_TRADE_DB", Path(tmp_path)):
                db = _init_trade_db()

                # Verify table exists
                cursor = db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='kraken_trades'"
                )
                tables = cursor.fetchall()
                self.assertEqual(len(tables), 1)
                self.assertEqual(tables[0][0], "kraken_trades")

                # Verify columns
                cursor = db.execute("PRAGMA table_info(kraken_trades)")
                columns = {row[1] for row in cursor.fetchall()}
                expected = {"id", "pair", "side", "volume", "price", "order_type",
                            "txid", "status", "confidence", "created_at"}
                self.assertEqual(columns, expected)

                db.close()
        finally:
            os.unlink(tmp_path)


class TestNoKeysNoTrading(unittest.TestCase):
    """Test that trading methods fail gracefully without API keys."""

    @patch("kraken_connector.API_KEY", "")
    @patch("kraken_connector.PRIVATE_KEY", "")
    def test_no_keys_no_trading(self):
        """Trading methods return error without API keys."""
        from kraken_connector import KrakenConnector

        # _private_request should return error
        result = KrakenConnector._private_request("Balance")
        self.assertIn("error", result)
        self.assertTrue(len(result["error"]) > 0)
        self.assertIn("not configured", result["error"][0])

        # get_account_balance calls _private_request
        result = KrakenConnector.get_account_balance()
        self.assertIn("error", result)

        # get_open_orders
        result = KrakenConnector.get_open_orders()
        self.assertIn("error", result)

        # get_trade_balance
        result = KrakenConnector.get_trade_balance()
        self.assertIn("error", result)


class TestAdvancedOrderTypes(unittest.TestCase):
    """Test advanced Kraken order types."""

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    @patch("kraken_connector._init_trade_db")
    def test_stop_loss_order(self, mock_db, mock_req):
        """Stop-loss order sends correct params to Kraken API."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = True
        mock_req.return_value = {"result": {"txid": ["STOP-123"]}}
        mock_db.return_value = MagicMock()

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="BTC-USD", side="sell", volume=0.001,
                order_type="stop-loss", price=95000.0,
                confidence=0.80
            )
        call_data = mock_req.call_args[0][1]
        self.assertEqual(call_data["ordertype"], "stop-loss")
        self.assertEqual(call_data["price"], "95000.0")

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    @patch("kraken_connector._init_trade_db")
    def test_stop_loss_limit_order(self, mock_db, mock_req):
        """Stop-loss-limit order maps price and price2 correctly."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = True
        mock_req.return_value = {"result": {"txid": ["SLL-123"]}}
        mock_db.return_value = MagicMock()

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="BTC-USD", side="sell", volume=0.001,
                order_type="stop-loss-limit", price=94500.0, stop_price=95000.0,
                confidence=0.80
            )
        call_data = mock_req.call_args[0][1]
        self.assertEqual(call_data["ordertype"], "stop-loss-limit")
        self.assertEqual(call_data["price"], "94500.0")
        self.assertEqual(call_data["price2"], "95000.0")

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    @patch("kraken_connector._init_trade_db")
    def test_take_profit_order(self, mock_db, mock_req):
        """Take-profit order sends correct params."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = True
        mock_req.return_value = {"result": {"txid": ["TP-123"]}}
        mock_db.return_value = MagicMock()

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="BTC-USD", side="sell", volume=0.001,
                order_type="take-profit", price=105000.0,
                confidence=0.80
            )
        call_data = mock_req.call_args[0][1]
        self.assertEqual(call_data["ordertype"], "take-profit")
        self.assertEqual(call_data["price"], "105000.0")

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    @patch("kraken_connector._init_trade_db")
    def test_trailing_stop_order(self, mock_db, mock_req):
        """Trailing-stop order maps trailing_offset to price field."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = True
        mock_req.return_value = {"result": {"txid": ["TS-123"]}}
        mock_db.return_value = MagicMock()

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="BTC-USD", side="sell", volume=0.001,
                order_type="trailing-stop", trailing_offset=500.0,
                confidence=0.80
            )
        call_data = mock_req.call_args[0][1]
        self.assertEqual(call_data["ordertype"], "trailing-stop")
        self.assertEqual(call_data["price"], "500.0")

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    @patch("kraken_connector._init_trade_db")
    def test_iceberg_order(self, mock_db, mock_req):
        """Iceberg order includes displayvol."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = True
        mock_req.return_value = {"result": {"txid": ["ICE-123"]}}
        mock_db.return_value = MagicMock()

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="BTC-USD", side="buy", volume=1.0,
                order_type="limit", price=95000.0,
                visible_volume=0.1, confidence=0.80
            )
        call_data = mock_req.call_args[0][1]
        self.assertEqual(call_data["displayvol"], "0.1")

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    @patch("kraken_connector._init_trade_db")
    def test_conditional_close_order(self, mock_db, mock_req):
        """Conditional close order maps to close[ordertype], close[price], close[price2]."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = True
        mock_req.return_value = {"result": {"txid": ["CC-123"]}}
        mock_db.return_value = MagicMock()

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="BTC-USD", side="buy", volume=0.001,
                order_type="limit", price=95000.0,
                close_order={"ordertype": "stop-loss-limit", "price": "93000.0", "price2": "92500.0"},
                confidence=0.80
            )
        call_data = mock_req.call_args[0][1]
        self.assertEqual(call_data["close[ordertype]"], "stop-loss-limit")
        self.assertEqual(call_data["close[price]"], "93000.0")
        self.assertEqual(call_data["close[price2]"], "92500.0")

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    @patch("kraken_connector._init_trade_db")
    def test_post_only_flag(self, mock_db, mock_req):
        """oflags='post' is passed to API."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = True
        mock_req.return_value = {"result": {"txid": ["PO-123"]}}
        mock_db.return_value = MagicMock()

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="BTC-USD", side="buy", volume=0.001,
                order_type="limit", price=95000.0,
                oflags="post", confidence=0.80
            )
        call_data = mock_req.call_args[0][1]
        self.assertEqual(call_data["oflags"], "post")

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    @patch("kraken_connector._init_trade_db")
    def test_ioc_time_in_force(self, mock_db, mock_req):
        """timeinforce='IOC' is passed to API."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = True
        mock_req.return_value = {"result": {"txid": ["IOC-123"]}}
        mock_db.return_value = MagicMock()

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="BTC-USD", side="buy", volume=0.001,
                order_type="limit", price=95000.0,
                timeinforce="IOC", confidence=0.80
            )
        call_data = mock_req.call_args[0][1]
        self.assertEqual(call_data["timeinforce"], "IOC")

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    @patch("kraken_connector._init_trade_db")
    def test_take_profit_limit_order(self, mock_db, mock_req):
        """Take-profit-limit maps price (limit) and price2 (trigger)."""
        from kraken_connector import KrakenConnector

        mock_gv = MagicMock()
        mock_gv.should_trade.return_value = True
        mock_req.return_value = {"result": {"txid": ["TPL-123"]}}
        mock_db.return_value = MagicMock()

        with patch("kraken_connector.GoalValidator", mock_gv):
            result = KrakenConnector.place_order(
                pair="ETH-USD", side="sell", volume=1.0,
                order_type="take-profit-limit", price=4000.0, stop_price=4100.0,
                confidence=0.85
            )
        call_data = mock_req.call_args[0][1]
        self.assertEqual(call_data["ordertype"], "take-profit-limit")
        self.assertEqual(call_data["price"], "4000.0")
        self.assertEqual(call_data["price2"], "4100.0")


class TestDynamicFees(unittest.TestCase):
    """Test get_fee_schedule method."""

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    def test_get_fee_schedule(self, mock_req):
        """Fee schedule returns parsed maker/taker fees."""
        from kraken_connector import KrakenConnector

        mock_req.return_value = {
            "error": [],
            "result": {
                "currency": "ZUSD",
                "volume": "500.00",
                "fees": {"XXBTZUSD": {"fee": "0.2600"}},
                "fees_maker": {"XXBTZUSD": {"fee": "0.1600"}}
            }
        }
        result = KrakenConnector.get_fee_schedule("BTC-USD")
        self.assertIn("maker_fee", result)
        self.assertIn("taker_fee", result)
        self.assertEqual(result["maker_fee"], 0.16)
        self.assertEqual(result["taker_fee"], 0.26)
        self.assertEqual(result["volume_30d"], 500.0)

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    def test_fee_schedule_high_volume_tier(self, mock_req):
        """Higher volume tier returns lower fees."""
        from kraken_connector import KrakenConnector

        mock_req.return_value = {
            "error": [],
            "result": {
                "currency": "ZUSD",
                "volume": "250000.00",
                "fees": {"XXBTZUSD": {"fee": "0.2000"}},
                "fees_maker": {"XXBTZUSD": {"fee": "0.1200"}}
            }
        }
        result = KrakenConnector.get_fee_schedule("BTC-USD")
        self.assertEqual(result["maker_fee"], 0.12)
        self.assertEqual(result["taker_fee"], 0.20)
        self.assertEqual(result["volume_30d"], 250000.0)

    @patch("kraken_connector.API_KEY", "")
    @patch("kraken_connector.PRIVATE_KEY", "")
    def test_fee_schedule_no_keys(self):
        """Fee schedule without API keys returns error."""
        from kraken_connector import KrakenConnector

        result = KrakenConnector.get_fee_schedule()
        self.assertIn("error", result)

    @patch("kraken_connector.API_KEY", "test_key")
    @patch("kraken_connector.PRIVATE_KEY", "dGVzdF9wcml2YXRlX2tleQ==")
    @patch("kraken_connector.KrakenConnector._private_request")
    def test_fee_schedule_no_pair(self, mock_req):
        """Fee schedule without pair calls TradeVolume with no pair param."""
        from kraken_connector import KrakenConnector

        mock_req.return_value = {
            "error": [],
            "result": {
                "currency": "ZUSD",
                "volume": "100.00",
                "fees": {},
                "fees_maker": {}
            }
        }
        result = KrakenConnector.get_fee_schedule()
        # Should still call _private_request with "TradeVolume"
        mock_req.assert_called_once()
        call_args = mock_req.call_args
        self.assertEqual(call_args[0][0], "TradeVolume")
        # With empty fees, should fall back to defaults
        self.assertEqual(result["maker_fee"], 0.16)
        self.assertEqual(result["taker_fee"], 0.26)


if __name__ == "__main__":
    unittest.main()
