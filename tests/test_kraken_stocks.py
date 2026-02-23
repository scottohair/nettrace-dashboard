#!/usr/bin/env python3
"""Tests for Kraken Stock/Equity Trading Connector.

All API calls are mocked — no real network access.
"""

import datetime
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

# Ensure agents/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "agents"))


# ── Helper: build a mock urllib response ──
def _mock_urlopen(data_dict, status=200):
    """Create a mock context manager for urllib.request.urlopen."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(data_dict).encode()
    mock_resp.status = status
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


# ── Sample Kraken API responses ──
SAMPLE_TICKER_RESPONSE = {
    "error": [],
    "result": {
        "AAPLUSD": {
            "a": ["185.50", "1", "1.000"],       # ask
            "b": ["185.40", "1", "1.000"],       # bid
            "c": ["185.45", "10.000"],            # last trade
            "v": ["50000.000", "120000.000"],     # volume [today, 24h]
            "p": ["185.00", "184.50"],            # vwap
            "t": [1000, 2500],                    # trades
            "l": ["183.00", "182.50"],            # low [today, 24h]
            "h": ["186.00", "187.00"],            # high [today, 24h]
            "o": "184.00",                        # open
        }
    },
}

SAMPLE_ORDERBOOK_RESPONSE = {
    "error": [],
    "result": {
        "AAPLUSD": {
            "asks": [
                ["185.50", "100", 1700000000],
                ["185.60", "50", 1700000001],
                ["185.70", "200", 1700000002],
            ],
            "bids": [
                ["185.40", "80", 1700000000],
                ["185.30", "150", 1700000001],
                ["185.20", "90", 1700000002],
            ],
        }
    },
}

SAMPLE_ASSET_PAIRS_RESPONSE = {
    "error": [],
    "result": {
        "AAPLUSD": {
            "altname": "AAPLUSD",
            "wsname": "AAPL/USD",
            "aclass_base": "equity",
            "base": "AAPL",
            "aclass_quote": "currency",
            "quote": "ZUSD",
            "lot": "unit",
            "pair_decimals": 2,
            "lot_decimals": 4,
            "lot_multiplier": 1,
        },
        "SPYUSD": {
            "altname": "SPYUSD",
            "wsname": "SPY/USD",
            "aclass_base": "equity",
            "base": "SPY",
            "aclass_quote": "currency",
            "quote": "ZUSD",
            "lot": "unit",
            "pair_decimals": 2,
            "lot_decimals": 4,
            "lot_multiplier": 1,
        },
        "COINUSD": {
            "altname": "COINUSD",
            "wsname": "COIN/USD",
            "aclass_base": "equity",
            "base": "COIN",
            "aclass_quote": "currency",
            "quote": "ZUSD",
            "lot": "unit",
            "pair_decimals": 2,
            "lot_decimals": 4,
            "lot_multiplier": 1,
        },
        "XXBTZUSD": {
            "altname": "XBTUSD",
            "wsname": "XBT/USD",
            "aclass_base": "currency",
            "base": "XXBT",
            "aclass_quote": "currency",
            "quote": "ZUSD",
            "lot": "unit",
            "pair_decimals": 1,
            "lot_decimals": 8,
            "lot_multiplier": 1,
        },
    },
}

SAMPLE_ADD_ORDER_RESPONSE = {
    "error": [],
    "result": {
        "descr": {"order": "buy 10.0000 AAPL/USD @ limit 185.00"},
        "txid": ["OQCLML-BW3P3-BUCMWZ"],
    },
}

SAMPLE_CANCEL_RESPONSE = {
    "error": [],
    "result": {"count": 1},
}

SAMPLE_BALANCE_RESPONSE = {
    "error": [],
    "result": {
        "AAPL": "10.5000",
        "SPY": "5.0000",
        "ZUSD": "1000.00",
        "XXBT": "0.01000000",
    },
}


class TestIsMarketOpen(unittest.TestCase):
    """Tests for is_market_open()."""

    @patch("kraken_stock_connector.ZoneInfo")
    def test_is_market_open_weekday_during_hours(self, mock_zi):
        """Market should be open during regular hours on a weekday."""
        from kraken_stock_connector import is_market_open

        # Monday at 10:00 AM ET
        mock_now = datetime.datetime(2025, 6, 2, 10, 0, 0)  # Monday
        mock_zi.return_value = "America/New_York"

        with patch("kraken_stock_connector.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = mock_now
            mock_dt.time = datetime.time
            mock_dt.timedelta = datetime.timedelta
            mock_dt.timezone = datetime.timezone
            result = is_market_open()

        self.assertTrue(result)

    @patch("kraken_stock_connector.ZoneInfo")
    def test_is_market_open_weekend(self, mock_zi):
        """Market should be closed on weekends."""
        from kraken_stock_connector import is_market_open

        # Saturday at 10:00 AM ET
        mock_now = datetime.datetime(2025, 6, 7, 10, 0, 0)  # Saturday
        mock_zi.return_value = "America/New_York"

        with patch("kraken_stock_connector.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = mock_now
            mock_dt.time = datetime.time
            mock_dt.timedelta = datetime.timedelta
            mock_dt.timezone = datetime.timezone
            result = is_market_open()

        self.assertFalse(result)

    @patch("kraken_stock_connector.ZoneInfo")
    def test_is_market_open_after_hours_extended(self, mock_zi):
        """Extended hours should include pre-market and after-hours."""
        from kraken_stock_connector import is_market_open

        # Monday at 5:00 AM ET (pre-market)
        mock_now = datetime.datetime(2025, 6, 2, 5, 0, 0)  # Monday
        mock_zi.return_value = "America/New_York"

        with patch("kraken_stock_connector.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = mock_now
            mock_dt.time = datetime.time
            mock_dt.timedelta = datetime.timedelta
            mock_dt.timezone = datetime.timezone

            # Regular hours: closed (5 AM < 9:30 AM)
            self.assertFalse(is_market_open(extended_hours=False))
            # Extended hours: open (5 AM is between 4 AM and 8 PM)
            self.assertTrue(is_market_open(extended_hours=True))


class TestGetStockQuote(unittest.TestCase):
    """Tests for KrakenStockConnector.get_stock_quote()."""

    @patch("kraken_stock_connector.urllib.request.urlopen")
    @patch("kraken_stock_connector._discover_stock_pairs")
    def test_get_stock_quote_success(self, mock_discover, mock_urlopen):
        """Successful stock quote returns parsed ticker data."""
        from kraken_stock_connector import KrakenStockConnector

        mock_discover.return_value = {"AAPL": "AAPLUSD"}
        mock_urlopen.return_value = _mock_urlopen(SAMPLE_TICKER_RESPONSE)

        result = KrakenStockConnector.get_stock_quote("AAPL")

        self.assertNotIn("error", result)
        self.assertEqual(result["symbol"], "AAPL")
        self.assertAlmostEqual(result["last_price"], 185.45)
        self.assertAlmostEqual(result["bid"], 185.40)
        self.assertAlmostEqual(result["ask"], 185.50)
        self.assertAlmostEqual(result["spread"], 0.10)
        self.assertAlmostEqual(result["volume_24h"], 120000.0)

    @patch("kraken_stock_connector._discover_stock_pairs")
    def test_get_stock_quote_not_found(self, mock_discover):
        """Quote for unknown stock returns error."""
        from kraken_stock_connector import KrakenStockConnector

        mock_discover.return_value = {}

        result = KrakenStockConnector.get_stock_quote("ZZZZZ")
        self.assertIn("error", result)
        self.assertIn("not found", result["error"])


class TestGetStockOrderbook(unittest.TestCase):
    """Tests for KrakenStockConnector.get_stock_orderbook()."""

    @patch("kraken_stock_connector.urllib.request.urlopen")
    @patch("kraken_stock_connector._discover_stock_pairs")
    def test_get_stock_orderbook_success(self, mock_discover, mock_urlopen):
        """Orderbook returns asks and bids."""
        from kraken_stock_connector import KrakenStockConnector

        mock_discover.return_value = {"AAPL": "AAPLUSD"}
        mock_urlopen.return_value = _mock_urlopen(SAMPLE_ORDERBOOK_RESPONSE)

        result = KrakenStockConnector.get_stock_orderbook("AAPL", depth=3)

        self.assertNotIn("error", result)
        self.assertEqual(len(result["asks"]), 3)
        self.assertEqual(len(result["bids"]), 3)
        self.assertEqual(result["asks"][0][0], "185.50")

    @patch("kraken_stock_connector._discover_stock_pairs")
    def test_get_stock_orderbook_not_found(self, mock_discover):
        """Orderbook for unknown stock returns error."""
        from kraken_stock_connector import KrakenStockConnector

        mock_discover.return_value = {}

        result = KrakenStockConnector.get_stock_orderbook("NOPE")
        self.assertIn("error", result)


class TestPlaceStockOrder(unittest.TestCase):
    """Tests for KrakenStockConnector.place_stock_order()."""

    @patch("kraken_stock_connector.is_market_open", return_value=False)
    def test_place_stock_order_market_closed(self, mock_market):
        """Order rejected when market is closed."""
        from kraken_stock_connector import KrakenStockConnector

        result = KrakenStockConnector.place_stock_order(
            symbol="AAPL", side="buy", shares=1.0,
            order_type="limit", price=185.00, confidence=0.80,
        )
        self.assertIn("error", result)
        self.assertIn("closed", result["error"])

    @patch("kraken_stock_connector.is_market_open", return_value=True)
    @patch("kraken_stock_connector.GoalValidator")
    def test_place_stock_order_low_confidence(self, mock_gv, mock_market):
        """Order rejected when GoalValidator blocks due to low confidence."""
        from kraken_stock_connector import KrakenStockConnector

        mock_gv.should_trade.return_value = False

        result = KrakenStockConnector.place_stock_order(
            symbol="AAPL", side="buy", shares=1.0,
            order_type="limit", price=185.00, confidence=0.50,
        )
        self.assertIn("error", result)
        self.assertIn("GoalValidator blocked", result["error"])

    @patch("kraken_stock_connector._record_stock_trade")
    @patch("kraken_stock_connector.KrakenConnector")
    @patch("kraken_stock_connector._get_stock_pair", return_value="AAPLUSD")
    @patch("kraken_stock_connector.GoalValidator")
    @patch("kraken_stock_connector.is_market_open", return_value=True)
    def test_place_stock_order_success(self, mock_market, mock_gv, mock_pair,
                                        mock_kc, mock_record):
        """Successful order placement returns txid."""
        from kraken_stock_connector import KrakenStockConnector

        mock_gv.should_trade.return_value = True
        mock_kc._private_request.return_value = SAMPLE_ADD_ORDER_RESPONSE

        result = KrakenStockConnector.place_stock_order(
            symbol="AAPL", side="buy", shares=10.0,
            order_type="limit", price=185.00, confidence=0.85,
        )

        self.assertIn("result", result)
        self.assertEqual(result["result"]["txid"], ["OQCLML-BW3P3-BUCMWZ"])
        mock_record.assert_called_once()

    @patch("kraken_stock_connector.is_market_open", return_value=True)
    @patch("kraken_stock_connector.GoalValidator")
    def test_place_stock_order_limit_no_price(self, mock_gv, mock_market):
        """Limit order without price is rejected."""
        from kraken_stock_connector import KrakenStockConnector

        mock_gv.should_trade.return_value = True

        result = KrakenStockConnector.place_stock_order(
            symbol="AAPL", side="buy", shares=1.0,
            order_type="limit", price=None, confidence=0.80,
        )
        self.assertIn("error", result)
        self.assertIn("requires price", result["error"])


class TestCancelStockOrder(unittest.TestCase):
    """Tests for KrakenStockConnector.cancel_stock_order()."""

    @patch("kraken_stock_connector._update_stock_trade_status")
    @patch("kraken_stock_connector.KrakenConnector")
    def test_cancel_stock_order_success(self, mock_kc, mock_update):
        """Successful cancellation updates trade status."""
        from kraken_stock_connector import KrakenStockConnector

        mock_kc._private_request.return_value = SAMPLE_CANCEL_RESPONSE

        result = KrakenStockConnector.cancel_stock_order("OQCLML-BW3P3-BUCMWZ")

        self.assertIn("result", result)
        self.assertEqual(result["result"]["count"], 1)
        mock_update.assert_called_once_with("OQCLML-BW3P3-BUCMWZ", "cancelled")


class TestGetStockPositions(unittest.TestCase):
    """Tests for KrakenStockConnector.get_stock_positions()."""

    @patch("kraken_stock_connector._discover_stock_pairs")
    @patch("kraken_stock_connector.KrakenConnector")
    def test_get_stock_positions_filters_equities(self, mock_kc, mock_discover):
        """Positions filter for equity assets only."""
        from kraken_stock_connector import KrakenStockConnector

        mock_discover.return_value = {"AAPL": "AAPLUSD", "SPY": "SPYUSD"}
        mock_kc._private_request.return_value = SAMPLE_BALANCE_RESPONSE

        result = KrakenStockConnector.get_stock_positions()

        self.assertNotIn("error", result)
        self.assertEqual(result["count"], 2)
        self.assertIn("AAPL", result["positions"])
        self.assertIn("SPY", result["positions"])
        # ZUSD and XXBT should NOT appear
        self.assertNotIn("ZUSD", result["positions"])
        self.assertNotIn("XXBT", result["positions"])


class TestSearchStocks(unittest.TestCase):
    """Tests for KrakenStockConnector.search_stocks()."""

    @patch("kraken_stock_connector._discover_stock_pairs")
    def test_search_stocks_partial_match(self, mock_discover):
        """Search returns partial symbol matches."""
        from kraken_stock_connector import KrakenStockConnector

        mock_discover.return_value = {
            "AAPL": "AAPLUSD",
            "AMZN": "AMZNUSD",
            "AMD": "AMDUSD",
            "SPY": "SPYUSD",
        }

        results = KrakenStockConnector.search_stocks("A")
        self.assertEqual(len(results), 3)  # AAPL, AMZN, AMD

        results = KrakenStockConnector.search_stocks("AM")
        self.assertEqual(len(results), 2)  # AMZN, AMD

        results = KrakenStockConnector.search_stocks("SPY")
        self.assertEqual(len(results), 1)


class TestStockTradeDb(unittest.TestCase):
    """Tests for stock trade DB recording."""

    def test_stock_trade_db_record(self):
        """Trades are recorded in the SQLite database."""
        import kraken_stock_connector as ksc

        # Use a temp DB
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_db_path = f.name

        original_db = ksc.STOCK_TRADE_DB
        try:
            ksc.STOCK_TRADE_DB = Path(tmp_db_path)

            # Record a trade
            result = {"result": {"txid": ["TX123456"]}}
            ksc._record_stock_trade(
                "AAPL", "buy", 10.0, 185.00, "limit", result, 0.85,
            )

            # Verify in DB
            db = sqlite3.connect(tmp_db_path)
            cursor = db.execute("SELECT * FROM kraken_stock_trades")
            rows = cursor.fetchall()
            db.close()

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row[1], "AAPL")     # symbol
            self.assertEqual(row[2], "buy")       # side
            self.assertAlmostEqual(row[3], 10.0)  # shares
            self.assertAlmostEqual(row[4], 185.0) # price
            self.assertEqual(row[5], "limit")     # order_type
            self.assertEqual(row[6], "TX123456")  # txid
            self.assertEqual(row[7], "submitted") # status
        finally:
            ksc.STOCK_TRADE_DB = original_db
            os.unlink(tmp_db_path)


class TestDiscoverStockPairs(unittest.TestCase):
    """Tests for _discover_stock_pairs()."""

    @patch("kraken_stock_connector.urllib.request.urlopen")
    def test_discover_stock_pairs_filters_equity(self, mock_urlopen):
        """Discovery correctly identifies equity pairs."""
        import kraken_stock_connector as ksc

        # Reset cache
        ksc.STOCK_PAIR_CACHE = {}
        ksc.STOCK_CACHE_EXPIRY = 0

        mock_urlopen.return_value = _mock_urlopen(SAMPLE_ASSET_PAIRS_RESPONSE)

        pairs = ksc._discover_stock_pairs()

        # Should include AAPL, SPY, COIN (equity aclass_base)
        self.assertIn("AAPL", pairs)
        self.assertIn("SPY", pairs)
        self.assertIn("COIN", pairs)
        # Should NOT include XXBT (crypto)
        self.assertNotIn("XXBT", pairs)
        self.assertNotIn("XBT", pairs)

    @patch("kraken_stock_connector.urllib.request.urlopen")
    def test_discover_stock_pairs_cached(self, mock_urlopen):
        """Discovery uses cache on second call."""
        import kraken_stock_connector as ksc

        ksc.STOCK_PAIR_CACHE = {"AAPL": "AAPLUSD"}
        ksc.STOCK_CACHE_EXPIRY = time.time() + 3600  # Cache still valid

        pairs = ksc._discover_stock_pairs()

        self.assertEqual(pairs, {"AAPL": "AAPLUSD"})
        mock_urlopen.assert_not_called()  # Should use cache

        # Reset
        ksc.STOCK_PAIR_CACHE = {}
        ksc.STOCK_CACHE_EXPIRY = 0


class TestCommissionFree(unittest.TestCase):
    """Tests that verify Kraken stocks are commission-free in quotes."""

    @patch("smart_router.SmartRouter._get_kraken_stock_quote")
    def test_kraken_stock_zero_fee(self, mock_quote):
        """Kraken stock venue reports 0% commission fee."""
        mock_quote.return_value = {
            "venue": "kraken_stock",
            "chain": "cex",
            "price": 185.45,
            "amount_out": 0.027,
            "fee_pct": 0.0,
            "gas_usd": 0,
            "slippage_pct": 0.027,
            "total_cost_pct": 0.027,
            "total_cost_usd": 0.0014,
            "commission_free": True,
        }

        result = mock_quote("AAPL", "BUY", 5.0)
        self.assertEqual(result["fee_pct"], 0.0)
        self.assertTrue(result["commission_free"])
        self.assertEqual(result["venue"], "kraken_stock")

    @patch("kraken_stock_connector.urllib.request.urlopen")
    @patch("kraken_stock_connector._discover_stock_pairs")
    def test_stock_quote_spread_calculation(self, mock_discover, mock_urlopen):
        """Verify spread is calculated from bid-ask."""
        from kraken_stock_connector import KrakenStockConnector

        mock_discover.return_value = {"AAPL": "AAPLUSD"}
        mock_urlopen.return_value = _mock_urlopen(SAMPLE_TICKER_RESPONSE)

        result = KrakenStockConnector.get_stock_quote("AAPL")

        # Spread = ask - bid = 185.50 - 185.40 = 0.10
        self.assertAlmostEqual(result["spread"], 0.10)
        # Spread as % of price: 0.10 / 185.45 * 100 = ~0.054%
        spread_pct = result["spread"] / result["last_price"] * 100
        self.assertLess(spread_pct, 0.1)  # Spread should be very tight


class TestMarketStatus(unittest.TestCase):
    """Tests for KrakenStockConnector.get_market_status()."""

    @patch("kraken_stock_connector.is_market_open")
    def test_get_market_status(self, mock_market):
        """Market status returns correct structure."""
        from kraken_stock_connector import KrakenStockConnector

        # First call (extended_hours=False) -> True, second (extended_hours=True) -> True
        mock_market.side_effect = [True, True]

        status = KrakenStockConnector.get_market_status()

        self.assertTrue(status["regular_hours_open"])
        self.assertTrue(status["extended_hours_open"])
        self.assertEqual(status["timezone"], "America/New_York")
        self.assertIn("9:30", status["regular_hours"])
        self.assertIn("4:00", status["extended_hours"])


if __name__ == "__main__":
    unittest.main()
