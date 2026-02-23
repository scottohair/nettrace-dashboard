#!/usr/bin/env python3
"""Tests for cross-venue transfer manager."""
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch, MagicMock, PropertyMock
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

# Set env vars BEFORE importing the module so config picks them up
os.environ.setdefault("CROSS_VENUE_TRANSFER_ENABLED", "1")
os.environ.setdefault("CROSS_VENUE_MIN_RESERVE_USD", "10.0")
os.environ.setdefault("CROSS_VENUE_PREFERRED_ASSET", "USDC")

from cross_venue_transfer import (
    CrossVenueTransfer,
    _init_transfer_db,
    CROSS_VENUE_MIN_RESERVE_USD,
    SUPPORTED_VENUES,
)


class TestTransferDBInit(unittest.TestCase):
    """Test that the transfer database initializes correctly."""

    def test_transfer_db_init(self):
        """DB creates the cross_venue_transfers table correctly."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            db = _init_transfer_db(db_path)
            # Check the table exists
            cursor = db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='cross_venue_transfers'"
            )
            tables = cursor.fetchall()
            self.assertEqual(len(tables), 1)
            self.assertEqual(tables[0]["name"], "cross_venue_transfers")

            # Check columns
            cursor = db.execute("PRAGMA table_info(cross_venue_transfers)")
            columns = {row["name"] for row in cursor.fetchall()}
            expected_cols = {
                "id", "from_venue", "to_venue", "asset", "amount",
                "status", "txid", "deposit_address", "network_fee",
                "initiated_at", "completed_at", "notes",
            }
            self.assertTrue(expected_cols.issubset(columns))
            db.close()
        finally:
            os.unlink(db_path)


class TestValidateTransfer(unittest.TestCase):
    """Test transfer validation logic."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.mgr = CrossVenueTransfer(db_path=self.tmp.name)

    def tearDown(self):
        self.mgr.close()
        os.unlink(self.tmp.name)

    @patch.object(CrossVenueTransfer, "get_venue_balances")
    def test_validate_transfer_enabled(self, mock_balances):
        """Validation passes when enabled and balances are sufficient."""
        mock_balances.return_value = {
            "coinbase": {"USDC": 100.0, "USD": 50.0},
            "kraken": {"USDC": 20.0},
            "etrade": {"USD": 0.0},
        }
        with patch("cross_venue_transfer.CROSS_VENUE_TRANSFER_ENABLED", True):
            valid, reason = self.mgr.validate_transfer("coinbase", "kraken", "USDC", 50.0)
            self.assertTrue(valid)
            self.assertEqual(reason, "Transfer validated")

    def test_validate_transfer_disabled(self):
        """Validation blocks when transfers are disabled."""
        with patch("cross_venue_transfer.CROSS_VENUE_TRANSFER_ENABLED", False):
            valid, reason = self.mgr.validate_transfer("coinbase", "kraken", "USDC", 50.0)
            self.assertFalse(valid)
            self.assertIn("disabled", reason)

    @patch.object(CrossVenueTransfer, "get_venue_balances")
    def test_validate_below_reserve(self, mock_balances):
        """Validation blocks if transfer would drain venue below reserve."""
        mock_balances.return_value = {
            "coinbase": {"USDC": 15.0},
            "kraken": {"USDC": 20.0},
            "etrade": {"USD": 0.0},
        }
        with patch("cross_venue_transfer.CROSS_VENUE_TRANSFER_ENABLED", True):
            # Trying to transfer 10 USDC from a 15 USDC balance:
            # 15 - 10 = 5 < 10 minimum reserve
            valid, reason = self.mgr.validate_transfer("coinbase", "kraken", "USDC", 10.0)
            self.assertFalse(valid)
            self.assertIn("reserve", reason.lower())

    def test_validate_same_venue(self):
        """Cannot transfer from a venue to itself."""
        with patch("cross_venue_transfer.CROSS_VENUE_TRANSFER_ENABLED", True):
            valid, reason = self.mgr.validate_transfer("coinbase", "coinbase", "USDC", 10.0)
            self.assertFalse(valid)
            self.assertIn("different", reason)

    def test_validate_negative_amount(self):
        """Cannot transfer a negative or zero amount."""
        with patch("cross_venue_transfer.CROSS_VENUE_TRANSFER_ENABLED", True):
            valid, reason = self.mgr.validate_transfer("coinbase", "kraken", "USDC", 0)
            self.assertFalse(valid)
            self.assertIn("positive", reason)

            valid, reason = self.mgr.validate_transfer("coinbase", "kraken", "USDC", -5)
            self.assertFalse(valid)
            self.assertIn("positive", reason)

    def test_validate_unsupported_venue(self):
        """Cannot transfer from/to unsupported venues."""
        with patch("cross_venue_transfer.CROSS_VENUE_TRANSFER_ENABLED", True):
            valid, reason = self.mgr.validate_transfer("binance", "kraken", "USDC", 10.0)
            self.assertFalse(valid)
            self.assertIn("Unsupported", reason)


class TestCoinbaseToKraken(unittest.TestCase):
    """Test the Coinbase -> Kraken transfer path."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.mgr = CrossVenueTransfer(db_path=self.tmp.name)

    def tearDown(self):
        self.mgr.close()
        os.unlink(self.tmp.name)

    @patch.object(CrossVenueTransfer, "initiate_withdrawal")
    @patch.object(CrossVenueTransfer, "get_deposit_address")
    @patch.object(CrossVenueTransfer, "validate_transfer")
    def test_coinbase_to_kraken_happy_path(self, mock_validate, mock_addr, mock_withdraw):
        """Full Coinbase -> Kraken transfer succeeds with mocked APIs."""
        mock_validate.return_value = (True, "Transfer validated")
        mock_addr.return_value = {
            "address": "0x1234567890abcdef1234567890abcdef12345678",
            "network": "ethereum",
            "memo": None,
            "venue": "kraken",
            "asset": "USDC",
        }
        mock_withdraw.return_value = {
            "txid": "tx_abc123",
            "status": "initiated",
            "venue": "coinbase",
        }

        result = self.mgr.coinbase_to_kraken("USDC", 50.0)

        self.assertNotIn("error", result)
        self.assertEqual(result["from_venue"], "coinbase")
        self.assertEqual(result["to_venue"], "kraken")
        self.assertEqual(result["asset"], "USDC")
        self.assertEqual(result["amount"], 50.0)
        self.assertEqual(result["status"], "initiated")
        self.assertEqual(result["txid"], "tx_abc123")
        self.assertIn("transfer_id", result)

        # Verify DB record
        row = self.mgr.db.execute(
            "SELECT * FROM cross_venue_transfers WHERE id = ?",
            (result["transfer_id"],)
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["from_venue"], "coinbase")
        self.assertEqual(row["to_venue"], "kraken")
        self.assertEqual(row["status"], "initiated")


class TestKrakenToCoinbase(unittest.TestCase):
    """Test the Kraken -> Coinbase transfer path."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.mgr = CrossVenueTransfer(db_path=self.tmp.name)

    def tearDown(self):
        self.mgr.close()
        os.unlink(self.tmp.name)

    @patch.object(CrossVenueTransfer, "initiate_withdrawal")
    @patch.object(CrossVenueTransfer, "get_deposit_address")
    @patch.object(CrossVenueTransfer, "validate_transfer")
    def test_kraken_to_coinbase_happy_path(self, mock_validate, mock_addr, mock_withdraw):
        """Full Kraken -> Coinbase transfer succeeds with mocked APIs."""
        mock_validate.return_value = (True, "Transfer validated")
        mock_addr.return_value = {
            "address": "0xabcdef1234567890abcdef1234567890abcdef12",
            "network": "ethereum",
            "memo": None,
            "venue": "coinbase",
            "asset": "USDC",
        }
        mock_withdraw.return_value = {
            "txid": "refid_xyz789",
            "status": "initiated",
            "venue": "kraken",
        }

        result = self.mgr.kraken_to_coinbase("USDC", 30.0)

        self.assertNotIn("error", result)
        self.assertEqual(result["from_venue"], "kraken")
        self.assertEqual(result["to_venue"], "coinbase")
        self.assertEqual(result["asset"], "USDC")
        self.assertEqual(result["amount"], 30.0)
        self.assertEqual(result["status"], "initiated")
        self.assertEqual(result["txid"], "refid_xyz789")


class TestDepositAddress(unittest.TestCase):
    """Test deposit address retrieval."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.mgr = CrossVenueTransfer(db_path=self.tmp.name)

    def tearDown(self):
        self.mgr.close()
        os.unlink(self.tmp.name)

    @patch("cross_venue_transfer.KrakenConnector")
    def test_get_deposit_address_kraken(self, mock_kraken_cls):
        """Kraken deposit address API returns correct format."""
        mock_kraken_cls._private_request.return_value = {
            "error": [],
            "result": [{
                "address": "0xKrakenDepositAddr1234567890abcdef12345678",
                "expiretm": "0",
                "new": True,
            }],
        }

        result = self.mgr._get_kraken_deposit_address("USDC")

        self.assertNotIn("error", result)
        self.assertEqual(result["address"], "0xKrakenDepositAddr1234567890abcdef12345678")
        self.assertEqual(result["venue"], "kraken")
        self.assertEqual(result["asset"], "USDC")
        mock_kraken_cls._private_request.assert_called_once_with("DepositAddresses", {
            "asset": "USDC",
            "method": "Ethereum (ERC20)",
        })

    @patch.object(CrossVenueTransfer, "_get_coinbase")
    def test_get_deposit_address_coinbase(self, mock_get_cb):
        """Coinbase deposit address API returns correct format."""
        mock_cb = MagicMock()
        mock_get_cb.return_value = mock_cb

        # Mock get_accounts
        mock_cb.get_accounts.return_value = {
            "accounts": [{
                "currency": "USDC",
                "uuid": "acc-uuid-123",
                "available_balance": {"value": "100.0"},
            }],
        }
        # Mock _request for address creation
        mock_cb._request.return_value = {
            "data": {
                "address": "0xCoinbaseAddr1234567890abcdef1234567890ab",
                "network": "ethereum",
            },
        }

        result = self.mgr._get_coinbase_deposit_address("USDC")

        self.assertNotIn("error", result)
        self.assertEqual(result["address"], "0xCoinbaseAddr1234567890abcdef1234567890ab")
        self.assertEqual(result["venue"], "coinbase")


class TestTrackTransfer(unittest.TestCase):
    """Test transfer status tracking."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.mgr = CrossVenueTransfer(db_path=self.tmp.name)

    def tearDown(self):
        self.mgr.close()
        os.unlink(self.tmp.name)

    @patch("cross_venue_transfer.KrakenConnector")
    def test_track_transfer(self, mock_kraken_cls):
        """Tracking a transfer updates status based on Kraken deposit status."""
        # Insert a test transfer
        transfer_id = self.mgr._log_transfer(
            from_venue="coinbase",
            to_venue="kraken",
            asset="USDC",
            amount=50.0,
            status="initiated",
            txid="tx_test123",
        )

        # Mock Kraken deposit status showing completed
        mock_kraken_cls._private_request.return_value = {
            "error": [],
            "result": [{
                "amount": 50.0,
                "txid": "tx_test123",
                "status": "Success",
            }],
        }

        result = self.mgr.track_transfer(transfer_id)

        self.assertEqual(result["status"], "completed")
        self.assertIsNotNone(result["completed_at"])

    def test_track_transfer_not_found(self):
        """Tracking a nonexistent transfer returns error."""
        result = self.mgr.track_transfer(99999)
        self.assertIn("error", result)
        self.assertIn("not found", result["error"])


class TestTransferHistory(unittest.TestCase):
    """Test transfer history retrieval."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.mgr = CrossVenueTransfer(db_path=self.tmp.name)

    def tearDown(self):
        self.mgr.close()
        os.unlink(self.tmp.name)

    def test_transfer_history(self):
        """Returns recent transfers ordered by newest first."""
        # Insert several transfers
        for i in range(5):
            self.mgr._log_transfer(
                from_venue="coinbase",
                to_venue="kraken",
                asset="USDC",
                amount=10.0 * (i + 1),
                status="initiated" if i < 3 else "completed",
            )

        history = self.mgr.transfer_history(limit=3)

        self.assertEqual(len(history), 3)
        # Should be newest first
        self.assertEqual(history[0]["amount"], 50.0)
        self.assertEqual(history[1]["amount"], 40.0)
        self.assertEqual(history[2]["amount"], 30.0)

    def test_transfer_history_empty(self):
        """Empty DB returns empty list."""
        history = self.mgr.transfer_history()
        self.assertEqual(history, [])


class TestVenueBalances(unittest.TestCase):
    """Test aggregated venue balance retrieval."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.mgr = CrossVenueTransfer(db_path=self.tmp.name)

    def tearDown(self):
        self.mgr.close()
        os.unlink(self.tmp.name)

    @patch.object(CrossVenueTransfer, "_get_etrade_balances")
    @patch.object(CrossVenueTransfer, "_get_kraken_balances")
    @patch.object(CrossVenueTransfer, "_get_coinbase_balances")
    def test_venue_balances(self, mock_cb, mock_kr, mock_et):
        """Aggregates balances from all venues."""
        mock_cb.return_value = {"USDC": 100.0, "BTC": 0.001}
        mock_kr.return_value = {"USDC": 50.0, "ETH": 0.5}
        mock_et.return_value = {"USD": 200.0, "total_value": 290.0}

        result = self.mgr.get_venue_balances()

        self.assertIn("coinbase", result)
        self.assertIn("kraken", result)
        self.assertIn("etrade", result)
        self.assertEqual(result["coinbase"]["USDC"], 100.0)
        self.assertEqual(result["kraken"]["USDC"], 50.0)
        self.assertEqual(result["etrade"]["USD"], 200.0)


class TestEstimateETradeToKraken(unittest.TestCase):
    """Test ACH estimation."""

    def test_estimate_etrade_to_kraken(self):
        """Returns info dict with correct structure."""
        info = CrossVenueTransfer.estimate_etrade_to_kraken(500.0)

        self.assertEqual(info["method"], "ACH")
        self.assertEqual(info["estimated_days"], "3-5")
        self.assertEqual(info["fee"], 0.0)
        self.assertEqual(info["amount"], 500.0)
        self.assertIsInstance(info["steps"], list)
        self.assertEqual(len(info["steps"]), 2)
        self.assertIn("note", info)


class TestMinimumReserveCheck(unittest.TestCase):
    """Test minimum reserve checking."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.mgr = CrossVenueTransfer(db_path=self.tmp.name)

    def tearDown(self):
        self.mgr.close()
        os.unlink(self.tmp.name)

    @patch.object(CrossVenueTransfer, "get_venue_balances")
    def test_minimum_reserve_check_above(self, mock_balances):
        """Venue above minimum reserve returns above_reserve=True."""
        mock_balances.return_value = {
            "coinbase": {"USDC": 50.0, "USD": 10.0},
            "kraken": {},
            "etrade": {},
        }

        result = self.mgr.check_minimum_reserve("coinbase")

        self.assertEqual(result["venue"], "coinbase")
        self.assertEqual(result["balance"], 60.0)
        self.assertEqual(result["reserve"], CROSS_VENUE_MIN_RESERVE_USD)
        self.assertTrue(result["above_reserve"])

    @patch.object(CrossVenueTransfer, "get_venue_balances")
    def test_minimum_reserve_check_below(self, mock_balances):
        """Venue below minimum reserve returns above_reserve=False."""
        mock_balances.return_value = {
            "coinbase": {"USDC": 3.0},
            "kraken": {},
            "etrade": {},
        }

        result = self.mgr.check_minimum_reserve("coinbase")

        self.assertEqual(result["balance"], 3.0)
        self.assertFalse(result["above_reserve"])


class TestAddressValidation(unittest.TestCase):
    """Test deposit address format validation."""

    def test_valid_ethereum_address(self):
        """Valid Ethereum address passes."""
        valid, reason = CrossVenueTransfer.validate_address(
            "0x1234567890abcdef1234567890abcdef12345678",
            "ethereum",
        )
        self.assertTrue(valid)

    def test_invalid_ethereum_address(self):
        """Invalid Ethereum address fails."""
        valid, reason = CrossVenueTransfer.validate_address("not-an-address", "ethereum")
        self.assertFalse(valid)

    def test_empty_address(self):
        """Empty address fails."""
        valid, reason = CrossVenueTransfer.validate_address("", "ethereum")
        self.assertFalse(valid)

    def test_none_address(self):
        """None address fails."""
        valid, reason = CrossVenueTransfer.validate_address(None, "ethereum")
        self.assertFalse(valid)

    def test_unknown_network_long_address(self):
        """Unknown network accepts sufficiently long address."""
        valid, reason = CrossVenueTransfer.validate_address(
            "a" * 40, "polygon"
        )
        self.assertTrue(valid)


class TestEnsureVenueFunded(unittest.TestCase):
    """Test intelligent auto-funding logic."""

    def _make_mgr(self):
        return CrossVenueTransfer(db_path=":memory:")

    @patch.object(CrossVenueTransfer, "get_venue_balances")
    def test_already_funded(self, mock_bal):
        """No transfer needed when venue has enough."""
        mock_bal.return_value = {
            "coinbase": {"USD": 100, "USDC": 50},
            "kraken": {"USD": 20, "USDC": 30},
            "etrade": {},
        }
        with patch("cross_venue_transfer.CROSS_VENUE_TRANSFER_ENABLED", True):
            mgr = self._make_mgr()
            result = mgr.ensure_venue_funded("kraken", "USDC", 10.0)
            mgr.close()
        self.assertTrue(result["funded"])
        self.assertEqual(result["transferred"], 0)

    @patch.object(CrossVenueTransfer, "coinbase_to_kraken")
    @patch.object(CrossVenueTransfer, "get_venue_balances")
    def test_auto_funds_from_richest(self, mock_bal, mock_xfer):
        """Transfers from richest venue when target is short."""
        mock_bal.return_value = {
            "coinbase": {"USD": 50, "USDC": 80},
            "kraken": {"USD": 0, "USDC": 2},
            "etrade": {"USD": 10},
        }
        mock_xfer.return_value = {"transfer_id": 42}
        with patch("cross_venue_transfer.CROSS_VENUE_TRANSFER_ENABLED", True):
            mgr = self._make_mgr()
            result = mgr.ensure_venue_funded("kraken", "USDC", 25.0)
            mgr.close()
        self.assertTrue(result["funded"])
        self.assertGreater(result["transferred"], 0)
        self.assertEqual(result["from_venue"], "coinbase")
        mock_xfer.assert_called_once()

    @patch.object(CrossVenueTransfer, "get_venue_balances")
    def test_insufficient_everywhere(self, mock_bal):
        """Returns funded=False when no venue has enough."""
        mock_bal.return_value = {
            "coinbase": {"USD": 5, "USDC": 3},
            "kraken": {"USD": 0, "USDC": 1},
            "etrade": {},
        }
        with patch("cross_venue_transfer.CROSS_VENUE_TRANSFER_ENABLED", True):
            mgr = self._make_mgr()
            result = mgr.ensure_venue_funded("kraken", "USDC", 100.0)
            mgr.close()
        self.assertFalse(result["funded"])
        self.assertIn("insufficient", result["reason"])

    def test_disabled(self):
        """Returns funded=False when transfers disabled."""
        with patch("cross_venue_transfer.CROSS_VENUE_TRANSFER_ENABLED", False):
            mgr = self._make_mgr()
            result = mgr.ensure_venue_funded("kraken", "USDC", 10.0)
            mgr.close()
        self.assertFalse(result["funded"])
        self.assertIn("disabled", result["reason"])


if __name__ == "__main__":
    unittest.main()
