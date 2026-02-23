#!/usr/bin/env python3
"""Tests for Kraken Staking/Earn connector."""
import unittest
from unittest.mock import patch, MagicMock
import json
import sys
import os
import tempfile
import sqlite3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

# Mock responses
MOCK_STRATEGIES = {
    "result": {
        "items": [
            {
                "id": "ESRG-ETH-FLEX",
                "asset": "ETH",
                "yield_source": {"apy": 3.5},
                "min_amount": 0.01,
                "lock_type": {"type": "flexible"},
                "can_trade": True,
            },
            {
                "id": "ESRG-DOT-BOND",
                "asset": "DOT",
                "yield_source": {"apy": 12.0},
                "min_amount": 1.0,
                "lock_type": {"type": "bonded", "bonding_period_days": 28},
                "can_trade": False,
            },
        ]
    }
}

MOCK_LEGACY_ASSETS = {
    "result": [
        {
            "asset": "ETH",
            "method": "ethereum",
            "rewards": {"reward": "4.0"},
            "minimum_amount": {"staking": "0.01"},
            "lock": None,
        },
        {
            "asset": "DOT",
            "method": "polkadot",
            "rewards": {"reward": "10.0"},
            "minimum_amount": {"staking": "1.0"},
            "lock": {"lockup": "14", "unstaking": "28"},
        },
    ]
}

MOCK_EARN_ALLOCATIONS = {
    "result": {
        "items": [
            {
                "native_asset": "ETH",
                "amount_allocated": {
                    "total": {"native": "1.5"},
                    "pending": {"native": "0.0"},
                },
                "total_rewarded": {"native": "0.01"},
                "strategy_id": "ESRG-ETH-FLEX",
            },
            {
                "native_asset": "DOT",
                "amount_allocated": {
                    "total": {"native": "50.0"},
                    "pending": {"native": "5.0"},
                },
                "total_rewarded": {"native": "2.5"},
                "strategy_id": "ESRG-DOT-BOND",
            },
        ]
    }
}

MOCK_LEGACY_PENDING = {
    "result": [
        {"asset": "SOL", "amount": "10.0", "type": "staking"},
        {"asset": "SOL", "amount": "5.0", "type": "staking"},
    ]
}

MOCK_BALANCE = {
    "result": {
        "ETH": "5.0",
        "DOT": "200.0",
        "USD": "100.0",
    }
}


class TestListStakeableAssets(unittest.TestCase):
    """Tests for KrakenStakingConnector.list_stakeable_assets."""

    @patch("kraken_staking_connector.KrakenConnector")
    def test_earn_strategies_format(self, mock_kc):
        """Parse Earn/Strategies response correctly."""
        mock_kc._private_request.return_value = MOCK_STRATEGIES

        from kraken_staking_connector import KrakenStakingConnector
        assets = KrakenStakingConnector.list_stakeable_assets()

        self.assertEqual(len(assets), 2)
        eth = assets[0]
        self.assertEqual(eth["id"], "ESRG-ETH-FLEX")
        self.assertEqual(eth["asset"], "ETH")
        self.assertEqual(eth["apy"], 3.5)
        self.assertEqual(eth["lock_type"], "flexible")
        self.assertTrue(eth["can_trade_while_staked"])

        dot = assets[1]
        self.assertEqual(dot["asset"], "DOT")
        self.assertEqual(dot["apy"], 12.0)
        self.assertEqual(dot["lock_type"], "bonded")
        self.assertEqual(dot["lock_days"], 28)
        self.assertFalse(dot["can_trade_while_staked"])

    @patch("kraken_staking_connector.KrakenConnector")
    def test_legacy_staking_assets_format(self, mock_kc):
        """Fall back to Staking/Assets and parse legacy format."""
        mock_kc._private_request.side_effect = [
            {"error": ["EAPI:Invalid"]},  # Earn/Strategies fails
            MOCK_LEGACY_ASSETS,           # Staking/Assets succeeds
        ]

        from kraken_staking_connector import KrakenStakingConnector
        assets = KrakenStakingConnector.list_stakeable_assets()

        self.assertEqual(len(assets), 2)
        eth = assets[0]
        self.assertEqual(eth["asset"], "ETH")
        self.assertEqual(eth["apy"], 4.0)
        self.assertEqual(eth["lock_type"], "flexible")
        self.assertEqual(eth["lock_days"], 0)

        dot = assets[1]
        self.assertEqual(dot["asset"], "DOT")
        self.assertEqual(dot["apy"], 10.0)
        self.assertEqual(dot["lock_type"], "bonded")
        self.assertEqual(dot["lock_days"], 14)
        self.assertEqual(dot["unstake_days"], 28)

    @patch("kraken_staking_connector.KrakenConnector", None)
    def test_no_connector(self):
        """Return empty list when KrakenConnector is not available."""
        from kraken_staking_connector import KrakenStakingConnector
        assets = KrakenStakingConnector.list_stakeable_assets()
        self.assertEqual(assets, [])


class TestStakingRates(unittest.TestCase):
    """Tests for KrakenStakingConnector.get_staking_rates."""

    @patch("kraken_staking_connector.KrakenConnector")
    def test_rates_from_strategies(self, mock_kc):
        """Get APY per asset from strategies."""
        mock_kc._private_request.return_value = MOCK_STRATEGIES

        from kraken_staking_connector import KrakenStakingConnector
        rates = KrakenStakingConnector.get_staking_rates()

        self.assertIn("ETH", rates)
        self.assertEqual(rates["ETH"]["apy"], 3.5)
        self.assertEqual(rates["ETH"]["lock_type"], "flexible")
        self.assertIn("DOT", rates)
        self.assertEqual(rates["DOT"]["apy"], 12.0)

    @patch("kraken_staking_connector.KrakenConnector")
    def test_empty_strategies(self, mock_kc):
        """Return empty dict when no strategies available."""
        mock_kc._private_request.return_value = {"result": {"items": []}}

        from kraken_staking_connector import KrakenStakingConnector
        rates = KrakenStakingConnector.get_staking_rates()
        self.assertEqual(rates, {})


class TestStake(unittest.TestCase):
    """Tests for KrakenStakingConnector.stake."""

    @patch("kraken_staking_connector.STAKING_ENABLED", False)
    def test_stake_disabled(self):
        """Blocked when STAKING_ENABLED=0."""
        from kraken_staking_connector import KrakenStakingConnector
        result = KrakenStakingConnector.stake("ETH", 1.0)
        self.assertIn("error", result)
        self.assertIn("not enabled", result["error"])

    @patch("kraken_staking_connector.STAKING_ENABLED", True)
    @patch("kraken_staking_connector.MIN_STAKE_USD", 5.0)
    def test_stake_below_minimum(self):
        """Blocked when amount < MIN_STAKE_USD."""
        from kraken_staking_connector import KrakenStakingConnector
        result = KrakenStakingConnector.stake("ETH", 1.0)
        self.assertIn("error", result)
        self.assertIn("below minimum", result["error"])

    @patch("kraken_staking_connector._record_staking_event")
    @patch("kraken_staking_connector.STAKING_ENABLED", True)
    @patch("kraken_staking_connector.MIN_STAKE_USD", 0.01)
    @patch("kraken_staking_connector.KrakenConnector")
    def test_stake_earn_api(self, mock_kc, mock_record):
        """Uses Earn/Allocate when strategy found."""
        # First call: list_stakeable_assets -> Earn/Strategies
        # Second call: Earn/Allocate
        mock_kc._private_request.side_effect = [
            MOCK_STRATEGIES,
            {"result": True},
        ]

        from kraken_staking_connector import KrakenStakingConnector
        result = KrakenStakingConnector.stake("ETH", 1.0, "flexible")

        self.assertEqual(result, {"result": True})
        # Verify Earn/Allocate was called with strategy_id
        calls = mock_kc._private_request.call_args_list
        self.assertEqual(calls[1][0][0], "Earn/Allocate")
        self.assertEqual(calls[1][0][1]["strategy_id"], "ESRG-ETH-FLEX")
        mock_record.assert_called_once()

    @patch("kraken_staking_connector._record_staking_event")
    @patch("kraken_staking_connector.STAKING_ENABLED", True)
    @patch("kraken_staking_connector.MIN_STAKE_USD", 0.01)
    @patch("kraken_staking_connector.KrakenConnector")
    def test_stake_legacy_api(self, mock_kc, mock_record):
        """Falls back to legacy Stake when no Earn strategy matches."""
        # Earn/Strategies returns empty -> no strategy_id found
        # Then legacy Stake is called
        mock_kc._private_request.side_effect = [
            {"result": {"items": []}},  # No strategies
            {"result": {"refid": "ABC123"}},  # Legacy Stake success
        ]

        from kraken_staking_connector import KrakenStakingConnector
        result = KrakenStakingConnector.stake("SOL", 5.0, "flexible")

        self.assertIn("result", result)
        calls = mock_kc._private_request.call_args_list
        self.assertEqual(calls[1][0][0], "Stake")
        self.assertEqual(calls[1][0][1]["asset"], "SOL")

    @patch("kraken_staking_connector.STAKING_ENABLED", True)
    @patch("kraken_staking_connector.MIN_STAKE_USD", 0.01)
    @patch("kraken_staking_connector.KrakenConnector")
    def test_stake_records_db(self, mock_kc):
        """Event is recorded in the staking DB."""
        mock_kc._private_request.side_effect = [
            {"result": {"items": []}},
            {"result": {"refid": "XYZ789"}},
        ]

        # Use a temp DB
        import kraken_staking_connector as mod
        original_db = mod.STAKING_DB
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            mod.STAKING_DB = f.name

        try:
            from kraken_staking_connector import KrakenStakingConnector
            KrakenStakingConnector.stake("ETH", 2.0, "flexible")

            db = sqlite3.connect(mod.STAKING_DB)
            rows = db.execute("SELECT * FROM kraken_staking_events").fetchall()
            db.close()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][1], "ETH")  # asset
            self.assertEqual(rows[0][2], 2.0)     # amount
            self.assertEqual(rows[0][4], "stake")  # action
            self.assertEqual(rows[0][5], "success")  # status
        finally:
            mod.STAKING_DB = original_db
            os.unlink(f.name)


class TestUnstake(unittest.TestCase):
    """Tests for KrakenStakingConnector.unstake."""

    @patch("kraken_staking_connector._record_staking_event")
    @patch("kraken_staking_connector.STAKING_ENABLED", True)
    @patch("kraken_staking_connector.KrakenConnector")
    def test_unstake_success(self, mock_kc, mock_record):
        """Calls Earn/Deallocate successfully."""
        mock_kc._private_request.return_value = {"result": True}

        from kraken_staking_connector import KrakenStakingConnector
        result = KrakenStakingConnector.unstake("ETH", 0.5)

        self.assertEqual(result, {"result": True})
        mock_kc._private_request.assert_called_once_with("Earn/Deallocate", {
            "asset": "ETH",
            "amount": "0.5",
        })

    @patch("kraken_staking_connector._record_staking_event")
    @patch("kraken_staking_connector.STAKING_ENABLED", True)
    @patch("kraken_staking_connector.KrakenConnector")
    def test_unstake_fallback(self, mock_kc, mock_record):
        """Falls back to legacy Unstake when Earn/Deallocate fails."""
        mock_kc._private_request.side_effect = [
            {"error": ["EAPI:Invalid"]},
            {"result": {"refid": "UNSTAKE1"}},
        ]

        from kraken_staking_connector import KrakenStakingConnector
        result = KrakenStakingConnector.unstake("DOT", 10.0)

        self.assertIn("result", result)
        calls = mock_kc._private_request.call_args_list
        self.assertEqual(calls[1][0][0], "Unstake")

    @patch("kraken_staking_connector.STAKING_ENABLED", False)
    def test_unstake_disabled(self):
        """Blocked when staking is disabled."""
        from kraken_staking_connector import KrakenStakingConnector
        result = KrakenStakingConnector.unstake("ETH", 1.0)
        self.assertIn("error", result)
        self.assertIn("not enabled", result["error"])


class TestStakingPositions(unittest.TestCase):
    """Tests for KrakenStakingConnector.get_staking_positions."""

    @patch("kraken_staking_connector.KrakenConnector")
    def test_earn_allocations(self, mock_kc):
        """Parse Earn/Allocations format."""
        mock_kc._private_request.return_value = MOCK_EARN_ALLOCATIONS

        from kraken_staking_connector import KrakenStakingConnector
        positions = KrakenStakingConnector.get_staking_positions()

        self.assertIn("ETH", positions)
        self.assertEqual(positions["ETH"]["staked"], 1.5)
        self.assertEqual(positions["ETH"]["rewards"], 0.01)
        self.assertIn("DOT", positions)
        self.assertEqual(positions["DOT"]["staked"], 50.0)
        self.assertEqual(positions["DOT"]["pending"], 5.0)
        self.assertEqual(positions["DOT"]["rewards"], 2.5)

    @patch("kraken_staking_connector.KrakenConnector")
    def test_legacy_pending(self, mock_kc):
        """Parse legacy Staking/Pending format."""
        mock_kc._private_request.side_effect = [
            {"error": ["EAPI:Invalid"]},  # Earn/Allocations fails
            MOCK_LEGACY_PENDING,           # Staking/Pending succeeds
        ]

        from kraken_staking_connector import KrakenStakingConnector
        positions = KrakenStakingConnector.get_staking_positions()

        self.assertIn("SOL", positions)
        # Two SOL entries should accumulate
        self.assertEqual(positions["SOL"]["pending"], 15.0)

    @patch("kraken_staking_connector.KrakenConnector", None)
    def test_no_positions(self):
        """Return empty dict when no connector."""
        from kraken_staking_connector import KrakenStakingConnector
        positions = KrakenStakingConnector.get_staking_positions()
        self.assertEqual(positions, {})


class TestOptimalAllocation(unittest.TestCase):
    """Tests for KrakenStakingConnector.optimal_staking_allocation."""

    @patch("kraken_staking_connector.KrakenStakingConnector.get_staking_positions")
    @patch("kraken_staking_connector.KrakenStakingConnector.get_staking_rates")
    def test_allocation_respects_reserve(self, mock_rates, mock_positions):
        """Keeps liquid reserve (2x daily trading volume)."""
        mock_rates.return_value = {
            "ETH": {"apy": 3.5, "lock_type": "flexible", "min_amount": 0.01},
        }
        mock_positions.return_value = {}

        from kraken_staking_connector import KrakenStakingConnector
        recs = KrakenStakingConnector.optimal_staking_allocation(
            balances={"ETH": 200.0},
            daily_trading_volume=50.0,
        )

        self.assertEqual(len(recs), 1)
        # 200 - 0 (already staked) - 100 (2x50 reserve) = 100 stakeable
        self.assertEqual(recs[0]["amount"], 100.0)
        self.assertEqual(recs[0]["liquid_reserve"], 100.0)

    @patch("kraken_staking_connector.PREFER_FLEXIBLE", True)
    @patch("kraken_staking_connector.KrakenStakingConnector.get_staking_positions")
    @patch("kraken_staking_connector.KrakenStakingConnector.get_staking_rates")
    def test_allocation_prefers_flexible(self, mock_rates, mock_positions):
        """Method selection prefers flexible when configured."""
        mock_rates.return_value = {
            "ETH": {"apy": 3.5, "lock_type": "flexible", "min_amount": 0.01},
        }
        mock_positions.return_value = {}

        from kraken_staking_connector import KrakenStakingConnector
        recs = KrakenStakingConnector.optimal_staking_allocation(
            balances={"ETH": 500.0},
            daily_trading_volume=10.0,
        )

        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["method"], "flexible")

    @patch("kraken_staking_connector.KrakenStakingConnector.get_staking_positions")
    @patch("kraken_staking_connector.KrakenStakingConnector.get_staking_rates")
    def test_allocation_skips_unstakeable(self, mock_rates, mock_positions):
        """Assets not in rates are skipped."""
        mock_rates.return_value = {
            "ETH": {"apy": 3.5, "lock_type": "flexible", "min_amount": 0.01},
        }
        mock_positions.return_value = {}

        from kraken_staking_connector import KrakenStakingConnector
        recs = KrakenStakingConnector.optimal_staking_allocation(
            balances={"ETH": 200.0, "USD": 500.0, "USDC": 300.0},
            daily_trading_volume=10.0,
        )

        # Only ETH should be recommended (USD and USDC not in rates)
        assets = [r["asset"] for r in recs]
        self.assertIn("ETH", assets)
        self.assertNotIn("USD", assets)
        self.assertNotIn("USDC", assets)

    @patch("kraken_staking_connector.KrakenStakingConnector.get_staking_positions")
    @patch("kraken_staking_connector.KrakenStakingConnector.get_staking_rates")
    def test_allocation_with_existing_stakes(self, mock_rates, mock_positions):
        """Deducts already-staked amounts."""
        mock_rates.return_value = {
            "ETH": {"apy": 3.5, "lock_type": "flexible", "min_amount": 0.01},
        }
        mock_positions.return_value = {
            "ETH": {"staked": 50.0, "pending": 0, "rewards": 0},
        }

        from kraken_staking_connector import KrakenStakingConnector
        recs = KrakenStakingConnector.optimal_staking_allocation(
            balances={"ETH": 200.0},
            daily_trading_volume=10.0,
        )

        self.assertEqual(len(recs), 1)
        # 200 - 50 (staked) - 20 (2x10 reserve) = 130 stakeable
        self.assertEqual(recs[0]["amount"], 130.0)


class TestAutoStake(unittest.TestCase):
    """Tests for KrakenStakingConnector.auto_stake_idle."""

    @patch("kraken_staking_connector._record_staking_event")
    @patch("kraken_staking_connector.STAKING_ENABLED", True)
    @patch("kraken_staking_connector.MIN_STAKE_USD", 0.01)
    @patch("kraken_staking_connector.KrakenStakingConnector.optimal_staking_allocation")
    @patch("kraken_staking_connector.KrakenConnector")
    def test_auto_stake_executes(self, mock_kc, mock_alloc, mock_record):
        """Stakes recommended assets."""
        mock_alloc.return_value = [
            {"asset": "ETH", "amount": 10.0, "method": "flexible", "expected_apy": 3.5, "liquid_reserve": 100.0},
            {"asset": "DOT", "amount": 50.0, "method": "bonded", "expected_apy": 12.0, "liquid_reserve": 100.0},
        ]
        # Each stake call does: list_stakeable_assets + Earn/Allocate
        mock_kc._private_request.side_effect = [
            {"result": {"items": []}},  # list_stakeable for ETH
            {"result": {"refid": "S1"}},  # legacy Stake for ETH
            {"result": {"items": []}},  # list_stakeable for DOT
            {"result": {"refid": "S2"}},  # legacy Stake for DOT
        ]

        from kraken_staking_connector import KrakenStakingConnector
        results = KrakenStakingConnector.auto_stake_idle()

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["asset"], "ETH")
        self.assertEqual(results[0]["amount"], 10.0)
        self.assertEqual(results[1]["asset"], "DOT")
        self.assertEqual(results[1]["amount"], 50.0)

    @patch("kraken_staking_connector.STAKING_ENABLED", False)
    def test_auto_stake_disabled(self):
        """Returns empty when staking is disabled."""
        from kraken_staking_connector import KrakenStakingConnector
        results = KrakenStakingConnector.auto_stake_idle()
        self.assertEqual(results, [])


class TestStakingDB(unittest.TestCase):
    """Tests for staking DB initialization and event recording."""

    def test_db_init(self):
        """Table is created correctly."""
        from kraken_staking_connector import _init_staking_db
        import kraken_staking_connector as mod

        original_db = mod.STAKING_DB
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            mod.STAKING_DB = f.name

        try:
            db = _init_staking_db()
            # Check table exists
            cursor = db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='kraken_staking_events'"
            )
            tables = cursor.fetchall()
            self.assertEqual(len(tables), 1)
            db.close()
        finally:
            mod.STAKING_DB = original_db
            os.unlink(f.name)

    def test_record_event(self):
        """Event is stored correctly in the DB."""
        from kraken_staking_connector import _record_staking_event, _init_staking_db
        import kraken_staking_connector as mod

        original_db = mod.STAKING_DB
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            mod.STAKING_DB = f.name

        try:
            _record_staking_event("SOL", 25.0, "flexible", "stake", {"result": {"refid": "R1"}})

            db = sqlite3.connect(mod.STAKING_DB)
            rows = db.execute("SELECT * FROM kraken_staking_events").fetchall()
            db.close()

            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row[1], "SOL")        # asset
            self.assertEqual(row[2], 25.0)          # amount
            self.assertEqual(row[3], "flexible")    # method
            self.assertEqual(row[4], "stake")       # action
            self.assertEqual(row[5], "success")     # status
            result_data = json.loads(row[6])
            self.assertIn("result", result_data)
        finally:
            mod.STAKING_DB = original_db
            os.unlink(f.name)


class TestPendingTransactions(unittest.TestCase):
    """Tests for get_pending_transactions."""

    @patch("kraken_staking_connector.KrakenConnector")
    def test_pending_returns_list(self, mock_kc):
        """Returns list of pending transactions."""
        mock_kc._private_request.return_value = MOCK_LEGACY_PENDING

        from kraken_staking_connector import KrakenStakingConnector
        pending = KrakenStakingConnector.get_pending_transactions()

        self.assertEqual(len(pending), 2)
        self.assertEqual(pending[0]["asset"], "SOL")

    @patch("kraken_staking_connector.KrakenConnector", None)
    def test_pending_no_connector(self):
        """Returns empty list when no connector."""
        from kraken_staking_connector import KrakenStakingConnector
        pending = KrakenStakingConnector.get_pending_transactions()
        self.assertEqual(pending, [])


if __name__ == "__main__":
    unittest.main()
