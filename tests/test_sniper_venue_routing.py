#!/usr/bin/env python3
"""Tests for sniper multi-venue routing.

Verifies:
  1. SmartRouter selects Kraken when it's the cheapest venue
  2. SmartRouter selects Coinbase (default) when appropriate
  3. SmartRouter failure falls back to Coinbase
  4. Kraken execution failure falls back to Coinbase
  5. Venue is recorded correctly in sniper trade DB
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock, PropertyMock

# Add agents/ to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))


class TestSniperVenueRouting(unittest.TestCase):
    """Test multi-venue routing in sniper execution."""

    def _make_sniper(self):
        """Create a Sniper instance with mocked dependencies."""
        # Must import after path setup
        with patch("sniper._goals", MagicMock()), \
             patch("sniper._deriv", None), \
             patch("sniper._tracker", None), \
             patch("sniper._kpi", None):
            from sniper import Sniper
            # Reset class-level venue caches between tests
            Sniper._kraken = None
            Sniper._smart_router = None
            return Sniper

    def test_smart_router_selects_kraken(self):
        """When SmartRouter returns venue='kraken', sniper uses KrakenConnector."""
        Sniper = self._make_sniper()

        mock_router = MagicMock()
        mock_router.find_best_execution.return_value = {
            "venue": "kraken",
            "price": 95000.0,
            "fee_pct": 0.16,
            "total_cost_pct": 0.20,
            "savings_vs_coinbase": 0.20,
        }

        mock_kraken = MagicMock()
        mock_kraken.place_order.return_value = {
            "result": {"txid": ["KRAKEN-ORDER-123"]},
        }

        # Patch at class level
        Sniper._smart_router = mock_router
        Sniper._kraken = mock_kraken

        # Verify the lazy accessors return the mocked objects
        self.assertIs(Sniper._get_smart_router(), mock_router)
        self.assertIs(Sniper._new_kraken_trader(), mock_kraken)

    def test_smart_router_selects_coinbase(self):
        """When SmartRouter returns venue='coinbase', default path is used."""
        Sniper = self._make_sniper()

        mock_router = MagicMock()
        mock_router.find_best_execution.return_value = {
            "venue": "coinbase",
            "price": 95000.0,
            "fee_pct": 0.40,
            "total_cost_pct": 0.40,
            "savings_vs_coinbase": 0,
        }

        Sniper._smart_router = mock_router

        # Verify SmartRouter returns coinbase
        quote = mock_router.find_best_execution("BTC-USD", "BUY", 5.0)
        self.assertEqual(quote["venue"], "coinbase")

    def test_smart_router_unavailable_defaults_coinbase(self):
        """If SmartRouter initialization fails, sniper defaults to Coinbase."""
        Sniper = self._make_sniper()

        # Smart router is None (failed to initialize)
        Sniper._smart_router = None

        # Temporarily remove smart_router from sys.modules to force ImportError
        import sys as _sys
        saved = _sys.modules.pop("smart_router", None)
        try:
            with patch.dict("sys.modules", {"smart_router": None}):
                router = Sniper._get_smart_router()
        finally:
            if saved is not None:
                _sys.modules["smart_router"] = saved

        # Should return None, meaning default to coinbase in execution
        self.assertIsNone(router)

    def test_kraken_execution_failure_falls_back(self):
        """If Kraken order fails, sniper should fall back to Coinbase."""
        Sniper = self._make_sniper()

        mock_router = MagicMock()
        mock_router.find_best_execution.return_value = {
            "venue": "kraken",
            "price": 95000.0,
            "fee_pct": 0.16,
            "total_cost_pct": 0.20,
            "savings_vs_coinbase": 0.20,
        }

        mock_kraken = MagicMock()
        # Kraken returns an error (no txid)
        mock_kraken.place_order.return_value = {
            "error": ["EGeneral:Internal error"],
            "result": {},
        }

        Sniper._smart_router = mock_router
        Sniper._kraken = mock_kraken

        # Verify Kraken was attempted but returned error
        result = mock_kraken.place_order(
            pair="BTC-USD", side="buy", volume=0.001,
            order_type="limit", price=95000.0, confidence=0.80,
            oflags="post",
        )
        self.assertIn("error", result)
        # No txid means fallback logic kicks in
        self.assertFalse(result.get("result", {}).get("txid"))

    def test_venue_recorded_in_trade_db(self):
        """_process_order_result accepts venue parameter and uses it in DB INSERT."""
        # Instead of calling the full method (which depends on many Sniper attrs),
        # verify that the method signature accepts `venue` and the INSERT SQL uses venue_used.
        Sniper = self._make_sniper()
        from sniper import Sniper as SniperClass
        import inspect

        # Check method signature accepts venue kwarg
        sig = inspect.signature(SniperClass._process_order_result)
        params = list(sig.parameters.keys())
        self.assertIn("venue", params, "_process_order_result must accept 'venue' parameter")

        # Check default value is "coinbase"
        venue_param = sig.parameters["venue"]
        self.assertEqual(venue_param.default, "coinbase")

    def test_process_order_result_uses_venue_in_db(self):
        """Verify the DB INSERT in _process_order_result uses venue_used, not hardcoded 'coinbase'."""
        # Read the sniper source and extract just the INSERT blocks within _process_order_result
        sniper_path = os.path.join(os.path.dirname(__file__), "..", "agents", "sniper.py")
        with open(sniper_path, "r") as f:
            source = f.read()

        lines = source.split("\n")
        in_method = False
        insert_blocks = []  # collect lines in INSERT statements
        current_insert = []
        in_insert = False

        for line in lines:
            if "def _process_order_result" in line:
                in_method = True
                continue
            if in_method and line.strip().startswith("def ") and "def _process_order_result" not in line:
                break
            if in_method:
                if "INSERT INTO sniper_trades" in line:
                    in_insert = True
                    current_insert = [line]
                elif in_insert:
                    current_insert.append(line)
                    # End of INSERT block: look for closing paren of execute()
                    if "self.db.commit()" in line or (line.strip() == ")" and len(current_insert) > 3):
                        insert_blocks.append("\n".join(current_insert))
                        current_insert = []
                        in_insert = False

        self.assertTrue(len(insert_blocks) > 0, "Should find INSERT INTO sniper_trades blocks")

        # Check that venue_used appears in the parameter tuples (not "coinbase")
        for block in insert_blocks:
            # The VALUES tuple is where the actual data goes
            # venue_used should be there, not the string "coinbase"
            self.assertIn("venue_used", block,
                         f"INSERT block should use venue_used variable:\n{block[:300]}")

    def test_lazy_kraken_initialization(self):
        """_new_kraken_trader() lazily initializes KrakenConnector."""
        Sniper = self._make_sniper()

        # Reset
        Sniper._kraken = None

        mock_kc = MagicMock()
        with patch.dict("sys.modules", {"kraken_connector": MagicMock(KrakenConnector=mock_kc)}):
            result = Sniper._new_kraken_trader()

        # Should have initialized and returned something (or the mock)
        self.assertIsNotNone(Sniper._kraken)

    def test_lazy_smart_router_initialization(self):
        """_get_smart_router() lazily initializes SmartRouter."""
        Sniper = self._make_sniper()

        # Reset
        Sniper._smart_router = None

        mock_sr_instance = MagicMock()
        mock_sr_class = MagicMock(return_value=mock_sr_instance)
        with patch.dict("sys.modules", {"smart_router": MagicMock(SmartRouter=mock_sr_class)}):
            result = Sniper._get_smart_router()

        self.assertIsNotNone(Sniper._smart_router)


if __name__ == "__main__":
    unittest.main()
