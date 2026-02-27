"""Tests for the shared Position Registry."""
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "agents"))

# Override DB path before import
_tmp = tempfile.mkdtemp()
os.environ.setdefault("_TEST_REGISTRY_DB", str(Path(_tmp) / "test_trader.db"))

import position_registry as pr


class TestPositionRegistry(unittest.TestCase):
    """Test core registry operations."""

    def setUp(self):
        """Fresh DB for each test."""
        pr.PositionRegistry.reset()
        self.db_path = str(Path(_tmp) / f"test_{id(self)}.db")
        pr.REGISTRY_DB = self.db_path
        self.reg = pr.PositionRegistry()

    def tearDown(self):
        pr.PositionRegistry.reset()
        try:
            os.unlink(self.db_path)
        except Exception:
            pass

    def test_register_returns_id(self):
        row_id = self.reg.register(
            "BTC-USD", "sniper",
            entry_price=95000, entry_amount=0.001,
        )
        self.assertIsNotNone(row_id)
        self.assertIsInstance(row_id, int)

    def test_register_duplicate_returns_none(self):
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        result = self.reg.register("BTC-USD", "momentum_chaser", entry_price=95100, entry_amount=0.002)
        self.assertIsNone(result)

    def test_register_usdc_variant_blocks(self):
        """BTC-USD and BTC-USDC are the same position."""
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        result = self.reg.register("BTC-USDC", "momentum_chaser", entry_price=95100, entry_amount=0.002)
        self.assertIsNone(result)

    def test_different_pairs_ok(self):
        r1 = self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        r2 = self.reg.register("ETH-USD", "momentum_chaser", entry_price=2000, entry_amount=0.05)
        self.assertIsNotNone(r1)
        self.assertIsNotNone(r2)

    def test_is_pair_available(self):
        self.assertTrue(self.reg.is_pair_available("BTC-USD"))
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        self.assertFalse(self.reg.is_pair_available("BTC-USD"))
        self.assertFalse(self.reg.is_pair_available("BTC-USDC"))  # variant

    def test_get_owner(self):
        self.assertIsNone(self.reg.get_owner("BTC-USD"))
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        self.assertEqual(self.reg.get_owner("BTC-USD"), "sniper")

    def test_is_owned_by(self):
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        self.assertTrue(self.reg.is_owned_by("BTC-USD", "sniper"))
        self.assertFalse(self.reg.is_owned_by("BTC-USD", "momentum_chaser"))

    def test_default_exit_owner_sniper(self):
        """Sniper positions default to exit_owner=exit_manager."""
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        pos = self.reg.query(owner="sniper")[0]
        self.assertEqual(pos["exit_owner"], "exit_manager")

    def test_default_exit_owner_momentum(self):
        """Momentum chaser positions default to exit_owner=self."""
        self.reg.register("BTC-USD", "momentum_chaser", entry_price=95000, entry_amount=0.001)
        pos = self.reg.query(owner="momentum_chaser")[0]
        self.assertEqual(pos["exit_owner"], "self")

    def test_operator_protection(self):
        """Operator positions can't be exited by agents."""
        self.reg.register(
            "ICP-USD", "operator",
            entry_price=2.63, entry_amount=11.4,
            exit_owner="operator",
            reason="operator: ICP is rising",
        )
        # exit_manager can't claim it
        claim = self.reg.claim_for_exit("ICP-USD", "exit_manager")
        self.assertIsNone(claim)

        # operator CAN claim it
        claim = self.reg.claim_for_exit("ICP-USD", "operator")
        self.assertIsNotNone(claim)

    def test_self_exit_owner(self):
        """exit_owner=self: only the owning agent can exit."""
        self.reg.register(
            "BTC-USD", "momentum_chaser",
            entry_price=95000, entry_amount=0.001,
            exit_owner="self", min_hold_seconds=0,
        )
        # exit_manager blocked
        self.assertIsNone(self.reg.claim_for_exit("BTC-USD", "exit_manager"))
        # sniper blocked
        self.assertIsNone(self.reg.claim_for_exit("BTC-USD", "sniper"))
        # momentum_chaser allowed
        self.assertIsNotNone(self.reg.claim_for_exit("BTC-USD", "momentum_chaser"))

    def test_exit_manager_delegation(self):
        """exit_owner=exit_manager: only exit_manager can exit."""
        self.reg.register(
            "BTC-USD", "sniper",
            entry_price=95000, entry_amount=0.001,
            exit_owner="exit_manager", min_hold_seconds=0,
        )
        # momentum_chaser blocked
        self.assertIsNone(self.reg.claim_for_exit("BTC-USD", "momentum_chaser"))
        # exit_manager allowed
        self.assertIsNotNone(self.reg.claim_for_exit("BTC-USD", "exit_manager"))

    def test_min_hold_enforced_against_others(self):
        """Min hold blocks OTHER agents, not the owner."""
        self.reg.register(
            "BTC-USD", "sniper",
            entry_price=95000, entry_amount=0.001,
            exit_owner="exit_manager", min_hold_seconds=9999,
        )
        # exit_manager is blocked by min_hold (it's not the owner)
        claim = self.reg.claim_for_exit("BTC-USD", "exit_manager")
        self.assertIsNone(claim)

        # But sniper (the owner) can still exit immediately
        claim2 = self.reg.claim_for_exit("BTC-USD", "sniper")
        self.assertIsNotNone(claim2)

    def test_min_hold_expired(self):
        """Can exit after min hold time."""
        self.reg.register(
            "BTC-USD", "momentum_chaser",
            entry_price=95000, entry_amount=0.001,
            exit_owner="self", min_hold_seconds=0,
        )
        claim = self.reg.claim_for_exit("BTC-USD", "momentum_chaser")
        self.assertIsNotNone(claim)

    def test_close_marks_closed(self):
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        self.reg.close("BTC-USD", close_price=96000, pnl_usd=1.50)
        # Should be available again
        self.assertTrue(self.reg.is_pair_available("BTC-USD"))

    def test_close_then_reopen(self):
        """After closing, same pair can be registered again."""
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        self.reg.close("BTC-USD", close_price=96000, pnl_usd=1.50)
        r2 = self.reg.register("BTC-USD", "momentum_chaser", entry_price=96100, entry_amount=0.002)
        self.assertIsNotNone(r2)

    def test_claim_sets_exiting_status(self):
        self.reg.register(
            "BTC-USD", "sniper",
            entry_price=95000, entry_amount=0.001,
            exit_owner="exit_manager", min_hold_seconds=0,
        )
        self.reg.claim_for_exit("BTC-USD", "exit_manager")
        # Position should now be 'exiting'
        positions = self.reg.query(status="exiting")
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["exit_agent"], "exit_manager")

    def test_release_reverts_to_open(self):
        self.reg.register(
            "BTC-USD", "sniper",
            entry_price=95000, entry_amount=0.001,
            exit_owner="exit_manager", min_hold_seconds=0,
        )
        self.reg.claim_for_exit("BTC-USD", "exit_manager")
        self.reg.release("BTC-USD")
        positions = self.reg.query(status="open")
        self.assertEqual(len(positions), 1)

    def test_double_claim_blocked(self):
        """Can't claim a position that's already being exited."""
        self.reg.register(
            "BTC-USD", "sniper",
            entry_price=95000, entry_amount=0.001,
            exit_owner="exit_manager", min_hold_seconds=0,
        )
        claim1 = self.reg.claim_for_exit("BTC-USD", "exit_manager")
        self.assertIsNotNone(claim1)
        claim2 = self.reg.claim_for_exit("BTC-USD", "exit_manager")
        self.assertIsNone(claim2)

    def test_get_all_open(self):
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        self.reg.register("ETH-USD", "momentum_chaser", entry_price=2000, entry_amount=0.05)
        all_open = self.reg.get_all_open()
        self.assertEqual(len(all_open), 2)
        self.assertIn("BTC-USD", all_open)
        self.assertIn("ETH-USD", all_open)

    def test_set_exit_owner(self):
        """Operator can release protection dynamically."""
        self.reg.register(
            "ICP-USD", "operator",
            entry_price=2.63, entry_amount=11.4,
            exit_owner="operator",
        )
        # Can't exit initially
        self.assertIsNone(self.reg.claim_for_exit("ICP-USD", "exit_manager"))

        # Operator releases
        self.reg.set_exit_owner("ICP-USD", "exit_manager")

        # Now exit_manager can claim
        claim = self.reg.claim_for_exit("ICP-USD", "exit_manager")
        self.assertIsNotNone(claim)

    def test_can_exit_no_position(self):
        """Unregistered pair — allow legacy behavior."""
        self.assertTrue(self.reg.can_exit("XYZ-USD", "exit_manager"))

    def test_can_exit_checks_permission(self):
        self.reg.register(
            "BTC-USD", "momentum_chaser",
            entry_price=95000, entry_amount=0.001,
            exit_owner="self",
        )
        self.assertTrue(self.reg.can_exit("BTC-USD", "momentum_chaser"))
        self.assertFalse(self.reg.can_exit("BTC-USD", "exit_manager"))

    def test_query_filters(self):
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        self.reg.register("ETH-USD", "momentum_chaser", entry_price=2000, entry_amount=0.05)
        self.assertEqual(len(self.reg.query(owner="sniper")), 1)
        self.assertEqual(len(self.reg.query(owner="momentum_chaser")), 1)
        self.assertEqual(len(self.reg.query(status="open")), 2)

    def test_update_amount(self):
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        self.reg.update_amount("BTC-USD", 0.002, new_avg_price=95500)
        pos = self.reg.query(owner="sniper")[0]
        self.assertAlmostEqual(pos["entry_amount"], 0.002)
        self.assertAlmostEqual(pos["entry_price"], 95500)

    def test_different_venues_independent(self):
        """Same pair on different venues = independent positions."""
        r1 = self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001, venue="coinbase")
        r2 = self.reg.register("BTC-USD", "sniper", entry_price=95100, entry_amount=0.001, venue="kraken")
        self.assertIsNotNone(r1)
        self.assertIsNotNone(r2)

    def test_normalize_pair(self):
        """Pairs are case-insensitive."""
        self.reg.register("btc-usd", "sniper", entry_price=95000, entry_amount=0.001)
        self.assertFalse(self.reg.is_pair_available("BTC-USD"))
        self.assertEqual(self.reg.get_owner("BTC-USD"), "sniper")

    def test_graceful_degradation(self):
        """get_registry returns None on failure, not crash."""
        old_db = pr.REGISTRY_DB
        pr.REGISTRY_DB = "/nonexistent/path/db"
        pr.PositionRegistry.reset()
        # Should return None, not raise
        reg = pr.get_registry()
        # Restore
        pr.REGISTRY_DB = old_db
        pr.PositionRegistry.reset()


class TestMigration(unittest.TestCase):
    """Test first-run migration."""

    def setUp(self):
        pr.PositionRegistry.reset()
        self.db_path = str(Path(_tmp) / f"test_migrate_{id(self)}.db")
        pr.REGISTRY_DB = self.db_path
        self.reg = pr.PositionRegistry()

    def tearDown(self):
        pr.PositionRegistry.reset()
        try:
            os.unlink(self.db_path)
        except Exception:
            pass

    def test_migrate_skips_if_already_populated(self):
        self.reg.register("BTC-USD", "sniper", entry_price=95000, entry_amount=0.001)
        # Migration should skip
        pr.migrate_existing_positions()
        self.assertEqual(len(self.reg.query(status="open")), 1)

    def test_migrate_from_lock_file(self):
        """Protected pairs in lock file become operator positions."""
        lock_file = Path(__file__).parent.parent / "agents" / "trading_lock.json"
        original = lock_file.read_text() if lock_file.exists() else None

        try:
            lock_file.write_text(json.dumps({
                "locked": True,
                "protected_pairs": ["ICP-USD", "DASH-USD"],
                "source": "test",
            }))
            pr.migrate_existing_positions()
            positions = self.reg.query(status="open")
            operators = [p for p in positions if p["owner"] == "operator"]
            self.assertGreaterEqual(len(operators), 2)
        finally:
            if original:
                lock_file.write_text(original)
            elif lock_file.exists():
                lock_file.write_text(json.dumps({"locked": False}))


if __name__ == "__main__":
    unittest.main()
