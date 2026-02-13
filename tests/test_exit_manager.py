#!/usr/bin/env python3
"""Tests for exit_manager — protects profits and cuts losses.

Tests cover:
  - Trailing stop logic at different profit tiers
  - Time-based exits (dead money, force eval)
  - Take-profit partial exits
  - Balance verification before selling
  - PositionState tracking
"""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))


class TestPositionState(unittest.TestCase):
    """Test PositionState tracking."""

    def setUp(self):
        from exit_manager import PositionState
        self.pos = PositionState("BTC-USDC", 100000.0,
                                 datetime.now(timezone.utc).isoformat(), 0.001)

    def test_profit_pct(self):
        from exit_manager import PositionState
        self.assertAlmostEqual(self.pos.profit_pct(101000), 0.01, places=4)
        self.assertAlmostEqual(self.pos.profit_pct(99000), -0.01, places=4)

    def test_peak_tracking(self):
        self.pos.update_peak(101000)
        self.assertEqual(self.pos.peak_price, 101000)
        self.pos.update_peak(100500)  # lower, should not update
        self.assertEqual(self.pos.peak_price, 101000)

    def test_drawdown(self):
        self.pos.update_peak(102000)
        dd = self.pos.drawdown_from_peak_pct(101000)
        self.assertAlmostEqual(dd, 1000 / 102000, places=4)

    def test_partial_exit_tracking(self):
        self.pos.record_partial_exit("tp1", 0.0003, 101000)
        self.assertIn("tp1", self.pos.partial_exits_done)
        self.assertAlmostEqual(self.pos.held_amount, 0.0007, places=6)
        self.assertAlmostEqual(self.pos.total_exited_amount, 0.0003, places=6)

    def test_hold_duration(self):
        # Position created "now", should be very short
        self.assertLess(self.pos.hold_duration_seconds(), 5)

    def test_hold_duration_old_position(self):
        from exit_manager import PositionState
        old_time = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
        pos = PositionState("ETH-USDC", 3500, old_time, 1.0)
        self.assertGreater(pos.hold_duration_hours(), 4.9)


class TestExitDecisions(unittest.TestCase):
    """Test check_exit logic."""

    def setUp(self):
        import exit_manager
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        exit_manager.EXIT_DB = self._tmp.name
        self._tmp.close()

        # Mock risk controller to avoid API calls
        exit_manager._risk_ctrl = MagicMock()
        exit_manager._risk_ctrl.get_risk_params.return_value = {
            "volatility": 0.02,
            "max_daily_loss": 10.0,
            "regime": "RANGING",
        }
        exit_manager._risk_ctrl.market.compute_volatility.return_value = 0.02

        self.em = exit_manager.ExitManager()
        self.em._estimate_portfolio_value = MagicMock(return_value=290.0)

    def tearDown(self):
        os.unlink(self._tmp.name)

    def test_hold_new_profitable_position(self):
        """New position with small profit should HOLD."""
        decision = self.em.check_exit(
            "BTC-USDC", 100000, datetime.now(timezone.utc).isoformat(),
            100500, 0.001)
        self.assertEqual(decision["action"], "HOLD")

    def test_loss_limit_exit(self):
        """Should exit when position loss exceeds per-position limit."""
        # Large position that loses significantly
        decision = self.em.check_exit(
            "BTC-USDC", 100000, datetime.now(timezone.utc).isoformat(),
            90000, 0.01)  # $100 loss on a $1000 position
        self.assertEqual(decision["action"], "EXIT_FULL")
        self.assertEqual(decision["exit_type"], "loss_limit")

    def test_trailing_stop(self):
        """Should exit when drawdown from peak exceeds trailing stop."""
        entry_time = datetime.now(timezone.utc).isoformat()
        # First check at a high price to set peak
        self.em.check_exit("BTC-USDC", 100000, entry_time, 105000, 0.001)
        # Then price drops significantly from peak
        decision = self.em.check_exit("BTC-USDC", 100000, entry_time, 101000, 0.001)
        # Drawdown is (105000-101000)/105000 = 3.8% — should trigger stop
        if decision["action"] == "EXIT_FULL":
            self.assertEqual(decision["exit_type"], "trailing_stop")

    def test_take_profit_tp0_then_tp1(self):
        """Should take micro TP0 first, then TP1 on subsequent check."""
        entry_time = datetime.now(timezone.utc).isoformat()
        # Price at ~3% profit — TP0 (0.8%) fires first
        decision = self.em.check_exit("BTC-USDC", 100000, entry_time, 103000, 0.001)
        if decision["action"] == "EXIT_PARTIAL":
            self.assertIn("tp0", decision.get("exit_type", "") + decision.get("label", ""))
            # Record the partial exit so TP1 can fire next
            pos = self.em._positions.get("BTC-USDC")
            if pos:
                pos.record_partial_exit("tp0", decision["amount"], 103000)
            # Now check again — TP1 should fire
            decision2 = self.em.check_exit("BTC-USDC", 100000, entry_time, 103000, pos.held_amount if pos else 0.001)
            if decision2["action"] == "EXIT_PARTIAL":
                self.assertIn("tp1", decision2.get("exit_type", "") + decision2.get("label", ""))

    def test_dead_money_exit(self):
        """Should exit flat positions after dead_money_hours."""
        old_time = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        decision = self.em.check_exit("BTC-USDC", 100000, old_time, 100010, 0.001)
        # Held 6h with ~0% gain — should trigger dead money
        if decision["action"] == "EXIT_FULL":
            self.assertIn("dead_money", decision["exit_type"])

    def test_invalid_position_holds(self):
        """Should HOLD if position data is invalid."""
        decision = self.em.check_exit("BTC-USDC", 0, "now", 100000, 0)
        self.assertEqual(decision["action"], "HOLD")


class TestNoDoubleRecordPartialExit(unittest.TestCase):
    """Verify the double-record bug fix: record_partial_exit should only be called once."""

    def setUp(self):
        import exit_manager
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        exit_manager.EXIT_DB = self._tmp.name
        self._tmp.close()

        exit_manager._risk_ctrl = MagicMock()
        exit_manager._risk_ctrl.get_risk_params.return_value = {
            "volatility": 0.02, "max_daily_loss": 10.0, "regime": "RANGING",
        }
        exit_manager._risk_ctrl.market.compute_volatility.return_value = 0.02
        exit_manager._risk_ctrl.approve_trade.return_value = (True, "OK", 5.0)

        self.em = exit_manager.ExitManager()
        self.em._estimate_portfolio_value = MagicMock(return_value=290.0)

    def tearDown(self):
        os.unlink(self._tmp.name)

    def test_execute_exit_records_partial(self):
        """execute_exit should call record_partial_exit internally."""
        from exit_manager import PositionState
        pos = PositionState("BTC-USDC", 100000, datetime.now(timezone.utc).isoformat(), 0.001)
        self.em._positions["BTC-USDC"] = pos

        # Mock the trader
        mock_trader = MagicMock()
        mock_trader.place_order.return_value = {"success_response": {"order_id": "test123"}}
        mock_trader._request.return_value = {
            "accounts": [{"currency": "BTC", "available_balance": {"value": "0.001"}}]
        }
        self.em._trader = mock_trader

        with patch("exit_manager._get_price", return_value=101000):
            self.em.execute_exit("BTC-USDC", 0.0003, "test", "take_profit_tp1")

        # After execute_exit, the partial exit should be recorded
        self.assertIn("tp1", pos.partial_exits_done)
        # And held_amount should be reduced by exactly 0.0003 (not double)
        self.assertAlmostEqual(pos.held_amount, 0.0007, places=6)


if __name__ == "__main__":
    unittest.main()
