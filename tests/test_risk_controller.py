#!/usr/bin/env python3
"""Tests for risk_controller — the most critical module for protecting capital.

Tests cover:
  - Dynamic scaling of trade limits, daily loss, reserves
  - Trade approval logic (daily loss, trend, volatility, rate limiting)
  - Cross-agent allocation tracking (SQLite-based atomic locking)
  - Kelly criterion calculations
  - Edge cases: zero portfolio, negative values, extreme volatility
"""

import os
import sys
import sqlite3
import tempfile
import unittest
from unittest.mock import patch, MagicMock

# Ensure agents dir is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

# Use a temp DB so tests don't touch real data
_test_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_test_db_path = _test_db.name
_test_db.close()


class TestDynamicScaling(unittest.TestCase):
    """Test that risk parameters scale correctly with portfolio size."""

    def setUp(self):
        import risk_controller
        risk_controller.DB_PATH = _test_db_path
        self.rc = risk_controller.RiskController()

    def test_max_trade_scales_with_portfolio(self):
        """Larger portfolios should have larger max trade sizes."""
        t50 = self.rc.max_trade_usd(50)
        t290 = self.rc.max_trade_usd(290)
        t1000 = self.rc.max_trade_usd(1000)
        t10000 = self.rc.max_trade_usd(10000)

        self.assertGreater(t290, t50)
        self.assertGreater(t1000, t290)
        self.assertGreater(t10000, t1000)

    def test_max_trade_at_290_is_reasonable(self):
        """At $290 portfolio, max trade should be ~$14-20, not $5."""
        t = self.rc.max_trade_usd(290)
        self.assertGreater(t, 10.0, f"Max trade ${t:.2f} too low for $290 portfolio")
        self.assertLess(t, 50.0, f"Max trade ${t:.2f} too high for $290 portfolio")

    def test_max_trade_zero_portfolio(self):
        """Zero portfolio should return 0."""
        self.assertEqual(self.rc.max_trade_usd(0), 0)

    def test_max_trade_reduced_by_high_volatility(self):
        """High volatility should reduce trade size."""
        normal = self.rc.max_trade_usd(290, volatility=0.02)
        high_vol = self.rc.max_trade_usd(290, volatility=0.10)
        self.assertGreater(normal, high_vol)

    def test_max_trade_reduced_in_downtrend(self):
        """Downtrend should reduce trade size."""
        neutral = self.rc.max_trade_usd(290, trend=0.0)
        downtrend = self.rc.max_trade_usd(290, trend=-0.8)
        self.assertGreater(neutral, downtrend)

    def test_daily_loss_scales_with_portfolio(self):
        """Daily loss limit should scale with portfolio."""
        d50 = self.rc.max_daily_loss(50)
        d290 = self.rc.max_daily_loss(290)
        d1000 = self.rc.max_daily_loss(1000)

        self.assertGreater(d290, d50)
        self.assertGreater(d1000, d290)

    def test_daily_loss_at_290_is_reasonable(self):
        """At $290, daily loss should be ~$8-15, not $2."""
        d = self.rc.max_daily_loss(290)
        self.assertGreater(d, 5.0, f"Daily loss ${d:.2f} too low for $290 portfolio")
        self.assertLess(d, 30.0, f"Daily loss ${d:.2f} too high for $290 portfolio")

    def test_min_reserve_scales(self):
        """Reserve should be a reasonable fraction of portfolio."""
        r = self.rc.min_reserve(290)
        self.assertGreater(r, 10.0, "Reserve too low for $290")
        self.assertLess(r, 100.0, "Reserve too high for $290")

    def test_kelly_fraction(self):
        """Kelly criterion should return reasonable bet fractions."""
        # 60% win rate, 2:1 payoff = positive Kelly
        k = self.rc.kelly_fraction(0.60, 2.0, 1.0)
        self.assertGreater(k, 0)
        self.assertLessEqual(k, 0.25)  # fractional Kelly caps at 25%

        # 30% win rate, 1:1 payoff = negative Kelly → 0
        k = self.rc.kelly_fraction(0.30, 1.0, 1.0)
        self.assertEqual(k, 0)


class TestTradeApproval(unittest.TestCase):
    """Test the central trade approval system."""

    def setUp(self):
        import risk_controller
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        risk_controller.DB_PATH = self._tmp.name
        self._tmp.close()

        # Mock market data to avoid real API calls
        self.rc = risk_controller.RiskController()
        self.rc.market.compute_volatility = MagicMock(return_value=0.02)
        self.rc.market.compute_trend = MagicMock(return_value=0.1)

    def tearDown(self):
        os.unlink(self._tmp.name)

    def test_approve_basic_buy(self):
        """A normal BUY should be approved."""
        approved, reason, size = self.rc.approve_trade("test_agent", "BTC-USDC", "BUY", 5.0, 290)
        self.assertTrue(approved, f"Should approve: {reason}")
        self.assertGreater(size, 0)

    def test_approve_sell_always(self):
        """SELL should almost always be approved (we want to exit positions)."""
        approved, reason, size = self.rc.approve_trade("test_agent", "BTC-USDC", "SELL", 5.0, 290)
        self.assertTrue(approved, f"SELL should be approved: {reason}")

    def test_block_after_daily_loss(self):
        """Should HARDSTOP after daily loss limit exceeded."""
        max_loss = self.rc.max_daily_loss(290)
        self.rc._daily_loss = max_loss + 1  # exceed limit

        approved, reason, _ = self.rc.approve_trade("test_agent", "BTC-USDC", "BUY", 5.0, 290)
        self.assertFalse(approved)
        self.assertIn("HARDSTOP", reason)

    def test_block_strong_downtrend(self):
        """Should block BUY in strong downtrend."""
        self.rc.market.compute_trend = MagicMock(return_value=-0.8)

        approved, reason, _ = self.rc.approve_trade("test_agent", "BTC-USDC", "BUY", 5.0, 290)
        self.assertFalse(approved)
        self.assertIn("downtrend", reason.lower())

    def test_downtrend_doesnt_block_sell(self):
        """Downtrend should NOT block SELL orders."""
        self.rc.market.compute_trend = MagicMock(return_value=-0.8)

        approved, reason, _ = self.rc.approve_trade("test_agent", "BTC-USDC", "SELL", 5.0, 290)
        self.assertTrue(approved, f"SELL should not be blocked by downtrend: {reason}")

    def test_rate_limiting(self):
        """Should stop approving after too many trades."""
        for i in range(200):
            approved, reason, _ = self.rc.approve_trade(f"agent_{i}", "BTC-USDC", "BUY", 1.0, 290)
            if not approved and "Trade limit" in reason:
                return  # test passes — rate limit kicked in
        self.fail("Rate limiting never triggered after 200 trades")

    def test_pending_allocation_tracking(self):
        """Should track pending allocations in DB for cross-process safety."""
        self.rc.approve_trade("agent_a", "BTC-USDC", "BUY", 10.0, 290)

        row = self.rc._db.execute(
            "SELECT COUNT(*) FROM pending_allocations WHERE agent='agent_a' AND status='pending'"
        ).fetchone()
        self.assertEqual(row[0], 1)

    def test_resolve_allocation(self):
        """Should mark allocations as resolved after trade completes."""
        self.rc.approve_trade("agent_a", "BTC-USDC", "BUY", 10.0, 290)
        self.rc.resolve_allocation("agent_a", "BTC-USDC")

        row = self.rc._db.execute(
            "SELECT status FROM pending_allocations WHERE agent='agent_a' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(row[0], "resolved")


class TestRequestTrade(unittest.TestCase):
    """Test the unified request_trade entry point."""

    def setUp(self):
        import risk_controller
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        risk_controller.DB_PATH = self._tmp.name
        self._tmp.close()

        self.rc = risk_controller.RiskController()
        self.rc.market.compute_volatility = MagicMock(return_value=0.02)
        self.rc.market.compute_trend = MagicMock(return_value=0.1)

    def tearDown(self):
        os.unlink(self._tmp.name)

    def test_request_trade_with_portfolio(self):
        """request_trade with explicit portfolio value."""
        approved, reason, size = self.rc.request_trade("sniper", "BTC-USDC", "BUY", 5.0, 290)
        self.assertTrue(approved, f"Should approve: {reason}")


class TestGetRiskParams(unittest.TestCase):
    """Test get_risk_params returns all expected keys."""

    def setUp(self):
        import risk_controller
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        risk_controller.DB_PATH = self._tmp.name
        self._tmp.close()

        self.rc = risk_controller.RiskController()
        self.rc.market.compute_volatility = MagicMock(return_value=0.02)
        self.rc.market.compute_trend = MagicMock(return_value=0.1)
        self.rc.market.compute_momentum = MagicMock(return_value=0.05)

    def tearDown(self):
        os.unlink(self._tmp.name)

    def test_all_keys_present(self):
        params = self.rc.get_risk_params(290, "BTC-USDC")
        expected_keys = [
            "portfolio_value", "volatility", "trend", "momentum",
            "max_trade_usd", "max_daily_loss", "min_reserve",
            "max_position_pct", "daily_loss_so_far", "trades_today",
            "can_buy", "regime",
        ]
        for key in expected_keys:
            self.assertIn(key, params, f"Missing key: {key}")

    def test_regime_detection(self):
        """Regime should be UPTREND/DOWNTREND/RANGING based on trend."""
        self.rc.market.compute_trend = MagicMock(return_value=0.5)
        params = self.rc.get_risk_params(290, "BTC-USDC")
        self.assertEqual(params["regime"], "UPTREND")

        self.rc.market.compute_trend = MagicMock(return_value=-0.5)
        params = self.rc.get_risk_params(290, "BTC-USDC")
        self.assertEqual(params["regime"], "DOWNTREND")


if __name__ == "__main__":
    unittest.main()
