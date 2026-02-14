#!/usr/bin/env python3
"""Tests for trading execution path behavior in live_trader."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))
import live_trader


class TestLiveTraderExecution(unittest.TestCase):
    def test_place_buy_with_fallback_skips_all_routes_when_price_invalid(self):
        """If the computed limit price is unusable, skip trade entirely (maker-only, no market fallback)."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        with patch("live_trader.TRADER_DB", db_path):
            trader = live_trader.LiveTrader()
        trader.exchange = MagicMock()
        trader.exchange.place_order.side_effect = AssertionError("should not use market")
        trader.exchange.place_limit_order.side_effect = AssertionError("should not use limit")

        result = trader._place_buy_with_fallback(
            pair="BTC-USD",
            usd_amount=100.0,
            limit_price=-5.0,
            expected_edge_pct=1.0,
            signal_confidence=0.9,
            market_regime="UPTREND",
        )

        self.assertFalse(result["ok"])  # No trade when price invalid (maker-only, no taker fallback)
        trader.exchange.place_order.assert_not_called()
        trader.exchange.place_limit_order.assert_not_called()
        os.remove(db_path)

    def test_execute_trade_uses_regime_cache(self):
        """_execute_trade should not call _get_regime when regime was precomputed."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        fake_rc = MagicMock()
        fake_rc.approve_trade.return_value = (True, "APPROVED|trade_id=abc123", 100.0)
        fake_rc.complete_trade.return_value = None
        fake_rc.resolve_allocation.return_value = None

        with patch("live_trader.TRADER_DB", db_path), \
             patch("live_trader._live_risk_ctrl", fake_rc):
            trader = live_trader.LiveTrader()

            trader.pricefeed = MagicMock()
            trader.pricefeed.get_price.return_value = 30000.0
            trader.exchange = MagicMock()
            trader.exchange.get_order_book.return_value = {
                "pricebook": {
                    "bids": [{"price": "29990.0"}],
                    "asks": [{"price": "30010.0"}],
                }
            }
            trader._place_buy_with_fallback = MagicMock(return_value={
                "ok": True,
                "pair": "BTC-USD",
                "route": "market_ioc_fallback",
                "result": {"success_response": {"order_id": "order-xyz"}},
                "attempts": [],
            })
            trader._evaluate_no_loss_gate = MagicMock(return_value=({"approved": True}, {"costs_pct": 0.0}))

            with patch.object(live_trader.LiveTrader, "_get_regime", side_effect=AssertionError("should not call _get_regime")):
                trader._execute_trade(
                    "BTC-USD",
                    "BUY",
                    100.0,
                    {"signal_type": "test", "confidence": 0.9},
                    expected_edge_pct=0.5,
                    signal_confidence=0.9,
                    market_regime="UPTREND",
                    market_regime_info={"regime": "UPTREND"},
                    risk_portfolio_value=10000.0,
                )

        fake_rc.complete_trade.assert_called_once_with("abc123", status="filled")
        fake_rc.resolve_allocation.assert_called_once_with("live_trader", "BTC-USD", trade_id="abc123")
        os.remove(db_path)


if __name__ == "__main__":
    unittest.main()
