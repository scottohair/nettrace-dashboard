#!/usr/bin/env python3
"""Tests for strict profitable-exit gating in strike teams."""

import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import strike_teams as st  # noqa: E402


def _coingecko_mock_response(price):
    response = MagicMock()
    response.read.return_value = json.dumps({"bitcoin": {"usd": price}}).encode()
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    return response


class TestStrikeTeamExitValidation(unittest.TestCase):
    def test_validate_profitable_exit_rejects_unprofitable_target(self):
        team = st.StrikeTeam()
        ok, reason = team.validate_profitable_exit(
            "BTC-USD",
            entry_price=100.0,
            direction="BUY",
            analysis={"take_profit": 101.0},
        )
        self.assertFalse(ok)
        self.assertIn("below_required", reason)

    def test_validate_profitable_exit_accepts_profitable_target(self):
        team = st.StrikeTeam()
        ok, reason = team.validate_profitable_exit(
            "BTC-USD",
            entry_price=100.0,
            direction="BUY",
            analysis={"take_profit": 102.0},
        )
        self.assertTrue(ok)
        self.assertIn("target_ok", reason)

    def test_execute_blocks_buy_without_profitable_exit(self):
        team = st.StrikeTeam()
        result = team.execute(
            "BTC-USD",
            {
                "approved": True,
                "direction": "BUY",
                "size_usd": 5.0,
                "entry_price": 100.0,
                "take_profit": 100.5,
                "confidence": 0.9,
                "confirming_signals": 3,
                "regime": "neutral",
            },
        )
        self.assertIsNone(result)
        self.assertEqual(team.trades_executed, 0)

    def test_buy_is_throttled_when_sell_close_completion_is_low(self):
        with patch.object(st, "EXECUTION_HEALTH_GATE", False), patch.object(
            st, "BUY_THROTTLE_ON_SELL_GAP", True
        ), patch.object(st, "SELL_CLOSE_MIN_OBS", 4), patch.object(
            st, "SELL_CLOSE_TARGET_RATE", 0.75
        ):
            team = st.StrikeTeam()
            team._record_sell_completion(False)
            team._record_sell_completion(False)
            team._record_sell_completion(True)
            team._record_sell_completion(False)
            result = team.execute(
                "BTC-USD",
                {
                    "approved": True,
                    "direction": "BUY",
                    "size_usd": 5.0,
                    "entry_price": 100.0,
                    "take_profit": 102.0,
                    "confidence": 0.9,
                    "confirming_signals": 3,
                    "regime": "neutral",
                },
            )
            self.assertIsNone(result)
            self.assertEqual(team.buy_throttled, 1)

    def test_status_reports_sell_close_completion_metrics(self):
        team = st.StrikeTeam()
        team._record_sell_completion(True)
        team._record_sell_completion(False)
        status = team.status()
        self.assertEqual(status["sell_attempted"], 2)
        self.assertEqual(status["sell_completed"], 1)
        self.assertEqual(status["sell_failed"], 1)
        self.assertAlmostEqual(status["sell_close_completion_rate"], 0.5, places=9)

    def test_execute_blocks_buy_when_execution_health_unhealthy(self):
        with patch.object(st, "EXECUTION_HEALTH_GATE", True), patch.object(
            st, "EXECUTION_HEALTH_BUY_ONLY", True
        ), patch.object(
            st,
            "_execution_health_status",
            lambda *_args, **_kwargs: {"green": False, "reason": "dns_unhealthy"},
        ):
            team = st.StrikeTeam()
            result = team.execute(
                "BTC-USD",
                {
                    "approved": True,
                    "direction": "BUY",
                    "size_usd": 5.0,
                    "entry_price": 100.0,
                    "take_profit": 102.0,
                    "confidence": 0.9,
                    "confirming_signals": 3,
                    "regime": "neutral",
                },
            )
            self.assertIsNone(result)
            self.assertEqual(team.exec_health_blocked, 1)

    def test_close_first_plan_forces_sell_when_open_inventory_and_close_rate_low(self):
        with patch.object(st, "CLOSE_FIRST_MODE_ENABLED", True), patch.object(
            st, "BUY_THROTTLE_ON_SELL_GAP", True
        ), patch.object(st, "SELL_CLOSE_MIN_OBS", 2), patch.object(
            st, "SELL_CLOSE_TARGET_RATE", 0.75
        ):
            team = st.StrikeTeam()
            team._record_sell_completion(False)
            team._record_sell_completion(True)  # 50% completion < 75% target
            with patch.object(
                team,
                "_position_snapshot",
                return_value={"base_qty": 0.15, "source": "test"},
            ):
                plan = team._close_first_plan(
                    "BTC-USD",
                    {
                        "approved": True,
                        "direction": "BUY",
                        "entry_price": 100.0,
                        "size_usd": 5.0,
                    },
                )
            self.assertTrue(plan["active"])
            self.assertIn(plan["trigger"], {"buy_throttle", "sell_completion_rate_low"})
            self.assertGreaterEqual(float(plan["sell_size_usd"]), 15.0)

    def test_execute_blocks_sell_without_inventory(self):
        with patch.object(st, "EXECUTION_HEALTH_GATE", False):
            team = st.StrikeTeam()
            with patch.object(
                team,
                "_position_snapshot",
                return_value={"base_qty": 0.0, "source": "test"},
            ):
                result = team.execute(
                    "BTC-USD",
                    {
                        "approved": True,
                        "direction": "SELL",
                        "size_usd": 5.0,
                        "entry_price": 100.0,
                        "confidence": 0.9,
                        "confirming_signals": 3,
                        "regime": "neutral",
                    },
                )
            self.assertIsNone(result)
            self.assertEqual(team.sell_no_inventory_blocked, 1)

    def test_arbitrage_scout_filters_by_basis_regime(self):
        st._reset_arbitrage_basis_state()
        team = st.ArbitrageStrike()
        with patch.object(st, "_fetch_price", return_value=100.0), patch.object(
            st, "ARBITRAGE_BASIS_MIN_HISTORY", 3
        ), patch.object(st, "ARBITRAGE_BASIS_Z_REQUIRED", 1.05), patch.object(
            st, "ARBITRAGE_BASIS_STRESS_Z", 1.90
        ), patch.object(
            st, "ARBITRAGE_MIN_SPREAD_THRESHOLD", 0.008
        ), patch(
            "strike_teams.urllib.request.urlopen",
            side_effect=[
                _coingecko_mock_response(99.9),
                _coingecko_mock_response(99.9),
                _coingecko_mock_response(89.0),
            ],
        ):
            self.assertFalse(team.scout("BTC-USD").get("signal"))
            self.assertFalse(team.scout("BTC-USD").get("signal"))
            result = team.scout("BTC-USD")

        self.assertTrue(result["signal"])
        self.assertIn(
            result.get("basis", {}).get("regime"),
            {"normal_coinbase_premium", "stressed_coinbase_premium"},
        )
        self.assertEqual(result["basis"]["sample_count"], 3)

    def test_arbitrage_scout_suppresses_flat_basis_regime(self):
        st._reset_arbitrage_basis_state()
        team = st.ArbitrageStrike()
        with patch.object(st, "_fetch_price", return_value=100.0), patch.object(
            st, "ARBITRAGE_BASIS_MIN_HISTORY", 3
        ), patch.object(st, "ARBITRAGE_BASIS_Z_REQUIRED", 1.05), patch.object(
            st, "ARBITRAGE_BASIS_STRESS_Z", 1.90
        ), patch.object(
            st, "ARBITRAGE_MIN_SPREAD_THRESHOLD", 0.008
        ), patch(
            "strike_teams.urllib.request.urlopen",
            side_effect=[
                _coingecko_mock_response(99.0),
                _coingecko_mock_response(99.0),
                _coingecko_mock_response(99.0),
            ],
        ):
            team.scout("BTC-USD")
            team.scout("BTC-USD")
            result = team.scout("BTC-USD")

        self.assertFalse(result["signal"])
        self.assertIsNone(result.get("basis"))

    def test_fetch_candles_uses_local_aggregator_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "candle_aggregator.db"
            conn = sqlite3.connect(str(db_path))
            conn.execute(
                """
                CREATE TABLE aggregated_candles (
                    pair TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    start_ts INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL DEFAULT 0.0,
                    source TEXT NOT NULL DEFAULT 'test',
                    fetched_at TEXT
                )
                """
            )
            now = int(time.time())
            for i in range(6):
                ts = now - ((5 - i) * 60)
                open_px = 100.0 + (i * 0.1)
                conn.execute(
                    """
                    INSERT INTO aggregated_candles
                        (pair, timeframe, start_ts, open, high, low, close, volume, source, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'test', datetime('now'))
                    """,
                    ("BTC-USD", "1m", ts, open_px, open_px + 0.2, open_px - 0.2, open_px + 0.05, 10.0 + i),
                )
            conn.commit()
            conn.close()

            with patch.object(st, "CANDLE_AGG_DB_PATH", db_path), patch.object(
                st, "STRIKE_CANDLE_FALLBACK_ENABLED", True
            ), patch(
                "strike_teams.urllib.request.urlopen", side_effect=RuntimeError("offline")
            ):
                candles = st._fetch_candles("BTC-USD", granularity=60, limit=4)

            self.assertEqual(len(candles), 4)
            self.assertGreaterEqual(int(candles[0][0]), int(candles[-1][0]))
            self.assertAlmostEqual(float(candles[0][4]), 100.55, places=6)

    def test_dynamic_pair_universe_prefers_scored_feed_pairs(self):
        with tempfile.TemporaryDirectory() as tmp:
            feed_path = Path(tmp) / "candle_feed_latest.json"
            base_ts = int(time.time()) - (30 * 60)
            points = []
            for i in range(20):
                ts = base_ts + (i * 60)
                # Mild BTC drift.
                btc_open = 100.0 + (i * 0.02)
                points.append(
                    {
                        "pair": "BTC-USD",
                        "timeframe": "1m",
                        "start_ts": ts,
                        "open": btc_open,
                        "high": btc_open + 0.05,
                        "low": btc_open - 0.05,
                        "close": btc_open + 0.01,
                        "volume": 4.0,
                    }
                )
                # Stronger AVAX move with rising activity.
                avax_open = 50.0 + (i * 0.35)
                points.append(
                    {
                        "pair": "AVAX-USD",
                        "timeframe": "1m",
                        "start_ts": ts,
                        "open": avax_open,
                        "high": avax_open + 0.4,
                        "low": avax_open - 0.2,
                        "close": avax_open + 0.3,
                        "volume": 8.0 + i,
                    }
                )
            feed_path.write_text(json.dumps({"points": points}))

            st._PAIR_UNIVERSE_CACHE.update({"ts": 0.0, "hf": [], "lf": [], "source": "none"})
            with patch.object(st, "CANDLE_FEED_PATH", feed_path), patch.object(
                st, "STRIKE_DYNAMIC_PAIR_SELECTION", True
            ), patch.object(st, "STRIKE_DYNAMIC_PAIRS_LIMIT_HF", 4), patch.object(
                st, "STRIKE_DYNAMIC_PAIRS_REFRESH_SECONDS", 0
            ), patch.object(st, "STRIKE_DYNAMIC_PAIR_MIN_POINTS", 6), patch.object(
                st, "STRIKE_DYNAMIC_PAIR_LOOKBACK", 16
            ):
                pairs, source = st._dynamic_pair_universe("HF", ["BTC-USD"])

            self.assertEqual(source, "candle_feed")
            self.assertIn("AVAX-USD", pairs)
