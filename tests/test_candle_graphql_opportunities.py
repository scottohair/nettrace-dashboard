#!/usr/bin/env python3
"""Tests for candle opportunity ranking."""

import os
import sqlite3
import tempfile
import time
import unittest


import candle_graphql_api as cg


class TestCandleOpportunities(unittest.TestCase):
    def test_opportunities_rank_high_momentum_pair_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "agg.db")
            agg = cg.CandleAggregator(db_path=db_path)

            conn = sqlite3.connect(db_path)
            now = int(time.time())
            rows = []
            for i in range(14):
                ts = now - ((14 - i) * 300)
                # Strong trend pair.
                o1 = 50.0 + (i * 1.2)
                rows.append(("AVAX-USD", "5m", ts, o1, o1 + 1.0, o1 - 0.5, o1 + 0.9, 20.0 + i, "test"))
                # Flat pair.
                o2 = 100.0 + (i * 0.03)
                rows.append(("BTC-USD", "5m", ts, o2, o2 + 0.1, o2 - 0.1, o2 + 0.02, 8.0, "test"))
            conn.executemany(
                """
                INSERT INTO aggregated_candles
                    (pair, timeframe, start_ts, open, high, low, close, volume, source, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                rows,
            )
            conn.commit()
            conn.close()

            ranked = agg.opportunities(
                limit=5,
                timeframe="5m",
                since_hours=48,
                min_samples=10,
                lookback_candles=12,
            )

            self.assertGreaterEqual(len(ranked), 2)
            self.assertEqual(ranked[0]["pair"], "AVAX-USD")
            self.assertGreater(
                float(ranked[0]["opportunity_score"]),
                float(ranked[1]["opportunity_score"]),
            )


if __name__ == "__main__":
    unittest.main()

