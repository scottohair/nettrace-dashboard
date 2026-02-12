"""Quant signal generation engine for NetTrace."""

import json
import logging
import os
import sqlite3
import statistics
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("nettrace.quant")

DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent / "traceroute.db"))


class QuantEngine:
    """Analyze latency data and generate lightweight quant signals."""

    ROUTE_CHANGE_LOOKBACK_HOURS = 12
    METRIC_LOOKBACK_HOURS = 48
    MAX_POINTS_PER_HOST = 64
    ROLLING_WINDOW = 12
    ZSCORE_THRESHOLD = 2.0
    TREND_POINTS = 12
    TREND_MIN_POINTS = 6
    TREND_SLOPE_THRESHOLD = 0.75
    TREND_R2_THRESHOLD = 0.4

    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH

    def run(self, emit_stdout=True):
        """Generate signals, persist to DB, and optionally emit JSON to stdout."""
        db = sqlite3.connect(self.db_path)
        db.row_factory = sqlite3.Row
        inserted = []

        try:
            self._ensure_schema(db)
            has_scan_metrics = self._table_exists(db, "scan_metrics")
            has_route_changes = self._table_exists(db, "route_changes")
            history = self._load_crypto_history(db) if has_scan_metrics else {}

            candidate_signals = []
            if has_scan_metrics and has_route_changes:
                candidate_signals.extend(self._route_change_signals(db))

            if has_scan_metrics:
                anomaly_signals, zscore_map = self._rtt_anomaly_signals(history)
                candidate_signals.extend(anomaly_signals)
                candidate_signals.extend(self._cross_exchange_signals(history, zscore_map))
                candidate_signals.extend(self._trend_signals(history))

            for signal in candidate_signals:
                if self._insert_signal(db, signal):
                    inserted.append(signal)

            db.commit()
        finally:
            db.close()

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(inserted),
            "signals": inserted,
        }

        if emit_stdout:
            print(json.dumps(payload))

        return inserted

    def _ensure_schema(self, db):
        db.executescript("""
            CREATE TABLE IF NOT EXISTS quant_signals (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              signal_type TEXT,
              target_host TEXT,
              target_name TEXT,
              direction TEXT,
              confidence REAL,
              details_json TEXT,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_quant_signals_created ON quant_signals(created_at);
            CREATE INDEX IF NOT EXISTS idx_quant_signals_type_time ON quant_signals(signal_type, created_at);
            CREATE INDEX IF NOT EXISTS idx_quant_signals_host_time ON quant_signals(target_host, created_at);
        """)

    def _load_crypto_history(self, db):
        rows = db.execute(
            """
            SELECT id, target_host, target_name, total_rtt, created_at
            FROM scan_metrics
            WHERE total_rtt IS NOT NULL
              AND lower(COALESCE(category, '')) LIKE '%crypto%'
              AND created_at >= datetime('now', ?)
            ORDER BY target_host ASC, id DESC
            """,
            (f"-{self.METRIC_LOOKBACK_HOURS} hours",),
        ).fetchall()

        history = {}
        for row in rows:
            host = row["target_host"]
            bucket = history.setdefault(
                host,
                {"target_name": row["target_name"] or host, "points": []},
            )
            if len(bucket["points"]) >= self.MAX_POINTS_PER_HOST:
                continue
            bucket["points"].append(
                {
                    "id": row["id"],
                    "rtt": float(row["total_rtt"]),
                    "created_at": row["created_at"],
                }
            )
        return history

    def _route_change_signals(self, db):
        rows = db.execute(
            """
            SELECT rc.id, rc.target_host, COALESCE(rc.target_name, lm.target_name) AS target_name,
                   rc.rtt_delta, rc.detected_at
            FROM route_changes rc
            JOIN (
                SELECT m.target_host, m.target_name, m.category
                FROM scan_metrics m
                INNER JOIN (
                    SELECT target_host, MAX(id) AS max_id
                    FROM scan_metrics
                    GROUP BY target_host
                ) latest ON latest.max_id = m.id
            ) lm ON lm.target_host = rc.target_host
            WHERE lower(COALESCE(lm.category, '')) LIKE '%crypto%'
              AND rc.detected_at >= datetime('now', ?)
            ORDER BY rc.id DESC
            LIMIT 300
            """,
            (f"-{self.ROUTE_CHANGE_LOOKBACK_HOURS} hours",),
        ).fetchall()

        signals = []
        for row in rows:
            rtt_delta = row["rtt_delta"]
            if rtt_delta is not None and abs(float(rtt_delta)) < 1.0:
                continue

            if rtt_delta is None:
                direction = "route_shift"
                confidence = 0.55
            elif rtt_delta >= 0:
                direction = "latency_up"
                confidence = min(0.99, 0.55 + abs(float(rtt_delta)) / 80.0)
            else:
                direction = "latency_down"
                confidence = min(0.99, 0.55 + abs(float(rtt_delta)) / 80.0)

            signals.append(
                {
                    "signal_type": "route_change_crypto_arb",
                    "target_host": row["target_host"],
                    "target_name": row["target_name"] or row["target_host"],
                    "direction": direction,
                    "confidence": round(confidence, 4),
                    "details": {
                        "route_change_id": row["id"],
                        "detected_at": row["detected_at"],
                        "rtt_delta_ms": self._round_or_none(rtt_delta),
                        "reason": "Route change detected on crypto exchange endpoint",
                    },
                }
            )
        return signals

    def _rtt_anomaly_signals(self, history):
        signals = []
        zscore_map = {}

        for host, data in history.items():
            z = self._latest_zscore(data["points"])
            if not z:
                continue
            zscore_map[host] = z

            if abs(z["zscore"]) <= self.ZSCORE_THRESHOLD:
                continue

            confidence = min(0.99, abs(z["zscore"]) / 4.0)
            signals.append(
                {
                    "signal_type": "rtt_anomaly",
                    "target_host": host,
                    "target_name": data["target_name"],
                    "direction": "up" if z["zscore"] > 0 else "down",
                    "confidence": round(confidence, 4),
                    "details": {
                        "latest_rtt_ms": self._round_or_none(z["latest_rtt"]),
                        "rolling_mean_ms": self._round_or_none(z["mean"]),
                        "rolling_stddev_ms": self._round_or_none(z["stdev"]),
                        "zscore": self._round_or_none(z["zscore"]),
                        "window_size": self.ROLLING_WINDOW,
                        "measured_at": z["created_at"],
                    },
                }
            )

        return signals, zscore_map

    def _cross_exchange_signals(self, history, zscore_map):
        binance_host = self._host_for_name(history, "binance")
        coinbase_host = self._host_for_name(history, "coinbase")
        if not binance_host or not coinbase_host:
            return []

        binance_z = zscore_map.get(binance_host) or self._latest_zscore(history[binance_host]["points"])
        coinbase_z = zscore_map.get(coinbase_host) or self._latest_zscore(history[coinbase_host]["points"])
        if not binance_z or not coinbase_z:
            return []

        binance_rtt = binance_z["latest_rtt"]
        coinbase_rtt = coinbase_z["latest_rtt"]
        rtt_gap = binance_rtt - coinbase_rtt

        signals = []

        if binance_z["zscore"] > self.ZSCORE_THRESHOLD and abs(coinbase_z["zscore"]) < self.ZSCORE_THRESHOLD:
            confidence = self._differential_confidence(binance_z["zscore"], coinbase_z["zscore"], abs(rtt_gap))
            signals.append(
                {
                    "signal_type": "cross_exchange_latency_diff",
                    "target_host": binance_host,
                    "target_name": history[binance_host]["target_name"],
                    "direction": "favor_coinbase",
                    "confidence": round(confidence, 4),
                    "details": {
                        "leader_exchange": history[coinbase_host]["target_name"],
                        "lagging_exchange": history[binance_host]["target_name"],
                        "leader_host": coinbase_host,
                        "lagging_host": binance_host,
                        "leader_rtt_ms": self._round_or_none(coinbase_rtt),
                        "lagging_rtt_ms": self._round_or_none(binance_rtt),
                        "rtt_gap_ms": self._round_or_none(rtt_gap),
                        "binance_zscore": self._round_or_none(binance_z["zscore"]),
                        "coinbase_zscore": self._round_or_none(coinbase_z["zscore"]),
                        "measured_at": binance_z["created_at"],
                    },
                }
            )

        if coinbase_z["zscore"] > self.ZSCORE_THRESHOLD and abs(binance_z["zscore"]) < self.ZSCORE_THRESHOLD:
            confidence = self._differential_confidence(coinbase_z["zscore"], binance_z["zscore"], abs(rtt_gap))
            signals.append(
                {
                    "signal_type": "cross_exchange_latency_diff",
                    "target_host": coinbase_host,
                    "target_name": history[coinbase_host]["target_name"],
                    "direction": "favor_binance",
                    "confidence": round(confidence, 4),
                    "details": {
                        "leader_exchange": history[binance_host]["target_name"],
                        "lagging_exchange": history[coinbase_host]["target_name"],
                        "leader_host": binance_host,
                        "lagging_host": coinbase_host,
                        "leader_rtt_ms": self._round_or_none(binance_rtt),
                        "lagging_rtt_ms": self._round_or_none(coinbase_rtt),
                        "rtt_gap_ms": self._round_or_none(-rtt_gap),
                        "binance_zscore": self._round_or_none(binance_z["zscore"]),
                        "coinbase_zscore": self._round_or_none(coinbase_z["zscore"]),
                        "measured_at": coinbase_z["created_at"],
                    },
                }
            )

        return signals

    def _trend_signals(self, history):
        signals = []
        for host, data in history.items():
            points_desc = data["points"]
            if len(points_desc) < self.TREND_MIN_POINTS:
                continue

            subset_desc = points_desc[:self.TREND_POINTS]
            subset = list(reversed(subset_desc))
            y_values = [p["rtt"] for p in subset]
            regression = self._linear_regression(y_values)
            if not regression:
                continue

            slope = regression["slope"]
            r2 = regression["r2"]
            if abs(slope) < self.TREND_SLOPE_THRESHOLD or r2 < self.TREND_R2_THRESHOLD:
                continue

            confidence = min(0.99, (abs(slope) / 3.0) * 0.6 + (r2 * 0.4))
            signals.append(
                {
                    "signal_type": "rtt_trend",
                    "target_host": host,
                    "target_name": data["target_name"],
                    "direction": "uptrend" if slope > 0 else "downtrend",
                    "confidence": round(confidence, 4),
                    "details": {
                        "slope_ms_per_scan": self._round_or_none(slope),
                        "r2": self._round_or_none(r2),
                        "sample_size": len(subset),
                        "start_at": subset[0]["created_at"],
                        "end_at": subset[-1]["created_at"],
                    },
                }
            )

        return signals

    def _insert_signal(self, db, signal):
        signal_type = signal["signal_type"]
        target_host = signal.get("target_host")
        direction = signal.get("direction")
        confidence = max(0.0, min(float(signal.get("confidence", 0.0)), 1.0))
        details_json = json.dumps(signal.get("details", {}), sort_keys=True, separators=(",", ":"))

        existing = db.execute(
            """
            SELECT 1
            FROM quant_signals
            WHERE signal_type = ?
              AND COALESCE(target_host, '') = COALESCE(?, '')
              AND COALESCE(direction, '') = COALESCE(?, '')
              AND details_json = ?
              AND created_at >= datetime('now', '-24 hours')
            LIMIT 1
            """,
            (signal_type, target_host, direction, details_json),
        ).fetchone()

        if existing:
            return False

        db.execute(
            """
            INSERT INTO quant_signals
                (signal_type, target_host, target_name, direction, confidence, details_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                signal_type,
                target_host,
                signal.get("target_name"),
                direction,
                confidence,
                details_json,
            ),
        )
        return True

    def _latest_zscore(self, points_desc):
        if len(points_desc) < self.ROLLING_WINDOW + 1:
            return None

        latest = points_desc[0]
        baseline = [p["rtt"] for p in points_desc[1:self.ROLLING_WINDOW + 1]]
        if len(baseline) < 2:
            return None

        stdev = statistics.stdev(baseline)
        if stdev <= 0:
            return None

        mean = statistics.mean(baseline)
        zscore = (latest["rtt"] - mean) / stdev
        return {
            "latest_rtt": latest["rtt"],
            "mean": mean,
            "stdev": stdev,
            "zscore": zscore,
            "created_at": latest["created_at"],
        }

    def _linear_regression(self, y_values):
        n = len(y_values)
        if n < 2:
            return None

        x_values = list(range(n))
        mean_x = (n - 1) / 2.0
        mean_y = statistics.mean(y_values)

        denominator = sum((x - mean_x) ** 2 for x in x_values)
        if denominator == 0:
            return None

        numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_values, y_values))
        slope = numerator / denominator
        intercept = mean_y - (slope * mean_x)

        predictions = [intercept + slope * x for x in x_values]
        ss_tot = sum((y - mean_y) ** 2 for y in y_values)
        ss_res = sum((y - pred) ** 2 for y, pred in zip(y_values, predictions))
        r2 = 0.0 if ss_tot <= 0 else max(0.0, 1.0 - (ss_res / ss_tot))

        return {"slope": slope, "intercept": intercept, "r2": r2}

    def _host_for_name(self, history, needle):
        needle = needle.lower()
        for host, data in history.items():
            name = (data.get("target_name") or "").lower()
            if needle in name:
                return host
        return None

    def _differential_confidence(self, lagging_z, stable_z, rtt_gap_abs):
        gap = max(0.0, abs(lagging_z) - abs(stable_z))
        confidence = 0.35 + min(0.45, gap / 4.0) + min(0.2, rtt_gap_abs / 120.0)
        return min(0.99, confidence)

    def _round_or_none(self, value, digits=4):
        if value is None:
            return None
        return round(float(value), digits)

    def _table_exists(self, db, table_name):
        row = db.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            LIMIT 1
            """,
            (table_name,),
        ).fetchone()
        return bool(row)


if __name__ == "__main__":
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    QuantEngine().run(emit_stdout=True)
