#!/usr/bin/env python3
"""Candle aggregation + GraphQL-style API for fast multi-market consumers.

Goals:
  - Poll broad market universe (Coinbase candles + local fallbacks).
  - Normalize to homogeneous OHLCV candles across timeframes.
  - Keep >= target live points available for downstream consumers.
  - Expose GraphQL endpoint + REST convenience endpoints.
"""

import json
import logging
import math
import os
import sqlite3
import statistics
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

try:
    from flask import Blueprint, g, jsonify, request  # type: ignore
    _HAS_FLASK = True
except Exception:  # pragma: no cover - allows CLI use in minimal envs
    _HAS_FLASK = False

    class _DummyBlueprint:  # type: ignore
        def __init__(self, *_args, **_kwargs):
            pass

        def route(self, *_args, **_kwargs):
            def _decorator(f):
                return f

            return _decorator

    class _DummyRequest:  # type: ignore
        args = {}

        @staticmethod
        def get_json(silent=True):
            _ = silent
            return {}

    class _DummyG:  # type: ignore
        api_tier = "local"
        api_usage_today = 0
        api_rate_limit = 0

    Blueprint = _DummyBlueprint  # type: ignore
    request = _DummyRequest()  # type: ignore
    g = _DummyG()  # type: ignore

    def jsonify(payload):  # type: ignore
        return payload

if _HAS_FLASK:
    from api_auth import require_tier, verify_api_key
else:
    def verify_api_key(f):  # type: ignore
        return f

    def require_tier(*_tiers):  # type: ignore
        def _decorator(f):
            return f

        return _decorator

logger = logging.getLogger("candle_graphql")

BASE_DIR = Path(__file__).parent
AGENTS_DIR = BASE_DIR / "agents"

AGG_DB_PATH = os.environ.get("CANDLE_AGG_DB", str(AGENTS_DIR / "candle_aggregator.db"))
EXCHANGE_SCANNER_DB = AGENTS_DIR / "exchange_scanner.db"
SNIPER_DB = AGENTS_DIR / "sniper.db"
LATENCY_ARB_DB = AGENTS_DIR / "latency_arb.db"
TRADER_DB = AGENTS_DIR / "trader.db"
FEED_FILE = Path(os.environ.get("CANDLE_FEED_FILE", str(AGENTS_DIR / "candle_feed_latest.json")))

TARGET_POINTS_DEFAULT = int(os.environ.get("CANDLE_GRAPHQL_TARGET_POINTS", "1000"))
DEFAULT_SINCE_HOURS = int(os.environ.get("CANDLE_GRAPHQL_SINCE_HOURS", "48"))
DEFAULT_CANDLES_PER_PAIR = int(os.environ.get("CANDLE_GRAPHQL_CANDLES_PER_PAIR", "80"))
DEFAULT_MIN_VOLUME = float(os.environ.get("CANDLE_GRAPHQL_MIN_VOLUME", "0"))
HTTP_TIMEOUT_SECONDS = int(os.environ.get("CANDLE_GRAPHQL_HTTP_TIMEOUT", "8"))
MAX_REFRESH_PAIRS = int(os.environ.get("CANDLE_GRAPHQL_MAX_PAIRS", "64"))
OPPORTUNITY_LOOKBACK_CANDLES = int(os.environ.get("CANDLE_OPP_LOOKBACK_CANDLES", "24"))
OPPORTUNITY_MIN_SAMPLES = int(os.environ.get("CANDLE_OPP_MIN_SAMPLES", "10"))

PRO_PLUS_TIERS = ("pro", "enterprise", "enterprise_pro", "government")

DEFAULT_TIMEFRAMES = tuple(
    x.strip().lower()
    for x in os.environ.get("CANDLE_GRAPHQL_TIMEFRAMES", "1m,5m,15m,1h").split(",")
    if x.strip()
)

TIMEFRAME_TO_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
}
TIMEFRAME_TO_COINBASE_GRANULARITY = {
    "1m": "ONE_MINUTE",
    "5m": "FIVE_MINUTE",
    "15m": "FIFTEEN_MINUTE",
    "1h": "ONE_HOUR",
}

DEFAULT_MARKETS = [
    # Majors
    "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD", "DOGE-USD",
    "LTC-USD", "BCH-USD", "LINK-USD", "AVAX-USD", "MATIC-USD", "DOT-USD",
    "ATOM-USD", "UNI-USD", "AAVE-USD", "FIL-USD", "ARB-USD", "OP-USD",
    "INJ-USD", "NEAR-USD", "SUI-USD", "APT-USD", "HBAR-USD", "ALGO-USD",
    "XLM-USD", "ETC-USD", "TRX-USD", "PEPE-USD", "SHIB-USD", "BONK-USD",
    "RNDR-USD", "FET-USD", "TIA-USD", "SEI-USD", "JUP-USD", "RUNE-USD",
    "ICP-USD", "GRT-USD", "EOS-USD", "CRV-USD", "COMP-USD", "MKR-USD",
    "SUSHI-USD", "SNX-USD", "YFI-USD", "ZEC-USD", "KSM-USD", "1INCH-USD",
]


def _normalize_pair(pair):
    p = str(pair or "").strip().upper().replace("_", "-")
    if not p:
        return ""
    if "-" not in p:
        p = f"{p}-USD"
    return p


def _pair_variants(pair):
    p = _normalize_pair(pair)
    variants = {p}
    if p.endswith("-USD"):
        variants.add(p.replace("-USD", "-USDC"))
    if p.endswith("-USDC"):
        variants.add(p.replace("-USDC", "-USD"))
    return sorted(variants)


def _to_epoch(ts_value):
    if ts_value is None:
        return None
    s = str(ts_value).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def _timeframe_seconds(timeframe):
    return int(TIMEFRAME_TO_SECONDS.get(str(timeframe).lower(), 300))


class CandleAggregator:
    def __init__(self, db_path=AGG_DB_PATH):
        self.db_path = str(db_path)
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=20)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _init_db(self):
        conn = self._connect()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS aggregated_candles (
                pair TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                start_ts INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL DEFAULT 0.0,
                source TEXT NOT NULL DEFAULT 'unknown',
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (pair, timeframe, start_ts)
            );
            CREATE INDEX IF NOT EXISTS idx_agg_pair_tf_time
              ON aggregated_candles(pair, timeframe, start_ts DESC);
            CREATE INDEX IF NOT EXISTS idx_agg_time
              ON aggregated_candles(start_ts DESC);

            CREATE TABLE IF NOT EXISTS candle_refresh_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                requested_pairs INTEGER NOT NULL DEFAULT 0,
                requested_timeframes INTEGER NOT NULL DEFAULT 0,
                target_points INTEGER NOT NULL DEFAULT 0,
                inserted_points INTEGER NOT NULL DEFAULT 0,
                recent_points INTEGER NOT NULL DEFAULT 0,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.commit()
        conn.close()

    def _table_exists(self, conn, table_name):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return bool(row)

    def _fetch_json(self, url, timeout=HTTP_TIMEOUT_SECONDS):
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-CandleGraphQL/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())

    def _fetch_coinbase_candles(self, pair, timeframe, candles_per_pair):
        pair = _normalize_pair(pair)
        tf = str(timeframe).lower()
        gran = TIMEFRAME_TO_COINBASE_GRANULARITY.get(tf)
        step = _timeframe_seconds(tf)
        if not gran:
            return []
        end = int(time.time())
        start = max(0, end - (max(1, int(candles_per_pair)) * step))
        query = urllib.parse.urlencode(
            {"start": start, "end": end, "granularity": gran},
            doseq=False,
        )
        url = f"https://api.coinbase.com/api/v3/brokerage/market/products/{pair}/candles?{query}"
        try:
            data = self._fetch_json(url)
        except Exception as e:
            logger.debug("coinbase candles fetch failed %s %s: %s", pair, tf, e)
            return []
        out = []
        for c in data.get("candles", []) if isinstance(data, dict) else []:
            try:
                ts = int(c["start"])
                o = float(c["open"])
                h = float(c["high"])
                l = float(c["low"])
                cl = float(c["close"])
                v = max(0.0, float(c.get("volume", 0.0) or 0.0))
            except Exception:
                continue
            out.append(
                {
                    "pair": pair,
                    "timeframe": tf,
                    "start_ts": ts,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": cl,
                    "volume": v,
                    "source": "coinbase_adv_public",
                }
            )
        out.sort(key=lambda x: x["start_ts"])
        return out

    def _load_exchange_scanner_points(self, pair, hours):
        if not EXCHANGE_SCANNER_DB.exists():
            return []
        base = _normalize_pair(pair).split("-", 1)[0].lower()
        lookback = f"-{max(1, int(hours))} hours"
        points = []
        try:
            conn = sqlite3.connect(str(EXCHANGE_SCANNER_DB))
            rows = conn.execute(
                """
                SELECT fetched_at, price_usd
                FROM price_feeds
                WHERE lower(asset)=?
                  AND fetched_at >= datetime('now', ?)
                ORDER BY fetched_at ASC
                """,
                (base, lookback),
            ).fetchall()
            conn.close()
        except Exception:
            return []
        for ts, px in rows:
            epoch = _to_epoch(ts)
            if epoch is None:
                continue
            try:
                price = float(px)
            except Exception:
                continue
            if price <= 0:
                continue
            points.append((epoch, price, 1.0, "exchange_scanner"))
        return points

    def _load_trade_points_from_db(self, db_path, table, ts_col, price_col, vol_expr, pair_col="pair", status_col="status", hours=24, pair="BTC-USD"):
        if not Path(db_path).exists():
            return []
        variants = _pair_variants(pair)
        placeholders = ",".join("?" for _ in variants)
        lookback = f"-{max(1, int(hours))} hours"
        points = []
        try:
            conn = sqlite3.connect(str(db_path))
            if not self._table_exists(conn, table):
                conn.close()
                return []
            sql = f"""
                SELECT {ts_col}, {price_col}, {vol_expr}
                FROM {table}
                WHERE {pair_col} IN ({placeholders})
                  AND {ts_col} >= datetime('now', ?)
            """
            if status_col:
                sql += f"""
                  AND ({status_col} IS NULL OR lower(COALESCE({status_col}, '')) NOT IN (
                        'pending','placed','open','accepted','ack_ok',
                        'failed','blocked','canceled','cancelled','expired'
                      ))
                """
            sql += f" ORDER BY {ts_col} ASC"
            rows = conn.execute(sql, tuple(variants) + (lookback,)).fetchall()
            conn.close()
        except Exception:
            return []
        for ts, px, vol in rows:
            epoch = _to_epoch(ts)
            if epoch is None:
                continue
            try:
                price = float(px)
                volume = max(0.0, float(vol or 0.0))
            except Exception:
                continue
            if price <= 0:
                continue
            points.append((epoch, price, volume, Path(db_path).stem))
        return points

    def _load_local_points(self, pair, hours):
        out = []
        out.extend(self._load_exchange_scanner_points(pair, hours))
        out.extend(
            self._load_trade_points_from_db(
                TRADER_DB,
                table="agent_trades",
                ts_col="created_at",
                price_col="price",
                vol_expr="COALESCE(total_usd, quantity, 1.0)",
                hours=hours,
                pair=pair,
            )
        )
        out.extend(
            self._load_trade_points_from_db(
                TRADER_DB,
                table="live_trades",
                ts_col="created_at",
                price_col="price",
                vol_expr="COALESCE(total_usd, quantity, 1.0)",
                hours=hours,
                pair=pair,
            )
        )
        out.extend(
            self._load_trade_points_from_db(
                SNIPER_DB,
                table="sniper_trades",
                ts_col="created_at",
                price_col="entry_price",
                vol_expr="COALESCE(amount_usd, 1.0)",
                hours=hours,
                pair=pair,
            )
        )
        out.extend(
            self._load_trade_points_from_db(
                LATENCY_ARB_DB,
                table="trades",
                ts_col="created_at",
                price_col="entry_price",
                vol_expr="COALESCE(amount_usd, 1.0)",
                hours=hours,
                pair=pair,
            )
        )
        out.sort(key=lambda x: x[0])
        return out

    def _points_to_candles(self, pair, timeframe, points):
        tf = str(timeframe).lower()
        step = _timeframe_seconds(tf)
        buckets = {}
        pair_norm = _normalize_pair(pair)
        for ts, px, vol, source in points:
            bucket = (int(ts) // step) * step
            c = buckets.get(bucket)
            if c is None:
                buckets[bucket] = {
                    "pair": pair_norm,
                    "timeframe": tf,
                    "start_ts": bucket,
                    "open": float(px),
                    "high": float(px),
                    "low": float(px),
                    "close": float(px),
                    "volume": max(0.0, float(vol or 0.0)),
                    "source": f"local:{source}",
                }
                continue
            c["high"] = max(float(c["high"]), float(px))
            c["low"] = min(float(c["low"]), float(px))
            c["close"] = float(px)
            c["volume"] = float(c["volume"]) + max(0.0, float(vol or 0.0))
        return [buckets[t] for t in sorted(buckets.keys())]

    def _upsert_candles(self, rows):
        if not rows:
            return 0
        conn = self._connect()
        conn.executemany(
            """
            INSERT INTO aggregated_candles
                (pair, timeframe, start_ts, open, high, low, close, volume, source, fetched_at)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(pair, timeframe, start_ts) DO UPDATE SET
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                volume=excluded.volume,
                source=excluded.source,
                fetched_at=CURRENT_TIMESTAMP
            """,
            [
                (
                    str(r["pair"]),
                    str(r["timeframe"]),
                    int(r["start_ts"]),
                    float(r["open"]),
                    float(r["high"]),
                    float(r["low"]),
                    float(r["close"]),
                    float(r.get("volume", 0.0) or 0.0),
                    str(r.get("source", "unknown")),
                )
                for r in rows
            ],
        )
        inserted = conn.total_changes
        conn.commit()
        conn.close()
        return int(inserted)

    def _recent_point_count(self, since_hours):
        cutoff_ts = int(time.time()) - max(1, int(since_hours)) * 3600
        conn = self._connect()
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM aggregated_candles WHERE start_ts >= ?",
            (cutoff_ts,),
        ).fetchone()
        conn.close()
        return int(row["cnt"] if row else 0)

    def _dynamic_pairs(self):
        pairs = []
        pairs.extend(DEFAULT_MARKETS)
        for db_path, table in (
            (TRADER_DB, "agent_trades"),
            (TRADER_DB, "live_trades"),
            (SNIPER_DB, "sniper_trades"),
            (LATENCY_ARB_DB, "trades"),
        ):
            if not Path(db_path).exists():
                continue
            try:
                conn = sqlite3.connect(str(db_path))
                if not self._table_exists(conn, table):
                    conn.close()
                    continue
                rows = conn.execute(
                    f"""
                    SELECT pair, COUNT(*) AS n
                    FROM {table}
                    WHERE pair IS NOT NULL
                    GROUP BY pair
                    ORDER BY n DESC
                    LIMIT 30
                    """
                ).fetchall()
                conn.close()
            except Exception:
                continue
            for pair, _n in rows:
                p = _normalize_pair(pair)
                if p:
                    pairs.append(p)

        deduped = []
        seen = set()
        for pair in pairs:
            p = _normalize_pair(pair)
            if not p or p in seen:
                continue
            seen.add(p)
            deduped.append(p)
        return deduped[: max(1, int(MAX_REFRESH_PAIRS))]

    def refresh(self, pairs=None, timeframes=None, target_points=TARGET_POINTS_DEFAULT, candles_per_pair=DEFAULT_CANDLES_PER_PAIR, since_hours=DEFAULT_SINCE_HOURS):
        pair_list = [_normalize_pair(p) for p in (pairs or self._dynamic_pairs()) if _normalize_pair(p)]
        tf_list = [str(tf).lower() for tf in (timeframes or DEFAULT_TIMEFRAMES) if str(tf).lower() in TIMEFRAME_TO_SECONDS]
        if not pair_list:
            pair_list = list(DEFAULT_MARKETS)
        if not tf_list:
            tf_list = ["5m", "1h"]

        total_inserted = 0
        source_counts = {"coinbase_adv_public": 0, "local_fallback": 0}

        for pair in pair_list:
            for tf in tf_list:
                remote = self._fetch_coinbase_candles(pair, tf, candles_per_pair=candles_per_pair)
                rows = remote
                if not rows:
                    points = self._load_local_points(pair, hours=max(24, int(since_hours)))
                    rows = self._points_to_candles(pair, tf, points)
                if not rows:
                    continue
                if remote:
                    source_counts["coinbase_adv_public"] += len(rows)
                else:
                    source_counts["local_fallback"] += len(rows)
                total_inserted += self._upsert_candles(rows)

        effective_since_hours = max(1, int(since_hours))
        recent_points = self._recent_point_count(since_hours=effective_since_hours)
        if recent_points < int(target_points):
            # Expand local fallback lookback to saturate target point count.
            for pair in pair_list:
                for tf in tf_list:
                    points = self._load_local_points(pair, hours=max(72, int(since_hours) * 4))
                    rows = self._points_to_candles(pair, tf, points)
                    if not rows:
                        continue
                    source_counts["local_fallback"] += len(rows)
                    total_inserted += self._upsert_candles(rows)
            recent_points = self._recent_point_count(since_hours=effective_since_hours)
            while recent_points < int(target_points) and effective_since_hours < 168:
                effective_since_hours = min(168, max(effective_since_hours + 1, effective_since_hours * 2))
                recent_points = self._recent_point_count(since_hours=effective_since_hours)

        summary = self.summary(
            pairs=pair_list,
            timeframes=tf_list,
            since_hours=effective_since_hours,
            target_points=target_points,
        )
        summary.update(
            {
                "requested_since_hours": int(since_hours),
                "effective_since_hours": int(effective_since_hours),
                "requested_pairs": len(pair_list),
                "requested_timeframes": len(tf_list),
                "inserted_points": int(total_inserted),
                "source_counts": source_counts,
            }
        )

        conn = self._connect()
        conn.execute(
            """
            INSERT INTO candle_refresh_runs
                (requested_pairs, requested_timeframes, target_points, inserted_points, recent_points, details_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                int(len(pair_list)),
                int(len(tf_list)),
                int(target_points),
                int(total_inserted),
                int(recent_points),
                json.dumps(summary),
            ),
        )
        conn.commit()
        conn.close()

        self.publish_feed(
            limit=max(int(target_points), 1000),
            pairs=pair_list,
            timeframes=tf_list,
            since_hours=effective_since_hours,
            min_volume=DEFAULT_MIN_VOLUME,
        )
        return summary

    def query_points(self, limit=1000, pairs=None, timeframes=None, since_hours=DEFAULT_SINCE_HOURS, min_volume=DEFAULT_MIN_VOLUME):
        limit = max(1, min(5000, int(limit or 1000)))
        conditions = []
        params = []

        cutoff_ts = int(time.time()) - max(1, int(since_hours)) * 3600
        conditions.append("start_ts >= ?")
        params.append(cutoff_ts)

        if pairs:
            pair_list = [_normalize_pair(p) for p in pairs if _normalize_pair(p)]
            if pair_list:
                conditions.append(f"pair IN ({','.join('?' for _ in pair_list)})")
                params.extend(pair_list)

        if timeframes:
            tf_list = [str(tf).lower() for tf in timeframes if str(tf).lower() in TIMEFRAME_TO_SECONDS]
            if tf_list:
                conditions.append(f"timeframe IN ({','.join('?' for _ in tf_list)})")
                params.extend(tf_list)

        if float(min_volume or 0.0) > 0:
            conditions.append("volume >= ?")
            params.append(float(min_volume))

        where = " AND ".join(conditions) if conditions else "1=1"
        sql = f"""
            SELECT pair, timeframe, start_ts, open, high, low, close, volume, source, fetched_at
            FROM aggregated_candles
            WHERE {where}
            ORDER BY start_ts DESC
            LIMIT ?
        """
        params.append(limit)

        conn = self._connect()
        rows = conn.execute(sql, tuple(params)).fetchall()
        conn.close()
        out = []
        for r in rows:
            out.append(
                {
                    "pair": str(r["pair"]),
                    "timeframe": str(r["timeframe"]),
                    "start_ts": int(r["start_ts"]),
                    "start_at": datetime.fromtimestamp(int(r["start_ts"]), tz=timezone.utc).isoformat(),
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "volume": float(r["volume"]),
                    "source": str(r["source"]),
                    "fetched_at": str(r["fetched_at"]),
                }
            )
        return out

    def summary(self, pairs=None, timeframes=None, since_hours=DEFAULT_SINCE_HOURS, target_points=TARGET_POINTS_DEFAULT):
        cutoff_ts = int(time.time()) - max(1, int(since_hours)) * 3600
        conditions = ["start_ts >= ?"]
        params = [cutoff_ts]

        if pairs:
            pair_list = [_normalize_pair(p) for p in pairs if _normalize_pair(p)]
            if pair_list:
                conditions.append(f"pair IN ({','.join('?' for _ in pair_list)})")
                params.extend(pair_list)
        if timeframes:
            tf_list = [str(tf).lower() for tf in timeframes if str(tf).lower() in TIMEFRAME_TO_SECONDS]
            if tf_list:
                conditions.append(f"timeframe IN ({','.join('?' for _ in tf_list)})")
                params.extend(tf_list)

        where = " AND ".join(conditions)
        conn = self._connect()
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total_points,
                COUNT(DISTINCT pair) AS pair_count,
                COUNT(DISTINCT timeframe) AS timeframe_count,
                MAX(start_ts) AS latest_ts
            FROM aggregated_candles
            WHERE {where}
            """,
            tuple(params),
        ).fetchone()
        src_rows = conn.execute(
            f"""
            SELECT source, COUNT(*) AS cnt
            FROM aggregated_candles
            WHERE {where}
            GROUP BY source
            ORDER BY cnt DESC
            LIMIT 8
            """,
            tuple(params),
        ).fetchall()
        conn.close()

        total_points = int(row["total_points"] if row else 0)
        latest_ts = int(row["latest_ts"] if row and row["latest_ts"] else 0)
        now_ts = int(time.time())
        freshness = max(0, now_ts - latest_ts) if latest_ts > 0 else None
        return {
            "target_points": int(target_points),
            "total_points": total_points,
            "target_achieved": bool(total_points >= int(target_points)),
            "pair_count": int(row["pair_count"] if row else 0),
            "timeframe_count": int(row["timeframe_count"] if row else 0),
            "latest_ts": latest_ts if latest_ts > 0 else None,
            "latest_at": datetime.fromtimestamp(latest_ts, tz=timezone.utc).isoformat() if latest_ts > 0 else None,
            "freshness_seconds": freshness,
            "source_breakdown": [
                {"source": str(r["source"]), "count": int(r["cnt"])} for r in src_rows
            ],
        }

    def opportunities(
        self,
        limit=12,
        timeframe="5m",
        since_hours=DEFAULT_SINCE_HOURS,
        pairs=None,
        min_volume=DEFAULT_MIN_VOLUME,
        lookback_candles=OPPORTUNITY_LOOKBACK_CANDLES,
        min_samples=OPPORTUNITY_MIN_SAMPLES,
    ):
        """Rank pairs by short-horizon tradability from normalized candles."""
        tf = str(timeframe or "5m").lower()
        if tf not in TIMEFRAME_TO_SECONDS:
            tf = "5m"
        limit = max(1, min(100, int(limit or 12)))
        lookback = max(4, min(512, int(lookback_candles or OPPORTUNITY_LOOKBACK_CANDLES)))
        min_samples = max(4, min(lookback, int(min_samples or OPPORTUNITY_MIN_SAMPLES)))
        cutoff_ts = int(time.time()) - max(1, int(since_hours)) * 3600

        conditions = ["start_ts >= ?", "timeframe = ?"]
        params = [cutoff_ts, tf]
        if float(min_volume or 0.0) > 0:
            conditions.append("volume >= ?")
            params.append(float(min_volume))
        if pairs:
            pair_list = [_normalize_pair(p) for p in pairs if _normalize_pair(p)]
            if pair_list:
                conditions.append(f"pair IN ({','.join('?' for _ in pair_list)})")
                params.extend(pair_list)

        where = " AND ".join(conditions)
        conn = self._connect()
        rows = conn.execute(
            f"""
            SELECT pair, start_ts, open, high, low, close, volume
            FROM aggregated_candles
            WHERE {where}
            ORDER BY pair ASC, start_ts ASC
            """,
            tuple(params),
        ).fetchall()
        conn.close()

        grouped = {}
        for r in rows:
            pair = str(r["pair"])
            bucket = grouped.setdefault(pair, [])
            bucket.append(r)
            if len(bucket) > lookback:
                del bucket[:-lookback]

        ranked = []
        for pair, sample in grouped.items():
            if len(sample) < min_samples:
                continue
            opens = []
            highs = []
            lows = []
            closes = []
            vols = []
            for row in sample:
                try:
                    o = float(row["open"])
                    h = float(row["high"])
                    l = float(row["low"])
                    c = float(row["close"])
                    v = max(0.0, float(row["volume"] or 0.0))
                except Exception:
                    continue
                if min(o, h, l, c) <= 0:
                    continue
                opens.append(o)
                highs.append(h)
                lows.append(l)
                closes.append(c)
                vols.append(v)
            if len(closes) < min_samples:
                continue

            returns = []
            for i in range(1, len(closes)):
                prev = closes[i - 1]
                if prev > 0:
                    returns.append((closes[i] - prev) / prev)
            if not returns:
                continue

            first_open = opens[0] if opens else closes[0]
            momentum = ((closes[-1] - first_open) / first_open) if first_open > 0 else 0.0
            volatility = statistics.pstdev(returns) if len(returns) > 1 else abs(returns[-1])
            range_pct = statistics.fmean(
                ((highs[i] - lows[i]) / max(opens[i], 1e-9) for i in range(len(opens)))
            )
            if len(vols) >= 6:
                vol_recent = statistics.fmean(vols[-3:])
                vol_base_raw = statistics.fmean(vols[:-3])
                vol_base = vol_base_raw if vol_base_raw > 0 else 0.0
            else:
                vol_recent = statistics.fmean(vols) if vols else 0.0
                vol_base = vol_recent
            volume_ratio = (vol_recent / vol_base) if vol_base > 0 else (1.0 if vol_recent > 0 else 0.0)

            # Score weights: direction-agnostic movement + tradability + activity.
            score = (
                (abs(momentum) * 0.46)
                + (volatility * 0.30)
                + (range_pct * 0.14)
                + (max(0.0, min(3.0, volume_ratio - 1.0)) * 0.10)
            )
            if not math.isfinite(score):
                continue

            latest_ts = int(sample[-1]["start_ts"])
            ranked.append(
                {
                    "pair": pair,
                    "timeframe": tf,
                    "direction_bias": "BUY" if momentum >= 0 else "SELL",
                    "opportunity_score": round(float(max(0.0, score)), 8),
                    "sample_count": int(len(closes)),
                    "momentum_pct": round(float(momentum * 100.0), 6),
                    "volatility_pct": round(float(volatility * 100.0), 6),
                    "range_pct": round(float(range_pct * 100.0), 6),
                    "volume_ratio": round(float(volume_ratio), 6),
                    "last_close": round(float(closes[-1]), 8),
                    "latest_ts": latest_ts,
                    "latest_at": datetime.fromtimestamp(latest_ts, tz=timezone.utc).isoformat(),
                }
            )

        ranked.sort(
            key=lambda x: (
                float(x.get("opportunity_score", 0.0)),
                abs(float(x.get("momentum_pct", 0.0))),
                float(x.get("volume_ratio", 0.0)),
            ),
            reverse=True,
        )
        return ranked[:limit]

    def publish_feed(self, limit=1000, pairs=None, timeframes=None, since_hours=DEFAULT_SINCE_HOURS, min_volume=DEFAULT_MIN_VOLUME):
        points = self.query_points(
            limit=limit,
            pairs=pairs,
            timeframes=timeframes,
            since_hours=since_hours,
            min_volume=min_volume,
        )
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": self.summary(
                pairs=pairs,
                timeframes=timeframes,
                since_hours=since_hours,
                target_points=max(int(limit), TARGET_POINTS_DEFAULT),
            ),
            "filters": {
                "limit": int(limit),
                "pairs": pairs or [],
                "timeframes": timeframes or [],
                "since_hours": int(since_hours),
                "min_volume": float(min_volume),
            },
            "points": points,
        }
        FEED_FILE.parent.mkdir(parents=True, exist_ok=True)
        FEED_FILE.write_text(json.dumps(payload, indent=2))
        return payload


candle_graphql_api = Blueprint("candle_graphql_api", __name__, url_prefix="/api/v1")
_agg = CandleAggregator()


@candle_graphql_api.route("/candles/summary")
@verify_api_key
@require_tier(*PRO_PLUS_TIERS)
def candles_summary():
    since_hours = max(1, int(request.args.get("since_hours", DEFAULT_SINCE_HOURS)))
    target_points = max(1, int(request.args.get("target_points", TARGET_POINTS_DEFAULT)))
    pairs = [x.strip() for x in str(request.args.get("pairs", "")).split(",") if x.strip()]
    timeframes = [x.strip().lower() for x in str(request.args.get("timeframes", "")).split(",") if x.strip()]
    return jsonify(
        {
            "summary": _agg.summary(
                pairs=pairs or None,
                timeframes=timeframes or None,
                since_hours=since_hours,
                target_points=target_points,
            ),
            "tier": g.api_tier,
            "usage": {"used": g.api_usage_today, "limit": g.api_rate_limit},
        }
    )


@candle_graphql_api.route("/candles")
@verify_api_key
@require_tier(*PRO_PLUS_TIERS)
def candles_list():
    limit = max(1, min(5000, int(request.args.get("limit", TARGET_POINTS_DEFAULT))))
    since_hours = max(1, int(request.args.get("since_hours", DEFAULT_SINCE_HOURS)))
    min_volume = float(request.args.get("min_volume", DEFAULT_MIN_VOLUME) or 0.0)
    pairs = [x.strip() for x in str(request.args.get("pairs", "")).split(",") if x.strip()]
    timeframes = [x.strip().lower() for x in str(request.args.get("timeframes", "")).split(",") if x.strip()]
    rows = _agg.query_points(
        limit=limit,
        pairs=pairs or None,
        timeframes=timeframes or None,
        since_hours=since_hours,
        min_volume=min_volume,
    )
    return jsonify(
        {
            "count": len(rows),
            "points": rows,
            "tier": g.api_tier,
            "usage": {"used": g.api_usage_today, "limit": g.api_rate_limit},
        }
    )


@candle_graphql_api.route("/candles/refresh", methods=["POST"])
@verify_api_key
@require_tier(*PRO_PLUS_TIERS)
def candles_refresh():
    body = request.get_json(silent=True) or {}
    summary = _agg.refresh(
        pairs=body.get("pairs"),
        timeframes=body.get("timeframes"),
        target_points=int(body.get("target_points", TARGET_POINTS_DEFAULT)),
        candles_per_pair=int(body.get("candles_per_pair", DEFAULT_CANDLES_PER_PAIR)),
        since_hours=int(body.get("since_hours", DEFAULT_SINCE_HOURS)),
    )
    return jsonify({"ok": True, "summary": summary})


@candle_graphql_api.route("/candles/opportunities")
@verify_api_key
@require_tier(*PRO_PLUS_TIERS)
def candles_opportunities():
    limit = max(1, min(100, int(request.args.get("limit", 12) or 12)))
    since_hours = max(1, int(request.args.get("since_hours", DEFAULT_SINCE_HOURS)))
    timeframe = str(request.args.get("timeframe", "5m") or "5m").lower()
    lookback_candles = max(4, int(request.args.get("lookback_candles", OPPORTUNITY_LOOKBACK_CANDLES) or OPPORTUNITY_LOOKBACK_CANDLES))
    min_samples = max(4, int(request.args.get("min_samples", OPPORTUNITY_MIN_SAMPLES) or OPPORTUNITY_MIN_SAMPLES))
    min_volume = float(request.args.get("min_volume", DEFAULT_MIN_VOLUME) or 0.0)
    pairs = [x.strip() for x in str(request.args.get("pairs", "")).split(",") if x.strip()]
    rows = _agg.opportunities(
        limit=limit,
        timeframe=timeframe,
        since_hours=since_hours,
        pairs=pairs or None,
        min_volume=min_volume,
        lookback_candles=lookback_candles,
        min_samples=min_samples,
    )
    return jsonify(
        {
            "count": len(rows),
            "opportunities": rows,
            "filters": {
                "limit": limit,
                "timeframe": timeframe,
                "since_hours": since_hours,
                "pairs": pairs,
                "min_volume": min_volume,
                "lookback_candles": lookback_candles,
                "min_samples": min_samples,
            },
            "tier": g.api_tier,
            "usage": {"used": g.api_usage_today, "limit": g.api_rate_limit},
        }
    )


@candle_graphql_api.route("/graphql", methods=["POST"])
@verify_api_key
@require_tier(*PRO_PLUS_TIERS)
def graphql_candles():
    """Minimal GraphQL-compatible endpoint for candle consumers.

    Supported fields:
      - refreshCandles
      - candleSummary
      - candlePoints
      - candleOpportunities
    Arguments are read from GraphQL `variables`.
    """
    body = request.get_json(silent=True) or {}
    query = str(body.get("query", "") or "")
    variables = body.get("variables") if isinstance(body.get("variables"), dict) else {}
    op_name = str(body.get("operationName", "") or "")
    if not query:
        return jsonify({"errors": [{"message": "GraphQL query is required"}]}), 400

    data = {}
    if "refreshCandles" in query:
        data["refreshCandles"] = _agg.refresh(
            pairs=variables.get("pairs"),
            timeframes=variables.get("timeframes"),
            target_points=int(variables.get("targetPoints", TARGET_POINTS_DEFAULT)),
            candles_per_pair=int(variables.get("candlesPerPair", DEFAULT_CANDLES_PER_PAIR)),
            since_hours=int(variables.get("sinceHours", DEFAULT_SINCE_HOURS)),
        )

    if "candleSummary" in query:
        data["candleSummary"] = _agg.summary(
            pairs=variables.get("pairs"),
            timeframes=variables.get("timeframes"),
            since_hours=int(variables.get("sinceHours", DEFAULT_SINCE_HOURS)),
            target_points=int(variables.get("targetPoints", TARGET_POINTS_DEFAULT)),
        )

    if "candlePoints" in query:
        data["candlePoints"] = _agg.query_points(
            limit=int(variables.get("limit", TARGET_POINTS_DEFAULT)),
            pairs=variables.get("pairs"),
            timeframes=variables.get("timeframes"),
            since_hours=int(variables.get("sinceHours", DEFAULT_SINCE_HOURS)),
            min_volume=float(variables.get("minVolume", DEFAULT_MIN_VOLUME)),
        )

    if "candleOpportunities" in query:
        data["candleOpportunities"] = _agg.opportunities(
            limit=int(variables.get("limit", 12)),
            timeframe=str(variables.get("timeframe", "5m") or "5m").lower(),
            since_hours=int(variables.get("sinceHours", DEFAULT_SINCE_HOURS)),
            pairs=variables.get("pairs"),
            min_volume=float(variables.get("minVolume", DEFAULT_MIN_VOLUME)),
            lookback_candles=int(variables.get("lookbackCandles", OPPORTUNITY_LOOKBACK_CANDLES)),
            min_samples=int(variables.get("minSamples", OPPORTUNITY_MIN_SAMPLES)),
        )

    if not data:
        # Fallback if consumer omits explicit fields in query parser assumptions.
        data = {
            "candleSummary": _agg.summary(
                since_hours=int(variables.get("sinceHours", DEFAULT_SINCE_HOURS)),
                target_points=int(variables.get("targetPoints", TARGET_POINTS_DEFAULT)),
            ),
            "candlePoints": _agg.query_points(
                limit=int(variables.get("limit", TARGET_POINTS_DEFAULT)),
                since_hours=int(variables.get("sinceHours", DEFAULT_SINCE_HOURS)),
                min_volume=float(variables.get("minVolume", DEFAULT_MIN_VOLUME)),
            ),
        }

    return jsonify(
        {
            "data": data,
            "extensions": {
                "operationName": op_name or None,
                "tier": getattr(g, "api_tier", None),
                "usage": {"used": getattr(g, "api_usage_today", None), "limit": getattr(g, "api_rate_limit", None)},
            },
        }
    )


def _cli():
    agg = CandleAggregator()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "refresh"
    if cmd == "summary":
        print(json.dumps(agg.summary(target_points=TARGET_POINTS_DEFAULT), indent=2))
        return
    if cmd == "refresh":
        print(
            json.dumps(
                agg.refresh(
                    target_points=TARGET_POINTS_DEFAULT,
                    candles_per_pair=DEFAULT_CANDLES_PER_PAIR,
                    since_hours=DEFAULT_SINCE_HOURS,
                ),
                indent=2,
            )
        )
        return
    if cmd == "daemon":
        interval = max(5, int(os.environ.get("CANDLE_GRAPHQL_POLL_INTERVAL_SECONDS", "20")))
        while True:
            try:
                summary = agg.refresh(
                    target_points=TARGET_POINTS_DEFAULT,
                    candles_per_pair=DEFAULT_CANDLES_PER_PAIR,
                    since_hours=DEFAULT_SINCE_HOURS,
                )
                logger.info(
                    "candle daemon refresh points=%s achieved=%s",
                    summary.get("total_points"),
                    summary.get("target_achieved"),
                )
            except Exception as e:
                logger.warning("candle daemon refresh failed: %s", e)
            time.sleep(interval)
        return
    print("Usage: python candle_graphql_api.py [refresh|summary|daemon]")


if __name__ == "__main__":
    _cli()
