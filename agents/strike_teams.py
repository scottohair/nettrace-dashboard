#!/usr/bin/env python3
"""Financial Strike Teams — autonomous rapid-gain trading squads.

Each strike team is a self-contained unit with:
  - Scout: finds opportunities (market scan, anomaly detection, cross-venue)
  - Analyst: validates signals (multi-timeframe, correlation, risk/reward)
  - Executor: places and manages orders (routing, sizing, execution quality)

Teams compete for capital allocation based on performance (evolutionary).
Top performers get more capital. Underperformers get fired and replaced.

Strike Team Types:
  1. MomentumStrike — catches fast moves (1-5 min, volume/EMA breakouts)
  2. ArbitrageStrike — cross-venue price gaps (CEX-CEX, CEX-DEX)
  3. MeanReversionStrike — statistical deviation plays (z-score > 2)
  4. BreakoutStrike — support/resistance breaks with volume confirmation
  5. CorrelationStrike — inter-asset correlation breakdown trades

Design: stateless, ephemeral, no persistent DB per team. All results feed
back to KPI tracker and agent_goals for evolutionary management.
"""

import json
import logging
import os
import sqlite3
import sys
import statistics
import time
import math
from collections import defaultdict
from threading import Thread, Event, Lock
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logger = logging.getLogger("strike_teams")


def _parse_csv_values(value):
    """Parse comma/iterable values into normalized, non-empty tokens."""
    if value is None:
        return tuple()
    if isinstance(value, (list, tuple, set)):
        items = [str(v or "").strip() for v in value]
    else:
        items = [item.strip() for item in str(value).split(",")]
    return tuple(item for item in items if item)

MAKER_FEE_RATE = float(os.environ.get("STRIKE_MAKER_FEE_RATE", "0.004") or 0.004)
TAKER_FEE_RATE = float(os.environ.get("STRIKE_TAKER_FEE_RATE", "0.006") or 0.006)
EXIT_BUFFER_RATE = float(os.environ.get("STRIKE_EXIT_BUFFER_RATE", "0.0025") or 0.0025)
MIN_SCAN_INTERVAL_SECONDS = int(os.environ.get("STRIKE_MIN_SCAN_INTERVAL_SECONDS", "90") or 90)
DEFAULT_HF_INTERVAL_SECONDS = int(os.environ.get("STRIKE_DEFAULT_HF_INTERVAL_SECONDS", "120") or 120)
DEFAULT_LF_INTERVAL_SECONDS = int(os.environ.get("STRIKE_DEFAULT_LF_INTERVAL_SECONDS", "240") or 240)
SELL_CLOSE_TARGET_RATE = float(os.environ.get("STRIKE_SELL_CLOSE_TARGET_RATE", "0.70") or 0.70)
SELL_CLOSE_MIN_OBS = int(os.environ.get("STRIKE_SELL_CLOSE_MIN_OBS", "4") or 4)
SELL_CLOSE_WINDOW = int(os.environ.get("STRIKE_SELL_CLOSE_WINDOW", "24") or 24)
BUY_THROTTLE_ON_SELL_GAP = os.environ.get("STRIKE_BUY_THROTTLE_ON_SELL_GAP", "1").lower() not in ("0", "false", "no")
EXECUTION_HEALTH_GATE = os.environ.get("STRIKE_EXECUTION_HEALTH_GATE", "1").lower() not in ("0", "false", "no")
EXECUTION_HEALTH_BUY_ONLY = os.environ.get("STRIKE_EXECUTION_HEALTH_BUY_ONLY", "1").lower() not in ("0", "false", "no")
EXECUTION_HEALTH_CACHE_SECONDS = int(os.environ.get("STRIKE_EXECUTION_HEALTH_CACHE_SECONDS", "45") or 45)
EXECUTION_HEALTH_DEGRADED_MODE = os.environ.get("STRIKE_EXECUTION_HEALTH_DEGRADED_MODE", "1").lower() not in ("0", "false", "no")
EXECUTION_HEALTH_DEGRADED_REASONS = _parse_csv_values(
    os.environ.get(
        "STRIKE_EXECUTION_HEALTH_DEGRADED_REASONS",
        "telemetry_samples_low,telemetry_success_rate_low,telemetry_failure_rate_high,telemetry_p90_high",
    )
)
EXECUTION_HEALTH_DEGRADED_SIZE_FACTOR = float(os.environ.get("STRIKE_EXECUTION_HEALTH_DEGRADED_SIZE_FACTOR", "0.75"))
CLOSE_FIRST_MODE_ENABLED = os.environ.get("STRIKE_CLOSE_FIRST_MODE_ENABLED", "1").lower() not in ("0", "false", "no")
CLOSE_FIRST_POSITION_LOOKBACK_HOURS = int(os.environ.get("STRIKE_CLOSE_FIRST_LOOKBACK_HOURS", "96") or 96)
CLOSE_FIRST_MIN_OPEN_NOTIONAL_USD = float(os.environ.get("STRIKE_CLOSE_FIRST_MIN_OPEN_NOTIONAL_USD", "2.0") or 2.0)
STRIKE_DB_BUSY_TIMEOUT_MS = int(os.environ.get("STRIKE_DB_BUSY_TIMEOUT_MS", "5000") or 5000)
TRADER_DB_PATH = Path(__file__).parent / "trader.db"
CANDLE_AGG_DB_PATH = Path(__file__).parent / "candle_aggregator.db"
CANDLE_FEED_PATH = Path(__file__).parent / "candle_feed_latest.json"
STRIKE_CANDLE_FALLBACK_ENABLED = os.environ.get("STRIKE_CANDLE_FALLBACK_ENABLED", "1").lower() not in (
    "0",
    "false",
    "no",
)
STRIKE_CANDLE_REMOTE_MIN_ROWS = int(os.environ.get("STRIKE_CANDLE_REMOTE_MIN_ROWS", "8") or 8)
STRIKE_DYNAMIC_PAIR_SELECTION = os.environ.get("STRIKE_DYNAMIC_PAIR_SELECTION", "1").lower() not in (
    "0",
    "false",
    "no",
)
STRIKE_DYNAMIC_PAIRS_REFRESH_SECONDS = int(
    os.environ.get("STRIKE_DYNAMIC_PAIRS_REFRESH_SECONDS", "90") or 90
)
STRIKE_DYNAMIC_PAIRS_LIMIT_HF = int(os.environ.get("STRIKE_DYNAMIC_PAIRS_LIMIT_HF", "8") or 8)
STRIKE_DYNAMIC_PAIRS_LIMIT_LF = int(os.environ.get("STRIKE_DYNAMIC_PAIRS_LIMIT_LF", "10") or 10)
STRIKE_DYNAMIC_PAIR_MIN_POINTS = int(os.environ.get("STRIKE_DYNAMIC_PAIR_MIN_POINTS", "8") or 8)
STRIKE_DYNAMIC_PAIR_LOOKBACK = int(os.environ.get("STRIKE_DYNAMIC_PAIR_LOOKBACK", "20") or 20)
STRIKE_DYNAMIC_PAIR_LOOKBACK_HOURS = int(os.environ.get("STRIKE_DYNAMIC_PAIR_LOOKBACK_HOURS", "72") or 72)
STRIKE_DYNAMIC_INCLUDE_USDC = os.environ.get("STRIKE_DYNAMIC_INCLUDE_USDC", "0").lower() not in (
    "0",
    "false",
    "no",
)
COMPLETED_TRADE_STATUSES = (
    "filled",
    "closed",
    "executed",
    "partial_filled",
    "partially_filled",
    "settled",
)
ARBITRAGE_BASIS_HISTORY_LIMIT = int(
    os.environ.get("STRIKE_ARBITRAGE_BASIS_HISTORY_LIMIT", "24") or 24
)
ARBITRAGE_BASIS_MIN_HISTORY = int(
    os.environ.get("STRIKE_ARBITRAGE_BASIS_MIN_HISTORY", "3") or 3
)
ARBITRAGE_BASIS_Z_REQUIRED = float(
    os.environ.get("STRIKE_ARBITRAGE_BASIS_Z_REQUIRED", "1.05") or 1.05
)
ARBITRAGE_BASIS_STRESS_Z = float(
    os.environ.get("STRIKE_ARBITRAGE_BASIS_STRESS_Z", "1.90") or 1.90
)
ARBITRAGE_MIN_SPREAD_THRESHOLD = float(
    os.environ.get("STRIKE_ARBITRAGE_MIN_SPREAD_THRESHOLD", "0.008") or 0.008
)
ARBITRAGE_BOOTSTRAP_MIN_SPREAD = float(
    os.environ.get("STRIKE_ARBITRAGE_BOOTSTRAP_MIN_SPREAD", "0.012") or 0.012
)
_ARBITRAGE_BASIS_HISTORY = defaultdict(list)
_ARBITRAGE_BASIS_LOCK = Lock()

# Core imports
try:
    from agent_goals import GoalValidator
    _goals = GoalValidator()
except ImportError:
    _goals = None

try:
    from kpi_tracker import get_kpi_tracker
    _kpi = get_kpi_tracker()
except Exception:
    _kpi = None

try:
    from risk_controller import get_controller
    _risk = get_controller()
except Exception:
    _risk = None

try:
    from execution_health import evaluate_execution_health
except Exception:
    try:
        from agents.execution_health import evaluate_execution_health  # type: ignore
    except Exception:
        evaluate_execution_health = None  # type: ignore


_EXECUTION_HEALTH_CACHE = {"ts": 0.0, "payload": {"green": False, "reason": "uninitialized"}}


def _safe_float(value, fallback=0.0):
    try:
        v = float(value)
    except Exception:
        return float(fallback)
    return v if math.isfinite(v) else float(fallback)


def _reset_arbitrage_basis_state():
    """Reset in-memory arbitrage spread history."""
    with _ARBITRAGE_BASIS_LOCK:
        _ARBITRAGE_BASIS_HISTORY.clear()


def _classify_arbitrage_basis(pair, spread):
    """Classify cross-venue spread regime for arbitrage decisions."""
    spread_f = _safe_float(spread, 0.0)
    spread_abs = abs(spread_f)
    pair_key = str(pair or "").upper()

    with _ARBITRAGE_BASIS_LOCK:
        history = _ARBITRAGE_BASIS_HISTORY[pair_key]
        history.append(spread_f)
        limit = max(1, int(ARBITRAGE_BASIS_HISTORY_LIMIT))
        if len(history) > limit:
            del history[:-limit]
        samples = list(history)

    sample_count = len(samples)
    z_score = 0.0
    mean_spread = 0.0
    stdev_spread = 0.0

    if sample_count > 1:
        mean_spread = statistics.mean(samples)
        stdev_spread = statistics.pstdev(samples)
        stdev_spread = max(1e-9, _safe_float(stdev_spread, 0.0))
        z_score = (spread_f - mean_spread) / stdev_spread

    if sample_count < ARBITRAGE_BASIS_MIN_HISTORY:
        if spread_abs >= ARBITRAGE_BOOTSTRAP_MIN_SPREAD:
            regime = (
                "bootstrap_coinbase_premium" if spread_f > 0 else "bootstrap_coinbase_discount"
            )
            confidence_boost = 0.02
        else:
            regime = "neutral"
            confidence_boost = 0.0
    else:
        if abs(z_score) >= ARBITRAGE_BASIS_STRESS_Z:
            regime = (
                "stressed_coinbase_premium" if spread_f > 0 else "stressed_coinbase_discount"
            )
            confidence_boost = 0.10
        elif abs(z_score) >= ARBITRAGE_BASIS_Z_REQUIRED:
            regime = "normal_coinbase_premium" if spread_f > 0 else "normal_coinbase_discount"
            confidence_boost = 0.05
        else:
            regime = "neutral"
            confidence_boost = 0.0

    return {
        "pair": pair_key,
        "spread": spread_f,
        "spread_abs": spread_abs,
        "sample_count": sample_count,
        "regime": regime,
        "basis_z_score": round(float(z_score), 4),
        "mean_spread": round(float(mean_spread), 10),
        "stdev_spread": round(float(stdev_spread), 10),
        "confidence_boost": confidence_boost,
    }


def _fetch_price(pair):
    """Get spot price from Coinbase."""
    try:
        dp = pair.replace("-USDC", "-USD")
        url = f"https://api.coinbase.com/v2/prices/{dp}/spot"
        req = urllib.request.Request(url, headers={"User-Agent": "StrikeTeam/1.0"})
        resp = urllib.request.urlopen(req, timeout=5)
        return float(json.loads(resp.read())["data"]["amount"])
    except Exception:
        return _latest_local_close(pair)


def _fetch_candles(pair, granularity=60, limit=30):
    """Get recent 1-min candles."""
    remote = []
    try:
        dp = pair.replace("-USDC", "-USD")
        url = f"https://api.exchange.coinbase.com/products/{dp}/candles?granularity={granularity}"
        req = urllib.request.Request(url, headers={"User-Agent": "StrikeTeam/1.0"})
        resp = urllib.request.urlopen(req, timeout=5)
        candles = json.loads(resp.read())
        if isinstance(candles, list):
            remote = candles[:limit]  # [time, low, high, open, close, volume]
    except Exception:
        remote = []
    if len(remote) >= max(1, int(STRIKE_CANDLE_REMOTE_MIN_ROWS)) and not STRIKE_CANDLE_FALLBACK_ENABLED:
        return remote[:limit]
    if len(remote) >= max(1, int(limit)):
        return remote[:limit]

    local = _load_aggregated_candles(pair, granularity=granularity, limit=limit)
    if not local:
        return remote[:limit]
    if not remote:
        return local[:limit]

    # Merge by timestamp, preferring remote rows when both sources have same candle.
    merged = {}
    for row in local:
        try:
            merged[int(row[0])] = row
        except Exception:
            continue
    for row in remote:
        try:
            merged[int(row[0])] = row
        except Exception:
            continue
    out = [merged[k] for k in sorted(merged.keys(), reverse=True)]
    return out[:limit]


def _normalize_pair(pair):
    p = str(pair or "").strip().upper().replace("_", "-")
    if not p:
        return ""
    if "-" not in p:
        p = f"{p}-USD"
    return p


def _pair_variants(pair):
    p = _normalize_pair(pair)
    out = {p}
    if p.endswith("-USD"):
        out.add(p.replace("-USD", "-USDC"))
    elif p.endswith("-USDC"):
        out.add(p.replace("-USDC", "-USD"))
    return [x for x in sorted(out) if x]


def _canonical_dynamic_pair(pair):
    p = _normalize_pair(pair)
    if not p:
        return ""
    if not STRIKE_DYNAMIC_INCLUDE_USDC and p.endswith("-USDC"):
        return p.replace("-USDC", "-USD")
    return p


def _granularity_to_timeframe(granularity):
    g = int(granularity or 60)
    if g <= 60:
        return "1m"
    if g <= 300:
        return "5m"
    if g <= 900:
        return "15m"
    return "1h"


def _open_candle_db():
    conn = sqlite3.connect(str(CANDLE_AGG_DB_PATH), timeout=max(1.0, float(STRIKE_DB_BUSY_TIMEOUT_MS) / 1000.0))
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(max(1000, STRIKE_DB_BUSY_TIMEOUT_MS))}")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _table_exists(conn, name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (str(name),),
    ).fetchone()
    return bool(row)


def _load_aggregated_candles(pair, granularity=60, limit=30):
    if not STRIKE_CANDLE_FALLBACK_ENABLED:
        return []
    if not CANDLE_AGG_DB_PATH.exists():
        return []
    pair_list = _pair_variants(pair)
    if not pair_list:
        return []
    timeframe = _granularity_to_timeframe(granularity)
    placeholders = ",".join("?" for _ in pair_list)
    conn = None
    try:
        conn = _open_candle_db()
        if not _table_exists(conn, "aggregated_candles"):
            return []
        rows = conn.execute(
            f"""
            SELECT start_ts, low, high, open, close, volume
            FROM aggregated_candles
            WHERE pair IN ({placeholders})
              AND timeframe=?
            ORDER BY start_ts DESC
            LIMIT ?
            """,
            tuple(pair_list) + (timeframe, int(max(1, limit))),
        ).fetchall()
        out = []
        for r in rows:
            try:
                out.append(
                    [
                        int(r["start_ts"]),
                        float(r["low"]),
                        float(r["high"]),
                        float(r["open"]),
                        float(r["close"]),
                        max(0.0, float(r["volume"] or 0.0)),
                    ]
                )
            except Exception:
                continue
        return out
    except Exception:
        return []
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _latest_local_close(pair):
    candles = _load_aggregated_candles(pair, granularity=300, limit=1)
    if candles:
        try:
            return float(candles[0][4])
        except Exception:
            return None
    return None


_PAIR_UNIVERSE_CACHE = {"ts": 0.0, "hf": [], "lf": [], "source": "none"}


def _score_pair_window(candles):
    if not candles or len(candles) < 4:
        return None
    closes = []
    opens = []
    highs = []
    lows = []
    vols = []
    for c in candles:
        try:
            opens.append(float(c[3]))
            highs.append(float(c[2]))
            lows.append(float(c[1]))
            closes.append(float(c[4]))
            vols.append(max(0.0, float(c[5] or 0.0)))
        except Exception:
            continue
    if len(closes) < 4:
        return None
    first_open = opens[0] if opens[0] > 0 else closes[0]
    momentum = ((closes[-1] - first_open) / first_open) if first_open > 0 else 0.0
    returns = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        if prev > 0:
            returns.append((closes[i] - prev) / prev)
    if not returns:
        return None
    volatility = statistics.pstdev(returns) if len(returns) > 1 else abs(returns[-1])
    ranges = []
    for i in range(len(opens)):
        o = opens[i]
        if o > 0:
            ranges.append((highs[i] - lows[i]) / o)
    range_pct = statistics.fmean(ranges) if ranges else 0.0
    if len(vols) >= 6:
        recent = statistics.fmean(vols[-3:])
        baseline = statistics.fmean(vols[:-3])
    else:
        recent = statistics.fmean(vols)
        baseline = recent
    volume_ratio = (recent / baseline) if baseline > 0 else (1.0 if recent > 0 else 0.0)
    score = (
        (abs(momentum) * 0.48)
        + (volatility * 0.30)
        + (range_pct * 0.14)
        + (max(0.0, min(3.0, volume_ratio - 1.0)) * 0.08)
    )
    return {
        "score": float(max(0.0, score)),
        "momentum": float(momentum),
        "volatility": float(volatility),
        "volume_ratio": float(volume_ratio),
    }


def _pairs_from_feed(team_type, limit):
    if not CANDLE_FEED_PATH.exists():
        return [], "feed_missing"
    try:
        payload = json.loads(CANDLE_FEED_PATH.read_text())
    except Exception:
        return [], "feed_parse_error"
    points = payload.get("points", []) if isinstance(payload, dict) else []
    if not isinstance(points, list) or not points:
        return [], "feed_points_empty"
    preferred_tfs = {"1m", "5m"} if str(team_type).upper() == "HF" else {"5m", "15m", "1h"}
    grouped = {}
    for row in points:
        if not isinstance(row, dict):
            continue
        tf = str(row.get("timeframe", "")).lower()
        if tf not in preferred_tfs:
            continue
        pair = _normalize_pair(row.get("pair"))
        if not pair:
            continue
        try:
            item = [
                int(row.get("start_ts")),
                float(row.get("low")),
                float(row.get("high")),
                float(row.get("open")),
                float(row.get("close")),
                max(0.0, float(row.get("volume", 0.0) or 0.0)),
            ]
        except Exception:
            continue
        bucket = grouped.setdefault(pair, [])
        bucket.append(item)
    ranked = []
    lookback = max(6, int(STRIKE_DYNAMIC_PAIR_LOOKBACK))
    min_points = max(4, int(STRIKE_DYNAMIC_PAIR_MIN_POINTS))
    for pair, rows in grouped.items():
        rows.sort(key=lambda x: x[0])
        window = rows[-lookback:]
        if len(window) < min_points:
            continue
        scored = _score_pair_window(window)
        if not scored:
            continue
        ranked.append((pair, scored["score"]))
    ranked.sort(key=lambda x: x[1], reverse=True)
    return [p for p, _ in ranked[:limit]], "candle_feed"


def _pairs_from_aggregator_db(team_type, limit):
    if not CANDLE_AGG_DB_PATH.exists():
        return [], "agg_db_missing"
    conn = None
    preferred_tfs = ("1m", "5m") if str(team_type).upper() == "HF" else ("5m", "15m", "1h")
    placeholders = ",".join("?" for _ in preferred_tfs)
    cutoff = int(time.time()) - max(1, int(STRIKE_DYNAMIC_PAIR_LOOKBACK_HOURS)) * 3600
    try:
        conn = _open_candle_db()
        if not _table_exists(conn, "aggregated_candles"):
            return [], "agg_table_missing"
        rows = conn.execute(
            f"""
            SELECT pair, COUNT(*) AS n, AVG(volume) AS avg_vol, MAX(start_ts) AS latest_ts
            FROM aggregated_candles
            WHERE timeframe IN ({placeholders})
              AND start_ts >= ?
            GROUP BY pair
            ORDER BY n DESC, avg_vol DESC, latest_ts DESC
            LIMIT ?
            """,
            tuple(preferred_tfs) + (cutoff, int(max(1, limit))),
        ).fetchall()
        pairs = [_normalize_pair(r["pair"]) for r in rows if _normalize_pair(r["pair"])]
        return pairs[:limit], "agg_db"
    except Exception:
        return [], "agg_db_query_error"
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _dynamic_pair_universe(team_type, defaults):
    if not STRIKE_DYNAMIC_PAIR_SELECTION:
        return list(defaults), "dynamic_disabled"
    now = time.time()
    ttl = max(10, int(STRIKE_DYNAMIC_PAIRS_REFRESH_SECONDS))
    key = "hf" if str(team_type).upper() == "HF" else "lf"
    cached = _PAIR_UNIVERSE_CACHE.get(key, [])
    if cached and (now - float(_PAIR_UNIVERSE_CACHE.get("ts", 0.0))) <= ttl:
        return list(cached), str(_PAIR_UNIVERSE_CACHE.get("source", "cache"))

    limit = max(2, int(STRIKE_DYNAMIC_PAIRS_LIMIT_HF if key == "hf" else STRIKE_DYNAMIC_PAIRS_LIMIT_LF))
    dynamic_pairs, source = _pairs_from_feed(team_type, limit=limit)
    if not dynamic_pairs:
        dynamic_pairs, source = _pairs_from_aggregator_db(team_type, limit=limit)

    out = []
    seen = set()
    for pair in list(defaults) + list(dynamic_pairs):
        p = _canonical_dynamic_pair(pair)
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(p)
    out = out[:limit]
    _PAIR_UNIVERSE_CACHE["ts"] = now
    _PAIR_UNIVERSE_CACHE[key] = list(out)
    _PAIR_UNIVERSE_CACHE["source"] = source
    return out, source


def _execution_health_status(force_refresh=False):
    if not EXECUTION_HEALTH_GATE:
        return {"green": True, "reason": "gate_disabled"}
    fn = evaluate_execution_health
    if fn is None:
        return {"green": False, "reason": "execution_health_module_unavailable"}
    now = time.time()
    ttl = max(5, int(EXECUTION_HEALTH_CACHE_SECONDS))
    if not force_refresh and (now - float(_EXECUTION_HEALTH_CACHE.get("ts", 0.0) or 0.0)) <= ttl:
        cached = _EXECUTION_HEALTH_CACHE.get("payload", {})
        if isinstance(cached, dict) and cached:
            return cached
    try:
        payload = fn(refresh=True, probe_http=False, write_status=True)
    except Exception as e:
        payload = {"green": False, "reason": f"execution_health_exception:{e}"}
    if not isinstance(payload, dict):
        payload = {"green": False, "reason": "execution_health_invalid_payload"}
    _EXECUTION_HEALTH_CACHE["ts"] = now
    _EXECUTION_HEALTH_CACHE["payload"] = payload
    return payload


def _is_execution_health_degraded_allowed(payload):
    reasons = payload.get("reasons", [])
    if not isinstance(reasons, (list, tuple)):
        reasons = [payload.get("reason", "unknown")]
    reasons = [str(r or "").strip().lower() for r in reasons if str(r or "").strip()]
    if not reasons:
        return False
    allowed = tuple(str(v).strip().lower() for v in EXECUTION_HEALTH_DEGRADED_REASONS)
    if not allowed:
        return False
    return all(any(r == allow or r.startswith(f"{allow}:") for allow in allowed) for r in reasons)


def _open_trader_db():
    conn = sqlite3.connect(str(TRADER_DB_PATH), timeout=max(1.0, float(STRIKE_DB_BUSY_TIMEOUT_MS) / 1000.0))
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(max(1000, STRIKE_DB_BUSY_TIMEOUT_MS))}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _ensure_trade_ledger_schema(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT NOT NULL,
            pair TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL,
            quantity REAL,
            total_usd REAL,
            order_type TEXT DEFAULT 'market',
            order_id TEXT,
            status TEXT DEFAULT 'pending',
            pnl REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


class StrikeTeam:
    """Base class for all strike teams."""

    name = "base"
    team_type = "LF"
    pairs = ["BTC-USD", "ETH-USD", "SOL-USD"]

    def __init__(self):
        self.running = False
        self._stop = Event()
        self.scan_count = 0
        self.signals_generated = 0
        self.trades_executed = 0
        self.total_pnl = 0.0
        self.buy_throttled = 0
        self.exec_health_blocked = 0
        self.sell_attempted = 0
        self.sell_completed = 0
        self.sell_failed = 0
        self._sell_close_recent = []
        self.close_first_forced = 0
        self.close_first_forced_blocked = 0
        self.sell_no_inventory_blocked = 0
        self.pairs_active = list(self.pairs)
        self.pairs_refresh_count = 0
        self.pairs_source = "static"

    def _sell_completion_rate(self):
        if not self._sell_close_recent:
            return 1.0
        wins = sum(1 for x in self._sell_close_recent if bool(x))
        return float(wins) / float(len(self._sell_close_recent))

    def _record_sell_completion(self, completed):
        ok = bool(completed)
        self.sell_attempted += 1
        if ok:
            self.sell_completed += 1
        else:
            self.sell_failed += 1
        self._sell_close_recent.append(ok)
        max_window = max(1, int(SELL_CLOSE_WINDOW))
        if len(self._sell_close_recent) > max_window:
            self._sell_close_recent = self._sell_close_recent[-max_window:]

    def _buy_throttle_active(self):
        if not BUY_THROTTLE_ON_SELL_GAP:
            return False
        obs = len(self._sell_close_recent)
        if obs < max(1, int(SELL_CLOSE_MIN_OBS)):
            return False
        return self._sell_completion_rate() < float(SELL_CLOSE_TARGET_RATE)

    def _trade_status_marks(self):
        return ",".join("?" for _ in COMPLETED_TRADE_STATUSES)

    def _estimate_realized_pnl(self, conn, side, pair, qty, gross):
        if str(side or "").upper() != "SELL":
            return 0.0
        qty_f = float(qty or 0.0)
        gross_f = float(gross or 0.0)
        if qty_f <= 0.0 or gross_f <= 0.0:
            return None
        try:
            row = conn.execute(
                f"""
                SELECT
                    COALESCE(SUM(quantity), 0) AS buy_qty,
                    COALESCE(SUM(total_usd), 0) AS buy_usd
                FROM agent_trades
                WHERE agent=?
                  AND pair=?
                  AND side='BUY'
                  AND LOWER(COALESCE(status, '')) IN ({self._trade_status_marks()})
                """,
                (f"strike_{self.name}", pair, *COMPLETED_TRADE_STATUSES),
            ).fetchone()
            buy_qty = float((row["buy_qty"] if row else 0.0) or 0.0)
            buy_usd = float((row["buy_usd"] if row else 0.0) or 0.0)
            if buy_qty <= 0.0:
                return None
            avg_buy = buy_usd / buy_qty
            fee_usd = gross_f * max(0.0, float(TAKER_FEE_RATE))
            return round(gross_f - (qty_f * avg_buy) - fee_usd, 8)
        except Exception:
            return None

    def _record_trade_ledger(self, pair, side, price, quantity, total_usd, order_id=None, status="pending"):
        if not TRADER_DB_PATH.exists():
            try:
                TRADER_DB_PATH.touch()
            except Exception:
                return
        conn = None
        try:
            conn = _open_trader_db()
            _ensure_trade_ledger_schema(conn)
            pnl = self._estimate_realized_pnl(conn, side, pair, quantity, total_usd)
            conn.execute(
                """
                INSERT INTO agent_trades
                    (agent, pair, side, price, quantity, total_usd, order_type, order_id, status, pnl)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"strike_{self.name}",
                    str(pair),
                    str(side).upper(),
                    float(price or 0.0),
                    float(quantity or 0.0),
                    float(total_usd or 0.0),
                    "market",
                    str(order_id or ""),
                    str(status or "pending").lower(),
                    pnl,
                ),
            )
            conn.commit()
        except Exception as e:
            logger.debug("STRIKE %s: trade ledger insert failed: %s", self.name, e)
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    def _position_snapshot(self, pair, lookback_hours=CLOSE_FIRST_POSITION_LOOKBACK_HOURS):
        snap = {
            "pair": str(pair),
            "lookback_hours": int(max(1, int(lookback_hours))),
            "base_qty": 0.0,
            "buy_usd": 0.0,
            "sell_usd": 0.0,
            "net_usd": 0.0,
            "source": "empty",
        }
        if not TRADER_DB_PATH.exists():
            return snap
        conn = None
        try:
            conn = _open_trader_db()
            _ensure_trade_ledger_schema(conn)
            rows = conn.execute(
                f"""
                SELECT side, price, quantity, total_usd
                FROM agent_trades
                WHERE agent=?
                  AND pair=?
                  AND created_at >= datetime('now', ?)
                  AND LOWER(COALESCE(status, '')) IN ({self._trade_status_marks()})
                """,
                (
                    f"strike_{self.name}",
                    str(pair),
                    f"-{int(max(1, int(lookback_hours)))} hours",
                    *COMPLETED_TRADE_STATUSES,
                ),
            ).fetchall()
            qty = 0.0
            buy_usd = 0.0
            sell_usd = 0.0
            for row in rows:
                side = str(row["side"] or "").upper()
                px = float(row["price"] or 0.0)
                q = float(row["quantity"] or 0.0)
                usd = float(row["total_usd"] or 0.0)
                if q <= 0.0 and px > 0.0 and usd > 0.0:
                    q = usd / px
                if side == "BUY":
                    qty += q
                    buy_usd += usd
                elif side == "SELL":
                    qty -= q
                    sell_usd += usd
            snap["base_qty"] = round(max(0.0, qty), 10)
            snap["buy_usd"] = round(float(buy_usd), 6)
            snap["sell_usd"] = round(float(sell_usd), 6)
            snap["net_usd"] = round(float(buy_usd - sell_usd), 6)
            snap["source"] = "trader_db.agent_trades"
            return snap
        except Exception as e:
            snap["source"] = f"error:{e}"
            return snap
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    def _close_first_plan(self, pair, analysis):
        if not CLOSE_FIRST_MODE_ENABLED:
            return {"active": False, "reason": "close_first_disabled"}
        payload = analysis if isinstance(analysis, dict) else {}
        direction = str(payload.get("direction", "BUY") or "BUY").upper()
        if direction != "BUY":
            return {"active": False, "reason": "non_buy_direction"}
        try:
            entry_price = float(payload.get("entry_price", 0.0) or 0.0)
        except Exception:
            entry_price = 0.0
        if entry_price <= 0.0:
            return {"active": False, "reason": "invalid_entry_price"}

        pos = self._position_snapshot(pair)
        open_qty = float(pos.get("base_qty", 0.0) or 0.0)
        if open_qty <= 0.0:
            return {"active": False, "reason": "no_open_position", "position": pos}

        open_notional = open_qty * entry_price
        if open_notional < float(CLOSE_FIRST_MIN_OPEN_NOTIONAL_USD):
            return {
                "active": False,
                "reason": f"open_notional_below_floor:{open_notional:.4f}",
                "position": pos,
            }

        throttle = bool(self._buy_throttle_active())
        low_completion = self._sell_completion_rate() < float(SELL_CLOSE_TARGET_RATE)
        if not (throttle or low_completion):
            return {"active": False, "reason": "close_rate_ok", "position": pos}

        return {
            "active": True,
            "reason": "close_first_enforced",
            "position": pos,
            "sell_qty": open_qty,
            "sell_size_usd": max(1.0, open_notional),
            "trigger": "buy_throttle" if throttle else "sell_completion_rate_low",
        }

    def scout(self, pair):
        """Find opportunities. Override in subclass.
        Returns: {"signal": True/False, "direction": "BUY"/"SELL",
                  "confidence": 0-1, "reason": "..."}
        """
        return {"signal": False}

    def analyze(self, pair, scout_result):
        """Validate and refine a scout signal. Override in subclass.
        Returns: {"approved": True/False, "size_usd": float,
                  "entry_price": float, "stop_loss": float, "take_profit": float}
        """
        return {"approved": False}

    def execute(self, pair, analysis):
        """Execute a validated trade. Uses exchange_connector."""
        if not analysis.get("approved"):
            return None

        direction = str(analysis.get("direction", "BUY") or "BUY").upper()
        size_usd = float(analysis.get("size_usd", 0) or 0.0)
        price = float(analysis.get("entry_price", 0) or 0.0)

        if size_usd < 1.0 or price <= 0:
            return None
        base_amount = size_usd / price

        if direction == "SELL":
            pos = self._position_snapshot(pair)
            open_qty = float(pos.get("base_qty", 0.0) or 0.0)
            if open_qty <= 0.0:
                self.sell_no_inventory_blocked += 1
                logger.info(
                    "STRIKE %s: blocked SELL %s — no tracked inventory (source=%s)",
                    self.name,
                    pair,
                    pos.get("source", "unknown"),
                )
                return None
            base_amount = min(float(base_amount), float(open_qty))
            size_usd = base_amount * price
            if size_usd < 1.0 or base_amount <= 0.0:
                self.sell_no_inventory_blocked += 1
                logger.info(
                    "STRIKE %s: blocked SELL %s — effective close size below floor ($%.4f, qty=%.8f)",
                    self.name,
                    pair,
                    size_usd,
                    base_amount,
                )
                return None

        if EXECUTION_HEALTH_GATE:
            require_gate = True
            allow_degraded_health = False
            if EXECUTION_HEALTH_BUY_ONLY and direction != "BUY":
                require_gate = False
            if require_gate:
                exec_health = _execution_health_status()
                exec_health_reason = str(exec_health.get("reason", "unknown"))
                if not bool(exec_health.get("green", False)):
                    reasons = exec_health.get("reasons", [])
                    if not isinstance(reasons, (list, tuple)):
                        reasons = []
                    hard_blocks = (
                        bool(exec_health.get("egress_blocked", False))
                        or any(str(r).startswith("dns_") for r in reasons)
                        or any(str(r).startswith("reconcile_") for r in reasons)
                        or any(str(r).startswith("candle_feed") for r in reasons)
                        or any(str(r) == "api_probe_failed" for r in reasons)
                    )
                    if hard_blocks:
                        self.exec_health_blocked += 1
                        logger.info(
                            "STRIKE %s: blocked %s %s due to hard execution health condition (%s)",
                            self.name,
                            direction,
                            pair,
                            exec_health_reason,
                        )
                        return None

                    allow_degraded = bool(EXECUTION_HEALTH_DEGRADED_MODE) and _is_execution_health_degraded_allowed(exec_health)
                    if not allow_degraded:
                        self.exec_health_blocked += 1
                        logger.info(
                            "STRIKE %s: blocked %s %s due to execution health gate (%s)",
                            self.name,
                            direction,
                            pair,
                            exec_health_reason,
                        )
                        return None
                    allow_degraded_health = True
                    logger.info(
                        "STRIKE %s: running in execution_health_degraded mode for %s %s",
                        self.name,
                        direction,
                        pair,
                    )
        if direction == "BUY" and self._buy_throttle_active():
            self.buy_throttled += 1
            logger.info(
                "STRIKE %s: BUY throttled on %s (sell_close_rate=%.2f%% target=%.2f%% obs=%d)",
                self.name,
                pair,
                self._sell_completion_rate() * 100.0,
                float(SELL_CLOSE_TARGET_RATE) * 100.0,
                len(self._sell_close_recent),
            )
            return None

        exit_ok, exit_reason = self.validate_profitable_exit(
            pair,
            entry_price=price,
            direction=direction,
            analysis=analysis,
        )
        if not exit_ok:
            logger.info(
                "STRIKE %s: blocked %s %s before execution (exit validation: %s)",
                self.name,
                direction,
                pair,
                exit_reason,
            )
            return None

        # Gate through GoalValidator
        confidence = analysis.get("confidence", 0)
        confirming = analysis.get("confirming_signals", 1)
        regime = analysis.get("regime", "neutral")

        if _goals and not _goals.should_trade(confidence, confirming, direction, regime):
            logger.debug("%s: GoalValidator blocked %s %s (conf=%.2f)",
                         self.name, direction, pair, confidence)
            return None

        # Gate through risk controller
        if _risk:
            # Get FULL portfolio value (cash + positions) for proper risk sizing
            try:
                import urllib.request as _ur
                from exchange_connector import CoinbaseTrader
                _t = CoinbaseTrader()
                accts = _t._request("GET", "/api/v3/brokerage/accounts?limit=250")
                portfolio = 0
                for a in accts.get("accounts", []):
                    cur = a.get("currency", "")
                    bal = float(a.get("available_balance", {}).get("value", 0))
                    if cur in ("USD", "USDC"):
                        portfolio += bal
                    elif bal > 0:
                        try:
                            _url = f"https://api.coinbase.com/v2/prices/{cur}-USD/spot"
                            _req = _ur.Request(_url, headers={"User-Agent": "Strike/1.0"})
                            _resp = _ur.urlopen(_req, timeout=3)
                            _price = float(json.loads(_resp.read())["data"]["amount"])
                            portfolio += bal * _price
                        except Exception:
                            pass
            except Exception:
                portfolio = 250.0  # conservative fallback

            approved, reason, adj_size = _risk.approve_trade(
                self.name, pair, direction, size_usd, portfolio
            )
            if not approved:
                logger.debug("%s: Risk blocked %s %s: %s", self.name, direction, pair, reason)
                return None
            size_usd = adj_size
        if allow_degraded_health:
            size_usd *= float(EXECUTION_HEALTH_DEGRADED_SIZE_FACTOR)
            size_usd = round(float(size_usd), 2)
            if size_usd < 1.0:
                return None

        # Execute via exchange connector
        try:
            from exchange_connector import CoinbaseTrader
            trader = CoinbaseTrader()
            amount = size_usd / price
            result = trader.place_order(pair, direction, amount)
            order_id = None
            if isinstance(result, dict):
                if isinstance(result.get("success_response"), dict):
                    order_id = result["success_response"].get("order_id")
                elif result.get("order_id"):
                    order_id = result.get("order_id")

            # Persist order ACK so reconciliation can track full trade lifecycle.
            if order_id:
                self._record_trade_ledger(
                    pair=pair,
                    side=direction,
                    price=price,
                    quantity=base_amount,
                    total_usd=size_usd,
                    order_id=order_id,
                    status="ack_ok",
                )

            fill = None
            if order_id:
                try:
                    fill = trader.get_order_fill(order_id, max_wait=2, poll_interval=0.4)
                except Exception:
                    fill = None

            status_u = str((fill or {}).get("status", "")).upper()
            filled_sz = float((fill or {}).get("filled_size", 0.0) or 0.0)
            filled_px = float((fill or {}).get("average_filled_price", 0.0) or 0.0)
            filled = status_u in {"FILLED", "PARTIAL_FILLED", "PARTIALLY_FILLED"} or filled_sz > 0.0
            if direction == "SELL":
                self._record_sell_completion(filled)

            if order_id and filled:
                qty = float(filled_sz) if filled_sz > 0 else float(base_amount)
                px = float(filled_px) if filled_px > 0 else float(price)
                usd = qty * px
                self._record_trade_ledger(
                    pair=pair,
                    side=direction,
                    price=px,
                    quantity=qty,
                    total_usd=usd,
                    order_id=order_id,
                    status="filled",
                )

            if result and "success_response" in result:
                self.trades_executed += 1
                logger.info("STRIKE %s: %s %s $%.2f FILLED", self.name, direction, pair, size_usd)

                # Record to KPI
                if _kpi:
                    _kpi.record_trade(
                        strategy_name=f"strike_{self.name}", pair=pair,
                        direction=direction, amount_usd=size_usd,
                        pnl=0, fees=size_usd * 0.004,
                        strategy_type=self.team_type, won=True,
                    )

                # Resolve allocation
                if _risk:
                    _risk.resolve_allocation(self.name, pair)

                return result
            else:
                if _risk:
                    _risk.resolve_allocation(self.name, pair)
                return None

        except Exception as e:
            logger.error("STRIKE %s execute error: %s", self.name, e)
            if _risk:
                _risk.resolve_allocation(self.name, pair)
            return None

    def _required_exit_price(self, entry_price):
        """Return minimum profitable exit price including fees and safety buffer."""
        gross_cost = (2.0 * TAKER_FEE_RATE) + EXIT_BUFFER_RATE
        return float(entry_price) * (1.0 + gross_cost)

    def validate_profitable_exit(self, pair, entry_price, direction, analysis=None):
        """Hard gate for BUY orders: exit must clear costs with margin."""
        side = str(direction or "").upper()
        if side != "BUY":
            return True, "sell_or_non_buy_direction"
        if not entry_price or float(entry_price) <= 0:
            return False, "invalid_entry_price"

        payload = analysis if isinstance(analysis, dict) else {}
        required_exit = self._required_exit_price(float(entry_price))
        explicit_target = 0.0
        for key in ("take_profit", "target_exit_price", "expected_exit_price"):
            val = payload.get(key)
            try:
                explicit_target = max(explicit_target, float(val or 0.0))
            except Exception:
                continue
        if explicit_target > 0.0:
            if explicit_target > required_exit:
                return True, f"explicit_target_ok:{explicit_target:.8f}>{required_exit:.8f}"
            return False, f"explicit_target_below_required:{explicit_target:.8f}<={required_exit:.8f}"

        # No explicit target: infer from microstructure + support/momentum.
        candles = _fetch_candles(pair, granularity=300, limit=30)
        if len(candles) < 12:
            return False, "insufficient_candles_for_exit_validation"

        highs = [float(c[2]) for c in candles[1:16]]
        lows = [float(c[1]) for c in candles[1:16]]
        closes = [float(c[4]) for c in candles[:8]]
        support = min(lows) if lows else 0.0
        inferred_exit = max(highs) if highs else 0.0
        current = float(entry_price)

        near_support = support > 0.0 and ((current - support) / support) <= 0.015
        momentum_up = len(closes) >= 4 and closes[0] > closes[1] > closes[2]
        if inferred_exit <= required_exit:
            return False, f"inferred_exit_below_required:{inferred_exit:.8f}<={required_exit:.8f}"
        if not (near_support or momentum_up):
            return False, "no_support_or_momentum_confirmation"
        return True, f"inferred_exit_ok:{inferred_exit:.8f}>{required_exit:.8f}"

    def has_exit_path(self, pair, entry_price, direction, analysis=None):
        """Backward-compatible bool wrapper around strict exit validation."""
        ok, _reason = self.validate_profitable_exit(pair, entry_price, direction, analysis=analysis)
        return bool(ok)

    def _scan_pairs(self):
        pairs, source = _dynamic_pair_universe(self.team_type, self.pairs)
        if not pairs:
            pairs = list(self.pairs)
            source = "static_fallback"
        if pairs != self.pairs_active or str(source) != str(self.pairs_source):
            self.pairs_active = list(pairs)
            self.pairs_refresh_count += 1
            self.pairs_source = str(source)
            logger.info(
                "STRIKE %s: pair universe refresh source=%s pairs=%s",
                self.name,
                self.pairs_source,
                self.pairs_active,
            )
        return list(self.pairs_active)

    def run(self, interval=60):
        """Main loop: scout → analyze → exit-validate → execute.

        Interval floor prevents over-trading and cash drain.
        """
        self.running = True
        interval = max(int(MIN_SCAN_INTERVAL_SECONDS), int(interval))
        logger.info(
            "Strike team '%s' starting (type=%s, static_pairs=%s, interval=%ds, dynamic_pairs=%s)",
            self.name,
            self.team_type,
            self.pairs,
            interval,
            STRIKE_DYNAMIC_PAIR_SELECTION,
        )

        while self.running and not self._stop.is_set():
            for pair in self._scan_pairs():
                try:
                    self.scan_count += 1

                    # Scout
                    scout_result = self.scout(pair)
                    if not scout_result.get("signal"):
                        continue

                    self.signals_generated += 1
                    logger.info("STRIKE %s: signal on %s — %s (conf=%.2f)",
                                self.name, pair, scout_result.get("reason", ""),
                                scout_result.get("confidence", 0))

                    # Analyze
                    analysis = self.analyze(pair, scout_result)
                    if not analysis.get("approved"):
                        continue

                    close_first = self._close_first_plan(pair, analysis)
                    if close_first.get("active"):
                        self.close_first_forced += 1
                        forced_sell = dict(analysis)
                        forced_sell["direction"] = "SELL"
                        forced_sell["size_usd"] = float(close_first.get("sell_size_usd", analysis.get("size_usd", 0.0)) or 0.0)
                        logger.info(
                            "STRIKE %s: close-first SELL enforced on %s (trigger=%s, open_qty=%.8f, open_notional=$%.2f)",
                            self.name,
                            pair,
                            str(close_first.get("trigger", "unknown")),
                            float(((close_first.get("position", {}) or {}).get("base_qty", 0.0) or 0.0)),
                            float(close_first.get("sell_size_usd", 0.0) or 0.0),
                        )
                        out = self.execute(pair, forced_sell)
                        if out is None:
                            self.close_first_forced_blocked += 1
                        continue

                    # Exit path validation: ONLY buy if profitable exit exists
                    direction = analysis.get("direction", "BUY")
                    entry_price = analysis.get("entry_price", 0)
                    exit_ok, exit_reason = self.validate_profitable_exit(
                        pair, entry_price, direction, analysis=analysis
                    )
                    if not exit_ok:
                        logger.info(
                            "STRIKE %s: no profitable exit path for %s %s — SKIP (%s)",
                            self.name,
                            direction,
                            pair,
                            exit_reason,
                        )
                        continue

                    # Execute
                    self.execute(pair, analysis)

                except Exception as e:
                    logger.error("Strike %s scan error on %s: %s", self.name, pair, e)

            self._stop.wait(interval)

    def stop(self):
        self.running = False
        self._stop.set()

    def status(self):
        return {
            "name": self.name,
            "type": self.team_type,
            "scans": self.scan_count,
            "signals": self.signals_generated,
            "trades": self.trades_executed,
            "pnl": round(self.total_pnl, 4),
            "buy_throttled": int(self.buy_throttled),
            "exec_health_blocked": int(self.exec_health_blocked),
            "sell_attempted": int(self.sell_attempted),
            "sell_completed": int(self.sell_completed),
            "sell_failed": int(self.sell_failed),
            "sell_close_completion_rate": round(self._sell_completion_rate(), 4),
            "close_first_forced": int(self.close_first_forced),
            "close_first_forced_blocked": int(self.close_first_forced_blocked),
            "sell_no_inventory_blocked": int(self.sell_no_inventory_blocked),
            "pairs_source": str(self.pairs_source),
            "pairs_refresh_count": int(self.pairs_refresh_count),
            "active_pairs": list(self.pairs_active),
        }


class MomentumStrike(StrikeTeam):
    """Catches fast momentum moves using price velocity and volume spikes."""

    name = "momentum"
    team_type = "HF"
    pairs = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD"]

    def scout(self, pair):
        candles = _fetch_candles(pair, granularity=60, limit=15)
        if len(candles) < 10:
            return {"signal": False}

        # Candles: [time, low, high, open, close, volume]
        closes = [c[4] for c in candles]
        volumes = [c[5] for c in candles]

        # Price velocity: last 3 candles vs previous 7
        recent = closes[:3]
        baseline = closes[3:10]

        if not recent or not baseline:
            return {"signal": False}

        recent_avg = sum(recent) / len(recent)
        baseline_avg = sum(baseline) / len(baseline)
        velocity = (recent_avg - baseline_avg) / baseline_avg if baseline_avg else 0

        # Volume spike: recent vs baseline
        recent_vol = sum(volumes[:3])
        baseline_vol = sum(volumes[3:10]) / max(len(volumes[3:10]), 1) * 3
        vol_spike = recent_vol / baseline_vol if baseline_vol > 0 else 1

        # Signal: price moving + volume confirming
        if abs(velocity) > 0.002 and vol_spike > 1.5:
            direction = "BUY" if velocity > 0 else "SELL"
            confidence = min(0.95, 0.60 + abs(velocity) * 10 + (vol_spike - 1) * 0.1)
            return {
                "signal": True,
                "direction": direction,
                "confidence": confidence,
                "reason": f"momentum v={velocity:.4f} vol_spike={vol_spike:.1f}x",
                "velocity": velocity,
                "vol_spike": vol_spike,
            }

        return {"signal": False}

    def analyze(self, pair, scout_result):
        price = _fetch_price(pair)
        if not price:
            return {"approved": False}

        confidence = scout_result.get("confidence", 0)
        direction = scout_result.get("direction", "BUY")

        # Only BUY on upward momentum, only SELL on downward
        # Size dynamically
        size = 3.0

        return {
            "approved": confidence >= 0.72,
            "direction": direction,
            "size_usd": size,
            "entry_price": price,
            "confidence": confidence,
            "confirming_signals": 2,
            "regime": "neutral",
        }


class ArbitrageStrike(StrikeTeam):
    """Cross-venue price gap detection."""

    name = "arbitrage"
    team_type = "HF"
    pairs = ["BTC-USD", "ETH-USD"]

    def scout(self, pair):
        # Get Coinbase price
        cb_price = _fetch_price(pair)
        if not cb_price:
            return {"signal": False}

        # Compare with other exchanges via CoinGecko
        try:
            asset = pair.split("-")[0].lower()
            cg_map = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana"}
            cg_id = cg_map.get(asset)
            if not cg_id:
                return {"signal": False}

            url = f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd"
            req = urllib.request.Request(url, headers={"User-Agent": "StrikeTeam/1.0"})
            resp = urllib.request.urlopen(req, timeout=5)
            data = json.loads(resp.read())
            cg_price = data.get(cg_id, {}).get("usd")

            if cg_price and cg_price > 0:
                spread = (cb_price - cg_price) / cg_price
                basis = _classify_arbitrage_basis(pair, spread)
                if abs(spread) >= ARBITRAGE_MIN_SPREAD_THRESHOLD and basis["regime"] != "neutral":
                    if basis["regime"].startswith("normal_"):
                        confidence = min(
                            0.95, 0.70 + abs(spread) * 5 + basis["confidence_boost"]
                        )
                    elif basis["regime"].startswith("stressed_"):
                        confidence = min(
                            0.95, 0.74 + abs(spread) * 5 + basis["confidence_boost"]
                        )
                    else:
                        confidence = min(
                            0.95, 0.65 + abs(spread) * 4 + basis["confidence_boost"]
                        )
                    direction = "SELL" if spread > 0 else "BUY"
                    return {
                        "signal": True,
                        "direction": direction,
                        "confidence": confidence,
                        "reason": (
                            f"arb spread={spread:.4f} cb={cb_price:.2f} mkt={cg_price:.2f} "
                            f"regime={basis['regime']} z={basis['basis_z_score']}"
                        ),
                        "basis": basis,
                    }
        except Exception:
            pass

        return {"signal": False}

    def analyze(self, pair, scout_result):
        price = _fetch_price(pair)
        if not price:
            return {"approved": False}

        return {
            "approved": scout_result.get("confidence", 0) >= 0.75,
            "direction": scout_result.get("direction", "BUY"),
            "size_usd": round(
                3.0
                * {
                    "stressed_coinbase_premium": 1.10,
                    "stressed_coinbase_discount": 1.10,
                    "normal_coinbase_premium": 1.05,
                    "normal_coinbase_discount": 1.05,
                    "bootstrap_coinbase_premium": 0.85,
                    "bootstrap_coinbase_discount": 0.85,
                }.get((scout_result.get("basis") or {}).get("regime", "neutral"), 1.0),
                3,
            ),
            "entry_price": price,
            "confidence": scout_result.get("confidence", 0),
            "confirming_signals": 2,
            "regime": (scout_result.get("basis") or {}).get("regime", "neutral"),
            "basis": scout_result.get("basis"),
        }


class MeanReversionStrike(StrikeTeam):
    """Statistical mean reversion on z-score deviation."""

    name = "mean_reversion"
    team_type = "LF"
    pairs = ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD"]

    def scout(self, pair):
        candles = _fetch_candles(pair, granularity=300, limit=50)
        if len(candles) < 20:
            return {"signal": False}

        closes = [c[4] for c in candles]
        mean = sum(closes) / len(closes)
        std = (sum((c - mean) ** 2 for c in closes) / len(closes)) ** 0.5

        if std <= 0:
            return {"signal": False}

        current = closes[0]
        z_score = (current - mean) / std

        # z > 2: overbought (sell), z < -2: oversold (buy)
        if abs(z_score) > 2.0:
            direction = "BUY" if z_score < -2 else "SELL"
            confidence = min(0.95, 0.65 + (abs(z_score) - 2) * 0.15)
            return {
                "signal": True,
                "direction": direction,
                "confidence": confidence,
                "reason": f"z-score={z_score:.2f} mean={mean:.2f} current={current:.2f}",
            }

        return {"signal": False}

    def analyze(self, pair, scout_result):
        price = _fetch_price(pair)
        if not price:
            return {"approved": False}

        return {
            "approved": scout_result.get("confidence", 0) >= 0.72,
            "direction": scout_result.get("direction", "BUY"),
            "size_usd": 3.0,
            "entry_price": price,
            "confidence": scout_result.get("confidence", 0),
            "confirming_signals": 2,
            "regime": "neutral",
        }


class BreakoutStrike(StrikeTeam):
    """Support/resistance breakout with volume confirmation."""

    name = "breakout"
    team_type = "LF"
    pairs = ["BTC-USD", "ETH-USD", "SOL-USD"]

    def scout(self, pair):
        candles = _fetch_candles(pair, granularity=300, limit=30)
        if len(candles) < 20:
            return {"signal": False}

        # Find support/resistance from recent highs/lows
        highs = [c[2] for c in candles[1:]]  # skip most recent
        lows = [c[1] for c in candles[1:]]
        current_close = candles[0][4]
        current_vol = candles[0][5]

        resistance = max(highs)
        support = min(lows)
        avg_vol = sum(c[5] for c in candles[1:]) / len(candles[1:])

        # Breakout above resistance with volume
        if current_close > resistance and current_vol > avg_vol * 1.5:
            pct_above = (current_close - resistance) / resistance
            confidence = min(0.90, 0.70 + pct_above * 10)
            return {
                "signal": True,
                "direction": "BUY",
                "confidence": confidence,
                "reason": f"breakout above R={resistance:.2f}, vol={current_vol/avg_vol:.1f}x",
            }

        # Breakdown below support with volume
        if current_close < support and current_vol > avg_vol * 1.5:
            pct_below = (support - current_close) / support
            confidence = min(0.90, 0.70 + pct_below * 10)
            return {
                "signal": True,
                "direction": "SELL",
                "confidence": confidence,
                "reason": f"breakdown below S={support:.2f}, vol={current_vol/avg_vol:.1f}x",
            }

        return {"signal": False}

    def analyze(self, pair, scout_result):
        price = _fetch_price(pair)
        if not price:
            return {"approved": False}

        return {
            "approved": scout_result.get("confidence", 0) >= 0.72,
            "direction": scout_result.get("direction", "BUY"),
            "size_usd": 3.0,
            "entry_price": price,
            "confidence": scout_result.get("confidence", 0),
            "confirming_signals": 2,
            "regime": "neutral",
        }


class CorrelationStrike(StrikeTeam):
    """Inter-asset correlation breakdown trades.

    When historically correlated assets diverge, trade the convergence.
    """

    name = "correlation"
    team_type = "LF"
    pairs = ["ETH-USD", "SOL-USD"]

    def scout(self, pair):
        # Compare with BTC as the base
        btc_candles = _fetch_candles("BTC-USD", granularity=300, limit=20)
        pair_candles = _fetch_candles(pair, granularity=300, limit=20)

        if len(btc_candles) < 15 or len(pair_candles) < 15:
            return {"signal": False}

        # Calculate returns
        btc_returns = [(btc_candles[i][4] - btc_candles[i+1][4]) / btc_candles[i+1][4]
                       for i in range(min(len(btc_candles), len(pair_candles)) - 1)]
        pair_returns = [(pair_candles[i][4] - pair_candles[i+1][4]) / pair_candles[i+1][4]
                        for i in range(min(len(btc_candles), len(pair_candles)) - 1)]

        if len(btc_returns) < 5:
            return {"signal": False}

        # Simple correlation
        n = len(btc_returns)
        mean_b = sum(btc_returns) / n
        mean_p = sum(pair_returns) / n
        cov = sum((btc_returns[i] - mean_b) * (pair_returns[i] - mean_p) for i in range(n)) / n
        std_b = (sum((r - mean_b) ** 2 for r in btc_returns) / n) ** 0.5
        std_p = (sum((r - mean_p) ** 2 for r in pair_returns) / n) ** 0.5

        if std_b <= 0 or std_p <= 0:
            return {"signal": False}

        correlation = cov / (std_b * std_p)

        # Recent divergence: last 3 returns diverge from correlation
        recent_btc = sum(btc_returns[:3]) / 3
        recent_pair = sum(pair_returns[:3]) / 3

        # If BTC up but pair lagging (and normally correlated), buy the pair
        if correlation > 0.5 and recent_btc > 0.003 and recent_pair < 0:
            confidence = min(0.88, 0.65 + correlation * 0.2)
            return {
                "signal": True,
                "direction": "BUY",
                "confidence": confidence,
                "reason": f"corr={correlation:.2f} BTC↑{recent_btc:.4f} {pair}↓{recent_pair:.4f}",
            }

        # If BTC down but pair still up (and normally correlated), sell the pair
        if correlation > 0.5 and recent_btc < -0.003 and recent_pair > 0:
            confidence = min(0.88, 0.65 + correlation * 0.2)
            return {
                "signal": True,
                "direction": "SELL",
                "confidence": confidence,
                "reason": f"corr={correlation:.2f} BTC↓{recent_btc:.4f} {pair}↑{recent_pair:.4f}",
            }

        return {"signal": False}

    def analyze(self, pair, scout_result):
        price = _fetch_price(pair)
        if not price:
            return {"approved": False}

        return {
            "approved": scout_result.get("confidence", 0) >= 0.72,
            "direction": scout_result.get("direction", "BUY"),
            "size_usd": 3.0,
            "entry_price": price,
            "confidence": scout_result.get("confidence", 0),
            "confirming_signals": 2,
            "regime": "neutral",
        }


# ── Strike Team Manager ────────────────────────────────────────────────────

ALL_TEAMS = [
    MomentumStrike,
    ArbitrageStrike,
    MeanReversionStrike,
    BreakoutStrike,
    CorrelationStrike,
]


class StrikeTeamManager:
    """Manages all strike teams as threads with evolutionary selection."""

    def __init__(self):
        self.teams = {}
        self.threads = {}

    def deploy_all(self):
        """Launch all strike teams as daemon threads."""
        for team_cls in ALL_TEAMS:
            team = team_cls()
            self.teams[team.name] = team
            base_interval = (
                int(DEFAULT_HF_INTERVAL_SECONDS)
                if str(team.team_type).upper() == "HF"
                else int(DEFAULT_LF_INTERVAL_SECONDS)
            )
            team_interval = max(int(MIN_SCAN_INTERVAL_SECONDS), int(base_interval))
            t = Thread(target=team.run, args=(team_interval,), daemon=True, name=f"strike-{team.name}")
            t.start()
            self.threads[team.name] = t
            logger.info("Deployed strike team: %s (type=%s, interval=%ds)", team.name, team.team_type, team_interval)

        logger.info("All %d strike teams deployed", len(self.teams))

    def stop_all(self):
        """Stop all strike teams."""
        for name, team in self.teams.items():
            team.stop()
        self.teams.clear()
        self.threads.clear()

    def status(self):
        """Get status of all teams."""
        return {
            name: {
                **team.status(),
                "alive": self.threads.get(name, None) is not None and self.threads[name].is_alive(),
            }
            for name, team in self.teams.items()
        }

    def rankings(self):
        """Rank teams by performance for capital allocation."""
        stats = []
        for name, team in self.teams.items():
            s = team.status()
            # Score: trades * win_implied * pnl_direction
            score = s["trades"] * max(s["pnl"], 0.01) if s["trades"] > 0 else 0
            stats.append({**s, "score": score})
        return sorted(stats, key=lambda x: x["score"], reverse=True)


# Singleton
_manager = None

def get_strike_manager():
    global _manager
    if _manager is None:
        _manager = StrikeTeamManager()
    return _manager
