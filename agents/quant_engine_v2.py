#!/usr/bin/env python3
"""Multi-Strategy Quantitative Trading Engine v2.

Production-grade engine running five simultaneous strategies with independent
risk management, signal aggregation, and unified execution routing.

Strategies:
  1. Mean Reversion  — VWAP deviation on BTC/ETH/SOL (30-min candles)
  2. Momentum        — EMA crossover with volume confirmation (crypto + equities)
  3. Latency Arb     — RTT anomaly exploitation from our traceroute scanner
  4. Statistical Arb  — Pairs trading on correlated crypto ratios
  5. Sentiment Alpha — HackerNews/Reddit mention velocity vs price divergence

Architecture:
  StrategyEngine -> [BaseStrategy x5] -> SignalAggregator -> RiskManager -> TradeExecutor

Risk Rules (HARD CODED, NEVER overridden):
  Rule #1: NEVER LOSE MONEY — every trade has a stop loss
  Rule #2: Always make money — compound wins, cut losses fast
  Rule #3: Grow faster — reinvest profits, scale with account growth

CLI:
  python quant_engine_v2.py run              Start all strategies
  python quant_engine_v2.py status           Show strategy performance
  python quant_engine_v2.py backtest --days 7  Run on historical data
  python quant_engine_v2.py signals          Show current live signals

Dependencies: Python stdlib only (no pip installs).
State: SQLite at agents/quant_v2.db
Logs:  agents/quant_engine.log
"""

import collections
import enum
import hashlib
import json
import logging
import math
import os
import sqlite3
import statistics
import sys
import textwrap
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

AGENTS_DIR = Path(__file__).parent
ENGINE_DB = str(AGENTS_DIR / "quant_v2.db")
TRACEROUTE_DB = str(AGENTS_DIR.parent / "traceroute.db")
LOG_FILE = str(AGENTS_DIR / "quant_engine.log")
CANDLE_AGG_DB = str(AGENTS_DIR / "candle_aggregator.db")

# ---------------------------------------------------------------------------
# Logging — dual handler: file + stderr
# ---------------------------------------------------------------------------

_log_formatter = logging.Formatter(
    "%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_file_handler = logging.FileHandler(LOG_FILE)
_file_handler.setFormatter(_log_formatter)
_file_handler.setLevel(logging.DEBUG)

_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_log_formatter)
_stream_handler.setLevel(logging.INFO)

logger = logging.getLogger("quant_v2")
logger.setLevel(logging.DEBUG)
logger.addHandler(_file_handler)
logger.addHandler(_stream_handler)


# ============================================================================
# ENUMS & DATA CLASSES
# ============================================================================

class Side(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


class AssetClass(enum.Enum):
    CRYPTO = "CRYPTO"
    EQUITY = "EQUITY"


@dataclass
class Signal:
    """A trading signal produced by a strategy."""
    strategy: str
    symbol: str
    side: Side
    confidence: float          # 0.0 — 1.0
    entry_price: float
    stop_price: float
    target_price: float
    size_usd: float
    reason: str
    asset_class: AssetClass = AssetClass.CRYPTO
    timestamp: float = field(default_factory=time.time)
    signal_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    @property
    def risk_reward(self) -> float:
        """Reward-to-risk ratio. Higher is better."""
        risk = abs(self.entry_price - self.stop_price)
        reward = abs(self.target_price - self.entry_price)
        if risk == 0:
            return 0.0
        return reward / risk


@dataclass
class Position:
    """A live position held by a strategy."""
    position_id: str
    strategy: str
    symbol: str
    side: Side
    entry_price: float
    current_price: float
    quantity: float
    size_usd: float
    stop_price: float
    target_price: float
    trailing_stop: float
    opened_at: float
    unrealized_pnl: float = 0.0
    high_water_mark: float = 0.0  # best price seen (for trailing stops)

    @property
    def pnl_pct(self) -> float:
        if self.entry_price == 0:
            return 0.0
        if self.side == Side.BUY:
            return (self.current_price - self.entry_price) / self.entry_price
        return (self.entry_price - self.current_price) / self.entry_price


@dataclass
class Fill:
    """An executed trade fill."""
    fill_id: str
    signal_id: str
    strategy: str
    symbol: str
    side: Side
    price: float
    quantity: float
    total_usd: float
    slippage_bps: float
    timestamp: float
    asset_class: AssetClass


@dataclass
class ExecutionReport:
    """Detailed order lifecycle report for execution observability."""
    report_id: str
    signal_id: str
    strategy: str
    symbol: str
    side: Side
    mode: str
    venue: str
    status: str              # filled | rejected | error
    requested_price: float
    executed_price: float
    requested_qty: float
    filled_qty: float
    requested_usd: float
    filled_usd: float
    slippage_bps: float
    latency_ms: float
    error: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


# ============================================================================
# MARKET DATA FEEDS  (all free, no auth)
# ============================================================================

class MarketData:
    """Unified market data feed. Coinbase candles + Yahoo Finance + caching."""

    _cache: Dict[str, Any] = {}
    _candle_meta: Dict[str, Dict[str, Any]] = {}
    _cache_ttl = 8  # seconds
    _user_agent = "QuantEngine/2.0"

    # ----- spot prices -----

    @classmethod
    def get_price(cls, symbol: str) -> Optional[float]:
        """Get current spot price for a symbol (crypto or equity)."""
        key = f"price:{symbol}"
        now = time.time()
        if key in cls._cache and now - cls._cache[key]["t"] < cls._cache_ttl:
            return cls._cache[key]["v"]

        price = None
        if cls._is_crypto(symbol):
            price = cls._coinbase_spot(symbol)
        if price is None:
            price = cls._yahoo_price(symbol)

        if price is not None:
            cls._cache[key] = {"v": price, "t": now}
        return price

    @classmethod
    def get_prices(cls, symbols: List[str]) -> Dict[str, Optional[float]]:
        return {s: cls.get_price(s) for s in symbols}

    # ----- candles -----

    @classmethod
    def get_candles(cls, symbol: str, granularity_seconds: int = 300,
                    limit: int = 100) -> List[dict]:
        """Return candles as list of dicts with keys:
        start, low, high, open, close, volume."""
        key = f"candles:{symbol}:{granularity_seconds}:{limit}"
        now = time.time()
        if key in cls._cache and now - cls._cache[key]["t"] < 30:
            return cls._cache[key]["v"]

        candles = []
        source = "none"
        if cls._is_crypto(symbol):
            candles = cls._coinbase_candles(symbol, granularity_seconds, limit)
            if candles:
                source = "coinbase"
        if not candles:
            candles = cls._yahoo_candles(symbol, granularity_seconds, limit)
            if candles:
                source = "yahoo"
        if not candles:
            candles = cls._local_aggregated_candles(symbol, granularity_seconds, limit)
            if candles:
                source = "candle_aggregator"

        cleaned, quality = cls._sanitize_candles(candles, granularity_seconds)
        quality.update({
            "symbol": symbol,
            "granularity_seconds": granularity_seconds,
            "limit": limit,
            "source": source,
        })
        cls._candle_meta[key] = quality

        if cleaned:
            cls._cache[key] = {"v": cleaned, "t": now}
        return cleaned

    @classmethod
    def get_candle_meta(cls, symbol: str, granularity_seconds: int = 300,
                        limit: int = 100) -> Dict[str, Any]:
        """Return quality metadata for the most recent candle fetch."""
        key = f"candles:{symbol}:{granularity_seconds}:{limit}"
        if key not in cls._candle_meta:
            cls.get_candles(symbol, granularity_seconds, limit)
        return dict(cls._candle_meta.get(key, {}))

    @staticmethod
    def _sanitize_candles(candles: List[dict],
                          expected_step_sec: int) -> Tuple[List[dict], Dict[str, Any]]:
        """Normalize candle rows and collect quality diagnostics."""
        meta = {
            "raw_count": len(candles),
            "clean_count": 0,
            "invalid_rows": 0,
            "duplicates_removed": 0,
            "had_out_of_order": False,
            "gap_count": 0,
            "max_gap_seconds": 0,
            "coverage_ratio": 0.0,
        }
        if not candles:
            return [], meta

        normalized = []
        for c in candles:
            try:
                normalized.append({
                    "start": int(c["start"]),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                    "volume": max(0.0, float(c.get("volume", 0.0))),
                })
            except (KeyError, TypeError, ValueError):
                meta["invalid_rows"] += 1

        if not normalized:
            return [], meta

        starts_before = [c["start"] for c in normalized]
        normalized.sort(key=lambda x: x["start"])
        starts_after = [c["start"] for c in normalized]
        meta["had_out_of_order"] = starts_before != starts_after

        deduped: List[dict] = []
        for row in normalized:
            if deduped and deduped[-1]["start"] == row["start"]:
                # Merge duplicates deterministically.
                prev = deduped[-1]
                prev["high"] = max(prev["high"], row["high"])
                prev["low"] = min(prev["low"], row["low"])
                prev["close"] = row["close"]
                prev["volume"] += row["volume"]
                meta["duplicates_removed"] += 1
            else:
                deduped.append(dict(row))

        gaps = []
        for i in range(1, len(deduped)):
            delta = deduped[i]["start"] - deduped[i - 1]["start"]
            if delta > int(expected_step_sec * 1.5):
                gaps.append(delta)

        span = deduped[-1]["start"] - deduped[0]["start"] if len(deduped) > 1 else 0
        expected_points = int(span / expected_step_sec) + 1 if span > 0 else len(deduped)
        coverage = (len(deduped) / expected_points) if expected_points > 0 else 1.0

        meta["clean_count"] = len(deduped)
        meta["gap_count"] = len(gaps)
        meta["max_gap_seconds"] = max(gaps) if gaps else 0
        meta["coverage_ratio"] = round(coverage, 4)
        return deduped, meta

    # ----- internal helpers -----

    @staticmethod
    def _is_crypto(symbol: str) -> bool:
        crypto_bases = {
            "BTC", "ETH", "SOL", "XRP", "ADA", "DOGE", "AVAX",
            "DOT", "LINK", "MATIC", "UNI", "AAVE",
        }
        base = symbol.split("-")[0].upper() if "-" in symbol else symbol.upper()
        return base in crypto_bases

    @classmethod
    def _http_get(cls, url: str, timeout: int = 8) -> Optional[dict]:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": cls._user_agent})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            logger.debug("HTTP GET failed %s: %s", url, e)
            return None

    @classmethod
    def _coinbase_spot(cls, symbol: str) -> Optional[float]:
        pair = symbol if "-" in symbol else f"{symbol}-USD"
        base, quote = pair.split("-", 1)
        url = f"https://api.coinbase.com/v2/prices/{base}-{quote}/spot"
        data = cls._http_get(url)
        if data:
            try:
                return float(data["data"]["amount"])
            except (KeyError, TypeError, ValueError):
                pass
        return None

    @classmethod
    def _coinbase_candles(cls, symbol: str, gran_sec: int, limit: int) -> List[dict]:
        """Coinbase Advanced Trade public candle endpoint."""
        pair = symbol if "-" in symbol else f"{symbol}-USD"
        gran_map = {
            60: "ONE_MINUTE", 300: "FIVE_MINUTE", 900: "FIFTEEN_MINUTE",
            1800: "THIRTY_MINUTE", 3600: "ONE_HOUR", 21600: "SIX_HOUR",
            86400: "ONE_DAY",
        }
        # Snap to nearest supported granularity
        best_gran = min(gran_map.keys(), key=lambda g: abs(g - gran_sec))
        gran_str = gran_map[best_gran]

        end = int(time.time())
        start = end - (limit * best_gran)
        url = (
            f"https://api.coinbase.com/api/v3/brokerage/market/products/"
            f"{pair}/candles?start={start}&end={end}&granularity={gran_str}"
        )
        data = cls._http_get(url)
        if not data:
            return []

        raw = data.get("candles", [])
        candles = []
        for c in raw:
            try:
                candles.append({
                    "start": int(c["start"]),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                    "volume": float(c["volume"]),
                })
            except (KeyError, TypeError, ValueError):
                continue
        candles.sort(key=lambda c: c["start"])
        return candles

    @classmethod
    def _yahoo_price(cls, symbol: str) -> Optional[float]:
        ticker = symbol.replace("-USD", "").replace("-", "")
        url = (
            f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
            f"?interval=1m&range=1d"
        )
        data = cls._http_get(url)
        if data:
            try:
                result = data["chart"]["result"][0]
                return float(result["meta"]["regularMarketPrice"])
            except (KeyError, TypeError, ValueError, IndexError):
                pass
        return None

    @classmethod
    def _yahoo_candles(cls, symbol: str, gran_sec: int, limit: int) -> List[dict]:
        ticker = symbol.replace("-USD", "").replace("-", "")
        interval_map = {
            60: "1m", 300: "5m", 900: "15m", 1800: "30m",
            3600: "1h", 86400: "1d",
        }
        best = min(interval_map.keys(), key=lambda g: abs(g - gran_sec))
        interval = interval_map[best]
        range_days = max(1, (limit * best) // 86400 + 1)
        range_str = f"{min(range_days, 30)}d"

        url = (
            f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
            f"?interval={interval}&range={range_str}"
        )
        data = cls._http_get(url)
        if not data:
            return []

        try:
            result = data["chart"]["result"][0]
            timestamps = result["timestamp"]
            quotes = result["indicators"]["quote"][0]
            candles = []
            for i, ts in enumerate(timestamps):
                o = quotes["open"][i]
                h = quotes["high"][i]
                lo = quotes["low"][i]
                c = quotes["close"][i]
                v = quotes["volume"][i]
                if None in (o, h, lo, c):
                    continue
                candles.append({
                    "start": int(ts),
                    "open": float(o), "high": float(h),
                    "low": float(lo), "close": float(c),
                    "volume": float(v or 0),
                })
            return candles[-limit:]
        except (KeyError, TypeError, IndexError):
            return []

    @classmethod
    def _local_aggregated_candles(cls, symbol: str, gran_sec: int, limit: int) -> List[dict]:
        db_path = Path(CANDLE_AGG_DB)
        if not db_path.exists():
            return []

        pair = symbol if "-" in symbol else f"{symbol}-USD"
        pair = pair.upper()
        pair_variants = {pair}
        if pair.endswith("-USD"):
            pair_variants.add(pair.replace("-USD", "-USDC"))
        if pair.endswith("-USDC"):
            pair_variants.add(pair.replace("-USDC", "-USD"))

        tf_map = {
            60: "1m",
            300: "5m",
            900: "15m",
            3600: "1h",
        }
        tf = tf_map.get(min(tf_map.keys(), key=lambda g: abs(g - int(gran_sec))), "5m")
        placeholders = ",".join("?" for _ in pair_variants)
        sql = f"""
            SELECT start_ts, open, high, low, close, volume
            FROM aggregated_candles
            WHERE pair IN ({placeholders})
              AND timeframe=?
            ORDER BY start_ts DESC
            LIMIT ?
        """
        try:
            conn = sqlite3.connect(str(db_path))
            rows = conn.execute(sql, tuple(pair_variants) + (tf, max(1, int(limit)))).fetchall()
            conn.close()
        except Exception:
            return []
        candles = []
        for ts, o, h, l, c, v in reversed(rows):
            try:
                candles.append(
                    {
                        "start": int(ts),
                        "open": float(o),
                        "high": float(h),
                        "low": float(l),
                        "close": float(c),
                        "volume": float(v or 0.0),
                    }
                )
            except Exception:
                continue
        return candles


# ============================================================================
# MATH UTILITIES (stdlib only)
# ============================================================================

def ema(values: List[float], period: int) -> List[float]:
    """Exponential Moving Average. Returns list same length as input
    (first `period-1` values use cumulative average)."""
    if not values or period <= 0:
        return []
    result = []
    k = 2.0 / (period + 1)
    avg = values[0]
    result.append(avg)
    for v in values[1:]:
        avg = v * k + avg * (1 - k)
        result.append(avg)
    return result


def sma(values: List[float], period: int) -> List[float]:
    """Simple Moving Average with leading partial-window averages."""
    if not values:
        return []
    result = []
    for i in range(len(values)):
        window = values[max(0, i - period + 1):i + 1]
        result.append(sum(window) / len(window))
    return result


def std_dev(values: List[float]) -> float:
    """Population standard deviation."""
    if len(values) < 2:
        return 0.0
    return statistics.pstdev(values)


def z_score(value: float, mean: float, sd: float) -> float:
    if sd == 0:
        return 0.0
    return (value - mean) / sd


def vwap(candles: List[dict]) -> float:
    """Volume-Weighted Average Price from candle data."""
    total_vp = 0.0
    total_vol = 0.0
    for c in candles:
        typical = (c["high"] + c["low"] + c["close"]) / 3.0
        vol = c["volume"]
        total_vp += typical * vol
        total_vol += vol
    if total_vol == 0:
        return 0.0
    return total_vp / total_vol


def rolling_vwap(candles: List[dict], window: int) -> List[float]:
    """Rolling VWAP over a sliding window of candles."""
    result = []
    for i in range(len(candles)):
        start = max(0, i - window + 1)
        result.append(vwap(candles[start:i + 1]))
    return result


def pearson_correlation(x: List[float], y: List[float]) -> float:
    """Pearson correlation coefficient between two series."""
    n = min(len(x), len(y))
    if n < 3:
        return 0.0
    x, y = x[:n], y[:n]
    mx = sum(x) / n
    my = sum(y) / n
    sx = math.sqrt(sum((xi - mx) ** 2 for xi in x) / n)
    sy = math.sqrt(sum((yi - my) ** 2 for yi in y) / n)
    if sx == 0 or sy == 0:
        return 0.0
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n)) / n
    return cov / (sx * sy)


def sharpe_ratio(returns: List[float], risk_free_rate: float = 0.0) -> float:
    """Annualized Sharpe ratio from a list of periodic returns."""
    if len(returns) < 2:
        return 0.0
    excess = [r - risk_free_rate for r in returns]
    avg = sum(excess) / len(excess)
    sd = std_dev(excess)
    if sd == 0:
        return 0.0
    # Assume ~252 trading days, ~78 five-minute bars per day
    periods_per_year = 252 * 78
    return (avg / sd) * math.sqrt(periods_per_year)


# ============================================================================
# BASE STRATEGY
# ============================================================================

class BaseStrategy(ABC):
    """Abstract base for all trading strategies."""

    def __init__(self, name: str, symbols: List[str],
                 max_position_usd: float = 500.0):
        self.name = name
        self.symbols = symbols
        self.max_position_usd = max_position_usd
        self._positions: Dict[str, Position] = {}
        self._closed_trades: List[dict] = []
        self._returns: List[float] = []
        self._win_count = 0
        self._loss_count = 0
        self._total_pnl = 0.0
        self._enabled = True

    @abstractmethod
    def update(self, market_data: Dict[str, Any]) -> List[Signal]:
        """Process new market data and return any signals."""
        ...

    def get_positions(self) -> List[Position]:
        return list(self._positions.values())

    def has_position(self, symbol: str) -> bool:
        return symbol in self._positions

    @staticmethod
    def _snapshot_key(symbol: str, granularity: int, limit: int) -> str:
        return f"{symbol}:{granularity}:{limit}"

    def _get_candles(self, market_data: Dict[str, Any], symbol: str,
                     granularity: int, limit: int) -> List[dict]:
        if isinstance(market_data, dict):
            candle_map = market_data.get("candles", {})
            key = self._snapshot_key(symbol, granularity, limit)
            if key in candle_map:
                return candle_map[key]
            prefix = f"{symbol}:{granularity}:"
            for k, rows in candle_map.items():
                if k.startswith(prefix) and isinstance(rows, list) and len(rows) >= limit:
                    return rows[-limit:]
        return MarketData.get_candles(symbol, granularity, limit)

    def _get_price(self, market_data: Dict[str, Any], symbol: str) -> Optional[float]:
        if isinstance(market_data, dict):
            price_map = market_data.get("prices", {})
            if symbol in price_map:
                return price_map[symbol]
        return MarketData.get_price(symbol)

    def record_fill(self, fill: Fill, signal: Signal) -> Optional[Position]:
        """Record an executed fill, opening or closing a position."""
        symbol = fill.symbol
        if symbol in self._positions:
            # Closing an existing position
            pos = self._positions.pop(symbol)
            if pos.side == Side.BUY:
                pnl = (fill.price - pos.entry_price) * pos.quantity
            else:
                pnl = (pos.entry_price - fill.price) * pos.quantity
            pnl_pct = pnl / pos.size_usd if pos.size_usd else 0.0
            self._total_pnl += pnl
            self._returns.append(pnl_pct)
            if pnl > 0:
                self._win_count += 1
            else:
                self._loss_count += 1
            self._closed_trades.append({
                "symbol": symbol, "pnl": pnl, "pnl_pct": pnl_pct,
                "entry": pos.entry_price, "exit": fill.price,
                "side": pos.side.value, "duration": time.time() - pos.opened_at,
            })
            logger.info("[%s] CLOSED %s %s pnl=$%.2f (%.2f%%)",
                        self.name, pos.side.value, symbol, pnl, pnl_pct * 100)
            return None
        else:
            # Opening a new position
            pos = Position(
                position_id=fill.fill_id,
                strategy=self.name,
                symbol=symbol,
                side=fill.side,
                entry_price=fill.price,
                current_price=fill.price,
                quantity=fill.quantity,
                size_usd=fill.total_usd,
                stop_price=signal.stop_price,
                target_price=signal.target_price,
                trailing_stop=signal.stop_price,
                opened_at=time.time(),
                high_water_mark=fill.price,
            )
            self._positions[symbol] = pos
            logger.info("[%s] OPENED %s %s @ $%.2f stop=$%.2f target=$%.2f",
                        self.name, fill.side.value, symbol, fill.price,
                        signal.stop_price, signal.target_price)
            return pos

    def update_position_prices(self, prices: Dict[str, float]):
        """Update current prices and trailing stops on open positions."""
        for symbol, pos in list(self._positions.items()):
            if symbol in prices and prices[symbol] is not None:
                pos.current_price = prices[symbol]
                if pos.side == Side.BUY:
                    pos.unrealized_pnl = (pos.current_price - pos.entry_price) * pos.quantity
                    if pos.current_price > pos.high_water_mark:
                        pos.high_water_mark = pos.current_price
                        # RULE: NEVER let a winning trade become a losing trade
                        if pos.high_water_mark > pos.entry_price * 1.003:
                            new_trail = pos.high_water_mark * 0.995
                            pos.trailing_stop = max(pos.trailing_stop, new_trail)
                else:
                    pos.unrealized_pnl = (pos.entry_price - pos.current_price) * pos.quantity
                    if pos.current_price < pos.high_water_mark or pos.high_water_mark == 0:
                        pos.high_water_mark = pos.current_price
                        if pos.high_water_mark < pos.entry_price * 0.997:
                            new_trail = pos.high_water_mark * 1.005
                            pos.trailing_stop = min(pos.trailing_stop, new_trail)

    def check_stops(self) -> List[Signal]:
        """Check if any position has hit its stop or target. Returns exit signals."""
        exits = []
        for symbol, pos in list(self._positions.items()):
            price = pos.current_price
            if price <= 0:
                continue
            should_exit = False
            reason = ""

            if pos.side == Side.BUY:
                # Hard stop — NEVER exceed
                if price <= pos.stop_price:
                    should_exit = True
                    reason = f"HARD STOP hit: ${price:.2f} <= ${pos.stop_price:.2f}"
                # Trailing stop
                elif price <= pos.trailing_stop and pos.trailing_stop > pos.stop_price:
                    should_exit = True
                    reason = f"TRAILING STOP: ${price:.2f} <= ${pos.trailing_stop:.2f}"
                # Target hit
                elif price >= pos.target_price:
                    should_exit = True
                    reason = f"TARGET HIT: ${price:.2f} >= ${pos.target_price:.2f}"
            else:  # SELL / short
                if price >= pos.stop_price:
                    should_exit = True
                    reason = f"HARD STOP hit: ${price:.2f} >= ${pos.stop_price:.2f}"
                elif price >= pos.trailing_stop and pos.trailing_stop < pos.stop_price:
                    should_exit = True
                    reason = f"TRAILING STOP: ${price:.2f} >= ${pos.trailing_stop:.2f}"
                elif price <= pos.target_price:
                    should_exit = True
                    reason = f"TARGET HIT: ${price:.2f} <= ${pos.target_price:.2f}"

            if should_exit:
                exit_side = Side.SELL if pos.side == Side.BUY else Side.BUY
                exits.append(Signal(
                    strategy=self.name,
                    symbol=symbol,
                    side=exit_side,
                    confidence=1.0,
                    entry_price=price,
                    stop_price=price,
                    target_price=price,
                    size_usd=pos.size_usd,
                    reason=f"EXIT: {reason}",
                    asset_class=AssetClass.CRYPTO if MarketData._is_crypto(symbol) else AssetClass.EQUITY,
                ))
        return exits

    def performance(self) -> dict:
        total = self._win_count + self._loss_count
        win_rate = self._win_count / total if total > 0 else 0.0
        sr = sharpe_ratio(self._returns) if len(self._returns) >= 2 else 0.0
        unrealized = sum(p.unrealized_pnl for p in self._positions.values())
        return {
            "strategy": self.name,
            "enabled": self._enabled,
            "total_trades": total,
            "wins": self._win_count,
            "losses": self._loss_count,
            "win_rate": win_rate,
            "total_pnl": self._total_pnl,
            "unrealized_pnl": unrealized,
            "sharpe": sr,
            "open_positions": len(self._positions),
            "symbols": self.symbols,
        }


# ============================================================================
# STRATEGY 1: MEAN REVERSION (Crypto VWAP)
# ============================================================================

class MeanReversionStrategy(BaseStrategy):
    """Trade reversions to 30-minute VWAP on BTC, ETH, SOL.

    Entry: price deviates >1.5 sigma from rolling VWAP.
    Exit:  price returns to VWAP or 0.5 sigma on the other side.
    Stop:  2.5 sigma (HARD, NEVER exceed).
    """

    ENTRY_SIGMA = 1.5
    EXIT_SIGMA = 0.5
    HARD_STOP_SIGMA = 2.5
    VWAP_WINDOW = 60  # candles for rolling VWAP (60 x 5min = 5 hours context)
    CANDLE_GRANULARITY = 300  # 5-minute candles
    CANDLE_LIMIT = 80

    def __init__(self):
        super().__init__(
            name="mean_reversion",
            symbols=["BTC-USD", "ETH-USD", "SOL-USD"],
            max_position_usd=500.0,
        )
        self._candle_cache: Dict[str, List[dict]] = {}

    def update(self, market_data: Dict[str, Any]) -> List[Signal]:
        signals = []

        for symbol in self.symbols:
            try:
                candles = self._get_candles(
                    market_data, symbol, self.CANDLE_GRANULARITY, self.CANDLE_LIMIT
                )
                if len(candles) < 20:
                    continue
                self._candle_cache[symbol] = candles

                # Compute rolling VWAP
                vwap_val = vwap(candles[-self.VWAP_WINDOW:])
                if vwap_val <= 0:
                    continue

                # Compute standard deviation of close prices around VWAP
                closes = [c["close"] for c in candles[-self.VWAP_WINDOW:]]
                deviations = [c - vwap_val for c in closes]
                sigma = std_dev(deviations)
                if sigma <= 0:
                    continue

                current_price = candles[-1]["close"]
                z = z_score(current_price, vwap_val, sigma)

                # Skip if we already have a position
                if self.has_position(symbol):
                    # Check for exit: price returned to VWAP or crossed to other side
                    pos = self._positions[symbol]
                    if pos.side == Side.BUY and z >= -self.EXIT_SIGMA:
                        signals.append(Signal(
                            strategy=self.name, symbol=symbol, side=Side.SELL,
                            confidence=0.9, entry_price=current_price,
                            stop_price=current_price, target_price=current_price,
                            size_usd=pos.size_usd,
                            reason=f"MR EXIT: z={z:.2f} returned to VWAP region",
                        ))
                    elif pos.side == Side.SELL and z <= self.EXIT_SIGMA:
                        signals.append(Signal(
                            strategy=self.name, symbol=symbol, side=Side.BUY,
                            confidence=0.9, entry_price=current_price,
                            stop_price=current_price, target_price=current_price,
                            size_usd=pos.size_usd,
                            reason=f"MR EXIT: z={z:.2f} returned to VWAP region",
                        ))
                    continue

                # Entry: price >1.5 sigma below VWAP -> BUY the reversion up
                if z < -self.ENTRY_SIGMA:
                    stop = vwap_val - self.HARD_STOP_SIGMA * sigma
                    target = vwap_val + self.EXIT_SIGMA * sigma
                    confidence = min(1.0, abs(z) / 3.0)
                    signals.append(Signal(
                        strategy=self.name, symbol=symbol, side=Side.BUY,
                        confidence=confidence,
                        entry_price=current_price,
                        stop_price=stop,
                        target_price=target,
                        size_usd=self.max_position_usd * confidence,
                        reason=f"MR BUY: z={z:.2f} < -{self.ENTRY_SIGMA} | "
                               f"VWAP=${vwap_val:.2f} sigma=${sigma:.2f}",
                    ))
                # Entry: price >1.5 sigma above VWAP -> SELL the reversion down
                elif z > self.ENTRY_SIGMA:
                    stop = vwap_val + self.HARD_STOP_SIGMA * sigma
                    target = vwap_val - self.EXIT_SIGMA * sigma
                    confidence = min(1.0, abs(z) / 3.0)
                    signals.append(Signal(
                        strategy=self.name, symbol=symbol, side=Side.SELL,
                        confidence=confidence,
                        entry_price=current_price,
                        stop_price=stop,
                        target_price=target,
                        size_usd=self.max_position_usd * confidence,
                        reason=f"MR SELL: z={z:.2f} > {self.ENTRY_SIGMA} | "
                               f"VWAP=${vwap_val:.2f} sigma=${sigma:.2f}",
                    ))

            except Exception as e:
                logger.warning("[mean_reversion] Error processing %s: %s", symbol, e)

        return signals


# ============================================================================
# STRATEGY 2: MOMENTUM (EMA Crossover)
# ============================================================================

class MomentumStrategy(BaseStrategy):
    """EMA crossover with volume confirmation.

    Fast EMA: 5-minute.  Slow EMA: 15-minute (using 5-min candles, periods 5 vs 15).
    Entry: fast EMA crosses slow EMA with volume > 1.2x average.
    Exit:  reverse cross or trailing stop (0.5%).
    """

    FAST_PERIOD = 5
    SLOW_PERIOD = 15
    VOLUME_THRESHOLD = 1.2
    TRAILING_STOP_PCT = 0.005
    CANDLE_GRANULARITY = 300  # 5-minute candles
    CANDLE_LIMIT = 60

    def __init__(self):
        crypto = ["BTC-USD", "ETH-USD", "SOL-USD"]
        equities = ["AAPL", "MSFT", "NVDA", "TSLA", "SPY"]
        super().__init__(
            name="momentum",
            symbols=crypto + equities,
            max_position_usd=400.0,
        )
        self._prev_fast_above: Dict[str, Optional[bool]] = {}

    def update(self, market_data: Dict[str, Any]) -> List[Signal]:
        signals = []

        for symbol in self.symbols:
            try:
                candles = self._get_candles(
                    market_data, symbol, self.CANDLE_GRANULARITY, self.CANDLE_LIMIT
                )
                if len(candles) < self.SLOW_PERIOD + 5:
                    continue

                closes = [c["close"] for c in candles]
                volumes = [c["volume"] for c in candles]

                fast_ema = ema(closes, self.FAST_PERIOD)
                slow_ema = ema(closes, self.SLOW_PERIOD)

                # Current crossover state
                fast_above = fast_ema[-1] > slow_ema[-1]
                prev_fast_above = self._prev_fast_above.get(symbol)
                self._prev_fast_above[symbol] = fast_above

                if prev_fast_above is None:
                    continue  # Need at least one prior state

                # Volume confirmation
                avg_volume = sum(volumes[-20:]) / max(1, len(volumes[-20:]))
                current_volume = volumes[-1] if volumes else 0
                volume_ok = avg_volume > 0 and current_volume > avg_volume * self.VOLUME_THRESHOLD

                current_price = closes[-1]
                is_crypto = MarketData._is_crypto(symbol)
                asset_class = AssetClass.CRYPTO if is_crypto else AssetClass.EQUITY

                # Check for exit on existing position
                if self.has_position(symbol):
                    pos = self._positions[symbol]
                    cross_reversed = (
                        (pos.side == Side.BUY and not fast_above and prev_fast_above) or
                        (pos.side == Side.SELL and fast_above and not prev_fast_above)
                    )
                    if cross_reversed:
                        exit_side = Side.SELL if pos.side == Side.BUY else Side.BUY
                        signals.append(Signal(
                            strategy=self.name, symbol=symbol, side=exit_side,
                            confidence=0.85,
                            entry_price=current_price,
                            stop_price=current_price,
                            target_price=current_price,
                            size_usd=pos.size_usd,
                            reason="MOM EXIT: reverse EMA cross",
                            asset_class=asset_class,
                        ))
                    continue

                # New entry: bullish crossover
                if fast_above and not prev_fast_above and volume_ok:
                    stop = current_price * (1 - self.TRAILING_STOP_PCT * 5)
                    target = current_price * (1 + self.TRAILING_STOP_PCT * 6)
                    vol_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0
                    confidence = min(1.0, 0.5 + (vol_ratio - 1.0) * 0.3)
                    signals.append(Signal(
                        strategy=self.name, symbol=symbol, side=Side.BUY,
                        confidence=confidence,
                        entry_price=current_price,
                        stop_price=stop,
                        target_price=target,
                        size_usd=self.max_position_usd * confidence,
                        reason=f"MOM BUY: fast EMA crossed above slow | "
                               f"vol={vol_ratio:.2f}x avg",
                        asset_class=asset_class,
                    ))
                # New entry: bearish crossover
                elif not fast_above and prev_fast_above and volume_ok:
                    stop = current_price * (1 + self.TRAILING_STOP_PCT * 5)
                    target = current_price * (1 - self.TRAILING_STOP_PCT * 6)
                    vol_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0
                    confidence = min(1.0, 0.5 + (vol_ratio - 1.0) * 0.3)
                    signals.append(Signal(
                        strategy=self.name, symbol=symbol, side=Side.SELL,
                        confidence=confidence,
                        entry_price=current_price,
                        stop_price=stop,
                        target_price=target,
                        size_usd=self.max_position_usd * confidence,
                        reason=f"MOM SELL: fast EMA crossed below slow | "
                               f"vol={vol_ratio:.2f}x avg",
                        asset_class=asset_class,
                    ))

            except Exception as e:
                logger.warning("[momentum] Error processing %s: %s", symbol, e)

        return signals


# ============================================================================
# STRATEGY 3: LATENCY ARBITRAGE (Our Edge)
# ============================================================================

class LatencyArbStrategy(BaseStrategy):
    """Exploit our traceroute scanner's RTT anomaly data.

    When we detect a latency spike to exchange X but not exchange Y,
    exchange X's prices may be stale. We trade the "true" price on Coinbase.

    Data source: traceroute.db — scans table (result_json has RTT data),
    and quant_signals table for pre-computed signals.
    """

    # Exchange endpoints we monitor
    EXCHANGE_HOSTS = {
        "api.coinbase.com": "coinbase",
        "api.binance.com": "binance",
        "api.kraken.com": "kraken",
        "api.bybit.com": "bybit",
        "api.okx.com": "okx",
    }

    RTT_SPIKE_THRESHOLD = 2.0   # z-score for RTT anomaly
    MIN_CONFIDENCE = 0.6
    SIGNAL_MAX_AGE = 300        # 5 minutes

    def __init__(self):
        super().__init__(
            name="latency_arb",
            symbols=["BTC-USD", "ETH-USD", "SOL-USD"],
            max_position_usd=600.0,  # Higher size — this is our edge
        )
        self._rtt_history: Dict[str, List[Tuple[float, float]]] = {}  # host -> [(ts, rtt)]
        self._last_scan_id = 0

    def _load_scan_data(self) -> List[dict]:
        """Load recent scan data from traceroute.db."""
        results = []
        try:
            db = sqlite3.connect(TRACEROUTE_DB)
            db.row_factory = sqlite3.Row

            # Load from quant_signals table (pre-computed by ml_signal_agent)
            rows = db.execute("""
                SELECT id, signal_type, target_host, direction, confidence,
                       details_json, created_at
                FROM quant_signals
                WHERE created_at >= datetime('now', '-10 minutes')
                  AND id > ?
                ORDER BY created_at DESC
                LIMIT 100
            """, (self._last_scan_id,)).fetchall()

            for row in rows:
                self._last_scan_id = max(self._last_scan_id, row["id"])
                results.append(dict(row))

            # Also load raw scans for RTT data
            scan_rows = db.execute("""
                SELECT id, target_host, result_json, created_at
                FROM scans
                WHERE result_json IS NOT NULL
                  AND created_at >= datetime('now', '-15 minutes')
                ORDER BY created_at DESC
                LIMIT 200
            """).fetchall()

            for row in scan_rows:
                host = row["target_host"]
                if host in self.EXCHANGE_HOSTS:
                    try:
                        result = json.loads(row["result_json"]) if row["result_json"] else {}
                        rtt = result.get("avg_rtt") or result.get("rtt") or result.get("latency")
                        if rtt and isinstance(rtt, (int, float)):
                            ts = time.time()
                            if host not in self._rtt_history:
                                self._rtt_history[host] = []
                            self._rtt_history[host].append((ts, float(rtt)))
                            # Keep last 200 measurements
                            self._rtt_history[host] = self._rtt_history[host][-200:]
                    except (json.JSONDecodeError, TypeError):
                        pass

            db.close()
        except Exception as e:
            logger.debug("[latency_arb] DB read error: %s", e)

        return results

    def _detect_rtt_anomalies(self) -> Dict[str, dict]:
        """Detect RTT anomalies across exchange endpoints.

        Returns dict of host -> {z_score, is_spike, mean_rtt, current_rtt}.
        """
        anomalies = {}
        for host, measurements in self._rtt_history.items():
            if len(measurements) < 10:
                continue

            rtts = [m[1] for m in measurements]
            current_rtt = rtts[-1]
            mean_rtt = sum(rtts) / len(rtts)
            sd = std_dev(rtts)

            z = z_score(current_rtt, mean_rtt, sd) if sd > 0 else 0.0

            anomalies[host] = {
                "z_score": z,
                "is_spike": z > self.RTT_SPIKE_THRESHOLD,
                "mean_rtt": mean_rtt,
                "current_rtt": current_rtt,
                "exchange": self.EXCHANGE_HOSTS.get(host, host),
            }
        return anomalies

    def update(self, market_data: Dict[str, Any]) -> List[Signal]:
        signals = []

        # Load fresh data from our scanner DB
        quant_signals = self._load_scan_data()

        # Path 1: Use pre-computed quant_signals
        for qs in quant_signals:
            try:
                sig_type = qs.get("signal_type", "")
                confidence = float(qs.get("confidence", 0))
                direction = qs.get("direction", "")
                host = qs.get("target_host", "")

                if confidence < self.MIN_CONFIDENCE:
                    continue

                if sig_type in ("rtt_anomaly", "cross_exchange_latency_diff",
                                "route_change_crypto_arb"):
                    for symbol in self.symbols:
                        if self.has_position(symbol):
                            continue

                        price = self._get_price(market_data, symbol)
                        if not price:
                            continue

                        if "up" in direction or "favor" in direction:
                            side = Side.BUY
                            stop = price * 0.985
                            target = price * 1.012
                        else:
                            side = Side.SELL
                            stop = price * 1.015
                            target = price * 0.988

                        signals.append(Signal(
                            strategy=self.name, symbol=symbol, side=side,
                            confidence=confidence,
                            entry_price=price, stop_price=stop,
                            target_price=target,
                            size_usd=self.max_position_usd * confidence,
                            reason=f"LATARB {sig_type}: {direction} on {host} | "
                                   f"conf={confidence:.2f}",
                        ))
                        break  # One signal per quant_signal row

            except Exception as e:
                logger.debug("[latency_arb] Signal processing error: %s", e)

        # Path 2: Direct RTT anomaly detection
        anomalies = self._detect_rtt_anomalies()
        spike_hosts = [h for h, a in anomalies.items() if a["is_spike"]]
        normal_hosts = [h for h, a in anomalies.items() if not a["is_spike"]]

        if spike_hosts and normal_hosts:
            # Exchange with spike has stale prices -> opportunity
            for symbol in self.symbols:
                if self.has_position(symbol):
                    continue

                price = self._get_price(market_data, symbol)
                if not price:
                    continue

                spike_z = max(anomalies[h]["z_score"] for h in spike_hosts)
                confidence = min(1.0, spike_z / 4.0)
                if confidence < self.MIN_CONFIDENCE:
                    continue

                # If Coinbase is slow, prices there are stale (could be too low/high)
                # If Coinbase is fast, we see the "true" price — trade based on
                # other exchange staleness
                coinbase_slow = "api.coinbase.com" in spike_hosts
                if coinbase_slow:
                    # Can't reliably trade on Coinbase if Coinbase itself is slow
                    continue

                side = Side.BUY  # Default: buy on fast exchange
                stop = price * 0.985
                target = price * 1.012

                spike_names = [anomalies[h]["exchange"] for h in spike_hosts]
                signals.append(Signal(
                    strategy=self.name, symbol=symbol, side=side,
                    confidence=confidence,
                    entry_price=price, stop_price=stop, target_price=target,
                    size_usd=self.max_position_usd * confidence,
                    reason=f"LATARB RTT spike on {','.join(spike_names)} | "
                           f"z={spike_z:.2f} | Coinbase fast -> stale prices elsewhere",
                ))
                break

        return signals


# ============================================================================
# STRATEGY 4: STATISTICAL ARBITRAGE (Pairs Trading)
# ============================================================================

class StatArbStrategy(BaseStrategy):
    """Pairs trading on correlated crypto price ratios.

    Tracks: BTC/ETH, BTC/SOL, ETH/SOL ratios.
    Entry: ratio deviates >2 sigma from 30-day mean.
    Exit:  ratio returns within 0.5 sigma.
    """

    ENTRY_SIGMA = 2.0
    EXIT_SIGMA = 0.5
    RATIO_WINDOW = 100  # candles for computing ratio statistics
    CANDLE_GRANULARITY = 3600  # 1-hour candles for longer-term stat arb
    CANDLE_LIMIT = 120

    PAIRS = [
        ("BTC-USD", "ETH-USD"),
        ("BTC-USD", "SOL-USD"),
        ("ETH-USD", "SOL-USD"),
    ]

    def __init__(self):
        all_symbols = list(set(s for pair in self.PAIRS for s in pair))
        super().__init__(
            name="stat_arb",
            symbols=all_symbols,
            max_position_usd=400.0,
        )
        self._ratio_history: Dict[str, List[float]] = {}

    def _pair_key(self, a: str, b: str) -> str:
        return f"{a}/{b}"

    def update(self, market_data: Dict[str, Any]) -> List[Signal]:
        signals = []

        # Fetch candles for all symbols
        candle_data: Dict[str, List[dict]] = {}
        for symbol in self.symbols:
            candles = self._get_candles(
                market_data, symbol, self.CANDLE_GRANULARITY, self.CANDLE_LIMIT
            )
            if candles:
                candle_data[symbol] = candles

        for sym_a, sym_b in self.PAIRS:
            try:
                if sym_a not in candle_data or sym_b not in candle_data:
                    continue

                candles_a = candle_data[sym_a]
                candles_b = candle_data[sym_b]

                # Align candles by timestamp
                ts_a = {c["start"]: c["close"] for c in candles_a}
                ts_b = {c["start"]: c["close"] for c in candles_b}
                common_ts = sorted(set(ts_a.keys()) & set(ts_b.keys()))

                if len(common_ts) < 30:
                    continue

                ratios = [ts_a[t] / ts_b[t] for t in common_ts if ts_b[t] > 0]
                if len(ratios) < 30:
                    continue

                pk = self._pair_key(sym_a, sym_b)
                self._ratio_history[pk] = ratios[-self.RATIO_WINDOW:]

                current_ratio = ratios[-1]
                mean_ratio = sum(ratios) / len(ratios)
                sigma = std_dev(ratios)
                if sigma <= 0:
                    continue

                z = z_score(current_ratio, mean_ratio, sigma)

                price_a = self._get_price(market_data, sym_a)
                price_b = self._get_price(market_data, sym_b)
                if not price_a or not price_b:
                    continue

                # Check for exit on existing pairs positions
                has_a = self.has_position(sym_a)
                has_b = self.has_position(sym_b)
                if has_a or has_b:
                    if abs(z) < self.EXIT_SIGMA:
                        # Convergence — exit both legs
                        if has_a:
                            pos_a = self._positions[sym_a]
                            exit_side_a = Side.SELL if pos_a.side == Side.BUY else Side.BUY
                            signals.append(Signal(
                                strategy=self.name, symbol=sym_a, side=exit_side_a,
                                confidence=0.9, entry_price=price_a,
                                stop_price=price_a, target_price=price_a,
                                size_usd=pos_a.size_usd,
                                reason=f"STATARB EXIT: {pk} z={z:.2f} converged",
                            ))
                        if has_b:
                            pos_b = self._positions[sym_b]
                            exit_side_b = Side.SELL if pos_b.side == Side.BUY else Side.BUY
                            signals.append(Signal(
                                strategy=self.name, symbol=sym_b, side=exit_side_b,
                                confidence=0.9, entry_price=price_b,
                                stop_price=price_b, target_price=price_b,
                                size_usd=pos_b.size_usd,
                                reason=f"STATARB EXIT: {pk} z={z:.2f} converged",
                            ))
                    continue

                # New entry: ratio too high -> A is expensive, B is cheap
                if z > self.ENTRY_SIGMA:
                    confidence = min(1.0, abs(z) / 4.0)
                    half_size = self.max_position_usd * confidence * 0.5

                    # Sell A (expensive), Buy B (cheap)
                    signals.append(Signal(
                        strategy=self.name, symbol=sym_a, side=Side.SELL,
                        confidence=confidence, entry_price=price_a,
                        stop_price=price_a * 1.03,
                        target_price=price_a * 0.985,
                        size_usd=half_size,
                        reason=f"STATARB: {pk} z={z:.2f} > {self.ENTRY_SIGMA} | "
                               f"SELL {sym_a} (expensive)",
                    ))
                    signals.append(Signal(
                        strategy=self.name, symbol=sym_b, side=Side.BUY,
                        confidence=confidence, entry_price=price_b,
                        stop_price=price_b * 0.97,
                        target_price=price_b * 1.015,
                        size_usd=half_size,
                        reason=f"STATARB: {pk} z={z:.2f} > {self.ENTRY_SIGMA} | "
                               f"BUY {sym_b} (cheap)",
                    ))
                # Ratio too low -> A is cheap, B is expensive
                elif z < -self.ENTRY_SIGMA:
                    confidence = min(1.0, abs(z) / 4.0)
                    half_size = self.max_position_usd * confidence * 0.5

                    signals.append(Signal(
                        strategy=self.name, symbol=sym_a, side=Side.BUY,
                        confidence=confidence, entry_price=price_a,
                        stop_price=price_a * 0.97,
                        target_price=price_a * 1.015,
                        size_usd=half_size,
                        reason=f"STATARB: {pk} z={z:.2f} < -{self.ENTRY_SIGMA} | "
                               f"BUY {sym_a} (cheap)",
                    ))
                    signals.append(Signal(
                        strategy=self.name, symbol=sym_b, side=Side.SELL,
                        confidence=confidence, entry_price=price_b,
                        stop_price=price_b * 1.03,
                        target_price=price_b * 0.985,
                        size_usd=half_size,
                        reason=f"STATARB: {pk} z={z:.2f} < -{self.ENTRY_SIGMA} | "
                               f"SELL {sym_b} (expensive)",
                    ))

            except Exception as e:
                logger.warning("[stat_arb] Error processing %s/%s: %s",
                               sym_a, sym_b, e)

        return signals


# ============================================================================
# STRATEGY 5: SENTIMENT / NEWS ALPHA
# ============================================================================

class SentimentAlphaStrategy(BaseStrategy):
    """Monitor HackerNews and Reddit for crypto/finance sentiment signals.

    When sentiment diverges from price (e.g., positive buzz + price down),
    trade the convergence.
    """

    # Keywords and their sentiment weights
    BULLISH_KEYWORDS = {
        "bitcoin": 1, "btc": 1, "ethereum": 1, "eth": 1, "solana": 1,
        "sol": 1, "crypto": 0.5, "blockchain": 0.3, "defi": 0.5,
        "bull": 1.5, "bullish": 2, "moon": 1.5, "buy": 0.8,
        "rally": 1.5, "surge": 1.5, "breakout": 1.5, "ath": 2,
        "adoption": 1, "etf": 1, "institutional": 1, "upgrade": 0.8,
    }
    BEARISH_KEYWORDS = {
        "crash": -2, "bearish": -2, "dump": -1.5, "sell": -0.8,
        "scam": -1.5, "fraud": -1.5, "hack": -2, "exploit": -1.5,
        "sec": -0.5, "regulation": -0.5, "ban": -2, "bubble": -1.5,
        "ponzi": -2, "collapse": -2, "bankrupt": -2, "fear": -1,
        "fud": -0.5, "plunge": -1.5, "tank": -1.5, "rug": -2,
    }

    SYMBOL_KEYWORDS = {
        "BTC-USD": ["bitcoin", "btc"],
        "ETH-USD": ["ethereum", "eth"],
        "SOL-USD": ["solana", "sol"],
    }

    HN_API = "https://hacker-news.firebaseio.com/v0"
    FETCH_LIMIT = 30  # top stories to check
    MIN_MENTIONS = 3
    SENTIMENT_DIVERGENCE_THRESHOLD = 0.4

    def __init__(self):
        super().__init__(
            name="sentiment_alpha",
            symbols=["BTC-USD", "ETH-USD", "SOL-USD"],
            max_position_usd=300.0,
        )
        self._last_fetch = 0.0
        self._fetch_interval = 300  # 5 minutes between fetches
        self._cached_sentiment: Dict[str, float] = {}
        self._cached_mention_count: Dict[str, int] = {}
        self._sentiment_history: Dict[str, List[Tuple[float, float]]] = {}

    def _fetch_hn_stories(self) -> List[dict]:
        """Fetch top HackerNews stories."""
        stories = []
        try:
            data = MarketData._http_get(f"{self.HN_API}/topstories.json", timeout=10)
            if not data or not isinstance(data, list):
                return []

            for story_id in data[:self.FETCH_LIMIT]:
                story = MarketData._http_get(
                    f"{self.HN_API}/item/{story_id}.json", timeout=5
                )
                if story and isinstance(story, dict):
                    stories.append(story)
                time.sleep(0.1)  # Polite rate limiting

        except Exception as e:
            logger.debug("[sentiment] HN fetch error: %s", e)
        return stories

    def _fetch_reddit_data(self) -> List[dict]:
        """Fetch Reddit posts from crypto/finance subreddits (JSON API)."""
        posts = []
        subreddits = ["cryptocurrency", "bitcoin", "ethtrader", "wallstreetbets"]
        for sub in subreddits:
            try:
                url = f"https://www.reddit.com/r/{sub}/hot.json?limit=10"
                data = MarketData._http_get(url, timeout=8)
                if data and "data" in data:
                    for child in data["data"].get("children", []):
                        post = child.get("data", {})
                        posts.append({
                            "title": post.get("title", ""),
                            "score": post.get("score", 0),
                            "num_comments": post.get("num_comments", 0),
                            "subreddit": sub,
                        })
            except Exception as e:
                logger.debug("[sentiment] Reddit fetch error for r/%s: %s", sub, e)
            time.sleep(0.2)
        return posts

    def _score_text(self, text: str) -> float:
        """Score a piece of text for crypto sentiment. Returns -1.0 to +1.0."""
        text_lower = text.lower()
        score = 0.0
        matches = 0
        for kw, weight in self.BULLISH_KEYWORDS.items():
            if kw in text_lower:
                score += weight
                matches += 1
        for kw, weight in self.BEARISH_KEYWORDS.items():
            if kw in text_lower:
                score += weight  # weight is already negative
                matches += 1
        if matches == 0:
            return 0.0
        return max(-1.0, min(1.0, score / max(1, matches)))

    def _symbol_relevance(self, text: str, symbol: str) -> bool:
        """Check if text is relevant to a specific symbol."""
        text_lower = text.lower()
        keywords = self.SYMBOL_KEYWORDS.get(symbol, [])
        return any(kw in text_lower for kw in keywords)

    def update(self, market_data: Dict[str, Any]) -> List[Signal]:
        signals = []
        now = time.time()

        # Rate-limit API fetches
        if now - self._last_fetch < self._fetch_interval:
            return signals
        self._last_fetch = now

        # Gather text data
        all_texts = []

        hn_stories = self._fetch_hn_stories()
        for story in hn_stories:
            title = story.get("title", "")
            if title:
                all_texts.append(title)

        reddit_posts = self._fetch_reddit_data()
        for post in reddit_posts:
            title = post.get("title", "")
            if title:
                # Weight by engagement
                engagement = 1.0 + math.log1p(post.get("score", 0)) * 0.1
                all_texts.append(title)

        if not all_texts:
            return signals

        # Score sentiment per symbol
        for symbol in self.symbols:
            try:
                relevant_texts = [t for t in all_texts if self._symbol_relevance(t, symbol)]
                mention_count = len(relevant_texts)
                self._cached_mention_count[symbol] = mention_count

                if mention_count < self.MIN_MENTIONS:
                    self._cached_sentiment[symbol] = 0.0
                    continue

                # Aggregate sentiment
                scores = [self._score_text(t) for t in relevant_texts]
                avg_sentiment = sum(scores) / len(scores) if scores else 0.0
                self._cached_sentiment[symbol] = avg_sentiment

                # Track sentiment history
                if symbol not in self._sentiment_history:
                    self._sentiment_history[symbol] = []
                self._sentiment_history[symbol].append((now, avg_sentiment))
                self._sentiment_history[symbol] = self._sentiment_history[symbol][-100:]

                # Get price action
                candles = self._get_candles(market_data, symbol, 3600, 24)
                if len(candles) < 6:
                    continue

                recent_closes = [c["close"] for c in candles[-6:]]
                price_change = (recent_closes[-1] - recent_closes[0]) / recent_closes[0]
                current_price = recent_closes[-1]

                # Detect divergence: sentiment vs price
                # Positive sentiment + negative price = BUY (sentiment leads)
                # Negative sentiment + positive price = SELL (sentiment warns)
                divergence = avg_sentiment - (price_change * 10)  # Scale price change

                if self.has_position(symbol):
                    continue

                if divergence > self.SENTIMENT_DIVERGENCE_THRESHOLD and avg_sentiment > 0.2:
                    confidence = min(1.0, abs(divergence) * 0.5 + mention_count * 0.02)
                    signals.append(Signal(
                        strategy=self.name, symbol=symbol, side=Side.BUY,
                        confidence=confidence,
                        entry_price=current_price,
                        stop_price=current_price * 0.975,
                        target_price=current_price * 1.02,
                        size_usd=self.max_position_usd * confidence,
                        reason=f"SENT BUY: sentiment={avg_sentiment:+.2f} but "
                               f"price={price_change:+.2%} | {mention_count} mentions | "
                               f"divergence={divergence:.2f}",
                    ))
                elif divergence < -self.SENTIMENT_DIVERGENCE_THRESHOLD and avg_sentiment < -0.2:
                    confidence = min(1.0, abs(divergence) * 0.5 + mention_count * 0.02)
                    signals.append(Signal(
                        strategy=self.name, symbol=symbol, side=Side.SELL,
                        confidence=confidence,
                        entry_price=current_price,
                        stop_price=current_price * 1.025,
                        target_price=current_price * 0.98,
                        size_usd=self.max_position_usd * confidence,
                        reason=f"SENT SELL: sentiment={avg_sentiment:+.2f} but "
                               f"price={price_change:+.2%} | {mention_count} mentions | "
                               f"divergence={divergence:.2f}",
                    ))

            except Exception as e:
                logger.warning("[sentiment] Error processing %s: %s", symbol, e)

        return signals


# ============================================================================
# RISK MANAGER
# ============================================================================

class RiskManager:
    """Centralized risk management. Rules are HARD CODED and NEVER overridden.

    Limits:
      - Max position per strategy: configurable (default $600)
      - Max total exposure: 80% of capital
      - Max daily loss: 2% of capital then FULL STOP
      - Max single trade: 5% of capital
      - Correlation check: no same-direction bets on correlated assets
      - NEVER let a winning trade become a losing trade (trailing stops)
    """

    # === HARD CODED RISK LIMITS — DO NOT MODIFY ===
    MAX_EXPOSURE_PCT = 0.80       # 80% of capital
    MAX_DAILY_LOSS_PCT = 0.02     # 2% of capital
    MAX_SINGLE_TRADE_PCT = 0.05   # 5% of capital
    MIN_RISK_REWARD = 1.2         # Minimum reward/risk ratio
    CORRELATION_THRESHOLD = 0.75  # Above this = correlated
    # ===============================================

    def __init__(self, initial_capital: float = 10000.0):
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.daily_pnl = 0.0
        self.daily_trades = 0
        self.daily_stopped = False
        self._last_reset_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Correlation matrix (hard-coded known correlations)
        self._correlations = {
            ("BTC-USD", "ETH-USD"): 0.85,
            ("BTC-USD", "SOL-USD"): 0.78,
            ("ETH-USD", "SOL-USD"): 0.82,
        }

    def _reset_daily_if_needed(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self._last_reset_date:
            logger.info("New trading day: resetting daily P&L from $%.2f",
                        self.daily_pnl)
            self.daily_pnl = 0.0
            self.daily_trades = 0
            self.daily_stopped = False
            self._last_reset_date = today

    def check_signal(self, signal: Signal,
                     all_positions: List[Position]) -> Tuple[bool, str]:
        """Validate a signal against ALL risk rules.

        Returns (approved, reason).
        """
        self._reset_daily_if_needed()

        # RULE: Daily loss limit — FULL STOP
        max_daily_loss = self.current_capital * self.MAX_DAILY_LOSS_PCT
        if self.daily_pnl <= -max_daily_loss:
            self.daily_stopped = True
            return False, (f"DAILY LOSS LIMIT: P&L=${self.daily_pnl:+.2f} "
                           f"exceeds -${max_daily_loss:.2f} (2% of capital). STOPPED.")

        if self.daily_stopped:
            return False, "DAILY STOP ACTIVE: no new trades until tomorrow"

        # RULE: Max single trade size
        max_trade = self.current_capital * self.MAX_SINGLE_TRADE_PCT
        if signal.size_usd > max_trade:
            signal.size_usd = max_trade  # Downsize, don't reject

        # RULE: Max total exposure
        total_exposure = sum(p.size_usd for p in all_positions)
        max_exposure = self.current_capital * self.MAX_EXPOSURE_PCT
        if total_exposure + signal.size_usd > max_exposure:
            remaining = max_exposure - total_exposure
            if remaining <= 0:
                return False, (f"MAX EXPOSURE: ${total_exposure:.2f} of "
                               f"${max_exposure:.2f} used. No room.")
            signal.size_usd = remaining

        # RULE: Every trade MUST have a stop loss (Rule #1)
        if signal.stop_price <= 0:
            return False, "NO STOP LOSS: every trade MUST have a stop. REJECTED."

        # RULE: Minimum risk/reward ratio
        rr = signal.risk_reward
        if rr < self.MIN_RISK_REWARD and "EXIT" not in signal.reason:
            return False, (f"RISK/REWARD: {rr:.2f} < {self.MIN_RISK_REWARD}. "
                           f"REJECTED.")

        # RULE: Confidence minimum
        if signal.confidence < 0.4 and "EXIT" not in signal.reason:
            return False, f"LOW CONFIDENCE: {signal.confidence:.2f} < 0.40. REJECTED."

        # RULE: Correlation check — no same-direction bets on correlated assets
        if "EXIT" not in signal.reason:
            for pos in all_positions:
                pair_key = tuple(sorted([signal.symbol, pos.symbol]))
                corr = self._correlations.get(pair_key, 0.0)
                if corr > self.CORRELATION_THRESHOLD:
                    same_direction = (
                        (signal.side == Side.BUY and pos.side == Side.BUY) or
                        (signal.side == Side.SELL and pos.side == Side.SELL)
                    )
                    if same_direction:
                        return False, (
                            f"CORRELATION: {signal.symbol} & {pos.symbol} "
                            f"corr={corr:.2f} — same direction {signal.side.value} blocked"
                        )

        return True, "APPROVED"

    def update_pnl(self, pnl: float):
        """Update daily P&L after a trade closes."""
        self.daily_pnl += pnl
        self.daily_trades += 1
        max_daily_loss = self.current_capital * self.MAX_DAILY_LOSS_PCT
        if self.daily_pnl <= -max_daily_loss:
            self.daily_stopped = True
            logger.critical(
                "DAILY LOSS LIMIT HIT: P&L=$%.2f exceeds -$%.2f. "
                "ALL TRADING STOPPED.",
                self.daily_pnl, max_daily_loss
            )

    def update_capital(self, new_capital: float):
        """Update capital for position sizing (Rule #3: scale with growth)."""
        self.current_capital = new_capital

    def status(self) -> dict:
        return {
            "capital": self.current_capital,
            "daily_pnl": self.daily_pnl,
            "daily_trades": self.daily_trades,
            "daily_stopped": self.daily_stopped,
            "max_daily_loss": self.current_capital * self.MAX_DAILY_LOSS_PCT,
            "max_exposure": self.current_capital * self.MAX_EXPOSURE_PCT,
            "max_single_trade": self.current_capital * self.MAX_SINGLE_TRADE_PCT,
        }


# ============================================================================
# SIGNAL AGGREGATOR
# ============================================================================

class SignalAggregator:
    """Combines signals from multiple strategies for execution.

    Requirements:
      - 2+ strategies must agree on direction for a symbol.
      - Weighted by strategy quality + regime alignment + data quality.
      - Exit signals from any strategy are passed through immediately.
    """

    MIN_AGREEING_STRATEGIES = 2
    MIN_AGGREGATE_CONFIDENCE = 0.5
    CORRELATION_CAP = 0.80
    MIN_POST_CORR_SIZE_USD = 20.0

    def aggregate(self, signals: List[Signal],
                  strategy_performance: Dict[str, dict],
                  market_snapshot: Optional[Dict[str, Any]] = None) -> List[Signal]:
        """Aggregate signals from all strategies.

        Exit signals pass through directly.
        Entry signals require consensus.

        Returns list of approved signals for execution.
        """
        approved = []

        # Exit signals always pass through
        exits = [s for s in signals if "EXIT" in s.reason]
        entries = [s for s in signals if "EXIT" not in s.reason]
        approved.extend(exits)

        # Group entry signals by symbol
        by_symbol: Dict[str, List[Signal]] = {}
        for sig in entries:
            by_symbol.setdefault(sig.symbol, []).append(sig)

        for symbol, sym_signals in by_symbol.items():
            # Count strategies agreeing on each direction
            buy_signals = [s for s in sym_signals if s.side == Side.BUY]
            sell_signals = [s for s in sym_signals if s.side == Side.SELL]

            # Check buy consensus
            if len(buy_signals) >= self.MIN_AGREEING_STRATEGIES:
                merged = self._merge_signals(
                    buy_signals, strategy_performance, market_snapshot
                )
                if merged and merged.confidence >= self.MIN_AGGREGATE_CONFIDENCE:
                    approved.append(merged)

            # Check sell consensus
            if len(sell_signals) >= self.MIN_AGREEING_STRATEGIES:
                merged = self._merge_signals(
                    sell_signals, strategy_performance, market_snapshot
                )
                if merged and merged.confidence >= self.MIN_AGGREGATE_CONFIDENCE:
                    approved.append(merged)

        return self._apply_cross_symbol_correlation_caps(approved, market_snapshot)

    def _merge_signals(self, signals: List[Signal],
                       perf: Dict[str, dict],
                       market_snapshot: Optional[Dict[str, Any]] = None) -> Optional[Signal]:
        """Merge agreeing signals into one, weighted by quality and regime."""
        if not signals:
            return None

        symbol = signals[0].symbol
        regime = self._infer_regime(symbol, market_snapshot)
        quality = self._symbol_quality(symbol, market_snapshot)

        # Compute weights from strategy quality + regime fit.
        weights = []
        for sig in signals:
            weights.append(self._signal_weight(sig, perf, regime, quality))

        total_weight = sum(weights)
        if total_weight <= 0:
            return None

        # Weighted averages
        w_confidence = sum(s.confidence * w for s, w in zip(signals, weights)) / total_weight
        w_entry = sum(s.entry_price * w for s, w in zip(signals, weights)) / total_weight
        w_stop = sum(s.stop_price * w for s, w in zip(signals, weights)) / total_weight
        w_target = sum(s.target_price * w for s, w in zip(signals, weights)) / total_weight
        w_size = sum(s.size_usd * w for s, w in zip(signals, weights)) / total_weight
        w_size = max(10.0, w_size)

        strategies = [s.strategy for s in signals]
        reasons = [s.reason for s in signals]
        regime_tag = (
            f"trend={regime['trend_score']:+.4f},vol={regime['volatility']:.4f},"
            f"ranging={1 if regime['is_ranging'] else 0},cov={quality['coverage_ratio']:.2f}"
        )

        return Signal(
            strategy=f"CONSENSUS({','.join(strategies)})",
            symbol=signals[0].symbol,
            side=signals[0].side,
            confidence=w_confidence,
            entry_price=w_entry,
            stop_price=w_stop,
            target_price=w_target,
            size_usd=w_size,
            reason=f"AGGREGATED from {len(signals)} strategies: " +
                   " | ".join(reasons) + f" | regime[{regime_tag}]",
            asset_class=signals[0].asset_class,
        )

    @staticmethod
    def _strategy_style(strategy_name: str) -> str:
        s = (strategy_name or "").lower()
        if "mean_reversion" in s or "stat_arb" in s or "vwap" in s or "rsi" in s:
            return "reversion"
        if "momentum" in s:
            return "momentum"
        if "latency_arb" in s:
            return "latency"
        if "sentiment" in s:
            return "sentiment"
        return "hybrid"

    def _signal_weight(self, signal: Signal, perf: Dict[str, dict],
                       regime: Dict[str, Any], quality: Dict[str, Any]) -> float:
        strat_perf = perf.get(signal.strategy, {})
        sharpe = float(strat_perf.get("sharpe", 0.0) or 0.0)
        win_rate = float(strat_perf.get("win_rate", 0.5) or 0.5)

        weight = 1.0
        weight *= max(0.35, min(1.8, 0.9 + sharpe * 0.2))
        weight *= max(0.60, min(1.35, 0.8 + (win_rate - 0.5) * 0.8))

        style = self._strategy_style(signal.strategy)
        trend = float(regime.get("trend_score", 0.0) or 0.0)
        is_ranging = bool(regime.get("is_ranging"))
        high_vol = bool(regime.get("high_volatility"))

        if style == "momentum":
            if abs(trend) > 0.0015:
                weight *= 1.20
            else:
                weight *= 0.85
            if (signal.side == Side.BUY and trend > 0) or (signal.side == Side.SELL and trend < 0):
                weight *= 1.10
            else:
                weight *= 0.85
        elif style == "reversion":
            weight *= 1.20 if is_ranging else 0.78
        elif style == "latency":
            weight *= 1.15 if high_vol else 0.95
        elif style == "sentiment":
            weight *= 1.08 if high_vol or abs(trend) > 0.001 else 0.95
        else:
            weight *= 1.0

        coverage = float(quality.get("coverage_ratio", 1.0) or 1.0)
        gap_count = int(quality.get("gap_count", 0) or 0)
        weight *= max(0.45, min(1.2, 0.45 + coverage * 0.75))
        if gap_count >= 5:
            weight *= 0.85

        return max(0.05, min(3.0, weight))

    @staticmethod
    def _candles_from_snapshot(symbol: str, market_snapshot: Optional[Dict[str, Any]],
                               granularity: int, min_count: int = 40) -> List[dict]:
        if not isinstance(market_snapshot, dict):
            return []
        candle_map = market_snapshot.get("candles", {})
        key = f"{symbol}:{granularity}:120"
        rows = candle_map.get(key, [])
        if len(rows) >= min_count:
            return rows
        for k, v in candle_map.items():
            if k.startswith(f"{symbol}:{granularity}:") and isinstance(v, list) and len(v) >= min_count:
                return v
        return []

    def _symbol_quality(self, symbol: str,
                        market_snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(market_snapshot, dict):
            return {"coverage_ratio": 1.0, "gap_count": 0}
        meta_map = market_snapshot.get("candle_meta", {})
        key = f"{symbol}:300:120"
        meta = meta_map.get(key) or {}
        return {
            "coverage_ratio": float(meta.get("coverage_ratio", 1.0) or 1.0),
            "gap_count": int(meta.get("gap_count", 0) or 0),
        }

    def _infer_regime(self, symbol: str,
                      market_snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        candles = self._candles_from_snapshot(symbol, market_snapshot, 300, min_count=40)
        if len(candles) < 40:
            return {
                "trend_score": 0.0,
                "volatility": 0.0,
                "is_ranging": True,
                "high_volatility": False,
            }

        closes = [float(c["close"]) for c in candles[-80:]]
        fast = ema(closes, 12)
        slow = ema(closes, 36)
        trend_score = ((fast[-1] - slow[-1]) / slow[-1]) if slow and slow[-1] else 0.0

        returns = []
        for i in range(1, len(closes)):
            prev = closes[i - 1]
            if prev > 0:
                returns.append((closes[i] - prev) / prev)
        vol = std_dev(returns) if returns else 0.0
        high_vol = vol > 0.0045
        is_ranging = abs(trend_score) < 0.0015 and not high_vol

        return {
            "trend_score": trend_score,
            "volatility": vol,
            "is_ranging": is_ranging,
            "high_volatility": high_vol,
        }

    @staticmethod
    def _compute_symbol_correlation(a: str, b: str,
                                    market_snapshot: Optional[Dict[str, Any]]) -> float:
        if not isinstance(market_snapshot, dict):
            return 0.0
        candle_map = market_snapshot.get("candles", {})
        a_rows = candle_map.get(f"{a}:3600:120", [])
        b_rows = candle_map.get(f"{b}:3600:120", [])
        if not a_rows or not b_rows:
            return 0.0

        a_by_ts = {int(c["start"]): float(c["close"]) for c in a_rows}
        b_by_ts = {int(c["start"]): float(c["close"]) for c in b_rows}
        ts = sorted(set(a_by_ts.keys()) & set(b_by_ts.keys()))
        if len(ts) < 20:
            return 0.0

        a_ret = []
        b_ret = []
        for i in range(1, len(ts)):
            pa0, pa1 = a_by_ts[ts[i - 1]], a_by_ts[ts[i]]
            pb0, pb1 = b_by_ts[ts[i - 1]], b_by_ts[ts[i]]
            if pa0 <= 0 or pb0 <= 0:
                continue
            a_ret.append((pa1 - pa0) / pa0)
            b_ret.append((pb1 - pb0) / pb0)
        if len(a_ret) < 15:
            return 0.0
        return pearson_correlation(a_ret, b_ret)

    def _apply_cross_symbol_correlation_caps(
        self, signals: List[Signal], market_snapshot: Optional[Dict[str, Any]]
    ) -> List[Signal]:
        entries = [s for s in signals if "EXIT" not in s.reason]
        exits = [s for s in signals if "EXIT" in s.reason]
        if len(entries) < 2:
            return signals

        for i in range(len(entries)):
            for j in range(i + 1, len(entries)):
                a = entries[i]
                b = entries[j]
                if a.side != b.side:
                    continue
                corr = self._compute_symbol_correlation(a.symbol, b.symbol, market_snapshot)
                if abs(corr) < self.CORRELATION_CAP:
                    continue

                loser = a if a.confidence <= b.confidence else b
                old_size = loser.size_usd
                loser.size_usd = max(self.MIN_POST_CORR_SIZE_USD, loser.size_usd * 0.65)
                loser.reason += (
                    f" | corr_cap({a.symbol},{b.symbol})={corr:.2f} "
                    f"size {old_size:.2f}->{loser.size_usd:.2f}"
                )

        filtered = [s for s in entries if s.size_usd >= self.MIN_POST_CORR_SIZE_USD]
        return exits + filtered


class PortfolioOptimizer:
    """Portfolio-level sizing optimizer for cross-strategy signal allocation."""

    MAX_NEW_EXPOSURE_PCT = 0.25
    MAX_SYMBOL_EXPOSURE_PCT = 0.18
    MAX_STRATEGY_EXPOSURE_PCT = 0.30
    MIN_TRADE_USD = 25.0
    TARGET_BAR_VOL = 0.004

    @staticmethod
    def _strategy_components(strategy_label: str) -> List[str]:
        s = str(strategy_label or "")
        if s.startswith("CONSENSUS(") and s.endswith(")"):
            inner = s[len("CONSENSUS("):-1]
            return [x.strip() for x in inner.split(",") if x.strip()]
        return [s]

    def _performance_scale(self, signal: Signal,
                           strategy_performance: Dict[str, dict]) -> float:
        comps = self._strategy_components(signal.strategy)
        if not comps:
            return 1.0
        sharpes = []
        win_rates = []
        for c in comps:
            p = strategy_performance.get(c, {})
            sharpes.append(float(p.get("sharpe", 0.0) or 0.0))
            win_rates.append(float(p.get("win_rate", 0.5) or 0.5))
        avg_sharpe = sum(sharpes) / len(sharpes)
        avg_win = sum(win_rates) / len(win_rates)
        scale = 0.9 + avg_sharpe * 0.15 + (avg_win - 0.5) * 0.6
        return max(0.60, min(1.50, scale))

    @staticmethod
    def _candles(snapshot: Optional[Dict[str, Any]], symbol: str,
                 granularity: int, min_count: int = 30) -> List[dict]:
        if not isinstance(snapshot, dict):
            return []
        candle_map = snapshot.get("candles", {})
        key = f"{symbol}:{granularity}:120"
        rows = candle_map.get(key, [])
        if len(rows) >= min_count:
            return rows
        for k, v in candle_map.items():
            if k.startswith(f"{symbol}:{granularity}:") and isinstance(v, list) and len(v) >= min_count:
                return v
        return []

    def _estimate_volatility(self, symbol: str,
                             snapshot: Optional[Dict[str, Any]]) -> float:
        candles = self._candles(snapshot, symbol, 300, min_count=30)
        if len(candles) < 30:
            return 0.0
        closes = [float(c["close"]) for c in candles[-60:]]
        rets = []
        for i in range(1, len(closes)):
            prev = closes[i - 1]
            if prev > 0:
                rets.append((closes[i] - prev) / prev)
        return std_dev(rets) if rets else 0.0

    def _pair_correlation(self, a: str, b: str,
                          snapshot: Optional[Dict[str, Any]]) -> float:
        a_rows = self._candles(snapshot, a, 3600, min_count=25)
        b_rows = self._candles(snapshot, b, 3600, min_count=25)
        if not a_rows or not b_rows:
            return 0.0
        a_by_ts = {int(c["start"]): float(c["close"]) for c in a_rows}
        b_by_ts = {int(c["start"]): float(c["close"]) for c in b_rows}
        ts = sorted(set(a_by_ts.keys()) & set(b_by_ts.keys()))
        if len(ts) < 20:
            return 0.0
        ar = []
        br = []
        for i in range(1, len(ts)):
            pa0, pa1 = a_by_ts[ts[i - 1]], a_by_ts[ts[i]]
            pb0, pb1 = b_by_ts[ts[i - 1]], b_by_ts[ts[i]]
            if pa0 <= 0 or pb0 <= 0:
                continue
            ar.append((pa1 - pa0) / pa0)
            br.append((pb1 - pb0) / pb0)
        if len(ar) < 15:
            return 0.0
        return pearson_correlation(ar, br)

    def _correlation_penalty(self, signal: Signal, positions: List[Position],
                             snapshot: Optional[Dict[str, Any]]) -> float:
        penalty = 1.0
        for pos in positions:
            corr = abs(self._pair_correlation(signal.symbol, pos.symbol, snapshot))
            if corr < 0.80:
                continue
            same_direction = (
                (signal.side == Side.BUY and pos.side == Side.BUY) or
                (signal.side == Side.SELL and pos.side == Side.SELL)
            )
            if same_direction:
                penalty *= 0.75
        return max(0.40, min(1.0, penalty))

    def _regime_scale(self, signal: Signal, snapshot: Optional[Dict[str, Any]]) -> float:
        candles = self._candles(snapshot, signal.symbol, 300, min_count=40)
        if len(candles) < 40:
            return 1.0
        closes = [float(c["close"]) for c in candles[-80:]]
        fast = ema(closes, 12)
        slow = ema(closes, 36)
        trend = ((fast[-1] - slow[-1]) / slow[-1]) if slow and slow[-1] else 0.0
        s = signal.strategy.lower()
        if "momentum" in s or "latency_arb" in s:
            aligned = (signal.side == Side.BUY and trend > 0) or (signal.side == Side.SELL and trend < 0)
            return 1.12 if aligned else 0.85
        if "mean_reversion" in s or "stat_arb" in s or "vwap" in s or "rsi" in s:
            return 1.15 if abs(trend) < 0.0015 else 0.82
        return 1.0

    def optimize(self, signals: List[Signal], all_positions: List[Position],
                 strategy_performance: Dict[str, dict], risk_manager: RiskManager,
                 market_snapshot: Optional[Dict[str, Any]] = None) -> List[Signal]:
        if not signals:
            return []

        exits = [s for s in signals if "EXIT" in s.reason]
        entries = [s for s in signals if "EXIT" not in s.reason]
        if not entries:
            return exits

        current_exposure = sum(float(p.size_usd) for p in all_positions)
        max_exposure = risk_manager.current_capital * risk_manager.MAX_EXPOSURE_PCT
        remaining_exposure = max(0.0, max_exposure - current_exposure)
        cycle_budget = min(
            remaining_exposure,
            risk_manager.current_capital * self.MAX_NEW_EXPOSURE_PCT,
        )
        if cycle_budget <= self.MIN_TRADE_USD:
            return exits

        symbol_exposure: Dict[str, float] = {}
        strategy_exposure: Dict[str, float] = {}
        for p in all_positions:
            symbol_exposure[p.symbol] = symbol_exposure.get(p.symbol, 0.0) + float(p.size_usd)
            strategy_exposure[p.strategy] = strategy_exposure.get(p.strategy, 0.0) + float(p.size_usd)

        ranked = sorted(
            entries,
            key=lambda s: s.confidence * max(1.0, s.risk_reward),
            reverse=True,
        )

        accepted = []
        for sig in ranked:
            if cycle_budget <= self.MIN_TRADE_USD:
                break

            base = max(0.0, float(sig.size_usd))
            if base <= 0:
                continue

            vol = self._estimate_volatility(sig.symbol, market_snapshot)
            vol_scale = 1.0
            if vol > 0:
                vol_scale = max(0.55, min(1.45, self.TARGET_BAR_VOL / vol))

            corr_scale = self._correlation_penalty(sig, all_positions, market_snapshot)
            perf_scale = self._performance_scale(sig, strategy_performance)
            regime_scale = self._regime_scale(sig, market_snapshot)
            proposed = base * vol_scale * corr_scale * perf_scale * regime_scale

            symbol_cap = max(
                0.0,
                risk_manager.current_capital * self.MAX_SYMBOL_EXPOSURE_PCT
                - symbol_exposure.get(sig.symbol, 0.0),
            )
            bucket = self._strategy_components(sig.strategy)[0]
            strategy_cap = max(
                0.0,
                risk_manager.current_capital * self.MAX_STRATEGY_EXPOSURE_PCT
                - strategy_exposure.get(bucket, 0.0),
            )

            final_size = min(proposed, cycle_budget, symbol_cap, strategy_cap)
            if final_size < self.MIN_TRADE_USD:
                continue

            old = sig.size_usd
            sig.size_usd = round(final_size, 2)
            sig.reason += (
                f" | portfolio_opt(size {old:.2f}->{sig.size_usd:.2f},"
                f" vol={vol_scale:.2f},corr={corr_scale:.2f},"
                f" perf={perf_scale:.2f},reg={regime_scale:.2f})"
            )
            accepted.append(sig)

            cycle_budget -= final_size
            symbol_exposure[sig.symbol] = symbol_exposure.get(sig.symbol, 0.0) + final_size
            strategy_exposure[bucket] = strategy_exposure.get(bucket, 0.0) + final_size

        return exits + accepted


# ============================================================================
# TRADE EXECUTOR
# ============================================================================

class TradeExecutor:
    """Routes orders to Coinbase (crypto) or E*Trade (equities).

    Tracks fills, calculates slippage, updates strategy positions.
    In paper mode, simulates fills at market price.
    """

    def __init__(self, mode: str = "paper"):
        self.mode = mode  # "paper" or "live"
        self.fills: List[Fill] = []
        self.execution_reports: List[ExecutionReport] = []
        self._last_execution_report: Optional[ExecutionReport] = None
        self._coinbase_trader = None
        self._etrade_trader = None

    def _record_report(self, report: ExecutionReport):
        self._last_execution_report = report
        self.execution_reports.append(report)
        if len(self.execution_reports) > 2000:
            self.execution_reports = self.execution_reports[-2000:]

    def get_last_execution_report(self) -> Optional[ExecutionReport]:
        return self._last_execution_report

    def _get_coinbase_trader(self):
        if self._coinbase_trader is None:
            try:
                sys.path.insert(0, str(AGENTS_DIR))
                from exchange_connector import CoinbaseTrader
                self._coinbase_trader = CoinbaseTrader()
            except Exception as e:
                logger.warning("Could not init CoinbaseTrader: %s", e)
        return self._coinbase_trader

    def _get_etrade_trader(self):
        if self._etrade_trader is None:
            try:
                sys.path.insert(0, str(AGENTS_DIR))
                from etrade_connector import ETradeTrader
                self._etrade_trader = ETradeTrader()
            except Exception as e:
                logger.warning("Could not init ETradeTrader: %s", e)
        return self._etrade_trader

    def execute(self, signal: Signal) -> Optional[Fill]:
        """Execute a signal. Returns Fill on success, None on failure."""
        self._last_execution_report = None
        if self.mode == "paper":
            return self._paper_fill(signal)
        elif self.mode == "live":
            if signal.asset_class == AssetClass.CRYPTO:
                return self._coinbase_fill(signal)
            else:
                return self._etrade_fill(signal)
        self._record_report(ExecutionReport(
            report_id=uuid.uuid4().hex[:12],
            signal_id=signal.signal_id,
            strategy=signal.strategy,
            symbol=signal.symbol,
            side=signal.side,
            mode=self.mode,
            venue="unknown",
            status="error",
            requested_price=signal.entry_price,
            executed_price=0.0,
            requested_qty=(signal.size_usd / signal.entry_price) if signal.entry_price else 0.0,
            filled_qty=0.0,
            requested_usd=signal.size_usd,
            filled_usd=0.0,
            slippage_bps=0.0,
            latency_ms=0.0,
            error=f"Unsupported execution mode: {self.mode}",
        ))
        return None

    def _paper_fill(self, signal: Signal) -> Fill:
        """Simulate a fill at market price with realistic slippage."""
        t0 = time.time()
        price = signal.entry_price
        # Simulate 1-5 bps slippage
        slippage_bps = 1.0 + (hash(signal.signal_id) % 5)
        if signal.side == Side.BUY:
            fill_price = price * (1 + slippage_bps / 10000)
        else:
            fill_price = price * (1 - slippage_bps / 10000)

        quantity = signal.size_usd / fill_price if fill_price > 0 else 0
        total = fill_price * quantity

        fill = Fill(
            fill_id=uuid.uuid4().hex[:12],
            signal_id=signal.signal_id,
            strategy=signal.strategy,
            symbol=signal.symbol,
            side=signal.side,
            price=fill_price,
            quantity=quantity,
            total_usd=total,
            slippage_bps=slippage_bps,
            timestamp=time.time(),
            asset_class=signal.asset_class,
        )
        self.fills.append(fill)
        self._record_report(ExecutionReport(
            report_id=uuid.uuid4().hex[:12],
            signal_id=signal.signal_id,
            strategy=signal.strategy,
            symbol=signal.symbol,
            side=signal.side,
            mode=self.mode,
            venue="paper_sim",
            status="filled",
            requested_price=signal.entry_price,
            executed_price=fill.price,
            requested_qty=(signal.size_usd / signal.entry_price) if signal.entry_price else 0.0,
            filled_qty=fill.quantity,
            requested_usd=signal.size_usd,
            filled_usd=fill.total_usd,
            slippage_bps=slippage_bps,
            latency_ms=(time.time() - t0) * 1000.0,
            meta={"simulated": True},
        ))
        logger.info("[PAPER] FILL %s %s %.6f %s @ $%.2f ($%.2f) slip=%.1fbps",
                    signal.side.value, signal.symbol, quantity,
                    signal.strategy, fill_price, total, slippage_bps)
        return fill

    def _coinbase_fill(self, signal: Signal) -> Optional[Fill]:
        """Execute on Coinbase via exchange_connector."""
        t0 = time.time()
        trader = self._get_coinbase_trader()
        if not trader:
            logger.error("Coinbase trader not available, falling back to paper")
            return self._paper_fill(signal)

        try:
            pair = signal.symbol if "-" in signal.symbol else f"{signal.symbol}-USD"
            result = trader.place_order(
                pair, signal.side.value, round(signal.size_usd, 2)
            )
            if "error" in result:
                logger.error("Coinbase order failed: %s", result.get("error"))
                self._record_report(ExecutionReport(
                    report_id=uuid.uuid4().hex[:12],
                    signal_id=signal.signal_id,
                    strategy=signal.strategy,
                    symbol=signal.symbol,
                    side=signal.side,
                    mode=self.mode,
                    venue="coinbase",
                    status="rejected",
                    requested_price=signal.entry_price,
                    executed_price=0.0,
                    requested_qty=(signal.size_usd / signal.entry_price) if signal.entry_price else 0.0,
                    filled_qty=0.0,
                    requested_usd=signal.size_usd,
                    filled_usd=0.0,
                    slippage_bps=0.0,
                    latency_ms=(time.time() - t0) * 1000.0,
                    error=str(result.get("error", "unknown")),
                    meta={"raw_result": result},
                ))
                return None

            order_id = result.get("success_response", result).get("order_id", "")
            fill = Fill(
                fill_id=order_id or uuid.uuid4().hex[:12],
                signal_id=signal.signal_id,
                strategy=signal.strategy,
                symbol=signal.symbol,
                side=signal.side,
                price=signal.entry_price,
                quantity=signal.size_usd / signal.entry_price,
                total_usd=signal.size_usd,
                slippage_bps=0.0,
                timestamp=time.time(),
                asset_class=AssetClass.CRYPTO,
            )
            self.fills.append(fill)
            self._record_report(ExecutionReport(
                report_id=uuid.uuid4().hex[:12],
                signal_id=signal.signal_id,
                strategy=signal.strategy,
                symbol=signal.symbol,
                side=signal.side,
                mode=self.mode,
                venue="coinbase",
                status="filled",
                requested_price=signal.entry_price,
                executed_price=fill.price,
                requested_qty=(signal.size_usd / signal.entry_price) if signal.entry_price else 0.0,
                filled_qty=fill.quantity,
                requested_usd=signal.size_usd,
                filled_usd=fill.total_usd,
                slippage_bps=fill.slippage_bps,
                latency_ms=(time.time() - t0) * 1000.0,
                meta={"order_id": order_id},
            ))
            logger.info("[LIVE] Coinbase FILL %s %s $%.2f order=%s",
                        signal.side.value, signal.symbol, signal.size_usd, order_id)
            return fill
        except Exception as e:
            logger.error("Coinbase execution error: %s", e)
            self._record_report(ExecutionReport(
                report_id=uuid.uuid4().hex[:12],
                signal_id=signal.signal_id,
                strategy=signal.strategy,
                symbol=signal.symbol,
                side=signal.side,
                mode=self.mode,
                venue="coinbase",
                status="error",
                requested_price=signal.entry_price,
                executed_price=0.0,
                requested_qty=(signal.size_usd / signal.entry_price) if signal.entry_price else 0.0,
                filled_qty=0.0,
                requested_usd=signal.size_usd,
                filled_usd=0.0,
                slippage_bps=0.0,
                latency_ms=(time.time() - t0) * 1000.0,
                error=str(e),
            ))
            return None

    def _etrade_fill(self, signal: Signal) -> Optional[Fill]:
        """Execute equity order on E*Trade."""
        t0 = time.time()
        trader = self._get_etrade_trader()
        if not trader:
            logger.error("E*Trade trader not available, falling back to paper")
            return self._paper_fill(signal)

        try:
            # Get account
            accounts = trader.get_accounts()
            if not accounts:
                logger.error("No E*Trade accounts available")
                self._record_report(ExecutionReport(
                    report_id=uuid.uuid4().hex[:12],
                    signal_id=signal.signal_id,
                    strategy=signal.strategy,
                    symbol=signal.symbol,
                    side=signal.side,
                    mode=self.mode,
                    venue="etrade",
                    status="rejected",
                    requested_price=signal.entry_price,
                    executed_price=0.0,
                    requested_qty=(signal.size_usd / signal.entry_price) if signal.entry_price else 0.0,
                    filled_qty=0.0,
                    requested_usd=signal.size_usd,
                    filled_usd=0.0,
                    slippage_bps=0.0,
                    latency_ms=(time.time() - t0) * 1000.0,
                    error="No E*Trade accounts available",
                ))
                return self._paper_fill(signal)

            account_id = accounts[0].get("accountIdKey", "")
            quantity = max(1, int(signal.size_usd / signal.entry_price))

            result = trader.place_order(
                account_id, signal.symbol, signal.side.value,
                quantity, order_type="MARKET",
                signal_confidence=signal.confidence,
            )

            if result.get("success"):
                fill = Fill(
                    fill_id=str(result.get("order_id", uuid.uuid4().hex[:12])),
                    signal_id=signal.signal_id,
                    strategy=signal.strategy,
                    symbol=signal.symbol,
                    side=signal.side,
                    price=result.get("price", signal.entry_price),
                    quantity=quantity,
                    total_usd=result.get("estimated_total", signal.size_usd),
                    slippage_bps=0.0,
                    timestamp=time.time(),
                    asset_class=AssetClass.EQUITY,
                )
                self.fills.append(fill)
                self._record_report(ExecutionReport(
                    report_id=uuid.uuid4().hex[:12],
                    signal_id=signal.signal_id,
                    strategy=signal.strategy,
                    symbol=signal.symbol,
                    side=signal.side,
                    mode=self.mode,
                    venue="etrade",
                    status="filled",
                    requested_price=signal.entry_price,
                    executed_price=fill.price,
                    requested_qty=(signal.size_usd / signal.entry_price) if signal.entry_price else 0.0,
                    filled_qty=fill.quantity,
                    requested_usd=signal.size_usd,
                    filled_usd=fill.total_usd,
                    slippage_bps=fill.slippage_bps,
                    latency_ms=(time.time() - t0) * 1000.0,
                    meta={"order_id": result.get("order_id")},
                ))
                logger.info("[LIVE] E*Trade FILL %s %s x%d order=%s",
                            signal.side.value, signal.symbol, quantity,
                            result.get("order_id"))
                return fill
            else:
                logger.error("E*Trade order failed: %s", result)
                self._record_report(ExecutionReport(
                    report_id=uuid.uuid4().hex[:12],
                    signal_id=signal.signal_id,
                    strategy=signal.strategy,
                    symbol=signal.symbol,
                    side=signal.side,
                    mode=self.mode,
                    venue="etrade",
                    status="rejected",
                    requested_price=signal.entry_price,
                    executed_price=0.0,
                    requested_qty=(signal.size_usd / signal.entry_price) if signal.entry_price else 0.0,
                    filled_qty=0.0,
                    requested_usd=signal.size_usd,
                    filled_usd=0.0,
                    slippage_bps=0.0,
                    latency_ms=(time.time() - t0) * 1000.0,
                    error=str(result),
                    meta={"raw_result": result},
                ))
                return None
        except Exception as e:
            logger.error("E*Trade execution error: %s", e)
            self._record_report(ExecutionReport(
                report_id=uuid.uuid4().hex[:12],
                signal_id=signal.signal_id,
                strategy=signal.strategy,
                symbol=signal.symbol,
                side=signal.side,
                mode=self.mode,
                venue="etrade",
                status="error",
                requested_price=signal.entry_price,
                executed_price=0.0,
                requested_qty=(signal.size_usd / signal.entry_price) if signal.entry_price else 0.0,
                filled_qty=0.0,
                requested_usd=signal.size_usd,
                filled_usd=0.0,
                slippage_bps=0.0,
                latency_ms=(time.time() - t0) * 1000.0,
                error=str(e),
            ))
            return None


# ============================================================================
# STATE PERSISTENCE (SQLite)
# ============================================================================

class StateDB:
    """Persists engine state to SQLite."""

    def __init__(self, db_path: str = ENGINE_DB):
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS engine_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS signals_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT UNIQUE,
                strategy TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                confidence REAL,
                entry_price REAL,
                stop_price REAL,
                target_price REAL,
                size_usd REAL,
                reason TEXT,
                approved INTEGER DEFAULT 0,
                rejection_reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS fills_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fill_id TEXT UNIQUE,
                signal_id TEXT,
                strategy TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL,
                quantity REAL,
                total_usd REAL,
                slippage_bps REAL,
                asset_class TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS execution_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id TEXT UNIQUE,
                signal_id TEXT,
                strategy TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                mode TEXT,
                venue TEXT,
                status TEXT,
                requested_price REAL,
                executed_price REAL,
                requested_qty REAL,
                filled_qty REAL,
                requested_usd REAL,
                filled_usd REAL,
                slippage_bps REAL,
                latency_ms REAL,
                error TEXT,
                meta_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS positions_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id TEXT,
                strategy TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price REAL,
                exit_price REAL,
                quantity REAL,
                size_usd REAL,
                pnl REAL,
                pnl_pct REAL,
                duration_sec REAL,
                opened_at TIMESTAMP,
                closed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS performance_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                capital REAL,
                daily_pnl REAL,
                total_pnl REAL,
                open_positions INTEGER,
                strategies_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_signals_created
                ON signals_log(created_at);
            CREATE INDEX IF NOT EXISTS idx_signals_strategy
                ON signals_log(strategy, created_at);
            CREATE INDEX IF NOT EXISTS idx_fills_created
                ON fills_log(created_at);
            CREATE INDEX IF NOT EXISTS idx_exec_reports_created
                ON execution_reports(created_at);
            CREATE INDEX IF NOT EXISTS idx_positions_strategy
                ON positions_log(strategy, closed_at);
        """)
        self.db.commit()

    def log_signal(self, signal: Signal, approved: bool, reason: str):
        try:
            self.db.execute("""
                INSERT OR IGNORE INTO signals_log
                (signal_id, strategy, symbol, side, confidence,
                 entry_price, stop_price, target_price, size_usd,
                 reason, approved, rejection_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (signal.signal_id, signal.strategy, signal.symbol,
                  signal.side.value, signal.confidence,
                  signal.entry_price, signal.stop_price, signal.target_price,
                  signal.size_usd, signal.reason, 1 if approved else 0,
                  reason if not approved else None))
            self.db.commit()
        except Exception as e:
            logger.debug("Signal log error: %s", e)

    def log_fill(self, fill: Fill):
        try:
            self.db.execute("""
                INSERT OR IGNORE INTO fills_log
                (fill_id, signal_id, strategy, symbol, side,
                 price, quantity, total_usd, slippage_bps, asset_class)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (fill.fill_id, fill.signal_id, fill.strategy, fill.symbol,
                  fill.side.value, fill.price, fill.quantity, fill.total_usd,
                  fill.slippage_bps, fill.asset_class.value))
            self.db.commit()
        except Exception as e:
            logger.debug("Fill log error: %s", e)

    def log_execution_report(self, report: ExecutionReport):
        try:
            self.db.execute("""
                INSERT OR IGNORE INTO execution_reports
                (report_id, signal_id, strategy, symbol, side, mode, venue, status,
                 requested_price, executed_price, requested_qty, filled_qty,
                 requested_usd, filled_usd, slippage_bps, latency_ms, error, meta_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                report.report_id,
                report.signal_id,
                report.strategy,
                report.symbol,
                report.side.value,
                report.mode,
                report.venue,
                report.status,
                report.requested_price,
                report.executed_price,
                report.requested_qty,
                report.filled_qty,
                report.requested_usd,
                report.filled_usd,
                report.slippage_bps,
                report.latency_ms,
                report.error,
                json.dumps(report.meta),
            ))
            self.db.commit()
        except Exception as e:
            logger.debug("Execution report log error: %s", e)

    def log_position_close(self, pos: Position, exit_price: float, pnl: float):
        try:
            duration = time.time() - pos.opened_at
            pnl_pct = pnl / pos.size_usd if pos.size_usd else 0.0
            self.db.execute("""
                INSERT INTO positions_log
                (position_id, strategy, symbol, side, entry_price,
                 exit_price, quantity, size_usd, pnl, pnl_pct,
                 duration_sec, opened_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (pos.position_id, pos.strategy, pos.symbol, pos.side.value,
                  pos.entry_price, exit_price, pos.quantity, pos.size_usd,
                  pnl, pnl_pct, duration,
                  datetime.fromtimestamp(pos.opened_at, tz=timezone.utc).isoformat()))
            self.db.commit()
        except Exception as e:
            logger.debug("Position log error: %s", e)

    def save_snapshot(self, capital: float, daily_pnl: float, total_pnl: float,
                      open_positions: int, strategies: dict):
        try:
            self.db.execute("""
                INSERT INTO performance_snapshots
                (capital, daily_pnl, total_pnl, open_positions, strategies_json)
                VALUES (?, ?, ?, ?, ?)
            """, (capital, daily_pnl, total_pnl, open_positions,
                  json.dumps(strategies)))
            self.db.commit()
        except Exception as e:
            logger.debug("Snapshot error: %s", e)

    def get_state(self, key: str, default: str = "") -> str:
        row = self.db.execute(
            "SELECT value FROM engine_state WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else default

    def set_state(self, key: str, value: str):
        self.db.execute("""
            INSERT INTO engine_state (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value=?, updated_at=CURRENT_TIMESTAMP
        """, (key, value, value))
        self.db.commit()

    def get_recent_signals(self, limit: int = 50) -> List[dict]:
        rows = self.db.execute("""
            SELECT * FROM signals_log
            ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def get_recent_fills(self, limit: int = 50) -> List[dict]:
        rows = self.db.execute("""
            SELECT * FROM fills_log
            ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def get_recent_execution_reports(self, limit: int = 50) -> List[dict]:
        rows = self.db.execute("""
            SELECT * FROM execution_reports
            ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def get_closed_positions(self, limit: int = 100) -> List[dict]:
        rows = self.db.execute("""
            SELECT * FROM positions_log
            ORDER BY closed_at DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def get_performance_history(self, limit: int = 200) -> List[dict]:
        rows = self.db.execute("""
            SELECT * FROM performance_snapshots
            ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self.db.close()


# ============================================================================
# STRATEGY ENGINE (orchestrator)
# ============================================================================

class StrategyEngine:
    """Runs all strategies, aggregates signals, manages risk, routes execution.

    This is the top-level orchestrator that ties everything together.
    """

    CYCLE_INTERVAL = 30  # seconds between strategy updates
    SNAPSHOT_INTERVAL = 300  # 5 minutes between performance snapshots

    def __init__(self, mode: str = "paper", capital: float = 10000.0):
        self.mode = mode
        self.initial_capital = capital

        # Core components
        self.strategies: List[BaseStrategy] = [
            MeanReversionStrategy(),
            MomentumStrategy(),
            LatencyArbStrategy(),
            StatArbStrategy(),
            SentimentAlphaStrategy(),
        ]
        self.risk_manager = RiskManager(initial_capital=capital)
        self.signal_aggregator = SignalAggregator()
        self.portfolio_optimizer = PortfolioOptimizer()
        self.executor = TradeExecutor(mode=mode)
        self.state_db = StateDB()

        # Runtime state
        self._running = False
        self._cycle = 0
        self._last_snapshot = 0.0
        self._total_pnl = 0.0

        # Load persisted capital
        saved_capital = self.state_db.get_state("capital")
        if saved_capital:
            try:
                self.risk_manager.current_capital = float(saved_capital)
                logger.info("Restored capital: $%.2f", self.risk_manager.current_capital)
            except ValueError:
                pass

        logger.info(
            "StrategyEngine initialized | mode=%s | capital=$%.2f | strategies=%d",
            mode, self.risk_manager.current_capital, len(self.strategies)
        )

    def _get_all_positions(self) -> List[Position]:
        """Collect positions from all strategies."""
        positions = []
        for strat in self.strategies:
            positions.extend(strat.get_positions())
        return positions

    def _get_strategy_performance(self) -> Dict[str, dict]:
        """Collect performance metrics from all strategies."""
        return {s.name: s.performance() for s in self.strategies}

    def _update_prices(self):
        """Fetch current prices and update all strategy positions."""
        all_symbols = set()
        for strat in self.strategies:
            for pos in strat.get_positions():
                all_symbols.add(pos.symbol)

        if not all_symbols:
            return

        prices = MarketData.get_prices(list(all_symbols))
        for strat in self.strategies:
            strat.update_position_prices(prices)

    def _build_market_snapshot(self) -> Dict[str, Any]:
        """Pre-fetch shared market data once per cycle for all strategies."""
        symbols = set()
        for strat in self.strategies:
            symbols.update(strat.symbols)
            for pos in strat.get_positions():
                symbols.add(pos.symbol)

        prices = MarketData.get_prices(sorted(symbols))
        candles: Dict[str, List[dict]] = {}
        candle_meta: Dict[str, Dict[str, Any]] = {}
        for symbol in symbols:
            for granularity, limit in ((300, 120), (3600, 120)):
                key = f"{symbol}:{granularity}:{limit}"
                rows = MarketData.get_candles(symbol, granularity, limit)
                if rows:
                    candles[key] = rows
                candle_meta[key] = MarketData.get_candle_meta(symbol, granularity, limit)

        return {
            "built_at": time.time(),
            "prices": prices,
            "candles": candles,
            "candle_meta": candle_meta,
        }

    def run_cycle(self) -> List[Signal]:
        """Run one complete engine cycle. Returns executed signals."""
        self._cycle += 1
        executed = []

        try:
            # 1. Update prices on existing positions
            self._update_prices()

            # 2. Check stops on all strategies
            all_exit_signals = []
            for strat in self.strategies:
                if strat._enabled:
                    exits = strat.check_stops()
                    all_exit_signals.extend(exits)

            # 3. Execute exit signals immediately (no aggregation needed)
            for signal in all_exit_signals:
                approved, reason = self.risk_manager.check_signal(
                    signal, self._get_all_positions()
                )
                self.state_db.log_signal(signal, approved, reason)
                if approved:
                    fill = self.executor.execute(signal)
                    exec_report = self.executor.get_last_execution_report()
                    if exec_report:
                        self.state_db.log_execution_report(exec_report)
                    if fill:
                        self.state_db.log_fill(fill)
                        # Find the strategy that owns this position
                        for strat in self.strategies:
                            if strat.has_position(signal.symbol):
                                pos = strat._positions.get(signal.symbol)
                                if pos:
                                    self.state_db.log_position_close(
                                        pos, fill.price,
                                        pos.unrealized_pnl
                                    )
                                    self.risk_manager.update_pnl(pos.unrealized_pnl)
                                    self._total_pnl += pos.unrealized_pnl
                                strat.record_fill(fill, signal)
                                break
                        executed.append(signal)

            # 4. Build shared snapshot and run all strategies for entry signals
            market_data = self._build_market_snapshot()
            all_entry_signals = []
            for strat in self.strategies:
                if not strat._enabled:
                    continue
                try:
                    signals = strat.update(market_data)
                    all_entry_signals.extend(signals)
                except Exception as e:
                    logger.error("[%s] Strategy update error: %s", strat.name, e)
                    logger.debug(traceback.format_exc())

            # 5. Aggregate entry signals (require consensus)
            perf = self._get_strategy_performance()
            aggregated = self.signal_aggregator.aggregate(
                all_entry_signals, perf, market_snapshot=market_data
            )
            optimized = self.portfolio_optimizer.optimize(
                aggregated,
                self._get_all_positions(),
                perf,
                self.risk_manager,
                market_snapshot=market_data,
            )

            # 6. Risk check and execute optimized signals
            for signal in optimized:
                if "EXIT" in signal.reason:
                    continue  # Already handled above

                approved, reason = self.risk_manager.check_signal(
                    signal, self._get_all_positions()
                )
                self.state_db.log_signal(signal, approved, reason)

                if not approved:
                    logger.info("REJECTED: %s %s %s — %s",
                                signal.side.value, signal.symbol,
                                signal.strategy, reason)
                    continue

                fill = self.executor.execute(signal)
                exec_report = self.executor.get_last_execution_report()
                if exec_report:
                    self.state_db.log_execution_report(exec_report)
                if fill:
                    self.state_db.log_fill(fill)
                    # Record fill on the originating strategy (or first one)
                    for strat in self.strategies:
                        if strat.name in signal.strategy:
                            strat.record_fill(fill, signal)
                            break
                    executed.append(signal)

            # 7. Update capital (Rule #3: reinvest profits)
            total_unrealized = sum(
                p.unrealized_pnl for p in self._get_all_positions()
            )
            effective_capital = self.initial_capital + self._total_pnl + total_unrealized
            self.risk_manager.update_capital(max(effective_capital, self.initial_capital * 0.5))

            # 8. Periodic snapshot
            now = time.time()
            if now - self._last_snapshot > self.SNAPSHOT_INTERVAL:
                self._save_snapshot()
                self._last_snapshot = now

        except Exception as e:
            logger.error("Engine cycle error: %s", e)
            logger.debug(traceback.format_exc())

        return executed

    def _save_snapshot(self):
        """Save performance snapshot to DB."""
        perf = self._get_strategy_performance()
        self.state_db.save_snapshot(
            capital=self.risk_manager.current_capital,
            daily_pnl=self.risk_manager.daily_pnl,
            total_pnl=self._total_pnl,
            open_positions=len(self._get_all_positions()),
            strategies=perf,
        )
        self.state_db.set_state("capital", str(self.risk_manager.current_capital))
        self.state_db.set_state("total_pnl", str(self._total_pnl))
        self.state_db.set_state("last_cycle", str(self._cycle))

    def run(self):
        """Main engine loop. Runs until interrupted."""
        self._running = True
        logger.info("=" * 70)
        logger.info("  QUANT ENGINE V2 STARTING")
        logger.info("  Mode: %s | Capital: $%.2f | Strategies: %d",
                     self.mode, self.risk_manager.current_capital,
                     len(self.strategies))
        logger.info("  Strategies: %s",
                     ", ".join(s.name for s in self.strategies))
        logger.info("=" * 70)

        while self._running:
            try:
                t0 = time.time()
                executed = self.run_cycle()

                if executed:
                    logger.info("Cycle %d: %d signals executed", self._cycle,
                                len(executed))

                # Status log every 10 cycles
                if self._cycle % 10 == 0:
                    pos_count = len(self._get_all_positions())
                    logger.info(
                        "Cycle %d | Capital: $%.2f | Daily P&L: $%+.2f | "
                        "Total P&L: $%+.2f | Positions: %d",
                        self._cycle, self.risk_manager.current_capital,
                        self.risk_manager.daily_pnl, self._total_pnl, pos_count
                    )

                elapsed = time.time() - t0
                sleep_time = max(1, self.CYCLE_INTERVAL - elapsed)
                time.sleep(sleep_time)

            except KeyboardInterrupt:
                logger.info("Shutdown requested...")
                self._running = False
            except Exception as e:
                logger.error("Main loop error: %s", e)
                logger.debug(traceback.format_exc())
                time.sleep(10)

        self._save_snapshot()
        self.state_db.close()
        logger.info("Engine stopped. Final P&L: $%+.2f", self._total_pnl)

    def stop(self):
        self._running = False

    # ----- status / reporting -----

    def print_status(self):
        """Print comprehensive engine status."""
        risk = self.risk_manager.status()
        positions = self._get_all_positions()
        perf = self._get_strategy_performance()

        print()
        print("=" * 78)
        print(f"  QUANT ENGINE V2 — {self.mode.upper()} MODE")
        print("=" * 78)
        print()
        print(f"  Capital:      ${risk['capital']:>12,.2f}")
        print(f"  Daily P&L:    ${risk['daily_pnl']:>+12,.2f}  "
              f"(limit: -${risk['max_daily_loss']:,.2f})")
        print(f"  Total P&L:    ${self._total_pnl:>+12,.2f}")
        print(f"  Daily Trades: {risk['daily_trades']:>12d}")
        print(f"  Stopped:      {'YES — NO NEW TRADES' if risk['daily_stopped'] else 'No'}")
        print()

        # Strategy performance table
        print("  STRATEGY PERFORMANCE")
        print("  " + "-" * 74)
        print(f"  {'Strategy':<18} {'Trades':>6} {'Wins':>5} {'Win%':>6} "
              f"{'P&L':>10} {'Sharpe':>7} {'Pos':>4}")
        print("  " + "-" * 74)
        for name, p in perf.items():
            wr = f"{p['win_rate']*100:.0f}%" if p['total_trades'] > 0 else "  - "
            print(f"  {name:<18} {p['total_trades']:>6d} {p['wins']:>5d} "
                  f"{wr:>6} ${p['total_pnl']:>+9,.2f} {p['sharpe']:>7.2f} "
                  f"{p['open_positions']:>4d}")
        print("  " + "-" * 74)

        # Open positions
        if positions:
            print()
            print("  OPEN POSITIONS")
            print("  " + "-" * 74)
            print(f"  {'Strategy':<16} {'Symbol':<10} {'Side':<5} "
                  f"{'Entry':>10} {'Current':>10} {'P&L':>10} {'Stop':>10}")
            print("  " + "-" * 74)
            for pos in positions:
                print(f"  {pos.strategy:<16} {pos.symbol:<10} "
                      f"{pos.side.value:<5} "
                      f"${pos.entry_price:>9,.2f} ${pos.current_price:>9,.2f} "
                      f"${pos.unrealized_pnl:>+9,.2f} ${pos.stop_price:>9,.2f}")
            print("  " + "-" * 74)

        print()
        print("=" * 78)
        print()

    def print_signals(self):
        """Print recent signals from DB."""
        signals = self.state_db.get_recent_signals(30)

        print()
        print("=" * 78)
        print("  RECENT SIGNALS (last 30)")
        print("=" * 78)
        print()
        if not signals:
            print("  No signals recorded yet. Run the engine first.")
        else:
            print(f"  {'Time':<20} {'Strategy':<16} {'Symbol':<10} "
                  f"{'Side':<5} {'Conf':>5} {'OK':>3} {'Reason'}")
            print("  " + "-" * 74)
            for s in signals:
                ts = s.get("created_at", "")[:19]
                ok = "Y" if s.get("approved") else "N"
                reason = s.get("reason", "")[:40]
                if not s.get("approved") and s.get("rejection_reason"):
                    reason = s["rejection_reason"][:40]
                print(f"  {ts:<20} {s.get('strategy',''):<16} "
                      f"{s.get('symbol',''):<10} {s.get('side',''):<5} "
                      f"{s.get('confidence',0):>5.2f} {ok:>3} {reason}")
        print()
        print("=" * 78)
        print()


# ============================================================================
# BACKTESTER
# ============================================================================

class Backtester:
    """Runs strategies on historical data to validate performance."""

    def __init__(self, days: int = 7, capital: float = 10000.0):
        self.days = days
        self.capital = capital

    def run(self):
        """Run backtest on all strategies using historical candle data."""
        print()
        print("=" * 78)
        print(f"  BACKTEST — {self.days} DAYS | Capital: ${self.capital:,.2f}")
        print("=" * 78)
        print()

        engine = StrategyEngine(mode="paper", capital=self.capital)
        symbols = ["BTC-USD", "ETH-USD", "SOL-USD"]

        # Fetch historical data
        print("  Fetching historical data...")
        historical: Dict[str, List[dict]] = {}
        for symbol in symbols:
            # Get daily candles for the backtest period
            candles = MarketData.get_candles(symbol, 3600, self.days * 24)
            if candles:
                historical[symbol] = candles
                print(f"    {symbol}: {len(candles)} hourly candles")
            else:
                print(f"    {symbol}: NO DATA")
            time.sleep(0.5)

        if not historical:
            print("\n  ERROR: No historical data available for backtesting.")
            return

        # Simulate cycle-by-cycle
        total_candles = max(len(c) for c in historical.values()) if historical else 0
        print(f"\n  Running {total_candles} simulation steps...")

        for i in range(20, total_candles):
            # Feed partial candle history to simulate real-time
            for symbol, candles in historical.items():
                if i < len(candles):
                    partial = candles[:i + 1]
                    MarketData._cache[f"candles:{symbol}:300:80"] = {
                        "v": partial[-80:], "t": time.time()
                    }
                    MarketData._cache[f"candles:{symbol}:3600:120"] = {
                        "v": partial[-120:], "t": time.time()
                    }
                    price = partial[-1]["close"]
                    MarketData._cache[f"price:{symbol}"] = {
                        "v": price, "t": time.time()
                    }

            engine.run_cycle()

            if (i - 20) % 50 == 0:
                pct = ((i - 20) / max(1, total_candles - 20)) * 100
                print(f"    Progress: {pct:.0f}% | Cycle {i-20} | "
                      f"P&L: ${engine._total_pnl:+,.2f}")

        # Results
        print()
        print("  BACKTEST RESULTS")
        print("  " + "-" * 74)
        engine.print_status()

        perf = engine._get_strategy_performance()
        total_trades = sum(p["total_trades"] for p in perf.values())
        total_wins = sum(p["wins"] for p in perf.values())
        overall_wr = total_wins / total_trades * 100 if total_trades > 0 else 0

        print(f"  Total Trades:  {total_trades}")
        print(f"  Win Rate:      {overall_wr:.1f}%")
        print(f"  Total P&L:     ${engine._total_pnl:+,.2f}")
        print(f"  Return:        {(engine._total_pnl / self.capital) * 100:+.2f}%")
        print()

        # Per-strategy breakdown
        for name, p in perf.items():
            sr_str = f"{p['sharpe']:.2f}" if p['total_trades'] >= 2 else "N/A"
            wr_str = f"{p['win_rate']*100:.0f}%" if p['total_trades'] > 0 else "N/A"
            print(f"  {name:<20} trades={p['total_trades']:<4} "
                  f"WR={wr_str:<6} P&L=${p['total_pnl']:>+8,.2f} "
                  f"Sharpe={sr_str}")

        print()
        print("=" * 78)
        print()

        engine.state_db.close()


# ============================================================================
# CLI
# ============================================================================

def _load_env():
    """Load .env file if present."""
    env_paths = [
        AGENTS_DIR / ".env",
        AGENTS_DIR.parent / ".env",
    ]
    for env_path in env_paths:
        if env_path.exists():
            try:
                for line in env_path.read_text().splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key and key not in os.environ:
                        os.environ[key] = value
            except Exception:
                pass


def cmd_run(args: List[str]):
    """Start all strategies."""
    mode = "live" if "--live" in args else "paper"
    capital = 10000.0

    for i, arg in enumerate(args):
        if arg == "--capital" and i + 1 < len(args):
            try:
                capital = float(args[i + 1])
            except ValueError:
                pass

    engine = StrategyEngine(mode=mode, capital=capital)
    engine.run()


def cmd_status(args: List[str]):
    """Show strategy performance."""
    capital = 10000.0
    saved_db = StateDB()
    saved_capital = saved_db.get_state("capital")
    if saved_capital:
        try:
            capital = float(saved_capital)
        except ValueError:
            pass

    engine = StrategyEngine(mode="paper", capital=capital)

    # Load recent fills to reconstruct performance
    closed = saved_db.get_closed_positions(200)
    if closed:
        print(f"  ({len(closed)} historical closed positions found)")

    engine.print_status()

    # Also show risk manager status
    risk = engine.risk_manager.status()
    print(f"  Risk Limits:")
    print(f"    Max Exposure:     ${risk['max_exposure']:,.2f} (80% of capital)")
    print(f"    Max Single Trade: ${risk['max_single_trade']:,.2f} (5% of capital)")
    print(f"    Max Daily Loss:   ${risk['max_daily_loss']:,.2f} (2% of capital)")
    print()

    saved_db.close()
    engine.state_db.close()


def cmd_backtest(args: List[str]):
    """Run strategies on historical data."""
    days = 7
    capital = 10000.0

    for i, arg in enumerate(args):
        if arg == "--days" and i + 1 < len(args):
            try:
                days = int(args[i + 1])
            except ValueError:
                pass
        if arg == "--capital" and i + 1 < len(args):
            try:
                capital = float(args[i + 1])
            except ValueError:
                pass

    bt = Backtester(days=days, capital=capital)
    bt.run()


def cmd_signals(args: List[str]):
    """Show current live signals."""
    engine = StrategyEngine(mode="paper", capital=10000.0)

    print()
    print("  Generating live signals from all strategies...")
    print()

    # Run one cycle to generate fresh signals
    all_signals = []
    market_data = {}
    for strat in engine.strategies:
        try:
            signals = strat.update(market_data)
            all_signals.extend(signals)
            if signals:
                print(f"  [{strat.name}] generated {len(signals)} signal(s)")
        except Exception as e:
            print(f"  [{strat.name}] error: {e}")

    if not all_signals:
        print("  No signals generated. Markets may be quiet or data unavailable.")
    else:
        print()
        print("=" * 78)
        print(f"  LIVE SIGNALS ({len(all_signals)} total)")
        print("=" * 78)
        print()
        for sig in sorted(all_signals, key=lambda s: s.confidence, reverse=True):
            rr = sig.risk_reward
            print(f"  [{sig.strategy}] {sig.side.value} {sig.symbol}")
            print(f"    Confidence: {sig.confidence:.2f} | R:R = {rr:.2f}")
            print(f"    Entry: ${sig.entry_price:,.2f} | "
                  f"Stop: ${sig.stop_price:,.2f} | "
                  f"Target: ${sig.target_price:,.2f}")
            print(f"    Size: ${sig.size_usd:,.2f}")
            print(f"    Reason: {sig.reason}")
            print()

    # Show aggregated signals
    perf = engine._get_strategy_performance()
    aggregated = engine.signal_aggregator.aggregate(all_signals, perf)
    consensus = [s for s in aggregated if "EXIT" not in s.reason]

    if consensus:
        print("=" * 78)
        print(f"  CONSENSUS SIGNALS ({len(consensus)} — 2+ strategies agree)")
        print("=" * 78)
        for sig in consensus:
            print(f"\n  >>> {sig.side.value} {sig.symbol} "
                  f"(conf={sig.confidence:.2f})")
            print(f"      {sig.reason}")
    else:
        print("  No consensus signals (need 2+ strategies to agree).")

    print()
    engine.print_signals()
    engine.state_db.close()


def main():
    """CLI entry point."""
    _load_env()

    usage = textwrap.dedent(f"""\
    Quant Engine V2 — Multi-Strategy Trading Engine

    Usage:
      python {Path(__file__).name} run [--live] [--capital N]   Start all strategies
      python {Path(__file__).name} status                       Show strategy performance
      python {Path(__file__).name} backtest [--days N]          Run on historical data
      python {Path(__file__).name} signals                      Show current live signals

    Options:
      --live        Execute real trades (default: paper trading)
      --capital N   Starting capital in USD (default: 10000)
      --days N      Backtest period in days (default: 7)

    Strategies:
      1. mean_reversion   VWAP deviation on BTC/ETH/SOL
      2. momentum         EMA crossover with volume (crypto + equities)
      3. latency_arb      RTT anomaly exploitation (our edge)
      4. stat_arb         Pairs trading on correlated ratios
      5. sentiment_alpha  HackerNews/Reddit sentiment divergence

    Risk Rules (HARD CODED):
      Max exposure:     80% of capital
      Max daily loss:   2% of capital (then FULL STOP)
      Max single trade: 5% of capital
      Every trade MUST have a stop loss
      NEVER let a winning trade become a losing trade

    State: {ENGINE_DB}
    Logs:  {LOG_FILE}
    """)

    if len(sys.argv) < 2:
        print(usage)
        sys.exit(0)

    command = sys.argv[1].lower()
    args = sys.argv[2:]

    commands = {
        "run": cmd_run,
        "status": cmd_status,
        "backtest": cmd_backtest,
        "signals": cmd_signals,
    }

    if command in commands:
        commands[command](args)
    elif command in ("--help", "-h", "help"):
        print(usage)
    else:
        print(f"Unknown command: {command}")
        print(f"Run 'python {Path(__file__).name}' for usage.")
        sys.exit(1)


if __name__ == "__main__":
    main()
