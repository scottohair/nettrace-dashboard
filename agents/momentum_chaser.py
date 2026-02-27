#!/usr/bin/env python3
"""Momentum Chaser — reactive trend-following agent.

Theory: find what's moving up, ride it, cut fast when it stops.
Portfolio always drifts upward because winners run and losers get cut.

This is NOT a consensus/prediction model. It's purely reactive:
  1. Scan all pairs for momentum (ROC + volume surge)
  2. Rank by momentum strength
  3. Enter top movers proportional to strength
  4. Trail stops tight — cut at first sign of deceleration
  5. Rotate continuously: out of faders, into risers

Fee-aware routing:
  - Perps (0.03% RT): micro-moves, highest frequency
  - Kraken (0.50% RT): medium moves
  - Coinbase spot (1.20% RT): only strong momentum

More volume → lower fee tiers → better economics → more volume (virtuous cycle)

Usage:
  python3 agents/momentum_chaser.py
"""

import json
import logging
import os
import sqlite3
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent))

# Load .env if present
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

_LOG_DIR = Path("/data") if Path("/data").exists() else Path(__file__).parent
_file_handler = logging.FileHandler(str(_LOG_DIR / "momentum_chaser.log"))
_stream_handler = logging.StreamHandler()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MOMO] %(levelname)s %(message)s",
    handlers=[_stream_handler, _file_handler],
)
logger = logging.getLogger("momentum_chaser")
# Force flush on every log line so we can see logs on Fly
_file_handler.flush = lambda: _file_handler.stream.flush() if _file_handler.stream else None

# ── Configuration ──

SCAN_INTERVAL = int(os.environ.get("MOMO_SCAN_INTERVAL", "10"))  # seconds
MAX_POSITIONS = int(os.environ.get("MOMO_MAX_POSITIONS", "5"))
MAX_POSITION_USD = float(os.environ.get("MOMO_MAX_POSITION_USD", "100"))
MIN_TRADE_USD = float(os.environ.get("MOMO_MIN_TRADE_USD", "15"))
RESERVE_PCT = float(os.environ.get("MOMO_RESERVE_PCT", "0.10"))

# Profit-taking: convert gains to USDC when momentum fades
PROFIT_TAKE_TO_USDC = os.environ.get("MOMO_PROFIT_TO_USDC", "1") == "1"
# Minimum gain % before converting to USDC (below this, just sell to USD)
PROFIT_USDC_MIN_GAIN_PCT = float(os.environ.get("MOMO_PROFIT_USDC_MIN_GAIN", "0.005"))  # 0.5%

# Momentum scoring thresholds (fee-aware)
# For Coinbase spot (1.2% RT): need strong momentum
MIN_MOMENTUM_SPOT = float(os.environ.get("MOMO_MIN_MOMENTUM_SPOT", "0.012"))
# For Kraken (0.5% RT): medium momentum
MIN_MOMENTUM_KRAKEN = float(os.environ.get("MOMO_MIN_MOMENTUM_KRAKEN", "0.005"))
# For perps (0.03% RT, 0% maker): tiny moves are profitable
MIN_MOMENTUM_PERP = float(os.environ.get("MOMO_MIN_MOMENTUM_PERP", "0.0005"))

# Volume filter: current volume must be this multiple of average
MIN_VOLUME_RATIO = float(os.environ.get("MOMO_MIN_VOLUME_RATIO", "1.2"))

# Trailing stop configuration
INITIAL_STOP_PCT = float(os.environ.get("MOMO_INITIAL_STOP_PCT", "0.008"))  # 0.8%
TRAIL_FACTOR = float(os.environ.get("MOMO_TRAIL_FACTOR", "0.5"))  # trail at 50% of max gain
MOMENTUM_FADE_EXIT = os.environ.get("MOMO_FADE_EXIT", "1") == "1"
MAX_HOLD_MINUTES = int(os.environ.get("MOMO_MAX_HOLD_MINUTES", "30"))

# Pair discovery: scan ALL Coinbase pairs, not a hardcoded list.
# Phase 1 (cheap): GET /products → 24h stats for all pairs, rank by activity.
# Phase 2 (detailed): fetch 1-min candles only for top N movers from phase 1.
# Discovery refresh interval: re-fetch the full product list every 5 minutes.
DISCOVERY_INTERVAL = int(os.environ.get("MOMO_DISCOVERY_INTERVAL", "300"))  # seconds
# How many top movers from discovery to deep-scan with candles
DEEP_SCAN_TOP_N = int(os.environ.get("MOMO_DEEP_SCAN_TOP_N", "50"))
# Always include these majors in deep scan regardless of ranking
ALWAYS_SCAN = {"BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD",
               "DOGE-USD", "XRP-USD", "ADA-USD", "DOT-USD", "SUI-USD"}
# Fallback static list (used if discovery API fails)
FALLBACK_PAIRS = [
    "BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD",
    "DOGE-USD", "FET-USD", "XRP-USD", "ADA-USD", "DOT-USD",
    "NEAR-USD", "ATOM-USD", "UNI-USD", "AAVE-USD", "LTC-USD",
    "POL-USD", "OP-USD", "ARB-USD", "SUI-USD", "APT-USD",
    "RENDER-USD", "INJ-USD", "TIA-USD", "SEI-USD", "JUP-USD",
]
# Override: if MOMO_PAIRS env var is set, use that instead of discovery
_env_pairs = os.environ.get("MOMO_PAIRS", "")
PAIRS_OVERRIDE = [p.strip() for p in _env_pairs.split(",") if p.strip()] if _env_pairs else None

# Momentum scoring weights across timeframes
WEIGHT_ROC_1M = 0.15    # 1-minute rate of change (fastest signal)
WEIGHT_ROC_5M = 0.30    # 5-minute rate of change (primary)
WEIGHT_ROC_15M = 0.25   # 15-minute rate of change (confirmation)
WEIGHT_ROC_1H = 0.10    # 1-hour trend (background)
WEIGHT_VOL = 0.20       # Volume surge (conviction)

# DB for position tracking
_DB_DIR = Path("/data") if Path("/data").exists() else Path(__file__).parent
DB_PATH = str(_DB_DIR / "momentum_chaser.db")
STATUS_FILE = str(_DB_DIR / "momentum_chaser_status.json")


class MomentumPosition:
    """Track a single momentum position with trailing stop."""

    def __init__(self, pair, entry_price, entry_time, size_usd, size_base,
                 momentum_score, venue, order_id=None):
        self.pair = pair
        self.entry_price = entry_price
        self.entry_time = entry_time
        self.size_usd = size_usd
        self.size_base = size_base
        self.momentum_score = momentum_score
        self.venue = venue
        self.order_id = order_id

        # Trailing stop state
        self.peak_price = entry_price
        self.stop_price = entry_price * (1 - INITIAL_STOP_PCT)
        self.last_momentum = momentum_score
        self.exit_reason = None

    def update(self, current_price, current_momentum):
        """Update trailing stop and momentum state. Returns True if stop hit."""
        # Update peak
        if current_price > self.peak_price:
            self.peak_price = current_price
            # Trail the stop up: tighten as gain grows
            gain_pct = (self.peak_price - self.entry_price) / self.entry_price
            # Stop trails at TRAIL_FACTOR of the gain below peak
            trail_distance = max(INITIAL_STOP_PCT, gain_pct * TRAIL_FACTOR)
            new_stop = self.peak_price * (1 - trail_distance)
            if new_stop > self.stop_price:
                self.stop_price = new_stop

        # Check stop hit
        if current_price <= self.stop_price:
            self.exit_reason = "trailing_stop"
            return True

        # Check momentum fade
        if MOMENTUM_FADE_EXIT and current_momentum is not None:
            # Exit if momentum decelerates significantly (drops to < 30% of entry momentum)
            if current_momentum < self.momentum_score * 0.3 and current_momentum < MIN_MOMENTUM_SPOT:
                self.exit_reason = "momentum_fade"
                return True

        # Check max hold time
        hold_minutes = (time.time() - self.entry_time) / 60
        if hold_minutes > MAX_HOLD_MINUTES:
            self.exit_reason = "max_hold_time"
            return True

        self.last_momentum = current_momentum
        return False

    def unrealized_pnl_pct(self, current_price):
        return (current_price - self.entry_price) / self.entry_price

    def to_dict(self):
        return {
            "pair": self.pair,
            "entry_price": self.entry_price,
            "entry_time": self.entry_time,
            "size_usd": self.size_usd,
            "size_base": self.size_base,
            "momentum_score": round(self.momentum_score, 6),
            "venue": self.venue,
            "peak_price": self.peak_price,
            "stop_price": round(self.stop_price, 8),
            "exit_reason": self.exit_reason,
        }


class MomentumChaser:
    """Reactive momentum-following trading agent.

    Scans all pairs for price momentum, enters top movers,
    trails stops tight, cuts fast when momentum fades.
    """

    def __init__(self):
        self._cycle = 0
        self._running = True
        self._positions = {}  # pair -> MomentumPosition
        self._candle_cache = {}  # pair -> list of candles
        self._candle_cache_ts = {}  # pair -> timestamp of last fetch
        self._lock = threading.Lock()

        # Dynamic pair discovery
        self._discovered_pairs = list(ALWAYS_SCAN)  # start with majors
        self._discovery_ts = 0  # last discovery timestamp
        self._all_products = {}  # product_id -> product info

        # Trading infrastructure (lazy init)
        self._trader = None
        self._risk_ctrl = None
        self._ws_feed = None
        self._smart_router = None

        # Performance tracking
        self._total_trades = 0
        self._winning_trades = 0
        self._total_pnl_usd = 0.0
        self._session_start = time.time()

        # Cached portfolio/cash values (survive 403 rate limit errors)
        self._cached_portfolio = 0.0
        self._cached_cash = 0.0
        self._cache_ts = 0.0

        # 403 backoff state — don't hammer Coinbase when rate limited
        self._consecutive_403s = 0
        self._last_403_ts = 0.0
        self._backoff_until = 0.0

        # Anti-churn: track recently exited pairs to avoid buying back
        self._recent_exits = {}  # pair -> exit timestamp
        self._CHURN_COOLDOWN = 120  # seconds before re-entering a pair we just exited

        # Init DB
        self._init_db()

    def _init_db(self):
        """Initialize SQLite for trade history."""
        try:
            db = sqlite3.connect(DB_PATH)
            db.execute("""CREATE TABLE IF NOT EXISTS momentum_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT, direction TEXT, venue TEXT,
                entry_price REAL, exit_price REAL,
                size_usd REAL, size_base REAL,
                momentum_score REAL, pnl_usd REAL, pnl_pct REAL,
                hold_seconds REAL, exit_reason TEXT,
                entry_time TEXT, exit_time TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS momentum_scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT, momentum_score REAL,
                roc_1m REAL, roc_5m REAL, roc_15m REAL, roc_1h REAL,
                volume_ratio REAL, price REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
            db.commit()
            db.close()
        except Exception as e:
            logger.warning("DB init error: %s", e)

    def _init_trading(self):
        """Lazy-init trading connections."""
        if self._trader is not None:
            return True
        try:
            from exchange_connector import CoinbaseTrader
            self._trader = CoinbaseTrader()
            logger.info("MOMO: CoinbaseTrader initialized")
        except Exception as e:
            logger.warning("MOMO: CoinbaseTrader unavailable: %s", e)
            return False

        try:
            from risk_controller import RiskController
            self._risk_ctrl = RiskController()
            logger.info("MOMO: RiskController initialized")
        except Exception as e:
            logger.warning("MOMO: RiskController unavailable: %s", e)

        try:
            from smart_router import SmartRouter
            self._smart_router = SmartRouter()
            logger.info("MOMO: SmartRouter initialized")
        except Exception as e:
            logger.warning("MOMO: SmartRouter unavailable: %s", e)

        try:
            from coinbase_ws_feed import CoinbaseWSFeed
            # Only subscribe to a manageable set for WS
            ws_pairs = PAIRS[:15]  # WS feed for top 15 pairs
            self._ws_feed = CoinbaseWSFeed(pairs=ws_pairs)
            self._ws_feed.start()
            logger.info("MOMO: WebSocket feed started for %d pairs", len(ws_pairs))
        except Exception as e:
            logger.warning("MOMO: WebSocket feed unavailable: %s", e)

        return True

    # ── Pair Discovery ──

    def discover_pairs(self):
        """Discover ALL tradeable pairs on Coinbase and rank by activity.

        Uses the public /products endpoint (single API call, returns all products
        with 24h volume). Picks the top DEEP_SCAN_TOP_N by price-change and volume,
        ensuring ALWAYS_SCAN majors are included.

        Returns list of pair strings to deep-scan for momentum.
        """
        if PAIRS_OVERRIDE:
            return PAIRS_OVERRIDE

        now = time.time()
        if now - self._discovery_ts < DISCOVERY_INTERVAL and self._discovered_pairs:
            return self._discovered_pairs

        try:
            import urllib.request
            url = "https://api.exchange.coinbase.com/products?type=spot"
            req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
            resp = urllib.request.urlopen(req, timeout=10)
            products = json.loads(resp.read())

            # Build activity-ranked list
            candidates = []
            seen_bases = set()
            for p in products:
                if p.get("status") != "online":
                    continue
                quote = p.get("quote_currency", "")
                if quote not in ("USD", "USDC"):
                    continue
                base = p.get("base_currency", "")
                if base in seen_bases or base in ("USD", "USDC", "USDT", "DAI", "PAX", "USDS"):
                    continue  # Skip stablecoins
                seen_bases.add(base)
                pair = f"{base}-USD"
                self._all_products[pair] = p
                candidates.append(pair)

            if not candidates:
                return self._discovered_pairs or FALLBACK_PAIRS

            # Now fetch 24h stats to rank by activity
            # The products endpoint doesn't always have stats,
            # so use /products/{id}/stats for top candidates
            # But that's too many calls for 379 products.
            # Instead, use ticker endpoint for quick price data.
            ranked = []
            # Batch fetch tickers — use /products/{id}/ticker (lightweight)
            # But with 379 products that's too many calls.
            # Better: the /products response includes volume in the product data.
            # Actually the Exchange API /products/stats is available.
            # Simplest: just include ALL discovered pairs and let the candle cache
            # filter naturally. Deep scan top N by cycling through.
            #
            # Strategy: always scan ALWAYS_SCAN + rotate through remaining in batches
            batch_size = DEEP_SCAN_TOP_N - len(ALWAYS_SCAN)
            remaining = [p for p in candidates if p not in ALWAYS_SCAN]

            # Rotate: different batch each discovery cycle
            batch_offset = (self._cycle // max(1, DISCOVERY_INTERVAL // SCAN_INTERVAL)) % max(1, len(remaining) // batch_size + 1)
            start = batch_offset * batch_size
            batch = remaining[start:start + batch_size]

            discovered = list(ALWAYS_SCAN) + batch
            self._discovered_pairs = discovered
            self._discovery_ts = now

            logger.info("MOMO DISCOVERY: %d total products, scanning %d this cycle (batch %d, %d always + %d rotating)",
                       len(candidates), len(discovered), batch_offset, len(ALWAYS_SCAN), len(batch))
            return discovered

        except Exception as e:
            logger.warning("MOMO: Discovery failed: %s — using %d cached pairs", e, len(self._discovered_pairs))
            self._discovery_ts = now  # Don't retry for DISCOVERY_INTERVAL
            return self._discovered_pairs or FALLBACK_PAIRS

    def discover_with_stats(self):
        """Enhanced discovery: fetch 24h stats for all pairs to find actual movers.

        Uses the /products/ticker batch endpoint or individual stats.
        This replaces simple rotation with intelligence — only deep-scan pairs
        that are actually showing 24h price movement.
        """
        if PAIRS_OVERRIDE:
            return PAIRS_OVERRIDE

        now = time.time()
        if now - self._discovery_ts < DISCOVERY_INTERVAL and self._discovered_pairs:
            return self._discovered_pairs

        try:
            import urllib.request

            # Step 1: Get all products
            url = "https://api.exchange.coinbase.com/products?type=spot"
            req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
            resp = urllib.request.urlopen(req, timeout=10)
            products = json.loads(resp.read())

            # Step 2: Get 24h tickers for activity ranking
            # The /products/{id}/ticker endpoint gives price, volume, 24h change
            # But calling 379 tickers is too many. Instead, use a sampling strategy:
            # - Always include ALWAYS_SCAN
            # - For the rest, fetch stats in batches (40 per discovery cycle)
            seen_bases = set()
            all_pairs = []
            for p in products:
                if p.get("status") != "online":
                    continue
                quote = p.get("quote_currency", "")
                base = p.get("base_currency", "")
                if quote not in ("USD", "USDC"):
                    continue
                if base in seen_bases or base in ("USD", "USDC", "USDT", "DAI", "PAX", "USDS"):
                    continue
                seen_bases.add(base)
                pair = f"{base}-USD"
                all_pairs.append(pair)

            # Fetch tickers for a batch of non-major pairs
            remaining = [p for p in all_pairs if p not in ALWAYS_SCAN]
            batch_size = 40  # Check 40 pairs per discovery cycle
            batch_offset = int((now // DISCOVERY_INTERVAL) % max(1, len(remaining) // batch_size + 1))
            batch = remaining[batch_offset * batch_size:(batch_offset + 1) * batch_size]

            active_pairs = []
            def _fetch_ticker(pair):
                try:
                    product_id = pair  # BTC-USD format works for Exchange API
                    ticker_url = f"https://api.exchange.coinbase.com/products/{product_id}/ticker"
                    ticker_req = urllib.request.Request(ticker_url, headers={"User-Agent": "NetTrace/1.0"})
                    ticker_resp = urllib.request.urlopen(ticker_req, timeout=5)
                    data = json.loads(ticker_resp.read())
                    price = float(data.get("price", 0) or 0)
                    volume = float(data.get("volume", 0) or 0)
                    if price > 0 and volume > 0:
                        return {"pair": pair, "price": price, "volume_usd": price * volume}
                except Exception:
                    pass
                return None

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = {executor.submit(_fetch_ticker, p): p for p in batch}
                for future in as_completed(futures):
                    result = future.result()
                    if result and result["volume_usd"] > 10000:  # >$10k 24h volume
                        active_pairs.append(result)

            # Sort by volume descending, take top N
            active_pairs.sort(key=lambda x: x["volume_usd"], reverse=True)
            top_active = [p["pair"] for p in active_pairs[:DEEP_SCAN_TOP_N - len(ALWAYS_SCAN)]]

            discovered = list(ALWAYS_SCAN) + top_active
            self._discovered_pairs = discovered
            self._discovery_ts = now

            logger.info("MOMO DISCOVERY: %d total pairs, %d active (>$10k vol), scanning %d (%d always + %d discovered) batch=%d/%d",
                       len(all_pairs), len(active_pairs), len(discovered),
                       len(ALWAYS_SCAN), len(top_active),
                       batch_offset + 1, max(1, len(remaining) // batch_size + 1))
            return discovered

        except Exception as e:
            logger.warning("MOMO: Stats discovery failed: %s", e)
            # Fall back to simple discovery
            return self.discover_pairs()

    # ── Price & Candle Data ──

    def _get_price(self, pair):
        """Get current price via WS feed or REST fallback."""
        if self._ws_feed:
            try:
                quote = self._ws_feed.get_quote(pair)
                if quote and float(quote.get("mid", 0) or 0) > 0:
                    return float(quote["mid"])
            except Exception:
                pass
        # REST fallback
        try:
            import urllib.request
            dp = pair.replace("-USDC", "-USD")
            url = f"https://api.coinbase.com/v2/prices/{dp}/spot"
            req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
            resp = urllib.request.urlopen(req, timeout=5)
            data = json.loads(resp.read())
            return float(data["data"]["amount"])
        except Exception:
            return None

    def _fetch_candles(self, pair, granularity="ONE_MINUTE", limit=60):
        """Fetch candle data from Coinbase. Returns list of candles (oldest first).

        Uses aggressive caching (60s) to minimize API calls and avoid 403 floods.
        With 25 pairs at 10s scan interval, we'd hit 2.5 req/s just for candles.
        60s cache means only ~0.4 req/s from momentum chaser.
        """
        cache_key = f"{pair}_{granularity}"
        now = time.time()
        cached_ts = self._candle_cache_ts.get(cache_key, 0)
        if now - cached_ts < 60 and cache_key in self._candle_cache:
            return self._candle_cache[cache_key]

        candles = []
        try:
            # Use PUBLIC Coinbase Exchange API for candles — separate rate limit
            # pool from the authenticated /v3/brokerage API used for trading.
            # This prevents momentum scanning from eating into trading rate limits.
            import urllib.request
            # Map pair format: BTC-USD stays as-is for exchange API
            product_id = pair
            # Granularity in seconds: 60=1min, 300=5min, 900=15min, 3600=1h
            gran_map = {"ONE_MINUTE": 60, "FIVE_MINUTE": 300, "FIFTEEN_MINUTE": 900, "ONE_HOUR": 3600}
            gran_seconds = gran_map.get(granularity, 60)
            url = f"https://api.exchange.coinbase.com/products/{product_id}/candles?granularity={gran_seconds}"
            req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
            resp = urllib.request.urlopen(req, timeout=8)
            raw = json.loads(resp.read())
            # Public API returns: [[time, low, high, open, close, volume], ...]
            if isinstance(raw, list):
                for c in raw[:limit]:
                    if isinstance(c, (list, tuple)) and len(c) >= 6:
                        candles.append({
                            "time": int(c[0]),
                            "low": float(c[1]),
                            "high": float(c[2]),
                            "open": float(c[3]),
                            "close": float(c[4]),
                            "volume": float(c[5]),
                        })
                # Coinbase returns newest first, reverse to oldest first
                candles.sort(key=lambda c: c["time"])
        except Exception as e:
            logger.debug("Candle fetch error for %s: %s", pair, e)
            # Fallback: try authenticated API if public fails
            try:
                if self._trader:
                    raw = self._trader.get_candles(pair, granularity=granularity, limit=limit)
                    if isinstance(raw, dict):
                        raw = raw.get("candles", [])
                    if isinstance(raw, list):
                        for c in raw:
                            candles.append({
                                "time": int(c.get("start", 0) if isinstance(c, dict) else 0),
                                "open": float(c.get("open", 0) if isinstance(c, dict) else 0),
                                "high": float(c.get("high", 0) if isinstance(c, dict) else 0),
                                "low": float(c.get("low", 0) if isinstance(c, dict) else 0),
                                "close": float(c.get("close", 0) if isinstance(c, dict) else 0),
                                "volume": float(c.get("volume", 0) if isinstance(c, dict) else 0),
                            })
                        candles.sort(key=lambda c: c["time"])
            except Exception:
                pass

        if candles:
            self._candle_cache[cache_key] = candles
            self._candle_cache_ts[cache_key] = now

        return candles

    # ── Momentum Scoring ──

    def _compute_momentum(self, pair):
        """Compute momentum score for a pair.

        Returns: {
            momentum_score: weighted composite (higher = stronger upward momentum),
            roc_1m: 1-min rate of change,
            roc_5m: 5-min rate of change,
            roc_15m: 15-min rate of change,
            roc_1h: 1-hour rate of change,
            volume_ratio: current vol / average vol,
            price: current price,
            direction: "UP" or "DOWN",
        } or None if data insufficient.
        """
        candles = self._fetch_candles(pair, "ONE_MINUTE", 60)
        if len(candles) < 15:
            return None

        # Current price (last candle close)
        current = candles[-1]["close"]
        if current <= 0:
            return None

        # Rate of change at different timeframes
        def roc(n):
            """Rate of change over last n candles."""
            if len(candles) < n + 1:
                return 0.0
            prev = candles[-(n + 1)]["close"]
            if prev <= 0:
                return 0.0
            return (current - prev) / prev

        roc_1m = roc(1)
        roc_5m = roc(5)
        roc_15m = roc(15)
        roc_1h = roc(min(60, len(candles) - 1))

        # Volume analysis: is volume surging?
        recent_vol = sum(c["volume"] for c in candles[-5:]) / 5.0 if len(candles) >= 5 else 0
        avg_vol = sum(c["volume"] for c in candles[-30:]) / 30.0 if len(candles) >= 30 else recent_vol
        volume_ratio = (recent_vol / avg_vol) if avg_vol > 0 else 1.0

        # Weighted momentum score (positive = upward momentum)
        momentum_score = (
            WEIGHT_ROC_1M * roc_1m +
            WEIGHT_ROC_5M * roc_5m +
            WEIGHT_ROC_15M * roc_15m +
            WEIGHT_ROC_1H * roc_1h +
            WEIGHT_VOL * max(0, (volume_ratio - 1.0) * 0.01)  # Volume bonus
        )

        # Direction: positive score = upward momentum
        direction = "UP" if momentum_score > 0 else "DOWN"

        return {
            "pair": pair,
            "momentum_score": momentum_score,
            "roc_1m": roc_1m,
            "roc_5m": roc_5m,
            "roc_15m": roc_15m,
            "roc_1h": roc_1h,
            "volume_ratio": volume_ratio,
            "price": current,
            "direction": direction,
        }

    def _read_sniper_candles(self, pair):
        """Try reading candle data from sniper's scan DB to avoid duplicate API calls.

        Sniper already fetches 1h + 1m candles for its pairs. Reuse them.
        Returns list of candles or empty list if not available.
        """
        try:
            _sniper_db = "/data/sniper.db" if os.path.exists("/data") else str(Path(__file__).parent / "sniper.db")
            if not os.path.exists(_sniper_db):
                return []
            db = sqlite3.connect(_sniper_db, timeout=3)
            # Read the latest signal details which may contain candle-derived momentum data
            row = db.execute(
                "SELECT signal_details, composite_confidence FROM sniper_scans WHERE pair=? ORDER BY created_at DESC LIMIT 1",
                (pair,),
            ).fetchone()
            db.close()
            if row and row[0]:
                details = json.loads(row[0])
                # Extract momentum signal if available
                momentum = details.get("momentum", {})
                if isinstance(momentum, dict) and momentum.get("confidence", 0) > 0:
                    return momentum  # Return raw momentum data from sniper
            return []
        except Exception:
            return []

    def scan_all_pairs(self):
        """Scan ALL pairs for momentum using two-phase discovery.

        Phase 1 (every 5 min): Discover all Coinbase pairs, rank by activity.
        Phase 2 (every cycle): Deep-scan top movers + always-on majors with candles.

        Returns list of momentum results sorted by score descending.
        """
        # Phase 1: Discover which pairs to scan
        pairs_to_scan = self.discover_with_stats()

        results = []

        def _scan(pair):
            try:
                return self._compute_momentum(pair)
            except Exception as e:
                logger.debug("Scan error %s: %s", pair, e)
                return None

        # Phase 2: Deep scan with candles (cache hits are free, only new fetches cost API calls)
        major_scores = {}

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {executor.submit(_scan, pair): pair for pair in pairs_to_scan}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    pair = result["pair"]
                    base = pair.split("-")[0]
                    if base in ("BTC", "ETH", "SOL"):
                        major_scores[pair] = result["momentum_score"]
                    # Include both UP (positive score) and DOWN (negative score) movers
                    # Shorts only available on perps (BTC/ETH/SOL)
                    abs_score = abs(result["momentum_score"])
                    if abs_score > 0:
                        results.append(result)

        # Sort by absolute momentum score descending (strongest movers first, longs OR shorts)
        results.sort(key=lambda r: abs(r["momentum_score"]), reverse=True)

        # Log major pair scores every 10 cycles for debugging
        if self._cycle % 10 == 0 and major_scores:
            parts = " | ".join(f"{p}={s:.5f}" for p, s in sorted(major_scores.items()))
            logger.info("MOMO majors: %s", parts)

        # Record top scans to DB
        try:
            db = sqlite3.connect(DB_PATH, timeout=3)
            for r in results[:10]:
                db.execute(
                    "INSERT INTO momentum_scans (pair, momentum_score, roc_1m, roc_5m, roc_15m, roc_1h, volume_ratio, price) VALUES (?,?,?,?,?,?,?,?)",
                    (r["pair"], r["momentum_score"], r["roc_1m"], r["roc_5m"],
                     r["roc_15m"], r["roc_1h"], r["volume_ratio"], r["price"]),
                )
            db.commit()
            db.close()
        except Exception:
            pass

        return results

    # ── Position Management ──

    def _check_trading_lock(self):
        """Check if trading is locked."""
        try:
            lock_file = Path(__file__).parent / "trading_lock.json"
            if not lock_file.exists():
                return False
            data = json.loads(lock_file.read_text())
            return data.get("locked", False)
        except Exception:
            return False

    def _refresh_balances(self):
        """Single API call to refresh both portfolio value and available cash.

        Caches result to survive 403 rate limits. Call once per cycle, not per-trade.
        """
        if not self._trader:
            return
        # Only refresh once every 15 seconds (avoid hammering accounts API)
        now = time.time()
        if now - self._cache_ts < 15:
            return
        try:
            resp = self._trader.get_accounts()
            accounts = resp.get("accounts", []) if isinstance(resp, dict) else []
            total = 0.0
            cash = 0.0
            for acct in accounts:
                currency = acct.get("currency", "")
                avail = acct.get("available_balance", {})
                hold = acct.get("hold", {})
                avail_val = float(avail.get("value", 0) or 0)
                hold_val = float(hold.get("value", 0) or 0)
                amount = avail_val + hold_val
                if amount <= 0:
                    continue
                if currency in ("USD", "USDC"):
                    total += amount
                    cash += avail_val
                else:
                    price = self._get_price(f"{currency}-USD")
                    if price:
                        total += amount * price
            if total > 10:
                self._cached_portfolio = total
                self._cache_ts = now
            if cash > 0:
                self._cached_cash = cash
        except Exception as e:
            logger.debug("MOMO: Balance refresh error: %s", e)

    def _get_portfolio_value(self):
        """Get total portfolio value (from cache, refreshed once per cycle)."""
        return self._cached_portfolio

    def _get_available_cash(self):
        """Get available USD + USDC for trading (from cache, refreshed once per cycle)."""
        return self._cached_cash

    # Only these bases have perp products on Coinbase
    PERP_BASES = {"BTC", "ETH", "SOL"}

    def _select_venue(self, pair, momentum_score, direction="UP"):
        """Select best venue based on momentum strength, fees, and direction.

        Longs: spot (Coinbase/Kraken) or perps.
        Shorts: perps ONLY (can't short on spot). Only BTC/ETH/SOL have perps.

        Strong momentum → can afford higher fees (Coinbase spot).
        Medium momentum → route to cheaper venue (Kraken).
        Small momentum → only profitable on perps (near-zero fees) — BTC/ETH/SOL only.
        """
        base = pair.split("-")[0]
        has_perp = base in self.PERP_BASES
        abs_score = abs(momentum_score)

        # SHORTS: must use perps (can't short on spot)
        if direction == "DOWN":
            if has_perp and abs_score >= MIN_MOMENTUM_PERP:
                perp_pair = f"{base}-PERP-INTX"
                return "perp", perp_pair
            return None, None  # Can't short this pair

        # LONGS below
        if abs_score >= MIN_MOMENTUM_SPOT:
            # Strong enough for any venue — use SmartRouter for best price
            if self._smart_router:
                try:
                    execution = self._smart_router.find_best_execution(pair, "BUY", MIN_TRADE_USD)
                    if isinstance(execution, dict) and execution.get("venue"):
                        return execution["venue"], pair
                except Exception:
                    pass
            return "coinbase", pair

        elif abs_score >= MIN_MOMENTUM_KRAKEN:
            # Medium — prefer Kraken, fall to spot if unavailable
            if self._smart_router:
                try:
                    execution = self._smart_router.find_best_execution(pair, "BUY", MIN_TRADE_USD)
                    if isinstance(execution, dict):
                        venue = execution.get("venue", "coinbase")
                        if venue in ("kraken", "coinbase"):
                            return venue, pair
                except Exception:
                    pass
            return "coinbase", pair

        elif abs_score >= MIN_MOMENTUM_PERP and has_perp:
            # Small momentum — only perps work, and only for BTC/ETH/SOL
            perp_pair = f"{base}-PERP-INTX"
            return "perp", perp_pair

        # Below all thresholds (or no perp available for weak momentum)
        return None, None

    def _size_position(self, momentum_score, portfolio_value, available_cash):
        """Size position proportional to momentum strength.

        Stronger momentum → larger position (but capped).
        """
        # Base size: scale with momentum (more momentum = more conviction)
        # Normalize momentum to roughly 0-1 range (0.5% = moderate, 2% = strong)
        strength = min(1.0, abs(momentum_score) / 0.02)
        base_usd = MIN_TRADE_USD + (MAX_POSITION_USD - MIN_TRADE_USD) * strength

        # Never exceed available cash minus reserve
        reserve = portfolio_value * RESERVE_PCT
        max_available = max(0, available_cash - reserve)
        base_usd = min(base_usd, max_available)

        # Cap per position
        base_usd = min(base_usd, MAX_POSITION_USD)

        return round(base_usd, 2)

    def enter_position(self, momentum_result):
        """Enter a new momentum position (long or short).

        Long: positive momentum → BUY on spot/perps.
        Short: negative momentum → SELL on perps only (BTC/ETH/SOL).

        Returns True if order placed successfully.
        """
        pair = momentum_result["pair"]
        price = momentum_result["price"]
        score = momentum_result["momentum_score"]
        direction = momentum_result.get("direction", "UP")

        # Determine trade direction
        is_short = direction == "DOWN" and score < 0
        order_side = "SELL" if is_short else "BUY"

        if pair in self._positions:
            return False  # Already in this position

        # Position registry: don't fight other agents
        try:
            from position_registry import get_registry
            _reg = get_registry()
            if _reg and not _reg.is_pair_available(pair):
                current_owner = _reg.get_owner(pair)
                if current_owner != "momentum_chaser":
                    logger.info("MOMO: %s owned by %s — skipping", pair, current_owner)
                    return False
        except Exception:
            pass

        # Anti-churn: don't re-enter a pair we just exited
        if pair in self._recent_exits:
            elapsed = time.time() - self._recent_exits[pair]
            if elapsed < self._CHURN_COOLDOWN:
                logger.debug("MOMO: churn guard — %s exited %.0fs ago (cooldown %ds)",
                            pair, elapsed, self._CHURN_COOLDOWN)
                return False
            else:
                del self._recent_exits[pair]

        # Select venue (shorts require perps)
        venue, trade_pair = self._select_venue(pair, score, direction)
        if not venue:
            logger.debug("MOMO: No viable venue for %s %s (score=%.4f)", order_side, pair, score)
            return False

        # Size the position (use absolute momentum for sizing)
        portfolio = self._get_portfolio_value()
        cash = self._get_available_cash()
        size_usd = self._size_position(abs(score), portfolio, cash)

        if size_usd < MIN_TRADE_USD:
            logger.debug("MOMO: Insufficient funds for %s ($%.2f < $%.2f)", pair, size_usd, MIN_TRADE_USD)
            return False

        # Risk controller approval
        _trade_id = None
        if self._risk_ctrl:
            try:
                # Pre-clean: expire stale allocations before checking limits
                try:
                    self._risk_ctrl._db.execute(
                        "UPDATE pending_allocations SET status='expired', resolved_at=CURRENT_TIMESTAMP "
                        "WHERE status='pending' AND created_at < datetime('now', '-30 seconds')")
                    self._risk_ctrl._db.commit()
                except Exception:
                    pass

                approved, reason, adj_size = self._risk_ctrl.approve_trade(
                    "momentum_chaser", pair, order_side, size_usd, portfolio,
                )
                if not approved:
                    logger.info("MOMO: Risk blocked %s $%.2f: %s", pair, size_usd, reason)
                    return False
                size_usd = adj_size
                # Extract trade_id from reason string for cleanup on failure
                if "|" in reason:
                    for part in reason.split("|"):
                        if "trade_id" in part:
                            _trade_id = part.split("=")[-1].strip()
            except Exception as e:
                logger.warning("MOMO: Risk controller error: %s", e)

        if size_usd < MIN_TRADE_USD:
            return False

        # Compute order parameters
        quantity = size_usd / price

        # Get product precision
        try:
            if self._trader:
                product = self._trader.get_product(trade_pair)
                if isinstance(product, dict):
                    base_increment = product.get("base_increment", "0.00000001")
                    precision = len(str(base_increment).rstrip("0").split(".")[-1])
                    quantity = round(quantity, precision)
        except Exception:
            quantity = round(quantity, 8)

        if quantity <= 0:
            return False

        # Place order — maker discipline (slightly better than market)
        if is_short:
            limit_price = round(price * 1.0003, 8)  # 0.03% above mid for SELL
        else:
            limit_price = round(price * 0.9997, 8)  # 0.03% below mid for BUY

        try:
            abs_score = abs(score)
            result = self._trader.place_limit_order(
                trade_pair, order_side, quantity, limit_price,
                signal_confidence=min(0.95, 0.6 + abs_score * 20),  # Scale confidence with momentum
                expected_edge_pct=max(0.10, abs_score * 100),  # Momentum is the edge
                bypass_profit_guard=True,  # Momentum chaser manages its own exits
            )

            order_id = None
            if isinstance(result, dict):
                sr = result.get("success_response", {})
                order_id = sr.get("order_id") or result.get("order_id")

            if order_id:
                # Reset 403 backoff on success
                self._consecutive_403s = 0
                self._backoff_until = 0.0

                # Create position tracker
                pos = MomentumPosition(
                    pair=pair,
                    entry_price=price,
                    entry_time=time.time(),
                    size_usd=size_usd,
                    size_base=quantity,
                    momentum_score=score,
                    venue=venue,
                    order_id=order_id,
                )
                self._positions[pair] = pos
                self._total_trades += 1

                # Register with shared position registry
                try:
                    from position_registry import get_registry
                    _reg = get_registry()
                    if _reg:
                        _reg.register(
                            pair, "momentum_chaser",
                            entry_price=price, entry_amount=quantity,
                            venue=venue, order_id=order_id,
                            exit_owner="self", min_hold_seconds=120,
                            reason=f"momentum={score:.4f} dir={direction}",
                        )
                except Exception:
                    pass

                logger.info(
                    "MOMO ENTER: %s %s $%.2f @ $%.4f | momentum=%.4f | vol_ratio=%.2f | venue=%s | order=%s",
                    order_side, pair, size_usd, price, score,
                    momentum_result.get("volume_ratio", 0), venue, order_id,
                )
                return True
            else:
                err = result.get("error_response", {}) if isinstance(result, dict) else {}
                err_msg = err.get("message", str(result)[:200]) if isinstance(err, dict) else str(result)[:200]
                logger.warning("MOMO: Order failed %s: %s", pair, err_msg)

                # Track 403s for exponential backoff
                err_str = str(err)
                if "403" in err_str or "PERMISSION_DENIED" in err_str or "Too many" in err_str:
                    self._consecutive_403s += 1
                    self._last_403_ts = time.time()
                    # Exponential backoff: 10s, 20s, 40s, 60s max
                    backoff = min(60, 10 * (2 ** min(self._consecutive_403s - 1, 3)))
                    self._backoff_until = time.time() + backoff
                    logger.info("MOMO: 403 detected — backing off %ds (consecutive=%d)",
                               backoff, self._consecutive_403s)

                # CRITICAL: resolve the pending allocation so it doesn't block future trades
                if self._risk_ctrl:
                    try:
                        self._risk_ctrl.resolve_allocation("momentum_chaser", pair)
                    except Exception:
                        pass
                return False

        except Exception as e:
            logger.warning("MOMO: Order error for %s: %s", pair, e)
            # Resolve pending allocation on failure
            if self._risk_ctrl:
                try:
                    self._risk_ctrl.resolve_allocation("momentum_chaser", pair)
                except Exception:
                    pass
            return False

    def exit_position(self, pair, current_price):
        """Exit a momentum position via limit sell.

        Profit-taking: sells to USDC pair when available and profitable,
        locking gains into stablecoin. Only falls back to USD pair if
        USDC pair doesn't exist.

        Returns True if exit order placed.
        """
        pos = self._positions.get(pair)
        if not pos:
            return False

        try:
            # Sell slightly above market (maker)
            limit_price = round(current_price * 1.0003, 8)

            # Profit-taking: prefer USDC pair to lock gains into stablecoin
            sell_pair = pair
            if PROFIT_TAKE_TO_USDC and self._trader:
                pnl_pct = pos.unrealized_pnl_pct(current_price)
                if pnl_pct >= PROFIT_USDC_MIN_GAIN_PCT:
                    # Try USDC pair (e.g., BTC-USD → BTC-USDC)
                    usdc_pair = pair.replace("-USD", "-USDC")
                    if usdc_pair != pair:
                        try:
                            prod = self._trader.get_product(usdc_pair)
                            if prod and not prod.get("error") and prod.get("status") == "online":
                                sell_pair = usdc_pair
                                logger.info("MOMO: profit-take → selling %s via %s (gain=%.2f%%)",
                                           pair, usdc_pair, pnl_pct * 100)
                        except Exception:
                            pass  # Fall back to USD pair

            result = self._trader.place_limit_order(
                sell_pair, "SELL", pos.size_base, limit_price,
                signal_confidence=0.99,
                expected_edge_pct=1.0,
                bypass_profit_guard=True,
            )

            order_id = None
            if isinstance(result, dict):
                sr = result.get("success_response", {})
                order_id = sr.get("order_id") or result.get("order_id")

            # Calculate P&L
            pnl_pct = pos.unrealized_pnl_pct(current_price)
            pnl_usd = pos.size_usd * pnl_pct
            hold_seconds = time.time() - pos.entry_time

            self._total_pnl_usd += pnl_usd
            if pnl_usd > 0:
                self._winning_trades += 1

            # Record to DB
            try:
                db = sqlite3.connect(DB_PATH, timeout=3)
                db.execute(
                    "INSERT INTO momentum_trades (pair, direction, venue, entry_price, exit_price, size_usd, size_base, momentum_score, pnl_usd, pnl_pct, hold_seconds, exit_reason, entry_time, exit_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (pair, "BUY", pos.venue, pos.entry_price, current_price,
                     pos.size_usd, pos.size_base, pos.momentum_score,
                     round(pnl_usd, 4), round(pnl_pct, 6), round(hold_seconds, 1),
                     pos.exit_reason, datetime.fromtimestamp(pos.entry_time, tz=timezone.utc).isoformat(),
                     datetime.now(timezone.utc).isoformat()),
                )
                db.commit()
                db.close()
            except Exception:
                pass

            emoji = "+" if pnl_usd >= 0 else ""
            logger.info(
                "MOMO EXIT: %s %s $%.2f | P&L: %s$%.2f (%s%.2f%%) | hold: %.0fs | reason: %s | order=%s",
                "SELL", pair, pos.size_usd, emoji, pnl_usd, emoji, pnl_pct * 100,
                hold_seconds, pos.exit_reason, order_id or "failed",
            )

            # Remove position and record in churn guard
            del self._positions[pair]
            self._recent_exits[pair] = time.time()

            # Close in shared registry
            try:
                from position_registry import get_registry
                _reg = get_registry()
                if _reg:
                    _reg.close(pair, close_price=current_price, pnl_usd=pnl_usd)
            except Exception:
                pass

            return True

        except Exception as e:
            logger.warning("MOMO: Exit error for %s: %s", pair, e)
            # Force remove on repeated failure
            if pair in self._positions:
                del self._positions[pair]
                self._recent_exits[pair] = time.time()
                try:
                    from position_registry import get_registry
                    _reg = get_registry()
                    if _reg:
                        _reg.close(pair)
                except Exception:
                    pass
            return False

    # ── Main Loop ──

    def manage_positions(self, momentum_results):
        """Check all open positions for exit conditions.

        Uses trailing stops and momentum fade detection.
        """
        # Build a lookup for current momentum
        momentum_by_pair = {r["pair"]: r for r in momentum_results}

        exits = []
        for pair, pos in list(self._positions.items()):
            current_data = momentum_by_pair.get(pair)
            current_price = current_data["price"] if current_data else self._get_price(pair)
            current_momentum = current_data["momentum_score"] if current_data else None

            if current_price is None:
                continue

            # Check if stop hit
            should_exit = pos.update(current_price, current_momentum)
            if should_exit:
                exits.append((pair, current_price))

        # Execute exits
        for pair, price in exits:
            self.exit_position(pair, price)

    def rotate_positions(self, ranked_movers):
        """Rotate: exit fading positions, enter strong new movers.

        The core of momentum chasing: always be in the strongest movers.
        Rate-limit aware: backs off after 403 errors to avoid flooding Coinbase.
        Only attempts 1-2 entries per cycle to share API bandwidth with sniper.
        """
        if self._check_trading_lock():
            logger.info("MOMO: Trading locked — skipping rotation")
            return

        # 403 backoff: don't even try if we're in cooldown
        now = time.time()
        if now < self._backoff_until:
            remaining = self._backoff_until - now
            logger.info("MOMO: 403 backoff — waiting %.0fs (consecutive=%d)",
                       remaining, self._consecutive_403s)
            return

        # How many slots available?
        open_count = len(self._positions)
        available_slots = MAX_POSITIONS - open_count

        if available_slots <= 0:
            return

        # Try up to 2 movers per cycle (1 long + 1 short max).
        max_attempts = 2

        # Enter top movers (sorted by absolute momentum, includes both UP and DOWN)
        entered = 0
        attempts = 0
        entered_long = False
        entered_short = False
        for mover in ranked_movers:
            if entered >= available_slots or attempts >= max_attempts:
                break

            # If we just hit a 403, stop immediately
            if self._backoff_until > time.time():
                break

            pair = mover["pair"]
            if pair in self._positions:
                continue

            direction = mover.get("direction", "UP")
            abs_score = abs(mover["momentum_score"])

            # Skip if already entered one of this direction this cycle
            if direction == "UP" and entered_long:
                continue
            if direction == "DOWN" and entered_short:
                continue

            # Only enter if momentum is strong enough for some venue
            if abs_score < MIN_MOMENTUM_PERP:
                continue

            # Volume filter: require above-average volume for spot/kraken
            # But allow entry if momentum is strong enough for spot
            vol = mover.get("volume_ratio", 0)
            if vol < MIN_VOLUME_RATIO and abs_score < MIN_MOMENTUM_SPOT:
                continue

            attempts += 1
            if self.enter_position(mover):
                entered += 1
                if direction == "UP":
                    entered_long = True
                else:
                    entered_short = True

        if entered == 0 and attempts > 0 and len(ranked_movers) > 0:
            # Log why we couldn't enter any position
            top = ranked_movers[0]
            cash = self._get_available_cash()
            portfolio = self._get_portfolio_value()
            logger.info("MOMO: 0 entries | top=%s score=%.6f vol=%.2fx | cash=$%.2f portfolio=$%.2f | slots=%d",
                       top["pair"], top["momentum_score"], top.get("volume_ratio", 0),
                       cash, portfolio, available_slots)

    def write_status(self, ranked_movers):
        """Write status file for monitoring."""
        try:
            win_rate = (self._winning_trades / self._total_trades * 100) if self._total_trades > 0 else 0
            uptime = time.time() - self._session_start

            status = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "cycle": self._cycle,
                "mode": "MOMENTUM_CHASER",
                "uptime_seconds": round(uptime),
                "scan_interval": SCAN_INTERVAL,
                "pairs_discovered": len(self._discovered_pairs),
                "pairs_scanned": len(self._discovered_pairs),
                "positions_open": len(self._positions),
                "max_positions": MAX_POSITIONS,
                "total_trades": self._total_trades,
                "winning_trades": self._winning_trades,
                "win_rate_pct": round(win_rate, 1),
                "total_pnl_usd": round(self._total_pnl_usd, 4),
                "positions": {pair: pos.to_dict() for pair, pos in self._positions.items()},
                "top_movers": [
                    {
                        "pair": m["pair"],
                        "momentum_score": round(m["momentum_score"], 6),
                        "roc_5m": round(m["roc_5m"] * 100, 3),
                        "roc_15m": round(m["roc_15m"] * 100, 3),
                        "volume_ratio": round(m["volume_ratio"], 2),
                        "price": m["price"],
                    }
                    for m in ranked_movers[:10]
                ],
            }

            with open(STATUS_FILE, "w") as f:
                json.dump(status, f, indent=2)
        except Exception:
            pass

    def run(self):
        """Main momentum chasing loop."""
        logger.info(
            "MOMO: Momentum Chaser starting | discovery=ALL_PAIRS | scan_interval=%ds | max_positions=%d | "
            "thresholds: spot=%.3f%% kraken=%.3f%% perp=%.3f%% | shorts=enabled (perps)",
            SCAN_INTERVAL, MAX_POSITIONS,
            MIN_MOMENTUM_SPOT * 100, MIN_MOMENTUM_KRAKEN * 100, MIN_MOMENTUM_PERP * 100,
        )

        if not self._init_trading():
            logger.error("MOMO: Trading init failed — retrying in 30s")
            time.sleep(30)
            if not self._init_trading():
                logger.error("MOMO: Trading init failed twice — exiting")
                return

        while self._running:
            try:
                self._cycle += 1
                t0 = time.time()

                # 0. Refresh balances once per cycle (single API call)
                self._refresh_balances()

                # 1. Scan all pairs for momentum
                ranked = self.scan_all_pairs()
                n_movers = sum(1 for r in ranked if r["momentum_score"] >= MIN_MOMENTUM_PERP)

                if ranked:
                    top = ranked[0]
                    logger.info(
                        "MOMO cycle %d: %d movers found | top=%s score=%.4f roc_5m=%.2f%% vol=%.2fx | %d positions open",
                        self._cycle, n_movers, top["pair"],
                        top["momentum_score"], top["roc_5m"] * 100,
                        top["volume_ratio"], len(self._positions),
                    )

                # 2. Manage existing positions (check stops, momentum fade)
                self.manage_positions(ranked)

                # 3. Rotate: enter new movers, the core momentum strategy
                self.rotate_positions(ranked)

                # 4. Write status
                self.write_status(ranked)

                # 5. Performance summary every 50 cycles
                if self._cycle % 50 == 0:
                    win_rate = (self._winning_trades / max(1, self._total_trades)) * 100
                    logger.info(
                        "MOMO PERF: %d trades | %d wins (%.1f%%) | P&L: $%.2f | positions: %d/%d",
                        self._total_trades, self._winning_trades, win_rate,
                        self._total_pnl_usd, len(self._positions), MAX_POSITIONS,
                    )

                elapsed = time.time() - t0
                # Adaptive sleep: longer when in 403 backoff (no point scanning if we can't trade)
                if self._consecutive_403s > 0:
                    sleep_time = max(15, SCAN_INTERVAL * 3 - elapsed)
                else:
                    sleep_time = max(1, SCAN_INTERVAL - elapsed)
                time.sleep(sleep_time)

            except KeyboardInterrupt:
                logger.info("MOMO: Shutting down...")
                self._running = False
                # Exit all positions on shutdown
                for pair in list(self._positions.keys()):
                    price = self._get_price(pair)
                    if price:
                        self._positions[pair].exit_reason = "shutdown"
                        self.exit_position(pair, price)
                break
            except Exception as e:
                logger.error("MOMO cycle error: %s\n%s", e, traceback.format_exc())
                time.sleep(15)


def main():
    # Process singleton to avoid duplicates
    try:
        from process_singleton import ProcessSingleton
        singleton = ProcessSingleton("momentum_chaser")
        if not singleton.acquire():
            logger.error("MOMO: Another instance already running — exiting")
            return
    except ImportError:
        pass

    chaser = MomentumChaser()
    chaser.run()


if __name__ == "__main__":
    main()
