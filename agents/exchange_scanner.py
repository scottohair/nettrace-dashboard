#!/usr/bin/env python3
"""Multi-Exchange Correlation Scanner — monitors global exchanges for trading signals.

Scans PUBLIC data feeds from major global exchanges and commodity markets.
Extracts correlation signals: gold, oil, forex, energy prices that historically
correlate with crypto price movements.

Correlation logic:
  - Gold up       -> BTC often follows (both are "digital/real gold" hedge)
  - Oil spike     -> risk-off sentiment -> crypto down
  - DXY/USD up    -> crypto down (inverse correlation)
  - EUR/USD down  -> flight to USD -> crypto down
  - Energy spike  -> mining cost up -> potential BTC support long-term
  - VIX spike     -> fear -> crypto down short-term

Exchanges monitored (PUBLIC feeds only, no auth required for data):
  1. CME Group     — futures (gold, oil, indices via public delayed data)
  2. ICE           — commodities/energy futures
  3. NYMEX         — energy/metals futures (part of CME)
  4. London SE     — FX/equity data
  5. EEX           — European energy exchange
  6. DGCX          — Dubai Gold & Commodities
  7. APEX          — Asia Pacific Exchange
  8. IB            — Interactive Brokers (multi-asset)
  9. Saxo Bank     — Multi-asset broker
  10. Plus500      — CFD provider

Since most of these require accounts for live data, we use free public
commodity/forex APIs as proxies:
  - open.er-api.com       — free forex rates
  - metals-api.com        — gold/silver prices (free tier)
  - Coinbase public API   — crypto reference prices
  - Yahoo Finance scrape  — commodity indices (backup)

RULES (NEVER VIOLATE):
  - Max $5 per trade
  - $2 daily loss limit (HARDSTOP)
  - LIMIT orders only (maker fee)
  - 70%+ confidence, 2+ confirming signals required
  - This agent DOES NOT trade directly — it emits correlation signals

Usage:
  python exchange_scanner.py              # Run continuous scanner
  python exchange_scanner.py scan         # One-shot scan
  python exchange_scanner.py status       # Show status from DB
"""

import json
import logging
import os
import sqlite3
import sys
import time
import urllib.request
import ssl
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Load .env from agents/ dir
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

AGENTS_DIR = Path(__file__).parent
LOG_FILE = str(AGENTS_DIR / "exchange_scanner.log")
DB_FILE = str(AGENTS_DIR / "exchange_scanner.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ]
)
logger = logging.getLogger("exchange_scanner")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCAN_INTERVAL = 120  # seconds between scans
HTTP_TIMEOUT = 15    # seconds per request

# Crypto trading pairs we feed signals to (via CoinbaseTrader)
CRYPTO_PAIRS = ["BTC-USDC", "ETH-USDC", "SOL-USDC"]

# Risk limits (this agent emits signals, not direct trades)
MAX_TRADE_USD = 5.00
MAX_DAILY_LOSS_USD = 2.00
MIN_CONFIDENCE = 0.70
MIN_CONFIRMING_SIGNALS = 2

# Correlation thresholds — minimum % move to trigger a signal
GOLD_MOVE_THRESHOLD = 0.005      # 0.5% gold move
OIL_MOVE_THRESHOLD = 0.010       # 1.0% oil move
FOREX_MOVE_THRESHOLD = 0.003     # 0.3% DXY/EUR-USD move
ENERGY_MOVE_THRESHOLD = 0.015    # 1.5% energy move
VIX_MOVE_THRESHOLD = 0.05        # 5% VIX move

# Free API endpoints (no auth required)
API_ENDPOINTS = {
    "forex": "https://open.er-api.com/v6/latest/USD",
    "coinbase_btc": "https://api.coinbase.com/v2/prices/BTC-USD/spot",
    "coinbase_eth": "https://api.coinbase.com/v2/prices/ETH-USD/spot",
    "coinbase_sol": "https://api.coinbase.com/v2/prices/SOL-USD/spot",
}

# Exchange data sources — what we try to fetch from each
EXCHANGE_SOURCES = {
    "cme_group": {
        "name": "CME Group",
        "url": "https://www.cmegroup.com",
        "data_type": "futures",
        "assets": ["gold", "oil_wti", "sp500", "nasdaq"],
        "requires_account": True,
        "public_delayed": True,
        "notes": "Delayed quotes available publicly; real-time needs account",
    },
    "ice": {
        "name": "ICE (Intercontinental Exchange)",
        "url": "https://www.ice.com",
        "data_type": "commodities",
        "assets": ["brent_crude", "natural_gas", "cotton", "coffee"],
        "requires_account": True,
        "public_delayed": True,
        "notes": "Delayed data via public pages; live feed needs ICE Connect",
    },
    "nymex": {
        "name": "NYMEX (part of CME)",
        "url": "https://www.cmegroup.com/markets/energy.html",
        "data_type": "energy_metals",
        "assets": ["oil_wti", "natural_gas", "gold", "silver", "platinum"],
        "requires_account": True,
        "public_delayed": True,
        "notes": "Same as CME — NYMEX is a CME division",
    },
    "lse": {
        "name": "London Stock Exchange",
        "url": "https://www.londonstockexchange.com",
        "data_type": "fx_equity",
        "assets": ["gbpusd", "ftse100"],
        "requires_account": True,
        "public_delayed": True,
        "notes": "Delayed data on public pages; real-time needs LSE subscription",
    },
    "eex": {
        "name": "EEX (European Energy Exchange)",
        "url": "https://www.eex.com",
        "data_type": "energy",
        "assets": ["eu_power", "eu_gas", "eu_carbon"],
        "requires_account": True,
        "public_delayed": True,
        "notes": "Some settlement prices public; live needs EEX account",
    },
    "dgcx": {
        "name": "DGCX (Dubai Gold & Commodities)",
        "url": "https://www.dgcx.ae",
        "data_type": "gold_commodities",
        "assets": ["gold", "silver", "inr_futures"],
        "requires_account": True,
        "public_delayed": True,
        "notes": "End-of-day data public; intraday needs DGCX terminal",
    },
    "apex": {
        "name": "Asia Pacific Exchange",
        "url": "https://www.asiapacificexchange.com",
        "data_type": "asian_derivatives",
        "assets": ["palm_oil", "usd_cnh"],
        "requires_account": True,
        "public_delayed": False,
        "notes": "Limited public data; trading needs full account",
    },
    "interactive_brokers": {
        "name": "Interactive Brokers",
        "url": "https://www.interactivebrokers.com",
        "data_type": "multi_asset",
        "assets": ["all"],
        "requires_account": True,
        "public_delayed": False,
        "notes": "Broker — needs funded account. Could use TWS API for data.",
    },
    "saxo_bank": {
        "name": "Saxo Bank",
        "url": "https://www.saxobank.com",
        "data_type": "multi_asset",
        "assets": ["all"],
        "requires_account": True,
        "public_delayed": False,
        "notes": "Broker — needs account. OpenAPI available for registered developers.",
    },
    "plus500": {
        "name": "Plus500",
        "url": "https://www.plus500.com",
        "data_type": "cfd",
        "assets": ["all"],
        "requires_account": True,
        "public_delayed": False,
        "notes": "CFD provider — no public API. Needs funded trading account.",
    },
}

# Correlation rules: how commodity/macro moves map to crypto direction
CORRELATION_RULES = {
    "gold_up": {
        "crypto_direction": "long",
        "confidence_base": 0.55,
        "reasoning": "Gold and BTC both seen as inflation hedges; gold rally often precedes BTC rally",
        "pairs": ["BTC-USDC"],
    },
    "gold_down": {
        "crypto_direction": "short",
        "confidence_base": 0.50,
        "reasoning": "Gold selloff can indicate risk appetite returning to equities, away from crypto",
        "pairs": ["BTC-USDC"],
    },
    "oil_spike": {
        "crypto_direction": "short",
        "confidence_base": 0.55,
        "reasoning": "Oil spike -> inflation fear -> rate hike expectations -> risk-off -> crypto down",
        "pairs": ["BTC-USDC", "ETH-USDC"],
    },
    "oil_crash": {
        "crypto_direction": "long",
        "confidence_base": 0.50,
        "reasoning": "Oil crash -> deflation signal -> potential rate cuts -> risk-on -> crypto up",
        "pairs": ["BTC-USDC", "ETH-USDC"],
    },
    "dxy_up": {
        "crypto_direction": "short",
        "confidence_base": 0.60,
        "reasoning": "Strong USD inversely correlated with BTC; DXY up -> crypto down",
        "pairs": ["BTC-USDC", "ETH-USDC", "SOL-USDC"],
    },
    "dxy_down": {
        "crypto_direction": "long",
        "confidence_base": 0.60,
        "reasoning": "Weak USD -> investors seek alternatives -> BTC/crypto up",
        "pairs": ["BTC-USDC", "ETH-USDC", "SOL-USDC"],
    },
    "energy_spike": {
        "crypto_direction": "short",
        "confidence_base": 0.45,
        "reasoning": "Energy cost spike -> mining cost up, macro drag -> mixed but short-term bearish",
        "pairs": ["BTC-USDC"],
    },
    "vix_spike": {
        "crypto_direction": "short",
        "confidence_base": 0.55,
        "reasoning": "VIX spike -> fear -> de-risk -> sell crypto",
        "pairs": ["BTC-USDC", "ETH-USDC", "SOL-USDC"],
    },
    "vix_crash": {
        "crypto_direction": "long",
        "confidence_base": 0.50,
        "reasoning": "VIX crash -> complacency -> risk-on -> buy crypto",
        "pairs": ["BTC-USDC", "ETH-USDC", "SOL-USDC"],
    },
}


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _fetch_json(url, headers=None, timeout=HTTP_TIMEOUT):
    """Fetch JSON from a URL. Returns parsed dict or None on failure."""
    h = {"User-Agent": "NetTrace-ExchangeScanner/1.0"}
    if headers:
        h.update(headers)
    try:
        req = urllib.request.Request(url, headers=h)
        # Some APIs need relaxed SSL (e.g. self-signed certs on dev)
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        logger.debug("Fetch failed for %s: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class ScannerDB:
    """Persistent storage for price feeds and correlation signals."""

    def __init__(self, db_path=DB_FILE):
        self.db = sqlite3.connect(db_path, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS price_feeds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                asset TEXT NOT NULL,
                price_usd REAL NOT NULL,
                change_pct REAL DEFAULT 0.0,
                metadata TEXT DEFAULT '{}',
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_pf_source ON price_feeds(source);
            CREATE INDEX IF NOT EXISTS idx_pf_asset ON price_feeds(asset);
            CREATE INDEX IF NOT EXISTS idx_pf_time ON price_feeds(fetched_at);

            CREATE TABLE IF NOT EXISTS correlation_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_type TEXT NOT NULL,
                crypto_pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                confidence REAL NOT NULL,
                trigger_asset TEXT NOT NULL,
                trigger_move_pct REAL NOT NULL,
                reasoning TEXT,
                acted_on INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_cs_pair ON correlation_signals(crypto_pair);
            CREATE INDEX IF NOT EXISTS idx_cs_time ON correlation_signals(created_at);
            CREATE INDEX IF NOT EXISTS idx_cs_acted ON correlation_signals(acted_on);

            CREATE TABLE IF NOT EXISTS scan_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sources_checked INTEGER DEFAULT 0,
                prices_fetched INTEGER DEFAULT 0,
                signals_generated INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                scan_duration_ms REAL DEFAULT 0,
                scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

    def store_price(self, source, asset, price_usd, change_pct=0.0, metadata=None):
        """Store a price feed data point."""
        self.db.execute(
            """INSERT INTO price_feeds (source, asset, price_usd, change_pct, metadata)
               VALUES (?, ?, ?, ?, ?)""",
            (source, asset, price_usd, change_pct,
             json.dumps(metadata or {}))
        )
        self.db.commit()

    def get_last_price(self, asset, source=None):
        """Get the most recent price for an asset."""
        if source:
            row = self.db.execute(
                """SELECT * FROM price_feeds WHERE asset = ? AND source = ?
                   ORDER BY fetched_at DESC LIMIT 1""",
                (asset, source)
            ).fetchone()
        else:
            row = self.db.execute(
                """SELECT * FROM price_feeds WHERE asset = ?
                   ORDER BY fetched_at DESC LIMIT 1""",
                (asset,)
            ).fetchone()
        return dict(row) if row else None

    def get_price_history(self, asset, hours=24, limit=500):
        """Get price history for an asset."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        rows = self.db.execute(
            """SELECT * FROM price_feeds WHERE asset = ? AND fetched_at > ?
               ORDER BY fetched_at DESC LIMIT ?""",
            (asset, cutoff, limit)
        ).fetchall()
        return [dict(r) for r in rows]

    def store_signal(self, signal_type, crypto_pair, direction, confidence,
                     trigger_asset, trigger_move_pct, reasoning=None,
                     expires_minutes=30):
        """Store a correlation signal."""
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=expires_minutes)).isoformat()
        self.db.execute(
            """INSERT INTO correlation_signals
               (signal_type, crypto_pair, direction, confidence,
                trigger_asset, trigger_move_pct, reasoning, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (signal_type, crypto_pair, direction, confidence,
             trigger_asset, trigger_move_pct, reasoning, expires_at)
        )
        self.db.commit()

    def get_active_signals(self, min_confidence=0.50):
        """Get active (non-expired, not acted on) signals."""
        now = datetime.now(timezone.utc).isoformat()
        rows = self.db.execute(
            """SELECT * FROM correlation_signals
               WHERE confidence >= ? AND expires_at > ? AND acted_on = 0
               ORDER BY confidence DESC, created_at DESC""",
            (min_confidence, now)
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_acted(self, signal_id):
        """Mark a signal as acted upon."""
        self.db.execute(
            "UPDATE correlation_signals SET acted_on = 1 WHERE id = ?",
            (signal_id,)
        )
        self.db.commit()

    def record_scan(self, sources_checked, prices_fetched, signals_generated,
                    errors, duration_ms):
        """Record a scan history entry."""
        self.db.execute(
            """INSERT INTO scan_history
               (sources_checked, prices_fetched, signals_generated, errors, scan_duration_ms)
               VALUES (?, ?, ?, ?, ?)""",
            (sources_checked, prices_fetched, signals_generated, errors, duration_ms)
        )
        self.db.commit()

    def get_recent_scans(self, limit=10):
        """Get recent scan history."""
        rows = self.db.execute(
            "SELECT * FROM scan_history ORDER BY scanned_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self.db.close()


# ---------------------------------------------------------------------------
# Exchange Scanner — main class
# ---------------------------------------------------------------------------

class ExchangeScanner:
    """Multi-exchange correlation scanner.

    Fetches commodity, forex, and index prices from free public APIs,
    then generates correlation signals for the crypto trading agents.
    """

    def __init__(self):
        self.db = ScannerDB()
        self._price_cache = {}   # asset -> {"price": float, "time": float}
        self._prev_prices = {}   # asset -> float (previous scan's price for change calc)
        self._daily_signals = 0
        self._daily_reset = datetime.now(timezone.utc).date()
        logger.info("ExchangeScanner initialized | DB=%s", DB_FILE)

    # ── Price fetchers (free public APIs) ──

    def fetch_forex_rates(self):
        """Fetch USD-based forex rates from open.er-api.com (free, no auth)."""
        data = _fetch_json(API_ENDPOINTS["forex"])
        if not data or "rates" not in data:
            return {}

        rates = data["rates"]
        results = {}

        # EUR/USD
        eur = rates.get("EUR", 0)
        if eur > 0:
            eurusd = 1.0 / eur
            results["eurusd"] = eurusd
            self._store_and_cache("forex_api", "eurusd", eurusd)

        # GBP/USD
        gbp = rates.get("GBP", 0)
        if gbp > 0:
            gbpusd = 1.0 / gbp
            results["gbpusd"] = gbpusd
            self._store_and_cache("forex_api", "gbpusd", gbpusd)

        # JPY/USD (inverse for DXY proxy)
        jpy = rates.get("JPY", 0)
        if jpy > 0:
            usdjpy = jpy  # JPY per 1 USD
            results["usdjpy"] = usdjpy
            self._store_and_cache("forex_api", "usdjpy", usdjpy)

        # CHF/USD
        chf = rates.get("CHF", 0)
        if chf > 0:
            usdchf = chf
            results["usdchf"] = usdchf
            self._store_and_cache("forex_api", "usdchf", usdchf)

        # Compute a simple DXY proxy from major pairs
        # Real DXY = USD vs basket (EUR 57.6%, JPY 13.6%, GBP 11.9%, CAD 9.1%, SEK 4.2%, CHF 3.6%)
        if eur > 0 and jpy > 0 and gbp > 0:
            cad = rates.get("CAD", 1.35)
            sek = rates.get("SEK", 10.5)
            # Simplified DXY proxy (higher = stronger USD)
            dxy_proxy = (eur ** 0.576) * (jpy ** 0.136) * (gbp ** 0.119) * (cad ** 0.091) * (sek ** 0.042) * (chf ** 0.036)
            # Normalize to approximately 100
            dxy_proxy = dxy_proxy * 50.0  # rough scaling factor
            results["dxy_proxy"] = round(dxy_proxy, 2)
            self._store_and_cache("forex_api", "dxy_proxy", dxy_proxy)

        logger.info("Forex: EUR/USD=%.4f GBP/USD=%.4f USD/JPY=%.2f DXY~%.2f",
                     results.get("eurusd", 0), results.get("gbpusd", 0),
                     results.get("usdjpy", 0), results.get("dxy_proxy", 0))
        return results

    def fetch_gold_price(self):
        """Fetch gold price from multiple free sources.

        Tries:
        1. metals-api.com free tier (may need API key)
        2. Coinbase gold-backed tokens as proxy
        3. Public commodity price pages
        """
        gold_price = None

        # Method 1: metals-api.com (free tier — 50 requests/month)
        metals_key = os.environ.get("METALS_API_KEY", "")
        if metals_key:
            data = _fetch_json(
                f"https://metals-api.com/api/latest?access_key={metals_key}&base=USD&symbols=XAU",
                timeout=10
            )
            if data and data.get("success") and "rates" in data:
                xau = data["rates"].get("XAU", 0)
                if xau > 0:
                    gold_price = 1.0 / xau  # XAU rate is ounces per USD, invert
                    logger.debug("Gold from metals-api: $%.2f/oz", gold_price)

        # Method 2: try goldapi.io free tier
        if gold_price is None:
            gold_key = os.environ.get("GOLD_API_KEY", "")
            if gold_key:
                data = _fetch_json(
                    "https://www.goldapi.io/api/XAU/USD",
                    headers={"x-access-token": gold_key},
                    timeout=10
                )
                if data and "price" in data:
                    gold_price = data["price"]
                    logger.debug("Gold from goldapi.io: $%.2f/oz", gold_price)

        # Method 3: try Coinbase PAX Gold (PAXG) as proxy (1 PAXG ~ 1 oz gold)
        if gold_price is None:
            data = _fetch_json(
                "https://api.coinbase.com/v2/prices/PAXG-USD/spot",
                timeout=5
            )
            if data and "data" in data:
                try:
                    gold_price = float(data["data"]["amount"])
                    logger.debug("Gold proxy (PAXG): $%.2f/oz", gold_price)
                except (ValueError, KeyError):
                    pass

        if gold_price and gold_price > 500:  # sanity check
            self._store_and_cache("gold_api", "gold", gold_price)
            return gold_price

        logger.warning("Could not fetch gold price from any source")
        return None

    def fetch_oil_price(self):
        """Fetch crude oil (WTI) price from free sources.

        Tries:
        1. commodities-api.com free tier
        2. Public data from other free endpoints
        """
        oil_price = None

        # Method 1: commodities-api.com
        commodities_key = os.environ.get("COMMODITIES_API_KEY", "")
        if commodities_key:
            data = _fetch_json(
                f"https://commodities-api.com/api/latest?access_key={commodities_key}&base=USD&symbols=BRENTOIL",
                timeout=10
            )
            if data and data.get("data", {}).get("success"):
                rates = data["data"].get("rates", {})
                brent = rates.get("BRENTOIL", 0)
                if brent > 0:
                    oil_price = 1.0 / brent  # rate is barrels per USD
                    logger.debug("Oil (Brent) from commodities-api: $%.2f/bbl", oil_price)

        # Method 2: try oil-price free data
        if oil_price is None:
            # Use a forex API that includes commodities
            data = _fetch_json(
                "https://open.er-api.com/v6/latest/USD",
                timeout=10
            )
            # open.er-api doesn't have commodities, but we can note the attempt
            logger.debug("Oil price: no free commodity API available this cycle")

        if oil_price and oil_price > 10:  # sanity check
            self._store_and_cache("oil_api", "oil_wti", oil_price)
            return oil_price

        # Use last known price from DB as fallback
        last = self.db.get_last_price("oil_wti")
        if last:
            logger.debug("Oil: using last known price $%.2f", last["price_usd"])
            return last["price_usd"]

        return None

    def fetch_crypto_prices(self):
        """Fetch current crypto prices from Coinbase public API."""
        results = {}
        for label, url in [
            ("BTC", API_ENDPOINTS["coinbase_btc"]),
            ("ETH", API_ENDPOINTS["coinbase_eth"]),
            ("SOL", API_ENDPOINTS["coinbase_sol"]),
        ]:
            data = _fetch_json(url, timeout=5)
            if data and "data" in data:
                try:
                    price = float(data["data"]["amount"])
                    results[label] = price
                    self._store_and_cache("coinbase", label.lower(), price)
                except (ValueError, KeyError):
                    pass

        if results:
            logger.info("Crypto: BTC=$%s ETH=$%s SOL=$%s",
                         f"{results.get('BTC', 0):,.2f}",
                         f"{results.get('ETH', 0):,.2f}",
                         f"{results.get('SOL', 0):,.2f}")
        return results

    def fetch_silver_price(self):
        """Fetch silver price (PAXG-correlated or metals API)."""
        # Try Coinbase for a silver proxy — not available on CB, skip
        # Try metals API if we have a key
        metals_key = os.environ.get("METALS_API_KEY", "")
        if metals_key:
            data = _fetch_json(
                f"https://metals-api.com/api/latest?access_key={metals_key}&base=USD&symbols=XAG",
                timeout=10
            )
            if data and data.get("success") and "rates" in data:
                xag = data["rates"].get("XAG", 0)
                if xag > 0:
                    silver = 1.0 / xag
                    self._store_and_cache("metals_api", "silver", silver)
                    return silver
        return None

    # ── Cache & change tracking ──

    def _store_and_cache(self, source, asset, price):
        """Store price in DB and update cache, compute change from previous."""
        now = time.time()
        prev = self._prev_prices.get(asset)
        change_pct = 0.0
        if prev and prev > 0:
            change_pct = (price - prev) / prev

        self.db.store_price(source, asset, price, change_pct,
                           {"cached_at": now})
        self._price_cache[asset] = {"price": price, "time": now}
        # Update previous for next cycle
        self._prev_prices[asset] = price

    def _get_cached_price(self, asset):
        """Get cached price if fresh (within 5 min)."""
        cached = self._price_cache.get(asset)
        if cached and time.time() - cached["time"] < 300:
            return cached["price"]
        return None

    def _get_change_pct(self, asset):
        """Get the latest change percentage for an asset."""
        last = self.db.get_last_price(asset)
        if last:
            return last.get("change_pct", 0.0)
        return 0.0

    # ── Signal generation ──

    def generate_correlation_signals(self):
        """Analyze price feeds and generate correlation signals for crypto trading.

        Returns list of signal dicts.
        """
        signals = []

        # Reset daily counter at midnight UTC
        today = datetime.now(timezone.utc).date()
        if today != self._daily_reset:
            self._daily_signals = 0
            self._daily_reset = today

        # --- Gold correlation ---
        gold_change = self._get_change_pct("gold")
        if abs(gold_change) >= GOLD_MOVE_THRESHOLD:
            if gold_change > 0:
                rule = CORRELATION_RULES["gold_up"]
                signal_type = "gold_up"
            else:
                rule = CORRELATION_RULES["gold_down"]
                signal_type = "gold_down"

            confidence = min(0.85, rule["confidence_base"] + abs(gold_change) * 5.0)

            for pair in rule["pairs"]:
                sig = {
                    "signal_type": signal_type,
                    "crypto_pair": pair,
                    "direction": rule["crypto_direction"],
                    "confidence": round(confidence, 4),
                    "trigger_asset": "gold",
                    "trigger_move_pct": round(gold_change * 100, 4),
                    "reasoning": rule["reasoning"],
                }
                signals.append(sig)
                logger.info("SIGNAL: %s -> %s %s conf=%.2f (gold %.2f%%)",
                            signal_type, rule["crypto_direction"], pair,
                            confidence, gold_change * 100)

        # --- Oil correlation ---
        oil_change = self._get_change_pct("oil_wti")
        if abs(oil_change) >= OIL_MOVE_THRESHOLD:
            if oil_change > OIL_MOVE_THRESHOLD:
                rule = CORRELATION_RULES["oil_spike"]
                signal_type = "oil_spike"
            else:
                rule = CORRELATION_RULES["oil_crash"]
                signal_type = "oil_crash"

            confidence = min(0.80, rule["confidence_base"] + abs(oil_change) * 3.0)

            for pair in rule["pairs"]:
                sig = {
                    "signal_type": signal_type,
                    "crypto_pair": pair,
                    "direction": rule["crypto_direction"],
                    "confidence": round(confidence, 4),
                    "trigger_asset": "oil_wti",
                    "trigger_move_pct": round(oil_change * 100, 4),
                    "reasoning": rule["reasoning"],
                }
                signals.append(sig)
                logger.info("SIGNAL: %s -> %s %s conf=%.2f (oil %.2f%%)",
                            signal_type, rule["crypto_direction"], pair,
                            confidence, oil_change * 100)

        # --- DXY/USD strength correlation ---
        dxy_change = self._get_change_pct("dxy_proxy")
        if abs(dxy_change) >= FOREX_MOVE_THRESHOLD:
            if dxy_change > 0:
                rule = CORRELATION_RULES["dxy_up"]
                signal_type = "dxy_up"
            else:
                rule = CORRELATION_RULES["dxy_down"]
                signal_type = "dxy_down"

            confidence = min(0.85, rule["confidence_base"] + abs(dxy_change) * 8.0)

            for pair in rule["pairs"]:
                sig = {
                    "signal_type": signal_type,
                    "crypto_pair": pair,
                    "direction": rule["crypto_direction"],
                    "confidence": round(confidence, 4),
                    "trigger_asset": "dxy_proxy",
                    "trigger_move_pct": round(dxy_change * 100, 4),
                    "reasoning": rule["reasoning"],
                }
                signals.append(sig)
                logger.info("SIGNAL: %s -> %s %s conf=%.2f (DXY %.2f%%)",
                            signal_type, rule["crypto_direction"], pair,
                            confidence, dxy_change * 100)

        # --- EUR/USD as additional forex signal ---
        eurusd_change = self._get_change_pct("eurusd")
        if abs(eurusd_change) >= FOREX_MOVE_THRESHOLD:
            # EUR/USD down = USD strong = crypto down (same as dxy_up)
            if eurusd_change < -FOREX_MOVE_THRESHOLD:
                rule = CORRELATION_RULES["dxy_up"]
                signal_type = "eurusd_drop"
            else:
                rule = CORRELATION_RULES["dxy_down"]
                signal_type = "eurusd_rally"

            confidence = min(0.80, rule["confidence_base"] + abs(eurusd_change) * 6.0)

            for pair in rule["pairs"]:
                sig = {
                    "signal_type": signal_type,
                    "crypto_pair": pair,
                    "direction": rule["crypto_direction"],
                    "confidence": round(confidence, 4),
                    "trigger_asset": "eurusd",
                    "trigger_move_pct": round(eurusd_change * 100, 4),
                    "reasoning": f"EUR/USD move: {rule['reasoning']}",
                }
                signals.append(sig)

        # Store all signals in DB
        for sig in signals:
            self.db.store_signal(
                signal_type=sig["signal_type"],
                crypto_pair=sig["crypto_pair"],
                direction=sig["direction"],
                confidence=sig["confidence"],
                trigger_asset=sig["trigger_asset"],
                trigger_move_pct=sig["trigger_move_pct"],
                reasoning=sig["reasoning"],
                expires_minutes=30,
            )
            self._daily_signals += 1

        return signals

    # ── Full scan cycle ──

    def run_scan(self):
        """Execute one full scan cycle across all data sources.

        Returns dict with scan results.
        """
        t0 = time.time()
        sources_checked = 0
        prices_fetched = 0
        errors = 0

        # 1. Forex rates
        try:
            sources_checked += 1
            forex = self.fetch_forex_rates()
            prices_fetched += len(forex)
        except Exception as e:
            errors += 1
            logger.error("Forex fetch error: %s", e)

        # 2. Gold price
        try:
            sources_checked += 1
            gold = self.fetch_gold_price()
            if gold:
                prices_fetched += 1
        except Exception as e:
            errors += 1
            logger.error("Gold fetch error: %s", e)

        # 3. Oil price
        try:
            sources_checked += 1
            oil = self.fetch_oil_price()
            if oil:
                prices_fetched += 1
        except Exception as e:
            errors += 1
            logger.error("Oil fetch error: %s", e)

        # 4. Silver price
        try:
            sources_checked += 1
            silver = self.fetch_silver_price()
            if silver:
                prices_fetched += 1
        except Exception as e:
            errors += 1
            logger.error("Silver fetch error: %s", e)

        # 5. Crypto reference prices
        try:
            sources_checked += 1
            crypto = self.fetch_crypto_prices()
            prices_fetched += len(crypto)
        except Exception as e:
            errors += 1
            logger.error("Crypto fetch error: %s", e)

        # 6. Generate correlation signals
        signals = self.generate_correlation_signals()

        duration_ms = (time.time() - t0) * 1000

        # Record scan history
        self.db.record_scan(sources_checked, prices_fetched,
                           len(signals), errors, duration_ms)

        logger.info("Scan complete: %d sources | %d prices | %d signals | %d errors | %.0fms",
                     sources_checked, prices_fetched, len(signals), errors, duration_ms)

        return {
            "sources_checked": sources_checked,
            "prices_fetched": prices_fetched,
            "signals": signals,
            "errors": errors,
            "duration_ms": round(duration_ms, 1),
        }

    def get_active_signals(self):
        """Get all active correlation signals for other agents to consume."""
        return self.db.get_active_signals(min_confidence=MIN_CONFIDENCE)

    def get_status(self):
        """Get scanner status summary."""
        active_signals = self.db.get_active_signals()
        recent_scans = self.db.get_recent_scans(5)

        # Latest prices
        latest_prices = {}
        for asset in ["gold", "oil_wti", "eurusd", "gbpusd", "usdjpy",
                      "dxy_proxy", "btc", "eth", "sol", "silver"]:
            last = self.db.get_last_price(asset)
            if last:
                latest_prices[asset] = {
                    "price": last["price_usd"],
                    "change_pct": round(last.get("change_pct", 0) * 100, 4),
                    "fetched_at": last["fetched_at"],
                }

        return {
            "active_signals": len(active_signals),
            "signals": active_signals[:10],
            "latest_prices": latest_prices,
            "recent_scans": recent_scans,
            "daily_signals": self._daily_signals,
            "exchanges_configured": len(EXCHANGE_SOURCES),
        }

    def close(self):
        self.db.close()


# ---------------------------------------------------------------------------
# CLI modes
# ---------------------------------------------------------------------------

def run_continuous(scanner):
    """Run the scanner in a continuous loop."""
    logger.info("=" * 70)
    logger.info("Exchange Scanner starting — continuous mode")
    logger.info("Scan interval: %ds | Crypto pairs: %s", SCAN_INTERVAL, CRYPTO_PAIRS)
    logger.info("Exchanges configured: %d", len(EXCHANGE_SOURCES))
    logger.info("=" * 70)

    cycle = 0
    while True:
        try:
            cycle += 1
            logger.info("--- Scan cycle %d ---", cycle)
            result = scanner.run_scan()

            if result["signals"]:
                logger.info("Active signals after scan:")
                for sig in result["signals"][:5]:
                    logger.info("  %s -> %s %s conf=%.2f (%s %.2f%%)",
                                sig["signal_type"], sig["direction"],
                                sig["crypto_pair"], sig["confidence"],
                                sig["trigger_asset"], sig["trigger_move_pct"])

            # Status log every 10 cycles (~20 min)
            if cycle % 10 == 0:
                status = scanner.get_status()
                logger.info("STATUS: %d active signals | %d prices tracked | %d daily signals",
                            status["active_signals"],
                            len(status["latest_prices"]),
                            status["daily_signals"])

            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Shutting down exchange scanner (cycle=%d)", cycle)
            break
        except Exception as e:
            logger.error("Scan loop error: %s", e, exc_info=True)
            time.sleep(30)


def run_oneshot(scanner):
    """Run a single scan and print results."""
    print("=" * 70)
    print("  EXCHANGE SCANNER — One-Shot Scan")
    print("=" * 70)

    result = scanner.run_scan()

    print(f"\n  Sources checked: {result['sources_checked']}")
    print(f"  Prices fetched:  {result['prices_fetched']}")
    print(f"  Signals:         {len(result['signals'])}")
    print(f"  Errors:          {result['errors']}")
    print(f"  Duration:        {result['duration_ms']:.0f}ms")

    if result["signals"]:
        print(f"\n  Correlation Signals:")
        for sig in result["signals"]:
            print(f"    {sig['signal_type']:<16} -> {sig['direction']:5s} "
                  f"{sig['crypto_pair']:<10} conf={sig['confidence']:.2f} "
                  f"({sig['trigger_asset']} {sig['trigger_move_pct']:+.2f}%)")

    # Print latest prices
    status = scanner.get_status()
    if status["latest_prices"]:
        print(f"\n  Latest Prices:")
        for asset, info in sorted(status["latest_prices"].items()):
            print(f"    {asset:<12} ${info['price']:>12,.4f}  "
                  f"chg={info['change_pct']:+.4f}%  "
                  f"({info['fetched_at']})")

    # Print exchange account requirements
    print(f"\n  Exchange Data Access:")
    for key, ex in sorted(EXCHANGE_SOURCES.items()):
        acct = "ACCOUNT REQUIRED" if ex["requires_account"] else "FREE"
        delayed = " (delayed public)" if ex.get("public_delayed") else ""
        print(f"    {ex['name']:<35} {acct}{delayed}")

    print(f"\n{'=' * 70}")


def show_status(scanner):
    """Show scanner status from DB."""
    status = scanner.get_status()

    print(f"\n{'=' * 70}")
    print(f"  EXCHANGE SCANNER STATUS")
    print(f"{'=' * 70}")

    print(f"\n  Active signals:       {status['active_signals']}")
    print(f"  Daily signals:        {status['daily_signals']}")
    print(f"  Exchanges configured: {status['exchanges_configured']}")

    if status["latest_prices"]:
        print(f"\n  Latest Prices:")
        for asset, info in sorted(status["latest_prices"].items()):
            print(f"    {asset:<12} ${info['price']:>12,.4f}  "
                  f"chg={info['change_pct']:+.4f}%")

    if status["signals"]:
        print(f"\n  Active Signals:")
        for sig in status["signals"][:10]:
            print(f"    [{sig['created_at']}] {sig['signal_type']:<16} "
                  f"{sig['direction']:5s} {sig['crypto_pair']:<10} "
                  f"conf={sig['confidence']:.2f}")

    if status["recent_scans"]:
        print(f"\n  Recent Scans:")
        for scan in status["recent_scans"][:5]:
            print(f"    [{scan['scanned_at']}] sources={scan['sources_checked']} "
                  f"prices={scan['prices_fetched']} signals={scan['signals_generated']} "
                  f"errors={scan['errors']} {scan['scan_duration_ms']:.0f}ms")

    print(f"\n{'=' * 70}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    scanner = ExchangeScanner()

    try:
        if len(sys.argv) > 1:
            cmd = sys.argv[1].lower()
            if cmd == "scan":
                run_oneshot(scanner)
            elif cmd == "status":
                show_status(scanner)
            elif cmd == "run":
                run_continuous(scanner)
            else:
                print("Usage: exchange_scanner.py [run|scan|status]")
                print("  run    — continuous scanning (default)")
                print("  scan   — one-shot scan and print results")
                print("  status — show status from database")
        else:
            # Default: continuous mode
            run_continuous(scanner)
    finally:
        scanner.close()


if __name__ == "__main__":
    main()
