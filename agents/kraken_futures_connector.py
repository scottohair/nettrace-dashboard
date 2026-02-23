#!/usr/bin/env python3
"""Kraken Futures/Derivatives connector -- perpetual futures and dated contracts.

Follows CoinbaseDerivativesConnector pattern with 4-layer safety gate:
  Layer 1: Environment gate (KRAKEN_FUTURES_ENABLED must be "1")
  Layer 2: Margin health check (margin ratio > MIN_MARGIN_HEALTH_RATIO)
  Layer 3: Liquidation distance check (> MIN_LIQUIDATION_BUFFER_PCT)
  Layer 4: Leverage cap (never exceed MAX_LEVERAGE)

Kraken Futures API:
  Base URL: https://futures.kraken.com/derivatives/api/v3
  Auth: Separate API keys (KRAKEN_FUTURES_API_KEY, KRAKEN_FUTURES_PRIVATE_KEY)
  Products: PF_XBTUSD (BTC perp), PF_ETHUSD (ETH perp), etc.

All trades gated through GoalValidator (70% confidence, 2+ signals).
BE A MAKER: use post-only limit orders (Kraken Futures maker fee: 0.02%).
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("kraken_futures")

# Load .env if present
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"'))

# Import GoalValidator
try:
    from agent_goals import GoalValidator
except ImportError:
    try:
        from agents.agent_goals import GoalValidator
    except ImportError:
        GoalValidator = None

# ---------------------------------------------------------------------------
# Configuration (env-driven, no hardcoded values)
# ---------------------------------------------------------------------------

# Kraken Futures API
FUTURES_API_URL = os.environ.get(
    "KRAKEN_FUTURES_API_URL",
    "https://futures.kraken.com/derivatives/api/v3",
)
FUTURES_API_KEY = os.environ.get("KRAKEN_FUTURES_API_KEY", "")
FUTURES_PRIVATE_KEY = os.environ.get("KRAKEN_FUTURES_PRIVATE_KEY", "")

# 4-layer safety gates
FUTURES_ENABLED = os.environ.get("KRAKEN_FUTURES_ENABLED", "0") == "1"
MAX_LEVERAGE = float(os.environ.get("KRAKEN_FUTURES_MAX_LEVERAGE", "3.0"))
MIN_MARGIN_HEALTH_RATIO = float(os.environ.get("KRAKEN_FUTURES_MIN_MARGIN_HEALTH", "2.0"))
MIN_LIQUIDATION_BUFFER_PCT = float(os.environ.get("KRAKEN_FUTURES_MIN_LIQ_BUFFER", "50.0"))
MAX_POSITIONS = int(os.environ.get("KRAKEN_FUTURES_MAX_POSITIONS", "5"))

# Fee schedule (Kraken Futures)
FUTURES_MAKER_FEE = float(os.environ.get("KRAKEN_FUTURES_MAKER_FEE", "0.0002"))  # 0.02%
FUTURES_TAKER_FEE = float(os.environ.get("KRAKEN_FUTURES_TAKER_FEE", "0.0005"))  # 0.05%

# Cache TTLs
PRODUCT_CACHE_TTL = int(os.environ.get("KRAKEN_FUTURES_PRODUCT_CACHE_TTL", "300"))
FUNDING_CACHE_TTL = int(os.environ.get("KRAKEN_FUTURES_FUNDING_CACHE_TTL", "30"))

# ---------------------------------------------------------------------------
# Static perpetual product mapping (fallback when API unavailable)
# ---------------------------------------------------------------------------

PERP_PRODUCTS = {
    "BTC": "PF_XBTUSD",
    "ETH": "PF_ETHUSD",
    "SOL": "PF_SOLUSD",
    "XRP": "PF_XRPUSD",
    "LTC": "PF_LTCUSD",
    "LINK": "PF_LINKUSD",
    "AVAX": "PF_AVAXUSD",
    "DOT": "PF_DOTUSD",
    "ADA": "PF_ADAUSD",
    "DOGE": "PF_XDGUSD",
}

# Reverse map: product_id -> base currency
PERP_TO_BASE = {}
for _base, _pid in PERP_PRODUCTS.items():
    PERP_TO_BASE[_pid] = _base

# ---------------------------------------------------------------------------
# Trade database
# ---------------------------------------------------------------------------

FUTURES_TRADE_DB = Path(__file__).parent / "kraken_futures_trades.db"


def _init_futures_trade_db():
    """Initialize Kraken Futures trade tracking database."""
    db = sqlite3.connect(str(FUTURES_TRADE_DB))
    db.execute("""
        CREATE TABLE IF NOT EXISTS kraken_futures_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT NOT NULL,
            side TEXT NOT NULL,
            size REAL NOT NULL,
            price REAL,
            leverage REAL DEFAULT 1.0,
            order_id TEXT,
            status TEXT DEFAULT 'pending',
            confidence REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.commit()
    return db


def _record_futures_trade(product_id, side, size, price, result, confidence, leverage):
    """Record a futures trade in the local database."""
    order_id = None
    status = "error"

    if isinstance(result, dict):
        if result.get("sendStatus"):
            send_status = result["sendStatus"]
            order_id = send_status.get("order_id") or send_status.get("orderId")
            status = send_status.get("status", "submitted")
        elif result.get("result") == "success":
            order_id = result.get("order_id") or result.get("orderId")
            status = "submitted"
        elif not result.get("error"):
            status = "submitted"

    try:
        db = _init_futures_trade_db()
        db.execute(
            """INSERT INTO kraken_futures_trades
               (product_id, side, size, price, leverage, order_id, status, confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (product_id, side.lower(), size, price, leverage, order_id, status, confidence),
        )
        db.commit()
        db.close()
    except Exception as e:
        logger.error("Failed to record futures trade in DB: %s", e)


# ---------------------------------------------------------------------------
# Kraken Futures API Authentication
# ---------------------------------------------------------------------------

def _futures_sign(endpoint, postdata, nonce):
    """Sign Kraken Futures API request.

    Kraken Futures auth:
      Authent = HMAC-SHA512(base64_decode(private_key),
                            SHA256(postdata + nonce + endpoint))
    """
    sha256_hash = hashlib.sha256()
    sha256_hash.update((postdata + nonce + endpoint).encode("utf-8"))

    hmac_digest = hmac.new(
        base64.b64decode(FUTURES_PRIVATE_KEY),
        sha256_hash.digest(),
        hashlib.sha512,
    ).digest()

    return base64.b64encode(hmac_digest).decode()


def _futures_request(method, endpoint, data=None):
    """Make authenticated request to Kraken Futures API.

    Args:
        method: "GET" or "POST"
        endpoint: API path (e.g., "/openpositions")
        data: POST body dict

    Returns:
        dict: JSON response from the API, or {"error": ...} on failure
    """
    if not FUTURES_API_KEY or not FUTURES_PRIVATE_KEY:
        return {"error": "Kraken Futures API keys not configured"}

    url = f"{FUTURES_API_URL}{endpoint}"
    nonce = str(int(time.time() * 1000))

    postdata = ""
    if data:
        postdata = urllib.parse.urlencode(data)

    auth_header = _futures_sign(endpoint, postdata, nonce)

    headers = {
        "APIKey": FUTURES_API_KEY,
        "Authent": auth_header,
        "Nonce": nonce,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    try:
        if method == "GET" and data:
            url += "?" + postdata
            req = urllib.request.Request(url, headers=headers)
        elif method == "GET":
            req = urllib.request.Request(url, headers=headers)
        else:
            body = postdata.encode() if postdata else None
            req = urllib.request.Request(url, data=body, headers=headers, method=method)

        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.error("Kraken Futures request failed: %s %s: %s", method, endpoint, e)
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _safe_float(val, default=0.0):
    """Safely convert to float, returning default on failure."""
    try:
        v = float(val)
        return v if v == v else default  # NaN check
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# KrakenFuturesConnector
# ---------------------------------------------------------------------------

class KrakenFuturesConnector:
    """Kraken Futures connector with 4-layer safety gate.

    All perpetual and futures operations go through this class. It handles
    Kraken Futures API auth, position management, margin monitoring, and
    gated order placement.

    Safety layers:
      Layer 1: KRAKEN_FUTURES_ENABLED env gate
      Layer 2: Margin health ratio check
      Layer 3: Liquidation distance check
      Layer 4: Leverage cap
    """

    def __init__(self):
        """Initialize with caches."""
        self._product_cache = None
        self._product_cache_ts = 0
        self._funding_cache = {}  # product_id -> (data, ts)
        self._perp_map = None
        self._perp_map_ts = 0

    @property
    def enabled(self):
        """Whether Kraken Futures trading is enabled via env vars."""
        return FUTURES_ENABLED and bool(FUTURES_API_KEY) and bool(FUTURES_PRIVATE_KEY)

    # ---- Product Discovery ------------------------------------------------

    @staticmethod
    def list_futures_products():
        """List all available futures products.

        Returns: list of product dicts with symbol, type, etc.
        """
        result = _futures_request("GET", "/instruments")
        if result.get("instruments"):
            return result["instruments"]
        return []

    def build_perp_mapping(self):
        """Build base_currency -> perp product_id mapping from live data.

        Returns: dict like {"BTC": "PF_XBTUSD", "ETH": "PF_ETHUSD"}
        Falls back to static PERP_PRODUCTS if API unavailable.
        """
        now = time.time()
        if self._perp_map is not None and (now - self._perp_map_ts) < PRODUCT_CACHE_TTL:
            return self._perp_map

        products = self.list_futures_products()
        mapping = {}
        for p in products:
            if not isinstance(p, dict):
                continue
            sym = str(p.get("symbol", "")).upper()
            p_type = str(p.get("type", ""))
            # Perpetual futures on Kraken start with PF_
            if sym.startswith("PF_") and p_type in (
                "futures_inverse", "futures_vanilla", "",
            ):
                # Extract base currency: PF_XBTUSD -> XBT -> BTC
                base_raw = sym[3:].replace("USD", "")
                if base_raw == "XBT":
                    base = "BTC"
                elif base_raw == "XDG":
                    base = "DOGE"
                else:
                    base = base_raw
                mapping[base] = sym

        self._perp_map = mapping or PERP_PRODUCTS.copy()
        self._perp_map_ts = now
        if self._perp_map:
            logger.info("Kraken Futures perp mapping: %d bases -- %s",
                        len(self._perp_map), self._perp_map)
        return self._perp_map

    @staticmethod
    def perp_for_spot_pair(pair):
        """Map spot pair to perpetual product.

        Args:
            pair: e.g., "BTC-USD" or "ETH-USDC"

        Returns:
            str product_id (e.g., "PF_XBTUSD") or None
        """
        if not pair:
            return None
        try:
            base = str(pair).split("-")[0].upper()
        except Exception:
            return None
        return PERP_PRODUCTS.get(base)

    # ---- Position Management -----------------------------------------------

    @staticmethod
    def get_positions():
        """Get all open futures positions.

        Returns: list of position dicts from the API
        """
        result = _futures_request("GET", "/openpositions")
        if result.get("openPositions"):
            return result["openPositions"]
        return []

    @staticmethod
    def get_position(product_id):
        """Get specific position for a product.

        Args:
            product_id: e.g., "PF_XBTUSD"

        Returns: position dict or None
        """
        positions = KrakenFuturesConnector.get_positions()
        pid = str(product_id).upper()
        for p in positions:
            if str(p.get("symbol", "")).upper() == pid:
                return p
        return None

    @staticmethod
    def close_position(product_id):
        """Close an open position (market order, reduce-only).

        Closing is NEVER blocked by trading gates -- always allowed
        to reduce risk.

        Args:
            product_id: e.g., "PF_XBTUSD"

        Returns: order result dict or error
        """
        position = KrakenFuturesConnector.get_position(product_id)
        if not position:
            return {"error": f"No open position for {product_id}"}

        side = "sell" if str(position.get("side", "")).lower() == "long" else "buy"
        size = abs(_safe_float(position.get("size", 0)))

        if size <= 0:
            return {"error": f"Position size is zero for {product_id}"}

        logger.info("CLOSE POSITION: %s %s %.6f (reduce-only market)", side, product_id, size)

        return _futures_request("POST", "/sendorder", {
            "orderType": "mkt",
            "symbol": product_id,
            "side": side,
            "size": size,
            "reduceOnly": "true",
        })

    # ---- Margin & Risk ----------------------------------------------------

    @staticmethod
    def get_portfolio_summary():
        """Get futures account portfolio (margin, equity, PnL).

        Returns: dict with portfolio_value, available_margin, initial_margin,
                 maintenance_margin, unrealized_pnl, currency
                 or {"error": ...} on failure
        """
        result = _futures_request("GET", "/accounts")
        if result.get("error"):
            return {"error": result["error"]}

        accounts = result.get("accounts")
        if not accounts or not isinstance(accounts, dict):
            return {"error": "No accounts data in response"}

        # Kraken returns accounts keyed by collateral currency
        # Prefer multiCollateralMargin, then first available
        for currency, account in accounts.items():
            if not isinstance(account, dict):
                continue
            if account.get("type") == "multiCollateralMargin":
                return {
                    "portfolio_value": _safe_float(account.get("pv", 0)),
                    "available_margin": _safe_float(account.get("am", 0)),
                    "initial_margin": _safe_float(account.get("im", 0)),
                    "maintenance_margin": _safe_float(account.get("mm", 0)),
                    "unrealized_pnl": _safe_float(account.get("pnl", 0)),
                    "currency": currency,
                }

        # Fallback: first account
        for currency, account in accounts.items():
            if not isinstance(account, dict):
                continue
            return {
                "portfolio_value": _safe_float(account.get("pv", 0)),
                "available_margin": _safe_float(account.get("am", 0)),
                "initial_margin": _safe_float(account.get("im", 0)),
                "maintenance_margin": _safe_float(account.get("mm", 0)),
                "unrealized_pnl": _safe_float(account.get("pnl", 0)),
                "currency": currency,
            }

        return {"error": "Failed to get portfolio summary"}

    @staticmethod
    def margin_health():
        """Check margin health ratio.

        Returns: dict with ratio, healthy, can_open_new, portfolio_value,
                 maintenance_margin
        """
        portfolio = KrakenFuturesConnector.get_portfolio_summary()
        if portfolio.get("error"):
            return {
                "ratio": 0,
                "healthy": False,
                "can_open_new": False,
                "error": portfolio["error"],
            }

        pv = portfolio.get("portfolio_value", 0)
        mm = portfolio.get("maintenance_margin", 0)

        if mm <= 0:
            ratio = 999.0  # No margin used
        else:
            ratio = pv / mm

        positions = KrakenFuturesConnector.get_positions()
        num_positions = len(positions)

        healthy = ratio >= MIN_MARGIN_HEALTH_RATIO
        # Extra buffer for new positions (1.5x the minimum ratio)
        can_open_new = (
            ratio >= MIN_MARGIN_HEALTH_RATIO * 1.5
            and num_positions < MAX_POSITIONS
        )

        # If no margin used and we have portfolio value, allow opening
        if mm <= 0 and pv > 0:
            healthy = True
            can_open_new = num_positions < MAX_POSITIONS

        return {
            "ratio": round(ratio, 4) if ratio != 999.0 else 999.0,
            "healthy": healthy,
            "can_open_new": can_open_new,
            "portfolio_value": pv,
            "maintenance_margin": mm,
            "open_positions": num_positions,
        }

    @staticmethod
    def liquidation_distance(product_id):
        """Calculate distance to liquidation for a position.

        Args:
            product_id: e.g., "PF_XBTUSD"

        Returns: dict with distance_pct, safe, liq_price, mark_price, entry_price
        """
        position = KrakenFuturesConnector.get_position(product_id)
        if not position:
            return {"distance_pct": 100, "safe": True, "liq_price": 0}

        entry_price = _safe_float(position.get("price", 0))
        mark_price = _safe_float(position.get("markPrice", entry_price))
        liq_price = _safe_float(position.get("liquidationThreshold", 0))

        if liq_price <= 0 or mark_price <= 0:
            return {"distance_pct": 100, "safe": True, "liq_price": liq_price}

        side = str(position.get("side", "")).lower()
        if side == "long":
            distance_pct = ((mark_price - liq_price) / mark_price) * 100
        else:
            distance_pct = ((liq_price - mark_price) / mark_price) * 100

        return {
            "distance_pct": round(max(0.0, distance_pct), 2),
            "safe": distance_pct >= MIN_LIQUIDATION_BUFFER_PCT,
            "liq_price": liq_price,
            "mark_price": mark_price,
            "entry_price": entry_price,
        }

    # ---- Funding Rates ----------------------------------------------------

    @staticmethod
    def get_funding_rate(product_id=None):
        """Get current funding rate for perpetuals.

        Args:
            product_id: e.g., "PF_XBTUSD". If None, returns all perpetuals.

        Returns: dict of product_id -> rate data, or single rate data if product_id given
        """
        result = _futures_request("GET", "/tickers")
        if not result.get("tickers"):
            return {"error": "Failed to get tickers"}

        rates = {}
        for ticker in result["tickers"]:
            sym = str(ticker.get("symbol", "")).upper()
            if sym.startswith("PF_"):
                rate = _safe_float(ticker.get("fundingRate", 0))
                relative_rate = _safe_float(ticker.get("fundingRatePrediction", rate))
                rates[sym] = {
                    "funding_rate": rate,
                    "relative_rate": relative_rate,
                    "last_price": _safe_float(ticker.get("last", 0)),
                    "mark_price": _safe_float(ticker.get("markPrice", 0)),
                    "volume_24h": _safe_float(ticker.get("vol24h", 0)),
                    "open_interest": _safe_float(ticker.get("openInterest", 0)),
                }

        if product_id:
            return rates.get(product_id.upper(), {"error": f"Product {product_id} not found"})
        return rates

    @staticmethod
    def funding_opportunities(threshold=0.01):
        """Find perpetuals with high funding rates (potential arb).

        Returns products where |funding_rate| > threshold (1% default).
        Positive funding = longs pay shorts -> direction to collect = SHORT.
        Negative funding = shorts pay longs -> direction to collect = LONG.

        Args:
            threshold: minimum absolute funding rate to consider

        Returns: list of opportunity dicts sorted by |funding_rate| desc
        """
        rates = KrakenFuturesConnector.get_funding_rate()
        if isinstance(rates, dict) and rates.get("error"):
            return []

        opportunities = []
        for product_id, data in rates.items():
            if not isinstance(data, dict):
                continue
            rate = _safe_float(data.get("funding_rate", 0))
            if abs(rate) > threshold:
                direction = "SHORT" if rate > 0 else "LONG"
                opportunities.append({
                    "product_id": product_id,
                    "funding_rate": rate,
                    "direction": direction,  # Direction to collect funding
                    "annualized_pct": abs(rate) * 3 * 365,  # Kraken funds every 8 hours
                    "last_price": data.get("last_price"),
                    "open_interest": data.get("open_interest"),
                })

        return sorted(opportunities, key=lambda x: abs(x["funding_rate"]), reverse=True)

    # ---- Order Placement (4-layer safety gate) ----------------------------

    @staticmethod
    def place_futures_order(
        product_id,
        side,
        size,
        price=None,
        leverage=None,
        post_only=True,
        reduce_only=False,
        confidence=0.70,
    ):
        """Place a futures order with 4-layer safety gate.

        Layer 1: KRAKEN_FUTURES_ENABLED must be "1"
        Layer 2: Margin health ratio > MIN_MARGIN_HEALTH_RATIO
        Layer 3: Liquidation distance > MIN_LIQUIDATION_BUFFER_PCT
        Layer 4: Leverage <= MAX_LEVERAGE

        GoalValidator also gates all orders (70% confidence, 2+ signals).

        Args:
            product_id: Kraken Futures product (e.g., "PF_XBTUSD")
            side: "buy" or "sell"
            size: order size (number of contracts/units)
            price: limit price (None for market order)
            leverage: requested leverage (default 1.0)
            post_only: True for maker-only (BE A MAKER)
            reduce_only: True to only reduce position
            confidence: signal confidence (0-1)

        Returns: dict with order result or error
        """
        # Layer 1: Environment gate
        if not FUTURES_ENABLED:
            return {"error": "Kraken Futures not enabled (set KRAKEN_FUTURES_ENABLED=1)"}

        if not FUTURES_API_KEY or not FUTURES_PRIVATE_KEY:
            return {"error": "Kraken Futures API keys not configured"}

        # GoalValidator gate (applies to all orders)
        direction = str(side).upper()
        if GoalValidator:
            allowed = GoalValidator.should_trade(confidence, 2, direction, "neutral")
            if not allowed:
                return {
                    "error": f"GoalValidator blocked: {direction} {product_id} conf={confidence:.2f}"
                }
        elif confidence < 0.70:
            return {"error": f"Confidence {confidence:.2f} below 0.70 threshold"}

        # Layer 2: Margin health check (skip for reduce-only)
        if not reduce_only:
            health = KrakenFuturesConnector.margin_health()
            if not health.get("can_open_new"):
                return {
                    "error": (
                        f"Margin health too low: ratio={health.get('ratio', 0):.2f}, "
                        f"positions={health.get('open_positions', 0)}/{MAX_POSITIONS}"
                    )
                }

        # Layer 3: Liquidation distance check (for existing positions, skip reduce-only)
        if not reduce_only:
            existing = KrakenFuturesConnector.get_position(product_id)
            if existing:
                liq = KrakenFuturesConnector.liquidation_distance(product_id)
                if not liq.get("safe"):
                    return {
                        "error": (
                            f"Too close to liquidation: {liq.get('distance_pct', 0):.1f}% "
                            f"(min {MIN_LIQUIDATION_BUFFER_PCT}%)"
                        )
                    }

        # Layer 4: Leverage cap
        effective_leverage = _safe_float(leverage, 1.0)
        if effective_leverage > MAX_LEVERAGE:
            return {
                "error": f"Leverage {effective_leverage}x exceeds max {MAX_LEVERAGE}x"
            }

        # Check max positions (skip for reduce-only)
        if not reduce_only:
            positions = KrakenFuturesConnector.get_positions()
            if len(positions) >= MAX_POSITIONS:
                return {"error": f"Max positions ({MAX_POSITIONS}) reached"}

        # Build order
        order_data = {
            "orderType": "lmt" if price else "mkt",
            "symbol": product_id,
            "side": str(side).lower(),
            "size": size,
        }
        if price:
            order_data["limitPrice"] = price
        if reduce_only:
            order_data["reduceOnly"] = "true"
        if post_only and price:
            order_data["postOnly"] = "true"  # BE A MAKER

        logger.info(
            "Placing futures %s %s: %s size=%.4f @ %s (leverage=%.1fx, conf=%.2f)",
            order_data["orderType"],
            side,
            product_id,
            float(size),
            price or "market",
            effective_leverage,
            confidence,
        )

        result = _futures_request("POST", "/sendorder", order_data)

        # Record in trade DB
        _record_futures_trade(
            product_id, side, size, price, result, confidence, effective_leverage
        )

        return result

    # ---- Basis Trade Scanning ---------------------------------------------

    @staticmethod
    def scan_basis_opportunities(spot_prices=None):
        """Scan for spot vs futures basis opportunities.

        When futures premium > 0.3%, there may be a basis trade:
          - Long spot, short futures
          - Collect the premium as the futures converge to spot

        Args:
            spot_prices: optional dict of {base_currency: spot_price}

        Returns: list of basis opportunity dicts sorted by |basis_pct| desc
        """
        rates = KrakenFuturesConnector.get_funding_rate()
        if isinstance(rates, dict) and rates.get("error"):
            return []

        opportunities = []
        for product_id, data in rates.items():
            if not isinstance(data, dict):
                continue
            mark_price = _safe_float(data.get("mark_price", 0))
            if mark_price <= 0:
                continue

            # Extract base currency from product_id
            base = str(product_id).replace("PF_", "").replace("USD", "")
            if base == "XBT":
                base = "BTC"
            elif base == "XDG":
                base = "DOGE"

            # Get spot price
            spot_price = None
            if spot_prices and base in spot_prices:
                spot_price = _safe_float(spot_prices[base])
            else:
                try:
                    from kraken_connector import KrakenConnector
                    vol = KrakenConnector.get_24h_volume(f"{base}-USD")
                    if not vol.get("error"):
                        spot_price = _safe_float(vol.get("last_price", 0))
                except Exception:
                    pass

            if not spot_price or spot_price <= 0:
                continue

            basis_pct = ((mark_price - spot_price) / spot_price) * 100
            if abs(basis_pct) > 0.3:  # >0.3% premium/discount
                opportunities.append({
                    "product_id": product_id,
                    "base": base,
                    "spot_price": spot_price,
                    "futures_price": mark_price,
                    "basis_pct": round(basis_pct, 4),
                    "direction": "short_futures" if basis_pct > 0 else "long_futures",
                    "funding_rate": _safe_float(data.get("funding_rate", 0)),
                    "venue": "kraken",
                })

        return sorted(opportunities, key=lambda x: abs(x["basis_pct"]), reverse=True)

    # ---- Order Book -------------------------------------------------------

    @staticmethod
    def get_orderbook(product_id):
        """Get order book for a futures product.

        Args:
            product_id: e.g., "PF_XBTUSD"

        Returns: dict with bids and asks
        """
        result = _futures_request("GET", "/orderbook", {"symbol": product_id})
        if result.get("orderBook"):
            return result["orderBook"]
        return result

    # ---- Trade History ----------------------------------------------------

    @staticmethod
    def get_fills(last_fill_time=None):
        """Get recent fills (executed trades).

        Args:
            last_fill_time: ISO timestamp to get fills after

        Returns: list of fill dicts
        """
        data = {}
        if last_fill_time:
            data["lastFillTime"] = last_fill_time

        result = _futures_request("GET", "/fills", data if data else None)
        if result.get("fills"):
            return result["fills"]
        return []

    # ---- Open Orders ------------------------------------------------------

    @staticmethod
    def get_open_orders():
        """Get all open futures orders.

        Returns: list of order dicts
        """
        result = _futures_request("GET", "/openorders")
        if result.get("openOrders"):
            return result["openOrders"]
        return []

    @staticmethod
    def cancel_order(order_id):
        """Cancel an open futures order.

        Args:
            order_id: Kraken Futures order ID

        Returns: dict with cancellation result
        """
        logger.info("Cancelling futures order: %s", order_id)
        return _futures_request("POST", "/cancelorder", {"order_id": order_id})

    @staticmethod
    def cancel_all_orders(product_id=None):
        """Cancel all open futures orders, optionally filtered by product.

        Args:
            product_id: optional product to cancel orders for

        Returns: dict with cancellation result
        """
        data = {}
        if product_id:
            data["symbol"] = product_id
        logger.info("Cancelling all futures orders%s",
                     f" for {product_id}" if product_id else "")
        return _futures_request("POST", "/cancelallorders", data if data else None)

    # ---- Transfer ---------------------------------------------------------

    @staticmethod
    def transfer_to_futures(amount, currency="USD"):
        """Transfer funds from spot wallet to futures wallet.

        Args:
            amount: amount to transfer
            currency: currency (default USD)

        Returns: dict with transfer result
        """
        return _futures_request("POST", "/transfer", {
            "amount": str(amount),
            "fromAccount": "cash",
            "toAccount": "futures",
            "unit": currency,
        })

    # ---- Summary / Status -------------------------------------------------

    def status_summary(self):
        """Get a comprehensive status summary for monitoring.

        Returns: dict with enabled, positions, margin health, open orders
        """
        summary = {
            "enabled": self.enabled,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if not self.enabled:
            summary["reason"] = "KRAKEN_FUTURES_ENABLED=0 or missing API keys"
            return summary

        try:
            summary["margin_health"] = KrakenFuturesConnector.margin_health()
        except Exception as e:
            summary["margin_health"] = {"error": str(e)}

        try:
            positions = KrakenFuturesConnector.get_positions()
            summary["positions"] = len(positions)
            summary["position_details"] = [
                {
                    "symbol": p.get("symbol"),
                    "side": p.get("side"),
                    "size": p.get("size"),
                    "price": p.get("price"),
                }
                for p in positions
            ]
        except Exception as e:
            summary["positions"] = 0
            summary["position_error"] = str(e)

        try:
            orders = KrakenFuturesConnector.get_open_orders()
            summary["open_orders"] = len(orders)
        except Exception as e:
            summary["open_orders"] = 0
            summary["order_error"] = str(e)

        return summary


# ---------------------------------------------------------------------------
# Module-level test
# ---------------------------------------------------------------------------

def test_kraken_futures_connection():
    """Test Kraken Futures API connectivity."""
    logging.basicConfig(level=logging.INFO)
    logger.info("Testing Kraken Futures connection...")

    if not FUTURES_API_KEY or not FUTURES_PRIVATE_KEY:
        logger.warning("Kraken Futures API keys not set -- skipping auth test")
        logger.info("Testing public endpoint (instruments)...")

    products = KrakenFuturesConnector.list_futures_products()
    if products:
        logger.info("Kraken Futures connection OK: %d instruments", len(products))
        perps = [p for p in products
                 if isinstance(p, dict) and str(p.get("symbol", "")).startswith("PF_")]
        logger.info("Perpetual products: %d", len(perps))
        for p in perps[:5]:
            logger.info("  %s (%s)", p.get("symbol"), p.get("type"))
        return True
    else:
        logger.error("Kraken Futures connection failed: no instruments returned")
        return False


if __name__ == "__main__":
    test_kraken_futures_connection()
