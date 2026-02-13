#!/usr/bin/env python3
"""Exchange connectors for real market data and trading.

Supports:
  - Coinbase (Advanced Trade API)
  - Public price feeds (no auth needed)

All exchange secrets are loaded from env vars or .env file.
NEVER hardcode keys in source.
"""

import json
import logging
import os
import secrets
import sqlite3
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("exchange")

# Load from env — CDP (Coinbase Developer Platform) JWT auth
COINBASE_API_KEY_ID = os.environ.get("COINBASE_API_KEY_ID", "")
COINBASE_API_KEY_SECRET = os.environ.get("COINBASE_API_KEY_SECRET", "")

TRADER_DB = str(Path(__file__).parent / "trader.db")
LOCK_FILE = Path(__file__).parent / "trading_lock.json"
STRICT_PROFIT_ONLY = os.environ.get("STRICT_PROFIT_ONLY", "1").lower() not in ("0", "false", "no")
# Require enough upside to cover fees/slippage and still be net positive.
ROUND_TRIP_COST_PCT = float(os.environ.get("ROUND_TRIP_COST_PCT", "0.013"))  # ~1.3%
MIN_NET_PROFIT_PCT = float(os.environ.get("MIN_NET_PROFIT_PCT", "0.002"))    # +0.2%


def _load_trading_lock():
    if not LOCK_FILE.exists():
        return {"locked": False}
    try:
        data = json.loads(LOCK_FILE.read_text())
        if not isinstance(data, dict):
            return {"locked": True, "reason": "Invalid lock file format", "source": "exchange_connector"}
        return data
    except Exception:
        return {"locked": True, "reason": "Unreadable lock file", "source": "exchange_connector"}


def _is_trading_locked():
    lock = _load_trading_lock()
    return bool(lock.get("locked", False)), str(lock.get("reason", "")), str(lock.get("source", ""))


def _normalize_pair_variants(product_id):
    variants = {product_id}
    if product_id.endswith("-USD"):
        variants.add(product_id.replace("-USD", "-USDC"))
    if product_id.endswith("-USDC"):
        variants.add(product_id.replace("-USDC", "-USD"))
    return tuple(variants)


def _last_buy_price(product_id):
    """Read latest BUY cost basis from shared trade ledger."""
    variants = _normalize_pair_variants(product_id)
    try:
        db = sqlite3.connect(TRADER_DB)
        db.row_factory = sqlite3.Row
        placeholders = ",".join("?" for _ in variants)
        row = db.execute(
            f"""SELECT price
                FROM agent_trades
                WHERE side='BUY' AND pair IN ({placeholders}) AND price > 0
                ORDER BY id DESC
                LIMIT 1""",
            variants
        ).fetchone()
        db.close()
        if row:
            return float(row["price"])
    except Exception:
        return None
    return None


def _sell_break_even_min(entry_price):
    return entry_price * (1.0 + ROUND_TRIP_COST_PCT + MIN_NET_PROFIT_PCT)


class PriceFeed:
    """Public price feed — no auth needed. Free and unlimited."""

    CACHE = {}
    CACHE_TTL = 10  # seconds

    @classmethod
    def get_price(cls, pair):
        """Get current spot price for a trading pair."""
        now = time.time()
        if pair in cls.CACHE and now - cls.CACHE[pair]["t"] < cls.CACHE_TTL:
            return cls.CACHE[pair]["price"]

        # Normalize pair format
        base, quote = cls._parse_pair(pair)

        price = cls._fetch_coinbase_price(base, quote)
        if price is None:
            price = cls._fetch_coingecko_price(base, quote)

        if price is not None:
            cls.CACHE[pair] = {"price": price, "t": now}

        return price

    @classmethod
    def get_prices(cls, pairs):
        """Get prices for multiple pairs."""
        return {p: cls.get_price(p) for p in pairs}

    @staticmethod
    def _parse_pair(pair):
        for sep in ["-", "/", "_"]:
            if sep in pair:
                parts = pair.split(sep)
                return parts[0].upper(), parts[1].upper()
        return pair[:3].upper(), pair[3:].upper()

    @staticmethod
    def _fetch_coinbase_price(base, quote):
        """Coinbase public API — no auth, no rate limit issues."""
        try:
            url = f"https://api.coinbase.com/v2/prices/{base}-{quote}/spot"
            req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
            return float(data["data"]["amount"])
        except Exception as e:
            logger.debug("Coinbase price fetch failed for %s-%s: %s", base, quote, e)
            return None

    @staticmethod
    def _fetch_coingecko_price(base, quote):
        """CoinGecko fallback — free, 10-30 calls/min."""
        coin_map = {
            "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
            "XRP": "ripple", "ADA": "cardano", "DOGE": "dogecoin",
            "AVAX": "avalanche-2", "DOT": "polkadot", "LINK": "chainlink",
        }
        coin_id = coin_map.get(base)
        if not coin_id:
            return None
        try:
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies={quote.lower()}"
            req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
            return float(data[coin_id][quote.lower()])
        except Exception:
            return None


class MultiExchangeFeed:
    """Public price feeds from multiple exchanges — no auth needed.

    Enables:
      1. Better arb detection (more price sources)
      2. Cross-exchange spread analysis
      3. Volume-weighted optimal execution routing

    All endpoints are public REST APIs with no rate limit issues at our scale.
    """

    CACHE = {}
    CACHE_TTL = 15  # seconds

    EXCHANGES = {
        "binance": {
            "url": "https://api.binance.com/api/v3/ticker/price?symbol={symbol}",
            "pair_format": lambda b, q: f"{b}{q}",
            "price_key": "price",
        },
        "kraken": {
            "url": "https://api.kraken.com/0/public/Ticker?pair={symbol}",
            "pair_format": lambda b, q: f"{b}{q}",
            "price_key": None,  # special handling
        },
        "okx": {
            "url": "https://www.okx.com/api/v5/market/ticker?instId={symbol}",
            "pair_format": lambda b, q: f"{b}-{q}",
            "price_key": "data.0.last",
        },
        "bybit": {
            "url": "https://api.bybit.com/v5/market/tickers?category=spot&symbol={symbol}",
            "pair_format": lambda b, q: f"{b}{q}",
            "price_key": "result.list.0.lastPrice",
        },
    }

    # Kraken uses non-standard ticker names
    KRAKEN_MAP = {
        "BTC": "XBT", "DOGE": "XDG",
    }

    @classmethod
    def get_price(cls, exchange, base, quote="USD"):
        """Get price from a specific exchange."""
        cache_key = f"{exchange}:{base}-{quote}"
        now = time.time()
        if cache_key in cls.CACHE and now - cls.CACHE[cache_key]["t"] < cls.CACHE_TTL:
            return cls.CACHE[cache_key]["price"]

        price = None
        try:
            if exchange == "binance":
                price = cls._fetch_binance(base, quote)
            elif exchange == "kraken":
                price = cls._fetch_kraken(base, quote)
            elif exchange == "okx":
                price = cls._fetch_okx(base, quote)
            elif exchange == "bybit":
                price = cls._fetch_bybit(base, quote)
        except Exception as e:
            logger.debug("MultiExchange %s %s-%s: %s", exchange, base, quote, e)

        if price is not None:
            cls.CACHE[cache_key] = {"price": price, "t": now}
        return price

    @classmethod
    def get_all_prices(cls, base, quote="USD"):
        """Get prices from all exchanges for cross-exchange analysis."""
        # Binance uses USDT not USD for most pairs
        binance_quote = "USDT" if quote == "USD" else quote
        results = {}

        # Coinbase (via PriceFeed)
        cb_price = PriceFeed.get_price(f"{base}-{quote}")
        if cb_price:
            results["coinbase"] = cb_price

        for exchange in ["binance", "kraken", "bybit"]:
            q = binance_quote if exchange in ("binance", "bybit") else quote
            price = cls.get_price(exchange, base, q)
            if price:
                results[exchange] = price

        # OKX uses USDT
        okx_price = cls.get_price("okx", base, "USDT" if quote == "USD" else quote)
        if okx_price:
            results["okx"] = okx_price

        return results

    @classmethod
    def find_arb_opportunities(cls, pairs=None, min_spread_pct=0.002):
        """Find cross-exchange arbitrage opportunities.

        Returns list of {pair, buy_exchange, sell_exchange, spread_pct, buy_price, sell_price}
        """
        if pairs is None:
            pairs = ["BTC", "ETH", "SOL"]

        opportunities = []
        for base in pairs:
            prices = cls.get_all_prices(base)
            if len(prices) < 2:
                continue

            exchanges = list(prices.keys())
            for i in range(len(exchanges)):
                for j in range(i + 1, len(exchanges)):
                    e1, e2 = exchanges[i], exchanges[j]
                    p1, p2 = prices[e1], prices[e2]
                    spread = abs(p1 - p2) / min(p1, p2)

                    if spread >= min_spread_pct:
                        buy_ex = e1 if p1 < p2 else e2
                        sell_ex = e2 if p1 < p2 else e1
                        opportunities.append({
                            "pair": f"{base}-USD",
                            "buy_exchange": buy_ex,
                            "sell_exchange": sell_ex,
                            "buy_price": min(p1, p2),
                            "sell_price": max(p1, p2),
                            "spread_pct": round(spread * 100, 4),
                        })

        opportunities.sort(key=lambda x: -x["spread_pct"])
        return opportunities

    @staticmethod
    def _fetch_binance(base, quote):
        symbol = f"{base}{quote}"
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        return float(data["price"])

    @staticmethod
    def _fetch_kraken(base, quote):
        # Kraken uses XBT for BTC
        kraken_base = MultiExchangeFeed.KRAKEN_MAP.get(base, base)
        symbol = f"{kraken_base}{quote}"
        url = f"https://api.kraken.com/0/public/Ticker?pair={symbol}"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        result = data.get("result", {})
        if not result:
            return None
        ticker = list(result.values())[0]
        return float(ticker["c"][0])  # last trade price

    @staticmethod
    def _fetch_okx(base, quote):
        symbol = f"{base}-{quote}"
        url = f"https://www.okx.com/api/v5/market/ticker?instId={symbol}"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        items = data.get("data", [])
        if not items:
            return None
        return float(items[0]["last"])

    @staticmethod
    def _fetch_bybit(base, quote):
        symbol = f"{base}{quote}"
        url = f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={symbol}"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        items = data.get("result", {}).get("list", [])
        if not items:
            return None
        return float(items[0]["lastPrice"])


class CoinbaseTrader:
    """Coinbase Advanced Trade API connector using CDP JWT auth.

    Uses EC private key to sign JWTs for the Coinbase Developer Platform API.
    Requires COINBASE_API_KEY_ID and COINBASE_API_KEY_SECRET env vars.
    """

    BASE_URL = "https://api.coinbase.com"

    def __init__(self, key_id=None, key_secret=None):
        self.key_id = key_id or COINBASE_API_KEY_ID
        self.key_secret = key_secret or COINBASE_API_KEY_SECRET
        if self.key_secret:
            # Unescape newlines from env var
            self.key_secret = self.key_secret.replace("\\n", "\n").strip('"')
        if not self.key_id or not self.key_secret:
            logger.warning("Coinbase CDP credentials not set")

    def _build_jwt(self, method, path):
        """Build a JWT token signed with the EC private key."""
        import jwt
        uri = f"{method.upper()} api.coinbase.com{path}"
        now = int(time.time())
        payload = {
            "sub": self.key_id,
            "iss": "cdp",
            "aud": ["cdp_service"],
            "nbf": now,
            "exp": now + 120,
            "uris": [uri],
        }
        headers = {
            "kid": self.key_id,
            "nonce": secrets.token_hex(16),
            "typ": "JWT",
        }
        token = jwt.encode(payload, self.key_secret, algorithm="ES256", headers=headers)
        return token

    # Circuit breaker state
    _consecutive_failures = 0
    _circuit_open_until = 0  # timestamp when circuit should try half-open

    def _request(self, method, path, body=None):
        """Make API request with retry logic and circuit breaker."""
        return self._request_with_retry(method, path, body)

    def _request_with_retry(self, method, path, body=None, max_retries=3):
        """Make API request with exponential backoff retry on transient errors.

        Retries on: 429 (rate limit), 500, 502, 503 (server errors), timeouts.
        Does NOT retry on: 400, 401, 403, 404 (client errors — retrying won't help).
        """
        # Circuit breaker: if open, skip calls until cooldown expires
        now = time.time()
        if self._circuit_open_until > now:
            remaining = int(self._circuit_open_until - now)
            logger.warning("Circuit breaker OPEN — skipping API call (retry in %ds)", remaining)
            return {"error": "Circuit breaker open", "status": 503}

        last_error = None
        for attempt in range(max_retries):
            # Rebuild JWT for each attempt (they have short expiry)
            sign_path = path.split("?")[0]
            token = self._build_jwt(method, sign_path)

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "NetTrace/1.0",
            }

            url = self.BASE_URL + path
            body_data = json.dumps(body).encode() if body else None
            req = urllib.request.Request(url, data=body_data, headers=headers, method=method)

            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    result = json.loads(resp.read().decode())
                    # Success — reset circuit breaker
                    CoinbaseTrader._consecutive_failures = 0
                    if CoinbaseTrader._circuit_open_until > 0:
                        logger.info("Circuit breaker CLOSED — API recovered")
                        CoinbaseTrader._circuit_open_until = 0
                    return result

            except urllib.error.HTTPError as e:
                error_body = e.read().decode()
                last_error = {"error": error_body, "status": e.code}

                # Only retry on transient server errors
                if e.code in (429, 500, 502, 503):
                    backoff = 0.5 * (2 ** attempt)  # 0.5s, 1s, 2s
                    logger.warning("Coinbase API %d (attempt %d/%d) — retrying in %.1fs: %s",
                                   e.code, attempt + 1, max_retries, backoff, error_body[:200])
                    time.sleep(backoff)
                    continue
                else:
                    # Client error — don't retry
                    logger.error("Coinbase API error %d: %s", e.code, error_body)
                    return last_error

            except Exception as e:
                last_error = {"error": str(e)}
                backoff = 0.5 * (2 ** attempt)
                logger.warning("Coinbase request failed (attempt %d/%d) — retrying in %.1fs: %s",
                               attempt + 1, max_retries, backoff, e)
                time.sleep(backoff)

        # All retries exhausted — update circuit breaker
        CoinbaseTrader._consecutive_failures += 1
        if CoinbaseTrader._consecutive_failures >= 3:
            CoinbaseTrader._circuit_open_until = time.time() + 30  # OPEN for 30s
            logger.error("Circuit breaker OPEN — %d consecutive failures, pausing for 30s",
                         CoinbaseTrader._consecutive_failures)

        logger.error("Coinbase request failed after %d attempts: %s", max_retries, last_error)
        return last_error or {"error": "All retries exhausted"}

    def get_accounts(self):
        """List all accounts/wallets."""
        return self._request("GET", "/api/v3/brokerage/accounts")

    def get_product(self, product_id):
        """Get product details (e.g., BTC-USD)."""
        return self._request("GET", f"/api/v3/brokerage/products/{product_id}")

    def get_candles(self, product_id, granularity="ONE_HOUR", limit=100):
        """Get price candles."""
        end = int(time.time())
        start = end - (limit * 3600)
        path = f"/api/v3/brokerage/products/{product_id}/candles?start={start}&end={end}&granularity={granularity}"
        return self._request("GET", path)

    # Product precision cache — maps product_id to base_increment
    _precision_cache = {}

    def _get_precision(self, product_id):
        """Get base_increment for a product (cached)."""
        if product_id not in self._precision_cache:
            info = self.get_product(product_id)
            base_incr = info.get("base_increment", "0.00000001")
            self._precision_cache[product_id] = base_incr
        return self._precision_cache[product_id]

    @staticmethod
    def _truncate_to_increment(value, increment):
        """Truncate value to match Coinbase's required precision."""
        import math
        incr = float(increment)
        if incr <= 0:
            return value
        # Count decimal places in increment
        incr_str = increment.rstrip("0")
        if "." in incr_str:
            decimals = len(incr_str.split(".")[-1])
        else:
            decimals = 0
        factor = 10 ** decimals
        return math.floor(float(value) * factor) / factor

    def place_order(self, product_id, side, size, order_type="market"):
        """Place a market order with automatic precision handling.

        Args:
            product_id: e.g., "BTC-USD"
            side: "BUY" or "SELL"
            size: amount in quote currency for BUY, base currency for SELL
            order_type: "market" only for now
        """
        import uuid
        side_u = side.upper()

        # Global lock guard (applies to ALL order flow).
        locked, lock_reason, lock_source = _is_trading_locked()
        if locked:
            msg = f"Trading locked by {lock_source}: {lock_reason}"
            logger.error("Order blocked: %s", msg)
            return {"error_response": {"error": "TRADING_LOCKED", "message": msg}}

        # Profit-only SELL guard.
        if side_u == "SELL" and STRICT_PROFIT_ONLY:
            entry = _last_buy_price(product_id)
            if entry is None:
                msg = f"SELL blocked for {product_id}: missing cost basis in trader.db"
                logger.warning(msg)
                return {"error_response": {"error": "NO_COST_BASIS", "message": msg}}

            # Market sell uses reference spot for safety check.
            market_price = PriceFeed.get_price(product_id.replace("-USDC", "-USD")) or PriceFeed.get_price(product_id)
            if market_price is None:
                msg = f"SELL blocked for {product_id}: no reference market price"
                logger.warning(msg)
                return {"error_response": {"error": "NO_MARKET_PRICE", "message": msg}}

            min_ok = _sell_break_even_min(entry)
            if market_price < min_ok:
                msg = (f"SELL blocked for {product_id}: ${market_price:.4f} < "
                       f"required ${min_ok:.4f} (entry ${entry:.4f})")
                logger.warning(msg)
                return {"error_response": {"error": "SELL_AT_LOSS_BLOCKED", "message": msg}}

        # For SELL orders, truncate base_size to product's precision
        if side_u == "SELL":
            base_incr = self._get_precision(product_id)
            size = self._truncate_to_increment(size, base_incr)
            if size <= 0:
                return {"error_response": {"error": "SIZE_TOO_SMALL", "message": "Amount too small after precision truncation"}}

        order = {
            "client_order_id": str(uuid.uuid4()),
            "product_id": product_id,
            "side": side_u,
            "order_configuration": {
                "market_market_ioc": {
                    "quote_size": str(size) if side_u == "BUY" else None,
                    "base_size": str(size) if side_u == "SELL" else None,
                }
            }
        }
        # Clean None values
        ioc = order["order_configuration"]["market_market_ioc"]
        order["order_configuration"]["market_market_ioc"] = {k: v for k, v in ioc.items() if v is not None}

        logger.info("Placing %s order: %s %s @ market", side, product_id, size)
        return self._request("POST", "/api/v3/brokerage/orders", order)

    def place_limit_order(self, product_id, side, base_size, limit_price, post_only=True):
        """Place a limit order (MAKER — 0.4% fee instead of 0.6%).

        Game theory: Be the house, not the player.
        Limit orders let us SET the price. If filled, we entered at our chosen level.
        post_only=True ensures we ONLY get maker fees (rejects if it would match immediately).

        Args:
            product_id: e.g., "BTC-USD"
            side: "BUY" or "SELL"
            base_size: amount of base currency (e.g., 0.0001 BTC)
            limit_price: price to buy/sell at
            post_only: if True, reject order if it would take liquidity
        """
        import uuid
        side_u = side.upper()

        # Global lock guard (applies to ALL order flow).
        locked, lock_reason, lock_source = _is_trading_locked()
        if locked:
            msg = f"Trading locked by {lock_source}: {lock_reason}"
            logger.error("Limit order blocked: %s", msg)
            return {"error_response": {"error": "TRADING_LOCKED", "message": msg}}

        # Truncate to product precision
        base_incr = self._get_precision(product_id)
        base_size = self._truncate_to_increment(base_size, base_incr)
        if base_size <= 0:
            return {"error_response": {"error": "SIZE_TOO_SMALL", "message": "Amount too small"}}

        # Get quote increment for price precision
        info = self.get_product(product_id)
        quote_incr = info.get("quote_increment", "0.01")
        limit_price = self._truncate_to_increment(limit_price, quote_incr)

        # Profit-only SELL guard.
        if side_u == "SELL" and STRICT_PROFIT_ONLY:
            entry = _last_buy_price(product_id)
            if entry is None:
                msg = f"LIMIT SELL blocked for {product_id}: missing cost basis in trader.db"
                logger.warning(msg)
                return {"error_response": {"error": "NO_COST_BASIS", "message": msg}}

            min_ok = _sell_break_even_min(entry)
            if float(limit_price) < min_ok:
                msg = (f"LIMIT SELL blocked for {product_id}: ${float(limit_price):.4f} < "
                       f"required ${min_ok:.4f} (entry ${entry:.4f})")
                logger.warning(msg)
                return {"error_response": {"error": "SELL_AT_LOSS_BLOCKED", "message": msg}}

        order = {
            "client_order_id": str(uuid.uuid4()),
            "product_id": product_id,
            "side": side_u,
            "order_configuration": {
                "limit_limit_gtc": {
                    "base_size": str(base_size),
                    "limit_price": str(limit_price),
                    "post_only": post_only,
                }
            }
        }

        logger.info("Placing LIMIT %s: %s %s @ $%s (post_only=%s)",
                     side, product_id, base_size, limit_price, post_only)
        return self._request("POST", "/api/v3/brokerage/orders", order)

    def cancel_order(self, order_id):
        """Cancel an open order."""
        return self._request("POST", "/api/v3/brokerage/orders/batch_cancel",
                             {"order_ids": [order_id]})

    def get_orders(self, product_id=None, status="OPEN"):
        """List orders."""
        path = f"/api/v3/brokerage/orders/historical/batch?order_status={status}"
        if product_id:
            path += f"&product_id={product_id}"
        return self._request("GET", path)

    def get_order_book(self, product_id, level=2):
        """Get order book to find optimal limit price placement."""
        path = f"/api/v3/brokerage/product_book?product_id={product_id}&limit=10"
        return self._request("GET", path)

    def get_order_fill(self, order_id, max_wait=10, poll_interval=1.0):
        """Poll an order until it settles and return actual filled size.

        Limit orders may partially fill. This returns the real filled amount
        so agents don't assume full fills when registering with exit_manager.

        Returns: dict with {filled_size, filled_value, status, avg_price} or None on error.
        """
        start = time.time()
        while time.time() - start < max_wait:
            try:
                result = self._request("GET", f"/api/v3/brokerage/orders/historical/{order_id}")
                order = result.get("order", result)
                status = order.get("status", "UNKNOWN")
                filled_size = float(order.get("filled_size", 0))
                filled_value = float(order.get("filled_value", 0))
                avg_price = filled_value / filled_size if filled_size > 0 else 0

                if status in ("FILLED", "CANCELLED", "EXPIRED", "FAILED"):
                    return {
                        "filled_size": filled_size,
                        "filled_value": filled_value,
                        "avg_price": avg_price,
                        "status": status,
                    }

                # Still pending — wait and poll again
                time.sleep(poll_interval)
            except Exception as e:
                logger.debug("get_order_fill error for %s: %s", order_id, e)
                time.sleep(poll_interval)

        # Timed out — return what we know from last poll
        logger.warning("Order %s did not settle within %ds", order_id, max_wait)
        return None


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "multi":
        # Multi-exchange price comparison
        print("=== Multi-Exchange Price Comparison ===")
        for base in ["BTC", "ETH", "SOL"]:
            prices = MultiExchangeFeed.get_all_prices(base)
            if prices:
                min_p = min(prices.values())
                max_p = max(prices.values())
                spread = (max_p - min_p) / min_p * 100
                print(f"\n  {base}-USD (spread: {spread:.3f}%):")
                for ex, p in sorted(prices.items(), key=lambda x: x[1]):
                    print(f"    {ex:<12} ${p:>10,.2f}")

        print("\n=== Arb Opportunities ===")
        opps = MultiExchangeFeed.find_arb_opportunities()
        if opps:
            for o in opps[:5]:
                print(f"  {o['pair']}: buy {o['buy_exchange']} ${o['buy_price']:,.2f} → "
                      f"sell {o['sell_exchange']} ${o['sell_price']:,.2f} = {o['spread_pct']:.3f}%")
        else:
            print("  No arb opportunities found above threshold")

    else:
        # Quick test
        print("=== Public Price Feed (Coinbase) ===")
        for pair in ["BTC-USD", "ETH-USD", "SOL-USD"]:
            price = PriceFeed.get_price(pair)
            print(f"  {pair}: ${price:,.2f}" if price else f"  {pair}: unavailable")

        if COINBASE_API_KEY_ID:
            print("\n=== Coinbase Account (CDP JWT Auth) ===")
            trader = CoinbaseTrader()
            accounts = trader.get_accounts()
            if "accounts" in accounts:
                for acc in accounts["accounts"]:
                    bal = acc.get("available_balance", {})
                    hold = acc.get("hold", {})
                    avail = float(bal.get("value", 0))
                    held = float(hold.get("value", 0))
                    if avail > 0 or held > 0:
                        held_note = f" (held: {held})" if held > 0 else ""
                        print(f"  {acc.get('currency')}: {avail}{held_note}")
            else:
                print(f"  Response: {json.dumps(accounts, indent=2)[:500]}")
        else:
            print("\nNo Coinbase CDP key set. Set COINBASE_API_KEY_ID and COINBASE_API_KEY_SECRET env vars.")
