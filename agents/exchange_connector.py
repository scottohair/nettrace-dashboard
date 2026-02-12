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
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("exchange")

# Load from env — CDP (Coinbase Developer Platform) JWT auth
COINBASE_API_KEY_ID = os.environ.get("COINBASE_API_KEY_ID", "")
COINBASE_API_KEY_SECRET = os.environ.get("COINBASE_API_KEY_SECRET", "")


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

    def _request(self, method, path, body=None):
        # Strip query params from the URI used for JWT signing
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
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            error_body = e.read().decode()
            logger.error("Coinbase API error %d: %s", e.code, error_body)
            return {"error": error_body, "status": e.code}
        except Exception as e:
            logger.error("Coinbase request failed: %s", e)
            return {"error": str(e)}

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

    def place_order(self, product_id, side, size, order_type="market"):
        """Place a market order.

        Args:
            product_id: e.g., "BTC-USD"
            side: "BUY" or "SELL"
            size: amount in quote currency for BUY, base currency for SELL
            order_type: "market" only for now
        """
        import uuid
        order = {
            "client_order_id": str(uuid.uuid4()),
            "product_id": product_id,
            "side": side.upper(),
            "order_configuration": {
                "market_market_ioc": {
                    "quote_size": str(size) if side.upper() == "BUY" else None,
                    "base_size": str(size) if side.upper() == "SELL" else None,
                }
            }
        }
        # Clean None values
        ioc = order["order_configuration"]["market_market_ioc"]
        order["order_configuration"]["market_market_ioc"] = {k: v for k, v in ioc.items() if v is not None}

        logger.info("Placing %s order: %s %s @ market", side, product_id, size)
        return self._request("POST", "/api/v3/brokerage/orders", order)

    def get_orders(self, product_id=None, status="OPEN"):
        """List orders."""
        path = f"/api/v3/brokerage/orders/historical/batch?order_status={status}"
        if product_id:
            path += f"&product_id={product_id}"
        return self._request("GET", path)


if __name__ == "__main__":
    # Quick test
    print("=== Public Price Feed ===")
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
                if float(bal.get("value", 0)) > 0:
                    print(f"  {acc.get('currency')}: {bal.get('value')}")
        else:
            print(f"  Response: {json.dumps(accounts, indent=2)[:500]}")
    else:
        print("\nNo Coinbase CDP key set. Set COINBASE_API_KEY_ID and COINBASE_API_KEY_SECRET env vars.")
