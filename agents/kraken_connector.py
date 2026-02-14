#!/usr/bin/env python3
"""Kraken Authenticated API Connector — read-only data access.

Provides access to:
  - Funding rates (for liquidation hunting)
  - Open interest (for leverage estimation)
  - Order book depth (for execution planning)
  - Recent trades (for microstructure analysis)

All read-only (no trading permissions).
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("kraken_connector")

# Load credentials from environment (set via flyctl secrets)
API_KEY = os.environ.get("KRAKEN_API_KEY", "")
PRIVATE_KEY = os.environ.get("KRAKEN_PRIVATE_KEY", "")
API_URL = "https://api.kraken.com"
API_VERSION = "0"

# Kraken pair mappings
KRAKEN_PAIRS = {
    "BTC": "XXBT",  # Bitcoin special case
    "XDG": "XDG",   # Doge
    "ETH": "XETH",
    "SOL": "SOL",
    "AVAX": "AVAX",
    "LINK": "LINK",
    "DOGE": "XDG",
}


def _get_kraken_pair(symbol: str) -> str:
    """Map standard symbol to Kraken asset code."""
    return KRAKEN_PAIRS.get(symbol, symbol)


def _sign_request(endpoint: str, data: dict, nonce: str) -> tuple:
    """Sign Kraken private API request."""
    postdata = urllib.parse.urlencode(data)
    encoded = (str(nonce) + postdata).encode()
    message = endpoint.encode() + hashlib.sha256(encoded).digest()

    signature = hmac.new(
        base64.b64decode(PRIVATE_KEY),
        message,
        hashlib.sha512
    )
    return signature.digest(), postdata


class KrakenConnector:
    """Read-only Kraken API access (data collection only)."""

    @staticmethod
    def get_funding_rate(pair: str) -> dict:
        """Get funding rate for perpetuals pair (if available).

        Kraken uses a different model than Binance — returns interest rates
        instead of traditional funding rates. But useful for leverage cost estimation.
        """
        try:
            if not API_KEY or not PRIVATE_KEY:
                logger.warning("Kraken API keys not configured")
                return {"error": "Credentials not set"}

            # For now, return placeholder (Kraken doesn't expose perpetual funding rates
            # in the same way Binance does — we'd need to use their margin interest rates)
            # This is a limitation of Kraken's API structure
            logger.info(f"Funding rate check for {pair}: Kraken uses margin interest, not perpetual funding")
            return {"pair": pair, "funding_rate": None, "source": "kraken_margin_interest"}

        except Exception as e:
            logger.error(f"Failed to get funding rate for {pair}: {e}")
            return {"error": str(e)}

    @staticmethod
    def get_open_interest(pair: str) -> dict:
        """Get open interest data for leverage estimation."""
        try:
            if not API_KEY or not PRIVATE_KEY:
                logger.warning("Kraken API keys not configured")
                return {"error": "Credentials not set"}

            kraken_pair = _get_kraken_pair(pair.split("-")[0])
            logger.info(f"Open interest check for {pair} ({kraken_pair}): Limited API support")

            # Kraken doesn't expose open interest via API like Binance does
            # This is a known limitation — would need exchange documentation updates
            return {
                "pair": pair,
                "open_interest": None,
                "note": "Kraken API does not expose open interest directly"
            }

        except Exception as e:
            logger.error(f"Failed to get open interest for {pair}: {e}")
            return {"error": str(e)}

    @staticmethod
    def get_orderbook(pair: str, depth: int = 20) -> dict:
        """Get order book for liquidity analysis."""
        try:
            kraken_pair = _get_kraken_pair(pair.split("-")[0]) + "USD"
            url = f"{API_URL}/{API_VERSION}/public/Depth?pair={kraken_pair}&count={depth}"

            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())

            if data.get("error"):
                logger.error(f"Kraken error: {data['error']}")
                return {"error": data["error"]}

            result = data.get("result", {})
            return {
                "pair": pair,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "asks": result.get("asks", [])[:depth],  # [price, volume, timestamp]
                "bids": result.get("bids", [])[:depth],
            }

        except Exception as e:
            logger.error(f"Failed to get orderbook for {pair}: {e}")
            return {"error": str(e)}

    @staticmethod
    def get_recent_trades(pair: str, limit: int = 100) -> dict:
        """Get recent trades for microstructure analysis."""
        try:
            kraken_pair = _get_kraken_pair(pair.split("-")[0]) + "USD"
            url = f"{API_URL}/{API_VERSION}/public/Trades?pair={kraken_pair}"

            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())

            if data.get("error"):
                logger.error(f"Kraken error: {data['error']}")
                return {"error": data["error"]}

            result = data.get("result", {})
            trades = result.get(kraken_pair, [])[-limit:]  # Most recent

            return {
                "pair": pair,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "trades": trades,  # [price, volume, time, buy/sell, market/limit, misc]
                "count": len(trades),
            }

        except Exception as e:
            logger.error(f"Failed to get recent trades for {pair}: {e}")
            return {"error": str(e)}

    @staticmethod
    def get_24h_volume(pair: str) -> dict:
        """Get 24h volume for pair."""
        try:
            kraken_pair = _get_kraken_pair(pair.split("-")[0]) + "USD"
            url = f"{API_URL}/{API_VERSION}/public/Ticker?pair={kraken_pair}"

            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())

            if data.get("error"):
                logger.error(f"Kraken error: {data['error']}")
                return {"error": data["error"]}

            result = data.get("result", {})
            ticker = result.get(kraken_pair, {})

            return {
                "pair": pair,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "volume_24h": float(ticker.get("v", [0, 0])[1] or 0),  # [30m, 24h]
                "price_24h_high": float(ticker.get("h", [0, 0])[1] or 0),
                "price_24h_low": float(ticker.get("l", [0, 0])[1] or 0),
                "last_price": float(ticker.get("c", [0])[0] or 0),
            }

        except Exception as e:
            logger.error(f"Failed to get 24h volume for {pair}: {e}")
            return {"error": str(e)}


def test_kraken_connection():
    """Test that Kraken API is accessible."""
    logger.info("Testing Kraken API connection...")

    if not API_KEY or not PRIVATE_KEY:
        logger.error("Kraken credentials not set")
        return False

    try:
        # Test public endpoint (no auth required)
        result = KrakenConnector.get_orderbook("BTC-USD", depth=5)
        if "error" not in result:
            logger.info(f"✓ Kraken connection OK: {result['pair']} orderbook fetched")
            return True
        else:
            logger.error(f"✗ Kraken API error: {result['error']}")
            return False
    except Exception as e:
        logger.error(f"✗ Kraken connection failed: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_kraken_connection()
