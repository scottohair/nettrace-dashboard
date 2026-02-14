#!/usr/bin/env python3
"""Deribit Authenticated API Connector — derivatives data + perpetual funding.

Provides:
  - Perpetual funding rates (BTC, ETH perpetuals)
  - Open interest by strike (for liquidation estimation)
  - Mark price vs index price (basis tracking)
  - Order book depth (for execution planning)

All read-only (no trading permissions).
"""

import hashlib
import hmac
import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("deribit_connector")

# Load credentials from environment (set via flyctl secrets)
CLIENT_ID = os.environ.get("DERIBIT_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("DERIBIT_CLIENT_SECRET", "")
API_URL = "https://www.deribit.com/api/v2"


class DeribitConnector:
    """Read-only Deribit API access (derivatives data only)."""

    @staticmethod
    def get_perpetual_funding_rate(instrument: str) -> dict:
        """Get perpetual funding rate.

        Args:
            instrument: e.g., "BTC-PERPETUAL", "ETH-PERPETUAL"

        Returns:
            Dict with funding rate, marked price, index price
        """
        try:
            if not CLIENT_ID or not CLIENT_SECRET:
                logger.warning("Deribit credentials not configured")
                return {"error": "Credentials not set"}

            # Public endpoint (no auth required)
            url = f"{API_URL}/public/get_funding_rate_history?instrument_name={instrument}&count=1"

            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())

            if not data.get("result"):
                logger.warning(f"No funding rate data for {instrument}")
                return {"pair": instrument, "funding_rate": None}

            result = data["result"][0] if data["result"] else {}
            return {
                "pair": instrument,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "funding_rate": result.get("interest_rate"),
                "index_price": result.get("index_price"),
                "mark_price": result.get("mark_price"),
                "basis_pct": (
                    (result.get("mark_price", 0) / result.get("index_price", 1) - 1) * 100
                    if result.get("index_price")
                    else 0
                ),
            }

        except Exception as e:
            logger.error(f"Failed to get funding rate for {instrument}: {e}")
            return {"error": str(e)}

    @staticmethod
    def get_open_interest(instrument: str) -> dict:
        """Get open interest (contracts and USD value)."""
        try:
            url = f"{API_URL}/public/get_open_interest?instrument_name={instrument}"

            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())

            result = data.get("result", {})
            return {
                "pair": instrument,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "open_interest": result.get("open_interest"),
                "usd_value": result.get("open_interest_usd"),
            }

        except Exception as e:
            logger.error(f"Failed to get open interest for {instrument}: {e}")
            return {"error": str(e)}

    @staticmethod
    def get_orderbook(instrument: str, depth: int = 20) -> dict:
        """Get order book for basis analysis."""
        try:
            url = f"{API_URL}/public/get_order_book?instrument_name={instrument}&depth={depth}"

            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())

            result = data.get("result", {})
            return {
                "pair": instrument,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "bid": result.get("bids", [])[:depth],
                "ask": result.get("asks", [])[:depth],
                "last_price": result.get("last_price"),
                "mark_price": result.get("mark_price"),
                "index_price": result.get("index_price"),
            }

        except Exception as e:
            logger.error(f"Failed to get orderbook for {instrument}: {e}")
            return {"error": str(e)}

    @staticmethod
    def get_spot_futures_basis(spot_price: float, futures_instrument: str) -> dict:
        """Calculate spot-futures basis (for arb detection).

        Args:
            spot_price: Current spot price (e.g., from Kraken)
            futures_instrument: e.g., "BTC-PERPETUAL"

        Returns:
            Basis points and arb opportunity
        """
        try:
            orderbook = DeribitConnector.get_orderbook(futures_instrument, depth=1)
            if "error" in orderbook:
                return orderbook

            futures_price = orderbook.get("mark_price", 0)
            if not futures_price:
                return {"error": "No futures price available"}

            basis_pct = (futures_price / spot_price - 1) * 100
            basis_bps = basis_pct * 100

            return {
                "pair": futures_instrument,
                "spot_price": spot_price,
                "futures_price": futures_price,
                "basis_pct": basis_pct,
                "basis_bps": basis_bps,
                "arb_side": "SHORT spot / LONG futures" if basis_pct > 0.3 else "LONG spot / SHORT futures" if basis_pct < -0.3 else "NO_ARB",
                "min_arb_pct": 0.3,  # Need >0.3% to justify fees
            }

        except Exception as e:
            logger.error(f"Failed to calculate basis: {e}")
            return {"error": str(e)}


def test_deribit_connection():
    """Test that Deribit API is accessible."""
    logger.info("Testing Deribit API connection...")

    if not CLIENT_ID or not CLIENT_SECRET:
        logger.warning("Deribit credentials not set (optional for read-only data)")
        logger.info("Still testing public endpoints...")

    try:
        # Test public endpoint
        result = DeribitConnector.get_perpetual_funding_rate("BTC-PERPETUAL")
        if "error" not in result:
            logger.info(f"✓ Deribit connection OK: BTC funding rate = {result.get('funding_rate')}")
            return True
        else:
            logger.error(f"✗ Deribit API error: {result.get('error')}")
            return False
    except Exception as e:
        logger.error(f"✗ Deribit connection failed: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_deribit_connection()
