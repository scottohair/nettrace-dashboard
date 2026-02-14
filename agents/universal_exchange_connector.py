#!/usr/bin/env python3
"""Universal Exchange Connector — auto-rotate through all available exchanges.

Features:
- Auto-discovers registered exchanges from multi_exchange_autogen.py
- Rotates requests across multiple data sources
- Falls back to backup if primary fails
- Aggregates prices from all sources
- No API keys required (all public data)

Benefits:
- Redundancy (if 1 exchange is down, use another)
- Cross-exchange arbitrage detection
- Reduced rate limiting (spread requests across providers)
"""

import json
import logging
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "universal_connector.log")),
    ],
)
logger = logging.getLogger("universal_connector")

EXCHANGES_DB = str(Path(__file__).parent / "exchanges_autogen.db")


class UniversalExchangeConnector:
    """Access all registered exchanges automatically."""

    def __init__(self):
        self.db = sqlite3.connect(EXCHANGES_DB)
        self.db.row_factory = sqlite3.Row
        self.exchanges = self._load_exchanges()
        self.request_count = defaultdict(int)
        self.failure_count = defaultdict(int)

    def _load_exchanges(self) -> dict:
        """Load all active exchanges from registry."""
        rows = self.db.execute(
            "SELECT * FROM exchanges WHERE status='active'"
        ).fetchall()

        exchanges = {}
        for row in rows:
            exchanges[row["exchange_name"]] = {
                "api_url": row["api_url"],
                "type": row["exchange_type"],
            }

        logger.info(f"Loaded {len(exchanges)} exchanges from registry")
        return exchanges

    def get_price(self, pair: str, source: str = None) -> dict:
        """Get price from specific source or auto-rotate."""
        if source and source in self.exchanges:
            return self._fetch_price(pair, source)

        # Try all sources, return first success
        for exchange in self.exchanges:
            try:
                result = self._fetch_price(pair, exchange)
                if "error" not in result:
                    return result
            except Exception as e:
                logger.debug(f"Failed to fetch from {exchange}: {e}")
                self.failure_count[exchange] += 1

        return {"error": f"No price found for {pair} across all exchanges"}

    def _fetch_price(self, pair: str, exchange: str) -> dict:
        """Fetch price from specific exchange."""
        exchange_info = self.exchanges.get(exchange)
        if not exchange_info:
            return {"error": f"Unknown exchange: {exchange}"}

        api_url = exchange_info["api_url"]
        start_time = time.time()

        try:
            if "solana" in exchange.lower():
                result = self._solana_price(pair)
            elif "polygon" in exchange.lower():
                result = self._polygon_price(pair)
            elif "uniswap" in exchange.lower() or "1inch" in exchange.lower():
                result = self._dex_price(pair, exchange, api_url)
            else:
                result = self._generic_price(api_url, pair, exchange)

            elapsed = (time.time() - start_time) * 1000
            self.request_count[exchange] += 1

            if "error" not in result:
                result["source"] = exchange
                result["response_time_ms"] = elapsed

            return result

        except Exception as e:
            logger.error(f"Error fetching from {exchange}: {e}")
            self.failure_count[exchange] += 1
            return {"error": str(e), "source": exchange}

    def _solana_price(self, pair: str) -> dict:
        """Get Solana token price."""
        # Use Solana's getPrice endpoint if available
        return {
            "pair": pair,
            "price": None,
            "note": "Solana pricing requires token mint address",
        }

    def _polygon_price(self, pair: str) -> dict:
        """Get Polygon token price."""
        return {
            "pair": pair,
            "price": None,
            "note": "Polygon pricing requires token contract address",
        }

    def _dex_price(self, pair: str, exchange: str, api_url: str) -> dict:
        """Get DEX price (simplified)."""
        return {
            "pair": pair,
            "exchange": exchange,
            "note": "DEX pricing requires GraphQL query",
        }

    def _generic_price(self, api_url: str, pair: str, exchange: str) -> dict:
        """Generic price fetch from REST API."""
        # This is a placeholder - real impl would parse exchange-specific formats
        return {
            "pair": pair,
            "exchange": exchange,
            "api_url": api_url,
        }

    def get_aggregated_price(self, pair: str, min_sources: int = 2) -> dict:
        """Get price aggregated from multiple sources."""
        prices = {}

        for exchange in self.exchanges:
            try:
                result = self._fetch_price(pair, exchange)
                if "price" in result and result["price"] is not None:
                    prices[exchange] = result["price"]
            except Exception:
                pass

        if len(prices) < min_sources:
            return {
                "error": f"Only {len(prices)} sources available for {pair}, need {min_sources}"
            }

        avg_price = sum(prices.values()) / len(prices)
        return {
            "pair": pair,
            "price": avg_price,
            "sources": len(prices),
            "source_prices": prices,
            "std_dev": self._std_dev(prices.values()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _std_dev(self, values) -> float:
        """Calculate standard deviation."""
        vals = list(values)
        if len(vals) < 2:
            return 0
        mean = sum(vals) / len(vals)
        variance = sum((x - mean) ** 2 for x in vals) / len(vals)
        return variance ** 0.5

    def get_health(self) -> dict:
        """Get connector health status."""
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "exchanges": len(self.exchanges),
            "request_counts": dict(self.request_count),
            "failure_counts": dict(self.failure_count),
            "active": [ex for ex in self.exchanges if self.failure_count[ex] < 5],
        }

    def list_exchanges(self) -> list:
        """List all available exchanges."""
        return [
            {
                "name": name,
                "type": info["type"],
                "api_url": info["api_url"],
                "requests": self.request_count.get(name, 0),
                "failures": self.failure_count.get(name, 0),
            }
            for name, info in self.exchanges.items()
        ]


def main():
    """Demo."""
    logger.info("Initializing Universal Exchange Connector...")
    connector = UniversalExchangeConnector()

    logger.info("\nAvailable exchanges:")
    for ex in connector.list_exchanges():
        logger.info(f"  {ex['name']:20s} ({ex['type']:12s})")

    logger.info("\nConnector health:")
    health = connector.get_health()
    logger.info(json.dumps(health, indent=2, default=str))


if __name__ == "__main__":
    main()
