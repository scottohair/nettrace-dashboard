#!/usr/bin/env python3
"""ResearchAgent — scrapes market data, news, on-chain metrics.

Data sources:
  1. Coinbase Advanced Trade API (portfolio, candles, order book)
  2. Fly.io NetTrace signals API (latency anomalies)
  3. Fear & Greed Index (alternative.me)
  4. CoinGecko (market cap, volume, trending)
  5. Multi-exchange price feeds (arb detection)

Produces research memos containing:
  - Current prices and 24h changes
  - Volatility metrics (ATR, Bollinger width)
  - Cross-exchange spreads
  - Fear & Greed score
  - NetTrace latency signals
  - Learning agent insights (from previous cycle)

Output: research_memo -> StrategyAgent
"""

import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Setup paths
_AGENTS_DIR = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _AGENTS_DIR)

# Load .env from parent agents/ dir
_env_path = Path(_AGENTS_DIR) / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

from advanced_team.message_bus import MessageBus

logger = logging.getLogger("research_agent")

NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")
FLY_URL = "https://nettrace-dashboard.fly.dev"

# Assets we research
RESEARCH_PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD", "DOGE-USD"]
COINGECKO_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
    "AVAX": "avalanche-2", "LINK": "chainlink", "DOGE": "dogecoin",
}


class ResearchAgent:
    """Scrapes market data from multiple sources and produces research memos."""

    NAME = "research"

    def __init__(self):
        self.bus = MessageBus()
        self.state = {
            "last_msg_id": 0,
            "last_run": None,
            "cycle_count": 0,
            "error_count": 0,
        }

    def _http_get(self, url, headers=None, timeout=8):
        """Safe HTTP GET with timeout."""
        hdrs = {"User-Agent": "NetTrace-AdvancedTeam/1.0"}
        if headers:
            hdrs.update(headers)
        req = urllib.request.Request(url, headers=hdrs)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            logger.debug("HTTP GET failed %s: %s", url, e)
            return None

    def fetch_coinbase_prices(self):
        """Get current prices from Coinbase public API."""
        prices = {}
        for pair in RESEARCH_PAIRS:
            base, quote = pair.split("-")
            data = self._http_get(f"https://api.coinbase.com/v2/prices/{base}-{quote}/spot")
            if data and "data" in data:
                prices[pair] = {
                    "price": float(data["data"]["amount"]),
                    "source": "coinbase",
                }
        return prices

    def fetch_coinbase_candles(self, pair="BTC-USD"):
        """Get 24h of hourly candles for volatility analysis."""
        try:
            # Use public exchange API for candles
            product = pair.replace("-USD", "-USDC") if "USDC" not in pair else pair
            end = int(time.time())
            start = end - 86400  # 24h
            url = (f"https://api.exchange.coinbase.com/products/{product}/candles"
                   f"?start={datetime.fromtimestamp(start, tz=timezone.utc).isoformat()}"
                   f"&end={datetime.fromtimestamp(end, tz=timezone.utc).isoformat()}"
                   f"&granularity=3600")
            data = self._http_get(url)
            if data and isinstance(data, list) and len(data) > 0:
                # Candles: [time, low, high, open, close, volume]
                closes = [c[4] for c in data]
                highs = [c[2] for c in data]
                lows = [c[1] for c in data]
                volumes = [c[5] for c in data]

                # Compute volatility metrics
                if len(closes) >= 2:
                    price_changes = [abs(closes[i] - closes[i-1]) / closes[i-1]
                                     for i in range(1, len(closes))]
                    avg_vol = sum(price_changes) / len(price_changes)
                    max_vol = max(price_changes)
                    total_volume = sum(volumes)

                    # ATR approximation
                    trs = [highs[i] - lows[i] for i in range(len(highs))]
                    atr = sum(trs) / len(trs) if trs else 0

                    return {
                        "pair": pair,
                        "candle_count": len(closes),
                        "latest_close": closes[0] if closes else None,
                        "high_24h": max(highs),
                        "low_24h": min(lows),
                        "avg_hourly_volatility": round(avg_vol * 100, 4),
                        "max_hourly_volatility": round(max_vol * 100, 4),
                        "atr_24h": round(atr, 2),
                        "volume_24h": round(total_volume, 2),
                        "range_pct": round((max(highs) - min(lows)) / min(lows) * 100, 4),
                    }
        except Exception as e:
            logger.debug("Candle fetch failed for %s: %s", pair, e)
        return None

    def fetch_nettrace_signals(self):
        """Fetch latency signals from Fly.io NetTrace API."""
        url = f"{FLY_URL}/api/v1/signals?limit=20"
        headers = {}
        if NETTRACE_API_KEY:
            headers["Authorization"] = f"Bearer {NETTRACE_API_KEY}"
        data = self._http_get(url, headers=headers)
        if data and "signals" in data:
            signals = data["signals"]
            # Summarize by type
            summary = {}
            for sig in signals:
                sig_type = sig.get("signal_type", "unknown")
                summary.setdefault(sig_type, []).append({
                    "host": sig.get("target_host", ""),
                    "confidence": float(sig.get("confidence", 0)),
                    "direction": sig.get("direction", ""),
                    "timestamp": sig.get("timestamp", ""),
                })
            return {"signal_count": len(signals), "by_type": summary, "raw": signals[:5]}
        return {"signal_count": 0, "by_type": {}, "raw": []}

    def fetch_fear_greed(self):
        """Fetch Crypto Fear & Greed Index."""
        data = self._http_get("https://api.alternative.me/fng/?limit=1")
        if data and "data" in data and len(data["data"]) > 0:
            entry = data["data"][0]
            return {
                "value": int(entry.get("value", 50)),
                "classification": entry.get("value_classification", "Neutral"),
                "timestamp": entry.get("timestamp", ""),
            }
        return {"value": 50, "classification": "Neutral", "timestamp": ""}

    def fetch_coingecko_market(self):
        """Fetch market data from CoinGecko (market cap, volume, 24h change)."""
        ids = ",".join(COINGECKO_IDS.values())
        url = (f"https://api.coingecko.com/api/v3/simple/price"
               f"?ids={ids}&vs_currencies=usd"
               f"&include_24hr_vol=true&include_24hr_change=true"
               f"&include_market_cap=true")
        data = self._http_get(url)
        if not data:
            return {}

        result = {}
        for ticker, cg_id in COINGECKO_IDS.items():
            if cg_id in data:
                entry = data[cg_id]
                result[ticker] = {
                    "price": entry.get("usd", 0),
                    "change_24h_pct": round(entry.get("usd_24h_change", 0), 2),
                    "volume_24h": entry.get("usd_24h_vol", 0),
                    "market_cap": entry.get("usd_market_cap", 0),
                }
        return result

    def fetch_cross_exchange_spreads(self):
        """Get cross-exchange price spreads for arb detection."""
        try:
            from exchange_connector import MultiExchangeFeed
            opportunities = MultiExchangeFeed.find_arb_opportunities(
                pairs=["BTC", "ETH", "SOL"],
                min_spread_pct=0.001,
            )
            spreads = {}
            for base in ["BTC", "ETH", "SOL"]:
                prices = MultiExchangeFeed.get_all_prices(base)
                if len(prices) >= 2:
                    min_p = min(prices.values())
                    max_p = max(prices.values())
                    spread = (max_p - min_p) / min_p * 100
                    spreads[base] = {
                        "spread_pct": round(spread, 4),
                        "exchanges": {k: round(v, 2) for k, v in prices.items()},
                        "cheapest": min(prices, key=prices.get),
                        "priciest": max(prices, key=prices.get),
                    }
            return {"spreads": spreads, "arb_opportunities": opportunities[:3]}
        except Exception as e:
            logger.debug("Cross-exchange spread check failed: %s", e)
            return {"spreads": {}, "arb_opportunities": []}

    def get_learning_insights(self):
        """Read any insights from LearningAgent (previous cycle feedback)."""
        msgs = self.bus.read_latest(self.NAME, msg_type="learning_insights", count=1)
        if msgs:
            return msgs[0].get("payload", {})
        return {}

    def run(self, cycle):
        """Execute one research cycle. Publish memo to StrategyAgent."""
        logger.info("ResearchAgent cycle %d starting", cycle)
        self.state["cycle_count"] = cycle
        self.state["last_run"] = datetime.now(timezone.utc).isoformat()

        # Gather data from all sources (best-effort — don't fail on any single source)
        memo = {
            "cycle": cycle,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # 1. Coinbase prices
        try:
            memo["prices"] = self.fetch_coinbase_prices()
        except Exception as e:
            logger.warning("Coinbase prices failed: %s", e)
            memo["prices"] = {}

        # 2. Volatility from candles (BTC and ETH)
        candle_data = {}
        for pair in ["BTC-USD", "ETH-USD", "SOL-USD"]:
            try:
                cd = self.fetch_coinbase_candles(pair)
                if cd:
                    candle_data[pair] = cd
            except Exception as e:
                logger.debug("Candle fetch %s: %s", pair, e)
        memo["volatility"] = candle_data

        # 3. NetTrace signals
        try:
            memo["nettrace_signals"] = self.fetch_nettrace_signals()
        except Exception as e:
            logger.debug("NetTrace signals: %s", e)
            memo["nettrace_signals"] = {"signal_count": 0, "by_type": {}, "raw": []}

        # 4. Fear & Greed
        try:
            memo["fear_greed"] = self.fetch_fear_greed()
        except Exception as e:
            logger.debug("Fear & Greed: %s", e)
            memo["fear_greed"] = {"value": 50, "classification": "Neutral"}

        # 5. CoinGecko market data
        try:
            memo["coingecko"] = self.fetch_coingecko_market()
        except Exception as e:
            logger.debug("CoinGecko: %s", e)
            memo["coingecko"] = {}

        # 6. Cross-exchange spreads
        try:
            memo["cross_exchange"] = self.fetch_cross_exchange_spreads()
        except Exception as e:
            logger.debug("Cross-exchange: %s", e)
            memo["cross_exchange"] = {"spreads": {}, "arb_opportunities": []}

        # 7. Learning agent feedback from previous cycle
        memo["learning_insights"] = self.get_learning_insights()

        # Publish research memo to StrategyAgent
        msg_id = self.bus.publish(
            sender=self.NAME,
            recipient="strategy",
            msg_type="research_memo",
            payload=memo,
            cycle=cycle,
        )
        logger.info("ResearchAgent published memo (msg_id=%d) with %d price pairs, "
                     "%d signals, fear_greed=%d",
                     msg_id, len(memo.get("prices", {})),
                     memo.get("nettrace_signals", {}).get("signal_count", 0),
                     memo.get("fear_greed", {}).get("value", 50))

        return memo
