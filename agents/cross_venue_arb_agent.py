#!/usr/bin/env python3
"""Cross-Venue Arbitrage Agent — finds price divergences between exchanges.

Compares prices across Coinbase and Kraken every 30s.
When divergence > 0.3% after fees, emits an arb signal.

Fee structure:
  Coinbase taker: 0.8% (standard tier)
  Kraken taker: 0.26%
  Total round-trip: ~1.06% (need divergence > this to profit after transfer)
  But for same-direction trades, just need divergence > max(fee) to prefer one venue.
  Arb threshold: 0.3% divergence = "buy on cheaper venue" signal
"""

import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CROSS_VENUE_ARB] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "cross_venue_arb_agent.log")),
    ],
)
logger = logging.getLogger("cross_venue_arb")

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

try:
    from creative_agent_bridge import CreativeAgentBridge
except ImportError:
    CreativeAgentBridge = None

try:
    from kraken_connector import KrakenConnector
except ImportError:
    KrakenConnector = None

CROSS_VENUE_ARB_INTERVAL_S = int(os.environ.get("CROSS_VENUE_ARB_INTERVAL_S", "30"))
DIVERGENCE_THRESHOLD_PCT = 0.003  # 0.3% minimum divergence to signal
PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD"]

# Kraken pair code mapping (Kraken uses non-standard ticker symbols)
KRAKEN_TICKER_MAP = {
    "BTC-USD": "XXBTZUSD",
    "ETH-USD": "XETHZUSD",
    "SOL-USD": "SOLUSD",
}


def _fetch_coinbase_price(pair: str) -> float | None:
    """Fetch spot price from Coinbase public API."""
    try:
        url = f"https://api.coinbase.com/v2/prices/{pair}/spot"
        req = urllib.request.Request(url, headers={"User-Agent": "CrossVenueArb/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        return float(data["data"]["amount"])
    except Exception as e:
        logger.debug("Coinbase price fetch failed for %s: %s", pair, e)
        return None


def _fetch_kraken_price(pair: str) -> float | None:
    """Fetch last trade price from Kraken public Ticker API."""
    kraken_pair = KRAKEN_TICKER_MAP.get(pair)
    if not kraken_pair:
        return None
    try:
        url = f"https://api.kraken.com/0/public/Ticker?pair={kraken_pair}"
        req = urllib.request.Request(url, headers={"User-Agent": "CrossVenueArb/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if data.get("error"):
            logger.debug("Kraken API error for %s: %s", pair, data["error"])
            return None
        result = data.get("result", {})
        # Kraken returns ticker under the pair key; "c" = last trade [price, lot_volume]
        ticker = result.get(kraken_pair, {})
        if not ticker:
            # Try alternative key (Kraken sometimes uses different keys)
            for key in result:
                ticker = result[key]
                break
        last_price = float(ticker.get("c", [0])[0])
        return last_price if last_price > 0 else None
    except Exception as e:
        logger.debug("Kraken price fetch failed for %s: %s", pair, e)
        return None


def _compute_confidence(divergence_pct: float) -> float:
    """Scale confidence from 0.70 (at 0.3% divergence) to 0.90 (at 1%+ divergence).

    Linear interpolation: conf = 0.70 + (div - 0.003) / (0.01 - 0.003) * 0.20
    Clamped to [0.70, 0.90].
    """
    if divergence_pct <= DIVERGENCE_THRESHOLD_PCT:
        return 0.70
    ratio = (divergence_pct - DIVERGENCE_THRESHOLD_PCT) / (0.01 - DIVERGENCE_THRESHOLD_PCT)
    return min(0.90, 0.70 + ratio * 0.20)


class CrossVenueArbAgent:
    """Compares prices across Coinbase and Kraken, emits arb signals on divergence."""

    def __init__(self):
        self.bridge = CreativeAgentBridge("cross_venue_arb") if CreativeAgentBridge else None
        self.last_signals = {}  # pair -> timestamp of last signal (cooldown)
        self.scan_count = 0
        self.signal_count = 0
        logger.info(
            "CrossVenueArbAgent initialized | pairs=%s | interval=%ds | threshold=%.2f%%",
            PAIRS, CROSS_VENUE_ARB_INTERVAL_S, DIVERGENCE_THRESHOLD_PCT * 100,
        )

    def scan_once(self):
        """Run one scan cycle across all pairs."""
        self.scan_count += 1

        for pair in PAIRS:
            try:
                cb_price = _fetch_coinbase_price(pair)
                kr_price = _fetch_kraken_price(pair)

                if cb_price is None or kr_price is None:
                    logger.debug("Skipping %s: cb=%s kr=%s", pair, cb_price, kr_price)
                    continue

                if cb_price <= 0 or kr_price <= 0:
                    continue

                # Compute divergence: positive = kraken is cheaper, negative = coinbase is cheaper
                mid_price = (cb_price + kr_price) / 2.0
                divergence = (cb_price - kr_price) / mid_price
                abs_divergence = abs(divergence)

                logger.debug(
                    "%s | cb=$%.2f kr=$%.2f | div=%.4f%%",
                    pair, cb_price, kr_price, divergence * 100,
                )

                if abs_divergence < DIVERGENCE_THRESHOLD_PCT:
                    continue

                # Cooldown: do not signal the same pair more than once per 120s
                now = time.time()
                last_signal_time = self.last_signals.get(pair, 0)
                if now - last_signal_time < 120:
                    logger.debug("Cooldown active for %s, skipping signal", pair)
                    continue

                confidence = _compute_confidence(abs_divergence)

                if divergence > 0:
                    # Kraken is cheaper than Coinbase -> BUY on Kraken
                    venue_hint = "kraken"
                    reasoning = (
                        f"Cross-venue arb: {pair} Kraken ${kr_price:.2f} < Coinbase ${cb_price:.2f} "
                        f"({abs_divergence:.3%} divergence)"
                    )
                else:
                    # Coinbase is cheaper than Kraken -> BUY on Coinbase (default)
                    venue_hint = "coinbase"
                    reasoning = (
                        f"Cross-venue arb: {pair} Coinbase ${cb_price:.2f} < Kraken ${kr_price:.2f} "
                        f"({abs_divergence:.3%} divergence)"
                    )

                logger.info(
                    "ARB SIGNAL: %s | div=%.3f%% | buy_on=%s | conf=%.2f",
                    pair, abs_divergence * 100, venue_hint, confidence,
                )

                if self.bridge:
                    ok = self.bridge.submit_signal(
                        pair=pair,
                        direction="BUY",
                        confidence=confidence,
                        urgency="high",
                        reasoning=reasoning,
                        market_type="crypto",
                    )
                    if ok:
                        self.signal_count += 1
                        self.last_signals[pair] = now
                else:
                    logger.warning("No bridge available, arb signal dropped: %s", pair)

            except Exception as e:
                logger.error("Error scanning %s: %s", pair, e, exc_info=True)

    def run(self):
        """Main loop: scan every CROSS_VENUE_ARB_INTERVAL_S seconds."""
        logger.info("CrossVenueArbAgent main loop starting")

        while True:
            try:
                self.scan_once()
            except Exception as e:
                logger.error("Scan cycle error: %s", e, exc_info=True)

            if self.scan_count % 20 == 0:
                logger.info(
                    "Cross-venue arb status: scans=%d signals=%d pairs=%d",
                    self.scan_count, self.signal_count, len(PAIRS),
                )

            time.sleep(CROSS_VENUE_ARB_INTERVAL_S)


def main():
    agent = CrossVenueArbAgent()
    agent.run()


if __name__ == "__main__":
    main()
