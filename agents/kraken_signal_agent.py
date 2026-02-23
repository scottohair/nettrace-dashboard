#!/usr/bin/env python3
"""Kraken Signal Agent — generates trading signals from Kraken market data.

Signal types:
  1. Orderbook imbalance: bid/ask volume ratio detects directional pressure
  2. Volume anomaly: 24h volume spikes vs rolling average = momentum
  3. Cross-venue divergence: Kraken vs Coinbase price gap = arb opportunity

All signals use public endpoints (no API keys required).
Signals are pushed via CreativeAgentBridge with market_type="crypto".
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
    format="%(asctime)s [KRAKEN_SIGNAL] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "kraken_signal_agent.log")),
    ],
)
logger = logging.getLogger("kraken_signal_agent")

# Load .env if present
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"'))

try:
    from creative_agent_bridge import CreativeAgentBridge
except ImportError:
    CreativeAgentBridge = None

try:
    from kraken_connector import KrakenConnector
except ImportError:
    KrakenConnector = None

# Configuration
KRAKEN_SIGNAL_ENABLED = os.environ.get("KRAKEN_SIGNAL_ENABLED", "1").lower() in ("1", "true", "yes")
KRAKEN_SCAN_INTERVAL_S = int(os.environ.get("KRAKEN_SCAN_INTERVAL_S", "30"))

# Pairs to scan
SCAN_PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD", "DOGE-USD"]

# Orderbook imbalance thresholds
IMBALANCE_BUY_THRESHOLD = 1.5   # bid_vol/ask_vol > 1.5 = BUY signal
IMBALANCE_SELL_THRESHOLD = 0.67  # bid_vol/ask_vol < 0.67 = SELL signal

# Volume anomaly: hardcoded 7-day rolling averages (USD volume, will calibrate later)
VOLUME_7D_AVERAGES = {
    "BTC-USD": 4500.0,   # ~4500 BTC/day on Kraken
    "ETH-USD": 40000.0,  # ~40k ETH/day
    "SOL-USD": 300000.0, # ~300k SOL/day
    "AVAX-USD": 500000.0,
    "LINK-USD": 800000.0,
    "DOGE-USD": 150000000.0,
}
VOLUME_ANOMALY_MULTIPLIER = 2.0  # 24h vol > 2x average = momentum signal

# Cross-venue divergence
KRAKEN_TAKER_FEE = 0.0026   # 0.26%
COINBASE_TAKER_FEE = 0.008  # 0.80%
MIN_DIVERGENCE_PCT = 0.003   # 0.3% gap after fees


def _fetch_coinbase_price(pair: str) -> float:
    """Fetch spot price from Coinbase public API.

    Args:
        pair: Standard pair (e.g., "BTC-USD")

    Returns:
        Spot price as float, or 0.0 on failure
    """
    try:
        url = f"https://api.coinbase.com/v2/prices/{pair}/spot"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "NetTrace-KrakenSignal/1.0")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        return float(data.get("data", {}).get("amount", 0))
    except Exception as e:
        logger.debug("Coinbase price fetch failed for %s: %s", pair, e)
        return 0.0


class KrakenSignalAgent:
    """Generates crypto trading signals from Kraken market data.

    Supports two modes:
        1. WebSocket (preferred) — sub-100ms streaming via kraken_websocket.py
        2. REST polling (fallback) — 30-second scan cycles via kraken_connector.py

    WebSocket mode registers callbacks that fire on every tick; a periodic
    REST fallback scan runs every 120 s in case WS messages stop arriving.
    """

    def __init__(self):
        self.bridge = CreativeAgentBridge("kraken_signal") if CreativeAgentBridge else None
        self.scan_count = 0
        self.scan_pairs = list(SCAN_PAIRS)
        self.scan_interval = KRAKEN_SCAN_INTERVAL_S
        self.running = True

        # WebSocket state (populated by _init_websocket)
        self.ws = None
        self._ws_mode = False

        # Track last WS-emitted signal per pair to avoid rapid duplicates
        self._ws_last_signal = {}  # pair -> (direction, timestamp)
        self._ws_signal_cooldown = 10  # seconds between same-pair WS signals

    # ------------------------------------------------------------------
    # WebSocket initialization and callbacks
    # ------------------------------------------------------------------

    def _init_websocket(self):
        """Try to initialize WebSocket for real-time feeds."""
        try:
            from kraken_websocket import KrakenWebSocketManager, KRAKEN_WS_PAIRS
            ws_pairs = [
                KRAKEN_WS_PAIRS.get(s, s.replace("-", "/"))
                for s in self.scan_pairs
            ]
            self.ws = KrakenWebSocketManager(pairs=ws_pairs)
            self.ws.on_ticker(self._on_ws_ticker)
            self.ws.on_book_update(self._on_ws_book)
            self.ws.on_trade(self._on_ws_trade)
            self.ws.start()
            self._ws_mode = True
            logger.info("WebSocket mode enabled -- real-time signals active")
        except Exception as e:
            logger.warning("WebSocket not available, using REST polling: %s", e)
            self._ws_mode = False

    def _ws_cooldown_ok(self, pair, direction):
        """Check whether enough time has passed since the last WS signal for this pair."""
        key = pair
        last = self._ws_last_signal.get(key)
        now = time.time()
        if last and last[0] == direction and (now - last[1]) < self._ws_signal_cooldown:
            return False
        return True

    def _ws_record_signal(self, pair, direction):
        """Record that a WS signal was emitted for cooldown tracking."""
        self._ws_last_signal[pair] = (direction, time.time())

    # --- Ticker callback (cross-venue divergence) ---

    def _on_ws_ticker(self, pair, ticker_data):
        """Real-time ticker callback -- check cross-venue divergence."""
        std_pair = pair.replace("/", "-")
        try:
            last_price = float(ticker_data.get("last", 0))
            if last_price > 0:
                self._check_cross_venue_divergence_rt(std_pair, last_price)
        except Exception as e:
            logger.debug("Ticker callback error: %s", e)

    def _check_cross_venue_divergence_rt(self, pair, kraken_price):
        """Real-time cross-venue divergence using WS Kraken price."""
        coinbase_price = _fetch_coinbase_price(pair)
        if coinbase_price == 0 or kraken_price == 0:
            return

        mid_price = (kraken_price + coinbase_price) / 2
        raw_gap_pct = abs(kraken_price - coinbase_price) / mid_price
        total_fees = KRAKEN_TAKER_FEE + COINBASE_TAKER_FEE
        net_gap_pct = raw_gap_pct - total_fees

        if net_gap_pct < MIN_DIVERGENCE_PCT:
            return

        if kraken_price < coinbase_price:
            direction = "BUY"
        else:
            direction = "SELL"

        if not self._ws_cooldown_ok(pair, direction):
            return

        confidence = min(0.90, 0.75 + net_gap_pct * 15)
        signal = {
            "pair": pair,
            "direction": direction,
            "confidence": round(confidence, 3),
            "urgency": "high",
            "reasoning": (
                f"RT cross-venue divergence: Kraken=${kraken_price:.2f} vs "
                f"Coinbase=${coinbase_price:.2f} "
                f"(raw={raw_gap_pct*100:.3f}%, net={net_gap_pct*100:.3f}% after fees)"
            ),
            "venue_hint": "kraken",
        }
        if self._emit_signal(signal):
            self._ws_record_signal(pair, direction)

    # --- Orderbook callback (imbalance) ---

    def _on_ws_book(self, pair, book_data):
        """Real-time orderbook callback -- check imbalance."""
        std_pair = pair.replace("/", "-")
        try:
            self._check_orderbook_imbalance_rt(std_pair, book_data)
        except Exception as e:
            logger.debug("Book callback error: %s", e)

    def _check_orderbook_imbalance_rt(self, pair, book_data):
        """Real-time orderbook imbalance using WS book snapshot/delta."""
        bids = book_data.get("bids", [])
        asks = book_data.get("asks", [])
        if not bids or not asks:
            return

        # WS book entries are dicts: {"price": X, "qty": Y}
        bid_vol = sum(float(b.get("qty", 0)) for b in bids)
        ask_vol = sum(float(a.get("qty", 0)) for a in asks)

        if ask_vol == 0:
            return

        ratio = bid_vol / ask_vol

        if ratio > IMBALANCE_BUY_THRESHOLD:
            direction = "BUY"
            confidence = min(0.90, 0.70 + (ratio - IMBALANCE_BUY_THRESHOLD) * 0.133)
        elif ratio < IMBALANCE_SELL_THRESHOLD:
            direction = "SELL"
            confidence = min(0.90, 0.70 + (IMBALANCE_SELL_THRESHOLD - ratio) * 0.588)
        else:
            return

        if not self._ws_cooldown_ok(pair, direction):
            return

        signal = {
            "pair": pair,
            "direction": direction,
            "confidence": round(confidence, 3),
            "urgency": "medium",
            "reasoning": (
                f"RT orderbook imbalance: bid/ask ratio {ratio:.2f} "
                f"(bids={bid_vol:.2f}, asks={ask_vol:.2f})"
            ),
        }
        if self._emit_signal(signal):
            self._ws_record_signal(pair, direction)

    # --- Trade callback (volume anomaly) ---

    def _on_ws_trade(self, pair, trade_data):
        """Real-time trade callback -- check volume anomaly."""
        std_pair = pair.replace("/", "-")
        try:
            self._check_volume_anomaly_rt(std_pair, trade_data)
        except Exception as e:
            logger.debug("Trade callback error: %s", e)

    def _check_volume_anomaly_rt(self, pair, trade_data):
        """Real-time volume anomaly detection using WS trade data.

        For individual trades we check if a single trade is unusually large
        (> 5% of the 7-day daily average volume).  This catches whale trades
        in real time rather than waiting for 24-hour aggregation.
        """
        avg_volume = VOLUME_7D_AVERAGES.get(pair, 0)
        if avg_volume == 0:
            return

        qty = float(trade_data.get("qty", 0))
        if qty == 0:
            return

        # Single-trade threshold: 5% of daily average = whale trade
        whale_threshold = avg_volume * 0.05
        if qty < whale_threshold:
            return

        side = trade_data.get("side", "").lower()
        direction = "BUY" if side == "buy" else "SELL"

        if not self._ws_cooldown_ok(pair, direction):
            return

        ratio = qty / avg_volume
        confidence = min(0.90, 0.72 + ratio * 0.5)
        signal = {
            "pair": pair,
            "direction": direction,
            "confidence": round(confidence, 3),
            "urgency": "high",
            "reasoning": (
                f"RT whale trade: {qty:.4f} = {ratio*100:.1f}% of daily avg "
                f"({avg_volume:.0f}), side={side}"
            ),
        }
        if self._emit_signal(signal):
            self._ws_record_signal(pair, direction)

    def scan_orderbook_imbalance(self, pair: str) -> dict:
        """Detect orderbook imbalance for directional signal.

        Computes bid_vol / ask_vol ratio from top-of-book.
        Ratio > 1.5 = BUY pressure, ratio < 0.67 = SELL pressure.

        Args:
            pair: Standard pair (e.g., "BTC-USD")

        Returns:
            Signal dict or None if no signal
        """
        if not KrakenConnector:
            return None

        book = KrakenConnector.get_orderbook(pair, depth=20)
        if "error" in book:
            return None

        bids = book.get("bids", [])
        asks = book.get("asks", [])

        if not bids or not asks:
            return None

        # Sum volumes: each entry is [price, volume, timestamp]
        bid_vol = sum(float(b[1]) for b in bids)
        ask_vol = sum(float(a[1]) for a in asks)

        if ask_vol == 0:
            return None

        ratio = bid_vol / ask_vol

        if ratio > IMBALANCE_BUY_THRESHOLD:
            # Scale confidence: ratio 1.5 -> 0.70, ratio 3.0 -> 0.90
            confidence = min(0.90, 0.70 + (ratio - IMBALANCE_BUY_THRESHOLD) * 0.133)
            return {
                "pair": pair,
                "direction": "BUY",
                "confidence": round(confidence, 3),
                "urgency": "medium",
                "reasoning": f"Orderbook imbalance: bid/ask ratio {ratio:.2f} (bids={bid_vol:.2f}, asks={ask_vol:.2f})",
            }
        elif ratio < IMBALANCE_SELL_THRESHOLD:
            # Scale confidence: ratio 0.67 -> 0.70, ratio 0.33 -> 0.90
            confidence = min(0.90, 0.70 + (IMBALANCE_SELL_THRESHOLD - ratio) * 0.588)
            return {
                "pair": pair,
                "direction": "SELL",
                "confidence": round(confidence, 3),
                "urgency": "medium",
                "reasoning": f"Orderbook imbalance: bid/ask ratio {ratio:.2f} (bids={bid_vol:.2f}, asks={ask_vol:.2f})",
            }

        return None

    def scan_volume_anomaly(self, pair: str) -> dict:
        """Detect volume anomaly (24h volume spike vs rolling average).

        If 24h volume > 2x the 7-day rolling average, emit a momentum signal.

        Args:
            pair: Standard pair (e.g., "BTC-USD")

        Returns:
            Signal dict or None if no signal
        """
        if not KrakenConnector:
            return None

        vol_data = KrakenConnector.get_24h_volume(pair)
        if "error" in vol_data:
            return None

        volume_24h = vol_data.get("volume_24h", 0)
        avg_volume = VOLUME_7D_AVERAGES.get(pair, 0)

        if avg_volume == 0 or volume_24h == 0:
            return None

        multiplier = volume_24h / avg_volume

        if multiplier >= VOLUME_ANOMALY_MULTIPLIER:
            # Determine direction from price action (high > low implies up)
            high = vol_data.get("price_24h_high", 0)
            low = vol_data.get("price_24h_low", 0)
            last = vol_data.get("last_price", 0)

            if high == 0 or low == 0 or last == 0:
                return None

            mid = (high + low) / 2
            direction = "BUY" if last > mid else "SELL"

            # Scale confidence: 2x -> 0.72, 5x -> 0.90
            confidence = min(0.90, 0.70 + (multiplier - VOLUME_ANOMALY_MULTIPLIER) * 0.067)
            return {
                "pair": pair,
                "direction": direction,
                "confidence": round(confidence, 3),
                "urgency": "high" if multiplier >= 3.0 else "medium",
                "reasoning": f"Volume anomaly: 24h vol {volume_24h:.0f} = {multiplier:.1f}x avg ({avg_volume:.0f})",
            }

        return None

    def scan_cross_venue_divergence(self, pair: str) -> dict:
        """Detect price divergence between Kraken and Coinbase.

        If price gap > 0.3% after fees (Kraken 0.26% taker + Coinbase 0.80% taker),
        emit an arb signal with venue_hint.

        Args:
            pair: Standard pair (e.g., "BTC-USD")

        Returns:
            Signal dict or None if no signal
        """
        if not KrakenConnector:
            return None

        # Get Kraken price
        kraken_data = KrakenConnector.get_24h_volume(pair)
        if "error" in kraken_data:
            return None
        kraken_price = kraken_data.get("last_price", 0)

        # Get Coinbase price
        coinbase_price = _fetch_coinbase_price(pair)

        if kraken_price == 0 or coinbase_price == 0:
            return None

        # Compute price gap
        mid_price = (kraken_price + coinbase_price) / 2
        raw_gap_pct = abs(kraken_price - coinbase_price) / mid_price

        # Subtract fees from both sides
        total_fees = KRAKEN_TAKER_FEE + COINBASE_TAKER_FEE
        net_gap_pct = raw_gap_pct - total_fees

        if net_gap_pct < MIN_DIVERGENCE_PCT:
            return None

        # Direction: buy where cheaper, sell where expensive
        if kraken_price < coinbase_price:
            direction = "BUY"  # Buy on Kraken (cheaper)
            venue_hint = "kraken"
        else:
            direction = "SELL"  # Sell on Kraken (more expensive) / buy on Coinbase
            venue_hint = "kraken"

        # Scale confidence: 0.3% net gap -> 0.75, 1% net gap -> 0.90
        confidence = min(0.90, 0.75 + net_gap_pct * 15)

        return {
            "pair": pair,
            "direction": direction,
            "confidence": round(confidence, 3),
            "urgency": "high",
            "reasoning": (
                f"Cross-venue divergence: Kraken=${kraken_price:.2f} vs Coinbase=${coinbase_price:.2f} "
                f"(raw={raw_gap_pct*100:.3f}%, net={net_gap_pct*100:.3f}% after fees)"
            ),
            "venue_hint": venue_hint,
        }

    def _emit_signal(self, signal: dict):
        """Push signal through CreativeAgentBridge."""
        if not signal:
            return False

        if self.bridge:
            ok = self.bridge.submit_signal(
                pair=signal["pair"],
                direction=signal["direction"],
                confidence=signal["confidence"],
                urgency=signal.get("urgency", "medium"),
                reasoning=signal.get("reasoning", ""),
                region_hint=signal.get("venue_hint"),
                market_type="crypto",
            )
            if ok:
                logger.info(
                    "Signal emitted: %s %s (conf=%.3f) -- %s",
                    signal["direction"], signal["pair"],
                    signal["confidence"], signal.get("reasoning", ""),
                )
            return ok
        else:
            logger.warning(
                "No bridge available, signal dropped: %s %s",
                signal["direction"], signal["pair"],
            )
            return False

    def run_scan_cycle(self):
        """Run one full scan cycle across all pairs and signal types (REST mode)."""
        if not KRAKEN_SIGNAL_ENABLED:
            return

        self.scan_count += 1
        signals_emitted = 0

        for pair in self.scan_pairs:
            try:
                # 1. Orderbook imbalance
                sig = self.scan_orderbook_imbalance(pair)
                if sig and self._emit_signal(sig):
                    signals_emitted += 1

                # 2. Volume anomaly
                sig = self.scan_volume_anomaly(pair)
                if sig and self._emit_signal(sig):
                    signals_emitted += 1

                # 3. Cross-venue divergence
                sig = self.scan_cross_venue_divergence(pair)
                if sig and self._emit_signal(sig):
                    signals_emitted += 1

            except Exception as e:
                logger.error("Error scanning %s: %s", pair, e, exc_info=True)

            # Small delay between pairs to avoid rate limiting
            time.sleep(0.5)

        if signals_emitted > 0:
            logger.info("Scan #%d complete: %d signals emitted", self.scan_count, signals_emitted)
        else:
            logger.debug("Scan #%d complete: no signals", self.scan_count)

    def run(self):
        """Run the signal agent in the best available mode.

        1. Try WebSocket mode (sub-100ms streaming).
        2. Fall back to REST polling (30-second cycles).

        In WebSocket mode the agent stays alive while callbacks handle
        real-time signals.  A periodic REST fallback scan runs every 120 s
        in case the WebSocket stream stalls.
        """
        self._init_websocket()

        if self._ws_mode:
            logger.info("Running in WebSocket mode (callbacks active)")
            while self.running:
                time.sleep(60)  # Keep-alive loop; callbacks do the work
                # If no WS messages for 120 s, run a REST fallback scan
                if self.ws and time.time() - self.ws.last_message_time > 120:
                    logger.warning(
                        "No WS messages for 120s, running REST fallback scan"
                    )
                    try:
                        self.run_scan_cycle()
                    except Exception as e:
                        logger.error("REST fallback scan error: %s", e)
        else:
            logger.info("Running in REST polling mode (interval=%ds)", self.scan_interval)
            while self.running:
                try:
                    self.run_scan_cycle()
                except Exception as e:
                    logger.error("Scan cycle error: %s", e, exc_info=True)
                time.sleep(self.scan_interval)

    def stop(self):
        """Signal the agent to stop."""
        self.running = False
        if self.ws:
            self.ws.stop()


def main():
    """Main loop for standalone Kraken signal agent."""
    agent = KrakenSignalAgent()
    logger.info(
        "Kraken Signal Agent started (interval=%ds, pairs=%s)",
        KRAKEN_SCAN_INTERVAL_S, ",".join(SCAN_PAIRS),
    )

    try:
        agent.run()
    except KeyboardInterrupt:
        logger.info("Shutting down Kraken Signal Agent")
        agent.stop()


if __name__ == "__main__":
    main()
