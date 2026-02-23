#!/usr/bin/env python3
"""Equity Signal Agent — generates trading signals for US equities.

Signal types:
  1. Crypto-correlated equities: When BTC/ETH signals fire, also signal
     correlated stocks (COIN, MSTR, RIOT, MARA).
  2. Momentum: SMA(20) > SMA(50) crossover on daily bars.
  3. Mean reversion: RSI extreme on SPY/QQQ.

Signals are pushed via CreativeAgentBridge with market_type="equity".
Only generates signals during market hours (checked by GoalValidator).
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
    format="%(asctime)s [EQUITY_SIGNAL] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "equity_signal_agent.log")),
    ],
)
logger = logging.getLogger("equity_signal_agent")

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

try:
    from agent_goals import GoalValidator
except ImportError:
    GoalValidator = None

try:
    from creative_agent_bridge import CreativeAgentBridge
except ImportError:
    CreativeAgentBridge = None

EQUITY_SIGNAL_ENABLED = os.environ.get("EQUITY_SIGNAL_ENABLED", "1").lower() in ("1", "true", "yes")
EQUITY_SCAN_INTERVAL_S = int(os.environ.get("EQUITY_SCAN_INTERVAL_S", "60"))

# Crypto-correlated equity tickers
CRYPTO_CORRELATED = {
    "BTC": ["COIN", "MSTR", "RIOT", "MARA"],
    "ETH": ["COIN"],
}

# Equity universe for momentum/mean-reversion signals
EQUITY_UNIVERSE = ["SPY", "QQQ", "AAPL", "TSLA", "COIN", "MSTR", "NVDA", "AMD", "META"]


def _fetch_yahoo_chart(symbol, interval="1d", range_str="3mo"):
    """Fetch price history from Yahoo Finance chart API.

    Returns list of dicts with keys: timestamp, open, high, low, close, volume.
    """
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?interval={interval}&range={range_str}"
    )
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "NetTrace-EquitySignal/1.0")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())

        result = data.get("chart", {}).get("result", [])
        if not result:
            return []

        timestamps = result[0].get("timestamp", [])
        indicators = result[0].get("indicators", {}).get("quote", [{}])[0]
        opens = indicators.get("open", [])
        highs = indicators.get("high", [])
        lows = indicators.get("low", [])
        closes = indicators.get("close", [])
        volumes = indicators.get("volume", [])

        candles = []
        for i in range(len(timestamps)):
            if closes[i] is not None:
                candles.append({
                    "timestamp": timestamps[i],
                    "open": opens[i],
                    "high": highs[i],
                    "low": lows[i],
                    "close": closes[i],
                    "volume": volumes[i] or 0,
                })
        return candles
    except Exception as e:
        logger.debug("Yahoo chart fetch failed for %s: %s", symbol, e)
        return []


def _sma(prices, period):
    """Simple moving average."""
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period


def _rsi(prices, period=14):
    """Relative Strength Index."""
    if len(prices) < period + 1:
        return None
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    recent = deltas[-period:]
    gains = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]
    avg_gain = sum(gains) / period if gains else 0
    avg_loss = sum(losses) / period if losses else 0.001
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


class EquitySignalAgent:
    """Generates equity signals and pushes them through the bridge."""

    def __init__(self):
        self.last_crypto_signals = {}  # pair -> (direction, timestamp)
        self.bridge = CreativeAgentBridge("equity_signal") if CreativeAgentBridge else None

    def check_market_hours(self):
        """Return True if equity market is open."""
        if GoalValidator and hasattr(GoalValidator, "is_equity_market_open"):
            return GoalValidator.is_equity_market_open()
        return False

    def on_crypto_signal(self, pair, direction, confidence):
        """Called when a crypto signal fires — checks for correlated equities.

        Args:
            pair: Crypto pair (e.g., "BTC-USD")
            direction: "BUY" or "SELL"
            confidence: Signal confidence 0-1
        """
        if not self.check_market_hours():
            return

        base = pair.split("-")[0].upper() if pair else ""
        correlated = CRYPTO_CORRELATED.get(base, [])

        for ticker in correlated:
            # Slightly lower confidence for correlated signal
            adj_confidence = confidence * 0.85
            if adj_confidence < 0.70:
                continue

            self._emit_signal(
                symbol=ticker,
                direction=direction,
                confidence=adj_confidence,
                urgency="medium",
                reasoning=f"Crypto-correlated: {pair} {direction} @ {confidence:.2f}",
            )

    def scan_momentum(self):
        """Scan equity universe for SMA crossover signals."""
        if not self.check_market_hours():
            return []

        signals = []
        for symbol in EQUITY_UNIVERSE:
            candles = _fetch_yahoo_chart(symbol, interval="1d", range_str="3mo")
            if len(candles) < 50:
                continue

            closes = [c["close"] for c in candles]
            sma_20 = _sma(closes, 20)
            sma_50 = _sma(closes, 50)

            if sma_20 is None or sma_50 is None:
                continue

            # Check if SMA20 just crossed above SMA50 (golden cross)
            prev_closes = closes[:-1]
            prev_sma_20 = _sma(prev_closes, 20)
            prev_sma_50 = _sma(prev_closes, 50)

            if prev_sma_20 is None or prev_sma_50 is None:
                continue

            if prev_sma_20 <= prev_sma_50 and sma_20 > sma_50:
                # Golden cross — bullish
                confidence = min(0.85, 0.70 + (sma_20 - sma_50) / sma_50 * 10)
                signals.append({
                    "symbol": symbol,
                    "direction": "BUY",
                    "confidence": confidence,
                    "reasoning": f"Momentum: SMA20 crossed above SMA50 ({sma_20:.2f} > {sma_50:.2f})",
                })
            elif prev_sma_20 >= prev_sma_50 and sma_20 < sma_50:
                # Death cross — bearish
                confidence = min(0.80, 0.70 + (sma_50 - sma_20) / sma_50 * 10)
                signals.append({
                    "symbol": symbol,
                    "direction": "SELL",
                    "confidence": confidence,
                    "reasoning": f"Momentum: SMA20 crossed below SMA50 ({sma_20:.2f} < {sma_50:.2f})",
                })

        for sig in signals:
            self._emit_signal(**sig)

        return signals

    def scan_mean_reversion(self):
        """Scan SPY/QQQ for RSI extremes."""
        if not self.check_market_hours():
            return []

        signals = []
        for symbol in ["SPY", "QQQ"]:
            candles = _fetch_yahoo_chart(symbol, interval="1d", range_str="1mo")
            if len(candles) < 15:
                continue

            closes = [c["close"] for c in candles]
            rsi = _rsi(closes, 14)

            if rsi is None:
                continue

            if rsi < 30:
                confidence = min(0.85, 0.70 + (30 - rsi) / 30 * 0.15)
                signals.append({
                    "symbol": symbol,
                    "direction": "BUY",
                    "confidence": confidence,
                    "reasoning": f"Mean reversion: RSI={rsi:.1f} (oversold <30)",
                })
            elif rsi > 70:
                confidence = min(0.80, 0.70 + (rsi - 70) / 30 * 0.10)
                signals.append({
                    "symbol": symbol,
                    "direction": "SELL",
                    "confidence": confidence,
                    "reasoning": f"Mean reversion: RSI={rsi:.1f} (overbought >70)",
                })

        for sig in signals:
            self._emit_signal(**sig)

        return signals

    def _emit_signal(self, symbol, direction, confidence, urgency="medium", reasoning=""):
        """Push equity signal through the bridge."""
        pair = f"{symbol}-USD"
        if self.bridge:
            ok = self.bridge.submit_signal(
                pair=pair,
                direction=direction,
                confidence=confidence,
                urgency=urgency,
                reasoning=reasoning,
                market_type="equity",
            )
            if ok:
                logger.info("Equity signal: %s %s (conf=%.2f) — %s", direction, pair, confidence, reasoning)
        else:
            logger.warning("No bridge available, equity signal dropped: %s %s", direction, pair)

    def run_scan_cycle(self):
        """Run one full scan cycle."""
        if not EQUITY_SIGNAL_ENABLED:
            return

        if not self.check_market_hours():
            logger.debug("Market closed, skipping equity scan")
            return

        logger.info("Running equity signal scan...")
        self.scan_momentum()
        self.scan_mean_reversion()


def main():
    """Main loop for standalone equity signal agent."""
    agent = EquitySignalAgent()
    logger.info("Equity Signal Agent started (interval=%ds)", EQUITY_SCAN_INTERVAL_S)

    while True:
        try:
            agent.run_scan_cycle()
        except Exception as e:
            logger.error("Scan cycle error: %s", e, exc_info=True)
        time.sleep(EQUITY_SCAN_INTERVAL_S)


if __name__ == "__main__":
    main()
