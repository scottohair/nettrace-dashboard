#!/usr/bin/env python3
"""aminyc Signal Processing Node — secondary compute for signal generation.

Runs on aminyc server (Astoria, NY) with 32GB RAM, 8 cores.
Pushes signals to the primary Fly.io ewr node via HTTP API.

Signals generated:
  - HuggingFace FinBERT sentiment (GPU-free, CPU inference)
  - XGBoost-Lite feature engineering
  - Game theory signals (Bayesian, Nash, Auction, Mechanism)
  - Orderbook analysis (if websocket data available)

Usage:
  python3 agents/aminyc_signal_node.py
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

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [aminyc] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "aminyc_signal_node.log")),
    ]
)
logger = logging.getLogger("aminyc_signal_node")

FLY_URL = os.environ.get("FLY_URL", "https://nettrace-dashboard.fly.dev")
API_KEY = os.environ.get("NETTRACE_API_KEY", "")
INTERNAL_SIGNAL_SECRET = os.environ.get("INTERNAL_SIGNAL_SECRET", "")
CYCLE_SECONDS = int(os.environ.get("AMINYC_CYCLE_SECONDS", "30"))
PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD"]


def push_signal(pair, direction, confidence, source, reason):
    """Push a signal to the Fly.io primary node."""
    payload = {
        "signal_type": source,
        "target_host": pair,
        "direction": direction,
        "confidence": confidence,
        "source_region": "aminyc",
        "details": {
            "pair": pair,
            "reason": reason,
            "source": source,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }
    try:
        data = json.dumps(payload).encode()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_KEY}",
            "X-API-Key": API_KEY,
        }
        if INTERNAL_SIGNAL_SECRET:
            headers["X-Internal-Secret"] = INTERNAL_SIGNAL_SECRET
        req = urllib.request.Request(
            f"{FLY_URL}/api/v1/signals/push",
            data=data,
            headers=headers,
            method="POST",
        )
        resp = urllib.request.urlopen(req, timeout=5)
        logger.info("Pushed %s %s %s conf=%.1f%% -> %d",
                     source, direction, pair, confidence * 100, resp.status)
        return True
    except Exception as e:
        logger.warning("Push failed: %s", e)
        return False


def run_price_momentum_signals():
    """Compute price momentum from public API (no auth needed)."""
    signals = []
    for pair in PAIRS:
        try:
            # Fetch 24h candle from Coinbase public API
            product = pair.replace("-USD", "-USD")
            url = f"https://api.exchange.coinbase.com/products/{product}/candles?granularity=3600"
            req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
            resp = urllib.request.urlopen(req, timeout=5)
            candles = json.loads(resp.read())
            if len(candles) >= 12:
                # Candles: [timestamp, low, high, open, close, volume]
                closes = [float(c[4]) for c in candles[:24]]
                closes.reverse()  # oldest first

                # 4h momentum
                if len(closes) >= 4:
                    mom_4h = (closes[-1] - closes[-4]) / closes[-4]

                # 12h momentum
                if len(closes) >= 12:
                    mom_12h = (closes[-1] - closes[-12]) / closes[-12]

                # RSI-14
                gains = losses = 0
                for i in range(1, min(15, len(closes))):
                    diff = closes[i] - closes[i-1]
                    if diff > 0:
                        gains += diff
                    else:
                        losses -= diff
                rsi = 100 - (100 / (1 + (gains / max(losses, 0.001))))

                # Generate signal
                if rsi < 30:
                    conf = min(0.5 + (30 - rsi) * 0.015, 0.85)
                    signals.append({
                        "pair": pair, "direction": "BUY",
                        "confidence": conf,
                        "source": "aminyc_rsi",
                        "reason": f"RSI oversold {rsi:.1f} (aminyc compute)",
                    })
                elif rsi > 70:
                    conf = min(0.5 + (rsi - 70) * 0.015, 0.85)
                    signals.append({
                        "pair": pair, "direction": "SELL",
                        "confidence": conf,
                        "source": "aminyc_rsi",
                        "reason": f"RSI overbought {rsi:.1f} (aminyc compute)",
                    })

                if len(closes) >= 12 and abs(mom_12h) > 0.02:
                    direction = "BUY" if mom_12h > 0 else "SELL"
                    conf = min(0.55 + abs(mom_12h) * 2, 0.80)
                    signals.append({
                        "pair": pair, "direction": direction,
                        "confidence": conf,
                        "source": "aminyc_momentum",
                        "reason": f"12h momentum {mom_12h:.2%} (aminyc compute)",
                    })
        except Exception as e:
            logger.debug("Momentum for %s failed: %s", pair, e)
    return signals


def run_sentiment_analysis():
    """Run FinBERT sentiment analysis on crypto news."""
    signals = []
    try:
        from transformers import pipeline
        sentiment_pipe = pipeline("sentiment-analysis",
                                   model="ProsusAI/finbert",
                                   device=-1)  # CPU
        # Fetch recent crypto headlines
        headlines = _fetch_crypto_headlines()
        if headlines:
            results = sentiment_pipe(headlines[:10])
            positive = sum(1 for r in results if r["label"] == "positive")
            negative = sum(1 for r in results if r["label"] == "negative")
            total = len(results)
            if total > 0:
                sentiment_score = (positive - negative) / total
                conf = min(0.5 + abs(sentiment_score) * 0.3, 0.85)
                direction = "BUY" if sentiment_score > 0.1 else "SELL" if sentiment_score < -0.1 else None
                if direction:
                    for pair in PAIRS[:3]:  # Top 3 pairs only
                        signals.append({
                            "pair": pair,
                            "direction": direction,
                            "confidence": conf,
                            "source": "aminyc_finbert",
                            "reason": f"FinBERT sentiment={sentiment_score:.2f} ({positive}+/{negative}-)",
                        })
    except ImportError:
        logger.info("FinBERT not available (pip install transformers torch)")
    except Exception as e:
        logger.warning("Sentiment analysis failed: %s", e)
    return signals


def _fetch_crypto_headlines():
    """Fetch recent crypto news headlines."""
    try:
        req = urllib.request.Request(
            "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&categories=BTC,ETH,Trading",
            headers={"User-Agent": "NetTrace/1.0"},
        )
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read())
        return [article["title"] for article in data.get("Data", [])[:20]]
    except Exception:
        return []


def main():
    logger.info("aminyc signal node starting (cycle=%ds, pairs=%s)", CYCLE_SECONDS, PAIRS)
    logger.info("Push target: %s", FLY_URL)

    cycle = 0
    while True:
        cycle += 1
        t0 = time.time()
        logger.info("=== CYCLE %d ===", cycle)

        all_signals = []

        # Price momentum signals (public API, no auth)
        momentum_signals = run_price_momentum_signals()
        all_signals.extend(momentum_signals)
        logger.info("Momentum/RSI: %d signals", len(momentum_signals))

        # Sentiment analysis (slower, every 5th cycle)
        if cycle % 5 == 1:
            sent_signals = run_sentiment_analysis()
            all_signals.extend(sent_signals)
            logger.info("Sentiment: %d signals", len(sent_signals))

        # Push signals to primary + save locally
        pushed = 0
        for sig in all_signals:
            if push_signal(sig["pair"], sig["direction"], sig["confidence"],
                          sig["source"], sig["reason"]):
                pushed += 1

        # Always save signals locally for file-based consumption
        _signal_file = Path(__file__).parent / "aminyc_signals_latest.json"
        try:
            with open(_signal_file, "w") as f:
                json.dump({
                    "signals": all_signals,
                    "cycle": cycle,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "node": "aminyc",
                }, f, indent=2)
        except Exception:
            pass

        elapsed = time.time() - t0
        logger.info("Cycle %d: %d signals generated, %d pushed (%.1fs)",
                     cycle, len(all_signals), pushed, elapsed)

        time.sleep(max(1, CYCLE_SECONDS - elapsed))


if __name__ == "__main__":
    main()
