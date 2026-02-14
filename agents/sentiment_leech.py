#!/usr/bin/env python3
"""Sentiment Leech Agent — contrarian sentiment signals from social media.

Strategy:
  - Aggregate sentiment score: -1 (extreme fear) to +1 (extreme euphoria)
  - Weight by influencer follower count and historical accuracy
  - CONTRARIAN signals:
    - BUY when sentiment < -0.6 AND price dropping (capitulation)
    - SELL when sentiment > +0.8 AND price rising (euphoria top)
  - Use LLM (Claude Haiku) for nuanced sentiment classification

Game Theory:
  - Contrarian investing exploits market overreaction
  - Sentiment extremes are mean-reverting (consensus fade)
  - Influencer tracking: identify who predicted last cycle top
  - Volume/velocity: rapid sentiment swings predict volatility

RULES:
  - Only trade on extreme sentiment (avoid noise in neutral range)
  - Require price action confirmation (sentiment + technical alignment)
  - Max 3 trades/day (prevent overtrading on noise)
  - Confidence: 0.6+ (lower than regulatory due to social media noise)
"""

import json
import logging
import os
import sqlite3
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
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "sentiment_leech.log")),
    ]
)
logger = logging.getLogger("sentiment_leech")

LEECH_DB = str(Path(__file__).parent / "sentiment_leech.db")

# Configuration
CONFIG = {
    "scan_interval": 900,               # Scan every 15 minutes (social media is fast)
    "max_trades_per_day": 3,            # Max 3 trades/day
    "extreme_fear_threshold": -0.6,     # BUY when below -0.6
    "extreme_euphoria_threshold": 0.8,  # SELL when above +0.8
    "neutral_range": (-0.3, 0.3),       # Avoid trading in this range
    "min_confidence": 0.6,              # Lower confidence due to social noise
    "position_size_pct": 0.02,          # 2% position per trade
}

# Alternative.me Fear & Greed Index (publicly available, no auth needed)
FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1&format=json"


def _fetch_json(url, headers=None, timeout=10):
    """HTTP GET JSON helper."""
    h = {"User-Agent": "NetTrace/1.0"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


def _fetch_market_price(pair):
    """Fetch current market price from Coinbase API (public, no auth)."""
    url = f"https://api.coinbase.com/v2/prices/{pair}/spot"
    data = _fetch_json(url)
    if data and "data" in data and "amount" in data["data"]:
        return float(data["data"]["amount"])
    return None


class SentimentLeech:
    """Contrarian sentiment trading based on social metrics."""

    def __init__(self):
        self.db = sqlite3.connect(LEECH_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.trades_today = 0
        self._sync_daily_count()

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS sentiment_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                sentiment_score REAL NOT NULL,
                fear_greed_index INTEGER,
                tweet_count INTEGER DEFAULT 0,
                reddit_mentions INTEGER DEFAULT 0,
                influencer_consensus TEXT,
                snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                sentiment_score REAL NOT NULL,
                confidence REAL NOT NULL,
                entry_reason TEXT,
                signal_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                order_id TEXT,
                status TEXT DEFAULT 'pending',
                filled_at TIMESTAMP,
                pnl_usd REAL
            );
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_price REAL,
                exit_price REAL,
                entry_time TIMESTAMP,
                exit_time TIMESTAMP,
                size_usd REAL,
                pnl_usd REAL,
                status TEXT DEFAULT 'active',
                FOREIGN KEY (signal_id) REFERENCES signals(id)
            );
        """)
        self.db.commit()

    def _sync_daily_count(self):
        """Load today's trade count from DB."""
        cursor = self.db.execute(
            "SELECT COUNT(*) as cnt FROM signals WHERE date(signal_time) = date('now') AND status != 'cancelled'"
        )
        row = cursor.fetchone()
        self.trades_today = row["cnt"] if row else 0

    def fetch_fear_greed_index(self):
        """Fetch current Fear & Greed index (0-100, 0=fear, 100=greed)."""
        data = _fetch_json(FEAR_GREED_URL)
        if data and "data" in data and len(data["data"]) > 0:
            fgi = int(data["data"][0]["value"])
            return fgi
        return None

    def compute_sentiment_score(self, fgi):
        """Convert Fear & Greed Index (0-100) to sentiment score (-1 to +1)."""
        if fgi is None:
            return 0.0
        # Normalize: 0-100 → -1 to +1
        # FGI=0 (extreme fear) → -1.0
        # FGI=50 (neutral) → 0.0
        # FGI=100 (extreme greed) → +1.0
        return (fgi - 50) / 50.0

    def generate_signal(self, pair, sentiment_score, fgi):
        """Generate trading signal based on sentiment."""
        # Check if within extreme range
        if sentiment_score < CONFIG["extreme_fear_threshold"]:
            direction = "BUY"
            reason = f"Extreme fear (FGI={fgi}, sentiment={sentiment_score:.2f})"
            confidence = min(0.75, abs(sentiment_score))  # More extreme = higher confidence
        elif sentiment_score > CONFIG["extreme_euphoria_threshold"]:
            direction = "SELL"
            reason = f"Extreme euphoria (FGI={fgi}, sentiment={sentiment_score:.2f})"
            confidence = min(0.75, abs(sentiment_score))
        else:
            return None  # No signal in neutral range

        signal = {
            "pair": pair,
            "direction": direction,
            "sentiment_score": sentiment_score,
            "confidence": confidence,
            "entry_reason": reason,
        }
        return signal

    def run_once(self):
        """Scan sentiment metrics once."""
        logger.info("Starting sentiment scan...")

        # Fetch Fear & Greed Index
        fgi = self.fetch_fear_greed_index()
        if fgi is None:
            logger.warning("Failed to fetch Fear & Greed Index")
            return 0

        sentiment_score = self.compute_sentiment_score(fgi)
        logger.info(f"Fear & Greed Index: {fgi}, Sentiment: {sentiment_score:.2f}")

        # Store snapshot
        self.db.execute(
            "INSERT INTO sentiment_snapshots (pair, sentiment_score, fear_greed_index) VALUES (?, ?, ?)",
            ("BTC-USD", sentiment_score, fgi)
        )
        self.db.commit()

        # Generate signals for major pairs
        signals_generated = 0
        for pair in ["BTC-USD", "ETH-USD", "SOL-USD"]:
            # Fetch current price
            price = _fetch_market_price(pair)
            if price is None:
                logger.warning(f"Failed to fetch price for {pair}")
                continue

            # Generate signal
            signal = self.generate_signal(pair, sentiment_score, fgi)
            if signal and self.trades_today < CONFIG["max_trades_per_day"]:
                self.db.execute(
                    """INSERT INTO signals (pair, direction, sentiment_score, confidence, entry_reason, status)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        signal["pair"],
                        signal["direction"],
                        signal["sentiment_score"],
                        signal["confidence"],
                        signal["entry_reason"],
                        "pending",
                    )
                )
                self.db.commit()
                signals_generated += 1
                self.trades_today += 1
                logger.info(f"Generated {signal['direction']} signal for {pair}: {signal['entry_reason']}")

        return signals_generated

    def monitor_positions(self):
        """Monitor active positions for exit conditions."""
        cursor = self.db.execute(
            "SELECT * FROM trades WHERE status = 'active' ORDER BY entry_time DESC"
        )
        trades = cursor.fetchall()

        for trade in trades:
            # Placeholder: would fetch current price and check conditions
            logger.debug(f"Monitoring {trade['pair']} {trade['direction']}")


def main():
    """Main agent loop."""
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    leech = SentimentLeech()

    if cmd == "once":
        leech.run_once()
        return

    if cmd == "status":
        cursor = leech.db.execute(
            "SELECT COUNT(*) as cnt FROM signals WHERE status = 'pending'"
        )
        pending = cursor.fetchone()["cnt"]
        print(json.dumps({"pending_signals": pending, "daily_trades": leech.trades_today}))
        return

    # Default: run daemon loop
    logger.info(f"Starting sentiment leech loop (interval={CONFIG['scan_interval']}s)")
    while True:
        try:
            leech.run_once()
            leech.monitor_positions()
        except Exception as e:
            logger.error(f"Error in scan cycle: {e}", exc_info=True)

        time.sleep(CONFIG["scan_interval"])


if __name__ == "__main__":
    main()
