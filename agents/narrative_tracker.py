#!/usr/bin/env python3
"""Narrative Tracker Agent — memetic trading on theme lifecycle.

Strategy:
  - Define 20+ narrative themes (AI, gaming, DeFi 2.0, L2s, restaking, RWA, etc.)
  - Score narrative strength: Google Trends slope + mentions + market cap growth
  - Lifecycle detection: Birth (hockey stick) → Growth → Saturation → Death
  - LONG when narrative entering hockey stick phase (S-curve inflection)
  - SHORT when narrative peaks (everyone talking = top)

Game Theory:
  - Memetic spread: ideas propagate in S-curve adoption patterns
  - Consensus fade: what's obvious to everyone is already priced in
  - Early mover: identify narratives in inflection before mainstream
  - Momentum: ride trend until saturation

RULES:
  - Only trade top 3 trending narratives (avoid noise)
  - Require 7-day momentum confirmation (avoid false starts)
  - Exit on narrative death (Google Trends drops >30%)
  - Max 5 simultaneous narrative positions
"""

import json
import logging
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
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
        logging.FileHandler(str(Path(__file__).parent / "narrative_tracker.log")),
    ]
)
logger = logging.getLogger("narrative_tracker")

TRACKER_DB = str(Path(__file__).parent / "narrative_tracker.db")

# Narrative themes and associated tokens
NARRATIVES = {
    "AI": {"tokens": ["RENDER", "FET", "AGIX", "SUI"], "category": "ai"},
    "RWA": {"tokens": ["MKR", "UNI", "COMP", "AAVE"], "category": "defi"},
    "Gaming": {"tokens": ["AXS", "ENJ", "SAND", "BLUR"], "category": "gaming"},
    "L2s": {"tokens": ["OP", "ARB", "STRK", "LINA"], "category": "scaling"},
    "Restaking": {"tokens": ["EIGEN", "LSD"], "category": "staking"},
    "DeFi 2.0": {"tokens": ["AAVE", "UNI", "COMP", "LIDO"], "category": "defi"},
    "Modular": {"tokens": ["MEME", "OP", "ARB", "STRK"], "category": "scaling"},
    "Solana": {"tokens": ["SOL", "JUP", "MAGIC", "COPE"], "category": "l1"},
    "Bitcoin Ordinals": {"tokens": ["BTC", "ORDI", "SATS"], "category": "bitcoin"},
    "Quantum": {"tokens": ["IQT", "QUBT"], "category": "emerging"},
}

# Configuration
CONFIG = {
    "scan_interval": 3600,              # Scan every 1 hour (narratives move slowly)
    "max_positions": 5,                 # Max 5 simultaneous narrative positions
    "min_confidence": 0.6,              # Minimum confidence for entry
    "position_size_pct": 0.03,          # 3% position per narrative
    "momentum_days": 7,                 # 7-day momentum confirmation
    "lifecycle_threshold": {
        "birth": 0.4,                   # Rapid growth = birth stage
        "growth": 0.6,                  # Sustained growth
        "saturation": 0.8,              # Plateau or decline
        "death": -0.3,                  # Significant decline
    },
}


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


def _fetch_coingecko_market_cap(token_id):
    """Fetch token market cap from CoinGecko."""
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={token_id}&vs_currencies=usd&include_market_cap=true"
    data = _fetch_json(url)
    if data and token_id in data and "usd_market_cap" in data[token_id]:
        return data[token_id]["usd_market_cap"]
    return None


class NarrativeTracker:
    """Track narrative lifecycle and generate memetic trading signals."""

    def __init__(self):
        self.db = sqlite3.connect(TRACKER_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.active_positions = 0

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS narratives (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                category TEXT,
                google_trends_score INTEGER DEFAULT 0,
                momentum_7d REAL DEFAULT 0.0,
                twitter_mentions INTEGER DEFAULT 0,
                top_tokens_json TEXT,
                lifecycle_stage TEXT DEFAULT 'unknown',
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS narrative_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                narrative_id INTEGER,
                google_trends_score INTEGER,
                momentum REAL,
                twitter_mentions INTEGER,
                total_market_cap REAL,
                snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (narrative_id) REFERENCES narratives(id)
            );
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                narrative_id INTEGER,
                direction TEXT NOT NULL,
                confidence REAL NOT NULL,
                lifecycle_stage TEXT,
                entry_reason TEXT,
                signal_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                order_id TEXT,
                status TEXT DEFAULT 'pending',
                filled_at TIMESTAMP,
                FOREIGN KEY (narrative_id) REFERENCES narratives(id)
            );
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                narrative_id INTEGER,
                tokens_json TEXT,
                entry_price REAL,
                entry_time TIMESTAMP,
                exit_price REAL,
                exit_time TIMESTAMP,
                pnl_usd REAL,
                status TEXT DEFAULT 'active',
                FOREIGN KEY (narrative_id) REFERENCES narratives(id)
            );
        """)
        self.db.commit()

        # Initialize narratives
        for name, data in NARRATIVES.items():
            cursor = self.db.execute("SELECT id FROM narratives WHERE name = ?", (name,))
            if not cursor.fetchone():
                self.db.execute(
                    "INSERT INTO narratives (name, category, top_tokens_json) VALUES (?, ?, ?)",
                    (name, data["category"], json.dumps(data["tokens"]))
                )
        self.db.commit()

    def score_narrative_momentum(self, narrative_name):
        """Score narrative momentum using available public metrics."""
        # In production: use Google Trends API, Twitter API v2, CoinGecko
        # For now: simulate with random variation
        import random
        score = random.uniform(-0.5, 1.0)
        return score

    def detect_lifecycle_stage(self, narrative_id):
        """Detect narrative lifecycle stage from recent snapshots."""
        cursor = self.db.execute(
            """SELECT momentum FROM narrative_snapshots WHERE narrative_id = ?
               ORDER BY snapshot_time DESC LIMIT 7""",
            (narrative_id,)
        )
        snapshots = cursor.fetchall()
        if not snapshots:
            return "unknown", 0.0

        momentums = [s["momentum"] for s in snapshots]
        avg_momentum = sum(momentums) / len(momentums)

        # Detect stage
        if avg_momentum > CONFIG["lifecycle_threshold"]["birth"]:
            stage = "birth"
        elif avg_momentum > CONFIG["lifecycle_threshold"]["growth"]:
            stage = "growth"
        elif avg_momentum > CONFIG["lifecycle_threshold"]["saturation"]:
            stage = "saturation"
        elif avg_momentum < CONFIG["lifecycle_threshold"]["death"]:
            stage = "death"
        else:
            stage = "stable"

        return stage, avg_momentum

    def generate_signal(self, narrative_id, narrative_name, stage, momentum):
        """Generate trading signal based on narrative lifecycle."""
        if stage == "birth" and momentum > 0.4:
            # Birth stage: rapid growth = BUY
            direction = "LONG"
            confidence = min(0.8, momentum)
            reason = f"Early {narrative_name} adoption (hockey stick)"
        elif stage == "saturation" and momentum > 0.7:
            # Saturation: everyone talking = TOP = SHORT
            direction = "SHORT"
            confidence = 0.7
            reason = f"{narrative_name} saturation (consensus fade)"
        elif stage == "death":
            # Death stage: exit positions
            direction = "SELL"
            confidence = 0.8
            reason = f"{narrative_name} narrative death"
        else:
            return None

        signal = {
            "narrative_id": narrative_id,
            "direction": direction,
            "confidence": confidence,
            "lifecycle_stage": stage,
            "entry_reason": reason,
        }
        return signal

    def run_once(self):
        """Scan narrative metrics and generate signals."""
        logger.info("Starting narrative scan...")

        signals_generated = 0
        for narrative_name, data in NARRATIVES.items():
            cursor = self.db.execute("SELECT id FROM narratives WHERE name = ?", (narrative_name,))
            row = cursor.fetchone()
            if not row:
                continue

            narrative_id = row["id"]

            # Score narrative momentum
            momentum = self.score_narrative_momentum(narrative_name)

            # Store snapshot
            self.db.execute(
                """INSERT INTO narrative_snapshots (narrative_id, google_trends_score, momentum)
                   VALUES (?, ?, ?)""",
                (narrative_id, int(momentum * 100), momentum)
            )
            self.db.commit()

            # Detect lifecycle stage
            stage, avg_momentum = self.detect_lifecycle_stage(narrative_id)

            # Update narrative lifecycle
            self.db.execute(
                "UPDATE narratives SET lifecycle_stage = ?, momentum_7d = ? WHERE id = ?",
                (stage, avg_momentum, narrative_id)
            )
            self.db.commit()

            logger.info(f"{narrative_name}: stage={stage}, momentum={momentum:.2f}")

            # Generate signal
            signal = self.generate_signal(narrative_id, narrative_name, stage, momentum)
            if signal and self.active_positions < CONFIG["max_positions"]:
                self.db.execute(
                    """INSERT INTO signals (narrative_id, direction, confidence, lifecycle_stage, entry_reason, status)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        signal["narrative_id"],
                        signal["direction"],
                        signal["confidence"],
                        signal["lifecycle_stage"],
                        signal["entry_reason"],
                        "pending",
                    )
                )
                self.db.commit()
                signals_generated += 1
                self.active_positions += 1
                logger.info(f"Generated {signal['direction']} signal for {narrative_name}")

        # Check active positions
        cursor = self.db.execute(
            "SELECT COUNT(*) as cnt FROM positions WHERE status = 'active'"
        )
        self.active_positions = cursor.fetchone()["cnt"]

        logger.info(f"Generated {signals_generated} signals. Active positions: {self.active_positions}/{CONFIG['max_positions']}")
        return signals_generated

    def monitor_positions(self):
        """Monitor active narrative positions for exit."""
        cursor = self.db.execute(
            """SELECT p.*, n.lifecycle_stage FROM positions p
               JOIN narratives n ON p.narrative_id = n.id
               WHERE p.status = 'active'"""
        )
        positions = cursor.fetchall()

        for pos in positions:
            # Exit if narrative entered death stage
            if pos["lifecycle_stage"] == "death":
                logger.info(f"Exiting {pos['narrative_id']} - narrative death detected")


def main():
    """Main agent loop."""
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    tracker = NarrativeTracker()

    if cmd == "once":
        tracker.run_once()
        return

    if cmd == "status":
        cursor = tracker.db.execute(
            "SELECT COUNT(*) as cnt FROM signals WHERE status = 'pending'"
        )
        pending = cursor.fetchone()["cnt"]
        cursor = tracker.db.execute(
            "SELECT COUNT(*) as cnt FROM positions WHERE status = 'active'"
        )
        active = cursor.fetchone()["cnt"]
        print(json.dumps({"pending_signals": pending, "active_positions": active}))
        return

    # Default: run daemon loop
    logger.info(f"Starting narrative tracker loop (interval={CONFIG['scan_interval']}s)")
    while True:
        try:
            tracker.run_once()
            tracker.monitor_positions()
        except Exception as e:
            logger.error(f"Error in scan cycle: {e}", exc_info=True)

        time.sleep(CONFIG["scan_interval"])


if __name__ == "__main__":
    main()
