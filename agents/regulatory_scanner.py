#!/usr/bin/env python3
"""Regulatory Scanner Agent — front-run compliance changes.

Strategy:
  - Monitor SEC, CFTC, Federal Reserve RSS/API feeds
  - Parse for regulatory keywords: stablecoin, leverage, custody, DeFi, securities
  - Score impact on affected tokens/venues
  - Generate signals BEFORE market reacts (6-48 hour window)
  - Example: "SEC threatens stablecoin regulation" → SHORT perpetual futures heavy in stablecoins

Game Theory:
  - Information asymmetry: we have early regulatory data access
  - Front-running: first mover advantage on policy arbitrage
  - Risk: regulatory changes are definitive (not opinions), so confidence is high (0.75+)

RULES:
  - Confidence threshold: 0.75+ (regulatory moves are high-conviction)
  - Position size: 2-5% of portfolio (concentrated bets on policy arb)
  - Exit: Trailing stop at 1% (fast exits on invalidation)
  - Max 3 trades/week (avoid overexposure to single announcements)
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
from xml.etree import ElementTree as ET

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
        logging.FileHandler(str(Path(__file__).parent / "regulatory_scanner.log")),
    ]
)
logger = logging.getLogger("regulatory_scanner")

SCANNER_DB = str(Path(__file__).parent / "regulatory_scanner.db")

# Regulatory keywords and their impact
REGULATORY_KEYWORDS = {
    "stablecoin": {"impact_score": 0.8, "affected_tokens": ["USDC", "USDT", "BUSD", "DAI"]},
    "leverage": {"impact_score": 0.7, "affected_tokens": ["All"]},
    "custody": {"impact_score": 0.6, "affected_tokens": ["All"]},
    "defi": {"impact_score": 0.7, "affected_tokens": ["All"]},
    "securities": {"impact_score": 0.8, "affected_tokens": ["BTC", "ETH"]},
    "margin": {"impact_score": 0.65, "affected_tokens": ["All"]},
    "derivatives": {"impact_score": 0.7, "affected_tokens": ["All"]},
}

# Data sources
DATA_SOURCES = {
    "sec": "https://www.sec.gov/news/pressreleases.rss",
    "cftc": "https://www.cftc.gov/PressRoom/PressReleases/index.htm",
    "fed": "https://www.federalreserve.gov/feeds/press_all.xml",
}

# Configuration
CONFIG = {
    "scan_interval": 1800,              # Scan every 30 minutes
    "max_trades_per_week": 3,           # Max 3 regulatory trades/week
    "min_confidence": 0.75,             # Only high-confidence trades
    "position_size_pct": 0.03,          # 3% of portfolio per trade
    "exit_stop_loss_pct": 0.01,         # 1% trailing stop
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


def _fetch_rss(url, timeout=10):
    """Fetch and parse RSS feed."""
    h = {"User-Agent": "NetTrace/1.0"}
    req = urllib.request.Request(url, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return ET.fromstring(resp.read())
    except Exception as e:
        logger.error(f"Failed to fetch RSS {url}: {e}")
        return None


class RegulatoryScanner:
    """Monitor regulatory announcements for trading signals."""

    def __init__(self):
        self.db = sqlite3.connect(SCANNER_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.trades_this_week = 0
        self._sync_weekly_count()

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT,
                content TEXT,
                keywords_json TEXT,
                impact_score REAL DEFAULT 0.0,
                affected_tokens_json TEXT,
                parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                announcement_id INTEGER,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                confidence REAL NOT NULL,
                size_pct REAL NOT NULL,
                entry_reason TEXT,
                signal_generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                order_id TEXT,
                status TEXT DEFAULT 'pending',
                filled_at TIMESTAMP,
                pnl_usd REAL,
                FOREIGN KEY (announcement_id) REFERENCES announcements(id)
            );
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_price REAL,
                exit_price REAL,
                size_usd REAL,
                pnl_usd REAL,
                entry_time TIMESTAMP,
                exit_time TIMESTAMP,
                status TEXT DEFAULT 'active',
                FOREIGN KEY (signal_id) REFERENCES signals(id)
            );
        """)
        self.db.commit()

    def _sync_weekly_count(self):
        """Load current week's trade count from DB."""
        cursor = self.db.execute(
            "SELECT COUNT(*) as cnt FROM signals WHERE signal_generated_at > datetime('now', '-7 days') AND status != 'cancelled'"
        )
        row = cursor.fetchone()
        self.trades_this_week = row["cnt"] if row else 0

    def scan_sec_feed(self):
        """Scan SEC press releases RSS."""
        root = _fetch_rss(DATA_SOURCES["sec"])
        if not root:
            return []

        announcements = []
        try:
            for item in root.findall(".//item"):
                title = item.find("title")
                link = item.find("link")
                pub_date = item.find("pubDate")
                description = item.find("description")

                if title is not None and title.text:
                    announcement = {
                        "source": "SEC",
                        "title": title.text,
                        "url": link.text if link is not None else None,
                        "content": description.text if description is not None else "",
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                    announcements.append(announcement)
        except Exception as e:
            logger.error(f"Error parsing SEC feed: {e}")

        return announcements

    def scan_fed_feed(self):
        """Scan Federal Reserve press releases RSS."""
        root = _fetch_rss(DATA_SOURCES["fed"])
        if not root:
            return []

        announcements = []
        try:
            for item in root.findall(".//item"):
                title = item.find("title")
                link = item.find("link")
                description = item.find("description")

                if title is not None and title.text:
                    announcement = {
                        "source": "FED",
                        "title": title.text,
                        "url": link.text if link is not None else None,
                        "content": description.text if description is not None else "",
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                    announcements.append(announcement)
        except Exception as e:
            logger.error(f"Error parsing FED feed: {e}")

        return announcements

    def score_announcement(self, announcement):
        """Score announcement for regulatory impact."""
        text = (announcement.get("title", "") + " " + announcement.get("content", "")).lower()

        keywords = []
        impact_score = 0.0
        affected_tokens = set()

        for keyword, data in REGULATORY_KEYWORDS.items():
            if keyword in text:
                keywords.append(keyword)
                impact_score = max(impact_score, data["impact_score"])
                if data["affected_tokens"] != ["All"]:
                    affected_tokens.update(data["affected_tokens"])
                else:
                    affected_tokens.add("All")

        return {
            "keywords": keywords,
            "impact_score": impact_score,
            "affected_tokens": list(affected_tokens),
        }

    def generate_signal(self, announcement, scoring):
        """Generate trading signal from announcement."""
        if scoring["impact_score"] < CONFIG["min_confidence"]:
            return None

        # Determine direction based on keywords
        # Restrictive announcements = SHORT, supportive = LONG
        restrictive = ["stablecoin", "leverage", "margin", "derivatives"]
        text = (announcement.get("title", "") + " " + announcement.get("content", "")).lower()

        direction = "SHORT" if any(r in text for r in restrictive) else "LONG"

        # Choose pair based on affected tokens
        affected_tokens = scoring["affected_tokens"]
        if "All" in affected_tokens:
            pair = "BTC-USD"  # Default to BTC for broad announcements
        elif "USDC" in affected_tokens or "USDT" in affected_tokens:
            pair = "ETH-USD"  # Stablecoin issues affect ETH ecosystem
        else:
            pair = "BTC-USD"

        signal = {
            "pair": pair,
            "direction": direction,
            "confidence": scoring["impact_score"],
            "size_pct": CONFIG["position_size_pct"],
            "entry_reason": f"Regulatory: {', '.join(scoring['keywords'])}",
        }
        return signal

    def run_once(self):
        """Scan feeds once and process signals."""
        logger.info("Starting regulatory scan...")

        # Fetch announcements
        announcements = []
        announcements.extend(self.scan_sec_feed())
        announcements.extend(self.scan_fed_feed())

        logger.info(f"Fetched {len(announcements)} announcements")

        signals_generated = 0
        for announcement in announcements:
            # Check if already processed
            cursor = self.db.execute(
                "SELECT id FROM announcements WHERE url = ? AND source = ?",
                (announcement.get("url"), announcement.get("source"))
            )
            if cursor.fetchone():
                continue

            # Score and generate signal
            scoring = self.score_announcement(announcement)
            if scoring["impact_score"] == 0:
                continue

            # Store announcement
            cursor = self.db.execute(
                """INSERT INTO announcements
                   (source, title, url, content, keywords_json, impact_score, affected_tokens_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    announcement["source"],
                    announcement["title"],
                    announcement.get("url"),
                    announcement.get("content"),
                    json.dumps(scoring["keywords"]),
                    scoring["impact_score"],
                    json.dumps(scoring["affected_tokens"]),
                    announcement.get("created_at"),
                )
            )
            self.db.commit()
            announcement_id = cursor.lastrowid

            # Generate signal
            signal = self.generate_signal(announcement, scoring)
            if signal and self.trades_this_week < CONFIG["max_trades_per_week"]:
                self.db.execute(
                    """INSERT INTO signals (announcement_id, pair, direction, confidence, size_pct, entry_reason, status)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        announcement_id,
                        signal["pair"],
                        signal["direction"],
                        signal["confidence"],
                        signal["size_pct"],
                        signal["entry_reason"],
                        "pending",
                    )
                )
                self.db.commit()
                signals_generated += 1
                self.trades_this_week += 1

        logger.info(f"Generated {signals_generated} signals. Weekly count: {self.trades_this_week}/{CONFIG['max_trades_per_week']}")
        return signals_generated

    def monitor_positions(self):
        """Monitor active positions and apply exit stops."""
        # Fetch active trades
        cursor = self.db.execute(
            "SELECT * FROM trades WHERE status = 'active' ORDER BY entry_time DESC"
        )
        trades = cursor.fetchall()

        for trade in trades:
            # Placeholder: would fetch current price and check stop-loss
            # For now, just log
            logger.debug(f"Monitoring {trade['pair']} {trade['direction']} entry={trade['entry_price']}")


def main():
    """Main agent loop."""
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    scanner = RegulatoryScanner()

    if cmd == "once":
        scanner.run_once()
        return

    if cmd == "status":
        cursor = scanner.db.execute(
            "SELECT COUNT(*) as cnt FROM signals WHERE status = 'pending'"
        )
        pending = cursor.fetchone()["cnt"]
        print(json.dumps({"pending_signals": pending, "weekly_trades": scanner.trades_this_week}))
        return

    # Default: run daemon loop
    logger.info(f"Starting regulatory scanner loop (interval={CONFIG['scan_interval']}s)")
    while True:
        try:
            scanner.run_once()
            scanner.monitor_positions()
        except Exception as e:
            logger.error(f"Error in scan cycle: {e}", exc_info=True)

        time.sleep(CONFIG["scan_interval"])


if __name__ == "__main__":
    main()
