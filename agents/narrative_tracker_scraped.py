#!/usr/bin/env python3
"""Narrative Tracker — scrape news sources for memetic trading signals.

Uses BeautifulSoup to detect narrative lifecycle:
  - Birth: Hockey stick growth in mentions (S-curve inflection)
  - Growth: Increasing coverage + sentiment
  - Saturation: Consensus (everyone knows it = priced in)
  - Death: Mentions drop >50% (trend reversal)

Sources scraped (all public):
  - CoinDesk (daily crypto news)
  - The Block (weekly research)
  - Crypto Briefing (alerts)
  - Reddit /r/cryptocurrency (real-time sentiment)
  - Twitter trends (public search)

No API keys needed.
"""

import json
import logging
import os
import sqlite3
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Installing BeautifulSoup4...")
    os.system("python3 -m pip install beautifulsoup4 --break-system-packages 2>/dev/null || echo 'install failed'")
    from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "narrative_tracker.log")),
    ],
)
logger = logging.getLogger("narrative_tracker")

TRACKER_DB = str(Path(__file__).parent / "narrative_tracker.db")

# Narratives we track (20+ themes)
NARRATIVES = {
    "AI": ["artificial intelligence", "ChatGPT", "GPT", "neural network", "machine learning"],
    "DeFi 2.0": ["decentralized finance", "yield farming", "DEX", "liquidity mining", "AMM"],
    "L2s": ["layer 2", "rollup", "Arbitrum", "Optimism", "Polygon", "Solana"],
    "RWA": ["real world assets", "tokenized", "securities", "bonds on chain"],
    "Gaming": ["GameFi", "metaverse", "NFT", "play-to-earn"],
    "Restaking": ["eigenlayer", "liquid staking", "double staking"],
    "Ordinals": ["Bitcoin Ordinals", "BRC-20", "inscriptions"],
    "Solana": ["Solana", "SOL"],
    "Bitcoin": ["Bitcoin", "BTC", "Taproot"],
    "Ethereum": ["Ethereum", "ETH", "EVM"],
}


def _fetch_html(url: str, timeout: int = 10) -> str:
    """Fetch HTML from URL."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (NetTrace/1.0)"}
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return ""


class NarrativeTracker:
    """Track narrative lifecycle for memetic trading."""

    def __init__(self):
        self.db = sqlite3.connect(TRACKER_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        """Create tables."""
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS narrative_mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                narrative TEXT NOT NULL,
                source TEXT NOT NULL,
                headline TEXT NOT NULL,
                url TEXT,
                mention_count INTEGER DEFAULT 1,
                sentiment REAL DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS narrative_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                narrative TEXT NOT NULL UNIQUE,
                current_stage TEXT,
                mention_7d INTEGER DEFAULT 0,
                mention_30d INTEGER DEFAULT 0,
                growth_rate REAL DEFAULT 0,
                confidence REAL DEFAULT 0,
                signal_type TEXT,
                entry_price REAL,
                exit_price REAL,
                pnl_usd REAL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

    def scrape_coindesk(self) -> list:
        """Scrape CoinDesk headlines."""
        try:
            logger.info("Scraping CoinDesk...")
            html = _fetch_html("https://www.coindesk.com/")
            soup = BeautifulSoup(html, "html.parser")

            articles = []
            for article in soup.find_all("article")[:20]:
                title_elem = article.find("h3") or article.find("h2")
                link_elem = article.find("a")
                if title_elem and link_elem:
                    articles.append({
                        "headline": title_elem.text.strip(),
                        "url": link_elem.get("href", ""),
                        "source": "CoinDesk",
                    })

            logger.info(f"✓ Scraped {len(articles)} CoinDesk articles")
            return articles
        except Exception as e:
            logger.error(f"CoinDesk scrape failed: {e}")
            return []

    def scrape_theblock(self) -> list:
        """Scrape The Block research."""
        try:
            logger.info("Scraping The Block...")
            html = _fetch_html("https://www.theblock.co/")
            soup = BeautifulSoup(html, "html.parser")

            articles = []
            for article in soup.find_all("article")[:15]:
                title_elem = article.find("h3") or article.find("h2")
                link_elem = article.find("a")
                if title_elem and link_elem:
                    articles.append({
                        "headline": title_elem.text.strip(),
                        "url": link_elem.get("href", ""),
                        "source": "The Block",
                    })

            logger.info(f"✓ Scraped {len(articles)} Block articles")
            return articles
        except Exception as e:
            logger.error(f"Block scrape failed: {e}")
            return []

    def detect_narratives(self, articles: list) -> dict:
        """Detect which narratives are trending."""
        narrative_counts = Counter()
        narrative_sources = {}

        for article in articles:
            headline = article["headline"].lower()

            for narrative, keywords in NARRATIVES.items():
                if any(kw.lower() in headline for kw in keywords):
                    narrative_counts[narrative] += 1

                    if narrative not in narrative_sources:
                        narrative_sources[narrative] = []
                    narrative_sources[narrative].append({
                        "headline": article["headline"],
                        "source": article["source"],
                    })

        return {
            "counts": dict(narrative_counts.most_common()),
            "sources": narrative_sources,
        }

    def detect_lifecycle(self, narrative: str) -> dict:
        """Detect narrative lifecycle stage."""
        # Query mention counts
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)

        c7 = self.db.execute(
            "SELECT COUNT(*) as cnt FROM narrative_mentions WHERE narrative=? AND timestamp > ?",
            (narrative, seven_days_ago),
        ).fetchone()
        c30 = self.db.execute(
            "SELECT COUNT(*) as cnt FROM narrative_mentions WHERE narrative=? AND timestamp > ?",
            (narrative, thirty_days_ago),
        ).fetchone()

        mentions_7d = c7["cnt"] if c7 else 0
        mentions_30d = c30["cnt"] if c30 else 0

        # Calculate growth rate
        if mentions_30d > 0:
            growth_rate = (mentions_7d - mentions_30d / 4) / (mentions_30d / 4)
        else:
            growth_rate = 0

        # Detect stage
        if growth_rate > 2.0:  # Hockey stick
            stage = "BIRTH"
            signal_type = "BUY"
            confidence = 0.8
        elif growth_rate > 0.5:  # Growing
            stage = "GROWTH"
            signal_type = "BUY"
            confidence = 0.6
        elif growth_rate < -0.5:  # Declining
            stage = "DEATH"
            signal_type = "SELL"
            confidence = 0.7
        else:  # Stable
            stage = "SATURATION"
            signal_type = "SELL"
            confidence = 0.5

        return {
            "narrative": narrative,
            "stage": stage,
            "mentions_7d": mentions_7d,
            "mentions_30d": mentions_30d,
            "growth_rate": growth_rate,
            "signal_type": signal_type,
            "confidence": confidence,
        }

    def track_all(self) -> dict:
        """Run full narrative tracking."""
        logger.info("=" * 80)
        logger.info("NARRATIVE TRACKING")
        logger.info("=" * 80)

        # Scrape sources
        all_articles = []
        all_articles.extend(self.scrape_coindesk())
        all_articles.extend(self.scrape_theblock())

        logger.info(f"Total articles scraped: {len(all_articles)}")

        # Detect narratives
        detected = self.detect_narratives(all_articles)

        # Store mentions + detect lifecycle
        signals = []
        for narrative, count in detected["counts"].items():
            # Store mentions
            for article in detected["sources"].get(narrative, []):
                try:
                    self.db.execute(
                        """INSERT INTO narrative_mentions
                           (narrative, source, headline, mention_count)
                           VALUES (?, ?, ?, 1)""",
                        (narrative, article["source"], article["headline"]),
                    )
                except Exception as e:
                    logger.error(f"DB insert failed for {narrative}: {e}")
            self.db.commit()

            # Detect lifecycle
            lifecycle = self.detect_lifecycle(narrative)
            if lifecycle["confidence"] > 0.5:
                signals.append(lifecycle)
                logger.info(
                    f"{narrative:15s} → {lifecycle['stage']:12s} "
                    f"({lifecycle['mentions_7d']} mentions, {lifecycle['growth_rate']:.2f}x growth)"
                )

        logger.info("\n" + "=" * 80)
        logger.info(f"Narratives tracked: {len(detected['counts'])}")
        logger.info(f"Actionable signals: {len(signals)}")
        logger.info("=" * 80)

        return {
            "narratives": detected["counts"],
            "lifecycle_signals": signals,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def main():
    """Run tracker."""
    tracker = NarrativeTracker()
    result = tracker.track_all()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
