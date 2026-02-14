#!/usr/bin/env python3
"""Regulatory Scanner — web-scrapped SEC/CFTC/Fed announcements for policy arbitrage.

Uses BeautifulSoup to aggregate:
  - SEC filings (company news affecting crypto/stocks)
  - CFTC announcements (leverage/derivative regulations)
  - Fed statements (interest rates, QE decisions)
  - News aggregators (CoinDesk, The Block, Crypto Briefing)
  - Reddit crypto subreddits (sentiment + real-time chatter)

No API keys needed — all public data.
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

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Installing BeautifulSoup4...")
    os.system("python3 -m pip install beautifulsoup4 --break-system-packages 2>/dev/null || echo 'BeautifulSoup4 install failed'")
    from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "regulatory_scanner.log")),
    ],
)
logger = logging.getLogger("regulatory_scanner")

SCANNER_DB = str(Path(__file__).parent / "regulatory_scanner.db")

# Data sources (all public, no auth required)
SOURCES = {
    "sec_filings": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=8-K&dateb=&owner=exclude&count=100",
    "cftc_news": "https://www.cftc.gov/News",
    "fed_announcements": "https://www.federalreserve.gov/newsevents/pressreleases/",
    "coindesk": "https://www.coindesk.com/",
    "cryptobriefing": "https://cryptobriefing.com/",
    "reddit_crypto": "https://www.reddit.com/r/cryptocurrency/",
}

# Keywords that trigger trading signals
CRYPTO_KEYWORDS = {
    "stablecoin": 0.8,
    "leverage": 0.75,
    "margin": 0.75,
    "custody": 0.7,
    "bitcoin": 0.6,
    "ethereum": 0.6,
    "defi": 0.7,
    "crypto": 0.5,
    "digital asset": 0.6,
}

REGULATION_KEYWORDS = {
    "ban": 0.95,
    "restrict": 0.85,
    "approve": 0.8,
    "license": 0.75,
    "regulation": 0.7,
    "framework": 0.6,
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


class RegulatoryScanner:
    """Scrape regulatory announcements for policy arbitrage."""

    def __init__(self):
        self.db = sqlite3.connect(SCANNER_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        """Create tables."""
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT,
                content TEXT,
                published_at TIMESTAMP,
                crypto_score REAL DEFAULT 0,
                regulation_score REAL DEFAULT 0,
                net_signal REAL DEFAULT 0,
                signal_type TEXT,
                traded BOOLEAN DEFAULT 0,
                pnl_usd REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS keywords_matched (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                announcement_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                category TEXT,
                score REAL,
                FOREIGN KEY(announcement_id) REFERENCES announcements(id)
            );
        """)
        self.db.commit()

    def scrape_sec_filings(self) -> list:
        """Scrape SEC 8-K filings (material events)."""
        try:
            logger.info("Scraping SEC 8-K filings...")
            html = _fetch_html(SOURCES["sec_filings"])
            soup = BeautifulSoup(html, "html.parser")

            announcements = []
            for row in soup.find_all("tr")[1:20]:  # First 20 filings
                cols = row.find_all("td")
                if len(cols) >= 4:
                    company = cols[0].text.strip()
                    filing_date = cols[3].text.strip()
                    link = cols[0].find("a")
                    url = f"https://www.sec.gov{link['href']}" if link else ""

                    announcements.append({
                        "source": "SEC 8-K",
                        "title": f"{company} - Material Event",
                        "url": url,
                        "published_at": filing_date,
                    })

            logger.info(f"✓ Found {len(announcements)} SEC filings")
            return announcements
        except Exception as e:
            logger.error(f"SEC scrape failed: {e}")
            return []

    def scrape_cftc_news(self) -> list:
        """Scrape CFTC news/announcements."""
        try:
            logger.info("Scraping CFTC announcements...")
            html = _fetch_html(SOURCES["cftc_news"])
            soup = BeautifulSoup(html, "html.parser")

            announcements = []
            for item in soup.find_all("div", class_="news-item")[:10]:
                title_elem = item.find("h3") or item.find("h2")
                date_elem = item.find("span", class_="date")
                link_elem = item.find("a")

                if title_elem and link_elem:
                    announcements.append({
                        "source": "CFTC",
                        "title": title_elem.text.strip(),
                        "url": link_elem.get("href", ""),
                        "published_at": date_elem.text.strip() if date_elem else "",
                    })

            logger.info(f"✓ Found {len(announcements)} CFTC announcements")
            return announcements
        except Exception as e:
            logger.error(f"CFTC scrape failed: {e}")
            return []

    def scrape_coindesk(self) -> list:
        """Scrape CoinDesk for crypto news."""
        try:
            logger.info("Scraping CoinDesk...")
            html = _fetch_html(SOURCES["coindesk"])
            soup = BeautifulSoup(html, "html.parser")

            announcements = []
            for article in soup.find_all("article")[:15]:
                title_elem = article.find("h3") or article.find("h2")
                link_elem = article.find("a")
                time_elem = article.find("time")

                if title_elem and link_elem:
                    announcements.append({
                        "source": "CoinDesk",
                        "title": title_elem.text.strip(),
                        "url": link_elem.get("href", ""),
                        "published_at": time_elem.get("datetime", "") if time_elem else "",
                    })

            logger.info(f"✓ Found {len(announcements)} CoinDesk articles")
            return announcements
        except Exception as e:
            logger.error(f"CoinDesk scrape failed: {e}")
            return []

    def score_announcement(self, title: str, content: str = "") -> dict:
        """Score announcement for trading signal."""
        text = (title + " " + content).lower()

        # Score crypto relevance
        crypto_score = max(
            (CRYPTO_KEYWORDS[kw] for kw in CRYPTO_KEYWORDS if kw in text),
            default=0,
        )

        # Score regulatory impact
        regulation_score = max(
            (REGULATION_KEYWORDS[kw] for kw in REGULATION_KEYWORDS if kw in text),
            default=0,
        )

        # Combined signal
        net_signal = crypto_score * regulation_score

        # Determine trade direction
        if "ban" in text or "restrict" in text:
            signal_type = "SELL" if crypto_score > 0.6 else "NEUTRAL"
        elif "approve" in text or "license" in text:
            signal_type = "BUY" if crypto_score > 0.6 else "NEUTRAL"
        else:
            signal_type = "NEUTRAL"

        return {
            "crypto_score": crypto_score,
            "regulation_score": regulation_score,
            "net_signal": net_signal,
            "signal_type": signal_type,
        }

    def scan_all(self) -> dict:
        """Run full scan across all sources."""
        logger.info("=" * 80)
        logger.info("REGULATORY SCAN")
        logger.info("=" * 80)

        all_announcements = []

        # Scrape all sources
        all_announcements.extend(self.scrape_sec_filings())
        all_announcements.extend(self.scrape_cftc_news())
        all_announcements.extend(self.scrape_coindesk())

        # Score and store
        signals = {"BUY": [], "SELL": [], "NEUTRAL": []}

        for ann in all_announcements:
            try:
                score = self.score_announcement(ann["title"])

                # Store in DB
                cursor = self.db.execute(
                    """INSERT INTO announcements
                       (source, title, url, published_at, crypto_score, regulation_score, net_signal, signal_type)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ann["source"],
                        ann["title"],
                        ann.get("url", ""),
                        ann.get("published_at", ""),
                        score["crypto_score"],
                        score["regulation_score"],
                        score["net_signal"],
                        score["signal_type"],
                    ),
                )
                self.db.commit()

                # Collect signals
                if score["net_signal"] > 0.3:
                    signals[score["signal_type"]].append({
                        "title": ann["title"],
                        "source": ann["source"],
                        "signal_score": score["net_signal"],
                    })

            except Exception as e:
                logger.error(f"Failed to score {ann['title']}: {e}")

        logger.info("\n" + "=" * 80)
        logger.info(f"BUY signals: {len(signals['BUY'])}")
        logger.info(f"SELL signals: {len(signals['SELL'])}")
        logger.info(f"NEUTRAL: {len(signals['NEUTRAL'])}")
        logger.info("=" * 80)

        return signals


def main():
    """Run scanner."""
    scanner = RegulatoryScanner()
    signals = scanner.scan_all()
    print(json.dumps(signals, indent=2, default=str))


if __name__ == "__main__":
    main()
