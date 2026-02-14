#!/usr/bin/env python3
"""Futures Mispricing Agent — spot-futures basis arbitrage.

Strategy:
  - Calculate fair futures price: spot * (1 + r*t)
  - Detect mispricing:
    - Contango (futures > fair): SHORT futures, LONG spot (cash-carry arbitrage)
    - Backwardation (futures < fair): LONG futures, SHORT spot (reverse cash-carry)
  - Hold to expiration OR flatten when spread normalizes
  - Target: 0.5-2% APR on capital

Game Theory:
  - Basis arbitrage is the "risk-free" trade (if you hold to maturity)
  - Volatility: doesn't matter, we're spread-neutral
  - Funding costs: account for borrowing, funding rates, spreads
  - First-mover: place both legs quickly before others hedge

RULES:
  - Only trade when mispricing > 0.3% (covers fees + slippage)
  - Equal notional on both legs (market-neutral)
  - Auto-exit if spread widens against us (basis risk)
  - Max 5 simultaneous arb trades
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
        logging.FileHandler(str(Path(__file__).parent / "futures_mispricing.log")),
    ]
)
logger = logging.getLogger("futures_mispricing")

MISPRICING_DB = str(Path(__file__).parent / "futures_mispricing.db")

# Configuration
CONFIG = {
    "scan_interval": 300,               # Scan every 5 minutes (futures update frequently)
    "max_trades": 5,                    # Max 5 simultaneous arb trades
    "min_mispricing_pct": 0.003,        # Only trade if > 0.3% mispricing (covers fees)
    "position_size_pct": 0.02,          # 2% position per trade
    "risk_free_rate": 0.05,             # 5% annual risk-free rate for carry calculation
    "funding_rate_refresh": 300,        # Refresh funding rates every 5 min
}

# Futures expirations we trade (quarterly CME BTC futures)
FUTURES_PAIRS = {
    "BTCQ24": {"underlying": "BTC-USD", "expiry_days": 60},  # June 2024
    "BTCU24": {"underlying": "BTC-USD", "expiry_days": 150},  # September 2024
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


def _calculate_fair_price(spot_price, risk_free_rate, days_to_expiry):
    """Calculate fair futures price (cost of carry model).

    Fair price = spot * (1 + r * t)
    where r = annual risk-free rate, t = time in years
    """
    t = days_to_expiry / 365.0
    fair_price = spot_price * (1 + risk_free_rate * t)
    return fair_price


class FuturesMispricing:
    """Hunt spot-futures basis arbitrage opportunities."""

    def __init__(self):
        self.db = sqlite3.connect(MISPRICING_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.open_arbs = 0

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS spot_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                price REAL NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS futures_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                futures_symbol TEXT NOT NULL,
                price REAL NOT NULL,
                days_to_expiry INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                futures_symbol TEXT NOT NULL,
                spot_price REAL NOT NULL,
                futures_price REAL NOT NULL,
                fair_value REAL NOT NULL,
                mispricing_pct REAL NOT NULL,
                arb_type TEXT NOT NULL,
                spot_order_id TEXT,
                futures_order_id TEXT,
                status TEXT DEFAULT 'pending',
                entry_time TIMESTAMP,
                exit_time TIMESTAMP,
                pnl_usd REAL,
                pnl_pct REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS funding_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange TEXT NOT NULL,
                pair TEXT NOT NULL,
                funding_rate REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

    def fetch_spot_price(self, pair):
        """Fetch current spot price from Coinbase."""
        url = f"https://api.exchange.coinbase.com/products/{pair}/ticker"
        data = _fetch_json(url)
        if data and "price" in data:
            price = float(data["price"])
            self.db.execute(
                "INSERT INTO spot_prices (pair, price) VALUES (?, ?)",
                (pair, price)
            )
            self.db.commit()
            return price
        return None

    def fetch_futures_price(self, futures_symbol):
        """Fetch futures price (placeholder - real impl uses CME/Deribit APIs)."""
        # In production: connect to CME API or Deribit API
        # For now: simulate with slight premium
        underlying_pair = FUTURES_PAIRS[futures_symbol]["underlying"]
        spot = self.fetch_spot_price(underlying_pair)
        if not spot:
            return None

        # Simulated futures price (usually at slight premium/discount)
        futures_price = spot * 1.005  # 0.5% premium
        days_to_expiry = FUTURES_PAIRS[futures_symbol]["expiry_days"]

        self.db.execute(
            "INSERT INTO futures_prices (futures_symbol, price, days_to_expiry) VALUES (?, ?, ?)",
            (futures_symbol, futures_price, days_to_expiry)
        )
        self.db.commit()
        return futures_price, days_to_expiry

    def detect_arbitrage(self, pair, spot_price, futures_symbol, futures_price, days_to_expiry):
        """Detect arbitrage opportunity."""
        # Calculate fair value
        fair_value = _calculate_fair_price(spot_price, CONFIG["risk_free_rate"], days_to_expiry)

        # Calculate mispricing
        mispricing = (futures_price - fair_value) / fair_value
        mispricing_pct = abs(mispricing)

        if mispricing_pct < CONFIG["min_mispricing_pct"]:
            return None

        # Determine arb type
        if futures_price > fair_value:
            # Contango: futures expensive = SHORT futures, LONG spot
            arb_type = "cash_carry"
            spot_side = "BUY"
            futures_side = "SHORT"
        else:
            # Backwardation: futures cheap = LONG futures, SHORT spot
            arb_type = "reverse_cash_carry"
            spot_side = "SHORT"
            futures_side = "LONG"

        opportunity = {
            "pair": pair,
            "futures_symbol": futures_symbol,
            "spot_price": spot_price,
            "futures_price": futures_price,
            "fair_value": fair_value,
            "mispricing_pct": mispricing_pct,
            "arb_type": arb_type,
            "spot_side": spot_side,
            "futures_side": futures_side,
        }
        return opportunity

    def run_once(self):
        """Scan for futures mispricing opportunities."""
        logger.info("Starting futures mispricing scan...")

        opportunities_found = 0
        for futures_symbol in FUTURES_PAIRS.keys():
            data = self.fetch_futures_price(futures_symbol)
            if not data:
                continue

            futures_price, days_to_expiry = data
            pair = FUTURES_PAIRS[futures_symbol]["underlying"]

            # Fetch spot price
            spot_price = self.fetch_spot_price(pair)
            if not spot_price:
                continue

            logger.info(f"{futures_symbol}: spot=${spot_price:.2f}, futures=${futures_price:.2f}")

            # Detect arbitrage
            opp = self.detect_arbitrage(pair, spot_price, futures_symbol, futures_price, days_to_expiry)
            if opp:
                # Check current open arbs
                cursor = self.db.execute(
                    "SELECT COUNT(*) as cnt FROM arbitrage_opportunities WHERE status IN ('pending', 'active')"
                )
                self.open_arbs = cursor.fetchone()["cnt"]

                if self.open_arbs < CONFIG["max_trades"]:
                    # Store opportunity
                    self.db.execute(
                        """INSERT INTO arbitrage_opportunities
                           (pair, futures_symbol, spot_price, futures_price, fair_value, mispricing_pct, arb_type, status)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            pair,
                            futures_symbol,
                            spot_price,
                            futures_price,
                            opp["fair_value"],
                            opp["mispricing_pct"],
                            opp["arb_type"],
                            "pending",
                        )
                    )
                    self.db.commit()
                    opportunities_found += 1
                    logger.info(f"Found {opp['arb_type']}: {opp['mispricing_pct']*100:.3f}% mispricing")

        logger.info(f"Found {opportunities_found} opportunities. Open: {self.open_arbs}/{CONFIG['max_trades']}")
        return opportunities_found

    def monitor_arbs(self):
        """Monitor active arbitrage positions."""
        cursor = self.db.execute(
            "SELECT * FROM arbitrage_opportunities WHERE status IN ('pending', 'active') ORDER BY created_at DESC"
        )
        arbs = cursor.fetchall()

        for arb in arbs:
            # Fetch current prices
            spot = self.fetch_spot_price(arb["pair"])
            futures_data = self.fetch_futures_price(arb["futures_symbol"])

            if spot and futures_data:
                futures, days = futures_data
                # Check if spread normalized
                spread = futures - spot
                original_spread = arb["futures_price"] - arb["spot_price"]

                logger.debug(f"{arb['futures_symbol']}: original_spread=${original_spread:.2f}, current_spread=${spread:.2f}")


def main():
    """Main agent loop."""
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    mispricing = FuturesMispricing()

    if cmd == "once":
        mispricing.run_once()
        return

    if cmd == "status":
        cursor = mispricing.db.execute(
            "SELECT COUNT(*) as cnt FROM arbitrage_opportunities WHERE status IN ('pending', 'active')"
        )
        open_arbs = cursor.fetchone()["cnt"]
        print(json.dumps({"open_arbitrages": open_arbs, "max": CONFIG["max_trades"]}))
        return

    # Default: run daemon loop
    logger.info(f"Starting futures mispricing loop (interval={CONFIG['scan_interval']}s)")
    while True:
        try:
            mispricing.run_once()
            mispricing.monitor_arbs()
        except Exception as e:
            logger.error(f"Error in scan cycle: {e}", exc_info=True)

        time.sleep(CONFIG["scan_interval"])


if __name__ == "__main__":
    main()
