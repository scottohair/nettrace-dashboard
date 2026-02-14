#!/usr/bin/env python3
"""Liquidation Hunter Agent — hunt leverage cascades for quick profits.

Strategy:
  - Monitor funding rates: Binance, Deribit, Coinbase perpetuals
  - Estimate leverage distribution from Open Interest/volume ratios
  - Simulate cascade: "If BTC drops 5%, which strike prices liquidate?"
  - Pre-place limit orders 0.3-0.5% above predicted liquidation levels
  - When cascade triggers: orders fill, flip position for 1-2% instant profit

Game Theory:
  - Market microstructure: liquidations are forced selling (supply shock)
  - Stacked stops: many positions at same level (Pareto power law)
  - First-mover advantage: orders placed first get priority fills
  - Liquidity provision: we provide liquidity during stress for profit

RULES:
  - Small position sizes (0.5-1% per bet) - many small wins
  - Tight stops (0.3% loss) - cascade either happens or it doesn't
  - Max 10 open cascade bets simultaneously
  - Max 3% of portfolio at risk in liquidation bets
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
        logging.FileHandler(str(Path(__file__).parent / "liquidation_hunter.log")),
    ]
)
logger = logging.getLogger("liquidation_hunter")

HUNTER_DB = str(Path(__file__).parent / "liquidation_hunter.db")

# Configuration
CONFIG = {
    "scan_interval": 120,               # Scan every 2 minutes (cascades are fast)
    "max_open_bets": 10,                # Max 10 simultaneous cascade bets
    "max_risk_pct": 0.03,               # Max 3% portfolio at risk
    "position_size_pct": 0.01,          # 1% position per bet
    "stop_loss_pct": 0.003,             # 0.3% stop loss
    "target_profit_pct": 0.015,         # 1.5% profit target
    "funding_rate_threshold": 0.001,    # 0.1% funding rate = stress signal
    "cascade_price_drop_pct": 0.05,     # Simulate 5% drop to find liquidations
}

# Public funding rate APIs
FUNDING_RATE_URLS = {
    "BTC-USD": "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
    "ETH-USD": "https://api.exchange.coinbase.com/products/ETH-USD/ticker",
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


class LiquidationHunter:
    """Hunt leverage cascades for quick profits."""

    def __init__(self):
        self.db = sqlite3.connect(HUNTER_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.open_bets = 0

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS cascade_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                funding_rate REAL,
                open_interest_usd REAL,
                open_interest_btc REAL,
                leverage_estimate REAL,
                predicted_cascade_price REAL,
                limit_order_price REAL,
                order_id TEXT,
                status TEXT DEFAULT 'pending',
                filled BOOLEAN DEFAULT 0,
                filled_at TIMESTAMP,
                exit_price REAL,
                pnl_usd REAL,
                pnl_pct REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS funding_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                funding_rate REAL,
                open_interest_usd REAL,
                mark_price REAL,
                index_price REAL,
                snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS cascade_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                price_drop_pct REAL,
                liquidation_price REAL,
                estimated_liquidations_usd REAL,
                event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

    def fetch_funding_rate(self, pair):
        """Fetch current funding rate (placeholder - real impl uses Binance/Deribit)."""
        # Coinbase API doesn't directly expose funding rates
        # In production: use Binance perpetuals API or Deribit
        # For now: return simulated data
        data = _fetch_json(FUNDING_RATE_URLS.get(pair, ""))
        if data:
            return {
                "pair": pair,
                "price": float(data.get("price", 0)),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        return None

    def estimate_cascade_levels(self, pair, current_price, funding_rate):
        """Estimate liquidation cascade levels given a price drop."""
        # Simplified model: if funding_rate is high, positions are levered
        if funding_rate is None or funding_rate < CONFIG["funding_rate_threshold"]:
            return []

        # Estimate average leverage from funding rate
        # High funding (0.1%+) = heavily levered market
        leverage_estimate = min(10.0, max(2.0, funding_rate * 1000.0))

        # Simulated liquidation cascade
        cascade_levels = []
        for drop_pct in [0.02, 0.05, 0.10]:  # 2%, 5%, 10% drops
            liq_price = current_price * (1 - drop_pct)
            cascade_levels.append({
                "drop_pct": drop_pct,
                "liquidation_price": liq_price,
                "estimated_liquidations": current_price * 1e6,  # Placeholder
            })

        return cascade_levels

    def generate_cascade_bet(self, pair, current_price, cascade_level, funding_rate):
        """Generate a bet on cascade liquidations."""
        # Place limit order 0.3-0.5% above predicted liquidation level
        # If cascade happens, we accumulate at bottom and flip for quick profit
        order_price = cascade_level["liquidation_price"] * (1 + 0.004)

        bet = {
            "pair": pair,
            "current_price": current_price,
            "predicted_cascade_price": cascade_level["liquidation_price"],
            "limit_order_price": order_price,
            "funding_rate": funding_rate,
            "position_size_pct": CONFIG["position_size_pct"],
            "stop_loss_pct": CONFIG["stop_loss_pct"],
            "target_profit_pct": CONFIG["target_profit_pct"],
        }
        return bet

    def run_once(self):
        """Scan funding rates and generate cascade predictions."""
        logger.info("Starting liquidation cascade scan...")

        predictions_made = 0
        for pair in ["BTC-USD", "ETH-USD"]:
            # Fetch current price and funding rate
            ticker = _fetch_json(f"https://api.exchange.coinbase.com/products/{pair}/ticker")
            if not ticker:
                continue

            current_price = float(ticker.get("price", 0))
            # In real implementation, fetch actual funding rate from Binance/Deribit
            # For now, use simulated value
            funding_rate = 0.0001 if pair == "BTC-USD" else 0.00008

            logger.info(f"{pair}: price=${current_price:.2f}, funding={funding_rate:.5f}")

            # Estimate cascade levels
            cascade_levels = self.estimate_cascade_levels(pair, current_price, funding_rate)
            if not cascade_levels:
                continue

            # Check current open bets
            cursor = self.db.execute(
                "SELECT COUNT(*) as cnt FROM cascade_predictions WHERE status = 'pending' OR status = 'active'"
            )
            self.open_bets = cursor.fetchone()["cnt"]

            # Generate cascade bets if under limit
            for cascade in cascade_levels:
                if self.open_bets >= CONFIG["max_open_bets"]:
                    break

                bet = self.generate_cascade_bet(pair, current_price, cascade, funding_rate)

                # Store prediction
                self.db.execute(
                    """INSERT INTO cascade_predictions
                       (pair, funding_rate, predicted_cascade_price, limit_order_price, status, created_at)
                       VALUES (?, ?, ?, ?, ?, datetime('now'))""",
                    (
                        pair,
                        funding_rate,
                        cascade["liquidation_price"],
                        bet["limit_order_price"],
                        "pending",
                    )
                )
                self.db.commit()
                predictions_made += 1
                self.open_bets += 1

        logger.info(f"Generated {predictions_made} cascade predictions. Open: {self.open_bets}/{CONFIG['max_open_bets']}")
        return predictions_made

    def monitor_bets(self):
        """Monitor active cascade bets for fills or stop-loss."""
        cursor = self.db.execute(
            "SELECT * FROM cascade_predictions WHERE status IN ('pending', 'active') ORDER BY created_at DESC"
        )
        bets = cursor.fetchall()

        for bet in bets:
            # Fetch current price
            ticker = _fetch_json(f"https://api.exchange.coinbase.com/products/{bet['pair']}/ticker")
            if not ticker:
                continue

            current_price = float(ticker.get("price", 0))

            # Check if bet should close
            # Placeholder: real implementation would check fills, stops, and targets
            logger.debug(f"Monitoring {bet['pair']}: predicted={bet['predicted_cascade_price']:.2f}, current={current_price:.2f}")


def main():
    """Main agent loop."""
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    hunter = LiquidationHunter()

    if cmd == "once":
        hunter.run_once()
        return

    if cmd == "status":
        cursor = hunter.db.execute(
            "SELECT COUNT(*) as cnt FROM cascade_predictions WHERE status IN ('pending', 'active')"
        )
        open_bets = cursor.fetchone()["cnt"]
        print(json.dumps({"open_cascade_bets": open_bets, "max": CONFIG["max_open_bets"]}))
        return

    # Default: run daemon loop
    logger.info(f"Starting liquidation hunter loop (interval={CONFIG['scan_interval']}s)")
    while True:
        try:
            hunter.run_once()
            hunter.monitor_bets()
        except Exception as e:
            logger.error(f"Error in scan cycle: {e}", exc_info=True)

        time.sleep(CONFIG["scan_interval"])


if __name__ == "__main__":
    main()
