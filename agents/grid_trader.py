#!/usr/bin/env python3
"""Grid Trading Bot — the primary money maker.

Grid trading is a MARKET MAKING strategy. We're the house, not the player.
We place limit orders at regular price intervals and earn the spread.

How it works:
  1. Place BUY orders at grid levels BELOW current price
  2. Place SELL orders at grid levels ABOVE current price
  3. When a BUY fills, immediately place a SELL one grid above
  4. When a SELL fills, immediately place a BUY one grid below
  5. Each round-trip = guaranteed profit (grid spacing - 2x maker fee)

With 0.5% grid spacing and 0.4% maker fees:
  Net per round-trip = 0.5% - 0.8% (2x 0.4%) ≈ -0.3% on tight grids
  → Use 1.0% spacing: 1.0% - 0.8% = 0.2% net per round-trip ✓

Game Theory: We provide liquidity and earn the bid-ask spread.
We are MAKERS (0.4% fee), never takers (0.6% fee).

RULES:
  - ALL orders are post_only=True (maker only, 0.4% fee)
  - Grid spacing MUST exceed 2x maker fee (0.8%) to be profitable
  - Keep $4 reserve — never deploy all capital
  - Max $1.50 per grid level with $13 portfolio
"""

import json
import logging
import os
import sqlite3
import sys
import time
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
        logging.FileHandler(str(Path(__file__).parent / "grid_trader.log")),
    ]
)
logger = logging.getLogger("grid_trader")

GRID_DB = str(Path(__file__).parent / "grid_trader.db")

# Grid configuration
DEFAULT_CONFIG = {
    "pair": "BTC-USD",
    "grid_spacing_pct": 0.01,    # 1.0% between grid levels
    "levels_above": 2,           # sell levels above current price
    "levels_below": 2,           # buy levels below current price
    "order_size_usd": 1.00,     # USD per grid level (fits $13 budget)
    "min_reserve_usd": 2.00,    # keep $2 reserve (rest deployed)
    "check_interval": 60,        # seconds between order checks
    "max_open_orders": 6,        # max concurrent open orders
    "recenter_threshold": 0.03,  # recenter grid if price moves 3% from center
}


class GridTrader:
    """Grid trading bot for crypto pairs."""

    def __init__(self, config=None):
        from agent_tools import AgentTools
        self.tools = AgentTools()
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.pair = self.config["pair"]
        self.db = sqlite3.connect(GRID_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.grid_center = None
        self.total_round_trips = 0
        self.total_profit = 0.0

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS grid_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                side TEXT NOT NULL,
                grid_level REAL NOT NULL,
                price REAL NOT NULL,
                base_size REAL NOT NULL,
                order_id TEXT,
                status TEXT DEFAULT 'pending',
                fill_price REAL,
                counterpart_order_id TEXT,
                pnl REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                filled_at TIMESTAMP,
                cancelled_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS grid_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                grid_center REAL,
                total_round_trips INTEGER DEFAULT 0,
                total_profit_usd REAL DEFAULT 0.0,
                total_fees_usd REAL DEFAULT 0.0,
                active_buy_orders INTEGER DEFAULT 0,
                active_sell_orders INTEGER DEFAULT 0,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

    def _get_product_info(self, pair):
        """Get minimum order size and price increment."""
        info = self.tools.exchange.get_product(pair)
        return {
            "base_min_size": float(info.get("base_min_size", "0.00000001")),
            "base_increment": info.get("base_increment", "0.00000001"),
            "quote_increment": info.get("quote_increment", "0.01"),
            "price": float(info.get("price", "0")),
        }

    def calculate_grid_levels(self, center_price):
        """Calculate grid price levels around a center price."""
        spacing = self.config["grid_spacing_pct"]
        levels = []

        # Buy levels below current price
        for i in range(1, self.config["levels_below"] + 1):
            price = center_price * (1 - spacing * i)
            levels.append({"side": "BUY", "level": -i, "price": price})

        # Sell levels above current price
        for i in range(1, self.config["levels_above"] + 1):
            price = center_price * (1 + spacing * i)
            levels.append({"side": "SELL", "level": i, "price": price})

        return levels

    def setup_grid(self):
        """Initialize the grid around current price with adaptive sizing."""
        price = self.tools.get_price(self.pair)
        if not price:
            logger.error("Cannot get price for %s", self.pair)
            return False

        self.grid_center = price
        product = self._get_product_info(self.pair)
        base_increment = product["base_increment"]
        quote_increment = product["quote_increment"]

        # Use adaptive risk for sizing
        self.tools.refresh_risk()
        risk = self.tools.risk
        reserve = risk.min_reserve
        order_size = max(1.00, risk.optimal_grid_size)

        # Override config with adaptive values
        self.config["order_size_usd"] = round(order_size, 2)
        self.config["min_reserve_usd"] = round(reserve, 2)
        self.config["levels_below"] = risk.optimal_grid_levels
        self.config["levels_above"] = min(risk.optimal_grid_levels, 2)  # sell only if we hold

        # Check available cash
        available = self.tools.get_available_cash()
        deployable = available - reserve

        if deployable < 1.00:
            # Check if we already have grid orders running
            existing = self.db.execute(
                "SELECT COUNT(*) as cnt FROM grid_orders WHERE pair=? AND status='placed'", (self.pair,)
            ).fetchone()["cnt"]
            if existing > 0:
                logger.info("Grid already has %d active orders, cash is deployed. Monitoring fills.", existing)
                return True
            logger.warning("Not enough cash to deploy grid. Available: $%.2f, Reserve: $%.2f, Deployable: $%.2f",
                          available, reserve, deployable)
            return False

        # Cancel existing grid orders
        existing = self.db.execute(
            "SELECT order_id FROM grid_orders WHERE pair=? AND status='placed'", (self.pair,)
        ).fetchall()
        for row in existing:
            if row["order_id"]:
                self.tools.cancel_order(row["order_id"])
        self.db.execute("UPDATE grid_orders SET status='cancelled', cancelled_at=CURRENT_TIMESTAMP "
                       "WHERE pair=? AND status='placed'", (self.pair,))
        self.db.commit()

        # Calculate grid levels
        levels = self.calculate_grid_levels(price)

        # Determine how many levels we can afford
        max_buy_levels = int(deployable / self.config["order_size_usd"])
        buy_levels = [l for l in levels if l["side"] == "BUY"][:max_buy_levels]
        sell_levels = [l for l in levels if l["side"] == "SELL"]

        logger.info("Setting up grid on %s @ $%.2f | %d buy + %d sell levels | $%.2f per level",
                     self.pair, price, len(buy_levels), len(sell_levels), self.config["order_size_usd"])

        # Check if we have any holdings to place sell orders
        holdings = self.tools.get_holdings()
        base_currency = self.pair.split("-")[0]
        base_holding = holdings.get(base_currency, {}).get("available", 0)

        placed = 0
        for level in buy_levels:
            order_id = self._place_grid_order(level, base_increment, quote_increment)
            if order_id:
                placed += 1

        # Only place sell levels if we hold the base currency
        for level in sell_levels:
            base_size = self.config["order_size_usd"] / level["price"]
            if base_holding >= base_size:
                order_id = self._place_grid_order(level, base_increment, quote_increment)
                if order_id:
                    placed += 1
                    base_holding -= base_size

        logger.info("Grid setup complete: %d orders placed", placed)
        self._record_stats()
        return placed > 0

    def _place_grid_order(self, level, base_increment, quote_increment):
        """Place a single grid order."""
        from exchange_connector import CoinbaseTrader
        price = CoinbaseTrader._truncate_to_increment(level["price"], quote_increment)
        base_size = self.config["order_size_usd"] / price
        base_size = CoinbaseTrader._truncate_to_increment(base_size, base_increment)

        if base_size <= 0:
            return None

        if level["side"] == "BUY":
            order_id = self.tools.place_limit_buy(self.pair, price, base_size, agent_name="grid_trader")
        else:
            order_id = self.tools.place_limit_sell(self.pair, price, base_size, agent_name="grid_trader")

        if order_id:
            self.db.execute(
                """INSERT INTO grid_orders (pair, side, grid_level, price, base_size, order_id, status)
                   VALUES (?, ?, ?, ?, ?, ?, 'placed')""",
                (self.pair, level["side"], level["level"], price, base_size, order_id)
            )
            self.db.commit()
            logger.info("Grid %s @ $%.2f (level %+d) → order %s",
                        level["side"], price, level["level"], order_id[:8] if order_id else "?")

        return order_id

    def check_fills(self):
        """Check for filled grid orders and place counterpart orders."""
        # Get all placed orders from our DB
        placed = self.db.execute(
            "SELECT * FROM grid_orders WHERE pair=? AND status='placed' AND order_id IS NOT NULL",
            (self.pair,)
        ).fetchall()

        if not placed:
            return 0

        # Check order status on Coinbase
        fills = 0
        for order in placed:
            order_id = order["order_id"]
            try:
                status_resp = self.tools.exchange._request(
                    "GET", f"/api/v3/brokerage/orders/historical/{order_id}"
                )
                cb_order = status_resp.get("order", {})
                cb_status = cb_order.get("status", "")

                if cb_status == "FILLED":
                    fills += 1
                    fill_price = float(cb_order.get("average_filled_price", order["price"]))

                    logger.info("GRID FILL: %s %s @ $%.2f (order %s)",
                                order["side"], self.pair, fill_price, order_id[:8])

                    # Update our record
                    self.db.execute(
                        "UPDATE grid_orders SET status='filled', fill_price=?, filled_at=CURRENT_TIMESTAMP WHERE id=?",
                        (fill_price, order["id"])
                    )
                    self.db.commit()

                    # Place counterpart order
                    self._place_counterpart(order, fill_price)

                elif cb_status in ("CANCELLED", "EXPIRED", "FAILED"):
                    self.db.execute(
                        "UPDATE grid_orders SET status=?, cancelled_at=CURRENT_TIMESTAMP WHERE id=?",
                        (cb_status.lower(), order["id"])
                    )
                    self.db.commit()

            except Exception as e:
                logger.debug("Order check failed for %s: %s", order_id[:8] if order_id else "?", e)

        if fills > 0:
            self._record_stats()
        return fills

    def _place_counterpart(self, filled_order, fill_price):
        """After a fill, place the opposite order one grid level away."""
        product = self._get_product_info(self.pair)
        base_increment = product["base_increment"]
        quote_increment = product["quote_increment"]
        spacing = self.config["grid_spacing_pct"]

        if filled_order["side"] == "BUY":
            # Buy filled → place SELL one grid above
            sell_price = fill_price * (1 + spacing)
            level = {"side": "SELL", "level": filled_order["grid_level"] + 1, "price": sell_price}
            order_id = self._place_grid_order(level, base_increment, quote_increment)

            if order_id:
                # Link the counterpart
                self.db.execute(
                    "UPDATE grid_orders SET counterpart_order_id=? WHERE id=?",
                    (order_id, filled_order["id"])
                )

                # Calculate profit for this pair when sell fills
                expected_profit = (sell_price - fill_price) * filled_order["base_size"]
                fees = fill_price * filled_order["base_size"] * 0.004 + sell_price * filled_order["base_size"] * 0.004
                net_profit = expected_profit - fees
                logger.info("Grid round-trip pending: buy $%.2f → sell $%.2f | gross $%.4f - fees $%.4f = net $%.4f",
                            fill_price, sell_price, expected_profit, fees, net_profit)

        elif filled_order["side"] == "SELL":
            # Sell filled → place BUY one grid below
            buy_price = fill_price * (1 - spacing)

            # Calculate P&L for this completed round-trip
            # Find the original buy that triggered this sell
            self.total_round_trips += 1
            pnl = filled_order["base_size"] * (fill_price - filled_order["price"])
            fees = filled_order["base_size"] * filled_order["price"] * 0.004 + filled_order["base_size"] * fill_price * 0.004
            net_pnl = pnl - fees
            self.total_profit += net_pnl
            self.tools.record_pnl(net_pnl)

            self.db.execute(
                "UPDATE grid_orders SET pnl=? WHERE id=?",
                (round(net_pnl, 6), filled_order["id"])
            )

            logger.info("GRID PROFIT: round-trip #%d | $%.6f net (total: $%.6f)",
                        self.total_round_trips, net_pnl, self.total_profit)

            # Replace with a new buy
            level = {"side": "BUY", "level": filled_order["grid_level"] - 1, "price": buy_price}

            # Check if we have enough cash
            available = self.tools.get_available_cash()
            if available > self.config["min_reserve_usd"] + self.config["order_size_usd"]:
                order_id = self._place_grid_order(level, base_increment, quote_increment)

        self.db.commit()

    def check_recenter(self):
        """Check if price has moved far enough to recenter the grid."""
        if not self.grid_center:
            return False

        price = self.tools.get_price(self.pair)
        if not price:
            return False

        deviation = abs(price - self.grid_center) / self.grid_center
        if deviation >= self.config["recenter_threshold"]:
            logger.info("Recentering grid: price moved %.1f%% from center ($%.2f → $%.2f)",
                        deviation * 100, self.grid_center, price)
            return self.setup_grid()
        return False

    def _record_stats(self):
        """Record grid stats snapshot."""
        active_buys = self.db.execute(
            "SELECT COUNT(*) as cnt FROM grid_orders WHERE pair=? AND side='BUY' AND status='placed'",
            (self.pair,)
        ).fetchone()["cnt"]
        active_sells = self.db.execute(
            "SELECT COUNT(*) as cnt FROM grid_orders WHERE pair=? AND side='SELL' AND status='placed'",
            (self.pair,)
        ).fetchone()["cnt"]

        self.db.execute(
            """INSERT INTO grid_stats
               (pair, grid_center, total_round_trips, total_profit_usd,
                active_buy_orders, active_sell_orders)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (self.pair, self.grid_center, self.total_round_trips,
             self.total_profit, active_buys, active_sells)
        )
        self.db.commit()

    def print_status(self):
        """Print current grid status."""
        price = self.tools.get_price(self.pair) or 0
        active = self.db.execute(
            "SELECT side, COUNT(*) as cnt, MIN(price) as min_p, MAX(price) as max_p "
            "FROM grid_orders WHERE pair=? AND status='placed' GROUP BY side",
            (self.pair,)
        ).fetchall()

        filled_count = self.db.execute(
            "SELECT COUNT(*) as cnt FROM grid_orders WHERE pair=? AND status='filled'",
            (self.pair,)
        ).fetchone()["cnt"]

        total_pnl = self.db.execute(
            "SELECT COALESCE(SUM(pnl), 0) as total FROM grid_orders WHERE pair=? AND pnl IS NOT NULL",
            (self.pair,)
        ).fetchone()["total"]

        print(f"\n{'='*60}")
        print(f"  GRID TRADER | {self.pair} @ ${price:,.2f}")
        print(f"{'='*60}")
        print(f"  Grid center:    ${self.grid_center:,.2f}" if self.grid_center else "  Grid: not set")
        print(f"  Grid spacing:   {self.config['grid_spacing_pct']*100:.1f}%")
        print(f"  Order size:     ${self.config['order_size_usd']:.2f}")
        for row in active:
            print(f"  {row['side']} orders:    {row['cnt']} (${row['min_p']:,.2f} - ${row['max_p']:,.2f})")
        print(f"  Total fills:    {filled_count}")
        print(f"  Round-trips:    {self.total_round_trips}")
        print(f"  Total P&L:      ${total_pnl:+.6f}")
        print(f"{'='*60}\n")

    def run(self):
        """Main grid trading loop."""
        logger.info("Grid Trader starting on %s", self.pair)
        logger.info("Config: %s", json.dumps(self.config, indent=2))

        # Setup initial grid
        if not self.setup_grid():
            logger.error("Failed to setup grid. Exiting.")
            return

        self.print_status()
        cycle = 0

        while True:
            try:
                cycle += 1

                # Check for fills and place counterpart orders
                fills = self.check_fills()
                if fills > 0:
                    logger.info("Cycle %d: %d fills detected", cycle, fills)

                # Check if grid needs recentering
                if cycle % 10 == 0:
                    self.check_recenter()

                # Status update every 30 cycles
                if cycle % 30 == 0:
                    self.print_status()
                    self.tools.record_snapshot(agent="grid_trader")

                time.sleep(self.config["check_interval"])

            except KeyboardInterrupt:
                logger.info("Grid Trader shutting down...")
                self.print_status()
                break
            except Exception as e:
                logger.error("Grid error: %s", e, exc_info=True)
                time.sleep(30)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        trader = GridTrader()
        trader.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "setup":
        trader = GridTrader()
        trader.setup_grid()
        trader.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "cancel":
        trader = GridTrader()
        cancelled = trader.tools.cancel_all_orders(trader.pair)
        print(f"Cancelled {cancelled} orders")
    else:
        config = {}
        # Allow pair override: python grid_trader.py run ETH-USD
        if len(sys.argv) > 2:
            config["pair"] = sys.argv[2]
        trader = GridTrader(config)
        trader.run()
