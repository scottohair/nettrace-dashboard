#!/usr/bin/env python3
"""DCA Accumulator Bot — dollar-cost average into quality crypto assets.

Strategy:
  - Allocate $0.50/day across BTC, ETH, SOL
  - Place limit orders slightly below market (0.1-0.2% below spot)
  - Buy extra on dips > 2% (detected via candle analysis)
  - NEVER sell — pure accumulation mode until portfolio > $100
  - All orders are post_only=True (maker fee 0.4%)

Game Theory: DCA eliminates timing risk. We accumulate at the average price
over time, which converges to fair value. In a long-term uptrend (crypto
since inception), this is a winning strategy.

RULES:
  - Never sell
  - Keep $4 reserve
  - Only limit orders (maker fee)
  - Max $5 per single order
"""

import json
import logging
import os
import sqlite3
import sys
import time
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
        logging.FileHandler(str(Path(__file__).parent / "dca_bot.log")),
    ]
)
logger = logging.getLogger("dca_bot")

DCA_DB = str(Path(__file__).parent / "dca_bot.db")

# DCA Configuration
ALLOCATIONS = [
    {"pair": "BTC-USDC", "weight": 0.50, "name": "Bitcoin"},
    {"pair": "ETH-USDC", "weight": 0.30, "name": "Ethereum"},
    {"pair": "SOL-USDC", "weight": 0.20, "name": "Solana"},
]

DAILY_BUDGET_USD = 0.50       # Total daily DCA spend
MIN_RESERVE_USD = 4.00        # Always keep in reserve
DIP_THRESHOLD_PCT = 0.02      # Buy extra on 2%+ dips
DIP_MULTIPLIER = 2.0          # Double allocation on dips
BELOW_SPOT_PCT = 0.002        # Place orders 0.2% below spot
CHECK_INTERVAL = 3600         # Check every hour (24 checks/day)
ORDER_CANCEL_HOURS = 4        # Cancel unfilled orders after 4 hours


class DCABot:
    """Dollar-cost averaging accumulator bot."""

    def __init__(self):
        from agent_tools import AgentTools
        self.tools = AgentTools()
        self.db = sqlite3.connect(DCA_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS dca_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                price REAL NOT NULL,
                base_size REAL NOT NULL,
                usd_amount REAL NOT NULL,
                order_id TEXT,
                order_type TEXT DEFAULT 'dca',
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                filled_at TIMESTAMP,
                cancelled_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS dca_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL UNIQUE,
                total_spent REAL DEFAULT 0.0,
                orders_placed INTEGER DEFAULT 0,
                orders_filled INTEGER DEFAULT 0,
                dip_buys INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS dca_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL UNIQUE,
                total_invested_usd REAL DEFAULT 0.0,
                total_base_acquired REAL DEFAULT 0.0,
                avg_entry_price REAL DEFAULT 0.0,
                orders_count INTEGER DEFAULT 0
            );
        """)
        self.db.commit()

    def _get_today(self):
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _get_daily_spent(self):
        """How much we've already spent today."""
        today = self._get_today()
        row = self.db.execute(
            "SELECT total_spent FROM dca_daily WHERE date=?", (today,)
        ).fetchone()
        return row["total_spent"] if row else 0.0

    def _record_daily_spend(self, amount, is_dip=False):
        today = self._get_today()
        self.db.execute(
            """INSERT INTO dca_daily (date, total_spent, orders_placed, dip_buys)
               VALUES (?, ?, 1, ?)
               ON CONFLICT(date) DO UPDATE SET
                   total_spent = total_spent + ?,
                   orders_placed = orders_placed + 1,
                   dip_buys = dip_buys + ?""",
            (today, amount, 1 if is_dip else 0, amount, 1 if is_dip else 0)
        )
        self.db.commit()

    def _update_holdings(self, pair, usd_amount, base_amount):
        """Update running average entry price and total invested."""
        existing = self.db.execute(
            "SELECT * FROM dca_holdings WHERE pair=?", (pair,)
        ).fetchone()

        if existing:
            new_invested = existing["total_invested_usd"] + usd_amount
            new_base = existing["total_base_acquired"] + base_amount
            new_avg = new_invested / new_base if new_base > 0 else 0
            self.db.execute(
                """UPDATE dca_holdings SET
                       total_invested_usd=?, total_base_acquired=?,
                       avg_entry_price=?, orders_count=orders_count+1
                   WHERE pair=?""",
                (new_invested, new_base, new_avg, pair)
            )
        else:
            avg = usd_amount / base_amount if base_amount > 0 else 0
            self.db.execute(
                """INSERT INTO dca_holdings
                   (pair, total_invested_usd, total_base_acquired, avg_entry_price, orders_count)
                   VALUES (?, ?, ?, ?, 1)""",
                (pair, usd_amount, base_amount, avg)
            )
        self.db.commit()

    def detect_dip(self, pair):
        """Check if current price is a dip (>2% below 24h high)."""
        try:
            candles = self.tools.get_candles(pair, granularity=3600, hours=24)
            if len(candles) < 6:
                return False, 0

            highs = [c["high"] for c in candles]
            high_24h = max(highs)
            current = candles[-1]["close"]
            drop = (high_24h - current) / high_24h

            return drop >= DIP_THRESHOLD_PCT, drop
        except Exception as e:
            logger.debug("Dip detection failed for %s: %s", pair, e)
            return False, 0

    def place_dca_order(self, pair, usd_amount, order_type="dca"):
        """Place a DCA limit buy order slightly below spot."""
        price = self.tools.get_price(pair)
        if not price:
            logger.warning("No price for %s", pair)
            return None

        # Place 0.2% below spot for better fill as maker
        limit_price = price * (1 - BELOW_SPOT_PCT)

        # Calculate base size
        base_size = usd_amount / limit_price

        order_id = self.tools.place_limit_buy(pair, limit_price, base_size, agent_name="dca_bot")

        if order_id:
            self.db.execute(
                """INSERT INTO dca_orders (pair, price, base_size, usd_amount, order_id, order_type, status)
                   VALUES (?, ?, ?, ?, ?, ?, 'placed')""",
                (pair, limit_price, base_size, usd_amount, order_id, order_type)
            )
            self.db.commit()
            self._record_daily_spend(usd_amount, is_dip=(order_type == "dip_buy"))

            logger.info("DCA %s: $%.2f @ $%.2f (%s) → order %s",
                        pair, usd_amount, limit_price, order_type, order_id[:8])
        else:
            logger.warning("DCA order failed for %s", pair)

        return order_id

    def execute_dca_round(self):
        """Execute one DCA round — allocate daily budget across pairs."""
        daily_spent = self._get_daily_spent()
        remaining = DAILY_BUDGET_USD - daily_spent

        if remaining < 0.10:
            logger.debug("Daily budget exhausted ($%.2f of $%.2f spent)", daily_spent, DAILY_BUDGET_USD)
            return

        # Check available cash
        available = self.tools.get_available_cash()
        if available < MIN_RESERVE_USD + 0.50:
            logger.debug("Insufficient cash: $%.2f (need $%.2f reserve + order)", available, MIN_RESERVE_USD)
            return

        # DCA runs ~4x/day (every 6 hours), so divide remaining by checks left today
        now = datetime.now(timezone.utc)
        hours_left = max(1, 24 - now.hour)
        checks_left = max(1, hours_left // (CHECK_INTERVAL // 3600))
        round_budget = remaining / checks_left

        # Cap at what we can afford
        deployable = available - MIN_RESERVE_USD
        round_budget = min(round_budget, deployable)

        if round_budget < 0.10:
            return

        orders_placed = 0
        for alloc in ALLOCATIONS:
            pair = alloc["pair"]
            usd = round_budget * alloc["weight"]

            if usd < 0.10:
                continue

            # Check for dip — buy more on dips
            is_dip, drop_pct = self.detect_dip(pair)
            if is_dip:
                dip_extra = min(usd * DIP_MULTIPLIER, deployable * 0.3)
                logger.info("DIP DETECTED: %s down %.1f%% — adding $%.2f extra", pair, drop_pct * 100, dip_extra)
                usd += dip_extra

            if self.place_dca_order(pair, round(usd, 2), "dip_buy" if is_dip else "dca"):
                orders_placed += 1

            time.sleep(1)  # Rate limit

        if orders_placed:
            logger.info("DCA round complete: %d orders, $%.2f spent today", orders_placed, self._get_daily_spent())

    def check_fills(self):
        """Check for filled DCA orders and update holdings."""
        pending = self.db.execute(
            "SELECT * FROM dca_orders WHERE status='placed' AND order_id IS NOT NULL"
        ).fetchall()

        fills = 0
        for order in pending:
            try:
                result = self.tools.exchange._request(
                    "GET", f"/api/v3/brokerage/orders/historical/{order['order_id']}"
                )
                cb_order = result.get("order", {})
                status = cb_order.get("status", "")

                if status == "FILLED":
                    fills += 1
                    fill_price = float(cb_order.get("average_filled_price", order["price"]))
                    filled_size = float(cb_order.get("filled_size", order["base_size"]))

                    self.db.execute(
                        "UPDATE dca_orders SET status='filled', filled_at=CURRENT_TIMESTAMP WHERE id=?",
                        (order["id"],)
                    )
                    self._update_holdings(order["pair"], order["usd_amount"], filled_size)

                    logger.info("DCA FILL: %s %.8f @ $%.2f ($%.2f)", order["pair"],
                                filled_size, fill_price, order["usd_amount"])

                    # Update daily filled count
                    today = self._get_today()
                    self.db.execute(
                        """UPDATE dca_daily SET orders_filled = orders_filled + 1
                           WHERE date=?""", (today,)
                    )

                elif status in ("CANCELLED", "EXPIRED", "FAILED"):
                    self.db.execute(
                        "UPDATE dca_orders SET status=?, cancelled_at=CURRENT_TIMESTAMP WHERE id=?",
                        (status.lower(), order["id"])
                    )

            except Exception as e:
                logger.debug("Order check failed: %s", e)

        self.db.commit()
        return fills

    def cancel_stale_orders(self):
        """Cancel orders older than ORDER_CANCEL_HOURS that haven't filled."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=ORDER_CANCEL_HOURS)
        stale = self.db.execute(
            "SELECT * FROM dca_orders WHERE status='placed' AND created_at < ? AND order_id IS NOT NULL",
            (cutoff.isoformat(),)
        ).fetchall()

        cancelled = 0
        for order in stale:
            if self.tools.cancel_order(order["order_id"]):
                self.db.execute(
                    "UPDATE dca_orders SET status='cancelled', cancelled_at=CURRENT_TIMESTAMP WHERE id=?",
                    (order["id"],)
                )
                cancelled += 1

        if cancelled:
            self.db.commit()
            logger.info("Cancelled %d stale DCA orders (>%dh old)", cancelled, ORDER_CANCEL_HOURS)

        return cancelled

    def print_status(self):
        """Print DCA bot status."""
        holdings = self.db.execute("SELECT * FROM dca_holdings ORDER BY total_invested_usd DESC").fetchall()
        daily = self.db.execute("SELECT * FROM dca_daily ORDER BY date DESC LIMIT 7").fetchall()
        pending = self.db.execute(
            "SELECT COUNT(*) as cnt FROM dca_orders WHERE status='placed'"
        ).fetchone()["cnt"]

        print(f"\n{'='*60}")
        print(f"  DCA ACCUMULATOR BOT")
        print(f"{'='*60}")

        if holdings:
            total_invested = sum(h["total_invested_usd"] for h in holdings)
            total_value = 0
            for h in holdings:
                price = self.tools.get_price(h["pair"]) or 0
                current_value = h["total_base_acquired"] * price
                total_value += current_value
                pnl = current_value - h["total_invested_usd"]
                pnl_pct = (pnl / h["total_invested_usd"] * 100) if h["total_invested_usd"] > 0 else 0
                print(f"  {h['pair']:<10} invested: ${h['total_invested_usd']:.2f} | "
                      f"held: {h['total_base_acquired']:.8f} | "
                      f"avg: ${h['avg_entry_price']:,.2f} | "
                      f"now: ${price:,.2f} | "
                      f"P&L: ${pnl:+.4f} ({pnl_pct:+.1f}%)")
            print(f"  {'─'*56}")
            total_pnl = total_value - total_invested
            print(f"  Total invested: ${total_invested:.2f} | Value: ${total_value:.2f} | P&L: ${total_pnl:+.4f}")

        print(f"\n  Pending orders: {pending}")
        print(f"  Daily budget: ${DAILY_BUDGET_USD:.2f} | Spent today: ${self._get_daily_spent():.2f}")

        if daily:
            print(f"\n  Recent daily activity:")
            for d in daily:
                print(f"    {d['date']}: ${d['total_spent']:.2f} spent, "
                      f"{d['orders_placed']} placed, {d['orders_filled']} filled, "
                      f"{d['dip_buys']} dip buys")

        print(f"{'='*60}\n")

    def run(self):
        """Main DCA loop."""
        logger.info("DCA Bot starting with $%.2f/day budget across %d assets",
                     DAILY_BUDGET_USD, len(ALLOCATIONS))

        cycle = 0
        while True:
            try:
                cycle += 1

                # Check for fills
                fills = self.check_fills()
                if fills:
                    logger.info("Cycle %d: %d DCA orders filled", cycle, fills)

                # Execute DCA allocation
                self.execute_dca_round()

                # Cancel stale orders
                if cycle % 4 == 0:  # Every 4 hours
                    self.cancel_stale_orders()

                # Status every 6 hours
                if cycle % 6 == 0:
                    self.print_status()
                    self.tools.record_snapshot(agent="dca_bot")

                time.sleep(CHECK_INTERVAL)

            except KeyboardInterrupt:
                logger.info("DCA Bot shutting down...")
                self.print_status()
                break
            except Exception as e:
                logger.error("DCA error: %s", e, exc_info=True)
                time.sleep(300)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        bot = DCABot()
        bot.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "buy":
        # Manual single DCA buy
        bot = DCABot()
        pair = sys.argv[2] if len(sys.argv) > 2 else "BTC-USD"
        amount = float(sys.argv[3]) if len(sys.argv) > 3 else 0.50
        bot.place_dca_order(pair, amount, "manual")
        print(f"Placed DCA buy: {pair} ${amount:.2f}")
    else:
        bot = DCABot()
        bot.run()
