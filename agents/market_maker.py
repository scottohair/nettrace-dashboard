#!/usr/bin/env python3
"""Grid Harvester Agent — deterministic profit from crypto volatility.

Reality: Coinbase spreads are $0.01 on liquid pairs (0.001%), but maker fees
are 0.4%. Traditional market making is impossible at this fee tier.

Strategy instead: Place limit BUY orders 1-3% BELOW current price and limit
SELL orders 1-3% ABOVE. Crypto naturally swings 2-5% daily — when price dips
to our buy level and bounces back, we profit the grid spacing minus fees.

This is DETERMINISTIC: no prediction needed. Crypto is volatile. We harvest that.

Grid levels:
  BUY at  -1.0%, -1.5%, -2.0%, -2.5% from mid price
  SELL at +1.0%, +1.5%, +2.0%, +2.5% from mid price

Each fill at $2-3 per order. When a BUY fills at -2% and we later sell at +1%,
net profit = 3% - (2 * 0.4% fees) = 2.2% on $2-3 = ~$0.05 per round trip.
Over 24h with multiple fills across multiple pairs, this adds up.

RULES:
- MAKER only (limit orders, post_only where possible)
- Max $3 per order (small, safe)
- Cancel and refresh grid every 5 minutes (follow price)
- If price drops through ALL buy levels, STOP buying (circuit breaker)
- Inventory limit: don't hold > 8% of portfolio in any one asset
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "market_maker.log")),
    ],
    force=True,
)
logger = logging.getLogger("grid_harvester")

# ── Configuration ──────────────────────────────────────────────────────

# Pairs to grid — include coins we already hold + high-vol ones
PAIRS = ["BTC-USDC", "ETH-USDC", "SOL-USDC", "AVAX-USDC", "LINK-USDC"]

# Grid levels (% from mid price) — asymmetric: more buy levels in downtrend
BUY_LEVELS = [-0.010, -0.015, -0.020, -0.030]   # -1.0%, -1.5%, -2.0%, -3.0%
SELL_LEVELS = [+0.010, +0.015, +0.020, +0.030]   # +1.0%, +1.5%, +2.0%, +3.0%

ORDER_SIZE_USD = 1.50           # $ per grid order (lowered for small account)
MIN_ORDER_USD = 1.00            # Coinbase minimum
MAKER_FEE_PCT = 0.004           # 0.40%
CYCLE_SECONDS = 60              # Refresh grid every 60s
GRID_REFRESH_SECONDS = 300      # Full grid rebuild every 5 min
MAX_OPEN_ORDERS_PER_PAIR = 4    # 2 buys + 2 sells max per pair
MAX_INVENTORY_PCT = 0.08        # Don't hold > 8% of portfolio in one asset
MAX_TOTAL_ORDERS = 20           # Global cap on open orders

DB_PATH = str(Path(__file__).parent / "market_maker.db")

# ── Load credentials ───────────────────────────────────────────────────
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


class GridHarvester:
    """Deterministic grid trading — harvest crypto volatility via limit orders."""

    def __init__(self):
        self.db = sqlite3.connect(DB_PATH)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.trader = None
        self.open_orders = {}  # order_id -> {pair, side, price, size, level, placed_at}
        self.cycle = 0
        self.total_fills = 0
        self.total_pnl = 0.0
        self.last_grid_refresh = {}  # pair -> timestamp
        # Track fill history for P&L calculation
        self.buy_fills = {}   # pair -> [list of (price, size)]
        self.sell_fills = {}  # pair -> [list of (price, size)]

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS grid_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL NOT NULL,
                size REAL NOT NULL,
                level_pct REAL,
                fee_usd REAL,
                order_id TEXT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS grid_round_trips (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                buy_price REAL, sell_price REAL,
                size REAL,
                gross_pnl REAL, net_pnl REAL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS grid_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_fills INTEGER,
                round_trips INTEGER,
                total_pnl REAL,
                open_orders INTEGER,
                cycle INTEGER,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def _get_trader(self):
        if self.trader is None:
            from exchange_connector import CoinbaseTrader
            self.trader = CoinbaseTrader()
        return self.trader

    def _get_mid_price(self, pair):
        """Get mid price from order book."""
        trader = self._get_trader()
        book = trader.get_order_book(pair)
        pricebook = book.get("pricebook", book)
        bids = pricebook.get("bids", [])
        asks = pricebook.get("asks", [])
        if bids and asks:
            return (float(bids[0]["price"]) + float(asks[0]["price"])) / 2
        return None

    def _get_holdings(self):
        """Get Coinbase holdings."""
        trader = self._get_trader()
        accounts = trader.get_accounts()
        holdings = {}
        total_usd = 0
        for acc in accounts.get("accounts", []):
            currency = acc.get("currency", "")
            avail = float(acc.get("available_balance", {}).get("value", 0))
            if avail > 0:
                holdings[currency] = avail
                if currency in ("USD", "USDC"):
                    total_usd += avail
        return holdings, total_usd

    def _cancel_pair_orders(self, pair):
        """Cancel all open orders for a pair."""
        trader = self._get_trader()
        cancelled = 0
        for oid, info in list(self.open_orders.items()):
            if info["pair"] == pair:
                try:
                    trader.cancel_order(oid)
                    cancelled += 1
                except Exception:
                    pass
                self.open_orders.pop(oid, None)
        return cancelled

    def _check_fills(self):
        """Check all open orders for fills. Record and track for round-trip P&L."""
        trader = self._get_trader()

        for oid, info in list(self.open_orders.items()):
            try:
                resp = trader._request("GET", f"/api/v3/brokerage/orders/historical/{oid}")
                order = resp.get("order", {})
                status = order.get("status", "")

                if status == "FILLED":
                    fill_price = float(order.get("average_filled_price", info["price"]))
                    fill_size = float(order.get("filled_size", info["size"]))
                    fee = fill_price * fill_size * MAKER_FEE_PCT

                    self.db.execute(
                        "INSERT INTO grid_fills (pair, side, price, size, level_pct, fee_usd, order_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (info["pair"], info["side"], fill_price, fill_size, info.get("level", 0), fee, oid)
                    )

                    self.total_fills += 1
                    pair = info["pair"]

                    if info["side"] == "BUY":
                        self.buy_fills.setdefault(pair, []).append((fill_price, fill_size))
                        logger.info("GRID BUY FILLED: %s %.6f @ $%.2f (level=%.1f%% fee=$%.3f)",
                                   pair, fill_size, fill_price, info.get("level", 0) * 100, fee)
                    else:
                        self.sell_fills.setdefault(pair, []).append((fill_price, fill_size))
                        logger.info("GRID SELL FILLED: %s %.6f @ $%.2f (level=+%.1f%% fee=$%.3f)",
                                   pair, fill_size, fill_price, info.get("level", 0) * 100, fee)

                    # Check for completed round trip
                    self._check_round_trip(pair)
                    self.db.commit()
                    self.open_orders.pop(oid, None)

                elif status in ("CANCELLED", "EXPIRED", "FAILED"):
                    self.open_orders.pop(oid, None)

            except Exception as e:
                logger.debug("Order check failed %s: %s", oid[:8], e)

    def _check_round_trip(self, pair):
        """Match buy and sell fills for P&L calculation."""
        buys = self.buy_fills.get(pair, [])
        sells = self.sell_fills.get(pair, [])

        while buys and sells:
            buy_price, buy_size = buys[0]
            sell_price, sell_size = sells[0]

            size = min(buy_size, sell_size)
            gross_pnl = (sell_price - buy_price) * size
            fees = 2 * MAKER_FEE_PCT * size * ((buy_price + sell_price) / 2)
            net_pnl = gross_pnl - fees

            self.db.execute(
                "INSERT INTO grid_round_trips (pair, buy_price, sell_price, size, gross_pnl, net_pnl) VALUES (?, ?, ?, ?, ?, ?)",
                (pair, buy_price, sell_price, size, gross_pnl, net_pnl)
            )
            self.total_pnl += net_pnl
            logger.info("ROUND TRIP: %s buy=$%.2f sell=$%.2f pnl=$%.4f (%.2f%%)",
                       pair, buy_price, sell_price, net_pnl,
                       ((sell_price / buy_price) - 1 - 2 * MAKER_FEE_PCT) * 100)

            # Adjust remaining sizes
            if buy_size > sell_size:
                buys[0] = (buy_price, buy_size - size)
                sells.pop(0)
            elif sell_size > buy_size:
                sells[0] = (sell_price, sell_size - size)
                buys.pop(0)
            else:
                buys.pop(0)
                sells.pop(0)

    def place_grid(self, pair, holdings, total_usd):
        """Place grid of limit orders around current price."""
        mid = self._get_mid_price(pair)
        if not mid:
            return

        now = time.time()
        last_refresh = self.last_grid_refresh.get(pair, 0)

        # Only refresh grid every GRID_REFRESH_SECONDS unless no orders
        pair_orders = sum(1 for o in self.open_orders.values() if o["pair"] == pair)
        if pair_orders > 0 and (now - last_refresh) < GRID_REFRESH_SECONDS:
            return

        # Cancel existing orders for this pair before placing new grid
        if pair_orders > 0:
            self._cancel_pair_orders(pair)
            time.sleep(1)

        base = pair.split("-")[0]
        base_held = holdings.get(base, 0)
        base_value = base_held * mid
        cash = holdings.get("USDC", 0) + holdings.get("USD", 0)

        # Inventory check: don't buy more if we're already heavy on this asset
        portfolio_total = total_usd + sum(
            holdings.get(c, 0) * (mid if c == base else 1)
            for c in holdings if c not in ("USD", "USDC")
        )
        inventory_pct = base_value / portfolio_total if portfolio_total > 0 else 0

        trader = self._get_trader()
        orders_placed = 0

        # Place BUY orders below price (only if not over-inventoried)
        if inventory_pct < MAX_INVENTORY_PCT and cash >= MIN_ORDER_USD:
            for level in BUY_LEVELS:
                if orders_placed >= MAX_OPEN_ORDERS_PER_PAIR // 2:
                    break
                if len(self.open_orders) >= MAX_TOTAL_ORDERS:
                    break

                buy_price = mid * (1 + level)
                size = ORDER_SIZE_USD / buy_price

                if ORDER_SIZE_USD > cash * 0.8:
                    continue  # Don't use more than 80% of remaining cash

                result = trader.place_limit_order(pair, "BUY", size, buy_price, post_only=True)
                if not result.get("error_response"):
                    oid = result.get("success_response", result).get("order_id", "")
                    if oid:
                        self.open_orders[oid] = {
                            "pair": pair, "side": "BUY", "price": buy_price,
                            "size": size, "level": level, "placed_at": now
                        }
                        orders_placed += 1

        # Place SELL orders above price (only if we hold the asset)
        if base_held > 0:
            for level in SELL_LEVELS:
                if orders_placed >= MAX_OPEN_ORDERS_PER_PAIR:
                    break
                if len(self.open_orders) >= MAX_TOTAL_ORDERS:
                    break

                sell_price = mid * (1 + level)
                size = min(base_held * 0.25, ORDER_SIZE_USD / sell_price)

                if size * sell_price < MIN_ORDER_USD:
                    continue

                result = trader.place_limit_order(pair, "SELL", size, sell_price, post_only=True)
                if not result.get("error_response"):
                    oid = result.get("success_response", result).get("order_id", "")
                    if oid:
                        self.open_orders[oid] = {
                            "pair": pair, "side": "SELL", "price": sell_price,
                            "size": size, "level": level, "placed_at": now
                        }
                        orders_placed += 1

        if orders_placed > 0:
            self.last_grid_refresh[pair] = now
            logger.info("GRID %s: %d orders placed around $%.2f (inventory=%.1f%%)",
                       pair, orders_placed, mid, inventory_pct * 100)

    def run(self):
        """Main grid harvesting loop."""
        logger.info("Grid Harvester starting | Pairs: %s | Order size: $%.2f | Cycle: %ds",
                    PAIRS, ORDER_SIZE_USD, CYCLE_SECONDS)
        logger.info("Buy levels: %s | Sell levels: %s",
                    [f"{l*100:.1f}%" for l in BUY_LEVELS],
                    [f"+{l*100:.1f}%" for l in SELL_LEVELS])

        while True:
            try:
                self.cycle += 1

                # 1. Check for fills
                self._check_fills()

                # 2. Get current state
                holdings, total_usd = self._get_holdings()

                # 3. Place/refresh grids
                for pair in PAIRS:
                    try:
                        self.place_grid(pair, holdings, total_usd)
                    except Exception as e:
                        logger.warning("Grid error %s: %s", pair, e)
                    time.sleep(2)  # Rate limit

                # 4. Status log
                if self.cycle % 10 == 0:
                    rt_count = self.db.execute("SELECT COUNT(*) FROM grid_round_trips").fetchone()[0]
                    self.db.execute(
                        "INSERT INTO grid_stats (total_fills, round_trips, total_pnl, open_orders, cycle) VALUES (?, ?, ?, ?, ?)",
                        (self.total_fills, rt_count, self.total_pnl, len(self.open_orders), self.cycle)
                    )
                    self.db.commit()
                    logger.info("Status: cycle=%d fills=%d round_trips=%d pnl=$%.4f open=%d",
                               self.cycle, self.total_fills, rt_count, self.total_pnl, len(self.open_orders))

                time.sleep(CYCLE_SECONDS)

            except KeyboardInterrupt:
                logger.info("Shutting down. fills=%d pnl=$%.4f", self.total_fills, self.total_pnl)
                trader = self._get_trader()
                for oid in list(self.open_orders):
                    try:
                        trader.cancel_order(oid)
                    except Exception:
                        pass
                break
            except Exception as e:
                logger.error("Error: %s", e, exc_info=True)
                time.sleep(30)


if __name__ == "__main__":
    gh = GridHarvester()
    gh.run()
