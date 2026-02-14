#!/usr/bin/env python3
"""DEX Grid Trader — HFT grid bot on Uniswap Base L2.

Market-making on Uniswap V3 (Base chain) with the lowest fees in crypto:
  - Uniswap 1bp pool: 0.01% per swap
  - Base L2 gas: ~$0.01 per tx
  - Total cost: ~$0.0105 per swap

Grid Strategy (adapted for AMM — no limit orders):
  1. Set price grid levels around current ETH/USDC price
  2. Poll price every 15 seconds via RPC
  3. Price crosses BUY level → swap USDC → ETH
  4. Price crosses SELL level → swap ETH → USDC
  5. Each round-trip = grid_spacing - 2x(fee + gas) = profit

Economics at 1% grid, $5 trades:
  Gross per round-trip: $0.05
  Fees: 2 × $0.0105 = $0.021
  Net per round-trip: $0.029
  20 round-trips/day: $0.58/day

RULES (NEVER VIOLATE):
  - Max $5 per swap (trading rule)
  - $2 daily loss limit (HARDSTOP)
  - Grid spacing MUST exceed 2x(fee+gas) in percentage terms
  - Keep reserve — never deploy 100% of checking balance
  - Only trade from checking sub-account
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
        logging.FileHandler(str(Path(__file__).parent / "dex_grid_trader.log")),
    ]
)
logger = logging.getLogger("dex_grid_trader")

GRID_DB = str(Path(__file__).parent / "dex_grid_trader.db")
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")
FLY_URL = "https://nettrace-dashboard.fly.dev"

# Wallet config (from env — encrypted private key decrypted at runtime)
WALLET_ADDRESS = os.environ.get("WALLET_ADDRESS", "")
WALLET_KEY_ENC = os.environ.get("WALLET_PRIVATE_KEY_ENC", "")
# SECRET_KEY is on Fly.io — for local dev, set it or use the raw key directly
SECRET_KEY = os.environ.get("SECRET_KEY", "")
CREDENTIAL_ENCRYPTION_KEY = os.environ.get("CREDENTIAL_ENCRYPTION_KEY", "")
# Fallback: raw private key for local development (never stored in git)
WALLET_PRIVATE_KEY_RAW = os.environ.get("WALLET_PRIVATE_KEY", "")
ALLOW_RAW_WALLET_PRIVATE_KEY = str(os.environ.get("ALLOW_RAW_WALLET_PRIVATE_KEY", "0")).lower() not in ("0", "false", "no")
APP_ENV = str(os.environ.get("APP_ENV") or os.environ.get("FLASK_ENV") or os.environ.get("ENV") or "production").lower()
IS_PRODUCTION = APP_ENV in {"prod", "production"}

# Grid configuration
DEFAULT_CONFIG = {
    "pair": "WETH-USDC",
    "chain": "base",
    "grid_spacing_pct": 0.01,     # 1.0% between levels
    "levels_above": 2,
    "levels_below": 2,
    "order_size_usd": 5.00,       # max $5 per trade
    "min_reserve_usd": 5.00,      # keep $5 reserve in checking
    "check_interval": 15,         # poll price every 15s
    "max_swaps_per_hour": 20,     # rate limit
    "recenter_threshold": 0.03,   # recenter if price moves 3%
    "max_slippage_pct": 0.005,    # 0.5% max slippage
    "max_daily_loss_usd": 2.00,   # HARDSTOP
}


def _decrypt_private_key():
    """Decrypt the wallet private key from WALLET_PRIVATE_KEY_ENC."""
    # Fallback: raw key for local development
    if WALLET_PRIVATE_KEY_RAW:
        if IS_PRODUCTION and not ALLOW_RAW_WALLET_PRIVATE_KEY:
            raise ValueError("Raw wallet private key is blocked in production; use encrypted key material")
        return WALLET_PRIVATE_KEY_RAW

    decrypt_password = CREDENTIAL_ENCRYPTION_KEY or SECRET_KEY
    if not WALLET_KEY_ENC or not decrypt_password:
        raise ValueError(
            "Set CREDENTIAL_ENCRYPTION_KEY (preferred) or SECRET_KEY env var (or use local dev raw key override)"
        )

    # Import decrypt from app
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from app import decrypt_credential
    return decrypt_credential(WALLET_KEY_ENC, decrypt_password)


def _get_eth_price():
    """Get current ETH/USD price from Coinbase (fast, free, no auth)."""
    try:
        url = "https://api.coinbase.com/v2/prices/ETH-USD/spot"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        return float(data["data"]["amount"])
    except Exception as e:
        logger.error("Price fetch failed: %s", e)
        return None


class DEXGridTrader:
    """Grid trading bot for Uniswap V3 on Base L2."""

    def __init__(self, config=None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.chain = self.config["chain"]
        self.db = sqlite3.connect(GRID_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.grid_center = None
        self.total_round_trips = 0
        self.total_profit = 0.0
        self.daily_loss = 0.0
        self.swaps_this_hour = 0
        self.hour_start = time.time()
        self._private_key = None
        self._dex = None
        self._wallet = None

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS dex_grid_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                chain TEXT NOT NULL,
                side TEXT NOT NULL,
                grid_level REAL NOT NULL,
                trigger_price REAL NOT NULL,
                swap_amount_usd REAL NOT NULL,
                tx_hash TEXT,
                status TEXT DEFAULT 'pending',
                executed_price REAL,
                amount_in REAL,
                amount_out REAL,
                gas_cost_usd REAL,
                pnl REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                executed_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS dex_grid_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                chain TEXT NOT NULL,
                grid_center REAL,
                eth_price REAL,
                total_round_trips INTEGER DEFAULT 0,
                total_profit_usd REAL DEFAULT 0.0,
                total_gas_usd REAL DEFAULT 0.0,
                swaps_today INTEGER DEFAULT 0,
                daily_pnl REAL DEFAULT 0.0,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

    def _get_private_key(self):
        """Lazy-load and cache decrypted private key."""
        if self._private_key is None:
            self._private_key = _decrypt_private_key()
            logger.info("Private key decrypted successfully")
        return self._private_key

    def _get_dex(self):
        """Lazy-load DEX connector."""
        if self._dex is None:
            from dex_connector import DEXConnector
            pk = self._get_private_key()
            self._dex = DEXConnector(
                wallet_address=WALLET_ADDRESS,
                private_key=pk,
                chain=self.chain
            )
        return self._dex

    def _get_wallet(self):
        """Lazy-load wallet connector."""
        if self._wallet is None:
            from wallet_connector import WalletConnector
            pk = self._get_private_key()
            self._wallet = WalletConnector(
                WALLET_ADDRESS,
                chain=self.chain,
                private_key=pk
            )
        return self._wallet

    def get_balances(self):
        """Get current ETH and USDC balances on Base."""
        wallet = self._get_wallet()
        try:
            balances = wallet.get_balances()
            eth_bal = balances.get("ETH", 0) + balances.get("WETH", 0)
            usdc_bal = balances.get("USDC", 0)
            return {"eth": eth_bal, "usdc": usdc_bal}
        except Exception as e:
            logger.error("Balance check failed: %s", e)
            return {"eth": 0, "usdc": 0}

    def calculate_grid_levels(self, center_price):
        """Calculate grid price levels around center."""
        spacing = self.config["grid_spacing_pct"]
        levels = []

        # Buy levels below current price (swap USDC → ETH when price drops)
        for i in range(1, self.config["levels_below"] + 1):
            price = center_price * (1 - spacing * i)
            levels.append({"side": "BUY", "level": -i, "price": price})

        # Sell levels above current price (swap ETH → USDC when price rises)
        for i in range(1, self.config["levels_above"] + 1):
            price = center_price * (1 + spacing * i)
            levels.append({"side": "SELL", "level": i, "price": price})

        return levels

    def setup_grid(self):
        """Initialize grid around current price."""
        price = _get_eth_price()
        if not price:
            logger.error("Cannot get ETH price")
            return False

        self.grid_center = price
        balances = self.get_balances()

        logger.info("Setting up DEX grid on %s (Base L2)", self.config["pair"])
        logger.info("  ETH price: $%.2f", price)
        logger.info("  Balances: %.6f ETH, %.2f USDC", balances["eth"], balances["usdc"])
        logger.info("  Grid spacing: %.1f%%", self.config["grid_spacing_pct"] * 100)
        logger.info("  Swap size: $%.2f", self.config["order_size_usd"])

        eth_value_usd = balances["eth"] * price
        total_value = eth_value_usd + balances["usdc"]

        if total_value < self.config["min_reserve_usd"]:
            logger.warning("Total value ($%.2f) below reserve ($%.2f). Cannot trade.",
                          total_value, self.config["min_reserve_usd"])
            return False

        # Calculate grid levels
        levels = self.calculate_grid_levels(price)

        # Clear old pending grid levels
        self.db.execute(
            "UPDATE dex_grid_orders SET status='cancelled' WHERE pair=? AND chain=? AND status='pending'",
            (self.config["pair"], self.chain)
        )

        # Insert new grid levels as "watching" (we monitor price and execute when crossed)
        for level in levels:
            self.db.execute(
                """INSERT INTO dex_grid_orders (pair, chain, side, grid_level, trigger_price, swap_amount_usd, status)
                   VALUES (?, ?, ?, ?, ?, ?, 'watching')""",
                (self.config["pair"], self.chain, level["side"],
                 level["level"], round(level["price"], 2), self.config["order_size_usd"])
            )
        self.db.commit()

        logger.info("Grid setup: %d levels watching", len(levels))
        self._record_stats(price)
        return True

    def check_price_and_execute(self):
        """Poll price and execute swaps when grid levels are crossed."""
        price = _get_eth_price()
        if not price:
            return 0

        # Rate limiting
        if time.time() - self.hour_start > 3600:
            self.swaps_this_hour = 0
            self.hour_start = time.time()

        if self.swaps_this_hour >= self.config["max_swaps_per_hour"]:
            return 0

        # Daily loss limit check
        if self.daily_loss >= self.config["max_daily_loss_usd"]:
            logger.warning("HARDSTOP: Daily loss limit reached ($%.2f)", self.daily_loss)
            return -1

        # Find triggered grid levels
        watching = self.db.execute(
            "SELECT * FROM dex_grid_orders WHERE pair=? AND chain=? AND status='watching'",
            (self.config["pair"], self.chain)
        ).fetchall()

        executions = 0
        for order in watching:
            triggered = False

            if order["side"] == "BUY" and price <= order["trigger_price"]:
                triggered = True
            elif order["side"] == "SELL" and price >= order["trigger_price"]:
                triggered = True

            if triggered:
                success = self._execute_swap(order, price)
                if success:
                    executions += 1
                    self.swaps_this_hour += 1

        return executions

    def _execute_swap(self, grid_order, current_price):
        """Execute a Uniswap swap for a triggered grid level."""
        side = grid_order["side"]
        amount_usd = grid_order["swap_amount_usd"]

        # Check balances before swap
        balances = self.get_balances()

        if side == "BUY":
            # Swap USDC → ETH
            if balances["usdc"] < amount_usd:
                logger.warning("Insufficient USDC ($%.2f) for $%.2f BUY", balances["usdc"], amount_usd)
                return False
            token_in, token_out = "USDC", "ETH"
            # USDC amount is the swap size
            amount_in = amount_usd
        else:
            # Swap ETH → USDC
            eth_needed = amount_usd / current_price
            if balances["eth"] < eth_needed:
                logger.warning("Insufficient ETH (%.6f) for %.6f SELL", balances["eth"], eth_needed)
                return False
            token_in, token_out = "ETH", "USDC"
            amount_in = eth_needed

        logger.info("GRID EXECUTE: %s %s → %s | $%.2f @ $%.2f",
                    side, token_in, token_out, amount_usd, current_price)

        try:
            dex = self._get_dex()

            # Get quote first
            quote = dex.get_quote_uniswap(token_in, token_out, amount_in, self.chain)
            if "error" in quote:
                logger.error("Quote failed: %s", quote["error"])
                return False

            # Check slippage
            if side == "BUY":
                expected_eth = amount_usd / current_price
                actual_eth = quote["amount_out"]
                slippage = abs(actual_eth - expected_eth) / expected_eth
            else:
                expected_usd = amount_in * current_price
                actual_usd = quote["amount_out"]
                slippage = abs(actual_usd - expected_usd) / expected_usd

            if slippage > self.config["max_slippage_pct"]:
                logger.warning("Slippage too high: %.2f%% > %.2f%%",
                              slippage * 100, self.config["max_slippage_pct"] * 100)
                return False

            # Execute swap
            result = dex.swap_uniswap(
                token_in, token_out, amount_in,
                slippage=self.config["max_slippage_pct"],
                chain=self.chain
            )

            if "error" in result:
                logger.error("Swap failed: %s", result["error"])
                return False

            tx_hash = result.get("tx_hash", "")
            amount_out = result.get("amount_out", 0)
            gas_usd = result.get("gas_cost_usd", 0.01)

            # Calculate P&L for this execution
            if side == "SELL":
                # We sold ETH → got USDC. Profit = USDC received - original cost
                # For grid trading, we approximate: profit comes from the round-trip
                pnl = -gas_usd  # Gas cost (profit tracked on round-trip completion)
            else:
                pnl = -gas_usd

            # Update order record
            self.db.execute(
                """UPDATE dex_grid_orders
                   SET status='executed', tx_hash=?, executed_price=?,
                       amount_in=?, amount_out=?, gas_cost_usd=?, pnl=?,
                       executed_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (tx_hash, current_price, amount_in, amount_out, gas_usd, pnl, grid_order["id"])
            )

            # Place counterpart grid level
            self._place_counterpart(grid_order, current_price)

            self.db.commit()

            logger.info("GRID SWAP OK: %s %s → %s | in=%.6f out=%.6f | gas=$%.4f | tx=%s",
                        side, token_in, token_out, amount_in, amount_out, gas_usd,
                        tx_hash[:16] if tx_hash else "?")

            # Track round-trip profit when a SELL completes
            if side == "SELL":
                self._check_round_trip_profit(grid_order, current_price, amount_out, gas_usd)

            return True

        except Exception as e:
            logger.error("Swap execution error: %s", e, exc_info=True)
            return False

    def _place_counterpart(self, executed_order, execution_price):
        """After execution, place the opposite grid level."""
        spacing = self.config["grid_spacing_pct"]

        if executed_order["side"] == "BUY":
            # Buy executed → watch for SELL one grid above
            sell_price = execution_price * (1 + spacing)
            new_level = {"side": "SELL", "level": executed_order["grid_level"] + 1, "price": sell_price}
        else:
            # Sell executed → watch for BUY one grid below
            buy_price = execution_price * (1 - spacing)
            new_level = {"side": "BUY", "level": executed_order["grid_level"] - 1, "price": buy_price}

        self.db.execute(
            """INSERT INTO dex_grid_orders (pair, chain, side, grid_level, trigger_price, swap_amount_usd, status)
               VALUES (?, ?, ?, ?, ?, ?, 'watching')""",
            (self.config["pair"], self.chain, new_level["side"],
             new_level["level"], round(new_level["price"], 2), self.config["order_size_usd"])
        )

        logger.info("Grid counterpart: watching %s @ $%.2f", new_level["side"], new_level["price"])

    def _check_round_trip_profit(self, sell_order, sell_price, usdc_received, gas_usd):
        """Check if a sell completes a round-trip and calculate profit."""
        # Find the most recent corresponding BUY
        buy = self.db.execute(
            """SELECT executed_price, amount_in, gas_cost_usd FROM dex_grid_orders
               WHERE pair=? AND chain=? AND side='BUY' AND status='executed'
               ORDER BY executed_at DESC LIMIT 1""",
            (self.config["pair"], self.chain)
        ).fetchone()

        if buy and buy["executed_price"]:
            buy_price = buy["executed_price"]
            buy_gas = buy["gas_cost_usd"] or 0.01
            price_gain_pct = (sell_price - buy_price) / buy_price
            gross_profit = self.config["order_size_usd"] * price_gain_pct
            total_fees = gas_usd + buy_gas + (self.config["order_size_usd"] * 0.0001 * 2)  # 2x Uniswap 1bp fee
            net_profit = gross_profit - total_fees

            self.total_round_trips += 1
            self.total_profit += net_profit

            if net_profit < 0:
                self.daily_loss += abs(net_profit)

            logger.info("ROUND-TRIP #%d: buy $%.2f → sell $%.2f | gross $%.4f - fees $%.4f = net $%.4f | total P&L: $%.4f",
                        self.total_round_trips, buy_price, sell_price,
                        gross_profit, total_fees, net_profit, self.total_profit)

    def check_recenter(self):
        """Recenter grid if price moved too far."""
        if not self.grid_center:
            return False

        price = _get_eth_price()
        if not price:
            return False

        deviation = abs(price - self.grid_center) / self.grid_center
        if deviation >= self.config["recenter_threshold"]:
            logger.info("Recentering: price moved %.1f%% ($%.2f → $%.2f)",
                        deviation * 100, self.grid_center, price)
            return self.setup_grid()
        return False

    def _record_stats(self, price=None):
        """Record grid stats snapshot."""
        if not price:
            price = _get_eth_price() or 0

        swaps_today = self.db.execute(
            "SELECT COUNT(*) as cnt FROM dex_grid_orders WHERE chain=? AND status='executed' AND date(executed_at)=date('now')",
            (self.chain,)
        ).fetchone()["cnt"]

        self.db.execute(
            """INSERT INTO dex_grid_stats
               (pair, chain, grid_center, eth_price, total_round_trips,
                total_profit_usd, swaps_today, daily_pnl)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (self.config["pair"], self.chain, self.grid_center, price,
             self.total_round_trips, self.total_profit, swaps_today, -self.daily_loss)
        )
        self.db.commit()

    def push_snapshot(self):
        """Push stats to Fly.io dashboard."""
        if not NETTRACE_API_KEY:
            return
        try:
            balances = self.get_balances()
            price = _get_eth_price() or 0
            total_value = balances["eth"] * price + balances["usdc"]

            payload = json.dumps({
                "user_id": 2,
                "total_value_usd": round(total_value, 2),
                "daily_pnl": round(self.total_profit, 4),
                "trades_today": self.swaps_this_hour,
                "trades_total": self.total_round_trips,
                "holdings": {
                    "ETH (Base)": {"amount": balances["eth"], "value_usd": round(balances["eth"] * price, 2)},
                    "USDC (Base)": {"amount": balances["usdc"], "value_usd": balances["usdc"]},
                },
                "trades": [],
            }).encode()

            url = f"{FLY_URL}/api/trading-snapshot"
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {NETTRACE_API_KEY}"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                json.loads(resp.read().decode())
        except Exception as e:
            logger.debug("Snapshot push failed: %s", e)

    def print_status(self):
        """Print current grid status."""
        price = _get_eth_price() or 0
        balances = self.get_balances()

        watching = self.db.execute(
            "SELECT side, COUNT(*) as cnt, MIN(trigger_price) as min_p, MAX(trigger_price) as max_p "
            "FROM dex_grid_orders WHERE pair=? AND chain=? AND status='watching' GROUP BY side",
            (self.config["pair"], self.chain)
        ).fetchall()

        executed = self.db.execute(
            "SELECT COUNT(*) as cnt FROM dex_grid_orders WHERE pair=? AND chain=? AND status='executed'",
            (self.config["pair"], self.chain)
        ).fetchone()["cnt"]

        print(f"\n{'='*60}")
        print(f"  DEX GRID TRADER | {self.config['pair']} on {self.chain.upper()}")
        print(f"{'='*60}")
        print(f"  ETH Price:      ${price:,.2f}")
        print(f"  Grid center:    ${self.grid_center:,.2f}" if self.grid_center else "  Grid: not set")
        print(f"  Grid spacing:   {self.config['grid_spacing_pct']*100:.1f}%")
        print(f"  Swap size:      ${self.config['order_size_usd']:.2f}")
        print(f"  ETH balance:    {balances['eth']:.6f} (${balances['eth']*price:.2f})")
        print(f"  USDC balance:   {balances['usdc']:.2f}")
        for row in watching:
            print(f"  {row['side']} watching:  {row['cnt']} (${row['min_p']:,.2f} - ${row['max_p']:,.2f})")
        print(f"  Total executions: {executed}")
        print(f"  Round-trips:      {self.total_round_trips}")
        print(f"  Total P&L:        ${self.total_profit:+.4f}")
        print(f"  Daily loss:       ${self.daily_loss:.4f} / ${self.config['max_daily_loss_usd']:.2f}")
        print(f"{'='*60}\n")

    def run(self):
        """Main grid trading loop."""
        logger.info("DEX Grid Trader starting on %s (%s)", self.config["pair"], self.chain)
        logger.info("Wallet: %s", WALLET_ADDRESS)
        logger.info("Config: %s", json.dumps(self.config, indent=2))

        if not WALLET_ADDRESS:
            logger.error("WALLET_ADDRESS not set. Cannot trade.")
            return

        # Decrypt key on startup
        try:
            self._get_private_key()
        except Exception as e:
            logger.error("Key decryption failed: %s", e)
            return

        # Setup initial grid
        if not self.setup_grid():
            logger.error("Grid setup failed.")
            return

        self.print_status()
        cycle = 0

        while True:
            try:
                cycle += 1

                # Check price and execute any triggered grid levels
                result = self.check_price_and_execute()
                if result == -1:
                    logger.error("HARDSTOP: Daily loss limit. Shutting down.")
                    break
                if result > 0:
                    logger.info("Cycle %d: %d swaps executed", cycle, result)

                # Recenter every 60 cycles (~15 minutes)
                if cycle % 60 == 0:
                    self.check_recenter()

                # Status + snapshot every 120 cycles (~30 minutes)
                if cycle % 120 == 0:
                    self.print_status()
                    self._record_stats()
                    self.push_snapshot()

                time.sleep(self.config["check_interval"])

            except KeyboardInterrupt:
                logger.info("DEX Grid Trader shutting down...")
                self.print_status()
                break
            except Exception as e:
                logger.error("Grid loop error: %s", e, exc_info=True)
                time.sleep(30)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        trader = DEXGridTrader()
        trader.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "setup":
        trader = DEXGridTrader()
        trader.setup_grid()
        trader.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "balances":
        trader = DEXGridTrader()
        b = trader.get_balances()
        p = _get_eth_price() or 0
        print(f"ETH: {b['eth']:.6f} (${b['eth']*p:.2f})")
        print(f"USDC: {b['usdc']:.2f}")
        print(f"Total: ${b['eth']*p + b['usdc']:.2f}")
    else:
        trader = DEXGridTrader()
        trader.run()
