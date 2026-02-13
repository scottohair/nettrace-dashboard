#!/usr/bin/env python3
"""Capital Allocator — Principle protection + profit allocation.

Manages the treasury across sub-accounts:
  - checking:   Active trading capital
  - savings:    Reserve (never auto-traded)
  - growth:     Reinvestment pool (20-35% of profits)
  - subsavings: Long-term hold (manual withdrawal only)

Rules:
  1. Track principle vs gains at all times
  2. Once gains > 100% of principle → move principle to savings (LOCK)
  3. Allocate profits: 25% savings, 35% growth, 20% checking, 20% subsavings
  4. Pull to USD 4x daily (every 6 hours)
  5. Never trade savings or subsavings automatically
  6. Growth fund reinvests via best-performing strategy

Usage:
    from capital_allocator import CapitalAllocator
    alloc = CapitalAllocator()
    alloc.sync_balances()
    alloc.allocate_profits()
    alloc.check_principle_protection()
"""

import json
import logging
import os
import sqlite3
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger("capital_allocator")

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

ALLOC_DB = str(Path(__file__).parent / "allocator.db")
WALLET_ADDRESS = os.environ.get("WALLET_ADDRESS", "")
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")
FLY_URL = "https://nettrace-dashboard.fly.dev"

# Allocation percentages for profit distribution
PROFIT_ALLOCATION = {
    "checking":   0.20,  # 20% back to active trading
    "savings":    0.25,  # 25% locked savings
    "growth":     0.35,  # 35% reinvestment pool
    "subsavings": 0.20,  # 20% long-term hold
}

# Principle protection threshold — lock principle when gains exceed this multiplier
PRINCIPLE_LOCK_THRESHOLD = 1.0  # 100% gains = lock principle
PULL_TO_USD_INTERVAL = 6 * 3600  # 4x daily = every 6 hours


def _fetch_json(url, payload=None, headers=None, timeout=10):
    _headers = {"User-Agent": "NetTrace/1.0"}
    if headers:
        _headers.update(headers)
    if payload:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=data,
                                     headers={**_headers, "Content-Type": "application/json"})
    else:
        req = urllib.request.Request(url, headers=_headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


class CapitalAllocator:
    """Manages capital across sub-accounts with principle protection."""

    def __init__(self):
        self.db = sqlite3.connect(ALLOC_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self._last_pull_time = 0
        self._principle_locked = False

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS treasury (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL,
                chain TEXT DEFAULT 'all',
                balance_usd REAL DEFAULT 0,
                principle_usd REAL DEFAULT 0,
                gains_usd REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS allocations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_account TEXT NOT NULL,
                to_account TEXT NOT NULL,
                amount_usd REAL NOT NULL,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS principle_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                principle_usd REAL,
                gains_usd REAL,
                total_usd REAL,
                action_taken TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS pull_to_usd_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amount_usd REAL,
                source TEXT,
                tx_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Initialize accounts if empty
        existing = self.db.execute("SELECT COUNT(*) as cnt FROM treasury").fetchone()["cnt"]
        if existing == 0:
            # Initial allocation based on ~$63 wallet
            accounts = {
                "checking": {"balance": 25.0, "principle": 25.0},
                "savings":  {"balance": 20.0, "principle": 20.0},
                "growth":   {"balance": 12.0, "principle": 12.0},
                "subsavings": {"balance": 6.62, "principle": 6.62},
            }
            for name, data in accounts.items():
                self.db.execute(
                    "INSERT INTO treasury (account, balance_usd, principle_usd, gains_usd) "
                    "VALUES (?, ?, ?, 0)",
                    (name, data["balance"], data["principle"])
                )
            self.db.commit()
            logger.info("Initialized treasury accounts")

    def get_accounts(self):
        """Get all account balances."""
        rows = self.db.execute(
            "SELECT * FROM treasury ORDER BY account"
        ).fetchall()
        accounts = {}
        total = 0
        total_principle = 0
        total_gains = 0
        for row in rows:
            accounts[row["account"]] = {
                "balance_usd": row["balance_usd"],
                "principle_usd": row["principle_usd"],
                "gains_usd": row["gains_usd"],
                "updated_at": row["updated_at"],
            }
            total += row["balance_usd"]
            total_principle += row["principle_usd"]
            total_gains += row["gains_usd"]

        return {
            "accounts": accounts,
            "total_usd": round(total, 2),
            "total_principle": round(total_principle, 2),
            "total_gains": round(total_gains, 2),
            "gain_pct": round((total_gains / total_principle * 100), 2) if total_principle > 0 else 0,
            "principle_locked": self._principle_locked,
        }

    def sync_balances(self):
        """Sync treasury with actual wallet + CEX balances."""
        total_actual = 0

        # Get wallet balances across all chains
        if WALLET_ADDRESS:
            try:
                from wallet_connector import MultiChainWallet
                wallet = MultiChainWallet(WALLET_ADDRESS, ["ethereum", "base", "arbitrum", "polygon"])
                balances = wallet.get_all_balances()
                total_actual += balances["total_usd"]
                logger.info("Wallet total: $%.2f across %d chains",
                            balances["total_usd"], len(balances["chains"]))
            except Exception as e:
                logger.warning("Wallet sync failed: %s", e)

        # Get Coinbase balances
        try:
            from agent_tools import AgentTools
            tools = AgentTools()
            portfolio = tools.get_portfolio()
            total_actual += portfolio["total_usd"]
            logger.info("Coinbase total: $%.2f", portfolio["total_usd"])
        except Exception as e:
            logger.warning("Coinbase sync failed: %s", e)

        if total_actual <= 0:
            logger.warning("No balances found to sync")
            return

        # Get current treasury total
        current = self.get_accounts()
        current_total = current["total_usd"]

        if current_total <= 0:
            return

        # Calculate the change
        delta = total_actual - current_total

        if abs(delta) < 0.01:
            logger.debug("Treasury in sync (delta $%.4f)", delta)
            return

        if delta > 0:
            # We made money — allocate gains
            self._distribute_gains(delta)
        else:
            # We lost money — debit from checking first
            self._absorb_loss(abs(delta))

        logger.info("Treasury synced: was $%.2f, now $%.2f (delta $%+.2f)",
                     current_total, total_actual, delta)

    def _distribute_gains(self, amount):
        """Distribute gains according to allocation percentages."""
        for account, pct in PROFIT_ALLOCATION.items():
            share = amount * pct
            self.db.execute(
                "UPDATE treasury SET balance_usd = balance_usd + ?, "
                "gains_usd = gains_usd + ?, updated_at = CURRENT_TIMESTAMP "
                "WHERE account = ?",
                (share, share, account)
            )
            self.db.execute(
                "INSERT INTO allocations (from_account, to_account, amount_usd, reason) "
                "VALUES ('gains', ?, ?, 'profit_distribution')",
                (account, share)
            )
        self.db.commit()
        logger.info("Distributed $%.2f gains: checking=$%.2f savings=$%.2f "
                     "growth=$%.2f subsavings=$%.2f",
                     amount,
                     amount * PROFIT_ALLOCATION["checking"],
                     amount * PROFIT_ALLOCATION["savings"],
                     amount * PROFIT_ALLOCATION["growth"],
                     amount * PROFIT_ALLOCATION["subsavings"])

    def _absorb_loss(self, amount):
        """Absorb losses from checking first, then growth."""
        remaining = amount

        # First: debit checking
        checking = self.db.execute(
            "SELECT balance_usd FROM treasury WHERE account = 'checking'"
        ).fetchone()
        if checking:
            debit = min(remaining, checking["balance_usd"])
            if debit > 0:
                self.db.execute(
                    "UPDATE treasury SET balance_usd = balance_usd - ?, "
                    "gains_usd = gains_usd - ?, updated_at = CURRENT_TIMESTAMP "
                    "WHERE account = 'checking'",
                    (debit, debit)
                )
                remaining -= debit

        # Second: debit growth
        if remaining > 0:
            growth = self.db.execute(
                "SELECT balance_usd FROM treasury WHERE account = 'growth'"
            ).fetchone()
            if growth:
                debit = min(remaining, growth["balance_usd"])
                if debit > 0:
                    self.db.execute(
                        "UPDATE treasury SET balance_usd = balance_usd - ?, "
                        "gains_usd = gains_usd - ?, updated_at = CURRENT_TIMESTAMP "
                        "WHERE account = 'growth'",
                        (debit, debit)
                    )
                    remaining -= debit

        # NEVER touch savings or subsavings for losses
        if remaining > 0.01:
            logger.critical("LOSS exceeds checking+growth by $%.2f! "
                            "Savings/subsavings protected.", remaining)
            self.db.execute(
                "INSERT INTO principle_events "
                "(event_type, principle_usd, gains_usd, total_usd, action_taken) "
                "VALUES ('loss_overflow', ?, ?, ?, 'savings_protected')",
                (remaining, -amount, 0)
            )

        self.db.commit()
        logger.info("Absorbed $%.2f loss from checking/growth", amount - remaining)

    def check_principle_protection(self):
        """Check if gains have exceeded principle — if so, lock principle in savings.

        Rule: Once gains >= 100% of principle, move original principle to savings.
        This means we're now "playing with house money."
        """
        current = self.get_accounts()
        total_principle = current["total_principle"]
        total_gains = current["total_gains"]

        if total_principle <= 0:
            return

        gain_ratio = total_gains / total_principle

        if gain_ratio >= PRINCIPLE_LOCK_THRESHOLD and not self._principle_locked:
            # LOCK PRINCIPLE — move original investment to savings
            logger.critical("PRINCIPLE LOCK: Gains ($%.2f) exceeded %.0f%% of "
                            "principle ($%.2f). Locking principle in savings!",
                            total_gains, PRINCIPLE_LOCK_THRESHOLD * 100, total_principle)

            # Move principle amount from checking → savings
            checking = current["accounts"].get("checking", {})
            checking_bal = checking.get("balance_usd", 0)
            principle_to_lock = min(checking_bal, total_principle * 0.5)

            if principle_to_lock > 0:
                self.db.execute(
                    "UPDATE treasury SET balance_usd = balance_usd - ? "
                    "WHERE account = 'checking'",
                    (principle_to_lock,)
                )
                self.db.execute(
                    "UPDATE treasury SET balance_usd = balance_usd + ?, "
                    "principle_usd = principle_usd + ? "
                    "WHERE account = 'savings'",
                    (principle_to_lock, principle_to_lock)
                )
                self.db.execute(
                    "INSERT INTO allocations "
                    "(from_account, to_account, amount_usd, reason) "
                    "VALUES ('checking', 'savings', ?, 'principle_lock')",
                    (principle_to_lock,)
                )

            self.db.execute(
                "INSERT INTO principle_events "
                "(event_type, principle_usd, gains_usd, total_usd, action_taken) "
                "VALUES ('principle_locked', ?, ?, ?, 'moved_to_savings')",
                (total_principle, total_gains, current["total_usd"])
            )
            self.db.commit()

            self._principle_locked = True
            logger.info("Locked $%.2f principle in savings. Now playing with house money.",
                        principle_to_lock)
            return True

        return False

    def get_tradeable_capital(self):
        """Get the amount available for active trading (checking + growth)."""
        checking = self.db.execute(
            "SELECT balance_usd FROM treasury WHERE account = 'checking'"
        ).fetchone()
        growth = self.db.execute(
            "SELECT balance_usd FROM treasury WHERE account = 'growth'"
        ).fetchone()

        checking_bal = checking["balance_usd"] if checking else 0
        growth_bal = growth["balance_usd"] if growth else 0

        # Only use 80% of checking for trades, keep 20% buffer
        return round(checking_bal * 0.80 + growth_bal * 0.50, 2)

    def should_pull_to_usd(self):
        """Check if it's time for a pull-to-USD event (4x daily)."""
        return time.time() - self._last_pull_time >= PULL_TO_USD_INTERVAL

    def pull_to_usd(self):
        """Convert volatile gains to USDC/USD. Called 4x daily."""
        if not self.should_pull_to_usd():
            return None

        self._last_pull_time = time.time()

        # Calculate unrealized gains in volatile assets
        current = self.get_accounts()
        total_gains = current["total_gains"]

        if total_gains <= 1.0:
            logger.debug("No significant gains to pull ($%.2f)", total_gains)
            return None

        # Pull 50% of gains to USD (keep some exposure)
        pull_amount = total_gains * 0.50

        logger.info("PULL-TO-USD: Converting $%.2f of gains to USD (4x daily cycle)",
                     pull_amount)

        self.db.execute(
            "INSERT INTO pull_to_usd_log (amount_usd, source) VALUES (?, 'auto_4x_daily')",
            (pull_amount,)
        )
        self.db.commit()

        return {"amount_usd": pull_amount, "source": "auto_4x_daily"}

    def push_to_dashboard(self):
        """Push treasury status to Fly dashboard."""
        if self.get_tradeable_capital() <= 0:
            logger.debug("Skipping push — $0 tradeable capital (API timeout?)")
            return None

        current = self.get_accounts()
        if current["total_usd"] <= 0:
            logger.debug("Skipping push — $0 total (API timeout?)")
            return None

        payload = {
            "treasury": current,
            "tradeable_capital": self.get_tradeable_capital(),
            "principle_locked": self._principle_locked,
            "source": "capital_allocator",
            "user_id": 2,
        }

        try:
            data = json.dumps(payload).encode()
            req = urllib.request.Request(
                f"{FLY_URL}/api/trading-snapshot",
                data=data,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {NETTRACE_API_KEY}",
                },
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            logger.debug("Dashboard push failed: %s", e)
            return None

    def run_cycle(self):
        """Run a single allocation cycle."""
        self.sync_balances()
        self.check_principle_protection()
        if self.should_pull_to_usd():
            self.pull_to_usd()
        self.push_to_dashboard()

    def run_loop(self, interval=300):
        """Run allocation loop (every 5 minutes)."""
        logger.info("Capital Allocator starting (interval=%ds)", interval)
        while True:
            try:
                self.run_cycle()
            except Exception as e:
                logger.error("Allocation cycle failed: %s", e, exc_info=True)
            time.sleep(interval)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [ALLOC] %(levelname)s %(message)s")

    alloc = CapitalAllocator()

    if len(sys.argv) > 1 and sys.argv[1] == "status":
        current = alloc.get_accounts()
        print(f"\n{'='*60}")
        print(f"  TREASURY STATUS")
        print(f"{'='*60}")
        for name, data in sorted(current["accounts"].items()):
            print(f"  {name:<12} ${data['balance_usd']:>8.2f}  "
                  f"(principle: ${data['principle_usd']:.2f}, "
                  f"gains: ${data['gains_usd']:+.2f})")
        print(f"{'─'*60}")
        print(f"  Total:       ${current['total_usd']:>8.2f}")
        print(f"  Principle:   ${current['total_principle']:>8.2f}")
        print(f"  Gains:       ${current['total_gains']:>+8.2f} ({current['gain_pct']:+.1f}%)")
        print(f"  Locked:      {'YES' if current['principle_locked'] else 'NO'}")
        print(f"  Tradeable:   ${alloc.get_tradeable_capital():>8.2f}")
        print(f"{'='*60}\n")

    elif len(sys.argv) > 1 and sys.argv[1] == "sync":
        alloc.sync_balances()
        current = alloc.get_accounts()
        print(f"Synced. Total: ${current['total_usd']:.2f}")

    elif len(sys.argv) > 1 and sys.argv[1] == "run":
        alloc.run_loop()

    else:
        print("Usage: python capital_allocator.py [status|sync|run]")
