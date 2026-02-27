#!/usr/bin/env python3
"""Shared Position Registry — single source of truth for position ownership.

Solves agent coordination: prevents exit_manager from selling momentum_chaser's
positions, prevents double-buying, protects operator directives.

All agents call this before BUY (check ownership) and before SELL (claim for exit).
SQLite in trader.db with WAL mode for concurrent readers.

Usage:
    from position_registry import get_registry

    reg = get_registry()

    # Before buying:
    if reg.is_pair_available("BTC-USD"):
        # ... place order ...
        reg.register("BTC-USD", "sniper", entry_price=95000,
                     entry_amount=0.001, exit_owner="exit_manager")

    # Before selling (exit_manager):
    pos = reg.claim_for_exit("BTC-USD", "exit_manager")
    if pos:
        # ... execute sell ...
        reg.close("BTC-USD", close_price=96000, pnl_usd=1.50)
"""

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Tuple

logger = logging.getLogger("position_registry")

# DB path: same trader.db used by exchange_connector and sniper
_AGENTS_DIR = Path(__file__).parent
_PERSISTENT_DIR = Path("/data") if Path("/data").is_dir() else _AGENTS_DIR
REGISTRY_DB = str(_PERSISTENT_DIR / "trader.db")

# Min hold defaults per owner (seconds)
DEFAULT_MIN_HOLD = {
    "operator": 999_999_999,  # effectively infinite — only operator can close
    "sniper": 300,            # 5 minutes
    "momentum_chaser": 120,   # 2 minutes
    "continuous_trader": 60,  # 1 minute
}

# Exit claim timeout: if an agent claims a position for exit but doesn't
# close it within this window, the claim expires (crash recovery)
EXIT_CLAIM_TIMEOUT_S = int(os.environ.get("REGISTRY_CLAIM_TIMEOUT_S", "300"))


class PositionRegistry:
    """Thread-safe SQLite registry for position ownership.

    Singleton — all agents in the same process share one instance.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._db_path = REGISTRY_DB
        self._local = threading.local()
        self._init_schema()

    @classmethod
    def reset(cls):
        """Reset singleton (for testing only)."""
        with cls._lock:
            cls._instance = None

    def _conn(self) -> sqlite3.Connection:
        """Thread-local SQLite connection with WAL mode."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self._db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            self._local.conn = conn
        return self._local.conn

    def _init_schema(self):
        """Create table if not exists."""
        conn = self._conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS position_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                venue TEXT NOT NULL DEFAULT 'coinbase',
                owner TEXT NOT NULL,
                exit_owner TEXT NOT NULL DEFAULT 'self',
                entry_price REAL NOT NULL,
                entry_amount REAL NOT NULL,
                entry_usd REAL,
                entry_time TEXT NOT NULL,
                order_id TEXT,
                min_hold_seconds INTEGER DEFAULT 0,
                reason TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                exit_agent TEXT,
                exit_claimed_at TEXT,
                closed_at TEXT,
                close_price REAL,
                pnl_usd REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_pr_pair_status
                ON position_registry(pair, status);
            CREATE INDEX IF NOT EXISTS idx_pr_owner_status
                ON position_registry(owner, status);
            CREATE INDEX IF NOT EXISTS idx_pr_status
                ON position_registry(status);
        """)
        conn.commit()

    # ── Core API ──

    def register(self, pair: str, owner: str, *,
                 entry_price: float, entry_amount: float,
                 venue: str = "coinbase",
                 entry_usd: float = None,
                 order_id: str = None,
                 exit_owner: str = None,
                 min_hold_seconds: int = None,
                 reason: str = None) -> Optional[int]:
        """Register a new position. Returns row ID or None if pair already owned.

        Call AFTER a BUY order fills (not before).
        """
        pair = _normalize(pair)
        if exit_owner is None:
            exit_owner = "exit_manager" if owner == "sniper" else "self"
        if min_hold_seconds is None:
            min_hold_seconds = DEFAULT_MIN_HOLD.get(owner, 60)
        if entry_usd is None:
            entry_usd = entry_price * entry_amount

        now = datetime.now(timezone.utc).isoformat()
        try:
            conn = self._conn()
            # Check both USD and USDC variants
            variants = _pair_variants(pair)
            placeholders = ",".join("?" for _ in variants)
            existing = conn.execute(
                f"SELECT id, owner FROM position_registry "
                f"WHERE pair IN ({placeholders}) AND venue = ? AND status IN ('open','exiting')",
                (*variants, venue),
            ).fetchone()

            if existing:
                logger.info("REGISTRY: %s already owned by %s — skipping %s",
                            pair, existing["owner"], owner)
                return None

            cursor = conn.execute(
                """INSERT INTO position_registry
                   (pair, venue, owner, exit_owner, entry_price, entry_amount,
                    entry_usd, entry_time, order_id, min_hold_seconds, reason, status)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (pair, venue, owner, exit_owner, entry_price, entry_amount,
                 entry_usd, now, order_id, min_hold_seconds, reason, "open"),
            )
            conn.commit()
            row_id = cursor.lastrowid
            logger.info("REGISTRY: +%s owner=%s exit_owner=%s hold=%ds id=%d",
                        pair, owner, exit_owner, min_hold_seconds, row_id)
            return row_id

        except Exception as e:
            logger.warning("REGISTRY: register(%s, %s) failed: %s", pair, owner, e)
            return None

    def update_amount(self, pair: str, new_amount: float,
                      new_avg_price: float = None, venue: str = "coinbase"):
        """Update position amount (DCA / averaging)."""
        pair = _normalize(pair)
        try:
            conn = self._conn()
            if new_avg_price is not None:
                conn.execute(
                    "UPDATE position_registry SET entry_amount=?, entry_price=?, "
                    "entry_usd=?*?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE pair=? AND venue=? AND status='open'",
                    (new_amount, new_avg_price, new_amount, new_avg_price, pair, venue),
                )
            else:
                conn.execute(
                    "UPDATE position_registry SET entry_amount=?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE pair=? AND venue=? AND status='open'",
                    (new_amount, pair, venue),
                )
            conn.commit()
        except Exception as e:
            logger.warning("REGISTRY: update_amount(%s) failed: %s", pair, e)

    def is_pair_available(self, pair: str, venue: str = "coinbase") -> bool:
        """Check if pair is free to open a new position."""
        return self._get_open(pair, venue) is None

    def is_owned_by(self, pair: str, owner: str, venue: str = "coinbase") -> bool:
        """Check if a specific agent owns this pair."""
        existing = self._get_open(pair, venue)
        return existing is not None and existing["owner"] == owner

    def get_owner(self, pair: str, venue: str = "coinbase") -> Optional[str]:
        """Get current owner of a pair, or None."""
        existing = self._get_open(pair, venue)
        return existing["owner"] if existing else None

    def can_exit(self, pair: str, agent: str, venue: str = "coinbase") -> bool:
        """Quick check: can this agent exit this pair? (No state change.)"""
        pos = self._get_open(pair, venue)
        if not pos:
            return True  # No registered position — allow legacy behavior
        return self._check_exit_permission(pos, agent)

    def claim_for_exit(self, pair: str, agent: str,
                       venue: str = "coinbase") -> Optional[Dict]:
        """Attempt to claim a position for exit. Returns position dict or None.

        Rules:
        1. Position must be 'open' (not already 'exiting')
        2. Agent must have exit permission (exit_owner check)
        3. min_hold_seconds must have elapsed
        4. Stale claims auto-released (crash recovery)
        """
        pair = _normalize(pair)
        self._expire_stale_claims()

        try:
            conn = self._conn()
            now = datetime.now(timezone.utc).isoformat()

            # Check both USD/USDC variants
            variants = _pair_variants(pair)
            placeholders = ",".join("?" for _ in variants)
            row = conn.execute(
                f"SELECT * FROM position_registry "
                f"WHERE pair IN ({placeholders}) AND venue=? AND status='open'",
                (*variants, venue),
            ).fetchone()

            if not row:
                return None

            if not self._check_exit_permission(dict(row), agent):
                return None

            # Check min hold time (owner can always exit their own positions)
            try:
                entry_dt = datetime.fromisoformat(
                    row["entry_time"].replace("Z", "+00:00")
                )
                held_s = time.time() - entry_dt.timestamp()
                is_owner = agent == row["owner"]
                if not is_owner and held_s < row["min_hold_seconds"]:
                    logger.debug("REGISTRY: %s min hold not met (%.0f < %d s)",
                                 pair, held_s, row["min_hold_seconds"])
                    return None
            except Exception:
                held_s = 0

            # Claim it
            conn.execute(
                "UPDATE position_registry SET status='exiting', exit_agent=?, "
                "exit_claimed_at=?, updated_at=CURRENT_TIMESTAMP "
                "WHERE id=? AND status='open'",
                (agent, now, row["id"]),
            )
            conn.commit()
            logger.info("REGISTRY: %s claimed %s for exit (owner=%s, held=%.0fs)",
                        agent, pair, row["owner"], held_s)
            return dict(row)

        except Exception as e:
            logger.warning("REGISTRY: claim_for_exit(%s, %s) failed: %s", pair, agent, e)
            return None

    def close(self, pair: str, *, close_price: float = None,
              pnl_usd: float = None, venue: str = "coinbase"):
        """Mark a position as closed after exit is complete."""
        pair = _normalize(pair)
        now = datetime.now(timezone.utc).isoformat()
        try:
            conn = self._conn()
            variants = _pair_variants(pair)
            placeholders = ",".join("?" for _ in variants)
            conn.execute(
                f"UPDATE position_registry SET status='closed', closed_at=?, "
                f"close_price=?, pnl_usd=?, updated_at=CURRENT_TIMESTAMP "
                f"WHERE pair IN ({placeholders}) AND venue=? AND status IN ('open','exiting')",
                (now, close_price, pnl_usd, *variants, venue),
            )
            conn.commit()
            logger.info("REGISTRY: closed %s pnl=$%.2f", pair, pnl_usd or 0)
        except Exception as e:
            logger.warning("REGISTRY: close(%s) failed: %s", pair, e)

    def release(self, pair: str, venue: str = "coinbase"):
        """Release an exit claim (revert 'exiting' → 'open')."""
        pair = _normalize(pair)
        try:
            conn = self._conn()
            conn.execute(
                "UPDATE position_registry SET status='open', exit_agent=NULL, "
                "exit_claimed_at=NULL, updated_at=CURRENT_TIMESTAMP "
                "WHERE pair=? AND venue=? AND status='exiting'",
                (pair, venue),
            )
            conn.commit()
        except Exception as e:
            logger.warning("REGISTRY: release(%s) failed: %s", pair, e)

    def query(self, owner: str = None, status: str = "open",
              venue: str = None) -> List[Dict]:
        """Query positions matching filters."""
        try:
            conn = self._conn()
            sql = "SELECT * FROM position_registry WHERE 1=1"
            params: list = []
            if owner:
                sql += " AND owner=?"
                params.append(owner)
            if status:
                sql += " AND status=?"
                params.append(status)
            if venue:
                sql += " AND venue=?"
                params.append(venue)
            sql += " ORDER BY created_at DESC"
            return [dict(r) for r in conn.execute(sql, params).fetchall()]
        except Exception as e:
            logger.warning("REGISTRY: query failed: %s", e)
            return []

    def get_all_open(self) -> Dict[str, Dict]:
        """Get all open positions as {pair: position_dict}."""
        return {p["pair"]: p for p in self.query(status="open")}

    def set_exit_owner(self, pair: str, new_exit_owner: str,
                       venue: str = "coinbase"):
        """Change who can exit a position (operator → exit_manager to release)."""
        pair = _normalize(pair)
        try:
            conn = self._conn()
            min_hold = 0 if new_exit_owner != "operator" else 999_999_999
            conn.execute(
                "UPDATE position_registry SET exit_owner=?, min_hold_seconds=?, "
                "updated_at=CURRENT_TIMESTAMP WHERE pair=? AND venue=? AND status='open'",
                (new_exit_owner, min_hold, pair, venue),
            )
            conn.commit()
            logger.info("REGISTRY: %s exit_owner → %s", pair, new_exit_owner)
        except Exception as e:
            logger.warning("REGISTRY: set_exit_owner(%s) failed: %s", pair, e)

    # ── Internal helpers ──

    def _get_open(self, pair: str, venue: str) -> Optional[Dict]:
        """Get open position for pair+venue (checks USD/USDC variants)."""
        pair = _normalize(pair)
        try:
            conn = self._conn()
            variants = _pair_variants(pair)
            placeholders = ",".join("?" for _ in variants)
            row = conn.execute(
                f"SELECT * FROM position_registry "
                f"WHERE pair IN ({placeholders}) AND venue=? AND status IN ('open','exiting') "
                f"LIMIT 1",
                (*variants, venue),
            ).fetchone()
            return dict(row) if row else None
        except Exception:
            return None

    def _check_exit_permission(self, pos: Dict, agent: str) -> bool:
        """Check if agent has permission to exit this position."""
        exit_owner = pos.get("exit_owner", "self")
        owner = pos.get("owner", "")

        # Owner can ALWAYS exit their own position
        if agent == owner:
            return True

        if exit_owner == "operator":
            logger.debug("REGISTRY: %s is operator-protected, %s blocked",
                         pos.get("pair"), agent)
            return False
        if exit_owner == "self":
            return False
        if exit_owner == "exit_manager" and agent != "exit_manager":
            return False
        if exit_owner not in ("self", "exit_manager", "any", "operator"):
            # Custom exit_owner name — must match agent
            if exit_owner != agent:
                return False
        return True

    def _expire_stale_claims(self):
        """Release exit claims older than EXIT_CLAIM_TIMEOUT_S (crash recovery)."""
        try:
            conn = self._conn()
            cutoff = datetime.fromtimestamp(
                time.time() - EXIT_CLAIM_TIMEOUT_S, tz=timezone.utc
            ).isoformat()
            released = conn.execute(
                "UPDATE position_registry SET status='open', exit_agent=NULL, "
                "exit_claimed_at=NULL, updated_at=CURRENT_TIMESTAMP "
                "WHERE status='exiting' AND exit_claimed_at < ?",
                (cutoff,),
            ).rowcount
            if released > 0:
                conn.commit()
                logger.info("REGISTRY: Released %d stale claims (>%ds)", released, EXIT_CLAIM_TIMEOUT_S)
        except Exception:
            pass


# ── Module-level helpers ──

def _normalize(pair: str) -> str:
    """Normalize pair to uppercase."""
    return str(pair or "").strip().upper()


def _pair_variants(pair: str) -> Tuple[str, ...]:
    """Generate USD/USDC variants for a pair."""
    pair = _normalize(pair)
    variants = {pair}
    if pair.endswith("-USD"):
        variants.add(pair.replace("-USD", "-USDC"))
    elif pair.endswith("-USDC"):
        variants.add(pair.replace("-USDC", "-USD"))
    return tuple(sorted(variants))


def get_registry() -> Optional[PositionRegistry]:
    """Get singleton registry. Returns None on failure (graceful degradation)."""
    try:
        return PositionRegistry()
    except Exception as e:
        logger.warning("REGISTRY: init failed: %s", e)
        return None


# ── Migration ──

def migrate_existing_positions():
    """Seed registry from existing state on first run.

    Sources:
      1. trading_lock.json protected_pairs → operator positions
      2. sniper.db open trades → sniper positions
      3. Coinbase holdings not yet in registry → unknown/exit_manager
    """
    reg = get_registry()
    if not reg:
        return

    # Skip if already populated
    if reg.query(status="open"):
        logger.info("REGISTRY: Already has open positions — skipping migration")
        return

    logger.info("REGISTRY: Running first-time migration...")
    count = 0

    # 1. Protected pairs from trading_lock.json
    lock_file = _AGENTS_DIR / "trading_lock.json"
    if lock_file.exists():
        try:
            lock_data = json.loads(lock_file.read_text())
            for pair in lock_data.get("protected_pairs", []):
                row_id = reg.register(
                    pair, "operator",
                    entry_price=0, entry_amount=0,
                    exit_owner="operator",
                    reason="migrated from trading_lock.json protected_pairs",
                )
                if row_id:
                    count += 1
        except Exception as e:
            logger.warning("REGISTRY: migration from lock file failed: %s", e)

    # 2. Sniper trades from sniper.db
    for db_path in [_PERSISTENT_DIR / "sniper.db", _AGENTS_DIR / "sniper.db"]:
        if not db_path.exists():
            continue
        try:
            sdb = sqlite3.connect(str(db_path), timeout=5)
            sdb.row_factory = sqlite3.Row
            buys = sdb.execute(
                "SELECT pair, entry_price, amount_usd, created_at, order_id "
                "FROM sniper_trades WHERE direction='BUY' AND status='filled' "
                "ORDER BY id DESC LIMIT 50"
            ).fetchall()
            sdb.close()

            seen = set()
            for buy in buys:
                pair = buy["pair"]
                if pair in seen:
                    continue
                seen.add(pair)
                price = buy["entry_price"] or 0
                amount = (buy["amount_usd"] or 0) / price if price > 0 else 0
                row_id = reg.register(
                    pair, "sniper",
                    entry_price=price, entry_amount=amount,
                    order_id=buy["order_id"],
                    exit_owner="exit_manager",
                    reason="migrated from sniper.db",
                )
                if row_id:
                    count += 1
            break
        except Exception as e:
            logger.warning("REGISTRY: migration from sniper.db failed: %s", e)

    logger.info("REGISTRY: Migration complete — %d positions registered", count)


# ── CLI ──

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    reg = get_registry()
    if not reg:
        print("Failed to initialize registry")
        sys.exit(1)

    args = sys.argv[1:]
    if not args or args[0] == "list":
        positions = reg.query(status="open")
        if not positions:
            print("No open positions in registry")
        else:
            print(f"{'Pair':14s} {'Owner':20s} {'Exit Owner':14s} {'Entry $':>10s} {'Amount':>10s} {'Hold':>6s}s {'Reason'}")
            print("-" * 100)
            for p in positions:
                held = ""
                try:
                    entry_dt = datetime.fromisoformat(p["entry_time"].replace("Z", "+00:00"))
                    held = f"{time.time() - entry_dt.timestamp():.0f}"
                except Exception:
                    pass
                print(f"{p['pair']:14s} {p['owner']:20s} {p['exit_owner']:14s} "
                      f"{p['entry_price']:10.4f} {p['entry_amount']:10.6f} {held:>6s}  "
                      f"{p.get('reason', '')}")

    elif args[0] == "protect" and len(args) > 1:
        pair = args[1].upper()
        reason = " ".join(args[2:]) or "operator directive"
        row_id = reg.register(
            pair, "operator",
            entry_price=0, entry_amount=0,
            exit_owner="operator",
            reason=f"operator: {reason}",
        )
        print(f"Protected {pair} (id={row_id})" if row_id else f"{pair} already registered")

    elif args[0] == "release" and len(args) > 1:
        pair = args[1].upper()
        reg.set_exit_owner(pair, "exit_manager")
        print(f"Released {pair} — exit_manager can now exit it")

    elif args[0] == "close" and len(args) > 1:
        pair = args[1].upper()
        reg.close(pair)
        print(f"Closed {pair}")

    elif args[0] == "migrate":
        migrate_existing_positions()

    else:
        print("Usage: position_registry.py [list|protect PAIR|release PAIR|close PAIR|migrate]")
