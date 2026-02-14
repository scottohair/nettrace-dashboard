"""Unified Capital Ledger Writer.

Every dollar movement — trades, deposits, withdrawals, fees, allocations,
appreciation — gets a single row in the ``capital_ledger`` table
(traceroute.db).  Agents call the thin helpers here instead of writing
ledger SQL themselves.
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("ledger_writer")

# The ledger lives in the main dashboard DB so the Flask app can query it.
_DEFAULT_DB = os.environ.get(
    "DB_PATH",
    str(Path(__file__).resolve().parent.parent / "traceroute.db"),
)


class LedgerWriter:
    """Append-only writer for the capital_ledger table."""

    def __init__(self, db_path: str | None = None):
        self._db_path = db_path or _DEFAULT_DB
        self._ensure_table()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self):
        conn = sqlite3.connect(self._db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_table(self):
        """Idempotent CREATE TABLE (in case app.py hasn't run yet)."""
        conn = self._connect()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS capital_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    user_id INTEGER,
                    asset TEXT,
                    venue TEXT,
                    amount REAL,
                    value_usd REAL,
                    pair TEXT,
                    side TEXT,
                    price REAL,
                    order_id TEXT,
                    agent TEXT,
                    strategy_name TEXT,
                    cost_basis_usd REAL,
                    realized_pnl_usd REAL,
                    fees_usd REAL,
                    trigger TEXT,
                    trade_id INTEGER,
                    allocation_id INTEGER,
                    snapshot_id INTEGER,
                    metadata_json TEXT,
                    reconciled INTEGER DEFAULT 0,
                    reconciliation_delta_usd REAL
                );
                CREATE INDEX IF NOT EXISTS idx_capital_ledger_timestamp ON capital_ledger(timestamp);
                CREATE INDEX IF NOT EXISTS idx_capital_ledger_event_type ON capital_ledger(event_type);
                CREATE INDEX IF NOT EXISTS idx_capital_ledger_asset ON capital_ledger(asset);
                CREATE INDEX IF NOT EXISTS idx_capital_ledger_user_id ON capital_ledger(user_id);
                CREATE INDEX IF NOT EXISTS idx_capital_ledger_pair ON capital_ledger(pair);
                CREATE INDEX IF NOT EXISTS idx_capital_ledger_agent ON capital_ledger(agent);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_capital_ledger_dedup
                    ON capital_ledger(event_type, trade_id, timestamp)
                    WHERE trade_id IS NOT NULL;

                CREATE TABLE IF NOT EXISTS cost_basis_lots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    asset TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    acquired_at TIMESTAMP NOT NULL,
                    quantity REAL NOT NULL,
                    cost_basis_per_unit REAL NOT NULL,
                    total_cost_usd REAL NOT NULL,
                    quantity_remaining REAL NOT NULL,
                    disposed_at TIMESTAMP,
                    source_trade_id INTEGER,
                    source_event_type TEXT,
                    lot_method TEXT DEFAULT 'FIFO'
                );
                CREATE INDEX IF NOT EXISTS idx_cost_basis_lots_asset ON cost_basis_lots(asset, venue);
                CREATE INDEX IF NOT EXISTS idx_cost_basis_lots_acquired ON cost_basis_lots(acquired_at);
            """)
        finally:
            conn.close()

    def _insert(self, **kw):
        """Insert a single ledger row.  Returns the new row id."""
        kw.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
        kw.setdefault("user_id", 2)  # Scott

        # Serialize metadata dict → JSON
        meta = kw.get("metadata_json")
        if isinstance(meta, dict):
            kw["metadata_json"] = json.dumps(meta)

        cols = list(kw.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)

        conn = self._connect()
        try:
            cur = conn.execute(
                f"INSERT INTO capital_ledger ({col_names}) VALUES ({placeholders})",
                tuple(kw[c] for c in cols),
            )
            conn.commit()
            row_id = cur.lastrowid
            logger.info(
                "LEDGER %s | %s %s $%.4f (id=%d)",
                kw.get("event_type"),
                kw.get("side") or kw.get("trigger") or "",
                kw.get("asset") or kw.get("pair") or "",
                float(kw.get("value_usd") or 0),
                row_id,
            )
            return row_id
        except sqlite3.IntegrityError:
            # Duplicate (dedup index) — harmless
            logger.debug("LEDGER duplicate skipped: %s trade_id=%s", kw.get("event_type"), kw.get("trade_id"))
            return None
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_trade_fill(
        self,
        *,
        asset: str,
        venue: str,
        amount: float,
        value_usd: float,
        pair: str,
        side: str,
        price: float,
        order_id: str | None = None,
        agent: str | None = None,
        strategy_name: str | None = None,
        trigger: str | None = None,
        trade_id: int | None = None,
        fees_usd: float | None = None,
        realized_pnl_usd: float | None = None,
        cost_basis_usd: float | None = None,
        metadata: dict | None = None,
        timestamp: str | None = None,
        user_id: int = 2,
    ) -> int | None:
        """Record a trade fill (BUY or SELL)."""
        kw = dict(
            event_type="trade_fill",
            asset=asset,
            venue=venue,
            amount=amount,
            value_usd=value_usd,
            pair=pair,
            side=side.upper(),
            price=price,
            order_id=order_id,
            agent=agent,
            strategy_name=strategy_name,
            trigger=trigger,
            trade_id=trade_id,
            fees_usd=fees_usd,
            realized_pnl_usd=realized_pnl_usd,
            cost_basis_usd=cost_basis_usd,
            metadata_json=metadata,
            user_id=user_id,
        )
        if timestamp:
            kw["timestamp"] = timestamp

        row_id = self._insert(**kw)

        # Maintain cost-basis lots
        if side.upper() == "BUY" and row_id:
            self._create_lot(
                asset=asset,
                venue=venue,
                quantity=abs(amount),
                cost_per_unit=price,
                total_cost=abs(value_usd),
                trade_id=trade_id,
                timestamp=timestamp,
                user_id=user_id,
            )
        elif side.upper() == "SELL" and row_id:
            cost_basis = self.match_cost_basis_fifo(asset, venue, abs(amount))
            if cost_basis is not None and realized_pnl_usd is None:
                # Update ledger row with calculated cost basis
                conn = self._connect()
                try:
                    conn.execute(
                        "UPDATE capital_ledger SET cost_basis_usd=?, realized_pnl_usd=? WHERE id=?",
                        (cost_basis, abs(value_usd) - cost_basis, row_id),
                    )
                    conn.commit()
                finally:
                    conn.close()

        return row_id

    def record_deposit(
        self,
        asset: str,
        venue: str,
        amount: float,
        value_usd: float,
        source: str = "manual",
        timestamp: str | None = None,
        user_id: int = 2,
    ) -> int | None:
        """Record external deposit."""
        kw = dict(
            event_type="deposit",
            asset=asset,
            venue=venue,
            amount=abs(amount),
            value_usd=abs(value_usd),
            trigger=source,
            metadata_json={"source": source},
            user_id=user_id,
        )
        if timestamp:
            kw["timestamp"] = timestamp
        return self._insert(**kw)

    def record_withdrawal(
        self,
        asset: str,
        venue: str,
        amount: float,
        value_usd: float,
        destination: str = "manual",
        timestamp: str | None = None,
        user_id: int = 2,
    ) -> int | None:
        """Record external withdrawal."""
        kw = dict(
            event_type="withdrawal",
            asset=asset,
            venue=venue,
            amount=-abs(amount),
            value_usd=-abs(value_usd),
            trigger=destination,
            metadata_json={"destination": destination},
            user_id=user_id,
        )
        if timestamp:
            kw["timestamp"] = timestamp
        return self._insert(**kw)

    def record_fee(
        self,
        asset: str,
        venue: str,
        fee_usd: float,
        fee_type: str = "trading",
        agent: str | None = None,
        trade_id: int | None = None,
        timestamp: str | None = None,
        user_id: int = 2,
    ) -> int | None:
        """Record a fee (trading, gas, performance, etc.)."""
        return self._insert(
            event_type="fee",
            asset=asset,
            venue=venue,
            amount=0,
            value_usd=-abs(fee_usd),
            fees_usd=abs(fee_usd),
            trigger=fee_type,
            agent=agent,
            trade_id=trade_id,
            user_id=user_id,
            **({"timestamp": timestamp} if timestamp else {}),
        )

    def record_allocation(
        self,
        from_account: str,
        to_account: str,
        amount_usd: float,
        reason: str,
        allocation_id: int | None = None,
        timestamp: str | None = None,
        user_id: int = 2,
    ) -> int | None:
        """Record internal capital movement (checking → savings, etc.)."""
        return self._insert(
            event_type="allocation",
            asset="USD",
            venue="internal",
            amount=amount_usd,
            value_usd=amount_usd,
            trigger=f"{from_account}->{to_account}:{reason}",
            allocation_id=allocation_id,
            metadata_json={"from": from_account, "to": to_account, "reason": reason},
            user_id=user_id,
            **({"timestamp": timestamp} if timestamp else {}),
        )

    def record_realized_pnl(
        self,
        trade_id: int,
        realized_pnl_usd: float,
        cost_basis_usd: float | None = None,
        fees_usd: float | None = None,
        pair: str | None = None,
        agent: str | None = None,
        timestamp: str | None = None,
        user_id: int = 2,
    ) -> int | None:
        """Record realized P&L for a completed SELL (or exit)."""
        return self._insert(
            event_type="realized_pnl",
            asset=pair.split("-")[0] if pair else None,
            venue="coinbase",
            value_usd=realized_pnl_usd,
            pair=pair,
            agent=agent,
            realized_pnl_usd=realized_pnl_usd,
            cost_basis_usd=cost_basis_usd,
            fees_usd=fees_usd,
            trade_id=trade_id,
            trigger="reconciliation",
            user_id=user_id,
            **({"timestamp": timestamp} if timestamp else {}),
        )

    def record_appreciation(
        self,
        value_usd: float,
        snapshot_id: int | None = None,
        metadata: dict | None = None,
        timestamp: str | None = None,
        user_id: int = 2,
    ) -> int | None:
        """Record unexplained portfolio appreciation (mark-to-market delta)."""
        return self._insert(
            event_type="appreciation",
            value_usd=value_usd,
            snapshot_id=snapshot_id,
            trigger="reconciliation_gap",
            reconciliation_delta_usd=value_usd,
            metadata_json=metadata,
            user_id=user_id,
            **({"timestamp": timestamp} if timestamp else {}),
        )

    # ------------------------------------------------------------------
    # Cost-basis lot management (FIFO)
    # ------------------------------------------------------------------

    def _create_lot(
        self,
        asset: str,
        venue: str,
        quantity: float,
        cost_per_unit: float,
        total_cost: float,
        trade_id: int | None = None,
        timestamp: str | None = None,
        user_id: int = 2,
    ):
        """Create a new cost-basis lot for a BUY."""
        ts = timestamp or datetime.now(timezone.utc).isoformat()
        conn = self._connect()
        try:
            conn.execute(
                """INSERT INTO cost_basis_lots
                   (user_id, asset, venue, acquired_at, quantity,
                    cost_basis_per_unit, total_cost_usd, quantity_remaining,
                    source_trade_id, source_event_type)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'trade_fill')""",
                (user_id, asset, venue, ts, quantity,
                 cost_per_unit, total_cost, quantity, trade_id),
            )
            conn.commit()
        finally:
            conn.close()

    def match_cost_basis_fifo(
        self, asset: str, venue: str, quantity_sold: float
    ) -> float | None:
        """Match a SELL against oldest BUY lots (FIFO).  Returns total cost basis."""
        conn = self._connect()
        try:
            lots = conn.execute(
                """SELECT id, quantity_remaining, cost_basis_per_unit
                   FROM cost_basis_lots
                   WHERE asset=? AND venue=? AND quantity_remaining > 0
                   ORDER BY acquired_at ASC""",
                (asset, venue),
            ).fetchall()

            if not lots:
                return None

            total_cost = 0.0
            qty_remaining = quantity_sold

            for lot in lots:
                if qty_remaining <= 0:
                    break
                take_qty = min(lot["quantity_remaining"], qty_remaining)
                total_cost += take_qty * lot["cost_basis_per_unit"]
                new_remaining = lot["quantity_remaining"] - take_qty

                conn.execute(
                    """UPDATE cost_basis_lots
                       SET quantity_remaining=?,
                           disposed_at=CASE WHEN ?=0 THEN CURRENT_TIMESTAMP ELSE disposed_at END
                       WHERE id=?""",
                    (new_remaining, new_remaining, lot["id"]),
                )
                qty_remaining -= take_qty

            conn.commit()
            return total_cost
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Query helpers (used by reconciler and API)
    # ------------------------------------------------------------------

    def get_ledger_total(self, user_id: int = 2, before: str | None = None) -> float:
        """Sum of all value_usd up to a given timestamp."""
        conn = self._connect()
        try:
            if before:
                row = conn.execute(
                    "SELECT COALESCE(SUM(value_usd), 0) AS total FROM capital_ledger WHERE user_id=? AND timestamp<=?",
                    (user_id, before),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COALESCE(SUM(value_usd), 0) AS total FROM capital_ledger WHERE user_id=?",
                    (user_id,),
                ).fetchone()
            return float(row["total"])
        finally:
            conn.close()

    def get_events(
        self,
        user_id: int = 2,
        event_type: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 500,
    ) -> list[dict]:
        """Fetch ledger events with optional filters."""
        conn = self._connect()
        try:
            where = ["user_id=?"]
            params: list = [user_id]
            if event_type:
                where.append("event_type=?")
                params.append(event_type)
            if start:
                where.append("timestamp>=?")
                params.append(start)
            if end:
                where.append("timestamp<=?")
                params.append(end)
            params.append(limit)

            rows = conn.execute(
                f"SELECT * FROM capital_ledger WHERE {' AND '.join(where)} ORDER BY timestamp DESC LIMIT ?",
                params,
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
