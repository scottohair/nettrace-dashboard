"""Ledger Reconciler — compares trading_snapshots against capital_ledger.

Detects unexplained deltas (missing deposits, untracked appreciation,
fee gaps) and records them as reconciliation events in the ledger.

Run periodically (every 5 min via flywheel or standalone).
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("ledger_reconciler")

_DEFAULT_DB = os.environ.get(
    "DB_PATH",
    str(Path(__file__).resolve().parent.parent / "traceroute.db"),
)

# Gaps smaller than this are ignored (rounding noise)
RECONCILE_THRESHOLD_USD = 0.50


class LedgerReconciler:
    """Compare snapshots to ledger events and surface gaps."""

    def __init__(self, db_path: str | None = None):
        self._db_path = db_path or _DEFAULT_DB

    def _connect(self):
        conn = sqlite3.connect(self._db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def reconcile_snapshot(self, snapshot_id: int) -> dict:
        """Reconcile a single trading_snapshot against ledger events.

        Returns ``{"snapshot_id", "snapshot_total", "ledger_total", "delta"}``.
        """
        conn = self._connect()
        try:
            snap = conn.execute(
                "SELECT id, user_id, total_value_usd, recorded_at FROM trading_snapshots WHERE id=?",
                (snapshot_id,),
            ).fetchone()
            if not snap:
                return {"error": f"snapshot {snapshot_id} not found"}

            user_id = snap["user_id"] or 2
            snap_total = float(snap["total_value_usd"] or 0)
            snap_ts = snap["recorded_at"]

            # Sum all ledger events up to this snapshot's timestamp.
            # We include trade_fill value_usd (net flow), deposits, withdrawals,
            # fees, and appreciation events.
            ledger_row = conn.execute(
                """SELECT COALESCE(SUM(value_usd), 0) AS ledger_total
                   FROM capital_ledger
                   WHERE user_id=? AND timestamp<=?""",
                (user_id, snap_ts),
            ).fetchone()
            ledger_total = float(ledger_row["ledger_total"])

            delta = snap_total - ledger_total

            result = {
                "snapshot_id": snapshot_id,
                "snapshot_total": snap_total,
                "ledger_total": ledger_total,
                "delta": round(delta, 4),
                "timestamp": snap_ts,
            }

            if abs(delta) > RECONCILE_THRESHOLD_USD:
                logger.warning(
                    "Reconciliation gap: snapshot=%d snapshot=$%.2f ledger=$%.2f delta=$%.2f",
                    snapshot_id, snap_total, ledger_total, delta,
                )

                if delta > 0:
                    event_type = "deposit_unreconciled" if delta > 50 else "appreciation"
                else:
                    event_type = "fee_unreconciled"

                # Record the gap as a ledger event so future reconciliations balance
                conn.execute(
                    """INSERT INTO capital_ledger
                       (event_type, timestamp, user_id, value_usd, trigger,
                        snapshot_id, reconciliation_delta_usd, reconciled, metadata_json)
                       VALUES (?, ?, ?, ?, 'reconciliation_gap', ?, ?, 1, ?)""",
                    (
                        event_type,
                        snap_ts,
                        user_id,
                        delta,
                        snapshot_id,
                        delta,
                        json.dumps({
                            "snapshot_total": snap_total,
                            "ledger_total": ledger_total,
                        }),
                    ),
                )
                result["event_type"] = event_type
            else:
                result["event_type"] = "balanced"

            # Mark snapshot reconciled
            try:
                conn.execute(
                    "UPDATE trading_snapshots SET reconciled=1 WHERE id=?",
                    (snapshot_id,),
                )
            except sqlite3.OperationalError:
                # Column doesn't exist yet — add it
                try:
                    conn.execute("ALTER TABLE trading_snapshots ADD COLUMN reconciled INTEGER DEFAULT 0")
                    conn.execute("UPDATE trading_snapshots SET reconciled=1 WHERE id=?", (snapshot_id,))
                except sqlite3.OperationalError:
                    pass

            conn.commit()
            return result
        finally:
            conn.close()

    def reconcile_all_unreconciled(self, limit: int = 50) -> list[dict]:
        """Find unreconciled snapshots and reconcile them.

        Returns a list of reconciliation results.
        """
        conn = self._connect()
        try:
            # Ensure reconciled column exists
            try:
                conn.execute("ALTER TABLE trading_snapshots ADD COLUMN reconciled INTEGER DEFAULT 0")
                conn.commit()
            except sqlite3.OperationalError:
                pass

            rows = conn.execute(
                """SELECT id FROM trading_snapshots
                   WHERE COALESCE(reconciled, 0) = 0
                   ORDER BY recorded_at ASC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        finally:
            conn.close()

        results = []
        for row in rows:
            result = self.reconcile_snapshot(row["id"])
            results.append(result)

        if results:
            gaps = [r for r in results if abs(r.get("delta", 0)) > RECONCILE_THRESHOLD_USD]
            logger.info(
                "Reconciled %d snapshots: %d balanced, %d with gaps",
                len(results),
                len(results) - len(gaps),
                len(gaps),
            )

        return results

    def get_unreconciled_gaps(self, user_id: int = 2) -> list[dict]:
        """Return all reconciliation gap events."""
        conn = self._connect()
        try:
            rows = conn.execute(
                """SELECT id, event_type, timestamp, value_usd, snapshot_id,
                          reconciliation_delta_usd, metadata_json
                   FROM capital_ledger
                   WHERE user_id=?
                     AND event_type IN ('appreciation', 'deposit_unreconciled', 'fee_unreconciled')
                   ORDER BY timestamp DESC""",
                (user_id,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def summary(self, user_id: int = 2) -> dict:
        """High-level reconciliation summary."""
        conn = self._connect()
        try:
            total_snapshots = conn.execute(
                "SELECT COUNT(*) AS n FROM trading_snapshots WHERE user_id=?",
                (user_id,),
            ).fetchone()["n"]

            reconciled_count = 0
            try:
                reconciled_count = conn.execute(
                    "SELECT COUNT(*) AS n FROM trading_snapshots WHERE user_id=? AND reconciled=1",
                    (user_id,),
                ).fetchone()["n"]
            except sqlite3.OperationalError:
                pass

            gap_row = conn.execute(
                """SELECT COUNT(*) AS n, COALESCE(SUM(ABS(reconciliation_delta_usd)), 0) AS total
                   FROM capital_ledger
                   WHERE user_id=?
                     AND event_type IN ('appreciation', 'deposit_unreconciled', 'fee_unreconciled')""",
                (user_id,),
            ).fetchone()

            ledger_total = conn.execute(
                "SELECT COALESCE(SUM(value_usd), 0) AS total FROM capital_ledger WHERE user_id=?",
                (user_id,),
            ).fetchone()["total"]

            latest_snap = conn.execute(
                "SELECT total_value_usd FROM trading_snapshots WHERE user_id=? ORDER BY recorded_at DESC LIMIT 1",
                (user_id,),
            ).fetchone()

            return {
                "total_snapshots": total_snapshots,
                "reconciled": reconciled_count,
                "unreconciled": total_snapshots - reconciled_count,
                "gap_events": gap_row["n"],
                "total_gap_usd": round(gap_row["total"], 2),
                "ledger_total_usd": round(ledger_total, 2),
                "latest_snapshot_usd": round(float(latest_snap["total_value_usd"] or 0), 2) if latest_snap else None,
            }
        finally:
            conn.close()


def main():
    """CLI entry point for manual reconciliation."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="Reconcile trading snapshots against capital ledger")
    parser.add_argument("--limit", type=int, default=50, help="Max snapshots to reconcile")
    parser.add_argument("--summary", action="store_true", help="Print summary only")
    parser.add_argument("--snapshot-id", type=int, help="Reconcile a specific snapshot")
    args = parser.parse_args()

    reconciler = LedgerReconciler()

    if args.summary:
        s = reconciler.summary()
        print(json.dumps(s, indent=2))
        return

    if args.snapshot_id:
        result = reconciler.reconcile_snapshot(args.snapshot_id)
        print(json.dumps(result, indent=2))
        return

    results = reconciler.reconcile_all_unreconciled(limit=args.limit)
    for r in results:
        status = "GAP" if abs(r.get("delta", 0)) > RECONCILE_THRESHOLD_USD else "OK"
        print(f"  [{status}] snapshot={r['snapshot_id']} "
              f"snap=${r['snapshot_total']:.2f} ledger=${r['ledger_total']:.2f} "
              f"delta=${r['delta']:.2f}")

    s = reconciler.summary()
    print(f"\nSummary: {s['reconciled']}/{s['total_snapshots']} reconciled, "
          f"{s['gap_events']} gaps totaling ${s['total_gap_usd']:.2f}")


if __name__ == "__main__":
    main()
