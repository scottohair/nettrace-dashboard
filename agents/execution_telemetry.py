#!/usr/bin/env python3
"""Execution telemetry store for request latency and order lifecycle metrics."""

import json
import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).parent / "execution_telemetry.db"


def _connect():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=3000")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS api_call_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue TEXT NOT NULL,
            method TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            latency_ms REAL NOT NULL,
            ok INTEGER NOT NULL DEFAULT 0,
            status_code INTEGER,
            error_text TEXT,
            context_json TEXT,
            created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS order_lifecycle_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue TEXT NOT NULL,
            order_id TEXT,
            pair TEXT NOT NULL,
            side TEXT NOT NULL,
            requested_usd REAL,
            ack_latency_ms REAL,
            fill_latency_ms REAL,
            status TEXT NOT NULL,
            details_json TEXT,
            created_at REAL NOT NULL
        );
        """
    )
    return db


def record_api_call(venue, method, endpoint, latency_ms, ok, status_code=None, error_text="", context=None):
    db = _connect()
    try:
        db.execute(
            """
            INSERT INTO api_call_metrics
                (venue, method, endpoint, latency_ms, ok, status_code, error_text, context_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(venue),
                str(method).upper(),
                str(endpoint),
                float(latency_ms),
                1 if ok else 0,
                None if status_code is None else int(status_code),
                str(error_text or ""),
                json.dumps(context or {}),
                float(time.time()),
            ),
        )
        db.commit()
    finally:
        db.close()


def record_order_lifecycle(
    venue,
    pair,
    side,
    status,
    order_id=None,
    requested_usd=None,
    ack_latency_ms=None,
    fill_latency_ms=None,
    details=None,
):
    db = _connect()
    try:
        db.execute(
            """
            INSERT INTO order_lifecycle_metrics
                (venue, order_id, pair, side, requested_usd, ack_latency_ms, fill_latency_ms, status, details_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(venue),
                None if order_id is None else str(order_id),
                str(pair),
                str(side).upper(),
                None if requested_usd is None else float(requested_usd),
                None if ack_latency_ms is None else float(ack_latency_ms),
                None if fill_latency_ms is None else float(fill_latency_ms),
                str(status),
                json.dumps(details or {}),
                float(time.time()),
            ),
        )
        db.commit()
    finally:
        db.close()


def _percentile(values, pct):
    if not values:
        return 0.0
    vals = sorted(float(v) for v in values)
    if len(vals) == 1:
        return vals[0]
    pct = max(0.0, min(100.0, float(pct)))
    rank = (pct / 100.0) * (len(vals) - 1)
    lo = int(rank)
    hi = min(len(vals) - 1, lo + 1)
    if lo == hi:
        return vals[lo]
    w = rank - lo
    return vals[lo] * (1.0 - w) + vals[hi] * w


def venue_health_snapshot(venue, window_minutes=30):
    db = _connect()
    try:
        now = float(time.time())
        cutoff = now - max(1, int(window_minutes)) * 60.0
        rows = db.execute(
            """
            SELECT latency_ms, ok
            FROM api_call_metrics
            WHERE venue=? AND created_at>=?
            ORDER BY id DESC
            LIMIT 2000
            """,
            (str(venue), cutoff),
        ).fetchall()
    finally:
        db.close()

    lat = [float(r["latency_ms"] or 0.0) for r in rows]
    ok = [int(r["ok"] or 0) for r in rows]
    total = len(rows)
    success = sum(ok)
    failure = max(0, total - success)
    failure_rate = (failure / total) if total > 0 else 1.0
    return {
        "venue": str(venue),
        "window_minutes": int(window_minutes),
        "samples": total,
        "success_rate": round((success / total) if total > 0 else 0.0, 4),
        "failure_rate": round(failure_rate, 4),
        "p50_latency_ms": round(_percentile(lat, 50), 3),
        "p90_latency_ms": round(_percentile(lat, 90), 3),
        "p99_latency_ms": round(_percentile(lat, 99), 3),
    }


def endpoint_latency_ms(venue, endpoint_prefix="", window_minutes=30, pct=90):
    db = _connect()
    try:
        now = float(time.time())
        cutoff = now - max(1, int(window_minutes)) * 60.0
        if endpoint_prefix:
            rows = db.execute(
                """
                SELECT latency_ms
                FROM api_call_metrics
                WHERE venue=? AND endpoint LIKE ? AND created_at>=?
                ORDER BY id DESC
                LIMIT 2000
                """,
                (str(venue), f"{endpoint_prefix}%", cutoff),
            ).fetchall()
        else:
            rows = db.execute(
                """
                SELECT latency_ms
                FROM api_call_metrics
                WHERE venue=? AND created_at>=?
                ORDER BY id DESC
                LIMIT 2000
                """,
                (str(venue), cutoff),
            ).fetchall()
    finally:
        db.close()
    vals = [float(r["latency_ms"] or 0.0) for r in rows]
    return round(_percentile(vals, pct), 3)


if __name__ == "__main__":
    print(json.dumps(venue_health_snapshot("coinbase", window_minutes=60), indent=2))
