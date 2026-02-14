#!/usr/bin/env python3
"""Execution telemetry store for request latency and order lifecycle metrics."""

import json
import os
import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).parent / "execution_telemetry.db"

EXCLUDE_DNS_FAILURES = os.environ.get(
    "EXEC_HEALTH_EXCLUDE_DNS_FAILURES", "1"
).lower() not in ("0", "false", "no")


def _is_dns_failure(error_text):
    """Detect DNS resolution failures (infrastructure, not API errors)."""
    t = str(error_text or "").lower()
    if not t:
        return False
    return "nodename" in t or "servname" in t or "[errno 8]" in t or "name resolution" in t


def _parse_prefix_list(value):
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set)):
        items = [str(v or "").strip() for v in value]
    else:
        items = [item.strip() for item in str(value).split(",")]
    ordered = []
    seen = set()
    for item in items:
        item_s = str(item or "").strip()
        if not item_s:
            continue
        if item_s in seen:
            continue
        seen.add(item_s)
        ordered.append(item_s)
    return tuple(ordered)


def _has_prefix(value, prefixes):
    if not prefixes:
        return True
    text = str(value or "")
    return any(text.startswith(prefix) for prefix in prefixes)


def _filter_api_rows(rows, include_prefixes=(), exclude_prefixes=()):
    includes = _parse_prefix_list(include_prefixes)
    excludes = _parse_prefix_list(exclude_prefixes)
    if not includes and not excludes:
        return rows

    filtered = []
    for row in rows:
        endpoint = ""
        if isinstance(row, sqlite3.Row):
            try:
                endpoint = row["endpoint"]
            except (IndexError, KeyError):
                endpoint = ""
        endpoint_s = str(endpoint or "")
        if includes and not _has_prefix(endpoint_s, includes):
            continue
        if excludes and _has_prefix(endpoint_s, excludes):
            continue
        filtered.append(row)
    return filtered


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


def venue_health_snapshot(
    venue,
    window_minutes=30,
    include_endpoint_prefixes=(),
    exclude_endpoint_prefixes=(),
):
    db = _connect()
    try:
        now = float(time.time())
        cutoff = now - max(1, int(window_minutes)) * 60.0
        rows = db.execute(
            """
            SELECT endpoint, latency_ms, ok, error_text
            FROM api_call_metrics
            WHERE venue=? AND created_at>=?
            ORDER BY id DESC
            LIMIT 2000
            """,
            (str(venue), cutoff),
        ).fetchall()
    finally:
        db.close()

    rows = _filter_api_rows(rows, include_prefixes=include_endpoint_prefixes, exclude_prefixes=exclude_endpoint_prefixes)

    # Filter out DNS resolution failures (infrastructure issue, not API issue)
    dns_failures_excluded = 0
    if EXCLUDE_DNS_FAILURES:
        filtered = []
        for r in rows:
            err = ""
            try:
                err = r["error_text"]
            except (IndexError, KeyError):
                pass
            if not int(r["ok"] or 0) and _is_dns_failure(err):
                dns_failures_excluded += 1
            else:
                filtered.append(r)
        rows = filtered

    lat = [float(r["latency_ms"] or 0.0) for r in rows]
    ok = [int(r["ok"] or 0) for r in rows]
    total = len(rows)
    success = sum(ok)
    failure = max(0, total - success)
    failure_rate = (failure / total) if total > 0 else 0.0  # assume healthy until proven otherwise
    return {
        "venue": str(venue),
        "window_minutes": int(window_minutes),
        "samples": total,
        "success_rate": round((success / total) if total > 0 else 0.0, 4),
        "failure_rate": round(failure_rate, 4),
        "p50_latency_ms": round(_percentile(lat, 50), 3),
        "p90_latency_ms": round(_percentile(lat, 90), 3),
        "p99_latency_ms": round(_percentile(lat, 99), 3),
        "dns_failures_excluded": dns_failures_excluded,
    }


def endpoint_latency_ms(
    venue,
    endpoint_prefix="",
    window_minutes=30,
    pct=90,
    include_endpoint_prefixes=(),
    exclude_endpoint_prefixes=(),
):
    db = _connect()
    try:
        now = float(time.time())
        cutoff = now - max(1, int(window_minutes)) * 60.0
        if endpoint_prefix:
            rows = db.execute(
                """
                SELECT endpoint, latency_ms
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
                SELECT endpoint, latency_ms
                FROM api_call_metrics
                WHERE venue=? AND created_at>=?
                ORDER BY id DESC
                LIMIT 2000
                """,
                (str(venue), cutoff),
            ).fetchall()
    finally:
        db.close()
    rows = _filter_api_rows(rows, include_prefixes=include_endpoint_prefixes, exclude_prefixes=exclude_endpoint_prefixes)
    vals = [float(r["latency_ms"] or 0.0) for r in rows]
    return round(_percentile(vals, pct), 3)


if __name__ == "__main__":
    print(json.dumps(venue_health_snapshot("coinbase", window_minutes=60), indent=2))
