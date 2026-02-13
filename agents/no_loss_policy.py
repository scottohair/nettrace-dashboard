#!/usr/bin/env python3
"""Strict no-loss policy gate + root-cause recorder."""

import json
import os
import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).parent / "no_loss_policy.db"

MIN_EXPECTED_EDGE_PCT = float(os.environ.get("NO_LOSS_MIN_EXPECTED_EDGE_PCT", "0.20"))
MAX_SPREAD_PCT = float(os.environ.get("NO_LOSS_MAX_SPREAD_PCT", "0.35"))
MAX_LATENCY_MS = float(os.environ.get("NO_LOSS_MAX_LATENCY_MS", "450"))
MAX_FAILURE_RATE = float(os.environ.get("NO_LOSS_MAX_FAILURE_RATE", "0.20"))
CONFIDENCE_FLOOR = float(os.environ.get("NO_LOSS_MIN_CONFIDENCE", "0.70"))


def _connect():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=3000")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS trade_policy_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            side TEXT NOT NULL,
            expected_edge_pct REAL,
            total_cost_pct REAL,
            spread_pct REAL,
            venue_latency_ms REAL,
            venue_failure_rate REAL,
            signal_confidence REAL,
            market_regime TEXT,
            approved INTEGER NOT NULL DEFAULT 0,
            reason TEXT,
            details_json TEXT,
            created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS policy_root_causes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            side TEXT NOT NULL,
            category TEXT NOT NULL,
            summary TEXT NOT NULL,
            details_json TEXT,
            created_at REAL NOT NULL
        );
        """
    )
    return db


def evaluate_trade(
    pair,
    side,
    expected_edge_pct,
    total_cost_pct,
    spread_pct,
    venue_latency_ms,
    venue_failure_rate,
    signal_confidence,
    market_regime="UNKNOWN",
):
    side_u = str(side).upper()
    expected = float(expected_edge_pct or 0.0)
    costs = max(0.0, float(total_cost_pct or 0.0))
    spread = max(0.0, float(spread_pct or 0.0))
    latency = max(0.0, float(venue_latency_ms or 0.0))
    failure = max(0.0, float(venue_failure_rate or 0.0))
    confidence = max(0.0, float(signal_confidence or 0.0))
    regime = str(market_regime or "UNKNOWN").upper()

    reasons = []
    min_required_edge = max(MIN_EXPECTED_EDGE_PCT, costs)
    if expected < min_required_edge:
        reasons.append(f"expected_edge {expected:.3f}% < required {min_required_edge:.3f}%")
    if spread > MAX_SPREAD_PCT:
        reasons.append(f"spread {spread:.3f}% > max {MAX_SPREAD_PCT:.3f}%")
    if latency > MAX_LATENCY_MS:
        reasons.append(f"latency {latency:.1f}ms > max {MAX_LATENCY_MS:.1f}ms")
    if failure > MAX_FAILURE_RATE:
        reasons.append(f"venue_failure_rate {failure:.2%} > max {MAX_FAILURE_RATE:.2%}")
    if confidence < CONFIDENCE_FLOOR:
        reasons.append(f"signal_confidence {confidence:.2%} < min {CONFIDENCE_FLOOR:.2%}")
    if side_u == "BUY" and regime in {"DOWNTREND", "BEARISH"}:
        reasons.append(f"regime {regime} blocks BUY under no-loss policy")

    approved = len(reasons) == 0
    decision = {
        "pair": str(pair),
        "side": side_u,
        "approved": approved,
        "reason": "approved" if approved else "; ".join(reasons),
        "expected_edge_pct": round(expected, 6),
        "total_cost_pct": round(costs, 6),
        "spread_pct": round(spread, 6),
        "venue_latency_ms": round(latency, 3),
        "venue_failure_rate": round(failure, 6),
        "signal_confidence": round(confidence, 6),
        "market_regime": regime,
        "min_required_edge_pct": round(min_required_edge, 6),
    }
    return decision


def log_decision(decision, details=None):
    payload = dict(decision or {})
    db = _connect()
    try:
        db.execute(
            """
            INSERT INTO trade_policy_decisions
                (pair, side, expected_edge_pct, total_cost_pct, spread_pct, venue_latency_ms, venue_failure_rate,
                 signal_confidence, market_regime, approved, reason, details_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(payload.get("pair", "")),
                str(payload.get("side", "")),
                float(payload.get("expected_edge_pct", 0.0) or 0.0),
                float(payload.get("total_cost_pct", 0.0) or 0.0),
                float(payload.get("spread_pct", 0.0) or 0.0),
                float(payload.get("venue_latency_ms", 0.0) or 0.0),
                float(payload.get("venue_failure_rate", 0.0) or 0.0),
                float(payload.get("signal_confidence", 0.0) or 0.0),
                str(payload.get("market_regime", "UNKNOWN")),
                1 if bool(payload.get("approved", False)) else 0,
                str(payload.get("reason", "")),
                json.dumps(details or {}),
                float(time.time()),
            ),
        )
        db.commit()
    finally:
        db.close()


def record_root_cause(pair, side, category, summary, details=None):
    db = _connect()
    try:
        db.execute(
            """
            INSERT INTO policy_root_causes (pair, side, category, summary, details_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(pair),
                str(side).upper(),
                str(category),
                str(summary),
                json.dumps(details or {}),
                float(time.time()),
            ),
        )
        db.commit()
    finally:
        db.close()


def summarize(window_hours=24):
    db = _connect()
    try:
        cutoff = float(time.time()) - max(1, int(window_hours)) * 3600.0
        rows = db.execute(
            """
            SELECT approved, reason
            FROM trade_policy_decisions
            WHERE created_at>=?
            ORDER BY id DESC
            """,
            (cutoff,),
        ).fetchall()
        causes = db.execute(
            """
            SELECT category, COUNT(*) AS n
            FROM policy_root_causes
            WHERE created_at>=?
            GROUP BY category
            ORDER BY n DESC
            LIMIT 20
            """,
            (cutoff,),
        ).fetchall()
    finally:
        db.close()

    total = len(rows)
    approved = sum(1 for r in rows if int(r["approved"] or 0) == 1)
    blocked = max(0, total - approved)
    return {
        "window_hours": int(window_hours),
        "total_decisions": total,
        "approved": approved,
        "blocked": blocked,
        "approval_rate": round((approved / total) if total > 0 else 0.0, 4),
        "top_root_causes": [{"category": str(r["category"]), "count": int(r["n"] or 0)} for r in causes],
    }


if __name__ == "__main__":
    print(json.dumps(summarize(24), indent=2))
