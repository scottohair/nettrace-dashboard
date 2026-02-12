"""Signals API Blueprint for NetTrace quant signals."""

import json
import os
import sqlite3
from pathlib import Path

from flask import Blueprint, g, jsonify, request

from api_auth import require_tier, verify_api_key

signals_api = Blueprint("signals_api", __name__, url_prefix="/api/v1")

DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent / "traceroute.db"))
PRO_PLUS_TIERS = ("pro", "enterprise", "enterprise_pro", "government")


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


def ensure_quant_signals_table(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS quant_signals (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_type TEXT,
          target_host TEXT,
          target_name TEXT,
          direction TEXT,
          confidence REAL,
          details_json TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_quant_signals_created ON quant_signals(created_at);
        CREATE INDEX IF NOT EXISTS idx_quant_signals_type_time ON quant_signals(signal_type, created_at);
        CREATE INDEX IF NOT EXISTS idx_quant_signals_host_time ON quant_signals(target_host, created_at);
    """)


def parse_int_param(value, default, minimum=None, maximum=None):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default

    if minimum is not None:
        parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


@signals_api.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    return response


@signals_api.route("/signals")
@verify_api_key
@require_tier(*PRO_PLUS_TIERS)
def list_signals():
    """List recent quant signals (pro+ tiers)."""
    limit = parse_int_param(request.args.get("limit", "50"), 50, minimum=1, maximum=200)
    signal_type = request.args.get("signal_type", "").strip()
    target_host = request.args.get("host", "").strip()
    direction = request.args.get("direction", "").strip()

    db = get_db()
    ensure_quant_signals_table(db)

    sql = """
        SELECT id, signal_type, target_host, target_name, direction,
               confidence, details_json, created_at
        FROM quant_signals
        WHERE 1 = 1
    """
    params = []

    if signal_type:
        sql += " AND signal_type = ?"
        params.append(signal_type)
    if target_host:
        sql += " AND target_host = ?"
        params.append(target_host)
    if direction:
        sql += " AND direction = ?"
        params.append(direction)

    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    rows = db.execute(sql, tuple(params)).fetchall()
    signals = []
    for row in rows:
        try:
            details = json.loads(row["details_json"]) if row["details_json"] else {}
        except (TypeError, json.JSONDecodeError):
            details = {}

        signals.append({
            "id": row["id"],
            "signal_type": row["signal_type"],
            "target_host": row["target_host"],
            "target_name": row["target_name"],
            "direction": row["direction"],
            "confidence": row["confidence"],
            "details": details,
            "created_at": row["created_at"],
        })

    return jsonify({
        "signals": signals,
        "count": len(signals),
        "tier": g.api_tier,
        "usage": {"used": g.api_usage_today, "limit": g.api_rate_limit},
    })


@signals_api.route("/signals/summary")
@verify_api_key
@require_tier(*PRO_PLUS_TIERS)
def signals_summary():
    """Aggregate quant signal stats."""
    hours = parse_int_param(request.args.get("hours", "24"), 24, minimum=1, maximum=720)
    window_expr = f"-{hours} hours"

    db = get_db()
    ensure_quant_signals_table(db)

    total_row = db.execute("SELECT COUNT(*) AS cnt FROM quant_signals").fetchone()
    recent_row = db.execute(
        """
        SELECT COUNT(*) AS cnt, AVG(confidence) AS avg_confidence
        FROM quant_signals
        WHERE created_at >= datetime('now', ?)
        """,
        (window_expr,),
    ).fetchone()
    latest_row = db.execute(
        "SELECT created_at FROM quant_signals ORDER BY id DESC LIMIT 1"
    ).fetchone()

    by_type_rows = db.execute(
        """
        SELECT signal_type, COUNT(*) AS cnt
        FROM quant_signals
        WHERE created_at >= datetime('now', ?)
        GROUP BY signal_type
        ORDER BY cnt DESC
        """,
        (window_expr,),
    ).fetchall()

    by_direction_rows = db.execute(
        """
        SELECT direction, COUNT(*) AS cnt
        FROM quant_signals
        WHERE created_at >= datetime('now', ?)
        GROUP BY direction
        ORDER BY cnt DESC
        """,
        (window_expr,),
    ).fetchall()

    top_target_rows = db.execute(
        """
        SELECT target_host, target_name, COUNT(*) AS cnt
        FROM quant_signals
        WHERE created_at >= datetime('now', ?)
        GROUP BY target_host, target_name
        ORDER BY cnt DESC
        LIMIT 10
        """,
        (window_expr,),
    ).fetchall()

    return jsonify({
        "window_hours": hours,
        "total_signals": total_row["cnt"] if total_row else 0,
        "signals_in_window": recent_row["cnt"] if recent_row else 0,
        "avg_confidence_in_window": round(float(recent_row["avg_confidence"]), 4)
        if recent_row and recent_row["avg_confidence"] is not None else None,
        "latest_signal_at": latest_row["created_at"] if latest_row else None,
        "by_type": [
            {"signal_type": row["signal_type"], "count": row["cnt"]}
            for row in by_type_rows
        ],
        "by_direction": [
            {"direction": row["direction"], "count": row["cnt"]}
            for row in by_direction_rows
        ],
        "top_targets": [
            {"target_host": row["target_host"], "target_name": row["target_name"], "count": row["cnt"]}
            for row in top_target_rows
        ],
        "tier": g.api_tier,
        "usage": {"used": g.api_usage_today, "limit": g.api_rate_limit},
    })
