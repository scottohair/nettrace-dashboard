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
          source_region TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_quant_signals_created ON quant_signals(created_at);
        CREATE INDEX IF NOT EXISTS idx_quant_signals_type_time ON quant_signals(signal_type, created_at);
        CREATE INDEX IF NOT EXISTS idx_quant_signals_host_time ON quant_signals(target_host, created_at);
    """)
    # Migration: add source_region to existing tables that lack it
    try:
        db.execute("ALTER TABLE quant_signals ADD COLUMN source_region TEXT")
        db.commit()
    except sqlite3.OperationalError:
        pass  # column already exists


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
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
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


@signals_api.route("/signals/crypto-latency")
@verify_api_key
@require_tier(*PRO_PLUS_TIERS)
def crypto_latency():
    """Real-time crypto exchange latency data for trading signal generation.

    Returns the latest RTT measurements for all crypto exchange targets,
    including anomaly detection. This is the core trading edge:
    latency changes reveal infrastructure events before price moves.

    Query params:
        hours: lookback window (default 1, max 24)
        min_confidence: filter signals by confidence (default 0.5)
    """
    hours = parse_int_param(request.args.get("hours", "1"), 1, minimum=1, maximum=24)
    min_confidence = float(request.args.get("min_confidence", "0.5"))
    window_expr = f"-{hours} hours"

    db = get_db()

    # Get latest RTT for each crypto exchange
    crypto_hosts = db.execute("""
        SELECT target_host, target_name,
               total_rtt, first_hop_rtt, last_hop_rtt, hop_count,
               route_hash, scan_source, created_at
        FROM scan_metrics
        WHERE category = 'Crypto Exchanges'
          AND created_at >= datetime('now', ?)
        ORDER BY created_at DESC
    """, (window_expr,)).fetchall()

    # Aggregate: latest + stats per host
    host_data = {}
    for row in crypto_hosts:
        host = row["target_host"]
        if host not in host_data:
            host_data[host] = {
                "name": row["target_name"],
                "host": host,
                "latest_rtt": row["total_rtt"],
                "latest_at": row["created_at"],
                "measurements": [],
                "route_hash": row["route_hash"],
            }
        if len(host_data[host]["measurements"]) < 30:
            host_data[host]["measurements"].append({
                "rtt": row["total_rtt"],
                "hops": row["hop_count"],
                "at": row["created_at"],
            })

    # Compute stats and detect anomalies for each host
    for host, data in host_data.items():
        rtts = [m["rtt"] for m in data["measurements"] if m["rtt"] is not None]
        if rtts:
            data["avg_rtt"] = round(sum(rtts) / len(rtts), 2)
            data["min_rtt"] = round(min(rtts), 2)
            data["max_rtt"] = round(max(rtts), 2)
            data["samples"] = len(rtts)
            if data["latest_rtt"] and data["avg_rtt"] > 0:
                deviation = (data["latest_rtt"] - data["avg_rtt"]) / data["avg_rtt"]
                data["deviation_pct"] = round(deviation * 100, 2)
                data["is_anomaly"] = abs(deviation) > 0.20  # >20% deviation
            else:
                data["deviation_pct"] = 0
                data["is_anomaly"] = False
        del data["measurements"]  # don't send raw measurements

    # Get recent crypto signals
    ensure_quant_signals_table(db)
    recent_signals = db.execute("""
        SELECT signal_type, target_host, target_name, direction, confidence,
               details_json, created_at
        FROM quant_signals
        WHERE created_at >= datetime('now', ?)
          AND confidence >= ?
          AND target_host IN (
              SELECT DISTINCT target_host FROM scan_metrics WHERE category = 'Crypto Exchanges'
          )
        ORDER BY created_at DESC
        LIMIT 50
    """, (window_expr, min_confidence)).fetchall()

    signals = []
    for row in recent_signals:
        try:
            details = json.loads(row["details_json"]) if row["details_json"] else {}
        except Exception:
            details = {}
        signals.append({
            "signal_type": row["signal_type"],
            "target_host": row["target_host"],
            "direction": row["direction"],
            "confidence": row["confidence"],
            "details": details,
            "created_at": row["created_at"],
        })

    # Route changes = infrastructure events
    route_changes = db.execute("""
        SELECT target_host, target_name, rtt_delta, created_at
        FROM route_changes
        WHERE created_at >= datetime('now', ?)
          AND target_host IN (
              SELECT DISTINCT target_host FROM scan_metrics WHERE category = 'Crypto Exchanges'
          )
        ORDER BY created_at DESC
        LIMIT 20
    """, (window_expr,)).fetchall()

    return jsonify({
        "window_hours": hours,
        "exchanges": sorted(host_data.values(), key=lambda x: x.get("latest_rtt") or 999),
        "anomalies": [h for h in host_data.values() if h.get("is_anomaly")],
        "signals": signals,
        "route_changes": [
            {"host": r["target_host"], "name": r["target_name"],
             "rtt_delta": r["rtt_delta"], "at": r["created_at"]}
            for r in route_changes
        ],
        "total_exchanges": len(host_data),
        "total_anomalies": sum(1 for h in host_data.values() if h.get("is_anomaly")),
    })


# ---------------------------------------------------------------------------
# Cross-Region Signal Endpoints (scout regions push signals to primary)
# ---------------------------------------------------------------------------

VALID_SIGNAL_TYPES = {
    "latency_anomaly", "latency_spike", "route_change", "exchange_down",
    "price_divergence", "volume_spike", "scout_heartbeat",
}
VALID_DIRECTIONS = {"BUY", "SELL", "CAUTION", "INFO"}


@signals_api.route("/signals/push", methods=["POST"])
@verify_api_key
def push_signal():
    """Accept signals from regional scout agents.

    Scout regions (lhr, nrt, sin, etc.) push anomaly signals here.
    The primary region (ewr) collects them for cross-region divergence analysis.

    Body (JSON):
        signal_type: str (required)
        target_host: str (required)
        direction: str (required, one of BUY/SELL/CAUTION/INFO)
        confidence: float (required, 0-1)
        source_region: str (optional, auto-detected from FLY_REGION header)
        details: dict (optional, extra metadata)
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "JSON body required"}), 400

    signal_type = (data.get("signal_type") or "").strip()
    target_host = (data.get("target_host") or "").strip()
    direction = (data.get("direction") or "").strip().upper()
    confidence = data.get("confidence")
    source_region = (data.get("source_region")
                     or request.headers.get("X-Fly-Region")
                     or os.environ.get("FLY_REGION", "unknown"))
    details = data.get("details", {})

    # Validate required fields
    if not signal_type:
        return jsonify({"error": "signal_type required"}), 400
    if not target_host:
        return jsonify({"error": "target_host required"}), 400
    if direction not in VALID_DIRECTIONS:
        return jsonify({"error": f"direction must be one of {VALID_DIRECTIONS}"}), 400
    if confidence is None or not (0 <= float(confidence) <= 1):
        return jsonify({"error": "confidence must be 0-1"}), 400

    confidence = float(confidence)

    # Include source_region in details for backward compatibility
    if isinstance(details, dict):
        details["source_region"] = source_region
    details_json = json.dumps(details) if details else None

    db = get_db()
    ensure_quant_signals_table(db)

    db.execute(
        """INSERT INTO quant_signals
           (signal_type, target_host, target_name, direction, confidence, details_json, source_region)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (signal_type, target_host, target_host, direction, confidence,
         details_json, source_region),
    )
    db.commit()

    return jsonify({
        "status": "ok",
        "signal_type": signal_type,
        "target_host": target_host,
        "source_region": source_region,
    })


@signals_api.route("/signals/cross-region")
@verify_api_key
@require_tier(*PRO_PLUS_TIERS)
def cross_region_signals():
    """Return signals where multiple regions report on the same target.

    This is the alpha: cross-region latency divergence reveals infrastructure
    events invisible to single-region observers.

    If lhr sees a spike but ewr doesn't -> US-side routing normal, EU problem.
    If ALL regions see a spike -> exchange infrastructure issue -> strong signal.

    Query params:
        hours: lookback window (default 1, max 24)
        min_confidence: minimum signal confidence (default 0.5)
    """
    hours = parse_int_param(request.args.get("hours", "1"), 1, minimum=1, maximum=24)
    min_confidence = float(request.args.get("min_confidence", "0.5"))
    window_expr = f"-{hours} hours"

    db = get_db()
    ensure_quant_signals_table(db)

    # Find targets with signals from multiple regions in the time window
    multi_region = db.execute("""
        SELECT target_host,
               COUNT(DISTINCT source_region) AS region_count,
               GROUP_CONCAT(DISTINCT source_region) AS regions,
               AVG(confidence) AS avg_confidence,
               COUNT(*) AS signal_count
        FROM quant_signals
        WHERE created_at >= datetime('now', ?)
          AND confidence >= ?
          AND source_region IS NOT NULL
          AND signal_type != 'scout_heartbeat'
        GROUP BY target_host
        HAVING COUNT(DISTINCT source_region) >= 1
        ORDER BY region_count DESC, avg_confidence DESC
    """, (window_expr, min_confidence)).fetchall()

    # For each multi-region target, get the individual signals
    results = []
    for row in multi_region:
        host = row["target_host"]
        signals = db.execute("""
            SELECT signal_type, target_host, direction, confidence,
                   source_region, details_json, created_at
            FROM quant_signals
            WHERE target_host = ?
              AND created_at >= datetime('now', ?)
              AND confidence >= ?
              AND source_region IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 20
        """, (host, window_expr, min_confidence)).fetchall()

        signal_list = []
        for s in signals:
            try:
                det = json.loads(s["details_json"]) if s["details_json"] else {}
            except (TypeError, json.JSONDecodeError):
                det = {}
            signal_list.append({
                "signal_type": s["signal_type"],
                "direction": s["direction"],
                "confidence": s["confidence"],
                "source_region": s["source_region"],
                "details": det,
                "created_at": s["created_at"],
            })

        # Determine if this is a divergence (regions disagree) or consensus (all agree)
        directions = set(s["direction"] for s in signal_list if s["direction"] != "INFO")
        is_divergence = len(directions) > 1

        results.append({
            "target_host": host,
            "region_count": row["region_count"],
            "regions": row["regions"].split(",") if row["regions"] else [],
            "avg_confidence": round(float(row["avg_confidence"]), 4) if row["avg_confidence"] else 0,
            "signal_count": row["signal_count"],
            "is_divergence": is_divergence,
            "signals": signal_list,
        })

    # Scout heartbeats (separate — for monitoring which scouts are alive)
    heartbeats = db.execute("""
        SELECT target_host, source_region, confidence, details_json, created_at
        FROM quant_signals
        WHERE signal_type = 'scout_heartbeat'
          AND created_at >= datetime('now', '-10 minutes')
        ORDER BY created_at DESC
    """).fetchall()

    active_scouts = {}
    for hb in heartbeats:
        region = hb["source_region"]
        if region not in active_scouts:
            try:
                det = json.loads(hb["details_json"]) if hb["details_json"] else {}
            except (TypeError, json.JSONDecodeError):
                det = {}
            active_scouts[region] = {
                "region": region,
                "last_seen": hb["created_at"],
                "uptime_seconds": det.get("uptime_seconds", 0),
                "agents": det.get("agents_running", []),
            }

    return jsonify({
        "window_hours": hours,
        "min_confidence": min_confidence,
        "cross_region_signals": results,
        "total_targets": len(results),
        "divergences": sum(1 for r in results if r["is_divergence"]),
        "active_scouts": active_scouts,
        "tier": g.api_tier,
    })
