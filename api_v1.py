"""REST API v1 Blueprint for NetTrace Financial Network Intelligence."""

import json
import csv
import io
import sqlite3
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

from flask import Blueprint, request, jsonify, g, Response
from api_auth import verify_api_key, require_tier, TIER_CONFIG

api_v1 = Blueprint("api_v1", __name__, url_prefix="/api/v1")

DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent / "traceroute.db"))


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


def parse_int_query_param(name, default, max_value=None):
    """Parse an integer query param and return a 400 JSON response if malformed."""
    raw_value = request.args.get(name)

    if raw_value is None:
        value = default
    else:
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            return None, (
                jsonify({"error": f"Invalid query parameter '{name}': expected integer"}),
                400,
            )

    if value < 1:
        return None, (
            jsonify({"error": f"Invalid query parameter '{name}': must be >= 1"}),
            400,
        )

    if max_value is not None:
        value = min(value, max_value)

    return value, None


@api_v1.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    return response


@api_v1.route("/")
def api_docs():
    """API documentation / index."""
    return jsonify({
        "api": "NetTrace Financial Network Intelligence",
        "version": "v1",
        "endpoints": {
            "GET /api/v1/targets": "List all monitored targets",
            "GET /api/v1/latency/<host>": "Current latency for a target",
            "GET /api/v1/latency/<host>/history": "Historical latency time-series",
            "GET /api/v1/routes/<host>": "Current route data (Pro+)",
            "GET /api/v1/routes/<host>/changes": "Route change history (Pro+)",
            "GET /api/v1/rankings": "Latency rankings by category",
            "GET /api/v1/compare?hosts=a,b": "Compare targets (Pro+)",
            "GET /api/v1/export/<host>?format=csv|json": "Bulk export (Enterprise)",
            "GET /api/v1/signals": "Recent quant signals (Pro+)",
            "GET /api/v1/signals/summary": "Aggregated quant signal stats (Pro+)",
        },
        "auth": "Pass API key via Authorization: Bearer <key> header or ?api_key= query param",
        "tiers": {
            "free": "$0/mo — 100 API calls/day, 24h history, demo data",
            "pro": "$249/mo — 10k calls/day, 30d history, routes, alerts",
            "enterprise": "$2,499/mo — Unlimited, 90d history, bulk export, WebSocket stream",
            "enterprise_pro": "$50,000/mo — Dedicated scanning, 365d history, custom targets, SLA",
            "government": "$500,000/mo — FedRAMP-ready, 2yr history, dedicated infrastructure, 24/7 SLA",
        },
        "rate_limit": "Daily quota resets at UTC midnight. Check X-RateLimit-* response headers."
    })


# ── GET /api/v1/targets ──────────────────────────────────────────────────

@api_v1.route("/targets")
@verify_api_key
def list_targets():
    """List all monitored targets with latest metrics."""
    db = get_db()

    # Get latest metric per target
    rows = db.execute("""
        SELECT m.target_host, m.target_name, m.category, m.total_rtt,
               m.hop_count, m.route_hash, m.created_at
        FROM scan_metrics m
        INNER JOIN (
            SELECT target_host, MAX(id) as max_id FROM scan_metrics GROUP BY target_host
        ) latest ON m.id = latest.max_id
        ORDER BY m.category, m.target_name
    """).fetchall()

    targets = []
    for r in rows:
        targets.append({
            "host": r["target_host"],
            "name": r["target_name"],
            "category": r["category"],
            "total_rtt": r["total_rtt"],
            "hop_count": r["hop_count"],
            "route_hash": r["route_hash"],
            "last_scan": r["created_at"]
        })

    return jsonify({
        "targets": targets,
        "count": len(targets),
        "tier": g.api_tier,
        "usage": {"used": g.api_usage_today, "limit": g.api_rate_limit}
    })


# ── GET /api/v1/latency/<host> ───────────────────────────────────────────

@api_v1.route("/latency/<path:host>")
@verify_api_key
def get_latency(host):
    """Get current latency for a target."""
    db = get_db()
    row = db.execute(
        """SELECT target_host, target_name, category, total_rtt, hop_count,
                  first_hop_rtt, last_hop_rtt, route_hash, created_at
           FROM scan_metrics WHERE target_host = ? ORDER BY id DESC LIMIT 1""",
        (host,)
    ).fetchone()

    if not row:
        return jsonify({"error": "Target not found", "host": host}), 404

    return jsonify({
        "host": row["target_host"],
        "name": row["target_name"],
        "category": row["category"],
        "latency": {
            "total_rtt": row["total_rtt"],
            "first_hop_rtt": row["first_hop_rtt"],
            "last_hop_rtt": row["last_hop_rtt"],
        },
        "hop_count": row["hop_count"],
        "route_hash": row["route_hash"],
        "measured_at": row["created_at"],
    })


# ── GET /api/v1/latency/<host>/history ───────────────────────────────────

@api_v1.route("/latency/<path:host>/history")
@verify_api_key
def get_latency_history(host):
    """Get historical latency time-series."""
    tier_conf = g.tier_config
    history_days = tier_conf["history_days"]

    # Allow limit param (max 1000)
    limit, error_response = parse_int_query_param("limit", default=500, max_value=1000)
    if error_response:
        return error_response

    cutoff = (datetime.now(timezone.utc) - timedelta(days=history_days)).isoformat()

    db = get_db()
    rows = db.execute(
        """SELECT total_rtt, hop_count, first_hop_rtt, last_hop_rtt, route_hash, created_at
           FROM scan_metrics WHERE target_host = ? AND created_at > ?
           ORDER BY created_at DESC LIMIT ?""",
        (host, cutoff, limit)
    ).fetchall()

    if not rows:
        return jsonify({"error": "No data found for this target", "host": host}), 404

    data_points = []
    for r in rows:
        data_points.append({
            "total_rtt": r["total_rtt"],
            "hop_count": r["hop_count"],
            "first_hop_rtt": r["first_hop_rtt"],
            "last_hop_rtt": r["last_hop_rtt"],
            "route_hash": r["route_hash"],
            "timestamp": r["created_at"]
        })

    return jsonify({
        "host": host,
        "history_days": history_days,
        "data_points": len(data_points),
        "tier": g.api_tier,
        "data": data_points
    })


# ── GET /api/v1/routes/<host> ────────────────────────────────────────────

@api_v1.route("/routes/<path:host>")
@verify_api_key
@require_tier("pro", "enterprise", "enterprise_pro", "government")
def get_routes(host):
    """Get current route data with full hop details."""
    db = get_db()

    # Get latest metric
    metric = db.execute(
        "SELECT id, route_hash, created_at FROM scan_metrics WHERE target_host = ? ORDER BY id DESC LIMIT 1",
        (host,)
    ).fetchone()

    if not metric:
        return jsonify({"error": "Target not found"}), 404

    # Get matching snapshot
    snapshot = db.execute(
        "SELECT hops_json FROM scan_snapshots WHERE metric_id = ?",
        (metric["id"],)
    ).fetchone()

    if not snapshot:
        # Try the most recent snapshot for this target
        snapshot = db.execute(
            """SELECT s.hops_json FROM scan_snapshots s
               JOIN scan_metrics m ON s.metric_id = m.id
               WHERE m.target_host = ? ORDER BY s.id DESC LIMIT 1""",
            (host,)
        ).fetchone()

    hops = json.loads(snapshot["hops_json"]) if snapshot else []

    return jsonify({
        "host": host,
        "route_hash": metric["route_hash"],
        "measured_at": metric["created_at"],
        "hops": hops
    })


# ── GET /api/v1/routes/<host>/changes ────────────────────────────────────

@api_v1.route("/routes/<path:host>/changes")
@verify_api_key
@require_tier("pro", "enterprise", "enterprise_pro", "government")
def get_route_changes(host):
    """Get route change history for a target."""
    limit, error_response = parse_int_query_param("limit", default=50, max_value=200)
    if error_response:
        return error_response

    db = get_db()
    rows = db.execute(
        """SELECT old_route_hash, new_route_hash, new_hops_json, rtt_delta, detected_at
           FROM route_changes WHERE target_host = ?
           ORDER BY detected_at DESC LIMIT ?""",
        (host, limit)
    ).fetchall()

    changes = []
    for r in rows:
        changes.append({
            "old_route_hash": r["old_route_hash"],
            "new_route_hash": r["new_route_hash"],
            "rtt_delta": r["rtt_delta"],
            "detected_at": r["detected_at"],
            "hops": json.loads(r["new_hops_json"]) if r["new_hops_json"] else []
        })

    return jsonify({
        "host": host,
        "changes": changes,
        "count": len(changes)
    })


# ── GET /api/v1/rankings ─────────────────────────────────────────────────

@api_v1.route("/rankings")
@verify_api_key
def get_rankings():
    """Get latency rankings by category."""
    category = request.args.get("category")
    limit, error_response = parse_int_query_param("limit", default=50, max_value=100)
    if error_response:
        return error_response

    db = get_db()

    if category:
        rows = db.execute("""
            SELECT m.target_host, m.target_name, m.category, m.total_rtt, m.hop_count, m.created_at
            FROM scan_metrics m
            INNER JOIN (
                SELECT target_host, MAX(id) as max_id FROM scan_metrics
                WHERE category = ? GROUP BY target_host
            ) latest ON m.id = latest.max_id
            WHERE m.total_rtt IS NOT NULL
            ORDER BY m.total_rtt ASC LIMIT ?
        """, (category, limit)).fetchall()
    else:
        rows = db.execute("""
            SELECT m.target_host, m.target_name, m.category, m.total_rtt, m.hop_count, m.created_at
            FROM scan_metrics m
            INNER JOIN (
                SELECT target_host, MAX(id) as max_id FROM scan_metrics GROUP BY target_host
            ) latest ON m.id = latest.max_id
            WHERE m.total_rtt IS NOT NULL
            ORDER BY m.total_rtt ASC LIMIT ?
        """, (limit,)).fetchall()

    rankings = []
    for i, r in enumerate(rows, 1):
        rankings.append({
            "rank": i,
            "host": r["target_host"],
            "name": r["target_name"],
            "category": r["category"],
            "total_rtt": r["total_rtt"],
            "hop_count": r["hop_count"],
            "last_scan": r["created_at"]
        })

    return jsonify({
        "rankings": rankings,
        "count": len(rankings),
        "category": category or "all"
    })


# ── GET /api/v1/compare ──────────────────────────────────────────────────

@api_v1.route("/compare")
@verify_api_key
@require_tier("pro", "enterprise", "enterprise_pro", "government")
def compare_targets():
    """Compare latency between multiple targets."""
    hosts_param = request.args.get("hosts", "")
    if not hosts_param:
        return jsonify({"error": "Provide hosts as comma-separated list: ?hosts=a.com,b.com"}), 400

    hosts = [h.strip() for h in hosts_param.split(",") if h.strip()]
    if len(hosts) < 2:
        return jsonify({"error": "Provide at least 2 hosts to compare"}), 400
    if len(hosts) > 10:
        return jsonify({"error": "Maximum 10 hosts per comparison"}), 400

    db = get_db()
    results = []
    for host in hosts:
        row = db.execute(
            """SELECT target_host, target_name, category, total_rtt, hop_count,
                      first_hop_rtt, last_hop_rtt, route_hash, created_at
               FROM scan_metrics WHERE target_host = ? ORDER BY id DESC LIMIT 1""",
            (host,)
        ).fetchone()

        if row:
            results.append({
                "host": row["target_host"],
                "name": row["target_name"],
                "category": row["category"],
                "total_rtt": row["total_rtt"],
                "hop_count": row["hop_count"],
                "first_hop_rtt": row["first_hop_rtt"],
                "last_hop_rtt": row["last_hop_rtt"],
                "route_hash": row["route_hash"],
                "last_scan": row["created_at"]
            })
        else:
            results.append({"host": host, "error": "not found"})

    # Sort by RTT
    found = [r for r in results if "total_rtt" in r and r["total_rtt"]]
    found.sort(key=lambda x: x["total_rtt"])
    not_found = [r for r in results if "error" in r]

    return jsonify({
        "comparison": found + not_found,
        "fastest": found[0]["host"] if found else None,
        "rtt_spread": round(found[-1]["total_rtt"] - found[0]["total_rtt"], 2) if len(found) >= 2 else None
    })


# ── GET /api/v1/export/<host> ────────────────────────────────────────────

@api_v1.route("/export/<path:host>")
@verify_api_key
@require_tier("enterprise", "enterprise_pro", "government")
def export_data(host):
    """Bulk export historical data (Enterprise only)."""
    fmt = request.args.get("format", "json").lower()
    limit, error_response = parse_int_query_param("limit", default=10000, max_value=50000)
    if error_response:
        return error_response

    tier_conf = g.tier_config
    cutoff = (datetime.now(timezone.utc) - timedelta(days=tier_conf["history_days"])).isoformat()

    db = get_db()
    rows = db.execute(
        """SELECT target_host, target_name, category, total_rtt, hop_count,
                  first_hop_rtt, last_hop_rtt, route_hash, scan_source, created_at
           FROM scan_metrics WHERE target_host = ? AND created_at > ?
           ORDER BY created_at DESC LIMIT ?""",
        (host, cutoff, limit)
    ).fetchall()

    if not rows:
        return jsonify({"error": "No data found"}), 404

    if fmt == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["timestamp", "host", "name", "category", "total_rtt",
                         "hop_count", "first_hop_rtt", "last_hop_rtt", "route_hash", "source"])
        for r in rows:
            writer.writerow([r["created_at"], r["target_host"], r["target_name"],
                             r["category"], r["total_rtt"], r["hop_count"],
                             r["first_hop_rtt"], r["last_hop_rtt"], r["route_hash"],
                             r["scan_source"]])
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=nettrace_{host}.csv"}
        )

    # JSON format
    data = []
    for r in rows:
        data.append({
            "timestamp": r["created_at"],
            "host": r["target_host"],
            "name": r["target_name"],
            "category": r["category"],
            "total_rtt": r["total_rtt"],
            "hop_count": r["hop_count"],
            "first_hop_rtt": r["first_hop_rtt"],
            "last_hop_rtt": r["last_hop_rtt"],
            "route_hash": r["route_hash"],
            "source": r["scan_source"]
        })

    return jsonify({
        "host": host,
        "export_rows": len(data),
        "history_days": tier_conf["history_days"],
        "data": data
    })


# ── GET /api/v1/meta-engine/status ────────────────────────────────────────

def _get_meta_engine_db_path():
    """Resolve meta_engine.db path: /app/agents/ on Fly, agents/ locally."""
    fly_path = "/app/agents/meta_engine.db"
    local_path = str(Path(__file__).parent / "agents" / "meta_engine.db")
    if os.path.exists(fly_path):
        return fly_path
    return local_path


def _read_meta_engine_status():
    """Read meta_engine.db and return status dict. Returns empty data if DB missing."""
    db_path = _get_meta_engine_db_path()
    empty = {
        "agents": [],
        "recent_ideas": [],
        "recent_predictions": [],
        "evolution_log": [],
        "cycle_count": 0,
    }

    if not os.path.exists(db_path):
        return empty

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Agents
        agents = []
        try:
            rows = conn.execute("""
                SELECT name, strategy_type, status, sharpe_ratio, trades, wins,
                       losses, total_pnl, max_drawdown, last_active, created_at
                FROM meta_agents ORDER BY sharpe_ratio DESC
            """).fetchall()
            for r in rows:
                agents.append({
                    "name": r["name"],
                    "strategy_type": r["strategy_type"],
                    "status": r["status"],
                    "sharpe": r["sharpe_ratio"] or 0.0,
                    "trades": r["trades"] or 0,
                    "wins": r["wins"] or 0,
                    "losses": r["losses"] or 0,
                    "total_pnl": r["total_pnl"] or 0.0,
                    "max_drawdown": r["max_drawdown"] or 0.0,
                    "last_active": r["last_active"],
                    "created_at": r["created_at"],
                })
        except sqlite3.OperationalError:
            pass

        # Recent ideas
        recent_ideas = []
        try:
            rows = conn.execute("""
                SELECT source, idea_type, description, confidence, status, created_at
                FROM meta_ideas ORDER BY id DESC LIMIT 20
            """).fetchall()
            for r in rows:
                recent_ideas.append({
                    "source": r["source"],
                    "type": r["idea_type"],
                    "description": r["description"],
                    "confidence": r["confidence"] or 0.0,
                    "status": r["status"],
                    "created_at": r["created_at"],
                })
        except sqlite3.OperationalError:
            pass

        # Recent predictions (paper trades as proxy)
        recent_predictions = []
        try:
            rows = conn.execute("""
                SELECT pair, direction, confidence, entry_price, current_price,
                       paper_pnl, status, created_at
                FROM meta_paper_trades ORDER BY id DESC LIMIT 20
            """).fetchall()
            for r in rows:
                recent_predictions.append({
                    "pair": r["pair"],
                    "direction": r["direction"],
                    "confidence": r["confidence"] or 0.0,
                    "entry_price": r["entry_price"],
                    "current_price": r["current_price"],
                    "paper_pnl": r["paper_pnl"] or 0.0,
                    "status": r["status"],
                    "created_at": r["created_at"],
                })
        except sqlite3.OperationalError:
            pass

        # Evolution log
        evolution_log = []
        try:
            rows = conn.execute("""
                SELECT action, details, created_at
                FROM meta_evolution_log ORDER BY id DESC LIMIT 20
            """).fetchall()
            for r in rows:
                evolution_log.append({
                    "action": r["action"],
                    "details": r["details"],
                    "timestamp": r["created_at"],
                })
        except sqlite3.OperationalError:
            pass

        # Cycle count = number of evolution log entries
        cycle_count = 0
        try:
            row = conn.execute("SELECT COUNT(*) as cnt FROM meta_evolution_log").fetchone()
            cycle_count = row["cnt"] if row else 0
        except sqlite3.OperationalError:
            pass

        conn.close()

        return {
            "agents": agents,
            "recent_ideas": recent_ideas,
            "recent_predictions": recent_predictions,
            "evolution_log": evolution_log,
            "cycle_count": cycle_count,
        }
    except Exception:
        return empty


@api_v1.route("/meta-engine/status")
@verify_api_key
def meta_engine_status():
    """Get meta-engine status: agents, ideas, predictions, evolution log."""
    status = _read_meta_engine_status()
    return jsonify(status)
