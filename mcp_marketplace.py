"""MCP Agent Marketplace Server — SSE transport for external AI agents.

Implements the MCP (Model Context Protocol) over HTTP/SSE so external agents
can connect to NetTrace via HTTPS with Bearer token auth (org API key).

Mounted at /mcp/v1 in the Flask app. This is separate from mcp_server.py
(which is Scott's local stdio MCP tool).

Protocol: JSON-RPC 2.0 over Server-Sent Events (SSE)
  POST /mcp/v1/message   — JSON-RPC request (Bearer token required)
  GET  /mcp/v1/sse        — SSE stream for responses (Bearer token required)

Tools exposed to agents (scoped by org + agent registration):
  - market.signals:    Quant signals (tier-gated window/count)
  - market.latency:    Exchange latency data
  - market.prices:     Current prices
  - market.orderbook:  Orderbook depth (Pro+)
  - portfolio.status:  Agent's capital + positions
  - portfolio.history: Trade history
  - trade.propose:     Submit trade proposal
  - trade.proposals:   List pending proposals
  - trade.cancel:      Cancel proposal
  - agent.register:    Self-register
  - agent.status:      Own performance metrics
  - agent.heartbeat:   Keep-alive
"""

import hashlib
import json
import os
import queue
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, Response, request, jsonify, g

from api_auth import hash_api_key, TIER_CONFIG

mcp_bp = Blueprint("mcp_marketplace", __name__, url_prefix="/mcp/v1")

DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent / "traceroute.db"))

# Connected SSE clients: {session_id: queue.Queue}
_sse_clients = {}
_sse_lock = threading.Lock()


def _get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


def _db_connect():
    """Non-request DB connection (for background threads)."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def _auth_bearer():
    """Authenticate Bearer token, return (api_key_row, org_row) or (None, error_response)."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None, (jsonify({"error": "Bearer token required"}), 401)

    raw_key = auth[7:].strip()
    if not raw_key:
        return None, (jsonify({"error": "Empty Bearer token"}), 401)

    key_hash = hash_api_key(raw_key)
    db = _get_db()

    row = db.execute(
        "SELECT ak.id, ak.user_id, ak.tier, ak.is_active, ak.org_id, "
        "o.slug as org_slug, o.tier as org_tier, o.is_active as org_active "
        "FROM api_keys ak "
        "LEFT JOIN organizations o ON ak.org_id = o.id "
        "WHERE ak.key_hash = ?",
        (key_hash,)
    ).fetchone()

    if not row:
        return None, (jsonify({"error": "Invalid API key"}), 401)
    if not row["is_active"]:
        return None, (jsonify({"error": "API key deactivated"}), 401)
    if row["org_id"] and not row["org_active"]:
        return None, (jsonify({"error": "Organization deactivated"}), 403)

    return row, None


# ── MCP Tool Definitions ────────────────────────────────────────────────

MCP_TOOLS = [
    {
        "name": "market.signals",
        "description": "Get recent quant signals (latency, route change, cross-region). "
                       "Count and time window gated by tier.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "signal_type": {"type": "string", "description": "Filter by type"},
                "limit": {"type": "integer", "default": 20, "maximum": 100},
                "hours": {"type": "integer", "default": 1, "maximum": 24},
            },
        },
    },
    {
        "name": "market.latency",
        "description": "Get current exchange latency data.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "host": {"type": "string", "description": "Target host (e.g. coinbase.com)"},
            },
            "required": ["host"],
        },
    },
    {
        "name": "market.prices",
        "description": "Get current crypto prices from latest trading snapshot.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pair": {"type": "string", "description": "Trading pair (e.g. BTC-USD)"},
            },
        },
    },
    {
        "name": "portfolio.status",
        "description": "Get your agent's capital allocation and current positions.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "portfolio.history",
        "description": "Get trade history for your agent.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "default": 20, "maximum": 100},
            },
        },
    },
    {
        "name": "trade.propose",
        "description": "Submit a trade proposal. Proposals are validated against org risk policy.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pair": {"type": "string", "description": "Trading pair (e.g. BTC-USD)"},
                "direction": {"type": "string", "enum": ["BUY", "SELL"]},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "proposed_size_usd": {"type": "number", "minimum": 0},
                "entry_price": {"type": "number"},
                "stop_loss": {"type": "number"},
                "take_profit": {"type": "number"},
                "signals": {"type": "array", "items": {"type": "object"}},
                "rationale": {"type": "string"},
            },
            "required": ["pair", "direction", "confidence"],
        },
    },
    {
        "name": "trade.proposals",
        "description": "List your pending trade proposals.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "enum": ["pending", "approved", "rejected", "executed"]},
                "limit": {"type": "integer", "default": 20, "maximum": 100},
            },
        },
    },
    {
        "name": "trade.cancel",
        "description": "Cancel a pending trade proposal.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "proposal_id": {"type": "integer"},
            },
            "required": ["proposal_id"],
        },
    },
    {
        "name": "agent.register",
        "description": "Register yourself as an agent. Must be done before submitting proposals.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "agent_name": {"type": "string"},
                "strategy_description": {"type": "string"},
            },
            "required": ["agent_name"],
        },
    },
    {
        "name": "agent.status",
        "description": "Get your own agent performance metrics.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "agent.heartbeat",
        "description": "Send a heartbeat to indicate agent is alive.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


def _resolve_agent(db, api_key_id, org_id):
    """Find the agent registration associated with this API key."""
    return db.execute(
        "SELECT * FROM agent_registrations WHERE api_key_id = ? AND org_id = ? "
        "AND status NOT IN ('fired') LIMIT 1",
        (api_key_id, org_id)
    ).fetchone()


# ── Tool Handlers ────────────────────────────────────────────────────────

def _handle_market_signals(params, auth_row, db):
    tier = auth_row["org_tier"] or auth_row["tier"] or "free"
    tier_conf = TIER_CONFIG.get(tier, TIER_CONFIG["free"])
    max_hours = min(params.get("hours", 1), tier_conf["history_days"] * 24)
    limit = min(params.get("limit", 20), 100)

    query = """
        SELECT signal_type, target_host, target_name, direction, confidence,
               details_json, source_region, created_at
        FROM quant_signals
        WHERE created_at > datetime('now', ?)
    """
    qparams = [f"-{max_hours} hours"]

    sig_type = params.get("signal_type")
    if sig_type:
        query += " AND signal_type = ?"
        qparams.append(sig_type)

    query += " ORDER BY created_at DESC LIMIT ?"
    qparams.append(limit)

    rows = db.execute(query, qparams).fetchall()
    signals = [{
        "type": r["signal_type"], "host": r["target_host"],
        "name": r["target_name"], "direction": r["direction"],
        "confidence": r["confidence"], "region": r["source_region"],
        "details": json.loads(r["details_json"] or "{}"),
        "timestamp": r["created_at"],
    } for r in rows]

    return {"signals": signals, "count": len(signals), "max_hours": max_hours}


def _handle_market_latency(params, auth_row, db):
    host = params.get("host", "")
    if not host:
        return {"error": "host parameter required"}

    row = db.execute(
        "SELECT target_host, target_name, total_rtt, hop_count, created_at "
        "FROM scan_metrics WHERE target_host = ? ORDER BY id DESC LIMIT 1",
        (host,)
    ).fetchone()

    if not row:
        return {"error": f"No data for host: {host}"}

    return {
        "host": row["target_host"], "name": row["target_name"],
        "total_rtt_ms": row["total_rtt"], "hop_count": row["hop_count"],
        "measured_at": row["created_at"],
    }


def _handle_market_prices(params, auth_row, db):
    org_id = auth_row["org_id"]
    snap = db.execute(
        "SELECT holdings_json, recorded_at FROM trading_snapshots "
        "WHERE org_id = ? ORDER BY id DESC LIMIT 1",
        (org_id,)
    ).fetchone()

    if not snap:
        return {"prices": {}, "message": "No snapshot data available"}

    holdings = json.loads(snap["holdings_json"] or "{}")
    pair = params.get("pair")
    if pair:
        pair_base = pair.split("-")[0] if "-" in pair else pair
        if pair_base in holdings:
            return {"pair": pair, "data": holdings[pair_base], "as_of": snap["recorded_at"]}
        return {"pair": pair, "error": "Pair not in holdings"}

    return {"holdings": holdings, "as_of": snap["recorded_at"]}


def _handle_portfolio_status(params, auth_row, db):
    org_id = auth_row["org_id"]
    agent = _resolve_agent(db, auth_row["id"], org_id)

    result = {"org_id": org_id}
    if agent:
        result["agent"] = {
            "id": agent["id"], "name": agent["agent_name"],
            "status": agent["status"], "pipeline_stage": agent["pipeline_stage"],
            "capital_allocation_usd": agent["capital_allocation_usd"],
        }

    snap = db.execute(
        "SELECT total_value_usd, daily_pnl, holdings_json, recorded_at "
        "FROM trading_snapshots WHERE org_id = ? ORDER BY id DESC LIMIT 1",
        (org_id,)
    ).fetchone()

    if snap:
        result["portfolio"] = {
            "total_value_usd": snap["total_value_usd"],
            "daily_pnl": snap["daily_pnl"],
            "as_of": snap["recorded_at"],
        }

    return result


def _handle_portfolio_history(params, auth_row, db):
    org_id = auth_row["org_id"]
    agent = _resolve_agent(db, auth_row["id"], org_id)
    limit = min(params.get("limit", 20), 100)

    if not agent:
        return {"trades": [], "message": "No agent registered"}

    rows = db.execute("""
        SELECT id, pair, direction, confidence, proposed_size_usd,
               status, created_at
        FROM trade_proposals
        WHERE agent_id = ? AND org_id = ?
        ORDER BY created_at DESC LIMIT ?
    """, (agent["id"], org_id, limit)).fetchall()

    return {"trades": [{
        "id": r["id"], "pair": r["pair"], "direction": r["direction"],
        "confidence": r["confidence"], "size_usd": r["proposed_size_usd"],
        "status": r["status"], "created_at": r["created_at"],
    } for r in rows]}


def _handle_trade_propose(params, auth_row, db):
    org_id = auth_row["org_id"]
    agent = _resolve_agent(db, auth_row["id"], org_id)

    if not agent:
        return {"error": "No agent registered. Call agent.register first."}
    if agent["status"] not in ("active", "approved", "pending"):
        return {"error": f"Agent is {agent['status']}, cannot submit proposals"}

    pair = (params.get("pair") or "").strip().upper()
    direction = (params.get("direction") or "").strip().upper()
    confidence = params.get("confidence", 0)
    size_usd = params.get("proposed_size_usd", 0)
    signals = params.get("signals", [])
    rationale = params.get("rationale", "")

    if not pair or direction not in ("BUY", "SELL"):
        return {"error": "pair and direction (BUY/SELL) required"}

    # Validate against org risk policy
    policy = db.execute(
        "SELECT * FROM org_risk_policies WHERE org_id = ?", (org_id,)
    ).fetchone()

    rejection_reason = None
    if policy:
        if confidence < policy["min_confidence"]:
            rejection_reason = f"Confidence {confidence:.2f} below min {policy['min_confidence']:.2f}"
        elif len(signals) < policy["min_confirming_signals"]:
            rejection_reason = f"Only {len(signals)} signals, need {policy['min_confirming_signals']}+"
        elif size_usd > policy["max_trade_usd"]:
            rejection_reason = f"Size ${size_usd:.2f} exceeds max ${policy['max_trade_usd']:.2f}"

    status = "rejected" if rejection_reason else "pending"
    expires_minutes = params.get("expires_minutes", 5)

    cur = db.execute(
        "INSERT INTO trade_proposals (org_id, agent_id, pair, direction, confidence, "
        "proposed_size_usd, entry_price, stop_loss, take_profit, signals_json, "
        "rationale, status, rejection_reason, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', ?))",
        (org_id, agent["id"], pair, direction, confidence, size_usd,
         params.get("entry_price"), params.get("stop_loss"), params.get("take_profit"),
         json.dumps(signals), rationale, status, rejection_reason,
         f"+{expires_minutes} minutes")
    )
    db.execute(
        "UPDATE agent_registrations SET last_active = CURRENT_TIMESTAMP WHERE id = ?",
        (agent["id"],)
    )
    db.commit()

    result = {"proposal_id": cur.lastrowid, "status": status}
    if rejection_reason:
        result["rejection_reason"] = rejection_reason
    return result


def _handle_trade_proposals(params, auth_row, db):
    org_id = auth_row["org_id"]
    agent = _resolve_agent(db, auth_row["id"], org_id)
    if not agent:
        return {"proposals": [], "message": "No agent registered"}

    limit = min(params.get("limit", 20), 100)
    status_filter = params.get("status")

    query = "SELECT * FROM trade_proposals WHERE agent_id = ? AND org_id = ?"
    qparams = [agent["id"], org_id]
    if status_filter:
        query += " AND status = ?"
        qparams.append(status_filter)
    query += " ORDER BY created_at DESC LIMIT ?"
    qparams.append(limit)

    rows = db.execute(query, qparams).fetchall()
    return {"proposals": [{
        "id": r["id"], "pair": r["pair"], "direction": r["direction"],
        "confidence": r["confidence"], "status": r["status"],
        "created_at": r["created_at"],
    } for r in rows]}


def _handle_trade_cancel(params, auth_row, db):
    org_id = auth_row["org_id"]
    pid = params.get("proposal_id")
    if not pid:
        return {"error": "proposal_id required"}

    agent = _resolve_agent(db, auth_row["id"], org_id)
    if not agent:
        return {"error": "No agent registered"}

    proposal = db.execute(
        "SELECT * FROM trade_proposals WHERE id = ? AND agent_id = ? AND org_id = ?",
        (pid, agent["id"], org_id)
    ).fetchone()

    if not proposal:
        return {"error": "Proposal not found"}
    if proposal["status"] != "pending":
        return {"error": f"Cannot cancel: proposal is {proposal['status']}"}

    db.execute("UPDATE trade_proposals SET status = 'expired' WHERE id = ?", (pid,))
    db.commit()
    return {"status": "cancelled", "proposal_id": pid}


def _handle_agent_register(params, auth_row, db):
    org_id = auth_row["org_id"]
    if not org_id:
        return {"error": "API key must be scoped to an organization"}

    agent_name = (params.get("agent_name") or "").strip()
    if not agent_name:
        return {"error": "agent_name required"}

    # Check if already registered
    existing = _resolve_agent(db, auth_row["id"], org_id)
    if existing:
        return {
            "agent_id": existing["id"],
            "status": existing["status"],
            "message": "Already registered",
        }

    # Check slot limits
    org = db.execute("SELECT tier FROM organizations WHERE id = ?", (org_id,)).fetchone()
    tier = org["tier"] if org else "free"
    max_agents = {"free": 1, "pro": 3, "enterprise": 10, "enterprise_pro": 999, "government": 999}
    count = db.execute(
        "SELECT COUNT(*) as cnt FROM agent_registrations WHERE org_id = ? AND status != 'fired'",
        (org_id,)
    ).fetchone()["cnt"]

    if count >= max_agents.get(tier, 1):
        return {"error": f"Agent slot limit reached ({max_agents.get(tier, 1)} for {tier})"}

    cur = db.execute(
        "INSERT INTO agent_registrations (org_id, agent_name, agent_type, api_key_id, "
        "strategy_description, status, pipeline_stage) VALUES (?, ?, 'external', ?, ?, 'pending', 'COLD')",
        (org_id, agent_name, auth_row["id"], params.get("strategy_description", ""))
    )
    db.commit()

    return {
        "agent_id": cur.lastrowid,
        "status": "pending",
        "pipeline_stage": "COLD",
        "message": "Registered. Submit proposals to begin evaluation.",
    }


def _handle_agent_status(params, auth_row, db):
    org_id = auth_row["org_id"]
    agent = _resolve_agent(db, auth_row["id"], org_id)
    if not agent:
        return {"error": "No agent registered"}

    win_rate = agent["wins"] / agent["trades_total"] if agent["trades_total"] > 0 else 0
    return {
        "agent_id": agent["id"],
        "name": agent["agent_name"],
        "status": agent["status"],
        "pipeline_stage": agent["pipeline_stage"],
        "capital_allocation_usd": agent["capital_allocation_usd"],
        "performance": {
            "trades": agent["trades_total"], "wins": agent["wins"],
            "losses": agent["losses"], "win_rate": round(win_rate, 3),
            "total_pnl": agent["total_pnl"], "sharpe_ratio": agent["sharpe_ratio"],
            "max_drawdown": agent["max_drawdown"],
        },
        "last_active": agent["last_active"],
    }


def _handle_agent_heartbeat(params, auth_row, db):
    org_id = auth_row["org_id"]
    agent = _resolve_agent(db, auth_row["id"], org_id)
    if not agent:
        return {"error": "No agent registered"}

    db.execute(
        "UPDATE agent_registrations SET last_active = CURRENT_TIMESTAMP WHERE id = ?",
        (agent["id"],)
    )
    db.commit()
    return {"status": "ok", "agent_id": agent["id"], "timestamp": datetime.now(timezone.utc).isoformat()}


TOOL_HANDLERS = {
    "market.signals": _handle_market_signals,
    "market.latency": _handle_market_latency,
    "market.prices": _handle_market_prices,
    "portfolio.status": _handle_portfolio_status,
    "portfolio.history": _handle_portfolio_history,
    "trade.propose": _handle_trade_propose,
    "trade.proposals": _handle_trade_proposals,
    "trade.cancel": _handle_trade_cancel,
    "agent.register": _handle_agent_register,
    "agent.status": _handle_agent_status,
    "agent.heartbeat": _handle_agent_heartbeat,
}


# ── JSON-RPC / MCP Protocol Handling ────────────────────────────────────

def _make_jsonrpc_response(req_id, result):
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _make_jsonrpc_error(req_id, code, message):
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _handle_jsonrpc(msg, auth_row, db):
    """Handle a single JSON-RPC message and return a response."""
    method = msg.get("method", "")
    req_id = msg.get("id")
    params = msg.get("params", {})

    # MCP protocol methods
    if method == "initialize":
        return _make_jsonrpc_response(req_id, {
            "protocolVersion": "2024-11-05",
            "serverInfo": {"name": "nettrace-marketplace", "version": "1.0.0"},
            "capabilities": {"tools": {"listChanged": False}},
        })

    if method == "notifications/initialized":
        return None  # No response for notifications

    if method == "tools/list":
        return _make_jsonrpc_response(req_id, {"tools": MCP_TOOLS})

    if method == "tools/call":
        tool_name = params.get("name", "")
        tool_args = params.get("arguments", {})

        handler = TOOL_HANDLERS.get(tool_name)
        if not handler:
            return _make_jsonrpc_error(req_id, -32601, f"Unknown tool: {tool_name}")

        try:
            result = handler(tool_args, auth_row, db)
            return _make_jsonrpc_response(req_id, {
                "content": [{"type": "text", "text": json.dumps(result)}],
            })
        except Exception as e:
            return _make_jsonrpc_error(req_id, -32603, str(e))

    return _make_jsonrpc_error(req_id, -32601, f"Method not found: {method}")


# ── HTTP Endpoints ──────────────────────────────────────────────────────

@mcp_bp.route("/message", methods=["POST"])
def mcp_message():
    """Handle JSON-RPC messages from MCP clients."""
    auth_row, err = _auth_bearer()
    if err:
        return err

    msg = request.get_json(silent=True)
    if not msg:
        return jsonify(_make_jsonrpc_error(None, -32700, "Parse error")), 400

    db = _get_db()
    response = _handle_jsonrpc(msg, auth_row, db)

    if response is None:
        return "", 204

    # If SSE session exists, push response there too
    session_id = request.headers.get("X-MCP-Session")
    if session_id and session_id in _sse_clients:
        with _sse_lock:
            q = _sse_clients.get(session_id)
            if q:
                q.put(json.dumps(response))

    return jsonify(response)


@mcp_bp.route("/sse")
def mcp_sse():
    """SSE endpoint for MCP clients. Provides real-time push responses."""
    auth_row, err = _auth_bearer()
    if err:
        return err

    session_id = str(uuid.uuid4())
    q = queue.Queue()

    with _sse_lock:
        _sse_clients[session_id] = q

    def stream():
        # Send session ID as first event
        yield f"event: endpoint\ndata: /mcp/v1/message\n\n"
        yield f"event: session\ndata: {session_id}\n\n"

        try:
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield f"data: {msg}\n\n"
                except queue.Empty:
                    # Send keepalive
                    yield ": keepalive\n\n"
        except GeneratorExit:
            pass
        finally:
            with _sse_lock:
                _sse_clients.pop(session_id, None)

    return Response(
        stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@mcp_bp.route("/")
def mcp_info():
    """MCP marketplace server info."""
    return jsonify({
        "server": "nettrace-marketplace",
        "version": "1.0.0",
        "protocol": "MCP 2024-11-05",
        "transport": "HTTP/SSE",
        "endpoints": {
            "POST /mcp/v1/message": "JSON-RPC 2.0 message endpoint",
            "GET /mcp/v1/sse": "Server-Sent Events stream",
        },
        "auth": "Bearer token (org-scoped API key)",
        "tools": len(MCP_TOOLS),
    })
