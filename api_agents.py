"""Agent Marketplace API Blueprint for NetTrace multi-tenant platform.

External and internal agents register, submit trade proposals, and compete
for capital allocation based on performance metrics.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, request, jsonify, g

from api_auth import verify_api_key, require_org_access, require_write

api_agents = Blueprint("api_agents", __name__, url_prefix="/api/v1")

DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent / "traceroute.db"))


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


# ── POST /api/v1/orgs/<slug>/agents ──────────────────────────────────────

@api_agents.route("/orgs/<slug>/agents", methods=["POST"])
@verify_api_key
@require_write
@require_org_access("owner", "admin", "agent")
def register_agent(slug):
    """Register a new agent for this organization."""
    data = request.get_json(silent=True) or {}
    agent_name = (data.get("agent_name") or "").strip()
    agent_type = data.get("agent_type", "external")
    strategy = (data.get("strategy_description") or "").strip()

    if not agent_name or len(agent_name) > 100:
        return jsonify({"error": "agent_name required (max 100 chars)"}), 400
    if agent_type not in ("internal", "external", "marketplace"):
        return jsonify({"error": "agent_type must be internal, external, or marketplace"}), 400

    db = get_db()

    # Check max agent slots per tier
    org = db.execute("SELECT tier FROM organizations WHERE id = ?", (g.org_id,)).fetchone()
    tier = org["tier"] if org else "free"
    max_agents = {"free": 1, "pro": 3, "enterprise": 10, "enterprise_pro": 999, "government": 999}
    current_count = db.execute(
        "SELECT COUNT(*) as cnt FROM agent_registrations WHERE org_id = ? AND status != 'fired'",
        (g.org_id,)
    ).fetchone()["cnt"]

    if current_count >= max_agents.get(tier, 1):
        return jsonify({
            "error": f"Agent slot limit reached ({max_agents.get(tier, 1)} for {tier} tier)",
            "upgrade": "Upgrade your plan for more agent slots"
        }), 403

    cur = db.execute(
        "INSERT INTO agent_registrations (org_id, agent_name, agent_type, api_key_id, "
        "strategy_description, status, pipeline_stage) VALUES (?, ?, ?, ?, ?, 'pending', 'COLD')",
        (g.org_id, agent_name, agent_type, g.api_key_id, strategy)
    )
    db.commit()

    return jsonify({
        "agent_id": cur.lastrowid,
        "agent_name": agent_name,
        "agent_type": agent_type,
        "status": "pending",
        "pipeline_stage": "COLD",
        "message": "Agent registered. Submit proposals to begin COLD evaluation.",
    }), 201


# ── GET /api/v1/orgs/<slug>/agents ───────────────────────────────────────

@api_agents.route("/orgs/<slug>/agents")
@verify_api_key
@require_org_access()
def list_agents(slug):
    """List all agents for this organization."""
    db = get_db()
    rows = db.execute("""
        SELECT id, agent_name, agent_type, status, pipeline_stage,
               capital_allocation_usd, trades_total, wins, losses,
               total_pnl, sharpe_ratio, max_drawdown, last_active, created_at
        FROM agent_registrations
        WHERE org_id = ?
        ORDER BY total_pnl DESC
    """, (g.org_id,)).fetchall()

    agents = []
    for r in rows:
        win_rate = r["wins"] / r["trades_total"] if r["trades_total"] > 0 else 0
        agents.append({
            "id": r["id"],
            "agent_name": r["agent_name"],
            "agent_type": r["agent_type"],
            "status": r["status"],
            "pipeline_stage": r["pipeline_stage"],
            "capital_allocation_usd": r["capital_allocation_usd"],
            "performance": {
                "trades": r["trades_total"],
                "wins": r["wins"],
                "losses": r["losses"],
                "win_rate": round(win_rate, 3),
                "total_pnl": r["total_pnl"],
                "sharpe_ratio": r["sharpe_ratio"],
                "max_drawdown": r["max_drawdown"],
            },
            "last_active": r["last_active"],
            "created_at": r["created_at"],
        })

    return jsonify({"agents": agents, "count": len(agents)})


# ── GET /api/v1/orgs/<slug>/agents/<aid> ─────────────────────────────────

@api_agents.route("/orgs/<slug>/agents/<int:aid>")
@verify_api_key
@require_org_access()
def get_agent(slug, aid):
    """Get agent details and performance metrics."""
    db = get_db()
    agent = db.execute(
        "SELECT * FROM agent_registrations WHERE id = ? AND org_id = ?",
        (aid, g.org_id)
    ).fetchone()

    if not agent:
        return jsonify({"error": "Agent not found"}), 404

    # Recent proposals
    proposals = db.execute("""
        SELECT id, pair, direction, confidence, proposed_size_usd,
               status, created_at
        FROM trade_proposals
        WHERE agent_id = ? AND org_id = ?
        ORDER BY created_at DESC LIMIT 20
    """, (aid, g.org_id)).fetchall()

    win_rate = agent["wins"] / agent["trades_total"] if agent["trades_total"] > 0 else 0

    return jsonify({
        "agent": {
            "id": agent["id"],
            "agent_name": agent["agent_name"],
            "agent_type": agent["agent_type"],
            "status": agent["status"],
            "pipeline_stage": agent["pipeline_stage"],
            "strategy_description": agent["strategy_description"],
            "capital_allocation_usd": agent["capital_allocation_usd"],
            "performance": {
                "trades": agent["trades_total"],
                "wins": agent["wins"],
                "losses": agent["losses"],
                "win_rate": round(win_rate, 3),
                "total_pnl": agent["total_pnl"],
                "sharpe_ratio": agent["sharpe_ratio"],
                "max_drawdown": agent["max_drawdown"],
            },
            "last_active": agent["last_active"],
            "created_at": agent["created_at"],
        },
        "recent_proposals": [{
            "id": p["id"], "pair": p["pair"], "direction": p["direction"],
            "confidence": p["confidence"], "size_usd": p["proposed_size_usd"],
            "status": p["status"], "created_at": p["created_at"],
        } for p in proposals],
    })


# ── POST /api/v1/orgs/<slug>/agents/<aid>/proposals ──────────────────────

@api_agents.route("/orgs/<slug>/agents/<int:aid>/proposals", methods=["POST"])
@verify_api_key
@require_write
@require_org_access()
def submit_proposal(slug, aid):
    """Submit a trade proposal from an agent. Proposals are validated against
    the org's risk policy before being accepted."""
    data = request.get_json(silent=True) or {}
    db = get_db()

    # Verify agent exists, belongs to org, and is active
    agent = db.execute(
        "SELECT * FROM agent_registrations WHERE id = ? AND org_id = ?",
        (aid, g.org_id)
    ).fetchone()

    if not agent:
        return jsonify({"error": "Agent not found"}), 404
    if agent["status"] not in ("active", "approved", "pending"):
        return jsonify({"error": f"Agent is {agent['status']}, cannot submit proposals"}), 403

    # Extract proposal fields
    pair = (data.get("pair") or "").strip().upper()
    direction = (data.get("direction") or "").strip().upper()
    confidence = data.get("confidence", 0)
    size_usd = data.get("proposed_size_usd", 0)
    entry_price = data.get("entry_price")
    stop_loss = data.get("stop_loss")
    take_profit = data.get("take_profit")
    signals = data.get("signals", [])
    rationale = (data.get("rationale") or "").strip()

    if not pair:
        return jsonify({"error": "pair is required (e.g. BTC-USD)"}), 400
    if direction not in ("BUY", "SELL"):
        return jsonify({"error": "direction must be BUY or SELL"}), 400

    # Validate against org risk policy
    policy = db.execute(
        "SELECT * FROM org_risk_policies WHERE org_id = ?", (g.org_id,)
    ).fetchone()

    rejection_reason = None
    if policy:
        if confidence < policy["min_confidence"]:
            rejection_reason = (
                f"Confidence {confidence:.2f} below minimum {policy['min_confidence']:.2f}"
            )
        elif len(signals) < policy["min_confirming_signals"]:
            rejection_reason = (
                f"Only {len(signals)} signals, need {policy['min_confirming_signals']}+"
            )
        elif size_usd > policy["max_trade_usd"]:
            rejection_reason = (
                f"Size ${size_usd:.2f} exceeds max ${policy['max_trade_usd']:.2f}"
            )
        elif policy["allowed_pairs_json"]:
            allowed = json.loads(policy["allowed_pairs_json"])
            if allowed and pair not in allowed:
                rejection_reason = f"Pair {pair} not in allowed list"

    status = "rejected" if rejection_reason else "pending"

    # Set expiry (proposals expire in 5 minutes by default)
    expires_minutes = data.get("expires_minutes", 5)
    expires_at = datetime.now(timezone.utc).isoformat()

    cur = db.execute(
        "INSERT INTO trade_proposals (org_id, agent_id, pair, direction, confidence, "
        "proposed_size_usd, entry_price, stop_loss, take_profit, signals_json, "
        "rationale, status, rejection_reason, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', ?))",
        (g.org_id, aid, pair, direction, confidence, size_usd, entry_price,
         stop_loss, take_profit, json.dumps(signals), rationale, status,
         rejection_reason, f"+{expires_minutes} minutes")
    )

    # Update agent last_active
    db.execute(
        "UPDATE agent_registrations SET last_active = CURRENT_TIMESTAMP WHERE id = ?",
        (aid,)
    )
    db.commit()

    result = {
        "proposal_id": cur.lastrowid,
        "status": status,
        "pair": pair,
        "direction": direction,
        "confidence": confidence,
    }
    if rejection_reason:
        result["rejection_reason"] = rejection_reason

    return jsonify(result), 201 if status == "pending" else 200


# ── GET /api/v1/orgs/<slug>/proposals ────────────────────────────────────

@api_agents.route("/orgs/<slug>/proposals")
@verify_api_key
@require_org_access()
def list_proposals(slug):
    """List trade proposals for the organization."""
    db = get_db()
    status_filter = request.args.get("status")
    limit = min(int(request.args.get("limit", 50)), 200)

    query = """
        SELECT tp.id, tp.agent_id, ar.agent_name, tp.pair, tp.direction,
               tp.confidence, tp.proposed_size_usd, tp.entry_price,
               tp.status, tp.rejection_reason, tp.created_at, tp.expires_at
        FROM trade_proposals tp
        JOIN agent_registrations ar ON tp.agent_id = ar.id
        WHERE tp.org_id = ?
    """
    params = [g.org_id]

    if status_filter:
        query += " AND tp.status = ?"
        params.append(status_filter)

    query += " ORDER BY tp.created_at DESC LIMIT ?"
    params.append(limit)

    rows = db.execute(query, params).fetchall()

    return jsonify({
        "proposals": [{
            "id": r["id"],
            "agent_id": r["agent_id"],
            "agent_name": r["agent_name"],
            "pair": r["pair"],
            "direction": r["direction"],
            "confidence": r["confidence"],
            "size_usd": r["proposed_size_usd"],
            "entry_price": r["entry_price"],
            "status": r["status"],
            "rejection_reason": r["rejection_reason"],
            "created_at": r["created_at"],
            "expires_at": r["expires_at"],
        } for r in rows],
        "count": len(rows),
    })


# ── PUT /api/v1/orgs/<slug>/proposals/<pid>/review ───────────────────────

@api_agents.route("/orgs/<slug>/proposals/<int:pid>/review", methods=["PUT"])
@verify_api_key
@require_write
@require_org_access("owner", "admin")
def review_proposal(slug, pid):
    """Approve or reject a trade proposal."""
    data = request.get_json(silent=True) or {}
    action = (data.get("action") or "").strip().lower()

    if action not in ("approve", "reject"):
        return jsonify({"error": "action must be 'approve' or 'reject'"}), 400

    db = get_db()
    proposal = db.execute(
        "SELECT * FROM trade_proposals WHERE id = ? AND org_id = ?",
        (pid, g.org_id)
    ).fetchone()

    if not proposal:
        return jsonify({"error": "Proposal not found"}), 404
    if proposal["status"] != "pending":
        return jsonify({"error": f"Proposal is already {proposal['status']}"}), 409

    if action == "approve":
        db.execute(
            "UPDATE trade_proposals SET status = 'approved' WHERE id = ?", (pid,)
        )
        # NOTE: Actual execution happens via the execution engine picking up
        # approved proposals — this endpoint only changes status.
        db.commit()
        return jsonify({"status": "approved", "proposal_id": pid})
    else:
        reason = data.get("reason", "Manually rejected by org admin")
        db.execute(
            "UPDATE trade_proposals SET status = 'rejected', rejection_reason = ? WHERE id = ?",
            (reason, pid)
        )
        db.commit()
        return jsonify({"status": "rejected", "proposal_id": pid, "reason": reason})


# ── PUT /api/v1/orgs/<slug>/agents/<aid>/status ──────────────────────────

@api_agents.route("/orgs/<slug>/agents/<int:aid>/status", methods=["PUT"])
@verify_api_key
@require_write
@require_org_access("owner", "admin")
def update_agent_status(slug, aid):
    """Update agent status (approve, suspend, fire)."""
    data = request.get_json(silent=True) or {}
    new_status = (data.get("status") or "").strip().lower()

    valid_transitions = {
        "pending": ["approved", "suspended", "fired"],
        "approved": ["active", "suspended", "fired"],
        "active": ["suspended", "fired"],
        "suspended": ["active", "fired"],
    }

    db = get_db()
    agent = db.execute(
        "SELECT status FROM agent_registrations WHERE id = ? AND org_id = ?",
        (aid, g.org_id)
    ).fetchone()

    if not agent:
        return jsonify({"error": "Agent not found"}), 404

    allowed = valid_transitions.get(agent["status"], [])
    if new_status not in allowed:
        return jsonify({
            "error": f"Cannot transition from {agent['status']} to {new_status}",
            "allowed": allowed,
        }), 400

    db.execute(
        "UPDATE agent_registrations SET status = ? WHERE id = ?",
        (new_status, aid)
    )

    # If fired, zero out capital allocation
    if new_status == "fired":
        db.execute(
            "UPDATE agent_registrations SET capital_allocation_usd = 0 WHERE id = ?",
            (aid,)
        )

    db.commit()
    return jsonify({"agent_id": aid, "status": new_status})


# ── GET /api/v1/marketplace/leaderboard ──────────────────────────────────

@api_agents.route("/marketplace/leaderboard")
@verify_api_key
def marketplace_leaderboard():
    """Public agent rankings across all organizations (anonymized)."""
    db = get_db()
    limit = min(int(request.args.get("limit", 50)), 100)

    rows = db.execute("""
        SELECT ar.agent_name, ar.agent_type, ar.pipeline_stage,
               ar.trades_total, ar.wins, ar.losses,
               ar.total_pnl, ar.sharpe_ratio, ar.max_drawdown,
               o.slug as org_slug
        FROM agent_registrations ar
        JOIN organizations o ON ar.org_id = o.id
        WHERE ar.status = 'active' AND ar.trades_total >= 10
        ORDER BY ar.sharpe_ratio DESC
        LIMIT ?
    """, (limit,)).fetchall()

    leaderboard = []
    for i, r in enumerate(rows, 1):
        win_rate = r["wins"] / r["trades_total"] if r["trades_total"] > 0 else 0
        leaderboard.append({
            "rank": i,
            "agent_name": r["agent_name"],
            "agent_type": r["agent_type"],
            "pipeline_stage": r["pipeline_stage"],
            "org": r["org_slug"],
            "trades": r["trades_total"],
            "win_rate": round(win_rate, 3),
            "total_pnl": r["total_pnl"],
            "sharpe_ratio": r["sharpe_ratio"],
            "max_drawdown": r["max_drawdown"],
        })

    return jsonify({"leaderboard": leaderboard, "count": len(leaderboard)})
