"""Organization Management API Blueprint for NetTrace multi-tenant platform."""

import json
import os
import re
import sqlite3
from pathlib import Path

from flask import Blueprint, request, jsonify, g

from api_auth import (
    verify_api_key, require_org_access, require_write,
    create_api_key, TIER_CONFIG,
)

api_orgs = Blueprint("api_orgs", __name__, url_prefix="/api/v1")

DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent / "traceroute.db"))


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9\-]{1,48}[a-z0-9]$")


def _valid_slug(slug):
    return bool(_SLUG_RE.match(slug))


# ── POST /api/v1/orgs ────────────────────────────────────────────────────

@api_orgs.route("/orgs", methods=["POST"])
@verify_api_key
@require_write
def create_org():
    """Create a new organization. The API key user becomes the owner."""
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    slug = (data.get("slug") or "").strip().lower()

    if not name or len(name) > 100:
        return jsonify({"error": "name is required (max 100 chars)"}), 400
    if not slug or not _valid_slug(slug):
        return jsonify({"error": "slug must be 3-50 lowercase alphanumeric + hyphens"}), 400

    tier = data.get("tier", "free")
    if tier not in TIER_CONFIG:
        return jsonify({"error": f"Invalid tier. Choose from: {', '.join(TIER_CONFIG)}"}), 400

    db = get_db()

    # Check slug uniqueness
    if db.execute("SELECT 1 FROM organizations WHERE slug = ?", (slug,)).fetchone():
        return jsonify({"error": "Slug already taken"}), 409

    user_id = g.api_user_id
    cur = db.execute(
        "INSERT INTO organizations (name, slug, owner_user_id, tier) VALUES (?, ?, ?, ?)",
        (name, slug, user_id, tier)
    )
    org_id = cur.lastrowid

    # Add owner as member
    db.execute(
        "INSERT INTO org_members (org_id, user_id, role) VALUES (?, ?, 'owner')",
        (org_id, user_id)
    )

    # Create default risk policy
    db.execute(
        "INSERT INTO org_risk_policies (org_id) VALUES (?)", (org_id,)
    )

    db.commit()

    return jsonify({
        "id": org_id,
        "name": name,
        "slug": slug,
        "tier": tier,
        "role": "owner",
    }), 201


# ── GET /api/v1/orgs ─────────────────────────────────────────────────────

@api_orgs.route("/orgs")
@verify_api_key
def list_orgs():
    """List organizations the authenticated user belongs to."""
    db = get_db()
    rows = db.execute("""
        SELECT o.id, o.name, o.slug, o.tier, o.is_active, o.created_at, m.role
        FROM organizations o
        JOIN org_members m ON o.id = m.org_id
        WHERE m.user_id = ?
        ORDER BY o.created_at
    """, (g.api_user_id,)).fetchall()

    return jsonify({
        "organizations": [{
            "id": r["id"], "name": r["name"], "slug": r["slug"],
            "tier": r["tier"], "is_active": bool(r["is_active"]),
            "role": r["role"], "created_at": r["created_at"],
        } for r in rows],
        "count": len(rows),
    })


# ── GET /api/v1/orgs/<slug> ──────────────────────────────────────────────

@api_orgs.route("/orgs/<slug>")
@verify_api_key
@require_org_access()
def get_org(slug):
    """Get organization details."""
    db = get_db()
    org = db.execute(
        "SELECT * FROM organizations WHERE id = ?", (g.org_id,)
    ).fetchone()

    member_count = db.execute(
        "SELECT COUNT(*) as cnt FROM org_members WHERE org_id = ?", (g.org_id,)
    ).fetchone()["cnt"]

    return jsonify({
        "id": org["id"], "name": org["name"], "slug": org["slug"],
        "tier": org["tier"], "is_active": bool(org["is_active"]),
        "settings": json.loads(org["settings_json"] or "{}"),
        "member_count": member_count,
        "your_role": g.org_role,
        "created_at": org["created_at"],
    })


# ── PUT /api/v1/orgs/<slug> ──────────────────────────────────────────────

@api_orgs.route("/orgs/<slug>", methods=["PUT"])
@verify_api_key
@require_write
@require_org_access("owner", "admin")
def update_org(slug):
    """Update organization settings."""
    data = request.get_json(silent=True) or {}
    db = get_db()

    updates = []
    params = []
    if "name" in data:
        name = data["name"].strip()
        if not name or len(name) > 100:
            return jsonify({"error": "name must be 1-100 chars"}), 400
        updates.append("name = ?")
        params.append(name)
    if "settings" in data:
        updates.append("settings_json = ?")
        params.append(json.dumps(data["settings"]))

    if not updates:
        return jsonify({"error": "Nothing to update"}), 400

    params.append(g.org_id)
    db.execute(f"UPDATE organizations SET {', '.join(updates)} WHERE id = ?", params)
    db.commit()

    return jsonify({"status": "updated"})


# ── POST /api/v1/orgs/<slug>/members ─────────────────────────────────────

@api_orgs.route("/orgs/<slug>/members", methods=["POST"])
@verify_api_key
@require_write
@require_org_access("owner", "admin")
def invite_member(slug):
    """Add a member to the organization."""
    data = request.get_json(silent=True) or {}
    user_id = data.get("user_id")
    role = data.get("role", "member")

    if not user_id:
        return jsonify({"error": "user_id required"}), 400
    if role not in ("admin", "member", "viewer", "agent"):
        return jsonify({"error": "role must be admin, member, viewer, or agent"}), 400

    db = get_db()

    # Verify user exists
    if not db.execute("SELECT 1 FROM users WHERE id = ?", (user_id,)).fetchone():
        return jsonify({"error": "User not found"}), 404

    try:
        db.execute(
            "INSERT INTO org_members (org_id, user_id, role) VALUES (?, ?, ?)",
            (g.org_id, user_id, role)
        )
        db.commit()
    except sqlite3.IntegrityError:
        return jsonify({"error": "User is already a member"}), 409

    return jsonify({"status": "added", "user_id": user_id, "role": role}), 201


# ── DELETE /api/v1/orgs/<slug>/members/<uid> ─────────────────────────────

@api_orgs.route("/orgs/<slug>/members/<int:uid>", methods=["DELETE"])
@verify_api_key
@require_write
@require_org_access("owner", "admin")
def remove_member(slug, uid):
    """Remove a member from the organization."""
    db = get_db()

    # Can't remove the owner
    member = db.execute(
        "SELECT role FROM org_members WHERE org_id = ? AND user_id = ?",
        (g.org_id, uid)
    ).fetchone()
    if not member:
        return jsonify({"error": "Member not found"}), 404
    if member["role"] == "owner":
        return jsonify({"error": "Cannot remove the org owner"}), 403

    db.execute(
        "DELETE FROM org_members WHERE org_id = ? AND user_id = ?",
        (g.org_id, uid)
    )
    db.commit()

    return jsonify({"status": "removed", "user_id": uid})


# ── POST /api/v1/orgs/<slug>/credentials ─────────────────────────────────

@api_orgs.route("/orgs/<slug>/credentials", methods=["POST"])
@verify_api_key
@require_write
@require_org_access("owner", "admin")
def store_credentials(slug):
    """Store exchange API keys (encrypted) for the organization."""
    data = request.get_json(silent=True) or {}
    exchange = (data.get("exchange") or "").strip().lower()
    credential_data = data.get("credential_data")

    if not exchange or not credential_data:
        return jsonify({"error": "exchange and credential_data required"}), 400

    db = get_db()
    # Upsert: replace existing credentials for this org/exchange
    db.execute(
        "INSERT OR REPLACE INTO user_credentials (user_id, exchange, credential_data, org_id) "
        "VALUES (?, ?, ?, ?)",
        (g.api_user_id, exchange, json.dumps(credential_data), g.org_id)
    )
    db.commit()

    return jsonify({"status": "stored", "exchange": exchange}), 201


# ── POST /api/v1/orgs/<slug>/api-keys ────────────────────────────────────

@api_orgs.route("/orgs/<slug>/api-keys", methods=["POST"])
@verify_api_key
@require_write
@require_org_access("owner", "admin")
def create_org_api_key(slug):
    """Create an API key scoped to this organization."""
    data = request.get_json(silent=True) or {}
    key_name = (data.get("name") or "default").strip()[:50]
    read_only = bool(data.get("read_only", False))

    db = get_db()
    org = db.execute("SELECT tier FROM organizations WHERE id = ?", (g.org_id,)).fetchone()
    tier = org["tier"] if org else "free"

    raw_key, key_id = create_api_key(
        g.api_user_id, key_name, tier=tier, read_only=read_only, org_id=g.org_id
    )

    return jsonify({
        "key": raw_key,
        "key_id": key_id,
        "name": key_name,
        "tier": tier,
        "org_slug": slug,
        "read_only": read_only,
        "warning": "Store this key securely — it won't be shown again.",
    }), 201


# ── GET /api/v1/orgs/<slug>/portfolio ────────────────────────────────────

@api_orgs.route("/orgs/<slug>/portfolio")
@verify_api_key
@require_org_access()
def get_portfolio(slug):
    """Get latest portfolio snapshot for the organization."""
    db = get_db()
    row = db.execute(
        "SELECT * FROM trading_snapshots WHERE org_id = ? ORDER BY recorded_at DESC LIMIT 1",
        (g.org_id,)
    ).fetchone()

    if not row:
        return jsonify({"portfolio": None, "message": "No snapshots yet"})

    return jsonify({
        "portfolio": {
            "total_value_usd": row["total_value_usd"],
            "daily_pnl": row["daily_pnl"],
            "trades_today": row["trades_today"],
            "trades_total": row["trades_total"],
            "holdings": json.loads(row["holdings_json"] or "{}"),
            "recorded_at": row["recorded_at"],
        }
    })


# ── GET /api/v1/orgs/<slug>/pnl ─────────────────────────────────────────

@api_orgs.route("/orgs/<slug>/pnl")
@verify_api_key
@require_org_access()
def get_pnl(slug):
    """Get P&L summary for the organization."""
    db = get_db()
    days = int(request.args.get("days", 30))
    days = min(max(days, 1), 365)

    rows = db.execute("""
        SELECT total_value_usd, daily_pnl, trades_today, recorded_at
        FROM trading_snapshots
        WHERE org_id = ? AND recorded_at > datetime('now', ?)
        ORDER BY recorded_at
    """, (g.org_id, f"-{days} days")).fetchall()

    if not rows:
        return jsonify({"pnl": [], "summary": None})

    data = [{
        "total_value_usd": r["total_value_usd"],
        "daily_pnl": r["daily_pnl"],
        "trades": r["trades_today"],
        "recorded_at": r["recorded_at"],
    } for r in rows]

    total_pnl = sum(r["daily_pnl"] or 0 for r in rows)
    return jsonify({
        "pnl": data,
        "summary": {
            "total_pnl": round(total_pnl, 2),
            "data_points": len(data),
            "period_days": days,
            "latest_value": rows[-1]["total_value_usd"],
        }
    })


# ── GET /api/v1/orgs/<slug>/risk-policy ──────────────────────────────────

@api_orgs.route("/orgs/<slug>/risk-policy")
@verify_api_key
@require_org_access()
def get_risk_policy(slug):
    """Get risk policy for the organization."""
    db = get_db()
    row = db.execute(
        "SELECT * FROM org_risk_policies WHERE org_id = ?", (g.org_id,)
    ).fetchone()

    if not row:
        return jsonify({"risk_policy": None})

    return jsonify({
        "risk_policy": {
            "risk_profile": row["risk_profile"],
            "max_daily_loss_pct": row["max_daily_loss_pct"],
            "max_position_pct": row["max_position_pct"],
            "max_trade_usd": row["max_trade_usd"],
            "min_confidence": row["min_confidence"],
            "min_confirming_signals": row["min_confirming_signals"],
            "allowed_pairs": json.loads(row["allowed_pairs_json"] or "[]"),
        }
    })


# ── PUT /api/v1/orgs/<slug>/risk-policy ──────────────────────────────────

RISK_PRESETS = {
    "conservative": {
        "max_daily_loss_pct": 1.0, "max_position_pct": 3.0,
        "max_trade_usd": 500.0, "min_confidence": 0.80,
        "min_confirming_signals": 3,
    },
    "moderate": {
        "max_daily_loss_pct": 3.0, "max_position_pct": 5.0,
        "max_trade_usd": 1000.0, "min_confidence": 0.70,
        "min_confirming_signals": 2,
    },
    "aggressive": {
        "max_daily_loss_pct": 5.0, "max_position_pct": 10.0,
        "max_trade_usd": 5000.0, "min_confidence": 0.65,
        "min_confirming_signals": 2,
    },
}


@api_orgs.route("/orgs/<slug>/risk-policy", methods=["PUT"])
@verify_api_key
@require_write
@require_org_access("owner", "admin")
def update_risk_policy(slug):
    """Update risk policy for the organization."""
    data = request.get_json(silent=True) or {}
    db = get_db()

    # If a preset is specified, merge its values
    profile = data.get("risk_profile")
    if profile and profile in RISK_PRESETS:
        preset = RISK_PRESETS[profile]
        for k, v in preset.items():
            data.setdefault(k, v)

    updates = []
    params = []
    field_map = {
        "risk_profile": "risk_profile",
        "max_daily_loss_pct": "max_daily_loss_pct",
        "max_position_pct": "max_position_pct",
        "max_trade_usd": "max_trade_usd",
        "min_confidence": "min_confidence",
        "min_confirming_signals": "min_confirming_signals",
        "allowed_pairs": "allowed_pairs_json",
    }

    for key, col in field_map.items():
        if key in data:
            val = data[key]
            if col == "allowed_pairs_json":
                val = json.dumps(val)
            updates.append(f"{col} = ?")
            params.append(val)

    if not updates:
        return jsonify({"error": "Nothing to update"}), 400

    updates.append("updated_at = CURRENT_TIMESTAMP")
    params.append(g.org_id)
    db.execute(
        f"UPDATE org_risk_policies SET {', '.join(updates)} WHERE org_id = ?",
        params
    )
    db.commit()

    return jsonify({"status": "updated", "risk_profile": profile or "custom"})


# ── GET /api/v1/orgs/<slug>/aum ──────────────────────────────────────────

@api_orgs.route("/orgs/<slug>/aum")
@verify_api_key
@require_org_access()
def get_aum(slug):
    """Get current AUM and high water mark."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent / "agents"))
    from fee_engine import FeeEngine

    engine = FeeEngine()
    history = engine.get_aum_history(g.org_id, days=int(request.args.get("days", 30)))

    return jsonify({
        "org_id": g.org_id,
        "history": history,
        "current": history[-1] if history else None,
        "data_points": len(history),
    })


# ── GET /api/v1/orgs/<slug>/fees ─────────────────────────────────────────

@api_orgs.route("/orgs/<slug>/fees")
@verify_api_key
@require_org_access()
def get_fees(slug):
    """Get fee invoice history."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent / "agents"))
    from fee_engine import FeeEngine

    engine = FeeEngine()
    invoices = engine.get_invoices(g.org_id, limit=int(request.args.get("limit", 12)))

    return jsonify({"invoices": invoices, "count": len(invoices)})


# ── POST /api/v1/orgs/<slug>/fees/preview ────────────────────────────────

@api_orgs.route("/orgs/<slug>/fees/preview", methods=["POST"])
@verify_api_key
@require_org_access()
def preview_fees(slug):
    """Preview the next fee calculation without creating an invoice."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent / "agents"))
    from fee_engine import FeeEngine

    engine = FeeEngine()
    preview = engine.preview_fee(g.org_id)

    return jsonify(preview)
