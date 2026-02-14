"""API key authentication and rate limiting for NetTrace."""

import hashlib
import os
import secrets
import sqlite3
import time
from functools import wraps

from flask import request, jsonify, g

# Tier configuration
TIER_CONFIG = {
    "free":            {"rate_limit_daily": 100,     "history_days": 1,   "routes": False, "compare": False, "export": False, "ws_stream": False, "dedicated_scanning": False, "sla": False},
    "pro":             {"rate_limit_daily": 10000,   "history_days": 30,  "routes": True,  "compare": True,  "export": False, "ws_stream": False, "dedicated_scanning": False, "sla": False},
    "enterprise":      {"rate_limit_daily": 999999,  "history_days": 90,  "routes": True,  "compare": True,  "export": True,  "ws_stream": True,  "dedicated_scanning": False, "sla": False},
    "enterprise_pro":  {"rate_limit_daily": 9999999, "history_days": 365, "routes": True,  "compare": True,  "export": True,  "ws_stream": True,  "dedicated_scanning": True,  "sla": True},
    "government":      {"rate_limit_daily": 9999999, "history_days": 730, "routes": True,  "compare": True,  "export": True,  "ws_stream": True,  "dedicated_scanning": True,  "sla": True},
}

ALLOW_QUERY_API_KEY_AUTH = (
    str(os.environ.get("ALLOW_QUERY_API_KEY_AUTH", "0")).strip().lower()
    not in {"0", "false", "no", ""}
)


def generate_api_key():
    """Generate an API key with prefix nt_, return (raw_key, key_hash, key_prefix)."""
    raw = "nt_" + secrets.token_urlsafe(32)
    key_hash = hashlib.sha256(raw.encode()).hexdigest()
    key_prefix = raw[:10]
    return raw, key_hash, key_prefix


def hash_api_key(raw_key):
    """Hash a raw API key for lookup."""
    return hashlib.sha256(raw_key.encode()).hexdigest()


def verify_api_key(f):
    """Decorator: extract and verify API key from request, attach tier info to g."""
    @wraps(f)
    def decorated(*args, **kwargs):
        # Check Authorization header first, then X-Api-Key, then optional query param.
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            raw_key = auth_header[7:].strip()
        else:
            raw_key = (request.headers.get("X-Api-Key") or "").strip()
            if not raw_key and ALLOW_QUERY_API_KEY_AUTH:
                raw_key = request.args.get("api_key", "").strip()

        if not raw_key:
            return jsonify({"error": "API key required. Pass via Authorization: Bearer <key> or X-Api-Key header.",
                            "docs": "/api/v1/"}), 401

        key_hash = hash_api_key(raw_key)
        db = _get_db()
        row = db.execute(
            "SELECT id, user_id, tier, rate_limit_daily, is_active, "
            "COALESCE(read_only, 0) as read_only, org_id FROM api_keys WHERE key_hash = ?",
            (key_hash,)
        ).fetchone()

        if not row:
            return jsonify({"error": "Invalid API key"}), 401
        if not row["is_active"]:
            return jsonify({"error": "API key has been deactivated"}), 401

        # Rate limit check
        api_key_id = row["id"]
        rate_limit = row["rate_limit_daily"]
        today_start = int(time.time()) - (int(time.time()) % 86400)
        usage_count = db.execute(
            "SELECT COUNT(*) as cnt FROM api_usage WHERE api_key_id = ? AND created_at > datetime(?, 'unixepoch')",
            (api_key_id, today_start)
        ).fetchone()["cnt"]

        if usage_count >= rate_limit:
            tier = row["tier"]
            upgrade_msg = ""
            if tier == "free":
                upgrade_msg = "Upgrade to Pro ($249/mo) for 10,000 calls/day."
            elif tier == "pro":
                upgrade_msg = "Upgrade to Enterprise ($2,499/mo) for unlimited API access."
            elif tier == "enterprise":
                upgrade_msg = "Upgrade to Enterprise Pro ($50,000/mo) for dedicated scanning, 365d history, and SLA."
            return jsonify({
                "error": "Rate limit exceeded",
                "limit": rate_limit,
                "used": usage_count,
                "resets": "daily at UTC midnight",
                "upgrade": upgrade_msg
            }), 429

        # Log usage
        endpoint = request.path
        db.execute("INSERT INTO api_usage (api_key_id, endpoint) VALUES (?, ?)",
                   (api_key_id, endpoint))
        # Update last_used_at
        db.execute("UPDATE api_keys SET last_used_at = CURRENT_TIMESTAMP WHERE id = ?",
                   (api_key_id,))
        db.commit()

        # Attach to request context
        g.api_key_id = api_key_id
        g.api_user_id = row["user_id"]
        g.api_tier = row["tier"]
        g.api_rate_limit = rate_limit
        g.api_usage_today = usage_count + 1
        g.api_read_only = bool(row["read_only"])
        g.tier_config = TIER_CONFIG.get(row["tier"], TIER_CONFIG["free"])

        # Multi-tenant: resolve org_id from API key
        try:
            g.api_org_id = row["org_id"]
        except (IndexError, KeyError):
            g.api_org_id = None

        # Read-only keys: block all write operations (POST/PUT/DELETE)
        # except explicitly allowlisted read-safe POST endpoints
        if g.api_read_only and request.method in ("POST", "PUT", "DELETE"):
            return jsonify({
                "error": "This API key is read-only",
                "method": request.method,
                "hint": "Use a full-access key for write operations"
            }), 403

        return f(*args, **kwargs)
    return decorated


def require_tier(*allowed_tiers):
    """Decorator: require specific tier(s) for an endpoint."""
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if g.api_tier not in allowed_tiers:
                tier_names = ", ".join(allowed_tiers)
                return jsonify({
                    "error": f"This endpoint requires {tier_names} tier",
                    "your_tier": g.api_tier,
                    "upgrade": "Visit https://nettrace-dashboard.fly.dev to upgrade"
                }), 403
            return f(*args, **kwargs)
        return decorated
    return decorator


def require_write(f):
    """Decorator: block read-only API keys from write endpoints."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if getattr(g, "api_read_only", False):
            return jsonify({
                "error": "This API key is read-only",
                "endpoint": request.path,
                "hint": "Use a full-access key for write operations"
            }), 403
        return f(*args, **kwargs)
    return decorated


def require_org_access(*roles):
    """Decorator: verify API key's org matches the requested resource slug.

    Usage: @require_org_access("owner", "admin") on a route with <slug> param.
    If no roles specified, any org member role is accepted.
    """
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            slug = kwargs.get("slug")
            if not slug:
                return jsonify({"error": "Missing org slug"}), 400

            db = _get_db()
            org = db.execute(
                "SELECT id, tier, is_active FROM organizations WHERE slug = ?", (slug,)
            ).fetchone()
            if not org:
                return jsonify({"error": "Organization not found"}), 404
            if not org["is_active"]:
                return jsonify({"error": "Organization is deactivated"}), 403

            org_id = org["id"]

            # Check API key belongs to this org
            if getattr(g, "api_org_id", None) != org_id:
                # Fall back: check if the user is a member of this org
                membership = db.execute(
                    "SELECT role FROM org_members WHERE org_id = ? AND user_id = ?",
                    (org_id, g.api_user_id)
                ).fetchone()
                if not membership:
                    return jsonify({"error": "Access denied to this organization"}), 403
                member_role = membership["role"]
            else:
                membership = db.execute(
                    "SELECT role FROM org_members WHERE org_id = ? AND user_id = ?",
                    (org_id, g.api_user_id)
                ).fetchone()
                member_role = membership["role"] if membership else "member"

            if roles and member_role not in roles:
                return jsonify({
                    "error": f"Requires role: {', '.join(roles)}",
                    "your_role": member_role
                }), 403

            g.org_id = org_id
            g.org_tier = org["tier"]
            g.org_role = member_role
            return f(*args, **kwargs)
        return decorated
    return decorator


def create_api_key(user_id, name, tier="free", read_only=False, org_id=None):
    """Create an API key. Returns (raw_key, key_id) — raw_key shown only once."""
    raw, key_hash, key_prefix = generate_api_key()
    rate_limit = TIER_CONFIG.get(tier, TIER_CONFIG["free"])["rate_limit_daily"]
    db = _get_db()
    cur = db.execute(
        "INSERT INTO api_keys (user_id, key_hash, key_prefix, name, tier, rate_limit_daily, read_only, org_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (user_id, key_hash, key_prefix, name, tier, rate_limit, 1 if read_only else 0, org_id)
    )
    db.commit()
    return raw, cur.lastrowid


def ensure_read_only_column(db):
    """Add read_only column to api_keys if it doesn't exist (safe migration)."""
    try:
        db.execute("SELECT read_only FROM api_keys LIMIT 1")
    except sqlite3.OperationalError:
        db.execute("ALTER TABLE api_keys ADD COLUMN read_only INTEGER DEFAULT 0")
        db.commit()


def _get_db():
    """Get DB connection from Flask g context."""
    if "db" not in g:
        import os
        from pathlib import Path
        DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent / "traceroute.db"))
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db
