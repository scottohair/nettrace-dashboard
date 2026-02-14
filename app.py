#!/usr/bin/env python3
"""NetTrace - Traceroute dashboard with auth, Stripe subscriptions, and self-service scanning."""

import json
import subprocess
import re
import os
import sqlite3
import secrets
import time
import urllib.request
import urllib.parse
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import base64
import hashlib
import hmac

import stripe
from flask import (
    Flask, render_template, request, jsonify, redirect, url_for, g, session
)
from flask_login import (
    LoginManager, UserMixin, login_user, logout_user,
    login_required, current_user
)
from flask_socketio import SocketIO, Namespace, emit, disconnect
from werkzeug.security import generate_password_hash, check_password_hash

BASE_DIR = Path(__file__).parent
DB_PATH = os.environ.get("DB_PATH", str(BASE_DIR / "traceroute.db"))
DEMO_RESULTS = BASE_DIR / "results.json"


def _env_flag(name: str, default: str = "0") -> bool:
    return str(os.environ.get(name, default)).strip().lower() not in {"0", "false", "no", ""}


def _parse_origin_list(raw: str) -> set[str]:
    origins: set[str] = set()
    for part in str(raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        parsed = urllib.parse.urlparse(part)
        if parsed.scheme and parsed.netloc:
            origins.add(f"{parsed.scheme.lower()}://{parsed.netloc.lower()}")
    return origins


def _normalize_request_origin(origin_or_referrer: str) -> str:
    parsed = urllib.parse.urlparse(str(origin_or_referrer or "").strip())
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
    return ""


app = Flask(__name__, template_folder=str(BASE_DIR / "templates"),
            static_folder=str(BASE_DIR / "static"))
_secret_from_env = str(os.environ.get("SECRET_KEY", "")).strip()
app.secret_key = _secret_from_env or secrets.token_hex(32)
MISSING_PERSISTENT_SECRET_KEY = not bool(_secret_from_env)

APP_ENV = str(
    os.environ.get("APP_ENV")
    or os.environ.get("FLASK_ENV")
    or os.environ.get("ENV")
    or "production"
).strip().lower()
IS_PRODUCTION = APP_ENV in {"prod", "production"}

# Session and request hardening (SOC2-style baseline controls).
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = os.environ.get("SESSION_COOKIE_SAMESITE", "Lax")
app.config["SESSION_COOKIE_SECURE"] = _env_flag("SESSION_COOKIE_SECURE", "1" if IS_PRODUCTION else "0")
app.config["REMEMBER_COOKIE_HTTPONLY"] = True
app.config["REMEMBER_COOKIE_SAMESITE"] = app.config["SESSION_COOKIE_SAMESITE"]
app.config["REMEMBER_COOKIE_SECURE"] = app.config["SESSION_COOKIE_SECURE"]
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(
    hours=max(1, int(os.environ.get("SESSION_MAX_AGE_HOURS", "12")))
)
app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("MAX_CONTENT_LENGTH_BYTES", str(4 * 1024 * 1024)))

TRUSTED_ORIGINS = set()
TRUSTED_ORIGINS.add(_normalize_request_origin(os.environ.get("APP_URL", "http://localhost:12034")))
TRUSTED_ORIGINS.update(_parse_origin_list(os.environ.get("TRUSTED_ORIGINS", "")))
TRUSTED_ORIGINS.discard("")

# API key in query params leaks into logs/referrers. Keep off by default.
ALLOW_QUERY_API_KEY_AUTH = _env_flag("ALLOW_QUERY_API_KEY_AUTH", "0")

# Login brute-force controls.
LOGIN_MAX_ATTEMPTS = max(1, int(os.environ.get("LOGIN_MAX_ATTEMPTS", "8")))
LOGIN_WINDOW_SECONDS = max(60, int(os.environ.get("LOGIN_WINDOW_SECONDS", "900")))
LOGIN_BACKOFF_MS = max(0, int(os.environ.get("LOGIN_BACKOFF_MS", "300")))
MFA_CHALLENGE_TTL_SECONDS = max(60, int(os.environ.get("MFA_CHALLENGE_TTL_SECONDS", "300")))
MFA_MAX_VERIFY_ATTEMPTS = max(1, int(os.environ.get("MFA_MAX_VERIFY_ATTEMPTS", "5")))
MFA_TOTP_WINDOW_STEPS = max(0, int(os.environ.get("MFA_TOTP_WINDOW_STEPS", "1")))
MFA_ISSUER = str(os.environ.get("MFA_ISSUER", "NetTrace")).strip() or "NetTrace"
REQUIRE_MFA_FOR_SENSITIVE = _env_flag("REQUIRE_MFA_FOR_SENSITIVE", "1" if IS_PRODUCTION else "0")
REQUIRE_MFA_FOR_USERNAMES = {
    x.strip().lower()
    for x in str(os.environ.get("REQUIRE_MFA_FOR_USERNAMES", "scott")).split(",")
    if x.strip()
}

# Key management controls.
CREDENTIAL_KEY_ENV = "CREDENTIAL_ENCRYPTION_KEY"
CREDENTIAL_KEY_FALLBACKS_ENV = "CREDENTIAL_ENCRYPTION_KEY_FALLBACKS"
ALLOW_LEGACY_XOR_CREDENTIAL_DECRYPT = _env_flag("ALLOW_LEGACY_XOR_CREDENTIAL_DECRYPT", "0")

socketio = SocketIO(app, async_mode="gevent", cors_allowed_origins="*")

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

# Stripe config - all secrets from env vars, nothing hardcoded
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY", "")
STRIPE_PRICE_ID = os.environ.get("STRIPE_PRICE_ID", "")  # Legacy $20/mo price
STRIPE_PRO_PRICE_ID = os.environ.get("STRIPE_PRO_PRICE_ID", "")  # $249/mo Pro
STRIPE_ENTERPRISE_PRICE_ID = os.environ.get("STRIPE_ENTERPRISE_PRICE_ID", "")  # $2,499/mo Enterprise
STRIPE_ENTERPRISE_PRO_PRICE_ID = os.environ.get("STRIPE_ENTERPRISE_PRO_PRICE_ID", "")  # $50,000/mo Enterprise Pro
STRIPE_GOVERNMENT_PRICE_ID = os.environ.get("STRIPE_GOVERNMENT_PRICE_ID", "")  # $500,000/mo Government
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
COINBASE_COMMERCE_API_KEY = os.environ.get("COINBASE_COMMERCE_API_KEY", "")
COINBASE_WEBHOOK_SECRET = (
    os.environ.get("COINBASE_WEBHOOK_SECRET", "")
    or os.environ.get("COINBASE_WEBHOOK_SHARED_SECRET", "")
)
APP_URL = os.environ.get("APP_URL", "http://localhost:12034")
MCP_AGENT_SECRET = os.environ.get("MCP_AGENT_SECRET", "")

# Rate limiting
MAX_SCANS_PER_HOUR = int(os.environ.get("MAX_SCANS_PER_HOUR", "10"))
MAX_CONCURRENT_SCANS = int(os.environ.get("MAX_CONCURRENT_SCANS", "3"))
active_scans = {}
scan_lock = threading.Lock()

GEO_CACHE = {}
GEO_LOCK = threading.Lock()

# ---------------------------------------------------------------------------
# Fly.io Region Guard — only primary region can sign transactions
# ---------------------------------------------------------------------------

PRIMARY_REGION = os.environ.get("PRIMARY_REGION", "ewr")
FLY_REGION = os.environ.get("FLY_REGION", "local")

WALLET_ADDRESS = os.environ.get("WALLET_ADDRESS", "")

# Wallet sub-accounts (logical partitions of one on-chain address)
WALLET_ACCOUNTS = {
    "checking": {"label": "Checking", "purpose": "Active trading capital"},
    "savings": {"label": "Savings", "purpose": "Reserve capital, no auto-trade"},
    "growth": {"label": "Growth", "purpose": "Reinvestment pool (20-35% of profits)"},
    "subsavings": {"label": "Sub-Savings", "purpose": "Long-term hold, manual withdrawal only"},
}


def require_primary_region(f):
    """Decorator: only allow transaction-signing on the primary Fly.io region."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if FLY_REGION != "local" and FLY_REGION != PRIMARY_REGION:
            return jsonify({
                "error": f"Trade execution only available on primary region ({PRIMARY_REGION})",
                "region": FLY_REGION,
                "primary": PRIMARY_REGION,
            }), 403
        return f(*args, **kwargs)
    return decorated

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db:
        db.close()


def _request_uses_api_key_auth() -> bool:
    auth_header = str(request.headers.get("Authorization", "")).strip()
    if auth_header.startswith("Bearer "):
        return True
    if request.headers.get("X-Api-Key"):
        return True
    if ALLOW_QUERY_API_KEY_AUTH and request.args.get("api_key"):
        return True
    return False


@app.before_request
def csrf_origin_guard():
    """Block cross-site state-changing requests for cookie-authenticated sessions."""
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return None

    # External system callbacks are exempt.
    if request.path in {"/api/coinbase-webhook", "/api/stripe-webhook"}:
        return None

    # API key/bearer-auth requests are not cookie-authenticated.
    if _request_uses_api_key_auth():
        return None

    has_session_cookie = bool(request.cookies.get(app.config.get("SESSION_COOKIE_NAME", "session")))
    if not (current_user.is_authenticated or has_session_cookie):
        return None

    origin_header = request.headers.get("Origin") or request.headers.get("Referer")
    normalized = _normalize_request_origin(origin_header or "")
    if not normalized:
        return jsonify({"error": "Missing Origin/Referer for state-changing request"}), 403
    if normalized not in TRUSTED_ORIGINS:
        return jsonify({"error": "Cross-site request blocked", "origin": normalized}), 403
    return None


@app.after_request
def apply_security_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    if IS_PRODUCTION and _env_flag("ENABLE_HSTS", "1"):
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


def init_db():
    db = sqlite3.connect(DB_PATH)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            stripe_customer_id TEXT,
            subscription_status TEXT DEFAULT 'none',
            mfa_enabled INTEGER DEFAULT 0,
            mfa_secret_enc TEXT,
            mfa_secret_pending_enc TEXT,
            mfa_enrolled_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            target_host TEXT NOT NULL,
            target_name TEXT,
            category TEXT DEFAULT 'Custom',
            status TEXT DEFAULT 'pending',
            result_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_scans_user ON scans(user_id);
        CREATE INDEX IF NOT EXISTS idx_scans_created ON scans(created_at);

        -- Phase 1: Data Engine tables
        CREATE TABLE IF NOT EXISTS scan_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_host TEXT NOT NULL,
            target_name TEXT,
            category TEXT,
            total_rtt REAL,
            hop_count INTEGER,
            first_hop_rtt REAL,
            last_hop_rtt REAL,
            route_hash TEXT,
            scan_source TEXT DEFAULT 'auto',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_metrics_host_time ON scan_metrics(target_host, created_at);
        CREATE INDEX IF NOT EXISTS idx_metrics_cat_time ON scan_metrics(category, created_at);

        CREATE TABLE IF NOT EXISTS scan_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_id INTEGER NOT NULL,
            hops_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (metric_id) REFERENCES scan_metrics(id)
        );

        CREATE TABLE IF NOT EXISTS api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            key_hash TEXT UNIQUE NOT NULL,
            key_prefix TEXT NOT NULL,
            name TEXT DEFAULT 'default',
            tier TEXT DEFAULT 'free',
            rate_limit_daily INTEGER DEFAULT 100,
            is_active INTEGER DEFAULT 1,
            last_used_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS api_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_key_id INTEGER NOT NULL,
            endpoint TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
        );
        CREATE INDEX IF NOT EXISTS idx_usage_key_time ON api_usage(api_key_id, created_at);

        CREATE TABLE IF NOT EXISTS route_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_host TEXT NOT NULL,
            target_name TEXT,
            old_route_hash TEXT,
            new_route_hash TEXT,
            new_hops_json TEXT,
            rtt_delta REAL,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_route_changes_host ON route_changes(target_host, detected_at);

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

        CREATE TABLE IF NOT EXISTS ip_geo_cache (
            ip TEXT PRIMARY KEY,
            geo_json TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Trading dashboard: portfolio snapshots pushed from live trader
        CREATE TABLE IF NOT EXISTS trading_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER DEFAULT 1,
            total_value_usd REAL,
            daily_pnl REAL,
            trades_today INTEGER DEFAULT 0,
            trades_total INTEGER DEFAULT 0,
            holdings_json TEXT,
            trades_json TEXT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_trading_snapshots_time ON trading_snapshots(recorded_at);

        -- Per-user exchange/wallet credentials (encrypted at rest)
        CREATE TABLE IF NOT EXISTS user_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            exchange TEXT NOT NULL,
            credential_data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS user_credentials_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            exchange TEXT NOT NULL,
            credential_data TEXT NOT NULL,
            rotated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_user_credentials_user ON user_credentials(user_id);
        CREATE INDEX IF NOT EXISTS idx_user_credentials_hist_user ON user_credentials_history(user_id, exchange, rotated_at);

        -- Stripe Treasury accounts
        CREATE TABLE IF NOT EXISTS treasury_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            stripe_fa_id TEXT,
            balance_cents INTEGER DEFAULT 0,
            yield_earned_cents INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_treasury_user ON treasury_accounts(user_id);

        -- Wallet sub-accounts (logical partitions of one on-chain wallet)
        CREATE TABLE IF NOT EXISTS wallet_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            account_type TEXT NOT NULL,
            balance_usd REAL DEFAULT 0.0,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id),
            UNIQUE(user_id, account_type)
        );
        CREATE INDEX IF NOT EXISTS idx_wallet_accts_user ON wallet_accounts(user_id);

        -- Wallet transfers (between sub-accounts)
        CREATE TABLE IF NOT EXISTS wallet_transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            from_account TEXT NOT NULL,
            to_account TEXT NOT NULL,
            amount_usd REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_wallet_xfers_user ON wallet_transfers(user_id);

        -- Stripe Financial Connections (linked external accounts)
        CREATE TABLE IF NOT EXISTS financial_connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            fc_account_id TEXT,
            institution TEXT,
            account_name TEXT,
            balance_cents INTEGER DEFAULT 0,
            currency TEXT DEFAULT 'usd',
            last_synced_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_fc_user ON financial_connections(user_id);

        -- Asset Pool: unified view of ALL assets across all venues
        CREATE TABLE IF NOT EXISTS asset_pool (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL DEFAULT 1,
            asset TEXT NOT NULL,            -- e.g. ETH, BTC, USDC, SOL
            venue TEXT NOT NULL,            -- coinbase, base, ethereum, arbitrum, polygon, solana, bridge, stuck
            chain TEXT,                     -- blockchain if on-chain
            amount REAL DEFAULT 0.0,
            value_usd REAL DEFAULT 0.0,
            state TEXT DEFAULT 'available', -- available, in_transit, stuck, locked, pending, reserved
            eta_seconds INTEGER,            -- estimated time to available (bridges, confirms)
            tx_hash TEXT,                   -- transaction hash if relevant
            address TEXT,                   -- contract/wallet address
            metadata_json TEXT,             -- JSON blob for deep metadata
            last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_asset_pool_user ON asset_pool(user_id, state);
        CREATE INDEX IF NOT EXISTS idx_asset_pool_venue ON asset_pool(venue, asset);

        -- Asset State Transitions: every change feeds into learning
        CREATE TABLE IF NOT EXISTS asset_state_transitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL DEFAULT 1,
            asset TEXT NOT NULL,
            venue TEXT NOT NULL,
            from_state TEXT NOT NULL,
            to_state TEXT NOT NULL,
            amount REAL,
            value_usd REAL,
            cost_usd REAL DEFAULT 0.0,      -- gas, fees, slippage
            duration_seconds REAL,           -- time spent in previous state
            trigger TEXT,                    -- what caused the transition (agent, bridge, trade, manual)
            tx_hash TEXT,
            metadata_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_ast_user_time ON asset_state_transitions(user_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_ast_asset ON asset_state_transitions(asset, venue);

        -- Login brute-force protection and forensic trail
        CREATE TABLE IF NOT EXISTS auth_login_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            ip_address TEXT NOT NULL,
            success INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_auth_login_attempts_actor
            ON auth_login_attempts(username, ip_address, created_at);

        CREATE TABLE IF NOT EXISTS auth_mfa_challenges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            challenge_hash TEXT NOT NULL UNIQUE,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            ip_address TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            expires_at TIMESTAMP NOT NULL,
            consumed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_auth_mfa_challenges_lookup
            ON auth_mfa_challenges(challenge_hash, expires_at, consumed_at);
        CREATE INDEX IF NOT EXISTS idx_auth_mfa_challenges_user
            ON auth_mfa_challenges(user_id, created_at);

        CREATE TABLE IF NOT EXISTS security_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            user_id INTEGER,
            username TEXT,
            ip_address TEXT,
            request_path TEXT,
            details_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_security_audit_event_time
            ON security_audit_log(event_type, created_at);
        CREATE INDEX IF NOT EXISTS idx_security_audit_user_time
            ON security_audit_log(user_id, created_at);

        -- Multi-tenant: Organizations
        CREATE TABLE IF NOT EXISTS organizations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            slug TEXT UNIQUE NOT NULL,
            owner_user_id INTEGER NOT NULL,
            tier TEXT DEFAULT 'free',
            stripe_customer_id TEXT,
            subscription_status TEXT DEFAULT 'none',
            settings_json TEXT DEFAULT '{}',
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (owner_user_id) REFERENCES users(id)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_organizations_slug ON organizations(slug);

        CREATE TABLE IF NOT EXISTS org_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT DEFAULT 'member',
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (org_id) REFERENCES organizations(id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            UNIQUE(org_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_org_members_user ON org_members(user_id);

        CREATE TABLE IF NOT EXISTS org_risk_policies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id INTEGER NOT NULL UNIQUE,
            risk_profile TEXT DEFAULT 'moderate',
            max_daily_loss_pct REAL DEFAULT 3.0,
            max_position_pct REAL DEFAULT 5.0,
            max_trade_usd REAL DEFAULT 1000.0,
            min_confidence REAL DEFAULT 0.70,
            min_confirming_signals INTEGER DEFAULT 2,
            allowed_pairs_json TEXT DEFAULT '[]',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (org_id) REFERENCES organizations(id)
        );

        -- Agent Marketplace: registrations and trade proposals
        CREATE TABLE IF NOT EXISTS agent_registrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id INTEGER NOT NULL,
            agent_name TEXT NOT NULL,
            agent_type TEXT DEFAULT 'internal',
            api_key_id INTEGER,
            status TEXT DEFAULT 'pending',
            strategy_description TEXT,
            pipeline_stage TEXT DEFAULT 'COLD',
            capital_allocation_usd REAL DEFAULT 0.0,
            trades_total INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            total_pnl REAL DEFAULT 0.0,
            sharpe_ratio REAL DEFAULT 0.0,
            max_drawdown REAL DEFAULT 0.0,
            last_active TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (org_id) REFERENCES organizations(id),
            FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
        );
        CREATE INDEX IF NOT EXISTS idx_agent_registrations_org ON agent_registrations(org_id, status);

        CREATE TABLE IF NOT EXISTS trade_proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id INTEGER NOT NULL,
            agent_id INTEGER NOT NULL,
            pair TEXT NOT NULL,
            direction TEXT NOT NULL,
            confidence REAL NOT NULL,
            proposed_size_usd REAL,
            entry_price REAL,
            stop_loss REAL,
            take_profit REAL,
            signals_json TEXT,
            rationale TEXT,
            status TEXT DEFAULT 'pending',
            rejection_reason TEXT,
            executed_order_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,
            FOREIGN KEY (org_id) REFERENCES organizations(id),
            FOREIGN KEY (agent_id) REFERENCES agent_registrations(id)
        );
        CREATE INDEX IF NOT EXISTS idx_trade_proposals_org ON trade_proposals(org_id, status);
        CREATE INDEX IF NOT EXISTS idx_trade_proposals_agent ON trade_proposals(agent_id, status);

        -- Fee Engine: AUM snapshots and invoices
        CREATE TABLE IF NOT EXISTS org_aum_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id INTEGER NOT NULL,
            total_aum_usd REAL DEFAULT 0.0,
            high_water_mark_usd REAL DEFAULT 0.0,
            gains_above_hwm REAL DEFAULT 0.0,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (org_id) REFERENCES organizations(id)
        );
        CREATE INDEX IF NOT EXISTS idx_org_aum_org_time ON org_aum_snapshots(org_id, recorded_at);

        CREATE TABLE IF NOT EXISTS fee_invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id INTEGER NOT NULL,
            invoice_type TEXT NOT NULL,
            period_start TIMESTAMP,
            period_end TIMESTAMP,
            amount_usd REAL DEFAULT 0.0,
            calculation_json TEXT,
            stripe_invoice_id TEXT,
            status TEXT DEFAULT 'draft',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            paid_at TIMESTAMP,
            FOREIGN KEY (org_id) REFERENCES organizations(id)
        );
        CREATE INDEX IF NOT EXISTS idx_fee_invoices_org ON fee_invoices(org_id, status);
    """)

    # Keep one active credential row per user/exchange, preserve previous rows in history.
    try:
        db.execute("""
            INSERT INTO user_credentials_history (user_id, exchange, credential_data, rotated_at)
            SELECT user_id, exchange, credential_data, created_at
            FROM user_credentials
            WHERE id NOT IN (
                SELECT MAX(id) FROM user_credentials GROUP BY user_id, exchange
            )
        """)
        db.execute("""
            DELETE FROM user_credentials
            WHERE id NOT IN (
                SELECT MAX(id) FROM user_credentials GROUP BY user_id, exchange
            )
        """)
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_user_credentials_user_exchange "
            "ON user_credentials(user_id, exchange)"
        )
        db.commit()
    except Exception:
        db.rollback()

    # Migration: add columns if missing
    migrations = [
        "ALTER TABLE users ADD COLUMN stripe_customer_id TEXT",
        "ALTER TABLE users ADD COLUMN subscription_status TEXT DEFAULT 'none'",
        "ALTER TABLE users ADD COLUMN subscription_expires_at TIMESTAMP",
        "ALTER TABLE users ADD COLUMN payment_method TEXT DEFAULT 'none'",
        "ALTER TABLE users ADD COLUMN tier TEXT DEFAULT 'free'",
        "ALTER TABLE users ADD COLUMN mfa_enabled INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN mfa_secret_enc TEXT",
        "ALTER TABLE users ADD COLUMN mfa_secret_pending_enc TEXT",
        "ALTER TABLE users ADD COLUMN mfa_enrolled_at TIMESTAMP",
        "ALTER TABLE trading_snapshots ADD COLUMN user_id INTEGER DEFAULT 1",
        "CREATE INDEX IF NOT EXISTS idx_trading_snapshots_user ON trading_snapshots(user_id, recorded_at)",
        # Multi-tenant: add org_id to existing tables
        "ALTER TABLE api_keys ADD COLUMN org_id INTEGER REFERENCES organizations(id)",
        "ALTER TABLE trading_snapshots ADD COLUMN org_id INTEGER REFERENCES organizations(id)",
        "ALTER TABLE user_credentials ADD COLUMN org_id INTEGER REFERENCES organizations(id)",
        "ALTER TABLE asset_pool ADD COLUMN org_id INTEGER REFERENCES organizations(id)",
        "CREATE INDEX IF NOT EXISTS idx_api_keys_org ON api_keys(org_id)",
        "CREATE INDEX IF NOT EXISTS idx_trading_snapshots_org ON trading_snapshots(org_id, recorded_at)",
    ]
    for sql in migrations:
        try:
            db.execute(sql)
        except sqlite3.OperationalError:
            pass
    db.commit()

    # Bootstrap Scott's org (org_id=1) — idempotent
    try:
        existing = db.execute("SELECT id FROM organizations WHERE id = 1").fetchone()
        if not existing:
            db.execute(
                "INSERT INTO organizations (id, name, slug, owner_user_id, tier, subscription_status, is_active) "
                "VALUES (1, 'NetTrace Primary', 'nettrace-primary', 2, 'enterprise_pro', 'active', 1)"
            )
            db.execute("INSERT OR IGNORE INTO org_members (org_id, user_id, role) VALUES (1, 2, 'owner')")
            db.execute(
                "INSERT OR IGNORE INTO org_risk_policies (org_id, risk_profile, max_daily_loss_pct, "
                "max_position_pct, max_trade_usd, min_confidence, min_confirming_signals) "
                "VALUES (1, 'aggressive', 5.0, 10.0, 10000.0, 0.70, 2)"
            )
            db.execute("UPDATE api_keys SET org_id = 1 WHERE user_id = 2 AND org_id IS NULL")
            db.execute("UPDATE trading_snapshots SET org_id = 1 WHERE org_id IS NULL")
            db.execute("UPDATE asset_pool SET org_id = 1 WHERE org_id IS NULL")
            db.commit()
    except Exception:
        db.rollback()

    db.close()


# ---------------------------------------------------------------------------
# User model
# ---------------------------------------------------------------------------

class User(UserMixin):
    def __init__(self, id, username, subscription_status="none",
                 stripe_customer_id=None, payment_method="none",
                 subscription_expires_at=None, created_at=None):
        self.id = id
        self.username = username
        self.subscription_status = subscription_status
        self.stripe_customer_id = stripe_customer_id
        self.payment_method = payment_method or "none"
        self.subscription_expires_at = subscription_expires_at
        self.created_at = created_at

    @property
    def is_subscribed(self):
        if self.subscription_status in ("active", "trialing", "past_due"):
            if self.payment_method == "crypto" and self.subscription_expires_at:
                try:
                    exp = datetime.fromisoformat(self.subscription_expires_at)
                    if exp.tzinfo is None:
                        exp = exp.replace(tzinfo=timezone.utc)
                    return datetime.now(timezone.utc) < exp
                except (ValueError, TypeError):
                    return False
            return True
        return False


@login_manager.user_loader
def load_user(user_id):
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    row = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    db.close()
    if row:
        return User(row["id"], row["username"],
                    row["subscription_status"] or "none",
                    row["stripe_customer_id"],
                    row["payment_method"] if "payment_method" in row.keys() else "none",
                    row["subscription_expires_at"] if "subscription_expires_at" in row.keys() else None,
                    row["created_at"] if "created_at" in row.keys() else None)
    return None


def check_and_expire_crypto(user_id):
    """If a crypto user's subscription has expired, flip status to 'expired'."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    row = db.execute("SELECT payment_method, subscription_status, subscription_expires_at FROM users WHERE id=?",
                     (user_id,)).fetchone()
    if row and row["payment_method"] == "crypto" and row["subscription_status"] in ("active",) and row["subscription_expires_at"]:
        try:
            exp = datetime.fromisoformat(row["subscription_expires_at"])
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) >= exp:
                db.execute("UPDATE users SET subscription_status='expired' WHERE id=?", (user_id,))
                db.commit()
        except (ValueError, TypeError):
            pass
    db.close()


def require_subscription(f):
    """Decorator: require active subscription for scanning endpoints."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({"error": "Login required"}), 401
        check_and_expire_crypto(current_user.id)
        if not current_user.is_subscribed:
            return jsonify({"error": "Active subscription required", "needs_subscription": True}), 403
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Traceroute engine
# ---------------------------------------------------------------------------

def run_traceroute(host, max_hops=20):
    try:
        result = subprocess.run(
            ["traceroute", "-m", str(max_hops), "-q", "1", "-w", "2", host],
            capture_output=True, text=True, timeout=60
        )
        output = result.stdout
    except subprocess.TimeoutExpired:
        return {"error": "Traceroute timed out", "hops": []}
    except FileNotFoundError:
        return {"error": "traceroute not installed", "hops": []}

    hops = []
    for line in output.strip().split("\n")[1:]:
        line = line.strip()
        if not line:
            continue
        m = re.match(r'\s*(\d+)\s+(\S+)\s+\((\d+\.\d+\.\d+\.\d+)\)\s+([\d.]+)\s*ms', line)
        if m:
            hops.append({"hop": int(m.group(1)), "host": m.group(2),
                         "ip": m.group(3), "rtt_ms": float(m.group(4))})
        else:
            m2 = re.match(r'\s*(\d+)\s+(\d+\.\d+\.\d+\.\d+)\s+([\d.]+)\s*ms', line)
            if m2:
                hops.append({"hop": int(m2.group(1)), "host": m2.group(2),
                             "ip": m2.group(2), "rtt_ms": float(m2.group(3))})
            else:
                m3 = re.match(r'\s*(\d+)\s+\*', line)
                if m3:
                    hops.append({"hop": int(m3.group(1)), "host": "*",
                                 "ip": None, "rtt_ms": None})
    return {"hops": hops}


def geolocate_ip(ip):
    if not ip:
        return None
    for prefix in ("10.", "192.168.", "172.16.", "172.17.", "172.18.",
                    "172.19.", "172.2", "172.30.", "172.31.", "127."):
        if ip.startswith(prefix):
            return None
    with GEO_LOCK:
        if ip in GEO_CACHE:
            return GEO_CACHE[ip]
    try:
        url = f"http://ip-api.com/json/{ip}?fields=status,country,regionName,city,lat,lon,isp,org,as"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        if data.get("status") == "success":
            geo = {
                "lat": data["lat"], "lon": data["lon"],
                "city": data.get("city", ""), "region": data.get("regionName", ""),
                "country": data.get("country", ""),
                "isp": data.get("isp", ""), "org": data.get("org", ""),
                "as": data.get("as", "")
            }
            with GEO_LOCK:
                GEO_CACHE[ip] = geo
            return geo
    except Exception:
        pass
    return None


def sanitize_hops(hops, sanitize_first_n=3):
    for h in hops:
        if h["hop"] <= sanitize_first_n and h["ip"]:
            h["ip"] = f"10.0.0.{h['hop']}"
            h["host"] = f"hop-{h['hop']}.local"
            h["geo"] = None
    return hops


def execute_scan(scan_id, host, user_id, sid=None):
    def _run():
        db = sqlite3.connect(DB_PATH)
        try:
            db.execute("UPDATE scans SET status='running' WHERE id=?", (scan_id,))
            db.commit()
            if sid:
                socketio.emit("scan_status", {"scan_id": scan_id, "status": "running",
                              "message": f"Tracing route to {host}..."}, to=sid)

            tr = run_traceroute(host)
            hops = tr["hops"]
            for i, h in enumerate(hops):
                if h["ip"]:
                    h["geo"] = geolocate_ip(h["ip"])
                    time.sleep(0.15)
                else:
                    h["geo"] = None
                if sid and i % 3 == 0:
                    socketio.emit("scan_progress", {"scan_id": scan_id,
                                  "hops_done": i + 1, "total_hops": len(hops)}, to=sid)

            hops = sanitize_hops(hops)
            total_rtt = None
            for h in reversed(hops):
                if h.get("rtt_ms"):
                    total_rtt = h["rtt_ms"]
                    break

            result = {"host": host, "hop_count": len(hops),
                      "total_rtt": total_rtt, "hops": hops}
            db.execute("UPDATE scans SET status='completed', result_json=? WHERE id=?",
                       (json.dumps(result), scan_id))
            db.commit()
            if sid:
                socketio.emit("scan_complete", {"scan_id": scan_id, "result": result}, to=sid)
        except Exception as e:
            db.execute("UPDATE scans SET status='failed', result_json=? WHERE id=?",
                       (json.dumps({"error": str(e)}), scan_id))
            db.commit()
            if sid:
                socketio.emit("scan_error", {"scan_id": scan_id, "error": str(e)}, to=sid)
        finally:
            db.close()
            with scan_lock:
                active_scans.pop(scan_id, None)

    with scan_lock:
        user_active = sum(1 for s in active_scans.values() if s == user_id)
        if user_active >= MAX_CONCURRENT_SCANS:
            return False
        active_scans[scan_id] = user_id

    threading.Thread(target=_run, daemon=True).start()
    return True


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

_PASSWORD_COMPLEXITY_RE = re.compile(
    r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z0-9]).+$"
)


def _validate_password_strength(password: str) -> str | None:
    min_len = max(8, int(os.environ.get("PASSWORD_MIN_LENGTH", "12")))
    if len(password) < min_len:
        return f"Password must be at least {min_len} characters"
    if not _PASSWORD_COMPLEXITY_RE.match(password):
        return "Password must include upper, lower, number, and symbol"
    return None


def _client_ip() -> str:
    forwarded = str(request.headers.get("X-Forwarded-For", "")).split(",")[0].strip()
    return forwarded or str(request.remote_addr or "unknown")


def _is_login_throttled(db, username: str, ip_address: str) -> tuple[bool, int]:
    now_ts = int(time.time())
    cutoff = now_ts - LOGIN_WINDOW_SECONDS
    row = db.execute(
        """
        SELECT COUNT(*) AS failures
        FROM auth_login_attempts
        WHERE username = ? AND ip_address = ? AND success = 0
          AND created_at > datetime(?, 'unixepoch')
        """,
        (username, ip_address, cutoff),
    ).fetchone()
    failures = int(row["failures"] if row else 0)
    if failures < LOGIN_MAX_ATTEMPTS:
        return False, 0
    return True, LOGIN_WINDOW_SECONDS


def _record_login_attempt(db, username: str, ip_address: str, success: bool) -> None:
    db.execute(
        "INSERT INTO auth_login_attempts (username, ip_address, success) VALUES (?, ?, ?)",
        (username, ip_address, 1 if success else 0),
    )
    # Keep table bounded.
    db.execute("DELETE FROM auth_login_attempts WHERE created_at < datetime('now', '-30 days')")
    db.commit()


def _record_security_audit(
    event_type: str,
    *,
    user_id: int | None = None,
    username: str | None = None,
    ip_address: str | None = None,
    request_path: str | None = None,
    details: dict | None = None,
) -> None:
    payload = json.dumps(details or {}, separators=(",", ":"))
    ip = ip_address or _client_ip()
    path = request_path or request.path
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            """
            INSERT INTO security_audit_log
                (event_type, user_id, username, ip_address, request_path, details_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_type, user_id, username, ip, path, payload),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _is_mfa_enforced_for_username(username: str | None) -> bool:
    if not REQUIRE_MFA_FOR_SENSITIVE:
        return False
    if not REQUIRE_MFA_FOR_USERNAMES:
        return True
    return str(username or "").strip().lower() in REQUIRE_MFA_FOR_USERNAMES


def require_mfa_for_sensitive(f):
    from functools import wraps

    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({"error": "Login required"}), 401
        username = current_user.username or ""
        if not _is_mfa_enforced_for_username(username):
            return f(*args, **kwargs)

        db = get_db()
        row = db.execute("SELECT mfa_enabled FROM users WHERE id = ?", (current_user.id,)).fetchone()
        mfa_enabled = bool(row["mfa_enabled"]) if row and "mfa_enabled" in row.keys() else False
        if not mfa_enabled:
            _record_security_audit(
                "mfa_required_block",
                user_id=int(current_user.id),
                username=username,
                details={"reason": "mfa_not_enabled_for_sensitive_route"},
            )
            return jsonify({
                "error": "MFA required for sensitive actions",
                "action_required": "enable_mfa",
                "setup_endpoint": "/api/mfa/setup",
            }), 403
        return f(*args, **kwargs)

    return decorated


def _generate_totp_secret() -> str:
    return base64.b32encode(secrets.token_bytes(20)).decode().rstrip("=")


def _b32_decode_secret(secret: str) -> bytes:
    normalized = str(secret or "").strip().replace(" ", "").upper()
    padding = "=" * ((8 - (len(normalized) % 8)) % 8)
    return base64.b32decode(normalized + padding, casefold=True)


def _totp_code(secret: str, counter: int, digits: int = 6) -> str:
    key = _b32_decode_secret(secret)
    msg = int(counter).to_bytes(8, "big")
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    dbc = int.from_bytes(digest[offset:offset + 4], "big") & 0x7FFFFFFF
    return str(dbc % (10 ** digits)).zfill(digits)


def _verify_totp(secret: str, code: str, window: int = MFA_TOTP_WINDOW_STEPS) -> bool:
    candidate = str(code or "").strip()
    if not re.fullmatch(r"\d{6}", candidate):
        return False
    step = int(time.time() // 30)
    for delta in range(-window, window + 1):
        expected = _totp_code(secret, step + delta)
        if hmac.compare_digest(candidate, expected):
            return True
    return False


def _issue_mfa_challenge(db, user_id: int, username: str, ip_address: str) -> tuple[str, int]:
    token = "mfa_" + secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    expires_sec = MFA_CHALLENGE_TTL_SECONDS
    db.execute(
        """
        INSERT INTO auth_mfa_challenges
            (challenge_hash, user_id, username, ip_address, expires_at)
        VALUES (?, ?, ?, ?, datetime('now', ?))
        """,
        (token_hash, user_id, username, ip_address, f"+{expires_sec} seconds"),
    )
    db.execute(
        "DELETE FROM auth_mfa_challenges WHERE expires_at < datetime('now', '-1 day')"
    )
    db.commit()
    return token, expires_sec


def _lookup_active_mfa_challenge(db, token: str):
    token_hash = hashlib.sha256(str(token or "").encode()).hexdigest()
    return db.execute(
        """
        SELECT id, user_id, username, ip_address, attempts, expires_at
        FROM auth_mfa_challenges
        WHERE challenge_hash = ?
          AND consumed_at IS NULL
          AND expires_at > datetime('now')
        LIMIT 1
        """,
        (token_hash,),
    ).fetchone()


def _mark_mfa_challenge_failed(db, challenge_id: int) -> None:
    db.execute(
        "UPDATE auth_mfa_challenges SET attempts = attempts + 1 WHERE id = ?",
        (challenge_id,),
    )
    db.commit()


def _mark_mfa_challenge_consumed(db, challenge_id: int) -> None:
    db.execute(
        "UPDATE auth_mfa_challenges SET consumed_at = CURRENT_TIMESTAMP WHERE id = ?",
        (challenge_id,),
    )
    db.commit()


def _decrypt_user_mfa_secret(row) -> str | None:
    enc = str(row["mfa_secret_enc"] or "").strip() if row and "mfa_secret_enc" in row.keys() else ""
    if not enc:
        return None
    try:
        return decrypt_credential(enc)
    except Exception:
        return None


@app.route("/")
def index():
    # Enterprise/Pro users go straight to trading dashboard
    if current_user.is_authenticated:
        try:
            db = get_db()
            row = db.execute("SELECT tier FROM users WHERE id = ?", (current_user.id,)).fetchone()
            user_tier = row["tier"] if row and "tier" in row.keys() else "free"
            if user_tier in ("enterprise", "enterprise_pro", "government"):
                return redirect(url_for("trading_dashboard"))
        except Exception:
            pass
    return render_template("index.html",
                           stripe_pk=STRIPE_PUBLISHABLE_KEY,
                           price_id=STRIPE_PRICE_ID,
                           stripe_live=bool(stripe.api_key and STRIPE_PRICE_ID),
                           crypto_live=bool(COINBASE_COMMERCE_API_KEY))


@app.route("/api/register", methods=["POST"])
def register():
    data = request.get_json() or {}
    username = str(data.get("username", "")).strip().lower()
    password = str(data.get("password", ""))

    if not username or len(username) < 3 or len(username) > 30:
        return jsonify({"error": "Username must be 3-30 characters"}), 400
    if not re.match(r'^[a-z0-9_]+$', username):
        return jsonify({"error": "Username: lowercase letters, numbers, underscores only"}), 400
    password_error = _validate_password_strength(password)
    if password_error:
        return jsonify({"error": password_error}), 400

    db = get_db()
    if db.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone():
        return jsonify({"error": "Username already taken"}), 409

    pw_hash = generate_password_hash(password)
    cur = db.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)",
                     (username, pw_hash))
    db.commit()
    user = User(cur.lastrowid, username)
    _record_security_audit(
        "user_registered",
        user_id=int(cur.lastrowid),
        username=username,
    )
    session.clear()
    login_user(user, fresh=True)
    session.permanent = True
    return jsonify({"ok": True, "username": username, "subscribed": False})


@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json() or {}
    username = str(data.get("username", "")).strip().lower()
    password = str(data.get("password", ""))

    db = get_db()
    ip_address = _client_ip()
    throttled, retry_after = _is_login_throttled(db, username, ip_address)
    if throttled:
        _record_security_audit(
            "login_throttled",
            username=username,
            ip_address=ip_address,
            details={"retry_after_seconds": retry_after},
        )
        return jsonify({
            "error": "Too many failed login attempts",
            "retry_after_seconds": retry_after,
        }), 429

    row = db.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if not row or not check_password_hash(row["password_hash"], password):
        _record_login_attempt(db, username, ip_address, success=False)
        _record_security_audit(
            "login_failed",
            username=username,
            ip_address=ip_address,
            details={"reason": "invalid_credentials"},
        )
        if LOGIN_BACKOFF_MS:
            time.sleep(LOGIN_BACKOFF_MS / 1000.0)
        return jsonify({"error": "Invalid credentials"}), 401

    mfa_enabled = bool(row["mfa_enabled"]) if "mfa_enabled" in row.keys() else False
    if mfa_enabled:
        token, expires_sec = _issue_mfa_challenge(db, int(row["id"]), username, ip_address)
        _record_security_audit(
            "login_mfa_challenge_issued",
            user_id=int(row["id"]),
            username=username,
            ip_address=ip_address,
            details={"expires_in_seconds": expires_sec},
        )
        return jsonify({
            "ok": True,
            "mfa_required": True,
            "challenge_token": token,
            "expires_in_seconds": expires_sec,
            "method": "totp",
        })

    _record_login_attempt(db, username, ip_address, success=True)
    _record_security_audit(
        "login_success",
        user_id=int(row["id"]),
        username=username,
        ip_address=ip_address,
        details={"mfa": False},
    )
    session.clear()
    user = User(row["id"], row["username"],
                row["subscription_status"] or "none",
                row["stripe_customer_id"],
                row["payment_method"] if "payment_method" in row.keys() else "none",
                row["subscription_expires_at"] if "subscription_expires_at" in row.keys() else None,
                row["created_at"] if "created_at" in row.keys() else None)
    login_user(user, fresh=True)
    session.permanent = True
    return jsonify({"ok": True, "username": username, "subscribed": user.is_subscribed})


@app.route("/api/login/mfa", methods=["POST"])
def login_mfa():
    data = request.get_json() or {}
    token = str(data.get("challenge_token", "")).strip()
    code = str(data.get("code", "")).strip()
    if not token or not code:
        return jsonify({"error": "challenge_token and code are required"}), 400

    db = get_db()
    ip_address = _client_ip()
    challenge = _lookup_active_mfa_challenge(db, token)
    if not challenge:
        _record_security_audit(
            "login_mfa_failed",
            ip_address=ip_address,
            details={"reason": "invalid_or_expired_challenge"},
        )
        return jsonify({"error": "Invalid or expired MFA challenge"}), 401
    if int(challenge["attempts"] or 0) >= MFA_MAX_VERIFY_ATTEMPTS:
        _record_security_audit(
            "login_mfa_failed",
            user_id=int(challenge["user_id"]),
            username=str(challenge["username"]),
            ip_address=ip_address,
            details={"reason": "challenge_locked"},
        )
        return jsonify({"error": "MFA challenge locked; start login again"}), 401

    if str(challenge["ip_address"] or "") != ip_address:
        _mark_mfa_challenge_failed(db, int(challenge["id"]))
        _record_login_attempt(db, str(challenge["username"]), ip_address, success=False)
        _record_security_audit(
            "login_mfa_failed",
            user_id=int(challenge["user_id"]),
            username=str(challenge["username"]),
            ip_address=ip_address,
            details={"reason": "challenge_ip_mismatch"},
        )
        return jsonify({"error": "MFA challenge IP mismatch"}), 401

    user_row = db.execute("SELECT * FROM users WHERE id = ?", (challenge["user_id"],)).fetchone()
    if not user_row:
        _mark_mfa_challenge_failed(db, int(challenge["id"]))
        _record_security_audit(
            "login_mfa_failed",
            user_id=int(challenge["user_id"]),
            username=str(challenge["username"]),
            ip_address=ip_address,
            details={"reason": "user_missing"},
        )
        return jsonify({"error": "User not found"}), 401

    secret = _decrypt_user_mfa_secret(user_row)
    if not secret:
        _mark_mfa_challenge_failed(db, int(challenge["id"]))
        _record_login_attempt(db, str(user_row["username"]), ip_address, success=False)
        _record_security_audit(
            "login_mfa_failed",
            user_id=int(user_row["id"]),
            username=str(user_row["username"]),
            ip_address=ip_address,
            details={"reason": "mfa_secret_unavailable"},
        )
        return jsonify({"error": "MFA secret unavailable; contact support"}), 503
    if not _verify_totp(secret, code):
        _mark_mfa_challenge_failed(db, int(challenge["id"]))
        _record_login_attempt(db, str(user_row["username"]), ip_address, success=False)
        _record_security_audit(
            "login_mfa_failed",
            user_id=int(user_row["id"]),
            username=str(user_row["username"]),
            ip_address=ip_address,
            details={"reason": "invalid_totp"},
        )
        return jsonify({"error": "Invalid MFA code"}), 401

    _mark_mfa_challenge_consumed(db, int(challenge["id"]))
    _record_login_attempt(db, str(user_row["username"]), ip_address, success=True)
    _record_security_audit(
        "login_success",
        user_id=int(user_row["id"]),
        username=str(user_row["username"]),
        ip_address=ip_address,
        details={"mfa": True},
    )
    session.clear()
    user = User(user_row["id"], user_row["username"],
                user_row["subscription_status"] or "none",
                user_row["stripe_customer_id"],
                user_row["payment_method"] if "payment_method" in user_row.keys() else "none",
                user_row["subscription_expires_at"] if "subscription_expires_at" in user_row.keys() else None,
                user_row["created_at"] if "created_at" in user_row.keys() else None)
    login_user(user, fresh=True)
    session.permanent = True
    return jsonify({"ok": True, "username": user.username, "subscribed": user.is_subscribed})


@app.route("/api/logout", methods=["POST"])
@login_required
def logout():
    logout_user()
    return jsonify({"ok": True})


@app.route("/api/me")
def me():
    if current_user.is_authenticated:
        check_and_expire_crypto(current_user.id)
        # Reload user to get fresh status after possible expiry
        db = get_db()
        row = db.execute("SELECT * FROM users WHERE id=?", (current_user.id,)).fetchone()
        pm = row["payment_method"] if row and "payment_method" in row.keys() else "none"
        status = row["subscription_status"] if row else "none"
        expires = row["subscription_expires_at"] if row and "subscription_expires_at" in row.keys() else None
        created = row["created_at"] if row and "created_at" in row.keys() else None
        cust_id = row["stripe_customer_id"] if row else None
        user = User(current_user.id, current_user.username, status or "none",
                    cust_id, pm, expires, created)
        user_tier = row["tier"] if row and "tier" in row.keys() else "free"
        # Grandfathered: old $20/mo subscribers get Pro tier
        if user_tier == "free" and user.is_subscribed:
            user_tier = "pro"
        return jsonify({
            "authenticated": True,
            "username": user.username,
            "subscribed": user.is_subscribed,
            "subscription_status": user.subscription_status,
            "payment_method": user.payment_method,
            "tier": user_tier,
            "mfa_enabled": bool(row["mfa_enabled"]) if row and "mfa_enabled" in row.keys() else False,
            "mfa_required_for_sensitive": _is_mfa_enforced_for_username(user.username),
            "created_at": user.created_at,
            "subscription_expires_at": expires if pm == "crypto" else None,
            "has_stripe_billing": bool(cust_id),
        })
    return jsonify({"authenticated": False})


@app.route("/api/mfa/status")
@login_required
def mfa_status():
    db = get_db()
    row = db.execute(
        "SELECT mfa_enabled, mfa_enrolled_at FROM users WHERE id = ?",
        (current_user.id,),
    ).fetchone()
    return jsonify({
        "enabled": bool(row["mfa_enabled"]) if row else False,
        "enrolled_at": row["mfa_enrolled_at"] if row else None,
        "required_for_sensitive": _is_mfa_enforced_for_username(current_user.username),
    })


@app.route("/api/mfa/setup", methods=["POST"])
@login_required
def mfa_setup():
    data = request.get_json() or {}
    password = str(data.get("password", ""))

    db = get_db()
    row = db.execute("SELECT username, password_hash FROM users WHERE id = ?", (current_user.id,)).fetchone()
    if not row or not check_password_hash(row["password_hash"], password):
        return jsonify({"error": "Invalid password"}), 401

    secret = _generate_totp_secret()
    try:
        enc_secret = encrypt_credential(secret)
    except Exception as e:
        return jsonify({"error": f"MFA setup unavailable: {e}"}), 503

    db.execute(
        "UPDATE users SET mfa_secret_pending_enc = ? WHERE id = ?",
        (enc_secret, current_user.id),
    )
    db.commit()
    _record_security_audit(
        "mfa_setup_started",
        user_id=int(current_user.id),
        username=current_user.username,
    )
    label = f"{MFA_ISSUER}:{current_user.username}"
    otp_uri = (
        f"otpauth://totp/{urllib.parse.quote(label)}"
        f"?secret={secret}&issuer={urllib.parse.quote(MFA_ISSUER)}&algorithm=SHA1&digits=6&period=30"
    )
    return jsonify({
        "ok": True,
        "issuer": MFA_ISSUER,
        "secret": secret,
        "otpauth_uri": otp_uri,
        "message": "Add this to your authenticator app, then call /api/mfa/enable with a 6-digit code.",
    })


@app.route("/api/mfa/enable", methods=["POST"])
@login_required
def mfa_enable():
    data = request.get_json() or {}
    code = str(data.get("code", "")).strip()
    if not code:
        return jsonify({"error": "code required"}), 400

    db = get_db()
    row = db.execute(
        "SELECT mfa_secret_pending_enc FROM users WHERE id = ?",
        (current_user.id,),
    ).fetchone()
    pending_enc = str(row["mfa_secret_pending_enc"] or "") if row else ""
    if not pending_enc:
        return jsonify({"error": "No pending MFA setup; call /api/mfa/setup first"}), 400
    try:
        pending_secret = decrypt_credential(pending_enc)
    except Exception:
        return jsonify({"error": "Pending MFA secret invalid; restart setup"}), 400
    if not _verify_totp(pending_secret, code):
        return jsonify({"error": "Invalid MFA code"}), 401

    final_enc = encrypt_credential(pending_secret)
    db.execute(
        """
        UPDATE users
        SET mfa_enabled = 1,
            mfa_secret_enc = ?,
            mfa_secret_pending_enc = NULL,
            mfa_enrolled_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (final_enc, current_user.id),
    )
    db.commit()
    _record_security_audit(
        "mfa_enabled",
        user_id=int(current_user.id),
        username=current_user.username,
    )
    return jsonify({"ok": True, "enabled": True})


@app.route("/api/mfa/disable", methods=["POST"])
@login_required
def mfa_disable():
    data = request.get_json() or {}
    password = str(data.get("password", ""))
    code = str(data.get("code", "")).strip()
    if not password or not code:
        return jsonify({"error": "password and code required"}), 400

    db = get_db()
    row = db.execute(
        "SELECT password_hash, mfa_secret_enc FROM users WHERE id = ?",
        (current_user.id,),
    ).fetchone()
    if not row or not check_password_hash(row["password_hash"], password):
        return jsonify({"error": "Invalid password"}), 401
    secret = _decrypt_user_mfa_secret(row)
    if not secret or not _verify_totp(secret, code):
        return jsonify({"error": "Invalid MFA code"}), 401

    db.execute(
        """
        UPDATE users
        SET mfa_enabled = 0,
            mfa_secret_enc = NULL,
            mfa_secret_pending_enc = NULL
        WHERE id = ?
        """,
        (current_user.id,),
    )
    db.commit()
    _record_security_audit(
        "mfa_disabled",
        user_id=int(current_user.id),
        username=current_user.username,
    )
    return jsonify({"ok": True, "enabled": False})


@app.route("/api/security/audit")
@login_required
@require_mfa_for_sensitive
def security_audit_feed():
    """Read recent security audit events for current user."""
    limit = min(500, max(1, int(request.args.get("limit", 100) or 100)))
    db = get_db()
    rows = db.execute(
        """
        SELECT id, event_type, user_id, username, ip_address, request_path, details_json, created_at
        FROM security_audit_log
        WHERE user_id = ? OR (user_id IS NULL AND username = ?)
        ORDER BY id DESC
        LIMIT ?
        """,
        (current_user.id, current_user.username, limit),
    ).fetchall()
    events = []
    for r in rows:
        try:
            details = json.loads(r["details_json"] or "{}")
        except Exception:
            details = {}
        events.append({
            "id": r["id"],
            "event_type": r["event_type"],
            "user_id": r["user_id"],
            "username": r["username"],
            "ip_address": r["ip_address"],
            "request_path": r["request_path"],
            "details": details,
            "created_at": r["created_at"],
        })
    return jsonify({"events": events, "count": len(events)})


# ---------------------------------------------------------------------------
# Stripe routes
# ---------------------------------------------------------------------------

@app.route("/api/create-checkout", methods=["POST"])
@login_required
def create_checkout():
    if not stripe.api_key or not STRIPE_PRICE_ID:
        return jsonify({"error": "Payments not configured"}), 503

    db = get_db()
    row = db.execute("SELECT stripe_customer_id FROM users WHERE id=?",
                     (current_user.id,)).fetchone()
    customer_id = row["stripe_customer_id"] if row else None

    # Create or reuse Stripe customer
    if not customer_id:
        customer = stripe.Customer.create(
            metadata={"nettrace_user_id": str(current_user.id),
                      "nettrace_username": current_user.username}
        )
        customer_id = customer.id
        db.execute("UPDATE users SET stripe_customer_id=? WHERE id=?",
                   (customer_id, current_user.id))
        db.commit()

    # Use automatic_payment_methods to only offer what's enabled in Stripe dashboard
    checkout_session = stripe.checkout.Session.create(
        customer=customer_id,
        line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
        mode="subscription",
        success_url=APP_URL + "/?subscription=success",
        cancel_url=APP_URL + "/?subscription=cancelled",
        metadata={"nettrace_user_id": str(current_user.id)}
    )
    return jsonify({"checkout_url": checkout_session.url})


@app.route("/api/create-crypto-checkout", methods=["POST"])
@login_required
def create_crypto_checkout():
    if not COINBASE_COMMERCE_API_KEY:
        return jsonify({"error": "Crypto payments not configured"}), 503

    try:
        req = urllib.request.Request(
            "https://api.commerce.coinbase.com/charges",
            data=json.dumps({
                "name": "NetTrace Pro - Monthly",
                "description": "1 month of unlimited traceroute scanning",
                "pricing_type": "fixed_price",
                "local_price": {"amount": "20.00", "currency": "USD"},
                "metadata": {"user_id": str(current_user.id), "username": current_user.username},
                "redirect_url": APP_URL + "/?subscription=success",
                "cancel_url": APP_URL + "/?subscription=cancelled"
            }).encode(),
            headers={
                "Content-Type": "application/json",
                "X-CC-Api-Key": COINBASE_COMMERCE_API_KEY,
                "X-CC-Version": "2018-03-22",
                "Accept": "application/json",
                "User-Agent": "NetTrace/1.0"
            }
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        hosted_url = data["data"]["hosted_url"]
        return jsonify({"checkout_url": hosted_url})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/coinbase-webhook", methods=["POST"])
def coinbase_webhook():
    if not COINBASE_WEBHOOK_SECRET:
        return jsonify({"error": "Webhook not configured"}), 503

    raw_payload = request.get_data()
    sig = request.headers.get("X-CC-Webhook-Signature", "")
    if not sig:
        return jsonify({"error": "Missing signature"}), 400

    expected_sig = hmac.new(
        COINBASE_WEBHOOK_SECRET.encode(),
        raw_payload,
        hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(sig, expected_sig):
        return jsonify({"error": "Invalid signature"}), 400

    try:
        payload = json.loads(raw_payload.decode("utf-8"))
    except ValueError:
        return jsonify({"error": "Invalid payload"}), 400

    event_type = payload.get("event", {}).get("type", "")
    data = payload.get("event", {}).get("data", {})

    if event_type == "charge:confirmed":
        user_id = data.get("metadata", {}).get("user_id")
        if user_id:
            try:
                user_id_int = int(user_id)
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid user metadata"}), 400
            expires = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
            db = sqlite3.connect(DB_PATH)
            db.execute(
                "UPDATE users SET subscription_status='active', payment_method='crypto', subscription_expires_at=? WHERE id=?",
                (expires, user_id_int))
            db.commit()
            db.close()

    return jsonify({"ok": True})


@app.route("/api/manage-billing", methods=["POST"])
@login_required
def manage_billing():
    if not stripe.api_key:
        return jsonify({"error": "Payments not configured"}), 503

    db = get_db()
    row = db.execute("SELECT stripe_customer_id FROM users WHERE id=?",
                     (current_user.id,)).fetchone()
    if not row or not row["stripe_customer_id"]:
        return jsonify({"error": "No billing account found"}), 404

    portal = stripe.billing_portal.Session.create(
        customer=row["stripe_customer_id"],
        return_url=APP_URL
    )
    return jsonify({"portal_url": portal.url})


@app.route("/api/cancel-subscription", methods=["POST"])
@login_required
def cancel_subscription():
    db = get_db()
    row = db.execute("SELECT payment_method, stripe_customer_id FROM users WHERE id=?",
                     (current_user.id,)).fetchone()
    if not row:
        return jsonify({"error": "User not found"}), 404

    pm = row["payment_method"] if "payment_method" in row.keys() else "none"

    if pm == "stripe":
        return jsonify({"use_portal": True})

    if pm == "crypto":
        db.execute("UPDATE users SET subscription_status='cancelled' WHERE id=?",
                   (current_user.id,))
        db.commit()
        return jsonify({"ok": True, "message": "Subscription cancelled. Access continues until expiry date."})

    return jsonify({"error": "No active subscription to cancel"}), 400


@app.route("/api/stripe-webhook", methods=["POST"])
def stripe_webhook():
    if not STRIPE_WEBHOOK_SECRET:
        return jsonify({"error": "Webhook not configured"}), 503

    payload = request.get_data()
    sig = request.headers.get("Stripe-Signature")
    if not sig:
        return jsonify({"error": "Missing signature"}), 400

    try:
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
    except ValueError:
        return jsonify({"error": "Invalid payload"}), 400
    except stripe.error.SignatureVerificationError:
        return jsonify({"error": "Invalid signature"}), 400

    event_type = event.get("type", "")
    data_obj = event.get("data", {}).get("object", {})

    if event_type in ("customer.subscription.created",
                      "customer.subscription.updated",
                      "customer.subscription.deleted"):
        customer_id = data_obj.get("customer")
        status = data_obj.get("status", "none")
        if customer_id:
            db = sqlite3.connect(DB_PATH)
            db.execute("UPDATE users SET subscription_status=?, payment_method='stripe', subscription_expires_at=NULL WHERE stripe_customer_id=?",
                       (status, customer_id))
            db.commit()
            db.close()

    elif event_type == "checkout.session.completed":
        customer_id = data_obj.get("customer")
        user_id = data_obj.get("metadata", {}).get("nettrace_user_id")
        checkout_tier = data_obj.get("metadata", {}).get("tier", "pro")
        if customer_id and user_id:
            try:
                user_id_int = int(user_id)
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid user metadata"}), 400
            db = sqlite3.connect(DB_PATH)
            db.execute("UPDATE users SET stripe_customer_id=?, subscription_status='active', payment_method='stripe', subscription_expires_at=NULL, tier=? WHERE id=?",
                       (customer_id, checkout_tier, user_id_int))
            db.commit()
            db.close()

    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Scan routes (subscription required)
# ---------------------------------------------------------------------------

@app.route("/api/scan", methods=["POST"])
@login_required
@require_subscription
def start_scan():
    data = request.get_json()
    host = data.get("host", "").strip()
    name = data.get("name", host).strip()

    if not host:
        return jsonify({"error": "Host is required"}), 400
    if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9\-\.]+[a-zA-Z0-9]$', host):
        return jsonify({"error": "Invalid hostname"}), 400
    if len(host) > 253:
        return jsonify({"error": "Hostname too long"}), 400

    db = get_db()
    hour_ago = time.time() - 3600
    count = db.execute(
        "SELECT COUNT(*) as cnt FROM scans WHERE user_id=? AND created_at > datetime(?, 'unixepoch')",
        (current_user.id, hour_ago)
    ).fetchone()["cnt"]
    if count >= MAX_SCANS_PER_HOUR:
        return jsonify({"error": f"Rate limit: max {MAX_SCANS_PER_HOUR} scans/hour"}), 429

    cur = db.execute(
        "INSERT INTO scans (user_id, target_host, target_name) VALUES (?, ?, ?)",
        (current_user.id, host, name)
    )
    db.commit()
    scan_id = cur.lastrowid

    sid = request.args.get("sid") or data.get("sid")
    if not execute_scan(scan_id, host, current_user.id, sid=sid):
        return jsonify({"error": f"Max {MAX_CONCURRENT_SCANS} concurrent scans"}), 429

    return jsonify({"ok": True, "scan_id": scan_id})


@app.route("/api/scans")
@login_required
def list_scans():
    db = get_db()
    rows = db.execute(
        "SELECT id, target_host, target_name, category, status, result_json, created_at "
        "FROM scans WHERE user_id=? ORDER BY created_at DESC LIMIT 50",
        (current_user.id,)
    ).fetchall()
    return jsonify([{
        "id": r["id"], "host": r["target_host"], "name": r["target_name"],
        "category": r["category"], "status": r["status"], "created_at": r["created_at"],
        **({"result": json.loads(r["result_json"])} if r["result_json"] else {})
    } for r in rows])


@app.route("/api/demo")
def demo_data():
    if DEMO_RESULTS.exists():
        with open(DEMO_RESULTS) as f:
            return jsonify(json.load(f))
    return jsonify([])


# WebSocket events
@socketio.on("connect")
def ws_connect():
    emit("connected", {"sid": request.sid})


# ---------------------------------------------------------------------------
# Agent WebSocket namespace (/ws/agent)
# ---------------------------------------------------------------------------

class AgentNamespace(Namespace):
    """Full-duplex agent endpoint for the local MCP server."""

    authenticated_sids = set()

    def on_connect(self):
        pass  # wait for auth message

    def on_disconnect(self):
        self.authenticated_sids.discard(request.sid)

    def on_message(self, data):
        if isinstance(data, str):
            data = json.loads(data)

        # Auth handshake
        if data.get("type") == "auth":
            if not MCP_AGENT_SECRET:
                emit("message", {"status": "error", "error": "Agent secret not configured"})
                disconnect()
                return
            if not hmac.compare_digest(str(data.get("secret", "")), str(MCP_AGENT_SECRET)):
                emit("message", {"status": "error", "error": "Invalid secret"})
                disconnect()
                return
            self.authenticated_sids.add(request.sid)
            emit("message", {"status": "ok"})
            return

        # All other commands require auth
        if request.sid not in self.authenticated_sids:
            emit("message", {"status": "error", "error": "Not authenticated"})
            disconnect()
            return

        req_id = data.get("id", "")
        cmd = data.get("cmd", "")
        args = data.get("args", {})

        if cmd == "ping":
            emit("message", {"id": req_id, "status": "complete", "data": {"pong": True}})

        elif cmd == "status":
            load = os.getloadavg()
            with scan_lock:
                n_scans = len(active_scans)
            try:
                import resource
                mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
            except Exception:
                mem_mb = None
            emit("message", {
                "id": req_id, "status": "complete",
                "data": {"active_scans": n_scans, "load_avg": list(load), "memory_mb": mem_mb}
            })

        elif cmd == "scan":
            host = args.get("host", "").strip()
            if not host:
                emit("message", {"id": req_id, "status": "error", "data": {"error": "host required"}})
                return
            emit("message", {"id": req_id, "status": "running", "data": {"message": f"Scanning {host}..."}})

            def _do_scan():
                tr = run_traceroute(host)
                hops = tr.get("hops", [])
                for h in hops:
                    if h.get("ip"):
                        h["geo"] = geolocate_ip(h["ip"])
                        time.sleep(0.15)
                    else:
                        h["geo"] = None
                hops = sanitize_hops(hops)
                total_rtt = None
                for h in reversed(hops):
                    if h.get("rtt_ms"):
                        total_rtt = h["rtt_ms"]
                        break
                result = {"host": host, "hop_count": len(hops), "total_rtt": total_rtt, "hops": hops}
                socketio.emit("message", {"id": req_id, "status": "complete", "data": result},
                              namespace="/ws/agent", to=request.sid)

            threading.Thread(target=_do_scan, daemon=True).start()

        elif cmd == "exec":
            # SECURITY: Remote code execution removed — arbitrary shell=True was an RCE vulnerability.
            # Anyone on the WebSocket could steal wallet keys or drain funds.
            emit("message", {"id": req_id, "status": "error",
                             "data": {"error": "exec command disabled for security"}})

        else:
            emit("message", {"id": req_id, "status": "error", "data": {"error": f"Unknown command: {cmd}"}})


socketio.on_namespace(AgentNamespace("/ws/agent"))


# ---------------------------------------------------------------------------
# API v1 Blueprint
# ---------------------------------------------------------------------------

from api_v1 import api_v1
app.register_blueprint(api_v1)
from signals_api import signals_api
app.register_blueprint(signals_api)
from candle_graphql_api import candle_graphql_api
app.register_blueprint(candle_graphql_api)
from api_orgs import api_orgs
app.register_blueprint(api_orgs)
from api_agents import api_agents
app.register_blueprint(api_agents)
from mcp_marketplace import mcp_bp
app.register_blueprint(mcp_bp)


# ---------------------------------------------------------------------------
# API Key Management Routes
# ---------------------------------------------------------------------------

from api_auth import generate_api_key, TIER_CONFIG, verify_api_key, require_write, ensure_read_only_column

@app.route("/api/keys")
@login_required
def list_api_keys():
    db = get_db()
    rows = db.execute(
        "SELECT id, key_prefix, name, tier, rate_limit_daily, is_active, last_used_at, created_at FROM api_keys WHERE user_id = ? ORDER BY created_at DESC",
        (current_user.id,)
    ).fetchall()
    return jsonify([{
        "id": r["id"], "prefix": r["key_prefix"], "name": r["name"],
        "tier": r["tier"], "rate_limit_daily": r["rate_limit_daily"],
        "is_active": bool(r["is_active"]), "last_used_at": r["last_used_at"],
        "created_at": r["created_at"]
    } for r in rows])


@app.route("/api/keys", methods=["POST"])
@login_required
@require_mfa_for_sensitive
def create_api_key():
    db = get_db()

    # Max 5 keys per user
    count = db.execute("SELECT COUNT(*) as cnt FROM api_keys WHERE user_id = ?",
                       (current_user.id,)).fetchone()["cnt"]
    if count >= 5:
        return jsonify({"error": "Maximum 5 API keys per account"}), 400

    data = request.get_json() or {}
    key_name = data.get("name", "default").strip()[:50]

    # Determine tier from user's subscription
    user_row = db.execute("SELECT tier, subscription_status FROM users WHERE id = ?",
                          (current_user.id,)).fetchone()
    user_tier = (user_row["tier"] if user_row and user_row["tier"] else "free")
    # If they have an active subscription but tier is still 'free', default to pro
    if user_tier == "free" and current_user.is_subscribed:
        user_tier = "pro"

    tier_conf = TIER_CONFIG.get(user_tier, TIER_CONFIG["free"])
    rate_limit = tier_conf["rate_limit_daily"]

    raw_key, key_hash, key_prefix = generate_api_key()

    db.execute(
        "INSERT INTO api_keys (user_id, key_hash, key_prefix, name, tier, rate_limit_daily) VALUES (?, ?, ?, ?, ?, ?)",
        (current_user.id, key_hash, key_prefix, key_name, user_tier, rate_limit)
    )
    db.commit()
    _record_security_audit(
        "api_key_created",
        user_id=int(current_user.id),
        username=current_user.username,
        details={"prefix": key_prefix, "tier": user_tier, "name": key_name},
    )

    return jsonify({
        "ok": True,
        "api_key": raw_key,
        "prefix": key_prefix,
        "name": key_name,
        "tier": user_tier,
        "rate_limit_daily": rate_limit,
        "warning": "Save this key now — it won't be shown again."
    })


@app.route("/api/keys/<int:key_id>", methods=["DELETE"])
@login_required
@require_mfa_for_sensitive
def delete_api_key(key_id):
    db = get_db()
    row = db.execute("SELECT id FROM api_keys WHERE id = ? AND user_id = ?",
                     (key_id, current_user.id)).fetchone()
    if not row:
        return jsonify({"error": "Key not found"}), 404

    db.execute("UPDATE api_keys SET is_active = 0 WHERE id = ?", (key_id,))
    db.commit()
    _record_security_audit(
        "api_key_deactivated",
        user_id=int(current_user.id),
        username=current_user.username,
        details={"key_id": int(key_id)},
    )
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Internal history endpoint (for dashboard Chart.js)
# ---------------------------------------------------------------------------

@app.route("/api/internal/history/<path:host>")
def internal_history(host):
    """Lightweight history endpoint for the dashboard (no API key needed, limited data)."""
    db = get_db()
    rows = db.execute(
        """SELECT total_rtt, hop_count, created_at FROM scan_metrics
           WHERE target_host = ? ORDER BY created_at DESC LIMIT 100""",
        (host,)
    ).fetchall()
    return jsonify([{
        "rtt": r["total_rtt"], "hops": r["hop_count"], "t": r["created_at"]
    } for r in rows])


# ---------------------------------------------------------------------------
# WebSocket Stream namespace (Enterprise real-time feed)
# ---------------------------------------------------------------------------

class StreamNamespace(Namespace):
    """Enterprise WebSocket stream for real-time latency updates."""

    authenticated_sids = set()

    def on_connect(self):
        pass

    def on_disconnect(self):
        self.authenticated_sids.discard(request.sid)

    def on_auth(self, data):
        """Authenticate with API key."""
        from api_auth import hash_api_key
        raw_key = data.get("api_key", "")
        if not raw_key:
            emit("error", {"error": "API key required"})
            disconnect()
            return

        key_hash = hash_api_key(raw_key)
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT tier, is_active FROM api_keys WHERE key_hash = ?", (key_hash,)).fetchone()
        db.close()

        if not row or not row["is_active"]:
            emit("error", {"error": "Invalid API key"})
            disconnect()
            return
        if row["tier"] not in ("enterprise", "enterprise_pro", "government"):
            emit("error", {"error": "WebSocket stream requires Enterprise tier or above"})
            disconnect()
            return

        self.authenticated_sids.add(request.sid)
        emit("authenticated", {"status": "ok", "tier": "enterprise"})

    def on_subscribe(self, data):
        """Subscribe to specific target or category updates."""
        if request.sid not in self.authenticated_sids:
            emit("error", {"error": "Not authenticated"})
            return
        # Subscription filtering handled client-side for now
        emit("subscribed", {"status": "ok", "filters": data})


socketio.on_namespace(StreamNamespace("/api/v1/stream"))


# ---------------------------------------------------------------------------
# Public Status Pages (SEO traffic → signups → revenue)
# ---------------------------------------------------------------------------

@app.route("/status")
def status_index():
    """Public status overview — indexed by search engines."""
    db = get_db()
    rows = db.execute("""
        SELECT m.target_host, m.target_name, m.category, m.total_rtt,
               m.hop_count, m.created_at
        FROM scan_metrics m
        INNER JOIN (
            SELECT target_host, MAX(id) as max_id FROM scan_metrics GROUP BY target_host
        ) latest ON m.id = latest.max_id
        WHERE m.total_rtt IS NOT NULL
        ORDER BY m.category, m.total_rtt ASC
    """).fetchall()
    categories = {}
    for r in rows:
        cat = r["category"] or "other"
        categories.setdefault(cat, []).append({
            "host": r["target_host"], "name": r["target_name"],
            "rtt": r["total_rtt"], "hops": r["hop_count"],
            "last_scan": r["created_at"]
        })
    return render_template("status.html", categories=categories, total=len(rows))


@app.route("/status/<path:host>")
def status_detail(host):
    """Public status page for a single target — SEO-indexed."""
    db = get_db()
    latest = db.execute(
        """SELECT target_host, target_name, category, total_rtt, hop_count,
                  first_hop_rtt, last_hop_rtt, route_hash, created_at
           FROM scan_metrics WHERE target_host = ? ORDER BY id DESC LIMIT 1""",
        (host,)
    ).fetchone()
    if not latest:
        return render_template("status_404.html", host=host), 404
    history = db.execute(
        """SELECT total_rtt, created_at FROM scan_metrics
           WHERE target_host = ? AND total_rtt IS NOT NULL
           ORDER BY created_at DESC LIMIT 96""",
        (host,)
    ).fetchall()
    changes = db.execute(
        """SELECT rtt_delta, detected_at FROM route_changes
           WHERE target_host = ? ORDER BY detected_at DESC LIMIT 5""",
        (host,)
    ).fetchall()
    return render_template("status_detail.html",
                           target=dict(latest), host=host,
                           history=[dict(r) for r in history],
                           changes=[dict(r) for r in changes])


# ---------------------------------------------------------------------------
# Prometheus Metrics Endpoint (free Fly.io monitoring)
# ---------------------------------------------------------------------------

@app.route("/metrics")
def prometheus_metrics():
    """Expose Prometheus metrics for free Fly.io scraping + Grafana dashboards."""
    db = get_db()
    lines = []
    lines.append("# HELP nettrace_target_rtt_ms Current RTT to target in milliseconds")
    lines.append("# TYPE nettrace_target_rtt_ms gauge")
    rows = db.execute("""
        SELECT m.target_host, m.target_name, m.category, m.total_rtt, m.hop_count
        FROM scan_metrics m
        INNER JOIN (
            SELECT target_host, MAX(id) as max_id FROM scan_metrics GROUP BY target_host
        ) latest ON m.id = latest.max_id
        WHERE m.total_rtt IS NOT NULL
    """).fetchall()
    for r in rows:
        name = (r["target_name"] or r["target_host"]).replace('"', '\\"')
        cat = (r["category"] or "other").replace('"', '\\"')
        lines.append(f'nettrace_target_rtt_ms{{host="{r["target_host"]}",name="{name}",category="{cat}"}} {r["total_rtt"]}')
    lines.append("")
    lines.append("# HELP nettrace_target_hops Current hop count to target")
    lines.append("# TYPE nettrace_target_hops gauge")
    for r in rows:
        name = (r["target_name"] or r["target_host"]).replace('"', '\\"')
        lines.append(f'nettrace_target_hops{{host="{r["target_host"]}",name="{name}"}} {r["hop_count"]}')
    lines.append("")
    lines.append("# HELP nettrace_targets_total Total number of monitored targets")
    lines.append("# TYPE nettrace_targets_total gauge")
    lines.append(f"nettrace_targets_total {len(rows)}")
    lines.append("")
    # Route changes in last hour
    rc = db.execute(
        "SELECT COUNT(*) as cnt FROM route_changes WHERE detected_at >= datetime('now', '-1 hour')"
    ).fetchone()
    lines.append("# HELP nettrace_route_changes_1h Route changes in the last hour")
    lines.append("# TYPE nettrace_route_changes_1h gauge")
    lines.append(f"nettrace_route_changes_1h {rc['cnt']}")
    lines.append("")
    # Quant signals in last hour
    qs = db.execute(
        "SELECT COUNT(*) as cnt FROM quant_signals WHERE created_at >= datetime('now', '-1 hour')"
    ).fetchone()
    lines.append("# HELP nettrace_quant_signals_1h Quant signals generated in last hour")
    lines.append("# TYPE nettrace_quant_signals_1h gauge")
    lines.append(f"nettrace_quant_signals_1h {qs['cnt']}")
    lines.append("")
    # API usage today
    au = db.execute(
        "SELECT COUNT(*) as cnt FROM api_usage WHERE created_at >= date('now')"
    ).fetchone()
    lines.append("# HELP nettrace_api_calls_today Total API calls today")
    lines.append("# TYPE nettrace_api_calls_today gauge")
    lines.append(f"nettrace_api_calls_today {au['cnt']}")
    lines.append("")
    # Users
    users = db.execute("SELECT COUNT(*) as cnt FROM users").fetchone()
    lines.append("# HELP nettrace_users_total Total registered users")
    lines.append("# TYPE nettrace_users_total gauge")
    lines.append(f"nettrace_users_total {users['cnt']}")
    lines.append("")
    from flask import Response
    return Response("\n".join(lines) + "\n", mimetype="text/plain; version=0.0.4; charset=utf-8")


# ---------------------------------------------------------------------------
# Embeddable Widget (viral distribution)
# ---------------------------------------------------------------------------

@app.route("/widget/<path:host>")
def latency_widget(host):
    """Embeddable latency badge — users put this on their sites."""
    db = get_db()
    row = db.execute(
        "SELECT target_name, total_rtt FROM scan_metrics WHERE target_host = ? ORDER BY id DESC LIMIT 1",
        (host,)
    ).fetchone()
    if not row:
        svg = _widget_svg("Not Found", "N/A", "#ff4444")
    elif row["total_rtt"] is None:
        svg = _widget_svg(row["target_name"] or host, "timeout", "#ff8800")
    elif row["total_rtt"] < 20:
        svg = _widget_svg(row["target_name"] or host, f'{row["total_rtt"]:.1f}ms', "#00ff88")
    elif row["total_rtt"] < 50:
        svg = _widget_svg(row["target_name"] or host, f'{row["total_rtt"]:.1f}ms', "#00d4ff")
    elif row["total_rtt"] < 100:
        svg = _widget_svg(row["target_name"] or host, f'{row["total_rtt"]:.1f}ms', "#ffaa00")
    else:
        svg = _widget_svg(row["target_name"] or host, f'{row["total_rtt"]:.1f}ms', "#ff4444")
    from flask import Response
    return Response(svg, mimetype="image/svg+xml",
                    headers={"Cache-Control": "public, max-age=300"})


def _widget_svg(label, value, color):
    label_w = max(80, len(label) * 7 + 10)
    value_w = max(50, len(value) * 7 + 10)
    total_w = label_w + value_w
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{total_w}" height="20">
  <rect width="{label_w}" height="20" fill="#333"/>
  <rect x="{label_w}" width="{value_w}" height="20" fill="{color}"/>
  <text x="{label_w/2}" y="14" fill="#fff" text-anchor="middle" font-family="monospace" font-size="11">{label}</text>
  <text x="{label_w + value_w/2}" y="14" fill="#fff" text-anchor="middle" font-family="monospace" font-size="11" font-weight="bold">{value}</text>
</svg>'''


# ---------------------------------------------------------------------------
# Public API Playground (try before you buy → conversion)
# ---------------------------------------------------------------------------

@app.route("/playground")
def api_playground():
    """Interactive API playground — try endpoints before signing up."""
    return render_template("playground.html")


# ---------------------------------------------------------------------------
# SEO: Sitemap + Robots.txt (free Google traffic)
# ---------------------------------------------------------------------------

@app.route("/sitemap.xml")
def sitemap():
    """Dynamic sitemap for search engine indexing."""
    db = get_db()
    rows = db.execute("""
        SELECT DISTINCT target_host FROM scan_metrics ORDER BY target_host
    """).fetchall()
    from flask import Response
    urls = ['<url><loc>https://nettrace-dashboard.fly.dev/status</loc><changefreq>hourly</changefreq><priority>1.0</priority></url>',
            '<url><loc>https://nettrace-dashboard.fly.dev/playground</loc><changefreq>weekly</changefreq><priority>0.8</priority></url>']
    for r in rows:
        urls.append(f'<url><loc>https://nettrace-dashboard.fly.dev/status/{r["target_host"]}</loc><changefreq>hourly</changefreq><priority>0.7</priority></url>')
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + '\n'.join(urls) + '\n</urlset>'
    return Response(xml, mimetype="application/xml")


@app.route("/robots.txt")
def robots():
    from flask import Response
    return Response("User-agent: *\nAllow: /\nSitemap: https://nettrace-dashboard.fly.dev/sitemap.xml\n", mimetype="text/plain")


# ---------------------------------------------------------------------------
# Tiered Stripe checkout
# ---------------------------------------------------------------------------

@app.route("/api/create-checkout-tier", methods=["POST"])
@login_required
def create_checkout_tier():
    """Create checkout for Pro or Enterprise tier."""
    data = request.get_json() or {}
    tier = data.get("tier", "pro")

    price_map = {
        "pro": STRIPE_PRO_PRICE_ID or STRIPE_PRICE_ID,
        "enterprise": STRIPE_ENTERPRISE_PRICE_ID,
        "enterprise_pro": STRIPE_ENTERPRISE_PRO_PRICE_ID,
        "government": STRIPE_GOVERNMENT_PRICE_ID,
    }

    price_id = price_map.get(tier)
    if not price_id or not stripe.api_key:
        return jsonify({"error": "Payments not configured for this tier"}), 503

    db = get_db()
    row = db.execute("SELECT stripe_customer_id FROM users WHERE id=?",
                     (current_user.id,)).fetchone()
    customer_id = row["stripe_customer_id"] if row else None

    if not customer_id:
        customer = stripe.Customer.create(
            metadata={"nettrace_user_id": str(current_user.id),
                      "nettrace_username": current_user.username}
        )
        customer_id = customer.id
        db.execute("UPDATE users SET stripe_customer_id=? WHERE id=?",
                   (customer_id, current_user.id))
        db.commit()

    checkout_session = stripe.checkout.Session.create(
        customer=customer_id,
        line_items=[{"price": price_id, "quantity": 1}],
        mode="subscription",
        success_url=APP_URL + "/?subscription=success",
        cancel_url=APP_URL + "/?subscription=cancelled",
        metadata={"nettrace_user_id": str(current_user.id), "tier": tier}
    )
    return jsonify({"checkout_url": checkout_session.url})


# ---------------------------------------------------------------------------
# Live Trading Dashboard
# ---------------------------------------------------------------------------

@app.route("/trading")
@login_required
def trading_dashboard():
    """Live trading dashboard — shows real portfolio, P&L, trades."""
    db = get_db()
    row = db.execute("SELECT tier FROM users WHERE id = ?", (current_user.id,)).fetchone()
    user_tier = row["tier"] if row and "tier" in row.keys() else "free"
    if user_tier not in ("enterprise", "enterprise_pro", "government") and not current_user.is_subscribed:
        return redirect(url_for("index"))
    # Pass user's orgs to template for org context bar
    user_orgs = []
    try:
        orgs = db.execute(
            "SELECT o.id, o.slug, o.name, o.tier, m.role FROM organizations o "
            "JOIN org_members m ON o.id = m.org_id WHERE m.user_id = ?",
            (current_user.id,)
        ).fetchall()
        user_orgs = [{"id": o["id"], "slug": o["slug"], "name": o["name"],
                      "tier": o["tier"], "role": o["role"]} for o in orgs]
    except Exception:
        pass
    return render_template("trading.html", user_orgs=json.dumps(user_orgs))


@app.route("/api/trading-data")
@login_required
def trading_data():
    """Return latest trading snapshot + signals for the dashboard."""
    db = get_db()
    row = db.execute("SELECT tier FROM users WHERE id = ?", (current_user.id,)).fetchone()
    user_tier = row["tier"] if row and "tier" in row.keys() else "free"
    if user_tier not in ("enterprise", "enterprise_pro", "government") and not current_user.is_subscribed:
        return jsonify({"error": "Enterprise tier required"}), 403

    user_id = current_user.id

    # Org-aware filtering: if org_slug provided, filter by org_id
    org_slug = request.args.get("org_slug")
    org_id = None
    if org_slug:
        org_row = db.execute(
            "SELECT o.id FROM organizations o JOIN org_members m ON o.id = m.org_id "
            "WHERE o.slug = ? AND m.user_id = ?", (org_slug, user_id)
        ).fetchone()
        if org_row:
            org_id = org_row["id"]

    # Latest snapshot (filtered by user or org)
    if org_id:
        snap = db.execute(
            "SELECT * FROM trading_snapshots WHERE org_id = ? ORDER BY id DESC LIMIT 1",
            (org_id,)
        ).fetchone()
        snapshots = db.execute(
            "SELECT total_value_usd, daily_pnl, trades_today, recorded_at FROM trading_snapshots WHERE org_id = ? AND recorded_at >= datetime('now', '-24 hours') ORDER BY recorded_at ASC",
            (org_id,)
        ).fetchall()
    else:
        snap = db.execute(
            "SELECT * FROM trading_snapshots WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (user_id,)
        ).fetchone()
        snapshots = db.execute(
            "SELECT total_value_usd, daily_pnl, trades_today, recorded_at FROM trading_snapshots WHERE user_id = ? AND recorded_at >= datetime('now', '-24 hours') ORDER BY recorded_at ASC",
            (user_id,)
        ).fetchall()

    # Recent signals
    signals = db.execute(
        "SELECT signal_type, target_host, direction, confidence, created_at FROM quant_signals WHERE created_at >= datetime('now', '-1 hour') ORDER BY created_at DESC LIMIT 20"
    ).fetchall()

    holdings = {}
    trades = []
    portfolio_value = 0
    daily_pnl = 0
    trades_today = 0
    trades_total = 0
    initial_value = 13.66  # Starting capital

    if snap:
        portfolio_value = snap["total_value_usd"] or 0
        daily_pnl = snap["daily_pnl"] or 0
        trades_today = snap["trades_today"] or 0
        trades_total = snap["trades_total"] or 0
        if snap["holdings_json"]:
            try:
                holdings = json.loads(snap["holdings_json"])
            except Exception:
                pass
        if snap["trades_json"]:
            try:
                trades = json.loads(snap["trades_json"])
            except Exception:
                pass

    # Snapshot age
    snapshot_age = "no data yet"
    if snap and snap["recorded_at"]:
        try:
            snap_time = datetime.fromisoformat(snap["recorded_at"].replace("Z", "+00:00"))
            if snap_time.tzinfo is None:
                snap_time = snap_time.replace(tzinfo=timezone.utc)
            age_s = (datetime.now(timezone.utc) - snap_time).total_seconds()
            if age_s < 60:
                snapshot_age = f"{int(age_s)}s ago"
            elif age_s < 3600:
                snapshot_age = f"{int(age_s/60)}m ago"
            else:
                snapshot_age = f"{int(age_s/3600)}h ago"
        except Exception:
            pass

    # Pool wallet balances into total
    wallet_total = 0
    wallet_chains = {}
    try:
        import sys as _sys
        _agents = str(BASE_DIR / "agents")
        if _agents not in _sys.path:
            _sys.path.insert(0, _agents)
        from wallet_connector import MultiChainWallet
        _wa = os.environ.get("WALLET_ADDRESS", "")
        _sa = os.environ.get("SOLANA_WALLET_ADDRESS", "")
        if _wa:
            mcw = MultiChainWallet(_wa, ["ethereum", "base", "arbitrum", "polygon"])
            if _sa:
                mcw.add_solana_wallet(_sa)
            wb = mcw.get_all_balances()
            wallet_total = wb.get("total_usd", 0)
            for chain, data in wb.get("chains", {}).items():
                if "error" not in data:
                    wallet_chains[chain] = {
                        "native": data.get("native", {}),
                        "tokens": data.get("tokens", []),
                        "total_usd": data.get("total_usd", 0),
                    }
    except Exception:
        pass

    # Include stuck/in-transit assets from pending_bridges.json
    stuck_total = 0
    in_transit_total = 0
    stuck_assets = []
    bridges_file = BASE_DIR / "agents" / "pending_bridges.json"
    if bridges_file.exists():
        try:
            bridges = json.loads(bridges_file.read_text())
            for b in bridges:
                amt_eth = b.get("amount_eth", 0)
                amt_usd = b.get("amount_usd", 0)
                status = b.get("status", "")
                # Get live ETH price if needed
                if amt_usd == 0 and amt_eth > 0:
                    try:
                        _req = urllib.request.Request(
                            "https://api.coinbase.com/v2/prices/ETH-USD/spot",
                            headers={"User-Agent": "NetTrace/1.0"})
                        with urllib.request.urlopen(_req, timeout=3) as resp:
                            eth_price = float(json.loads(resp.read().decode())["data"]["amount"])
                        amt_usd = amt_eth * eth_price
                    except Exception:
                        pass
                if "loss" in status or "stuck" in status:
                    # Chalked losses: tracked for records but NOT counted in portfolio
                    stuck_total += amt_usd
                    stuck_assets.append({
                        "asset": "ETH", "amount": amt_eth, "value_usd": round(amt_usd, 2),
                        "state": "loss" if "loss" in status else "stuck",
                        "chain": b.get("chain", "unknown"),
                        "note": b.get("note", ""),
                    })
                else:
                    in_transit_total += amt_usd
        except Exception:
            pass

    # Portfolio = liquid only. Losses are tracked separately, NOT counted.
    combined_total = round(portfolio_value + wallet_total + in_transit_total, 2)

    # Claude optimization feed from advanced_team dashboard optimizer
    claude_insights = {}
    try:
        insights_file = BASE_DIR / "agents" / "advanced_team" / "dashboard_insights.json"
        if insights_file.exists():
            claude_insights = json.loads(insights_file.read_text())
    except Exception:
        pass

    # Claude Quant 100 status/feed
    quant100_summary = {}
    quant100_agent = {}
    try:
        q100_results = BASE_DIR / "agents" / "quant_100_results.json"
        if q100_results.exists():
            q100 = json.loads(q100_results.read_text())
            quant100_summary = q100.get("summary", {})
    except Exception:
        pass
    try:
        q100_status = BASE_DIR / "agents" / "quant_100_agent_status.json"
        if q100_status.exists():
            quant100_agent = json.loads(q100_status.read_text())
    except Exception:
        pass

    # Claude staging ingest bundle + stager status
    claude_ingest = {}
    claude_stager = {}
    claude_duplex = {}
    try:
        ingest_file = BASE_DIR / "agents" / "claude_staging" / "claude_ingest_bundle.json"
        if ingest_file.exists():
            ingest = json.loads(ingest_file.read_text())
            claude_ingest = {
                "updated_at": ingest.get("updated_at"),
                "summary": ingest.get("summary", {}),
            }
    except Exception:
        pass
    try:
        stager_file = BASE_DIR / "agents" / "claude_stager_status.json"
        if stager_file.exists():
            claude_stager = json.loads(stager_file.read_text())
    except Exception:
        pass
    try:
        import sys as _sys
        _agents = str(BASE_DIR / "agents")
        if _agents not in _sys.path:
            _sys.path.insert(0, _agents)
        import claude_duplex as _cd
        snap = _cd.get_duplex_snapshot(max_items=20)
        claude_duplex = {
            "to_count": snap.get("to_claude_count", 0),
            "from_count": snap.get("from_claude_count", 0),
            "to_last_id": snap.get("to_claude_last_id", 0),
            "from_last_id": snap.get("from_claude_last_id", 0),
        }
    except Exception:
        pass

    # AmiCoin network (simulation-only; excluded from real holdings/portfolio math)
    amicoin = {
        "summary": {
            "network_name": "AmiCoin Network",
            "network_mode": "simulation_only",
            "excluded_from_real_holdings": True,
            "counts_toward_portfolio": False,
            "go_live_ready": False,
            "go_live_checklist": {},
        },
        "wallets": [],
        "pool": [],
        "excluded_from_portfolio": True,
    }
    amicoin_agent = {}
    try:
        ami_state_file = BASE_DIR / "agents" / "amicoin_state.json"
        if ami_state_file.exists():
            ami_state = json.loads(ami_state_file.read_text())
            network = ami_state.get("network", {})
            reserve = ami_state.get("reserve", {})
            checklist = ami_state.get("go_live_checklist", {})
            coins = ami_state.get("coins", [])
            wallets = ami_state.get("wallets", {})
            stats = ami_state.get("stats", {})

            pool_rows = []
            for c in coins:
                start_px = float(c.get("day_start_price_usd", 0) or 0)
                now_px = float(c.get("price_usd", 0) or 0)
                day_change = ((now_px - start_px) / start_px * 100) if start_px > 0 else 0
                pool_rows.append({
                    "symbol": c.get("symbol", ""),
                    "network": c.get("network", ""),
                    "anchor_pair": c.get("anchor_pair", ""),
                    "price_usd": round(now_px, 6),
                    "day_change_pct": round(day_change, 4),
                    "open_positions": int(c.get("open_positions", 0)),
                    "realized_pnl_usd": round(float(c.get("realized_pnl_usd", 0.0)), 4),
                    "unrealized_pnl_usd": round(float(c.get("unrealized_pnl_usd", 0.0)), 4),
                    "trades": int(c.get("trades", 0)),
                })
            pool_rows.sort(key=lambda x: x["symbol"])

            wallet_rows = []
            for net in ("ethereum", "solana", "bitcoin"):
                w = wallets.get(net, {})
                wallet_rows.append({
                    "network": net,
                    "address": w.get("address", ""),
                    "reserve_usd": round(float(w.get("reserve_usd", 0.0)), 4),
                    "allocated_usd": round(float(w.get("allocated_usd", 0.0)), 4),
                    "realized_pnl_usd": round(float(w.get("realized_pnl_usd", 0.0)), 4),
                })

            amicoin = {
                "summary": {
                    "network_name": network.get("name", "AmiCoin Network"),
                    "network_mode": network.get("mode", "simulation_only"),
                    "excluded_from_real_holdings": bool(network.get("excluded_from_real_holdings", True)),
                    "counts_toward_portfolio": bool(network.get("counts_toward_portfolio", False)),
                    "go_live_ready": bool(network.get("go_live_ready", False)),
                    "go_live_checklist": checklist,
                    "reserve_symbol": reserve.get("symbol", "AMIR"),
                    "liquid_usd": round(float(reserve.get("liquid_usd", 0.0)), 4),
                    "allocated_usd": round(float(reserve.get("allocated_usd", 0.0)), 4),
                    "reserve_equity_usd": round(float(reserve.get("liquid_usd", 0.0)) + float(reserve.get("allocated_usd", 0.0)), 4),
                    "realized_pnl_usd": round(float(reserve.get("realized_pnl_usd", 0.0)), 4),
                    "max_drawdown_pct": round(float(reserve.get("max_drawdown_pct", 0.0)), 4),
                    "risk_lock": bool(reserve.get("risk_lock", False)),
                    "lock_reason": reserve.get("lock_reason", ""),
                    "coins_total": len(pool_rows),
                    "open_positions": sum(int(p.get("open_positions", 0)) for p in pool_rows),
                    "closed_trades": int(stats.get("closed_trades", 0)),
                    "wins": int(stats.get("wins", 0)),
                    "blocked_losses": int(stats.get("blocked_losses", 0)),
                    "cycles": int(stats.get("cycles", 0)),
                },
                "wallets": wallet_rows,
                "pool": pool_rows,
                "excluded_from_portfolio": True,
                "updated_at": ami_state.get("updated_at"),
            }
    except Exception:
        pass
    try:
        ami_agent_file = BASE_DIR / "agents" / "amicoin_agent_status.json"
        if ami_agent_file.exists():
            amicoin_agent = json.loads(ami_agent_file.read_text())
    except Exception:
        pass

    return jsonify({
        "portfolio_value": combined_total,
        "coinbase_value": portfolio_value,
        "wallet_value": round(wallet_total, 2),
        "stuck_value": round(stuck_total, 2),
        "in_transit_value": round(in_transit_total, 2),
        "stuck_assets": stuck_assets,
        "wallet_chains": wallet_chains,
        "daily_pnl": daily_pnl,
        "trades_today": trades_today,
        "trades_total": trades_total,
        "initial_value": initial_value,
        "holdings": holdings,
        "trades": trades,
        "snapshot_age": snapshot_age,
        "evm_address": os.environ.get("WALLET_ADDRESS", ""),
        "solana_address": os.environ.get("SOLANA_WALLET_ADDRESS", ""),
        "snapshots": [{"total_value_usd": s["total_value_usd"], "daily_pnl": s["daily_pnl"],
                        "recorded_at": s["recorded_at"]} for s in snapshots],
        "signals": [{"signal_type": s["signal_type"], "target_host": s["target_host"],
                      "direction": s["direction"], "confidence": s["confidence"]} for s in signals],
        "claude_insights": claude_insights,
        "quant100_summary": quant100_summary,
        "quant100_agent": quant100_agent,
        "claude_ingest": claude_ingest,
        "claude_stager": claude_stager,
        "claude_duplex": claude_duplex,
        "amicoin": amicoin,
        "amicoin_agent": amicoin_agent,
    })


@app.route("/api/amicoin-data")
@login_required
def amicoin_data():
    """AmiCoin simulation feed (always isolated from real holdings)."""
    db = get_db()
    row = db.execute("SELECT tier FROM users WHERE id = ?", (current_user.id,)).fetchone()
    user_tier = row["tier"] if row and "tier" in row.keys() else "free"
    if user_tier not in ("enterprise", "enterprise_pro", "government") and not current_user.is_subscribed:
        return jsonify({"error": "Enterprise tier required"}), 403

    data = {
        "summary": {
            "network_name": "AmiCoin Network",
            "network_mode": "simulation_only",
            "excluded_from_real_holdings": True,
            "counts_toward_portfolio": False,
            "go_live_ready": False,
            "go_live_checklist": {},
        },
        "wallets": [],
        "pool": [],
        "excluded_from_portfolio": True,
    }
    try:
        import sys as _sys
        _agents = str(BASE_DIR / "agents")
        if _agents not in _sys.path:
            _sys.path.insert(0, _agents)
        import amicoin_system as _amc
        data = _amc.get_snapshot()
    except Exception:
        try:
            ami_state_file = BASE_DIR / "agents" / "amicoin_state.json"
            if ami_state_file.exists():
                data = json.loads(ami_state_file.read_text())
        except Exception:
            pass

    agent = {}
    try:
        ami_agent_file = BASE_DIR / "agents" / "amicoin_agent_status.json"
        if ami_agent_file.exists():
            agent = json.loads(ami_agent_file.read_text())
    except Exception:
        pass

    return jsonify({
        "amicoin": data,
        "amicoin_agent": agent,
        "excluded_from_real_holdings": True,
        "counts_toward_portfolio": False,
    })


@app.route("/api/claude-staging")
@login_required
def claude_staging_data():
    """Claude staging snapshot for strategy/framework/message ingest context."""
    db = get_db()
    row = db.execute("SELECT tier FROM users WHERE id = ?", (current_user.id,)).fetchone()
    user_tier = row["tier"] if row and "tier" in row.keys() else "free"
    if user_tier not in ("enterprise", "enterprise_pro", "government") and not current_user.is_subscribed:
        return jsonify({"error": "Enterprise tier required"}), 403

    bundle = {}
    stager = {}
    try:
        ingest_file = BASE_DIR / "agents" / "claude_staging" / "claude_ingest_bundle.json"
        if ingest_file.exists():
            bundle = json.loads(ingest_file.read_text())
    except Exception:
        pass
    try:
        stager_file = BASE_DIR / "agents" / "claude_stager_status.json"
        if stager_file.exists():
            stager = json.loads(stager_file.read_text())
    except Exception:
        pass
    duplex = {}
    try:
        import sys as _sys
        _agents = str(BASE_DIR / "agents")
        if _agents not in _sys.path:
            _sys.path.insert(0, _agents)
        import claude_duplex as _cd
        duplex = _cd.get_duplex_snapshot(max_items=120)
    except Exception:
        pass
    return jsonify({"bundle": bundle, "stager": stager, "duplex": duplex})


@app.route("/api/claude-staging/message", methods=["POST"])
@login_required
def claude_staging_message():
    """Stage operator message for Claude ingestion."""
    db = get_db()
    row = db.execute("SELECT tier FROM users WHERE id = ?", (current_user.id,)).fetchone()
    user_tier = row["tier"] if row and "tier" in row.keys() else "free"
    if user_tier not in ("enterprise", "enterprise_pro", "government") and not current_user.is_subscribed:
        return jsonify({"error": "Enterprise tier required"}), 403

    data = request.get_json() or {}
    message = str(data.get("message", "")).strip()
    if not message:
        return jsonify({"error": "message is required"}), 400
    priority = str(data.get("priority", "normal")).strip().lower()[:20] or "normal"
    category = str(data.get("category", "operator")).strip().lower()[:40] or "operator"

    try:
        import sys as _sys
        _agents = str(BASE_DIR / "agents")
        if _agents not in _sys.path:
            _sys.path.insert(0, _agents)
        import claude_staging as _cs
        import claude_duplex as _cd
        item = _cs.stage_operator_message(
            message,
            category=category,
            priority=priority,
            sender=(current_user.username or "user"),
        )
        duplex = _cd.send_to_claude(
            message,
            msg_type="directive",
            priority=priority,
            source=(current_user.username or "user"),
            meta={"category": category},
        )
        bundle = _cs.build_ingest_bundle(reason="operator_message_api")
        return jsonify({"ok": True, "staged": item, "duplex": duplex, "summary": bundle.get("summary", {})})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/claude-duplex", methods=["GET"])
@login_required
def claude_duplex_get():
    """Read full-duplex Claude channel."""
    db = get_db()
    row = db.execute("SELECT tier FROM users WHERE id = ?", (current_user.id,)).fetchone()
    user_tier = row["tier"] if row and "tier" in row.keys() else "free"
    if user_tier not in ("enterprise", "enterprise_pro", "government") and not current_user.is_subscribed:
        return jsonify({"error": "Enterprise tier required"}), 403

    channel = str(request.args.get("channel", "to_claude")).strip().lower()
    since_id = int(request.args.get("since_id", 0) or 0)
    limit = min(500, max(1, int(request.args.get("limit", 100) or 100)))

    try:
        import sys as _sys
        _agents = str(BASE_DIR / "agents")
        if _agents not in _sys.path:
            _sys.path.insert(0, _agents)
        import claude_duplex as _cd
        if channel == "from_claude":
            rows = _cd.read_from_claude(since_id=since_id, limit=limit)
        elif channel == "both":
            rows = {
                "to_claude": _cd.read_to_claude(since_id=since_id, limit=limit),
                "from_claude": _cd.read_from_claude(since_id=since_id, limit=limit),
            }
            return jsonify({"channel": channel, "messages": rows})
        else:
            rows = _cd.read_to_claude(since_id=since_id, limit=limit)
        return jsonify({"channel": channel, "messages": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/claude-duplex", methods=["POST"])
@login_required
def claude_duplex_post():
    """Post a duplex message to or from Claude."""
    db = get_db()
    row = db.execute("SELECT tier FROM users WHERE id = ?", (current_user.id,)).fetchone()
    user_tier = row["tier"] if row and "tier" in row.keys() else "free"
    if user_tier not in ("enterprise", "enterprise_pro", "government") and not current_user.is_subscribed:
        return jsonify({"error": "Enterprise tier required"}), 403

    data = request.get_json() or {}
    channel = str(data.get("channel", "to_claude")).strip().lower()
    message = str(data.get("message", "")).strip()
    msg_type = str(data.get("msg_type", "directive")).strip().lower()
    priority = str(data.get("priority", "normal")).strip().lower()
    meta = data.get("meta", {})
    if not message:
        return jsonify({"error": "message is required"}), 400

    try:
        import sys as _sys
        _agents = str(BASE_DIR / "agents")
        if _agents not in _sys.path:
            _sys.path.insert(0, _agents)
        import claude_duplex as _cd
        source = current_user.username or "user"
        if channel == "from_claude":
            row = _cd.send_from_claude(
                message, msg_type=msg_type, priority=priority, source=source, meta=meta
            )
        else:
            row = _cd.send_to_claude(
                message, msg_type=msg_type, priority=priority, source=source, meta=meta
            )
        return jsonify({"ok": True, "message": row})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/trading-snapshot", methods=["POST"])
def receive_trading_snapshot():
    """Receive portfolio snapshot from local live trader."""
    # Auth via API key — Bearer/X-Api-Key headers; query param only when explicitly enabled.
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        api_key = auth_header[7:].strip()
    else:
        api_key = request.headers.get("X-Api-Key") or ""
        if not api_key and ALLOW_QUERY_API_KEY_AUTH:
            api_key = request.args.get("api_key") or ""
    expected_key = os.environ.get("NETTRACE_API_KEY", "")
    if not api_key or not expected_key or not hmac.compare_digest(str(api_key), str(expected_key)):
        return jsonify({"error": "Unauthorized"}), 401

    # Route all writes to the primary region (ewr) where the persistent DB lives
    current_region = os.environ.get("FLY_REGION", "")
    primary_region = os.environ.get("PRIMARY_REGION", "ewr")
    if current_region and current_region != primary_region:
        return "", 307, {"fly-replay": f"region={primary_region}"}

    data = request.get_json()
    if not data:
        return jsonify({"error": "No data"}), 400

    # Reject $0 snapshots (likely API timeout — don't pollute dashboard)
    total = data.get("total_value_usd", 0)
    if total is not None and float(total) <= 0:
        return jsonify({"ok": False, "reason": "Rejected $0 snapshot"}), 200

    # Accept user_id in payload (default: 1 for Scott's agents)
    user_id = data.get("user_id", 1)
    # Accept org_id in payload (default: 1 for Scott's org)
    org_id = data.get("org_id", 1)

    db = get_db()
    db.execute(
        """INSERT INTO trading_snapshots
           (user_id, org_id, total_value_usd, daily_pnl, trades_today, trades_total, holdings_json, trades_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (user_id, org_id,
         data.get("total_value_usd", 0),
         data.get("daily_pnl", 0),
         data.get("trades_today", 0),
         data.get("trades_total", 0),
         json.dumps(data.get("holdings", {})),
         json.dumps(data.get("trades", [])))
    )
    db.commit()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Credential encryption (AES-256-GCM via PBKDF2-derived key)
# ---------------------------------------------------------------------------

def _derive_key(password: str, salt: bytes) -> bytes:
    """Derive a 32-byte AES key from password + salt via PBKDF2."""
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000, dklen=32)


def _credential_key_candidates(preferred_password: str | None = None) -> list[str]:
    candidates: list[str] = []
    if preferred_password:
        candidates.append(str(preferred_password))
    primary = str(os.environ.get(CREDENTIAL_KEY_ENV, "")).strip()
    if primary:
        candidates.append(primary)
    secret_key = str(os.environ.get("SECRET_KEY", "")).strip()
    if secret_key:
        candidates.append(secret_key)
    for part in str(os.environ.get(CREDENTIAL_KEY_FALLBACKS_ENV, "")).split(","):
        key = part.strip()
        if key:
            candidates.append(key)
    # Preserve order, drop duplicates.
    unique: list[str] = []
    seen = set()
    for item in candidates:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
    return unique


def encrypt_credential(plaintext: str, password: str | None = None) -> str:
    """Encrypt credential data using AES-256-GCM and a managed key chain."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    key_candidates = _credential_key_candidates(password)
    if not key_candidates:
        raise ValueError(
            "Credential encryption key not configured. Set CREDENTIAL_ENCRYPTION_KEY (preferred) or SECRET_KEY."
        )
    salt = os.urandom(16)
    nonce = os.urandom(12)
    key = _derive_key(key_candidates[0], salt)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext.encode(), None)
    return "v2:" + base64.b64encode(salt + nonce + ciphertext).decode()


def _try_decrypt_aes_payload(raw_payload: bytes, key_candidates: list[str]) -> str | None:
    if len(raw_payload) < 29:
        return None
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    salt = raw_payload[:16]
    nonce = raw_payload[16:28]
    ciphertext = raw_payload[28:]
    for candidate in key_candidates:
        key = _derive_key(candidate, salt)
        try:
            return AESGCM(key).decrypt(nonce, ciphertext, None).decode()
        except Exception:
            continue
    return None


def _try_decrypt_legacy_xor(raw_payload: bytes, key_candidates: list[str]) -> str | None:
    if len(raw_payload) < 33 or not ALLOW_LEGACY_XOR_CREDENTIAL_DECRYPT:
        return None
    salt = raw_payload[:16]
    mac = raw_payload[16:32]
    encrypted = raw_payload[32:]
    for candidate in key_candidates:
        key = _derive_key(candidate, salt)
        expected_mac = hmac.new(key, encrypted, hashlib.sha256).digest()[:16]
        if not hmac.compare_digest(mac, expected_mac):
            continue
        try:
            decrypted = bytes(
                a ^ b for a, b in zip(
                    encrypted,
                    (key * ((len(encrypted) // 32) + 1))[:len(encrypted)]
                )
            )
            return decrypted.decode()
        except Exception:
            continue
    return None


def decrypt_credential(encrypted_b64: str, password: str | None = None) -> str:
    """Decrypt credential data with support for key rotation and legacy payloads."""
    if not encrypted_b64:
        raise ValueError("No credential payload")
    raw_text = encrypted_b64
    version = "legacy"
    if encrypted_b64.startswith("v2:"):
        version = "v2"
        raw_text = encrypted_b64[3:]
    raw_payload = base64.b64decode(raw_text)
    key_candidates = _credential_key_candidates(password)
    if not key_candidates:
        raise ValueError("No key candidates available for credential decryption")

    plaintext = _try_decrypt_aes_payload(raw_payload, key_candidates)
    if plaintext is not None:
        return plaintext
    if version == "v2":
        raise ValueError("Credential decryption failed with configured key set")

    legacy = _try_decrypt_legacy_xor(raw_payload, key_candidates)
    if legacy is not None:
        return legacy
    raise ValueError("Credential decryption failed")


# ---------------------------------------------------------------------------
# Credential management endpoints
# ---------------------------------------------------------------------------

@app.route("/api/credentials", methods=["GET"])
@login_required
def list_credentials():
    """List connected exchanges for current user (no secrets returned)."""
    db = get_db()
    creds = db.execute(
        "SELECT id, exchange, created_at FROM user_credentials WHERE user_id = ? ORDER BY created_at",
        (current_user.id,)
    ).fetchall()
    return jsonify({
        "credentials": [{"id": c["id"], "exchange": c["exchange"],
                          "created_at": c["created_at"]} for c in creds]
    })


@app.route("/api/credentials", methods=["POST"])
@login_required
@require_mfa_for_sensitive
def store_credential():
    """Store encrypted exchange credentials for current user."""
    data = request.get_json() or {}
    if not data:
        return jsonify({"error": "No data"}), 400

    exchange = str(data.get("exchange", "")).strip().lower()
    if exchange not in ("coinbase", "metamask", "wallet"):
        return jsonify({"error": "Unsupported exchange. Use: coinbase, metamask, wallet"}), 400

    credential_json = json.dumps({
        k: v for k, v in data.items() if k not in ("exchange",)
    })

    try:
        encrypted = encrypt_credential(credential_json)
    except Exception as e:
        return jsonify({"error": str(e)}), 503

    db = get_db()
    try:
        existing = db.execute(
            "SELECT id, credential_data FROM user_credentials WHERE user_id = ? AND exchange = ?",
            (current_user.id, exchange),
        ).fetchone()
        if existing:
            db.execute(
                "INSERT INTO user_credentials_history (user_id, exchange, credential_data) VALUES (?, ?, ?)",
                (current_user.id, exchange, existing["credential_data"]),
            )
            db.execute(
                "UPDATE user_credentials SET credential_data = ?, created_at = CURRENT_TIMESTAMP WHERE id = ?",
                (encrypted, existing["id"]),
            )
        else:
            db.execute(
                "INSERT INTO user_credentials (user_id, exchange, credential_data) VALUES (?, ?, ?)",
                (current_user.id, exchange, encrypted),
            )
        db.commit()
    except Exception:
        db.rollback()
        raise
    _record_security_audit(
        "credential_stored",
        user_id=int(current_user.id),
        username=current_user.username,
        details={"exchange": exchange},
    )
    return jsonify({"ok": True, "exchange": exchange})


@app.route("/api/credentials/<int:cred_id>", methods=["DELETE"])
@login_required
@require_mfa_for_sensitive
def delete_credential(cred_id):
    """Delete a stored credential (only own credentials)."""
    db = get_db()
    row = db.execute(
        "SELECT exchange, credential_data FROM user_credentials WHERE id = ? AND user_id = ?",
        (cred_id, current_user.id),
    ).fetchone()
    if row:
        db.execute(
            "INSERT INTO user_credentials_history (user_id, exchange, credential_data) VALUES (?, ?, ?)",
            (current_user.id, row["exchange"], row["credential_data"]),
        )
        db.execute(
            "DELETE FROM user_credentials WHERE id = ? AND user_id = ?",
            (cred_id, current_user.id),
        )
        db.commit()
        _record_security_audit(
            "credential_deleted",
            user_id=int(current_user.id),
            username=current_user.username,
            details={"credential_id": int(cred_id), "exchange": str(row["exchange"])},
        )
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Wallet Balances + Venue Comparison (trading dashboard endpoints)
# ---------------------------------------------------------------------------

@app.route("/api/wallet-balances")
@login_required
def wallet_balances():
    """Return multi-chain wallet balances pooled into a single view."""
    try:
        import sys
        agents_dir = str(BASE_DIR / "agents")
        if agents_dir not in sys.path:
            sys.path.insert(0, agents_dir)
        from wallet_connector import WalletConnector, MultiChainWallet

        wallet_addr = os.environ.get("WALLET_ADDRESS", "")
        solana_addr = os.environ.get("SOLANA_WALLET_ADDRESS", "")
        if not wallet_addr:
            return jsonify({"chains": {}, "total_usd": 0, "error": "No wallet configured"})

        # EVM chains — same address works on all
        evm_chains = ["ethereum", "base", "arbitrum", "polygon"]
        mcw = MultiChainWallet(wallet_addr, evm_chains)

        # Add Solana if configured
        if solana_addr:
            mcw.add_solana_wallet(solana_addr)

        result = mcw.get_all_balances()

        # Flatten for easy display
        chain_details = []
        for chain, data in result.get("chains", {}).items():
            if "error" in data:
                chain_details.append({"chain": chain, "total_usd": 0, "error": data["error"]})
                continue
            entry = {
                "chain": chain,
                "address": solana_addr if chain == "solana" else wallet_addr,
                "native": data.get("native", {}),
                "tokens": data.get("tokens", []),
                "total_usd": data.get("total_usd", 0),
            }
            chain_details.append(entry)

        return jsonify({
            "chains": chain_details,
            "total_usd": result.get("total_usd", 0),
            "evm_address": wallet_addr,
            "solana_address": solana_addr or None,
        })
    except ImportError as e:
        return jsonify({"chains": [], "total_usd": 0, "error": f"wallet_connector not available: {e}"})
    except Exception as e:
        return jsonify({"chains": [], "total_usd": 0, "error": str(e)})


@app.route("/api/asset-pools")
@login_required
def asset_pools():
    """Unified view of ALL assets across all venues with state metadata.

    Returns every asset pool with:
    - state: available | in_transit | stuck | locked | pending | reserved
    - venue: coinbase | base | ethereum | arbitrum | polygon | solana | bridge
    - metadata: ETA, tx_hash, fees, transition history
    """
    import sys as _sys
    _agents = str(BASE_DIR / "agents")
    if _agents not in _sys.path:
        _sys.path.insert(0, _agents)

    pools = []       # All asset entries
    total_available = 0.0
    total_locked = 0.0
    total_in_transit = 0.0
    total_stuck = 0.0
    now = datetime.now(timezone.utc)

    # ── 1. Coinbase holdings (state=available) ──
    try:
        from exchange_connector import CoinbaseTrader
        trader = CoinbaseTrader()
        accts = trader.client.get_accounts()
        for a in accts.get("accounts", []):
            bal = float(a.get("available_balance", {}).get("value", 0))
            if bal <= 0:
                continue
            currency = a.get("currency", "?")
            # Get USD value
            usd_val = bal
            if currency not in ("USD", "USDC"):
                try:
                    import urllib.request as _ur
                    _req = _ur.Request(
                        f"https://api.coinbase.com/v2/prices/{currency}-USD/spot",
                        headers={"User-Agent": "NetTrace/1.0"})
                    with _ur.urlopen(_req, timeout=3) as resp:
                        price = float(json.loads(resp.read().decode())["data"]["amount"])
                    usd_val = bal * price
                except Exception:
                    usd_val = 0
            pools.append({
                "asset": currency,
                "venue": "coinbase",
                "chain": None,
                "amount": round(bal, 8),
                "value_usd": round(usd_val, 4),
                "state": "available",
                "eta_seconds": None,
                "address": None,
                "metadata": {"account_id": a.get("uuid", "")},
            })
            total_available += usd_val
    except Exception as e:
        pools.append({"asset": "ERROR", "venue": "coinbase", "state": "error",
                       "metadata": {"error": str(e)}, "amount": 0, "value_usd": 0})

    # ── 2. On-chain wallet balances (state=available) ──
    try:
        from wallet_connector import MultiChainWallet
        _wa = os.environ.get("WALLET_ADDRESS", "")
        _sa = os.environ.get("SOLANA_WALLET_ADDRESS", "")
        if _wa:
            evm_chains = ["ethereum", "base", "arbitrum", "polygon"]
            mcw = MultiChainWallet(_wa, evm_chains)
            if _sa:
                mcw.add_solana_wallet(_sa)
            wb = mcw.get_all_balances()
            for chain, data in wb.get("chains", {}).items():
                if "error" in data:
                    pools.append({
                        "asset": chain.upper(),
                        "venue": chain,
                        "chain": chain,
                        "amount": 0, "value_usd": 0,
                        "state": "unavailable",
                        "metadata": {"error": data["error"]},
                    })
                    continue
                native = data.get("native", {})
                if native.get("amount", 0) > 0:
                    usd = native.get("usd", 0)
                    pools.append({
                        "asset": native.get("symbol", chain.upper()),
                        "venue": chain,
                        "chain": chain,
                        "amount": round(native["amount"], 8),
                        "value_usd": round(usd, 4),
                        "state": "available",
                        "address": _sa if chain == "solana" else _wa,
                        "metadata": {},
                    })
                    total_available += usd
                for tok in data.get("tokens", []):
                    if tok.get("amount", 0) > 0:
                        pools.append({
                            "asset": tok.get("symbol", "?"),
                            "venue": chain,
                            "chain": chain,
                            "amount": round(tok["amount"], 8),
                            "value_usd": round(tok.get("usd", 0), 4),
                            "state": "available",
                            "address": _sa if chain == "solana" else _wa,
                            "metadata": {"contract": tok.get("contract", "")},
                        })
                        total_available += tok.get("usd", 0)
    except Exception:
        pass

    # ── 3. Pending bridges / in-transit / stuck assets ──
    bridges_file = BASE_DIR / "agents" / "pending_bridges.json"
    if bridges_file.exists():
        try:
            bridges = json.loads(bridges_file.read_text())
            for b in bridges:
                status = b.get("status", "in_transit")
                if "loss" in status:
                    state = "loss"
                elif "stuck" in status:
                    state = "stuck"
                else:
                    state = "in_transit"
                amt_eth = b.get("amount_eth", 0)
                amt_usd = b.get("amount_usd", 0)
                # Try to get current ETH price for stuck assets
                if amt_usd == 0 and amt_eth > 0:
                    try:
                        import urllib.request as _ur
                        _req = _ur.Request(
                            "https://api.coinbase.com/v2/prices/ETH-USD/spot",
                            headers={"User-Agent": "NetTrace/1.0"})
                        with _ur.urlopen(_req, timeout=3) as resp:
                            eth_price = float(json.loads(resp.read().decode())["data"]["amount"])
                        amt_usd = amt_eth * eth_price
                    except Exception:
                        pass
                # ETA calculation
                eta = None
                ts = b.get("timestamp", 0)
                if state == "in_transit" and ts < 9999999999:
                    elapsed = (now - datetime.fromtimestamp(ts, tz=timezone.utc)).total_seconds()
                    # Base L2 bridge typically takes 5-15 min
                    expected_duration = 900  # 15 min
                    eta = max(0, int(expected_duration - elapsed))

                pools.append({
                    "asset": "ETH",
                    "venue": "bridge",
                    "chain": b.get("chain", "unknown"),
                    "amount": round(amt_eth, 8),
                    "value_usd": round(amt_usd, 4),
                    "state": state,
                    "eta_seconds": eta,
                    "address": b.get("address", ""),
                    "tx_hash": b.get("tx_hash", ""),
                    "metadata": {
                        "note": b.get("note", ""),
                        "original_status": status,
                        "timestamp": ts,
                    },
                })
                if state == "stuck":
                    total_stuck += amt_usd
                else:
                    total_in_transit += amt_usd
        except Exception:
            pass

    # ── 4. Recent state transitions (for learning feedback) ──
    transitions = []
    try:
        db = get_db()
        rows = db.execute(
            """SELECT asset, venue, from_state, to_state, amount, value_usd,
                      cost_usd, duration_seconds, trigger, tx_hash, created_at
               FROM asset_state_transitions
               WHERE user_id = ? AND created_at >= datetime('now', '-24 hours')
               ORDER BY created_at DESC LIMIT 50""",
            (current_user.id,)
        ).fetchall()
        transitions = [dict(r) for r in rows]
    except Exception:
        pass

    # ── 5. Learning insights (derived from transitions) ──
    insights = {}
    try:
        db = get_db()
        # Average bridge time
        avg_bridge = db.execute(
            """SELECT AVG(duration_seconds) as avg_time, AVG(cost_usd) as avg_cost
               FROM asset_state_transitions
               WHERE to_state = 'available' AND from_state = 'in_transit'
               AND user_id = ?""",
            (current_user.id,)
        ).fetchone()
        if avg_bridge and avg_bridge["avg_time"]:
            insights["avg_bridge_time_s"] = round(avg_bridge["avg_time"], 1)
            insights["avg_bridge_cost_usd"] = round(avg_bridge["avg_cost"] or 0, 4)
        # State distribution over time
        state_counts = db.execute(
            """SELECT to_state, COUNT(*) as cnt
               FROM asset_state_transitions WHERE user_id = ?
               GROUP BY to_state""",
            (current_user.id,)
        ).fetchall()
        insights["transition_counts"] = {r["to_state"]: r["cnt"] for r in state_counts}
    except Exception:
        pass

    return jsonify({
        "pools": pools,
        "summary": {
            "total_usd": round(total_available + total_locked + total_in_transit + total_stuck, 2),
            "available_usd": round(total_available, 2),
            "in_transit_usd": round(total_in_transit, 2),
            "locked_usd": round(total_locked, 2),
            "stuck_usd": round(total_stuck, 2),
            "pool_count": len(pools),
        },
        "transitions": transitions,
        "learning_insights": insights,
        "updated_at": now.isoformat(),
    })


@app.route("/api/asset-pools/transition", methods=["POST"])
def record_asset_transition():
    """Record an asset state transition (called by agents for learning).

    POST JSON:
      {asset, venue, from_state, to_state, amount, value_usd, cost_usd,
       duration_seconds, trigger, tx_hash, metadata}
    """
    # Auth via API key — Bearer/X-Api-Key headers; query param only when explicitly enabled.
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        api_key = auth_header[7:].strip()
    else:
        api_key = request.headers.get("X-Api-Key") or ""
        if not api_key and ALLOW_QUERY_API_KEY_AUTH:
            api_key = request.args.get("api_key") or ""
    expected_key = os.environ.get("NETTRACE_API_KEY", "")
    if not api_key or not expected_key or not hmac.compare_digest(str(api_key), str(expected_key)):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    if not data or "asset" not in data:
        return jsonify({"error": "asset required"}), 400

    user_id = data.get("user_id", 1)
    db = get_db()
    db.execute(
        """INSERT INTO asset_state_transitions
           (user_id, asset, venue, from_state, to_state, amount, value_usd,
            cost_usd, duration_seconds, trigger, tx_hash, metadata_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (user_id,
         data["asset"],
         data.get("venue", "unknown"),
         data.get("from_state", "unknown"),
         data.get("to_state", "unknown"),
         data.get("amount", 0),
         data.get("value_usd", 0),
         data.get("cost_usd", 0),
         data.get("duration_seconds"),
         data.get("trigger", "agent"),
         data.get("tx_hash"),
         json.dumps(data.get("metadata", {})))
    )
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/venue-comparison")
@login_required
def venue_comparison():
    """Compare prices across CEX (Coinbase) and DEX (Uniswap, Jupiter)."""
    pair = request.args.get("pair", "ETH-USDC")
    amount = float(request.args.get("amount", "0.1"))

    parts = pair.split("-")
    if len(parts) != 2:
        return jsonify({"error": "pair format: TOKEN-TOKEN"}), 400
    token_in, token_out = parts

    venues = []

    # Coinbase price
    try:
        url = f"https://api.coinbase.com/v2/prices/{token_in}-USD/spot"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        coinbase_price = float(data["data"]["amount"])
        venues.append({
            "venue": "Coinbase",
            "price": coinbase_price,
            "amount_out": round(amount * coinbase_price, 6) if token_out in ("USDC", "USD") else round(amount / coinbase_price, 8),
            "fee_pct": 0.4,
            "type": "CEX",
        })
    except Exception as e:
        venues.append({"venue": "Coinbase", "error": str(e), "type": "CEX"})

    # DEX prices via dex_connector (if available)
    try:
        import sys
        agents_dir = str(BASE_DIR / "agents")
        if agents_dir not in sys.path:
            sys.path.insert(0, agents_dir)
        from dex_connector import DEXConnector

        dex = DEXConnector(chain="base")

        # Uniswap quote
        try:
            uni_quote = dex.get_quote_uniswap(token_in, token_out, amount, "base")
            if "error" not in uni_quote:
                venues.append({
                    "venue": "Uniswap (Base)",
                    "price": uni_quote.get("price", 0),
                    "amount_out": uni_quote.get("amount_out", 0),
                    "fee_pct": uni_quote.get("fee_pct", 0.3),
                    "type": "DEX",
                })
            else:
                venues.append({"venue": "Uniswap (Base)", "error": uni_quote["error"], "type": "DEX"})
        except Exception as e:
            venues.append({"venue": "Uniswap (Base)", "error": str(e), "type": "DEX"})

        # Jupiter quote (Solana pairs)
        try:
            from dex_connector import SOLANA_MINTS
            if token_in in SOLANA_MINTS or token_out in SOLANA_MINTS:
                jup_quote = dex.get_quote_jupiter(token_in, token_out, amount)
                if "error" not in jup_quote:
                    venues.append({
                        "venue": "Jupiter (Solana)",
                        "price": jup_quote.get("price", 0),
                        "amount_out": jup_quote.get("amount_out", 0),
                        "fee_pct": jup_quote.get("price_impact_pct", 0),
                        "type": "DEX",
                    })
        except Exception:
            pass

    except ImportError:
        pass

    return jsonify({"pair": pair, "amount": amount, "venues": venues})


# ---------------------------------------------------------------------------
# Wallet Accounts (logical sub-accounts: checking, savings, growth, subsavings)
# ---------------------------------------------------------------------------

@app.route("/api/wallet-accounts")
@login_required
def list_wallet_accounts():
    """List wallet sub-accounts with balances."""
    db = get_db()
    rows = db.execute(
        "SELECT account_type, balance_usd, notes, updated_at FROM wallet_accounts WHERE user_id = ? ORDER BY created_at",
        (current_user.id,)
    ).fetchall()

    # Build response with defaults for any missing account types
    existing = {r["account_type"]: r for r in rows}
    accounts = []
    for acct_type, meta in WALLET_ACCOUNTS.items():
        if acct_type in existing:
            r = existing[acct_type]
            accounts.append({
                "type": acct_type,
                "label": meta["label"],
                "purpose": meta["purpose"],
                "balance_usd": round(r["balance_usd"], 2),
                "notes": r["notes"],
                "updated_at": r["updated_at"],
            })
        else:
            accounts.append({
                "type": acct_type,
                "label": meta["label"],
                "purpose": meta["purpose"],
                "balance_usd": 0.0,
                "notes": None,
                "updated_at": None,
            })

    return jsonify({
        "wallet_address": WALLET_ADDRESS or None,
        "region": FLY_REGION,
        "primary_region": PRIMARY_REGION,
        "accounts": accounts,
        "total_usd": round(sum(a["balance_usd"] for a in accounts), 2),
    })


@app.route("/api/wallet-accounts/init", methods=["POST"])
@login_required
@require_mfa_for_sensitive
def init_wallet_accounts():
    """Initialize all sub-accounts for the current user (idempotent)."""
    db = get_db()
    for acct_type in WALLET_ACCOUNTS:
        db.execute(
            "INSERT OR IGNORE INTO wallet_accounts (user_id, account_type) VALUES (?, ?)",
            (current_user.id, acct_type)
        )
    db.commit()
    _record_security_audit(
        "wallet_accounts_initialized",
        user_id=int(current_user.id),
        username=current_user.username,
    )
    return jsonify({"ok": True, "accounts": list(WALLET_ACCOUNTS.keys())})


@app.route("/api/wallet-accounts/transfer", methods=["POST"])
@login_required
@require_mfa_for_sensitive
@require_primary_region
def transfer_between_accounts():
    """Move money between sub-accounts (checking <-> savings, etc.)."""
    data = request.get_json() or {}
    from_acct = data.get("from", "").strip().lower()
    to_acct = data.get("to", "").strip().lower()
    amount = float(data.get("amount", 0))

    if from_acct not in WALLET_ACCOUNTS or to_acct not in WALLET_ACCOUNTS:
        return jsonify({"error": f"Valid accounts: {', '.join(WALLET_ACCOUNTS.keys())}"}), 400
    if from_acct == to_acct:
        return jsonify({"error": "Cannot transfer to same account"}), 400
    if amount <= 0:
        return jsonify({"error": "Amount must be positive"}), 400

    db = get_db()
    # Check source balance
    src = db.execute(
        "SELECT balance_usd FROM wallet_accounts WHERE user_id = ? AND account_type = ?",
        (current_user.id, from_acct)
    ).fetchone()
    if not src or src["balance_usd"] < amount:
        return jsonify({"error": "Insufficient balance"}), 400

    # Atomic transfer
    db.execute(
        "UPDATE wallet_accounts SET balance_usd = balance_usd - ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND account_type = ?",
        (amount, current_user.id, from_acct)
    )
    db.execute(
        "UPDATE wallet_accounts SET balance_usd = balance_usd + ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND account_type = ?",
        (amount, current_user.id, to_acct)
    )
    db.execute(
        "INSERT INTO wallet_transfers (user_id, from_account, to_account, amount_usd) VALUES (?, ?, ?, ?)",
        (current_user.id, from_acct, to_acct, amount)
    )
    db.commit()
    _record_security_audit(
        "wallet_transfer",
        user_id=int(current_user.id),
        username=current_user.username,
        details={"from": from_acct, "to": to_acct, "amount": float(amount)},
    )

    return jsonify({
        "ok": True,
        "from": from_acct,
        "to": to_acct,
        "amount": amount,
    })


@app.route("/api/wallet-accounts/deposit", methods=["POST"])
@login_required
@require_mfa_for_sensitive
@require_primary_region
def deposit_to_account():
    """Record a deposit into a specific sub-account (from on-chain or external)."""
    data = request.get_json() or {}
    account = data.get("account", "checking").strip().lower()
    amount = float(data.get("amount", 0))

    if account not in WALLET_ACCOUNTS:
        return jsonify({"error": f"Valid accounts: {', '.join(WALLET_ACCOUNTS.keys())}"}), 400
    if amount <= 0:
        return jsonify({"error": "Amount must be positive"}), 400

    db = get_db()
    # Ensure account exists
    db.execute(
        "INSERT OR IGNORE INTO wallet_accounts (user_id, account_type) VALUES (?, ?)",
        (current_user.id, account)
    )
    db.execute(
        "UPDATE wallet_accounts SET balance_usd = balance_usd + ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND account_type = ?",
        (amount, current_user.id, account)
    )
    db.commit()
    _record_security_audit(
        "wallet_deposit_recorded",
        user_id=int(current_user.id),
        username=current_user.username,
        details={"account": account, "amount": float(amount)},
    )

    return jsonify({"ok": True, "account": account, "deposited": amount})


# ---------------------------------------------------------------------------
# Stripe Treasury endpoints
# ---------------------------------------------------------------------------

@app.route("/api/treasury/balance")
@login_required
def treasury_balance():
    """Get treasury account balance for current user."""
    db = get_db()
    acct = db.execute(
        "SELECT stripe_fa_id, balance_cents, yield_earned_cents, status, created_at "
        "FROM treasury_accounts WHERE user_id = ? ORDER BY id DESC LIMIT 1",
        (current_user.id,)
    ).fetchone()

    if not acct:
        return jsonify({
            "has_treasury": False,
            "balance": 0,
            "yield_earned": 0,
            "status": "none",
        })

    # If we have a Stripe FA ID, try to sync balance from Stripe
    if acct["stripe_fa_id"] and stripe.api_key:
        try:
            fa = stripe.treasury.FinancialAccount.retrieve(acct["stripe_fa_id"])
            balance_cents = fa.balance.cash.usd if hasattr(fa, 'balance') else acct["balance_cents"]
            db.execute(
                "UPDATE treasury_accounts SET balance_cents = ? WHERE user_id = ? AND stripe_fa_id = ?",
                (balance_cents, current_user.id, acct["stripe_fa_id"])
            )
            db.commit()
        except Exception:
            balance_cents = acct["balance_cents"]
    else:
        balance_cents = acct["balance_cents"]

    return jsonify({
        "has_treasury": True,
        "balance": round(balance_cents / 100, 2),
        "yield_earned": round(acct["yield_earned_cents"] / 100, 2),
        "status": acct["status"],
        "created_at": acct["created_at"],
    })


@app.route("/api/treasury/create", methods=["POST"])
@login_required
@require_mfa_for_sensitive
def treasury_create():
    """Create a Stripe Treasury financial account."""
    if not stripe.api_key:
        return jsonify({"error": "Stripe not configured"}), 503

    db = get_db()
    existing = db.execute(
        "SELECT id FROM treasury_accounts WHERE user_id = ? AND status != 'closed'",
        (current_user.id,)
    ).fetchone()
    if existing:
        return jsonify({"error": "Treasury account already exists"}), 409

    try:
        # Ensure user has a Stripe customer
        row = db.execute("SELECT stripe_customer_id FROM users WHERE id = ?",
                         (current_user.id,)).fetchone()
        customer_id = row["stripe_customer_id"] if row else None
        if not customer_id:
            customer = stripe.Customer.create(
                metadata={"nettrace_user_id": str(current_user.id)}
            )
            customer_id = customer.id
            db.execute("UPDATE users SET stripe_customer_id = ? WHERE id = ?",
                       (customer_id, current_user.id))

        # Create Financial Account
        fa = stripe.treasury.FinancialAccount.create(
            supported_currencies=["usd"],
            features={
                "card_issuing": {"requested": True},
                "deposit_insurance": {"requested": True},
                "financial_addresses": {"aba": {"requested": True}},
                "inbound_transfers": {"ach": {"requested": True}},
                "outbound_payments": {"ach": {"requested": True}, "us_domestic_wire": {"requested": True}},
                "outbound_transfers": {"ach": {"requested": True}, "us_domestic_wire": {"requested": True}},
            },
        )

        db.execute(
            "INSERT INTO treasury_accounts (user_id, stripe_fa_id, status) VALUES (?, ?, ?)",
            (current_user.id, fa.id, fa.status)
        )
        db.commit()
        _record_security_audit(
            "treasury_account_created",
            user_id=int(current_user.id),
            username=current_user.username,
            details={"stripe_fa_id": str(fa.id), "status": str(fa.status)},
        )

        return jsonify({
            "ok": True,
            "fa_id": fa.id,
            "status": fa.status,
        })
    except stripe.error.StripeError as e:
        return jsonify({"error": f"Stripe error: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/treasury/deploy", methods=["POST"])
@login_required
@require_mfa_for_sensitive
@require_primary_region
def treasury_deploy():
    """Deploy capital from treasury to an exchange or wallet."""
    if not stripe.api_key:
        return jsonify({"error": "Stripe not configured"}), 503

    data = request.get_json() or {}
    amount_usd = data.get("amount", 0)
    destination = data.get("destination", "")  # "coinbase", "wallet"

    if amount_usd <= 0 or amount_usd > 5:
        return jsonify({"error": "Amount must be $0.01-$5.00 (trading rule: max $5/trade)"}), 400
    if destination not in ("coinbase", "wallet"):
        return jsonify({"error": "Destination must be 'coinbase' or 'wallet'"}), 400

    db = get_db()
    acct = db.execute(
        "SELECT stripe_fa_id, balance_cents FROM treasury_accounts WHERE user_id = ? AND status = 'open'",
        (current_user.id,)
    ).fetchone()
    if not acct:
        return jsonify({"error": "No open treasury account"}), 404

    amount_cents = int(amount_usd * 100)
    if amount_cents > acct["balance_cents"]:
        return jsonify({"error": "Insufficient treasury balance"}), 400

    # Record the deployment (actual transfer handled by agent)
    db.execute(
        "UPDATE treasury_accounts SET balance_cents = balance_cents - ? WHERE user_id = ? AND stripe_fa_id = ?",
        (amount_cents, current_user.id, acct["stripe_fa_id"])
    )
    db.commit()
    _record_security_audit(
        "treasury_deploy_recorded",
        user_id=int(current_user.id),
        username=current_user.username,
        details={"amount_usd": float(amount_usd), "destination": destination},
    )

    return jsonify({
        "ok": True,
        "amount": amount_usd,
        "destination": destination,
        "remaining_balance": round((acct["balance_cents"] - amount_cents) / 100, 2),
    })


# ---------------------------------------------------------------------------
# Financial Connections endpoints
# ---------------------------------------------------------------------------

@app.route("/api/fc/link", methods=["POST"])
@login_required
@require_mfa_for_sensitive
def fc_link():
    """Start a Stripe Financial Connections Link session."""
    if not stripe.api_key:
        return jsonify({"error": "Stripe not configured"}), 503

    try:
        # Ensure Stripe customer exists
        db = get_db()
        row = db.execute("SELECT stripe_customer_id FROM users WHERE id = ?",
                         (current_user.id,)).fetchone()
        customer_id = row["stripe_customer_id"] if row else None
        if not customer_id:
            customer = stripe.Customer.create(
                metadata={"nettrace_user_id": str(current_user.id)}
            )
            customer_id = customer.id
            db.execute("UPDATE users SET stripe_customer_id = ? WHERE id = ?",
                       (customer_id, current_user.id))
            db.commit()

        session = stripe.financial_connections.Session.create(
            account_holder={"type": "customer", "customer": customer_id},
            permissions=["balances", "transactions"],
        )
        _record_security_audit(
            "financial_connections_link_started",
            user_id=int(current_user.id),
            username=current_user.username,
            details={"session_id": str(session.id)},
        )

        return jsonify({
            "ok": True,
            "client_secret": session.client_secret,
            "session_id": session.id,
        })
    except stripe.error.StripeError as e:
        return jsonify({"error": f"Stripe error: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/fc/accounts")
@login_required
def fc_accounts():
    """List linked Financial Connections accounts for current user."""
    db = get_db()
    rows = db.execute(
        "SELECT id, fc_account_id, institution, account_name, balance_cents, currency, last_synced_at "
        "FROM financial_connections WHERE user_id = ? ORDER BY created_at",
        (current_user.id,)
    ).fetchall()

    accounts = []
    for r in rows:
        accounts.append({
            "id": r["id"],
            "account_id": r["fc_account_id"],
            "institution": r["institution"],
            "name": r["account_name"],
            "balance": round(r["balance_cents"] / 100, 2),
            "currency": r["currency"],
            "last_synced": r["last_synced_at"],
        })

    return jsonify({"accounts": accounts})


# ---------------------------------------------------------------------------
# Agent Control API (for OpenClaw skills / external automation)
# ---------------------------------------------------------------------------

@app.route("/api/agent-control/status")
@verify_api_key
def agent_control_status():
    """Return current agent runner status. Works with read-only API keys."""
    if agent_runner is None:
        return jsonify({
            "running": False,
            "agents": {},
            "region": None,
            "is_primary": False,
            "uptime_seconds": 0,
            "note": "Agent runner not enabled (ENABLE_AGENTS != 1)",
        })
    try:
        return jsonify(agent_runner.status())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/agent-control/pause", methods=["POST"])
@verify_api_key
@require_write
def agent_control_pause():
    """Pause agent runner via stop(). Requires write-capable key."""
    if agent_runner is None:
        return jsonify({"error": "Agent runner not enabled"}), 404
    try:
        agent_runner.stop()
        return jsonify({"status": "paused", "running": agent_runner.running})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/agent-control/resume", methods=["POST"])
@verify_api_key
@require_write
def agent_control_resume():
    """Resume agent runner via start(). Requires write-capable key."""
    if agent_runner is None:
        return jsonify({"error": "Agent runner not enabled"}), 404
    try:
        agent_runner.start()
        return jsonify({"status": "resumed", "running": agent_runner.running})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/agent-control/portfolio")
@verify_api_key
def agent_control_portfolio():
    """Lightweight portfolio view: holdings + cash from Coinbase. Read-only."""
    import sys as _sys
    _agents = str(BASE_DIR / "agents")
    if _agents not in _sys.path:
        _sys.path.insert(0, _agents)
    try:
        from exchange_connector import CoinbaseTrader
        trader = CoinbaseTrader()
        accts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
        holdings = []
        total_usd = 0.0
        for a in accts.get("accounts", []):
            bal = float(a.get("available_balance", {}).get("value", 0))
            if bal <= 0:
                continue
            currency = a.get("currency", "?")
            usd_val = bal
            if currency not in ("USD", "USDC"):
                try:
                    _req = urllib.request.Request(
                        f"https://api.coinbase.com/v2/prices/{currency}-USD/spot",
                        headers={"User-Agent": "NetTrace/1.0"})
                    with urllib.request.urlopen(_req, timeout=3) as resp:
                        price = float(json.loads(resp.read().decode())["data"]["amount"])
                    usd_val = bal * price
                except Exception:
                    usd_val = 0.0
            total_usd += usd_val
            holdings.append({
                "currency": currency,
                "balance": round(bal, 8),
                "value_usd": round(usd_val, 4),
            })
        return jsonify({
            "venue": "coinbase",
            "holdings": holdings,
            "total_usd": round(total_usd, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        return jsonify({
            "error": f"Coinbase unavailable: {str(e)}",
            "venue": "coinbase",
            "holdings": [],
            "total_usd": 0.0,
        }), 503


@app.route("/api/agent-control/force-scan", methods=["POST"])
@verify_api_key
@require_write
def agent_control_force_scan():
    """Queue an immediate market scan. Not yet implemented."""
    return jsonify({"status": "scan_queued"})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

init_db()

# Migrate: add read_only column to api_keys table
try:
    _mig_db_ro = sqlite3.connect(DB_PATH)
    ensure_read_only_column(_mig_db_ro)
    _mig_db_ro.close()
except Exception:
    pass

# Migrate: add source_region column to existing quant_signals tables
try:
    _mig_db = sqlite3.connect(DB_PATH)
    _mig_db.execute("ALTER TABLE quant_signals ADD COLUMN source_region TEXT")
    _mig_db.commit()
    _mig_db.close()
except sqlite3.OperationalError:
    pass  # column already exists or table doesn't exist yet

# Start continuous scanner
import logging
logging.basicConfig(level=logging.INFO)
from scheduler import ContinuousScanner
scanner = ContinuousScanner(socketio=socketio)
scanner.start()

# Start autonomous agent runner (Fly.io multi-region trading network)
agent_runner = None  # Set below if ENABLE_AGENTS=1
if os.environ.get("ENABLE_AGENTS", "0") == "1":
    try:
        from agents.fly_agent_runner import FlyAgentRunner
        agent_runner = FlyAgentRunner()
        agent_runner.start()
        logging.getLogger("app").info(
            "Agent runner started: region=%s, agents=%s",
            agent_runner.region, list(agent_runner.agents.keys())
        )
    except Exception as _agent_err:
        logging.getLogger("app").error("Failed to start agent runner: %s", _agent_err)

# ---------------------------------------------------------------------------
# D2C Self-Service Platform API
# ---------------------------------------------------------------------------

from api_orgs import RISK_PRESETS

@app.route("/api/v1/signup", methods=["POST"])
def platform_signup():
    """Create user + org + choose tier in one step (D2C onboarding)."""
    data = request.get_json() or {}
    username = str(data.get("username", "")).strip().lower()
    password = str(data.get("password", ""))
    org_name = str(data.get("org_name", "")).strip()
    tier = data.get("tier", "free")

    if not username or len(username) < 3 or len(username) > 30:
        return jsonify({"error": "Username must be 3-30 characters"}), 400
    if not re.match(r'^[a-z0-9_]+$', username):
        return jsonify({"error": "Username: lowercase letters, numbers, underscores only"}), 400
    password_error = _validate_password_strength(password)
    if password_error:
        return jsonify({"error": password_error}), 400
    if tier not in ("free", "pro", "enterprise", "enterprise_pro"):
        return jsonify({"error": "Invalid tier"}), 400

    if not org_name:
        org_name = f"{username}'s Trading"
    slug = re.sub(r'[^a-z0-9\-]', '-', org_name.lower().strip())[:50].strip('-')
    if len(slug) < 3:
        slug = f"{username}-org"

    db = get_db()
    if db.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone():
        return jsonify({"error": "Username already taken"}), 409
    if db.execute("SELECT id FROM organizations WHERE slug=?", (slug,)).fetchone():
        slug = f"{slug}-{secrets.token_hex(3)}"

    # Create user
    pw_hash = generate_password_hash(password)
    cur = db.execute("INSERT INTO users (username, password_hash, tier) VALUES (?, ?, ?)",
                     (username, pw_hash, tier))
    user_id = cur.lastrowid

    # Create org
    cur2 = db.execute(
        "INSERT INTO organizations (name, slug, owner_user_id, tier) VALUES (?, ?, ?, ?)",
        (org_name, slug, user_id, tier)
    )
    org_id = cur2.lastrowid

    # Owner membership + default risk policy
    db.execute("INSERT INTO org_members (org_id, user_id, role) VALUES (?, ?, 'owner')",
               (org_id, user_id))
    db.execute("INSERT INTO org_risk_policies (org_id) VALUES (?)", (org_id,))

    db.commit()

    user = User(user_id, username)
    session.clear()
    login_user(user, fresh=True)
    session.permanent = True

    return jsonify({
        "ok": True,
        "user_id": user_id,
        "username": username,
        "org_id": org_id,
        "org_slug": slug,
        "tier": tier,
        "next_step": "POST /api/v1/onboard to connect your exchange",
    }), 201


@app.route("/api/v1/onboard", methods=["POST"])
@login_required
def platform_onboard():
    """Connect exchange API keys and set risk preferences."""
    data = request.get_json() or {}
    db = get_db()

    # Find user's org
    membership = db.execute(
        "SELECT org_id FROM org_members WHERE user_id = ? AND role = 'owner' LIMIT 1",
        (current_user.id,)
    ).fetchone()
    if not membership:
        return jsonify({"error": "No organization found. Sign up first."}), 404
    org_id = membership["org_id"]

    result = {"org_id": org_id, "steps_completed": []}

    # Step 1: Set risk preferences
    risk_profile = data.get("risk_profile")
    if risk_profile and risk_profile in RISK_PRESETS:
        preset = RISK_PRESETS[risk_profile]
        db.execute(
            "UPDATE org_risk_policies SET risk_profile = ?, max_daily_loss_pct = ?, "
            "max_position_pct = ?, max_trade_usd = ?, min_confidence = ?, "
            "min_confirming_signals = ?, updated_at = CURRENT_TIMESTAMP WHERE org_id = ?",
            (risk_profile, preset["max_daily_loss_pct"], preset["max_position_pct"],
             preset["max_trade_usd"], preset["min_confidence"],
             preset["min_confirming_signals"], org_id)
        )
        result["steps_completed"].append("risk_profile")
        result["risk_profile"] = risk_profile

    # Step 2: Store exchange credentials (if provided)
    exchange = data.get("exchange")
    credential_data = data.get("credential_data")
    if exchange and credential_data:
        db.execute(
            "INSERT OR REPLACE INTO user_credentials (user_id, exchange, credential_data, org_id) "
            "VALUES (?, ?, ?, ?)",
            (current_user.id, exchange.lower(), json.dumps(credential_data), org_id)
        )
        result["steps_completed"].append("exchange_connected")
        result["exchange"] = exchange

    # Step 3: Set allowed trading pairs (if provided)
    allowed_pairs = data.get("allowed_pairs")
    if allowed_pairs:
        db.execute(
            "UPDATE org_risk_policies SET allowed_pairs_json = ? WHERE org_id = ?",
            (json.dumps(allowed_pairs), org_id)
        )
        result["steps_completed"].append("trading_pairs")

    db.commit()

    result["status"] = "onboarding_complete" if len(result["steps_completed"]) >= 2 else "partial"
    return jsonify(result)


@app.route("/api/v1/dashboard")
@login_required
def platform_dashboard():
    """Unified dashboard data: portfolio, P&L, agents, org info."""
    db = get_db()

    # Find user's org
    membership = db.execute(
        "SELECT org_id, role FROM org_members WHERE user_id = ? LIMIT 1",
        (current_user.id,)
    ).fetchone()

    if not membership:
        return jsonify({"error": "No organization. Sign up first."}), 404

    org_id = membership["org_id"]
    org = db.execute("SELECT * FROM organizations WHERE id = ?", (org_id,)).fetchone()

    # Portfolio
    snap = db.execute(
        "SELECT * FROM trading_snapshots WHERE org_id = ? ORDER BY recorded_at DESC LIMIT 1",
        (org_id,)
    ).fetchone()

    # Agents
    agents = db.execute(
        "SELECT id, agent_name, status, pipeline_stage, total_pnl, sharpe_ratio "
        "FROM agent_registrations WHERE org_id = ? AND status != 'fired' "
        "ORDER BY total_pnl DESC",
        (org_id,)
    ).fetchall()

    # Recent proposals
    proposals = db.execute(
        "SELECT pair, direction, confidence, status, created_at "
        "FROM trade_proposals WHERE org_id = ? "
        "ORDER BY created_at DESC LIMIT 10",
        (org_id,)
    ).fetchall()

    # Risk policy
    risk = db.execute(
        "SELECT * FROM org_risk_policies WHERE org_id = ?", (org_id,)
    ).fetchone()

    return jsonify({
        "org": {
            "name": org["name"], "slug": org["slug"],
            "tier": org["tier"], "role": membership["role"],
        },
        "portfolio": {
            "total_value_usd": snap["total_value_usd"] if snap else 0,
            "daily_pnl": snap["daily_pnl"] if snap else 0,
            "trades_today": snap["trades_today"] if snap else 0,
            "holdings": json.loads(snap["holdings_json"] or "{}") if snap else {},
            "last_updated": snap["recorded_at"] if snap else None,
        } if snap else None,
        "agents": [{
            "id": a["id"], "name": a["agent_name"], "status": a["status"],
            "stage": a["pipeline_stage"], "pnl": a["total_pnl"],
            "sharpe": a["sharpe_ratio"],
        } for a in agents],
        "recent_proposals": [{
            "pair": p["pair"], "direction": p["direction"],
            "confidence": p["confidence"], "status": p["status"],
            "created_at": p["created_at"],
        } for p in proposals],
        "risk_policy": {
            "profile": risk["risk_profile"],
            "max_daily_loss_pct": risk["max_daily_loss_pct"],
            "max_position_pct": risk["max_position_pct"],
            "max_trade_usd": risk["max_trade_usd"],
        } if risk else None,
    })


@app.route("/api/v1/risk-preferences", methods=["PUT"])
@login_required
def update_risk_preferences():
    """Update risk profile for the user's organization."""
    data = request.get_json() or {}
    db = get_db()

    membership = db.execute(
        "SELECT org_id, role FROM org_members WHERE user_id = ? AND role IN ('owner', 'admin') LIMIT 1",
        (current_user.id,)
    ).fetchone()
    if not membership:
        return jsonify({"error": "Not authorized"}), 403

    org_id = membership["org_id"]
    profile = data.get("risk_profile")

    if profile not in RISK_PRESETS:
        return jsonify({
            "error": f"Invalid risk_profile. Choose from: {', '.join(RISK_PRESETS)}",
            "presets": {k: v for k, v in RISK_PRESETS.items()},
        }), 400

    preset = RISK_PRESETS[profile]
    db.execute(
        "UPDATE org_risk_policies SET risk_profile = ?, max_daily_loss_pct = ?, "
        "max_position_pct = ?, max_trade_usd = ?, min_confidence = ?, "
        "min_confirming_signals = ?, updated_at = CURRENT_TIMESTAMP WHERE org_id = ?",
        (profile, preset["max_daily_loss_pct"], preset["max_position_pct"],
         preset["max_trade_usd"], preset["min_confidence"],
         preset["min_confirming_signals"], org_id)
    )
    db.commit()

    return jsonify({"status": "updated", "risk_profile": profile, "settings": preset})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 12034))
    print(f">>> NetTrace running at http://localhost:{port}")
    if not stripe.api_key:
        print(">>> WARNING: STRIPE_SECRET_KEY not set - payments disabled")
    if IS_PRODUCTION and MISSING_PERSISTENT_SECRET_KEY:
        print(">>> WARNING: SECRET_KEY not set - session and credential security degraded")
    if not (os.environ.get(CREDENTIAL_KEY_ENV) or os.environ.get("SECRET_KEY")):
        print(">>> WARNING: CREDENTIAL_ENCRYPTION_KEY not set - credential storage endpoints will fail closed")
    socketio.run(app, host="0.0.0.0", port=port, debug=False)
