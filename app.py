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
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import stripe
from flask import (
    Flask, render_template, request, jsonify, redirect, url_for, g
)
from flask_login import (
    LoginManager, UserMixin, login_user, logout_user,
    login_required, current_user
)
from flask_socketio import SocketIO, emit
from werkzeug.security import generate_password_hash, check_password_hash

BASE_DIR = Path(__file__).parent
DB_PATH = os.environ.get("DB_PATH", str(BASE_DIR / "traceroute.db"))
DEMO_RESULTS = BASE_DIR / "results.json"

app = Flask(__name__, template_folder=str(BASE_DIR / "templates"),
            static_folder=str(BASE_DIR / "static"))
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))

socketio = SocketIO(app, async_mode="gevent", cors_allowed_origins="*")

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

# Stripe config - all secrets from env vars, nothing hardcoded
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY", "")
STRIPE_PRICE_ID = os.environ.get("STRIPE_PRICE_ID", "")  # $20/mo recurring price
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
COINBASE_COMMERCE_API_KEY = os.environ.get("COINBASE_COMMERCE_API_KEY", "")
APP_URL = os.environ.get("APP_URL", "http://localhost:12034")

# Rate limiting
MAX_SCANS_PER_HOUR = int(os.environ.get("MAX_SCANS_PER_HOUR", "10"))
MAX_CONCURRENT_SCANS = int(os.environ.get("MAX_CONCURRENT_SCANS", "3"))
active_scans = {}
scan_lock = threading.Lock()

GEO_CACHE = {}
GEO_LOCK = threading.Lock()

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


def init_db():
    db = sqlite3.connect(DB_PATH)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            stripe_customer_id TEXT,
            subscription_status TEXT DEFAULT 'none',
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
    """)
    # Migration: add stripe columns if missing
    try:
        db.execute("ALTER TABLE users ADD COLUMN stripe_customer_id TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        db.execute("ALTER TABLE users ADD COLUMN subscription_status TEXT DEFAULT 'none'")
    except sqlite3.OperationalError:
        pass
    try:
        db.execute("ALTER TABLE users ADD COLUMN subscription_expires_at TIMESTAMP")
    except sqlite3.OperationalError:
        pass
    try:
        db.execute("ALTER TABLE users ADD COLUMN payment_method TEXT DEFAULT 'none'")
    except sqlite3.OperationalError:
        pass
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

@app.route("/")
def index():
    return render_template("index.html",
                           stripe_pk=STRIPE_PUBLISHABLE_KEY,
                           price_id=STRIPE_PRICE_ID,
                           stripe_live=bool(stripe.api_key and STRIPE_PRICE_ID),
                           crypto_live=bool(COINBASE_COMMERCE_API_KEY))


@app.route("/api/register", methods=["POST"])
def register():
    data = request.get_json()
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")

    if not username or len(username) < 3 or len(username) > 30:
        return jsonify({"error": "Username must be 3-30 characters"}), 400
    if not re.match(r'^[a-z0-9_]+$', username):
        return jsonify({"error": "Username: lowercase letters, numbers, underscores only"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400

    db = get_db()
    if db.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone():
        return jsonify({"error": "Username already taken"}), 409

    pw_hash = generate_password_hash(password)
    cur = db.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)",
                     (username, pw_hash))
    db.commit()
    user = User(cur.lastrowid, username)
    login_user(user)
    return jsonify({"ok": True, "username": username, "subscribed": False})


@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json()
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")

    db = get_db()
    row = db.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if not row or not check_password_hash(row["password_hash"], password):
        return jsonify({"error": "Invalid credentials"}), 401

    user = User(row["id"], row["username"],
                row["subscription_status"] or "none",
                row["stripe_customer_id"],
                row["payment_method"] if "payment_method" in row.keys() else "none",
                row["subscription_expires_at"] if "subscription_expires_at" in row.keys() else None,
                row["created_at"] if "created_at" in row.keys() else None)
    login_user(user)
    return jsonify({"ok": True, "username": username, "subscribed": user.is_subscribed})


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
        return jsonify({
            "authenticated": True,
            "username": user.username,
            "subscribed": user.is_subscribed,
            "subscription_status": user.subscription_status,
            "payment_method": user.payment_method,
            "created_at": user.created_at,
            "subscription_expires_at": expires if pm == "crypto" else None,
            "has_stripe_billing": bool(cust_id),
        })
    return jsonify({"authenticated": False})


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
    payload = json.loads(request.get_data())
    event_type = payload.get("event", {}).get("type", "")
    data = payload.get("event", {}).get("data", {})

    if event_type == "charge:confirmed":
        user_id = data.get("metadata", {}).get("user_id")
        if user_id:
            expires = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
            db = sqlite3.connect(DB_PATH)
            db.execute(
                "UPDATE users SET subscription_status='active', payment_method='crypto', subscription_expires_at=? WHERE id=?",
                (expires, int(user_id)))
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
    payload = request.get_data()
    sig = request.headers.get("Stripe-Signature")

    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(payload)
    except (ValueError, stripe.error.SignatureVerificationError):
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
        if customer_id and user_id:
            db = sqlite3.connect(DB_PATH)
            db.execute("UPDATE users SET stripe_customer_id=?, subscription_status='active', payment_method='stripe', subscription_expires_at=NULL WHERE id=?",
                       (customer_id, int(user_id)))
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
# Main
# ---------------------------------------------------------------------------

init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 12034))
    print(f">>> NetTrace running at http://localhost:{port}")
    if not stripe.api_key:
        print(">>> WARNING: STRIPE_SECRET_KEY not set - payments disabled")
    socketio.run(app, host="0.0.0.0", port=port, debug=False)
