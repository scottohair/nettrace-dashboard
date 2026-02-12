#!/usr/bin/env python3
"""Traceroute Dashboard - Flask app with auth, self-service scanning, and interactive map."""

import json
import subprocess
import re
import os
import sqlite3
import secrets
import time
import urllib.request
import threading
from pathlib import Path
from functools import wraps

from flask import (
    Flask, render_template, request, jsonify, redirect, url_for, flash,
    session, g
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

# Rate limiting: max scans per user
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
    db.close()


# ---------------------------------------------------------------------------
# User model
# ---------------------------------------------------------------------------

class User(UserMixin):
    def __init__(self, id, username):
        self.id = id
        self.username = username


@login_manager.user_loader
def load_user(user_id):
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    row = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    db.close()
    if row:
        return User(row["id"], row["username"])
    return None


# ---------------------------------------------------------------------------
# Traceroute engine
# ---------------------------------------------------------------------------

def run_traceroute(host, max_hops=20):
    """Run traceroute and parse output."""
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
            hops.append({
                "hop": int(m.group(1)),
                "host": m.group(2),
                "ip": m.group(3),
                "rtt_ms": float(m.group(4))
            })
        else:
            m2 = re.match(r'\s*(\d+)\s+(\d+\.\d+\.\d+\.\d+)\s+([\d.]+)\s*ms', line)
            if m2:
                hops.append({
                    "hop": int(m2.group(1)),
                    "host": m2.group(2),
                    "ip": m2.group(2),
                    "rtt_ms": float(m2.group(3))
                })
            else:
                m3 = re.match(r'\s*(\d+)\s+\*', line)
                if m3:
                    hops.append({
                        "hop": int(m3.group(1)),
                        "host": "*",
                        "ip": None,
                        "rtt_ms": None
                    })
    return {"hops": hops}


def geolocate_ip(ip):
    """Get geolocation for an IP."""
    if not ip:
        return None
    # Skip private ranges
    for prefix in ("10.", "192.168.", "172.16.", "172.17.", "172.18.",
                    "172.19.", "172.2", "172.30.", "172.31.", "127."):
        if ip.startswith(prefix):
            return None

    with GEO_LOCK:
        if ip in GEO_CACHE:
            return GEO_CACHE[ip]
    try:
        url = f"http://ip-api.com/json/{ip}?fields=status,country,regionName,city,lat,lon,isp,org,as"
        req = urllib.request.Request(url, headers={"User-Agent": "TracerouteDashboard/1.0"})
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
    """Remove PII from first N hops (local network)."""
    for h in hops:
        if h["hop"] <= sanitize_first_n:
            if h["ip"]:
                h["ip"] = f"10.0.0.{h['hop']}"
                h["host"] = f"hop-{h['hop']}.local"
                h["geo"] = None
    return hops


def execute_scan(scan_id, host, user_id, sid=None):
    """Run a scan in background thread and emit progress via WebSocket."""
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

            # Geolocate
            for i, h in enumerate(hops):
                if h["ip"]:
                    geo = geolocate_ip(h["ip"])
                    h["geo"] = geo
                    time.sleep(0.15)  # rate limit
                else:
                    h["geo"] = None
                if sid and i % 3 == 0:
                    socketio.emit("scan_progress", {
                        "scan_id": scan_id, "hops_done": i + 1,
                        "total_hops": len(hops)
                    }, to=sid)

            # Sanitize first hops
            hops = sanitize_hops(hops)

            total_rtt = None
            for h in reversed(hops):
                if h.get("rtt_ms"):
                    total_rtt = h["rtt_ms"]
                    break

            result = {
                "host": host,
                "hop_count": len(hops),
                "total_rtt": total_rtt,
                "hops": hops
            }

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

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return True


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


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
    existing = db.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
    if existing:
        return jsonify({"error": "Username already taken"}), 409

    pw_hash = generate_password_hash(password)
    cur = db.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)",
                     (username, pw_hash))
    db.commit()

    user = User(cur.lastrowid, username)
    login_user(user)
    return jsonify({"ok": True, "username": username})


@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json()
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")

    db = get_db()
    row = db.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if not row or not check_password_hash(row["password_hash"], password):
        return jsonify({"error": "Invalid credentials"}), 401

    user = User(row["id"], row["username"])
    login_user(user)
    return jsonify({"ok": True, "username": username})


@app.route("/api/logout", methods=["POST"])
@login_required
def logout():
    logout_user()
    return jsonify({"ok": True})


@app.route("/api/me")
def me():
    if current_user.is_authenticated:
        return jsonify({"authenticated": True, "username": current_user.username})
    return jsonify({"authenticated": False})


@app.route("/api/scan", methods=["POST"])
@login_required
def start_scan():
    data = request.get_json()
    host = data.get("host", "").strip()
    name = data.get("name", host).strip()

    # Validate host
    if not host:
        return jsonify({"error": "Host is required"}), 400
    if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9\-\.]+[a-zA-Z0-9]$', host):
        return jsonify({"error": "Invalid hostname"}), 400
    if len(host) > 253:
        return jsonify({"error": "Hostname too long"}), 400

    # Rate limit
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
    ok = execute_scan(scan_id, host, current_user.id, sid=sid)
    if not ok:
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
    scans = []
    for r in rows:
        s = {"id": r["id"], "host": r["target_host"], "name": r["target_name"],
             "category": r["category"], "status": r["status"],
             "created_at": r["created_at"]}
        if r["result_json"]:
            s["result"] = json.loads(r["result_json"])
        scans.append(s)
    return jsonify(scans)


@app.route("/api/scan/<int:scan_id>")
@login_required
def get_scan(scan_id):
    db = get_db()
    row = db.execute(
        "SELECT * FROM scans WHERE id=? AND user_id=?",
        (scan_id, current_user.id)
    ).fetchone()
    if not row:
        return jsonify({"error": "Not found"}), 404
    s = {"id": row["id"], "host": row["target_host"], "name": row["target_name"],
         "status": row["status"], "created_at": row["created_at"]}
    if row["result_json"]:
        s["result"] = json.loads(row["result_json"])
    return jsonify(s)


@app.route("/api/demo")
def demo_data():
    """Return pre-loaded demo traceroute data."""
    if DEMO_RESULTS.exists():
        with open(DEMO_RESULTS) as f:
            return jsonify(json.load(f))
    return jsonify([])


@app.route("/api/community")
def community_scans():
    """Return recent public scans (anonymized)."""
    db_conn = sqlite3.connect(DB_PATH)
    db_conn.row_factory = sqlite3.Row
    rows = db_conn.execute(
        "SELECT target_host, target_name, result_json, created_at "
        "FROM scans WHERE status='completed' ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    db_conn.close()
    scans = []
    for r in rows:
        if r["result_json"]:
            scans.append({
                "host": r["target_host"],
                "name": r["target_name"],
                "result": json.loads(r["result_json"]),
                "created_at": r["created_at"]
            })
    return jsonify(scans)


# WebSocket events
@socketio.on("connect")
def ws_connect():
    emit("connected", {"sid": request.sid})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 12034))
    print(f">>> Traceroute Dashboard running at http://localhost:{port}")
    socketio.run(app, host="0.0.0.0", port=port, debug=False)
