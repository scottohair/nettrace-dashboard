import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict, deque
from pathlib import Path

from flask import Flask, jsonify, request
import psycopg

app = Flask(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "agents" / "donation_broadcast.py"
OUTBOX_PATH = REPO_ROOT / "agents" / "mcp_global_broadcast_outbox.json"
CONTACT_EVENTS_PATH = REPO_ROOT / "agents" / "mcp_donation_contact_events.jsonl"
CONTACT_SUMMARY_PATH = REPO_ROOT / "agents" / "mcp_donation_contact_summary.json"
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

AUTH_TOKEN = os.environ.get("DONATION_BROADCAST_TOKEN", "").strip()
RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("DONATION_RATE_LIMIT_WINDOW_SECONDS", "60"))
RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("DONATION_RATE_LIMIT_MAX_REQUESTS", "30"))
REQUIRE_AUTH = os.environ.get("DONATION_REQUIRE_AUTH", "1").strip() != "0"

ALLOWED_BROADCAST_FIELDS = {"assets", "note", "sender", "pool_id"}

SENSITIVE_FIELD_MARKERS = {
    "private_key",
    "secret",
    "seed",
    "mnemonic",
    "passphrase",
    "api_key",
    "token",
    "auth",
    "credential",
}

HEX_PRIVATE_KEY_RE = re.compile(r"\b(?:0x)?[a-fA-F0-9]{64}\b")
MNEMONIC_RE = re.compile(r"\b(?:[a-z]{3,}\s+){11,23}[a-z]{3,}\b", re.IGNORECASE)
HEX_ADDR_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")

_rate_limit_buckets: dict[str, deque] = defaultdict(deque)


def _db_conn():
    if not DATABASE_URL:
        return None
    return psycopg.connect(DATABASE_URL)


def _init_db() -> None:
    if not DATABASE_URL:
        return
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS donation_contact_events (
                  id BIGSERIAL PRIMARY KEY,
                  ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  event_type TEXT NOT NULL,
                  status TEXT NOT NULL,
                  ip TEXT NOT NULL,
                  sender TEXT NOT NULL,
                  user_agent TEXT NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_donation_contact_events_ts
                ON donation_contact_events (ts DESC);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_donation_contact_events_sender
                ON donation_contact_events (sender);
                """
            )
        conn.commit()


def _parse_assets(raw: str) -> str:
    parts = [p.strip().upper() for p in str(raw or "").split(",") if p.strip()]
    return ",".join(parts) if parts else "BTC,ETH,SOL,USDC"


def _client_ip() -> str:
    forwarded = str(request.headers.get("X-Forwarded-For", "")).strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    return str(request.remote_addr or "unknown")


def _is_authorized() -> bool:
    if not REQUIRE_AUTH:
        return True
    if not AUTH_TOKEN:
        return False
    auth = str(request.headers.get("Authorization", "")).strip()
    if auth.startswith("Bearer "):
        token = auth[7:].strip()
    else:
        token = str(request.headers.get("X-Donation-Token", "")).strip()
    return bool(token) and token == AUTH_TOKEN


def _rate_limit_ok(ip: str) -> bool:
    now = time.time()
    q = _rate_limit_buckets[ip]
    while q and (now - q[0]) > RATE_LIMIT_WINDOW_SECONDS:
        q.popleft()
    if len(q) >= RATE_LIMIT_MAX_REQUESTS:
        return False
    q.append(now)
    return True


def _append_jsonl(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":")) + "\n")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def _read_contact_summary() -> dict:
    if not CONTACT_SUMMARY_PATH.exists():
        return {
            "totals": {"requests": 0, "broadcasts": 0, "rejected": 0},
            "unique_ips": [],
            "unique_senders": [],
            "last_seen_at": None,
        }
    try:
        data = json.loads(CONTACT_SUMMARY_PATH.read_text())
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {
        "totals": {"requests": 0, "broadcasts": 0, "rejected": 0},
        "unique_ips": [],
        "unique_senders": [],
        "last_seen_at": None,
    }


_init_db()


def _track_contact(event_type: str, sender: str = "", status: str = "ok") -> None:
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    ip = _client_ip()
    ua = str(request.headers.get("User-Agent", ""))[:200]
    if DATABASE_URL:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO donation_contact_events
                    (event_type, status, ip, sender, user_agent)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (event_type, status, ip, sender.strip(), ua),
                )
            conn.commit()
        return
    _append_jsonl(
        CONTACT_EVENTS_PATH,
        {
            "ts": now_iso,
            "event_type": event_type,
            "status": status,
            "ip": ip,
            "sender": sender.strip(),
            "ua": ua,
        },
    )
    summary = _read_contact_summary()
    totals = summary.get("totals", {})
    totals["requests"] = int(totals.get("requests", 0)) + 1
    if event_type == "broadcast":
        totals["broadcasts"] = int(totals.get("broadcasts", 0)) + 1
    if status != "ok":
        totals["rejected"] = int(totals.get("rejected", 0)) + 1
    summary["totals"] = totals
    ips = set(summary.get("unique_ips", []) or [])
    ips.add(ip)
    summary["unique_ips"] = sorted(ips)
    senders = set(summary.get("unique_senders", []) or [])
    if sender.strip():
        senders.add(sender.strip())
    summary["unique_senders"] = sorted(senders)
    summary["last_seen_at"] = now_iso
    _write_json(CONTACT_SUMMARY_PATH, summary)


def _looks_sensitive(value: object) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    if HEX_ADDR_RE.search(text):
        return False
    return bool(HEX_PRIVATE_KEY_RE.search(text) or MNEMONIC_RE.search(text))


def _contains_sensitive_payload(obj: object) -> bool:
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = str(k).lower()
            if any(marker in key for marker in SENSITIVE_FIELD_MARKERS):
                return True
            if _contains_sensitive_payload(v):
                return True
        return False
    if isinstance(obj, list):
        return any(_contains_sensitive_payload(v) for v in obj)
    return _looks_sensitive(obj)


def _redact_sensitive(obj: object) -> object:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            key = str(k).lower()
            if any(marker in key for marker in SENSITIVE_FIELD_MARKERS):
                out[k] = "[REDACTED]"
            else:
                out[k] = _redact_sensitive(v)
        return out
    if isinstance(obj, list):
        return [_redact_sensitive(v) for v in obj]
    if isinstance(obj, str) and _looks_sensitive(obj):
        return "[REDACTED]"
    return obj


@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "service": "donation-mcp-broadcast",
        "script_exists": SCRIPT_PATH.exists(),
        "outbox_exists": OUTBOX_PATH.exists(),
        "auth_required": REQUIRE_AUTH,
        "has_auth_token": bool(AUTH_TOKEN),
        "stats_backend": "postgres" if DATABASE_URL else "file",
    })


@app.get("/latest")
def latest():
    if not OUTBOX_PATH.exists():
        return jsonify({"ok": True, "latest": None, "message": "No broadcast yet"})
    try:
        payload = json.loads(OUTBOX_PATH.read_text())
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Failed to parse latest payload: {exc}"}), 500
    return jsonify({"ok": True, "latest": payload})


@app.get("/stats")
def stats():
    if DATABASE_URL:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM donation_contact_events")
                requests_total = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT COUNT(*) FROM donation_contact_events WHERE event_type='broadcast'"
                )
                broadcasts_total = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT COUNT(*) FROM donation_contact_events WHERE status <> 'ok'"
                )
                rejected_total = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(DISTINCT ip) FROM donation_contact_events")
                unique_ips = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT COUNT(DISTINCT sender) FROM donation_contact_events WHERE sender <> ''"
                )
                unique_senders = int(cur.fetchone()[0])
                cur.execute("SELECT MAX(ts) FROM donation_contact_events")
                last_seen = cur.fetchone()[0]
        return jsonify(
            {
                "ok": True,
                "stats": {
                    "totals": {
                        "requests": requests_total,
                        "broadcasts": broadcasts_total,
                        "rejected": rejected_total,
                    },
                    "unique_ip_count": unique_ips,
                    "unique_sender_count": unique_senders,
                    "last_seen_at": last_seen.isoformat() if last_seen else None,
                    "backend": "postgres",
                },
            }
        )
    summary = _read_contact_summary()
    summary["backend"] = "file"
    return jsonify({"ok": True, "stats": summary, "path": str(CONTACT_SUMMARY_PATH)})


@app.post("/broadcast")
def broadcast():
    body = request.get_json(silent=True) or {}
    sender = str(body.get("sender", "render-donation-mcp")).strip() or "render-donation-mcp"
    if not _is_authorized():
        _track_contact(event_type="broadcast", sender=sender, status="unauthorized")
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    if not _rate_limit_ok(_client_ip()):
        _track_contact(event_type="broadcast", sender=sender, status="rate_limited")
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    if not isinstance(body, dict):
        _track_contact(event_type="broadcast", sender=sender, status="invalid_json")
        return jsonify({"ok": False, "error": "invalid_json"}), 400
    unknown_fields = sorted(set(body.keys()) - ALLOWED_BROADCAST_FIELDS)
    if unknown_fields:
        _track_contact(event_type="broadcast", sender=sender, status="unknown_fields")
        return jsonify({"ok": False, "error": "unknown_fields", "fields": unknown_fields}), 400
    if _contains_sensitive_payload(body):
        _track_contact(event_type="broadcast", sender=sender, status="contains_sensitive_material")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Request appears to include sensitive material (private key/secret). Refusing to broadcast.",
                }
            ),
            400,
        )

    assets = _parse_assets(body.get("assets", "BTC,ETH,SOL,USDC"))
    note = str(body.get("note", "")).strip()
    pool_id = str(body.get("pool_id", "global_growth_pool_v1")).strip() or "global_growth_pool_v1"

    cmd = [
        sys.executable,
        str(SCRIPT_PATH),
        "--assets",
        assets,
        "--note",
        note,
        "--sender",
        sender,
        "--pool-id",
        pool_id,
    ]

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except Exception as exc:
        _track_contact(event_type="broadcast", sender=sender, status="execution_error")
        return jsonify({"ok": False, "error": f"Execution error: {exc}"}), 500

    if proc.returncode != 0:
        _track_contact(event_type="broadcast", sender=sender, status="script_error")
        return jsonify(
            {
                "ok": False,
                "returncode": proc.returncode,
                "stderr": proc.stderr[-4000:],
                "stdout": proc.stdout[-4000:],
            }
        ), 500

    payload = None
    try:
        payload = json.loads(proc.stdout)
    except Exception:
        pass
    _track_contact(event_type="broadcast", sender=sender, status="ok")

    return jsonify(
        {
            "ok": True,
            "message": "Broadcast published to local coordination channels",
            "payload": _redact_sensitive(payload),
        }
    )


if __name__ == "__main__":
    _init_db()
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
