import json
import os
import re
import subprocess
import sys
from pathlib import Path

from flask import Flask, jsonify, request

app = Flask(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "agents" / "donation_broadcast.py"
OUTBOX_PATH = REPO_ROOT / "agents" / "mcp_global_broadcast_outbox.json"

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


def _parse_assets(raw: str) -> str:
    parts = [p.strip().upper() for p in str(raw or "").split(",") if p.strip()]
    return ",".join(parts) if parts else "BTC,ETH,SOL,USDC"


def _looks_sensitive(value: object) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
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


@app.post("/broadcast")
def broadcast():
    body = request.get_json(silent=True) or {}
    if _contains_sensitive_payload(body):
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
    sender = str(body.get("sender", "render-donation-mcp")).strip() or "render-donation-mcp"
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
        return jsonify({"ok": False, "error": f"Execution error: {exc}"}), 500

    if proc.returncode != 0:
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

    return jsonify(
        {
            "ok": True,
            "message": "Broadcast published to local coordination channels",
            "payload": _redact_sensitive(payload),
        }
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
