#!/usr/bin/env python3
"""Claude duplex messaging channel.

Provides bidirectional queues:
  - to_claude   : operator/system -> Claude
  - from_claude : Claude -> operator/system
"""

import fcntl
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).parent
STAGING_DIR = BASE_DIR / "claude_staging"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

TO_CLAUDE_FILE = STAGING_DIR / "duplex_to_claude.jsonl"
FROM_CLAUDE_FILE = STAGING_DIR / "duplex_from_claude.jsonl"
COUNTER_FILE = STAGING_DIR / ".duplex_counter"
SCHEMA_VERSION = 1


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _next_id():
    COUNTER_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(COUNTER_FILE, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.seek(0)
            raw = f.read().strip()
            cur = int(raw) if raw else 0
            nxt = cur + 1
            f.seek(0)
            f.truncate()
            f.write(str(nxt))
            f.flush()
            return nxt
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _compute_trace_id(channel, source, msg_type, message, msg_id, timestamp):
    raw = f"{channel}|{source}|{msg_type}|{msg_id}|{timestamp}|{str(message)[:256]}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _normalize_meta(meta):
    if isinstance(meta, dict):
        return dict(meta)
    return {}


def _normalize_row(row):
    if not isinstance(row, dict):
        return {}
    normalized = dict(row)
    normalized["channel"] = str(normalized.get("channel", "") or "")
    normalized["msg_type"] = str(normalized.get("msg_type", "") or "")
    normalized["priority"] = str(normalized.get("priority", "normal") or "normal")
    normalized["source"] = str(normalized.get("source", "unknown") or "unknown")
    normalized["message"] = str(normalized.get("message", "") or "").strip()
    normalized["meta"] = _normalize_meta(normalized.get("meta"))
    msg_id = int(normalized.get("id", 0) or 0)
    timestamp = str(normalized.get("timestamp", "") or "")
    trace_id = (
        str(normalized.get("trace_id", "") or "").strip()
        or str(normalized["meta"].get("trace_id", "") or "").strip()
        or _compute_trace_id(
            normalized["channel"],
            normalized["source"],
            normalized["msg_type"],
            normalized["message"],
            msg_id,
            timestamp,
        )
    )
    normalized["trace_id"] = trace_id
    normalized["schema_version"] = int(normalized.get("schema_version", SCHEMA_VERSION) or SCHEMA_VERSION)
    normalized["status"] = str(normalized.get("status", "queued") or "queued")
    normalized["meta"].setdefault("trace_id", trace_id)
    normalized["meta"].setdefault("schema_version", normalized["schema_version"])
    return normalized


def _append_jsonl(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, separators=(",", ":")) + "\n"
    with open(path, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.write(line)
            f.flush()
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _read_jsonl(path: Path, since_id=0, limit=200):
    if not path.exists():
        return []
    rows = []
    with open(path, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        try:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = _normalize_row(json.loads(line))
                except Exception:
                    continue
                if int(row.get("id", 0)) <= int(since_id or 0):
                    continue
                rows.append(row)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
    rows.sort(key=lambda x: int(x.get("id", 0)))
    if limit and limit > 0:
        rows = rows[-limit:]
    return rows


def _last_id(path: Path):
    if not path.exists():
        return 0
    last = 0
    with open(path, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        try:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                rid = int(row.get("id", 0))
                if rid > last:
                    last = rid
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
    return last


def send_to_claude(message, msg_type="directive", priority="normal", source="operator", meta=None):
    msg_id = _next_id()
    timestamp = _utc_now()
    clean_meta = _normalize_meta(meta)
    trace_id = (
        str(clean_meta.get("trace_id", "") or "").strip()
        or _compute_trace_id("to_claude", source, msg_type, message, msg_id, timestamp)
    )
    clean_meta.setdefault("trace_id", trace_id)
    clean_meta.setdefault("schema_version", SCHEMA_VERSION)
    payload = {
        "id": msg_id,
        "timestamp": timestamp,
        "channel": "to_claude",
        "msg_type": str(msg_type),
        "priority": str(priority),
        "source": str(source),
        "message": str(message).strip(),
        "meta": clean_meta,
        "trace_id": trace_id,
        "schema_version": SCHEMA_VERSION,
        "status": "queued",
    }
    _append_jsonl(TO_CLAUDE_FILE, payload)
    return _normalize_row(payload)


def send_from_claude(message, msg_type="response", priority="normal", source="claude", meta=None):
    msg_id = _next_id()
    timestamp = _utc_now()
    clean_meta = _normalize_meta(meta)
    trace_id = (
        str(clean_meta.get("trace_id", "") or "").strip()
        or _compute_trace_id("from_claude", source, msg_type, message, msg_id, timestamp)
    )
    clean_meta.setdefault("trace_id", trace_id)
    clean_meta.setdefault("schema_version", SCHEMA_VERSION)
    payload = {
        "id": msg_id,
        "timestamp": timestamp,
        "channel": "from_claude",
        "msg_type": str(msg_type),
        "priority": str(priority),
        "source": str(source),
        "message": str(message).strip(),
        "meta": clean_meta,
        "trace_id": trace_id,
        "schema_version": SCHEMA_VERSION,
        "status": "queued",
    }
    _append_jsonl(FROM_CLAUDE_FILE, payload)
    return _normalize_row(payload)


def read_to_claude(since_id=0, limit=200):
    return _read_jsonl(TO_CLAUDE_FILE, since_id=since_id, limit=limit)


def read_from_claude(since_id=0, limit=200):
    return _read_jsonl(FROM_CLAUDE_FILE, since_id=since_id, limit=limit)


def get_duplex_snapshot(max_items=80):
    to_rows = _read_jsonl(TO_CLAUDE_FILE, since_id=0, limit=max_items)
    from_rows = _read_jsonl(FROM_CLAUDE_FILE, since_id=0, limit=max_items)
    return {
        "updated_at": _utc_now(),
        "to_claude_count": len(to_rows),
        "from_claude_count": len(from_rows),
        "to_claude_last_id": _last_id(TO_CLAUDE_FILE),
        "from_claude_last_id": _last_id(FROM_CLAUDE_FILE),
        "to_claude": to_rows,
        "from_claude": from_rows,
    }
