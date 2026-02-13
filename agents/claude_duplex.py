#!/usr/bin/env python3
"""Claude duplex messaging channel.

Provides bidirectional queues:
  - to_claude   : operator/system -> Claude
  - from_claude : Claude -> operator/system
"""

import fcntl
import json
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).parent
STAGING_DIR = BASE_DIR / "claude_staging"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

TO_CLAUDE_FILE = STAGING_DIR / "duplex_to_claude.jsonl"
FROM_CLAUDE_FILE = STAGING_DIR / "duplex_from_claude.jsonl"
COUNTER_FILE = STAGING_DIR / ".duplex_counter"


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
                    row = json.loads(line)
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
    payload = {
        "id": _next_id(),
        "timestamp": _utc_now(),
        "channel": "to_claude",
        "msg_type": str(msg_type),
        "priority": str(priority),
        "source": str(source),
        "message": str(message).strip(),
        "meta": meta or {},
    }
    _append_jsonl(TO_CLAUDE_FILE, payload)
    return payload


def send_from_claude(message, msg_type="response", priority="normal", source="claude", meta=None):
    payload = {
        "id": _next_id(),
        "timestamp": _utc_now(),
        "channel": "from_claude",
        "msg_type": str(msg_type),
        "priority": str(priority),
        "source": str(source),
        "message": str(message).strip(),
        "meta": meta or {},
    }
    _append_jsonl(FROM_CLAUDE_FILE, payload)
    return payload


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

