#!/usr/bin/env python3
"""Global trading guardrails shared across all agents.

This module provides a process-safe, file-backed kill switch so risk systems can
atomically halt all order placement across independent agent processes.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

LOCK_FILE = Path(__file__).parent / "trading_lock.json"


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def read_trading_lock():
    """Return the persisted lock state dict."""
    if not LOCK_FILE.exists():
        return {
            "locked": False,
            "reason": "",
            "source": "",
            "set_at": "",
            "metadata": {},
        }
    try:
        data = json.loads(LOCK_FILE.read_text())
        if not isinstance(data, dict):
            raise ValueError("lock file must contain a JSON object")
        return {
            "locked": bool(data.get("locked", False)),
            "reason": str(data.get("reason", "")),
            "source": str(data.get("source", "")),
            "set_at": str(data.get("set_at", "")),
            "metadata": data.get("metadata", {}) if isinstance(data.get("metadata", {}), dict) else {},
        }
    except Exception:
        # Corrupt lock file defaults to safe mode (locked).
        return {
            "locked": True,
            "reason": "Lock file unreadable/corrupt",
            "source": "trading_guard",
            "set_at": _now_iso(),
            "metadata": {},
        }


def is_trading_locked():
    """Return (locked: bool, reason: str, source: str)."""
    state = read_trading_lock()
    return state["locked"], state.get("reason", ""), state.get("source", "")


def set_trading_lock(reason, source="system", metadata=None):
    """Activate global trading lock."""
    state = {
        "locked": True,
        "reason": str(reason or "manual lock"),
        "source": str(source or "system"),
        "set_at": _now_iso(),
        "metadata": metadata if isinstance(metadata, dict) else {},
        "epoch": time.time(),
        "pid": os.getpid(),
    }
    LOCK_FILE.write_text(json.dumps(state, indent=2))
    return state


def clear_trading_lock(source="system", note=""):
    """Clear global trading lock."""
    state = {
        "locked": False,
        "reason": str(note or ""),
        "source": str(source or "system"),
        "set_at": _now_iso(),
        "metadata": {"cleared": True},
        "epoch": time.time(),
        "pid": os.getpid(),
    }
    LOCK_FILE.write_text(json.dumps(state, indent=2))
    return state

