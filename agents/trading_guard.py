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
import tempfile
import threading
from contextlib import suppress


_LOCK = threading.Lock()

LOCK_FILE = Path(__file__).parent / "trading_lock.json"


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _write_lock_state(state):
    """Persist lock state atomically using a temp file + rename."""
    payload = json.dumps(state, indent=2).encode("utf-8")
    parent = LOCK_FILE.parent
    if not parent.exists():
        parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="trading_lock_", suffix=".json", dir=str(parent))
    tmp_file = Path(tmp_path)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, LOCK_FILE)
        with suppress(Exception):
            dir_fd = os.open(str(parent), os.O_RDONLY)
            with suppress(Exception):
                os.fsync(dir_fd)
            with suppress(Exception):
                os.close(dir_fd)
    finally:
        if tmp_file.exists():
            with suppress(Exception):
                tmp_file.unlink()


def _is_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # Permission checks can fail in restricted environments even when the
        # process is gone; treat this as unknown rather than definitely alive for
        # stale-owner recovery scenarios.
        return False
    except Exception:
        # Conservatively treat unexpected PID checks as alive to avoid
        # clearing locks that may still be in use.
        return True


def _looks_like_exit_manager_stale_lock(state):
    if not state.get("locked"):
        return False
    reason = str(state.get("reason", "")).lower()
    return "exit_manager_status_stale" in reason


def _looks_like_startup_preflight_lock(state):
    if not state.get("locked"):
        return False
    reason = str(state.get("reason", ""))
    source = str(state.get("source", ""))
    return (
        source == "orchestrator_v2"
        and reason.startswith("Startup preflight blocked:")
    )


def _recover_stale_lock(state):
    pid = state.get("pid")
    if not isinstance(pid, int):
        return state, False
    if not (_looks_like_exit_manager_stale_lock(state) or _looks_like_startup_preflight_lock(state)):
        return state, False
    if _is_pid_alive(pid):
        return state, False

    return (
        {
            "locked": False,
            "reason": f"recovered_stale_dead_owner_lock:{state.get('reason', '')}",
            "source": "trading_guard",
            "set_at": _now_iso(),
            "metadata": state.get("metadata", {}),
            "epoch": time.time(),
            "pid": os.getpid(),
        },
        True,
    )


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
        state = {
            "locked": bool(data.get("locked", False)),
            "reason": str(data.get("reason", "")),
            "source": str(data.get("source", "")),
            "set_at": str(data.get("set_at", "")),
            "epoch": float(data.get("epoch", 0.0)) if isinstance(data.get("epoch"), (int, float)) else None,
            "pid": int(data.get("pid", 0)) if isinstance(data.get("pid"), int) else data.get("pid"),
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

    recovered_state, changed = _recover_stale_lock(state)
    if changed:
        with _LOCK:
            _write_lock_state(recovered_state)
        return recovered_state
    return state


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
    with _LOCK:
        _write_lock_state(state)
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
    with _LOCK:
        _write_lock_state(state)
    return state
