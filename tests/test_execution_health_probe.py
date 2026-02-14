#!/usr/bin/env python3
"""Tests for execution health probe self-heal helpers."""

from datetime import datetime, timedelta, timezone
import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import execution_health_probe as ehp  # noqa: E402


def test_self_heal_block_reason_flags_stale_status():
    stale_iso = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
    reason = ehp.self_heal_block_reason(
        {
            "updated_at": stale_iso,
            "green": True,
            "reason": "passed",
            "egress_blocked": False,
        },
        require_fresh_within_seconds=120,
        lock_on_not_green=True,
    )
    assert reason.startswith("execution_health_stale:")


def test_run_probe_cycle_attaches_reconcile_refresh(monkeypatch):
    now_iso = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(
        ehp,
        "evaluate_execution_health",
        lambda **_kwargs: {
            "updated_at": now_iso,
            "green": True,
            "reason": "passed",
            "reasons": [],
        },
    )
    monkeypatch.setattr(
        ehp,
        "_run_reconcile_once",
        lambda max_orders, lookback_hours, timeout_seconds: {
            "ok": True,
            "max_orders": int(max_orders),
            "lookback_hours": int(lookback_hours),
            "timeout_seconds": float(timeout_seconds),
        },
    )

    payload = ehp.run_probe_cycle(
        refresh=True,
        probe_http=None,
        run_reconcile=True,
        reconcile_max_orders=11,
        reconcile_lookback_hours=22,
        reconcile_timeout_seconds=33.0,
    )
    assert payload["green"] is True
    assert payload["reconcile_refresh"]["ok"] is True
    assert payload["reconcile_refresh"]["max_orders"] == 11
    assert payload["reconcile_refresh"]["lookback_hours"] == 22


def test_self_heal_block_reason_ignores_non_blocking_reason_prefix(monkeypatch):
    now_iso = datetime.now(timezone.utc).isoformat()
    monkeypatch.setattr(ehp, "SELF_HEAL_NON_BLOCKING_REASON_PREFIXES", ("candle_feed_",))
    reason = ehp.self_heal_block_reason(
        {
            "updated_at": now_iso,
            "green": False,
            "reason": "candle_feed_stale:999s>360s",
            "reasons": ["candle_feed_stale:999s>360s"],
            "egress_blocked": False,
        },
        require_fresh_within_seconds=120,
        lock_on_not_green=True,
    )
    assert reason == ""


def test_apply_fail_safe_trading_lock_sets_and_clears_owned_lock(monkeypatch):
    now_iso = datetime.now(timezone.utc).isoformat()
    state = {"locked": False, "source": "", "reason": ""}
    set_calls = []
    clear_calls = []

    def _read():
        return dict(state)

    def _set(reason, source="system", metadata=None):
        state.update({"locked": True, "source": str(source), "reason": str(reason), "metadata": metadata or {}})
        set_calls.append({"reason": reason, "source": source})
        return dict(state)

    def _clear(source="system", note=""):
        state.update({"locked": False, "source": str(source), "reason": str(note), "metadata": {"cleared": True}})
        clear_calls.append({"source": source, "note": note})
        return dict(state)

    fake_mod = types.SimpleNamespace(
        read_trading_lock=_read,
        set_trading_lock=_set,
        clear_trading_lock=_clear,
    )
    monkeypatch.setitem(sys.modules, "trading_guard", fake_mod)

    blocked = ehp.apply_fail_safe_trading_lock(
        {
            "updated_at": now_iso,
            "green": False,
            "reason": "egress_blocked",
            "egress_blocked": True,
        },
        lock_source="unit.exec_health",
        require_fresh_within_seconds=60,
        lock_on_not_green=True,
        clear_on_recovery=True,
    )
    assert blocked["blocked"] is True
    assert blocked["lock_action"] == "set"
    assert len(set_calls) == 1
    assert state["locked"] is True
    assert state["source"] == "unit.exec_health"

    recovered = ehp.apply_fail_safe_trading_lock(
        {
            "updated_at": now_iso,
            "green": True,
            "reason": "passed",
            "egress_blocked": False,
        },
        lock_source="unit.exec_health",
        require_fresh_within_seconds=60,
        lock_on_not_green=True,
        clear_on_recovery=True,
    )
    assert recovered["blocked"] is False
    assert recovered["lock_action"] == "cleared"
    assert len(clear_calls) == 1
    assert state["locked"] is False
