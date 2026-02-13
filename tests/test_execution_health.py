#!/usr/bin/env python3
"""Tests for execution-health gating logic."""

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import execution_health as eh  # noqa: E402


def test_execution_health_green_when_all_components_pass(monkeypatch, tmp_path):
    now_iso = datetime.now(timezone.utc).isoformat()
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": now_iso,
                "summary": {"checked": 4, "early_exit_reason": ""},
            }
        )
    )
    monkeypatch.setattr(eh, "RECON_STATUS_PATH", reconcile)
    monkeypatch.setattr(eh, "DNS_HOSTS", ("api.coinbase.com", "api.exchange.coinbase.com"))
    monkeypatch.setattr(eh, "DNS_REQUIRE_ALL", True)
    monkeypatch.setattr(eh, "MIN_TELEMETRY_SAMPLES", 1)
    monkeypatch.setattr(
        eh,
        "venue_health_snapshot",
        lambda *_args, **_kwargs: {
            "samples": 8,
            "success_rate": 0.95,
            "failure_rate": 0.05,
            "p90_latency_ms": 210.0,
        },
    )
    monkeypatch.setattr(eh, "_dns_probe", lambda host: {"host": host, "ok": True, "ips": ["127.0.0.1"], "error": "", "latency_ms": 1.0})
    monkeypatch.setattr(eh, "_http_probe", lambda url, _timeout: {"url": url, "ok": True, "status": 200, "error": "", "latency_ms": 3.0})
    monkeypatch.setattr(eh, "HTTP_PROBE_URLS", ("https://example.com/ok",))

    payload = eh.evaluate_execution_health(
        refresh=True,
        probe_http=True,
        write_status=False,
        status_path=tmp_path / "health.json",
    )
    assert payload["green"] is True
    assert payload["reason"] == "passed"
    assert payload["dns_degraded"] is False
    assert payload["components"]["dns"]["failover_profile"]["active"] is False


def test_execution_health_fails_when_reconcile_early_exit(monkeypatch, tmp_path):
    now_iso = datetime.now(timezone.utc).isoformat()
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": now_iso,
                "summary": {"checked": 0, "early_exit_reason": "exchange_circuit_open"},
            }
        )
    )
    monkeypatch.setattr(eh, "RECON_STATUS_PATH", reconcile)
    monkeypatch.setattr(eh, "DNS_HOSTS", ("api.coinbase.com",))
    monkeypatch.setattr(eh, "DNS_REQUIRE_ALL", False)
    monkeypatch.setattr(eh, "MIN_TELEMETRY_SAMPLES", 1)
    monkeypatch.setattr(
        eh,
        "venue_health_snapshot",
        lambda *_args, **_kwargs: {
            "samples": 8,
            "success_rate": 0.95,
            "failure_rate": 0.05,
            "p90_latency_ms": 210.0,
        },
    )
    monkeypatch.setattr(eh, "_dns_probe", lambda host: {"host": host, "ok": True, "ips": ["127.0.0.1"], "error": "", "latency_ms": 1.0})

    payload = eh.evaluate_execution_health(
        refresh=True,
        probe_http=False,
        write_status=False,
        status_path=tmp_path / "health.json",
    )
    assert payload["green"] is False
    assert "reconcile_early_exit" in payload["reason"]


def test_execution_health_prioritizes_reconcile_early_exit_over_stale(monkeypatch, tmp_path):
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": "2020-01-01T00:00:00+00:00",
                "summary": {"checked": 0, "early_exit_reason": "exchange_circuit_open"},
            }
        )
    )
    monkeypatch.setattr(eh, "RECON_STATUS_PATH", reconcile)
    monkeypatch.setattr(eh, "DNS_HOSTS", ("api.coinbase.com",))
    monkeypatch.setattr(eh, "DNS_REQUIRE_ALL", False)
    monkeypatch.setattr(eh, "MIN_TELEMETRY_SAMPLES", 1)
    monkeypatch.setattr(
        eh,
        "venue_health_snapshot",
        lambda *_args, **_kwargs: {
            "samples": 8,
            "success_rate": 0.95,
            "failure_rate": 0.05,
            "p90_latency_ms": 210.0,
        },
    )
    monkeypatch.setattr(eh, "_dns_probe", lambda host: {"host": host, "ok": True, "ips": ["127.0.0.1"], "error": "", "latency_ms": 1.0})

    payload = eh.evaluate_execution_health(
        refresh=True,
        probe_http=False,
        write_status=False,
        status_path=tmp_path / "health.json",
    )
    assert payload["green"] is False
    assert "reconcile_early_exit" in payload["reason"]


def test_execution_health_marks_dns_failover_profile_active_when_dns_degraded(monkeypatch, tmp_path):
    now_iso = datetime.now(timezone.utc).isoformat()
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": now_iso,
                "summary": {"checked": 2, "early_exit_reason": ""},
            }
        )
    )
    monkeypatch.setattr(eh, "RECON_STATUS_PATH", reconcile)
    monkeypatch.setattr(eh, "DNS_HOSTS", ("api.coinbase.com",))
    monkeypatch.setattr(eh, "DNS_REQUIRE_ALL", True)
    monkeypatch.setattr(eh, "MIN_TELEMETRY_SAMPLES", 1)
    monkeypatch.setattr(eh, "DNS_FAILOVER_PROFILE", "fallback_then_system")
    monkeypatch.setattr(eh, "DNS_FAILOVER_FALLBACK_IPS", ("203.0.113.9",))
    monkeypatch.setattr(eh, "DNS_FAILOVER_HOST_MAP", {"api.coinbase.com": ["198.51.100.10"]})
    monkeypatch.setattr(
        eh,
        "venue_health_snapshot",
        lambda *_args, **_kwargs: {
            "samples": 5,
            "success_rate": 0.9,
            "failure_rate": 0.1,
            "p90_latency_ms": 200.0,
        },
    )
    monkeypatch.setattr(
        eh,
        "_dns_probe",
        lambda host: {"host": host, "ok": False, "ips": [], "error": "dns down", "latency_ms": 2.0},
    )

    payload = eh.evaluate_execution_health(
        refresh=True,
        probe_http=False,
        write_status=False,
        status_path=tmp_path / "health.json",
    )
    assert payload["green"] is False
    assert payload["reason"] == "dns_unhealthy"
    assert "dns_failover_profile_active" in payload["reasons"]
    assert payload["dns_degraded"] is True
    assert payload["components"]["dns"]["failover_profile"]["active"] is True


def test_execution_health_allows_recovery_override_on_fresh_green_window(monkeypatch, tmp_path):
    now_iso = datetime.now(timezone.utc).isoformat()
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": now_iso,
                "summary": {"checked": 12, "early_exit_reason": ""},
            }
        )
    )
    monkeypatch.setattr(eh, "RECON_STATUS_PATH", reconcile)
    monkeypatch.setattr(eh, "DNS_HOSTS", ("api.coinbase.com",))
    monkeypatch.setattr(eh, "DNS_REQUIRE_ALL", True)
    monkeypatch.setattr(eh, "MIN_TELEMETRY_SAMPLES", 3)
    monkeypatch.setattr(eh, "RECOVERY_WINDOW_MINUTES", 3)
    monkeypatch.setattr(eh, "RECOVERY_MIN_SAMPLES", 2)
    monkeypatch.setattr(eh, "RECOVERY_MIN_SUCCESS_RATE", 0.60)
    monkeypatch.setattr(eh, "RECOVERY_MAX_FAILURE_RATE", 0.40)
    monkeypatch.setattr(eh, "_dns_probe", lambda host: {"host": host, "ok": True, "ips": ["127.0.0.1"], "error": "", "latency_ms": 1.0})

    def _snap(_venue, window_minutes=30):
        if int(window_minutes) <= 3:
            return {
                "samples": 4,
                "success_rate": 1.0,
                "failure_rate": 0.0,
                "p90_latency_ms": 120.0,
            }
        return {
            "samples": 200,
            "success_rate": 0.01,
            "failure_rate": 0.99,
            "p90_latency_ms": 500.0,
        }

    monkeypatch.setattr(eh, "venue_health_snapshot", _snap)
    payload = eh.evaluate_execution_health(
        refresh=True,
        probe_http=False,
        write_status=False,
        status_path=tmp_path / "health.json",
    )
    assert payload["green"] is True
    assert payload["components"]["telemetry"]["recovery_override"] is True


def test_execution_health_treats_no_pending_reconcile_as_healthy(monkeypatch, tmp_path):
    now_iso = datetime.now(timezone.utc).isoformat()
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": now_iso,
                "summary": {
                    "checked": 0,
                    "early_exit_reason": "",
                    "close_gate_passed": True,
                    "close_gate_reason": "no_pending_sell_closes",
                },
            }
        )
    )
    monkeypatch.setattr(eh, "RECON_STATUS_PATH", reconcile)
    monkeypatch.setattr(eh, "DNS_HOSTS", ("api.coinbase.com",))
    monkeypatch.setattr(eh, "DNS_REQUIRE_ALL", True)
    monkeypatch.setattr(eh, "MIN_TELEMETRY_SAMPLES", 1)
    monkeypatch.setattr(
        eh,
        "venue_health_snapshot",
        lambda *_args, **_kwargs: {
            "samples": 8,
            "success_rate": 0.95,
            "failure_rate": 0.05,
            "p90_latency_ms": 210.0,
        },
    )
    monkeypatch.setattr(eh, "_dns_probe", lambda host: {"host": host, "ok": True, "ips": ["127.0.0.1"], "error": "", "latency_ms": 1.0})

    payload = eh.evaluate_execution_health(
        refresh=True,
        probe_http=False,
        write_status=False,
        status_path=tmp_path / "health.json",
    )
    assert payload["green"] is True
