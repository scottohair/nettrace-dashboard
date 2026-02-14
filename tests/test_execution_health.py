#!/usr/bin/env python3
"""Tests for execution-health gating logic."""

import json
import os
import socket
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
    monkeypatch.setattr(
        eh,
        "_http_probe",
        lambda url, _timeout, fallback_ips=None: {
            "url": url,
            "ok": True,
            "status": 200,
            "error": "",
            "latency_ms": 3.0,
            "fallback_ip_used": bool(fallback_ips),
        },
    )
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


def test_dns_probe_uses_public_dns_when_system_resolver_fails(monkeypatch):
    monkeypatch.setattr(eh, "PUBLIC_DNS_ENABLED", True)
    monkeypatch.setattr(eh, "PUBLIC_DNS_RESOLVERS", ("1.1.1.1",))
    monkeypatch.setattr(eh, "PUBLIC_DNS_TIMEOUT_SECONDS", 0.1)
    monkeypatch.setattr(eh, "PUBLIC_DNS_MAX_IPS", 4)
    monkeypatch.setattr(eh, "DNS_FAILOVER_HOST_MAP", {})
    monkeypatch.setattr(eh, "DNS_FAILOVER_FALLBACK_IPS", ())

    def _raise_gai(*_args, **_kwargs):
        raise socket.gaierror(8, "nodename nor servname provided")

    monkeypatch.setattr(eh.socket, "getaddrinfo", _raise_gai)
    monkeypatch.setattr(
        eh,
        "resolve_host_via_public_dns",
        lambda **_kwargs: ["203.0.113.8", "203.0.113.9"],
    )

    row = eh._dns_probe("api.coinbase.com")
    assert row["ok"] is True
    assert row["resolver_source"] == "public_dns"
    assert row["ips"] == ["203.0.113.8", "203.0.113.9"]


def test_dns_probe_uses_host_map_when_system_resolver_fails(monkeypatch):
    monkeypatch.setattr(eh, "PUBLIC_DNS_ENABLED", False)
    monkeypatch.setattr(eh, "DNS_FAILOVER_HOST_MAP", {"api.coinbase.com": ["198.51.100.11", "198.51.100.12"]})
    monkeypatch.setattr(eh, "DNS_FAILOVER_FALLBACK_IPS", ())

    def _raise_gai(*_args, **_kwargs):
        raise socket.gaierror(8, "nodename nor servname provided")

    monkeypatch.setattr(eh.socket, "getaddrinfo", _raise_gai)
    row = eh._dns_probe("api.coinbase.com")
    assert row["ok"] is True
    assert row["resolver_source"] == "host_map"
    assert row["ips"] == ["198.51.100.11", "198.51.100.12"]


def test_execution_health_marks_egress_blocked(monkeypatch, tmp_path):
    now_iso = datetime.now(timezone.utc).isoformat()
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": now_iso,
                "summary": {"checked": 3, "early_exit_reason": ""},
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
    monkeypatch.setattr(
        eh,
        "_dns_probe",
        lambda host: {
            "host": host,
            "ok": False,
            "ips": [],
            "error": "[Errno 1] Operation not permitted",
            "resolver_source": "system",
            "latency_ms": 1.0,
        },
    )

    payload = eh.evaluate_execution_health(
        refresh=True,
        probe_http=False,
        write_status=False,
        status_path=tmp_path / "health.json",
    )
    assert payload["green"] is False
    assert payload["egress_blocked"] is True
    assert "egress_blocked" in payload["reasons"]


def test_execution_health_local_test_mode_ignores_egress_and_telemetry_gate(monkeypatch, tmp_path):
    now_iso = datetime.now(timezone.utc).isoformat()
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": now_iso,
                "summary": {"checked": 3, "early_exit_reason": ""},
            }
        )
    )
    monkeypatch.setattr(eh, "RECON_STATUS_PATH", reconcile)
    monkeypatch.setattr(eh, "DNS_HOSTS", ("api.coinbase.com",))
    monkeypatch.setattr(eh, "DNS_REQUIRE_ALL", True)
    monkeypatch.setattr(eh, "MIN_TELEMETRY_SAMPLES", 3)
    monkeypatch.setattr(eh, "LOCAL_TEST_MODE", True)
    monkeypatch.setattr(
        eh,
        "venue_health_snapshot",
        lambda *_args, **_kwargs: {
            "samples": 8,
            "success_rate": 0.01,
            "failure_rate": 0.99,
            "p90_latency_ms": 50.0,
        },
    )
    monkeypatch.setattr(
        eh,
        "_dns_probe",
        lambda host: {
            "host": host,
            "ok": True,
            "ips": ["127.0.0.1"],
            "error": "[Errno 1] Operation not permitted",
            "resolver_source": "system",
            "latency_ms": 1.0,
        },
    )
    monkeypatch.setattr(
        eh,
        "_http_probe",
        lambda url, _timeout, fallback_ips=None: {
            "url": url,
            "ok": False,
            "status": 0,
            "error": "should_not_run_in_local_mode",
            "latency_ms": 3.0,
        },
    )

    payload = eh.evaluate_execution_health(
        refresh=True,
        probe_http=None,
        write_status=False,
        status_path=tmp_path / "health.json",
    )
    assert payload["green"] is True
    assert payload["reason"] == "passed"
    assert payload["components"]["api_probe"]["enabled"] is False
    assert payload["egress_blocked"] is False
    assert payload["components"]["telemetry"]["green"] is True


def test_dns_probe_retries_system_dns_before_fallback(monkeypatch):
    monkeypatch.setattr(eh, "DNS_PROBE_ATTEMPTS", 3)
    monkeypatch.setattr(eh, "DNS_PROBE_RETRY_DELAY_SECONDS", 0.0)
    monkeypatch.setattr(eh, "PUBLIC_DNS_ENABLED", False)
    monkeypatch.setattr(eh, "DNS_FAILOVER_HOST_MAP", {})

    state = {"attempts": 0}

    def _getaddrinfo(*_args, **_kwargs):
        state["attempts"] += 1
        if state["attempts"] == 1:
            raise socket.gaierror(8, "nodename nor servname provided")
        return [(None, None, None, "", ("203.0.113.10", 443))]

    monkeypatch.setattr(eh.socket, "getaddrinfo", _getaddrinfo)
    row = eh._dns_probe("api.coinbase.com")
    assert row["ok"] is True
    assert row["resolver_source"] == "system"
    assert row["ips"] == ["203.0.113.10"]
    assert state["attempts"] == 2


def test_dns_probe_uses_default_host_map_when_fallback_available(monkeypatch):
    monkeypatch.setattr(eh, "DNS_PROBE_ATTEMPTS", 1)
    monkeypatch.setattr(eh, "DNS_PROBE_RETRY_DELAY_SECONDS", 0.0)
    monkeypatch.setattr(eh, "PUBLIC_DNS_ENABLED", False)
    monkeypatch.setattr(eh, "DNS_FAILOVER_HOST_MAP", {})
    monkeypatch.setattr(eh, "DNS_FAILOVER_FALLBACK_IPS", ())
    monkeypatch.setattr(eh, "DNS_FAILOVER_HOST_MAP", {"api.coinbase.com": ["104.18.35.15", "172.64.152.241"]})

    monkeypatch.setattr(
        eh.socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(socket.gaierror(8, "nodename nor servname provided")),
    )

    row = eh._dns_probe("api.coinbase.com")
    assert row["ok"] is True
    assert row["resolver_source"] == "host_map"
    assert row["ips"] == ["104.18.35.15", "172.64.152.241"]


def test_execution_health_allows_stale_no_pending_with_grace(monkeypatch, tmp_path):
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": "2020-01-01T00:00:00+00:00",
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
    monkeypatch.setattr(eh, "RECON_MAX_AGE_SECONDS", 60)
    monkeypatch.setattr(eh, "RECON_ALLOW_STALE_NO_PENDING", True)
    monkeypatch.setattr(eh, "RECON_STALE_NO_PENDING_GRACE_SECONDS", 10**12)
    monkeypatch.setattr(
        eh,
        "venue_health_snapshot",
        lambda *_args, **_kwargs: {
            "samples": 10,
            "success_rate": 0.9,
            "failure_rate": 0.1,
            "p90_latency_ms": 250.0,
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
    assert payload["components"]["reconcile"]["stale_grace_applied"] is True


def test_execution_health_writes_history_rows(monkeypatch, tmp_path):
    now_iso = datetime.now(timezone.utc).isoformat()
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": now_iso,
                "summary": {"checked": 1, "early_exit_reason": "", "close_gate_passed": True, "close_gate_reason": "no_pending_sell_closes"},
            }
        )
    )
    monkeypatch.setattr(eh, "RECON_STATUS_PATH", reconcile)
    monkeypatch.setattr(eh, "HISTORY_PATH", tmp_path / "health_history.jsonl")
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

    _ = eh.evaluate_execution_health(
        refresh=True,
        probe_http=False,
        write_status=True,
        status_path=tmp_path / "health.json",
    )
    lines = (tmp_path / "health_history.jsonl").read_text().splitlines()
    assert len(lines) >= 1


def test_execution_health_candle_feed_gate_passes_when_feed_is_healthy(monkeypatch, tmp_path):
    now_iso = datetime.now(timezone.utc).isoformat()
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": now_iso,
                "summary": {
                    "checked": 1,
                    "early_exit_reason": "",
                    "close_gate_passed": True,
                    "close_gate_reason": "no_pending_sell_closes",
                },
            }
        )
    )
    feed = tmp_path / "candle_feed_latest.json"
    feed.write_text(
        json.dumps(
            {
                "generated_at": now_iso,
                "summary": {
                    "total_points": 1500,
                    "target_points": 1000,
                    "target_achieved": True,
                    "pair_count": 12,
                    "timeframe_count": 4,
                    "latest_ts": int(datetime.now(timezone.utc).timestamp()),
                    "latest_at": now_iso,
                },
                "points": [],
            }
        )
    )
    monkeypatch.setattr(eh, "RECON_STATUS_PATH", reconcile)
    monkeypatch.setattr(eh, "CANDLE_FEED_PATH", feed)
    monkeypatch.setattr(eh, "CANDLE_AGG_STATUS_PATH", tmp_path / "agg_status.json")
    monkeypatch.setattr(eh, "CANDLE_FEED_GATE_ENABLED", True)
    monkeypatch.setattr(eh, "CANDLE_FEED_MIN_POINTS", 1000)
    monkeypatch.setattr(eh, "CANDLE_FEED_MIN_PAIR_COUNT", 6)
    monkeypatch.setattr(eh, "CANDLE_FEED_MIN_TIMEFRAME_COUNT", 2)
    monkeypatch.setattr(eh, "CANDLE_FEED_MAX_AGE_SECONDS", 600)
    monkeypatch.setattr(eh, "CANDLE_FEED_REQUIRE_TARGET_ACHIEVED", True)
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
    monkeypatch.setattr(
        eh,
        "_dns_probe",
        lambda host: {"host": host, "ok": True, "ips": ["127.0.0.1"], "error": "", "latency_ms": 1.0},
    )

    payload = eh.evaluate_execution_health(
        refresh=True,
        probe_http=False,
        write_status=False,
        status_path=tmp_path / "health.json",
    )
    assert payload["green"] is True
    assert payload["components"]["candle_feed"]["green"] is True
    assert payload["components"]["candle_feed"]["reason"] == "candle_feed_healthy"


def test_execution_health_candle_feed_gate_fails_on_low_points(monkeypatch, tmp_path):
    now_iso = datetime.now(timezone.utc).isoformat()
    reconcile = tmp_path / "reconcile_status.json"
    reconcile.write_text(
        json.dumps(
            {
                "updated_at": now_iso,
                "summary": {
                    "checked": 1,
                    "early_exit_reason": "",
                    "close_gate_passed": True,
                    "close_gate_reason": "no_pending_sell_closes",
                },
            }
        )
    )
    feed = tmp_path / "candle_feed_latest.json"
    feed.write_text(
        json.dumps(
            {
                "generated_at": now_iso,
                "summary": {
                    "total_points": 120,
                    "target_points": 1000,
                    "target_achieved": False,
                    "pair_count": 3,
                    "timeframe_count": 1,
                    "latest_ts": int(datetime.now(timezone.utc).timestamp()),
                    "latest_at": now_iso,
                },
                "points": [],
            }
        )
    )
    monkeypatch.setattr(eh, "RECON_STATUS_PATH", reconcile)
    monkeypatch.setattr(eh, "CANDLE_FEED_PATH", feed)
    monkeypatch.setattr(eh, "CANDLE_AGG_STATUS_PATH", tmp_path / "agg_status.json")
    monkeypatch.setattr(eh, "CANDLE_FEED_GATE_ENABLED", True)
    monkeypatch.setattr(eh, "CANDLE_FEED_MIN_POINTS", 1000)
    monkeypatch.setattr(eh, "CANDLE_FEED_MIN_PAIR_COUNT", 6)
    monkeypatch.setattr(eh, "CANDLE_FEED_MIN_TIMEFRAME_COUNT", 2)
    monkeypatch.setattr(eh, "CANDLE_FEED_MAX_AGE_SECONDS", 600)
    monkeypatch.setattr(eh, "CANDLE_FEED_REQUIRE_TARGET_ACHIEVED", True)
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
    monkeypatch.setattr(
        eh,
        "_dns_probe",
        lambda host: {"host": host, "ok": True, "ips": ["127.0.0.1"], "error": "", "latency_ms": 1.0},
    )

    payload = eh.evaluate_execution_health(
        refresh=True,
        probe_http=False,
        write_status=False,
        status_path=tmp_path / "health.json",
    )
    assert payload["green"] is False
    assert payload["components"]["candle_feed"]["green"] is False
    assert "candle_feed_points_low" in payload["reason"]
