#!/usr/bin/env python3
"""Tests for execution telemetry endpoint filtering."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import execution_telemetry as et


def _seed_api_calls():
    calls = [
        ("/api/v3/brokerage/orders", True),
        ("/api/v3/brokerage/orders/historical/abc-123", False),
        ("/products/BTC-USD/book", False),
    ]
    for endpoint, ok in calls:
        et.record_api_call(
            venue="coinbase",
            method="GET",
            endpoint=endpoint,
            latency_ms=12.5,
            ok=ok,
            status_code=(200 if ok else 500),
            error_text="" if ok else "api_error",
        )


def test_venue_health_snapshot_filters_by_include_and_exclude_prefixes(tmp_path, monkeypatch):
    monkeypatch.setattr(et, "DB_PATH", tmp_path / "execution_telemetry.db")
    _seed_api_calls()

    default = et.venue_health_snapshot("coinbase", window_minutes=60)
    assert default["samples"] == 3
    assert default["success_rate"] == 0.3333

    orders_only = et.venue_health_snapshot(
        "coinbase",
        window_minutes=60,
        include_endpoint_prefixes=("/api/v3/brokerage/orders",),
    )
    assert orders_only["samples"] == 2
    assert orders_only["success_rate"] == 0.5

    orders_non_history = et.venue_health_snapshot(
        "coinbase",
        window_minutes=60,
        include_endpoint_prefixes=("/api/v3/brokerage/orders",),
        exclude_endpoint_prefixes=("/api/v3/brokerage/orders/historical",),
    )
    assert orders_non_history["samples"] == 1
    assert orders_non_history["success_rate"] == 1.0


def test_endpoint_latency_ms_with_default_query_and_filters(tmp_path, monkeypatch):
    monkeypatch.setattr(et, "DB_PATH", tmp_path / "execution_telemetry.db")
    _seed_api_calls()

    p90 = et.endpoint_latency_ms(
        "coinbase",
        endpoint_prefix="",
        window_minutes=60,
        pct=90,
        include_endpoint_prefixes=("/api/v3/brokerage/orders",),
    )
    assert p90 == 12.5
