#!/usr/bin/env python3
"""Execution health checks for venue DNS/API/reconciliation readiness."""

import json
import os
import socket
import time
import urllib.request
from urllib.parse import urlparse
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).parent
STATUS_PATH = BASE / "execution_health_status.json"
RECON_STATUS_PATH = BASE / "reconcile_agent_trades_status.json"

DNS_HOSTS = tuple(
    h.strip().lower()
    for h in str(
        os.environ.get(
            "EXEC_HEALTH_DNS_HOSTS",
            "api.coinbase.com,api.exchange.coinbase.com",
        )
    ).split(",")
    if h.strip()
)
DNS_REQUIRE_ALL = os.environ.get("EXEC_HEALTH_DNS_REQUIRE_ALL", "1").lower() not in ("0", "false", "no")

MIN_TELEMETRY_SAMPLES = int(os.environ.get("EXEC_HEALTH_MIN_TELEMETRY_SAMPLES", "3"))
MIN_SUCCESS_RATE = float(os.environ.get("EXEC_HEALTH_MIN_SUCCESS_RATE", "0.55"))
MAX_FAILURE_RATE = float(os.environ.get("EXEC_HEALTH_MAX_FAILURE_RATE", "0.45"))
MAX_P90_LATENCY_MS = float(os.environ.get("EXEC_HEALTH_MAX_P90_LATENCY_MS", "4500"))
RECOVERY_WINDOW_MINUTES = int(os.environ.get("EXEC_HEALTH_RECOVERY_WINDOW_MINUTES", "3"))
RECOVERY_MIN_SAMPLES = int(os.environ.get("EXEC_HEALTH_RECOVERY_MIN_SAMPLES", "2"))
RECOVERY_MIN_SUCCESS_RATE = float(os.environ.get("EXEC_HEALTH_RECOVERY_MIN_SUCCESS_RATE", "0.60"))
RECOVERY_MAX_FAILURE_RATE = float(os.environ.get("EXEC_HEALTH_RECOVERY_MAX_FAILURE_RATE", "0.40"))

RECON_MAX_AGE_SECONDS = int(os.environ.get("EXEC_HEALTH_RECON_MAX_AGE_SECONDS", "900"))
HEALTH_CACHE_MAX_AGE_SECONDS = int(os.environ.get("EXEC_HEALTH_CACHE_MAX_AGE_SECONDS", "120"))

HTTP_PROBE_ENABLED = os.environ.get("EXEC_HEALTH_HTTP_PROBE_ENABLED", "1").lower() not in ("0", "false", "no")
HTTP_PROBE_TIMEOUT_SECONDS = float(os.environ.get("EXEC_HEALTH_HTTP_PROBE_TIMEOUT_SECONDS", "4.0"))
HTTP_PROBE_URLS = tuple(
    u.strip()
    for u in str(
        os.environ.get(
            "EXEC_HEALTH_HTTP_PROBE_URLS",
            "https://api.coinbase.com/v2/prices/BTC-USD/spot,"
            "https://api.exchange.coinbase.com/products/BTC-USD/book?level=1",
        )
    ).split(",")
    if u.strip()
)


def _parse_ip_list(value):
    items = str(value or "").split(",")
    ordered = []
    seen = set()
    for item in items:
        ip = str(item or "").strip()
        if not ip or ip in seen:
            continue
        seen.add(ip)
        ordered.append(ip)
    return tuple(ordered)


def _load_json_dict_env(name):
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


DNS_FAILOVER_PROFILE = str(
    os.environ.get("COINBASE_DNS_FAILOVER_PROFILE", "system_then_fallback")
).strip().lower()
DNS_FAILOVER_FALLBACK_IPS = _parse_ip_list(os.environ.get("COINBASE_DNS_FALLBACK_IPS", ""))
DNS_FAILOVER_HOST_MAP = _load_json_dict_env("COINBASE_DNS_FAILOVER_HOST_MAP_JSON")

try:
    from execution_telemetry import venue_health_snapshot
except Exception:
    try:
        from agents.execution_telemetry import venue_health_snapshot  # type: ignore
    except Exception:
        def venue_health_snapshot(*_args, **_kwargs):
            return {}

try:
    from execution_telemetry import record_api_call
except Exception:
    try:
        from agents.execution_telemetry import record_api_call  # type: ignore
    except Exception:
        def record_api_call(*_args, **_kwargs):
            return None


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _load_json(path, default):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except Exception:
        return default


def _iso_age_seconds(ts_text):
    text = str(ts_text or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())
    except Exception:
        return None


def _dns_probe(host):
    started = time.perf_counter()
    ips = []
    error = ""
    try:
        infos = socket.getaddrinfo(str(host), 443, type=socket.SOCK_STREAM)
        seen = set()
        for info in infos:
            addr = info[4][0] if info and len(info) >= 5 and info[4] else ""
            if addr and addr not in seen:
                seen.add(addr)
                ips.append(addr)
    except Exception as e:
        error = str(e)
    return {
        "host": str(host),
        "ok": bool(ips),
        "ips": ips[:8],
        "error": error,
        "latency_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }


def _http_probe(url, timeout_seconds):
    started = time.perf_counter()
    status = 0
    error = ""
    ok = False
    try:
        req = urllib.request.Request(str(url), headers={"User-Agent": "NetTrace-ExecHealth/1.0"})
        with urllib.request.urlopen(req, timeout=max(0.5, float(timeout_seconds))) as resp:
            status = int(getattr(resp, "status", 200) or 200)
            _ = resp.read(128)
        ok = 200 <= status < 300
    except Exception as e:
        error = str(e)
    try:
        endpoint = str(urlparse(str(url)).path or "/")
        record_api_call(
            venue="coinbase",
            method="GET",
            endpoint=endpoint,
            latency_ms=round((time.perf_counter() - started) * 1000.0, 3),
            ok=bool(ok),
            status_code=(int(status) if status else None),
            error_text=error,
            context={"source": "execution_health_probe"},
        )
    except Exception:
        pass
    return {
        "url": str(url),
        "ok": bool(ok),
        "status": int(status),
        "error": error,
        "latency_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }


def evaluate_execution_health(refresh=True, probe_http=None, write_status=True, status_path=STATUS_PATH):
    """Evaluate venue execution health and optionally persist status."""
    path = Path(status_path)
    if not refresh:
        cached = _load_json(path, {})
        if isinstance(cached, dict) and cached:
            age = _iso_age_seconds(cached.get("updated_at"))
            if age is not None and age <= max(5, int(HEALTH_CACHE_MAX_AGE_SECONDS)):
                return cached

    dns_rows = [_dns_probe(h) for h in DNS_HOSTS]
    dns_ok_count = sum(1 for r in dns_rows if bool(r.get("ok")))
    dns_green = (dns_ok_count == len(dns_rows)) if DNS_REQUIRE_ALL else (dns_ok_count > 0)
    if not dns_rows:
        dns_green = False
    dns_degraded = not bool(dns_green)
    failover_hosts = sorted(
        str(k).strip().lower()
        for k, v in DNS_FAILOVER_HOST_MAP.items()
        if str(k).strip() and v
    )
    failover_configured = bool(
        DNS_FAILOVER_PROFILE not in ("", "disabled", "off", "none")
        and (DNS_FAILOVER_FALLBACK_IPS or failover_hosts)
    )
    failover_active = bool(dns_degraded and failover_configured)

    telemetry = venue_health_snapshot("coinbase", window_minutes=30) or {}
    telemetry_samples = int(telemetry.get("samples", 0) or 0)
    telemetry_success_rate = float(telemetry.get("success_rate", 0.0) or 0.0)
    telemetry_failure_rate = float(telemetry.get("failure_rate", 0.0) or 0.0)
    telemetry_p90 = float(telemetry.get("p90_latency_ms", 0.0) or 0.0)

    telemetry_green = (
        telemetry_samples >= int(MIN_TELEMETRY_SAMPLES)
        and telemetry_success_rate >= float(MIN_SUCCESS_RATE)
        and telemetry_failure_rate <= float(MAX_FAILURE_RATE)
        and telemetry_p90 <= float(MAX_P90_LATENCY_MS)
    )

    do_probe = HTTP_PROBE_ENABLED if probe_http is None else bool(probe_http)
    probe_rows = []
    if do_probe:
        probe_rows = [_http_probe(u, HTTP_PROBE_TIMEOUT_SECONDS) for u in HTTP_PROBE_URLS]
    probe_green = True
    if do_probe:
        probe_green = bool(probe_rows) and all(bool(r.get("ok")) for r in probe_rows)

    reconcile_payload = _load_json(RECON_STATUS_PATH, {})
    reconcile_summary = (
        reconcile_payload.get("summary", {})
        if isinstance(reconcile_payload, dict) and isinstance(reconcile_payload.get("summary"), dict)
        else {}
    )
    reconcile_age = _iso_age_seconds(reconcile_payload.get("updated_at")) if isinstance(reconcile_payload, dict) else None
    reconcile_early_exit = str(reconcile_summary.get("early_exit_reason", "") or "").strip()
    reconcile_checked = int(reconcile_summary.get("checked", 0) or 0)
    close_gate_passed = bool(reconcile_summary.get("close_gate_passed", False))
    close_gate_reason = str(reconcile_summary.get("close_gate_reason", "") or "").strip()
    reconcile_has_positive_signal = bool(
        reconcile_checked > 0
        or close_gate_passed
        or close_gate_reason in {"no_pending_sell_closes", "sell_close_completion_observed"}
    )
    reconcile_green = (
        isinstance(reconcile_payload, dict)
        and bool(reconcile_summary)
        and reconcile_age is not None
        and reconcile_age <= max(30, int(RECON_MAX_AGE_SECONDS))
        and not reconcile_early_exit
        and reconcile_has_positive_signal
    )
    telemetry_recent = venue_health_snapshot("coinbase", window_minutes=max(1, int(RECOVERY_WINDOW_MINUTES))) or {}
    recent_samples = int(telemetry_recent.get("samples", 0) or 0)
    recent_success_rate = float(telemetry_recent.get("success_rate", 0.0) or 0.0)
    recent_failure_rate = float(telemetry_recent.get("failure_rate", 0.0) or 0.0)
    recent_p90 = float(telemetry_recent.get("p90_latency_ms", 0.0) or 0.0)
    telemetry_recovery_override = (
        (not telemetry_green)
        and bool(dns_green)
        and bool(probe_green)
        and bool(reconcile_green)
        and (
            (
                recent_samples >= int(RECOVERY_MIN_SAMPLES)
                and recent_success_rate >= float(RECOVERY_MIN_SUCCESS_RATE)
                and recent_failure_rate <= float(RECOVERY_MAX_FAILURE_RATE)
                and recent_p90 <= float(MAX_P90_LATENCY_MS)
            )
            or (bool(do_probe) and bool(probe_rows) and all(bool(r.get("ok")) for r in probe_rows))
        )
    )
    if telemetry_recovery_override:
        telemetry_green = True

    reasons = []
    if not dns_green:
        reasons.append("dns_unhealthy")
        if failover_active:
            reasons.append("dns_failover_profile_active")
    if not probe_green:
        reasons.append("api_probe_failed")
    if not telemetry_green:
        if telemetry_samples < int(MIN_TELEMETRY_SAMPLES):
            reasons.append(
                f"telemetry_samples_low:{telemetry_samples}<{int(MIN_TELEMETRY_SAMPLES)}"
            )
        elif telemetry_success_rate < float(MIN_SUCCESS_RATE):
            reasons.append(
                f"telemetry_success_rate_low:{telemetry_success_rate:.4f}<{float(MIN_SUCCESS_RATE):.4f}"
            )
        elif telemetry_failure_rate > float(MAX_FAILURE_RATE):
            reasons.append(
                f"telemetry_failure_rate_high:{telemetry_failure_rate:.4f}>{float(MAX_FAILURE_RATE):.4f}"
            )
        else:
            reasons.append(
                f"telemetry_p90_high:{telemetry_p90:.2f}>{float(MAX_P90_LATENCY_MS):.2f}"
            )
    if not reconcile_green:
        if not reconcile_summary:
            reasons.append("reconcile_status_missing")
        elif reconcile_early_exit:
            reasons.append(f"reconcile_early_exit:{reconcile_early_exit}")
        elif reconcile_age is None or reconcile_age > max(30, int(RECON_MAX_AGE_SECONDS)):
            reasons.append("reconcile_status_stale")
        elif reconcile_checked <= 0 and not close_gate_passed:
            reasons.append("reconcile_no_orders_checked")
        else:
            reasons.append("reconcile_unhealthy")

    green = len(reasons) == 0
    payload = {
        "updated_at": _now_iso(),
        "green": bool(green),
        "reason": "passed" if green else reasons[0],
        "reasons": reasons,
        "dns_degraded": bool(dns_degraded),
        "components": {
            "dns": {
                "required_hosts": list(DNS_HOSTS),
                "require_all": bool(DNS_REQUIRE_ALL),
                "ok_count": int(dns_ok_count),
                "total": len(dns_rows),
                "green": bool(dns_green),
                "degraded": bool(dns_degraded),
                "failover_profile": {
                    "mode": str(DNS_FAILOVER_PROFILE or ""),
                    "fallback_ips": list(DNS_FAILOVER_FALLBACK_IPS),
                    "host_map_keys": failover_hosts,
                    "configured": bool(failover_configured),
                    "active": bool(failover_active),
                },
                "rows": dns_rows,
            },
            "api_probe": {
                "enabled": bool(do_probe),
                "green": bool(probe_green),
                "rows": probe_rows,
            },
            "telemetry": {
                "green": bool(telemetry_green),
                "min_samples": int(MIN_TELEMETRY_SAMPLES),
                "min_success_rate": float(MIN_SUCCESS_RATE),
                "max_failure_rate": float(MAX_FAILURE_RATE),
                "max_p90_latency_ms": float(MAX_P90_LATENCY_MS),
                "snapshot": telemetry,
                "recovery_override": bool(telemetry_recovery_override),
                "recovery_window_minutes": int(RECOVERY_WINDOW_MINUTES),
                "recovery_min_samples": int(RECOVERY_MIN_SAMPLES),
                "recovery_min_success_rate": float(RECOVERY_MIN_SUCCESS_RATE),
                "recovery_max_failure_rate": float(RECOVERY_MAX_FAILURE_RATE),
                "recovery_snapshot": telemetry_recent,
            },
            "reconcile": {
                "green": bool(reconcile_green),
                "max_age_seconds": int(RECON_MAX_AGE_SECONDS),
                "age_seconds": None if reconcile_age is None else round(float(reconcile_age), 3),
                "summary": reconcile_summary,
            },
        },
    }
    if write_status:
        try:
            path.write_text(json.dumps(payload, indent=2))
        except Exception:
            pass
    return payload


def load_execution_health(status_path=STATUS_PATH):
    return _load_json(status_path, {})


if __name__ == "__main__":
    print(json.dumps(evaluate_execution_health(refresh=True, probe_http=None, write_status=True), indent=2))
