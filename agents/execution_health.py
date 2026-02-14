#!/usr/bin/env python3
"""Execution health checks for venue DNS/API/reconciliation readiness."""

import json
import os
import socket
import time
import urllib.request
from urllib.parse import urlparse, urlunparse
import ssl
from datetime import datetime, timezone
from pathlib import Path


def _parse_prefix_list(value):
    items = str(value or "").split(",")
    ordered = []
    seen = set()
    for item in items:
        item_s = str(item or "").strip()
        if not item_s or item_s in seen:
            continue
        seen.add(item_s)
        ordered.append(item_s)
    return tuple(ordered)


BASE = Path(__file__).parent
STATUS_PATH = BASE / "execution_health_status.json"
HISTORY_PATH = BASE / "execution_health_history.jsonl"
RECON_STATUS_PATH = BASE / "reconcile_agent_trades_status.json"
CANDLE_FEED_PATH = BASE / "candle_feed_latest.json"
CANDLE_AGG_STATUS_PATH = BASE / "candle_aggregator_status.json"
HEALTH_HISTORY_MAX_LINES = int(os.environ.get("EXEC_HEALTH_HISTORY_MAX_LINES", "2000"))

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
PUBLIC_DNS_ENABLED = os.environ.get("EXEC_HEALTH_PUBLIC_DNS_ENABLED", "1").lower() not in ("0", "false", "no")
PUBLIC_DNS_TIMEOUT_SECONDS = float(os.environ.get("EXEC_HEALTH_PUBLIC_DNS_TIMEOUT_SECONDS", "1.2"))
PUBLIC_DNS_MAX_IPS = int(os.environ.get("EXEC_HEALTH_PUBLIC_DNS_MAX_IPS", "8"))
DNS_PROBE_ATTEMPTS = int(os.environ.get("EXEC_HEALTH_DNS_PROBE_ATTEMPTS", "2"))
DNS_PROBE_RETRY_DELAY_SECONDS = float(os.environ.get("EXEC_HEALTH_DNS_PROBE_RETRY_DELAY_SECONDS", "0.15"))

MIN_TELEMETRY_SAMPLES = int(os.environ.get("EXEC_HEALTH_MIN_TELEMETRY_SAMPLES", "3"))
MIN_SUCCESS_RATE = float(os.environ.get("EXEC_HEALTH_MIN_SUCCESS_RATE", "0.55"))
MAX_FAILURE_RATE = float(os.environ.get("EXEC_HEALTH_MAX_FAILURE_RATE", "0.45"))
MAX_P90_LATENCY_MS = float(os.environ.get("EXEC_HEALTH_MAX_P90_LATENCY_MS", "4500"))
TELEMETRY_INCLUDE_ENDPOINT_PREFIXES = _parse_prefix_list(
    os.environ.get(
        "EXEC_HEALTH_TELEMETRY_INCLUDE_ENDPOINT_PREFIXES",
        "/api/v3/brokerage/products,/api/v3/brokerage/orders",
    )
)
TELEMETRY_EXCLUDE_ENDPOINT_PREFIXES = _parse_prefix_list(
    os.environ.get(
        "EXEC_HEALTH_TELEMETRY_EXCLUDE_ENDPOINT_PREFIXES",
        "/api/v3/brokerage/orders/historical",
    )
)
RECOVERY_WINDOW_MINUTES = int(os.environ.get("EXEC_HEALTH_RECOVERY_WINDOW_MINUTES", "3"))
RECOVERY_MIN_SAMPLES = int(os.environ.get("EXEC_HEALTH_RECOVERY_MIN_SAMPLES", "2"))
RECOVERY_MIN_SUCCESS_RATE = float(os.environ.get("EXEC_HEALTH_RECOVERY_MIN_SUCCESS_RATE", "0.60"))
RECOVERY_MAX_FAILURE_RATE = float(os.environ.get("EXEC_HEALTH_RECOVERY_MAX_FAILURE_RATE", "0.40"))

RECON_MAX_AGE_SECONDS = int(os.environ.get("EXEC_HEALTH_RECON_MAX_AGE_SECONDS", "900"))
RECON_ALLOW_STALE_NO_PENDING = os.environ.get(
    "EXEC_HEALTH_RECON_ALLOW_STALE_NO_PENDING", "1"
).lower() not in ("0", "false", "no")
RECON_STALE_NO_PENDING_GRACE_SECONDS = int(
    os.environ.get("EXEC_HEALTH_RECON_STALE_NO_PENDING_GRACE_SECONDS", "3600")
)
HEALTH_CACHE_MAX_AGE_SECONDS = int(os.environ.get("EXEC_HEALTH_CACHE_MAX_AGE_SECONDS", "120"))

HTTP_PROBE_ENABLED = os.environ.get("EXEC_HEALTH_HTTP_PROBE_ENABLED", "1").lower() not in ("0", "false", "no")
HTTP_PROBE_TIMEOUT_SECONDS = float(os.environ.get("EXEC_HEALTH_HTTP_PROBE_TIMEOUT_SECONDS", "4.0"))
HTTP_PROBE_ATTEMPTS = int(os.environ.get("EXEC_HEALTH_HTTP_PROBE_ATTEMPTS", "2"))
HTTP_PROBE_RETRY_DELAY_SECONDS = float(os.environ.get("EXEC_HEALTH_HTTP_PROBE_RETRY_DELAY_SECONDS", "0.2"))
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
LOCAL_TEST_MODE = os.environ.get("EXEC_HEALTH_LOCAL_TEST_MODE", "0").lower() not in ("0", "false", "no")
FORCE_TELEMETRY_BYPASS = os.environ.get("EXEC_HEALTH_FORCE_TELEMETRY_BYPASS", "0").lower() not in (
    "0",
    "false",
    "no",
    "off",
)

CANDLE_FEED_GATE_ENABLED = os.environ.get("EXEC_HEALTH_CANDLE_FEED_GATE_ENABLED", "0").lower() not in (
    "0",
    "false",
    "no",
)
CANDLE_FEED_MIN_POINTS = int(os.environ.get("EXEC_HEALTH_CANDLE_FEED_MIN_POINTS", "1000"))
CANDLE_FEED_MIN_PAIR_COUNT = int(os.environ.get("EXEC_HEALTH_CANDLE_FEED_MIN_PAIR_COUNT", "6"))
CANDLE_FEED_MIN_TIMEFRAME_COUNT = int(os.environ.get("EXEC_HEALTH_CANDLE_FEED_MIN_TIMEFRAME_COUNT", "2"))
CANDLE_FEED_MAX_AGE_SECONDS = int(os.environ.get("EXEC_HEALTH_CANDLE_FEED_MAX_AGE_SECONDS", "360"))
CANDLE_FEED_REQUIRE_TARGET_ACHIEVED = os.environ.get(
    "EXEC_HEALTH_CANDLE_FEED_REQUIRE_TARGET_ACHIEVED", "1"
).lower() not in ("0", "false", "no")


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
if not DNS_FAILOVER_HOST_MAP:
    DNS_FAILOVER_HOST_MAP = {
        "api.coinbase.com": ["104.18.35.15", "172.64.152.241"],
        "api.exchange.coinbase.com": ["104.18.36.178", "172.64.151.78"],
    }


def _coerce_dns_host_map(value):
    if not isinstance(value, dict):
        return {}
    mapped = {}
    for host, raw_ips in value.items():
        host_s = str(host or "").strip().lower()
        if not host_s:
            continue
        ips = []
        if isinstance(raw_ips, (list, tuple, set)):
            entries = raw_ips
        else:
            entries = [raw_ips]
        for ip in entries:
            ip_s = str(ip or "").strip()
            if ip_s:
                ips.append(ip_s)
        if ips:
            deduped = []
            seen = set()
            for item in ips:
                if item in seen:
                    continue
                seen.add(item)
                deduped.append(item)
            mapped[host_s] = deduped
    return mapped


DNS_FAILOVER_HOST_MAP = _coerce_dns_host_map(DNS_FAILOVER_HOST_MAP)

try:
    from public_dns_resolver import (
        parse_resolver_list as _parse_public_dns_resolvers,
        resolve_host_via_public_dns,
    )
except Exception:
    try:
        from agents.public_dns_resolver import (  # type: ignore
            parse_resolver_list as _parse_public_dns_resolvers,
            resolve_host_via_public_dns,
        )
    except Exception:
        def _parse_public_dns_resolvers(value, default=None):
            raw = str(value or "").split(",")
            out = []
            seen = set()
            for item in raw:
                ip = str(item or "").strip()
                if not ip or ip in seen:
                    continue
                seen.add(ip)
                out.append(ip)
            if out:
                return tuple(out)
            if default is None:
                return ()
            return tuple(str(x).strip() for x in default if str(x).strip())

        def resolve_host_via_public_dns(*_args, **_kwargs):
            return []

PUBLIC_DNS_RESOLVERS = _parse_public_dns_resolvers(
    os.environ.get("EXEC_HEALTH_PUBLIC_DNS_RESOLVERS", "1.1.1.1,8.8.8.8,9.9.9.9"),
    ("1.1.1.1", "8.8.8.8", "9.9.9.9"),
)

try:
    from execution_telemetry import venue_health_snapshot
except Exception:
    try:
        from agents.execution_telemetry import venue_health_snapshot  # type: ignore
    except Exception:
        def venue_health_snapshot(*_args, **_kwargs):
            return {}


def _venue_health_snapshot_safe(venue, window_minutes, include_endpoint_prefixes, exclude_endpoint_prefixes):
    """Call snapshot helper with kwargs-first and positional fallback."""
    try:
        return venue_health_snapshot(
            venue,
            window_minutes=window_minutes,
            include_endpoint_prefixes=include_endpoint_prefixes,
            exclude_endpoint_prefixes=exclude_endpoint_prefixes,
        )
    except TypeError:
        try:
            return venue_health_snapshot(venue, window_minutes)
        except TypeError:
            return venue_health_snapshot(venue)

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


def _append_health_history(path, payload):
    p = Path(path)
    row = {
        "updated_at": str(payload.get("updated_at", "")),
        "green": bool(payload.get("green", False)),
        "reason": str(payload.get("reason", "")),
        "dns_degraded": bool(payload.get("dns_degraded", False)),
    }
    try:
        lines = []
        if p.exists():
            lines = p.read_text().splitlines()
        lines.append(json.dumps(row))
        max_lines = max(100, int(HEALTH_HISTORY_MAX_LINES))
        if len(lines) > max_lines:
            lines = lines[-max_lines:]
        p.write_text("\n".join(lines) + ("\n" if lines else ""))
    except Exception:
        return


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


def _coerce_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return int(default)


def _candle_feed_health():
    health = {
        "enabled": bool(CANDLE_FEED_GATE_ENABLED),
        "green": True,
        "reason": "gate_disabled",
        "path": str(CANDLE_FEED_PATH),
        "aggregator_status_path": str(CANDLE_AGG_STATUS_PATH),
        "feed_exists": False,
        "aggregator_status_exists": False,
        "age_seconds": None,
        "generated_at": "",
        "latest_at": "",
        "latest_ts": None,
        "total_points": 0,
        "target_points": 0,
        "target_achieved": False,
        "pair_count": 0,
        "timeframe_count": 0,
        "filters": {},
        "aggregator_status": {},
        "requirements": {
            "min_points": int(CANDLE_FEED_MIN_POINTS),
            "min_pair_count": int(CANDLE_FEED_MIN_PAIR_COUNT),
            "min_timeframe_count": int(CANDLE_FEED_MIN_TIMEFRAME_COUNT),
            "max_age_seconds": int(CANDLE_FEED_MAX_AGE_SECONDS),
            "require_target_achieved": bool(CANDLE_FEED_REQUIRE_TARGET_ACHIEVED),
        },
    }
    if not CANDLE_FEED_GATE_ENABLED:
        return health

    feed_payload = _load_json(CANDLE_FEED_PATH, {})
    agg_status = _load_json(CANDLE_AGG_STATUS_PATH, {})
    if isinstance(agg_status, dict) and agg_status:
        health["aggregator_status_exists"] = True
        health["aggregator_status"] = agg_status

    if not isinstance(feed_payload, dict) or not feed_payload:
        health["green"] = False
        health["reason"] = "candle_feed_missing"
        return health

    health["feed_exists"] = True
    summary = feed_payload.get("summary", {}) if isinstance(feed_payload.get("summary"), dict) else {}
    filters = feed_payload.get("filters", {}) if isinstance(feed_payload.get("filters"), dict) else {}
    points = feed_payload.get("points", [])
    generated_at = str(feed_payload.get("generated_at", "") or "")
    age_seconds = _iso_age_seconds(generated_at)

    total_points = _coerce_int(summary.get("total_points"), len(points) if isinstance(points, list) else 0)
    target_points = _coerce_int(summary.get("target_points"), _coerce_int(filters.get("limit"), 0))
    pair_count = _coerce_int(summary.get("pair_count"), 0)
    timeframe_count = _coerce_int(summary.get("timeframe_count"), 0)
    latest_ts = summary.get("latest_ts")
    latest_ts_int = _coerce_int(latest_ts, 0) if latest_ts is not None else 0
    latest_at = str(summary.get("latest_at", "") or "")
    target_achieved = bool(summary.get("target_achieved", False))

    health.update(
        {
            "age_seconds": None if age_seconds is None else round(float(age_seconds), 3),
            "generated_at": generated_at,
            "latest_at": latest_at,
            "latest_ts": (latest_ts_int if latest_ts_int > 0 else None),
            "total_points": int(total_points),
            "target_points": int(target_points),
            "target_achieved": bool(target_achieved),
            "pair_count": int(pair_count),
            "timeframe_count": int(timeframe_count),
            "filters": filters,
        }
    )

    if age_seconds is None:
        health["green"] = False
        health["reason"] = "candle_feed_generated_at_missing"
        return health
    if age_seconds > float(CANDLE_FEED_MAX_AGE_SECONDS):
        health["green"] = False
        health["reason"] = (
            f"candle_feed_stale:{age_seconds:.1f}s>{float(CANDLE_FEED_MAX_AGE_SECONDS):.1f}s"
        )
        return health
    if total_points < int(CANDLE_FEED_MIN_POINTS):
        health["green"] = False
        health["reason"] = f"candle_feed_points_low:{total_points}<{int(CANDLE_FEED_MIN_POINTS)}"
        return health
    if pair_count < int(CANDLE_FEED_MIN_PAIR_COUNT):
        health["green"] = False
        health["reason"] = f"candle_feed_pairs_low:{pair_count}<{int(CANDLE_FEED_MIN_PAIR_COUNT)}"
        return health
    if timeframe_count < int(CANDLE_FEED_MIN_TIMEFRAME_COUNT):
        health["green"] = False
        health["reason"] = (
            f"candle_feed_timeframes_low:{timeframe_count}<{int(CANDLE_FEED_MIN_TIMEFRAME_COUNT)}"
        )
        return health
    if CANDLE_FEED_REQUIRE_TARGET_ACHIEVED and not bool(target_achieved):
        health["green"] = False
        health["reason"] = "candle_feed_target_not_achieved"
        return health

    health["reason"] = "candle_feed_healthy"
    return health


def _dns_probe(host):
    started = time.perf_counter()
    ips = []
    error = ""
    resolver_source = "system"
    attempts = max(1, int(DNS_PROBE_ATTEMPTS))
    for attempt in range(attempts):
        try:
            infos = socket.getaddrinfo(str(host), 443, type=socket.SOCK_STREAM)
            seen = set()
            for info in infos:
                addr = info[4][0] if info and len(info) >= 5 and info[4] else ""
                if addr and addr not in seen:
                    seen.add(addr)
                    ips.append(addr)
            if ips:
                break
        except Exception as e:
            error = str(e)
        if not ips and attempt + 1 < attempts:
            time.sleep(max(0.0, float(DNS_PROBE_RETRY_DELAY_SECONDS)))
    if not ips:
        host_s = str(host or "").strip().lower()
        mapped = []
        direct = DNS_FAILOVER_HOST_MAP.get(host_s)
        wildcard = DNS_FAILOVER_HOST_MAP.get("*")
        for item in (direct or []):
            ip = str(item or "").strip()
            if ip:
                mapped.append(ip)
        for item in (wildcard or []):
            ip = str(item or "").strip()
            if ip:
                mapped.append(ip)
        for item in DNS_FAILOVER_FALLBACK_IPS:
            ip = str(item or "").strip()
            if ip:
                mapped.append(ip)
        deduped = []
        seen = set()
        for ip in mapped:
            if ip in seen:
                continue
            seen.add(ip)
            deduped.append(ip)
        if deduped:
            ips = deduped[:8]
            resolver_source = "host_map"
            if error:
                error = f"system_dns_failed_host_map_recovered:{error}"
    if not ips and PUBLIC_DNS_ENABLED and PUBLIC_DNS_RESOLVERS:
        try:
            fallback_ips = resolve_host_via_public_dns(
                host=str(host),
                resolvers=PUBLIC_DNS_RESOLVERS,
                timeout_seconds=float(PUBLIC_DNS_TIMEOUT_SECONDS),
                max_ips=max(1, int(PUBLIC_DNS_MAX_IPS)),
                include_ipv6=False,
            )
            if fallback_ips:
                ips = list(fallback_ips)
                resolver_source = "public_dns"
                if error:
                    error = f"system_dns_failed_public_dns_recovered:{error}"
        except Exception as e:
            if not error:
                error = str(e)
    return {
        "host": str(host),
        "ok": bool(ips),
        "ips": ips[:8],
        "error": error,
        "resolver_source": resolver_source,
        "latency_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }


def _probe_url_with_ip(base_url, ip):
    parsed = urlparse(str(base_url))
    if not parsed.scheme or not parsed.netloc:
        return str(base_url)
    host = str(ip or "").strip()
    if not host:
        return str(base_url)
    netloc = str(host)
    if parsed.port:
        netloc = f"{netloc}:{parsed.port}"
    return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))


def _http_probe(url, timeout_seconds, fallback_ips=None):
    started = time.perf_counter()
    status = 0
    error = ""
    ok = False
    parsed = urlparse(str(url))
    host = str(parsed.hostname or "").strip()
    fallback_targets = []
    for ip in fallback_ips or ():
        if not ip:
            continue
        fallback_targets.append(_probe_url_with_ip(url, ip))

    targets = [str(url), *fallback_targets]
    attempts = max(1, int(HTTP_PROBE_ATTEMPTS))
    for attempt in range(attempts):
        for target_url in targets:
            req_headers = {"User-Agent": "NetTrace-ExecHealth/1.0"}
            if host and target_url != str(url):
                req_headers["Host"] = host
            try:
                req = urllib.request.Request(str(target_url), headers=req_headers)
                with urllib.request.urlopen(
                    req,
                    timeout=max(0.5, float(timeout_seconds)),
                    context=ssl._create_unverified_context(),
                ) as resp:
                    status = int(getattr(resp, "status", 200) or 200)
                    _ = resp.read(128)
                ok = 200 <= status < 300
            except Exception as e:
                error = str(e)
                continue
            if ok:
                break
        if ok:
            break
        if attempt + 1 < attempts and not ok:
            time.sleep(max(0.0, float(HTTP_PROBE_RETRY_DELAY_SECONDS)))
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


def _is_egress_error_text(text):
    t = str(text or "").strip().lower()
    if not t:
        return False
    needles = (
        "operation not permitted",
        "network is unreachable",
        "permission denied",
        "errno 1",
        "errno 101",
        "errno 13",
    )
    return any(n in t for n in needles)


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
    public_dns_configured = bool(PUBLIC_DNS_ENABLED and PUBLIC_DNS_RESOLVERS)
    failover_configured = bool(
        DNS_FAILOVER_PROFILE not in ("", "disabled", "off", "none")
        and (DNS_FAILOVER_FALLBACK_IPS or failover_hosts)
    ) or public_dns_configured
    failover_active = bool(dns_degraded and failover_configured)

    telemetry = (
        _venue_health_snapshot_safe(
            "coinbase",
            30,
            TELEMETRY_INCLUDE_ENDPOINT_PREFIXES,
            TELEMETRY_EXCLUDE_ENDPOINT_PREFIXES,
        )
        or {}
    )
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
    if LOCAL_TEST_MODE and probe_http is None:
        do_probe = False
    probe_rows = []
    if do_probe:
        host_to_ips = {
            str(row.get("host", "")).strip().lower(): list(row.get("ips", []))
            for row in dns_rows
            if row.get("ok")
        }
        probe_rows = [
            _http_probe(
                u,
                HTTP_PROBE_TIMEOUT_SECONDS,
                fallback_ips=host_to_ips.get(urlparse(str(u)).hostname or "", []),
            )
            for u in HTTP_PROBE_URLS
        ]
    probe_green = True
    if do_probe:
        probe_green = bool(probe_rows) and all(bool(r.get("ok")) for r in probe_rows)

    egress_blocked = False if LOCAL_TEST_MODE else (
        any(
            (not bool(r.get("ok")))
            and _is_egress_error_text(r.get("error", ""))
            for r in dns_rows
        )
        or any(
            (not bool(r.get("ok")))
            and _is_egress_error_text(r.get("error", ""))
            for r in probe_rows
        )
    )

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
    no_pending_gate = bool(close_gate_passed and close_gate_reason in {"no_pending_sell_closes", "sell_close_completion_observed"})
    stale_grace_limit = max(
        max(30, int(RECON_MAX_AGE_SECONDS)),
        max(30, int(RECON_STALE_NO_PENDING_GRACE_SECONDS)),
    )
    reconcile_stale_grace_ok = bool(
        RECON_ALLOW_STALE_NO_PENDING
        and no_pending_gate
        and reconcile_age is not None
        and reconcile_age <= float(stale_grace_limit)
    )
    reconcile_has_positive_signal = bool(
        reconcile_checked > 0
        or close_gate_passed
        or close_gate_reason in {"no_pending_sell_closes", "sell_close_completion_observed"}
    )
    reconcile_green = (
        isinstance(reconcile_payload, dict)
        and bool(reconcile_summary)
        and reconcile_age is not None
        and not reconcile_early_exit
        and reconcile_has_positive_signal
        and (
            reconcile_age <= max(30, int(RECON_MAX_AGE_SECONDS))
            or reconcile_stale_grace_ok
        )
    )
    candle_feed = _candle_feed_health()
    candle_feed_green = bool(candle_feed.get("green", True))
    telemetry_recent = {}
    recent_samples = 0
    recent_success_rate = 0.0
    recent_failure_rate = 0.0
    recent_p90 = 0.0
    telemetry_recovery_override = False
    if not telemetry_green:
        telemetry_recent = (
            _venue_health_snapshot_safe(
                "coinbase",
                max(1, int(RECOVERY_WINDOW_MINUTES)),
                TELEMETRY_INCLUDE_ENDPOINT_PREFIXES,
                TELEMETRY_EXCLUDE_ENDPOINT_PREFIXES,
            )
            or {}
        )
        recent_samples = int(telemetry_recent.get("samples", 0) or 0)
        recent_success_rate = float(telemetry_recent.get("success_rate", 0.0) or 0.0)
        recent_failure_rate = float(telemetry_recent.get("failure_rate", 0.0) or 0.0)
        recent_p90 = float(telemetry_recent.get("p90_latency_ms", 0.0) or 0.0)
        telemetry_recovery_override = (
            bool(dns_green)
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
    if egress_blocked:
        reasons.append("egress_blocked")
    if not probe_green:
        reasons.append("api_probe_failed")
    if LOCAL_TEST_MODE:
        telemetry_green = True
        telemetry_samples = 0
        telemetry_success_rate = 1.0
        telemetry_failure_rate = 0.0
        telemetry_p90 = 0.0
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
    telemetry_bypass_used = False
    if (
        FORCE_TELEMETRY_BYPASS
        and not dns_degraded
        and probe_green
        and reconcile_green
        and (telemetry_samples < int(MIN_TELEMETRY_SAMPLES) or telemetry_success_rate < float(MIN_SUCCESS_RATE))
    ):
        telemetry_bypass_used = True
        telemetry_green = True
        reasons = [r for r in reasons if not r.startswith("telemetry_")]
        reasons.append("telemetry_bypass_enabled")
    if not reconcile_green:
        if not reconcile_summary:
            reasons.append("reconcile_status_missing")
        elif reconcile_early_exit:
            reasons.append(f"reconcile_early_exit:{reconcile_early_exit}")
        elif (
            reconcile_age is None
            or (
                reconcile_age > max(30, int(RECON_MAX_AGE_SECONDS))
                and not reconcile_stale_grace_ok
            )
        ):
            reasons.append("reconcile_status_stale")
        elif reconcile_checked <= 0 and not close_gate_passed:
            reasons.append("reconcile_no_orders_checked")
        else:
            reasons.append("reconcile_unhealthy")
    if not candle_feed_green:
        reasons.append(str(candle_feed.get("reason", "candle_feed_unhealthy")))

    green = len(reasons) == 0
    payload = {
        "updated_at": _now_iso(),
        "green": bool(green),
        "reason": "passed" if green else reasons[0],
        "reasons": reasons,
        "dns_degraded": bool(dns_degraded),
        "egress_blocked": bool(egress_blocked),
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
                    "public_dns_enabled": bool(PUBLIC_DNS_ENABLED),
                    "public_dns_resolvers": list(PUBLIC_DNS_RESOLVERS),
                    "public_dns_configured": bool(public_dns_configured),
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
            "bypass_enabled": bool(FORCE_TELEMETRY_BYPASS),
            "bypass_used": bool(telemetry_bypass_used),
            "dns_failures_excluded": int(telemetry.get("dns_failures_excluded", 0) or 0),
            "min_samples": int(MIN_TELEMETRY_SAMPLES),
            "min_success_rate": float(MIN_SUCCESS_RATE),
            "max_failure_rate": float(MAX_FAILURE_RATE),
                "max_p90_latency_ms": float(MAX_P90_LATENCY_MS),
                "include_endpoint_prefixes": list(TELEMETRY_INCLUDE_ENDPOINT_PREFIXES),
                "exclude_endpoint_prefixes": list(TELEMETRY_EXCLUDE_ENDPOINT_PREFIXES),
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
                "allow_stale_no_pending": bool(RECON_ALLOW_STALE_NO_PENDING),
                "stale_no_pending_grace_seconds": int(RECON_STALE_NO_PENDING_GRACE_SECONDS),
                "stale_grace_applied": bool(reconcile_stale_grace_ok),
                "age_seconds": None if reconcile_age is None else round(float(reconcile_age), 3),
                "summary": reconcile_summary,
            },
            "candle_feed": candle_feed,
        },
    }
    if write_status:
        try:
            path.write_text(json.dumps(payload, indent=2))
            _append_health_history(HISTORY_PATH, payload)
        except Exception:
            pass
    return payload


def load_execution_health(status_path=STATUS_PATH):
    return _load_json(status_path, {})


if __name__ == "__main__":
    print(json.dumps(evaluate_execution_health(refresh=True, probe_http=None, write_status=True), indent=2))
