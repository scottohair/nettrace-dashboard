#!/usr/bin/env python3
"""Additional execution-health tests executed by unittest-style runner."""

import json
import tempfile
import shutil
from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "agents"))

import execution_health as eh  # noqa: E402


class TestExecutionHealthLocalMode(unittest.TestCase):
    def setUp(self):
        self._tmpdir = Path(tempfile.mkdtemp(prefix="qhealth-"))
        self._restore = {}
        for name in [
            "RECON_STATUS_PATH",
            "DNS_HOSTS",
            "DNS_REQUIRE_ALL",
            "MIN_TELEMETRY_SAMPLES",
            "LOCAL_TEST_MODE",
            "DNS_FAILOVER_HOST_MAP",
            "_dns_probe",
            "_http_probe",
            "venue_health_snapshot",
            "HTTP_PROBE_URLS",
        ]:
            self._restore[name] = getattr(eh, name)

    def tearDown(self):
        for name, value in self._restore.items():
            setattr(eh, name, value)
        if self._tmpdir.exists():
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_local_test_mode_forces_green_without_probe_or_telemetry(self):
        now_iso = datetime.now(timezone.utc).isoformat()
        reconcile = self._tmpdir / "reconcile_status.json"
        reconcile.write_text(
            json.dumps(
                {
                    "updated_at": now_iso,
                    "summary": {"checked": 3, "early_exit_reason": ""},
                }
            )
        )

        eh.RECON_STATUS_PATH = reconcile
        eh.DNS_HOSTS = ("api.coinbase.com",)
        eh.DNS_REQUIRE_ALL = True
        eh.MIN_TELEMETRY_SAMPLES = 3
        eh.LOCAL_TEST_MODE = True

        eh.venue_health_snapshot = lambda *_args, **_kwargs: {
            "samples": 8,
            "success_rate": 0.01,
            "failure_rate": 0.99,
            "p90_latency_ms": 50.0,
        }
        eh._dns_probe = lambda host: {
            "host": host,
            "ok": True,
            "ips": ["127.0.0.1"],
            "error": "[Errno 1] Operation not permitted",
            "resolver_source": "system",
            "latency_ms": 1.0,
        }
        eh._http_probe = (
            lambda url, _timeout, fallback_ips=None: {
                "url": url,
                "ok": False,
                "status": 0,
                "error": "should_not_run_in_local_mode",
                "latency_ms": 3.0,
            }
        )
        eh.HTTP_PROBE_URLS = ("https://example.com/ok",)

        payload = eh.evaluate_execution_health(
            refresh=True,
            probe_http=None,
            write_status=False,
            status_path=self._tmpdir / "health.json",
        )
        self.assertTrue(payload["green"])
        self.assertEqual(payload["reason"], "passed")
        self.assertFalse(payload["egress_blocked"])
        self.assertFalse(payload["components"]["api_probe"]["enabled"])
        self.assertTrue(payload["components"]["telemetry"]["green"])
