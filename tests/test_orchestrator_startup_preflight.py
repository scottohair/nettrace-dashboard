#!/usr/bin/env python3
"""Unit tests for startup preflight fail-reason ignore handling."""

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import shutil
import os
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "agents"))
import orchestrator_v2 as orch  # noqa: E402


class TestOrchestratorStartupPrefight(unittest.TestCase):
    def setUp(self):
        self._tmpdir = Path(tempfile.mkdtemp(prefix="q_orch_pref_"))
        self._restore = {}
        self._restore_vals = [
            "ORCH_STARTUP_PREFLIGHT_ENABLED",
            "ORCH_STARTUP_PREFLIGHT_FAIL_OPEN",
            "ORCH_STARTUP_PREFLIGHT_REQUIRE_GREEN",
            "ORCH_STARTUP_PREFLIGHT_MAX_AGE_SECONDS",
            "ORCH_STARTUP_PREFLIGHT_IGNORE_REASONS",
            "ORCH_DB",
            "PENDING_BRIDGES_FILE",
            "INTEGRATION_GUARD_STATUS_FILE",
            "STARTUP_PREFLIGHT_STATUS_FILE",
        ]
        for key in self._restore_vals:
            self._restore[key] = getattr(orch, key)

    def tearDown(self):
        for key, value in self._restore.items():
            setattr(orch, key, value)
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _make_orchestrator(self):
        orch.ORCH_DB = str(self._tmpdir / "orchestrator.db")
        orch.PENDING_BRIDGES_FILE = str(self._tmpdir / "pending_bridges.json")
        orch.INTEGRATION_GUARD_STATUS_FILE = self._tmpdir / "integration_guard_status.json"
        orch.STARTUP_PREFLIGHT_STATUS_FILE = self._tmpdir / "orchestrator_startup_preflight.json"
        orch.OrchestratorV2._get_starting_capital = lambda self: 100.0  # type: ignore
        return orch.OrchestratorV2()

    def test_startup_prefight_allows_ignorable_reasons(self):
        instance = self._make_orchestrator()
        orch.ORCH_STARTUP_PREFLIGHT_ENABLED = True
        orch.ORCH_STARTUP_PREFLIGHT_FAIL_OPEN = False
        orch.ORCH_STARTUP_PREFLIGHT_REQUIRE_GREEN = True
        orch.ORCH_STARTUP_PREFLIGHT_MAX_AGE_SECONDS = 300
        orch.ORCH_STARTUP_PREFLIGHT_IGNORE_REASONS = (
            "egress_blocked",
            "api_probe_failed",
            "telemetry_success_rate_low",
        )
        now_iso = datetime.now(timezone.utc).isoformat()

        def _payload(*_args, **_kwargs):
            return {
                "updated_at": now_iso,
                "green": False,
                "reason": "egress_blocked",
                "reasons": [
                    "egress_blocked",
                    "api_probe_failed",
                    "telemetry_success_rate_low:0.30<0.55",
                ],
            }

        instance._fetch_execution_health_payload = _payload  # type: ignore
        report = instance.run_startup_preflight(force_refresh=True)
        self.assertTrue(report["passed"])
        self.assertEqual(report["reason"], "execution_health_warnings_ignored")
        self.assertIn("egress_blocked", report["ignore_reasons"])
        self.assertIn("api_probe_failed", report["ignore_reasons"])

    def test_startup_prefight_blocks_unignored_reasons(self):
        instance = self._make_orchestrator()
        orch.ORCH_STARTUP_PREFLIGHT_ENABLED = True
        orch.ORCH_STARTUP_PREFLIGHT_FAIL_OPEN = False
        orch.ORCH_STARTUP_PREFLIGHT_REQUIRE_GREEN = True
        orch.ORCH_STARTUP_PREFLIGHT_MAX_AGE_SECONDS = 300
        orch.ORCH_STARTUP_PREFLIGHT_IGNORE_REASONS = ("egress_blocked", "api_probe_failed")
        now_iso = datetime.now(timezone.utc).isoformat()

        def _payload(*_args, **_kwargs):
            return {
                "updated_at": now_iso,
                "green": False,
                "reason": "reconcile_status_stale",
                "reasons": ["egress_blocked", "reconcile_status_stale"],
            }

        instance._fetch_execution_health_payload = _payload  # type: ignore
        report = instance.run_startup_preflight(force_refresh=True)
        self.assertFalse(report["passed"])
        self.assertEqual(report["reason"], "execution_health_not_green:reconcile_status_stale")


if __name__ == "__main__":
    unittest.main()
