#!/usr/bin/env python3
"""Tests for safe integration guard local-runtime overrides."""

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "safe_integration"))

from integration_guard import run_guard  # noqa: E402


def _prepare_base_guard_repo(tmp_path: Path):
    agents = tmp_path / "agents"
    claude_staging = agents / "claude_staging"
    claude_staging.mkdir(parents=True, exist_ok=True)

    (agents / "trading_lock.json").write_text(
        json.dumps(
            {
                "locked": True,
                "reason": "NO_GO: exit_manager_gate_failed:exit_manager_status_stale:500s>300s",
                "source": "flywheel_controller",
            }
        )
    )
    (agents / "quant_100_results.json").write_text(
        json.dumps({"summary": {"promoted_warm": 1, "rejected_cold": 0, "no_data": 0}})
    )
    (agents / "sniper.py").write_text(
        "# placeholder for integration guard check\n"
        "def has_exit_plan():\n"
        "    return True\n"
        "def ev_positive():\n"
        "    return True\n"
    )
    (agents / "execution_health_status.json").write_text(
        json.dumps({"green": True, "reason": "passed", "updated_at": "2026-02-14T00:00:00+00:00"})
    )
    (agents / "reconcile_agent_trades_status.json").write_text(
        json.dumps(
            {
                "summary": {
                    "checked": 1,
                    "updated_at": "2026-02-14T00:00:00+00:00",
                    "early_exit_reason": "",
                }
            }
        )
    )
    (claude_staging / "duplex_to_claude.jsonl").write_text(
        '{"trace_id":"t-1","meta":{"trace_id":"t-1"}}\n'
    )
    (claude_staging / "duplex_from_claude.jsonl").write_text(
        '{"trace_id":"t-2","meta":{"trace_id":"t-2"}}\n'
    )
    (claude_staging / "claude_ingest_bundle.json").write_text(
        json.dumps({"metadata": {"bundle_id": "bundle-1", "bundle_hash": "hash", "bundle_sequence": 1}})
    )


class TestSafeIntegrationGuard(unittest.TestCase):
    def setUp(self):
        self._tmpdir = Path(tempfile.mkdtemp(prefix="qguard-"))

    def tearDown(self):
        if self._tmpdir.exists():
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_integration_guard_blocks_stale_lock_by_default(self):
        _prepare_base_guard_repo(self._tmpdir)
        if "INTEGRATION_GUARD_ALLOW_STALE_EXIT_MANAGER_LOCK" in os.environ:  # pragma: no cover
            del os.environ["INTEGRATION_GUARD_ALLOW_STALE_EXIT_MANAGER_LOCK"]
        report = run_guard(self._tmpdir)
        self.assertFalse(report["ready_for_staged_rollout"])
        self.assertIn("trading_lock_allows_rollout", report["required_failures"])

    def test_integration_guard_relaxes_stale_lock_with_override(self):
        _prepare_base_guard_repo(self._tmpdir)
        os.environ["INTEGRATION_GUARD_ALLOW_STALE_EXIT_MANAGER_LOCK"] = "1"
        try:
            report = run_guard(self._tmpdir)
            self.assertTrue(report["ready_for_staged_rollout"])
            self.assertNotIn("trading_lock_allows_rollout", report["required_failures"])
        finally:
            del os.environ["INTEGRATION_GUARD_ALLOW_STALE_EXIT_MANAGER_LOCK"]
