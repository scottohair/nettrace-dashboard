from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
import os

from agents import trading_guard


class TradingGuardLockTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.orig_lock_file = trading_guard.LOCK_FILE
        trading_guard.LOCK_FILE = Path(self.tmpdir.name) / "trading_lock.json"

    def tearDown(self):
        trading_guard.LOCK_FILE = self.orig_lock_file
        self.tmpdir.cleanup()

    def test_set_and_clear_trading_lock(self):
        set_state = trading_guard.set_trading_lock("manual test", source="unit", metadata={"scope": "ci"})
        self.assertTrue(set_state["locked"])
        self.assertTrue(trading_guard.LOCK_FILE.exists())
        raw_set = json.loads(trading_guard.LOCK_FILE.read_text())
        self.assertEqual(raw_set["locked"], True)
        self.assertEqual(raw_set["reason"], "manual test")
        self.assertEqual(raw_set["source"], "unit")
        self.assertEqual(raw_set["metadata"]["scope"], "ci")
        self.assertIsInstance(raw_set["epoch"], float)
        self.assertIsInstance(raw_set["pid"], int)

        clear_state = trading_guard.clear_trading_lock(source="unit", note="resume")
        self.assertFalse(clear_state["locked"])
        self.assertTrue(trading_guard.LOCK_FILE.exists())
        raw_clear = json.loads(trading_guard.LOCK_FILE.read_text())
        self.assertEqual(raw_clear["locked"], False)
        self.assertEqual(raw_clear["reason"], "resume")
        self.assertEqual(raw_clear["source"], "unit")
        self.assertEqual(raw_clear["metadata"]["cleared"], True)
        self.assertIsInstance(raw_clear["epoch"], float)
        self.assertIsInstance(raw_clear["pid"], int)

    def test_corrupt_trading_lock_defaults_to_locked(self):
        trading_guard.LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        trading_guard.LOCK_FILE.write_text("{invalid-json")
        state = trading_guard.read_trading_lock()
        self.assertTrue(state["locked"])
        self.assertEqual(state["source"], "trading_guard")

    def test_concurrent_lock_updates_do_not_corrupt_file(self):
        def worker(idx: int):
            if idx % 2:
                trading_guard.set_trading_lock(f"locked-{idx}", source=f"worker-{idx}")
            else:
                trading_guard.clear_trading_lock(source=f"worker-{idx}", note=f"clear-{idx}")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(24)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        raw = json.loads(trading_guard.LOCK_FILE.read_text())
        self.assertIsInstance(raw, dict)
        self.assertIn("locked", raw)
        self.assertIn("reason", raw)
        self.assertIn("source", raw)
        self.assertIn("set_at", raw)
        self.assertIn("epoch", raw)
        self.assertIn("pid", raw)
        self.assertNotIn("~", str(raw))

    def test_stale_exit_manager_lock_is_recovered_when_owner_dead(self):
        stale = {
            "locked": True,
            "reason": "NO_GO: exit_manager_gate_failed:exit_manager_status_stale:500s>300s",
            "source": "flywheel_controller",
            "set_at": "2026-02-14T00:00:00+00:00",
            "pid": 999999,
            "metadata": {"decision": "NO_GO"},
        }
        trading_guard.LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        trading_guard.LOCK_FILE.write_text(json.dumps(stale))

        original_pid_check = trading_guard._is_pid_alive
        trading_guard._is_pid_alive = lambda pid: False  # type: ignore[misc]
        try:
            state = trading_guard.read_trading_lock()
        finally:
            trading_guard._is_pid_alive = original_pid_check

        self.assertFalse(state["locked"])
        self.assertEqual(state["source"], "trading_guard")
        self.assertIn("recovered_stale_dead_owner_lock", state["reason"])
        recovered = json.loads(trading_guard.LOCK_FILE.read_text())
        self.assertFalse(recovered["locked"])

    def test_live_owner_exit_manager_lock_is_not_recovered(self):
        stale = {
            "locked": True,
            "reason": "NO_GO: exit_manager_gate_failed:exit_manager_status_stale:500s>300s",
            "source": "flywheel_controller",
            "set_at": "2026-02-14T00:00:00+00:00",
            "pid": 999999,
            "metadata": {"decision": "NO_GO"},
        }
        trading_guard.LOCK_FILE.write_text(json.dumps(stale))

        original_pid_check = trading_guard._is_pid_alive
        trading_guard._is_pid_alive = lambda pid: True  # type: ignore[misc]
        try:
            state = trading_guard.read_trading_lock()
        finally:
            trading_guard._is_pid_alive = original_pid_check

        self.assertTrue(state["locked"])
        self.assertEqual(state["source"], "flywheel_controller")
        self.assertIn("exit_manager_status_stale", state["reason"])

    def test_permission_error_pid_checks_treated_as_stale_for_recovery(self):
        stale = {
            "locked": True,
            "reason": "NO_GO: exit_manager_gate_failed:exit_manager_status_stale:500s>300s",
            "source": "flywheel_controller",
            "set_at": "2026-02-14T00:00:00+00:00",
            "pid": 999999,
            "metadata": {"decision": "NO_GO"},
        }
        trading_guard.LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        trading_guard.LOCK_FILE.write_text(json.dumps(stale))

        original_kill = os.kill

        def kill_raises_permission(_pid: int, _sig: int) -> None:
            raise PermissionError(13, "Permission denied")

        os.kill = kill_raises_permission  # type: ignore[method-assign]
        try:
            state = trading_guard.read_trading_lock()
        finally:
            os.kill = original_kill  # type: ignore[method-assign]

        self.assertFalse(state["locked"])
        self.assertEqual(state["source"], "trading_guard")

    def test_stale_startup_preflight_lock_is_recovered_when_owner_dead(self):
        stale = {
            "locked": True,
            "reason": "Startup preflight blocked: execution_health_not_green:egress_blocked",
            "source": "orchestrator_v2",
            "set_at": "2026-02-14T00:00:00+00:00",
            "pid": 999999,
            "metadata": {"event": "STARTUP_PREFLIGHT_BLOCK"},
        }
        trading_guard.LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        trading_guard.LOCK_FILE.write_text(json.dumps(stale))

        original_pid_check = trading_guard._is_pid_alive
        trading_guard._is_pid_alive = lambda pid: False  # type: ignore[misc]
        try:
            state = trading_guard.read_trading_lock()
        finally:
            trading_guard._is_pid_alive = original_pid_check

        self.assertFalse(state["locked"])
        self.assertEqual(state["source"], "trading_guard")
        self.assertIn("recovered_stale_dead_owner_lock", state["reason"])
