#!/usr/bin/env python3
"""Tests for venue onboarding worker auto-accept behavior."""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))
import venue_onboarding_worker  # noqa: E402


class TestVenueOnboardingAutoAccept(unittest.TestCase):
    def test_auto_accept_promotes_pending_human(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            queue_path = base / "queue.json"
            status_path = base / "status.json"
            events_path = base / "events.jsonl"
            db_path = base / "trader.db"

            queue_payload = {
                "items": [
                    {
                        "task_id": "dex:test:1:sandbox_order_test",
                        "venue_id": "dex:test",
                        "step": "sandbox_order_test",
                        "kind": "cex",
                        "status": "pending_human",
                        "rank": 1,
                    }
                ]
            }
            queue_path.write_text(json.dumps(queue_payload))

            out = venue_onboarding_worker.run_once(
                max_tasks=4,
                queue_path=queue_path,
                status_path=status_path,
                events_path=events_path,
                db_path=db_path,
                network_smoke=False,
                parallelism=1,
                auto_accept_always=True,
            )
            self.assertTrue(out["ok"])
            self.assertEqual(out["auto_accepted_human"], 1)
            self.assertEqual(out["remaining_pending_human"], 0)

            final_queue = json.loads(queue_path.read_text())
            item = final_queue["items"][0]
            self.assertEqual(item["status"], "approved_human")
            self.assertTrue(bool(item.get("auto_accepted_human")))
            self.assertEqual(final_queue.get("manual_action_items"), 0)

    def test_without_auto_accept_keeps_pending_human(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            queue_path = base / "queue.json"
            status_path = base / "status.json"
            events_path = base / "events.jsonl"
            db_path = base / "trader.db"

            queue_payload = {
                "items": [
                    {
                        "task_id": "dex:test:1:sandbox_order_test",
                        "venue_id": "dex:test",
                        "step": "sandbox_order_test",
                        "kind": "cex",
                        "status": "pending_human",
                        "rank": 1,
                    }
                ]
            }
            queue_path.write_text(json.dumps(queue_payload))

            out = venue_onboarding_worker.run_once(
                max_tasks=4,
                queue_path=queue_path,
                status_path=status_path,
                events_path=events_path,
                db_path=db_path,
                network_smoke=False,
                parallelism=1,
                auto_accept_always=False,
            )
            self.assertTrue(out["ok"])
            self.assertEqual(out["auto_accepted_human"], 0)
            self.assertEqual(out["remaining_pending_human"], 1)

            final_queue = json.loads(queue_path.read_text())
            self.assertEqual(final_queue["items"][0]["status"], "pending_human")
            self.assertEqual(final_queue.get("manual_action_items"), 1)


if __name__ == "__main__":
    unittest.main()
