#!/usr/bin/env python3
"""Thread/process-safety tests for execution queue writes."""

import json
import os
import shutil
import sys
import tempfile
import threading
import unittest

from unittest.mock import patch


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))
import advanced_team.execution_agent as execution_agent  # noqa: E402


class TestExecutionAgentQueueSafety(unittest.TestCase):
    """Validate write safety and malformed input handling in ExecutionAgent."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.trade_queue = os.path.join(self.tmpdir, "trade_queue.json")
        self.lock_file = os.path.join(self.tmpdir, "trade_queue.json.lock")
        self._patch_queue = patch.object(
            execution_agent,
            "TRADE_QUEUE_FILE",
            self.trade_queue,
        )
        self._patch_lock = patch.object(
            execution_agent,
            "QUEUE_LOCK_FILE",
            self.lock_file,
        )
        self._patch_queue.start()
        self._patch_lock.start()
        self.addCleanup(self._patch_queue.stop)
        self.addCleanup(self._patch_lock.stop)
        self.addCleanup(shutil.rmtree, self.tmpdir)

    def _make_agent(self, cycle_msgs=None, publish_capture=None):
        if cycle_msgs is None:
            cycle_msgs = []
        if publish_capture is None:
            publish_capture = []

        class _DummyBus:
            def publish(self, **kwargs):
                publish_capture.append(kwargs)
                return 1

            def read_latest(self, *args, **kwargs):
                return cycle_msgs

        with patch.object(execution_agent, "MessageBus", return_value=_DummyBus()):
            agent = execution_agent.ExecutionAgent()
        return agent, publish_capture

    def test_invalid_trade_record_is_skipped(self):
        agent, _ = self._make_agent()
        result = agent._write_to_queue({"pair": "BTCUSD", "size_usd": 0.5})

        self.assertFalse(result)
        self.assertFalse(os.path.exists(self.trade_queue))

    def test_corrupt_queue_file_is_recovered(self):
        with open(self.trade_queue, "w") as f:
            f.write("{ this is not json }")

        agent, _ = self._make_agent()
        result = agent._write_to_queue(
            {
                "pair": "btc-usdc",
                "size_usd": 5,
                "direction": "BUY",
                "confidence": 0.75,
            }
        )

        self.assertTrue(result)
        with open(self.trade_queue) as fh:
            payload = json.loads(fh.read())
        self.assertIn("trades", payload)
        self.assertEqual(len(payload["trades"]), 1)
        self.assertEqual(payload["trades"][0]["pair"], "BTC-USDC")
        self.assertEqual(payload["trades"][0]["size_usd"], 5.0)

    def test_run_with_invalid_payload_is_skipped_and_not_queued(self):
        msg = {
            "cycle": 7,
            "payload": {
                "verdict": "APPROVED",
                "proposal": {
                    "pair": "btcusdc",
                    "approved_size_usd": 0.5,
                    "direction": "BUY",
                    "confidence": 0.9,
                },
            },
        }
        agent, published = self._make_agent(cycle_msgs=[msg])
        results = agent.run(7)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["action"], "SKIPPED")
        self.assertEqual(results[0]["reason"], "Invalid execution payload")
        self.assertFalse(os.path.exists(self.trade_queue))
        self.assertTrue(published)

    def test_concurrent_queue_writes_remain_json_and_complete(self):
        agent, _ = self._make_agent()
        records = [
            {
                "pair": f"BTC-USDC",
                "size_usd": 5 + i,
                "direction": "BUY",
                "confidence": 0.5,
                "id": f"test-{i}",
            }
            for i in range(20)
        ]
        threads = [
            threading.Thread(target=agent._write_to_queue, args=(records[i],))
            for i in range(len(records))
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with open(self.trade_queue) as fh:
            payload = json.loads(fh.read())
        self.assertIn("trades", payload)
        self.assertGreaterEqual(len(payload["trades"]), 20)
