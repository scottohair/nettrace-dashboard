#!/usr/bin/env python3
"""Tests for FlyAgentRunner lifecycle semantics."""

import os
import unittest
from unittest.mock import patch
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))
import fly_agent_runner


class TestFlyAgentRunner(unittest.TestCase):
    @patch("fly_agent_runner.get_region")
    def test_start_and_stop_primary_region(self, mock_region):
        """start() should launch threads for primary region and stop() should clear state."""
        mock_region.return_value = "ewr"
        with patch("fly_agent_runner.get_agents_for_region") as mock_agents:
            mock_agents.return_value = [{"name": "capital_allocator"}, {"name": "sniper"}]

            runner = fly_agent_runner.FlyAgentRunner()
            runner._get_agent_run_fn = lambda name: lambda: None

            runner.start()
            try:
                status = runner.status()
                self.assertTrue(status["running"])
                self.assertTrue(status["is_primary"])
                self.assertEqual(set(status["agents"].keys()), {"capital_allocator", "sniper"})
                self.assertEqual(len(runner.agents), 2)
            finally:
                runner.stop()
                self.assertFalse(runner.running)
                self.assertEqual(runner.agents, {})
                self.assertEqual(runner.assistants, {})

    @patch("fly_agent_runner.get_region")
    def test_stop_cleans_state_when_not_running(self, mock_region):
        """stop() is idempotent when already stopped."""
        mock_region.return_value = "ewr"
        runner = fly_agent_runner.FlyAgentRunner()
        self.assertFalse(runner.running)
        runner.stop()
        self.assertFalse(runner.running)
        self.assertEqual(runner.agents, {})

    @patch("fly_agent_runner.get_region")
    def test_start_is_idempotent(self, mock_region):
        """start() should not start twice while already running."""
        mock_region.return_value = "ewr"
        runner = fly_agent_runner.FlyAgentRunner()
        runner.is_primary = True

        with patch.object(runner, "_start_primary_agents") as start_agents:
            runner.start()
            runner.start()
            self.assertEqual(start_agents.call_count, 1)
            runner.running = False


if __name__ == "__main__":
    unittest.main()
