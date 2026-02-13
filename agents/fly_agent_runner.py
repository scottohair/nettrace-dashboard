#!/usr/bin/env python3
"""Fly Agent Runner — in-process agent execution for Fly.io containers.

Runs trading agents as daemon threads inside the Flask container. No subprocess.Popen
(unreliable in 256MB containers). Each region runs agents appropriate to its role:

  ewr (primary): sniper, meta_engine, advanced_team, capital_allocator
  lhr/nrt/sin (scouts): signal_scout (scan + anomaly detect + push signals to ewr)

The signal_scout is a lightweight agent that:
  1. Scans nearby exchanges (from DEPLOYMENT_MANIFEST)
  2. Detects latency anomalies (RTT spike > 20% = signal)
  3. Pushes signals to the central DB via /api/v1/signals/push
  4. Sleeps CRYPTO_SCAN_INTERVAL, repeats

Usage:
  # Auto-detect region from FLY_REGION env var:
  runner = FlyAgentRunner()
  runner.start()

  # Check status:
  runner.status()
"""

import json
import logging
import os
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Ensure agents/ is on sys.path
_AGENTS_DIR = str(Path(__file__).resolve().parent)
if _AGENTS_DIR not in sys.path:
    sys.path.insert(0, _AGENTS_DIR)

from fly_deployer import (
    get_region,
    get_agents_for_region,
    DEPLOYMENT_MANIFEST,
    AgentAssistant,
)
from agent_goals import GoalValidator, RULES

logger = logging.getLogger("fly_agent_runner")

# Signal push config
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")
FLY_URL = os.environ.get("APP_URL", "https://nettrace-dashboard.fly.dev")
PRIMARY_REGION = os.environ.get("PRIMARY_REGION", "ewr")
SCOUT_INTERVAL = int(os.environ.get("CRYPTO_SCAN_INTERVAL", "120"))


class FlyAgentRunner:
    """Lightweight in-process agent runner for Fly.io containers."""

    def __init__(self):
        self.region = get_region()
        self.is_primary = (self.region == PRIMARY_REGION or self.region == "local")
        self.agents = {}       # name -> thread
        self.assistants = {}   # name -> AgentAssistant
        self.running = False
        self.goals = GoalValidator()
        self._started_at = None

        logger.info("FlyAgentRunner init: region=%s, is_primary=%s",
                     self.region, self.is_primary)

    def get_agents_for_region(self):
        """Return agent list for current region."""
        return get_agents_for_region(self.region)

    def start(self):
        """Start agents appropriate for this region."""
        if self.running:
            logger.warning("Agent runner already running")
            return

        self.running = True
        self._started_at = time.time()

        # Log the goals every agent is trained on
        logger.info("=== AGENT GOALS (encoded in every decision) ===")
        for num, rule in RULES.items():
            logger.info("  Rule #%d: %s", num, rule)
        logger.info("  Order type: %s (fee: %.1f%%)",
                     GoalValidator.optimal_order_type()["order_type"],
                     GoalValidator.optimal_order_type()["fee"] * 100)
        logger.info("================================================")

        if self.is_primary:
            self._start_primary_agents()
        else:
            self._start_scout_agent()

        agent_names = list(self.agents.keys())
        logger.info("Started %d agent(s) in region %s: %s",
                     len(agent_names), self.region, agent_names)

    def stop(self):
        """Signal all agents to stop."""
        logger.info("Stopping agent runner...")
        self.running = False
        # Threads are daemons — they'll die with the process
        # But give them a moment to clean up
        time.sleep(2)
        self.agents.clear()
        self.assistants.clear()
        logger.info("Agent runner stopped")

    def _start_primary_agents(self):
        """Start the full trading stack on the primary region (ewr)."""
        manifest_agents = get_agents_for_region(self.region)
        logger.info("Primary region %s: starting %d agents from manifest",
                     self.region, len(manifest_agents))

        for agent_def in manifest_agents:
            name = agent_def["name"]
            try:
                self._start_agent_thread(name)
            except Exception as e:
                logger.error("Failed to start agent '%s': %s", name, e)

    def _start_agent_thread(self, name):
        """Start a named agent as a daemon thread."""
        target = self._get_agent_run_fn(name)
        if target is None:
            logger.warning("No run function found for agent '%s', skipping", name)
            return

        thread = threading.Thread(
            target=self._agent_wrapper,
            args=(name, target),
            name=f"agent-{name}",
            daemon=True,
        )
        thread.start()
        self.agents[name] = thread
        self.assistants[name] = AgentAssistant(name)
        logger.info("Started agent thread: %s (tid=%s)", name, thread.ident)

    def _get_agent_run_fn(self, name):
        """Import and return the run function for a named agent.

        Returns a callable that runs the agent's main loop, or None.
        """
        try:
            if name == "sniper":
                from sniper import Sniper
                instance = Sniper()
                return instance.run

            elif name == "meta_engine":
                from meta_engine import MetaEngine
                instance = MetaEngine()
                return instance.run

            elif name == "advanced_team":
                from advanced_team.coordinator import Coordinator
                instance = Coordinator()
                return instance.run

            elif name == "capital_allocator":
                from capital_allocator import CapitalAllocator
                instance = CapitalAllocator()
                return instance.run_loop

            elif name in ("exchange_scanner", "latency_arb",
                          "momentum_scalper", "forex_arb"):
                # Scout-type agents — run signal_scout for non-primary regions
                # On primary, these are informational-only
                return lambda: self._run_signal_scout()

            else:
                logger.warning("Unknown agent '%s' — no loader defined", name)
                return None

        except ImportError as e:
            logger.error("Cannot import agent '%s': %s", name, e)
            return None

    def _agent_wrapper(self, name, target):
        """Wrapper that catches crashes and logs them."""
        restart_count = 0
        max_restarts = 3

        while self.running and restart_count <= max_restarts:
            try:
                logger.info("Agent '%s' starting (attempt %d)", name, restart_count + 1)
                target()
                # If target() returns normally, the agent chose to stop
                logger.info("Agent '%s' exited normally", name)
                break
            except Exception as e:
                restart_count += 1
                logger.error("Agent '%s' crashed (attempt %d/%d): %s",
                             name, restart_count, max_restarts + 1, e,
                             exc_info=True)
                if self.assistants.get(name):
                    self.assistants[name].metrics["crashes"] += 1
                    self.assistants[name].metrics["last_error"] = str(e)[:200]
                if restart_count <= max_restarts and self.running:
                    backoff = min(30 * restart_count, 120)
                    logger.info("Restarting '%s' in %ds...", name, backoff)
                    time.sleep(backoff)

        if restart_count > max_restarts:
            logger.error("Agent '%s' exceeded max restarts (%d), giving up",
                         name, max_restarts)

    # ──────────────────────────────────────────────────────────────────────
    # Signal Scout — for non-primary (scout) regions
    # ──────────────────────────────────────────────────────────────────────

    def _start_scout_agent(self):
        """Start a signal scout for non-primary regions."""
        thread = threading.Thread(
            target=self._agent_wrapper,
            args=("signal_scout", self._run_signal_scout),
            name="agent-signal_scout",
            daemon=True,
        )
        thread.start()
        self.agents["signal_scout"] = thread
        self.assistants["signal_scout"] = AgentAssistant("signal_scout")
        logger.info("Started signal scout for region %s", self.region)

    def _run_signal_scout(self):
        """Scout loop: scan nearby exchanges, detect anomalies, push signals.

        This runs on non-primary regions (lhr, nrt, sin, etc.) and provides
        the cross-region latency divergence that is our information asymmetry edge.
        """
        region_info = DEPLOYMENT_MANIFEST.get(self.region, {})
        exchanges = region_info.get("exchanges", [])
        logger.info("Signal scout starting: region=%s, exchanges=%s",
                     self.region, exchanges)

        while self.running:
            try:
                # 1. Fetch latest crypto latency data from our own API
                signals = self._fetch_local_signals()

                # 2. Detect anomalies (RTT spike > 20%)
                anomalies = [s for s in signals if s.get("is_anomaly")]

                if anomalies:
                    logger.info("Scout %s detected %d anomalies",
                                self.region, len(anomalies))

                    # 3. Push each anomaly as a signal to the central API
                    for anomaly in anomalies:
                        self._push_signal(anomaly)

                # 4. Also push a heartbeat signal so ewr knows we're alive
                self._push_heartbeat()

                # Sleep until next scan
                self._interruptible_sleep(SCOUT_INTERVAL)

            except Exception as e:
                logger.error("Scout loop error: %s", e)
                self._interruptible_sleep(30)

    def _fetch_local_signals(self):
        """Fetch crypto latency data from the local Fly app (same container)."""
        try:
            # Use internal URL (localhost) since we're in the same container
            url = f"http://localhost:{os.environ.get('PORT', '8080')}/api/v1/signals/crypto-latency?hours=1"
            headers = {"Authorization": f"Bearer {NETTRACE_API_KEY}"}
            req = urllib.request.Request(url, headers=headers)
            resp = urllib.request.urlopen(req, timeout=10)
            data = json.loads(resp.read())
            return data.get("exchanges", [])
        except Exception as e:
            logger.debug("Could not fetch local signals: %s", e)
            return []

    def _push_signal(self, anomaly):
        """Push an anomaly signal to the central API for cross-region analysis."""
        if not NETTRACE_API_KEY:
            logger.debug("No API key, skipping signal push")
            return

        payload = {
            "signal_type": "latency_anomaly",
            "target_host": anomaly.get("host", "unknown"),
            "direction": "CAUTION",
            "confidence": min(abs(anomaly.get("deviation_pct", 0)) / 100, 0.99),
            "source_region": self.region,
            "details": {
                "latest_rtt": anomaly.get("latest_rtt"),
                "avg_rtt": anomaly.get("avg_rtt"),
                "deviation_pct": anomaly.get("deviation_pct"),
                "exchange_name": anomaly.get("name", ""),
            },
        }

        try:
            url = f"{FLY_URL}/api/v1/signals/push"
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url, data=data,
                headers={
                    "Authorization": f"Bearer {NETTRACE_API_KEY}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            resp = urllib.request.urlopen(req, timeout=10)
            if resp.status == 200:
                logger.debug("Pushed signal: %s %s (%.1f%% deviation)",
                             anomaly.get("host"), self.region,
                             anomaly.get("deviation_pct", 0))
        except Exception as e:
            logger.debug("Signal push failed: %s", e)

    def _push_heartbeat(self):
        """Push a heartbeat so the primary region knows this scout is alive."""
        if not NETTRACE_API_KEY:
            return

        payload = {
            "signal_type": "scout_heartbeat",
            "target_host": f"scout-{self.region}",
            "direction": "INFO",
            "confidence": 1.0,
            "source_region": self.region,
            "details": {
                "uptime_seconds": int(time.time() - self._started_at) if self._started_at else 0,
                "agents_running": list(self.agents.keys()),
            },
        }

        try:
            url = f"{FLY_URL}/api/v1/signals/push"
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url, data=data,
                headers={
                    "Authorization": f"Bearer {NETTRACE_API_KEY}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            pass  # heartbeat failures are non-critical

    def _interruptible_sleep(self, seconds):
        """Sleep in small increments so we can stop quickly."""
        end = time.time() + seconds
        while self.running and time.time() < end:
            time.sleep(min(5, end - time.time()))

    # ──────────────────────────────────────────────────────────────────────
    # Status
    # ──────────────────────────────────────────────────────────────────────

    def status(self):
        """Return current runner status."""
        return {
            "region": self.region,
            "is_primary": self.is_primary,
            "running": self.running,
            "uptime_seconds": int(time.time() - self._started_at) if self._started_at else 0,
            "agents": {
                name: {
                    "alive": thread.is_alive(),
                    "metrics": self.assistants[name].get_metrics() if name in self.assistants else {},
                }
                for name, thread in self.agents.items()
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


# ---------------------------------------------------------------------------
# Standalone entry point (for testing)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    runner = FlyAgentRunner()
    print(f"Region: {runner.region}")
    print(f"Is primary: {runner.is_primary}")
    print(f"Agents for region: {runner.get_agents_for_region()}")

    if len(sys.argv) > 1 and sys.argv[1] == "run":
        runner.start()
        try:
            while True:
                time.sleep(60)
                print(json.dumps(runner.status(), indent=2))
        except KeyboardInterrupt:
            runner.stop()
    else:
        print("\nUse 'python fly_agent_runner.py run' to start agents")
