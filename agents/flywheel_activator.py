#!/usr/bin/env python3
"""Flywheel Activator — Coordinate clawdbot + growth_engine + meta_engine + strategy_pipeline.

This master orchestrator creates a self-reinforcing loop:

  Strategy Pipeline (COLD→WARM→HOT)
       ↓ (feeds winning strategies)
  Growth Engine (signal fusion + position sizing)
       ↓ (generates opportunities)
  Meta Engine (strategy evolution + ML research)
       ↓ (creates new strategies)
  Back to Strategy Pipeline
       ↓ (top of loop)
  ClawdBot (orchestration + alerts)
       ↓ (monitors all agents, surfaces insights)
  Back to Meta Engine (research feedback)

This creates a virtuous cycle where:
1. Pipeline graduates strategies COLD→WARM→HOT
2. Growth engine boosts confidence, meta_engine evolves best performers
3. Meta engine discovers new alpha, feeds back to pipeline
4. ClawdBot orchestrates across all 7 Fly regions + alerts on milestones
5. Loop repeats with increasing portfolio size
"""

import json
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "flywheel_activator.log")),
    ],
)
logger = logging.getLogger("flywheel_activator")

# Paths
DATA_DIR = Path(__file__).parent
FLYWHEEL_STATUS = DATA_DIR / "flywheel_status.json"
FLYWHEEL_CYCLES = DATA_DIR / "flywheel_cycles.jsonl"


class FlywheelActivator:
    """Master orchestrator for the trading flywheel."""

    def __init__(self):
        self.status = {
            "activated": False,
            "start_time": None,
            "cycles_completed": 0,
            "agents": {
                "clawdbot": {"status": "pending", "pid": None},
                "growth_engine": {"status": "pending", "pid": None},
                "meta_engine": {"status": "pending", "pid": None},
                "strategy_pipeline": {"status": "pending", "pid": None},
            },
            "portfolio": {"size": 0, "compounding_rate": 0},
            "alerts": [],
        }
        self.load_status()

    def load_status(self):
        """Load previous activation state."""
        if FLYWHEEL_STATUS.exists():
            try:
                with open(FLYWHEEL_STATUS) as f:
                    saved = json.load(f)
                    self.status.update(saved)
                    logger.info(f"Loaded previous status: {self.status['cycles_completed']} cycles")
            except Exception as e:
                logger.warning(f"Could not load status: {e}")

    def save_status(self):
        """Persist current activation state."""
        with open(FLYWHEEL_STATUS, "w") as f:
            json.dump(self.status, f, indent=2, default=str)

    def start_agent(self, agent_name: str, script_name: str, region: str = "ewr"):
        """Start a single agent (locally or remote via Fly)."""
        try:
            if agent_name == "clawdbot":
                # ClawdBot: deploy as separate Fly app if not already deployed
                logger.info("Starting ClawdBot orchestrator...")
                result = subprocess.run(
                    ["npm", "list", "-g", "openclaw"],
                    capture_output=True,
                    timeout=10,
                )
                if result.returncode == 0:
                    logger.info("OpenClaw installed, starting gateway...")
                    proc = subprocess.Popen(
                        ["openclaw", "gateway", "--port", "18789", "--verbose"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    self.status["agents"]["clawdbot"]["pid"] = proc.pid
                    self.status["agents"]["clawdbot"]["status"] = "running"
                else:
                    logger.warning("OpenClaw not installed, skipping clawdbot")
                    self.status["agents"]["clawdbot"]["status"] = "skipped"
            else:
                # Python agents: start local process (will be picked up by Fly if running there)
                logger.info(f"Starting {agent_name} on region {region}...")
                script_path = DATA_DIR / script_name
                if not script_path.exists():
                    logger.error(f"Script not found: {script_path}")
                    return

                proc = subprocess.Popen(
                    [sys.executable, str(script_path)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=str(DATA_DIR),
                )
                self.status["agents"][agent_name]["pid"] = proc.pid
                self.status["agents"][agent_name]["status"] = "running"
                logger.info(f"{agent_name} started with PID {proc.pid}")

            self.save_status()

        except Exception as e:
            logger.error(f"Failed to start {agent_name}: {e}")
            self.status["agents"][agent_name]["status"] = "failed"
            self.save_status()

    def activate_flywheel(self):
        """Kick off all 4 agents in coordination."""
        logger.info("=" * 80)
        logger.info("FLYWHEEL ACTIVATION SEQUENCE")
        logger.info("=" * 80)

        # Order matters: pipeline seeds growth_engine, growth_engine feeds meta_engine, meta_engine improves pipeline
        agents = [
            ("strategy_pipeline", "strategy_pipeline.py"),
            ("growth_engine", "growth_engine.py"),
            ("meta_engine", "meta_engine.py"),
            ("clawdbot", "clawdbot_gateway.py"),  # Orchestrator (runs last)
        ]

        for agent_name, script in agents:
            logger.info(f"\n>>> Starting Agent: {agent_name}")
            self.start_agent(agent_name, script)
            time.sleep(2)  # Stagger startup

        self.status["activated"] = True
        self.status["start_time"] = datetime.now(timezone.utc).isoformat()
        self.save_status()

        logger.info("\n" + "=" * 80)
        logger.info("FLYWHEEL ACTIVATED ✓")
        logger.info("=" * 80)
        logger.info("Agent Status:")
        for agent, info in self.status["agents"].items():
            logger.info(f"  {agent:20s} → {info['status']:10s} (PID: {info['pid']})")
        logger.info("=" * 80)

    def monitor_cycle(self):
        """Monitor one flywheel cycle and log metrics."""
        try:
            # Check portfolio size
            from sniper import SnipperTradingAgent
            try:
                sniper = SnipperTradingAgent()
                portfolio = sniper.get_portfolio()
                if portfolio:
                    self.status["portfolio"]["size"] = portfolio.get("total_value", 0)
            except Exception:
                pass

            # Check strategy pipeline promotions
            pipeline_db = DATA_DIR / "pipeline.db"
            if pipeline_db.exists():
                try:
                    conn = sqlite3.connect(str(pipeline_db))
                    c = conn.cursor()
                    c.execute("SELECT COUNT(*) FROM strategies WHERE stage='HOT'")
                    hot_count = c.fetchone()[0]
                    c.execute("SELECT COUNT(*) FROM strategies WHERE stage='WARM'")
                    warm_count = c.fetchone()[0]
                    c.execute("SELECT COUNT(*) FROM strategies WHERE stage='COLD'")
                    cold_count = c.fetchone()[0]
                    conn.close()

                    logger.info(
                        f"Pipeline: COLD={cold_count}, WARM={warm_count}, HOT={hot_count}"
                    )
                except Exception as e:
                    logger.warning(f"Could not read pipeline DB: {e}")

            self.status["cycles_completed"] += 1
            self.save_status()

            # Log cycle metrics
            cycle = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "cycle": self.status["cycles_completed"],
                "portfolio_size": self.status["portfolio"]["size"],
                "agents_running": sum(
                    1
                    for a in self.status["agents"].values()
                    if a["status"] == "running"
                ),
            }
            with open(FLYWHEEL_CYCLES, "a") as f:
                f.write(json.dumps(cycle) + "\n")

        except Exception as e:
            logger.error(f"Monitor cycle failed: {e}")

    def run(self):
        """Main loop: activate, monitor, persist."""
        if not self.status["activated"]:
            self.activate_flywheel()

        logger.info("Starting monitor loop (checking every 60s)...")
        try:
            while True:
                self.monitor_cycle()
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Shutting down flywheel...")
            self.status["activated"] = False
            self.save_status()


def main():
    """Entry point."""
    activator = FlywheelActivator()

    # Auto-activate if not already running
    if not activator.status.get("activated"):
        activator.activate_flywheel()

    # Start monitoring
    activator.run()


if __name__ == "__main__":
    main()
