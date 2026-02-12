#!/usr/bin/env python3
"""AdvancedTeam Coordinator — runs the round-robin DFA loop.

DFA (Deterministic Finite Automaton) pattern:
  Research -> Strategy -> Risk -> Execution -> Learning -> Research -> ...

Each cycle:
  1. ResearchAgent gathers market data -> publishes research_memo
  2. StrategyAgent reads memo -> publishes strategy_proposals
  3. RiskAgent evaluates proposals -> publishes risk_verdicts
  4. ExecutionAgent processes verdicts -> publishes execution_results
  5. LearningAgent reviews results -> publishes learning_insights
  6. (back to Research with insights from Learning)

Timing:
  - Full cycle every 2 minutes (research is the bottleneck — HTTP calls)
  - Message bus GC every 50 cycles (truncate old messages)
  - Status print every 10 cycles

Usage:
  python advanced_team/coordinator.py           # Run continuously
  python advanced_team/coordinator.py --once     # Run one cycle and exit
  python advanced_team/coordinator.py --status   # Print team status
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Setup paths — coordinator lives in advanced_team/ but agents/ is the working dir
_TEAM_DIR = str(Path(__file__).resolve().parent)
_AGENTS_DIR = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _AGENTS_DIR)

# Load .env from parent agents/ dir
_env_path = Path(_AGENTS_DIR) / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

# Logging — to both console and team.log
LOG_FILE = str(Path(_TEAM_DIR) / "team.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ],
)
logger = logging.getLogger("coordinator")

from advanced_team.message_bus import MessageBus
from advanced_team.research_agent import ResearchAgent
from advanced_team.strategy_agent import StrategyAgent
from advanced_team.risk_agent import RiskAgent
from advanced_team.execution_agent import ExecutionAgent
from advanced_team.learning_agent import LearningAgent

# DFA cycle timing
CYCLE_INTERVAL = 120   # seconds between full cycles
AGENT_DELAY = 2        # seconds between agent steps (let messages settle)
GC_EVERY_N_CYCLES = 50  # truncate bus every N cycles
STATUS_EVERY_N = 10     # print status every N cycles
MAX_AGENT_ERRORS = 5    # max consecutive errors before skipping an agent


class Coordinator:
    """Runs the round-robin DFA loop across all 5 agents."""

    def __init__(self):
        self.bus = MessageBus()
        self.agents = {
            "research": ResearchAgent(),
            "strategy": StrategyAgent(),
            "risk": RiskAgent(),
            "execution": ExecutionAgent(),
            "learning": LearningAgent(),
        }
        self.dfa_order = ["research", "strategy", "risk", "execution", "learning"]
        self.cycle = 0
        self.running = True
        self.error_counts = {name: 0 for name in self.dfa_order}
        self.start_time = time.time()

    def run_cycle(self):
        """Execute one full DFA cycle: Research -> Strategy -> Risk -> Execution -> Learning."""
        self.cycle += 1
        cycle_start = time.time()
        logger.info("=" * 60)
        logger.info("CYCLE %d STARTING at %s",
                     self.cycle, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
        logger.info("=" * 60)

        results = {}

        for agent_name in self.dfa_order:
            if not self.running:
                break

            # Skip agents with too many consecutive errors
            if self.error_counts[agent_name] >= MAX_AGENT_ERRORS:
                logger.warning("Skipping %s — %d consecutive errors",
                               agent_name, self.error_counts[agent_name])
                # Reset every 10 cycles to retry
                if self.cycle % 10 == 0:
                    self.error_counts[agent_name] = 0
                    logger.info("Resetting error count for %s", agent_name)
                continue

            agent = self.agents[agent_name]
            step_start = time.time()

            try:
                result = agent.run(self.cycle)
                results[agent_name] = result
                elapsed = time.time() - step_start
                self.error_counts[agent_name] = 0  # Reset on success

                # Log step completion
                if agent_name == "research":
                    n_prices = len(result.get("prices", {})) if isinstance(result, dict) else 0
                    logger.info("  [%s] %.1fs — %d price pairs gathered",
                                agent_name, elapsed, n_prices)
                elif agent_name == "strategy":
                    n_proposals = len(result) if isinstance(result, list) else 0
                    logger.info("  [%s] %.1fs — %d proposals generated",
                                agent_name, elapsed, n_proposals)
                elif agent_name == "risk":
                    n_approved = sum(1 for v in (result or [])
                                     if isinstance(v, dict) and v.get("verdict") == "APPROVED")
                    logger.info("  [%s] %.1fs — %d/%d approved",
                                agent_name, elapsed, n_approved, len(result or []))
                elif agent_name == "execution":
                    n_exec = sum(1 for r in (result or [])
                                 if isinstance(r, dict) and r.get("action") in ("EXECUTED", "QUEUED"))
                    logger.info("  [%s] %.1fs — %d trades processed",
                                agent_name, elapsed, n_exec)
                elif agent_name == "learning":
                    sharpe = result.get("sharpe_ratio", 0) if isinstance(result, dict) else 0
                    portfolio = result.get("portfolio_value", 0) if isinstance(result, dict) else 0
                    logger.info("  [%s] %.1fs — portfolio=$%.2f Sharpe=%.2f",
                                agent_name, elapsed, portfolio, sharpe)

            except Exception as e:
                self.error_counts[agent_name] += 1
                elapsed = time.time() - step_start
                logger.error("  [%s] FAILED (%.1fs): %s (error #%d)",
                             agent_name, elapsed, e, self.error_counts[agent_name])

            time.sleep(AGENT_DELAY)

        cycle_elapsed = time.time() - cycle_start
        logger.info("CYCLE %d COMPLETE in %.1fs", self.cycle, cycle_elapsed)

        # Garbage collect the message bus periodically
        if self.cycle % GC_EVERY_N_CYCLES == 0:
            self.bus.truncate(keep_last_n=500)
            logger.info("Message bus truncated (keep last 500)")

        return results

    def print_status(self):
        """Print team status summary."""
        uptime = time.time() - self.start_time
        hours = int(uptime // 3600)
        mins = int((uptime % 3600) // 60)

        print(f"\n{'='*65}")
        print(f"  ADVANCED TEAM STATUS — Cycle {self.cycle} | Uptime: {hours}h {mins}m")
        print(f"{'='*65}")

        for name in self.dfa_order:
            agent = self.agents[name]
            state = agent.state if hasattr(agent, "state") else {}
            errors = self.error_counts.get(name, 0)
            status = "OK" if errors == 0 else f"ERR({errors})"
            cycle_count = state.get("cycle_count", 0)
            print(f"  {name:<12} {status:<10} cycles={cycle_count}")

        # Message bus stats
        max_id = self.bus.get_max_id()
        print(f"\n  Message bus: {max_id} messages total")
        print(f"  Bus file: {self.bus.bus_file}")

        # Trade queue
        queue_file = str(Path(_TEAM_DIR) / "trade_queue.json")
        if os.path.exists(queue_file):
            try:
                with open(queue_file) as f:
                    queue = json.load(f)
                n_trades = len(queue.get("trades", []))
                print(f"  Trade queue: {n_trades} pending")
            except Exception:
                pass

        # Performance summary
        perf_file = str(Path(_TEAM_DIR) / "performance.json")
        if os.path.exists(perf_file):
            try:
                with open(perf_file) as f:
                    perf = json.load(f)
                print(f"\n  Performance:")
                print(f"    Portfolio: ${perf.get('last_portfolio_value', 0):.2f}")
                print(f"    Peak: ${perf.get('peak_portfolio', 0):.2f}")
                print(f"    Max DD: {perf.get('max_drawdown_pct', 0):.1f}%")
                print(f"    Total trades: {len(perf.get('trades', []))}")
            except Exception:
                pass

        print(f"{'='*65}\n")

    def run(self, once=False):
        """Main coordinator loop."""
        logger.info("AdvancedTeam Coordinator starting (PID %d)", os.getpid())
        logger.info("DFA order: %s", " -> ".join(self.dfa_order))
        logger.info("Cycle interval: %ds | Agent delay: %ds", CYCLE_INTERVAL, AGENT_DELAY)

        # Signal handlers for graceful shutdown
        def handle_shutdown(signum, frame):
            logger.info("Received signal %d, shutting down...", signum)
            self.running = False

        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)

        if once:
            # Single cycle mode
            self.run_cycle()
            self.print_status()
            return

        # Continuous loop
        while self.running:
            try:
                self.run_cycle()

                # Status print
                if self.cycle % STATUS_EVERY_N == 0:
                    self.print_status()

                # Wait for next cycle
                if self.running:
                    logger.info("Next cycle in %ds...", CYCLE_INTERVAL)
                    # Interruptible sleep
                    for _ in range(CYCLE_INTERVAL):
                        if not self.running:
                            break
                        time.sleep(1)

            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt — shutting down")
                self.running = False
            except Exception as e:
                logger.error("Coordinator error: %s", e, exc_info=True)
                time.sleep(30)

        logger.info("AdvancedTeam Coordinator stopped after %d cycles", self.cycle)
        self.print_status()


def main():
    """Entry point."""
    coordinator = Coordinator()

    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg in ("--once", "once"):
            coordinator.run(once=True)
        elif arg in ("--status", "status"):
            coordinator.print_status()
        else:
            print("Usage: coordinator.py [--once|--status]")
            print("  (no args) — run continuously")
            print("  --once    — run one cycle and exit")
            print("  --status  — print team status")
    else:
        coordinator.run()


if __name__ == "__main__":
    main()
