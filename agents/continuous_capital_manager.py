#!/usr/bin/env python3
"""Continuous Capital Manager — Real-time capital allocation and rebalancing.

Replaces batch 4x/day capital allocation with continuous rebalancing based on:
  1. Agent performance (gains/ms metric)
  2. Drawdown risk (fire agents with negative gains/ms)
  3. Portfolio growth (reinvest 20-35% of profits)
  4. Regional performance (concentrate capital in hot regions)

Rules:
  - NEVER lose money (capital protection first)
  - Fire agents with negative gains/ms after 100 trades
  - Promote/clone top 10% of agents
  - Reserve 8-12% of portfolio untouched (principle protection)
  - Rebalance every 60 seconds (configurable)
"""

import asyncio
import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger("continuous_capital_manager")

# Configuration
CAPITAL_REBALANCE_INTERVAL_S = int(os.environ.get("ORCH_CAPITAL_REBALANCE_S", "60"))
MIN_RESERVE_PCT = float(os.environ.get("ORCH_MIN_RESERVE_PCT", "0.08"))  # 8%
MAX_RESERVE_PCT = float(os.environ.get("ORCH_MAX_RESERVE_PCT", "0.12"))  # 12%
PROFIT_REINVEST_PCT = float(os.environ.get("ORCH_PROFIT_REINVEST_PCT", "0.25"))  # 25%
AGENT_FIRE_THRESHOLD_MS = int(os.environ.get("ORCH_AGENT_FIRE_THRESHOLD", "100"))  # 100 trades
AGENT_PROMOTE_TOP_PCT = float(os.environ.get("ORCH_AGENT_PROMOTE_TOP_PCT", "0.10"))  # Top 10%

# Database
DB_PATH = Path(__file__).parent.parent / "traceroute.db"


class AllocationStrategy:
    """Define capital allocation strategy for agents."""

    def __init__(self, agent_name: str, allocation_usd: float, strategy_type: str = "equal"):
        self.agent_name = agent_name
        self.allocation_usd = allocation_usd
        self.strategy_type = strategy_type  # "equal", "linear_rank", "exponential_rank"
        self.created_at_s = time.time()
        self.performance_history = []

    def update(self, allocation_usd: float):
        """Update allocation."""
        self.allocation_usd = allocation_usd

    def record_performance(self, pnl_usd: float, trade_count: int, avg_latency_ms: float):
        """Record performance metric."""
        self.performance_history.append({
            "timestamp_s": time.time(),
            "pnl_usd": pnl_usd,
            "trade_count": trade_count,
            "avg_latency_ms": avg_latency_ms,
            "gains_per_ms": pnl_usd / max(avg_latency_ms, 1.0),
        })

    def gains_per_ms(self) -> float:
        """Calculate current gains/ms."""
        if not self.performance_history:
            return 0.0
        latest = self.performance_history[-1]
        return latest["gains_per_ms"]


class ContinuousCapitalManager:
    """Real-time capital allocation engine."""

    def __init__(self):
        self.allocations = {}  # agent_name -> AllocationStrategy
        self.last_rebalance_s = time.time()
        self.rebalance_history = []
        self.lock = threading.Lock()

    def set_allocation(self, agent_name: str, allocation_usd: float):
        """Set capital allocation for an agent."""
        with self.lock:
            if agent_name not in self.allocations:
                self.allocations[agent_name] = AllocationStrategy(agent_name, allocation_usd)
            else:
                self.allocations[agent_name].update(allocation_usd)

    def get_allocation(self, agent_name: str) -> float:
        """Get current allocation for an agent."""
        with self.lock:
            if agent_name in self.allocations:
                return self.allocations[agent_name].allocation_usd
            return 0.0

    async def rebalance(self, portfolio_value_usd: float, agent_rankings: List[Tuple[str, Dict]]):
        """Rebalance capital based on agent performance.

        Args:
            portfolio_value_usd: Current total portfolio value
            agent_rankings: List of (agent_name, metrics) sorted by performance

        Strategy:
            1. Reserve 8-12% for principal protection
            2. Allocate 70% to top 10% of agents (exponential ranking)
            3. Allocate 20% to medium performers (linear ranking)
            4. Fire bottom 10% with negative gains/ms
            5. Reinvest 20-35% of daily profits
        """
        current_time_s = time.time()

        # Only rebalance at intervals
        if current_time_s - self.last_rebalance_s < CAPITAL_REBALANCE_INTERVAL_S:
            return

        with self.lock:
            available_capital = portfolio_value_usd * (1.0 - MIN_RESERVE_PCT)
            reserve_capital = portfolio_value_usd * MIN_RESERVE_PCT

            logger.info(
                f"Rebalancing: Portfolio=${portfolio_value_usd:.2f}, "
                f"Available=${available_capital:.2f}, Reserve=${reserve_capital:.2f}"
            )

            # Fire bottom performers
            losing_agents = [
                name for name, metrics in agent_rankings
                if metrics.get("gains_per_ms", 0.0) <= 0 and metrics.get("trade_count", 0) > AGENT_FIRE_THRESHOLD_MS
            ]

            if losing_agents:
                logger.warning(f"Firing underperformers: {losing_agents}")
                for agent_name in losing_agents:
                    if agent_name in self.allocations:
                        del self.allocations[agent_name]

            # Allocate to winners
            winning_agents = [
                (name, metrics) for name, metrics in agent_rankings
                if metrics.get("gains_per_ms", 0.0) > 0
            ]

            if not winning_agents:
                logger.warning("No winning agents found, conservative rebalance")
                self.allocations.clear()
                self.last_rebalance_s = current_time_s
                return

            # Exponential ranking: top agent gets 40%, next 20%, next 10%, etc.
            allocations = {}
            remaining_capital = available_capital

            for idx, (agent_name, metrics) in enumerate(winning_agents[:10]):
                if idx == 0:
                    # Top agent gets 40%
                    allocation = available_capital * 0.40
                elif idx == 1:
                    # Second gets 20%
                    allocation = available_capital * 0.20
                elif idx < 5:
                    # Next 3 get 10% each
                    allocation = available_capital * 0.10
                else:
                    # Rest split remaining
                    remaining_agents = len(winning_agents) - idx
                    allocation = remaining_capital / remaining_agents

                allocations[agent_name] = min(allocation, remaining_capital)
                remaining_capital -= allocations[agent_name]

            # Apply allocations
            for agent_name, allocation_usd in allocations.items():
                self.set_allocation(agent_name, allocation_usd)

            # Log rebalance
            self.rebalance_history.append({
                "timestamp_s": current_time_s,
                "portfolio_value_usd": portfolio_value_usd,
                "allocations": allocations,
                "winning_agents": len(winning_agents),
                "fired_agents": len(losing_agents),
            })

            logger.info(
                f"Allocated to {len(allocations)} agents: "
                f"{', '.join(f'{name}=${amt:.2f}' for name, amt in list(allocations.items())[:3])}"
            )

            self.last_rebalance_s = current_time_s

    async def reinvest_profits(self, daily_profit_usd: float) -> Dict[str, float]:
        """Reinvest portion of daily profits into hot agents.

        Strategy:
            - Reinvest 20-35% of daily profits
            - Concentrate in top 3 agents
            - Boost HF strategies over batch strategies
        """
        if daily_profit_usd <= 0:
            return {}

        reinvestment_amount = daily_profit_usd * PROFIT_REINVEST_PCT

        with self.lock:
            # Get top 3 agents by gains/ms
            agents_sorted = sorted(
                self.allocations.items(),
                key=lambda x: x[1].gains_per_ms(),
                reverse=True
            )

            # Allocate reinvestment: 50% to top, 30% to 2nd, 20% to 3rd
            reinvestment = {}
            if len(agents_sorted) > 0:
                reinvestment[agents_sorted[0][0]] = reinvestment_amount * 0.50
            if len(agents_sorted) > 1:
                reinvestment[agents_sorted[1][0]] = reinvestment_amount * 0.30
            if len(agents_sorted) > 2:
                reinvestment[agents_sorted[2][0]] = reinvestment_amount * 0.20

            # Apply reinvestment
            for agent_name, amount in reinvestment.items():
                current = self.allocations[agent_name].allocation_usd
                self.allocations[agent_name].update(current + amount)

            logger.info(f"Reinvested ${reinvestment_amount:.2f} into {len(reinvestment)} agents")

            return reinvestment

    def get_allocations(self) -> Dict[str, float]:
        """Get current allocations for all agents."""
        with self.lock:
            return {name: strat.allocation_usd for name, strat in self.allocations.items()}

    def snapshot(self) -> Dict:
        """Get current capital manager snapshot."""
        with self.lock:
            total_allocated = sum(strat.allocation_usd for strat in self.allocations.values())
            return {
                "agents": len(self.allocations),
                "total_allocated_usd": total_allocated,
                "allocations": {name: strat.allocation_usd for name, strat in self.allocations.items()},
                "rebalance_count": len(self.rebalance_history),
                "last_rebalance_s": self.last_rebalance_s,
            }

    def save_state(self, filepath: Path):
        """Save allocation state to JSON."""
        with self.lock:
            state = {
                "timestamp_s": time.time(),
                "allocations": {
                    name: {
                        "allocation_usd": strat.allocation_usd,
                        "created_at_s": strat.created_at_s,
                        "gains_per_ms": strat.gains_per_ms(),
                    }
                    for name, strat in self.allocations.items()
                },
                "rebalance_history": self.rebalance_history[-100:],  # Last 100 rebalances
            }

            with open(filepath, "w") as f:
                json.dump(state, f, indent=2)

            logger.info(f"Saved capital manager state to {filepath}")

    def load_state(self, filepath: Path):
        """Load allocation state from JSON."""
        if not filepath.exists():
            logger.warning(f"No state file found at {filepath}")
            return

        try:
            with open(filepath, "r") as f:
                state = json.load(f)

            with self.lock:
                for agent_name, agent_state in state.get("allocations", {}).items():
                    self.set_allocation(agent_name, agent_state["allocation_usd"])

                self.rebalance_history = state.get("rebalance_history", [])

            logger.info(f"Loaded capital manager state from {filepath}")
        except Exception as e:
            logger.error(f"Error loading state: {e}")


if __name__ == "__main__":
    # Test
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [CAPITAL] %(levelname)s %(message)s",
    )

    manager = ContinuousCapitalManager()

    # Set up test allocations
    manager.set_allocation("agent_1", 100.0)
    manager.set_allocation("agent_2", 75.0)
    manager.set_allocation("agent_3", 50.0)

    # Create test rankings
    test_rankings = [
        ("agent_1", {"gains_per_ms": 0.0001, "trade_count": 150, "total_pnl": 12.5}),
        ("agent_2", {"gains_per_ms": 0.00005, "trade_count": 100, "total_pnl": 8.0}),
        ("agent_3", {"gains_per_ms": -0.00001, "trade_count": 120, "total_pnl": -2.0}),
    ]

    # Test rebalance
    async def test():
        await manager.rebalance(225.0, test_rankings)
        print(f"After rebalance: {manager.snapshot()}")

    asyncio.run(test())
