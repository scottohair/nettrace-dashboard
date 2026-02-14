#!/usr/bin/env python3
"""Autonomy Coordinator for NetTrace.

Master coordinator preventing conflicts between autonomous systems:
  - Phase 1: Deployment (deploy_controller.py)
  - Phase 2: Parameter Optimization (parameter_optimizer.py)
  - Phase 3: Strategy Discovery (strategy_discovery_agent.py)

Arbitrates resources and prevents deadlocks:
  - Capital allocation (exits > trades > research)
  - Compute resources (backtest, paper trade, live limits)
  - API rate limits (shared across all agents)
  - Global state synchronization

Usage:
  python autonomy_coordinator.py --mode status
  python autonomy_coordinator.py --mode reserve --resource capital --amount 100
  python autonomy_coordinator.py --mode release --resource capital --amount 100
"""

import json
import logging
import sqlite3
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from enum import Enum

logger = logging.getLogger("autonomy_coordinator")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

BASE = Path(__file__).parent
COORDINATOR_DB = BASE / "autonomy_coordinator.db"
AUTONOMY_STATE = BASE / "autonomy_state.json"
CONFLICT_LOG = BASE / "conflict_log.jsonl"


class ResourceType(Enum):
    """Types of resources that need coordination."""
    CAPITAL = "capital"
    COMPUTE = "compute"
    API_CALLS = "api_calls"


class AgentType(Enum):
    """Types of autonomous agents."""
    DEPLOYER = "deployer"
    OPTIMIZER = "optimizer"
    DISCOVERER = "discoverer"
    TRADER = "trader"


class Priority(Enum):
    """Priority levels for resource allocation."""
    CRITICAL = 1  # Exits, HARDSTOP
    HIGH = 2      # Live trades
    MEDIUM = 3    # Paper trades
    LOW = 4       # Research, backtests


class AutonomyCoordinator:
    """Master coordinator for autonomous systems."""

    # Resource limits
    RESOURCE_LIMITS = {
        "capital": {
            "total_pool": 100.0,  # Total available capital (USD)
            "min_reserve": 10.0,  # Always keep 10% in reserve
        },
        "compute": {
            "max_concurrent_backtests": 3,
            "max_concurrent_paper_trades": 5,
            "max_concurrent_live_trades": float("inf"),
        },
        "api_calls": {
            "calls_per_minute": 600,
            "calls_per_hour": 30000,
        },
    }

    # Priority queue for resource allocation
    PRIORITY_ORDER = [
        (AgentType.TRADER, "exit"),           # Exits (critical)
        (AgentType.TRADER, "live_trade"),     # Live trades (high)
        (AgentType.DEPLOYER, "deploy"),       # Deploy (medium-high)
        (AgentType.TRADER, "paper_trade"),    # Paper trades (medium)
        (AgentType.OPTIMIZER, "backtest"),    # Backtests (low)
        (AgentType.DISCOVERER, "research"),   # Research (low)
    ]

    def __init__(self):
        self._init_db()
        self._lock = threading.RLock()
        self.global_state = self._load_global_state()

    def _init_db(self) -> None:
        """Initialize coordinator database."""
        self.db = sqlite3.connect(str(COORDINATOR_DB))
        self.db.row_factory = sqlite3.Row

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS resource_reservations (
                id TEXT PRIMARY KEY,
                agent_type TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                amount REAL NOT NULL,
                priority INTEGER NOT NULL,
                purpose TEXT,
                reserved_at TEXT NOT NULL,
                expires_at TEXT,
                status TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS state_transitions (
                id INTEGER PRIMARY KEY,
                timestamp TEXT NOT NULL,
                agent_type TEXT NOT NULL,
                state_change TEXT NOT NULL,
                details TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.db.commit()

    def _load_global_state(self) -> Dict:
        """Load global autonomy state."""
        if AUTONOMY_STATE.exists():
            with open(AUTONOMY_STATE) as f:
                return json.load(f)

        return {
            "deployment_in_progress": False,
            "param_optimization_active": False,
            "strategy_discovery_active": False,
            "hardstop_triggered": False,
            "available_capital_usd": self.RESOURCE_LIMITS["capital"]["total_pool"],
            "reserved_capital": {
                "exits": 0.0,
                "live_trades": 0.0,
                "paper_trades": 0.0,
                "backtests": 0.0,
            },
            "active_backtests": 0,
            "active_paper_trades": 0,
            "last_update": datetime.now(timezone.utc).isoformat(),
        }

    def _save_global_state(self) -> None:
        """Save global state to file."""
        self.global_state["last_update"] = datetime.now(timezone.utc).isoformat()

        with open(AUTONOMY_STATE, "w") as f:
            json.dump(self.global_state, f, indent=2)

    def reserve_resource(self, agent_type: AgentType, resource_type: ResourceType,
                        amount: float, purpose: str,
                        ttl_seconds: int = 3600) -> Tuple[bool, str]:
        """Reserve a resource (capital, compute, API calls)."""

        logger.info(f"{agent_type.value} requesting {resource_type.value}: {amount} ({purpose})")

        with self._lock:
            # Check if deployment is in progress
            if resource_type == ResourceType.CAPITAL and self.global_state["deployment_in_progress"]:
                return False, "Deployment in progress, capital frozen"

            # Check HARDSTOP
            if self.global_state["hardstop_triggered"]:
                return False, "HARDSTOP triggered, no resources available"

            # Check specific resource type
            if resource_type == ResourceType.CAPITAL:
                return self._reserve_capital(agent_type, amount, purpose, ttl_seconds)
            elif resource_type == ResourceType.COMPUTE:
                return self._reserve_compute(agent_type, amount, purpose, ttl_seconds)
            elif resource_type == ResourceType.API_CALLS:
                return self._reserve_api_calls(agent_type, amount, purpose, ttl_seconds)

        return False, "Unknown resource type"

    def _reserve_capital(self, agent_type: AgentType, amount: float,
                        purpose: str, ttl_seconds: int) -> Tuple[bool, str]:
        """Reserve capital."""

        available = (
            self.global_state["available_capital_usd"] -
            self.RESOURCE_LIMITS["capital"]["min_reserve"]
        )

        if amount > available:
            return False, f"Insufficient capital: {amount} > {available}"

        # Determine reservation category
        if "exit" in purpose:
            category = "exits"
            priority = Priority.CRITICAL.value
        elif "live_trade" in purpose:
            category = "live_trades"
            priority = Priority.HIGH.value
        elif "paper_trade" in purpose:
            category = "paper_trades"
            priority = Priority.MEDIUM.value
        else:
            category = "backtests"
            priority = Priority.LOW.value

        # Create reservation
        reservation_id = f"{agent_type.value}_{int(time.time() * 1000)}"
        expires_at = (datetime.now(timezone.utc) +
                     timedelta(seconds=ttl_seconds)).isoformat()

        self.db.execute("""
            INSERT INTO resource_reservations
            (id, agent_type, resource_type, amount, priority, purpose, reserved_at, expires_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            reservation_id,
            agent_type.value,
            ResourceType.CAPITAL.value,
            amount,
            priority,
            purpose,
            datetime.now(timezone.utc).isoformat(),
            expires_at,
            "active",
        ))

        # Update global state
        self.global_state["available_capital_usd"] -= amount
        self.global_state["reserved_capital"][category] = \
            self.global_state["reserved_capital"].get(category, 0) + amount
        self._save_global_state()

        self.db.commit()

        logger.info(f"✅ Reserved {amount} for {purpose} (reservation: {reservation_id})")
        return True, f"Reserved: {reservation_id}"

    def _reserve_compute(self, agent_type: AgentType, amount: float,
                        purpose: str, ttl_seconds: int) -> Tuple[bool, str]:
        """Reserve compute resources (backtests, paper trades)."""

        if "backtest" in purpose:
            if self.global_state["active_backtests"] >= \
               self.RESOURCE_LIMITS["compute"]["max_concurrent_backtests"]:
                return False, f"Max concurrent backtests reached: " \
                       f"{self.global_state['active_backtests']}/{self.RESOURCE_LIMITS['compute']['max_concurrent_backtests']}"

            self.global_state["active_backtests"] += 1

        elif "paper_trade" in purpose:
            if self.global_state["active_paper_trades"] >= \
               self.RESOURCE_LIMITS["compute"]["max_concurrent_paper_trades"]:
                return False, f"Max concurrent paper trades reached: " \
                       f"{self.global_state['active_paper_trades']}/{self.RESOURCE_LIMITS['compute']['max_concurrent_paper_trades']}"

            self.global_state["active_paper_trades"] += 1

        self._save_global_state()
        logger.info(f"✅ Reserved compute for {purpose}")
        return True, f"Compute reserved for {purpose}"

    def _reserve_api_calls(self, agent_type: AgentType, amount: float,
                          purpose: str, ttl_seconds: int) -> Tuple[bool, str]:
        """Reserve API call quota."""

        # Simple: check if we have enough quota this minute
        limit = self.RESOURCE_LIMITS["api_calls"]["calls_per_minute"]

        if amount > limit:
            return False, f"API quota exceeded: {amount} > {limit}"

        logger.info(f"✅ Reserved {amount} API calls for {purpose}")
        return True, f"API calls reserved"

    def release_resource(self, reservation_id: str) -> bool:
        """Release a reserved resource."""

        logger.info(f"Releasing reservation: {reservation_id}")

        with self._lock:
            cursor = self.db.execute(
                "SELECT * FROM resource_reservations WHERE id = ?",
                (reservation_id,)
            )
            row = cursor.fetchone()

            if not row:
                logger.warning(f"Reservation not found: {reservation_id}")
                return False

            # Release capital
            if row["resource_type"] == ResourceType.CAPITAL.value:
                self.global_state["available_capital_usd"] += row["amount"]

                # Find category
                category = None
                for cat, reserved in self.global_state["reserved_capital"].items():
                    if reserved > 0:
                        category = cat
                        break

                if category:
                    self.global_state["reserved_capital"][category] = \
                        max(0, self.global_state["reserved_capital"][category] - row["amount"])

            # Release compute
            elif row["resource_type"] == ResourceType.COMPUTE.value:
                if "backtest" in row["purpose"]:
                    self.global_state["active_backtests"] = \
                        max(0, self.global_state["active_backtests"] - 1)
                elif "paper_trade" in row["purpose"]:
                    self.global_state["active_paper_trades"] = \
                        max(0, self.global_state["active_paper_trades"] - 1)

            # Mark as released
            self.db.execute(
                "UPDATE resource_reservations SET status = ? WHERE id = ?",
                ("released", reservation_id)
            )

            self._save_global_state()
            self.db.commit()

            logger.info(f"✅ Released reservation: {reservation_id}")
            return True

    def detect_conflicts(self) -> List[Dict]:
        """Detect conflicts between autonomous systems."""

        conflicts = []

        # Conflict 1: Deploy during param optimization
        if self.global_state["deployment_in_progress"] and \
           self.global_state["param_optimization_active"]:
            conflicts.append({
                "type": "resource_contention",
                "agents": ["deployer", "optimizer"],
                "description": "Deploy in progress conflicts with param optimization",
                "resolution": "Queue param optimization until deploy complete",
            })

        # Conflict 2: Multiple strategies want same capital
        total_reserved = sum(self.global_state["reserved_capital"].values())
        if total_reserved > self.global_state["available_capital_usd"]:
            conflicts.append({
                "type": "capital_overallocation",
                "amount": total_reserved - self.global_state["available_capital_usd"],
                "description": "More capital reserved than available",
                "resolution": "Use multi-armed bandit allocation by Sharpe ratio",
            })

        # Conflict 3: HARDSTOP during discovery
        if self.global_state["hardstop_triggered"] and \
           self.global_state["strategy_discovery_active"]:
            conflicts.append({
                "type": "safety_override",
                "agents": ["discoverer"],
                "description": "HARDSTOP triggered, discoverer should pause COLD/WARM",
                "resolution": "Pause COLD/WARM, allow exits only",
            })

        if conflicts:
            logger.warning(f"Detected {len(conflicts)} conflicts")
            for conflict in conflicts:
                logger.warning(f"  - {conflict['description']}: {conflict['resolution']}")

        return conflicts

    def trigger_hardstop(self, reason: str) -> bool:
        """Emergency stop all autonomous systems."""

        logger.error(f"🚨 HARDSTOP triggered: {reason}")

        with self._lock:
            self.global_state["hardstop_triggered"] = True
            self.global_state["deployment_in_progress"] = False
            self.global_state["param_optimization_active"] = False
            self.global_state["strategy_discovery_active"] = False

            self._save_global_state()

            # Log to conflict log
            with open(CONFLICT_LOG, "a") as f:
                f.write(json.dumps({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "type": "hardstop",
                    "reason": reason,
                }) + "\n")

            return True

    def resume_operations(self) -> bool:
        """Resume autonomous operations after HARDSTOP."""

        logger.info("🔄 Resuming operations after HARDSTOP")

        with self._lock:
            self.global_state["hardstop_triggered"] = False
            self._save_global_state()
            return True

    def get_status(self) -> Dict:
        """Get coordinator status."""

        return {
            "coordinator_version": "1.0",
            "global_state": self.global_state,
            "active_conflicts": self.detect_conflicts(),
            "resource_limits": {k: v for k, v in self.RESOURCE_LIMITS.items()},
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Autonomy Coordinator")
    parser.add_argument("--mode", choices=["status", "reserve", "release", "hardstop"],
                        default="status")
    parser.add_argument("--resource", choices=["capital", "compute", "api_calls"])
    parser.add_argument("--amount", type=float)
    parser.add_argument("--purpose")
    parser.add_argument("--reservation-id")

    args = parser.parse_args()

    coordinator = AutonomyCoordinator()

    if args.mode == "status":
        status = coordinator.get_status()
        print(json.dumps(status, indent=2, default=str))

    elif args.mode == "reserve":
        if not args.resource or args.amount is None or not args.purpose:
            print("❌ --resource, --amount, and --purpose required")
            return 1

        resource_type = ResourceType[args.resource.upper()]
        agent_type = AgentType.TRADER  # Default to trader

        success, message = coordinator.reserve_resource(
            agent_type, resource_type, args.amount, args.purpose
        )

        print(json.dumps({
            "success": success,
            "message": message,
            "global_state": coordinator.global_state,
        }, indent=2, default=str))

        return 0 if success else 1

    elif args.mode == "release":
        if not args.reservation_id:
            print("❌ --reservation-id required")
            return 1

        success = coordinator.release_resource(args.reservation_id)
        print(json.dumps({
            "success": success,
            "global_state": coordinator.global_state,
        }, indent=2, default=str))

        return 0 if success else 1

    elif args.mode == "hardstop":
        coordinator.trigger_hardstop("Manual HARDSTOP via CLI")
        print(json.dumps({"status": "HARDSTOP triggered"}, indent=2))

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
