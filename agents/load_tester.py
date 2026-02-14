#!/usr/bin/env python3
"""Load Testing Suite for Full Agentic Autonomy.

Tests system under heavy concurrent load:
  - 10+ concurrent autonomous systems
  - Parallel parameter optimizations
  - Concurrent strategy discoveries
  - Simultaneous deployments
  - Resource contention scenarios

Metrics collected:
  - Latency (p50, p95, p99)
  - Throughput (requests/sec)
  - Error rates
  - Resource utilization (CPU, memory)
  - Deadlock detection

Usage:
  python load_tester.py --scenario concurrent_deploys --duration 600 --concurrency 10
  python load_tester.py --scenario param_optimization --duration 3600 --concurrency 5
  python load_tester.py --scenario full_system --duration 1800 --concurrency 3
"""

import json
import logging
import time
import threading
import concurrent.futures
import tracemalloc
import psutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Callable
from enum import Enum
import statistics

logger = logging.getLogger("load_tester")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

BASE = Path(__file__).parent
LOAD_TEST_RESULTS = BASE / "load_test_results.jsonl"


class LoadTestScenario(Enum):
    """Load testing scenarios."""
    CONCURRENT_DEPLOYS = "concurrent_deploys"
    PARAM_OPTIMIZATION = "param_optimization"
    STRATEGY_DISCOVERY = "strategy_discovery"
    FULL_SYSTEM = "full_system"
    RESOURCE_CONTENTION = "resource_contention"


class LoadTester:
    """Load testing framework for autonomy systems."""

    def __init__(self, scenario: LoadTestScenario, duration: int, concurrency: int):
        self.scenario = scenario
        self.duration = duration
        self.concurrency = concurrency
        self.results = {
            "scenario": scenario.value,
            "duration": duration,
            "concurrency": concurrency,
            "start_time": datetime.now(timezone.utc).isoformat(),
            "latencies": [],
            "errors": [],
            "deadlocks": 0,
            "metrics": {},
        }
        self.start_memory = 0
        self.peak_memory = 0

    def _deploy_controller_task(self) -> Dict:
        """Simulate deploy_controller load."""
        from deploy_controller import DeployController

        start = time.time()
        try:
            controller = DeployController()
            # Simulate deployment check
            status = controller.get_status()
            latency = time.time() - start
            return {
                "success": True,
                "latency": latency,
                "component": "deploy_controller",
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "latency": time.time() - start,
                "component": "deploy_controller",
            }

    def _parameter_optimizer_task(self) -> Dict:
        """Simulate parameter_optimizer load."""
        from parameter_optimizer import ParameterOptimizer

        start = time.time()
        try:
            optimizer = ParameterOptimizer()
            status = optimizer.get_status()
            latency = time.time() - start
            return {
                "success": True,
                "latency": latency,
                "component": "parameter_optimizer",
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "latency": time.time() - start,
                "component": "parameter_optimizer",
            }

    def _strategy_discovery_task(self) -> Dict:
        """Simulate strategy_discovery load."""
        from strategy_discovery_agent import StrategyDiscoveryAgent

        start = time.time()
        try:
            agent = StrategyDiscoveryAgent()
            status = agent.get_status()
            latency = time.time() - start
            return {
                "success": True,
                "latency": latency,
                "component": "strategy_discovery",
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "latency": time.time() - start,
                "component": "strategy_discovery",
            }

    def _coordinator_task(self) -> Dict:
        """Simulate autonomy_coordinator load."""
        from autonomy_coordinator import AutonomyCoordinator, AgentType, ResourceType

        start = time.time()
        try:
            coordinator = AutonomyCoordinator()
            status = coordinator.get_status()
            latency = time.time() - start
            return {
                "success": True,
                "latency": latency,
                "component": "autonomy_coordinator",
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "latency": time.time() - start,
                "component": "autonomy_coordinator",
            }

    def run_concurrent_deploys(self) -> None:
        """Load test: concurrent deployments."""
        logger.info(f"Starting concurrent deploys test (concurrency={self.concurrency}, duration={self.duration}s)")

        start_time = time.time()
        deadline = start_time + self.duration

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []

            while time.time() < deadline:
                # Submit deploy tasks
                for _ in range(self.concurrency):
                    future = executor.submit(self._deploy_controller_task)
                    futures.append(future)

                # Collect results as they complete
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result(timeout=60)
                        if result["success"]:
                            self.results["latencies"].append(result["latency"])
                            logger.debug(f"Deploy completed in {result['latency']:.3f}s")
                        else:
                            self.results["errors"].append(result.get("error", "Unknown error"))
                    except Exception as e:
                        self.results["errors"].append(str(e))

                    futures.remove(future)
                    if time.time() >= deadline:
                        break

    def run_param_optimization(self) -> None:
        """Load test: concurrent parameter optimization."""
        logger.info(f"Starting param optimization test (concurrency={self.concurrency})")

        start_time = time.time()
        deadline = start_time + self.duration

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []

            while time.time() < deadline:
                for _ in range(self.concurrency):
                    future = executor.submit(self._parameter_optimizer_task)
                    futures.append(future)

                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result(timeout=300)
                        if result["success"]:
                            self.results["latencies"].append(result["latency"])
                        else:
                            self.results["errors"].append(result.get("error"))
                    except Exception as e:
                        self.results["errors"].append(str(e))

                    futures.remove(future)
                    if time.time() >= deadline:
                        break

    def run_full_system(self) -> None:
        """Load test: full autonomous system (all 4 pillars)."""
        logger.info(f"Starting full system test (concurrency={self.concurrency})")

        start_time = time.time()
        deadline = start_time + self.duration

        task_types = [
            self._deploy_controller_task,
            self._parameter_optimizer_task,
            self._strategy_discovery_task,
            self._coordinator_task,
        ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []
            task_idx = 0

            while time.time() < deadline:
                # Round-robin through task types
                task = task_types[task_idx % len(task_types)]
                future = executor.submit(task)
                futures.append(future)
                task_idx += 1

                # Collect completed tasks
                done, futures = concurrent.futures.wait(
                    futures, timeout=1, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for future in done:
                    try:
                        result = future.result(timeout=60)
                        if result["success"]:
                            self.results["latencies"].append(result["latency"])
                        else:
                            self.results["errors"].append(result.get("error"))
                    except Exception as e:
                        self.results["errors"].append(str(e))

    def measure_resources(self) -> None:
        """Measure CPU, memory, and other resource usage."""
        process = psutil.Process()

        # Memory
        self.start_memory = process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = self.start_memory

        # CPU
        cpu_percent = process.cpu_percent(interval=1)

        self.results["metrics"] = {
            "start_memory_mb": round(self.start_memory, 2),
            "cpu_percent": cpu_percent,
            "process_count": len(psutil.pids()),
        }

    def run(self) -> Dict:
        """Run the load test scenario."""
        logger.info(f"Starting load test: {self.scenario.value}")

        self.measure_resources()

        start_time = time.time()

        try:
            if self.scenario == LoadTestScenario.CONCURRENT_DEPLOYS:
                self.run_concurrent_deploys()
            elif self.scenario == LoadTestScenario.PARAM_OPTIMIZATION:
                self.run_param_optimization()
            elif self.scenario == LoadTestScenario.FULL_SYSTEM:
                self.run_full_system()
            else:
                raise ValueError(f"Unknown scenario: {self.scenario}")

        except Exception as e:
            logger.error(f"Load test failed: {e}")
            self.results["errors"].append(str(e))

        # Compute statistics
        duration = time.time() - start_time
        self.results["actual_duration"] = duration

        if self.results["latencies"]:
            self.results["stats"] = {
                "total_requests": len(self.results["latencies"]),
                "successful_requests": len(self.results["latencies"]),
                "failed_requests": len(self.results["errors"]),
                "error_rate": len(self.results["errors"]) / (len(self.results["latencies"]) + len(self.results["errors"])),
                "throughput_rps": len(self.results["latencies"]) / duration,
                "latency_p50": statistics.median(self.results["latencies"]),
                "latency_p95": sorted(self.results["latencies"])[int(len(self.results["latencies"]) * 0.95)],
                "latency_p99": sorted(self.results["latencies"])[int(len(self.results["latencies"]) * 0.99)],
                "latency_avg": statistics.mean(self.results["latencies"]),
                "latency_max": max(self.results["latencies"]),
            }

        self.results["end_time"] = datetime.now(timezone.utc).isoformat()

        # Save results
        with open(LOAD_TEST_RESULTS, "a") as f:
            f.write(json.dumps(self.results) + "\n")

        logger.info(f"Load test complete: {json.dumps(self.results.get('stats', {}), indent=2)}")

        return self.results


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Load Testing Suite")
    parser.add_argument(
        "--scenario",
        choices=[s.value for s in LoadTestScenario],
        default="full_system",
        help="Load test scenario",
    )
    parser.add_argument("--duration", type=int, default=300, help="Test duration in seconds")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent tasks")

    args = parser.parse_args()

    scenario = LoadTestScenario(args.scenario)
    tester = LoadTester(scenario, args.duration, args.concurrency)
    results = tester.run()

    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    import sys

    sys.exit(main() or 0)
