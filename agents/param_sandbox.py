#!/usr/bin/env python3
"""Parameter Sandbox for A/B Testing and Paper Trading.

Provides isolated environments for testing parameter changes before
deploying to live trading.

Modes:
  - COLD: Historical backtest (7 days)
  - WARM: Paper trading vs baseline (4 hours)
  - LIMITED: Live trading with 10% capital (24 hours)
  - FULL: Live deployment with full capital

Usage:
  python param_sandbox.py --mode create --baseline current_params.json
  python param_sandbox.py --mode cold --test-params test_params.json
  python param_sandbox.py --mode warm --baseline-id baseline_001
  python param_sandbox.py --mode status
"""

import json
import logging
import sqlite3
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, List
from enum import Enum

logger = logging.getLogger("param_sandbox")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

BASE = Path(__file__).parent
SANDBOX_DB = BASE / "param_sandbox.db"
SANDBOX_RESULTS_LOG = BASE / "param_sandbox_results.jsonl"


class SandboxMode(Enum):
    """Parameter testing modes."""
    COLD = "COLD"      # Historical backtest
    WARM = "WARM"      # Paper trading
    LIMITED = "LIMITED"  # Live trading 10% capital
    FULL = "FULL"      # Live deployment


class TestStatus(Enum):
    """Test execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    REJECTED = "rejected"


class ParameterSandbox:
    """A/B testing and paper trading framework for parameters."""

    # Promotion gates between modes
    COLD_REQUIREMENTS = {
        "min_sharpe_ratio": 0.5,
        "min_win_rate": 0.40,
        "positive_pnl": True,
    }

    WARM_REQUIREMENTS = {
        "min_sharpe_ratio_improvement": 0.1,  # +10% vs baseline
        "min_win_rate_improvement": 0.02,      # +2% vs baseline
        "max_drawdown": 0.08,                  # 8% max
        "duration_hours": 4,
    }

    LIMITED_REQUIREMENTS = {
        "min_sharpe_ratio_improvement": 0.15,  # +15% vs baseline
        "min_positive_trades": 10,
        "max_realized_loss_usd": 5,
        "duration_hours": 24,
    }

    def __init__(self):
        self._init_db()

    def _init_db(self) -> None:
        """Initialize sandbox database."""
        self.db = sqlite3.connect(str(SANDBOX_DB))
        self.db.row_factory = sqlite3.Row

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS sandbox_tests (
                id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                baseline_id TEXT,
                test_params TEXT NOT NULL,
                status TEXT NOT NULL,
                start_time TEXT,
                end_time TEXT,
                duration_seconds INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS sandbox_results (
                id INTEGER PRIMARY KEY,
                test_id TEXT NOT NULL,
                sharpe_ratio REAL,
                win_rate REAL,
                num_trades INTEGER,
                pnl_usd REAL,
                max_drawdown REAL,
                sharpe_vs_baseline REAL,
                win_rate_vs_baseline REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (test_id) REFERENCES sandbox_tests(id)
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS sandbox_evaluations (
                id INTEGER PRIMARY KEY,
                test_id TEXT NOT NULL,
                requirements TEXT NOT NULL,
                passed_requirements TEXT NOT NULL,
                passed BOOLEAN,
                reason TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (test_id) REFERENCES sandbox_tests(id)
            )
        """)

        self.db.commit()

    def create_test(self, mode: SandboxMode, test_params: Dict,
                   baseline_id: Optional[str] = None) -> str:
        """Create a new parameter test."""

        test_id = f"{mode.value}_{int(time.time() * 1000)}"

        self.db.execute("""
            INSERT INTO sandbox_tests (id, mode, baseline_id, test_params, status, start_time)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            test_id,
            mode.value,
            baseline_id,
            json.dumps(test_params),
            TestStatus.PENDING.value,
            datetime.now(timezone.utc).isoformat(),
        ))
        self.db.commit()

        logger.info(f"Created test {test_id} ({mode.value})")
        return test_id

    def execute_cold_test(self, test_id: str) -> Dict:
        """Execute COLD (historical backtest) test."""

        logger.info(f"Starting COLD test {test_id}")

        # Update status
        self.db.execute("""
            UPDATE sandbox_tests SET status = ? WHERE id = ?
        """, (TestStatus.RUNNING.value, test_id))
        self.db.commit()

        try:
            # Get test params
            cursor = self.db.execute(
                "SELECT test_params FROM sandbox_tests WHERE id = ?",
                (test_id,)
            )
            row = cursor.fetchone()
            test_params = json.loads(row["test_params"])

            # Run backtest on last 7 days
            result = self._backtest_7day(test_params)

            # Evaluate against requirements
            passed, reason = self._evaluate_cold(result)

            # Save results
            self.db.execute("""
                INSERT INTO sandbox_results
                (test_id, sharpe_ratio, win_rate, num_trades, pnl_usd, max_drawdown)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                test_id,
                result.get("sharpe_ratio", 0),
                result.get("win_rate", 0),
                result.get("num_trades", 0),
                result.get("pnl_usd", 0),
                result.get("max_drawdown", 0),
            ))

            # Save evaluation
            self.db.execute("""
                INSERT INTO sandbox_evaluations (test_id, requirements, passed_requirements, passed, reason)
                VALUES (?, ?, ?, ?, ?)
            """, (
                test_id,
                json.dumps(self.COLD_REQUIREMENTS),
                json.dumps(result),
                passed,
                reason,
            ))

            # Update test status
            final_status = TestStatus.SUCCESS.value if passed else TestStatus.REJECTED.value
            self.db.execute("""
                UPDATE sandbox_tests SET status = ?, end_time = ?
                WHERE id = ?
            """, (
                final_status,
                datetime.now(timezone.utc).isoformat(),
                test_id,
            ))
            self.db.commit()

            logger.info(f"COLD test {test_id} complete: status={final_status}, reason={reason}")

            return {
                "test_id": test_id,
                "mode": "COLD",
                "status": final_status,
                "result": result,
                "passed": passed,
                "reason": reason,
            }

        except Exception as e:
            logger.error(f"COLD test failed: {e}")
            self.db.execute("""
                UPDATE sandbox_tests SET status = ?, end_time = ?
                WHERE id = ?
            """, (TestStatus.FAILED.value, datetime.now(timezone.utc).isoformat(), test_id))
            self.db.commit()

            return {
                "test_id": test_id,
                "mode": "COLD",
                "status": "failed",
                "error": str(e),
                "passed": False,
            }

    def execute_warm_test(self, test_id: str, baseline_id: str) -> Dict:
        """Execute WARM (paper trading) test vs baseline."""

        logger.info(f"Starting WARM test {test_id} vs baseline {baseline_id}")

        self.db.execute("""
            UPDATE sandbox_tests SET status = ? WHERE id = ?
        """, (TestStatus.RUNNING.value, test_id))
        self.db.commit()

        try:
            # Get test and baseline params
            cursor = self.db.execute(
                "SELECT test_params FROM sandbox_tests WHERE id = ?",
                (test_id,)
            )
            test_params = json.loads(cursor.fetchone()["test_params"])

            # Run paper trading for 4 hours
            test_result = self._paper_trade_4hours(test_params)
            baseline_result = self._paper_trade_4hours({})  # Baseline (current params)

            # Calculate vs baseline
            sharpe_improvement = test_result["sharpe_ratio"] - baseline_result["sharpe_ratio"]
            win_rate_improvement = test_result["win_rate"] - baseline_result["win_rate"]

            # Evaluate
            passed, reason = self._evaluate_warm(test_result, sharpe_improvement, win_rate_improvement)

            # Save results with baseline comparison
            self.db.execute("""
                INSERT INTO sandbox_results
                (test_id, sharpe_ratio, win_rate, num_trades, pnl_usd, max_drawdown, sharpe_vs_baseline, win_rate_vs_baseline)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                test_id,
                test_result["sharpe_ratio"],
                test_result["win_rate"],
                test_result["num_trades"],
                test_result["pnl_usd"],
                test_result["max_drawdown"],
                sharpe_improvement,
                win_rate_improvement,
            ))

            final_status = TestStatus.SUCCESS.value if passed else TestStatus.REJECTED.value
            self.db.execute("""
                UPDATE sandbox_tests SET status = ?, end_time = ?
                WHERE id = ?
            """, (final_status, datetime.now(timezone.utc).isoformat(), test_id))
            self.db.commit()

            logger.info(f"WARM test {test_id} complete: sharpe_improvement={sharpe_improvement:.2f}, "
                       f"passed={passed}")

            return {
                "test_id": test_id,
                "mode": "WARM",
                "status": final_status,
                "test_result": test_result,
                "baseline_result": baseline_result,
                "sharpe_improvement": sharpe_improvement,
                "win_rate_improvement": win_rate_improvement,
                "passed": passed,
                "reason": reason,
            }

        except Exception as e:
            logger.error(f"WARM test failed: {e}")
            self.db.execute("""
                UPDATE sandbox_tests SET status = ?, end_time = ?
                WHERE id = ?
            """, (TestStatus.FAILED.value, datetime.now(timezone.utc).isoformat(), test_id))
            self.db.commit()

            return {
                "test_id": test_id,
                "mode": "WARM",
                "status": "failed",
                "error": str(e),
                "passed": False,
            }

    def _backtest_7day(self, params: Dict) -> Dict:
        """Run backtest on last 7 days of data."""
        import random

        # Mock backtest result
        return {
            "sharpe_ratio": random.uniform(0.8, 2.0),
            "win_rate": random.uniform(0.45, 0.65),
            "num_trades": random.randint(15, 50),
            "pnl_usd": random.uniform(0, 50),
            "max_drawdown": random.uniform(0.02, 0.08),
        }

    def _paper_trade_4hours(self, params: Dict) -> Dict:
        """Run paper trading for 4 hours."""
        import random

        # Mock paper trading result
        return {
            "sharpe_ratio": random.uniform(0.5, 1.5),
            "win_rate": random.uniform(0.40, 0.60),
            "num_trades": random.randint(5, 20),
            "pnl_usd": random.uniform(-5, 20),
            "max_drawdown": random.uniform(0.01, 0.06),
        }

    def _evaluate_cold(self, result: Dict) -> tuple:
        """Evaluate COLD test results."""
        passed = True
        reasons = []

        if result.get("sharpe_ratio", 0) < self.COLD_REQUIREMENTS["min_sharpe_ratio"]:
            passed = False
            reasons.append(f"Sharpe {result.get('sharpe_ratio'):.2f} < {self.COLD_REQUIREMENTS['min_sharpe_ratio']}")

        if result.get("win_rate", 0) < self.COLD_REQUIREMENTS["min_win_rate"]:
            passed = False
            reasons.append(f"Win rate {result.get('win_rate'):.1%} < {self.COLD_REQUIREMENTS['min_win_rate']:.1%}")

        if result.get("pnl_usd", 0) <= 0 and self.COLD_REQUIREMENTS["positive_pnl"]:
            passed = False
            reasons.append("Negative or zero PnL")

        reason = "; ".join(reasons) if reasons else "All requirements met"
        return passed, reason

    def _evaluate_warm(self, test_result: Dict, sharpe_improvement: float,
                      win_rate_improvement: float) -> tuple:
        """Evaluate WARM test results vs baseline."""
        passed = True
        reasons = []

        if sharpe_improvement < self.WARM_REQUIREMENTS["min_sharpe_ratio_improvement"]:
            passed = False
            reasons.append(f"Sharpe improvement {sharpe_improvement:.2f} < {self.WARM_REQUIREMENTS['min_sharpe_ratio_improvement']}")

        if win_rate_improvement < self.WARM_REQUIREMENTS["min_win_rate_improvement"]:
            passed = False
            reasons.append(f"Win rate improvement {win_rate_improvement:.1%} < "
                          f"{self.WARM_REQUIREMENTS['min_win_rate_improvement']:.1%}")

        if test_result.get("max_drawdown", 0) > self.WARM_REQUIREMENTS["max_drawdown"]:
            passed = False
            reasons.append(f"Drawdown {test_result.get('max_drawdown'):.1%} > "
                          f"{self.WARM_REQUIREMENTS['max_drawdown']:.1%}")

        reason = "; ".join(reasons) if reasons else "All requirements met"
        return passed, reason

    def get_test_status(self, test_id: str) -> Dict:
        """Get status of a test."""
        cursor = self.db.execute("""
            SELECT t.*, r.sharpe_ratio, r.win_rate, r.num_trades, r.pnl_usd
            FROM sandbox_tests t
            LEFT JOIN sandbox_results r ON t.id = r.test_id
            WHERE t.id = ?
        """, (test_id,))

        row = cursor.fetchone()
        if not row:
            return {"error": f"Test {test_id} not found"}

        return {
            "test_id": row["id"],
            "mode": row["mode"],
            "status": row["status"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "sharpe_ratio": row["sharpe_ratio"],
            "win_rate": row["win_rate"],
            "num_trades": row["num_trades"],
            "pnl_usd": row["pnl_usd"],
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NetTrace Parameter Sandbox")
    parser.add_argument("--mode", choices=["create", "cold", "warm", "status"],
                        default="status")
    parser.add_argument("--test-params", help="Path to test parameters JSON")
    parser.add_argument("--baseline-id", help="Baseline test ID")
    parser.add_argument("--test-id", help="Test ID")

    args = parser.parse_args()

    sandbox = ParameterSandbox()

    if args.mode == "create":
        if not args.test_params:
            print("❌ --test-params required")
            return 1

        with open(args.test_params) as f:
            params = json.load(f)

        test_id = sandbox.create_test(SandboxMode.COLD, params)
        print(json.dumps({"test_id": test_id}, indent=2))

    elif args.mode == "cold":
        if not args.test_id:
            print("❌ --test-id required")
            return 1

        result = sandbox.execute_cold_test(args.test_id)
        print(json.dumps(result, indent=2, default=str))

    elif args.mode == "status":
        if args.test_id:
            status = sandbox.get_test_status(args.test_id)
            print(json.dumps(status, indent=2, default=str))
        else:
            print("❌ --test-id required")
            return 1

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
