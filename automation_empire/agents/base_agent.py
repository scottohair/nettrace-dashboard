#!/usr/bin/env python3
"""
Base automation agent template
All revenue-generating agents inherit from this
"""
import json
import sqlite3
import time
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

class BaseAgent(ABC):
    """
    Base class for all automation agents
    Implements common patterns: logging, state persistence, health checks
    """

    def __init__(self, agent_id: str, category: str):
        self.agent_id = agent_id
        self.category = category
        self.db_path = Path(f"~/src/quant/automation_empire/agents/{agent_id}.db").expanduser()
        self.state_file = Path(f"~/src/quant/automation_empire/agents/{agent_id}_state.json").expanduser()
        self.log_file = Path(f"~/src/quant/automation_empire/agents/{agent_id}.log").expanduser()

        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(agent_id)

        # Initialize database
        self._init_db()

        # Load state
        self.state = self._load_state()

        # Metrics
        self.metrics = {
            "total_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "total_revenue_usd": 0.0,
            "total_cost_usd": 0.0,
            "net_profit_usd": 0.0,
            "started_at": datetime.utcnow().isoformat(),
            "last_run_at": None,
            "status": "initialized",
        }

    def _init_db(self):
        """Initialize SQLite database for agent state"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Core tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,  -- running, success, failed
                revenue_usd REAL DEFAULT 0.0,
                cost_usd REAL DEFAULT 0.0,
                net_profit_usd REAL DEFAULT 0.0,
                details_json TEXT,
                error_message TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value REAL NOT NULL,
                metadata_json TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discovered_at TEXT NOT NULL,
                opportunity_type TEXT NOT NULL,
                potential_revenue_usd REAL,
                confidence REAL,  -- 0.0 to 1.0
                status TEXT,  -- pending, executing, completed, failed, expired
                details_json TEXT,
                executed_at TEXT,
                result_json TEXT
            )
        """)

        conn.commit()
        conn.close()

    def _load_state(self):
        """Load agent state from JSON"""
        if self.state_file.exists():
            with open(self.state_file) as f:
                return json.load(f)
        return {}

    def _save_state(self):
        """Persist agent state to JSON"""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)

    def _record_run(self, status, revenue=0.0, cost=0.0, details=None, error=None):
        """Record a run to the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO runs (started_at, completed_at, status, revenue_usd, cost_usd,
                             net_profit_usd, details_json, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.utcnow().isoformat(),
            datetime.utcnow().isoformat(),
            status,
            revenue,
            cost,
            revenue - cost,
            json.dumps(details) if details else None,
            error
        ))

        conn.commit()
        conn.close()

        # Update metrics
        self.metrics["total_runs"] += 1
        if status == "success":
            self.metrics["successful_runs"] += 1
        else:
            self.metrics["failed_runs"] += 1

        self.metrics["total_revenue_usd"] += revenue
        self.metrics["total_cost_usd"] += cost
        self.metrics["net_profit_usd"] += (revenue - cost)
        self.metrics["last_run_at"] = datetime.utcnow().isoformat()

    def _record_metric(self, name, value, metadata=None):
        """Record a custom metric"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO metrics (recorded_at, metric_name, metric_value, metadata_json)
            VALUES (?, ?, ?, ?)
        """, (
            datetime.utcnow().isoformat(),
            name,
            value,
            json.dumps(metadata) if metadata else None
        ))

        conn.commit()
        conn.close()

    def _record_opportunity(self, opp_type, potential_revenue, confidence, details):
        """Record a discovered opportunity"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO opportunities (discovered_at, opportunity_type, potential_revenue_usd,
                                     confidence, status, details_json)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            datetime.utcnow().isoformat(),
            opp_type,
            potential_revenue,
            confidence,
            "pending",
            json.dumps(details)
        ))

        opportunity_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return opportunity_id

    def get_health(self):
        """Return agent health status"""
        return {
            "agent_id": self.agent_id,
            "category": self.category,
            "status": self.metrics["status"],
            "metrics": self.metrics,
            "state_summary": {
                k: v for k, v in self.state.items()
                if k not in ["credentials", "secrets"]
            }
        }

    @abstractmethod
    def scan(self):
        """
        Scan for opportunities
        Returns: List of opportunities
        """
        pass

    @abstractmethod
    def execute(self, opportunity):
        """
        Execute on an opportunity
        Returns: (success: bool, revenue: float, cost: float, details: dict)
        """
        pass

    def run_cycle(self):
        """
        Run one full scan-execute cycle
        """
        self.logger.info(f"Starting cycle for {self.agent_id}")
        self.metrics["status"] = "running"

        try:
            # Scan for opportunities
            opportunities = self.scan()
            self.logger.info(f"Found {len(opportunities)} opportunities")

            # Execute on best opportunities
            total_revenue = 0.0
            total_cost = 0.0
            executed = 0

            for opp in opportunities:
                try:
                    success, revenue, cost, details = self.execute(opp)

                    if success:
                        total_revenue += revenue
                        total_cost += cost
                        executed += 1
                        self.logger.info(f"Executed opportunity: +${revenue - cost:.2f}")

                except Exception as e:
                    self.logger.error(f"Failed to execute opportunity: {e}")

            # Record cycle
            self._record_run(
                status="success",
                revenue=total_revenue,
                cost=total_cost,
                details={
                    "opportunities_found": len(opportunities),
                    "opportunities_executed": executed
                }
            )

            self.metrics["status"] = "idle"
            self.logger.info(f"Cycle complete: +${total_revenue - total_cost:.2f}")

        except Exception as e:
            self.logger.error(f"Cycle failed: {e}")
            self._record_run(status="failed", error=str(e))
            self.metrics["status"] = "error"

        finally:
            self._save_state()

    def run_forever(self, interval_seconds=60):
        """
        Run continuous cycles
        """
        self.logger.info(f"Starting continuous mode (interval={interval_seconds}s)")

        while True:
            self.run_cycle()
            time.sleep(interval_seconds)
