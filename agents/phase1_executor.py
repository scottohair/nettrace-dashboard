#!/usr/bin/env python3
"""Phase 1 Signal Executor — execute pending signals from quick-win agents.

Monitors all Phase 1 agents for pending signals and executes them through
the trading system (risk_controller → order placement → exit_manager).
"""

import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [phase1_executor] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "phase1_executor.log")),
    ]
)
logger = logging.getLogger("phase1_executor")

CONFIG = {"scan_interval": 60}  # Check for signals every 60s


class Phase1Executor:
    """Execute pending signals from Phase 1 agents."""

    def __init__(self):
        self.base_path = Path(__file__).parent
        self._import_trading_deps()
        self.trades_executed = 0

    def _import_trading_deps(self):
        """Import trading dependencies."""
        try:
            from agent_tools import AgentTools
            self.tools = AgentTools()
        except Exception as e:
            logger.warning(f"AgentTools unavailable: {e}")
            self.tools = None

        try:
            from risk_controller import get_controller
            self.rc = get_controller()
        except Exception as e:
            logger.warning(f"RiskController unavailable: {e}")
            self.rc = None

        try:
            from exit_manager import get_exit_manager
            self.exit_mgr = get_exit_manager()
        except Exception as e:
            logger.warning(f"ExitManager unavailable: {e}")
            self.exit_mgr = None

    def _get_pending_signals(self, db_name, table):
        """Fetch pending signals from a database."""
        db_path = self.base_path / db_name
        if not db_path.exists():
            return []

        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(f"SELECT * FROM {table} WHERE status = 'pending' LIMIT 5")
            signals = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return signals
        except Exception as e:
            logger.debug(f"Error reading {db_name}/{table}: {e}")
            return []

    def _update_signal(self, db_name, table, signal_id, status, order_id=None):
        """Update signal status in database."""
        db_path = self.base_path / db_name
        if not db_path.exists():
            return

        try:
            conn = sqlite3.connect(str(db_path))
            if order_id:
                conn.execute(
                    f"UPDATE {table} SET status = ?, order_id = ? WHERE id = ?",
                    (status, order_id, signal_id),
                )
            else:
                conn.execute(f"UPDATE {table} SET status = ? WHERE id = ?", (status, signal_id))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"Error updating {db_name}/{table}: {e}")

    def run_once(self):
        """Execute pending signals from all Phase 1 agents."""
        logger.info("Starting Phase 1 signal execution...")

        # Sentiment Leech
        signals = self._get_pending_signals("sentiment_leech.db", "signals")
        for sig in signals:
            if self._execute_sentiment_signal(sig):
                self.trades_executed += 1

        # Regulatory Scanner
        signals = self._get_pending_signals("regulatory_scanner.db", "signals")
        for sig in signals:
            if self._execute_regulatory_signal(sig):
                self.trades_executed += 1

        # Liquidation Hunter
        sigs = self._get_pending_signals("liquidation_hunter.db", "cascade_predictions")
        for sig in sigs:
            if self._execute_liquidation_signal(sig):
                self.trades_executed += 1

        # Narrative Tracker
        signals = self._get_pending_signals("narrative_tracker.db", "signals")
        for sig in signals:
            if self._execute_narrative_signal(sig):
                self.trades_executed += 1

        # Futures Mispricing
        opps = self._get_pending_signals("futures_mispricing.db", "arbitrage_opportunities")
        for opp in opps:
            if self._execute_futures_signal(opp):
                self.trades_executed += 1

        logger.info(f"Cycle complete (total: {self.trades_executed} trades)")

    def _execute_sentiment_signal(self, signal):
        """Execute sentiment leech signal."""
        if not self.tools:
            return False

        try:
            pair = signal.get("pair")
            direction = signal.get("direction", "BUY")
            confidence = signal.get("confidence", 0.6)
            signal_id = signal.get("id")

            logger.info(f"EXECUTE sentiment_leech: {direction} {pair} (conf={confidence:.2f})")
            # Trading logic would go here - for now just mark as processed
            self._update_signal("sentiment_leech.db", "signals", signal_id, "pending")
            return False  # Don't count until actually executed
        except Exception as e:
            logger.error(f"Error executing sentiment signal: {e}")
            return False

    def _execute_regulatory_signal(self, signal):
        """Execute regulatory scanner signal."""
        if not self.tools:
            return False
        try:
            pair = signal.get("pair")
            direction = signal.get("direction", "BUY")
            signal_id = signal.get("id")
            logger.info(f"EXECUTE regulatory_scanner: {direction} {pair}")
            return False
        except Exception:
            return False

    def _execute_liquidation_signal(self, signal):
        """Execute liquidation hunter signal."""
        if not self.tools:
            return False
        try:
            pair = signal.get("pair")
            signal_id = signal.get("id")
            logger.info(f"EXECUTE liquidation_hunter: BUY {pair}")
            return False
        except Exception:
            return False

    def _execute_narrative_signal(self, signal):
        """Execute narrative tracker signal."""
        if not self.tools:
            return False
        try:
            direction = signal.get("direction", "BUY")
            signal_id = signal.get("id")
            logger.info(f"EXECUTE narrative_tracker: {direction}")
            return False
        except Exception:
            return False

    def _execute_futures_signal(self, opp):
        """Execute futures mispricing signal."""
        if not self.tools:
            return False
        try:
            pair = opp.get("pair")
            arb_type = opp.get("arb_type")
            opp_id = opp.get("id")
            logger.info(f"EXECUTE futures_mispricing: {arb_type} for {pair}")
            return False
        except Exception:
            return False


def main():
    """Main executor loop."""
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    executor = Phase1Executor()

    if cmd == "once":
        executor.run_once()
        return

    # Default: run daemon loop
    logger.info(f"Starting Phase 1 executor (interval={CONFIG['scan_interval']}s)")
    while True:
        try:
            executor.run_once()
        except Exception as e:
            logger.error(f"Error in execution cycle: {e}", exc_info=True)

        time.sleep(CONFIG["scan_interval"])


if __name__ == "__main__":
    main()
