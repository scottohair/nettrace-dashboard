#!/usr/bin/env python3
"""Phase 1 Signal Executor — execute pending signals from quick-win agents.

Monitors all Phase 1 agents for pending signals and executes them through
the trading system (risk_controller → order placement → exit_manager).

Allocation: $10 for sentiment_leech (currently enabled)
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

# Phase 1 Capital Allocation
ALLOCATIONS = {
    "sentiment_leech": {"enabled": True, "allocation_usd": 10.0, "max_position_pct": 0.25},
    "regulatory_scanner": {"enabled": False, "allocation_usd": 5.0, "max_position_pct": 0.15},
    "liquidation_hunter": {"enabled": False, "allocation_usd": 5.0, "max_position_pct": 0.20},
    "narrative_tracker": {"enabled": False, "allocation_usd": 5.0, "max_position_pct": 0.15},
    "futures_mispricing": {"enabled": False, "allocation_usd": 5.0, "max_position_pct": 0.10},
}

CONFIG = {"scan_interval": 60}  # Check for signals every 60s


class Phase1Executor:
    """Execute pending signals from Phase 1 agents."""

    def __init__(self):
        self.base_path = Path(__file__).parent
        self._import_trading_deps()
        self.trades_executed = 0
        self.total_capital_used = 0.0

    def _import_trading_deps(self):
        """Import trading dependencies."""
        try:
            from agent_tools import AgentTools
            self.tools = AgentTools()
            logger.info("✓ AgentTools loaded")
        except Exception as e:
            logger.error(f"AgentTools unavailable: {e}")
            self.tools = None

        try:
            from risk_controller import get_controller
            self.rc = get_controller()
            logger.info("✓ RiskController loaded")
        except Exception as e:
            logger.error(f"RiskController unavailable: {e}")
            self.rc = None

        try:
            from exit_manager import get_exit_manager
            self.exit_mgr = get_exit_manager()
            logger.info("✓ ExitManager loaded")
        except Exception as e:
            logger.error(f"ExitManager unavailable: {e}")
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
                    f"UPDATE {table} SET status = ?, order_id = ?, filled_at = datetime('now') WHERE id = ?",
                    (status, order_id, signal_id),
                )
            else:
                conn.execute(f"UPDATE {table} SET status = ? WHERE id = ?", (status, signal_id))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"Error updating {db_name}/{table}: {e}")

    def _get_price(self, pair):
        """Fetch current market price."""
        if not self.tools:
            return None
        try:
            return self.tools.get_price(pair)
        except Exception as e:
            logger.warning(f"Failed to fetch price for {pair}: {e}")
            return None

    def run_once(self):
        """Execute pending signals from all Phase 1 agents."""
        logger.info("Starting Phase 1 signal execution...")

        # Sentiment Leech (ENABLED)
        if ALLOCATIONS["sentiment_leech"]["enabled"]:
            signals = self._get_pending_signals("sentiment_leech.db", "signals")
            for sig in signals:
                if self._execute_sentiment_signal(sig):
                    self.trades_executed += 1

        # Regulatory Scanner
        if ALLOCATIONS["regulatory_scanner"]["enabled"]:
            signals = self._get_pending_signals("regulatory_scanner.db", "signals")
            for sig in signals:
                if self._execute_regulatory_signal(sig):
                    self.trades_executed += 1

        # Liquidation Hunter
        if ALLOCATIONS["liquidation_hunter"]["enabled"]:
            sigs = self._get_pending_signals("liquidation_hunter.db", "cascade_predictions")
            for sig in sigs:
                if self._execute_liquidation_signal(sig):
                    self.trades_executed += 1

        # Narrative Tracker
        if ALLOCATIONS["narrative_tracker"]["enabled"]:
            signals = self._get_pending_signals("narrative_tracker.db", "signals")
            for sig in signals:
                if self._execute_narrative_signal(sig):
                    self.trades_executed += 1

        # Futures Mispricing
        if ALLOCATIONS["futures_mispricing"]["enabled"]:
            opps = self._get_pending_signals("futures_mispricing.db", "arbitrage_opportunities")
            for opp in opps:
                if self._execute_futures_signal(opp):
                    self.trades_executed += 1

        logger.info(f"Cycle complete (executed: {self.trades_executed}, capital used: ${self.total_capital_used:.2f})")

    def _execute_sentiment_signal(self, signal):
        """Execute sentiment leech signal with actual trade placement."""
        if not self.tools or not self.rc or not self.exit_mgr:
            logger.warning("Trading system not available, skipping sentiment_leech execution")
            return False

        try:
            pair = signal.get("pair")
            direction = signal.get("direction", "BUY")
            confidence = signal.get("confidence", 0.6)
            signal_id = signal.get("id")

            if not pair:
                logger.warning("Sentiment signal missing pair field")
                return False

            # Get current price
            price = self._get_price(pair)
            if price is None:
                logger.warning(f"Failed to fetch price for {pair}")
                self._update_signal("sentiment_leech.db", "signals", signal_id, "failed")
                return False

            # Get portfolio value
            portfolio = self.tools.get_portfolio()
            portfolio_value = portfolio.get("total_usd", 0)

            # Allocate $10 from portfolio for sentiment_leech
            allocation = ALLOCATIONS["sentiment_leech"]["allocation_usd"]
            max_position_pct = ALLOCATIONS["sentiment_leech"]["max_position_pct"]

            # Position size: min of allocation or max_position_pct of portfolio
            position_size_usd = min(allocation, portfolio_value * max_position_pct)

            if position_size_usd < 0.50:
                logger.warning(f"Position size ${position_size_usd:.2f} too small, skipping")
                return False

            # Request approval from risk controller
            approved, reason_denied, approved_size = self.rc.approve_trade(
                agent_name="sentiment_leech",
                pair=pair,
                direction=direction,
                size_usd=position_size_usd,
                portfolio_value=portfolio_value,
            )

            if not approved:
                logger.info(f"DENIED sentiment_leech {direction} {pair}: {reason_denied}")
                self._update_signal("sentiment_leech.db", "signals", signal_id, "denied")
                return False

            # Use approved size
            position_size_usd = approved_size

            # Place limit order (0.2% slippage for maker fee)
            if direction == "BUY":
                order_price = price * 0.998  # 0.2% below spot
                logger.info(f"→ PLACE sentiment_leech BUY {pair} ${position_size_usd:.2f} @ ${order_price:.2f}")
                order_id = self.tools.place_limit_buy(pair, order_price, position_size_usd)
            else:  # SELL
                order_price = price * 1.002  # 0.2% above spot
                logger.info(f"→ PLACE sentiment_leech SELL {pair} ${position_size_usd:.2f} @ ${order_price:.2f}")
                order_id = self.tools.place_limit_sell(pair, order_price, position_size_usd)

            if not order_id:
                logger.error(f"Failed to place order for {pair}")
                self._update_signal("sentiment_leech.db", "signals", signal_id, "failed")
                return False

            logger.info(f"✓ FILLED sentiment_leech {direction} {pair} order={order_id}")

            # Register with exit manager
            self.exit_mgr.register_position(
                agent_name="sentiment_leech",
                pair=pair,
                direction=direction,
                entry_price=price,
                position_size_usd=position_size_usd,
                order_id=order_id,
                confidence=confidence,
            )

            # Update signal status to filled
            self._update_signal("sentiment_leech.db", "signals", signal_id, "filled", order_id)
            self.total_capital_used += position_size_usd

            return True

        except Exception as e:
            logger.error(f"Error executing sentiment signal: {e}", exc_info=True)
            self._update_signal("sentiment_leech.db", "signals", signal.get("id"), "error")
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

    if cmd == "status":
        print(json.dumps({
            "trades_executed": executor.trades_executed,
            "capital_used_usd": executor.total_capital_used,
            "sentiment_leech_enabled": ALLOCATIONS["sentiment_leech"]["enabled"],
            "sentiment_leech_allocation_usd": ALLOCATIONS["sentiment_leech"]["allocation_usd"],
        }))
        return

    # Default: run daemon loop
    logger.info(f"Starting Phase 1 executor (interval={CONFIG['scan_interval']}s)")
    logger.info(f"Allocations: {json.dumps(ALLOCATIONS, indent=2)}")
    while True:
        try:
            executor.run_once()
        except Exception as e:
            logger.error(f"Error in execution cycle: {e}", exc_info=True)

        time.sleep(CONFIG["scan_interval"])


if __name__ == "__main__":
    main()
