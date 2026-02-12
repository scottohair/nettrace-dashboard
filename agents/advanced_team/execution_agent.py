#!/usr/bin/env python3
"""ExecutionAgent — executes approved trades and writes to shared queue.

Takes approved risk verdicts and:
  1. Writes trade recommendations to trade_queue.json (for sniper/orchestrator)
  2. Optionally executes directly via CoinbaseTrader (if enabled)
  3. Reports execution results back to the bus for LearningAgent

Game Theory:
  - Maker orders preferred (limit orders at 0.4% fee vs 0.6% market)
  - Best venue routing based on cross-exchange spread data
  - Optimal entry: place limit orders at support levels from orderbook

Input:  risk_verdict from RiskAgent
Output: execution_result -> LearningAgent
        trade_queue.json -> sniper.py / orchestrator
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_AGENTS_DIR = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _AGENTS_DIR)

_env_path = Path(_AGENTS_DIR) / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

from advanced_team.message_bus import MessageBus

logger = logging.getLogger("execution_agent")

TRADE_QUEUE_FILE = str(Path(__file__).parent / "trade_queue.json")

# Execution mode: "queue" = write to file only, "live" = also execute on Coinbase
EXECUTION_MODE = os.environ.get("ADVANCED_TEAM_EXEC_MODE", "queue")


class ExecutionAgent:
    """Executes approved trades and writes to shared queue."""

    NAME = "execution"

    def __init__(self):
        self.bus = MessageBus()
        self.state = {
            "executed_count": 0,
            "queued_count": 0,
            "failed_count": 0,
            "cycle_count": 0,
            "total_volume_usd": 0.0,
        }
        self._trader = None

    def _get_trader(self):
        """Lazy-load CoinbaseTrader."""
        if self._trader is None:
            try:
                from exchange_connector import CoinbaseTrader
                self._trader = CoinbaseTrader()
            except Exception as e:
                logger.warning("Could not initialize CoinbaseTrader: %s", e)
        return self._trader

    def _write_to_queue(self, trade_rec):
        """Append trade recommendation to trade_queue.json."""
        try:
            # Read existing queue
            if os.path.exists(TRADE_QUEUE_FILE):
                with open(TRADE_QUEUE_FILE, "r") as f:
                    queue = json.load(f)
            else:
                queue = {"trades": [], "last_updated": ""}

            # Append new trade
            queue["trades"].append(trade_rec)
            queue["last_updated"] = datetime.now(timezone.utc).isoformat()

            # Keep only last 100 trades in queue
            if len(queue["trades"]) > 100:
                queue["trades"] = queue["trades"][-100:]

            with open(TRADE_QUEUE_FILE, "w") as f:
                json.dump(queue, f, indent=2)

            self.state["queued_count"] += 1
            return True
        except Exception as e:
            logger.error("Failed to write to trade queue: %s", e)
            return False

    def _execute_live(self, proposal):
        """Execute trade live on Coinbase."""
        trader = self._get_trader()
        if not trader:
            return {"status": "error", "reason": "No trader available"}

        pair = proposal.get("pair", "")
        direction = proposal.get("direction", "")
        size_usd = proposal.get("approved_size_usd", 0)
        order_type = proposal.get("order_type", "market")
        entry_price = proposal.get("entry_price", 0)

        if direction != "BUY":
            return {"status": "skipped", "reason": "Only BUY orders in accumulation mode"}

        try:
            if order_type == "limit" and entry_price > 0:
                # Limit order: place 0.1% below current price for better fill
                limit_price = round(entry_price * 0.999, 2)
                # Calculate base size from USD amount
                base_size = size_usd / entry_price if entry_price > 0 else 0
                result = trader.place_limit_order(
                    pair, "BUY", base_size, limit_price, post_only=True
                )
            else:
                # Market order
                result = trader.place_order(pair, "BUY", round(size_usd, 2))

            if "success_response" in result:
                order_id = result["success_response"].get("order_id", "")
                self.state["executed_count"] += 1
                self.state["total_volume_usd"] += size_usd
                return {
                    "status": "filled",
                    "order_id": order_id,
                    "pair": pair,
                    "size_usd": size_usd,
                    "order_type": order_type,
                }
            elif "error_response" in result:
                err = result["error_response"]
                self.state["failed_count"] += 1
                return {
                    "status": "failed",
                    "error": err.get("message", str(err)),
                    "pair": pair,
                }
            else:
                # Check for order_id at top level (alternate response format)
                if "order_id" in result:
                    self.state["executed_count"] += 1
                    self.state["total_volume_usd"] += size_usd
                    return {
                        "status": "filled",
                        "order_id": result["order_id"],
                        "pair": pair,
                        "size_usd": size_usd,
                    }
                return {"status": "unknown", "response": str(result)[:200]}

        except Exception as e:
            self.state["failed_count"] += 1
            logger.error("Live execution failed: %s", e)
            return {"status": "error", "reason": str(e)}

    def run(self, cycle):
        """Process all risk verdicts for this cycle."""
        logger.info("ExecutionAgent cycle %d starting (mode=%s)", cycle, EXECUTION_MODE)
        self.state["cycle_count"] = cycle

        # Read risk verdicts
        verdicts = self.bus.read_latest("execution", msg_type="risk_verdict", count=10)
        if not verdicts:
            logger.info("No verdicts to process")
            return []

        results = []

        for msg in verdicts:
            if msg.get("cycle", -1) != cycle:
                continue

            verdict_data = msg.get("payload", {})
            verdict = verdict_data.get("verdict", "")
            proposal = verdict_data.get("proposal", {})
            reason = verdict_data.get("reason", "")

            if verdict == "HOLD":
                results.append({
                    "action": "HOLD",
                    "pair": proposal.get("pair", "NONE"),
                    "cycle": cycle,
                })
                continue

            if verdict != "APPROVED":
                results.append({
                    "action": "SKIPPED",
                    "pair": proposal.get("pair", "?"),
                    "reason": reason,
                    "cycle": cycle,
                })
                continue

            # --- Execute the approved trade ---
            pair = proposal.get("pair", "")
            size_usd = proposal.get("approved_size_usd", 0)
            confidence = proposal.get("confidence", 0)

            # Build trade record
            trade_rec = {
                "id": f"at_{cycle}_{pair}_{int(time.time())}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "advanced_team",
                "pair": pair,
                "direction": proposal.get("direction", "BUY"),
                "size_usd": size_usd,
                "confidence": confidence,
                "strategy_type": proposal.get("strategy_type", "multi_factor"),
                "order_type": proposal.get("order_type", "market"),
                "entry_price": proposal.get("entry_price", 0),
                "stop_loss_pct": proposal.get("stop_loss_pct", 2.0),
                "take_profit_pct": proposal.get("take_profit_pct", 3.0),
                "reasons": proposal.get("reasons", []),
                "risk_adjustments": proposal.get("risk_adjustments", []),
                "status": "pending",
            }

            # Write to queue (always)
            queue_ok = self._write_to_queue(trade_rec)

            # Execute live if enabled
            exec_result = None
            if EXECUTION_MODE == "live":
                exec_result = self._execute_live(proposal)
                trade_rec["execution"] = exec_result
                trade_rec["status"] = exec_result.get("status", "unknown")
            else:
                trade_rec["status"] = "queued"

            # Build result for LearningAgent
            result = {
                "action": "EXECUTED" if EXECUTION_MODE == "live" else "QUEUED",
                "trade": trade_rec,
                "queue_written": queue_ok,
                "execution": exec_result,
                "cycle": cycle,
            }
            results.append(result)

            logger.info("%s: %s %s $%.2f conf=%.2f%s",
                         "EXECUTED" if exec_result else "QUEUED",
                         proposal.get("direction", "?"),
                         pair, size_usd, confidence,
                         f" order_id={exec_result.get('order_id', '')}" if exec_result and exec_result.get("order_id") else "")

        # Publish all execution results to LearningAgent
        if results:
            self.bus.publish(
                sender=self.NAME,
                recipient="learning",
                msg_type="execution_results",
                payload={
                    "cycle": cycle,
                    "results": results,
                    "stats": {
                        "executed": self.state["executed_count"],
                        "queued": self.state["queued_count"],
                        "failed": self.state["failed_count"],
                        "total_volume": self.state["total_volume_usd"],
                    },
                },
                cycle=cycle,
            )

        return results
