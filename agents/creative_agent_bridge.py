#!/usr/bin/env python3
"""Creative Agent Bridge — Standardized API for agents to submit real-time signals.

Allows creative agents (autonomous_research, multi_hop_arb, ml_signal) to:
  1. Submit signals to the realtime_orchestrator priority queue
  2. Receive feedback on signal acceptance and P&L attribution
  3. Coordinate timing (urgency="critical" for <100ms, etc.)

Signal Schema:
  - pair: str (e.g., "BTC-USD")
  - direction: str ("BUY", "SELL", "HOLD")
  - confidence: float (0.0-1.0)
  - urgency: str ("critical"=<100ms, "high"=<1s, "medium"=<10s, "low"=<60s)
  - reasoning: str (brief explanation)
  - region_hint: str (optional, e.g., "nrt" for Asia arbitrage)
  - expected_hold_ms: int (position duration in milliseconds)
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, List
from collections import defaultdict

logger = logging.getLogger("creative_agent_bridge")


class CreativeAgentBridge:
    """Unified API for creative agents to submit signals to orchestrator."""

    # Shared signal queue (thread-safe)
    _signal_queue = []
    _queue_lock = threading.Lock()

    # Performance tracking by agent
    _agent_feedback = defaultdict(lambda: {
        "submitted": 0,
        "accepted": 0,
        "rejected": 0,
        "total_pnl": 0.0,
        "avg_latency_ms": 0.0,
    })
    _feedback_lock = threading.Lock()

    # Class variable for orchestrator instance
    _orchestrator = None

    @classmethod
    def set_orchestrator(cls, orchestrator):
        """Register orchestrator instance for signal routing."""
        cls._orchestrator = orchestrator

    def __init__(self, agent_name: str):
        """Initialize bridge for a specific agent."""
        self.agent_name = agent_name
        self.start_time = time.time()
        logger.info(f"CreativeAgentBridge initialized for {agent_name}")

    def submit_signal(
        self,
        pair: str,
        direction: str,
        confidence: float,
        urgency: str = "medium",
        reasoning: str = "",
        region_hint: Optional[str] = None,
        expected_hold_ms: int = 60000,
        market_type: str = "crypto",
    ) -> bool:
        """Submit signal to orchestrator priority queue.

        Args:
            pair: Trading pair (e.g., "BTC-USD")
            direction: "BUY", "SELL", or "HOLD"
            confidence: 0.0-1.0 confidence score
            urgency: "critical", "high", "medium", or "low"
            reasoning: Brief explanation of the signal
            region_hint: Preferred region (e.g., "nrt" for Asia)
            expected_hold_ms: Expected position hold time
            market_type: "crypto" (default) or "equity"

        Returns:
            True if submitted, False if rejected
        """
        # Validate inputs
        if confidence < 0.0 or confidence > 1.0:
            logger.warning(f"{self.agent_name}: Invalid confidence {confidence}")
            return False

        if direction not in ("BUY", "SELL", "HOLD"):
            logger.warning(f"{self.agent_name}: Invalid direction {direction}")
            return False

        if urgency not in ("critical", "high", "medium", "low"):
            logger.warning(f"{self.agent_name}: Invalid urgency {urgency}")
            return False

        # Gate: Only submit if confidence >= 70% or urgency is critical
        if confidence < 0.70 and urgency != "critical":
            logger.debug(f"{self.agent_name}: Confidence {confidence:.2f} below threshold, rejecting")
            with self._feedback_lock:
                self._agent_feedback[self.agent_name]["rejected"] += 1
            return False

        signal = {
            "pair": pair,
            "direction": direction,
            "confidence": confidence,
            "urgency": urgency,
            "reasoning": reasoning,
            "region_hint": region_hint,
            "expected_hold_ms": expected_hold_ms,
            "market_type": market_type,
            "timestamp_ms": time.time() * 1000,
        }

        # Submit to orchestrator if available
        if self._orchestrator:
            self._orchestrator.signal_collector.submit_signal(self.agent_name, signal)
        else:
            # Fallback: queue locally
            with self._queue_lock:
                self._signal_queue.append((self.agent_name, signal))

        # Track submission
        with self._feedback_lock:
            self._agent_feedback[self.agent_name]["submitted"] += 1
            self._agent_feedback[self.agent_name]["accepted"] += 1

        logger.info(
            f"{self.agent_name} → {pair} {direction} "
            f"(confidence={confidence:.2f}, urgency={urgency})"
        )

        return True

    def get_feedback(self, limit: int = 10) -> Dict:
        """Get real-time feedback on signal performance.

        Returns dict with:
            - total_submitted: int
            - total_accepted: int
            - acceptance_rate: float (0.0-1.0)
            - total_pnl: float
            - avg_latency_ms: float
        """
        if not self._orchestrator:
            return {}

        with self._feedback_lock:
            feedback = self._agent_feedback[self.agent_name]

        # Get performance attribution from orchestrator
        perf = self._orchestrator.performance_tracker.agent_scoreboard(self.agent_name, limit)

        return {
            "agent": self.agent_name,
            "total_submitted": feedback["submitted"],
            "total_accepted": feedback["accepted"],
            "total_rejected": feedback["rejected"],
            "acceptance_rate": feedback["accepted"] / max(feedback["submitted"], 1),
            "total_pnl": perf.get("total_pnl", 0.0),
            "trade_count": perf.get("trade_count", 0),
            "avg_latency_ms": perf.get("avg_latency_ms", 0.0),
            "gains_per_ms": perf.get("gains_per_ms", 0.0),
        }

    @classmethod
    def broadcast_signal(
        cls,
        agent_name: str,
        pair: str,
        direction: str,
        confidence: float,
        urgency: str = "medium",
        reasoning: str = "",
        region_hint: Optional[str] = None,
        expected_hold_ms: int = 60000,
        market_type: str = "crypto",
    ) -> bool:
        """Broadcast signal without creating bridge instance (fire-and-forget)."""
        bridge = cls(agent_name)
        return bridge.submit_signal(
            pair=pair,
            direction=direction,
            confidence=confidence,
            urgency=urgency,
            reasoning=reasoning,
            region_hint=region_hint,
            expected_hold_ms=expected_hold_ms,
            market_type=market_type,
        )

    @classmethod
    def get_all_feedback(cls) -> Dict[str, Dict]:
        """Get feedback for all agents."""
        if not cls._orchestrator:
            return {}

        result = {}
        rankings = cls._orchestrator.performance_tracker.agent_rankings(limit=100)

        for agent_name, metrics in rankings:
            with cls._feedback_lock:
                feedback = cls._agent_feedback.get(agent_name, {})
            result[agent_name] = {
                **feedback,
                **metrics,
            }

        return result

    @classmethod
    def get_queue(cls) -> List[tuple]:
        """Get pending signals in queue."""
        with cls._queue_lock:
            return list(cls._signal_queue)

    @classmethod
    def clear_queue(cls):
        """Clear pending signals (dangerous, use cautiously)."""
        with cls._queue_lock:
            cls._signal_queue.clear()


# Convenience functions for agents
def submit_signal(
    agent_name: str,
    pair: str,
    direction: str,
    confidence: float,
    urgency: str = "medium",
    reasoning: str = "",
    region_hint: Optional[str] = None,
    expected_hold_ms: int = 60000,
    market_type: str = "crypto",
) -> bool:
    """Global function to submit a signal."""
    return CreativeAgentBridge.broadcast_signal(
        agent_name=agent_name,
        pair=pair,
        direction=direction,
        confidence=confidence,
        urgency=urgency,
        reasoning=reasoning,
        region_hint=region_hint,
        expected_hold_ms=expected_hold_ms,
        market_type=market_type,
    )


def get_agent_feedback(agent_name: str) -> Dict:
    """Global function to get agent feedback."""
    bridge = CreativeAgentBridge(agent_name)
    return bridge.get_feedback()


if __name__ == "__main__":
    # Test mode
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [BRIDGE] %(name)s %(levelname)s %(message)s",
    )

    # Create test bridge
    bridge = CreativeAgentBridge("test_agent")

    # Submit test signals
    print("Testing signal submission...")
    bridge.submit_signal("BTC-USD", "BUY", 0.85, "high", "Test signal")
    bridge.submit_signal("ETH-USD", "SELL", 0.75, "medium", "Test signal")

    print(f"Queue: {CreativeAgentBridge.get_queue()}")
    print(f"Feedback: {bridge.get_feedback()}")
