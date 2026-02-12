#!/usr/bin/env python3
"""Asset State Tracker — logs state transitions for learning.

Every time an asset changes state (available→in_transit, in_transit→available, etc.),
this module logs the transition with timing and cost data. The dashboard displays
this as the "Learning Feed" and agents use the aggregated insights to make better
routing, timing, and venue decisions.

States:
  - available:  Liquid, tradeable, ready for use
  - in_transit: Being bridged, transferred, or pending confirmation
  - stuck:      Sent to wrong address or otherwise unrecoverable
  - locked:     In LP position, staking, or time-locked
  - pending:    Order placed but not yet filled
  - reserved:   Earmarked for a specific trade/strategy

Usage:
    from asset_tracker import AssetTracker
    tracker = AssetTracker()  # Uses local Fly API by default

    # Record a trade execution
    tracker.transition("ETH", "coinbase", "available", "pending",
                       amount=0.01, value_usd=19.10, trigger="sniper")

    # Record a fill
    tracker.transition("ETH", "coinbase", "pending", "available",
                       amount=0.01, value_usd=19.10, cost_usd=0.08,
                       duration_seconds=2.3, trigger="sniper")

    # Record a bridge
    tracker.transition("ETH", "base", "available", "in_transit",
                       amount=0.032, value_usd=61.00, trigger="bridge",
                       tx_hash="0xabc...")

    # Get learning insights
    insights = tracker.get_insights()
"""

import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("asset_tracker")

# Load .env from agents dir
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


class AssetTracker:
    """Logs asset state transitions to the dashboard API for learning."""

    def __init__(self, api_url=None, api_key=None):
        self.api_url = api_url or os.environ.get(
            "NETTRACE_API_URL", "https://nettrace-dashboard.fly.dev")
        self.api_key = api_key or os.environ.get("NETTRACE_API_KEY", "")
        # In-memory cache of last known states (for duration calculation)
        self._state_cache = {}  # key: (asset, venue) → {"state": ..., "timestamp": ...}
        # Local file fallback for when API is unavailable
        self._local_log = Path(__file__).parent / "asset_transitions.jsonl"

    def transition(self, asset, venue, from_state, to_state,
                   amount=0, value_usd=0, cost_usd=0,
                   duration_seconds=None, trigger="agent",
                   tx_hash=None, metadata=None):
        """Record a state transition.

        Args:
            asset: Token symbol (ETH, BTC, USDC, etc.)
            venue: Where (coinbase, base, ethereum, bridge, etc.)
            from_state: Previous state
            to_state: New state
            amount: Token amount
            value_usd: USD value at time of transition
            cost_usd: Fees/gas/slippage cost
            duration_seconds: Time spent in from_state (auto-calculated if None)
            trigger: What caused it (sniper, grid_trader, bridge, manual, etc.)
            tx_hash: Transaction hash if on-chain
            metadata: Additional JSON-serializable data
        """
        now = time.time()
        cache_key = (asset, venue)

        # Auto-calculate duration from last known state
        if duration_seconds is None and cache_key in self._state_cache:
            prev = self._state_cache[cache_key]
            if prev["state"] == from_state:
                duration_seconds = round(now - prev["timestamp"], 2)

        # Update cache
        self._state_cache[cache_key] = {"state": to_state, "timestamp": now}

        payload = {
            "asset": asset,
            "venue": venue,
            "from_state": from_state,
            "to_state": to_state,
            "amount": amount,
            "value_usd": value_usd,
            "cost_usd": cost_usd,
            "duration_seconds": duration_seconds,
            "trigger": trigger,
            "tx_hash": tx_hash,
            "metadata": metadata or {},
        }

        # Try API first, fallback to local file
        try:
            self._post_to_api(payload)
            logger.info(
                "Transition: %s@%s %s→%s ($%.2f, cost=$%.4f, %s)",
                asset, venue, from_state, to_state, value_usd, cost_usd, trigger)
        except Exception as e:
            logger.warning("API post failed (%s), logging locally", e)
            self._log_local(payload)

    def _post_to_api(self, payload):
        """POST transition to dashboard API."""
        url = f"{self.api_url}/api/asset-pools/transition"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=data,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": self.api_key,
            })
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())

    def _log_local(self, payload):
        """Append transition to local JSONL file (fallback)."""
        payload["logged_at"] = datetime.now(timezone.utc).isoformat()
        with open(self._local_log, "a") as f:
            f.write(json.dumps(payload) + "\n")

    def get_insights(self):
        """Get learning insights from local transition log.

        Returns dict with:
          - avg_bridge_time: Average bridge duration in seconds
          - avg_trade_cost: Average trade cost in USD
          - state_distribution: Count of transitions per state
          - venue_performance: Avg cost per venue
        """
        insights = {
            "avg_bridge_time": None,
            "avg_trade_cost": None,
            "state_distribution": {},
            "venue_performance": {},
        }

        if not self._local_log.exists():
            return insights

        bridge_times = []
        trade_costs = []
        state_counts = {}
        venue_costs = {}

        for line in self._local_log.read_text().splitlines():
            try:
                t = json.loads(line)
            except Exception:
                continue

            # Count state transitions
            to_state = t.get("to_state", "unknown")
            state_counts[to_state] = state_counts.get(to_state, 0) + 1

            # Bridge timing
            if t.get("from_state") == "in_transit" and to_state == "available":
                dur = t.get("duration_seconds")
                if dur and dur > 0:
                    bridge_times.append(dur)

            # Trade costs
            cost = t.get("cost_usd", 0)
            if cost > 0:
                trade_costs.append(cost)
                venue = t.get("venue", "unknown")
                if venue not in venue_costs:
                    venue_costs[venue] = []
                venue_costs[venue].append(cost)

        insights["state_distribution"] = state_counts
        if bridge_times:
            insights["avg_bridge_time"] = round(sum(bridge_times) / len(bridge_times), 1)
        if trade_costs:
            insights["avg_trade_cost"] = round(sum(trade_costs) / len(trade_costs), 4)
        insights["venue_performance"] = {
            v: round(sum(c) / len(c), 4) for v, c in venue_costs.items()
        }

        return insights

    def sync_pending_bridges(self, bridges_file=None):
        """Read pending_bridges.json and ensure transitions are recorded."""
        if bridges_file is None:
            bridges_file = Path(__file__).parent / "pending_bridges.json"
        if not bridges_file.exists():
            return

        try:
            bridges = json.loads(bridges_file.read_text())
            for b in bridges:
                cache_key = ("ETH", b.get("chain", "bridge"))
                if cache_key not in self._state_cache:
                    state = "stuck" if "stuck" in b.get("status", "") else "in_transit"
                    self._state_cache[cache_key] = {
                        "state": state,
                        "timestamp": b.get("timestamp", time.time()),
                    }
        except Exception as e:
            logger.warning("Failed to sync pending bridges: %s", e)


# Singleton for easy import
_default_tracker = None


def get_tracker():
    """Get or create the default AssetTracker singleton."""
    global _default_tracker
    if _default_tracker is None:
        _default_tracker = AssetTracker()
    return _default_tracker
