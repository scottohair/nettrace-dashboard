#!/usr/bin/env python3
"""Meta-engine message bus bridge — publishes status to advanced_team bus.

Call publish_meta_status() after each evolution cycle to broadcast
the meta-engine's current state to all listening agents.
"""

import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

logger = logging.getLogger("meta_engine_bus")


def publish_meta_status(agents, predictions, ideas, cycle=0):
    """Publish meta_engine status to the advanced_team message bus.

    Args:
        agents: list of agent dicts (from AgentPool.list_agents())
        predictions: list of ML prediction dicts
        ideas: list of alpha idea dicts
        cycle: current evolution cycle number
    """
    try:
        from advanced_team.message_bus import MessageBus
        bus = MessageBus()
    except Exception as e:
        logger.debug("MessageBus import failed, skipping broadcast: %s", e)
        return None

    # Summarize agents by status
    status_counts = {}
    top_agents = []
    for a in agents:
        status = a.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        # Include top 5 active agents by Sharpe
        if status in ("cold", "warm", "hot") and len(top_agents) < 5:
            top_agents.append({
                "name": a.get("name"),
                "status": status,
                "sharpe": a.get("sharpe_ratio", 0),
                "trades": a.get("trades", 0),
                "wins": a.get("wins", 0),
            })

    # Summarize predictions
    pred_summary = []
    for p in predictions[:5]:
        pred_summary.append({
            "pair": p.get("pair"),
            "direction": p.get("direction"),
            "confidence": p.get("confidence", 0),
        })

    # Summarize ideas
    idea_summary = []
    for idea in ideas[:5]:
        idea_summary.append({
            "type": idea.get("type") or idea.get("idea_type"),
            "reason": idea.get("reason") or idea.get("description", ""),
            "confidence": idea.get("confidence", 0),
        })

    payload = {
        "agent_pool": {
            "total": len(agents),
            "by_status": status_counts,
            "top_agents": top_agents,
        },
        "predictions": pred_summary,
        "ideas": idea_summary,
        "cycle": cycle,
    }

    try:
        msg_id = bus.publish(
            sender="meta_engine",
            recipient="broadcast",
            msg_type="meta_engine_evolution",
            payload=payload,
            cycle=cycle,
        )
        logger.info("Published meta_engine_evolution to bus (msg_id=%s, cycle=%d)", msg_id, cycle)
        return msg_id
    except Exception as e:
        logger.debug("Bus publish failed: %s", e)
        return None
