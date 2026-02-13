#!/usr/bin/env python3
"""DashboardOptimizerAgent — prepares Claude-readable dashboard directives.

Inputs:
  - algorithm_tuning
  - quant_optimization
  - learning_insights

Outputs:
  - dashboard_optimization (broadcast)
  - dashboard_insights.json (artifact consumed by /api/trading-data)
"""

import json
import logging
import os
import sys
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

logger = logging.getLogger("dashboard_optimizer_agent")

INSIGHTS_FILE = str(Path(__file__).parent / "dashboard_insights.json")


class DashboardOptimizerAgent:
    """Synthesizes agent outputs into dashboard-ready recommendations."""

    NAME = "dashboard_optimizer"

    def __init__(self):
        self.bus = MessageBus()
        self.state = {
            "cycle_count": 0,
            "last_msg_id": 0,
        }

    def _read_latest_payload(self, recipient, msg_type, count=5):
        msgs = self.bus.read_latest(recipient, msg_type=msg_type, count=count)
        if not msgs:
            return {}
        return msgs[-1].get("payload", {})

    def run(self, cycle):
        logger.info("DashboardOptimizerAgent cycle %d starting", cycle)
        self.state["cycle_count"] = cycle

        algo = self._read_latest_payload("dashboard_optimizer", "algorithm_tuning", count=3)
        quant = self._read_latest_payload("dashboard_optimizer", "quant_optimization", count=3)
        learning = self._read_latest_payload("research", "learning_insights", count=3)

        risk_overrides = quant.get("risk_overrides", {})
        ranking = quant.get("ranking", [])
        focus_pairs = [row.get("pair") for row in ranking[:3] if row.get("pair")]

        agent_status = [
            {
                "agent": "algorithm_optimizer",
                "label": "Algorithm Optimizer",
                "state": "active",
                "detail": f"min_conf={algo.get('min_confidence', 0.55):.2f}",
            },
            {
                "agent": "quant_optimizer",
                "label": "Quant Optimizer",
                "state": "active",
                "detail": f"posture={risk_overrides.get('risk_posture', 'BALANCED')}",
            },
            {
                "agent": "dashboard_optimizer",
                "label": "Dashboard Optimizer",
                "state": "active",
                "detail": f"focus={', '.join(focus_pairs) if focus_pairs else 'none'}",
            },
        ]

        dashboard_recs = [
            f"Risk posture: {risk_overrides.get('risk_posture', 'BALANCED')}",
            f"Confidence floor: {risk_overrides.get('min_confidence', 0.70):.2f}",
            f"Max trade: ${risk_overrides.get('max_trade_usd', 5.0):.2f}",
        ]
        if risk_overrides.get("blocked_pairs"):
            dashboard_recs.append(
                "Blocked pairs: " + ", ".join(risk_overrides["blocked_pairs"][:3])
            )

        next_actions = []
        next_actions.extend((algo.get("recommendations") or [])[:2])
        next_actions.extend((quant.get("recommendations") or [])[:2])
        if not next_actions:
            next_actions.append("No optimization deltas yet; collecting more cycles.")

        payload = {
            "cycle": cycle,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "risk_posture": risk_overrides.get("risk_posture", "BALANCED"),
            "focus_pairs": focus_pairs,
            "agent_status": agent_status,
            "algorithm_recommendations": (algo.get("recommendations") or [])[:6],
            "quant_recommendations": (quant.get("recommendations") or [])[:6],
            "dashboard_recommendations": dashboard_recs,
            "next_actions": next_actions[:6],
            "learning_snapshot": {
                "sharpe_ratio": learning.get("sharpe_ratio", 0),
                "max_drawdown_pct": learning.get("max_drawdown_pct", 0),
                "total_trades": learning.get("total_trades", 0),
            },
            "source": self.NAME,
        }

        try:
            with open(INSIGHTS_FILE, "w") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.warning("Could not persist %s: %s", INSIGHTS_FILE, e)

        msg_id = self.bus.publish(
            sender=self.NAME,
            recipient="broadcast",
            msg_type="dashboard_optimization",
            payload=payload,
            cycle=cycle,
        )
        self.state["last_msg_id"] = msg_id

        logger.info(
            "Dashboard optimization published (msg_id=%d): posture=%s focus_pairs=%s",
            msg_id, payload["risk_posture"], ",".join(payload["focus_pairs"]) or "none",
        )

        return payload
