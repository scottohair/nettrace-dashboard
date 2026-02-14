#!/usr/bin/env python3
"""Webhook-based alerting to Slack and Discord for NetTrace autonomous systems.

Supports priority-based alerts (P0/P1/P2/P3) and forwards to multiple channels.

Usage:
  from webhook_notifier import send_webhook_alert, AlertLevel
  send_webhook_alert(AlertLevel.P0, "Critical", "System failure", {"details": "..."})
"""

import json
import logging
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional, Dict
import urllib.request
import urllib.error
import threading

logger = logging.getLogger("webhook_notifier")

BASE = Path(__file__).parent
AUDIT_LOG_PATH = BASE / "webhook_audit_trail.jsonl"


class AlertLevel(Enum):
    """Alert priority levels."""
    P0 = "critical"      # HARDSTOP, deploy rollback, execution health failure
    P1 = "high"          # Parameter optimization rollback, strategy rejection
    P2 = "medium"        # Successful deployment, strategy promotion
    P3 = "info"          # Daily digest, agent lifecycle events


# Webhook URLs from environment variables
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")


def _log_audit_trail(level: AlertLevel, title: str, message: str, context: Dict) -> None:
    """Log alert to audit trail."""
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level.value,
        "title": title,
        "message": message,
        "context": context
    }
    with open(AUDIT_LOG_PATH, "a") as f:
        f.write(json.dumps(record) + "\n")


def _send_slack(title: str, message: str, context: Dict, level: AlertLevel) -> bool:
    """Send alert to Slack."""
    if not SLACK_WEBHOOK_URL:
        return False

    try:
        color_map = {
            AlertLevel.P0: "#ff0000",  # Red
            AlertLevel.P1: "#ff9900",  # Orange
            AlertLevel.P2: "#0099ff",  # Blue
            AlertLevel.P3: "#999999",  # Gray
        }

        payload = {
            "attachments": [
                {
                    "color": color_map.get(level, "#999999"),
                    "title": title,
                    "text": message,
                    "fields": [
                        {
                            "title": k.replace("_", " ").title(),
                            "value": str(v),
                            "short": True
                        }
                        for k, v in context.items()
                    ],
                    "ts": int(datetime.now(timezone.utc).timestamp())
                }
            ]
        }

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"}
        )

        with urllib.request.urlopen(req, timeout=10) as response:
            response.read()
            logger.info(f"Slack alert sent: {title}")
            return True

    except urllib.error.URLError as e:
        logger.error(f"Slack webhook failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Slack webhook error: {e}")
        return False


def _send_discord(title: str, message: str, context: Dict, level: AlertLevel) -> bool:
    """Send alert to Discord."""
    if not DISCORD_WEBHOOK_URL:
        return False

    try:
        color_map = {
            AlertLevel.P0: 0xff0000,  # Red
            AlertLevel.P1: 0xff9900,  # Orange
            AlertLevel.P2: 0x0099ff,  # Blue
            AlertLevel.P3: 0x999999,  # Gray
        }

        # Build field array
        fields = [
            {
                "name": k.replace("_", " ").title(),
                "value": str(v),
                "inline": True
            }
            for k, v in context.items()
        ]

        payload = {
            "embeds": [
                {
                    "title": title,
                    "description": message,
                    "color": color_map.get(level, 0x999999),
                    "fields": fields,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            ]
        }

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            DISCORD_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"}
        )

        with urllib.request.urlopen(req, timeout=10) as response:
            response.read()
            logger.info(f"Discord alert sent: {title}")
            return True

    except urllib.error.URLError as e:
        logger.error(f"Discord webhook failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Discord webhook error: {e}")
        return False


def send_webhook_alert(
    level: AlertLevel,
    title: str,
    message: str,
    context: Optional[Dict] = None,
    async_send: bool = True
) -> None:
    """Send alert to configured webhooks (Slack, Discord).

    Args:
        level: Alert priority level (P0-P3)
        title: Alert title
        message: Alert message
        context: Additional context dict to include in alert
        async_send: Send in background thread to avoid blocking
    """
    context = context or {}

    def _do_send():
        _log_audit_trail(level, title, message, context)

        # Skip sending if no webhooks configured
        if not SLACK_WEBHOOK_URL and not DISCORD_WEBHOOK_URL:
            logger.warning(f"No webhooks configured, logged locally: {title}")
            return

        # Send to Slack
        if SLACK_WEBHOOK_URL:
            _send_slack(title, message, context, level)

        # Send to Discord
        if DISCORD_WEBHOOK_URL:
            _send_discord(title, message, context, level)

    if async_send:
        thread = threading.Thread(target=_do_send, daemon=True)
        thread.start()
    else:
        _do_send()


def get_audit_trail(limit: int = 100) -> list:
    """Get recent audit trail entries."""
    if not AUDIT_LOG_PATH.exists():
        return []

    entries = []
    with open(AUDIT_LOG_PATH) as f:
        for line in f:
            if line.strip():
                entries.append(json.loads(line))

    return entries[-limit:]


if __name__ == "__main__":
    # Test webhook
    logging.basicConfig(level=logging.INFO)

    logger.info("Testing webhook notifications...")

    send_webhook_alert(
        AlertLevel.P2,
        "✅ Test Notification",
        "This is a test alert from webhook_notifier.py",
        {"version": "test", "timestamp": datetime.now(timezone.utc).isoformat()},
        async_send=False
    )

    logger.info("Test complete. Check Slack/Discord for message.")
