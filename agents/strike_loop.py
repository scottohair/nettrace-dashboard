#!/usr/bin/env python3
"""Long-running strike team loop runner.

Keeps all strike teams deployed in daemon threads and writes a compact
status heartbeat so runtime monitors can confirm activity.
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from strike_teams import StrikeTeamManager


BASE_DIR = Path(__file__).resolve().parent
STATUS_PATH = BASE_DIR / "strike_loop_status.json"
HEARTBEAT_SECONDS = max(5, int(os.environ.get("STRIKE_LOOP_HEARTBEAT_SECONDS", "15") or 15))


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _load_env():
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))


def main():
    _load_env()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [strike_loop] %(levelname)s %(message)s",
    )
    logger = logging.getLogger("strike_loop")
    manager = StrikeTeamManager()

    stopping = {"flag": False}

    def _stop(_signum=None, _frame=None):
        stopping["flag"] = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    manager.deploy_all()
    logger.info("strike loop started with %d teams", len(manager.teams))

    try:
        while not stopping["flag"]:
            status = manager.status()
            payload = {
                "updated_at": _utc_now(),
                "running": True,
                "team_count": len(status),
                "teams": status,
            }
            try:
                STATUS_PATH.write_text(json.dumps(payload, indent=2))
            except Exception:
                pass
            time.sleep(HEARTBEAT_SECONDS)
    finally:
        manager.stop_all()
        payload = {
            "updated_at": _utc_now(),
            "running": False,
            "team_count": 0,
            "teams": {},
        }
        try:
            STATUS_PATH.write_text(json.dumps(payload, indent=2))
        except Exception:
            pass
        logger.info("strike loop stopped")


if __name__ == "__main__":
    main()
