#!/usr/bin/env python3
"""AmiCoin Agent — runs isolated AmiCoin network simulation 24/7.

Safety:
  - Simulation-only
  - Explicitly excluded from real holdings and portfolio accounting
  - No live exchange orders, no on-chain mint/transfer
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import amicoin_system as amc

BASE_DIR = Path(__file__).parent
STATUS_FILE = BASE_DIR / "amicoin_agent_status.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [amicoin_agent] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE_DIR / "amicoin_agent.log")),
    ],
)
logger = logging.getLogger("amicoin_agent")

INTERVAL_SECONDS = int(os.environ.get("AMICOIN_INTERVAL_SECONDS", "180"))  # 3 minutes


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


class AmiCoinAgent:
    def __init__(self):
        self.running = True
        self.cycles = 0

    def _write_status(self, state, snapshot=None, error=None):
        summary = (snapshot or {}).get("summary", {}) if snapshot else {}
        payload = {
            "agent": "amicoin_agent",
            "state": state,
            "updated_at": _utc_now(),
            "interval_seconds": INTERVAL_SECONDS,
            "cycle_count": self.cycles,
            "simulation_only": True,
            "excluded_from_real_holdings": True,
            "counts_toward_portfolio": False,
            "summary": summary,
            "error": str(error) if error else "",
        }
        STATUS_FILE.write_text(json.dumps(payload, indent=2))

    def run_once(self):
        self.cycles += 1
        self._write_status("running")
        snapshot = amc.run_cycle()
        self._write_status("idle", snapshot=snapshot)
        s = snapshot.get("summary", {})
        logger.info(
            "cycle=%d reserve=$%.2f open=%d realized=$%.2f blocked_losses=%d",
            self.cycles,
            float(s.get("reserve_equity_usd", 0.0)),
            int(s.get("open_positions", 0)),
            float(s.get("realized_pnl_usd", 0.0)),
            int(s.get("blocked_losses", 0)),
        )
        return snapshot

    def run_forever(self):
        def _stop(signum, _frame):
            logger.info("Received signal %d, stopping...", signum)
            self.running = False

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

        self._write_status("starting")
        logger.info("AmiCoin agent started (interval=%ds)", INTERVAL_SECONDS)

        while self.running:
            try:
                self.run_once()
            except Exception as e:
                logger.error("cycle failed: %s", e, exc_info=True)
                self._write_status("error", error=e)

            if not self.running:
                break

            self._write_status("sleeping")
            for _ in range(INTERVAL_SECONDS):
                if not self.running:
                    break
                time.sleep(1)

        self._write_status("stopped")
        logger.info("AmiCoin agent stopped")


def print_status():
    if not STATUS_FILE.exists():
        print("No status file yet.")
        return
    print(STATUS_FILE.read_text())


def print_snapshot():
    print(json.dumps(amc.get_snapshot(), indent=2))


if __name__ == "__main__":
    agent = AmiCoinAgent()

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd in ("--once", "once"):
            print(json.dumps(agent.run_once(), indent=2))
        elif cmd in ("--status", "status"):
            print_status()
        elif cmd in ("--snapshot", "snapshot"):
            print_snapshot()
        else:
            print("Usage: amicoin_agent.py [--once|--status|--snapshot]")
    else:
        agent.run_forever()

