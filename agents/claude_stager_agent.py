#!/usr/bin/env python3
"""Claude Stager Agent — continuously prepares Claude ingest bundle."""

import argparse
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import claude_staging as staging
try:
    import claude_duplex as duplex
except Exception:
    duplex = None

BASE_DIR = Path(__file__).parent
STATUS_FILE = BASE_DIR / "claude_stager_status.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [claude_stager] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE_DIR / "claude_stager.log")),
    ],
)
logger = logging.getLogger("claude_stager")

INTERVAL_SECONDS = int(os.environ.get("CLAUDE_STAGING_INTERVAL_SECONDS", "180"))


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


class ClaudeStagerAgent:
    def __init__(self):
        self.running = True
        self.cycles = 0

    def _write_status(self, state, bundle=None, error=None):
        summary = (bundle or {}).get("summary", {}) if bundle else {}
        payload = {
            "agent": "claude_stager",
            "state": state,
            "updated_at": _utc_now(),
            "interval_seconds": INTERVAL_SECONDS,
            "cycle_count": self.cycles,
            "summary": summary,
            "bundle_path": str(staging.BUNDLE_FILE),
            "error": str(error) if error else "",
        }
        STATUS_FILE.write_text(json.dumps(payload, indent=2))

    def run_once(self, reason="scheduled"):
        self.cycles += 1
        self._write_status("running")
        bundle = staging.build_ingest_bundle(reason=reason)
        self._write_status("idle", bundle=bundle)
        s = bundle.get("summary", {})
        logger.info(
            "cycle=%d focus_pairs=%d online_nodes=%d frameworks=%d msgs=%d/%d",
            self.cycles,
            len(s.get("focus_pairs", [])),
            int(s.get("online_nodes", 0)),
            len(s.get("frameworks", {})),
            int(s.get("advanced_msgs", 0)),
            int(s.get("operator_msgs", 0)),
        )
        return bundle

    def run_forever(self):
        def _stop(signum, _frame):
            logger.info("Received signal %d, stopping...", signum)
            self.running = False

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

        self._write_status("starting")
        logger.info("Claude stager started (interval=%ds)", INTERVAL_SECONDS)

        while self.running:
            try:
                self.run_once(reason="scheduled")
            except Exception as e:
                logger.error("staging failed: %s", e, exc_info=True)
                self._write_status("error", error=e)

            if not self.running:
                break

            self._write_status("sleeping")
            for _ in range(INTERVAL_SECONDS):
                if not self.running:
                    break
                time.sleep(1)

        self._write_status("stopped")
        logger.info("Claude stager stopped")


def print_status():
    if not STATUS_FILE.exists():
        print("No status file yet.")
        return
    print(STATUS_FILE.read_text())


def main():
    parser = argparse.ArgumentParser(description="Claude staging agent")
    parser.add_argument("--once", action="store_true", help="run one staging cycle")
    parser.add_argument("--status", action="store_true", help="print status file")
    parser.add_argument("--message", type=str, default="", help="stage a new operator message")
    parser.add_argument("--set-priority-pairs", type=str, default="", help="comma-separated priority pairs, e.g. BTC-USD,ETH-USD")
    parser.add_argument("--priority", type=str, default="normal", help="message priority")
    parser.add_argument("--category", type=str, default="operator", help="message category")
    parser.add_argument("--sender", type=str, default="user", help="message sender")
    args = parser.parse_args()

    if args.status:
        print_status()
        return

    if args.set_priority_pairs:
        pairs = [p.strip().upper() for p in args.set_priority_pairs.split(",") if p.strip()]
        if not pairs:
            print("No valid pairs supplied.")
            return
        cfg = staging.get_priority_config()
        cfg["priority_pairs"] = pairs
        cfg["updated_at"] = _utc_now()
        staging.PRIORITY_CONFIG_FILE.write_text(json.dumps(cfg, indent=2))
        if duplex is not None:
            duplex.send_to_claude(
                "Priority pairs updated",
                msg_type="priority_update",
                priority="high",
                source=args.sender,
                meta={"priority_pairs": pairs},
            )
        bundle = staging.build_ingest_bundle(reason="priority_pairs_update")
        print(json.dumps({"updated_priority_pairs": pairs, "summary": bundle.get("summary", {})}, indent=2))
        return

    if args.message:
        item = staging.stage_operator_message(
            args.message,
            category=args.category,
            priority=args.priority,
            sender=args.sender,
        )
        if duplex is not None:
            duplex.send_to_claude(
                args.message,
                msg_type="directive",
                priority=args.priority,
                source=args.sender,
            )
        print(json.dumps(item, indent=2))
        bundle = staging.build_ingest_bundle(reason="operator_message")
        print(json.dumps(bundle.get("summary", {}), indent=2))
        return

    agent = ClaudeStagerAgent()
    if args.once:
        print(json.dumps(agent.run_once(reason="manual"), indent=2))
    else:
        agent.run_forever()


if __name__ == "__main__":
    main()
