#!/usr/bin/env python3
"""Always-on candle aggregation agent.

Maintains normalized multi-market candle feed for downstream consumers.
Publishes status to agents/candle_aggregator_status.json.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent
sys.path.insert(0, str(ROOT))

from candle_graphql_api import CandleAggregator  # noqa: E402

LOG_FILE = BASE / "candle_aggregator_agent.log"
STATUS_FILE = BASE / "candle_aggregator_status.json"

INTERVAL_SECONDS = max(5, int(os.environ.get("CANDLE_AGG_AGENT_INTERVAL_SECONDS", "20")))
TARGET_POINTS = max(100, int(os.environ.get("CANDLE_GRAPHQL_TARGET_POINTS", "1000")))
CANDLES_PER_PAIR = max(10, int(os.environ.get("CANDLE_GRAPHQL_CANDLES_PER_PAIR", "40")))
SINCE_HOURS = max(1, int(os.environ.get("CANDLE_GRAPHQL_SINCE_HOURS", "48")))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [candle_agg_agent] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(str(LOG_FILE))],
)
logger = logging.getLogger("candle_agg_agent")


def _write_status(payload):
    STATUS_FILE.write_text(json.dumps(payload, indent=2))


def _run_once(aggregator):
    started = time.time()
    ok = True
    error = ""
    summary = {}
    try:
        summary = aggregator.refresh(
            target_points=TARGET_POINTS,
            candles_per_pair=CANDLES_PER_PAIR,
            since_hours=SINCE_HOURS,
        )
    except Exception as e:
        ok = False
        error = str(e)

    status = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "ok": bool(ok),
        "error": error,
        "runtime_ms": round((time.time() - started) * 1000.0, 3),
        "target_points": int(TARGET_POINTS),
        "since_hours": int(SINCE_HOURS),
        "interval_seconds": int(INTERVAL_SECONDS),
        "summary": summary if isinstance(summary, dict) else {},
    }
    _write_status(status)
    if ok:
        logger.info(
            "refresh ok points=%s target=%s achieved=%s pairs=%s tf=%s",
            status["summary"].get("total_points"),
            status["summary"].get("target_points"),
            status["summary"].get("target_achieved"),
            status["summary"].get("pair_count"),
            status["summary"].get("timeframe_count"),
        )
    else:
        logger.warning("refresh failed: %s", error)
    return status


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    aggregator = CandleAggregator()

    if cmd == "status":
        if STATUS_FILE.exists():
            print(STATUS_FILE.read_text())
        else:
            print(json.dumps({"ok": False, "reason": "status_missing"}))
        return

    if cmd == "once":
        payload = _run_once(aggregator)
        print(json.dumps(payload, indent=2))
        return

    # default: run daemon loop
    logger.info(
        "starting candle aggregator loop interval=%ss target_points=%s since_hours=%s",
        INTERVAL_SECONDS,
        TARGET_POINTS,
        SINCE_HOURS,
    )
    while True:
        _run_once(aggregator)
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

