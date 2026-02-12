#!/usr/bin/env python3
"""Scanner Agent — runs additional scans from local hardware for free.

This agent runs traceroutes from the local network, adding a home-network
perspective to our multi-region data. More data points = more valuable product.
"""

import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from orchestrator import log_action, get_db as get_agent_db

TARGETS_PATH = Path(__file__).parent.parent / "targets.json"
FLY_API = "https://nettrace-dashboard.fly.dev"
SCAN_INTERVAL = 900  # 15 minutes
AGENT_NAME = "scanner-alpha"


def load_targets():
    if not TARGETS_PATH.exists():
        return []
    with open(TARGETS_PATH) as f:
        data = json.load(f)
    targets = []
    for cat, entries in data.items():
        for e in entries:
            targets.append({"name": e["name"], "host": e["host"], "category": cat})
    return targets


def run_traceroute(host):
    try:
        result = subprocess.run(
            ["traceroute", "-m", "20", "-q", "1", "-w", "2", host],
            capture_output=True, text=True, timeout=60
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []

    hops = []
    for line in result.stdout.strip().split("\n")[1:]:
        line = line.strip()
        if not line:
            continue
        m = re.match(r'\s*(\d+)\s+(\S+)\s+\((\d+\.\d+\.\d+\.\d+)\)\s+([\d.]+)\s*ms', line)
        if m:
            hops.append({"hop": int(m.group(1)), "ip": m.group(3), "rtt_ms": float(m.group(4))})
        else:
            m2 = re.match(r'\s*(\d+)\s+\*', line)
            if m2:
                hops.append({"hop": int(m2.group(1)), "ip": None, "rtt_ms": None})
    return hops


def scan_cycle(targets):
    """Run one full scan cycle."""
    scanned = 0
    for t in targets:
        hops = run_traceroute(t["host"])
        if not hops:
            continue

        total_rtt = None
        for h in reversed(hops):
            if h.get("rtt_ms"):
                total_rtt = h["rtt_ms"]
                break

        if total_rtt is not None:
            scanned += 1

        # Log every 10th scan to avoid DB bloat
        if scanned % 10 == 0:
            log_action(AGENT_NAME, f"scanned {t['host']}", f"rtt={total_rtt}ms")

        time.sleep(5)  # stagger

    return scanned


def main():
    print(f"[{AGENT_NAME}] Starting local scanner agent")
    targets = load_targets()
    if not targets:
        print(f"[{AGENT_NAME}] No targets found")
        return

    print(f"[{AGENT_NAME}] Loaded {len(targets)} targets, interval={SCAN_INTERVAL}s")

    while True:
        try:
            start = time.time()
            scanned = scan_cycle(targets)
            elapsed = time.time() - start
            log_action(AGENT_NAME, f"cycle_complete: {scanned} targets in {elapsed:.0f}s")
            print(f"[{AGENT_NAME}] Cycle: {scanned} scanned in {elapsed:.0f}s")

            remaining = max(0, SCAN_INTERVAL - elapsed)
            if remaining > 0:
                time.sleep(remaining)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[{AGENT_NAME}] Error: {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()
