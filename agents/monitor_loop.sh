#!/bin/bash
# Monitor loop - runs report every 5 minutes
# Outputs to monitor.log for Claude to read
cd "$(dirname "$0")"

while true; do
    python3 monitor_report.py >> monitor.log 2>&1
    sleep 300
done
