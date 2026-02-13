#!/usr/bin/env python3
"""Run and persist execution-health checks."""

import argparse
import json

from execution_health import evaluate_execution_health


def main():
    parser = argparse.ArgumentParser(description="Probe venue execution health (DNS/API/reconciliation).")
    parser.add_argument("--refresh", action="store_true", help="force fresh probes instead of cache")
    parser.add_argument(
        "--no-http-probe",
        action="store_true",
        help="skip outbound HTTP API probes and rely on DNS/telemetry/reconcile status",
    )
    args = parser.parse_args()

    payload = evaluate_execution_health(
        refresh=bool(args.refresh),
        probe_http=(False if args.no_http_probe else None),
        write_status=True,
    )
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
