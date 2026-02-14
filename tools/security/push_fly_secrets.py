#!/usr/bin/env python3
"""Push selected security env keys from a local .env file to Fly secrets.

Default mode prints planned keys only. Use --apply to run flyctl.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
from pathlib import Path


DEFAULT_KEYS = [
    "APP_ENV",
    "SECRET_KEY",
    "CREDENTIAL_ENCRYPTION_KEY",
    "INTERNAL_SIGNAL_SECRET",
    "MCP_AGENT_SECRET",
    "NETTRACE_API_KEY",
    "ALLOW_QUERY_API_KEY_AUTH",
    "MCP_ENABLE_REMOTE_EXEC",
    "MCP_ENABLE_FLY_MUTATIONS",
    "ALLOW_RAW_WALLET_PRIVATE_KEY",
    "ALLOW_LEGACY_XOR_CREDENTIAL_DECRYPT",
    "SESSION_COOKIE_SECURE",
    "ENABLE_HSTS",
    "PASSWORD_MIN_LENGTH",
    "MFA_CHALLENGE_TTL_SECONDS",
    "MFA_MAX_VERIFY_ATTEMPTS",
    "MFA_TOTP_WINDOW_STEPS",
    "REQUIRE_MFA_FOR_SENSITIVE",
    "REQUIRE_MFA_FOR_USERNAMES",
    "TRUSTED_ORIGINS",
]


def parse_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Push security secrets to Fly app")
    parser.add_argument("--env-file", default="agents/.env", help="Source env file")
    parser.add_argument("--app", default=os.environ.get("FLY_APP", "nettrace-dashboard"), help="Fly app name")
    parser.add_argument("--keys", default=",".join(DEFAULT_KEYS), help="Comma-separated keys to sync")
    parser.add_argument("--flyctl", default="/Users/scott/.fly/bin/flyctl", help="flyctl path")
    parser.add_argument("--apply", action="store_true", help="Run flyctl secrets set")
    args = parser.parse_args()

    env = parse_env(Path(args.env_file))
    keys = [k.strip() for k in str(args.keys).split(",") if k.strip()]
    present = [(k, env[k]) for k in keys if k in env and env[k] != ""]
    missing = [k for k in keys if k not in env or env[k] == ""]

    if not present:
        raise SystemExit("no_values_to_sync")

    print(f"app={args.app} env_file={args.env_file} keys_present={len(present)} keys_missing={len(missing)}")
    if missing:
        print("missing_keys=" + ",".join(missing))

    if not args.apply:
        print("dry_run=1 keys=" + ",".join(k for k, _ in present))
        return

    cmd = [args.flyctl, "secrets", "set"]
    for key, value in present:
        cmd.append(f"{key}={value}")
    cmd.extend(["-a", args.app])

    # Print non-secret summary only.
    print("apply=1 keys=" + ",".join(k for k, _ in present))
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"flyctl_failed rc={result.returncode} stderr={result.stderr.strip()}")
    print("flyctl_ok=1")


if __name__ == "__main__":
    main()
