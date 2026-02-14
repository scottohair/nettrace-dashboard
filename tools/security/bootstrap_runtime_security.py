#!/usr/bin/env python3
"""Bootstrap secure runtime env defaults and missing secret material.

This script only appends missing keys; it never overwrites existing values.
"""

from __future__ import annotations

import argparse
import os
import secrets
from pathlib import Path


SAFE_DEFAULTS = {
    "APP_ENV": "production",
    "ALLOW_QUERY_API_KEY_AUTH": "0",
    "MCP_ENABLE_REMOTE_EXEC": "0",
    "MCP_ENABLE_FLY_MUTATIONS": "0",
    "ALLOW_RAW_WALLET_PRIVATE_KEY": "0",
    "ALLOW_LEGACY_XOR_CREDENTIAL_DECRYPT": "0",
    "SESSION_COOKIE_SECURE": "1",
    "ENABLE_HSTS": "1",
    "PASSWORD_MIN_LENGTH": "12",
    "LOGIN_MAX_ATTEMPTS": "8",
    "LOGIN_WINDOW_SECONDS": "900",
    "MFA_CHALLENGE_TTL_SECONDS": "300",
    "MFA_MAX_VERIFY_ATTEMPTS": "5",
    "MFA_TOTP_WINDOW_STEPS": "1",
    "REQUIRE_MFA_FOR_SENSITIVE": "1",
    "REQUIRE_MFA_FOR_USERNAMES": "scott",
    "TRUSTED_ORIGINS": "https://nettrace-dashboard.fly.dev,http://localhost:12034,http://127.0.0.1:12034",
}

SECRET_FACTORIES = {
    "SECRET_KEY": lambda: secrets.token_hex(32),
    "CREDENTIAL_ENCRYPTION_KEY": lambda: secrets.token_urlsafe(48),
    "INTERNAL_SIGNAL_SECRET": lambda: secrets.token_urlsafe(48),
    "MCP_AGENT_SECRET": lambda: secrets.token_urlsafe(48),
    "NETTRACE_API_KEY": lambda: "nt_" + secrets.token_urlsafe(32),
}


def _parse_env(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def _append_missing(path: Path, values: dict[str, str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _parse_env(path)
    missing = {k: v for k, v in values.items() if k not in existing}
    if not missing:
        return 0

    lines = []
    if path.exists() and path.read_text().strip():
        lines.append("")
    lines.append("# Added by tools/security/bootstrap_runtime_security.py")
    for key in sorted(missing):
        lines.append(f"{key}={missing[key]}")
    with path.open("a") as fh:
        fh.write("\n".join(lines) + "\n")

    try:
        os.chmod(path, 0o600)
    except Exception:
        pass
    return len(missing)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap runtime security env defaults")
    parser.add_argument("--env-file", default="agents/.env", help="Target env file (default: agents/.env)")
    parser.add_argument("--apply", action="store_true", help="Append missing keys to env file")
    args = parser.parse_args()

    env_path = Path(args.env_file)
    existing = _parse_env(env_path)

    generated: dict[str, str] = {}
    for key, factory in SECRET_FACTORIES.items():
        if not existing.get(key):
            generated[key] = factory()

    desired = dict(SAFE_DEFAULTS)
    desired.update(generated)

    if args.apply:
        added = _append_missing(env_path, desired)
        print(
            f"applied=1 env_file={env_path} added={added} "
            f"generated_secrets={len(generated)} generated_keys={','.join(sorted(generated.keys())) if generated else '-'}"
        )
    else:
        missing_count = sum(1 for k in desired if k not in existing)
        print(
            f"applied=0 env_file={env_path} would_add={missing_count} "
            f"would_generate={len(generated)} generated_keys={','.join(sorted(generated.keys())) if generated else '-'}"
        )


if __name__ == "__main__":
    main()
