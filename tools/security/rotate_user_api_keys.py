#!/usr/bin/env python3
"""Rotate API keys for a specific user in staged or cutover mode.

Default mode is staged:
- create one new active key
- keep existing keys active

Use --deactivate-old for full cutover.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path


def generate_api_key() -> tuple[str, str, str]:
    raw = "nt_" + secrets.token_urlsafe(32)
    key_hash = hashlib.sha256(raw.encode()).hexdigest()
    key_prefix = raw[:10]
    return raw, key_hash, key_prefix


def main() -> None:
    parser = argparse.ArgumentParser(description="Rotate API keys for a user")
    parser.add_argument("--db", default="traceroute.db", help="Path to sqlite DB")
    parser.add_argument("--username", default="scott", help="Username to rotate keys for")
    parser.add_argument("--deactivate-old", action="store_true", help="Deactivate old active keys after creating new one")
    parser.add_argument("--output", default="", help="Output file for new key material (default /tmp)")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"db_not_found path={db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    user = conn.execute("SELECT id, username, tier FROM users WHERE username = ?", (args.username,)).fetchone()
    if not user:
        raise SystemExit(f"user_not_found username={args.username}")

    user_id = int(user["id"])
    tier = str(user["tier"] if "tier" in user.keys() else "free")

    active_keys = conn.execute(
        "SELECT id, key_prefix, tier FROM api_keys WHERE user_id = ? AND is_active = 1 ORDER BY id",
        (user_id,),
    ).fetchall()

    rate_limit_defaults = {
        "free": 100,
        "pro": 10000,
        "enterprise": 999999,
        "enterprise_pro": 9999999,
        "government": 9999999,
    }
    rate_limit = rate_limit_defaults.get(tier, 100)

    raw, key_hash, key_prefix = generate_api_key()

    try:
        conn.execute("BEGIN")
        cur = conn.execute(
            """
            INSERT INTO api_keys (user_id, key_hash, key_prefix, name, tier, rate_limit_daily, is_active)
            VALUES (?, ?, ?, ?, ?, ?, 1)
            """,
            (user_id, key_hash, key_prefix, f"rotated_{int(time.time())}", tier, rate_limit),
        )
        new_key_id = int(cur.lastrowid)

        deactivated = 0
        if args.deactivate_old and active_keys:
            prior_ids = [int(r["id"]) for r in active_keys]
            placeholders = ",".join("?" for _ in prior_ids)
            conn.execute(
                f"UPDATE api_keys SET is_active = 0 WHERE id IN ({placeholders}) AND user_id = ?",
                (*prior_ids, user_id),
            )
            deactivated = len(prior_ids)

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    output_path = Path(args.output) if args.output else Path(f"/tmp/nettrace_api_key_rotate_{args.username}.json")
    payload = {
        "username": args.username,
        "user_id": user_id,
        "tier": tier,
        "new_key": {
            "id": new_key_id,
            "prefix": key_prefix,
            "api_key": raw,
        },
        "old_keys": [{"id": int(r["id"]), "prefix": str(r["key_prefix"])} for r in active_keys],
        "deactivated_old": bool(args.deactivate_old),
    }
    output_path.write_text(json.dumps(payload, indent=2))
    try:
        os.chmod(output_path, 0o600)
    except Exception:
        pass

    print(
        f"ok=1 username={args.username} new_key_id={new_key_id} old_active={len(active_keys)} "
        f"deactivated={deactivated} output={output_path}"
    )


if __name__ == "__main__":
    main()
