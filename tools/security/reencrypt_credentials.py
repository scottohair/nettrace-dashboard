#!/usr/bin/env python3
"""Re-encrypt stored credentials to the active CREDENTIAL_ENCRYPTION_KEY.

Safe behavior:
- Dry-run by default
- Archives previous encrypted payload into user_credentials_history before update
- Never deletes rows
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import os
import sqlite3
from pathlib import Path


def derive_key(password: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000, dklen=32)


def encrypt_v2(plaintext: str, key_material: str) -> str:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    salt = os.urandom(16)
    nonce = os.urandom(12)
    key = derive_key(key_material, salt)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext.encode(), None)
    return "v2:" + base64.b64encode(salt + nonce + ciphertext).decode()


def decrypt_any(payload: str, key_candidates: list[str], allow_legacy_xor: bool) -> str | None:
    if not payload:
        return None
    raw_payload = payload
    version = "legacy"
    if payload.startswith("v2:"):
        version = "v2"
        raw_payload = payload[3:]
    try:
        blob = base64.b64decode(raw_payload)
    except Exception:
        return None

    # Try AES-GCM path first.
    if len(blob) >= 29:
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM

            salt = blob[:16]
            nonce = blob[16:28]
            ciphertext = blob[28:]
            for candidate in key_candidates:
                try:
                    key = derive_key(candidate, salt)
                    return AESGCM(key).decrypt(nonce, ciphertext, None).decode()
                except Exception:
                    continue
        except Exception:
            pass

    if version == "v2":
        return None

    # Optional legacy XOR/HMAC fallback.
    if not allow_legacy_xor or len(blob) < 33:
        return None
    salt = blob[:16]
    mac = blob[16:32]
    encrypted = blob[32:]
    for candidate in key_candidates:
        key = derive_key(candidate, salt)
        expected = hmac.new(key, encrypted, hashlib.sha256).digest()[:16]
        if not hmac.compare_digest(mac, expected):
            continue
        try:
            plain = bytes(
                a ^ b
                for a, b in zip(encrypted, (key * ((len(encrypted) // 32) + 1))[: len(encrypted)])
            )
            return plain.decode()
        except Exception:
            continue
    return None


def parse_candidates(new_key: str, old_keys_csv: str) -> list[str]:
    out: list[str] = []

    def add(value: str) -> None:
        value = str(value or "").strip()
        if value and value not in out:
            out.append(value)

    add(new_key)
    for k in str(old_keys_csv or "").split(","):
        add(k)
    for k in str(os.environ.get("CREDENTIAL_ENCRYPTION_KEY_FALLBACKS", "")).split(","):
        add(k)
    add(os.environ.get("SECRET_KEY", ""))
    return out


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"").strip("'"))


def ensure_history_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_credentials_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            exchange TEXT NOT NULL,
            credential_data TEXT NOT NULL,
            rotated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-encrypt user_credentials rows with active key")
    parser.add_argument("--db", default="traceroute.db", help="Path to sqlite DB")
    parser.add_argument("--env-file", default="agents/.env", help="Optional env file to source")
    parser.add_argument("--old-keys", default="", help="Comma-separated legacy key materials")
    parser.add_argument("--allow-legacy-xor", action="store_true", help="Allow decrypting legacy XOR payloads")
    parser.add_argument("--apply", action="store_true", help="Apply updates (default is dry-run)")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"db_not_found path={db_path}")

    load_env_file(Path(args.env_file))
    new_key = str(os.environ.get("CREDENTIAL_ENCRYPTION_KEY", "")).strip()
    if not new_key:
        raise SystemExit("missing_new_key: set CREDENTIAL_ENCRYPTION_KEY")

    key_candidates = parse_candidates(new_key, args.old_keys)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    ensure_history_table(conn)

    rows = conn.execute(
        "SELECT id, user_id, exchange, credential_data FROM user_credentials ORDER BY id"
    ).fetchall()

    scanned = 0
    decryptable = 0
    failures = 0
    to_update: list[tuple[int, int, str, str, str]] = []

    for row in rows:
        scanned += 1
        payload = str(row["credential_data"] or "")
        plain = decrypt_any(payload, key_candidates, allow_legacy_xor=args.allow_legacy_xor)
        if plain is None:
            failures += 1
            continue
        decryptable += 1
        rotated = encrypt_v2(plain, new_key)
        if rotated != payload:
            to_update.append((int(row["id"]), int(row["user_id"]), str(row["exchange"]), payload, rotated))

    updated = 0
    if args.apply and to_update:
        try:
            conn.execute("BEGIN")
            for row_id, user_id, exchange, old_payload, new_payload in to_update:
                conn.execute(
                    "INSERT INTO user_credentials_history (user_id, exchange, credential_data) VALUES (?, ?, ?)",
                    (user_id, exchange, old_payload),
                )
                conn.execute(
                    "UPDATE user_credentials SET credential_data = ?, created_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (new_payload, row_id),
                )
                updated += 1
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(
        f"apply={1 if args.apply else 0} scanned={scanned} decryptable={decryptable} "
        f"failures={failures} candidates={len(key_candidates)} updates_ready={len(to_update)} updated={updated}"
    )


if __name__ == "__main__":
    main()
