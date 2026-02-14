# Security Baseline (SOC 2 + DoD-Oriented)

## Implemented now

1. Login protection
- Password policy upgraded to strong-complexity by default (`PASSWORD_MIN_LENGTH`, uppercase/lowercase/digit/symbol).
- Brute-force protection added (`auth_login_attempts` table + windowed lockout + backoff).
- Session fixation mitigation on login/register (`session.clear()`, fresh session).

2. Session and web protection
- Hardened cookie defaults: `HttpOnly`, `SameSite`, conditional `Secure`, bounded lifetime.
- Added baseline headers: `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, `Permissions-Policy`, optional HSTS.
- Added origin guard for state-changing cookie-authenticated requests (CSRF resistance via trusted origins).

3. Key and credential protection
- Replaced insecure fallback encryption behavior for new credentials with AES-256-GCM only.
- Introduced managed key chain:
  - primary: `CREDENTIAL_ENCRYPTION_KEY`
  - fallback rotation set: `CREDENTIAL_ENCRYPTION_KEY_FALLBACKS`
  - compatibility fallback: `SECRET_KEY`
- Added credential history table and atomic upsert/update flow to avoid destructive credential loss.
- Credential deletes now archive prior encrypted record to history before removal.

4. API and internal auth hardening
- API key transport moved to header-first (`Authorization` / `X-Api-Key`), query key disabled by default.
- Constant-time comparison for service API key validation.
- Signals internal auth now supports dedicated secret (`INTERNAL_SIGNAL_SECRET`) and constant-time compare.
- CORS restricted to configured allowlists in `api_v1` and `signals_api` (no wildcard by default).

5. MCP and remote command hardening
- MCP remote agent connection now requires `MCP_AGENT_SECRET`.
- Remote exec disabled by default (`MCP_ENABLE_REMOTE_EXEC=0`).
- Control-plane mutations disabled by default (`MCP_ENABLE_FLY_MUTATIONS=0`).
- Exec commands gated by explicit prefix allowlist (`MCP_EXEC_ALLOWLIST`).

6. Wallet-key safety
- Raw wallet private key usage blocked in production by default (`ALLOW_RAW_WALLET_PRIVATE_KEY=0`).
- Encrypted key decrypt prefers `CREDENTIAL_ENCRYPTION_KEY` and supports optional legacy decrypt mode only via explicit flag.

## Scott asset-safety guarantees in code

- Credential updates are non-destructive and archived first.
- Credential deletions are archived first.
- Trading snapshot and asset transition service-auth now uses constant-time secret validation.
- Cross-site state-changing cookie requests are blocked unless from trusted origins.

## Required operational steps (still needed for compliance)

1. Enforce MFA for all operator and admin access.
2. Move secrets into managed KMS/HSM (no long-lived plaintext env secrets on developer machines).
3. Turn on centralized immutable audit logs (SIEM) and alerting.
4. Add quarterly key rotation with tested rollback runbook.
5. Add formal least-privilege IAM/RBAC reviews for all bots/agents.
6. Add disaster recovery drills and backup restore proofs.
7. Add dependency and container CVE scanning in CI.
8. Add production WAF and egress policy enforcement.
9. Add formal incident response and evidence retention procedures.
10. Map controls to SOC 2 Trust Services Criteria and DoD SRG/NIST 800-53 profile with evidence collection.

## Operator runbook commands

1. Bootstrap secure runtime env defaults and missing secrets:
`python3.11 tools/security/bootstrap_runtime_security.py --env-file agents/.env --apply`

2. Re-encrypt stored credentials to active encryption key:
`python3.11 tools/security/reencrypt_credentials.py --db traceroute.db --allow-legacy-xor --apply`

3. Stage API key rotation for Scott (creates new key, keeps old active):
`python3.11 tools/security/rotate_user_api_keys.py --db traceroute.db --username scott`

4. Cut over and deactivate prior active API keys (only after clients are updated):
`python3.11 tools/security/rotate_user_api_keys.py --db traceroute.db --username scott --deactivate-old`

5. Push hardened security settings to Fly secrets:
`python3.11 tools/security/push_fly_secrets.py --env-file agents/.env --app nettrace-dashboard --apply`

## MFA enrollment flow

1. Login with username/password:
`POST /api/login`

2. Start MFA setup:
`POST /api/mfa/setup` with body `{ "password": "..." }`

3. Confirm MFA:
`POST /api/mfa/enable` with body `{ "code": "123456" }`

4. Login after MFA is enabled:
- `POST /api/login` returns `mfa_required=true` and a challenge token
- complete with `POST /api/login/mfa` body `{ "challenge_token": "...", "code": "123456" }`
