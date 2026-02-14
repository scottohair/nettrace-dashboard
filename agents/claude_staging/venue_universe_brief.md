# Venue Universe Expansion Brief

- Generated: 2026-02-14T04:39:48.773797+00:00
- Universe: 1000 venues (target 1000)
- Mix: DEX=900, CEX=100
- Active onboarding batch: 120 venues
- Queue tasks: 360 (manual=120)

## Security Policy
- No plaintext password storage
- CEX signup/KYC/API key creation requires human approval
- Auto steps restricted to read-only discovery and dry-run smoke tests

## Next Actions
1. Work queue `pending_auto` items first (wallet/rpc/quote smoke tests).
2. Present `pending_human` CEX steps to Scott for approval/OTP.
3. Only move venues to live trading after execution-health + risk gates pass.
