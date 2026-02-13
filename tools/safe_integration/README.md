# Safe Integration Toolkit

This toolkit integrates agent/quant upgrades into the platform with a strict safety-first rollout.

## What it validates

- Execution health is green (`agents/execution_health_status.json`).
- Trading lock is not active (`agents/trading_lock.json`).
- Quant validation has usable signal (not all `no_data`).
- Claude ingest bundle has traceable metadata (`bundle_id`, `bundle_hash`).
- Claude duplex messages include `trace_id`.
- Sniper BUY path includes end-to-end exit + EV gates.

## Run the guard

```bash
python3.11 tools/safe_integration/integration_guard.py
```

Optional custom root:

```bash
python3.11 tools/safe_integration/integration_guard.py --root /Users/scott/src/quant
```

## Rollout policy

1. Paper/staged first: no live budget escalation until guard passes.
2. Realized-close evidence required: no promotions on open-only BUY flow.
3. Traceability required: Claude/Codex directives and outcomes must share trace IDs.
4. Fast rollback: if execution health turns red, re-enable lock and halt new BUY entries.
