# Codex Stage Rollout Handoff (2026-02-24)

## Objective

Stage the new quantitative rollout/routing/scanner improvements behind measurable gates, keep capital protection strict, and hand a deterministic execution plan to Claude for continuation.

## Verified Runtime Snapshot

All values below are from local artifacts as of 2026-02-24.

1. `agents/execution_health_status.json`
- `green=false`
- `reason=reconcile_status_stale`
- `components.reconcile.age_seconds=808190.325`
- Coinbase DNS and telemetry are healthy, but reconcile freshness is blocking.

2. `agents/execution_health_history.jsonl` (tail)
- Repeated `reason=reconcile_status_stale` through `2026-02-24T07:22:54.586562+00:00`.

3. `agents/orchestrator_heartbeat.json`
- Last heartbeat timestamp is `2026-02-23T20:48:37.032268+00:00`.
- Indicates orchestrator heartbeat exists and metric writes can succeed, but runtime recency is stale.

4. `agents/top25_live_campaign_report.json`
- 1000 ideas generated and top-25 selected.
- Campaign stopped early with `NO_GO` and trading lock due to close/reconcile path issue.

5. `agents/hf_quant_models_scan.json`
- Candidate set exists (`candidates_total=239`, `top_k=25`) and includes deployable baseline models for sentiment/time-series lanes.

## Implemented Code (Now in Working Tree)

1. `agents/realtime_orchestrator.py`
- Added `AdaptiveRolloutGate` with live sizing multiplier from:
  - recent trade outcomes
  - venue API health snapshot
  - order lifecycle flow metrics (`fill_per_ack`, `blocked_rate`)
- Added rollout env controls:
  - `ORCH_ROLLOUT_*`
  - `ORCH_NYNJ_*`
  - `ORCH_ROUTE_VENUE_PENALTY_MS`
- Integrated rollout multiplier into Coinbase, Kraken, Kraken-stocks, and E*Trade order sizing.
- Added venue-aware `CrossRegionRouter` scoring and NY/NJ tie-break preference for US venues.

2. `scheduler.py`
- Switched to batch persistence for scan cycle DB writes.
- Avoided per-target connect/commit overhead.
- Added geolocation fetch metadata and dedup to reduce external-call sleeps and cycle latency.

3. Build path (Apple Silicon performance scaffold)
- Added `agents/build_fast_engines.sh` with Darwin arm64 optimization flags (`-mcpu=apple-m3`) and `--dry-run`.
- Updated compile guidance in:
  - `agents/fast_bridge.py`
  - `agents/fast_exec_bridge.py`

4. Validation completed
- `python3 -m py_compile agents/realtime_orchestrator.py scheduler.py agents/fast_bridge.py agents/fast_exec_bridge.py`
- `./agents/build_fast_engines.sh --dry-run`

## Staged Rollout Plan (Quantitative, Not Blind)

### Phase 0: Guarded Canary (single-region execution preference)

Use these runtime settings first:

```bash
ORCH_ROLLOUT_ENABLED=1
ORCH_ROLLOUT_MODE=aggressive
ORCH_ROLLOUT_MIN_MULT=0.50
ORCH_ROLLOUT_MAX_MULT=1.35
ORCH_ROLLOUT_MIN_RECENT_TRADES=8
ORCH_ROLLOUT_RECENT_WINDOW=30
ORCH_ROLLOUT_API_SUCCESS_FLOOR=0.90
ORCH_ROLLOUT_MIN_FILL_PER_ACK=0.08
ORCH_ROLLOUT_MAX_BLOCKED_RATE=0.75
ORCH_ROLLOUT_CACHE_TTL_S=3
ORCH_NYNJ_PREFERENCE_ENABLED=1
ORCH_NYNJ_HOME_REGION=ewr
ORCH_NYNJ_TIE_MS=5.0
ORCH_ROUTE_VENUE_PENALTY_MS=12.0
```

### Phase 1: Promotion Conditions

Promote only if all are true over a rolling window:

1. Reconcile freshness recovered:
- `execution_health_status.green=true`
- no stale reconcile reason for 3 consecutive checks.

2. Rollout flow quality:
- `fill_per_ack >= 0.10`
- `blocked_rate <= 0.60`
- `api_success >= 0.95`

3. PnL sanity:
- no negative realized drift across the promotion window.

### Phase 2: Controlled expansion

Increase `ORCH_ROLLOUT_MAX_MULT` from `1.35` to `1.75` only after Phase 1 is sustained and lock remains clear.

## Claude Objectives (Next Cycle)

Objective 1:
- Recover reconcile freshness and keep it green under live load.
- Inputs: `agents/execution_health_status.json`, `agents/execution_health_history.jsonl`, `agents/exit_manager.py`, `agents/reconcile_loop.out`.
- Acceptance:
  - `reconcile_status_stale` eliminated.
  - stale age under threshold for 3 consecutive checks.

Objective 2:
- Add explicit telemetry proving rollout gate impact by venue.
- Inputs: `agents/realtime_orchestrator.py`, `agents/execution_telemetry.db`, `agents/kpi_tracker.py`.
- Acceptance:
  - venue-level `rollout_mult`, `fill_per_ack`, `blocked_rate`, realized PnL deltas in one reportable view.

Objective 3:
- Tune no-loss gate tolerance for high-confidence latency-arb without removing capital protection.
- Inputs: `agents/fast_exec.c`, `agents/sniper.py`, `agents/agent_goals.py`.
- Acceptance:
  - measurable increase in approved opportunities with no degradation in realized close quality.

## Rollback Conditions

Immediately revert to `ORCH_ROLLOUT_MAX_MULT=1.0` and disable aggressive mode if any:

1. Execution health returns to stale reconcile.
2. Blocked rate spikes above configured max for two consecutive windows.
3. Realized PnL window turns negative beyond configured drawdown guardrails.

## Artifact Index

1. `agents/HANDOFF.md`
2. `agents/claude_staging/codex_to_claude_handoff.json`
3. `agents/claude_staging/duplex_to_claude.jsonl`
4. `agents/realtime_orchestrator.py`
5. `scheduler.py`
6. `agents/build_fast_engines.sh`
