# Quant Company Roadmap

Updated: 2026-02-14T06:44:23.387587+00:00

## WIN Objective
- WIN = maximize mathematically validated, risk-governed realized gains with resource-efficient multi-path execution and treasury capture in USD/USDC.
- Treasury capture assets: USD, USDC

## Scores
- Alpha score: 0.45
- Migration score: 0.70
- GTM score: 0.75
- Deployment score: 0.31
- GO live: True
- HF live ready: False

## Profit Targets
- Daily PnL: $0.00
- Next target: $1,000.00
- Target progress: 0.00%
- Required run-rate: $57.94/hour
- Budget escalator: de_escalate x0.80
- Realized close gate: passed=True reason=passed

## Migration Phases
- Phase 1: platform_hardening [in_progress]
- Phase 2: market_connector_migration [in_progress]
- Phase 3: go_to_market_rollout [blocked]

## Region Targets
- ewr: score=0.47 role=Primary Coordinator (US East)
- nrt: score=0.23 role=Asian Exchange Primary (Tokyo)
- sin: score=0.23 role=Asian Exchange Backup (Singapore)
- ord: score=0.20 role=CME/NYMEX Proximity (US Central)
- lhr: score=0.15 role=European Exchange Hub (London)
- fra: score=0.15 role=European Backup + Risk Monitor (Frankfurt)

## Market Priorities

## GTM Stages
- private_alpha: in_progress (risk-capped automated strategy basket)
- partner_beta: blocked (dashboard + execution transparency + reserve reporting)
- public_launch: planned (multi-market quant platform + treasury custody controls)

## Blockers
- hf_live_not_ready

## Profit Task Queue
- Raise realized close frequency: prioritize strategies with deterministic exits and net-positive close expectancy.
- HF live gate is blocked; keep HF lane in paper mode and fix DNS + venue credentials before live budget.
- Fix DNS resolution for api.coinbase.com/api.exchange.coinbase.com in execution runtime.
- Stabilize Coinbase API health before live budget escalation (improve retries + region routing).
- Deploy FIX gateway and set FIX_GATEWAY_URL as live fallback route.
- Execution-health gate failed (telemetry_success_rate_low:0.0100<0.5500); block budget escalations until DNS/API/reconcile checks are green.
- Current run-rate gap: need $57.94/hour to hit next daily target $1,000.00.
- Deploy primary execution to region order: ewr -> nrt -> sin.
- Run base-10 and hexadecimal radix feature experiments on microstructure deltas; promote only if out-of-sample realized PnL improves.
- Apply network-stack tuning (DNS resilience, timeout policy, socket path efficiency) to reduce execution latency variance.
- Continuously harvest realized gains into treasury assets: USD and USDC.
- Prefer strategies with fast, repeatable close cycles that improve realized USD/USDC run-rate.
