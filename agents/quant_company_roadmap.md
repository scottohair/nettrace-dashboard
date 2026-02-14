# Quant Company Roadmap

Updated: 2026-02-14T22:52:10.499139+00:00

## WIN Objective
- WIN = maximize mathematically validated, risk-governed realized gains with resource-efficient multi-path execution and treasury capture in USD/USDC.
- Treasury capture assets: USD, USDC

## Scores
- Alpha score: 0.45
- Migration score: 0.75
- GTM score: 0.50
- Deployment score: 0.52
- GO live: False
- HF live ready: True

## Profit Targets
- Daily PnL: $-0.55
- Next target: $1,000.00
- Target progress: -0.05%
- Required run-rate: $885.01/hour
- Budget escalator: de_escalate x0.80
- Realized close gate: passed=True reason=passed

## Migration Phases
- Phase 1: platform_hardening [in_progress]
- Phase 2: market_connector_migration [in_progress]
- Phase 3: go_to_market_rollout [blocked]

## Region Targets
- ewr: score=0.75 role=Primary Coordinator (US East)
- nrt: score=0.41 role=Asian Exchange Primary (Tokyo)
- sin: score=0.41 role=Asian Exchange Backup (Singapore)
- ord: score=0.38 role=CME/NYMEX Proximity (US Central)
- lhr: score=0.33 role=European Exchange Hub (London)
- fra: score=0.33 role=European Backup + Risk Monitor (Frankfurt)

## Market Priorities

## GTM Stages
- private_alpha: in_progress (risk-capped automated strategy basket)
- partner_beta: blocked (dashboard + execution transparency + reserve reporting)
- public_launch: planned (multi-market quant platform + treasury custody controls)

## Blockers
- execution_health_not_green:candle_feed_stale:4052.4s>360.0s

## Profit Task Queue
- Raise realized close frequency: prioritize strategies with deterministic exits and net-positive close expectancy.
- Current run-rate gap: need $885.01/hour to hit next daily target $1,000.00.
- Deploy primary execution to region order: ewr -> nrt -> sin.
- Run base-10 and hexadecimal radix feature experiments on microstructure deltas; promote only if out-of-sample realized PnL improves.
- Apply network-stack tuning (DNS resilience, timeout policy, socket path efficiency) to reduce execution latency variance.
- Continuously harvest realized gains into treasury assets: USD and USDC.
- Prefer strategies with fast, repeatable close cycles that improve realized USD/USDC run-rate.
- Send Claude a high-priority directive each cycle with blockers, required run-rate, and top migration actions.
- Promote only strategies with positive realized PnL evidence; de-escalate automatically on drawdown or failed close windows.
