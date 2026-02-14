# System Test Report

Generated: 2026-02-13T04:43:49Z

## Result
- Status: PASS_WITH_ACTIONS
- py_compile: True
- pytest exit: 0

## Key Money Metrics
- Latest portfolio snapshot: $12.26
- Peak portfolio snapshot: $13.4
- Live closed-trade PnL sum: $0.0

## Pipeline
- GO decision: GO (go_live=True)
- Stage counts: {'COLD': 259, 'HOT': 15, 'WARM': 23}
- HOT promotions: 15

## Actions Needed
- No realized live-trading profit evidence yet; run paper-to-live staged trials and capture closed-trade PnL.
- Fly dashboard endpoints unresolved from this runtime; fix DNS/network/agent deployment path.
- Equities/forex connector expansion incomplete (E*Trade/IBKR unavailable).
- Quant-100 currently yields 0 promoted_warm; retune parameter space and relax only statistically justified gates.
- Funded capital is concentrated in one pair; seed at least one more non-correlated pair before scaling.

## HOT Simulation
- Strategies simulated: 15
- Positive-return strategies: 15
- Avg return: 0.201907%
- Avg Sharpe: 0.57284

