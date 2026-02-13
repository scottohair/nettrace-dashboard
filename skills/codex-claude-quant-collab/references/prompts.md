# Prompt Templates

## Claude Ask Template

```text
Objective: <single objective>
Context: <current metrics and blocker>
Inputs:
- <artifact path>
- <artifact path>
Constraints:
- Preserve strict risk guardrails
- Provide deterministic validation path
Required output:
- Ranked options with expected impact
- Exact code/data changes
- Validation plan and pass thresholds
Acceptance criteria:
- <criterion>
- <criterion>
```

## Codex Response Back To Claude

```text
Result summary:
- Implemented: <change>
- Metrics delta: <before -> after>
- Decision: keep | revise | rollback
Next ask:
- <one high-priority objective>
```

## Promotion Recommendation Format

```json
{
  "strategy": "name",
  "pair": "BTC-USD",
  "decision": "promote|reject|hold",
  "evidence": {
    "return_pct": 0.0,
    "drawdown_pct": 0.0,
    "walkforward_oos_return_pct": 0.0,
    "monte_carlo_p05_return_pct": 0.0
  },
  "risk_budget_usd": 0.0,
  "kill_switch": "condition"
}
```
