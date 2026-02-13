---
name: codex-claude-quant-collab
description: Coordinate Codex and Claude on quant research, validation, and rollout using a deterministic handoff protocol with measurable gates. Use when the user asks to collaborate with Claude, stage messages for Claude ingestion, share strategy results, or drive end-to-end improvements from backtest to promotion and budget.
---

# Codex-Claude Quant Collaboration

Run this skill when collaboration must be explicit, auditable, and tied to quant outcomes.

## Execute Collaboration Loop

1. Build a deterministic handoff bundle from local artifacts.
2. Send only prioritized asks to Claude with clear acceptance criteria.
3. Receive Claude directives and map each directive to a local action.
4. Execute actions and rerun validation.
5. Publish outcome deltas and next asks.

## Build Handoff Bundle

Run:

```bash
python3 skills/codex-claude-quant-collab/scripts/build_handoff.py
```

The script writes:

- `agents/claude_staging/codex_to_claude_handoff.json`
- append event to `agents/claude_staging/duplex_to_claude.jsonl`

Use these fields as the source of truth:

- `quant100.summary`
- `pipeline.stage_counts`
- `pipeline.failed_cold_reasons_top`
- `asks_for_claude`
- `acceptance_criteria`

## Force Deterministic Decisions

Require each candidate to meet all gates before promotion/budget:

1. Positive expected return after fees/slippage assumptions.
2. Out-of-sample walk-forward robustness.
3. Monte Carlo downside bounds within limits.
4. Max drawdown below strategy/pool limit.
5. Clear risk budget and kill-switch condition.

Reject candidates with missing data, ambiguous metrics, or unverifiable claims.

## Handoff Message Format

Use this fixed shape when sending tasks:

```json
{
  "objective": "single measurable objective",
  "inputs": ["artifact1", "artifact2"],
  "constraints": ["risk constraint 1", "risk constraint 2"],
  "required_output": ["decision", "numbers", "implementation steps"],
  "acceptance_criteria": ["criterion 1", "criterion 2"]
}
```

Keep objectives narrow. Send 1-3 objectives per cycle.

## References

- Collaboration protocol: `references/protocol.md`
- Prompt templates: `references/prompts.md`
