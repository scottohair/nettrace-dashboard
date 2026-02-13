# Protocol

## Cycle Steps

1. Snapshot system state from local artifacts.
2. Produce a compact brief with top blockers.
3. Request targeted algorithm/system proposals from Claude.
4. Execute only proposals that can be tested locally.
5. Run quant validation and compare deltas vs previous cycle.
6. Keep, revise, or roll back based on metrics.

## Required Inputs

- `agents/quant_100_results.json`
- `agents/quant_100_plan.json`
- `agents/pipeline.db`
- `agents/claude_staging/duplex_from_claude.jsonl` if present

## Required Outputs

- Updated handoff JSON for Claude
- Updated duplex message line for Claude ingestion
- Local change summary with objective pass/fail

## Evaluation Rules

1. Treat missing data as a blocker, not a win.
2. Prefer robust strategies over high single-run returns.
3. Gate budget increases by realized stability, not optimism.
4. Keep all decisions reproducible from local files.
