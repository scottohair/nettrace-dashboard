# Strategy Pipeline Execution Modes

## COLD pipeline data mode

- `PIPELINE_DATA_MODE=candle` (default)
  - Uses existing candle-based paths (`get_5min_candles` / `get_candles`).
  - Existing regime filters and strategy families are unchanged.

- `PIPELINE_DATA_MODE=non_candle`
  - Uses local event-derived rows from `HistoricalPrices.get_non_candle_rows`.
  - Requires at least `PIPELINE_NON_CANDLE_MIN_BARS` rows for execution.
  - Enforces non-candle-only strategy family by default.
  - With `PIPELINE_NON_CANDLE_STRICT_MODE=1` and insufficient rows, the pair is skipped.
  - With strict mode off (default), it transparently falls back to candle data.

- `PIPELINE_DATA_MODE=hybrid`
  - Prefers non-candle rows when at least `PIPELINE_NON_CANDLE_MIN_BARS` are available.
  - Falls back to candle history when non-candle rows are too sparse.

## Configuration

- `PIPELINE_DATA_MODE`: `candle` | `non_candle` | `hybrid`
- `PIPELINE_NON_CANDLE_MIN_BARS`: minimum row requirement for non-candle feed (default `24`)
- `PIPELINE_NON_CANDLE_STRICT_MODE`: force skip instead of fallback when rows are insufficient (`0`/`1`)
- `WARM_EVIDENCE_DATA_MODE`: controls WARM evidence lanes:
  - `candle` (default) uses candle rows for warm promotion/runtime collection
  - `non_candle` requires local non-candle rows for warm evidence
  - `hybrid` prefers non-candle rows and falls back to candles
- `WARM_EVIDENCE_NON_CANDLE_MIN_BARS`: minimum row requirement for non-candle warm evidence (default follows `WARM_MIN_EVIDENCE_CANDLES`, default `24`)
- `WARM_EVIDENCE_NON_CANDLE_STRICT_MODE`: when `1`, strict non-candle requirement rejects warm evidence when insufficient rows
- `WARM_EVIDENCE_NON_CANDLE_FALLBACK`: when `1`, fallback to candle evidence when non-candle rows are insufficient

## Non-candle strategy path

- Added strategy: `non_candle_microstructure` (`NonCandleMicrostructureStrategy`)
- Works with row features produced by local feed points:
  - `return_1`
  - `momentum_8`
  - `momentum_20`
  - `micro_flow`
  - `micro_volatility`
  - `spread_proxy`
- Strategy output remains compatible with the existing backtester and validator pipeline.

## WARM evidence threshold note

- `WARM_MIN_EVIDENCE_CANDLES` remains candle-count based in:
  - `agents/warm_runtime_collector.py`
  - `agents/warm_promotion_runner.py`
- Skip reason is currently recorded as:
  - `insufficient_candles:{actual}<{required}`
