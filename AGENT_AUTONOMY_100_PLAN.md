# 100-Point Autonomy Roadmap

## Status Goal
Aim: run the quant platform with minimal manual intervention while enforcing strict safety and derisking.

## 1) Environment & Tooling
1. Add a single executable bootstrap script for agent startup (`scripts/autonomy_bootstrap.sh`).
2. Add dependency check at bootstrap for Python + core modules.
3. Add a one-command installer for repo requirements in an isolated venv.
4. Add runtime self-check for network connectivity before any feed operations.
5. Add automatic fallback to cached dependencies when pip install fails.
6. Add CLI health preflight script (`scripts/preflight_check.py`) returning machine-readable status.
7. Add automatic log rotation policy for all agents to prevent disk exhaustion.
8. Add centralized config loader with schema validation.
9. Add environment drift detector and drift diff report.
10. Add lockfile checksum verification for every startup script change.

## 2) Orchestration
11. Add a single orchestrator manifest for startup order.
12. Add deterministic startup sequencing with explicit dependencies.
13. Add auto-restart with exponential backoff for failing agents.
14. Add per-agent health probe endpoint/heartbeat updates.
15. Add global STOP/PAUSE/CHECK gates.
16. Add automatic worker pool resizing based on queue length.
17. Add supervisor runbook generator from current agent states.
18. Add auto-promote stale agents from warning to active when safe.
19. Add per-agent max uptime and recycle policy.
20. Add graceful shutdown hooks for all long-running agents.

## 3) Pipeline Automation
21. Add scheduled `run_full_pipeline` cycle every N minutes with jitter.
22. Add dual-mode backtest lane (`candle` + `non_candle`) with compare report.
23. Add strategy pool autoscaling by recent acceptance rate.
24. Add adaptive strategy sampling for cold starts (fast path, full path).
25. Add automatic evidence snapshot persistence per pipeline run.
26. Add pipeline run deduplication on same candle window.
27. Add warm/minimum evidence threshold alerts.
28. Add automatic rerun rules for transient evaluator failures.
29. Add run-level feature flag export for reproducible experiments.
30. Add strategy hash + params fingerprint for deterministic promotions.
31. Add auto-remediation for repeated WARM rejections.
32. Add performance leaderboard with decayed memory for all strategies.
33. Add auto-prune of dead strategies by inactivity.
34. Add automated cross-split validation before HOT eligibility.
35. Add paper-to-live simulation dry-run envelope checks.

## 4) Trading Intelligence Upgrades
36. Add multi-horizon feature engine (1m, 5m, 15m, 1h).
37. Add non-candle features from order book depth snapshots.
38. Add signal conflict resolver (max confidence arbitration).
39. Add regime-specific strategy weights.
40. Add volatility-adaptive position sizing prefilter.
41. Add microstructure anomaly detector for spoof/liquidity shocks.
42. Add momentum/mean-reversion ensemble scoring.
43. Add model confidence calibration by regime and spread.
44. Add strategy-level feature attribution logs.
45. Add adaptive signal throttling under high false-positive pressure.
46. Add market-hours aware behavior profile.
47. Add inter-market spread arbitrage guardrails.
48. Add venue-specific slippage model calibration.
49. Add dynamic horizon selection from realized alpha persistence.
50. Add policy that blocks new entries during extreme quote divergence.

## 5) Risk and Governance
51. Add hard per-pair exposure limits with hard stop at breach.
52. Add auto-risk budget allocator with confidence penalties.
53. Add max consecutive loss hard stop and cool-down.
54. Add drawdown-aware strategy kill switch.
55. Add VaR and ES real-time tracking per strategy.
56. Add pair concentration cap and heatmap.
57. Add treasury cash floor guard for emergency stop.
58. Add automatic position unwind ladder for severe stress.
59. Add anti-overtrade governor (`max_trades_per_minute`).
60. Add risk explainability report each time budget is changed.
61. Add kill switch triggers from execution_health (egress/dns/telemetry).
62. Add adverse trade clustering detector.
63. Add realized-loss-based stage demotion with audit entry.
64. Add risk governor simulation before every cold-to-warm transition.
65. Add stale-order age and cancellation rate controls.
66. Add margin/maker-taker fee guard for pair selection.

## 6) Execution & Connectivity
67. Add exchange connector circuit-breaker health scoring.
68. Add DNS resolver fallback sequence verification.
69. Add auto-switch to backup exchange on sustained errors.
70. Add order routing matrix by liquidity and gas/fees.
71. Add idempotent order-key generation.
72. Add pre-trade orderbook sanity check.
73. Add execution latency histogram and p99 gating.
74. Add throttled retry with jitter and cap.
75. Add partial-fill reconciliation automation.
76. Add auto-cancel stale open orders after configured TTL.
77. Add trade outcome validator before accounting updates.
78. Add broker-mode simulation fallback when venue unavailable.
79. Add heartbeat-based connector warmup before trading.
80. Add queue priority for high-confidence actions only.

## 7) Data Quality & Storage
81. Add schema checks for incoming data feeds.
82. Add candle and point deduplication with deterministic ordering.
83. Add missing-data gap filling policy for non-critical feeds.
84. Add immutable append-only event log for each strategy.
85. Add archival retention policy and compression.
86. Add fast local cache invalidation strategy.
87. Add data freshness SLAs by source.
88. Add outlier filter for price jumps.
89. Add time-sync checks for all timestamp fields.
90. Add source lineage tags on each feature row.

## 8) Observability & Ops
91. Add single-pane status dashboard for stage transitions.
92. Add 5-second status heartbeat snapshots.
93. Add alert channel mapping: NO_GO/GO transitions.
94. Add anomaly scoring for telemetry starvation.
95. Add execution trace IDs across all agents.
96. Add incident auto-ticket generation on repeated failures.
97. Add periodic audit report diff to `growth_supervisor`.
98. Add synthetic canary trade in a test mode for end-to-end checks.
99. Add one-command rollback of latest model/pipeline change.
100. Add end-of-day autonomous postmortem generator with failed/successful decisions.
