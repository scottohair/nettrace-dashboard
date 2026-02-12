"""AdvancedTeam — 5-agent research/strategy/risk/execution/learning team.

Architecture:
  - Shared message bus (JSONL append-only log)
  - Round-robin DFA: Research -> Strategy -> Risk -> Execution -> Learning -> Research
  - Each agent has sandboxed state, communicates only via the message bus
  - Coordinator runs the DFA loop

Agents:
  1. ResearchAgent  — Scrapes market data, news, on-chain metrics
  2. StrategyAgent  — Generates trade signals and strategy proposals
  3. RiskAgent      — Evaluates proposals against risk limits
  4. ExecutionAgent — Writes approved trades to queue for orchestrator
  5. LearningAgent  — Reviews results, feeds insights back to research
"""

__version__ = "1.0.0"
