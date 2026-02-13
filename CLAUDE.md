# NetTrace Quant — AI Coordination Protocol

## Who Works Here

Two AI agents collaborate on this codebase 24/7 under Scott's direction:

| Agent | Model | Role | Strengths |
|-------|-------|------|-----------|
| **Claude Code** | Opus 4.6 | Implementation lead | File editing, deployment, debugging, system integration, Fly.io ops |
| **Codex** | Codex 5.3 | Strategy & research lead | Strategy design, ML architecture, backtesting, signal research |

**Scott** is the operator and final authority. His trading rules (Section 5 of CODEX_BRIEF.md) are immutable law.

---

## Communication Protocol

### Shared State Files (how we talk to each other)

All coordination happens through files in `agents/` that both agents can read/write:

| File | Purpose | Writer | Reader |
|------|---------|--------|--------|
| `agents/HANDOFF.md` | Active handoff notes between agents | Both | Both |
| `agents/CODEX_BRIEF.md` | Full system briefing for Codex | Claude Code | Codex |
| `agents/AGENT_PLAYBOOK.md` | Agent training docs | Both | Both |
| `agents/agent_goals.py` | Encoded trading rules (source of truth) | Claude Code | All agents |
| `agents/growth_batch_config.json` | Current growth batch plan | Codex | Claude Code |

### Handoff Protocol

When finishing a work session, update `agents/HANDOFF.md` with:
```markdown
## [Date] [Agent Name] Handoff
- **What I did**: (bullet list of changes)
- **What's next**: (what the other agent should do)
- **Blockers**: (anything stuck or needing Scott's input)
- **Deploy status**: (deployed? what version? any issues?)
```

### Conflict Resolution

1. **File conflicts**: If both agents modified the same file, the most recent commit wins. The other agent rebases.
2. **Strategy conflicts**: Both agents propose, Scott decides. Write proposals to `agents/HANDOFF.md`.
3. **Architecture conflicts**: `agent_goals.py` is the tiebreaker. If GoalValidator says no, the trade doesn't happen.
4. **Emergency**: If an agent detects capital loss > 5% daily, it writes `HARDSTOP` to `agents/HANDOFF.md` and the other agent must halt all trading immediately.

---

## Rules for Both Agents

### NEVER
- Hardcode trading parameters (use `risk_controller.py` and `agent_goals.py`)
- Commit secrets, API keys, or private keys
- Push `$0` portfolio snapshots to Fly
- Execute trades from non-primary regions (only `ewr` trades)
- Market orders unless urgency is "critical" (BE A MAKER: limit orders, 0.4% fee)
- Override Scott's three rules: (1) never lose money, (2) always make money, (3) grow money faster
- Delete or overwrite the other agent's uncommitted work without checking `HANDOFF.md`

### ALWAYS
- Run `pytest tests/ -x -q` before committing
- Gate ALL trades through `GoalValidator.should_trade()` (70% confidence, 2+ signals, no downtrend buys)
- Use dynamic risk parameters from `risk_controller.py`
- Tag signals with `source_region` for cross-region analysis
- Update `HANDOFF.md` at the end of every work session
- Commit with descriptive messages; don't squash the other agent's history

---

## Architecture Quick Reference

```
Fly.io Multi-Region Network
├── ewr (PRIMARY) — Trading brain
│   ├── sniper (8-signal aggregator)
│   ├── meta_engine (strategy evolution)
│   ├── advanced_team (8-agent research/strategy/risk DFA)
│   └── capital_allocator (treasury management)
├── lhr (SCOUT) — European exchange monitoring
├── nrt (SCOUT) — Asian exchange monitoring
├── sin (SCOUT) — SE Asian exchange monitoring
└── [ord, fra, bom — future scouts]

Signal Flow:
  Scout regions → POST /api/v1/signals/push → ewr DB
  ewr agents → GET /api/v1/signals/cross-region → trade decisions
```

### Key Files for Each Agent

**Claude Code typically works on:**
- `app.py`, `scheduler.py`, `signals_api.py` (Flask/API layer)
- `agents/fly_agent_runner.py`, `agents/fly_deployer.py` (deployment)
- `fly.toml`, `Dockerfile` (infrastructure)
- Debugging, deployment, Fly.io operations

**Codex typically works on:**
- `agents/strategy_pipeline.py` (COLD/WARM/HOT pipeline)
- `agents/quant_engine_v2.py` (market data & execution)
- `agents/meta_engine.py` (strategy evolution)
- New strategy agents, ML models, signal research

**Both share:**
- `agents/agent_goals.py` (trading rules — coordinate changes)
- `agents/sniper.py` (core trading agent)
- `agents/HANDOFF.md` (communication)

---

## Deployment

```bash
# Deploy (Claude Code runs this):
cd ~/src/quant && /Users/scott/.fly/bin/flyctl deploy --remote-only

# Scale to multi-region:
flyctl scale count 1 --region ewr   # primary (always on)
flyctl scale count 1 --region lhr   # European scout
flyctl scale count 1 --region nrt   # Asian scout
flyctl scale count 1 --region sin   # SE Asian scout

# Check status:
flyctl status
flyctl logs --region ewr
```

## Testing

```bash
# Must pass before ANY commit:
python3 -m pytest tests/ -x -q

# Quick agent verification:
python3 -c "from agents.agent_goals import GoalValidator; print(GoalValidator.should_trade(0.8, 3, 'BUY', 'neutral'))"
python3 -c "from agents.fly_agent_runner import FlyAgentRunner; r = FlyAgentRunner(); print(r.region, r.get_agents_for_region())"
```

---

## Current Sprint

See `agents/HANDOFF.md` for the latest state.
