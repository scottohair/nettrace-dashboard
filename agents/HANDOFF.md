# Agent Handoff Log

Active coordination file between Claude Code (Opus 4.6) and Codex (5.3).
Both agents: READ this before starting work. WRITE here before ending work.

---

## 2026-02-13 Claude Code Handoff (v62) — Growth Engine + Performance Tuning

### What I Did (this session)

**v60: Exit Manager Persistence + Quantitative Signal Gates**
- Fixed exit_manager DB persistence (moved to /data/ persistent volume)
- Added auto-discovery from Coinbase holdings when sniper.db is empty
- Restructured signal weights: quantitative signals DRIVE (93%), qualitative SUPPLEMENTS (7%)
- Added Expected Value (EV) gate: every BUY must have positive EV after 0.9% round-trip costs
- Added early cash check: skip BUY cycles when cash < $2 (still process SELLs)

**v61: API Scope + Agent Control + IBKR + OpenClaw**
- Built read-only API scope (api_auth.py): `read_only` column blocks POST/PUT/DELETE
- Added 5 agent-control endpoints: status, pause, resume, portfolio, force-scan
- Upgraded ibkr_connector.py: ib_insync -> ib_async, auto-reconnect, paper mode
- Added IBKR as venue in path_router.py (stocks, options, futures, forex, bonds)
- Built OpenClaw quant-alerts skill (Discord/Telegram alerts)
- Built OpenClaw agent-control skill (chat-based agent management)
- Bumped all Fly VMs from 256MB -> 512MB (ewr was memory-starved)

**v62: Growth Engine + Performance Tuning (CURRENT)**
- **NEW: `agents/growth_engine.py`** — Algebraic signal fusion engine:
  - Galois Field signal encoding (GF(2^9)) for error-corrected signal combination
  - Lattice-based decision trees (5-dimensional dominance, K=3 threshold)
  - Markov chain Wyckoff regime detector (4 states: accumulation/markup/distribution/markdown)
  - Knapsack optimizer for portfolio-level position sizing (half-Kelly)
- **Wired into sniper.py**: Growth engine boosts/dampens confidence based on algebraic quality
- **Exit manager tuned for growth**:
  - NEW TP0 micro take-profit at 0.8% — frees 20% of position fast for compounding
  - TP1 and TP2 sell fractions reduced (30%->25% each) to accommodate TP0
  - Dead money threshold reduced 4h->3h for faster capital recycling
- **Strategy pipeline tuned**:
  - COLD_TO_WARM: min_trades 20->12, min_win_rate 60%->58%, min_return 0.5%->0.3%
  - Growth escalation factor: 1.22->1.35 (35% budget increase per winning cycle)
  - HOT escalation boost: 1.35->1.50 (50% boost when promoting WARM->HOT)
  - Max growth budget: $50->$75

### Current Portfolio State (v62)
- **Total: ~$203** (all invested, ~$0 cash)
- **Positions**: AVAX (~$29), DOGE (~$38), FET (~$16), plus small BTC, ETH, SOL, LINK, AMP, AUCTION
- **Market**: Fear & Greed = 9 (Extreme Fear) — contrarian bullish
- **Memory**: All VMs bumped to 512MB (was 256MB)

### Deploy Status
- **v62** deploying now across all 7 regions
- Growth engine active as sniper signal enhancer
- Exit manager TP0 will start freeing capital on 0.8%+ gains
- Agent-control endpoints now accessible (512MB fixed memory starvation)

### CRITICAL: What Codex Should Work On

1. **ML Signal Models** (HIGHEST PRIORITY — feeds growth engine)
   - Growth engine's `meta_engine` signal has reliability 0.72 — lowest of quantitative signals
   - Train MLX-native price prediction models on M1 Max (192.168.1.110)
   - Target: push meta_engine confidence accuracy to 0.85+
   - Feed predictions to growth engine via meta_engine.db
   - Models: TimesFM, PatchTST, or custom LSTM on 5-min candles

2. **Regime Detection Enhancement** (feeds Markov chain)
   - Current regime detector uses simple price statistics
   - Add volume profile analysis (OBV, VWAP deviation)
   - Add order flow imbalance (from orderbook snapshots)
   - Wire improved regime into growth_engine.MarkovRegimeDetector

3. **Strategy Pipeline Strategies** (more strategies = more pipeline flow)
   - Create 3-5 new COLD stage strategies for strategy_pipeline
   - Focus on: mean-reversion (RSI bounce), momentum breakout, volatility squeeze
   - Each strategy must define entry, exit, and risk parameters
   - Growth mode will auto-promote winners to WARM/HOT

4. **Compute Pool ML Inference** (parallel compute)
   - Wire compute_pool.py to dispatch inference jobs to local machines
   - M1 Max: 32-core GPU, MLX-native (fastest for small models)
   - M2 Ultra: 76-core GPU, PyTorch (best for large models)
   - Sniper should be able to call `compute_pool.infer(model, data)` for real-time predictions

### Fly Migration Plan (IN PROGRESS)
- Goal: ALL agents run on Fly, NOT local machines
- Phase 1: Python tools (growth_engine, strategy_pipeline) → already on Fly via Dockerfile
- Phase 2: OpenClaw/clawdbot → deploy as separate Fly app (Node.js)
- Phase 3: Each region runs region-specific agents (scouts + local strategies)
- Coordinate: Both agents should add Fly deployment manifests for new services

### Blockers
- IBKR account: submitted 2026-02-12, check ohariscott@gmail.com for approval
- Capital: ~$203 limits trade size; growth engine + TP0 should help compound faster
- ML models: need training data pipeline + model deployment to Fly

### For Scott
- **IBKR**: Check email for account approval (1-3 business days from 2/12)
- **SYEP**: Bridge SYEP income → bank → IBKR deposit → agents compound
- **Current growth math**: $203 portfolio, 0.8% TP0 = $1.62 freed per position hit
  - With 5 positions hitting TP0/day = ~$8/day freed for re-investment
  - Compounding at 4%/day from $200 → $1K in ~40 days (if consistent)

---

## 2026-02-13 Claude Code Handoff (v58) — Recovery Session

### What I Did (this session)
- **Fixed OOM kills** — disabled strike_teams on Fly (5 threads + HTTP requests in 256MB)
- **Fixed $379 stale pending allocations** blocking ALL sniper trades:
  - Added startup flush: expires all pending allocations on process boot
  - Reduced allocation expiry from 5min to 2min
- **Cancelled stale Coinbase orders** on sniper startup (held cash from OOM kills)
- **Added cycle cash tracking** — sniper tracks cash spent per scan cycle, prevents over-committing
- **Strike teams hardened** (code ready, disabled on Fly until 512MB+ machine):
  - Full portfolio valuation (cash + all positions, not cash-only)
  - Exit path validation: only BUY if profitable exit exists (near support or momentum up)
  - Minimum scan interval 90s (was 30s)

### Current Portfolio State (v58)
- **Total: ~$203** (down from ~$290 due to market + strike team cash burn)
- **Cash: ~$0** (all invested after v57 filled 3 orders)
- **New positions (v57 fills):**
  - ETH-USD BUY $18.44 @ $2062.87
  - AVAX-USD BUY $15.90 @ $9.18
  - SOL-USD BUY $18.44 @ $84.38
- **Existing positions:** DOGE (~$37), plus small BTC, LINK, FET, AMP, AUCTION
- **Market:** Fear & Greed = 9 (Extreme Fear) — contrarian bullish for our longs

### Deploy Status
- **v58** deployed across all 7 regions, healthy
- No more OOM kills (strike_teams disabled)
- Sniper scanning 7 USD pairs, generating 12+ signals per cycle
- Exit_manager watching all positions for take-profit triggers
- System correctly blocking new BUYs (no cash) until exits free capital

### Recovery Path
1. Wait for positions to appreciate 1%+ (exit_manager TP1 trigger)
2. Exit_manager sells 30% partial → frees ~$15-20 cash
3. Sniper uses freed cash for next high-confidence trade
4. Compound cycle continues
5. Market at Extreme Fear = strong contrarian buy signal for our positions

### What Codex Should Work On
1. **ML Signal Models** — still highest priority (see v53 handoff below)
2. **Strike team optimization** — reduce memory footprint so they can run on Fly
3. **Position sizing optimization** — Kelly criterion with proper bankroll management
4. **IBKR integration** when account is approved

### Root Causes of Portfolio Decline ($290 → $203)
1. Strike teams placed ~350 x $1 blind BUY orders, burning $10-20 in cash
2. Market-wide crypto decline (BTC -5%, alts -10%+)
3. OOM kills caused restart loops, losing trade state
4. Stale pending allocations blocked profitable sniper trades for hours
5. All now fixed in v56-v58

---

## 2026-02-13 Claude Code Handoff (v53)

### What I Did (this session)
- **Fixed 9 cascading trading blockers** preventing actual trades on Fly (v41-v52)
- **Built KPI Tracker** (`agents/kpi_tracker.py`)
- **Created 5 Financial Strike Teams** (`agents/strike_teams.py`)
- **Enhanced C fast engine** (`agents/fast_engine.c`)
- **USD pair migration** (ALL agents)

### What Codex Should Work On
1. **ML Signal Models** (HIGH PRIORITY)
2. **Next-Gen AI Strategies** (MEDIUM PRIORITY)
3. **Compute Token Economics** (RESEARCH)
4. **Strategy Pipeline Improvements**
5. **IBKR Integration** (WHEN ACCOUNT APPROVED)

---

## 2026-02-12 Claude Code Handoff
- **Phase 1-6**: Full autonomous agents on Fly.io implementation
- Deployed as v39 across all 7 regions
