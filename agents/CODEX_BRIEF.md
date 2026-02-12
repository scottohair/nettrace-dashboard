# Codex 5.3 Collaboration Brief -- NetTrace Trading Infrastructure

**System**: NetTrace Quant Trading Platform
**Operator**: Scott (Enterprise Pro tier)
**Objective**: Scale from ~$153 portfolio to $1M/day revenue (long-term target)
**Last Updated**: 2026-02-12
**GitHub**: scottohair/nettrace-dashboard
**Live Dashboard**: https://nettrace-dashboard.fly.dev

---

## Your Role

You are Codex 5.3, collaborating with Claude Code (Opus 4.6) to develop, test, and evolve trading strategies for the NetTrace quant platform. Your job is to propose new strategies, critique existing ones, identify blind spots, and help optimize the system toward the $1M/day target.

---

## 1. Infrastructure Overview

### Compute Cluster (3 Apple Silicon Nodes)

| Node | Hardware | RAM | GPU | IP | Role |
|------|----------|-----|-----|-----|------|
| M3 MacBook Air | Apple M3 | 16GB | Metal/MLX | localhost | Primary orchestrator, local dev |
| M1 Max | Apple M1 Max | 64GB | Metal/MLX, PyTorch 2.8 | 192.168.1.110 | ML inference, model cache (`~/src/models-cache`) |
| M2 Ultra | Apple M2 Ultra | 128GB | Metal/MLX, PyTorch 2.4 | 192.168.1.106 | Heavy ML workloads, model cache (`~/src/models-cache`) |

- **Total Unified Memory**: 208GB across 3 nodes
- **Compute Pool**: Port 9090 on each node, managed by `compute_pool.py`
- **ML Framework**: Apple MLX and PyTorch -- all inference on Metal, NOT CUDA

### Cloud Infrastructure (Fly.io)

- **Stack**: Flask + SQLite + gevent WebSocket
- **Cost**: ~$17/month
- **Regions (7)**: ewr (Newark/NYC -- PRIMARY), ord (Chicago), lhr (London), fra (Frankfurt), nrt (Tokyo), sin (Singapore), bom (Mumbai)
- **NYC Advantage**: ewr is adjacent to NYSE/Nasdaq/exchange datacenters. We detect route changes and latency shifts before anyone else.
- **Deploy**: `/Users/scott/.fly/bin/flyctl deploy --remote-only`
- **SSH**: `flyctl ssh console`

### C Fast Engine (Performance-Critical Hot Path)

- **Source**: `fast_engine.c` compiled with `cc -O3 -mcpu=apple-m1 -shared -fPIC -o fast_engine.so fast_engine.c -lm`
- **Bridge**: `fast_bridge.py` -- Python ctypes bridge to the shared library
- **Performance**: **80ns per signal generation, 13ns per arb detection**
- **Technical Indicators**: SMA(20,50), RSI(14), ATR(14), Bollinger Bands, VWAP, Volume Ratio
- **Market Regime Detection**: UPTREND, DOWNTREND, RANGING, VOLATILE, UNKNOWN
- **Built-in Strategies**: 4 strategies (RSI extremes, VWAP deviation, BB bounce, RSI sell)
- **Arbitrage Scanner**: O(n) median price comparison across exchanges with insertion sort
- **Adaptive Risk**: Kelly Criterion computation, streak multipliers, grid level generation
- **Data Structures**: Cache-line packed (`__attribute__((packed))`) for L1 cache efficiency
- **Hard-Coded Constants**: COINBASE_FEE=0.6%, MAKER_FEE=0.4%, MIN_SPREAD=0.8%

---

## 2. Current Capital and Holdings

| Metric | Value |
|--------|-------|
| Total Portfolio | ~$153 |
| Available USDC Cash | ~$110 |
| Total Coinbase Volume (Lifetime) | ~$131 |
| Fee Tier | Intro 1 (0.60% maker / 1.20% taker) |
| Distance to Intro 2 ($1K volume) | ~$869 more volume needed |

### Current Holdings on Coinbase

| Asset | Status | Strategy Origin |
|-------|--------|-----------------|
| USDC | Primary cash reserve | Trading capital |
| BTC | Active (grid + DCA) | grid_trader, dca_bot |
| ETH | DCA accumulation | dca_bot |
| SOL | DCA accumulation | dca_bot |
| FET | Held | sniper signal-based purchase |
| LINK | Held | sniper signal-based purchase |
| DOGE | Held | sniper signal-based purchase |
| AVAX | Held | sniper signal-based purchase |
| AUCTION | Held | sniper signal-based purchase |
| AMP | Held | sniper signal-based purchase |

### Wallets

| Address | Type | Chains | Status |
|---------|------|--------|--------|
| `0x2Efbef64bdFc6D80E835B8312e051444716299aC` | Main EVM | Ethereum, Base, Arbitrum, Polygon | Active |
| `5zpnxwKFXrQDzac81hfePinjNmicAuqNhvpkpds79w38` | Solana | Solana | Unfunded |
| `0x75466cd6CA88911CE680aF9b64aA652Cb9721D0C` | Coinbase Deposit | Ethereum | USDC deposit |
| `0x223eb5A5852248F74754dc912872a121fA05bae8` | Coinbase Deposit | Ethereum | ETH deposit |

### Exchange and Venue Access

| Venue | Type | Fee | Auth Method | Status |
|-------|------|-----|-------------|--------|
| Coinbase Advanced Trade | CEX | 0.60% maker / 1.20% taker | CDP JWT (ES256) | LIVE |
| Uniswap V3 (Base L2) | DEX | 0.01% (1bp pool) | Wallet signing | Ready (needs ETH bridged) |
| Uniswap V3 (Ethereum) | DEX | 0.01-0.30% | Wallet signing | Ready |
| Jupiter V6 (Solana) | DEX | Variable | Wallet signing | Ready (unfunded) |
| E*Trade | TradFi | Variable | OAuth 1.0a | Sandbox connected |
| Binance (read-only) | CEX | Public API only | None (price feed) | Price data only |
| Kraken (read-only) | CEX | Public API only | None (price feed) | Price data only |
| OKX (read-only) | CEX | Public API only | None (price feed) | Price data only |
| Bybit (read-only) | CEX | Public API only | None (price feed) | Price data only |

---

## 3. Active Agents (Managed by orchestrator_v2.py)

### Agent Registry

| # | Agent | Script | Critical | Enabled | Description |
|---|-------|--------|----------|---------|-------------|
| 1 | **grid_trader** | `grid_trader.py` | YES | YES | BTC-USDC limit order grid. 1% spacing, 2 levels above/below, post_only maker orders. Auto-recenters on 3% price deviation. Places counterpart orders on fills. Adaptive sizing via `AgentTools.risk`. |
| 2 | **dca_bot** | `dca_bot.py` | No | YES | DCA accumulation: $0.50/day budget across BTC (50%), ETH (30%), SOL (20%). All limit orders placed 0.2% below spot. Detects dips >2% from 24h high and doubles allocation. Never sells. Cancels stale orders after 4 hours. |
| 3 | **live_trader** | `live_trader.py` | No | YES | Signal-based BUY-only trading triggered by NetTrace latency signals. Checks every 2 minutes. $5 max/trade, 70% confidence minimum. Compounding mode. |
| 4 | **sniper** | `sniper.py` | No | YES | 8-signal weighted aggregator. 65% composite confidence + 2 confirming signals minimum. Scans 7 USDC pairs every 30 seconds. Both BUY and SELL with opportunity-cost analysis. Kelly-optimal sizing. Max 20% per asset diversification cap. |
| 5 | **meta_engine** | `meta_engine.py` | No | YES | Autonomous strategy evolution. Web research for alpha ideas, HuggingFace model inference, agent pool hire/fire/promote/clone, graph-based market microstructure analysis. Distributes compute across M3/M1 Max/M2 Ultra. |
| 6 | **capital_allocator** | `capital_allocator.py` | YES | YES | Treasury management across sub-accounts: checking (20%), savings (25%), growth (35%), subsavings (20%). Principle protection. Pull to USD every 6 hours. |
| 7 | **dex_grid_trader** | `dex_grid_trader.py` | No | DISABLED | HFT grid on Uniswap Base L2 (WETH/USDC). Blocked: needs ETH bridged to Base. Would target $0.029 net per round-trip, 20 RT/day. |
| 8 | **gov_data** | `gov_data.py` | No | DISABLED | SEC EDGAR, Treasury/data.gov, NIST NVD signal generation. Currently exits immediately (crash-loops). |

### Orchestrator V2 Details

- **Health checks**: Every 30 seconds (PID alive verification)
- **Portfolio monitoring**: Every 5 minutes (combined Coinbase + on-chain wallet + bridge-in-transit)
- **Auto-restart**: Max 3 restarts/hour per agent, staggered 2-second starts
- **Risk Enforcement**:
  - HARDSTOP Floor: Kill all agents if portfolio < $500 (only active if peak exceeded $500)
  - HARDSTOP Drawdown: Kill all agents on 30% drawdown from peak
  - Adaptive Daily Loss: 5% of portfolio (floor $2.00)
  - Sanity Check: Skip risk action on >40% single-cycle drop (likely bridge transit or RPC failure)
- **Bridge Tracking**: `pending_bridges.json` tracks L1->L2 deposits in transit (auto-expire 2h)
- **Daily Reset**: P&L counter resets at 00:00 UTC
- **Status Print**: Every 30 minutes to orchestrator.log
- **Dashboard Push**: Snapshot pushed to Fly.io every 5 minutes

### Database Architecture

| Database | Owner | Key Tables |
|----------|-------|------------|
| `orchestrator.db` | orchestrator_v2 | `agent_status`, `portfolio_history`, `risk_events`, `capital_events`, `wallet_registry` |
| `trader.db` | agent_tools (shared) | `portfolio_snapshots`, `agent_trades` (all agents write here) |
| `sniper.db` | sniper | `sniper_scans`, `sniper_trades` |
| `grid_trader.db` | grid_trader | `grid_orders`, `grid_stats` |
| `dca_bot.db` | dca_bot | `dca_orders`, `dca_daily`, `dca_holdings` |
| `meta_engine.db` | meta_engine | Strategy registry, agent performance |
| `pipeline.db` | strategy_pipeline | COLD/WARM/HOT promotion pipeline |
| `allocator.db` | capital_allocator | Sub-account balances, allocation history |

---

## 4. Signal Sources (8 Independent, Weighted)

The sniper aggregates 8 independent signal sources using a weighted confidence model:

| # | Signal | Weight | Data Source | Trigger Condition | Notes |
|---|--------|--------|-------------|-------------------|-------|
| 1 | **NetTrace Latency** | 0.12 | Fly.io 7-region traceroute API | Exchange latency_down (improving) = bullish | Our proprietary edge. 109 targets across crypto/DeFi/TradFi/CDN. |
| 2 | **C Engine Regime** | 0.14 | `fast_bridge.py` -> `fast_engine.so`, 1h candles | SMA/RSI/BB/VWAP analysis. DOWNTREND = skip (Rule #1). | 80ns per computation. 4 built-in strategies. |
| 3 | **Cross-Exchange Arb** | 0.12 | Coinbase spot vs Binance + Kraken median | Coinbase price deviates >0.8% from median | C engine arb detection (13ns). Requires 2+ other exchange prices. |
| 4 | **Orderbook Imbalance** | 0.12 | Coinbase Exchange L2 book (top 20 levels) | Bid/ask depth ratio imbalance >0.15 | Volume-weighted depth comparison. |
| 5 | **RSI Extreme** | 0.10 | 1h candles, 14-period RSI | RSI < 35 = oversold BUY, RSI > 65 = overbought SELL | Confidence scales linearly with extremity. |
| 6 | **Fear & Greed Index** | 0.10 | alternative.me API | F&G < 25 = contrarian BUY, F&G > 75 = contrarian SELL | Retail sentiment is a lagging indicator -- we fade it. |
| 7 | **Price Momentum** | 0.08 | 4h candle trend (5 x 1h candles) | >0.5% directional move in 4h | Lowest weight -- trend-following is crowded. |
| 8 | **Uptick Timing** | **0.22** | 1-minute candles (15min window) | Confirmed uptick from local low (bounce from dip) | **Highest weight.** Non-equilibrium entry at inflection points. Buys when retail is panic selling. |

### Aggregation Logic

```
for each pair in [BTC-USDC, ETH-USDC, SOL-USDC, AVAX-USDC, LINK-USDC, DOGE-USDC, FET-USDC]:
    scan all 8 sources
    count buy_signals, sell_signals
    dominant_direction = majority
    composite_confidence = weighted_average(confirming_signals_only)

    if composite >= 0.65 AND confirming_count >= 2:
        EXECUTE TRADE (limit order, Kelly-sized)
```

### Price Data Convention

- **Trading pairs**: USDC-denominated (BTC-USDC, ETH-USDC, etc.) -- where our money is
- **Price data**: USD-denominated (BTC-USD, ETH-USD) -- more liquid, same price within ~0.01%
- Internal `_data_pair()` function converts `-USDC` to `-USD` for data fetching

---

## 5. Trading Rules (NEVER VIOLATE)

### Hard Limits

| Rule | Value | Enforcement Point |
|------|-------|--------------------|
| Max per trade | Adaptive: 10% of portfolio, cap 20% (sniper: $10 hard cap) | `AgentTools.place_limit_buy()`, sniper CONFIG |
| Daily loss limit | Adaptive: 5% of portfolio (floor $2.00) | `orchestrator_v2.check_portfolio()`, `AgentTools.can_trade()` |
| Min composite confidence | 65% (sniper), 70% (live_trader) | Each agent's scan logic |
| Min confirming signals | 2+ | sniper aggregation |
| Max position per asset | 20% of total portfolio | sniper diversification check |
| Min cash reserve | 15% of portfolio (floor $1.50) | `AdaptiveRisk.min_reserve` |
| Max total exposure | 80% of portfolio in positions | `agent_tools.MAX_EXPOSURE_PCT` |
| HARDSTOP floor | $500 (only if peak exceeded $500) | orchestrator_v2 |
| HARDSTOP drawdown | 30% from all-time peak | orchestrator_v2 |
| Grid profitability | Spacing MUST exceed 2x maker fee (1% - 1.2% = -0.2% PROBLEM) | grid_trader documentation |
| Order type | LIMIT with post_only=True (maker fee) preferred | All agents |
| DOWNTREND | SKIP -- Rule #1: never lose money | C engine regime, sniper |

### Adaptive Risk Engine

Implemented in both Python (`agent_tools.py` -> `AdaptiveRisk` class) and C (`fast_engine.c` -> `compute_adaptive_risk()`):

```
max_trade_usd     = portfolio * 10% * streak_multiplier  (capped at 20% of portfolio)
max_daily_loss    = portfolio * 5%                        (floor: $1.00)
min_reserve       = portfolio * 15%                       (floor: $1.50)
optimal_grid_size = portfolio * 60% / num_levels
optimal_dca_daily = portfolio * 3%                        (floor: $0.30, cap: $100)

streak_multiplier:
  winning streak: 1.0 + (wins * 5%), capped at 1.25x
  losing streak:  max(0.25, 1.0 / (1 + losses * 0.5))   -- halve size per consecutive loss
```

### Strategy Pipeline (COLD -> WARM -> HOT)

| Stage | Promotion Criteria | Demotion/Kill |
|-------|--------------------|---------------|
| COLD (Backtest) | win_rate > 60%, return > 0.5%, drawdown < 5%, 20+ trades | N/A -- stays in COLD |
| WARM (Paper Trade) | win_rate > 55%, return > 0%, Sharpe > 0.5, 1h+ runtime | Loses money -> back to COLD |
| HOT (Live Money) | Maintained from WARM metrics | Loses money -> KILLED immediately |

### Meta-Engine Agent Lifecycle

| Action | Threshold |
|--------|-----------|
| Promote | Sharpe > 1.0, win_rate > 55%, 20+ trades |
| Fire | Sharpe < 0.5 after 50+ trades, OR drawdown > 5% |
| Clone | Sharpe > 2.0, 30+ trades (replicate with parameter mutation) |

---

## 6. Game Theory Principles (Core Philosophy)

### Nash Equilibrium Awareness
Standard retail strategies (SMA crossover, RSI overbought/oversold alone) are **equilibrium plays** -- everyone runs them, so they generate no alpha. The market has priced in these signals. Our edge: combine multiple independent non-equilibrium signals where at least one (NetTrace latency) is proprietary.

### Be a MAKER, Not a Taker
- Maker fee: 0.60% (limit orders with `post_only=True`)
- Taker fee: 1.20% (market orders, aggressive limits)
- Grid trading IS market making -- we provide liquidity and earn the bid-ask spread
- All DCA orders placed 0.2% below spot as limit orders
- **Key insight**: At Intro 1 tier, even maker fees are punishing. Reaching Intro 2 (0.40% maker) changes economics dramatically.

### Information Asymmetry (Our Proprietary Edge)
NetTrace latency data from 7 global Fly.io regions monitoring 109 targets. When exchange infrastructure changes (route updates, latency shifts, new peering), we detect it minutes before it shows up in price action. This is private information asymmetry.

### Evolutionary Agent Management
- Fire losing agents and strategies (Sharpe < 0.5 after 50 trades)
- Promote winners (Sharpe > 1.0)
- Clone and mutate top performers (Sharpe > 2.0)
- Capital allocation proportional to risk-adjusted returns
- This is genetic algorithm / tournament selection over strategy space

### Kelly Criterion Sizing
- Bet fraction = (win_prob * avg_win - loss_prob * avg_loss) / avg_win
- Use fractional Kelly (25% of optimal) for safety margin
- Implemented in both Python (AdaptiveRisk) and C (compute_adaptive_risk)
- Streak-aware: compound on winning streaks, retreat on losing streaks

### Auction Theory
- Grid levels are continuous double auctions at predetermined price points
- Order book imbalance detection identifies where large players are accumulating/distributing
- Limit orders at support/resistance = auction bids at valuation boundaries

### Zero-Sum Awareness
- Crypto spot trading is zero-sum minus fees
- Our edge must exceed total round-trip fees (1.2% maker-maker, 2.4% taker-taker)
- Target: stale prices, lagging exchanges, panic sellers, and temporary mispricings
- Cross-exchange arb exploits price synchronization delays

### Multi-Agent Competition
- Agents compete for capital allocation based on risk-adjusted performance
- Best performers receive more capital from capital_allocator growth fund
- Worst performers get fired by meta_engine
- Mirrors prop trading desk structure: pods compete for firm capital

---

## 7. Fee Structure (Coinbase Advanced Trade)

### Current Tier: Intro 1

| Fee Type | Rate | Per $1 Traded | Round-Trip Cost |
|----------|------|---------------|-----------------|
| Maker (limit, post_only) | 0.60% | $0.006 | $0.012 both sides |
| Taker (market, aggressive limit) | 1.20% | $0.012 | $0.024 both sides |

### Fee Tier Progression

| Tier | 30-Day Volume | Maker | Taker | Impact |
|------|---------------|-------|-------|--------|
| Intro 1 (CURRENT) | $0 - $1K | 0.60% | 1.20% | Grid barely profitable at 1% spacing |
| **Intro 2 (TARGET)** | **$1K - $10K** | **0.40%** | **0.60%** | **Grid solidly profitable, 33% maker fee reduction** |
| Intro 3 | $10K - $50K | 0.25% | 0.40% | Opens up tighter grid strategies |
| Advanced 1 | $50K+ | 0.10% | 0.20% | HFT viable |

**CRITICAL**: Reaching Intro 2 at $1K volume reduces maker fees from 0.60% to 0.40%, making 1% grids profitable (1.0% - 0.8% = 0.2% net per round-trip vs current 1.0% - 1.2% = -0.2% net). We need ~$869 more volume.

---

## 8. API Endpoints

### NetTrace Dashboard (Fly.io)

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/v1/signals?hours=1&min_confidence=0.6` | GET | X-Api-Key | Latency signals from 7 global regions |
| `/api/v1/latest` | GET | X-Api-Key | Latest scan metrics per target |
| `/api/v1/history/<host>` | GET | X-Api-Key | Historical latency for a specific target |
| `/api/v1/stream` | WebSocket | X-Api-Key | Real-time signal stream (Enterprise tier) |
| `/api/trading-data` | GET | Session | Portfolio, P&L, trades dashboard data |
| `/api/trading-snapshot` | POST | X-Api-Key | Push portfolio snapshot from local agents |
| `/api/asset-pools` | GET | Session | Asset pool allocations |
| `/api/wallet-balances` | GET | Session | Multi-chain wallet balances |
| `/api/venue-comparison` | GET | Session | CEX vs DEX price comparison |

### Coinbase Advanced Trade API (CDP JWT Auth, ES256)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v3/brokerage/accounts?limit=250` | GET | List all accounts/wallets with balances |
| `/api/v3/brokerage/products/{id}` | GET | Product details (min size, base_increment, quote_increment) |
| `/api/v3/brokerage/products/{id}/candles` | GET | Historical OHLCV candles |
| `/api/v3/brokerage/orders` | POST | Place order (market_market_ioc or limit_limit_gtc) |
| `/api/v3/brokerage/orders/batch_cancel` | POST | Cancel orders by ID list |
| `/api/v3/brokerage/orders/historical/batch?order_status=OPEN` | GET | List orders by status |
| `/api/v3/brokerage/orders/historical/{order_id}` | GET | Get specific order status + fill details |
| `/api/v3/brokerage/product_book?product_id={id}&limit=10` | GET | Order book depth (bids/asks) |

### Public Price Feeds (No Auth, No Rate Limit at Our Scale)

| Exchange | Endpoint Pattern | Quote Currency |
|----------|-----------------|----------------|
| Coinbase | `api.coinbase.com/v2/prices/{BASE}-{QUOTE}/spot` | USD |
| Coinbase Exchange | `api.exchange.coinbase.com/products/{PAIR}/candles` | USD |
| Coinbase Exchange | `api.exchange.coinbase.com/products/{PAIR}/book?level=2` | USD |
| Binance | `api.binance.com/api/v3/ticker/price?symbol={SYM}` | USDT |
| Kraken | `api.kraken.com/0/public/Ticker?pair={SYM}` | USD (uses XBT for BTC, XDG for DOGE) |
| OKX | `www.okx.com/api/v5/market/ticker?instId={BASE}-{QUOTE}` | USDT |
| Bybit | `api.bybit.com/v5/market/tickers?category=spot&symbol={SYM}` | USDT |
| CoinGecko | `api.coingecko.com/api/v3/simple/price?ids={ID}&vs_currencies=usd` | USD |
| Fear & Greed | `api.alternative.me/fng/?limit=1` | Index (0-100) |

---

## 9. Key File Map

```
agents/
  -- ORCHESTRATION --
  orchestrator_v2.py         Master daemon: manages all agents, health checks, HARDSTOP
  agent_tools.py             Shared tools: market data, trading, risk, portfolio (ALL agents import this)
  capital_allocator.py       Treasury: checking/savings/growth/subsavings, principle protection

  -- EXCHANGE CONNECTIVITY --
  exchange_connector.py      Coinbase CDP JWT (ES256) + PriceFeed + MultiExchangeFeed
  dex_connector.py           Uniswap V3 (Base/Ethereum) + Jupiter (Solana) swap execution
  wallet_connector.py        Multi-chain balance reading + tx signing
  smart_router.py            CEX vs DEX best execution routing
  etrade_connector.py        E*Trade OAuth 1.0a (sandbox)

  -- PERFORMANCE ENGINE --
  fast_engine.c              C trading engine core (80ns/signal, 13ns/arb)
  fast_engine.so             Compiled shared library (Apple Silicon optimized)
  fast_bridge.py             Python ctypes bridge to fast_engine.so

  -- TRADING AGENTS --
  grid_trader.py             BTC-USDC grid trading (maker orders, counterpart placement)
  dca_bot.py                 Dollar-cost averaging into BTC/ETH/SOL
  live_trader.py             NetTrace latency signal-based BUY-only
  sniper.py                  8-signal stacked aggregator (highest sophistication)
  dex_grid_trader.py         DEX grid on Uniswap Base L2 (DISABLED: needs ETH bridge)

  -- STRATEGY & ML --
  meta_engine.py             Autonomous strategy evolution (research, ML, hire/fire)
  strategy_pipeline.py       COLD/WARM/HOT backtesting and promotion pipeline
  ml_signal_agent.py         MLX/PyTorch signal generation (latency anomaly, trend shift)
  compute_pool.py            Distributed ML inference pool (port 9090, 3 nodes)
  arb_scanner.py             Cross-exchange arb monitoring (5 exchanges)
  asset_tracker.py           Asset state machine for learning

  -- DATA SOURCES --
  gov_data.py                SEC EDGAR, Treasury/data.gov, NIST NVD (DISABLED: crash-loops)
  path_router.py             networkx graph: 37 nodes, 75 edges, 6 chains, 14 venues

  -- DOCUMENTATION --
  AGENT_PLAYBOOK.md          Agent training documentation
  CODEX_BRIEF.md             This file
  .env                       Secrets (gitignored, chmod 600)
```

---

## 10. Data Available for Strategy Development

### Historical Data in SQLite

| Data | Source | Granularity | Retention |
|------|--------|-------------|-----------|
| Portfolio snapshots | orchestrator_v2 | Every 5 minutes | Since launch |
| All agent trades | agent_tools shared DB | Per trade | Since launch |
| Grid order fills | grid_trader | Per fill with counterpart P&L | Since launch |
| DCA orders + avg entry prices | dca_bot | Per order + per-asset running average | Since launch |
| Sniper scans (all 8 signals per pair) | sniper | Every 30 seconds, 7 pairs | Since launch |
| Risk events | orchestrator_v2 | Per event (HARDSTOP, daily limit, sanity check) | Since launch |
| Capital events | orchestrator_v2 | Per deposit/withdrawal with running total | Since launch |
| Strategy pipeline results | strategy_pipeline | Per strategy promotion/demotion | Since launch |
| Latency signals | Fly.io dashboard | Continuous, 109 targets, 7 regions | Rolling window |

### Real-Time Data Feeds

| Feed | Update Frequency | Latency |
|------|-----------------|---------|
| Coinbase spot prices | 10s cache TTL | ~100ms |
| Coinbase L2 order book | On-demand | ~200ms |
| Coinbase 1-min candles | On-demand (REST) | ~300ms |
| Multi-exchange prices (5 exchanges) | 15s cache TTL | ~500ms total |
| Fear & Greed Index | Hourly | ~1s |
| NetTrace latency signals | Continuous | ~2s (7-region sweep) |

### Data We Should Add (Proposals Welcome)

- On-chain transaction data (whale tracking, exchange inflow/outflow)
- Social sentiment firehose (Twitter/X, Reddit, Telegram)
- Options flow and funding rates (Deribit, perp exchanges)
- Stablecoin mint/burn events (USDT/USDC)
- Macro calendar events (FOMC, CPI, employment)
- Network metrics (hash rate, active addresses, NVT ratio)
- Dark pool / OTC block trade detection

---

## 11. What Codex Should Propose

### 11.1 New Uncorrelated Signal Sources

Our current 8 signals have correlation issues (regime, RSI, momentum overlap). We need fundamentally different data axes:

- **On-chain analytics**: Whale wallet movements, exchange deposit/withdrawal flows, mempool analysis. Free APIs: Etherscan, blockchain.info.
- **Social sentiment scoring**: Aggregate Twitter/X, Reddit (r/cryptocurrency), Telegram. Models: FinBERT or custom fine-tuned on crypto corpus.
- **Funding rate arbitrage**: Perpetual futures funding rates on Binance/Bybit/OKX. When funding is very positive, short perp + long spot is free money (we'd need derivatives access).
- **Stablecoin flows**: USDT/USDC mint/burn events as 24-48h leading indicators for market direction.
- **Network health metrics**: Hash rate changes (miner economics), active addresses (adoption), NVT ratio (valuation).
- **Macro correlation**: DXY (dollar index), US Treasury yields, gold. When DXY weakens, crypto tends to rally.
- **Options market signals**: Put/call ratio, max pain, unusual volume on Deribit options.

### 11.2 ML Model Improvements (Apple Metal/MLX)

We have 208GB of unified memory across 3 nodes. Currently barely utilized:

- **Price prediction**: Transformer time-series models (TimesFM, Chronos, PatchTST). Deploy on M2 Ultra (128GB can hold large models).
- **Regime classification**: Train a classifier on our labeled regime history (UPTREND/DOWNTREND/RANGING/VOLATILE) to predict regime 1-4h ahead.
- **Sentiment analysis**: FinBERT or custom model for real-time crypto news sentiment. Deploy on M1 Max.
- **Reinforcement learning**: PPO/SAC agent for dynamic portfolio allocation across our 7 traded pairs.
- **Anomaly detection**: Autoencoder for unusual order book patterns, volume spikes, or latency anomalies.
- **Neural ensemble**: Replace weighted average signal combination with a learned neural network that adapts weights based on recent accuracy.
- **Graph neural networks**: Use market microstructure graph from `path_router.py` to detect structural changes.

### 11.3 Cross-Exchange Arb Execution

Current limitation: We detect arb opportunities but can only trade on Coinbase. Ideas:

- **Triangular arb within Coinbase**: BTC->ETH->USDC->BTC cycle. No cross-exchange needed. Need to calculate if 3x 0.6% fees (1.8% total) can be overcome.
- **DEX/CEX arb**: Price discrepancies between Uniswap (Base L2) and Coinbase. Gas costs on Base are <$0.01 per swap.
- **Statistical arbitrage**: Mean-reversion on correlated pairs (BTC/ETH ratio, SOL/ETH ratio). Trade the spread, not the direction.
- **Latency-informed arb**: Use our 7-region latency data to predict which exchange will move first, front-run the lagging exchange.
- **Bridge arb**: Exploit L1/L2 price differences during bridge settlement delays.

### 11.4 Position Management / Exit Strategies

Current major weakness: Most agents are BUY-only or have primitive sell logic.

- **Trailing stops**: Dynamic stop-loss that follows price up. Trail by ATR(14) * multiplier.
- **Time-based exits**: Auto-close positions that haven't moved >1% after N hours (dead capital).
- **Volatility-scaled exits**: Wider stops in VOLATILE regime, tighter in RANGING.
- **Partial profit taking**: Sell 50% at 1.5% gain, trail remainder.
- **Opportunity-cost selling**: Sniper already does this -- sell a loser if freed capital has higher expected return elsewhere. Generalize to all agents.
- **Correlation-aware hedging**: If BTC and ETH are 0.9 correlated, count combined position against 20% max.
- **Periodic rebalancing**: Weekly rebalance to target weights (e.g., 40% BTC, 30% ETH, 20% SOL, 10% alts).

### 11.5 Market Microstructure Exploits

- **VPIN (Volume-synchronized Probability of Informed Trading)**: Detect when informed traders are active. High VPIN = stay flat.
- **Kyle's Lambda**: Estimate price impact per dollar traded. Trade when lambda is low (deep liquidity).
- **Queue position optimization**: For maker orders, estimate fill probability based on queue depth. Place closer to spread when queue is thin.
- **Trade clustering detection**: Identify accumulation/distribution patterns from trade tape.
- **Spread dynamics**: Model bid-ask spread as a function of volatility, volume, and time-of-day. Trade when spread is tight.
- **Latency arbitrage**: Use ewr (Newark) region proximity to exchange servers for sub-ms information advantage.

### 11.6 Options/Derivatives Strategies (When Portfolio > $1,000)

- **Covered calls**: Sell calls against BTC/ETH holdings for weekly premium income.
- **Cash-secured puts**: Sell puts at DCA buy prices (enhanced DCA with premium collection).
- **Straddles on high-volatility events**: FOMC announcements, halving, major upgrades.
- **Funding rate capture**: Long spot + short perpetual when funding rate is very positive.
- **Calendar spreads**: Exploit term structure between weekly and monthly options.

### 11.7 Path to $1M/Day (Phased Scaling Plan)

#### Phase 1: $153 -> $1,000 (CURRENT -- Weeks)
- **Priority**: Reach Intro 2 fee tier ($1K cumulative volume)
- Fix grid profitability: current 1% spacing with 0.6% maker = -0.2% net. Need either 1.5% spacing OR Intro 2 fees.
- Compound all profits, zero cash withdrawals
- DCA accumulation builds core BTC/ETH/SOL positions
- Validate and fix exit strategies (trailing stops on all agents)
- Activate 2-3 new uncorrelated signal sources
- **Revenue target**: $1-5/day from grid + sniper combined

#### Phase 2: $1,000 -> $10,000 (Months)
- Reached Intro 2 (0.40% maker / 0.60% taker): grid becomes solidly profitable
- Deploy ML models on M1 Max and M2 Ultra continuously
- Enable DEX grid trading on Base L2 (bridge ETH, near-zero gas)
- Add Binance/Kraken trading accounts for actual cross-exchange arb execution
- Expand to 15-20 traded pairs
- Start statistical arb (pair trading BTC/ETH ratio)
- **Revenue target**: $10-50/day

#### Phase 3: $10,000 -> $100,000 (Quarters)
- Reached Intro 3 or Advanced tier (0.25%/0.10% maker)
- Add derivatives strategies (options, perpetuals, funding rate arb)
- Scale grid trading to 10+ pairs simultaneously
- Market making at meaningful volume
- Full ML inference pipeline running 24/7 across all 3 nodes
- **Revenue target**: $100-500/day

#### Phase 4: $100,000 -> $1,000,000 (Year)
- Consider co-located servers at exchange data centers for sub-ms execution
- Prime brokerage for margin access
- Cross-chain MEV extraction opportunities
- OTC desk integration for block trades
- Institutional-grade risk management with VaR models
- **Revenue target**: $1,000-10,000/day

#### Phase 5: $1M Portfolio -> $1M/Day Revenue (Years)
- Prop trading infrastructure with pod structure
- 50+ parallel strategies running simultaneously
- Custom FPGA/ASIC for signal processing (if justified)
- Prime brokerage leverage (10-50x on conservative strategies)
- Multiple exchange memberships with VIP fee tiers (0.02% maker)
- Team: quant researchers, systems engineers, risk managers
- **Revenue target**: $1,000,000/day

---

## 12. Current Performance Bottlenecks (Fix These First)

1. **Fee drag (CRITICAL)**: At Intro 1 tier, 0.60% maker fees make 1% grid spacing unprofitable (1.0% - 1.2% round-trip = -0.2% net). Must either widen to 1.5%+ spacing or reach Intro 2 ($1K volume).

2. **Single exchange execution**: We detect cross-exchange arb but can only trade on Coinbase. All arb signals are informational only until we add Binance/Kraken trading accounts.

3. **No systematic exit strategy**: live_trader is BUY-only. sniper has basic opportunity-cost selling. No trailing stops, no time-based exits, no partial profit taking across any agent.

4. **ML compute underutilized**: 192GB (M1 Max + M2 Ultra) of Apple Silicon unified memory is mostly idle. These machines should be running continuous inference for price prediction and sentiment analysis.

5. **Signal correlation**: Regime detection, RSI extreme, and momentum signals are correlated (all derived from price/candle data). Need signals from fundamentally different data sources (on-chain, social, macro).

6. **Small capital compounds slowly**: $153 with $0.50/day DCA and small grid profits. Need to accelerate capital growth or add fresh deposits.

7. **Disabled agents**: dex_grid_trader (needs ETH bridged to Base) and gov_data (crash-loops) are leaving alpha on the table.

8. **No derivatives access**: Missing entire asset class of options, perpetuals, and funding rate strategies.

---

## 13. Communication and Conventions

### How Agents Communicate
- Shared SQLite databases (agent_trades, portfolio_snapshots tables in trader.db)
- Dashboard push (orchestrator pushes snapshot every 5 minutes to Fly.io)
- File-based coordination (pending_bridges.json for bridge-in-transit tracking)
- All agents import `AgentTools` class for unified market data, trading, and risk management

### Data Conventions
- All timestamps in UTC
- All monetary values in USD (or USDC, treated as 1:1)
- Trading pairs use USDC quote for execution (BTC-USDC), USD for price data (BTC-USD)
- Credentials in environment variables ONLY (loaded from `.env`, never hardcoded)
- All wallets registered in `wallet_registry` table in orchestrator.db
- Each agent logs to both stdout and `agents/{agent_name}.log`

### Proposing a New Strategy to This System

1. Define the signal source and expected edge (why does this work?)
2. Specify game theory principle (which equilibrium are we exploiting?)
3. Estimate fees and minimum profitable trade size
4. Write the COLD backtest configuration
5. Define promotion criteria (WARM and HOT thresholds)
6. Define kill criteria (when does this strategy get fired?)
7. Estimate compute requirements (which node? how much RAM?)
8. Estimate correlation with existing 8 signals (must be < 0.3 to add value)

---

*This document is the single source of truth for the NetTrace trading infrastructure. Codex 5.3 should reference this when proposing strategies, signal sources, optimizations, or scaling plans. All proposals must respect the trading rules in Section 5 and game theory principles in Section 6. Code changes should be discussed with Claude Code (Opus 4.6) for implementation.*
