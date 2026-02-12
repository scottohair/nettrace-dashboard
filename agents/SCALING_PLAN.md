# Scaling Plan: From Proof of Concept to Institutional-Grade Quant Operation

> Last updated: 2026-02-12
> Status: Active Planning Document

---

## Table of Contents

1. [Broker Infrastructure](#1-broker-infrastructure)
2. [Capital Deployment Plan](#2-capital-deployment-plan)
3. [Infrastructure Scaling Plan](#3-infrastructure-scaling-plan)
4. [Strategy Scaling](#4-strategy-scaling)
5. [Regulatory Considerations](#5-regulatory-considerations)
6. [Timeline and Milestones](#6-timeline-and-milestones)

---

## 1. Broker Infrastructure

### 1A. Interactive Brokers (IB) -- The Gold Standard for Quant Firms

Interactive Brokers is the primary broker for serious algorithmic trading operations, offering
unmatched asset coverage, low commissions, and a mature API ecosystem.

**Account Requirements:**
- IBKR Pro account required (IBKR Lite does NOT support API access)
- $0 minimum to open an account
- $2,000 minimum to access margin
- TWS (Trader Workstation) or IB Gateway must be running locally or on a server
- IB Gateway is preferred for headless/server deployments (lighter weight, no GUI overhead)

**Asset Coverage:**
- US and international stocks and ETFs
- Options (all US exchanges)
- Futures and futures options
- Forex (20+ currency pairs)
- Bonds (corporate, municipal, government)
- Crypto (Bitcoin, Ethereum, Litecoin, Bitcoin Cash via Paxos)
- Mutual funds, warrants, structured products

**Commission Structure (IBKR Pro, Fixed Pricing):**
- Stocks/ETFs: $0.005/share ($1.00 minimum, 1% of trade value maximum)
- Stocks/ETFs (Tiered): $0.0005 - $0.0035/share depending on monthly volume
- Options: $0.65/contract (no fee for exercise/assignment)
- Futures: $0.85/contract
- Forex: 0.08-0.20 basis points x trade value ($2.00 minimum)

**Margin:**
- Day trading (pattern day trader): 4:1 leverage (25% margin requirement)
- Overnight (Reg T): 2:1 leverage (50% margin requirement)
- Portfolio margin (available at $110k+): up to 6.67:1 leverage based on risk
- Margin interest rates: benchmark + 0.5% to 1.5% depending on balance tier

**API Ecosystem:**

| Component | Description |
|-----------|-------------|
| **TWS API (ibapi)** | Official native Python SDK. Stable, well-documented, socket-based. Current version: API v10.44+. |
| **ib_async** | Community successor to ib_insync (renamed early 2024). Async/await native, implements full IBKR binary protocol internally. Recommended over raw ibapi for new projects. |
| **ib_insync** | Legacy async wrapper (archived 2024, replaced by ib_async). Still widely referenced in tutorials. |
| **Client Portal API** | REST API for account management, not ideal for trading. |
| **IBKR Web API** | Newer REST + WebSocket API. OAuth authentication. |

**Connection Architecture:**
```
Our System --> TCP Socket (port 7496/7497) --> TWS / IB Gateway --> IB Servers
```

**What We Need to Get Started:**
1. Open IBKR Pro account ($0 minimum, but fund with at least $2,000 for margin)
2. Install TWS or IB Gateway on a dedicated machine or server
3. Enable API connections in TWS/Gateway settings (Configure > API > Settings)
4. Install Python SDK: `pip install ibapi` (official) or `pip install ib_async` (recommended wrapper)
5. Enable paper trading first for strategy validation (port 7497 for paper, 7496 for live)

**Key Limitations:**
- Maximum 50 simultaneous API messages/second
- Market data requires subscriptions ($1.50-$30/month per exchange)
- No native cloud deployment; TWS/Gateway must run somewhere with a display server (or use IBC for headless)
- API connection drops require automatic reconnection handling

---

### 1B. Alpaca -- Commission-Free Stock/Crypto Trading

Alpaca is ideal for strategy prototyping, paper trading, and commission-sensitive strategies
where execution quality is less critical.

**Account Requirements:**
- $0 minimum balance
- Free paper trading account (no funding required)
- US residents only for live trading (international via Alpaca International LLC)

**Asset Coverage:**
- US stocks and ETFs (commission-free)
- Options (commission-free for retail)
- Crypto (BTC, ETH, and other major tokens)
- No futures, forex, or bonds

**Commission Structure:**
- Stocks/ETFs: $0 commission
- Options: $0 commission
- Crypto: Spread markup only (no explicit commission)

**API Architecture:**
- REST API for order management and account data
- WebSocket for real-time market data streaming
- WebSocket for real-time order/trade updates
- Official Python SDK: `pip install alpaca-py`

**Market Data:**
- Free: IEX real-time data (15-min delayed SIP)
- Paid ($99/month): Full SIP real-time data from all US exchanges
- Historical: 6+ years of bars, trades, quotes
- Crypto: Real-time from multiple venues

**Paper Trading:**
- Full simulation environment with real-time market data
- Identical API endpoints (just different base URL)
- No funding required
- Ideal for CI/CD strategy testing pipelines

**Key Advantages Over IB for Testing:**
- No software installation required (pure REST/WebSocket)
- No running gateway process to maintain
- Simpler authentication (API key + secret)
- Commission-free means lower drag on strategy backtests vs. live performance

**Key Limitations:**
- US equities and crypto only (no futures, forex, bonds)
- No margin beyond 2:1 (Reg T)
- Execution quality may be lower than IB (payment for order flow)
- Not suitable for latency-sensitive strategies

---

### 1C. Broker Selection Matrix

| Criteria | Interactive Brokers | Alpaca |
|----------|-------------------|--------|
| Asset classes | Stocks, options, futures, forex, bonds, crypto | Stocks, options, crypto |
| Commissions | $0.005/share stocks, $0.65/contract options | $0 stocks/options |
| API complexity | Medium-High (socket-based, requires gateway) | Low (REST + WebSocket) |
| Margin | 4:1 day, 2:1 overnight, portfolio margin | 2:1 Reg T |
| Execution quality | Excellent (direct market access) | Good (PFOF model) |
| Paper trading | Yes (separate port) | Yes (separate endpoint) |
| Best for | Production trading, multi-asset, HFT | Prototyping, equity-only strategies |

**Recommendation:** Use Alpaca for initial proof-of-concept and paper trading. Migrate to
Interactive Brokers for production deployment at the $10k+ capital level.

---

## 2. Capital Deployment Plan

### Phase 1: $100 -- Proof of Concept (Current)

**Objective:** Validate that our signal detection and execution pipeline works end-to-end with real money.

- Deploy on Alpaca (commission-free, minimize friction)
- Single strategy: cross-exchange arbitrage signals from traceroute latency data
- Position sizing: $10-$25 per trade
- Maximum simultaneous positions: 4
- Daily loss limit: $10 (10% of capital)
- Target: Demonstrate positive expectancy over 30 trading days
- Key metric: Sharpe ratio > 1.0 after transaction costs

### Phase 2: $10,000 -- Multi-Strategy Deployment

**Objective:** Run 3-5 strategies simultaneously with proper risk management.

- Migrate primary execution to Interactive Brokers
- Maintain Alpaca as secondary/backup broker
- Strategies: cross-exchange arb, mean reversion, momentum
- Position sizing: Kelly criterion with half-Kelly conservative adjustment
- Maximum single position: $2,000 (20% of capital)
- Maximum sector exposure: $4,000 (40% of capital)
- Daily loss limit: $500 (5% of capital)
- Drawdown halt: 15% from equity peak triggers full review
- Target: 2-5% monthly returns, Sharpe > 1.5

### Phase 3: $100,000 -- Serious Quant Operation

**Objective:** Institutional-quality risk management with diversified alpha sources.

- Interactive Brokers with Portfolio Margin (6.67:1 leverage available)
- 8-12 uncorrelated strategies running simultaneously
- Add: options strategies, volatility arbitrage, statistical arbitrage
- Dedicated risk management system with real-time P&L monitoring
- Position sizing: risk parity across strategies
- Maximum single position: $10,000 (10% of capital)
- Daily loss limit: $3,000 (3% of capital)
- Strategy-level stop losses with automatic deleveraging
- Target: 3-8% monthly returns, Sharpe > 2.0, max drawdown < 15%

### Phase 4: $1,000,000 -- Institutional Grade

**Objective:** Run a professional trading operation with proper legal structure.

- Form LLC or LP for fund structure
- Multiple prime broker relationships (IB primary, consider adding a second)
- 20+ strategies across all asset classes
- Dedicated operations: risk management, compliance, infrastructure monitoring
- Proper disaster recovery and business continuity planning
- Audited track record for potential outside investors
- Target: 2-5% monthly returns, Sharpe > 2.5, max drawdown < 10%

### Phase 5: $10,000,000 -- Target $1M/Day Revenue

**Objective:** High-frequency, multi-venue market making generating consistent daily revenue.

- Multiple prime brokers with DMA (Direct Market Access)
- Co-located servers at major exchange data centers
- Custom hardware (FPGA) for latency-critical strategies
- Full compliance and regulatory infrastructure
- 50+ strategies, 1000+ instruments traded daily
- Revenue target: $200k-$1M/day gross ($50k-$250k/day net)
- Risk budget: daily VaR < $500k (5% of capital)
- Full-time team: 3-5 engineers, 1-2 quant researchers, 1 compliance officer

---

## 3. Infrastructure Scaling Plan

### Phase 1: Current State -- 7 Fly Nodes + Local M3 Air

**Architecture:**
```
M3 MacBook Air (local)
  - Strategy development and backtesting
  - Signal generation from traceroute data
  - Order management and execution

7x Fly.io Nodes (distributed)
  - Continuous traceroute monitoring
  - Latency data collection from global vantage points
  - Data aggregation and forwarding to local machine
```

**Limitations:**
- Single point of failure (local laptop)
- Limited compute for ML model training
- Network latency to brokers from consumer ISP
- No redundancy in execution path

### Phase 2: Add M1 Max + M2 Ultra as Compute Nodes

**Architecture:**
```
M2 Ultra (primary compute)
  - ML model training (76-core GPU)
  - Strategy backtesting (24-core CPU, 192GB RAM)
  - Real-time signal generation
  - Primary execution engine

M1 Max (secondary compute)
  - Redundant execution engine (failover)
  - Live strategy monitoring dashboard
  - Historical data management and storage
  - TWS/IB Gateway host

M3 Air (development)
  - Strategy development and research
  - Code deployment coordinator
  - Mobile monitoring

7x Fly.io Nodes (unchanged)
  - Distributed latency monitoring
```

**Network:**
- Dedicated VLAN between Mac machines
- Tailscale mesh VPN for secure inter-node communication
- UPS on all compute nodes (minimum 30 minutes runtime)
- Automated failover: if M2 Ultra goes down, M1 Max takes over execution within 5 seconds

**Estimated Cost:** $0/month incremental (hardware already owned), ~$20/month Fly.io

### Phase 3: AWS/GCP Free Tier for Additional Vantage Points

**Architecture additions:**
```
AWS Free Tier (12 months)
  - t2.micro in us-east-1 (Virginia) -- near NYSE/NASDAQ
  - t2.micro in eu-west-1 (Ireland) -- European market coverage
  - t2.micro in ap-northeast-1 (Tokyo) -- Asian market coverage

GCP Free Tier
  - e2-micro in us-central1 (Iowa)
  - e2-micro in europe-west1 (Belgium)

Oracle Cloud Free Tier (always free)
  - 2x ARM instances (4 OCPU, 24GB RAM each)
  - Use for heavy data processing
```

**Benefits:**
- 12+ global vantage points for latency arbitrage signals
- Cloud instances can run lightweight IB Gateway for redundant execution
- Geographic diversity in signal generation
- Near-zero incremental cost

**Estimated Cost:** $0-$50/month (free tier + minimal overages)

### Phase 4: Co-Located Servers Near Exchanges

**Target Data Centers:**

| Facility | Location | Exchanges | Use Case |
|----------|----------|-----------|----------|
| **Equinix NY5** | Secaucus, NJ | NYSE, NASDAQ, BATS/Cboe | US equity execution |
| **Equinix NY4** | Secaucus, NJ | CME (futures), ICE | Futures execution |
| **Equinix LD4** | Slough, UK | LSE, Euronext | European markets |
| **Equinix TY3** | Tokyo, Japan | TSE, JPX | Asian markets |

**Equinix NY5 Specifications:**
- 800 Secaucus Road, Secaucus, NJ 07094
- Direct cross-connects to NYSE, NASDAQ, BATS, Direct Edge
- Sub-microsecond latency to exchange matching engines
- 20MW critical power capacity, N+1 redundancy
- Phase two expansion of adjacent NY3 facility scheduled for 2026

**Colocation Setup:**
- Start with 1U server in NY5 ($500-$2,000/month)
- Cross-connect to exchange feeds ($200-$500/month per exchange)
- Dedicated 10Gbps network ($500-$1,500/month)
- Total estimated cost: $2,000-$5,000/month for initial NY5 presence

**Estimated Cost:** $2,000-$10,000/month depending on number of venues

### Phase 5: FPGA/Custom Hardware for HFT

**Architecture:**
```
FPGA-Based Trading System
  - Xilinx Alveo U250/U280 or Intel Stratix 10 FPGA cards
  - Market data parsing in hardware (< 1 microsecond)
  - Order generation in hardware (< 500 nanoseconds)
  - Risk checks in hardware (< 200 nanoseconds)
  - Total tick-to-trade: < 2 microseconds

Custom Network Stack
  - Kernel bypass (DPDK or Solarflare OpenOnload)
  - Raw Ethernet frames for minimum latency
  - Custom TCP/IP stack optimized for trading protocols
  - Hardware timestamping for precise latency measurement
```

**FPGA Development:**
- Language: Verilog/VHDL or high-level synthesis (HLS) from C++
- Development timeline: 6-12 months for production-ready system
- Consider vendor solutions: Xilinx Vitis, Intel OneAPI
- Alternative: License existing FPGA trading frameworks

**Estimated Cost:**
- FPGA cards: $5,000-$15,000 each
- Development tools: $10,000-$50,000/year
- Specialized engineers: $200,000-$400,000/year salary
- Total Phase 5 investment: $500,000-$2,000,000

---

## 4. Strategy Scaling

### Phase 1 (Current): Cross-Exchange Arbitrage + Latency Signals

**Strategies:**
1. **Cross-Exchange Latency Arbitrage** -- Exploit measurable latency differences between
   exchange data centers detected via traceroute analysis. When we detect degraded
   connectivity to one venue, anticipate stale prices and trade against them.
2. **Network Event Trading** -- Trade on detected network infrastructure events (BGP route
   changes, CDN failovers, cloud provider outages) that may temporarily impact market
   microstructure.

**Instruments:** 5-10 highly liquid US equities and ETFs (SPY, QQQ, AAPL, MSFT, NVDA)
**Expected Edge:** 5-15 bps per trade
**Capacity:** $100-$1,000/day trading volume

### Phase 2: Mean Reversion, Momentum, Statistical Arbitrage

**New Strategies:**
3. **Intraday Mean Reversion** -- Trade short-term deviations from VWAP/TWAP in liquid
   names. Entry on 2+ sigma deviation, exit on reversion to mean.
4. **Momentum/Trend Following** -- Identify and ride intraday momentum using volume-weighted
   signals. Position in direction of detected institutional flow.
5. **Statistical Arbitrage** -- Cointegrated pairs (e.g., XOM/CVX, GS/MS, SPY/IVV) with
   z-score based entry/exit. Half-life optimization for mean reversion speed.
6. **Earnings/Event Drift** -- Post-earnings announcement drift captured via options or
   equity positions.

**Instruments:** 50-100 US equities, major ETFs
**Expected Edge:** 3-10 bps per trade
**Capacity:** $10,000-$100,000/day trading volume

### Phase 3: Options Market Making, Volatility Arbitrage

**New Strategies:**
7. **Options Market Making** -- Quote two-sided markets in liquid options chains. Manage
   delta/gamma/vega exposure dynamically. Target: capture bid-ask spread while staying
   delta-neutral.
8. **Volatility Arbitrage** -- Trade implied vs. realized volatility divergences. Long
   gamma when IV < RV (buy straddles), short gamma when IV > RV (sell strangles with hedging).
9. **Dispersion Trading** -- Trade index volatility vs. component volatility. Sell index
   straddles, buy component straddles when correlation is expected to decrease.
10. **Calendar Spread Arbitrage** -- Exploit term structure anomalies in volatility surface.

**Instruments:** Options on 200+ underlyings, VIX futures, volatility ETFs
**Expected Edge:** 2-8 bps per trade
**Capacity:** $100,000-$1,000,000/day notional volume

### Phase 4: Pairs Trading Across 100+ Equities

**New Strategies:**
11. **Multi-Factor Equity Long/Short** -- Systematic long/short equity based on value,
    momentum, quality, and proprietary signals. Sector-neutral, beta-neutral construction.
12. **Cross-Asset Pairs** -- Trade relationships between equities and related instruments
    (e.g., stock vs. ADR, stock vs. convertible bond, equity vs. CDS).
13. **ETF Arbitrage** -- Exploit NAV vs. market price discrepancies in ETFs. Creation/redemption
    arbitrage in commodity and international ETFs.
14. **Sector Rotation** -- Systematic rotation between sectors based on macro regime,
    relative momentum, and mean-reversion signals.

**Instruments:** 500+ equities, 100+ ETFs, futures, forex crosses
**Expected Edge:** 1-5 bps per trade
**Capacity:** $1,000,000-$10,000,000/day trading volume

### Phase 5: Full Market Making Across Multiple Venues

**New Strategies:**
15. **Multi-Venue Market Making** -- Simultaneously quote on NYSE, NASDAQ, BATS, IEX, and
    dark pools. Manage inventory risk across all venues. Capture spread + rebates.
16. **Cross-Asset Market Making** -- Make markets in related instruments simultaneously
    (e.g., SPY + ES futures + SPX options). Hedge across asset classes in real-time.
17. **International Arbitrage** -- Trade the same or correlated instruments across US,
    European, and Asian exchanges. Exploit timezone and liquidity gaps.
18. **Latency Arbitrage at Scale** -- Sub-microsecond execution exploiting speed advantages
    from co-located FPGA hardware.

**Instruments:** 2000+ instruments across all asset classes and geographies
**Expected Edge:** 0.5-3 bps per trade (compensated by massive volume)
**Capacity:** $10,000,000-$100,000,000/day trading volume

---

## 5. Regulatory Considerations

### 5A. Pattern Day Trader (PDT) Rule

**Current Rule (FINRA Rule 4210):**
- Applies if you execute 4+ day trades within 5 business days in a margin account
- Day trade = open and close same position within one trading day
- Requires $25,000 minimum equity in the account at all times
- Violation results in account restriction (90-day freeze on day trading)

**Upcoming Changes (2026):**
- FINRA Board voted to replace the PDT rule with an intraday margin framework
- The $25,000 minimum equity requirement would be eliminated
- Replaced by risk-based intraday margin calculations applied to actual exposure
- Filed with SEC; pending formal approval as of February 2026
- Until SEC approves: the $25,000 PDT rule remains fully in force

**Workarounds Under Current Rules:**
- Use a cash account (no PDT rule, but no margin and T+1 settlement)
- Trade on Alpaca with cash account for unlimited day trades (settled funds only)
- Multiple broker accounts (each with separate PDT tracking)
- Trade futures (PDT rule applies only to equities and options in margin accounts)
- Maintain $25,000+ in IB margin account to avoid restrictions entirely

### 5B. SEC/State Registration Requirements

**Under $25 Million AUM:**
- Register with state securities regulators (varies by state)
- New York: must register with SEC if managing $25M+ (lower threshold than federal)
- Most states: register as Investment Adviser with state if < $100M AUM

**$25 Million - $100 Million AUM:**
- May register with SEC or state depending on state rules
- "Mid-sized advisers" in most states register at the state level
- Must file Form ADV with SEC regardless

**$100 Million - $150 Million AUM:**
- SEC registration required if managing separately managed accounts (SMAs)
- Private fund adviser exemption available if ONLY managing private funds < $150M AUM

**$150 Million+ AUM:**
- SEC registration mandatory (Investment Advisers Act of 1940)
- Must file Form ADV Parts 1 and 2
- Annual ADV updates required within 90 days of fiscal year end
- Form PF filing required for private fund advisers (compliance date extended to October 1, 2026)
- Subject to SEC examination and audit

### 5C. Accredited Investor Requirements

If raising outside capital (relevant at Phase 4-5):
- Accredited investors: $200k income ($300k joint) or $1M net worth (excluding primary residence)
- Regulation D, Rule 506(b): up to 35 non-accredited + unlimited accredited investors (no general solicitation)
- Regulation D, Rule 506(c): unlimited accredited only (general solicitation allowed, must verify status)
- Qualified Purchaser: $5M+ in investments (required for 3(c)(7) funds with 100+ investors)

### 5D. Additional Regulatory Considerations

**CFTC/NFA Registration:**
- Required if trading futures or commodity options for others
- Commodity Trading Advisor (CTA) registration
- Commodity Pool Operator (CPO) if pooling funds for futures trading

**Broker-Dealer Registration:**
- Generally NOT required for proprietary trading
- Required if executing trades for others or providing brokerage services
- Market making on registered exchanges may require broker-dealer status

**Tax Considerations:**
- Mark-to-market election (Section 475(f)) for trader tax status
- Must elect by April 15 of the tax year
- Allows deduction of trading losses as ordinary losses (no $3,000 capital loss limit)
- Wash sale rule does not apply with Section 475 election
- Requires "trader" (not "investor") status per IRS criteria

**Reporting:**
- Form 13F: Required quarterly if managing $100M+ in Section 13(f) securities
- Form 13H: Required for "large traders" (2M+ shares or $20M+ in any single day, or 20M+ shares or $200M+ in any month)
- Regulation SHO: Short sale reporting and locate requirements

---

## 6. Timeline and Milestones

### Q1 2026 (Now) -- Foundation

- [x] Cross-exchange arbitrage signal detection operational
- [x] 7 Fly.io nodes collecting traceroute data
- [ ] Open Interactive Brokers Pro account
- [ ] Open Alpaca paper trading account
- [ ] Deploy proof-of-concept strategy on Alpaca paper trading
- [ ] Validate signal-to-execution pipeline end-to-end
- [ ] Achieve positive Sharpe ratio on paper for 30+ days

### Q2 2026 -- First Live Capital

- [ ] Fund Alpaca account with $100 for live proof-of-concept
- [ ] Fund IB account with $2,000 (margin access minimum)
- [ ] Deploy 2-3 strategies on paper simultaneously
- [ ] Set up M1 Max as secondary compute node
- [ ] Implement proper risk management (position limits, daily loss limits)
- [ ] Build real-time P&L monitoring dashboard

### Q3 2026 -- Scale to $10,000

- [ ] Demonstrate consistent positive returns over 60+ trading days
- [ ] Deploy multi-strategy portfolio on IB
- [ ] Add mean reversion and momentum strategies
- [ ] Expand to 50+ instruments
- [ ] Set up AWS/GCP free tier vantage points
- [ ] Achieve Sharpe > 1.5

### Q4 2026 -- Scale to $100,000

- [ ] Deploy M2 Ultra as primary compute
- [ ] 8-12 strategies running simultaneously
- [ ] Add options strategies (market making, vol arb)
- [ ] Implement portfolio margin on IB
- [ ] Build automated strategy monitoring and alerting
- [ ] Begin evaluating colocation options

### 2027 -- Scale to $1,000,000

- [ ] Establish legal entity (LLC or LP)
- [ ] Deploy co-located server at Equinix NY5
- [ ] 20+ strategies across multiple asset classes
- [ ] Hire or contract compliance consultant
- [ ] Audited track record for potential investors
- [ ] Evaluate FPGA development or vendor solutions

### 2028+ -- Scale to $10,000,000+

- [ ] SEC registration (if managing outside capital above thresholds)
- [ ] Multiple co-located servers across exchanges
- [ ] FPGA-accelerated execution
- [ ] Full market making across multiple venues
- [ ] Target: consistent $200k-$1M/day gross revenue
- [ ] Consider outside investor capital raise (Reg D)

---

## Appendix A: Key API Endpoints

### Interactive Brokers (TWS API via ib_async)

```python
from ib_async import IB, Stock, MarketOrder

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)  # 7497=paper, 7496=live

# Request market data
contract = Stock('AAPL', 'SMART', 'USD')
ib.qualifyContracts(contract)
ticker = ib.reqMktData(contract)

# Place order
order = MarketOrder('BUY', 100)
trade = ib.placeOrder(contract, order)

# Monitor positions
positions = ib.positions()
pnl = ib.reqPnL(account)
```

### Alpaca (REST API via alpaca-py)

```python
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

client = TradingClient('API_KEY', 'SECRET_KEY', paper=True)

# Place order
order_data = MarketOrderRequest(
    symbol='AAPL',
    qty=10,
    side=OrderSide.BUY,
    time_in_force=TimeInForce.DAY
)
order = client.submit_order(order_data)

# Get positions
positions = client.get_all_positions()
account = client.get_account()
```

---

## Appendix B: Risk Management Framework

### Position-Level Controls

| Parameter | Phase 1 ($100) | Phase 2 ($10k) | Phase 3 ($100k) | Phase 4 ($1M) |
|-----------|----------------|-----------------|-------------------|----------------|
| Max position size | $25 | $2,000 | $10,000 | $50,000 |
| Max positions | 4 | 10 | 25 | 50 |
| Max sector exposure | 50% | 40% | 30% | 20% |
| Daily loss limit | $10 (10%) | $500 (5%) | $3,000 (3%) | $20,000 (2%) |
| Drawdown halt | 20% | 15% | 12% | 10% |
| Max leverage | 1x | 2x | 4x (portfolio margin) | 4x |

### Strategy-Level Controls

- Each strategy has independent risk budget (VaR allocation)
- Automatic deleveraging if strategy drawdown exceeds 2x expected
- Strategy correlation monitoring -- reduce allocation if correlations spike
- Circuit breaker: halt all trading if portfolio loses > daily limit
- Manual override capability at all times

---

## Appendix C: Cost Projections

| Phase | Capital | Monthly Infra Cost | Monthly Trading Cost | Monthly Revenue Target |
|-------|---------|-------------------|---------------------|----------------------|
| 1 | $100 | $20 (Fly.io) | $0 (Alpaca) | $5-$15 |
| 2 | $10,000 | $20 | $50-$200 (IB commissions) | $200-$500 |
| 3 | $100,000 | $50 | $500-$2,000 | $3,000-$8,000 |
| 4 | $1,000,000 | $5,000 (colo) | $5,000-$20,000 | $20,000-$50,000 |
| 5 | $10,000,000 | $20,000 (colo+FPGA) | $50,000-$200,000 | $200,000-$1,000,000 |

---

*This document is a living plan and should be updated as strategies are validated,
capital grows, and market conditions change. All projections are targets, not guarantees.
Past performance in paper trading does not guarantee live trading results.*
