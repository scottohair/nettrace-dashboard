# Codex Needs From Scott to Start Earning Real Money

## 1) API Keys / Accounts Needed

1. Coinbase Advanced Trade (production) API credentials:
- `API Key`
- `API Secret`
- `Passphrase` (if configured)
- Permissions: `trade` + `view` only, **no withdrawal permission**

2. Coinbase account readiness:
- Full identity verification complete
- USD funding enabled (ACH/wire)
- Spot trading enabled for BTC-USD and ETH-USD

3. Payments account for immediate revenue:
- Stripe account (or Lemon Squeezy) for paid signal subscriptions
- One product configured: `NetTrace Alpha Alerts` (monthly)

4. Distribution account for paid alerts:
- Telegram bot token + admin rights in a private paid channel
- (Alternative: Discord bot token + private server admin)

## 2) Permissions / Access Needed

1. Permission to place live trades automatically on Coinbase under strict limits.
2. Permission to set hard risk controls in code and exchange settings:
- Max risk per trade
- Max daily loss
- Kill switch on latency/feed anomalies

3. Fly.io deployment access:
- Ability to set/rotate secrets
- Deploy rights to `nettrace-dashboard`
- Log access for incident response

4. Data/infra access:
- Read access to scanner outputs and quant signal DB in production
- Permission to run the execution service 24/7

5. Commercial permissions:
- Approval to sell a paid signal product immediately (landing page + checkout + private channel access)

## 3) Initial Capital Needed

Minimum to start this week:

1. Trading capital: **$3,000**
- $2,000 allocated to BTC strategy
- $1,000 allocated to ETH strategy

2. Risk reserve: **$1,000**
- Held as drawdown/fee buffer, not actively deployed

3. Operating cash (first month): **$300**
- Existing hosting + monitoring + messaging/payment tooling

Total recommended starting budget: **$4,300**.

## 4) What I Would Build First (Fastest Path to Revenue)

1. **Live micro-size execution on Coinbase (first 3-5 days)**
- Convert current paper bot to live with tiny position sizes
- Trade only BTC-USD and ETH-USD
- Enforce hard caps: small notional, max open positions, max daily loss
- Goal: produce real-money PnL track record fast

2. **Risk and reliability layer (parallel, same week)**
- Exchange-side and app-side kill switch
- Circuit breaker when scanner coverage drops or latency signals are stale
- Auto-pause after consecutive losses or slippage spikes

3. **Paid signal product launch (within 7-10 days)**
- Stripe checkout + webhook provisioning
- Private Telegram channel with real-time alerts + daily summary
- Start pricing at **$99/month** and target first 10 paid users

4. **Weekly performance report pipeline**
- Auto-generate weekly metrics (PnL, win rate, drawdown, latency regime)
- Use this for subscriber retention and conversion

Fastest cash flow is: launch paid alerts while live trading runs at small size.

## 5) Tools / Services Needed

1. **Stripe** (or Lemon Squeezy): collect subscription payments.
2. **Telegram Bot API**: deliver paid real-time alerts.
3. **Sentry** (or Better Stack): production error and uptime monitoring.
4. **Secrets manager** (1Password/Doppler/Fly secrets discipline): secure API key handling and rotation.
5. **Simple analytics** (Plausible/PostHog): conversion tracking on pricing/signup flow.

## Immediate Items I Need From Scott (Today)

1. Coinbase production API secret/passphrase with trade+view permissions (no withdrawals).
2. Approval for live auto-trading with these hard limits:
- Max 0.5% account risk per trade
- Max 2% daily loss then stop
- BTC/ETH only

3. Stripe account access (or owner setup) so I can create checkout and webhook flow.
4. Telegram bot token and private channel admin access.
5. Confirmation of starting capital: **$4,300** target (minimum).
