# NetTrace — Financial Network Intelligence Platform

Real-time latency monitoring for **100+ financial endpoints** from **7 global scanner nodes**.

[![Binance Latency](https://nettrace-dashboard.fly.dev/widget/api.binance.com)](https://nettrace-dashboard.fly.dev/status/api.binance.com)
[![Coinbase Latency](https://nettrace-dashboard.fly.dev/widget/api.coinbase.com)](https://nettrace-dashboard.fly.dev/status/api.coinbase.com)
[![Kraken Latency](https://nettrace-dashboard.fly.dev/widget/api.kraken.com)](https://nettrace-dashboard.fly.dev/status/api.kraken.com)

## What It Does

NetTrace continuously monitors network latency to crypto exchanges, stock brokers, cloud providers, quant APIs, and government data endpoints. Data is collected every 15 minutes from 7 locations worldwide and exposed via REST API.

**Live Dashboard:** https://nettrace-dashboard.fly.dev/status
**API Playground:** https://nettrace-dashboard.fly.dev/playground

## Monitored Targets (100+)

| Category | Count | Examples |
|----------|-------|---------|
| Crypto Exchanges | 20 | Binance, Coinbase, Kraken, OKX, Bybit, dYdX, Gemini |
| Quant APIs | 15 | Alpaca, Polygon.io, IEX, Tiingo, Alpha Vantage |
| NYSE & Financial | 15 | NYSE, NASDAQ, Bloomberg, CME, CBOE |
| Forex & Brokers | 15 | Interactive Brokers, OANDA, Robinhood, Schwab |
| Cloud Providers | 10 | AWS, GCP, Azure, Cloudflare |
| CDN & DNS | 10 | Cloudflare DNS, Google DNS, Akamai, Fastly |
| Gov & Data | 15 | FRED, SEC EDGAR, Treasury, BLS, Census |

## Scanner Regions

| Region | Location | Covers |
|--------|----------|--------|
| `ewr` | Newark, NJ | US East (primary) |
| `ord` | Chicago, IL | CME/CBOE proximity |
| `lhr` | London, UK | LSE/European markets |
| `fra` | Frankfurt, DE | EU/Balkans/Turkey |
| `sin` | Singapore | SE Asian exchanges |
| `nrt` | Tokyo, JP | TSE/Asian markets |
| `bom` | Mumbai, IN | Middle East/India |

## API

```bash
# Public docs (no auth required)
curl https://nettrace-dashboard.fly.dev/api/v1/

# With API key
curl -H "Authorization: Bearer nt_your_key" \
  https://nettrace-dashboard.fly.dev/api/v1/targets

# Latency for a specific target
curl -H "Authorization: Bearer nt_your_key" \
  https://nettrace-dashboard.fly.dev/api/v1/latency/api.binance.com

# Historical data
curl -H "Authorization: Bearer nt_your_key" \
  https://nettrace-dashboard.fly.dev/api/v1/latency/api.binance.com/history

# Rankings
curl -H "Authorization: Bearer nt_your_key" \
  https://nettrace-dashboard.fly.dev/api/v1/rankings?category=Crypto%20Exchanges

# Quant signals (Pro+)
curl -H "Authorization: Bearer nt_your_key" \
  https://nettrace-dashboard.fly.dev/api/v1/signals
```

## Pricing

| Tier | Price | API Calls/Day | History | Features |
|------|-------|---------------|---------|----------|
| Free | $0 | 100 | 24h | Latency, rankings |
| Pro | $249/mo | 10,000 | 30d | Routes, signals, compare |
| Enterprise | $2,499/mo | Unlimited | 90d | Bulk export, WebSocket stream |

## Quant Signals

The built-in quant engine generates signals from latency data:

- **RTT Anomaly** — Z-score > 2.0 from rolling baseline (latency spike/drop)
- **Route Change** — BGP path shift detected on financial endpoint
- **Cross-Exchange Divergence** — One exchange lags while another stays stable
- **Trend** — Linear regression detects sustained latency increase/decrease

## Embed Badges

Add live latency badges to your README or website:

```markdown
[![Binance](https://nettrace-dashboard.fly.dev/widget/api.binance.com)](https://nettrace-dashboard.fly.dev/status/api.binance.com)
```

## Prometheus Metrics

```
https://nettrace-dashboard.fly.dev/metrics
```

Exposes `nettrace_target_rtt_ms`, `nettrace_target_hops`, `nettrace_route_changes_1h`, `nettrace_quant_signals_1h`, and more.

## Tech Stack

- Python 3.12 / Flask / SQLite
- 7 Fly.io VMs (shared-cpu-1x, 256MB)
- Total hosting cost: ~$17/mo
- No external dependencies beyond stdlib + Flask

## Developer Fast Path

Use `tools/dev_cycle.py` for faster local iterations:

- `python3.11 tools/dev_cycle.py` runs:
  - parallel `py_compile` on changed `.py` files
  - mapped test files for those changes
- `python3.11 tools/dev_cycle.py --full` adds full-suite validation after mapped tests
- `python3.11 tools/dev_cycle.py --full --include-untracked` includes new Python files
- `python3.11 tools/dev_cycle.py --no-parallel-tests` disables parallel test execution when you want deterministic, single-process replay
