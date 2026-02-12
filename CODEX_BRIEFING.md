# NetTrace - Financial Network Intelligence Platform

## What This Is
NetTrace is a live financial data intelligence platform that continuously monitors network latency to 100+ financial endpoints (exchanges, brokers, APIs, gov data sources) from a Fly.io edge node in Newark, NJ (EWR). It sells this latency data via tiered API subscriptions and is building toward running its own quant strategies on the data.

## Live Deployment
- **URL**: https://nettrace-dashboard.fly.dev
- **Region**: ewr (Newark, NJ — close to NYSE/NASDAQ data centers)
- **Stack**: Python Flask + SQLite + gevent WebSocket + Chart.js frontend
- **Scanner**: Continuous background scanner hits 100 targets every 15 minutes

## Revenue Model (Tiers)
| Tier | Price | What |
|------|-------|------|
| Free | $0 | 100 API calls/day, 24h history |
| Pro | $249/mo | 10k calls/day, 30d history, routes, alerts |
| Enterprise | $2,499/mo | Unlimited API, 90d history, bulk export, WebSocket stream |
| Enterprise Pro | $50,000/mo | Dedicated scanning, 365d history, custom targets, SLA |
| Government | $500,000/mo | FedRAMP-ready, 2yr history, isolated infra, 24/7 SLA |

## Data We Collect
- **scan_metrics**: target_host, total_rtt, hop_count, first_hop_rtt, last_hop_rtt, route_hash, every 15min
- **scan_snapshots**: Full hop-by-hop JSON with geo data, every 4th scan or on route change
- **route_changes**: When BGP routing shifts — old/new route hash, RTT delta, timestamp
- **ip_geo_cache**: IP → lat/lon/city/country/ISP mapping

## 100 Monitored Targets (7 categories)
- Quant APIs (15): Alpaca, Polygon, IEX, Twelve Data, Tiingo, Tradier, Alpha Vantage, Finnhub, etc.
- Cloud Providers (10): AWS, GCP, Azure, Oracle, DigitalOcean, etc.
- NYSE & Financial (15): NYSE, Bloomberg, NASDAQ, CBOE, CME, S&P Global, etc.
- Crypto Exchanges (20): Binance, Coinbase, Kraken, OKX, Bybit, dYdX, etc.
- Forex & Brokers (15): OANDA, Interactive Brokers, Robinhood, Schwab, etc.
- CDN & DNS (10): Cloudflare, Google DNS, Akamai, Fastly, etc.
- Gov & Data (15): FRED, SEC EDGAR, Treasury, BLS, Census, etc.

## API Endpoints
- GET /api/v1/targets — all monitored targets with latest metrics
- GET /api/v1/latency/{host} — current latency
- GET /api/v1/latency/{host}/history — time-series data
- GET /api/v1/routes/{host} — hop-by-hop route data
- GET /api/v1/routes/{host}/changes — route change history
- GET /api/v1/rankings — latency rankings
- GET /api/v1/compare?hosts=a,b — compare targets
- GET /api/v1/export/{host} — bulk CSV/JSON export

## Key Files
- app.py — Main Flask app, DB schema, auth, payments, API key management
- scheduler.py — ContinuousScanner background thread
- api_v1.py — REST API Blueprint
- api_auth.py — API key auth, rate limiting, tier config
- targets.json — 100 targets across 7 categories
- mcp_server.py — MCP server for remote tool access

## What Needs To Happen Next
1. **Domain & Email** — Register domain, set up support email
2. **Quant Strategies** — Use the latency data to detect trading signals:
   - Route changes to exchanges = potential latency arbitrage
   - Latency spikes correlate with network congestion during high-volume trading
   - RTT regression models to predict degradation
3. **Digital Wallets** — Expand into global crypto/forex wallet integration
4. **Cloud Expansion** — Deploy scanner nodes near financial centers (NY4, LD4, TY3, etc.)
5. **Revenue Automation** — Automatically generate and act on quant signals
6. **Odd Regions** — Focus on underserved markets (Southeast Asia, Africa, LatAm, Middle East)

## MCP Tools Available
You have access to the NetTrace MCP server with tools: scan, scan_remote, status, machines, scale, deploy, logs, exec
