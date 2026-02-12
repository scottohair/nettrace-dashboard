# [Tool] Free API for real-time latency to crypto exchanges and brokers

**Type:** reddit_algotrading
**Generated:** 2026-02-12T12:36:58.100665+00:00

---

# Free API: Real-time latency data for 100+ financial endpoints

Built a platform that continuously pings crypto exchanges, brokers, and financial APIs from 7 global locations and exposes the data via REST API.

**What it does:**
- Monitors latency to Binance, Coinbase, Kraken, Interactive Brokers, Alpaca, NYSE, NASDAQ, etc.
- Detects route changes (BGP shifts that affect execution speed)
- Generates quant signals from latency anomalies
- Scans every 15 minutes from Newark, Chicago, London, Frankfurt, Singapore, Tokyo, Mumbai

**Free tier:** 100 API calls/day, 24h history
**Pro ($249/mo):** 10k calls/day, 30d history, route data, quant signals
**Enterprise ($2,499/mo):** Unlimited, bulk export, WebSocket stream

API Playground: https://nettrace-dashboard.fly.dev/playground
Status Dashboard: https://nettrace-dashboard.fly.dev/status

Currently seeing some interesting patterns — Binance from Newark is 0.85ms while from Tokyo it's 170ms+. Route changes on Tradier cause 20-30ms spikes.

Anyone doing latency arbitrage research? Curious what latency thresholds matter for your strategies.
