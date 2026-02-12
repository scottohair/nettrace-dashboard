# Show HN: Real-time latency monitoring for 100+ financial endpoints

**Type:** hn_post
**Generated:** 2026-02-12T12:36:58.099419+00:00

---

Show HN: NetTrace — Real-time latency monitoring for 100+ financial endpoints

I built a network intelligence platform that continuously monitors latency to crypto exchanges, stock brokers, cloud providers, and government data APIs from 7 global locations.

Key features:
- 100+ targets: Binance, Coinbase, NYSE, NASDAQ, Interactive Brokers, Alpaca, etc.
- 7 scanner regions: Newark, Chicago, London, Frankfurt, Singapore, Tokyo, Mumbai
- Quant signals: Z-score anomaly detection, route change alerts, cross-exchange divergence
- REST API with free tier (100 calls/day)
- Prometheus metrics, embeddable badges

Live: https://nettrace-dashboard.fly.dev/status
API Playground: https://nettrace-dashboard.fly.dev/playground
Prometheus: https://nettrace-dashboard.fly.dev/metrics

Built with Python/Flask/SQLite on Fly.io. ~$17/mo hosting for 7 global nodes.

The data is useful for:
- HFT firms optimizing execution paths
- Quant funds looking for latency-based alpha
- DevOps teams monitoring financial API reliability
- Researchers studying internet infrastructure

Free API tier available. Feedback welcome.
