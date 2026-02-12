# Thread: Network latency between crypto exchanges

**Type:** twitter_thread
**Generated:** 2026-02-12T12:36:58.102650+00:00

---

1/ Built a tool that monitors network latency to 20+ crypto exchanges from 7 global locations. Here's what I found:

2/ Binance from Newark: 0.85ms
Binance from Tokyo: 170ms+
That 170ms gap is the difference between getting filled first or getting rekt.

3/ Route changes are the silent killer. When BGP routes shift, your "fast" exchange connection can suddenly add 20-30ms. We detect these in real-time.

4/ Cross-exchange divergence: When Binance latency spikes but Coinbase stays stable, there's an arb window. Our quant engine generates signals for this.

5/ Free API and status dashboard:
Status: https://nettrace-dashboard.fly.dev/status
API: https://nettrace-dashboard.fly.dev/playground

Built on $17/mo of cloud infra. 100+ financial endpoints monitored.
