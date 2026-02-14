# Deployment Optimizer Roadmap

Updated: 2026-02-14T19:34:22.188104+00:00

## Summary
- Runtime region: local
- Deployment score: 0.31
- Live HF ready: False

## Venue Readiness
- coinbase: live_ready=False dns_ok=True creds=True reason=requires_dns+credentials+healthy_telemetry
- fix: live_ready=False dns_ok=True creds=False reason=requires_gateway_url+dns+timeout
- ibkr: live_ready=False dns_ok=True creds=False reason=requires_host_port+health

## Region Ranking
- ewr: score=0.470 role=Primary Coordinator (US East)
- nrt: score=0.230 role=Asian Exchange Primary (Tokyo)
- sin: score=0.230 role=Asian Exchange Backup (Singapore)
- ord: score=0.200 role=CME/NYMEX Proximity (US Central)
- lhr: score=0.150 role=European Exchange Hub (London)
- fra: score=0.150 role=European Backup + Risk Monitor (Frankfurt)
- bom: score=0.150 role=DGCX + India Market Monitor (Mumbai)

## Priority Actions
- Stabilize Coinbase API health before live budget escalation (improve retries + region routing).
- Deploy FIX gateway and set FIX_GATEWAY_URL as live fallback route.
- Set IBKR_HOST/IBKR_PORT (and gateway process) for futures/equity routing.
- Top recent Coinbase failures: 8x fallback_spot_unavailable | 8x _fetch_json:api.exchange.coinbase.com:fallback_failed:<urlopen error [Errno 1] Operation not permitted> | 6x <urlopen error [Errno 1] Operation not permitted> | [Errno 1] Operation not permitted | errno 1
- Start continuous traceroute sampling to venue hosts for region-level routing evidence.
