# Deployment Optimizer Roadmap

Updated: 2026-02-14T22:33:26.692917+00:00

## Summary
- Runtime region: local
- Deployment score: 0.52
- Live HF ready: True

## Venue Readiness
- coinbase: live_ready=True dns_ok=True creds=True reason=ready
- fix: live_ready=False dns_ok=True creds=False reason=requires_gateway_url+dns+timeout
- ibkr: live_ready=False dns_ok=True creds=False reason=requires_host_port+health

## Region Ranking
- ewr: score=0.750 role=Primary Coordinator (US East)
- nrt: score=0.410 role=Asian Exchange Primary (Tokyo)
- sin: score=0.410 role=Asian Exchange Backup (Singapore)
- ord: score=0.380 role=CME/NYMEX Proximity (US Central)
- lhr: score=0.330 role=European Exchange Hub (London)
- fra: score=0.330 role=European Backup + Risk Monitor (Frankfurt)
- bom: score=0.330 role=DGCX + India Market Monitor (Mumbai)

## Priority Actions
- Deploy FIX gateway and set FIX_GATEWAY_URL as live fallback route.
- Set IBKR_HOST/IBKR_PORT (and gateway process) for futures/equity routing.
- Top recent Coinbase failures: 9x API down | 8x <urlopen error [Errno 1] Operation not permitted> | [Errno 1] Operation not permitted | errno 1 | 6x Server Error
- Start continuous traceroute sampling to venue hosts for region-level routing evidence.
