# Fly Migration Roadmap

Updated: 2026-02-14T10:11:56.347705+00:00

## Summary
- Runtime region: local
- Regions in rollout scope: 7
- Quant apps planned: 7
- Clawdbot apps planned: 7
- Target mode: fly_primary_remote

## Priority Actions
- Deploy/scale nettrace app per region and keep ENABLE_AGENTS=1 everywhere.
- Pin primary treasury + allocation authority in PRIMARY_REGION; run scout stacks in secondary regions.
- Deploy one clawdbot/OpenClaw instance per region for local tool/agent orchestration.
- Route quant agent control through API keys in Fly secrets, never through repository files.
- Cut local-only orchestrator loops after remote health + PnL close reconciliation is green.

## Region Matrix
- ewr: nettrace_app=nettrace-dashboard clawdbot_app=clawdbot-ewr agents=capital_allocator,advanced_team,sniper,meta_engine
- nrt: nettrace_app=nettrace-dashboard-nrt clawdbot_app=clawdbot-nrt agents=latency_arb
- sin: nettrace_app=nettrace-dashboard-sin clawdbot_app=clawdbot-sin agents=latency_arb,exchange_scanner
- ord: nettrace_app=nettrace-dashboard-ord clawdbot_app=clawdbot-ord agents=exchange_scanner
- lhr: nettrace_app=nettrace-dashboard-lhr clawdbot_app=clawdbot-lhr agents=exchange_scanner,forex_arb
- fra: nettrace_app=nettrace-dashboard-fra clawdbot_app=clawdbot-fra agents=exchange_scanner,forex_arb
- bom: nettrace_app=nettrace-dashboard-bom clawdbot_app=clawdbot-bom agents=exchange_scanner,forex_arb
