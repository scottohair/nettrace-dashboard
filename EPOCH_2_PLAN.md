# EPOCH 2: clawd.bot Automation Platform

**Goal**: Launch automation platform, agent coordination, self-deployment
**Target**: +$20/day revenue, 500 active agents, zero-touch deployment

---

## 2.1 Launch clawd.bot UI

**New Files**:
- `automation_empire/websites/clawd_bot/app.py` (Flask backend)
- `automation_empire/websites/clawd_bot/templates/dashboard.html`
- `automation_empire/websites/clawd_bot/templates/marketplace.html`
- `automation_empire/websites/clawd_bot/static/app.js`

**Features**:
- **Strategy Marketplace**
  - Browse strategies by category
  - View backtest results (Sharpe, WR, P&L)
  - Buy/sell strategies (revenue share: 70/30)
  - Upload custom strategies (Docker containers)

- **Live Dashboard**
  - All agents status (CPU, memory, P&L)
  - Real-time signal feed
  - Portfolio breakdown by strategy
  - Risk metrics (VaR, max drawdown)

- **Backtest Interface**
  - Upload strategy code
  - Select date range + pairs
  - Run backtest (COLD tier)
  - View results (charts, metrics)

- **No-Code Workflow Builder**
  - Visual strategy designer (drag-and-drop)
  - Condition blocks (price, volume, signals)
  - Action blocks (BUY, SELL, notify)
  - Test mode before deployment

**Revenue Model**:
- Free: 1 agent, 100 API calls/day
- Pro ($99/mo): 10 agents, 10K calls/day, backtest unlimited
- Enterprise (custom): Unlimited + white-label + API access

**Tech Stack**:
- Frontend: React + Tailwind CSS + Chart.js
- Backend: Flask + WebSocket (for live updates)
- Database: PostgreSQL (migrate from SQLite)
- Auth: JWT tokens + Stripe integration

---

## 2.2 Agent Communication Bus

**New Files**:
- `agents/message_bus.py` (Redis pub/sub wrapper)
- `agents/bus_subscriber.py` (agent subscriber)
- `agents/bus_publisher.py` (agent publisher)

**Technology**: Redis pub/sub

**Channels**:
```
/signals/btc          → BTC signals from all agents
/signals/eth          → ETH signals
/positions/open       → Position opened notification
/positions/close      → Position closed notification
/alerts/risk          → Risk alerts (drawdown, exposure)
/alerts/opportunity   → High-confidence opportunities
/system/heartbeat     → Agent health checks
```

**Message Format**:
```json
{
  "channel": "/signals/btc",
  "agent_id": "sniper_v2",
  "timestamp": "2026-02-14T12:00:00Z",
  "data": {
    "signal": "BUY",
    "confidence": 0.85,
    "pair": "BTC-USD",
    "price": 96500,
    "reason": "multi_signal_breakout"
  }
}
```

**Benefits**:
- Real-time coordination between 100+ agents
- No polling (event-driven)
- Decouple agents (add/remove without changes)
- Cross-region communication (ewr ↔ lhr ↔ nrt)

**Implementation**:
```python
from agents.message_bus import MessageBus

bus = MessageBus()

# Publish
bus.publish('/signals/btc', {
    'signal': 'BUY',
    'confidence': 0.85,
    'pair': 'BTC-USD'
})

# Subscribe
def handle_signal(message):
    print(f"Signal: {message['data']}")

bus.subscribe('/signals/btc', handle_signal)
bus.run()
```

---

## 2.3 Self-Deploying Agents

**New Files**:
- `agents/self_deployer.py` (agent deployment automation)
- `agents/fly_deployer_v2.py` (enhanced Fly.io deployer)
- `agents/deployment_pipeline.py` (COLD → WARM → HOT → FLY)

**Concept**: Agents can deploy themselves to Fly.io when they pass WARM tier

**Process**:
```
1. Strategy passes WARM tier (10 trades, 65% WR, Sharpe > 1.0)
2. Agent generates Dockerfile
3. Agent builds image
4. Agent deploys to Fly.io (ewr region)
5. Agent monitors health
6. If profitable: promote to HOT, scale to multi-region
7. If unprofitable: kill deployment
```

**Example**:
```python
class SelfDeployingAgent:
    def check_promotion_criteria(self):
        if self.trades >= 10 and self.win_rate > 0.65 and self.sharpe > 1.0:
            return True
        return False

    def deploy_to_fly(self):
        # 1. Generate Dockerfile
        dockerfile = self.generate_dockerfile()

        # 2. Build image
        image_name = f'agent-{self.agent_id}:latest'
        self.build_image(image_name)

        # 3. Deploy to Fly
        app_name = f'agent-{self.agent_id}'
        self.flyctl_deploy(app_name, image_name, region='ewr')

        # 4. Monitor health
        self.monitor_health(app_name)

    def run(self):
        while True:
            # Trade
            self.execute_strategy()

            # Check promotion
            if self.check_promotion_criteria():
                logger.info('Deploying to Fly.io...')
                self.deploy_to_fly()
                break
```

**Security**:
- Fly API token from env var
- Resource limits (memory, CPU)
- Auto-kill runaway agents
- Budget caps per agent

**Expected**: Zero-touch deployment, 100+ agents on Fly.io

---

## 📊 EPOCH 2 METRICS

| Metric | EPOCH 1 | EPOCH 2 Target | Growth |
|--------|---------|----------------|--------|
| **Daily P&L** | +$5 | +$20 | 4x |
| **Active Agents** | 95 | 500 | 5.3x |
| **Strategies** | 50 | 200 | 4x |
| **Users** | 1 (Scott) | 10 (early adopters) | 10x |
| **Revenue** | Trading only | Trading + SaaS | 2 streams |

---

## 🎯 MILESTONES

**Week 1** (Days 8-14):
- Build clawd.bot dashboard (React + Flask)
- Integrate Stripe billing
- Launch beta (invite 5 users)

**Week 2** (Days 15-21):
- Implement Redis message bus
- Migrate agents to pub/sub
- Cross-region signal sharing

**Week 3** (Days 22-28):
- Self-deploying agents
- Automated Fly.io deployment
- Budget controls + monitoring

**Week 4** (Days 29-30):
- Public launch clawd.bot
- Marketing (ProductHunt, HN, Twitter)
- First paying customers

---

## 💰 REVENUE MODEL

**Trading Revenue**:
- Current: ~$5/day (EPOCH 1)
- Target: $10/day (better strategies)

**SaaS Revenue** (clawd.bot):
- 10 users × $99/mo = $990/mo = $33/day
- Target: $10/day in month 1

**Total EPOCH 2**: $20/day (+400% from EPOCH 1)

---

## 🚀 QUICK START (When Ready)

```bash
cd ~/src/quant

# 1. Build clawd.bot UI
cd automation_empire/websites/clawd_bot
npm install
npm run build

# 2. Launch backend
python3 app.py

# 3. Set up Redis
brew install redis
redis-server

# 4. Start message bus
python3 agents/message_bus.py

# 5. Deploy self-deploying agent
python3 agents/self_deployer.py --strategy-id <top_strategy>
```

---

**Ready to start EPOCH 2?**
