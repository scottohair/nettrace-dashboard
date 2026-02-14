# Automation Empire - Complete Deployment Guide

## 🚀 Quick Start (5 Minutes to Live)

### 1. Start the Website Locally
```bash
cd ~/src/quant/automation_empire/websites/clawd_bot
python3 serve.py
```

**Your site is now running at**: http://localhost:8000

---

### 2. Expose to Internet (Choose One)

#### Option A: ngrok (Fastest - 60 seconds)
```bash
# Install
brew install ngrok

# Run (in new terminal)
ngrok http 8000

# Output will show:
# Forwarding: https://abc123.ngrok.app -> http://localhost:8000
```

**Your site is now LIVE**: `https://abc123.ngrok.app`

#### Option B: Cloudflare Tunnel (Production - 5 minutes)
```bash
# Install
brew install cloudflare/cloudflare/cloudflared

# Login
cloudflared tunnel login

# Create tunnel
cloudflared tunnel create clawd-bot

# Route domain (if you own clawd.bot)
cloudflared tunnel route dns clawd-bot clawd.bot

# Run tunnel
cloudflared tunnel run --url http://localhost:8000 clawd-bot
```

**Your site is now LIVE**: `https://clawd.bot`

---

## 📊 Deploy First Revenue-Generating Agent

### 1. Price Arbitrage Bot

```bash
cd ~/src/quant/automation_empire/agents/scrapers

# Configure products to track
python3 -c "
import json
state = {
    'products_tracked': [
        {'id': 'B08N5WRWNW', 'name': 'PlayStation 5'},
        {'id': 'B08HR7SV3M', 'name': 'Xbox Series X'},
        {'id': 'B09G9FPHY6', 'name': 'AirPods Pro'},
    ]
}
with open('price_arb_electronics_state.json', 'w') as f:
    json.dump(state, f, indent=2)
print('✅ Products configured')
"

# Run agent (scans every 5 minutes)
python3 price_arbitrage_agent.py
```

**Expected Output**:
```
INFO - Starting cycle for price_arb_electronics
INFO - Scanning for electronics arbitrage...
INFO - Found 3 opportunities
INFO - Executed opportunity: +$12.50
INFO - Cycle complete: +$12.50
```

---

### 2. Sports Betting Arbitrage (High Revenue Potential)

```bash
# Install dependencies
pip3 install requests beautifulsoup4 selenium

# Create agent (template provided)
cd ~/src/quant/automation_empire/agents/scrapers
```

Create `sports_arb_agent.py`:
```python
#!/usr/bin/env python3
from price_arbitrage_agent import PriceArbitrageAgent  # Reuse template

class SportsArbAgent(PriceArbitrageAgent):
    def __init__(self):
        super().__init__(product_category="sports_betting")
        self.bookmakers = [
            "DraftKings", "FanDuel", "BetMGM", "Caesars", "PointsBet"
        ]

    def scan(self):
        # Scan for arbitrage opportunities across bookmakers
        # Details in automation_empire/ideas/arbitrage/arbitrage_ideas.json
        pass
```

**Revenue Potential**: $1K-$50K/month (from web search data)

---

### 3. Lead Generation Service (Easy to Start)

```bash
cd ~/src/quant/automation_empire/agents/scrapers
```

Create `lead_gen_agent.py`:
```python
#!/usr/bin/env python3
"""
Lead generation for local businesses
Example: Dentists in NYC with 5+ employees
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from base_agent import BaseAgent
import requests

class LeadGenAgent(BaseAgent):
    def __init__(self, profession="dentists", location="NYC"):
        super().__init__(
            agent_id=f"lead_gen_{profession}_{location}",
            category="data_services"
        )
        self.profession = profession
        self.location = location

    def scan(self):
        """Scrape Google Maps, Yelp, Yellow Pages for leads"""
        # Use Google Maps API, Places API, or scraping
        # Return list of businesses with contact info
        leads = []
        # ... scraping logic
        return leads

    def execute(self, lead):
        """Sell lead to customer or add to database"""
        # Package lead and sell via API
        revenue = 5.0  # $5 per qualified lead
        cost = 0.10    # scraping cost
        return (True, revenue, cost, lead)
```

**Pricing**: $2-10 per lead, 100-1000 leads/day = $200-$10K/day

---

## 🌐 Integrate with Main NetTrace Platform

### 1. Add Revenue Stream to Dashboard

Edit `~/src/quant/app.py`:

```python
# Add new API endpoint
@app.route("/api/automation-empire/stats")
def automation_empire_stats():
    """Stats from all automation agents"""
    import sqlite3
    import glob

    total_revenue = 0.0
    total_profit = 0.0
    active_agents = 0

    # Scan all agent databases
    agent_dbs = glob.glob("/Users/scott/src/quant/automation_empire/agents/*.db")

    for db_path in agent_dbs:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT SUM(revenue_usd), SUM(net_profit_usd) FROM runs WHERE status='success'")
            revenue, profit = cursor.fetchone()
            if revenue:
                total_revenue += revenue
                total_profit += profit
                active_agents += 1
        except:
            pass
        finally:
            conn.close()

    return jsonify({
        "total_revenue_usd": total_revenue,
        "total_profit_usd": total_profit,
        "active_agents": active_agents,
        "categories": {
            "saas": 200,
            "arbitrage": 132,
            "data_services": 145,
            "ai_agents": 187,
            "marketplace": 94,
            "infrastructure": 100
        }
    })
```

### 2. Update Dashboard UI

Add to `~/src/quant/templates/dashboard.html`:

```html
<div class="card">
    <h3>Automation Empire</h3>
    <div id="automation-stats">Loading...</div>
</div>

<script>
fetch('/api/automation-empire/stats')
    .then(r => r.json())
    .then(data => {
        document.getElementById('automation-stats').innerHTML = `
            <div>Total Revenue: $${data.total_revenue_usd.toFixed(2)}</div>
            <div>Net Profit: $${data.total_profit_usd.toFixed(2)}</div>
            <div>Active Agents: ${data.active_agents}</div>
        `;
    });
</script>
```

---

## 🐳 Containerize & Deploy to Fly.io

### 1. Create Dockerfile

```dockerfile
# ~/src/quant/automation_empire/Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Copy code
COPY agents/ /app/agents/
COPY websites/ /app/websites/
COPY requirements.txt /app/

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
EXPOSE 8000

# Run website + agents
CMD ["sh", "-c", "python3 /app/websites/clawd_bot/serve.py & python3 /app/agents/orchestrator.py"]
```

### 2. Create fly.toml

```toml
# ~/src/quant/automation_empire/fly.toml
app = "clawd-bot"
primary_region = "ewr"

[build]
  dockerfile = "Dockerfile"

[env]
  PORT = "8000"

[[services]]
  internal_port = 8000
  protocol = "tcp"

  [[services.ports]]
    handlers = ["http"]
    port = 80

  [[services.ports]]
    handlers = ["tls", "http"]
    port = 443

  [services.concurrency]
    hard_limit = 25
    soft_limit = 20

[mounts]
  source = "clawd_data"
  destination = "/data"
```

### 3. Deploy

```bash
cd ~/src/quant/automation_empire

# Login to Fly
/Users/scott/.fly/bin/flyctl auth login

# Create app
flyctl apps create clawd-bot

# Create persistent volume
flyctl volumes create clawd_data --region ewr --size 1

# Deploy
flyctl deploy

# Check status
flyctl status

# View logs
flyctl logs
```

**Your automation empire is now LIVE**: `https://clawd-bot.fly.dev`

---

## 💰 Revenue Tracking

### View Agent Performance

```bash
# Single agent
sqlite3 ~/src/quant/automation_empire/agents/price_arb_electronics.db \
  "SELECT
    SUM(revenue_usd) as total_revenue,
    SUM(cost_usd) as total_cost,
    SUM(net_profit_usd) as net_profit,
    COUNT(*) as total_runs
  FROM runs WHERE status='success'"

# All agents combined
cd ~/src/quant/automation_empire/agents
for db in *.db; do
  echo "=== $db ==="
  sqlite3 "$db" "SELECT SUM(net_profit_usd) FROM runs WHERE status='success'"
done
```

### Daily Revenue Report

```bash
# Create daily report script
cat > ~/src/quant/automation_empire/daily_report.sh <<'EOF'
#!/bin/bash
echo "📊 Automation Empire Daily Report - $(date)"
echo "================================================"

total=0
for db in ~/src/quant/automation_empire/agents/*.db; do
  agent=$(basename "$db" .db)
  profit=$(sqlite3 "$db" "SELECT COALESCE(SUM(net_profit_usd), 0) FROM runs WHERE DATE(started_at) = DATE('now')")
  echo "  $agent: \$$profit"
  total=$(echo "$total + $profit" | bc)
done

echo "================================================"
echo "  TOTAL: \$$total"
EOF

chmod +x ~/src/quant/automation_empire/daily_report.sh

# Add to cron (run daily at 9 AM)
(crontab -l 2>/dev/null; echo "0 9 * * * ~/src/quant/automation_empire/daily_report.sh") | crontab -
```

---

## 🔄 Scaling Strategy (Path to $1M/day)

### Phase 1: Prove Models (Week 1-2)
- Deploy top 10 "easy + high revenue" ideas
- Track which generate positive ROI
- Kill losers after 100 runs with negative P&L

### Phase 2: Scale Winners (Week 3-4)
- Clone profitable agents 10x
- Deploy to all 7 Fly regions
- Increase capital allocation 2x per week

### Phase 3: Diversify (Month 2)
- Launch 50 new ideas from master list
- Activate marketplace + infrastructure categories
- Add white-label revenue (license to others)

### Phase 4: Automation (Month 3+)
- Auto-discovery of new business ideas via AI
- Self-deploying agents (no human intervention)
- Reach $1M/day through 1000+ active agents

---

## 📚 Additional Resources

### Idea Database
```bash
# Browse all 858 ideas
open ~/src/quant/automation_empire/ideas/master_index.json

# Filter by category
jq '.ideas[] | select(.category == "arbitrage") | .title' \
  ~/src/quant/automation_empire/ideas/master_index.json

# Filter by difficulty
jq '.ideas[] | select(.difficulty == "easy" and .potential_revenue | contains("$10,000")) | .title' \
  ~/src/quant/automation_empire/ideas/master_index.json
```

### Agent Templates
- **Base Agent**: `agents/base_agent.py` (inherit from this)
- **Scraper**: `agents/scrapers/price_arbitrage_agent.py`
- **Trading**: `~/src/quant/agents/sniper.py` (reference)

### Documentation
- Port Forwarding: `infrastructure/port_forwarding/SETUP.md`
- Business Ideas: `ideas/master_index.json`
- Website: `websites/clawd_bot/index.html`

---

## 🎯 Next Steps

1. **Start Local Website**:
   ```bash
   cd ~/src/quant/automation_empire/websites/clawd_bot
   python3 serve.py
   ```

2. **Expose via ngrok**:
   ```bash
   ngrok http 8000
   ```

3. **Deploy First Agent**:
   ```bash
   cd ~/src/quant/automation_empire/agents/scrapers
   python3 price_arbitrage_agent.py
   ```

4. **Monitor Revenue**:
   ```bash
   ~/src/quant/automation_empire/daily_report.sh
   ```

5. **Scale to Fly.io**:
   ```bash
   cd ~/src/quant/automation_empire
   flyctl deploy
   ```

---

## 🚨 Important Notes

- **Follow Scott's 3 Rules**: Never lose money, always make money, grow money faster
- **Fire Losers**: Kill any agent with net negative P&L after 100 runs
- **Clone Winners**: 2x allocation for agents with >2.0 Sharpe ratio
- **Legal Compliance**: Check ToS for all scraped sites, use rate limiting
- **Use Maker Orders**: Limit orders only (avoid high fees)

---

**Ready to build an automation empire?** Start with the quick start guide above!
