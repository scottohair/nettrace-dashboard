#!/bin/bash
# Quick Start Script for Automation Empire

set -e

echo "🚀 Automation Empire - Quick Start"
echo "=================================="
echo ""

# Navigate to automation empire directory
EMPIRE_DIR="$HOME/src/quant/automation_empire"

if [ ! -d "$EMPIRE_DIR" ]; then
    echo "❌ Error: automation_empire directory not found at $EMPIRE_DIR"
    exit 1
fi

cd "$EMPIRE_DIR"

# 1. Install dependencies
echo "📦 Step 1: Installing dependencies..."
pip3 install -q flask flask-cors requests beautifulsoup4 2>/dev/null || true
echo "   ✅ Dependencies installed"
echo ""

# 2. Check if ideas are generated
if [ ! -f "ideas/master_index.json" ]; then
    echo "💡 Step 2: Generating 858 business ideas..."
    python3 generate_ideas.py > /dev/null
    echo "   ✅ Ideas generated: ideas/master_index.json"
else
    echo "💡 Step 2: Business ideas already exist"
    idea_count=$(jq '.total_ideas' ideas/master_index.json)
    echo "   ℹ️  Found $idea_count ideas"
fi
echo ""

# 3. Show top 5 easy + high revenue ideas
echo "🎯 Step 3: Top 5 Easy + High Revenue Ideas:"
jq -r '.ideas[] | select(.difficulty == "easy") | select(.potential_revenue | contains("$10,000") or contains("$20,000") or contains("$30,000") or contains("$50,000") or contains("$100,000")) | "   • [\(.category)] \(.title) - \(.potential_revenue)"' ideas/master_index.json | head -5
echo ""

# 4. Port forwarding options
echo "🌐 Step 4: Port Forwarding Options"
echo ""
echo "Choose how to expose your site to the internet:"
echo ""
echo "A) ngrok (Easiest - 60 seconds)"
echo "   $ brew install ngrok"
echo "   $ ngrok http 8000"
echo ""
echo "B) Cloudflare Tunnel (Production - Free Forever)"
echo "   $ brew install cloudflare/cloudflare/cloudflared"
echo "   $ cloudflared tunnel login"
echo "   $ cloudflared tunnel create clawd-bot"
echo "   $ cloudflared tunnel run --url http://localhost:8000 clawd-bot"
echo ""
echo "C) Router Port Forwarding (Full Control)"
echo "   See: infrastructure/port_forwarding/SETUP.md"
echo ""

# 5. Start website in background
echo "🖥️  Step 5: Starting clawd.bot website..."
cd websites/clawd_bot

# Kill existing process if running
pkill -f "serve.py" 2>/dev/null || true

# Start in background
nohup python3 serve.py > /tmp/clawd_bot.log 2>&1 &
WEBSITE_PID=$!
echo "   ✅ Website running on http://localhost:8000 (PID: $WEBSITE_PID)"
echo "   📄 Logs: tail -f /tmp/clawd_bot.log"
echo ""

# Wait for server to start
sleep 2

# Test if server is up
if curl -s http://localhost:8000/api/status > /dev/null; then
    echo "   ✅ Server health check passed"
else
    echo "   ⚠️  Server may not be ready yet (check logs)"
fi
echo ""

# 6. Instructions for next steps
echo "✨ Next Steps:"
echo ""
echo "1. View your website locally:"
echo "   $ open http://localhost:8000"
echo ""
echo "2. Expose to internet with ngrok (in new terminal):"
echo "   $ ngrok http 8000"
echo ""
echo "3. Deploy your first agent:"
echo "   $ cd ~/src/quant/automation_empire/agents/scrapers"
echo "   $ python3 price_arbitrage_agent.py"
echo ""
echo "4. View daily revenue:"
echo "   $ ~/src/quant/automation_empire/daily_report.sh"
echo ""
echo "5. Deploy to Fly.io (production):"
echo "   $ cd ~/src/quant/automation_empire"
echo "   $ flyctl deploy"
echo ""

echo "=================================="
echo "🎉 Automation Empire is ready!"
echo "=================================="
echo ""
echo "📊 Stats:"
jq '{total_ideas, categories}' ideas/master_index.json
echo ""
echo "🌐 Website: http://localhost:8000"
echo "📚 Docs: ~/src/quant/automation_empire/DEPLOYMENT_GUIDE.md"
echo ""
echo "Press Ctrl+C to stop the website, or leave it running."
echo "Logs: tail -f /tmp/clawd_bot.log"
