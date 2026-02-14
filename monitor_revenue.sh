#!/bin/bash
# Real-time revenue monitoring - updates every 3 seconds

while true; do
    clear
    echo "════════════════════════════════════════════════════════════════════"
    echo "💰 LIVE REVENUE MONITOR - $(date '+%H:%M:%S')"
    echo "════════════════════════════════════════════════════════════════════"
    echo ""
    
    # Check sniper
    if ps -p $(cat /tmp/sniper.pid 2>/dev/null) >/dev/null 2>&1; then
        echo "✅ Sniper: RUNNING (PID: $(cat /tmp/sniper.pid))"
    else
        echo "❌ Sniper: STOPPED"
    fi
    
    echo ""
    echo "💵 PROFIT & LOSS:"
    echo "────────────────────────────────────────────────────────────────────"
    
    cd ~/src/quant/agents
    sqlite3 risk_controller.db "SELECT agent, wins, losses, ROUND(total_pnl, 2) FROM agent_performance WHERE total_pnl != 0" 2>/dev/null | while IFS='|' read agent wins losses pnl; do
        wr=$(echo "scale=1; $wins * 100 / ($wins + $losses)" | bc 2>/dev/null || echo "0")
        echo "   $agent: ${wins}W-${losses}L (${wr}% WR) - \$${pnl}"
    done
    
    echo ""
    echo "📝 RECENT LOGS:"
    echo "────────────────────────────────────────────────────────────────────"
    tail -5 /tmp/sniper_live.log 2>/dev/null | sed 's/^/   /' || echo "   No logs yet"
    
    echo ""
    echo "════════════════════════════════════════════════════════════════════"
    echo "Refreshing in 3s... (Ctrl+C to stop)"
    
    sleep 3
done
