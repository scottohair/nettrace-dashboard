#!/bin/bash
cd ~/src/quant/agents
echo "💰 QUICK P&L"
echo "═══════════════════════════════════════════════════════════════"
sqlite3 risk_controller.db "
SELECT agent || ': ' || wins || 'W-' || losses || 'L = $' || ROUND(total_pnl, 2)
FROM agent_performance 
WHERE total_pnl != 0 OR wins > 0
" 2>/dev/null || echo "No data yet"
echo "═══════════════════════════════════════════════════════════════"
