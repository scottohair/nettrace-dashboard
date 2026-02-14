#!/usr/bin/env python3
"""Quick P&L analyzer — check performance in 30-min windows.

Usage: python3 quick_pnl.py [agent] [minutes]
Examples:
  python3 quick_pnl.py sentiment_leech 30    # Last 30 min of sentiment_leech
  python3 quick_pnl.py                       # All agents, last 30 min
  python3 quick_pnl.py sentiment_leech 120   # Last 2 hours
"""

import sqlite3
import sys
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict
import statistics

agents_config = {
    "sentiment_leech": {
        "db": "sentiment_leech.db",
        "table": "signals",
    },
    "regulatory_scanner": {
        "db": "regulatory_scanner.db",
        "table": "signals",
    },
    "liquidation_hunter": {
        "db": "liquidation_hunter.db",
        "table": "cascade_predictions",
    },
    "narrative_tracker": {
        "db": "narrative_tracker.db",
        "table": "signals",
    },
    "futures_mispricing": {
        "db": "futures_mispricing.db",
        "table": "arbitrage_opportunities",
    },
}


def analyze_agent(agent_name, minutes=30):
    """Analyze agent P&L for last N minutes."""
    config = agents_config.get(agent_name)
    if not config:
        return None

    db_path = Path(__file__).parent / config["db"]
    if not db_path.exists():
        return None

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        table = config["table"]
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        cutoff_str = cutoff.isoformat()

        cursor = conn.execute(
            f"SELECT * FROM {table} WHERE status = 'filled' AND filled_at > ? ORDER BY filled_at ASC",
            (cutoff_str,)
        )
        trades = [dict(row) for row in cursor.fetchall()]
        conn.close()

        if not trades:
            return {
                "agent": agent_name,
                "trades": 0,
                "pnl_usd": 0.0,
                "win_count": 0,
                "loss_count": 0,
                "win_rate": 0.0,
                "avg_pnl": 0.0,
                "sharpe": 0.0,
            }

        # Calculate metrics
        pnl_values = []
        win_count = 0
        loss_count = 0
        total_pnl = 0.0

        for trade in trades:
            pnl = trade.get("pnl_usd", 0.0) or 0.0
            pnl_values.append(pnl)
            total_pnl += pnl

            if pnl > 0:
                win_count += 1
            elif pnl < 0:
                loss_count += 1

        # Sharpe ratio
        try:
            stdev = statistics.stdev(pnl_values) if len(pnl_values) > 1 else 0
            mean = sum(pnl_values) / len(pnl_values)
            sharpe = (mean / stdev) if stdev > 0 else 0.0
        except Exception:
            sharpe = 0.0

        win_rate = win_count / len(trades)

        return {
            "agent": agent_name,
            "trades": len(trades),
            "pnl_usd": total_pnl,
            "win_count": win_count,
            "loss_count": loss_count,
            "win_rate": win_rate,
            "avg_pnl": total_pnl / len(trades) if trades else 0.0,
            "sharpe": sharpe,
        }

    except Exception as e:
        print(f"Error analyzing {agent_name}: {e}")
        return None


def main():
    agent_filter = sys.argv[1] if len(sys.argv) > 1 else None
    minutes = int(sys.argv[2]) if len(sys.argv) > 2 else 30

    print(f"\n📊 Phase 1 Quick P&L Analysis ({minutes}-min window)")
    print(f"   as of {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("━" * 80)

    agents_to_check = [agent_filter] if agent_filter else list(agents_config.keys())

    total_pnl = 0.0
    total_trades = 0

    for agent_name in agents_to_check:
        result = analyze_agent(agent_name, minutes)
        if result is None:
            print(f"{agent_name:25} | NO DATA")
            continue

        total_pnl += result["pnl_usd"]
        total_trades += result["trades"]

        # Color coding
        pnl_str = f"${result['pnl_usd']:+.2f}"
        if result["pnl_usd"] > 0:
            pnl_color = "🟢"
        elif result["pnl_usd"] < 0:
            pnl_color = "🔴"
        else:
            pnl_color = "⚪"

        sharpe_str = f"{result['sharpe']:.3f}"
        if result["sharpe"] >= 0.5:
            sharpe_color = "🟢"
        elif result["sharpe"] >= 0.0:
            sharpe_color = "🟡"
        else:
            sharpe_color = "🔴"

        print(
            f"{agent_name:25} | "
            f"Trades: {result['trades']:2d} | "
            f"P&L: {pnl_color} {pnl_str:>10} | "
            f"WR: {result['win_rate']:>5.1%} | "
            f"Sharpe: {sharpe_color} {sharpe_str:>6} | "
            f"Avg: ${result['avg_pnl']:>7.2f}"
        )

    print("━" * 80)
    print(f"{'TOTAL':25} | Trades: {total_trades:2d} | P&L: ${total_pnl:+.2f}")
    print()

    # Escalation recommendations
    print("🚀 ESCALATION RECOMMENDATIONS:")
    print("   Sharpe > 0.5 AND Win Rate > 55% = ESCALATE")
    print()

    for agent_name in agents_to_check:
        result = analyze_agent(agent_name, minutes)
        if result and result["trades"] >= 3:
            if result["sharpe"] >= 0.5 and result["win_rate"] >= 0.55:
                print(f"   ✅ {agent_name}: READY TO ESCALATE")
            else:
                print(f"   ⏳ {agent_name}: Keep monitoring")


if __name__ == "__main__":
    main()
