#!/usr/bin/env python3
"""Quick portfolio report — run every 5 minutes."""
import json, os, sys, time, sqlite3
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent))

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

from exchange_connector import CoinbaseTrader, PriceFeed

def report():
    t = CoinbaseTrader()
    accts = t._request("GET", "/api/v3/brokerage/accounts?limit=250")

    holdings = {}
    cash_usdc = 0
    cash_usd = 0
    hold_usdc = 0
    hold_usd = 0
    total_value = 0

    for a in accts.get("accounts", []):
        cur = a.get("currency", "")
        avail = float(a.get("available_balance", {}).get("value", 0))
        hold = float(a.get("hold", {}).get("value", 0))
        if cur == "USDC":
            cash_usdc = avail
            hold_usdc = hold
        elif cur == "USD":
            cash_usd = avail
            hold_usd = hold
        elif avail > 0 or hold > 0:
            price = PriceFeed.get_price(f"{cur}-USD")
            usd_val = (avail + hold) * price if price else 0
            if usd_val > 0.01:
                holdings[cur] = {"amount": avail + hold, "usd": usd_val, "price": price}

    total_held = sum(h["usd"] for h in holdings.values())
    total_cash = cash_usdc + cash_usd + hold_usdc + hold_usd
    total_value = total_held + total_cash

    now = datetime.now().strftime("%H:%M:%S")
    print(f"\n{'='*60}")
    print(f"  PORTFOLIO REPORT — {now}")
    print(f"{'='*60}")
    print(f"  Total Value:  ${total_value:.2f}")
    print(f"  Cash:         ${total_cash:.2f} (USDC: ${cash_usdc:.2f} avail + ${hold_usdc:.2f} hold | USD: ${cash_usd:.2f})")
    print(f"  Holdings:     ${total_held:.2f}")
    print()

    for cur, h in sorted(holdings.items(), key=lambda x: -x[1]["usd"]):
        print(f"    {cur:6s}: {h['amount']:>12.4f} @ ${h['price']:>10,.2f} = ${h['usd']:>8.2f}")

    # Check sniper recent trades
    sniper_db = str(Path(__file__).parent / "sniper.db")
    if Path(sniper_db).exists():
        db = sqlite3.connect(sniper_db)
        db.row_factory = sqlite3.Row
        recent = db.execute(
            "SELECT pair, direction, amount_usd, status, created_at FROM sniper_trades ORDER BY id DESC LIMIT 5"
        ).fetchall()
        stats = db.execute(
            "SELECT status, COUNT(*) as cnt, SUM(amount_usd) as vol FROM sniper_trades WHERE created_at > datetime('now', '-1 hour') GROUP BY status"
        ).fetchall()

        print(f"\n  SNIPER (last hour):")
        for s in stats:
            print(f"    {s['status']:8s}: {s['cnt']} trades, ${s['vol'] or 0:.2f} volume")

        print(f"\n  Recent trades:")
        for r in recent:
            marker = "+" if r["status"] == "filled" else "X"
            print(f"    [{marker}] {r['direction']} {r['pair']} ${r['amount_usd']:.2f} [{r['status']}] {r['created_at']}")
        db.close()

    # Check running agents
    import subprocess
    result = subprocess.run(
        ["pgrep", "-lf", "python.*agents/"],
        capture_output=True, text=True
    )
    agents = [l.split()[-1].split("/")[-1] for l in result.stdout.strip().split("\n") if l]
    print(f"\n  Agents running: {len(agents)}")
    for a in agents:
        print(f"    - {a}")

    print(f"{'='*60}\n")
    return total_value


if __name__ == "__main__":
    report()
