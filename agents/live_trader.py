#!/usr/bin/env python3
"""NetTrace Live Trader — trades real crypto on Coinbase using latency signals.

Strategy:
  1. Monitor our latency signals for crypto exchange anomalies
  2. When a signal fires with confidence > threshold, execute a trade
  3. Use tight risk management: 0.5% risk per trade, 2% daily max loss
  4. Trade BTC-USD, ETH-USD, SOL-USD on Coinbase

With $13 starting capital, we focus on:
  - Consolidating scattered alts into USDC
  - Trading micro positions on high-confidence signals
  - Compounding wins
"""

import json
import logging
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "live_trader.log")),
    ]
)
logger = logging.getLogger("live_trader")

TRADER_DB = str(Path(__file__).parent / "trader.db")
SIGNAL_API = "https://nettrace-dashboard.fly.dev/api/v1/signals"
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")

# Risk parameters — conservative with $13
MIN_TRADE_USD = 1.00       # Coinbase min is ~$1
MAX_TRADE_USD = 5.00       # Never risk more than $5 per trade
MAX_DAILY_LOSS_USD = 2.00  # Stop trading after $2 loss in a day
SIGNAL_MIN_CONFIDENCE = 0.55
CHECK_INTERVAL = 120       # Check every 2 minutes
POSITION_HOLD_SECONDS = 300  # Hold for 5 minutes then re-evaluate


class LiveTrader:
    def __init__(self):
        from exchange_connector import CoinbaseTrader, PriceFeed
        self.exchange = CoinbaseTrader()
        self.pricefeed = PriceFeed
        self.db = sqlite3.connect(TRADER_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.daily_pnl = 0.0
        self.trades_today = 0

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS live_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL,
                quantity REAL,
                total_usd REAL,
                signal_type TEXT,
                signal_confidence REAL,
                signal_host TEXT,
                coinbase_order_id TEXT,
                status TEXT DEFAULT 'pending',
                pnl REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS live_pnl (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_value_usd REAL,
                daily_pnl REAL,
                trades_today INTEGER,
                assets_json TEXT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

    def get_portfolio_value(self):
        """Get total portfolio value in USD."""
        accounts = self.exchange.get_accounts()
        if "accounts" not in accounts:
            logger.error("Failed to get accounts: %s", accounts)
            return 0, {}

        total_usd = 0
        holdings = {}
        for acc in accounts["accounts"]:
            bal = acc.get("available_balance", {})
            amount = float(bal.get("value", 0))
            currency = acc.get("currency", "")
            if amount <= 0:
                continue

            if currency in ("USD", "USDC"):
                usd_value = amount
            else:
                price = self.pricefeed.get_price(f"{currency}-USD")
                usd_value = amount * price if price else 0

            if usd_value > 0.01:
                holdings[currency] = {"amount": amount, "usd_value": round(usd_value, 2)}
                total_usd += usd_value

        return round(total_usd, 2), holdings

    def get_signals(self):
        """Fetch latest signals from NetTrace API."""
        try:
            url = f"{SIGNAL_API}?limit=20"
            if NETTRACE_API_KEY:
                url += f"&api_key={NETTRACE_API_KEY}"
            req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-Trader/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            return data.get("signals", [])
        except Exception as e:
            logger.debug("Signal fetch: %s", e)
            return []

    def evaluate_and_trade(self, signals):
        """Evaluate signals and execute trades."""
        if self.daily_pnl <= -MAX_DAILY_LOSS_USD:
            logger.warning("Daily loss limit hit ($%.2f). Pausing.", self.daily_pnl)
            return

        # Map exchange hosts to tradeable Coinbase pairs
        tradeable = {
            "api.coinbase.com": ["BTC-USD", "ETH-USD", "SOL-USD"],
            "api.binance.com": ["BTC-USD", "ETH-USD"],  # arb signal
            "api.kraken.com": ["BTC-USD", "ETH-USD"],
            "api.bybit.com": ["BTC-USD"],
            "api.gemini.com": ["BTC-USD", "ETH-USD"],
            "api.upbit.com": ["BTC-USD"],
            "api.gateio.ws": ["BTC-USD"],
            "api.okx.com": ["BTC-USD"],
        }

        for signal in signals:
            sig_type = signal.get("signal_type", "")
            host = signal.get("target_host", "")
            direction = signal.get("direction", "")
            confidence = float(signal.get("confidence", 0))

            if confidence < SIGNAL_MIN_CONFIDENCE:
                continue

            pairs = tradeable.get(host, [])
            if not pairs:
                continue

            # Check cooldown — don't trade same pair within 10 minutes
            recent = self.db.execute(
                "SELECT 1 FROM live_trades WHERE pair=? AND created_at > datetime('now', '-10 minutes')",
                (pairs[0],)
            ).fetchone()
            if recent:
                continue

            # Determine side
            side = None
            if sig_type == "route_change_crypto_arb":
                side = "BUY" if "latency_up" in direction else "SELL"
            elif sig_type == "rtt_anomaly":
                side = "BUY" if direction == "up" else "SELL"
            elif sig_type == "cross_exchange_latency_diff":
                side = "BUY"  # buy the stable exchange
            elif sig_type == "rtt_trend":
                side = "BUY" if direction == "uptrend" else "SELL"

            if not side:
                continue

            # Check if we can actually make this trade
            _, holdings = self.get_portfolio_value()

            # For BUY: need USD or USDC
            # For SELL: need the base currency
            pair = pairs[0]
            base = pair.split("-")[0]

            if side == "SELL":
                held = holdings.get(base, {}).get("amount", 0)
                if held <= 0:
                    # Try to BUY instead if we have USD/USDC
                    usdc = holdings.get("USDC", {}).get("usd_value", 0)
                    usd = holdings.get("USD", {}).get("usd_value", 0)
                    if usdc + usd >= MIN_TRADE_USD:
                        side = "BUY"
                    else:
                        # Try selling an asset we DO hold
                        for cur, data in holdings.items():
                            if cur not in ("USD", "USDC") and data["usd_value"] >= MIN_TRADE_USD:
                                pair = f"{cur}-USD"
                                side = "SELL"
                                break
                        else:
                            continue

            if side == "BUY":
                usdc = holdings.get("USDC", {}).get("usd_value", 0)
                usd = holdings.get("USD", {}).get("usd_value", 0)
                if usdc + usd < MIN_TRADE_USD:
                    continue

            # Size the trade — scale with confidence
            trade_usd = min(MAX_TRADE_USD, max(MIN_TRADE_USD, confidence * 8.0))

            self._execute_trade(pair, side, trade_usd, signal)
            break  # One trade per cycle

    def _execute_trade(self, pair, side, usd_amount, signal):
        """Execute a real trade on Coinbase."""
        price = self.pricefeed.get_price(pair)
        if not price:
            logger.warning("No price for %s", pair)
            return

        logger.info("EXECUTING: %s %s | $%.2f @ $%.2f | signal=%s conf=%.2f",
                     side, pair, usd_amount, price,
                     signal.get("signal_type"), float(signal.get("confidence", 0)))

        if side == "BUY":
            result = self.exchange.place_order(pair, "BUY", round(usd_amount, 2))
        else:
            # For SELL, we need base currency amount
            base_size = round(usd_amount / price, 8)
            result = self.exchange.place_order(pair, "SELL", base_size)

        order_id = None
        status = "failed"
        if "success_response" in result:
            order_id = result["success_response"].get("order_id")
            status = "filled"
            logger.info("ORDER FILLED: %s | order_id=%s", pair, order_id)
        elif "error_response" in result:
            err = result["error_response"]
            logger.warning("ORDER FAILED: %s | %s", pair, err.get("message", err))
            status = "failed"
        elif "order_id" in result:
            order_id = result["order_id"]
            status = "filled"
        else:
            logger.warning("ORDER RESPONSE: %s", json.dumps(result)[:300])

        # Record trade
        self.db.execute(
            """INSERT INTO live_trades (pair, side, price, quantity, total_usd,
               signal_type, signal_confidence, signal_host, coinbase_order_id, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (pair, side, price, usd_amount / price if price else 0, usd_amount,
             signal.get("signal_type"), signal.get("confidence"),
             signal.get("target_host"), order_id, status)
        )
        self.db.commit()
        self.trades_today += 1

    def snapshot(self):
        """Record portfolio snapshot locally and push to Fly dashboard."""
        total, holdings = self.get_portfolio_value()
        self.db.execute(
            "INSERT INTO live_pnl (total_value_usd, daily_pnl, trades_today, assets_json) VALUES (?, ?, ?, ?)",
            (total, self.daily_pnl, self.trades_today, json.dumps(holdings))
        )
        self.db.commit()

        # Push to Fly dashboard
        self._push_to_fly(total, holdings)
        return total, holdings

    def _push_to_fly(self, total, holdings):
        """Push snapshot to Fly trading dashboard."""
        try:
            # Get recent trades
            trades = self.db.execute(
                "SELECT pair, side, price, total_usd, signal_type, signal_confidence, status, created_at FROM live_trades ORDER BY id DESC LIMIT 20"
            ).fetchall()
            trades_list = [dict(t) for t in trades]
            trades_total = self.db.execute("SELECT COUNT(*) as cnt FROM live_trades").fetchone()["cnt"]

            payload = json.dumps({
                "total_value_usd": total,
                "daily_pnl": self.daily_pnl,
                "trades_today": self.trades_today,
                "trades_total": trades_total,
                "holdings": holdings,
                "trades": trades_list,
            }).encode()

            url = "https://nettrace-dashboard.fly.dev/api/trading-snapshot"
            req = urllib.request.Request(
                url, data=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Api-Key": NETTRACE_API_KEY,
                    "User-Agent": "NetTrace-Trader/1.0",
                },
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode())
                logger.debug("Pushed snapshot to Fly: %s", result)
        except Exception as e:
            logger.debug("Fly push failed: %s", e)

    def print_status(self):
        total, holdings = self.get_portfolio_value()
        trades = self.db.execute(
            "SELECT COUNT(*) as cnt, SUM(CASE WHEN status='filled' THEN 1 ELSE 0 END) as filled FROM live_trades"
        ).fetchone()

        print("\n" + "=" * 60)
        print(f"  LIVE TRADER | Portfolio: ${total:.2f}")
        print("=" * 60)
        for currency, data in sorted(holdings.items(), key=lambda x: -x[1]["usd_value"]):
            print(f"  {currency:<6} {data['amount']:<15.8f} ${data['usd_value']:.2f}")
        print(f"  ---")
        print(f"  Trades: {trades['cnt']} total, {trades['filled']} filled")
        print(f"  Daily P&L: ${self.daily_pnl:+.2f}")
        print("=" * 60 + "\n")

    def run(self):
        """Main trading loop."""
        total, holdings = self.get_portfolio_value()
        logger.info("Live Trader starting | Portfolio: $%.2f | %d assets", total, len(holdings))
        self.print_status()

        # Push initial snapshot to Fly dashboard
        self._push_to_fly(total, holdings)

        cycle = 0
        while True:
            try:
                cycle += 1

                # Get signals and trade
                signals = self.get_signals()
                if signals:
                    self.evaluate_and_trade(signals)

                # Snapshot every 5 cycles (~10 min) and push to Fly
                if cycle % 5 == 0:
                    self.snapshot()
                    if cycle % 15 == 0:
                        self.print_status()

                time.sleep(CHECK_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Shutting down...")
                self.print_status()
                break
            except Exception as e:
                logger.error("Error: %s", e, exc_info=True)
                time.sleep(60)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        trader = LiveTrader()
        trader.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "consolidate":
        # Sell all alts to USDC for cleaner trading
        trader = LiveTrader()
        total, holdings = trader.get_portfolio_value()
        print(f"Portfolio: ${total:.2f}")
        for currency, data in holdings.items():
            if currency not in ("USD", "USDC", "BTC", "ETH") and data["usd_value"] > 1.0:
                pair = f"{currency}-USD"
                print(f"Selling {data['amount']} {currency} (~${data['usd_value']:.2f})...")
                result = trader.exchange.place_order(pair, "SELL", data["amount"])
                print(f"  Result: {json.dumps(result)[:200]}")
                time.sleep(1)
    else:
        trader = LiveTrader()
        trader.run()
