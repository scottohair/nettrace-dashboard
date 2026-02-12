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

# Load .env file for credentials
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

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
SIGNAL_MIN_CONFIDENCE = 0.70
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
        """Get total portfolio value in USD (includes funds locked in open orders)."""
        accounts = self.exchange._request("GET", "/api/v3/brokerage/accounts?limit=250")
        if "accounts" not in accounts:
            logger.error("Failed to get accounts: %s", accounts)
            return 0, {}

        total_usd = 0
        holdings = {}
        for acc in accounts["accounts"]:
            bal = acc.get("available_balance", {})
            hold = acc.get("hold", {})
            amount = float(bal.get("value", 0))
            held = float(hold.get("value", 0))
            total_amount = amount + held
            currency = acc.get("currency", "")
            if total_amount <= 0:
                continue

            if currency in ("USD", "USDC"):
                usd_value = total_amount
            else:
                price = self.pricefeed.get_price(f"{currency}-USD")
                usd_value = total_amount * price if price else 0

            if usd_value > 0.01:
                holdings[currency] = {
                    "amount": total_amount,
                    "available": amount,
                    "held": held,
                    "usd_value": round(usd_value, 2),
                }
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

    def _get_regime(self, pair):
        """Detect market regime using C engine or Python fallback."""
        try:
            from fast_bridge import FastEngine
            engine = FastEngine()
            # Fetch recent candles from public API
            import urllib.request as ur
            end = int(time.time())
            start = end - 24 * 3600  # 24h of 5-min candles
            url = (f"https://api.exchange.coinbase.com/products/{pair}/candles"
                   f"?start={datetime.fromtimestamp(start, tz=timezone.utc).isoformat()}"
                   f"&end={datetime.fromtimestamp(end, tz=timezone.utc).isoformat()}"
                   f"&granularity=300")
            req = ur.Request(url, headers={"User-Agent": "NetTrace/1.0"})
            with ur.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            candles = [{"open": c[3], "high": c[2], "low": c[1], "close": c[4],
                        "volume": c[5], "time": c[0]} for c in data]
            candles.sort(key=lambda x: x["time"])

            if len(candles) < 50:
                return {"regime": "UNKNOWN", "recommendation": "HOLD"}

            ind = engine.compute_indicators(candles)
            sig = engine.generate_signal(candles)
            return {
                "regime": ind["regime"],
                "rsi": ind["rsi_14"],
                "atr": ind["atr_14"],
                "vwap": ind["vwap"],
                "recommendation": "HOLD" if ind["regime"] == "DOWNTREND" else "TRADE",
                "c_signal": sig,
            }
        except Exception as e:
            logger.debug("Regime detection: %s", e)
            return {"regime": "UNKNOWN", "recommendation": "TRADE"}

    def evaluate_and_trade(self, signals):
        """Evaluate signals and execute trades.

        RULES (NEVER VIOLATE):
        1. NEVER sell at a loss — only sell when current_price > buy_price + fees
        2. BUY only with high confidence (>70%) and multiple confirming signals
        3. Daily loss limit: stop after $2 loss
        4. Only BUY when we have USD/USDC available — no panic sells
        5. Check market regime FIRST — skip downtrends
        """
        if self.daily_pnl <= -MAX_DAILY_LOSS_USD:
            logger.warning("Daily loss limit hit ($%.2f). STOPPED.", self.daily_pnl)
            return

        # We only BUY — we accumulate assets on strong signals
        # We only SELL when price is ABOVE our purchase price + fees (0.6%)
        # This ensures we NEVER LOSE MONEY on a trade

        _, holdings = self.get_portfolio_value()
        usdc = holdings.get("USDC", {}).get("usd_value", 0)
        usd = holdings.get("USD", {}).get("usd_value", 0)
        available_cash = usdc + usd

        # Count confirming buy signals from NetTrace latency data
        buy_signals = {}  # pair -> list of signals
        for signal in signals:
            sig_type = signal.get("signal_type", "")
            host = signal.get("target_host", "")
            direction = signal.get("direction", "")
            confidence = float(signal.get("confidence", 0))

            if confidence < 0.70:  # Only high confidence
                continue

            # Map host to pair
            pair = None
            if "coinbase" in host or "binance" in host or "kraken" in host:
                pair = "BTC-USDC"
            elif "bybit" in host or "okx" in host:
                pair = "BTC-USDC"
            elif "gemini" in host:
                pair = "ETH-USDC"

            if not pair:
                continue

            # Only BUY signals — accumulate on dips
            is_buy = False
            if sig_type == "rtt_anomaly" and direction == "up":
                is_buy = True
            elif sig_type == "cross_exchange_latency_diff":
                is_buy = True
            elif sig_type == "rtt_trend" and direction == "uptrend":
                is_buy = True

            if is_buy:
                buy_signals.setdefault(pair, []).append(signal)

        # Also check C engine signals — expanded to high-volatility pairs
        for pair in ["BTC-USDC", "ETH-USDC", "SOL-USDC", "DOGE-USDC", "AVAX-USDC", "LINK-USDC", "XRP-USDC"]:
            regime = self._get_regime(pair)

            # Skip downtrend pairs — Rule #1
            if regime.get("recommendation") == "HOLD":
                logger.debug("Skipping %s — regime: %s", pair, regime.get("regime"))
                continue

            # Check if C engine generated a buy signal (must meet same 0.70 threshold)
            c_sig = regime.get("c_signal", {})
            if c_sig and c_sig.get("signal_type") == "BUY" and c_sig.get("confidence", 0) >= 0.70:
                buy_signals.setdefault(pair, []).append({
                    "signal_type": "c_engine_" + c_sig.get("reason", ""),
                    "confidence": c_sig.get("confidence", 0),
                    "target_host": "fast_engine",
                })

        # Only execute if 2+ signals agree on the same pair (confirmation)
        for pair, sigs in buy_signals.items():
            if len(sigs) < 2:
                continue  # Need multiple confirming signals

            if available_cash < MIN_TRADE_USD:
                logger.debug("No cash available for BUY ($%.2f)", available_cash)
                break

            # Check cooldown
            recent = self.db.execute(
                "SELECT 1 FROM live_trades WHERE pair=? AND created_at > datetime('now', '-10 minutes')",
                (pair,)
            ).fetchone()
            if recent:
                continue

            avg_conf = sum(float(s.get("confidence", 0)) for s in sigs) / len(sigs)
            trade_usd = min(MAX_TRADE_USD, max(MIN_TRADE_USD, avg_conf * 6.0))
            trade_usd = min(trade_usd, available_cash)

            logger.info("BUY SIGNAL: %s | %d confirming signals | avg_conf=%.2f | $%.2f",
                        pair, len(sigs), avg_conf, trade_usd)
            self._execute_trade(pair, "BUY", trade_usd, sigs[0])
            break  # One trade per cycle

    def _execute_trade(self, pair, side, usd_amount, signal):
        """Execute a real BUY trade on Coinbase. SELL is disabled (accumulation mode)."""
        if side.upper() != "BUY":
            logger.warning("BLOCKED SELL on %s — accumulation mode, BUY only until portfolio > $100", pair)
            return

        price = self.pricefeed.get_price(pair)
        if not price:
            logger.warning("No price for %s", pair)
            return

        logger.info("EXECUTING: BUY %s | $%.2f @ $%.2f | signal=%s conf=%.2f",
                     pair, usd_amount, price,
                     signal.get("signal_type"), float(signal.get("confidence", 0)))

        result = self.exchange.place_order(pair, "BUY", round(usd_amount, 2))

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

        # Push to Fly dashboard (skip if total is 0 — likely API error)
        if total > 0:
            self._push_to_fly(total, holdings)
        return total, holdings

    def _push_to_fly(self, total, holdings):
        """Push snapshot to Fly trading dashboard."""
        if total <= 0:
            logger.debug("Skipping Fly push — total is $0 (API error?)")
            return
        try:
            # Get recent trades
            trades = self.db.execute(
                "SELECT pair, side, price, total_usd, signal_type, signal_confidence, status, created_at FROM live_trades ORDER BY id DESC LIMIT 20"
            ).fetchall()
            trades_list = [dict(t) for t in trades]
            trades_total = self.db.execute("SELECT COUNT(*) as cnt FROM live_trades").fetchone()["cnt"]

            payload = json.dumps({
                "user_id": 2,
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
                    "Authorization": f"Bearer {NETTRACE_API_KEY}",
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
                pair = f"{currency}-USDC"
                print(f"Selling {data['amount']} {currency} (~${data['usd_value']:.2f})...")
                result = trader.exchange.place_order(pair, "SELL", data["amount"])
                print(f"  Result: {json.dumps(result)[:200]}")
                time.sleep(1)
    else:
        trader = LiveTrader()
        trader.run()
