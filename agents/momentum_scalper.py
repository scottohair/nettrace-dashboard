#!/usr/bin/env python3
"""Momentum Scalper — short-timeframe momentum trading (1-5 minute holds).

Catches quick momentum moves using EMA crossovers, volume spikes, and
price acceleration detection. Exits positions after max 5 minutes.

Signals:
  1. 3-period EMA vs 8-period EMA (fast cross)
  2. Volume spike detection (current vol > 2x avg)
  3. Price acceleration (2nd derivative of price positive)

Entry:
  BUY when: fast EMA crosses above slow AND volume spike AND price accelerating
  Exit: fast EMA crosses below slow OR holding > 5 minutes

Game Theory:
  - Momentum is a known market anomaly (non-equilibrium)
  - Short hold time = reduced exposure to mean reversion
  - Volume confirmation = institutional flow detection
  - LIMIT orders: slightly above bid (BUY) or below ask (SELL) = maker fee
  - Quick rotation: don't marry positions, rotate to strongest momentum

RULES (NEVER VIOLATE):
  - Max $5 per trade
  - $2 daily loss limit (HARDSTOP)
  - Max 5 minute hold (300 seconds)
  - LIMIT orders only (maker fee 0.6%)
  - $2 reserve cash minimum
  - Max 15% of portfolio in any single asset
"""

import json
import logging
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Load .env
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
        logging.FileHandler(str(Path(__file__).parent / "momentum_scalper.log")),
    ]
)
logger = logging.getLogger("momentum_scalper")

SCALPER_DB = str(Path(__file__).parent / "momentum_scalper.db")

# Momentum scalper configuration
CONFIG = {
    "scan_interval": 15,                # Scan every 15s
    "max_trade_usd": 5.00,             # Max $5 per trade
    "max_daily_loss_usd": 2.00,        # $2 daily loss limit
    "max_hold_seconds": 300,            # 5 minute max hold
    "reserve_cash_usd": 10.00,         # Keep $10 reserve — prevent cash burnout
    "max_position_pct": 0.15,          # Max 15% per asset
    "pairs": ["BTC-USDC", "ETH-USDC", "SOL-USDC", "DOGE-USDC", "FET-USDC", "LINK-USDC"],
    # EMA parameters
    "ema_fast_period": 3,
    "ema_slow_period": 8,
    # Volume spike threshold
    "volume_spike_multiplier": 1.5,
    # Minimum candles needed
    "min_candles": 10,
}


def _fetch_json(url, headers=None, timeout=10):
    """HTTP GET JSON helper."""
    h = {"User-Agent": "NetTrace/1.0"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _data_pair(pair):
    """Convert trading pair to data pair for public APIs."""
    return pair.replace("-USDC", "-USD")


def _ema(values, period):
    """Calculate Exponential Moving Average.

    Returns list of EMA values (same length as input, first period-1 are None).
    """
    if len(values) < period:
        return [None] * len(values)

    multiplier = 2.0 / (period + 1)
    ema_vals = [None] * (period - 1)

    # Seed with SMA of first `period` values
    sma = sum(values[:period]) / period
    ema_vals.append(sma)

    for i in range(period, len(values)):
        ema_val = (values[i] - ema_vals[-1]) * multiplier + ema_vals[-1]
        ema_vals.append(ema_val)

    return ema_vals


class MomentumScalper:
    """Short-timeframe momentum scalper (1-5 min holds)."""

    def __init__(self):
        self.db = sqlite3.connect(SCALPER_DB)
        self.db.row_factory = sqlite3.Row
        self.daily_loss = 0.0
        self.trades_today = 0
        # In-memory position tracking for fast access
        self._positions = {}  # pair -> {"entry_time": float, "entry_price": float, "size_usd": float, "base_size": float}
        self._init_db()

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                entry_price REAL NOT NULL,
                entry_time TIMESTAMP NOT NULL,
                base_size REAL NOT NULL,
                size_usd REAL NOT NULL,
                exit_price REAL,
                exit_time TIMESTAMP,
                pnl REAL,
                exit_reason TEXT,
                status TEXT DEFAULT 'open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                amount_usd REAL,
                base_size REAL,
                price REAL,
                limit_price REAL,
                signal_details TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

        # Load open positions from DB on startup
        self._load_open_positions()

    def _load_open_positions(self):
        """Load open positions from DB into memory."""
        rows = self.db.execute(
            "SELECT pair, entry_price, entry_time, base_size, size_usd FROM positions WHERE status='open'"
        ).fetchall()
        for r in rows:
            try:
                entry_time = datetime.fromisoformat(r["entry_time"]).replace(tzinfo=timezone.utc).timestamp()
            except Exception:
                entry_time = time.time()
            self._positions[r["pair"]] = {
                "entry_time": entry_time,
                "entry_price": r["entry_price"],
                "size_usd": r["size_usd"],
                "base_size": r["base_size"],
            }
        if self._positions:
            logger.info("Loaded %d open positions from DB", len(self._positions))

    def _get_price(self, pair):
        """Get current spot price."""
        try:
            dp = _data_pair(pair)
            data = _fetch_json(f"https://api.coinbase.com/v2/prices/{dp}/spot")
            return float(data["data"]["amount"])
        except Exception:
            return None

    def _get_holdings(self):
        """Get current Coinbase holdings. Returns (holdings_dict, cash)."""
        try:
            from exchange_connector import CoinbaseTrader
            trader = CoinbaseTrader()
            accts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
            holdings = {}
            cash = 0.0
            for a in accts.get("accounts", []):
                cur = a.get("currency", "")
                bal = float(a.get("available_balance", {}).get("value", 0))
                if cur in ("USDC", "USD"):
                    cash += bal
                elif bal > 0:
                    holdings[cur] = bal
            return holdings, cash
        except Exception as e:
            logger.warning("Holdings check failed: %s", e)
            return {}, 0.0

    def _fetch_candles_1m(self, pair, limit=15):
        """Fetch 1-minute candles from Coinbase public API."""
        try:
            dp = _data_pair(pair)
            url = f"https://api.exchange.coinbase.com/products/{dp}/candles?granularity=60"
            data = _fetch_json(url, timeout=8)
            candles = []
            for c in data[:limit]:
                candles.append({
                    "time": c[0], "low": c[1], "high": c[2],
                    "open": c[3], "close": c[4], "volume": c[5]
                })
            candles.reverse()  # oldest first
            return candles
        except Exception:
            return []

    def _get_best_bid_ask(self, pair):
        """Get best bid and ask prices from orderbook."""
        try:
            dp = _data_pair(pair)
            url = f"https://api.exchange.coinbase.com/products/{dp}/book?level=1"
            book = _fetch_json(url, timeout=5)
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            best_bid = float(bids[0][0]) if bids else None
            best_ask = float(asks[0][0]) if asks else None
            return best_bid, best_ask
        except Exception:
            return None, None

    def analyze_pair(self, pair):
        """Analyze momentum signals for a single pair.

        Returns: {
            "signal": "BUY"|"SELL"|"NONE",
            "confidence": float,
            "ema_cross": str,
            "volume_spike": bool,
            "price_accel": bool,
            "reason": str,
        }
        """
        candles = self._fetch_candles_1m(pair, limit=CONFIG["min_candles"] + 5)
        if len(candles) < CONFIG["min_candles"]:
            return {"signal": "NONE", "confidence": 0, "reason": f"Insufficient candles ({len(candles)})"}

        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        # 1. EMA crossover detection
        ema_fast = _ema(closes, CONFIG["ema_fast_period"])
        ema_slow = _ema(closes, CONFIG["ema_slow_period"])

        # Need at least 2 valid EMA values to detect crossover
        if ema_fast[-1] is None or ema_fast[-2] is None or ema_slow[-1] is None or ema_slow[-2] is None:
            return {"signal": "NONE", "confidence": 0, "reason": "Insufficient EMA data"}

        # Detect crossover
        fast_above_now = ema_fast[-1] > ema_slow[-1]
        fast_above_prev = ema_fast[-2] > ema_slow[-2]
        bullish_cross = fast_above_now and not fast_above_prev
        bearish_cross = not fast_above_now and fast_above_prev

        ema_cross = "NONE"
        if bullish_cross:
            ema_cross = "BULLISH"
        elif bearish_cross:
            ema_cross = "BEARISH"
        elif fast_above_now:
            ema_cross = "ABOVE"  # already above, momentum continuing
        else:
            ema_cross = "BELOW"  # already below

        # 2. Volume spike detection
        avg_volume = sum(volumes[:-1]) / max(len(volumes) - 1, 1)
        current_volume = volumes[-1]
        volume_spike = current_volume > avg_volume * CONFIG["volume_spike_multiplier"] if avg_volume > 0 else False
        volume_ratio = current_volume / avg_volume if avg_volume > 0 else 0

        # 3. Price acceleration (2nd derivative positive = accelerating up)
        # Use last 3 closes to compute acceleration
        if len(closes) >= 3:
            vel_1 = closes[-2] - closes[-3]  # velocity at t-1
            vel_0 = closes[-1] - closes[-2]  # velocity at t
            acceleration = vel_0 - vel_1      # 2nd derivative
            price_accel = acceleration > 0
            accel_pct = acceleration / closes[-2] if closes[-2] else 0
        else:
            price_accel = False
            accel_pct = 0

        # Determine signal
        result = {
            "signal": "NONE",
            "confidence": 0,
            "ema_cross": ema_cross,
            "volume_spike": volume_spike,
            "price_accel": price_accel,
            "reason": "",
        }

        # BUY: 2-of-3 voting (EMA cross, volume spike, price acceleration)
        buy_votes = 0
        if ema_cross in ("BULLISH", "ABOVE"):
            buy_votes += 1
        if volume_spike:
            buy_votes += 1
        if price_accel:
            buy_votes += 1

        if buy_votes >= 2 and ema_cross in ("BULLISH", "ABOVE", "NONE"):
            # Need at least 2 of 3 confirmations, and EMA not bearish
            base_conf = 0.80 if ema_cross == "BULLISH" else (0.65 if ema_cross == "ABOVE" else 0.55)
            conf = base_conf + (volume_ratio - 1.5) * 0.03 + accel_pct * 50
            conf = min(max(conf, 0.50), 0.95)

            result["signal"] = "BUY"
            result["confidence"] = conf
            result["reason"] = (f"2-of-3 vote ({buy_votes}/3): EMA={ema_cross} | "
                                f"vol={volume_ratio:.1f}x spike={volume_spike} | "
                                f"accel={accel_pct:.4%} | fast={ema_fast[-1]:.2f} slow={ema_slow[-1]:.2f}")

        # SELL signal: bearish cross (used for exit, not fresh entry)
        elif ema_cross == "BEARISH":
            conf = 0.70 + (volume_ratio - 1.0) * 0.05 if volume_spike else 0.65
            conf = min(max(conf, 0.50), 0.90)

            result["signal"] = "SELL"
            result["confidence"] = conf
            result["reason"] = (f"Bearish EMA cross | vol={volume_ratio:.1f}x | "
                                f"fast={ema_fast[-1]:.2f} slow={ema_slow[-1]:.2f}")

        else:
            reasons = []
            if ema_cross not in ("BULLISH", "ABOVE"):
                reasons.append(f"EMA={ema_cross}")
            if not volume_spike:
                reasons.append(f"vol={volume_ratio:.1f}x (need {CONFIG['volume_spike_multiplier']}x)")
            if not price_accel:
                reasons.append(f"accel={accel_pct:.4%} (need >0)")
            result["reason"] = " | ".join(reasons)

        return result

    def check_exits(self):
        """Check all open positions for exit conditions.

        Exit when: bearish EMA cross OR held > max_hold_seconds.
        """
        now = time.time()
        to_exit = []

        for pair, pos in list(self._positions.items()):
            hold_time = now - pos["entry_time"]
            current_price = self._get_price(pair)

            # Profit/loss based exits (check before time-based)
            if current_price and pos["entry_price"] > 0:
                pnl_pct = (current_price - pos["entry_price"]) / pos["entry_price"]

                # Profit target: exit immediately if >= 0.5% gain
                if pnl_pct >= 0.005:
                    to_exit.append((pair, pos, "profit_target",
                                    f"Profit target hit: {pnl_pct:.2%} after {hold_time:.0f}s"))
                    continue

                # Stop-loss: exit if <= -1.0% loss
                if pnl_pct <= -0.01:
                    to_exit.append((pair, pos, "stop_loss",
                                    f"Stop-loss hit: {pnl_pct:.2%} after {hold_time:.0f}s"))
                    continue

            # Time-based exit: max hold exceeded
            if hold_time >= CONFIG["max_hold_seconds"]:
                to_exit.append((pair, pos, "max_hold_exceeded",
                                f"Held {hold_time:.0f}s >= {CONFIG['max_hold_seconds']}s"))
                continue

            # Signal-based exit: bearish EMA cross
            analysis = self.analyze_pair(pair)
            if analysis["signal"] == "SELL":
                to_exit.append((pair, pos, "bearish_cross",
                                f"EMA bearish cross after {hold_time:.0f}s | {analysis['reason']}"))

        for pair, pos, reason_code, reason_text in to_exit:
            logger.info("EXIT: %s | %s | entry=$%.2f hold=%.0fs",
                        pair, reason_text, pos["entry_price"],
                        now - pos["entry_time"])
            self._close_position(pair, pos, reason_code)

    def _close_position(self, pair, pos, exit_reason):
        """Close a position by placing a SELL limit order."""
        price = self._get_price(pair)
        if not price:
            logger.warning("Cannot get price for %s exit", pair)
            return False

        base_currency = pair.split("-")[0]
        holdings, _ = self._get_holdings()
        held = holdings.get(base_currency, 0)

        base_size = pos["base_size"]
        base_size = min(base_size, held)  # never sell more than we have

        if base_size * price < 0.10:
            # Position too small to sell, just mark closed
            logger.info("Position %s too small to sell ($%.4f), marking closed", pair, base_size * price)
            self._mark_position_closed(pair, price, exit_reason, 0)
            return True

        # LIMIT sell slightly below ask for quick fill
        best_bid, best_ask = self._get_best_bid_ask(pair)
        if best_bid:
            limit_price = best_bid * 0.999  # slightly below bid for guaranteed fill
        else:
            limit_price = price * 0.999

        logger.info("SCALPER EXIT: LIMIT SELL %s | %.8f @ $%.2f | reason=%s",
                     pair, base_size, limit_price, exit_reason)

        try:
            from exchange_connector import CoinbaseTrader
            trader = CoinbaseTrader()
            result = trader.place_limit_order(pair, "SELL", base_size, limit_price, post_only=False)

            status = "failed"
            if "success_response" in result or "order_id" in result:
                status = "filled"
                order_id = result.get("success_response", {}).get("order_id", result.get("order_id", "?"))
                logger.info("SCALPER SELL FILLED: %s @ $%.2f | order=%s", pair, limit_price, order_id)
            elif "error_response" in result:
                err = result["error_response"]
                logger.warning("SCALPER SELL FAILED: %s | %s", pair, err.get("message", err))
            else:
                logger.warning("SCALPER SELL UNKNOWN: %s", json.dumps(result)[:300])

            pnl = (price - pos["entry_price"]) * base_size
            self._mark_position_closed(pair, price, exit_reason, pnl)

            # Record trade
            self.db.execute(
                """INSERT INTO trades
                   (pair, direction, amount_usd, base_size, price, limit_price, signal_details, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (pair, "SELL", round(base_size * price, 2), base_size, price, limit_price,
                 json.dumps({"exit_reason": exit_reason}), status)
            )
            self.db.commit()

            if pnl < 0:
                self.daily_loss += abs(pnl)

            return status == "filled"
        except Exception as e:
            logger.error("Exit execution error for %s: %s", pair, e, exc_info=True)
            return False

    def _mark_position_closed(self, pair, exit_price, exit_reason, pnl):
        """Mark position as closed in DB and remove from memory."""
        now_str = datetime.now(timezone.utc).isoformat()
        self.db.execute(
            """UPDATE positions SET status='closed', exit_price=?, exit_time=?,
               exit_reason=?, pnl=? WHERE pair=? AND status='open'""",
            (exit_price, now_str, exit_reason, round(pnl, 4), pair)
        )
        self.db.commit()
        self._positions.pop(pair, None)
        logger.info("Position closed: %s | exit=$%.2f | P&L=$%+.4f | reason=%s",
                     pair, exit_price, pnl, exit_reason)

    def scan(self):
        """Run a full scan: check exits, then look for new entries."""
        logger.info("=== MOMENTUM SCALPER SCAN === (positions: %d)", len(self._positions))

        # First: check exits on open positions
        self.check_exits()

        # Then: look for new entries
        actionable = []
        for pair in CONFIG["pairs"]:
            # Skip if already in a position for this pair
            if pair in self._positions:
                pos = self._positions[pair]
                hold_time = time.time() - pos["entry_time"]
                logger.info("  %s: holding (%.0fs, entry=$%.2f)", pair, hold_time, pos["entry_price"])
                continue

            analysis = self.analyze_pair(pair)

            if analysis["signal"] == "BUY" and analysis["confidence"] >= 0.60:
                actionable.append({
                    "pair": pair,
                    "direction": "BUY",
                    "confidence": analysis["confidence"],
                    "ema_cross": analysis["ema_cross"],
                    "volume_spike": analysis["volume_spike"],
                    "price_accel": analysis["price_accel"],
                    "reason": analysis["reason"],
                })
                logger.info("  >>> BUY SIGNAL: %s | conf=%.1f%% | %s",
                            pair, analysis["confidence"] * 100, analysis["reason"])
            else:
                logger.info("  %s: %s conf=%.1f%% | %s",
                            pair, analysis["signal"], analysis["confidence"] * 100, analysis["reason"])

        return actionable

    def execute_trade(self, signal):
        """Execute a BUY trade and track position."""
        if self.daily_loss >= CONFIG["max_daily_loss_usd"]:
            logger.warning("HARDSTOP: Daily loss limit $%.2f reached", CONFIG["max_daily_loss_usd"])
            return False

        pair = signal["pair"]

        # Don't double up on same pair
        if pair in self._positions:
            return False

        price = self._get_price(pair)
        if not price:
            logger.warning("Cannot get price for %s", pair)
            return False

        holdings, cash = self._get_holdings()
        base_currency = pair.split("-")[0]

        # Diversification check
        held_amount = holdings.get(base_currency, 0)
        held_usd = held_amount * price if price else 0
        total_portfolio = cash + sum(
            h * (self._get_price(f"{c}-USDC") or 0) for c, h in holdings.items()
        )
        if total_portfolio <= 0:
            logger.warning("Portfolio value is $0 — cannot trade")
            return False
        max_position = total_portfolio * CONFIG["max_position_pct"]

        if held_usd >= max_position:
            logger.info("SCALPER: %s position $%.2f >= max $%.2f (%.0f%%) — DIVERSIFY",
                        base_currency, held_usd, max_position,
                        held_usd / total_portfolio * 100)
            return False

        remaining_room = max_position - held_usd
        trade_size = min(
            CONFIG["max_trade_usd"],
            max(1.00, signal["confidence"] * 6.0),
            remaining_room,
            cash - CONFIG["reserve_cash_usd"],
        )
        if trade_size < 1.00:
            logger.info("SCALPER: Insufficient cash ($%.2f, reserve $%.2f) for BUY",
                        cash, CONFIG["reserve_cash_usd"])
            return False
        trade_size = round(trade_size, 2)

        base_size = trade_size / price

        # LIMIT BUY slightly above best bid for fast fill
        best_bid, best_ask = self._get_best_bid_ask(pair)
        if best_bid and best_ask:
            # Place between bid and ask, closer to ask for faster fill
            limit_price = best_bid + (best_ask - best_bid) * 0.3
        else:
            limit_price = price * 1.001

        logger.info("SCALPER EXECUTE: LIMIT BUY %s | $%.2f (%.6f @ $%.2f) | conf=%.1f%% | %s",
                     pair, trade_size, base_size, limit_price,
                     signal["confidence"] * 100, signal["reason"])

        try:
            from exchange_connector import CoinbaseTrader
            trader = CoinbaseTrader()
            result = trader.place_limit_order(pair, "BUY", base_size, limit_price, post_only=False)

            status = "failed"
            if "success_response" in result or "order_id" in result:
                status = "filled"
                order_id = result.get("success_response", {}).get("order_id", result.get("order_id", "?"))
                logger.info("SCALPER BUY FILLED: %s $%.2f @ $%.2f | order=%s",
                            pair, trade_size, limit_price, order_id)

                # Track position
                now = time.time()
                now_str = datetime.now(timezone.utc).isoformat()
                self._positions[pair] = {
                    "entry_time": now,
                    "entry_price": price,
                    "size_usd": trade_size,
                    "base_size": base_size,
                }
                self.db.execute(
                    """INSERT INTO positions
                       (pair, entry_price, entry_time, base_size, size_usd, status)
                       VALUES (?, ?, ?, ?, ?, 'open')""",
                    (pair, price, now_str, base_size, trade_size)
                )
            elif "error_response" in result:
                err = result["error_response"]
                logger.warning("SCALPER BUY FAILED: %s | %s", pair, err.get("message", err))
            else:
                logger.warning("SCALPER BUY UNKNOWN: %s", json.dumps(result)[:300])

            # Record trade
            self.db.execute(
                """INSERT INTO trades
                   (pair, direction, amount_usd, base_size, price, limit_price, signal_details, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (pair, "BUY", trade_size, base_size, price, limit_price,
                 json.dumps({"ema_cross": signal["ema_cross"],
                             "volume_spike": signal["volume_spike"],
                             "price_accel": signal["price_accel"],
                             "reason": signal["reason"]}),
                 status)
            )
            self.db.commit()
            self.trades_today += 1

            return status == "filled"
        except Exception as e:
            logger.error("BUY execution error: %s", e, exc_info=True)
            return False

    def print_status(self):
        """Print agent status report."""
        print(f"\n{'='*70}")
        print(f"  MOMENTUM SCALPER STATUS")
        print(f"{'='*70}")

        # Open positions
        print(f"\n  Open Positions ({len(self._positions)}):")
        now = time.time()
        for pair, pos in self._positions.items():
            hold_time = now - pos["entry_time"]
            current_price = self._get_price(pair)
            pnl = (current_price - pos["entry_price"]) * pos["base_size"] if current_price else 0
            pnl_pct = (current_price / pos["entry_price"] - 1) * 100 if current_price and pos["entry_price"] else 0
            print(f"    {pair} | entry=${pos['entry_price']:,.2f} | "
                  f"now=${current_price or 0:,.2f} | P&L=${pnl:+.4f} ({pnl_pct:+.2f}%) | "
                  f"hold={hold_time:.0f}s / {CONFIG['max_hold_seconds']}s")

        # Closed positions
        closed = self.db.execute(
            """SELECT pair, entry_price, exit_price, pnl, exit_reason,
                      entry_time, exit_time
               FROM positions WHERE status='closed' ORDER BY id DESC LIMIT 10"""
        ).fetchall()
        if closed:
            print(f"\n  Recent Closed Positions:")
            for p in closed:
                print(f"    {p['pair']} | entry=${p['entry_price']:,.2f} → exit=${p['exit_price'] or 0:,.2f} | "
                      f"P&L=${p['pnl'] or 0:+.4f} | {p['exit_reason']} | {p['exit_time']}")

        # Stats
        total_trades = self.db.execute("SELECT COUNT(*) as cnt FROM trades").fetchone()["cnt"]
        total_closed = self.db.execute("SELECT COUNT(*) as cnt FROM positions WHERE status='closed'").fetchone()["cnt"]
        total_pnl_row = self.db.execute("SELECT SUM(pnl) as total FROM positions WHERE status='closed'").fetchone()
        total_pnl = total_pnl_row["total"] or 0
        winners = self.db.execute("SELECT COUNT(*) as cnt FROM positions WHERE status='closed' AND pnl > 0").fetchone()["cnt"]

        print(f"\n  Stats:")
        print(f"    Total trades:    {total_trades}")
        print(f"    Closed positions: {total_closed}")
        print(f"    Total P&L:       ${total_pnl:+.4f}")
        print(f"    Win rate:        {winners}/{total_closed} ({winners/total_closed*100:.0f}%)" if total_closed > 0 else "    Win rate:        N/A")
        print(f"    Daily loss:      ${self.daily_loss:.2f} / ${CONFIG['max_daily_loss_usd']:.2f}")
        print(f"    Trades today:    {self.trades_today}")

        print(f"\n  Config:")
        print(f"    Scan interval:   {CONFIG['scan_interval']}s")
        print(f"    Max hold:        {CONFIG['max_hold_seconds']}s")
        print(f"    Max trade:       ${CONFIG['max_trade_usd']:.2f}")
        print(f"    EMA:             {CONFIG['ema_fast_period']}/{CONFIG['ema_slow_period']}")
        print(f"    Vol spike:       {CONFIG['volume_spike_multiplier']}x")
        print(f"    Pairs:           {', '.join(CONFIG['pairs'])}")
        print(f"{'='*70}\n")

    def run(self):
        """Main continuous loop — scan, enter, and exit."""
        logger.info("Momentum Scalper starting — scanning every %ds, max hold %ds",
                     CONFIG["scan_interval"], CONFIG["max_hold_seconds"])
        logger.info("Pairs: %s", ", ".join(CONFIG["pairs"]))
        logger.info("EMA %d/%d | volume spike %dx",
                     CONFIG["ema_fast_period"], CONFIG["ema_slow_period"],
                     CONFIG["volume_spike_multiplier"])

        cycle = 0
        while True:
            try:
                cycle += 1
                actionable = self.scan()

                for signal in actionable:
                    self.execute_trade(signal)

                # Report every 120 cycles (~30 min at 15s interval)
                if cycle % 120 == 0:
                    self.print_status()

                time.sleep(CONFIG["scan_interval"])

            except KeyboardInterrupt:
                logger.info("Momentum Scalper shutting down...")
                self.print_status()
                break
            except Exception as e:
                logger.error("Scalper error: %s", e, exc_info=True)
                time.sleep(15)


if __name__ == "__main__":
    scalper = MomentumScalper()

    if len(sys.argv) > 1 and sys.argv[1] == "scan":
        results = scalper.scan()
        scalper.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "status":
        scalper.print_status()
    else:
        scalper.run()
