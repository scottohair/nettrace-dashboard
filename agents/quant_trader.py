#!/usr/bin/env python3
"""NetTrace Quant Trader — trades on latency intelligence signals.

Strategy: When our scanner detects a latency anomaly or route change on a
crypto exchange, prices on that exchange may lag. We exploit the divergence.

Modes:
  paper  — simulate trades, track P&L (default)
  live   — execute real trades via exchange APIs

Edge: We see route changes and latency spikes 15 minutes before anyone
without multi-region scanning infrastructure.
"""

import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("quant_trader")

DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent.parent / "traceroute.db"))
TRADER_DB = str(Path(__file__).parent / "trader.db")

# Trading parameters
INITIAL_CAPITAL = float(os.environ.get("INITIAL_CAPITAL", "1000.0"))  # USD
POSITION_SIZE_PCT = 0.10  # risk 10% per trade
MAX_OPEN_POSITIONS = 3
SIGNAL_MIN_CONFIDENCE = 0.55
TAKE_PROFIT_PCT = 0.005  # 0.5% target
STOP_LOSS_PCT = 0.003    # 0.3% stop
COOLDOWN_MINUTES = 30    # min time between trades on same pair


class TradingEngine:
    """Paper + live trading engine driven by latency signals."""

    def __init__(self, mode="paper"):
        self.mode = mode
        self.db = None
        self._init_db()

    def _init_db(self):
        self.db = sqlite3.connect(TRADER_DB)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id INTEGER PRIMARY KEY,
                cash REAL NOT NULL,
                total_value REAL NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                side TEXT NOT NULL,  -- 'long' or 'short'
                entry_price REAL NOT NULL,
                quantity REAL NOT NULL,
                cost_basis REAL NOT NULL,
                signal_type TEXT,
                signal_confidence REAL,
                signal_details TEXT,
                status TEXT DEFAULT 'open',  -- open, closed, stopped
                exit_price REAL,
                pnl REAL,
                pnl_pct REAL,
                opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id INTEGER,
                pair TEXT NOT NULL,
                side TEXT NOT NULL,  -- 'buy' or 'sell'
                price REAL NOT NULL,
                quantity REAL NOT NULL,
                total REAL NOT NULL,
                fee REAL DEFAULT 0.0,
                mode TEXT NOT NULL,  -- 'paper' or 'live'
                executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (position_id) REFERENCES positions(id)
            );

            CREATE TABLE IF NOT EXISTS pnl_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_value REAL,
                cash REAL,
                unrealized_pnl REAL,
                realized_pnl REAL,
                win_count INTEGER,
                loss_count INTEGER,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS signal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_type TEXT,
                target_host TEXT,
                direction TEXT,
                confidence REAL,
                action_taken TEXT,  -- 'trade', 'skip', 'cooldown'
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Initialize portfolio if empty
        row = self.db.execute("SELECT id FROM portfolio LIMIT 1").fetchone()
        if not row:
            self.db.execute(
                "INSERT INTO portfolio (id, cash, total_value) VALUES (1, ?, ?)",
                (INITIAL_CAPITAL, INITIAL_CAPITAL)
            )
            self.db.commit()
            logger.info("Initialized portfolio with $%.2f", INITIAL_CAPITAL)

    def get_portfolio(self):
        return dict(self.db.execute("SELECT * FROM portfolio WHERE id=1").fetchone())

    def get_open_positions(self):
        rows = self.db.execute("SELECT * FROM positions WHERE status='open'").fetchall()
        return [dict(r) for r in rows]

    def get_latest_signals(self):
        """Pull latest signals from live NetTrace API or local DB."""
        # Try local DB first
        try:
            ndb = sqlite3.connect(DB_PATH)
            ndb.row_factory = sqlite3.Row
            rows = ndb.execute("""
                SELECT signal_type, target_host, target_name, direction,
                       confidence, details_json, created_at
                FROM quant_signals
                WHERE created_at >= datetime('now', '-1 hour')
                ORDER BY created_at DESC
                LIMIT 50
            """).fetchall()
            ndb.close()
            if rows:
                return [dict(r) for r in rows]
        except Exception:
            pass

        # Fallback: pull from live API
        try:
            import urllib.request
            # Use the live Fly API to get signals
            api_url = f"{FLY_API_URL}/api/v1/signals?limit=50"
            req = urllib.request.Request(api_url, headers={
                "User-Agent": "NetTrace-Trader/1.0"
            })
            # Add API key if available
            api_key = os.environ.get("NETTRACE_API_KEY", "")
            if api_key:
                req.add_header("Authorization", f"Bearer {api_key}")
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            signals = data.get("signals", [])
            logger.info("Fetched %d signals from live API", len(signals))
            return signals
        except Exception as e:
            logger.debug("Live API signal fetch failed: %s", e)
            return []

    def get_price(self, pair):
        """Get real market price from public exchange APIs."""
        from exchange_connector import PriceFeed
        # Normalize USDT pairs to USD for price lookup
        lookup_pair = pair.replace("-USDT", "-USD")
        return PriceFeed.get_price(lookup_pair)

    def _check_cooldown(self, pair):
        """Check if we traded this pair recently."""
        row = self.db.execute(
            """SELECT MAX(executed_at) as last FROM trades
               WHERE pair=? AND executed_at >= datetime('now', ?)""",
            (pair, f"-{COOLDOWN_MINUTES} minutes")
        ).fetchone()
        return bool(row and row["last"])

    def evaluate_signal(self, signal):
        """Decide whether to trade on a signal."""
        sig_type = signal["signal_type"]
        confidence = float(signal["confidence"])
        direction = signal["direction"]
        host = signal["target_host"]

        # Map exchange hosts to trading pairs
        pair_map = {
            "api.binance.com": "BTC-USDT",
            "api.coinbase.com": "BTC-USD",
            "api.kraken.com": "BTC-USD",
            "api.bybit.com": "BTC-USDT",
            "api.okx.com": "BTC-USDT",
        }
        pair = pair_map.get(host)
        if not pair:
            return None, "no_tradeable_pair"

        if confidence < SIGNAL_MIN_CONFIDENCE:
            return None, "low_confidence"

        # Check cooldown
        if self._check_cooldown(pair):
            return None, "cooldown"

        # Check max positions
        open_pos = self.get_open_positions()
        if len(open_pos) >= MAX_OPEN_POSITIONS:
            return None, "max_positions"

        # Don't open duplicate position on same pair
        for p in open_pos:
            if p["pair"] == pair:
                return None, "already_positioned"

        # Determine trade direction
        if sig_type == "cross_exchange_latency_diff":
            # Divergence signal: one exchange lagging
            if "favor_coinbase" in direction:
                return ("BTC-USD", "long"), "cross_exchange_arb"
            elif "favor_binance" in direction:
                return ("BTC-USDT", "long"), "cross_exchange_arb"

        elif sig_type == "rtt_anomaly":
            # Latency spike on exchange = prices may lag = buy opportunity
            if direction == "up":  # latency went up
                return (pair, "long"), "latency_spike_buy"
            elif direction == "down":  # latency dropped = exchange faster
                return (pair, "short"), "latency_drop_short"

        elif sig_type == "route_change_crypto_arb":
            if "latency_up" in direction:
                return (pair, "long"), "route_change_arb"
            elif "latency_down" in direction:
                return (pair, "short"), "route_change_arb"

        elif sig_type == "rtt_trend":
            if direction == "uptrend":
                return (pair, "long"), "trend_follow"
            elif direction == "downtrend":
                return (pair, "short"), "trend_follow"

        return None, "no_actionable_signal"

    def open_position(self, pair, side, signal):
        """Open a new position."""
        portfolio = self.get_portfolio()
        cash = portfolio["cash"]

        price = self.get_price(pair)
        if not price:
            logger.warning("Cannot get price for %s", pair)
            return None

        position_value = cash * POSITION_SIZE_PCT
        if position_value < 10:
            logger.warning("Insufficient capital: $%.2f", cash)
            return None

        quantity = position_value / price
        fee = position_value * 0.001  # 0.1% fee

        # Record position
        cur = self.db.execute(
            """INSERT INTO positions (pair, side, entry_price, quantity, cost_basis,
               signal_type, signal_confidence, signal_details)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (pair, side, price, quantity, position_value + fee,
             signal["signal_type"], signal["confidence"],
             json.dumps(signal.get("details_json", "{}")))
        )
        pos_id = cur.lastrowid

        # Record trade
        self.db.execute(
            """INSERT INTO trades (position_id, pair, side, price, quantity, total, fee, mode)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (pos_id, pair, "buy" if side == "long" else "sell",
             price, quantity, position_value, fee, self.mode)
        )

        # Update cash
        self.db.execute(
            "UPDATE portfolio SET cash = cash - ?, updated_at = ? WHERE id=1",
            (position_value + fee, datetime.now(timezone.utc).isoformat())
        )
        self.db.commit()

        logger.info("OPENED %s %s @ $%.2f | qty=%.6f | cost=$%.2f | fee=$%.2f",
                     side.upper(), pair, price, quantity, position_value, fee)
        return pos_id

    def check_exits(self):
        """Check if any open positions should be closed."""
        open_positions = self.get_open_positions()
        for pos in open_positions:
            current_price = self.get_price(pos["pair"])
            if not current_price:
                continue

            entry = pos["entry_price"]
            if pos["side"] == "long":
                pnl_pct = (current_price - entry) / entry
            else:
                pnl_pct = (entry - current_price) / entry

            # Take profit or stop loss
            if pnl_pct >= TAKE_PROFIT_PCT:
                self._close_position(pos, current_price, "take_profit")
            elif pnl_pct <= -STOP_LOSS_PCT:
                self._close_position(pos, current_price, "stop_loss")
            # Time-based exit: close after 2 hours
            elif pos["opened_at"]:
                try:
                    opened = datetime.fromisoformat(pos["opened_at"])
                    if (datetime.now() - opened).total_seconds() > 7200:
                        self._close_position(pos, current_price, "time_exit")
                except (ValueError, TypeError):
                    pass

    def _close_position(self, pos, exit_price, reason):
        """Close a position and realize P&L."""
        entry = pos["entry_price"]
        qty = pos["quantity"]

        if pos["side"] == "long":
            pnl = (exit_price - entry) * qty
        else:
            pnl = (entry - exit_price) * qty

        pnl_pct = pnl / pos["cost_basis"] * 100
        fee = abs(pnl) * 0.001  # exit fee
        net_pnl = pnl - fee

        # Update position
        self.db.execute(
            """UPDATE positions SET status='closed', exit_price=?, pnl=?,
               pnl_pct=?, closed_at=? WHERE id=?""",
            (exit_price, net_pnl, pnl_pct,
             datetime.now(timezone.utc).isoformat(), pos["id"])
        )

        # Record exit trade
        self.db.execute(
            """INSERT INTO trades (position_id, pair, side, price, quantity, total, fee, mode)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (pos["id"], pos["pair"],
             "sell" if pos["side"] == "long" else "buy",
             exit_price, qty, exit_price * qty, fee, self.mode)
        )

        # Return capital to cash
        self.db.execute(
            "UPDATE portfolio SET cash = cash + ?, updated_at = ? WHERE id=1",
            (pos["cost_basis"] + net_pnl, datetime.now(timezone.utc).isoformat())
        )

        # Update total value
        self._update_total_value()
        self.db.commit()

        emoji = "W" if net_pnl > 0 else "L"
        logger.info("CLOSED [%s] %s %s @ $%.2f → $%.2f | P&L: $%.2f (%.2f%%) | %s",
                     emoji, pos["side"].upper(), pos["pair"],
                     entry, exit_price, net_pnl, pnl_pct, reason)

    def _update_total_value(self):
        """Recalculate portfolio total value."""
        portfolio = self.get_portfolio()
        cash = portfolio["cash"]
        unrealized = 0
        for pos in self.get_open_positions():
            price = self.get_price(pos["pair"])
            if price:
                if pos["side"] == "long":
                    unrealized += (price - pos["entry_price"]) * pos["quantity"]
                else:
                    unrealized += (pos["entry_price"] - price) * pos["quantity"]
        total = cash + unrealized
        self.db.execute(
            "UPDATE portfolio SET total_value=?, updated_at=? WHERE id=1",
            (total, datetime.now(timezone.utc).isoformat())
        )

    def record_snapshot(self):
        """Record P&L snapshot for history."""
        portfolio = self.get_portfolio()
        closed = self.db.execute(
            "SELECT COALESCE(SUM(pnl), 0) as realized, "
            "SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins, "
            "SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses "
            "FROM positions WHERE status='closed'"
        ).fetchone()
        self.db.execute(
            """INSERT INTO pnl_history (total_value, cash, unrealized_pnl,
               realized_pnl, win_count, loss_count) VALUES (?, ?, ?, ?, ?, ?)""",
            (portfolio["total_value"], portfolio["cash"],
             portfolio["total_value"] - portfolio["cash"],
             closed["realized"], closed["wins"] or 0, closed["losses"] or 0)
        )
        self.db.commit()

    def print_status(self):
        """Print current trading status."""
        portfolio = self.get_portfolio()
        open_pos = self.get_open_positions()
        closed = self.db.execute(
            "SELECT COUNT(*) as cnt, COALESCE(SUM(pnl), 0) as total_pnl, "
            "SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins "
            "FROM positions WHERE status='closed'"
        ).fetchone()

        pnl_total = closed["total_pnl"]
        roi = ((portfolio["total_value"] - INITIAL_CAPITAL) / INITIAL_CAPITAL) * 100

        print("\n" + "=" * 60)
        print(f"  QUANT TRADER [{self.mode.upper()}] | ROI: {roi:+.2f}%")
        print("=" * 60)
        print(f"  Capital:    ${INITIAL_CAPITAL:.2f}")
        print(f"  Cash:       ${portfolio['cash']:.2f}")
        print(f"  Total:      ${portfolio['total_value']:.2f}")
        print(f"  P&L:        ${pnl_total:+.2f}")
        print(f"  Trades:     {closed['cnt']} ({closed['wins'] or 0}W / {(closed['cnt'] or 0) - (closed['wins'] or 0)}L)")
        if closed["cnt"] and closed["cnt"] > 0:
            print(f"  Win Rate:   {((closed['wins'] or 0) / closed['cnt']) * 100:.0f}%")
        print(f"  Open:       {len(open_pos)} positions")
        for p in open_pos:
            price = self.get_price(p["pair"])
            if price:
                if p["side"] == "long":
                    upnl = (price - p["entry_price"]) * p["quantity"]
                else:
                    upnl = (p["entry_price"] - price) * p["quantity"]
                print(f"    {p['side'].upper()} {p['pair']} @ ${p['entry_price']:.2f} → ${price:.2f} | ${upnl:+.2f}")
        print("=" * 60 + "\n")

    def run(self):
        """Main trading loop."""
        logger.info("Quant Trader starting in %s mode | Capital: $%.2f", self.mode, INITIAL_CAPITAL)

        cycle = 0
        while True:
            try:
                cycle += 1

                # 1. Check exits on open positions
                self.check_exits()

                # 2. Get new signals
                signals = self.get_latest_signals()

                # 3. Evaluate each signal
                for signal in signals:
                    trade_decision, reason = self.evaluate_signal(signal)

                    self.db.execute(
                        """INSERT INTO signal_log (signal_type, target_host, direction,
                           confidence, action_taken, reason) VALUES (?, ?, ?, ?, ?, ?)""",
                        (signal["signal_type"], signal["target_host"],
                         signal["direction"], signal["confidence"],
                         "trade" if trade_decision else "skip", reason)
                    )

                    if trade_decision:
                        pair, side = trade_decision
                        self.open_position(pair, side, signal)

                self.db.commit()

                # 4. Update portfolio value
                self._update_total_value()
                self.db.commit()

                # 5. Print status every 10 cycles
                if cycle % 10 == 0:
                    self.record_snapshot()
                    self.print_status()

                # Sleep between cycles
                time.sleep(60)  # check every minute

            except KeyboardInterrupt:
                logger.info("Shutting down...")
                self.print_status()
                break
            except Exception as e:
                logger.error("Trading loop error: %s", e)
                time.sleep(30)

        self.db.close()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "paper"
    if mode not in ("paper", "live"):
        print(f"Usage: {sys.argv[0]} [paper|live]")
        sys.exit(1)

    engine = TradingEngine(mode=mode)

    if len(sys.argv) > 2 and sys.argv[2] == "status":
        engine.print_status()
    else:
        engine.run()
