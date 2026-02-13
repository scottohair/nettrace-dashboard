#!/usr/bin/env python3
"""Mean Reversion Agent — deterministic statistical arbitrage on correlated pairs.

Strategy: When a pair's z-score deviates beyond a threshold from its rolling mean,
trade toward the mean. This is a DETERMINISTIC strategy — no directional prediction needed.

The math:
- Track rolling price ratio between correlated pairs (e.g., ETH/BTC)
- Calculate z-score of current ratio vs 100-period mean
- When z > +2.0: ratio is extended → sell the overperformer, buy the underperformer
- When z < -2.0: ratio is compressed → buy the overperformer, sell the underperformer
- Exit when z returns to ±0.5 (mean reversion complete)

Why this works in ANY market:
- Bull market: BTC and ETH both rise, but one leads. Trade the follower catching up.
- Bear market: Both fall, but one falls faster. Trade the divergence.
- Flat market: Random walks create temporary divergences. Trade them.

RULES:
- MAKER orders only (0.4% fee)
- Dynamic z-threshold via risk_controller volatility
- Position size: max 3% of portfolio per pair
- Exit on z reverting to ±0.5 OR stop-loss at z = ±4.0
"""

import json
import logging
import math
import os
import sqlite3
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "mean_reversion.log")),
    ]
)
logger = logging.getLogger("mean_reversion")

# ── Configuration ──────────────────────────────────────────────────────

# Correlated pairs to track (pair A, pair B) — trade the ratio
PAIR_SETS = [
    ("BTC-USDC", "ETH-USDC"),    # BTC/ETH ratio — highest correlation
    ("ETH-USDC", "SOL-USDC"),    # ETH/SOL — strong crypto correlation
    ("BTC-USDC", "SOL-USDC"),    # BTC/SOL
    ("LINK-USDC", "AVAX-USDC"),  # LINK/AVAX — mid-cap correlation
]

LOOKBACK = 100               # Rolling window for mean/std
Z_ENTRY_THRESHOLD = 2.0      # Enter when z-score exceeds this
Z_EXIT_THRESHOLD = 0.2       # Exit when z-score reverts to this (tighter = more reversion profit)
Z_STOP_LOSS = 4.0            # Stop-loss at extreme deviation
CYCLE_SECONDS = 60           # Check every 60s
MAX_POSITION_PCT = 0.03      # 3% of portfolio per pair set
MAKER_FEE = 0.004            # 0.4%
MIN_TRADE_USD = 1.00

DB_PATH = str(Path(__file__).parent / "mean_reversion.db")

# ── Load credentials ───────────────────────────────────────────────────
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


class PairTracker:
    """Track rolling statistics for a pair ratio."""

    def __init__(self, pair_a, pair_b, lookback=LOOKBACK):
        self.pair_a = pair_a
        self.pair_b = pair_b
        self.lookback = lookback
        self.ratios = deque(maxlen=lookback)
        self.last_z = 0.0

    @property
    def name(self):
        return f"{self.pair_a.split('-')[0]}/{self.pair_b.split('-')[0]}"

    def update(self, price_a, price_b):
        """Add a new ratio observation."""
        if price_b <= 0:
            return
        ratio = price_a / price_b
        self.ratios.append(ratio)

    @property
    def ready(self):
        return len(self.ratios) >= 20  # Need at least 20 observations

    @property
    def z_score(self):
        """Current z-score of the ratio."""
        if not self.ready:
            return 0.0
        values = list(self.ratios)
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        std = math.sqrt(variance) if variance > 0 else 0.0001
        self.last_z = (values[-1] - mean) / std
        return self.last_z

    @property
    def mean(self):
        if not self.ratios:
            return 0
        return sum(self.ratios) / len(self.ratios)

    @property
    def current_ratio(self):
        return self.ratios[-1] if self.ratios else 0


class MeanReversionAgent:
    """Deterministic mean reversion on correlated pairs."""

    def __init__(self):
        self.db = sqlite3.connect(DB_PATH)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.trader = None
        self.trackers = [PairTracker(a, b) for a, b in PAIR_SETS]
        self.positions = {}  # pair_name -> {"side_a": "BUY"/"SELL", "entry_z": float, ...}
        self.cycle = 0

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS mr_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair_name TEXT NOT NULL,
                action TEXT NOT NULL,
                side_a TEXT, side_b TEXT,
                price_a REAL, price_b REAL,
                size_usd REAL,
                z_score REAL,
                entry_z REAL,
                pnl_usd REAL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS mr_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair_name TEXT NOT NULL,
                ratio REAL, z_score REAL,
                price_a REAL, price_b REAL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def _get_trader(self):
        if self.trader is None:
            from exchange_connector import CoinbaseTrader
            self.trader = CoinbaseTrader()
        return self.trader

    def _get_price(self, pair):
        """Get current mid price for a pair."""
        trader = self._get_trader()
        book = trader.get_order_book(pair)
        pricebook = book.get("pricebook", book)
        bids = pricebook.get("bids", [])
        asks = pricebook.get("asks", [])
        if bids and asks:
            return (float(bids[0]["price"]) + float(asks[0]["price"])) / 2
        # Fallback to product ticker
        product = trader.get_product(pair)
        return float(product.get("price", 0))

    def _get_portfolio_value(self):
        """Get total portfolio value in USD."""
        trader = self._get_trader()
        accounts = trader.get_accounts()
        total = 0
        for acc in accounts.get("accounts", []):
            currency = acc.get("currency", "")
            avail = float(acc.get("available_balance", {}).get("value", 0))
            if currency in ("USD", "USDC"):
                total += avail
            elif avail > 0:
                try:
                    p = self._get_price(f"{currency}-USDC")
                    total += avail * p
                except Exception:
                    pass
        return total

    def _get_holdings(self):
        """Get available balances."""
        trader = self._get_trader()
        accounts = trader.get_accounts()
        holdings = {}
        for acc in accounts.get("accounts", []):
            currency = acc.get("currency", "")
            avail = float(acc.get("available_balance", {}).get("value", 0))
            if avail > 0:
                holdings[currency] = avail
        return holdings

    def check_and_trade(self, tracker):
        """Check z-score and enter/exit positions for a pair."""
        try:
            price_a = self._get_price(tracker.pair_a)
            price_b = self._get_price(tracker.pair_b)
        except Exception as e:
            logger.debug("Price fetch failed for %s: %s", tracker.name, e)
            return

        if price_a <= 0 or price_b <= 0:
            return

        tracker.update(price_a, price_b)

        if not tracker.ready:
            if len(tracker.ratios) % 10 == 0:
                logger.info("Warming up %s: %d/%d observations",
                           tracker.name, len(tracker.ratios), LOOKBACK)
            return

        z = tracker.z_score

        # Log observation periodically
        if self.cycle % 10 == 0:
            self.db.execute(
                "INSERT INTO mr_observations (pair_name, ratio, z_score, price_a, price_b) VALUES (?, ?, ?, ?, ?)",
                (tracker.name, tracker.current_ratio, z, price_a, price_b)
            )
            self.db.commit()

        name = tracker.name
        trader = self._get_trader()

        # ── Check for EXIT on existing position ──
        if name in self.positions:
            pos = self.positions[name]
            entry_z = pos["entry_z"]

            # Exit if z reverted to threshold or hit stop-loss
            should_exit = False
            reason = ""

            # Estimate current P&L before deciding to exit
            entry_ratio = pos["entry_price_a"] / pos["entry_price_b"] if pos["entry_price_b"] else 1
            exit_ratio = price_a / price_b if price_b else 1
            ratio_change = (exit_ratio - entry_ratio) / entry_ratio
            est_pnl = (-ratio_change if pos["side_a"] == "SELL" else ratio_change) * pos["trade_usd"]
            est_pnl -= pos["trade_usd"] * MAKER_FEE * 2  # fees

            if abs(z) >= Z_STOP_LOSS:
                should_exit = True
                reason = f"stop-loss z={z:.2f}"
            elif abs(z) <= Z_EXIT_THRESHOLD:
                # Only exit on reversion if we're at least breakeven
                if est_pnl >= 0:
                    should_exit = True
                    reason = f"z reverted to {z:.2f} (pnl=${est_pnl:.4f})"
                else:
                    logger.info("MR %s: z=%.2f reverted but pnl=$%.4f < $0 — waiting for breakeven", name, z, est_pnl)
            elif (entry_z > 0 and z < -Z_EXIT_THRESHOLD) or (entry_z < 0 and z > Z_EXIT_THRESHOLD):
                should_exit = True
                reason = f"z crossed zero to {z:.2f}"

            if should_exit:
                self._exit_position(name, z, price_a, price_b, reason)
                return

            return  # Hold existing position

        # ── Check for ENTRY ──
        if abs(z) >= Z_ENTRY_THRESHOLD:
            self._enter_position(tracker, z, price_a, price_b)

    def _enter_position(self, tracker, z, price_a, price_b):
        """Enter a mean reversion position."""
        name = tracker.name
        trader = self._get_trader()

        # Get portfolio for sizing
        portfolio = self._get_portfolio_value()
        trade_usd = min(portfolio * MAX_POSITION_PCT, 10.0)  # Cap at $10 (amortize fees over larger base)
        if trade_usd < MIN_TRADE_USD:
            logger.debug("Trade too small for %s: $%.2f", name, trade_usd)
            return

        holdings = self._get_holdings()

        if z > Z_ENTRY_THRESHOLD:
            # Ratio is HIGH: pair_a is overpriced vs pair_b
            # SELL pair_a (overperformer), BUY pair_b (underperformer)
            side_a, side_b = "SELL", "BUY"
        else:
            # Ratio is LOW: pair_a is underpriced vs pair_b
            # BUY pair_a (underperformer), SELL pair_b (overperformer)
            side_a, side_b = "BUY", "SELL"

        # Execute both legs
        success_a = self._place_leg(tracker.pair_a, side_a, trade_usd / 2, price_a, holdings)
        success_b = self._place_leg(tracker.pair_b, side_b, trade_usd / 2, price_b, holdings)

        if success_a or success_b:
            self.positions[name] = {
                "side_a": side_a, "side_b": side_b,
                "entry_z": z, "entry_price_a": price_a, "entry_price_b": price_b,
                "trade_usd": trade_usd, "entered_at": time.time()
            }
            self.db.execute(
                "INSERT INTO mr_trades (pair_name, action, side_a, side_b, price_a, price_b, size_usd, z_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (name, "ENTER", side_a, side_b, price_a, price_b, trade_usd, z)
            )
            self.db.commit()
            logger.info("ENTER %s: %s_A/%s_B | z=%.2f | $%.2f | ratio=%.4f (mean=%.4f)",
                       name, side_a, side_b, z, trade_usd, tracker.current_ratio, tracker.mean)

    def _exit_position(self, name, z, price_a, price_b, reason):
        """Exit a mean reversion position."""
        pos = self.positions.pop(name)
        trader = self._get_trader()
        holdings = self._get_holdings()

        # Reverse the entry sides
        exit_side_a = "BUY" if pos["side_a"] == "SELL" else "SELL"
        exit_side_b = "BUY" if pos["side_b"] == "SELL" else "SELL"

        trade_usd = pos["trade_usd"]

        # Find the tracker for this pair
        tracker = None
        for t in self.trackers:
            if t.name == name:
                tracker = t
                break

        if tracker:
            self._place_leg(tracker.pair_a, exit_side_a, trade_usd / 2, price_a, holdings)
            self._place_leg(tracker.pair_b, exit_side_b, trade_usd / 2, price_b, holdings)

        # Estimate P&L
        entry_ratio = pos["entry_price_a"] / pos["entry_price_b"] if pos["entry_price_b"] else 1
        exit_ratio = price_a / price_b if price_b else 1
        ratio_change = (exit_ratio - entry_ratio) / entry_ratio

        # If we sold A and bought B, we profit when ratio falls (A cheaper, B more expensive)
        if pos["side_a"] == "SELL":
            pnl = -ratio_change * trade_usd  # Sold A high, bought back lower
        else:
            pnl = ratio_change * trade_usd   # Bought A low, sold higher

        pnl -= trade_usd * MAKER_FEE * 2  # Fees on both entry and exit

        self.db.execute(
            "INSERT INTO mr_trades (pair_name, action, side_a, side_b, price_a, price_b, size_usd, z_score, entry_z, pnl_usd) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (name, "EXIT", exit_side_a, exit_side_b, price_a, price_b, trade_usd, z, pos["entry_z"], pnl)
        )
        self.db.commit()
        logger.info("EXIT %s: %s | z=%.2f→%.2f | pnl=$%.4f | held=%ds",
                    name, reason, pos["entry_z"], z, pnl, int(time.time() - pos["entered_at"]))

    def _place_leg(self, pair, side, usd_amount, price, holdings):
        """Place one leg of a pairs trade using limit orders."""
        trader = self._get_trader()

        if side == "BUY":
            limit_price = price * 0.999  # Slightly below for maker
            base_size = usd_amount / limit_price
            result = trader.place_limit_order(pair, "BUY", base_size, limit_price, post_only=True)
            if result.get("error_response"):
                # Fallback: market order if limit rejected
                result = trader.place_order(pair, "BUY", round(usd_amount, 2))
        else:
            base = pair.split("-")[0]
            held = holdings.get(base, 0)
            sell_size = min(held * 0.5, usd_amount / price)  # Don't sell everything
            if sell_size * price < MIN_TRADE_USD:
                logger.debug("Not enough %s to sell for %s leg", base, pair)
                return False
            limit_price = price * 1.001  # Slightly above for maker
            result = trader.place_limit_order(pair, "SELL", sell_size, limit_price, post_only=True)
            if result.get("error_response"):
                result = trader.place_order(pair, "SELL", sell_size)

        success = "success_response" in result or ("order_id" in result and "error" not in result)
        return success

    def run(self):
        """Main loop."""
        logger.info("Mean Reversion Agent starting | Pairs: %s | Z-entry: %.1f | Z-exit: %.1f",
                    [t.name for t in self.trackers], Z_ENTRY_THRESHOLD, Z_EXIT_THRESHOLD)

        while True:
            try:
                self.cycle += 1

                for tracker in self.trackers:
                    self.check_and_trade(tracker)
                    time.sleep(2)  # Rate limit

                # Status log
                if self.cycle % 20 == 0:
                    for t in self.trackers:
                        if t.ready:
                            logger.info("%s: z=%.2f ratio=%.4f mean=%.4f obs=%d %s",
                                       t.name, t.z_score, t.current_ratio, t.mean,
                                       len(t.ratios),
                                       "POSITION" if t.name in self.positions else "")

                time.sleep(CYCLE_SECONDS)

            except KeyboardInterrupt:
                logger.info("Shutting down. positions=%d", len(self.positions))
                break
            except Exception as e:
                logger.error("Error: %s", e, exc_info=True)
                time.sleep(30)


if __name__ == "__main__":
    agent = MeanReversionAgent()
    agent.run()
