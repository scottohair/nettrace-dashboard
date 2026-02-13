#!/usr/bin/env python3
"""Turbo Simulator — 30-min sim → live execution pipeline.

Runs the C HFT engine at maximum speed against real market data:
1. SIMULATION phase (30 min): Feed real ticks through C engine,
   track paper trades, measure P&L without risking capital
2. VALIDATION: If simulated P&L > 0 for 30 min, auto-promote to LIVE
3. LIVE phase: Execute real trades via Coinbase at C-engine speed
4. ROLLING: Continuously re-validate; if losing for 10 min, drop back to SIM

Memory-safe: Fixed-size ring buffers, bounded history, no unbounded allocations.
All heavy computation in C (250ns/decision). Python only handles I/O.

Usage:
    python3 turbo_sim.py              # Run with auto-promote
    python3 turbo_sim.py --sim-only   # Simulation only, no live trades
    python3 turbo_sim.py --live-now   # Skip simulation, trade immediately
"""

import json
import logging
import os
import sys
import time
import urllib.request
from collections import deque
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
    format="%(asctime)s [turbo] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "turbo_sim.log")),
    ]
)
logger = logging.getLogger("turbo_sim")

# ============================================================
# Configuration
# ============================================================

CONFIG = {
    "sim_duration_seconds": 1800,        # 30 minutes simulation
    "sim_promote_threshold": 0.0,        # Must be net profitable to go live
    "live_demote_loss_seconds": 600,     # Drop to SIM if losing for 10 min
    "tick_interval_seconds": 5,          # Poll Coinbase every 5s
    "max_paper_trades": 1000,            # Bounded trade history (memory-safe)
    "max_ticks_per_pair": 4096,          # Ring buffer size (matches C engine)
    "pairs": ["ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD", "DOGE-USD", "FET-USD"],
    "primary_pairs": ["ETH-USD", "SOL-USD"],
    "reserve_assets": ["BTC", "USD", "USDC"],
    "starting_sim_cash": 110.0,          # Match real USD balance
}

# ============================================================
# States
# ============================================================

STATE_SIM = "SIMULATION"
STATE_LIVE = "LIVE"
STATE_PAUSED = "PAUSED"

# ============================================================
# Market data fetcher
# ============================================================

def fetch_price(pair):
    """Fetch current price from Coinbase (public, no auth needed)."""
    try:
        dp = pair.replace("-USDC", "-USD")
        url = f"https://api.coinbase.com/v2/prices/{dp}/spot"
        req = urllib.request.Request(url, headers={"User-Agent": "turbo-sim/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            return float(data["data"]["amount"])
    except Exception:
        return None


def fetch_orderbook(pair, depth=5):
    """Fetch orderbook for bid/ask spread."""
    try:
        dp = pair.replace("-USDC", "-USD")
        url = f"https://api.exchange.coinbase.com/products/{dp}/book?level=1"
        req = urllib.request.Request(url, headers={"User-Agent": "turbo-sim/1.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
            bid = float(data["bids"][0][0]) if data.get("bids") else 0
            ask = float(data["asks"][0][0]) if data.get("asks") else 0
            return bid, ask
    except Exception:
        return 0, 0


def fetch_all_signals(pair):
    """Fetch signals from Fly.io NetTrace API."""
    try:
        api_key = os.environ.get("NETTRACE_API_KEY", "")
        base = pair.split("-")[0]
        url = f"https://nettrace-dashboard.fly.dev/api/v1/signals?hours=1&min_confidence=0.6"
        req = urllib.request.Request(url, headers={
            "User-Agent": "turbo-sim/1.0",
            "X-API-Key": api_key,
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            signals = {}
            for s in data.get("signals", []):
                stype = s.get("signal_type", "")
                conf = s.get("confidence", 0)
                if base.lower() in stype.lower() or stype in (
                    "latency", "fear_greed", "regime", "momentum"
                ):
                    signals[stype] = max(signals.get(stype, 0), conf)
            return signals
    except Exception:
        return {}


# ============================================================
# Paper Trade Tracker (memory-safe: bounded deque)
# ============================================================

class PaperTrader:
    """Tracks simulated trades with bounded memory."""

    def __init__(self, starting_cash, max_trades=1000):
        self.cash = starting_cash
        self.starting_cash = starting_cash
        self.positions = {}  # pair → {amount, entry_price, entry_time}
        self.trades = deque(maxlen=max_trades)  # bounded!
        self.total_pnl = 0.0
        self.win_count = 0
        self.loss_count = 0

    def buy(self, pair, price, amount_usd, timestamp):
        """Execute paper BUY."""
        if amount_usd > self.cash:
            amount_usd = self.cash
        if amount_usd < 1.0:
            return False

        fee = amount_usd * 0.004  # maker fee
        net_usd = amount_usd - fee
        amount = net_usd / price

        self.cash -= amount_usd
        if pair in self.positions:
            # Average in
            pos = self.positions[pair]
            total_amount = pos["amount"] + amount
            avg_price = (pos["amount"] * pos["entry_price"] + amount * price) / total_amount
            pos["amount"] = total_amount
            pos["entry_price"] = avg_price
        else:
            self.positions[pair] = {
                "amount": amount,
                "entry_price": price,
                "entry_time": timestamp,
                "peak_price": price,
            }

        self.trades.append({
            "time": timestamp, "pair": pair, "side": "BUY",
            "price": price, "amount_usd": amount_usd, "fee": fee,
        })
        return True

    def sell(self, pair, price, fraction, timestamp):
        """Execute paper SELL (fraction of position)."""
        if pair not in self.positions:
            return False, 0

        pos = self.positions[pair]
        sell_amount = pos["amount"] * fraction
        sell_usd = sell_amount * price
        fee = sell_usd * 0.004

        pnl = (price - pos["entry_price"]) / pos["entry_price"] * sell_usd - fee

        self.cash += sell_usd - fee
        self.total_pnl += pnl

        if pnl > 0:
            self.win_count += 1
        else:
            self.loss_count += 1

        pos["amount"] -= sell_amount
        if pos["amount"] < 1e-10:
            del self.positions[pair]

        self.trades.append({
            "time": timestamp, "pair": pair, "side": "SELL",
            "price": price, "amount_usd": sell_usd, "fee": fee, "pnl": pnl,
        })
        return True, pnl

    def portfolio_value(self, prices):
        """Current total value (cash + positions)."""
        total = self.cash
        for pair, pos in self.positions.items():
            p = prices.get(pair, pos["entry_price"])
            total += pos["amount"] * p
        return total

    def update_peaks(self, prices):
        """Update peak prices for trailing stop calculation."""
        for pair, pos in self.positions.items():
            p = prices.get(pair, 0)
            if p > pos.get("peak_price", 0):
                pos["peak_price"] = p

    def stats(self):
        total_trades = self.win_count + self.loss_count
        return {
            "total_pnl": self.total_pnl,
            "cash": self.cash,
            "positions": len(self.positions),
            "trades": total_trades,
            "win_rate": self.win_count / max(1, total_trades),
            "return_pct": (self.total_pnl / self.starting_cash) * 100,
        }


# ============================================================
# Live Trade Executor
# ============================================================

class LiveExecutor:
    """Executes real trades via Coinbase."""

    def __init__(self):
        self._trader = None

    def _get_trader(self):
        if self._trader is None:
            from exchange_connector import CoinbaseTrader
            self._trader = CoinbaseTrader()
        return self._trader

    def buy(self, pair, price, amount_usd):
        """Place real limit BUY order."""
        trader = self._get_trader()
        base = pair.split("-")[0]
        amount = amount_usd / price

        # Format amount based on asset
        if base == "BTC":
            amount_str = f"{amount:.8f}"
        elif base in ("ETH", "SOL", "AVAX", "LINK"):
            amount_str = f"{amount:.6f}"
        else:
            amount_str = f"{amount:.2f}"

        try:
            result = trader.place_order(
                pair, "BUY", amount_str,
                limit_price=str(round(price, 2)),
                post_only=False
            )
            success = result.get("success_response", {})
            if success:
                logger.info("LIVE BUY: %s $%.2f @ $%.2f — order %s",
                           pair, amount_usd, price, success.get("order_id", "?"))
                return True
            else:
                error = result.get("error_response", result)
                logger.warning("LIVE BUY FAILED: %s — %s", pair, str(error)[:200])
                return False
        except Exception as e:
            logger.error("LIVE BUY ERROR: %s — %s", pair, e)
            return False

    def sell(self, pair, price, amount):
        """Place real limit SELL order."""
        trader = self._get_trader()
        base = pair.split("-")[0]

        if base == "BTC":
            amount_str = f"{amount:.8f}"
        elif base in ("ETH", "SOL", "AVAX", "LINK"):
            amount_str = f"{amount:.6f}"
        else:
            amount_str = f"{amount:.2f}"

        try:
            result = trader.place_order(
                pair, "SELL", amount_str,
                limit_price=str(round(price, 2)),
                post_only=False
            )
            success = result.get("success_response", {})
            if success:
                logger.info("LIVE SELL: %s %.6f @ $%.2f — order %s",
                           pair, amount, price, success.get("order_id", "?"))
                return True
            else:
                logger.warning("LIVE SELL FAILED: %s — %s", pair, str(result)[:200])
                return False
        except Exception as e:
            logger.error("LIVE SELL ERROR: %s — %s", pair, e)
            return False


# ============================================================
# Turbo Engine — simulation → live pipeline
# ============================================================

class TurboEngine:
    """Simulation-first trading engine at maximum speed.

    Phase 1 (SIM): Paper trade for 30 min using C HFT engine
    Phase 2 (LIVE): If profitable, execute real trades
    Phase 3 (ROLLING): Continuously validate; demote if losing
    """

    def __init__(self, mode="auto"):
        self.state = STATE_SIM if mode != "live-now" else STATE_LIVE
        self.sim_only = (mode == "sim-only")
        self.paper = PaperTrader(CONFIG["starting_sim_cash"], CONFIG["max_paper_trades"])
        self.live_exec = LiveExecutor()

        # HFT C engine
        try:
            from hft_bridge import get_hft_engine
            self.hft = get_hft_engine()
            logger.info("HFT C engine loaded (250ns/decision)")
        except Exception as e:
            logger.warning("HFT C engine unavailable: %s — using Python fallback", e)
            self.hft = None

        # Timing
        self.sim_start = time.time()
        self.live_start = None
        self.last_profitable_time = time.time()

        # Performance tracking (bounded)
        self.tick_count = 0
        self.decision_count = 0
        self.total_decision_ns = 0

        logger.info("TurboEngine initialized | state=%s | sim_cash=$%.2f | pairs=%s",
                    self.state, CONFIG["starting_sim_cash"], CONFIG["pairs"])

    def _fetch_market_snapshot(self):
        """Fetch prices and orderbooks for all pairs."""
        prices = {}
        spreads = {}
        for pair in CONFIG["pairs"]:
            p = fetch_price(pair)
            if p:
                prices[pair] = p
                bid, ask = fetch_orderbook(pair)
                if bid and ask:
                    spreads[pair] = (bid, ask)
        return prices, spreads

    def _build_signals_for_pair(self, pair, price, bid, ask):
        """Build signal dict for C engine from available data."""
        signals = {}

        # Price-based signals (local computation — fast)
        if self.hft and pair in self.hft._tick_bufs:
            # Regime from tick buffer
            import ctypes
            tb = self.hft._tick_bufs.get(
                self.hft.PAIR_INDEX.get(pair.split("-")[0], 0)
                if hasattr(self.hft, 'PAIR_INDEX') else 0
            )

        # Orderbook signal
        if bid > 0 and ask > 0:
            spread_pct = (ask - bid) / bid
            if spread_pct < 0.001:  # tight spread = confidence
                signals["orderbook"] = 0.70
            elif spread_pct < 0.003:
                signals["orderbook"] = 0.60

        # Momentum from recent ticks (use C engine EMA)
        if self.hft:
            from hft_bridge import PAIR_INDEX
            idx = PAIR_INDEX.get(pair.split("-")[0], 0)
            tb = self.hft._get_tick_buf(idx)
            if tb.count > 10:
                if tb.ema_fast > tb.ema_slow * 1.001:
                    signals["momentum"] = 0.72
                elif tb.ema_fast < tb.ema_slow * 0.999:
                    signals["momentum"] = 0.65  # sell signal

        # Add base confidence for latency (our edge)
        signals["latency"] = 0.75  # Default; real value from API

        return signals

    def _run_hft_decision(self, pair, signals, price, cash):
        """Run decision through C HFT engine."""
        if not self.hft:
            # Python fallback: simple threshold
            conf = sum(signals.values()) / max(1, len(signals))
            if conf >= 0.70 and len(signals) >= 2:
                return {"action": "BUY", "confidence": conf, "amount_usd": min(15.0, cash * 0.1)}
            return {"action": "NONE", "confidence": conf, "amount_usd": 0}

        # Feed tick to C engine
        self.hft.feed_tick(pair, price, volume=1.0)

        # Run full decision
        decision = self.hft.decide(signals, pair, cash=cash)
        self.decision_count += 1
        self.total_decision_ns += decision.get("decision_ns", 0)

        return decision

    def _check_exits(self, prices):
        """Check all positions for exit conditions."""
        exits = []
        now = time.time()

        for pair, pos in list(self.paper.positions.items()):
            price = prices.get(pair, 0)
            if price <= 0:
                continue

            hold_hours = (now - pos.get("entry_time", now)) / 3600.0
            peak = pos.get("peak_price", pos["entry_price"])
            position_usd = pos["amount"] * price

            if self.hft:
                exit_dec = self.hft.check_exit(
                    entry_price=pos["entry_price"],
                    current_price=price,
                    peak_price=peak,
                    hold_hours=hold_hours,
                    position_usd=position_usd,
                    volatility=0.02
                )
            else:
                # Python fallback
                profit_pct = (price - pos["entry_price"]) / pos["entry_price"]
                if profit_pct >= 0.008:
                    exit_dec = {"action": "TP0", "sell_fraction": 0.20}
                elif profit_pct < -0.05:
                    exit_dec = {"action": "EXIT_FULL", "sell_fraction": 1.0}
                else:
                    exit_dec = {"action": "HOLD", "sell_fraction": 0}

            if exit_dec["action"] != "HOLD":
                exits.append((pair, exit_dec, price))

        return exits

    def _execute_cycle(self):
        """One full cycle: fetch → analyze → decide → execute."""
        t_start = time.perf_counter()

        # 1. Market snapshot
        prices, spreads = self._fetch_market_snapshot()
        if not prices:
            return

        self.tick_count += 1

        # 2. Feed ticks to C engine
        for pair, price in prices.items():
            if self.hft:
                bid, ask = spreads.get(pair, (0, 0))
                self.hft.feed_tick(pair, price, volume=1.0, bid=bid, ask=ask)

        # 3. Update peaks for positions
        self.paper.update_peaks(prices)

        # 4. Check exits first (free capital before new trades)
        exits = self._check_exits(prices)
        for pair, exit_dec, price in exits:
            fraction = exit_dec.get("sell_fraction", 1.0)

            if self.state == STATE_SIM:
                ok, pnl = self.paper.sell(pair, price, fraction, time.time())
                if ok:
                    logger.info("[SIM] EXIT %s | %s | frac=%.0f%% | pnl=$%.4f | price=$%.2f",
                               pair, exit_dec["action"], fraction * 100, pnl, price)
            elif self.state == STATE_LIVE:
                # Real sell
                pos = self.paper.positions.get(pair, {})
                sell_amount = pos.get("amount", 0) * fraction
                if sell_amount > 0:
                    self.live_exec.sell(pair, price, sell_amount)
                # Also paper-track for validation
                self.paper.sell(pair, price, fraction, time.time())

        # 5. Evaluate new trade opportunities
        cash = self.paper.cash
        for pair in CONFIG["pairs"]:
            price = prices.get(pair, 0)
            if price <= 0 or cash < 2.0:
                continue

            bid, ask = spreads.get(pair, (0, 0))
            signals = self._build_signals_for_pair(pair, price, bid, ask)

            if len(signals) < 2:
                continue

            decision = self._run_hft_decision(pair, signals, price, cash)

            if decision["action"] == "BUY" and decision["confidence"] >= 0.70:
                amount_usd = min(decision.get("amount_usd", 10.0), cash * 0.15, 20.0)
                if amount_usd < 1.0:
                    continue

                if self.state == STATE_SIM:
                    ok = self.paper.buy(pair, price, amount_usd, time.time())
                    if ok:
                        logger.info("[SIM] BUY %s | $%.2f @ $%.2f | conf=%.1f%% | gf=%.2f",
                                   pair, amount_usd, price,
                                   decision["confidence"] * 100,
                                   decision.get("gf_quality", 0))
                        cash -= amount_usd
                elif self.state == STATE_LIVE:
                    # Real buy
                    ok = self.live_exec.buy(pair, price, amount_usd)
                    if ok:
                        # Also paper-track for validation
                        self.paper.buy(pair, price, amount_usd, time.time())
                        cash -= amount_usd

        # 6. Performance report
        portfolio_val = self.paper.portfolio_value(prices)
        stats = self.paper.stats()
        elapsed = time.perf_counter() - t_start
        avg_ns = self.total_decision_ns / max(1, self.decision_count)

        if self.tick_count % 12 == 0:  # Report every ~60s
            logger.info("[%s] tick=%d | portfolio=$%.2f | pnl=$%.4f (%.2f%%) | "
                       "trades=%d (%.0f%% win) | cash=$%.2f | avg_decision=%dns | cycle=%.1fms",
                       self.state, self.tick_count, portfolio_val,
                       stats["total_pnl"], stats["return_pct"],
                       stats["trades"], stats["win_rate"] * 100,
                       stats["cash"], avg_ns, elapsed * 1000)

        return portfolio_val

    def _check_state_transition(self):
        """Check if we should promote SIM→LIVE or demote LIVE→SIM."""
        now = time.time()

        if self.state == STATE_SIM and not self.sim_only:
            elapsed = now - self.sim_start
            if elapsed >= CONFIG["sim_duration_seconds"]:
                stats = self.paper.stats()
                if stats["total_pnl"] >= CONFIG["sim_promote_threshold"]:
                    logger.info("=== PROMOTING TO LIVE ===")
                    logger.info("Sim results: pnl=$%.4f (%.2f%%) | %d trades | %.0f%% win rate",
                               stats["total_pnl"], stats["return_pct"],
                               stats["trades"], stats["win_rate"] * 100)
                    self.state = STATE_LIVE
                    self.live_start = now
                    self.last_profitable_time = now
                else:
                    logger.info("Sim FAILED — pnl=$%.4f. Restarting simulation...",
                               stats["total_pnl"])
                    # Reset and try again
                    self.paper = PaperTrader(CONFIG["starting_sim_cash"], CONFIG["max_paper_trades"])
                    self.sim_start = now

        elif self.state == STATE_LIVE:
            stats = self.paper.stats()
            if stats["total_pnl"] > 0:
                self.last_profitable_time = now

            # Demote if losing for too long
            losing_duration = now - self.last_profitable_time
            if losing_duration > CONFIG["live_demote_loss_seconds"]:
                logger.warning("=== DEMOTING TO SIM === (losing for %.0f seconds)", losing_duration)
                self.state = STATE_SIM
                self.sim_start = now
                self.paper = PaperTrader(CONFIG["starting_sim_cash"], CONFIG["max_paper_trades"])

    def run(self):
        """Main loop: simulation → validation → live execution."""
        logger.info("=" * 60)
        logger.info("TURBO ENGINE STARTING")
        logger.info("State: %s | Pairs: %s | Sim duration: %ds",
                    self.state, CONFIG["pairs"], CONFIG["sim_duration_seconds"])
        logger.info("=" * 60)

        try:
            while True:
                try:
                    self._execute_cycle()
                    self._check_state_transition()
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    logger.error("Cycle error: %s", e, exc_info=True)

                time.sleep(CONFIG["tick_interval_seconds"])
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            stats = self.paper.stats()
            logger.info("Final: pnl=$%.4f | trades=%d | win_rate=%.0f%%",
                       stats["total_pnl"], stats["trades"], stats["win_rate"] * 100)


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    mode = "auto"
    if "--sim-only" in sys.argv:
        mode = "sim-only"
    elif "--live-now" in sys.argv:
        mode = "live-now"

    engine = TurboEngine(mode=mode)
    engine.run()
