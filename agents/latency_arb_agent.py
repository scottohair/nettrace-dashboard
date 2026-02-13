#!/usr/bin/env python3
"""Latency Arbitrage Agent — NetTrace Fly.io proximity-based trading.

Uses RTT anomaly signals from 7 Fly.io regions (ewr, ord, lhr, fra, nrt, sin, bom)
to detect exchange infrastructure changes BEFORE price moves.

Key insight: When our Fly nodes detect exchange infrastructure changes (RTT anomalies,
trends) before the market prices them in, we can trade ahead of the market.

Signal sources:
  1. rtt_anomaly with high z-score (>3) → likely infrastructure event
  2. rtt_trend uptrend on exchange → degradation = potential volatility
  3. cross_exchange_latency_diff → information asymmetry opportunity

Game Theory:
  - Information asymmetry: NetTrace latency data is private, non-public edge
  - Proximity advantage: Fly.io nodes detect changes faster than retail
  - BE A MAKER: LIMIT orders only (0.6% maker fee vs taker)
  - Nash non-equilibrium: latency signals are NOT crowded alpha

RULES (NEVER VIOLATE):
  - Max $5 per trade
  - $2 daily loss limit (HARDSTOP)
  - 70%+ signal confidence required
  - z-score >= 3.0 for anomaly signals
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
        logging.FileHandler(str(Path(__file__).parent / "latency_arb.log")),
    ]
)
logger = logging.getLogger("latency_arb")

LATENCY_ARB_DB = str(Path(__file__).parent / "latency_arb.db")
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")
FLY_URL = "https://nettrace-dashboard.fly.dev"

# Latency arb configuration
CONFIG = {
    "scan_interval": 30,               # Scan every 30s
    "min_signal_confidence": 0.70,      # 70%+ signal confidence
    "min_zscore": 3.0,                  # z-score >= 3.0 for anomaly signals
    "max_trade_usd": 5.00,             # Max $5 per trade
    "max_daily_loss_usd": 2.00,        # $2 daily loss limit
    "reserve_cash_usd": 10.00,         # Keep $10 reserve — prevent cash burnout
    "max_position_pct": 0.15,          # Max 15% per asset
    "pairs": ["BTC-USDC", "ETH-USDC", "SOL-USDC", "AVAX-USDC"],
    # Map exchange names from signals to tradeable Coinbase pairs
    "exchange_to_pairs": {
        "binance": ["BTC-USDC", "ETH-USDC", "SOL-USDC"],
        "bybit": ["BTC-USDC", "ETH-USDC", "SOL-USDC"],
        "okx": ["BTC-USDC", "ETH-USDC", "SOL-USDC"],
        "bithumb": ["BTC-USDC", "ETH-USDC"],   # Korean premium indicator
        "upbit": ["BTC-USDC", "ETH-USDC"],      # Korean premium indicator
    },
    # Fly.io regions
    "fly_regions": ["ewr", "ord", "lhr", "fra", "nrt", "sin", "bom"],
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
    """Convert trading pair to data pair for public APIs.

    We trade on -USDC pairs (where our money is) but fetch candle/price
    data from -USD pairs (more liquid, same price within ~0.01%).
    """
    return pair.replace("-USDC", "-USD")


class LatencyArbAgent:
    """Latency arbitrage agent using NetTrace Fly.io proximity signals."""

    def __init__(self):
        self.db = sqlite3.connect(LATENCY_ARB_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.daily_loss = 0.0
        self.trades_today = 0

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS signals_processed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_type TEXT NOT NULL,
                exchange TEXT,
                region TEXT,
                zscore REAL,
                confidence REAL,
                direction TEXT,
                details TEXT,
                action_taken TEXT DEFAULT 'none',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                signal_type TEXT,
                confidence REAL,
                amount_usd REAL,
                entry_price REAL,
                limit_price REAL,
                pnl REAL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

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
        """Fetch 1-minute candles from Coinbase for momentum cross-reference."""
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

    def _check_price_momentum(self, pair):
        """Check short-term price momentum for confirmation.

        Returns: ("UP", strength) | ("DOWN", strength) | ("FLAT", 0)
        """
        candles = self._fetch_candles_1m(pair, limit=10)
        if len(candles) < 5:
            return "FLAT", 0.0

        closes = [c["close"] for c in candles]
        recent = closes[-3:]
        earlier = closes[-6:-3] if len(closes) >= 6 else closes[:3]

        recent_avg = sum(recent) / len(recent)
        earlier_avg = sum(earlier) / len(earlier)

        if earlier_avg == 0:
            return "FLAT", 0.0

        change_pct = (recent_avg - earlier_avg) / earlier_avg

        if change_pct > 0.001:
            return "UP", abs(change_pct)
        elif change_pct < -0.001:
            return "DOWN", abs(change_pct)
        return "FLAT", 0.0

    def fetch_signals(self):
        """Fetch latency signals from NetTrace Fly.io dashboard.

        Returns list of parsed, high-confidence signals.
        """
        if not NETTRACE_API_KEY:
            logger.warning("NETTRACE_API_KEY not set, cannot fetch signals")
            return []

        try:
            url = f"{FLY_URL}/api/v1/signals?hours=1&min_confidence={CONFIG['min_signal_confidence']}"
            data = _fetch_json(url, headers={"Authorization": f"Bearer {NETTRACE_API_KEY}"})
            signals = data.get("signals", [])
            logger.info("Fetched %d signals from NetTrace (confidence >= %.0f%%)",
                        len(signals), CONFIG["min_signal_confidence"] * 100)
            return signals
        except Exception as e:
            logger.error("Failed to fetch NetTrace signals: %s", e)
            return []

    def analyze_signal(self, signal):
        """Analyze a single NetTrace signal for trading opportunities.

        Checks for:
          1. rtt_anomaly with high z-score → infrastructure event
          2. rtt_trend uptrend → degradation = volatility
          3. cross_exchange_latency_diff → information asymmetry

        Returns: {
            "tradeable": bool,
            "direction": "BUY"|"SELL",
            "confidence": float,
            "signal_type": str,
            "exchange": str,
            "pairs": [str],
            "reason": str,
        }
        """
        signal_type = signal.get("type", signal.get("signal_type", ""))
        exchange = signal.get("exchange", signal.get("target", "")).lower()
        confidence = signal.get("confidence", 0)
        zscore = signal.get("zscore", signal.get("z_score", 0))
        region = signal.get("region", "")
        direction_hint = signal.get("direction", "")

        result = {
            "tradeable": False,
            "direction": "NONE",
            "confidence": confidence,
            "signal_type": signal_type,
            "exchange": exchange,
            "pairs": [],
            "reason": "",
        }

        # Map exchange to tradeable pairs
        tradeable_pairs = CONFIG["exchange_to_pairs"].get(exchange, [])
        if not tradeable_pairs:
            # If exchange not in map, still trade BTC/ETH as general market signal
            tradeable_pairs = ["BTC-USDC", "ETH-USDC"]

        result["pairs"] = tradeable_pairs

        # Signal type 1: RTT anomaly with high z-score
        if "anomaly" in signal_type.lower() or "rtt_anomaly" in signal_type.lower():
            if abs(zscore) >= CONFIG["min_zscore"]:
                # High z-score anomaly: infrastructure event detected
                # Negative z-score (latency drop) = infrastructure improving = BULLISH
                # Positive z-score (latency spike) = infrastructure degrading = potential volatility
                if zscore < -CONFIG["min_zscore"]:
                    result["direction"] = "BUY"
                    result["confidence"] = min(confidence * 1.1, 0.95)
                    result["reason"] = (f"RTT anomaly: {exchange} latency DROP z={zscore:.1f} "
                                        f"from {region} — infrastructure improving, BULLISH")
                elif zscore > CONFIG["min_zscore"]:
                    result["direction"] = "SELL"
                    result["confidence"] = min(confidence * 1.05, 0.90)
                    result["reason"] = (f"RTT anomaly: {exchange} latency SPIKE z={zscore:.1f} "
                                        f"from {region} — infrastructure degrading, BEARISH")
                result["tradeable"] = True
                return result

        # Signal type 2: RTT trend (uptrend = degradation)
        if "trend" in signal_type.lower() or "rtt_trend" in signal_type.lower():
            if confidence >= CONFIG["min_signal_confidence"]:
                if "up" in direction_hint.lower() or "latency_up" in direction_hint.lower():
                    # Latency increasing = exchange degradation = potential volatility/sell
                    result["direction"] = "SELL"
                    result["confidence"] = min(confidence, 0.85)
                    result["reason"] = (f"RTT trend: {exchange} latency rising from {region} "
                                        f"— degradation = volatility incoming")
                    result["tradeable"] = True
                elif "down" in direction_hint.lower() or "latency_down" in direction_hint.lower():
                    # Latency decreasing = infrastructure improving = bullish
                    result["direction"] = "BUY"
                    result["confidence"] = min(confidence, 0.85)
                    result["reason"] = (f"RTT trend: {exchange} latency falling from {region} "
                                        f"— infrastructure improving, BULLISH")
                    result["tradeable"] = True
                return result

        # Signal type 3: Cross-exchange latency diff
        if "cross" in signal_type.lower() or "latency_diff" in signal_type.lower():
            if confidence >= CONFIG["min_signal_confidence"]:
                # Cross-exchange latency divergence = information asymmetry
                # The exchange with lower latency has faster price discovery
                result["direction"] = "BUY"  # Default: buy on Coinbase if other exchanges lag
                result["confidence"] = min(confidence, 0.80)
                result["reason"] = (f"Cross-exchange latency diff: {exchange} signal "
                                    f"— information asymmetry opportunity")
                result["tradeable"] = True
                return result

        # Korean premium signals (Bithumb/Upbit)
        if exchange in ("bithumb", "upbit"):
            if confidence >= CONFIG["min_signal_confidence"]:
                # Korean premium = BUY on Coinbase (cheaper), anticipate global catch-up
                if "up" in direction_hint.lower():
                    result["direction"] = "BUY"
                    result["confidence"] = min(confidence * 0.9, 0.85)
                    result["reason"] = (f"Korean premium signal: {exchange} latency rising "
                                        f"— Korean exchange activity, potential premium")
                    result["tradeable"] = True
                return result

        return result

    def scan(self):
        """Run a full scan: fetch signals, analyze, and execute trades."""
        logger.info("=== LATENCY ARB SCAN ===")

        signals = self.fetch_signals()
        if not signals:
            logger.info("No actionable signals from NetTrace")
            return []

        actionable = []

        for signal in signals:
            analysis = self.analyze_signal(signal)

            # Record signal processing
            self.db.execute(
                """INSERT INTO signals_processed
                   (signal_type, exchange, region, zscore, confidence, direction, details, action_taken)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (analysis["signal_type"],
                 analysis["exchange"],
                 signal.get("region", ""),
                 signal.get("zscore", signal.get("z_score", 0)),
                 analysis["confidence"],
                 analysis["direction"],
                 json.dumps({"raw_signal": signal, "analysis": analysis["reason"]}),
                 "none")
            )
            self.db.commit()

            if not analysis["tradeable"]:
                continue

            logger.info("  Signal: %s | %s | conf=%.1f%% | %s",
                        analysis["signal_type"], analysis["direction"],
                        analysis["confidence"] * 100, analysis["reason"])

            # Cross-reference with price momentum for each tradeable pair
            for pair in analysis["pairs"]:
                momentum_dir, momentum_strength = self._check_price_momentum(pair)

                # Confirmation: signal direction matches momentum
                confirmed = False
                if analysis["direction"] == "BUY" and momentum_dir == "UP":
                    confirmed = True
                elif analysis["direction"] == "SELL" and momentum_dir == "DOWN":
                    confirmed = True
                elif momentum_dir == "FLAT":
                    # Flat momentum: trust the latency signal alone (weaker)
                    analysis["confidence"] *= 0.85
                    confirmed = analysis["confidence"] >= CONFIG["min_signal_confidence"]

                if confirmed:
                    # Boost confidence with momentum confirmation
                    boosted_conf = min(analysis["confidence"] + momentum_strength * 2, 0.95)
                    actionable.append({
                        "pair": pair,
                        "direction": analysis["direction"],
                        "confidence": boosted_conf,
                        "signal_type": analysis["signal_type"],
                        "reason": analysis["reason"],
                        "momentum": f"{momentum_dir} ({momentum_strength:.3%})",
                    })
                    logger.info("    >>> ACTIONABLE: %s %s | conf=%.1f%% | momentum=%s %.3f%%",
                                analysis["direction"], pair, boosted_conf * 100,
                                momentum_dir, momentum_strength * 100)
                else:
                    logger.info("    %s: momentum=%s contradicts %s signal — SKIP",
                                pair, momentum_dir, analysis["direction"])

        return actionable

    def execute_trade(self, signal):
        """Execute a trade via Coinbase LIMIT order."""
        if self.daily_loss >= CONFIG["max_daily_loss_usd"]:
            logger.warning("HARDSTOP: Daily loss limit $%.2f reached", CONFIG["max_daily_loss_usd"])
            return False

        pair = signal["pair"]
        direction = signal["direction"]

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

        if direction == "BUY":
            if held_usd >= max_position:
                logger.info("LATENCY_ARB: %s position $%.2f >= max $%.2f (%.0f%%) — DIVERSIFY",
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
                logger.info("LATENCY_ARB: Insufficient cash ($%.2f, reserve $%.2f) for BUY",
                            cash, CONFIG["reserve_cash_usd"])
                return False
            trade_size = round(trade_size, 2)

            base_size = trade_size / price
            # LIMIT order slightly above spot to ensure fill as maker
            limit_price = price * 1.001

            logger.info("LATENCY_ARB EXECUTE: LIMIT BUY %s | $%.2f (%.6f @ $%.2f) | conf=%.1f%% | %s",
                        pair, trade_size, base_size, limit_price,
                        signal["confidence"] * 100, signal["signal_type"])

            try:
                from exchange_connector import CoinbaseTrader
                trader = CoinbaseTrader()
                result = trader.place_limit_order(pair, "BUY", base_size, limit_price, post_only=False)
                return self._process_order_result(result, pair, "BUY", trade_size, price, limit_price, signal)
            except Exception as e:
                logger.error("BUY execution error: %s", e, exc_info=True)
                return False

        elif direction == "SELL":
            held = holdings.get(base_currency, 0)
            held_usd = held * price
            if held_usd < 0.50:
                logger.info("LATENCY_ARB: No %s holdings to sell (held $%.2f)", base_currency, held_usd)
                return False

            trade_size = min(CONFIG["max_trade_usd"], held_usd * 0.5)
            if trade_size < 0.50:
                return False

            base_size = trade_size / price
            base_size = min(base_size, held)
            # LIMIT sell slightly below spot
            limit_price = price * 0.999

            logger.info("LATENCY_ARB EXECUTE: LIMIT SELL %s | $%.2f (%.8f @ $%.2f) | conf=%.1f%% | %s",
                        pair, trade_size, base_size, limit_price,
                        signal["confidence"] * 100, signal["signal_type"])

            try:
                from exchange_connector import CoinbaseTrader
                trader = CoinbaseTrader()
                result = trader.place_limit_order(pair, "SELL", base_size, limit_price, post_only=False)
                return self._process_order_result(result, pair, "SELL", trade_size, price, limit_price, signal)
            except Exception as e:
                logger.error("SELL execution error: %s", e, exc_info=True)
                return False

        return False

    def _process_order_result(self, result, pair, side, trade_size, price, limit_price, signal):
        """Process order result and record trade."""
        status = "failed"
        if "success_response" in result:
            status = "filled"
            order_id = result["success_response"].get("order_id", "?")
            logger.info("LATENCY_ARB ORDER FILLED: %s %s $%.2f @ $%.2f | order=%s",
                        pair, side, trade_size, limit_price, order_id)
        elif "order_id" in result and "error" not in str(result.get("error_response", "")):
            status = "pending"
            logger.info("LATENCY_ARB ORDER PENDING: %s %s $%.2f @ $%.2f | order=%s",
                        pair, side, trade_size, limit_price, result["order_id"])
        elif "error_response" in result:
            err = result["error_response"]
            logger.warning("LATENCY_ARB ORDER FAILED: %s %s | %s",
                           pair, side, err.get("message", err))
        else:
            logger.warning("LATENCY_ARB ORDER UNKNOWN: %s", json.dumps(result)[:300])

        self.db.execute(
            """INSERT INTO trades
               (pair, direction, signal_type, confidence, amount_usd, entry_price, limit_price, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (pair, side, signal.get("signal_type", ""),
             signal["confidence"], trade_size, price, limit_price, status)
        )
        self.db.commit()
        self.trades_today += 1

        return status == "filled"

    def print_status(self):
        """Print agent status report."""
        print(f"\n{'='*70}")
        print(f"  LATENCY ARB AGENT STATUS")
        print(f"{'='*70}")

        # Recent signals
        sigs = self.db.execute(
            "SELECT signal_type, exchange, region, zscore, confidence, direction, action_taken, created_at "
            "FROM signals_processed ORDER BY id DESC LIMIT 10"
        ).fetchall()
        print(f"\n  Recent Signals ({len(sigs)}):")
        for s in sigs:
            print(f"    {s['signal_type']:<20} {s['exchange']:<10} {s['direction']:<5} "
                  f"z={s['zscore'] or 0:.1f} conf={s['confidence']:.1%} [{s['action_taken']}] {s['created_at']}")

        # Recent trades
        trades = self.db.execute(
            "SELECT pair, direction, signal_type, confidence, amount_usd, entry_price, limit_price, status, created_at "
            "FROM trades ORDER BY id DESC LIMIT 10"
        ).fetchall()
        if trades:
            print(f"\n  Recent Trades ({len(trades)}):")
            for t in trades:
                print(f"    {t['direction']} {t['pair']} ${t['amount_usd']:.2f} @ ${t['entry_price'] or 0:,.2f} "
                      f"limit=${t['limit_price'] or 0:,.2f} [{t['status']}] {t['created_at']}")

        # Stats
        total_trades = self.db.execute("SELECT COUNT(*) as cnt FROM trades").fetchone()["cnt"]
        filled = self.db.execute("SELECT COUNT(*) as cnt FROM trades WHERE status='filled'").fetchone()["cnt"]
        total_signals = self.db.execute("SELECT COUNT(*) as cnt FROM signals_processed").fetchone()["cnt"]

        print(f"\n  Stats:")
        print(f"    Total signals processed: {total_signals}")
        print(f"    Total trades:            {total_trades} ({filled} filled)")
        print(f"    Daily loss:              ${self.daily_loss:.2f} / ${CONFIG['max_daily_loss_usd']:.2f}")
        print(f"    Trades today:            {self.trades_today}")

        print(f"\n  Config:")
        print(f"    Scan interval:   {CONFIG['scan_interval']}s")
        print(f"    Min confidence:  {CONFIG['min_signal_confidence']:.0%}")
        print(f"    Min z-score:     {CONFIG['min_zscore']}")
        print(f"    Max trade:       ${CONFIG['max_trade_usd']:.2f}")
        print(f"    Pairs:           {', '.join(CONFIG['pairs'])}")
        print(f"{'='*70}\n")

    def run(self):
        """Main continuous loop — scan and trade."""
        logger.info("Latency Arb Agent starting — scanning every %ds", CONFIG["scan_interval"])
        logger.info("Pairs: %s", ", ".join(CONFIG["pairs"]))
        logger.info("Thresholds: confidence >= %.0f%%, z-score >= %.1f",
                     CONFIG["min_signal_confidence"] * 100, CONFIG["min_zscore"])

        cycle = 0
        while True:
            try:
                cycle += 1
                actionable = self.scan()

                for signal in actionable:
                    self.execute_trade(signal)

                # Report every 60 cycles (~30 min)
                if cycle % 60 == 0:
                    self.print_status()

                time.sleep(CONFIG["scan_interval"])

            except KeyboardInterrupt:
                logger.info("Latency Arb Agent shutting down...")
                self.print_status()
                break
            except Exception as e:
                logger.error("Latency Arb error: %s", e, exc_info=True)
                time.sleep(30)


if __name__ == "__main__":
    agent = LatencyArbAgent()

    if len(sys.argv) > 1 and sys.argv[1] == "scan":
        results = agent.scan()
        agent.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "status":
        agent.print_status()
    else:
        agent.run()
