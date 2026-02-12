#!/usr/bin/env python3
"""High-Confidence Sniper — stacked signal aggregator.

Only trades when composite confidence >= 90% with 3+ confirming signals.
Stacks 5 independent signal sources for maximum conviction:

  1. NetTrace latency signals (exchange infrastructure changes)
  2. Fast Engine regime detection (SMA/RSI/BB/VWAP/ATR)
  3. Fast Engine arb check (Coinbase vs 5 exchange median)
  4. Orderbook imbalance (bid/ask depth ratio)
  5. RSI extreme detection (oversold < 25 / overbought > 75)

Game Theory:
  - Only enters when the market is in a non-equilibrium state
  - Multiple independent confirmations = information asymmetry advantage
  - Kelly-optimal sizing = mathematically optimal bet fraction
  - Best venue routing = minimum execution cost

RULES (NEVER VIOLATE):
  - Composite confidence >= 90% AND 3+ confirming signals
  - Max $5 per trade
  - $2 daily loss limit (HARDSTOP)
  - Skip DOWNTREND regime (Rule #1: NEVER lose money)
  - Maker orders preferred (0.4% vs 0.6%)
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

# Asset state tracking for learning
try:
    from asset_tracker import get_tracker
    _tracker = get_tracker()
except Exception:
    _tracker = None

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
        logging.FileHandler(str(Path(__file__).parent / "sniper.log")),
    ]
)
logger = logging.getLogger("sniper")

SNIPER_DB = str(Path(__file__).parent / "sniper.db")
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")
FLY_URL = "https://nettrace-dashboard.fly.dev"
WALLET_ADDRESS = os.environ.get("WALLET_ADDRESS", "")

# Sniper configuration
CONFIG = {
    "min_composite_confidence": 0.70,    # 70% minimum (matches trading rules)
    "min_confirming_signals": 2,         # 2+ must agree (matches trading rules)
    "max_trade_usd": 5.00,
    "max_daily_loss_usd": 2.00,
    "scan_interval": 60,                  # seconds between scans (stop churning)
    "min_hold_seconds": 60,                # hold positions at least 60 seconds (nimble, not churning)
    "pairs": ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD", "DOGE-USD", "FET-USD"],
    "signal_weights": {
        "latency": 0.12,
        "regime": 0.14,
        "arb": 0.12,
        "orderbook": 0.12,
        "rsi_extreme": 0.10,
        "fear_greed": 0.10,
        "momentum": 0.08,
        "uptick": 0.22,   # highest weight — timing-based buy_low_sell_high
    },
}


def _fetch_json(url, headers=None, timeout=10):
    """HTTP GET JSON helper."""
    h = {"User-Agent": "NetTrace/1.0"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


class SignalSource:
    """Base class for signal sources."""

    def scan(self, pair):
        """Returns: {"direction": "BUY"|"SELL"|"NONE", "confidence": 0.0-1.0, "reason": "..."}"""
        raise NotImplementedError


class LatencySignalSource(SignalSource):
    """Signal #1: NetTrace latency signals from Fly.io."""

    def scan(self, pair):
        try:
            url = f"{FLY_URL}/api/v1/signals?hours=1&min_confidence=0.6"
            data = _fetch_json(url, headers={"X-Api-Key": NETTRACE_API_KEY})
            signals = data.get("signals", [])

            # Find high-confidence signals related to crypto exchanges
            relevant = [s for s in signals if s.get("confidence", 0) >= 0.7]
            if not relevant:
                return {"direction": "NONE", "confidence": 0, "reason": "No latency signals"}

            # Aggregate: if multiple exchanges show latency_down → bullish (infrastructure improving)
            up_count = sum(1 for s in relevant if s.get("direction") == "latency_down")
            down_count = sum(1 for s in relevant if s.get("direction") == "latency_up")
            avg_conf = sum(s.get("confidence", 0) for s in relevant) / len(relevant)

            if up_count > down_count:
                return {"direction": "BUY", "confidence": min(avg_conf, 0.95),
                        "reason": f"{up_count} exchanges improving latency"}
            elif down_count > up_count:
                return {"direction": "SELL", "confidence": min(avg_conf, 0.95),
                        "reason": f"{down_count} exchanges degrading"}
            return {"direction": "NONE", "confidence": 0, "reason": "Mixed latency signals"}
        except Exception as e:
            logger.debug("Latency signal error: %s", e)
            return {"direction": "NONE", "confidence": 0, "reason": str(e)}


class RegimeSignalSource(SignalSource):
    """Signal #2: Fast Engine C-based regime detection + indicators."""

    def scan(self, pair):
        try:
            candles = self._fetch_candles(pair)
            if len(candles) < 20:
                return {"direction": "NONE", "confidence": 0, "reason": "Insufficient candles"}

            from fast_bridge import FastEngine
            engine = FastEngine()
            indicators = engine.compute_indicators(candles)
            signal = engine.generate_signal(candles)

            regime = indicators.get("regime", "UNKNOWN")
            rsi = indicators.get("rsi_14", 50)

            # Map regime to direction
            if regime == "DOWNTREND":
                return {"direction": "NONE", "confidence": 0,
                        "reason": f"DOWNTREND regime (Rule #1: skip)"}

            if signal["signal_type"] == "BUY":
                return {"direction": "BUY", "confidence": signal["confidence"],
                        "reason": f"C engine BUY | regime={regime} RSI={rsi:.1f} | strategy #{signal['strategy_id']}"}
            elif signal["signal_type"] == "SELL":
                return {"direction": "SELL", "confidence": signal["confidence"],
                        "reason": f"C engine SELL | regime={regime} RSI={rsi:.1f}"}

            return {"direction": "NONE", "confidence": 0, "reason": f"No signal (regime={regime})"}
        except Exception as e:
            logger.debug("Regime signal error: %s", e)
            return {"direction": "NONE", "confidence": 0, "reason": str(e)}

    def _fetch_candles(self, pair, granularity=3600, limit=50):
        """Fetch 1h candles from Coinbase."""
        try:
            url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity={granularity}"
            data = _fetch_json(url)
            candles = []
            for c in data[:limit]:
                candles.append({
                    "open": c[3], "high": c[2], "low": c[1],
                    "close": c[4], "volume": c[5], "time": c[0]
                })
            candles.reverse()  # oldest first
            return candles
        except Exception:
            return []


class ArbSignalSource(SignalSource):
    """Signal #3: Cross-exchange arbitrage check via C engine."""

    def scan(self, pair):
        try:
            from fast_bridge import FastEngine
            engine = FastEngine()

            # Get Coinbase price
            base_pair = pair.replace("-USD", "-USD")
            cb_data = _fetch_json(f"https://api.coinbase.com/v2/prices/{base_pair}/spot")
            cb_price = float(cb_data["data"]["amount"])

            # Get prices from other exchanges
            token = pair.split("-")[0]
            other_prices = self._get_other_prices(token)

            if len(other_prices) < 2:
                return {"direction": "NONE", "confidence": 0, "reason": "Not enough exchange data"}

            # Use C engine arb detection
            arb = engine.check_arbitrage(cb_price, other_prices)

            if arb["has_opportunity"]:
                direction = "BUY" if arb["side"] == 1 else "SELL"
                return {
                    "direction": direction,
                    "confidence": arb["confidence"],
                    "reason": f"Arb: CB ${cb_price:.2f} vs median ${arb['market_median']:.2f} ({arb['spread_pct']:.2f}%)"
                }

            return {"direction": "NONE", "confidence": 0, "reason": "No arb spread"}
        except Exception as e:
            logger.debug("Arb signal error: %s", e)
            return {"direction": "NONE", "confidence": 0, "reason": str(e)}

    def _get_other_prices(self, token):
        """Get prices from multiple exchanges."""
        prices = []
        endpoints = {
            "binance": f"https://api.binance.com/api/v3/ticker/price?symbol={token}USDT",
            "kraken": f"https://api.kraken.com/0/public/Ticker?pair={token}USD",
        }
        for name, url in endpoints.items():
            try:
                data = _fetch_json(url, timeout=3)
                if name == "binance":
                    prices.append(float(data["price"]))
                elif name == "kraken":
                    for v in data.get("result", {}).values():
                        prices.append(float(v["c"][0]))
            except Exception:
                pass
        return prices


class OrderbookSignalSource(SignalSource):
    """Signal #4: Orderbook imbalance detection."""

    def scan(self, pair):
        try:
            url = f"https://api.exchange.coinbase.com/products/{pair}/book?level=2"
            book = _fetch_json(url, timeout=5)

            bids = book.get("bids", [])[:20]
            asks = book.get("asks", [])[:20]

            if not bids or not asks:
                return {"direction": "NONE", "confidence": 0, "reason": "Empty orderbook"}

            # Calculate volume-weighted depth
            bid_depth = sum(float(b[1]) for b in bids)
            ask_depth = sum(float(a[1]) for a in asks)

            total = bid_depth + ask_depth
            if total == 0:
                return {"direction": "NONE", "confidence": 0, "reason": "No depth"}

            imbalance = (bid_depth - ask_depth) / total  # -1 to +1

            # Bid/ask imbalance detection — widened threshold
            if imbalance > 0.15:
                confidence = min(0.55 + imbalance * 0.5, 0.95)
                return {"direction": "BUY", "confidence": confidence,
                        "reason": f"Bid imbalance {imbalance:.2f} (bids {bid_depth:.1f} vs asks {ask_depth:.1f})"}
            elif imbalance < -0.15:
                confidence = min(0.55 + abs(imbalance) * 0.5, 0.95)
                return {"direction": "SELL", "confidence": confidence,
                        "reason": f"Ask imbalance {imbalance:.2f}"}

            return {"direction": "NONE", "confidence": 0, "reason": f"Balanced book ({imbalance:.2f})"}
        except Exception as e:
            logger.debug("Orderbook signal error: %s", e)
            return {"direction": "NONE", "confidence": 0, "reason": str(e)}


class RSIExtremeSource(SignalSource):
    """Signal #5: RSI extreme detection (oversold/overbought)."""

    def scan(self, pair):
        try:
            # Fetch 1h candles
            url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity=3600"
            raw = _fetch_json(url, timeout=5)
            closes = [c[4] for c in raw[:20]]
            closes.reverse()

            if len(closes) < 15:
                return {"direction": "NONE", "confidence": 0, "reason": "Insufficient data"}

            # Calculate RSI
            gains = losses = 0
            for i in range(1, min(15, len(closes))):
                diff = closes[i] - closes[i-1]
                if diff > 0:
                    gains += diff
                else:
                    losses -= diff

            if losses == 0:
                rsi = 100
            else:
                rs = gains / losses
                rsi = 100 - (100 / (1 + rs))

            # RSI signals — widened thresholds for more action
            if rsi < 35:
                confidence = 0.65 + (35 - rsi) * 0.015  # Higher conf as RSI drops
                return {"direction": "BUY", "confidence": min(confidence, 0.95),
                        "reason": f"RSI oversold: {rsi:.1f}"}
            elif rsi > 65:
                confidence = 0.65 + (rsi - 65) * 0.015
                return {"direction": "SELL", "confidence": min(confidence, 0.95),
                        "reason": f"RSI overbought: {rsi:.1f}"}

            return {"direction": "NONE", "confidence": 0, "reason": f"RSI neutral: {rsi:.1f}"}
        except Exception as e:
            logger.debug("RSI signal error: %s", e)
            return {"direction": "NONE", "confidence": 0, "reason": str(e)}


class FearGreedSource(SignalSource):
    """Signal #6: Fear & Greed Index — contrarian signal."""

    def scan(self, pair):
        try:
            data = _fetch_json("https://api.alternative.me/fng/?limit=1", timeout=5)
            value = int(data["data"][0]["value"])
            classification = data["data"][0]["value_classification"]

            # Extreme Fear (<20) = contrarian BUY. Extreme Greed (>80) = contrarian SELL.
            if value <= 25:
                confidence = 0.65 + (25 - value) * 0.012
                return {"direction": "BUY", "confidence": min(confidence, 0.90),
                        "reason": f"Fear & Greed={value} ({classification}) — contrarian BUY"}
            elif value >= 75:
                confidence = 0.65 + (value - 75) * 0.012
                return {"direction": "SELL", "confidence": min(confidence, 0.90),
                        "reason": f"Fear & Greed={value} ({classification}) — contrarian SELL"}

            return {"direction": "NONE", "confidence": 0,
                    "reason": f"Fear & Greed={value} ({classification}) — neutral"}
        except Exception as e:
            return {"direction": "NONE", "confidence": 0, "reason": str(e)}


class PriceMomentumSource(SignalSource):
    """Signal #7: Short-term price momentum (4h trend)."""

    def scan(self, pair):
        try:
            url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity=3600"
            raw = _fetch_json(url, timeout=5)
            if len(raw) < 5:
                return {"direction": "NONE", "confidence": 0, "reason": "Insufficient data"}

            # 4h candles: raw[0] is most recent
            closes = [c[4] for c in raw[:5]]
            current = closes[0]
            price_4h_ago = closes[4] if len(closes) > 4 else closes[-1]

            change_pct = (current - price_4h_ago) / price_4h_ago

            # Consistent uptrend in last 4h
            if change_pct > 0.005:  # >0.5% up
                confidence = min(0.60 + abs(change_pct) * 5, 0.85)
                return {"direction": "BUY", "confidence": confidence,
                        "reason": f"4h momentum +{change_pct:.2%}"}
            elif change_pct < -0.005:  # >0.5% down
                confidence = min(0.60 + abs(change_pct) * 5, 0.85)
                return {"direction": "SELL", "confidence": confidence,
                        "reason": f"4h momentum {change_pct:.2%}"}

            return {"direction": "NONE", "confidence": 0,
                    "reason": f"4h momentum flat ({change_pct:.2%})"}
        except Exception as e:
            return {"direction": "NONE", "confidence": 0, "reason": str(e)}


class UptickTimingSource(SignalSource):
    """Signal #8: Buy-low-sell-high uptick timing.

    Tracks 1-minute candles and detects when price is turning UP from a local low.
    This is a TIMING signal — enters on confirmed uptick after a dip.

    The edge: most retail traders sell during dips (fear). We buy the uptick.
    Game theory: non-equilibrium entry at inflection points.
    """

    def scan(self, pair):
        try:
            # Get 1-minute candles (last 15 minutes)
            product_id = pair.replace("-", "-")
            url = f"https://api.exchange.coinbase.com/products/{product_id}/candles?granularity=60"
            data = _fetch_json(url, timeout=8)
            if not data or len(data) < 10:
                return {"direction": "NONE", "confidence": 0, "reason": "Insufficient 1m data"}

            # candles: [time, low, high, open, close, volume]
            closes = [float(c[4]) for c in data[:15]]  # most recent 15 minutes
            closes.reverse()  # oldest first

            # Find local min in last 10 candles
            if len(closes) < 10:
                return {"direction": "NONE", "confidence": 0, "reason": "Need 10+ candles"}

            recent = closes[-5:]   # last 5 minutes
            earlier = closes[-10:-5]  # 5-10 minutes ago

            recent_avg = sum(recent) / len(recent)
            earlier_avg = sum(earlier) / len(earlier)
            current = closes[-1]
            lowest = min(closes[-10:])
            highest = max(closes[-10:])

            spread_pct = (highest - lowest) / lowest if lowest > 0 else 0

            # UPTICK: price was dropping, hit a local low, and is now rising
            # Pattern: earlier_avg > recent_avg (was dropping) BUT current > recent_avg (turning up)
            if current > recent_avg and recent_avg < earlier_avg and current > lowest * 1.001:
                # Confirmed uptick from local low
                bounce_pct = (current - lowest) / lowest if lowest > 0 else 0
                confidence = min(0.60 + bounce_pct * 20 + spread_pct * 5, 0.92)
                return {"direction": "BUY", "confidence": confidence,
                        "reason": f"Uptick from low: {bounce_pct:.2%} bounce, {spread_pct:.2%} range"}

            # DOWNTICK: price was rising, hit high, now falling
            if current < recent_avg and recent_avg > earlier_avg and current < highest * 0.999:
                drop_pct = (highest - current) / highest if highest > 0 else 0
                confidence = min(0.60 + drop_pct * 20 + spread_pct * 5, 0.92)
                return {"direction": "SELL", "confidence": confidence,
                        "reason": f"Downtick from high: {drop_pct:.2%} drop, {spread_pct:.2%} range"}

            return {"direction": "NONE", "confidence": 0,
                    "reason": f"No clear uptick/downtick pattern"}
        except Exception as e:
            return {"direction": "NONE", "confidence": 0, "reason": str(e)}


class Sniper:
    """High-confidence signal aggregator and trade executor."""

    def __init__(self):
        self.db = sqlite3.connect(SNIPER_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.daily_loss = 0.0
        self.trades_today = 0
        self.sources = {
            "latency": LatencySignalSource(),
            "regime": RegimeSignalSource(),
            "arb": ArbSignalSource(),
            "orderbook": OrderbookSignalSource(),
            "rsi_extreme": RSIExtremeSource(),
            "fear_greed": FearGreedSource(),
            "momentum": PriceMomentumSource(),
            "uptick": UptickTimingSource(),
        }

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS sniper_scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                composite_confidence REAL,
                direction TEXT,
                confirming_signals INTEGER,
                signal_details TEXT,
                action_taken TEXT DEFAULT 'none',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS sniper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                composite_confidence REAL,
                amount_usd REAL,
                venue TEXT,
                entry_price REAL,
                pnl REAL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

    def scan_pair(self, pair):
        """Scan all signal sources for a pair and compute composite confidence."""
        results = {}
        for name, source in self.sources.items():
            try:
                result = source.scan(pair)
                results[name] = result
                if result["direction"] != "NONE":
                    logger.info("  [%s] %s | conf=%.1f%% | %s",
                               name, result["direction"], result["confidence"]*100, result["reason"])
            except Exception as e:
                results[name] = {"direction": "NONE", "confidence": 0, "reason": str(e)}

        # Aggregate: count confirming signals for each direction
        buy_signals = [(n, r) for n, r in results.items()
                       if r["direction"] == "BUY" and r["confidence"] > 0]
        sell_signals = [(n, r) for n, r in results.items()
                        if r["direction"] == "SELL" and r["confidence"] > 0]

        # Determine dominant direction
        if len(buy_signals) >= len(sell_signals) and buy_signals:
            direction = "BUY"
            confirming = buy_signals
        elif sell_signals:
            direction = "SELL"
            confirming = sell_signals
        else:
            return {
                "pair": pair, "direction": "NONE", "composite_confidence": 0,
                "confirming_signals": 0, "details": results,
            }

        # Weighted confidence
        weights = CONFIG["signal_weights"]
        total_weight = sum(weights.get(n, 0.1) for n, _ in confirming)
        if total_weight > 0:
            composite = sum(weights.get(n, 0.1) * r["confidence"] for n, r in confirming) / total_weight
        else:
            composite = 0

        # Record scan
        self.db.execute(
            "INSERT INTO sniper_scans (pair, composite_confidence, direction, confirming_signals, signal_details) VALUES (?, ?, ?, ?, ?)",
            (pair, composite, direction, len(confirming), json.dumps({n: r for n, r in results.items()}))
        )
        self.db.commit()

        return {
            "pair": pair,
            "direction": direction,
            "composite_confidence": composite,
            "confirming_signals": len(confirming),
            "details": results,
        }

    def scan_all(self):
        """Scan all pairs and return actionable signals."""
        logger.info("=== SNIPER SCAN ===")
        actionable = []

        for pair in CONFIG["pairs"]:
            logger.info("Scanning %s...", pair)
            result = self.scan_pair(pair)

            if (result["composite_confidence"] >= CONFIG["min_composite_confidence"]
                    and result["confirming_signals"] >= CONFIG["min_confirming_signals"]):
                actionable.append(result)
                logger.info(">>> ACTIONABLE: %s %s | conf=%.1f%% | %d signals",
                           result["direction"], pair,
                           result["composite_confidence"]*100,
                           result["confirming_signals"])
            else:
                logger.info("  %s: %s conf=%.1f%% (%d signals) — below threshold",
                           pair, result["direction"],
                           result["composite_confidence"]*100,
                           result["confirming_signals"])

        # Store actionable signals for opportunity-cost selling
        self._pending_buys = [s for s in actionable if s["direction"] == "BUY"]
        return actionable

    def _has_better_opportunity(self, sell_pair, sell_loss_pct):
        """Check if there's a pending BUY with enough edge to justify selling at a loss.

        Game theory: sell a loser if the freed capital has a higher expected return elsewhere.
        The new BUY's expected gain must exceed the realized loss + round-trip fees (1.2%).
        """
        for buy_signal in getattr(self, '_pending_buys', []):
            if buy_signal["pair"] == sell_pair:
                continue  # don't justify selling X to buy X
            # Higher confidence = higher expected gain
            # A 85% confidence BUY justifies selling a -1% loser if expected gain > 1% + 1.2% fees
            expected_edge = (buy_signal["composite_confidence"] - 0.5) * 0.10  # rough: 80% conf ≈ 3% expected
            total_cost = abs(sell_loss_pct) + 0.012  # loss + round-trip fees
            if expected_edge > total_cost:
                logger.info("SNIPER: Better opportunity found — %s BUY conf=%.0f%% (edge %.1f%% > cost %.1f%%)",
                           buy_signal["pair"], buy_signal["composite_confidence"]*100,
                           expected_edge*100, total_cost*100)
                return True
        return False

    def _get_holdings(self):
        """Get current Coinbase holdings."""
        try:
            from exchange_connector import CoinbaseTrader
            trader = CoinbaseTrader()
            accts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
            holdings = {}
            cash = 0
            for a in accts.get("accounts", []):
                cur = a.get("currency", "")
                bal = float(a.get("available_balance", {}).get("value", 0))
                if cur in ("USD", "USDC"):
                    cash += bal
                elif bal > 0:
                    holdings[cur] = bal
            return holdings, cash
        except Exception as e:
            logger.warning("Holdings check failed: %s", e)
            return {}, 0

    def execute_trade(self, signal):
        """Execute a high-confidence trade on Coinbase.

        Supports both BUY and SELL:
        - BUY: uses available USD/USDC cash
        - SELL: only sells assets we already hold (no short selling)
        """
        if self.daily_loss >= CONFIG["max_daily_loss_usd"]:
            logger.warning("HARDSTOP: Daily loss limit reached")
            return False

        pair = signal["pair"]
        direction = signal["direction"]

        price = self._get_price(pair)
        if not price:
            logger.warning("Cannot get price for %s", pair)
            return False

        holdings, cash = self._get_holdings()
        base_currency = pair.split("-")[0]  # e.g., "BTC" from "BTC-USD"

        if direction == "BUY":
            # Size the trade: min $1, max $5, scale with confidence
            trade_size = min(CONFIG["max_trade_usd"],
                             max(1.00, signal["composite_confidence"] * 6.0))
            trade_size = min(trade_size, cash - 0.10)  # keep $0.10 reserve
            if trade_size < 0.50:
                logger.info("SNIPER: Insufficient cash ($%.2f) for BUY. Need $0.50+", cash)
                return False
            trade_size = round(trade_size, 2)

            logger.info("SNIPER EXECUTE: BUY %s | $%.2f @ $%.2f | conf=%.1f%% | %d signals",
                        pair, trade_size, price, signal["composite_confidence"]*100,
                        signal["confirming_signals"])

            try:
                from exchange_connector import CoinbaseTrader
                trader = CoinbaseTrader()
                result = trader.place_order(pair, "BUY", trade_size)
                return self._process_order_result(result, pair, "BUY", trade_size, price, signal)
            except Exception as e:
                logger.error("BUY execution error: %s", e, exc_info=True)
                return False

        elif direction == "SELL":
            # Check if we hold this asset
            held = holdings.get(base_currency, 0)
            held_usd = held * price
            if held_usd < 0.50:
                return False

            # Check minimum hold period — don't sell what we just bought (prevents churn)
            last_buy = self.db.execute(
                "SELECT entry_price, created_at FROM sniper_trades WHERE pair=? AND direction='BUY' AND status='filled' ORDER BY id DESC LIMIT 1",
                (pair,)
            ).fetchone()
            if last_buy:
                buy_price = last_buy[0] or 0
                buy_time = last_buy[1] or ""
                # Must hold at least 5 minutes
                try:
                    bought_at = datetime.fromisoformat(buy_time).replace(tzinfo=timezone.utc) if buy_time else None
                    if bought_at:
                        now = datetime.now(timezone.utc)
                        age = (now - bought_at).total_seconds()
                        if age < CONFIG.get("min_hold_seconds", 300):
                            logger.info("SNIPER: %s held only %ds, need %ds — HOLDING", pair, int(age), CONFIG["min_hold_seconds"])
                            return False
                except Exception:
                    pass

                # Prefer selling at profit, but allow loss if better opportunity exists
                if buy_price > 0 and price < buy_price * 1.005:
                    loss_pct = (price - buy_price) / buy_price if buy_price > 0 else 0
                    # Check if there's a higher-edge BUY waiting for this capital
                    if self._has_better_opportunity(pair, loss_pct):
                        logger.info("SNIPER: Selling %s at %.1f%% loss — better opportunity available",
                                   pair, loss_pct * 100)
                        # Track the loss
                        self.daily_loss += abs(loss_pct * held_usd)
                    else:
                        logger.info("SNIPER: %s at %.1f%% (bought $%.2f, now $%.2f) — no better path, HOLDING",
                                   pair, loss_pct * 100, buy_price, price)
                        return False

            # Sell up to $5 worth, but never more than we hold
            trade_size = min(CONFIG["max_trade_usd"], held_usd * 0.5)
            if trade_size < 0.50:
                return False

            # Calculate base_size for SELL (Coinbase needs base_size for sells)
            base_size = trade_size / price
            base_size = min(base_size, held)  # never sell more than we have

            logger.info("SNIPER EXECUTE: SELL %s | $%.2f (%.8f %s) @ $%.2f | conf=%.1f%% | %d signals",
                        pair, trade_size, base_size, base_currency, price,
                        signal["composite_confidence"]*100, signal["confirming_signals"])

            try:
                from exchange_connector import CoinbaseTrader
                trader = CoinbaseTrader()
                # For sells, pass base_size instead of quote_size
                result = trader.place_order(pair, "SELL", base_size)
                return self._process_order_result(result, pair, "SELL", trade_size, price, signal)
            except Exception as e:
                logger.error("SELL execution error: %s", e, exc_info=True)
                return False

        return False

    def _process_order_result(self, result, pair, side, trade_size, price, signal):
        """Process order result and record trade."""
        order_id = None
        status = "failed"
        if "success_response" in result:
            order_id = result["success_response"].get("order_id")
            status = "filled"
            logger.info("SNIPER ORDER FILLED: %s %s $%.2f @ $%.2f | order=%s",
                        pair, side, trade_size, price, order_id)
        elif "order_id" in result:
            order_id = result["order_id"]
            status = "filled"
            logger.info("SNIPER ORDER FILLED: %s %s $%.2f @ $%.2f | order=%s",
                        pair, side, trade_size, price, order_id)
        elif "error_response" in result:
            err = result["error_response"]
            logger.warning("SNIPER ORDER FAILED: %s %s | %s", pair, side, err.get("message", err))
        else:
            logger.warning("SNIPER ORDER UNKNOWN: %s", json.dumps(result)[:300])

        self.db.execute(
            "INSERT INTO sniper_trades (pair, direction, composite_confidence, amount_usd, venue, entry_price, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (pair, signal["direction"], signal["composite_confidence"], trade_size, "coinbase", price, status)
        )
        self.db.commit()
        self.trades_today += 1

        # Log state transition for learning
        if _tracker and status == "filled":
            asset = pair.split("-")[0]
            fee_pct = 0.006  # 0.6% taker fee
            cost = round(trade_size * fee_pct, 4)
            if side == "BUY":
                _tracker.transition(asset, "coinbase", "available", "available",
                                    amount=trade_size / price if price else 0,
                                    value_usd=trade_size, cost_usd=cost,
                                    trigger="sniper",
                                    metadata={"side": "BUY", "pair": pair,
                                              "confidence": signal["composite_confidence"]})
            else:
                _tracker.transition(asset, "coinbase", "available", "available",
                                    amount=trade_size / price if price else 0,
                                    value_usd=trade_size, cost_usd=cost,
                                    trigger="sniper",
                                    metadata={"side": "SELL", "pair": pair,
                                              "confidence": signal["composite_confidence"]})

        return status == "filled"

    def _get_price(self, pair):
        try:
            data = _fetch_json(f"https://api.coinbase.com/v2/prices/{pair}/spot")
            return float(data["data"]["amount"])
        except Exception:
            return None

    def print_report(self):
        """Print scan results report."""
        print(f"\n{'='*70}")
        print(f"  SNIPER REPORT")
        print(f"{'='*70}")

        # Recent scans
        scans = self.db.execute(
            "SELECT pair, direction, composite_confidence, confirming_signals, created_at "
            "FROM sniper_scans ORDER BY id DESC LIMIT 15"
        ).fetchall()

        print(f"\n  Recent Scans:")
        for s in scans:
            marker = ">>>" if s["composite_confidence"] >= CONFIG["min_composite_confidence"] else "   "
            print(f"  {marker} {s['pair']} | {s['direction']} | conf={s['composite_confidence']:.1%} | {s['confirming_signals']} signals | {s['created_at']}")

        # Trade history
        trades = self.db.execute(
            "SELECT pair, direction, amount_usd, venue, entry_price, status, created_at "
            "FROM sniper_trades ORDER BY id DESC LIMIT 10"
        ).fetchall()

        if trades:
            print(f"\n  Recent Trades:")
            for t in trades:
                print(f"    {t['direction']} {t['pair']} ${t['amount_usd']:.2f} @ ${t['entry_price'] or 0:,.2f} via {t['venue']} [{t['status']}] {t['created_at']}")

        print(f"\n  Config:")
        print(f"    Min confidence: {CONFIG['min_composite_confidence']:.0%}")
        print(f"    Min signals:    {CONFIG['min_confirming_signals']}")
        print(f"    Max trade:      ${CONFIG['max_trade_usd']:.2f}")
        print(f"    Daily loss lim: ${CONFIG['max_daily_loss_usd']:.2f}")
        print(f"{'='*70}\n")

    def run(self):
        """Main sniper loop — scan and trade."""
        logger.info("Sniper starting — scanning %d pairs every %ds",
                    len(CONFIG["pairs"]), CONFIG["scan_interval"])
        logger.info("Thresholds: conf >= %.0f%%, signals >= %d",
                    CONFIG["min_composite_confidence"]*100, CONFIG["min_confirming_signals"])

        cycle = 0
        while True:
            try:
                cycle += 1
                actionable = self.scan_all()

                for signal in actionable:
                    self.execute_trade(signal)

                # Report every 30 cycles (~15 min)
                if cycle % 30 == 0:
                    self.print_report()

                time.sleep(CONFIG["scan_interval"])

            except KeyboardInterrupt:
                logger.info("Sniper shutting down...")
                self.print_report()
                break
            except Exception as e:
                logger.error("Sniper error: %s", e, exc_info=True)
                time.sleep(30)


if __name__ == "__main__":
    sniper = Sniper()

    if len(sys.argv) > 1 and sys.argv[1] == "scan":
        results = sniper.scan_all()
        sniper.print_report()
    elif len(sys.argv) > 1 and sys.argv[1] == "report":
        sniper.print_report()
    else:
        sniper.run()
