#!/usr/bin/env python3
"""Strategy Pipeline: COLD → WARM → HOT

Every strategy MUST prove itself before touching real money.

Pipeline stages:
  COLD (Backtest)  → Run against historical price data. Must show profit.
  WARM (Paper)     → Run live with fake money. Must maintain profit for 1h+.
  HOT  (Live)      → Real money. Only strategies that passed COLD + WARM.

Promotion criteria:
  COLD → WARM: win_rate > 60%, total_return > 0%, max_drawdown < 5%
  WARM → HOT:  win_rate > 55%, total_return > 0% over 1h+, sharpe > 0.5

RULE #1: NEVER LOSE MONEY.
Any strategy that loses money in WARM gets demoted back to COLD.
Any strategy that loses money in HOT gets KILLED immediately.
"""

import json
import logging
import os
import sqlite3
import sys
import time
import math
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "strategy_pipeline.log")),
    ]
)
logger = logging.getLogger("strategy_pipeline")

PIPELINE_DB = str(Path(__file__).parent / "pipeline.db")

# Promotion thresholds — CONSERVATIVE
COLD_TO_WARM = {
    "min_trades": 20,
    "min_win_rate": 0.60,      # 60% win rate in backtest
    "min_return_pct": 0.5,     # Must be profitable
    "max_drawdown_pct": 5.0,   # Max 5% drawdown
}

WARM_TO_HOT = {
    "min_trades": 10,
    "min_win_rate": 0.55,      # 55% win rate in paper trading
    "min_return_pct": 0.1,     # Must be profitable
    "max_drawdown_pct": 3.0,   # Max 3% drawdown (tighter for real money)
    "min_runtime_seconds": 3600,  # Must run for at least 1 hour
    "min_sharpe": 0.5,
}

# Fee assumptions
COINBASE_FEE = 0.006  # 0.6% taker fee
SLIPPAGE = 0.001      # 0.1% slippage assumption


class HistoricalPrices:
    """Fetch and cache historical price data for backtesting."""

    def __init__(self):
        self.cache = {}

    def get_candles(self, pair, hours=168):
        """Get hourly candles from Coinbase PUBLIC API (no auth needed)."""
        cache_key = f"{pair}_{hours}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        candles = self._fetch_public_candles(pair, "3600", hours)
        self.cache[cache_key] = candles
        return candles

    def get_5min_candles(self, pair, hours=24):
        """Get 5-minute candles for more granular backtesting."""
        return self._fetch_public_candles(pair, "300", hours)

    def _fetch_public_candles(self, pair, granularity_seconds, hours):
        """Fetch candles from Coinbase Exchange public API (no auth needed)."""
        # Coinbase Exchange API: max 300 candles per request
        candles = []
        end = int(time.time())
        start = end - (hours * 3600)
        gran = int(granularity_seconds)

        # Chunk into 300-candle batches
        chunk_seconds = 300 * gran
        current_start = start

        while current_start < end:
            current_end = min(current_start + chunk_seconds, end)
            url = (f"https://api.exchange.coinbase.com/products/{pair}/candles"
                   f"?start={datetime.fromtimestamp(current_start, tz=timezone.utc).isoformat()}"
                   f"&end={datetime.fromtimestamp(current_end, tz=timezone.utc).isoformat()}"
                   f"&granularity={gran}")
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode())
                for c in data:
                    # Format: [time, low, high, open, close, volume]
                    candles.append({
                        "time": int(c[0]),
                        "low": float(c[1]),
                        "high": float(c[2]),
                        "open": float(c[3]),
                        "close": float(c[4]),
                        "volume": float(c[5]),
                    })
            except Exception as e:
                logger.warning("Candle fetch failed for %s: %s", pair, e)
                break

            current_start = current_end
            time.sleep(0.3)  # Rate limit

        candles.sort(key=lambda x: x["time"])
        # Deduplicate by time
        seen = set()
        unique = []
        for c in candles:
            if c["time"] not in seen:
                seen.add(c["time"])
                unique.append(c)
        return unique


# ============================================================
# STRATEGIES — Each must implement generate_signals(candles)
# Returns list of {"time", "side", "confidence", "reason"}
# ============================================================

class MeanReversionStrategy:
    """Buy when price drops below lower Bollinger Band, sell when above upper.

    Classic mean reversion — works well in ranging markets.
    """
    name = "mean_reversion"

    def __init__(self, window=20, num_std=2.0):
        self.window = window
        self.num_std = num_std

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]

        for i in range(self.window, len(candles)):
            window_closes = closes[i - self.window:i]
            mean = sum(window_closes) / len(window_closes)
            variance = sum((x - mean) ** 2 for x in window_closes) / len(window_closes)
            std = math.sqrt(variance) if variance > 0 else 0.001

            upper = mean + self.num_std * std
            lower = mean - self.num_std * std
            price = closes[i]

            if price < lower:
                # Price below lower band — BUY (expect reversion to mean)
                distance = (lower - price) / std if std > 0 else 0
                confidence = min(0.95, 0.60 + distance * 0.1)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": round(confidence, 3),
                    "reason": f"price_below_lower_bb (z={-distance:.2f})",
                    "price": price,
                })
            elif price > upper:
                # Price above upper band — potential sell signal (logged but not acted on with buy-only)
                distance = (price - upper) / std if std > 0 else 0
                confidence = min(0.95, 0.60 + distance * 0.1)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "SELL",
                    "confidence": round(confidence, 3),
                    "reason": f"price_above_upper_bb (z={distance:.2f})",
                    "price": price,
                })

        return signals


class MomentumStrategy:
    """Buy when short MA crosses above long MA with volume confirmation."""
    name = "momentum"

    def __init__(self, short_window=5, long_window=20, volume_mult=1.5):
        self.short_window = short_window
        self.long_window = long_window
        self.volume_mult = volume_mult

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        for i in range(self.long_window + 1, len(candles)):
            short_ma = sum(closes[i - self.short_window:i]) / self.short_window
            long_ma = sum(closes[i - self.long_window:i]) / self.long_window
            prev_short = sum(closes[i - self.short_window - 1:i - 1]) / self.short_window
            prev_long = sum(closes[i - self.long_window - 1:i - 1]) / self.long_window

            avg_vol = sum(volumes[i - self.long_window:i]) / self.long_window if self.long_window > 0 else 1
            curr_vol = volumes[i]

            # Golden cross — short MA crosses above long MA
            if prev_short <= prev_long and short_ma > long_ma:
                vol_conf = min(1.0, curr_vol / (avg_vol * self.volume_mult)) if avg_vol > 0 else 0.5
                confidence = min(0.90, 0.55 + vol_conf * 0.2 + (short_ma - long_ma) / long_ma * 10)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": round(max(0.5, confidence), 3),
                    "reason": f"golden_cross (vol={vol_conf:.2f})",
                    "price": closes[i],
                })

            # Death cross — short MA crosses below long MA
            elif prev_short >= prev_long and short_ma < long_ma:
                vol_conf = min(1.0, curr_vol / (avg_vol * self.volume_mult)) if avg_vol > 0 else 0.5
                confidence = min(0.90, 0.55 + vol_conf * 0.2 + (long_ma - short_ma) / long_ma * 10)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "SELL",
                    "confidence": round(max(0.5, confidence), 3),
                    "reason": f"death_cross (vol={vol_conf:.2f})",
                    "price": closes[i],
                })

        return signals


class RSIStrategy:
    """Buy when RSI < 30 (oversold), sell when RSI > 70 (overbought)."""
    name = "rsi"

    def __init__(self, period=14, oversold=30, overbought=70):
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    def _compute_rsi(self, closes, i):
        if i < self.period + 1:
            return 50  # neutral

        gains = []
        losses = []
        for j in range(i - self.period, i):
            change = closes[j] - closes[j - 1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))

        avg_gain = sum(gains) / self.period
        avg_loss = sum(losses) / self.period

        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]

        for i in range(self.period + 1, len(candles)):
            rsi = self._compute_rsi(closes, i)

            if rsi < self.oversold:
                confidence = min(0.95, 0.60 + (self.oversold - rsi) / 100)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": round(confidence, 3),
                    "reason": f"rsi_oversold ({rsi:.1f})",
                    "price": closes[i],
                })
            elif rsi > self.overbought:
                confidence = min(0.95, 0.60 + (rsi - self.overbought) / 100)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "SELL",
                    "confidence": round(confidence, 3),
                    "reason": f"rsi_overbought ({rsi:.1f})",
                    "price": closes[i],
                })

        return signals


class VWAPStrategy:
    """Buy below VWAP, sell above. Uses volume-weighted average price as fair value."""
    name = "vwap"

    def __init__(self, deviation_pct=0.005):
        self.deviation_pct = deviation_pct

    def generate_signals(self, candles):
        signals = []

        cum_vol_price = 0
        cum_vol = 0

        for i, c in enumerate(candles):
            cum_vol_price += c["close"] * c["volume"]
            cum_vol += c["volume"]

            if cum_vol == 0 or i < 5:
                continue

            vwap = cum_vol_price / cum_vol
            deviation = (c["close"] - vwap) / vwap

            if deviation < -self.deviation_pct:
                confidence = min(0.90, 0.55 + abs(deviation) * 20)
                signals.append({
                    "time": c["time"],
                    "side": "BUY",
                    "confidence": round(confidence, 3),
                    "reason": f"below_vwap ({deviation*100:.2f}%)",
                    "price": c["close"],
                })
            elif deviation > self.deviation_pct:
                confidence = min(0.90, 0.55 + abs(deviation) * 20)
                signals.append({
                    "time": c["time"],
                    "side": "SELL",
                    "confidence": round(confidence, 3),
                    "reason": f"above_vwap ({deviation*100:.2f}%)",
                    "price": c["close"],
                })

        return signals


class DipBuyerStrategy:
    """Buy significant dips (>1.5% drops in 4 candles), sell on recovery.

    Designed for Coinbase's 0.6% fee — only trades when expected recovery > 2x fee.
    """
    name = "dip_buyer"

    def __init__(self, dip_threshold=0.015, recovery_target=0.02, lookback=4):
        self.dip_threshold = dip_threshold
        self.recovery_target = recovery_target
        self.lookback = lookback

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]

        for i in range(self.lookback, len(candles)):
            recent_high = max(closes[i - self.lookback:i])
            price = closes[i]

            # Check for dip from recent high
            drop_pct = (recent_high - price) / recent_high

            if drop_pct >= self.dip_threshold:
                # Volume confirmation: is current volume above average?
                avg_vol = sum(c["volume"] for c in candles[max(0, i-20):i]) / min(20, i) if i > 0 else 1
                vol_ratio = candles[i]["volume"] / avg_vol if avg_vol > 0 else 1

                confidence = min(0.95, 0.60 + drop_pct * 5 + max(0, vol_ratio - 1) * 0.1)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": round(confidence, 3),
                    "reason": f"dip_{drop_pct*100:.1f}pct (vol={vol_ratio:.1f}x)",
                    "price": price,
                })

            # Check for recovery sell signal (price recovered above buy + fee + target)
            if i > self.lookback + 2:
                recent_low = min(closes[i - self.lookback:i])
                recovery_pct = (price - recent_low) / recent_low

                if recovery_pct >= self.recovery_target:
                    confidence = min(0.90, 0.55 + recovery_pct * 5)
                    signals.append({
                        "time": candles[i]["time"],
                        "side": "SELL",
                        "confidence": round(confidence, 3),
                        "reason": f"recovery_{recovery_pct*100:.1f}pct",
                        "price": price,
                    })

        return signals


class MultiTimeframeStrategy:
    """Combine RSI + Bollinger + Volume across multiple timeframes.

    Only buys when ALL three agree: RSI oversold + below BB + high volume.
    This high-confirmation approach minimizes false signals.
    """
    name = "multi_timeframe"

    def __init__(self, rsi_period=14, bb_window=20, bb_std=2.0):
        self.rsi_period = rsi_period
        self.bb_window = bb_window
        self.bb_std = bb_std

    def _rsi(self, closes, i):
        if i < self.rsi_period + 1:
            return 50
        gains, losses = [], []
        for j in range(i - self.rsi_period, i):
            change = closes[j] - closes[j - 1]
            gains.append(max(0, change))
            losses.append(max(0, -change))
        avg_gain = sum(gains) / self.rsi_period
        avg_loss = sum(losses) / self.rsi_period
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        for i in range(max(self.bb_window, self.rsi_period + 1), len(candles)):
            price = closes[i]

            # RSI
            rsi = self._rsi(closes, i)

            # Bollinger Bands
            window_closes = closes[i - self.bb_window:i]
            mean = sum(window_closes) / len(window_closes)
            variance = sum((x - mean) ** 2 for x in window_closes) / len(window_closes)
            std = math.sqrt(variance) if variance > 0 else 0.001
            lower_bb = mean - self.bb_std * std
            upper_bb = mean + self.bb_std * std

            # Volume
            avg_vol = sum(volumes[max(0, i-20):i]) / min(20, i) if i > 0 else 1
            vol_ratio = volumes[i] / avg_vol if avg_vol > 0 else 1

            # BUY: RSI oversold + below lower BB + volume spike
            if rsi < 35 and price < lower_bb and vol_ratio > 1.2:
                rsi_score = (35 - rsi) / 35
                bb_score = (lower_bb - price) / std if std > 0 else 0
                vol_score = min(1, (vol_ratio - 1) / 2)
                confidence = min(0.95, 0.65 + rsi_score * 0.1 + bb_score * 0.05 + vol_score * 0.1)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": round(confidence, 3),
                    "reason": f"multi_confirm (RSI={rsi:.0f}, BB_dist={bb_score:.2f}, vol={vol_ratio:.1f}x)",
                    "price": price,
                })

            # SELL: RSI overbought + above upper BB
            elif rsi > 65 and price > upper_bb:
                rsi_score = (rsi - 65) / 35
                bb_score = (price - upper_bb) / std if std > 0 else 0
                confidence = min(0.90, 0.60 + rsi_score * 0.1 + bb_score * 0.05)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "SELL",
                    "confidence": round(confidence, 3),
                    "reason": f"multi_overbought (RSI={rsi:.0f}, BB_dist={bb_score:.2f})",
                    "price": price,
                })

        return signals


class AccumulateAndHoldStrategy:
    """DCA on schedule + buy extra on dips. Never sell at a loss.

    Designed for small accounts: accumulate quality assets consistently.
    Sell only when holding for 24h+ AND price > entry + 2% (covers fees + profit).
    """
    name = "accumulate_hold"

    def __init__(self, dca_interval=12, dip_threshold=0.02, profit_target=0.02):
        self.dca_interval = dca_interval  # Buy every N candles (at 5min = every hour)
        self.dip_threshold = dip_threshold
        self.profit_target = profit_target

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]

        for i in range(20, len(candles)):
            price = closes[i]

            # DCA buy on schedule
            if i % self.dca_interval == 0:
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": 0.60,
                    "reason": "dca_scheduled",
                    "price": price,
                })

            # Extra buy on significant dips
            if i >= 5:
                recent_high = max(closes[i-5:i])
                drop = (recent_high - price) / recent_high
                if drop >= self.dip_threshold:
                    confidence = min(0.90, 0.65 + drop * 5)
                    signals.append({
                        "time": candles[i]["time"],
                        "side": "BUY",
                        "confidence": round(confidence, 3),
                        "reason": f"dip_buy ({drop*100:.1f}%)",
                        "price": price,
                    })

            # Sell only on sustained rally (well above 24h low)
            if i >= 288:  # ~24h of 5-min candles
                low_24h = min(closes[i-288:i])
                gain = (price - low_24h) / low_24h
                if gain >= self.profit_target:
                    confidence = min(0.85, 0.55 + gain * 5)
                    signals.append({
                        "time": candles[i]["time"],
                        "side": "SELL",
                        "confidence": round(confidence, 3),
                        "reason": f"profit_take ({gain*100:.1f}%)",
                        "price": price,
                    })

        return signals


# ============================================================
# BACKTESTER — Simulates trades against historical data
# ============================================================

class Backtester:
    """Run a strategy against historical data and compute performance metrics."""

    def __init__(self, initial_capital=100.0, fee_rate=COINBASE_FEE, slippage=SLIPPAGE):
        self.initial_capital = initial_capital
        self.fee_rate = fee_rate
        self.slippage = slippage

    def run(self, strategy, candles, pair="BTC-USD"):
        """Run backtest. Returns performance metrics."""
        signals = strategy.generate_signals(candles)

        capital = self.initial_capital
        position = 0.0  # amount of base asset held
        entry_price = 0.0
        trades = []
        equity_curve = [capital]
        peak_equity = capital

        for sig in signals:
            price = sig["price"]
            side = sig["side"]
            confidence = sig["confidence"]

            # Apply slippage
            if side == "BUY":
                exec_price = price * (1 + self.slippage)
            else:
                exec_price = price * (1 - self.slippage)

            fee = self.fee_rate

            if side == "BUY" and position == 0 and capital > 1.0:
                # Size based on confidence
                trade_pct = min(0.5, confidence * 0.4)
                trade_usd = capital * trade_pct
                position = trade_usd / exec_price * (1 - fee)
                entry_price = exec_price
                capital -= trade_usd
                trades.append({
                    "time": sig["time"],
                    "side": "BUY",
                    "price": exec_price,
                    "amount": position,
                    "usd": trade_usd,
                    "reason": sig["reason"],
                    "confidence": confidence,
                })

            elif side == "SELL" and position > 0:
                # Only sell at profit (Rule #1: NEVER LOSE MONEY)
                gross_value = position * exec_price
                net_value = gross_value * (1 - fee)
                cost_basis = position * entry_price

                if net_value > cost_basis:  # Profitable trade
                    pnl = net_value - cost_basis
                    capital += net_value
                    trades.append({
                        "time": sig["time"],
                        "side": "SELL",
                        "price": exec_price,
                        "amount": position,
                        "usd": net_value,
                        "pnl": round(pnl, 4),
                        "reason": sig["reason"],
                        "confidence": confidence,
                    })
                    position = 0
                    entry_price = 0

            # Track equity
            if position > 0:
                curr_equity = capital + position * price
            else:
                curr_equity = capital
            equity_curve.append(curr_equity)
            peak_equity = max(peak_equity, curr_equity)

        # Close any remaining position at last price
        if position > 0 and len(candles) > 0:
            final_price = candles[-1]["close"]
            final_value = position * final_price * (1 - self.fee_rate)
            capital += final_value
            position = 0

        # Compute metrics
        total_return = (capital - self.initial_capital) / self.initial_capital
        wins = [t for t in trades if t.get("pnl", 0) > 0]
        losses = [t for t in trades if t.get("pnl", 0) < 0]
        sell_trades = [t for t in trades if t["side"] == "SELL"]
        win_rate = len(wins) / len(sell_trades) if sell_trades else 0

        # Max drawdown
        max_drawdown = 0
        peak = equity_curve[0]
        for eq in equity_curve:
            peak = max(peak, eq)
            dd = (peak - eq) / peak if peak > 0 else 0
            max_drawdown = max(max_drawdown, dd)

        # Sharpe ratio (simplified — hourly returns)
        returns = []
        for i in range(1, len(equity_curve)):
            if equity_curve[i - 1] > 0:
                returns.append((equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1])
        if returns:
            avg_return = sum(returns) / len(returns)
            std_return = math.sqrt(sum((r - avg_return) ** 2 for r in returns) / len(returns)) if len(returns) > 1 else 0.001
            sharpe = (avg_return / std_return) * math.sqrt(len(returns)) if std_return > 0 else 0
        else:
            sharpe = 0

        return {
            "strategy": strategy.name,
            "pair": pair,
            "initial_capital": self.initial_capital,
            "final_capital": round(capital, 4),
            "total_return_pct": round(total_return * 100, 4),
            "total_trades": len(trades),
            "buy_trades": len([t for t in trades if t["side"] == "BUY"]),
            "sell_trades": len(sell_trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(win_rate, 4),
            "max_drawdown_pct": round(max_drawdown * 100, 4),
            "sharpe_ratio": round(sharpe, 4),
            "total_pnl": round(capital - self.initial_capital, 4),
            "trades": trades,
            "equity_curve_start": equity_curve[0] if equity_curve else 0,
            "equity_curve_end": equity_curve[-1] if equity_curve else 0,
        }


# ============================================================
# PAPER TRADER — Runs strategy live with fake money
# ============================================================

class PaperTrader:
    """Run a strategy with fake money against live prices."""

    def __init__(self, strategy, pair, initial_capital=100.0):
        self.strategy = strategy
        self.pair = pair
        self.capital = initial_capital
        self.initial_capital = initial_capital
        self.position = 0.0
        self.entry_price = 0.0
        self.trades = []
        self.equity_history = []
        self.start_time = time.time()
        self.peak_equity = initial_capital

    def tick(self, price):
        """Process one price tick."""
        from exchange_connector import PriceFeed

        # Build a mini-candle from recent prices for signal generation
        # In paper mode, we just use the latest price as a candle
        candle = {
            "time": int(time.time()),
            "open": price,
            "high": price * 1.001,
            "low": price * 0.999,
            "close": price,
            "volume": 1000,
        }

        # Get current equity
        equity = self.capital + (self.position * price if self.position > 0 else 0)
        self.equity_history.append({"time": time.time(), "equity": equity})
        self.peak_equity = max(self.peak_equity, equity)

        return equity

    def get_metrics(self):
        """Get current paper trading metrics."""
        runtime = time.time() - self.start_time
        equity = self.equity_history[-1]["equity"] if self.equity_history else self.initial_capital
        total_return = (equity - self.initial_capital) / self.initial_capital

        wins = [t for t in self.trades if t.get("pnl", 0) > 0]
        sell_trades = [t for t in self.trades if t["side"] == "SELL"]
        win_rate = len(wins) / len(sell_trades) if sell_trades else 0

        max_drawdown = 0
        peak = self.initial_capital
        for entry in self.equity_history:
            peak = max(peak, entry["equity"])
            dd = (peak - entry["equity"]) / peak if peak > 0 else 0
            max_drawdown = max(max_drawdown, dd)

        return {
            "strategy": self.strategy.name,
            "pair": self.pair,
            "runtime_seconds": int(runtime),
            "equity": round(equity, 4),
            "total_return_pct": round(total_return * 100, 4),
            "total_trades": len(self.trades),
            "win_rate": round(win_rate, 4),
            "max_drawdown_pct": round(max_drawdown * 100, 4),
        }


# ============================================================
# STRATEGY VALIDATOR — Gates promotion between pipeline stages
# ============================================================

class StrategyValidator:
    """Validates strategy performance and manages pipeline promotion."""

    def __init__(self):
        self.db = sqlite3.connect(PIPELINE_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS strategy_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                pair TEXT NOT NULL,
                stage TEXT DEFAULT 'COLD',
                params_json TEXT,
                backtest_results_json TEXT,
                paper_results_json TEXT,
                promoted_at TIMESTAMP,
                demoted_at TIMESTAMP,
                killed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(name, pair)
            );
            CREATE TABLE IF NOT EXISTS pipeline_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_name TEXT,
                pair TEXT,
                event_type TEXT,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

    def register_strategy(self, strategy, pair, params=None):
        """Register a new strategy in the pipeline."""
        self.db.execute(
            """INSERT OR IGNORE INTO strategy_registry (name, pair, params_json)
               VALUES (?, ?, ?)""",
            (strategy.name, pair, json.dumps(params or {}))
        )
        self.db.commit()
        self._log_event(strategy.name, pair, "registered", {})

    def submit_backtest(self, strategy_name, pair, results):
        """Submit backtest results and check for COLD → WARM promotion."""
        self.db.execute(
            """UPDATE strategy_registry SET backtest_results_json=?, updated_at=CURRENT_TIMESTAMP
               WHERE name=? AND pair=?""",
            (json.dumps(results), strategy_name, pair)
        )
        self.db.commit()

        # Check promotion criteria
        passed = True
        reasons = []

        if results["total_trades"] < COLD_TO_WARM["min_trades"]:
            passed = False
            reasons.append(f"trades {results['total_trades']} < {COLD_TO_WARM['min_trades']}")

        if results["win_rate"] < COLD_TO_WARM["min_win_rate"]:
            passed = False
            reasons.append(f"win_rate {results['win_rate']:.2%} < {COLD_TO_WARM['min_win_rate']:.2%}")

        if results["total_return_pct"] < COLD_TO_WARM["min_return_pct"]:
            passed = False
            reasons.append(f"return {results['total_return_pct']:.2f}% < {COLD_TO_WARM['min_return_pct']}%")

        if results["max_drawdown_pct"] > COLD_TO_WARM["max_drawdown_pct"]:
            passed = False
            reasons.append(f"drawdown {results['max_drawdown_pct']:.2f}% > {COLD_TO_WARM['max_drawdown_pct']}%")

        if passed:
            self.db.execute(
                """UPDATE strategy_registry SET stage='WARM', promoted_at=CURRENT_TIMESTAMP
                   WHERE name=? AND pair=?""",
                (strategy_name, pair)
            )
            self.db.commit()
            self._log_event(strategy_name, pair, "promoted_to_WARM", results)
            logger.info("PROMOTED %s/%s to WARM (backtest passed)", strategy_name, pair)
            return True, "Promoted to WARM"
        else:
            self._log_event(strategy_name, pair, "failed_COLD", {"reasons": reasons})
            logger.warning("REJECTED %s/%s from WARM: %s", strategy_name, pair, ", ".join(reasons))
            return False, f"Failed: {', '.join(reasons)}"

    def check_warm_promotion(self, strategy_name, pair, paper_metrics):
        """Check if a WARM strategy should be promoted to HOT."""
        criteria = WARM_TO_HOT

        passed = True
        reasons = []

        if paper_metrics.get("total_trades", 0) < criteria["min_trades"]:
            passed = False
            reasons.append(f"trades {paper_metrics['total_trades']} < {criteria['min_trades']}")

        if paper_metrics.get("win_rate", 0) < criteria["min_win_rate"]:
            passed = False
            reasons.append(f"win_rate {paper_metrics['win_rate']:.2%} < {criteria['min_win_rate']:.2%}")

        if paper_metrics.get("total_return_pct", 0) < criteria["min_return_pct"]:
            passed = False
            reasons.append(f"return {paper_metrics['total_return_pct']:.2f}% < {criteria['min_return_pct']}%")

        if paper_metrics.get("max_drawdown_pct", 0) > criteria["max_drawdown_pct"]:
            passed = False
            reasons.append(f"drawdown {paper_metrics['max_drawdown_pct']:.2f}% > {criteria['max_drawdown_pct']}%")

        if paper_metrics.get("runtime_seconds", 0) < criteria["min_runtime_seconds"]:
            passed = False
            reasons.append(f"runtime {paper_metrics['runtime_seconds']}s < {criteria['min_runtime_seconds']}s")

        if passed:
            self.db.execute(
                """UPDATE strategy_registry SET stage='HOT', paper_results_json=?,
                   promoted_at=CURRENT_TIMESTAMP WHERE name=? AND pair=?""",
                (json.dumps(paper_metrics), strategy_name, pair)
            )
            self.db.commit()
            self._log_event(strategy_name, pair, "promoted_to_HOT", paper_metrics)
            logger.info("PROMOTED %s/%s to HOT (paper trading passed)", strategy_name, pair)
            return True, "Promoted to HOT"
        else:
            return False, f"Not ready: {', '.join(reasons)}"

    def demote_strategy(self, strategy_name, pair, reason="loss"):
        """Demote a strategy back to COLD (losing money)."""
        self.db.execute(
            """UPDATE strategy_registry SET stage='COLD', demoted_at=CURRENT_TIMESTAMP
               WHERE name=? AND pair=?""",
            (strategy_name, pair)
        )
        self.db.commit()
        self._log_event(strategy_name, pair, "demoted_to_COLD", {"reason": reason})
        logger.warning("DEMOTED %s/%s to COLD: %s", strategy_name, pair, reason)

    def kill_strategy(self, strategy_name, pair, reason="loss"):
        """Kill a strategy permanently (lost money in HOT)."""
        self.db.execute(
            """UPDATE strategy_registry SET stage='KILLED', killed_at=CURRENT_TIMESTAMP
               WHERE name=? AND pair=?""",
            (strategy_name, pair)
        )
        self.db.commit()
        self._log_event(strategy_name, pair, "KILLED", {"reason": reason})
        logger.error("KILLED %s/%s: %s", strategy_name, pair, reason)

    def get_hot_strategies(self):
        """Get all strategies currently approved for HOT (live) trading."""
        rows = self.db.execute(
            "SELECT * FROM strategy_registry WHERE stage='HOT'"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_all_strategies(self):
        """Get all registered strategies."""
        rows = self.db.execute(
            "SELECT name, pair, stage, promoted_at, demoted_at, killed_at, created_at FROM strategy_registry ORDER BY stage, name"
        ).fetchall()
        return [dict(r) for r in rows]

    def _log_event(self, name, pair, event_type, details):
        self.db.execute(
            "INSERT INTO pipeline_events (strategy_name, pair, event_type, details_json) VALUES (?, ?, ?, ?)",
            (name, pair, event_type, json.dumps(details))
        )
        self.db.commit()


# ============================================================
# MARKET REGIME DETECTOR — Classify market conditions
# ============================================================

class MarketRegimeDetector:
    """Analyze candle data to classify the current market regime.

    Regimes:
      UPTREND   — SMA slope positive, price above SMA  → TRADE
      DOWNTREND — SMA slope negative, price below SMA  → HOLD
      RANGING   — Low ATR, price crossing SMA often     → TRADE (mean reversion)
      VOLATILE  — High ATR                              → ACCUMULATE (buy dips only)
    """

    REGIME_UPTREND = "UPTREND"
    REGIME_DOWNTREND = "DOWNTREND"
    REGIME_RANGING = "RANGING"
    REGIME_VOLATILE = "VOLATILE"

    def __init__(self, sma_period=20, atr_period=14, atr_volatile_threshold=0.03,
                 atr_ranging_threshold=0.01, crossover_threshold=5, adx_trend_threshold=25):
        self.sma_period = sma_period
        self.atr_period = atr_period
        self.atr_volatile_threshold = atr_volatile_threshold  # ATR/price ratio above this = volatile
        self.atr_ranging_threshold = atr_ranging_threshold    # ATR/price ratio below this = ranging
        self.crossover_threshold = crossover_threshold        # SMA crosses in lookback to count as ranging
        self.adx_trend_threshold = adx_trend_threshold        # ADX above this = trending

    def _compute_sma(self, closes, period):
        """Compute Simple Moving Average for the last `period` values."""
        if len(closes) < period:
            return None
        return sum(closes[-period:]) / period

    def _compute_sma_slope(self, closes, period, lookback=5):
        """Compute the slope of the SMA over `lookback` periods.

        Returns the average per-period change in SMA, normalized by price.
        A positive slope means the SMA is rising (uptrend signal).
        """
        if len(closes) < period + lookback:
            return 0.0

        sma_values = []
        for i in range(lookback + 1):
            idx = len(closes) - lookback + i
            window = closes[idx - period:idx]
            sma_values.append(sum(window) / period)

        # Linear slope: average change per period, normalized by current SMA
        total_change = sma_values[-1] - sma_values[0]
        current_sma = sma_values[-1]
        if current_sma == 0:
            return 0.0
        return (total_change / lookback) / current_sma

    def _compute_atr(self, candles, period):
        """Compute Average True Range over `period` candles.

        True Range = max(high - low, |high - prev_close|, |low - prev_close|)
        ATR = SMA of True Range over `period`.
        """
        if len(candles) < period + 1:
            return 0.0

        true_ranges = []
        for i in range(len(candles) - period, len(candles)):
            high = candles[i]["high"]
            low = candles[i]["low"]
            prev_close = candles[i - 1]["close"] if i > 0 else candles[i]["open"]

            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close),
            )
            true_ranges.append(tr)

        return sum(true_ranges) / len(true_ranges) if true_ranges else 0.0

    def _compute_adx_like_strength(self, candles, period=14):
        """Compute an ADX-like trend strength measure (0-100).

        Uses directional movement (+DM / -DM) to gauge how strongly the
        market is trending regardless of direction.  Higher values indicate
        a stronger trend; lower values indicate a range-bound market.
        """
        if len(candles) < period + 1:
            return 0.0

        plus_dm_list = []
        minus_dm_list = []
        tr_list = []

        start_idx = max(1, len(candles) - period - 1)
        for i in range(start_idx, len(candles)):
            high = candles[i]["high"]
            low = candles[i]["low"]
            prev_high = candles[i - 1]["high"]
            prev_low = candles[i - 1]["low"]
            prev_close = candles[i - 1]["close"]

            # True Range
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_list.append(tr)

            # Directional Movement
            up_move = high - prev_high
            down_move = prev_low - low

            plus_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
            minus_dm = down_move if (down_move > up_move and down_move > 0) else 0.0
            plus_dm_list.append(plus_dm)
            minus_dm_list.append(minus_dm)

        atr_sum = sum(tr_list)
        if atr_sum == 0:
            return 0.0

        plus_di = (sum(plus_dm_list) / atr_sum) * 100
        minus_di = (sum(minus_dm_list) / atr_sum) * 100

        di_sum = plus_di + minus_di
        if di_sum == 0:
            return 0.0

        dx = abs(plus_di - minus_di) / di_sum * 100
        return dx

    def _count_sma_crossovers(self, closes, period, lookback=50):
        """Count how many times price crossed the SMA in the recent `lookback` candles.

        Frequent crossovers indicate a ranging / mean-reverting market.
        """
        if len(closes) < period + lookback:
            lookback = len(closes) - period
        if lookback < 2:
            return 0

        crossovers = 0
        for i in range(len(closes) - lookback, len(closes)):
            if i < period:
                continue
            sma = sum(closes[i - period:i]) / period
            prev_sma = sum(closes[i - period - 1:i - 1]) / period if i > period else sma

            price = closes[i]
            prev_price = closes[i - 1]

            # A crossover occurs when price moves from one side of SMA to the other
            if (prev_price <= prev_sma and price > sma) or (prev_price >= prev_sma and price < sma):
                crossovers += 1

        return crossovers

    def detect(self, candles):
        """Analyze candles and return the current market regime.

        Returns:
            dict with keys:
                regime:         One of UPTREND, DOWNTREND, RANGING, VOLATILE
                confidence:     Float 0.0–1.0 indicating detection confidence
                recommendation: One of TRADE, HOLD, ACCUMULATE
                details:        Dict with supporting indicator values
        """
        if len(candles) < self.sma_period + 5:
            # Not enough data — default to cautious HOLD
            logger.warning("MarketRegimeDetector: not enough candles (%d), defaulting to HOLD", len(candles))
            return {
                "regime": self.REGIME_RANGING,
                "confidence": 0.3,
                "recommendation": "HOLD",
                "details": {"reason": "insufficient_data", "candle_count": len(candles)},
            }

        closes = [c["close"] for c in candles]
        current_price = closes[-1]

        # --- Indicators ---
        sma = self._compute_sma(closes, self.sma_period)
        sma_slope = self._compute_sma_slope(closes, self.sma_period, lookback=5)
        atr = self._compute_atr(candles, self.atr_period)
        atr_ratio = atr / current_price if current_price > 0 else 0.0
        adx = self._compute_adx_like_strength(candles, self.atr_period)
        crossovers = self._count_sma_crossovers(closes, self.sma_period, lookback=50)
        price_above_sma = current_price > sma if sma else False

        details = {
            "sma": round(sma, 4) if sma else None,
            "sma_slope": round(sma_slope, 6),
            "atr": round(atr, 4),
            "atr_ratio": round(atr_ratio, 6),
            "adx": round(adx, 2),
            "sma_crossovers": crossovers,
            "price_above_sma": price_above_sma,
            "current_price": round(current_price, 4),
        }

        # --- Classification logic ---

        # 1. Check VOLATILE first — high ATR overrides everything
        if atr_ratio >= self.atr_volatile_threshold:
            confidence = min(0.95, 0.60 + (atr_ratio - self.atr_volatile_threshold) * 10)
            return {
                "regime": self.REGIME_VOLATILE,
                "confidence": round(confidence, 3),
                "recommendation": "ACCUMULATE",
                "details": details,
            }

        # 2. Check for strong trend via ADX + SMA slope
        if adx >= self.adx_trend_threshold:
            if sma_slope > 0 and price_above_sma:
                # Strong uptrend
                confidence = min(0.95, 0.60 + adx / 200 + abs(sma_slope) * 50)
                return {
                    "regime": self.REGIME_UPTREND,
                    "confidence": round(confidence, 3),
                    "recommendation": "TRADE",
                    "details": details,
                }
            elif sma_slope < 0 and not price_above_sma:
                # Strong downtrend
                confidence = min(0.95, 0.60 + adx / 200 + abs(sma_slope) * 50)
                return {
                    "regime": self.REGIME_DOWNTREND,
                    "confidence": round(confidence, 3),
                    "recommendation": "HOLD",
                    "details": details,
                }

        # 3. Check for trend without strong ADX (weaker trend signals)
        if sma_slope > 0.0005 and price_above_sma:
            confidence = min(0.80, 0.50 + abs(sma_slope) * 100)
            return {
                "regime": self.REGIME_UPTREND,
                "confidence": round(confidence, 3),
                "recommendation": "TRADE",
                "details": details,
            }
        elif sma_slope < -0.0005 and not price_above_sma:
            confidence = min(0.80, 0.50 + abs(sma_slope) * 100)
            return {
                "regime": self.REGIME_DOWNTREND,
                "confidence": round(confidence, 3),
                "recommendation": "HOLD",
                "details": details,
            }

        # 4. Check RANGING — low ATR and/or frequent SMA crossovers
        if atr_ratio <= self.atr_ranging_threshold or crossovers >= self.crossover_threshold:
            confidence = min(0.85, 0.55 + crossovers * 0.03 + (self.atr_ranging_threshold - atr_ratio) * 20)
            confidence = max(0.40, confidence)
            return {
                "regime": self.REGIME_RANGING,
                "confidence": round(confidence, 3),
                "recommendation": "TRADE",
                "details": details,
            }

        # 5. Default — mild conditions, classify by SMA position
        if price_above_sma:
            return {
                "regime": self.REGIME_UPTREND,
                "confidence": 0.45,
                "recommendation": "TRADE",
                "details": details,
            }
        else:
            return {
                "regime": self.REGIME_RANGING,
                "confidence": 0.40,
                "recommendation": "TRADE",
                "details": details,
            }


# ============================================================
# MAIN — Run backtests on all strategies, promote winners
# ============================================================

def run_full_pipeline(pairs=None, granularity="5min"):
    """Run the full COLD pipeline: backtest all strategies on all pairs.

    Uses 5-minute candles by default for higher resolution backtesting.
    """
    if pairs is None:
        pairs = ["BTC-USD", "ETH-USD", "SOL-USD"]

    all_strategies = [
        MeanReversionStrategy(window=20, num_std=2.0),
        MomentumStrategy(short_window=5, long_window=20),
        RSIStrategy(period=14, oversold=30, overbought=70),
        VWAPStrategy(deviation_pct=0.005),
        DipBuyerStrategy(dip_threshold=0.015, recovery_target=0.02),
        MultiTimeframeStrategy(rsi_period=14, bb_window=20, bb_std=2.0),
        AccumulateAndHoldStrategy(dca_interval=12, dip_threshold=0.02),
    ]

    # Strategy selections per regime
    regime_strategies = {
        MarketRegimeDetector.REGIME_UPTREND: [
            "momentum", "rsi", "vwap", "multi_timeframe", "dip_buyer",
        ],
        MarketRegimeDetector.REGIME_DOWNTREND: [],  # skip entirely
        MarketRegimeDetector.REGIME_RANGING: [
            "mean_reversion", "rsi", "vwap", "multi_timeframe",
        ],
        MarketRegimeDetector.REGIME_VOLATILE: [
            "dip_buyer", "accumulate_hold",
        ],
    }

    prices = HistoricalPrices()
    backtester = Backtester(initial_capital=100.0)
    validator = StrategyValidator()
    regime_detector = MarketRegimeDetector()

    results_summary = []

    for pair in pairs:
        logger.info("Fetching historical data for %s...", pair)

        if granularity == "5min":
            candles = prices.get_5min_candles(pair, hours=48)  # 2 days of 5-min
        else:
            candles = prices.get_candles(pair, hours=168)  # 7 days hourly

        if len(candles) < 30:
            logger.warning("Not enough data for %s (%d candles)", pair, len(candles))
            continue

        logger.info("Got %d candles for %s", len(candles), pair)

        # --- Detect market regime FIRST ---
        regime_result = regime_detector.detect(candles)
        regime = regime_result["regime"]
        regime_conf = regime_result["confidence"]
        regime_rec = regime_result["recommendation"]

        print(f"\n{'='*60}")
        print(f"  MARKET REGIME: {pair}")
        print(f"{'='*60}")
        print(f"  Regime:         {regime}")
        print(f"  Confidence:     {regime_conf:.1%}")
        print(f"  Recommendation: {regime_rec}")
        details = regime_result.get("details", {})
        if details.get("sma") is not None:
            print(f"  SMA({regime_detector.sma_period}):       {details['sma']}")
            print(f"  SMA Slope:      {details['sma_slope']}")
            print(f"  ATR Ratio:      {details['atr_ratio']}")
            print(f"  ADX Strength:   {details['adx']}")
            print(f"  SMA Crossovers: {details['sma_crossovers']}")
            print(f"  Price > SMA:    {details['price_above_sma']}")

        # --- Skip DOWNTREND pairs ---
        if regime == MarketRegimeDetector.REGIME_DOWNTREND:
            logger.info(
                "SKIPPING %s — detected DOWNTREND regime (confidence=%.1f%%, recommendation=%s). "
                "Preserving capital per Rule #1: NEVER LOSE MONEY.",
                pair, regime_conf * 100, regime_rec,
            )
            print(f"  >> SKIPPING {pair}: DOWNTREND detected — preserving capital")
            continue

        # --- Filter strategies based on regime ---
        allowed_names = regime_strategies.get(regime, [s.name for s in all_strategies])
        strategies = [s for s in all_strategies if s.name in allowed_names]

        if not strategies:
            logger.info("No strategies suitable for %s regime on %s, skipping", regime, pair)
            print(f"  >> No strategies selected for {regime} regime on {pair}")
            continue

        logger.info(
            "Regime %s for %s — running %d strategies: %s",
            regime, pair, len(strategies), [s.name for s in strategies],
        )

        for strategy in strategies:
            logger.info("Backtesting %s on %s...", strategy.name, pair)
            validator.register_strategy(strategy, pair)

            results = backtester.run(strategy, candles, pair)

            # Print results
            print(f"\n{'='*60}")
            print(f"  {strategy.name} / {pair}")
            print(f"{'='*60}")
            print(f"  Return:     {results['total_return_pct']:+.2f}%")
            print(f"  Win Rate:   {results['win_rate']:.1%} ({results['wins']}W/{results['losses']}L)")
            print(f"  Trades:     {results['total_trades']} ({results['buy_trades']} buys, {results['sell_trades']} sells)")
            print(f"  Drawdown:   {results['max_drawdown_pct']:.2f}%")
            print(f"  Sharpe:     {results['sharpe_ratio']:.2f}")
            print(f"  Final:      ${results['final_capital']:.2f} (from ${results['initial_capital']:.2f})")

            # Submit for promotion
            passed, msg = validator.submit_backtest(strategy.name, pair, results)
            status = "PROMOTED to WARM" if passed else f"REJECTED: {msg}"
            print(f"  Status:     {status}")

            results_summary.append({
                "strategy": strategy.name,
                "pair": pair,
                "return": results["total_return_pct"],
                "win_rate": results["win_rate"],
                "trades": results["total_trades"],
                "drawdown": results["max_drawdown_pct"],
                "sharpe": results["sharpe_ratio"],
                "promoted": passed,
            })

    # Summary
    print(f"\n{'='*60}")
    print(f"  PIPELINE SUMMARY")
    print(f"{'='*60}")

    promoted = [r for r in results_summary if r["promoted"]]
    rejected = [r for r in results_summary if not r["promoted"]]

    print(f"\n  Promoted to WARM ({len(promoted)}):")
    for r in promoted:
        print(f"    {r['strategy']}/{r['pair']}: {r['return']:+.2f}% return, {r['win_rate']:.1%} WR, {r['sharpe']:.2f} Sharpe")

    print(f"\n  Rejected ({len(rejected)}):")
    for r in rejected:
        print(f"    {r['strategy']}/{r['pair']}: {r['return']:+.2f}% return, {r['win_rate']:.1%} WR")

    # Show all registered strategies
    all_strats = validator.get_all_strategies()
    hot = [s for s in all_strats if s["stage"] == "HOT"]
    if hot:
        print(f"\n  HOT (Live Trading) ({len(hot)}):")
        for s in hot:
            print(f"    {s['name']}/{s['pair']}")

    return results_summary


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        validator = StrategyValidator()
        strats = validator.get_all_strategies()
        print(f"\n{'='*60}")
        print(f"  STRATEGY PIPELINE STATUS")
        print(f"{'='*60}")
        for stage in ["HOT", "WARM", "COLD", "KILLED"]:
            group = [s for s in strats if s["stage"] == stage]
            if group:
                print(f"\n  {stage} ({len(group)}):")
                for s in group:
                    print(f"    {s['name']}/{s['pair']} (since {s['promoted_at'] or s['created_at']})")
        if not strats:
            print("\n  No strategies registered. Run 'python strategy_pipeline.py' to backtest.")
        print()

    elif len(sys.argv) > 1 and sys.argv[1] == "backtest":
        # Backtest specific pair
        pair = sys.argv[2] if len(sys.argv) > 2 else "BTC-USD"
        run_full_pipeline(pairs=[pair])

    else:
        # Run full pipeline
        run_full_pipeline()
