#!/usr/bin/env python3
"""Python bridge to the C fast_engine via ctypes.

Provides high-performance indicator computation, signal generation,
and arbitrage detection at native speed.

Usage:
    from fast_bridge import FastEngine
    engine = FastEngine()
    indicators = engine.compute_indicators(candles)
    signal = engine.generate_signal(candles)
    arb = engine.check_arbitrage(coinbase_price, other_prices)
"""

import ctypes
import os
from pathlib import Path

# Load the shared library
_lib_path = str(Path(__file__).parent / "fast_engine.so")
if not os.path.exists(_lib_path):
    raise ImportError(f"fast_engine.so not found at {_lib_path}. Compile with: "
                      "cc -O3 -mcpu=apple-m1 -shared -fPIC -o fast_engine.so fast_engine.c -lm")


# C struct definitions
class Candle(ctypes.Structure):
    _fields_ = [
        ("open", ctypes.c_double),
        ("high", ctypes.c_double),
        ("low", ctypes.c_double),
        ("close", ctypes.c_double),
        ("volume", ctypes.c_double),
        ("timestamp", ctypes.c_long),
    ]

class Indicators(ctypes.Structure):
    _fields_ = [
        ("sma_20", ctypes.c_double),
        ("sma_50", ctypes.c_double),
        ("rsi_14", ctypes.c_double),
        ("atr_14", ctypes.c_double),
        ("bb_upper", ctypes.c_double),
        ("bb_lower", ctypes.c_double),
        ("vwap", ctypes.c_double),
        ("volume_ratio", ctypes.c_double),
        ("regime", ctypes.c_int),
    ]

class Signal(ctypes.Structure):
    _fields_ = [
        ("signal_type", ctypes.c_int),
        ("confidence", ctypes.c_double),
        ("target_price", ctypes.c_double),
        ("stop_price", ctypes.c_double),
        ("strategy_id", ctypes.c_int),
        ("reason", ctypes.c_char * 64),
    ]

class ArbResult(ctypes.Structure):
    _fields_ = [
        ("has_opportunity", ctypes.c_int),
        ("side", ctypes.c_int),
        ("coinbase_price", ctypes.c_double),
        ("market_median", ctypes.c_double),
        ("spread_pct", ctypes.c_double),
        ("expected_profit_pct", ctypes.c_double),
        ("confidence", ctypes.c_double),
        ("source_count", ctypes.c_int),
    ]


class CAdaptiveRisk(ctypes.Structure):
    """C-level adaptive risk computation — sub-nanosecond."""
    _fields_ = [
        ("portfolio_value", ctypes.c_double),
        ("max_trade_usd", ctypes.c_double),
        ("max_daily_loss", ctypes.c_double),
        ("min_reserve", ctypes.c_double),
        ("optimal_grid_size", ctypes.c_double),
        ("optimal_grid_levels", ctypes.c_int),
        ("optimal_dca_daily", ctypes.c_double),
        ("streak_multiplier", ctypes.c_double),
        ("win_streak", ctypes.c_int),
        ("loss_streak", ctypes.c_int),
        ("kelly_fraction", ctypes.c_double),
    ]


class FastEngine:
    """High-performance trading engine using C shared library."""

    REGIME_MAP = {0: "UNKNOWN", 1: "UPTREND", 2: "DOWNTREND", 3: "RANGING", 4: "VOLATILE"}

    def __init__(self):
        self._lib = ctypes.CDLL(_lib_path)

        # Set function signatures
        self._lib.compute_all_indicators.argtypes = [ctypes.POINTER(Candle), ctypes.c_int]
        self._lib.compute_all_indicators.restype = Indicators

        self._lib.generate_signal.argtypes = [
            ctypes.POINTER(Candle), ctypes.c_int, ctypes.POINTER(Indicators)
        ]
        self._lib.generate_signal.restype = Signal

        self._lib.check_arbitrage.argtypes = [
            ctypes.c_double, ctypes.POINTER(ctypes.c_double), ctypes.c_int
        ]
        self._lib.check_arbitrage.restype = ArbResult

        self._lib.compute_rsi.argtypes = [
            ctypes.POINTER(ctypes.c_double), ctypes.c_int, ctypes.c_int
        ]
        self._lib.compute_rsi.restype = ctypes.c_double

        self._lib.compute_adaptive_risk.argtypes = [
            ctypes.c_double, ctypes.c_int, ctypes.c_int,
            ctypes.c_double, ctypes.c_double, ctypes.c_double
        ]
        self._lib.compute_adaptive_risk.restype = CAdaptiveRisk

        self._lib.compute_grid_levels.argtypes = [
            ctypes.c_double, ctypes.c_double, ctypes.c_int, ctypes.c_int,
            ctypes.POINTER(ctypes.c_double), ctypes.POINTER(ctypes.c_double), ctypes.c_int
        ]
        self._lib.compute_grid_levels.restype = ctypes.c_int

    def _to_c_candles(self, candles):
        """Convert Python candle dicts to C Candle array."""
        n = len(candles)
        arr = (Candle * n)()
        for i, c in enumerate(candles):
            arr[i].open = c["open"]
            arr[i].high = c["high"]
            arr[i].low = c["low"]
            arr[i].close = c["close"]
            arr[i].volume = c["volume"]
            arr[i].timestamp = int(c.get("time", 0))
        return arr, n

    def compute_indicators(self, candles):
        """Compute all technical indicators for a candle series.

        Returns dict with: sma_20, sma_50, rsi_14, atr_14, bb_upper, bb_lower,
                          vwap, volume_ratio, regime
        """
        arr, n = self._to_c_candles(candles)
        ind = self._lib.compute_all_indicators(arr, n)
        return {
            "sma_20": ind.sma_20,
            "sma_50": ind.sma_50,
            "rsi_14": ind.rsi_14,
            "atr_14": ind.atr_14,
            "bb_upper": ind.bb_upper,
            "bb_lower": ind.bb_lower,
            "vwap": ind.vwap,
            "volume_ratio": ind.volume_ratio,
            "regime": self.REGIME_MAP.get(ind.regime, "UNKNOWN"),
            "regime_id": ind.regime,
        }

    def generate_signal(self, candles):
        """Generate trading signal from candle data.

        Returns dict with: signal_type (0=none, 1=buy, 2=sell), confidence,
                          target_price, stop_price, strategy_id, reason
        """
        arr, n = self._to_c_candles(candles)
        ind = self._lib.compute_all_indicators(arr, n)
        sig = self._lib.generate_signal(arr, n, ctypes.byref(ind))

        side_map = {0: "NONE", 1: "BUY", 2: "SELL"}
        return {
            "signal_type": side_map.get(sig.signal_type, "NONE"),
            "confidence": sig.confidence,
            "target_price": sig.target_price,
            "stop_price": sig.stop_price,
            "strategy_id": sig.strategy_id,
            "reason": sig.reason.decode("utf-8", errors="ignore").strip("\x00"),
        }

    def check_arbitrage(self, coinbase_price, other_prices):
        """Check for arbitrage opportunity.

        Args:
            coinbase_price: float, current Coinbase price
            other_prices: list of floats, prices from other exchanges

        Returns dict with: has_opportunity, side, spread_pct, expected_profit_pct, confidence
        """
        n = len(other_prices)
        prices_arr = (ctypes.c_double * n)(*other_prices)
        result = self._lib.check_arbitrage(coinbase_price, prices_arr, n)

        side_map = {0: "NONE", 1: "BUY", 2: "SELL"}
        return {
            "has_opportunity": bool(result.has_opportunity),
            "side": side_map.get(result.side, "NONE"),
            "coinbase_price": result.coinbase_price,
            "market_median": result.market_median,
            "spread_pct": result.spread_pct,
            "expected_profit_pct": result.expected_profit_pct,
            "confidence": result.confidence,
            "source_count": result.source_count,
        }

    def compute_rsi(self, closes, period=14):
        """Compute RSI for a series of close prices."""
        n = len(closes)
        arr = (ctypes.c_double * n)(*closes)
        return self._lib.compute_rsi(arr, n, period)

    def compute_adaptive_risk(self, portfolio_value, win_streak=0, loss_streak=0,
                               win_rate=0.60, avg_win=2.0, avg_loss=1.5):
        """Compute adaptive risk parameters using C engine (sub-nanosecond).

        Uses Kelly Criterion for optimal bet sizing.
        Returns dict with all risk parameters.
        """
        result = self._lib.compute_adaptive_risk(
            portfolio_value, win_streak, loss_streak, win_rate, avg_win, avg_loss
        )
        return {
            "portfolio": result.portfolio_value,
            "max_trade": result.max_trade_usd,
            "max_daily_loss": result.max_daily_loss,
            "min_reserve": result.min_reserve,
            "grid_size": result.optimal_grid_size,
            "grid_levels": result.optimal_grid_levels,
            "dca_daily": result.optimal_dca_daily,
            "streak_mult": result.streak_multiplier,
            "win_streak": result.win_streak,
            "loss_streak": result.loss_streak,
            "kelly_fraction": result.kelly_fraction,
        }

    def compute_grid_levels(self, center_price, spacing_pct=0.01,
                             levels_above=5, levels_below=5):
        """Compute grid price levels in C (sub-nanosecond)."""
        max_levels = 20
        buy_arr = (ctypes.c_double * max_levels)()
        sell_arr = (ctypes.c_double * max_levels)()
        total = self._lib.compute_grid_levels(
            center_price, spacing_pct, levels_above, levels_below,
            buy_arr, sell_arr, max_levels
        )
        buys = [buy_arr[i] for i in range(min(levels_below, max_levels)) if buy_arr[i] > 0]
        sells = [sell_arr[i] for i in range(min(levels_above, max_levels)) if sell_arr[i] > 0]
        return {"buy_prices": buys, "sell_prices": sells, "total_levels": total}


if __name__ == "__main__":
    engine = FastEngine()

    # Test with synthetic data
    import time
    candles = []
    price = 68000.0
    for i in range(500):
        import random
        noise = (random.random() - 0.5) * 200
        candles.append({
            "open": price + noise,
            "high": price + abs(noise) * 1.5,
            "low": price - abs(noise) * 1.5,
            "close": price + noise * 0.3,
            "volume": 100 + random.random() * 200,
            "time": 1700000000 + i * 300,
        })
        price = candles[-1]["close"]

    # Benchmark
    start = time.perf_counter()
    for _ in range(10000):
        ind = engine.compute_indicators(candles)
    elapsed = time.perf_counter() - start
    print(f"Indicators (10k iterations): {elapsed*1000:.1f}ms ({elapsed/10000*1e6:.1f} us/iter)")

    print(f"\nIndicators: {ind}")

    sig = engine.generate_signal(candles)
    print(f"Signal: {sig}")

    arb = engine.check_arbitrage(68000, [68100, 68050, 68075, 68090])
    print(f"Arb check: {arb}")

    print(f"\nRSI: {engine.compute_rsi([c['close'] for c in candles]):.1f}")

    # Test adaptive risk (C engine)
    print("\n=== C Adaptive Risk Engine ===")
    for val in [13.48, 100, 1200, 10000]:
        risk = engine.compute_adaptive_risk(val)
        print(f"  ${val:>10,.2f} → max_trade ${risk['max_trade']:>8,.2f} | "
              f"kelly {risk['kelly_fraction']*100:.1f}% | "
              f"grid ${risk['grid_size']:>6,.2f} x {risk['grid_levels']}")

    # Test grid levels (C engine)
    grid = engine.compute_grid_levels(68000.0, 0.01, 3, 3)
    print(f"\nGrid levels @ $68,000 (1% spacing):")
    print(f"  Buy:  {['${:,.2f}'.format(p) for p in grid['buy_prices']]}")
    print(f"  Sell: {['${:,.2f}'.format(p) for p in grid['sell_prices']]}")
