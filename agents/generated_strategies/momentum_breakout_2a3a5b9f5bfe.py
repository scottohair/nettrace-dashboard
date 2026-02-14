#!/usr/bin/env python3
"""
Auto-generated momentum_breakout strategy
"""

import numpy as np
from datetime import datetime


class Strategy:
    def __init__(self, pair, params=None):
        self.pair = pair
        self.params = params or {}

        # Default parameters for momentum_breakout
        self.threshold = self.params.get('threshold', 0.02)
        self.window = self.params.get('window', 20)

    def generate_signal(self, candle_data):
        """Generate trading signal from candle data"""

        if len(candle_data) < self.window:
            return {'signal': 'HOLD', 'confidence': 0.0, 'reason': 'insufficient_data'}

        # Extract price data
        prices = np.array([c['close'] for c in candle_data[-self.window:]])
        volumes = np.array([c['volume'] for c in candle_data[-self.window:]])

        # momentum_breakout logic
        current_price = prices[-1]
        avg_price = np.mean(prices)
        price_change_pct = (current_price - avg_price) / avg_price

        if price_change_pct > self.threshold:
            signal = 'BUY'
            confidence = min(0.95, 0.6 + abs(price_change_pct) * 5)
            reason = f'momentum_breakout_bullish_breakout'
        elif price_change_pct < -self.threshold:
            signal = 'SELL'
            confidence = min(0.95, 0.6 + abs(price_change_pct) * 5)
            reason = f'momentum_breakout_bearish_breakdown'
        else:
            signal = 'HOLD'
            confidence = 0.5
            reason = f'momentum_breakout_neutral'

        return {
            'signal': signal,
            'confidence': confidence,
            'reason': reason,
            'price_change_pct': price_change_pct
        }

    def get_params(self):
        """Return strategy parameters"""
        return {
            'threshold': self.threshold,
            'window': self.window,
            'template': 'momentum_breakout'
        }
