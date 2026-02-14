#!/usr/bin/env python3
"""Market Regime Detector for NetTrace.

Classifies market regime (BULL, BEAR, SIDEWAYS) using:
  - 7-day momentum (20-day MA slope)
  - Volatility (rolling standard deviation)
  - Volume profile (trend strength)

Regime-specific parameter tuning:
  - BULL: Tighter stops, higher TP targets, faster agent promotion
  - BEAR: Wider stops, lower TP targets, stricter agent thresholds
  - SIDEWAYS: Mean reversion, balanced parameters

Usage:
  python regime_detector.py --mode detect
  python regime_detector.py --mode history
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from enum import Enum
import time

logger = logging.getLogger("regime_detector")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

BASE = Path(__file__).parent
REGIME_DB = BASE / "regime_detector.db"
REGIME_LOG = BASE / "regime_history.jsonl"


class MarketRegime(Enum):
    """Market regime classifications."""
    BULL = "BULL"
    BEAR = "BEAR"
    SIDEWAYS = "SIDEWAYS"


class RegimeDetector:
    """Detects market regime from price and volume data."""

    # Regime detection thresholds
    BULL_THRESHOLD = 0.01  # 1% / day momentum
    BEAR_THRESHOLD = -0.01  # -1% / day momentum
    VOLATILITY_HIGH = 0.025  # 2.5% volatility = high
    VOLATILITY_LOW = 0.008  # 0.8% volatility = low

    def __init__(self):
        self._init_db()

    def _init_db(self) -> None:
        """Initialize regime detection database."""
        os.makedirs(BASE, exist_ok=True)
        self.db = sqlite3.connect(str(REGIME_DB))
        self.db.row_factory = sqlite3.Row

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS regime_detections (
                id INTEGER PRIMARY KEY,
                timestamp TEXT NOT NULL,
                regime TEXT NOT NULL,
                momentum_7d REAL,
                volatility REAL,
                volume_strength REAL,
                btc_price REAL,
                eth_price REAL,
                confidence REAL,
                source TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS regime_parameters (
                id INTEGER PRIMARY KEY,
                regime TEXT NOT NULL UNIQUE,
                fire_max_sharpe REAL,
                promote_min_sharpe REAL,
                clone_min_sharpe REAL,
                exit_trailing_stop REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.db.commit()

        # Seed regime parameters if empty
        self._seed_regime_parameters()

    def _seed_regime_parameters(self) -> None:
        """Initialize regime-specific parameters."""
        cursor = self.db.execute("SELECT COUNT(*) as count FROM regime_parameters")
        if cursor.fetchone()["count"] == 0:
            self.db.execute("""
                INSERT INTO regime_parameters (regime, fire_max_sharpe, promote_min_sharpe, clone_min_sharpe, exit_trailing_stop)
                VALUES (?, ?, ?, ?, ?)
            """, ("BULL", 0.9, 1.2, 1.8, 0.015))

            self.db.execute("""
                INSERT INTO regime_parameters (regime, fire_max_sharpe, promote_min_sharpe, clone_min_sharpe, exit_trailing_stop)
                VALUES (?, ?, ?, ?, ?)
            """, ("BEAR", 1.2, 1.5, 2.2, 0.025))

            self.db.execute("""
                INSERT INTO regime_parameters (regime, fire_max_sharpe, promote_min_sharpe, clone_min_sharpe, exit_trailing_stop)
                VALUES (?, ?, ?, ?, ?)
            """, ("SIDEWAYS", 1.0, 1.3, 2.0, 0.020))

            self.db.commit()

    def _get_price_data(self, lookback_days: int = 30) -> Dict:
        """Get price data for regime detection."""
        try:
            # Try to get from exchange_connector
            from exchange_connector import ExchangeConnector
            connector = ExchangeConnector()

            # Get BTC/USD and ETH/USD candles
            btc_candles = connector.get_historical_candles("BTC-USD", lookback_days * 24)
            eth_candles = connector.get_historical_candles("ETH-USD", lookback_days * 24)

            return {
                "btc_candles": btc_candles,
                "eth_candles": eth_candles,
                "source": "exchange_connector"
            }
        except Exception as e:
            logger.warning(f"Failed to get price data: {e}")
            return self._get_mock_price_data()

    def _get_mock_price_data(self) -> Dict:
        """Return mock price data for testing."""
        import random

        # Generate mock candles for last 30 days
        now = datetime.now(timezone.utc)
        candles = []

        price = 42000  # Starting BTC price
        for i in range(30):
            timestamp = now - timedelta(days=30 - i)
            change = random.uniform(-0.02, 0.02)  # ±2%
            price = price * (1 + change)

            candles.append({
                "timestamp": timestamp.isoformat(),
                "close": price,
                "volume": random.uniform(100, 1000),
            })

        return {
            "btc_candles": candles,
            "eth_candles": candles,
            "source": "mock"
        }

    def _calculate_momentum(self, candles: List[Dict], period: int = 7) -> float:
        """Calculate momentum (% change per day over period)."""
        if len(candles) < period:
            return 0.0

        recent = candles[-period]
        oldest = candles[0]

        if oldest["close"] == 0:
            return 0.0

        total_change = (recent["close"] - oldest["close"]) / oldest["close"]
        return total_change / period  # Per-day momentum

    def _calculate_volatility(self, candles: List[Dict], period: int = 20) -> float:
        """Calculate volatility (rolling standard deviation of returns)."""
        if len(candles) < period:
            return 0.0

        returns = []
        for i in range(1, min(period, len(candles))):
            prev_close = candles[i - 1]["close"]
            curr_close = candles[i]["close"]
            if prev_close > 0:
                ret = (curr_close - prev_close) / prev_close
                returns.append(ret)

        if not returns:
            return 0.0

        # Calculate standard deviation
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
        return variance ** 0.5

    def _calculate_volume_strength(self, candles: List[Dict]) -> float:
        """Calculate volume strength (trend vs noise)."""
        if len(candles) < 2:
            return 0.5  # Neutral

        # Simple: volume-weighted momentum
        total_volume = sum(c.get("volume", 1) for c in candles)
        if total_volume == 0:
            return 0.5

        weighted_momentum = 0
        for i in range(1, len(candles)):
            price_change = candles[i]["close"] / candles[i - 1]["close"] - 1
            volume = candles[i].get("volume", 1)
            weighted_momentum += price_change * (volume / total_volume)

        # Normalize to 0-1 range
        return max(0, min(1, 0.5 + weighted_momentum * 10))

    def detect_regime(self) -> str:
        """Detect current market regime."""
        try:
            price_data = self._get_price_data()
            candles = price_data.get("btc_candles", [])

            if not candles:
                logger.warning("No price data available, returning SIDEWAYS")
                return MarketRegime.SIDEWAYS.value

            # Calculate indicators
            momentum = self._calculate_momentum(candles, period=7)
            volatility = self._calculate_volatility(candles, period=20)
            volume_strength = self._calculate_volume_strength(candles)

            # Detect regime
            if momentum > self.BULL_THRESHOLD:
                regime = MarketRegime.BULL
                confidence = min(abs(momentum) / 0.03, 1.0)  # Up to 3% momentum = 100% confidence
            elif momentum < self.BEAR_THRESHOLD:
                regime = MarketRegime.BEAR
                confidence = min(abs(momentum) / 0.03, 1.0)
            else:
                regime = MarketRegime.SIDEWAYS
                confidence = 1.0 - abs(momentum) / self.BULL_THRESHOLD

            # Log detection
            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "regime": regime.value,
                "momentum_7d": momentum,
                "volatility": volatility,
                "volume_strength": volume_strength,
                "confidence": confidence,
                "btc_price": candles[-1]["close"] if candles else 0,
                "source": price_data.get("source", "unknown"),
            }

            with open(REGIME_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")

            # Save to DB
            self.db.execute("""
                INSERT INTO regime_detections
                (timestamp, regime, momentum_7d, volatility, volume_strength, btc_price, confidence, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record["timestamp"],
                record["regime"],
                record["momentum_7d"],
                record["volatility"],
                record["volume_strength"],
                record["btc_price"],
                record["confidence"],
                record["source"],
            ))
            self.db.commit()

            logger.info(f"Detected regime: {regime.value} (confidence: {confidence:.1%}, "
                       f"momentum: {momentum:.4f}, volatility: {volatility:.4f})")

            return regime.value

        except Exception as e:
            logger.error(f"Regime detection failed: {e}")
            return MarketRegime.SIDEWAYS.value

    def get_regime_parameters(self, regime: str) -> Dict:
        """Get parameters for a specific regime."""
        try:
            cursor = self.db.execute(
                "SELECT * FROM regime_parameters WHERE regime = ?",
                (regime.upper(),)
            )
            row = cursor.fetchone()

            if row:
                return {
                    "regime": row["regime"],
                    "fire_max_sharpe": row["fire_max_sharpe"],
                    "promote_min_sharpe": row["promote_min_sharpe"],
                    "clone_min_sharpe": row["clone_min_sharpe"],
                    "exit_trailing_stop": row["exit_trailing_stop"],
                }
            else:
                logger.warning(f"No parameters for regime {regime}")
                return {}
        except Exception as e:
            logger.error(f"Failed to get regime parameters: {e}")
            return {}

    def get_history(self, limit: int = 30) -> List[Dict]:
        """Get regime detection history."""
        try:
            cursor = self.db.execute("""
                SELECT * FROM regime_detections
                ORDER BY timestamp DESC LIMIT ?
            """, (limit,))

            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get history: {e}")
            return []

    def get_current_regime(self) -> str:
        """Get most recent regime detection."""
        try:
            cursor = self.db.execute("""
                SELECT regime FROM regime_detections
                ORDER BY timestamp DESC LIMIT 1
            """)
            row = cursor.fetchone()
            return row["regime"] if row else MarketRegime.SIDEWAYS.value
        except:
            return MarketRegime.SIDEWAYS.value


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NetTrace Regime Detector")
    parser.add_argument("--mode", choices=["detect", "history", "parameters"],
                        default="detect", help="Operation mode")
    parser.add_argument("--regime", help="Regime for parameters (BULL/BEAR/SIDEWAYS)")
    parser.add_argument("--limit", type=int, default=30, help="History limit")

    args = parser.parse_args()

    detector = RegimeDetector()

    if args.mode == "detect":
        regime = detector.detect_regime()
        params = detector.get_regime_parameters(regime)
        print(json.dumps({"regime": regime, "parameters": params}, indent=2))

    elif args.mode == "history":
        history = detector.get_history(args.limit)
        print(json.dumps({"history": history}, indent=2, default=str))

    elif args.mode == "parameters":
        if not args.regime:
            print("❌ --regime required")
            return 1
        params = detector.get_regime_parameters(args.regime)
        print(json.dumps(params, indent=2))

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
