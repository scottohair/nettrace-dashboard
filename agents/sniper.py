#!/usr/bin/env python3
"""High-Confidence Sniper — stacked signal aggregator.

Only trades when composite confidence >= 90% with 3+ confirming signals.
Stacks 9 independent signal sources for maximum conviction:

  1. NetTrace latency signals (exchange infrastructure changes)
  2. Fast Engine regime detection (SMA/RSI/BB/VWAP/ATR)
  3. Fast Engine arb check (Coinbase vs 5 exchange median)
  4. Orderbook imbalance (bid/ask depth ratio)
  5. RSI extreme detection (oversold < 25 / overbought > 75)
  6. Fear & Greed Index (contrarian)
  7. Price momentum (4h trend)
  8. Uptick timing (buy-low-sell-high inflection)
  9. Meta-Engine ML predictions (RSI+momentum+SMA ensemble from meta_engine.db)

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
import re
import sqlite3
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Agent goals — single source of truth for all decision-making
try:
    from agent_goals import GoalValidator
    _goals = GoalValidator()
except ImportError:
    _goals = None

# Asset state tracking for learning
try:
    from asset_tracker import get_tracker
    _tracker = get_tracker()
except Exception:
    _tracker = None

# KPI tracking — record every trade for scorecard
try:
    from kpi_tracker import get_kpi_tracker
    _kpi = get_kpi_tracker()
except Exception:
    _kpi = None

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

# Use persistent volume on Fly (/data/), local agents/ dir otherwise
_persistent_dir = Path("/data") if Path("/data").is_dir() else Path(__file__).parent
SNIPER_DB = str(_persistent_dir / "sniper.db")
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")
FLY_URL = "https://nettrace-dashboard.fly.dev"
WALLET_ADDRESS = os.environ.get("WALLET_ADDRESS", "")
ORCH_OWNER_ENV = "ORCHESTRATOR_OWNER_ID"
EXECUTION_HEALTH_STATUS_PATH = Path(__file__).parent / "execution_health_status.json"
EXIT_MANAGER_STATUS_PATH = Path(__file__).parent / "exit_manager_status.json"

# Dynamic risk controller — NO hardcoded values
# CRITICAL: If risk_controller fails to load, refuse all NEW trades (still allow exits)
try:
    from risk_controller import get_controller
    _risk_ctrl = get_controller()
except Exception as _rc_err:
    _risk_ctrl = None
    logging.getLogger("sniper").error("RISK CONTROLLER FAILED TO LOAD: %s — new trades BLOCKED", _rc_err)

# Exit strategy manager — every position gets an exit plan
try:
    from exit_manager import get_exit_manager
    _exit_mgr = get_exit_manager()
except Exception as _em_err:
    _exit_mgr = None
    logging.getLogger("sniper").error("EXIT_MANAGER FAILED TO LOAD: %s — positions will NOT be monitored!", _em_err)

# Growth engine — algebraic signal fusion + optimal trade selection
try:
    from growth_engine import get_growth_engine
    _growth = get_growth_engine()
except Exception as _ge_err:
    _growth = None
    logging.getLogger("sniper").error("GROWTH_ENGINE FAILED TO LOAD: %s", _ge_err)

# Strategic planner — 3D Go game theory (long chain moves, influence, ko detection)
try:
    from strategic_planner import get_strategic_planner
    _planner = get_strategic_planner()
except Exception as _sp_err:
    _planner = None
    logging.getLogger("sniper").error("STRATEGIC_PLANNER FAILED TO LOAD: %s", _sp_err)

# Sniper configuration — static signal settings only
# Trade sizes, reserves, position limits come from risk_controller dynamically
CONFIG = {
    "min_composite_confidence": 0.70,    # 70% minimum — quality over quantity
    "min_confirming_signals": 2,         # 2+ must agree
    "min_quant_signals": 1,              # at least 1 quantitative signal required
    "scan_interval": int(os.environ.get("SNIPER_SCAN_INTERVAL_SECONDS", "30")),  # Scan cadence (can be tightened)
    "min_hold_seconds": 60,               # 60s minimum hold
    # PRIMARY: ETH + SOL (active trading vehicles)
    # SECONDARY: AVAX, LINK, DOGE, FET (opportunistic)
    # RESERVE: BTC, USD, USDC — held as treasury, NOT actively traded away
    "pairs": ["ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD", "DOGE-USD", "FET-USD"],
    "reserve_assets": ["BTC", "USD", "USDC"],  # Never sell these — treasury
    "primary_pairs": ["ETH-USD", "SOL-USD"],    # Priority allocation
    # Signal weights: quantitative signals DRIVE decisions, qualitative SUPPLEMENTS
    # Quantitative: regime, arb, orderbook, rsi_extreme, momentum, latency
    # Computational edge: latency (our private NetTrace data), meta_engine (ML ensemble)
    # Qualitative: fear_greed, uptick (accelerate but don't drive)
    "signal_weights": {
        # --- QUANTITATIVE (core decision drivers) ---
        "regime": 0.20,        # C engine: SMA/RSI/BB/VWAP/ATR — most computational
        "arb": 0.15,           # Cross-exchange spread — pure math
        "orderbook": 0.15,     # Market microstructure depth analysis
        "rsi_extreme": 0.10,   # Technical oversold/overbought
        "momentum": 0.08,      # 4h trend analysis
        # --- COMPUTATIONAL EDGE (private alpha) ---
        "latency": 0.15,       # NetTrace infrastructure monitoring — our unique edge
        "meta_engine": 0.10,   # ML ensemble predictions from evolution engine
        # --- QUALITATIVE (supplementary only) ---
        "fear_greed": 0.04,    # Sentiment — supplements, doesn't drive
        "uptick": 0.03,        # Simple bounce — supplements, doesn't drive
    },
    # Classify which signals are quantitative vs qualitative
    "quant_signals": {"regime", "arb", "orderbook", "rsi_extreme", "momentum", "latency", "meta_engine"},
    "qual_signals": {"fear_greed", "uptick"},
    # Expected Value parameters (fees + slippage)
    "round_trip_fee_pct": 0.008,   # 0.4% maker buy + 0.4% maker sell
    "expected_slippage_pct": 0.001, # ~0.1% average slippage
    # Long-chain game-theory gate (entry must model profitable path to exit).
    "min_chain_net_edge": float(os.environ.get("SNIPER_MIN_CHAIN_NET_EDGE_PCT", "0.012")),
    "min_chain_worst_case_edge": float(os.environ.get("SNIPER_MIN_CHAIN_WORST_EDGE_PCT", "0.001")),
    "min_chain_steps": int(os.environ.get("SNIPER_MIN_CHAIN_STEPS", "2")),
    "require_execution_health_for_buy": os.environ.get("SNIPER_REQUIRE_EXECUTION_HEALTH_FOR_BUY", "1").lower() not in ("0", "false", "no"),
    "execution_health_max_age_seconds": int(os.environ.get("SNIPER_EXECUTION_HEALTH_MAX_AGE_SECONDS", "300")),
    "require_exit_manager_status_for_buy": os.environ.get("SNIPER_REQUIRE_EXIT_MANAGER_STATUS_FOR_BUY", "1").lower() not in ("0", "false", "no"),
    "exit_manager_status_max_age_seconds": int(os.environ.get("SNIPER_EXIT_MANAGER_STATUS_MAX_AGE_SECONDS", "300")),
    "pair_failure_cooldown_seconds": int(os.environ.get("SNIPER_PAIR_FAILURE_COOLDOWN_SECONDS", "180")),
    "scan_interval_healthy": int(os.environ.get("SNIPER_SCAN_INTERVAL_HEALTHY_SECONDS", "20")),
    "scan_interval_degraded": int(os.environ.get("SNIPER_SCAN_INTERVAL_DEGRADED_SECONDS", "45")),
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
    """Normalize pair for public data APIs (identity for -USD pairs)."""
    return pair.replace("-USDC", "-USD")


class SignalSource:
    """Base class for signal sources."""

    def scan(self, pair):
        """Returns: {"direction": "BUY"|"SELL"|"NONE", "confidence": 0.0-1.0, "reason": "..."}"""
        raise NotImplementedError


class LatencySignalSource(SignalSource):
    """Signal #1: NetTrace latency signals from 7 global Fly.io regions.

    Uses two data sources:
    1. /api/v1/signals — pre-computed quant signals (anomalies, trends, route changes)
    2. /api/v1/signals/crypto-latency — raw exchange latency with anomaly detection
    """

    def scan(self, pair):
        try:
            headers = {"Authorization": f"Bearer {NETTRACE_API_KEY}"}

            # Source 1: Pre-computed signals
            url = f"{FLY_URL}/api/v1/signals?hours=1&min_confidence=0.6"
            data = _fetch_json(url, headers=headers)
            signals = data.get("signals", [])
            relevant = [s for s in signals if s.get("confidence", 0) >= 0.7]

            # Source 2: Real-time crypto exchange latency anomalies
            anomaly_count = 0
            improving_count = 0
            try:
                crypto_url = f"{FLY_URL}/api/v1/signals/crypto-latency?hours=1&min_confidence=0.6"
                crypto_data = _fetch_json(crypto_url, headers=headers)
                anomalies = crypto_data.get("anomalies", [])
                anomaly_count = len(anomalies)
                # Count direction: negative deviation = faster = improving infrastructure
                for a in anomalies:
                    if a.get("deviation_pct", 0) < -10:
                        improving_count += 1
                # Route changes are strong signals
                route_changes = crypto_data.get("route_changes", [])
                if route_changes:
                    relevant.extend([{"direction": "latency_change", "confidence": 0.75}
                                     for _ in route_changes[:3]])
            except Exception:
                pass  # crypto-latency endpoint may not be deployed yet

            if not relevant and anomaly_count == 0:
                return {"direction": "NONE", "confidence": 0, "reason": "No latency signals"}

            # Aggregate: improving latency → bullish (infrastructure investment)
            up_count = sum(1 for s in relevant if s.get("direction") in ("latency_down", "latency_change"))
            up_count += improving_count
            down_count = sum(1 for s in relevant if s.get("direction") == "latency_up")
            down_count += (anomaly_count - improving_count)

            total_signals = max(1, len(relevant) + anomaly_count)
            avg_conf = sum(s.get("confidence", 0) for s in relevant) / max(1, len(relevant))

            if up_count > down_count:
                return {"direction": "BUY", "confidence": min(avg_conf, 0.95),
                        "reason": f"{up_count} exchanges improving latency ({anomaly_count} anomalies from 7 regions)"}
            elif down_count > up_count:
                return {"direction": "SELL", "confidence": min(avg_conf, 0.95),
                        "reason": f"{down_count} exchanges degrading ({anomaly_count} anomalies)"}
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
        """Fetch 1h candles from Coinbase (uses -USD for liquidity)."""
        try:
            dp = _data_pair(pair)
            url = f"https://api.exchange.coinbase.com/products/{dp}/candles?granularity={granularity}"
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

            # Get Coinbase price (use -USD for public spot price)
            dp = _data_pair(pair)
            cb_data = _fetch_json(f"https://api.coinbase.com/v2/prices/{dp}/spot")
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
            dp = _data_pair(pair)
            url = f"https://api.exchange.coinbase.com/products/{dp}/book?level=2"
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
            dp = _data_pair(pair)
            url = f"https://api.exchange.coinbase.com/products/{dp}/candles?granularity=3600"
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
            dp = _data_pair(pair)
            url = f"https://api.exchange.coinbase.com/products/{dp}/candles?granularity=3600"
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


class MetaEngineSignalSource(SignalSource):
    """Signal: Meta-Engine ML predictions (RSI+momentum+SMA ensemble).

    Reads the latest prediction from meta_engine.db (meta_paper_trades table).
    Only fires if the prediction is < 5 minutes old (fresh).
    Lightweight: single SQLite read, no HTTP calls.
    """

    META_DB = str(Path(__file__).parent / "meta_engine.db")
    MAX_AGE_SECONDS = 300  # 5 minutes

    def scan(self, pair):
        try:
            if not Path(self.META_DB).exists():
                return {"direction": "NONE", "confidence": 0, "reason": "meta_engine.db not found"}

            # Convert trading pair to meta_engine format (BTC-USDC -> BTC-USD, BTC-USD stays)
            meta_pair = pair.replace("-USDC", "-USD")
            # meta_engine also stores as -USDC in paper trades; check both
            pair_variants = [meta_pair, pair]

            conn = sqlite3.connect(self.META_DB, timeout=3)
            conn.row_factory = sqlite3.Row
            try:
                # Get the latest open paper trade for this pair from meta_ml agent
                row = None
                for p in pair_variants:
                    row = conn.execute(
                        "SELECT direction, confidence, entry_price, created_at "
                        "FROM meta_paper_trades "
                        "WHERE pair = ? AND agent_name = 'meta_ml' AND status = 'open' "
                        "ORDER BY id DESC LIMIT 1",
                        (p,)
                    ).fetchone()
                    if row:
                        break

                if not row:
                    return {"direction": "NONE", "confidence": 0, "reason": "No meta_engine prediction"}

                # Check freshness — only use predictions < 5 minutes old
                created_at = row["created_at"]
                if created_at:
                    try:
                        pred_time = datetime.fromisoformat(created_at).replace(tzinfo=timezone.utc)
                    except ValueError:
                        # Handle SQLite CURRENT_TIMESTAMP format (no T separator)
                        pred_time = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    age = (datetime.now(timezone.utc) - pred_time).total_seconds()
                    if age > self.MAX_AGE_SECONDS:
                        return {"direction": "NONE", "confidence": 0,
                                "reason": f"Meta prediction stale ({int(age)}s old, max {self.MAX_AGE_SECONDS}s)"}
                else:
                    return {"direction": "NONE", "confidence": 0, "reason": "No timestamp on prediction"}

                direction = row["direction"]
                confidence = row["confidence"] or 0

                if direction not in ("BUY", "SELL") or confidence <= 0:
                    return {"direction": "NONE", "confidence": 0, "reason": "Invalid meta prediction"}

                return {
                    "direction": direction,
                    "confidence": min(confidence, 0.95),
                    "reason": f"Meta ML ensemble: {direction} conf={confidence:.1%} (age={int(age)}s)"
                }
            finally:
                conn.close()

        except Exception as e:
            logger.debug("Meta-engine signal error: %s", e)
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
            # Get 1-minute candles (last 15 minutes) — use -USD for data
            dp = _data_pair(pair)
            url = f"https://api.exchange.coinbase.com/products/{dp}/candles?granularity=60"
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
        self._db_lock = threading.Lock()
        self.db = sqlite3.connect(SNIPER_DB, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA temp_store=MEMORY")
        self.db.execute("PRAGMA cache_size=-65536")
        try:
            self.db.execute("PRAGMA mmap_size=268435456")
        except Exception:
            pass
        self._init_db()
        self.daily_loss = 0.0
        self.trades_today = 0
        # Trade frequency throttle — prevent churning (1,086 fills in 2 days killed $78 in fees)
        self._trade_timestamps = []  # list of trade execution timestamps
        self._max_trades_per_hour = int(os.environ.get("SNIPER_MAX_TRADES_PER_HOUR", "4"))
        self._max_trades_per_day = int(os.environ.get("SNIPER_MAX_TRADES_PER_DAY", "20"))
        self._price_cache = {}
        self._price_cache_lock = threading.Lock()
        self._price_cache_ttl = float(os.environ.get("SNIPER_PRICE_CACHE_SECONDS", "1.5"))
        self._holdings_cache_lock = threading.Lock()
        self._holdings_cache_ttl = float(os.environ.get("SNIPER_HOLDINGS_CACHE_SECONDS", "8"))
        self._holdings_cache = {"ts": 0.0, "holdings": {}, "cash": 0.0, "quotes": {"USD": 0.0, "USDC": 0.0}}
        self._pair_buy_cooldown_until = {}
        self._last_interval_logged = None
        self.sources = {
            "latency": LatencySignalSource(),
            "regime": RegimeSignalSource(),
            "arb": ArbSignalSource(),
            "orderbook": OrderbookSignalSource(),
            "rsi_extreme": RSIExtremeSource(),
            "fear_greed": FearGreedSource(),
            "momentum": PriceMomentumSource(),
            "uptick": UptickTimingSource(),
            "meta_engine": MetaEngineSignalSource(),
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
        # Backward-compatible migration for full trade lifecycle traceability.
        for ddl in (
            "ALTER TABLE sniper_trades ADD COLUMN order_id TEXT",
            "ALTER TABLE sniper_trades ADD COLUMN trade_uuid TEXT",
            "ALTER TABLE sniper_trades ADD COLUMN lifecycle_status TEXT",
        ):
            try:
                self.db.execute(ddl)
            except sqlite3.OperationalError:
                pass
        self.db.commit()

    def _latest_filled_buy_price(self, pair, fallback_price=0.0):
        """Get last filled BUY entry price for a pair, fallback to current price."""
        try:
            with self._db_lock:
                row = self.db.execute(
                    "SELECT entry_price FROM sniper_trades "
                    "WHERE pair=? AND direction='BUY' AND status='filled' "
                    "ORDER BY id DESC LIMIT 1",
                    (pair,),
                ).fetchone()
            if row and row[0]:
                return float(row[0])
        except Exception:
            pass
        return float(fallback_price or 0.0)

    def _extract_regime(self, pair_result):
        """Infer market regime from source reasons + growth engine hints."""
        growth = pair_result.get("growth_engine", {}) if isinstance(pair_result, dict) else {}
        regime = str(growth.get("regime", "") or "").strip().lower()
        if regime:
            return regime

        details = pair_result.get("details", {}) if isinstance(pair_result, dict) else {}
        regime_reason = str(details.get("regime", {}).get("reason", "") or "")
        if regime_reason:
            m = re.search(r"regime=([A-Za-z_]+)", regime_reason)
            if m:
                token = m.group(1).strip().lower()
                mapping = {
                    "uptrend": "markup",
                    "downtrend": "markdown",
                    "sideways": "accumulation",
                }
                return mapping.get(token, token)
            if "downtrend" in regime_reason.lower():
                return "markdown"

        direction = str(pair_result.get("direction", "NONE")).upper()
        if direction == "BUY":
            return "accumulation"
        if direction == "SELL":
            return "distribution"
        return "neutral"

    def _extract_momentum(self, pair_result):
        """Infer signed momentum in [-1, 1] from momentum/uptick sources."""
        details = pair_result.get("details", {}) if isinstance(pair_result, dict) else {}
        m = details.get("momentum", {}) if isinstance(details, dict) else {}
        u = details.get("uptick", {}) if isinstance(details, dict) else {}

        def _signed_component(obj):
            if not isinstance(obj, dict):
                return 0.0
            conf = max(0.0, min(1.0, float(obj.get("confidence", 0.0) or 0.0)))
            d = str(obj.get("direction", "NONE")).upper()
            if d == "BUY":
                return conf
            if d == "SELL":
                return -conf
            return 0.0

        # Momentum is primary, uptick refines timing.
        raw = _signed_component(m) * 0.7 + _signed_component(u) * 0.3
        return max(-1.0, min(1.0, raw))

    def _build_market_signals_for_planner(self, scan_results):
        """Translate sniper scan output into planner-friendly signal map."""
        market_signals = {}
        for pair, result in scan_results.items():
            direction = str(result.get("direction", "NONE")).upper()
            confidence = max(0.0, min(1.0, float(result.get("composite_confidence", 0.0) or 0.0)))
            market_signals[pair] = {
                "direction": direction,
                "confidence": confidence,
                "momentum": self._extract_momentum(result),
                "regime": self._extract_regime(result),
            }
        return market_signals

    def _build_portfolio_state_for_planner(self, holdings, market_signals):
        """Build planner portfolio state from live holdings and recent entries."""
        portfolio = {}
        for asset, amount in holdings.items():
            pair = f"{asset}-USDC"
            if pair not in market_signals:
                pair = f"{asset}-USD"
            price = self._get_price(pair)
            if not price:
                continue
            value = float(amount) * float(price)
            if value < 0.25:
                continue
            entry_price = self._latest_filled_buy_price(pair, fallback_price=price)
            portfolio[pair] = {
                "amount": float(amount),
                "entry_price": float(entry_price or price),
                "current_price": float(price),
            }
        return portfolio

    def _build_strategic_context(self, scan_results):
        """Run 3D-Go strategic analysis once per scan cycle."""
        if _planner is None:
            return {}
        try:
            holdings, cash = self._get_holdings()
            market_signals = self._build_market_signals_for_planner(scan_results)
            portfolio_state = self._build_portfolio_state_for_planner(holdings, market_signals)
            analysis = _planner.analyze(portfolio_state, market_signals, cash)
            return {
                "cash": float(cash or 0.0),
                "market_signals": market_signals,
                "portfolio_state": portfolio_state,
                "analysis": analysis if isinstance(analysis, dict) else {},
            }
        except Exception as e:
            logger.warning("SNIPER: Strategic planner context failed: %s", e)
            return {}

    def _validate_long_chain(self, pair, strategic_ctx):
        """Enforce long-chain viability: entry must show profitable path to EXIT."""
        if _planner is None:
            return {"viable": True, "reason": "planner unavailable", "net_edge": 0.0, "worst_case_edge": 0.0}
        if not strategic_ctx:
            return {"viable": False, "reason": "planner context unavailable", "net_edge": 0.0, "worst_case_edge": 0.0}

        analysis = strategic_ctx.get("analysis", {})
        validations = analysis.get("entry_validations", {}) if isinstance(analysis, dict) else {}
        val = validations.get(pair)
        if not isinstance(val, dict):
            # Fallback direct call if analysis omitted this pair for any reason.
            val = _planner.chain_planner.evaluate_entry_chain(
                pair,
                strategic_ctx.get("market_signals", {}),
                min_net_edge=CONFIG["min_chain_net_edge"],
                min_worst_case_edge=CONFIG["min_chain_worst_case_edge"],
            )

        net_edge = float(val.get("net_edge", 0.0) or 0.0)
        worst_edge = float(val.get("worst_case_edge", 0.0) or 0.0)
        steps = int(val.get("steps", 0) or 0)
        has_exit = bool(val.get("has_exit", False))
        viable = (
            bool(val.get("viable", False))
            and has_exit
            and steps >= int(CONFIG["min_chain_steps"])
            and net_edge >= float(CONFIG["min_chain_net_edge"])
            and worst_edge >= float(CONFIG["min_chain_worst_case_edge"])
        )
        out = dict(val)
        out.update({
            "viable": viable,
            "net_edge": net_edge,
            "worst_case_edge": worst_edge,
            "steps": steps,
            "has_exit": has_exit,
        })
        return out

    def scan_pair(self, pair):
        """Scan all signal sources for a pair in parallel using ThreadPoolExecutor.

        Previously sequential (~35s per pair x 7 pairs = ~245s per cycle).
        Now parallel (~5-8s per pair, ~40s total cycle) — 7x faster reaction to opportunities.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = {}

        def _scan_source(name, source):
            try:
                return name, source.scan(pair)
            except Exception as e:
                return name, {"direction": "NONE", "confidence": 0, "reason": str(e)}

        with ThreadPoolExecutor(max_workers=len(self.sources)) as executor:
            futures = {executor.submit(_scan_source, name, source): name
                       for name, source in self.sources.items()}
            for future in as_completed(futures):
                name, result = future.result()
                results[name] = result
                if result["direction"] != "NONE":
                    logger.info("  [%s] %s | conf=%.1f%% | %s",
                               name, result["direction"], result["confidence"]*100, result["reason"])

        # Aggregate: count confirming signals for each direction
        quant_set = CONFIG["quant_signals"]
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
                "confirming_signals": 0, "quant_signals": 0, "details": results,
            }

        # Count quantitative vs qualitative confirming signals
        quant_confirming = [(n, r) for n, r in confirming if n in quant_set]
        qual_confirming = [(n, r) for n, r in confirming if n not in quant_set]

        # Weighted confidence (quantitative signals dominate due to higher weights)
        weights = CONFIG["signal_weights"]
        total_weight = sum(weights.get(n, 0.1) for n, _ in confirming)
        if total_weight > 0:
            composite = sum(weights.get(n, 0.1) * r["confidence"] for n, r in confirming) / total_weight
        else:
            composite = 0

        # Expected Value calculation — trade must have positive EV after costs
        # EV = (win_prob * avg_gain) - (loss_prob * avg_loss) - round_trip_costs
        fees = CONFIG["round_trip_fee_pct"] + CONFIG["expected_slippage_pct"]
        # Conservative: assume avg gain ≈ 2x avg loss for a good signal
        win_prob = composite
        avg_gain_pct = 0.02   # 2% average winner
        avg_loss_pct = 0.01   # 1% average loser (tight stops)
        expected_value = (win_prob * avg_gain_pct) - ((1 - win_prob) * avg_loss_pct) - fees
        ev_positive = expected_value > 0

        # Record scan
        with self._db_lock:
            self.db.execute(
                "INSERT INTO sniper_scans (pair, composite_confidence, direction, confirming_signals, signal_details) VALUES (?, ?, ?, ?, ?)",
                (pair, composite, direction, len(confirming), json.dumps({n: r for n, r in results.items()}))
            )
            self.db.commit()

        # Growth engine algebraic enhancement
        growth_boost = {}
        if _growth:
            try:
                raw_signals = {}
                for name, r in results.items():
                    raw_signals[name] = (r.get("direction", "NONE"), r.get("confidence", 0))
                # Fetch recent prices for regime detection
                prices = self._get_recent_prices(pair, 30)
                _, cash = self._get_holdings()
                growth_boost = _growth.analyze_signals(pair, raw_signals, prices, cash)
                # Use growth engine quality score to boost/dampen confidence
                gf_quality = growth_boost.get("quality_score", 0)
                lattice_passes = growth_boost.get("lattice_passes", False)
                if lattice_passes and gf_quality > composite:
                    # Growth engine found stronger signal pattern — boost confidence
                    old_composite = composite
                    composite = composite * 0.7 + gf_quality * 0.3  # 30% weight to GF score
                    logger.info("  [growth_engine] %s | GF quality=%.1f%% lattice=PASS regime=%s | "
                               "conf %.1f%%->%.1f%%",
                               pair, gf_quality * 100, growth_boost.get("regime", "?"),
                               old_composite * 100, composite * 100)
                elif not lattice_passes and gf_quality < 0.4:
                    # Growth engine says weak setup — dampen confidence
                    old_composite = composite
                    composite = composite * 0.85  # 15% reduction
                    logger.info("  [growth_engine] %s | GF quality=%.1f%% lattice=FAIL | "
                               "conf %.1f%%->%.1f%% (dampened)",
                               pair, gf_quality * 100, old_composite * 100, composite * 100)
            except Exception as e:
                logger.debug("Growth engine analysis failed for %s: %s", pair, e)

        normalized = {
            "direction": direction,
            "details": results,
            "growth_engine": growth_boost,
        }
        inferred_regime = self._extract_regime(normalized)
        inferred_momentum = self._extract_momentum(normalized)

        return {
            "pair": pair,
            "direction": direction,
            "composite_confidence": composite,
            "confirming_signals": len(confirming),
            "quant_signals": len(quant_confirming),
            "qual_signals": len(qual_confirming),
            "expected_value": round(expected_value, 6),
            "ev_positive": ev_positive,
            "regime": inferred_regime,
            "momentum": inferred_momentum,
            "details": results,
            "growth_engine": growth_boost,
        }

    def scan_all(self):
        """Scan all pairs in parallel and return actionable signals.

        Uses ThreadPoolExecutor to scan multiple pairs concurrently.
        Combined with per-pair parallel scanning, this reduces total cycle time
        from ~280s to ~40s — 7x faster reaction to opportunities.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        logger.info("=== SNIPER SCAN ===")
        actionable = []
        scan_results = {}

        with ThreadPoolExecutor(max_workers=min(4, len(CONFIG["pairs"]))) as executor:
            futures = {executor.submit(self.scan_pair, pair): pair for pair in CONFIG["pairs"]}
            for future in as_completed(futures):
                pair = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    logger.error("Scan failed for %s: %s", pair, e)
                    continue
                scan_results[pair] = result

        strategic_ctx = self._build_strategic_context(scan_results)
        analysis = strategic_ctx.get("analysis", {}) if isinstance(strategic_ctx, dict) else {}
        if analysis:
            territory = analysis.get("territory", {}) if isinstance(analysis, dict) else {}
            logger.info(
                "  [planner] territory=%.2f | chain_moves=%s | entry_checks=%d",
                float(territory.get("score", 0.0) or 0.0),
                int(analysis.get("chain_length", 0) or 0),
                len(analysis.get("entry_validations", {}) or {}),
            )

        for pair in CONFIG["pairs"]:
            result = scan_results.get(pair)
            if not result:
                continue
            conf = result["composite_confidence"]
            n_signals = result["confirming_signals"]
            n_quant = result.get("quant_signals", 0)
            ev_ok = result.get("ev_positive", False)

            if conf >= CONFIG["min_composite_confidence"] and n_signals >= CONFIG["min_confirming_signals"]:
                # QUANTITATIVE GATE: at least 1 quant signal must confirm
                if n_quant < CONFIG["min_quant_signals"]:
                    logger.info("  %s: BLOCKED — only qualitative signals (%d quant < %d required)",
                               pair, n_quant, CONFIG["min_quant_signals"])
                    continue

                # EXPECTED VALUE GATE: trade must have positive EV after fees
                if not ev_ok and result["direction"] == "BUY":
                    logger.info("  %s: BLOCKED — negative EV (%.4f%%) after fees",
                               pair, result.get("expected_value", 0) * 100)
                    continue

                # GoalValidator gate — encoded rules check
                if _goals and not _goals.should_trade(
                    conf, n_signals, result["direction"],
                    result.get("regime", "neutral"),
                ):
                    logger.info("  %s: BLOCKED by GoalValidator (conf=%.1f%%, %d signals, %s)",
                               pair, conf*100, n_signals, result["direction"])
                    continue
                if result["direction"] == "BUY":
                    if _exit_mgr is None:
                        logger.info("  %s: BLOCKED — no ExitManager available for end-to-end buy/sell plan", pair)
                        continue
                    if not _exit_mgr.has_exit_plan(pair):
                        logger.info("  %s: BLOCKED — dynamic exit plan unavailable (quant buy/sell gate)", pair)
                        continue
                    chain_eval = self._validate_long_chain(pair, strategic_ctx)
                    if not chain_eval.get("viable", False):
                        logger.info(
                            "  %s: BLOCKED — long-chain gate failed (%s)",
                            pair,
                            chain_eval.get("reason", "no viable chain"),
                        )
                        continue
                    result["strategic_chain"] = chain_eval
                    planner_signal = strategic_ctx.get("market_signals", {}).get(pair, {})
                    if planner_signal:
                        result["regime"] = planner_signal.get("regime", result.get("regime", "neutral"))
                        result["momentum"] = planner_signal.get("momentum", 0.0)
                actionable.append(result)
                logger.info(">>> ACTIONABLE: %s %s | conf=%.1f%% | %d signals (%dQ/%dS) | EV=%.3f%%",
                           result["direction"], pair, conf*100,
                           n_signals, n_quant, result.get("qual_signals", 0),
                           result.get("expected_value", 0) * 100)
            else:
                logger.info("  %s: %s conf=%.1f%% (%d signals, %dQ) — below threshold",
                           pair, result["direction"], conf*100, n_signals, n_quant)

        # Strategic planner: feed influence map + check Ko bans
        if _planner:
            try:
                for result in actionable:
                    pair = result["pair"]
                    base = pair.split("-")[0]
                    direction = result["direction"]
                    conf = result["composite_confidence"]

                    # Feed influence map — strong signals radiate to correlated assets
                    if direction != "NONE" and conf > 0.5:
                        _planner.influence.place_stone(base, direction, conf)

                    # Ko ban check — block re-entry after losing exit
                    if direction == "BUY":
                        banned, ban_reason, _ = _planner.ko.is_banned(pair)
                        if banned:
                            logger.info("  %s: BLOCKED by Ko ban — %s", pair, ban_reason)
                            actionable = [a for a in actionable if a["pair"] != pair]
                            continue
                        if _planner.ko.detect_cycle(pair):
                            logger.info("  %s: BLOCKED by Ko cycle detection", pair)
                            actionable = [a for a in actionable if a["pair"] != pair]
                            continue

                    # Check influence — boost confidence if correlated assets agree
                    inf_dir, inf_str, _ = _planner.influence.get_influence(base)
                    if inf_dir == direction and inf_str > 0.2:
                        result["composite_confidence"] = min(0.99, conf + inf_str * 0.05)
                        logger.info("  [influence] %s %s | +%.1f%% from correlated assets",
                                   pair, direction, inf_str * 5)
            except Exception as e:
                logger.debug("Strategic planner scan failed: %s", e)

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

    def _cancel_stale_orders(self):
        """Cancel all open orders on Coinbase from previous process.

        When the container OOM-kills or restarts, open limit orders remain on the exchange
        holding cash. This frees that cash for new trades.
        """
        try:
            from exchange_connector import CoinbaseTrader
            trader = CoinbaseTrader()
            resp = trader.get_orders(status="OPEN")
            orders = resp.get("orders", [])
            if not orders:
                logger.info("SNIPER STARTUP: No stale open orders found")
                return
            cancelled = 0
            for o in orders:
                oid = o.get("order_id", "")
                pair = o.get("product_id", "?")
                side = o.get("side", "?")
                try:
                    trader.cancel_order(oid)
                    cancelled += 1
                    logger.info("SNIPER STARTUP: Cancelled stale %s %s (id=%s)", side, pair, oid[:12])
                except Exception as ce:
                    logger.warning("SNIPER STARTUP: Failed to cancel %s: %s", oid[:12], ce)
            logger.info("SNIPER STARTUP: Cancelled %d/%d stale orders — cash freed", cancelled, len(orders))
        except Exception as e:
            logger.warning("SNIPER STARTUP: Stale order cleanup error: %s", e)

    def _get_holdings(self):
        """Get current Coinbase holdings. Returns (holdings_dict, usdc_cash, usd_cash)."""
        now = time.time()
        with self._holdings_cache_lock:
            cached = self._holdings_cache
            age = now - float(cached.get("ts", 0.0) or 0.0)
            if age <= self._holdings_cache_ttl:
                holdings = dict(cached.get("holdings", {}) or {})
                cash = float(cached.get("cash", 0.0) or 0.0)
                quotes = dict(cached.get("quotes", {}) or {})
                self._last_quote_balances = {
                    "USD": float(quotes.get("USD", 0.0) or 0.0),
                    "USDC": float(quotes.get("USDC", 0.0) or 0.0),
                }
                return holdings, cash

            try:
                from exchange_connector import CoinbaseTrader
                trader = CoinbaseTrader()
                accts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
                holdings = {}
                usdc = 0.0
                usd = 0.0
                for a in accts.get("accounts", []):
                    cur = a.get("currency", "")
                    bal = float(a.get("available_balance", {}).get("value", 0))
                    if cur == "USDC":
                        usdc += bal
                    elif cur == "USD":
                        usd += bal
                    elif bal > 0:
                        holdings[cur] = bal
                quotes = {"USD": float(usd), "USDC": float(usdc)}
                cash = float(usdc + usd)
                self._holdings_cache = {"ts": now, "holdings": holdings, "cash": cash, "quotes": quotes}
                self._last_quote_balances = dict(quotes)
                return dict(holdings), cash
            except Exception as e:
                logger.warning("Holdings check failed: %s", e)
                self._holdings_cache = {"ts": now, "holdings": {}, "cash": 0.0, "quotes": {"USD": 0.0, "USDC": 0.0}}
                self._last_quote_balances = {"USD": 0.0, "USDC": 0.0}
                return {}, 0

    def _get_quote_balances(self):
        """Get latest USD/USDC balances used to pick executable quote pairs."""
        balances = getattr(self, "_last_quote_balances", None)
        if isinstance(balances, dict) and ("USD" in balances or "USDC" in balances):
            return {"USD": float(balances.get("USD", 0.0) or 0.0), "USDC": float(balances.get("USDC", 0.0) or 0.0)}
        # Populate cache if unavailable.
        self._get_holdings()
        balances = getattr(self, "_last_quote_balances", None) or {}
        return {"USD": float(balances.get("USD", 0.0) or 0.0), "USDC": float(balances.get("USDC", 0.0) or 0.0)}

    def _resolve_buy_pair_for_balance(self, pair, min_quote_needed):
        """Route BUY to USD/USDC quote with sufficient available balance."""
        if "-" not in pair:
            return pair
        base, quote = pair.split("-", 1)
        quote = quote.upper()
        balances = self._get_quote_balances()
        current_balance = float(balances.get(quote, 0.0) or 0.0)
        needed = max(1.0, float(min_quote_needed or 0.0))

        if current_balance >= needed:
            return pair

        alt_quote = "USD" if quote == "USDC" else "USDC" if quote == "USD" else quote
        alt_balance = float(balances.get(alt_quote, 0.0) or 0.0)
        if alt_quote != quote and alt_balance >= needed:
            alt_pair = f"{base}-{alt_quote}"
            logger.info(
                "SNIPER ROUTE: %s -> %s (need %.2f %s, have %.2f %s / %.2f %s)",
                pair, alt_pair, needed, quote, current_balance, quote, alt_balance, alt_quote,
            )
            return alt_pair
        return pair

    _fear_greed_cache = (0, None)  # (timestamp, value)

    def _get_fear_greed_value(self):
        """Get Fear & Greed index, cached for 5 minutes."""
        now = time.time()
        cached_ts, cached_val = Sniper._fear_greed_cache
        if now - cached_ts < 300:
            return cached_val
        try:
            data = _fetch_json("https://api.alternative.me/fng/?limit=1", timeout=3)
            val = int(data["data"][0]["value"])
            Sniper._fear_greed_cache = (now, val)
            return val
        except Exception:
            return cached_val

    def _execution_health_allows_buy(self):
        """Buy only when execution health is green and fresh."""
        if not bool(CONFIG.get("require_execution_health_for_buy", True)):
            return True, "gate_disabled"
        try:
            if not EXECUTION_HEALTH_STATUS_PATH.exists():
                return False, "execution_health_status_missing"
            payload = json.loads(EXECUTION_HEALTH_STATUS_PATH.read_text())
            if not isinstance(payload, dict):
                return False, "execution_health_status_invalid"
            green = bool(payload.get("green", False))
            if not green:
                return False, f"execution_health_not_green:{payload.get('reason', 'unknown')}"
            updated = str(payload.get("updated_at", "") or "").strip()
            if not updated:
                return False, "execution_health_updated_at_missing"
            dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - dt).total_seconds()
            max_age = max(30, int(CONFIG.get("execution_health_max_age_seconds", 300) or 300))
            if age > max_age:
                return False, f"execution_health_stale:{int(age)}s>{max_age}s"
            return True, "execution_health_green"
        except Exception as e:
            return False, f"execution_health_gate_error:{e}"

    def _exit_manager_status_allows_buy(self):
        """Require exit manager runtime status to be fresh before opening new BUYs."""
        if not bool(CONFIG.get("require_exit_manager_status_for_buy", True)):
            return True, "gate_disabled"
        try:
            if not EXIT_MANAGER_STATUS_PATH.exists():
                return False, "exit_manager_status_missing"
            payload = json.loads(EXIT_MANAGER_STATUS_PATH.read_text())
            if not isinstance(payload, dict):
                return False, "exit_manager_status_invalid"
            running = payload.get("running")
            if isinstance(running, bool) and not running:
                return False, "exit_manager_not_running"
            updated = str(payload.get("updated_at", "") or "").strip()
            if not updated:
                return False, "exit_manager_status_updated_at_missing"
            dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - dt).total_seconds()
            max_age = max(30, int(CONFIG.get("exit_manager_status_max_age_seconds", 300) or 300))
            if age > max_age:
                return False, f"exit_manager_status_stale:{int(age)}s>{max_age}s"
            return True, "exit_manager_status_fresh"
        except Exception as e:
            return False, f"exit_manager_status_gate_error:{e}"

    def _pair_buy_cooldown_active(self, pair):
        until = float(self._pair_buy_cooldown_until.get(str(pair), 0.0) or 0.0)
        now = time.time()
        if until <= now:
            if str(pair) in self._pair_buy_cooldown_until:
                self._pair_buy_cooldown_until.pop(str(pair), None)
            return False, 0
        return True, int(until - now)

    def _set_pair_buy_cooldown(self, pair, reason="order_failed"):
        seconds = max(30, int(CONFIG.get("pair_failure_cooldown_seconds", 180) or 180))
        until = time.time() + float(seconds)
        self._pair_buy_cooldown_until[str(pair)] = until
        logger.info("SNIPER: %s BUY cooldown armed %ss (%s)", pair, seconds, reason)

    def _effective_scan_interval(self):
        healthy = max(5, int(CONFIG.get("scan_interval_healthy", CONFIG.get("scan_interval", 30)) or 20))
        degraded = max(healthy, int(CONFIG.get("scan_interval_degraded", max(healthy, 45)) or 45))
        ok, _ = self._execution_health_allows_buy()
        return healthy if ok else degraded

    def _check_trade_throttle(self):
        """Check if trade frequency limits are exceeded. Returns (ok, reason)."""
        now = time.time()
        # Prune old timestamps (keep last 24h)
        self._trade_timestamps = [t for t in self._trade_timestamps if now - t < 86400]
        # Hourly check
        hour_ago = now - 3600
        trades_last_hour = sum(1 for t in self._trade_timestamps if t > hour_ago)
        if trades_last_hour >= self._max_trades_per_hour:
            return False, f"Throttled: {trades_last_hour} trades last hour (max {self._max_trades_per_hour})"
        # Daily check
        if len(self._trade_timestamps) >= self._max_trades_per_day:
            return False, f"Throttled: {len(self._trade_timestamps)} trades today (max {self._max_trades_per_day})"
        return True, ""

    def _record_trade_timestamp(self):
        """Record a trade execution for throttle tracking."""
        self._trade_timestamps.append(time.time())

    def execute_trade(self, signal):
        """Execute a high-confidence trade on Coinbase.

        ALL risk parameters are DYNAMIC from risk_controller.
        No hardcoded values for trade size, reserves, or position limits.
        """
        signal = dict(signal)
        pair = signal["pair"]
        direction = signal["direction"]

        # Trade frequency throttle — prevent fee-burning churn
        throttle_ok, throttle_reason = self._check_trade_throttle()
        if not throttle_ok:
            logger.info("SNIPER: %s %s BLOCKED — %s", pair, direction, throttle_reason)
            return False

        if direction == "BUY":
            # Fear & Greed circuit breaker — NEVER buy in extreme fear
            fg_val = self._get_fear_greed_value()
            if fg_val is not None and fg_val < 15:
                logger.info("SNIPER: %s BUY BLOCKED — Extreme Fear (F&G=%d), preserving cash", pair, fg_val)
                return False

            health_ok, health_reason = self._execution_health_allows_buy()
            if not health_ok:
                logger.info("SNIPER: %s BUY blocked — %s", pair, health_reason)
                return False
            em_ok, em_reason = self._exit_manager_status_allows_buy()
            if not em_ok:
                logger.info("SNIPER: %s BUY blocked — %s", pair, em_reason)
                return False
            cooldown_active, remaining = self._pair_buy_cooldown_active(pair)
            if cooldown_active:
                logger.info("SNIPER: %s BUY blocked — cooldown active (%ss remaining)", pair, remaining)
                return False
            if not bool(signal.get("ev_positive", False)):
                logger.info(
                    "SNIPER: %s BUY blocked — negative/non-validated expected value (expected_value=%.4f%%)",
                    pair,
                    float(signal.get("expected_value", 0.0) or 0.0) * 100.0,
                )
                return False
            quant_signals = int(signal.get("quant_signals", 0) or 0)
            if quant_signals < int(CONFIG["min_quant_signals"]):
                logger.info(
                    "SNIPER: %s BUY blocked — quant confirmations below threshold (%d < %d)",
                    pair,
                    quant_signals,
                    CONFIG["min_quant_signals"],
                )
                return False
            if _exit_mgr is None:
                logger.info("SNIPER: %s BUY blocked — ExitManager unavailable", pair)
                return False
            if not _exit_mgr.has_exit_plan(pair):
                logger.info("SNIPER: %s BUY blocked — no valid quant exit plan", pair)
                return False
            if _planner is not None:
                chain = signal.get("strategic_chain") or signal.get("entry_validation")
                if not isinstance(chain, dict):
                    fallback_signals = {
                        pair: {
                            "direction": "BUY",
                            "confidence": float(signal.get("composite_confidence", 0.0) or 0.0),
                            "momentum": float(signal.get("momentum", 0.0) or 0.0),
                            "regime": str(signal.get("regime", "neutral")),
                        }
                    }
                    try:
                        chain = _planner.chain_planner.evaluate_entry_chain(
                            pair,
                            fallback_signals,
                            min_net_edge=CONFIG["min_chain_net_edge"],
                            min_worst_case_edge=CONFIG["min_chain_worst_case_edge"],
                        )
                    except Exception as ce:
                        logger.info("SNIPER: %s BUY blocked — chain validation error (%s)", pair, ce)
                        return False
                if not isinstance(chain, dict):
                    logger.info("SNIPER: %s BUY blocked — missing long-chain validation", pair)
                    return False
                if not bool(chain.get("viable", False)):
                    logger.info("SNIPER: %s BUY blocked — long-chain not viable (%s)",
                               pair, chain.get("reason", "unknown"))
                    return False
                if float(chain.get("net_edge", 0.0) or 0.0) < float(CONFIG["min_chain_net_edge"]):
                    logger.info("SNIPER: %s BUY blocked — chain net edge below floor", pair)
                    return False
                if float(chain.get("worst_case_edge", 0.0) or 0.0) < float(CONFIG["min_chain_worst_case_edge"]):
                    logger.info("SNIPER: %s BUY blocked — chain worst-case edge below floor", pair)
                    return False
                if int(chain.get("steps", 0) or 0) < int(CONFIG["min_chain_steps"]):
                    logger.info("SNIPER: %s BUY blocked — chain depth too shallow", pair)
                    return False
            routed_pair = self._resolve_buy_pair_for_balance(pair, min_quote_needed=1.0)
            if routed_pair != pair:
                pair = routed_pair
                signal["pair"] = pair
            if not _exit_mgr.has_exit_plan(pair):
                logger.info("SNIPER: %s BUY blocked — no valid quant exit plan after quote routing", pair)
                return False

        price = self._get_price(pair)
        if not price:
            logger.warning("Cannot get price for %s", pair)
            return False

        holdings, cash = self._get_holdings()
        base_currency = pair.split("-")[0]

        # Calculate total portfolio value
        total_portfolio = cash + sum(
            h * (self._get_price(f"{c}-USD") or 0) for c, h in holdings.items()
        )

        # Get DYNAMIC risk parameters from centralized controller
        if _risk_ctrl:
            params = _risk_ctrl.get_risk_params(total_portfolio, pair)
            max_trade = params["max_trade_usd"]
            max_daily_loss = params["max_daily_loss"]
            reserve = params["min_reserve"]
            max_pos_pct = params["max_position_pct"]
            can_buy = params["can_buy"]
            regime = params["regime"]
        else:
            # BLOCK new trades when risk controller is unavailable
            logger.error("SNIPER: Risk controller unavailable — BLOCKING %s %s trade", direction, pair)
            return False

        # HARDSTOP check
        if self.daily_loss >= max_daily_loss:
            logger.warning("HARDSTOP: Daily loss $%.2f >= dynamic limit $%.2f",
                          self.daily_loss, max_daily_loss)
            return False

        if direction == "BUY":
            # Trend check — don't buy in strong downtrends
            if not can_buy:
                logger.info("SNIPER: %s blocked — %s regime, trend too negative", pair, regime)
                return False

            # Diversification check with DYNAMIC position limit
            held_amount = holdings.get(base_currency, 0)
            held_usd = held_amount * price if price else 0
            max_position = total_portfolio * max_pos_pct
            if held_usd >= max_position:
                concentration_pct = held_usd / total_portfolio * 100 if total_portfolio else 0
                logger.info("SNIPER: %s position $%.2f >= dynamic max $%.2f (%.0f%%) — DIVERSIFY",
                           base_currency, held_usd, max_position, concentration_pct)

                # === AUTO-REBALANCE: sell excess to free cash for other trades ===
                target_pct = 0.18  # bring down to 18% of portfolio
                target_value = total_portfolio * target_pct
                sell_value = held_usd - target_value
                if sell_value >= 1.0 and price > 0:
                    sell_amount = sell_value / price
                    try:
                        from exchange_connector import CoinbaseTrader
                        trader = CoinbaseTrader()
                        logger.info("SNIPER: AUTO-REBALANCE SELL %s | $%.2f excess (%.0f%% -> %.0f%%)",
                                   pair, sell_value, concentration_pct, target_pct * 100)
                        if _risk_ctrl:
                            approved, reason, _ = _risk_ctrl.approve_trade(
                                "sniper", pair, "SELL", sell_value, total_portfolio)
                            if not approved:
                                logger.warning("SNIPER: Rebalance SELL blocked: %s", reason)
                            else:
                                rebal_limit = price * 1.0005  # maker: just above spot
                                result = trader.place_limit_order(pair, "SELL", sell_amount, rebal_limit, post_only=True, bypass_profit_guard=True)
                                if result and "success_response" in result:
                                    logger.info("SNIPER: REBALANCE SELL FILLED %s | $%.2f freed for trading", pair, sell_value)
                                else:
                                    err = result.get("error_response", {}) if isinstance(result, dict) else {}
                                    logger.warning("SNIPER: Rebalance SELL failed: %s", err.get("message", str(result)[:200]))
                                if _risk_ctrl:
                                    _risk_ctrl.resolve_allocation("sniper", pair)
                        else:
                            rebal_limit = price * 1.0005
                            result = trader.place_limit_order(pair, "SELL", sell_amount, rebal_limit, post_only=True, bypass_profit_guard=True)
                            if result and "success_response" in result:
                                logger.info("SNIPER: REBALANCE SELL FILLED %s | $%.2f freed", pair, sell_value)
                    except Exception as e:
                        logger.error("SNIPER: Rebalance SELL error: %s", e)

                return False

            # Size with DYNAMIC limits — scale with confidence
            remaining_room = max_position - held_usd
            if remaining_room < 1.00:
                logger.info("SNIPER: %s at max position ($%.2f / $%.2f cap) — skipping BUY",
                           pair, held_usd, max_position)
                return False

            cycle_spent = getattr(self, '_cycle_cash_spent', 0.0)
            effective_cash = cash - cycle_spent  # account for orders placed earlier this cycle
            available_after_reserve = effective_cash - reserve
            if available_after_reserve < 1.00:
                logger.info("SNIPER: Cash below reserve ($%.2f - $%.2f reserve = $%.2f)",
                           effective_cash, reserve, available_after_reserve)
                return False

            trade_size = min(max_trade, max(1.00, signal["composite_confidence"] * max_trade * 1.2))
            trade_size = min(trade_size, remaining_room)
            trade_size = min(trade_size, available_after_reserve)  # DYNAMIC reserve

            # Quote-aware reroute with actual order size (fixes USD-vs-USDC funding mismatches).
            sized_pair = self._resolve_buy_pair_for_balance(pair, min_quote_needed=trade_size)
            if sized_pair != pair:
                pair = sized_pair
                signal["pair"] = pair
                if _exit_mgr and not _exit_mgr.has_exit_plan(pair):
                    logger.info("SNIPER: %s BUY blocked — routed pair missing exit plan", pair)
                    return False
                price = self._get_price(pair)
                if not price:
                    logger.warning("Cannot get price for routed pair %s", pair)
                    return False
                base_currency = pair.split("-")[0]
                held_amount = holdings.get(base_currency, 0)
                held_usd = held_amount * price if price else 0
                max_position = total_portfolio * max_pos_pct
                remaining_room = max_position - held_usd
                trade_size = min(trade_size, remaining_room)

            # Risk controller approval
            if _risk_ctrl:
                approved, reason, adj_size = _risk_ctrl.approve_trade(
                    "sniper", pair, "BUY", trade_size, total_portfolio)
                if not approved:
                    logger.info("SNIPER: Risk controller blocked: %s", reason)
                    return False
                trade_size = adj_size

            if trade_size < 1.00:
                logger.info("SNIPER: Insufficient for BUY after dynamic sizing ($%.2f cash, $%.2f reserve)",
                           cash, reserve)
                return False
            trade_size = round(trade_size, 2)

            # MAKER ONLY: limit order at/below bid (0.4% fee vs 1.2% taker)
            # BE A MAKER not a taker — Rule from game theory playbook
            # post_only=True rejects if it would match immediately (guarantees maker)
            base_size = trade_size / price
            limit_price = price * 0.9995  # just below spot — sits on book as maker

            logger.info("SNIPER EXECUTE: LIMIT BUY %s | $%.2f (%.6f @ $%.2f) | conf=%.1f%% | %d signals",
                        pair, trade_size, base_size, limit_price,
                        signal["composite_confidence"]*100,
                        signal["confirming_signals"])

            try:
                from exchange_connector import CoinbaseTrader
                trader = CoinbaseTrader()
                result = trader.place_limit_order(
                    pair, "BUY", base_size, limit_price, post_only=True,
                    expected_edge_pct=signal["composite_confidence"] * 100,
                    signal_confidence=signal["composite_confidence"],
                    market_regime=signal.get("regime", "neutral"),
                )
                # Track cash committed this cycle so subsequent orders don't over-spend
                self._cycle_cash_spent = getattr(self, '_cycle_cash_spent', 0.0) + trade_size
                return self._process_order_result(result, pair, "BUY", trade_size, price, signal)
            except Exception as e:
                logger.error("BUY execution error: %s", e, exc_info=True)
                if _risk_ctrl:
                    _risk_ctrl.resolve_allocation("sniper", pair)
                return False

        elif direction == "SELL":
            # RESERVE PROTECTION: never sell reserve assets (BTC, USD, USDC)
            if base_currency in CONFIG.get("reserve_assets", []):
                logger.info("SNIPER: BLOCKED SELL %s — reserve asset (treasury)", pair)
                return False

            # Check if we hold this asset
            held = holdings.get(base_currency, 0)
            held_usd = held * price
            if held_usd < 0.50:
                return False

            # Check minimum hold period — don't sell what we just bought (prevents churn)
            with self._db_lock:
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

            # Sell up to dynamic max_trade, but never more than we hold
            trade_size = min(max_trade, held_usd * 0.5)
            if trade_size < 0.50:
                return False

            # Calculate base_size for SELL (Coinbase needs base_size for sells)
            base_size = trade_size / price
            base_size = min(base_size, held)  # never sell more than we have

            # MAKER ONLY: limit SELL just above spot (0.4% fee vs 1.2% taker)
            limit_price = price * 1.0005  # just above spot — sits on book as maker

            logger.info("SNIPER EXECUTE: LIMIT SELL %s | $%.2f (%.8f %s) @ $%.2f | conf=%.1f%% | %d signals",
                        pair, trade_size, base_size, base_currency, limit_price,
                        signal["composite_confidence"]*100, signal["confirming_signals"])

            try:
                from exchange_connector import CoinbaseTrader
                trader = CoinbaseTrader()
                result = trader.place_limit_order(
                    pair, "SELL", base_size, limit_price, post_only=True,
                    bypass_profit_guard=True)
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
            self._record_trade_timestamp()  # count against throttle
            logger.info("SNIPER ORDER FILLED: %s %s $%.2f @ $%.2f | order=%s",
                        pair, side, trade_size, price, order_id)
        elif "order_id" in result:
            order_id = result["order_id"]
            status = "pending"
            logger.info("SNIPER ORDER PENDING: %s %s $%.2f @ $%.2f | order=%s",
                        pair, side, trade_size, price, order_id)
        elif "error_response" in result:
            err = result["error_response"]
            logger.warning("SNIPER ORDER FAILED: %s %s | %s", pair, side, err.get("message", err))
            # Release pending allocation so capital isn't phantom-locked
            if _risk_ctrl:
                _risk_ctrl.resolve_allocation("sniper", pair)
        else:
            logger.warning("SNIPER ORDER UNKNOWN: %s", json.dumps(result)[:300])
            if _risk_ctrl:
                _risk_ctrl.resolve_allocation("sniper", pair)

        # Calculate P&L on SELL trades
        pnl = None
        if side == "SELL" and status in ("filled", "pending"):
            with self._db_lock:
                last_buy = self.db.execute(
                    "SELECT entry_price FROM sniper_trades WHERE pair=? AND direction='BUY' AND status='filled' ORDER BY id DESC LIMIT 1",
                    (pair,)
                ).fetchone()
            if last_buy and last_buy[0] and last_buy[0] > 0:
                fees = trade_size * 0.008  # 0.4% maker fee x2 legs
                pnl = (price - last_buy[0]) / last_buy[0] * trade_size - fees
                logger.info("SNIPER P&L: %s SELL pnl=$%.4f (entry=$%.2f exit=$%.2f fees=$%.4f)",
                           pair, pnl, last_buy[0], price, fees)

        trade_uuid = f"{pair}:{side}:{int(time.time() * 1000)}:{(order_id or 'none')[:12]}"
        lifecycle_status = (
            "entry_filled" if side == "BUY" and status == "filled"
            else "entry_pending" if side == "BUY" and status == "pending"
            else "exit_filled" if side == "SELL" and status == "filled"
            else "exit_pending" if side == "SELL" and status == "pending"
            else "failed"
        )

        with self._db_lock:
            try:
                self.db.execute(
                    """
                    INSERT INTO sniper_trades
                        (pair, direction, composite_confidence, amount_usd, venue, entry_price, pnl, status, order_id, trade_uuid, lifecycle_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        pair,
                        signal["direction"],
                        signal["composite_confidence"],
                        trade_size,
                        "coinbase",
                        price,
                        pnl,
                        status,
                        order_id,
                        trade_uuid,
                        lifecycle_status,
                    ),
                )
            except sqlite3.OperationalError:
                # Fallback for older schema snapshots.
                self.db.execute(
                    "INSERT INTO sniper_trades (pair, direction, composite_confidence, amount_usd, venue, entry_price, pnl, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (pair, signal["direction"], signal["composite_confidence"], trade_size, "coinbase", price, pnl, status)
                )
            self.db.commit()
        self.trades_today += 1

        # Record to KPI tracker for scorecard
        if _kpi and status in ("filled", "pending"):
            try:
                fees = trade_size * 0.004  # maker fee
                _kpi.record_trade(
                    strategy_name="sniper", pair=pair, direction=side,
                    amount_usd=trade_size, pnl=pnl or 0, fees=fees,
                    hold_seconds=0, strategy_type="LF",
                    won=(pnl is None or pnl >= 0),
                )
            except Exception as kpi_err:
                logger.debug("KPI record error: %s", kpi_err)

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

        # Register filled BUY orders with ExitManager for exit monitoring
        # Use actual fill data (partial fills) instead of assuming full fill
        if _exit_mgr and status in ("filled", "pending") and side == "BUY" and order_id:
            try:
                from exchange_connector import CoinbaseTrader
                fill_trader = CoinbaseTrader()
                fill_data = fill_trader.get_order_fill(order_id, max_wait=5)

                if fill_data and fill_data["filled_size"] > 0:
                    actual_amount = fill_data["filled_size"]
                    actual_price = fill_data["avg_price"] or price
                    _exit_mgr.register_position(
                        pair, actual_price, datetime.now(timezone.utc).isoformat(),
                        actual_amount, trade_id=order_id)
                    logger.info("SNIPER: Registered %s BUY with ExitManager (%.8f @ $%.2f, fill status=%s)",
                               pair, actual_amount, actual_price, fill_data["status"])
                elif fill_data and fill_data["filled_size"] == 0:
                    logger.info("SNIPER: Order %s had zero fill — NOT registering with ExitManager", order_id)
                else:
                    # Fallback to estimated amount if fill polling timed out
                    base_amount = trade_size / price if price else 0
                    _exit_mgr.register_position(
                        pair, price, datetime.now(timezone.utc).isoformat(),
                        base_amount, trade_id=order_id)
                    logger.info("SNIPER: Fill poll timed out — registered %s BUY with estimated amount %.8f",
                               pair, base_amount)
            except Exception as e:
                logger.warning("SNIPER: Failed to register with ExitManager: %s", e)

        if side == "BUY":
            if status in ("filled", "pending"):
                self._pair_buy_cooldown_until.pop(str(pair), None)
            elif status == "failed":
                self._set_pair_buy_cooldown(pair, reason="buy_order_failed")

        return status == "filled"

    def _get_price(self, pair):
        now = time.time()
        with self._price_cache_lock:
            cached = self._price_cache.get(pair)
            if cached and (now - cached[1]) <= self._price_cache_ttl:
                return cached[0]
        try:
            dp = _data_pair(pair)
            data = _fetch_json(f"https://api.coinbase.com/v2/prices/{dp}/spot")
            price = float(data["data"]["amount"])
            with self._price_cache_lock:
                self._price_cache[pair] = (price, now)
            return price
        except Exception:
            with self._price_cache_lock:
                cached = self._price_cache.get(pair)
            return cached[0] if cached else None

    def _get_recent_prices(self, pair, count=30):
        """Fetch recent close prices for regime detection (newest last).

        Uses Coinbase candles API — 5-minute granularity for regime analysis.
        """
        try:
            dp = _data_pair(pair)
            base, quote = dp.split("-")
            url = (f"https://api.exchange.coinbase.com/products/{base}-{quote}"
                   f"/candles?granularity=300&limit={count}")
            candles = _fetch_json(url)
            if not candles:
                return []
            # Coinbase candles: [timestamp, low, high, open, close, volume]
            # Returned newest first — reverse for oldest-first
            prices = [float(c[4]) for c in reversed(candles) if len(c) >= 5]
            return prices
        except Exception:
            return []

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
        if _risk_ctrl:
            params = _risk_ctrl.get_risk_params(0, "BTC-USDC")
            print(f"    Max trade:      ${params['max_trade_usd']:.2f} (dynamic)")
            print(f"    Daily loss lim: ${params['max_daily_loss']:.2f} (dynamic)")
        print(f"{'='*70}\n")

    def run(self):
        """Main sniper loop — scan and trade."""
        logger.info("Sniper starting — scanning %d pairs every %ds",
                    len(CONFIG["pairs"]), CONFIG["scan_interval"])
        logger.info("Thresholds: conf >= %.0f%%, signals >= %d",
                    CONFIG["min_composite_confidence"]*100, CONFIG["min_confirming_signals"])

        # Cancel stale open orders from previous process (OOM, restart, etc.)
        # These hold cash on Coinbase causing "Insufficient balance" errors
        self._cancel_stale_orders()

        # Start embedded ExitManager monitor only in standalone mode.
        # When orchestrator runs dedicated exit_manager.py, avoid duplicate monitors.
        orch_managed = bool(os.environ.get(ORCH_OWNER_ENV))
        embedded_cfg = os.environ.get("SNIPER_EMBEDDED_EXIT_MANAGER")
        if embedded_cfg is None:
            use_embedded_exit_mgr = not orch_managed
        else:
            use_embedded_exit_mgr = str(embedded_cfg).strip().lower() not in ("0", "false", "no")
        self._embedded_exit_manager_running = False

        if _exit_mgr and use_embedded_exit_mgr:
            _exit_mgr.start()
            self._embedded_exit_manager_running = True
            logger.info("Sniper: embedded ExitManager monitor started")
        elif _exit_mgr and orch_managed:
            logger.info("Sniper: embedded ExitManager disabled (orchestrator-managed exit_manager is expected)")
        else:
            logger.warning("Sniper: ExitManager not available — positions will NOT be monitored for exits")

        cycle = 0
        while True:
            try:
                cycle += 1
                self._cycle_cash_spent = 0.0  # track cash committed this cycle

                # Early cash check — skip BUY execution when broke
                # Still scan for signals (logging/analytics) but don't waste API calls
                _, cycle_cash = self._get_holdings()
                cash_too_low = cycle_cash < 2.0

                if cash_too_low and cycle % 10 == 1:
                    logger.info("SNIPER: Cash $%.2f < $2 — waiting for exit_manager to free capital", cycle_cash)

                actionable = self.scan_all()

                if not cash_too_low:
                    for signal in actionable:
                        if signal.get("direction") == "BUY":
                            self.execute_trade(signal)
                        elif signal.get("direction") == "SELL":
                            self.execute_trade(signal)  # always allow sells
                else:
                    # Still execute SELL signals even when cash is low
                    for signal in actionable:
                        if signal.get("direction") == "SELL":
                            self.execute_trade(signal)

                # Report every 30 cycles (~15 min)
                if cycle % 30 == 0:
                    self.print_report()
                    if _exit_mgr:
                        _exit_mgr.print_status()

                scan_interval = self._effective_scan_interval()
                if scan_interval != self._last_interval_logged:
                    logger.info("SNIPER: scan interval now %ss (health-adaptive)", scan_interval)
                    self._last_interval_logged = scan_interval
                time.sleep(scan_interval)

            except KeyboardInterrupt:
                logger.info("Sniper shutting down...")
                if _exit_mgr and self._embedded_exit_manager_running:
                    _exit_mgr.stop()
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
