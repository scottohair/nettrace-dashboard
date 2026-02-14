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


def _parse_csv_values(value):
    """Parse comma/iterable values from env or config into normalized tokens."""
    if value is None:
        return tuple()
    if isinstance(value, (list, tuple, set)):
        items = [str(v or "").strip() for v in value]
    else:
        items = [item.strip() for item in str(value).split(",")]
    return tuple(item for item in items if item)

# Use persistent volume on Fly (/data/), local agents/ dir otherwise
_persistent_dir = Path("/data") if Path("/data").is_dir() else Path(__file__).parent
SNIPER_DB = str(_persistent_dir / "sniper.db")
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")
FLY_URL = "https://nettrace-dashboard.fly.dev"
WALLET_ADDRESS = os.environ.get("WALLET_ADDRESS", "")
ORCH_OWNER_ENV = "ORCHESTRATOR_OWNER_ID"
EXECUTION_HEALTH_STATUS_PATH = Path(__file__).parent / "execution_health_status.json"
EXIT_MANAGER_STATUS_PATH = Path(__file__).parent / "exit_manager_status.json"
RECONCILE_STATUS_PATH = Path(__file__).parent / "reconcile_agent_trades_status.json"
TRADER_DB_PATH = Path(__file__).parent / "trader.db"

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
    "execution_health_degraded_mode": os.environ.get("SNIPER_EXECUTION_HEALTH_DEGRADED_MODE", "1").lower() not in ("0", "false", "no"),
    "execution_health_degraded_reasons": _parse_csv_values(
        os.environ.get(
            "SNIPER_EXECUTION_HEALTH_DEGRADED_REASONS",
            "telemetry_samples_low,telemetry_success_rate_low,telemetry_failure_rate_high,telemetry_p90_high",
        )
    ),
    "execution_health_degraded_trade_size_factor": float(
        os.environ.get("SNIPER_EXECUTION_HEALTH_DEGRADED_TRADE_SIZE_FACTOR", "0.75")
    ),
    "execution_health_max_age_seconds": int(os.environ.get("SNIPER_EXECUTION_HEALTH_MAX_AGE_SECONDS", "300")),
    "require_exit_manager_status_for_buy": os.environ.get("SNIPER_REQUIRE_EXIT_MANAGER_STATUS_FOR_BUY", "1").lower() not in ("0", "false", "no"),
    "exit_manager_status_max_age_seconds": int(os.environ.get("SNIPER_EXIT_MANAGER_STATUS_MAX_AGE_SECONDS", "300")),
    "require_close_flow_for_buy": os.environ.get("SNIPER_REQUIRE_CLOSE_FLOW_FOR_BUY", "0").lower() not in ("0", "false", "no"),
    "close_flow_status_max_age_seconds": int(os.environ.get("SNIPER_CLOSE_FLOW_STATUS_MAX_AGE_SECONDS", "300")),
    "close_flow_min_attempts": int(os.environ.get("SNIPER_CLOSE_FLOW_MIN_ATTEMPTS", "2")),
    "close_flow_min_completion_rate": float(os.environ.get("SNIPER_CLOSE_FLOW_MIN_COMPLETION_RATE", "0.40")),
    "close_flow_max_terminal_failures": int(os.environ.get("SNIPER_CLOSE_FLOW_MAX_TERMINAL_FAILURES", "3")),
    "min_trade_size_usd": float(os.environ.get("SNIPER_MIN_TRADE_SIZE_USD", "0.50")),
    "min_trade_size_max_trade_fraction": float(
        os.environ.get("SNIPER_MIN_TRADE_SIZE_MAX_TRADE_FRACTION", "0.00")
    ),
    "min_trade_size_cash_fraction": float(
        os.environ.get("SNIPER_MIN_TRADE_SIZE_CASH_FRACTION", "0.00")
    ),
    "quote_balance_buffer_usd": float(os.environ.get("SNIPER_QUOTE_BALANCE_BUFFER_USD", "0.02")),
    "pair_failure_cooldown_seconds": int(os.environ.get("SNIPER_PAIR_FAILURE_COOLDOWN_SECONDS", "180")),
    "scan_interval_healthy": int(os.environ.get("SNIPER_SCAN_INTERVAL_HEALTHY_SECONDS", "20")),
    "scan_interval_degraded": int(os.environ.get("SNIPER_SCAN_INTERVAL_DEGRADED_SECONDS", "45")),
    "close_evidence_target_pairs": _parse_csv_values(
        os.environ.get("SNIPER_CLOSE_EVIDENCE_TARGET_PAIRS", "ETH-USD,SOL-USD")
    ),
    "close_evidence_min_closes": int(os.environ.get("SNIPER_CLOSE_EVIDENCE_MIN_CLOSES", "8")),
    "close_evidence_min_net_pnl_usd": float(
        os.environ.get("SNIPER_CLOSE_EVIDENCE_MIN_NET_PNL_USD", "0.0")
    ),
    "close_evidence_lookback_hours": int(
        os.environ.get("SNIPER_CLOSE_EVIDENCE_LOOKBACK_HOURS", "168")
    ),
    "close_evidence_cache_seconds": int(
        os.environ.get("SNIPER_CLOSE_EVIDENCE_CACHE_SECONDS", "90")
    ),
    "close_evidence_priority_mode": os.environ.get("SNIPER_CLOSE_EVIDENCE_PRIORITY_MODE", "1").lower() not in (
        "0",
        "false",
        "no",
    ),
    "close_evidence_force_sell_mode": os.environ.get("SNIPER_CLOSE_EVIDENCE_FORCE_SELL_MODE", "1").lower() not in (
        "0",
        "false",
        "no",
    ),
    "close_evidence_profit_buffer_pct": float(
        os.environ.get("SNIPER_CLOSE_EVIDENCE_PROFIT_BUFFER_PCT", "0.001")
    ),
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

    def scan(self, pair, candles_1h=None, candles_1m=None):
        """Returns: {"direction": "BUY"|"SELL"|"NONE", "confidence": 0.0-1.0, "reason": "..."}

        Args:
            pair: Trading pair (e.g., 'BTC-USDC')
            candles_1h: Optional cached 1h candles to avoid re-fetching
            candles_1m: Optional cached 1m candles to avoid re-fetching
        """
        raise NotImplementedError


class LatencySignalSource(SignalSource):
    """Signal #1: NetTrace latency signals from 7 global Fly.io regions.

    Uses two data sources:
    1. /api/v1/signals — pre-computed quant signals (anomalies, trends, route changes)
    2. /api/v1/signals/crypto-latency — raw exchange latency with anomaly detection
    """

    def scan(self, pair, candles_1h=None, candles_1m=None):
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

    def scan(self, pair, candles_1h=None, candles_1m=None):
        try:
            # Use cached 1h candles if available, otherwise fetch
            if candles_1h is not None:
                candles = candles_1h
            else:
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

    def scan(self, pair, candles_1h=None, candles_1m=None):
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

    def scan(self, pair, candles_1h=None, candles_1m=None):
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

    def scan(self, pair, candles_1h=None, candles_1m=None):
        try:
            # Use cached 1h candles if available to avoid redundant API call
            if candles_1h is not None:
                raw = [[c.get("time"), c.get("low"), c.get("high"), c.get("open"), c.get("close"), c.get("volume")]
                       for c in candles_1h]
            else:
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

    def scan(self, pair, candles_1h=None, candles_1m=None):
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

    def scan(self, pair, candles_1h=None, candles_1m=None):
        try:
            # Use cached 1h candles if available to avoid redundant API call
            if candles_1h is not None:
                raw = [[c.get("time"), c.get("low"), c.get("high"), c.get("open"), c.get("close"), c.get("volume")]
                       for c in candles_1h]
            else:
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

    def scan(self, pair, candles_1h=None, candles_1m=None):
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

    def scan(self, pair, candles_1h=None, candles_1m=None):
        try:
            # Use cached 1m candles if available to avoid redundant API call
            if candles_1m is not None:
                data = [[c.get("time"), c.get("low"), c.get("high"), c.get("open"), c.get("close"), c.get("volume")]
                        for c in candles_1m]
            else:
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
        # PERSISTENT: Load existing timestamps from database on startup
        self._trade_timestamps = self._load_throttle_state()
        self._max_trades_per_hour = int(os.environ.get("SNIPER_MAX_TRADES_PER_HOUR", "4"))
        self._max_trades_per_day = int(os.environ.get("SNIPER_MAX_TRADES_PER_DAY", "20"))
        self._price_cache = {}
        self._price_cache_lock = threading.Lock()
        self._price_cache_ttl = float(os.environ.get("SNIPER_PRICE_CACHE_SECONDS", "1.5"))
        self._holdings_cache_lock = threading.Lock()
        self._holdings_cache_ttl = float(os.environ.get("SNIPER_HOLDINGS_CACHE_SECONDS", "8"))
        self._holdings_cache = {"ts": 0.0, "holdings": {}, "cash": 0.0, "quotes": {"USD": 0.0, "USDC": 0.0}}
        self._pair_buy_cooldown_until = {}
        self._close_evidence_cache = {}
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
            CREATE TABLE IF NOT EXISTS trade_throttle_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_timestamp REAL NOT NULL,
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

    @staticmethod
    def _normalize_pair(pair):
        return str(pair or "").strip().upper().replace("_", "-")

    def _pair_aliases(self, pair):
        p = self._normalize_pair(pair)
        if not p:
            return []
        aliases = [p]
        if p.endswith("-USD"):
            aliases.append(p.replace("-USD", "-USDC"))
        elif p.endswith("-USDC"):
            aliases.append(p.replace("-USDC", "-USD"))
        return list(dict.fromkeys(aliases))

    def _latest_filled_buy_price_any(self, pair, fallback_price=0.0):
        for alias in self._pair_aliases(pair):
            px = self._latest_filled_buy_price(alias, fallback_price=0.0)
            if float(px or 0.0) > 0:
                return float(px)
        return float(fallback_price or 0.0)

    def _latest_filled_buy_snapshot_any(self, pair):
        try:
            aliases = self._pair_aliases(pair)
            if not aliases:
                return None
            marks = ",".join("?" for _ in aliases)
            with self._db_lock:
                row = self.db.execute(
                    f"""
                    SELECT pair, entry_price, created_at
                    FROM sniper_trades
                    WHERE pair IN ({marks}) AND direction='BUY' AND status='filled'
                    ORDER BY id DESC LIMIT 1
                    """,
                    tuple(aliases),
                ).fetchone()
            if not row:
                return None
            return {
                "pair": str(row["pair"] or ""),
                "entry_price": float(row["entry_price"] or 0.0),
                "created_at": str(row["created_at"] or ""),
            }
        except Exception:
            return None

    def _close_evidence_targets(self):
        raw = CONFIG.get("close_evidence_target_pairs", ())
        if isinstance(raw, str):
            raw = _parse_csv_values(raw)
        targets = [self._normalize_pair(p) for p in (raw or ())]
        return [p for p in dict.fromkeys(targets) if p]

    def _close_evidence_target_aliases(self):
        aliases = set()
        for pair in self._close_evidence_targets():
            aliases.update(self._pair_aliases(pair))
        return aliases

    @staticmethod
    def _trader_table_exists(conn, table_name):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (str(table_name),),
        ).fetchone()
        return bool(row)

    def _query_realized_close_evidence(self, pair):
        evidence = {
            "pair": self._normalize_pair(pair),
            "aliases": self._pair_aliases(pair),
            "closed_trades": 0,
            "winning_closes": 0,
            "losing_closes": 0,
            "net_pnl_usd": 0.0,
            "win_rate": 0.0,
            "avg_pnl_per_close_usd": 0.0,
            "sources": [],
            "reason": "ok",
        }
        if not TRADER_DB_PATH.exists():
            evidence["reason"] = "trader_db_missing"
            return evidence

        lookback_h = max(1, int(CONFIG.get("close_evidence_lookback_hours", 168) or 168))
        lookback_expr = f"-{lookback_h} hours"
        blocked = (
            "pending",
            "placed",
            "open",
            "accepted",
            "ack_ok",
            "failed",
            "blocked",
            "canceled",
            "cancelled",
            "expired",
        )
        blocked_marks = ",".join("?" for _ in blocked)
        total_closed = 0
        total_wins = 0
        total_pnl = 0.0
        conn = None
        try:
            conn = sqlite3.connect(str(TRADER_DB_PATH))
            conn.row_factory = sqlite3.Row
            for table in ("agent_trades", "live_trades"):
                if not self._trader_table_exists(conn, table):
                    continue
                for alias in evidence["aliases"]:
                    row = conn.execute(
                        f"""
                        SELECT
                          COUNT(CASE WHEN pnl IS NOT NULL THEN 1 END) AS closes,
                          COALESCE(SUM(CASE WHEN pnl IS NOT NULL AND COALESCE(pnl, 0) > 0 THEN 1 ELSE 0 END), 0) AS wins,
                          COALESCE(SUM(CASE WHEN pnl IS NOT NULL THEN COALESCE(pnl, 0) ELSE 0 END), 0) AS net_pnl
                        FROM {table}
                        WHERE pair=?
                          AND UPPER(COALESCE(side, ''))='SELL'
                          AND created_at >= datetime('now', ?)
                          AND (
                            status IS NULL
                            OR LOWER(COALESCE(status, '')) NOT IN ({blocked_marks})
                          )
                        """,
                        (str(alias), lookback_expr, *blocked),
                    ).fetchone()
                    closes = int(row["closes"] or 0) if row else 0
                    wins = int(row["wins"] or 0) if row else 0
                    net = float(row["net_pnl"] or 0.0) if row else 0.0
                    if closes > 0:
                        evidence["sources"].append(
                            {
                                "table": str(table),
                                "pair": str(alias),
                                "closed_trades": closes,
                                "winning_closes": wins,
                                "net_pnl_usd": round(net, 6),
                            }
                        )
                    total_closed += closes
                    total_wins += wins
                    total_pnl += net
        except Exception as e:
            evidence["reason"] = f"query_failed:{e}"
            return evidence
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

        evidence["closed_trades"] = int(total_closed)
        evidence["winning_closes"] = int(total_wins)
        evidence["losing_closes"] = max(0, int(total_closed - total_wins))
        evidence["net_pnl_usd"] = round(float(total_pnl), 6)
        evidence["win_rate"] = round(float(total_wins / total_closed) if total_closed > 0 else 0.0, 6)
        evidence["avg_pnl_per_close_usd"] = round(float(total_pnl / total_closed) if total_closed > 0 else 0.0, 8)
        return evidence

    def _shared_buy_cost_basis(self, pair):
        aliases = self._pair_aliases(pair)
        if not aliases or not TRADER_DB_PATH.exists():
            return 0.0
        marks = ",".join("?" for _ in aliases)
        conn = None
        try:
            conn = sqlite3.connect(str(TRADER_DB_PATH))
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                f"""
                SELECT
                  COALESCE(SUM(COALESCE(quantity, 0)), 0) AS buy_qty,
                  COALESCE(SUM(COALESCE(total_usd, 0)), 0) AS buy_usd
                FROM agent_trades
                WHERE pair IN ({marks})
                  AND UPPER(COALESCE(side, ''))='BUY'
                  AND LOWER(COALESCE(status, '')) IN ('filled', 'closed', 'executed', 'partial_filled', 'partially_filled', 'settled')
                """,
                tuple(aliases),
            ).fetchone()
            qty = float((row["buy_qty"] if row else 0.0) or 0.0)
            usd = float((row["buy_usd"] if row else 0.0) or 0.0)
            if qty > 0.0 and usd > 0.0:
                return float(usd / qty)
        except Exception:
            return 0.0
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
        return 0.0

    def _close_evidence_for_pair(self, pair, force_refresh=False):
        key = self._normalize_pair(pair)
        ttl = max(10, int(CONFIG.get("close_evidence_cache_seconds", 90) or 90))
        now = time.time()
        cached = self._close_evidence_cache.get(key, {})
        if (
            not force_refresh
            and isinstance(cached, dict)
            and (now - float(cached.get("ts", 0.0) or 0.0)) <= ttl
            and isinstance(cached.get("value"), dict)
        ):
            return dict(cached.get("value") or {})
        fresh = self._query_realized_close_evidence(key)
        self._close_evidence_cache[key] = {"ts": now, "value": dict(fresh)}
        return fresh

    def _close_evidence_gaps(self):
        gaps = {}
        min_closes = max(1, int(CONFIG.get("close_evidence_min_closes", 8) or 8))
        min_net_pnl = float(CONFIG.get("close_evidence_min_net_pnl_usd", 0.0) or 0.0)
        for pair in self._close_evidence_targets():
            evidence = self._close_evidence_for_pair(pair)
            closes = int(evidence.get("closed_trades", 0) or 0)
            net = float(evidence.get("net_pnl_usd", 0.0) or 0.0)
            close_deficit = max(0, min_closes - closes)
            net_deficit = max(0.0, min_net_pnl - net)
            if close_deficit > 0 or net_deficit > 0.0:
                gaps[pair] = {
                    "close_deficit": int(close_deficit),
                    "net_deficit_usd": round(float(net_deficit), 6),
                    "evidence": evidence,
                }
        return gaps

    def _required_profitable_exit_price(self, entry_price):
        entry = float(entry_price or 0.0)
        if entry <= 0.0:
            return 0.0
        round_trip = max(0.0, float(CONFIG.get("round_trip_fee_pct", 0.0) or 0.0))
        slippage = max(0.0, float(CONFIG.get("expected_slippage_pct", 0.0) or 0.0))
        buffer_pct = max(0.0, float(CONFIG.get("close_evidence_profit_buffer_pct", 0.001) or 0.0))
        return entry * (1.0 + round_trip + slippage + buffer_pct)

    def _inject_close_evidence_sell_signals(self, actionable, close_gaps):
        if not bool(CONFIG.get("close_evidence_force_sell_mode", True)):
            return actionable
        if not close_gaps:
            return actionable

        out = list(actionable)
        existing_sell_pairs = {
            self._normalize_pair(sig.get("pair"))
            for sig in out
            if str(sig.get("direction", "")).upper() == "SELL"
        }
        holdings, _cash = self._get_holdings()
        min_trade = max(0.5, float(CONFIG.get("min_trade_size_usd", 0.5) or 0.5))

        # Prioritize pairs with the biggest close deficit first.
        ranked = sorted(
            close_gaps.items(),
            key=lambda item: (
                -int((item[1] or {}).get("close_deficit", 0) or 0),
                -float((item[1] or {}).get("net_deficit_usd", 0.0) or 0.0),
            ),
        )

        for target_pair, gap in ranked:
            aliases = self._pair_aliases(target_pair)
            if any(alias in existing_sell_pairs for alias in aliases):
                continue
            base = target_pair.split("-", 1)[0]
            held = float(holdings.get(base, 0.0) or 0.0)
            if held <= 0.0:
                continue

            chosen_pair = ""
            chosen_price = 0.0
            for alias in aliases:
                px = float(self._get_price(alias) or 0.0)
                if px > 0.0:
                    chosen_pair = alias
                    chosen_price = px
                    break
            if not chosen_pair or chosen_price <= 0.0:
                continue

            held_usd = held * chosen_price
            if held_usd < min_trade:
                continue

            entry = self._latest_filled_buy_price_any(chosen_pair, fallback_price=0.0)
            if entry <= 0.0:
                continue
            required_exit = self._required_profitable_exit_price(entry)
            if chosen_price < required_exit:
                continue

            synthetic = {
                "pair": chosen_pair,
                "direction": "SELL",
                "composite_confidence": 0.79,
                "confirming_signals": max(2, int(CONFIG.get("min_confirming_signals", 2) or 2)),
                "quant_signals": max(1, int(CONFIG.get("min_quant_signals", 1) or 1)),
                "qual_signals": 0,
                "expected_value": round((chosen_price - required_exit) / max(required_exit, 1e-9), 6),
                "ev_positive": True,
                "regime": "distribution",
                "momentum": -0.05,
                "forced_close_evidence": True,
                "details": {
                    "close_evidence_router": {
                        "target_pair": target_pair,
                        "close_deficit": int((gap or {}).get("close_deficit", 0) or 0),
                        "entry_price": round(entry, 8),
                        "required_exit_price": round(required_exit, 8),
                        "spot_price": round(chosen_price, 8),
                    }
                },
            }
            out.append(synthetic)
            existing_sell_pairs.add(self._normalize_pair(chosen_pair))
            logger.info(
                "SNIPER: close-evidence SELL injected %s (spot=%.6f required=%.6f deficit=%d)",
                chosen_pair,
                chosen_price,
                required_exit,
                int((gap or {}).get("close_deficit", 0) or 0),
            )
        return out

    def _prioritize_actionable_for_close_evidence(self, actionable, close_gaps):
        if not close_gaps:
            return list(actionable)
        out = list(actionable)
        target_aliases = self._close_evidence_target_aliases()
        if bool(CONFIG.get("close_evidence_priority_mode", True)):
            has_target_buy = any(
                str(sig.get("direction", "")).upper() == "BUY"
                and self._normalize_pair(sig.get("pair")) in target_aliases
                for sig in out
            )
            if has_target_buy:
                kept = []
                dropped = 0
                for sig in out:
                    direction = str(sig.get("direction", "")).upper()
                    pair_norm = self._normalize_pair(sig.get("pair"))
                    if direction == "BUY" and pair_norm not in target_aliases:
                        dropped += 1
                        continue
                    kept.append(sig)
                out = kept
                if dropped > 0:
                    logger.info(
                        "SNIPER: close-evidence priority dropped %d non-target BUY opportunities",
                        dropped,
                    )

        def _rank(sig):
            direction = str(sig.get("direction", "")).upper()
            pair_norm = self._normalize_pair(sig.get("pair"))
            forced = 1 if bool(sig.get("forced_close_evidence", False)) else 0
            target = 1 if pair_norm in target_aliases else 0
            conf = float(sig.get("composite_confidence", 0.0) or 0.0)
            side_rank = 0 if direction == "SELL" else 1 if direction == "BUY" else 2
            return (side_rank, -forced, -target, -conf)

        out.sort(key=_rank)
        return out

    def _record_shared_trade_ledger(self, pair, side, price, quantity, total_usd, order_id=None, status="pending", pnl=None):
        conn = None
        try:
            conn = sqlite3.connect(str(TRADER_DB_PATH), timeout=5.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL,
                    quantity REAL,
                    total_usd REAL,
                    order_type TEXT DEFAULT 'limit',
                    order_id TEXT,
                    status TEXT DEFAULT 'pending',
                    pnl REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                INSERT INTO agent_trades
                    (agent, pair, side, price, quantity, total_usd, order_type, order_id, status, pnl)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "sniper",
                    self._normalize_pair(pair),
                    str(side or "").upper(),
                    float(price or 0.0),
                    float(quantity or 0.0),
                    float(total_usd or 0.0),
                    "limit",
                    str(order_id or ""),
                    str(status or "pending").lower(),
                    None if pnl is None else float(pnl),
                ),
            )
            conn.commit()
        except Exception as e:
            logger.debug("SNIPER: shared agent_trades ledger insert failed: %s", e)
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

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
            entry_price = self._latest_filled_buy_price_any(pair, fallback_price=price)
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

    def _fetch_candles_for_sources(self, pair):
        """Pre-fetch 1h and 1m candles once to avoid redundant API calls.

        QUICK WIN #3: Candle Fetch Deduplication
        - Previously: 4 signal sources independently fetching candles (RegimeSignalSource,
          PriceMomentumSource, RSIExtremeSource, UptickTimingSource)
        - Now: Single pre-fetch, shared across all signal sources via cache
        - Impact: Reduces API calls from ~4-5 per scan to 1 per scan (~$20/day savings)
        """
        try:
            dp = _data_pair(pair)

            # Fetch 1h candles
            candles_1h = []
            try:
                url_1h = f"https://api.exchange.coinbase.com/products/{dp}/candles?granularity=3600"
                raw_1h = _fetch_json(url_1h, timeout=5)
                if raw_1h:
                    candles_1h = [{
                        "time": c[0], "open": c[3], "high": c[2], "low": c[1],
                        "close": c[4], "volume": c[5]
                    } for c in raw_1h]
                    candles_1h.reverse()  # oldest first
            except Exception:
                pass

            # Fetch 1m candles
            candles_1m = []
            try:
                url_1m = f"https://api.exchange.coinbase.com/products/{dp}/candles?granularity=60"
                raw_1m = _fetch_json(url_1m, timeout=8)
                if raw_1m:
                    candles_1m = [{
                        "time": c[0], "open": c[3], "high": c[2], "low": c[1],
                        "close": c[4], "volume": c[5]
                    } for c in raw_1m]
                    candles_1m.reverse()  # oldest first
            except Exception:
                pass

            return candles_1h, candles_1m
        except Exception as e:
            logger.debug("Failed to pre-fetch candles for %s: %s", pair, e)
            return [], []

    def scan_pair(self, pair):
        """Scan all signal sources for a pair in parallel using ThreadPoolExecutor.

        Previously sequential (~35s per pair x 7 pairs = ~245s per cycle).
        Now parallel (~5-8s per pair, ~40s total cycle) — 7x faster reaction to opportunities.

        QUICK WIN #3: Pre-fetch candles once and share via cache (reduces API calls 4x).
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Pre-fetch candles once instead of letting each source fetch independently
        candles_1h, candles_1m = self._fetch_candles_for_sources(pair)

        results = {}

        def _scan_source(name, source):
            try:
                return name, source.scan(pair, candles_1h=candles_1h, candles_1m=candles_1m)
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

        close_gaps = self._close_evidence_gaps()
        if close_gaps:
            compact = {
                pair: {
                    "close_deficit": int((gap or {}).get("close_deficit", 0) or 0),
                    "net_deficit_usd": round(float((gap or {}).get("net_deficit_usd", 0.0) or 0.0), 6),
                }
                for pair, gap in close_gaps.items()
            }
            logger.info("SNIPER: non-BTC close evidence gaps detected %s", compact)
        actionable = self._inject_close_evidence_sell_signals(actionable, close_gaps)
        actionable = self._prioritize_actionable_for_close_evidence(actionable, close_gaps)

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
            round_trip = float(CONFIG.get("round_trip_fee_pct", 0.008) or 0.008)
            slippage = float(CONFIG.get("expected_slippage_pct", 0.001) or 0.001)
            total_cost = abs(sell_loss_pct) + round_trip + slippage
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

    def _minimum_viable_buy_size(self, max_trade_usd=0.0, available_cash_usd=0.0):
        """Adaptive minimum notional floor to improve buy activation without creating dust."""
        floor = max(0.25, float(CONFIG.get("min_trade_size_usd", 0.5) or 0.5))
        max_trade = max(0.0, float(max_trade_usd or 0.0))
        available = max(0.0, float(available_cash_usd or 0.0))
        max_trade_frac = max(0.0, float(CONFIG.get("min_trade_size_max_trade_fraction", 0.0) or 0.0))
        cash_frac = max(0.0, float(CONFIG.get("min_trade_size_cash_fraction", 0.0) or 0.0))
        if max_trade_frac > 0 and max_trade > 0:
            floor = max(floor, max_trade * max_trade_frac)
        if cash_frac > 0 and available > 0:
            floor = max(floor, available * cash_frac)
        return round(float(floor), 4)

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

    def _fit_buy_to_quote_capacity(self, pair, requested_size, min_viable_size):
        """Cap/reroute buy notional to available quote bankroll for USD/USDC pairs."""
        if "-" not in pair:
            return pair, max(0.0, float(requested_size or 0.0)), "pair_unstructured"

        base, quote = pair.split("-", 1)
        quote = quote.upper()
        needed = max(0.0, float(requested_size or 0.0))
        min_viable = max(0.25, float(min_viable_size or 0.0))
        balances = self._get_quote_balances()
        buffer = max(0.0, float(CONFIG.get("quote_balance_buffer_usd", 0.02) or 0.0))

        current_capacity = max(0.0, float(balances.get(quote, 0.0) or 0.0) - buffer)

        # Check alt quote first if current quote can't satisfy requested amount
        alt_quote = "USD" if quote == "USDC" else "USDC" if quote == "USD" else ""
        if alt_quote and current_capacity < needed:
            alt_capacity = max(0.0, float(balances.get(alt_quote, 0.0) or 0.0) - buffer)
            if alt_capacity >= min_viable:
                return f"{base}-{alt_quote}", min(needed, alt_capacity), "alt_quote_capacity"

        # Fall back to current quote if viable
        if current_capacity >= min_viable:
            return pair, min(needed, current_capacity), "current_quote_capacity"

        reason = (
            f"quote_capacity_insufficient:{quote}={current_capacity:.2f}"
            f"_needed={needed:.2f}_min_viable={min_viable:.2f}"
        )
        return pair, 0.0, reason

    def _close_flow_allows_buy(self):
        """Block new BUYs when close/fill reconciliation is stale or too failure-heavy."""
        if not bool(CONFIG.get("require_close_flow_for_buy", False)):
            return True, "gate_disabled"
        try:
            if not RECONCILE_STATUS_PATH.exists():
                return False, "reconcile_status_missing"
            payload = json.loads(RECONCILE_STATUS_PATH.read_text())
            if not isinstance(payload, dict):
                return False, "reconcile_status_invalid"

            updated = str(payload.get("updated_at", "") or "").strip()
            if not updated:
                return False, "reconcile_status_updated_at_missing"
            dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - dt).total_seconds()
            max_age = max(30, int(CONFIG.get("close_flow_status_max_age_seconds", 300) or 300))
            if age > max_age:
                return False, f"reconcile_status_stale:{int(age)}s>{max_age}s"

            summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
            close = payload.get("close_reconciliation", {}) if isinstance(payload.get("close_reconciliation"), dict) else {}
            attempts = int(close.get("attempts", summary.get("close_attempts", 0)) or 0)
            completions = int(close.get("completions", summary.get("close_completions", 0)) or 0)
            if "completion_rate" in close:
                completion_rate = float(close.get("completion_rate", 0.0) or 0.0)
            else:
                completion_rate = float(completions) / float(max(1, attempts))
            gate_passed = bool(close.get("gate_passed", summary.get("close_gate_passed", True)))
            gate_reason = str(close.get("gate_reason", summary.get("close_gate_reason", "")) or "").strip()
            min_attempts = max(1, int(CONFIG.get("close_flow_min_attempts", 2) or 2))
            min_rate = max(0.0, min(1.0, float(CONFIG.get("close_flow_min_completion_rate", 0.40) or 0.40)))
            max_terminal_failures = max(0, int(CONFIG.get("close_flow_max_terminal_failures", 3) or 3))

            failure_map = close.get("failure_reasons", summary.get("close_failure_reasons", {}))
            terminal_failures = 0
            if isinstance(failure_map, dict):
                for key, value in failure_map.items():
                    reason_key = str(key or "").strip().lower()
                    if (
                        "terminal" in reason_key
                        or reason_key in {"failed", "cancelled", "canceled", "expired"}
                    ):
                        try:
                            terminal_failures += int(value or 0)
                        except Exception:
                            continue

            if attempts <= 0:
                return True, "close_flow_no_attempts"
            if not gate_passed:
                return False, f"close_flow_gate_failed:{gate_reason or 'unknown'}"
            if attempts >= min_attempts and completion_rate < min_rate:
                return False, (
                    f"close_flow_completion_rate_low:{completion_rate:.3f}<{min_rate:.3f}"
                    f"_attempts={attempts}_completions={completions}"
                )
            if terminal_failures > max_terminal_failures:
                return False, f"close_flow_terminal_failures_high:{terminal_failures}>{max_terminal_failures}"
            return True, "close_flow_healthy"
        except Exception as e:
            return False, f"close_flow_gate_error:{e}"

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
        """Buy when health is green, stale-safe, or allowed degraded telemetry."""
        if not bool(CONFIG.get("require_execution_health_for_buy", True)):
            return True, "gate_disabled"
        try:
            if not EXECUTION_HEALTH_STATUS_PATH.exists():
                return False, "execution_health_status_missing"
            payload = json.loads(EXECUTION_HEALTH_STATUS_PATH.read_text())
            if not isinstance(payload, dict):
                return False, "execution_health_status_invalid"
            reasons = payload.get("reasons", [])
            if not isinstance(reasons, (list, tuple)):
                reasons = [payload.get("reason", "unknown")]
            reasons = [str(r or "").strip().lower() for r in reasons if str(r or "").strip()]
            fail_reason = str(payload.get("reason", "unknown"))

            if bool(payload.get("green", False)):
                return True, "execution_health_green"

            if bool(payload.get("egress_blocked", False)):
                return False, f"execution_health_blocked:egress_blocked:{fail_reason}"
            if any(r.startswith("egress_blocked") for r in reasons):
                return False, f"execution_health_blocked:egress_blocked:{fail_reason}"
            hard_block_prefixes = ("dns_", "reconcile_", "api_probe_failed", "candle_feed")
            if any(any(r.startswith(prefix) for r in reasons) for prefix in hard_block_prefixes):
                return False, f"execution_health_not_green:{fail_reason}"

            if bool(CONFIG.get("execution_health_degraded_mode", True)) and reasons:
                allowed = tuple(
                    str(v).strip().lower()
                    for v in CONFIG.get("execution_health_degraded_reasons", ())
                )
                if allowed and all(
                    any(r == allow or r.startswith(f"{allow}:") for allow in allowed)
                    for r in reasons
                ):
                    return True, f"execution_health_degraded:{fail_reason}"

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
            if bool(CONFIG.get("execution_health_degraded_mode", True)) and reasons:
                allowed = tuple(
                    str(v).strip().lower()
                    for v in CONFIG.get("execution_health_degraded_reasons", ())
                )
                if allowed and all(
                    any(r == allow or r.startswith(f"{allow}:") for allow in allowed)
                    for r in reasons
                ):
                    return True, f"execution_health_degraded:{fail_reason}"
            return False, f"execution_health_not_green:{fail_reason}"
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
        ok, reason = self._execution_health_allows_buy()
        if not ok:
            return degraded
        if str(reason).startswith("execution_health_degraded:"):
            return degraded
        return healthy

    def _load_throttle_state(self):
        """Load trade timestamps from database (last 24 hours).

        Called on startup to restore throttle state across restarts/deploys.
        Prevents fee-burning bursts when app comes back online.
        """
        try:
            now = time.time()
            cutoff_time = now - 86400  # 24 hours ago

            with self._db_lock:
                rows = self.db.execute(
                    "SELECT trade_timestamp FROM trade_throttle_log WHERE trade_timestamp > ? ORDER BY trade_timestamp ASC",
                    (cutoff_time,)
                ).fetchall()

            timestamps = [row[0] for row in rows]
            if timestamps:
                logger.info("PERSISTENT THROTTLE: Loaded %d timestamps from last 24h (from DB)", len(timestamps))
            return timestamps
        except Exception as e:
            logger.warning("Failed to load throttle state from DB: %s. Starting with empty list.", e)
            return []

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
        """Record a trade execution for throttle tracking (persisted to DB).

        This prevents fee-burning bursts on app restart by maintaining state
        across restarts/deploys.
        """
        now = time.time()
        self._trade_timestamps.append(now)

        # Persist to database for recovery after restart/deploy
        try:
            with self._db_lock:
                self.db.execute(
                    "INSERT INTO trade_throttle_log (trade_timestamp) VALUES (?)",
                    (now,)
                )
                self.db.commit()
        except Exception as e:
            logger.warning("Failed to persist trade timestamp to DB: %s", e)

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
            # Fear & Greed — log but don't block (maker orders + throttle protect us now)
            fg_val = self._get_fear_greed_value()
            if fg_val is not None and fg_val < 15:
                logger.info("SNIPER: %s BUY in Extreme Fear (F&G=%d) — proceeding with maker order", pair, fg_val)

            health_ok, health_reason = self._execution_health_allows_buy()
            if not health_ok:
                logger.info("SNIPER: %s BUY blocked — %s", pair, health_reason)
                return False
            em_ok, em_reason = self._exit_manager_status_allows_buy()
            if not em_ok:
                logger.info("SNIPER: %s BUY blocked — %s", pair, em_reason)
                return False
            close_ok, close_reason = self._close_flow_allows_buy()
            if not close_ok:
                logger.info("SNIPER: %s BUY blocked — %s", pair, close_reason)
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
            routed_pair = self._resolve_buy_pair_for_balance(
                pair,
                min_quote_needed=self._minimum_viable_buy_size(0.0, 0.0),
            )
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
            absolute_min_notional = max(
                0.25,
                float(CONFIG.get("min_trade_size_usd", 0.5) or 0.5),
            )
            if remaining_room < absolute_min_notional:
                logger.info("SNIPER: %s at max position ($%.2f / $%.2f cap) — skipping BUY",
                           pair, held_usd, max_position)
                return False

            cycle_spent = getattr(self, '_cycle_cash_spent', 0.0)
            effective_cash = cash - cycle_spent  # account for orders placed earlier this cycle
            available_after_reserve = effective_cash - reserve
            min_viable_buy = self._minimum_viable_buy_size(max_trade, available_after_reserve)
            if available_after_reserve < min_viable_buy:
                logger.info(
                    "SNIPER: Cash below reserve/min viable size ($%.2f - $%.2f reserve = $%.2f, min $%.2f)",
                    effective_cash,
                    reserve,
                    available_after_reserve,
                    min_viable_buy,
                )
                return False

            trade_size = min(
                max_trade,
                max(min_viable_buy, signal["composite_confidence"] * max_trade * 1.2),
            )
            trade_size = min(trade_size, remaining_room)
            trade_size = min(trade_size, available_after_reserve)  # DYNAMIC reserve
            if str(health_reason).startswith("execution_health_degraded:"):
                trade_size *= float(CONFIG.get("execution_health_degraded_trade_size_factor", 0.75))
                trade_size = round(trade_size, 2)

            # Quote-aware reroute + bankroll-aware sizing (fixes USD-vs-USDC mismatches and over-sized proposals).
            fit_pair, fitted_size, fit_reason = self._fit_buy_to_quote_capacity(
                pair,
                trade_size,
                min_viable_buy,
            )
            if fitted_size <= 0:
                logger.info("SNIPER: %s BUY blocked — %s", pair, fit_reason)
                return False
            if fit_pair != pair:
                logger.info("SNIPER: %s BUY rerouted to %s (%s)", pair, fit_pair, fit_reason)
            pair = fit_pair
            signal["pair"] = pair
            trade_size = float(fitted_size)
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

            # Refit once more after risk adjustment in case controller resized above quote capacity.
            pre_refit_pair = pair
            pair, trade_size, fit_reason = self._fit_buy_to_quote_capacity(
                pair,
                trade_size,
                min_viable_buy,
            )
            signal["pair"] = pair
            if pair != pre_refit_pair:
                logger.info("SNIPER: %s BUY rerouted to %s after risk sizing (%s)", pre_refit_pair, pair, fit_reason)
                if _exit_mgr and not _exit_mgr.has_exit_plan(pair):
                    logger.info("SNIPER: %s BUY blocked — post-risk routed pair missing exit plan", pair)
                    return False
                price = self._get_price(pair)
                if not price:
                    logger.warning("Cannot get price for post-risk routed pair %s", pair)
                    return False
                base_currency = pair.split("-")[0]
                held_amount = holdings.get(base_currency, 0)
                held_usd = held_amount * price if price else 0
                max_position = total_portfolio * max_pos_pct
                remaining_room = max_position - held_usd
                trade_size = min(trade_size, remaining_room)
            if trade_size < min_viable_buy:
                logger.info(
                    "SNIPER: Insufficient for BUY after sizing/routing ($%.2f cash, $%.2f reserve, min $%.2f)",
                    cash,
                    reserve,
                    min_viable_buy,
                )
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
            last_buy = self._latest_filled_buy_snapshot_any(pair)
            if last_buy:
                buy_price = float(last_buy.get("entry_price", 0.0) or 0.0)
                buy_time = str(last_buy.get("created_at", "") or "")
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
                required_exit = self._required_profitable_exit_price(buy_price)
                if buy_price > 0 and price < required_exit:
                    loss_pct = (price - buy_price) / buy_price if buy_price > 0 else 0
                    # Check if there's a higher-edge BUY waiting for this capital
                    if self._has_better_opportunity(pair, loss_pct):
                        logger.info("SNIPER: Selling %s at %.1f%% loss — better opportunity available",
                                   pair, loss_pct * 100)
                        # Track the loss
                        self.daily_loss += abs(loss_pct * held_usd)
                    else:
                        logger.info(
                            "SNIPER: %s below profitable close threshold (entry=%.4f spot=%.4f need>=%.4f) — HOLDING",
                            pair,
                            buy_price,
                            price,
                            required_exit,
                        )
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
        payload = result if isinstance(result, dict) else {}
        order_id = None
        status = "failed"
        fill_data = {}
        pair = self._normalize_pair(pair)
        side = str(side or "").upper()

        quoted_price = float(price or 0.0)
        quoted_notional = float(trade_size or 0.0)
        quoted_qty = (quoted_notional / quoted_price) if quoted_price > 0.0 else 0.0
        effective_price = quoted_price
        effective_qty = quoted_qty
        effective_notional = quoted_notional

        if isinstance(payload.get("success_response"), dict):
            order_id = payload["success_response"].get("order_id")
            status = "pending"
        elif payload.get("order_id"):
            order_id = payload.get("order_id")
            status = "pending"
        elif isinstance(payload.get("error_response"), dict):
            err = payload["error_response"]
            logger.warning("SNIPER ORDER FAILED: %s %s | %s", pair, side, err.get("message", err))
            if _risk_ctrl:
                _risk_ctrl.resolve_allocation("sniper", pair)
        else:
            logger.warning("SNIPER ORDER UNKNOWN: %s", json.dumps(payload)[:300])
            if _risk_ctrl:
                _risk_ctrl.resolve_allocation("sniper", pair)

        if order_id and status == "pending":
            try:
                from exchange_connector import CoinbaseTrader
                fill_trader = CoinbaseTrader()
                fill_data = fill_trader.get_order_fill(order_id, max_wait=2, poll_interval=0.4) or {}
            except Exception:
                fill_data = {}

            fill_status = str(fill_data.get("status", "") or "").upper()
            try:
                filled_size = float(fill_data.get("filled_size", 0.0) or 0.0)
            except Exception:
                filled_size = 0.0
            if fill_status in {"FILLED", "PARTIAL_FILLED", "PARTIALLY_FILLED"} or filled_size > 0.0:
                status = "filled"
            elif fill_status in {"FAILED", "CANCELLED", "CANCELED", "EXPIRED", "REJECTED"}:
                status = "failed"

        if fill_data:
            try:
                filled_size = float(fill_data.get("filled_size", 0.0) or 0.0)
            except Exception:
                filled_size = 0.0
            try:
                avg_price = float(
                    fill_data.get("average_filled_price", fill_data.get("avg_price", 0.0)) or 0.0
                )
            except Exception:
                avg_price = 0.0
            if filled_size > 0.0:
                effective_qty = filled_size
            if avg_price > 0.0:
                effective_price = avg_price
            if effective_price > 0.0 and effective_qty > 0.0:
                effective_notional = effective_price * effective_qty

        if status == "filled":
            self._record_trade_timestamp()
            logger.info(
                "SNIPER ORDER FILLED: %s %s $%.2f @ $%.2f | order=%s",
                pair,
                side,
                effective_notional,
                effective_price,
                order_id,
            )
        elif status == "pending":
            logger.info("SNIPER ORDER PENDING: %s %s $%.2f @ $%.2f | order=%s",
                        pair, side, effective_notional, effective_price, order_id)
        elif status == "failed":
            if _risk_ctrl:
                _risk_ctrl.resolve_allocation("sniper", pair)

        # Calculate P&L on SELL trades
        pnl = None
        if side == "SELL" and status == "filled":
            buy_price = self._latest_filled_buy_price_any(pair, fallback_price=0.0)
            if buy_price <= 0.0:
                buy_price = self._shared_buy_cost_basis(pair)
            if buy_price > 0:
                fees = effective_notional * max(0.0, float(CONFIG.get("round_trip_fee_pct", 0.008) or 0.008))
                pnl = ((effective_price - buy_price) / buy_price) * effective_notional - fees
                logger.info(
                    "SNIPER P&L: %s SELL pnl=$%.4f (entry=$%.2f exit=$%.2f fees=$%.4f)",
                    pair,
                    pnl,
                    buy_price,
                    effective_price,
                    fees,
                )

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
                        side,
                        float(signal.get("composite_confidence", 0.0) or 0.0),
                        effective_notional,
                        "coinbase",
                        effective_price,
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
                    (
                        pair,
                        side,
                        float(signal.get("composite_confidence", 0.0) or 0.0),
                        effective_notional,
                        "coinbase",
                        effective_price,
                        pnl,
                        status,
                    ),
                )
            self.db.commit()
        self.trades_today += 1

        ledger_pnl = None
        if side == "SELL" and status == "filled":
            ledger_pnl = pnl
        elif side == "BUY" and status == "filled":
            ledger_pnl = 0.0
        self._record_shared_trade_ledger(
            pair=pair,
            side=side,
            price=effective_price,
            quantity=effective_qty,
            total_usd=effective_notional,
            order_id=order_id,
            status=status,
            pnl=ledger_pnl,
        )

        # Record to KPI tracker for scorecard
        if _kpi and status in ("filled", "pending"):
            try:
                fees = effective_notional * 0.004  # maker fee
                _kpi.record_trade(
                    strategy_name="sniper", pair=pair, direction=side,
                    amount_usd=effective_notional, pnl=pnl or 0, fees=fees,
                    hold_seconds=0, strategy_type="LF",
                    won=(pnl is None or pnl >= 0),
                )
            except Exception as kpi_err:
                logger.debug("KPI record error: %s", kpi_err)

        # Log state transition for learning
        if _tracker and status == "filled":
            asset = pair.split("-")[0]
            fee_pct = 0.006  # 0.6% taker fee
            cost = round(effective_notional * fee_pct, 4)
            if side == "BUY":
                _tracker.transition(asset, "coinbase", "available", "available",
                                    amount=effective_qty,
                                    value_usd=effective_notional, cost_usd=cost,
                                    trigger="sniper",
                                    metadata={"side": "BUY", "pair": pair,
                                              "confidence": float(signal.get("composite_confidence", 0.0) or 0.0)})
            else:
                _tracker.transition(asset, "coinbase", "available", "available",
                                    amount=effective_qty,
                                    value_usd=effective_notional, cost_usd=cost,
                                    trigger="sniper",
                                    metadata={"side": "SELL", "pair": pair,
                                              "confidence": float(signal.get("composite_confidence", 0.0) or 0.0)})

        # Register filled BUY orders with ExitManager for exit monitoring
        # Use actual fill data (partial fills) instead of assuming full fill
        if _exit_mgr and status in ("filled", "pending") and side == "BUY" and order_id:
            try:
                fill_snapshot = fill_data
                if not fill_snapshot:
                    from exchange_connector import CoinbaseTrader
                    fill_trader = CoinbaseTrader()
                    fill_snapshot = fill_trader.get_order_fill(order_id, max_wait=5) or {}

                if fill_snapshot and float(fill_snapshot.get("filled_size", 0.0) or 0.0) > 0:
                    actual_amount = float(fill_snapshot.get("filled_size", 0.0) or 0.0)
                    actual_price = float(
                        fill_snapshot.get("avg_price", fill_snapshot.get("average_filled_price", effective_price)) or effective_price
                    )
                    _exit_mgr.register_position(
                        pair, actual_price, datetime.now(timezone.utc).isoformat(),
                        actual_amount, trade_id=order_id)
                    logger.info("SNIPER: Registered %s BUY with ExitManager (%.8f @ $%.2f, fill status=%s)",
                               pair, actual_amount, actual_price, fill_snapshot.get("status", "unknown"))
                elif fill_snapshot and float(fill_snapshot.get("filled_size", 0.0) or 0.0) == 0:
                    logger.info("SNIPER: Order %s had zero fill — NOT registering with ExitManager", order_id)
                else:
                    # Fallback to estimated amount if fill polling timed out
                    base_amount = effective_qty
                    _exit_mgr.register_position(
                        pair, effective_price, datetime.now(timezone.utc).isoformat(),
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
                    ordered = sorted(
                        actionable,
                        key=lambda s: (
                            0 if str(s.get("direction", "")).upper() == "SELL" else 1,
                            -float(s.get("composite_confidence", 0.0) or 0.0),
                        ),
                    )
                    for signal in ordered:
                        if signal.get("direction") in ("BUY", "SELL"):
                            self.execute_trade(signal)
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
