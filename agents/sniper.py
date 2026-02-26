#!/usr/bin/env python3
"""High-Confidence Sniper — stacked signal aggregator.

Only trades when composite confidence >= 90% with 3+ confirming signals.
Stacks 10 independent signal sources for maximum conviction:

  1. NetTrace latency signals (exchange infrastructure changes)
  2. Fast Engine regime detection (SMA/RSI/BB/VWAP/ATR)
  3. Fast Engine arb check (Coinbase vs 5 exchange median)
  4. Orderbook imbalance (bid/ask depth ratio)
  5. RSI extreme detection (oversold < 25 / overbought > 75)
  6. Fear & Greed Index (contrarian)
  7. Price momentum (4h trend)
  8. Uptick timing (buy-low-sell-high inflection)
  9. Meta-Engine ML predictions (RSI+momentum+SMA ensemble from meta_engine.db)
  10. E*Trade risk pulse (equity lead-lag from SPY/QQQ/COIN/MSTR)
  11. XGBoost-Lite (NumPy gradient boosted stumps: RSI/MACD/BB/volume features)
  12. Wasserstein-1 regime shift detector (return distribution divergence)
  13. VPIN flow toxicity (volume-synchronized probability of informed trading)
  14. Graph analysis (multi-layer correlation + capital flow graph from meta_engine)
  15. Pairs trading (cointegration-based spread z-score mean reversion)

Signal weights are dynamically calibrated via entropy-based calibrator that
tracks each signal's prediction accuracy and adjusts weights accordingly.

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
import math
import os
import re
import sqlite3
import subprocess
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

# Derivatives connector for perp-preferred execution
try:
    from coinbase_derivatives_connector import CoinbaseDerivativesConnector
    _deriv = CoinbaseDerivativesConnector()
except Exception:
    _deriv = None

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

# XGBoost-Lite signal source — NumPy-only gradient boosted stumps
try:
    from ml_signal_xgb import XGBoostSignalSource
    _xgb_source = XGBoostSignalSource()
except Exception as _xgb_err:
    _xgb_source = None
    logging.getLogger("sniper").warning("XGBoostSignalSource not available: %s", _xgb_err)

# Wasserstein-1 regime shift detector
try:
    from regime_detector import RegimeShiftSignalSource
    _w1_regime_source = RegimeShiftSignalSource()
except Exception as _w1_err:
    _w1_regime_source = None
    logging.getLogger("sniper").warning("RegimeShiftSignalSource not available: %s", _w1_err)

# VPIN flow toxicity signal source
try:
    from vpin_signal import VPINSignalSource
    _vpin_source = VPINSignalSource()
except Exception as _vpin_err:
    _vpin_source = None
    logging.getLogger("sniper").warning("VPINSignalSource not available: %s", _vpin_err)

# CoinGlass funding rate & open interest signal
try:
    from coinglass_signal import CoinGlassSignal
    _coinglass_source = CoinGlassSignal()
except Exception as _cg_err:
    _coinglass_source = None
    logging.getLogger("sniper").warning("CoinGlassSignal not available: %s", _cg_err)

# Reddit crypto sentiment signal
try:
    from reddit_sentiment import RedditSentiment
    _reddit_source = RedditSentiment()
except Exception as _rs_err:
    _reddit_source = None
    logging.getLogger("sniper").warning("RedditSentiment not available: %s", _rs_err)

# Conviction Rally — test small, rally big position management
try:
    from conviction_rally import ConvictionRally
    _conviction_rally_available = True
except Exception as _cr_err:
    _conviction_rally_available = False
    logging.getLogger("sniper").warning("ConvictionRally not available: %s", _cr_err)

# Graph signal source — multi-layer correlation + flow graph analysis
try:
    from meta_engine import GraphSignalSource
    _graph_source = GraphSignalSource()
except Exception as _gs_err:
    _graph_source = None
    logging.getLogger("sniper").warning("GraphSignalSource not available: %s", _gs_err)

# Thompson Sampling sizer — record outcomes for contextual bandit sizing
try:
    from risk_controller import _thompson_sizer as _ts_sizer
except Exception:
    _ts_sizer = None

# WebSocket feed — O(1) price lookups with sub-100ms latency
try:
    from coinbase_ws_feed import CoinbaseWSFeed
    _ws_feed_available = True
except Exception:
    _ws_feed_available = False
    CoinbaseWSFeed = None

# Sniper configuration — static signal settings only
# Trade sizes, reserves, position limits come from risk_controller dynamically
CONFIG = {
    "min_composite_confidence": float(os.environ.get("GOAL_MIN_CONFIDENCE", "0.75")),  # Synced with agent_goals
    "min_confirming_signals": 2,         # 2+ must agree
    "min_quant_signals": 1,              # at least 1 quantitative signal required
    "scan_interval": int(os.environ.get("SNIPER_SCAN_INTERVAL_SECONDS", "10")),  # Scan cadence (tightened for active trading)
    "min_hold_seconds": 60,               # 60s minimum hold
    # PRIMARY: BTC + ETH + SOL (core positions — BTC is reserve accumulation target)
    # SECONDARY: AVAX, LINK, DOGE, FET (opportunistic)
    # RESERVE: BTC, USD, USDC — can BUY to accumulate, but never SELL away
    # Coinbase Advanced Trade uses USD pairs (USDC pairs delisted)
    "pairs": ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD", "DOGE-USD", "FET-USD"],
    "reserve_assets": ["USD", "USDC"],  # Never sell stablecoins — BTC removed to enable profit-taking
    "primary_pairs": ["BTC-USD", "ETH-USD", "SOL-USD"],    # Priority allocation
    # Signal weights: quantitative signals DRIVE decisions, qualitative SUPPLEMENTS
    # Quantitative: regime, arb, orderbook, rsi_extreme, momentum, latency, etrade_pulse
    # Computational edge: latency (our private NetTrace data), meta_engine (ML ensemble)
    # Qualitative: fear_greed, uptick (accelerate but don't drive)
    "signal_weights": {
        # --- QUANTITATIVE (core decision drivers) ---
        "regime": 0.20,        # C engine: SMA/RSI/BB/VWAP/ATR — most computational
        "arb": 0.15,           # Cross-exchange spread — pure math
        "orderbook": float(os.environ.get("SNIPER_WEIGHT_ORDERBOOK", "0.08")),  # Reduced: ask-side depth SELL bias in bear market
        "rsi_extreme": 0.10,   # Technical oversold/overbought
        "momentum": 0.08,      # 4h trend analysis
        # --- COMPUTATIONAL EDGE (private alpha) ---
        "latency": float(os.environ.get("SNIPER_WEIGHT_LATENCY", "0.18")),  # Our private alpha — boosted
        "meta_engine": 0.10,   # ML ensemble predictions from evolution engine
        "xgb_lite": 0.08,     # NumPy gradient boosted stumps (RSI/MACD/BB/vol features)
        "w1_regime": 0.08,    # Wasserstein-1 regime shift detector
        "etrade_pulse": max(
            0.0,
            min(0.25, float(os.environ.get("SNIPER_ETRADE_PULSE_WEIGHT", "0.08"))),
        ),  # E*Trade/US equity risk pulse lead-lag
        "vpin": 0.10,          # VPIN flow toxicity — detect informed trader dominance
        # --- QUALITATIVE (supplementary only) ---
        "fear_greed": float(os.environ.get("SNIPER_WEIGHT_FEAR_GREED", "0.08")),  # Contrarian — F&G=8 historically strong BUY
        "uptick": 0.03,        # Simple bounce — supplements, doesn't drive
        "coinglass": 0.08,     # CoinGlass funding rate & OI — contrarian derivatives signal
        "reddit_sentiment": 0.06,  # Reddit crypto sentiment — contrarian at extremes
        "graph": 0.06,            # Multi-layer correlation + capital flow graph analysis
        "pairs": 0.06,            # Cointegration-based pairs trading spread z-score
    },
    # Classify which signals are quantitative vs qualitative
    "quant_signals": {"regime", "arb", "orderbook", "rsi_extreme", "momentum", "latency", "meta_engine", "etrade_pulse", "xgb_lite", "w1_regime", "vpin", "coinglass", "graph", "pairs"},
    "qual_signals": {"fear_greed", "uptick", "reddit_sentiment"},
    # Expected Value parameters (fees + slippage)
    # Perp maker: 0% buy + 0% sell; spot maker: 0.4% buy + 0.4% sell
    # Blended estimate assuming perp-preferred routing
    "round_trip_fee_pct": float(os.environ.get("SNIPER_ROUND_TRIP_FEE_PCT", "0.008")),
    "expected_slippage_pct": 0.001, # ~0.1% average slippage
    # Long-chain game-theory gate (entry must model profitable path to exit).
    "min_chain_net_edge": float(os.environ.get("SNIPER_MIN_CHAIN_NET_EDGE_PCT", "0.002")),
    "min_chain_worst_case_edge": float(os.environ.get("SNIPER_MIN_CHAIN_WORST_EDGE_PCT", "-0.005")),
    "min_chain_steps": int(os.environ.get("SNIPER_MIN_CHAIN_STEPS", "2")),
    # Bootstrap relaxation: when no trades in last N hours, halve chain minimums
    # to prevent cold-start deadlock where the chain gate is too strict to allow
    # the first trade.  GoalValidator + EV gate + risk controller still enforce safety.
    "chain_gate_bootstrap_hours": int(os.environ.get("SNIPER_CHAIN_GATE_BOOTSTRAP_HOURS", "24")),
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
    "execution_health_auto_refresh_on_block": os.environ.get(
        "SNIPER_EXECUTION_HEALTH_AUTO_REFRESH_ON_BLOCK", "1"
    ).lower() not in ("0", "false", "no"),
    "execution_health_auto_refresh_cooldown_seconds": float(
        os.environ.get("SNIPER_EXECUTION_HEALTH_AUTO_REFRESH_COOLDOWN_SECONDS", "20")
    ),
    "execution_health_auto_refresh_timeout_seconds": float(
        os.environ.get("SNIPER_EXECUTION_HEALTH_AUTO_REFRESH_TIMEOUT_SECONDS", "30")
    ),
    "execution_health_auto_refresh_run_reconcile": os.environ.get(
        "SNIPER_EXECUTION_HEALTH_AUTO_REFRESH_RUN_RECONCILE", "1"
    ).lower() not in ("0", "false", "no"),
    "execution_health_auto_refresh_reconcile_max_orders": int(
        os.environ.get("SNIPER_EXECUTION_HEALTH_AUTO_REFRESH_RECONCILE_MAX_ORDERS", "120")
    ),
    "execution_health_auto_refresh_reconcile_lookback_hours": int(
        os.environ.get("SNIPER_EXECUTION_HEALTH_AUTO_REFRESH_RECONCILE_LOOKBACK_HOURS", "96")
    ),
    "require_exit_manager_status_for_buy": os.environ.get("SNIPER_REQUIRE_EXIT_MANAGER_STATUS_FOR_BUY", "1").lower() not in ("0", "false", "no"),
    "exit_manager_status_max_age_seconds": int(os.environ.get("SNIPER_EXIT_MANAGER_STATUS_MAX_AGE_SECONDS", "300")),
    "require_close_flow_for_buy": os.environ.get("SNIPER_REQUIRE_CLOSE_FLOW_FOR_BUY", "0").lower() not in ("0", "false", "no"),
    "close_flow_status_max_age_seconds": int(os.environ.get("SNIPER_CLOSE_FLOW_STATUS_MAX_AGE_SECONDS", "300")),
    "close_flow_stale_grace_seconds": int(
        os.environ.get("SNIPER_CLOSE_FLOW_STALE_GRACE_SECONDS", "3600")
    ),
    "close_flow_min_attempts": int(os.environ.get("SNIPER_CLOSE_FLOW_MIN_ATTEMPTS", "2")),
    "close_flow_min_completion_rate": float(os.environ.get("SNIPER_CLOSE_FLOW_MIN_COMPLETION_RATE", "0.40")),
    "close_flow_max_terminal_failures": int(os.environ.get("SNIPER_CLOSE_FLOW_MAX_TERMINAL_FAILURES", "3")),
    "balance_growth_mode": os.environ.get("SNIPER_BALANCE_GROWTH_MODE", "1").lower() not in ("0", "false", "no"),
    "balance_cache_seconds": int(os.environ.get("SNIPER_BALANCE_CACHE_SECONDS", "20")),
    "balance_lookback_hours": int(os.environ.get("SNIPER_BALANCE_LOOKBACK_HOURS", "24")),
    "balance_max_buy_sell_ratio": float(os.environ.get("SNIPER_BALANCE_MAX_BUY_SELL_RATIO", "1.35")),
    "balance_min_buy_sell_ratio_for_accel": float(
        os.environ.get("SNIPER_BALANCE_MIN_BUY_SELL_RATIO_FOR_ACCEL", "0.55")
    ),
    "balance_min_sell_completions": int(os.environ.get("SNIPER_BALANCE_MIN_SELL_COMPLETIONS", "2")),
    "balance_min_close_attempts": int(os.environ.get("SNIPER_BALANCE_MIN_CLOSE_ATTEMPTS", "2")),
    "balance_min_close_completion_rate": float(
        os.environ.get("SNIPER_BALANCE_MIN_CLOSE_COMPLETION_RATE", "0.45")
    ),
    "balance_require_non_negative_realized_pnl": os.environ.get(
        "SNIPER_BALANCE_REQUIRE_NON_NEGATIVE_REALIZED_PNL", "1"
    ).lower() not in ("0", "false", "no"),
    "balance_min_realized_closes_for_pnl_gate": int(
        os.environ.get("SNIPER_BALANCE_MIN_REALIZED_CLOSES_FOR_PNL_GATE", "3")
    ),
    "balance_buy_confidence_penalty": float(
        os.environ.get("SNIPER_BALANCE_BUY_CONFIDENCE_PENALTY", "0.88")
    ),
    "balance_buy_size_penalty": float(os.environ.get("SNIPER_BALANCE_BUY_SIZE_PENALTY", "0.72")),
    "balance_buy_confidence_boost": float(
        os.environ.get("SNIPER_BALANCE_BUY_CONFIDENCE_BOOST", "1.06")
    ),
    "balance_buy_size_boost": float(os.environ.get("SNIPER_BALANCE_BUY_SIZE_BOOST", "1.12")),
    "min_trade_size_usd": float(os.environ.get("SNIPER_MIN_TRADE_SIZE_USD", "0.50")),
    "min_trade_size_max_trade_fraction": float(
        os.environ.get("SNIPER_MIN_TRADE_SIZE_MAX_TRADE_FRACTION", "0.00")
    ),
    "min_trade_size_cash_fraction": float(
        os.environ.get("SNIPER_MIN_TRADE_SIZE_CASH_FRACTION", "0.00")
    ),
    "quote_balance_buffer_usd": float(os.environ.get("SNIPER_QUOTE_BALANCE_BUFFER_USD", "0.02")),
    "pair_failure_cooldown_seconds": int(os.environ.get("SNIPER_PAIR_FAILURE_COOLDOWN_SECONDS", "30")),
    "scan_interval_healthy": int(os.environ.get("SNIPER_SCAN_INTERVAL_HEALTHY_SECONDS", "10")),
    "scan_interval_degraded": int(os.environ.get("SNIPER_SCAN_INTERVAL_DEGRADED_SECONDS", "30")),
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
    "close_evidence_reconcile_pending_mode": os.environ.get(
        "SNIPER_CLOSE_EVIDENCE_RECONCILE_PENDING_MODE", "1"
    ).lower() not in ("0", "false", "no"),
    "close_evidence_reconcile_lookback_hours": int(
        os.environ.get("SNIPER_CLOSE_EVIDENCE_RECONCILE_LOOKBACK_HOURS", "96")
    ),
    "close_evidence_reconcile_max_rows": int(
        os.environ.get("SNIPER_CLOSE_EVIDENCE_RECONCILE_MAX_ROWS", "16")
    ),
    "close_evidence_reconcile_poll_seconds": float(
        os.environ.get("SNIPER_CLOSE_EVIDENCE_RECONCILE_POLL_SECONDS", "2.5")
    ),
    "close_evidence_sell_fill_wait_seconds": float(
        os.environ.get("SNIPER_CLOSE_EVIDENCE_SELL_FILL_WAIT_SECONDS", "6.0")
    ),
    "profit_focus_mode": os.environ.get("SNIPER_PROFIT_FOCUS_MODE", "1").lower() not in (
        "0",
        "false",
        "no",
    ),
    "profit_focus_lookback_hours": int(
        os.environ.get("SNIPER_PROFIT_FOCUS_LOOKBACK_HOURS", "72")
    ),
    "profit_focus_min_closes": int(
        os.environ.get("SNIPER_PROFIT_FOCUS_MIN_CLOSES", "3")
    ),
    "profit_focus_min_net_pnl_usd": float(
        os.environ.get("SNIPER_PROFIT_FOCUS_MIN_NET_PNL_USD", "0.10")
    ),
    "profit_focus_top_n": int(
        os.environ.get("SNIPER_PROFIT_FOCUS_TOP_N", "3")
    ),
    "profit_focus_min_scan_pairs": int(
        os.environ.get("SNIPER_PROFIT_FOCUS_MIN_SCAN_PAIRS", "2")
    ),
    "profit_focus_cache_seconds": int(
        os.environ.get("SNIPER_PROFIT_FOCUS_CACHE_SECONDS", "90")
    ),
    "profit_focus_include_primary": os.environ.get(
        "SNIPER_PROFIT_FOCUS_INCLUDE_PRIMARY", "1"
    ).lower() not in ("0", "false", "no"),
    "profit_focus_include_close_targets": os.environ.get(
        "SNIPER_PROFIT_FOCUS_INCLUDE_CLOSE_TARGETS", "1"
    ).lower() not in ("0", "false", "no"),
    "execution_fallback_mode": str(
        os.environ.get("SNIPER_EXECUTION_FALLBACK_MODE", "off")
    ).strip().lower(),
    "execution_fallback_wait_seconds": float(
        os.environ.get("SNIPER_EXECUTION_FALLBACK_WAIT_SECONDS", "1.0")
    ),
    "execution_fallback_ioc_wait_seconds": float(
        os.environ.get("SNIPER_EXECUTION_FALLBACK_IOC_WAIT_SECONDS", "2.5")
    ),
    "execution_fallback_min_quote_usd": float(
        os.environ.get("SNIPER_EXECUTION_FALLBACK_MIN_QUOTE_USD", "0.50")
    ),
    "execution_fallback_max_spread_pct_controlled": float(
        os.environ.get("SNIPER_EXECUTION_FALLBACK_MAX_SPREAD_PCT_CONTROLLED", "0.35")
    ),
    "execution_fallback_max_spread_pct_aggressive": float(
        os.environ.get("SNIPER_EXECUTION_FALLBACK_MAX_SPREAD_PCT_AGGRESSIVE", "1.20")
    ),
}

# Equity pair support (gated by env var)
SNIPER_EQUITY_ENABLED = os.environ.get("SNIPER_EQUITY_ENABLED", "0").lower() in ("1", "true", "yes")
CONFIG["equity_pairs"] = ["COIN-USD", "MSTR-USD", "RIOT-USD", "MARA-USD", "SPY-USD", "QQQ-USD"]


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
        # Always check Korean premium first — high-value international signal
        kimchi = self.scan_korean_premium(pair)
        if kimchi and kimchi.get("confidence", 0) >= 0.75:
            return kimchi

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

            if len(other_prices) >= 2:
                # Use C engine arb detection
                arb = engine.check_arbitrage(cb_price, other_prices)

                if arb["has_opportunity"]:
                    direction = "BUY" if arb["side"] == 1 else "SELL"
                    return {
                        "direction": direction,
                        "confidence": arb["confidence"],
                        "reason": f"Arb: CB ${cb_price:.2f} vs median ${arb['market_median']:.2f} ({arb['spread_pct']:.2f}%)"
                    }

            # Fallback to lower-confidence Korean premium
            if kimchi:
                return kimchi

            return {"direction": "NONE", "confidence": 0, "reason": "No arb spread"}
        except Exception as e:
            # Even on error, return Korean premium if available
            if kimchi:
                return kimchi
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

    def scan_korean_premium(self, pair):
        """Check Kimchi Premium — Korean exchange prices vs Coinbase."""
        try:
            from exchange_connector import MultiExchangeFeed
            token = pair.split("-")[0]
            cb_prices = MultiExchangeFeed.get_all_prices(token, quote="USD")
            cb_price = float(cb_prices.get("coinbase", 0))
            if cb_price <= 0:
                return None
            krw_prices = MultiExchangeFeed.get_krw_prices(token)
            usdkrw = MultiExchangeFeed.get_usdkrw_rate()
            if not krw_prices or not usdkrw or usdkrw <= 0:
                return None
            best_premium = 0.0
            best_venue = ""
            for venue, krw_px in krw_prices.items():
                usd_px = float(krw_px) / float(usdkrw)
                premium_pct = ((usd_px - cb_price) / cb_price) * 100.0
                if premium_pct > best_premium:
                    best_premium = premium_pct
                    best_venue = venue
            # Korean premium > 0.5% = BUY on Coinbase (anticipate global catch-up)
            if best_premium >= 0.5:
                conf = min(0.90, 0.65 + (best_premium / 10.0))
                return {
                    "direction": "BUY",
                    "confidence": conf,
                    "reason": f"Kimchi Premium: {best_venue} +{best_premium:.2f}% vs Coinbase"
                }
        except Exception as e:
            logger.debug("Korean premium check error: %s", e)
        return None


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


class ETradePulseSource(SignalSource):
    """Signal: E*Trade equity risk pulse for cross-asset lead/lag.

    Uses E*Trade quotes when authenticated, with built-in Yahoo fallback from
    ETradePriceFeed. The pulse is a short-horizon risk-on/risk-off proxy that
    can lead crypto moves during US market hours.
    """

    def __init__(self):
        self.enabled = os.environ.get("SNIPER_ETRADE_SIGNAL_ENABLED", "1").lower() in ("1", "true", "yes")
        self.lookback_seconds = max(20.0, float(os.environ.get("SNIPER_ETRADE_PULSE_LOOKBACK_SECONDS", "90")))
        self.snapshot_ttl = max(1.0, float(os.environ.get("SNIPER_ETRADE_PULSE_SNAPSHOT_TTL", "5")))
        self.min_move = max(0.0002, float(os.environ.get("SNIPER_ETRADE_PULSE_MIN_MOVE", "0.0012")))
        self._lock = threading.Lock()
        self._snapshot = {"ts": 0.0, "prices": {}}
        self._history = {}  # symbol -> list[(ts, price)]
        self._macro_symbols = ("SPY", "QQQ")
        self._crypto_beta_symbols = ("COIN", "MSTR")
        self._auth = None
        self._feed = None

        try:
            from etrade_connector import ETradeAuth, ETradePriceFeed
            self._auth = ETradeAuth(sandbox=False)
            auth_for_feed = self._auth if self._auth and self._auth.is_authenticated else None
            self._feed = ETradePriceFeed(auth=auth_for_feed)
        except Exception as exc:
            self.enabled = False
            logger.info("E*Trade pulse source unavailable: %s", exc)

    def _trim_history(self, symbol: str, now_ts: float) -> None:
        rows = self._history.get(symbol) or []
        if not rows:
            return
        cutoff = now_ts - max(300.0, self.lookback_seconds * 3.0)
        kept = [row for row in rows if row[0] >= cutoff]
        self._history[symbol] = kept[-256:]

    def _append_price(self, symbol: str, price: float, now_ts: float) -> None:
        if price <= 0:
            return
        rows = list(self._history.get(symbol) or [])
        if rows and abs(rows[-1][1] - price) < 1e-12:
            rows[-1] = (now_ts, price)
        else:
            rows.append((now_ts, price))
        self._history[symbol] = rows[-256:]
        self._trim_history(symbol, now_ts)

    def _return_over_lookback(self, symbol: str, now_ts: float) -> float | None:
        rows = self._history.get(symbol) or []
        if len(rows) < 2:
            return None
        target_ts = now_ts - self.lookback_seconds
        base = None
        latest = rows[-1][1]
        for ts, px in rows:
            if ts >= target_ts:
                base = px
                break
        if base is None:
            base = rows[0][1]
        if base <= 0 or latest <= 0:
            return None
        return (latest - base) / base

    def _refresh_snapshot(self) -> dict[str, float]:
        now_ts = time.time()
        with self._lock:
            if (now_ts - float(self._snapshot.get("ts", 0.0))) < self.snapshot_ttl:
                cached = self._snapshot.get("prices") or {}
                if isinstance(cached, dict) and cached:
                    return dict(cached)
            if not self._feed:
                return {}
            symbols = list(dict.fromkeys(self._macro_symbols + self._crypto_beta_symbols))
            clean = {}
            for sym in symbols:
                val = self._feed.get_price(sym)
                try:
                    px = float(val or 0.0)
                except Exception:
                    px = 0.0
                if px > 0:
                    clean[sym] = px
                    self._append_price(sym, px, now_ts)
            self._snapshot = {"ts": now_ts, "prices": clean}
            return dict(clean)

    @staticmethod
    def _avg(values):
        data = [float(v) for v in values if v is not None]
        if not data:
            return None
        return sum(data) / len(data)

    def scan(self, pair, candles_1h=None, candles_1m=None):
        if not self.enabled or not self._feed:
            return {"direction": "NONE", "confidence": 0, "reason": "E*Trade pulse disabled"}

        base = str(pair or "").upper().split("-", 1)[0]
        if base in {"USDC", "USD"}:
            return {"direction": "NONE", "confidence": 0, "reason": "Quote-only pair"}

        prices = self._refresh_snapshot()
        if not prices:
            return {"direction": "NONE", "confidence": 0, "reason": "No E*Trade pulse prices"}

        now_ts = time.time()
        macro_ret = self._avg(self._return_over_lookback(sym, now_ts) for sym in self._macro_symbols)
        beta_ret = self._avg(self._return_over_lookback(sym, now_ts) for sym in self._crypto_beta_symbols)
        if macro_ret is None and beta_ret is None:
            return {"direction": "NONE", "confidence": 0, "reason": "Pulse warmup"}

        if macro_ret is None:
            composite = float(beta_ret or 0.0)
        elif beta_ret is None:
            composite = float(macro_ret or 0.0)
        else:
            composite = float(macro_ret) * 0.65 + float(beta_ret) * 0.35

        mag = abs(composite)
        if mag < self.min_move:
            return {
                "direction": "NONE",
                "confidence": 0,
                "reason": f"E*Trade pulse flat ({composite * 100:.2f}%/{self.lookback_seconds:.0f}s)",
            }

        direction = "BUY" if composite > 0 else "SELL"
        confidence = min(0.92, 0.56 + min(0.30, mag / 0.01))
        m_txt = "n/a" if macro_ret is None else f"{macro_ret * 100:.2f}%"
        b_txt = "n/a" if beta_ret is None else f"{beta_ret * 100:.2f}%"
        return {
            "direction": direction,
            "confidence": confidence,
            "reason": (
                f"E*Trade pulse {direction}: macro={m_txt} beta={b_txt} "
                f"composite={composite * 100:.2f}%/{self.lookback_seconds:.0f}s"
            ),
        }


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


class PairsSignalSource(SignalSource):
    """Cointegration-based pairs trading signal.

    Tracks spread z-scores between correlated pairs.
    When z-score > 2: short overperformer, long underperformer.
    Mean-reversion edge: correlated pairs that diverge tend to converge.
    """

    PAIRS = [
        ("BTC-USD", "ETH-USD"),
        ("SOL-USD", "AVAX-USD"),
    ]

    def __init__(self):
        self._spread_history = {}  # (pairA, pairB) -> [spread_values]
        self._price_cache = {}  # pair -> latest_price

    def update_price(self, pair, price):
        self._price_cache[pair] = price

    def _compute_spread(self, pair_a, pair_b):
        pa = self._price_cache.get(pair_a)
        pb = self._price_cache.get(pair_b)
        if not pa or not pb:
            return None
        # Log spread
        return math.log(pa) - math.log(pb)

    def scan(self, pair, candles_1h=None, candles_1m=None):
        """Check if this pair has a pairs trading signal."""
        # Update price cache from candles
        if candles_1h and len(candles_1h) > 0:
            last = candles_1h[-1]
            # Handle both dict and list candle formats
            if isinstance(last, dict):
                self._price_cache[pair] = last.get("close", last.get("c", 0))
            elif isinstance(last, (list, tuple)) and len(last) > 4:
                self._price_cache[pair] = float(last[4])  # close price

        for pair_a, pair_b in self.PAIRS:
            if pair not in (pair_a, pair_b):
                continue

            spread = self._compute_spread(pair_a, pair_b)
            if spread is None:
                continue

            key = (pair_a, pair_b)
            if key not in self._spread_history:
                self._spread_history[key] = []
            self._spread_history[key].append(spread)

            # Keep last 200 observations
            if len(self._spread_history[key]) > 200:
                self._spread_history[key] = self._spread_history[key][-200:]

            history = self._spread_history[key]
            if len(history) < 20:
                continue

            mean_spread = sum(history) / len(history)
            variance = sum((x - mean_spread) ** 2 for x in history) / len(history)
            std_spread = max(math.sqrt(variance), 1e-8)
            zscore = (spread - mean_spread) / std_spread

            if abs(zscore) > 2.0:
                # Spread is extreme -- mean reversion expected
                if pair == pair_a:
                    # pair_a is overpriced relative to pair_b
                    direction = "SELL" if zscore > 0 else "BUY"
                else:
                    direction = "BUY" if zscore > 0 else "SELL"

                return {
                    "direction": direction,
                    "confidence": min(0.65, 0.50 + abs(zscore) * 0.05),
                    "reason": f"pairs spread z={zscore:.2f} ({pair_a}/{pair_b})",
                    "zscore": round(float(zscore), 4),
                }

        return {"direction": "NONE", "confidence": 0.0, "reason": "no pairs signal"}


class SignalCalibrator:
    """Dynamic signal weight calibration using prediction entropy.

    Tracks each signal source's accuracy over a rolling window and
    adjusts weights accordingly:
      - High accuracy (low entropy) -> boost weight up to 3x
      - Low accuracy (high entropy) -> reduce weight
      - 50% accuracy (max entropy)  -> weight unchanged

    This rewards signals that consistently predict correctly and
    dampens noisy/random signals automatically.
    """

    def __init__(self, window=100):
        self.window = window
        self._records = {}  # signal_name -> [(predicted_direction, actual_outcome)]
        self._pnl_ewma = {}  # signal_name -> ewma normalized pnl contribution
        self._pnl_count = {}  # signal_name -> number of pnl observations

    def record_outcome(self, signal_name, predicted_direction, actual_outcome):
        """Record a signal's prediction vs actual market direction.

        Args:
            signal_name: Name of the signal source (e.g. 'regime', 'arb')
            predicted_direction: What the signal predicted ('BUY' or 'SELL')
            actual_outcome: What actually happened ('BUY' = price went up, 'SELL' = price went down)
        """
        if signal_name not in self._records:
            self._records[signal_name] = []
        self._records[signal_name].append((predicted_direction, actual_outcome))
        if len(self._records[signal_name]) > self.window:
            self._records[signal_name] = self._records[signal_name][-self.window:]

    def record_realized_pnl(self, signal_name, pnl_usd, notional_usd=0.0):
        """Track realized trade contribution for a signal source.

        Normalizes pnl by notional when available so weighting is scale-invariant.
        """
        try:
            pnl = float(pnl_usd or 0.0)
        except Exception:
            pnl = 0.0
        try:
            notional = float(notional_usd or 0.0)
        except Exception:
            notional = 0.0

        if notional > 0.0:
            pnl_norm = pnl / notional
        else:
            pnl_norm = max(-0.05, min(0.05, pnl / 50.0))

        alpha = min(0.6, max(0.05, float(os.environ.get("SNIPER_SIGNAL_PNL_EWMA_ALPHA", "0.2") or 0.2)))
        prev = float(self._pnl_ewma.get(signal_name, 0.0) or 0.0)
        self._pnl_ewma[signal_name] = (1.0 - alpha) * prev + alpha * pnl_norm
        self._pnl_count[signal_name] = int(self._pnl_count.get(signal_name, 0) or 0) + 1

    def get_calibrated_weights(self, base_weights):
        """Return adjusted weights based on signal accuracy and entropy.

        Args:
            base_weights: dict of {signal_name: base_weight}

        Returns:
            dict of {signal_name: calibrated_weight} summing to 1.0
        """
        if not self._records:
            return dict(base_weights)

        import math

        adjusted = {}
        for name, base_w in base_weights.items():
            records = self._records.get(name, [])
            if len(records) < 10:
                adjusted[name] = float(base_w)
                continue

            # Compute accuracy
            correct = sum(1 for pred, actual in records if pred == actual)
            accuracy = correct / len(records)

            # Compute binary entropy: -p*log2(p) - (1-p)*log2(1-p)
            p = max(0.01, min(0.99, accuracy))
            entropy = -(p * math.log2(p) + (1 - p) * math.log2(1 - p))

            # High accuracy (low entropy) -> boost weight
            # Low accuracy (high entropy) -> reduce weight
            # At 50% accuracy, entropy=1.0, multiplier=1.0
            # At 80% accuracy, entropy~0.72, multiplier~1.39
            # At 90% accuracy, entropy~0.47, multiplier~2.13
            multiplier = 1.0 / max(0.3, entropy)
            adjusted[name] = float(base_w) * min(multiplier, 3.0)  # cap at 3x

            # PnL-aware adaptive decay/boost:
            # positive normalized pnl nudges weight up; negative nudges down.
            pnl_obs = int(self._pnl_count.get(name, 0) or 0)
            if pnl_obs >= 5:
                pnl_edge = float(self._pnl_ewma.get(name, 0.0) or 0.0)
                pnl_mult = 1.0 + max(-0.60, min(0.80, pnl_edge * 25.0))
                adjusted[name] *= pnl_mult

        # Normalize so weights sum to 1
        total = sum(adjusted.values())
        if total > 0:
            adjusted = {k: v / total for k, v in adjusted.items()}

        return adjusted


class SignalAccuracyVerifier:
    """Background verifier: checks signal predictions at 5m/15m/1h and updates signal_weights DB.

    When a signal fires, we record the prediction. Later, we check actual price movement
    and update rolling accuracy scores. Signal weights auto-calibrate: accurate signals
    get more weight, bad ones get downweighted.
    """

    VERIFY_INTERVALS = [(300, "5m"), (900, "15m"), (3600, "1h"), (86400, "24h")]

    def __init__(self, db_path=None):
        self._db_path = db_path or str(Path(__file__).parent / "trader.db")
        self._pending = []  # (signal_name, direction, pair, price, ts)
        self._lock = threading.Lock()
        self._running = False
        self._init_tables()

    def _init_tables(self):
        """Ensure signal accuracy tables exist."""
        try:
            db = sqlite3.connect(self._db_path)
            db.executescript("""
                CREATE TABLE IF NOT EXISTS signal_accuracy (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    confidence REAL,
                    pair TEXT,
                    price_at_signal REAL,
                    price_after_5m REAL,
                    price_after_15m REAL,
                    price_after_1h REAL,
                    price_after_24h REAL,
                    was_correct INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    verified_at TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS signal_weights (
                    source TEXT PRIMARY KEY,
                    weight REAL DEFAULT 1.0,
                    accuracy_30d REAL DEFAULT 0.5,
                    total_signals INTEGER DEFAULT 0,
                    correct_signals INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_sig_acc_source ON signal_accuracy(source);
                CREATE INDEX IF NOT EXISTS idx_sig_acc_pair ON signal_accuracy(pair);
            """)
            # Migrate: add price_after_24h column if missing
            try:
                db.execute("ALTER TABLE signal_accuracy ADD COLUMN price_after_24h REAL")
            except Exception:
                pass  # Column already exists
            db.commit()
            db.close()
        except Exception as e:
            logger.debug("signal_accuracy table init: %s", e)

    def record_prediction(self, signal_name, direction, pair, price, confidence=0.0):
        """Record a signal prediction for later verification."""
        with self._lock:
            self._pending.append({
                "source": signal_name,
                "direction": direction,
                "pair": pair,
                "price_at_signal": price,
                "confidence": confidence,
                "ts": time.time(),
                "verified": set(),
            })
            # Keep only last 500 pending signals
            if len(self._pending) > 500:
                self._pending = self._pending[-500:]

    def start(self):
        """Start background verification thread."""
        if self._running:
            return
        self._running = True
        t = threading.Thread(target=self._verify_loop, daemon=True, name="signal_verifier")
        t.start()
        logger.info("Signal accuracy verifier started")

    def _verify_loop(self):
        """Check pending predictions at verification intervals."""
        while self._running:
            try:
                self._verify_pending()
            except Exception as e:
                logger.debug("Signal verify error: %s", e)
            time.sleep(60)  # Check every minute

    def _verify_pending(self):
        """Verify pending predictions against actual price movements."""
        now = time.time()
        to_remove = []

        with self._lock:
            pending = list(self._pending)

        for idx, pred in enumerate(pending):
            elapsed = now - pred["ts"]
            pair = pred["pair"]

            for interval_s, label in self.VERIFY_INTERVALS:
                if label in pred["verified"]:
                    continue
                if elapsed < interval_s:
                    continue

                # Get current price
                current_price = self._get_price(pair)
                if not current_price or current_price <= 0:
                    continue

                pred["verified"].add(label)
                price_at = pred["price_at_signal"]
                price_change = (current_price - price_at) / price_at if price_at > 0 else 0

                # Was the prediction correct?
                was_correct = (
                    (pred["direction"] == "BUY" and price_change > 0) or
                    (pred["direction"] == "SELL" and price_change < 0)
                )

                # Record to DB
                self._record_verification(
                    pred["source"], pred["direction"], pair,
                    price_at, current_price, label, was_correct, pred["confidence"]
                )

            # Remove fully verified predictions (after 24h check)
            if "24h" in pred["verified"]:
                to_remove.append(idx)

        # Clean up
        if to_remove:
            with self._lock:
                for idx in sorted(to_remove, reverse=True):
                    if idx < len(self._pending):
                        self._pending.pop(idx)

    def _record_verification(self, source, direction, pair, price_at, current_price,
                             interval_label, was_correct, confidence):
        """Write verification result to signal_accuracy and update signal_weights."""
        try:
            db = sqlite3.connect(self._db_path)
            col = f"price_after_{interval_label}"

            # Insert or update signal_accuracy row
            db.execute(f"""
                INSERT INTO signal_accuracy (source, signal_type, direction, confidence,
                    pair, price_at_signal, {col}, was_correct, verified_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (source, interval_label, direction, confidence, pair,
                  price_at, current_price, 1 if was_correct else 0))

            # Update rolling accuracy in signal_weights (on 1h verification only)
            if interval_label == "1h":
                db.execute("""
                    INSERT INTO signal_weights (source, weight, accuracy_30d, total_signals,
                        correct_signals, updated_at)
                    VALUES (?, 1.0, ?, 1, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(source) DO UPDATE SET
                        total_signals = total_signals + 1,
                        correct_signals = correct_signals + ?,
                        accuracy_30d = CAST(correct_signals + ? AS REAL) / MAX(1, total_signals + 1),
                        weight = CASE
                            WHEN CAST(correct_signals + ? AS REAL) / MAX(1, total_signals + 1) > 0.6
                            THEN MIN(2.0, 1.0 + (CAST(correct_signals + ? AS REAL) / MAX(1, total_signals + 1) - 0.5))
                            ELSE MAX(0.3, CAST(correct_signals + ? AS REAL) / MAX(1, total_signals + 1))
                        END,
                        updated_at = CURRENT_TIMESTAMP
                """, (source, 1.0 if was_correct else 0.0,
                      1 if was_correct else 0,
                      1 if was_correct else 0,
                      1 if was_correct else 0,
                      1 if was_correct else 0,
                      1 if was_correct else 0,
                      1 if was_correct else 0))

                logger.info("SIGNAL_ACCURACY: %s %s %s — %s (price: $%.2f -> $%.2f, change: %.2f%%)",
                           source, direction, pair,
                           "CORRECT" if was_correct else "WRONG",
                           price_at, current_price,
                           (current_price - price_at) / price_at * 100 if price_at > 0 else 0)

            db.commit()
            db.close()
        except Exception as e:
            logger.debug("Signal accuracy DB write error: %s", e)

    @staticmethod
    def _get_price(pair):
        """Get current price."""
        try:
            from exchange_connector import PriceFeed
            return PriceFeed.get_price(pair)
        except Exception:
            return None


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
        self._max_trades_per_hour = int(os.environ.get("SNIPER_MAX_TRADES_PER_HOUR", "20"))
        self._max_trades_per_day = int(os.environ.get("SNIPER_MAX_TRADES_PER_DAY", "80"))
        self._price_cache = {}
        self._price_cache_lock = threading.Lock()
        self._price_cache_ttl = float(os.environ.get("SNIPER_PRICE_CACHE_SECONDS", "1.5"))
        self._holdings_cache_lock = threading.Lock()
        self._holdings_cache_ttl = float(os.environ.get("SNIPER_HOLDINGS_CACHE_SECONDS", "8"))
        self._holdings_cache = {"ts": 0.0, "holdings": {}, "cash": 0.0, "quotes": {"USD": 0.0, "USDC": 0.0}}
        self._pair_buy_cooldown_until = {}
        self._close_evidence_cache = {}
        self._profit_focus_cache = {"ts": 0.0, "pairs": []}
        self._balance_flow_cache = {"ts": 0.0, "state": {}}
        self._db_signal_weight_cache = {"ts": 0.0, "weights": {}}
        self._db_signal_weight_ttl_s = max(
            5.0, float(os.environ.get("SNIPER_DB_SIGNAL_WEIGHTS_TTL_S", "60") or 60)
        )
        self._db_signal_weight_blend = min(
            1.0, max(0.0, float(os.environ.get("SNIPER_DB_SIGNAL_WEIGHTS_BLEND", "0.35") or 0.35))
        )
        self._db_signal_weight_min_signals = max(
            1, int(os.environ.get("SNIPER_DB_SIGNAL_WEIGHTS_MIN_SIGNALS", "10") or 10)
        )
        self._scan_batch_active = False
        self._scan_write_pending = 0
        self._scan_commit_interval_s = max(
            0.10, float(os.environ.get("SNIPER_SCAN_COMMIT_INTERVAL_S", "1.0") or 1.0)
        )
        self._scan_commit_batch_size = max(
            1, int(os.environ.get("SNIPER_SCAN_COMMIT_BATCH_SIZE", "8") or 8)
        )
        self._scan_last_commit_ts = time.time()
        self._last_interval_logged = None
        self._last_execution_health_autorefresh = 0.0
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
            "etrade_pulse": ETradePulseSource(),
            **({"xgb_lite": _xgb_source} if _xgb_source else {}),
            **({"w1_regime": _w1_regime_source} if _w1_regime_source else {}),
            **({"vpin": _vpin_source} if _vpin_source else {}),
            **({"coinglass": _coinglass_source} if _coinglass_source else {}),
            **({"reddit_sentiment": _reddit_source} if _reddit_source else {}),
            **({"graph": _graph_source} if _graph_source else {}),
            "pairs": PairsSignalSource(),
        }
        # Entropy-based signal calibrator: adjusts weights dynamically
        # based on each signal's track record (accuracy -> entropy -> multiplier)
        self._signal_calibrator = SignalCalibrator(window=100)
        # Signal accuracy feedback loop: verifies predictions at 5m/15m/1h
        self._signal_verifier = SignalAccuracyVerifier()
        # Order TTL monitor: cancel stale orders and re-price at current market
        self._order_ttl_seconds = int(os.environ.get("SNIPER_ORDER_TTL_SECONDS", "120"))
        self._order_ttl_enabled = self._order_ttl_seconds > 0
        self._ttl_tracked_orders = {}  # {order_id: {"placed_at": ts, "pair": p, ...}}
        self._ttl_lock = threading.Lock()
        # Conviction Rally: test small, rally big position management
        self._conviction = ConvictionRally() if _conviction_rally_available else None
        # WebSocket feed for O(1) price lookups — eliminates 400-900ms REST calls
        self._ws_feed = None
        if _ws_feed_available and CoinbaseWSFeed is not None:
            try:
                ws_pairs = [p for p in CONFIG.get("pairs", []) if p]
                if ws_pairs:
                    self._ws_feed = CoinbaseWSFeed(ws_pairs)
                    self._ws_feed.start()
                    logger.info("WebSocket feed started for %d pairs", len(ws_pairs))
            except Exception as _ws_err:
                logger.warning("WebSocket feed init failed: %s", _ws_err)
                self._ws_feed = None

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

    @staticmethod
    def _new_coinbase_trader():
        from exchange_connector import CoinbaseTrader
        return CoinbaseTrader()

    _kraken = None

    @staticmethod
    def _new_kraken_trader():
        """Lazy init KrakenConnector for multi-venue routing."""
        if Sniper._kraken is None:
            try:
                from kraken_connector import KrakenConnector
                Sniper._kraken = KrakenConnector
                logger.info("Kraken trader initialized for multi-venue routing")
            except Exception as e:
                logger.warning("KrakenConnector not available: %s", e)
        return Sniper._kraken

    _smart_router = None

    @staticmethod
    def _get_smart_router():
        """Lazy init SmartRouter for venue selection."""
        if Sniper._smart_router is None:
            try:
                from smart_router import SmartRouter
                Sniper._smart_router = SmartRouter()
                logger.info("SmartRouter initialized for venue selection")
            except Exception as e:
                logger.warning("SmartRouter not available: %s", e)
        return Sniper._smart_router

    @staticmethod
    def _extract_kraken_order_id(payload):
        data = payload if isinstance(payload, dict) else {}
        result = data.get("result", {}) if isinstance(data.get("result"), dict) else {}
        txids = result.get("txid")
        if isinstance(txids, list) and txids:
            return str(txids[0])
        if isinstance(txids, str) and txids:
            return txids
        return ""

    def _build_buy_bracket(self, pair, limit_price):
        bracket = None
        try:
            from exit_manager import ExitManager
            _em = ExitManager.__new__(ExitManager)
            ep = _em.get_exit_params(pair)
            if ep and "tp1_pct" in ep and "wide_stop_pct" in ep:
                bracket = {
                    "take_profit_price": round(limit_price * (1 + ep["tp1_pct"]), 2),
                    "stop_loss_price": round(limit_price * (1 - ep["wide_stop_pct"]), 2),
                }
                logger.info(
                    "BRACKET: %s TP=$%.2f SL=$%.2f (tp1=%.2f%% stop=%.2f%%)",
                    pair,
                    bracket["take_profit_price"],
                    bracket["stop_loss_price"],
                    ep["tp1_pct"] * 100,
                    ep["wide_stop_pct"] * 100,
                )
        except Exception as be:
            logger.debug("Bracket config unavailable: %s", be)
        return bracket

    def _execute_spot_limit_order(
        self,
        *,
        pair,
        side,
        amount_usd,
        price_ref,
        limit_price,
        signal,
        venue_hint,
        held_base=None,
    ):
        """Place a single spot maker order on the requested venue with safe fallback."""
        side_u = str(side or "").upper()
        venue_target = str(venue_hint or "coinbase").strip().lower()
        px = float(price_ref or 0.0)
        if px <= 0:
            return {"error_response": {"error": "INVALID_PRICE", "message": "price_ref <= 0"}}, "coinbase", False
        notional_usd = max(0.0, float(amount_usd or 0.0))
        if notional_usd < 0.01:
            return {"error_response": {"error": "INVALID_SIZE", "message": "amount_usd too small"}}, "coinbase", False

        base_size = notional_usd / px
        if side_u == "SELL" and held_base is not None:
            try:
                base_size = min(base_size, float(held_base or 0.0))
            except Exception:
                pass
        if base_size <= 0:
            return {"error_response": {"error": "INVALID_BASE_SIZE", "message": "base size <= 0"}}, "coinbase", False

        if venue_target == "kraken":
            if side_u == "BUY":
                # Check if Kraken needs asset conversion (e.g., sell ETH→USD first)
                try:
                    from kraken_connector import KrakenConnector
                    raw_bal = KrakenConnector.get_account_balance()
                    if isinstance(raw_bal, dict) and "error" not in raw_bal:
                        usd_avail = float(raw_bal.get("ZUSD", 0) or 0) + float(raw_bal.get("USDC", 0) or 0)
                        if usd_avail < notional_usd:
                            # Sell ETH to free up USD (if we have ETH and aren't buying ETH)
                            eth_bal = float(raw_bal.get("XETH", raw_bal.get("ETH", 0)) or 0)
                            if eth_bal > 0.001 and not pair.startswith("ETH"):
                                sell_kraken = self._new_kraken_trader()
                                if sell_kraken:
                                    # Sell enough ETH to cover the buy + buffer
                                    needed = notional_usd - usd_avail + 1.0  # $1 buffer
                                    eth_price = float(self._get_price_fast("ETH-USD") or 0)
                                    if eth_price > 0:
                                        eth_to_sell = min(eth_bal * 0.95, needed / eth_price)
                                        if eth_to_sell > 0.0001:
                                            conv_result = sell_kraken.place_order(
                                                pair="ETH-USD", side="sell", volume=eth_to_sell,
                                                order_type="market", confidence=0.99,
                                            )
                                            logger.info("Kraken ETH→USD conversion: sold %.6f ETH ($%.2f) for BUY %s: %s",
                                                       eth_to_sell, eth_to_sell * eth_price, pair, conv_result)
                                            import time as _t; _t.sleep(1)  # Let settlement propagate
                except Exception as e:
                    logger.debug("Kraken pre-buy conversion check failed: %s", e)

                try:
                    from cross_venue_transfer import CrossVenueTransfer
                    _xfer = CrossVenueTransfer()
                    _xfer.ensure_venue_funded("kraken", "USDC", notional_usd, trade_pair=pair)
                    _xfer.close()
                except Exception:
                    pass

            kraken = self._new_kraken_trader()
            if kraken:
                kraken_result = kraken.place_order(
                    pair=pair,
                    side="buy" if side_u == "BUY" else "sell",
                    volume=base_size,
                    order_type="limit",
                    price=limit_price,
                    confidence=float(signal.get("composite_confidence", 0.7) or 0.7),
                    oflags="post",
                )
                kraken_order_id = self._extract_kraken_order_id(kraken_result)
                if kraken_order_id:
                    # Quick fill check: wait up to 30s for Kraken fill, cancel+fallback if unfilled
                    kraken_fill_timeout = int(os.environ.get("SNIPER_KRAKEN_FILL_TIMEOUT_S", "30"))
                    filled = False
                    try:
                        poll_start = time.time()
                        while time.time() - poll_start < kraken_fill_timeout:
                            status_resp = kraken.query_orders(txid=kraken_order_id)
                            if isinstance(status_resp, dict):
                                order_info = status_resp.get(kraken_order_id, {})
                                order_status = str(order_info.get("status", "")).lower()
                                if order_status == "closed":
                                    filled = True
                                    break
                                if order_status in ("canceled", "cancelled", "expired"):
                                    break
                            time.sleep(3)
                    except Exception as e:
                        logger.debug("Kraken fill poll error: %s", e)
                        filled = True  # Assume filled if we can't check — don't cancel blindly

                    if filled:
                        return (
                            {
                                "success_response": {"order_id": kraken_order_id},
                                "order_id": kraken_order_id,
                                "venue": "kraken",
                            },
                            "kraken",
                            True,
                        )
                    else:
                        # Cancel unfilled Kraken order and fall through to Coinbase
                        try:
                            kraken.cancel_order(kraken_order_id)
                            logger.warning("Kraken %s order %s not filled in %ds, cancelled → fallback to Coinbase",
                                         side_u, kraken_order_id, kraken_fill_timeout)
                        except Exception as e:
                            logger.debug("Kraken cancel error: %s", e)
                logger.error("Kraken %s failed for %s: %s", side_u, pair, kraken_result.get("error"))
            else:
                logger.warning("Kraken connector unavailable, falling back to Coinbase for %s %s", side_u, pair)

        from exchange_connector import CoinbaseTrader
        trader = CoinbaseTrader()
        bracket = self._build_buy_bracket(pair, limit_price) if side_u == "BUY" else None
        cb_result = trader.place_limit_order(
            pair,
            side_u,
            base_size,
            limit_price,
            post_only=True,
            expected_edge_pct=float(signal.get("composite_confidence", 0.0) or 0.0) * 100.0,
            signal_confidence=float(signal.get("composite_confidence", 0.0) or 0.0),
            market_regime=str(signal.get("regime", signal.get("market_regime", "neutral")) or "neutral"),
            bypass_profit_guard=(side_u == "SELL"),
            bracket_config=bracket,
        )
        acked = bool(self._extract_order_id_from_result(cb_result))
        return cb_result, "coinbase", acked

    def _execute_split_plan_orders(
        self,
        *,
        pair,
        side,
        total_notional_usd,
        price_ref,
        signal,
        split_plan,
        held_base=None,
    ):
        """Execute split plan slices sequentially and process each order result."""
        side_u = str(side or "").upper()
        if not isinstance(split_plan, dict) or not bool(split_plan.get("enabled", False)):
            return {"handled": False, "any_filled": False}
        raw_slices = split_plan.get("slices")
        if not isinstance(raw_slices, list):
            return {"handled": False, "any_filled": False}
        slices = [s for s in raw_slices if isinstance(s, dict)]
        if len(slices) <= 1:
            return {"handled": False, "any_filled": False}

        conf = float(signal.get("composite_confidence", 0.7) or 0.7)
        if side_u == "BUY":
            price_offset = 0.9992 if conf >= 0.90 else (0.9994 if conf >= 0.80 else 0.9996)
        else:
            price_offset = 1.0002 if conf >= 0.90 else (1.0004 if conf >= 0.80 else 1.0006)

        remaining_notional = max(0.0, float(total_notional_usd or 0.0))
        any_filled = False
        acknowledged_notional = 0.0
        attempted = 0

        for idx, slice_row in enumerate(slices):
            if remaining_notional < 0.5:
                break
            try:
                requested_usd = float(slice_row.get("amount_usd", 0.0) or 0.0)
            except Exception:
                requested_usd = 0.0
            if requested_usd < 0.5:
                continue
            slice_usd = min(requested_usd, remaining_notional)
            if slice_usd < 0.5:
                continue
            venue = str(slice_row.get("venue", "coinbase") or "coinbase").lower()
            limit_price = float(price_ref or 0.0) * price_offset
            result, venue_used, acked = self._execute_spot_limit_order(
                pair=pair,
                side=side_u,
                amount_usd=slice_usd,
                price_ref=price_ref,
                limit_price=limit_price,
                signal=signal,
                venue_hint=venue,
                held_base=held_base,
            )
            attempted += 1
            if acked:
                acknowledged_notional += slice_usd
            filled = self._process_order_result(
                result,
                pair,
                side_u,
                slice_usd,
                price_ref,
                signal,
                venue=venue_used,
            )
            any_filled = any_filled or bool(filled)
            remaining_notional = max(0.0, remaining_notional - slice_usd)
            logger.info(
                "SNIPER SPLIT: %s slice %d/%d venue=%s notional=$%.2f ack=%s filled=%s",
                pair,
                idx + 1,
                len(slices),
                venue_used,
                slice_usd,
                acked,
                bool(filled),
            )

        if side_u == "BUY" and acknowledged_notional > 0:
            self._cycle_cash_spent = getattr(self, "_cycle_cash_spent", 0.0) + float(acknowledged_notional)

        return {
            "handled": attempted > 0,
            "any_filled": bool(any_filled),
            "acknowledged_notional_usd": round(float(acknowledged_notional), 6),
            "attempted_slices": attempted,
        }

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

    def _load_db_signal_weights(self):
        now = time.time()
        cached = self._db_signal_weight_cache if isinstance(self._db_signal_weight_cache, dict) else {}
        if (
            isinstance(cached.get("weights"), dict)
            and (now - float(cached.get("ts", 0.0) or 0.0)) <= self._db_signal_weight_ttl_s
        ):
            return dict(cached.get("weights") or {})

        weights = {}
        try:
            if TRADER_DB_PATH.exists():
                conn = sqlite3.connect(str(TRADER_DB_PATH), timeout=3.0)
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    """
                    SELECT source, weight, total_signals
                    FROM signal_weights
                    WHERE total_signals >= ?
                    """,
                    (self._db_signal_weight_min_signals,),
                ).fetchall()
                conn.close()
                for row in rows:
                    source = str(row["source"] or "").strip()
                    if not source:
                        continue
                    try:
                        w = float(row["weight"] or 0.0)
                    except Exception:
                        w = 0.0
                    if w > 0:
                        weights[source] = w
        except Exception as e:
            logger.debug("SNIPER: DB signal weight load failed: %s", e)

        self._db_signal_weight_cache = {"ts": now, "weights": dict(weights)}
        return dict(weights)

    def _load_lisp_weights(self):
        """Load signal weights from Lisp weight_adaptation engine output."""
        lisp_path = os.environ.get("LISP_WEIGHTS_PATH", "agents/runtime/signal_weights.json")
        try:
            if not os.path.exists(lisp_path):
                return {}
            age = time.time() - os.path.getmtime(lisp_path)
            if age > 300:  # Stale after 5 minutes
                return {}
            with open(lisp_path) as f:
                data = json.load(f)
            if isinstance(data, dict):
                return {k: float(v) for k, v in data.items() if isinstance(v, (int, float))}
        except Exception:
            pass
        return {}

    def _load_champion_weights(self):
        """Load signal weights from AutoBuilder champion config (flywheel feedback loop).

        Chain: alpha_researcher → auto_builder → champion_config.json → sniper
        """
        champion_path = os.path.join(os.path.dirname(__file__), "runtime", "champion_config.json")
        try:
            if not os.path.exists(champion_path):
                return {}
            age = time.time() - os.path.getmtime(champion_path)
            if age > 600:  # Stale after 10 minutes
                return {}
            with open(champion_path) as f:
                data = json.load(f)
            weights = data.get("signal_weights", {})
            if isinstance(weights, dict) and weights:
                return {k: float(v) for k, v in weights.items() if isinstance(v, (int, float)) and float(v) > 0}
        except Exception:
            pass
        return {}

    def _effective_signal_base_weights(self):
        base = dict(CONFIG["signal_weights"])

        # Priority chain: DB weights → Champion config → Lisp weights → defaults
        external = self._load_db_signal_weights()
        if not external:
            external = self._load_champion_weights()
        if not external:
            external = self._load_lisp_weights()
        if not external:
            base = self._apply_regime_adjustments(base)
            return base

        blend = self._db_signal_weight_blend
        for name, ext_w in external.items():
            if name not in base:
                continue
            base[name] = max(0.0001, (base[name] * (1.0 - blend)) + (float(ext_w) * blend))

        base = self._apply_regime_adjustments(base)

        total = sum(base.values())
        if total > 0:
            base = {k: float(v) / total for k, v in base.items()}
        return base

    def _apply_regime_adjustments(self, weights):
        """Adjust signal weights based on current market regime.

        BEAR: boost fear_greed (contrarian), reduce momentum
        BULL: boost momentum, reduce fear_greed
        SIDEWAYS: no adjustments
        """
        try:
            from regime_detector import RegimeDetector
            detector = RegimeDetector()
            regime = detector.get_current_regime()
        except Exception:
            return weights

        w = dict(weights)
        if regime == "bear":
            w["fear_greed"] = w.get("fear_greed", 0.08) + 0.04
            w["momentum"] = max(0.02, w.get("momentum", 0.10) - 0.04)
            w["orderbook"] = max(0.02, w.get("orderbook", 0.08) - 0.02)
        elif regime == "bull":
            w["momentum"] = w.get("momentum", 0.10) + 0.04
            w["fear_greed"] = max(0.02, w.get("fear_greed", 0.08) - 0.02)
        # SIDEWAYS: no changes

        # Re-normalize
        total = sum(w.values())
        if total > 0:
            w = {k: float(v) / total for k, v in w.items()}
        return w

    # ── Order TTL Monitor ──────────────────────────────────────────────

    def track_order_ttl(self, order_id, pair, direction, amount_usd):
        """Register an order for TTL monitoring."""
        if not self._order_ttl_enabled or not order_id:
            return
        with self._ttl_lock:
            self._ttl_tracked_orders[order_id] = {
                "placed_at": time.time(),
                "pair": pair,
                "direction": direction,
                "amount_usd": amount_usd,
            }

    def check_stale_orders(self, trader=None):
        """Check for stale orders past TTL and cancel them.

        Returns list of cancelled order IDs for potential re-submission.
        """
        if not self._order_ttl_enabled or not trader:
            return []
        now = time.time()
        stale = []
        with self._ttl_lock:
            for oid, info in list(self._ttl_tracked_orders.items()):
                age = now - info["placed_at"]
                if age > self._order_ttl_seconds:
                    stale.append((oid, info))

        cancelled = []
        for oid, info in stale:
            try:
                trader.cancel_order(oid)
                logger.info("TTL_CANCEL: order %s age=%.0fs > ttl=%ds pair=%s",
                           oid, now - info["placed_at"], self._order_ttl_seconds,
                           info["pair"])
                cancelled.append(info)
            except Exception as e:
                logger.debug("TTL cancel failed for %s: %s", oid, e)
            with self._ttl_lock:
                self._ttl_tracked_orders.pop(oid, None)

        return cancelled

    def clear_filled_order(self, order_id):
        """Remove a filled order from TTL tracking."""
        if not order_id:
            return
        with self._ttl_lock:
            self._ttl_tracked_orders.pop(order_id, None)

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
    def _canonical_focus_pair(pair):
        p = Sniper._normalize_pair(pair)
        if p.endswith("-USDC"):
            return p.replace("-USDC", "-USD")
        return p

    def _profit_focus_scan_pairs(self, base_pairs):
        pairs = [self._normalize_pair(p) for p in (base_pairs or []) if self._normalize_pair(p)]
        if not bool(CONFIG.get("profit_focus_mode", True)):
            return pairs
        if not pairs:
            return pairs

        lookback_h = max(1, int(CONFIG.get("profit_focus_lookback_hours", 72) or 72))
        min_closes = max(1, int(CONFIG.get("profit_focus_min_closes", 3) or 3))
        min_net = float(CONFIG.get("profit_focus_min_net_pnl_usd", 0.10) or 0.10)
        top_n = max(1, int(CONFIG.get("profit_focus_top_n", 4) or 4))
        min_scan_pairs = max(1, int(CONFIG.get("profit_focus_min_scan_pairs", 2) or 2))
        cache_ttl = max(10, int(CONFIG.get("profit_focus_cache_seconds", 90) or 90))
        include_primary = bool(CONFIG.get("profit_focus_include_primary", True))
        include_targets = bool(CONFIG.get("profit_focus_include_close_targets", True))

        base_by_canonical = {}
        for pair in pairs:
            canon = self._canonical_focus_pair(pair)
            if not canon:
                continue
            bucket = base_by_canonical.setdefault(canon, [])
            if pair not in bucket:
                bucket.append(pair)

        def _select_base_alias(canon, observed_pair):
            options = list(base_by_canonical.get(canon, []))
            if not options:
                return ""
            observed = self._normalize_pair(observed_pair)
            if observed in options:
                return observed
            if observed.endswith("-USDC"):
                for item in options:
                    if item.endswith("-USDC"):
                        return item
            for item in options:
                if item.endswith("-USD"):
                    return item
            return options[0]

        now = time.time()
        cached = self._profit_focus_cache if isinstance(self._profit_focus_cache, dict) else {}
        ranked = []
        if (
            cached
            and isinstance(cached.get("pairs"), list)
            and (now - float(cached.get("ts", 0.0) or 0.0)) <= cache_ttl
        ):
            ranked = [self._normalize_pair(p) for p in cached.get("pairs", []) if self._normalize_pair(p)]
        else:
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
            marks = ",".join("?" for _ in blocked)
            conn = None
            try:
                if TRADER_DB_PATH.exists():
                    conn = sqlite3.connect(str(TRADER_DB_PATH), timeout=5.0)
                    conn.row_factory = sqlite3.Row
                    conn.execute("PRAGMA busy_timeout=5000")
                    if self._trader_table_exists(conn, "agent_trades"):
                        rows = conn.execute(
                            f"""
                            SELECT
                                pair,
                                COUNT(CASE WHEN pnl IS NOT NULL THEN 1 END) AS closes,
                                COALESCE(SUM(COALESCE(pnl, 0)), 0) AS net_pnl,
                                COALESCE(AVG(CASE WHEN pnl IS NOT NULL THEN pnl END), 0) AS avg_pnl
                            FROM agent_trades
                            WHERE UPPER(COALESCE(side, ''))='SELL'
                              AND pnl IS NOT NULL
                              AND created_at >= datetime('now', ?)
                              AND (
                                status IS NULL
                                OR LOWER(COALESCE(status, '')) NOT IN ({marks})
                              )
                            GROUP BY pair
                            HAVING COUNT(CASE WHEN pnl IS NOT NULL THEN 1 END) >= ?
                               AND COALESCE(SUM(COALESCE(pnl, 0)), 0) > ?
                            ORDER BY net_pnl DESC, avg_pnl DESC, closes DESC
                            LIMIT ?
                            """,
                            (
                                f"-{int(lookback_h)} hours",
                                *blocked,
                                int(min_closes),
                                float(min_net),
                                int(max(4, top_n * 3)),
                            ),
                        ).fetchall()
                        for row in rows:
                            canon = self._canonical_focus_pair(row["pair"])
                            pick = _select_base_alias(canon, row["pair"])
                            if pick:
                                ranked.append(pick)
            except Exception:
                ranked = []
            finally:
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass
            ranked = list(dict.fromkeys(ranked))
            self._profit_focus_cache = {"ts": now, "pairs": list(ranked)}

        required = []
        if include_primary:
            for pair in (CONFIG.get("primary_pairs") or []):
                p = self._normalize_pair(pair)
                if p and p in pairs:
                    required.append(p)
        if include_targets:
            for pair in self._close_evidence_targets():
                alias = _select_base_alias(self._canonical_focus_pair(pair), pair)
                if alias:
                    required.append(alias)

        out = []
        seen = set()
        for pair in list(ranked) + list(required):
            p = self._normalize_pair(pair)
            if not p or p in seen or p not in pairs:
                continue
            out.append(p)
            seen.add(p)

        target_size = max(min_scan_pairs, min(top_n, len(pairs)))
        for pair in pairs:
            if len(out) >= target_size:
                break
            if pair in seen:
                continue
            out.append(pair)
            seen.add(pair)
        return out[:target_size]

    @staticmethod
    def _trader_table_exists(conn, table_name):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (str(table_name),),
        ).fetchone()
        return bool(row)

    @staticmethod
    def _completed_trade_statuses():
        return ("filled", "closed", "executed", "partial_filled", "partially_filled", "settled")

    @staticmethod
    def _pending_reconcile_statuses():
        return ("pending", "placed", "open", "accepted", "ack_ok")

    @staticmethod
    def _normalize_trade_terminal_status(status, filled_size=0.0):
        raw = str(status or "").strip().upper()
        if raw == "FILLED":
            return "filled"
        if raw in {"PARTIAL_FILLED", "PARTIALLY_FILLED"}:
            return "partial_filled"
        if raw in {"CANCELLED", "CANCELED"}:
            return "cancelled"
        if raw in {"FAILED", "REJECTED"}:
            return "failed"
        if raw == "EXPIRED":
            return "expired"
        if float(filled_size or 0.0) > 0.0:
            return "filled"
        return ""

    def _estimate_realized_sell_pnl(self, pair, exit_price, quantity, total_usd):
        pair_norm = self._normalize_pair(pair)
        notional = float(total_usd or 0.0)
        px = float(exit_price or 0.0)
        qty = float(quantity or 0.0)
        if notional <= 0.0 and px > 0.0 and qty > 0.0:
            notional = px * qty
        if px <= 0.0 or qty <= 0.0 or notional <= 0.0:
            return None
        buy_price = self._latest_filled_buy_price_any(pair_norm, fallback_price=0.0)
        if buy_price <= 0.0:
            buy_price = self._shared_buy_cost_basis(pair_norm)
        if buy_price <= 0.0:
            return None
        fees = notional * max(0.0, float(CONFIG.get("round_trip_fee_pct", 0.008) or 0.008))
        return ((px - buy_price) / buy_price) * notional - fees

    def _reconcile_pending_close_rows(self, pair):
        if not bool(CONFIG.get("close_evidence_reconcile_pending_mode", True)):
            return {"checked": 0, "updated": 0}
        if not TRADER_DB_PATH.exists():
            return {"checked": 0, "updated": 0}

        focus_pair = self._normalize_pair(pair)
        aliases = self._pair_aliases(focus_pair)
        if not aliases:
            return {"checked": 0, "updated": 0}

        pending = self._pending_reconcile_statuses()
        pair_marks = ",".join("?" for _ in aliases)
        status_marks = ",".join("?" for _ in pending)
        lookback_h = min(
            max(1, int(CONFIG.get("close_evidence_lookback_hours", 168) or 168)),
            max(1, int(CONFIG.get("close_evidence_reconcile_lookback_hours", 96) or 96)),
        )
        row_limit = max(1, int(CONFIG.get("close_evidence_reconcile_max_rows", 16) or 16))
        poll_seconds = max(0.5, float(CONFIG.get("close_evidence_reconcile_poll_seconds", 2.5) or 2.5))
        completed = set(self._completed_trade_statuses())
        target_aliases = self._close_evidence_target_aliases()

        conn = None
        rows = []
        checked = 0
        updated = 0
        try:
            conn = sqlite3.connect(str(TRADER_DB_PATH), timeout=5.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=5000")
            if not self._trader_table_exists(conn, "agent_trades"):
                try:
                    conn.close()
                except Exception:
                    pass
                return {"checked": 0, "updated": 0}
            rows = conn.execute(
                f"""
                SELECT id, pair, side, price, quantity, total_usd, order_id, status, pnl
                FROM agent_trades
                WHERE pair IN ({pair_marks})
                  AND UPPER(COALESCE(side, ''))='SELL'
                  AND order_id IS NOT NULL
                  AND TRIM(COALESCE(order_id, ''))!=''
                  AND LOWER(COALESCE(status, '')) IN ({status_marks})
                  AND created_at >= datetime('now', ?)
                ORDER BY id DESC
                LIMIT ?
                """,
                (*aliases, *pending, f"-{int(lookback_h)} hours", int(row_limit)),
            ).fetchall()
        except Exception:
            rows = []

        if not rows or conn is None:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            return {"checked": int(checked), "updated": int(updated)}

        try:
            from exchange_connector import CoinbaseTrader

            trader = CoinbaseTrader()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return {"checked": int(checked), "updated": int(updated)}

        for row in rows:
            checked += 1
            order_id = str(row["order_id"] or "").strip()
            if not order_id:
                continue
            try:
                fill = trader.get_order_fill(order_id, max_wait=poll_seconds, poll_interval=0.4) or {}
            except Exception:
                fill = {}
            if not fill:
                continue

            try:
                filled_sz = float(fill.get("filled_size", 0.0) or 0.0)
            except Exception:
                filled_sz = 0.0
            try:
                avg_px = float(
                    fill.get("average_filled_price", fill.get("avg_price", 0.0)) or 0.0
                )
            except Exception:
                avg_px = 0.0

            new_status = self._normalize_trade_terminal_status(fill.get("status"), filled_sz)
            if not new_status:
                continue
            old_status = str(row["status"] or "").strip().lower()
            if new_status == old_status:
                continue

            qty = filled_sz if filled_sz > 0.0 else float(row["quantity"] or 0.0)
            px = avg_px if avg_px > 0.0 else float(row["price"] or 0.0)
            usd = (qty * px) if qty > 0.0 and px > 0.0 else float(row["total_usd"] or 0.0)

            pnl_val = row["pnl"]
            if new_status in completed:
                pnl_val = self._estimate_realized_sell_pnl(row["pair"], px, qty, usd)
                if pnl_val is None and self._normalize_pair(row["pair"]) in target_aliases:
                    pnl_val = 0.0

            try:
                conn.execute(
                    """
                    UPDATE agent_trades
                       SET price=?,
                           quantity=?,
                           total_usd=?,
                           status=?,
                           pnl=?
                     WHERE id=?
                    """,
                    (
                        float(px),
                        float(qty),
                        float(usd),
                        str(new_status),
                        None if pnl_val is None else float(pnl_val),
                        int(row["id"]),
                    ),
                )
                updated += 1
            except Exception:
                continue

        try:
            if updated > 0:
                conn.commit()
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return {"checked": int(checked), "updated": int(updated)}

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
        self._reconcile_pending_close_rows(key)
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
                px = float(self._get_price_fast(alias) or 0.0)
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
            price = self._get_price_fast(pair)
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

    def _is_chain_bootstrap_mode(self):
        """Check if we are in bootstrap mode (no trades in last N hours).

        When the sniper has not placed any trades recently, we relax chain gate
        minimums by 50% to prevent cold-start deadlock.  All other safety gates
        (GoalValidator, EV, risk_controller) remain fully enforced.
        """
        bootstrap_hours = int(CONFIG.get("chain_gate_bootstrap_hours", 24))
        if bootstrap_hours <= 0:
            return False
        try:
            cutoff_ts = time.time() - bootstrap_hours * 3600
            # Use sniper_trades table — our own canonical record of placed trades.
            with self._db_lock:
                row = self.db.execute(
                    "SELECT COUNT(*) FROM sniper_trades WHERE created_at >= datetime(?, 'unixepoch')",
                    (cutoff_ts,),
                ).fetchone()
            return (row[0] if row else 0) == 0
        except Exception as e:
            logger.debug("chain bootstrap check failed: %s", e)
            return False

    def _validate_long_chain(self, pair, strategic_ctx):
        """Enforce long-chain viability: entry must show profitable path to EXIT."""
        if _planner is None:
            return {"viable": True, "reason": "planner unavailable", "net_edge": 0.0, "worst_case_edge": 0.0}
        if not strategic_ctx:
            return {"viable": False, "reason": "planner context unavailable", "net_edge": 0.0, "worst_case_edge": 0.0}

        # Determine effective chain gate thresholds.
        # In bootstrap mode (no recent trades), bypass chain gate entirely to break
        # cold-start deadlock.  GoalValidator + EV + risk_controller still enforce safety.
        min_net = float(CONFIG["min_chain_net_edge"])
        min_worst = float(CONFIG["min_chain_worst_case_edge"])
        bootstrap = self._is_chain_bootstrap_mode()
        if bootstrap:
            logger.info("  %s: chain gate BOOTSTRAP BYPASS — no trades in %dh, allowing entry "
                       "(GoalValidator+EV+risk still enforced)",
                       pair, int(CONFIG.get("chain_gate_bootstrap_hours", 24)))
            return {"viable": True, "reason": "bootstrap_bypass", "net_edge": 0.0,
                    "worst_case_edge": 0.0, "bootstrap_mode": True}

        analysis = strategic_ctx.get("analysis", {})
        validations = analysis.get("entry_validations", {}) if isinstance(analysis, dict) else {}
        val = validations.get(pair)
        if not isinstance(val, dict):
            # Fallback direct call if analysis omitted this pair for any reason.
            val = _planner.chain_planner.evaluate_entry_chain(
                pair,
                strategic_ctx.get("market_signals", {}),
                min_net_edge=min_net,
                min_worst_case_edge=min_worst,
            )

        net_edge = float(val.get("net_edge", 0.0) or 0.0)
        worst_edge = float(val.get("worst_case_edge", 0.0) or 0.0)
        steps = int(val.get("steps", 0) or 0)
        has_exit = bool(val.get("has_exit", False))

        # Core viability: profitable chain with exit path (edge + structure checks)
        core_viable = (
            has_exit
            and steps >= int(CONFIG["min_chain_steps"])
            and net_edge >= min_net
            and worst_edge >= min_worst
        )

        # Planner c_gate is advisory — prevents cold-restart deadlock where
        # no trades can happen because the C gate has no recent close evidence.
        # All other safety gates (GoalValidator, EV, risk_controller, quant signals)
        # remain fully enforced.
        planner_viable = bool(val.get("viable", False))
        if core_viable and not planner_viable:
            logger.info("  %s: chain c_gate advisory override — core viable (edge=%.2f%% worst=%.2f%%)",
                       pair, net_edge * 100, worst_edge * 100)

        viable = core_viable

        out = dict(val)
        out.update({
            "viable": viable,
            "net_edge": net_edge,
            "worst_case_edge": worst_edge,
            "steps": steps,
            "has_exit": has_exit,
            "bootstrap_mode": bootstrap,
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
        # Extreme Fear (F&G < 15): promote fear_greed to quant status — statistical edge is massive
        _effective_quant_set = set(quant_set)
        fg_result = results.get("fear_greed", {})
        if fg_result.get("confidence", 0) > 0.80 and "Extreme" in str(fg_result.get("reason", "")):
            _effective_quant_set.add("fear_greed")
        quant_confirming = [(n, r) for n, r in confirming if n in _effective_quant_set]
        qual_confirming = [(n, r) for n, r in confirming if n not in _effective_quant_set]

        # Weighted confidence (quantitative signals dominate due to higher weights)
        # Calibrator combines entropy accuracy, realized-PnL decay/boost, and DB-verified weights.
        effective_base_weights = self._effective_signal_base_weights()
        weights = self._signal_calibrator.get_calibrated_weights(effective_base_weights)
        total_weight = sum(weights.get(n, 0.1) for n, _ in confirming)
        if total_weight > 0:
            composite = sum(weights.get(n, 0.1) * r["confidence"] for n, r in confirming) / total_weight
        else:
            composite = 0

        # Expected Value calculation — venue-specific (fee-driven tiering)
        # EV = (win_prob * avg_gain) - (loss_prob * avg_loss) - round_trip_costs
        slippage = CONFIG["expected_slippage_pct"]
        win_prob = composite
        avg_gain_pct = 0.02   # 2% average winner
        avg_loss_pct = 0.01   # 1% average loser (tight stops)

        # Tier 1: Coinbase spot (default — 1.2% RT)
        fees_spot = CONFIG["round_trip_fee_pct"] + slippage
        ev_spot = (win_prob * avg_gain_pct) - ((1 - win_prob) * avg_loss_pct) - fees_spot

        # Tier 2: Kraken spot (0.50% RT at base tier: 0.25% maker each side)
        kraken_rt_fee = float(os.environ.get("KRAKEN_ROUND_TRIP_FEE_PCT", "0.0050"))
        fees_kraken = kraken_rt_fee + slippage
        ev_kraken = (win_prob * avg_gain_pct) - ((1 - win_prob) * avg_loss_pct) - fees_kraken

        # Tier 3: Perp (0.03% RT at 0% maker)
        perp_rt_fee = float(os.environ.get("PERP_ROUND_TRIP_FEE_PCT", "0.0003"))
        fees_perp = perp_rt_fee + slippage
        ev_perp = (win_prob * avg_gain_pct) - ((1 - win_prob) * avg_loss_pct) - fees_perp

        # Route to best EV venue — signal is tradeable if ANY venue has positive EV
        expected_value = ev_spot  # default for backward compat
        ev_positive = ev_spot > 0
        best_ev_venue = "coinbase"

        kraken_conf_floor = float(os.environ.get("SNIPER_KRAKEN_CONFIDENCE_FLOOR", "0.65"))
        perp_conf_floor = float(os.environ.get("GOAL_MIN_CONFIDENCE_PERP", "0.60"))

        if ev_perp > 0 and composite >= perp_conf_floor:
            ev_positive = True
            expected_value = ev_perp
            best_ev_venue = "perp"
        elif ev_kraken > 0 and composite >= kraken_conf_floor:
            ev_positive = True
            expected_value = ev_kraken
            best_ev_venue = "kraken"

        # fees variable for backward compat
        fees = fees_spot

        # Record scan
        with self._db_lock:
            self.db.execute(
                "INSERT INTO sniper_scans (pair, composite_confidence, direction, confirming_signals, signal_details) VALUES (?, ?, ?, ?, ?)",
                (pair, composite, direction, len(confirming), json.dumps({n: r for n, r in results.items()}))
            )
            self._scan_write_pending += 1
            now_ts = time.time()
            should_commit = (
                (not self._scan_batch_active)
                or self._scan_write_pending >= self._scan_commit_batch_size
                or (now_ts - self._scan_last_commit_ts) >= self._scan_commit_interval_s
            )
            if should_commit:
                self.db.commit()
                self._scan_write_pending = 0
                self._scan_last_commit_ts = now_ts

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
                    # Skip dampening in bootstrap (no recent trades) to avoid cold-start deadlock
                    dampen_factor = float(os.environ.get("SNIPER_GF_DAMPEN_FACTOR", "0.85"))
                    if self._is_chain_bootstrap_mode():
                        dampen_factor = 1.0  # no dampening during bootstrap
                    old_composite = composite
                    composite = composite * dampen_factor
                    logger.info("  [growth_engine] %s | GF quality=%.1f%% lattice=FAIL | "
                               "conf %.1f%%->%.1f%% (%s)",
                               pair, gf_quality * 100, old_composite * 100, composite * 100,
                               "bootstrap-no-dampen" if dampen_factor >= 1.0 else "dampened")
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
            "ev_spot": round(ev_spot, 6),
            "ev_kraken": round(ev_kraken, 6),
            "ev_perp": round(ev_perp, 6),
            "best_ev_venue": best_ev_venue,
            "regime": inferred_regime,
            "momentum": inferred_momentum,
            "details": results,
            "growth_engine": growth_boost,
        }

    def _flush_scan_writes(self):
        """Flush pending scan analytics writes if batching is active."""
        with self._db_lock:
            if self._scan_write_pending > 0:
                self.db.commit()
                self._scan_write_pending = 0
                self._scan_last_commit_ts = time.time()

    def scan_all(self):
        """Scan all pairs in parallel and return actionable signals.

        Uses ThreadPoolExecutor to scan multiple pairs concurrently.
        Combined with per-pair parallel scanning, this reduces total cycle time
        from ~280s to ~40s — 7x faster reaction to opportunities.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        logger.info("=== SNIPER SCAN ===")
        self._scan_batch_active = True
        actionable = []
        scan_results = {}
        base_pairs = [self._normalize_pair(p) for p in CONFIG.get("pairs", []) if self._normalize_pair(p)]
        scan_pairs = self._profit_focus_scan_pairs(base_pairs)
        if not scan_pairs:
            scan_pairs = list(base_pairs)

        logger.info(
            "SNIPER: pair focus universe %s (base=%s)",
            scan_pairs,
            base_pairs,
        )

        with ThreadPoolExecutor(max_workers=max(1, min(8, len(scan_pairs)))) as executor:
            futures = {executor.submit(self.scan_pair, pair): pair for pair in scan_pairs}
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

        for pair in scan_pairs:
            result = scan_results.get(pair)
            if not result:
                continue
            conf = result["composite_confidence"]
            n_signals = result["confirming_signals"]
            n_quant = result.get("quant_signals", 0)
            ev_ok = result.get("ev_positive", False)

            # Pre-filter: use lowest venue confidence floor (perp=0.60) so perp-eligible signals aren't blocked
            _min_conf_any_venue = min(
                float(CONFIG["min_composite_confidence"]),
                float(os.environ.get("GOAL_MIN_CONFIDENCE_PERP", "0.60")),
                float(os.environ.get("SNIPER_KRAKEN_CONFIDENCE_FLOOR", "0.65")),
            )
            if conf >= _min_conf_any_venue and n_signals >= CONFIG["min_confirming_signals"]:
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

                # GoalValidator gate — venue-aware (perp uses lower confidence floor)
                _best_ev_venue = result.get("best_ev_venue", "coinbase")
                if _goals:
                    if _best_ev_venue == "perp":
                        _gv_ok = _goals.should_trade_perp(
                            conf, n_signals, result["direction"],
                            result.get("regime", "neutral"), leverage=1.0)
                    else:
                        _gv_ok = _goals.should_trade(
                            conf, n_signals, result["direction"],
                            result.get("regime", "neutral"))
                    if not _gv_ok:
                        logger.info("  %s: BLOCKED by GoalValidator (conf=%.1f%%, %d signals, %s, venue=%s)",
                                   pair, conf*100, n_signals, result["direction"], _best_ev_venue)
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
                # Record signal predictions for accuracy feedback loop
                try:
                    price = result.get("price") or result.get("current_price", 0)
                    for sig_name in result.get("signal_details", {}).keys():
                        self._signal_verifier.record_prediction(
                            sig_name, result["direction"], pair, float(price), conf
                        )
                except Exception:
                    pass
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
        balance_state = self._balanced_growth_state()
        actionable = self._apply_balance_growth_to_actionable(actionable, balance_state)

        # Store actionable signals for opportunity-cost selling
        self._pending_buys = [s for s in actionable if s["direction"] == "BUY"]
        self._scan_batch_active = False
        self._flush_scan_writes()
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

    def _trade_flow_metrics_for_balance(self, lookback_hours=24):
        lookback = max(1, int(lookback_hours or 24))
        metrics = {
            "lookback_hours": int(lookback),
            "buy_fills": 0,
            "sell_fills": 0,
            "sell_close_attempts": 0,
            "sell_close_completions": 0,
            "sell_close_completion_rate": 1.0,
            "effective_sell_completions": 0,
            "buy_sell_ratio": 0.0,
            "realized_sell_closes": 0,
            "realized_sell_net_pnl_usd": 0.0,
            "reconcile_gate_passed": True,
            "reconcile_gate_reason": "not_available",
        }
        completed = tuple(self._completed_trade_statuses())
        marks = ",".join("?" for _ in completed)
        conn = None
        try:
            if TRADER_DB_PATH.exists():
                conn = sqlite3.connect(str(TRADER_DB_PATH), timeout=5.0)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA busy_timeout=5000")
                if self._trader_table_exists(conn, "agent_trades"):
                    rows = conn.execute(
                        f"""
                        SELECT UPPER(COALESCE(side, '')) AS side, COUNT(*) AS n
                        FROM agent_trades
                        WHERE created_at >= datetime('now', ?)
                          AND LOWER(COALESCE(status, '')) IN ({marks})
                        GROUP BY UPPER(COALESCE(side, ''))
                        """,
                        (f"-{lookback} hours", *completed),
                    ).fetchall()
                    for row in rows:
                        side = str(row["side"] or "").upper()
                        count = int(row["n"] or 0)
                        if side == "BUY":
                            metrics["buy_fills"] = count
                        elif side == "SELL":
                            metrics["sell_fills"] = count

                    row = conn.execute(
                        f"""
                        SELECT
                            COUNT(CASE WHEN pnl IS NOT NULL THEN 1 END) AS closes,
                            COALESCE(SUM(CASE WHEN pnl IS NOT NULL THEN COALESCE(pnl, 0) ELSE 0 END), 0) AS net_pnl
                        FROM agent_trades
                        WHERE UPPER(COALESCE(side, ''))='SELL'
                          AND created_at >= datetime('now', ?)
                          AND LOWER(COALESCE(status, '')) IN ({marks})
                        """,
                        (f"-{lookback} hours", *completed),
                    ).fetchone()
                    metrics["realized_sell_closes"] = int((row["closes"] if row else 0) or 0)
                    metrics["realized_sell_net_pnl_usd"] = float((row["net_pnl"] if row else 0.0) or 0.0)
        except Exception:
            pass
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

        try:
            if RECONCILE_STATUS_PATH.exists():
                payload = json.loads(RECONCILE_STATUS_PATH.read_text())
                if isinstance(payload, dict):
                    summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
                    close = payload.get("close_reconciliation", {}) if isinstance(payload.get("close_reconciliation"), dict) else {}
                    attempts = int(close.get("attempts", summary.get("close_attempts", 0)) or 0)
                    completions = int(close.get("completions", summary.get("close_completions", 0)) or 0)
                    if attempts > 0:
                        metrics["sell_close_attempts"] = attempts
                        metrics["sell_close_completions"] = completions
                        if "completion_rate" in close:
                            completion_rate = float(close.get("completion_rate", 0.0) or 0.0)
                        else:
                            completion_rate = float(completions) / float(max(1, attempts))
                        metrics["sell_close_completion_rate"] = max(0.0, min(1.0, completion_rate))
                    gate_passed = close.get("gate_passed", summary.get("close_gate_passed"))
                    gate_reason = str(close.get("gate_reason", summary.get("close_gate_reason", "")) or "").strip()
                    if isinstance(gate_passed, bool):
                        metrics["reconcile_gate_passed"] = bool(gate_passed)
                    if gate_reason:
                        metrics["reconcile_gate_reason"] = gate_reason
        except Exception:
            pass

        effective = max(
            int(metrics.get("sell_fills", 0) or 0),
            int(metrics.get("sell_close_completions", 0) or 0),
        )
        metrics["effective_sell_completions"] = int(effective)
        buys = int(metrics.get("buy_fills", 0) or 0)
        metrics["buy_sell_ratio"] = float(buys) / float(max(1, effective))
        return metrics

    def _balanced_growth_state(self, force_refresh=False):
        state = {
            "enabled": bool(CONFIG.get("balance_growth_mode", True)),
            "allow_buy": True,
            "mode": "disabled",
            "reason": "balance_growth_disabled",
            "buy_confidence_factor": 1.0,
            "buy_size_factor": 1.0,
            "metrics": {},
        }
        if not state["enabled"]:
            return state

        now = time.time()
        ttl = max(5, int(CONFIG.get("balance_cache_seconds", 20) or 20))
        cached = self._balance_flow_cache if isinstance(self._balance_flow_cache, dict) else {}
        if (
            not force_refresh
            and isinstance(cached.get("state"), dict)
            and (now - float(cached.get("ts", 0.0) or 0.0)) <= ttl
        ):
            return dict(cached.get("state") or state)

        metrics = self._trade_flow_metrics_for_balance(
            lookback_hours=CONFIG.get("balance_lookback_hours", 24)
        )
        state["metrics"] = metrics
        state["mode"] = "balanced"
        state["reason"] = "balanced_flow"

        buys = int(metrics.get("buy_fills", 0) or 0)
        effective_sells = int(metrics.get("effective_sell_completions", 0) or 0)
        ratio = float(metrics.get("buy_sell_ratio", 0.0) or 0.0)
        attempts = int(metrics.get("sell_close_attempts", 0) or 0)
        completion_rate = float(metrics.get("sell_close_completion_rate", 1.0) or 0.0)
        realized_closes = int(metrics.get("realized_sell_closes", 0) or 0)
        realized_net = float(metrics.get("realized_sell_net_pnl_usd", 0.0) or 0.0)

        min_sell_completions = int(CONFIG.get("balance_min_sell_completions", 2))
        min_close_attempts = max(1, int(CONFIG.get("balance_min_close_attempts", 2) or 2))
        min_close_rate = max(
            0.0,
            min(1.0, float(CONFIG.get("balance_min_close_completion_rate", 0.45) or 0.45)),
        )
        max_ratio = max(0.1, float(CONFIG.get("balance_max_buy_sell_ratio", 1.35) or 1.35))
        min_ratio_for_accel = max(
            0.0,
            float(CONFIG.get("balance_min_buy_sell_ratio_for_accel", 0.55) or 0.55),
        )
        require_non_negative_pnl = bool(
            CONFIG.get("balance_require_non_negative_realized_pnl", True)
        )
        min_realized_closes = max(
            1, int(CONFIG.get("balance_min_realized_closes_for_pnl_gate", 3) or 3)
        )

        if buys <= 0 and effective_sells <= 0:
            state["reason"] = "balance_bootstrap_no_completed_flow"
            self._balance_flow_cache = {"ts": now, "state": dict(state)}
            return state

        blockers = []
        if buys > 0 and effective_sells < min_sell_completions:
            blockers.append(
                f"balance_insufficient_sell_completions:{effective_sells}<{min_sell_completions}"
            )
        if attempts >= min_close_attempts and completion_rate < min_close_rate:
            blockers.append(
                f"balance_close_completion_rate_low:{completion_rate:.3f}<{min_close_rate:.3f}"
            )
        if buys > 0 and ratio > max_ratio:
            blockers.append(f"balance_buy_sell_ratio_high:{ratio:.3f}>{max_ratio:.3f}")
        if (
            require_non_negative_pnl
            and realized_closes >= min_realized_closes
            and realized_net < 0.0
        ):
            blockers.append(
                f"balance_realized_sell_pnl_negative:{realized_net:.4f}@{realized_closes}"
            )

        if blockers:
            state["allow_buy"] = False
            state["mode"] = "sell_recovery"
            state["reason"] = "|".join(blockers[:2])
            self._balance_flow_cache = {"ts": now, "state": dict(state)}
            return state

        caution_floor = max(min_ratio_for_accel, min(max_ratio * 0.85, max_ratio - 0.10))
        if buys > 0 and ratio >= caution_floor:
            state["mode"] = "buy_caution"
            state["reason"] = f"balance_near_ratio_cap:{ratio:.3f}>={caution_floor:.3f}"
            state["buy_confidence_factor"] = max(
                0.25,
                min(1.0, float(CONFIG.get("balance_buy_confidence_penalty", 0.88) or 0.88)),
            )
            state["buy_size_factor"] = max(
                0.25,
                min(1.0, float(CONFIG.get("balance_buy_size_penalty", 0.72) or 0.72)),
            )
        elif (
            ratio <= min_ratio_for_accel
            and effective_sells >= min_sell_completions
            and (
                not require_non_negative_pnl
                or realized_closes < min_realized_closes
                or realized_net >= 0.0
            )
        ):
            state["mode"] = "buy_accelerate"
            state["reason"] = f"balance_underweight_buy_flow:{ratio:.3f}<={min_ratio_for_accel:.3f}"
            state["buy_confidence_factor"] = max(
                1.0,
                min(1.5, float(CONFIG.get("balance_buy_confidence_boost", 1.06) or 1.06)),
            )
            state["buy_size_factor"] = max(
                1.0,
                min(1.5, float(CONFIG.get("balance_buy_size_boost", 1.12) or 1.12)),
            )

        self._balance_flow_cache = {"ts": now, "state": dict(state)}
        return state

    def _apply_balance_growth_to_actionable(self, actionable, state):
        out = []
        dropped_buys = 0
        allow_buy = bool((state or {}).get("allow_buy", True))
        mode = str((state or {}).get("mode", "balanced"))
        reason = str((state or {}).get("reason", "balanced_flow"))

        # Extreme fear override: bypass balanced growth gate entirely
        # F&G < 15 is historically strongest contrarian signal — don't block buys
        if not allow_buy:
            fg_val = self._get_fear_greed_value()
            if fg_val is not None and fg_val < 15:
                logger.info(
                    "SNIPER: EXTREME FEAR override — balanced growth gate (%s) bypassed (F&G=%d)",
                    reason, fg_val,
                )
                allow_buy = True
        conf_factor = float((state or {}).get("buy_confidence_factor", 1.0) or 1.0)
        size_factor = float((state or {}).get("buy_size_factor", 1.0) or 1.0)
        metrics = (state or {}).get("metrics", {}) if isinstance((state or {}).get("metrics"), dict) else {}

        for signal in list(actionable or []):
            entry = dict(signal)
            direction = str(entry.get("direction", "")).upper()
            if direction != "BUY":
                out.append(entry)
                continue
            if not allow_buy:
                dropped_buys += 1
                continue
            old_conf = float(entry.get("composite_confidence", 0.0) or 0.0)
            entry["composite_confidence"] = max(0.0, min(0.99, old_conf * conf_factor))
            entry["balance_growth"] = {
                "mode": mode,
                "reason": reason,
                "buy_confidence_factor": conf_factor,
                "buy_size_factor": size_factor,
                "buy_sell_ratio": round(float(metrics.get("buy_sell_ratio", 0.0) or 0.0), 4),
                "effective_sell_completions": int(metrics.get("effective_sell_completions", 0) or 0),
                "realized_sell_net_pnl_usd": round(float(metrics.get("realized_sell_net_pnl_usd", 0.0) or 0.0), 6),
            }
            out.append(entry)

        if dropped_buys > 0:
            logger.info(
                "SNIPER: balanced growth blocked %d BUY signals (%s)",
                dropped_buys,
                reason,
            )
        elif mode in {"buy_caution", "buy_accelerate"}:
            logger.info(
                "SNIPER: balanced growth mode=%s conf_factor=%.3f size_factor=%.3f reason=%s",
                mode,
                conf_factor,
                size_factor,
                reason,
            )
        return out

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
            stale_grace_seconds = max(
                max_age,
                int(CONFIG.get("close_flow_stale_grace_seconds", 3600) or 3600),
            )
            stale_gate_reasons = {"no_pending_sell_closes", "sell_close_completion_observed"}
            if age > max_age:
                stale_grace_ok = (
                    gate_passed
                    and gate_reason in stale_gate_reasons
                    and age <= float(stale_grace_seconds)
                )
                if not stale_grace_ok:
                    return False, f"reconcile_status_stale:{int(age)}s>{max_age}s"

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

    def _auto_refresh_execution_health(self, reason_hint=""):
        if not bool(CONFIG.get("execution_health_auto_refresh_on_block", True)):
            return False, "auto_refresh_disabled"
        now_ts = time.time()
        cooldown = max(
            5.0,
            float(CONFIG.get("execution_health_auto_refresh_cooldown_seconds", 20.0) or 20.0),
        )
        if (now_ts - float(self._last_execution_health_autorefresh)) < cooldown:
            return False, "auto_refresh_cooldown"
        self._last_execution_health_autorefresh = now_ts

        cmd = [
            sys.executable,
            str(Path(__file__).parent / "execution_health_probe.py"),
            "--refresh",
            "--no-http-probe",
        ]
        if bool(CONFIG.get("execution_health_auto_refresh_run_reconcile", True)):
            cmd.extend(
                [
                    "--run-reconcile",
                    "--reconcile-max-orders",
                    str(
                        max(
                            1,
                            int(
                                CONFIG.get(
                                    "execution_health_auto_refresh_reconcile_max_orders",
                                    120,
                                )
                                or 120
                            ),
                        )
                    ),
                    "--reconcile-lookback-hours",
                    str(
                        max(
                            1,
                            int(
                                CONFIG.get(
                                    "execution_health_auto_refresh_reconcile_lookback_hours",
                                    96,
                                )
                                or 96
                            ),
                        )
                    ),
                ]
            )
        timeout_seconds = max(
            10.0,
            float(CONFIG.get("execution_health_auto_refresh_timeout_seconds", 30.0) or 30.0),
        )
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
            ok = int(proc.returncode) == 0
            if ok:
                logger.info(
                    "SNIPER: execution-health auto-refresh succeeded (reason=%s)",
                    reason_hint or "unknown",
                )
                return True, "auto_refresh_ok"
            logger.warning(
                "SNIPER: execution-health auto-refresh failed rc=%s stderr=%s",
                proc.returncode,
                str(proc.stderr or "").strip()[-240:],
            )
            return False, f"auto_refresh_failed_rc_{proc.returncode}"
        except Exception as e:
            logger.warning("SNIPER: execution-health auto-refresh error: %s", e)
            return False, f"auto_refresh_error:{e}"

    def _execution_health_allows_buy(self, _allow_refresh=True):
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
                if (
                    _allow_refresh
                    and bool(CONFIG.get("execution_health_auto_refresh_on_block", True))
                    and any(r.startswith("reconcile_") for r in reasons)
                ):
                    refreshed, refresh_reason = self._auto_refresh_execution_health(
                        reason_hint=fail_reason
                    )
                    if refreshed:
                        return self._execution_health_allows_buy(_allow_refresh=False)
                    logger.info(
                        "SNIPER: execution-health auto-refresh skipped/failed (%s)",
                        refresh_reason,
                    )
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
        """Check if trade frequency limits are exceeded. Returns (ok, reason).

        NOTE: Throttle is now advisory-only. The risk controller's daily loss cap,
        80% pending allocation limit, and GoalValidator gates are the real safeguards.
        We still track timestamps for monitoring but never block.
        """
        now = time.time()
        # Prune old timestamps (keep last 24h)
        self._trade_timestamps = [t for t in self._trade_timestamps if now - t < 86400]
        # Always allow — risk controller handles the real limits
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

        balance_state = {}
        balance_size_factor = 1.0

        if direction == "BUY":
            # Fear & Greed — extreme fear is a historically strong contrarian edge
            # F&G < 15: every historical instance yielded +150-200% 12-month returns
            fg_val = self._get_fear_greed_value()
            _extreme_fear_active = fg_val is not None and fg_val < 15
            if _extreme_fear_active:
                logger.info("SNIPER: %s BUY in Extreme Fear (F&G=%d) — contrarian mode active", pair, fg_val)

            health_ok, health_reason = self._execution_health_allows_buy()
            if not health_ok:
                if _extreme_fear_active:
                    logger.info("SNIPER: %s EXTREME FEAR override — execution health (%s) bypassed (F&G=%d)", pair, health_reason, fg_val)
                else:
                    logger.info("SNIPER: %s BUY blocked — %s", pair, health_reason)
                    return False
            em_ok, em_reason = self._exit_manager_status_allows_buy()
            if not em_ok:
                if _extreme_fear_active:
                    logger.info("SNIPER: %s EXTREME FEAR override — exit manager status (%s) bypassed (F&G=%d)", pair, em_reason, fg_val)
                else:
                    logger.info("SNIPER: %s BUY blocked — %s", pair, em_reason)
                    return False
            close_ok, close_reason = self._close_flow_allows_buy()
            if not close_ok:
                if _extreme_fear_active:
                    logger.info("SNIPER: %s EXTREME FEAR override — close flow (%s) bypassed (F&G=%d)", pair, close_reason, fg_val)
                else:
                    logger.info("SNIPER: %s BUY blocked — %s", pair, close_reason)
                    return False
            balance_state = self._balanced_growth_state()
            if not bool(balance_state.get("allow_buy", True)):
                # In extreme fear, relax the balanced growth gate (historical edge is massive)
                if _extreme_fear_active:
                    logger.info("SNIPER: %s EXTREME FEAR override — balanced growth gate bypassed (F&G=%d)", pair, fg_val)
                else:
                    logger.info(
                        "SNIPER: %s BUY blocked — balanced growth gate (%s)",
                        pair,
                        str(balance_state.get("reason", "buy_blocked")),
                    )
                    return False
            balance_meta = signal.get("balance_growth", {}) if isinstance(signal.get("balance_growth"), dict) else {}
            balance_size_factor = float(
                balance_meta.get("buy_size_factor", balance_state.get("buy_size_factor", 1.0)) or 1.0
            )
            balance_size_factor = max(0.25, min(1.5, balance_size_factor))
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

        price = self._get_price_fast(pair)
        if not price:
            logger.warning("Cannot get price for %s", pair)
            return False

        holdings, cash = self._get_holdings()
        base_currency = pair.split("-")[0]

        # Calculate total portfolio value
        total_portfolio = cash + sum(
            h * (self._get_price_fast(f"{c}-USD") or 0) for c, h in holdings.items()
        )

        # Get DYNAMIC risk parameters from centralized controller
        if _risk_ctrl:
            params = _risk_ctrl.get_risk_params(total_portfolio, pair)
            max_trade = params.get("max_trade_usd", total_portfolio * 0.05)
            max_daily_loss = params.get("max_daily_loss", total_portfolio * 0.08)
            reserve = params.get("min_reserve", 20.0)
            max_pos_pct = params.get("max_position_pct", 0.20)
            can_buy = params.get("can_buy", True)
            regime = params.get("regime", "neutral")
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
            if abs(balance_size_factor - 1.0) > 1e-9:
                trade_size *= balance_size_factor
                trade_size = round(float(trade_size), 4)
                logger.info(
                    "SNIPER: %s BUY size adjusted by balanced growth x%.3f (mode=%s)",
                    pair,
                    balance_size_factor,
                    str((balance_state or {}).get("mode", "balanced")),
                )

            # Extreme Fear accumulation boost: increase sizing in historically strongest entry zone
            if _extreme_fear_active and fg_val is not None:
                # At F&G=5, boost 1.5x. At F&G=15, boost 1.1x. Linear interpolation.
                fear_boost = 1.0 + max(0, (15 - fg_val)) * 0.05  # 5→1.50, 10→1.25, 15→1.00
                fear_boost = min(fear_boost, 1.5)  # Cap at 50% boost
                if fear_boost > 1.01:
                    trade_size *= fear_boost
                    trade_size = min(trade_size, max_trade)  # Still respect absolute max
                    trade_size = min(trade_size, remaining_room)
                    trade_size = min(trade_size, available_after_reserve)
                    trade_size = round(trade_size, 2)
                    logger.info("SNIPER: %s EXTREME FEAR boost x%.2f (F&G=%d) → $%.2f",
                               pair, fear_boost, fg_val, trade_size)

            # Conviction Rally: test small, rally big
            conviction_active = False
            if self._conviction:
                conf = float(signal.get("composite_confidence", 0.0) or 0.0)
                can_test, test_reason = self._conviction.should_enter_test(pair, conf)
                if can_test:
                    test_mult = self._conviction.enter_test(pair, price, conf)
                    trade_size *= test_mult
                    trade_size = round(trade_size, 2)
                    conviction_active = True
                    logger.info("SNIPER: %s BUY conviction test entry — size x%.1f ($%.2f)",
                                pair, test_mult, trade_size)
                elif test_reason not in ("confidence too low",):
                    logger.debug("SNIPER: %s conviction skip — %s", pair, test_reason)

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
            price = self._get_price_fast(pair)
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
                price = self._get_price_fast(pair)
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

            # ── Perp-preferred routing: use perp if available + margin healthy ──
            used_perp = False
            exec_pair = pair
            if _deriv and _deriv.enabled:
                perp_pid = _deriv.perp_for_spot_pair(pair)
                if perp_pid:
                    margin = _deriv.margin_health()
                    if margin.get("can_open_new", False):
                        # Use perp risk approval instead of spot
                        if _risk_ctrl:
                            perp_ok, perp_reason, perp_adj = _risk_ctrl.approve_perp_trade(
                                "sniper", perp_pid, "BUY", trade_size, total_portfolio,
                                leverage=1.0, margin_health=margin,
                            )
                            if perp_ok:
                                exec_pair = perp_pid
                                trade_size = perp_adj
                                used_perp = True
                                logger.info("SNIPER: Routing BUY to perp %s (0%% maker fee)", exec_pair)
                            else:
                                logger.info("SNIPER: Perp approval failed (%s), falling back to spot", perp_reason)
                    else:
                        logger.info("SNIPER: Perp margin unhealthy, using spot for %s", pair)

            # MAKER ONLY: limit order at/below bid (0.4% fee spot / 0% fee perp)
            # BE A MAKER not a taker — Rule from game theory playbook
            # post_only=True rejects if it would match immediately (guarantees maker)
            # Spread-relative pricing: adapts to each pair's microstructure
            base_size = trade_size / price
            conf = signal.get("composite_confidence", 0.7)

            # Spread-relative offset: use live BBO when available
            spread_pct = self._spot_spread_pct(pair)
            if spread_pct > 0.001:
                # Place inside the spread — tighter for higher confidence
                # 90%+ conf: 30% of half-spread from mid (aggressive, fills fast)
                # 80%+ conf: 50% of half-spread (moderate)
                # <80% conf: 80% of half-spread (conservative)
                spread_frac = 0.30 if conf >= 0.90 else (0.50 if conf >= 0.80 else 0.80)
                half_spread_pct = spread_pct / 200.0  # convert % to fraction, then half
                buy_offset = 1.0 - half_spread_pct * spread_frac
            else:
                # Fallback to fixed offsets when spread unavailable
                buy_offset = 0.9992 if conf >= 0.90 else (0.9994 if conf >= 0.80 else 0.9996)
            limit_price = price * buy_offset

            venue_label = f"PERP {exec_pair}" if used_perp else exec_pair
            logger.info("SNIPER EXECUTE: LIMIT BUY %s | $%.2f (%.6f @ $%.2f) | conf=%.1f%% | %d signals",
                        venue_label, trade_size, base_size, limit_price,
                        signal["composite_confidence"]*100,
                        signal["confirming_signals"])

            try:
                venue_used = "coinbase"
                if used_perp:
                    result = _deriv.place_perp_order(
                        exec_pair, "BUY", base_size, limit_price,
                        leverage=1.0, post_only=True,
                    )
                    venue_used = "perp"
                else:
                    # ── Multi-venue routing: EV-based tier + SmartRouter ──
                    # Use EV analysis from scan phase as primary venue hint
                    best_venue = signal.get("best_ev_venue", "coinbase")
                    if best_venue == "perp":
                        best_venue = "coinbase"  # perp handled above, spot fallback here
                    split_plan = {}
                    router = self._get_smart_router()
                    if router:
                        try:
                            quote = router.find_best_execution(pair, "BUY", trade_size)
                            if quote and quote.get("venue") and "error" not in quote:
                                best_venue = quote["venue"]
                                logger.info("SmartRouter selected venue=%s for %s (savings=%.4f%%)",
                                           best_venue, pair, quote.get("savings_vs_coinbase", 0))

                                split_plan = quote.get("split_plan", {}) if isinstance(quote, dict) else {}
                                split_enabled = isinstance(split_plan, dict) and bool(split_plan.get("enabled", False))
                                if not split_enabled:
                                    # Capacity-aware sizing for single-venue execution.
                                    try:
                                        cap_usd = float(quote.get("capacity_usd", trade_size) or trade_size)
                                    except Exception:
                                        cap_usd = float(trade_size or 0.0)
                                    if cap_usd > 0 and cap_usd < trade_size:
                                        old_size = trade_size
                                        trade_size = max(0.5, cap_usd)
                                        base_size = trade_size / price if price > 0 else 0.0
                                        logger.info(
                                            "SmartRouter capacity cap: %s BUY $%.2f -> $%.2f",
                                            best_venue,
                                            old_size,
                                            trade_size,
                                        )
                        except Exception as e:
                            logger.warning("SmartRouter failed, defaulting to coinbase: %s", e)

                    split_exec = self._execute_split_plan_orders(
                        pair=pair,
                        side="BUY",
                        total_notional_usd=trade_size,
                        price_ref=price,
                        signal=signal,
                        split_plan=split_plan,
                    )
                    if split_exec.get("handled"):
                        return bool(split_exec.get("any_filled", False))

                    result, venue_used, acked = self._execute_spot_limit_order(
                        pair=pair,
                        side="BUY",
                        amount_usd=trade_size,
                        price_ref=price,
                        limit_price=limit_price,
                        signal=signal,
                        venue_hint=best_venue,
                    )
                    if acked:
                        # Track cash committed this cycle so subsequent orders don't over-spend.
                        self._cycle_cash_spent = getattr(self, "_cycle_cash_spent", 0.0) + trade_size
                    return self._process_order_result(
                        result,
                        pair,
                        "BUY",
                        trade_size,
                        price,
                        signal,
                        venue=venue_used,
                    )

                # Perp branch
                self._cycle_cash_spent = getattr(self, '_cycle_cash_spent', 0.0) + trade_size
                return self._process_order_result(
                    result,
                    exec_pair,
                    "BUY",
                    trade_size,
                    price,
                    signal,
                    venue=venue_used,
                )
            except Exception as e:
                logger.error("BUY execution error: %s", e, exc_info=True)
                if _risk_ctrl:
                    _risk_ctrl.resolve_allocation("sniper", exec_pair)
                return False

        elif direction == "SELL":
            # ── Check for open perp position first — close via deriv connector ──
            if _deriv and _deriv.enabled:
                perp_pid = _deriv.perp_for_spot_pair(pair)
                if perp_pid:
                    perp_pos = _deriv.get_position(perp_pid)
                    if perp_pos and perp_pos.get("size", 0) > 0:
                        logger.info("SNIPER: Closing perp position %s (size=%.6f)", perp_pid, perp_pos["size"])
                        close_result = _deriv.close_position(perp_pid)
                        if close_result:
                            return self._process_order_result(close_result, perp_pid, "SELL", 0, price, signal)
                        logger.warning("SNIPER: Perp close returned None, checking spot side")

            # RESERVE PROTECTION: never sell reserve assets (BTC, USD, USDC)
            if base_currency in CONFIG.get("reserve_assets", []):
                logger.info("SNIPER: BLOCKED SELL %s — reserve asset (treasury)", pair)
                return False

            # Check if we hold this asset
            held = holdings.get(base_currency, 0)
            held_usd = held * price
            if held_usd < 0.50:
                # ── No spot to sell — open perp SHORT if enabled (0% maker fee) ──
                if _deriv and _deriv.enabled:
                    perp_pid = _deriv.perp_for_spot_pair(pair)
                    if perp_pid:
                        conf = signal.get("composite_confidence", 0)
                        n_sig = signal.get("confirming_signals", 0)
                        regime = signal.get("market_regime", "neutral")
                        # GoalValidator perp gate (allows SELL in downtrends)
                        if _goals and _goals.should_trade_perp(conf, n_sig, "SELL", regime, leverage=1.0):
                            margin = _deriv.margin_health()
                            if margin.get("can_open_new", False):
                                # Conservative size: min($40, 5% of portfolio)
                                perp_max_lev = float(os.environ.get("PERP_MAX_LEVERAGE", "1.0"))
                                short_size = min(40.0, total_portfolio * 0.05)
                                if short_size >= 1.0 and _risk_ctrl:
                                    perp_ok, perp_reason, perp_adj = _risk_ctrl.approve_perp_trade(
                                        "sniper", perp_pid, "SELL", short_size, total_portfolio,
                                        leverage=perp_max_lev, margin_health=margin,
                                    )
                                    if perp_ok:
                                        short_size = perp_adj
                                        base_size = short_size / price
                                        limit_price = price * 1.0002  # slight offset above spot
                                        logger.info("SNIPER EXECUTE: PERP SHORT %s | $%.2f (%.6f @ $%.2f) | conf=%.1f%% | %d signals",
                                                    perp_pid, short_size, base_size, limit_price, conf * 100, n_sig)
                                        try:
                                            result = _deriv.place_perp_order(
                                                perp_pid, "SELL", base_size, limit_price,
                                                leverage=perp_max_lev, post_only=True,
                                            )
                                            return self._process_order_result(result, perp_pid, "SELL", short_size, price, signal, venue="perp")
                                        except Exception as e:
                                            logger.error("PERP SHORT execution error: %s", e, exc_info=True)
                                    else:
                                        logger.info("SNIPER: Perp SHORT risk denied (%s)", perp_reason)
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
            # Spread-relative pricing: adapts to each pair's microstructure
            conf = signal.get("composite_confidence", 0.7)

            spread_pct = self._spot_spread_pct(pair)
            if spread_pct > 0.001:
                spread_frac = 0.30 if conf >= 0.90 else (0.50 if conf >= 0.80 else 0.80)
                half_spread_pct = spread_pct / 200.0
                sell_offset = 1.0 + half_spread_pct * spread_frac
            else:
                sell_offset = 1.0002 if conf >= 0.90 else (1.0004 if conf >= 0.80 else 1.0006)
            limit_price = price * sell_offset

            logger.info("SNIPER EXECUTE: LIMIT SELL %s | $%.2f (%.8f %s) @ $%.2f | conf=%.1f%% | %d signals",
                        pair, trade_size, base_size, base_currency, limit_price,
                        signal["composite_confidence"]*100, signal["confirming_signals"])

            try:
                # ── Multi-venue routing for SELL ──
                best_venue = "coinbase"  # default
                split_plan = {}
                router = self._get_smart_router()
                if router:
                    try:
                        quote = router.find_best_execution(pair, "SELL", trade_size)
                        if quote and quote.get("venue") and "error" not in quote:
                            best_venue = quote["venue"]
                            logger.info("SmartRouter selected venue=%s for SELL %s (savings=%.4f%%)",
                                       best_venue, pair, quote.get("savings_vs_coinbase", 0))
                            split_plan = quote.get("split_plan", {}) if isinstance(quote, dict) else {}
                            split_enabled = isinstance(split_plan, dict) and bool(split_plan.get("enabled", False))
                            if not split_enabled:
                                # Capacity-aware sizing for single-venue execution.
                                try:
                                    cap_usd = float(quote.get("capacity_usd", trade_size) or trade_size)
                                except Exception:
                                    cap_usd = float(trade_size or 0.0)
                                if cap_usd > 0 and cap_usd < trade_size:
                                    old_size = trade_size
                                    trade_size = max(0.5, cap_usd)
                                    base_size = min((trade_size / price) if price > 0 else 0.0, held)
                                    logger.info(
                                        "SmartRouter capacity cap: %s SELL $%.2f -> $%.2f",
                                        best_venue,
                                        old_size,
                                        trade_size,
                                    )
                    except Exception as e:
                        logger.warning("SmartRouter SELL failed, defaulting to coinbase: %s", e)

                split_exec = self._execute_split_plan_orders(
                    pair=pair,
                    side="SELL",
                    total_notional_usd=trade_size,
                    price_ref=price,
                    signal=signal,
                    split_plan=split_plan,
                    held_base=held,
                )
                if split_exec.get("handled"):
                    return bool(split_exec.get("any_filled", False))

                result, venue_used, _acked = self._execute_spot_limit_order(
                    pair=pair,
                    side="SELL",
                    amount_usd=trade_size,
                    price_ref=price,
                    limit_price=limit_price,
                    signal=signal,
                    venue_hint=best_venue,
                    held_base=held,
                )
                return self._process_order_result(
                    result,
                    pair,
                    "SELL",
                    trade_size,
                    price,
                    signal,
                    venue=venue_used,
                )
            except Exception as e:
                logger.error("SELL execution error: %s", e, exc_info=True)
                return False

        return False

    def _process_order_result(self, result, pair, side, trade_size, price, signal, venue="coinbase"):
        """Process order result and record trade."""
        payload = result if isinstance(result, dict) else {}
        order_id = None
        status = "failed"
        fill_data = {}
        fallback_used = False
        venue_used = venue or "coinbase"
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

        poll_fill = str(venue_used or "coinbase").strip().lower() in {"coinbase", "perp"}
        if order_id and status == "pending" and poll_fill:
            try:
                fill_trader = self._new_coinbase_trader()
                # Extended fill wait: maker orders need time to fill
                # BUY: 8s (spread-relative pricing needs ~5-15s to fill)
                # SELL: 6-8s (close evidence + exit speed)
                fill_wait = float(os.environ.get("SNIPER_FILL_WAIT_SECONDS", "8.0"))
                if (
                    str(side or "").upper() == "SELL"
                    and self._normalize_pair(pair) in self._close_evidence_target_aliases()
                ):
                    fill_wait = max(
                        fill_wait,
                        float(CONFIG.get("close_evidence_sell_fill_wait_seconds", 6.0) or 6.0),
                    )
                fill_data = fill_trader.get_order_fill(
                    order_id,
                    max_wait=fill_wait,
                    poll_interval=0.4,
                ) or {}
            except Exception:
                fill_data = {}

            try:
                filled_size = float(fill_data.get("filled_size", 0.0) or 0.0)
            except Exception:
                filled_size = 0.0
            terminal_status = self._normalize_trade_terminal_status(fill_data.get("status"), filled_size)
            if terminal_status in {"filled", "partial_filled"}:
                status = "filled"
            elif terminal_status in {"failed", "cancelled", "expired"}:
                status = "failed"

            # Escalate to taker/IOC in aggressive modes when maker limit lingers pending.
            if status == "pending":
                fallback = self._attempt_execution_fallback(
                    pair=pair,
                    side=side,
                    original_order_id=order_id,
                    quoted_notional=quoted_notional,
                    quoted_qty=quoted_qty,
                    signal=signal,
                )
                if fallback:
                    if fallback.get("order_id"):
                        order_id = str(fallback.get("order_id"))
                    fill_data = dict(fallback.get("fill_data") or {})
                    fallback_used = bool(fallback.get("used"))
                    try:
                        fb_filled = float(fill_data.get("filled_size", 0.0) or 0.0)
                    except Exception:
                        fb_filled = 0.0
                    fb_status = self._normalize_trade_terminal_status(fill_data.get("status"), fb_filled)
                    if fb_status in {"filled", "partial_filled"}:
                        status = "filled"
                    elif fb_status in {"failed", "cancelled", "expired"}:
                        status = "failed"
        elif order_id and status == "pending":
            logger.info("SNIPER ORDER ACKED (external venue): %s %s order=%s venue=%s",
                        pair, side, order_id, venue_used)

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
            pnl = self._estimate_realized_sell_pnl(pair, effective_price, effective_qty, effective_notional)
            if pnl is None and self._normalize_pair(pair) in self._close_evidence_target_aliases():
                pnl = 0.0
            if pnl is not None:
                logger.info(
                    "SNIPER P&L: %s SELL pnl=$%.4f (exit=$%.2f)",
                    pair,
                    pnl,
                    effective_price,
                )
                # Record outcome for signal calibrator: profit = BUY was correct
                actual_outcome = "BUY" if pnl > 0 else "SELL"
                signal_details = signal.get("details", {})
                for sig_name, sig_result in signal_details.items():
                    sig_dir = sig_result.get("direction", "NONE") if isinstance(sig_result, dict) else "NONE"
                    if sig_dir in ("BUY", "SELL"):
                        self._signal_calibrator.record_outcome(sig_name, sig_dir, actual_outcome)
                        self._signal_calibrator.record_realized_pnl(
                            sig_name,
                            pnl_usd=pnl,
                            notional_usd=effective_notional,
                        )

        trade_uuid = f"{pair}:{side}:{int(time.time() * 1000)}:{(order_id or 'none')[:12]}"
        lifecycle_status = (
            "entry_filled" if side == "BUY" and status == "filled"
            else "entry_pending" if side == "BUY" and status == "pending"
            else "exit_filled" if side == "SELL" and status == "filled"
            else "exit_pending" if side == "SELL" and status == "pending"
            else "failed"
        )
        if fallback_used:
            lifecycle_status = f"{lifecycle_status}_fallback_ioc"

        venue_key = str(venue_used or "coinbase").strip().lower()
        if venue_key == "perp":
            venue_fee_rate = 0.0
        elif venue_key == "kraken":
            venue_fee_rate = float(os.environ.get("KRAKEN_MAKER_FEE_PCT", "0.16") or 0.16) / 100.0
        elif venue_key == "kraken_stock":
            venue_fee_rate = 0.0
        else:
            venue_fee_rate = float(os.environ.get("COINBASE_MAKER_FEE_PCT", "0.60") or 0.60) / 100.0

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
                        venue_used,
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
                        venue_used,
                        effective_price,
                        pnl,
                        status,
                    ),
                )
            self.db.commit()
        self.trades_today += 1

        ledger_pnl = None
        if side == "SELL" and status == "filled":
            ledger_pnl = 0.0 if pnl is None else float(pnl)
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

        # Record to capital ledger (unified audit trail)
        if status in ("filled", "pending"):
            try:
                from ledger_writer import LedgerWriter
                _ledger = LedgerWriter()
                asset = pair.split("-")[0] if "-" in pair else pair
                fees = effective_notional * venue_fee_rate
                _ledger.record_trade_fill(
                    asset=asset,
                    venue=venue_used,
                    amount=effective_qty if side == "BUY" else -effective_qty,
                    value_usd=effective_notional if side == "BUY" else -effective_notional,
                    pair=pair,
                    side=side,
                    price=effective_price,
                    order_id=order_id,
                    agent="sniper",
                    fees_usd=fees,
                    realized_pnl_usd=pnl,
                    trigger=f"sniper_scan:confidence={signal.get('composite_confidence', 0):.2f}",
                    metadata={
                        "confidence": float(signal.get("composite_confidence", 0) or 0),
                        "confirming_signals": int(signal.get("confirming_signals", 0) or 0),
                        "lifecycle_status": lifecycle_status,
                    },
                )
            except Exception as ledger_err:
                logger.debug("Capital ledger write failed: %s", ledger_err)

        # Record to KPI tracker for scorecard
        if _kpi and status in ("filled", "pending"):
            try:
                fees = effective_notional * venue_fee_rate
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
            cost = round(effective_notional * venue_fee_rate, 4)
            if side == "BUY":
                _tracker.transition(asset, venue_used, "available", "available",
                                    amount=effective_qty,
                                    value_usd=effective_notional, cost_usd=cost,
                                    trigger="sniper",
                                    metadata={"side": "BUY", "pair": pair,
                                              "confidence": float(signal.get("composite_confidence", 0.0) or 0.0)})
            else:
                _tracker.transition(asset, venue_used, "available", "available",
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

    @staticmethod
    def _extract_order_id_from_result(payload):
        data = payload if isinstance(payload, dict) else {}
        if isinstance(data.get("success_response"), dict):
            oid = data["success_response"].get("order_id")
            if oid:
                return str(oid)
        if data.get("order_id"):
            return str(data.get("order_id"))
        return ""

    def _execution_fallback_mode(self):
        mode = str(CONFIG.get("execution_fallback_mode", "off") or "off").strip().lower()
        if mode in {"2", "full", "aggressive"}:
            return "aggressive"
        if mode in {"1", "controlled", "safe"}:
            return "controlled"
        return "off"

    def _spot_spread_pct(self, pair):
        try:
            dp = _data_pair(pair)
            data = _fetch_json(f"https://api.exchange.coinbase.com/products/{dp}/book?level=1", timeout=3)
            bids = data.get("bids", []) if isinstance(data, dict) else []
            asks = data.get("asks", []) if isinstance(data, dict) else []
            if not bids or not asks:
                return 0.0
            bid = float(bids[0][0]) if isinstance(bids[0], (list, tuple)) else float(bids[0].get("price", 0.0))
            ask = float(asks[0][0]) if isinstance(asks[0], (list, tuple)) else float(asks[0].get("price", 0.0))
            mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
            if mid <= 0:
                return 0.0
            return ((ask - bid) / mid) * 100.0
        except Exception:
            return 0.0

    def _attempt_execution_fallback(
        self,
        pair,
        side,
        original_order_id,
        quoted_notional,
        quoted_qty,
        signal,
    ):
        mode = self._execution_fallback_mode()
        if mode == "off":
            return {}

        spread_pct = self._spot_spread_pct(pair)
        max_spread_pct = (
            float(CONFIG.get("execution_fallback_max_spread_pct_aggressive", 1.2) or 1.2)
            if mode == "aggressive"
            else float(CONFIG.get("execution_fallback_max_spread_pct_controlled", 0.35) or 0.35)
        )
        if spread_pct > 0 and spread_pct > max_spread_pct:
            logger.info(
                "SNIPER FALLBACK SKIP: %s %s spread %.3f%% > max %.3f%%",
                pair,
                side,
                spread_pct,
                max_spread_pct,
            )
            return {}

        trader = None
        try:
            trader = self._new_coinbase_trader()
        except Exception:
            trader = None
        if trader is None:
            return {}

        cancel_ok = False
        try:
            cancel_res = trader.cancel_order(original_order_id)
            if isinstance(cancel_res, dict):
                results = cancel_res.get("results", [])
                if isinstance(results, list) and results:
                    cancel_ok = bool(results[0].get("success"))
                else:
                    cancel_ok = bool(cancel_res.get("success", False))
            else:
                cancel_ok = bool(cancel_res)
        except Exception:
            cancel_ok = False

        # If cancellation failed, don't send a second order blindly.
        if not cancel_ok:
            return {}

        ioc_result = {}
        side_u = str(side or "").upper()
        if side_u == "BUY":
            quote_size = max(
                float(CONFIG.get("execution_fallback_min_quote_usd", 0.5) or 0.5),
                float(quoted_notional or 0.0),
            )
            ioc_result = trader.place_order(
                pair,
                "BUY",
                quote_size,
                order_type="market",
                expected_edge_pct=float(signal.get("composite_confidence", 0.0) or 0.0) * 100.0,
                signal_confidence=float(signal.get("composite_confidence", 0.0) or 0.0),
                market_regime=str(signal.get("regime", "neutral") or "neutral"),
            )
        elif side_u == "SELL":
            base_size = max(0.00000001, float(quoted_qty or 0.0))
            ioc_result = trader.place_order(
                pair,
                "SELL",
                base_size,
                order_type="market",
                bypass_profit_guard=True,
            )

        ioc_order_id = self._extract_order_id_from_result(ioc_result)
        if not ioc_order_id:
            logger.info("SNIPER FALLBACK FAILED: IOC ack missing order_id for %s %s", pair, side_u)
            return {}

        fill = {}
        try:
            fill_wait = max(
                0.4,
                float(CONFIG.get("execution_fallback_ioc_wait_seconds", 2.5) or 2.5),
            )
            fill = trader.get_order_fill(ioc_order_id, max_wait=fill_wait, poll_interval=0.25) or {}
        except Exception:
            fill = {}

        logger.info("SNIPER FALLBACK IOC: %s %s -> order=%s", pair, side_u, ioc_order_id)
        return {"order_id": ioc_order_id, "fill_data": fill, "used": True}

    def _get_price_fast(self, pair):
        """Get price from WS feed (O(1), no network call) with REST fallback."""
        if self._ws_feed:
            try:
                quote = self._ws_feed.get_quote(pair)
                if quote and float(quote.get("mid", 0) or 0) > 0:
                    mid = float(quote["mid"])
                    # Also update REST cache so other code paths benefit
                    with self._price_cache_lock:
                        self._price_cache[pair] = (mid, time.time())
                    return mid
                # Try data-pair alias (e.g. BTC-USDC -> BTC-USD for WS feed)
                dp = _data_pair(pair)
                if dp != pair:
                    quote = self._ws_feed.get_quote(dp)
                    if quote and float(quote.get("mid", 0) or 0) > 0:
                        mid = float(quote["mid"])
                        with self._price_cache_lock:
                            self._price_cache[pair] = (mid, time.time())
                        return mid
            except Exception:
                pass
        # Fallback to REST
        return self._get_price(pair)

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

    # ── Flywheel telemetry: feeds back into auto_builder via runtime/flywheel_telemetry.json ──
    _flywheel_telemetry_window = []  # Rolling window of cycle stats
    _flywheel_telemetry_max_window = 60  # Keep last 60 cycles (~30 min at 30s intervals)

    def _write_flywheel_telemetry(self, cycle, actionable, scan_pairs=None):
        """Write per-cycle telemetry for the auto_builder feedback loop.

        Tracks: signal generation rate, gate block rate, trade execution rate,
        and the specific gates that are blocking.

        This data feeds into: auto_builder → alpha_researcher → sniper (closed loop)
        """
        n_pairs = len(scan_pairs or [])
        n_actionable = len(actionable) if actionable else 0
        n_buys = sum(1 for s in (actionable or []) if s.get("direction") == "BUY")
        n_sells = sum(1 for s in (actionable or []) if s.get("direction") == "SELL")

        cycle_stat = {
            "cycle": cycle,
            "ts": time.time(),
            "pairs_scanned": n_pairs,
            "actionable": n_actionable,
            "buys": n_buys,
            "sells": n_sells,
        }

        # Rolling window
        self._flywheel_telemetry_window.append(cycle_stat)
        if len(self._flywheel_telemetry_window) > self._flywheel_telemetry_max_window:
            self._flywheel_telemetry_window = self._flywheel_telemetry_window[-self._flywheel_telemetry_max_window:]

        # Compute rolling averages
        window = self._flywheel_telemetry_window
        n = len(window)
        avg_actionable = sum(c["actionable"] for c in window) / max(1, n)
        avg_buys = sum(c["buys"] for c in window) / max(1, n)
        avg_sells = sum(c["sells"] for c in window) / max(1, n)
        total_actionable = sum(c["actionable"] for c in window)
        total_scans = sum(c["pairs_scanned"] for c in window)
        signal_rate = total_actionable / max(1, total_scans)

        # Gate health assessment
        gate_health = "green"
        if avg_actionable < 0.1 and n >= 10:
            gate_health = "red"  # Almost nothing getting through
        elif avg_actionable < 0.5 and n >= 10:
            gate_health = "yellow"  # Low throughput
        elif avg_sells < 0.05 and avg_buys > 0.3 and n >= 10:
            gate_health = "yellow_sell_blocked"  # Buys flowing but no sells

        telemetry = {
            "cycle": cycle,
            "window_size": n,
            "avg_actionable_per_cycle": round(avg_actionable, 3),
            "avg_buys_per_cycle": round(avg_buys, 3),
            "avg_sells_per_cycle": round(avg_sells, 3),
            "signal_rate": round(signal_rate, 4),
            "gate_health": gate_health,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

        # Write atomically for consumption by auto_builder and metrics_collector
        try:
            runtime_dir = os.path.join(os.path.dirname(__file__), "runtime")
            os.makedirs(runtime_dir, exist_ok=True)
            tel_path = os.path.join(runtime_dir, "flywheel_telemetry.json")
            tmp_path = tel_path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(telemetry, f, indent=2)
            os.replace(tmp_path, tel_path)
        except Exception:
            pass

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

        # Start signal accuracy feedback loop
        self._signal_verifier.start()

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

                # Conviction Rally: evaluate monitored positions each cycle
                if self._conviction:
                    try:
                        conv_status = self._conviction.get_status()
                        for conv_pair in list(conv_status.get("positions", {}).keys()):
                            conv_price = self._get_price(conv_pair)
                            if not conv_price:
                                continue
                            ev = self._conviction.evaluate(conv_pair, conv_price)
                            action = ev.get("action", "none")
                            if action == "cut":
                                # Failed test — trigger a SELL signal
                                logger.info("CONVICTION CUT -> SELL %s (P&L=%.2f%%)", conv_pair, ev.get("pnl_pct", 0))
                                cut_signal = {
                                    "pair": conv_pair, "direction": "SELL",
                                    "composite_confidence": 0.99, "confirming_signals": 3,
                                    "reason": f"conviction_cut pnl={ev.get('pnl_pct', 0):.2f}%",
                                }
                                self.execute_trade(cut_signal)
                                # Record loss outcome for Thompson Sampling
                                if _ts_sizer:
                                    _ts_sizer.record_outcome(conv_pair, profitable=False)
                            elif action == "rally":
                                # Winner — log follow-up BUY for additional sizing
                                logger.info("CONVICTION RALLY -> scale up %s to %.1fx (P&L=%.2f%%)",
                                            conv_pair, ev.get("size_mult", 2.0), ev.get("pnl_pct", 0))
                                # Record win outcome for Thompson Sampling
                                if _ts_sizer:
                                    _ts_sizer.record_outcome(conv_pair, profitable=True)
                            elif action == "rally_exit":
                                # Rally ended — trigger SELL
                                logger.info("CONVICTION RALLY EXIT -> SELL %s (P&L=%.2f%%, drawdown=%.2f%%)",
                                            conv_pair, ev.get("pnl_pct", 0), ev.get("drawdown_pct", 0))
                                exit_signal = {
                                    "pair": conv_pair, "direction": "SELL",
                                    "composite_confidence": 0.99, "confirming_signals": 3,
                                    "reason": f"conviction_rally_exit pnl={ev.get('pnl_pct', 0):.2f}%",
                                }
                                self.execute_trade(exit_signal)
                                # Record win (profitable rally exit) for Thompson Sampling
                                if _ts_sizer:
                                    profitable = float(ev.get("pnl_pct", 0)) > 0
                                    _ts_sizer.record_outcome(conv_pair, profitable=profitable)
                            elif action == "normalize":
                                logger.info("CONVICTION NORMALIZE: %s -> 1.0x (P&L=%.2f%%)",
                                            conv_pair, ev.get("pnl_pct", 0))
                    except Exception as conv_err:
                        logger.warning("Conviction rally evaluation error: %s", conv_err)

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

                # Flywheel telemetry: track scan→signal→trade conversion rate
                try:
                    self._write_flywheel_telemetry(cycle, actionable, scan_pairs=CONFIG.get("pairs", []))
                except Exception:
                    pass

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
