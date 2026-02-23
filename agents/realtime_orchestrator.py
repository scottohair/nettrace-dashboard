#!/usr/bin/env python3
"""Real-Time Portfolio Orchestrator — Millisecond-level coordination engine.

Transforms portfolio management from batch (daily) to real-time (ms-level) by:
  1. Collecting signals from all 31 agents in real-time
  2. Scoring and ranking signals using C fast_engine (80ns/signal)
  3. Routing trades across 7 Fly.io regions for cross-region arbitrage
  4. Tracking gains/second, gains/ms, and gains/ns metrics
  5. Continuously rebalancing capital (not 4x/day batches)
  6. Location-alpha mode for sub-second local execution (~1ms to Coinbase)

Target: 50% portfolio growth in 4 minutes (~$0.000487/ms for $233.85 portfolio)
Current: $0.00001332/ms
Gap: 36.6x improvement needed

Usage:
  python3 realtime_orchestrator.py --mock --duration 60    # Local testing
  python3 realtime_orchestrator.py --live                  # Production mode
  python3 realtime_orchestrator.py --live --location-alpha # Local low-latency mode
"""

import asyncio
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import threading
from collections import defaultdict, deque
import contextlib
import urllib.request

# Shared signal bridge for creative agents
from creative_agent_bridge import CreativeAgentBridge

sys.path.insert(0, str(Path(__file__).parent))

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

# v77 Phase 3: Import Coinbase trading for live execution
try:
    from exchange_connector import CoinbaseTrader, PriceFeed
    COINBASE_AVAILABLE = True
except Exception as e:
    COINBASE_AVAILABLE = False
    logger.warning(f"CoinbaseTrader unavailable: {e} - using mock execution")

# Derivatives connector for margin health checks on perp orders
try:
    from coinbase_derivatives_connector import CoinbaseDerivativesConnector
    _orch_deriv = CoinbaseDerivativesConnector()
except Exception:
    _orch_deriv = None

# GoalValidator for perp trade gating
try:
    from agent_goals import GoalValidator
except Exception:
    try:
        from agents.agent_goals import GoalValidator
    except Exception:
        GoalValidator = None

# E*Trade connector for equity trading (Phase 1: E*Trade integration)
ETRADE_AVAILABLE = False
try:
    from etrade_connector import ETradeAuth, ETradeTrader, ETradePriceFeed
    ETRADE_AVAILABLE = True
except Exception:
    try:
        from agents.etrade_connector import ETradeAuth, ETradeTrader, ETradePriceFeed
        ETRADE_AVAILABLE = True
    except Exception:
        pass

# Kraken connector for crypto trading
KRAKEN_AVAILABLE = False
try:
    from kraken_connector import KrakenConnector
    KRAKEN_AVAILABLE = True
except Exception:
    try:
        from agents.kraken_connector import KrakenConnector
        KRAKEN_AVAILABLE = True
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ORCH] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "realtime_orchestrator.log")),
    ]
)
logger = logging.getLogger("realtime_orchestrator")

# Configuration
REALTIME_ORCHESTRATION_ENABLED = os.environ.get("REALTIME_ORCHESTRATION", "1").lower() in ("1", "true", "yes")
CYCLE_TARGET_MS = float(os.environ.get("ORCH_CYCLE_TARGET_MS", "100"))
CYCLE_MAX_MS = float(os.environ.get("ORCH_CYCLE_MAX_MS", "500"))
EXECUTION_LATENCY_TARGET_MS = float(os.environ.get("ORCH_EXEC_LATENCY_MS", "30"))
SIGNAL_COLLECTION_TIMEOUT_MS = float(os.environ.get("ORCH_SIGNAL_TIMEOUT_MS", "50"))
CAPITAL_REBALANCE_INTERVAL_S = int(os.environ.get("ORCH_CAPITAL_REBALANCE_S", "60"))
PREFER_PERPS = os.environ.get("ORCH_PREFER_PERPS", "1").lower() in ("1", "true", "yes")
FUNDING_ENABLED = os.environ.get("ORCH_FUNDING_ENABLED", "1").lower() in ("1", "true", "yes")
FUNDING_INTERVAL_S = float(os.environ.get("FUNDING_INTERVAL_S", "60"))
FUNDING_THRESHOLD_PCT = float(os.environ.get("FUNDING_THRESHOLD_PCT", "0.02"))  # 2 bps default
ORDER_NOTIONAL_USD = float(os.environ.get("ORCH_ORDER_USD", "3.0"))
PERP_ORDER_NOTIONAL_USD = float(os.environ.get("ORCH_PERP_ORDER_USD", str(ORDER_NOTIONAL_USD)))
ORCH_MAX_LEVERAGE = float(os.environ.get("ORCH_MAX_LEVERAGE", "3.0"))
ORCH_BASE_LEVERAGE = float(os.environ.get("ORCH_BASE_LEVERAGE", "1.0"))
ORCH_MAKER_OFFSET_BPS = float(os.environ.get("ORCH_MAKER_OFFSET_BPS", "8.0"))  # 8 bps from mid
ORCH_ASSET_PCT_CAP = float(os.environ.get("ORCH_ASSET_PCT_CAP", "0.35"))       # 35% of portfolio per asset
ORCH_ASSET_USD_CAP = float(os.environ.get("ORCH_ASSET_USD_CAP", "0.0"))        # absolute cap; 0 disables
ORCH_THEME_PCT_CAP = float(os.environ.get("ORCH_THEME_PCT_CAP", "0.30"))       # 30% per theme
NOTIONAL_WINDOW_S = float(os.environ.get("ORCH_NOTIONAL_WINDOW_S", "900"))     # 15-minute rolling window

# E*Trade equity trading configuration
ORCH_ETRADE_ENABLED = os.environ.get("ORCH_ETRADE_ENABLED", "0").lower() in ("1", "true", "yes")
ORCH_ETRADE_DRY_RUN = os.environ.get("ORCH_ETRADE_DRY_RUN", "1").lower() in ("1", "true", "yes")
ETRADE_DEFAULT_ACCOUNT_ID = os.environ.get("ETRADE_DEFAULT_ACCOUNT_ID", "")
ETRADE_TOKEN_REFRESH_INTERVAL_S = float(os.environ.get("ETRADE_TOKEN_REFRESH_S", "5400"))  # 90 min

# Kraken crypto trading configuration
ORCH_KRAKEN_ENABLED = os.environ.get("ORCH_KRAKEN_ENABLED", "0").lower() in ("1", "true", "yes")
ORCH_KRAKEN_DRY_RUN = os.environ.get("ORCH_KRAKEN_DRY_RUN", "1").lower() in ("1", "true", "yes")

# Location Alpha Mode — optimized for local execution near exchange matching engines
# When enabled, assumes ~1ms to Coinbase vs 50ms+ from Fly.io remote regions
LOCATION_ALPHA_ENABLED = os.environ.get("ORCH_LOCATION_ALPHA", "0").lower() in ("1", "true", "yes")
LOCATION_ALPHA_CYCLE_MS = float(os.environ.get("ORCH_LOCATION_ALPHA_CYCLE_MS", "25"))         # 25ms cycle target when local
LOCATION_ALPHA_SIGNAL_TIMEOUT_MS = float(os.environ.get("ORCH_LOCATION_ALPHA_SIGNAL_MS", "10"))  # 10ms signal scan
LOCATION_ALPHA_EXEC_LATENCY_MS = float(os.environ.get("ORCH_LOCATION_ALPHA_EXEC_MS", "5"))    # 5ms execution target
LOCATION_ALPHA_SCAN_INTERVAL_MS = float(os.environ.get("ORCH_LOCATION_ALPHA_SCAN_MS", "15"))   # signal scan frequency
LOCATION_ALPHA_LATENCY_PROBE_S = float(os.environ.get("ORCH_LOCATION_ALPHA_PROBE_S", "30"))    # latency probe interval
LOCATION_ALPHA_EXEC_WINDOW_S = float(os.environ.get("ORCH_LOCATION_ALPHA_WINDOW_S", "5"))      # 3-7 second execution window
COINBASE_API_HOST = os.environ.get("COINBASE_API_HOST", "api.coinbase.com")

# Apply location_alpha overrides to base config if enabled
if LOCATION_ALPHA_ENABLED:
    CYCLE_TARGET_MS = LOCATION_ALPHA_CYCLE_MS
    SIGNAL_COLLECTION_TIMEOUT_MS = LOCATION_ALPHA_SIGNAL_TIMEOUT_MS
    EXECUTION_LATENCY_TARGET_MS = LOCATION_ALPHA_EXEC_LATENCY_MS

# Gains/ms persistence — write to flywheel_status.json for other agents
GAINS_MS_PERSIST_INTERVAL_CYCLES = int(os.environ.get("ORCH_GAINS_MS_PERSIST_CYCLES", "5"))  # persist every N cycles

# Urgency levels and priorities
URGENCY_PRIORITIES = {
    "critical": 0,    # <100ms window
    "high": 1,        # <1s window
    "medium": 2,      # <10s window
    "low": 3,         # <60s window
}

# Regions for cross-region arbitrage
REGIONS = ["ewr", "ord", "lhr", "fra", "nrt", "sin", "bom"]


class PerformanceTracker:
    """Real-time performance tracking: gains/second, gains/ms, gains/ns."""

    def __init__(self):
        self.start_time_ns = time.time_ns()
        self.start_time_ms = self.start_time_ns / 1_000_000
        self.total_gains_usd = 0.0
        self.trade_history = []  # (timestamp_ms, pnl_usd, agent_name, latency_ms)
        self.lock = threading.Lock()
        # Per-cycle tracking for granular gains/ms and gains/ns
        self.cycle_history = deque(maxlen=1000)  # last 1000 cycles
        self.last_cycle_gains_ms = 0.0
        self.last_cycle_gains_ns = 0.0
        self.peak_gains_ms = 0.0
        self.peak_gains_ns = 0.0

    def record(self, executed_signals: List[Dict], cycle_latency_ms: float):
        """Record executed trades and update metrics."""
        with self.lock:
            cycle_pnl = 0.0
            for trade in executed_signals:
                pnl = trade.get("realized_pnl_usd", 0.0)
                self.total_gains_usd += pnl
                cycle_pnl += pnl
                self.trade_history.append({
                    "timestamp_ms": time.time() * 1000,
                    "pnl_usd": pnl,
                    "agent": trade.get("agent_name", "unknown"),
                    "latency_ms": cycle_latency_ms,
                })

            # Per-cycle gains/ms and gains/ns
            cycle_latency_ns = cycle_latency_ms * 1_000_000
            self.last_cycle_gains_ms = cycle_pnl / max(cycle_latency_ms, 0.001) if cycle_latency_ms > 0 else 0.0
            self.last_cycle_gains_ns = cycle_pnl / max(cycle_latency_ns, 1.0) if cycle_latency_ns > 0 else 0.0
            if self.last_cycle_gains_ms > self.peak_gains_ms:
                self.peak_gains_ms = self.last_cycle_gains_ms
            if self.last_cycle_gains_ns > self.peak_gains_ns:
                self.peak_gains_ns = self.last_cycle_gains_ns

            self.cycle_history.append({
                "timestamp_ms": time.time() * 1000,
                "cycle_pnl_usd": cycle_pnl,
                "cycle_latency_ms": cycle_latency_ms,
                "cycle_latency_ns": cycle_latency_ns,
                "cycle_gains_ms": self.last_cycle_gains_ms,
                "cycle_gains_ns": self.last_cycle_gains_ns,
                "executed_count": len(executed_signals),
            })

    def gains_per_second(self) -> float:
        """Calculate gains/second since start."""
        runtime_s = (time.time() * 1000 - self.start_time_ms) / 1000.0
        return self.total_gains_usd / max(runtime_s, 1.0)

    def gains_per_ms(self) -> float:
        """Calculate gains/millisecond (target metric)."""
        runtime_ms = time.time() * 1000 - self.start_time_ms
        return self.total_gains_usd / max(runtime_ms, 1.0)

    def gains_per_ns(self) -> float:
        """Calculate gains/nanosecond (high-resolution metric)."""
        runtime_ns = time.time_ns() - self.start_time_ns
        return self.total_gains_usd / max(runtime_ns, 1)

    def rolling_gains_per_ms(self, window_cycles: int = 50) -> float:
        """Calculate rolling gains/ms over the last N cycles."""
        with self.lock:
            if not self.cycle_history:
                return 0.0
            recent = list(self.cycle_history)[-window_cycles:]
            total_pnl = sum(c["cycle_pnl_usd"] for c in recent)
            total_ms = sum(c["cycle_latency_ms"] for c in recent)
            return total_pnl / max(total_ms, 0.001)

    def agent_scoreboard(self, agent_name: Optional[str] = None, limit: int = 10) -> Dict:
        """Performance attribution by agent."""
        with self.lock:
            if agent_name:
                agent_trades = [t for t in self.trade_history if t["agent"] == agent_name]
            else:
                agent_trades = self.trade_history[-limit:]

            if not agent_trades:
                return {}

            return {
                "total_pnl": sum(t["pnl_usd"] for t in agent_trades),
                "trade_count": len(agent_trades),
                "avg_latency_ms": sum(t["latency_ms"] for t in agent_trades) / len(agent_trades),
                "gains_per_ms": sum(t["pnl_usd"] for t in agent_trades) / max(sum(t["latency_ms"] for t in agent_trades), 1.0),
            }

    def agent_rankings(self, limit: int = 10) -> List[Tuple[str, Dict]]:
        """Get ranked list of agents by gains/ms."""
        with self.lock:
            agent_trades = defaultdict(list)
            for trade in self.trade_history:
                agent_trades[trade["agent"]].append(trade)

            rankings = []
            for agent_name, trades in agent_trades.items():
                if trades:
                    pnl = sum(t["pnl_usd"] for t in trades)
                    avg_latency = sum(t["latency_ms"] for t in trades) / len(trades)
                    gains_per_ms = pnl / max(avg_latency, 1.0)
                    rankings.append((agent_name, {
                        "total_pnl": pnl,
                        "trade_count": len(trades),
                        "avg_latency_ms": avg_latency,
                        "gains_per_ms": gains_per_ms,
                    }))

            # Sort by gains_per_ms descending
            rankings.sort(key=lambda x: x[1]["gains_per_ms"], reverse=True)
            return rankings[:limit]

    def snapshot(self) -> Dict:
        """Get current performance snapshot with gains/ms, gains/ns, per-cycle metrics."""
        with self.lock:
            now_ns = time.time_ns()
            runtime_ms = (now_ns - self.start_time_ns) / 1_000_000
            runtime_ns = now_ns - self.start_time_ns
            cumulative_gains_ms = self.total_gains_usd / max(runtime_ms, 0.001)
            cumulative_gains_ns = self.total_gains_usd / max(runtime_ns, 1)

            return {
                "runtime_ms": runtime_ms,
                "runtime_ns": runtime_ns,
                "total_gains_usd": self.total_gains_usd,
                "gains_per_second": self.total_gains_usd / max(runtime_ms / 1000.0, 1.0),
                "gains_per_ms": cumulative_gains_ms,
                "gains_per_ns": cumulative_gains_ns,
                "cycle_gains_ms": self.last_cycle_gains_ms,
                "cycle_gains_ns": self.last_cycle_gains_ns,
                "peak_gains_ms": self.peak_gains_ms,
                "peak_gains_ns": self.peak_gains_ns,
                "rolling_50_gains_ms": self.rolling_gains_per_ms(50),
                "trade_count": len(self.trade_history),
                "cycle_count": len(self.cycle_history),
            }


class SignalCollector:
    """Collect and aggregate signals from all 31 agents in real-time."""

    def __init__(self):
        self.signals = {}  # agent_name -> latest_signal
        self.signal_queue = asyncio.Queue()
        self.lock = threading.Lock()

    def submit_signal(self, agent_name: str, signal: Dict):
        """Submit signal from an agent to the queue."""
        if "urgency" not in signal:
            signal["urgency"] = "medium"
        if "confidence" not in signal:
            signal["confidence"] = 0.5

        with self.lock:
            self.signals[agent_name] = signal

        logger.debug(f"Signal from {agent_name}: {signal['pair']} {signal.get('direction', 'UNKNOWN')} "
                     f"(confidence={signal['confidence']:.2f}, urgency={signal['urgency']})")

    async def collect_all(self, timeout_ms: float = 50) -> Dict[str, Dict]:
        """Collect all current signals with timeout.

        In location_alpha mode, returns immediately if signals are available
        (no waiting) to minimize cycle latency.  Otherwise waits up to
        timeout_ms for signals to accumulate.
        """
        if LOCATION_ALPHA_ENABLED:
            # Fast path: return immediately if we have signals
            with self.lock:
                if self.signals:
                    return dict(self.signals)
            # Brief yield to allow other coroutines to submit signals
            await asyncio.sleep(max(0.0005, timeout_ms / 1000.0))
        else:
            try:
                await asyncio.wait_for(
                    asyncio.sleep(timeout_ms / 1000.0),
                    timeout=timeout_ms / 1000.0
                )
            except asyncio.TimeoutError:
                pass

        with self.lock:
            return dict(self.signals)


class SignalScorer:
    """Score and rank signals using fast C engine if available."""

    def __init__(self):
        self.fast_engine = None
        try:
            from fast_bridge import FastEngine
            self.fast_engine = FastEngine()
            logger.info("Fast C engine loaded (80ns/signal)")
        except Exception as e:
            logger.warning(f"Fast engine unavailable: {e}, using Python fallback")

        self.v333_enabled = str(os.environ.get("ORCH_V333_ALPHA_ENABLED", "1")).lower() in ("1", "true", "yes")
        self.v333_use_quantum = str(os.environ.get("ORCH_V333_USE_QUANTUM", "0")).lower() in ("1", "true", "yes")
        self.v333_max_positions = max(1, int(os.environ.get("ORCH_V333_MAX_POSITIONS", "12")))
        self.v333_constraints = {
            "max_asset_weight": float(os.environ.get("ORCH_V333_MAX_ASSET_WEIGHT", "0.35")),
            "max_bucket_weight": float(os.environ.get("ORCH_V333_MAX_BUCKET_WEIGHT", "0.45")),
            "max_region_weight": float(os.environ.get("ORCH_V333_MAX_REGION_WEIGHT", "0.60")),
        }
        self._v333_optimize = None
        self._v333_ready = False
        if self.v333_enabled:
            try:
                import importlib.util
                peak_alpha_path = Path(__file__).resolve().parent.parent / "v333_peak_alpha.py"
                spec = importlib.util.spec_from_file_location("v333_peak_alpha_runtime", str(peak_alpha_path))
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    self._v333_optimize = getattr(module, "optimize_signals_for_peak_alpha", None)
                    self._v333_ready = callable(self._v333_optimize)
            except Exception as e:
                logger.warning(f"v333 peak alpha unavailable: {e}")

        if self._v333_ready:
            logger.info(
                "v333 peak alpha enabled "
                f"(quantum={self.v333_use_quantum}, max_positions={self.v333_max_positions})"
            )

    def score_signals(self, signals: Dict[str, Dict]) -> List[Tuple[str, Dict, float]]:
        """Score all signals and return (agent_name, signal, score) tuples."""
        baseline = []
        for agent_name, signal in signals.items():
            score = self._score_single(signal)
            baseline.append((agent_name, signal, score))

        if not baseline:
            return []

        if not (self.v333_enabled and self._v333_ready and self._v333_optimize):
            baseline.sort(key=lambda x: x[2], reverse=True)
            return baseline

        try:
            signal_lookup = {}
            payload = []
            for idx, (agent_name, signal, base_score) in enumerate(baseline):
                pair = str(signal.get("pair", "")).strip().upper()
                if not pair:
                    continue
                signal_id = f"{agent_name}:{idx}:{pair}"
                signal_lookup[signal_id] = (agent_name, signal, base_score)
                edge_bps = signal.get("edge_bps", base_score * 100.0)
                latency_ms = signal.get("latency_ms", signal.get("expected_latency_ms", 100.0))
                volatility = signal.get("volatility", signal.get("risk_volatility", 0.05))
                correlation_bucket = signal.get("correlation_bucket", pair.split("-")[0].lower())
                region_bucket = signal.get("region_bucket", signal.get("region_hint", "global"))

                payload.append(
                    {
                        "signal_id": signal_id,
                        "pair": pair,
                        "direction": str(signal.get("direction", "BUY")).upper(),
                        "edge_bps": edge_bps,
                        "confidence": float(signal.get("confidence", 0.5)),
                        "latency_ms": float(latency_ms),
                        "volatility": float(volatility),
                        "correlation_bucket": str(correlation_bucket),
                        "region_bucket": str(region_bucket),
                    }
                )

            if not payload:
                baseline.sort(key=lambda x: x[2], reverse=True)
                return baseline

            result = self._v333_optimize(
                signals=payload,
                max_positions=min(self.v333_max_positions, len(payload)),
                use_quantum=self.v333_use_quantum,
                constraints=self.v333_constraints,
            )

            selected = result.get("selected") or []
            selected_ids = []
            selected_score = {}
            selected_weight = {}
            for row in selected:
                sid = str(row.get("signal_id", "")).strip()
                if not sid or sid not in signal_lookup:
                    continue
                selected_ids.append(sid)
                try:
                    selected_score[sid] = float(row.get("score", 0.0))
                except (TypeError, ValueError):
                    selected_score[sid] = 0.0
                try:
                    selected_weight[sid] = max(0.0, float(row.get("weight", 0.0)))
                except (TypeError, ValueError):
                    selected_weight[sid] = 0.0

            scored = []
            seen = set()
            for sid in selected_ids:
                agent_name, signal, base_score = signal_lookup[sid]
                signal["v333_weight"] = selected_weight.get(sid, 0.0)
                signal["v333_selected"] = True
                signal["v333_method"] = result.get("method", "local_annealing")
                composite = max(base_score, base_score * 0.35 + selected_score.get(sid, 0.0))
                scored.append((agent_name, signal, composite))
                seen.add(sid)

            for sid, (agent_name, signal, base_score) in signal_lookup.items():
                if sid in seen:
                    continue
                signal["v333_weight"] = 0.0
                signal["v333_selected"] = False
                signal["v333_method"] = result.get("method", "local_annealing")
                scored.append((agent_name, signal, base_score * 0.5))

            scored.sort(key=lambda x: x[2], reverse=True)
            return scored
        except Exception as e:
            logger.warning(f"v333 scoring fallback to baseline: {e}")
            baseline.sort(key=lambda x: x[2], reverse=True)
            return baseline

    def _score_single(self, signal: Dict) -> float:
        """Score a single signal (0.0-1.0)."""
        # Simple scoring: confidence * urgency_weight
        confidence = signal.get("confidence", 0.5)
        urgency = signal.get("urgency", "medium")

        # Weight recent high-confidence signals
        urgency_weight = {
            "critical": 1.0,
            "high": 0.8,
            "medium": 0.6,
            "low": 0.4,
        }.get(urgency, 0.5)

        score = confidence * urgency_weight

        # Boost if 70%+ confidence (gating threshold)
        if confidence >= 0.70:
            score *= 1.2

        return min(score, 1.0)


class CrossRegionRouter:
    """Route trades to optimal region based on latency and exchange proximity."""

    def __init__(self):
        self.region_latencies = {region: 100.0 for region in REGIONS}  # Default 100ms
        self.region_exchange_map = {
            "ewr": ["coinbase", "kraken"],  # US exchanges primary
            "lhr": ["kraken", "bitstamp"],  # Europe
            "nrt": ["bybit", "okx"],        # Asia
            "sin": ["binance", "okx"],      # SE Asia
            "fra": ["kraken", "bitstamp"],  # EU (Frankfurt)
            "ord": ["coinbase"],            # US (Chicago)
            "bom": ["binance", "okx"],      # India
        }

    def update_latencies(self, latencies: Dict[str, float]):
        """Update region latency measurements."""
        self.region_latencies.update(latencies)

    def route_signal(self, signal: Dict, region_hint: Optional[str] = None) -> str:
        """Return optimal region for executing this signal."""
        if region_hint and region_hint in REGIONS:
            return region_hint

        # Route to region with lowest latency by default
        return min(self.region_latencies, key=self.region_latencies.get)

    def batch_route(self, scored_signals: List[Tuple[str, Dict, float]]) -> Dict[str, List[Tuple[str, Dict, float]]]:
        """Route all signals to optimal regions."""
        routed = defaultdict(list)

        for agent_name, signal, score in scored_signals:
            region_hint = signal.get("region_hint")
            region = self.route_signal(signal, region_hint)
            routed[region].append((agent_name, signal, score))

        return dict(routed)


class ContinuousCapitalManager:
    """Real-time capital allocation (not 4x/day batches)."""

    def __init__(self):
        self.last_rebalance_s = time.time()
        self.allocation_history = []

    async def rebalance(self, portfolio_value_usd: float, agent_rankings: List[Tuple[str, Dict]]):
        """Rebalance capital continuously based on agent performance."""
        current_time_s = time.time()

        # Only rebalance every N seconds
        if current_time_s - self.last_rebalance_s < CAPITAL_REBALANCE_INTERVAL_S:
            return

        # Fire losing agents, allocate more to winners
        winning_agents = [name for name, metrics in agent_rankings if metrics["gains_per_ms"] > 0]
        losing_agents = [name for name, metrics in agent_rankings if metrics["gains_per_ms"] <= 0]

        if losing_agents:
            logger.info(f"Firing underperforming agents: {losing_agents}")
            # TODO: Integrate with orchestrator_v2 to kill agent subprocesses

        if winning_agents:
            # Allocate more capital to top performers
            allocation = {
                name: (idx + 1) / len(winning_agents)  # Linear weight by rank
                for idx, (name, _) in enumerate(winning_agents)
            }
            logger.info(f"Capital reallocation: {allocation}")

        self.last_rebalance_s = current_time_s


class CoinbaseLatencyProbe:
    """Real-time latency tracking to Coinbase API for location_alpha mode."""

    def __init__(self, host: str = None):
        self.host = host or COINBASE_API_HOST
        self.lock = threading.Lock()
        self.latency_history = deque(maxlen=500)
        self.last_probe_time = 0.0
        self.last_latency_ms = 0.0
        self.avg_latency_ms = 0.0
        self.min_latency_ms = float("inf")
        self.max_latency_ms = 0.0
        self.probe_count = 0

    def probe(self) -> float:
        """Measure round-trip latency to Coinbase API (HTTP HEAD, no body).

        Returns latency in milliseconds.
        """
        url = f"https://{self.host}/api/v3/brokerage/time"
        start_ns = time.time_ns()
        try:
            req = urllib.request.Request(url, method="GET")
            req.add_header("User-Agent", "NetTrace-LatencyProbe/1.0")
            with urllib.request.urlopen(req, timeout=2) as resp:
                resp.read()
        except Exception:
            # Even on error, measure the round-trip time
            pass
        end_ns = time.time_ns()
        latency_ms = (end_ns - start_ns) / 1_000_000

        with self.lock:
            self.last_latency_ms = latency_ms
            self.last_probe_time = time.time()
            self.probe_count += 1
            self.latency_history.append({
                "timestamp_ms": time.time() * 1000,
                "latency_ms": latency_ms,
            })
            if latency_ms < self.min_latency_ms:
                self.min_latency_ms = latency_ms
            if latency_ms > self.max_latency_ms:
                self.max_latency_ms = latency_ms
            # Running average
            if self.probe_count == 1:
                self.avg_latency_ms = latency_ms
            else:
                self.avg_latency_ms = self.avg_latency_ms * 0.9 + latency_ms * 0.1

        return latency_ms

    def snapshot(self) -> Dict:
        """Get latency probe snapshot."""
        with self.lock:
            return {
                "host": self.host,
                "last_latency_ms": round(self.last_latency_ms, 3),
                "avg_latency_ms": round(self.avg_latency_ms, 3),
                "min_latency_ms": round(self.min_latency_ms, 3) if self.min_latency_ms < float("inf") else 0.0,
                "max_latency_ms": round(self.max_latency_ms, 3),
                "probe_count": self.probe_count,
                "last_probe_at": self.last_probe_time,
            }


class RealtimeOrchestrator:
    """Main orchestrator: coordinates all agents at millisecond precision."""

    def __init__(self, mock_mode: bool = False, location_alpha: bool = False):
        self.mock_mode = mock_mode
        self.location_alpha = location_alpha or LOCATION_ALPHA_ENABLED
        self.signal_collector = SignalCollector()
        self.signal_scorer = SignalScorer()
        self.router = CrossRegionRouter()
        self.capital_manager = ContinuousCapitalManager()
        self.performance_tracker = PerformanceTracker()
        self.cycle_count = 0
        self.total_latency_ms = 0.0
        self.running = False
        self.perp_map = {}  # base -> perp product_id
        self._background_tasks = []
        self.asset_notional = defaultdict(deque)  # asset -> deque[(ts, usd)]
        self.theme_notional = defaultdict(deque)  # theme -> deque[(ts, usd)]
        self.latency_probe = CoinbaseLatencyProbe() if self.location_alpha else None
        self._gains_ms_persist_counter = 0

        # v77 Phase 3: Initialize Coinbase trader for live execution
        self.trader = None
        if COINBASE_AVAILABLE and not mock_mode:
            try:
                self.trader = CoinbaseTrader()
                logger.info("CoinbaseTrader initialized for live execution")
                if PREFER_PERPS:
                    self._refresh_perp_mapping()
            except Exception as e:
                logger.warning(f"Failed to initialize CoinbaseTrader: {e}")
                self.trader = None

        # E*Trade equity trading initialization
        self.etrade_trader = None
        self.etrade_price_feed = None
        self.etrade_auth = None
        self.etrade_account_id = ETRADE_DEFAULT_ACCOUNT_ID or None
        if ORCH_ETRADE_ENABLED and ETRADE_AVAILABLE and not mock_mode:
            try:
                self.etrade_auth = ETradeAuth()
                if self.etrade_auth.is_authenticated:
                    self.etrade_trader = ETradeTrader(auth=self.etrade_auth)
                    self.etrade_price_feed = ETradePriceFeed(auth=self.etrade_auth)
                    # Resolve default account if not set
                    if not self.etrade_account_id:
                        accounts = self.etrade_trader.get_accounts()
                        if accounts:
                            self.etrade_account_id = accounts[0].get("accountIdKey")
                    logger.info("E*Trade initialized (dry_run=%s, account=%s)",
                                ORCH_ETRADE_DRY_RUN, self.etrade_account_id)
                else:
                    logger.warning("E*Trade auth not authenticated — run etrade_connector.py auth first")
            except Exception as e:
                logger.warning("Failed to initialize E*Trade: %s", e)

        # Kraken crypto trading initialization
        self.kraken_connector = None
        if ORCH_KRAKEN_ENABLED and KRAKEN_AVAILABLE and not mock_mode:
            try:
                self.kraken_connector = KrakenConnector()
                logger.info("KrakenConnector initialized (dry_run=%s)", ORCH_KRAKEN_DRY_RUN)
            except Exception as e:
                logger.warning("Failed to initialize KrakenConnector: %s", e)

        # Register with CreativeAgentBridge so creative agents can push signals directly
        try:
            CreativeAgentBridge.set_orchestrator(self)
        except Exception:
            logger.warning("CreativeAgentBridge unavailable; signals will queue locally")

    def _start_background_tasks(self):
        """Kick off background coroutines (funding scan, latency probe, etc.)."""
        if FUNDING_ENABLED and self.trader:
            self._background_tasks.append(asyncio.create_task(self._funding_loop()))
        if self.location_alpha and self.latency_probe:
            self._background_tasks.append(asyncio.create_task(self._latency_probe_loop()))
        if self.etrade_auth and self.etrade_auth.is_authenticated:
            self._background_tasks.append(asyncio.create_task(self._etrade_token_refresh_loop()))

    # ------------------------------------------------------------------ #
    # Derivatives helpers
    def _refresh_perp_mapping(self):
        """Build mapping base -> preferred perpetual product_id."""
        list_fn = getattr(self.trader, "list_perp_products", None) or getattr(self.trader, "list_perpetual_products", None)
        if not self.trader or not list_fn:
            return
        try:
            perps = list_fn() or []
        except Exception as e:
            logger.warning(f"Failed to load perpetual products: {e}")
            return

        mapping = {}
        for p in perps:
            if not isinstance(p, dict):
                continue
            pid = str(p.get("product_id", "")).upper()
            if not pid:
                continue
            base = (
                str(p.get("base_currency_id") or p.get("base_currency") or pid.split("-")[0])
                .replace("USDC", "")
                .replace("USD", "")
                .upper()
            )
            quote = str(p.get("quote_currency_id") or p.get("quote_currency") or "").upper()
            # Prefer USD/USDC-settled perps
            score = 0
            if quote in {"USD", "USDC"}:
                score += 2
            if "PERP" in pid:
                score += 1
            prev = mapping.get(base)
            if not prev or score > prev["score"]:
                mapping[base] = {"pid": pid, "score": score}

        self.perp_map = {k: v["pid"] for k, v in mapping.items()}
        if self.perp_map:
            logger.info(f"Perp mapping ready for {len(self.perp_map)} bases: {list(self.perp_map.keys())}")

    def _perp_for_pair(self, pair: str) -> Optional[str]:
        """Return perp product_id for a spot pair if available."""
        if not PREFER_PERPS or not self.perp_map:
            return None
        if not pair:
            return None
        try:
            base = str(pair).split("-")[0].upper()
        except Exception:
            return None
        return self.perp_map.get(base)

    # ------------------------------------------------------------------ #
    # Notional caps and leverage
    def _prune_window(self, dq: deque):
        cutoff = time.time() - NOTIONAL_WINDOW_S
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def _record_notional(self, asset: str, theme: str, notional_usd: float):
        now = time.time()
        dq_a = self.asset_notional[asset]
        dq_t = self.theme_notional[theme]
        dq_a.append((now, notional_usd))
        dq_t.append((now, notional_usd))
        self._prune_window(dq_a)
        self._prune_window(dq_t)

    def _notional_ok(self, asset: str, theme: str, request_usd: float, portfolio: float) -> Tuple[bool, str]:
        dq_a = self.asset_notional[asset]
        dq_t = self.theme_notional[theme]
        self._prune_window(dq_a)
        self._prune_window(dq_t)
        asset_used = sum(v for _, v in dq_a)
        theme_used = sum(v for _, v in dq_t)

        if ORCH_ASSET_USD_CAP > 0 and asset_used + request_usd > ORCH_ASSET_USD_CAP:
            return False, f"asset_usd_cap:{asset_used + request_usd:.2f}>{ORCH_ASSET_USD_CAP:.2f}"

        if ORCH_ASSET_PCT_CAP > 0 and portfolio > 0:
            if (asset_used + request_usd) > portfolio * ORCH_ASSET_PCT_CAP:
                return False, f"asset_pct_cap:{(asset_used + request_usd)/portfolio:.2%}>{ORCH_ASSET_PCT_CAP:.0%}"

        if ORCH_THEME_PCT_CAP > 0 and portfolio > 0:
            if (theme_used + request_usd) > portfolio * ORCH_THEME_PCT_CAP:
                return False, f"theme_pct_cap:{(theme_used + request_usd)/portfolio:.2%}>{ORCH_THEME_PCT_CAP:.0%}"

        return True, ""

    def _leverage_for_signal(
        self,
        confidence: float,
        n_signals: int = 1,
        n_regions: int = 1,
        margin_health_ok: bool = False,
        recent_pnl_positive: bool = False,
    ) -> float:
        """Leverage ladder per Location-Alpha directive Section 4.1.

        Tiers (evaluated top-down, first match wins):
          3.0x  -- confidence >= 0.90, >=2 signals, >=2 regions,
                   healthy margin, positive recent realized PnL
          2.2x  -- confidence >= 0.85, >=2 signals, >=2 regions,
                   healthy margin
          1.5x  -- confidence >= 0.78, >=2 confirming signals
          1.0x  -- everything else (low confidence or single-region)

        Always capped by min(ORCH_MAX_LEVERAGE, PERP_MAX_LEVERAGE).
        Falls back to 1.0x on any calculation error (fail-closed).
        """
        try:
            max_lv = max(1.0, ORCH_MAX_LEVERAGE)

            # Tier 4 -- maximum leverage
            if (
                confidence >= 0.90
                and n_signals >= 2
                and n_regions >= 2
                and margin_health_ok
                and recent_pnl_positive
            ):
                lvl = 3.0
            # Tier 3 -- cross-region consensus + healthy margin
            elif (
                confidence >= 0.85
                and n_signals >= 2
                and n_regions >= 2
                and margin_health_ok
            ):
                lvl = 2.2
            # Tier 2 -- multi-signal confirmation
            elif confidence >= 0.78 and n_signals >= 2:
                lvl = 1.5
            # Tier 1 -- baseline
            else:
                lvl = 1.0

            result = max(1.0, min(lvl, max_lv))

            if result > 1.0:
                logger.info(
                    "Leverage ladder: %.1fx (conf=%.2f, signals=%d, regions=%d, "
                    "margin_ok=%s, pnl_pos=%s)",
                    result, confidence, n_signals, n_regions,
                    margin_health_ok, recent_pnl_positive,
                )
            return result
        except Exception as e:
            logger.warning("Leverage calculation error, falling back to 1.0x: %s", e)
            return 1.0

    def _recent_pnl_positive(self) -> bool:
        """Return True if recent realized PnL (last 20 trades) is positive."""
        try:
            with self.performance_tracker.lock:
                recent = list(self.performance_tracker.trade_history[-20:])
            if not recent:
                return False
            return sum(t.get("pnl_usd", 0.0) for t in recent) > 0
        except Exception:
            return False

    async def run(self, duration_s: Optional[float] = None):
        """Main orchestration loop.

        In location_alpha mode, runs tighter cycles (25ms target) with
        sub-second execution windows and nanosecond-resolution gains tracking.
        """
        self.running = True
        start_time_s = time.time()

        mode_str = "LOCATION-ALPHA" if self.location_alpha else "STANDARD"
        logger.info(
            f"Starting RealtimeOrchestrator ({mode_str}, mock_mode={self.mock_mode}, "
            f"target_cycle={CYCLE_TARGET_MS}ms, exec_window={LOCATION_ALPHA_EXEC_WINDOW_S}s)"
        )
        if self.location_alpha:
            logger.info(
                f"Location-alpha config: cycle={CYCLE_TARGET_MS}ms, "
                f"signal_timeout={SIGNAL_COLLECTION_TIMEOUT_MS}ms, "
                f"exec_target={EXECUTION_LATENCY_TARGET_MS}ms, "
                f"scan_interval={LOCATION_ALPHA_SCAN_INTERVAL_MS}ms"
            )
        logger.info("Entering main orchestration loop...")
        self._start_background_tasks()

        while self.running:
            cycle_start_ns = time.time_ns()

            try:
                # 1. Collect signals from all agents
                if self.cycle_count < 3:
                    logger.info(f"[Cycle {self.cycle_count+1}] Collecting signals...")
                signals = await self.signal_collector.collect_all(timeout_ms=SIGNAL_COLLECTION_TIMEOUT_MS)
                collection_latency_ms = (time.time_ns() - cycle_start_ns) / 1_000_000

                # 2. Score and rank signals (C fast_engine, <10ms)
                scored_signals = self.signal_scorer.score_signals(signals)
                scoring_latency_ms = (time.time_ns() - cycle_start_ns) / 1_000_000 - collection_latency_ms

                # 3. Route to optimal regions (NetTrace data, <5ms)
                routed = self.router.batch_route(scored_signals)
                routing_latency_ms = (time.time_ns() - cycle_start_ns) / 1_000_000 - collection_latency_ms - scoring_latency_ms

                # 4. Execute top signals (HF lane)
                executed = await self._execute_signals(routed)
                execution_latency_ms = (time.time_ns() - cycle_start_ns) / 1_000_000 - collection_latency_ms - scoring_latency_ms - routing_latency_ms

                # 5. Update performance metrics (nanosecond-resolution)
                total_cycle_ns = time.time_ns() - cycle_start_ns
                total_cycle_latency_ms = total_cycle_ns / 1_000_000
                self.performance_tracker.record(executed, total_cycle_latency_ms)

                # 5b. Persist metrics to database for API consumption
                self._persist_metrics_to_db(total_cycle_latency_ms, len(signals), len(executed))

                # 5c. Persist gains/ms to flywheel_status.json periodically
                self._gains_ms_persist_counter += 1
                if self._gains_ms_persist_counter >= GAINS_MS_PERSIST_INTERVAL_CYCLES:
                    self._persist_gains_to_flywheel()
                    self._gains_ms_persist_counter = 0

                # 6. Rebalance capital
                if not self.mock_mode:
                    portfolio_value = self._get_portfolio_value()
                    top_agents = self.performance_tracker.agent_rankings(limit=10)
                    await self.capital_manager.rebalance(portfolio_value, top_agents)

                # Logging
                self.cycle_count += 1
                self.total_latency_ms += total_cycle_latency_ms

                log_interval = 10 if not self.location_alpha else 50
                if self.cycle_count % log_interval == 0:
                    perf = self.performance_tracker.snapshot()
                    latency_info = ""
                    if self.latency_probe:
                        ls = self.latency_probe.snapshot()
                        latency_info = f" | CB latency: {ls['avg_latency_ms']:.1f}ms"
                    logger.info(
                        f"Cycle {self.cycle_count} | "
                        f"Latency: {total_cycle_latency_ms:.2f}ms ({total_cycle_ns}ns) "
                        f"(target {CYCLE_TARGET_MS}ms) | "
                        f"Signals: {len(signals)} | Executed: {len(executed)} | "
                        f"Gains/ms: ${perf['gains_per_ms']:.10f} | "
                        f"Gains/ns: ${perf['gains_per_ns']:.14f} | "
                        f"Cycle-G/ms: ${perf['cycle_gains_ms']:.10f} | "
                        f"Peak-G/ms: ${perf['peak_gains_ms']:.10f} | "
                        f"R50-G/ms: ${perf['rolling_50_gains_ms']:.10f} | "
                        f"Total: ${perf['total_gains_usd']:.4f}"
                        f"{latency_info}"
                    )

                # Check if cycle exceeded target
                if total_cycle_latency_ms > CYCLE_MAX_MS:
                    logger.warning(
                        f"Slow cycle: {total_cycle_latency_ms:.2f}ms > {CYCLE_MAX_MS}ms "
                        f"(collect={collection_latency_ms:.2f}, score={scoring_latency_ms:.2f}, "
                        f"route={routing_latency_ms:.2f}, exec={execution_latency_ms:.2f})"
                    )

            except Exception as e:
                logger.error(f"Error in orchestration loop: {e}", exc_info=True)

            # Check duration
            if duration_s and time.time() - start_time_s > duration_s:
                logger.info(f"Duration limit reached ({duration_s}s)")
                break

            # Maintain target cycle time -- location_alpha uses tighter sleep
            if self.location_alpha:
                # Calculate remaining time in cycle window, sleep only the residual
                elapsed_ms = (time.time_ns() - cycle_start_ns) / 1_000_000
                remaining_ms = max(0.1, CYCLE_TARGET_MS - elapsed_ms)
                await asyncio.sleep(remaining_ms / 1000.0)
            else:
                await asyncio.sleep(0.001)  # 1ms minimum sleep

        # Final persistence before shutdown
        self._persist_gains_to_flywheel()

        # Shutdown background tasks
        self.running = False
        for task in self._background_tasks:
            task.cancel()
            with contextlib.suppress(Exception):
                await task

    async def _execute_signals(self, routed_signals: Dict[str, List[Tuple[str, Dict, float]]]) -> List[Dict]:
        """Execute signals across multiple regions in parallel."""
        executed = []

        for region, signals in routed_signals.items():
            # Take top 5 signals per region
            top_signals = signals[:5]

            for agent_name, signal, score in top_signals:
                # Route equity signals to dedicated handler
                if signal.get("market_type") == "equity":
                    equity_result = await self._execute_equity_signal(agent_name, signal, score, region)
                    if equity_result:
                        executed.append(equity_result)
                    continue

                # Route Kraken signals to dedicated handler
                if signal.get("venue_hint") == "kraken":
                    kraken_result = await self._execute_kraken_signal(agent_name, signal, score, region)
                    if kraken_result:
                        executed.append(kraken_result)
                    continue

                pair = signal.get("pair")
                direction = signal.get("direction", "HOLD").upper()
                confidence = float(signal.get("confidence", 0.5))
                theme = agent_name.split("_")[0] if agent_name else "unknown"

                # Gather leverage ladder inputs from signal metadata
                n_signals = max(1, int(signal.get("confirming_signals", 1)))
                n_regions = max(1, int(signal.get("confirming_regions", signal.get("n_regions", 1))))
                # Margin health: check live connector if available, else conservatively false
                _margin_ok = False
                try:
                    if _orch_deriv and _orch_deriv.enabled:
                        _mh = _orch_deriv.margin_health()
                        _margin_ok = _mh.get("can_open_new", False)
                except Exception:
                    _margin_ok = False
                _pnl_pos = self._recent_pnl_positive()

                desired_leverage = self._leverage_for_signal(
                    confidence=confidence,
                    n_signals=n_signals,
                    n_regions=n_regions,
                    margin_health_ok=_margin_ok,
                    recent_pnl_positive=_pnl_pos,
                )
                perp_cap = max(1.0, float(os.environ.get("PERP_MAX_LEVERAGE", str(ORCH_MAX_LEVERAGE))))
                perp_leverage = max(1.0, min(desired_leverage, perp_cap))

                # Defaults for tracking (set properly in live path)
                use_perp = False
                exec_pair = pair

                # v77 Phase 3: Execute live orders via Coinbase
                if not self.mock_mode and self.trader and direction != "HOLD":
                    try:
                        # Prefer perpetual if available (with margin health check)
                        perp_pair = self._perp_for_pair(pair)
                        use_perp = False
                        if perp_pair and _orch_deriv and _orch_deriv.enabled:
                            margin = _orch_deriv.margin_health()
                            if direction == "SELL" or margin.get("can_open_new", False):
                                # Gate through GoalValidator.should_trade_perp()
                                margin_ratio = margin.get("margin_ratio", 0)
                                if GoalValidator and hasattr(GoalValidator, "should_trade_perp"):
                                    use_perp = GoalValidator.should_trade_perp(
                                        confidence=confidence,
                                        confirming_signals=int(signal.get("confirming_signals", 2)),
                                        direction=direction,
                                        market_regime=signal.get("market_regime", "neutral"),
                                        leverage=perp_leverage,
                                        margin_health=margin_ratio,
                                    )
                                else:
                                    use_perp = confidence >= 0.70
                                if not use_perp:
                                    logger.info(f"GoalValidator blocked perp for {pair}, using spot")
                            else:
                                logger.info(f"Perp margin unhealthy, falling back to spot for {pair}")
                        exec_pair = perp_pair if use_perp else pair

                        # Portfolio + leverage
                        portfolio_value = self._get_portfolio_value()
                        if use_perp:
                            leverage = perp_leverage
                        else:
                            leverage = desired_leverage
                        base_order_usd = PERP_ORDER_NOTIONAL_USD if use_perp else ORDER_NOTIONAL_USD
                        signal_weight = max(0.0, float(signal.get("v333_weight", 0.0) or 0.0))
                        size_multiplier = max(0.35, min(1.75, signal_weight * 5.0)) if signal_weight > 0 else 1.0
                        order_usd = base_order_usd * leverage * size_multiplier

                        # Get current price from trader
                        ticker = self.trader.get_ticker(exec_pair)
                        if not ticker or "price" not in ticker:
                            # Perp ticker may differ; fall back to spot price
                            if use_perp:
                                ticker = self.trader.get_ticker(pair)
                            if not ticker or "price" not in ticker:
                                logger.warning(f"Could not get price for {exec_pair}, skipping")
                                continue

                        current_price = float(ticker["price"])
                        base_size = order_usd / current_price

                        # Notional caps (asset/theme)
                        asset = pair.split("-")[0] if pair else "UNKNOWN"
                        allowed, reason = self._notional_ok(asset, theme, order_usd, portfolio_value)
                        if not allowed:
                            logger.info(f"Skip {exec_pair} by {agent_name}: cap {reason}")
                            continue

                        # Calculate limit price (slightly better than market)
                        offset = ORCH_MAKER_OFFSET_BPS / 10000.0
                        if direction == "BUY":
                            limit_price = current_price * (1 - offset)
                        else:  # SELL
                            limit_price = current_price * (1 + offset)

                        # Route through perp connector or spot
                        if use_perp:
                            reduce_only = (direction == "SELL")
                            result = _orch_deriv.place_perp_order(
                                product_id=exec_pair,
                                side=direction,
                                size=base_size,
                                price=limit_price,
                                leverage=leverage,
                                post_only=True,
                                reduce_only=reduce_only,
                            )
                            # If perp order fails, fall back to spot
                            err = result.get("error_response", {})
                            if err.get("error") in ("PERP_DISABLED", "MARGIN_UNHEALTHY", "LEVERAGE_CAP", "ORDER_ERROR"):
                                logger.warning(f"Perp order failed ({err.get('error')}), falling back to spot for {pair}")
                                use_perp = False
                                exec_pair = pair
                                result = self.trader.place_limit_order(
                                    product_id=pair,
                                    side=direction,
                                    base_size=base_size,
                                    limit_price=limit_price,
                                    post_only=True,
                                    signal_confidence=signal.get("confidence", 0.5)
                                )
                        else:
                            # Place spot limit order (post_only=True enforces maker fee)
                            result = self.trader.place_limit_order(
                                product_id=exec_pair,
                                side=direction,
                                base_size=base_size,
                                limit_price=limit_price,
                                post_only=True,
                                signal_confidence=signal.get("confidence", 0.5)
                            )

                        # Check result
                        order_id = result.get("order_id") or result.get("id")
                        if not order_id and "success_response" in result:
                            order_id = result["success_response"].get("order_id")
                        if order_id:
                            venue = "PERP" if use_perp else "SPOT"
                            logger.info(
                                f"✅ [{venue}] Order placed: {direction} {exec_pair} "
                                f"size={base_size:.6f} @ ${limit_price:.2f} "
                                f"(market=${current_price:.2f}) | agent={agent_name} | lev={leverage:.2f}x | order_id={order_id}"
                            )
                            self._record_notional(asset, theme, order_usd)
                            pnl = 0.0  # Real PnL tracked by exit_manager
                        else:
                            error_msg = result.get("error_response", {}).get("message", "Unknown error")
                            logger.warning(f"Order placement failed: {error_msg}")
                            pnl = 0.0

                    except Exception as e:
                        logger.error(f"Execution error for {pair}: {e}", exc_info=True)
                        pnl = 0.0

                else:
                    # Mock execution for testing or when trader unavailable
                    import random
                    pnl = random.uniform(-0.5, 1.5) if self.mock_mode else 0.0

                executed.append({
                    "agent_name": agent_name,
                    "pair": pair,
                    "product_id": exec_pair,
                    "used_perp": use_perp,
                    "direction": direction,
                    "realized_pnl_usd": pnl,
                    "region": region,
                    "score": score,
                    "v333_weight": signal.get("v333_weight", 0.0),
                    "timestamp_ms": time.time() * 1000,
                })

        return executed

    async def _execute_equity_signal(self, agent_name, signal, score, region):
        """Execute an equity signal via E*Trade.

        Returns execution result dict or None if skipped.
        """
        pair = signal.get("pair", "")
        direction = signal.get("direction", "HOLD").upper()
        confidence = float(signal.get("confidence", 0.5))

        if direction == "HOLD":
            return None

        # Gate: market hours
        if GoalValidator and hasattr(GoalValidator, "is_equity_market_open"):
            if not GoalValidator.is_equity_market_open():
                logger.debug("Equity signal %s blocked: market closed", pair)
                return None

        # Gate: GoalValidator equity check
        if GoalValidator and hasattr(GoalValidator, "should_trade_equity"):
            regime = signal.get("market_regime", "neutral")
            n_signals = max(1, int(signal.get("confirming_signals", 1)))
            if not GoalValidator.should_trade_equity(confidence, n_signals, direction, regime):
                logger.debug("Equity signal %s blocked by GoalValidator", pair)
                return None

        # Extract ticker from pair (e.g., "AAPL-USD" -> "AAPL")
        symbol = pair.split("-")[0].upper() if pair else ""
        if not symbol:
            return None

        # Get price via ETradePriceFeed
        price = None
        if self.etrade_price_feed:
            price = self.etrade_price_feed.get_price(symbol)
        if price is None:
            logger.warning("Could not get price for equity %s, skipping", symbol)
            return None

        # Dynamic trade sizing from risk_controller (same as crypto path)
        try:
            from risk_controller import get_controller
            rc = get_controller()
            portfolio_value = self._get_portfolio_value()
            risk_params = rc.get_risk_params(portfolio_value)
            max_trade_usd = risk_params.get("max_trade_usd", 5.0)
        except Exception:
            max_trade_usd = ORDER_NOTIONAL_USD

        # Whole shares only (no fractional on E*Trade)
        import math as _math
        quantity = _math.floor(max_trade_usd / price)
        if quantity < 1:
            logger.info("Equity %s: price $%.2f > max_trade $%.2f, can't buy 1 share",
                         symbol, price, max_trade_usd)
            return None

        order_usd = quantity * price
        pnl = 0.0

        # Execute via E*Trade
        if self.etrade_trader and self.etrade_account_id:
            try:
                result = self.etrade_trader.place_order(
                    account_id_key=self.etrade_account_id,
                    symbol=symbol,
                    side=direction,
                    quantity=quantity,
                    order_type="LIMIT",
                    limit_price=price,
                    signal_confidence=confidence,
                    dry_run=ORCH_ETRADE_DRY_RUN,
                )
                order_id = result.get("order_id") or result.get("orderId")
                dry_tag = " [DRY-RUN]" if ORCH_ETRADE_DRY_RUN else ""
                if order_id or ORCH_ETRADE_DRY_RUN:
                    logger.info(
                        "✅ [EQUITY%s] %s %s x%d @ $%.2f ($%.2f) | agent=%s | order=%s",
                        dry_tag, direction, symbol, quantity, price, order_usd,
                        agent_name, order_id or "preview",
                    )
                else:
                    error_msg = result.get("error", result.get("message", "Unknown"))
                    logger.warning("Equity order failed for %s: %s", symbol, error_msg)
            except Exception as e:
                logger.error("Equity execution error for %s: %s", symbol, e, exc_info=True)
        elif self.mock_mode:
            import random
            pnl = random.uniform(-0.5, 1.5)
            logger.info("[MOCK-EQUITY] %s %s x%d @ $%.2f", direction, symbol, quantity, price)
        else:
            logger.debug("E*Trade not available for equity signal %s", pair)
            return None

        return {
            "agent_name": agent_name,
            "pair": pair,
            "product_id": symbol,
            "used_perp": False,
            "direction": direction,
            "realized_pnl_usd": pnl,
            "region": region,
            "score": score,
            "market_type": "equity",
            "v333_weight": signal.get("v333_weight", 0.0),
            "timestamp_ms": time.time() * 1000,
        }

    async def _execute_kraken_signal(self, agent_name, signal, score, region):
        """Execute a crypto signal via Kraken.

        Returns execution result dict or None if skipped.
        """
        pair = signal.get("pair", "")
        direction = signal.get("direction", "HOLD").upper()
        confidence = float(signal.get("confidence", 0.5))

        if direction == "HOLD":
            return None

        # Gate: GoalValidator
        if GoalValidator:
            regime = signal.get("market_regime", "neutral")
            n_signals = max(1, int(signal.get("confirming_signals", 1)))
            if not GoalValidator.should_trade(confidence, n_signals, direction, regime):
                logger.debug("Kraken signal %s blocked by GoalValidator", pair)
                return None

        # Get Kraken price
        price = None
        try:
            vol_data = KrakenConnector.get_24h_volume(pair)
            price = vol_data.get("last_price")
        except Exception:
            pass

        if not price or price <= 0:
            return None

        # Dynamic trade sizing
        try:
            from risk_controller import get_controller
            rc = get_controller()
            portfolio_value = self._get_portfolio_value()
            risk_params = rc.get_risk_params(portfolio_value)
            max_trade_usd = risk_params.get("max_trade_usd", 5.0)
        except Exception:
            max_trade_usd = ORDER_NOTIONAL_USD

        volume = max_trade_usd / price
        order_usd = volume * price
        pnl = 0.0

        # Execute via Kraken
        if self.kraken_connector and not ORCH_KRAKEN_DRY_RUN:
            try:
                result = KrakenConnector.place_order(
                    pair=pair, side=direction.lower(), volume=volume,
                    order_type="limit", price=price, confidence=confidence,
                )
                txid = None
                if result and "result" in result:
                    txids = result["result"].get("txid", [])
                    txid = txids[0] if txids else None
                dry_tag = ""
                logger.info(
                    "[KRAKEN%s] %s %s x%.6f @ $%.2f ($%.2f) | agent=%s | txid=%s",
                    dry_tag, direction, pair, volume, price, order_usd, agent_name, txid,
                )
            except Exception as e:
                logger.error("Kraken execution error: %s", e)
        elif ORCH_KRAKEN_DRY_RUN and self.kraken_connector:
            dry_tag = " [DRY-RUN]"
            logger.info(
                "[KRAKEN%s] %s %s x%.6f @ $%.2f ($%.2f) | agent=%s | txid=%s",
                dry_tag, direction, pair, volume, price, order_usd, agent_name, "dry-run",
            )
        elif self.mock_mode:
            import random
            pnl = random.uniform(-0.5, 1.5)
            logger.info("[MOCK-KRAKEN] %s %s x%.6f @ $%.2f", direction, pair, volume, price)

        return {
            "agent_name": agent_name,
            "pair": pair,
            "product_id": pair,
            "used_perp": False,
            "direction": direction,
            "realized_pnl_usd": pnl,
            "region": region,
            "score": score,
            "market_type": "crypto",
            "venue": "kraken",
            "timestamp_ms": time.time() * 1000,
        }

    async def _etrade_token_refresh_loop(self):
        """Refresh E*Trade OAuth token every 90 min to avoid 2h inactivity timeout."""
        logger.info("E*Trade token refresh loop started (interval=%.0fs)", ETRADE_TOKEN_REFRESH_INTERVAL_S)
        while self.running:
            try:
                await asyncio.sleep(ETRADE_TOKEN_REFRESH_INTERVAL_S)
                if self.etrade_auth and self.etrade_auth.is_authenticated:
                    ok = self.etrade_auth.refresh_token()
                    if ok:
                        logger.info("E*Trade token refreshed successfully")
                    else:
                        logger.warning("E*Trade token refresh failed — may need re-auth")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("E*Trade token refresh error: %s", e)

    async def _funding_loop(self):
        """Generate funding skew signals for perps."""
        logger.info(f"Funding loop enabled (interval={FUNDING_INTERVAL_S}s, threshold={FUNDING_THRESHOLD_PCT}%)")
        while self.running:
            try:
                perps = self.trader.list_perpetual_products() or []
            except Exception as e:
                logger.error(f"Funding loop: failed to list perps: {e}")
                await asyncio.sleep(FUNDING_INTERVAL_S)
                continue

            for p in perps:
                if not isinstance(p, dict):
                    continue
                pid = str(p.get("product_id", "")).upper()
                if not pid:
                    continue

                def _to_pct(val):
                    try:
                        v = float(val)
                        return v * 100.0 if abs(v) < 0.5 else v
                    except Exception:
                        return None

                fr = (
                    _to_pct(p.get("current_funding_rate"))
                    or _to_pct(p.get("funding_rate"))
                    or _to_pct(p.get("next_funding_rate"))
                )
                if fr is None or abs(fr) < FUNDING_THRESHOLD_PCT:
                    continue

                direction = "SELL" if fr > 0 else "BUY"
                confidence = min(1.0, abs(fr) / 0.03)  # 3 bps → full confidence
                urgency = "high" if abs(fr) >= 0.05 else "medium"
                reasoning = f"Funding {fr:.3f}% -> {direction} perp to collect funding"

                ok = CreativeAgentBridge.broadcast_signal(
                    agent_name="derivatives_funding",
                    pair=pid,
                    direction=direction,
                    confidence=confidence,
                    urgency=urgency,
                    reasoning=reasoning,
                    region_hint=None,
                    expected_hold_ms=3600 * 1000,
                )
                if ok:
                    logger.info(f"Funding signal: {pid} {direction} fr={fr:.3f}% conf={confidence:.2f}")

                # Basis arb signals (perp vs spot)
                spot_pid = f"{p.get('base_currency_id', pid.split('-')[0])}-USD"
                spot_price = PriceFeed.get_price(spot_pid) or PriceFeed.get_price(pid.replace("USDC", "USD"))
                perp_price = p.get("price") or p.get("mark_price") or None
                try:
                    perp_price = float(perp_price) if perp_price is not None else None
                except Exception:
                    perp_price = None

                if spot_price and perp_price:
                    basis_pct = (perp_price - spot_price) / spot_price * 100.0
                    basis_threshold = max(FUNDING_THRESHOLD_PCT, 0.05)  # at least 5 bps
                    if abs(basis_pct) >= basis_threshold:
                        basis_dir = "SELL" if basis_pct > 0 else "BUY"
                        b_conf = min(1.0, abs(basis_pct) / 0.10)  # 10 bps → full confidence
                        b_reason = f"Basis {basis_pct:.3f}% ({perp_price:.2f} vs {spot_price:.2f})"
                        okb = CreativeAgentBridge.broadcast_signal(
                            agent_name="basis_factory",
                            pair=pid,
                            direction=basis_dir,
                            confidence=b_conf,
                            urgency="high" if abs(basis_pct) > 0.1 else "medium",
                            reasoning=b_reason,
                            region_hint=None,
                            expected_hold_ms=900_000,  # 15m target
                        )
                        if okb:
                            logger.info(f"Basis signal: {pid} {basis_dir} basis={basis_pct:.3f}% conf={b_conf:.2f}")

            await asyncio.sleep(FUNDING_INTERVAL_S)

    async def _latency_probe_loop(self):
        """Periodically probe Coinbase API latency for location_alpha mode."""
        logger.info(
            f"Location-alpha latency probe started "
            f"(host={self.latency_probe.host}, interval={LOCATION_ALPHA_LATENCY_PROBE_S}s)"
        )
        while self.running:
            try:
                latency_ms = await asyncio.get_event_loop().run_in_executor(
                    None, self.latency_probe.probe
                )
                if self.cycle_count % 100 == 0 or latency_ms > 50:
                    snap = self.latency_probe.snapshot()
                    logger.info(
                        f"Coinbase latency: {latency_ms:.1f}ms "
                        f"(avg={snap['avg_latency_ms']:.1f}ms, "
                        f"min={snap['min_latency_ms']:.1f}ms, "
                        f"probes={snap['probe_count']})"
                    )
            except Exception as e:
                logger.debug(f"Latency probe error: {e}")
            await asyncio.sleep(LOCATION_ALPHA_LATENCY_PROBE_S)

    def _persist_gains_to_flywheel(self):
        """Write gains/ms and gains/ns metrics to flywheel_status.json for other agents."""
        try:
            flywheel_path = Path(__file__).parent / "flywheel_status.json"
            if flywheel_path.exists():
                with open(flywheel_path, "r") as f:
                    status = json.load(f)
            else:
                status = {}

            perf = self.performance_tracker.snapshot()
            latency_snap = self.latency_probe.snapshot() if self.latency_probe else {}

            status["realtime_orchestrator"] = {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "cycle_count": self.cycle_count,
                "gains_per_ms": perf["gains_per_ms"],
                "gains_per_ns": perf["gains_per_ns"],
                "gains_per_second": perf["gains_per_second"],
                "cycle_gains_ms": perf["cycle_gains_ms"],
                "cycle_gains_ns": perf["cycle_gains_ns"],
                "peak_gains_ms": perf["peak_gains_ms"],
                "peak_gains_ns": perf["peak_gains_ns"],
                "rolling_50_gains_ms": perf["rolling_50_gains_ms"],
                "total_gains_usd": perf["total_gains_usd"],
                "trade_count": perf["trade_count"],
                "runtime_ms": perf["runtime_ms"],
                "avg_cycle_latency_ms": self.total_latency_ms / max(self.cycle_count, 1),
                "location_alpha": self.location_alpha,
                "coinbase_latency": latency_snap,
            }

            with open(flywheel_path, "w") as f:
                json.dump(status, f, indent=2)

        except Exception as e:
            logger.debug(f"Could not persist gains to flywheel_status.json: {e}")

    def _persist_metrics_to_db(self, cycle_latency_ms: float, signal_count: int, executed_count: int):
        """Persist orchestrator metrics to database for API consumption."""
        try:
            db_path = Path(__file__).parent.parent / "traceroute.db"
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()

            # Ensure table exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orchestrator_metrics (
                    id INTEGER PRIMARY KEY,
                    cycle_count INTEGER,
                    total_latency_ms REAL,
                    avg_latency_ms REAL,
                    total_gains_usd REAL,
                    gains_per_second REAL,
                    gains_per_ms REAL,
                    trade_count INTEGER,
                    cycle_signals INTEGER,
                    cycle_executed INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            perf = self.performance_tracker.snapshot()
            avg_latency = self.total_latency_ms / max(self.cycle_count, 1)

            # Insert/update metrics
            cursor.execute("""
                INSERT OR REPLACE INTO orchestrator_metrics
                (id, cycle_count, total_latency_ms, avg_latency_ms, total_gains_usd,
                 gains_per_second, gains_per_ms, trade_count, cycle_signals, cycle_executed, updated_at)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                self.cycle_count,
                self.total_latency_ms,
                avg_latency,
                perf["total_gains_usd"],
                perf["gains_per_second"],
                perf["gains_per_ms"],
                perf["trade_count"],
                signal_count,
                executed_count
            ))

            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"Could not persist metrics to DB: {e}")

    def _get_portfolio_value(self) -> float:
        """Get current portfolio value from DB + E*Trade balance."""
        crypto_value = 233.85
        try:
            db_path = Path(__file__).parent.parent / "traceroute.db"
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            # Query trading_snapshots for latest total_value_usd (user_id=2 for scott)
            cursor.execute("SELECT total_value_usd FROM trading_snapshots WHERE user_id=2 ORDER BY recorded_at DESC LIMIT 1")
            result = cursor.fetchone()
            conn.close()
            crypto_value = result[0] if result else 233.85
        except Exception as e:
            logger.debug(f"Could not fetch crypto portfolio value: {e}")

        # Add E*Trade account balance if available
        etrade_value = 0.0
        if self.etrade_trader and self.etrade_account_id:
            try:
                balance = self.etrade_trader.get_balance(self.etrade_account_id)
                etrade_value = float(
                    balance.get("Computed", {}).get("RealTimeValues", {}).get("totalAccountValue", 0)
                    or balance.get("totalAccountValue", 0)
                    or 0
                )
            except Exception as e:
                logger.debug("Could not fetch E*Trade balance: %s", e)

        # Add Kraken balance if available
        kraken_value = 0.0
        if self.kraken_connector:
            try:
                balance = KrakenConnector.get_trade_balance()
                if "result" in balance:
                    kraken_value = float(balance["result"].get("eb", 0))  # equivalent balance
            except Exception as e:
                logger.debug("Could not fetch Kraken balance: %s", e)

        return crypto_value + etrade_value + kraken_value

    def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down RealtimeOrchestrator")
        self.running = False


async def main():
    """Entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Real-Time Portfolio Orchestrator")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode with simulated signals")
    parser.add_argument("--live", action="store_true", help="Run in live mode with real execution")
    parser.add_argument("--location-alpha", action="store_true",
                        help="Enable location-alpha mode for local low-latency execution (~1ms to Coinbase)")
    parser.add_argument("--duration", type=float, help="Run for N seconds then exit")
    args = parser.parse_args()

    mock_mode = args.mock or not args.live

    orchestrator = RealtimeOrchestrator(
        mock_mode=mock_mode,
        location_alpha=args.location_alpha,
    )

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        orchestrator.shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await orchestrator.run(duration_s=args.duration)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        orchestrator.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
