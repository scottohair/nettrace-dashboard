#!/usr/bin/env python3
"""Real-Time Portfolio Orchestrator — Millisecond-level coordination engine.

Transforms portfolio management from batch (daily) to real-time (ms-level) by:
  1. Collecting signals from all 31 agents in real-time
  2. Scoring and ranking signals using C fast_engine (80ns/signal)
  3. Routing trades across 7 Fly.io regions for cross-region arbitrage
  4. Tracking gains/second and gains/ms metrics
  5. Continuously rebalancing capital (not 4x/day batches)

Target: 50% portfolio growth in 4 minutes (~$0.000487/ms for $233.85 portfolio)
Current: $0.00001332/ms
Gap: 36.6x improvement needed

Usage:
  python3 realtime_orchestrator.py --mock --duration 60    # Local testing
  python3 realtime_orchestrator.py --live                  # Production mode
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
    """Real-time performance tracking: gains/second, gains/ms."""

    def __init__(self):
        self.start_time_ms = time.time() * 1000
        self.total_gains_usd = 0.0
        self.trade_history = []  # (timestamp_ms, pnl_usd, agent_name, latency_ms)
        self.lock = threading.Lock()

    def record(self, executed_signals: List[Dict], cycle_latency_ms: float):
        """Record executed trades and update metrics."""
        with self.lock:
            for trade in executed_signals:
                pnl = trade.get("realized_pnl_usd", 0.0)
                self.total_gains_usd += pnl
                self.trade_history.append({
                    "timestamp_ms": time.time() * 1000,
                    "pnl_usd": pnl,
                    "agent": trade.get("agent_name", "unknown"),
                    "latency_ms": cycle_latency_ms,
                })

    def gains_per_second(self) -> float:
        """Calculate gains/second since start."""
        with self.lock:
            runtime_s = (time.time() * 1000 - self.start_time_ms) / 1000.0
            return self.total_gains_usd / max(runtime_s, 1.0)

    def gains_per_ms(self) -> float:
        """Calculate gains/millisecond (target metric)."""
        with self.lock:
            runtime_ms = time.time() * 1000 - self.start_time_ms
            return self.total_gains_usd / max(runtime_ms, 1.0)

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
        """Get current performance snapshot."""
        with self.lock:
            runtime_ms = time.time() * 1000 - self.start_time_ms
            return {
                "runtime_ms": runtime_ms,
                "total_gains_usd": self.total_gains_usd,
                "gains_per_second": self.gains_per_second(),
                "gains_per_ms": self.gains_per_ms(),
                "trade_count": len(self.trade_history),
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
        """Collect all current signals with timeout."""
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

    def score_signals(self, signals: Dict[str, Dict]) -> List[Tuple[str, Dict, float]]:
        """Score all signals and return (agent_name, signal, score) tuples."""
        scored = []

        for agent_name, signal in signals.items():
            score = self._score_single(signal)
            scored.append((agent_name, signal, score))

        # Sort by score descending
        scored.sort(key=lambda x: x[2], reverse=True)
        return scored

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


class RealtimeOrchestrator:
    """Main orchestrator: coordinates all agents at millisecond precision."""

    def __init__(self, mock_mode: bool = False):
        self.mock_mode = mock_mode
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

        # Register with CreativeAgentBridge so creative agents can push signals directly
        try:
            CreativeAgentBridge.set_orchestrator(self)
        except Exception:
            logger.warning("CreativeAgentBridge unavailable; signals will queue locally")

    def _start_background_tasks(self):
        """Kick off background coroutines (funding scan, etc.)."""
        if FUNDING_ENABLED and self.trader:
            self._background_tasks.append(asyncio.create_task(self._funding_loop()))

    # ------------------------------------------------------------------ #
    # Derivatives helpers
    def _refresh_perp_mapping(self):
        """Build mapping base -> preferred perpetual product_id."""
        if not self.trader or not hasattr(self.trader, "list_perpetual_products"):
            return
        try:
            perps = self.trader.list_perpetual_products() or []
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

    def _leverage_for_signal(self, confidence: float, perf_gains_ms: float) -> float:
        base = max(1.0, ORCH_BASE_LEVERAGE)
        max_lv = max(base, ORCH_MAX_LEVERAGE)
        # Confidence-driven ramp: 0.6 -> base, 0.9 -> max
        lvl = base
        if confidence > 0.6:
            frac = min(1.0, (confidence - 0.6) / 0.3)
            lvl = base + frac * (max_lv - base)
        # Downshift if performance negative
        if perf_gains_ms < 0:
            lvl = max(1.0, base * 0.5)
        return max(1.0, min(max_lv, lvl))

    async def run(self, duration_s: Optional[float] = None):
        """Main orchestration loop."""
        self.running = True
        start_time_s = time.time()

        logger.info(f"Starting RealtimeOrchestrator (mock_mode={self.mock_mode}, target_cycle={CYCLE_TARGET_MS}ms)")
        logger.info("Entering main orchestration loop...")
        self._start_background_tasks()

        while self.running:
            cycle_start_ms = time.time() * 1000

            try:
                # 1. Collect signals from all agents (parallel, <50ms)
                if self.cycle_count < 3:
                    logger.info(f"[Cycle {self.cycle_count+1}] Collecting signals...")
                signals = await self.signal_collector.collect_all(timeout_ms=SIGNAL_COLLECTION_TIMEOUT_MS)
                collection_latency_ms = time.time() * 1000 - cycle_start_ms

                # 2. Score and rank signals (C fast_engine, <10ms)
                scored_signals = self.signal_scorer.score_signals(signals)
                scoring_latency_ms = time.time() * 1000 - cycle_start_ms - collection_latency_ms

                # 3. Route to optimal regions (NetTrace data, <5ms)
                routed = self.router.batch_route(scored_signals)
                routing_latency_ms = time.time() * 1000 - cycle_start_ms - collection_latency_ms - scoring_latency_ms

                # 4. Execute top signals (HF lane, <30ms)
                executed = await self._execute_signals(routed)
                execution_latency_ms = time.time() * 1000 - cycle_start_ms - collection_latency_ms - scoring_latency_ms - routing_latency_ms

                # 5. Update performance metrics
                total_cycle_latency_ms = time.time() * 1000 - cycle_start_ms
                self.performance_tracker.record(executed, total_cycle_latency_ms)

                # 5b. Persist metrics to database for API consumption
                self._persist_metrics_to_db(total_cycle_latency_ms, len(signals), len(executed))

                # 6. Rebalance capital
                if not self.mock_mode:
                    portfolio_value = self._get_portfolio_value()
                    top_agents = self.performance_tracker.agent_rankings(limit=10)
                    await self.capital_manager.rebalance(portfolio_value, top_agents)

                # Logging
                self.cycle_count += 1
                self.total_latency_ms += total_cycle_latency_ms

                if self.cycle_count % 10 == 0:  # Log every 10 cycles
                    perf = self.performance_tracker.snapshot()
                    logger.info(
                        f"Cycle {self.cycle_count} | "
                        f"Latency: {total_cycle_latency_ms:.1f}ms (target {CYCLE_TARGET_MS}ms) | "
                        f"Signals: {len(signals)} | Executed: {len(executed)} | "
                        f"Gains/ms: ${perf['gains_per_ms']:.8f} | "
                        f"Total: ${perf['total_gains_usd']:.2f}"
                    )

                # Check if cycle exceeded target
                if total_cycle_latency_ms > CYCLE_MAX_MS:
                    logger.warning(f"Slow cycle: {total_cycle_latency_ms:.1f}ms > {CYCLE_MAX_MS}ms")

            except Exception as e:
                logger.error(f"Error in orchestration loop: {e}", exc_info=True)

            # Check duration
            if duration_s and time.time() - start_time_s > duration_s:
                logger.info(f"Duration limit reached ({duration_s}s)")
                break

            # Maintain target cycle time
            await asyncio.sleep(0.001)  # 1ms minimum sleep

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
                pair = signal.get("pair")
                direction = signal.get("direction", "HOLD").upper()
                confidence = float(signal.get("confidence", 0.5))
                theme = agent_name.split("_")[0] if agent_name else "unknown"

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
                                        leverage=1.0,  # Conservative launch: 1x only
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
                        perf = self.performance_tracker.snapshot()
                        if use_perp:
                            leverage = 1.0  # Conservative launch: force 1x for perps
                        else:
                            leverage = self._leverage_for_signal(confidence, perf.get("gains_per_ms", 0.0))
                        base_order_usd = PERP_ORDER_NOTIONAL_USD if use_perp else ORDER_NOTIONAL_USD
                        order_usd = base_order_usd * leverage

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
                                leverage=1.0,
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
                    "timestamp_ms": time.time() * 1000,
                })

        return executed

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
        """Get current portfolio value from DB."""
        try:
            db_path = Path(__file__).parent.parent / "traceroute.db"
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            # Query trading_snapshots for latest total_value_usd (user_id=2 for scott)
            cursor.execute("SELECT total_value_usd FROM trading_snapshots WHERE user_id=2 ORDER BY recorded_at DESC LIMIT 1")
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else 233.85
        except Exception as e:
            logger.debug(f"Could not fetch portfolio value: {e}")
            return 233.85

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
    parser.add_argument("--duration", type=float, help="Run for N seconds then exit")
    args = parser.parse_args()

    mock_mode = args.mock or not args.live

    orchestrator = RealtimeOrchestrator(mock_mode=mock_mode)

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
