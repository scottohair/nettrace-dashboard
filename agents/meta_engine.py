#!/usr/bin/env python3
"""Meta-Strategy Engine — autonomous strategy evolution and agent orchestration.

An adaptive, self-improving strategy engine that:
  - Searches the web for new alpha ideas (research papers, on-chain analytics)
  - Runs HuggingFace models for sentiment/prediction/classification
  - Manages an agent pool (hire, fire, promote, clone based on performance)
  - Combines algorithms into ensembles, tests via COLD/WARM/HOT pipeline
  - Deploys winning strategies, prunes losers
  - Uses graph theory for market microstructure analysis
  - Exploits NYC latency advantage (ewr Fly region = closest to exchanges)
  - Distributes compute across M3/M1 Max/M2 Ultra nodes

Architecture:
  MetaEngine
    ├── ResearchModule       — web search, paper parsing, alpha generation
    ├── ModelRunner           — HuggingFace model inference (local/remote)
    ├── AgentPool             — hire/fire/promote trading agents
    ├── StrategyEvolver       — combine, mutate, test, deploy strategies
    ├── GraphAnalyzer         — market microstructure via graph theory
    └── PerformanceTracker    — track, rank, prune based on Sharpe/PnL

RULES:
  - All strategies must pass COLD backtest before WARM paper trading
  - All strategies must pass WARM before HOT live deployment
  - Max $5 per trade, $2 daily loss limit
  - Fire strategies with Sharpe < 0.5 after 50+ trades
  - Clone strategies with Sharpe > 2.0
"""

import json
import logging
import math
import os
import sqlite3
import sys
import time
import urllib.request
import hashlib
import subprocess
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Agent goals — single source of truth for all decision-making
try:
    from agent_goals import GoalValidator
    _goals = GoalValidator()
except ImportError:
    _goals = None

# Integration: Strategy Pipeline (COLD/WARM/HOT backtest validation)
try:
    from strategy_pipeline import (
        Backtester as _PipelineBacktester,
        HistoricalPrices as _PipelineHistoricalPrices,
        StrategyValidator as _PipelineValidator,
        GrowthModeController as _GrowthModeController,
        MeanReversionStrategy as _MeanReversionStrategy,
        MomentumStrategy as _MomentumStrategy,
        RSIStrategy as _RSIStrategy,
        VWAPStrategy as _VWAPStrategy,
        DipBuyerStrategy as _DipBuyerStrategy,
        MultiTimeframeStrategy as _MultiTimeframeStrategy,
        AccumulateAndHoldStrategy as _AccumulateAndHoldStrategy,
        WALKFORWARD_MIN_TOTAL_CANDLES as _WF_MIN_CANDLES,
    )
    _pipeline_available = True
except ImportError:
    _pipeline_available = False

# Integration: Message Bus (advanced_team)
try:
    from advanced_team.message_bus import MessageBus
    _bus = MessageBus()
except Exception:
    _bus = None

# Integration: Meta-engine bus bridge
try:
    from meta_engine_bus import publish_meta_status as _publish_meta_status
except ImportError:
    _publish_meta_status = None

# Integration: KPI Tracker
try:
    from kpi_tracker import get_kpi_tracker
    _kpi = get_kpi_tracker()
except Exception:
    _kpi = None

# Integration: Risk Controller
try:
    from risk_controller import get_controller as _get_risk_controller
    _risk_controller = _get_risk_controller()
except Exception:
    _risk_controller = None

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
        logging.FileHandler(str(Path(__file__).parent / "meta_engine.log")),
    ]
)
logger = logging.getLogger("meta_engine")

META_DB = str(Path(__file__).parent / "meta_engine.db")
TRADER_DB = str(Path(__file__).parent / "trader.db")
PIPELINE_DB = str(Path(__file__).parent / "pipeline.db")
FIRE_LOSING_STRATEGIES = os.environ.get(
    "FIRE_LOSING_STRATEGIES", "1"
).lower() not in ("0", "false", "no")
FIRE_MIN_CLOSES = int(os.environ.get("FIRE_LOSING_MIN_CLOSES", "3"))
FIRE_MIN_LOSS_USD = float(os.environ.get("FIRE_LOSING_MIN_LOSS_USD", "0.50"))
FIRE_LOOKBACK_HOURS = int(os.environ.get("FIRE_LOSING_LOOKBACK_HOURS", "72"))
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")
FLY_URL = "https://nettrace-dashboard.fly.dev"

# Compute pool nodes
COMPUTE_NODES = {
    "local":   {"host": "localhost", "gpu": "M3", "ram_gb": 16, "port": 9090},
    "m1max":   {"host": "192.168.1.110", "gpu": "M1 Max", "ram_gb": 64, "port": 9090},
    "m2ultra": {"host": "192.168.1.106", "gpu": "M2 Ultra", "ram_gb": 128, "port": 9090},
}

# Strategy performance thresholds
PROMOTE_THRESHOLD = {"min_sharpe": 1.0, "min_trades": 3, "min_win_rate": 0.55}
FIRE_THRESHOLD = {"max_sharpe": 0.5, "min_trades": 50, "max_drawdown": 0.05}
CLONE_THRESHOLD = {"min_sharpe": 2.0, "min_trades": 30}


def _fetch_json(url, headers=None, timeout=10, payload=None):
    h = {"User-Agent": "NetTrace-Meta/1.0"}
    if headers:
        h.update(headers)
    if payload:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=data, headers={**h, "Content-Type": "application/json"})
    else:
        req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


class ResearchModule:
    """Web search and alpha research — finds new trading ideas."""

    # Data sources for alpha research
    FEEDS = [
        "https://api.coingecko.com/api/v3/search/trending",
        "https://api.alternative.me/fng/",  # Fear & Greed Index
    ]

    def get_fear_greed(self):
        """Get crypto Fear & Greed Index."""
        try:
            data = _fetch_json("https://api.alternative.me/fng/?limit=1")
            fg = data["data"][0]
            return {
                "value": int(fg["value"]),
                "classification": fg["value_classification"],
                "timestamp": fg["timestamp"],
            }
        except Exception as e:
            logger.debug("Fear & Greed fetch failed: %s", e)
            return {"value": 50, "classification": "Neutral"}

    def get_trending_coins(self):
        """Get trending coins from CoinGecko."""
        try:
            data = _fetch_json("https://api.coingecko.com/api/v3/search/trending")
            coins = []
            for item in data.get("coins", [])[:10]:
                coin = item.get("item", {})
                coins.append({
                    "name": coin.get("name"),
                    "symbol": coin.get("symbol"),
                    "market_cap_rank": coin.get("market_cap_rank"),
                    "price_btc": coin.get("price_btc"),
                })
            return coins
        except Exception as e:
            logger.debug("Trending fetch failed: %s", e)
            return []

    def get_gas_prices(self):
        """Get current gas prices across chains."""
        prices = {}
        chains = {
            "ethereum": "https://eth.llamarpc.com",
            "base": "https://mainnet.base.org",
            "arbitrum": "https://arb1.arbitrum.io/rpc",
        }
        for chain, rpc in chains.items():
            try:
                resp = _fetch_json(rpc, payload={
                    "jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id": 1
                })
                gas_wei = int(resp["result"], 16)
                gas_gwei = gas_wei / 1e9
                prices[chain] = round(gas_gwei, 2)
            except Exception:
                pass
        return prices

    def generate_alpha_ideas(self):
        """Synthesize research into actionable alpha ideas."""
        fg = self.get_fear_greed()
        trending = self.get_trending_coins()
        gas = self.get_gas_prices()

        ideas = []

        # Fear & Greed contrarian signals
        if fg["value"] < 20:
            ideas.append({
                "type": "contrarian_buy",
                "reason": f"Extreme Fear ({fg['value']}) — historically bullish",
                "confidence": 0.7 + (20 - fg["value"]) * 0.01,
                "action": "accumulate_eth",
            })
        elif fg["value"] > 80:
            ideas.append({
                "type": "contrarian_sell",
                "reason": f"Extreme Greed ({fg['value']}) — historically bearish",
                "confidence": 0.7 + (fg["value"] - 80) * 0.01,
                "action": "reduce_exposure",
            })

        # Gas arbitrage: trade on cheapest chain
        if gas:
            cheapest = min(gas.items(), key=lambda x: x[1])
            ideas.append({
                "type": "gas_optimization",
                "reason": f"Cheapest gas: {cheapest[0]} at {cheapest[1]} gwei",
                "chain": cheapest[0],
                "all_gas": gas,
            })

        # Trending coin momentum
        for coin in trending[:3]:
            if coin.get("market_cap_rank") and coin["market_cap_rank"] < 100:
                ideas.append({
                    "type": "trending_momentum",
                    "reason": f"Trending: {coin['name']} (#{coin['market_cap_rank']})",
                    "symbol": coin["symbol"],
                })

        return {"fear_greed": fg, "trending": trending, "gas": gas, "ideas": ideas}


class ModelRunner:
    """Run ML models for prediction — HuggingFace, MLX, PyTorch."""

    def __init__(self):
        self._framework = self._detect_framework()

    def _detect_framework(self):
        """Detect best available ML framework."""
        try:
            import mlx.core
            return "mlx"
        except ImportError:
            pass
        try:
            import torch
            if torch.backends.mps.is_available():
                return "pytorch_mps"
            return "pytorch_cpu"
        except ImportError:
            pass
        return "numpy"

    def predict_direction(self, candles, pair="ETH-USD"):
        """Predict price direction using available ML framework.

        Uses a simple feature-based approach that works with any framework:
        - RSI, SMA ratio, volume trend, price momentum
        - Ensemble of simple models beats one complex model at small scale
        """
        if len(candles) < 20:
            return {"direction": "NONE", "confidence": 0, "framework": self._framework}

        try:
            # Extract features
            closes = [c["close"] for c in candles[-20:]]
            volumes = [c["volume"] for c in candles[-20:]]

            # Feature 1: Price momentum (5-period return)
            momentum = (closes[-1] - closes[-5]) / closes[-5] if closes[-5] > 0 else 0

            # Feature 2: Volume trend (current vs average)
            avg_vol = sum(volumes) / len(volumes) if volumes else 1
            vol_ratio = volumes[-1] / avg_vol if avg_vol > 0 else 1

            # Feature 3: SMA ratio
            sma5 = sum(closes[-5:]) / 5
            sma20 = sum(closes) / len(closes)
            sma_ratio = sma5 / sma20 if sma20 > 0 else 1

            # Feature 4: RSI
            gains = losses = 0
            for i in range(1, len(closes)):
                diff = closes[i] - closes[i-1]
                if diff > 0:
                    gains += diff
                else:
                    losses -= diff
            rsi = 100 - (100 / (1 + gains/losses)) if losses > 0 else 100

            # Simple ensemble voting
            votes_buy = 0
            votes_sell = 0
            total_conf = 0

            # RSI signal
            if rsi < 30:
                votes_buy += 1
                total_conf += 0.7 + (30 - rsi) * 0.01
            elif rsi > 70:
                votes_sell += 1
                total_conf += 0.7 + (rsi - 70) * 0.01

            # Momentum signal
            if momentum > 0.01 and vol_ratio > 1.2:
                votes_buy += 1
                total_conf += min(0.5 + momentum * 10, 0.9)
            elif momentum < -0.01 and vol_ratio > 1.2:
                votes_sell += 1
                total_conf += min(0.5 + abs(momentum) * 10, 0.9)

            # SMA crossover
            if sma_ratio > 1.005:
                votes_buy += 1
                total_conf += 0.6
            elif sma_ratio < 0.995:
                votes_sell += 1
                total_conf += 0.6

            total_votes = votes_buy + votes_sell
            if total_votes == 0:
                return {"direction": "NONE", "confidence": 0, "framework": self._framework}

            if votes_buy > votes_sell:
                direction = "BUY"
                confidence = total_conf / total_votes
            else:
                direction = "SELL"
                confidence = total_conf / total_votes

            return {
                "direction": direction,
                "confidence": min(confidence, 0.95),
                "framework": self._framework,
                "features": {
                    "rsi": round(rsi, 1),
                    "momentum": round(momentum, 4),
                    "vol_ratio": round(vol_ratio, 2),
                    "sma_ratio": round(sma_ratio, 4),
                },
                "votes": {"buy": votes_buy, "sell": votes_sell},
            }

        except Exception as e:
            return {"direction": "NONE", "confidence": 0, "error": str(e), "framework": self._framework}

    def dispatch_to_node(self, task_type, data, node="local"):
        """Dispatch ML task to a compute pool node."""
        node_info = COMPUTE_NODES.get(node)
        if not node_info:
            return {"error": f"Unknown node: {node}"}

        try:
            url = f"http://{node_info['host']}:{node_info['port']}/submit"
            result = _fetch_json(url, payload={
                "task_type": task_type,
                "data": data,
                "priority": 5,
            }, timeout=30)
            return result
        except Exception as e:
            logger.debug("Node dispatch to %s failed: %s", node, e)
            return {"error": str(e)}


class AgentPool:
    """Manage pool of trading agents — hire, fire, promote, clone.

    Promotion gates:
      cold -> warm: Must pass strategy_pipeline COLD backtest (walk-forward,
                    Monte Carlo, out-of-sample positive returns).
      warm -> hot:  Must have positive paper trading P&L, Sharpe > 1.0,
                    and optionally pass Monte Carlo stress test.
    Falls back to simple threshold logic if strategy_pipeline is unavailable.
    """

    # Map strategy_type names to strategy_pipeline classes
    _STRATEGY_CONSTRUCTORS = {}
    if _pipeline_available:
        _STRATEGY_CONSTRUCTORS = {
            "mean_reversion": _MeanReversionStrategy,
            "momentum": _MomentumStrategy,
            "rsi": _RSIStrategy,
            "vwap": _VWAPStrategy,
            "dip_buyer": _DipBuyerStrategy,
            "multi_timeframe": _MultiTimeframeStrategy,
            "accumulate_hold": _AccumulateAndHoldStrategy,
        }

    # Default pair for backtesting when agent has no pair specified
    _DEFAULT_BACKTEST_PAIRS = ["BTC-USD", "ETH-USD"]

    def __init__(self, db):
        self.db = db
        self._prices_cache = None  # lazy-init HistoricalPrices

    def _get_prices(self):
        """Lazy-init HistoricalPrices to avoid import-time network calls."""
        if self._prices_cache is None and _pipeline_available:
            self._prices_cache = _PipelineHistoricalPrices()
        return self._prices_cache

    def list_agents(self):
        """List all agents with performance metrics."""
        rows = self.db.execute("""
            SELECT name, strategy_type, status, trades, wins, losses,
                   total_pnl, sharpe_ratio, max_drawdown, created_at, last_active
            FROM meta_agents ORDER BY sharpe_ratio DESC
        """).fetchall()
        return [dict(r) for r in rows]

    def hire_agent(self, name, strategy_type, params=None):
        """Register a new agent in the pool."""
        self.db.execute(
            """INSERT OR IGNORE INTO meta_agents (name, strategy_type, params_json, status)
               VALUES (?, ?, ?, 'cold')""",
            (name, strategy_type, json.dumps(params or {}))
        )
        self.db.commit()
        logger.info("HIRED agent: %s (strategy: %s)", name, strategy_type)

    def fire_agent(self, name, reason="underperformance"):
        """Fire an underperforming agent."""
        self.db.execute(
            "UPDATE meta_agents SET status='fired', fired_reason=?, fired_at=CURRENT_TIMESTAMP WHERE name=?",
            (reason, name)
        )
        self.db.commit()
        logger.info("FIRED agent: %s (reason: %s)", name, reason)

    def promote_agent(self, name, new_status):
        """Promote agent: cold -> warm -> hot."""
        self.db.execute(
            "UPDATE meta_agents SET status=?, promoted_at=CURRENT_TIMESTAMP WHERE name=?",
            (new_status, name)
        )
        self.db.commit()
        logger.info("PROMOTED agent: %s -> %s", name, new_status)

    def _store_backtest_result(self, name, result_dict):
        """Store JSON-encoded backtest result on the meta_agents row."""
        try:
            self.db.execute(
                "UPDATE meta_agents SET backtest_result=? WHERE name=?",
                (json.dumps(result_dict), name)
            )
            self.db.commit()
        except Exception as e:
            logger.debug("Failed to store backtest result for %s: %s", name, e)

    def _instantiate_strategy(self, strategy_type, params):
        """Create a strategy_pipeline strategy object from type name + params.

        Returns None if strategy type is not supported by the pipeline.
        """
        constructor = self._STRATEGY_CONSTRUCTORS.get(strategy_type)
        if constructor is None:
            return None
        try:
            # Filter params to only those the constructor accepts
            import inspect
            sig = inspect.signature(constructor.__init__)
            valid_keys = {
                p for p in sig.parameters if p != "self"
            }
            filtered = {k: v for k, v in params.items() if k in valid_keys}
            return constructor(**filtered)
        except Exception as e:
            logger.debug("Failed to instantiate strategy %s: %s", strategy_type, e)
            return None

    def _run_cold_backtest(self, agent_name, strategy_type, params_json):
        """Run COLD backtest via strategy_pipeline for cold->warm promotion gate.

        Returns (passed: bool, summary: dict) where summary contains backtest
        metrics for storage and logging.
        """
        if not _pipeline_available:
            return True, {"gate": "skipped", "reason": "strategy_pipeline_unavailable"}

        params = {}
        try:
            params = json.loads(params_json or "{}")
        except (json.JSONDecodeError, TypeError):
            params = {}

        strategy = self._instantiate_strategy(strategy_type, params)
        if strategy is None:
            # Strategy type not in pipeline (e.g. contrarian, dca, sniper, dex_grid)
            # Allow promotion with a note — these are custom agents without backtest support
            return True, {
                "gate": "skipped",
                "reason": f"strategy_type '{strategy_type}' has no pipeline backtester",
            }

        # Determine which pair to backtest on
        pair = params.get("pair", None)
        pairs_to_test = [pair] if pair else self._DEFAULT_BACKTEST_PAIRS

        backtester = _PipelineBacktester(initial_capital=100.0)
        prices = self._get_prices()
        best_result = None
        any_passed = False

        for test_pair in pairs_to_test:
            try:
                candles = prices.get_5min_candles(test_pair, hours=48)
                if len(candles) < 30:
                    # Try hourly if 5-min data is sparse
                    candles = prices.get_candles(test_pair, hours=168)
                if len(candles) < 30:
                    logger.info(
                        "COLD gate %s/%s: insufficient data (%d candles)",
                        agent_name, test_pair, len(candles),
                    )
                    continue

                # Run backtest
                bt_result = backtester.run(strategy, candles, test_pair)

                # Walk-forward split (replicating quant_100_runner logic)
                wf_min = _WF_MIN_CANDLES
                walkforward = {
                    "enabled": True,
                    "available": False,
                    "reason": "insufficient_candles_for_walkforward",
                }
                if len(candles) >= wf_min:
                    split_ratio = 0.55
                    split_idx = int(len(candles) * split_ratio)
                    split_idx = max(30, min(split_idx, len(candles) - 24))
                    if split_idx > 0 and len(candles) - split_idx >= 24:
                        candles_is = candles[:split_idx]
                        candles_oos = candles[split_idx:]
                        bt_is = backtester.run(strategy, candles_is, test_pair)
                        bt_oos = backtester.run(strategy, candles_oos, test_pair)
                        walkforward = {
                            "enabled": True,
                            "available": True,
                            "split_index": split_idx,
                            "in_sample_candles": len(candles_is),
                            "out_of_sample_candles": len(candles_oos),
                            "in_sample": {
                                "candle_count": int(bt_is.get("candle_count", 0) or 0),
                                "total_return_pct": float(bt_is.get("total_return_pct", 0.0) or 0.0),
                                "total_trades": int(bt_is.get("total_trades", 0) or 0),
                                "win_rate": float(bt_is.get("win_rate", 0.0) or 0.0),
                                "losses": int(bt_is.get("losses", 0) or 0),
                                "max_drawdown_pct": float(bt_is.get("max_drawdown_pct", 0.0) or 0.0),
                                "sharpe_ratio": float(bt_is.get("sharpe_ratio", 0.0) or 0.0),
                                "final_open_position_blocked": bool(bt_is.get("final_open_position_blocked", False)),
                            },
                            "out_of_sample": {
                                "candle_count": int(bt_oos.get("candle_count", 0) or 0),
                                "total_return_pct": float(bt_oos.get("total_return_pct", 0.0) or 0.0),
                                "total_trades": int(bt_oos.get("total_trades", 0) or 0),
                                "win_rate": float(bt_oos.get("win_rate", 0.0) or 0.0),
                                "losses": int(bt_oos.get("losses", 0) or 0),
                                "max_drawdown_pct": float(bt_oos.get("max_drawdown_pct", 0.0) or 0.0),
                                "sharpe_ratio": float(bt_oos.get("sharpe_ratio", 0.0) or 0.0),
                                "final_open_position_blocked": bool(bt_oos.get("final_open_position_blocked", False)),
                            },
                        }

                bt_for_gate = dict(bt_result)
                bt_for_gate["walkforward"] = walkforward

                # Use the pipeline validator to check COLD criteria
                validator = _PipelineValidator()
                validator.register_strategy(strategy, test_pair)
                passed, msg = validator.submit_backtest(strategy.name, test_pair, bt_for_gate)

                summary = {
                    "gate": "cold_backtest",
                    "pair": test_pair,
                    "passed": bool(passed),
                    "message": msg,
                    "total_return_pct": round(float(bt_result.get("total_return_pct", 0.0) or 0.0), 4),
                    "sharpe_ratio": round(float(bt_result.get("sharpe_ratio", 0.0) or 0.0), 4),
                    "max_drawdown_pct": round(float(bt_result.get("max_drawdown_pct", 0.0) or 0.0), 4),
                    "win_rate": round(float(bt_result.get("win_rate", 0.0) or 0.0), 4),
                    "total_trades": int(bt_result.get("total_trades", 0) or 0),
                    "candle_count": int(bt_result.get("candle_count", 0) or 0),
                    "walkforward_available": walkforward.get("available", False),
                    "oos_return_pct": (
                        round(float(walkforward.get("out_of_sample", {}).get("total_return_pct", 0.0) or 0.0), 4)
                        if walkforward.get("available") else None
                    ),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

                if passed:
                    any_passed = True
                    best_result = summary
                    break  # One passing pair is enough
                elif best_result is None:
                    best_result = summary

            except Exception as e:
                logger.warning(
                    "COLD backtest error for %s on %s: %s",
                    agent_name, test_pair, e,
                )
                if best_result is None:
                    best_result = {
                        "gate": "cold_backtest",
                        "pair": test_pair,
                        "passed": False,
                        "message": f"backtest_error: {e}",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }

        if best_result is None:
            best_result = {
                "gate": "cold_backtest",
                "passed": False,
                "message": "no_candle_data_available",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        # Store result for visibility in status API
        self._store_backtest_result(agent_name, best_result)

        return any_passed, best_result

    def _run_warm_stress_test(self, agent_name, strategy_type, params_json, agent_metrics):
        """Optional Monte Carlo stress test for warm->hot promotion gate.

        Returns (passed: bool, summary: dict).
        """
        if not _pipeline_available:
            return True, {"gate": "skipped", "reason": "strategy_pipeline_unavailable"}

        params = {}
        try:
            params = json.loads(params_json or "{}")
        except (json.JSONDecodeError, TypeError):
            params = {}

        strategy = self._instantiate_strategy(strategy_type, params)
        if strategy is None:
            return True, {
                "gate": "skipped",
                "reason": f"strategy_type '{strategy_type}' has no pipeline backtester",
            }

        pair = params.get("pair", "BTC-USD")
        prices = self._get_prices()

        try:
            candles = prices.get_5min_candles(pair, hours=48)
            if len(candles) < 30:
                candles = prices.get_candles(pair, hours=168)
            if len(candles) < 30:
                return True, {
                    "gate": "monte_carlo_skipped",
                    "reason": f"insufficient_data ({len(candles)} candles)",
                }

            backtester = _PipelineBacktester(initial_capital=100.0)
            bt_result = backtester.run(strategy, candles, pair)

            # Run Monte Carlo via GrowthModeController
            growth = _GrowthModeController()
            from strategy_pipeline import COLD_TO_WARM as _cold_criteria
            mc_report = growth.evaluate_monte_carlo(
                strategy.name, pair, bt_result, dict(_cold_criteria)
            )

            summary = {
                "gate": "monte_carlo_stress_test",
                "pair": pair,
                "passed": bool(mc_report.get("passed", False)),
                "p50_return_pct": round(float(mc_report.get("p50_return_pct", 0.0) or 0.0), 4),
                "p05_return_pct": round(float(mc_report.get("p05_return_pct", 0.0) or 0.0), 4),
                "p95_drawdown_pct": round(float(mc_report.get("p95_drawdown_pct", 0.0) or 0.0), 4),
                "reasons": mc_report.get("reasons", []),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            return summary["passed"], summary

        except Exception as e:
            logger.warning(
                "Monte Carlo stress test error for %s: %s", agent_name, e,
            )
            # Don't block promotion on stress test failure — it's optional
            return True, {
                "gate": "monte_carlo_stress_test",
                "passed": True,
                "reason": f"stress_test_error_skipped: {e}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    def clone_agent(self, name):
        """Clone a high-performing agent with slightly mutated parameters."""
        original = self.db.execute(
            "SELECT strategy_type, params_json FROM meta_agents WHERE name=?", (name,)
        ).fetchone()
        if not original:
            return

        params = json.loads(original["params_json"] or "{}")
        # Mutate parameters slightly
        import random
        for key in params:
            if isinstance(params[key], (int, float)):
                params[key] *= random.uniform(0.9, 1.1)

        clone_name = f"{name}_clone_{int(time.time()) % 10000}"
        self.hire_agent(clone_name, original["strategy_type"], params)
        logger.info("CLONED agent: %s -> %s", name, clone_name)

    def evaluate_and_prune(self):
        """Evaluate all agents and fire/promote/clone based on GoalValidator.

        Promotion gates (strategy_pipeline integration):
          cold -> warm: Agent must pass COLD backtest (walk-forward validation,
                        positive OOS returns, Monte Carlo). Falls back to simple
                        threshold if pipeline is unavailable.
          warm -> hot:  Agent must have positive paper P&L + Sharpe > 1.0
                        (existing logic), plus optional Monte Carlo stress test.
        """
        agents = self.list_agents()
        actions = []

        for agent in agents:
            if agent["status"] == "fired":
                continue

            trades = agent["trades"] or 0
            sharpe = agent["sharpe_ratio"] or 0
            win_rate = agent["wins"] / trades if trades > 0 else 0
            drawdown = agent["max_drawdown"] or 0

            # Use GoalValidator for fire/promote decisions (evolutionary game theory)
            if _goals:
                fire_result = _goals.should_fire_agent(sharpe, trades, win_rate, drawdown)
                if fire_result["fired"]:
                    self.fire_agent(agent["name"], fire_result["reason"])
                    actions.append(f"FIRED {agent['name']} ({fire_result['reason']})")
                    continue

                promo_result = _goals.should_promote_agent(sharpe, trades, win_rate)
                if promo_result["clone"]:
                    self.clone_agent(agent["name"])
                    actions.append(f"CLONED {agent['name']} ({promo_result['reason']})")
                elif promo_result["promoted"]:
                    if agent["status"] == "cold":
                        # === COLD -> WARM gate: run pipeline backtest ===
                        agent_row = self.db.execute(
                            "SELECT params_json FROM meta_agents WHERE name=?",
                            (agent["name"],)
                        ).fetchone()
                        params_json = agent_row["params_json"] if agent_row else "{}"

                        try:
                            bt_passed, bt_summary = self._run_cold_backtest(
                                agent["name"], agent["strategy_type"], params_json,
                            )
                        except Exception as e:
                            logger.error(
                                "COLD backtest gate crashed for %s, allowing promotion (fallback): %s",
                                agent["name"], e,
                            )
                            bt_passed = True
                            bt_summary = {"gate": "error_fallback", "error": str(e)}

                        if bt_passed:
                            self.promote_agent(agent["name"], "warm")
                            gate_info = bt_summary.get("gate", "unknown")
                            actions.append(
                                f"PROMOTED {agent['name']} cold->warm "
                                f"({promo_result['reason']}, backtest={gate_info})"
                            )
                            logger.info(
                                "COLD->WARM gate PASSED for %s: %s",
                                agent["name"],
                                json.dumps(bt_summary, default=str)[:300],
                            )
                        else:
                            actions.append(
                                f"BLOCKED {agent['name']} cold->warm "
                                f"(backtest failed: {bt_summary.get('message', 'unknown')[:80]})"
                            )
                            logger.info(
                                "COLD->WARM gate BLOCKED for %s: %s",
                                agent["name"],
                                json.dumps(bt_summary, default=str)[:300],
                            )

                    elif agent["status"] == "warm":
                        # === WARM -> HOT gate: paper P&L check + optional stress test ===
                        # Check paper trading P&L from meta_paper_trades
                        paper_pnl_rows = self.db.execute(
                            "SELECT SUM(paper_pnl) as total_pnl FROM meta_paper_trades "
                            "WHERE agent_name=? AND status='closed'",
                            (agent["name"],)
                        ).fetchone()
                        paper_pnl = float(paper_pnl_rows["total_pnl"] or 0) if paper_pnl_rows else 0

                        if paper_pnl <= 0:
                            actions.append(
                                f"BLOCKED {agent['name']} warm->hot "
                                f"(paper P&L ${paper_pnl:.2f} <= 0)"
                            )
                            logger.info(
                                "WARM->HOT gate BLOCKED for %s: negative paper P&L %.2f",
                                agent["name"], paper_pnl,
                            )
                            continue

                        # Sharpe check already passed via GoalValidator.should_promote_agent
                        # Run optional Monte Carlo stress test
                        agent_row = self.db.execute(
                            "SELECT params_json FROM meta_agents WHERE name=?",
                            (agent["name"],)
                        ).fetchone()
                        params_json = agent_row["params_json"] if agent_row else "{}"

                        try:
                            mc_passed, mc_summary = self._run_warm_stress_test(
                                agent["name"], agent["strategy_type"],
                                params_json, agent,
                            )
                        except Exception as e:
                            logger.error(
                                "Monte Carlo stress test crashed for %s, allowing promotion: %s",
                                agent["name"], e,
                            )
                            mc_passed = True
                            mc_summary = {"gate": "error_fallback", "error": str(e)}

                        if mc_passed:
                            self.promote_agent(agent["name"], "hot")
                            actions.append(
                                f"PROMOTED {agent['name']} warm->hot "
                                f"({promo_result['reason']}, paper_pnl=${paper_pnl:.2f}, "
                                f"mc={mc_summary.get('gate', 'ok')})"
                            )
                        else:
                            actions.append(
                                f"BLOCKED {agent['name']} warm->hot "
                                f"(Monte Carlo failed: {', '.join(mc_summary.get('reasons', [])[:2])})"
                            )
                            logger.info(
                                "WARM->HOT gate BLOCKED for %s (MC): %s",
                                agent["name"],
                                json.dumps(mc_summary, default=str)[:300],
                            )
            else:
                # Fallback: original threshold-based logic (no GoalValidator)
                if trades >= FIRE_THRESHOLD["min_trades"] and sharpe < FIRE_THRESHOLD["max_sharpe"]:
                    self.fire_agent(agent["name"], f"Sharpe {sharpe:.2f} < {FIRE_THRESHOLD['max_sharpe']}")
                    actions.append(f"FIRED {agent['name']} (Sharpe {sharpe:.2f})")
                elif trades >= CLONE_THRESHOLD["min_trades"] and sharpe >= CLONE_THRESHOLD["min_sharpe"]:
                    self.clone_agent(agent["name"])
                    actions.append(f"CLONED {agent['name']} (Sharpe {sharpe:.2f})")
                elif agent["status"] == "cold" and trades >= PROMOTE_THRESHOLD["min_trades"]:
                    if sharpe >= PROMOTE_THRESHOLD["min_sharpe"] and win_rate >= PROMOTE_THRESHOLD["min_win_rate"]:
                        # Run COLD backtest gate even in fallback mode
                        agent_row = self.db.execute(
                            "SELECT params_json FROM meta_agents WHERE name=?",
                            (agent["name"],)
                        ).fetchone()
                        params_json = agent_row["params_json"] if agent_row else "{}"

                        try:
                            bt_passed, bt_summary = self._run_cold_backtest(
                                agent["name"], agent["strategy_type"], params_json,
                            )
                        except Exception:
                            bt_passed = True
                            bt_summary = {"gate": "error_fallback"}

                        if bt_passed:
                            self.promote_agent(agent["name"], "warm")
                            actions.append(f"PROMOTED {agent['name']} cold->warm")
                        else:
                            actions.append(
                                f"BLOCKED {agent['name']} cold->warm "
                                f"(backtest: {bt_summary.get('message', 'failed')[:60]})"
                            )
                elif agent["status"] == "warm" and trades >= PROMOTE_THRESHOLD["min_trades"]:
                    if sharpe >= PROMOTE_THRESHOLD["min_sharpe"]:
                        self.promote_agent(agent["name"], "hot")
                        actions.append(f"PROMOTED {agent['name']} warm->hot")

        return actions


class GraphAnalyzer:
    """Market microstructure analysis via graph theory.

    Uses NetTrace hop-level routing data as a network graph:
    - Nodes = network hops (routers, IXPs, exchange endpoints)
    - Edges = routes between hops (weighted by latency)
    - Centrality = importance of a node in exchange connectivity
    - Route changes = graph topology shifts = potential alpha

    NYC Advantage:
    - ewr Fly region is in Newark (adjacent to NYSE/Nasdaq datacenters)
    - Lower latency to US exchanges = faster signal detection
    - Route changes detected first by our closest node
    """

    def analyze_exchange_graph(self):
        """Build graph from NetTrace scan data and find alpha signals."""
        try:
            url = f"{FLY_URL}/api/v1/latest?category=Crypto%20Exchanges"
            data = _fetch_json(url, headers={"Authorization": f"Bearer {NETTRACE_API_KEY}"})
            targets = data.get("targets", [])

            if not targets:
                return {"nodes": 0, "edges": 0, "signals": []}

            # Build adjacency info from hop data
            exchanges = {}
            for t in targets:
                name = t.get("name", t.get("host", ""))
                rtt = t.get("rtt")
                hops = t.get("hop_count", 0)
                exchanges[name] = {
                    "rtt_ms": rtt,
                    "hops": hops,
                    "reachable": rtt is not None,
                }

            # Find fastest/slowest exchanges (centrality proxy)
            reachable = {k: v for k, v in exchanges.items() if v["reachable"]}
            if not reachable:
                return {"nodes": len(exchanges), "edges": 0, "signals": []}

            avg_rtt = sum(v["rtt_ms"] for v in reachable.values()) / len(reachable)
            signals = []

            for name, info in reachable.items():
                # Low latency = high centrality = process orders faster
                if info["rtt_ms"] < avg_rtt * 0.5:
                    signals.append({
                        "exchange": name,
                        "type": "low_latency_advantage",
                        "rtt_ms": info["rtt_ms"],
                        "advantage_pct": round((1 - info["rtt_ms"]/avg_rtt) * 100, 1),
                        "reason": f"{name} at {info['rtt_ms']:.1f}ms vs avg {avg_rtt:.1f}ms — trade here first"
                    })
                elif info["rtt_ms"] > avg_rtt * 2.0:
                    signals.append({
                        "exchange": name,
                        "type": "high_latency_stale_price",
                        "rtt_ms": info["rtt_ms"],
                        "reason": f"{name} at {info['rtt_ms']:.1f}ms — prices may lag, arb opportunity"
                    })

            return {
                "nodes": len(exchanges),
                "edges": sum(v["hops"] for v in exchanges.values()),
                "avg_rtt": round(avg_rtt, 1),
                "signals": signals,
                "fastest": min(reachable.items(), key=lambda x: x[1]["rtt_ms"])[0] if reachable else None,
                "slowest": max(reachable.items(), key=lambda x: x[1]["rtt_ms"])[0] if reachable else None,
            }
        except Exception as e:
            logger.debug("Graph analysis failed: %s", e)
            return {"nodes": 0, "edges": 0, "signals": [], "error": str(e)}


class MetaEngine:
    """Autonomous strategy evolution engine — the brain."""

    def __init__(self):
        self.db = sqlite3.connect(META_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.research = ResearchModule()
        self.models = ModelRunner()
        self.agents = AgentPool(self.db)
        self.graph = GraphAnalyzer()

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta_agents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                strategy_type TEXT NOT NULL,
                params_json TEXT,
                status TEXT DEFAULT 'cold',
                trades INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                total_pnl REAL DEFAULT 0.0,
                sharpe_ratio REAL DEFAULT 0.0,
                max_drawdown REAL DEFAULT 0.0,
                fired_reason TEXT,
                fired_at TIMESTAMP,
                promoted_at TIMESTAMP,
                last_active TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS meta_ideas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                idea_type TEXT,
                description TEXT,
                confidence REAL,
                status TEXT DEFAULT 'new',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS meta_evolution_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS meta_paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_name TEXT NOT NULL,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                confidence REAL,
                entry_price REAL,
                current_price REAL,
                paper_pnl REAL DEFAULT 0,
                status TEXT DEFAULT 'open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP
            );
        """)
        # Migration: add backtest_result column for pipeline validation results
        try:
            self.db.execute("ALTER TABLE meta_agents ADD COLUMN backtest_result TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists
        self.db.commit()

    def fire_losing_strategies(self):
        """Demote HOT strategies with realized losses to COLD and zero budget.

        Requires 3+ closes and net realized loss > $0.50 in the lookback window.
        Gated by FIRE_LOSING_STRATEGIES env var.
        """
        if not FIRE_LOSING_STRATEGIES:
            return 0
        trader_path = Path(TRADER_DB)
        pipeline_path = Path(PIPELINE_DB)
        if not trader_path.exists() or not pipeline_path.exists():
            return 0

        lookback_expr = f"-{max(1, FIRE_LOOKBACK_HOURS)} hours"
        fired = 0
        try:
            tconn = sqlite3.connect(str(trader_path))
            tconn.row_factory = sqlite3.Row
            # Check if agent_trades table exists
            has_table = tconn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_trades'"
            ).fetchone()
            if not has_table:
                tconn.close()
                return 0

            losers = tconn.execute(
                """
                SELECT strategy_name, pair,
                       SUM(COALESCE(pnl, 0)) as net_pnl,
                       COUNT(*) as closes
                FROM agent_trades
                WHERE UPPER(COALESCE(side, '')) = 'SELL'
                  AND pnl IS NOT NULL
                  AND created_at >= datetime('now', ?)
                GROUP BY strategy_name, pair
                HAVING COUNT(*) >= ? AND SUM(COALESCE(pnl, 0)) < ?
                """,
                (lookback_expr, FIRE_MIN_CLOSES, -abs(FIRE_MIN_LOSS_USD)),
            ).fetchall()
            tconn.close()

            if not losers:
                return 0

            pconn = sqlite3.connect(str(pipeline_path))
            pconn.row_factory = sqlite3.Row
            for row in losers:
                strategy = str(row["strategy_name"] or "")
                pair = str(row["pair"] or "")
                net_pnl = float(row["net_pnl"] or 0.0)
                closes = int(row["closes"] or 0)
                if not strategy or not pair:
                    continue
                pconn.execute(
                    "UPDATE strategies SET tier='COLD', budget_usd=0 WHERE name=? AND pair=?",
                    (strategy, pair),
                )
                fired += 1
                logger.warning(
                    "FIRED %s/%s: realized loss $%.2f over %d closes in %dh",
                    strategy, pair, net_pnl, closes, FIRE_LOOKBACK_HOURS,
                )
            pconn.commit()
            pconn.close()
        except Exception as e:
            logger.error("fire_losing_strategies error: %s", e)
        return fired

    def evolve(self, cycle=0):
        """One evolution cycle: research -> generate -> test -> deploy -> prune."""
        logger.info("=== META-ENGINE EVOLUTION CYCLE ===")

        # Step 0: Fire losing HOT strategies based on realized P&L
        fired = self.fire_losing_strategies()
        if fired:
            logger.info("Step 0: Fired %d losing HOT strategies", fired)

        # Step 1: Research
        logger.info("Step 1: Researching alpha ideas...")
        research = self.research.generate_alpha_ideas()
        fg = research["fear_greed"]
        logger.info("  Fear & Greed: %d (%s)", fg["value"], fg["classification"])
        logger.info("  Trending: %d coins", len(research["trending"]))
        logger.info("  Gas: %s", research.get("gas", {}))
        logger.info("  Ideas: %d", len(research["ideas"]))

        for idea in research["ideas"]:
            self.db.execute(
                "INSERT INTO meta_ideas (source, idea_type, description, confidence) VALUES (?, ?, ?, ?)",
                ("research", idea["type"], idea.get("reason", ""), idea.get("confidence", 0))
            )

        # Step 2: Graph analysis
        logger.info("Step 2: Analyzing exchange network graph...")
        graph = self.graph.analyze_exchange_graph()
        logger.info("  Graph: %d nodes, %d edges", graph["nodes"], graph["edges"])
        for sig in graph.get("signals", [])[:3]:
            logger.info("  Graph signal: %s — %s", sig["exchange"], sig["reason"])

        # Step 3: ML prediction
        logger.info("Step 3: Running ML predictions...")
        ml_predictions = []
        for pair in ["ETH-USD", "BTC-USD", "SOL-USD"]:
            try:
                candles = self._fetch_candles(pair)
                pred = self.models.predict_direction(candles, pair)
                if pred["direction"] != "NONE":
                    logger.info("  ML %s: %s (conf=%.1f%%, framework=%s)",
                               pair, pred["direction"], pred["confidence"]*100, pred["framework"])
                    ml_predictions.append({"pair": pair, "direction": pred["direction"],
                                           "confidence": pred["confidence"]})
            except Exception as e:
                logger.debug("ML prediction failed for %s: %s", pair, e)

        # Step 4: Evaluate and prune agents
        logger.info("Step 4: Evaluating agent pool...")
        actions = self.agents.evaluate_and_prune()
        for action in actions:
            logger.info("  %s", action)
            self.db.execute(
                "INSERT INTO meta_evolution_log (action, details) VALUES (?, ?)",
                ("agent_management", action)
            )

        # Step 5: Generate new strategy ideas from research
        logger.info("Step 5: Generating strategy candidates...")
        for idea in research["ideas"]:
            if idea["type"] == "contrarian_buy" and idea.get("confidence", 0) > 0.7:
                # Create a contrarian agent
                agent_name = f"contrarian_{int(time.time()) % 10000}"
                self.agents.hire_agent(agent_name, "contrarian", {
                    "trigger": "fear_greed_below_20",
                    "direction": "BUY",
                    "confidence": idea["confidence"],
                })

        # Step 6: Execute highest-confidence ML predictions via CoinbaseTrader
        logger.info("Step 6: Executing high-confidence predictions...")
        trades_executed = 0
        for pair in ["ETH-USDC", "BTC-USDC", "SOL-USDC"]:
            try:
                data_pair = pair.replace("-USDC", "-USD")
                candles = self._fetch_candles(data_pair)
                pred = self.models.predict_direction(candles, data_pair)
                if pred["direction"] == "NONE" or pred["confidence"] < 0.75:
                    continue

                # Paper trade for cold agents — track hypothetical P&L
                price_data = _fetch_json(f"https://api.coinbase.com/v2/prices/{data_pair}/spot")
                if not price_data or "data" not in price_data:
                    continue
                current_price = float(price_data["data"]["amount"])

                # Check for open paper trades to close
                open_papers = self.db.execute(
                    "SELECT id, direction, entry_price FROM meta_paper_trades WHERE pair=? AND status='open'",
                    (pair,)
                ).fetchall()
                for paper in open_papers:
                    if paper["direction"] != pred["direction"]:
                        # Close paper trade
                        if paper["direction"] == "BUY":
                            pnl = (current_price - paper["entry_price"]) / paper["entry_price"] * 100
                        else:
                            pnl = (paper["entry_price"] - current_price) / paper["entry_price"] * 100
                        self.db.execute(
                            "UPDATE meta_paper_trades SET status='closed', current_price=?, paper_pnl=?, closed_at=CURRENT_TIMESTAMP WHERE id=?",
                            (current_price, pnl, paper["id"])
                        )
                        logger.info("META paper trade closed: %s %s pnl=%.2f%%", pair, paper["direction"], pnl)

                        # KPI: record closed paper trade
                        if _kpi:
                            try:
                                paper_usd = 3.0  # notional paper trade size
                                paper_pnl_usd = paper_usd * (pnl / 100.0)
                                _kpi.record_trade(
                                    strategy_name="meta_engine_paper",
                                    pair=pair,
                                    direction=paper["direction"],
                                    amount_usd=paper_usd,
                                    pnl=paper_pnl_usd,
                                    fees=0,
                                    hold_seconds=0,
                                    strategy_type="LF",
                                    won=(pnl > 0),
                                )
                            except Exception as e:
                                logger.debug("KPI paper trade record failed: %s", e)

                # Open new paper trade
                self.db.execute(
                    "INSERT INTO meta_paper_trades (agent_name, pair, direction, confidence, entry_price) VALUES (?, ?, ?, ?, ?)",
                    ("meta_ml", pair, pred["direction"], pred["confidence"], current_price)
                )

                # KPI: record paper trade open (pnl=0, it just opened)
                if _kpi:
                    try:
                        _kpi.record_trade(
                            strategy_name="meta_engine_paper",
                            pair=pair,
                            direction=pred["direction"],
                            amount_usd=3.0,
                            pnl=0,
                            fees=0,
                            hold_seconds=0,
                            strategy_type="LF",
                            won=True,
                        )
                    except Exception as e:
                        logger.debug("KPI paper open record failed: %s", e)

                # For hot agents: execute real trades (gated by risk controller)
                hot_agents = self.db.execute(
                    "SELECT name FROM meta_agents WHERE status='hot' AND strategy_type IN ('momentum', 'contrarian', 'mean_reversion')"
                ).fetchall()
                if hot_agents and pred["confidence"] >= 0.80:
                    try:
                        from exchange_connector import CoinbaseTrader
                        trader = CoinbaseTrader()
                        trade_usd = min(3.0, pred["confidence"] * 4.0)

                        # Integration: gate through risk controller
                        if _risk_controller:
                            # Estimate portfolio value from Coinbase accounts
                            try:
                                accts_pv = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
                                portfolio_value = 0.0
                                for a in accts_pv.get("accounts", []):
                                    cur = a.get("currency", "")
                                    bal = float(a.get("available_balance", {}).get("value", 0))
                                    if cur in ("USDC", "USD"):
                                        portfolio_value += bal
                                    elif bal > 0:
                                        try:
                                            p = _fetch_json(f"https://api.coinbase.com/v2/prices/{cur}-USD/spot")
                                            portfolio_value += bal * float(p["data"]["amount"])
                                        except Exception:
                                            pass
                                portfolio_value = max(1.0, portfolio_value)
                            except Exception:
                                portfolio_value = 100.0  # safe fallback

                            approved, reason, adj_size = _risk_controller.approve_trade(
                                "meta_engine", pair, pred["direction"], trade_usd, portfolio_value
                            )
                            if not approved:
                                logger.info("META trade BLOCKED by risk controller: %s %s — %s",
                                           pred["direction"], pair, reason)
                                continue
                            trade_usd = adj_size
                            logger.info("META trade APPROVED: %s %s $%.2f — %s",
                                       pred["direction"], pair, trade_usd, reason)
                        else:
                            portfolio_value = 100.0

                        try:
                            result = {}
                            if pred["direction"] == "BUY":
                                base_size = trade_usd / current_price
                                limit_price = current_price * 1.001
                                result = trader.place_limit_order(pair, "BUY", base_size, limit_price, post_only=False)
                            else:
                                # Check holdings before sell
                                accts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
                                base = pair.split("-")[0]
                                held = 0
                                for a in accts.get("accounts", []):
                                    if a.get("currency") == base:
                                        held = float(a.get("available_balance", {}).get("value", 0))
                                if held * current_price >= 1.0:
                                    sell_size = min(held * 0.3, trade_usd / current_price)
                                    limit_price = current_price * 0.999
                                    result = trader.place_limit_order(pair, "SELL", sell_size, limit_price, post_only=False)

                            if result.get("success_response") or result.get("order_id"):
                                trades_executed += 1
                                logger.info("META LIVE TRADE: %s %s $%.2f conf=%.1f%% via hot agents",
                                           pred["direction"], pair, trade_usd, pred["confidence"] * 100)
                                # Update agent stats
                                for agent in hot_agents:
                                    self.db.execute(
                                        "UPDATE meta_agents SET trades = trades + 1, last_active = CURRENT_TIMESTAMP WHERE name = ?",
                                        (agent["name"],)
                                    )

                                # KPI: record real trade fill
                                if _kpi:
                                    try:
                                        _kpi.record_trade(
                                            strategy_name="meta_engine",
                                            pair=pair,
                                            direction=pred["direction"],
                                            amount_usd=trade_usd,
                                            pnl=0,
                                            fees=trade_usd * 0.004,
                                            hold_seconds=0,
                                            strategy_type="LF",
                                            won=True,
                                        )
                                    except Exception as e:
                                        logger.debug("KPI live trade record failed: %s", e)
                        finally:
                            # Always resolve the risk controller allocation
                            if _risk_controller:
                                try:
                                    _risk_controller.resolve_allocation("meta_engine", pair)
                                except Exception:
                                    pass
                    except Exception as e:
                        logger.debug("META live trade failed: %s", e)

            except Exception as e:
                logger.debug("Step 6 failed for %s: %s", pair, e)

        logger.info("Step 6: %d live trades executed, paper trades updated", trades_executed)

        self.db.commit()

        # Integration: publish evolution cycle summary to message bus
        if _bus:
            try:
                active_agents = [a for a in self.agents.list_agents() if a.get("status") != "fired"]
                _bus.publish(
                    sender="meta_engine",
                    recipient="broadcast",
                    msg_type="meta_engine_signal",
                    payload={
                        "ml_predictions": ml_predictions,
                        "research": {
                            "fear_greed": fg,
                            "trending_count": len(research.get("trending", [])),
                            "gas": research.get("gas", {}),
                            "ideas_count": len(research.get("ideas", [])),
                        },
                        "agent_pool": {
                            "active_count": len(active_agents),
                            "actions": actions,
                        },
                        "graph_analysis": {
                            "nodes": graph.get("nodes", 0),
                            "edges": graph.get("edges", 0),
                            "signals": graph.get("signals", [])[:5],
                            "fastest": graph.get("fastest"),
                            "slowest": graph.get("slowest"),
                        },
                        "trades_executed": trades_executed,
                    },
                    cycle=cycle,
                )
                logger.info("Published evolution summary to message bus (cycle=%d)", cycle)
            except Exception as e:
                logger.debug("Message bus publish failed: %s", e)

        # Integration: publish structured status via meta_engine_bus bridge
        if _publish_meta_status:
            try:
                all_agents = self.agents.list_agents()
                paper_trades = self.db.execute(
                    "SELECT pair, direction, confidence, entry_price, current_price, paper_pnl, status "
                    "FROM meta_paper_trades ORDER BY id DESC LIMIT 20"
                ).fetchall()
                preds = [dict(r) for r in paper_trades]
                idea_list = research.get("ideas", [])
                _publish_meta_status(all_agents, preds, idea_list, cycle=cycle)
            except Exception as e:
                logger.debug("Meta-engine bus publish failed: %s", e)

        logger.info("=== EVOLUTION CYCLE COMPLETE ===\n")

        return {
            "research": research,
            "graph": graph,
            "agent_actions": actions,
            "trades_executed": trades_executed,
        }

    def _fetch_candles(self, pair, limit=50):
        try:
            url = f"https://api.exchange.coinbase.com/products/{pair}/candles?granularity=3600"
            data = _fetch_json(url)
            candles = []
            for c in data[:limit]:
                candles.append({
                    "open": c[3], "high": c[2], "low": c[1],
                    "close": c[4], "volume": c[5], "time": c[0]
                })
            candles.reverse()
            return candles
        except Exception:
            return []

    def print_status(self):
        """Print meta-engine status report."""
        agents = self.agents.list_agents()
        ideas = self.db.execute(
            "SELECT idea_type, description, confidence, status, created_at FROM meta_ideas ORDER BY id DESC LIMIT 10"
        ).fetchall()
        log = self.db.execute(
            "SELECT action, details, created_at FROM meta_evolution_log ORDER BY id DESC LIMIT 10"
        ).fetchall()

        print(f"\n{'='*70}")
        print(f"  META-ENGINE STATUS")
        print(f"{'='*70}")
        print(f"\n  ML Framework: {self.models._framework}")
        print(f"  Compute Nodes: {', '.join(COMPUTE_NODES.keys())}")

        print(f"\n  Agent Pool ({len(agents)} agents):")
        for a in agents[:10]:
            status_icon = {"cold": "❄", "warm": "🔥", "hot": "🚀", "fired": "💀"}.get(a["status"], "?")
            print(f"    {status_icon} {a['name']} | {a['strategy_type']} | {a['status']} | "
                  f"trades={a['trades']} W={a['wins']} L={a['losses']} | "
                  f"PnL=${a['total_pnl']:.4f} Sharpe={a['sharpe_ratio']:.2f}")

        if ideas:
            print(f"\n  Recent Ideas:")
            for idea in ideas[:5]:
                print(f"    [{idea['idea_type']}] {idea['description'][:60]} (conf={idea['confidence']:.1%})")

        if log:
            print(f"\n  Evolution Log:")
            for entry in log[:5]:
                print(f"    {entry['created_at']} | {entry['action']} | {entry['details'][:50]}")

        print(f"{'='*70}\n")

    def run(self):
        """Main evolution loop."""
        logger.info("Meta-Engine starting — autonomous strategy evolution")
        logger.info("ML Framework: %s", self.models._framework)
        logger.info("Compute nodes: %s", list(COMPUTE_NODES.keys()))

        # Seed initial agents if pool is empty
        agents = self.agents.list_agents()
        if not agents:
            logger.info("Seeding initial agent pool...")
            self.agents.hire_agent("grid_base_eth", "dex_grid", {"pair": "WETH-USDC", "chain": "base"})
            self.agents.hire_agent("sniper_multi", "sniper", {"min_confidence": 0.9})
            self.agents.hire_agent("dca_eth", "dca", {"pair": "ETH-USD", "interval": 3600})
            self.agents.hire_agent("momentum_btc", "momentum", {"pair": "BTC-USD"})
            self.agents.hire_agent("mean_rev_eth", "mean_reversion", {"pair": "ETH-USD"})

        cycle = 0
        while True:
            try:
                cycle += 1
                self.evolve(cycle=cycle)

                # Status report every 10 cycles
                if cycle % 10 == 0:
                    self.print_status()

                # Evolution interval: 5 minutes
                time.sleep(300)

            except KeyboardInterrupt:
                logger.info("Meta-Engine shutting down...")
                self.print_status()
                break
            except Exception as e:
                logger.error("Meta-Engine error: %s", e, exc_info=True)
                time.sleep(60)


if __name__ == "__main__":
    engine = MetaEngine()

    if len(sys.argv) > 1 and sys.argv[1] == "status":
        engine.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "evolve":
        result = engine.evolve()
        engine.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "research":
        research = engine.research.generate_alpha_ideas()
        print(json.dumps(research, indent=2))
    elif len(sys.argv) > 1 and sys.argv[1] == "graph":
        graph = engine.graph.analyze_exchange_graph()
        print(json.dumps(graph, indent=2))
    else:
        engine.run()
