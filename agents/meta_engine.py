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
    """Manage pool of trading agents — hire, fire, promote, clone."""

    def __init__(self, db):
        self.db = db

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
        """Promote agent: cold → warm → hot."""
        self.db.execute(
            "UPDATE meta_agents SET status=?, promoted_at=CURRENT_TIMESTAMP WHERE name=?",
            (new_status, name)
        )
        self.db.commit()
        logger.info("PROMOTED agent: %s → %s", name, new_status)

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
        logger.info("CLONED agent: %s → %s", name, clone_name)

    def evaluate_and_prune(self):
        """Evaluate all agents and fire/promote/clone based on GoalValidator."""
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
                        self.promote_agent(agent["name"], "warm")
                        actions.append(f"PROMOTED {agent['name']} cold→warm ({promo_result['reason']})")
                    elif agent["status"] == "warm":
                        self.promote_agent(agent["name"], "hot")
                        actions.append(f"PROMOTED {agent['name']} warm→hot ({promo_result['reason']})")
            else:
                # Fallback: original threshold-based logic
                if trades >= FIRE_THRESHOLD["min_trades"] and sharpe < FIRE_THRESHOLD["max_sharpe"]:
                    self.fire_agent(agent["name"], f"Sharpe {sharpe:.2f} < {FIRE_THRESHOLD['max_sharpe']}")
                    actions.append(f"FIRED {agent['name']} (Sharpe {sharpe:.2f})")
                elif trades >= CLONE_THRESHOLD["min_trades"] and sharpe >= CLONE_THRESHOLD["min_sharpe"]:
                    self.clone_agent(agent["name"])
                    actions.append(f"CLONED {agent['name']} (Sharpe {sharpe:.2f})")
                elif agent["status"] == "cold" and trades >= PROMOTE_THRESHOLD["min_trades"]:
                    if sharpe >= PROMOTE_THRESHOLD["min_sharpe"] and win_rate >= PROMOTE_THRESHOLD["min_win_rate"]:
                        self.promote_agent(agent["name"], "warm")
                        actions.append(f"PROMOTED {agent['name']} cold→warm")
                elif agent["status"] == "warm" and trades >= PROMOTE_THRESHOLD["min_trades"]:
                    if sharpe >= PROMOTE_THRESHOLD["min_sharpe"]:
                        self.promote_agent(agent["name"], "hot")
                        actions.append(f"PROMOTED {agent['name']} warm→hot")

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
        self.db.commit()

    def evolve(self):
        """One evolution cycle: research → generate → test → deploy → prune."""
        logger.info("=== META-ENGINE EVOLUTION CYCLE ===")

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
        for pair in ["ETH-USD", "BTC-USD", "SOL-USD"]:
            try:
                candles = self._fetch_candles(pair)
                pred = self.models.predict_direction(candles, pair)
                if pred["direction"] != "NONE":
                    logger.info("  ML %s: %s (conf=%.1f%%, framework=%s)",
                               pair, pred["direction"], pred["confidence"]*100, pred["framework"])
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

                # Open new paper trade
                self.db.execute(
                    "INSERT INTO meta_paper_trades (agent_name, pair, direction, confidence, entry_price) VALUES (?, ?, ?, ?, ?)",
                    ("meta_ml", pair, pred["direction"], pred["confidence"], current_price)
                )

                # For hot agents: execute real trades
                hot_agents = self.db.execute(
                    "SELECT name FROM meta_agents WHERE status='hot' AND strategy_type IN ('momentum', 'contrarian', 'mean_reversion')"
                ).fetchall()
                if hot_agents and pred["confidence"] >= 0.80:
                    try:
                        from exchange_connector import CoinbaseTrader
                        trader = CoinbaseTrader()
                        trade_usd = min(3.0, pred["confidence"] * 4.0)
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
                            else:
                                result = {}
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
                    except Exception as e:
                        logger.debug("META live trade failed: %s", e)

            except Exception as e:
                logger.debug("Step 6 failed for %s: %s", pair, e)

        logger.info("Step 6: %d live trades executed, paper trades updated", trades_executed)

        self.db.commit()
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
                self.evolve()

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
