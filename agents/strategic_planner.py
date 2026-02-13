#!/usr/bin/env python3
"""Strategic Planner — 3D Go Game Theory for Trading.

Treats the market as a 3D Go board with dimensions:
  1. TIME (when to act — timing of entries, holds, exits)
  2. ASSET SPACE (which assets, their correlations, influence propagation)
  3. VENUE SPACE (where to trade — Coinbase, IBKR, DEX, fee optimization)

Core Go concepts mapped to trading:

  TERRITORY: Capital allocation across assets = stones controlling area.
    More territory = more diversified income streams.
    Overconcentration = vulnerable to attack (single-asset crash).

  INFLUENCE: A strong signal on ETH radiates to correlated assets (SOL, AVAX).
    Like a Go stone's influence radiating outward.
    Quantified via correlation matrices and signal propagation.

  LONG CHAIN MOVES: Multi-step trade plans that only pay off after several moves:
    Buy ETH (cheap) → wait for ETH/BTC ratio → rotate to BTC → ride BTC breakout
    Each move is individually suboptimal but the CHAIN generates alpha.

  LADDERS: Forced sequences where each move necessitates the next.
    If you buy at support and support breaks → forced to cut loss → freed cash buys dip lower.
    Read the ladder BEFORE entering: is there a ladder breaker (reversal signal)?

  KO FIGHTS: Repetitive patterns that waste resources.
    Detect: buy-sell-buy-sell on same asset at same price = zero-sum minus fees.
    Ko rule: ban re-entering a position within N hours of exiting at a loss.

  SEKI (DEAD MONEY): Positions that are alive but generate no value.
    Detect: positions flat for >3h with no momentum.
    Resolution: either find a forcing move (catalyst) or abandon territory.

  LIFE & DEATH: Determining if a position will survive.
    Two eyes = two independent reasons to hold (profit + momentum).
    One eye = single reason (dangerous — one shock kills it).
    Dead = no reason to hold (exit immediately).

  MONTE CARLO TREE SEARCH (MCTS): Plan optimal move sequences.
    Simulate thousands of possible trade chains.
    Select the chain with highest expected portfolio value.
    Balance exploration (new strategies) vs exploitation (known winners).

Implementation: Pure Python, no external dependencies beyond stdlib.
All parameters dynamic via risk_controller.
"""

import json
import logging
import math
import os
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger("strategic_planner")

# Dynamic risk controller
try:
    from risk_controller import get_controller
    _risk_ctrl = get_controller()
except Exception:
    _risk_ctrl = None

try:
    from fast_exec_bridge import FastExec
except Exception:
    FastExec = None


# ============================================================
# 1. INFLUENCE MAP — Signal Propagation Across Correlated Assets
# ============================================================

class InfluenceMap:
    """3D influence map: propagate signals across correlated assets.

    When ETH gets a strong BUY signal, correlated assets (SOL, AVAX, LINK)
    get a weaker "influence" signal. Like Go stones radiating influence.

    Dimension 1: Asset correlation strength (static, from historical data)
    Dimension 2: Signal recency (decays over time)
    Dimension 3: Venue liquidity (stronger influence on liquid venues)
    """

    # Correlation matrix: how much signal propagates between assets
    # Values from crypto market structure (approximate, stable over months)
    # Row = source, Column = target, Value = influence weight (0-1)
    CORRELATIONS = {
        "BTC": {"ETH": 0.85, "SOL": 0.75, "AVAX": 0.70, "LINK": 0.72, "DOGE": 0.60, "FET": 0.55},
        "ETH": {"BTC": 0.85, "SOL": 0.82, "AVAX": 0.78, "LINK": 0.80, "DOGE": 0.55, "FET": 0.65},
        "SOL": {"BTC": 0.75, "ETH": 0.82, "AVAX": 0.85, "LINK": 0.70, "DOGE": 0.50, "FET": 0.72},
        "AVAX": {"BTC": 0.70, "ETH": 0.78, "SOL": 0.85, "LINK": 0.75, "DOGE": 0.45, "FET": 0.68},
        "LINK": {"BTC": 0.72, "ETH": 0.80, "SOL": 0.70, "AVAX": 0.75, "DOGE": 0.50, "FET": 0.70},
        "DOGE": {"BTC": 0.60, "ETH": 0.55, "SOL": 0.50, "AVAX": 0.45, "LINK": 0.50, "FET": 0.40},
        "FET": {"BTC": 0.55, "ETH": 0.65, "SOL": 0.72, "AVAX": 0.68, "LINK": 0.70, "DOGE": 0.40},
    }

    # Influence decay: signal strength halves every DECAY_HALF_LIFE_SECONDS
    DECAY_HALF_LIFE_SECONDS = 300  # 5 minutes

    # Minimum influence to consider (noise floor)
    MIN_INFLUENCE = 0.05

    def __init__(self):
        # Active influence stones: {asset: [(direction, strength, timestamp), ...]}
        self._stones = defaultdict(list)

    def place_stone(self, asset, direction, strength, timestamp=None):
        """Place an influence stone (record a signal) for an asset.

        Args:
            asset: e.g., "BTC", "ETH"
            direction: "BUY" or "SELL"
            strength: 0.0-1.0 signal confidence
            timestamp: when signal occurred (default: now)
        """
        if timestamp is None:
            timestamp = time.time()
        self._stones[asset].append((direction, strength, timestamp))
        # Keep only last 20 stones per asset (memory bound)
        if len(self._stones[asset]) > 20:
            self._stones[asset] = self._stones[asset][-20:]

    def get_influence(self, target_asset):
        """Get total influence on target_asset from all sources.

        Returns: (direction, total_influence, sources)
        - direction: "BUY" or "SELL" (net influence direction)
        - total_influence: 0.0-1.0 strength
        - sources: list of (source_asset, direction, contribution)
        """
        now = time.time()
        buy_influence = 0.0
        sell_influence = 0.0
        sources = []

        target_base = target_asset.split("-")[0] if "-" in target_asset else target_asset

        for source_asset, stones in self._stones.items():
            source_base = source_asset.split("-")[0] if "-" in source_asset else source_asset
            if source_base == target_base:
                continue  # Self-influence handled separately

            correlation = self.CORRELATIONS.get(source_base, {}).get(target_base, 0.0)
            if correlation < self.MIN_INFLUENCE:
                continue

            for direction, strength, ts in stones:
                # Time decay
                age = now - ts
                decay = math.pow(0.5, age / self.DECAY_HALF_LIFE_SECONDS)
                effective = strength * correlation * decay

                if effective < self.MIN_INFLUENCE:
                    continue

                if direction == "BUY":
                    buy_influence += effective
                elif direction == "SELL":
                    sell_influence += effective

                sources.append((source_base, direction, round(effective, 4)))

        total = max(buy_influence, sell_influence)
        net_direction = "BUY" if buy_influence > sell_influence else "SELL" if sell_influence > buy_influence else "NONE"

        return net_direction, min(1.0, total), sources

    def get_board_state(self):
        """Get full influence board state for all assets.

        Returns dict of {asset: (direction, influence, source_count)}.
        Like viewing the full Go board.
        """
        all_assets = set()
        for source in self._stones:
            base = source.split("-")[0] if "-" in source else source
            all_assets.add(base)
            for target in self.CORRELATIONS.get(base, {}):
                all_assets.add(target)

        board = {}
        for asset in sorted(all_assets):
            direction, influence, sources = self.get_influence(asset)
            board[asset] = {
                "direction": direction,
                "influence": round(influence, 4),
                "source_count": len(sources),
                "top_sources": sources[:3],
            }
        return board


# ============================================================
# 2. KO DETECTOR — Prevent Repetitive Losing Patterns
# ============================================================

class KoDetector:
    """Detect and prevent Ko fights (repetitive losing patterns).

    Ko rule: if you exit a position at a loss, you cannot re-enter
    the same position within KO_COOLDOWN_SECONDS. This prevents
    the buy-sell-buy-sell churn that destroys capital via fees.

    Also detects longer cycles: buy A, sell A, buy A pattern = Ko.
    """

    KO_COOLDOWN_SECONDS = 3600  # 1 hour ban after losing exit

    def __init__(self):
        # {pair: [(action, price, timestamp, pnl), ...]}
        self._history = defaultdict(list)
        # {pair: ban_until_timestamp}
        self._ko_bans = {}

    def record_action(self, pair, action, price, pnl=0.0):
        """Record a trade action for Ko detection.

        Args:
            pair: e.g., "BTC-USD"
            action: "BUY" or "SELL"
            price: execution price
            pnl: realized PnL (negative = loss)
        """
        now = time.time()
        self._history[pair].append((action, price, now, pnl))
        # Keep last 10 actions per pair
        if len(self._history[pair]) > 10:
            self._history[pair] = self._history[pair][-10:]

        # If SELL at a loss, set Ko ban
        if action == "SELL" and pnl < 0:
            self._ko_bans[pair] = now + self.KO_COOLDOWN_SECONDS
            logger.info("KO BAN: %s banned for %ds after losing exit (PnL=$%.4f)",
                       pair, self.KO_COOLDOWN_SECONDS, pnl)

    def is_banned(self, pair):
        """Check if a pair is under Ko ban (can't re-enter).

        Returns: (banned: bool, reason: str, seconds_remaining: float)
        """
        now = time.time()
        ban_until = self._ko_bans.get(pair, 0)
        if now < ban_until:
            remaining = ban_until - now
            return True, f"Ko ban: {remaining:.0f}s remaining after losing exit", remaining
        return False, "", 0

    def detect_cycle(self, pair):
        """Detect if we're in a repetitive buy-sell cycle (longer Ko).

        A cycle is: BUY → SELL(loss) → BUY → SELL(loss) within 4 hours.
        Returns True if cycle detected (should NOT enter).
        """
        history = self._history.get(pair, [])
        if len(history) < 4:
            return False

        now = time.time()
        recent = [(a, p, t, pnl) for a, p, t, pnl in history if now - t < 14400]  # 4 hours

        # Look for alternating BUY-SELL pattern with losses
        loss_exits = sum(1 for a, p, t, pnl in recent if a == "SELL" and pnl < 0)
        entries = sum(1 for a, p, t, pnl in recent if a == "BUY")

        # If we've had 2+ losing exits and 2+ entries in 4 hours = cycle
        if loss_exits >= 2 and entries >= 2:
            logger.warning("KO CYCLE DETECTED: %s has %d losing exits and %d entries in 4h — BLOCKING",
                          pair, loss_exits, entries)
            return True
        return False


# ============================================================
# 3. LIFE & DEATH READER — Position Survival Analysis
# ============================================================

class LifeDeathReader:
    """Determine if a position is alive, in danger, or dead.

    Go concept: a group is alive if it has two "eyes" (two independent
    reasons to exist). With one eye, it's vulnerable. With no eyes, it's dead.

    Trading eyes:
    - Eye 1: PROFIT — position is profitable (entry < current)
    - Eye 2: MOMENTUM — price momentum is favorable (trending up for longs)
    - Eye 3: INFLUENCE — correlated assets support the position
    - Eye 4: REGIME — market regime favors the position direction

    Two or more eyes = ALIVE (hold with confidence)
    One eye = DANGER (tighten stops, prepare to exit)
    No eyes = DEAD (exit immediately)
    """

    def read(self, pair, entry_price, current_price, momentum, regime, influence_direction):
        """Read the life/death status of a position.

        Args:
            pair: trading pair
            entry_price: position entry price
            current_price: current market price
            momentum: -1.0 to 1.0 (negative = downtrend)
            regime: "accumulation", "markup", "distribution", "markdown"
            influence_direction: "BUY" or "SELL" from influence map

        Returns:
            (status, eye_count, eyes, recommendation)
        """
        eyes = []
        direction = "LONG"  # assume long positions (we only buy on Coinbase)

        # Eye 1: Profit
        profit_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0
        if profit_pct > 0.002:  # > 0.2% profit (covers minimum fees)
            eyes.append(("profit", f"{profit_pct:.2%} gain"))

        # Eye 2: Momentum
        if direction == "LONG" and momentum > 0.1:
            eyes.append(("momentum", f"positive momentum {momentum:.2f}"))
        elif direction == "SHORT" and momentum < -0.1:
            eyes.append(("momentum", f"negative momentum {momentum:.2f}"))

        # Eye 3: Influence from correlated assets
        if influence_direction == "BUY" and direction == "LONG":
            eyes.append(("influence", "correlated assets support BUY"))
        elif influence_direction == "SELL" and direction == "SHORT":
            eyes.append(("influence", "correlated assets support SELL"))

        # Eye 4: Regime alignment
        bullish_regimes = {"accumulation", "markup"}
        bearish_regimes = {"distribution", "markdown"}
        if direction == "LONG" and regime in bullish_regimes:
            eyes.append(("regime", f"{regime} supports longs"))
        elif direction == "SHORT" and regime in bearish_regimes:
            eyes.append(("regime", f"{regime} supports shorts"))

        eye_count = len(eyes)

        if eye_count >= 2:
            status = "ALIVE"
            recommendation = "HOLD — position has two+ eyes, stable"
        elif eye_count == 1:
            status = "DANGER"
            recommendation = f"TIGHTEN STOPS — only one eye: {eyes[0][0]}"
        else:
            status = "DEAD"
            recommendation = "EXIT — no eyes, position is dead"

        return status, eye_count, eyes, recommendation


# ============================================================
# 4. CHAIN MOVE PLANNER (MCTS-inspired)
# ============================================================

class ChainMovePlanner:
    """Plan multi-step trade chains using Monte Carlo Tree Search principles.

    Instead of treating each trade independently, plan SEQUENCES:
    1. Buy ETH at support → 2. Wait for recovery → 3. Rotate to BTC before halving
    4. Ride BTC momentum → 5. Exit to stablecoins at resistance

    Each "move" in the chain has:
    - Prerequisites (what must happen before this move)
    - Expected outcome (probability-weighted return)
    - Downstream effects (what this enables next)

    MCTS-inspired: simulate many possible chains, select the one with
    highest expected terminal portfolio value.
    """

    # Move types in the game tree
    MOVE_TYPES = {
        "BUY": {"cost": 0.004, "min_confidence": 0.65},    # 0.4% maker fee
        "SELL": {"cost": 0.004, "min_confidence": 0.55},   # 0.4% maker fee
        "HOLD": {"cost": 0.0, "min_confidence": 0.0},      # Free but time cost
        "ROTATE": {"cost": 0.008, "min_confidence": 0.70},  # Buy + Sell = 0.8%
        "SCALE_IN": {"cost": 0.004, "min_confidence": 0.75},  # DCA buy
        "SCALE_OUT": {"cost": 0.004, "min_confidence": 0.60}, # Partial sell
    }

    # Maximum chain depth (moves ahead to plan)
    MAX_DEPTH = 5

    # MCTS simulation count
    NUM_SIMULATIONS = 200

    def __init__(self, influence_map=None, ko_detector=None, life_reader=None):
        self.influence = influence_map or InfluenceMap()
        self.ko = ko_detector or KoDetector()
        self.life_reader = life_reader or LifeDeathReader()
        # Long-chain planning controls (env-tunable for live experimentation)
        self.max_chain_depth = max(2, int(os.environ.get("LONG_CHAIN_MAX_DEPTH", str(self.MAX_DEPTH))))
        self.max_rotation_branches = max(1, int(os.environ.get("LONG_CHAIN_MAX_BRANCHES", "3")))
        self.min_chain_net_edge = float(os.environ.get("LONG_CHAIN_MIN_NET_EDGE_PCT", "0.012"))
        self.min_chain_worst_case_edge = float(os.environ.get("LONG_CHAIN_MIN_WORST_EDGE_PCT", "0.001"))
        self.horizon_decay = float(os.environ.get("LONG_CHAIN_HORIZON_DECAY", "0.84"))
        self._c_gate = None
        if str(os.environ.get("LONG_CHAIN_USE_C_GATE", "1")).strip() != "0" and FastExec is not None:
            try:
                self._c_gate = FastExec()
            except Exception:
                self._c_gate = None

    @staticmethod
    def _base_asset(pair: str) -> str:
        return pair.split("-")[0] if "-" in pair else pair

    @staticmethod
    def _pair_candidates_for_asset(asset: str, signals: Dict[str, dict]) -> List[str]:
        out = []
        for pair in signals:
            if ChainMovePlanner._base_asset(pair) == asset:
                out.append(pair)
        # Prefer USDC execution pairs, then USD
        out.sort(key=lambda p: (0 if p.endswith("-USDC") else 1 if p.endswith("-USD") else 2, p))
        return out

    @staticmethod
    def _pick_pair_for_asset(asset: str, signals: Dict[str, dict]) -> str:
        candidates = ChainMovePlanner._pair_candidates_for_asset(asset, signals)
        return candidates[0] if candidates else ""

    def _signal_edge(self, pair: str, signals: Dict[str, dict]) -> float:
        sig = signals.get(pair, {})
        conf = max(0.0, min(1.0, float(sig.get("confidence", 0.0) or 0.0)))
        momentum = max(-1.0, min(1.0, float(sig.get("momentum", 0.0) or 0.0)))
        regime = str(sig.get("regime", "neutral")).lower()
        regime_bonus = (
            0.0015 if regime in {"accumulation", "markup", "bullish", "uptrend"}
            else -0.001 if regime in {"distribution", "markdown", "bearish", "downtrend"}
            else 0.0
        )
        # Confidence is the core probabilistic edge; momentum/regime are tie-breakers.
        return (conf - 0.5) * 0.022 + momentum * 0.006 + regime_bonus

    def _signal_risk(self, pair: str, signals: Dict[str, dict]) -> float:
        sig = signals.get(pair, {})
        conf = max(0.0, min(1.0, float(sig.get("confidence", 0.0) or 0.0)))
        momentum = max(-1.0, min(1.0, float(sig.get("momentum", 0.0) or 0.0)))
        direction = str(sig.get("direction", "NONE")).upper()
        base = (1.0 - conf) * 0.02 + max(0.0, -momentum) * 0.012
        # If a node is not even BUY-biased, treat as adversarial pressure.
        if direction != "BUY":
            base += 0.006
        return base

    def _rotation_targets(self, pair: str, signals: Dict[str, dict]) -> List[Tuple[str, float]]:
        base = self._base_asset(pair)
        corr = self.influence.CORRELATIONS.get(base, {})
        ranked = sorted(corr.items(), key=lambda x: -x[1])
        out = []
        for asset, strength in ranked:
            next_pair = self._pick_pair_for_asset(asset, signals)
            if not next_pair:
                continue
            sig = signals.get(next_pair, {})
            if str(sig.get("direction", "NONE")).upper() != "BUY":
                continue
            if float(sig.get("confidence", 0.0) or 0.0) < 0.55:
                continue
            out.append((next_pair, strength))
            if len(out) >= self.max_rotation_branches:
                break
        return out

    def evaluate_entry_chain(
        self,
        pair: str,
        market_signals: Dict[str, dict],
        max_depth: int = None,
        min_net_edge: float = None,
        min_worst_case_edge: float = None,
    ) -> dict:
        """Evaluate whether entering `pair` has a profitable multi-step exit chain.

        Game-theoretic framing:
          - We assume an adversary (market noise/other participants) that pushes
            against weak-confidence nodes.
          - We only approve entries that keep positive expected edge after fees
            and after an adversarial risk haircut.
        """
        signal = market_signals.get(pair, {})
        if str(signal.get("direction", "NONE")).upper() != "BUY":
            return {
                "pair": pair,
                "viable": False,
                "reason": "Not a BUY signal",
                "net_edge": 0.0,
                "worst_case_edge": 0.0,
                "path": [],
                "steps": 0,
                "has_exit": False,
            }

        depth = max(2, int(max_depth if max_depth is not None else self.max_chain_depth))
        min_edge = float(min_net_edge if min_net_edge is not None else self.min_chain_net_edge)
        min_worst = float(
            min_worst_case_edge if min_worst_case_edge is not None else self.min_chain_worst_case_edge
        )

        best = {
            "objective": -1e9,
            "net_edge": -1.0,
            "risk_penalty": 1.0,
            "path": [],
            "has_exit": False,
        }

        entry_edge = self._signal_edge(pair, market_signals)
        entry_risk = self._signal_risk(pair, market_signals)
        path = [{
            "step": 1,
            "action": "BUY",
            "pair": pair,
            "edge": round(entry_edge, 5),
            "cost": self.MOVE_TYPES["BUY"]["cost"],
            "note": "entry node",
        }]

        def _register(net_edge: float, risk_penalty: float, candidate_path: List[dict], has_exit: bool):
            objective = net_edge - risk_penalty
            if objective > best["objective"]:
                best["objective"] = objective
                best["net_edge"] = net_edge
                best["risk_penalty"] = risk_penalty
                best["path"] = list(candidate_path)
                best["has_exit"] = has_exit

        def _dfs(current_pair: str, depth_left: int, net_edge: float, risk_penalty: float):
            # Voluntary early exit is always considered.
            exit_cost = self.MOVE_TYPES["SELL"]["cost"]
            exit_step = len(path) + 1
            exit_path = list(path) + [{
                "step": exit_step,
                "action": "EXIT",
                "pair": current_pair,
                "edge": 0.0,
                "cost": exit_cost,
                "note": "realize PnL and de-risk",
            }]
            _register(net_edge - exit_cost, risk_penalty + 0.001, exit_path, has_exit=True)

            if depth_left <= 0:
                return

            # HOLD branch (staying in current thesis for another step)
            hold_edge_raw = self._signal_edge(current_pair, market_signals)
            hold_decay = math.pow(self.horizon_decay, max(0, len(path) - 1))
            hold_edge = hold_edge_raw * hold_decay
            hold_risk = self._signal_risk(current_pair, market_signals)
            path.append({
                "step": len(path) + 1,
                "action": "HOLD",
                "pair": current_pair,
                "edge": round(hold_edge, 5),
                "cost": 0.0,
                "note": "let thesis mature",
            })
            _dfs(current_pair, depth_left - 1, net_edge + hold_edge, risk_penalty + hold_risk)
            path.pop()

            # ROTATE branches (multi-hop chain logic)
            for nxt_pair, corr in self._rotation_targets(current_pair, market_signals):
                rot_cost = self.MOVE_TYPES["ROTATE"]["cost"]
                rot_edge_raw = self._signal_edge(nxt_pair, market_signals)
                rot_decay = math.pow(self.horizon_decay, max(0, len(path) - 1))
                rot_edge = rot_edge_raw * rot_decay
                corr_penalty = (1.0 - float(corr)) * 0.006
                rot_risk = self._signal_risk(nxt_pair, market_signals) + corr_penalty
                path.append({
                    "step": len(path) + 1,
                    "action": "ROTATE",
                    "pair": f"{current_pair} -> {nxt_pair}",
                    "edge": round(rot_edge, 5),
                    "cost": rot_cost,
                    "note": f"correlation hop ({corr:.2f})",
                })
                _dfs(
                    nxt_pair,
                    depth_left - 1,
                    net_edge + rot_edge - rot_cost,
                    risk_penalty + rot_risk,
                )
                path.pop()

        # Entry includes fee cost immediately.
        _dfs(
            pair,
            depth_left=depth - 1,
            net_edge=entry_edge - self.MOVE_TYPES["BUY"]["cost"],
            risk_penalty=entry_risk,
        )

        net_edge = float(best.get("net_edge", -1.0))
        risk_penalty = float(best.get("risk_penalty", 1.0))
        worst_case_edge = net_edge - risk_penalty
        steps = len(best.get("path", []))
        viable = (
            bool(best.get("has_exit"))
            and steps >= 2
            and net_edge >= min_edge
            and worst_case_edge >= min_worst
        )
        c_gate = None
        if self._c_gate is not None:
            try:
                sig_conf = float(signal.get("confidence", 0.0) or 0.0)
                # C gate treats this as a strict final guard after planner's chain simulation.
                c_gate = self._c_gate.no_loss_gate(
                    expected_edge_pct=net_edge,
                    total_cost_pct=float(os.environ.get("LONG_CHAIN_COST_PCT", "0.009")),
                    spread_pct=float(os.environ.get("LONG_CHAIN_SPREAD_PCT", "0.002")),
                    latency_ms=float(os.environ.get("LONG_CHAIN_LATENCY_MS", "120")),
                    failure_rate=float(os.environ.get("LONG_CHAIN_FAILURE_RATE", "0.01")),
                    signal_confidence=sig_conf,
                    min_expected_edge_pct=min_edge,
                    max_spread_pct=float(os.environ.get("LONG_CHAIN_MAX_SPREAD_PCT", "0.02")),
                    max_latency_ms=float(os.environ.get("LONG_CHAIN_MAX_LATENCY_MS", "450")),
                    max_failure_rate=float(os.environ.get("LONG_CHAIN_MAX_FAILURE_RATE", "0.08")),
                    confidence_floor=float(os.environ.get("LONG_CHAIN_CONFIDENCE_FLOOR", "0.65")),
                    buy_blocked_regime=False,
                )
                viable = viable and bool(c_gate.get("approved", False))
            except Exception:
                c_gate = None
        reason = (
            f"chain edge {net_edge:.2%} (worst {worst_case_edge:.2%}) "
            f"vs mins {min_edge:.2%}/{min_worst:.2%}"
        )
        return {
            "pair": pair,
            "viable": viable,
            "reason": reason if viable else f"blocked: {reason}",
            "net_edge": round(net_edge, 5),
            "risk_penalty": round(risk_penalty, 5),
            "worst_case_edge": round(worst_case_edge, 5),
            "path": best.get("path", []),
            "steps": steps,
            "has_exit": bool(best.get("has_exit")),
            "required_min_edge": round(min_edge, 5),
            "required_min_worst_case": round(min_worst, 5),
            "c_gate": c_gate,
        }

    def plan_chain(self, portfolio_state, market_signals, available_capital):
        """Plan the optimal chain of moves.

        Args:
            portfolio_state: dict of {pair: {amount, entry_price, current_price, ...}}
            market_signals: dict of {pair: {direction, confidence, momentum, regime}}
            available_capital: USD cash available

        Returns:
            list of planned moves, each: {
                step: int,
                action: str,
                pair: str,
                size_pct: float (fraction of available capital),
                reason: str,
                expected_return: float,
                prerequisites: list,
                enables: list,
            }
        """
        # Step 1: Read life/death of all current positions
        position_health = {}
        for pair, pos in portfolio_state.items():
            base = pair.split("-")[0]
            signal = market_signals.get(pair, {})
            inf_dir, inf_str, _ = self.influence.get_influence(base)
            status, eyes, eye_list, rec = self.life_reader.read(
                pair,
                pos.get("entry_price", 0),
                pos.get("current_price", 0),
                signal.get("momentum", 0),
                signal.get("regime", "accumulation"),
                inf_dir,
            )
            position_health[pair] = {
                "status": status,
                "eyes": eyes,
                "eye_details": eye_list,
                "recommendation": rec,
                "profit_pct": (pos.get("current_price", 0) - pos.get("entry_price", 0)) / pos.get("entry_price", 1),
                "value_usd": pos.get("amount", 0) * pos.get("current_price", 0),
            }

        # Step 2: Identify DEAD positions to exit (forced moves)
        chain = []
        freed_capital = 0.0
        step = 1

        for pair, health in position_health.items():
            if health["status"] == "DEAD":
                value = health["value_usd"]
                chain.append({
                    "step": step,
                    "action": "SELL",
                    "pair": pair,
                    "size_pct": 1.0,
                    "reason": f"DEAD position — {health['recommendation']}",
                    "expected_return": health["profit_pct"],
                    "prerequisites": [],
                    "enables": [f"free ${value:.2f} for redeployment"],
                    "priority": 100,
                })
                freed_capital += value
                step += 1

        # Step 3: Identify DANGER positions to scale out
        for pair, health in position_health.items():
            if health["status"] == "DANGER" and health["value_usd"] > 2.0:
                chain.append({
                    "step": step,
                    "action": "SCALE_OUT",
                    "pair": pair,
                    "size_pct": 0.5,  # Sell 50% of danger positions
                    "reason": f"DANGER — only {health['eyes']} eye(s): {', '.join(e[0] for e in health['eye_details'])}",
                    "expected_return": health["profit_pct"] * 0.5,
                    "prerequisites": [],
                    "enables": [f"free ${health['value_usd'] * 0.5:.2f}, reduce risk"],
                    "priority": 80,
                })
                freed_capital += health["value_usd"] * 0.5
                step += 1

        # Step 4: Run MCTS to find best BUY targets for freed capital
        total_deployable = available_capital + freed_capital
        if total_deployable > 1.0:
            buy_candidates = self._score_buy_candidates(
                market_signals, portfolio_state, position_health, total_deployable)

            for candidate in buy_candidates[:3]:  # Top 3 opportunities
                pair = candidate["pair"]

                # Ko check
                banned, ban_reason, _ = self.ko.is_banned(pair)
                if banned:
                    logger.info("CHAIN: Skipping %s — %s", pair, ban_reason)
                    continue
                if self.ko.detect_cycle(pair):
                    logger.info("CHAIN: Skipping %s — Ko cycle detected", pair)
                    continue

                chain.append({
                    "step": step,
                    "action": "BUY",
                    "pair": pair,
                    "size_pct": candidate["allocation_pct"],
                    "reason": candidate["reason"],
                    "expected_return": candidate["expected_return"],
                    "prerequisites": [f"need ${candidate['size_usd']:.2f} cash"],
                    "enables": candidate["enables"],
                    "priority": candidate["priority"],
                })
                step += 1

        # Step 5: Plan rotation moves (sell weak → buy strong)
        rotations = self._find_rotation_opportunities(
            portfolio_state, position_health, market_signals)
        for rot in rotations[:2]:  # Max 2 rotations per cycle
            chain.append({
                "step": step,
                "action": "ROTATE",
                "pair": f"{rot['sell_pair']} → {rot['buy_pair']}",
                "size_pct": rot["size_pct"],
                "reason": rot["reason"],
                "expected_return": rot["expected_return"],
                "prerequisites": [f"sell {rot['sell_pair']} first"],
                "enables": [f"stronger position in {rot['buy_pair']}"],
                "priority": rot["priority"],
            })
            step += 1

        # Sort by priority (highest first)
        chain.sort(key=lambda x: -x.get("priority", 0))
        return chain

    def _score_buy_candidates(self, market_signals, portfolio_state, health, deployable):
        """Score and rank BUY opportunities using influence + signals.

        Combines:
        - Direct signal strength (confidence, EV)
        - Influence from correlated assets
        - Position health of existing holdings
        - Ko ban status
        """
        candidates = []
        existing_pairs = set(portfolio_state.keys())

        for pair, signal in market_signals.items():
            if signal.get("direction") != "BUY":
                continue
            if signal.get("confidence", 0) < 0.60:
                continue

            base = pair.split("-")[0]
            inf_dir, inf_str, inf_sources = self.influence.get_influence(base)

            # Direct signal score
            direct_score = signal.get("confidence", 0) * 0.5

            # Influence bonus (correlated assets agree)
            influence_bonus = inf_str * 0.2 if inf_dir == "BUY" else -inf_str * 0.1

            # Novelty bonus (not already holding = diversification)
            novelty = 0.15 if pair not in existing_pairs else 0.0

            # Regime alignment
            regime = signal.get("regime", "accumulation")
            regime_bonus = 0.15 if regime in ("accumulation", "markup") else 0.0

            total_score = direct_score + influence_bonus + novelty + regime_bonus
            entry_eval = self.evaluate_entry_chain(pair, market_signals)
            if not entry_eval.get("viable"):
                continue
            expected_return = float(entry_eval.get("net_edge", 0.0) or 0.0)

            # Allocation: proportional to score, capped at 15% of deployable
            allocation_pct = min(0.15, max(0.03, total_score * 0.2))
            size_usd = deployable * allocation_pct

            if size_usd < 1.0:
                continue

            candidates.append({
                "pair": pair,
                "score": round(total_score, 4),
                "confidence": signal.get("confidence", 0),
                "influence": round(inf_str, 4),
                "influence_dir": inf_dir,
                "regime": regime,
                "expected_return": round(expected_return, 4),
                "allocation_pct": round(allocation_pct, 4),
                "size_usd": round(size_usd, 2),
                "reason": (
                    f"Score={total_score:.2f} | conf={signal.get('confidence', 0):.0%} | "
                    f"influence={inf_dir} {inf_str:.2f} | regime={regime} | "
                    f"chain={entry_eval.get('net_edge', 0.0):.2%}"
                ),
                "enables": [f"exposure to {base} ({regime} phase)", "multi-step exit chain validated"],
                "priority": int(total_score * 100),
                "entry_validation": entry_eval,
            })

        candidates.sort(key=lambda x: -x["score"])
        return candidates

    def _find_rotation_opportunities(self, portfolio_state, health, signals):
        """Find opportunities to rotate from weak positions to strong ones.

        A rotation is: SELL weak position → BUY strong opportunity.
        Only worth it if expected gain > rotation cost (0.8% round-trip).
        """
        ROTATION_COST = 0.008  # 0.8% round-trip fees

        rotations = []
        # Find weak positions (1 eye or profitable but losing momentum)
        weak = [(pair, h) for pair, h in health.items()
                if h["status"] in ("DANGER", "DEAD") or
                (h["status"] == "ALIVE" and h.get("profit_pct", 0) < 0.005)]

        # Find strong opportunities
        strong = [(pair, sig) for pair, sig in signals.items()
                  if sig.get("direction") == "BUY" and sig.get("confidence", 0) > 0.75]

        for weak_pair, weak_health in weak:
            for strong_pair, strong_signal in strong:
                if weak_pair == strong_pair:
                    continue

                # Expected gain from rotation
                weak_return = weak_health.get("profit_pct", 0)
                strong_expected = (strong_signal.get("confidence", 0) - 0.5) * 0.08
                net_gain = strong_expected - abs(weak_return) - ROTATION_COST

                if net_gain > 0.005:  # Must gain at least 0.5% after costs
                    rotations.append({
                        "sell_pair": weak_pair,
                        "buy_pair": strong_pair,
                        "weak_status": weak_health["status"],
                        "strong_confidence": strong_signal.get("confidence", 0),
                        "expected_return": round(net_gain, 4),
                        "size_pct": 0.5 if weak_health["status"] == "DANGER" else 1.0,
                        "reason": (f"Rotate {weak_pair} ({weak_health['status']}, "
                                  f"{weak_return:+.2%}) → {strong_pair} "
                                  f"(conf={strong_signal.get('confidence', 0):.0%}) | "
                                  f"net gain {net_gain:.2%} after fees"),
                        "priority": int(net_gain * 10000),
                    })

        rotations.sort(key=lambda x: -x["expected_return"])
        return rotations


# ============================================================
# 5. TERRITORY SCORER — Portfolio Allocation Optimization
# ============================================================

class TerritoryScorer:
    """Score portfolio territory: how well is capital distributed?

    Good territory: diversified across uncorrelated assets, each in favorable regime.
    Bad territory: concentrated in correlated assets during unfavorable regimes.

    Metrics:
    - Herfindahl index (concentration) — lower is better
    - Effective diversification (accounts for correlations)
    - Regime alignment score
    - Territory efficiency (return per unit of territory)
    """

    def score(self, portfolio_state, market_signals):
        """Score the current portfolio territory.

        Returns:
            dict with territory metrics and recommendations
        """
        if not portfolio_state:
            return {
                "score": 0.0,
                "herfindahl": 1.0,
                "effective_positions": 0,
                "regime_alignment": 0.0,
                "recommendations": ["No positions — deploy capital"],
            }

        # Calculate position values
        total_value = 0.0
        position_values = {}
        for pair, pos in portfolio_state.items():
            value = pos.get("amount", 0) * pos.get("current_price", 0)
            position_values[pair] = value
            total_value += value

        if total_value <= 0:
            return {"score": 0.0, "herfindahl": 1.0, "effective_positions": 0,
                    "regime_alignment": 0.0, "recommendations": ["No value in portfolio"]}

        # Herfindahl index (sum of squared weights) — 1.0 = single position, 0.0 = perfect diversification
        weights = {pair: val / total_value for pair, val in position_values.items() if val > 0}
        herfindahl = sum(w ** 2 for w in weights.values())
        effective_positions = 1.0 / herfindahl if herfindahl > 0 else 0

        # Regime alignment: what fraction of portfolio is in favorable regimes?
        regime_aligned_value = 0.0
        for pair, pos in portfolio_state.items():
            signal = market_signals.get(pair, {})
            regime = signal.get("regime", "accumulation")
            if regime in ("accumulation", "markup"):
                regime_aligned_value += position_values.get(pair, 0)
        regime_alignment = regime_aligned_value / total_value if total_value > 0 else 0

        # Composite territory score
        diversification_score = 1.0 - herfindahl  # Higher = more diversified
        score = diversification_score * 0.4 + regime_alignment * 0.4 + min(1.0, effective_positions / 5) * 0.2

        # Recommendations
        recommendations = []
        if herfindahl > 0.4:
            top_pair = max(weights, key=weights.get)
            recommendations.append(f"Over-concentrated: {top_pair} is {weights[top_pair]:.0%} — diversify")
        if regime_alignment < 0.5:
            recommendations.append("Most portfolio in unfavorable regimes — consider rotating")
        if effective_positions < 3:
            recommendations.append(f"Only {effective_positions:.1f} effective positions — need more diversity")
        if not recommendations:
            recommendations.append("Territory looks strong — maintain positions")

        return {
            "score": round(score, 4),
            "herfindahl": round(herfindahl, 4),
            "effective_positions": round(effective_positions, 2),
            "regime_alignment": round(regime_alignment, 4),
            "position_weights": {k: round(v, 4) for k, v in weights.items()},
            "recommendations": recommendations,
        }


# ============================================================
# 6. STRATEGIC PLANNER (ORCHESTRATOR)
# ============================================================

class StrategicPlanner:
    """Main orchestrator — combines all Go-theory components.

    Call flow:
    1. Update influence map with latest signals
    2. Read life/death of all positions
    3. Score territory (portfolio health)
    4. Plan chain moves (MCTS-inspired)
    5. Return prioritized action plan

    This is the "AI player" that sees the full 3D board and plans moves.
    """

    def __init__(self):
        self.influence = InfluenceMap()
        self.ko = KoDetector()
        self.life_reader = LifeDeathReader()
        self.territory = TerritoryScorer()
        self.chain_planner = ChainMovePlanner(
            influence_map=self.influence,
            ko_detector=self.ko,
            life_reader=self.life_reader,
        )

    def analyze(self, portfolio_state, market_signals, available_capital):
        """Full strategic analysis — the main entry point.

        Args:
            portfolio_state: dict of {pair: {amount, entry_price, current_price}}
            market_signals: dict of {pair: {direction, confidence, momentum, regime}}
            available_capital: USD cash available

        Returns:
            dict with full strategic analysis and action plan
        """
        # Step 1: Update influence map with all signals
        for pair, signal in market_signals.items():
            base = pair.split("-")[0]
            direction = signal.get("direction", "NONE")
            confidence = signal.get("confidence", 0)
            if direction != "NONE" and confidence > 0.3:
                self.influence.place_stone(base, direction, confidence)

        # Step 2: Get influence board state
        board = self.influence.get_board_state()

        # Step 3: Score territory
        territory = self.territory.score(portfolio_state, market_signals)

        # Step 4: Plan chain moves
        chain = self.chain_planner.plan_chain(
            portfolio_state, market_signals, available_capital)

        # Step 4b: Validate BUY entries through long-chain viability checks.
        entry_validations = {}
        for pair, signal in market_signals.items():
            if str(signal.get("direction", "NONE")).upper() != "BUY":
                continue
            try:
                entry_validations[pair] = self.chain_planner.evaluate_entry_chain(
                    pair, market_signals
                )
            except Exception as e:
                entry_validations[pair] = {
                    "pair": pair,
                    "viable": False,
                    "reason": f"validation_error: {e}",
                    "net_edge": 0.0,
                    "worst_case_edge": 0.0,
                    "path": [],
                    "steps": 0,
                    "has_exit": False,
                }

        # Step 5: Compile analysis
        # Count position health
        health_summary = {"alive": 0, "danger": 0, "dead": 0}
        for pair, pos in portfolio_state.items():
            base = pair.split("-")[0]
            signal = market_signals.get(pair, {})
            inf_dir, _, _ = self.influence.get_influence(base)
            status, _, _, _ = self.life_reader.read(
                pair,
                pos.get("entry_price", 0),
                pos.get("current_price", 0),
                signal.get("momentum", 0),
                signal.get("regime", "accumulation"),
                inf_dir,
            )
            health_summary[status.lower()] = health_summary.get(status.lower(), 0) + 1

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "territory": territory,
            "influence_board": board,
            "position_health": health_summary,
            "chain_moves": chain,
            "chain_length": len(chain),
            "entry_validations": entry_validations,
            "available_capital": available_capital,
            "ko_bans": {pair: {"banned": True, "reason": self.ko.is_banned(pair)[1]}
                       for pair in market_signals
                       if self.ko.is_banned(pair)[0]},
        }

    def get_next_action(self, portfolio_state, market_signals, available_capital):
        """Get just the next recommended action (simplified interface).

        Returns the highest-priority move from the chain plan.
        """
        analysis = self.analyze(portfolio_state, market_signals, available_capital)
        chain = analysis.get("chain_moves", [])
        if chain:
            return chain[0]  # Highest priority move
        return {"action": "HOLD", "reason": "No advantageous moves found", "priority": 0}


# ============================================================
# SINGLETON
# ============================================================

_planner = None

def get_strategic_planner():
    """Get or create the singleton StrategicPlanner."""
    global _planner
    if _planner is None:
        _planner = StrategicPlanner()
    return _planner


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    planner = get_strategic_planner()

    print("=== STRATEGIC PLANNER DEMO (3D Go) ===\n")

    # Simulate portfolio
    portfolio = {
        "AVAX-USD": {"amount": 3.19, "entry_price": 9.18, "current_price": 9.25},
        "DOGE-USD": {"amount": 380, "entry_price": 0.098, "current_price": 0.097},
        "FET-USD": {"amount": 90, "entry_price": 0.18, "current_price": 0.17},
        "ETH-USD": {"amount": 0.009, "entry_price": 2062, "current_price": 2060},
        "SOL-USD": {"amount": 0.21, "entry_price": 84.4, "current_price": 85.0},
    }

    # Simulate signals
    signals = {
        "BTC-USD": {"direction": "BUY", "confidence": 0.82, "momentum": 0.3, "regime": "accumulation"},
        "ETH-USD": {"direction": "BUY", "confidence": 0.84, "momentum": 0.2, "regime": "accumulation"},
        "SOL-USD": {"direction": "BUY", "confidence": 0.87, "momentum": 0.4, "regime": "markup"},
        "AVAX-USD": {"direction": "BUY", "confidence": 0.81, "momentum": 0.1, "regime": "accumulation"},
        "DOGE-USD": {"direction": "SELL", "confidence": 0.65, "momentum": -0.2, "regime": "distribution"},
        "LINK-USD": {"direction": "BUY", "confidence": 0.78, "momentum": 0.3, "regime": "accumulation"},
        "FET-USD": {"direction": "SELL", "confidence": 0.70, "momentum": -0.3, "regime": "markdown"},
    }

    analysis = planner.analyze(portfolio, signals, available_capital=2.0)

    print(f"  Territory Score: {analysis['territory']['score']}")
    print(f"  Herfindahl (concentration): {analysis['territory']['herfindahl']}")
    print(f"  Effective Positions: {analysis['territory']['effective_positions']}")
    print(f"  Regime Alignment: {analysis['territory']['regime_alignment']:.0%}")
    print(f"  Position Health: {analysis['position_health']}")

    print(f"\n  Influence Board:")
    for asset, info in analysis["influence_board"].items():
        print(f"    {asset:6s} | {info['direction']:4s} | influence={info['influence']:.2f} | "
              f"sources={info['source_count']}")

    print(f"\n  Chain Moves ({analysis['chain_length']} planned):")
    for move in analysis["chain_moves"]:
        print(f"    Step {move['step']}: {move['action']:10s} {move['pair']:20s} | "
              f"priority={move['priority']} | {move['reason']}")
        if move.get("enables"):
            print(f"           enables: {', '.join(move['enables'])}")

    print(f"\n  Territory Recommendations:")
    for rec in analysis["territory"]["recommendations"]:
        print(f"    - {rec}")

    print(f"\n{'='*60}")
