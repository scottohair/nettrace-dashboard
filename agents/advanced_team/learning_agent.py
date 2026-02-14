#!/usr/bin/env python3
"""LearningAgent — reviews executed trades, calculates performance metrics.

Analyzes:
  - Win rate per pair
  - Total P&L
  - Sharpe ratio (risk-adjusted returns)
  - Strategy effectiveness (which signal combos work)
  - Drawdown tracking
  - Agent performance scores

Feeds insights back to ResearchAgent for the next cycle.

Input:  execution_results from ExecutionAgent
Output: learning_insights -> ResearchAgent (for next cycle)
"""

import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

_AGENTS_DIR = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _AGENTS_DIR)

_env_path = Path(_AGENTS_DIR) / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

from advanced_team.message_bus import MessageBus

logger = logging.getLogger("learning_agent")

# Performance tracking file (persistent across restarts)
PERF_FILE = str(Path(__file__).parent / "performance.json")

# Risk-free rate for Sharpe (annualized, T-bill ~ 5%)
RISK_FREE_RATE_DAILY = 0.05 / 365


MIN_SIGNAL_FAMILY_TRADES = 4
MIN_SIGNAL_FAMILY_DISABLE_AVG_PNL = -0.04
ESTIMATED_PNL_REFERENCE_CONFIDENCE = 0.65


class LearningAgent:
    """Reviews trade outcomes and generates performance insights."""

    NAME = "learning"

    def __init__(self):
        self.bus = MessageBus()
        self.performance = self._load_performance()
        self.state = {
            "cycle_count": 0,
            "insights_published": 0,
        }

    def _load_performance(self):
        """Load persistent performance data."""
        try:
            if os.path.exists(PERF_FILE):
                with open(PERF_FILE, "r") as f:
                    return json.load(f)
        except Exception as e:
            logger.debug("Performance load: %s", e)
        return {
            "trades": [],
            "daily_returns": [],
            "pair_stats": {},
            "strategy_stats": {},
            "signal_family_stats": {},
            "total_pnl": 0.0,
            "peak_portfolio": 0.0,
            "max_drawdown_pct": 0.0,
            "last_portfolio_value": 0.0,
        }

    def _save_performance(self):
        """Save performance data persistently."""
        try:
            with open(PERF_FILE, "w") as f:
                json.dump(self.performance, f, indent=2)
        except Exception as e:
            logger.warning("Performance save failed: %s", e)

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            parsed = float(value)
            if math.isfinite(parsed):
                return parsed
        except Exception:
            pass
        return default

    def _signal_family_contributions(self, trade):
        """Return per-family attribution weights for signal-breakdown based scoring.

        Uses factor score magnitude so stronger factors receive more attribution.
        """
        scores = trade.get("score_breakdown", {})
        if not isinstance(scores, dict) or not scores:
            return {}

        valid = {}
        for family, raw_score in scores.items():
            score = self._safe_float(raw_score, 0.0)
            if score > 0:
                valid[str(family)] = score

        total = sum(valid.values())
        if total <= 0:
            return {}

        return {family: score / total for family, score in valid.items()}

    def _resolve_trade_pnl(self, trade):
        """Resolve realized or estimated PnL for one trade record."""
        for key in ("pnl_usd", "pnl", "trade_pnl_usd"):
            if key in trade:
                value = self._safe_float(trade.get(key), None)
                if value is not None:
                    return value, True

        execution = trade.get("execution") or {}
        for key in ("pnl_usd", "pnl"):
            if key in execution:
                value = self._safe_float(execution.get(key), None)
                if value is not None:
                    return value, True

        # Fallback proxy PnL when only pre-trade signal quality is available.
        size_usd = self._safe_float(trade.get("size_usd"), 0.0)
        confidence = self._safe_float(trade.get("confidence"), 0.5)
        estimated = size_usd * (confidence - ESTIMATED_PNL_REFERENCE_CONFIDENCE)
        return estimated, False

    def _update_signal_family_stats(self, trade_result):
        """Track PnL by signal-family with de-identified attribution."""
        trade = trade_result.get("trade", {})
        family_contrib = self._signal_family_contributions(trade)
        if not family_contrib:
            return

        pnl, realized = self._resolve_trade_pnl(trade)
        confidence = self._safe_float(trade.get("confidence"), 0.0)

        if "signal_family_stats" not in self.performance:
            self.performance["signal_family_stats"] = {}

        for family, weight in family_contrib.items():
            stat = self.performance["signal_family_stats"].setdefault(
                family,
                {
                    "sample_count": 0,
                    "realized_samples": 0,
                    "estimated_samples": 0,
                    "win_count": 0,
                    "loss_count": 0,
                    "total_pnl_usd": 0.0,
                    "confidence_weight_sum": 0.0,
                    "last_trade": "",
                },
            )

            attributed_pnl = pnl * weight
            stat["sample_count"] += 1
            stat["total_pnl_usd"] += attributed_pnl
            stat["confidence_weight_sum"] += confidence * weight
            if realized:
                stat["realized_samples"] += 1
            else:
                stat["estimated_samples"] += 1

            if pnl > 0:
                stat["win_count"] += 1
            elif pnl < 0:
                stat["loss_count"] += 1
            stat["last_trade"] = datetime.now(timezone.utc).isoformat()

    def _get_current_portfolio(self):
        """Get current portfolio value."""
        try:
            from exchange_connector import CoinbaseTrader, PriceFeed
            trader = CoinbaseTrader()
            accounts = trader.get_accounts()
            if "accounts" not in accounts:
                return self.performance.get("last_portfolio_value", 0)

            total = 0.0
            for acc in accounts["accounts"]:
                bal = float(acc.get("available_balance", {}).get("value", 0))
                hold = float(acc.get("hold", {}).get("value", 0))
                total_amt = bal + hold
                currency = acc.get("currency", "")
                if total_amt <= 0:
                    continue
                if currency in ("USD", "USDC"):
                    total += total_amt
                else:
                    price = PriceFeed.get_price(f"{currency}-USD")
                    if price:
                        total += total_amt * price
            return round(total, 2)
        except Exception as e:
            logger.debug("Portfolio fetch: %s", e)
            return self.performance.get("last_portfolio_value", 0)

    def _update_pair_stats(self, pair, trade_result):
        """Update per-pair performance statistics."""
        if pair not in self.performance["pair_stats"]:
            self.performance["pair_stats"][pair] = {
                "trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "total_pnl": 0.0,
                "total_volume": 0.0,
                "avg_confidence": 0.0,
                "last_trade": "",
            }

        stats = self.performance["pair_stats"][pair]
        stats["trade_count"] += 1
        stats["last_trade"] = datetime.now(timezone.utc).isoformat()

        trade = trade_result.get("trade", {})
        size = trade.get("size_usd", 0)
        confidence = trade.get("confidence", 0)
        stats["total_volume"] += size

        # Running average confidence
        n = stats["trade_count"]
        stats["avg_confidence"] = round(
            (stats["avg_confidence"] * (n - 1) + confidence) / n, 4
        )

    def _update_strategy_stats(self, strategy_type, trade_result):
        """Update per-strategy performance statistics."""
        if strategy_type not in self.performance["strategy_stats"]:
            self.performance["strategy_stats"][strategy_type] = {
                "trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "total_pnl": 0.0,
                "avg_confidence": 0.0,
            }

        stats = self.performance["strategy_stats"][strategy_type]
        stats["trade_count"] += 1

        trade = trade_result.get("trade", {})
        confidence = trade.get("confidence", 0)
        n = stats["trade_count"]
        stats["avg_confidence"] = round(
            (stats["avg_confidence"] * (n - 1) + confidence) / n, 4
        )

    def _calculate_sharpe(self):
        """Calculate Sharpe ratio from daily returns."""
        returns = self.performance.get("daily_returns", [])
        if len(returns) < 3:
            return 0.0

        # Only use last 30 days
        recent = returns[-30:]
        avg_return = sum(recent) / len(recent)
        excess = avg_return - RISK_FREE_RATE_DAILY

        if len(recent) < 2:
            return 0.0

        # Standard deviation
        variance = sum((r - avg_return) ** 2 for r in recent) / (len(recent) - 1)
        std_dev = math.sqrt(variance) if variance > 0 else 0.001

        # Annualized Sharpe
        sharpe = (excess / std_dev) * math.sqrt(365)
        return round(sharpe, 4)

    def _calculate_max_drawdown(self, portfolio_value):
        """Track peak and maximum drawdown."""
        peak = self.performance.get("peak_portfolio", 0)
        if portfolio_value > peak:
            self.performance["peak_portfolio"] = portfolio_value
            peak = portfolio_value

        if peak > 0:
            drawdown = (peak - portfolio_value) / peak * 100
            if drawdown > self.performance.get("max_drawdown_pct", 0):
                self.performance["max_drawdown_pct"] = round(drawdown, 2)
            return round(drawdown, 2)
        return 0.0

    def _generate_insights(self, execution_data):
        """Generate actionable insights for the next research cycle."""
        insights = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pair_performance": {},
            "strategy_performance": {},
            "signal_family_performance": {},
            "signal_family_recommendations": [],
            "sharpe_ratio": self._calculate_sharpe(),
            "max_drawdown_pct": self.performance.get("max_drawdown_pct", 0),
            "total_trades": sum(
                s.get("trade_count", 0)
                for s in self.performance.get("pair_stats", {}).values()
            ),
            "recommendations": [],
        }

        # Per-pair performance summary
        for pair, stats in self.performance.get("pair_stats", {}).items():
            trade_count = stats.get("trade_count", 0)
            if trade_count == 0:
                continue
            win_rate = stats.get("win_count", 0) / trade_count if trade_count > 0 else 0
            insights["pair_performance"][pair] = {
                "trade_count": trade_count,
                "win_rate": round(win_rate, 4),
                "total_pnl": round(stats.get("total_pnl", 0), 2),
                "avg_confidence": stats.get("avg_confidence", 0),
                "total_volume": round(stats.get("total_volume", 0), 2),
            }

            # Generate recommendations
            if trade_count >= 5 and win_rate < 0.3:
                insights["recommendations"].append(
                    f"REDUCE: {pair} win rate {win_rate:.0%} on {trade_count} trades — reduce exposure"
                )
            elif trade_count >= 3 and win_rate > 0.7:
                insights["recommendations"].append(
                    f"INCREASE: {pair} win rate {win_rate:.0%} on {trade_count} trades — increase size"
                )

        # Per-strategy performance
        for strat, stats in self.performance.get("strategy_stats", {}).items():
            trade_count = stats.get("trade_count", 0)
            if trade_count == 0:
                continue
            win_rate = stats.get("win_count", 0) / trade_count if trade_count > 0 else 0
            insights["strategy_performance"][strat] = {
                "trade_count": trade_count,
                "win_rate": round(win_rate, 4),
                "total_pnl": round(stats.get("total_pnl", 0), 2),
            }

        # Per-signal-family performance.
        for family, stats in self.performance.get("signal_family_stats", {}).items():
            sample_count = stats.get("sample_count", 0)
            if sample_count <= 0:
                continue
            avg_pnl = self._safe_float(stats.get("total_pnl_usd"), 0.0) / sample_count
            win_rate = stats.get("win_count", 0) / sample_count
            realized_rate = (
                self._safe_float(stats.get("realized_samples"), 0.0) / sample_count
            )
            confidence_avg = (
                self._safe_float(stats.get("confidence_weight_sum"), 0.0) / sample_count
            )

            family_entry = {
                "sample_count": sample_count,
                "win_rate": round(win_rate, 4),
                "avg_pnl_usd": round(avg_pnl, 4),
                "realized_ratio": round(realized_rate, 4),
                "avg_confidence": round(confidence_avg, 4),
                "last_trade": stats.get("last_trade", ""),
            }
            insights["signal_family_performance"][family] = family_entry

            if sample_count >= MIN_SIGNAL_FAMILY_TRADES and avg_pnl < MIN_SIGNAL_FAMILY_DISABLE_AVG_PNL:
                insights["signal_family_recommendations"].append(
                    f"DISABLE_FAMILY: {family} avg_pnl={avg_pnl:.3f} on {sample_count} samples"
                )
                insights["signal_family_recommendations"].append(
                    f"DEPRIORITIZE_FAMILY: {family} low PnL evidence"
                )

        # Overall health
        sharpe = insights["sharpe_ratio"]
        if sharpe < -1.0:
            insights["recommendations"].append(
                f"WARNING: Sharpe ratio {sharpe:.2f} is negative — reduce all positions"
            )
        elif sharpe > 2.0:
            insights["recommendations"].append(
                f"POSITIVE: Sharpe ratio {sharpe:.2f} — strategy is working, maintain current approach"
            )

        return insights

    def run(self, cycle):
        """Process execution results and generate insights."""
        logger.info("LearningAgent cycle %d starting", cycle)
        self.state["cycle_count"] = cycle

        # Read execution results
        msgs = self.bus.read_latest("learning", msg_type="execution_results", count=5)

        execution_data = None
        for msg in msgs:
            if msg.get("cycle", -1) == cycle:
                execution_data = msg.get("payload", {})
                break

        # Get current portfolio value
        portfolio_value = self._get_current_portfolio()
        if portfolio_value > 0:
            self.performance["last_portfolio_value"] = portfolio_value

        # Track daily return
        last_value = self.performance.get("last_portfolio_value", 0)
        if last_value > 0 and portfolio_value > 0:
            daily_return = (portfolio_value - last_value) / last_value
            self.performance["daily_returns"].append(round(daily_return, 6))
            # Keep last 365 days
            if len(self.performance["daily_returns"]) > 365:
                self.performance["daily_returns"] = self.performance["daily_returns"][-365:]

        # Calculate drawdown
        current_dd = self._calculate_max_drawdown(portfolio_value)

        # Process execution results
        if execution_data:
            results = execution_data.get("results", [])
            for result in results:
                if result.get("action") in ("EXECUTED", "QUEUED"):
                    trade = result.get("trade", {})
                    pair = trade.get("pair", "")
                    strategy = trade.get("strategy_type", "unknown")
                    if pair:
                        self._update_pair_stats(pair, result)
                    if strategy:
                        self._update_strategy_stats(strategy, result)
                    self._update_signal_family_stats(result)

                    # Record trade
                    self.performance["trades"].append({
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "pair": pair,
                        "direction": trade.get("direction", ""),
                        "size_usd": trade.get("size_usd", 0),
                        "confidence": trade.get("confidence", 0),
                        "status": trade.get("status", ""),
                        "strategy": strategy,
                    })
                    # Keep last 500 trades
                    if len(self.performance["trades"]) > 500:
                        self.performance["trades"] = self.performance["trades"][-500:]

        # Generate insights
        insights = self._generate_insights(execution_data)
        insights["portfolio_value"] = portfolio_value
        insights["current_drawdown_pct"] = current_dd

        # Publish insights to ResearchAgent for next cycle
        msg_id = self.bus.publish(
            sender=self.NAME,
            recipient="research",
            msg_type="learning_insights",
            payload=insights,
            cycle=cycle,
        )
        self.state["insights_published"] += 1

        # Save performance data
        self._save_performance()

        logger.info("LearningAgent: portfolio=$%.2f | Sharpe=%.2f | DD=%.1f%% | "
                     "trades=%d | recommendations=%d (msg_id=%d)",
                     portfolio_value, insights["sharpe_ratio"], current_dd,
                     insights["total_trades"], len(insights["recommendations"]),
                     msg_id)

        for rec in insights.get("recommendations", []):
            logger.info("  Insight: %s", rec)

        return insights
