#!/usr/bin/env python3
"""RiskAgent — evaluates strategy proposals against risk limits.

Risk rules (NEVER VIOLATE):
  - $5 max per trade
  - $2 daily loss limit
  - 20% max position per asset
  - Never sell at a loss (accumulation mode)
  - 70%+ confidence required
  - 2+ confirming signals required
  - Cooldown: no same-pair trades within 10 minutes
  - Portfolio exposure: max 80% in positions

Evaluates each proposal and returns:
  - APPROVED (with possibly adjusted size)
  - REJECTED (with reason)

Input:  strategy_proposal from StrategyAgent
Output: risk_verdict -> ExecutionAgent
"""

import json
import logging
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

logger = logging.getLogger("risk_agent")

# Risk limits — absolute, non-negotiable
MAX_TRADE_USD = 5.00
MAX_DAILY_LOSS_USD = 2.00
MAX_POSITION_PCT = 0.20      # 20% max in any single asset
MAX_PORTFOLIO_EXPOSURE = 0.80  # 80% max deployed
MIN_CONFIDENCE = 0.70
MIN_CONFIRMING_SIGNALS = 2
COOLDOWN_SECONDS = 600         # 10 minutes between same-pair trades
MAX_TRADES_PER_CYCLE = 2       # Don't spam orders


class RiskAgent:
    """Evaluates proposals against risk limits. Approves or rejects."""

    NAME = "risk"

    def __init__(self):
        self.bus = MessageBus()
        self.state = {
            "daily_loss": 0.0,
            "daily_reset_date": datetime.now(timezone.utc).date().isoformat(),
            "trade_history": [],  # list of (pair, timestamp, size_usd)
            "approved_count": 0,
            "rejected_count": 0,
            "cycle_count": 0,
        }

    @staticmethod
    def _clamp(value, lo, hi):
        return max(lo, min(hi, value))

    def _get_quant_risk_overrides(self):
        """Read latest quant risk overrides from broadcast bus."""
        msgs = self.bus.read_latest("risk", msg_type="quant_risk_overrides", count=5)
        if not msgs:
            return {}
        payload = msgs[-1].get("payload", {})
        return payload if isinstance(payload, dict) else {}

    def _reset_daily_if_needed(self):
        """Reset daily counters at midnight UTC."""
        today = datetime.now(timezone.utc).date().isoformat()
        if self.state["daily_reset_date"] != today:
            self.state["daily_loss"] = 0.0
            self.state["daily_reset_date"] = today
            self.state["trade_history"] = [
                t for t in self.state["trade_history"]
                if t.get("date") == today
            ]
            logger.info("Daily risk counters reset for %s", today)

    def _get_portfolio_state(self):
        """Get current portfolio state for position sizing checks."""
        try:
            from exchange_connector import CoinbaseTrader, PriceFeed
            trader = CoinbaseTrader()
            accounts = trader.get_accounts()
            if "accounts" not in accounts:
                return None

            total_usd = 0.0
            positions = {}  # currency -> usd_value
            available_cash = 0.0

            for acc in accounts["accounts"]:
                bal = float(acc.get("available_balance", {}).get("value", 0))
                hold = float(acc.get("hold", {}).get("value", 0))
                total = bal + hold
                currency = acc.get("currency", "")
                if total <= 0:
                    continue

                if currency in ("USD", "USDC"):
                    usd_value = total
                    available_cash += total
                else:
                    price = PriceFeed.get_price(f"{currency}-USD")
                    usd_value = total * price if price else 0

                if usd_value > 0.01:
                    positions[currency] = round(usd_value, 2)
                    total_usd += usd_value

            return {
                "total_usd": round(total_usd, 2),
                "available_cash": round(available_cash, 2),
                "positions": positions,
                "exposure_pct": round((total_usd - available_cash) / total_usd * 100, 2) if total_usd > 0 else 0,
            }
        except Exception as e:
            logger.warning("Portfolio state fetch failed: %s", e)
            return None

    def _check_cooldown(self, pair):
        """Check if pair is in cooldown period."""
        now = time.time()
        for trade in reversed(self.state["trade_history"]):
            if trade.get("pair") == pair:
                elapsed = now - trade.get("timestamp", 0)
                if elapsed < COOLDOWN_SECONDS:
                    return False, f"Cooldown: {pair} traded {int(elapsed)}s ago (min {COOLDOWN_SECONDS}s)"
        return True, ""

    def evaluate_proposal(self, proposal, portfolio, overrides=None):
        """Evaluate a single strategy proposal. Returns (verdict, adjusted_proposal, reason)."""
        overrides = overrides or {}
        pair = proposal.get("pair", "NONE")
        direction = proposal.get("direction", "HOLD")
        confidence = proposal.get("confidence", 0)
        suggested_size = proposal.get("suggested_size_usd", 0)
        confirming = proposal.get("confirming_signals", 0)

        min_confidence = float(overrides.get("min_confidence", MIN_CONFIDENCE))
        max_trade_usd = float(overrides.get("max_trade_usd", MAX_TRADE_USD))
        blocked_pairs = set(overrides.get("blocked_pairs", []) or [])

        reasons = []

        # --- Hard rejections ---

        # HOLD proposals pass through
        if direction == "HOLD":
            return "HOLD", proposal, "No trade proposed"

        # Quant blocklist check
        if pair in blocked_pairs:
            return "REJECTED", proposal, f"{pair} blocked by quant risk override"

        # Confidence check
        if confidence < min_confidence:
            return "REJECTED", proposal, f"Confidence {confidence:.2f} < dynamic min {min_confidence:.2f}"

        # Confirming signals check
        if confirming < MIN_CONFIRMING_SIGNALS:
            return "REJECTED", proposal, f"Only {confirming} confirming signals (need {MIN_CONFIRMING_SIGNALS}+)"

        # Daily loss limit
        self._reset_daily_if_needed()
        if self.state["daily_loss"] <= -MAX_DAILY_LOSS_USD:
            return "REJECTED", proposal, f"Daily loss limit reached: ${self.state['daily_loss']:.2f}"

        # Cooldown check
        ok, reason = self._check_cooldown(pair)
        if not ok:
            return "REJECTED", proposal, reason

        # --- Soft checks (adjust size) ---
        adjusted_size = suggested_size

        # Cap at max trade size
        if adjusted_size > max_trade_usd:
            adjusted_size = max_trade_usd
            reasons.append(f"Capped size: ${suggested_size:.2f} -> ${max_trade_usd:.2f}")

        # Portfolio-based checks
        if portfolio:
            total = portfolio.get("total_usd", 0)
            cash = portfolio.get("available_cash", 0)
            positions = portfolio.get("positions", {})
            exposure = portfolio.get("exposure_pct", 0)

            # Ensure we have enough cash
            if cash < 1.0:
                return "REJECTED", proposal, f"Insufficient cash: ${cash:.2f}"

            if adjusted_size > cash * 0.9:  # Keep 10% buffer
                adjusted_size = round(cash * 0.8, 2)
                reasons.append(f"Reduced to 80% of cash: ${adjusted_size:.2f}")

            # Check position concentration
            base_currency = pair.split("-")[0]
            current_position = positions.get(base_currency, 0)
            if total > 0:
                position_pct = (current_position + adjusted_size) / total
                if position_pct > MAX_POSITION_PCT:
                    max_allowed = total * MAX_POSITION_PCT - current_position
                    if max_allowed < 1.0:
                        return "REJECTED", proposal, (
                            f"Position limit: {base_currency} would be "
                            f"{position_pct*100:.0f}% of portfolio (max {MAX_POSITION_PCT*100:.0f}%)"
                        )
                    adjusted_size = round(min(adjusted_size, max_allowed), 2)
                    reasons.append(f"Position-limited to ${adjusted_size:.2f}")

            # Check overall exposure
            if exposure > MAX_PORTFOLIO_EXPOSURE * 100:
                return "REJECTED", proposal, f"Portfolio exposure {exposure:.0f}% > {MAX_PORTFOLIO_EXPOSURE*100:.0f}% max"

        # Final min size check
        if adjusted_size < 1.0:
            return "REJECTED", proposal, f"Adjusted size ${adjusted_size:.2f} below $1.00 minimum"

        # --- APPROVED ---
        adjusted_proposal = dict(proposal)
        adjusted_proposal["approved_size_usd"] = adjusted_size
        if reasons:
            adjusted_proposal["risk_adjustments"] = reasons

        return "APPROVED", adjusted_proposal, f"Approved ${adjusted_size:.2f} ({'; '.join(reasons) if reasons else 'no adjustments'})"

    def run(self, cycle):
        """Process all strategy proposals for this cycle."""
        logger.info("RiskAgent cycle %d starting", cycle)
        self.state["cycle_count"] = cycle

        overrides = self._get_quant_risk_overrides()
        max_trades_cycle = int(self._clamp(overrides.get("max_trades_per_cycle", MAX_TRADES_PER_CYCLE), 1, 5))
        if overrides:
            logger.info(
                "Quant overrides: min_conf=%.2f max_trade=$%.2f max_trades=%d blocked=%d",
                float(overrides.get("min_confidence", MIN_CONFIDENCE)),
                float(overrides.get("max_trade_usd", MAX_TRADE_USD)),
                max_trades_cycle,
                len(overrides.get("blocked_pairs", []) or []),
            )

        # Read strategy proposals
        proposals = self.bus.read_latest("risk", msg_type="strategy_proposal", count=10)
        if not proposals:
            logger.info("No proposals to evaluate")
            return []

        # Get current portfolio state
        portfolio = self._get_portfolio_state()
        if portfolio:
            logger.info("Portfolio: $%.2f | Cash: $%.2f | Exposure: %.0f%%",
                         portfolio["total_usd"], portfolio["available_cash"],
                         portfolio["exposure_pct"])

        verdicts = []
        approved_this_cycle = 0

        for msg in proposals:
            if msg.get("cycle", -1) != cycle:
                continue  # Only process current cycle's proposals

            proposal = msg.get("payload", {})

            # Max trades per cycle
            if approved_this_cycle >= max_trades_cycle:
                verdict = "REJECTED"
                reason = f"Max {max_trades_cycle} trades per cycle already approved"
                adjusted = proposal
            else:
                verdict, adjusted, reason = self.evaluate_proposal(proposal, portfolio, overrides=overrides)

            if verdict == "APPROVED":
                approved_this_cycle += 1
                self.state["approved_count"] += 1
                # Record in trade history
                self.state["trade_history"].append({
                    "pair": proposal.get("pair"),
                    "timestamp": time.time(),
                    "size_usd": adjusted.get("approved_size_usd", 0),
                    "date": datetime.now(timezone.utc).date().isoformat(),
                })
            elif verdict == "REJECTED":
                self.state["rejected_count"] += 1

            verdict_msg = {
                "verdict": verdict,
                "proposal": adjusted,
                "reason": reason,
                "portfolio_snapshot": portfolio,
                "cycle": cycle,
            }
            verdicts.append(verdict_msg)

            # Publish verdict to ExecutionAgent
            msg_id = self.bus.publish(
                sender=self.NAME,
                recipient="execution",
                msg_type="risk_verdict",
                payload=verdict_msg,
                cycle=cycle,
            )

            status_icon = "APPROVED" if verdict == "APPROVED" else ("HOLD" if verdict == "HOLD" else "REJECTED")
            logger.info("%s: %s %s conf=%.2f — %s (msg_id=%d)",
                         status_icon,
                         adjusted.get("direction", "?"),
                         adjusted.get("pair", "?"),
                         adjusted.get("confidence", 0),
                         reason, msg_id)

        logger.info("RiskAgent: %d approved, %d total this cycle",
                     approved_this_cycle, len(verdicts))

        return verdicts
