#!/usr/bin/env python3
"""Kraken Funding Rate Arbitrage Agent -- cross-venue delta-neutral income.

Compares funding rates between Coinbase perps and Kraken perps.
When funding diverges: short the high-funding venue, long the low-funding venue.
Delta-neutral position captures the funding differential.

Example:
  Kraken BTC funding: +0.05% (longs pay shorts)
  Coinbase BTC funding: +0.01% (longs pay shorts)
  -> Short on Kraken (collect 0.05%), Long on Coinbase (pay 0.01%)
  -> Net income: 0.04% per 8 hours = ~18% annualized

Runs every 30 minutes. All trades gated through GoalValidator.
BE A MAKER: limit orders only on both venues.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Setup path
sys.path.insert(0, str(Path(__file__).parent))

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [funding_arb] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "kraken_funding_arb.log"),
    ],
)
logger = logging.getLogger("kraken_funding_arb")

# GoalValidator
try:
    from agent_goals import GoalValidator
except ImportError:
    GoalValidator = None

# Configuration
SCAN_INTERVAL_S = int(os.environ.get("FUNDING_ARB_SCAN_INTERVAL_S", "1800"))  # 30 min
MIN_DIVERGENCE_PCT = float(os.environ.get("FUNDING_ARB_MIN_DIVERGENCE", "0.02"))  # 0.02%
MAX_POSITION_USD = float(os.environ.get("FUNDING_ARB_MAX_POSITION_USD", "50.0"))
CONVERGENCE_CLOSE_PCT = float(os.environ.get("FUNDING_ARB_CONVERGENCE_CLOSE", "0.005"))  # close when < 0.005%
MAX_OPEN_ARBS = int(os.environ.get("FUNDING_ARB_MAX_OPEN", "3"))

STATUS_FILE = Path(__file__).parent / "kraken_funding_arb_status.json"
TRADES_FILE = Path(__file__).parent / "kraken_funding_arb_trades.jsonl"


def _save_status(data):
    """Persist status to file."""
    try:
        STATUS_FILE.write_text(json.dumps(data, indent=2, default=str))
    except Exception:
        pass


def _append_trade(trade):
    """Append trade record to JSONL file."""
    try:
        with open(TRADES_FILE, "a") as f:
            f.write(json.dumps(trade, default=str) + "\n")
    except Exception:
        pass


class FundingArbAgent:
    """Cross-venue funding rate arbitrage agent.

    Compares Kraken Futures and Coinbase perp funding rates.
    When divergence exceeds threshold, opens delta-neutral positions
    to capture the funding differential.
    """

    def __init__(self):
        self.kraken_connector = None
        self.coinbase_connector = None
        self.open_arbs = []  # [{base, kraken_side, cb_side, entry_divergence, ...}]
        self.total_pnl = 0.0
        self.trades_executed = 0
        self._init_connectors()
        self._load_state()

    def _init_connectors(self):
        """Initialize venue connectors."""
        try:
            from kraken_futures_connector import KrakenFuturesConnector
            self.kraken_connector = KrakenFuturesConnector()
        except ImportError:
            logger.warning("KrakenFuturesConnector not available")

        try:
            from coinbase_derivatives_connector import CoinbaseDerivativesConnector
            self.coinbase_connector = CoinbaseDerivativesConnector()
        except ImportError:
            logger.warning("CoinbaseDerivativesConnector not available")

    def _load_state(self):
        """Load persisted state."""
        if STATUS_FILE.exists():
            try:
                state = json.loads(STATUS_FILE.read_text())
                arbs = state.get("open_arbs", [])
                self.open_arbs = arbs if isinstance(arbs, list) else []
                self.total_pnl = state.get("total_pnl", 0.0)
                self.trades_executed = state.get("trades_executed", 0)
                logger.info("Loaded state: %d open arbs, $%.4f total PnL",
                            len(self.open_arbs), self.total_pnl)
            except Exception:
                pass

    def get_kraken_funding_rates(self):
        """Fetch all Kraken perpetual funding rates.

        Returns: dict of {base_currency: {"rate": float, "product_id": str}}
        """
        if not self.kraken_connector:
            return {}

        rates = self.kraken_connector.get_funding_rate()
        if isinstance(rates, dict) and rates.get("error"):
            return {}

        result = {}
        for product_id, data in rates.items():
            if not isinstance(data, dict):
                continue
            # Extract base from PF_XBTUSD -> BTC
            base = str(product_id).replace("PF_", "").replace("USD", "")
            if base == "XBT":
                base = "BTC"
            elif base == "XDG":
                base = "DOGE"
            result[base] = {
                "rate": data.get("funding_rate", 0),
                "product_id": product_id,
                "mark_price": data.get("mark_price", 0),
            }
        return result

    def get_coinbase_funding_rates(self):
        """Fetch all Coinbase perpetual funding rates.

        Returns: dict of {base_currency: {"rate": float, "product_id": str}}
        """
        if not self.coinbase_connector:
            return {}

        try:
            products = self.coinbase_connector.list_perp_products()
            result = {}
            for p in products:
                if not isinstance(p, dict):
                    continue
                pid = str(p.get("product_id", "")).upper()
                base = str(
                    p.get("base_currency_id") or p.get("base_currency") or ""
                ).upper()
                if not base:
                    continue
                rate = 0.0
                perp_details = p.get("perpetual_details", {})
                if isinstance(perp_details, dict):
                    rate = float(perp_details.get("funding_rate", 0) or 0)
                else:
                    rate = float(p.get("funding_rate", 0) or 0)
                result[base] = {
                    "rate": rate,
                    "product_id": pid,
                    "mark_price": float(p.get("price") or p.get("mid_market_price") or 0),
                }
            return result
        except Exception as e:
            logger.error("Failed to get Coinbase funding rates: %s", e)
            return {}

    def find_divergences(self):
        """Compare funding rates across venues and find divergences.

        Returns: list of divergence dicts sorted by |divergence| desc
        """
        kraken_rates = self.get_kraken_funding_rates()
        coinbase_rates = self.get_coinbase_funding_rates()

        if not kraken_rates or not coinbase_rates:
            return []

        divergences = []
        common_bases = set(kraken_rates.keys()) & set(coinbase_rates.keys())

        for base in common_bases:
            k_rate = kraken_rates[base]["rate"]
            c_rate = coinbase_rates[base]["rate"]
            divergence = abs(k_rate - c_rate)

            if divergence < MIN_DIVERGENCE_PCT:
                continue

            # Determine which venue to short and which to long
            # Short the venue with higher funding (collect funding)
            # Long the venue with lower funding (pay less)
            if k_rate > c_rate:
                # Kraken funding higher -> short Kraken, long Coinbase
                short_venue = "kraken"
                long_venue = "coinbase"
            else:
                # Coinbase funding higher -> short Coinbase, long Kraken
                short_venue = "coinbase"
                long_venue = "kraken"

            mark_price = kraken_rates[base].get("mark_price") or coinbase_rates[base].get("mark_price", 0)

            divergences.append({
                "base": base,
                "kraken_rate": k_rate,
                "coinbase_rate": c_rate,
                "divergence": divergence,
                "divergence_annualized_pct": divergence * 3 * 365,  # 8h funding
                "short_venue": short_venue,
                "long_venue": long_venue,
                "kraken_product": kraken_rates[base]["product_id"],
                "coinbase_product": coinbase_rates[base]["product_id"],
                "mark_price": mark_price,
            })

        return sorted(divergences, key=lambda x: x["divergence"], reverse=True)

    def execute_arb(self, divergence):
        """Execute a delta-neutral funding arb across venues.

        Args:
            divergence: dict from find_divergences()

        Returns: dict with execution result
        """
        base = divergence["base"]
        mark_price = divergence["mark_price"]

        if mark_price <= 0:
            return {"error": f"No mark price for {base}"}

        # Calculate position size
        size = MAX_POSITION_USD / mark_price if mark_price > 0 else 0
        if size <= 0:
            return {"error": "Position size too small"}

        # GoalValidator gate
        confidence = 0.75  # Funding arb has high confidence (structural edge)
        if GoalValidator:
            # Check both legs
            buy_ok = GoalValidator.should_trade(confidence, 2, "BUY", "neutral")
            sell_ok = GoalValidator.should_trade(confidence, 2, "SELL", "neutral")
            if not (buy_ok and sell_ok):
                return {"error": "GoalValidator blocked arb trade"}

        result = {
            "base": base,
            "divergence": divergence["divergence"],
            "short_venue": divergence["short_venue"],
            "long_venue": divergence["long_venue"],
        }

        # Place short leg
        if divergence["short_venue"] == "kraken":
            short_result = self._place_kraken_order(
                divergence["kraken_product"], "sell", size, mark_price
            )
            long_result = self._place_coinbase_order(
                divergence["coinbase_product"], "BUY", size, mark_price
            )
        else:
            short_result = self._place_coinbase_order(
                divergence["coinbase_product"], "SELL", size, mark_price
            )
            long_result = self._place_kraken_order(
                divergence["kraken_product"], "buy", size, mark_price
            )

        result["short_order"] = short_result
        result["long_order"] = long_result

        has_error = (
            (isinstance(short_result, dict) and short_result.get("error"))
            or (isinstance(long_result, dict) and long_result.get("error"))
        )

        if has_error:
            logger.warning("Arb execution had errors: %s", result)
        else:
            logger.info("ARB OPENED: %s divergence=%.4f%% short=%s long=%s",
                        base, divergence["divergence"],
                        divergence["short_venue"], divergence["long_venue"])

        return result

    def _place_kraken_order(self, product_id, side, size, price):
        """Place order on Kraken Futures (post-only limit)."""
        if not self.kraken_connector or not self.kraken_connector.enabled:
            return {"error": "Kraken Futures not enabled"}

        from kraken_futures_connector import KrakenFuturesConnector
        return KrakenFuturesConnector.place_futures_order(
            product_id=product_id,
            side=side,
            size=round(size, 6),
            price=price,
            post_only=True,
            confidence=0.75,
        )

    def _place_coinbase_order(self, product_id, side, size, price):
        """Place order on Coinbase perps (post-only limit)."""
        if not self.coinbase_connector or not self.coinbase_connector.enabled:
            return {"error": "Coinbase perps not enabled"}

        return self.coinbase_connector.place_perp_order(
            product_id=product_id,
            side=side,
            size=round(size, 6),
            price=price,
            post_only=True,
        )

    def monitor_arbs(self):
        """Monitor open arb positions. Close when funding converges."""
        if not self.open_arbs:
            return

        to_remove = []
        for i, arb in enumerate(self.open_arbs):
            try:
                base = arb["base"]
                kraken_rates = self.get_kraken_funding_rates()
                coinbase_rates = self.get_coinbase_funding_rates()

                k_rate = kraken_rates.get(base, {}).get("rate", 0)
                c_rate = coinbase_rates.get(base, {}).get("rate", 0)
                current_divergence = abs(k_rate - c_rate)

                entry_divergence = arb.get("entry_divergence", 0)

                logger.info("ARB %s: entry=%.4f%% current=%.4f%% (k=%.4f%% c=%.4f%%)",
                            base, entry_divergence, current_divergence, k_rate, c_rate)

                # Close if funding has converged
                if current_divergence < CONVERGENCE_CLOSE_PCT:
                    logger.info("CLOSING ARB %s: funding converged (%.4f%% < %.4f%%)",
                                base, current_divergence, CONVERGENCE_CLOSE_PCT)
                    self._close_arb(arb)
                    to_remove.append(i)

            except Exception as e:
                logger.error("Monitor error for arb %s: %s", arb.get("base"), e)

        for idx in sorted(to_remove, reverse=True):
            arb = self.open_arbs.pop(idx)
            _append_trade({**arb, "status": "closed",
                           "close_time": datetime.now(timezone.utc).isoformat()})

    def _close_arb(self, arb):
        """Close both legs of an arb position."""
        base = arb["base"]
        logger.info("Closing arb for %s", base)

        # Close Kraken leg
        kraken_product = arb.get("kraken_product")
        if kraken_product and self.kraken_connector:
            try:
                from kraken_futures_connector import KrakenFuturesConnector
                KrakenFuturesConnector.close_position(kraken_product)
            except Exception as e:
                logger.error("Failed to close Kraken leg %s: %s", kraken_product, e)

        # Close Coinbase leg
        cb_product = arb.get("coinbase_product")
        if cb_product and self.coinbase_connector:
            try:
                self.coinbase_connector.close_position(cb_product)
            except Exception as e:
                logger.error("Failed to close Coinbase leg %s: %s", cb_product, e)

    def scan_and_trade(self):
        """One scan cycle: find divergences, execute if profitable."""
        # Check both connectors
        if not self.kraken_connector or not self.coinbase_connector:
            logger.warning("Missing venue connector(s), skipping scan")
            return

        if not self.kraken_connector.enabled:
            logger.info("Kraken Futures not enabled, skipping scan")
            return

        if len(self.open_arbs) >= MAX_OPEN_ARBS:
            logger.info("Max open arbs (%d) reached", MAX_OPEN_ARBS)
            return

        divergences = self.find_divergences()
        if not divergences:
            logger.info("No funding divergences above %.4f%% threshold", MIN_DIVERGENCE_PCT)
            return

        logger.info("Found %d funding divergences", len(divergences))

        # Execute top divergence not already open
        existing_bases = {a.get("base") for a in self.open_arbs}

        for div in divergences:
            if div["base"] in existing_bases:
                continue

            logger.info("DIVERGENCE: %s kraken=%.4f%% coinbase=%.4f%% div=%.4f%% (%.1f%% ann)",
                        div["base"], div["kraken_rate"], div["coinbase_rate"],
                        div["divergence"], div["divergence_annualized_pct"])

            result = self.execute_arb(div)

            has_error = (
                isinstance(result.get("short_order"), dict) and result["short_order"].get("error")
            ) or (
                isinstance(result.get("long_order"), dict) and result["long_order"].get("error")
            ) or result.get("error")

            arb_record = {
                "base": div["base"],
                "entry_divergence": div["divergence"],
                "kraken_rate": div["kraken_rate"],
                "coinbase_rate": div["coinbase_rate"],
                "short_venue": div["short_venue"],
                "long_venue": div["long_venue"],
                "kraken_product": div["kraken_product"],
                "coinbase_product": div["coinbase_product"],
                "entry_time": datetime.now(timezone.utc).isoformat(),
                "result": result,
            }

            if has_error:
                logger.warning("Arb trade failed: %s", result.get("error") or result)
                _append_trade({**arb_record, "status": "failed"})
            else:
                self.open_arbs.append(arb_record)
                self.trades_executed += 1
                _append_trade({**arb_record, "status": "opened"})

            # One trade per cycle
            break

    def run(self):
        """Main loop."""
        logger.info("=== FUNDING ARB AGENT STARTING ===")
        logger.info("Config: scan=%ds min_div=%.4f%% max_pos=$%.2f max_open=%d",
                     SCAN_INTERVAL_S, MIN_DIVERGENCE_PCT, MAX_POSITION_USD, MAX_OPEN_ARBS)

        while True:
            try:
                cycle_start = time.time()

                # Monitor existing arbs
                self.monitor_arbs()

                # Scan for new opportunities
                self.scan_and_trade()

                # Save status
                _save_status({
                    "last_scan": datetime.now(timezone.utc).isoformat(),
                    "open_arbs": self.open_arbs,
                    "trades_executed": self.trades_executed,
                    "total_pnl": self.total_pnl,
                })

                elapsed = time.time() - cycle_start
                sleep_time = max(1, SCAN_INTERVAL_S - elapsed)
                logger.info("Cycle done in %.1fs, sleeping %.0fs", elapsed, sleep_time)
                time.sleep(sleep_time)

            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error("Cycle error: %s", e, exc_info=True)
                time.sleep(30)


if __name__ == "__main__":
    agent = FundingArbAgent()
    agent.run()
