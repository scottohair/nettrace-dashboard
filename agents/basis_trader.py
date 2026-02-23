#!/usr/bin/env python3
"""Futures Trade Agent — leveraged signal-driven trading on Coinbase CDE.

Monitors basis spreads and uses signal pipeline for leveraged directional trades.
Does NOT lock all capital in futures — uses futures when signals are strong
to amplify gains with margin efficiency (3-5x leverage = more bang per dollar).

Strategy:
  - When basis spread > threshold: execute basis trade (buy spot + sell future)
  - When strong signal on tradeable future: directional trade with leverage
  - All trades respect margin health, position limits, and risk caps
  - Futures are ONE tool in the arsenal, not the only one

Safety:
  - Respects PERP_TRADING_ENABLED env gate
  - Margin health checks before opening
  - Max position limits from risk controller
  - Automatic closeout 2 days before expiry
  - Never uses more than 70% of buying power
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
    format="%(asctime)s [basis] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / "basis_trader.log"),
    ],
)
logger = logging.getLogger("basis_trader")

# Configuration
SCAN_INTERVAL_S = int(os.environ.get("BASIS_SCAN_INTERVAL_S", "60"))
MIN_SPREAD_PCT = float(os.environ.get("BASIS_MIN_SPREAD_PCT", "1.5"))
MAX_TRADE_USD = float(os.environ.get("BASIS_MAX_TRADE_USD", "25.0"))
CLOSE_BEFORE_EXPIRY_DAYS = int(os.environ.get("BASIS_CLOSE_BEFORE_EXPIRY_DAYS", "2"))
MAX_OPEN_BASIS_TRADES = int(os.environ.get("BASIS_MAX_OPEN_TRADES", "3"))

STATUS_FILE = Path(__file__).parent / "basis_trader_status.json"
TRADES_FILE = Path(__file__).parent / "basis_trader_trades.jsonl"


def _save_status(data):
    try:
        STATUS_FILE.write_text(json.dumps(data, indent=2, default=str))
    except Exception:
        pass


def _append_trade(trade):
    try:
        with open(TRADES_FILE, "a") as f:
            f.write(json.dumps(trade, default=str) + "\n")
    except Exception:
        pass


class BasisTrader:
    """Continuously scans and executes basis trades on CDE futures."""

    def __init__(self):
        from coinbase_derivatives_connector import CoinbaseDerivativesConnector
        self.dc = CoinbaseDerivativesConnector()
        self.open_trades = []  # [{ticker, spot_pair, future_id, entry_spread, entry_time, ...}]
        self.total_pnl = 0.0
        self.trades_executed = 0
        self._load_state()

    def _load_state(self):
        """Load persisted state."""
        if STATUS_FILE.exists():
            try:
                state = json.loads(STATUS_FILE.read_text())
                trades = state.get("trades", [])
                # Backward compat: open_trades was stored as count in early versions
                self.open_trades = trades if isinstance(trades, list) else []
                self.total_pnl = state.get("total_pnl", 0.0)
                self.trades_executed = state.get("trades_executed", 0)
                logger.info("Loaded state: %d open trades, $%.4f total PnL",
                            len(self.open_trades), self.total_pnl)
            except Exception:
                pass

    def scan_and_trade(self):
        """One scan cycle: find opportunities, execute if profitable."""
        if not self.dc.enabled:
            logger.warning("Futures trading disabled (PERP_TRADING_ENABLED=0)")
            return

        health = self.dc.margin_health()
        if not health.get("can_open_new"):
            logger.info("Cannot open new positions: %s", health)
            return

        # Check open trade count
        if len(self.open_trades) >= MAX_OPEN_BASIS_TRADES:
            logger.info("Max open basis trades (%d) reached", MAX_OPEN_BASIS_TRADES)
            return

        # Scan Coinbase opportunities
        opportunities = self.dc.scan_basis_opportunities()

        # Scan Kraken Futures opportunities (additive, not replacing Coinbase)
        try:
            from kraken_futures_connector import KrakenFuturesConnector
            kfc = KrakenFuturesConnector()
            if kfc.enabled:
                # Gather spot prices from Coinbase for basis comparison
                spot_prices = {}
                for opp in opportunities:
                    spot_prices[opp["ticker"]] = opp["spot_price"]
                kraken_basis = kfc.scan_basis_opportunities(spot_prices or None)
                for opp in kraken_basis:
                    logger.info(
                        "Kraken basis opportunity: %s %.2f%% (spot=%.2f, futures=%.2f)",
                        opp["product_id"], opp["basis_pct"],
                        opp["spot_price"], opp["futures_price"],
                    )
                    # Adapt Kraken opportunity format to match Coinbase format
                    # so the downstream execution logic can handle both
                    opportunities.append({
                        "ticker": opp["base"],
                        "spot_pair": f"{opp['base']}-USD",
                        "future_id": opp["product_id"],
                        "spot_price": opp["spot_price"],
                        "future_price": opp["futures_price"],
                        "spread_usd": round(opp["futures_price"] - opp["spot_price"], 4),
                        "spread_pct": opp["basis_pct"],
                        "net_spread_pct": opp["basis_pct"],  # Kraken has lower fees
                        "annualized_pct": abs(opp["basis_pct"]) * 365 / 30,  # rough annualization
                        "net_annualized_pct": abs(opp["basis_pct"]) * 365 / 30,
                        "days_to_expiry": 999,  # perpetuals
                        "expiry": "2099-12-31T00:00:00+00:00",
                        "fee_cost_pct": 0.04,  # 2x Kraken futures maker fee
                        "contract_size": 1,
                        "margin_required_pct": 20.0,
                        "venue": "kraken",
                    })
                if kraken_basis:
                    logger.info("Added %d Kraken basis opportunities", len(kraken_basis))
        except ImportError:
            pass
        except Exception as e:
            logger.warning("Kraken basis scan failed: %s", e)

        if not opportunities:
            logger.info("No basis opportunities above %.1f%% threshold", MIN_SPREAD_PCT)
            return

        logger.info("Found %d total basis opportunities (Coinbase + Kraken)", len(opportunities))

        # Execute top opportunity that we don't already have
        existing_futures = {t.get("future_id") for t in self.open_trades}
        buying_power = health.get("buying_power", 0)

        for opp in opportunities:
            if opp["future_id"] in existing_futures:
                continue

            contract_size = opp.get("contract_size", 1)
            notional = opp["future_price"] * contract_size
            margin_pct = max(opp.get("margin_required_pct", 20), 20) / 100
            margin_needed = notional * margin_pct
            spot_cost = opp["spot_price"] * contract_size

            # Decide trade mode based on capital
            # Full basis (hedged): needs spot_cost + margin
            # Futures-only (directional): needs only margin
            can_basis = (spot_cost + margin_needed) < buying_power * 0.7
            can_futures_only = margin_needed < buying_power * 0.7

            if not can_futures_only:
                logger.info("Skip %s: margin $%.2f > 50%% of buying power $%.2f",
                            opp["ticker"], margin_needed, buying_power)
                continue

            trade_mode = "basis" if can_basis else "futures_short"

            logger.info("EXECUTING %s: %s | spread=%.2f%% ann=%.1f%% | "
                        "spot=$%.4f fut=$%.4f | notional=$%.2f margin=$%.2f",
                        trade_mode.upper(), opp["ticker"], opp["net_spread_pct"],
                        opp["net_annualized_pct"], opp["spot_price"], opp["future_price"],
                        notional, margin_needed)

            if trade_mode == "basis":
                result = self.dc.place_basis_trade(
                    ticker=opp["ticker"],
                    future_id=opp["future_id"],
                    spot_price=opp["spot_price"],
                    future_price=opp["future_price"],
                    trade_usd=spot_cost,  # match contract notional
                )
            else:
                # Futures-only: just sell the overpriced future
                result = self.dc.place_perp_order(
                    product_id=opp["future_id"],
                    side="SELL",
                    size=1,  # 1 contract
                    price=opp["future_price"] * 0.999,
                    post_only=True,
                )
                result = {"future_order": result, "mode": "futures_short"}

            # Track the trade
            trade_record = {
                "ticker": opp["ticker"],
                "future_id": opp["future_id"],
                "spot_pair": opp["spot_pair"],
                "mode": trade_mode,
                "entry_spread_pct": opp["spread_pct"],
                "entry_net_spread_pct": opp["net_spread_pct"],
                "entry_annualized_pct": opp["net_annualized_pct"],
                "spot_price": opp["spot_price"],
                "future_price": opp["future_price"],
                "notional": notional,
                "margin_needed": margin_needed,
                "days_to_expiry": opp["days_to_expiry"],
                "expiry": opp["expiry"],
                "entry_time": datetime.now(timezone.utc).isoformat(),
                "result": result,
                "contract_size": contract_size,
            }

            has_error = (
                isinstance(result.get("spot_order"), dict) and result["spot_order"].get("error_response")
            ) or (
                isinstance(result.get("future_order"), dict) and result["future_order"].get("error_response")
            ) or result.get("error")

            if has_error:
                logger.warning("Trade failed: %s", result.get("error") or result)
                _append_trade({**trade_record, "status": "failed"})
            else:
                self.open_trades.append(trade_record)
                self.trades_executed += 1
                _append_trade({**trade_record, "status": "opened"})
                logger.info("%s OPENED: %s %.2f%% spread, $%.2f notional",
                            trade_mode.upper(), opp["ticker"], opp["net_spread_pct"], notional)

            # Only one trade per cycle
            break

    def monitor_positions(self):
        """Monitor open basis trades, close if spread converged or near expiry."""
        if not self.open_trades:
            return

        to_remove = []
        for i, trade in enumerate(self.open_trades):
            try:
                # Check days to expiry
                expiry = datetime.fromisoformat(trade["expiry"])
                days_left = (expiry - datetime.now(timezone.utc)).days

                if days_left <= CLOSE_BEFORE_EXPIRY_DAYS:
                    logger.info("CLOSING basis %s: %d days to expiry",
                                trade["ticker"], days_left)
                    self._close_basis_trade(trade)
                    to_remove.append(i)
                    continue

                # Check current spread
                spot_prices = self.dc._fetch_spot_prices([trade["ticker"]])
                spot_now = spot_prices.get(trade["ticker"], 0)
                if spot_now <= 0:
                    continue

                futures_map = self.dc.build_futures_mapping()
                futs = futures_map.get(trade["ticker"], [])
                fut_price = 0
                for f in futs:
                    if f["product_id"] == trade["future_id"]:
                        fut_price = f.get("price", 0)
                        break

                if fut_price <= 0:
                    continue

                current_spread = ((fut_price - spot_now) / spot_now) * 100
                entry_spread = trade.get("entry_spread_pct", 0)
                convergence = entry_spread - current_spread

                logger.info("BASIS %s: entry=%.2f%% now=%.2f%% convergence=%.2f%% days=%d",
                            trade["ticker"], entry_spread, current_spread, convergence, days_left)

                # Close if spread has converged significantly (>80% of initial spread captured)
                if convergence > entry_spread * 0.8:
                    logger.info("EARLY CLOSE %s: %.2f%% of spread captured", trade["ticker"],
                                (convergence / entry_spread * 100) if entry_spread > 0 else 0)
                    self._close_basis_trade(trade)
                    to_remove.append(i)

            except Exception as e:
                logger.error("Monitor error for %s: %s", trade.get("ticker"), e)

        for idx in sorted(to_remove, reverse=True):
            trade = self.open_trades.pop(idx)
            _append_trade({**trade, "status": "closed", "close_time": datetime.now(timezone.utc).isoformat()})

    def _close_basis_trade(self, trade):
        """Close both legs of a basis trade."""
        ticker = trade["ticker"]
        future_id = trade["future_id"]

        # Close futures position (SELL was our entry, so BUY to close)
        logger.info("Closing futures leg: %s", future_id)
        self.dc.close_position(future_id)

        # Sell spot position
        spot_pair = trade.get("spot_pair", f"{ticker}-USD")
        logger.info("Selling spot leg: %s", spot_pair)
        try:
            from exchange_connector import CoinbaseTrader
            trader = CoinbaseTrader()
            ticker_data = trader.get_ticker(spot_pair)
            if ticker_data:
                price = float(ticker_data.get("price", 0))
                contract_size = trade.get("contract_size", 1)
                size = round(MAX_TRADE_USD / price, 8) if price > 0 else 0
                if size > 0:
                    trader.place_limit_order(
                        product_id=spot_pair,
                        side="SELL",
                        base_size=size,
                        limit_price=price * 0.999,
                        post_only=False,
                    )
        except Exception as e:
            logger.error("Error closing spot leg %s: %s", spot_pair, e)

    def run(self):
        """Main loop."""
        logger.info("=== BASIS TRADER STARTING ===")
        logger.info("Config: scan=%ds min_spread=%.1f%% max_trade=$%.2f max_open=%d",
                     SCAN_INTERVAL_S, MIN_SPREAD_PCT, MAX_TRADE_USD, MAX_OPEN_BASIS_TRADES)

        while True:
            try:
                cycle_start = time.time()

                # Monitor existing positions
                self.monitor_positions()

                # Scan for new opportunities
                self.scan_and_trade()

                # Save status
                _save_status({
                    "last_scan": datetime.now(timezone.utc).isoformat(),
                    "open_trades": len(self.open_trades),
                    "trades_executed": self.trades_executed,
                    "total_pnl": self.total_pnl,
                    "trades": [
                        {
                            "ticker": t["ticker"],
                            "future_id": t["future_id"],
                            "entry_spread": t.get("entry_spread_pct"),
                            "entry_time": t.get("entry_time"),
                            "days_to_expiry": t.get("days_to_expiry"),
                        }
                        for t in self.open_trades
                    ],
                    "health": self.dc.margin_health(),
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
    trader = BasisTrader()
    trader.run()
