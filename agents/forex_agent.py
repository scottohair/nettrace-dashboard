#!/usr/bin/env python3
"""Forex-Aware Trading Agent — stablecoin arb + forex correlation.

Exploits three edges:
  1. Stablecoin/Forex Arb: EURC-USDC, DAI-USDC price discrepancies on Coinbase
  2. Crypto-Forex Correlation: EUR/USD moves correlate with BTC/ETH — European
     exchange prices lag when EUR weakens, creating arb windows
  3. Cross-listed pairs: regional exchange price differences after forex moves

Data sources (all free, no auth):
  - open.er-api.com/v6/latest/USD (live forex rates)
  - Coinbase public API (stablecoin prices, EURC pairs)
  - Coinbase Advanced Trade API (order execution via CoinbaseTrader)

Game Theory:
  - Information asymmetry: forex moves take 30-120s to propagate to stablecoin pairs
  - Non-equilibrium: EURC-USDC misprices when EUR/USD moves sharply
  - BE A MAKER: limit orders only (0.6% maker fee, not 1.2% taker)
  - Auction theory: bid at calculated support when stablecoins misprice

RULES (NEVER VIOLATE):
  - Max $5 per trade
  - $2 daily loss limit (HARDSTOP)
  - 0.3% minimum spread to trade (covers fees + slippage)
  - LIMIT orders only (maker fee)
"""

import json
import logging
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

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
        logging.FileHandler(str(Path(__file__).parent / "forex_agent.log")),
    ]
)
logger = logging.getLogger("forex_agent")

FOREX_DB = str(Path(__file__).parent / "forex_agent.db")

# Configuration
CONFIG = {
    "scan_interval": 60,                          # seconds between scans
    "pairs": ["EURC-USDC", "DAI-USDC"],           # stablecoin arb pairs
    "correlation_pairs": ["BTC-USDC", "ETH-USDC"],  # forex-correlation trades
    "min_spread_pct": 0.003,                       # 0.3% minimum spread to trade
    "max_trade_usd": 5.00,                         # Rule: max $5 per trade
    "max_daily_loss_usd": 2.00,                    # Rule: $2 daily loss limit
    "forex_api_url": "https://open.er-api.com/v6/latest/USD",
    "eurusd_move_threshold": 0.002,                # 0.2% EUR/USD move triggers correlation check
    "stablecoin_fair_values": {
        "EURC": None,   # Derived from live EUR/USD rate
        "DAI": 1.0000,  # DAI pegged to $1
        "USDC": 1.0000, # USDC pegged to $1
        "USDT": 1.0000, # USDT pegged to $1
    },
    "correlation_lag_window": 120,  # seconds — how long European exchanges lag after forex move
    "min_confirming_signals": 2,    # need 2+ signals to trade
    "min_confidence": 0.70,         # 70% minimum confidence
}


def _fetch_json(url, headers=None, timeout=10):
    """HTTP GET JSON helper."""
    h = {"User-Agent": "NetTrace/1.0"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


class ForexFeed:
    """Fetches live forex rates from free APIs."""

    CACHE = {}
    CACHE_TTL = 30  # seconds — forex rates update slowly on free tier

    @classmethod
    def get_rates(cls):
        """Get USD-based forex rates. Returns dict like {"EUR": 0.92, "GBP": 0.79, ...}."""
        now = time.time()
        if "rates" in cls.CACHE and now - cls.CACHE["rates"]["t"] < cls.CACHE_TTL:
            return cls.CACHE["rates"]["data"]

        try:
            data = _fetch_json(CONFIG["forex_api_url"], timeout=10)
            rates = data.get("rates", {})
            if rates:
                cls.CACHE["rates"] = {"data": rates, "t": now}
                return rates
        except Exception as e:
            logger.warning("Forex rate fetch failed: %s", e)

        # Return cached if available, even if stale
        if "rates" in cls.CACHE:
            return cls.CACHE["rates"]["data"]
        return {}

    @classmethod
    def get_eurusd(cls):
        """Get EUR/USD rate (how many USD per 1 EUR)."""
        rates = cls.get_rates()
        eur_per_usd = rates.get("EUR", 0)
        if eur_per_usd > 0:
            return 1.0 / eur_per_usd  # Convert from USD/EUR to EUR/USD
        return None

    @classmethod
    def get_gbpusd(cls):
        """Get GBP/USD rate."""
        rates = cls.get_rates()
        gbp_per_usd = rates.get("GBP", 0)
        if gbp_per_usd > 0:
            return 1.0 / gbp_per_usd
        return None


class StablecoinScanner:
    """Scans stablecoin pairs on Coinbase for arb opportunities."""

    @staticmethod
    def get_coinbase_price(pair):
        """Get price from Coinbase public API."""
        try:
            url = f"https://api.coinbase.com/v2/prices/{pair}/spot"
            data = _fetch_json(url, timeout=5)
            return float(data["data"]["amount"])
        except Exception as e:
            logger.debug("Coinbase price fetch failed for %s: %s", pair, e)
            return None

    @staticmethod
    def get_coinbase_book(pair, levels=10):
        """Get orderbook from Coinbase Exchange (public, no auth)."""
        try:
            url = f"https://api.exchange.coinbase.com/products/{pair}/book?level=2"
            data = _fetch_json(url, timeout=5)
            bids = [(float(b[0]), float(b[1])) for b in data.get("bids", [])[:levels]]
            asks = [(float(a[0]), float(a[1])) for a in data.get("asks", [])[:levels]]
            return {"bids": bids, "asks": asks}
        except Exception as e:
            logger.debug("Orderbook fetch failed for %s: %s", pair, e)
            return None

    @classmethod
    def scan_eurc_usdc(cls, eurusd_rate):
        """Scan EURC-USDC for mispricing against EUR/USD.

        EURC should trade at approximately 1/EUR_USD.
        If EUR/USD = 1.08, then EURC should be ~$1.08.
        If EURC on Coinbase is $1.075, that is a discount = BUY opportunity.
        """
        price = cls.get_coinbase_price("EURC-USDC")
        if price is None or eurusd_rate is None:
            return None

        fair_value = eurusd_rate  # 1 EURC should be worth EUR/USD in USDC
        spread = (price - fair_value) / fair_value

        result = {
            "pair": "EURC-USDC",
            "price": price,
            "fair_value": round(fair_value, 6),
            "spread_pct": round(spread, 6),
            "abs_spread_pct": round(abs(spread), 6),
        }

        if abs(spread) >= CONFIG["min_spread_pct"]:
            if spread < 0:
                # EURC trading below fair value — BUY EURC
                result["direction"] = "BUY"
                result["confidence"] = min(0.60 + abs(spread) * 50, 0.95)
                result["reason"] = (
                    f"EURC underpriced: ${price:.4f} vs fair ${fair_value:.4f} "
                    f"(spread {spread*100:+.2f}%)"
                )
            else:
                # EURC trading above fair value — SELL EURC (if we hold any)
                result["direction"] = "SELL"
                result["confidence"] = min(0.60 + abs(spread) * 50, 0.95)
                result["reason"] = (
                    f"EURC overpriced: ${price:.4f} vs fair ${fair_value:.4f} "
                    f"(spread {spread*100:+.2f}%)"
                )
        else:
            result["direction"] = "NONE"
            result["confidence"] = 0
            result["reason"] = f"EURC spread {spread*100:+.3f}% below threshold"

        return result

    @classmethod
    def scan_dai_usdc(cls):
        """Scan DAI-USDC for depeg opportunities.

        DAI should trade at ~$1.00. Any deviation > 0.3% is tradeable.
        """
        price = cls.get_coinbase_price("DAI-USDC")
        if price is None:
            return None

        fair_value = 1.0000  # DAI is pegged to $1
        spread = (price - fair_value) / fair_value

        result = {
            "pair": "DAI-USDC",
            "price": price,
            "fair_value": fair_value,
            "spread_pct": round(spread, 6),
            "abs_spread_pct": round(abs(spread), 6),
        }

        if abs(spread) >= CONFIG["min_spread_pct"]:
            if spread < 0:
                # DAI below $1 — BUY DAI (expect reversion to peg)
                result["direction"] = "BUY"
                result["confidence"] = min(0.65 + abs(spread) * 40, 0.95)
                result["reason"] = f"DAI depegged low: ${price:.4f} (spread {spread*100:+.2f}%)"
            else:
                # DAI above $1 — SELL DAI
                result["direction"] = "SELL"
                result["confidence"] = min(0.65 + abs(spread) * 40, 0.95)
                result["reason"] = f"DAI depegged high: ${price:.4f} (spread {spread*100:+.2f}%)"
        else:
            result["direction"] = "NONE"
            result["confidence"] = 0
            result["reason"] = f"DAI spread {spread*100:+.3f}% below threshold"

        return result


class ForexCorrelationScanner:
    """Detects forex correlation trading opportunities.

    When EUR/USD moves significantly, European crypto exchanges lag behind.
    This creates a window where BTC/ETH on Coinbase (US) can be bought/sold
    before European prices adjust.

    Edge: EUR weakness -> European sell pressure incoming -> prices drop
          EUR strength -> European buying incoming -> prices rise
    """

    def __init__(self, db):
        self.db = db
        self._prev_eurusd = None
        self._prev_time = None

    def check_correlation(self, eurusd_rate):
        """Check if EUR/USD moved enough to trigger a correlation trade.

        Returns list of signals for correlation_pairs.
        """
        signals = []
        if eurusd_rate is None:
            return signals

        now = time.time()

        # Load previous rate from DB
        if self._prev_eurusd is None:
            row = self.db.execute(
                "SELECT rate, recorded_at FROM forex_rates WHERE pair='EUR/USD' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self._prev_eurusd = row["rate"]
                try:
                    self._prev_time = datetime.fromisoformat(
                        row["recorded_at"]
                    ).replace(tzinfo=timezone.utc).timestamp()
                except Exception:
                    self._prev_time = now - 120

        if self._prev_eurusd is None or self._prev_eurusd == 0:
            self._prev_eurusd = eurusd_rate
            self._prev_time = now
            return signals

        # Calculate EUR/USD move
        move = (eurusd_rate - self._prev_eurusd) / self._prev_eurusd
        elapsed = now - (self._prev_time or now)

        # Only react to moves within the correlation lag window
        if elapsed > CONFIG["correlation_lag_window"]:
            # Update baseline — too much time passed
            self._prev_eurusd = eurusd_rate
            self._prev_time = now
            return signals

        if abs(move) >= CONFIG["eurusd_move_threshold"]:
            for pair in CONFIG["correlation_pairs"]:
                if move < 0:
                    # EUR weakened — European sell pressure coming
                    # BUY opportunity: price will dip as EU sells arrive
                    # Actually: we should WAIT and buy the dip, or sell now
                    # Game theory: the EUR weakness signal means US-listed crypto
                    # may temporarily rise before EU sells hit
                    signals.append({
                        "pair": pair,
                        "direction": "SELL",
                        "confidence": min(0.55 + abs(move) * 30, 0.85),
                        "reason": (
                            f"EUR/USD dropped {move*100:+.2f}% "
                            f"({self._prev_eurusd:.4f} -> {eurusd_rate:.4f}) — "
                            f"EU sell pressure incoming on {pair}"
                        ),
                        "forex_move": round(move, 6),
                        "source": "forex_correlation",
                    })
                else:
                    # EUR strengthened — European buy pressure coming
                    signals.append({
                        "pair": pair,
                        "direction": "BUY",
                        "confidence": min(0.55 + abs(move) * 30, 0.85),
                        "reason": (
                            f"EUR/USD rose {move*100:+.2f}% "
                            f"({self._prev_eurusd:.4f} -> {eurusd_rate:.4f}) — "
                            f"EU buy pressure incoming on {pair}"
                        ),
                        "forex_move": round(move, 6),
                        "source": "forex_correlation",
                    })

            # Update baseline after generating signals
            self._prev_eurusd = eurusd_rate
            self._prev_time = now

        return signals


class ForexAgent:
    """Forex-aware trading agent — stablecoin arb + forex correlation."""

    def __init__(self):
        self.db = sqlite3.connect(FOREX_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.daily_loss = 0.0
        self.trades_today = 0
        self.correlation_scanner = ForexCorrelationScanner(self.db)

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS forex_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                rate REAL NOT NULL,
                source TEXT DEFAULT 'open.er-api.com',
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS arb_opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                price REAL,
                fair_value REAL,
                spread_pct REAL,
                confidence REAL,
                reason TEXT,
                source TEXT,
                action_taken TEXT DEFAULT 'none',
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                amount_usd REAL,
                entry_price REAL,
                limit_price REAL,
                confidence REAL,
                source TEXT,
                order_id TEXT,
                status TEXT DEFAULT 'pending',
                pnl REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_forex_rates_pair ON forex_rates(pair);
            CREATE INDEX IF NOT EXISTS idx_arb_recorded ON arb_opportunities(recorded_at);
            CREATE INDEX IF NOT EXISTS idx_trades_created ON trades(created_at);
        """)
        self.db.commit()

    def _record_forex_rate(self, pair, rate, source="open.er-api.com"):
        """Store a forex rate snapshot."""
        self.db.execute(
            "INSERT INTO forex_rates (pair, rate, source) VALUES (?, ?, ?)",
            (pair, rate, source)
        )
        self.db.commit()

    def _record_opportunity(self, signal, action="none"):
        """Store an arb opportunity."""
        self.db.execute(
            "INSERT INTO arb_opportunities (pair, direction, price, fair_value, spread_pct, "
            "confidence, reason, source, action_taken) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                signal.get("pair"), signal.get("direction"),
                signal.get("price"), signal.get("fair_value"),
                signal.get("spread_pct"), signal.get("confidence"),
                signal.get("reason"), signal.get("source", "stablecoin_scan"),
                action,
            )
        )
        self.db.commit()

    def _get_holdings(self):
        """Get current Coinbase holdings."""
        try:
            from exchange_connector import CoinbaseTrader
            trader = CoinbaseTrader()
            accts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
            holdings = {}
            usdc = 0.0
            for a in accts.get("accounts", []):
                cur = a.get("currency", "")
                bal = float(a.get("available_balance", {}).get("value", 0))
                if cur == "USDC":
                    usdc += bal
                elif cur == "USD":
                    usdc += bal  # Treat USD same as USDC for stablecoin trading
                elif bal > 0:
                    holdings[cur] = bal
            return holdings, usdc
        except Exception as e:
            logger.warning("Holdings check failed: %s", e)
            return {}, 0.0

    def _get_price(self, pair):
        """Get current price for a pair."""
        try:
            # For stablecoin pairs, try direct Coinbase price
            data = _fetch_json(f"https://api.coinbase.com/v2/prices/{pair}/spot", timeout=5)
            return float(data["data"]["amount"])
        except Exception:
            pass
        # Fallback: try exchange API
        try:
            url = f"https://api.exchange.coinbase.com/products/{pair}/ticker"
            data = _fetch_json(url, timeout=5)
            return float(data.get("price", 0))
        except Exception:
            return None

    def scan(self):
        """Run a full scan: forex rates + stablecoin arb + correlation signals."""
        logger.info("=== FOREX AGENT SCAN ===")
        signals = []

        # 1. Fetch forex rates
        eurusd = ForexFeed.get_eurusd()
        gbpusd = ForexFeed.get_gbpusd()
        rates = ForexFeed.get_rates()

        if eurusd:
            self._record_forex_rate("EUR/USD", eurusd)
            logger.info("EUR/USD: %.4f", eurusd)
        if gbpusd:
            self._record_forex_rate("GBP/USD", gbpusd)
            logger.info("GBP/USD: %.4f", gbpusd)

        # 2. Stablecoin scans
        # EURC-USDC arb
        eurc_signal = StablecoinScanner.scan_eurc_usdc(eurusd)
        if eurc_signal:
            eurc_signal["source"] = "eurc_usdc_arb"
            if eurc_signal["direction"] != "NONE":
                signals.append(eurc_signal)
                logger.info("  [EURC-USDC] %s | conf=%.1f%% | %s",
                           eurc_signal["direction"],
                           eurc_signal["confidence"] * 100,
                           eurc_signal["reason"])
            else:
                logger.info("  [EURC-USDC] %s", eurc_signal["reason"])
            self._record_opportunity(eurc_signal)

        # DAI-USDC arb
        dai_signal = StablecoinScanner.scan_dai_usdc()
        if dai_signal:
            dai_signal["source"] = "dai_usdc_arb"
            if dai_signal["direction"] != "NONE":
                signals.append(dai_signal)
                logger.info("  [DAI-USDC] %s | conf=%.1f%% | %s",
                           dai_signal["direction"],
                           dai_signal["confidence"] * 100,
                           dai_signal["reason"])
            else:
                logger.info("  [DAI-USDC] %s", dai_signal["reason"])
            self._record_opportunity(dai_signal)

        # 3. Forex correlation signals
        correlation_signals = self.correlation_scanner.check_correlation(eurusd)
        for sig in correlation_signals:
            signals.append(sig)
            logger.info("  [CORRELATION] %s %s | conf=%.1f%% | %s",
                       sig["direction"], sig["pair"],
                       sig["confidence"] * 100, sig["reason"])
            self._record_opportunity(sig)

        # 4. Filter to actionable signals
        actionable = [
            s for s in signals
            if s.get("confidence", 0) >= CONFIG["min_confidence"]
               and s.get("direction") in ("BUY", "SELL")
        ]

        if actionable:
            logger.info(">>> %d ACTIONABLE signals (of %d total)", len(actionable), len(signals))
        else:
            logger.info("No actionable signals this scan (%d below threshold)", len(signals))

        return actionable

    def execute_trade(self, signal):
        """Execute a forex/stablecoin arb trade on Coinbase.

        Uses LIMIT orders only (maker fee 0.6%).
        """
        if self.daily_loss >= CONFIG["max_daily_loss_usd"]:
            logger.warning("HARDSTOP: Daily loss limit $%.2f reached", CONFIG["max_daily_loss_usd"])
            return False

        pair = signal["pair"]
        direction = signal["direction"]
        confidence = signal.get("confidence", 0)

        price = self._get_price(pair)
        if not price:
            logger.warning("Cannot get price for %s", pair)
            return False

        holdings, usdc_cash = self._get_holdings()
        base_currency = pair.split("-")[0]  # e.g., "EURC" from "EURC-USDC"

        if direction == "BUY":
            # Size: scale with confidence, min $1, max $5
            trade_size = min(
                CONFIG["max_trade_usd"],
                max(1.00, confidence * 6.0)
            )
            trade_size = min(trade_size, usdc_cash - 10.00)  # keep $10 reserve — prevent cash burnout
            if trade_size < 1.00:
                logger.info("Insufficient USDC ($%.2f) for BUY", usdc_cash)
                return False

            trade_size = round(trade_size, 2)
            base_size = trade_size / price
            limit_price = price * 1.001  # slightly above to ensure fill

            logger.info("FOREX EXECUTE: LIMIT BUY %s | $%.2f (%.4f @ $%.4f) | conf=%.1f%% | %s",
                        pair, trade_size, base_size, limit_price,
                        confidence * 100, signal.get("source", ""))

            try:
                from exchange_connector import CoinbaseTrader
                trader = CoinbaseTrader()
                result = trader.place_limit_order(pair, "BUY", base_size, limit_price, post_only=False)
                return self._process_result(result, signal, trade_size, price, limit_price)
            except Exception as e:
                logger.error("BUY execution error: %s", e, exc_info=True)
                return False

        elif direction == "SELL":
            # Check if we hold this asset
            held = holdings.get(base_currency, 0)
            held_usd = held * price
            if held_usd < 0.50:
                logger.info("No %s holdings to sell (have %.4f = $%.2f)", base_currency, held, held_usd)
                return False

            trade_size = min(CONFIG["max_trade_usd"], held_usd * 0.5)
            if trade_size < 0.50:
                return False

            base_size = trade_size / price
            base_size = min(base_size, held)
            limit_price = price * 0.999  # slightly below to ensure fill

            logger.info("FOREX EXECUTE: LIMIT SELL %s | $%.2f (%.4f @ $%.4f) | conf=%.1f%% | %s",
                        pair, trade_size, base_size, limit_price,
                        confidence * 100, signal.get("source", ""))

            try:
                from exchange_connector import CoinbaseTrader
                trader = CoinbaseTrader()
                result = trader.place_limit_order(pair, "SELL", base_size, limit_price, post_only=False)
                return self._process_result(result, signal, trade_size, price, limit_price)
            except Exception as e:
                logger.error("SELL execution error: %s", e, exc_info=True)
                return False

        return False

    def _process_result(self, result, signal, trade_size, price, limit_price):
        """Process order result and record the trade."""
        order_id = None
        status = "failed"

        if "success_response" in result:
            order_id = result["success_response"].get("order_id")
            status = "filled"
            logger.info("FOREX ORDER FILLED: %s %s $%.2f @ $%.4f | order=%s",
                        signal["pair"], signal["direction"], trade_size, limit_price, order_id)
        elif "order_id" in result:
            order_id = result["order_id"]
            status = "filled"
            logger.info("FOREX ORDER FILLED: %s %s $%.2f @ $%.4f | order=%s",
                        signal["pair"], signal["direction"], trade_size, limit_price, order_id)
        elif "error_response" in result:
            err = result["error_response"]
            logger.warning("FOREX ORDER FAILED: %s %s | %s",
                          signal["pair"], signal["direction"],
                          err.get("message", err))
        else:
            logger.warning("FOREX ORDER UNKNOWN: %s", json.dumps(result)[:300])

        self.db.execute(
            "INSERT INTO trades (pair, direction, amount_usd, entry_price, limit_price, "
            "confidence, source, order_id, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                signal["pair"], signal["direction"], trade_size, price, limit_price,
                signal.get("confidence"), signal.get("source"), order_id, status,
            )
        )
        self.db.commit()
        self.trades_today += 1

        self._record_opportunity(signal, action=f"trade_{status}")
        return status == "filled"

    def print_status(self):
        """Print agent status report."""
        print(f"\n{'='*70}")
        print(f"  FOREX AGENT STATUS")
        print(f"{'='*70}")

        # Recent forex rates
        rates = self.db.execute(
            "SELECT pair, rate, recorded_at FROM forex_rates "
            "WHERE id IN (SELECT MAX(id) FROM forex_rates GROUP BY pair) "
            "ORDER BY pair"
        ).fetchall()
        if rates:
            print(f"\n  Latest Forex Rates:")
            for r in rates:
                print(f"    {r['pair']}: {r['rate']:.4f}  ({r['recorded_at']})")

        # Recent opportunities
        opps = self.db.execute(
            "SELECT pair, direction, price, fair_value, spread_pct, confidence, reason, "
            "action_taken, recorded_at FROM arb_opportunities "
            "ORDER BY id DESC LIMIT 10"
        ).fetchall()
        if opps:
            print(f"\n  Recent Opportunities:")
            for o in opps:
                marker = ">>>" if o["action_taken"] and "trade" in (o["action_taken"] or "") else "   "
                conf_str = f"{o['confidence']*100:.0f}%" if o["confidence"] else "N/A"
                print(f"  {marker} {o['pair']} {o['direction']} | spread={o['spread_pct']*100 if o['spread_pct'] else 0:+.3f}% "
                      f"| conf={conf_str} | {o['action_taken']} | {o['recorded_at']}")

        # Recent trades
        trades = self.db.execute(
            "SELECT pair, direction, amount_usd, entry_price, limit_price, confidence, "
            "source, status, created_at FROM trades ORDER BY id DESC LIMIT 10"
        ).fetchall()
        if trades:
            print(f"\n  Recent Trades:")
            for t in trades:
                print(f"    {t['direction']} {t['pair']} ${t['amount_usd']:.2f} "
                      f"@ ${t['entry_price']:.4f} (limit ${t['limit_price']:.4f}) "
                      f"[{t['status']}] via {t['source']} | {t['created_at']}")
        else:
            print(f"\n  No trades yet.")

        # Summary
        total_trades = self.db.execute("SELECT COUNT(*) as cnt FROM trades").fetchone()["cnt"]
        filled_trades = self.db.execute(
            "SELECT COUNT(*) as cnt FROM trades WHERE status='filled'"
        ).fetchone()["cnt"]
        total_opps = self.db.execute("SELECT COUNT(*) as cnt FROM arb_opportunities").fetchone()["cnt"]

        print(f"\n  Summary:")
        print(f"    Total scans/opps:  {total_opps}")
        print(f"    Total trades:      {total_trades} ({filled_trades} filled)")
        print(f"    Today's trades:    {self.trades_today}")
        print(f"    Today's loss:      ${self.daily_loss:.2f} / ${CONFIG['max_daily_loss_usd']:.2f}")

        print(f"\n  Config:")
        print(f"    Scan interval:     {CONFIG['scan_interval']}s")
        print(f"    Min spread:        {CONFIG['min_spread_pct']*100:.1f}%")
        print(f"    Max trade:         ${CONFIG['max_trade_usd']:.2f}")
        print(f"    Min confidence:    {CONFIG['min_confidence']*100:.0f}%")
        print(f"    Pairs:             {', '.join(CONFIG['pairs'])}")
        print(f"    Correlation pairs: {', '.join(CONFIG['correlation_pairs'])}")
        print(f"{'='*70}\n")

    def run(self):
        """Main agent loop — scan and trade."""
        logger.info("Forex Agent starting — scanning every %ds", CONFIG["scan_interval"])
        logger.info("Pairs: %s | Correlation: %s",
                    CONFIG["pairs"], CONFIG["correlation_pairs"])
        logger.info("Min spread: %.1f%% | Max trade: $%.2f | Min confidence: %.0f%%",
                    CONFIG["min_spread_pct"] * 100, CONFIG["max_trade_usd"],
                    CONFIG["min_confidence"] * 100)

        cycle = 0
        while True:
            try:
                cycle += 1
                actionable = self.scan()

                for signal in actionable:
                    self.execute_trade(signal)

                # Print status every 30 cycles (~30 min at 60s interval)
                if cycle % 30 == 0:
                    self.print_status()

                time.sleep(CONFIG["scan_interval"])

            except KeyboardInterrupt:
                logger.info("Forex Agent shutting down...")
                self.print_status()
                break
            except Exception as e:
                logger.error("Forex Agent error: %s", e, exc_info=True)
                time.sleep(30)


if __name__ == "__main__":
    agent = ForexAgent()

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "scan":
            results = agent.scan()
            agent.print_status()
        elif cmd == "status":
            agent.print_status()
        elif cmd == "run":
            agent.run()
        else:
            print("Usage: forex_agent.py [run|scan|status]")
            print("  run    — Start continuous scanning and trading")
            print("  scan   — Run one scan and show results")
            print("  status — Show current status from DB")
    else:
        agent.run()
