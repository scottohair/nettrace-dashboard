#!/usr/bin/env python3
"""Cross-Exchange Arbitrage Scanner — finds price discrepancies and executes on Coinbase.

Strategy: Monitor real-time prices from multiple public APIs (free, no auth).
When Coinbase price diverges from the market consensus by more than fees,
execute the profitable side.

RULE #1: NEVER LOSE MONEY.
- Only trade when spread > (fees + buffer)
- Coinbase taker fee: ~0.60% (varies by volume)
- Minimum profit threshold: 0.8% spread (covers fees + slippage)
- Validate prices from 3+ sources before acting
- Position size: max $5 per trade

Supported exchanges (public APIs, no auth):
- Coinbase (primary - where we trade)
- Binance (largest volume, best reference)
- Kraken (reliable, good for BTC/ETH)
- KuCoin (high liquidity alts)
- OKX (derivatives reference)
"""

import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "arb_scanner.log")),
    ]
)
logger = logging.getLogger("arb_scanner")

# Risk parameters — NEVER LOSE MONEY
COINBASE_MAKER_FEE_PCT = 0.004  # 0.40% maker fee (limit orders)
COINBASE_TAKER_FEE_PCT = 0.006  # 0.60% taker fee (market orders)
MIN_SPREAD_PCT = 0.012          # 1.2% minimum spread (covers fees + slippage + profit)
SAFE_SPREAD_PCT = 0.010         # 1.0% = high confidence, scale up size
MIN_TRADE_USD = 1.00            # Coinbase minimum
MIN_SOURCES = 3                 # Need 3+ price sources to validate
SCAN_INTERVAL = 10              # Check every 10 seconds (faster = more opportunities)
COOLDOWN_SECONDS = 60           # 1 min between trades on same pair (was 2 min)

# Dynamic risk controller — arb_scanner MUST check risk before trading
try:
    from risk_controller import get_controller as _get_rc_arb
    _arb_risk_ctrl = _get_rc_arb()
except Exception:
    _arb_risk_ctrl = None
    logging.getLogger("arb_scanner").error("Risk controller failed to load — arb trades will be BLOCKED")

# Pairs we can trade on Coinbase — expanded set
PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD", "DOGE-USD"]

# Track last trade time per pair
last_trade = {}

# Database for trade recording
ARB_DB = str(Path(__file__).parent / "arb_scanner.db")

def _init_arb_db():
    """Initialize arb scanner database."""
    import sqlite3
    db = sqlite3.connect(ARB_DB)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=5000")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS arb_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            side TEXT NOT NULL,
            amount_usd REAL,
            coinbase_price REAL,
            market_median REAL,
            spread_pct REAL,
            expected_profit_pct REAL,
            confidence REAL,
            sources INTEGER,
            status TEXT DEFAULT 'pending',
            order_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS arb_opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            side TEXT NOT NULL,
            spread_pct REAL,
            expected_profit_pct REAL,
            sources INTEGER,
            acted_on INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    db.commit()
    return db

_arb_db = _init_arb_db()


def fetch_coinbase_price(pair):
    """Coinbase public spot price."""
    try:
        base, quote = pair.split("-")
        url = f"https://api.coinbase.com/v2/prices/{base}-{quote}/spot"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-Arb/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        return float(data["data"]["amount"])
    except Exception:
        return None


def fetch_binance_price(pair):
    """Binance public ticker."""
    try:
        symbol = pair.replace("-", "").replace("USD", "USDT")
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-Arb/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        return float(data["price"])
    except Exception:
        return None


def fetch_kraken_price(pair):
    """Kraken public ticker."""
    kraken_map = {
        "BTC-USD": "XXBTZUSD",
        "ETH-USD": "XETHZUSD",
        "SOL-USD": "SOLUSD",
    }
    try:
        k_pair = kraken_map.get(pair)
        if not k_pair:
            return None
        url = f"https://api.kraken.com/0/public/Ticker?pair={k_pair}"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-Arb/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        result = data.get("result", {})
        for key, val in result.items():
            return float(val["c"][0])  # last trade price
    except Exception:
        return None


def fetch_kucoin_price(pair):
    """KuCoin public ticker."""
    try:
        symbol = pair.replace("-USD", "-USDT")
        url = f"https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={symbol}"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-Arb/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        return float(data["data"]["price"])
    except Exception:
        return None


def fetch_okx_price(pair):
    """OKX public ticker."""
    try:
        inst_id = pair.replace("-USD", "-USDT")
        url = f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-Arb/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
        return float(data["data"][0]["last"])
    except Exception:
        return None


def get_all_prices(pair):
    """Fetch prices from all exchanges. Returns dict of {exchange: price}."""
    fetchers = {
        "coinbase": fetch_coinbase_price,
        "binance": fetch_binance_price,
        "kraken": fetch_kraken_price,
        "kucoin": fetch_kucoin_price,
        "okx": fetch_okx_price,
    }
    prices = {}
    for name, fetch in fetchers.items():
        p = fetch(pair)
        if p and p > 0:
            prices[name] = p
    return prices


def find_arbitrage(pair, prices):
    """Analyze prices for arbitrage opportunity.

    Returns dict with opportunity details or None if no opportunity.
    Only signals when Coinbase price diverges from market consensus.
    """
    if len(prices) < MIN_SOURCES:
        return None

    cb_price = prices.get("coinbase")
    if not cb_price:
        return None

    # Calculate market consensus (median of non-Coinbase prices)
    other_prices = [p for ex, p in prices.items() if ex != "coinbase"]
    if len(other_prices) < 2:
        return None

    other_prices.sort()
    if len(other_prices) % 2 == 0:
        median = (other_prices[len(other_prices)//2 - 1] + other_prices[len(other_prices)//2]) / 2
    else:
        median = other_prices[len(other_prices)//2]

    # Calculate spread: positive = CB is expensive (sell opportunity)
    #                    negative = CB is cheap (buy opportunity)
    spread = (cb_price - median) / median

    # Check if spread exceeds our minimum threshold
    abs_spread = abs(spread)
    if abs_spread < MIN_SPREAD_PCT:
        return None

    # Validate: all non-CB prices should roughly agree (within 0.3%)
    price_range = (max(other_prices) - min(other_prices)) / median
    if price_range > 0.003:  # Other exchanges disagree too much
        return None

    # Determine direction — use maker fee (limit orders)
    if spread > 0:
        # Coinbase is expensive — SELL on Coinbase (sell high)
        side = "SELL"
        expected_profit_pct = abs_spread - COINBASE_MAKER_FEE_PCT
    else:
        # Coinbase is cheap — BUY on Coinbase (buy low)
        side = "BUY"
        expected_profit_pct = abs_spread - COINBASE_MAKER_FEE_PCT

    if expected_profit_pct <= 0:
        return None  # Not profitable after fees

    # Confidence: how many sources agree and how wide the spread
    confidence = min(1.0, (abs_spread / SAFE_SPREAD_PCT) * (len(prices) / 5))

    return {
        "pair": pair,
        "side": side,
        "coinbase_price": cb_price,
        "market_median": median,
        "spread_pct": round(spread * 100, 4),
        "expected_profit_pct": round(expected_profit_pct * 100, 4),
        "confidence": round(confidence, 3),
        "sources": len(prices),
        "prices": prices,
    }


def execute_arb(opportunity):
    """Execute an arbitrage trade on Coinbase using LIMIT orders (maker fee 0.4%).

    Deterministic: spread > fees = trade. No confidence gate.
    Uses limit orders at the spread edge for best execution.
    ALL trades go through risk_controller.approve_trade() first.
    """
    from exchange_connector import CoinbaseTrader

    # Block trades if risk controller unavailable
    if not _arb_risk_ctrl:
        logger.error("ARB: Risk controller unavailable — BLOCKING trade")
        return False

    pair = opportunity["pair"]
    side = opportunity["side"]
    now = time.time()

    # Cooldown check
    if pair in last_trade and now - last_trade[pair] < COOLDOWN_SECONDS:
        logger.debug("Cooldown active for %s", pair)
        return False

    # Get dynamic max trade from risk controller
    # Estimate portfolio from risk controller (arb_scanner doesn't track portfolio directly)
    portfolio_est = 290  # will be overridden by risk_controller's own check
    risk_params = _arb_risk_ctrl.get_risk_params(portfolio_est, pair.replace("-USD", "-USDC"))
    max_trade_usd = risk_params["max_trade_usd"]

    # Size scales with spread magnitude: bigger spread = more confident = bigger trade
    spread_mult = min(2.0, abs(opportunity["spread_pct"]) / (SAFE_SPREAD_PCT * 100))
    trade_usd = min(max_trade_usd, max(MIN_TRADE_USD, MIN_TRADE_USD + (max_trade_usd - MIN_TRADE_USD) * spread_mult))

    # Risk controller approval — prevents exceeding combined limits across agents
    approved, reason, adj_size = _arb_risk_ctrl.approve_trade(
        "arb_scanner", pair, side, trade_usd, portfolio_est)
    if not approved:
        logger.info("ARB: Risk controller blocked: %s", reason)
        return False
    trade_usd = adj_size

    logger.info("ARB EXECUTE: %s %s | spread=%.4f%% | profit=%.4f%% | $%.2f",
                side, pair, opportunity["spread_pct"], opportunity["expected_profit_pct"], trade_usd)

    trader = CoinbaseTrader()
    cb_price = opportunity["coinbase_price"]

    if side == "BUY":
        # Place limit BUY slightly below Coinbase spot (capture the discount)
        limit_price = cb_price * 0.999  # 0.1% below spot
        base_size = trade_usd / limit_price
        result = trader.place_limit_order(pair, "BUY", base_size, limit_price, post_only=True)
        if result.get("error_response"):
            # Fallback to market if post_only rejected (price moved)
            logger.info("Limit rejected, using market order for %s BUY", pair)
            result = trader.place_order(pair, "BUY", round(trade_usd, 2))
    else:
        # For SELL, check we hold the asset first
        accounts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
        base = pair.split("-")[0]
        held = 0
        for acc in accounts.get("accounts", []):
            if acc.get("currency") == base:
                held = float(acc.get("available_balance", {}).get("value", 0))
                break

        if held <= 0:
            logger.warning("No %s to sell for arb", base)
            return False

        # Sell up to trade_usd worth at a premium (limit order above spot)
        sell_amount = min(held, trade_usd / cb_price)
        limit_price = cb_price * 1.001  # 0.1% above spot
        result = trader.place_limit_order(pair, "SELL", sell_amount, limit_price, post_only=True)
        if result.get("error_response"):
            logger.info("Limit rejected, using market order for %s SELL", pair)
            result = trader.place_order(pair, "SELL", sell_amount)

    success = "success_response" in result or ("order_id" in result and "error" not in result)
    order_id = result.get("success_response", result).get("order_id", "?") if success else None
    status = "filled" if "success_response" in result else ("pending" if success else "failed")

    # Record trade in DB
    try:
        _arb_db.execute(
            """INSERT INTO arb_trades
               (pair, side, amount_usd, coinbase_price, market_median, spread_pct,
                expected_profit_pct, confidence, sources, status, order_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (pair, side, trade_usd, opportunity["coinbase_price"],
             opportunity["market_median"], opportunity["spread_pct"],
             opportunity["expected_profit_pct"], opportunity.get("confidence", 0),
             opportunity.get("sources", 0), status, order_id)
        )
        _arb_db.commit()
    except Exception as e:
        logger.debug("DB record failed: %s", e)

    if success:
        logger.info("ARB EXECUTED: %s %s $%.2f | order=%s [%s]", side, pair, trade_usd, order_id, status)
        last_trade[pair] = now
        return True
    else:
        err = result.get("error_response", result).get("message", str(result)[:200])
        logger.warning("ARB FAILED: %s %s | %s", side, pair, err)
        return False


def scan_loop():
    """Main arbitrage scanning loop."""
    logger.info("Arbitrage Scanner starting | Pairs: %s | Min spread: %.2f%%",
                PAIRS, MIN_SPREAD_PCT * 100)

    cycle = 0
    opportunities_found = 0
    trades_executed = 0

    while True:
        try:
            cycle += 1

            for pair in PAIRS:
                prices = get_all_prices(pair)

                if len(prices) >= MIN_SOURCES:
                    opp = find_arbitrage(pair, prices)

                    if opp:
                        opportunities_found += 1
                        logger.info("FOUND: %s %s spread=%.4f%% profit=%.4f%% sources=%d",
                                   opp["side"], pair, opp["spread_pct"],
                                   opp["expected_profit_pct"], opp["sources"])
                        try:
                            _arb_db.execute(
                                "INSERT INTO arb_opportunities (pair, side, spread_pct, expected_profit_pct, sources) VALUES (?, ?, ?, ?, ?)",
                                (pair, opp["side"], opp["spread_pct"], opp["expected_profit_pct"], opp["sources"])
                            )
                            _arb_db.commit()
                        except Exception:
                            pass

                        # Arb is DETERMINISTIC — spread > fees = execute
                        # No confidence gate needed; find_arbitrage already
                        # validates spread > fees + buffer and 3+ sources agree
                        if execute_arb(opp):
                            trades_executed += 1

                    elif cycle % 20 == 0:  # Log price comparison every ~5 min
                        cb = prices.get("coinbase", 0)
                        others = [p for ex, p in prices.items() if ex != "coinbase"]
                        if others and cb:
                            median = sorted(others)[len(others)//2]
                            spread = ((cb - median) / median) * 100
                            logger.debug("%s: CB=$%.2f median=$%.2f spread=%.4f%% sources=%d",
                                       pair, cb, median, spread, len(prices))

                time.sleep(1)  # Stagger between pairs

            # Status log every 40 cycles (~10 min)
            if cycle % 40 == 0:
                logger.info("Status: cycle=%d opportunities=%d trades=%d",
                           cycle, opportunities_found, trades_executed)

            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Shutting down. opportunities=%d trades=%d",
                       opportunities_found, trades_executed)
            break
        except Exception as e:
            logger.error("Error: %s", e, exc_info=True)
            time.sleep(30)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "check":
        # One-shot price check
        print("=== Cross-Exchange Price Check ===\n")
        for pair in PAIRS:
            prices = get_all_prices(pair)
            print(f"{pair}:")
            for ex, p in sorted(prices.items()):
                print(f"  {ex:<12} ${p:,.2f}")
            if len(prices) >= 2:
                vals = list(prices.values())
                spread = ((max(vals) - min(vals)) / min(vals)) * 100
                print(f"  spread: {spread:.4f}%")
                opp = find_arbitrage(pair, prices)
                if opp:
                    print(f"  >>> OPPORTUNITY: {opp['side']} | profit={opp['expected_profit_pct']}% | conf={opp['confidence']}")
                else:
                    print(f"  (no opportunity)")
            print()
    else:
        scan_loop()
