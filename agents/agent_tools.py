#!/usr/bin/env python3
"""Shared tool registry for all trading agents.

Every agent imports AgentTools for market data, trading, portfolio,
risk management, and communication. Single source of truth.

Usage:
    from agent_tools import AgentTools
    tools = AgentTools()
    price = tools.get_price("BTC-USD")
    tools.place_limit_buy("BTC-USD", price * 0.995, 0.0001)
"""

import json
import logging
import os
import sqlite3
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger("agent_tools")

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

TRADER_DB = str(Path(__file__).parent / "trader.db")
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")
FLY_URL = "https://nettrace-dashboard.fly.dev"

# Adaptive risk — scales with portfolio size automatically
# These are FLOORS, the adaptive engine scales up from here
BASE_MAX_TRADE_USD = 5.00
BASE_DAILY_LOSS_USD = 2.00
MAX_EXPOSURE_PCT = 0.80  # never have more than 80% in positions


class AdaptiveRisk:
    """Dynamic risk management that scales with portfolio.

    Instead of static $5 max trade / $2 daily loss:
      $13 portfolio  → $1.30 max trade (10%), $0.65 daily loss (5%)
      $100 portfolio → $10 max trade, $5 daily loss
      $1000 portfolio → $50 max trade, $20 daily loss
      $10000 portfolio → $200 max trade, $100 daily loss

    The system automatically grows more aggressive as we make money,
    and more conservative as we lose it. This is Kelly Criterion behavior.
    """

    # What % of portfolio we risk per trade and per day
    MAX_TRADE_PCT = 0.10       # 10% of portfolio per trade
    MAX_DAILY_LOSS_PCT = 0.05  # 5% daily stop-loss
    MIN_RESERVE_PCT = 0.15     # always keep 15% liquid
    REINVEST_PCT = 0.25        # reinvest 25% of profits

    # Absolute floors — never go below these
    ABS_MIN_TRADE = 1.00       # Coinbase minimum ~$1
    ABS_MIN_RESERVE = 1.50     # absolute minimum cash

    # Win streak multiplier — when on a streak, size up (Kelly)
    STREAK_BONUS = 0.05        # +5% per consecutive win, capped at +25%
    MAX_STREAK_BONUS = 0.25

    def __init__(self, portfolio_value=13.47):
        self.portfolio_value = portfolio_value
        self.win_streak = 0
        self.loss_streak = 0
        self._last_refresh = 0

    def refresh(self, portfolio_value):
        """Update portfolio value for risk calculations."""
        self.portfolio_value = portfolio_value
        self._last_refresh = time.time()

    def record_win(self):
        self.win_streak += 1
        self.loss_streak = 0

    def record_loss(self):
        self.loss_streak += 1
        self.win_streak = 0

    @property
    def streak_multiplier(self):
        """Kelly-inspired: size up on wins, size down on losses."""
        if self.win_streak > 0:
            bonus = min(self.win_streak * self.STREAK_BONUS, self.MAX_STREAK_BONUS)
            return 1.0 + bonus
        elif self.loss_streak > 0:
            # Halve size after each loss
            return max(0.25, 1.0 / (1 + self.loss_streak * 0.5))
        return 1.0

    @property
    def max_trade_usd(self):
        """Maximum USD per trade — scales with portfolio."""
        base = self.portfolio_value * self.MAX_TRADE_PCT * self.streak_multiplier
        return max(self.ABS_MIN_TRADE, min(base, self.portfolio_value * 0.20))  # cap at 20%

    @property
    def max_daily_loss(self):
        """Maximum daily loss before HARDSTOP — scales with portfolio."""
        return max(1.00, self.portfolio_value * self.MAX_DAILY_LOSS_PCT)

    @property
    def min_reserve(self):
        """Minimum cash reserve — scales with portfolio."""
        return max(self.ABS_MIN_RESERVE, self.portfolio_value * self.MIN_RESERVE_PCT)

    @property
    def optimal_grid_size(self):
        """Calculate optimal grid order size based on portfolio."""
        # Deploy up to 60% of portfolio across grid levels
        deployable = self.portfolio_value * 0.60
        # Aim for 4-6 grid levels
        levels = min(6, max(2, int(deployable / self.ABS_MIN_TRADE)))
        return max(self.ABS_MIN_TRADE, deployable / levels) if levels > 0 else self.ABS_MIN_TRADE

    @property
    def optimal_grid_levels(self):
        """How many grid levels we can afford."""
        deployable = self.portfolio_value * 0.60
        return min(6, max(2, int(deployable / max(1, self.optimal_grid_size))))

    @property
    def optimal_dca_daily(self):
        """Daily DCA budget — scales with portfolio."""
        # 2-5% of portfolio per day for DCA
        return max(0.30, min(self.portfolio_value * 0.03, 100.0))

    def to_dict(self):
        return {
            "portfolio": round(self.portfolio_value, 2),
            "max_trade": round(self.max_trade_usd, 2),
            "max_daily_loss": round(self.max_daily_loss, 2),
            "min_reserve": round(self.min_reserve, 2),
            "grid_size": round(self.optimal_grid_size, 2),
            "grid_levels": self.optimal_grid_levels,
            "dca_daily": round(self.optimal_dca_daily, 2),
            "streak_mult": round(self.streak_multiplier, 2),
            "win_streak": self.win_streak,
            "loss_streak": self.loss_streak,
        }


class AgentTools:
    """Shared tools for all trading agents with adaptive risk management."""

    def __init__(self):
        import sys
        sys.path.insert(0, str(Path(__file__).parent))
        from exchange_connector import CoinbaseTrader, PriceFeed
        self.exchange = CoinbaseTrader()
        self.pricefeed = PriceFeed
        self._db = None
        self._daily_loss = 0.0
        self._daily_reset = datetime.now(timezone.utc).date()
        self.risk = AdaptiveRisk()

    @property
    def db(self):
        if self._db is None:
            self._db = sqlite3.connect(TRADER_DB)
            self._db.row_factory = sqlite3.Row
            self._db.execute("""CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_value_usd REAL,
                available_cash REAL,
                held_in_orders REAL,
                holdings_json TEXT,
                agent TEXT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
            self._db.execute("""CREATE TABLE IF NOT EXISTS agent_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent TEXT NOT NULL,
                pair TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL,
                quantity REAL,
                total_usd REAL,
                order_type TEXT DEFAULT 'limit',
                order_id TEXT,
                status TEXT DEFAULT 'pending',
                pnl REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
            self._db.commit()
        return self._db

    # ── Market Data ──────────────────────────────────────────────

    def get_price(self, pair):
        """Get current spot price."""
        return self.pricefeed.get_price(pair)

    def get_prices(self, pairs):
        """Get prices for multiple pairs."""
        return {p: self.get_price(p) for p in pairs}

    def get_candles(self, pair, granularity=300, hours=24):
        """Get historical candles. granularity in seconds (300=5min, 3600=1h)."""
        candles = []
        end = int(time.time())
        start = end - (hours * 3600)
        gran = int(granularity)
        chunk_seconds = 300 * gran

        current_start = start
        while current_start < end:
            current_end = min(current_start + chunk_seconds, end)
            url = (f"https://api.exchange.coinbase.com/products/{pair}/candles"
                   f"?start={datetime.fromtimestamp(current_start, tz=timezone.utc).isoformat()}"
                   f"&end={datetime.fromtimestamp(current_end, tz=timezone.utc).isoformat()}"
                   f"&granularity={gran}")
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode())
                for c in data:
                    candles.append({
                        "time": int(c[0]), "low": float(c[1]), "high": float(c[2]),
                        "open": float(c[3]), "close": float(c[4]), "volume": float(c[5]),
                    })
            except Exception as e:
                logger.debug("Candle fetch %s: %s", pair, e)
                break
            current_start = current_end
            time.sleep(0.3)

        candles.sort(key=lambda x: x["time"])
        seen = set()
        return [c for c in candles if c["time"] not in seen and not seen.add(c["time"])]

    def get_order_book(self, pair, limit=10):
        """Get order book for spread analysis."""
        path = f"/api/v3/brokerage/product_book?product_id={pair}&limit={limit}"
        return self.exchange._request("GET", path)

    def get_spread(self, pair):
        """Get current bid-ask spread as a percentage."""
        book = self.get_order_book(pair, limit=1)
        pricebook = book.get("pricebook", {})
        bids = pricebook.get("bids", [])
        asks = pricebook.get("asks", [])
        if not bids or not asks:
            return None
        best_bid = float(bids[0].get("price", 0))
        best_ask = float(asks[0].get("price", 0))
        if best_bid <= 0:
            return None
        mid = (best_bid + best_ask) / 2
        return (best_ask - best_bid) / mid

    # ── Trading ──────────────────────────────────────────────────

    def refresh_risk(self):
        """Refresh adaptive risk with current portfolio value."""
        try:
            portfolio = self.get_portfolio()
            self.risk.refresh(portfolio["total_usd"])
        except Exception:
            pass

    def place_limit_buy(self, pair, price, base_size, agent_name="unknown"):
        """Place a limit BUY order (maker, 0.4% fee). Returns order_id or None."""
        if not self.can_trade():
            logger.warning("[%s] Trading blocked — daily loss limit hit", agent_name)
            return None

        usd_cost = base_size * price
        max_trade = self.risk.max_trade_usd
        if usd_cost > max_trade:
            logger.warning("[%s] Order $%.2f exceeds adaptive max $%.2f (portfolio $%.2f)",
                          agent_name, usd_cost, max_trade, self.risk.portfolio_value)
            return None

        result = self.exchange.place_limit_order(pair, "BUY", base_size, price, post_only=True)
        order_id = self._extract_order_id(result)

        self.log_trade(agent_name, pair, "BUY", price, base_size, usd_cost, "limit", order_id,
                       "placed" if order_id else "failed")
        return order_id

    def place_limit_sell(self, pair, price, base_size, agent_name="unknown"):
        """Place a limit SELL order (maker, 0.4% fee). Returns order_id or None."""
        if not self.can_trade():
            return None

        result = self.exchange.place_limit_order(pair, "SELL", base_size, price, post_only=True)
        order_id = self._extract_order_id(result)

        usd_value = base_size * price
        self.log_trade(agent_name, pair, "SELL", price, base_size, usd_value, "limit", order_id,
                       "placed" if order_id else "failed")
        return order_id

    def cancel_order(self, order_id):
        """Cancel an open order. Returns True on success."""
        result = self.exchange.cancel_order(order_id)
        results = result.get("results", [])
        if results and results[0].get("success"):
            return True
        return False

    def get_open_orders(self, pair=None):
        """Get all open orders, optionally filtered by pair."""
        result = self.exchange.get_orders(product_id=pair, status="OPEN")
        return result.get("orders", [])

    def cancel_all_orders(self, pair=None):
        """Cancel all open orders for a pair (or all pairs)."""
        orders = self.get_open_orders(pair)
        cancelled = 0
        for order in orders:
            oid = order.get("order_id")
            if oid and self.cancel_order(oid):
                cancelled += 1
        return cancelled

    def _extract_order_id(self, result):
        """Extract order_id from Coinbase response."""
        if "success_response" in result:
            return result["success_response"].get("order_id")
        if "order_id" in result:
            return result["order_id"]
        return None

    # ── Portfolio ─────────────────────────────────────────────────

    def get_portfolio(self):
        """Get full portfolio including funds locked in open orders."""
        accounts = self.exchange._request("GET", "/api/v3/brokerage/accounts?limit=250")
        if "accounts" not in accounts:
            return {"total_usd": 0, "holdings": {}, "available_cash": 0, "held_in_orders": 0}

        total_usd = 0
        holdings = {}
        available_cash = 0
        held_in_orders = 0

        for acc in accounts["accounts"]:
            bal = acc.get("available_balance", {})
            hold = acc.get("hold", {})
            amount = float(bal.get("value", 0))
            held = float(hold.get("value", 0))
            total_amount = amount + held
            currency = acc.get("currency", "")
            if total_amount <= 0:
                continue

            if currency in ("USD", "USDC"):
                usd_value = total_amount
                available_cash += amount
                held_in_orders += held
            else:
                price = self.pricefeed.get_price(f"{currency}-USD")
                usd_value = total_amount * price if price else 0
                if held > 0 and price:
                    held_in_orders += held * price

            if usd_value > 0.01:
                holdings[currency] = {
                    "amount": total_amount,
                    "available": amount,
                    "held": held,
                    "usd_value": round(usd_value, 2),
                }
                total_usd += usd_value

        return {
            "total_usd": round(total_usd, 2),
            "holdings": holdings,
            "available_cash": round(available_cash, 2),
            "held_in_orders": round(held_in_orders, 2),
        }

    def get_available_cash(self):
        """Get available USD/USDC not locked in orders."""
        portfolio = self.get_portfolio()
        return portfolio["available_cash"]

    def get_holdings(self):
        """Get all non-zero holdings."""
        portfolio = self.get_portfolio()
        return portfolio["holdings"]

    # ── Risk Management ──────────────────────────────────────────

    def check_daily_loss(self):
        """Check today's P&L. Returns negative number if losing."""
        today = datetime.now(timezone.utc).date()
        if today != self._daily_reset:
            self._daily_loss = 0.0
            self._daily_reset = today
        return self._daily_loss

    def record_pnl(self, pnl_amount):
        """Record a P&L event. Updates daily loss tracker."""
        today = datetime.now(timezone.utc).date()
        if today != self._daily_reset:
            self._daily_loss = 0.0
            self._daily_reset = today
        self._daily_loss += pnl_amount

    def can_trade(self):
        """Check if trading is allowed (adaptive daily loss limit not hit)."""
        return self.check_daily_loss() > -self.risk.max_daily_loss

    def max_position_size(self, pair):
        """Maximum USD to deploy on a single position (adaptive)."""
        cash = self.get_available_cash()
        max_from_cash = cash - self.risk.min_reserve
        return min(self.risk.max_trade_usd, max(0, max_from_cash))

    # ── Analysis (C Engine) ──────────────────────────────────────

    def compute_indicators(self, pair):
        """Compute technical indicators using C engine (or Python fallback)."""
        candles = self.get_candles(pair, granularity=300, hours=24)
        if len(candles) < 50:
            return {"regime": "UNKNOWN", "error": "insufficient_data"}

        try:
            from fast_bridge import FastEngine
            engine = FastEngine()
            return engine.compute_indicators(candles)
        except Exception:
            # Python fallback — basic RSI + SMA
            closes = [c["close"] for c in candles]
            sma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else closes[-1]
            price = closes[-1]
            return {
                "regime": "UPTREND" if price > sma20 else "DOWNTREND",
                "sma_20": sma20,
                "price": price,
                "source": "python_fallback",
            }

    def get_regime(self, pair):
        """Get current market regime for a pair."""
        indicators = self.compute_indicators(pair)
        return indicators.get("regime", "UNKNOWN")

    # ── Communication ────────────────────────────────────────────

    def push_to_dashboard(self, data):
        """Push data to Fly.io dashboard."""
        try:
            payload = json.dumps(data).encode()
            url = f"{FLY_URL}/api/trading-snapshot"
            req = urllib.request.Request(
                url, data=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Api-Key": NETTRACE_API_KEY,
                    "User-Agent": "NetTrace-Agent/1.0",
                },
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            logger.debug("Dashboard push failed: %s", e)
            return None

    def log_trade(self, agent, pair, side, price, quantity, total_usd,
                  order_type="limit", order_id=None, status="pending"):
        """Record a trade in the shared database."""
        self.db.execute(
            """INSERT INTO agent_trades
               (agent, pair, side, price, quantity, total_usd, order_type, order_id, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (agent, pair, side, price, quantity, total_usd, order_type, order_id, status)
        )
        self.db.commit()
        logger.info("[%s] %s %s %.8f @ $%.2f ($%.2f) [%s] %s",
                     agent, side, pair, quantity, price, total_usd, order_type, status)

    def record_snapshot(self, agent="system"):
        """Record a portfolio snapshot."""
        portfolio = self.get_portfolio()
        self.db.execute(
            """INSERT INTO portfolio_snapshots
               (total_value_usd, available_cash, held_in_orders, holdings_json, agent)
               VALUES (?, ?, ?, ?, ?)""",
            (portfolio["total_usd"], portfolio["available_cash"],
             portfolio["held_in_orders"], json.dumps(portfolio["holdings"]), agent)
        )
        self.db.commit()
        return portfolio

    def alert(self, message, severity="info"):
        """Log an alert. Pushes critical alerts to dashboard."""
        log_fn = getattr(logger, severity, logger.info)
        log_fn("[ALERT] %s", message)
        if severity in ("error", "critical"):
            self.push_to_dashboard({"alert": message, "severity": severity})

    # ── Wallet / DEX / Smart Routing ─────────────────────────────

    def get_wallet_balances(self, address, chain="ethereum"):
        """Get multi-chain wallet balances."""
        from wallet_connector import WalletConnector
        wallet = WalletConnector(address, chain)
        return wallet.get_balances()

    def get_multi_chain_balances(self, address, chains=None):
        """Get balances across all chains for an EVM address."""
        from wallet_connector import MultiChainWallet
        wallet = MultiChainWallet(address, chains)
        return wallet.get_all_balances()

    def get_best_route(self, pair, side, amount_usd):
        """Find the best execution venue (CEX vs DEX).

        Returns dict with venue, price, fees, total cost.
        """
        from smart_router import SmartRouter
        router = SmartRouter(coinbase_tools=self)
        return router.find_best_execution(pair, side, amount_usd)

    def execute_dex_swap(self, token_in, token_out, amount, chain="base",
                         wallet_address=None, private_key=None, slippage=0.005):
        """Execute a swap on a DEX (Uniswap or Jupiter).

        Args:
            token_in: Input token symbol
            token_out: Output token symbol
            amount: Amount of input token
            chain: Chain to trade on (base, ethereum, solana, etc.)
            wallet_address: Wallet address for the swap
            private_key: Private key for signing
            slippage: Max slippage (default 0.5%)
        """
        from dex_connector import DEXConnector
        dex = DEXConnector(wallet_address, private_key, chain)
        if chain == "solana":
            return dex.swap_jupiter(token_in, token_out, amount, int(slippage * 10000))
        return dex.swap_uniswap(token_in, token_out, amount, slippage, chain)

    def get_combined_portfolio(self, wallet_address=None, wallet_chains=None):
        """Get combined portfolio value: Coinbase + wallet.

        Returns: {
            'coinbase': {...},
            'wallet': {...},
            'total_usd': combined total
        }
        """
        result = {"coinbase": None, "wallet": None, "total_usd": 0}

        # Coinbase portfolio
        try:
            cb = self.get_portfolio()
            result["coinbase"] = cb
            result["total_usd"] += cb["total_usd"]
        except Exception as e:
            logger.warning("Coinbase portfolio unavailable: %s", e)

        # Wallet portfolio
        if wallet_address:
            try:
                from wallet_connector import MultiChainWallet
                wallet = MultiChainWallet(wallet_address, wallet_chains)
                wb = wallet.get_all_balances()
                result["wallet"] = wb
                result["total_usd"] += wb["total_usd"]
            except Exception as e:
                logger.warning("Wallet portfolio unavailable: %s", e)

        result["total_usd"] = round(result["total_usd"], 2)
        return result

    def compare_venues(self, pair, amount_usd=5.00):
        """Get venue comparison for display on dashboard."""
        from smart_router import SmartRouter
        router = SmartRouter(coinbase_tools=self)
        return router.compare_venues(pair, amount_usd)

    # ── Product Discovery ────────────────────────────────────────

    def discover_products(self, quote="USD", min_volume_usd=100000):
        """Find tradeable products on Coinbase sorted by opportunity score."""
        result = self.exchange._request("GET", "/api/v3/brokerage/products?limit=250")
        products = result.get("products", [])

        opportunities = []
        for p in products:
            if p.get("quote_currency_id") != quote:
                continue
            if p.get("status") != "online":
                continue

            try:
                price = float(p.get("price", 0))
                volume = float(p.get("volume_24h", 0))
                volume_usd = volume * price
                if volume_usd < min_volume_usd:
                    continue

                change_pct = float(p.get("price_percentage_change_24h", 0))
                product_id = p.get("product_id", "")
                min_size = float(p.get("base_min_size", "0"))
                min_order_usd = min_size * price

                opportunities.append({
                    "pair": product_id,
                    "price": price,
                    "volume_24h_usd": round(volume_usd),
                    "change_24h_pct": round(change_pct, 2),
                    "min_order_usd": round(min_order_usd, 4),
                    "base_min_size": min_size,
                    "base_increment": p.get("base_increment", "0.00000001"),
                    "quote_increment": p.get("quote_increment", "0.01"),
                })
            except (ValueError, TypeError):
                continue

        # Sort by volatility × volume (best grid trading candidates)
        opportunities.sort(key=lambda x: abs(x["change_24h_pct"]) * x["volume_24h_usd"], reverse=True)
        return opportunities


if __name__ == "__main__":
    import sys
    tools = AgentTools()

    if len(sys.argv) > 1 and sys.argv[1] == "portfolio":
        portfolio = tools.get_portfolio()
        tools.risk.refresh(portfolio["total_usd"])
        risk = tools.risk.to_dict()
        print(f"\nPortfolio: ${portfolio['total_usd']:.2f}")
        print(f"  Available cash: ${portfolio['available_cash']:.2f}")
        print(f"  Held in orders: ${portfolio['held_in_orders']:.2f}")
        for curr, data in sorted(portfolio["holdings"].items(), key=lambda x: -x[1]["usd_value"]):
            held_note = f" (held: {data['held']:.8f})" if data["held"] > 0 else ""
            print(f"  {curr}: {data['amount']:.8f} (${data['usd_value']:.2f}){held_note}")
        print(f"\n  Adaptive Risk (auto-scales with portfolio):")
        print(f"    Max trade:      ${risk['max_trade']:.2f} ({tools.risk.MAX_TRADE_PCT*100:.0f}% of portfolio)")
        print(f"    Daily loss cap: ${risk['max_daily_loss']:.2f} ({tools.risk.MAX_DAILY_LOSS_PCT*100:.0f}% of portfolio)")
        print(f"    Min reserve:    ${risk['min_reserve']:.2f} ({tools.risk.MIN_RESERVE_PCT*100:.0f}% of portfolio)")
        print(f"    Grid size:      ${risk['grid_size']:.2f} x {risk['grid_levels']} levels")
        print(f"    DCA daily:      ${risk['dca_daily']:.2f}")
        print(f"    Streak mult:    {risk['streak_mult']:.2f}x (W{risk['win_streak']}/L{risk['loss_streak']})")

    elif len(sys.argv) > 1 and sys.argv[1] == "discover":
        products = tools.discover_products()
        print(f"\nTop trading opportunities ({len(products)} products):")
        for p in products[:15]:
            print(f"  {p['pair']:<12} ${p['price']:>10,.2f}  vol=${p['volume_24h_usd']:>12,}  "
                  f"Δ{p['change_24h_pct']:+.1f}%  min=${p['min_order_usd']:.4f}")

    elif len(sys.argv) > 1 and sys.argv[1] == "spread":
        for pair in ["BTC-USD", "ETH-USD", "SOL-USD"]:
            spread = tools.get_spread(pair)
            if spread is not None:
                print(f"  {pair}: {spread*100:.4f}% spread")
    else:
        print("Usage: python agent_tools.py [portfolio|discover|spread]")
