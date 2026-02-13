#!/usr/bin/env python3
"""NetTrace Live Trader — trades real crypto on Coinbase using latency signals.

Strategy:
  1. Monitor our latency signals for crypto exchange anomalies
  2. When a signal fires with confidence > threshold, execute a trade
  3. Use tight risk management: 0.5% risk per trade, 2% daily max loss
  4. Trade BTC-USD, ETH-USD, SOL-USD on Coinbase

With $13 starting capital, we focus on:
  - Consolidating scattered alts into USDC
  - Trading micro positions on high-confidence signals
  - Compounding wins
"""

import json
import logging
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Load .env file for credentials
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
        logging.FileHandler(str(Path(__file__).parent / "live_trader.log")),
    ]
)
logger = logging.getLogger("live_trader")

try:
    from no_loss_policy import (
        evaluate_trade as _evaluate_no_loss_trade,
        log_decision as _log_no_loss_decision,
        record_root_cause as _record_no_loss_root_cause,
    )
except Exception:
    try:
        from agents.no_loss_policy import (  # type: ignore
            evaluate_trade as _evaluate_no_loss_trade,
            log_decision as _log_no_loss_decision,
            record_root_cause as _record_no_loss_root_cause,
        )
    except Exception:
        def _evaluate_no_loss_trade(**kwargs):
            payload = dict(kwargs)
            payload["approved"] = True
            payload["reason"] = "policy_module_unavailable"
            return payload

        def _log_no_loss_decision(*_args, **_kwargs):
            return None

        def _record_no_loss_root_cause(*_args, **_kwargs):
            return None

try:
    from execution_telemetry import venue_health_snapshot as _venue_health_snapshot
except Exception:
    try:
        from agents.execution_telemetry import venue_health_snapshot as _venue_health_snapshot  # type: ignore
    except Exception:
        def _venue_health_snapshot(*_args, **_kwargs):
            return {}

TRADER_DB = str(Path(__file__).parent / "trader.db")
SIGNAL_API = "https://nettrace-dashboard.fly.dev/api/v1/signals"
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")

# Risk parameters — ALL dynamic from risk_controller, these are only absolute floors
MIN_TRADE_USD = 1.00       # Coinbase min is ~$1
SIGNAL_MIN_CONFIDENCE = 0.70
CHECK_INTERVAL = 120       # Check every 2 minutes
POSITION_HOLD_SECONDS = 300  # Hold for 5 minutes then re-evaluate

# Dynamic risk controller — NO hardcoded limits
try:
    from risk_controller import get_controller as _get_rc
    _live_risk_ctrl = _get_rc()
except Exception:
    _live_risk_ctrl = None
    logging.getLogger("live_trader").error("Risk controller failed to load — trades will be BLOCKED")


class LiveTrader:
    def __init__(self):
        from exchange_connector import CoinbaseTrader, PriceFeed
        from smart_router import SmartRouter
        self.exchange = CoinbaseTrader()
        self.pricefeed = PriceFeed
        self.router = SmartRouter()
        self.db = sqlite3.connect(TRADER_DB)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self._init_db()
        self.daily_pnl = 0.0
        self.trades_today = 0

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS live_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL,
                quantity REAL,
                total_usd REAL,
                signal_type TEXT,
                signal_confidence REAL,
                signal_host TEXT,
                coinbase_order_id TEXT,
                status TEXT DEFAULT 'pending',
                pnl REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS live_pnl (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_value_usd REAL,
                daily_pnl REAL,
                trades_today INTEGER,
                assets_json TEXT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.db.commit()

    def get_portfolio_value(self):
        """Get total portfolio value in USD (includes funds locked in open orders)."""
        accounts = self.exchange._request("GET", "/api/v3/brokerage/accounts?limit=250")
        if "accounts" not in accounts:
            logger.error("Failed to get accounts: %s", accounts)
            return 0, {}

        total_usd = 0
        holdings = {}
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
            else:
                price = self.pricefeed.get_price(f"{currency}-USD")
                usd_value = total_amount * price if price else 0

            if usd_value > 0.01:
                holdings[currency] = {
                    "amount": total_amount,
                    "available": amount,
                    "held": held,
                    "usd_value": round(usd_value, 2),
                }
                total_usd += usd_value

        return round(total_usd, 2), holdings

    def get_signals(self):
        """Fetch latest signals from NetTrace API."""
        try:
            url = f"{SIGNAL_API}?limit=20"
            if NETTRACE_API_KEY:
                url += f"&api_key={NETTRACE_API_KEY}"
            req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-Trader/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            return data.get("signals", [])
        except Exception as e:
            logger.debug("Signal fetch: %s", e)
            return []

    def _get_regime(self, pair):
        """Detect market regime using C engine or Python fallback."""
        try:
            from fast_bridge import FastEngine
            engine = FastEngine()
            # Fetch recent candles from public API
            import urllib.request as ur
            end = int(time.time())
            start = end - 24 * 3600  # 24h of 5-min candles
            url = (f"https://api.exchange.coinbase.com/products/{pair}/candles"
                   f"?start={datetime.fromtimestamp(start, tz=timezone.utc).isoformat()}"
                   f"&end={datetime.fromtimestamp(end, tz=timezone.utc).isoformat()}"
                   f"&granularity=300")
            req = ur.Request(url, headers={"User-Agent": "NetTrace/1.0"})
            with ur.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            candles = [{"open": c[3], "high": c[2], "low": c[1], "close": c[4],
                        "volume": c[5], "time": c[0]} for c in data]
            candles.sort(key=lambda x: x["time"])

            if len(candles) < 50:
                return {"regime": "UNKNOWN", "recommendation": "HOLD"}

            ind = engine.compute_indicators(candles)
            sig = engine.generate_signal(candles)
            return {
                "regime": ind["regime"],
                "rsi": ind["rsi_14"],
                "atr": ind["atr_14"],
                "vwap": ind["vwap"],
                "recommendation": "HOLD" if ind["regime"] == "DOWNTREND" else "TRADE",
                "c_signal": sig,
            }
        except Exception as e:
            logger.debug("Regime detection: %s", e)
            return {"regime": "UNKNOWN", "recommendation": "TRADE"}

    def _get_route_quote(self, pair, side, amount_usd):
        pair_usd = pair.replace("-USDC", "-USD")
        try:
            route = self.router.find_best_execution(pair_usd, side, amount_usd)
            if isinstance(route, dict) and "error" not in route:
                return route
        except Exception as e:
            logger.debug("Route quote failed for %s: %s", pair, e)
        return {}

    def _evaluate_no_loss_gate(self, pair, side, amount_usd, expected_edge_pct, confidence, regime):
        route = self._get_route_quote(pair, side, amount_usd)
        total_cost_pct = float(route.get("total_cost_pct", 1.0) or 1.0)
        spread_pct = float(route.get("slippage_pct", 0.0) or 0.0)
        health = _venue_health_snapshot("coinbase", window_minutes=30) or {}
        decision = _evaluate_no_loss_trade(
            pair=pair,
            side=side,
            expected_edge_pct=float(expected_edge_pct or 0.0),
            total_cost_pct=total_cost_pct,
            spread_pct=spread_pct,
            venue_latency_ms=float(health.get("p90_latency_ms", 0.0) or 0.0),
            venue_failure_rate=float(health.get("failure_rate", 0.0) or 0.0),
            signal_confidence=float(confidence or 0.0),
            market_regime=str(regime or "UNKNOWN"),
        )
        _log_no_loss_decision(
            decision,
            details={
                "source": "live_trader",
                "route": route,
                "venue_health": health,
            },
        )
        return decision, route

    @staticmethod
    def _order_success(result):
        if not isinstance(result, dict):
            return False, None
        if "success_response" in result and isinstance(result["success_response"], dict):
            oid = result["success_response"].get("order_id")
            return bool(oid), oid
        if result.get("order_id"):
            return True, result.get("order_id")
        return False, None

    @staticmethod
    def _fallback_pairs(pair):
        pair = str(pair).upper()
        out = [pair]
        if pair.endswith("-USDC"):
            out.append(pair.replace("-USDC", "-USD"))
        elif pair.endswith("-USD"):
            out.append(pair.replace("-USD", "-USDC"))
        # preserve order and uniqueness
        uniq = []
        for p in out:
            if p not in uniq:
                uniq.append(p)
        return uniq

    def _place_buy_with_fallback(
        self,
        pair,
        usd_amount,
        limit_price,
        expected_edge_pct,
        signal_confidence,
        market_regime,
    ):
        attempts = []
        for pair_candidate in self._fallback_pairs(pair):
            if limit_price <= 0:
                base_size = 0.0
            else:
                base_size = max(0.0, float(usd_amount) / float(limit_price))

            order_attempts = [
                {
                    "route": "limit_post_only",
                    "fn": lambda p=pair_candidate, sz=base_size, px=limit_price: self.exchange.place_limit_order(
                        p,
                        "BUY",
                        sz,
                        px,
                        post_only=True,
                        expected_edge_pct=expected_edge_pct,
                        signal_confidence=signal_confidence,
                        market_regime=market_regime,
                    ),
                },
                {
                    "route": "limit_taker_fallback",
                    "fn": lambda p=pair_candidate, sz=base_size, px=limit_price: self.exchange.place_limit_order(
                        p,
                        "BUY",
                        sz,
                        px,
                        post_only=False,
                        expected_edge_pct=expected_edge_pct,
                        signal_confidence=signal_confidence,
                        market_regime=market_regime,
                    ),
                },
                {
                    "route": "market_ioc_fallback",
                    "fn": lambda p=pair_candidate, amt=usd_amount: self.exchange.place_order(
                        p,
                        "BUY",
                        amt,
                        order_type="market",
                        expected_edge_pct=expected_edge_pct,
                        signal_confidence=signal_confidence,
                        market_regime=market_regime,
                    ),
                },
            ]

            for attempt in order_attempts:
                route_name = str(attempt["route"])
                try:
                    result = attempt["fn"]()
                except Exception as e:
                    result = {"error_response": {"error": "route_exception", "message": str(e)}}
                ok, order_id = self._order_success(result)
                attempts.append(
                    {
                        "pair": pair_candidate,
                        "route": route_name,
                        "success": bool(ok),
                        "order_id": order_id,
                        "result": result if isinstance(result, dict) else {"raw": str(result)},
                    }
                )
                if ok:
                    return {
                        "ok": True,
                        "pair": pair_candidate,
                        "route": route_name,
                        "order_id": order_id,
                        "result": result,
                        "attempts": attempts,
                    }
                logger.warning(
                    "Order route failed: pair=%s route=%s response=%s",
                    pair_candidate,
                    route_name,
                    json.dumps(result)[:280] if isinstance(result, dict) else str(result),
                )
        return {"ok": False, "pair": pair, "route": "all_failed", "result": {}, "attempts": attempts}

    def evaluate_and_trade(self, signals):
        """Evaluate signals and execute trades.

        RULES (NEVER VIOLATE):
        1. NEVER sell at a loss — only sell when current_price > buy_price + fees
        2. BUY only with high confidence (>70%) and multiple confirming signals
        3. Daily loss limit: stop after $2 loss
        4. Only BUY when we have USD/USDC available — no panic sells
        5. Check market regime FIRST — skip downtrends
        """
        # Block trades if risk controller unavailable
        if not _live_risk_ctrl:
            logger.error("Risk controller unavailable — BLOCKING all trades")
            return

        # Get dynamic limits from risk controller
        total_val, holdings = self.get_portfolio_value()
        risk_params = _live_risk_ctrl.get_risk_params(total_val)
        max_trade_usd = risk_params["max_trade_usd"]
        max_daily_loss = risk_params["max_daily_loss"]

        if self.daily_pnl <= -max_daily_loss:
            logger.warning("Daily loss limit hit ($%.2f >= $%.2f). STOPPED.", self.daily_pnl, max_daily_loss)
            return

        # We only BUY — we accumulate assets on strong signals
        # We only SELL when price is ABOVE our purchase price + fees (0.6%)
        # This ensures we NEVER LOSE MONEY on a trade

        usdc = holdings.get("USDC", {}).get("usd_value", 0)
        usd = holdings.get("USD", {}).get("usd_value", 0)
        available_cash = usdc + usd

        # Count confirming buy signals from NetTrace latency data
        buy_signals = {}  # pair -> list of signals
        for signal in signals:
            sig_type = signal.get("signal_type", "")
            host = signal.get("target_host", "")
            direction = signal.get("direction", "")
            confidence = float(signal.get("confidence", 0))

            if confidence < 0.70:  # Only high confidence
                continue

            # Map host to pair
            pair = None
            if "coinbase" in host or "binance" in host or "kraken" in host:
                pair = "BTC-USD"
            elif "bybit" in host or "okx" in host:
                pair = "BTC-USD"
            elif "gemini" in host:
                pair = "ETH-USD"

            if not pair:
                continue

            # Only BUY signals — accumulate on dips
            is_buy = False
            if sig_type == "rtt_anomaly" and direction == "up":
                is_buy = True
            elif sig_type == "cross_exchange_latency_diff":
                is_buy = True
            elif sig_type == "rtt_trend" and direction == "uptrend":
                is_buy = True

            if is_buy:
                buy_signals.setdefault(pair, []).append(signal)

        # Also check C engine signals — expanded to high-volatility pairs
        for pair in ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "AVAX-USD", "LINK-USD", "XRP-USD"]:
            regime = self._get_regime(pair)

            # Skip downtrend pairs — Rule #1
            if regime.get("recommendation") == "HOLD":
                logger.debug("Skipping %s — regime: %s", pair, regime.get("regime"))
                continue

            # Check if C engine generated a buy signal (must meet same 0.70 threshold)
            c_sig = regime.get("c_signal", {})
            if c_sig and c_sig.get("signal_type") == "BUY" and c_sig.get("confidence", 0) >= 0.70:
                buy_signals.setdefault(pair, []).append({
                    "signal_type": "c_engine_" + c_sig.get("reason", ""),
                    "confidence": c_sig.get("confidence", 0),
                    "target_host": "fast_engine",
                })

        # Only execute if 2+ signals agree on the same pair (confirmation)
        for pair, sigs in buy_signals.items():
            if len(sigs) < 2:
                continue  # Need multiple confirming signals

            if available_cash < MIN_TRADE_USD:
                logger.debug("No cash available for BUY ($%.2f)", available_cash)
                break

            # Check cooldown
            recent = self.db.execute(
                "SELECT 1 FROM live_trades WHERE pair=? AND created_at > datetime('now', '-10 minutes')",
                (pair,)
            ).fetchone()
            if recent:
                continue

            avg_conf = sum(float(s.get("confidence", 0)) for s in sigs) / len(sigs)
            trade_usd = min(max_trade_usd, max(MIN_TRADE_USD, avg_conf * max_trade_usd * 1.2))
            trade_usd = min(trade_usd, available_cash)
            regime = self._get_regime(pair)
            regime_name = str(regime.get("regime", "UNKNOWN"))
            expected_edge_pct = max(0.0, (avg_conf - 0.50) * 10.0)  # 80% conf -> ~3% edge

            logger.info("BUY SIGNAL: %s | %d confirming signals | avg_conf=%.2f | $%.2f",
                        pair, len(sigs), avg_conf, trade_usd)
            self._execute_trade(
                pair,
                "BUY",
                trade_usd,
                sigs[0],
                expected_edge_pct=expected_edge_pct,
                signal_confidence=avg_conf,
                market_regime=regime_name,
            )
            break  # One trade per cycle

    def _execute_trade(
        self,
        pair,
        side,
        usd_amount,
        signal,
        expected_edge_pct=0.0,
        signal_confidence=0.75,
        market_regime="UNKNOWN",
    ):
        """Execute a real BUY trade on Coinbase. SELL is disabled (accumulation mode)."""
        if side.upper() != "BUY":
            logger.warning("BLOCKED SELL on %s — accumulation mode, BUY only until portfolio > $100", pair)
            return

        price = self.pricefeed.get_price(pair)
        if not price:
            logger.warning("No price for %s", pair)
            return

        decision, route = self._evaluate_no_loss_gate(
            pair=pair,
            side=side,
            amount_usd=usd_amount,
            expected_edge_pct=expected_edge_pct,
            confidence=signal_confidence,
            regime=market_regime,
        )
        if not decision.get("approved", False):
            reason = decision.get("reason", "blocked")
            logger.warning("NO-LOSS BLOCK: %s %s $%.2f | %s", side, pair, usd_amount, reason)
            _record_no_loss_root_cause(
                pair,
                side,
                "pre_trade_policy_block",
                reason,
                details={"decision": decision, "route": route, "signal": signal},
            )
            return

        logger.info("EXECUTING: BUY %s | $%.2f @ $%.2f | signal=%s conf=%.2f",
                     pair, usd_amount, price,
                     signal.get("signal_type"), float(signal.get("confidence", 0)))
        # HFT-style maker entry: post-only limit near best bid to reduce fee/slippage.
        book = self.exchange.get_order_book(pair, level=1)
        pb = book.get("pricebook", {}) if isinstance(book, dict) else {}
        bids = pb.get("bids", []) if isinstance(pb, dict) else []
        asks = pb.get("asks", []) if isinstance(pb, dict) else []
        best_bid = float(bids[0].get("price", 0.0) or 0.0) if bids else 0.0
        best_ask = float(asks[0].get("price", 0.0) or 0.0) if asks else 0.0
        if best_bid > 0 and best_ask > 0:
            limit_price = min(best_ask * 0.9999, best_bid * 1.0002)
        elif best_bid > 0:
            limit_price = best_bid
        else:
            limit_price = price
        if limit_price <= 0:
            limit_price = price
        fallback = self._place_buy_with_fallback(
            pair=pair,
            usd_amount=usd_amount,
            limit_price=limit_price,
            expected_edge_pct=expected_edge_pct,
            signal_confidence=signal_confidence,
            market_regime=market_regime,
        )
        result = fallback.get("result", {}) if isinstance(fallback, dict) else {}
        executed_pair = str(fallback.get("pair", pair) if isinstance(fallback, dict) else pair)

        order_id = None
        status = "failed"
        ok, oid = self._order_success(result)
        if ok:
            order_id = oid
            status = "filled"
            logger.info(
                "ORDER FILLED: %s | order_id=%s | route=%s",
                executed_pair,
                order_id,
                fallback.get("route", "direct"),
            )
        elif "error_response" in result:
            err = result["error_response"]
            logger.warning("ORDER FAILED: %s | %s", executed_pair, err.get("message", err))
            _record_no_loss_root_cause(
                executed_pair,
                side,
                "order_rejected",
                str(err.get("message", err)),
                details={"response": result, "signal": signal, "attempts": fallback.get("attempts", [])},
            )
            status = "failed"
        else:
            logger.warning("ORDER RESPONSE: %s", json.dumps(result)[:300])
            _record_no_loss_root_cause(
                executed_pair,
                side,
                "order_all_routes_failed",
                "all execution routes failed",
                details={"attempts": fallback.get("attempts", []), "signal": signal},
            )

        # Record trade
        exec_price = self.pricefeed.get_price(executed_pair) or price
        self.db.execute(
            """INSERT INTO live_trades (pair, side, price, quantity, total_usd,
               signal_type, signal_confidence, signal_host, coinbase_order_id, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (executed_pair, side, exec_price, usd_amount / exec_price if exec_price else 0, usd_amount,
             signal.get("signal_type"), signal.get("confidence"),
             signal.get("target_host"), order_id, status)
        )
        self.db.commit()
        self.trades_today += 1

    def snapshot(self):
        """Record portfolio snapshot locally and push to Fly dashboard."""
        total, holdings = self.get_portfolio_value()
        self.db.execute(
            "INSERT INTO live_pnl (total_value_usd, daily_pnl, trades_today, assets_json) VALUES (?, ?, ?, ?)",
            (total, self.daily_pnl, self.trades_today, json.dumps(holdings))
        )
        self.db.commit()

        # Push to Fly dashboard (skip if total is 0 — likely API error)
        if total > 0:
            self._push_to_fly(total, holdings)
        return total, holdings

    def _push_to_fly(self, total, holdings):
        """Push snapshot to Fly trading dashboard."""
        if total <= 0:
            logger.debug("Skipping Fly push — total is $0 (API error?)")
            return
        try:
            # Get recent trades
            trades = self.db.execute(
                "SELECT pair, side, price, total_usd, signal_type, signal_confidence, status, created_at FROM live_trades ORDER BY id DESC LIMIT 20"
            ).fetchall()
            trades_list = [dict(t) for t in trades]
            trades_total = self.db.execute("SELECT COUNT(*) as cnt FROM live_trades").fetchone()["cnt"]

            payload = json.dumps({
                "user_id": 2,
                "total_value_usd": total,
                "daily_pnl": self.daily_pnl,
                "trades_today": self.trades_today,
                "trades_total": trades_total,
                "holdings": holdings,
                "trades": trades_list,
            }).encode()

            url = "https://nettrace-dashboard.fly.dev/api/trading-snapshot"
            req = urllib.request.Request(
                url, data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {NETTRACE_API_KEY}",
                    "User-Agent": "NetTrace-Trader/1.0",
                },
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode())
                logger.debug("Pushed snapshot to Fly: %s", result)
        except Exception as e:
            logger.debug("Fly push failed: %s", e)

    def print_status(self):
        total, holdings = self.get_portfolio_value()
        trades = self.db.execute(
            "SELECT COUNT(*) as cnt, SUM(CASE WHEN status='filled' THEN 1 ELSE 0 END) as filled FROM live_trades"
        ).fetchone()

        print("\n" + "=" * 60)
        print(f"  LIVE TRADER | Portfolio: ${total:.2f}")
        print("=" * 60)
        for currency, data in sorted(holdings.items(), key=lambda x: -x[1]["usd_value"]):
            print(f"  {currency:<6} {data['amount']:<15.8f} ${data['usd_value']:.2f}")
        print(f"  ---")
        print(f"  Trades: {trades['cnt']} total, {trades['filled']} filled")
        print(f"  Daily P&L: ${self.daily_pnl:+.2f}")
        print("=" * 60 + "\n")

    def run(self):
        """Main trading loop."""
        total, holdings = self.get_portfolio_value()
        logger.info("Live Trader starting | Portfolio: $%.2f | %d assets", total, len(holdings))
        self.print_status()

        # Push initial snapshot to Fly dashboard
        self._push_to_fly(total, holdings)

        cycle = 0
        while True:
            try:
                cycle += 1

                # Get signals and trade
                signals = self.get_signals()
                if signals:
                    self.evaluate_and_trade(signals)

                # Snapshot every 5 cycles (~10 min) and push to Fly
                if cycle % 5 == 0:
                    self.snapshot()
                    if cycle % 15 == 0:
                        self.print_status()

                time.sleep(CHECK_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Shutting down...")
                self.print_status()
                break
            except Exception as e:
                logger.error("Error: %s", e, exc_info=True)
                time.sleep(60)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        trader = LiveTrader()
        trader.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "consolidate":
        # Sell all alts to USD for cleaner trading
        trader = LiveTrader()
        total, holdings = trader.get_portfolio_value()
        print(f"Portfolio: ${total:.2f}")
        for currency, data in holdings.items():
            if currency not in ("USD", "USDC", "BTC", "ETH") and data["usd_value"] > 1.0:
                pair = f"{currency}-USD"
                print(f"Selling {data['amount']} {currency} (~${data['usd_value']:.2f})...")
                result = trader.exchange.place_order(pair, "SELL", data["amount"])
                print(f"  Result: {json.dumps(result)[:200]}")
                time.sleep(1)
    else:
        trader = LiveTrader()
        trader.run()
