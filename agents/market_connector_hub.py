#!/usr/bin/env python3
"""Unified multi-market connector hub for crypto, equities, forex, and DEX."""

import json
import time
import urllib.request
from pathlib import Path

try:
    from exchange_connector import CoinbaseTrader, PriceFeed
except Exception:
    CoinbaseTrader = None
    PriceFeed = None

try:
    from etrade_connector import ETradeAuth, ETradeTrader
except Exception:
    ETradeAuth = None
    ETradeTrader = None

try:
    from ibkr_connector import IBKRTrader
except Exception:
    IBKRTrader = None

try:
    from smart_router import SmartRouter
except Exception:
    SmartRouter = None

try:
    from execution_telemetry import venue_health_snapshot
except Exception:
    def venue_health_snapshot(*_args, **_kwargs):
        return {}


class MarketConnectorHub:
    """Single entrypoint for market coverage + routing across connector types."""

    def __init__(self):
        self._coinbase = CoinbaseTrader() if CoinbaseTrader else None
        self._pricefeed = PriceFeed
        self._smart_router = SmartRouter() if SmartRouter else None
        self._etrade_auth = ETradeAuth() if ETradeAuth else None
        self._etrade = None
        self._ibkr = None

    def _etrade_trader(self):
        if not self._etrade_auth or not ETradeTrader:
            return None
        if not self._etrade_auth.is_authenticated:
            return None
        if self._etrade is None:
            self._etrade = ETradeTrader(auth=self._etrade_auth, sandbox=False)
        return self._etrade

    def _ibkr_trader(self):
        if not IBKRTrader:
            return None
        if self._ibkr is None:
            self._ibkr = IBKRTrader()
            try:
                ok = bool(self._ibkr.connect())
                if not ok:
                    self._ibkr = None
            except Exception:
                self._ibkr = None
        return self._ibkr

    def available_connectors(self):
        return {
            "coinbase": bool(self._coinbase),
            "smart_router": bool(self._smart_router),
            "etrade": bool(self._etrade_trader()),
            "ibkr": bool(self._ibkr_trader()),
        }

    def venue_health(self):
        return {
            "coinbase": venue_health_snapshot("coinbase", window_minutes=30),
            "etrade": venue_health_snapshot("etrade", window_minutes=30),
            "ibkr": venue_health_snapshot("ibkr", window_minutes=30),
            "dex": venue_health_snapshot("dex", window_minutes=30),
        }

    @staticmethod
    def check_dashboard_endpoints():
        urls = [
            "https://nettrace-dashboard.fly.dev/",
            "https://nettrace-dashboard.fly.dev/trading",
            "https://nettrace-dashboard.fly.dev/status",
            "https://nettrace-dashboard.fly.dev/playground",
        ]
        out = []
        for url in urls:
            t0 = time.perf_counter()
            row = {"url": url, "ok": False, "status_code": 0, "latency_ms": 0.0, "error": ""}
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-MarketHub/1.0"})
                with urllib.request.urlopen(req, timeout=8) as resp:
                    dt = (time.perf_counter() - t0) * 1000.0
                    row["ok"] = True
                    row["status_code"] = int(getattr(resp, "status", 200))
                    row["latency_ms"] = round(dt, 3)
            except Exception as e:
                row["latency_ms"] = round((time.perf_counter() - t0) * 1000.0, 3)
                row["error"] = str(e)
            out.append(row)
        return out

    def get_quote(self, symbol, market_type="crypto", side="BUY", amount=10.0):
        market = str(market_type).lower()
        if market == "crypto":
            pair = symbol if "-" in symbol else f"{symbol}-USD"
            if self._smart_router:
                routed = self._smart_router.find_best_execution(pair, side, float(amount))
                if isinstance(routed, dict) and "error" not in routed:
                    return {"source": "smart_router", "market_type": "crypto", "quote": routed}
            if self._pricefeed:
                px = self._pricefeed.get_price(pair)
                return {"source": "coinbase_spot", "market_type": "crypto", "pair": pair, "price": px}
            return {"error": "no_crypto_connector"}

        if market in {"equity", "stocks"}:
            et = self._etrade_trader()
            if et:
                q = et.get_quote([symbol])
                return {"source": "etrade", "market_type": "equity", "quote": q}
            ib = self._ibkr_trader()
            if ib:
                try:
                    q = ib.get_market_data(symbol)
                    return {"source": "ibkr", "market_type": "equity", "quote": q}
                except Exception as e:
                    return {"error": f"ibkr_quote_failed:{e}"}
            return {"error": "no_equity_connector"}

        if market == "forex":
            pair = symbol if "-" in symbol else symbol.replace("/", "-")
            if self._pricefeed:
                px = self._pricefeed.get_price(pair)
                return {"source": "coinbase_proxy", "market_type": "forex", "pair": pair, "price": px}
            return {"error": "no_forex_connector"}

        return {"error": f"unsupported_market_type:{market}"}

    def route_order(self, symbol, side, amount, market_type="crypto"):
        market = str(market_type).lower()
        if market == "crypto":
            pair = symbol if "-" in symbol else f"{symbol}-USD"
            if not self._coinbase:
                return {"error": "coinbase_connector_unavailable"}
            if side.upper() == "BUY":
                return self._coinbase.place_order(pair, "BUY", float(amount))
            return self._coinbase.place_order(pair, "SELL", float(amount))

        if market in {"equity", "stocks"}:
            et = self._etrade_trader()
            if not et:
                return {"error": "etrade_not_authenticated"}
            return {
                "error": "equity_order_requires_account_id_and_symbol_qty_mapping",
                "hint": "use ETradeTrader.place_order(account_id_key, symbol, side, quantity, ...)",
            }

        return {"error": f"unsupported_market_type:{market}"}


def write_hub_snapshot(path=None):
    out = Path(path) if path else (Path(__file__).parent / "market_connector_hub_status.json")
    hub = MarketConnectorHub()
    payload = {
        "connectors": hub.available_connectors(),
        "venue_health": hub.venue_health(),
        "dashboard_endpoints": hub.check_dashboard_endpoints(),
        "samples": {
            "crypto_quote": hub.get_quote("BTC-USD", market_type="crypto", side="BUY", amount=25.0),
            "equity_quote": hub.get_quote("AAPL", market_type="equity", side="BUY", amount=25.0),
            "forex_quote": hub.get_quote("EUR-USD", market_type="forex", side="BUY", amount=25.0),
        },
    }
    out.write_text(json.dumps(payload, indent=2))
    return payload


if __name__ == "__main__":
    print(json.dumps(write_hub_snapshot(), indent=2))
