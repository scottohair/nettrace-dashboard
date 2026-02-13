#!/usr/bin/env python3
"""Low-latency connector abstraction for HF execution.

Goals:
  - Minimize decision-to-order latency in Python stack.
  - Provide one interface for Coinbase + IBKR, with FIX-ready extension hooks.
  - Keep runtime mostly stateless; cache is in-memory only.
"""

import json
import logging
import os
import socket
import time
import urllib.error
import urllib.request
from pathlib import Path

logger = logging.getLogger("low_latency_connector")

try:
    from execution_telemetry import record_api_call, venue_health_snapshot
except Exception:
    try:
        from agents.execution_telemetry import record_api_call, venue_health_snapshot  # type: ignore
    except Exception:
        def record_api_call(*_args, **_kwargs):
            return None

        def venue_health_snapshot(*_args, **_kwargs):
            return {}

try:
    from exchange_connector import CoinbaseTrader, PriceFeed
except Exception:
    try:
        from agents.exchange_connector import CoinbaseTrader, PriceFeed  # type: ignore
    except Exception:
        CoinbaseTrader = None  # type: ignore
        PriceFeed = None  # type: ignore

try:
    from ibkr_connector import IBKRTrader
except Exception:
    try:
        from agents.ibkr_connector import IBKRTrader  # type: ignore
    except Exception:
        IBKRTrader = None  # type: ignore


def _fetch_json(url, timeout=3.5):
    req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-LowLatency/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


class LowLatencyConnector:
    """Execution facade for fast venue routing."""

    def __init__(self, prefer="coinbase", allow_ibkr=True, allow_fix_stub=True):
        self.prefer = str(prefer or "coinbase").lower()
        self.allow_ibkr = bool(allow_ibkr)
        self.allow_fix_stub = bool(allow_fix_stub)
        self.coinbase_api_key_id = os.environ.get("COINBASE_API_KEY_ID", "").strip()
        self.coinbase_api_key_secret = os.environ.get("COINBASE_API_KEY_SECRET", "").strip()
        self.fix_gateway_url = os.environ.get("FIX_GATEWAY_URL", "").strip()
        self.fix_api_key = os.environ.get("FIX_API_KEY", "").strip()
        self.fix_timeout_ms = int(os.environ.get("FIX_TIMEOUT_MS", "900"))
        self.synthetic_spread_pct = float(os.environ.get("HF_FALLBACK_SYNTH_SPREAD_PCT", "0.09"))
        self.default_quote_timeout = float(os.environ.get("HF_QUOTE_TIMEOUT_SECONDS", "3.5"))

        self._coinbase = None
        self._ibkr = None
        self._last_health = {
            "coinbase": {},
            "ibkr": {},
            "fix": {},
        }

        if CoinbaseTrader is not None and self._coinbase_trade_auth_present():
            try:
                self._coinbase = CoinbaseTrader()
            except Exception:
                self._coinbase = None

        if self.allow_ibkr and IBKRTrader is not None:
            try:
                ib = IBKRTrader()
                if ib.connect():
                    self._ibkr = ib
            except Exception:
                self._ibkr = None

    @staticmethod
    def _to_usd_pair(pair):
        p = str(pair or "").upper()
        if p.endswith("-USDC"):
            return p.replace("-USDC", "-USD")
        return p

    def _coinbase_trade_auth_present(self):
        return bool(self.coinbase_api_key_id and self.coinbase_api_key_secret)

    @staticmethod
    def _split_pair(pair):
        p = str(pair or "").upper()
        if "-" in p:
            base, quote = p.split("-", 1)
            return base.strip(), quote.strip()
        return p[:3], p[3:] if len(p) >= 6 else "USD"

    @staticmethod
    def _quick_dns_ok(host):
        try:
            socket.getaddrinfo(host, None)
            return True
        except Exception:
            return False

    def _spot_price_fallback(self, pair):
        base, quote = self._split_pair(pair)
        # Prefer existing feed abstraction first (has Coinbase+CoinGecko fallback).
        if PriceFeed is not None:
            try:
                px = PriceFeed.get_price(f"{base}-{quote}")
                if px is not None and float(px) > 0:
                    return float(px), "pricefeed"
            except Exception:
                pass
        # Final direct fallback to Coinbase spot.
        try:
            spot = _fetch_json(
                f"https://api.coinbase.com/v2/prices/{base}-{quote}/spot",
                timeout=self.default_quote_timeout,
            )
            amount = float((spot or {}).get("data", {}).get("amount", 0.0) or 0.0)
            if amount > 0:
                return amount, "coinbase_spot"
        except Exception:
            pass
        return None, ""

    def _synthetic_book(self, pair, mid, source):
        spread_pct = max(0.01, float(self.synthetic_spread_pct))
        half = (spread_pct / 100.0) / 2.0
        bid = float(mid) * (1.0 - half)
        ask = float(mid) * (1.0 + half)
        return {
            "pair": pair,
            "venue": "coinbase_fallback",
            "bid": bid,
            "ask": ask,
            "mid": float(mid),
            "spread_pct": spread_pct,
            "fallback_source": source,
        }

    def health(self):
        self._last_health["coinbase"] = venue_health_snapshot("coinbase", window_minutes=30)
        self._last_health["ibkr"] = venue_health_snapshot("ibkr", window_minutes=30)
        self._last_health["fix"] = {
            "enabled": bool(self.allow_fix_stub),
            "configured": bool(self.fix_gateway_url),
            "gateway_url": self.fix_gateway_url,
            "status": "configured" if self.fix_gateway_url else "stub",
            "note": "FIX gateway configured via FIX_GATEWAY_URL" if self.fix_gateway_url else "FIX integration hook ready; endpoint/wiring required",
        }
        coinbase_dns = self._quick_dns_ok("api.coinbase.com") or self._quick_dns_ok("api.exchange.coinbase.com")
        c_h = self._last_health["coinbase"]
        c_samples = int(c_h.get("samples", 0) or 0)
        c_success = float(c_h.get("success_rate", 0.0) or 0.0)
        c_p90 = float(c_h.get("p90_latency_ms", 0.0) or 0.0)
        coinbase_market_data_ready = bool(coinbase_dns and (c_samples < 10 or c_success >= 0.25))
        coinbase_trade_ready = bool(self._coinbase is not None and self._coinbase_trade_auth_present())
        return {
            "coinbase_ready": coinbase_market_data_ready,
            "coinbase_trade_ready": coinbase_trade_ready,
            "coinbase_auth_present": self._coinbase_trade_auth_present(),
            "coinbase_dns_ok": bool(coinbase_dns),
            "coinbase_telemetry_p90_ms": round(c_p90, 3),
            "ibkr_ready": self._ibkr is not None,
            "fix_ready": bool(self.fix_gateway_url),
            "telemetry": dict(self._last_health),
        }

    def quote_crypto(self, pair):
        pair_usd = self._to_usd_pair(pair)
        t0 = time.perf_counter()
        primary_ok = False
        primary_error = ""
        url = f"https://api.exchange.coinbase.com/products/{pair_usd}/book?level=1"
        try:
            payload = _fetch_json(url, timeout=self.default_quote_timeout)
            bids = payload.get("bids", []) or []
            asks = payload.get("asks", []) or []
            if not bids or not asks:
                raise RuntimeError("empty_orderbook")
            bid = float(bids[0][0])
            ask = float(asks[0][0])
            mid = (bid + ask) / 2.0
            spread_pct = ((ask - bid) / mid) * 100.0 if mid > 0 else 0.0
            primary_ok = True
            return {
                "pair": pair,
                "venue": "coinbase",
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread_pct": spread_pct,
            }
        except Exception as e:
            primary_error = str(e)
        finally:
            latency_ms = (time.perf_counter() - t0) * 1000.0
            record_api_call(
                venue="coinbase",
                method="GET",
                endpoint=f"/products/{pair_usd}/book",
                latency_ms=latency_ms,
                ok=primary_ok,
                status_code=200 if primary_ok else None,
                error_text="" if primary_ok else (primary_error or "book_fetch_failed"),
                context={"pair": pair_usd, "layer": "low_latency_connector", "stage": "primary"},
            )

        # Fallback path: still provide mid + synthetic spread so scanners keep functioning.
        fallback_t0 = time.perf_counter()
        fallback_ok = False
        fallback_error = ""
        fallback_source = ""
        try:
            mid, fallback_source = self._spot_price_fallback(pair_usd)
            if mid is None or float(mid) <= 0:
                raise RuntimeError("fallback_spot_unavailable")
            fallback_ok = True
            return self._synthetic_book(pair, float(mid), fallback_source or "spot")
        except Exception as e:
            fallback_error = str(e)
            raise
        finally:
            fb_latency = (time.perf_counter() - fallback_t0) * 1000.0
            record_api_call(
                venue="coinbase",
                method="GET",
                endpoint=f"/products/{pair_usd}/book:fallback",
                latency_ms=fb_latency,
                ok=fallback_ok,
                status_code=200 if fallback_ok else None,
                error_text="" if fallback_ok else (fallback_error or "fallback_failed"),
                context={
                    "pair": pair_usd,
                    "layer": "low_latency_connector",
                    "stage": "fallback",
                    "fallback_source": fallback_source,
                },
            )

    def quote_equity(self, symbol):
        if self._ibkr is None:
            return {"error": "ibkr_unavailable"}
        t0 = time.perf_counter()
        ok = False
        try:
            data = self._ibkr.get_market_data(symbol)
            bid = float(data.get("bid") or 0.0)
            ask = float(data.get("ask") or 0.0)
            last = float(data.get("last") or 0.0)
            mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else (last if last > 0 else 0.0)
            spread_pct = ((ask - bid) / mid) * 100.0 if bid > 0 and ask > 0 and mid > 0 else 0.0
            ok = True
            return {
                "symbol": symbol,
                "venue": "ibkr",
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "last": last,
                "spread_pct": spread_pct,
            }
        except Exception as e:
            return {"error": str(e)}
        finally:
            latency_ms = (time.perf_counter() - t0) * 1000.0
            record_api_call(
                venue="ibkr",
                method="SNAPSHOT",
                endpoint=f"market_data/{symbol}",
                latency_ms=latency_ms,
                ok=ok,
                status_code=200 if ok else None,
                error_text="" if ok else "ibkr_quote_failed",
                context={"symbol": symbol, "layer": "low_latency_connector"},
            )

    def place_order_crypto(self, pair, side, amount_usd, mid_price, post_only=True):
        if self._coinbase is None:
            return {"error": "coinbase_unavailable"}
        side_u = str(side).upper()
        amount_usd = float(amount_usd)
        mid = float(mid_price or 0.0)
        if amount_usd <= 0 or mid <= 0:
            return {"error": "invalid_size_or_price"}

        size = max(0.0000001, amount_usd / mid)
        if side_u == "BUY":
            limit_price = mid * 0.9995
        else:
            limit_price = mid * 1.0005

        t0 = time.perf_counter()
        ok = False
        try:
            result = self._coinbase.place_limit_order(
                pair,
                side_u,
                size,
                limit_price,
                post_only=bool(post_only),
                bypass_profit_guard=True,
            )
            order_id = str(result.get("success_response", {}).get("order_id", ""))
            ok = bool(order_id)
            if not ok and "order_id" in result:
                order_id = str(result.get("order_id") or "")
                ok = bool(order_id)
            return {
                "venue": "coinbase",
                "side": side_u,
                "pair": pair,
                "size": size,
                "limit_price": limit_price,
                "success": ok,
                "order_id": order_id,
                "raw": result,
            }
        except Exception as e:
            return {"error": str(e), "venue": "coinbase"}
        finally:
            latency_ms = (time.perf_counter() - t0) * 1000.0
            record_api_call(
                venue="coinbase",
                method="POST",
                endpoint="/orders/limit",
                latency_ms=latency_ms,
                ok=ok,
                status_code=200 if ok else None,
                error_text="" if ok else "coinbase_order_failed",
                context={"pair": pair, "side": side_u, "layer": "low_latency_connector"},
            )

    def place_order_equity(self, symbol, side, quantity):
        if self._ibkr is None:
            return {"error": "ibkr_unavailable"}
        side_u = str(side).upper()
        qty = float(quantity)
        if qty <= 0:
            return {"error": "invalid_quantity"}

        t0 = time.perf_counter()
        ok = False
        try:
            result = self._ibkr.place_order(symbol, side_u, qty)
            order_id = str(result.get("success_response", {}).get("order_id", ""))
            ok = bool(order_id)
            return {
                "venue": "ibkr",
                "symbol": symbol,
                "side": side_u,
                "quantity": qty,
                "success": ok,
                "order_id": order_id,
                "raw": result,
            }
        except Exception as e:
            return {"error": str(e), "venue": "ibkr"}
        finally:
            latency_ms = (time.perf_counter() - t0) * 1000.0
            record_api_call(
                venue="ibkr",
                method="POST",
                endpoint="/orders/market",
                latency_ms=latency_ms,
                ok=ok,
                status_code=200 if ok else None,
                error_text="" if ok else "ibkr_order_failed",
                context={"symbol": symbol, "side": side_u, "layer": "low_latency_connector"},
            )

    def place_order_fix(self, symbol, side, quantity_or_usd, order_type="MARKET"):
        side_u = str(side).upper()
        size = float(quantity_or_usd)
        if not self.allow_fix_stub:
            return {"error": "fix_disabled"}
        if size <= 0:
            return {"error": "invalid_fix_size"}
        if not self.fix_gateway_url:
            return {
                "venue": "fix_stub",
                "success": False,
                "error": "fix_not_configured",
                "symbol": symbol,
                "side": side_u,
                "size": size,
                "hint": "Set FIX_GATEWAY_URL (and FIX_API_KEY if required) to enable real FIX routing.",
            }

        payload = {
            "symbol": str(symbol),
            "side": side_u,
            "size": size,
            "order_type": str(order_type).upper(),
            "timestamp": time.time(),
            "client_tag": "quant-low-latency",
        }
        body = json.dumps(payload).encode()
        headers = {"Content-Type": "application/json", "User-Agent": "NetTrace-FIXBridge/1.0"}
        if self.fix_api_key:
            headers["Authorization"] = f"Bearer {self.fix_api_key}"

        t0 = time.perf_counter()
        ok = False
        status_code = None
        error_text = ""
        try:
            req = urllib.request.Request(
                self.fix_gateway_url,
                data=body,
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=max(0.1, float(self.fix_timeout_ms) / 1000.0)) as resp:
                status_code = int(getattr(resp, "status", 200))
                data = json.loads(resp.read().decode() or "{}")
            order_id = str(
                data.get("order_id")
                or data.get("client_order_id")
                or data.get("id")
                or ""
            )
            ok = bool(order_id) or bool(data.get("accepted", False))
            return {
                "venue": "fix_gateway",
                "success": bool(ok),
                "order_id": order_id,
                "raw": data,
                "symbol": str(symbol),
                "side": side_u,
                "size": size,
                "gateway_url": self.fix_gateway_url,
            }
        except urllib.error.HTTPError as e:
            status_code = int(getattr(e, "code", 0) or 0)
            try:
                error_text = e.read().decode()
            except Exception:
                error_text = str(e)
            return {
                "venue": "fix_gateway",
                "success": False,
                "error": f"http_{status_code}",
                "details": error_text,
                "symbol": str(symbol),
                "side": side_u,
                "size": size,
            }
        except Exception as e:
            error_text = str(e)
            return {
                "venue": "fix_gateway",
                "success": False,
                "error": error_text,
                "symbol": str(symbol),
                "side": side_u,
                "size": size,
            }
        finally:
            latency_ms = (time.perf_counter() - t0) * 1000.0
            record_api_call(
                venue="fix",
                method="POST",
                endpoint=self.fix_gateway_url or "fix_stub",
                latency_ms=latency_ms,
                ok=ok,
                status_code=status_code,
                error_text=error_text,
                context={"symbol": symbol, "side": side_u, "layer": "low_latency_connector"},
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    conn = LowLatencyConnector()
    print(json.dumps(conn.health(), indent=2))
    try:
        print(json.dumps(conn.quote_crypto("BTC-USDC"), indent=2))
    except Exception as e:
        print({"quote_error": str(e)})
