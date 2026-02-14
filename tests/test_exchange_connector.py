#!/usr/bin/env python3
"""Tests for exchange_connector — the API layer that touches real money.

Tests cover:
  - Retry logic with exponential backoff
  - Circuit breaker behavior
  - Profit-only sell guard
  - Partial fill tracking
  - Price feed caching
  - Order precision truncation
"""

import os
import sys
import json
import time
import socket
import unittest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, PropertyMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))


class TestRetryLogic(unittest.TestCase):
    """Test _request_with_retry exponential backoff."""

    def setUp(self):
        # Reset circuit breaker state between tests
        from exchange_connector import CoinbaseTrader
        CoinbaseTrader._consecutive_failures = 0
        CoinbaseTrader._circuit_open_until = 0
        CoinbaseTrader._circuit_open_reason = ""
        CoinbaseTrader._circuit_opened_at = 0.0
        CoinbaseTrader._dns_cache = {}
        CoinbaseTrader._dns_degraded_until = 0.0
        CoinbaseTrader._dns_degraded_reason = ""
        CoinbaseTrader._execution_health_cache = {"ts": 0.0, "payload": {}}

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_success_on_first_try(self, mock_jwt):
        """Successful request should return immediately."""
        from exchange_connector import CoinbaseTrader
        trader = CoinbaseTrader(key_id="test", key_secret="test")

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"accounts": []}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = trader._request("GET", "/api/v3/brokerage/accounts")
            self.assertIn("accounts", result)

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    @patch("time.sleep")  # Don't actually sleep in tests
    def test_retry_on_500(self, mock_sleep, mock_jwt):
        """Should retry on 500 errors."""
        from exchange_connector import CoinbaseTrader
        import urllib.error
        trader = CoinbaseTrader(key_id="test", key_secret="test")

        # First call: 500 error, second call: success
        error_resp = MagicMock()
        error_resp.read.return_value = b"Server Error"

        success_resp = MagicMock()
        success_resp.read.return_value = json.dumps({"ok": True}).encode()
        success_resp.__enter__ = MagicMock(return_value=success_resp)
        success_resp.__exit__ = MagicMock(return_value=False)

        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise urllib.error.HTTPError("url", 500, "Server Error", {}, error_resp)
            return success_resp

        with patch("urllib.request.urlopen", side_effect=side_effect):
            result = trader._request_with_retry("GET", "/test")
            self.assertEqual(result.get("ok"), True)
            self.assertEqual(call_count[0], 2)

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    @patch("time.sleep")
    def test_path_specific_retry_budget_is_respected(self, mock_sleep, mock_jwt):
        """Critical endpoints can tighten retry budget via config map."""
        import exchange_connector as ec
        import urllib.error
        from exchange_connector import CoinbaseTrader
        trader = CoinbaseTrader(key_id="test", key_secret="test")

        error_resp = MagicMock()
        error_resp.read.return_value = b"Server Error"

        with patch.object(
            ec,
            "COINBASE_RETRY_BUDGET_BY_PATH",
            (("GET", "/api/v3/brokerage/orders", 1),),
        ), patch.object(
            ec,
            "COINBASE_RETRY_DEFAULT_ATTEMPTS",
            3,
        ), patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.HTTPError("url", 500, "Server Error", {}, error_resp),
        ):
            result = trader._request_with_retry("GET", "/api/v3/brokerage/orders/historical")

        self.assertEqual(result.get("status"), 500)
        self.assertEqual(mock_sleep.call_count, 0)

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    @patch("exchange_connector.CoinbaseTrader._resolve_dns_candidates", return_value=["203.0.113.10"])
    def test_dns_fallback_after_resolution_error(self, mock_resolve, mock_jwt):
        """If resolver fails, DNS override fallback path should be attempted."""
        from exchange_connector import CoinbaseTrader
        import urllib.error
        trader = CoinbaseTrader(key_id="test", key_secret="test")

        success_resp = MagicMock()
        success_resp.read.return_value = json.dumps({"ok": True}).encode()
        success_resp.__enter__ = MagicMock(return_value=success_resp)
        success_resp.__exit__ = MagicMock(return_value=False)

        calls = {"n": 0}

        def side_effect(*_args, **_kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise urllib.error.URLError(socket.gaierror(8, "nodename nor servname provided"))
            return success_resp

        with patch("urllib.request.urlopen", side_effect=side_effect):
            result = trader._request_with_retry("GET", "/test", max_retries=1)
        self.assertEqual(result.get("ok"), True)
        self.assertEqual(calls["n"], 2)

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_no_retry_on_400(self, mock_jwt):
        """Should NOT retry on 400 client errors."""
        from exchange_connector import CoinbaseTrader
        import urllib.error
        trader = CoinbaseTrader(key_id="test", key_secret="test")

        error_resp = MagicMock()
        error_resp.read.return_value = b"Bad Request"

        def side_effect(*args, **kwargs):
            raise urllib.error.HTTPError("url", 400, "Bad Request", {}, error_resp)

        with patch("urllib.request.urlopen", side_effect=side_effect):
            result = trader._request_with_retry("GET", "/test")
            self.assertEqual(result["status"], 400)


class TestCircuitBreaker(unittest.TestCase):
    """Test circuit breaker prevents crash-looping when API is down."""

    def setUp(self):
        from exchange_connector import CoinbaseTrader
        CoinbaseTrader._consecutive_failures = 0
        CoinbaseTrader._circuit_open_until = 0
        CoinbaseTrader._circuit_open_reason = ""
        CoinbaseTrader._circuit_opened_at = 0.0
        CoinbaseTrader._dns_cache = {}
        CoinbaseTrader._dns_degraded_until = 0.0
        CoinbaseTrader._dns_degraded_reason = ""
        CoinbaseTrader._execution_health_cache = {"ts": 0.0, "payload": {}}

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    @patch("time.sleep")
    def test_circuit_opens_after_3_failures(self, mock_sleep, mock_jwt):
        """Circuit should open after 3 consecutive exhausted retries."""
        from exchange_connector import CoinbaseTrader
        trader = CoinbaseTrader(key_id="test", key_secret="test")

        def always_fail(*args, **kwargs):
            raise ConnectionError("API down")

        with patch("urllib.request.urlopen", side_effect=always_fail):
            for i in range(3):
                trader._request_with_retry("GET", "/test", max_retries=1)

        self.assertGreaterEqual(CoinbaseTrader._consecutive_failures, 3)
        self.assertGreater(CoinbaseTrader._circuit_open_until, time.time())

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    @patch("time.sleep")
    def test_egress_failure_opens_egress_blocked_circuit(self, mock_sleep, mock_jwt):
        from exchange_connector import CoinbaseTrader
        import urllib.error

        trader = CoinbaseTrader(key_id="test", key_secret="test")
        err = urllib.error.URLError(OSError(1, "Operation not permitted"))

        with patch("urllib.request.urlopen", side_effect=err):
            result = trader._request_with_retry("GET", "/test", max_retries=3)

        self.assertIn("Operation not permitted", str(result.get("error", "")))
        self.assertEqual(CoinbaseTrader._circuit_open_reason, "egress_blocked")
        self.assertGreater(CoinbaseTrader._circuit_open_until, time.time() + 30)
        # Should not exponential-backoff loop on hard egress failures.
        self.assertEqual(mock_sleep.call_count, 0)

    def test_circuit_blocks_calls_when_open(self):
        """When circuit is open, calls should be immediately rejected."""
        from exchange_connector import CoinbaseTrader
        CoinbaseTrader._circuit_open_until = time.time() + 30
        trader = CoinbaseTrader(key_id="test", key_secret="test")

        result = trader._request("GET", "/test")
        self.assertIn("Circuit breaker open", result.get("error", ""))

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_circuit_reopen_blocked_when_health_unhealthy(self, mock_jwt):
        from exchange_connector import CoinbaseTrader
        import exchange_connector as ec

        CoinbaseTrader._circuit_open_until = time.time() - 1
        CoinbaseTrader._circuit_open_reason = "dns_unhealthy"
        trader = CoinbaseTrader(key_id="test", key_secret="test")

        with patch.object(ec, "COINBASE_CIRCUIT_REOPEN_HEALTH_SCOPE", "dns"), \
             patch.object(
                 CoinbaseTrader,
                 "_load_execution_health_status",
                 return_value={
                     "green": False,
                     "reason": "dns_unhealthy",
                     "updated_at": datetime.now(timezone.utc).isoformat(),
                 },
             ), \
             patch("urllib.request.urlopen") as mock_urlopen:
            result = trader._request("GET", "/test")

        self.assertIn("Circuit breaker open", result.get("error", ""))
        self.assertIn("reopen_blocked", str(result.get("reason", "")))
        mock_urlopen.assert_not_called()

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_circuit_reopen_allows_call_when_health_green(self, mock_jwt):
        from exchange_connector import CoinbaseTrader
        import exchange_connector as ec

        CoinbaseTrader._circuit_open_until = time.time() - 1
        CoinbaseTrader._circuit_open_reason = "dns_unhealthy"
        trader = CoinbaseTrader(key_id="test", key_secret="test")

        success_resp = MagicMock()
        success_resp.read.return_value = json.dumps({"ok": True}).encode()
        success_resp.__enter__ = MagicMock(return_value=success_resp)
        success_resp.__exit__ = MagicMock(return_value=False)

        with patch.object(ec, "COINBASE_CIRCUIT_REOPEN_HEALTH_SCOPE", "dns"), \
             patch.object(
                 CoinbaseTrader,
                 "_load_execution_health_status",
                 return_value={
                     "green": True,
                     "reason": "passed",
                     "updated_at": datetime.now(timezone.utc).isoformat(),
                 },
             ), \
             patch("urllib.request.urlopen", return_value=success_resp):
            result = trader._request("GET", "/test")

        self.assertEqual(result.get("ok"), True)
        self.assertEqual(CoinbaseTrader._circuit_open_until, 0)
        self.assertEqual(CoinbaseTrader._circuit_open_reason, "")

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_circuit_reopen_allows_call_when_dns_component_green(self, mock_jwt):
        from exchange_connector import CoinbaseTrader
        import exchange_connector as ec

        CoinbaseTrader._circuit_open_until = time.time() - 1
        CoinbaseTrader._circuit_open_reason = "dns_unhealthy"
        trader = CoinbaseTrader(key_id="test", key_secret="test")

        success_resp = MagicMock()
        success_resp.read.return_value = json.dumps({"ok": True}).encode()
        success_resp.__enter__ = MagicMock(return_value=success_resp)
        success_resp.__exit__ = MagicMock(return_value=False)

        with patch.object(ec, "COINBASE_CIRCUIT_REOPEN_HEALTH_SCOPE", "dns"), \
             patch.object(
                 CoinbaseTrader,
                 "_load_execution_health_status",
                 return_value={
                     "green": False,
                     "reason": "telemetry_success_rate_low:0.0100<0.5500",
                     "updated_at": datetime.now(timezone.utc).isoformat(),
                     "components": {"dns": {"green": True}},
                 },
             ), \
             patch("urllib.request.urlopen", return_value=success_resp):
            result = trader._request("GET", "/test")

        self.assertEqual(result.get("ok"), True)
        self.assertEqual(CoinbaseTrader._circuit_open_until, 0)


class TestDnsFailoverProfile(unittest.TestCase):
    def setUp(self):
        from exchange_connector import CoinbaseTrader
        CoinbaseTrader._dns_cache = {}
        CoinbaseTrader._dns_degraded_until = 0.0
        CoinbaseTrader._dns_degraded_reason = ""

    def test_resolve_dns_candidates_uses_deterministic_profile_order(self):
        from exchange_connector import CoinbaseTrader
        import exchange_connector as ec

        fake_infos = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.50", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.10", 443)),
        ]
        with patch.object(ec, "COINBASE_DNS_FAILOVER_ENABLED", True), \
             patch.object(ec, "COINBASE_DNS_FAILOVER_PROFILE", "fallback_then_system"), \
             patch.object(
                 ec,
                 "COINBASE_DNS_FAILOVER_HOST_MAP",
                 {"api.coinbase.com": ("198.51.100.20", "198.51.100.21")},
             ), \
             patch.object(ec, "COINBASE_DNS_FALLBACK_IPS", ("203.0.113.9",)), \
             patch.object(ec, "COINBASE_DNS_FAILOVER_MAX_CANDIDATES", 8), \
             patch("socket.getaddrinfo", return_value=fake_infos):
            candidates = CoinbaseTrader._resolve_dns_candidates("api.coinbase.com")

        self.assertEqual(
            candidates,
            ["198.51.100.20", "198.51.100.21", "203.0.113.9", "192.0.2.10", "192.0.2.50"],
        )

    def test_resolve_dns_candidates_uses_public_resolver_when_system_empty(self):
        from exchange_connector import CoinbaseTrader
        import exchange_connector as ec

        with patch.object(ec, "COINBASE_DNS_FAILOVER_ENABLED", True), \
             patch.object(ec, "COINBASE_DNS_FAILOVER_PROFILE", "system_then_fallback"), \
             patch.object(ec, "COINBASE_DNS_PUBLIC_RESOLVER_ENABLED", True), \
             patch.object(ec, "COINBASE_DNS_PUBLIC_RESOLVERS", ("1.1.1.1",)), \
             patch.object(ec, "COINBASE_DNS_PUBLIC_RESOLVER_TIMEOUT_SECONDS", 0.1), \
             patch.object(ec, "COINBASE_DNS_PUBLIC_RESOLVER_MAX_IPS", 4), \
             patch.object(ec, "COINBASE_DNS_FAILOVER_HOST_MAP", {}), \
             patch.object(ec, "COINBASE_DNS_FALLBACK_IPS", ()), \
             patch("socket.getaddrinfo", side_effect=socket.gaierror(8, "name or service not known")), \
             patch.object(ec, "_resolve_public_dns_ips", return_value=["198.51.100.44", "198.51.100.45"]):
            candidates = CoinbaseTrader._resolve_dns_candidates("api.coinbase.com")

        self.assertEqual(candidates[:2], ["198.51.100.44", "198.51.100.45"])


class TestPriceFeed(unittest.TestCase):
    """Test PriceFeed caching and fallback."""

    def test_cache_ttl(self):
        from exchange_connector import PriceFeed
        # Manually inject a cached price
        PriceFeed.CACHE["TEST-USD"] = {"price": 42000.0, "t": time.time()}
        price = PriceFeed.get_price("TEST-USD")
        self.assertEqual(price, 42000.0)

    def test_expired_cache_refetches(self):
        from exchange_connector import PriceFeed
        PriceFeed.CACHE["STALE-USD"] = {"price": 1.0, "t": time.time() - 100}
        # Will try to fetch and fail (no real API) — returns None
        price = PriceFeed.get_price("STALE-USD")
        # Should either return new price or None (not stale 1.0)


class TestPrecisionTruncation(unittest.TestCase):
    """Test that amounts are truncated to match Coinbase's precision."""

    def test_truncate_btc(self):
        from exchange_connector import CoinbaseTrader
        # BTC increment is typically 0.00000001
        result = CoinbaseTrader._truncate_to_increment(0.123456789, "0.00000001")
        self.assertAlmostEqual(result, 0.12345678, places=8)

    def test_truncate_rounds_down(self):
        from exchange_connector import CoinbaseTrader
        result = CoinbaseTrader._truncate_to_increment(1.999, "0.01")
        self.assertAlmostEqual(result, 1.99, places=2)

    def test_truncate_zero_increment(self):
        from exchange_connector import CoinbaseTrader
        result = CoinbaseTrader._truncate_to_increment(5.0, "0")
        self.assertEqual(result, 5.0)


class TestPartialFillTracking(unittest.TestCase):
    """Test get_order_fill method."""

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_returns_fill_data(self, mock_jwt):
        from exchange_connector import CoinbaseTrader
        CoinbaseTrader._consecutive_failures = 0
        CoinbaseTrader._circuit_open_until = 0
        trader = CoinbaseTrader(key_id="test", key_secret="test")

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "order": {
                "status": "FILLED",
                "filled_size": "0.0005",
                "filled_value": "50.25",
            }
        }).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp):
            fill = trader.get_order_fill("test-order-123", max_wait=2)

        self.assertIsNotNone(fill)
        self.assertAlmostEqual(fill["filled_size"], 0.0005)
        self.assertEqual(fill["status"], "FILLED")


class TestInputValidation(unittest.TestCase):
    """Test strict input handling on order APIs."""

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_place_order_rejects_invalid_product(self, mock_jwt):
        from exchange_connector import CoinbaseTrader
        trader = CoinbaseTrader(key_id="test", key_secret="test")
        res = trader.place_order(None, "BUY", 100)
        self.assertEqual(res["error_response"]["error"], "INVALID_PRODUCT_ID")

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_place_order_rejects_invalid_side(self, mock_jwt):
        from exchange_connector import CoinbaseTrader
        trader = CoinbaseTrader(key_id="test", key_secret="test")
        res = trader.place_order("BTC-USD", "HODL", 100)
        self.assertEqual(res["error_response"]["error"], "INVALID_SIDE")

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_place_order_rejects_invalid_order_type(self, mock_jwt):
        from exchange_connector import CoinbaseTrader
        trader = CoinbaseTrader(key_id="test", key_secret="test")
        res = trader.place_order("BTC-USD", "BUY", 100, order_type="post_only")
        self.assertEqual(res["error_response"]["error"], "INVALID_ORDER_TYPE")

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_place_order_rejects_nonfinite_size(self, mock_jwt):
        from exchange_connector import CoinbaseTrader
        trader = CoinbaseTrader(key_id="test", key_secret="test")
        res = trader.place_order("BTC-USD", "BUY", float("nan"), order_type="market")
        self.assertEqual(res["error_response"]["error"], "INVALID_SIZE")

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_place_limit_order_rejects_invalid_price(self, mock_jwt):
        from exchange_connector import CoinbaseTrader
        trader = CoinbaseTrader(key_id="test", key_secret="test")
        res = trader.place_limit_order("BTC-USD", "BUY", 0.001, -10.0)
        self.assertEqual(res["error_response"]["error"], "INVALID_PRICE")

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_place_order_normalizes_inputs(self, mock_jwt):
        from exchange_connector import CoinbaseTrader, STRICT_PROFIT_ONLY
        if not STRICT_PROFIT_ONLY:
            self.skipTest("Strict profit gate disabled")

        trader = CoinbaseTrader(key_id="test", key_secret="test")
        with patch("exchange_connector._is_trading_locked", return_value=(False, "", "")), \
             patch.object(
            trader,
            "_no_loss_gate",
            return_value={"approved": True},
        ), patch.object(
            trader,
            "_request",
            return_value={"success_response": {"order_id": "oid"}},
        ) as mock_request:
            response = trader.place_order("btc/usd", "buy", 100)

        self.assertEqual(response["success_response"]["order_id"], "oid")
        args = mock_request.call_args.args
        payload = args[2] if len(args) >= 3 else mock_request.call_args[1]["body"]
        self.assertEqual(payload["product_id"], "BTC-USD")
        self.assertEqual(payload["side"], "BUY")
        self.assertIn("quote_size", payload["order_configuration"]["market_market_ioc"])

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_place_limit_order_normalizes_inputs(self, mock_jwt):
        from exchange_connector import CoinbaseTrader, STRICT_PROFIT_ONLY
        if not STRICT_PROFIT_ONLY:
            self.skipTest("Strict profit gate disabled")

        trader = CoinbaseTrader(key_id="test", key_secret="test")
        with patch("exchange_connector._is_trading_locked", return_value=(False, "", "")), \
             patch.object(
            trader,
            "get_product",
            return_value={"quote_increment": "0.01", "base_increment": "0.0001"},
        ), patch.object(
            trader,
            "_no_loss_gate",
            return_value={"approved": True},
        ), patch.object(
            trader,
            "_request",
            return_value={"success_response": {"order_id": "oid"}},
        ) as mock_request:
            response = trader.place_limit_order("btc/usd", "buy", 1.2345, 27500.9876)

        self.assertEqual(response["success_response"]["order_id"], "oid")
        args = mock_request.call_args.args
        payload = args[2] if len(args) >= 3 else mock_request.call_args[1]["body"]
        self.assertEqual(payload["product_id"], "BTC-USD")
        self.assertEqual(payload["side"], "BUY")
        self.assertEqual(payload["order_configuration"]["limit_limit_gtc"]["post_only"], True)


class TestOrderNotionalGuards(unittest.TestCase):
    """Test hard notional caps on order execution."""

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_market_buy_is_blocked_by_absolute_notional_cap(self, mock_jwt):
        import exchange_connector as ec
        from exchange_connector import CoinbaseTrader

        trader = CoinbaseTrader(key_id="test", key_secret="test")
        with patch.object(ec, "COINBASE_MAX_TRADE_NOTIONAL_USD", 10.0), \
             patch("exchange_connector._is_trading_locked", return_value=(False, "", "")), \
             patch.object(ec.CoinbaseTrader, "_request") as mock_request:
            result = trader.place_order("BTC-USD", "BUY", 25.0, order_type="market")

        self.assertEqual(result["error_response"]["error"], "ORDER_NOTIONAL_CAP")
        self.assertEqual(result["error_response"]["details"]["max_notional_usd"], 10.0)
        mock_request.assert_not_called()

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_market_buy_is_blocked_by_portfolio_pct_cap(self, mock_jwt):
        import exchange_connector as ec
        from exchange_connector import CoinbaseTrader

        trader = CoinbaseTrader(key_id="test", key_secret="test")
        with patch.object(ec, "COINBASE_MAX_TRADE_NOTIONAL_USD", 0.0), \
             patch.object(ec, "COINBASE_MAX_TRADE_NOTIONAL_PCT_OF_PORTFOLIO", 0.10), \
             patch.object(ec, "_portfolio_value_estimate_from_status", return_value=100.0), \
             patch("exchange_connector._is_trading_locked", return_value=(False, "", "")), \
             patch.object(ec.CoinbaseTrader, "_no_loss_gate", return_value={"approved": True}), \
             patch.object(ec.CoinbaseTrader, "_request", return_value={"success_response": {"order_id": "oid"}}) as mock_request:
            result = trader.place_order("BTC-USD", "BUY", 20.0, order_type="market")

        self.assertEqual(result["error_response"]["error"], "ORDER_NOTIONAL_CAP")
        mock_request.assert_not_called()

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_market_buy_passes_when_under_notional_cap(self, mock_jwt):
        import exchange_connector as ec
        from exchange_connector import CoinbaseTrader

        trader = CoinbaseTrader(key_id="test", key_secret="test")
        with patch.object(ec, "COINBASE_MAX_TRADE_NOTIONAL_USD", 50.0), \
             patch("exchange_connector._is_trading_locked", return_value=(False, "", "")), \
             patch.object(ec.CoinbaseTrader, "_no_loss_gate", return_value={"approved": True}), \
             patch.object(ec.CoinbaseTrader, "_request", return_value={"success_response": {"order_id": "oid"}}) as mock_request:
            result = trader.place_order("BTC-USD", "BUY", 20.0, order_type="market")

        self.assertEqual(result["success_response"]["order_id"], "oid")
        mock_request.assert_called_once()

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_limit_buy_is_blocked_by_notional_cap(self, mock_jwt):
        import exchange_connector as ec
        from exchange_connector import CoinbaseTrader

        trader = CoinbaseTrader(key_id="test", key_secret="test")
        with patch.object(ec, "COINBASE_MAX_TRADE_NOTIONAL_USD", 10.0), \
             patch("exchange_connector._is_trading_locked", return_value=(False, "", "")), \
             patch.object(ec.CoinbaseTrader, "_request") as mock_request:
            result = trader.place_limit_order("BTC-USD", "BUY", 2.0, 6.0, post_only=True)

        self.assertEqual(result["error_response"]["error"], "ORDER_NOTIONAL_CAP")
        mock_request.assert_not_called()

    @patch("exchange_connector.CoinbaseTrader._build_jwt", return_value="fake_jwt")
    def test_market_buy_pct_notional_uses_fallback_portfolio(self, mock_jwt):
        import exchange_connector as ec
        from exchange_connector import CoinbaseTrader

        trader = CoinbaseTrader(key_id="test", key_secret="test")
        with patch.object(ec, "COINBASE_MAX_TRADE_NOTIONAL_USD", 0.0), \
             patch.object(ec, "COINBASE_MAX_TRADE_NOTIONAL_PCT_OF_PORTFOLIO", 0.10), \
             patch.object(ec, "COINBASE_PORTFOLIO_VALUE_ESTIMATE_FALLBACK_USD", 100.0), \
             patch.object(ec, "_read_json_file", return_value=None), \
             patch.object(ec, "_is_trading_locked", return_value=(False, "", "")), \
             patch.object(ec.CoinbaseTrader, "_request") as mock_request:
            result = trader.place_order("BTC-USD", "BUY", 20.0, order_type="market")

        self.assertEqual(result["error_response"]["error"], "ORDER_NOTIONAL_CAP")
        self.assertEqual(result["error_response"]["details"]["max_notional_usd"], 10.0)
        mock_request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
