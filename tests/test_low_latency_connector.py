#!/usr/bin/env python3
"""Tests for low-latency connector fetch fallbacks."""

import io
import json
import socket
import unittest
from unittest import mock

from agents import low_latency_connector as llc


class FakeResponse(io.BytesIO):
    def __init__(self, payload):
        super().__init__(payload)
        self.status = 200

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestLowLatencyConnector(unittest.TestCase):
    def test_fetch_json_uses_dns_fallback_candidates_on_network_error(self):
        attempts = []

        class FakeCoinbaseTrader:
            @classmethod
            def _resolve_dns_candidates(cls, host):
                attempts.append(("resolve", host))
                if host == "api.exchange.coinbase.com":
                    return ["203.0.113.7"]
                return []

            @classmethod
            def _urlopen_with_host_override(cls, req, host, ip, timeout):
                attempts.append(("override", host, ip, timeout))
                payload = json.dumps({"ok": True}).encode()
                return FakeResponse(payload)

        def direct_failure(*_args, **_kwargs):
            attempts.append(("direct",))
            raise socket.gaierror(8, "nodename nor servname provided")

        with mock.patch.object(llc, "CoinbaseTrader", FakeCoinbaseTrader), mock.patch.object(
            llc.urllib.request, "urlopen", direct_failure
        ):
            payload = llc._fetch_json(
                "https://api.exchange.coinbase.com/products/BTC-USD/book?level=1", timeout=1.2
            )

        self.assertEqual(payload, {"ok": True})
        self.assertIn(("resolve", "api.exchange.coinbase.com"), attempts)
        self.assertIn(("override", "api.exchange.coinbase.com", "203.0.113.7", 1.2), attempts)

    def test_fetch_json_raises_when_no_fallback_candidates_and_network_error(self):
        class FakeCoinbaseTrader:
            @classmethod
            def _resolve_dns_candidates(cls, _host):
                return []

        def direct_failure(*_args, **_kwargs):
            raise socket.gaierror(8, "nodename or service not known")

        with mock.patch.object(llc, "CoinbaseTrader", FakeCoinbaseTrader), mock.patch.object(
            llc.urllib.request, "urlopen", direct_failure
        ):
            with self.assertRaises(socket.gaierror):
                llc._fetch_json(
                    "https://api.exchange.coinbase.com/products/BTC-USD/book?level=1", timeout=1.0
                )


if __name__ == "__main__":
    unittest.main()
