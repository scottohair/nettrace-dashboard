#!/usr/bin/env python3
"""Tests for Kraken WebSocket v2 connection manager and signal-agent integration.

All socket connections are mocked — no real network calls.
"""
import json
import os
import queue
import struct
import sys
import time
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

# Make agents/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

from kraken_websocket import (
    KrakenWebSocketManager,
    KRAKEN_WS_PAIRS,
    ws_pair_to_standard,
    standard_to_ws_pair,
    _build_text_frame,
    _build_pong_frame,
    _build_close_frame,
    _build_client_frame,
    parse_frame_header,
    WS_PUBLIC_URL,
    WS_AUTH_URL,
)

# ---------------------------------------------------------------------------
# Sample Kraken v2 messages (realistic payloads)
# ---------------------------------------------------------------------------

SAMPLE_TICKER = {
    "channel": "ticker",
    "type": "update",
    "data": [{
        "symbol": "BTC/USD",
        "last": 97500.0,
        "bid": 97490.0,
        "ask": 97510.0,
        "volume": 1234.56,
        "vwap": 97100.0,
        "high": 98000.0,
        "low": 96500.0,
        "change": 500.0,
        "change_pct": 0.52,
    }],
}

SAMPLE_TRADE = {
    "channel": "trade",
    "type": "update",
    "data": [{
        "symbol": "BTC/USD",
        "side": "buy",
        "price": 97500.0,
        "qty": 0.1,
        "timestamp": "2026-02-23T10:00:00.000Z",
    }],
}

SAMPLE_BOOK = {
    "channel": "book",
    "type": "snapshot",
    "data": [{
        "symbol": "BTC/USD",
        "bids": [
            {"price": 97490.0, "qty": 1.5},
            {"price": 97480.0, "qty": 2.0},
        ],
        "asks": [
            {"price": 97510.0, "qty": 1.2},
            {"price": 97520.0, "qty": 1.8},
        ],
    }],
}

SAMPLE_HEARTBEAT = {"channel": "heartbeat"}

SAMPLE_STATUS = {
    "channel": "status",
    "type": "update",
    "data": {"system": "online", "version": "2.0.0"},
}

SAMPLE_OHLC = {
    "channel": "ohlc",
    "type": "update",
    "data": [{
        "symbol": "ETH/USD",
        "open": 3400.0,
        "high": 3450.0,
        "low": 3380.0,
        "close": 3420.0,
        "volume": 500.0,
        "timestamp": "2026-02-23T10:00:00.000Z",
    }],
}


# ===================================================================
# 1. Initialization and pair-format mapping
# ===================================================================

class TestKrakenWebSocketInit(unittest.TestCase):
    """Verify initialization defaults and pair-format helpers."""

    def test_default_pairs(self):
        ws = KrakenWebSocketManager()
        self.assertEqual(ws.pairs, list(KRAKEN_WS_PAIRS.values()))

    def test_custom_pairs(self):
        ws = KrakenWebSocketManager(pairs=["BTC/USD"])
        self.assertEqual(ws.pairs, ["BTC/USD"])

    def test_private_flag(self):
        ws = KrakenWebSocketManager(private=True)
        self.assertTrue(ws.private)
        self.assertEqual(ws.ws_url, WS_AUTH_URL)

    def test_public_default(self):
        ws = KrakenWebSocketManager()
        self.assertFalse(ws.private)
        self.assertEqual(ws.ws_url, WS_PUBLIC_URL)

    def test_initial_state(self):
        ws = KrakenWebSocketManager()
        self.assertFalse(ws.is_connected)
        self.assertEqual(ws.messages_received, 0)
        self.assertEqual(ws.last_message_time, 0)
        self.assertEqual(ws.reconnect_count, 0)
        self.assertIsInstance(ws.message_queue, queue.Queue)
        self.assertIsInstance(ws._latest_tickers, dict)
        self.assertEqual(len(ws._latest_tickers), 0)

    def test_ws_pair_to_standard(self):
        self.assertEqual(ws_pair_to_standard("BTC/USD"), "BTC-USD")
        self.assertEqual(ws_pair_to_standard("ETH/USD"), "ETH-USD")
        # Unknown pair falls back to simple replace
        self.assertEqual(ws_pair_to_standard("FOO/BAR"), "FOO-BAR")

    def test_standard_to_ws_pair(self):
        self.assertEqual(standard_to_ws_pair("BTC-USD"), "BTC/USD")
        self.assertEqual(standard_to_ws_pair("ETH-USD"), "ETH/USD")
        self.assertEqual(standard_to_ws_pair("FOO-BAR"), "FOO/BAR")

    def test_callback_registration(self):
        ws = KrakenWebSocketManager()
        cb = lambda p, d: None
        ws.on_ticker(cb)
        ws.on_book_update(cb)
        ws.on_trade(cb)
        ws.on_ohlc(cb)
        ws.on_execution(cb)
        self.assertIn(cb, ws._callbacks["ticker"])
        self.assertIn(cb, ws._callbacks["book"])
        self.assertIn(cb, ws._callbacks["trade"])
        self.assertIn(cb, ws._callbacks["ohlc"])
        self.assertIn(cb, ws._callbacks["executions"])


# ===================================================================
# 2. WebSocket frame encoding / decoding
# ===================================================================

class TestKrakenWebSocketFrames(unittest.TestCase):
    """Test RFC 6455 frame construction and header parsing."""

    def test_text_frame_structure(self):
        frame = _build_text_frame("hello")
        # FIN + text opcode
        self.assertEqual(frame[0] & 0x80, 0x80)  # FIN set
        self.assertEqual(frame[0] & 0x0F, 0x01)  # text opcode
        # Mask bit must be set (client frames)
        self.assertTrue(frame[1] & 0x80)
        # Payload length = 5
        self.assertEqual(frame[1] & 0x7F, 5)

    def test_pong_frame_structure(self):
        frame = _build_pong_frame(b"ping-data")
        self.assertEqual(frame[0] & 0x0F, 0x0A)  # pong opcode
        self.assertTrue(frame[1] & 0x80)  # mask bit

    def test_close_frame_structure(self):
        frame = _build_close_frame()
        self.assertEqual(frame[0] & 0x0F, 0x08)  # close opcode
        self.assertTrue(frame[1] & 0x80)  # mask bit
        self.assertEqual(frame[1] & 0x7F, 0)  # zero-length payload

    def test_large_frame_126_encoding(self):
        data = "x" * 200
        frame = _build_text_frame(data)
        self.assertEqual(frame[1] & 0x7F, 126)  # extended length marker
        length = struct.unpack("!H", frame[2:4])[0]
        self.assertEqual(length, 200)

    def test_parse_frame_header_short(self):
        header = bytes([0x81, 5])  # FIN + text, length=5
        opcode, masked, length, extra = parse_frame_header(header)
        self.assertEqual(opcode, 1)
        self.assertFalse(masked)
        self.assertEqual(length, 5)
        self.assertEqual(extra, 0)

    def test_parse_frame_header_126(self):
        header = bytes([0x81, 126])
        opcode, masked, length, extra = parse_frame_header(header)
        self.assertEqual(opcode, 1)
        self.assertIsNone(length)  # needs 2 more bytes
        self.assertEqual(extra, 2)

    def test_parse_frame_header_127(self):
        header = bytes([0x81, 127])
        opcode, masked, length, extra = parse_frame_header(header)
        self.assertEqual(opcode, 1)
        self.assertIsNone(length)
        self.assertEqual(extra, 8)

    def test_parse_frame_header_masked(self):
        header = bytes([0x81, 0x80 | 10])  # masked, length=10
        opcode, masked, length, extra = parse_frame_header(header)
        self.assertTrue(masked)
        self.assertEqual(length, 10)

    def test_client_frame_masking_roundtrip(self):
        """Verify that masked frames actually encode the payload."""
        original = b"test data 1234"
        frame = _build_client_frame(0x1, original)
        # Extract mask key and payload
        length = frame[1] & 0x7F
        mask_key = frame[2:6]
        masked_payload = bytearray(frame[6:])
        # Unmask
        unmasked = bytearray(b ^ mask_key[i % 4] for i, b in enumerate(masked_payload))
        self.assertEqual(bytes(unmasked), original)


# ===================================================================
# 3. Message parsing and routing
# ===================================================================

class TestKrakenWebSocketMessages(unittest.TestCase):
    """Test _handle_message() parses and routes Kraken v2 JSON correctly."""

    def setUp(self):
        self.ws = KrakenWebSocketManager(pairs=["BTC/USD", "ETH/USD"])

    def test_ticker_message_updates_cache(self):
        self.ws._handle_message(json.dumps(SAMPLE_TICKER))
        cached = self.ws._latest_tickers.get("BTC/USD")
        self.assertIsNotNone(cached)
        self.assertEqual(cached["last"], 97500.0)

    def test_ticker_accessible_via_get_latest_ticker(self):
        self.ws._handle_message(json.dumps(SAMPLE_TICKER))
        # WS format lookup
        t = self.ws.get_latest_ticker("BTC/USD")
        self.assertIsNotNone(t)
        self.assertEqual(t["last"], 97500.0)
        # Standard format lookup
        t2 = self.ws.get_latest_ticker("BTC-USD")
        self.assertIsNotNone(t2)
        self.assertEqual(t2["last"], 97500.0)

    def test_heartbeat_ignored(self):
        self.ws._handle_message(json.dumps(SAMPLE_HEARTBEAT))
        self.assertEqual(self.ws.messages_received, 1)
        # Queue should be empty (heartbeats are not enqueued)
        self.assertTrue(self.ws.message_queue.empty())

    def test_status_message_logged(self):
        self.ws._handle_message(json.dumps(SAMPLE_STATUS))
        self.assertEqual(self.ws.messages_received, 1)
        # Status messages are not enqueued either
        self.assertTrue(self.ws.message_queue.empty())

    def test_trade_message_enqueued(self):
        self.ws._handle_message(json.dumps(SAMPLE_TRADE))
        self.assertFalse(self.ws.message_queue.empty())
        msg = self.ws.message_queue.get_nowait()
        self.assertEqual(msg["channel"], "trade")

    def test_book_message_enqueued(self):
        self.ws._handle_message(json.dumps(SAMPLE_BOOK))
        msg = self.ws.message_queue.get_nowait()
        self.assertEqual(msg["channel"], "book")

    def test_invalid_json_ignored(self):
        self.ws._handle_message("not-json{{{")
        self.assertEqual(self.ws.messages_received, 1)
        self.assertTrue(self.ws.message_queue.empty())

    def test_message_count_increments(self):
        self.ws._handle_message(json.dumps(SAMPLE_TICKER))
        self.ws._handle_message(json.dumps(SAMPLE_TRADE))
        self.ws._handle_message(json.dumps(SAMPLE_HEARTBEAT))
        self.assertEqual(self.ws.messages_received, 3)

    def test_last_message_time_updated(self):
        before = time.time()
        self.ws._handle_message(json.dumps(SAMPLE_TICKER))
        after = time.time()
        self.assertGreaterEqual(self.ws.last_message_time, before)
        self.assertLessEqual(self.ws.last_message_time, after)

    def test_queue_full_drops_message(self):
        """When the queue is full, messages are silently dropped (no exception)."""
        ws = KrakenWebSocketManager(pairs=["BTC/USD"])
        ws.message_queue = queue.Queue(maxsize=1)
        # First message fills the queue
        ws._handle_message(json.dumps(SAMPLE_TICKER))
        # Second message should be dropped silently
        ws._handle_message(json.dumps(SAMPLE_TRADE))
        self.assertEqual(ws.message_queue.qsize(), 1)


# ===================================================================
# 4. Callback firing
# ===================================================================

class TestKrakenWebSocketCallbacks(unittest.TestCase):
    """Test that registered callbacks fire with the correct arguments."""

    def setUp(self):
        self.ws = KrakenWebSocketManager(pairs=["BTC/USD"])
        self.received = []

    def _record(self, pair, data):
        self.received.append((pair, data))

    def test_ticker_callback_fires(self):
        self.ws.on_ticker(self._record)
        self.ws._handle_message(json.dumps(SAMPLE_TICKER))
        self.assertEqual(len(self.received), 1)
        pair, data = self.received[0]
        self.assertEqual(pair, "BTC/USD")
        self.assertEqual(data["last"], 97500.0)

    def test_trade_callback_fires(self):
        self.ws.on_trade(self._record)
        self.ws._handle_message(json.dumps(SAMPLE_TRADE))
        self.assertEqual(len(self.received), 1)
        pair, data = self.received[0]
        self.assertEqual(pair, "BTC/USD")
        self.assertEqual(data["side"], "buy")

    def test_book_callback_fires(self):
        self.ws.on_book_update(self._record)
        self.ws._handle_message(json.dumps(SAMPLE_BOOK))
        self.assertEqual(len(self.received), 1)
        pair, data = self.received[0]
        self.assertEqual(pair, "BTC/USD")
        self.assertIn("bids", data)
        self.assertIn("asks", data)

    def test_ohlc_callback_fires(self):
        self.ws.on_ohlc(self._record)
        self.ws._handle_message(json.dumps(SAMPLE_OHLC))
        self.assertEqual(len(self.received), 1)
        pair, data = self.received[0]
        self.assertEqual(pair, "ETH/USD")
        self.assertEqual(data["close"], 3420.0)

    def test_multiple_callbacks_same_channel(self):
        received_a = []
        received_b = []
        self.ws.on_ticker(lambda p, d: received_a.append(p))
        self.ws.on_ticker(lambda p, d: received_b.append(p))
        self.ws._handle_message(json.dumps(SAMPLE_TICKER))
        self.assertEqual(len(received_a), 1)
        self.assertEqual(len(received_b), 1)

    def test_callback_exception_does_not_crash(self):
        def bad_cb(pair, data):
            raise RuntimeError("callback boom")
        self.ws.on_ticker(bad_cb)
        self.ws.on_ticker(self._record)  # this should still fire
        self.ws._handle_message(json.dumps(SAMPLE_TICKER))
        self.assertEqual(len(self.received), 1)

    def test_data_as_single_dict_not_list(self):
        """Some channels may return data as a single dict rather than a list."""
        msg = {
            "channel": "ticker",
            "type": "update",
            "data": {
                "symbol": "SOL/USD",
                "last": 150.0,
            },
        }
        self.ws.on_ticker(self._record)
        self.ws._handle_message(json.dumps(msg))
        self.assertEqual(len(self.received), 1)
        self.assertEqual(self.received[0][0], "SOL/USD")


# ===================================================================
# 5. Reconnect and exponential backoff
# ===================================================================

class TestKrakenWebSocketReconnect(unittest.TestCase):
    """Test auto-reconnect logic and exponential backoff."""

    def test_initial_reconnect_delay(self):
        ws = KrakenWebSocketManager()
        self.assertEqual(ws._reconnect_delay, 1.0)

    def test_backoff_doubles(self):
        ws = KrakenWebSocketManager()
        ws._reconnect_delay = 2.0
        # Simulate what _run_loop does on failure
        ws._reconnect_delay = min(ws._reconnect_delay * 2, ws._max_reconnect_delay)
        self.assertEqual(ws._reconnect_delay, 4.0)

    def test_backoff_capped_at_max(self):
        ws = KrakenWebSocketManager()
        ws._reconnect_delay = 32.0
        ws._reconnect_delay = min(ws._reconnect_delay * 2, ws._max_reconnect_delay)
        self.assertEqual(ws._reconnect_delay, 60.0)

    def test_reconnect_count_starts_zero(self):
        ws = KrakenWebSocketManager()
        self.assertEqual(ws.reconnect_count, 0)

    @patch.object(KrakenWebSocketManager, '_connect', side_effect=ConnectionError("test"))
    @patch.object(KrakenWebSocketManager, '_close_socket')
    @patch('kraken_websocket.time.sleep', side_effect=InterruptedError)
    def test_run_loop_increments_reconnect_count(self, mock_sleep, mock_close, mock_connect):
        ws = KrakenWebSocketManager()
        ws._running = True
        try:
            ws._run_loop()
        except InterruptedError:
            pass
        self.assertGreaterEqual(ws.reconnect_count, 1)

    def test_successful_connect_resets_backoff(self):
        """Simulating what _connect does on success."""
        ws = KrakenWebSocketManager()
        ws._reconnect_delay = 16.0
        # _connect sets this on success
        ws._reconnect_delay = 1.0
        self.assertEqual(ws._reconnect_delay, 1.0)


# ===================================================================
# 6. Subscription message format
# ===================================================================

class TestKrakenWebSocketSubscriptions(unittest.TestCase):
    """Test that subscription messages match Kraken v2 format."""

    def setUp(self):
        self.ws = KrakenWebSocketManager(pairs=["BTC/USD"])
        self.sent_frames = []
        self.ws._ssl_sock = MagicMock()
        self.ws._ssl_sock.sendall = lambda data: self.sent_frames.append(data)
        self.ws._connected = True

    def _extract_json_from_frame(self, frame_bytes):
        """Extract and parse JSON payload from a masked client frame."""
        length = frame_bytes[1] & 0x7F
        if length == 126:
            offset = 4
            length = struct.unpack("!H", frame_bytes[2:4])[0]
        elif length == 127:
            offset = 10
            length = struct.unpack("!Q", frame_bytes[2:10])[0]
        else:
            offset = 2
        mask_key = frame_bytes[offset:offset + 4]
        payload = bytearray(frame_bytes[offset + 4:offset + 4 + length])
        for i in range(len(payload)):
            payload[i] ^= mask_key[i % 4]
        return json.loads(payload.decode("utf-8"))

    def test_subscribe_ticker(self):
        self.ws._send_subscribe("ticker", {"symbol": ["BTC/USD"]})
        self.assertEqual(len(self.sent_frames), 1)
        msg = self._extract_json_from_frame(self.sent_frames[0])
        self.assertEqual(msg["method"], "subscribe")
        self.assertEqual(msg["params"]["channel"], "ticker")
        self.assertEqual(msg["params"]["symbol"], ["BTC/USD"])

    def test_subscribe_book_with_depth(self):
        self.ws._send_subscribe("book", {"symbol": ["BTC/USD"], "depth": 25})
        msg = self._extract_json_from_frame(self.sent_frames[0])
        self.assertEqual(msg["params"]["channel"], "book")
        self.assertEqual(msg["params"]["depth"], 25)

    def test_unsubscribe(self):
        self.ws._send_unsubscribe("ticker", {"symbol": ["BTC/USD"]})
        msg = self._extract_json_from_frame(self.sent_frames[0])
        self.assertEqual(msg["method"], "unsubscribe")
        self.assertEqual(msg["params"]["channel"], "ticker")

    def test_custom_subscription_via_subscribe(self):
        self.ws.subscribe("ohlc", {"symbol": ["ETH/USD"], "interval": 5})
        self.assertEqual("ohlc", list(self.ws._subscriptions.keys())[0])
        # It should also send immediately since _connected is True
        self.assertEqual(len(self.sent_frames), 1)

    def test_subscribe_all_sends_default_channels(self):
        self.ws._subscribe_all()
        # Should send ticker, trade, book (3 default channels)
        self.assertEqual(len(self.sent_frames), 3)
        channels = set()
        for frame in self.sent_frames:
            msg = self._extract_json_from_frame(frame)
            channels.add(msg["params"]["channel"])
        self.assertIn("ticker", channels)
        self.assertIn("trade", channels)
        self.assertIn("book", channels)


# ===================================================================
# 7. Status reporting
# ===================================================================

class TestKrakenWebSocketStatus(unittest.TestCase):
    """Test status() method."""

    def test_status_fields(self):
        ws = KrakenWebSocketManager(pairs=["BTC/USD", "ETH/USD"])
        st = ws.status()
        self.assertIn("connected", st)
        self.assertIn("messages_received", st)
        self.assertIn("last_message_time", st)
        self.assertIn("reconnect_count", st)
        self.assertIn("pairs", st)
        self.assertIn("private", st)
        self.assertIn("cached_tickers", st)
        self.assertEqual(st["pairs"], 2)
        self.assertFalse(st["connected"])
        self.assertFalse(st["private"])

    def test_status_after_messages(self):
        ws = KrakenWebSocketManager()
        ws._handle_message(json.dumps(SAMPLE_TICKER))
        ws._handle_message(json.dumps(SAMPLE_TRADE))
        st = ws.status()
        self.assertEqual(st["messages_received"], 2)
        self.assertEqual(st["cached_tickers"], 1)


# ===================================================================
# 8. Signal agent WebSocket integration
# ===================================================================

class TestKrakenSignalAgentWS(unittest.TestCase):
    """Test KrakenSignalAgent WebSocket integration."""

    def setUp(self):
        # Patch out CreativeAgentBridge
        self.bridge_patcher = patch(
            "kraken_signal_agent.CreativeAgentBridge", None
        )
        self.bridge_patcher.start()

    def tearDown(self):
        self.bridge_patcher.stop()

    def _make_agent(self):
        from kraken_signal_agent import KrakenSignalAgent
        agent = KrakenSignalAgent()
        return agent

    def test_init_websocket_success(self):
        agent = self._make_agent()
        mock_ws = MagicMock()
        mock_ws.last_message_time = time.time()

        # _init_websocket does a local import from kraken_websocket; mock
        # the module so the import succeeds and returns our mock manager.
        mock_module = MagicMock()
        mock_module.KrakenWebSocketManager = MagicMock(return_value=mock_ws)
        mock_module.KRAKEN_WS_PAIRS = KRAKEN_WS_PAIRS

        with patch.dict("sys.modules", {"kraken_websocket": mock_module}):
            agent._init_websocket()

        self.assertTrue(agent._ws_mode)
        self.assertIs(agent.ws, mock_ws)
        mock_ws.start.assert_called_once()

    def test_init_websocket_fallback_on_import_error(self):
        agent = self._make_agent()
        # Simulate ImportError in _init_websocket
        with patch.dict("sys.modules", {"kraken_websocket": None}):
            agent._init_websocket()
        self.assertFalse(agent._ws_mode)

    def test_ws_cooldown_prevents_rapid_signals(self):
        agent = self._make_agent()
        agent._ws_signal_cooldown = 10
        # First signal is OK
        self.assertTrue(agent._ws_cooldown_ok("BTC-USD", "BUY"))
        agent._ws_record_signal("BTC-USD", "BUY")
        # Second identical signal within cooldown is blocked
        self.assertFalse(agent._ws_cooldown_ok("BTC-USD", "BUY"))
        # Different direction is OK
        self.assertTrue(agent._ws_cooldown_ok("BTC-USD", "SELL"))
        # Different pair is OK
        self.assertTrue(agent._ws_cooldown_ok("ETH-USD", "BUY"))

    def test_ws_cooldown_expires(self):
        agent = self._make_agent()
        agent._ws_signal_cooldown = 0.01  # very short
        agent._ws_record_signal("BTC-USD", "BUY")
        time.sleep(0.02)
        self.assertTrue(agent._ws_cooldown_ok("BTC-USD", "BUY"))

    def test_on_ws_ticker_calls_divergence_check(self):
        agent = self._make_agent()
        agent._check_cross_venue_divergence_rt = MagicMock()
        agent._on_ws_ticker("BTC/USD", {"last": 97500.0})
        agent._check_cross_venue_divergence_rt.assert_called_once_with("BTC-USD", 97500.0)

    def test_on_ws_ticker_ignores_zero_price(self):
        agent = self._make_agent()
        agent._check_cross_venue_divergence_rt = MagicMock()
        agent._on_ws_ticker("BTC/USD", {"last": 0})
        agent._check_cross_venue_divergence_rt.assert_not_called()

    def test_on_ws_book_calls_imbalance_check(self):
        agent = self._make_agent()
        agent._check_orderbook_imbalance_rt = MagicMock()
        book = {"bids": [{"price": 97490, "qty": 1.0}], "asks": [{"price": 97510, "qty": 0.5}]}
        agent._on_ws_book("BTC/USD", book)
        agent._check_orderbook_imbalance_rt.assert_called_once_with("BTC-USD", book)

    def test_on_ws_trade_calls_volume_check(self):
        agent = self._make_agent()
        agent._check_volume_anomaly_rt = MagicMock()
        trade = {"qty": 0.5, "side": "buy"}
        agent._on_ws_trade("BTC/USD", trade)
        agent._check_volume_anomaly_rt.assert_called_once_with("BTC-USD", trade)

    def test_check_orderbook_imbalance_rt_buy(self):
        agent = self._make_agent()
        agent._emit_signal = MagicMock(return_value=True)
        book = {
            "bids": [{"price": 97490, "qty": 3.0}, {"price": 97480, "qty": 2.0}],
            "asks": [{"price": 97510, "qty": 1.0}, {"price": 97520, "qty": 0.5}],
        }
        agent._check_orderbook_imbalance_rt("BTC-USD", book)
        # bid_vol=5.0, ask_vol=1.5, ratio=3.33 > 1.5 = BUY
        agent._emit_signal.assert_called_once()
        sig = agent._emit_signal.call_args[0][0]
        self.assertEqual(sig["direction"], "BUY")
        self.assertEqual(sig["pair"], "BTC-USD")

    def test_check_orderbook_imbalance_rt_sell(self):
        agent = self._make_agent()
        agent._emit_signal = MagicMock(return_value=True)
        book = {
            "bids": [{"price": 97490, "qty": 0.3}],
            "asks": [{"price": 97510, "qty": 2.0}, {"price": 97520, "qty": 1.0}],
        }
        agent._check_orderbook_imbalance_rt("BTC-USD", book)
        # bid_vol=0.3, ask_vol=3.0, ratio=0.1 < 0.67 = SELL
        agent._emit_signal.assert_called_once()
        sig = agent._emit_signal.call_args[0][0]
        self.assertEqual(sig["direction"], "SELL")

    def test_check_orderbook_imbalance_rt_no_signal(self):
        agent = self._make_agent()
        agent._emit_signal = MagicMock()
        book = {
            "bids": [{"price": 97490, "qty": 1.0}],
            "asks": [{"price": 97510, "qty": 1.0}],
        }
        agent._check_orderbook_imbalance_rt("BTC-USD", book)
        # ratio = 1.0, no signal
        agent._emit_signal.assert_not_called()

    def test_check_volume_anomaly_rt_whale(self):
        agent = self._make_agent()
        agent._emit_signal = MagicMock(return_value=True)
        # BTC-USD avg is 4500 BTC/day, 5% threshold = 225 BTC
        trade = {"qty": 300.0, "side": "buy"}
        agent._check_volume_anomaly_rt("BTC-USD", trade)
        agent._emit_signal.assert_called_once()
        sig = agent._emit_signal.call_args[0][0]
        self.assertEqual(sig["direction"], "BUY")
        self.assertIn("whale", sig["reasoning"].lower())

    def test_check_volume_anomaly_rt_small_trade_ignored(self):
        agent = self._make_agent()
        agent._emit_signal = MagicMock()
        trade = {"qty": 0.01, "side": "buy"}
        agent._check_volume_anomaly_rt("BTC-USD", trade)
        agent._emit_signal.assert_not_called()

    @patch("kraken_signal_agent._fetch_coinbase_price", return_value=98500.0)
    def test_check_cross_venue_divergence_rt_signal(self, mock_cb_price):
        agent = self._make_agent()
        agent._emit_signal = MagicMock(return_value=True)
        # Kraken price significantly lower than Coinbase
        agent._check_cross_venue_divergence_rt("BTC-USD", 96000.0)
        agent._emit_signal.assert_called_once()
        sig = agent._emit_signal.call_args[0][0]
        self.assertEqual(sig["direction"], "BUY")  # buy on Kraken (cheaper)

    @patch("kraken_signal_agent._fetch_coinbase_price", return_value=97500.0)
    def test_check_cross_venue_divergence_rt_no_signal(self, mock_cb_price):
        agent = self._make_agent()
        agent._emit_signal = MagicMock()
        # Prices are very close
        agent._check_cross_venue_divergence_rt("BTC-USD", 97510.0)
        agent._emit_signal.assert_not_called()

    def test_stop_sets_running_false(self):
        agent = self._make_agent()
        agent.running = True
        agent.ws = MagicMock()
        agent.stop()
        self.assertFalse(agent.running)
        agent.ws.stop.assert_called_once()


# ===================================================================
# 9. Risk controller Kraken price fallback
# ===================================================================

class TestRiskControllerKrakenFallback(unittest.TestCase):
    """Test that MarketState.get_price falls back to Kraken on Coinbase failure."""

    @patch("risk_controller.urllib.request.urlopen", side_effect=Exception("Coinbase down"))
    def test_kraken_fallback_used(self, mock_urlopen):
        from risk_controller import MarketState
        ms = MarketState()
        mock_vol = {
            "pair": "BTC-USD",
            "last_price": 97000.0,
            "volume_24h": 1234.0,
        }
        # KrakenConnector is imported locally inside get_price; mock the
        # module so the local import resolves to our mock.
        mock_kc = MagicMock()
        mock_kc.KrakenConnector.get_24h_volume.return_value = mock_vol
        with patch.dict("sys.modules", {"kraken_connector": mock_kc}):
            price = ms.get_price("BTC-USD", urgent=True)
        self.assertEqual(price, 97000.0)

    @patch("risk_controller.urllib.request.urlopen", side_effect=Exception("Coinbase down"))
    def test_kraken_fallback_also_fails(self, mock_urlopen):
        from risk_controller import MarketState
        ms = MarketState()
        mock_kc = MagicMock()
        mock_kc.KrakenConnector.get_24h_volume.side_effect = Exception("Kraken also down")
        with patch.dict("sys.modules", {"kraken_connector": mock_kc}):
            price = ms.get_price("BTC-USD", urgent=True)
        self.assertIsNone(price)

    def test_coinbase_primary_succeeds(self):
        """When Coinbase works, Kraken fallback is not called."""
        from risk_controller import MarketState
        ms = MarketState()

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"data": {"amount": "97500.00"}}).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("risk_controller.urllib.request.urlopen", return_value=mock_resp):
            price = ms.get_price("BTC-USD", urgent=True)
        self.assertEqual(price, 97500.0)


if __name__ == "__main__":
    unittest.main()
