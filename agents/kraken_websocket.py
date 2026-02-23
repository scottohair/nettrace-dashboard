#!/usr/bin/env python3
"""Kraken WebSocket v2 connection manager — real-time market data feeds.

Replaces 30-second REST polling with sub-100ms streaming for:
  - Ticker updates (price, volume, spread)
  - Order book snapshots + deltas
  - Individual trade events
  - OHLC candle updates

Public endpoint: wss://ws.kraken.com/v2
Private endpoint: wss://ws-auth.kraken.com/v2

Pair format: BTC/USD (not XBTUSD or BTC-USD)

Implementation uses stdlib only (ssl + socket + struct + threading) with
manual RFC 6455 WebSocket framing.  No external dependencies required.
"""

import base64
import hashlib
import json
import logging
import os
import queue
import socket
import ssl
import struct
import threading
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------------
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"'))

logger = logging.getLogger("kraken_websocket")

# ---------------------------------------------------------------------------
# Kraken WS v2 endpoints
# ---------------------------------------------------------------------------
WS_PUBLIC_URL = "wss://ws.kraken.com/v2"
WS_AUTH_URL = "wss://ws-auth.kraken.com/v2"

# Kraken WS uses BTC/USD format (not XBTUSD or BTC-USD)
KRAKEN_WS_PAIRS = {
    "BTC-USD": "BTC/USD",
    "ETH-USD": "ETH/USD",
    "SOL-USD": "SOL/USD",
    "AVAX-USD": "AVAX/USD",
    "LINK-USD": "LINK/USD",
    "DOGE-USD": "DOGE/USD",
    "XRP-USD": "XRP/USD",
    "ADA-USD": "ADA/USD",
    "DOT-USD": "DOT/USD",
}

# Reverse map for converting WS pair back to standard
_WS_PAIR_REVERSE = {v: k for k, v in KRAKEN_WS_PAIRS.items()}


def ws_pair_to_standard(ws_pair: str) -> str:
    """Convert Kraken WS pair format to standard (BTC/USD -> BTC-USD)."""
    if ws_pair in _WS_PAIR_REVERSE:
        return _WS_PAIR_REVERSE[ws_pair]
    return ws_pair.replace("/", "-")


def standard_to_ws_pair(std_pair: str) -> str:
    """Convert standard pair to Kraken WS format (BTC-USD -> BTC/USD)."""
    if std_pair in KRAKEN_WS_PAIRS:
        return KRAKEN_WS_PAIRS[std_pair]
    return std_pair.replace("-", "/")


# ---------------------------------------------------------------------------
# WebSocket frame helpers (RFC 6455)
# ---------------------------------------------------------------------------

def _build_client_frame(opcode: int, payload: bytes) -> bytes:
    """Build a masked WebSocket frame (client -> server must mask)."""
    frame = bytearray()
    frame.append(0x80 | opcode)  # FIN + opcode

    mask_key = os.urandom(4)
    length = len(payload)

    if length < 126:
        frame.append(0x80 | length)  # Mask bit set
    elif length < 65536:
        frame.append(0x80 | 126)
        frame.extend(struct.pack("!H", length))
    else:
        frame.append(0x80 | 127)
        frame.extend(struct.pack("!Q", length))

    frame.extend(mask_key)
    masked = bytearray(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    frame.extend(masked)
    return bytes(frame)


def _build_text_frame(data: str) -> bytes:
    """Build a masked text frame."""
    return _build_client_frame(0x1, data.encode("utf-8"))


def _build_pong_frame(payload: bytes) -> bytes:
    """Build a masked pong frame."""
    return _build_client_frame(0xA, payload)


def _build_close_frame() -> bytes:
    """Build a masked close frame."""
    return _build_client_frame(0x8, b"")


def parse_frame_header(header_bytes: bytes):
    """Parse the first 2 bytes of a WebSocket frame.

    Returns (opcode, masked, payload_length, extra_bytes_needed).
    extra_bytes_needed is 0, 2, or 8 depending on the length encoding.
    """
    if len(header_bytes) < 2:
        raise ValueError("Need at least 2 bytes for frame header")
    opcode = header_bytes[0] & 0x0F
    masked = (header_bytes[1] & 0x80) != 0
    length = header_bytes[1] & 0x7F

    if length == 126:
        return opcode, masked, None, 2  # need 2 more bytes
    elif length == 127:
        return opcode, masked, None, 8  # need 8 more bytes
    return opcode, masked, length, 0


# ---------------------------------------------------------------------------
# KrakenWebSocketManager
# ---------------------------------------------------------------------------

class KrakenWebSocketManager:
    """Persistent WebSocket connection to Kraken v2 API.

    Features:
        - Auto-reconnect with exponential backoff (1s, 2s, 4s, 8s, max 60s)
        - Heartbeat monitoring (35s timeout > Kraken's 30s heartbeat)
        - Channel subscription management (ticker, book, trade, ohlc)
        - Thread-safe message queue for consumers
        - Callback registration per channel
        - Latest-ticker cache for instant lookups
    """

    def __init__(self, pairs=None, private=False):
        self.pairs = pairs or list(KRAKEN_WS_PAIRS.values())
        self.private = private
        self.ws_url = WS_AUTH_URL if private else WS_PUBLIC_URL

        # Connection state
        self._ssl_sock = None
        self._connected = False
        self._running = False
        self._thread = None
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0

        # Subscriptions: channel -> params dict
        self._subscriptions = {}

        # Callbacks: channel -> list of callables
        self._callbacks = {
            "ticker": [],
            "book": [],
            "trade": [],
            "ohlc": [],
            "spread": [],
            "executions": [],   # private
            "balances": [],     # private
        }

        # Message queue for consumers (bounded to prevent memory leak)
        self.message_queue = queue.Queue(maxsize=10000)

        # Auth token (for private channels)
        self._ws_token = None

        # Latest ticker cache: ws_pair -> ticker_data dict
        self._latest_tickers = {}

        # Stats
        self.messages_received = 0
        self.last_message_time = 0
        self.reconnect_count = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self):
        """Start WebSocket connection in background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="kraken-ws"
        )
        self._thread.start()
        logger.info(
            "KrakenWebSocket started (pairs=%d, private=%s)",
            len(self.pairs), self.private,
        )

    def stop(self):
        """Stop WebSocket connection."""
        self._running = False
        self._close_socket()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("KrakenWebSocket stopped")

    def on_ticker(self, callback):
        """Register ticker update callback: callback(pair, ticker_data)."""
        self._callbacks["ticker"].append(callback)

    def on_book_update(self, callback):
        """Register orderbook update callback: callback(pair, book_data)."""
        self._callbacks["book"].append(callback)

    def on_trade(self, callback):
        """Register trade event callback: callback(pair, trade_data)."""
        self._callbacks["trade"].append(callback)

    def on_ohlc(self, callback):
        """Register OHLC candle callback: callback(pair, ohlc_data)."""
        self._callbacks["ohlc"].append(callback)

    def on_execution(self, callback):
        """Register order execution callback (private): callback(execution_data)."""
        self._callbacks["executions"].append(callback)

    def subscribe(self, channel, params=None):
        """Subscribe to a channel (takes effect on next connect or immediately)."""
        self._subscriptions[channel] = params or {}
        if self._connected:
            self._send_subscribe(channel, params)

    def get_latest_ticker(self, pair):
        """Get latest cached ticker for *pair* (WS format, e.g. BTC/USD).

        Also accepts standard format (BTC-USD) for convenience.
        """
        ws_pair = standard_to_ws_pair(pair) if "/" not in pair else pair
        return self._latest_tickers.get(ws_pair)

    @property
    def is_connected(self):
        return self._connected

    def status(self):
        """Get connection status dict."""
        return {
            "connected": self._connected,
            "messages_received": self.messages_received,
            "last_message_time": self.last_message_time,
            "reconnect_count": self.reconnect_count,
            "pairs": len(self.pairs),
            "private": self.private,
            "cached_tickers": len(self._latest_tickers),
        }

    # ------------------------------------------------------------------
    # Auth token (for private channels)
    # ------------------------------------------------------------------

    def get_ws_token(self):
        """Get WebSocket auth token from Kraken REST API."""
        try:
            from kraken_connector import KrakenConnector
            result = KrakenConnector._private_request("GetWebSocketsToken")
            if result.get("result"):
                self._ws_token = result["result"].get("token")
                logger.info(
                    "Got WebSocket auth token (expires in %ss)",
                    result["result"].get("expires", "?"),
                )
                return self._ws_token
        except Exception as e:
            logger.error("Failed to get WS token: %s", e)
        return None

    # ------------------------------------------------------------------
    # Internal: connection lifecycle
    # ------------------------------------------------------------------

    def _run_loop(self):
        """Main connection loop with auto-reconnect."""
        while self._running:
            try:
                self._connect()
                self._subscribe_all()
                self._read_loop()
            except Exception as e:
                if self._running:
                    logger.warning(
                        "WebSocket error: %s, reconnecting in %.1fs",
                        e, self._reconnect_delay,
                    )
                self.reconnect_count += 1
            finally:
                self._close_socket()

            if self._running:
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_reconnect_delay
                )

    def _connect(self):
        """Establish WebSocket connection via SSL socket + HTTP upgrade."""
        host = "ws-auth.kraken.com" if self.private else "ws.kraken.com"
        path = "/v2"

        # Create SSL socket
        ctx = ssl.create_default_context()
        raw_sock = socket.create_connection((host, 443), timeout=10)
        self._ssl_sock = ctx.wrap_socket(raw_sock, server_hostname=host)

        # WebSocket handshake (RFC 6455)
        ws_key = base64.b64encode(os.urandom(16)).decode()
        handshake = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {ws_key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"\r\n"
        )
        self._ssl_sock.sendall(handshake.encode())

        # Read handshake response
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = self._ssl_sock.recv(4096)
            if not chunk:
                raise ConnectionError("WebSocket handshake failed: no response")
            response += chunk

        status_line = response.split(b"\r\n")[0]
        if b"101" not in status_line:
            raise ConnectionError(
                f"WebSocket handshake failed: {status_line.decode(errors='replace')}"
            )

        self._connected = True
        self._reconnect_delay = 1.0  # Reset backoff on successful connect
        logger.info("WebSocket connected to %s", host)

    def _close_socket(self):
        """Close the socket connection."""
        self._connected = False
        try:
            if self._ssl_sock:
                # Try to send a close frame gracefully
                try:
                    self._ssl_sock.sendall(_build_close_frame())
                except Exception:
                    pass
                self._ssl_sock.close()
        except Exception:
            pass
        self._ssl_sock = None

    # ------------------------------------------------------------------
    # Internal: frame I/O
    # ------------------------------------------------------------------

    def _send_frame(self, data: str):
        """Send a WebSocket text frame."""
        if not self._ssl_sock:
            raise ConnectionError("Not connected")
        self._ssl_sock.sendall(_build_text_frame(data))

    def _recv_exact(self, n):
        """Read exactly *n* bytes from socket."""
        data = bytearray()
        while len(data) < n:
            chunk = self._ssl_sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed while reading")
            data.extend(chunk)
        return bytes(data)

    def _read_frame(self):
        """Read a WebSocket frame. Returns (opcode, payload_bytes)."""
        # Read first 2 bytes (header)
        header = self._recv_exact(2)
        opcode = header[0] & 0x0F
        masked = (header[1] & 0x80) != 0
        length = header[1] & 0x7F

        if length == 126:
            length = struct.unpack("!H", self._recv_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._recv_exact(8))[0]

        if masked:
            mask_key = self._recv_exact(4)
            raw = bytearray(self._recv_exact(length))
            for i in range(len(raw)):
                raw[i] ^= mask_key[i % 4]
            payload = bytes(raw)
        else:
            payload = self._recv_exact(length)

        return opcode, payload

    def _read_loop(self):
        """Read messages until disconnect."""
        # Timeout slightly longer than Kraken's 30-second heartbeat
        self._ssl_sock.settimeout(35)

        while self._running and self._connected:
            try:
                opcode, payload = self._read_frame()

                if opcode == 0x1:  # Text frame
                    self._handle_message(payload.decode("utf-8"))
                elif opcode == 0x8:  # Close frame
                    logger.info("Server sent close frame")
                    break
                elif opcode == 0x9:  # Ping
                    self._ssl_sock.sendall(_build_pong_frame(payload))
                elif opcode == 0xA:  # Pong — ignore
                    pass

            except socket.timeout:
                logger.warning("WebSocket read timeout (35s), reconnecting")
                break
            except Exception as e:
                if self._running:
                    logger.error("WebSocket read error: %s", e)
                break

    # ------------------------------------------------------------------
    # Internal: message handling
    # ------------------------------------------------------------------

    def _handle_message(self, raw_msg):
        """Parse and route incoming Kraken v2 JSON message."""
        self.messages_received += 1
        self.last_message_time = time.time()

        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            logger.debug("Non-JSON message: %s", raw_msg[:200])
            return

        channel = msg.get("channel")
        msg_type = msg.get("type")
        data = msg.get("data")

        # Heartbeat — just ignore
        if channel == "heartbeat":
            return

        # System status
        if channel == "status":
            status_data = msg.get("data")
            if isinstance(status_data, dict):
                logger.info("Kraken WS status: %s", status_data.get("system", "unknown"))
            elif isinstance(status_data, list) and status_data:
                logger.info("Kraken WS status: %s", status_data[0].get("system", "unknown"))
            else:
                logger.info("Kraken WS status message: %s", msg_type)
            return

        # Update ticker cache
        if channel == "ticker" and data:
            items = data if isinstance(data, list) else [data]
            for item in items:
                symbol = item.get("symbol", "")
                if symbol:
                    self._latest_tickers[symbol] = item

        # Route to registered callbacks
        if channel in self._callbacks and data:
            items = data if isinstance(data, list) else [data]
            for item in items:
                pair = item.get("symbol", "")
                for cb in self._callbacks.get(channel, []):
                    try:
                        cb(pair, item)
                    except Exception as e:
                        logger.error("Callback error for %s: %s", channel, e)

        # Also enqueue for poll-style consumers
        try:
            self.message_queue.put_nowait({
                "channel": channel,
                "type": msg_type,
                "data": data,
            })
        except queue.Full:
            pass  # Drop if queue is full — this is expected under heavy load

    # ------------------------------------------------------------------
    # Internal: subscriptions
    # ------------------------------------------------------------------

    def _subscribe_all(self):
        """Subscribe to all default + custom channels."""
        # Default subscriptions for configured pairs
        if self.pairs:
            self._send_subscribe("ticker", {"symbol": self.pairs})
            self._send_subscribe("trade", {"symbol": self.pairs})
            self._send_subscribe("book", {"symbol": self.pairs, "depth": 25})

        # Custom subscriptions added via subscribe()
        for channel, params in self._subscriptions.items():
            self._send_subscribe(channel, params)

        # Private channels (if authenticated)
        if self.private and self._ws_token:
            self._send_subscribe("executions", {"token": self._ws_token})
            self._send_subscribe("balances", {"token": self._ws_token})

    def _send_subscribe(self, channel, params=None):
        """Send a Kraken v2 subscribe message."""
        msg = {
            "method": "subscribe",
            "params": {"channel": channel},
        }
        if params:
            msg["params"].update(params)

        self._send_frame(json.dumps(msg))
        logger.debug("Subscribed to %s", channel)

    def _send_unsubscribe(self, channel, params=None):
        """Send a Kraken v2 unsubscribe message."""
        msg = {
            "method": "unsubscribe",
            "params": {"channel": channel},
        }
        if params:
            msg["params"].update(params)

        self._send_frame(json.dumps(msg))
        logger.debug("Unsubscribed from %s", channel)


# ---------------------------------------------------------------------------
# Module-level test utility
# ---------------------------------------------------------------------------

def test_websocket_connection(timeout=15):
    """Connect to Kraken WS v2 public endpoint, receive up to 5 messages,
    and disconnect.  Returns True on success.

    This makes a real network connection — use for manual verification only,
    NOT in unit tests.
    """
    received = []

    def _on_ticker(pair, data):
        received.append(("ticker", pair, data))

    ws = KrakenWebSocketManager(pairs=["BTC/USD", "ETH/USD"])
    ws.on_ticker(_on_ticker)
    ws.start()

    deadline = time.time() + timeout
    while time.time() < deadline and len(received) < 5:
        time.sleep(0.5)

    ws.stop()

    print(f"Received {len(received)} ticker messages in {timeout}s")
    for kind, pair, data in received[:5]:
        print(f"  [{kind}] {pair}: last={data.get('last', '?')}")

    return len(received) > 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )
    ok = test_websocket_connection()
    print(f"\nWebSocket test {'PASSED' if ok else 'FAILED'}")
