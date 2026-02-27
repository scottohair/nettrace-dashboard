#!/usr/bin/env python3
"""Kraken API Connector — data access and authenticated trading.

Provides access to:
  - Funding rates (for liquidation hunting)
  - Open interest (for leverage estimation)
  - Order book depth (for execution planning)
  - Recent trades (for microstructure analysis)
  - Authenticated trading (order placement, cancellation, balances)

Trading is gated through GoalValidator (70% confidence, 2+ signals).
BE A MAKER: limit orders by default (Kraken maker fee 0.16%).
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

try:
    from agent_goals import GoalValidator
except ImportError:
    GoalValidator = None

logger = logging.getLogger("kraken_connector")

# Load .env if present
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"'))

# Load credentials from environment (set via flyctl secrets)
API_KEY = os.environ.get("KRAKEN_API_KEY", "")
PRIVATE_KEY = os.environ.get("KRAKEN_PRIVATE_KEY", "")
API_URL = "https://api.kraken.com"
API_VERSION = "0"

# Trade tracking database
KRAKEN_TRADE_DB = Path(__file__).parent / "kraken_trades.db"

# Kraken pair mappings — these are the API pair parameter names (not asset codes)
# Kraken uses "XBT" in pair names for Bitcoin (not "BTC" or "XXBT")
# Result keys come back differently (e.g., "XXBTZUSD") — we handle that in lookups
KRAKEN_PAIRS = {
    "BTC": "XBT",   # Bitcoin: API pair = XBTUSD, result key = XXBTZUSD
    "ETH": "ETH",   # Ethereum: API pair = ETHUSD, result key = XETHZUSD
    "SOL": "SOL",
    "AVAX": "AVAX",
    "LINK": "LINK",
    "DOGE": "XDG",
    "XDG": "XDG",
    "XRP": "XRP",
    "ADA": "ADA",
    "DOT": "DOT",
}


def _get_kraken_pair(symbol: str) -> str:
    """Map standard symbol to Kraken API pair prefix (e.g., BTC -> XBT)."""
    return KRAKEN_PAIRS.get(symbol, symbol)


def _first_result(result: dict, fallback_key: str = ""):
    """Extract the first value from a Kraken result dict.

    Kraken returns result keys that differ from query pair names
    (e.g., query XBTUSD → result key XXBTZUSD). This helper grabs
    whatever the first (and usually only) key is.
    """
    if not result or not isinstance(result, dict):
        return {} if not fallback_key else []
    # Try exact key first, then first available
    if fallback_key and fallback_key in result:
        return result[fallback_key]
    for key in result:
        if key != "last":  # skip metadata keys
            return result[key]
    return {} if not fallback_key else []


def _sign_request(endpoint: str, data: dict, nonce: str) -> tuple:
    """Sign Kraken private API request."""
    postdata = urllib.parse.urlencode(data)
    encoded = (str(nonce) + postdata).encode()
    message = endpoint.encode() + hashlib.sha256(encoded).digest()

    signature = hmac.new(
        base64.b64decode(PRIVATE_KEY),
        message,
        hashlib.sha512
    )
    return signature.digest(), postdata


def _init_trade_db():
    """Initialize Kraken trade tracking database."""
    db = sqlite3.connect(str(KRAKEN_TRADE_DB))
    db.execute("""
        CREATE TABLE IF NOT EXISTS kraken_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            side TEXT NOT NULL,
            volume REAL NOT NULL,
            price REAL,
            order_type TEXT DEFAULT 'limit',
            txid TEXT,
            status TEXT DEFAULT 'pending',
            confidence REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.commit()
    return db


class KrakenConnector:
    """Kraken API connector — data access and authenticated trading."""

    @staticmethod
    def get_funding_rate(pair: str) -> dict:
        """Get funding rate for perpetuals pair (if available).

        Kraken uses a different model than Binance — returns interest rates
        instead of traditional funding rates. But useful for leverage cost estimation.
        """
        try:
            if not API_KEY or not PRIVATE_KEY:
                logger.warning("Kraken API keys not configured")
                return {"error": "Credentials not set"}

            # For now, return placeholder (Kraken doesn't expose perpetual funding rates
            # in the same way Binance does — we'd need to use their margin interest rates)
            # This is a limitation of Kraken's API structure
            logger.info(f"Funding rate check for {pair}: Kraken uses margin interest, not perpetual funding")
            return {"pair": pair, "funding_rate": None, "source": "kraken_margin_interest"}

        except Exception as e:
            logger.error(f"Failed to get funding rate for {pair}: {e}")
            return {"error": str(e)}

    @staticmethod
    def get_open_interest(pair: str) -> dict:
        """Get open interest data for leverage estimation."""
        try:
            if not API_KEY or not PRIVATE_KEY:
                logger.warning("Kraken API keys not configured")
                return {"error": "Credentials not set"}

            kraken_pair = _get_kraken_pair(pair.split("-")[0])
            logger.info(f"Open interest check for {pair} ({kraken_pair}): Limited API support")

            # Kraken doesn't expose open interest via API like Binance does
            # This is a known limitation — would need exchange documentation updates
            return {
                "pair": pair,
                "open_interest": None,
                "note": "Kraken API does not expose open interest directly"
            }

        except Exception as e:
            logger.error(f"Failed to get open interest for {pair}: {e}")
            return {"error": str(e)}

    @staticmethod
    def get_orderbook(pair: str, depth: int = 20) -> dict:
        """Get order book for liquidity analysis."""
        try:
            kraken_pair = _get_kraken_pair(pair.split("-")[0]) + "USD"
            url = f"{API_URL}/{API_VERSION}/public/Depth?pair={kraken_pair}&count={depth}"

            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())

            if data.get("error"):
                logger.error(f"Kraken error: {data['error']}")
                return {"error": data["error"]}

            result = data.get("result", {})
            book = _first_result(result)
            return {
                "pair": pair,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "asks": (book.get("asks", []) if isinstance(book, dict) else result.get("asks", []))[:depth],
                "bids": (book.get("bids", []) if isinstance(book, dict) else result.get("bids", []))[:depth],
            }

        except Exception as e:
            logger.error(f"Failed to get orderbook for {pair}: {e}")
            return {"error": str(e)}

    @staticmethod
    def get_recent_trades(pair: str, limit: int = 100) -> dict:
        """Get recent trades for microstructure analysis."""
        try:
            kraken_pair = _get_kraken_pair(pair.split("-")[0]) + "USD"
            url = f"{API_URL}/{API_VERSION}/public/Trades?pair={kraken_pair}"

            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())

            if data.get("error"):
                logger.error(f"Kraken error: {data['error']}")
                return {"error": data["error"]}

            result = data.get("result", {})
            trades_data = _first_result(result, kraken_pair)
            trades = (trades_data if isinstance(trades_data, list) else [])[-limit:]

            return {
                "pair": pair,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "trades": trades,  # [price, volume, time, buy/sell, market/limit, misc]
                "count": len(trades),
            }

        except Exception as e:
            logger.error(f"Failed to get recent trades for {pair}: {e}")
            return {"error": str(e)}

    @staticmethod
    def get_24h_volume(pair: str) -> dict:
        """Get 24h volume for pair."""
        try:
            kraken_pair = _get_kraken_pair(pair.split("-")[0]) + "USD"
            url = f"{API_URL}/{API_VERSION}/public/Ticker?pair={kraken_pair}"

            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())

            if data.get("error"):
                logger.error(f"Kraken error: {data['error']}")
                return {"error": data["error"]}

            result = data.get("result", {})
            ticker = _first_result(result)

            return {
                "pair": pair,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "volume_24h": float(ticker.get("v", [0, 0])[1] or 0),  # [30m, 24h]
                "price_24h_high": float(ticker.get("h", [0, 0])[1] or 0),
                "price_24h_low": float(ticker.get("l", [0, 0])[1] or 0),
                "last_price": float(ticker.get("c", [0])[0] or 0),
            }

        except Exception as e:
            logger.error(f"Failed to get 24h volume for {pair}: {e}")
            return {"error": str(e)}

    # =========================================================================
    # Private (authenticated) API methods
    # =========================================================================

    @staticmethod
    def _private_request(endpoint, data=None):
        """Make authenticated POST to Kraken private API.

        Uses existing _sign_request() for HMAC-SHA512 signing.
        Returns parsed JSON response or error dict.
        """
        if not API_KEY or not PRIVATE_KEY:
            return {"error": ["Kraken API keys not configured"]}
        try:
            url_path = f"/{API_VERSION}/private/{endpoint}"
            url = f"{API_URL}{url_path}"
            nonce = str(int(time.time() * 1000))
            post_data = data or {}
            post_data["nonce"] = nonce
            sig, postdata = _sign_request(url_path, post_data, nonce)
            headers = {
                "API-Key": API_KEY,
                "API-Sign": base64.b64encode(sig).decode(),
                "Content-Type": "application/x-www-form-urlencoded",
            }
            req = urllib.request.Request(
                url, data=postdata.encode(), headers=headers, method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read())
        except Exception as e:
            logger.error(f"Kraken private request {endpoint} failed: {e}")
            return {"error": [str(e)]}

    @staticmethod
    def place_order(
        pair: str,
        side: str,
        volume: float,
        order_type: str = "limit",
        price: float = None,
        confidence: float = 0.70,
        # New advanced parameters:
        stop_price: float = None,          # For stop-loss/take-profit trigger price
        trailing_offset: float = None,     # Trailing stop offset in price units
        close_order: dict = None,          # Conditional close: {"ordertype": "stop-loss-limit", "price": X, "price2": Y}
        oflags: str = None,                # Order flags: "post" (post-only), "fcib" (fee in base), "fciq" (fee in quote)
        timeinforce: str = None,           # "GTC", "IOC", "GTD"
        visible_volume: float = None,      # Iceberg: visible portion of total volume
    ) -> dict:
        """Place an order on Kraken, gated through GoalValidator.

        BE A MAKER: defaults to limit orders (0.16% fee vs 0.26% taker).

        Supports advanced order types:
            - "limit"              — standard limit order
            - "market"             — market order
            - "stop-loss"          — stop price triggers market sell/buy
            - "take-profit"        — take profit triggers market
            - "stop-loss-limit"    — stop triggers limit order (price=limit, stop_price=trigger)
            - "take-profit-limit"  — take profit triggers limit
            - "trailing-stop"      — trailing stop offset
            - "trailing-stop-limit"— trailing stop triggers limit

        Args:
            pair: Standard pair (e.g., "BTC-USD")
            side: "buy" or "sell"
            volume: Order size in base currency
            order_type: Order type (default "limit")
            price: Limit price (or trigger price for stop/take-profit types)
            confidence: Signal confidence (0-1), must be >= 0.70
            stop_price: Trigger price for stop-loss-limit / take-profit-limit
            trailing_offset: Trailing stop offset in price units
            close_order: Conditional close dict: {"ordertype": ..., "price": ..., "price2": ...}
            oflags: Order flags: "post", "fcib", "fciq", "nompp" (comma-separated)
            timeinforce: Time in force: "GTC", "IOC", "GTD"
            visible_volume: Visible volume for iceberg orders

        Returns:
            dict with txid on success, error on failure
        """
        # Order types that don't require a price param upfront
        TRIGGER_ORDER_TYPES = {
            "stop-loss", "take-profit", "trailing-stop",
            "stop-loss-limit", "take-profit-limit", "trailing-stop-limit",
            "market",
        }

        # GoalValidator gate
        direction = side.upper()
        if GoalValidator:
            allowed = GoalValidator.should_trade(confidence, 2, direction, "neutral")
            if not allowed:
                logger.warning(
                    "GoalValidator blocked %s %s (conf=%.2f)", direction, pair, confidence
                )
                return {"error": f"GoalValidator blocked: {direction} {pair} conf={confidence:.2f}"}
        elif confidence < 0.70:
            logger.warning("No GoalValidator, confidence %.2f < 0.70 — blocking", confidence)
            return {"error": f"Confidence {confidence:.2f} below 0.70 threshold"}

        # BE A MAKER: reject limit orders without price (but allow trigger types)
        if order_type == "limit" and price is None:
            return {"error": "Limit order requires price parameter"}

        kraken_pair = _get_kraken_pair(pair.split("-")[0]) + "USD"

        order_data = {
            "pair": kraken_pair,
            "type": side.lower(),  # buy or sell
            "ordertype": order_type,
            "volume": str(volume),
        }

        # ── Map advanced parameters to Kraken API fields ──
        if order_type in ("stop-loss-limit", "take-profit-limit"):
            # price = limit price, price2 = trigger (stop) price
            if price is not None:
                order_data["price"] = str(price)
            if stop_price is not None:
                order_data["price2"] = str(stop_price)
        elif order_type in ("stop-loss", "take-profit"):
            # price = trigger price
            if price is not None:
                order_data["price"] = str(price)
        elif order_type == "trailing-stop":
            # price = trailing offset (e.g., +100 means $100 from peak)
            if trailing_offset is not None:
                order_data["price"] = str(trailing_offset)
        elif order_type == "trailing-stop-limit":
            # price = trailing offset, price2 = limit offset
            if trailing_offset is not None:
                order_data["price"] = str(trailing_offset)
            if price is not None and trailing_offset is not None:
                # When both provided, price goes to price2 (limit), trailing to price
                order_data["price"] = str(trailing_offset)
                order_data["price2"] = str(price)
        else:
            # Standard limit/market
            if price is not None:
                order_data["price"] = str(price)

        # Iceberg order: visible volume
        if visible_volume is not None:
            order_data["displayvol"] = str(visible_volume)

        # Order flags (post-only, fee currency, etc.)
        if oflags is not None:
            order_data["oflags"] = oflags

        # Time in force
        if timeinforce is not None:
            order_data["timeinforce"] = timeinforce

        # Conditional close order
        if close_order and isinstance(close_order, dict):
            if close_order.get("ordertype"):
                order_data["close[ordertype]"] = str(close_order["ordertype"])
            if close_order.get("price"):
                order_data["close[price]"] = str(close_order["price"])
            if close_order.get("price2"):
                order_data["close[price2]"] = str(close_order["price2"])

        logger.info(
            "Placing %s %s order: %s %.6f @ %s (conf=%.2f)",
            order_type, side, pair, volume, price or "market", confidence,
        )

        result = KrakenConnector._private_request("AddOrder", order_data)

        # Record trade in DB
        txid = None
        status = "error"
        if result.get("result"):
            txid_list = result["result"].get("txid", [])
            txid = txid_list[0] if txid_list else None
            status = "submitted"
            logger.info("Order submitted: txid=%s", txid)

        try:
            db = _init_trade_db()
            db.execute(
                """INSERT INTO kraken_trades
                   (pair, side, volume, price, order_type, txid, status, confidence)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (pair, side.lower(), volume, price, order_type, txid, status, confidence),
            )
            db.commit()
            db.close()
        except Exception as e:
            logger.error("Failed to record trade in DB: %s", e)

        return result

    @staticmethod
    def get_fee_schedule(pair: str = None) -> dict:
        """Get current fee tier based on 30-day trading volume.

        Uses Kraken TradeVolume API endpoint.

        Args:
            pair: Optional pair for pair-specific fee info (e.g., "BTC-USD")

        Returns:
            {"fee_tier": str, "maker_fee": float, "taker_fee": float, "volume_30d": float}
            or {"error": ...} on failure.
        """
        data = {}
        if pair:
            kraken_pair = _get_kraken_pair(pair.split("-")[0]) + "USD"
            data["pair"] = kraken_pair

        result = KrakenConnector._private_request("TradeVolume", data if data else None)

        if result.get("error") and result["error"]:
            return {"error": result["error"]}

        res = result.get("result", {})
        volume_30d = float(res.get("volume", "0") or "0")
        currency = res.get("currency", "ZUSD")

        # Extract fee info — fees are keyed by the Kraken result pair name
        maker_fee = 0.16  # default
        taker_fee = 0.26  # default

        fees = res.get("fees", {})
        fees_maker = res.get("fees_maker", {})

        # Get the first pair's fee data (or the specific pair if provided)
        if fees:
            first_fee = next(iter(fees.values()), {})
            taker_fee = float(first_fee.get("fee", "0.26") or "0.26")

        if fees_maker:
            first_maker = next(iter(fees_maker.values()), {})
            maker_fee = float(first_maker.get("fee", "0.16") or "0.16")

        return {
            "fee_tier": currency,
            "maker_fee": maker_fee,
            "taker_fee": taker_fee,
            "volume_30d": volume_30d,
        }

    @staticmethod
    def cancel_order(txid: str) -> dict:
        """Cancel an open order by transaction ID.

        Args:
            txid: Kraken transaction ID to cancel

        Returns:
            dict with count of cancelled orders or error
        """
        logger.info("Cancelling order: txid=%s", txid)
        result = KrakenConnector._private_request("CancelOrder", {"txid": txid})

        if result.get("result"):
            # Update DB status
            try:
                db = _init_trade_db()
                db.execute(
                    "UPDATE kraken_trades SET status='cancelled' WHERE txid=?",
                    (txid,),
                )
                db.commit()
                db.close()
            except Exception as e:
                logger.error("Failed to update cancelled trade in DB: %s", e)

        return result

    @staticmethod
    def get_open_orders() -> dict:
        """Get all open orders.

        Returns:
            dict with open orders keyed by txid, or error
        """
        return KrakenConnector._private_request("OpenOrders")

    @staticmethod
    def get_closed_orders() -> dict:
        """Get closed orders (trade history).

        Returns:
            dict with closed orders keyed by txid, or error
        """
        return KrakenConnector._private_request("ClosedOrders")

    @staticmethod
    def get_trade_balance() -> dict:
        """Get trade balance (equity, margin, free margin).

        Returns:
            dict with eb (equity balance), tb (trade balance),
            m (margin), n (unrealized P&L), etc.
        """
        return KrakenConnector._private_request("TradeBalance")

    @staticmethod
    def get_account_balance() -> dict:
        """Get all asset balances.

        Returns:
            dict with asset codes as keys and balances as values
        """
        return KrakenConnector._private_request("Balance")

    @staticmethod
    def get_positions() -> dict:
        """Get open margin positions.

        Returns:
            dict with open positions keyed by txid, or error
        """
        return KrakenConnector._private_request("OpenPositions")


def test_kraken_connection():
    """Test that Kraken API is accessible (public endpoints only).

    Works without API keys — tests the public orderbook endpoint.
    """
    logger.info("Testing Kraken API connection...")

    if not API_KEY or not PRIVATE_KEY:
        logger.warning("Kraken API keys not set — testing public endpoint only")

    try:
        # Test public endpoint (no auth required)
        result = KrakenConnector.get_orderbook("BTC-USD", depth=5)
        if not result.get("error"):
            logger.info("Kraken connection OK: orderbook fetched")
            return True
        else:
            logger.error("Kraken API error: %s", result.get("error"))
            return False
    except Exception as e:
        logger.error("Kraken connection failed: %s", e)
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_kraken_connection()
