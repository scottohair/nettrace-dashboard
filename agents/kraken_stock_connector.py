#!/usr/bin/env python3
"""Kraken Stock/Equity Trading Connector — commission-free US stocks & ETFs.

Kraken launched equity trading in April 2025:
  - 11,000+ US-listed stocks and ETFs
  - Commission-free (only FINRA TAF + SEC fees on sells)
  - Fractional shares supported
  - Trading hours: 9:30 AM - 4:00 PM ET (with extended hours)

All trades gated through GoalValidator (70% confidence, 2+ signals).
Uses same Kraken REST API and HMAC signing as crypto connector.
"""

import datetime
import json
import logging
import os
import sqlite3
import time
import urllib.request
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore[assignment,misc]

# Load .env if present
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"'))

try:
    from kraken_connector import KrakenConnector, API_URL, API_VERSION
except ImportError:
    try:
        from agents.kraken_connector import KrakenConnector, API_URL, API_VERSION  # type: ignore[no-redef]
    except ImportError:
        KrakenConnector = None  # type: ignore[assignment,misc]
        API_URL = "https://api.kraken.com"
        API_VERSION = "0"

try:
    from agent_goals import GoalValidator
except ImportError:
    try:
        from agents.agent_goals import GoalValidator  # type: ignore[no-redef]
    except ImportError:
        GoalValidator = None  # type: ignore[assignment,misc]

logger = logging.getLogger("kraken_stock_connector")

# ── Market hours (US Eastern Time) ──
US_MARKET_OPEN = datetime.time(9, 30)    # 9:30 AM ET
US_MARKET_CLOSE = datetime.time(16, 0)   # 4:00 PM ET
EXTENDED_OPEN = datetime.time(4, 0)      # 4:00 AM ET (pre-market)
EXTENDED_CLOSE = datetime.time(20, 0)    # 8:00 PM ET (after-hours)

# ── Stock pair discovery cache ──
STOCK_PAIR_CACHE: dict = {}  # symbol -> kraken_pair_name
STOCK_CACHE_EXPIRY: float = 0

# ── Trade tracking database ──
STOCK_TRADE_DB = Path(__file__).parent / "kraken_stock_trades.db"


def is_market_open(extended_hours: bool = False) -> bool:
    """Check if US stock market is currently open.

    Args:
        extended_hours: If True, check extended trading hours (4 AM - 8 PM ET)
                       instead of regular hours (9:30 AM - 4 PM ET).

    Returns:
        True if market is currently open.
    """
    if ZoneInfo is not None:
        tz = ZoneInfo("America/New_York")
        now_et = datetime.datetime.now(tz)
    else:
        # Approximate ET as UTC-5
        et_offset = datetime.timedelta(hours=-5)
        now_et = datetime.datetime.now(datetime.timezone(et_offset))

    # Weekend check: Saturday=5, Sunday=6
    if now_et.weekday() >= 5:
        return False

    current_time = now_et.time()
    if extended_hours:
        return EXTENDED_OPEN <= current_time <= EXTENDED_CLOSE
    return US_MARKET_OPEN <= current_time <= US_MARKET_CLOSE


def _discover_stock_pairs() -> dict:
    """Query Kraken AssetPairs for equity products.

    Kraken stock pairs use aclass_base="equity" or similar classification.
    Results are cached for 1 hour.

    Returns:
        dict mapping stock symbol (e.g., "AAPL") to Kraken pair name.
    """
    global STOCK_PAIR_CACHE, STOCK_CACHE_EXPIRY
    if time.time() < STOCK_CACHE_EXPIRY and STOCK_PAIR_CACHE:
        return STOCK_PAIR_CACHE

    url = f"{API_URL}/{API_VERSION}/public/AssetPairs"
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "NetTrace-KrakenStocks/1.0")
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        if data.get("error"):
            logger.error("Kraken AssetPairs error: %s", data["error"])
            return STOCK_PAIR_CACHE

        result = data.get("result", {})
        new_cache = {}
        for pair_name, info in result.items():
            # Identify equity products by aclass_base or other markers
            aclass = info.get("aclass_base", "")
            wsname = info.get("wsname", "")
            altname = info.get("altname", "")

            # Equity products have aclass_base == "equity"
            # or have low lot_decimals (stocks typically 0-4 vs crypto 8-10)
            is_equity = False
            if aclass == "equity":
                is_equity = True
            elif info.get("lot_decimals", 8) <= 4 and "/" in wsname:
                # Stock-like naming: "AAPL/USD"
                base_part = wsname.split("/")[0] if "/" in wsname else ""
                # Filter out known crypto pairs
                crypto_bases = {
                    "XBT", "ETH", "SOL", "ADA", "DOT", "XRP", "DOGE",
                    "AVAX", "LINK", "MATIC", "UNI", "ATOM", "LTC",
                }
                if base_part and base_part not in crypto_bases and base_part.isalpha():
                    is_equity = True

            if is_equity:
                base = info.get("base", "")
                # Normalize: strip leading X or Z Kraken prefixes
                clean_base = base.lstrip("X").lstrip("Z") if base else ""
                if clean_base:
                    new_cache[clean_base] = pair_name
                # Also map by wsname base (e.g., "AAPL" from "AAPL/USD")
                if "/" in wsname:
                    ws_base = wsname.split("/")[0]
                    if ws_base:
                        new_cache[ws_base] = pair_name

        if new_cache:
            STOCK_PAIR_CACHE = new_cache
            STOCK_CACHE_EXPIRY = time.time() + 3600  # Cache for 1 hour
            logger.info("Discovered %d stock pairs on Kraken", len(new_cache))
        else:
            # If no equity pairs found, still update expiry to avoid hammering
            STOCK_CACHE_EXPIRY = time.time() + 300  # Retry in 5 min

    except Exception as e:
        logger.error("Failed to discover stock pairs: %s", e)

    return STOCK_PAIR_CACHE


def _get_stock_pair(symbol: str) -> str:
    """Map stock ticker to Kraken pair name.

    Args:
        symbol: Stock ticker (e.g., "AAPL", "SPY")

    Returns:
        Kraken pair name string, or empty string if not found.
    """
    pairs = _discover_stock_pairs()
    symbol_upper = symbol.upper()

    # Direct lookup
    if symbol_upper in pairs:
        return pairs[symbol_upper]

    # Try common Kraken pair formats
    for fmt in [f"{symbol_upper}USD", f"{symbol_upper}/USD", f"X{symbol_upper}ZUSD"]:
        # Check if this format is a value in the cache
        if fmt in pairs.values():
            return fmt
        # Check if it's a key
        if fmt in pairs:
            return pairs[fmt]

    return ""


def _init_stock_trade_db():
    """Initialize Kraken stock trade tracking database."""
    db = sqlite3.connect(str(STOCK_TRADE_DB))
    db.execute("""
        CREATE TABLE IF NOT EXISTS kraken_stock_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            shares REAL NOT NULL,
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


def _record_stock_trade(symbol, side, shares, price, order_type, result, confidence):
    """Record stock trade in DB.

    Args:
        symbol: Stock ticker
        side: "buy" or "sell"
        shares: Number of shares
        price: Order price (or None for market)
        order_type: "limit" or "market"
        result: Kraken API response dict
        confidence: Signal confidence
    """
    txid = None
    status = "error"
    if result.get("result"):
        txid_list = result["result"].get("txid", [])
        txid = txid_list[0] if txid_list else None
        status = "submitted"
    try:
        db = _init_stock_trade_db()
        db.execute(
            """INSERT INTO kraken_stock_trades
               (symbol, side, shares, price, order_type, txid, status, confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, side.lower(), shares, price, order_type, txid, status, confidence),
        )
        db.commit()
        db.close()
    except Exception as e:
        logger.error("Failed to record stock trade: %s", e)


def _update_stock_trade_status(txid, new_status):
    """Update status of a stock trade by transaction ID."""
    try:
        db = _init_stock_trade_db()
        db.execute(
            "UPDATE kraken_stock_trades SET status=? WHERE txid=?",
            (new_status, txid),
        )
        db.commit()
        db.close()
    except Exception as e:
        logger.error("Failed to update stock trade status: %s", e)


class KrakenStockConnector:
    """Kraken stock/equity trading connector.

    Provides commission-free access to 11,000+ US stocks and ETFs
    via the same Kraken REST API used for crypto trading.
    """

    # Popular stocks/ETFs to pre-cache
    POPULAR_STOCKS = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META",
        "SPY", "QQQ", "IWM", "DIA",           # ETFs
        "COIN", "MSTR", "MARA", "RIOT",        # Crypto-correlated
        "XLK", "XLF", "XLE", "XLV", "XLY",    # Sector ETFs
    ]

    @staticmethod
    def get_stock_quote(symbol: str) -> dict:
        """Get real-time stock quote from Kraken.

        Args:
            symbol: Stock ticker (e.g., "AAPL", "SPY")

        Returns:
            dict with last_price, bid, ask, volume, high, low
            or dict with "error" key on failure.
        """
        pair = _get_stock_pair(symbol)
        if not pair:
            return {"error": f"Stock {symbol} not found on Kraken"}

        url = f"{API_URL}/{API_VERSION}/public/Ticker?pair={pair}"
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "NetTrace-KrakenStocks/1.0")
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())

            if data.get("error"):
                return {"error": data["error"]}

            result = data.get("result", {})
            # Kraken returns result keyed by internal pair name
            # Get first (and usually only) result
            ticker = {}
            for key in result:
                if key != "last":
                    ticker = result[key]
                    break

            if not ticker:
                return {"error": f"No ticker data for {symbol}"}

            # Kraken ticker format:
            # a = ask [price, whole_lot_volume, lot_volume]
            # b = bid [price, whole_lot_volume, lot_volume]
            # c = last trade [price, lot_volume]
            # v = volume [today, last_24h]
            # h = high [today, last_24h]
            # l = low [today, last_24h]
            # o = open (today)
            ask_data = ticker.get("a", [0, 0, 0])
            bid_data = ticker.get("b", [0, 0, 0])
            last_data = ticker.get("c", [0, 0])
            vol_data = ticker.get("v", [0, 0])
            high_data = ticker.get("h", [0, 0])
            low_data = ticker.get("l", [0, 0])
            open_price = ticker.get("o", 0)

            last_price = float(last_data[0]) if last_data else 0
            ask_price = float(ask_data[0]) if ask_data else 0
            bid_price = float(bid_data[0]) if bid_data else 0
            spread = ask_price - bid_price if ask_price > 0 and bid_price > 0 else 0

            return {
                "symbol": symbol,
                "pair": pair,
                "last_price": last_price,
                "bid": bid_price,
                "ask": ask_price,
                "spread": spread,
                "volume_today": float(vol_data[0]) if vol_data else 0,
                "volume_24h": float(vol_data[1]) if len(vol_data) > 1 else 0,
                "high_today": float(high_data[0]) if high_data else 0,
                "low_today": float(low_data[0]) if low_data else 0,
                "high_24h": float(high_data[1]) if len(high_data) > 1 else 0,
                "low_24h": float(low_data[1]) if len(low_data) > 1 else 0,
                "open": float(open_price) if open_price else 0,
            }

        except Exception as e:
            logger.error("Failed to get stock quote for %s: %s", symbol, e)
            return {"error": str(e)}

    @staticmethod
    def get_stock_orderbook(symbol: str, depth: int = 10) -> dict:
        """Get Level 2 order book for stock.

        Args:
            symbol: Stock ticker (e.g., "AAPL")
            depth: Number of price levels (default 10)

        Returns:
            dict with asks and bids arrays, or error.
        """
        pair = _get_stock_pair(symbol)
        if not pair:
            return {"error": f"Stock {symbol} not found on Kraken"}

        url = f"{API_URL}/{API_VERSION}/public/Depth?pair={pair}&count={depth}"
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "NetTrace-KrakenStocks/1.0")
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())

            if data.get("error"):
                return {"error": data["error"]}

            result = data.get("result", {})
            # Get first result entry
            book = {}
            for key in result:
                book = result[key]
                break

            return {
                "symbol": symbol,
                "pair": pair,
                "asks": book.get("asks", [])[:depth],
                "bids": book.get("bids", [])[:depth],
            }

        except Exception as e:
            logger.error("Failed to get stock orderbook for %s: %s", symbol, e)
            return {"error": str(e)}

    @staticmethod
    def place_stock_order(
        symbol: str,
        side: str,
        shares: float,
        order_type: str = "limit",
        price: float = None,
        confidence: float = 0.70,
    ) -> dict:
        """Place a stock order on Kraken.

        GoalValidator gated. Market hours validated.
        Commission-free execution.

        Args:
            symbol: Stock ticker (e.g., "AAPL")
            side: "buy" or "sell"
            shares: Number of shares (fractional OK)
            order_type: "limit" or "market"
            price: Required for limit orders
            confidence: Signal confidence (>= 0.70)

        Returns:
            dict with Kraken API response (txid on success) or error.
        """
        # Market hours gate
        if not is_market_open():
            return {"error": "US stock market is closed", "market_hours": "9:30-16:00 ET"}

        # GoalValidator gate
        direction = side.upper()
        if GoalValidator:
            allowed = GoalValidator.should_trade(confidence, 2, direction, "neutral")
            if not allowed:
                return {"error": f"GoalValidator blocked: {direction} {symbol} conf={confidence:.2f}"}
        elif confidence < 0.70:
            return {"error": f"Confidence {confidence:.2f} below 0.70 threshold"}

        # BE A MAKER: limit orders require price
        if order_type == "limit" and price is None:
            return {"error": "Limit order requires price"}

        pair = _get_stock_pair(symbol)
        if not pair:
            return {"error": f"Stock {symbol} not found on Kraken"}

        order_data = {
            "pair": pair,
            "type": side.lower(),
            "ordertype": order_type,
            "volume": str(shares),
        }
        if price is not None:
            order_data["price"] = str(price)

        # Execute via KrakenConnector's private request
        if KrakenConnector is None:
            return {"error": "KrakenConnector not available"}

        logger.info(
            "Placing stock %s order: %s %s %.4f shares @ %s (conf=%.2f)",
            order_type, side, symbol, shares, price or "market", confidence,
        )

        result = KrakenConnector._private_request("AddOrder", order_data)

        # Record in stock trades DB
        _record_stock_trade(symbol, side, shares, price, order_type, result, confidence)

        # Log result
        if result.get("result"):
            txid_list = result["result"].get("txid", [])
            txid = txid_list[0] if txid_list else None
            logger.info("Stock order submitted: %s %s txid=%s", side, symbol, txid)
        else:
            error_info = result.get("error", "Unknown error")
            logger.warning("Stock order failed for %s: %s", symbol, error_info)

        return result

    @staticmethod
    def cancel_stock_order(txid: str) -> dict:
        """Cancel an open stock order.

        Args:
            txid: Kraken transaction ID to cancel.

        Returns:
            dict with Kraken API response or error.
        """
        if KrakenConnector is None:
            return {"error": "KrakenConnector not available"}

        logger.info("Cancelling stock order: txid=%s", txid)
        result = KrakenConnector._private_request("CancelOrder", {"txid": txid})

        if result.get("result"):
            _update_stock_trade_status(txid, "cancelled")

        return result

    @staticmethod
    def get_stock_positions() -> dict:
        """Get equity positions.

        Filters account balance for stock assets (identified via
        the stock pair discovery cache).

        Returns:
            dict with positions and count, or error.
        """
        if KrakenConnector is None:
            return {"error": "KrakenConnector not available"}

        result = KrakenConnector._private_request("Balance")
        if result.get("error") and result["error"]:
            return {"error": result["error"]}

        balances = result.get("result", {})
        stock_pairs = _discover_stock_pairs()
        stock_bases = set(stock_pairs.keys())

        stock_positions = {}
        for asset, balance in balances.items():
            bal_float = float(balance)
            if asset in stock_bases and bal_float > 0:
                stock_positions[asset] = bal_float

        return {"positions": stock_positions, "count": len(stock_positions)}

    @staticmethod
    def search_stocks(query: str) -> list:
        """Search for stocks by symbol or partial match.

        Args:
            query: Search string (e.g., "AAPL", "AA", "SP")

        Returns:
            List of matching stock dicts (up to 20).
        """
        pairs = _discover_stock_pairs()
        query_upper = query.upper()
        matches = []
        for symbol, pair_name in pairs.items():
            if query_upper in symbol:
                matches.append({"symbol": symbol, "pair": pair_name})
        return matches[:20]

    @staticmethod
    def get_market_status() -> dict:
        """Get current US market status.

        Returns:
            dict with market hours info and open/closed status.
        """
        return {
            "regular_hours_open": is_market_open(extended_hours=False),
            "extended_hours_open": is_market_open(extended_hours=True),
            "regular_hours": "9:30 AM - 4:00 PM ET",
            "extended_hours": "4:00 AM - 8:00 PM ET",
            "timezone": "America/New_York",
        }

    @staticmethod
    def get_stock_trade_history(symbol: str = None, limit: int = 50) -> list:
        """Get stock trade history from local DB.

        Args:
            symbol: Optional filter by stock ticker.
            limit: Max records to return.

        Returns:
            List of trade dicts.
        """
        try:
            db = _init_stock_trade_db()
            if symbol:
                cursor = db.execute(
                    "SELECT * FROM kraken_stock_trades WHERE symbol=? ORDER BY created_at DESC LIMIT ?",
                    (symbol.upper(), limit),
                )
            else:
                cursor = db.execute(
                    "SELECT * FROM kraken_stock_trades ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                )
            columns = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            db.close()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error("Failed to get stock trade history: %s", e)
            return []


def test_kraken_stocks():
    """Quick test of Kraken stock connectivity."""
    logger.info("Testing Kraken stock connector...")

    # Test market status
    status = KrakenStockConnector.get_market_status()
    logger.info("Market status: regular=%s, extended=%s",
                status["regular_hours_open"], status["extended_hours_open"])

    # Test pair discovery
    pairs = _discover_stock_pairs()
    logger.info("Discovered %d stock pairs", len(pairs))
    if pairs:
        sample = list(pairs.items())[:5]
        for sym, pair in sample:
            logger.info("  %s -> %s", sym, pair)

    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_kraken_stocks()
