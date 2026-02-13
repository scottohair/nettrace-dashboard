#!/usr/bin/env python3
"""Interactive Brokers connector — live trading via TWS/IB Gateway.

Same interface as exchange_connector.py CoinbaseTrader so agents can seamlessly
route orders to IBKR for stocks, options, futures, forex, and bonds.

Uses ib_async library (pip install ib_async), drop-in replacement for ib_insync.
Falls back to ib_insync if ib_async is not installed.

STATUS: Account APPROVED 2026-02-13 (email: ohariscott@gmail.com)
        Priority #1 exchange — gives access to all asset classes.

SETUP:
  1. Install TWS or IB Gateway
  2. Enable API access in TWS settings (port 7497 for paper, 7496 for live)
  3. pip install ib_async  (or ib_insync as fallback)
  4. Set IBKR_HOST, IBKR_PORT, IBKR_CLIENT_ID in agents/.env
  5. Set IBKR_PAPER_ONLY=1 to prevent accidental live connections
"""

import logging
import os
import time
import threading
from pathlib import Path

# ib_async is a drop-in replacement for ib_insync — try it first, fall back
try:
    import ib_async as _ib_api
    _IB_BACKEND = "ib_async"
except ImportError:
    try:
        import ib_insync as _ib_api
        _IB_BACKEND = "ib_insync"
    except ImportError:
        _ib_api = None
        _IB_BACKEND = None

logger = logging.getLogger("ibkr_connector")

# Connection settings from env
IBKR_HOST = os.environ.get("IBKR_HOST", "127.0.0.1")
IBKR_PORT = int(os.environ.get("IBKR_PORT", "7497"))  # 7497=paper, 7496=live
IBKR_CLIENT_ID = int(os.environ.get("IBKR_CLIENT_ID", "1"))


class IBKRTrader:
    """Interactive Brokers connector matching CoinbaseTrader interface.

    Methods mirror CoinbaseTrader so agents can route orders transparently:
      - place_order(product_id, side, size)
      - place_limit_order(product_id, side, base_size, limit_price)
      - get_accounts()
      - get_order_fill(order_id)
    """

    # Exponential backoff parameters for auto-reconnect
    RECONNECT_BASE_DELAY = 5    # seconds
    RECONNECT_MAX_DELAY = 60    # seconds
    RECONNECT_MULTIPLIER = 2

    def __init__(self, host=None, port=None, client_id=None):
        self.host = host or IBKR_HOST
        self.port = port or IBKR_PORT
        self.client_id = client_id or IBKR_CLIENT_ID
        self._ib = None
        self._connected = False
        self._reconnect_attempts = 0
        self._reconnecting = False
        self._auto_reconnect = True

    def connect(self):
        """Connect to TWS/IB Gateway.

        Logs whether connecting to PAPER (7497) or LIVE (7496) mode.
        Refuses to connect to LIVE port if IBKR_PAPER_ONLY env var is set.
        Registers auto-reconnect handler on disconnect.
        """
        if _ib_api is None:
            logger.error("Neither ib_async nor ib_insync installed — run: pip install ib_async")
            return False

        # Paper/live safety guard
        paper_only = os.environ.get("IBKR_PAPER_ONLY", "").strip()
        if paper_only and paper_only not in ("0", "false", "no"):
            if self.port == 7496:
                logger.error(
                    "SAFETY GUARD: IBKR_PAPER_ONLY is set but port is 7496 (LIVE). "
                    "Refusing to connect. Use port 7497 for paper trading or unset IBKR_PAPER_ONLY."
                )
                return False

        mode = "PAPER" if self.port == 7497 else "LIVE" if self.port == 7496 else f"CUSTOM(port={self.port})"
        logger.info("Connecting to IBKR %s mode at %s:%d (client %d) [backend: %s]",
                     mode, self.host, self.port, self.client_id, _IB_BACKEND)

        try:
            IB = _ib_api.IB
            self._ib = IB()
            self._ib.connect(self.host, self.port, clientId=self.client_id)
            self._connected = True
            self._reconnect_attempts = 0
            logger.info("Connected to IBKR %s at %s:%d (client %d)",
                         mode, self.host, self.port, self.client_id)

            # Register auto-reconnect on disconnect
            self._ib.disconnectedEvent += self._on_disconnected
            return True
        except Exception as e:
            logger.error("IBKR connection failed: %s", e)
            return False

    def _on_disconnected(self):
        """Handle unexpected disconnection — trigger auto-reconnect."""
        self._connected = False
        logger.warning("IBKR disconnected unexpectedly")
        if self._auto_reconnect and not self._reconnecting:
            logger.info("Auto-reconnect enabled — will attempt reconnection in %ds",
                         self.RECONNECT_BASE_DELAY)
            t = threading.Thread(target=self._reconnect, daemon=True)
            t.start()

    def _reconnect(self):
        """Reconnect with exponential backoff (5s, 10s, 20s, max 60s)."""
        self._reconnecting = True
        delay = self.RECONNECT_BASE_DELAY
        try:
            while self._auto_reconnect and not self._connected:
                self._reconnect_attempts += 1
                logger.info("Reconnect attempt #%d (delay=%ds)...", self._reconnect_attempts, delay)
                time.sleep(delay)

                if self.connect():
                    logger.info("Reconnected to IBKR after %d attempt(s)", self._reconnect_attempts)
                    self._reconnect_attempts = 0
                    return

                # Exponential backoff
                delay = min(delay * self.RECONNECT_MULTIPLIER, self.RECONNECT_MAX_DELAY)
        finally:
            self._reconnecting = False

    def _check_connection(self):
        """Verify connection health. Returns True if connected, False otherwise."""
        if self._ib is None:
            return False
        try:
            return self._ib.isConnected()
        except Exception:
            return False

    def disconnect(self):
        """Disconnect from TWS."""
        self._auto_reconnect = False  # Prevent auto-reconnect on intentional disconnect
        if self._ib and self._connected:
            try:
                self._ib.disconnectedEvent -= self._on_disconnected
            except Exception:
                pass
            self._ib.disconnect()
            self._connected = False
            logger.info("Disconnected from IBKR")
        self._auto_reconnect = True  # Re-enable for future connections

    def _ensure_connected(self):
        """Ensure we have an active connection, reconnecting if needed."""
        if not self._connected or not self._check_connection():
            self._connected = False
            if not self.connect():
                raise ConnectionError("Not connected to IBKR")

    def _make_contract(self, product_id):
        """Convert product_id string to an IB Contract.

        Supports formats:
          - "AAPL" → Stock
          - "BTC-USD" → Crypto
          - "ES" → Future (front month)
          - "AAPL_C_200_20260320" → Option
          - "EUR-USD" → Forex
        """
        Stock, Crypto, Forex, Future, Contract = (
            _ib_api.Stock, _ib_api.Crypto, _ib_api.Forex, _ib_api.Future, _ib_api.Contract
        )

        if "-" in product_id:
            base, quote = product_id.split("-", 1)
            # Crypto pairs
            if base in ("BTC", "ETH", "SOL", "DOGE", "AVAX", "LINK", "XRP", "ADA"):
                return Crypto(base, "PAXOS", quote)
            # Forex pairs
            if len(base) == 3 and len(quote) == 3:
                return Forex(base + quote)

        if "_" in product_id:
            # Options: SYMBOL_TYPE_STRIKE_EXPIRY
            parts = product_id.split("_")
            if len(parts) == 4:
                sym, opt_type, strike, expiry = parts
                Option = _ib_api.Option
                return Option(sym, expiry, float(strike), opt_type[0].upper(), "SMART")

        # Futures: check common symbols
        if product_id in ("ES", "NQ", "YM", "RTY", "CL", "GC", "SI", "ZB", "ZN"):
            return Future(product_id, exchange="CME" if product_id not in ("CL", "GC", "SI") else "NYMEX")

        # Default: US Stock
        return Stock(product_id, "SMART", "USD")

    def get_accounts(self):
        """Get account summary matching CoinbaseTrader format."""
        self._ensure_connected()
        summary = self._ib.accountSummary()
        accounts = []
        for item in summary:
            if item.tag in ("TotalCashValue", "NetLiquidation", "BuyingPower"):
                accounts.append({
                    "currency": item.currency,
                    "tag": item.tag,
                    "available_balance": {"value": item.value},
                })
        return {"accounts": accounts}

    def get_portfolio(self):
        """Get current positions."""
        self._ensure_connected()
        positions = self._ib.portfolio()
        result = []
        for pos in positions:
            result.append({
                "symbol": pos.contract.symbol,
                "position": pos.position,
                "market_price": pos.marketPrice,
                "market_value": pos.marketValue,
                "avg_cost": pos.averageCost,
                "unrealized_pnl": pos.unrealizedPNL,
                "realized_pnl": pos.realizedPNL,
            })
        return result

    def place_order(self, product_id, side, size, order_type="market"):
        """Place a market order (matches CoinbaseTrader.place_order interface).

        Args:
            product_id: e.g., "AAPL", "BTC-USD", "EUR-USD"
            side: "BUY" or "SELL"
            size: number of shares/contracts/coins
            order_type: "market" only for now
        """
        MarketOrder = _ib_api.MarketOrder

        self._ensure_connected()
        contract = self._make_contract(product_id)
        self._ib.qualifyContracts(contract)

        order = MarketOrder(side.upper(), float(size))
        trade = self._ib.placeOrder(contract, order)
        self._ib.sleep(1)  # wait for order to process

        return {
            "success_response": {
                "order_id": str(trade.order.orderId),
                "status": trade.orderStatus.status,
            }
        }

    def place_limit_order(self, product_id, side, base_size, limit_price, post_only=False):
        """Place a limit order (matches CoinbaseTrader.place_limit_order interface)."""
        LimitOrder = _ib_api.LimitOrder

        self._ensure_connected()
        contract = self._make_contract(product_id)
        self._ib.qualifyContracts(contract)

        order = LimitOrder(side.upper(), float(base_size), float(limit_price))
        trade = self._ib.placeOrder(contract, order)
        self._ib.sleep(1)

        return {
            "success_response": {
                "order_id": str(trade.order.orderId),
                "status": trade.orderStatus.status,
            }
        }

    def get_order_fill(self, order_id, max_wait=10, poll_interval=1.0):
        """Get fill information for an order (matches CoinbaseTrader interface)."""
        self._ensure_connected()
        start = time.time()
        while time.time() - start < max_wait:
            for trade in self._ib.trades():
                if str(trade.order.orderId) == str(order_id):
                    status = trade.orderStatus.status
                    filled = trade.orderStatus.filled
                    avg_price = trade.orderStatus.avgFillPrice
                    if status in ("Filled", "Cancelled", "Inactive"):
                        return {
                            "filled_size": float(filled),
                            "filled_value": float(filled) * float(avg_price) if avg_price else 0,
                            "avg_price": float(avg_price) if avg_price else 0,
                            "status": status.upper(),
                        }
            self._ib.sleep(poll_interval)
        return None

    def cancel_order(self, order_id):
        """Cancel an open order."""
        self._ensure_connected()
        for trade in self._ib.trades():
            if str(trade.order.orderId) == str(order_id):
                self._ib.cancelOrder(trade.order)
                return {"success": True}
        return {"error": f"Order {order_id} not found"}

    def get_market_data(self, product_id):
        """Get real-time market data snapshot."""
        self._ensure_connected()
        contract = self._make_contract(product_id)
        self._ib.qualifyContracts(contract)
        ticker = self._ib.reqMktData(contract, snapshot=True)
        self._ib.sleep(2)
        return {
            "bid": ticker.bid,
            "ask": ticker.ask,
            "last": ticker.last,
            "volume": ticker.volume,
        }


if __name__ == "__main__":
    print("=== IBKR Connector ===")
    print(f"Status: Account APPROVED 2026-02-13")
    print(f"Backend: {_IB_BACKEND or 'NOT INSTALLED (pip install ib_async)'}")
    print("Priority: #1 exchange — stocks, options, futures, forex, bonds, crypto")
    print()
    print("To test:")
    print("  1. Start TWS/IB Gateway")
    print("  2. Enable API on port 7497 (paper) or 7496 (live)")
    print("  3. pip install ib_async  (or ib_insync as fallback)")
    print("  4. python ibkr_connector.py test")
    print("  5. Set IBKR_PAPER_ONLY=1 to prevent accidental live connections")
    print()

    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        trader = IBKRTrader()
        if trader.connect():
            print("Connected! Account summary:")
            print(trader.get_accounts())
            print("\nPortfolio:")
            for pos in trader.get_portfolio():
                print(f"  {pos['symbol']}: {pos['position']} @ ${pos['market_price']:.2f} "
                      f"(PnL: ${pos['unrealized_pnl']:.2f})")
            trader.disconnect()
        else:
            print("Connection failed — is TWS/IB Gateway running?")
