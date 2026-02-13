#!/usr/bin/env python3
"""Interactive Brokers connector — scaffold ready for when IBKR account is approved.

Same interface as exchange_connector.py CoinbaseTrader so agents can seamlessly
route orders to IBKR for stocks, options, futures, forex, and bonds.

Uses ib_insync library (pip install ib_insync) for TWS API communication.

STATUS: Account pending (submitted 2026-02-12, email: ohariscott@gmail.com)
        Usually approved within 1-3 business days.
        Priority #1 exchange — gives access to all asset classes.

SETUP WHEN APPROVED:
  1. Install TWS or IB Gateway
  2. Enable API access in TWS settings (port 7497 for paper, 7496 for live)
  3. pip install ib_insync
  4. Set IBKR_HOST, IBKR_PORT, IBKR_CLIENT_ID in agents/.env
"""

import logging
import os
import time
from pathlib import Path

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

    def __init__(self, host=None, port=None, client_id=None):
        self.host = host or IBKR_HOST
        self.port = port or IBKR_PORT
        self.client_id = client_id or IBKR_CLIENT_ID
        self._ib = None
        self._connected = False

    def connect(self):
        """Connect to TWS/IB Gateway."""
        try:
            from ib_insync import IB
            self._ib = IB()
            self._ib.connect(self.host, self.port, clientId=self.client_id)
            self._connected = True
            logger.info("Connected to IBKR at %s:%d (client %d)", self.host, self.port, self.client_id)
            return True
        except ImportError:
            logger.error("ib_insync not installed — run: pip install ib_insync")
            return False
        except Exception as e:
            logger.error("IBKR connection failed: %s", e)
            return False

    def disconnect(self):
        """Disconnect from TWS."""
        if self._ib and self._connected:
            self._ib.disconnect()
            self._connected = False
            logger.info("Disconnected from IBKR")

    def _ensure_connected(self):
        if not self._connected:
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
        from ib_insync import Stock, Crypto, Forex, Future, Contract

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
                from ib_insync import Option
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
        from ib_insync import MarketOrder

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
        from ib_insync import LimitOrder

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
    print("Status: Account PENDING (submitted 2026-02-12)")
    print("Priority: #1 exchange — stocks, options, futures, forex, bonds")
    print()
    print("To test when approved:")
    print("  1. Start TWS/IB Gateway")
    print("  2. Enable API on port 7497 (paper) or 7496 (live)")
    print("  3. pip install ib_insync")
    print("  4. python ibkr_connector.py test")
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
