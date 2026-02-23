#!/usr/bin/env python3
"""Kraken Equity Signal Agent — generates equity trading signals for Kraken stocks.

Signal types:
  1. Crypto-equity correlation: BTC rally -> buy lagging COIN, MSTR, MARA
  2. SPY/QQQ momentum: Broad market trend signals
  3. Sector rotation: XLK/XLF/XLE relative strength
  4. Commission arbitrage: Route to Kraken (free) over E*Trade (fees)

Runs on configurable interval (default 60s during market hours).
Signals submitted via CreativeAgentBridge with market_type="equity",
venue_hint="kraken_stock".
"""

import json
import logging
import os
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [KRAKEN_EQUITY_SIGNAL] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "kraken_equity_signal_agent.log")),
    ],
)
logger = logging.getLogger("kraken_equity_signal_agent")

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"'))

try:
    from creative_agent_bridge import CreativeAgentBridge
except ImportError:
    CreativeAgentBridge = None  # type: ignore[assignment,misc]

try:
    from kraken_stock_connector import KrakenStockConnector, is_market_open
except ImportError:
    try:
        from agents.kraken_stock_connector import KrakenStockConnector, is_market_open  # type: ignore[no-redef]
    except ImportError:
        KrakenStockConnector = None  # type: ignore[assignment,misc]
        is_market_open = None  # type: ignore[assignment]

try:
    from kraken_connector import KrakenConnector
except ImportError:
    try:
        from agents.kraken_connector import KrakenConnector  # type: ignore[no-redef]
    except ImportError:
        KrakenConnector = None  # type: ignore[assignment,misc]

KRAKEN_EQUITY_SIGNAL_ENABLED = os.environ.get(
    "KRAKEN_EQUITY_SIGNAL_ENABLED", "1"
).lower() in ("1", "true", "yes")
KRAKEN_EQUITY_SCAN_INTERVAL_S = int(
    os.environ.get("KRAKEN_EQUITY_SCAN_INTERVAL_S", "60")
)

# ── Crypto-correlated stocks ──
CRYPTO_CORRELATED = {
    "BTC": ["COIN", "MSTR", "MARA", "RIOT", "BITO"],
    "ETH": ["ETHE"],
}

# ── Sector ETFs for rotation ──
SECTOR_ETFS = [
    "XLK", "XLF", "XLE", "XLV", "XLY",
    "XLI", "XLB", "XLP", "XLU", "XLRE",
]

# ── Price history window ──
PRICE_HISTORY_MAX = 120  # keep last 120 data points


class KrakenEquitySignalAgent:
    """Generates equity trading signals routed to Kraken (commission-free)."""

    def __init__(self):
        self.scan_interval = KRAKEN_EQUITY_SCAN_INTERVAL_S
        self.running = True
        self._price_history: dict = {}  # symbol -> deque[(timestamp, price)]
        self.bridge = (
            CreativeAgentBridge("kraken_equity_signal")
            if CreativeAgentBridge
            else None
        )

    def _get_price_history(self, symbol: str) -> deque:
        """Get or create price history deque for a symbol."""
        if symbol not in self._price_history:
            self._price_history[symbol] = deque(maxlen=PRICE_HISTORY_MAX)
        return self._price_history[symbol]

    def _record_price(self, symbol: str, price: float):
        """Record a price observation for a symbol."""
        history = self._get_price_history(symbol)
        history.append((time.time(), price))

    def _get_price_change_pct(self, symbol: str, lookback_s: float = 3600) -> float:
        """Calculate price change percentage over lookback window.

        Args:
            symbol: Asset symbol
            lookback_s: Lookback window in seconds (default 1 hour)

        Returns:
            Price change as percentage (e.g., 2.5 for +2.5%), or 0 if insufficient data.
        """
        history = self._get_price_history(symbol)
        if len(history) < 2:
            return 0.0

        current_price = history[-1][1]
        cutoff = time.time() - lookback_s

        # Find oldest price within lookback window
        old_price = None
        for ts, p in history:
            if ts >= cutoff:
                old_price = p
                break

        if old_price is None or old_price <= 0:
            return 0.0

        return ((current_price - old_price) / old_price) * 100.0

    def scan_crypto_equity_correlation(self):
        """When BTC/ETH rallies, buy lagging correlated stocks.

        Logic: If crypto is up >2% in the last hour and the correlated
        stock is lagging (up less than 1%), emit a BUY signal for the stock.
        """
        if KrakenConnector is None or KrakenStockConnector is None:
            return

        for crypto, stocks in CRYPTO_CORRELATED.items():
            # Get crypto price from Kraken
            try:
                crypto_data = KrakenConnector.get_24h_volume(f"{crypto}-USD")
                if crypto_data.get("error"):
                    continue
                crypto_price = float(crypto_data.get("last_price", 0))
                if crypto_price <= 0:
                    continue
            except Exception as e:
                logger.debug("Crypto price fetch failed for %s: %s", crypto, e)
                continue

            # Record crypto price
            self._record_price(crypto, crypto_price)
            crypto_change = self._get_price_change_pct(crypto, lookback_s=3600)

            if crypto_change <= 2.0:
                # Crypto not rallying enough, skip
                continue

            # Check each correlated stock
            for stock in stocks:
                try:
                    stock_quote = KrakenStockConnector.get_stock_quote(stock)
                    if stock_quote.get("error"):
                        continue
                    stock_price = float(stock_quote.get("last_price", 0))
                    if stock_price <= 0:
                        continue
                except Exception as e:
                    logger.debug("Stock quote failed for %s: %s", stock, e)
                    continue

                # Record stock price
                self._record_price(stock, stock_price)
                stock_change = self._get_price_change_pct(stock, lookback_s=3600)

                # If stock is lagging crypto by >1%, emit BUY signal
                lag = crypto_change - stock_change
                if lag > 1.0:
                    confidence = min(0.85, 0.70 + lag * 0.03)
                    self._emit_signal(
                        symbol=stock,
                        direction="BUY",
                        confidence=confidence,
                        signal_type="crypto_equity_correlation",
                        extra={
                            "crypto_base": crypto,
                            "crypto_change_pct": round(crypto_change, 2),
                            "stock_change_pct": round(stock_change, 2),
                            "lag_pct": round(lag, 2),
                        },
                    )

    def scan_market_momentum(self):
        """SPY/QQQ momentum signals.

        Logic: Track intraday price changes on major ETFs. Strong momentum
        (>0.5% in last 30 min) triggers BUY; reversal (>1% drop) triggers SELL.
        """
        if KrakenStockConnector is None:
            return

        for etf in ["SPY", "QQQ"]:
            try:
                quote = KrakenStockConnector.get_stock_quote(etf)
                if quote.get("error"):
                    continue
                price = float(quote.get("last_price", 0))
                if price <= 0:
                    continue
            except Exception as e:
                logger.debug("Market momentum quote failed for %s: %s", etf, e)
                continue

            self._record_price(etf, price)

            # 30-minute momentum
            change_30m = self._get_price_change_pct(etf, lookback_s=1800)
            # 1-hour change
            change_1h = self._get_price_change_pct(etf, lookback_s=3600)

            if change_30m > 0.5:
                # Strong positive momentum
                confidence = min(0.82, 0.70 + change_30m * 0.08)
                self._emit_signal(
                    symbol=etf,
                    direction="BUY",
                    confidence=confidence,
                    signal_type="market_momentum",
                    extra={
                        "change_30m_pct": round(change_30m, 2),
                        "change_1h_pct": round(change_1h, 2),
                    },
                )
            elif change_30m < -1.0:
                # Reversal / sell-off
                confidence = min(0.78, 0.70 + abs(change_30m) * 0.04)
                self._emit_signal(
                    symbol=etf,
                    direction="SELL",
                    confidence=confidence,
                    signal_type="market_momentum",
                    extra={
                        "change_30m_pct": round(change_30m, 2),
                        "change_1h_pct": round(change_1h, 2),
                    },
                )

    def scan_sector_rotation(self):
        """Track sector ETF relative strength.

        Strongest sectors get BUY signals, weakest get SELL signals.
        """
        if KrakenStockConnector is None:
            return

        sector_prices = {}
        for etf in SECTOR_ETFS:
            try:
                quote = KrakenStockConnector.get_stock_quote(etf)
                if quote.get("error"):
                    continue
                price = float(quote.get("last_price", 0))
                if price <= 0:
                    continue
                sector_prices[etf] = price
                self._record_price(etf, price)
            except Exception as e:
                logger.debug("Sector quote failed for %s: %s", etf, e)
                continue

        if len(sector_prices) < 3:
            return

        # Calculate relative performance (1h change)
        sector_perf = {}
        for etf in sector_prices:
            change = self._get_price_change_pct(etf, lookback_s=3600)
            sector_perf[etf] = change

        if not sector_perf:
            return

        # Sort by performance
        sorted_sectors = sorted(sector_perf.items(), key=lambda x: x[1], reverse=True)

        # Top sector: BUY if outperforming by >0.3%
        top_etf, top_change = sorted_sectors[0]
        avg_change = sum(sector_perf.values()) / len(sector_perf)
        if top_change - avg_change > 0.3 and top_change > 0:
            confidence = min(0.80, 0.70 + (top_change - avg_change) * 0.05)
            self._emit_signal(
                symbol=top_etf,
                direction="BUY",
                confidence=confidence,
                signal_type="sector_rotation",
                extra={
                    "sector_change_pct": round(top_change, 2),
                    "avg_change_pct": round(avg_change, 2),
                    "relative_strength": round(top_change - avg_change, 2),
                },
            )

        # Bottom sector: SELL if underperforming by >0.3%
        bot_etf, bot_change = sorted_sectors[-1]
        if avg_change - bot_change > 0.3 and bot_change < 0:
            confidence = min(0.78, 0.70 + (avg_change - bot_change) * 0.04)
            self._emit_signal(
                symbol=bot_etf,
                direction="SELL",
                confidence=confidence,
                signal_type="sector_rotation",
                extra={
                    "sector_change_pct": round(bot_change, 2),
                    "avg_change_pct": round(avg_change, 2),
                    "relative_weakness": round(avg_change - bot_change, 2),
                },
            )

    def scan_commission_arbitrage(self):
        """If a stock signal exists for E*Trade, check if Kraken is cheaper.

        Kraken is commission-free, so any stock available on both venues
        should prefer Kraken. Emits signals with venue_hint="kraken_stock"
        and a confidence boost for the cost advantage.
        """
        if KrakenStockConnector is None:
            return

        # Check popular stocks that are likely also on E*Trade
        for symbol in KrakenStockConnector.POPULAR_STOCKS:
            try:
                quote = KrakenStockConnector.get_stock_quote(symbol)
                if quote.get("error"):
                    continue
                price = float(quote.get("last_price", 0))
                if price <= 0:
                    continue

                # Record price for other signal scanners
                self._record_price(symbol, price)

                # Check if spread is reasonable (< 0.5%)
                spread = float(quote.get("spread", 0))
                if price > 0 and spread > 0:
                    spread_pct = (spread / price) * 100
                    if spread_pct > 0.5:
                        # Spread too wide, skip
                        continue

                    # Commission arbitrage: Kraken has 0% commission
                    # E*Trade charges ~$0.50-$6.95 per trade
                    # For small trades ($5-$50), this is significant
                    # Emit a venue preference signal
                    self._emit_signal(
                        symbol=symbol,
                        direction="BUY",
                        confidence=0.71,  # Just above threshold
                        signal_type="commission_arbitrage",
                        extra={
                            "spread_pct": round(spread_pct, 4),
                            "commission_savings": "0% vs E*Trade fees",
                            "price": price,
                        },
                    )
            except Exception as e:
                logger.debug("Commission arb check failed for %s: %s", symbol, e)
                continue

    def _emit_signal(self, symbol, direction, confidence, signal_type, extra=None):
        """Submit equity signal via CreativeAgentBridge.

        All signals are tagged with venue_hint="kraken_stock" to ensure
        they route to the Kraken stock connector (commission-free).
        """
        pair = f"{symbol}-USD"
        if self.bridge:
            # Build the signal to submit through the bridge
            # The bridge submit_signal method has a fixed API, so we
            # build a reasoning string that includes signal_type and extras
            extra_info = json.dumps(extra) if extra else ""
            reasoning = f"{signal_type}: {extra_info}"

            ok = self.bridge.submit_signal(
                pair=pair,
                direction=direction,
                confidence=confidence,
                urgency="medium",
                reasoning=reasoning,
                market_type="equity",
            )
            if ok:
                logger.info(
                    "Kraken equity signal: %s %s (conf=%.2f, type=%s)",
                    direction, pair, confidence, signal_type,
                )

                # Also inject venue_hint into the orchestrator's signal collector
                # so the orchestrator routes to kraken_stock handler
                if hasattr(self.bridge, "_orchestrator") and self.bridge._orchestrator:
                    collector = self.bridge._orchestrator.signal_collector
                    with collector.lock:
                        sig = collector.signals.get(f"kraken_equity_signal")
                        if sig:
                            sig["venue_hint"] = "kraken_stock"
                            sig["signal_type"] = signal_type
                            if extra:
                                sig.update(extra)
        else:
            logger.warning(
                "No bridge available, signal dropped: %s %s", direction, pair
            )

    def run_scan_cycle(self):
        """Run all signal scans (only during market hours)."""
        if not KRAKEN_EQUITY_SIGNAL_ENABLED:
            return

        if is_market_open is None or not is_market_open():
            logger.debug("Market closed, skipping Kraken equity scan")
            return

        logger.info("Running Kraken equity signal scan...")
        self.scan_crypto_equity_correlation()
        self.scan_market_momentum()
        self.scan_sector_rotation()
        self.scan_commission_arbitrage()

    def run(self):
        """Main loop."""
        logger.info(
            "Kraken Equity Signal Agent started (interval=%ds)", self.scan_interval
        )
        while self.running:
            try:
                self.run_scan_cycle()
            except Exception as e:
                logger.error("Scan cycle error: %s", e, exc_info=True)
            time.sleep(self.scan_interval)


def main():
    """Entry point for standalone execution."""
    agent = KrakenEquitySignalAgent()
    agent.run()


if __name__ == "__main__":
    main()
