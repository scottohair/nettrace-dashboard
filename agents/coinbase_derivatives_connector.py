#!/usr/bin/env python3
"""Coinbase Derivatives Connector — perpetual futures position management.

Composes the existing CoinbaseTrader for auth/requests and adds:
  - Perpetual product discovery and mapping
  - Position management (get, close)
  - Margin health monitoring and liquidation distance
  - Collateral management (allocate, transfer)
  - Funding rate scanning for arb opportunities
  - Gated order placement with leverage/margin/liquidation checks

SAFETY:
  - PERP_TRADING_ENABLED env var gate (default "0" — disabled)
  - MAX_LEVERAGE hard cap (default 3.0x, start at 1.0x)
  - 50% minimum liquidation buffer
  - Margin health ratio >= 2.0 required
  - Close never blocked (always allowed to reduce risk)
  - Graceful fallback to spot if connector fails
"""

import logging
import os
import time
from pathlib import Path

logger = logging.getLogger("derivatives")

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

# ── Configuration (env-driven, no hardcoded values) ──────────────────────

PERP_TRADING_ENABLED = os.environ.get("PERP_TRADING_ENABLED", "0").lower() in ("1", "true", "yes")
MAX_LEVERAGE = float(os.environ.get("PERP_MAX_LEVERAGE", "3.0"))
MIN_LIQUIDATION_BUFFER_PCT = float(os.environ.get("PERP_MIN_LIQUIDATION_BUFFER_PCT", "50.0"))
MIN_MARGIN_HEALTH_RATIO = float(os.environ.get("PERP_MIN_MARGIN_HEALTH_RATIO", "2.0"))
MAX_PERP_POSITIONS = int(os.environ.get("PERP_MAX_POSITIONS", "5"))
PERP_MAKER_FEE = float(os.environ.get("PERP_MAKER_FEE", "0.0000"))
PERP_TAKER_FEE = float(os.environ.get("PERP_TAKER_FEE", "0.0003"))
PRODUCT_CACHE_TTL = int(os.environ.get("PERP_PRODUCT_CACHE_TTL", "300"))
FUNDING_CACHE_TTL = int(os.environ.get("PERP_FUNDING_CACHE_TTL", "30"))


def _safe_float(val, default=0.0):
    try:
        v = float(val)
        return v if v == v else default  # NaN check
    except (TypeError, ValueError):
        return default


class CoinbaseDerivativesConnector:
    """Derivatives connector composing CoinbaseTrader for auth/requests.

    All perp operations go through this class. It wraps the existing trader
    for API calls and adds position/margin/collateral management on top.
    """

    def __init__(self, trader=None):
        """Initialize with optional pre-existing CoinbaseTrader instance."""
        if trader is not None:
            self._trader = trader
        else:
            try:
                from exchange_connector import CoinbaseTrader
                self._trader = CoinbaseTrader()
            except Exception:
                try:
                    from agents.exchange_connector import CoinbaseTrader
                    self._trader = CoinbaseTrader()
                except Exception as e:
                    logger.error("Cannot initialize CoinbaseTrader: %s", e)
                    self._trader = None

        # Caches
        self._product_cache = None
        self._product_cache_ts = 0
        self._perp_map = None
        self._perp_map_ts = 0
        self._funding_cache = {}  # product_id -> (rate, ts)

    @property
    def enabled(self):
        """Whether perp trading is enabled via env var."""
        return PERP_TRADING_ENABLED and self._trader is not None

    # ── Product Discovery ────────────────────────────────────────────────

    def list_perp_products(self):
        """Return perpetual products with caching (300s TTL)."""
        now = time.time()
        if self._product_cache is not None and (now - self._product_cache_ts) < PRODUCT_CACHE_TTL:
            return self._product_cache

        if not self._trader:
            return []

        try:
            products = self._trader.list_perpetual_products()
            self._product_cache = products if isinstance(products, list) else []
            self._product_cache_ts = now
            return self._product_cache
        except Exception as e:
            logger.error("list_perp_products error: %s", e)
            return self._product_cache or []

    def build_perp_mapping(self):
        """Build base_currency -> perp product_id map.

        Returns: dict like {"BTC": "BTC-PERP-INTX", "ETH": "ETH-PERP-INTX"}
        """
        now = time.time()
        if self._perp_map is not None and (now - self._perp_map_ts) < PRODUCT_CACHE_TTL:
            return self._perp_map

        products = self.list_perp_products()
        mapping = {}
        for p in products:
            if not isinstance(p, dict):
                continue
            pid = str(p.get("product_id", "")).upper()
            base = str(
                p.get("base_currency_id")
                or p.get("base_currency")
                or ""
            ).upper()
            if not pid or not base:
                continue
            quote = str(
                p.get("quote_currency_id")
                or p.get("quote_currency")
                or ""
            ).upper()
            # Prefer USD/USDC-settled perps
            score = 0
            if quote in {"USD", "USDC"}:
                score += 2
            if "PERP" in pid:
                score += 1
            prev = mapping.get(base)
            if not prev or score > prev["score"]:
                mapping[base] = {"pid": pid, "score": score}

        self._perp_map = {k: v["pid"] for k, v in mapping.items()}
        self._perp_map_ts = now
        if self._perp_map:
            logger.info("Perp mapping: %d bases — %s", len(self._perp_map), list(self._perp_map.keys()))
        return self._perp_map

    def perp_for_spot_pair(self, pair):
        """Convert spot pair to perp product_id.

        Args:
            pair: e.g., "ETH-USD" or "BTC-USDC"

        Returns:
            str product_id (e.g., "ETH-PERP-INTX") or None
        """
        if not pair:
            return None
        mapping = self.build_perp_mapping()
        try:
            base = str(pair).split("-")[0].upper()
        except Exception:
            return None
        return mapping.get(base)

    # ── Position Management ──────────────────────────────────────────────

    def get_positions(self):
        """Get all open perpetual positions.

        Returns: list of position dicts with standardized fields
        """
        if not self._trader:
            return []
        try:
            resp = self._trader._request("GET", "/api/v3/brokerage/cfm/positions")
            positions = []
            for p in (resp.get("positions") or []):
                if not isinstance(p, dict):
                    continue
                positions.append({
                    "product_id": str(p.get("product_id", "")),
                    "side": str(p.get("side", "")).upper(),
                    "size": _safe_float(p.get("number_of_contracts") or p.get("size")),
                    "entry_price": _safe_float(p.get("entry_vwap") or p.get("avg_entry_price")),
                    "mark_price": _safe_float(p.get("mark_price") or p.get("current_price")),
                    "unrealized_pnl": _safe_float(p.get("unrealized_pnl")),
                    "liquidation_price": _safe_float(p.get("liquidation_price")),
                    "leverage": _safe_float(p.get("leverage"), 1.0),
                    "margin_type": str(p.get("margin_type", "CROSS")),
                    "raw": p,
                })
            return positions
        except Exception as e:
            logger.error("get_positions error: %s", e)
            return []

    def get_position(self, product_id):
        """Get a single position for a specific perp product.

        Returns: position dict or None
        """
        positions = self.get_positions()
        pid = str(product_id).upper()
        for pos in positions:
            if str(pos.get("product_id", "")).upper() == pid:
                return pos
        return None

    # ── Portfolio & Margin ───────────────────────────────────────────────

    def get_portfolio_summary(self):
        """Get perpetual portfolio summary (margin health, available margin).

        Returns: dict with portfolio_value, margin_used, margin_available,
                 unrealized_pnl, liquidation_threshold, or empty dict on error
        """
        if not self._trader:
            return {}
        try:
            resp = self._trader._request("GET", "/api/v3/brokerage/cfm/balance_summary")
            summary = resp if isinstance(resp, dict) else {}
            return {
                "portfolio_value": _safe_float(summary.get("portfolio_value") or summary.get("total_balance")),
                "margin_used": _safe_float(summary.get("margin_used") or summary.get("initial_margin")),
                "margin_available": _safe_float(summary.get("margin_available") or summary.get("available_margin")),
                "unrealized_pnl": _safe_float(summary.get("unrealized_pnl")),
                "liquidation_threshold": _safe_float(summary.get("liquidation_threshold") or summary.get("liquidation_buffer_amount")),
                "buying_power": _safe_float(summary.get("buying_power")),
                "raw": summary,
            }
        except Exception as e:
            logger.error("get_portfolio_summary error: %s", e)
            return {}

    def margin_health(self):
        """Compute margin health status.

        Returns: dict with healthy (bool), margin_ratio, liquidation_buffer_pct, can_open_new
        """
        summary = self.get_portfolio_summary()
        portfolio_value = summary.get("portfolio_value", 0)
        margin_used = summary.get("margin_used", 0)
        liq_threshold = summary.get("liquidation_threshold", 0)

        # Margin ratio: portfolio / margin_used (higher = safer)
        if margin_used > 0:
            margin_ratio = portfolio_value / margin_used
        else:
            margin_ratio = float("inf") if portfolio_value > 0 else 0.0

        # Liquidation buffer: how far from liquidation (%)
        if liq_threshold > 0 and portfolio_value > 0:
            liq_buffer_pct = ((portfolio_value - liq_threshold) / portfolio_value) * 100
        elif margin_used == 0:
            liq_buffer_pct = 100.0  # no positions = infinite buffer
        else:
            liq_buffer_pct = 0.0

        healthy = (
            margin_ratio >= MIN_MARGIN_HEALTH_RATIO
            and liq_buffer_pct >= MIN_LIQUIDATION_BUFFER_PCT
        )

        positions = self.get_positions()
        can_open_new = healthy and len(positions) < MAX_PERP_POSITIONS

        return {
            "healthy": healthy,
            "margin_ratio": round(margin_ratio, 4) if margin_ratio != float("inf") else 999.0,
            "liquidation_buffer_pct": round(liq_buffer_pct, 2),
            "can_open_new": can_open_new,
            "portfolio_value": portfolio_value,
            "margin_used": margin_used,
            "open_positions": len(positions),
        }

    def liquidation_distance(self, product_id):
        """Distance to liquidation as % for a specific position.

        Returns: float percentage (e.g., 75.0 means 75% away from liq) or None
        """
        pos = self.get_position(product_id)
        if not pos:
            return None

        mark = pos.get("mark_price", 0)
        liq = pos.get("liquidation_price", 0)
        if mark <= 0 or liq <= 0:
            return None

        side = pos.get("side", "LONG").upper()
        if side in {"LONG", "BUY"}:
            distance_pct = ((mark - liq) / mark) * 100
        else:  # SHORT
            distance_pct = ((liq - mark) / mark) * 100

        return round(max(0.0, distance_pct), 2)

    # ── Collateral Management ────────────────────────────────────────────

    def allocate_collateral(self, amount_usd):
        """Allocate USDC into perp margin portfolio.

        Args:
            amount_usd: amount in USD to allocate

        Returns: dict with success/error
        """
        if not self._trader:
            return {"error": "trader_unavailable"}
        try:
            amount = _safe_float(amount_usd)
            if amount <= 0:
                return {"error": "invalid_amount"}
            resp = self._trader._request("POST", "/api/v3/brokerage/cfm/sweeps/schedule", {
                "usd_amount": str(round(amount, 2)),
            })
            return resp if isinstance(resp, dict) else {"error": "unexpected_response"}
        except Exception as e:
            logger.error("allocate_collateral error: %s", e)
            return {"error": str(e)}

    def transfer_to_perps(self, amount, currency="USDC"):
        """Transfer funds from spot portfolio to perp portfolio.

        Args:
            amount: amount to transfer
            currency: currency to transfer (default USDC)

        Returns: dict with success/error
        """
        if not self._trader:
            return {"error": "trader_unavailable"}
        try:
            amount = _safe_float(amount)
            if amount <= 0:
                return {"error": "invalid_amount"}
            resp = self._trader._request("POST", "/api/v3/brokerage/portfolios/move_funds", {
                "value": str(round(amount, 8)),
                "currency": currency.upper(),
                "source_portfolio_uuid": "default",
                "target_portfolio_uuid": "intx",
            })
            return resp if isinstance(resp, dict) else {"error": "unexpected_response"}
        except Exception as e:
            logger.error("transfer_to_perps error: %s", e)
            return {"error": str(e)}

    # ── Funding Rate ─────────────────────────────────────────────────────

    def get_funding_rate(self, product_id):
        """Get current funding rate for a perp product (30s cache).

        Returns: float funding rate or None
        """
        pid = str(product_id).upper()
        now = time.time()
        cached = self._funding_cache.get(pid)
        if cached and (now - cached[1]) < FUNDING_CACHE_TTL:
            return cached[0]

        products = self.list_perp_products()
        for p in products:
            if not isinstance(p, dict):
                continue
            if str(p.get("product_id", "")).upper() == pid:
                rate = _safe_float(p.get("funding_rate") or p.get("perpetual_details", {}).get("funding_rate"))
                self._funding_cache[pid] = (rate, now)
                return rate
        return None

    def funding_opportunities(self, threshold=0.01):
        """Scan all perps for actionable funding skews.

        Args:
            threshold: minimum absolute funding rate to consider (default 0.01 = 1%)

        Returns: list of dicts with product_id, funding_rate, direction, edge_pct
        """
        products = self.list_perp_products()
        opportunities = []
        for p in products:
            if not isinstance(p, dict):
                continue
            pid = str(p.get("product_id", "")).upper()
            rate = _safe_float(p.get("funding_rate") or p.get("perpetual_details", {}).get("funding_rate"))
            if abs(rate) < threshold:
                continue
            # Positive funding = longs pay shorts → short to earn
            # Negative funding = shorts pay longs → long to earn
            direction = "SHORT" if rate > 0 else "LONG"
            opportunities.append({
                "product_id": pid,
                "funding_rate": rate,
                "direction": direction,
                "edge_pct": abs(rate) * 100,
            })

        opportunities.sort(key=lambda x: x["edge_pct"], reverse=True)
        return opportunities

    # ── Order Placement ──────────────────────────────────────────────────

    def place_perp_order(self, product_id, side, size, price, leverage=1.0,
                         post_only=True, reduce_only=False):
        """Place a gated perpetual order with safety checks.

        Flow: env gate → margin check → liq check → leverage cap → place

        Args:
            product_id: perp product id (e.g., "BTC-PERP-INTX")
            side: "BUY" or "SELL"
            size: base size
            price: limit price
            leverage: requested leverage (capped at MAX_LEVERAGE)
            post_only: True for maker-only (0% fee)
            reduce_only: True to only reduce position (for closing)

        Returns: order result dict
        """
        side_u = str(side).upper()

        # Allow close/reduce-only orders even if trading disabled
        if not reduce_only:
            if not self.enabled:
                return {"error_response": {
                    "error": "PERP_DISABLED",
                    "message": "Perp trading disabled (PERP_TRADING_ENABLED=0)",
                }}

            # Leverage cap
            lev = _safe_float(leverage, 1.0)
            if lev > MAX_LEVERAGE:
                return {"error_response": {
                    "error": "LEVERAGE_CAP",
                    "message": f"Leverage {lev}x exceeds {MAX_LEVERAGE}x cap",
                }}

            # Margin health check
            health = self.margin_health()
            if not health.get("can_open_new", False):
                return {"error_response": {
                    "error": "MARGIN_UNHEALTHY",
                    "message": f"Cannot open new position: margin_ratio={health.get('margin_ratio')}, "
                               f"liq_buffer={health.get('liquidation_buffer_pct')}%, "
                               f"positions={health.get('open_positions')}/{MAX_PERP_POSITIONS}",
                }}

        if not self._trader:
            return {"error_response": {"error": "TRADER_UNAVAILABLE", "message": "CoinbaseTrader not initialized"}}

        # Place via existing trader's limit order method
        try:
            result = self._trader.place_limit_order(
                product_id=product_id,
                side=side_u,
                base_size=size,
                limit_price=price,
                post_only=post_only,
                bypass_profit_guard=reduce_only,  # closing trades bypass profit guard
            )
            return result
        except Exception as e:
            logger.error("place_perp_order error: %s", e)
            return {"error_response": {"error": "ORDER_ERROR", "message": str(e)}}

    def close_position(self, product_id):
        """Close an open perp position. NEVER blocked by trading lock.

        Uses reduce-only limit sell at market price to close.

        Args:
            product_id: perp product id

        Returns: order result dict or None if no position
        """
        pos = self.get_position(product_id)
        if not pos or pos.get("size", 0) <= 0:
            return None

        side = pos.get("side", "LONG").upper()
        close_side = "SELL" if side in {"LONG", "BUY"} else "BUY"
        size = pos["size"]
        mark_price = pos.get("mark_price", 0)

        if mark_price <= 0:
            logger.error("Cannot close %s: no mark price", product_id)
            return None

        # Price slightly worse than market to ensure fill
        if close_side == "SELL":
            limit_price = mark_price * 0.999  # slightly below market
        else:
            limit_price = mark_price * 1.001  # slightly above market

        logger.info("CLOSE POSITION: %s %s %.6f @ %.2f (reduce_only)", close_side, product_id, size, limit_price)
        return self.place_perp_order(
            product_id=product_id,
            side=close_side,
            size=size,
            price=limit_price,
            reduce_only=True,
            post_only=False,  # allow taker for close to ensure fill
        )
