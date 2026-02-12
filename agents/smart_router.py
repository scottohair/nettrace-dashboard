#!/usr/bin/env python3
"""Smart Order Router — routes orders to the cheapest venue.

Compares all available venues (CEX + DEX) and picks the one with
the lowest total cost (price + fees + gas + slippage).

Venues:
  - Coinbase Advanced (CEX): 0.4% maker / 0.6% taker
  - Uniswap V3 on Base (DEX): 0.05-0.3% fee, low gas
  - Uniswap V3 on Ethereum (DEX): 0.05-0.3% fee, higher gas
  - Jupiter on Solana (DEX): 0% platform fee, ~0.25% slippage

Usage:
    from smart_router import SmartRouter
    router = SmartRouter()
    best = router.find_best_execution("BTC-USD", "BUY", 5.00)
    print(best)  # {'venue': 'uniswap_base', 'total_cost_pct': 0.35, ...}
"""

import json
import logging
import os
import time
import urllib.request
from pathlib import Path

logger = logging.getLogger("smart_router")

# Gas price estimates (in USD) per chain — updated periodically
DEFAULT_GAS_COSTS_USD = {
    "ethereum": 5.00,   # ~$5 for a swap on mainnet
    "base": 0.01,       # ~$0.01 on Base L2
    "arbitrum": 0.10,   # ~$0.10 on Arbitrum
    "polygon": 0.02,    # ~$0.02 on Polygon
    "solana": 0.001,    # ~$0.001 on Solana
    "coinbase": 0.00,   # No gas, just fees
}


def _fetch_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


class SmartRouter:
    """Routes orders to the best venue (CEX vs DEX)."""

    def __init__(self, coinbase_tools=None, dex_connector=None):
        """
        Args:
            coinbase_tools: AgentTools instance (for Coinbase quotes)
            dex_connector: DEXConnector instance (for DEX quotes)
        """
        self._coinbase = coinbase_tools
        self._dex = dex_connector
        self._price_cache = {}
        self._cache_time = 0

    @property
    def coinbase(self):
        if self._coinbase is None:
            try:
                from agent_tools import AgentTools
                self._coinbase = AgentTools()
            except Exception as e:
                logger.warning("Coinbase tools not available: %s", e)
        return self._coinbase

    @property
    def dex(self):
        if self._dex is None:
            try:
                from dex_connector import DEXConnector
                self._dex = DEXConnector(chain="base")
            except Exception as e:
                logger.warning("DEX connector not available: %s", e)
        return self._dex

    def find_best_execution(self, pair, side, amount_usd):
        """Compare all venues and route to the cheapest one.

        Args:
            pair: Trading pair (e.g. "BTC-USD", "ETH-USD", "SOL-USD")
            side: "BUY" or "SELL"
            amount_usd: Dollar amount to trade

        Returns: {
            'venue': 'uniswap_base',
            'price': 68050.00,
            'fee_pct': 0.30,
            'gas_usd': 0.01,
            'slippage_pct': 0.05,
            'total_cost_pct': 0.36,
            'amount_out': 0.00007345,
            'savings_vs_coinbase': 0.24,
            'all_venues': [...]
        }
        """
        base_asset = pair.split("-")[0]  # "BTC" from "BTC-USD"
        venues = []

        # ── 1. Coinbase quote ──
        cb_quote = self._get_coinbase_quote(pair, side, amount_usd)
        if cb_quote:
            venues.append(cb_quote)

        # ── 2. Uniswap quotes (Base, then Ethereum) ──
        for chain in ["base", "arbitrum", "ethereum"]:
            uni_quote = self._get_uniswap_quote(base_asset, side, amount_usd, chain)
            if uni_quote:
                venues.append(uni_quote)

        # ── 3. Jupiter quote (Solana — only for SOL/SPL tokens) ──
        if base_asset in ("SOL", "BONK", "JUP"):
            jup_quote = self._get_jupiter_quote(base_asset, side, amount_usd)
            if jup_quote:
                venues.append(jup_quote)

        if not venues:
            return {"error": "No venues available", "pair": pair}

        # Sort by total_cost_pct (lowest cost wins)
        venues.sort(key=lambda v: v.get("total_cost_pct", 999))
        best = venues[0]

        # Calculate savings vs Coinbase
        cb = next((v for v in venues if v["venue"] == "coinbase"), None)
        if cb and best["venue"] != "coinbase":
            best["savings_vs_coinbase"] = round(
                cb["total_cost_pct"] - best["total_cost_pct"], 4
            )
        else:
            best["savings_vs_coinbase"] = 0

        best["all_venues"] = venues
        return best

    def _get_coinbase_quote(self, pair, side, amount_usd):
        """Get Coinbase price + fee estimate."""
        try:
            # Public ticker (no auth needed)
            url = f"https://api.exchange.coinbase.com/products/{pair}/ticker"
            data = _fetch_json(url)
            price = float(data.get("price", 0))
            if price <= 0:
                return None

            # Coinbase fees: 0.4% maker (limit), 0.6% taker (market)
            # We use maker fee since our strategy is limit orders
            fee_pct = 0.40  # maker
            base_amount = amount_usd / price
            fee_usd = amount_usd * (fee_pct / 100)

            return {
                "venue": "coinbase",
                "chain": "cex",
                "price": price,
                "amount_out": round(base_amount, 8) if side == "BUY" else round(amount_usd, 2),
                "fee_pct": fee_pct,
                "gas_usd": 0,
                "slippage_pct": 0,  # limit orders = no slippage
                "total_cost_pct": round(fee_pct, 4),
                "total_cost_usd": round(fee_usd, 4),
            }
        except Exception as e:
            logger.debug("Coinbase quote failed: %s", e)
            return None

    def _get_uniswap_quote(self, base_asset, side, amount_usd, chain):
        """Get Uniswap quote on a specific chain."""
        if not self.dex:
            return None

        try:
            # For BUY: swap USDC → base_asset
            # For SELL: swap base_asset → USDC
            if side == "BUY":
                token_in, token_out = "USDC", base_asset
                # Need to figure out how much base_asset we get for amount_usd USDC
                quote = self.dex.get_quote_uniswap("USDC", base_asset, amount_usd, chain)
            else:
                token_in, token_out = base_asset, "USDC"
                # Get price first to determine base_amount
                price = self._get_spot_price(base_asset)
                if price <= 0:
                    return None
                base_amount = amount_usd / price
                quote = self.dex.get_quote_uniswap(base_asset, "USDC", base_amount, chain)

            if "error" in quote:
                return None

            gas_usd = DEFAULT_GAS_COSTS_USD.get(chain, 1.00)
            fee_pct = quote.get("fee_pct", 0.30)
            # Estimate slippage from amount difference
            expected_price = self._get_spot_price(base_asset)
            if expected_price and side == "BUY":
                expected_out = amount_usd / expected_price
                actual_out = quote.get("amount_out", 0)
                slippage_pct = ((expected_out - actual_out) / expected_out * 100) if expected_out > 0 else 0
            else:
                slippage_pct = 0.05  # estimate

            total_cost_pct = fee_pct + max(0, slippage_pct) + (gas_usd / amount_usd * 100 if amount_usd > 0 else 0)

            return {
                "venue": f"uniswap_{chain}",
                "chain": chain,
                "price": quote.get("price", 0),
                "amount_out": quote.get("amount_out", 0),
                "fee_pct": round(fee_pct, 4),
                "gas_usd": gas_usd,
                "slippage_pct": round(max(0, slippage_pct), 4),
                "total_cost_pct": round(total_cost_pct, 4),
                "total_cost_usd": round(amount_usd * total_cost_pct / 100, 4),
                "fee_tier": quote.get("fee_tier"),
            }
        except Exception as e:
            logger.debug("Uniswap %s quote failed: %s", chain, e)
            return None

    def _get_jupiter_quote(self, base_asset, side, amount_usd):
        """Get Jupiter quote on Solana."""
        if not self.dex:
            return None

        try:
            price = self._get_spot_price(base_asset)
            if price <= 0:
                return None

            if side == "BUY":
                quote = self.dex.get_quote_jupiter("USDC", base_asset, amount_usd)
            else:
                base_amount = amount_usd / price
                quote = self.dex.get_quote_jupiter(base_asset, "USDC", base_amount)

            if "error" in quote:
                return None

            gas_usd = DEFAULT_GAS_COSTS_USD["solana"]
            price_impact = abs(quote.get("price_impact_pct", 0))
            total_cost_pct = price_impact + (gas_usd / amount_usd * 100 if amount_usd > 0 else 0)

            return {
                "venue": "jupiter_solana",
                "chain": "solana",
                "price": quote.get("price", 0),
                "amount_out": quote.get("amount_out", 0),
                "fee_pct": 0.0,
                "gas_usd": gas_usd,
                "slippage_pct": round(price_impact, 4),
                "total_cost_pct": round(total_cost_pct, 4),
                "total_cost_usd": round(amount_usd * total_cost_pct / 100, 4),
            }
        except Exception as e:
            logger.debug("Jupiter quote failed: %s", e)
            return None

    def _get_spot_price(self, symbol):
        """Get spot price from cache or Coinbase."""
        now = time.time()
        if symbol in self._price_cache and now - self._cache_time < 30:
            return self._price_cache[symbol]

        try:
            url = f"https://api.exchange.coinbase.com/products/{symbol}-USD/ticker"
            data = _fetch_json(url)
            price = float(data.get("price", 0))
            self._price_cache[symbol] = price
            self._cache_time = now
            return price
        except Exception:
            return 0

    def compare_venues(self, pair, amount_usd=5.00):
        """Quick comparison table of all venues for display.

        Returns list of venue dicts sorted by total cost.
        """
        result = self.find_best_execution(pair, "BUY", amount_usd)
        if "error" in result:
            return []
        return result.get("all_venues", [])

    def get_routing_recommendation(self, pair, side, amount_usd):
        """Get a human-readable routing recommendation.

        Returns: {
            'recommendation': 'Use Uniswap on Base — saves 0.24% vs Coinbase',
            'venue': 'uniswap_base',
            'execute': True,
            'details': {...}
        }
        """
        best = self.find_best_execution(pair, side, amount_usd)
        if "error" in best:
            return {"recommendation": "No venues available", "execute": False}

        venue = best["venue"]
        savings = best.get("savings_vs_coinbase", 0)

        if venue == "coinbase":
            rec = f"Use Coinbase ({best['fee_pct']}% maker fee) — best available price"
        elif savings > 0.1:
            rec = f"Use {venue} — saves {savings:.2f}% vs Coinbase"
        else:
            rec = f"Use {venue} — marginal improvement ({savings:.2f}%) over Coinbase"

        return {
            "recommendation": rec,
            "venue": venue,
            "execute": True,
            "details": best,
        }


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    router = SmartRouter()

    if len(sys.argv) > 1:
        pair = sys.argv[1]
        amount = float(sys.argv[2]) if len(sys.argv) > 2 else 5.00
    else:
        pair = "BTC-USD"
        amount = 5.00

    print(f"\nSmart Router: Best execution for ${amount:.2f} {pair}")
    print("=" * 60)

    result = router.find_best_execution(pair, "BUY", amount)
    if "error" in result:
        print(f"  Error: {result['error']}")
    else:
        print(f"\n  BEST: {result['venue']}")
        print(f"  Price:    ${result.get('price', 0):,.2f}")
        print(f"  Fee:      {result.get('fee_pct', 0):.2f}%")
        print(f"  Gas:      ${result.get('gas_usd', 0):.4f}")
        print(f"  Slippage: {result.get('slippage_pct', 0):.2f}%")
        print(f"  TOTAL:    {result.get('total_cost_pct', 0):.2f}%")
        if result.get("savings_vs_coinbase"):
            print(f"  Savings:  {result['savings_vs_coinbase']:.2f}% vs Coinbase")

        print(f"\n  All venues:")
        for v in result.get("all_venues", []):
            marker = " ◀ BEST" if v["venue"] == result["venue"] else ""
            print(f"    {v['venue']:<20} fee={v['fee_pct']:.2f}% "
                  f"gas=${v.get('gas_usd', 0):.4f} "
                  f"total={v.get('total_cost_pct', 0):.2f}%{marker}")

    rec = router.get_routing_recommendation(pair, "BUY", amount)
    print(f"\n  Recommendation: {rec['recommendation']}")
