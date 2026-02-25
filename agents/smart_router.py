#!/usr/bin/env python3
"""Smart Order Router — routes orders to the cheapest venue.

Compares all available venues (CEX + DEX) and picks the one with
the lowest total cost (price + fees + gas + slippage).

Venues:
  - Coinbase Advanced (CEX): 0.4% maker / 0.6% taker
  - Kraken Spot (CEX): 0.16% maker / 0.26% taker
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

# ── Dynamic Kraken fee cache (refreshed every hour) ──
_kraken_fee_cache = {"maker": 0.25, "taker": 0.40, "expires": 0}  # Kraken base tier ($0-$10k vol)

# ── Venue balance cache (refreshed every 60s) ──
_venue_balance_cache = {"kraken": {}, "coinbase": {}, "expires": 0}


def _get_venue_balances():
    """Get cached venue balances for routing decisions."""
    global _venue_balance_cache
    if time.time() < _venue_balance_cache["expires"]:
        return _venue_balance_cache
    try:
        from kraken_connector import KrakenConnector
        raw = KrakenConnector.get_account_balance()
        if isinstance(raw, dict) and "error" not in raw:
            # Kraken uses ZUSD, XXBT, XETH etc.
            usd = float(raw.get("ZUSD", 0) or 0)
            usdc = float(raw.get("USDC", 0) or 0)
            xbt = float(raw.get("XXBT", raw.get("XBT", 0)) or 0)
            eth = float(raw.get("XETH", raw.get("ETH", 0)) or 0)
            _venue_balance_cache["kraken"] = {
                "USD": usd, "USDC": usdc, "BTC": xbt, "ETH": eth,
                "quote_usd": usd + usdc,  # total quote currency available
            }
    except Exception:
        pass
    _venue_balance_cache["expires"] = time.time() + 60
    return _venue_balance_cache


def _get_kraken_fees():
    """Get dynamic Kraken fee tier (cached for 1 hour)."""
    global _kraken_fee_cache
    if time.time() < _kraken_fee_cache["expires"]:
        return _kraken_fee_cache
    try:
        from kraken_connector import KrakenConnector
        fees = KrakenConnector.get_fee_schedule()
        if fees and not fees.get("error"):
            _kraken_fee_cache = {
                "maker": fees.get("maker_fee", 0.16),
                "taker": fees.get("taker_fee", 0.26),
                "expires": time.time() + 3600,
            }
    except Exception:
        pass
    return _kraken_fee_cache


try:
    from execution_telemetry import venue_health_snapshot as _venue_health_snapshot
except Exception:
    try:
        from agents.execution_telemetry import venue_health_snapshot as _venue_health_snapshot  # type: ignore
    except Exception:
        def _venue_health_snapshot(*_args, **_kwargs):
            return {}

try:
    from route_account_registry import RouteAccountRegistry
except Exception:
    try:
        from agents.route_account_registry import RouteAccountRegistry  # type: ignore
    except Exception:
        RouteAccountRegistry = None

# Known stock/ETF symbols (for equity venue routing)
KNOWN_STOCK_SYMBOLS = {
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META",
    "SPY", "QQQ", "IWM", "DIA",
    "COIN", "MSTR", "MARA", "RIOT",
    "XLK", "XLF", "XLE", "XLV", "XLY", "XLI", "XLB", "XLP", "XLU", "XLRE",
    "AMD", "INTC", "NFLX", "GOOG", "JPM", "GS", "BAC",
    "BITO", "ETHE",
}

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

    def __init__(self, coinbase_tools=None, dex_connector=None, route_registry=None, strategy_tags=None):
        """
        Args:
            coinbase_tools: AgentTools instance (for Coinbase quotes)
            dex_connector: DEXConnector instance (for DEX quotes)
            route_registry: Optional RouteAccountRegistry instance
            strategy_tags: Optional list of strategy labels used for route/account selection
        """
        self._coinbase = coinbase_tools
        self._dex = dex_connector
        self._price_cache = {}
        self._price_cache_ttl_s = max(1.0, float(os.environ.get("SMART_ROUTER_SPOT_CACHE_TTL_S", "30")))
        self._split_enabled = os.environ.get("SMART_ROUTER_SPLIT_ENABLED", "1").lower() in ("1", "true", "yes")
        self._split_max_venues = max(1, int(os.environ.get("SMART_ROUTER_SPLIT_MAX_VENUES", "2") or 2))
        self._split_min_order_usd = max(0.1, float(os.environ.get("SMART_ROUTER_SPLIT_MIN_ORDER_USD", "1.0") or 1.0))
        self._split_min_savings_pct = max(0.0, float(os.environ.get("SMART_ROUTER_SPLIT_MIN_SAVINGS_PCT", "0.03") or 0.03))
        self._split_max_share_per_venue = min(
            1.0, max(0.2, float(os.environ.get("SMART_ROUTER_SPLIT_MAX_SHARE_PER_VENUE", "0.75") or 0.75))
        )
        self._strategy_tags = [str(x).strip().lower() for x in (strategy_tags or []) if str(x).strip()]
        if route_registry is not None:
            self._route_registry = route_registry
        elif RouteAccountRegistry is not None:
            try:
                self._route_registry = RouteAccountRegistry()
            except Exception:
                self._route_registry = None
        else:
            self._route_registry = None

    def _estimate_capacity_usd(self, quote, amount_usd):
        """Estimate notional capacity for low-slippage execution on a venue."""
        venue = str(quote.get("venue", "") or "").lower()
        chain = str(quote.get("chain", "") or "").lower()
        price = float(quote.get("price", 0.0) or 0.0)
        volume_24h = float(quote.get("volume_24h", 0.0) or 0.0)
        slippage_pct = max(0.0, float(quote.get("slippage_pct", 0.0) or 0.0))
        baseline = max(0.0, float(amount_usd or 0.0))

        capacity = baseline
        if volume_24h > 0 and price > 0:
            notional_24h = volume_24h * price
            # Conservative share of daily venue liquidity.
            capacity = max(baseline, min(notional_24h * 0.008, notional_24h * 0.02))
        elif chain in {"base", "arbitrum", "ethereum", "solana"}:
            # AMM routes can handle moderate clips if price impact is low.
            slippage_guard = max(0.05, slippage_pct)
            capacity = max(baseline, baseline * max(1.0, 0.35 / slippage_guard))
        elif venue == "coinbase":
            capacity = max(baseline, baseline * 2.5)
        elif venue == "kraken":
            capacity = max(baseline, baseline * 2.0)
        else:
            capacity = max(baseline, baseline * 1.5)

        if slippage_pct >= 0.20:
            capacity *= 0.75
        if slippage_pct >= 0.35:
            capacity *= 0.60

        cap = max(self._split_min_order_usd, float(capacity))
        liq_score = min(5.0, max(0.1, cap / max(self._split_min_order_usd, baseline, 1.0)))
        return cap, liq_score

    @staticmethod
    def _estimate_slice_cost_pct(quote, slice_amount_usd, full_amount_usd):
        """Estimate per-slice effective cost from quote primitives."""
        slice_amt = max(0.01, float(slice_amount_usd or 0.0))
        full_amt = max(slice_amt, float(full_amount_usd or slice_amt))
        fee_pct = max(0.0, float(quote.get("fee_pct", 0.0) or 0.0))
        gas_usd = max(0.0, float(quote.get("gas_usd", 0.0) or 0.0))
        slippage_full = max(0.0, float(quote.get("slippage_pct", 0.0) or 0.0))
        latency_penalty = max(0.0, float(quote.get("latency_penalty_pct", 0.0) or 0.0))
        cap_usd = max(0.0, float(quote.get("capacity_usd", full_amt) or full_amt))

        # Slippage generally grows sub-linearly with order size.
        scale = max(0.25, min(2.0, (slice_amt / full_amt) ** 0.5))
        slippage_pct = slippage_full * scale
        gas_pct = (gas_usd / slice_amt) * 100.0 if slice_amt > 0 else 0.0

        capacity_penalty = 0.0
        if cap_usd > 0 and slice_amt > cap_usd:
            overflow_ratio = (slice_amt - cap_usd) / slice_amt
            capacity_penalty = min(0.60, max(0.0, overflow_ratio * 1.2))

        return round(fee_pct + slippage_pct + gas_pct + latency_penalty + capacity_penalty, 6)

    def _build_split_plan(self, venues, amount_usd):
        """Build a cost/liquidity-aware split plan across venues."""
        notional = max(0.0, float(amount_usd or 0.0))
        if not self._split_enabled or notional <= 0 or not venues:
            return {
                "enabled": False,
                "strategy": "single",
                "reason": "split_disabled_or_no_notional",
                "slices": [],
            }

        ranked = [dict(v) for v in venues if float(v.get("capacity_usd", 0.0) or 0.0) >= self._split_min_order_usd]
        if not ranked:
            return {
                "enabled": False,
                "strategy": "single",
                "reason": "no_capacity",
                "slices": [],
            }

        ranked = ranked[: max(1, self._split_max_venues)]
        min_order = self._split_min_order_usd
        max_share = self._split_max_share_per_venue

        scored = []
        for v in ranked:
            cost_pct = max(0.0001, float(v.get("total_cost_pct", 0.0) or 0.0))
            liq = max(0.1, float(v.get("liquidity_score", 1.0) or 1.0))
            score = (1.0 / cost_pct) * min(4.0, max(0.25, liq))
            scored.append((v, score))

        score_total = sum(score for _, score in scored) or float(len(scored))
        remaining = notional
        slices = []

        for v, score in scored:
            target = notional * (score / score_total)
            target = min(target, notional * max_share, float(v.get("capacity_usd", notional) or notional), remaining)
            if target >= min_order:
                slices.append(
                    {
                        "venue": str(v.get("venue", "coinbase")),
                        "amount_usd": round(target, 4),
                        "expected_cost_pct": self._estimate_slice_cost_pct(v, target, notional),
                        "capacity_usd": float(v.get("capacity_usd", notional) or notional),
                        "liquidity_score": float(v.get("liquidity_score", 1.0) or 1.0),
                    }
                )
                remaining -= target

        # Greedy top-off by cheapest venues while respecting capacity/share constraints.
        if remaining >= min_order:
            for v, _score in scored:
                if remaining < min_order:
                    break
                venue_name = str(v.get("venue", "coinbase"))
                cap = float(v.get("capacity_usd", notional) or notional)
                hard_cap = min(cap, notional * max_share)
                entry = next((s for s in slices if s["venue"] == venue_name), None)
                used = float(entry.get("amount_usd", 0.0) or 0.0) if entry else 0.0
                room = max(0.0, hard_cap - used)
                if room < min_order:
                    continue
                add = min(room, remaining)
                if entry:
                    entry["amount_usd"] = round(float(entry["amount_usd"]) + add, 4)
                    entry["expected_cost_pct"] = self._estimate_slice_cost_pct(v, float(entry["amount_usd"]), notional)
                else:
                    slices.append(
                        {
                            "venue": venue_name,
                            "amount_usd": round(add, 4),
                            "expected_cost_pct": self._estimate_slice_cost_pct(v, add, notional),
                            "capacity_usd": cap,
                            "liquidity_score": float(v.get("liquidity_score", 1.0) or 1.0),
                        }
                    )
                remaining -= add

        # If still small remainder, assign to cheapest venue.
        if remaining > 0 and slices:
            slices[0]["amount_usd"] = round(float(slices[0]["amount_usd"]) + remaining, 4)
            ref_quote = next((v for v in ranked if str(v.get("venue", "")) == str(slices[0].get("venue", ""))), None)
            if isinstance(ref_quote, dict):
                slices[0]["expected_cost_pct"] = self._estimate_slice_cost_pct(
                    ref_quote,
                    float(slices[0]["amount_usd"]),
                    notional,
                )
            remaining = 0.0

        slices = [s for s in slices if float(s.get("amount_usd", 0.0) or 0.0) >= min_order]
        if not slices:
            return {
                "enabled": False,
                "strategy": "single",
                "reason": "no_valid_slices",
                "slices": [],
            }

        total_alloc = sum(float(s.get("amount_usd", 0.0) or 0.0) for s in slices) or notional
        blended_cost_pct = 0.0
        for s in slices:
            blended_cost_pct += float(s.get("expected_cost_pct", 0.0) or 0.0) * (
                float(s.get("amount_usd", 0.0) or 0.0) / total_alloc
            )

        single_cost_pct = float(venues[0].get("total_cost_pct", 0.0) or 0.0)
        savings_pct = max(0.0, single_cost_pct - blended_cost_pct)
        use_split = len(slices) > 1 and savings_pct >= self._split_min_savings_pct

        return {
            "enabled": use_split,
            "strategy": "split" if use_split else "single",
            "reason": "cost_and_liquidity_optimized" if use_split else "savings_below_threshold",
            "single_venue_cost_pct": round(single_cost_pct, 6),
            "blended_cost_pct": round(blended_cost_pct, 6),
            "estimated_savings_pct": round(savings_pct, 6),
            "estimated_savings_usd": round(notional * savings_pct / 100.0, 6),
            "total_amount_usd": round(notional, 6),
            "slices": slices,
        }

    @staticmethod
    def _latency_penalty_pct(venue_key, amount_usd):
        """Translate observed p90 latency into cost penalty (%).

        Kraken penalty is capped at 0.10% since fee savings (0.44% vs Coinbase)
        far outweigh any latency cost for limit orders.
        """
        venue = "coinbase" if venue_key.startswith("coinbase") else venue_key
        health = _venue_health_snapshot(venue, window_minutes=30) if venue else {}
        p90_ms = float(health.get("p90_latency_ms", 0.0) or 0.0)
        failure_rate = float(health.get("failure_rate", 0.0) or 0.0)
        if p90_ms <= 0 and failure_rate <= 0:
            return 0.0, p90_ms, failure_rate
        # Convert execution delay + failure risk into bps-style penalty.
        latency_component = min(0.20, max(0.0, p90_ms / 10000.0))  # 1000ms -> 0.10%
        reliability_component = min(0.30, max(0.0, failure_rate * 0.50))  # 20% fail -> 0.10%
        size_component = min(0.20, max(0.0, float(amount_usd or 0.0) / 10000.0))
        penalty = latency_component + reliability_component + size_component
        # Cap Kraken penalty — fee savings (0.44%) dwarf latency for limit orders
        if venue == "kraken":
            penalty = min(penalty, 0.10)
        return round(penalty, 4), round(p90_ms, 3), round(failure_rate, 4)

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

        # ── 2. Kraken quote (balance-aware routing) ──
        # Strategy: Kraken has 0.16% maker fees vs Coinbase 0.60% — always prefer
        # Kraken when we have assets there. For BUYs, check if we have quote currency
        # (USD/USDC) OR if we can convert held assets. For SELLs, always include
        # Kraken if we hold the base asset there (this converts to stablecoins/USD).
        kraken_quote = self._get_kraken_quote(pair, side, amount_usd)
        if kraken_quote:
            balances = _get_venue_balances()
            kraken_bal = balances.get("kraken", {})
            if side.upper() == "BUY":
                # Check total available: USD + USDC + value of held crypto
                kraken_quote_usd = float(kraken_bal.get("quote_usd", 0) or 0)
                # Also count ETH/BTC that could be sold first
                eth_val = float(kraken_bal.get("ETH", 0) or 0) * float(kraken_quote.get("price", 0) or 0) if base_asset != "ETH" else 0
                btc_val = float(kraken_bal.get("BTC", 0) or 0) * float(kraken_quote.get("price", 0) or 0) if base_asset != "BTC" else 0
                total_available = kraken_quote_usd + eth_val + btc_val
                if total_available >= amount_usd * 0.50:  # route if at least 50% fundable
                    kraken_quote["kraken_needs_conversion"] = kraken_quote_usd < amount_usd
                    venues.append(kraken_quote)
                else:
                    logger.debug("Kraken skipped for BUY: total available $%.2f < 50%% of $%.2f",
                                total_available, amount_usd)
            elif side.upper() == "SELL":
                # ALWAYS include Kraken for sells — this converts crypto → USD at lower fees
                # Even if the asset is on Coinbase, sniper will handle routing
                kraken_base = float(kraken_bal.get(base_asset, 0) or 0)
                if kraken_base > 0:
                    kraken_quote["kraken_has_base"] = True
                    kraken_quote["kraken_base_balance"] = kraken_base
                venues.append(kraken_quote)
            else:
                venues.append(kraken_quote)

        # ── 3. Uniswap quotes (Base, then Ethereum) ──
        for chain in ["base", "arbitrum", "ethereum"]:
            uni_quote = self._get_uniswap_quote(base_asset, side, amount_usd, chain)
            if uni_quote:
                venues.append(uni_quote)

        # ── 4. Jupiter quote (Solana — only for SOL/SPL tokens) ──
        if base_asset in ("SOL", "BONK", "JUP"):
            jup_quote = self._get_jupiter_quote(base_asset, side, amount_usd)
            if jup_quote:
                venues.append(jup_quote)

        # ── 5. Kraken Stock quote (equity symbols — commission-free) ──
        if base_asset in KNOWN_STOCK_SYMBOLS:
            ks_quote = self._get_kraken_stock_quote(base_asset, side, amount_usd)
            if ks_quote:
                venues.append(ks_quote)

        if not venues:
            return {"error": "No venues available", "pair": pair}

        route_plan = {}
        if self._route_registry is not None:
            try:
                route_plan = self._route_registry.plan(
                    pair=pair,
                    side=side,
                    strategy_tags=self._strategy_tags,
                )
            except Exception as e:
                logger.warning("Route registry plan failed for %s %s: %s", pair, side, e)
                route_plan = {}
        allowed_venues = route_plan.get("allowed_venues", []) if isinstance(route_plan, dict) else []
        if isinstance(allowed_venues, list) and allowed_venues:
            allowed_set = {str(v).strip().lower() for v in allowed_venues if str(v).strip()}
            filtered = [v for v in venues if str(v.get("venue", "")).strip().lower() in allowed_set]
            if filtered:
                venues = filtered
            else:
                return {
                    "error": "No venues match route policy",
                    "pair": pair,
                    "allowed_venues": sorted(allowed_set),
                    "available_venues": sorted(
                        {
                            str(v.get("venue", "")).strip().lower()
                            for v in venues
                            if str(v.get("venue", "")).strip()
                        }
                    ),
                }

        # Sort by total_cost_pct (lowest cost wins)
        for v in venues:
            v["quoted_amount_usd"] = round(float(amount_usd or 0.0), 6)
            cap_usd, liq_score = self._estimate_capacity_usd(v, amount_usd)
            v["capacity_usd"] = round(cap_usd, 4)
            v["liquidity_score"] = round(liq_score, 4)
            if amount_usd > 0 and cap_usd > 0 and amount_usd > cap_usd:
                overflow_ratio = (amount_usd - cap_usd) / amount_usd
                capacity_penalty = min(0.60, max(0.0, overflow_ratio * 1.2))
                v["capacity_penalty_pct"] = round(capacity_penalty, 4)
                v["total_cost_pct"] = round(float(v.get("total_cost_pct", 0.0) or 0.0) + capacity_penalty, 4)
                v["total_cost_usd"] = round(amount_usd * v["total_cost_pct"] / 100.0, 4)

            venue_key = str(v.get("venue", ""))
            penalty, p90_ms, fail_rate = self._latency_penalty_pct(venue_key, amount_usd)
            if penalty > 0:
                v["latency_penalty_pct"] = penalty
                v["observed_p90_latency_ms"] = p90_ms
                v["observed_failure_rate"] = fail_rate
                v["total_cost_pct"] = round(float(v.get("total_cost_pct", 0.0) or 0.0) + penalty, 4)
                v["total_cost_usd"] = round(amount_usd * v["total_cost_pct"] / 100.0, 4)

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

        if isinstance(route_plan, dict) and route_plan:
            best["route_policy"] = {
                "allowed_venues": list(route_plan.get("allowed_venues", [])),
                "route_count": int(route_plan.get("route_count", 0) or 0),
                "account_count": int(route_plan.get("account_count", 0) or 0),
            }
            accounts_by_venue = route_plan.get("accounts_by_venue", {})
            if isinstance(accounts_by_venue, dict):
                venue_accounts = accounts_by_venue.get(best.get("venue"), [])
                if isinstance(venue_accounts, list) and venue_accounts:
                    best["selected_account"] = str(venue_accounts[0])
                    best["candidate_accounts"] = [str(x) for x in venue_accounts[:5]]

        split_plan = self._build_split_plan(venues, amount_usd)
        best["split_plan"] = split_plan
        best["all_venues"] = venues

        # Net-edge computation: expected_return - total_cost
        # If caller provides expected_return_pct via self._expected_return_pct,
        # compute net_edge. Otherwise just embed total_cost for caller to use.
        total_cost = float(best.get("total_cost_pct", 0.0) or 0.0)
        best["net_edge_cost_pct"] = round(total_cost, 4)

        return best

    @staticmethod
    def compute_net_edge(expected_return_pct, total_cost_pct):
        """Compute net edge: expected return minus total execution cost.

        Returns net_edge_pct. Negative means the trade costs more than it returns.
        Used by sniper to reject negative-edge trades before execution.
        """
        return round(float(expected_return_pct or 0) - float(total_cost_pct or 0), 4)

    def plan_split_execution(self, pair, side, amount_usd):
        """Public helper: compute split-capable execution plan for a trade."""
        best = self.find_best_execution(pair, side, amount_usd)
        if "error" in best:
            return best
        plan = dict(best.get("split_plan", {}) or {})
        plan.setdefault("strategy", "single")
        plan.setdefault("enabled", False)
        plan["best_venue"] = str(best.get("venue", "coinbase"))
        plan["best_total_cost_pct"] = float(best.get("total_cost_pct", 0.0) or 0.0)
        return plan

    def _get_coinbase_quote(self, pair, side, amount_usd):
        """Get Coinbase price + fee estimate."""
        try:
            # Public ticker (no auth needed)
            url = f"https://api.exchange.coinbase.com/products/{pair}/ticker"
            data = _fetch_json(url)
            price = float(data.get("price", 0))
            if price <= 0:
                return None

            # Coinbase fees at <$1K/month tier: 0.60% maker, 1.20% taker
            # We use maker fee since our strategy is limit orders
            fee_pct = float(os.environ.get("COINBASE_MAKER_FEE_PCT", "0.60"))
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
                "latency_penalty_pct": 0.0,
                "total_cost_pct": round(fee_pct, 4),
                "total_cost_usd": round(fee_usd, 4),
            }
        except Exception as e:
            logger.debug("Coinbase quote failed: %s", e)
            return None

    def _get_kraken_quote(self, pair, side, amount_usd):
        """Get Kraken price + fee estimate."""
        try:
            try:
                from kraken_connector import KrakenConnector
            except Exception:
                from agents.kraken_connector import KrakenConnector  # type: ignore

            ticker = KrakenConnector.get_24h_volume(pair)
            if not isinstance(ticker, dict) or "error" in ticker:
                return None

            price = float(ticker.get("last_price", 0) or 0)
            volume_24h = float(ticker.get("volume_24h", 0) or 0)
            if price <= 0:
                return None

            # Dynamic maker fee from Kraken TradeVolume API (cached hourly).
            kraken_fees = _get_kraken_fees()
            fee_pct = float(kraken_fees.get("maker", 0.16))
            base_amount = amount_usd / price if price > 0 else 0.0

            # Basic size-aware slippage proxy from participation ratio.
            dollar_volume_24h = max(1.0, volume_24h * price)
            participation = max(0.0, float(amount_usd or 0.0) / dollar_volume_24h)
            slippage_pct = min(0.25, max(0.01, participation * 20.0))
            total_cost_pct = fee_pct + slippage_pct

            return {
                "venue": "kraken",
                "chain": "cex",
                "price": price,
                "amount_out": round(base_amount, 8) if side == "BUY" else round(amount_usd, 2),
                "fee_pct": round(fee_pct, 4),
                "gas_usd": 0,
                "slippage_pct": round(slippage_pct, 4),
                "total_cost_pct": round(total_cost_pct, 4),
                "total_cost_usd": round(amount_usd * total_cost_pct / 100.0, 4),
                "volume_24h": round(volume_24h, 4),
            }
        except Exception as e:
            logger.debug("Kraken quote failed: %s", e)
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

    def _get_kraken_stock_quote(self, symbol, side, amount_usd):
        """Get execution quote for stock on Kraken (commission-free).

        Args:
            symbol: Stock ticker (e.g., "AAPL", "SPY")
            side: "BUY" or "SELL"
            amount_usd: Dollar amount

        Returns:
            Venue quote dict or None if unavailable.
        """
        try:
            try:
                from kraken_stock_connector import KrakenStockConnector, is_market_open
            except ImportError:
                try:
                    from agents.kraken_stock_connector import KrakenStockConnector, is_market_open  # type: ignore[no-redef]
                except ImportError:
                    return None

            if not is_market_open():
                return None  # Can't trade stocks outside market hours

            quote = KrakenStockConnector.get_stock_quote(symbol)
            if quote.get("error"):
                return None

            price = float(quote.get("last_price", 0))
            if price <= 0:
                return None

            spread = float(quote.get("spread", 0))
            spread_pct = (spread / price * 100) if price > 0 else 999

            # Commission-free! Only spread cost
            total_cost_pct = spread_pct / 2  # Half-spread for maker

            shares = amount_usd / price if price > 0 else 0

            return {
                "venue": "kraken_stock",
                "chain": "cex",
                "price": price,
                "amount_out": round(shares, 6) if side == "BUY" else round(amount_usd, 2),
                "fee_pct": 0.0,  # Commission-free
                "gas_usd": 0,
                "slippage_pct": round(spread_pct / 2, 4),
                "total_cost_pct": round(total_cost_pct, 4),
                "total_cost_usd": round(amount_usd * total_cost_pct / 100, 4),
                "commission_free": True,
            }
        except Exception as e:
            logger.debug("Kraken stock quote failed for %s: %s", symbol, e)
            return None

    def _get_spot_price(self, symbol):
        """Get spot price from cache or Coinbase."""
        now = time.time()
        cached = self._price_cache.get(symbol)
        if isinstance(cached, tuple) and len(cached) == 2:
            cached_price, cached_ts = cached
            if (now - float(cached_ts)) < self._price_cache_ttl_s:
                return float(cached_price)

        try:
            url = f"https://api.exchange.coinbase.com/products/{symbol}-USD/ticker"
            data = _fetch_json(url)
            price = float(data.get("price", 0))
            self._price_cache[symbol] = (price, now)
            return price
        except Exception:
            if isinstance(cached, tuple) and len(cached) == 2:
                return float(cached[0])
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
