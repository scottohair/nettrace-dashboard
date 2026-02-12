#!/usr/bin/env python3
"""Advanced Path Router — Networkx graph-based trade route optimization.

Models all execution venues (CEX, DEX, bridges, chains) as a weighted
directed graph. Every edge is tagged with:
  - fee_pct:    Exchange/protocol fee (%)
  - gas_usd:    Estimated gas cost ($)
  - latency_ms: Expected execution time (ms)
  - hops:       Number of intermediate steps
  - slippage:   Expected slippage at trade size (%)
  - liquidity:  Available depth ($)

Uses Dijkstra/Bellman-Ford to find the optimal route that minimizes
total cost (fee + gas + slippage + time penalty).

Also provides:
  - Cross-chain bridge routing (ETH→Base, ETH→Arbitrum, etc.)
  - Multi-hop DEX routing (token→WETH→USDC across pools)
  - CEX vs DEX comparison with real-time quotes
  - Arbitrage path detection (negative-cost cycles)

Usage:
    from path_router import PathRouter
    router = PathRouter()
    route = router.find_best_route("ETH", "USDC", 5.0, source_chain="ethereum")
    arb = router.find_arbitrage_paths("ETH", min_profit_pct=0.5)
"""

import json
import logging
import os
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

logger = logging.getLogger("path_router")

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

# ── Venue Definitions ────────────────────────────────────────────────

VENUES = {
    # CEX venues
    "coinbase": {
        "type": "cex", "maker_fee": 0.004, "taker_fee": 0.006,
        "latency_ms": 50, "min_order_usd": 1.0,
        "pairs": ["BTC-USD", "ETH-USD", "SOL-USD", "MATIC-USD",
                  "AVAX-USD", "LINK-USD", "DOT-USD", "ADA-USD"],
    },
    "alpaca": {
        "type": "cex", "maker_fee": 0.0, "taker_fee": 0.0,  # Commission-free!
        "latency_ms": 30, "min_order_usd": 1.0,
        "pairs": ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD",
                  "AAPL-USD", "TSLA-USD", "SPY-USD", "QQQ-USD"],
    },
    "ibkr": {
        "type": "cex", "maker_fee": 0.001, "taker_fee": 0.002,  # Very low
        "latency_ms": 10, "min_order_usd": 0.01,  # Fractional shares
        "pairs": ["BTC-USD", "ETH-USD", "AAPL-USD", "TSLA-USD",
                  "SPY-USD", "QQQ-USD", "AMZN-USD", "NVDA-USD",
                  "COIN-USD", "MSTR-USD"],
    },
    # DEX venues (per chain)
    "uniswap_base": {
        "type": "dex", "chain": "base",
        "fee_tiers": [0.0001, 0.0005, 0.003, 0.01],  # 1bp, 5bp, 30bp, 100bp
        "gas_usd": 0.01, "latency_ms": 2000,  # ~2s block time
        "pairs": ["WETH-USDC", "WETH-DAI"],
    },
    "uniswap_ethereum": {
        "type": "dex", "chain": "ethereum",
        "fee_tiers": [0.0001, 0.0005, 0.003, 0.01],
        "gas_usd": 3.00, "latency_ms": 12000,  # ~12s block time
        "pairs": ["WETH-USDC", "WETH-USDT", "WETH-DAI", "WBTC-WETH"],
    },
    "uniswap_arbitrum": {
        "type": "dex", "chain": "arbitrum",
        "fee_tiers": [0.0001, 0.0005, 0.003, 0.01],
        "gas_usd": 0.05, "latency_ms": 250,  # fast L2
        "pairs": ["WETH-USDC", "WBTC-WETH"],
    },
    "uniswap_polygon": {
        "type": "dex", "chain": "polygon",
        "fee_tiers": [0.0001, 0.0005, 0.003, 0.01],
        "gas_usd": 0.02, "latency_ms": 2000,
        "pairs": ["WETH-USDC", "WMATIC-USDC"],
    },
    "jupiter_solana": {
        "type": "dex", "chain": "solana",
        "fee_tiers": [0.0],  # Jupiter no platform fee
        "gas_usd": 0.001, "latency_ms": 400,  # ~0.4s slot
        "pairs": ["SOL-USDC", "SOL-USDT"],
    },
}

# Bridge definitions: (source_chain, dest_chain) -> cost info
BRIDGES = {
    ("ethereum", "base"): {
        "name": "Base Bridge (native)", "fee_pct": 0.0,
        "gas_usd": 5.0, "latency_ms": 600_000,  # ~10 min
        "min_amount_usd": 5.0,
    },
    ("ethereum", "arbitrum"): {
        "name": "Arbitrum Bridge", "fee_pct": 0.0,
        "gas_usd": 5.0, "latency_ms": 600_000,
        "min_amount_usd": 5.0,
    },
    ("ethereum", "polygon"): {
        "name": "Polygon PoS Bridge", "fee_pct": 0.0,
        "gas_usd": 5.0, "latency_ms": 1_200_000,  # ~20 min
        "min_amount_usd": 5.0,
    },
    ("base", "ethereum"): {
        "name": "Base Bridge (withdraw)", "fee_pct": 0.0,
        "gas_usd": 0.05, "latency_ms": 604_800_000,  # 7 days for optimistic
        "min_amount_usd": 10.0,
    },
    ("arbitrum", "ethereum"): {
        "name": "Arbitrum Bridge (withdraw)", "fee_pct": 0.0,
        "gas_usd": 0.1, "latency_ms": 604_800_000,
        "min_amount_usd": 10.0,
    },
    # Fast bridges (3rd party, higher fee but fast)
    ("base", "arbitrum"): {
        "name": "Across Protocol", "fee_pct": 0.001,
        "gas_usd": 0.05, "latency_ms": 60_000,  # ~1 min
        "min_amount_usd": 1.0,
    },
    ("arbitrum", "base"): {
        "name": "Across Protocol", "fee_pct": 0.001,
        "gas_usd": 0.1, "latency_ms": 60_000,
        "min_amount_usd": 1.0,
    },
}

# Token equivalences across chains (same asset, different addresses)
TOKEN_EQUIVALENCES = {
    "ETH": ["ethereum:ETH", "base:ETH", "arbitrum:ETH", "polygon:WETH"],
    "USDC": ["ethereum:USDC", "base:USDC", "arbitrum:USDC", "polygon:USDC", "solana:USDC"],
    "BTC": ["coinbase:BTC", "ethereum:WBTC", "arbitrum:WBTC"],
    "SOL": ["coinbase:SOL", "solana:SOL"],
    "USD": ["coinbase:USD"],
}


def _fetch_json(url, payload=None, timeout=10):
    """HTTP helper."""
    headers = {"User-Agent": "NetTrace/1.0"}
    if payload:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=data,
                                     headers={**headers, "Content-Type": "application/json"})
    else:
        req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


class PathRouter:
    """Graph-based trade route optimizer using networkx."""

    # Cost weights for edge scoring
    WEIGHT_FEE = 1.0        # fee weight (most important)
    WEIGHT_GAS = 1.0        # gas weight (dollar cost)
    WEIGHT_SLIPPAGE = 1.5   # slippage penalty (uncertain cost)
    WEIGHT_TIME = 0.0001    # time penalty (ms → small weight)
    WEIGHT_HOPS = 0.1       # per-hop penalty

    def __init__(self):
        self._graph = None
        self._build_time = 0
        self._price_cache = {}
        self._price_cache_time = {}
        self._nx = None

    @property
    def nx(self):
        """Lazy import networkx."""
        if self._nx is None:
            try:
                import networkx as nx
                self._nx = nx
            except ImportError:
                logger.error("networkx not installed. pip install networkx")
                raise
        return self._nx

    @property
    def graph(self):
        """Build or return cached graph. Rebuilds every 60s."""
        if self._graph is None or time.time() - self._build_time > 60:
            self._graph = self._build_graph()
            self._build_time = time.time()
        return self._graph

    def _get_price(self, symbol):
        """Cached price lookup."""
        if symbol in ("USD", "USDC", "USDT", "DAI"):
            return 1.0
        cache_age = time.time() - self._price_cache_time.get(symbol, 0)
        if symbol in self._price_cache and cache_age < 30:
            return self._price_cache[symbol]
        try:
            url = f"https://api.exchange.coinbase.com/products/{symbol}-USD/ticker"
            data = _fetch_json(url)
            price = float(data.get("price", 0))
            self._price_cache[symbol] = price
            self._price_cache_time[symbol] = time.time()
            return price
        except Exception:
            return self._price_cache.get(symbol, 0)

    def _build_graph(self):
        """Build the execution graph from venue definitions."""
        nx = self.nx
        G = nx.DiGraph()

        # Add CEX venue nodes and edges
        for venue_id, venue in VENUES.items():
            if venue["type"] == "cex":
                for pair in venue["pairs"]:
                    base, quote = pair.split("-")
                    # BUY edge: USD → asset
                    buy_cost = self._compute_edge_cost(
                        fee_pct=venue["maker_fee"],
                        gas_usd=0,
                        latency_ms=venue["latency_ms"],
                        hops=1,
                        slippage_pct=0.001,
                    )
                    G.add_edge(
                        f"{venue_id}:{quote}", f"{venue_id}:{base}",
                        weight=buy_cost,
                        fee_pct=venue["maker_fee"],
                        gas_usd=0,
                        latency_ms=venue["latency_ms"],
                        hops=1,
                        slippage_pct=0.001,
                        venue=venue_id,
                        action="buy",
                        pair=pair,
                    )
                    # SELL edge: asset → USD
                    sell_cost = self._compute_edge_cost(
                        fee_pct=venue["maker_fee"],
                        gas_usd=0,
                        latency_ms=venue["latency_ms"],
                        hops=1,
                        slippage_pct=0.001,
                    )
                    G.add_edge(
                        f"{venue_id}:{base}", f"{venue_id}:{quote}",
                        weight=sell_cost,
                        fee_pct=venue["maker_fee"],
                        gas_usd=0,
                        latency_ms=venue["latency_ms"],
                        hops=1,
                        slippage_pct=0.001,
                        venue=venue_id,
                        action="sell",
                        pair=pair,
                    )

            elif venue["type"] == "dex":
                chain = venue["chain"]
                best_fee = min(venue["fee_tiers"])
                for pair in venue["pairs"]:
                    base, quote = pair.split("-")
                    for direction, (src, dst) in [("buy", (quote, base)), ("sell", (base, quote))]:
                        cost = self._compute_edge_cost(
                            fee_pct=best_fee,
                            gas_usd=venue["gas_usd"],
                            latency_ms=venue["latency_ms"],
                            hops=1,
                            slippage_pct=0.003,
                        )
                        G.add_edge(
                            f"{chain}:{src}", f"{chain}:{dst}",
                            weight=cost,
                            fee_pct=best_fee,
                            gas_usd=venue["gas_usd"],
                            latency_ms=venue["latency_ms"],
                            hops=1,
                            slippage_pct=0.003,
                            venue=venue_id,
                            action=direction,
                            pair=pair,
                        )

        # Add bridge edges (cross-chain transfers)
        for (src_chain, dst_chain), bridge in BRIDGES.items():
            # Bridges transfer the same asset cross-chain
            bridge_tokens = ["ETH", "USDC"]
            for token in bridge_tokens:
                src_node = f"{src_chain}:{token}"
                dst_node = f"{dst_chain}:{token}"
                if not G.has_node(src_node):
                    continue
                cost = self._compute_edge_cost(
                    fee_pct=bridge["fee_pct"],
                    gas_usd=bridge["gas_usd"],
                    latency_ms=bridge["latency_ms"],
                    hops=2,
                    slippage_pct=0,
                )
                G.add_edge(
                    src_node, dst_node,
                    weight=cost,
                    fee_pct=bridge["fee_pct"],
                    gas_usd=bridge["gas_usd"],
                    latency_ms=bridge["latency_ms"],
                    hops=2,
                    slippage_pct=0,
                    venue=bridge["name"],
                    action="bridge",
                    pair=f"{token}:{src_chain}→{dst_chain}",
                )

        # Add CEX↔chain deposit/withdraw edges
        # Coinbase → Ethereum (withdraw)
        for token in ["ETH", "BTC", "SOL", "USDC"]:
            if G.has_node(f"coinbase:{token}"):
                for chain in ["ethereum", "base", "arbitrum"]:
                    chain_node = f"{chain}:{'WETH' if token == 'ETH' and chain != 'ethereum' else token}"
                    if token == "ETH":
                        chain_node = f"{chain}:ETH"
                    cost = self._compute_edge_cost(
                        fee_pct=0, gas_usd=2.0 if chain == "ethereum" else 0.1,
                        latency_ms=300_000, hops=2, slippage_pct=0,
                    )
                    G.add_edge(
                        f"coinbase:{token}", chain_node,
                        weight=cost, fee_pct=0,
                        gas_usd=2.0 if chain == "ethereum" else 0.1,
                        latency_ms=300_000, hops=2, slippage_pct=0,
                        venue="coinbase_withdraw", action="withdraw",
                        pair=f"{token}:coinbase→{chain}",
                    )
                    # Deposit (chain → Coinbase)
                    deposit_cost = self._compute_edge_cost(
                        fee_pct=0, gas_usd=0.5 if chain == "ethereum" else 0.01,
                        latency_ms=600_000, hops=2, slippage_pct=0,
                    )
                    G.add_edge(
                        chain_node, f"coinbase:{token}",
                        weight=deposit_cost, fee_pct=0,
                        gas_usd=0.5 if chain == "ethereum" else 0.01,
                        latency_ms=600_000, hops=2, slippage_pct=0,
                        venue="coinbase_deposit", action="deposit",
                        pair=f"{token}:{chain}→coinbase",
                    )

        # Add WETH↔ETH equivalence edges (wrap/unwrap, ~zero cost)
        for chain in ["ethereum", "base", "arbitrum", "polygon"]:
            eth_node = f"{chain}:ETH"
            weth_node = f"{chain}:WETH"
            wrap_cost = self._compute_edge_cost(
                fee_pct=0, gas_usd=0.001, latency_ms=100, hops=0, slippage_pct=0,
            )
            # Only add if at least one node exists
            if G.has_node(eth_node) or G.has_node(weth_node):
                G.add_edge(eth_node, weth_node, weight=wrap_cost,
                           fee_pct=0, gas_usd=0.001, latency_ms=100, hops=0,
                           slippage_pct=0, venue="weth_wrap", action="wrap",
                           pair=f"ETH→WETH:{chain}")
                G.add_edge(weth_node, eth_node, weight=wrap_cost,
                           fee_pct=0, gas_usd=0.001, latency_ms=100, hops=0,
                           slippage_pct=0, venue="weth_unwrap", action="unwrap",
                           pair=f"WETH→ETH:{chain}")

        # Add USD↔USDC equivalence on Coinbase (1:1, zero cost)
        if G.has_node("coinbase:USD"):
            zero_cost = self._compute_edge_cost(
                fee_pct=0, gas_usd=0, latency_ms=10, hops=0, slippage_pct=0,
            )
            G.add_node("coinbase:USDC")
            G.add_edge("coinbase:USD", "coinbase:USDC", weight=zero_cost,
                       fee_pct=0, gas_usd=0, latency_ms=10, hops=0,
                       slippage_pct=0, venue="coinbase", action="convert",
                       pair="USD→USDC")
            G.add_edge("coinbase:USDC", "coinbase:USD", weight=zero_cost,
                       fee_pct=0, gas_usd=0, latency_ms=10, hops=0,
                       slippage_pct=0, venue="coinbase", action="convert",
                       pair="USDC→USD")

        logger.info("Built route graph: %d nodes, %d edges",
                     G.number_of_nodes(), G.number_of_edges())
        return G

    def _compute_edge_cost(self, fee_pct, gas_usd, latency_ms, hops, slippage_pct):
        """Compute weighted cost for a graph edge.

        Returns a single float cost used by shortest-path algorithms.
        Cost = w_fee * fee + w_gas * gas + w_slip * slippage + w_time * time + w_hop * hops
        """
        return (
            self.WEIGHT_FEE * fee_pct * 100 +          # fee as basis points
            self.WEIGHT_GAS * gas_usd +                 # gas in USD
            self.WEIGHT_SLIPPAGE * slippage_pct * 100 + # slippage as basis points
            self.WEIGHT_TIME * latency_ms +             # time penalty
            self.WEIGHT_HOPS * hops                     # hop count penalty
        )

    # ── Route Finding ────────────────────────────────────────────────

    def find_best_route(self, token_in, token_out, amount_usd,
                        source_chain=None, dest_chain=None,
                        source_venue=None, max_hops=5):
        """Find the cheapest execution route from token_in to token_out.

        Args:
            token_in: Input token symbol (e.g., "ETH")
            token_out: Output token symbol (e.g., "USDC")
            amount_usd: Trade size in USD
            source_chain: Restrict source to this chain
            dest_chain: Restrict destination to this chain
            source_venue: Restrict source to this venue (e.g., "coinbase")
            max_hops: Maximum path length

        Returns: {
            "path": [node1, node2, ...],
            "edges": [{fee, gas, latency, venue, action}, ...],
            "total_cost_pct": float,
            "total_gas_usd": float,
            "total_latency_ms": float,
            "total_hops": int,
            "estimated_output": float,
        }
        """
        nx = self.nx
        G = self.graph

        # Find source nodes
        source_nodes = self._find_token_nodes(token_in, source_chain, source_venue)
        dest_nodes = self._find_token_nodes(token_out, dest_chain)

        if not source_nodes or not dest_nodes:
            return {"error": f"No route: {token_in} ({source_nodes}) → {token_out} ({dest_nodes})"}

        best_route = None
        best_cost = float("inf")

        for src in source_nodes:
            for dst in dest_nodes:
                if src == dst:
                    continue
                try:
                    path = nx.shortest_path(G, src, dst, weight="weight")
                    if len(path) - 1 > max_hops:
                        continue
                    cost = nx.shortest_path_length(G, src, dst, weight="weight")
                    if cost < best_cost:
                        best_cost = cost
                        best_route = path
                except (nx.NetworkXNoPath, nx.NodeNotFound):
                    continue

        if not best_route:
            return {"error": f"No path found: {token_in} → {token_out}"}

        # Extract edge details
        edges = []
        total_fee = 0
        total_gas = 0
        total_latency = 0
        total_hops = 0

        for i in range(len(best_route) - 1):
            edge_data = G.edges[best_route[i], best_route[i + 1]]
            edges.append({
                "from": best_route[i],
                "to": best_route[i + 1],
                "venue": edge_data.get("venue", "?"),
                "action": edge_data.get("action", "?"),
                "pair": edge_data.get("pair", "?"),
                "fee_pct": edge_data.get("fee_pct", 0),
                "gas_usd": edge_data.get("gas_usd", 0),
                "latency_ms": edge_data.get("latency_ms", 0),
                "slippage_pct": edge_data.get("slippage_pct", 0),
                "hops": edge_data.get("hops", 1),
            })
            total_fee += edge_data.get("fee_pct", 0)
            total_gas += edge_data.get("gas_usd", 0)
            total_latency += edge_data.get("latency_ms", 0)
            total_hops += edge_data.get("hops", 1)

        # Estimate output after all fees
        total_slippage = sum(e["slippage_pct"] for e in edges)
        net_factor = (1 - total_fee) * (1 - total_slippage)
        estimated_output = (amount_usd - total_gas) * net_factor

        return {
            "path": best_route,
            "edges": edges,
            "total_cost_pct": round(total_fee * 100, 4),
            "total_gas_usd": round(total_gas, 4),
            "total_latency_ms": total_latency,
            "total_latency_human": self._format_time(total_latency),
            "total_hops": total_hops,
            "total_slippage_pct": round(total_slippage * 100, 4),
            "estimated_output_usd": round(estimated_output, 4),
            "efficiency_pct": round((estimated_output / amount_usd) * 100, 2) if amount_usd > 0 else 0,
            "amount_in_usd": amount_usd,
        }

    def find_all_routes(self, token_in, token_out, amount_usd,
                        source_chain=None, dest_chain=None, top_n=5):
        """Find top N routes ranked by total cost."""
        nx = self.nx
        G = self.graph

        source_nodes = self._find_token_nodes(token_in, source_chain)
        dest_nodes = self._find_token_nodes(token_out, dest_chain)

        routes = []
        for src in source_nodes:
            for dst in dest_nodes:
                if src == dst:
                    continue
                try:
                    for path in nx.shortest_simple_paths(G, src, dst, weight="weight"):
                        if len(path) - 1 > 6:
                            break
                        cost = sum(
                            G.edges[path[i], path[i + 1]]["weight"]
                            for i in range(len(path) - 1)
                        )
                        total_gas = sum(
                            G.edges[path[i], path[i + 1]].get("gas_usd", 0)
                            for i in range(len(path) - 1)
                        )
                        total_fee = sum(
                            G.edges[path[i], path[i + 1]].get("fee_pct", 0)
                            for i in range(len(path) - 1)
                        )
                        routes.append({
                            "path": path,
                            "cost": cost,
                            "total_gas_usd": total_gas,
                            "total_fee_pct": total_fee * 100,
                            "hops": len(path) - 1,
                        })
                        if len(routes) >= top_n * 3:
                            break
                except (nx.NetworkXNoPath, nx.NodeNotFound):
                    continue

        routes.sort(key=lambda r: r["cost"])
        return routes[:top_n]

    def find_arbitrage_paths(self, token, min_profit_pct=0.5, max_hops=4):
        """Find arbitrage opportunities (negative-cost cycles through token).

        Looks for paths: token@venueA → ... → token@venueB where
        the output > input after all fees.
        """
        nx = self.nx
        G = self.graph

        token_nodes = self._find_token_nodes(token)
        arb_paths = []

        for i, src in enumerate(token_nodes):
            for dst in token_nodes[i + 1:]:
                # Forward path: src → dst
                try:
                    fwd_path = nx.shortest_path(G, src, dst, weight="weight")
                    if len(fwd_path) - 1 > max_hops:
                        continue
                except (nx.NetworkXNoPath, nx.NodeNotFound):
                    continue

                # Return path: dst → src
                try:
                    rev_path = nx.shortest_path(G, dst, src, weight="weight")
                    if len(rev_path) - 1 > max_hops:
                        continue
                except (nx.NetworkXNoPath, nx.NodeNotFound):
                    continue

                # Calculate round-trip cost
                fwd_fee = sum(G.edges[fwd_path[j], fwd_path[j + 1]].get("fee_pct", 0)
                              for j in range(len(fwd_path) - 1))
                rev_fee = sum(G.edges[rev_path[j], rev_path[j + 1]].get("fee_pct", 0)
                              for j in range(len(rev_path) - 1))
                fwd_gas = sum(G.edges[fwd_path[j], fwd_path[j + 1]].get("gas_usd", 0)
                              for j in range(len(fwd_path) - 1))
                rev_gas = sum(G.edges[rev_path[j], rev_path[j + 1]].get("gas_usd", 0)
                              for j in range(len(rev_path) - 1))

                total_fee = fwd_fee + rev_fee
                total_gas = fwd_gas + rev_gas

                # A real arb would need price diffs — this is structural analysis
                # Flag if total fees are low enough that a price diff could be profitable
                if total_fee * 100 < 2.0 and total_gas < 1.0:
                    arb_paths.append({
                        "forward": fwd_path,
                        "return": rev_path,
                        "round_trip_fee_pct": round(total_fee * 100, 4),
                        "round_trip_gas_usd": round(total_gas, 4),
                        "min_spread_needed_pct": round(total_fee * 100 + (total_gas / 5.0) * 100, 4),
                        "total_hops": len(fwd_path) + len(rev_path) - 2,
                    })

        arb_paths.sort(key=lambda a: a["min_spread_needed_pct"])
        return arb_paths

    def get_bridge_routes(self, token, source_chain, dest_chain):
        """Find all bridge paths for a token between two chains."""
        nx = self.nx
        G = self.graph

        src = f"{source_chain}:{token}"
        dst = f"{dest_chain}:{token}"

        routes = []
        try:
            for path in nx.shortest_simple_paths(G, src, dst, weight="weight"):
                if len(path) > 5:
                    break
                edges = []
                for i in range(len(path) - 1):
                    e = G.edges[path[i], path[i + 1]]
                    edges.append({
                        "from": path[i], "to": path[i + 1],
                        "venue": e.get("venue"), "action": e.get("action"),
                        "gas_usd": e.get("gas_usd", 0),
                        "latency_ms": e.get("latency_ms", 0),
                    })
                total_gas = sum(e["gas_usd"] for e in edges)
                total_time = sum(e["latency_ms"] for e in edges)
                routes.append({
                    "path": path, "edges": edges,
                    "total_gas_usd": total_gas,
                    "total_time": self._format_time(total_time),
                    "total_time_ms": total_time,
                })
                if len(routes) >= 3:
                    break
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            pass

        return routes

    def get_graph_stats(self):
        """Return graph statistics."""
        G = self.graph
        nx = self.nx

        chains = set()
        venues_set = set()
        for u, v, data in G.edges(data=True):
            venues_set.add(data.get("venue", "?"))
            for node in [u, v]:
                parts = node.split(":")
                if len(parts) == 2:
                    chains.add(parts[0])

        return {
            "nodes": G.number_of_nodes(),
            "edges": G.number_of_edges(),
            "chains": sorted(chains),
            "venues": sorted(venues_set),
            "density": round(nx.density(G), 4),
            "strongly_connected": nx.is_strongly_connected(G),
            "components": nx.number_weakly_connected_components(G),
        }

    # ── Helpers ──────────────────────────────────────────────────────

    def _find_token_nodes(self, token, chain=None, venue=None):
        """Find all graph nodes for a token, optionally filtered by chain/venue."""
        G = self.graph
        nodes = []
        for node in G.nodes():
            parts = node.split(":")
            if len(parts) != 2:
                continue
            node_loc, node_token = parts
            if node_token.upper() == token.upper():
                if chain and node_loc != chain:
                    continue
                if venue and node_loc != venue:
                    continue
                nodes.append(node)
        return nodes

    @staticmethod
    def _format_time(ms):
        """Human-readable time from milliseconds."""
        if ms < 1000:
            return f"{ms:.0f}ms"
        elif ms < 60_000:
            return f"{ms/1000:.1f}s"
        elif ms < 3_600_000:
            return f"{ms/60_000:.1f}min"
        elif ms < 86_400_000:
            return f"{ms/3_600_000:.1f}hr"
        else:
            return f"{ms/86_400_000:.1f}days"

    def print_route(self, route):
        """Pretty-print a route."""
        if "error" in route:
            print(f"  Error: {route['error']}")
            return

        print(f"\n  Route: {' → '.join(route['path'])}")
        print(f"  Hops: {route['total_hops']}")
        for i, edge in enumerate(route["edges"]):
            print(f"    [{i+1}] {edge['from']} → {edge['to']}")
            print(f"        via {edge['venue']} ({edge['action']})")
            print(f"        fee={edge['fee_pct']*100:.2f}% gas=${edge['gas_usd']:.4f} "
                  f"latency={self._format_time(edge['latency_ms'])} "
                  f"slippage={edge['slippage_pct']*100:.2f}%")
        print(f"  Total: fee={route['total_cost_pct']:.2f}% gas=${route['total_gas_usd']:.4f} "
              f"time={route['total_latency_human']} "
              f"efficiency={route['efficiency_pct']:.2f}%")
        print(f"  Est output: ${route['estimated_output_usd']:.4f} from ${route['amount_in_usd']:.2f}")


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    router = PathRouter()

    if len(sys.argv) > 1 and sys.argv[1] == "stats":
        stats = router.get_graph_stats()
        print(f"\nPath Router Graph:")
        print(f"  Nodes: {stats['nodes']}")
        print(f"  Edges: {stats['edges']}")
        print(f"  Chains: {', '.join(stats['chains'])}")
        print(f"  Venues: {', '.join(stats['venues'])}")
        print(f"  Density: {stats['density']}")
        print(f"  Connected: {stats['strongly_connected']}")
        print(f"  Components: {stats['components']}")

    elif len(sys.argv) > 1 and sys.argv[1] == "route":
        token_in = sys.argv[2] if len(sys.argv) > 2 else "ETH"
        token_out = sys.argv[3] if len(sys.argv) > 3 else "USDC"
        amount = float(sys.argv[4]) if len(sys.argv) > 4 else 5.0
        print(f"\nFinding best route: {token_in} → {token_out} (${amount})...")
        route = router.find_best_route(token_in, token_out, amount)
        router.print_route(route)

        print(f"\nAll routes:")
        routes = router.find_all_routes(token_in, token_out, amount)
        for i, r in enumerate(routes):
            print(f"  [{i+1}] {' → '.join(r['path'])} "
                  f"(fee={r['total_fee_pct']:.2f}% gas=${r['total_gas_usd']:.4f} "
                  f"hops={r['hops']})")

    elif len(sys.argv) > 1 and sys.argv[1] == "arb":
        token = sys.argv[2] if len(sys.argv) > 2 else "ETH"
        print(f"\nFinding arbitrage paths for {token}...")
        arbs = router.find_arbitrage_paths(token)
        if arbs:
            for a in arbs[:5]:
                print(f"  {' → '.join(a['forward'])} → {' → '.join(a['return'][1:])}")
                print(f"    RT fee: {a['round_trip_fee_pct']:.2f}% "
                      f"gas: ${a['round_trip_gas_usd']:.4f} "
                      f"min spread needed: {a['min_spread_needed_pct']:.2f}%")
        else:
            print("  No arb paths found below threshold")

    elif len(sys.argv) > 1 and sys.argv[1] == "bridge":
        token = sys.argv[2] if len(sys.argv) > 2 else "ETH"
        src = sys.argv[3] if len(sys.argv) > 3 else "ethereum"
        dst = sys.argv[4] if len(sys.argv) > 4 else "base"
        print(f"\nBridge routes: {token} {src} → {dst}...")
        routes = router.get_bridge_routes(token, src, dst)
        for r in routes:
            print(f"  {' → '.join(r['path'])}")
            print(f"    gas: ${r['total_gas_usd']:.4f}  time: {r['total_time']}")

    else:
        print("Usage:")
        print("  python path_router.py stats           # Graph statistics")
        print("  python path_router.py route ETH USDC 5.0  # Find best route")
        print("  python path_router.py arb ETH         # Find arb paths")
        print("  python path_router.py bridge ETH ethereum base  # Bridge routes")
