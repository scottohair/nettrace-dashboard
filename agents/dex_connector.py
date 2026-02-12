#!/usr/bin/env python3
"""Unified DEX trading interface — Uniswap V3 + Jupiter V6.

Execute swaps on decentralized exchanges with best-price routing.

Supported DEXs:
  - Uniswap V3 (Ethereum, Polygon, Arbitrum, Base)
  - Jupiter V6 (Solana)
  - 1inch Fusion (aggregator fallback)

Usage:
    from dex_connector import DEXConnector
    dex = DEXConnector(wallet_address="0x...", private_key="0x...")
    quote = dex.get_best_quote("ETH", "USDC", 0.5)
    tx = dex.swap_uniswap("ETH", "USDC", 0.5, slippage=0.005)
"""

import json
import logging
import os
import time
import urllib.request
from pathlib import Path

logger = logging.getLogger("dex_connector")

# Uniswap V3 Router addresses per chain
UNISWAP_ROUTER = {
    "ethereum": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",  # SwapRouter02
    "polygon":  "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
    "arbitrum": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
    "base":     "0x2626664c2603336E57B271c5C0b26F421741e481",  # Universal Router
}

# Uniswap V3 Quoter V2 addresses
UNISWAP_QUOTER = {
    "ethereum": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
    "polygon":  "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
    "arbitrum": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
    "base":     "0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
}

# Well-known token addresses per chain (for routing)
TOKEN_ADDRESSES = {
    "ethereum": {
        "ETH":  "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "DAI":  "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    },
    "base": {
        "ETH":  "0x4200000000000000000000000000000000000006",  # WETH on Base
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    },
    "polygon": {
        "MATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",  # WMATIC
        "ETH":   "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "WETH":  "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "USDC":  "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
    },
    "arbitrum": {
        "ETH":  "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
    },
}

# Solana token mints
SOLANA_MINTS = {
    "SOL":  "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "JUP":  "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
}

# Jupiter V6 API base
JUPITER_API = "https://quote-api.jup.ag/v6"

# 1inch API base
ONEINCH_API = "https://api.1inch.dev/swap/v6.0"

# Fee tiers for Uniswap V3 (in basis points)
UNISWAP_FEE_TIERS = [100, 500, 3000, 10000]  # 0.01%, 0.05%, 0.3%, 1%

# Flashbots RPC for MEV-protected transactions on Ethereum mainnet
FLASHBOTS_RPC = "https://rpc.flashbots.net"

# WETH addresses per chain (for auto-unwrap after swap)
WETH_ADDRESSES = {
    "ethereum": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "base":     "0x4200000000000000000000000000000000000006",
    "arbitrum": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    "polygon":  "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
}


def _fetch_json(url, payload=None, headers=None, timeout=10):
    """HTTP JSON request helper."""
    _headers = {"User-Agent": "NetTrace/1.0"}
    if headers:
        _headers.update(headers)
    if payload:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=data, headers={**_headers, "Content-Type": "application/json"})
    else:
        req = urllib.request.Request(url, headers=_headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


class DEXConnector:
    """Unified DEX trading interface."""

    def __init__(self, wallet_address=None, private_key=None, chain="base"):
        """
        Args:
            wallet_address: EVM address (0x...) or Solana address
            private_key: For signing transactions
            chain: Default chain for Uniswap (base recommended — lowest gas)
        """
        self.wallet_address = wallet_address
        self.private_key = private_key
        self.chain = chain
        self._w3 = None

    @property
    def w3(self):
        """Lazy web3 connection."""
        if self._w3 is None:
            try:
                from web3 import Web3
                from wallet_connector import CHAIN_CONFIG
                rpc = CHAIN_CONFIG.get(self.chain, {}).get("rpc", "https://mainnet.base.org")
                self._w3 = Web3(Web3.HTTPProvider(rpc))
            except ImportError:
                logger.warning("web3 not installed")
        return self._w3

    # ── Uniswap V3 ──────────────────────────────────────────────────

    def get_quote_uniswap(self, token_in, token_out, amount_in, chain=None):
        """Get a Uniswap V3 swap quote.

        Args:
            token_in: Token symbol (e.g. "ETH") or address
            token_out: Token symbol (e.g. "USDC") or address
            amount_in: Amount of token_in to swap (human readable, e.g. 0.5 ETH)
            chain: Override chain (default: self.chain)

        Returns: {
            'amount_out': 1500.0,
            'price': 3000.0,
            'fee_tier': 500,
            'fee_pct': 0.05,
            'gas_estimate': 150000,
            'venue': 'uniswap_base',
        }
        """
        chain = chain or self.chain
        tokens = TOKEN_ADDRESSES.get(chain, {})

        # Resolve symbols to addresses
        addr_in = tokens.get(token_in, token_in)
        addr_out = tokens.get(token_out, token_out)

        if not addr_in.startswith("0x") or not addr_out.startswith("0x"):
            return {"error": f"Unknown token on {chain}: {token_in} or {token_out}"}

        # Get decimals
        decimals_in = self._get_decimals(token_in)
        decimals_out = self._get_decimals(token_out)

        amount_raw = int(amount_in * (10 ** decimals_in))

        # Try each fee tier to find best quote
        best_quote = None
        for fee in UNISWAP_FEE_TIERS:
            try:
                quote = self._quoter_call(addr_in, addr_out, amount_raw, fee, chain)
                if quote and (best_quote is None or quote["amount_out_raw"] > best_quote["amount_out_raw"]):
                    best_quote = quote
                    best_quote["fee_tier"] = fee
            except Exception as e:
                logger.debug("Uniswap quote failed for fee %d: %s", fee, e)

        if not best_quote:
            return {"error": "No Uniswap liquidity found"}

        amount_out = best_quote["amount_out_raw"] / (10 ** decimals_out)
        price = amount_out / amount_in if amount_in > 0 else 0

        return {
            "amount_in": amount_in,
            "amount_out": round(amount_out, 6),
            "price": round(price, 4),
            "fee_tier": best_quote["fee_tier"],
            "fee_pct": best_quote["fee_tier"] / 10000,  # basis points to pct
            "gas_estimate": best_quote.get("gas_estimate", 150000),
            "venue": f"uniswap_{chain}",
            "chain": chain,
        }

    def _quoter_call(self, token_in, token_out, amount_raw, fee, chain):
        """Call Uniswap V3 QuoterV2 for a quote."""
        quoter = UNISWAP_QUOTER.get(chain)
        if not quoter:
            return None

        # quoteExactInputSingle function selector
        # quoteExactInputSingle((address,address,uint256,uint24,uint160))
        selector = "0xc6a5026a"  # quoteExactInputSingle

        # Encode params (simplified — ABI encode the struct)
        params = (
            token_in.lower().replace("0x", "").zfill(64) +
            token_out.lower().replace("0x", "").zfill(64) +
            hex(amount_raw)[2:].zfill(64) +
            hex(fee)[2:].zfill(64) +
            "0" * 64  # sqrtPriceLimitX96 = 0
        )
        call_data = selector + params

        from wallet_connector import CHAIN_CONFIG
        rpc = CHAIN_CONFIG.get(chain, {}).get("rpc")
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "eth_call",
            "params": [{"to": quoter, "data": call_data}, "latest"]
        }
        result = _fetch_json(rpc, payload)
        hex_result = result.get("result", "0x")

        if hex_result == "0x" or len(hex_result) < 66:
            return None

        # First 32 bytes = amountOut
        amount_out_raw = int(hex_result[2:66], 16)
        return {"amount_out_raw": amount_out_raw, "gas_estimate": 150000}

    def swap_uniswap(self, token_in, token_out, amount_in, slippage=0.005,
                     chain=None, use_flashbots=False):
        """Execute a Uniswap V3 swap.

        Args:
            slippage: Max acceptable slippage (0.005 = 0.5%)
            use_flashbots: If True and chain is ethereum, send tx through
                           Flashbots RPC for MEV protection.

        Returns: Dict with tx_hash, receipt, and swap details — or error dict.
        """
        if not self.wallet_address or not self.private_key:
            return {"error": "Wallet address and private key required for swaps"}

        chain = chain or self.chain
        quote = self.get_quote_uniswap(token_in, token_out, amount_in, chain)
        if "error" in quote:
            return quote

        min_out = quote["amount_out"] * (1 - slippage)
        tokens = TOKEN_ADDRESSES.get(chain, {})
        addr_in = tokens.get(token_in, token_in)
        addr_out = tokens.get(token_out, token_out)
        router = UNISWAP_ROUTER.get(chain)

        if not router:
            return {"error": f"No Uniswap router on {chain}"}

        w3 = self.w3
        if not w3:
            return {"error": "web3 library required"}

        decimals_in = self._get_decimals(token_in)
        decimals_out = self._get_decimals(token_out)
        amount_raw = int(amount_in * (10 ** decimals_in))
        min_out_raw = int(min_out * (10 ** decimals_out))

        # Create WalletConnector for approval, simulation, signing, and receipt
        from wallet_connector import WalletConnector as WC
        wc = WC(self.wallet_address, chain, self.private_key)

        # ── Approval check: ensure router is approved for ERC-20 input ──
        is_native_in = token_in in ("ETH", "MATIC")
        if not is_native_in:
            try:
                approval = wc.ensure_approved(addr_in, router, amount_raw)
                if not approval.get("was_already_approved"):
                    logger.info("Approved %s for router %s (tx: %s)",
                                token_in, router, approval.get("tx_hash"))
            except Exception as e:
                return {"error": f"Token approval failed: {e}"}

        # Build exactInputSingle call
        # exactInputSingle((address,address,uint24,address,uint256,uint256,uint160))
        selector = "0x414bf389"
        params = (
            addr_in.lower().replace("0x", "").zfill(64) +
            addr_out.lower().replace("0x", "").zfill(64) +
            hex(quote["fee_tier"])[2:].zfill(64) +
            self.wallet_address.lower().replace("0x", "").zfill(64) +
            hex(amount_raw)[2:].zfill(64) +
            hex(min_out_raw)[2:].zfill(64) +
            "0" * 64  # sqrtPriceLimitX96 = 0
        )

        value = amount_raw if is_native_in else 0

        tx = {
            "to": router,
            "data": "0x" + selector[2:] + params,
            "value": value,
            "gas": quote["gas_estimate"] + 50000,  # buffer
            "chainId": self._get_chain_id(chain),
        }

        # ── Transaction simulation via eth_call ──────────────────────
        try:
            sim_result = wc.simulate_tx(tx)
            logger.debug("Swap simulation OK: %s", sim_result[:20] if sim_result else "empty")
        except Exception as e:
            logger.error("Swap simulation reverted: %s", e)
            return {"error": f"Swap simulation reverted — tx would fail: {e}"}

        # ── Flashbots RPC for MEV protection on Ethereum ─────────────
        original_w3 = None
        if use_flashbots and chain == "ethereum":
            from web3 import Web3
            flashbots_w3 = Web3(Web3.HTTPProvider(FLASHBOTS_RPC))
            # Override the web3 instance to use Flashbots
            original_w3 = wc._w3
            wc._w3 = flashbots_w3
            logger.info("Using Flashbots RPC for MEV protection on Ethereum mainnet")

        # ── Sign and send ────────────────────────────────────────────
        try:
            tx_hash = wc.sign_and_send(tx)
            logger.info("Uniswap swap tx: %s on %s", tx_hash, chain)
        except Exception as e:
            return {"error": f"Swap failed: {e}"}
        finally:
            # Restore original web3 if we swapped to Flashbots
            if original_w3 is not None:
                wc._w3 = original_w3

        # ── Wait for receipt ─────────────────────────────────────────
        try:
            receipt = wc.wait_for_tx(tx_hash, confirmations=2, timeout=120)
            if receipt and receipt.get("status") != 1:
                return {
                    "error": "Swap tx mined but reverted on-chain",
                    "tx_hash": tx_hash,
                    "receipt": receipt,
                }
        except Exception as e:
            logger.warning("Receipt monitoring failed for %s: %s", tx_hash, e)
            receipt = None

        result = {
            "tx_hash": tx_hash,
            "receipt": receipt,
            "venue": f"uniswap_{chain}",
            "amount_in": amount_in,
            "expected_out": quote["amount_out"],
            "min_out": round(min_out, 6),
        }

        # ── Auto-unwrap WETH on Base ─────────────────────────────────
        weth_addr = WETH_ADDRESSES.get(chain, "").lower()
        output_is_weth = addr_out.lower() == weth_addr and weth_addr != ""
        if chain == "base" and output_is_weth and token_out in ("WETH", "ETH"):
            try:
                # Unwrap the received WETH back to native ETH
                unwrap_amount = min_out_raw  # conservative: unwrap at least min_out
                unwrap_hash = wc.unwrap_weth(unwrap_amount)
                unwrap_receipt = wc.wait_for_tx(unwrap_hash, confirmations=1, timeout=60)
                result["weth_unwrap"] = {
                    "tx_hash": unwrap_hash,
                    "receipt": unwrap_receipt,
                    "amount_wei": unwrap_amount,
                }
                logger.info("Auto-unwrapped WETH on Base: %s", unwrap_hash)
            except Exception as e:
                logger.warning("WETH unwrap failed (swap succeeded): %s", e)
                result["weth_unwrap"] = {"error": str(e)}

        return result

    # ── Jupiter (Solana) ─────────────────────────────────────────────

    def get_quote_jupiter(self, input_mint, output_mint, amount, slippage_bps=50):
        """Get Jupiter V6 swap quote on Solana.

        Args:
            input_mint: Token symbol (e.g. "SOL") or mint address
            output_mint: Token symbol (e.g. "USDC") or mint address
            amount: Amount of input token (human readable)
            slippage_bps: Slippage in basis points (50 = 0.5%)

        Returns: {
            'amount_out': 150.0,
            'price': 150.0,
            'price_impact_pct': 0.01,
            'fee_pct': 0.0,
            'venue': 'jupiter_solana',
            'route_plan': [...],
        }
        """
        # Resolve symbols to mints
        in_mint = SOLANA_MINTS.get(input_mint, input_mint)
        out_mint = SOLANA_MINTS.get(output_mint, output_mint)

        # Jupiter expects amounts in lamports/smallest unit
        decimals_in = self._get_solana_decimals(input_mint)
        amount_raw = int(amount * (10 ** decimals_in))

        url = (f"{JUPITER_API}/quote"
               f"?inputMint={in_mint}"
               f"&outputMint={out_mint}"
               f"&amount={amount_raw}"
               f"&slippageBps={slippage_bps}")

        try:
            data = _fetch_json(url)
        except Exception as e:
            return {"error": f"Jupiter quote failed: {e}"}

        if "error" in data:
            return {"error": data["error"]}

        decimals_out = self._get_solana_decimals(output_mint)
        out_amount = int(data.get("outAmount", 0)) / (10 ** decimals_out)
        in_amount_actual = int(data.get("inAmount", amount_raw)) / (10 ** decimals_in)
        price = out_amount / in_amount_actual if in_amount_actual > 0 else 0
        price_impact = float(data.get("priceImpactPct", 0))

        return {
            "amount_in": round(in_amount_actual, 6),
            "amount_out": round(out_amount, 6),
            "price": round(price, 4),
            "price_impact_pct": round(price_impact, 4),
            "fee_pct": 0.0,  # Jupiter has no platform fee
            "venue": "jupiter_solana",
            "chain": "solana",
            "route_plan": data.get("routePlan", []),
            "_raw_quote": data,  # Keep for swap execution
        }

    def swap_jupiter(self, input_mint, output_mint, amount, slippage_bps=50):
        """Execute a Jupiter swap on Solana.

        Returns: Transaction hash or error dict.
        """
        if not self.wallet_address:
            return {"error": "Wallet address required"}

        quote = self.get_quote_jupiter(input_mint, output_mint, amount, slippage_bps)
        if "error" in quote:
            return quote

        raw_quote = quote.get("_raw_quote")
        if not raw_quote:
            return {"error": "No raw quote data for swap"}

        # Get swap transaction from Jupiter
        swap_url = f"{JUPITER_API}/swap"
        payload = {
            "quoteResponse": raw_quote,
            "userPublicKey": self.wallet_address,
            "wrapAndUnwrapSol": True,
        }

        try:
            swap_data = _fetch_json(swap_url, payload)
        except Exception as e:
            return {"error": f"Jupiter swap request failed: {e}"}

        swap_tx = swap_data.get("swapTransaction")
        if not swap_tx:
            return {"error": "No transaction returned from Jupiter"}

        # The swapTransaction is a base64-encoded versioned transaction
        # In production, sign with Solana private key and submit
        if not self.private_key:
            return {
                "unsigned_tx": swap_tx,
                "venue": "jupiter_solana",
                "quote": quote,
                "note": "Transaction needs signing — provide private key or sign externally",
            }

        # Sign and submit (requires solders or solana-py)
        try:
            return self._sign_solana_tx(swap_tx)
        except Exception as e:
            return {"error": f"Solana signing failed: {e}"}

    def _sign_solana_tx(self, base64_tx):
        """Sign and submit a Solana transaction."""
        import base64 as b64
        from wallet_connector import CHAIN_CONFIG

        # Decode and sign the transaction
        # This requires solders library for proper signing
        try:
            from solders.keypair import Keypair
            from solders.transaction import VersionedTransaction
            keypair = Keypair.from_base58_string(self.private_key)
            tx_bytes = b64.b64decode(base64_tx)
            tx = VersionedTransaction.from_bytes(tx_bytes)
            tx.sign([keypair])
            signed_bytes = bytes(tx)
        except ImportError:
            return {"error": "solders library required for Solana signing (pip install solders)"}

        # Submit to Solana RPC
        rpc = CHAIN_CONFIG["solana"]["rpc"]
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "sendTransaction",
            "params": [
                b64.b64encode(signed_bytes).decode(),
                {"encoding": "base64", "skipPreflight": False}
            ]
        }
        result = _fetch_json(rpc, payload)
        if "error" in result:
            return {"error": f"Solana RPC error: {result['error']}"}

        tx_hash = result.get("result", "")
        logger.info("Jupiter swap tx: %s", tx_hash)
        return {"tx_hash": tx_hash, "venue": "jupiter_solana"}

    # ── 1inch Aggregator (fallback) ──────────────────────────────────

    def get_quote_1inch(self, token_in, token_out, amount_in, chain=None):
        """Get 1inch aggregator quote (best across all DEX liquidity).

        Note: 1inch API requires an API key for production usage.
        """
        chain = chain or self.chain
        chain_ids = {"ethereum": 1, "polygon": 137, "arbitrum": 42161, "base": 8453}
        chain_id = chain_ids.get(chain, 8453)

        tokens = TOKEN_ADDRESSES.get(chain, {})
        addr_in = tokens.get(token_in, token_in)
        addr_out = tokens.get(token_out, token_out)

        decimals_in = self._get_decimals(token_in)
        amount_raw = str(int(amount_in * (10 ** decimals_in)))

        api_key = os.environ.get("ONEINCH_API_KEY", "")
        if not api_key:
            return {"error": "1inch API key required (set ONEINCH_API_KEY env var)"}

        url = f"{ONEINCH_API}/{chain_id}/quote?src={addr_in}&dst={addr_out}&amount={amount_raw}"
        headers = {"Authorization": f"Bearer {api_key}"}

        try:
            data = _fetch_json(url, headers=headers)
        except Exception as e:
            return {"error": f"1inch quote failed: {e}"}

        decimals_out = self._get_decimals(token_out)
        amount_out = int(data.get("dstAmount", 0)) / (10 ** decimals_out)
        price = amount_out / amount_in if amount_in > 0 else 0

        return {
            "amount_in": amount_in,
            "amount_out": round(amount_out, 6),
            "price": round(price, 4),
            "fee_pct": 0.0,  # 1inch doesn't charge protocol fee
            "gas_estimate": int(data.get("gas", 200000)),
            "venue": f"1inch_{chain}",
            "chain": chain,
        }

    # ── Best Quote (compare all) ─────────────────────────────────────

    def get_best_quote(self, token_in, token_out, amount_in, chains=None):
        """Get the best quote across all DEXs.

        Args:
            chains: List of chains to check (default: [self.chain, "solana"])

        Returns: Best quote dict with 'venue' indicating the winner.
        """
        if chains is None:
            chains = [self.chain]

        quotes = []

        for chain in chains:
            if chain == "solana":
                q = self.get_quote_jupiter(token_in, token_out, amount_in)
            else:
                q = self.get_quote_uniswap(token_in, token_out, amount_in, chain)
            if "error" not in q:
                quotes.append(q)

        if not quotes:
            return {"error": "No quotes available"}

        # Sort by amount_out (highest = best for buyer)
        quotes.sort(key=lambda q: q.get("amount_out", 0), reverse=True)
        best = quotes[0]
        best["all_quotes"] = quotes
        return best

    # ── Helpers ──────────────────────────────────────────────────────

    def _get_decimals(self, symbol):
        """Get token decimals for EVM tokens."""
        known = {"USDC": 6, "USDT": 6, "DAI": 18, "WETH": 18, "ETH": 18,
                 "WBTC": 8, "MATIC": 18, "BTC": 8}
        return known.get(symbol, 18)

    def _get_solana_decimals(self, symbol):
        """Get token decimals for Solana tokens."""
        known = {"SOL": 9, "USDC": 6, "USDT": 6, "BONK": 5, "JUP": 6}
        return known.get(symbol, 9)

    def _get_chain_id(self, chain):
        chain_ids = {"ethereum": 1, "polygon": 137, "arbitrum": 42161, "base": 8453}
        return chain_ids.get(chain, 8453)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    dex = DEXConnector(chain="base")

    if len(sys.argv) > 1 and sys.argv[1] == "quote":
        # python dex_connector.py quote ETH USDC 0.5
        token_in = sys.argv[2] if len(sys.argv) > 2 else "ETH"
        token_out = sys.argv[3] if len(sys.argv) > 3 else "USDC"
        amount = float(sys.argv[4]) if len(sys.argv) > 4 else 0.1

        print(f"\nGetting quotes for {amount} {token_in} → {token_out}...")

        # Uniswap (Base)
        uni_quote = dex.get_quote_uniswap(token_in, token_out, amount, "base")
        if "error" not in uni_quote:
            print(f"\n  Uniswap (Base): {uni_quote['amount_out']} {token_out}")
            print(f"    Price: {uni_quote['price']}, Fee: {uni_quote['fee_pct']}%")
        else:
            print(f"  Uniswap: {uni_quote['error']}")

        # Jupiter (if SOL pair)
        if token_in in SOLANA_MINTS or token_out in SOLANA_MINTS:
            jup_quote = dex.get_quote_jupiter(token_in, token_out, amount)
            if "error" not in jup_quote:
                print(f"\n  Jupiter (Solana): {jup_quote['amount_out']} {token_out}")
                print(f"    Price: {jup_quote['price']}, Impact: {jup_quote['price_impact_pct']}%")
            else:
                print(f"  Jupiter: {jup_quote['error']}")

    else:
        print("Usage: python dex_connector.py quote [TOKEN_IN] [TOKEN_OUT] [AMOUNT]")
        print("  Example: python dex_connector.py quote ETH USDC 0.5")
