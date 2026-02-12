#!/usr/bin/env python3
"""Uniswap V3 concentrated liquidity provider for on-chain LP positions.

Provides concentrated liquidity on Uniswap V3 (Base chain primarily, USDC/ETH).
Earns 0.3% fees on swaps through our price range.

Strategy:
  - Concentrate liquidity in narrow bands around current price
  - Earn swap fees proportional to our share of in-range liquidity
  - Auto-rebalance when price drifts out of range
  - Max $5 per position (trading rules enforced)

Supported chains:
  - Base (primary — lowest gas, Coinbase ecosystem)
  - Ethereum mainnet
  - Arbitrum
  - Polygon

Usage:
    from liquidity_provider import LiquidityProvider
    lp = LiquidityProvider("0x...", private_key="0x...", chain="base")
    info = lp.get_pool_info("USDC", "WETH")
    pos_id = lp.add_liquidity(pool, usdc_amt, weth_amt, tick_lo, tick_hi)
    fees = lp.collect_fees(pos_id)
"""

import json
import logging
import math
import os
import time
import urllib.request
from decimal import Decimal

logger = logging.getLogger("liquidity_provider")

# ── Contract Addresses ────────────────────────────────────────────────────

# Uniswap V3 NonfungiblePositionManager per chain
POSITION_MANAGER = {
    "base":     "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1",
    "ethereum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "arbitrum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "polygon":  "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
}

# Uniswap V3 Factory per chain
POOL_FACTORY = {
    "base":     "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
    "ethereum": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "arbitrum": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "polygon":  "0x1F98431c8aD98523631AE4a59f267346ea31F984",
}

# Common pool addresses (pre-computed for fast lookup)
KNOWN_POOLS = {
    "base": {
        "USDC/WETH/3000":  "0xd0b53D9277642d899DF5C87A3966A349A798F224",
        "USDC/WETH/500":   "0x4C36388bE6F416A29C8d8Eee81C771cE6bE14B18",
    },
    "ethereum": {
        "USDC/WETH/3000":  "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",
        "USDC/WETH/500":   "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
    },
}

# Token addresses per chain (matches wallet_connector.py / dex_connector.py)
TOKEN_ADDRESSES = {
    "base": {
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "WETH": "0x4200000000000000000000000000000000000006",
        "ETH":  "0x4200000000000000000000000000000000000006",
    },
    "ethereum": {
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "ETH":  "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI":  "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    },
    "arbitrum": {
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "ETH":  "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    },
    "polygon": {
        "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "ETH":  "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
    },
}

# Known decimals
TOKEN_DECIMALS = {
    "USDC": 6, "USDT": 6, "DAI": 18, "WETH": 18, "ETH": 18, "WBTC": 8,
}

# Fee tier -> tick spacing
FEE_TO_TICK_SPACING = {
    100:   1,     # 0.01%
    500:   10,    # 0.05%
    3000:  60,    # 0.3%
    10000: 200,   # 1.0%
}

# Maximum position value in USD (trading rules: max $5 per position)
MAX_POSITION_USD = 5.00
MAX_DAILY_LOSS_USD = 2.00

# Uniswap V3 math constants
Q96 = 2 ** 96
Q128 = 2 ** 128
MIN_TICK = -887272
MAX_TICK = 887272
MAX_UINT128 = (2 ** 128) - 1
MAX_UINT256 = (2 ** 256) - 1

# ── Function Selectors (NonfungiblePositionManager) ───────────────────────

# mint((address,address,uint24,int24,int24,uint256,uint256,uint256,uint256,address,uint256))
SELECTOR_MINT = "0x88316456"

# increaseLiquidity((uint256,uint256,uint256,uint256,uint256,uint256))
SELECTOR_INCREASE_LIQUIDITY = "0x219f5d17"

# decreaseLiquidity((uint256,uint128,uint256,uint256,uint256))
SELECTOR_DECREASE_LIQUIDITY = "0x0c49ccbe"

# collect((uint256,address,uint128,uint128))
SELECTOR_COLLECT = "0xfc6f7865"

# positions(uint256) -> (nonce,operator,token0,token1,fee,tickLower,tickUpper,liquidity,
#   feeGrowthInside0LastX128,feeGrowthInside1LastX128,tokensOwed0,tokensOwed1)
SELECTOR_POSITIONS = "0x99fbab88"

# balanceOf(address)
SELECTOR_BALANCE_OF = "0x70a08231"

# tokenOfOwnerByIndex(address,uint256)
SELECTOR_TOKEN_OF_OWNER = "0x2f745c59"

# ── Pool Function Selectors ──────────────────────────────────────────────

# slot0() -> (sqrtPriceX96,tick,observationIndex,observationCardinality,
#             observationCardinalityNext,feeProtocol,unlocked)
SELECTOR_SLOT0 = "0x3850c7bd"

# liquidity()
SELECTOR_LIQUIDITY = "0x1a686502"

# fee()
SELECTOR_FEE = "0xddca3f43"

# token0()
SELECTOR_TOKEN0 = "0x0dfe1681"

# token1()
SELECTOR_TOKEN1 = "0xd21220a7"

# ── Factory Function Selectors ───────────────────────────────────────────

# getPool(address,address,uint24) -> address
SELECTOR_GET_POOL = "0x1698ee82"


def _fetch_json(url, payload=None, timeout=10):
    """HTTP JSON request helper."""
    if payload:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json", "User-Agent": "NetTrace/1.0"}
        )
    else:
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _get_token_price_usd(symbol):
    """Get current USD price for a token."""
    pair = f"{symbol}-USD"
    try:
        url = f"https://api.exchange.coinbase.com/products/{pair}/ticker"
        data = _fetch_json(url)
        return float(data.get("price", 0))
    except Exception:
        pass
    # WETH is just ETH
    if symbol == "WETH":
        return _get_token_price_usd("ETH")
    # Stablecoins
    if symbol in ("USDC", "USDT", "DAI"):
        return 1.0
    return 0


def _pad32(value):
    """Left-pad a hex value to 32 bytes (64 hex chars)."""
    if isinstance(value, str):
        value = value.lower().replace("0x", "")
    return value.zfill(64)


def _encode_int256(value):
    """Encode a signed int256 as 32-byte hex (two's complement)."""
    if value >= 0:
        return _pad32(hex(value)[2:])
    else:
        return _pad32(hex((1 << 256) + value)[2:])


def _decode_int24(hex_str):
    """Decode a 32-byte hex string as int24 (signed)."""
    val = int(hex_str, 16)
    if val >= (1 << 23):
        val -= (1 << 24)
    # Handle full 256-bit two's complement
    if val >= (1 << 255):
        val -= (1 << 256)
    return val


def _decode_int256(hex_str):
    """Decode a 32-byte hex string as signed int256."""
    val = int(hex_str, 16)
    if val >= (1 << 255):
        val -= (1 << 256)
    return val


# ── Uniswap V3 Math ─────────────────────────────────────────────────────

def tick_to_sqrt_price_x96(tick):
    """Convert tick to sqrtPriceX96."""
    return int(math.sqrt(1.0001 ** tick) * Q96)


def sqrt_price_x96_to_price(sqrt_price_x96, decimals0=6, decimals1=18):
    """Convert sqrtPriceX96 to human-readable price (token0 per token1).

    For USDC/WETH pool: returns USDC per 1 ETH (e.g. ~3000).
    token0 = USDC (6 decimals), token1 = WETH (18 decimals).
    """
    price_raw = (sqrt_price_x96 / Q96) ** 2
    decimal_adjustment = 10 ** (decimals0 - decimals1)
    return price_raw * decimal_adjustment


def price_to_tick(price, decimals0=6, decimals1=18):
    """Convert a human-readable price to the nearest tick.

    price: token0 per token1 (e.g. 3000 USDC per ETH).
    """
    decimal_adjustment = 10 ** (decimals0 - decimals1)
    price_raw = price / decimal_adjustment
    if price_raw <= 0:
        return MIN_TICK
    tick = math.log(price_raw) / math.log(1.0001)
    return int(tick)


def nearest_usable_tick(tick, tick_spacing):
    """Round tick to the nearest valid tick for the given fee tier."""
    return round(tick / tick_spacing) * tick_spacing


def calculate_liquidity_amounts(sqrt_price_x96, tick_lower, tick_upper,
                                 amount0_desired, amount1_desired,
                                 decimals0=6, decimals1=18):
    """Calculate the liquidity delta and actual amounts for a position.

    Returns: (liquidity, amount0_actual, amount1_actual)
    """
    sqrt_price = sqrt_price_x96 / Q96
    sqrt_lower = math.sqrt(1.0001 ** tick_lower)
    sqrt_upper = math.sqrt(1.0001 ** tick_upper)

    # Convert human amounts to raw
    amount0_raw = int(amount0_desired * (10 ** decimals0))
    amount1_raw = int(amount1_desired * (10 ** decimals1))

    # Current price is below range: all token0
    if sqrt_price <= sqrt_lower:
        liquidity = int(amount0_raw * sqrt_lower * sqrt_upper / (sqrt_upper - sqrt_lower))
    # Current price is above range: all token1
    elif sqrt_price >= sqrt_upper:
        liquidity = int(amount1_raw / (sqrt_upper - sqrt_lower))
    # Current price is within range: both tokens
    else:
        liq0 = int(amount0_raw * sqrt_price * sqrt_upper / (sqrt_upper - sqrt_price))
        liq1 = int(amount1_raw / (sqrt_price - sqrt_lower))
        liquidity = min(liq0, liq1)

    return liquidity, amount0_raw, amount1_raw


class LiquidityProvider:
    """Uniswap V3 concentrated liquidity provider.

    Provides liquidity in narrow price ranges to maximize fee income.
    Enforces trading rules: max $5 per position, well-established pools only.
    """

    def __init__(self, wallet_address, private_key=None, chain="base"):
        """Initialize the liquidity provider.

        Args:
            wallet_address: EVM wallet address (0x...)
            private_key: Private key for signing transactions (from env, never hardcode)
            chain: Target chain — "base" recommended (lowest gas fees)
        """
        self.wallet_address = wallet_address
        self.private_key = private_key
        self.chain = chain

        # Load wallet connector for RPC and tx signing
        from wallet_connector import WalletConnector, CHAIN_CONFIG
        self.wallet = WalletConnector(wallet_address, chain, private_key)
        self.config = CHAIN_CONFIG.get(chain, CHAIN_CONFIG["base"])

        # Contract addresses for this chain
        self.position_manager = POSITION_MANAGER.get(chain)
        self.factory = POOL_FACTORY.get(chain)

        if not self.position_manager:
            raise ValueError(f"Unsupported chain for LP: {chain}")

        # Track our positions locally
        self._positions_cache = {}
        self._cache_time = 0

        logger.info("LiquidityProvider initialized: chain=%s, wallet=%s...%s",
                     chain, wallet_address[:6], wallet_address[-4:])

    # ── RPC Helpers ──────────────────────────────────────────────────────

    def _eth_call(self, to, data):
        """Execute eth_call and return raw hex result."""
        return self.wallet._eth_rpc("eth_call", [
            {"to": to, "data": data},
            "latest"
        ])

    def _decode_slot(self, hex_result, slot_index):
        """Extract a 32-byte slot from a multi-return hex result."""
        if not hex_result or hex_result == "0x":
            return None
        # Strip 0x prefix, each slot is 64 hex chars
        raw = hex_result[2:]
        start = slot_index * 64
        end = start + 64
        if end > len(raw):
            return None
        return raw[start:end]

    # ── Pool Information ─────────────────────────────────────────────────

    def get_pool_address(self, token0, token1, fee_tier=3000):
        """Get the Uniswap V3 pool address for a token pair.

        Tries known pools first, then queries the factory contract.
        """
        tokens = TOKEN_ADDRESSES.get(self.chain, {})
        addr0 = tokens.get(token0, token0)
        addr1 = tokens.get(token1, token1)

        # Check known pools cache
        pool_key = f"{token0}/{token1}/{fee_tier}"
        pool_key_rev = f"{token1}/{token0}/{fee_tier}"
        known = KNOWN_POOLS.get(self.chain, {})
        if pool_key in known:
            return known[pool_key]
        if pool_key_rev in known:
            return known[pool_key_rev]

        # Query factory: getPool(address,address,uint24)
        call_data = (
            SELECTOR_GET_POOL +
            _pad32(addr0) +
            _pad32(addr1) +
            _pad32(hex(fee_tier)[2:])
        )

        result = self._eth_call(self.factory, call_data)
        if not result or result == "0x" or int(result, 16) == 0:
            # Try reversed order
            call_data = (
                SELECTOR_GET_POOL +
                _pad32(addr1) +
                _pad32(addr0) +
                _pad32(hex(fee_tier)[2:])
            )
            result = self._eth_call(self.factory, call_data)

        if not result or result == "0x" or int(result, 16) == 0:
            raise ValueError(f"No pool found for {token0}/{token1} fee={fee_tier} on {self.chain}")

        pool_address = "0x" + result[-40:]
        return pool_address

    def get_pool_info(self, token0, token1, fee_tier=3000):
        """Get current pool state: tick, price, liquidity.

        Args:
            token0: Token symbol or address (e.g. "USDC")
            token1: Token symbol or address (e.g. "WETH")
            fee_tier: Fee tier in hundredths of bip (3000 = 0.3%)

        Returns: {
            "pool_address": "0x...",
            "current_tick": -201234,
            "sqrt_price_x96": 123456...,
            "liquidity": 123456...,
            "token0_price": 3000.50,  (token0 per token1, e.g. USDC per ETH)
            "fee_tier": 3000,
            "fee_pct": 0.3,
            "tick_spacing": 60,
        }
        """
        pool_address = self.get_pool_address(token0, token1, fee_tier)

        # Read slot0
        slot0_result = self._eth_call(pool_address, SELECTOR_SLOT0)
        if not slot0_result or slot0_result == "0x":
            raise ValueError(f"Failed to read slot0 from pool {pool_address}")

        sqrt_price_x96_hex = self._decode_slot(slot0_result, 0)
        tick_hex = self._decode_slot(slot0_result, 1)

        sqrt_price_x96 = int(sqrt_price_x96_hex, 16)
        current_tick = _decode_int256(tick_hex)

        # Read total liquidity
        liq_result = self._eth_call(pool_address, SELECTOR_LIQUIDITY)
        total_liquidity = int(liq_result, 16) if liq_result and liq_result != "0x" else 0

        # Get token decimals
        dec0 = TOKEN_DECIMALS.get(token0, 18)
        dec1 = TOKEN_DECIMALS.get(token1, 18)

        # Calculate human-readable price
        token0_price = sqrt_price_x96_to_price(sqrt_price_x96, dec0, dec1)

        tick_spacing = FEE_TO_TICK_SPACING.get(fee_tier, 60)

        return {
            "pool_address": pool_address,
            "current_tick": current_tick,
            "sqrt_price_x96": sqrt_price_x96,
            "liquidity": total_liquidity,
            "token0_price": round(token0_price, 4),
            "fee_tier": fee_tier,
            "fee_pct": fee_tier / 10000,
            "tick_spacing": tick_spacing,
            "token0": token0,
            "token1": token1,
            "chain": self.chain,
        }

    def calculate_optimal_range(self, token0, token1, width_pct=5.0, fee_tier=3000):
        """Calculate optimal tick range centered on current price.

        A narrower range = more concentrated liquidity = more fees per dollar,
        but higher risk of going out of range.

        Args:
            token0, token1: Token symbols
            width_pct: Total width of range as % of current price (default 5%)
            fee_tier: Pool fee tier

        Returns: {
            "tick_lower": -201300,
            "tick_upper": -200700,
            "price_lower": 2850.0,
            "price_upper": 3150.0,
            "current_price": 3000.0,
            "width_pct": 5.0,
        }
        """
        pool_info = self.get_pool_info(token0, token1, fee_tier)
        current_price = pool_info["token0_price"]
        tick_spacing = pool_info["tick_spacing"]

        dec0 = TOKEN_DECIMALS.get(token0, 18)
        dec1 = TOKEN_DECIMALS.get(token1, 18)

        # Calculate price bounds
        half_width = width_pct / 200.0  # half width as fraction
        price_lower = current_price * (1 - half_width)
        price_upper = current_price * (1 + half_width)

        # Convert to ticks and snap to tick spacing
        tick_lower = price_to_tick(price_lower, dec0, dec1)
        tick_upper = price_to_tick(price_upper, dec0, dec1)

        tick_lower = nearest_usable_tick(tick_lower, tick_spacing)
        tick_upper = nearest_usable_tick(tick_upper, tick_spacing)

        # Ensure lower < upper
        if tick_lower >= tick_upper:
            tick_upper = tick_lower + tick_spacing

        # Recalculate actual prices at snapped ticks
        actual_price_lower = sqrt_price_x96_to_price(
            tick_to_sqrt_price_x96(tick_lower), dec0, dec1)
        actual_price_upper = sqrt_price_x96_to_price(
            tick_to_sqrt_price_x96(tick_upper), dec0, dec1)

        return {
            "tick_lower": tick_lower,
            "tick_upper": tick_upper,
            "price_lower": round(actual_price_lower, 2),
            "price_upper": round(actual_price_upper, 2),
            "current_price": round(current_price, 2),
            "width_pct": width_pct,
            "tick_spacing": tick_spacing,
            "fee_tier": fee_tier,
        }

    # ── Liquidity Management ─────────────────────────────────────────────

    def add_liquidity(self, pool_address, token0_amount, token1_amount,
                      tick_lower, tick_upper, fee_tier=3000):
        """Open a new concentrated liquidity position.

        Args:
            pool_address: Uniswap V3 pool address
            token0_amount: Amount of token0 (human-readable, e.g. 2.50 USDC)
            token1_amount: Amount of token1 (human-readable, e.g. 0.001 WETH)
            tick_lower: Lower tick bound
            tick_upper: Upper tick bound
            fee_tier: Pool fee tier (3000 = 0.3%)

        Returns: position_id (NFT token ID) or raises on failure.
        """
        if not self.private_key:
            raise ValueError("Private key required to add liquidity")

        # ── Trading rule enforcement ──────────────────────────────
        # Read pool tokens to price the position
        token0_addr = self._read_pool_token(pool_address, 0)
        token1_addr = self._read_pool_token(pool_address, 1)
        token0_sym = self._addr_to_symbol(token0_addr)
        token1_sym = self._addr_to_symbol(token1_addr)
        dec0 = TOKEN_DECIMALS.get(token0_sym, 18)
        dec1 = TOKEN_DECIMALS.get(token1_sym, 18)

        price0 = _get_token_price_usd(token0_sym)
        price1 = _get_token_price_usd(token1_sym)
        position_value_usd = (token0_amount * price0) + (token1_amount * price1)

        if position_value_usd > MAX_POSITION_USD:
            raise ValueError(
                f"Position value ${position_value_usd:.2f} exceeds max ${MAX_POSITION_USD:.2f}. "
                f"Trading rules: max $5 per position."
            )

        logger.info("Adding liquidity: %.4f %s + %.6f %s (~$%.2f) ticks=[%d, %d]",
                     token0_amount, token0_sym, token1_amount, token1_sym,
                     position_value_usd, tick_lower, tick_upper)

        # ── Ensure token approvals ────────────────────────────────
        amount0_raw = int(token0_amount * (10 ** dec0))
        amount1_raw = int(token1_amount * (10 ** dec1))

        if amount0_raw > 0:
            self.wallet.ensure_approved(token0_addr, self.position_manager, amount0_raw)
        if amount1_raw > 0:
            self.wallet.ensure_approved(token1_addr, self.position_manager, amount1_raw)

        # ── Build mint calldata ───────────────────────────────────
        # Sort tokens (Uniswap requires token0 < token1 by address)
        # The pool already has them sorted, so use the pool's ordering
        deadline = int(time.time()) + 600  # 10 min deadline

        # Minimum amounts (allow 2% slippage on amounts)
        amount0_min = int(amount0_raw * 0.98)
        amount1_min = int(amount1_raw * 0.98)

        # mint((address,address,uint24,int24,int24,uint256,uint256,uint256,uint256,address,uint256))
        # Encode as ABI tuple: offset pointer + packed fields
        call_data = SELECTOR_MINT

        # The struct is encoded inline (not as dynamic — all fixed-size fields)
        call_data += _pad32(token0_addr)                              # token0
        call_data += _pad32(token1_addr)                              # token1
        call_data += _pad32(hex(fee_tier)[2:])                        # fee
        call_data += _encode_int256(tick_lower)                       # tickLower
        call_data += _encode_int256(tick_upper)                       # tickUpper
        call_data += _pad32(hex(amount0_raw)[2:])                     # amount0Desired
        call_data += _pad32(hex(amount1_raw)[2:])                     # amount1Desired
        call_data += _pad32(hex(amount0_min)[2:])                     # amount0Min
        call_data += _pad32(hex(amount1_min)[2:])                     # amount1Min
        call_data += _pad32(self.wallet_address)                      # recipient
        call_data += _pad32(hex(deadline)[2:])                        # deadline

        # ── Simulate first ────────────────────────────────────────
        tx_params = {
            "to": self.position_manager,
            "data": "0x" + call_data[len(SELECTOR_MINT):].lstrip("0x") if not call_data.startswith("0x") else call_data,
            "value": 0,
            "chainId": self.config["chain_id"],
        }
        # Ensure data is properly 0x-prefixed
        if not tx_params["data"].startswith("0x"):
            tx_params["data"] = "0x" + tx_params["data"]

        try:
            sim_result = self.wallet.simulate_tx(tx_params)
            logger.debug("Mint simulation result: %s...", str(sim_result)[:80])
        except Exception as e:
            raise ValueError(f"Mint simulation failed (likely bad tick range or insufficient balance): {e}")

        # ── Send transaction ──────────────────────────────────────
        gas_fees = self.wallet.get_eip1559_fees()
        tx_params["maxFeePerGas"] = gas_fees["maxFeePerGas"]
        tx_params["maxPriorityFeePerGas"] = gas_fees["maxPriorityFeePerGas"]

        gas_est = self.wallet.estimate_gas(tx_params)
        tx_params["gas"] = int(gas_est * 1.3)  # 30% buffer for complex LP tx

        tx_hash = self.wallet.sign_and_send(tx_params)
        logger.info("Mint tx sent: %s", tx_hash)

        # Wait for confirmation
        receipt = self.wallet.wait_for_tx(tx_hash, confirmations=2)
        if not receipt or receipt.get("status") != 1:
            raise Exception(f"Mint transaction failed: {tx_hash}")

        # Parse the IncreaseLiquidity event to get tokenId
        # For now, read latest position from NFT
        position_id = self._get_latest_position_id()
        logger.info("New position created: ID=%s, tx=%s", position_id, tx_hash)

        # Invalidate cache
        self._cache_time = 0

        return position_id

    def remove_liquidity(self, position_id, liquidity_pct=100):
        """Remove liquidity from a position (partial or full).

        Args:
            position_id: NFT token ID
            liquidity_pct: Percentage of liquidity to remove (1-100)

        Returns: {
            "token0_amount": 2.50,
            "token1_amount": 0.001,
            "tx_hash": "0x...",
        }
        """
        if not self.private_key:
            raise ValueError("Private key required to remove liquidity")

        if liquidity_pct < 1 or liquidity_pct > 100:
            raise ValueError("liquidity_pct must be 1-100")

        # Read current position to get liquidity amount
        pos = self._read_position(position_id)
        if not pos:
            raise ValueError(f"Position {position_id} not found or not owned by us")

        total_liquidity = pos["liquidity"]
        if total_liquidity == 0:
            raise ValueError(f"Position {position_id} has zero liquidity")

        remove_liquidity = int(total_liquidity * liquidity_pct / 100)
        deadline = int(time.time()) + 600

        logger.info("Removing %d%% liquidity from position %s (liq=%d, removing=%d)",
                     liquidity_pct, position_id, total_liquidity, remove_liquidity)

        # decreaseLiquidity((uint256,uint128,uint256,uint256,uint256))
        call_data = SELECTOR_DECREASE_LIQUIDITY
        call_data += _pad32(hex(position_id)[2:])         # tokenId
        call_data += _pad32(hex(remove_liquidity)[2:])    # liquidity
        call_data += _pad32("0")                          # amount0Min (0 for simplicity)
        call_data += _pad32("0")                          # amount1Min
        call_data += _pad32(hex(deadline)[2:])            # deadline

        tx_params = {
            "to": self.position_manager,
            "data": "0x" + call_data[2:] if call_data.startswith("0x") else "0x" + call_data,
            "value": 0,
            "chainId": self.config["chain_id"],
        }
        # Fix data prefix
        if not tx_params["data"].startswith("0x"):
            tx_params["data"] = "0x" + tx_params["data"]

        gas_fees = self.wallet.get_eip1559_fees()
        tx_params["maxFeePerGas"] = gas_fees["maxFeePerGas"]
        tx_params["maxPriorityFeePerGas"] = gas_fees["maxPriorityFeePerGas"]

        gas_est = self.wallet.estimate_gas(tx_params)
        tx_params["gas"] = int(gas_est * 1.3)

        tx_hash = self.wallet.sign_and_send(tx_params)
        logger.info("DecreaseLiquidity tx sent: %s", tx_hash)

        receipt = self.wallet.wait_for_tx(tx_hash, confirmations=2)
        if not receipt or receipt.get("status") != 1:
            raise Exception(f"DecreaseLiquidity failed: {tx_hash}")

        # Now collect the tokens that were freed
        collect_result = self.collect_fees(position_id)

        self._cache_time = 0

        return {
            "token0_amount": collect_result.get("token0_fees", 0),
            "token1_amount": collect_result.get("token1_fees", 0),
            "tx_hash": tx_hash,
            "collect_tx": collect_result.get("tx_hash"),
        }

    def collect_fees(self, position_id):
        """Collect accumulated fees from a position.

        Args:
            position_id: NFT token ID

        Returns: {
            "token0_fees": 0.05,
            "token1_fees": 0.00001,
            "total_usd": 0.08,
            "tx_hash": "0x...",
        }
        """
        if not self.private_key:
            raise ValueError("Private key required to collect fees")

        # collect((uint256,address,uint128,uint128))
        call_data = SELECTOR_COLLECT
        call_data += _pad32(hex(position_id)[2:])              # tokenId
        call_data += _pad32(self.wallet_address)               # recipient
        call_data += _pad32(hex(MAX_UINT128)[2:])              # amount0Max (collect all)
        call_data += _pad32(hex(MAX_UINT128)[2:])              # amount1Max (collect all)

        tx_params = {
            "to": self.position_manager,
            "data": "0x" + call_data[2:] if call_data.startswith("0x") else "0x" + call_data,
            "value": 0,
            "chainId": self.config["chain_id"],
        }
        if not tx_params["data"].startswith("0x"):
            tx_params["data"] = "0x" + tx_params["data"]

        # Simulate first to get amounts
        try:
            sim = self.wallet.simulate_tx(tx_params)
            # Parse collect return: (uint256 amount0, uint256 amount1)
            if sim and sim != "0x" and len(sim) >= 130:
                raw0 = int(sim[2:66], 16)
                raw1 = int(sim[66:130], 16)
            else:
                raw0, raw1 = 0, 0
        except Exception:
            raw0, raw1 = 0, 0

        gas_fees = self.wallet.get_eip1559_fees()
        tx_params["maxFeePerGas"] = gas_fees["maxFeePerGas"]
        tx_params["maxPriorityFeePerGas"] = gas_fees["maxPriorityFeePerGas"]

        gas_est = self.wallet.estimate_gas(tx_params)
        tx_params["gas"] = int(gas_est * 1.3)

        tx_hash = self.wallet.sign_and_send(tx_params)
        logger.info("Collect tx sent: %s", tx_hash)

        receipt = self.wallet.wait_for_tx(tx_hash, confirmations=2)
        if not receipt or receipt.get("status") != 1:
            raise Exception(f"Collect failed: {tx_hash}")

        # Read position to determine tokens
        pos = self._read_position(position_id)
        dec0 = TOKEN_DECIMALS.get(pos.get("token0_symbol", ""), 18) if pos else 18
        dec1 = TOKEN_DECIMALS.get(pos.get("token1_symbol", ""), 18) if pos else 18

        token0_fees = raw0 / (10 ** dec0)
        token1_fees = raw1 / (10 ** dec1)

        # USD value
        sym0 = pos.get("token0_symbol", "USDC") if pos else "USDC"
        sym1 = pos.get("token1_symbol", "WETH") if pos else "WETH"
        price0 = _get_token_price_usd(sym0)
        price1 = _get_token_price_usd(sym1)
        total_usd = (token0_fees * price0) + (token1_fees * price1)

        logger.info("Collected fees: %.6f %s + %.8f %s ($%.4f)",
                     token0_fees, sym0, token1_fees, sym1, total_usd)

        return {
            "token0_fees": round(token0_fees, 8),
            "token1_fees": round(token1_fees, 8),
            "total_usd": round(total_usd, 4),
            "tx_hash": tx_hash,
            "position_id": position_id,
        }

    def rebalance(self, position_id, new_tick_lower, new_tick_upper):
        """Rebalance a position to a new price range.

        This removes all liquidity from the old position, collects fees,
        and opens a new position at the new tick range.

        Args:
            position_id: Current NFT token ID
            new_tick_lower: New lower tick
            new_tick_upper: New upper tick

        Returns: new_position_id
        """
        logger.info("Rebalancing position %s to ticks [%d, %d]",
                     position_id, new_tick_lower, new_tick_upper)

        # Read current position for pool info
        pos = self._read_position(position_id)
        if not pos:
            raise ValueError(f"Position {position_id} not found")

        pool_address = self.get_pool_address(
            pos["token0_symbol"], pos["token1_symbol"], pos["fee"]
        )

        # Remove all liquidity + collect fees
        removal = self.remove_liquidity(position_id, liquidity_pct=100)

        token0_amount = removal["token0_amount"]
        token1_amount = removal["token1_amount"]

        if token0_amount <= 0 and token1_amount <= 0:
            raise ValueError("No tokens recovered from position removal")

        # Open new position at new range
        new_position_id = self.add_liquidity(
            pool_address,
            token0_amount,
            token1_amount,
            new_tick_lower,
            new_tick_upper,
            pos["fee"],
        )

        logger.info("Rebalanced: old=%s -> new=%s", position_id, new_position_id)
        return new_position_id

    # ── Position Reading ─────────────────────────────────────────────────

    def get_positions(self):
        """Get all our LP positions with current values and uncollected fees.

        Returns: list of position dicts with:
            - position_id, token0, token1, fee, tick_lower, tick_upper
            - liquidity, in_range (bool)
            - token0_amount, token1_amount (current)
            - uncollected_fees_0, uncollected_fees_1
            - total_value_usd
        """
        if time.time() - self._cache_time < 30 and self._positions_cache:
            return list(self._positions_cache.values())

        positions = []

        # Read number of NFTs owned
        call_data = SELECTOR_BALANCE_OF + _pad32(self.wallet_address)
        result = self._eth_call(self.position_manager, call_data)
        if not result or result == "0x":
            return []

        num_positions = int(result, 16)
        logger.debug("Found %d position NFTs", num_positions)

        for i in range(num_positions):
            try:
                # tokenOfOwnerByIndex(address, uint256) -> tokenId
                tok_data = (
                    SELECTOR_TOKEN_OF_OWNER +
                    _pad32(self.wallet_address) +
                    _pad32(hex(i)[2:])
                )
                tok_result = self._eth_call(self.position_manager, tok_data)
                if not tok_result or tok_result == "0x":
                    continue

                token_id = int(tok_result, 16)
                pos = self._read_position(token_id)
                if pos and pos["liquidity"] > 0:
                    # Get current pool tick to check if in range
                    try:
                        pool_info = self.get_pool_info(
                            pos["token0_symbol"], pos["token1_symbol"], pos["fee"]
                        )
                        pos["in_range"] = (
                            pool_info["current_tick"] >= pos["tick_lower"] and
                            pool_info["current_tick"] < pos["tick_upper"]
                        )
                        pos["current_tick"] = pool_info["current_tick"]
                        pos["current_price"] = pool_info["token0_price"]
                    except Exception:
                        pos["in_range"] = None
                        pos["current_tick"] = None
                        pos["current_price"] = None

                    # Calculate USD value
                    price0 = _get_token_price_usd(pos["token0_symbol"])
                    price1 = _get_token_price_usd(pos["token1_symbol"])
                    pos["total_value_usd"] = round(
                        (pos.get("tokens_owed0", 0) * price0) +
                        (pos.get("tokens_owed1", 0) * price1),
                        4
                    )

                    positions.append(pos)

            except Exception as e:
                logger.debug("Failed to read position index %d: %s", i, e)

        # Update cache
        self._positions_cache = {p["position_id"]: p for p in positions}
        self._cache_time = time.time()

        return positions

    def _read_position(self, position_id):
        """Read a single position's data from the NonfungiblePositionManager.

        positions(uint256) returns 12 fields packed in order:
          nonce, operator, token0, token1, fee, tickLower, tickUpper,
          liquidity, feeGrowthInside0LastX128, feeGrowthInside1LastX128,
          tokensOwed0, tokensOwed1
        """
        call_data = SELECTOR_POSITIONS + _pad32(hex(position_id)[2:])
        result = self._eth_call(self.position_manager, call_data)

        if not result or result == "0x" or len(result) < (2 + 12 * 64):
            return None

        raw = result[2:]  # strip 0x

        # Parse each 32-byte slot
        nonce = int(raw[0:64], 16)
        operator = "0x" + raw[64:128][-40:]
        token0_addr = "0x" + raw[128:192][-40:]
        token1_addr = "0x" + raw[192:256][-40:]
        fee = int(raw[256:320], 16)
        tick_lower = _decode_int256(raw[320:384])
        tick_upper = _decode_int256(raw[384:448])
        liquidity = int(raw[448:512], 16)
        fee_growth_0 = int(raw[512:576], 16)
        fee_growth_1 = int(raw[576:640], 16)
        tokens_owed0_raw = int(raw[640:704], 16)
        tokens_owed1_raw = int(raw[704:768], 16)

        token0_sym = self._addr_to_symbol(token0_addr)
        token1_sym = self._addr_to_symbol(token1_addr)
        dec0 = TOKEN_DECIMALS.get(token0_sym, 18)
        dec1 = TOKEN_DECIMALS.get(token1_sym, 18)

        return {
            "position_id": position_id,
            "nonce": nonce,
            "operator": operator,
            "token0_address": token0_addr,
            "token1_address": token1_addr,
            "token0_symbol": token0_sym,
            "token1_symbol": token1_sym,
            "fee": fee,
            "tick_lower": tick_lower,
            "tick_upper": tick_upper,
            "liquidity": liquidity,
            "fee_growth_inside0_x128": fee_growth_0,
            "fee_growth_inside1_x128": fee_growth_1,
            "tokens_owed0": tokens_owed0_raw / (10 ** dec0),
            "tokens_owed1": tokens_owed1_raw / (10 ** dec1),
        }

    def _read_pool_token(self, pool_address, index):
        """Read token0 or token1 address from a pool contract."""
        selector = SELECTOR_TOKEN0 if index == 0 else SELECTOR_TOKEN1
        result = self._eth_call(pool_address, selector)
        if not result or result == "0x":
            return None
        return "0x" + result[-40:]

    def _addr_to_symbol(self, address):
        """Reverse-lookup: token address to symbol."""
        addr_lower = address.lower()
        tokens = TOKEN_ADDRESSES.get(self.chain, {})
        for sym, addr in tokens.items():
            if addr.lower() == addr_lower:
                return sym
        return "UNKNOWN"

    def _get_latest_position_id(self):
        """Get the most recently minted position NFT owned by us.

        Reads balanceOf then tokenOfOwnerByIndex(balance - 1).
        """
        call_data = SELECTOR_BALANCE_OF + _pad32(self.wallet_address)
        result = self._eth_call(self.position_manager, call_data)
        if not result or result == "0x":
            return None

        count = int(result, 16)
        if count == 0:
            return None

        # Get the last token
        tok_data = (
            SELECTOR_TOKEN_OF_OWNER +
            _pad32(self.wallet_address) +
            _pad32(hex(count - 1)[2:])
        )
        tok_result = self._eth_call(self.position_manager, tok_data)
        if not tok_result or tok_result == "0x":
            return None

        return int(tok_result, 16)

    # ── Safety & Reporting ───────────────────────────────────────────────

    def check_position_health(self, position_id):
        """Check if a position is in range and earning fees.

        Returns: {
            "in_range": True/False,
            "distance_to_lower_pct": 2.5,
            "distance_to_upper_pct": 2.5,
            "recommendation": "hold" | "rebalance" | "collect"
        }
        """
        pos = self._read_position(position_id)
        if not pos:
            return {"error": f"Position {position_id} not found"}

        try:
            pool_info = self.get_pool_info(pos["token0_symbol"], pos["token1_symbol"], pos["fee"])
        except Exception as e:
            return {"error": f"Failed to read pool: {e}"}

        current_tick = pool_info["current_tick"]
        tick_lower = pos["tick_lower"]
        tick_upper = pos["tick_upper"]
        tick_range = tick_upper - tick_lower

        in_range = tick_lower <= current_tick < tick_upper

        if tick_range > 0:
            dist_lower_pct = ((current_tick - tick_lower) / tick_range) * 100
            dist_upper_pct = ((tick_upper - current_tick) / tick_range) * 100
        else:
            dist_lower_pct = 0
            dist_upper_pct = 0

        # Recommendation logic
        if not in_range:
            recommendation = "rebalance"
        elif dist_lower_pct < 10 or dist_upper_pct < 10:
            recommendation = "rebalance"  # close to edge
        elif pos["tokens_owed0"] > 0 or pos["tokens_owed1"] > 0:
            recommendation = "collect"
        else:
            recommendation = "hold"

        return {
            "position_id": position_id,
            "in_range": in_range,
            "current_tick": current_tick,
            "tick_lower": tick_lower,
            "tick_upper": tick_upper,
            "distance_to_lower_pct": round(dist_lower_pct, 2),
            "distance_to_upper_pct": round(dist_upper_pct, 2),
            "recommendation": recommendation,
            "current_price": pool_info["token0_price"],
        }

    def get_total_value_usd(self):
        """Total value of all our LP positions in USD."""
        positions = self.get_positions()
        return round(sum(p.get("total_value_usd", 0) for p in positions), 2)


# ── Main ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Default: show USDC/WETH pool info on Base
    chain = sys.argv[1] if len(sys.argv) > 1 else "base"
    wallet_addr = os.environ.get("WALLET_ADDRESS", "")

    print(f"\n{'='*60}")
    print(f"  Uniswap V3 Liquidity Provider — {chain.upper()}")
    print(f"{'='*60}")

    try:
        lp = LiquidityProvider(
            wallet_address=wallet_addr or "0x0000000000000000000000000000000000000000",
            chain=chain,
        )

        # Pool info for USDC/WETH 0.3% fee tier
        print(f"\n  Fetching USDC/WETH pool info (0.3% fee tier)...")
        pool = lp.get_pool_info("USDC", "WETH", fee_tier=3000)

        print(f"\n  Pool:          {pool['pool_address']}")
        print(f"  Current Tick:  {pool['current_tick']}")
        print(f"  ETH Price:     ${pool['token0_price']:,.2f} USDC")
        print(f"  Liquidity:     {pool['liquidity']:,}")
        print(f"  Fee Tier:      {pool['fee_pct']}%")
        print(f"  Tick Spacing:  {pool['tick_spacing']}")

        # Calculate optimal range
        print(f"\n  Calculating optimal LP range (5% width)...")
        optimal = lp.calculate_optimal_range("USDC", "WETH", width_pct=5.0)

        print(f"\n  Range Lower:   ${optimal['price_lower']:,.2f}")
        print(f"  Range Upper:   ${optimal['price_upper']:,.2f}")
        print(f"  Tick Lower:    {optimal['tick_lower']}")
        print(f"  Tick Upper:    {optimal['tick_upper']}")

        # Also show the 0.05% pool for comparison
        print(f"\n  Fetching USDC/WETH pool info (0.05% fee tier)...")
        try:
            pool_05 = lp.get_pool_info("USDC", "WETH", fee_tier=500)
            print(f"  Pool (0.05%):  {pool_05['pool_address']}")
            print(f"  ETH Price:     ${pool_05['token0_price']:,.2f} USDC")
            print(f"  Liquidity:     {pool_05['liquidity']:,}")
        except Exception as e:
            print(f"  0.05% pool: {e}")

        # Show positions if wallet is provided
        if wallet_addr:
            print(f"\n  Checking positions for {wallet_addr[:6]}...{wallet_addr[-4:]}...")
            positions = lp.get_positions()
            if positions:
                for pos in positions:
                    in_range_str = "IN RANGE" if pos.get("in_range") else "OUT OF RANGE"
                    print(f"\n  Position #{pos['position_id']}: {pos['token0_symbol']}/{pos['token1_symbol']}")
                    print(f"    Fee Tier:    {pos['fee'] / 10000}%")
                    print(f"    Ticks:       [{pos['tick_lower']}, {pos['tick_upper']}]")
                    print(f"    Liquidity:   {pos['liquidity']:,}")
                    print(f"    Status:      {in_range_str}")
                    print(f"    Value:       ${pos.get('total_value_usd', 0):.4f}")
            else:
                print(f"  No active LP positions found.")

    except Exception as e:
        print(f"\n  Error: {e}")
        logger.exception("Failed to get pool info")

    print(f"\n  Trading rules: max ${MAX_POSITION_USD:.2f}/position, "
          f"${MAX_DAILY_LOSS_USD:.2f}/day loss limit")
    print(f"{'='*60}\n")
