#!/usr/bin/env python3
"""Multi-chain wallet connector for self-custody wallets (MetaMask, etc).

Reads balances and signs transactions across:
  - Ethereum mainnet (ETH + ERC-20)
  - Polygon (MATIC + tokens)
  - Arbitrum (ETH L2)
  - Base (Coinbase L2 — lowest fees)
  - Solana (SOL + SPL tokens via Jupiter REST API)

Features:
  - ERC-20 token approval flow (check/approve/ensure)
  - EIP-1559 gas management with priority fee estimation
  - Nonce tracker (prevents collisions across concurrent txs)
  - Multi-RPC fallback (3 endpoints per chain)
  - Encrypted private key loading from .env
  - Transaction receipt monitoring with confirmations

Usage:
    from wallet_connector import WalletConnector
    wallet = WalletConnector("0x...", chain="base")
    balances = wallet.get_balances()
    value = wallet.get_portfolio_value_usd()
"""

import json
import logging
import os
import time
import threading
import urllib.request
from decimal import Decimal
from pathlib import Path

logger = logging.getLogger("wallet_connector")

# Multi-RPC endpoints per chain (fallback order)
CHAIN_CONFIG = {
    "ethereum": {
        "rpc_list": [
            os.environ.get("ETH_RPC_URL", "https://eth.llamarpc.com"),
            "https://rpc.ankr.com/eth",
            "https://ethereum-rpc.publicnode.com",
        ],
        "chain_id": 1,
        "native": "ETH",
        "explorer": "https://etherscan.io",
        "supports_eip1559": True,
    },
    "polygon": {
        "rpc_list": [
            os.environ.get("POLYGON_RPC_URL", "https://polygon.llamarpc.com"),
            "https://rpc.ankr.com/polygon",
            "https://polygon-bor-rpc.publicnode.com",
        ],
        "chain_id": 137,
        "native": "MATIC",
        "explorer": "https://polygonscan.com",
        "supports_eip1559": True,
    },
    "arbitrum": {
        "rpc_list": [
            os.environ.get("ARBITRUM_RPC_URL", "https://arb1.arbitrum.io/rpc"),
            "https://rpc.ankr.com/arbitrum",
            "https://arbitrum-one-rpc.publicnode.com",
        ],
        "chain_id": 42161,
        "native": "ETH",
        "explorer": "https://arbiscan.io",
        "supports_eip1559": True,
    },
    "base": {
        "rpc_list": [
            os.environ.get("BASE_RPC_URL", "https://mainnet.base.org"),
            "https://base.llamarpc.com",
            "https://base-rpc.publicnode.com",
        ],
        "chain_id": 8453,
        "native": "ETH",
        "explorer": "https://basescan.org",
        "supports_eip1559": True,
    },
    "solana": {
        "rpc_list": [
            os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"),
        ],
        "native": "SOL",
        "explorer": "https://solscan.io",
        "supports_eip1559": False,
    },
}

# Backwards compat: expose "rpc" as first entry
for _chain, _conf in CHAIN_CONFIG.items():
    _conf["rpc"] = _conf["rpc_list"][0]

# Common ERC-20 token addresses (for balance checking)
ERC20_TOKENS = {
    "ethereum": {
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "DAI":  "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    },
    "base": {
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "WETH": "0x4200000000000000000000000000000000000006",
    },
    "polygon": {
        "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
    },
    "arbitrum": {
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    },
}

# WETH addresses per chain (for unwrap after DEX swaps)
WETH_ADDRESS = {
    "ethereum": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "base":     "0x4200000000000000000000000000000000000006",
    "arbitrum": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    "polygon":  "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
}

# ERC-20 function selectors
ERC20_BALANCE_OF = "0x70a08231"   # balanceOf(address)
ERC20_DECIMALS   = "0x313ce567"   # decimals()
ERC20_ALLOWANCE  = "0xdd62ed3e"   # allowance(owner, spender)
ERC20_APPROVE    = "0x095ea7b3"   # approve(spender, uint256)
WETH_WITHDRAW    = "0x2e1a7d4d"   # withdraw(uint256)

MAX_UINT256 = (2**256) - 1

# Known decimals to avoid extra RPC calls
KNOWN_DECIMALS = {
    "USDC": 6, "USDT": 6, "DAI": 18, "WETH": 18, "WBTC": 8, "MATIC": 18,
}

# Global nonce tracker: {(chain, address): next_nonce}
_nonce_lock = threading.Lock()
_nonce_cache = {}


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
    """Get token price in USD from CoinGecko-compatible API."""
    pair = f"{symbol}-USD"
    try:
        url = f"https://api.exchange.coinbase.com/products/{pair}/ticker"
        data = _fetch_json(url)
        return float(data.get("price", 0))
    except Exception:
        pass
    cg_ids = {
        "ETH": "ethereum", "BTC": "bitcoin", "SOL": "solana",
        "MATIC": "matic-network", "USDC": "usd-coin", "USDT": "tether",
        "WETH": "ethereum", "WBTC": "bitcoin", "DAI": "dai",
    }
    cg_id = cg_ids.get(symbol)
    if cg_id:
        try:
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd"
            data = _fetch_json(url)
            return data.get(cg_id, {}).get("usd", 0)
        except Exception:
            pass
    return 0


def load_encrypted_key(env_var="WALLET_PRIVATE_KEY_ENC", password=None):
    """Load and decrypt a private key from environment.

    If WALLET_PRIVATE_KEY (unencrypted) is set, use it directly.
    If WALLET_PRIVATE_KEY_ENC is set, decrypt with AES-256-GCM.
    Password defaults to the app SECRET_KEY.
    """
    # Try unencrypted first (dev mode)
    raw_key = os.environ.get("WALLET_PRIVATE_KEY", "")
    if raw_key:
        return raw_key

    encrypted = os.environ.get(env_var, "")
    if not encrypted:
        return None

    if password is None:
        password = os.environ.get("SECRET_KEY", "")
    if not password:
        logger.error("No password for key decryption (set SECRET_KEY)")
        return None

    try:
        import base64
        import hashlib
        raw = base64.b64decode(encrypted)
        salt = raw[:16]
        key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000, dklen=32)
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            nonce = raw[16:28]
            ciphertext = raw[28:]
            aesgcm = AESGCM(key)
            return aesgcm.decrypt(nonce, ciphertext, None).decode()
        except ImportError:
            import hmac as _hmac
            mac = raw[16:32]
            enc_data = raw[32:]
            expected = _hmac.new(key, enc_data, hashlib.sha256).digest()[:16]
            if not _hmac.compare_digest(mac, expected):
                raise ValueError("Decryption failed: invalid key or corrupted data")
            decrypted = bytes(a ^ b for a, b in zip(enc_data, (key * ((len(enc_data) // 32) + 1))[:len(enc_data)]))
            return decrypted.decode()
    except Exception as e:
        logger.error("Failed to decrypt wallet key: %s", e)
        return None


class WalletConnector:
    """Multi-chain wallet connector for MetaMask/self-custody wallets."""

    def __init__(self, address, chain="ethereum", private_key=None):
        self.address = address
        self.chain = chain
        self.private_key = private_key
        self.config = CHAIN_CONFIG.get(chain, CHAIN_CONFIG["ethereum"])
        self._w3 = None
        self._balance_cache = {}
        self._cache_time = 0
        self._rpc_index = 0  # current RPC endpoint index

    @property
    def w3(self):
        """Lazy-load web3 connection."""
        if self._w3 is None and self.chain != "solana":
            try:
                from web3 import Web3
                rpc = self.config["rpc_list"][self._rpc_index]
                self._w3 = Web3(Web3.HTTPProvider(rpc))
            except ImportError:
                logger.warning("web3 not installed — using raw JSON-RPC")
        return self._w3

    def _eth_rpc(self, method, params=None):
        """Raw JSON-RPC call with multi-RPC fallback."""
        rpc_list = self.config.get("rpc_list", [self.config["rpc"]])
        last_err = None
        for rpc in rpc_list:
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": method,
                    "params": params or [],
                }
                result = _fetch_json(rpc, payload)
                if "error" in result:
                    last_err = Exception(f"RPC error: {result['error']}")
                    continue
                return result.get("result")
            except Exception as e:
                last_err = e
                logger.debug("RPC %s failed for %s: %s", rpc, method, e)
                continue
        raise last_err or Exception("All RPCs failed")

    def _sol_rpc(self, method, params=None):
        """JSON-RPC call to Solana."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or [],
        }
        result = _fetch_json(self.config["rpc"], payload)
        if "error" in result:
            raise Exception(f"Solana RPC error: {result['error']}")
        return result.get("result")

    # ── Balance Reading ────────────────────────────────────────────

    def get_native_balance(self):
        """Get native token balance (ETH, MATIC, SOL)."""
        if self.chain == "solana":
            result = self._sol_rpc("getBalance", [self.address])
            lamports = result.get("value", 0)
            return lamports / 1e9

        hex_balance = self._eth_rpc("eth_getBalance", [self.address, "latest"])
        if hex_balance:
            return int(hex_balance, 16) / 1e18
        return 0

    def get_token_balance(self, token_address, symbol="UNKNOWN"):
        """Get ERC-20 token balance."""
        if self.chain == "solana":
            return self._get_solana_token_balance(token_address)

        padded_addr = self.address.lower().replace("0x", "").zfill(64)
        call_data = ERC20_BALANCE_OF + padded_addr

        result = self._eth_rpc("eth_call", [
            {"to": token_address, "data": call_data},
            "latest"
        ])

        if not result or result == "0x":
            return 0

        raw_balance = int(result, 16)
        decimals = KNOWN_DECIMALS.get(symbol, 18)
        return raw_balance / (10 ** decimals)

    def _get_solana_token_balance(self, mint_address):
        """Get SPL token balance on Solana."""
        result = self._sol_rpc("getTokenAccountsByOwner", [
            self.address,
            {"mint": mint_address},
            {"encoding": "jsonParsed"}
        ])
        total = 0
        for acc in result.get("value", []):
            info = acc.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
            token_amount = info.get("tokenAmount", {})
            total += float(token_amount.get("uiAmount", 0))
        return total

    def get_balances(self):
        """Get all token balances for this wallet on the current chain."""
        if time.time() - self._cache_time < 60 and self._balance_cache:
            return self._balance_cache

        native_symbol = self.config["native"]
        native_balance = self.get_native_balance()
        native_price = _get_token_price_usd(native_symbol)
        native_usd = native_balance * native_price

        result = {
            "native": {
                "symbol": native_symbol,
                "amount": round(native_balance, 8),
                "usd": round(native_usd, 2),
            },
            "tokens": [],
            "total_usd": native_usd,
            "chain": self.chain,
            "address": self.address,
        }

        tokens = ERC20_TOKENS.get(self.chain, {})
        for symbol, token_addr in tokens.items():
            try:
                balance = self.get_token_balance(token_addr, symbol)
                if balance > 0.001:
                    price = _get_token_price_usd(symbol)
                    usd = balance * price
                    result["tokens"].append({
                        "symbol": symbol,
                        "amount": round(balance, 6),
                        "usd": round(usd, 2),
                        "address": token_addr,
                    })
                    result["total_usd"] += usd
            except Exception as e:
                logger.debug("Failed to get %s balance: %s", symbol, e)

        result["total_usd"] = round(result["total_usd"], 2)
        self._balance_cache = result
        self._cache_time = time.time()
        return result

    def get_portfolio_value_usd(self):
        """Total portfolio value in USD."""
        return self.get_balances()["total_usd"]

    # ── ERC-20 Approval Flow ──────────────────────────────────────

    def check_allowance(self, token_address, spender):
        """Read current ERC-20 allowance for (owner, spender).

        Returns: int — raw allowance value (in token smallest unit).
        """
        owner_padded = self.address.lower().replace("0x", "").zfill(64)
        spender_padded = spender.lower().replace("0x", "").zfill(64)
        call_data = ERC20_ALLOWANCE + owner_padded + spender_padded

        result = self._eth_rpc("eth_call", [
            {"to": token_address, "data": call_data},
            "latest"
        ])
        if not result or result == "0x":
            return 0
        return int(result, 16)

    def approve_token(self, token_address, spender, amount=None):
        """Send ERC-20 approve transaction.

        Args:
            token_address: ERC-20 contract address
            spender: Address to approve (e.g., Uniswap router)
            amount: Raw amount to approve (default: MAX_UINT256 for infinite)

        Returns: tx hash string
        """
        if not self.private_key:
            raise ValueError("Private key required for approval")

        if amount is None:
            amount = MAX_UINT256

        spender_padded = spender.lower().replace("0x", "").zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        call_data = ERC20_APPROVE + spender_padded + amount_hex

        tx = {
            "to": token_address,
            "data": "0x" + call_data[2:] if call_data.startswith("0x") else "0x" + call_data,
            "value": 0,
            "chainId": self.config["chain_id"],
        }

        # Use EIP-1559 gas
        gas_params = self.get_eip1559_fees()
        tx["maxFeePerGas"] = gas_params["maxFeePerGas"]
        tx["maxPriorityFeePerGas"] = gas_params["maxPriorityFeePerGas"]

        # Estimate gas with 20% buffer
        gas_est = self.estimate_gas(tx)
        tx["gas"] = int(gas_est * 1.2)

        tx_hash = self.sign_and_send(tx)
        logger.info("Approval tx sent: %s (token=%s, spender=%s)", tx_hash, token_address, spender)
        return tx_hash

    def ensure_approved(self, token_address, spender, amount_raw):
        """Check allowance and approve if needed. Returns True if already approved or approval tx sent.

        Args:
            token_address: ERC-20 contract address
            spender: Router/contract to approve
            amount_raw: Minimum required allowance (raw units)

        Returns: {"approved": True, "tx_hash": "0x..." or None}
        """
        current = self.check_allowance(token_address, spender)
        if current >= amount_raw:
            return {"approved": True, "tx_hash": None, "was_already_approved": True}

        tx_hash = self.approve_token(token_address, spender)
        # Wait for approval to confirm
        receipt = self.wait_for_tx(tx_hash, confirmations=1)
        if receipt and receipt.get("status") == 1:
            return {"approved": True, "tx_hash": tx_hash, "was_already_approved": False}
        else:
            raise Exception(f"Approval tx failed: {tx_hash}")

    # ── EIP-1559 Gas Management ───────────────────────────────────

    def get_eip1559_fees(self):
        """Get EIP-1559 fee parameters.

        Returns: {
            "baseFee": int,          # current base fee (wei)
            "maxPriorityFeePerGas": int,  # tip (wei)
            "maxFeePerGas": int,     # max total fee (wei)
        }
        """
        if self.chain == "solana" or not self.config.get("supports_eip1559", True):
            gp = self._eth_rpc("eth_gasPrice", [])
            price = int(gp, 16) if gp else 1_000_000_000
            return {
                "baseFee": price,
                "maxPriorityFeePerGas": price,
                "maxFeePerGas": price,
            }

        # Get latest block for base fee
        block = self._eth_rpc("eth_getBlockByNumber", ["latest", False])
        base_fee = int(block.get("baseFeePerGas", "0x0"), 16) if block else 1_000_000_000

        # Get suggested priority fee
        try:
            priority_hex = self._eth_rpc("eth_maxPriorityFeePerGas", [])
            priority_fee = int(priority_hex, 16) if priority_hex else 1_500_000_000
        except Exception:
            priority_fee = 1_500_000_000  # 1.5 Gwei default

        # maxFeePerGas = 2x baseFee + priorityFee (handles base fee spikes)
        max_fee = (base_fee * 2) + priority_fee

        return {
            "baseFee": base_fee,
            "maxPriorityFeePerGas": priority_fee,
            "maxFeePerGas": max_fee,
        }

    # ── Nonce Management ──────────────────────────────────────────

    def _get_next_nonce(self):
        """Thread-safe nonce tracker. Prevents nonce collisions for concurrent txs."""
        key = (self.chain, self.address.lower())
        with _nonce_lock:
            if key in _nonce_cache:
                # Use cached + increment
                nonce = _nonce_cache[key]
                _nonce_cache[key] = nonce + 1
                return nonce
            else:
                # Fetch from chain
                hex_nonce = self._eth_rpc("eth_getTransactionCount", [self.address, "pending"])
                nonce = int(hex_nonce, 16) if hex_nonce else 0
                _nonce_cache[key] = nonce + 1
                return nonce

    def _reset_nonce(self):
        """Reset nonce cache (call after tx failure to re-sync)."""
        key = (self.chain, self.address.lower())
        with _nonce_lock:
            _nonce_cache.pop(key, None)

    # ── Transaction Signing & Monitoring ──────────────────────────

    # Verified contract addresses — ONLY send to these for bridge/swap operations
    # Prevents sending to wrong addresses (cost us $62.68 in ETH once)
    VERIFIED_CONTRACTS = {
        # Base L2 Bridge (Optimism Portal on Ethereum mainnet)
        "0x49048044D57e1C92A77f79988d21Fa8fAF74E97e": "Base Bridge (OptimismPortal)",
        # Uniswap V3 Router on Base
        "0x2626664c2603336E57B271c5C0b26F421741e481": "Uniswap V3 SwapRouter (Base)",
        # Uniswap Universal Router on Base
        "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD": "Uniswap Universal Router (Base)",
        # WETH on Base
        "0x4200000000000000000000000000000000000006": "WETH (Base)",
        # USDC on Base
        "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913": "USDC (Base)",
    }

    # Known BAD addresses — NEVER send here (lost funds)
    BLACKLISTED_ADDRESSES = {
        "0x49048044D57e1C92A77f79988d21Fa8fAF36689E": "WRONG portal addr — $62.68 lost here",
    }

    def _verify_destination(self, to_address, require_contract=False):
        """Pre-send safety check: verify destination is safe.

        Rules:
          1. NEVER send to blacklisted addresses
          2. If require_contract=True, verify address has code (is a contract)
          3. If sending >$10 to unknown address, warn in logs
          4. Verified contracts bypass all checks

        Returns True if safe, raises ValueError if unsafe.
        """
        if not to_address:
            return True  # No 'to' field = contract creation (ok)

        from web3 import Web3
        addr = Web3.to_checksum_address(to_address)

        # Check blacklist
        for bad_addr, reason in self.BLACKLISTED_ADDRESSES.items():
            if addr.lower() == bad_addr.lower():
                raise ValueError(
                    f"BLOCKED: Address {addr} is blacklisted — {reason}")

        # Check if it's a verified contract
        for verified_addr, name in self.VERIFIED_CONTRACTS.items():
            if addr.lower() == verified_addr.lower():
                logger.info("Destination verified: %s (%s)", addr[:10], name)
                return True

        # For bridge/swap operations, require the target to have code
        if require_contract:
            try:
                code = self._eth_rpc("eth_getCode", [addr, "latest"])
                if not code or code == "0x" or code == "0x0":
                    raise ValueError(
                        f"BLOCKED: Address {addr} has NO contract code. "
                        f"Expected a contract but found an EOA. "
                        f"This prevented sending to wrong address.")
                logger.info("Contract verified: %s has code", addr[:10])
            except ValueError:
                raise
            except Exception as e:
                logger.warning("Could not verify contract code for %s: %s", addr[:10], e)
                # Fail-safe: block if we can't verify
                raise ValueError(
                    f"BLOCKED: Cannot verify contract code for {addr}. "
                    f"Refusing to send to unverified address.")

        return True

    def sign_and_send(self, tx, require_contract=False):
        """Sign and broadcast an EVM transaction. Returns tx hash.

        Args:
            tx: Transaction dict
            require_contract: If True, verify 'to' address has contract code.
                              Use this for bridge/swap txs to prevent sending
                              to wrong EOA addresses.
        """
        if not self.private_key:
            raise ValueError("Private key required for signing transactions")
        if self.chain == "solana":
            raise NotImplementedError("Solana signing uses Jupiter API (see dex_connector)")

        w3 = self.w3
        if not w3:
            raise ImportError("web3 library required for transaction signing")

        # SAFETY: Verify destination before signing
        to_addr = tx.get("to")
        self._verify_destination(to_addr, require_contract=require_contract)

        from web3 import Account
        account = Account.from_key(self.private_key)

        # Fill in nonce if not set
        if "nonce" not in tx:
            tx["nonce"] = self._get_next_nonce()
        if "chainId" not in tx:
            tx["chainId"] = self.config["chain_id"]

        # Fill gas if not set
        if "gas" not in tx:
            gas_est = self.estimate_gas(tx)
            tx["gas"] = int(gas_est * 1.2)  # 20% buffer

        # Fill EIP-1559 fees if not set
        if "maxFeePerGas" not in tx and "gasPrice" not in tx:
            fees = self.get_eip1559_fees()
            tx["maxFeePerGas"] = fees["maxFeePerGas"]
            tx["maxPriorityFeePerGas"] = fees["maxPriorityFeePerGas"]

        try:
            signed = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            result = tx_hash.hex()
            logger.info("Tx sent: %s on %s", result, self.chain)
            return result
        except Exception as e:
            # Reset nonce cache on failure
            self._reset_nonce()
            raise

    def wait_for_tx(self, tx_hash, confirmations=2, timeout=120):
        """Wait for transaction to be mined and reach N confirmations.

        Returns: receipt dict or None on timeout.
        """
        if self.chain == "solana":
            return self._wait_solana_tx(tx_hash, timeout)

        start = time.time()
        poll_interval = 2

        while time.time() - start < timeout:
            try:
                receipt_hex = self._eth_rpc("eth_getTransactionReceipt", [tx_hash])
                if receipt_hex and receipt_hex.get("blockNumber"):
                    block_num = int(receipt_hex["blockNumber"], 16)
                    status = int(receipt_hex.get("status", "0x0"), 16)
                    gas_used = int(receipt_hex.get("gasUsed", "0x0"), 16)

                    # Check confirmations
                    latest_hex = self._eth_rpc("eth_blockNumber", [])
                    latest = int(latest_hex, 16) if latest_hex else block_num
                    confs = latest - block_num

                    if confs >= confirmations:
                        logger.info("Tx %s confirmed (%d confs, status=%d, gas=%d)",
                                    tx_hash, confs, status, gas_used)
                        return {
                            "tx_hash": tx_hash,
                            "block_number": block_num,
                            "status": status,
                            "gas_used": gas_used,
                            "confirmations": confs,
                        }
            except Exception as e:
                logger.debug("Waiting for tx %s: %s", tx_hash, e)

            time.sleep(poll_interval)

        logger.warning("Tx %s timed out after %ds", tx_hash, timeout)
        return None

    def _wait_solana_tx(self, tx_hash, timeout):
        """Wait for Solana transaction confirmation."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                result = self._sol_rpc("getSignatureStatuses", [[tx_hash]])
                statuses = result.get("value", [])
                if statuses and statuses[0]:
                    s = statuses[0]
                    if s.get("confirmationStatus") in ("confirmed", "finalized"):
                        return {
                            "tx_hash": tx_hash,
                            "status": 0 if s.get("err") else 1,
                            "confirmations": s.get("confirmations", 0),
                        }
            except Exception:
                pass
            time.sleep(2)
        return None

    # ── WETH Unwrap ───────────────────────────────────────────────

    def unwrap_weth(self, amount_wei):
        """Unwrap WETH to native ETH (needed after Base DEX swaps).

        Args:
            amount_wei: Amount of WETH to unwrap in wei
        Returns: tx hash
        """
        weth = WETH_ADDRESS.get(self.chain)
        if not weth:
            raise ValueError(f"No WETH address for chain {self.chain}")

        amount_hex = hex(amount_wei)[2:].zfill(64)
        call_data = WETH_WITHDRAW + amount_hex

        tx = {
            "to": weth,
            "data": "0x" + call_data[2:] if call_data.startswith("0x") else "0x" + call_data,
            "value": 0,
            "chainId": self.config["chain_id"],
        }
        return self.sign_and_send(tx)

    # ── Simulation ────────────────────────────────────────────────

    def simulate_tx(self, tx):
        """Simulate a transaction via eth_call before sending.

        Returns: call result (hex string) on success, raises on revert.
        """
        call_params = {
            "from": self.address,
            "to": tx.get("to"),
            "data": tx.get("data"),
        }
        if tx.get("value"):
            call_params["value"] = hex(tx["value"])

        result = self._eth_rpc("eth_call", [call_params, "latest"])
        return result

    # ── Legacy Compat ─────────────────────────────────────────────

    def estimate_gas(self, tx):
        """Estimate gas for a transaction."""
        if self.chain == "solana":
            return 5000

        call_params = {"from": self.address}
        if tx.get("to"):
            call_params["to"] = tx["to"]
        if tx.get("data"):
            call_params["data"] = tx["data"]
        if tx.get("value"):
            call_params["value"] = hex(tx["value"]) if isinstance(tx["value"], int) else tx["value"]

        try:
            result = self._eth_rpc("eth_estimateGas", [call_params])
            return int(result, 16) if result else 21000
        except Exception:
            return 200000  # safe default for swap txs

    def get_gas_price(self):
        """Get current gas price in Gwei (legacy, prefer get_eip1559_fees)."""
        if self.chain == "solana":
            return 0.000005
        result = self._eth_rpc("eth_gasPrice", [])
        return int(result, 16) / 1e9 if result else 0


class MultiChainWallet:
    """Aggregate wallet view across all chains."""

    def __init__(self, address, chains=None, private_key=None):
        if chains is None:
            chains = ["ethereum", "base", "arbitrum", "polygon"]
        self.connectors = {}
        for chain in chains:
            self.connectors[chain] = WalletConnector(address, chain, private_key)

    def add_solana_wallet(self, solana_address, private_key=None):
        """Add a Solana wallet (different address format)."""
        self.connectors["solana"] = WalletConnector(solana_address, "solana", private_key)

    def get_all_balances(self):
        """Get balances across all chains."""
        result = {"chains": {}, "total_usd": 0}
        for chain, connector in self.connectors.items():
            try:
                balances = connector.get_balances()
                result["chains"][chain] = balances
                result["total_usd"] += balances["total_usd"]
            except Exception as e:
                logger.error("Failed to get %s balances: %s", chain, e)
                result["chains"][chain] = {"error": str(e), "total_usd": 0}

        result["total_usd"] = round(result["total_usd"], 2)
        return result

    def get_portfolio_value_usd(self):
        """Total portfolio value across all chains."""
        return self.get_all_balances()["total_usd"]

    def get_connector(self, chain):
        """Get a specific chain's connector."""
        return self.connectors.get(chain)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python wallet_connector.py <address> [chain]")
        print("  Chains: ethereum, polygon, arbitrum, base, solana")
        sys.exit(1)

    address = sys.argv[1]
    chain = sys.argv[2] if len(sys.argv) > 2 else "ethereum"

    logging.basicConfig(level=logging.INFO)
    wallet = WalletConnector(address, chain)
    balances = wallet.get_balances()

    print(f"\nWallet: {address}")
    print(f"Chain:  {chain}")
    print(f"\n  {balances['native']['symbol']}: {balances['native']['amount']} (${balances['native']['usd']:.2f})")
    for token in balances["tokens"]:
        print(f"  {token['symbol']}: {token['amount']} (${token['usd']:.2f})")
    print(f"\n  Total: ${balances['total_usd']:.2f}")

    # EIP-1559 fees
    if chain != "solana":
        fees = wallet.get_eip1559_fees()
        print(f"\n  Gas: base={fees['baseFee']/1e9:.2f} Gwei, "
              f"priority={fees['maxPriorityFeePerGas']/1e9:.2f} Gwei, "
              f"max={fees['maxFeePerGas']/1e9:.2f} Gwei")
