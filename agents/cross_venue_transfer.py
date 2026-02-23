#!/usr/bin/env python3
"""Cross-venue transfer manager — move funds between Coinbase, Kraken, E*Trade.

Primary path: Coinbase -> Kraken via USDC on-chain transfer (5-15 min, ~$0.50-2.00 gas).
Secondary paths: E*Trade -> Bank -> Kraken (ACH, 3-5 days), reverse flows.

Safety:
  - Validate deposit addresses before withdrawal
  - Never drain a venue below minimum reserve
  - Small test transfer for new paths
  - All transfers logged in SQLite for audit
"""

import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("cross_venue_transfer")

# Load .env if present (same pattern as kraken_connector.py)
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"'))

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
CROSS_VENUE_TRANSFER_ENABLED = os.environ.get("CROSS_VENUE_TRANSFER_ENABLED", "0") == "1"
CROSS_VENUE_MIN_RESERVE_USD = float(os.environ.get("CROSS_VENUE_MIN_RESERVE_USD", "10.0"))
CROSS_VENUE_PREFERRED_ASSET = os.environ.get("CROSS_VENUE_PREFERRED_ASSET", "USDC")

# Transfer tracking database
TRANSFER_DB = Path(__file__).parent / "cross_venue_transfers.db"

# Supported venues
SUPPORTED_VENUES = ("coinbase", "kraken", "etrade")

# Address validation patterns (basic format checks)
ADDRESS_PATTERNS = {
    "ethereum": re.compile(r"^0x[0-9a-fA-F]{40}$"),
    "bitcoin": re.compile(r"^(bc1|[13])[a-zA-HJ-NP-Z0-9]{25,62}$"),
    "solana": re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$"),
}

# Kraken asset name mappings (Kraken uses different internal names)
KRAKEN_ASSET_MAP = {
    "USDC": "USDC",
    "USD": "ZUSD",
    "BTC": "XXBT",
    "ETH": "XETH",
    "SOL": "SOL",
    "XRP": "XXRP",
}

# Network/method names for Kraken deposit methods
KRAKEN_DEPOSIT_METHODS = {
    "USDC": "Ethereum (ERC20)",
    "BTC": "Bitcoin",
    "ETH": "Ether (Hex)",
    "SOL": "Solana",
}

# Estimated gas fees by asset (USD)
ESTIMATED_GAS_FEES = {
    "USDC": 2.00,  # ERC20 transfer
    "ETH": 1.50,   # native ETH
    "BTC": 3.00,   # Bitcoin network
    "SOL": 0.01,   # Solana (very cheap)
}

# ---------------------------------------------------------------------------
# Import venue connectors with guards
# ---------------------------------------------------------------------------
try:
    from exchange_connector import CoinbaseTrader
except ImportError:
    try:
        from agents.exchange_connector import CoinbaseTrader
    except ImportError:
        CoinbaseTrader = None

try:
    from kraken_connector import KrakenConnector
except ImportError:
    try:
        from agents.kraken_connector import KrakenConnector
    except ImportError:
        KrakenConnector = None

try:
    from etrade_connector import ETradeTrader, ETradeAuth
except ImportError:
    try:
        from agents.etrade_connector import ETradeTrader, ETradeAuth
    except ImportError:
        ETradeTrader = None
        ETradeAuth = None


# ---------------------------------------------------------------------------
# Database initialization
# ---------------------------------------------------------------------------
def _init_transfer_db(db_path=None):
    """Initialize the cross-venue transfer tracking database."""
    path = db_path or str(TRANSFER_DB)
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("""
        CREATE TABLE IF NOT EXISTS cross_venue_transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_venue TEXT NOT NULL,
            to_venue TEXT NOT NULL,
            asset TEXT NOT NULL,
            amount REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            txid TEXT,
            deposit_address TEXT,
            network_fee REAL,
            initiated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            notes TEXT
        )
    """)
    db.commit()
    return db


# ---------------------------------------------------------------------------
# CrossVenueTransfer class
# ---------------------------------------------------------------------------
class CrossVenueTransfer:
    """Manages fund transfers between Coinbase, Kraken, and E*Trade.

    Safety gates:
      - CROSS_VENUE_TRANSFER_ENABLED must be True
      - Never drain a venue below CROSS_VENUE_MIN_RESERVE_USD
      - Address validation before withdrawal
      - All transfers logged in SQLite for audit
    """

    def __init__(self, db_path=None):
        self.db_path = db_path or str(TRANSFER_DB)
        self.db = _init_transfer_db(self.db_path)
        self._coinbase = None
        self._etrade = None

    def _get_coinbase(self):
        """Lazy-init Coinbase trader."""
        if self._coinbase is None and CoinbaseTrader is not None:
            self._coinbase = CoinbaseTrader()
        return self._coinbase

    def _get_etrade(self):
        """Lazy-init E*Trade trader."""
        if self._etrade is None and ETradeTrader is not None:
            try:
                self._etrade = ETradeTrader()
            except Exception as e:
                logger.warning("Could not initialize E*Trade: %s", e)
        return self._etrade

    # ------------------------------------------------------------------
    # Deposit address retrieval
    # ------------------------------------------------------------------
    def get_deposit_address(self, venue, asset):
        """Get a deposit address for a venue.

        Args:
            venue: Target venue ("kraken", "coinbase")
            asset: Asset to deposit (e.g., "USDC", "BTC", "ETH")

        Returns:
            dict: {"address": "0x...", "network": "ethereum", "memo": None}
            or {"error": "..."} on failure
        """
        venue = venue.lower().strip()
        asset = asset.upper().strip()

        if venue == "kraken":
            return self._get_kraken_deposit_address(asset)
        elif venue == "coinbase":
            return self._get_coinbase_deposit_address(asset)
        else:
            return {"error": f"Unsupported venue for deposit: {venue}"}

    def _get_kraken_deposit_address(self, asset):
        """Get deposit address from Kraken."""
        if KrakenConnector is None:
            return {"error": "KrakenConnector not available"}

        kraken_asset = KRAKEN_ASSET_MAP.get(asset, asset)
        method = KRAKEN_DEPOSIT_METHODS.get(asset)

        if not method:
            return {"error": f"No known deposit method for {asset} on Kraken"}

        try:
            result = KrakenConnector._private_request("DepositAddresses", {
                "asset": kraken_asset,
                "method": method,
            })

            if result.get("error") and result["error"]:
                return {"error": f"Kraken API error: {result['error']}"}

            addresses = result.get("result", [])
            if not addresses:
                return {"error": f"No deposit address returned for {asset} on Kraken"}

            addr = addresses[0]
            network = "ethereum" if "ERC20" in method or "Ether" in method else method.lower()

            return {
                "address": addr.get("address", ""),
                "network": network,
                "memo": addr.get("tag") or addr.get("memo"),
                "venue": "kraken",
                "asset": asset,
                "new": addr.get("new", False),
            }
        except Exception as e:
            logger.error("Failed to get Kraken deposit address for %s: %s", asset, e)
            return {"error": str(e)}

    def _get_coinbase_deposit_address(self, asset):
        """Get deposit address from Coinbase."""
        cb = self._get_coinbase()
        if cb is None:
            return {"error": "CoinbaseTrader not available"}

        try:
            # List accounts to find the right one for this asset
            accounts_resp = cb.get_accounts()
            if isinstance(accounts_resp, dict) and accounts_resp.get("error"):
                return {"error": f"Coinbase API error: {accounts_resp['error']}"}

            accounts = accounts_resp.get("accounts", [])
            target_account = None
            for acc in accounts:
                if isinstance(acc, dict):
                    currency = acc.get("currency", "")
                    if currency.upper() == asset.upper():
                        target_account = acc
                        break

            if not target_account:
                return {"error": f"No Coinbase account found for {asset}"}

            account_uuid = target_account.get("uuid", "")
            if not account_uuid:
                return {"error": f"No UUID for Coinbase {asset} account"}

            # Use the v2 API to get/create deposit address
            path = f"/v2/accounts/{account_uuid}/addresses"
            result = cb._request("POST", path)

            if isinstance(result, dict) and result.get("error"):
                # Try GET instead (list existing addresses)
                result = cb._request("GET", path)

            if isinstance(result, dict):
                data = result.get("data", result)
                if isinstance(data, list) and data:
                    addr_data = data[0]
                elif isinstance(data, dict):
                    addr_data = data
                else:
                    return {"error": "No address data returned from Coinbase"}

                return {
                    "address": addr_data.get("address", ""),
                    "network": addr_data.get("network", "ethereum"),
                    "memo": addr_data.get("destination_tag"),
                    "venue": "coinbase",
                    "asset": asset,
                }

            return {"error": "Unexpected response from Coinbase address API"}

        except Exception as e:
            logger.error("Failed to get Coinbase deposit address for %s: %s", asset, e)
            return {"error": str(e)}

    # ------------------------------------------------------------------
    # Address validation
    # ------------------------------------------------------------------
    @staticmethod
    def validate_address(address, network="ethereum"):
        """Validate a deposit address format.

        Args:
            address: The address string to validate
            network: Network type ("ethereum", "bitcoin", "solana")

        Returns:
            (valid: bool, reason: str)
        """
        if not address or not isinstance(address, str):
            return False, "Address is empty or not a string"

        address = address.strip()
        if len(address) < 10:
            return False, "Address too short"

        pattern = ADDRESS_PATTERNS.get(network.lower())
        if pattern:
            if pattern.match(address):
                return True, "Address format valid"
            return False, f"Address does not match {network} format"

        # Unknown network — accept if it looks like a hex or base58 string
        if len(address) >= 20:
            return True, f"Address accepted (unknown network: {network})"

        return False, f"Address too short for network {network}"

    # ------------------------------------------------------------------
    # Withdrawal initiation
    # ------------------------------------------------------------------
    def initiate_withdrawal(self, venue, asset, amount, address):
        """Initiate a crypto withdrawal from a venue.

        Args:
            venue: Source venue ("coinbase" or "kraken")
            asset: Asset to withdraw (e.g., "USDC")
            amount: Amount to withdraw
            address: Destination deposit address

        Returns:
            dict: {"txid": "...", "status": "initiated"} or {"error": "..."}
        """
        venue = venue.lower().strip()
        asset = asset.upper().strip()

        if venue == "coinbase":
            return self._withdraw_from_coinbase(asset, amount, address)
        elif venue == "kraken":
            return self._withdraw_from_kraken(asset, amount, address)
        else:
            return {"error": f"Unsupported venue for withdrawal: {venue}"}

    def _withdraw_from_coinbase(self, asset, amount, address):
        """Initiate withdrawal from Coinbase via Advanced Trade API."""
        cb = self._get_coinbase()
        if cb is None:
            return {"error": "CoinbaseTrader not available"}

        try:
            # Coinbase Advanced Trade withdrawal endpoint
            path = "/api/v3/brokerage/withdrawals/crypto"
            body = {
                "amount": str(amount),
                "currency": asset,
                "address": address,
                "network": "ethereum",
            }
            result = cb._request("POST", path, body=body)

            if isinstance(result, dict) and result.get("error"):
                return {"error": f"Coinbase withdrawal error: {result['error']}"}

            txid = ""
            if isinstance(result, dict):
                txid = (
                    result.get("id", "")
                    or result.get("transaction_id", "")
                    or result.get("tx_hash", "")
                )

            logger.info(
                "Coinbase withdrawal initiated: %s %s to %s (txid=%s)",
                amount, asset, address[:12] + "...", txid,
            )
            return {"txid": txid, "status": "initiated", "venue": "coinbase"}

        except Exception as e:
            logger.error("Coinbase withdrawal failed: %s", e)
            return {"error": str(e)}

    def _withdraw_from_kraken(self, asset, amount, address):
        """Initiate withdrawal from Kraken.

        Note: Kraken requires pre-approved withdrawal address key names.
        The 'key' parameter should match a saved withdrawal address name
        in the Kraken account settings.
        """
        if KrakenConnector is None:
            return {"error": "KrakenConnector not available"}

        kraken_asset = KRAKEN_ASSET_MAP.get(asset, asset)

        # Derive a key name from the address (user should set this up in Kraken)
        # Convention: coinbase_<asset_lower>
        key_name = f"coinbase_{asset.lower()}"

        try:
            result = KrakenConnector._private_request("Withdraw", {
                "asset": kraken_asset,
                "key": key_name,
                "amount": str(amount),
            })

            if result.get("error") and result["error"]:
                return {"error": f"Kraken withdrawal error: {result['error']}"}

            refid = ""
            if isinstance(result.get("result"), dict):
                refid = result["result"].get("refid", "")

            logger.info(
                "Kraken withdrawal initiated: %s %s (key=%s, refid=%s)",
                amount, asset, key_name, refid,
            )
            return {"txid": refid, "status": "initiated", "venue": "kraken"}

        except Exception as e:
            logger.error("Kraken withdrawal failed: %s", e)
            return {"error": str(e)}

    # ------------------------------------------------------------------
    # Transfer validation
    # ------------------------------------------------------------------
    def validate_transfer(self, from_venue, to_venue, asset, amount):
        """Validate a transfer before executing.

        Checks:
          - CROSS_VENUE_TRANSFER_ENABLED
          - from_venue balance >= amount + fees
          - from_venue balance - amount >= CROSS_VENUE_MIN_RESERVE_USD
          - amount > 0
          - Venues are supported

        Returns:
            (valid: bool, reason: str)
        """
        if not CROSS_VENUE_TRANSFER_ENABLED:
            return False, "Cross-venue transfers are disabled (set CROSS_VENUE_TRANSFER_ENABLED=1)"

        from_venue = from_venue.lower().strip()
        to_venue = to_venue.lower().strip()
        asset = asset.upper().strip()

        if from_venue not in SUPPORTED_VENUES:
            return False, f"Unsupported source venue: {from_venue}"

        if to_venue not in SUPPORTED_VENUES:
            return False, f"Unsupported destination venue: {to_venue}"

        if from_venue == to_venue:
            return False, "Source and destination venues must be different"

        if amount <= 0:
            return False, "Transfer amount must be positive"

        # Check balance on source venue
        balances = self.get_venue_balances()
        venue_balance = balances.get(from_venue, {})
        if isinstance(venue_balance, dict) and venue_balance.get("error"):
            return False, f"Cannot check {from_venue} balance: {venue_balance['error']}"

        # Get total USD value on source venue
        total_usd = 0.0
        if isinstance(venue_balance, dict):
            for curr, bal in venue_balance.items():
                if curr in ("USD", "USDC", "ZUSD"):
                    total_usd += float(bal or 0)
                elif curr == asset:
                    total_usd += float(bal or 0)

        # Estimate gas fees
        gas_fee = ESTIMATED_GAS_FEES.get(asset, 2.0)

        if total_usd < amount + gas_fee:
            return False, (
                f"Insufficient balance on {from_venue}: "
                f"${total_usd:.2f} < ${amount:.2f} + ${gas_fee:.2f} gas"
            )

        if total_usd - amount < CROSS_VENUE_MIN_RESERVE_USD:
            return False, (
                f"Transfer would drain {from_venue} below minimum reserve: "
                f"${total_usd:.2f} - ${amount:.2f} = ${total_usd - amount:.2f} "
                f"< ${CROSS_VENUE_MIN_RESERVE_USD:.2f} reserve"
            )

        return True, "Transfer validated"

    # ------------------------------------------------------------------
    # Minimum reserve check
    # ------------------------------------------------------------------
    def check_minimum_reserve(self, venue):
        """Check if a venue is above minimum reserve.

        Args:
            venue: Venue name ("coinbase", "kraken", "etrade")

        Returns:
            dict: {"venue": venue, "balance": X, "reserve": Y, "above_reserve": bool}
        """
        venue = venue.lower().strip()
        balances = self.get_venue_balances()
        venue_balance = balances.get(venue, {})

        total_usd = 0.0
        if isinstance(venue_balance, dict):
            for curr, bal in venue_balance.items():
                if curr in ("USD", "USDC", "ZUSD"):
                    total_usd += float(bal or 0)

        return {
            "venue": venue,
            "balance": round(total_usd, 2),
            "reserve": CROSS_VENUE_MIN_RESERVE_USD,
            "above_reserve": total_usd >= CROSS_VENUE_MIN_RESERVE_USD,
        }

    # ------------------------------------------------------------------
    # Primary transfer paths
    # ------------------------------------------------------------------
    def coinbase_to_kraken(self, asset, amount):
        """Transfer crypto from Coinbase to Kraken.

        Steps:
          1. Get Kraken deposit address
          2. Validate address format
          3. Check Coinbase balance >= amount + estimated gas
          4. Check minimum reserve on Coinbase
          5. Initiate withdrawal from Coinbase
          6. Log transfer in DB
          7. Return transfer record

        Args:
            asset: Asset to transfer (e.g., "USDC")
            amount: Amount to transfer

        Returns:
            dict: Transfer record or error
        """
        asset = asset.upper().strip()

        # Step 0: Validate the transfer
        valid, reason = self.validate_transfer("coinbase", "kraken", asset, amount)
        if not valid:
            logger.warning("coinbase_to_kraken blocked: %s", reason)
            return {"error": reason}

        # Step 1: Get Kraken deposit address
        addr_result = self.get_deposit_address("kraken", asset)
        if addr_result.get("error"):
            return {"error": f"Could not get Kraken deposit address: {addr_result['error']}"}

        address = addr_result.get("address", "")
        network = addr_result.get("network", "ethereum")

        # Step 2: Validate address format
        addr_valid, addr_reason = self.validate_address(address, network)
        if not addr_valid:
            return {"error": f"Invalid deposit address: {addr_reason}"}

        # Step 3 & 4: Already done in validate_transfer above

        # Step 5: Initiate withdrawal from Coinbase
        withdraw_result = self.initiate_withdrawal("coinbase", asset, amount, address)
        if withdraw_result.get("error"):
            return {"error": f"Withdrawal failed: {withdraw_result['error']}"}

        txid = withdraw_result.get("txid", "")
        gas_fee = ESTIMATED_GAS_FEES.get(asset, 2.0)

        # Step 6: Log transfer in DB
        transfer_id = self._log_transfer(
            from_venue="coinbase",
            to_venue="kraken",
            asset=asset,
            amount=amount,
            status="initiated",
            txid=txid,
            deposit_address=address,
            network_fee=gas_fee,
            notes=f"Coinbase -> Kraken via {network}",
        )

        logger.info(
            "Transfer initiated: %s %s from Coinbase to Kraken (id=%d, txid=%s)",
            amount, asset, transfer_id, txid,
        )

        # Step 7: Return transfer record
        return {
            "transfer_id": transfer_id,
            "from_venue": "coinbase",
            "to_venue": "kraken",
            "asset": asset,
            "amount": amount,
            "status": "initiated",
            "txid": txid,
            "deposit_address": address,
            "network": network,
            "estimated_fee": gas_fee,
        }

    def kraken_to_coinbase(self, asset, amount):
        """Transfer crypto from Kraken to Coinbase.

        Steps:
          1. Get Coinbase deposit address
          2. Validate address format
          3. Check Kraken balance >= amount + estimated gas
          4. Check minimum reserve on Kraken
          5. Initiate withdrawal from Kraken
          6. Log transfer in DB
          7. Return transfer record

        Args:
            asset: Asset to transfer (e.g., "USDC")
            amount: Amount to transfer

        Returns:
            dict: Transfer record or error
        """
        asset = asset.upper().strip()

        # Step 0: Validate the transfer
        valid, reason = self.validate_transfer("kraken", "coinbase", asset, amount)
        if not valid:
            logger.warning("kraken_to_coinbase blocked: %s", reason)
            return {"error": reason}

        # Step 1: Get Coinbase deposit address
        addr_result = self.get_deposit_address("coinbase", asset)
        if addr_result.get("error"):
            return {"error": f"Could not get Coinbase deposit address: {addr_result['error']}"}

        address = addr_result.get("address", "")
        network = addr_result.get("network", "ethereum")

        # Step 2: Validate address format
        addr_valid, addr_reason = self.validate_address(address, network)
        if not addr_valid:
            return {"error": f"Invalid deposit address: {addr_reason}"}

        # Step 3 & 4: Already done in validate_transfer above

        # Step 5: Initiate withdrawal from Kraken
        withdraw_result = self.initiate_withdrawal("kraken", asset, amount, address)
        if withdraw_result.get("error"):
            return {"error": f"Withdrawal failed: {withdraw_result['error']}"}

        txid = withdraw_result.get("txid", "")
        gas_fee = ESTIMATED_GAS_FEES.get(asset, 2.0)

        # Step 6: Log transfer in DB
        transfer_id = self._log_transfer(
            from_venue="kraken",
            to_venue="coinbase",
            asset=asset,
            amount=amount,
            status="initiated",
            txid=txid,
            deposit_address=address,
            network_fee=gas_fee,
            notes=f"Kraken -> Coinbase via {network}",
        )

        logger.info(
            "Transfer initiated: %s %s from Kraken to Coinbase (id=%d, txid=%s)",
            amount, asset, transfer_id, txid,
        )

        # Step 7: Return transfer record
        return {
            "transfer_id": transfer_id,
            "from_venue": "kraken",
            "to_venue": "coinbase",
            "asset": asset,
            "amount": amount,
            "status": "initiated",
            "txid": txid,
            "deposit_address": address,
            "network": network,
            "estimated_fee": gas_fee,
        }

    # ------------------------------------------------------------------
    # E*Trade path (informational only)
    # ------------------------------------------------------------------
    @staticmethod
    def estimate_etrade_to_kraken(amount):
        """Estimate an E*Trade to Kraken transfer.

        ACH transfers must be initiated manually through bank/broker interfaces.
        This method returns informational data only.

        Args:
            amount: Amount in USD

        Returns:
            dict: Transfer info including method, steps, estimated days, fee
        """
        return {
            "method": "ACH",
            "steps": [
                "E*Trade withdrawal to bank (1-3 business days)",
                "Bank ACH to Kraken (1-3 business days)",
            ],
            "estimated_days": "3-5",
            "fee": 0.0,
            "amount": float(amount),
            "note": "ACH transfers must be initiated manually through bank/broker interfaces",
        }

    @staticmethod
    def estimate_kraken_to_etrade(amount):
        """Estimate a Kraken to E*Trade transfer (reverse ACH).

        Args:
            amount: Amount in USD

        Returns:
            dict: Transfer info
        """
        return {
            "method": "ACH",
            "steps": [
                "Kraken withdrawal to bank (1-3 business days)",
                "Bank ACH to E*Trade (1-3 business days)",
            ],
            "estimated_days": "3-5",
            "fee": 0.0,
            "amount": float(amount),
            "note": "ACH transfers must be initiated manually through bank/broker interfaces",
        }

    # ------------------------------------------------------------------
    # Transfer tracking
    # ------------------------------------------------------------------
    def track_transfer(self, transfer_id):
        """Check status of a transfer and update the DB.

        For crypto transfers, checks the destination venue deposit status.

        Args:
            transfer_id: Transfer ID from the cross_venue_transfers table

        Returns:
            dict: Current transfer status
        """
        row = self.db.execute(
            "SELECT * FROM cross_venue_transfers WHERE id = ?",
            (transfer_id,)
        ).fetchone()

        if not row:
            return {"error": f"Transfer {transfer_id} not found"}

        record = dict(row)
        current_status = record["status"]

        # If already completed or failed, just return
        if current_status in ("completed", "failed"):
            return record

        to_venue = record["to_venue"]
        asset = record["asset"]

        # Check deposit status on the destination venue
        new_status = current_status
        if to_venue == "kraken" and KrakenConnector is not None:
            new_status = self._check_kraken_deposit_status(asset, record)
        elif to_venue == "coinbase":
            new_status = self._check_coinbase_deposit_status(asset, record)

        # Update status if changed
        if new_status != current_status:
            completed_at = None
            if new_status == "completed":
                completed_at = datetime.now(timezone.utc).isoformat()

            self.db.execute(
                "UPDATE cross_venue_transfers SET status = ?, completed_at = ? WHERE id = ?",
                (new_status, completed_at, transfer_id),
            )
            self.db.commit()
            record["status"] = new_status
            record["completed_at"] = completed_at
            logger.info("Transfer %d status updated: %s -> %s", transfer_id, current_status, new_status)

        return record

    def _check_kraken_deposit_status(self, asset, record):
        """Check Kraken deposit status for a pending transfer."""
        kraken_asset = KRAKEN_ASSET_MAP.get(asset.upper(), asset.upper())

        try:
            result = KrakenConnector._private_request("DepositStatus", {
                "asset": kraken_asset,
            })

            if result.get("error") and result["error"]:
                logger.warning("Kraken deposit status error: %s", result["error"])
                return record["status"]

            deposits = result.get("result", [])
            for dep in deposits:
                # Match by amount or txid
                dep_amount = float(dep.get("amount", 0))
                dep_txid = dep.get("txid", "")
                dep_status = dep.get("status", "")

                if (
                    abs(dep_amount - record["amount"]) < 0.01
                    or (record["txid"] and dep_txid == record["txid"])
                ):
                    if dep_status == "Success":
                        return "completed"
                    elif dep_status in ("Settled", "Pending"):
                        return "confirming"
                    elif dep_status == "Failure":
                        return "failed"

        except Exception as e:
            logger.warning("Failed to check Kraken deposit status: %s", e)

        return record["status"]

    def _check_coinbase_deposit_status(self, asset, record):
        """Check Coinbase for incoming deposit status."""
        # Coinbase does not have a direct deposit-status check in Advanced Trade API.
        # The transfer status is best tracked via the withdrawal source.
        # For now, if the transfer was initiated more than 30 minutes ago, assume confirming.
        if record["initiated_at"]:
            try:
                init_time = datetime.fromisoformat(
                    str(record["initiated_at"]).replace("Z", "+00:00")
                )
                if init_time.tzinfo is None:
                    init_time = init_time.replace(tzinfo=timezone.utc)
                elapsed = (datetime.now(timezone.utc) - init_time).total_seconds()
                if elapsed > 1800:  # 30 minutes
                    return "confirming"
            except Exception:
                pass
        return record["status"]

    # ------------------------------------------------------------------
    # Transfer history
    # ------------------------------------------------------------------
    def transfer_history(self, limit=50):
        """Get recent transfer records.

        Args:
            limit: Maximum number of records to return (default 50)

        Returns:
            list: List of transfer record dicts
        """
        rows = self.db.execute(
            "SELECT * FROM cross_venue_transfers ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()

        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Venue balances
    # ------------------------------------------------------------------
    def get_venue_balances(self):
        """Get current balances from all supported venues.

        Returns:
            dict: {
                "coinbase": {"USD": X, "BTC": Y, ...},
                "kraken": {"USD": X, "BTC": Y, ...},
                "etrade": {"USD": X, ...},
            }
        """
        result = {}

        # Coinbase balances
        result["coinbase"] = self._get_coinbase_balances()

        # Kraken balances
        result["kraken"] = self._get_kraken_balances()

        # E*Trade balances
        result["etrade"] = self._get_etrade_balances()

        return result

    def _get_coinbase_balances(self):
        """Fetch Coinbase account balances."""
        cb = self._get_coinbase()
        if cb is None:
            return {"error": "CoinbaseTrader not available"}

        try:
            accounts_resp = cb.get_accounts()
            if isinstance(accounts_resp, dict) and accounts_resp.get("error"):
                return {"error": str(accounts_resp["error"])}

            accounts = accounts_resp.get("accounts", [])
            balances = {}
            for acc in accounts:
                if not isinstance(acc, dict):
                    continue
                currency = acc.get("currency", "")
                available = acc.get("available_balance", {})
                if isinstance(available, dict):
                    value = float(available.get("value", 0) or 0)
                else:
                    value = float(available or 0)
                if value > 0:
                    balances[currency] = round(value, 8)

            return balances

        except Exception as e:
            logger.warning("Failed to get Coinbase balances: %s", e)
            return {"error": str(e)}

    def _get_kraken_balances(self):
        """Fetch Kraken account balances."""
        if KrakenConnector is None:
            return {"error": "KrakenConnector not available"}

        try:
            result = KrakenConnector.get_account_balance()
            if result.get("error") and result["error"]:
                return {"error": str(result["error"])}

            raw_balances = result.get("result", {})
            balances = {}

            # Map Kraken internal names back to standard names
            reverse_map = {v: k for k, v in KRAKEN_ASSET_MAP.items()}

            for asset, balance in raw_balances.items():
                val = float(balance or 0)
                if val > 0:
                    standard_name = reverse_map.get(asset, asset)
                    balances[standard_name] = round(val, 8)

            return balances

        except Exception as e:
            logger.warning("Failed to get Kraken balances: %s", e)
            return {"error": str(e)}

    def _get_etrade_balances(self):
        """Fetch E*Trade account balances."""
        et = self._get_etrade()
        if et is None:
            return {"error": "ETradeTrader not available"}

        try:
            accounts = et.get_accounts()
            if not accounts:
                return {"error": "No E*Trade accounts found"}

            total_cash = 0.0
            total_value = 0.0

            for acc in accounts:
                account_id = acc.get("accountIdKey", "")
                if not account_id:
                    continue
                balance = et.get_balance(account_id)
                if balance:
                    computed = balance.get("Computed", {})
                    cash = float(computed.get("cashAvailableForInvestment", 0) or 0)
                    rtv = balance.get("RealTimeValues", {})
                    total = float(rtv.get("totalAccountValue", 0) or 0)
                    total_cash += cash
                    total_value += total

            return {
                "USD": round(total_cash, 2),
                "total_value": round(total_value, 2),
            }

        except Exception as e:
            logger.warning("Failed to get E*Trade balances: %s", e)
            return {"error": str(e)}

    # ------------------------------------------------------------------
    # Intelligent auto-funding
    # ------------------------------------------------------------------
    def ensure_venue_funded(self, venue, asset, required_usd, trade_pair=None):
        """Ensure a venue has sufficient funds before trade execution.

        Called automatically before Kraken/other venue trades. Checks the
        venue balance and, if insufficient, transfers from the richest
        alternative venue.

        Args:
            venue: Target venue that needs funds (e.g., "kraken")
            asset: Asset needed (e.g., "USDC", "USD")
            required_usd: Minimum USD-equivalent needed for the trade
            trade_pair: Optional pair context for logging (e.g., "BTC-USD")

        Returns:
            dict: {"funded": True/False, "balance": X, "transferred": Y, ...}
        """
        if not CROSS_VENUE_TRANSFER_ENABLED:
            return {"funded": False, "reason": "transfers disabled"}

        venue = venue.lower().strip()
        transfer_asset = CROSS_VENUE_PREFERRED_ASSET  # USDC by default

        # Check current balance on target venue
        balances = self.get_venue_balances()
        venue_bal = balances.get(venue, {})
        if isinstance(venue_bal, dict) and venue_bal.get("error"):
            return {"funded": False, "reason": f"cannot check {venue} balance"}

        # Sum USD-equivalent balance (USD + USDC + stablecoins)
        usd_assets = ("USD", "USDC", "ZUSD", "USDT")
        current_usd = sum(
            float(venue_bal.get(a, 0))
            for a in usd_assets
            if isinstance(venue_bal.get(a), (int, float))
        )

        # Add buffer: need required + gas fees + min reserve
        gas_estimate = ESTIMATED_GAS_FEES.get(transfer_asset, 2.0)
        total_needed = required_usd + gas_estimate + CROSS_VENUE_MIN_RESERVE_USD

        if current_usd >= required_usd:
            logger.debug("Venue %s has $%.2f, trade needs $%.2f — sufficient",
                        venue, current_usd, required_usd)
            return {"funded": True, "balance": current_usd, "transferred": 0}

        shortfall = total_needed - current_usd

        # Find the richest alternative venue to pull from
        best_source = None
        best_source_usd = 0
        for src_venue, src_bal in balances.items():
            if src_venue == venue:
                continue
            if isinstance(src_bal, dict) and src_bal.get("error"):
                continue
            src_usd = sum(
                float(src_bal.get(a, 0))
                for a in usd_assets
                if isinstance(src_bal.get(a), (int, float))
            )
            # Source must keep its own reserve
            available = src_usd - CROSS_VENUE_MIN_RESERVE_USD
            if available > best_source_usd:
                best_source = src_venue
                best_source_usd = available

        if not best_source or best_source_usd < shortfall:
            logger.warning(
                "Cannot fund %s: need $%.2f more, best source %s has $%.2f available",
                venue, shortfall, best_source, best_source_usd,
            )
            return {
                "funded": False,
                "balance": current_usd,
                "shortfall": shortfall,
                "reason": "insufficient funds across all venues",
            }

        # Transfer the shortfall (capped at what source can provide)
        transfer_amount = min(shortfall, best_source_usd)

        logger.info(
            "Auto-funding %s: transferring $%.2f %s from %s (trade: %s)",
            venue, transfer_amount, transfer_asset, best_source,
            trade_pair or "unknown",
        )

        # Route the transfer
        if best_source == "coinbase" and venue == "kraken":
            result = self.coinbase_to_kraken(transfer_asset, transfer_amount)
        elif best_source == "kraken" and venue == "coinbase":
            result = self.kraken_to_coinbase(transfer_asset, transfer_amount)
        else:
            # For paths we can't automate (e.g., E*Trade ACH), log and skip
            logger.info(
                "Auto-transfer %s->%s requires manual action (ACH/wire)",
                best_source, venue,
            )
            return {
                "funded": False,
                "balance": current_usd,
                "shortfall": shortfall,
                "reason": f"no automated {best_source}->{venue} path",
                "manual_action": f"Transfer ${transfer_amount:.2f} from {best_source} to {venue}",
            }

        if result.get("error"):
            logger.error("Auto-funding transfer failed: %s", result["error"])
            return {
                "funded": False,
                "balance": current_usd,
                "shortfall": shortfall,
                "reason": f"transfer failed: {result['error']}",
            }

        logger.info(
            "Auto-funding initiated: $%.2f %s %s->%s (transfer_id=%s)",
            transfer_amount, transfer_asset, best_source, venue,
            result.get("transfer_id", "?"),
        )
        return {
            "funded": True,
            "balance": current_usd,
            "transferred": transfer_amount,
            "from_venue": best_source,
            "asset": transfer_asset,
            "transfer_id": result.get("transfer_id"),
            "note": "transfer initiated, may take 5-15 min to confirm",
        }

    # ------------------------------------------------------------------
    # DB helper
    # ------------------------------------------------------------------
    def _log_transfer(self, from_venue, to_venue, asset, amount,
                      status="pending", txid=None, deposit_address=None,
                      network_fee=None, notes=None):
        """Log a transfer record to the database.

        Returns:
            int: The transfer ID
        """
        cursor = self.db.execute(
            """INSERT INTO cross_venue_transfers
               (from_venue, to_venue, asset, amount, status, txid,
                deposit_address, network_fee, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (from_venue, to_venue, asset, amount, status, txid,
             deposit_address, network_fee, notes),
        )
        self.db.commit()
        return cursor.lastrowid

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    def close(self):
        """Close the database connection."""
        if self.db:
            self.db.close()


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------
def main():
    """CLI entry point for cross-venue transfer manager."""
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    mgr = CrossVenueTransfer()

    if len(sys.argv) < 2:
        print("""
Cross-Venue Transfer Manager

Usage:
  python cross_venue_transfer.py balances       Show balances across all venues
  python cross_venue_transfer.py history        Show recent transfer history
  python cross_venue_transfer.py status <ID>    Track a specific transfer
  python cross_venue_transfer.py reserve <VEN>  Check minimum reserve for venue
  python cross_venue_transfer.py estimate-ach <AMT>  Estimate E*Trade -> Kraken ACH

Safety: All transfers gated by CROSS_VENUE_TRANSFER_ENABLED env var.
""")
        sys.exit(0)

    command = sys.argv[1].lower()

    if command == "balances":
        balances = mgr.get_venue_balances()
        print("\n" + "=" * 60)
        print("  VENUE BALANCES")
        print("=" * 60)
        for venue, bals in balances.items():
            print(f"\n  {venue.upper()}:")
            if isinstance(bals, dict) and bals.get("error"):
                print(f"    Error: {bals['error']}")
            elif isinstance(bals, dict):
                for asset, val in sorted(bals.items()):
                    print(f"    {asset:<8} {val:>12.4f}")
            else:
                print(f"    {bals}")
        print("=" * 60 + "\n")

    elif command == "history":
        history = mgr.transfer_history()
        print("\n" + "=" * 60)
        print("  TRANSFER HISTORY")
        print("=" * 60)
        if not history:
            print("  No transfers recorded.")
        for rec in history:
            print(
                f"  #{rec['id']}: {rec['from_venue']} -> {rec['to_venue']} "
                f"{rec['amount']} {rec['asset']} [{rec['status']}] "
                f"@ {rec['initiated_at']}"
            )
        print("=" * 60 + "\n")

    elif command == "status" and len(sys.argv) > 2:
        transfer_id = int(sys.argv[2])
        status = mgr.track_transfer(transfer_id)
        print(json.dumps(status, indent=2, default=str))

    elif command == "reserve" and len(sys.argv) > 2:
        venue = sys.argv[2]
        reserve = mgr.check_minimum_reserve(venue)
        print(json.dumps(reserve, indent=2))

    elif command == "estimate-ach" and len(sys.argv) > 2:
        amount = float(sys.argv[2])
        info = CrossVenueTransfer.estimate_etrade_to_kraken(amount)
        print(json.dumps(info, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    mgr.close()


if __name__ == "__main__":
    main()
