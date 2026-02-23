#!/usr/bin/env python3
"""Kraken Staking/Earn connector — passive yield on idle crypto assets.

Kraken offers two staking types:
  - Bonded staking: Higher yield, 7-28 day lock period
  - Flexible staking (Earn): Lower yield, instant unstake, tradeable

Safety rules:
  - Only stake from savings/subsavings accounts (never trading capital)
  - Keep minimum 2x daily trading volume liquid
  - Prefer flexible staking for small balances
  - Bonded staking only when balance > 2x trading needs

Kraken Staking API endpoints:
  - Staking/Assets: List stakeable assets with rates
  - Stake: Stake an asset
  - Unstake: Unstake an asset
  - Staking/Pending: Pending staking transactions
  - Staking/Transactions: Staking history

Also supports Earn endpoints:
  - Earn/Strategies: List earn strategies with APYs
  - Earn/Allocations: Current earn positions
  - Earn/Allocate: Add to earn
  - Earn/Deallocate: Remove from earn
"""

import json
import logging
import os
import sqlite3
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"'))

try:
    from kraken_connector import KrakenConnector
except ImportError:
    KrakenConnector = None

logger = logging.getLogger("kraken_staking")

# Configuration
STAKING_ENABLED = os.environ.get("KRAKEN_STAKING_ENABLED", "0") == "1"
MIN_LIQUID_MULTIPLIER = float(os.environ.get("KRAKEN_STAKING_LIQUID_MULTIPLIER", "2.0"))
MIN_STAKE_USD = float(os.environ.get("KRAKEN_MIN_STAKE_USD", "1.0"))
PREFER_FLEXIBLE = os.environ.get("KRAKEN_STAKING_PREFER_FLEXIBLE", "1") == "1"

# Staking tracking DB
STAKING_DB = Path(__file__).parent / "kraken_staking.db"


def _init_staking_db():
    """Initialize the staking events database."""
    db = sqlite3.connect(str(STAKING_DB))
    db.execute("""
        CREATE TABLE IF NOT EXISTS kraken_staking_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL,
            amount REAL NOT NULL,
            method TEXT,
            action TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            result_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.commit()
    return db


def _record_staking_event(asset, amount, method, action, result):
    """Record a staking event to the tracking database."""
    try:
        db = _init_staking_db()
        status = "success" if result.get("result") else "error"
        db.execute(
            """INSERT INTO kraken_staking_events
               (asset, amount, method, action, status, result_json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (asset, amount, method, action, status, json.dumps(result)),
        )
        db.commit()
        db.close()
    except Exception as e:
        logger.error("Failed to record staking event: %s", e)


class KrakenStakingConnector:
    """Kraken staking/earn connector for passive yield."""

    @staticmethod
    def list_stakeable_assets() -> list:
        """List all assets available for staking with rates.

        Uses Earn/Strategies endpoint for comprehensive list.
        Returns: [{"asset": "ETH", "method": "ethereum", "apy": 3.5,
                   "min_amount": 0.01, "lock_type": "flexible", "lock_days": 0}, ...]
        """
        if not KrakenConnector:
            return []

        result = KrakenConnector._private_request("Earn/Strategies")
        if result.get("error"):
            # Fallback to legacy Staking/Assets
            result = KrakenConnector._private_request("Staking/Assets")
            if result.get("error"):
                logger.error("Failed to list stakeable assets: %s", result["error"])
                return []

            # Parse legacy format
            assets = []
            for asset_info in result.get("result", []):
                assets.append({
                    "asset": asset_info.get("asset", ""),
                    "method": asset_info.get("method", ""),
                    "apy": float(asset_info.get("rewards", {}).get("reward", 0)),
                    "min_amount": float(asset_info.get("minimum_amount", {}).get("staking", 0)),
                    "lock_type": "bonded" if asset_info.get("lock") else "flexible",
                    "lock_days": int(asset_info.get("lock", {}).get("lockup", 0)) if asset_info.get("lock") else 0,
                    "unstake_days": int(asset_info.get("lock", {}).get("unstaking", 0)) if asset_info.get("lock") else 0,
                })
            return assets

        # Parse Earn/Strategies format
        strategies = []
        for strategy in result.get("result", {}).get("items", []):
            apy_data = strategy.get("yield_source", {})
            lock_info = strategy.get("lock_type", {})

            strategies.append({
                "id": strategy.get("id", ""),
                "asset": strategy.get("asset", ""),
                "apy": float(apy_data.get("apy", 0)) if apy_data else 0,
                "min_amount": float(strategy.get("min_amount", 0)),
                "lock_type": lock_info.get("type", "flexible"),
                "lock_days": int(lock_info.get("bonding_period_days", 0)) if lock_info.get("type") == "bonded" else 0,
                "can_trade_while_staked": strategy.get("can_trade", False),
            })
        return strategies

    @staticmethod
    def get_staking_rates() -> dict:
        """Get current APY rates per asset (simplified view).

        Returns: {"ETH": {"apy": 3.5, "lock_type": "flexible"}, ...}
        """
        assets = KrakenStakingConnector.list_stakeable_assets()
        rates = {}
        for a in assets:
            asset = a.get("asset", "")
            if asset not in rates or a.get("apy", 0) > rates[asset].get("apy", 0):
                rates[asset] = {
                    "apy": a.get("apy", 0),
                    "lock_type": a.get("lock_type", "flexible"),
                    "min_amount": a.get("min_amount", 0),
                }
        return rates

    @staticmethod
    def stake(asset: str, amount: float, method: str = "flexible") -> dict:
        """Stake an asset.

        Args:
            asset: Asset code (e.g., "ETH", "DOT", "SOL")
            amount: Amount to stake
            method: "flexible" or "bonded"

        Returns: dict with staking transaction info
        """
        if not STAKING_ENABLED:
            return {"error": "Staking not enabled (set KRAKEN_STAKING_ENABLED=1)"}

        if amount < MIN_STAKE_USD:
            return {"error": f"Amount {amount} below minimum {MIN_STAKE_USD}"}

        if not KrakenConnector:
            return {"error": "KrakenConnector not available"}

        # Try Earn/Allocate first (newer API)
        strategies = KrakenStakingConnector.list_stakeable_assets()
        strategy_id = None
        for s in strategies:
            if s.get("asset") == asset:
                if method == "flexible" and s.get("lock_type") == "flexible":
                    strategy_id = s.get("id")
                    break
                elif method == "bonded" and s.get("lock_type") == "bonded":
                    strategy_id = s.get("id")
                    break

        if strategy_id:
            result = KrakenConnector._private_request("Earn/Allocate", {
                "strategy_id": strategy_id,
                "amount": str(amount),
            })
        else:
            # Fallback to legacy Stake endpoint
            result = KrakenConnector._private_request("Stake", {
                "asset": asset,
                "amount": str(amount),
                "method": method,
            })

        # Record in DB
        _record_staking_event(asset, amount, method, "stake", result)

        return result

    @staticmethod
    def unstake(asset: str, amount: float) -> dict:
        """Unstake an asset.

        For flexible staking: instant.
        For bonded staking: starts unbonding period.
        """
        if not STAKING_ENABLED:
            return {"error": "Staking not enabled"}

        if not KrakenConnector:
            return {"error": "KrakenConnector not available"}

        # Try Earn/Deallocate first
        result = KrakenConnector._private_request("Earn/Deallocate", {
            "asset": asset,
            "amount": str(amount),
        })

        if result.get("error"):
            # Fallback to legacy Unstake
            result = KrakenConnector._private_request("Unstake", {
                "asset": asset,
                "amount": str(amount),
            })

        _record_staking_event(asset, amount, "unknown", "unstake", result)

        return result

    @staticmethod
    def get_staking_positions() -> dict:
        """Get current staked balances.

        Returns: {"ETH": {"staked": 1.5, "pending": 0.0, "rewards": 0.01}, ...}
        """
        if not KrakenConnector:
            return {}

        # Try Earn/Allocations
        result = KrakenConnector._private_request("Earn/Allocations")
        if result.get("result"):
            positions = {}
            for alloc in result["result"].get("items", []):
                asset = alloc.get("native_asset", "")
                amount = float(alloc.get("amount_allocated", {}).get("total", {}).get("native", 0))
                pending = float(alloc.get("amount_allocated", {}).get("pending", {}).get("native", 0))
                rewards = float(alloc.get("total_rewarded", {}).get("native", 0))
                if amount > 0 or pending > 0:
                    positions[asset] = {
                        "staked": amount,
                        "pending": pending,
                        "rewards": rewards,
                        "strategy_id": alloc.get("strategy_id", ""),
                    }
            return positions

        # Fallback: legacy Staking/Pending
        result = KrakenConnector._private_request("Staking/Pending")
        positions = {}
        if result.get("result"):
            for tx in result["result"]:
                asset = tx.get("asset", "")
                amount = float(tx.get("amount", 0))
                if asset in positions:
                    positions[asset]["pending"] += amount
                else:
                    positions[asset] = {"staked": 0, "pending": amount, "rewards": 0}
        return positions

    @staticmethod
    def get_pending_transactions() -> list:
        """Get pending staking/unstaking transactions."""
        if not KrakenConnector:
            return []

        result = KrakenConnector._private_request("Staking/Pending")
        return result.get("result", [])

    @staticmethod
    def optimal_staking_allocation(balances: dict = None, daily_trading_volume: float = 50.0) -> list:
        """Calculate optimal staking allocation for idle capital.

        Rules:
          - Keep 2x daily_trading_volume liquid per asset
          - Stake everything else
          - Prefer flexible for amounts < $50 equivalent
          - Use bonded for larger amounts (higher APY)

        Args:
            balances: dict of {asset: balance}. If None, fetches from Kraken.
            daily_trading_volume: estimated daily trading volume in USD

        Returns: list of {"asset": X, "amount": Y, "method": Z, "expected_apy": W}
        """
        if balances is None:
            if not KrakenConnector:
                return []
            result = KrakenConnector._private_request("Balance")
            if result.get("error"):
                return []
            balances = {}
            for asset, bal in result.get("result", {}).items():
                bal_f = float(bal)
                if bal_f > 0:
                    balances[asset] = bal_f

        rates = KrakenStakingConnector.get_staking_rates()
        existing = KrakenStakingConnector.get_staking_positions()

        recommendations = []
        for asset, balance in balances.items():
            if asset not in rates:
                continue  # Not stakeable

            # Already staked amount
            already_staked = existing.get(asset, {}).get("staked", 0)
            available = balance - already_staked

            # Keep liquid reserve
            liquid_reserve = MIN_LIQUID_MULTIPLIER * daily_trading_volume
            stakeable = max(0, available - liquid_reserve)

            if stakeable < rates[asset].get("min_amount", 0):
                continue

            # Choose method
            method = "flexible"
            if not PREFER_FLEXIBLE and stakeable > 50:  # >$50 equivalent -> bonded for higher APY
                method = "bonded"

            recommendations.append({
                "asset": asset,
                "amount": stakeable,
                "method": method,
                "expected_apy": rates[asset].get("apy", 0),
                "liquid_reserve": liquid_reserve,
            })

        return sorted(recommendations, key=lambda x: x["expected_apy"], reverse=True)

    @staticmethod
    def auto_stake_idle():
        """Automatically stake idle balances based on optimal allocation.

        Only executes if STAKING_ENABLED=1.
        Returns list of staking results.
        """
        if not STAKING_ENABLED:
            return []

        recommendations = KrakenStakingConnector.optimal_staking_allocation()
        results = []

        for rec in recommendations:
            if rec["amount"] > 0:
                result = KrakenStakingConnector.stake(
                    asset=rec["asset"],
                    amount=rec["amount"],
                    method=rec["method"],
                )
                results.append({
                    "asset": rec["asset"],
                    "amount": rec["amount"],
                    "method": rec["method"],
                    "result": result,
                })
                logger.info("Auto-staked %.6f %s (%s) — expected APY: %.2f%%",
                            rec["amount"], rec["asset"], rec["method"], rec["expected_apy"])

        return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [STAKING] %(levelname)s %(message)s")

    print("Kraken Staking Connector")
    print(f"  STAKING_ENABLED: {STAKING_ENABLED}")
    print(f"  MIN_LIQUID_MULTIPLIER: {MIN_LIQUID_MULTIPLIER}")
    print(f"  MIN_STAKE_USD: {MIN_STAKE_USD}")
    print(f"  PREFER_FLEXIBLE: {PREFER_FLEXIBLE}")

    if KrakenConnector:
        print("\nFetching staking rates...")
        rates = KrakenStakingConnector.get_staking_rates()
        for asset, info in rates.items():
            print(f"  {asset}: APY={info['apy']:.2f}% ({info['lock_type']})")
    else:
        print("\nKrakenConnector not available — cannot fetch rates")
