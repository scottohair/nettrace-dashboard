#!/usr/bin/env python3
"""Treasury registry agent.

Maintains a persistent registry of wallets, connector readiness, reserve mix,
and retrieval metadata for liquidation workflows.

No secrets are written to disk; only presence flags + references are recorded.
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

try:
    from agent_tools import AgentTools
except Exception:
    try:
        from agents.agent_tools import AgentTools  # type: ignore
    except Exception:
        AgentTools = None  # type: ignore

try:
    from market_connector_hub import MarketConnectorHub
except Exception:
    try:
        from agents.market_connector_hub import MarketConnectorHub  # type: ignore
    except Exception:
        MarketConnectorHub = None  # type: ignore

try:
    import claude_duplex
except Exception:
    try:
        from agents import claude_duplex  # type: ignore
    except Exception:
        claude_duplex = None  # type: ignore

BASE = Path(__file__).parent
DB_PATH = BASE / "treasury_registry.db"
STATUS_PATH = BASE / "treasury_registry_status.json"
REGISTRY_PATH = BASE / "treasury_registry.json"
KEY_INDEX_PATH = BASE / "treasury_key_index.json"

ORCH_DB_PATH = BASE / "orchestrator.db"
RESERVE_STATUS_PATH = BASE / "reserve_targets_status.json"

DEFAULT_INTERVAL_SECONDS = int(os.environ.get("TREASURY_REGISTRY_INTERVAL_SECONDS", "300"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [treasury_registry] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE / "treasury_registry_agent.log")),
    ],
)
logger = logging.getLogger("treasury_registry_agent")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _load_json(path, default):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except Exception:
        return default


def _safe_bool_env(name):
    return bool(os.environ.get(name, "").strip())


class TreasuryRegistryAgent:
    def __init__(self, interval_seconds=DEFAULT_INTERVAL_SECONDS):
        self.interval_seconds = max(30, int(interval_seconds))
        self.running = True
        self.cycle = 0
        self.db = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self._init_db()

    def _init_db(self):
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS wallet_inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                chain TEXT,
                wallet_type TEXT,
                label TEXT,
                source TEXT,
                private_material_present INTEGER DEFAULT 0,
                retrievable INTEGER DEFAULT 0,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS connector_inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector TEXT NOT NULL,
                ready INTEGER DEFAULT 0,
                auth_present INTEGER DEFAULT 0,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS reserve_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_usd REAL,
                liquidity_usd REAL,
                holdings_json TEXT,
                reserve_targets_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self.db.commit()

    def _collect_orchestrator_wallets(self):
        wallets = []
        if not ORCH_DB_PATH.exists():
            return wallets
        try:
            conn = sqlite3.connect(str(ORCH_DB_PATH))
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT address, chain, wallet_type, label, is_ours
                FROM wallet_registry
                ORDER BY id ASC
                """
            ).fetchall()
            conn.close()
        except Exception as e:
            logger.debug("wallet_registry read failed: %s", e)
            return wallets

        for r in rows:
            wallets.append(
                {
                    "address": str(r["address"]),
                    "chain": str(r["chain"] or ""),
                    "wallet_type": str(r["wallet_type"] or "unknown"),
                    "label": str(r["label"] or ""),
                    "is_ours": bool(r["is_ours"]),
                    "source": "orchestrator.wallet_registry",
                }
            )
        return wallets

    def _collect_env_wallets(self):
        # Presence only; never persist raw keys.
        known = []
        env_wallets = [
            ("EVM_MAIN_WALLET", "ethereum,base,arbitrum,polygon", "evm"),
            ("SOLANA_MAIN_WALLET", "solana", "solana"),
            ("BTC_MAIN_WALLET", "bitcoin", "btc"),
        ]
        for name, chain, wtype in env_wallets:
            addr = os.environ.get(name, "").strip()
            if not addr:
                continue
            known.append(
                {
                    "address": addr,
                    "chain": chain,
                    "wallet_type": wtype,
                    "label": f"env:{name}",
                    "is_ours": True,
                    "source": "environment",
                }
            )
        return known

    def _private_material_matrix(self):
        # These booleans are used for retrievability scoring only.
        return {
            "wallet_private_key": _safe_bool_env("WALLET_PRIVATE_KEY") or _safe_bool_env("WALLET_PRIVATE_KEY_ENC"),
            "solana_private_key": _safe_bool_env("SOLANA_PRIVATE_KEY") or _safe_bool_env("SOLANA_KEYPAIR"),
            "coinbase_api": _safe_bool_env("COINBASE_API_KEY_ID") and _safe_bool_env("COINBASE_API_KEY_SECRET"),
            "etrade_api": _safe_bool_env("ETRADE_CONSUMER_KEY") and _safe_bool_env("ETRADE_CONSUMER_SECRET"),
            "ibkr_api": _safe_bool_env("IBKR_HOST") or _safe_bool_env("IBKR_PORT"),
            "stripe_api": _safe_bool_env("STRIPE_API_KEY") or _safe_bool_env("STRIPE_SECRET_KEY"),
        }

    def _connector_inventory(self):
        material = self._private_material_matrix()
        connectors = [
            {
                "connector": "coinbase",
                "auth_present": bool(material["coinbase_api"]),
                "ready": bool(material["coinbase_api"]),
                "details": {"api_key_present": bool(material["coinbase_api"])}
            },
            {
                "connector": "etrade",
                "auth_present": bool(material["etrade_api"]),
                "ready": bool(material["etrade_api"]),
                "details": {"consumer_key_present": bool(material["etrade_api"])}
            },
            {
                "connector": "ibkr",
                "auth_present": bool(material["ibkr_api"]),
                "ready": False,
                "details": {"host_config_present": bool(material["ibkr_api"])}
            },
            {
                "connector": "stripe",
                "auth_present": bool(material["stripe_api"]),
                "ready": bool(material["stripe_api"]),
                "details": {"secret_present": bool(material["stripe_api"])}
            },
        ]

        if MarketConnectorHub:
            try:
                hub = MarketConnectorHub()
                avail = hub.available_connectors()
                for c in connectors:
                    key = c["connector"]
                    if key in avail:
                        c["ready"] = bool(avail[key])
                        c["details"]["hub_ready"] = bool(avail[key])
            except Exception:
                pass

        return connectors

    def _portfolio_snapshot(self):
        if AgentTools is None:
            return {
                "total_usd": 0.0,
                "available_cash": 0.0,
                "held_in_orders": 0.0,
                "holdings": {},
                "source": "unavailable",
            }
        try:
            tools = AgentTools()
            p = tools.get_portfolio()
            if isinstance(p, dict):
                p["source"] = "agent_tools"
                return p
        except Exception as e:
            return {
                "total_usd": 0.0,
                "available_cash": 0.0,
                "held_in_orders": 0.0,
                "holdings": {},
                "source": f"error:{e}",
            }
        return {
            "total_usd": 0.0,
            "available_cash": 0.0,
            "held_in_orders": 0.0,
            "holdings": {},
            "source": "empty",
        }

    def _retrievability_score(self, wallets, connectors, material):
        wallet_count = max(1, len(wallets))
        retrievable_wallets = 0
        for w in wallets:
            chain = str(w.get("chain", "")).lower()
            if "sol" in chain:
                ok = bool(material.get("solana_private_key", False))
            elif "bitcoin" in chain or str(w.get("wallet_type", "")).lower() == "btc":
                ok = bool(material.get("wallet_private_key", False))
            else:
                ok = bool(material.get("wallet_private_key", False)) or bool(material.get("coinbase_api", False))
            if ok:
                retrievable_wallets += 1

        ready_connectors = sum(1 for c in connectors if bool(c.get("ready", False)))
        connector_score = ready_connectors / max(1, len(connectors))
        wallet_score = retrievable_wallets / wallet_count
        total = 0.68 * wallet_score + 0.32 * connector_score
        return {
            "wallet_score": round(wallet_score, 4),
            "connector_score": round(connector_score, 4),
            "total_score": round(total, 4),
            "retrievable_wallets": retrievable_wallets,
            "wallet_count": len(wallets),
            "ready_connectors": ready_connectors,
            "connector_count": len(connectors),
        }

    def run_cycle(self):
        self.cycle += 1

        wallets = self._collect_orchestrator_wallets() + self._collect_env_wallets()
        # Deduplicate by address.
        unique = {}
        for w in wallets:
            addr = str(w.get("address", "")).strip()
            if not addr:
                continue
            unique[addr] = w
        wallets = list(unique.values())

        material = self._private_material_matrix()
        connectors = self._connector_inventory()
        portfolio = self._portfolio_snapshot()
        reserve_targets = _load_json(RESERVE_STATUS_PATH, {})
        retrieval = self._retrievability_score(wallets, connectors, material)

        # Persist wallet inventory snapshot.
        for w in wallets:
            chain = str(w.get("chain", "")).lower()
            if "sol" in chain:
                has_material = bool(material.get("solana_private_key", False))
            elif "bitcoin" in chain:
                has_material = bool(material.get("wallet_private_key", False))
            else:
                has_material = bool(material.get("wallet_private_key", False)) or bool(material.get("coinbase_api", False))
            self.db.execute(
                """
                INSERT INTO wallet_inventory
                    (address, chain, wallet_type, label, source, private_material_present, retrievable, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(w.get("address", "")),
                    str(w.get("chain", "")),
                    str(w.get("wallet_type", "unknown")),
                    str(w.get("label", "")),
                    str(w.get("source", "unknown")),
                    1 if has_material else 0,
                    1 if has_material else 0,
                    json.dumps(w),
                ),
            )

        for c in connectors:
            self.db.execute(
                """
                INSERT INTO connector_inventory (connector, ready, auth_present, details_json)
                VALUES (?, ?, ?, ?)
                """,
                (
                    str(c.get("connector", "")),
                    1 if bool(c.get("ready", False)) else 0,
                    1 if bool(c.get("auth_present", False)) else 0,
                    json.dumps(c.get("details", {})),
                ),
            )

        self.db.execute(
            """
            INSERT INTO reserve_snapshots (total_usd, liquidity_usd, holdings_json, reserve_targets_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                float(portfolio.get("total_usd", 0.0) or 0.0),
                float(portfolio.get("available_cash", 0.0) or 0.0),
                json.dumps(portfolio.get("holdings", {})),
                json.dumps(reserve_targets),
            ),
        )
        self.db.commit()

        key_index = {
            "updated_at": _now_iso(),
            "presence": material,
            "note": "presence flags only; raw secrets are never written by treasury_registry_agent",
        }
        KEY_INDEX_PATH.write_text(json.dumps(key_index, indent=2))

        registry = {
            "updated_at": _now_iso(),
            "cycle": int(self.cycle),
            "wallets": wallets,
            "connectors": connectors,
            "portfolio": {
                "total_usd": float(portfolio.get("total_usd", 0.0) or 0.0),
                "available_cash": float(portfolio.get("available_cash", 0.0) or 0.0),
                "held_in_orders": float(portfolio.get("held_in_orders", 0.0) or 0.0),
                "source": str(portfolio.get("source", "unknown")),
            },
            "reserve_targets": reserve_targets,
            "retrievability": retrieval,
            "files": {
                "registry": str(REGISTRY_PATH),
                "key_index": str(KEY_INDEX_PATH),
                "status": str(STATUS_PATH),
            },
        }
        REGISTRY_PATH.write_text(json.dumps(registry, indent=2))

        status = {
            "updated_at": _now_iso(),
            "running": bool(self.running),
            "cycle": int(self.cycle),
            "interval_seconds": int(self.interval_seconds),
            "wallet_count": len(wallets),
            "connector_count": len(connectors),
            "retrievability": retrieval,
            "portfolio_total_usd": float(portfolio.get("total_usd", 0.0) or 0.0),
        }
        STATUS_PATH.write_text(json.dumps(status, indent=2))

        if claude_duplex:
            try:
                claude_duplex.send_to_claude(
                    message=(
                        f"treasury_registry cycle={self.cycle} wallets={len(wallets)} connectors_ready="
                        f"{retrieval['ready_connectors']}/{retrieval['connector_count']} "
                        f"retrievability={retrieval['total_score']:.2f}"
                    ),
                    msg_type="treasury_status",
                    priority="high" if retrieval["total_score"] < 0.60 else "normal",
                    source="treasury_registry_agent",
                    meta={"retrievability": retrieval},
                )
            except Exception:
                pass

        logger.info(
            "cycle=%d wallets=%d connectors_ready=%d/%d retrievability=%.2f total_usd=%.2f",
            self.cycle,
            len(wallets),
            retrieval["ready_connectors"],
            retrieval["connector_count"],
            retrieval["total_score"],
            float(portfolio.get("total_usd", 0.0) or 0.0),
        )

    def run_loop(self):
        logger.info("starting treasury_registry_agent loop interval=%ss", self.interval_seconds)
        while self.running:
            started = time.perf_counter()
            try:
                self.run_cycle()
            except Exception as e:
                logger.error("cycle_failed: %s", e, exc_info=True)
            elapsed = time.perf_counter() - started
            sleep_for = max(1, int(self.interval_seconds - elapsed))
            for _ in range(sleep_for):
                if not self.running:
                    break
                time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="Treasury registry + retrievability agent")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    agent = TreasuryRegistryAgent(interval_seconds=args.interval)
    if args.once:
        agent.run_cycle()
        print(json.dumps(json.loads(STATUS_PATH.read_text()), indent=2))
        return

    try:
        agent.run_loop()
    except KeyboardInterrupt:
        logger.info("stopped by keyboard interrupt")


if __name__ == "__main__":
    main()
