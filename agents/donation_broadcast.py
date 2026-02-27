#!/usr/bin/env python3
"""Broadcast donation intake details across local coordination channels."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

try:
    import claude_duplex
except Exception:
    from agents import claude_duplex  # type: ignore
try:
    from claude_staging import stage_operator_message
except Exception:
    try:
        from agents.claude_staging import stage_operator_message  # type: ignore
    except Exception:
        def stage_operator_message(*_args, **_kwargs):
            return None


BASE = Path(__file__).parent
TREASURY_PATH = BASE / "treasury_registry.json"
BUS_PATH = BASE / "agent_coordination_bus.jsonl"
CLAUDE_STAGE_PATH = BASE / "claude_staging" / "donation_broadcast.json"
MCP_GLOBAL_BROADCAST_PATH = BASE / "mcp_global_broadcast_outbox.json"
COINBASE_ASSETS_PATH = BASE / "coinbase_supported_assets.json"
DIRECT_WALLETS_PATH = BASE / "direct_wallet_framework.json"

ASSET_CHAIN_HINTS = {
    "BTC": ("bitcoin", "btc"),
    "ETH": ("ethereum", "eth"),
    "SOL": ("solana", "sol"),
    "USDC": ("ethereum", "base", "arbitrum", "polygon", "solana"),
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_public_exchange_wallet(wallet: dict) -> bool:
    wallet_type = str(wallet.get("wallet_type", "")).strip().lower()
    label = str(wallet.get("label", "")).strip().lower()
    chain = str(wallet.get("chain", "")).strip().lower()
    if not chain:
        return False
    markers = ("coinbase", "kraken")
    return any(m in wallet_type for m in markers) or any(m in label for m in markers)


def _is_direct_wallet(wallet: dict) -> bool:
    wallet_type = str(wallet.get("wallet_type", "")).strip().lower()
    label = str(wallet.get("label", "")).strip().lower()
    markers = ("coinbase", "kraken", "deposit", "exchange")
    if any(m in wallet_type for m in markers) or any(m in label for m in markers):
        return False
    return True


def _split_chains(chain_text: str) -> list[str]:
    raw = [c.strip().lower() for c in str(chain_text or "").split(",")]
    return [c for c in raw if c]


def _load_coinbase_supported_assets() -> list[str]:
    # Preferred source is local JSON to keep deployment deterministic.
    if COINBASE_ASSETS_PATH.exists():
        try:
            data = json.loads(COINBASE_ASSETS_PATH.read_text())
            if isinstance(data, dict):
                assets = data.get("assets", [])
            else:
                assets = data
            if isinstance(assets, list):
                return sorted({str(a).strip().upper() for a in assets if str(a).strip()})
        except Exception:
            pass
    # Optional fallback from environment.
    raw = os.environ.get("COINBASE_SUPPORTED_ASSETS", "").strip()
    if raw:
        return sorted({x.strip().upper() for x in raw.split(",") if x.strip()})
    return []


def _expand_assets(raw_assets: list[str]) -> list[str]:
    out: set[str] = set()
    for asset in raw_assets:
        token = str(asset).strip().upper()
        if not token:
            continue
        if token in {"COINBASE_ALL", "COINBASE_SUPPORTED", "ALL_COINBASE"}:
            out.update(_load_coinbase_supported_assets())
        else:
            out.add(token)
    return sorted(out)


def _asset_supported_by_wallet(asset: str, wallet: dict) -> bool:
    hints = ASSET_CHAIN_HINTS.get(asset.upper())
    if not hints:
        return False
    chains = _split_chains(str(wallet.get("chain", "")))
    if not chains:
        return False
    return any(any(h in chain for h in hints) for chain in chains)


def validate_asset_routes(assets: list[str], wallets: list[dict]) -> tuple[list[str], dict[str, list[dict]]]:
    route_map: dict[str, list[dict]] = {}
    unsupported: list[str] = []
    for asset in assets:
        candidates = [w for w in wallets if _asset_supported_by_wallet(asset, w)]
        if not candidates:
            unsupported.append(asset)
            continue
        route_map[asset] = candidates
    return unsupported, route_map


def _load_treasury_wallets() -> list[dict]:
    if not TREASURY_PATH.exists():
        return []
    try:
        data = json.loads(TREASURY_PATH.read_text())
    except Exception:
        return []
    wallets = data.get("wallets") or data.get("wallet_inventory") or []
    if not isinstance(wallets, list):
        return []
    return [w for w in wallets if isinstance(w, dict)]


def load_exchange_wallets() -> list[dict]:
    wallets = _load_treasury_wallets()
    out = []
    for w in wallets:
        addr = str(w.get("address", "")).strip()
        if not addr:
            continue
        if not _is_public_exchange_wallet(w):
            continue
        out.append(
            {
                "address": addr,
                "chain": str(w.get("chain", "")).strip(),
                "wallet_type": str(w.get("wallet_type", "")).strip(),
                "label": str(w.get("label", "")).strip(),
            }
        )
    return out


def load_direct_wallets() -> list[dict]:
    out = []
    # Preferred source: explicit direct wallet framework file.
    if DIRECT_WALLETS_PATH.exists():
        try:
            data = json.loads(DIRECT_WALLETS_PATH.read_text())
            wallets = data.get("wallets", []) if isinstance(data, dict) else []
            if isinstance(wallets, list):
                for w in wallets:
                    if not isinstance(w, dict):
                        continue
                    addr = str(w.get("address", "")).strip()
                    if not addr:
                        continue
                    if not bool(w.get("public_receive", True)):
                        continue
                    chains = w.get("chains", [])
                    if not isinstance(chains, list):
                        chains = []
                    out.append(
                        {
                            "address": addr,
                            "chain": ",".join(str(c).strip() for c in chains if str(c).strip()),
                            "wallet_type": str(w.get("wallet_type", "direct")).strip() or "direct",
                            "label": str(w.get("label", "Direct wallet")).strip() or "Direct wallet",
                            "custody": str(w.get("custody", "self")).strip() or "self",
                        }
                    )
        except Exception:
            pass

    # Fallback source: non-exchange wallets from treasury registry.
    if not out:
        for w in _load_treasury_wallets():
            addr = str(w.get("address", "")).strip()
            if not addr:
                continue
            if not _is_direct_wallet(w):
                continue
            out.append(
                {
                    "address": addr,
                    "chain": str(w.get("chain", "")).strip(),
                    "wallet_type": str(w.get("wallet_type", "direct")).strip() or "direct",
                    "label": str(w.get("label", "Direct wallet")).strip() or "Direct wallet",
                    "custody": "self",
                }
            )
    return out


def append_bus(payload: dict) -> None:
    BUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(BUS_PATH, "a") as f:
        f.write(json.dumps(payload, separators=(",", ":")) + "\n")


def build_message(
    assets: list[str],
    exchange_wallets: list[dict],
    direct_wallets: list[dict],
    note: str,
) -> str:
    lines = []
    lines.append("MCP donation broadcast: crypto donations enabled.")
    lines.append(f"Accepted assets: {', '.join(assets)}")
    lines.append("Important: verify chain/network before sending any funds.")
    if note:
        lines.append(f"Note: {note}")
    if exchange_wallets:
        lines.append("Exchange deposit endpoints:")
        for w in exchange_wallets:
            chain = w.get("chain") or "unknown"
            label = w.get("label") or w.get("wallet_type") or "wallet"
            lines.append(f"- {label} [{chain}] {w.get('address')}")
    if direct_wallets:
        lines.append("Direct on-chain receive endpoints:")
        for w in direct_wallets:
            chain = w.get("chain") or "unknown"
            label = w.get("label") or w.get("wallet_type") or "wallet"
            custody = w.get("custody") or "self"
            lines.append(f"- {label} [{chain}] {w.get('address')} (custody={custody})")
    else:
        lines.append("No direct wallets configured")
    return "\n".join(lines)


def _math_path_packet(min_edge_pct: float, min_confidence: float) -> dict:
    return {
        "score_formula": "score = 0.52*edge_score + 0.43*confidence - 0.25*latency_penalty",
        "edge_score": "clamp(expected_edge_pct / 1.2, 0, 1)",
        "latency_penalty": "clamp(latency_ms / 1200, 0, 0.65)",
        "entry_gate": {
            "expected_edge_pct_min": round(float(min_edge_pct), 4),
            "confidence_min": round(float(min_confidence), 4),
            "ev_rule": "EV = p_win*avg_win - (1-p_win)*avg_loss - costs; require EV > 0",
        },
    }


def _safety_rails_packet(
    reserve_floor_pct: float,
    deploy_cap_pct: float,
    daily_drawdown_halt_pct: float,
    leverage_cap: float,
) -> dict:
    return {
        "opt_in_only": True,
        "no_private_keys_or_credentials_collected": True,
        "manual_contribution_only": True,
        "autoconvert_take_profit_to_usd_usdc": True,
        "reserve_floor_pct": round(float(reserve_floor_pct), 4),
        "max_new_capital_deploy_pct": round(float(deploy_cap_pct), 4),
        "daily_drawdown_halt_pct": round(float(daily_drawdown_halt_pct), 4),
        "leverage_cap": round(float(leverage_cap), 4),
        "failsafe_mode": "halt_new_entries_on_risk_breach",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Broadcast donation intake details")
    parser.add_argument(
        "--assets",
        default="BTC,ETH,SOL,USDC",
        help="Comma-separated accepted assets",
    )
    parser.add_argument(
        "--note",
        default="",
        help="Optional operator note for the broadcast",
    )
    parser.add_argument(
        "--sender",
        default="codex",
        help="Sender tag for coordination bus",
    )
    parser.add_argument(
        "--pool-id",
        default="global_growth_pool_v1",
        help="Capital pool id",
    )
    parser.add_argument(
        "--min-edge-pct",
        type=float,
        default=0.20,
        help="Minimum expected edge percentage for eligible strategies",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.70,
        help="Minimum model confidence for eligible strategies",
    )
    parser.add_argument(
        "--reserve-floor-pct",
        type=float,
        default=35.0,
        help="Minimum reserve floor in USD/USDC",
    )
    parser.add_argument(
        "--deploy-cap-pct",
        type=float,
        default=25.0,
        help="Maximum share of newly contributed capital to deploy immediately",
    )
    parser.add_argument(
        "--daily-drawdown-halt-pct",
        type=float,
        default=3.0,
        help="Daily drawdown halt threshold",
    )
    parser.add_argument(
        "--leverage-cap",
        type=float,
        default=2.0,
        help="Global leverage cap for contributed capital",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and print payload without writing/sending",
    )
    args = parser.parse_args()

    assets = _expand_assets([x.strip().upper() for x in str(args.assets).split(",") if x.strip()])
    exchange_wallets = load_exchange_wallets()
    direct_wallets = load_direct_wallets()
    all_wallets = exchange_wallets + direct_wallets
    unsupported_assets, route_map = validate_asset_routes(assets=assets, wallets=all_wallets)
    message = build_message(
        assets=assets,
        exchange_wallets=exchange_wallets,
        direct_wallets=direct_wallets,
        note=str(args.note).strip(),
    )
    math_path = _math_path_packet(
        min_edge_pct=max(0.0, float(args.min_edge_pct)),
        min_confidence=max(0.0, min(1.0, float(args.min_confidence))),
    )
    safety_rails = _safety_rails_packet(
        reserve_floor_pct=max(0.0, float(args.reserve_floor_pct)),
        deploy_cap_pct=max(0.0, min(100.0, float(args.deploy_cap_pct))),
        daily_drawdown_halt_pct=max(0.0, min(100.0, float(args.daily_drawdown_halt_pct))),
        leverage_cap=max(1.0, float(args.leverage_cap)),
    )

    packet = {
        "timestamp": now_iso(),
        "type": "mcp_global_math_path_open_call",
        "sender": str(args.sender).strip() or "codex",
        "pool_id": str(args.pool_id).strip() or "global_growth_pool_v1",
        "accepted_assets": assets,
        "inbound_policy": "accept_any_asset_symbol_manual_review_for_unrouted",
        "unrouted_assets": unsupported_assets,
        "asset_routes": {
            asset: [
                {
                    "label": str(w.get("label", "")),
                    "chain": str(w.get("chain", "")),
                    "address": str(w.get("address", "")),
                }
                for w in route_map.get(asset, [])
            ]
            for asset in assets
        },
        "exchange_wallets": exchange_wallets,
        "direct_wallets": direct_wallets,
        "wallets": all_wallets,
        "note": str(args.note).strip(),
        "message": message,
        "target": "global_nodes_and_agents",
        "topic": "global_pool_open_call",
        "math_path": math_path,
        "safety_rails": safety_rails,
        "join_terms": {
            "contribution_mode": "donation_or_opt_in_capital_add",
            "custody": "sender-controlled transfer only",
            "auto_pull_from_contributors": False,
            "automatic_credit_on_incoming_settlement": True,
            "settlement_priority": ["USD", "USDC"],
            "route_policy": "unrouted_assets_allowed_but_require_manual_review",
        },
        "schema_version": 1,
    }

    print(json.dumps(packet, indent=2))
    if args.dry_run:
        return 0

    CLAUDE_STAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CLAUDE_STAGE_PATH.write_text(json.dumps(packet, indent=2))
    MCP_GLOBAL_BROADCAST_PATH.write_text(json.dumps(packet, indent=2))
    append_bus(
        {
            "timestamp": packet["timestamp"],
            "channel": "agent_coordination_bus",
            "sender": packet["sender"],
            "recipient": "global_nodes_and_agents",
            "type": "mcp_broadcast",
            "priority": "high",
            "topic": "global_pool_open_call",
            "message": packet["message"],
            "meta": {
                "accepted_assets": assets,
                "wallet_count": len(all_wallets),
                "pool_id": packet["pool_id"],
                "schema_version": 1,
            },
        }
    )
    stage_operator_message(
        (
            f"MCP global open call active: pool={packet['pool_id']} "
            f"assets={','.join(assets)} "
            f"edge>={packet['math_path']['entry_gate']['expected_edge_pct_min']}% "
            f"conf>={packet['math_path']['entry_gate']['confidence_min']}"
        ),
        category="mcp_broadcast",
        priority="high",
        sender=packet["sender"],
    )
    claude_duplex.send_to_claude(
        message=packet["message"],
        msg_type="mcp_broadcast",
        priority="high",
        source=packet["sender"],
        meta={
            "topic": "global_pool_open_call",
            "accepted_assets": assets,
            "wallet_count": len(all_wallets),
            "pool_id": packet["pool_id"],
            "math_path": math_path,
            "safety_rails": safety_rails,
            "schema_version": 1,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
