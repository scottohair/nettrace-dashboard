#!/usr/bin/env python3
"""AmiCoin system engine.

Simulation-first implementation of:
  - 10-network-scoped AmiCoins (ETH/SOL/BTC anchored)
  - Central reserve coin (AMIR)
  - Wallet buckets per network
  - Private 24/7 strategy pool with strict profit-only exits

This module intentionally does NOT mint real on-chain tokens. It is the
validation and operations layer that must pass before any live token issuance.
"""

import hashlib
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from exchange_connector import PriceFeed
except Exception:  # pragma: no cover - fallback when imported outside agents path
    PriceFeed = None

BASE_DIR = Path(__file__).parent
STATE_FILE = BASE_DIR / "amicoin_state.json"

FEE_PCT = 0.003
SLIPPAGE_PCT = 0.001
MIN_NET_PROFIT_PCT = 0.006  # must clear fees+slippage and still profit

# 4 ETH + 3 SOL + 3 BTC anchored = 10 total
COIN_SPECS = [
    {"symbol": "AMIETH1", "network": "ethereum", "anchor_pair": "ETH-USD", "peg_factor": 0.00042, "vol": 0.012, "drift": 0.0006},
    {"symbol": "AMIETH2", "network": "ethereum", "anchor_pair": "ETH-USD", "peg_factor": 0.00051, "vol": 0.014, "drift": 0.0007},
    {"symbol": "AMIETH3", "network": "ethereum", "anchor_pair": "ETH-USD", "peg_factor": 0.00063, "vol": 0.016, "drift": 0.0008},
    {"symbol": "AMIETH4", "network": "ethereum", "anchor_pair": "ETH-USD", "peg_factor": 0.00076, "vol": 0.018, "drift": 0.0009},
    {"symbol": "AMISOL1", "network": "solana", "anchor_pair": "SOL-USD", "peg_factor": 0.021, "vol": 0.019, "drift": 0.0010},
    {"symbol": "AMISOL2", "network": "solana", "anchor_pair": "SOL-USD", "peg_factor": 0.027, "vol": 0.021, "drift": 0.0011},
    {"symbol": "AMISOL3", "network": "solana", "anchor_pair": "SOL-USD", "peg_factor": 0.034, "vol": 0.023, "drift": 0.0012},
    {"symbol": "AMIBTC1", "network": "bitcoin", "anchor_pair": "BTC-USD", "peg_factor": 0.000022, "vol": 0.010, "drift": 0.0005},
    {"symbol": "AMIBTC2", "network": "bitcoin", "anchor_pair": "BTC-USD", "peg_factor": 0.000028, "vol": 0.011, "drift": 0.0006},
    {"symbol": "AMIBTC3", "network": "bitcoin", "anchor_pair": "BTC-USD", "peg_factor": 0.000034, "vol": 0.013, "drift": 0.0007},
]

STRATEGIES = ("momentum", "mean_reversion", "breakout")

DEFAULT_ANCHORS = {
    "ETH-USD": 2500.0,
    "SOL-USD": 100.0,
    "BTC-USD": 50000.0,
}


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _wallet_address(network: str) -> str:
    digest = hashlib.sha256(f"amicoin-{network}".encode()).hexdigest()
    if network == "solana":
        return f"AMISOL{digest[:34].upper()}"
    if network == "bitcoin":
        return f"bc1q{digest[:38]}"
    return "0x" + digest[:40]


def _price_feed(pair: str):
    if PriceFeed is None:
        return None
    try:
        return PriceFeed.get_price(pair)
    except Exception:
        return None


def _anchor_prices():
    prices = {}
    for pair, fallback in DEFAULT_ANCHORS.items():
        px = _price_feed(pair)
        prices[pair] = float(px) if px and px > 0 else fallback
    return prices


def _sync_go_live_flags(state):
    checks = state.setdefault("go_live_checklist", {})
    routes = checks.setdefault("conversion_routes", {})
    for sym in ("USDC", "BTC", "ETH", "SOL"):
        routes.setdefault(sym, False)
    checks.setdefault("coinbase_listing", False)
    checks.setdefault("dex_listing", False)

    # "Listening on Coinbase" means at least one live Coinbase anchor feed is reachable.
    live_hits = 0
    for pair in ("BTC-USD", "ETH-USD", "SOL-USD"):
        px = _price_feed(pair)
        if px and px > 0:
            live_hits += 1
    checks["coinbase_market_listening"] = live_hits >= 1

    go_live = (
        bool(checks.get("coinbase_listing")) and
        bool(checks.get("dex_listing")) and
        all(bool(routes.get(sym)) for sym in ("USDC", "BTC", "ETH", "SOL"))
    )
    network = state.setdefault("network", {})
    network["go_live_ready"] = bool(go_live)


def _initial_wallets(initial_reserve_usd: float):
    return {
        "ethereum": {
            "network": "ethereum",
            "address": _wallet_address("ethereum"),
            "reserve_usd": round(initial_reserve_usd * 0.4, 2),
            "allocated_usd": 0.0,
            "realized_pnl_usd": 0.0,
        },
        "solana": {
            "network": "solana",
            "address": _wallet_address("solana"),
            "reserve_usd": round(initial_reserve_usd * 0.3, 2),
            "allocated_usd": 0.0,
            "realized_pnl_usd": 0.0,
        },
        "bitcoin": {
            "network": "bitcoin",
            "address": _wallet_address("bitcoin"),
            "reserve_usd": round(initial_reserve_usd * 0.3, 2),
            "allocated_usd": 0.0,
            "realized_pnl_usd": 0.0,
        },
    }


def _initial_coins(anchor_prices):
    coins = []
    ts = int(time.time())
    for spec in COIN_SPECS:
        anchor = anchor_prices.get(spec["anchor_pair"], DEFAULT_ANCHORS[spec["anchor_pair"]])
        px = round(max(0.01, anchor * spec["peg_factor"]), 6)
        coins.append({
            "symbol": spec["symbol"],
            "network": spec["network"],
            "anchor_pair": spec["anchor_pair"],
            "peg_factor": spec["peg_factor"],
            "drift": spec["drift"],
            "vol": spec["vol"],
            "price_usd": px,
            "prev_price_usd": px,
            "day_start_price_usd": px,
            "history": [{"t": ts, "p": px}],
            "circulating_supply": 1_000_000,
            "strategies": list(STRATEGIES),
            "open_positions": 0,
            "unrealized_pnl_usd": 0.0,
            "realized_pnl_usd": 0.0,
            "trades": 0,
        })
    return coins


def _initial_state(initial_reserve_usd: float = 10000.0):
    anchors = _anchor_prices()
    return {
        "version": 1,
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
        "network": {
            "name": "AmiCoin Network",
            "mode": "simulation_only",
            "excluded_from_real_holdings": True,
            "counts_toward_portfolio": False,
            "go_live_ready": False,
        },
        "go_live_checklist": {
            "coinbase_market_listening": False,
            "coinbase_listing": False,
            "dex_listing": False,
            "conversion_routes": {
                "USDC": False,
                "BTC": False,
                "ETH": False,
                "SOL": False,
            },
        },
        "reserve": {
            "symbol": "AMIR",
            "liquid_usd": round(initial_reserve_usd, 2),
            "allocated_usd": 0.0,
            "realized_pnl_usd": 0.0,
            "high_watermark_usd": round(initial_reserve_usd, 2),
            "max_drawdown_pct": 0.0,
            "risk_lock": False,
            "lock_reason": "",
        },
        "wallets": _initial_wallets(initial_reserve_usd),
        "coins": _initial_coins(anchors),
        "positions": [],
        "stats": {
            "cycles": 0,
            "closed_trades": 0,
            "wins": 0,
            "blocked_losses": 0,
            "budget_rejections": 0,
        },
        "strategy_budgets": {
            "momentum": 0.33,
            "mean_reversion": 0.34,
            "breakout": 0.33,
        },
        "summary": {},
    }


def load_state(path: Path = STATE_FILE):
    if not path.exists():
        state = _initial_state()
        save_state(state, path=path)
        return state

    try:
        state = json.loads(path.read_text())
    except Exception:
        state = _initial_state()
        save_state(state, path=path)
        return state

    # Minimal migration guards
    if "network" not in state:
        state["network"] = _initial_state()["network"]
    if "go_live_checklist" not in state:
        state["go_live_checklist"] = _initial_state()["go_live_checklist"]
    if "reserve" not in state:
        state["reserve"] = _initial_state()["reserve"]
    if "wallets" not in state:
        state["wallets"] = _initial_wallets(state["reserve"].get("liquid_usd", 10000.0))
    if "coins" not in state or len(state["coins"]) != 10:
        state["coins"] = _initial_coins(_anchor_prices())
    if "positions" not in state:
        state["positions"] = []
    if "stats" not in state:
        state["stats"] = _initial_state()["stats"]
    if "strategy_budgets" not in state:
        state["strategy_budgets"] = _initial_state()["strategy_budgets"]
    if "summary" not in state:
        state["summary"] = {}
    return state


def save_state(state, path: Path = STATE_FILE):
    state["updated_at"] = _utc_now()
    path.write_text(json.dumps(state, indent=2))


def _sma(values, window):
    if len(values) < window or window <= 0:
        return None
    return sum(values[-window:]) / window


def _position_id(symbol, strategy):
    return f"{symbol}:{strategy}"


def _coin_signal(coin):
    prices = [float(h["p"]) for h in coin.get("history", [])]
    if len(prices) < 8:
        return {"momentum": False, "mean_reversion": False, "breakout": False}

    p = prices[-1]
    sma5 = _sma(prices, 5) or p
    sma8 = _sma(prices, 8) or p
    prev_max6 = max(prices[-7:-1]) if len(prices) > 7 else p

    momentum = p > sma5 > sma8
    mean_reversion = p < sma5 * 0.988
    breakout = p > prev_max6 * 1.002
    return {"momentum": momentum, "mean_reversion": mean_reversion, "breakout": breakout}


def _budget_for_coin(state, coin, strategy):
    reserve = state["reserve"]
    wallets = state["wallets"]
    wallet = wallets.get(coin["network"])
    if not wallet:
        return 0.0
    if reserve.get("risk_lock"):
        return 0.0

    liquid = float(reserve.get("liquid_usd", 0))
    w_liquid = float(wallet.get("reserve_usd", 0))
    if liquid <= 0 or w_liquid <= 0:
        return 0.0

    strategy_share = float(state["strategy_budgets"].get(strategy, 0.2))
    reserve_cap = liquid * 0.02 * strategy_share          # up to 2% reserve * strategy share
    wallet_cap = w_liquid * 0.15                          # up to 15% wallet liquid
    return round(max(0.0, min(reserve_cap, wallet_cap)), 4)


def _open_position(state, coin, strategy):
    budget = _budget_for_coin(state, coin, strategy)
    if budget < 25.0:
        state["stats"]["budget_rejections"] = state["stats"].get("budget_rejections", 0) + 1
        return False

    reserve = state["reserve"]
    wallet = state["wallets"][coin["network"]]
    px = float(coin["price_usd"])
    qty = budget / px if px > 0 else 0
    if qty <= 0:
        return False

    reserve["liquid_usd"] = round(reserve["liquid_usd"] - budget, 6)
    reserve["allocated_usd"] = round(reserve["allocated_usd"] + budget, 6)
    wallet["reserve_usd"] = round(wallet["reserve_usd"] - budget, 6)
    wallet["allocated_usd"] = round(wallet["allocated_usd"] + budget, 6)

    state["positions"].append({
        "id": _position_id(coin["symbol"], strategy),
        "symbol": coin["symbol"],
        "network": coin["network"],
        "strategy": strategy,
        "side": "LONG",
        "entry_price_usd": px,
        "qty": round(qty, 10),
        "entry_notional_usd": round(budget, 6),
        "opened_at": _utc_now(),
        "open_cycle": state["stats"]["cycles"],
    })
    coin["open_positions"] = coin.get("open_positions", 0) + 1
    coin["trades"] = coin.get("trades", 0) + 1
    return True


def _close_if_profitable(state, coin):
    reserve = state["reserve"]
    wallet = state["wallets"][coin["network"]]
    price = float(coin["price_usd"])
    kept = []
    unrealized = 0.0
    closed = 0

    for pos in state["positions"]:
        if pos["symbol"] != coin["symbol"]:
            kept.append(pos)
            continue

        entry = float(pos["entry_price_usd"])
        qty = float(pos["qty"])
        notional = float(pos["entry_notional_usd"])
        gross = qty * price
        cost = notional * (FEE_PCT + SLIPPAGE_PCT) + gross * (FEE_PCT + SLIPPAGE_PCT)
        net = gross - cost
        pnl = net - notional
        pnl_pct = pnl / notional if notional > 0 else 0.0

        if pnl_pct >= MIN_NET_PROFIT_PCT:
            reserve["allocated_usd"] = round(max(0.0, reserve["allocated_usd"] - notional), 6)
            reserve["liquid_usd"] = round(reserve["liquid_usd"] + notional + pnl, 6)
            reserve["realized_pnl_usd"] = round(reserve["realized_pnl_usd"] + pnl, 6)

            wallet["allocated_usd"] = round(max(0.0, wallet["allocated_usd"] - notional), 6)
            wallet["reserve_usd"] = round(wallet["reserve_usd"] + notional + pnl, 6)
            wallet["realized_pnl_usd"] = round(wallet["realized_pnl_usd"] + pnl, 6)

            coin["realized_pnl_usd"] = round(float(coin.get("realized_pnl_usd", 0.0)) + pnl, 6)
            state["stats"]["closed_trades"] = state["stats"].get("closed_trades", 0) + 1
            state["stats"]["wins"] = state["stats"].get("wins", 0) + 1
            closed += 1
        else:
            # Strict mode: never realize losses. Keep position open and track unrealized.
            if pnl < 0:
                state["stats"]["blocked_losses"] = state["stats"].get("blocked_losses", 0) + 1
            unrealized += pnl
            kept.append(pos)

    state["positions"] = kept
    coin["open_positions"] = sum(1 for p in kept if p["symbol"] == coin["symbol"])
    coin["unrealized_pnl_usd"] = round(unrealized, 6)
    return closed


def _update_risk_lock(state):
    reserve = state["reserve"]
    equity = float(reserve.get("liquid_usd", 0)) + float(reserve.get("allocated_usd", 0))
    hwm = float(reserve.get("high_watermark_usd", equity or 1))
    if equity > hwm:
        hwm = equity
    dd = ((hwm - equity) / hwm) if hwm > 0 else 0.0
    reserve["high_watermark_usd"] = round(hwm, 6)
    reserve["max_drawdown_pct"] = round(max(dd * 100, float(reserve.get("max_drawdown_pct", 0.0))), 4)

    if dd >= 0.12:
        reserve["risk_lock"] = True
        reserve["lock_reason"] = f"drawdown {dd*100:.2f}% >= 12%"
    elif reserve.get("risk_lock") and dd <= 0.04:
        reserve["risk_lock"] = False
        reserve["lock_reason"] = ""


def _simulate_price(coin, anchor, cycle):
    prev = float(coin["price_usd"])
    coin["prev_price_usd"] = prev
    history = coin.get("history", [])
    prior = float(history[-2]["p"]) if len(history) > 1 else prev
    momentum = (prev - prior) / prior if prior > 0 else 0.0

    target = max(0.01, anchor * float(coin["peg_factor"]))
    mean_revert = (target - prev) / target if target > 0 else 0.0

    rng_seed = hash((coin["symbol"], cycle, int(anchor * 100)))
    rng = random.Random(rng_seed)
    shock = rng.gauss(0.0, float(coin["vol"]))

    drift = float(coin["drift"])
    move = drift + (0.25 * momentum) + (0.18 * mean_revert) + shock
    move = max(-0.18, min(0.18, move))
    new_px = max(0.01, prev * (1.0 + move))
    coin["price_usd"] = round(new_px, 6)
    return coin["price_usd"]


def _day_change_pct(coin):
    start = float(coin.get("day_start_price_usd", 0) or 0)
    now = float(coin.get("price_usd", 0) or 0)
    if start <= 0:
        return 0.0
    return round(((now - start) / start) * 100.0, 4)


def _refresh_summary(state):
    reserve = state["reserve"]
    coins = state["coins"]
    wallets = state["wallets"]

    total_unrealized = sum(float(c.get("unrealized_pnl_usd", 0.0)) for c in coins)
    total_coin_realized = sum(float(c.get("realized_pnl_usd", 0.0)) for c in coins)
    open_positions = sum(int(c.get("open_positions", 0)) for c in coins)

    wallet_rows = []
    for name in ("ethereum", "solana", "bitcoin"):
        w = wallets.get(name, {})
        wallet_rows.append({
            "network": name,
            "address": w.get("address", ""),
            "reserve_usd": round(float(w.get("reserve_usd", 0.0)), 4),
            "allocated_usd": round(float(w.get("allocated_usd", 0.0)), 4),
            "realized_pnl_usd": round(float(w.get("realized_pnl_usd", 0.0)), 4),
        })

    pool_rows = []
    for c in coins:
        pool_rows.append({
            "symbol": c["symbol"],
            "network": c["network"],
            "anchor_pair": c["anchor_pair"],
            "price_usd": float(c["price_usd"]),
            "day_change_pct": _day_change_pct(c),
            "open_positions": int(c.get("open_positions", 0)),
            "unrealized_pnl_usd": float(c.get("unrealized_pnl_usd", 0.0)),
            "realized_pnl_usd": float(c.get("realized_pnl_usd", 0.0)),
            "trades": int(c.get("trades", 0)),
        })

    pool_rows.sort(key=lambda x: x["symbol"])
    summary = {
        "updated_at": _utc_now(),
        "network_name": state.get("network", {}).get("name", "AmiCoin Network"),
        "network_mode": state.get("network", {}).get("mode", "simulation_only"),
        "excluded_from_real_holdings": bool(state.get("network", {}).get("excluded_from_real_holdings", True)),
        "counts_toward_portfolio": bool(state.get("network", {}).get("counts_toward_portfolio", False)),
        "go_live_ready": bool(state.get("network", {}).get("go_live_ready", False)),
        "go_live_checklist": state.get("go_live_checklist", {}),
        "reserve_symbol": "AMIR",
        "liquid_usd": round(float(reserve.get("liquid_usd", 0.0)), 4),
        "allocated_usd": round(float(reserve.get("allocated_usd", 0.0)), 4),
        "reserve_equity_usd": round(float(reserve.get("liquid_usd", 0.0)) + float(reserve.get("allocated_usd", 0.0),), 4),
        "realized_pnl_usd": round(float(reserve.get("realized_pnl_usd", 0.0)), 4),
        "coin_realized_pnl_usd": round(total_coin_realized, 4),
        "unrealized_pnl_usd": round(total_unrealized, 4),
        "open_positions": open_positions,
        "coins_total": len(pool_rows),
        "risk_lock": bool(reserve.get("risk_lock", False)),
        "lock_reason": reserve.get("lock_reason", ""),
        "max_drawdown_pct": round(float(reserve.get("max_drawdown_pct", 0.0)), 4),
        "cycles": int(state["stats"].get("cycles", 0)),
        "closed_trades": int(state["stats"].get("closed_trades", 0)),
        "wins": int(state["stats"].get("wins", 0)),
        "blocked_losses": int(state["stats"].get("blocked_losses", 0)),
    }
    state["summary"] = summary
    return summary, pool_rows, wallet_rows


def run_cycle(state=None, persist=True):
    """Run one AmiCoin simulation cycle and optionally persist state."""
    state = state or load_state()
    state["stats"]["cycles"] = int(state["stats"].get("cycles", 0)) + 1
    cycle = state["stats"]["cycles"]
    _sync_go_live_flags(state)
    anchors = _anchor_prices()

    # Simulate price update + strategy actions per coin
    for coin in state["coins"]:
        anchor = anchors.get(coin["anchor_pair"], DEFAULT_ANCHORS[coin["anchor_pair"]])
        _simulate_price(coin, anchor, cycle)
        coin["history"].append({"t": int(time.time()), "p": coin["price_usd"]})
        if len(coin["history"]) > 400:
            coin["history"] = coin["history"][-400:]

        _close_if_profitable(state, coin)

        signals = _coin_signal(coin)
        for strategy in coin.get("strategies", []):
            if signals.get(strategy):
                exists = any(p["id"] == _position_id(coin["symbol"], strategy) for p in state["positions"])
                if not exists:
                    _open_position(state, coin, strategy)

    _update_risk_lock(state)
    summary, pool_rows, wallet_rows = _refresh_summary(state)

    if persist:
        save_state(state)

    return {
        "summary": summary,
        "pool": pool_rows,
        "wallets": wallet_rows,
        "positions": len(state.get("positions", [])),
        "updated_at": state.get("updated_at"),
    }


def get_snapshot():
    """Return latest AmiCoin snapshot without mutating state."""
    state = load_state()
    _sync_go_live_flags(state)
    summary, pool_rows, wallet_rows = _refresh_summary(state)
    return {
        "summary": summary,
        "pool": pool_rows,
        "wallets": wallet_rows,
        "positions": len(state.get("positions", [])),
        "updated_at": state.get("updated_at"),
    }
