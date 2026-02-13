#!/usr/bin/env python3
"""Quant 100 Runner — generate and validate 100 market expansion experiments.

What this does:
  1. Builds exactly 100 strategy experiments (parameterized variants).
  2. Tags each with target markets + server regions (latency-aware deployment hints).
  3. Backtests each against historical data.
  4. Pushes profitable, risk-compliant variants into Strategy Pipeline (COLD->WARM).
  5. Produces budget recommendations for promoted candidates.

Outputs:
  - agents/quant_100_plan.json
  - agents/quant_100_results.json
"""

import itertools
import json
import logging
import os
import random
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from strategy_pipeline import (  # noqa: E402
    STRICT_PROFIT_ONLY,
    AccumulateAndHoldStrategy,
    Backtester,
    DipBuyerStrategy,
    HistoricalPrices,
    MeanReversionStrategy,
    MomentumStrategy,
    MultiTimeframeStrategy,
    RSIStrategy,
    StrategyValidator,
    VWAPStrategy,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [quant_100] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("quant_100")

BASE_DIR = Path(__file__).parent
PLAN_FILE = BASE_DIR / "quant_100_plan.json"
RESULTS_FILE = BASE_DIR / "quant_100_results.json"
BATCH_CONFIG_FILE = BASE_DIR / "growth_batch_config.json"
MIN_CANDLES_REQUIRED = int(os.environ.get("QUANT100_MIN_CANDLES", "30"))
REJECT_ON_DATA_GAP = os.environ.get("QUANT100_REJECT_ON_DATA_GAP", "1").lower() not in (
    "0", "false", "no"
)
WALKFORWARD_SPLIT_RATIO = float(os.environ.get("QUANT100_WALKFORWARD_SPLIT_RATIO", "0.55"))
WALKFORWARD_MIN_TOTAL_CANDLES = int(os.environ.get("QUANT100_WALKFORWARD_MIN_TOTAL_CANDLES", "60"))
WALKFORWARD_MIN_OOS_CANDLES = int(os.environ.get("QUANT100_WALKFORWARD_MIN_OOS_CANDLES", "24"))
BACKTEST_HOURS = int(os.environ.get("QUANT100_BACKTEST_HOURS", "168"))
BACKTEST_GRANULARITY = os.environ.get("QUANT100_GRANULARITY", "5min").lower()
MARKET_PREFILTER_MIN_CANDLES = int(
    os.environ.get("QUANT100_MARKET_PREFILTER_MIN_CANDLES", str(MIN_CANDLES_REQUIRED))
)
UNIQUE_STRATEGY_NAMES = os.environ.get("QUANT100_UNIQUE_NAMES", "0").lower() not in (
    "0", "false", "no"
)
RUN_TAG_ENV = os.environ.get("QUANT100_RUN_TAG", "").strip()
ADAPTIVE_STRATEGY_SPLIT = os.environ.get("QUANT100_ADAPTIVE_STRATEGY_SPLIT", "1").lower() not in (
    "0", "false", "no"
)
ADAPTIVE_MIN_PER_STRATEGY = int(os.environ.get("QUANT100_ADAPTIVE_MIN_PER_STRATEGY", "8"))
RETUNE_LOW_ACTIVITY = os.environ.get("QUANT100_RETUNE_LOW_ACTIVITY", "1").lower() not in (
    "0", "false", "no"
)
RETUNE_MIN_TRADES = int(os.environ.get("QUANT100_RETUNE_MIN_TRADES", "2"))

# Use the same server topology already used by the platform.
SERVER_REGIONS = [
    ("ewr", "US-East / Coinbase core"),
    ("ord", "CME / derivatives proximity"),
    ("lhr", "EU liquidity overlap"),
    ("fra", "EU low-latency routing"),
    ("sin", "Asia night session"),
    ("nrt", "Japan open overlap"),
    ("bom", "India / MENA overlap"),
]

# Market expansion universe (Coinbase spot-first).
MARKET_UNIVERSE = [
    {"pair": "BTC-USD", "market": "crypto_major", "vol_bucket": "high_liquidity"},
    {"pair": "ETH-USD", "market": "crypto_major", "vol_bucket": "high_liquidity"},
    {"pair": "SOL-USD", "market": "crypto_major", "vol_bucket": "high_liquidity"},
    {"pair": "AVAX-USD", "market": "crypto_alt", "vol_bucket": "medium_liquidity"},
    {"pair": "LINK-USD", "market": "crypto_alt", "vol_bucket": "medium_liquidity"},
    {"pair": "DOGE-USD", "market": "crypto_alt", "vol_bucket": "high_beta"},
    {"pair": "XRP-USD", "market": "crypto_alt", "vol_bucket": "high_beta"},
    {"pair": "ADA-USD", "market": "crypto_alt", "vol_bucket": "medium_liquidity"},
    {"pair": "LTC-USD", "market": "crypto_alt", "vol_bucket": "medium_liquidity"},
    {"pair": "BCH-USD", "market": "crypto_alt", "vol_bucket": "medium_liquidity"},
]


def _fetch_candles(prices, pair, hours, granularity):
    granularity = granularity.lower()
    granularity_seconds = "300" if granularity == "5min" else "3600"
    if granularity == "5min":
        candles = prices.get_5min_candles(pair, hours=hours)
    else:
        candles = prices.get_candles(pair, hours=hours)
    source_meta = prices.get_cache_meta(pair, granularity_seconds, hours)
    return candles, source_meta


def discover_market_universe(prices, hours, granularity):
    """Select a data-ready market universe for this run."""
    ready = []
    deferred = []
    pair_cache = {}

    for market in MARKET_UNIVERSE:
        pair = market["pair"]
        candles, source_meta = _fetch_candles(prices, pair, hours=hours, granularity=granularity)
        pair_cache[pair] = candles
        meta = {
            "pair": pair,
            "market": market["market"],
            "vol_bucket": market["vol_bucket"],
            "candle_count": len(candles),
            "data_source": source_meta,
        }
        if len(candles) >= MARKET_PREFILTER_MIN_CANDLES:
            ready.append(meta)
        else:
            deferred.append(meta)

    ready.sort(key=lambda item: item["candle_count"], reverse=True)
    deferred.sort(key=lambda item: item["candle_count"], reverse=True)

    selected = list(ready)
    if len(selected) < 3:
        for item in deferred:
            if item["candle_count"] <= 0:
                continue
            selected.append(item)
            if len(selected) >= 3:
                break

    if not selected:
        selected = [
            {
                "pair": m["pair"],
                "market": m["market"],
                "vol_bucket": m["vol_bucket"],
                "candle_count": len(pair_cache.get(m["pair"], [])),
                "data_source": {},
            }
            for m in MARKET_UNIVERSE
        ]

    market_universe = [
        {
            "pair": item["pair"],
            "market": item["market"],
            "vol_bucket": item["vol_bucket"],
        }
        for item in selected
    ]

    return market_universe, pair_cache, {"ready": ready, "deferred": deferred, "selected": selected}


def _load_batch_config():
    if not BATCH_CONFIG_FILE.exists():
        return {}
    try:
        payload = json.loads(BATCH_CONFIG_FILE.read_text())
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {}


def _strategy_specs(count_overrides=None):
    """Return per-strategy parameter grids and target counts (sum=100)."""
    specs = [
        {
            "base_name": "mean_reversion",
            "count": 15,
            "factory": MeanReversionStrategy,
            "grid": {
                "window": [12, 16, 20, 24, 28],
                "num_std": [1.6, 2.0, 2.4],
            },
        },
        {
            "base_name": "momentum",
            "count": 20,
            "factory": MomentumStrategy,
            "grid": {
                "short_window": [3, 5, 8, 10],
                "long_window": [15, 20, 30],
                "volume_mult": [1.2, 1.5],
            },
        },
        {
            "base_name": "rsi",
            "count": 15,
            "factory": RSIStrategy,
            "grid": {
                "period": [10, 14, 21],
                "oversold": [25, 30, 35],
                "overbought": [65, 70, 75],
            },
        },
        {
            "base_name": "vwap",
            "count": 10,
            "factory": VWAPStrategy,
            "grid": {"deviation_pct": [0.003, 0.004, 0.005, 0.006, 0.007]},
        },
        {
            "base_name": "dip_buyer",
            "count": 15,
            "factory": DipBuyerStrategy,
            "grid": {
                "dip_threshold": [0.010, 0.015, 0.020, 0.025, 0.030],
                "recovery_target": [0.015, 0.020, 0.025],
                "lookback": [3, 4, 6],
            },
        },
        {
            "base_name": "multi_timeframe",
            "count": 15,
            "factory": MultiTimeframeStrategy,
            "grid": {
                "rsi_period": [10, 14, 21],
                "bb_window": [16, 20, 24],
                "bb_std": [1.8, 2.0, 2.2],
            },
        },
        {
            "base_name": "accumulate_hold",
            "count": 10,
            "factory": AccumulateAndHoldStrategy,
            "grid": {
                "dca_interval": [6, 12, 18, 24],
                "dip_threshold": [0.015, 0.020, 0.025],
                "profit_target": [0.015, 0.020, 0.030],
            },
        },
    ]
    overrides = count_overrides or {}
    if overrides:
        for spec in specs:
            base = spec["base_name"]
            if base in overrides:
                try:
                    spec["count"] = max(1, int(overrides[base]))
                except Exception:
                    pass
    total = sum(int(spec.get("count", 0) or 0) for spec in specs)
    if total != 100 and total > 0:
        factor = 100.0 / float(total)
        for spec in specs:
            spec["count"] = max(1, int(round(spec["count"] * factor)))
        # Fix any rounding drift to exactly 100.
        while sum(s["count"] for s in specs) > 100:
            for spec in sorted(specs, key=lambda s: s["count"], reverse=True):
                if sum(s["count"] for s in specs) <= 100:
                    break
                if spec["count"] > 1:
                    spec["count"] -= 1
        while sum(s["count"] for s in specs) < 100:
            for spec in sorted(specs, key=lambda s: s["count"]):
                if sum(s["count"] for s in specs) >= 100:
                    break
                spec["count"] += 1
    return specs


def _derive_adaptive_strategy_overrides():
    if not ADAPTIVE_STRATEGY_SPLIT or not RESULTS_FILE.exists():
        return {}

    try:
        payload = json.loads(RESULTS_FILE.read_text())
    except Exception:
        return {}

    rows = payload.get("results", [])
    if not isinstance(rows, list) or not rows:
        return {}

    bases = [spec["base_name"] for spec in _strategy_specs()]
    stats = {
        base: {"total": 0, "promoted": 0, "positive": 0, "strict_positive": 0}
        for base in bases
    }

    for row in rows:
        if not isinstance(row, dict):
            continue
        base = str(row.get("strategy_base", ""))
        if base not in stats:
            continue
        stats[base]["total"] += 1
        status = str(row.get("status", ""))
        if status in {
            "promoted_warm",
            "retained_warm",
            "retained_hot",
            "retained_warm_passed",
            "retained_hot_passed",
            "passed_warm",
            "passed_hot",
        }:
            stats[base]["promoted"] += 1
        metrics = row.get("metrics", {}) if isinstance(row.get("metrics"), dict) else {}
        ret = float(metrics.get("return_pct", 0.0) or 0.0)
        losses = int(metrics.get("losses", 0) or 0)
        trades = int(metrics.get("trades", 0) or 0)
        if ret > 0:
            stats[base]["positive"] += 1
        if ret > 0 and losses <= 0 and trades >= max(1, RETUNE_MIN_TRADES):
            stats[base]["strict_positive"] += 1

    if not any(v["total"] > 0 for v in stats.values()):
        return {}

    min_per = max(1, int(ADAPTIVE_MIN_PER_STRATEGY))
    if min_per * len(bases) > 100:
        min_per = max(1, 100 // max(1, len(bases)))

    weights = {}
    for base in bases:
        total = max(1, int(stats[base]["total"] or 0))
        promoted_rate = float(stats[base]["promoted"]) / total
        positive_rate = float(stats[base]["positive"]) / total
        strict_positive_rate = float(stats[base]["strict_positive"]) / total
        weights[base] = (
            0.12
            + promoted_rate * 0.60
            + strict_positive_rate * 0.20
            + positive_rate * 0.08
        )

    counts = {base: min_per for base in bases}
    remaining = max(0, 100 - sum(counts.values()))
    total_weight = sum(max(1e-6, weights[base]) for base in bases)
    for base in bases:
        share = (max(1e-6, weights[base]) / total_weight) * remaining
        counts[base] += int(share)

    while sum(counts.values()) < 100:
        base = max(bases, key=lambda b: (weights[b], -counts[b]))
        counts[base] += 1
    while sum(counts.values()) > 100:
        base = min(
            (b for b in bases if counts[b] > min_per),
            key=lambda b: (weights[b], counts[b]),
            default=None,
        )
        if base is None:
            break
        counts[base] -= 1

    return counts


def _grid_combinations(grid):
    keys = list(grid.keys())
    value_lists = [grid[k] for k in keys]
    for values in itertools.product(*value_lists):
        yield {k: v for k, v in zip(keys, values)}


def _normalize_tag(value):
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in raw)
    return cleaned.strip("_")


def _resolve_run_tag(batch_config):
    configured = _normalize_tag(RUN_TAG_ENV or batch_config.get("run_tag"))
    if configured:
        return configured
    if UNIQUE_STRATEGY_NAMES:
        return datetime.now(timezone.utc).strftime("r%Y%m%d%H%M%S")
    return _normalize_tag(batch_config.get("active_batch_id"))


def build_100_experiments(priority_pairs=None, market_universe=None, strategy_count_overrides=None, run_tag=""):
    """Build exactly 100 experiments with region + market assignments."""
    priority_pairs = [str(p).upper() for p in (priority_pairs or []) if p]
    market_base = market_universe or MARKET_UNIVERSE

    # Stable reorder: prioritized pairs first, then original ordering.
    indexed_markets = list(enumerate(market_base))

    def _market_key(item):
        idx, market = item
        pair = market["pair"].upper()
        if pair in priority_pairs:
            return (0, priority_pairs.index(pair), idx)
        return (1, 9999, idx)

    market_universe = [m for _, m in sorted(indexed_markets, key=_market_key)]

    experiments = []
    idx = 1
    market_idx = 0
    region_idx = 0

    for spec in _strategy_specs(count_overrides=strategy_count_overrides):
        combos = list(_grid_combinations(spec["grid"]))
        if not combos:
            continue
        for i in range(spec["count"]):
            params = combos[i % len(combos)]
            market = market_universe[market_idx % len(market_universe)]
            region = SERVER_REGIONS[region_idx % len(SERVER_REGIONS)]
            strategy_name_core = f"{spec['base_name']}_q100_{idx:03d}"
            strategy_name = strategy_name_core if not run_tag else f"{strategy_name_core}_{run_tag}"
            exp_id = f"Q100-{idx:03d}" if not run_tag else f"Q100-{run_tag}-{idx:03d}"

            exp = {
                "id": exp_id,
                "strategy_base": spec["base_name"],
                "strategy_name_core": strategy_name_core,
                "strategy_name": strategy_name,
                "params": params,
                "pair": market["pair"],
                "market": market["market"],
                "vol_bucket": market["vol_bucket"],
                "target_region": region[0],
                "region_rationale": region[1],
                "run_tag": run_tag,
                "status": "planned",
            }
            experiments.append(exp)
            idx += 1
            market_idx += 1
            region_idx += 1

    experiments = experiments[:100]
    if len(experiments) != 100:
        raise RuntimeError(f"Expected exactly 100 experiments, built {len(experiments)}")
    return experiments


def _recommend_budget(result):
    """Convert performance metrics to initial per-trade budget."""
    ret = float(result.get("total_return_pct", 0))
    sharpe = float(result.get("sharpe_ratio", 0))
    dd = float(result.get("max_drawdown_pct", 100))
    win = float(result.get("win_rate", 0))

    score = 0.0
    score += max(0.0, min(2.0, ret / 2.0))
    score += max(0.0, min(2.0, sharpe / 1.5))
    score += max(0.0, min(1.5, (win - 0.5) * 5.0))
    score += max(0.0, min(1.5, max(0.0, 5.0 - dd) / 5.0))

    # Keep within your current max-trade rule ($5).
    starter_budget = round(min(5.0, max(1.0, 1.0 + score * 0.8)), 2)
    if starter_budget <= 2.0:
        tier = "nano"
    elif starter_budget <= 3.5:
        tier = "small"
    else:
        tier = "full"
    return {"starter_budget_usd": starter_budget, "tier": tier}


def _instantiate_strategy(base_name, params):
    if base_name == "mean_reversion":
        return MeanReversionStrategy(**params)
    if base_name == "momentum":
        return MomentumStrategy(**params)
    if base_name == "rsi":
        return RSIStrategy(**params)
    if base_name == "vwap":
        return VWAPStrategy(**params)
    if base_name == "dip_buyer":
        return DipBuyerStrategy(**params)
    if base_name == "multi_timeframe":
        return MultiTimeframeStrategy(**params)
    if base_name == "accumulate_hold":
        return AccumulateAndHoldStrategy(**params)
    raise ValueError(f"Unknown strategy base: {base_name}")


def _walkforward_split_index(candle_count):
    if candle_count < WALKFORWARD_MIN_TOTAL_CANDLES:
        return None
    split = int(candle_count * WALKFORWARD_SPLIT_RATIO)
    split = max(30, min(split, candle_count - WALKFORWARD_MIN_OOS_CANDLES))
    if split <= 0 or candle_count - split < WALKFORWARD_MIN_OOS_CANDLES:
        return None
    return split


def _compact_bt(bt):
    return {
        "candle_count": int(bt.get("candle_count", 0) or 0),
        "total_return_pct": float(bt.get("total_return_pct", 0.0) or 0.0),
        "total_trades": int(bt.get("total_trades", 0) or 0),
        "win_rate": float(bt.get("win_rate", 0.0) or 0.0),
        "losses": int(bt.get("losses", 0) or 0),
        "max_drawdown_pct": float(bt.get("max_drawdown_pct", 0.0) or 0.0),
        "sharpe_ratio": float(bt.get("sharpe_ratio", 0.0) or 0.0),
        "final_open_position_blocked": bool(bt.get("final_open_position_blocked", False)),
    }


def _score_backtest(bt):
    return (
        float(bt.get("total_return_pct", 0.0) or 0.0)
        + float(bt.get("win_rate", 0.0) or 0.0) * 10.0
        + min(12, int(bt.get("total_trades", 0) or 0)) * 0.10
        - float(bt.get("max_drawdown_pct", 0.0) or 0.0) * 0.35
    )


def _retune_candidates(base_name, params):
    p = dict(params or {})
    out = []
    if base_name == "mean_reversion":
        out.append({**p, "num_std": max(1.1, float(p.get("num_std", 2.0)) - 0.3)})
        out.append({**p, "window": max(8, int(p.get("window", 20)) - 4)})
    elif base_name == "momentum":
        out.append({**p, "volume_mult": max(1.0, float(p.get("volume_mult", 1.5)) - 0.2)})
        out.append({
            **p,
            "short_window": max(2, int(p.get("short_window", 5)) - 1),
            "long_window": max(8, int(p.get("long_window", 20)) - 4),
        })
    elif base_name == "rsi":
        out.append({
            **p,
            "oversold": min(45, int(p.get("oversold", 30)) + 5),
            "overbought": max(55, int(p.get("overbought", 70)) - 5),
        })
        out.append({**p, "period": max(7, int(p.get("period", 14)) - 3)})
    elif base_name == "vwap":
        out.append({**p, "deviation_pct": max(0.0015, float(p.get("deviation_pct", 0.005)) * 0.75)})
    elif base_name == "dip_buyer":
        out.append({
            **p,
            "dip_threshold": max(0.006, float(p.get("dip_threshold", 0.015)) * 0.70),
            "recovery_target": max(0.008, float(p.get("recovery_target", 0.02)) * 0.85),
        })
        out.append({**p, "lookback": max(2, int(p.get("lookback", 4)) - 1)})
    elif base_name == "multi_timeframe":
        out.append({
            **p,
            "bb_std": max(1.4, float(p.get("bb_std", 2.0)) - 0.25),
            "rsi_period": max(8, int(p.get("rsi_period", 14)) - 2),
        })
    elif base_name == "accumulate_hold":
        out.append({
            **p,
            "dca_interval": max(3, int(p.get("dca_interval", 12)) // 2),
            "profit_target": max(0.010, float(p.get("profit_target", 0.02)) * 0.80),
            "dip_threshold": max(0.010, float(p.get("dip_threshold", 0.02)) * 0.80),
        })

    unique = []
    seen = set()
    for item in out:
        key = tuple(sorted(item.items()))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique[:4]


def _maybe_retune_strategy(exp, candles, backtester, baseline_bt):
    if not RETUNE_LOW_ACTIVITY or int(baseline_bt.get("total_trades", 0) or 0) >= RETUNE_MIN_TRADES:
        return None

    best_bt = baseline_bt
    best_params = dict(exp["params"])
    best_score = _score_backtest(baseline_bt)
    improved = False

    for candidate in _retune_candidates(exp["strategy_base"], exp["params"]):
        candidate_strategy = _instantiate_strategy(exp["strategy_base"], candidate)
        candidate_strategy.name = exp["strategy_name"]
        candidate_bt = backtester.run(candidate_strategy, candles, exp["pair"])
        candidate_score = _score_backtest(candidate_bt)
        if candidate_score > best_score and int(candidate_bt.get("total_trades", 0) or 0) >= int(best_bt.get("total_trades", 0) or 0):
            best_bt = candidate_bt
            best_params = candidate
            best_score = candidate_score
            improved = True

    if not improved:
        return None
    return {"params": best_params, "backtest": best_bt}


def run_experiments(experiments, hours=BACKTEST_HOURS, granularity=BACKTEST_GRANULARITY, prices=None, pair_cache=None):
    """Run backtests and register passing strategies into pipeline."""
    prices = prices or HistoricalPrices()
    backtester = Backtester(initial_capital=100.0)
    validator = StrategyValidator()

    pair_cache = dict(pair_cache or {})
    results = []

    for exp in experiments:
        pair = exp["pair"]
        prior_stage_row = validator.db.execute(
            "SELECT stage FROM strategy_registry WHERE name=? AND pair=?",
            (exp["strategy_name"], pair),
        ).fetchone()
        prior_stage = str(prior_stage_row["stage"] if prior_stage_row and prior_stage_row["stage"] else "COLD").upper()

        candles = pair_cache.get(pair)
        if candles is None:
            candles, _ = _fetch_candles(prices, pair, hours=hours, granularity=granularity)
            pair_cache[pair] = candles
        granularity_seconds = "300" if granularity == "5min" else "3600"
        source_meta = prices.get_cache_meta(pair, granularity_seconds, hours)

        strategy = _instantiate_strategy(exp["strategy_base"], exp["params"])
        strategy.name = exp["strategy_name"]
        validator.register_strategy(strategy, pair, params=exp["params"])

        if len(candles) < MIN_CANDLES_REQUIRED:
            fallback_bt = {
                "strategy": strategy.name,
                "pair": pair,
                "candle_count": len(candles),
                "initial_capital": 100.0,
                "final_capital": 100.0,
                "total_return_pct": 0.0,
                "total_trades": 0,
                "buy_trades": 0,
                "sell_trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "max_drawdown_pct": 0.0,
                "sharpe_ratio": 0.0,
                "total_pnl": 0.0,
                "trades": [],
                "equity_curve_start": 100.0,
                "equity_curve_end": 100.0,
                "final_open_position_blocked": False,
                "final_unrealized_pnl": 0.0,
                "walkforward": {
                    "enabled": True,
                    "available": False,
                    "reason": "insufficient_candles_for_walkforward",
                },
            }
            passed, gate_msg = validator.submit_backtest(strategy.name, pair, fallback_bt)
            status = "rejected_cold" if REJECT_ON_DATA_GAP else "no_data"
            budget_profile = validator.get_budget_profile(strategy.name, pair)
            rec = dict(exp)
            rec.update({
                "status": status,
                "reason": f"insufficient_candles {len(candles)} < {MIN_CANDLES_REQUIRED}",
                "gate_message": gate_msg,
                "strict_profit_only": STRICT_PROFIT_ONLY,
                "data_source": source_meta,
                "metrics": {
                    "candle_count": len(candles),
                    "return_pct": 0.0,
                    "win_rate": 0.0,
                    "trades": 0,
                    "losses": 0,
                    "drawdown_pct": 0.0,
                    "sharpe": 0.0,
                    "final_open_position_blocked": False,
                },
                "budget": {
                    "starter_budget_usd": 0.0,
                    "tier": budget_profile.get("risk_tier", "blocked"),
                    "max_budget_usd": 0.0,
                },
                "data_quality_reject": True,
                "validator_passed": bool(passed),
                "walkforward": fallback_bt["walkforward"],
            })
            results.append(rec)
            logger.warning("%s %s %s (%s)", exp["id"], pair, status, rec["reason"])
            continue

        bt = backtester.run(strategy, candles, pair)
        retune = _maybe_retune_strategy(exp, candles, backtester, bt)
        retuned_params = None
        if retune:
            bt = retune["backtest"]
            retuned_params = retune["params"]
            validator.update_strategy_params(strategy.name, pair, retuned_params)

        params_for_walkforward = retuned_params or exp["params"]
        split_idx = _walkforward_split_index(len(candles))
        walkforward = {
            "enabled": True,
            "available": False,
            "reason": "insufficient_candles_for_walkforward",
        }
        if split_idx is not None:
            candles_is = candles[:split_idx]
            candles_oos = candles[split_idx:]
            bt_is = backtester.run(_instantiate_strategy(exp["strategy_base"], params_for_walkforward), candles_is, pair)
            bt_oos = backtester.run(_instantiate_strategy(exp["strategy_base"], params_for_walkforward), candles_oos, pair)
            walkforward = {
                "enabled": True,
                "available": True,
                "split_index": split_idx,
                "in_sample_candles": len(candles_is),
                "out_of_sample_candles": len(candles_oos),
                "in_sample": _compact_bt(bt_is),
                "out_of_sample": _compact_bt(bt_oos),
            }

        bt_for_gate = dict(bt)
        bt_for_gate["walkforward"] = walkforward
        passed, gate_msg = validator.submit_backtest(strategy.name, pair, bt_for_gate)
        budget_profile = validator.get_budget_profile(strategy.name, pair)
        post_stage_row = validator.db.execute(
            "SELECT stage FROM strategy_registry WHERE name=? AND pair=?",
            (exp["strategy_name"], pair),
        ).fetchone()
        post_stage = str(post_stage_row["stage"] if post_stage_row and post_stage_row["stage"] else prior_stage).upper()

        if passed:
            budget = {
                "starter_budget_usd": round(float(budget_profile.get("starter_budget_usd", 0.0) or 0.0), 2),
                "tier": budget_profile.get("risk_tier", "standard"),
                "max_budget_usd": round(float(budget_profile.get("max_budget_usd", 0.0) or 0.0), 2),
            }
        else:
            budget = {"starter_budget_usd": 0.0, "tier": "none", "max_budget_usd": 0.0}
        if prior_stage in {"WARM", "HOT"} and post_stage == prior_stage:
            status = (
                f"retained_{prior_stage.lower()}_passed"
                if passed
                else f"retained_{prior_stage.lower()}_failed"
            )
        elif passed and post_stage == "WARM":
            status = "promoted_warm"
        elif passed:
            status = f"passed_{post_stage.lower()}"
        else:
            status = "rejected_cold"

        rec = dict(exp)
        rec.update({
            "status": status,
            "gate_message": gate_msg,
            "strict_profit_only": STRICT_PROFIT_ONLY,
            "data_source": source_meta,
            "retuned": bool(retune),
            "retuned_params": retuned_params,
            "metrics": {
                "candle_count": bt.get("candle_count", len(candles)),
                "return_pct": bt["total_return_pct"],
                "win_rate": bt["win_rate"],
                "trades": bt["total_trades"],
                "losses": bt["losses"],
                "drawdown_pct": bt["max_drawdown_pct"],
                "sharpe": bt["sharpe_ratio"],
                "final_open_position_blocked": bt.get("final_open_position_blocked", False),
            },
            "walkforward": walkforward,
            "budget": budget,
            "risk_profile": budget_profile,
            "prior_stage": prior_stage,
            "post_stage": post_stage,
        })
        results.append(rec)

        logger.info(
            "%s %s %s ret=%+.2f%% wr=%.1f%% dd=%.2f%% sharpe=%.2f budget=$%.2f tier=%s",
            exp["id"],
            pair,
            status,
            bt["total_return_pct"],
            bt["win_rate"] * 100,
            bt["max_drawdown_pct"],
            bt["sharpe_ratio"],
            budget["starter_budget_usd"],
            budget["tier"],
        )

    return results


def summarize(results):
    promoted = [r for r in results if r.get("status") == "promoted_warm"]
    rejected = [r for r in results if r.get("status") == "rejected_cold"]
    no_data = [r for r in results if r.get("status") == "no_data"]
    retained_warm = [r for r in results if str(r.get("status", "")).startswith("retained_warm")]
    retained_hot = [r for r in results if str(r.get("status", "")).startswith("retained_hot")]
    retained_warm_passed = [r for r in results if r.get("status") == "retained_warm_passed"]
    retained_warm_failed = [r for r in results if r.get("status") == "retained_warm_failed"]
    retained_hot_passed = [r for r in results if r.get("status") == "retained_hot_passed"]
    retained_hot_failed = [r for r in results if r.get("status") == "retained_hot_failed"]
    wf_available = [r for r in results if isinstance(r.get("walkforward"), dict) and r["walkforward"].get("available")]
    wf_positive_oos = [
        r for r in wf_available
        if (r.get("walkforward", {}).get("out_of_sample", {}).get("total_return_pct", 0) or 0) > 0
    ]
    rejection_reasons = {}
    for r in rejected:
        reason = r.get("reason") or r.get("gate_message") or "unknown"
        rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1

    top = sorted(
        promoted,
        key=lambda r: (
            r["metrics"]["sharpe"],
            r["metrics"]["return_pct"],
            -r["metrics"]["drawdown_pct"],
        ),
        reverse=True,
    )[:20]

    budgets = [r["budget"]["starter_budget_usd"] for r in promoted if r.get("budget")]
    avg_budget = round(statistics.mean(budgets), 2) if budgets else 0.0
    total_budget = round(sum(budgets), 2) if budgets else 0.0
    retuned = sum(1 for r in results if r.get("retuned"))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "strict_profit_only": STRICT_PROFIT_ONLY,
        "total": len(results),
        "promoted_warm": len(promoted),
        "rejected_cold": len(rejected),
        "no_data": len(no_data),
        "retained_warm": len(retained_warm),
        "retained_hot": len(retained_hot),
        "retained_warm_passed": len(retained_warm_passed),
        "retained_warm_failed": len(retained_warm_failed),
        "retained_hot_passed": len(retained_hot_passed),
        "retained_hot_failed": len(retained_hot_failed),
        "avg_starter_budget_usd": avg_budget,
        "total_starter_budget_usd": total_budget,
        "retuned_low_activity": retuned,
        "walkforward_available": len(wf_available),
        "walkforward_positive_oos": len(wf_positive_oos),
        "rejection_reasons_top": sorted(
            rejection_reasons.items(),
            key=lambda kv: kv[1],
            reverse=True,
        )[:15],
        "top_candidates": top,
    }


def save_json(path, payload):
    path.write_text(json.dumps(payload, indent=2))


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"

    if cmd == "status":
        if not RESULTS_FILE.exists():
            print("No results file yet. Run: python quant_100_runner.py run")
            return
        payload = json.loads(RESULTS_FILE.read_text())
        summary = payload.get("summary", {})
        print(f"Total: {summary.get('total', 0)}")
        print(f"Promoted WARM: {summary.get('promoted_warm', 0)}")
        print(f"Rejected COLD: {summary.get('rejected_cold', 0)}")
        print(f"No data: {summary.get('no_data', 0)}")
        print(f"Retained WARM: {summary.get('retained_warm', 0)}")
        print(f"Retained HOT: {summary.get('retained_hot', 0)}")
        print(f"Avg budget: ${summary.get('avg_starter_budget_usd', 0):.2f}")
        print(f"Total starter budget: ${summary.get('total_starter_budget_usd', 0):.2f}")
        print(f"Retuned low activity: {summary.get('retuned_low_activity', 0)}")
        return

    prices = HistoricalPrices()
    market_universe, discovered_pair_cache, market_discovery = discover_market_universe(
        prices, hours=BACKTEST_HOURS, granularity=BACKTEST_GRANULARITY
    )
    batch_config = _load_batch_config()
    run_tag = _resolve_run_tag(batch_config)
    discovered_priority_pairs = [m["pair"] for m in market_discovery.get("selected", [])]
    config_priority_pairs = [str(p).upper() for p in batch_config.get("priority_pairs", []) if p]
    merged_priority_pairs = []
    for pair in config_priority_pairs + discovered_priority_pairs:
        if pair not in merged_priority_pairs:
            merged_priority_pairs.append(pair)

    strategy_count_overrides = {}
    adaptive_strategy_overrides = {}
    if bool(batch_config.get("active")) and isinstance(batch_config.get("strategy_count_overrides"), dict):
        strategy_count_overrides = {
            str(k): int(v)
            for k, v in batch_config.get("strategy_count_overrides", {}).items()
            if isinstance(v, (int, float))
        }
    if not strategy_count_overrides:
        adaptive_strategy_overrides = _derive_adaptive_strategy_overrides()
        strategy_count_overrides = dict(adaptive_strategy_overrides)

    experiments = build_100_experiments(
        priority_pairs=merged_priority_pairs,
        market_universe=market_universe,
        strategy_count_overrides=strategy_count_overrides,
        run_tag=run_tag,
    )
    save_json(PLAN_FILE, {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "hours": BACKTEST_HOURS,
            "granularity": BACKTEST_GRANULARITY,
            "min_candles_required": MIN_CANDLES_REQUIRED,
            "market_prefilter_min_candles": MARKET_PREFILTER_MIN_CANDLES,
            "retune_low_activity": RETUNE_LOW_ACTIVITY,
            "retune_min_trades": RETUNE_MIN_TRADES,
            "batch_config_active": bool(batch_config.get("active")),
            "batch_config_id": batch_config.get("active_batch_id"),
            "run_tag": run_tag,
            "unique_strategy_names": UNIQUE_STRATEGY_NAMES,
            "adaptive_strategy_split": ADAPTIVE_STRATEGY_SPLIT,
            "adaptive_strategy_overrides": adaptive_strategy_overrides,
            "strategy_count_overrides": strategy_count_overrides,
            "priority_pairs": merged_priority_pairs,
        },
        "batch_config": batch_config,
        "market_discovery": market_discovery,
        "count": len(experiments),
        "experiments": experiments,
    })
    logger.info("Wrote plan: %s (%d experiments)", PLAN_FILE, len(experiments))

    if cmd == "plan":
        print(f"Generated plan only: {PLAN_FILE}")
        return

    # run
    results = run_experiments(
        experiments,
        hours=BACKTEST_HOURS,
        granularity=BACKTEST_GRANULARITY,
        prices=prices,
        pair_cache=discovered_pair_cache,
    )
    summary = summarize(results)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "hours": BACKTEST_HOURS,
            "granularity": BACKTEST_GRANULARITY,
            "min_candles_required": MIN_CANDLES_REQUIRED,
            "market_prefilter_min_candles": MARKET_PREFILTER_MIN_CANDLES,
            "retune_low_activity": RETUNE_LOW_ACTIVITY,
            "retune_min_trades": RETUNE_MIN_TRADES,
            "batch_config_active": bool(batch_config.get("active")),
            "batch_config_id": batch_config.get("active_batch_id"),
            "run_tag": run_tag,
            "unique_strategy_names": UNIQUE_STRATEGY_NAMES,
            "adaptive_strategy_split": ADAPTIVE_STRATEGY_SPLIT,
            "adaptive_strategy_overrides": adaptive_strategy_overrides,
            "strategy_count_overrides": strategy_count_overrides,
            "priority_pairs": merged_priority_pairs,
        },
        "batch_config": batch_config,
        "market_discovery": market_discovery,
        "summary": summary,
        "results": results,
    }
    save_json(RESULTS_FILE, payload)
    logger.info("Wrote results: %s", RESULTS_FILE)

    print("\n=== QUANT 100 SUMMARY ===")
    print(f"Total experiments: {summary['total']}")
    print(f"Promoted to WARM: {summary['promoted_warm']}")
    print(f"Rejected in COLD: {summary['rejected_cold']}")
    print(f"No data: {summary['no_data']}")
    print(f"Retained WARM: {summary.get('retained_warm', 0)}")
    print(f"Retained HOT: {summary.get('retained_hot', 0)}")
    print(f"Avg starter budget: ${summary['avg_starter_budget_usd']:.2f}")
    print(f"Total starter budget: ${summary['total_starter_budget_usd']:.2f}")
    print(f"Retuned low activity: {summary['retuned_low_activity']}")
    print(f"Strict profit mode: {summary['strict_profit_only']}")


if __name__ == "__main__":
    main()
