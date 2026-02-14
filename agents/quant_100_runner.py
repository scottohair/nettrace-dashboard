#!/usr/bin/env python3
"""Quant idea runner for scalable strategy discovery and promotion.

What this does:
  1. Builds N strategy experiments (parameterized variants).
  2. Tags each with target markets + server regions (latency-aware deployment hints).
  3. Backtests each against historical data.
  4. Applies gain-first gating before pipeline promotion (COLD->WARM).
  5. Produces ranked idea lists + budget recommendations for promoted candidates.

Outputs:
  - agents/quant_100_plan.json
  - agents/quant_100_results.json
  - agents/quant_idea_ranking.json
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
RANKING_FILE = BASE_DIR / "quant_idea_ranking.json"
BATCH_CONFIG_FILE = BASE_DIR / "growth_batch_config.json"
IDEA_COUNT = int(os.environ.get("QUANT100_IDEA_COUNT", os.environ.get("QUANT_IDEA_COUNT", "1000")))
MIN_CANDLES_REQUIRED = int(os.environ.get("QUANT100_MIN_CANDLES", "30"))
REJECT_ON_DATA_GAP = os.environ.get("QUANT100_REJECT_ON_DATA_GAP", "1").lower() not in (
    "0", "false", "no"
)
WALKFORWARD_SPLIT_RATIO = float(os.environ.get("QUANT100_WALKFORWARD_SPLIT_RATIO", "0.55"))
WALKFORWARD_MIN_TOTAL_CANDLES = int(os.environ.get("QUANT100_WALKFORWARD_MIN_TOTAL_CANDLES", "60"))
WALKFORWARD_MIN_OOS_CANDLES = int(os.environ.get("QUANT100_WALKFORWARD_MIN_OOS_CANDLES", "24"))
WALKFORWARD_RANDOM_SPLITS = int(os.environ.get("QUANT100_WALKFORWARD_RANDOM_SPLITS", "4"))
WALKFORWARD_SPLIT_JITTER_PCT = float(os.environ.get("QUANT100_WALKFORWARD_SPLIT_JITTER_PCT", "0.08"))
WALKFORWARD_EMBARGO_CANDLES = int(os.environ.get("QUANT100_WALKFORWARD_EMBARGO_CANDLES", "2"))
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
RETUNE_TARGET_TRADES = int(os.environ.get("QUANT100_RETUNE_TARGET_TRADES", "6"))
GAIN_GATE_ENABLED = os.environ.get("QUANT100_GAIN_GATE_ENABLED", "1").lower() not in (
    "0", "false", "no"
)
GAIN_GATE_REQUIRE_WALKFORWARD = os.environ.get(
    "QUANT100_GAIN_GATE_REQUIRE_WALKFORWARD", "1"
).lower() not in ("0", "false", "no")
GAIN_GATE_MIN_TRADES = int(os.environ.get("QUANT100_GAIN_GATE_MIN_TRADES", "2"))
GAIN_GATE_MIN_RETURN_PCT = float(os.environ.get("QUANT100_GAIN_GATE_MIN_RETURN_PCT", "0.05"))
GAIN_GATE_MIN_OOS_RETURN_PCT = float(os.environ.get("QUANT100_GAIN_GATE_MIN_OOS_RETURN_PCT", "0.02"))
GAIN_GATE_MAX_DRAWDOWN_PCT = float(os.environ.get("QUANT100_GAIN_GATE_MAX_DRAWDOWN_PCT", "3.0"))
GAIN_GATE_MIN_SHARPE = float(os.environ.get("QUANT100_GAIN_GATE_MIN_SHARPE", "0.0"))
TOP_RANKED_IDEAS = int(os.environ.get("QUANT100_TOP_RANKED_IDEAS", "100"))
QUANT100_EXECUTION_SHORTFALL_PENALTY_PER_BPS = float(
    os.environ.get("QUANT100_EXECUTION_SHORTFALL_PENALTY_PER_BPS", "0.02")
)
QUANT100_MAX_EXECUTION_SHORTFALL_BPS = float(
    os.environ.get("QUANT100_MAX_EXECUTION_SHORTFALL_BPS", "30.0")
)
QUANT100_WFO_RETURN_STD_PENALTY_PER_PCT = float(
    os.environ.get("QUANT100_WFO_RETURN_STD_PENALTY_PER_PCT", "0.50")
)
QUANT100_MIN_WFO_OOS_TRADES_FOR_GAIN = int(
    os.environ.get("QUANT100_MIN_WFO_OOS_TRADES_FOR_GAIN", "8")
)
QUANT100_MIN_WFO_OOS_TRADE_CANDLES_PER_TRADE = int(
    os.environ.get("QUANT100_MIN_WFO_OOS_TRADE_CANDLES_PER_TRADE", "25")
)
QUANT100_MIN_WFO_OOS_TRADE_FLOOR = int(
    os.environ.get("QUANT100_MIN_WFO_OOS_TRADE_FLOOR", "2")
)
QUANT100_MIN_OOS_TRADES_PENALTY_PER_TRADE = float(
    os.environ.get("QUANT100_MIN_OOS_TRADES_PENALTY_PER_TRADE", "0.45")
)
ACTIONABLE_QUEUE_FILE = BASE_DIR / "quant_actionable_queue.json"

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


def _strategy_specs(count_overrides=None, target_total=100):
    """Return per-strategy parameter grids and target counts (sum=target_total)."""
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
    disabled_bases = set()
    if overrides:
        for spec in specs:
            base = spec["base_name"]
            if base in overrides:
                try:
                    value = int(overrides[base])
                    if value <= 0:
                        spec["count"] = 0
                        disabled_bases.add(base)
                    else:
                        spec["count"] = value
                except Exception:
                    pass
    target_total = max(1, int(target_total or 1))
    total = sum(int(spec.get("count", 0) or 0) for spec in specs)
    if total != target_total and total > 0:
        factor = float(target_total) / float(total)
        for spec in specs:
            if spec["base_name"] in disabled_bases:
                spec["count"] = 0
            else:
                spec["count"] = max(1, int(round(spec["count"] * factor)))
        # Fix any rounding drift to exactly target_total.
        while sum(s["count"] for s in specs) > target_total:
            for spec in sorted(specs, key=lambda s: s["count"], reverse=True):
                if sum(s["count"] for s in specs) <= target_total:
                    break
                if spec["base_name"] in disabled_bases:
                    continue
                if spec["count"] > 1:
                    spec["count"] -= 1
        while sum(s["count"] for s in specs) < target_total:
            for spec in sorted(specs, key=lambda s: s["count"]):
                if sum(s["count"] for s in specs) >= target_total:
                    break
                if spec["base_name"] in disabled_bases:
                    continue
                spec["count"] += 1
        if sum(s["count"] for s in specs) < target_total:
            fallback = next((s for s in specs if s["base_name"] not in disabled_bases), None)
            if fallback is not None:
                fallback["count"] += target_total - sum(s["count"] for s in specs)
    return specs


def _derive_adaptive_strategy_overrides(target_total=100):
    if not ADAPTIVE_STRATEGY_SPLIT or not RESULTS_FILE.exists():
        return {}

    try:
        payload = json.loads(RESULTS_FILE.read_text())
    except Exception:
        return {}

    rows = payload.get("results", [])
    if not isinstance(rows, list) or not rows:
        return {}

    target_total = max(1, int(target_total or 1))
    bases = [spec["base_name"] for spec in _strategy_specs(target_total=target_total)]
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
    if min_per * len(bases) > target_total:
        min_per = max(1, target_total // max(1, len(bases)))

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
    remaining = max(0, target_total - sum(counts.values()))
    total_weight = sum(max(1e-6, weights[base]) for base in bases)
    for base in bases:
        share = (max(1e-6, weights[base]) / total_weight) * remaining
        counts[base] += int(share)

    while sum(counts.values()) < target_total:
        base = max(bases, key=lambda b: (weights[b], -counts[b]))
        counts[base] += 1
    while sum(counts.values()) > target_total:
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


def _sanitize_params(base_name, params):
    p = dict(params or {})
    if base_name == "mean_reversion":
        p["window"] = max(8, int(p.get("window", 20)))
        p["num_std"] = round(max(0.8, float(p.get("num_std", 2.0))), 6)
    elif base_name == "momentum":
        short_window = max(2, int(p.get("short_window", 5)))
        long_window = max(short_window + 4, int(p.get("long_window", 20)))
        p["short_window"] = short_window
        p["long_window"] = long_window
        p["volume_mult"] = round(max(1.0, float(p.get("volume_mult", 1.5))), 6)
    elif base_name == "rsi":
        period = max(6, int(p.get("period", 14)))
        oversold = max(5, min(45, int(p.get("oversold", 30))))
        overbought = max(55, min(95, int(p.get("overbought", 70))))
        if overbought <= oversold + 10:
            overbought = min(95, oversold + 15)
        p["period"] = period
        p["oversold"] = oversold
        p["overbought"] = overbought
    elif base_name == "vwap":
        p["deviation_pct"] = round(max(0.0005, float(p.get("deviation_pct", 0.005))), 6)
    elif base_name == "dip_buyer":
        p["dip_threshold"] = round(max(0.004, float(p.get("dip_threshold", 0.015))), 6)
        p["recovery_target"] = round(max(0.006, float(p.get("recovery_target", 0.020))), 6)
        p["lookback"] = max(2, int(p.get("lookback", 4)))
    elif base_name == "multi_timeframe":
        p["rsi_period"] = max(6, int(p.get("rsi_period", 14)))
        p["bb_window"] = max(10, int(p.get("bb_window", 20)))
        p["bb_std"] = round(max(1.1, float(p.get("bb_std", 2.0))), 6)
    elif base_name == "accumulate_hold":
        p["dca_interval"] = max(1, int(p.get("dca_interval", 12)))
        p["dip_threshold"] = round(max(0.004, float(p.get("dip_threshold", 0.020))), 6)
        p["profit_target"] = round(max(0.004, float(p.get("profit_target", 0.020))), 6)
    return p


def _expand_param_variants(base_name, grid, target_count):
    target_count = max(1, int(target_count or 1))
    bases = [_sanitize_params(base_name, combo) for combo in _grid_combinations(grid)]
    if not bases:
        return []

    out = []
    seen = set()

    def _add(candidate):
        key = tuple(sorted(candidate.items()))
        if key in seen:
            return False
        seen.add(key)
        out.append(candidate)
        return True

    for base in bases:
        _add(base)
        if len(out) >= target_count:
            return out[:target_count]

    cycle = 1
    max_cycles = max(96, int((target_count / max(1, len(bases))) * 6))
    while len(out) < target_count and cycle <= max_cycles:
        for base_idx, base in enumerate(bases):
            candidate = {}
            for k, v in base.items():
                if isinstance(v, bool):
                    candidate[k] = v
                    continue
                if isinstance(v, int):
                    span = max(6, min(120, int(abs(v) * 2.2) + 6))
                    delta_seed = (cycle * 13) + (base_idx * 7) + len(k)
                    delta = (delta_seed % (span * 2 + 1)) - span
                    if delta == 0:
                        delta = 1 if (delta_seed % 2 == 0) else -1
                    candidate[k] = max(1, v + delta)
                    continue
                if isinstance(v, float):
                    pct_seed = (cycle * 17) + (base_idx * 5) + len(k)
                    pct = ((pct_seed % 121) - 60) * 0.01
                    candidate[k] = max(1e-6, round(v * (1.0 + pct), 6))
                    continue
                candidate[k] = v

            candidate = _sanitize_params(base_name, candidate)
            _add(candidate)
            if len(out) >= target_count:
                break
        cycle += 1

    if len(out) < target_count:
        idx = 0
        while len(out) < target_count:
            out.append(dict(bases[idx % len(bases)]))
            idx += 1
    return out[:target_count]


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


def _idea_id_width(count):
    return max(3, len(str(max(1, int(count or 1)))))


def build_experiments(
    count=IDEA_COUNT,
    priority_pairs=None,
    market_universe=None,
    strategy_count_overrides=None,
    run_tag="",
):
    """Build exactly `count` experiments with region + market assignments."""
    count = max(1, int(count or 1))
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
    width = _idea_id_width(count)
    name_suffix = f"q{count}"
    id_prefix = f"Q{count}"
    market_idx = 0
    region_idx = 0

    for spec in _strategy_specs(
        count_overrides=strategy_count_overrides,
        target_total=count,
    ):
        combos = _expand_param_variants(
            spec["base_name"],
            spec["grid"],
            target_count=spec["count"],
        )
        if not combos:
            continue
        for i in range(spec["count"]):
            params = combos[i % len(combos)]
            market = market_universe[market_idx % len(market_universe)]
            region = SERVER_REGIONS[region_idx % len(SERVER_REGIONS)]
            strategy_name_core = f"{spec['base_name']}_{name_suffix}_{idx:0{width}d}"
            strategy_name = strategy_name_core if not run_tag else f"{strategy_name_core}_{run_tag}"
            exp_id = (
                f"{id_prefix}-{idx:0{width}d}"
                if not run_tag
                else f"{id_prefix}-{run_tag}-{idx:0{width}d}"
            )

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

    experiments = experiments[:count]
    if len(experiments) != count:
        raise RuntimeError(f"Expected exactly {count} experiments, built {len(experiments)}")
    return experiments


def build_100_experiments(priority_pairs=None, market_universe=None, strategy_count_overrides=None, run_tag=""):
    """Backwards-compatible wrapper for callers that still require exactly 100 ideas."""
    return build_experiments(
        count=100,
        priority_pairs=priority_pairs,
        market_universe=market_universe,
        strategy_count_overrides=strategy_count_overrides,
        run_tag=run_tag,
    )


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
    split = max(30, min(split, candle_count - WALKFORWARD_MIN_OOS_CANDLES - max(0, WALKFORWARD_EMBARGO_CANDLES)))
    if split <= 0 or candle_count - (split + max(0, WALKFORWARD_EMBARGO_CANDLES)) < WALKFORWARD_MIN_OOS_CANDLES:
        return None
    return split


def _walkforward_candidate_splits(candle_count, seed_material=""):
    base = _walkforward_split_index(candle_count)
    if base is None:
        return []
    min_split = 30
    max_split = candle_count - WALKFORWARD_MIN_OOS_CANDLES - max(0, WALKFORWARD_EMBARGO_CANDLES)
    if max_split < min_split:
        return [base]

    jitter = max(1, int(candle_count * max(0.0, WALKFORWARD_SPLIT_JITTER_PCT)))
    seed_text = str(seed_material or f"candles:{candle_count}")
    seed = sum((idx + 1) * ord(ch) for idx, ch in enumerate(seed_text)) % (2 ** 32)
    rng = random.Random(seed)

    candidates = {base}
    fixed_offsets = (-jitter, -(jitter // 2), (jitter // 2), jitter)
    for off in fixed_offsets:
        split = max(min_split, min(max_split, base + off))
        candidates.add(split)
    for _ in range(max(0, int(WALKFORWARD_RANDOM_SPLITS))):
        off = rng.randint(-jitter, jitter)
        split = max(min_split, min(max_split, base + off))
        candidates.add(split)
    return sorted(candidates)


def _walkforward_leakage_diagnostics(candles_is, candles_oos, split_idx, oos_start_idx):
    is_times = [int(c.get("time", 0) or 0) for c in (candles_is or [])]
    oos_times = [int(c.get("time", 0) or 0) for c in (candles_oos or [])]
    is_set = set(is_times)
    oos_set = set(oos_times)
    overlap_count = len(is_set.intersection(oos_set))
    duplicate_count_is = max(0, len(is_times) - len(is_set))
    duplicate_count_oos = max(0, len(oos_times) - len(oos_set))
    non_monotonic_is = sum(1 for i in range(1, len(is_times)) if is_times[i] < is_times[i - 1])
    non_monotonic_oos = sum(1 for i in range(1, len(oos_times)) if oos_times[i] < oos_times[i - 1])

    boundary_gap_seconds = 0
    if is_times and oos_times:
        boundary_gap_seconds = int(min(oos_times) - max(is_times))

    passed = (
        overlap_count == 0
        and duplicate_count_is == 0
        and duplicate_count_oos == 0
        and non_monotonic_is == 0
        and non_monotonic_oos == 0
        and boundary_gap_seconds >= 0
    )
    return {
        "passed": bool(passed),
        "split_index": int(split_idx),
        "oos_start_index": int(oos_start_idx),
        "embargo_candles": int(max(0, WALKFORWARD_EMBARGO_CANDLES)),
        "overlap_count": int(overlap_count),
        "duplicate_count_in_sample": int(duplicate_count_is),
        "duplicate_count_out_of_sample": int(duplicate_count_oos),
        "non_monotonic_in_sample": int(non_monotonic_is),
        "non_monotonic_out_of_sample": int(non_monotonic_oos),
        "boundary_gap_seconds": int(boundary_gap_seconds),
    }


def _build_walkforward(backtester, exp, params, candles, pair):
    walkforward = {
        "enabled": True,
        "available": False,
        "reason": "insufficient_candles_for_walkforward",
    }
    split_candidates = _walkforward_candidate_splits(
        len(candles),
        seed_material=f"{exp.get('strategy_name', '')}|{pair}|{len(candles)}",
    )
    if not split_candidates:
        return walkforward

    windows = []
    for split_idx in split_candidates:
        oos_start_idx = split_idx + max(0, WALKFORWARD_EMBARGO_CANDLES)
        candles_is = candles[:split_idx]
        candles_oos = candles[oos_start_idx:]
        if len(candles_oos) < WALKFORWARD_MIN_OOS_CANDLES or len(candles_is) < 30:
            continue

        strat_is = _instantiate_strategy(exp["strategy_base"], params)
        strat_is.name = exp["strategy_name"]
        strat_oos = _instantiate_strategy(exp["strategy_base"], params)
        strat_oos.name = exp["strategy_name"]

        bt_is = backtester.run(strat_is, candles_is, pair)
        bt_oos = backtester.run(strat_oos, candles_oos, pair)
        leakage = _walkforward_leakage_diagnostics(candles_is, candles_oos, split_idx, oos_start_idx)
        windows.append(
            {
                "split_index": int(split_idx),
                "oos_start_index": int(oos_start_idx),
                "in_sample_candles": len(candles_is),
                "out_of_sample_candles": len(candles_oos),
                "in_sample": _compact_bt(bt_is),
                "out_of_sample": _compact_bt(bt_oos),
                "leakage": leakage,
            }
        )

    if not windows:
        walkforward["reason"] = "walkforward_windows_not_viable"
        walkforward["split_candidates"] = split_candidates
        return walkforward

    valid = [w for w in windows if bool((w.get("leakage") or {}).get("passed", False))]
    selection_pool = valid or windows
    ranked = sorted(
        selection_pool,
        key=lambda w: float(
            ((w.get("out_of_sample") or {}).get("total_return_pct", 0.0) or 0.0)
        ),
    )
    selected = ranked[len(ranked) // 2]

    oos_returns = [
        float(((w.get("out_of_sample") or {}).get("total_return_pct", 0.0) or 0.0))
        for w in selection_pool
    ]
    oos_returns_sorted = sorted(oos_returns)
    oos_median = oos_returns_sorted[len(oos_returns_sorted) // 2] if oos_returns_sorted else 0.0
    oos_mean = statistics.mean(oos_returns) if oos_returns else 0.0
    oos_std = statistics.pstdev(oos_returns) if len(oos_returns) > 1 else 0.0
    leakage_summary = {
        "valid_windows": int(len(valid)),
        "total_windows": int(len(windows)),
        "any_overlap_detected": any(int((w.get("leakage") or {}).get("overlap_count", 0) or 0) > 0 for w in windows),
        "total_overlap_count": int(sum(int((w.get("leakage") or {}).get("overlap_count", 0) or 0) for w in windows)),
    }
    walkforward = {
        "enabled": True,
        "available": True,
        "selection_method": "median_oos_return",
        "split_index": int(selected.get("split_index", 0) or 0),
        "oos_start_index": int(selected.get("oos_start_index", 0) or 0),
        "in_sample_candles": int(selected.get("in_sample_candles", 0) or 0),
        "out_of_sample_candles": int(selected.get("out_of_sample_candles", 0) or 0),
        "in_sample": selected.get("in_sample", {}),
        "out_of_sample": selected.get("out_of_sample", {}),
        "split_candidates": [int(x) for x in split_candidates],
        "windows_tested": int(len(windows)),
        "leakage_diagnostics": selected.get("leakage", {}),
        "leakage_summary": leakage_summary,
        "oos_return_distribution_pct": {
            "median": round(float(oos_median), 6),
            "mean": round(float(oos_mean), 6),
            "stdev": round(float(oos_std), 6),
            "min": round(float(min(oos_returns) if oos_returns else 0.0), 6),
            "max": round(float(max(oos_returns) if oos_returns else 0.0), 6),
        },
    }
    return walkforward


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


def _score_backtest(bt, target_trades=0):
    trades = int(bt.get("total_trades", 0) or 0)
    target_trades = max(0, int(target_trades or 0))
    return (
        float(bt.get("total_return_pct", 0.0) or 0.0)
        + float(bt.get("win_rate", 0.0) or 0.0) * 10.0
        + min(16, trades) * 0.10
        - float(bt.get("max_drawdown_pct", 0.0) or 0.0) * 0.35
        + (0.75 if target_trades and trades >= target_trades else 0.0)
        - (0.25 * max(0, target_trades - trades) if target_trades else 0.0)
    )


def _extract_execution_shortfall_bps(backtest_payload):
    bt = backtest_payload or {}
    execution_summary = bt.get("execution_intelligence")
    if isinstance(execution_summary, dict):
        return float(execution_summary.get("shortfall_bps", 0.0) or 0.0)
    return 0.0


def _score_gain_potential(metrics, walkforward):
    metrics = metrics or {}
    walkforward = walkforward or {}
    ret = float(metrics.get("return_pct", 0.0) or 0.0)
    win = float(metrics.get("win_rate", 0.0) or 0.0)
    trades = int(metrics.get("trades", 0) or 0)
    dd = float(metrics.get("drawdown_pct", 0.0) or 0.0)
    sharpe = float(metrics.get("sharpe", 0.0) or 0.0)
    losses = int(metrics.get("losses", 0) or 0)
    oos_ret = float(
        ((walkforward.get("out_of_sample") or {}).get("total_return_pct", 0.0) or 0.0)
    )
    oos_trades = int(
        ((walkforward.get("out_of_sample") or {}).get("total_trades", 0) or 0)
    )
    wf_oos_stdev = float(
        ((walkforward.get("oos_return_distribution_pct") or {}).get("stdev", 0.0) or 0.0)
    )
    wf_available = bool(walkforward.get("available", False))
    execution_shortfall_bps = float(metrics.get("execution_shortfall_bps", 0.0) or 0.0)
    min_oos_trades = _effective_wfo_oos_trade_floor(walkforward)
    oos_trade_deficit = max(0, min_oos_trades - oos_trades)

    score = 0.0
    score += ret * 3.00
    score += oos_ret * 4.00 if wf_available else -0.50
    score += sharpe * 0.80
    score += (win - 0.50) * 16.0
    score += min(25, trades) * 0.08
    score += min(10, oos_trades) * 0.05
    score -= dd * 1.20
    score -= max(0, losses - 1) * 0.35
    score -= min(3.0, max(0.0, execution_shortfall_bps) * QUANT100_EXECUTION_SHORTFALL_PENALTY_PER_BPS)
    score -= min(4.0, max(0.0, wf_oos_stdev) * QUANT100_WFO_RETURN_STD_PENALTY_PER_PCT)
    score -= min(4.0, oos_trade_deficit * QUANT100_MIN_OOS_TRADES_PENALTY_PER_TRADE)
    return round(float(score), 6)


def _effective_wfo_oos_trade_floor(walkforward):
    base_min = max(0, int(QUANT100_MIN_WFO_OOS_TRADES_FOR_GAIN))
    if base_min <= 0:
        return 0
    if base_min <= 1:
        return base_min
    if not isinstance(walkforward, dict):
        return base_min

    oos = walkforward.get("out_of_sample") if isinstance(walkforward, dict) else {}
    oos_candles = int(
        ((walkforward or {}).get("out_of_sample_candles", 0) or 0)
        or ((oos or {}).get("candle_count", 0) or 0)
    )
    if oos_candles <= 0:
        return base_min

    candles_per_trade = max(1, int(QUANT100_MIN_WFO_OOS_TRADE_CANDLES_PER_TRADE))
    min_floor = max(1, int(QUANT100_MIN_WFO_OOS_TRADE_FLOOR))
    min_floor = max(2, min(base_min, min_floor))
    scaled_floor = max(1, oos_candles // candles_per_trade)
    return max(min_floor, min(base_min, scaled_floor))


def _extract_walkforward_oos_shortfall_bps(walkforward):
    return float(
        ((walkforward or {}).get("out_of_sample") or {}).get("execution_shortfall_bps", 0.0)
        or 0.0
    )


def _gain_gate(bt, walkforward):
    if not GAIN_GATE_ENABLED:
        return True, []

    bt = bt or {}
    walkforward = walkforward or {}
    reasons = []

    total_trades = int(bt.get("total_trades", 0) or 0)
    ret = float(bt.get("total_return_pct", 0.0) or 0.0)
    dd = float(bt.get("max_drawdown_pct", 0.0) or 0.0)
    sharpe = float(bt.get("sharpe_ratio", 0.0) or 0.0)
    execution_shortfall_bps = float(
        (bt.get("execution_intelligence") or {}).get("shortfall_bps", 0.0) or 0.0
    )

    if total_trades < GAIN_GATE_MIN_TRADES:
        reasons.append(f"trades {total_trades} < {GAIN_GATE_MIN_TRADES}")
    if ret < GAIN_GATE_MIN_RETURN_PCT:
        reasons.append(f"return {ret:.2f}% < {GAIN_GATE_MIN_RETURN_PCT:.2f}%")
    if dd > GAIN_GATE_MAX_DRAWDOWN_PCT:
        reasons.append(f"drawdown {dd:.2f}% > {GAIN_GATE_MAX_DRAWDOWN_PCT:.2f}%")
    if sharpe < GAIN_GATE_MIN_SHARPE:
        reasons.append(f"sharpe {sharpe:.2f} < {GAIN_GATE_MIN_SHARPE:.2f}")

    wf_available = bool(walkforward.get("available", False))
    oos = walkforward.get("out_of_sample") if isinstance(walkforward, dict) else {}
    oos_ret = float(((oos or {}).get("total_return_pct", 0.0) or 0.0))
    if GAIN_GATE_REQUIRE_WALKFORWARD and not wf_available:
        reasons.append("walkforward unavailable")
    if wf_available and oos_ret < GAIN_GATE_MIN_OOS_RETURN_PCT:
        reasons.append(
            f"walkforward_oos_return {oos_ret:.2f}% < {GAIN_GATE_MIN_OOS_RETURN_PCT:.2f}%"
        )
    oos_trades = int((oos or {}).get("total_trades", 0) or 0)
    max_shortfall_bps = max(0.0, QUANT100_MAX_EXECUTION_SHORTFALL_BPS)
    oos_shortfall_bps = _extract_walkforward_oos_shortfall_bps(walkforward)
    if max_shortfall_bps > 0.0:
        if execution_shortfall_bps > max_shortfall_bps:
            reasons.append(
                f"execution_shortfall_bps {execution_shortfall_bps:.2f} > {max_shortfall_bps:.2f}"
            )
        if oos_shortfall_bps > max_shortfall_bps:
            reasons.append(
                f"walkforward_oos_shortfall_bps {oos_shortfall_bps:.2f} > "
                f"{max_shortfall_bps:.2f}"
            )
    min_oos_trades = _effective_wfo_oos_trade_floor(walkforward)
    if wf_available and min_oos_trades > 0 and oos_trades < min_oos_trades:
        reasons.append(
            f"walkforward_oos_trades {oos_trades} < {min_oos_trades}"
        )

    return len(reasons) == 0, reasons


def _retune_candidates(base_name, params):
    p = dict(params or {})
    out = []
    if base_name == "mean_reversion":
        out.append({**p, "num_std": max(1.1, float(p.get("num_std", 2.0)) - 0.3)})
        out.append({**p, "window": max(8, int(p.get("window", 20)) - 4)})
        out.append({**p, "num_std": max(0.8, float(p.get("num_std", 2.0)) - 0.6)})
        out.append({**p, "window": max(6, int(p.get("window", 20)) - 8)})
    elif base_name == "momentum":
        out.append({**p, "volume_mult": max(1.0, float(p.get("volume_mult", 1.5)) - 0.2)})
        out.append({
            **p,
            "short_window": max(2, int(p.get("short_window", 5)) - 1),
            "long_window": max(8, int(p.get("long_window", 20)) - 4),
        })
        out.append({
            **p,
            "short_window": max(2, int(p.get("short_window", 5)) - 2),
            "long_window": max(6, int(p.get("long_window", 20)) - 6),
            "volume_mult": max(1.0, float(p.get("volume_mult", 1.5)) - 0.4),
        })
    elif base_name == "rsi":
        out.append({
            **p,
            "oversold": min(45, int(p.get("oversold", 30)) + 5),
            "overbought": max(55, int(p.get("overbought", 70)) - 5),
        })
        out.append({**p, "period": max(7, int(p.get("period", 14)) - 3)})
        out.append({
            **p,
            "oversold": min(48, int(p.get("oversold", 30)) + 10),
            "overbought": max(52, int(p.get("overbought", 70)) - 10),
            "period": max(6, int(p.get("period", 14)) - 5),
        })
    elif base_name == "vwap":
        out.append({**p, "deviation_pct": max(0.0015, float(p.get("deviation_pct", 0.005)) * 0.75)})
        out.append({**p, "deviation_pct": max(0.0008, float(p.get("deviation_pct", 0.005)) * 0.50)})
    elif base_name == "dip_buyer":
        out.append({
            **p,
            "dip_threshold": max(0.006, float(p.get("dip_threshold", 0.015)) * 0.70),
            "recovery_target": max(0.008, float(p.get("recovery_target", 0.02)) * 0.85),
        })
        out.append({**p, "lookback": max(2, int(p.get("lookback", 4)) - 1)})
        out.append({
            **p,
            "dip_threshold": max(0.004, float(p.get("dip_threshold", 0.015)) * 0.50),
            "recovery_target": max(0.006, float(p.get("recovery_target", 0.02)) * 0.70),
            "lookback": max(2, int(p.get("lookback", 4)) - 2),
        })
    elif base_name == "multi_timeframe":
        out.append({
            **p,
            "bb_std": max(1.4, float(p.get("bb_std", 2.0)) - 0.25),
            "rsi_period": max(8, int(p.get("rsi_period", 14)) - 2),
        })
        out.append({
            **p,
            "bb_std": max(1.1, float(p.get("bb_std", 2.0)) - 0.50),
            "rsi_period": max(6, int(p.get("rsi_period", 14)) - 4),
            "bb_window": max(10, int(p.get("bb_window", 20)) - 4),
        })
    elif base_name == "accumulate_hold":
        out.append({
            **p,
            "dca_interval": max(3, int(p.get("dca_interval", 12)) // 2),
            "profit_target": max(0.010, float(p.get("profit_target", 0.02)) * 0.80),
            "dip_threshold": max(0.010, float(p.get("dip_threshold", 0.02)) * 0.80),
        })
        out.append({
            **p,
            "dca_interval": max(1, int(p.get("dca_interval", 12)) // 3),
            "profit_target": max(0.006, float(p.get("profit_target", 0.02)) * 0.60),
            "dip_threshold": max(0.006, float(p.get("dip_threshold", 0.02)) * 0.60),
        })

    unique = []
    seen = set()
    for item in out:
        key = tuple(sorted(item.items()))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique[:8]


def _activity_boost_candidates(base_name, params):
    """Generate aggressive-but-valid variants that target higher signal/trade activity."""
    p = dict(params or {})
    out = []
    if base_name == "mean_reversion":
        for window in (6, 8, 10, 12):
            for num_std in (0.9, 1.1, 1.3):
                out.append({"window": window, "num_std": num_std})
    elif base_name == "momentum":
        for short_window, long_window in ((2, 8), (3, 10), (4, 12), (5, 14)):
            for volume_mult in (1.0, 1.1, 1.2):
                out.append(
                    {
                        "short_window": short_window,
                        "long_window": long_window,
                        "volume_mult": volume_mult,
                    }
                )
    elif base_name == "rsi":
        for period in (6, 8, 10):
            for oversold, overbought in ((38, 62), (40, 60), (42, 58), (45, 55)):
                out.append(
                    {
                        "period": period,
                        "oversold": oversold,
                        "overbought": overbought,
                    }
                )
    elif base_name == "vwap":
        for deviation_pct in (0.0010, 0.0015, 0.0020, 0.0025):
            out.append({"deviation_pct": deviation_pct})
    elif base_name == "dip_buyer":
        for dip_threshold, recovery_target, lookback in (
            (0.004, 0.006, 2),
            (0.006, 0.008, 2),
            (0.008, 0.010, 3),
            (0.010, 0.012, 3),
        ):
            out.append(
                {
                    "dip_threshold": dip_threshold,
                    "recovery_target": recovery_target,
                    "lookback": lookback,
                }
            )
    elif base_name == "multi_timeframe":
        for rsi_period, bb_window, bb_std in (
            (6, 10, 1.1),
            (7, 12, 1.2),
            (8, 14, 1.4),
            (10, 16, 1.5),
        ):
            out.append(
                {
                    "rsi_period": rsi_period,
                    "bb_window": bb_window,
                    "bb_std": bb_std,
                }
            )
    elif base_name == "accumulate_hold":
        for dca_interval, dip_threshold, profit_target in (
            (1, 0.006, 0.008),
            (2, 0.008, 0.010),
            (3, 0.010, 0.012),
            (4, 0.012, 0.014),
        ):
            out.append(
                {
                    "dca_interval": dca_interval,
                    "dip_threshold": dip_threshold,
                    "profit_target": profit_target,
                }
            )
    else:
        out.append(dict(p))

    unique = []
    seen = set()
    for item in out:
        candidate = _sanitize_params(base_name, item)
        key = tuple(sorted(candidate.items()))
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique[:16]


def _maybe_retune_strategy(exp, candles, backtester, baseline_bt):
    target_trades = max(int(RETUNE_MIN_TRADES), int(RETUNE_TARGET_TRADES))
    baseline_trades = int(baseline_bt.get("total_trades", 0) or 0)
    if not RETUNE_LOW_ACTIVITY or baseline_trades >= target_trades:
        return None

    best_bt = baseline_bt
    best_params = dict(exp["params"])
    best_score = _score_backtest(baseline_bt, target_trades=target_trades)
    best_rank = (
        int(baseline_trades >= target_trades),
        int(baseline_trades >= int(RETUNE_MIN_TRADES)),
        int(baseline_trades),
        float(best_score),
    )
    improved = False

    candidates = list(_retune_candidates(exp["strategy_base"], exp["params"]))
    if baseline_trades < max(int(RETUNE_MIN_TRADES), target_trades):
        candidates.extend(_activity_boost_candidates(exp["strategy_base"], exp["params"]))

    deduped = []
    seen = set()
    for candidate in candidates:
        normalized = _sanitize_params(exp["strategy_base"], candidate)
        key = tuple(sorted(normalized.items()))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)

    for candidate in deduped:
        candidate_strategy = _instantiate_strategy(exp["strategy_base"], candidate)
        candidate_strategy.name = exp["strategy_name"]
        candidate_bt = backtester.run(candidate_strategy, candles, exp["pair"])
        candidate_trades = int(candidate_bt.get("total_trades", 0) or 0)
        candidate_score = _score_backtest(candidate_bt, target_trades=target_trades)
        candidate_rank = (
            int(candidate_trades >= target_trades),
            int(candidate_trades >= int(RETUNE_MIN_TRADES)),
            int(candidate_trades),
            float(candidate_score),
        )
        if candidate_rank > best_rank:
            best_bt = candidate_bt
            best_params = candidate
            best_score = candidate_score
            best_rank = candidate_rank
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
                "execution_shortfall_bps": 0.0,
            },
                "budget": {
                    "starter_budget_usd": 0.0,
                    "tier": budget_profile.get("risk_tier", "blocked"),
                    "max_budget_usd": 0.0,
                },
                "data_quality_reject": True,
                "validator_passed": bool(passed),
                "walkforward": fallback_bt["walkforward"],
                "gain_gate": {
                    "enabled": bool(GAIN_GATE_ENABLED),
                    "passed": False,
                    "reasons": [f"insufficient_candles {len(candles)} < {MIN_CANDLES_REQUIRED}"],
                },
            })
            rec["expected_gain_score"] = _score_gain_potential(rec["metrics"], rec["walkforward"])
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
        walkforward = _build_walkforward(backtester, exp, params_for_walkforward, candles, pair)
        gain_gate_passed, gain_gate_reasons = _gain_gate(bt, walkforward)

        bt_for_gate = dict(bt)
        bt_for_gate["walkforward"] = walkforward
        validator_passed, gate_msg = validator.submit_backtest(strategy.name, pair, bt_for_gate)
        passed = bool(validator_passed and gain_gate_passed)
        if not gain_gate_passed:
            gate_prefix = f"gain_gate_failed: {', '.join(gain_gate_reasons)}"
            gate_msg = gate_prefix if not gate_msg else f"{gate_msg}; {gate_prefix}"

        budget_profile = validator.get_budget_profile(strategy.name, pair)
        post_stage_row = validator.db.execute(
            "SELECT stage FROM strategy_registry WHERE name=? AND pair=?",
            (exp["strategy_name"], pair),
        ).fetchone()
        post_stage = str(post_stage_row["stage"] if post_stage_row and post_stage_row["stage"] else prior_stage).upper()
        if validator_passed and not gain_gate_passed and post_stage in {"WARM", "HOT"}:
            validator.demote_strategy(strategy.name, pair, reason="gain_gate_failed")
            budget_profile = validator.get_budget_profile(strategy.name, pair)
            post_stage = "COLD"

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
                "execution_shortfall_bps": _extract_execution_shortfall_bps(bt),
            },
            "walkforward": walkforward,
            "budget": budget,
            "risk_profile": budget_profile,
            "prior_stage": prior_stage,
            "post_stage": post_stage,
            "validator_passed": bool(validator_passed),
            "gain_gate": {
                "enabled": bool(GAIN_GATE_ENABLED),
                "passed": bool(gain_gate_passed),
                "reasons": gain_gate_reasons,
            },
        })
        rec["expected_gain_score"] = _score_gain_potential(rec["metrics"], rec["walkforward"])
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


def _ranked_ideas(results):
    rows = []
    for row in results:
        metrics = row.get("metrics", {}) if isinstance(row.get("metrics"), dict) else {}
        walkforward = row.get("walkforward", {}) if isinstance(row.get("walkforward"), dict) else {}
        gain_gate = row.get("gain_gate", {}) if isinstance(row.get("gain_gate"), dict) else {}
        reasons = []
        if row.get("reason"):
            reasons.append(str(row.get("reason")))
        if row.get("gate_message"):
            reasons.append(str(row.get("gate_message")))
        for item in gain_gate.get("reasons", []) if isinstance(gain_gate.get("reasons"), list) else []:
            reasons.append(str(item))

        rows.append(
            {
                "id": row.get("id"),
                "strategy_name": row.get("strategy_name"),
                "strategy_base": row.get("strategy_base"),
                "pair": row.get("pair"),
                "target_region": row.get("target_region"),
                "status": row.get("status"),
                "expected_gain_score": float(
                    row.get("expected_gain_score", _score_gain_potential(metrics, walkforward)) or 0.0
                ),
                "return_pct": float(metrics.get("return_pct", 0.0) or 0.0),
                "walkforward_oos_return_pct": float(
                    ((walkforward.get("out_of_sample") or {}).get("total_return_pct", 0.0) or 0.0)
                ),
                "drawdown_pct": float(metrics.get("drawdown_pct", 0.0) or 0.0),
                "sharpe": float(metrics.get("sharpe", 0.0) or 0.0),
                "win_rate": float(metrics.get("win_rate", 0.0) or 0.0),
                "trades": int(metrics.get("trades", 0) or 0),
                "execution_shortfall_bps": float(
                    metrics.get("execution_shortfall_bps", 0.0) or 0.0
                ),
                "walkforward_oos_return_stdev_pct": float(
                    ((walkforward.get("oos_return_distribution_pct") or {}).get("stdev", 0.0) or 0.0)
                ),
                "walkforward_oos_trades": int(
                    ((walkforward.get("out_of_sample") or {}).get("total_trades", 0) or 0)
                ),
                "gain_gate_passed": bool(gain_gate.get("passed", False)),
                "implementable": bool(
                    row.get("status") in {"promoted_warm", "retained_warm_passed", "retained_hot_passed"}
                ),
                "blockers": reasons[:8],
            }
        )
    rows.sort(key=lambda r: r["expected_gain_score"], reverse=True)
    return rows


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
    gain_gate_passed = [
        r for r in results if bool((r.get("gain_gate") or {}).get("passed", False))
    ]
    rejection_reasons = {}
    for r in rejected:
        reason = r.get("reason") or r.get("gate_message") or "unknown"
        rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1

    ranked = _ranked_ideas(results)
    top_ranked = ranked[: min(20, len(ranked))]
    top = [r for r in ranked if r.get("implementable")][: min(20, len(ranked))]
    if not top:
        top = top_ranked
    top_actionable = [r for r in ranked if r.get("implementable")][: min(TOP_RANKED_IDEAS, len(ranked))]
    top_blocked = [r for r in ranked if not r.get("implementable")][: min(TOP_RANKED_IDEAS, len(ranked))]

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
        "gain_gate_passed": len(gain_gate_passed),
        "implementable_count": len([r for r in ranked if r.get("implementable")]),
        "rejection_reasons_top": sorted(
            rejection_reasons.items(),
            key=lambda kv: kv[1],
            reverse=True,
        )[:15],
        "top_candidates": top,
        "top_ranked": top_ranked,
        "top_actionable": top_actionable,
        "top_blocked": top_blocked,
    }


def save_json(path, payload):
    path.write_text(json.dumps(payload, indent=2))


def _build_actionable_queue(results, summary, max_items=100):
    top = summary.get("top_actionable", []) if isinstance(summary, dict) else []
    if not isinstance(top, list):
        top = []

    by_id = {str(r.get("id")): r for r in (results or []) if isinstance(r, dict)}
    queue = []
    for rank, row in enumerate(top[: max(1, int(max_items or 1))], start=1):
        if not isinstance(row, dict):
            continue
        rec = by_id.get(str(row.get("id")), {})
        budget = rec.get("budget", {}) if isinstance(rec.get("budget"), dict) else {}
        queue.append(
            {
                "rank": int(rank),
                "id": row.get("id"),
                "strategy_name": row.get("strategy_name"),
                "strategy_base": row.get("strategy_base"),
                "pair": row.get("pair"),
                "target_region": row.get("target_region"),
                "expected_gain_score": float(row.get("expected_gain_score", 0.0) or 0.0),
                "return_pct": float(row.get("return_pct", 0.0) or 0.0),
                "walkforward_oos_return_pct": float(
                    row.get("walkforward_oos_return_pct", 0.0) or 0.0
                ),
                "trades": int(row.get("trades", 0) or 0),
                "execution_shortfall_bps": float(
                    row.get("execution_shortfall_bps", 0.0) or 0.0
                ),
                "status": str(row.get("status", "")),
                "budget": {
                    "starter_budget_usd": float(
                        budget.get("starter_budget_usd", 0.0) or 0.0
                    ),
                    "max_budget_usd": float(budget.get("max_budget_usd", 0.0) or 0.0),
                    "tier": str(budget.get("tier", "none")),
                },
            }
        )
    return queue


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    idea_count = IDEA_COUNT
    if cmd in {"run100", "plan100"}:
        idea_count = 100
    elif cmd in {"run1000", "plan1000"}:
        idea_count = 1000

    if cmd == "status":
        if not RESULTS_FILE.exists():
            print("No results file yet. Run: python quant_100_runner.py run")
            return
        payload = json.loads(RESULTS_FILE.read_text())
        summary = payload.get("summary", {})
        print(f"Total: {summary.get('total', 0)}")
        print(f"Idea count configured: {payload.get('config', {}).get('idea_count', IDEA_COUNT)}")
        print(f"Promoted WARM: {summary.get('promoted_warm', 0)}")
        print(f"Rejected COLD: {summary.get('rejected_cold', 0)}")
        print(f"No data: {summary.get('no_data', 0)}")
        print(f"Gain gate passed: {summary.get('gain_gate_passed', 0)}")
        print(f"Implementable ideas: {summary.get('implementable_count', 0)}")
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
        adaptive_strategy_overrides = _derive_adaptive_strategy_overrides(
            target_total=idea_count
        )
        strategy_count_overrides = dict(adaptive_strategy_overrides)

    experiments = build_experiments(
        count=idea_count,
        priority_pairs=merged_priority_pairs,
        market_universe=market_universe,
        strategy_count_overrides=strategy_count_overrides,
        run_tag=run_tag,
    )
    save_json(PLAN_FILE, {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "idea_count": idea_count,
            "hours": BACKTEST_HOURS,
            "granularity": BACKTEST_GRANULARITY,
            "min_candles_required": MIN_CANDLES_REQUIRED,
            "market_prefilter_min_candles": MARKET_PREFILTER_MIN_CANDLES,
            "retune_low_activity": RETUNE_LOW_ACTIVITY,
            "retune_min_trades": RETUNE_MIN_TRADES,
            "gain_gate_enabled": GAIN_GATE_ENABLED,
            "gain_gate_require_walkforward": GAIN_GATE_REQUIRE_WALKFORWARD,
            "gain_gate_min_trades": GAIN_GATE_MIN_TRADES,
            "gain_gate_min_return_pct": GAIN_GATE_MIN_RETURN_PCT,
            "gain_gate_min_oos_return_pct": GAIN_GATE_MIN_OOS_RETURN_PCT,
            "gain_gate_max_drawdown_pct": GAIN_GATE_MAX_DRAWDOWN_PCT,
            "gain_gate_min_sharpe": GAIN_GATE_MIN_SHARPE,
            "wfo_return_std_penalty_per_pct": QUANT100_WFO_RETURN_STD_PENALTY_PER_PCT,
            "min_wfo_oos_trades_for_gain": QUANT100_MIN_WFO_OOS_TRADES_FOR_GAIN,
            "wfo_oos_trade_candles_per_trade": QUANT100_MIN_WFO_OOS_TRADE_CANDLES_PER_TRADE,
            "wfo_oos_trade_floor": QUANT100_MIN_WFO_OOS_TRADE_FLOOR,
            "min_oos_trades_penalty_per_trade": QUANT100_MIN_OOS_TRADES_PENALTY_PER_TRADE,
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

    if cmd in {"plan", "plan100", "plan1000"}:
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
            "idea_count": idea_count,
            "hours": BACKTEST_HOURS,
            "granularity": BACKTEST_GRANULARITY,
            "min_candles_required": MIN_CANDLES_REQUIRED,
            "market_prefilter_min_candles": MARKET_PREFILTER_MIN_CANDLES,
            "retune_low_activity": RETUNE_LOW_ACTIVITY,
            "retune_min_trades": RETUNE_MIN_TRADES,
            "gain_gate_enabled": GAIN_GATE_ENABLED,
            "gain_gate_require_walkforward": GAIN_GATE_REQUIRE_WALKFORWARD,
            "gain_gate_min_trades": GAIN_GATE_MIN_TRADES,
            "gain_gate_min_return_pct": GAIN_GATE_MIN_RETURN_PCT,
            "gain_gate_min_oos_return_pct": GAIN_GATE_MIN_OOS_RETURN_PCT,
            "gain_gate_max_drawdown_pct": GAIN_GATE_MAX_DRAWDOWN_PCT,
            "gain_gate_min_sharpe": GAIN_GATE_MIN_SHARPE,
            "wfo_return_std_penalty_per_pct": QUANT100_WFO_RETURN_STD_PENALTY_PER_PCT,
            "min_wfo_oos_trades_for_gain": QUANT100_MIN_WFO_OOS_TRADES_FOR_GAIN,
            "wfo_oos_trade_candles_per_trade": QUANT100_MIN_WFO_OOS_TRADE_CANDLES_PER_TRADE,
            "wfo_oos_trade_floor": QUANT100_MIN_WFO_OOS_TRADE_FLOOR,
            "min_oos_trades_penalty_per_trade": QUANT100_MIN_OOS_TRADES_PENALTY_PER_TRADE,
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
    save_json(
        RANKING_FILE,
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "idea_count": len(results),
            "top_n": TOP_RANKED_IDEAS,
            "ranked_ideas": _ranked_ideas(results),
            "top_actionable": summary.get("top_actionable", []),
            "top_blocked": summary.get("top_blocked", []),
        },
    )
    save_json(
        ACTIONABLE_QUEUE_FILE,
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source_results_file": str(RESULTS_FILE),
            "idea_count": len(results),
            "queue_size": min(TOP_RANKED_IDEAS, len(summary.get("top_actionable", []) or [])),
            "queue": _build_actionable_queue(
                results,
                summary,
                max_items=TOP_RANKED_IDEAS,
            ),
        },
    )
    logger.info("Wrote results: %s", RESULTS_FILE)
    logger.info("Wrote ranking: %s", RANKING_FILE)
    logger.info("Wrote actionable queue: %s", ACTIONABLE_QUEUE_FILE)

    print("\n=== QUANT IDEA SUMMARY ===")
    print(f"Total experiments: {summary['total']}")
    print(f"Idea count configured: {idea_count}")
    print(f"Promoted to WARM: {summary['promoted_warm']}")
    print(f"Rejected in COLD: {summary['rejected_cold']}")
    print(f"No data: {summary['no_data']}")
    print(f"Gain gate passed: {summary.get('gain_gate_passed', 0)}")
    print(f"Implementable ideas: {summary.get('implementable_count', 0)}")
    print(f"Retained WARM: {summary.get('retained_warm', 0)}")
    print(f"Retained HOT: {summary.get('retained_hot', 0)}")
    print(f"Avg starter budget: ${summary['avg_starter_budget_usd']:.2f}")
    print(f"Total starter budget: ${summary['total_starter_budget_usd']:.2f}")
    print(f"Retuned low activity: {summary['retuned_low_activity']}")
    print(f"Strict profit mode: {summary['strict_profit_only']}")


if __name__ == "__main__":
    main()
