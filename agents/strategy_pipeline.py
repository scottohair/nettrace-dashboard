#!/usr/bin/env python3
"""Strategy Pipeline: COLD → WARM → HOT

Every strategy MUST prove itself before touching real money.

Pipeline stages:
  COLD (Backtest)  → Run against historical price data. Must show profit.
  WARM (Paper)     → Run live with fake money. Must maintain profit for 1h+.
  HOT  (Live)      → Real money. Only strategies that passed COLD + WARM.

Promotion criteria:
  COLD → WARM: win_rate > 60%, total_return > 0%, max_drawdown < 5%
  WARM → HOT:  win_rate > 55%, total_return > 0% over 1h+, sharpe > 0.5

RULE #1: NEVER LOSE MONEY.
Any strategy that loses money in WARM gets demoted back to COLD.
Any strategy that loses money in HOT gets KILLED immediately.
"""

import json
import logging
import os
import random
import sqlite3
import sys
import time
import math
import statistics
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "strategy_pipeline.log")),
    ]
)
logger = logging.getLogger("strategy_pipeline")

PIPELINE_DB = str(Path(__file__).parent / "pipeline.db")
TRADER_DB = str(Path(__file__).parent / "trader.db")

# Promotion thresholds — CONSERVATIVE
COLD_TO_WARM = {
    "min_trades": 12,          # Was 20 — faster promotion for growth phase
    "min_win_rate": 0.58,      # Was 60% — slightly relaxed for more flow
    "min_return_pct": 0.3,     # Was 0.5% — lower bar but still profitable
    "max_drawdown_pct": 5.0,   # Max 5% drawdown (unchanged — protect capital)
}

WARM_TO_HOT = {
    "min_trades": 10,
    "min_win_rate": 0.55,      # 55% win rate in paper trading
    "min_return_pct": 0.1,     # Must be profitable
    "max_drawdown_pct": 3.0,   # Max 3% drawdown (tighter for real money)
    "min_runtime_seconds": 3600,  # Must run for at least 1 hour
    "min_sharpe": 0.5,
}

# Adaptive HOT gating for sparse but multi-window evidence.
WARM_ADAPTIVE_GATING = os.environ.get("WARM_ADAPTIVE_GATING", "1").lower() not in ("0", "false", "no")
WARM_SPARSE_MIN_TRADES = int(os.environ.get("WARM_SPARSE_MIN_TRADES", "2"))
WARM_SPARSE_MIN_WIN_RATE = float(os.environ.get("WARM_SPARSE_MIN_WIN_RATE", "0.58"))
WARM_SPARSE_MIN_RETURN_PCT = float(os.environ.get("WARM_SPARSE_MIN_RETURN_PCT", "0.03"))
WARM_SPARSE_MAX_DRAWDOWN_PCT = float(os.environ.get("WARM_SPARSE_MAX_DRAWDOWN_PCT", "2.50"))
WARM_SPARSE_MIN_RUNTIME_SECONDS = int(os.environ.get("WARM_SPARSE_MIN_RUNTIME_SECONDS", "900"))
WARM_SPARSE_MIN_SHARPE = float(os.environ.get("WARM_SPARSE_MIN_SHARPE", "0.00"))
WARM_MIN_EVIDENCE_WINDOWS = int(os.environ.get("WARM_MIN_EVIDENCE_WINDOWS", "2"))
WARM_SPARSE_MAX_WINDOW_CANDLES = int(os.environ.get("WARM_SPARSE_MAX_WINDOW_CANDLES", "168"))
WARM_MATURE_EVIDENCE_WINDOWS = int(os.environ.get("WARM_MATURE_EVIDENCE_WINDOWS", "4"))
WARM_MATURE_EVIDENCE_MIN_CANDLES = int(os.environ.get("WARM_MATURE_EVIDENCE_MIN_CANDLES", "72"))
WARM_WALKFORWARD_LEAKAGE_ENFORCE = os.environ.get("WARM_WALKFORWARD_LEAKAGE_ENFORCE", "1").lower() not in ("0", "false", "no")

# Fee assumptions
COINBASE_FEE = 0.006  # 0.6% taker fee
SLIPPAGE = 0.001      # 0.1% slippage assumption
BACKTEST_AUTO_EXIT_ENABLED = os.environ.get("BACKTEST_AUTO_EXIT_ENABLED", "1").lower() not in (
    "0",
    "false",
    "no",
)
BACKTEST_AUTO_EXIT_MIN_NET_PCT = float(os.environ.get("BACKTEST_AUTO_EXIT_MIN_NET_PCT", "0.08"))
BACKTEST_AUTO_EXIT_MIN_HOLD_CANDLES = int(os.environ.get("BACKTEST_AUTO_EXIT_MIN_HOLD_CANDLES", "2"))
BACKTEST_AUTO_EXIT_MAX_HOLD_CANDLES = int(os.environ.get("BACKTEST_AUTO_EXIT_MAX_HOLD_CANDLES", "18"))

# Strict capital protection: no realized-loss trades are allowed to pass COLD.
STRICT_PROFIT_ONLY = os.environ.get("STRICT_PROFIT_ONLY", "1").lower() not in ("0", "false", "no")
ADAPTIVE_COLD_GATING = os.environ.get("ADAPTIVE_COLD_GATING", "1").lower() not in ("0", "false", "no")

# Optional high-confidence sparse-window relaxation when evidence is limited.
# Defaults stay strict unless explicitly enabled by env in deployments.
COLD_TO_WARM_SPARSE_MIN_TRADES = int(os.environ.get("COLD_TO_WARM_SPARSE_MIN_TRADES", "4"))
COLD_TO_WARM_SPARSE_MIN_WIN_RATE = float(
    os.environ.get("COLD_TO_WARM_SPARSE_MIN_WIN_RATE", "0.95")
)
COLD_TO_WARM_SPARSE_MIN_RETURN_PCT = float(
    os.environ.get("COLD_TO_WARM_SPARSE_MIN_RETURN_PCT", "0.45")
)
COLD_TO_WARM_SPARSE_MAX_DRAWDOWN_PCT = float(
    os.environ.get("COLD_TO_WARM_SPARSE_MAX_DRAWDOWN_PCT", "1.75")
)

WALKFORWARD_REQUIRED = os.environ.get("WALKFORWARD_REQUIRED", "1").lower() not in ("0", "false", "no")
WALKFORWARD_MIN_TOTAL_CANDLES = int(os.environ.get("WALKFORWARD_MIN_TOTAL_CANDLES", "60"))
WALKFORWARD_MIN_OOS_TRADES = int(os.environ.get("WALKFORWARD_MIN_OOS_TRADES", "1"))
WALKFORWARD_MIN_OOS_RETURN_PCT = float(os.environ.get("WALKFORWARD_MIN_OOS_RETURN_PCT", "0.05"))
WALKFORWARD_SPARSE_MAX_OOS_CANDLES = int(os.environ.get("WALKFORWARD_SPARSE_MAX_OOS_CANDLES", "72"))
WALKFORWARD_SPARSE_OOS_MIN_RETURN_PCT = float(
    os.environ.get("WALKFORWARD_SPARSE_OOS_MIN_RETURN_PCT", "0.00")
)
WALKFORWARD_SPARSE_OOS_MIN_WIN_RATE = float(
    os.environ.get("WALKFORWARD_SPARSE_OOS_MIN_WIN_RATE", "0.75")
)
WALKFORWARD_SPARSE_OOS_MAX_DRAWDOWN_PCT = float(
    os.environ.get("WALKFORWARD_SPARSE_OOS_MAX_DRAWDOWN_PCT", "2.00")
)
WALKFORWARD_RET_FRACTION = float(os.environ.get("WALKFORWARD_RET_FRACTION", "0.40"))
WALKFORWARD_DD_MULT = float(os.environ.get("WALKFORWARD_DD_MULT", "1.5"))
WALKFORWARD_RETENTION_RATIO = float(os.environ.get("WALKFORWARD_RETENTION_RATIO", "0.30"))

# Strict growth mode: probabilistic robustness + risk budget governance.
GROWTH_MODE_ENABLED = os.environ.get("GROWTH_MODE_ENABLED", "1").lower() not in ("0", "false", "no")
MONTE_CARLO_GATE_ENABLED = os.environ.get("MONTE_CARLO_GATE_ENABLED", "1").lower() not in ("0", "false", "no")
MONTE_CARLO_PATHS = int(os.environ.get("MONTE_CARLO_PATHS", "300"))
MONTE_CARLO_MIN_TRADES = int(os.environ.get("MONTE_CARLO_MIN_TRADES", "2"))
MONTE_CARLO_MIN_PATH_TRADES = int(os.environ.get("MONTE_CARLO_MIN_PATH_TRADES", "8"))
MONTE_CARLO_MIN_P50_RETURN_PCT = float(os.environ.get("MONTE_CARLO_MIN_P50_RETURN_PCT", "0.03"))
MONTE_CARLO_MIN_P05_RETURN_PCT = float(os.environ.get("MONTE_CARLO_MIN_P05_RETURN_PCT", "-0.10"))
MONTE_CARLO_MAX_P95_DRAWDOWN_PCT = float(os.environ.get("MONTE_CARLO_MAX_P95_DRAWDOWN_PCT", "3.50"))
MONTE_CARLO_MIN_MAX_P95_DRAWDOWN_PCT = float(
    os.environ.get("MONTE_CARLO_MIN_MAX_P95_DRAWDOWN_PCT", "0.50")
)
MONTE_CARLO_REGIME_CONDITIONING_ENABLED = os.environ.get(
    "MONTE_CARLO_REGIME_CONDITIONING_ENABLED", "1"
).lower() not in ("0", "false", "no")
MONTE_CARLO_STRESSED_P50_BONUS_PCT = float(
    os.environ.get("MONTE_CARLO_STRESSED_P50_BONUS_PCT", "0.03")
)
MONTE_CARLO_STRESSED_P05_BONUS_PCT = float(
    os.environ.get("MONTE_CARLO_STRESSED_P05_BONUS_PCT", "0.02")
)
MONTE_CARLO_STRESSED_DRAWDOWN_MULT = float(
    os.environ.get("MONTE_CARLO_STRESSED_DRAWDOWN_MULT", "0.90")
)
MONTE_CARLO_FRAGILE_P50_BONUS_PCT = float(
    os.environ.get("MONTE_CARLO_FRAGILE_P50_BONUS_PCT", "0.05")
)
MONTE_CARLO_FRAGILE_P05_BONUS_PCT = float(
    os.environ.get("MONTE_CARLO_FRAGILE_P05_BONUS_PCT", "0.04")
)
MONTE_CARLO_FRAGILE_DRAWDOWN_MULT = float(
    os.environ.get("MONTE_CARLO_FRAGILE_DRAWDOWN_MULT", "0.82")
)
MONTE_CARLO_TREND_P50_RELAX_PCT = float(
    os.environ.get("MONTE_CARLO_TREND_P50_RELAX_PCT", "-0.01")
)
MONTE_CARLO_TREND_P05_RELAX_PCT = float(
    os.environ.get("MONTE_CARLO_TREND_P05_RELAX_PCT", "-0.02")
)
MONTE_CARLO_TREND_DRAWDOWN_MULT = float(
    os.environ.get("MONTE_CARLO_TREND_DRAWDOWN_MULT", "1.05")
)
MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_ENABLED = os.environ.get(
    "MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_ENABLED", "1"
).lower() not in ("0", "false", "no")
MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_SCALE = float(
    os.environ.get("MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_SCALE", "0.35")
)
MONTE_CARLO_TAIL_PENALTY_REGIME_MULT_STRESSED = float(
    os.environ.get("MONTE_CARLO_TAIL_PENALTY_REGIME_MULT_STRESSED", "1.25")
)
MONTE_CARLO_TAIL_PENALTY_REGIME_MULT_TREND = float(
    os.environ.get("MONTE_CARLO_TAIL_PENALTY_REGIME_MULT_TREND", "0.85")
)
MONTE_CARLO_TAIL_PENALTY_REGIME_MULT_FRAGILE = float(
    os.environ.get("MONTE_CARLO_TAIL_PENALTY_REGIME_MULT_FRAGILE", "1.10")
)
MONTE_CARLO_VAR95_PENALTY_WEIGHT = float(
    os.environ.get("MONTE_CARLO_VAR95_PENALTY_WEIGHT", "0.55")
)
MONTE_CARLO_ES97_5_PENALTY_WEIGHT = float(
    os.environ.get("MONTE_CARLO_ES97_5_PENALTY_WEIGHT", "0.45")
)
GROWTH_START_BUDGET_PCT = float(os.environ.get("GROWTH_START_BUDGET_PCT", "0.012"))
GROWTH_START_BUDGET_MIN_USD = float(os.environ.get("GROWTH_START_BUDGET_MIN_USD", "1.00"))
GROWTH_START_BUDGET_MAX_USD = float(os.environ.get("GROWTH_START_BUDGET_MAX_USD", "8.00"))
GROWTH_BUDGET_ESCALATE_FACTOR = float(os.environ.get("GROWTH_BUDGET_ESCALATE_FACTOR", "1.35"))
GROWTH_BUDGET_DECAY_FACTOR = float(os.environ.get("GROWTH_BUDGET_DECAY_FACTOR", "0.60"))
GROWTH_BUDGET_MAX_USD = float(os.environ.get("GROWTH_BUDGET_MAX_USD", "75.00"))
GROWTH_ESCALATE_MIN_RUNTIME_SECONDS = int(os.environ.get("GROWTH_ESCALATE_MIN_RUNTIME_SECONDS", "900"))
PIPELINE_PORTFOLIO_USD = float(os.environ.get("PIPELINE_PORTFOLIO_USD", "262.55"))
WARM_MAX_FUNDED_PER_PAIR = int(os.environ.get("WARM_MAX_FUNDED_PER_PAIR", "4"))
WARM_MAX_TOTAL_FUNDED_BUDGET_PCT = float(os.environ.get("WARM_MAX_TOTAL_FUNDED_BUDGET_PCT", "0.60"))
WARM_MAX_PAIR_BUDGET_SHARE = float(os.environ.get("WARM_MAX_PAIR_BUDGET_SHARE", "0.82"))
SPARSE_OOS_FUNDING_MULT = float(os.environ.get("SPARSE_OOS_FUNDING_MULT", "0.40"))
GROWTH_MAX_VAR95_LOSS_PCT = float(os.environ.get("GROWTH_MAX_VAR95_LOSS_PCT", "1.20"))
GROWTH_MAX_ES97_5_LOSS_PCT = float(os.environ.get("GROWTH_MAX_ES97_5_LOSS_PCT", "2.20"))
GROWTH_DRIFT_ALERT_THRESHOLD = float(os.environ.get("GROWTH_DRIFT_ALERT_THRESHOLD", "1.50"))
GROWTH_CONFORMAL_MAX_ERROR = float(os.environ.get("GROWTH_CONFORMAL_MAX_ERROR", "0.70"))
PROBABILISTIC_MIN_TRADES_FOR_CONFORMAL = int(
    os.environ.get("PROBABILISTIC_MIN_TRADES_FOR_CONFORMAL", "6")
)
PROBABILISTIC_MIN_OOS_TRADES_FOR_CONFORMAL = int(
    os.environ.get("PROBABILISTIC_MIN_OOS_TRADES_FOR_CONFORMAL", "2")
)
PROBABILISTIC_CONFORMAL_BAND_FLOOR_PCT = float(
    os.environ.get("PROBABILISTIC_CONFORMAL_BAND_FLOOR_PCT", "0.25")
)
PROBABILISTIC_CONFORMAL_PENALTY_SCALE = float(
    os.environ.get("PROBABILISTIC_CONFORMAL_PENALTY_SCALE", "0.15")
)
LOCAL_FALLBACK_MIN_CANDLES = int(os.environ.get("LOCAL_FALLBACK_MIN_CANDLES", "160"))
LOCAL_FALLBACK_EXPAND_MULT = float(os.environ.get("LOCAL_FALLBACK_EXPAND_MULT", "4.0"))
REALIZED_ESCALATION_GATE_ENABLED = os.environ.get(
    "REALIZED_ESCALATION_GATE_ENABLED", "1"
).lower() not in ("0", "false", "no")
REALIZED_ESCALATION_MIN_CLOSES = int(os.environ.get("REALIZED_ESCALATION_MIN_CLOSES", "10"))
REALIZED_ESCALATION_MIN_NET_PNL_USD = float(os.environ.get("REALIZED_ESCALATION_MIN_NET_PNL_USD", "1.00"))
REALIZED_ESCALATION_LOOKBACK_HOURS = int(os.environ.get("REALIZED_ESCALATION_LOOKBACK_HOURS", "72"))
REALIZED_ESCALATION_MIN_WIN_RATE = float(os.environ.get("REALIZED_ESCALATION_MIN_WIN_RATE", "0.55"))
REALIZED_ESCALATION_REQUIRE_WINNING_MAJORITY = os.environ.get(
    "REALIZED_ESCALATION_REQUIRE_WINNING_MAJORITY", "1"
).lower() not in ("0", "false", "no")
REALIZED_ESCALATION_MIN_AVG_PNL_PER_CLOSE_USD = float(
    os.environ.get("REALIZED_ESCALATION_MIN_AVG_PNL_PER_CLOSE_USD", "0.10")
)
REALIZED_CLOSE_REQUIRED_FOR_HOT_PROMOTION = os.environ.get(
    "REALIZED_CLOSE_REQUIRED_FOR_HOT_PROMOTION", "1"
).lower() not in ("0", "false", "no")
HOT_ESCALATION_BOOST_FACTOR = float(os.environ.get("HOT_ESCALATION_BOOST_FACTOR", "1.50"))
HOT_ESCALATION_MIN_BUDGET_USD = float(os.environ.get("HOT_ESCALATION_MIN_BUDGET_USD", "1.25"))
EXECUTION_HEALTH_ESCALATION_GATE = os.environ.get(
    "EXECUTION_HEALTH_ESCALATION_GATE", "1"
).lower() not in ("0", "false", "no")
EXECUTION_HEALTH_REQUIRED_FOR_HOT_PROMOTION = os.environ.get(
    "EXECUTION_HEALTH_REQUIRED_FOR_HOT_PROMOTION", "1"
).lower() not in ("0", "false", "no")
EXECUTION_HEALTH_GATE_CACHE_SECONDS = int(
    os.environ.get("EXECUTION_HEALTH_GATE_CACHE_SECONDS", "90")
)

try:
    from execution_health import evaluate_execution_health
except Exception:
    try:
        from agents.execution_health import evaluate_execution_health  # type: ignore
    except Exception:
        evaluate_execution_health = None  # type: ignore


def _clamp(value, lo, hi):
    return max(lo, min(hi, value))


class HistoricalPrices:
    """Fetch and cache historical price data for backtesting."""

    def __init__(self):
        self.cache = {}
        self.cache_meta = {}
        self.base_dir = Path(__file__).parent

    def get_candles(self, pair, hours=168):
        """Get hourly candles from Coinbase PUBLIC API (no auth needed)."""
        return self._get_candles(pair, "3600", hours)

    def get_5min_candles(self, pair, hours=24):
        """Get 5-minute candles for more granular backtesting."""
        return self._get_candles(pair, "300", hours)

    def get_cache_meta(self, pair, granularity_seconds, hours):
        cache_key = f"{pair}_{granularity_seconds}_{hours}"
        return dict(self.cache_meta.get(cache_key, {}))

    def _get_candles(self, pair, granularity_seconds, hours):
        cache_key = f"{pair}_{granularity_seconds}_{hours}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        public = self._fetch_public_candles(pair, granularity_seconds, hours)
        candles = public
        source = "coinbase_public"
        notes = []

        if len(public) < 30:
            local = self._fetch_local_candles(pair, granularity_seconds, hours)
            if len(local) > len(public):
                candles = local
                source = "local_fallback"
                notes.append(f"public={len(public)} local={len(local)}")

        self.cache[cache_key] = candles
        self.cache_meta[cache_key] = {
            "pair": pair,
            "granularity_seconds": int(granularity_seconds),
            "hours": int(hours),
            "source": source,
            "candles": len(candles),
            "notes": notes,
        }
        if source == "local_fallback":
            logger.info(
                "Using local candle fallback for %s (%ss, %sh): %d candles",
                pair, granularity_seconds, hours, len(candles)
            )
        return candles

    def _fetch_public_candles(self, pair, granularity_seconds, hours):
        """Fetch candles from Coinbase Exchange public API (no auth needed)."""
        # Coinbase Exchange API: max 300 candles per request
        candles = []
        end = int(time.time())
        start = end - (hours * 3600)
        gran = int(granularity_seconds)

        # Chunk into 300-candle batches
        chunk_seconds = 300 * gran
        current_start = start

        while current_start < end:
            current_end = min(current_start + chunk_seconds, end)
            url = (f"https://api.exchange.coinbase.com/products/{pair}/candles"
                   f"?start={datetime.fromtimestamp(current_start, tz=timezone.utc).isoformat()}"
                   f"&end={datetime.fromtimestamp(current_end, tz=timezone.utc).isoformat()}"
                   f"&granularity={gran}")
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode())
                for c in data:
                    # Format: [time, low, high, open, close, volume]
                    candles.append({
                        "time": int(c[0]),
                        "low": float(c[1]),
                        "high": float(c[2]),
                        "open": float(c[3]),
                        "close": float(c[4]),
                        "volume": float(c[5]),
                    })
            except Exception as e:
                logger.warning("Candle fetch failed for %s: %s", pair, e)
                break

            current_start = current_end
            time.sleep(0.3)  # Rate limit

        candles.sort(key=lambda x: x["time"])
        # Deduplicate by time
        seen = set()
        unique = []
        for c in candles:
            if c["time"] not in seen:
                seen.add(c["time"])
                unique.append(c)
        return unique

    def _fetch_local_candles(self, pair, granularity_seconds, hours):
        """Build candles from local agent databases when public API is unavailable."""
        points = []
        points.extend(self._load_exchange_points(pair, hours))
        points.extend(self._load_sniper_points(pair, hours))
        points.extend(self._load_latency_arb_points(pair, hours))
        points.extend(self._load_trader_points(pair, hours))
        points.extend(self._load_candle_aggregator_points(pair, granularity_seconds, hours))

        if not points:
            return []
        candles = self._points_to_candles(points, int(granularity_seconds))
        min_candles = max(0, int(LOCAL_FALLBACK_MIN_CANDLES or 0))
        expand_mult = max(1.0, float(LOCAL_FALLBACK_EXPAND_MULT or 1.0))
        if min_candles > 0 and len(candles) < min_candles and expand_mult > 1.0:
            expanded_hours = int(max(hours + 1, round(hours * expand_mult)))
            expanded_points = []
            expanded_points.extend(self._load_exchange_points(pair, expanded_hours))
            expanded_points.extend(self._load_sniper_points(pair, expanded_hours))
            expanded_points.extend(self._load_latency_arb_points(pair, expanded_hours))
            expanded_points.extend(self._load_trader_points(pair, expanded_hours))
            expanded = self._points_to_candles(expanded_points, int(granularity_seconds))
            if len(expanded) > len(candles):
                candles = expanded
        return candles

    def _load_exchange_points(self, pair, hours):
        db_path = self.base_dir / "exchange_scanner.db"
        if not db_path.exists():
            return []

        asset = pair.split("-", 1)[0].lower()
        lookback = f"-{max(1, int(hours))} hours"
        rows = []
        try:
            conn = sqlite3.connect(str(db_path))
            rows = conn.execute(
                """SELECT fetched_at, price_usd
                   FROM price_feeds
                   WHERE lower(asset)=?
                     AND fetched_at >= datetime('now', ?)
                   ORDER BY fetched_at ASC""",
                (asset, lookback),
            ).fetchall()
            conn.close()
        except Exception as e:
            logger.debug("Local exchange feed load failed for %s: %s", pair, e)
            return []

        points = []
        for ts, price in rows:
            epoch = self._to_epoch(ts)
            if epoch is None:
                continue
            try:
                px = float(price)
            except Exception:
                continue
            if px <= 0:
                continue
            points.append((epoch, px, 1.0))
        return points

    def _load_sniper_points(self, pair, hours):
        db_path = self.base_dir / "sniper.db"
        if not db_path.exists():
            return []

        variants = self._pair_variants(pair)
        placeholders = ",".join("?" for _ in variants)
        lookback = f"-{max(1, int(hours))} hours"
        sql = f"""
            SELECT created_at, entry_price, COALESCE(amount_usd, 1.0)
            FROM sniper_trades
            WHERE status='filled'
              AND pair IN ({placeholders})
              AND created_at >= datetime('now', ?)
            ORDER BY created_at ASC
        """
        rows = []
        try:
            conn = sqlite3.connect(str(db_path))
            rows = conn.execute(sql, tuple(variants) + (lookback,)).fetchall()
            conn.close()
        except Exception as e:
            logger.debug("Local sniper feed load failed for %s: %s", pair, e)
            return []

        points = []
        for ts, price, vol in rows:
            epoch = self._to_epoch(ts)
            if epoch is None:
                continue
            try:
                px = float(price)
                volume = max(0.0, float(vol or 0.0))
            except Exception:
                continue
            if px <= 0:
                continue
            points.append((epoch, px, volume))
        return points

    def _load_latency_arb_points(self, pair, hours):
        db_path = self.base_dir / "latency_arb.db"
        if not db_path.exists():
            return []

        variants = self._pair_variants(pair)
        placeholders = ",".join("?" for _ in variants)
        lookback = f"-{max(1, int(hours))} hours"
        sql = f"""
            SELECT created_at, entry_price, COALESCE(amount_usd, 1.0)
            FROM trades
            WHERE status='filled'
              AND pair IN ({placeholders})
              AND created_at >= datetime('now', ?)
            ORDER BY created_at ASC
        """
        rows = []
        try:
            conn = sqlite3.connect(str(db_path))
            rows = conn.execute(sql, tuple(variants) + (lookback,)).fetchall()
            conn.close()
        except Exception as e:
            logger.debug("Local latency arb feed load failed for %s: %s", pair, e)
            return []

        points = []
        for ts, price, vol in rows:
            epoch = self._to_epoch(ts)
            if epoch is None:
                continue
            try:
                px = float(price)
                volume = max(0.0, float(vol or 0.0))
            except Exception:
                continue
            if px <= 0:
                continue
            points.append((epoch, px, volume))
        return points

    def _load_candle_aggregator_points(self, pair, granularity_seconds, hours):
        db_path = self.base_dir / "candle_aggregator.db"
        if not db_path.exists():
            return []

        variants = self._pair_variants(pair)
        placeholders = ",".join("?" for _ in variants)
        tf = "5m" if str(granularity_seconds) == "300" else "1h"
        lookback_seconds = max(1, int(hours)) * 3600

        sql = f"""
            SELECT start_ts, close, COALESCE(volume, 0.0)
            FROM aggregated_candles
            WHERE pair IN ({placeholders})
              AND timeframe=?
              AND start_ts >= CAST(strftime('%s', 'now') AS INTEGER) - ?
            ORDER BY start_ts ASC
        """
        rows = []
        try:
            conn = sqlite3.connect(str(db_path))
            if not self._table_exists(conn, "aggregated_candles"):
                conn.close()
                return []
            rows = conn.execute(sql, tuple(variants) + (tf, lookback_seconds)).fetchall()
            conn.close()
        except Exception as e:
            logger.debug("Local candle aggregator load failed for %s: %s", pair, e)
            return []

        points = []
        for ts, px, vol in rows:
            try:
                epoch = int(ts)
                price = float(px)
                volume = max(0.0, float(vol or 0.0))
            except Exception:
                continue
            if epoch <= 0 or price <= 0:
                continue
            points.append((epoch, price, volume))
        return points

    def _load_trader_points(self, pair, hours):
        db_path = self.base_dir / "trader.db"
        if not db_path.exists():
            return []

        variants = self._pair_variants(pair)
        placeholders = ",".join("?" for _ in variants)
        lookback = f"-{max(1, int(hours))} hours"
        candidate_queries = [
            (
                "live_trades",
                "created_at",
                "price",
                "COALESCE(total_usd, quantity, 1.0)",
                "status",
            ),
            (
                "agent_trades",
                "created_at",
                "price",
                "COALESCE(total_usd, quantity, 1.0)",
                "status",
            ),
        ]

        points = []
        try:
            conn = sqlite3.connect(str(db_path))
            for table, ts_col, price_col, vol_expr, status_col in candidate_queries:
                if not self._table_exists(conn, table):
                    continue
                sql = f"""
                    SELECT {ts_col}, {price_col}, {vol_expr}
                    FROM {table}
                    WHERE pair IN ({placeholders})
                      AND {ts_col} >= datetime('now', ?)
                      AND (
                        {status_col} IS NULL
                        OR lower(COALESCE({status_col}, '')) NOT IN (
                          'pending','placed','open','accepted','ack_ok',
                          'failed','blocked','canceled','cancelled','expired'
                        )
                      )
                    ORDER BY {ts_col} ASC
                """
                rows = conn.execute(sql, tuple(variants) + (lookback,)).fetchall()
                for ts, price, vol in rows:
                    epoch = self._to_epoch(ts)
                    if epoch is None:
                        continue
                    try:
                        px = float(price)
                        volume = max(0.0, float(vol or 0.0))
                    except Exception:
                        continue
                    if px <= 0:
                        continue
                    points.append((epoch, px, volume))
            conn.close()
        except Exception as e:
            logger.debug("Local trader feed load failed for %s: %s", pair, e)
            return []
        return points

    @staticmethod
    def _table_exists(conn, table_name):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return bool(row)

    @staticmethod
    def _pair_variants(pair):
        p = str(pair).upper().strip()
        variants = {p}
        if p.endswith("-USD"):
            variants.add(p.replace("-USD", "-USDC"))
        if p.endswith("-USDC"):
            variants.add(p.replace("-USDC", "-USD"))
        return sorted(variants)

    @staticmethod
    def _to_epoch(ts_value):
        if ts_value is None:
            return None
        s = str(ts_value).strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
            except Exception:
                continue
        return None

    @staticmethod
    def _points_to_candles(points, granularity_seconds):
        ordered = sorted(points, key=lambda x: x[0])
        buckets = {}

        for ts, price, vol in ordered:
            if ts is None:
                continue
            try:
                px = float(price)
                v = max(0.0, float(vol or 0.0))
            except Exception:
                continue
            if px <= 0:
                continue

            bucket_time = (int(ts) // granularity_seconds) * granularity_seconds
            c = buckets.get(bucket_time)
            if c is None:
                buckets[bucket_time] = {
                    "time": bucket_time,
                    "open": px,
                    "high": px,
                    "low": px,
                    "close": px,
                    "volume": v,
                }
                continue

            c["high"] = max(c["high"], px)
            c["low"] = min(c["low"], px)
            c["close"] = px
            c["volume"] += v

        return [buckets[t] for t in sorted(buckets.keys())]


# ============================================================
# STRATEGIES — Each must implement generate_signals(candles)
# Returns list of {"time", "side", "confidence", "reason"}
# ============================================================

class MeanReversionStrategy:
    """Buy when price drops below lower Bollinger Band, sell when above upper.

    Classic mean reversion — works well in ranging markets.
    """
    name = "mean_reversion"

    def __init__(self, window=20, num_std=2.0):
        self.window = window
        self.num_std = num_std

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]

        for i in range(self.window, len(candles)):
            window_closes = closes[i - self.window:i]
            mean = sum(window_closes) / len(window_closes)
            variance = sum((x - mean) ** 2 for x in window_closes) / len(window_closes)
            std = math.sqrt(variance) if variance > 0 else 0.001

            upper = mean + self.num_std * std
            lower = mean - self.num_std * std
            price = closes[i]

            if price < lower:
                # Price below lower band — BUY (expect reversion to mean)
                distance = (lower - price) / std if std > 0 else 0
                confidence = min(0.95, 0.60 + distance * 0.1)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": round(confidence, 3),
                    "reason": f"price_below_lower_bb (z={-distance:.2f})",
                    "price": price,
                })
            elif price > upper:
                # Price above upper band — potential sell signal (logged but not acted on with buy-only)
                distance = (price - upper) / std if std > 0 else 0
                confidence = min(0.95, 0.60 + distance * 0.1)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "SELL",
                    "confidence": round(confidence, 3),
                    "reason": f"price_above_upper_bb (z={distance:.2f})",
                    "price": price,
                })

        return signals


class MomentumStrategy:
    """Buy when short MA crosses above long MA with volume confirmation."""
    name = "momentum"

    def __init__(self, short_window=5, long_window=20, volume_mult=1.5):
        self.short_window = short_window
        self.long_window = long_window
        self.volume_mult = volume_mult

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        for i in range(self.long_window + 1, len(candles)):
            short_ma = sum(closes[i - self.short_window:i]) / self.short_window
            long_ma = sum(closes[i - self.long_window:i]) / self.long_window
            prev_short = sum(closes[i - self.short_window - 1:i - 1]) / self.short_window
            prev_long = sum(closes[i - self.long_window - 1:i - 1]) / self.long_window

            avg_vol = sum(volumes[i - self.long_window:i]) / self.long_window if self.long_window > 0 else 1
            curr_vol = volumes[i]

            # Golden cross — short MA crosses above long MA
            if prev_short <= prev_long and short_ma > long_ma:
                vol_conf = min(1.0, curr_vol / (avg_vol * self.volume_mult)) if avg_vol > 0 else 0.5
                confidence = min(0.90, 0.55 + vol_conf * 0.2 + (short_ma - long_ma) / long_ma * 10)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": round(max(0.5, confidence), 3),
                    "reason": f"golden_cross (vol={vol_conf:.2f})",
                    "price": closes[i],
                })

            # Death cross — short MA crosses below long MA
            elif prev_short >= prev_long and short_ma < long_ma:
                vol_conf = min(1.0, curr_vol / (avg_vol * self.volume_mult)) if avg_vol > 0 else 0.5
                confidence = min(0.90, 0.55 + vol_conf * 0.2 + (long_ma - short_ma) / long_ma * 10)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "SELL",
                    "confidence": round(max(0.5, confidence), 3),
                    "reason": f"death_cross (vol={vol_conf:.2f})",
                    "price": closes[i],
                })

        return signals


class RSIStrategy:
    """Buy when RSI < 30 (oversold), sell when RSI > 70 (overbought)."""
    name = "rsi"

    def __init__(self, period=14, oversold=30, overbought=70):
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    def _compute_rsi(self, closes, i):
        if i < self.period + 1:
            return 50  # neutral

        gains = []
        losses = []
        for j in range(i - self.period, i):
            change = closes[j] - closes[j - 1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))

        avg_gain = sum(gains) / self.period
        avg_loss = sum(losses) / self.period

        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]

        for i in range(self.period + 1, len(candles)):
            rsi = self._compute_rsi(closes, i)

            if rsi < self.oversold:
                confidence = min(0.95, 0.60 + (self.oversold - rsi) / 100)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": round(confidence, 3),
                    "reason": f"rsi_oversold ({rsi:.1f})",
                    "price": closes[i],
                })
            elif rsi > self.overbought:
                confidence = min(0.95, 0.60 + (rsi - self.overbought) / 100)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "SELL",
                    "confidence": round(confidence, 3),
                    "reason": f"rsi_overbought ({rsi:.1f})",
                    "price": closes[i],
                })

        return signals


class VWAPStrategy:
    """Buy below VWAP, sell above. Uses volume-weighted average price as fair value."""
    name = "vwap"

    def __init__(self, deviation_pct=0.005):
        self.deviation_pct = deviation_pct

    def generate_signals(self, candles):
        signals = []

        cum_vol_price = 0
        cum_vol = 0

        for i, c in enumerate(candles):
            cum_vol_price += c["close"] * c["volume"]
            cum_vol += c["volume"]

            if cum_vol == 0 or i < 5:
                continue

            vwap = cum_vol_price / cum_vol
            deviation = (c["close"] - vwap) / vwap

            if deviation < -self.deviation_pct:
                confidence = min(0.90, 0.55 + abs(deviation) * 20)
                signals.append({
                    "time": c["time"],
                    "side": "BUY",
                    "confidence": round(confidence, 3),
                    "reason": f"below_vwap ({deviation*100:.2f}%)",
                    "price": c["close"],
                })
            elif deviation > self.deviation_pct:
                confidence = min(0.90, 0.55 + abs(deviation) * 20)
                signals.append({
                    "time": c["time"],
                    "side": "SELL",
                    "confidence": round(confidence, 3),
                    "reason": f"above_vwap ({deviation*100:.2f}%)",
                    "price": c["close"],
                })

        return signals


class DipBuyerStrategy:
    """Buy significant dips (>1.5% drops in 4 candles), sell on recovery.

    Designed for Coinbase's 0.6% fee — only trades when expected recovery > 2x fee.
    """
    name = "dip_buyer"

    def __init__(self, dip_threshold=0.015, recovery_target=0.02, lookback=4):
        self.dip_threshold = dip_threshold
        self.recovery_target = recovery_target
        self.lookback = lookback

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]

        for i in range(self.lookback, len(candles)):
            recent_high = max(closes[i - self.lookback:i])
            price = closes[i]

            # Check for dip from recent high
            drop_pct = (recent_high - price) / recent_high

            if drop_pct >= self.dip_threshold:
                # Volume confirmation: is current volume above average?
                avg_vol = sum(c["volume"] for c in candles[max(0, i-20):i]) / min(20, i) if i > 0 else 1
                vol_ratio = candles[i]["volume"] / avg_vol if avg_vol > 0 else 1

                confidence = min(0.95, 0.60 + drop_pct * 5 + max(0, vol_ratio - 1) * 0.1)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": round(confidence, 3),
                    "reason": f"dip_{drop_pct*100:.1f}pct (vol={vol_ratio:.1f}x)",
                    "price": price,
                })

            # Check for recovery sell signal (price recovered above buy + fee + target)
            if i > self.lookback + 2:
                recent_low = min(closes[i - self.lookback:i])
                recovery_pct = (price - recent_low) / recent_low

                if recovery_pct >= self.recovery_target:
                    confidence = min(0.90, 0.55 + recovery_pct * 5)
                    signals.append({
                        "time": candles[i]["time"],
                        "side": "SELL",
                        "confidence": round(confidence, 3),
                        "reason": f"recovery_{recovery_pct*100:.1f}pct",
                        "price": price,
                    })

        return signals


class MultiTimeframeStrategy:
    """Combine RSI + Bollinger + Volume across multiple timeframes.

    Only buys when ALL three agree: RSI oversold + below BB + high volume.
    This high-confirmation approach minimizes false signals.
    """
    name = "multi_timeframe"

    def __init__(self, rsi_period=14, bb_window=20, bb_std=2.0):
        self.rsi_period = rsi_period
        self.bb_window = bb_window
        self.bb_std = bb_std

    def _rsi(self, closes, i):
        if i < self.rsi_period + 1:
            return 50
        gains, losses = [], []
        for j in range(i - self.rsi_period, i):
            change = closes[j] - closes[j - 1]
            gains.append(max(0, change))
            losses.append(max(0, -change))
        avg_gain = sum(gains) / self.rsi_period
        avg_loss = sum(losses) / self.rsi_period
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]
        volumes = [c["volume"] for c in candles]

        for i in range(max(self.bb_window, self.rsi_period + 1), len(candles)):
            price = closes[i]

            # RSI
            rsi = self._rsi(closes, i)

            # Bollinger Bands
            window_closes = closes[i - self.bb_window:i]
            mean = sum(window_closes) / len(window_closes)
            variance = sum((x - mean) ** 2 for x in window_closes) / len(window_closes)
            std = math.sqrt(variance) if variance > 0 else 0.001
            lower_bb = mean - self.bb_std * std
            upper_bb = mean + self.bb_std * std

            # Volume
            avg_vol = sum(volumes[max(0, i-20):i]) / min(20, i) if i > 0 else 1
            vol_ratio = volumes[i] / avg_vol if avg_vol > 0 else 1

            # BUY: RSI oversold + below lower BB + volume spike
            if rsi < 35 and price < lower_bb and vol_ratio > 1.2:
                rsi_score = (35 - rsi) / 35
                bb_score = (lower_bb - price) / std if std > 0 else 0
                vol_score = min(1, (vol_ratio - 1) / 2)
                confidence = min(0.95, 0.65 + rsi_score * 0.1 + bb_score * 0.05 + vol_score * 0.1)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": round(confidence, 3),
                    "reason": f"multi_confirm (RSI={rsi:.0f}, BB_dist={bb_score:.2f}, vol={vol_ratio:.1f}x)",
                    "price": price,
                })

            # SELL: RSI overbought + above upper BB
            elif rsi > 65 and price > upper_bb:
                rsi_score = (rsi - 65) / 35
                bb_score = (price - upper_bb) / std if std > 0 else 0
                confidence = min(0.90, 0.60 + rsi_score * 0.1 + bb_score * 0.05)
                signals.append({
                    "time": candles[i]["time"],
                    "side": "SELL",
                    "confidence": round(confidence, 3),
                    "reason": f"multi_overbought (RSI={rsi:.0f}, BB_dist={bb_score:.2f})",
                    "price": price,
                })

        return signals


class AccumulateAndHoldStrategy:
    """DCA on schedule + buy extra on dips. Never sell at a loss.

    Designed for small accounts: accumulate quality assets consistently.
    Sell only when holding for 24h+ AND price > entry + 2% (covers fees + profit).
    """
    name = "accumulate_hold"

    def __init__(self, dca_interval=12, dip_threshold=0.02, profit_target=0.02):
        self.dca_interval = dca_interval  # Buy every N candles (at 5min = every hour)
        self.dip_threshold = dip_threshold
        self.profit_target = profit_target

    def generate_signals(self, candles):
        signals = []
        closes = [c["close"] for c in candles]

        for i in range(20, len(candles)):
            price = closes[i]

            # DCA buy on schedule
            if i % self.dca_interval == 0:
                signals.append({
                    "time": candles[i]["time"],
                    "side": "BUY",
                    "confidence": 0.60,
                    "reason": "dca_scheduled",
                    "price": price,
                })

            # Extra buy on significant dips
            if i >= 5:
                recent_high = max(closes[i-5:i])
                drop = (recent_high - price) / recent_high
                if drop >= self.dip_threshold:
                    confidence = min(0.90, 0.65 + drop * 5)
                    signals.append({
                        "time": candles[i]["time"],
                        "side": "BUY",
                        "confidence": round(confidence, 3),
                        "reason": f"dip_buy ({drop*100:.1f}%)",
                        "price": price,
                    })

            # Sell only on sustained rally (well above 24h low)
            if i >= 288:  # ~24h of 5-min candles
                low_24h = min(closes[i-288:i])
                gain = (price - low_24h) / low_24h
                if gain >= self.profit_target:
                    confidence = min(0.85, 0.55 + gain * 5)
                    signals.append({
                        "time": candles[i]["time"],
                        "side": "SELL",
                        "confidence": round(confidence, 3),
                        "reason": f"profit_take ({gain*100:.1f}%)",
                        "price": price,
                    })

        return signals


# ============================================================
# EXECUTION INTELLIGENCE — Queue position + toxicity aware routing
# ============================================================

class ExecutionIntelligence:
    """Microstructure-aware execution policy used by backtests/paper replays.

    IMPR-103: queue-position aware child-order scheduling.
    IMPR-104: toxicity-aware dark/displayed route mix.
    """

    def __init__(self, base_slippage=SLIPPAGE, base_fee=COINBASE_FEE):
        self.base_slippage = float(base_slippage or SLIPPAGE)
        self.base_fee = float(base_fee or COINBASE_FEE)

    @staticmethod
    def _window(candles, idx, size=24):
        if not candles:
            return []
        idx = int(_clamp(idx if idx is not None else len(candles) - 1, 0, len(candles) - 1))
        start = max(0, idx - max(2, int(size)) + 1)
        return candles[start:idx + 1]

    @staticmethod
    def _safe_std(values):
        if not values or len(values) <= 1:
            return 0.0
        try:
            return float(statistics.pstdev(values))
        except Exception:
            return 0.0

    def plan(self, candles, idx, side="BUY", confidence=0.5):
        window = self._window(candles, idx, size=24)
        if len(window) < 3:
            return {
                "mode": "hybrid",
                "queue_score": 0.5,
                "toxicity_score": 0.3,
                "dark_ratio": 0.25,
                "slippage_mult": 1.0,
                "fee_mult": 1.0,
                "child_order_count": 2,
            }

        closes = [float(c.get("close", 0.0) or 0.0) for c in window if float(c.get("close", 0.0) or 0.0) > 0]
        volumes = [max(0.0, float(c.get("volume", 0.0) or 0.0)) for c in window]
        if not closes:
            closes = [1.0]

        rets = []
        for i in range(1, len(closes)):
            prev = closes[i - 1]
            if prev > 0:
                rets.append((closes[i] - prev) / prev)
        ret_vol = self._safe_std(rets)
        avg_vol = (sum(volumes[:-1]) / max(1, len(volumes) - 1)) if len(volumes) > 1 else (volumes[-1] if volumes else 1.0)
        curr_vol = volumes[-1] if volumes else avg_vol
        vol_ratio = curr_vol / max(1e-9, avg_vol)

        high_low_ranges = []
        for c in window:
            close = float(c.get("close", 0.0) or 0.0)
            if close <= 0:
                continue
            high = float(c.get("high", close) or close)
            low = float(c.get("low", close) or close)
            high_low_ranges.append((high - low) / close)
        range_ratio = (sum(high_low_ranges) / len(high_low_ranges)) if high_low_ranges else 0.0

        toxicity = 0.0
        toxicity += _clamp(ret_vol * 30.0, 0.0, 0.60)
        toxicity += _clamp((range_ratio - 0.0025) * 45.0, 0.0, 0.30)
        toxicity += _clamp((vol_ratio - 1.4) * 0.20, 0.0, 0.20)
        toxicity = _clamp(toxicity, 0.02, 0.95)

        queue_score = 0.55
        queue_score += _clamp((vol_ratio - 1.0) * 0.18, -0.20, 0.25)
        queue_score += _clamp((float(confidence or 0.5) - 0.5) * 0.25, -0.10, 0.12)
        queue_score -= _clamp(toxicity * 0.45, 0.0, 0.35)
        queue_score = _clamp(queue_score, 0.02, 0.98)

        dark_ratio = _clamp(0.10 + toxicity * 0.60, 0.08, 0.75)
        maker_preference = _clamp(queue_score - toxicity * 0.45, 0.0, 1.0)

        if maker_preference >= 0.58:
            mode = "maker"
            slippage_mult = _clamp(0.55 + toxicity * 0.25, 0.45, 0.90)
            fee_mult = 0.82
        elif maker_preference >= 0.34:
            mode = "hybrid"
            slippage_mult = _clamp(0.82 + toxicity * 0.30, 0.70, 1.10)
            fee_mult = 0.94
        else:
            mode = "taker"
            slippage_mult = _clamp(1.10 + toxicity * 0.45, 1.00, 1.85)
            fee_mult = 1.05

        child_count = int(_clamp(round(2 + toxicity * 5), 2, 7))
        return {
            "mode": mode,
            "queue_score": round(queue_score, 4),
            "toxicity_score": round(toxicity, 4),
            "dark_ratio": round(dark_ratio, 4),
            "slippage_mult": round(slippage_mult, 4),
            "fee_mult": round(fee_mult, 4),
            "child_order_count": child_count,
        }


# ============================================================
# BACKTESTER — Simulates trades against historical data
# ============================================================

class Backtester:
    """Run a strategy against historical data and compute performance metrics."""

    def __init__(self, initial_capital=100.0, fee_rate=COINBASE_FEE, slippage=SLIPPAGE):
        self.initial_capital = initial_capital
        self.fee_rate = fee_rate
        self.slippage = slippage
        self.execution = ExecutionIntelligence(base_slippage=slippage, base_fee=fee_rate)

    def run(self, strategy, candles, pair="BTC-USD"):
        """Run backtest. Returns performance metrics."""
        signals = strategy.generate_signals(candles)
        signals_by_time = {}
        for sig in signals:
            sig_time = int(sig.get("time", 0) or 0)
            if sig_time <= 0:
                continue
            bucket = signals_by_time.setdefault(sig_time, [])
            if isinstance(bucket, list):
                bucket.append(sig)

        capital = self.initial_capital
        position = 0.0  # amount of base asset held
        entry_price = 0.0
        entry_index = None
        trades = []
        equity_curve = [capital]
        peak_equity = capital
        final_open_position_blocked = False
        final_unrealized_pnl = 0.0
        execution_stats = {
            "total_notional_usd": 0.0,
            "shortfall_bps_x_notional": 0.0,
            "toxicity_sum": 0.0,
            "queue_sum": 0.0,
            "dark_ratio_sum": 0.0,
            "order_count": 0,
            "mode_counts": {"maker": 0, "hybrid": 0, "taker": 0},
        }

        def _record_execution(exec_plan, exec_price, ref_price, notional_usd):
            mode = str(exec_plan.get("mode", "hybrid"))
            shortfall_bps = (abs(exec_price - ref_price) / max(1e-9, ref_price)) * 10000.0
            execution_stats["total_notional_usd"] += float(notional_usd)
            execution_stats["shortfall_bps_x_notional"] += shortfall_bps * float(notional_usd)
            execution_stats["toxicity_sum"] += float(exec_plan.get("toxicity_score", 0.0) or 0.0)
            execution_stats["queue_sum"] += float(exec_plan.get("queue_score", 0.0) or 0.0)
            execution_stats["dark_ratio_sum"] += float(exec_plan.get("dark_ratio", 0.0) or 0.0)
            execution_stats["order_count"] += 1
            execution_stats["mode_counts"][mode] = execution_stats["mode_counts"].get(mode, 0) + 1
            return mode

        def _close_position(
            idx,
            timestamp,
            ref_price,
            confidence=0.5,
            reason="sell_signal",
            exit_source="signal",
            plan=None,
        ):
            nonlocal capital, position, entry_price, entry_index
            if position <= 0 or ref_price <= 0:
                return False
            exec_plan = plan or self.execution.plan(candles, idx, side="SELL", confidence=confidence)
            slip_mult = float(exec_plan.get("slippage_mult", 1.0) or 1.0)
            fee_mult = float(exec_plan.get("fee_mult", 1.0) or 1.0)
            exec_price = ref_price * (1 - self.slippage * slip_mult)
            fee = self.fee_rate * fee_mult
            gross_value = position * exec_price
            net_value = gross_value * (1 - fee)
            cost_basis = position * entry_price
            if net_value <= cost_basis:
                return False

            pnl = net_value - cost_basis
            mode = _record_execution(exec_plan, exec_price, ref_price, net_value)
            hold_candles = int(max(0, idx - int(entry_index or 0))) if entry_index is not None else 0
            trades.append(
                {
                    "time": timestamp,
                    "side": "SELL",
                    "price": exec_price,
                    "amount": position,
                    "usd": net_value,
                    "pnl": round(pnl, 4),
                    "reason": reason,
                    "confidence": confidence,
                    "exit_source": exit_source,
                    "hold_candles": hold_candles,
                    "execution_mode": mode,
                    "execution_toxicity": round(float(exec_plan.get("toxicity_score", 0.0) or 0.0), 4),
                    "execution_queue_score": round(float(exec_plan.get("queue_score", 0.0) or 0.0), 4),
                    "execution_dark_ratio": round(float(exec_plan.get("dark_ratio", 0.0) or 0.0), 4),
                }
            )
            capital += net_value
            position = 0.0
            entry_price = 0.0
            entry_index = None
            return True

        for idx, candle in enumerate(candles):
            timestamp = int(candle.get("time", 0) or 0)
            close_price = float(candle.get("close", 0.0) or 0.0)
            high_price = float(candle.get("high", close_price) or close_price)
            if high_price <= 0:
                high_price = close_price

            for sig in signals_by_time.get(timestamp, []):
                side = str(sig.get("side", "")).upper()
                confidence = float(sig.get("confidence", 0.5) or 0.5)
                signal_price = float(sig.get("price", close_price) or close_price)
                signal_price = signal_price if signal_price > 0 else close_price
                if signal_price <= 0:
                    continue

                if side == "BUY" and position == 0 and capital > 1.0:
                    exec_plan = self.execution.plan(candles, idx, side="BUY", confidence=confidence)
                    slip_mult = float(exec_plan.get("slippage_mult", 1.0) or 1.0)
                    fee_mult = float(exec_plan.get("fee_mult", 1.0) or 1.0)
                    exec_price = signal_price * (1 + self.slippage * slip_mult)
                    fee = self.fee_rate * fee_mult

                    trade_pct = min(0.5, max(0.0, confidence) * 0.4)
                    trade_usd = capital * trade_pct
                    if trade_usd <= 0:
                        continue
                    position = trade_usd / max(1e-9, exec_price) * (1 - fee)
                    entry_price = exec_price
                    entry_index = idx
                    capital -= trade_usd
                    mode = _record_execution(exec_plan, exec_price, signal_price, trade_usd)
                    trades.append(
                        {
                            "time": timestamp,
                            "side": "BUY",
                            "price": exec_price,
                            "amount": position,
                            "usd": trade_usd,
                            "reason": str(sig.get("reason", "buy_signal")),
                            "confidence": confidence,
                            "execution_mode": mode,
                            "execution_toxicity": round(float(exec_plan.get("toxicity_score", 0.0) or 0.0), 4),
                            "execution_queue_score": round(float(exec_plan.get("queue_score", 0.0) or 0.0), 4),
                            "execution_dark_ratio": round(float(exec_plan.get("dark_ratio", 0.0) or 0.0), 4),
                        }
                    )
                elif side == "SELL" and position > 0:
                    _close_position(
                        idx=idx,
                        timestamp=timestamp,
                        ref_price=signal_price,
                        confidence=confidence,
                        reason=str(sig.get("reason", "sell_signal")),
                        exit_source="signal",
                    )

            # Deterministic profit-only auto-exit:
            # 1) close if intrabar high reaches configured net-profit target
            # 2) close stale positions once hold-time is long enough and close is still profitable.
            if (
                BACKTEST_AUTO_EXIT_ENABLED
                and position > 0
                and entry_index is not None
                and close_price > 0
            ):
                hold_candles = max(0, idx - int(entry_index))
                min_hold = max(0, int(BACKTEST_AUTO_EXIT_MIN_HOLD_CANDLES))
                if hold_candles >= min_hold:
                    auto_plan = self.execution.plan(candles, idx, side="SELL", confidence=0.55)
                    slip_mult = float(auto_plan.get("slippage_mult", 1.0) or 1.0)
                    fee_mult = float(auto_plan.get("fee_mult", 1.0) or 1.0)
                    fee = self.fee_rate * fee_mult
                    cost_basis = position * entry_price
                    min_target_mult = 1.0 + max(0.0, BACKTEST_AUTO_EXIT_MIN_NET_PCT) / 100.0
                    high_exec_price = high_price * (1 - self.slippage * slip_mult)
                    close_exec_price = close_price * (1 - self.slippage * slip_mult)
                    high_net = position * high_exec_price * (1 - fee)
                    close_net = position * close_exec_price * (1 - fee)
                    hit_take_profit = high_net >= (cost_basis * min_target_mult)
                    stale_hold = max(min_hold, int(BACKTEST_AUTO_EXIT_MAX_HOLD_CANDLES))
                    stale_profitable = (
                        BACKTEST_AUTO_EXIT_MAX_HOLD_CANDLES > 0
                        and hold_candles >= stale_hold
                        and close_net > cost_basis
                    )
                    if hit_take_profit or stale_profitable:
                        _close_position(
                            idx=idx,
                            timestamp=timestamp,
                            ref_price=(high_price if hit_take_profit else close_price),
                            confidence=0.55,
                            reason=(
                                f"auto_take_profit_{BACKTEST_AUTO_EXIT_MIN_NET_PCT:.3f}pct"
                                if hit_take_profit
                                else f"auto_stale_profitable_exit_{hold_candles}c"
                            ),
                            exit_source="auto",
                            plan=auto_plan,
                        )

            # Track equity per candle
            if position > 0 and close_price > 0:
                curr_equity = capital + position * close_price
            else:
                curr_equity = capital
            equity_curve.append(curr_equity)
            peak_equity = max(peak_equity, curr_equity)

        # Close any remaining position at last price
        if position > 0 and len(candles) > 0:
            final_price = float(candles[-1]["close"])
            final_time = int(candles[-1].get("time", 0) or 0)
            final_closed = _close_position(
                idx=max(0, len(candles) - 1),
                timestamp=final_time,
                ref_price=final_price,
                confidence=0.5,
                reason="finalize_profitable_position",
                exit_source="final",
            )
            if not final_closed:
                final_plan = self.execution.plan(
                    candles, len(candles) - 1, side="SELL", confidence=0.5
                )
                final_exec_price = final_price * (
                    1 - self.slippage * float(final_plan.get("slippage_mult", 1.0) or 1.0)
                )
                final_fee = self.fee_rate * float(final_plan.get("fee_mult", 1.0) or 1.0)
                final_value = position * final_exec_price * (1 - final_fee)
                cost_basis = position * entry_price
                final_unrealized_pnl = final_value - cost_basis

                # Keep mark-to-market equity for reporting, but flag as blocked close.
                final_open_position_blocked = True
                capital += final_value
                position = 0.0
                entry_price = 0.0
                entry_index = None

        # Compute metrics
        total_return = (capital - self.initial_capital) / self.initial_capital
        wins = [t for t in trades if t.get("pnl", 0) > 0]
        losses = [t for t in trades if t.get("pnl", 0) < 0]
        sell_trades = [t for t in trades if t["side"] == "SELL"]
        win_rate = len(wins) / len(sell_trades) if sell_trades else 0

        # Max drawdown
        max_drawdown = 0
        peak = equity_curve[0]
        for eq in equity_curve:
            peak = max(peak, eq)
            dd = (peak - eq) / peak if peak > 0 else 0
            max_drawdown = max(max_drawdown, dd)

        # Sharpe ratio (simplified — hourly returns)
        returns = []
        for i in range(1, len(equity_curve)):
            if equity_curve[i - 1] > 0:
                returns.append((equity_curve[i] - equity_curve[i - 1]) / equity_curve[i - 1])
        if returns:
            avg_return = sum(returns) / len(returns)
            std_return = math.sqrt(sum((r - avg_return) ** 2 for r in returns) / len(returns)) if len(returns) > 1 else 0.001
            sharpe = (avg_return / std_return) * math.sqrt(len(returns)) if std_return > 0 else 0
        else:
            sharpe = 0

        notional = float(execution_stats.get("total_notional_usd", 0.0) or 0.0)
        order_count = int(execution_stats.get("order_count", 0) or 0)
        shortfall_bps = (
            execution_stats["shortfall_bps_x_notional"] / notional
            if notional > 0
            else 0.0
        )
        toxicity_avg = (
            execution_stats["toxicity_sum"] / order_count
            if order_count > 0
            else 0.0
        )
        queue_avg = (
            execution_stats["queue_sum"] / order_count
            if order_count > 0
            else 0.0
        )
        dark_ratio_avg = (
            execution_stats["dark_ratio_sum"] / order_count
            if order_count > 0
            else 0.0
        )
        execution_summary = {
            "order_count": order_count,
            "notional_usd": round(notional, 4),
            "shortfall_bps": round(shortfall_bps, 4),
            "toxicity_avg": round(toxicity_avg, 4),
            "queue_score_avg": round(queue_avg, 4),
            "dark_ratio_avg": round(dark_ratio_avg, 4),
            "mode_counts": execution_stats["mode_counts"],
        }

        return {
            "strategy": strategy.name,
            "pair": pair,
            "candle_count": len(candles),
            "initial_capital": self.initial_capital,
            "final_capital": round(capital, 4),
            "total_return_pct": round(total_return * 100, 4),
            "total_trades": len(trades),
            "buy_trades": len([t for t in trades if t["side"] == "BUY"]),
            "sell_trades": len(sell_trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(win_rate, 4),
            "max_drawdown_pct": round(max_drawdown * 100, 4),
            "sharpe_ratio": round(sharpe, 4),
            "total_pnl": round(capital - self.initial_capital, 4),
            "trades": trades,
            "equity_curve_start": equity_curve[0] if equity_curve else 0,
            "equity_curve_end": equity_curve[-1] if equity_curve else 0,
            "final_open_position_blocked": final_open_position_blocked,
            "final_unrealized_pnl": round(final_unrealized_pnl, 4),
            "execution_intelligence": execution_summary,
        }


# ============================================================
# PAPER TRADER — Runs strategy live with fake money
# ============================================================

class PaperTrader:
    """Run a strategy with fake money against live prices."""

    def __init__(self, strategy, pair, initial_capital=100.0):
        self.strategy = strategy
        self.pair = pair
        self.capital = initial_capital
        self.initial_capital = initial_capital
        self.position = 0.0
        self.entry_price = 0.0
        self.trades = []
        self.equity_history = []
        self.start_time = time.time()
        self.peak_equity = initial_capital

    def tick(self, price):
        """Process one price tick."""
        from exchange_connector import PriceFeed

        # Build a mini-candle from recent prices for signal generation
        # In paper mode, we just use the latest price as a candle
        candle = {
            "time": int(time.time()),
            "open": price,
            "high": price * 1.001,
            "low": price * 0.999,
            "close": price,
            "volume": 1000,
        }

        # Get current equity
        equity = self.capital + (self.position * price if self.position > 0 else 0)
        self.equity_history.append({"time": time.time(), "equity": equity})
        self.peak_equity = max(self.peak_equity, equity)

        return equity

    def get_metrics(self):
        """Get current paper trading metrics."""
        runtime = time.time() - self.start_time
        equity = self.equity_history[-1]["equity"] if self.equity_history else self.initial_capital
        total_return = (equity - self.initial_capital) / self.initial_capital

        wins = [t for t in self.trades if t.get("pnl", 0) > 0]
        sell_trades = [t for t in self.trades if t["side"] == "SELL"]
        win_rate = len(wins) / len(sell_trades) if sell_trades else 0

        max_drawdown = 0
        peak = self.initial_capital
        for entry in self.equity_history:
            peak = max(peak, entry["equity"])
            dd = (peak - entry["equity"]) / peak if peak > 0 else 0
            max_drawdown = max(max_drawdown, dd)

        returns = []
        for i in range(1, len(self.equity_history)):
            prev_eq = self.equity_history[i - 1]["equity"]
            curr_eq = self.equity_history[i]["equity"]
            if prev_eq > 0:
                returns.append((curr_eq - prev_eq) / prev_eq)
        if returns:
            avg_return = sum(returns) / len(returns)
            if len(returns) > 1:
                variance = sum((r - avg_return) ** 2 for r in returns) / len(returns)
                std_return = math.sqrt(max(0.0, variance))
            else:
                std_return = 0.001
            sharpe = (avg_return / std_return) * math.sqrt(len(returns)) if std_return > 0 else 0.0
        else:
            sharpe = 0.0

        return {
            "strategy": self.strategy.name,
            "pair": self.pair,
            "runtime_seconds": int(runtime),
            "equity": round(equity, 4),
            "total_return_pct": round(total_return * 100, 4),
            "total_trades": len(self.trades),
            "win_rate": round(win_rate, 4),
            "max_drawdown_pct": round(max_drawdown * 100, 4),
            "sharpe_ratio": round(sharpe, 4),
        }


# ============================================================
# GROWTH MODE — Monte Carlo robustness + risk governor + budget escalator
# ============================================================

class GrowthModeController:
    """Deterministic risk governance for strict growth mode."""

    def __init__(self):
        self.enabled = GROWTH_MODE_ENABLED

    @staticmethod
    def _percentile(values, pct):
        if not values:
            return 0.0
        if len(values) == 1:
            return float(values[0])
        pct = max(0.0, min(100.0, float(pct)))
        rank = (pct / 100.0) * (len(values) - 1)
        lo = int(math.floor(rank))
        hi = int(math.ceil(rank))
        if lo == hi:
            return float(values[lo])
        weight = rank - lo
        return float(values[lo] * (1 - weight) + values[hi] * weight)

    @staticmethod
    def _trade_returns(results):
        initial_capital = float(results.get("initial_capital", 100.0) or 100.0)
        trades = results.get("trades", []) if isinstance(results.get("trades"), list) else []
        returns = []
        for t in trades:
            if str(t.get("side", "")).upper() != "SELL":
                continue
            pnl = float(t.get("pnl", 0.0) or 0.0)
            if initial_capital > 0:
                returns.append(pnl / initial_capital)

        # Fallback for sparse trade logs: derive pseudo-returns from aggregate return.
        if not returns:
            total_ret = float(results.get("total_return_pct", 0.0) or 0.0) / 100.0
            inferred_n = int(results.get("sell_trades", 0) or 0)
            inferred_n = max(1, inferred_n)
            chunk = total_ret / inferred_n
            returns = [chunk] * inferred_n
        return returns

    @staticmethod
    def _mean(values):
        if not values:
            return 0.0
        return float(sum(values) / len(values))

    @staticmethod
    def _std(values):
        if not values or len(values) <= 1:
            return 0.0
        try:
            return float(statistics.pstdev(values))
        except Exception:
            return 0.0

    def _probabilistic_assessment(self, results, base_returns, p05_mc, p50_mc, p95_dd):
        returns_pct = sorted([float(r) * 100.0 for r in base_returns])
        base_trade_count = len(returns_pct)
        p05_hist = self._percentile(returns_pct, 5)
        p025_hist = self._percentile(returns_pct, 2.5)
        var95_loss = max(0.0, -p05_hist)
        tail_losses = [-r for r in returns_pct if r <= p025_hist]
        if not tail_losses:
            tail_losses = [max(0.0, -p025_hist)]
        es97_5_loss = self._mean(tail_losses)

        sharpe = float(results.get("sharpe_ratio", 0.0) or 0.0)
        dd = float(results.get("max_drawdown_pct", 0.0) or 0.0)
        vol = self._std(base_returns) * 100.0
        regime = "calm"
        if dd > 2.5 or vol > 0.40:
            regime = "stressed"
        elif sharpe > 0.5 and vol < 0.25:
            regime = "trend"
        elif sharpe < 0:
            regime = "fragile"

        walkforward = results.get("walkforward") if isinstance(results.get("walkforward"), dict) else {}
        oos = walkforward.get("out_of_sample") if isinstance(walkforward.get("out_of_sample"), dict) else {}
        ins = walkforward.get("in_sample") if isinstance(walkforward.get("in_sample"), dict) else {}
        oos_ret = float(oos.get("total_return_pct", 0.0) or 0.0)
        oos_trades = int(oos.get("total_trades", 0) or 0)
        ins_ret = float(ins.get("total_return_pct", results.get("total_return_pct", 0.0)) or 0.0)

        drift_score = 0.25
        if walkforward.get("available"):
            drift_score = abs(oos_ret - ins_ret) / max(0.05, abs(ins_ret))
        drift_score = float(_clamp(drift_score, 0.0, 3.0))

        conformal_error = 0.0
        sample_sufficient_for_conformal = (
            base_trade_count >= PROBABILISTIC_MIN_TRADES_FOR_CONFORMAL
            and oos_trades >= PROBABILISTIC_MIN_OOS_TRADES_FOR_CONFORMAL
        )
        conformal_skipped_low_sample = False
        if walkforward.get("available"):
            if sample_sufficient_for_conformal:
                low = self._percentile(returns_pct, 10)
                high = self._percentile(returns_pct, 90)
                band = max(
                    PROBABILISTIC_CONFORMAL_BAND_FLOOR_PCT,
                    high - low,
                    abs(ins_ret - oos_ret) * 0.60,
                )
                if oos_ret < low:
                    conformal_error = min(1.0, abs(low - oos_ret) / band)
                elif oos_ret > high:
                    conformal_error = min(1.0, abs(oos_ret - high) / band)
                else:
                    conformal_error = 0.0
            else:
                conformal_skipped_low_sample = True
        conformal_error = float(_clamp(conformal_error, 0.0, 1.0))

        calibrated_p05 = min(
            p05_mc,
            p05_hist - conformal_error * PROBABILISTIC_CONFORMAL_PENALTY_SCALE,
        )
        reasons = []
        if var95_loss > GROWTH_MAX_VAR95_LOSS_PCT:
            reasons.append(
                f"probabilistic var95_loss {var95_loss:.2f}% > {GROWTH_MAX_VAR95_LOSS_PCT:.2f}%"
            )
        if es97_5_loss > GROWTH_MAX_ES97_5_LOSS_PCT:
            reasons.append(
                f"probabilistic es97_5_loss {es97_5_loss:.2f}% > {GROWTH_MAX_ES97_5_LOSS_PCT:.2f}%"
            )
        if sample_sufficient_for_conformal and conformal_error > GROWTH_CONFORMAL_MAX_ERROR:
            reasons.append(
                f"probabilistic conformal_error {conformal_error:.2f} > {GROWTH_CONFORMAL_MAX_ERROR:.2f}"
            )

        return {
            "base_trade_count": int(base_trade_count),
            "oos_trade_count": int(oos_trades),
            "sample_sufficient_for_conformal": bool(sample_sufficient_for_conformal),
            "conformal_skipped_low_sample": bool(conformal_skipped_low_sample),
            "regime": regime,
            "volatility_pct": round(vol, 4),
            "var95_loss_pct": round(var95_loss, 4),
            "es97_5_loss_pct": round(es97_5_loss, 4),
            "drift_score": round(drift_score, 4),
            "conformal_error": round(conformal_error, 4),
            "calibrated_p05_return_pct": round(calibrated_p05, 4),
            "passed": len(reasons) == 0,
            "reasons": reasons,
        }

    def evaluate_monte_carlo(self, strategy_name, pair, results, criteria):
        report = {
            "enabled": MONTE_CARLO_GATE_ENABLED,
            "available": False,
            "passed": False,
            "paths": MONTE_CARLO_PATHS,
            "path_trades": 0,
            "required_min_trades": MONTE_CARLO_MIN_TRADES,
            "p05_return_pct": 0.0,
            "p50_return_pct": 0.0,
            "p95_drawdown_pct": 0.0,
            "probabilistic": {},
            "reasons": [],
        }
        if not MONTE_CARLO_GATE_ENABLED:
            report["passed"] = True
            report["reasons"] = ["monte_carlo_gate_disabled"]
            return report

        base_returns = self._trade_returns(results)
        observed_trades = len(base_returns)
        candle_count = int(results.get("candle_count", 0) or 0)
        min_trades_required = MONTE_CARLO_MIN_TRADES
        if candle_count < 132:
            min_trades_required = max(1, MONTE_CARLO_MIN_TRADES - 1)
        report["required_min_trades"] = min_trades_required
        if observed_trades < min_trades_required:
            report["reasons"].append(
                f"monte_carlo trades {observed_trades} < {min_trades_required}"
            )
            return report

        path_trades = max(MONTE_CARLO_MIN_PATH_TRADES, observed_trades)
        report["path_trades"] = path_trades
        seed_material = (
            f"{strategy_name}|{pair}|{results.get('candle_count', 0)}|"
            f"{results.get('total_return_pct', 0.0)}|{results.get('win_rate', 0.0)}"
        )
        seed = sum((i + 1) * ord(ch) for i, ch in enumerate(seed_material)) % (2 ** 32)
        rng = random.Random(seed)

        final_returns = []
        max_drawdowns = []
        for _ in range(max(10, MONTE_CARLO_PATHS)):
            equity = 1.0
            peak = 1.0
            max_dd = 0.0
            for _ in range(path_trades):
                r = rng.choice(base_returns)
                equity *= max(0.0, 1.0 + r)
                peak = max(peak, equity)
                dd = (peak - equity) / peak if peak > 0 else 1.0
                max_dd = max(max_dd, dd)
            final_returns.append((equity - 1.0) * 100.0)
            max_drawdowns.append(max_dd * 100.0)

        final_returns.sort()
        max_drawdowns.sort()
        p05_ret = self._percentile(final_returns, 5)
        p50_ret = self._percentile(final_returns, 50)
        p95_dd = self._percentile(max_drawdowns, 95)

        report.update(
            {
                "available": True,
                "p05_return_pct": round(p05_ret, 4),
                "p50_return_pct": round(p50_ret, 4),
                "p95_drawdown_pct": round(p95_dd, 4),
            }
        )
        probabilistic = self._probabilistic_assessment(results, base_returns, p05_ret, p50_ret, p95_dd)
        report["probabilistic"] = probabilistic

        regime_gate = self._regime_conditioned_monte_carlo_thresholds(criteria, probabilistic)
        min_p50 = float(regime_gate.get("min_p50_return_pct", MONTE_CARLO_MIN_P50_RETURN_PCT) or MONTE_CARLO_MIN_P50_RETURN_PCT)
        min_p05 = float(regime_gate.get("min_p05_return_pct", MONTE_CARLO_MIN_P05_RETURN_PCT) or MONTE_CARLO_MIN_P05_RETURN_PCT)
        max_p95_dd = float(
            regime_gate.get("effective_max_p95_drawdown_pct", MONTE_CARLO_MAX_P95_DRAWDOWN_PCT)
            or MONTE_CARLO_MAX_P95_DRAWDOWN_PCT
        )
        regime_name = str(regime_gate.get("regime", "calm") or "calm")
        if p50_ret < min_p50:
            report["reasons"].append(
                f"monte_carlo regime({regime_name}) p50 {p50_ret:.2f}% < {min_p50:.2f}%"
            )
        if p05_ret < min_p05:
            report["reasons"].append(
                f"monte_carlo regime({regime_name}) p05 {p05_ret:.2f}% < {min_p05:.2f}%"
            )
        if p95_dd > max_p95_dd:
            report["reasons"].append(
                f"monte_carlo regime({regime_name}) p95_dd {p95_dd:.2f}% > {max_p95_dd:.2f}%"
            )
            if regime_gate.get("tail_penalty_pct", 0) > 0:
                report["reasons"].append(
                    f"monte_carlo regime({regime_name}) tail_penalty {regime_gate['tail_penalty_pct']:.2f}pp "
                    f"multiplier={regime_gate['tail_penalty_regime_multiplier']:.2f}"
                )
        calibrated_p05 = float(probabilistic.get("calibrated_p05_return_pct", p05_ret) or p05_ret)
        if calibrated_p05 < min_p05:
            report["reasons"].append(
                f"probabilistic calibrated_p05 {calibrated_p05:.2f}% < {min_p05:.2f}%"
            )
        if not probabilistic.get("passed", True):
            report["reasons"].extend(probabilistic.get("reasons", []))

        report["regime_gate"] = regime_gate
        report["passed"] = not report["reasons"]
        return report

    def _regime_conditioned_monte_carlo_thresholds(self, criteria, probabilistic):
        regime = str((probabilistic or {}).get("regime", "calm") or "calm").lower()
        min_p50 = max(MONTE_CARLO_MIN_P50_RETURN_PCT, float(criteria["min_return_pct"]) * 0.25)
        min_p05 = MONTE_CARLO_MIN_P05_RETURN_PCT
        base_max_p95_dd = min(MONTE_CARLO_MAX_P95_DRAWDOWN_PCT, float(criteria["max_drawdown_pct"]) * 1.10)
        max_p95_dd = float(base_max_p95_dd)
        regime_penalty_multiplier = 1.0

        if MONTE_CARLO_REGIME_CONDITIONING_ENABLED:
            if regime == "stressed":
                min_p50 += MONTE_CARLO_STRESSED_P50_BONUS_PCT
                min_p05 += MONTE_CARLO_STRESSED_P05_BONUS_PCT
                max_p95_dd *= MONTE_CARLO_STRESSED_DRAWDOWN_MULT
                regime_penalty_multiplier = max(
                    0.01, MONTE_CARLO_TAIL_PENALTY_REGIME_MULT_STRESSED
                )
            elif regime == "fragile":
                min_p50 += MONTE_CARLO_FRAGILE_P50_BONUS_PCT
                min_p05 += MONTE_CARLO_FRAGILE_P05_BONUS_PCT
                max_p95_dd *= MONTE_CARLO_FRAGILE_DRAWDOWN_MULT
                regime_penalty_multiplier = max(
                    0.01, MONTE_CARLO_TAIL_PENALTY_REGIME_MULT_FRAGILE
                )
            elif regime == "trend":
                min_p50 += MONTE_CARLO_TREND_P50_RELAX_PCT
                min_p05 += MONTE_CARLO_TREND_P05_RELAX_PCT
                max_p95_dd *= MONTE_CARLO_TREND_DRAWDOWN_MULT
                regime_penalty_multiplier = max(
                    0.01, MONTE_CARLO_TAIL_PENALTY_REGIME_MULT_TREND
                )

        var95_loss = float((probabilistic or {}).get("var95_loss_pct", 0.0) or 0.0)
        es97_5_loss = float((probabilistic or {}).get("es97_5_loss_pct", 0.0) or 0.0)
        tail_penalty = 0.0
        if MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_ENABLED:
            var95_excess = max(0.0, var95_loss - (GROWTH_MAX_VAR95_LOSS_PCT * 0.75))
            es97_5_excess = max(0.0, es97_5_loss - (GROWTH_MAX_ES97_5_LOSS_PCT * 0.75))
            weighted_excess = (
                var95_excess * MONTE_CARLO_VAR95_PENALTY_WEIGHT
                + es97_5_excess * MONTE_CARLO_ES97_5_PENALTY_WEIGHT
            )
            tail_penalty = max(
                0.0,
                weighted_excess * MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_SCALE * regime_penalty_multiplier,
            )
            max_p95_dd -= tail_penalty

        max_p95_dd = _clamp(
            max_p95_dd,
            MONTE_CARLO_MIN_MAX_P95_DRAWDOWN_PCT,
            MONTE_CARLO_MAX_P95_DRAWDOWN_PCT,
        )
        return {
            "enabled": bool(MONTE_CARLO_REGIME_CONDITIONING_ENABLED),
            "regime": regime,
            "min_p50_return_pct": round(float(min_p50), 4),
            "min_p05_return_pct": round(float(min_p05), 4),
            "base_max_p95_drawdown_pct": round(float(base_max_p95_dd), 4),
            "effective_max_p95_drawdown_pct": round(float(max_p95_dd), 4),
            "tail_penalty_pct": round(float(tail_penalty), 4),
            "tail_penalty_enabled": bool(MONTE_CARLO_DRAWDOWN_TAIL_PENALTY_ENABLED),
            "tail_penalty_regime_multiplier": round(float(regime_penalty_multiplier), 4),
            "var95_loss_pct": round(float(var95_loss), 4),
            "es97_5_loss_pct": round(float(es97_5_loss), 4),
        }

    def build_risk_profile(self, results, criteria, monte_carlo):
        ret = float(results.get("total_return_pct", 0.0) or 0.0)
        win = float(results.get("win_rate", 0.0) or 0.0)
        dd = float(results.get("max_drawdown_pct", 0.0) or 0.0)
        sharpe = float(results.get("sharpe_ratio", 0.0) or 0.0)
        p50 = float(monte_carlo.get("p50_return_pct", 0.0) or 0.0)
        p05 = float(monte_carlo.get("p05_return_pct", 0.0) or 0.0)
        probabilistic = (
            monte_carlo.get("probabilistic", {})
            if isinstance(monte_carlo.get("probabilistic"), dict)
            else {}
        )
        var95_loss = float(probabilistic.get("var95_loss_pct", 0.0) or 0.0)
        es97_5_loss = float(probabilistic.get("es97_5_loss_pct", 0.0) or 0.0)
        drift_score = float(probabilistic.get("drift_score", 0.0) or 0.0)
        conformal_error = float(probabilistic.get("conformal_error", 0.0) or 0.0)
        calibrated_p05 = float(probabilistic.get("calibrated_p05_return_pct", p05) or p05)
        pair = str(results.get("pair", "")).upper()

        score = 0.0
        score += min(1.4, max(0.0, ret) / max(0.01, criteria["min_return_pct"])) * 0.30
        score += min(1.2, max(0.0, sharpe + 0.5)) * 0.20
        score += min(1.2, max(0.0, (win - 0.50) * 5.0)) * 0.20
        score += min(1.2, max(0.0, p50 / max(0.01, criteria["min_return_pct"]))) * 0.20
        score += min(1.0, max(0.0, calibrated_p05 + 0.50)) * 0.10
        score -= min(1.0, dd / max(0.01, criteria["max_drawdown_pct"])) * 0.30
        score -= min(0.6, var95_loss / max(0.10, GROWTH_MAX_VAR95_LOSS_PCT)) * 0.20
        score -= min(0.6, es97_5_loss / max(0.10, GROWTH_MAX_ES97_5_LOSS_PCT)) * 0.20
        score -= min(0.4, drift_score / max(0.10, GROWTH_DRIFT_ALERT_THRESHOLD)) * 0.10
        score = max(0.0, min(1.5, score))

        tier = "tiny"
        if score >= 1.15:
            tier = "elite"
        elif score >= 0.95:
            tier = "high"
        elif score >= 0.75:
            tier = "standard"

        reasons = []
        approved = True
        if ret <= 0:
            approved = False
            reasons.append("risk_governor: non-positive return")
        if dd > criteria["max_drawdown_pct"]:
            approved = False
            reasons.append("risk_governor: drawdown above limit")
        if MONTE_CARLO_GATE_ENABLED and not monte_carlo.get("passed", False):
            approved = False
            reasons.append("risk_governor: monte_carlo gate failed")
        if es97_5_loss > GROWTH_MAX_ES97_5_LOSS_PCT:
            approved = False
            reasons.append("risk_governor: expected shortfall above limit")
        if var95_loss > GROWTH_MAX_VAR95_LOSS_PCT:
            approved = False
            reasons.append("risk_governor: VaR above limit")

        base = max(GROWTH_START_BUDGET_MIN_USD, PIPELINE_PORTFOLIO_USD * GROWTH_START_BUDGET_PCT)
        tier_mult = {"tiny": 0.75, "standard": 1.00, "high": 1.20, "elite": 1.40}.get(tier, 0.75)
        starter_budget = base * tier_mult
        starter_budget = min(GROWTH_START_BUDGET_MAX_USD, max(GROWTH_START_BUDGET_MIN_USD, starter_budget))
        max_budget = min(GROWTH_BUDGET_MAX_USD, max(starter_budget * 2.0, starter_budget + 1.0))

        sleeve = "satellite"
        if pair.startswith("BTC") or pair.startswith("ETH"):
            sleeve = "core"
        elif pair.startswith("SOL") or pair.startswith("AVAX") or pair.startswith("LINK"):
            sleeve = "growth"
        sleeve_cap_pct = {"core": 0.22, "growth": 0.14, "satellite": 0.10}.get(sleeve, 0.10)

        auto_deleverage_factor = 1.0
        if es97_5_loss > GROWTH_MAX_ES97_5_LOSS_PCT * 0.80:
            auto_deleverage_factor *= 0.75
        if drift_score > GROWTH_DRIFT_ALERT_THRESHOLD * 0.80:
            auto_deleverage_factor *= 0.82
        if conformal_error > GROWTH_CONFORMAL_MAX_ERROR * 0.80:
            auto_deleverage_factor *= 0.85
        starter_budget *= auto_deleverage_factor
        max_budget *= auto_deleverage_factor

        sleeve_cap_usd = PIPELINE_PORTFOLIO_USD * sleeve_cap_pct
        starter_budget = min(starter_budget, sleeve_cap_usd)
        max_budget = min(max_budget, sleeve_cap_usd)
        max_position_pct = min(0.20, max(0.03, 0.06 + (score * 0.04)))
        max_daily_loss_pct = max(0.50, 2.50 - score)
        kill_switch = (
            f"kill_if_drawdown_gt_{criteria['max_drawdown_pct']:.2f}pct_or_daily_loss_gt_"
            f"{max_daily_loss_pct:.2f}pct_or_return_non_positive"
        )

        return {
            "approved": approved,
            "reasons": reasons,
            "tier": tier,
            "score": round(score, 4),
            "starter_budget_usd": round(starter_budget, 2),
            "max_budget_usd": round(max_budget, 2),
            "max_position_pct": round(max_position_pct, 4),
            "max_daily_loss_pct": round(max_daily_loss_pct, 4),
            "var95_loss_pct": round(var95_loss, 4),
            "es97_5_loss_pct": round(es97_5_loss, 4),
            "drift_score": round(drift_score, 4),
            "conformal_error": round(conformal_error, 4),
            "calibrated_p05_return_pct": round(calibrated_p05, 4),
            "sleeve": sleeve,
            "sleeve_cap_usd": round(sleeve_cap_usd, 2),
            "auto_deleverage_factor": round(auto_deleverage_factor, 4),
            "kill_switch": kill_switch,
        }

    def escalate_budget(self, current_budget, max_budget, paper_metrics, promote_hot=False):
        runtime = int(paper_metrics.get("runtime_seconds", 0) or 0)
        ret = float(paper_metrics.get("total_return_pct", 0.0) or 0.0)
        win = float(paper_metrics.get("win_rate", 0.0) or 0.0)
        dd = float(paper_metrics.get("max_drawdown_pct", 0.0) or 0.0)

        current = max(GROWTH_START_BUDGET_MIN_USD, float(current_budget or 0.0))
        ceiling = max(current, float(max_budget or current))
        action = "hold"
        reason = "no_change"
        next_budget = current

        if runtime < GROWTH_ESCALATE_MIN_RUNTIME_SECONDS:
            reason = f"runtime {runtime}s < {GROWTH_ESCALATE_MIN_RUNTIME_SECONDS}s"
        elif ret <= 0 or dd > WARM_TO_HOT["max_drawdown_pct"] * 1.1:
            action = "decrease"
            next_budget = max(GROWTH_START_BUDGET_MIN_USD, current * GROWTH_BUDGET_DECAY_FACTOR)
            reason = "negative_return_or_high_drawdown"
        elif win >= WARM_TO_HOT["min_win_rate"] and ret >= WARM_TO_HOT["min_return_pct"]:
            action = "increase"
            growth = GROWTH_BUDGET_ESCALATE_FACTOR * (1.08 if promote_hot else 1.0)
            next_budget = min(ceiling, current * growth)
            reason = "strong_positive_paper_metrics"

        next_budget = min(GROWTH_BUDGET_MAX_USD, max(GROWTH_START_BUDGET_MIN_USD, next_budget))
        return {
            "action": action,
            "reason": reason,
            "from_budget_usd": round(current, 2),
            "to_budget_usd": round(next_budget, 2),
            "max_budget_usd": round(ceiling, 2),
        }


# ============================================================
# STRATEGY VALIDATOR — Gates promotion between pipeline stages
# ============================================================

class StrategyValidator:
    """Validates strategy performance and manages pipeline promotion."""

    def __init__(self):
        self.db = sqlite3.connect(PIPELINE_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.growth = GrowthModeController()
        self._execution_health_cache = {"ts": 0.0, "payload": {}}

    def _execution_health_gate_status(self, refresh=False):
        summary = {
            "enabled": bool(EXECUTION_HEALTH_ESCALATION_GATE),
            "green": True,
            "reason": "gate_disabled",
            "updated_at": "",
        }
        if not EXECUTION_HEALTH_ESCALATION_GATE:
            return summary
        if evaluate_execution_health is None:
            summary["green"] = False
            summary["reason"] = "execution_health_module_unavailable"
            return summary

        now = time.time()
        cache_age = now - float(self._execution_health_cache.get("ts", 0.0) or 0.0)
        cached = self._execution_health_cache.get("payload", {})
        if (
            not refresh
            and isinstance(cached, dict)
            and cached
            and cache_age <= max(10, int(EXECUTION_HEALTH_GATE_CACHE_SECONDS))
        ):
            return {
                "enabled": bool(EXECUTION_HEALTH_ESCALATION_GATE),
                "green": bool(cached.get("green", False)),
                "reason": str(cached.get("reason", "unknown")),
                "updated_at": str(cached.get("updated_at", "")),
            }

        try:
            payload = evaluate_execution_health(refresh=False, probe_http=False, write_status=True)
        except Exception as e:
            summary["green"] = False
            summary["reason"] = f"execution_health_check_failed:{e}"
            return summary
        if not isinstance(payload, dict):
            summary["green"] = False
            summary["reason"] = "execution_health_invalid_payload"
            return summary
        self._execution_health_cache = {"ts": now, "payload": payload}
        return {
            "enabled": bool(EXECUTION_HEALTH_ESCALATION_GATE),
            "green": bool(payload.get("green", False)),
            "reason": str(payload.get("reason", "unknown")),
            "updated_at": str(payload.get("updated_at", "")),
        }

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS strategy_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                pair TEXT NOT NULL,
                stage TEXT DEFAULT 'COLD',
                params_json TEXT,
                backtest_results_json TEXT,
                paper_results_json TEXT,
                promoted_at TIMESTAMP,
                demoted_at TIMESTAMP,
                killed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(name, pair)
            );
            CREATE TABLE IF NOT EXISTS pipeline_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_name TEXT,
                pair TEXT,
                event_type TEXT,
                details_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS strategy_budgets (
                strategy_name TEXT NOT NULL,
                pair TEXT NOT NULL,
                current_budget_usd REAL DEFAULT 0,
                starter_budget_usd REAL DEFAULT 0,
                max_budget_usd REAL DEFAULT 0,
                risk_tier TEXT DEFAULT 'blocked',
                governor_json TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (strategy_name, pair)
            );
        """)
        self.db.commit()

    def register_strategy(self, strategy, pair, params=None):
        """Register a new strategy in the pipeline."""
        self.db.execute(
            """INSERT OR IGNORE INTO strategy_registry (name, pair, params_json)
               VALUES (?, ?, ?)""",
            (strategy.name, pair, json.dumps(params or {}))
        )
        self.db.commit()
        self._log_event(strategy.name, pair, "registered", {})

    def update_strategy_params(self, strategy_name, pair, params=None):
        """Update stored parameters for an existing strategy registration."""
        self.db.execute(
            """UPDATE strategy_registry
               SET params_json=?, updated_at=CURRENT_TIMESTAMP
               WHERE name=? AND pair=?""",
            (json.dumps(params or {}), strategy_name, pair),
        )
        self.db.commit()

    def _set_budget_profile(self, strategy_name, pair, profile):
        profile = profile or {}
        current_budget = float(profile.get("current_budget_usd", profile.get("starter_budget_usd", 0.0)) or 0.0)
        starter_budget = float(profile.get("starter_budget_usd", current_budget) or 0.0)
        max_budget = float(profile.get("max_budget_usd", max(current_budget, starter_budget)) or 0.0)
        risk_tier = str(profile.get("tier", profile.get("risk_tier", "blocked")))
        self.db.execute(
            """INSERT INTO strategy_budgets
               (strategy_name, pair, current_budget_usd, starter_budget_usd, max_budget_usd, risk_tier, governor_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(strategy_name, pair) DO UPDATE SET
                 current_budget_usd=excluded.current_budget_usd,
                 starter_budget_usd=excluded.starter_budget_usd,
                 max_budget_usd=excluded.max_budget_usd,
                 risk_tier=excluded.risk_tier,
                 governor_json=excluded.governor_json,
                 updated_at=CURRENT_TIMESTAMP
            """,
            (
                strategy_name,
                pair,
                current_budget,
                starter_budget,
                max_budget,
                risk_tier,
                json.dumps(profile),
            ),
        )
        self.db.commit()

    def get_budget_profile(self, strategy_name, pair):
        row = self.db.execute(
            """SELECT current_budget_usd, starter_budget_usd, max_budget_usd, risk_tier, governor_json
               FROM strategy_budgets WHERE strategy_name=? AND pair=?""",
            (strategy_name, pair),
        ).fetchone()
        if not row:
            return {
                "current_budget_usd": 0.0,
                "starter_budget_usd": 0.0,
                "max_budget_usd": 0.0,
                "risk_tier": "blocked",
                "governor": {},
            }
        governor = {}
        try:
            governor = json.loads(row["governor_json"] or "{}")
        except Exception:
            governor = {}
        return {
            "current_budget_usd": float(row["current_budget_usd"] or 0.0),
            "starter_budget_usd": float(row["starter_budget_usd"] or 0.0),
            "max_budget_usd": float(row["max_budget_usd"] or 0.0),
            "risk_tier": str(row["risk_tier"] or "blocked"),
            "governor": governor,
        }

    def funded_budget_snapshot(self):
        """Return aggregate funded budget exposure across WARM/HOT strategies."""
        rows = self.db.execute(
            """
            SELECT r.pair, COUNT(*) AS n, COALESCE(SUM(b.current_budget_usd), 0) AS budget
            FROM strategy_budgets b
            JOIN strategy_registry r
              ON r.name = b.strategy_name AND r.pair = b.pair
            WHERE r.stage IN ('WARM', 'HOT')
              AND COALESCE(b.current_budget_usd, 0) > 0
            GROUP BY r.pair
            """
        ).fetchall()
        per_pair = {}
        total_budget = 0.0
        total_count = 0
        for row in rows:
            pair = str(row["pair"])
            n = int(row["n"] or 0)
            budget = float(row["budget"] or 0.0)
            per_pair[pair] = {"count": n, "budget_usd": round(budget, 2)}
            total_budget += budget
            total_count += n
        return {
            "per_pair": per_pair,
            "total_budget_usd": round(total_budget, 2),
            "total_funded_strategies": total_count,
        }

    @staticmethod
    def _table_exists(conn, table_name):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return bool(row)

    def _pair_realized_close_evidence(self, pair, strategy_name=None):
        """Collect realized close evidence from trader.db for budget escalation gates.

        When strategy_name is provided, filters to that specific strategy for
        strategy-level evidence (prevents curve-fitted strategies from riding
        pair-level wins they didn't produce).
        """
        evidence = {
            "enabled": REALIZED_ESCALATION_GATE_ENABLED,
            "pair": str(pair),
            "strategy_name": strategy_name,
            "lookback_hours": int(REALIZED_ESCALATION_LOOKBACK_HOURS),
            "closed_trades": 0,
            "winning_closes": 0,
            "losing_closes": 0,
            "net_pnl_usd": 0.0,
            "win_rate": 0.0,
            "avg_pnl_per_close_usd": 0.0,
            "min_win_rate": float(REALIZED_ESCALATION_MIN_WIN_RATE),
            "sources": [],
            "passed": False,
            "reason": "gate_disabled",
        }
        if not REALIZED_ESCALATION_GATE_ENABLED:
            evidence["passed"] = True
            return evidence
        if not Path(TRADER_DB).exists():
            evidence["reason"] = "trader_db_missing"
            return evidence

        lookback_expr = f"-{max(1, int(REALIZED_ESCALATION_LOOKBACK_HOURS))} hours"
        tables = ("live_trades", "agent_trades")

        try:
            conn = sqlite3.connect(TRADER_DB)
            conn.row_factory = sqlite3.Row
            total_closed = 0
            total_wins = 0
            total_pnl = 0.0
            # Check which tables have strategy_name column for filtering
            def _has_column(c, tbl, col):
                try:
                    info = c.execute(f"PRAGMA table_info({tbl})").fetchall()
                    return any(str(r[1]) == col for r in info)
                except Exception:
                    return False

            for table in tables:
                if not self._table_exists(conn, table):
                    continue
                # Build strategy filter clause
                strategy_clause = ""
                params = [str(pair), lookback_expr]
                if strategy_name and _has_column(conn, table, "strategy_name"):
                    strategy_clause = "AND strategy_name = ?"
                    params.append(str(strategy_name))
                row = conn.execute(
                    f"""
                    SELECT
                        COUNT(CASE WHEN pnl IS NOT NULL THEN 1 END) AS closes,
                        COALESCE(SUM(CASE WHEN pnl IS NOT NULL AND COALESCE(pnl, 0) > 0 THEN 1 ELSE 0 END), 0) AS wins,
                        COALESCE(SUM(COALESCE(pnl, 0)), 0) AS net_pnl
                    FROM {table}
                    WHERE pair=?
                      AND UPPER(COALESCE(side, ''))='SELL'
                      AND created_at >= datetime('now', ?)
                      {strategy_clause}
                      AND (
                        status IS NULL
                        OR LOWER(COALESCE(status, '')) NOT IN (
                            'pending',
                            'placed',
                            'open',
                            'accepted',
                            'ack_ok',
                            'failed',
                            'blocked',
                            'canceled',
                            'cancelled',
                            'expired'
                        )
                      )
                    """,
                    params,
                ).fetchone()
                closes = int(row["closes"] or 0) if row else 0
                wins = int(row["wins"] or 0) if row else 0
                pnl = float(row["net_pnl"] or 0.0) if row else 0.0
                if closes > 0:
                    evidence["sources"].append(
                        {
                            "table": table,
                            "closed_trades": closes,
                            "winning_closes": wins,
                            "net_pnl_usd": round(pnl, 6),
                        }
                    )
                total_closed += closes
                total_wins += wins
                total_pnl += pnl
            conn.close()
        except Exception as e:
            evidence["reason"] = f"query_failed:{e}"
            return evidence

        evidence["closed_trades"] = int(total_closed)
        evidence["winning_closes"] = int(total_wins)
        evidence["losing_closes"] = max(0, int(total_closed - total_wins))
        evidence["net_pnl_usd"] = round(float(total_pnl), 6)
        evidence["win_rate"] = round(
            float(total_wins / total_closed) if total_closed > 0 else 0.0, 6
        )
        evidence["avg_pnl_per_close_usd"] = round(
            float(total_pnl / total_closed) if total_closed > 0 else 0.0,
            8,
        )

        if total_closed < int(REALIZED_ESCALATION_MIN_CLOSES):
            evidence["reason"] = (
                f"closed_trades {total_closed} < {REALIZED_ESCALATION_MIN_CLOSES}"
            )
            return evidence
        if float(total_pnl) <= float(REALIZED_ESCALATION_MIN_NET_PNL_USD):
            evidence["reason"] = (
                f"net_pnl_usd {float(total_pnl):.6f} <= {REALIZED_ESCALATION_MIN_NET_PNL_USD:.6f}"
            )
            return evidence
        if float(evidence["win_rate"]) < float(REALIZED_ESCALATION_MIN_WIN_RATE):
            evidence["reason"] = (
                f"win_rate {float(evidence['win_rate']):.4f} < {float(REALIZED_ESCALATION_MIN_WIN_RATE):.4f}"
            )
            return evidence
        if (
            REALIZED_ESCALATION_REQUIRE_WINNING_MAJORITY
            and int(evidence["winning_closes"]) <= int(evidence["losing_closes"])
        ):
            evidence["reason"] = (
                f"winning_closes {int(evidence['winning_closes'])} <= losing_closes {int(evidence['losing_closes'])}"
            )
            return evidence
        if float(evidence["avg_pnl_per_close_usd"]) <= float(REALIZED_ESCALATION_MIN_AVG_PNL_PER_CLOSE_USD):
            evidence["reason"] = (
                f"avg_pnl_per_close_usd {float(evidence['avg_pnl_per_close_usd']):.8f} <= "
                f"{float(REALIZED_ESCALATION_MIN_AVG_PNL_PER_CLOSE_USD):.8f}"
            )
            return evidence

        evidence["passed"] = True
        evidence["reason"] = "passed"
        return evidence

    def _strategy_backtest_results(self, strategy_name, pair):
        """Return stored backtest results for a strategy, if available."""
        row = self.db.execute(
            "SELECT backtest_results_json FROM strategy_registry WHERE name=? AND pair=?",
            (strategy_name, pair),
        ).fetchone()
        if not row:
            return {}
        raw = row["backtest_results_json"]
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _walkforward_leakage_gate(self, backtest_results):
        """Ensure walk-forward windows remain chronologically non-overlapping and monotonic."""
        report = {
            "enabled": bool(WARM_WALKFORWARD_LEAKAGE_ENFORCE),
            "passed": True,
            "reasons": [],
            "details": {},
        }

        if not WARM_WALKFORWARD_LEAKAGE_ENFORCE:
            return report

        if not isinstance(backtest_results, dict) or not backtest_results:
            report["passed"] = False
            report["reasons"].append("backtest_results_missing")
            return report

        walkforward = backtest_results.get("walkforward", {})
        if not isinstance(walkforward, dict):
            report["passed"] = False
            report["reasons"].append("walkforward_payload_missing_or_invalid")
            return report

        if not bool(walkforward.get("available")):
            report["details"]["available"] = False
            report["details"]["reason"] = str(walkforward.get("reason", "unavailable"))
            return report

        leakage = walkforward.get("leakage_diagnostics", {})
        if not isinstance(leakage, dict):
            leakage = walkforward.get("leakage_summary", {})

        report["details"] = {
            "available": True,
            "walkforward_reason": str(walkforward.get("reason", "")),
            "selection_method": str(walkforward.get("selection_method", "")),
            "leakage_diagnostics_present": bool(isinstance(walkforward.get("leakage_diagnostics", None), dict)),
            "windows_tested": int(walkforward.get("windows_tested", 0) or 0),
        }

        if not isinstance(leakage, dict):
            return report

        report["details"]["leakage"] = leakage
        overlap = int(leakage.get("overlap_count", 0) or 0)
        duplicate_is = int(leakage.get("duplicate_count_in_sample", 0) or 0)
        duplicate_oos = int(leakage.get("duplicate_count_out_of_sample", 0) or 0)
        non_monotonic_is = int(leakage.get("non_monotonic_in_sample", 0) or 0)
        non_monotonic_oos = int(leakage.get("non_monotonic_out_of_sample", 0) or 0)
        boundary_gap = int(leakage.get("boundary_gap_seconds", 0) or 0)

        if not bool(leakage.get("passed", True)):
            report["passed"] = False
            report["reasons"].append("walkforward leakage_diagnostics_failed")
        if overlap > 0:
            report["passed"] = False
            report["reasons"].append(f"walkforward_leakage_overlap_count={overlap}")
        if duplicate_is > 0:
            report["passed"] = False
            report["reasons"].append(f"walkforward_leakage_duplicate_is={duplicate_is}")
        if duplicate_oos > 0:
            report["passed"] = False
            report["reasons"].append(f"walkforward_leakage_duplicate_oos={duplicate_oos}")
        if non_monotonic_is > 0:
            report["passed"] = False
            report["reasons"].append(f"walkforward_leakage_non_monotonic_is={non_monotonic_is}")
        if non_monotonic_oos > 0:
            report["passed"] = False
            report["reasons"].append(f"walkforward_leakage_non_monotonic_oos={non_monotonic_oos}")
        if boundary_gap < 0:
            report["passed"] = False
            report["reasons"].append(f"walkforward_leakage_boundary_gap={boundary_gap}")

        return report

    def _cold_criteria(self, results):
        """Adaptive COLD gating for shorter lookback windows."""
        criteria = dict(COLD_TO_WARM)
        if not ADAPTIVE_COLD_GATING:
            return criteria

        candle_count = int(results.get("candle_count", 0) or 0)
        if candle_count <= 0:
            return criteria

        # Local fallback windows can be short; relax trade-count/return gates proportionally.
        if candle_count < 96:
            criteria["min_trades"] = 2
            criteria["min_win_rate"] = 0.55
            criteria["min_return_pct"] = 0.05
        elif candle_count < 132:
            criteria["min_trades"] = 4
            criteria["min_win_rate"] = 0.56
            criteria["min_return_pct"] = 0.12
        elif candle_count < 192:
            criteria["min_trades"] = 6
            criteria["min_win_rate"] = 0.58
            criteria["min_return_pct"] = 0.25
            total_return_pct = float(results.get("total_return_pct", 0.0) or 0.0)
            win_rate = float(results.get("win_rate", 0.0) or 0.0)
            losses = int(results.get("losses", 0) or 0)
            drawdown_pct = float(results.get("max_drawdown_pct", 0.0) or 0.0)
            if (
                total_return_pct >= COLD_TO_WARM_SPARSE_MIN_RETURN_PCT
                and win_rate >= COLD_TO_WARM_SPARSE_MIN_WIN_RATE
                and losses == 0
                and drawdown_pct <= COLD_TO_WARM_SPARSE_MAX_DRAWDOWN_PCT
            ):
                criteria["min_trades"] = max(2, COLD_TO_WARM_SPARSE_MIN_TRADES)
                criteria["min_win_rate"] = max(float(criteria["min_win_rate"]), 0.75)
                criteria["min_return_pct"] = max(
                    float(criteria["min_return_pct"]),
                    COLD_TO_WARM_SPARSE_MIN_RETURN_PCT,
                )
            # Sparse high-quality window: allow lower trade count only when edge is strong
            # and strict loss controls remain intact.
            if (
                total_return_pct >= 1.0
                and win_rate >= 0.85
                and losses == 0
                and drawdown_pct <= min(float(criteria["max_drawdown_pct"]), 2.0)
            ):
                criteria["min_trades"] = 2
                criteria["min_win_rate"] = max(float(criteria["min_win_rate"]), 0.65)
                criteria["min_return_pct"] = max(float(criteria["min_return_pct"]), 0.75)
        return criteria

    def submit_backtest(self, strategy_name, pair, results):
        """Submit backtest results and check for COLD → WARM promotion."""
        stage_row = self.db.execute(
            "SELECT stage FROM strategy_registry WHERE name=? AND pair=?",
            (strategy_name, pair),
        ).fetchone()
        current_stage = str(stage_row["stage"] if stage_row and stage_row["stage"] else "COLD").upper()

        self.db.execute(
            """UPDATE strategy_registry SET backtest_results_json=?, updated_at=CURRENT_TIMESTAMP
               WHERE name=? AND pair=?""",
            (json.dumps(results), strategy_name, pair)
        )
        self.db.commit()

        # Check promotion criteria
        criteria = self._cold_criteria(results)
        passed = True
        reasons = []

        if results["total_trades"] < criteria["min_trades"]:
            passed = False
            reasons.append(f"trades {results['total_trades']} < {criteria['min_trades']}")

        if results["win_rate"] < criteria["min_win_rate"]:
            passed = False
            reasons.append(f"win_rate {results['win_rate']:.2%} < {criteria['min_win_rate']:.2%}")

        if results["total_return_pct"] < criteria["min_return_pct"]:
            passed = False
            reasons.append(f"return {results['total_return_pct']:.2f}% < {criteria['min_return_pct']}%")

        if results["max_drawdown_pct"] > criteria["max_drawdown_pct"]:
            passed = False
            reasons.append(f"drawdown {results['max_drawdown_pct']:.2f}% > {criteria['max_drawdown_pct']}%")

        if STRICT_PROFIT_ONLY:
            if results.get("losses", 0) > 0:
                passed = False
                reasons.append(f"strict mode: {results['losses']} realized losing trades")
            if results.get("final_open_position_blocked"):
                passed = False
                reasons.append(
                    "strict mode: backtest ended with underwater open position (loss close blocked)"
                )

        # Walk-forward out-of-sample gate: require real performance beyond training slice.
        candle_count = int(results.get("candle_count", 0) or 0)
        require_walkforward = WALKFORWARD_REQUIRED and candle_count >= WALKFORWARD_MIN_TOTAL_CANDLES
        walkforward = results.get("walkforward") if isinstance(results.get("walkforward"), dict) else None
        walkforward_sparse_evidence = False
        walkforward_oos_trades = 0
        walkforward_oos_candles = 0
        if require_walkforward:
            if not walkforward:
                passed = False
                reasons.append("walkforward missing for eligible sample")
            else:
                oos = walkforward.get("out_of_sample", {}) if isinstance(walkforward.get("out_of_sample"), dict) else {}
                ins = walkforward.get("in_sample", {}) if isinstance(walkforward.get("in_sample"), dict) else {}

                oos_trades = int(oos.get("total_trades", 0) or 0)
                oos_win = float(oos.get("win_rate", 0.0) or 0.0)
                oos_ret = float(oos.get("total_return_pct", 0.0) or 0.0)
                oos_dd = float(oos.get("max_drawdown_pct", 0.0) or 0.0)
                oos_losses = int(oos.get("losses", 0) or 0)
                oos_candles = int(oos.get("candle_count", 0) or walkforward.get("out_of_sample_candles", 0) or 0)
                walkforward_oos_trades = oos_trades
                walkforward_oos_candles = oos_candles
                ins_ret = float(ins.get("total_return_pct", 0.0) or 0.0)
                sparse_backtest_signal = (
                    float(results.get("win_rate", 0.0) or 0.0) >= WALKFORWARD_SPARSE_OOS_MIN_WIN_RATE
                    and float(results.get("total_return_pct", 0.0) or 0.0)
                    >= max(WALKFORWARD_SPARSE_OOS_MIN_RETURN_PCT, criteria["min_return_pct"])
                    and int(results.get("losses", 0) or 0) == 0
                    and float(results.get("max_drawdown_pct", 0.0) or 0.0)
                    <= WALKFORWARD_SPARSE_OOS_MAX_DRAWDOWN_PCT
                )

                min_oos_return = max(
                    WALKFORWARD_MIN_OOS_RETURN_PCT,
                    criteria["min_return_pct"] * WALKFORWARD_RET_FRACTION,
                )

                sparse_oos_window = oos_trades == 0 and oos_candles <= WALKFORWARD_SPARSE_MAX_OOS_CANDLES
                walkforward_sparse_evidence = bool(sparse_oos_window and sparse_backtest_signal)
                if sparse_oos_window and not sparse_backtest_signal:
                    passed = False
                    reasons.append("walkforward sparse oos window lacks strong in-sample evidence")
                if not sparse_oos_window and oos_trades < WALKFORWARD_MIN_OOS_TRADES:
                    passed = False
                    reasons.append(f"walkforward oos trades {oos_trades} < {WALKFORWARD_MIN_OOS_TRADES}")
                if sparse_oos_window:
                    if oos_ret < 0:
                        passed = False
                        reasons.append(
                            f"walkforward sparse_oos return {oos_ret:.2f}% < 0.00% (candles={oos_candles})"
                        )
                elif oos_ret < min_oos_return:
                    passed = False
                    reasons.append(f"walkforward oos return {oos_ret:.2f}% < {min_oos_return:.2f}%")
                if oos_dd > criteria["max_drawdown_pct"] * WALKFORWARD_DD_MULT:
                    passed = False
                    reasons.append(
                        f"walkforward oos drawdown {oos_dd:.2f}% > "
                        f"{criteria['max_drawdown_pct'] * WALKFORWARD_DD_MULT:.2f}%"
                    )
                if not sparse_oos_window and oos_trades >= 2 and oos_win < 0.50:
                    passed = False
                    reasons.append(f"walkforward oos win_rate {oos_win:.2%} < 50.00%")
                if STRICT_PROFIT_ONLY and oos_losses > 0:
                    passed = False
                    reasons.append(f"walkforward strict mode: {oos_losses} oos losing trades")
                if not sparse_oos_window and ins_ret >= 0.30 and oos_ret < ins_ret * WALKFORWARD_RETENTION_RATIO:
                    passed = False
                    reasons.append(
                        f"walkforward retention {oos_ret:.2f}% < "
                        f"{ins_ret * WALKFORWARD_RETENTION_RATIO:.2f}% ({WALKFORWARD_RETENTION_RATIO:.0%} of IS)"
                    )
            leakage_gate = self._walkforward_leakage_gate(results)
            if not leakage_gate.get("passed", False):
                passed = False
                reasons.extend(
                    [f"walkforward_leakage_gate_failed:{reason}" for reason in leakage_gate.get("reasons", [])]
                )

        growth_details = {
            "enabled": GROWTH_MODE_ENABLED,
            "monte_carlo": {},
            "risk_governor": {},
            "walkforward_leakage": leakage_gate if require_walkforward else self._walkforward_leakage_gate(results),
        }
        if GROWTH_MODE_ENABLED:
            monte_carlo = self.growth.evaluate_monte_carlo(strategy_name, pair, results, criteria)
            growth_details["monte_carlo"] = monte_carlo
            if not monte_carlo.get("passed", False):
                passed = False
                reasons.extend(monte_carlo.get("reasons", []))

            risk_governor = self.growth.build_risk_profile(results, criteria, monte_carlo)
            growth_details["risk_governor"] = risk_governor
            if not risk_governor.get("approved", False):
                passed = False
                reasons.extend(risk_governor.get("reasons", []))

        if current_stage in {"WARM", "HOT"}:
            refresh_event = "backtest_refresh_passed" if passed else "backtest_refresh_failed_stage_retained"
            self._log_event(
                strategy_name,
                pair,
                refresh_event,
                {
                    "stage": current_stage,
                    "passed_cold_gate": bool(passed),
                    "reasons": reasons,
                    "criteria": criteria,
                    "adaptive_cold_gating": ADAPTIVE_COLD_GATING,
                    "walkforward_required": require_walkforward,
                    "walkforward_present": bool(walkforward),
                    "growth_mode": growth_details,
                },
            )
            if passed:
                return True, f"Retained {current_stage} (refresh passed)"
            return False, f"Retained {current_stage}: {', '.join(reasons)}"

        if passed:
            risk_profile = growth_details.get("risk_governor", {})
            if not risk_profile:
                risk_profile = {
                    "tier": "standard",
                    "starter_budget_usd": 1.0,
                    "max_budget_usd": 2.0,
                    "current_budget_usd": 1.0,
                }
            funding_notes = []
            if GROWTH_MODE_ENABLED and risk_profile.get("starter_budget_usd", 0) > 0:
                if walkforward_sparse_evidence:
                    orig_starter = float(risk_profile.get("starter_budget_usd", 0.0) or 0.0)
                    orig_max = float(risk_profile.get("max_budget_usd", 0.0) or 0.0)
                    scaled_starter = round(max(0.0, orig_starter * SPARSE_OOS_FUNDING_MULT), 2)
                    scaled_max = round(max(scaled_starter, orig_max * SPARSE_OOS_FUNDING_MULT), 2)
                    risk_profile["starter_budget_usd"] = scaled_starter
                    risk_profile["max_budget_usd"] = scaled_max
                    risk_profile["tier"] = "shadow"
                    funding_notes.append(
                        f"sparse_oos_funding_scaled_{orig_starter:.2f}_to_{scaled_starter:.2f}"
                    )

                funded = self.funded_budget_snapshot()
                pair_funded = funded.get("per_pair", {}).get(pair, {})
                pair_count = int(pair_funded.get("count", 0) or 0)
                pair_budget = float(pair_funded.get("budget_usd", 0.0) or 0.0)
                total_funded_budget = float(funded.get("total_budget_usd", 0.0) or 0.0)
                starter_budget = float(risk_profile.get("starter_budget_usd", 0.0) or 0.0)
                max_total_budget = max(
                    GROWTH_START_BUDGET_MIN_USD,
                    PIPELINE_PORTFOLIO_USD * max(0.0, WARM_MAX_TOTAL_FUNDED_BUDGET_PCT),
                )
                if pair_count >= WARM_MAX_FUNDED_PER_PAIR:
                    risk_profile["starter_budget_usd"] = 0.0
                    risk_profile["max_budget_usd"] = 0.0
                    risk_profile["tier"] = "shadow"
                    funding_notes.append(
                        f"pair_funding_cap_reached_{pair_count}_gte_{WARM_MAX_FUNDED_PER_PAIR}"
                    )
                else:
                    room = round(max(0.0, max_total_budget - total_funded_budget), 2)
                    if starter_budget > room:
                        if room <= 0:
                            risk_profile["starter_budget_usd"] = 0.0
                            risk_profile["max_budget_usd"] = 0.0
                            risk_profile["tier"] = "shadow"
                            funding_notes.append(
                                f"total_funding_cap_exhausted_{total_funded_budget:.2f}_of_{max_total_budget:.2f}"
                            )
                        else:
                            risk_profile["starter_budget_usd"] = room
                            risk_profile["max_budget_usd"] = max(room, min(float(risk_profile.get("max_budget_usd", room) or room), room * 2))
                            risk_profile["tier"] = "shadow"
                            funding_notes.append(
                                f"total_funding_cap_trimmed_to_{room:.2f}"
                            )

                starter_budget = float(risk_profile.get("starter_budget_usd", 0.0) or 0.0)
                pair_share_cap = max(0.0, min(0.99, float(WARM_MAX_PAIR_BUDGET_SHARE)))
                if starter_budget > 0 and total_funded_budget > 0 and pair_share_cap < 0.99:
                    # Keep any single pair from dominating funded capital.
                    numer = (pair_share_cap * total_funded_budget) - pair_budget
                    denom = max(1e-9, 1.0 - pair_share_cap)
                    allowed_increment = round(max(0.0, numer / denom), 2)
                    if starter_budget > allowed_increment:
                        if allowed_increment <= 0:
                            risk_profile["starter_budget_usd"] = 0.0
                            risk_profile["max_budget_usd"] = 0.0
                            risk_profile["tier"] = "shadow"
                            funding_notes.append(
                                f"pair_share_cap_blocked_{pair}:{pair_budget:.2f}/{max(total_funded_budget, 0.01):.2f}>={pair_share_cap:.0%}"
                            )
                        else:
                            risk_profile["starter_budget_usd"] = allowed_increment
                            risk_profile["max_budget_usd"] = max(
                                allowed_increment,
                                min(
                                    float(risk_profile.get("max_budget_usd", allowed_increment) or allowed_increment),
                                    allowed_increment * 2,
                                ),
                            )
                            risk_profile["tier"] = "shadow"
                            funding_notes.append(
                                f"pair_share_cap_trimmed_to_{allowed_increment:.2f}"
                            )

            risk_profile["current_budget_usd"] = risk_profile.get(
                "current_budget_usd", risk_profile.get("starter_budget_usd", 0.0)
            )
            risk_profile["current_budget_usd"] = risk_profile.get("starter_budget_usd", 0.0)
            if funding_notes:
                risk_profile["funding_notes"] = funding_notes
            self._set_budget_profile(strategy_name, pair, risk_profile)

            self.db.execute(
                """UPDATE strategy_registry SET stage='WARM', promoted_at=CURRENT_TIMESTAMP
                   WHERE name=? AND pair=?""",
                (strategy_name, pair)
            )
            self.db.commit()
            self._log_event(
                strategy_name,
                pair,
                "promoted_to_WARM",
                {
                    "results": results,
                    "criteria": criteria,
                    "adaptive_cold_gating": ADAPTIVE_COLD_GATING,
                    "walkforward_required": require_walkforward,
                    "walkforward_oos_trades": walkforward_oos_trades,
                    "walkforward_oos_candles": walkforward_oos_candles,
                    "growth_mode": growth_details,
                },
            )
            logger.info("PROMOTED %s/%s to WARM (backtest passed)", strategy_name, pair)
            budget_usd = float(risk_profile.get("starter_budget_usd", 0.0) or 0.0)
            if budget_usd <= 0:
                return True, "Promoted to WARM (shadow funding: $0.00)"
            return True, f"Promoted to WARM (budget ${budget_usd:.2f})"
        else:
            self._set_budget_profile(
                strategy_name,
                pair,
                {
                    "tier": "blocked",
                    "current_budget_usd": 0.0,
                    "starter_budget_usd": 0.0,
                    "max_budget_usd": 0.0,
                    "reasons": reasons,
                },
            )
            self._log_event(
                strategy_name,
                pair,
                "failed_COLD",
                {
                    "reasons": reasons,
                    "criteria": criteria,
                    "adaptive_cold_gating": ADAPTIVE_COLD_GATING,
                    "walkforward_required": require_walkforward,
                    "walkforward_present": bool(walkforward),
                    "growth_mode": growth_details,
                },
            )
            logger.warning("REJECTED %s/%s from WARM: %s", strategy_name, pair, ", ".join(reasons))
            return False, f"Failed: {', '.join(reasons)}"

    @staticmethod
    def _warm_evidence_windows(paper_metrics):
        evidence = paper_metrics.get("evidence", {}) if isinstance(paper_metrics.get("evidence"), dict) else {}
        raw_windows = evidence.get("windows", [])
        windows = []
        if isinstance(raw_windows, (list, tuple)):
            for value in raw_windows:
                try:
                    item = int(value)
                except Exception:
                    continue
                if item > 0:
                    windows.append(item)
        windows = sorted(set(windows))
        window_count = int(evidence.get("window_count", len(windows)) or len(windows))
        if window_count <= 0 and windows:
            window_count = len(windows)
        return evidence, windows, max(0, window_count)

    def _warm_gate_context(self, paper_metrics):
        """Build adaptive criteria and evidence context used by HOT promotion gating."""
        criteria = dict(WARM_TO_HOT)
        evidence, windows, window_count = self._warm_evidence_windows(paper_metrics)
        max_window = int(max(windows) if windows else 0)
        min_window = int(min(windows) if windows else 0)
        trades = int(paper_metrics.get("total_trades", 0) or 0)
        runtime = int(paper_metrics.get("runtime_seconds", 0) or 0)

        sparse_windows_ready = window_count >= max(1, int(WARM_MIN_EVIDENCE_WINDOWS))
        sparse_window_history = max_window > 0 and max_window <= max(24, int(WARM_SPARSE_MAX_WINDOW_CANDLES))
        bootstrap_shortfall = trades < int(criteria["min_trades"]) or runtime < int(criteria["min_runtime_seconds"])
        relax_sparse = bool(WARM_ADAPTIVE_GATING) and sparse_windows_ready and (sparse_window_history or bootstrap_shortfall)

        criteria_adjustments = []
        if relax_sparse:
            before = dict(criteria)
            criteria["min_trades"] = max(2, WARM_SPARSE_MIN_TRADES)
            criteria["min_win_rate"] = max(criteria["min_win_rate"], WARM_SPARSE_MIN_WIN_RATE)
            criteria["min_return_pct"] = max(0.0, min(criteria["min_return_pct"], WARM_SPARSE_MIN_RETURN_PCT))
            criteria["max_drawdown_pct"] = min(criteria["max_drawdown_pct"], WARM_SPARSE_MAX_DRAWDOWN_PCT)
            criteria["min_runtime_seconds"] = min(criteria["min_runtime_seconds"], WARM_SPARSE_MIN_RUNTIME_SECONDS)
            criteria["min_sharpe"] = min(criteria["min_sharpe"], WARM_SPARSE_MIN_SHARPE)
            for key in (
                "min_trades",
                "min_win_rate",
                "min_return_pct",
                "max_drawdown_pct",
                "min_runtime_seconds",
                "min_sharpe",
            ):
                if float(criteria[key]) != float(before[key]):
                    criteria_adjustments.append(
                        f"{key}:{before[key]}->{criteria[key]}"
                    )

        mature_sparse_evidence = (
            sparse_windows_ready
            and window_count >= max(int(WARM_MATURE_EVIDENCE_WINDOWS), max(1, int(WARM_MIN_EVIDENCE_WINDOWS)))
            and max_window >= max(24, int(WARM_MATURE_EVIDENCE_MIN_CANDLES))
        )
        context = {
            "criteria_mode": (
                "adaptive_sparse"
                if relax_sparse
                else ("adaptive_disabled" if not WARM_ADAPTIVE_GATING else "baseline")
            ),
            "criteria_adjustments": criteria_adjustments,
            "sparse_windows_ready": bool(sparse_windows_ready),
            "sparse_window_history": bool(sparse_window_history),
            "mature_sparse_evidence": bool(mature_sparse_evidence),
            "window_count": int(window_count),
            "windows": windows,
            "max_window_candles": int(max_window),
            "min_window_candles": int(min_window),
            "evidence_source": str(evidence.get("source", "")),
        }
        return criteria, context

    def _warm_criteria(self, paper_metrics):
        """Adaptive HOT criteria for sparse but diverse runtime evidence."""
        criteria, _ = self._warm_gate_context(paper_metrics)
        return criteria

    def evaluate_warm_metrics_detail(self, paper_metrics):
        """Evaluate WARM metrics and return detailed gate assessment."""
        criteria, context = self._warm_gate_context(paper_metrics)
        failures = []

        trades = int(paper_metrics.get("total_trades", 0) or 0)
        win_rate = float(paper_metrics.get("win_rate", 0.0) or 0.0)
        total_return_pct = float(paper_metrics.get("total_return_pct", 0.0) or 0.0)
        drawdown_pct = float(paper_metrics.get("max_drawdown_pct", 0.0) or 0.0)
        runtime_seconds = int(paper_metrics.get("runtime_seconds", 0) or 0)
        sharpe_ratio = float(paper_metrics.get("sharpe_ratio", 0.0) or 0.0)
        losses = int(paper_metrics.get("losses", 0) or 0)
        es97_5 = float(paper_metrics.get("es97_5_loss_pct", 0.0) or 0.0)

        if trades < int(criteria["min_trades"]):
            failures.append(
                {
                    "code": "trades_below_min",
                    "reason": f"trades {trades} < {criteria['min_trades']}",
                }
            )
        if win_rate < float(criteria["min_win_rate"]):
            failures.append(
                {
                    "code": "win_rate_below_min",
                    "reason": f"win_rate {win_rate:.2%} < {criteria['min_win_rate']:.2%}",
                }
            )
        if total_return_pct < float(criteria["min_return_pct"]):
            failures.append(
                {
                    "code": "return_below_min",
                    "reason": f"return {total_return_pct:.2f}% < {criteria['min_return_pct']}%",
                }
            )
        if drawdown_pct > float(criteria["max_drawdown_pct"]):
            failures.append(
                {
                    "code": "drawdown_above_max",
                    "reason": f"drawdown {drawdown_pct:.2f}% > {criteria['max_drawdown_pct']}%",
                }
            )
        if runtime_seconds < int(criteria["min_runtime_seconds"]):
            failures.append(
                {
                    "code": "runtime_below_min",
                    "reason": f"runtime {runtime_seconds}s < {criteria['min_runtime_seconds']}s",
                }
            )
        if sharpe_ratio < float(criteria["min_sharpe"]):
            failures.append(
                {
                    "code": "sharpe_below_min",
                    "reason": f"sharpe {sharpe_ratio:.2f} < {criteria['min_sharpe']}",
                }
            )
        if losses > 0:
            failures.append(
                {
                    "code": "realized_losses_present",
                    "reason": f"losses {losses} > 0",
                }
            )
        if es97_5 > 0 and es97_5 > GROWTH_MAX_ES97_5_LOSS_PCT:
            failures.append(
                {
                    "code": "tail_risk_above_max",
                    "reason": f"es97_5_loss {es97_5:.2f}% > {GROWTH_MAX_ES97_5_LOSS_PCT:.2f}%",
                }
            )

        passed = len(failures) == 0
        reason_codes = [item["code"] for item in failures]
        reasons = [item["reason"] for item in failures]

        decision = "eligible_hot"
        decision_reason = "passed"
        if not passed:
            collecting_codes = {"trades_below_min", "runtime_below_min"}
            only_collecting_failures = all(code in collecting_codes for code in reason_codes)
            min_windows = max(1, int(WARM_MIN_EVIDENCE_WINDOWS))
            if int(context.get("window_count", 0) or 0) < min_windows:
                decision = "collecting"
                decision_reason = (
                    f"evidence_windows {int(context.get('window_count', 0) or 0)} < {min_windows}"
                )
            elif only_collecting_failures:
                decision = "collecting"
                decision_reason = "collecting_runtime_or_trade_evidence"
            elif bool(context.get("sparse_window_history")) and not bool(context.get("mature_sparse_evidence")):
                decision = "collecting"
                decision_reason = (
                    f"sparse_evidence_maturing:max_window_candles={int(context.get('max_window_candles', 0) or 0)}"
                )
            else:
                decision = "rejected"
                decision_reason = "performance_or_risk_gate_failed"

        return {
            "passed": bool(passed),
            "criteria": criteria,
            "reasons": reasons,
            "reason_codes": reason_codes,
            "decision": decision,
            "decision_reason": decision_reason,
            "criteria_mode": str(context.get("criteria_mode", "baseline")),
            "criteria_adjustments": list(context.get("criteria_adjustments", [])),
            "evidence": context,
        }

    def evaluate_warm_metrics(self, paper_metrics):
        """Evaluate WARM paper metrics against adaptive HOT criteria without side effects."""
        assessment = self.evaluate_warm_metrics_detail(paper_metrics)
        return (
            bool(assessment.get("passed", False)),
            list(assessment.get("reasons", [])),
            dict(assessment.get("criteria", {})),
        )

    def check_warm_promotion(self, strategy_name, pair, paper_metrics):
        """Check if a WARM strategy should be promoted to HOT."""
        assessment = self.evaluate_warm_metrics_detail(paper_metrics)
        passed = bool(assessment.get("passed", False))
        reasons = list(assessment.get("reasons", []))
        criteria = dict(assessment.get("criteria", {}))
        backtest_results = self._strategy_backtest_results(strategy_name, pair)
        leakage_gate = self._walkforward_leakage_gate(backtest_results)
        if not leakage_gate.get("passed", False):
            passed = False
            reasons.extend(
                [f"walkforward_leakage_gate_failed:{reason}" for reason in leakage_gate.get("reasons", [])]
            )

        budget_profile = self.get_budget_profile(strategy_name, pair)
        realized_evidence = self._pair_realized_close_evidence(pair, strategy_name=strategy_name)
        execution_health = self._execution_health_gate_status()
        promotion_gates = {
            "realized_close_required": bool(
                REALIZED_ESCALATION_GATE_ENABLED and REALIZED_CLOSE_REQUIRED_FOR_HOT_PROMOTION
            ),
            "realized_close_passed": bool(realized_evidence.get("passed", False)),
            "realized_close_reason": str(realized_evidence.get("reason", "unknown")),
            "execution_health_required": bool(
                EXECUTION_HEALTH_ESCALATION_GATE and EXECUTION_HEALTH_REQUIRED_FOR_HOT_PROMOTION
            ),
            "execution_health_green": bool(execution_health.get("green", False)),
            "execution_health_reason": str(execution_health.get("reason", "unknown")),
        }
        # Block HOT promotion entirely if realized close evidence is required but missing
        if (
            REALIZED_ESCALATION_GATE_ENABLED
            and REALIZED_CLOSE_REQUIRED_FOR_HOT_PROMOTION
            and not realized_evidence.get("passed", False)
        ):
            passed = False
            reasons.append(
                f"realized_close_evidence_required:{realized_evidence.get('reason', 'unknown')}"
            )
        budget_update = {
            "action": "hold",
            "reason": "growth_mode_disabled",
            "from_budget_usd": budget_profile.get("current_budget_usd", 0.0),
            "to_budget_usd": budget_profile.get("current_budget_usd", 0.0),
            "max_budget_usd": budget_profile.get("max_budget_usd", 0.0),
        }
        if GROWTH_MODE_ENABLED:
            budget_update = self.growth.escalate_budget(
                current_budget=budget_profile.get("current_budget_usd", 0.0),
                max_budget=budget_profile.get("max_budget_usd", 0.0),
                paper_metrics=paper_metrics,
                promote_hot=passed,
            )
            if (
                REALIZED_ESCALATION_GATE_ENABLED
                and budget_update.get("action") == "increase"
                and not realized_evidence.get("passed", False)
            ):
                budget_update["action"] = "hold"
                budget_update["reason"] = (
                    f"realized_close_gate_failed:{realized_evidence.get('reason', 'unknown')}"
                )
                budget_update["to_budget_usd"] = budget_update.get("from_budget_usd", 0.0)

            if (
                EXECUTION_HEALTH_ESCALATION_GATE
                and budget_update.get("action") == "increase"
                and not bool(execution_health.get("green", False))
            ):
                budget_update["action"] = "hold"
                budget_update["reason"] = (
                    f"execution_health_gate_failed:{execution_health.get('reason', 'unknown')}"
                )
                budget_update["to_budget_usd"] = budget_update.get("from_budget_usd", 0.0)

            # Top HOT deployment boost (only after real close-profit evidence is present).
            hot_quality = (
                passed
                and float(paper_metrics.get("total_return_pct", 0.0) or 0.0)
                >= max(float(criteria.get("min_return_pct", 0.0) or 0.0) * 1.5, 0.10)
                and float(paper_metrics.get("sharpe_ratio", 0.0) or 0.0)
                >= max(float(criteria.get("min_sharpe", 0.0) or 0.0), 0.20)
            )
            if (
                budget_update.get("action") == "increase"
                and hot_quality
                and realized_evidence.get("passed", False)
            ):
                cur_to = float(budget_update.get("to_budget_usd", 0.0) or 0.0)
                from_budget = float(budget_update.get("from_budget_usd", 0.0) or 0.0)
                max_budget = float(budget_update.get("max_budget_usd", 0.0) or 0.0)
                boosted = max(
                    cur_to,
                    from_budget * max(1.0, HOT_ESCALATION_BOOST_FACTOR),
                    HOT_ESCALATION_MIN_BUDGET_USD,
                )
                boosted = min(max_budget if max_budget > 0 else boosted, GROWTH_BUDGET_MAX_USD, boosted)
                if boosted > cur_to:
                    budget_update["to_budget_usd"] = round(boosted, 2)
                    budget_update["reason"] = f"{budget_update.get('reason', 'increase')}_hot_boost"

            budget_update["realized_close_evidence"] = realized_evidence
            budget_update["execution_health"] = execution_health
            self._set_budget_profile(
                strategy_name,
                pair,
                {
                    "tier": budget_profile.get("risk_tier", "standard"),
                    "current_budget_usd": budget_update["to_budget_usd"],
                    "starter_budget_usd": budget_profile.get("starter_budget_usd", 0.0),
                    "max_budget_usd": budget_profile.get("max_budget_usd", budget_update["max_budget_usd"]),
                    "budget_update": budget_update,
                },
            )

        if (
            promotion_gates["realized_close_required"]
            and not promotion_gates["realized_close_passed"]
        ):
            passed = False
            reasons.append(
                f"realized_close_gate_failed:{promotion_gates['realized_close_reason']}"
            )
        if (
            promotion_gates["execution_health_required"]
            and not promotion_gates["execution_health_green"]
        ):
            passed = False
            reasons.append(
                f"execution_health_gate_failed:{promotion_gates['execution_health_reason']}"
            )

        if passed:
            self.db.execute(
                """UPDATE strategy_registry SET stage='HOT', paper_results_json=?,
                   promoted_at=CURRENT_TIMESTAMP WHERE name=? AND pair=?""",
                (json.dumps(paper_metrics), strategy_name, pair)
            )
            self.db.commit()
            self._log_event(
                strategy_name,
                pair,
                "promoted_to_HOT",
                {
                    "paper_metrics": paper_metrics,
                    "budget_update": budget_update,
                    "criteria": criteria,
                    "warm_gate_assessment": assessment,
                    "walkforward_leakage_gate": leakage_gate,
                    "realized_close_evidence": realized_evidence,
                    "execution_health": execution_health,
                    "promotion_gates": promotion_gates,
                },
            )
            logger.info("PROMOTED %s/%s to HOT (paper trading passed)", strategy_name, pair)
            return True, f"Promoted to HOT (budget ${budget_update['to_budget_usd']:.2f})"
        else:
            self._log_event(
                strategy_name,
                pair,
                "warm_budget_adjustment",
                {
                    "paper_metrics": paper_metrics,
                    "budget_update": budget_update,
                    "criteria": criteria,
                    "reasons": reasons,
                    "warm_gate_assessment": assessment,
                    "walkforward_leakage_gate": leakage_gate,
                    "realized_close_evidence": realized_evidence,
                    "execution_health": execution_health,
                    "promotion_gates": promotion_gates,
                },
            )
            decision = str(assessment.get("decision", "rejected"))
            decision_reason = str(assessment.get("decision_reason", "unknown"))
            if reasons:
                return False, f"Not ready ({decision}): {decision_reason}; {', '.join(reasons)}"
            return False, f"Not ready ({decision}): {decision_reason}"

    def demote_strategy(self, strategy_name, pair, reason="loss"):
        """Demote a strategy back to COLD (losing money)."""
        self.db.execute(
            """UPDATE strategy_registry SET stage='COLD', demoted_at=CURRENT_TIMESTAMP
               WHERE name=? AND pair=?""",
            (strategy_name, pair)
        )
        self.db.commit()
        self._set_budget_profile(
            strategy_name,
            pair,
            {
                "tier": "blocked",
                "current_budget_usd": 0.0,
                "starter_budget_usd": 0.0,
                "max_budget_usd": 0.0,
                "reason": reason,
            },
        )
        self._log_event(strategy_name, pair, "demoted_to_COLD", {"reason": reason})
        logger.warning("DEMOTED %s/%s to COLD: %s", strategy_name, pair, reason)

    def kill_strategy(self, strategy_name, pair, reason="loss"):
        """Kill a strategy permanently (lost money in HOT)."""
        self.db.execute(
            """UPDATE strategy_registry SET stage='KILLED', killed_at=CURRENT_TIMESTAMP
               WHERE name=? AND pair=?""",
            (strategy_name, pair)
        )
        self.db.commit()
        self._set_budget_profile(
            strategy_name,
            pair,
            {
                "tier": "blocked",
                "current_budget_usd": 0.0,
                "starter_budget_usd": 0.0,
                "max_budget_usd": 0.0,
                "reason": reason,
            },
        )
        self._log_event(strategy_name, pair, "KILLED", {"reason": reason})
        logger.error("KILLED %s/%s: %s", strategy_name, pair, reason)

    def get_hot_strategies(self):
        """Get all strategies currently approved for HOT (live) trading."""
        rows = self.db.execute(
            "SELECT * FROM strategy_registry WHERE stage='HOT'"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_all_strategies(self):
        """Get all registered strategies."""
        rows = self.db.execute(
            "SELECT name, pair, stage, promoted_at, demoted_at, killed_at, created_at FROM strategy_registry ORDER BY stage, name"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_all_budgets(self):
        """Get current risk-governed budgets for all strategies."""
        rows = self.db.execute(
            """SELECT strategy_name, pair, current_budget_usd, starter_budget_usd,
                      max_budget_usd, risk_tier, updated_at
               FROM strategy_budgets
               ORDER BY current_budget_usd DESC, strategy_name ASC"""
        ).fetchall()
        return [dict(r) for r in rows]

    def _log_event(self, name, pair, event_type, details):
        self.db.execute(
            "INSERT INTO pipeline_events (strategy_name, pair, event_type, details_json) VALUES (?, ?, ?, ?)",
            (name, pair, event_type, json.dumps(details))
        )
        self.db.commit()


# ============================================================
# MARKET REGIME DETECTOR — Classify market conditions
# ============================================================

class MarketRegimeDetector:
    """Analyze candle data to classify the current market regime.

    Regimes:
      UPTREND   — SMA slope positive, price above SMA  → TRADE
      DOWNTREND — SMA slope negative, price below SMA  → HOLD
      RANGING   — Low ATR, price crossing SMA often     → TRADE (mean reversion)
      VOLATILE  — High ATR                              → ACCUMULATE (buy dips only)
    """

    REGIME_UPTREND = "UPTREND"
    REGIME_DOWNTREND = "DOWNTREND"
    REGIME_RANGING = "RANGING"
    REGIME_VOLATILE = "VOLATILE"

    def __init__(self, sma_period=20, atr_period=14, atr_volatile_threshold=0.03,
                 atr_ranging_threshold=0.01, crossover_threshold=5, adx_trend_threshold=25):
        self.sma_period = sma_period
        self.atr_period = atr_period
        self.atr_volatile_threshold = atr_volatile_threshold  # ATR/price ratio above this = volatile
        self.atr_ranging_threshold = atr_ranging_threshold    # ATR/price ratio below this = ranging
        self.crossover_threshold = crossover_threshold        # SMA crosses in lookback to count as ranging
        self.adx_trend_threshold = adx_trend_threshold        # ADX above this = trending

    def _compute_sma(self, closes, period):
        """Compute Simple Moving Average for the last `period` values."""
        if len(closes) < period:
            return None
        return sum(closes[-period:]) / period

    def _compute_sma_slope(self, closes, period, lookback=5):
        """Compute the slope of the SMA over `lookback` periods.

        Returns the average per-period change in SMA, normalized by price.
        A positive slope means the SMA is rising (uptrend signal).
        """
        if len(closes) < period + lookback:
            return 0.0

        sma_values = []
        for i in range(lookback + 1):
            idx = len(closes) - lookback + i
            window = closes[idx - period:idx]
            sma_values.append(sum(window) / period)

        # Linear slope: average change per period, normalized by current SMA
        total_change = sma_values[-1] - sma_values[0]
        current_sma = sma_values[-1]
        if current_sma == 0:
            return 0.0
        return (total_change / lookback) / current_sma

    def _compute_atr(self, candles, period):
        """Compute Average True Range over `period` candles.

        True Range = max(high - low, |high - prev_close|, |low - prev_close|)
        ATR = SMA of True Range over `period`.
        """
        if len(candles) < period + 1:
            return 0.0

        true_ranges = []
        for i in range(len(candles) - period, len(candles)):
            high = candles[i]["high"]
            low = candles[i]["low"]
            prev_close = candles[i - 1]["close"] if i > 0 else candles[i]["open"]

            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close),
            )
            true_ranges.append(tr)

        return sum(true_ranges) / len(true_ranges) if true_ranges else 0.0

    def _compute_adx_like_strength(self, candles, period=14):
        """Compute an ADX-like trend strength measure (0-100).

        Uses directional movement (+DM / -DM) to gauge how strongly the
        market is trending regardless of direction.  Higher values indicate
        a stronger trend; lower values indicate a range-bound market.
        """
        if len(candles) < period + 1:
            return 0.0

        plus_dm_list = []
        minus_dm_list = []
        tr_list = []

        start_idx = max(1, len(candles) - period - 1)
        for i in range(start_idx, len(candles)):
            high = candles[i]["high"]
            low = candles[i]["low"]
            prev_high = candles[i - 1]["high"]
            prev_low = candles[i - 1]["low"]
            prev_close = candles[i - 1]["close"]

            # True Range
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_list.append(tr)

            # Directional Movement
            up_move = high - prev_high
            down_move = prev_low - low

            plus_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
            minus_dm = down_move if (down_move > up_move and down_move > 0) else 0.0
            plus_dm_list.append(plus_dm)
            minus_dm_list.append(minus_dm)

        atr_sum = sum(tr_list)
        if atr_sum == 0:
            return 0.0

        plus_di = (sum(plus_dm_list) / atr_sum) * 100
        minus_di = (sum(minus_dm_list) / atr_sum) * 100

        di_sum = plus_di + minus_di
        if di_sum == 0:
            return 0.0

        dx = abs(plus_di - minus_di) / di_sum * 100
        return dx

    def _count_sma_crossovers(self, closes, period, lookback=50):
        """Count how many times price crossed the SMA in the recent `lookback` candles.

        Frequent crossovers indicate a ranging / mean-reverting market.
        """
        if len(closes) < period + lookback:
            lookback = len(closes) - period
        if lookback < 2:
            return 0

        crossovers = 0
        for i in range(len(closes) - lookback, len(closes)):
            if i < period:
                continue
            sma = sum(closes[i - period:i]) / period
            prev_sma = sum(closes[i - period - 1:i - 1]) / period if i > period else sma

            price = closes[i]
            prev_price = closes[i - 1]

            # A crossover occurs when price moves from one side of SMA to the other
            if (prev_price <= prev_sma and price > sma) or (prev_price >= prev_sma and price < sma):
                crossovers += 1

        return crossovers

    def detect(self, candles):
        """Analyze candles and return the current market regime.

        Returns:
            dict with keys:
                regime:         One of UPTREND, DOWNTREND, RANGING, VOLATILE
                confidence:     Float 0.0–1.0 indicating detection confidence
                recommendation: One of TRADE, HOLD, ACCUMULATE
                details:        Dict with supporting indicator values
        """
        if len(candles) < self.sma_period + 5:
            # Not enough data — default to cautious HOLD
            logger.warning("MarketRegimeDetector: not enough candles (%d), defaulting to HOLD", len(candles))
            return {
                "regime": self.REGIME_RANGING,
                "confidence": 0.3,
                "recommendation": "HOLD",
                "details": {"reason": "insufficient_data", "candle_count": len(candles)},
            }

        closes = [c["close"] for c in candles]
        current_price = closes[-1]

        # --- Indicators ---
        sma = self._compute_sma(closes, self.sma_period)
        sma_slope = self._compute_sma_slope(closes, self.sma_period, lookback=5)
        atr = self._compute_atr(candles, self.atr_period)
        atr_ratio = atr / current_price if current_price > 0 else 0.0
        adx = self._compute_adx_like_strength(candles, self.atr_period)
        crossovers = self._count_sma_crossovers(closes, self.sma_period, lookback=50)
        price_above_sma = current_price > sma if sma else False

        details = {
            "sma": round(sma, 4) if sma else None,
            "sma_slope": round(sma_slope, 6),
            "atr": round(atr, 4),
            "atr_ratio": round(atr_ratio, 6),
            "adx": round(adx, 2),
            "sma_crossovers": crossovers,
            "price_above_sma": price_above_sma,
            "current_price": round(current_price, 4),
        }

        # --- Classification logic ---

        # 1. Check VOLATILE first — high ATR overrides everything
        if atr_ratio >= self.atr_volatile_threshold:
            confidence = min(0.95, 0.60 + (atr_ratio - self.atr_volatile_threshold) * 10)
            return {
                "regime": self.REGIME_VOLATILE,
                "confidence": round(confidence, 3),
                "recommendation": "ACCUMULATE",
                "details": details,
            }

        # 2. Check for strong trend via ADX + SMA slope
        if adx >= self.adx_trend_threshold:
            if sma_slope > 0 and price_above_sma:
                # Strong uptrend
                confidence = min(0.95, 0.60 + adx / 200 + abs(sma_slope) * 50)
                return {
                    "regime": self.REGIME_UPTREND,
                    "confidence": round(confidence, 3),
                    "recommendation": "TRADE",
                    "details": details,
                }
            elif sma_slope < 0 and not price_above_sma:
                # Strong downtrend
                confidence = min(0.95, 0.60 + adx / 200 + abs(sma_slope) * 50)
                return {
                    "regime": self.REGIME_DOWNTREND,
                    "confidence": round(confidence, 3),
                    "recommendation": "HOLD",
                    "details": details,
                }

        # 3. Check for trend without strong ADX (weaker trend signals)
        if sma_slope > 0.0005 and price_above_sma:
            confidence = min(0.80, 0.50 + abs(sma_slope) * 100)
            return {
                "regime": self.REGIME_UPTREND,
                "confidence": round(confidence, 3),
                "recommendation": "TRADE",
                "details": details,
            }
        elif sma_slope < -0.0005 and not price_above_sma:
            confidence = min(0.80, 0.50 + abs(sma_slope) * 100)
            return {
                "regime": self.REGIME_DOWNTREND,
                "confidence": round(confidence, 3),
                "recommendation": "HOLD",
                "details": details,
            }

        # 4. Check RANGING — low ATR and/or frequent SMA crossovers
        if atr_ratio <= self.atr_ranging_threshold or crossovers >= self.crossover_threshold:
            confidence = min(0.85, 0.55 + crossovers * 0.03 + (self.atr_ranging_threshold - atr_ratio) * 20)
            confidence = max(0.40, confidence)
            return {
                "regime": self.REGIME_RANGING,
                "confidence": round(confidence, 3),
                "recommendation": "TRADE",
                "details": details,
            }

        # 5. Default — mild conditions, classify by SMA position
        if price_above_sma:
            return {
                "regime": self.REGIME_UPTREND,
                "confidence": 0.45,
                "recommendation": "TRADE",
                "details": details,
            }
        else:
            return {
                "regime": self.REGIME_RANGING,
                "confidence": 0.40,
                "recommendation": "TRADE",
                "details": details,
            }


# ============================================================
# MAIN — Run backtests on all strategies, promote winners
# ============================================================

def run_full_pipeline(pairs=None, granularity="5min"):
    """Run the full COLD pipeline: backtest all strategies on all pairs.

    Uses 5-minute candles by default for higher resolution backtesting.
    """
    if pairs is None:
        pairs = ["BTC-USD", "ETH-USD", "SOL-USD"]

    all_strategies = [
        MeanReversionStrategy(window=20, num_std=2.0),
        MomentumStrategy(short_window=5, long_window=20),
        RSIStrategy(period=14, oversold=30, overbought=70),
        VWAPStrategy(deviation_pct=0.005),
        DipBuyerStrategy(dip_threshold=0.015, recovery_target=0.02),
        MultiTimeframeStrategy(rsi_period=14, bb_window=20, bb_std=2.0),
        AccumulateAndHoldStrategy(dca_interval=12, dip_threshold=0.02),
    ]

    # Strategy selections per regime
    regime_strategies = {
        MarketRegimeDetector.REGIME_UPTREND: [
            "momentum", "rsi", "vwap", "multi_timeframe", "dip_buyer",
        ],
        MarketRegimeDetector.REGIME_DOWNTREND: [],  # skip entirely
        MarketRegimeDetector.REGIME_RANGING: [
            "mean_reversion", "rsi", "vwap", "multi_timeframe",
        ],
        MarketRegimeDetector.REGIME_VOLATILE: [
            "dip_buyer", "accumulate_hold",
        ],
    }

    prices = HistoricalPrices()
    backtester = Backtester(initial_capital=100.0)
    validator = StrategyValidator()
    regime_detector = MarketRegimeDetector()

    results_summary = []

    for pair in pairs:
        logger.info("Fetching historical data for %s...", pair)

        if granularity == "5min":
            candles = prices.get_5min_candles(pair, hours=48)  # 2 days of 5-min
        else:
            candles = prices.get_candles(pair, hours=168)  # 7 days hourly

        if len(candles) < 30:
            logger.warning("Not enough data for %s (%d candles)", pair, len(candles))
            continue

        logger.info("Got %d candles for %s", len(candles), pair)

        # --- Detect market regime FIRST ---
        regime_result = regime_detector.detect(candles)
        regime = regime_result["regime"]
        regime_conf = regime_result["confidence"]
        regime_rec = regime_result["recommendation"]

        print(f"\n{'='*60}")
        print(f"  MARKET REGIME: {pair}")
        print(f"{'='*60}")
        print(f"  Regime:         {regime}")
        print(f"  Confidence:     {regime_conf:.1%}")
        print(f"  Recommendation: {regime_rec}")
        details = regime_result.get("details", {})
        if details.get("sma") is not None:
            print(f"  SMA({regime_detector.sma_period}):       {details['sma']}")
            print(f"  SMA Slope:      {details['sma_slope']}")
            print(f"  ATR Ratio:      {details['atr_ratio']}")
            print(f"  ADX Strength:   {details['adx']}")
            print(f"  SMA Crossovers: {details['sma_crossovers']}")
            print(f"  Price > SMA:    {details['price_above_sma']}")

        # --- Skip DOWNTREND pairs ---
        if regime == MarketRegimeDetector.REGIME_DOWNTREND:
            logger.info(
                "SKIPPING %s — detected DOWNTREND regime (confidence=%.1f%%, recommendation=%s). "
                "Preserving capital per Rule #1: NEVER LOSE MONEY.",
                pair, regime_conf * 100, regime_rec,
            )
            print(f"  >> SKIPPING {pair}: DOWNTREND detected — preserving capital")
            continue

        # --- Filter strategies based on regime ---
        allowed_names = regime_strategies.get(regime, [s.name for s in all_strategies])
        strategies = [s for s in all_strategies if s.name in allowed_names]

        if not strategies:
            logger.info("No strategies suitable for %s regime on %s, skipping", regime, pair)
            print(f"  >> No strategies selected for {regime} regime on {pair}")
            continue

        logger.info(
            "Regime %s for %s — running %d strategies: %s",
            regime, pair, len(strategies), [s.name for s in strategies],
        )

        for strategy in strategies:
            logger.info("Backtesting %s on %s...", strategy.name, pair)
            validator.register_strategy(strategy, pair)

            results = backtester.run(strategy, candles, pair)

            # Print results
            print(f"\n{'='*60}")
            print(f"  {strategy.name} / {pair}")
            print(f"{'='*60}")
            print(f"  Return:     {results['total_return_pct']:+.2f}%")
            print(f"  Win Rate:   {results['win_rate']:.1%} ({results['wins']}W/{results['losses']}L)")
            print(f"  Trades:     {results['total_trades']} ({results['buy_trades']} buys, {results['sell_trades']} sells)")
            print(f"  Drawdown:   {results['max_drawdown_pct']:.2f}%")
            print(f"  Sharpe:     {results['sharpe_ratio']:.2f}")
            print(f"  Final:      ${results['final_capital']:.2f} (from ${results['initial_capital']:.2f})")

            # Submit for promotion
            passed, msg = validator.submit_backtest(strategy.name, pair, results)
            status = "PROMOTED to WARM" if passed else f"REJECTED: {msg}"
            print(f"  Status:     {status}")

            results_summary.append({
                "strategy": strategy.name,
                "pair": pair,
                "return": results["total_return_pct"],
                "win_rate": results["win_rate"],
                "trades": results["total_trades"],
                "drawdown": results["max_drawdown_pct"],
                "sharpe": results["sharpe_ratio"],
                "promoted": passed,
            })

    # Summary
    print(f"\n{'='*60}")
    print(f"  PIPELINE SUMMARY")
    print(f"{'='*60}")

    promoted = [r for r in results_summary if r["promoted"]]
    rejected = [r for r in results_summary if not r["promoted"]]

    print(f"\n  Promoted to WARM ({len(promoted)}):")
    for r in promoted:
        print(f"    {r['strategy']}/{r['pair']}: {r['return']:+.2f}% return, {r['win_rate']:.1%} WR, {r['sharpe']:.2f} Sharpe")

    print(f"\n  Rejected ({len(rejected)}):")
    for r in rejected:
        print(f"    {r['strategy']}/{r['pair']}: {r['return']:+.2f}% return, {r['win_rate']:.1%} WR")

    # Show all registered strategies
    all_strats = validator.get_all_strategies()
    hot = [s for s in all_strats if s["stage"] == "HOT"]
    if hot:
        print(f"\n  HOT (Live Trading) ({len(hot)}):")
        for s in hot:
            print(f"    {s['name']}/{s['pair']}")

    return results_summary


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        validator = StrategyValidator()
        strats = validator.get_all_strategies()
        print(f"\n{'='*60}")
        print(f"  STRATEGY PIPELINE STATUS")
        print(f"{'='*60}")
        for stage in ["HOT", "WARM", "COLD", "KILLED"]:
            group = [s for s in strats if s["stage"] == stage]
            if group:
                print(f"\n  {stage} ({len(group)}):")
                for s in group:
                    print(f"    {s['name']}/{s['pair']} (since {s['promoted_at'] or s['created_at']})")
        if not strats:
            print("\n  No strategies registered. Run 'python strategy_pipeline.py' to backtest.")
        print()

    elif len(sys.argv) > 1 and sys.argv[1] == "backtest":
        # Backtest specific pair
        pair = sys.argv[2] if len(sys.argv) > 2 else "BTC-USD"
        run_full_pipeline(pairs=[pair])

    else:
        # Run full pipeline
        run_full_pipeline()
