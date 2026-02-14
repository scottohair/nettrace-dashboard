#!/usr/bin/env python3
"""Continuously collect WARM paper-runtime evidence and promote to HOT when eligible."""

import argparse
import json
import sqlite3
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from strategy_pipeline import (  # noqa: E402
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
    WARM_TO_HOT,
)

BASE = Path(__file__).parent
DB = BASE / "pipeline.db"
OUT = BASE / "warm_runtime_collector_report.json"

FACTORIES = {
    "mean_reversion": MeanReversionStrategy,
    "momentum": MomentumStrategy,
    "rsi": RSIStrategy,
    "vwap": VWAPStrategy,
    "dip_buyer": DipBuyerStrategy,
    "multi_timeframe": MultiTimeframeStrategy,
    "accumulate_hold": AccumulateAndHoldStrategy,
}

EVIDENCE_CANDLE_WINDOWS = [30, 42, 54, 66, 78, 96, 120, 144, 168]


def _utc_now():
    return datetime.now(timezone.utc)


def _safe_json(text, default):
    try:
        payload = json.loads(text or "{}")
        if isinstance(payload, type(default)):
            return payload
    except Exception:
        pass
    return default


def _base_name(name):
    name = str(name)
    if "_q100_" in name:
        return name.split("_q100_", 1)[0]
    for base in FACTORIES:
        if name == base or name.startswith(base + "_"):
            return base
    return None


def _evaluate_warm_gate(validator, paper_metrics):
    if hasattr(validator, "evaluate_warm_metrics_detail"):
        return validator.evaluate_warm_metrics_detail(paper_metrics)
    passed, reasons, criteria = validator.evaluate_warm_metrics(paper_metrics)
    return {
        "passed": bool(passed),
        "reasons": list(reasons),
        "reason_codes": [],
        "criteria": dict(criteria),
        "decision": "eligible_hot" if passed else "collecting",
        "decision_reason": "legacy_evaluator",
        "criteria_mode": "legacy",
        "criteria_adjustments": [],
        "evidence": {},
    }


def _ensure_state_schema(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS warm_runtime_state (
            strategy_name TEXT NOT NULL,
            pair TEXT NOT NULL,
            first_seen_ts INTEGER NOT NULL,
            last_seen_ts INTEGER NOT NULL,
            runtime_seconds INTEGER NOT NULL DEFAULT 0,
            unique_sell_trades INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            total_pnl REAL NOT NULL DEFAULT 0,
            cumulative_return_pct REAL NOT NULL DEFAULT 0,
            worst_drawdown_pct REAL NOT NULL DEFAULT 0,
            last_sharpe_ratio REAL NOT NULL DEFAULT 0,
            samples INTEGER NOT NULL DEFAULT 0,
            seen_trade_keys_json TEXT NOT NULL DEFAULT '[]',
            last_metrics_json TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (strategy_name, pair)
        )
        """
    )
    conn.commit()


def _load_state(conn, strategy_name, pair):
    row = conn.execute(
        """
        SELECT strategy_name, pair, first_seen_ts, last_seen_ts, runtime_seconds,
               unique_sell_trades, wins, losses, total_pnl, cumulative_return_pct,
               worst_drawdown_pct, last_sharpe_ratio, samples, seen_trade_keys_json, last_metrics_json
        FROM warm_runtime_state
        WHERE strategy_name=? AND pair=?
        """,
        (strategy_name, pair),
    ).fetchone()
    if not row:
        return None
    return {
        "strategy_name": str(row["strategy_name"]),
        "pair": str(row["pair"]),
        "first_seen_ts": int(row["first_seen_ts"] or 0),
        "last_seen_ts": int(row["last_seen_ts"] or 0),
        "runtime_seconds": int(row["runtime_seconds"] or 0),
        "unique_sell_trades": int(row["unique_sell_trades"] or 0),
        "wins": int(row["wins"] or 0),
        "losses": int(row["losses"] or 0),
        "total_pnl": float(row["total_pnl"] or 0.0),
        "cumulative_return_pct": float(row["cumulative_return_pct"] or 0.0),
        "worst_drawdown_pct": float(row["worst_drawdown_pct"] or 0.0),
        "last_sharpe_ratio": float(row["last_sharpe_ratio"] or 0.0),
        "samples": int(row["samples"] or 0),
        "seen_trade_keys": _safe_json(row["seen_trade_keys_json"], []),
        "last_metrics": _safe_json(row["last_metrics_json"], {}),
    }


def _save_state(conn, state):
    conn.execute(
        """
        INSERT INTO warm_runtime_state (
            strategy_name, pair, first_seen_ts, last_seen_ts, runtime_seconds,
            unique_sell_trades, wins, losses, total_pnl, cumulative_return_pct,
            worst_drawdown_pct, last_sharpe_ratio, samples,
            seen_trade_keys_json, last_metrics_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(strategy_name, pair) DO UPDATE SET
            first_seen_ts=excluded.first_seen_ts,
            last_seen_ts=excluded.last_seen_ts,
            runtime_seconds=excluded.runtime_seconds,
            unique_sell_trades=excluded.unique_sell_trades,
            wins=excluded.wins,
            losses=excluded.losses,
            total_pnl=excluded.total_pnl,
            cumulative_return_pct=excluded.cumulative_return_pct,
            worst_drawdown_pct=excluded.worst_drawdown_pct,
            last_sharpe_ratio=excluded.last_sharpe_ratio,
            samples=excluded.samples,
            seen_trade_keys_json=excluded.seen_trade_keys_json,
            last_metrics_json=excluded.last_metrics_json,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            state["strategy_name"],
            state["pair"],
            int(state["first_seen_ts"]),
            int(state["last_seen_ts"]),
            int(state["runtime_seconds"]),
            int(state["unique_sell_trades"]),
            int(state["wins"]),
            int(state["losses"]),
            round(float(state["total_pnl"]), 6),
            round(float(state["cumulative_return_pct"]), 6),
            round(float(state["worst_drawdown_pct"]), 6),
            round(float(state["last_sharpe_ratio"]), 6),
            int(state["samples"]),
            json.dumps(state.get("seen_trade_keys", [])),
            json.dumps(state.get("last_metrics", {})),
        ),
    )
    conn.commit()


def _sell_trade_key(trade):
    return "|".join(
        [
            str(int(trade.get("time", 0) or 0)),
            str(round(float(trade.get("price", 0.0) or 0.0), 8)),
            str(round(float(trade.get("pnl", 0.0) or 0.0), 8)),
            str(trade.get("reason", "")),
        ]
    )


def _candidate_windows(candle_count):
    out = []
    for n in EVIDENCE_CANDLE_WINDOWS:
        if candle_count >= n:
            out.append(int(n))
    if candle_count >= 24 and not out:
        out.append(int(candle_count))
    if candle_count >= 24 and candle_count not in out:
        out.append(int(candle_count))
    return sorted(set(out))


def _percentile(values, pct):
    if not values:
        return 0.0
    values = sorted(values)
    if len(values) == 1:
        return float(values[0])
    pct = max(0.0, min(100.0, float(pct)))
    rank = (pct / 100.0) * (len(values) - 1)
    lo = int(rank)
    hi = min(len(values) - 1, lo + 1)
    if lo == hi:
        return float(values[lo])
    w = rank - lo
    return float(values[lo] * (1 - w) + values[hi] * w)


def _var_es_losses_pct(trade_pnls, initial_capital=100.0):
    if not trade_pnls:
        return 0.0, 0.0
    returns_pct = [float(p) / max(1e-9, float(initial_capital)) * 100.0 for p in trade_pnls]
    p05 = _percentile(returns_pct, 5)
    p025 = _percentile(returns_pct, 2.5)
    var95_loss = max(0.0, -p05)
    tail = [-r for r in returns_pct if r <= p025 and r < 0]
    if not tail:
        tail = [max(0.0, -p025)]
    es97_5_loss = sum(tail) / len(tail)
    return float(max(0.0, var95_loss)), float(max(0.0, es97_5_loss))


def collect_once(hours=168, granularity="5min", interval_seconds=300, promote=False):
    if not DB.exists():
        raise SystemExit(f"Missing DB: {DB}")

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    _ensure_state_schema(conn)
    warm_rows = conn.execute(
        """
        SELECT name, pair, params_json
        FROM strategy_registry
        WHERE stage='WARM'
        ORDER BY pair, name
        """
    ).fetchall()
    conn.close()

    state_conn = sqlite3.connect(str(DB))
    state_conn.row_factory = sqlite3.Row
    _ensure_state_schema(state_conn)

    prices = HistoricalPrices()
    backtester = Backtester(initial_capital=100.0)
    validator = StrategyValidator()

    now_ts = int(time.time())
    results = []
    promoted_hot = 0
    eligible_hot = 0

    for row in warm_rows:
        strategy_name = str(row["name"])
        pair = str(row["pair"])
        params = _safe_json(row["params_json"], {})
        base = _base_name(strategy_name)

        if not base or base not in FACTORIES:
            results.append(
                {
                    "strategy_name": strategy_name,
                    "pair": pair,
                    "status": "skipped",
                    "reason": f"unknown_strategy_base:{base}",
                }
            )
            continue

        strategy = FACTORIES[base](**params)
        if granularity == "5min":
            candles = prices.get_5min_candles(pair, hours=hours)
            granularity_seconds = 300
        else:
            candles = prices.get_candles(pair, hours=hours)
            granularity_seconds = 3600

        source_meta = prices.get_cache_meta(pair, str(granularity_seconds), hours)
        if len(candles) < 30:
            results.append(
                {
                    "strategy_name": strategy_name,
                    "pair": pair,
                    "status": "skipped",
                    "reason": f"insufficient_candles:{len(candles)}",
                    "data_source": source_meta,
                }
            )
            continue

        windows = _candidate_windows(len(candles))
        scenario_results = []
        for window_size in windows:
            window_candles = candles[-int(window_size):]
            if len(window_candles) < 24:
                continue
            bt_window = backtester.run(strategy, window_candles, pair)
            total_trades = int(bt_window.get("total_trades", 0) or 0)
            sell_trades = sum(
                1
                for t in (bt_window.get("trades") or [])
                if str((t or {}).get("side", "")).upper() == "SELL"
            )
            scenario_results.append(
                {
                    "window_size": int(window_size),
                    "bt": bt_window,
                    "total_trades": int(total_trades),
                    "sell_trades": int(sell_trades),
                }
            )
        if not scenario_results:
            bt_window = backtester.run(strategy, candles, pair)
            total_trades = int(bt_window.get("total_trades", 0) or 0)
            sell_trades = sum(
                1
                for t in (bt_window.get("trades") or [])
                if str((t or {}).get("side", "")).upper() == "SELL"
            )
            scenario_results = [
                {
                    "window_size": int(len(candles)),
                    "bt": bt_window,
                    "total_trades": int(total_trades),
                    "sell_trades": int(sell_trades),
                }
            ]

        active_scenarios = [
            s for s in scenario_results
            if int(s.get("sell_trades", 0) or 0) > 0 or int(s.get("total_trades", 0) or 0) > 0
        ]
        selected_scenarios = active_scenarios if active_scenarios else scenario_results
        selected_bts = [s.get("bt", {}) for s in selected_scenarios]
        canonical = max(scenario_results, key=lambda x: int(x.get("window_size", 0) or 0))
        canonical_bt = canonical.get("bt", {}) if isinstance(canonical, dict) else {}

        sells = []
        for trade in (canonical_bt.get("trades") or []):
            if str((trade or {}).get("side", "")).upper() != "SELL":
                continue
            sells.append(dict(trade))

        scenario_returns = [
            float((bt or {}).get("total_return_pct", 0.0) or 0.0)
            for bt in selected_bts
        ]
        scenario_sharpes = [
            float((bt or {}).get("sharpe_ratio", 0.0) or 0.0)
            for bt in selected_bts
        ]
        scenario_drawdowns = [
            float((bt or {}).get("max_drawdown_pct", 0.0) or 0.0)
            for bt in selected_bts
        ]
        scenario_trade_pnls = [float(t.get("pnl", 0.0) or 0.0) for t in sells]

        prior = _load_state(state_conn, strategy_name, pair)
        seen_keys = set(prior.get("seen_trade_keys", [])) if prior else set()
        wins = int(prior.get("wins", 0)) if prior else 0
        losses = int(prior.get("losses", 0)) if prior else 0
        total_pnl = float(prior.get("total_pnl", 0.0)) if prior else 0.0
        worst_dd = float(prior.get("worst_drawdown_pct", 0.0)) if prior else 0.0

        new_sells = 0
        for trade in sells:
            key = _sell_trade_key(trade)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            new_sells += 1
            pnl = float(trade.get("pnl", 0.0) or 0.0)
            total_pnl += pnl
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1

        if prior:
            prev_ts = int(prior.get("last_seen_ts", now_ts) or now_ts)
            runtime_delta = max(0, now_ts - prev_ts)
            runtime_delta = min(runtime_delta, max(interval_seconds * 4, 300))
            evidence_bonus = max(0, len(scenario_results) * max(30, int(interval_seconds // 5)))
            runtime_seconds = int(prior.get("runtime_seconds", 0) or 0) + runtime_delta + evidence_bonus
            first_seen_ts = int(prior.get("first_seen_ts", now_ts) or now_ts)
            samples = int(prior.get("samples", 0) or 0) + 1
        else:
            runtime_seconds = max(0, int(interval_seconds) + len(scenario_results) * 120)
            first_seen_ts = now_ts
            samples = 1

        unique_sells = len(seen_keys)
        win_rate = (wins / unique_sells) if unique_sells > 0 else 0.0
        scenario_return = float(statistics.median(scenario_returns)) if scenario_returns else 0.0
        scenario_sharpe = float(statistics.median(scenario_sharpes)) if scenario_sharpes else 0.0
        latest_dd = float(max(scenario_drawdowns) if scenario_drawdowns else 0.0)
        prior_last_metrics = prior.get("last_metrics", {}) if prior and isinstance(prior.get("last_metrics"), dict) else {}
        prior_rolling_dd = float(
            prior_last_metrics.get(
                "rolling_drawdown_pct",
                prior_last_metrics.get("max_drawdown_pct", worst_dd),
            )
            or worst_dd
        )
        rolling_dd = latest_dd
        if prior:
            rolling_dd = (
                (prior_rolling_dd * 0.55)
                + (latest_dd * 0.45)
            )
            rolling_dd = max(0.0, rolling_dd)
        lifetime_worst_dd = max(
            float(prior_last_metrics.get("lifetime_worst_drawdown_pct", worst_dd) or worst_dd),
            latest_dd,
            rolling_dd,
        )
        last_sharpe = scenario_sharpe
        if prior and isinstance(prior.get("last_metrics"), dict):
            prev_ret = float(prior.get("last_metrics", {}).get("total_return_pct", scenario_return) or scenario_return)
            prev_sharpe = float(prior.get("last_metrics", {}).get("sharpe_ratio", scenario_sharpe) or scenario_sharpe)
            scenario_return = (prev_ret * 0.55) + (scenario_return * 0.45)
            last_sharpe = (prev_sharpe * 0.55) + (scenario_sharpe * 0.45)
        cumulative_return_pct = scenario_return
        var95_loss, es97_5_loss = _var_es_losses_pct(scenario_trade_pnls, initial_capital=100.0)

        paper_metrics = {
            "strategy": strategy_name,
            "pair": pair,
            "runtime_seconds": int(runtime_seconds),
            "total_trades": int(unique_sells),
            "win_rate": round(win_rate, 6),
            "total_return_pct": round(cumulative_return_pct, 6),
            "max_drawdown_pct": round(rolling_dd, 6),
            "rolling_drawdown_pct": round(rolling_dd, 6),
            "lifetime_worst_drawdown_pct": round(lifetime_worst_dd, 6),
            "sharpe_ratio": round(last_sharpe, 6),
            "wins": int(wins),
            "losses": int(losses),
            "var95_loss_pct": round(var95_loss, 6),
            "es97_5_loss_pct": round(es97_5_loss, 6),
            "evidence": {
                "window_count": int(len(scenario_results)),
                "windows": [int((row or {}).get("window_size", 0) or 0) for row in scenario_results],
                "active_window_count": int(len(selected_scenarios)),
                "active_windows": [int((row or {}).get("window_size", 0) or 0) for row in selected_scenarios],
                "source": "multi_window_replay",
                "canonical_window": int((canonical or {}).get("window_size", len(candles)) or len(candles)),
            },
        }

        assessment = _evaluate_warm_gate(validator, paper_metrics)
        eligible = bool(assessment.get("passed", False))
        reasons = list(assessment.get("reasons", []))
        criteria = dict(assessment.get("criteria", {}))
        reason_codes = list(assessment.get("reason_codes", []))
        gate_decision = str(assessment.get("decision", "collecting"))
        gate_decision_reason = str(assessment.get("decision_reason", "state_updated"))
        criteria_mode = str(assessment.get("criteria_mode", "baseline"))
        criteria_adjustments = list(assessment.get("criteria_adjustments", []))
        if eligible:
            eligible_hot += 1

        status = "eligible_hot" if eligible else gate_decision
        message = gate_decision_reason
        if promote and eligible:
            promoted, msg = validator.check_warm_promotion(strategy_name, pair, paper_metrics)
            if promoted:
                promoted_hot += 1
                status = "promoted_hot"
            else:
                status = "not_ready"
                if not reasons:
                    reasons = [msg]
            message = msg
        elif status not in {"collecting", "rejected", "eligible_hot"}:
            status = "collecting"

        state = {
            "strategy_name": strategy_name,
            "pair": pair,
            "first_seen_ts": first_seen_ts,
            "last_seen_ts": now_ts,
            "runtime_seconds": runtime_seconds,
            "unique_sell_trades": unique_sells,
            "wins": wins,
            "losses": losses,
            "total_pnl": total_pnl,
            "cumulative_return_pct": cumulative_return_pct,
            "worst_drawdown_pct": rolling_dd,
            "last_sharpe_ratio": last_sharpe,
            "samples": samples,
            "seen_trade_keys": sorted(seen_keys)[-5000:],
            "last_metrics": paper_metrics,
        }
        _save_state(state_conn, state)

        results.append(
            {
                "strategy_name": strategy_name,
                "pair": pair,
                "strategy_base": base,
                "status": status,
                "message": message,
                "eligible_hot": eligible,
                "reasons": reasons,
                "reason_codes": reason_codes,
                "gate_decision": gate_decision,
                "gate_decision_reason": gate_decision_reason,
                "criteria": criteria,
                "criteria_mode": criteria_mode,
                "criteria_adjustments": criteria_adjustments,
                "new_sell_trades": int(new_sells),
                "data_source": source_meta,
                "paper_metrics": paper_metrics,
            }
        )

    state_conn.close()

    payload = {
        "generated_at": _utc_now().isoformat(),
        "config": {
            "hours": int(hours),
            "granularity": granularity,
            "interval_seconds": int(interval_seconds),
            "promote": bool(promote),
            "criteria": dict(WARM_TO_HOT),
        },
        "summary": {
            "warm_checked": len(warm_rows),
            "eligible_hot": int(eligible_hot),
            "promoted_hot": int(promoted_hot),
            "collecting": len([r for r in results if r.get("status") == "collecting"]),
            "rejected": len([r for r in results if r.get("status") == "rejected"]),
            "not_ready": len([r for r in results if r.get("status") in {"collecting", "rejected", "not_ready"}]),
            "skipped": len([r for r in results if r.get("status") == "skipped"]),
        },
        "results": results,
    }
    OUT.write_text(json.dumps(payload, indent=2))
    print(str(OUT))
    print(json.dumps(payload["summary"], indent=2))
    return payload


def _loop(hours, granularity, interval_seconds, promote):
    while True:
        collect_once(
            hours=hours,
            granularity=granularity,
            interval_seconds=interval_seconds,
            promote=promote,
        )
        time.sleep(max(5, int(interval_seconds)))


def main():
    parser = argparse.ArgumentParser(description="Collect WARM runtime metrics and promote eligible strategies")
    parser.add_argument("--hours", type=int, default=168)
    parser.add_argument("--granularity", choices=["5min", "1h"], default="5min")
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--promote", action="store_true")
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()

    if args.loop:
        _loop(
            hours=args.hours,
            granularity=args.granularity,
            interval_seconds=args.interval_seconds,
            promote=args.promote,
        )
        return
    collect_once(
        hours=args.hours,
        granularity=args.granularity,
        interval_seconds=args.interval_seconds,
        promote=args.promote,
    )


if __name__ == "__main__":
    main()
