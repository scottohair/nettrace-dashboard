#!/usr/bin/env python3
"""Evaluate WARM strategies for HOT promotion using deterministic paper metrics."""

import argparse
import json
import os
import sqlite3
import sys
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
OUT = BASE / "warm_promotion_report.json"
WARM_MIN_EVIDENCE_CANDLES = max(
    1, int(os.environ.get("WARM_MIN_EVIDENCE_CANDLES", "24"))
)
WARM_EVIDENCE_DATA_MODE = str(
    os.environ.get("WARM_EVIDENCE_DATA_MODE", "candle")
).lower().strip()
WARM_EVIDENCE_NON_CANDLE_MIN_BARS = max(
    1, int(os.environ.get("WARM_EVIDENCE_NON_CANDLE_MIN_BARS", str(WARM_MIN_EVIDENCE_CANDLES)))
)
WARM_EVIDENCE_NON_CANDLE_STRICT_MODE = os.environ.get(
    "WARM_EVIDENCE_NON_CANDLE_STRICT_MODE", "0"
).lower() not in ("0", "false", "no")
WARM_EVIDENCE_NON_CANDLE_FALLBACK = os.environ.get(
    "WARM_EVIDENCE_NON_CANDLE_FALLBACK", "1"
).lower() not in ("0", "false", "no")


FACTORIES = {
    "mean_reversion": MeanReversionStrategy,
    "momentum": MomentumStrategy,
    "rsi": RSIStrategy,
    "vwap": VWAPStrategy,
    "dip_buyer": DipBuyerStrategy,
    "multi_timeframe": MultiTimeframeStrategy,
    "accumulate_hold": AccumulateAndHoldStrategy,
}


def _normalize_evidence_mode():
    mode = str(WARM_EVIDENCE_DATA_MODE or "candle").lower().strip()
    if mode in {"candle", "non_candle", "hybrid"}:
        return mode
    return "candle"


def _warm_evidence_rows(prices, pair, granularity, hours):
    granularity_seconds = 300 if granularity == "5min" else 3600
    mode = _normalize_evidence_mode()
    data_mode = mode
    if mode == "candle":
        if granularity == "5min":
            candles = prices.get_5min_candles(pair, hours=hours)
            data_meta = prices.get_cache_meta(pair, str(granularity_seconds), hours)
        else:
            candles = prices.get_candles(pair, hours=hours)
            data_meta = prices.get_cache_meta(pair, str(granularity_seconds), hours)
        data_meta["mode"] = data_mode
        return candles, data_meta, None

    if mode == "non_candle":
        rows = prices.get_non_candle_rows(pair, hours=hours, granularity_seconds=granularity_seconds)
        data_meta = {
            "mode": "non_candle_local",
            "source": "non_candle_local",
            "pair": pair,
            "granularity_seconds": int(granularity_seconds),
            "hours": int(hours),
            "candles": int(len(rows)),
            "notes": ["local_non_candle_evidence"],
        }
        if len(rows) < WARM_EVIDENCE_NON_CANDLE_MIN_BARS:
            if WARM_EVIDENCE_NON_CANDLE_STRICT_MODE:
                return rows, data_meta, f"insufficient_non_candle_rows:{len(rows)}<{WARM_EVIDENCE_NON_CANDLE_MIN_BARS}"
            if WARM_EVIDENCE_NON_CANDLE_FALLBACK:
                # Fallback to candles to preserve progress during sparse non-candle periods.
                if granularity == "5min":
                    candles = prices.get_5min_candles(pair, hours=hours)
                    fallback_meta = prices.get_cache_meta(pair, str(granularity_seconds), hours)
                else:
                    candles = prices.get_candles(pair, hours=hours)
                    fallback_meta = prices.get_cache_meta(pair, str(granularity_seconds), hours)
                fallback_meta["mode"] = "non_candle_fallback"
                return candles, fallback_meta, None
        return rows, data_meta, None

    # hybrid
    rows = prices.get_non_candle_rows(pair, hours=hours, granularity_seconds=granularity_seconds)
    meta = {
        "mode": "non_candle_hybrid",
        "source": "non_candle_local",
        "pair": pair,
        "granularity_seconds": int(granularity_seconds),
        "hours": int(hours),
        "candles": int(len(rows)),
        "notes": ["local_non_candle_preference"],
    }
    if len(rows) >= WARM_EVIDENCE_NON_CANDLE_MIN_BARS:
        return rows, meta, None
    if granularity == "5min":
        candles = prices.get_5min_candles(pair, hours=hours)
        fallback_meta = prices.get_cache_meta(pair, str(granularity_seconds), hours)
    else:
        candles = prices.get_candles(pair, hours=hours)
        fallback_meta = prices.get_cache_meta(pair, str(granularity_seconds), hours)
    fallback_meta["mode"] = "hybrid_fallback"
    return candles, fallback_meta, None


def _base_name(name):
    name = str(name)
    if "_q100_" in name:
        return name.split("_q100_", 1)[0]
    for base in FACTORIES:
        if name == base or name.startswith(base + "_"):
            return base
    return None


def _safe_json(text, default):
    try:
        payload = json.loads(text or "{}")
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return default


def _load_runtime_state(conn, strategy_name, pair):
    row = conn.execute(
        """
        SELECT runtime_seconds, unique_sell_trades, wins, losses, cumulative_return_pct,
               worst_drawdown_pct, last_sharpe_ratio, last_metrics_json
        FROM warm_runtime_state
        WHERE strategy_name=? AND pair=?
        """,
        (strategy_name, pair),
    ).fetchone()
    if not row:
        return {}
    return {
        "runtime_seconds": int(row["runtime_seconds"] or 0),
        "total_trades": int(row["unique_sell_trades"] or 0),
        "wins": int(row["wins"] or 0),
        "losses": int(row["losses"] or 0),
        "total_return_pct": float(row["cumulative_return_pct"] or 0.0),
        "max_drawdown_pct": float(row["worst_drawdown_pct"] or 0.0),
        "sharpe_ratio": float(row["last_sharpe_ratio"] or 0.0),
        "last_metrics": _safe_json(row["last_metrics_json"], {}),
    }


def _merge_paper_metrics(base_metrics, runtime_state):
    if not runtime_state:
        return base_metrics
    merged = dict(base_metrics)
    merged["runtime_seconds"] = max(
        int(base_metrics.get("runtime_seconds", 0) or 0),
        int(runtime_state.get("runtime_seconds", 0) or 0),
    )
    merged["total_trades"] = max(
        int(base_metrics.get("total_trades", 0) or 0),
        int(runtime_state.get("total_trades", 0) or 0),
    )
    state_wins = int(runtime_state.get("wins", 0) or 0)
    state_losses = int(runtime_state.get("losses", 0) or 0)
    total_closed = state_wins + state_losses
    if total_closed > 0:
        merged["win_rate"] = state_wins / total_closed
        merged["losses"] = state_losses
    merged["total_return_pct"] = max(
        float(base_metrics.get("total_return_pct", 0.0) or 0.0),
        float(runtime_state.get("total_return_pct", 0.0) or 0.0),
    )
    merged["max_drawdown_pct"] = max(
        float(base_metrics.get("max_drawdown_pct", 0.0) or 0.0),
        float(runtime_state.get("max_drawdown_pct", 0.0) or 0.0),
    )
    merged["sharpe_ratio"] = max(
        float(base_metrics.get("sharpe_ratio", 0.0) or 0.0),
        float(runtime_state.get("sharpe_ratio", 0.0) or 0.0),
    )
    last_metrics = runtime_state.get("last_metrics", {})
    if isinstance(last_metrics, dict):
        merged["evidence"] = last_metrics.get("evidence", {})
        merged["var95_loss_pct"] = float(last_metrics.get("var95_loss_pct", merged.get("var95_loss_pct", 0.0)) or 0.0)
        merged["es97_5_loss_pct"] = float(last_metrics.get("es97_5_loss_pct", merged.get("es97_5_loss_pct", 0.0)) or 0.0)
    return merged


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
    }


def run(hours=168, granularity="5min", promote=False):
    if not DB.exists():
        raise SystemExit(f"Missing DB: {DB}")

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    runtime_state_exists = bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='warm_runtime_state'"
        ).fetchone()
    )
    rows = conn.execute(
        """
        SELECT name, pair, params_json
        FROM strategy_registry
        WHERE stage='WARM'
        ORDER BY pair, name
        """
    ).fetchall()

    prices = HistoricalPrices()
    backtester = Backtester(initial_capital=100.0)
    validator = StrategyValidator()

    report_rows = []
    for row in rows:
        name = str(row["name"])
        pair = str(row["pair"])
        params = _safe_json(row["params_json"], {})
        base = _base_name(name)

        if not base or base not in FACTORIES:
            report_rows.append(
                {
                    "strategy_name": name,
                    "pair": pair,
                    "status": "skipped",
                    "reason": f"unknown_strategy_base:{base}",
                }
            )
            continue

        strategy = FACTORIES[base](**params)
        granularity_seconds = 300 if granularity == "5min" else 3600
        candles, source_meta, forced_skip = _warm_evidence_rows(
            prices, pair, granularity, hours
        )
        if forced_skip:
            report_rows.append(
                {
                    "strategy_name": name,
                    "pair": pair,
                    "status": "skipped",
                    "reason": forced_skip,
                    "data_source": source_meta,
                }
            )
            continue

        if len(candles) < WARM_MIN_EVIDENCE_CANDLES:
            report_rows.append(
                {
                    "strategy_name": name,
                    "pair": pair,
                    "status": "skipped",
                    "reason": f"insufficient_candles:{len(candles)}<{WARM_MIN_EVIDENCE_CANDLES}",
                    "data_source": source_meta,
                }
            )
            continue

        bt = backtester.run(strategy, candles, pair)
        paper_metrics = {
            "strategy": name,
            "pair": pair,
            "runtime_seconds": int(len(candles) * granularity_seconds),
            "total_trades": int(bt.get("total_trades", 0) or 0),
            "win_rate": float(bt.get("win_rate", 0.0) or 0.0),
            "total_return_pct": float(bt.get("total_return_pct", 0.0) or 0.0),
            "max_drawdown_pct": float(bt.get("max_drawdown_pct", 0.0) or 0.0),
            "sharpe_ratio": float(bt.get("sharpe_ratio", 0.0) or 0.0),
            "losses": int(bt.get("losses", 0) or 0),
            "var95_loss_pct": 0.0,
            "es97_5_loss_pct": 0.0,
            "evidence": {},
        }
        runtime_state = _load_runtime_state(conn, name, pair) if runtime_state_exists else {}
        paper_metrics = _merge_paper_metrics(paper_metrics, runtime_state)
        assessment = _evaluate_warm_gate(validator, paper_metrics)
        eligible = bool(assessment.get("passed", False))
        reasons = list(assessment.get("reasons", []))
        criteria = dict(assessment.get("criteria", {}))
        reason_codes = list(assessment.get("reason_codes", []))
        gate_decision = str(assessment.get("decision", "collecting"))
        gate_decision_reason = str(assessment.get("decision_reason", "not_ready"))
        criteria_mode = str(assessment.get("criteria_mode", "baseline"))
        criteria_adjustments = list(assessment.get("criteria_adjustments", []))
        status = "eligible_hot" if eligible else gate_decision
        message = "eligible (dry run)" if eligible else gate_decision_reason

        if promote and eligible:
            promoted, msg = validator.check_warm_promotion(name, pair, paper_metrics)
            status = "promoted_hot" if promoted else "not_ready"
            message = msg
            if not promoted and not reasons:
                reasons = [msg]
        elif status not in {"collecting", "rejected", "eligible_hot"}:
            status = "collecting"

        report_rows.append(
            {
                "strategy_name": name,
                "pair": pair,
                "strategy_base": base,
                "params": params,
                "status": status,
                "message": message,
                "reasons": reasons,
                "reason_codes": reason_codes,
                "gate_decision": gate_decision,
                "gate_decision_reason": gate_decision_reason,
                "criteria": criteria,
                "criteria_mode": criteria_mode,
                "criteria_adjustments": criteria_adjustments,
                "paper_metrics": paper_metrics,
                "data_source": source_meta,
            }
        )

    promoted = [r for r in report_rows if r.get("status") == "promoted_hot"]
    eligible = [r for r in report_rows if r.get("status") == "eligible_hot"]
    collecting = [r for r in report_rows if r.get("status") == "collecting"]
    rejected = [r for r in report_rows if r.get("status") == "rejected"]
    blocked = [r for r in report_rows if r.get("status") in {"not_ready", "collecting", "rejected"}]
    skipped = [r for r in report_rows if r.get("status") == "skipped"]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "hours": int(hours),
            "granularity": granularity,
            "promote": bool(promote),
            "criteria": dict(WARM_TO_HOT),
        },
        "summary": {
            "warm_checked": len(report_rows),
            "promoted_hot": len(promoted),
            "eligible_hot": len(eligible),
            "collecting": len(collecting),
            "rejected": len(rejected),
            "not_ready": len(blocked),
            "skipped": len(skipped),
        },
        "results": report_rows,
    }
    conn.close()
    OUT.write_text(json.dumps(payload, indent=2))
    print(str(OUT))
    print(json.dumps(payload["summary"], indent=2))


def main():
    parser = argparse.ArgumentParser(description="Evaluate WARM strategies for HOT promotion")
    parser.add_argument("--hours", type=int, default=168)
    parser.add_argument("--granularity", choices=["5min", "1h"], default="5min")
    parser.add_argument("--promote", action="store_true", help="Promote eligible WARM strategies to HOT")
    args = parser.parse_args()
    run(hours=args.hours, granularity=args.granularity, promote=args.promote)


if __name__ == "__main__":
    main()
