#!/usr/bin/env python3
"""Retroactively enforce funded-budget risk caps for WARM/HOT strategies."""

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).parent
DB = BASE / "pipeline.db"

PIPELINE_PORTFOLIO_USD = float(os.environ.get("PIPELINE_PORTFOLIO_USD", "262.55"))
WARM_MAX_FUNDED_PER_PAIR = int(os.environ.get("WARM_MAX_FUNDED_PER_PAIR", "2"))
WARM_MAX_TOTAL_FUNDED_BUDGET_PCT = float(os.environ.get("WARM_MAX_TOTAL_FUNDED_BUDGET_PCT", "0.35"))
WARM_MAX_PAIR_BUDGET_SHARE = float(os.environ.get("WARM_MAX_PAIR_BUDGET_SHARE", "0.70"))
SPARSE_OOS_FUNDING_MULT = float(os.environ.get("SPARSE_OOS_FUNDING_MULT", "0.25"))
OOS_PROVEN_MIN_SEED_USD = float(os.environ.get("OOS_PROVEN_MIN_SEED_USD", "0.10"))


def _now():
    return datetime.now(timezone.utc).isoformat()


def _safe_json(text):
    try:
        return json.loads(text or "{}")
    except Exception:
        return {}


def _walkforward_sparse(backtest):
    wf = backtest.get("walkforward") if isinstance(backtest.get("walkforward"), dict) else {}
    if not wf:
        return False
    oos = wf.get("out_of_sample") if isinstance(wf.get("out_of_sample"), dict) else {}
    oos_trades = int(oos.get("total_trades", 0) or 0)
    oos_candles = int(oos.get("candle_count", 0) or wf.get("out_of_sample_candles", 0) or 0)
    return oos_trades == 0 and oos_candles < 48


def _walkforward_oos_quality(backtest):
    wf = backtest.get("walkforward") if isinstance(backtest.get("walkforward"), dict) else {}
    if not wf:
        return 0
    oos = wf.get("out_of_sample") if isinstance(wf.get("out_of_sample"), dict) else {}
    oos_trades = int(oos.get("total_trades", 0) or 0)
    oos_ret = float(oos.get("total_return_pct", 0.0) or 0.0)
    if oos_trades > 0 and oos_ret > 0:
        return 2
    if oos_trades > 0:
        return 1
    return 0


def main():
    if not DB.exists():
        raise SystemExit("pipeline.db not found")

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT b.strategy_name, b.pair, b.current_budget_usd, b.starter_budget_usd,
                   b.max_budget_usd, b.risk_tier, b.governor_json,
                   r.stage, r.backtest_results_json
            FROM strategy_budgets b
            JOIN strategy_registry r
              ON r.name = b.strategy_name AND r.pair = b.pair
            WHERE r.stage IN ('WARM', 'HOT')
            ORDER BY b.current_budget_usd DESC, b.strategy_name ASC
            """
        ).fetchall()
        promo_seed_rows = conn.execute(
            """
            SELECT strategy_name, pair,
                   COALESCE(MAX(COALESCE(json_extract(details_json, '$.budget_update.to_budget_usd'), 0)), 0) AS promo_budget
            FROM pipeline_events
            WHERE event_type='promoted_to_HOT'
            GROUP BY strategy_name, pair
            """
        ).fetchall()
        promo_seed_map = {
            (str(r["strategy_name"]), str(r["pair"])): float(r["promo_budget"] or 0.0)
            for r in promo_seed_rows
        }
        rows = sorted(
            rows,
            key=lambda row: (
                _walkforward_oos_quality(_safe_json(row["backtest_results_json"])),
                float(row["current_budget_usd"] or 0.0),
            ),
            reverse=True,
        )

        max_total_budget = PIPELINE_PORTFOLIO_USD * max(0.0, WARM_MAX_TOTAL_FUNDED_BUDGET_PCT)
        pair_counts = {}
        total_budget = 0.0
        proposed_rows = []

        for row in rows:
            name = str(row["strategy_name"])
            pair = str(row["pair"])
            stage = str(row["stage"] or "WARM").upper()
            curr = float(row["current_budget_usd"] or 0.0)
            starter = float(row["starter_budget_usd"] or curr)
            maxb = float(row["max_budget_usd"] or max(curr, starter))
            tier = str(row["risk_tier"] or "shadow")
            gov = _safe_json(row["governor_json"])
            backtest = _safe_json(row["backtest_results_json"])

            notes = []
            proposed = curr

            if _walkforward_sparse(backtest) and proposed > 0:
                proposed = round(max(0.0, proposed * SPARSE_OOS_FUNDING_MULT), 2)
                starter = round(max(0.0, starter * SPARSE_OOS_FUNDING_MULT), 2)
                maxb = round(max(starter, maxb * SPARSE_OOS_FUNDING_MULT), 2)
                tier = "shadow"
                notes.append("sparse_oos_scaled")

            pair_n = pair_counts.get(pair, 0)
            if proposed > 0 and pair_n >= WARM_MAX_FUNDED_PER_PAIR:
                proposed = 0.0
                starter = 0.0
                maxb = 0.0
                tier = "shadow"
                notes.append("pair_cap_zeroed")

            if proposed > 0:
                room = round(max(0.0, max_total_budget - total_budget), 2)
                if room <= 0:
                    proposed = 0.0
                    starter = 0.0
                    maxb = 0.0
                    tier = "shadow"
                    notes.append("total_cap_zeroed")
                elif proposed > room:
                    proposed = room
                    starter = min(starter, room)
                    maxb = max(proposed, min(maxb, room * 2))
                    tier = "shadow"
                    notes.append("total_cap_trimmed")

            oos_quality = _walkforward_oos_quality(backtest)
            promo_seed = float(promo_seed_map.get((name, pair), 0.0) or 0.0)
            if (
                proposed <= 0
                and stage == "HOT"
                and promo_seed > 0
                and pair_counts.get(pair, 0) < WARM_MAX_FUNDED_PER_PAIR
            ):
                room = round(max(0.0, max_total_budget - total_budget), 2)
                seed = round(min(max(OOS_PROVEN_MIN_SEED_USD, promo_seed), room), 2)
                if seed > 0:
                    proposed = seed
                    starter = max(starter, seed)
                    maxb = max(maxb, seed)
                    tier = "shadow"
                    notes.append("hot_promotion_seeded")

            if (
                proposed <= 0
                and oos_quality >= 2
                and pair_counts.get(pair, 0) < WARM_MAX_FUNDED_PER_PAIR
            ):
                room = round(max(0.0, max_total_budget - total_budget), 2)
                seed = round(min(max(0.0, OOS_PROVEN_MIN_SEED_USD), room), 2)
                if seed > 0:
                    proposed = seed
                    starter = max(starter, seed)
                    maxb = max(maxb, seed)
                    tier = "shadow"
                    notes.append("oos_positive_seeded")

            if proposed > 0:
                pair_counts[pair] = pair_counts.get(pair, 0) + 1
                total_budget += proposed

            proposed_rows.append(
                {
                    "strategy_name": name,
                    "pair": pair,
                    "orig_current_budget_usd": round(curr, 2),
                    "orig_starter_budget_usd": round(float(row["starter_budget_usd"] or curr), 2),
                    "orig_max_budget_usd": round(float(row["max_budget_usd"] or max(curr, starter)), 2),
                    "orig_risk_tier": str(row["risk_tier"] or "shadow"),
                    "current_budget_usd": round(proposed, 2),
                    "starter_budget_usd": round(starter, 2),
                    "max_budget_usd": round(maxb, 2),
                    "risk_tier": tier,
                    "governor": gov,
                    "notes": notes,
                }
            )

        pair_share_cap = max(0.0, min(0.99, float(WARM_MAX_PAIR_BUDGET_SHARE)))
        if pair_share_cap < 0.99:
            for _ in range(6):
                total_budget = round(
                    sum(r["current_budget_usd"] for r in proposed_rows if r["current_budget_usd"] > 0),
                    2,
                )
                if total_budget <= 0:
                    break

                pair_budgets = {}
                for r in proposed_rows:
                    amt = float(r["current_budget_usd"] or 0.0)
                    if amt <= 0:
                        continue
                    pair = str(r["pair"])
                    pair_budgets[pair] = round(pair_budgets.get(pair, 0.0) + amt, 2)
                if len(pair_budgets) < 2:
                    # Do not zero-out all funding when only one pair is currently fundable.
                    break

                violating = [
                    (pair, budget)
                    for pair, budget in pair_budgets.items()
                    if budget / total_budget > pair_share_cap + 1e-6
                ]
                if not violating:
                    break

                for pair, pair_budget in violating:
                    others = round(max(0.0, total_budget - pair_budget), 2)
                    allowed_pair_budget = 0.0
                    if others > 0:
                        allowed_pair_budget = round(
                            (pair_share_cap * others) / max(1e-9, 1.0 - pair_share_cap),
                            2,
                        )
                    scale = 0.0 if pair_budget <= 0 else max(
                        0.0,
                        min(1.0, allowed_pair_budget / pair_budget),
                    )
                    for r in proposed_rows:
                        if r["pair"] != pair:
                            continue
                        amt = float(r["current_budget_usd"] or 0.0)
                        if amt <= 0:
                            continue
                        new_amt = round(max(0.0, amt * scale), 2)
                        r["current_budget_usd"] = new_amt
                        r["starter_budget_usd"] = round(min(float(r["starter_budget_usd"] or 0.0), new_amt), 2)
                        r["max_budget_usd"] = round(max(new_amt, min(float(r["max_budget_usd"] or 0.0), new_amt * 2)), 2)
                        r["risk_tier"] = "shadow"
                        r["notes"].append("pair_share_scaled")

        pair_counts = {}
        total_budget = 0.0
        updates = []
        for r in proposed_rows:
            amt = float(r["current_budget_usd"] or 0.0)
            if amt > 0:
                pair = str(r["pair"])
                pair_counts[pair] = pair_counts.get(pair, 0) + 1
                total_budget += amt

            changed = (
                abs(float(r["current_budget_usd"]) - float(r["orig_current_budget_usd"])) > 1e-9
                or abs(float(r["starter_budget_usd"]) - float(r["orig_starter_budget_usd"])) > 1e-9
                or abs(float(r["max_budget_usd"]) - float(r["orig_max_budget_usd"])) > 1e-9
                or str(r["risk_tier"]) != str(r["orig_risk_tier"])
            )
            notes = sorted(set(r["notes"]))
            if changed or notes:
                gov = dict(r["governor"])
                gov["rebalance_notes"] = sorted(set((gov.get("rebalance_notes") or []) + notes))
                gov["rebalanced_at"] = _now()
                updates.append(
                    {
                        "strategy_name": r["strategy_name"],
                        "pair": r["pair"],
                        "current_budget_usd": round(float(r["current_budget_usd"] or 0.0), 2),
                        "starter_budget_usd": round(float(r["starter_budget_usd"] or 0.0), 2),
                        "max_budget_usd": round(float(r["max_budget_usd"] or 0.0), 2),
                        "risk_tier": str(r["risk_tier"] or "shadow"),
                        "governor_json": json.dumps(gov),
                    }
                )

        for u in updates:
            conn.execute(
                """
                UPDATE strategy_budgets
                SET current_budget_usd=?, starter_budget_usd=?, max_budget_usd=?,
                    risk_tier=?, governor_json=?, updated_at=CURRENT_TIMESTAMP
                WHERE strategy_name=? AND pair=?
                """,
                (
                    u["current_budget_usd"],
                    u["starter_budget_usd"],
                    u["max_budget_usd"],
                    u["risk_tier"],
                    u["governor_json"],
                    u["strategy_name"],
                    u["pair"],
                ),
            )
            conn.execute(
                "INSERT INTO pipeline_events (strategy_name, pair, event_type, details_json) VALUES (?, ?, ?, ?)",
                (
                    u["strategy_name"],
                    u["pair"],
                    "budget_rebalanced",
                    json.dumps(
                        {
                            "current_budget_usd": u["current_budget_usd"],
                            "starter_budget_usd": u["starter_budget_usd"],
                            "max_budget_usd": u["max_budget_usd"],
                            "risk_tier": u["risk_tier"],
                        }
                    ),
                ),
            )

        conn.commit()

        print(
            json.dumps(
                {
                    "updated": len(updates),
                    "final_total_funded_budget": round(total_budget, 2),
                    "max_total_budget": round(max_total_budget, 2),
                    "pair_counts": pair_counts,
                },
                indent=2,
            )
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
