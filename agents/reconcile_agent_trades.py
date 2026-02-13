#!/usr/bin/env python3
"""Reconcile agent_trades rows with exchange order fill snapshots."""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).parent
STATUS_FILE = BASE / "reconcile_agent_trades_status.json"
DEFAULT_RECONCILE_STATUSES = ("pending", "placed", "open", "accepted", "ack_ok")
SELL_CLOSE_COMPLETED_STATUSES = {"filled", "closed", "executed", "settled"}

sys.path.insert(0, str(BASE))
from agent_tools import AgentTools  # noqa: E402


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _normalize_statuses(statuses):
    raw = statuses if statuses is not None else DEFAULT_RECONCILE_STATUSES
    return tuple(sorted({str(s or "").strip().lower() for s in raw if str(s or "").strip()}))


def _new_summary():
    return {
        "checked": 0,
        "updated": 0,
        "filled": 0,
        "partial": 0,
        "cancelled": 0,
        "failed": 0,
        "expired": 0,
        "early_exit_reason": "",
        "close_attempts": 0,
        "close_completions": 0,
        "close_failures": 0,
        "close_failure_reasons": {},
        "close_gate_passed": True,
        "close_gate_reason": "no_pending_sell_closes",
    }


def _record_close_failure(summary, reason):
    key = str(reason or "unknown")
    summary["close_failures"] = int(summary.get("close_failures", 0) or 0) + 1
    reasons = summary.get("close_failure_reasons")
    if not isinstance(reasons, dict):
        reasons = {}
        summary["close_failure_reasons"] = reasons
    reasons[key] = int(reasons.get(key, 0) or 0) + 1


def _close_failure_reason(status):
    norm = str(status or "").lower().strip() or "unknown"
    if norm in {"cancelled", "failed", "expired"}:
        return f"terminal_{norm}"
    if norm in {"partial_filled", "partially_filled"}:
        return "partial_fill_only"
    return f"not_completed_{norm}"


def _fetch_candidate_rows(tools, status_norm, lookback_hours, limit, side_filter=None):
    if not status_norm or int(limit) <= 0:
        return []
    marks = ",".join(["?"] * len(status_norm))
    where = [
        "order_id IS NOT NULL",
        f"LOWER(COALESCE(status, '')) IN ({marks})",
        "created_at >= datetime('now', ?)",
    ]
    params = [*status_norm, f"-{max(1, int(lookback_hours))} hours"]

    if side_filter == "sell":
        where.append("UPPER(COALESCE(side, ''))='SELL'")
    elif side_filter == "non_sell":
        where.append("UPPER(COALESCE(side, ''))!='SELL'")

    query = f"""
        SELECT id, agent, pair, side, price, quantity, total_usd, order_id, status, pnl
        FROM agent_trades
        WHERE {' AND '.join(where)}
        ORDER BY id DESC
        LIMIT ?
    """
    params.append(max(1, int(limit)))
    return tools.db.execute(query, params).fetchall()


def _apply_reconcile_row(tools, row, summary):
    circuit_open_until = tools._safe_float(getattr(tools.exchange, "_circuit_open_until", 0.0), 0.0)
    if circuit_open_until > time.time():
        summary["early_exit_reason"] = "exchange_circuit_open"
        return False

    summary["checked"] += 1
    is_sell = str(row["side"] or "").upper() == "SELL"
    if is_sell:
        summary["close_attempts"] += 1

    fill_data = tools._fetch_order_fill_snapshot(row["order_id"])
    if not fill_data:
        if is_sell:
            _record_close_failure(summary, "snapshot_missing")
        return True

    trade = tools._fill_to_trade_values(
        fill_data,
        fallback_price=row["price"],
        fallback_quantity=row["quantity"],
        fallback_total_usd=row["total_usd"],
        default_status=row["status"] or "pending",
    )
    old_status = str(row["status"] or "").lower()
    new_status = str(trade["status"] or "").lower()
    unchanged = (
        new_status == old_status
        and abs(float(trade["quantity"]) - tools._safe_float(row["quantity"], 0.0)) < 1e-12
        and abs(float(trade["total_usd"]) - tools._safe_float(row["total_usd"], 0.0)) < 1e-8
    )

    if not unchanged:
        pnl = tools._estimate_realized_pnl(
            row["agent"],
            row["pair"],
            row["side"],
            trade["price"],
            trade["quantity"],
            trade["total_usd"],
            new_status,
        )
        old_pnl = row["pnl"]
        tools.db.execute(
            """
            UPDATE agent_trades
               SET price=?,
                   quantity=?,
                   total_usd=?,
                   status=?,
                   pnl=?
             WHERE id=?
            """,
            (
                float(trade["price"]),
                float(trade["quantity"]),
                float(trade["total_usd"]),
                new_status,
                pnl,
                int(row["id"]),
            ),
        )
        tools.db.commit()
        summary["updated"] += 1

        if pnl is not None:
            old_val = None if old_pnl is None else float(old_pnl)
            if old_val is None:
                tools.record_pnl(float(pnl))
            else:
                delta = float(pnl) - old_val
                if abs(delta) > 1e-12:
                    tools.record_pnl(delta)

        if new_status == "filled":
            summary["filled"] += 1
        elif new_status in {"partial_filled", "partially_filled"}:
            summary["partial"] += 1
        elif new_status == "cancelled":
            summary["cancelled"] += 1
        elif new_status == "expired":
            summary["expired"] += 1
        elif new_status == "failed":
            summary["failed"] += 1

    if is_sell:
        if new_status in SELL_CLOSE_COMPLETED_STATUSES:
            summary["close_completions"] += 1
        else:
            _record_close_failure(summary, _close_failure_reason(new_status))

    return True


def _reconcile_rows(tools, rows, summary, limit):
    processed = 0
    for row in rows:
        if processed >= int(limit):
            break
        keep_going = _apply_reconcile_row(tools, row, summary)
        if not keep_going:
            break
        processed += 1
    return processed


def _finalize_close_gate(summary):
    attempts = int(summary.get("close_attempts", 0) or 0)
    completions = int(summary.get("close_completions", 0) or 0)
    early_exit_reason = str(summary.get("early_exit_reason", "") or "").strip()
    reasons = summary.get("close_failure_reasons", {})
    if not isinstance(reasons, dict):
        reasons = {}
    if early_exit_reason:
        summary["close_gate_passed"] = False
        summary["close_gate_reason"] = f"early_exit:{early_exit_reason}"
    elif attempts <= 0:
        summary["close_gate_passed"] = True
        summary["close_gate_reason"] = "no_pending_sell_closes"
    elif completions > 0:
        summary["close_gate_passed"] = True
        summary["close_gate_reason"] = "sell_close_completion_observed"
    else:
        summary["close_gate_passed"] = False
        if reasons:
            top_reason = sorted(reasons.items(), key=lambda item: (-int(item[1]), str(item[0])))[0][0]
            summary["close_gate_reason"] = f"sell_close_completion_missing:{top_reason}"
        else:
            summary["close_gate_reason"] = "sell_close_completion_missing"


def reconcile_close_first(
    tools,
    max_orders=120,
    lookback_hours=96,
    reconcile_statuses=None,
):
    statuses = _normalize_statuses(reconcile_statuses)
    summary = _new_summary()
    if not statuses:
        summary["close_gate_reason"] = "no_reconcile_statuses"
        summary["close_gate_passed"] = False
        return summary

    order_limit = max(1, int(max_orders))
    lookback = max(1, int(lookback_hours))
    sell_rows = _fetch_candidate_rows(
        tools,
        status_norm=statuses,
        lookback_hours=lookback,
        limit=order_limit,
        side_filter="sell",
    )
    processed = _reconcile_rows(tools, sell_rows, summary, order_limit)
    remaining = max(0, order_limit - int(processed))
    if not summary.get("early_exit_reason") and remaining > 0:
        other_rows = _fetch_candidate_rows(
            tools,
            status_norm=statuses,
            lookback_hours=lookback,
            limit=remaining,
            side_filter="non_sell",
        )
        _reconcile_rows(tools, other_rows, summary, remaining)

    _finalize_close_gate(summary)
    return summary


def _close_reconciliation_payload(summary):
    attempts = int(summary.get("close_attempts", 0) or 0)
    completions = int(summary.get("close_completions", 0) or 0)
    failures = int(summary.get("close_failures", 0) or 0)
    rate = (float(completions) / float(attempts)) if attempts > 0 else 1.0
    return {
        "attempts": attempts,
        "completions": completions,
        "failures": failures,
        "completion_rate": round(rate, 6),
        "failure_reasons": dict(summary.get("close_failure_reasons", {}) or {}),
        "gate_passed": bool(summary.get("close_gate_passed", False)),
        "gate_reason": str(summary.get("close_gate_reason", "unknown")),
    }


def main():
    parser = argparse.ArgumentParser(description="Reconcile pending/placed agent trades to terminal statuses")
    parser.add_argument("--max-orders", type=int, default=120)
    parser.add_argument("--lookback-hours", type=int, default=96)
    parser.add_argument("--status-file", type=str, default=str(STATUS_FILE))
    parser.add_argument(
        "--reconcile-statuses",
        type=str,
        default=",".join(DEFAULT_RECONCILE_STATUSES),
        help="Comma-separated trade statuses eligible for reconciliation.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [reconcile_agent_trades] %(levelname)s %(message)s")
    tools = AgentTools()
    statuses = [s.strip() for s in str(args.reconcile_statuses or "").split(",") if str(s or "").strip()]

    summary = reconcile_close_first(
        tools,
        max_orders=max(1, int(args.max_orders)),
        lookback_hours=max(1, int(args.lookback_hours)),
        reconcile_statuses=statuses or None,
    )
    close_payload = _close_reconciliation_payload(summary)
    payload = {
        "updated_at": _utc_now(),
        "max_orders": max(1, int(args.max_orders)),
        "lookback_hours": max(1, int(args.lookback_hours)),
        "reconcile_statuses": _normalize_statuses(statuses or None),
        "summary": summary,
        "close_reconciliation": close_payload,
    }
    status_path = Path(str(args.status_file))
    status_path.write_text(json.dumps(payload, indent=2))
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
