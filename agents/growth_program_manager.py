#!/usr/bin/env python3
"""Manage staged execution of the strict growth mode 100-improvement program."""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).parent
REGISTRY_PATH = BASE_DIR / "growth_mode_100_improvements.json"
STATUS_PATH = BASE_DIR / "growth_mode_program_status.json"
LOG_PATH = BASE_DIR / "growth_mode_program_log.jsonl"
QUANT_RESULTS_PATH = BASE_DIR / "quant_100_results.json"
BATCH_CONFIG_PATH = BASE_DIR / "growth_batch_config.json"

PRIORITY_ORDER = {"high": 0, "medium": 1, "low": 2}
VALID_STATUSES = {"planned", "running", "validated", "blocked"}

BASE_STRATEGY_COUNTS = {
    "mean_reversion": 15,
    "momentum": 20,
    "rsi": 15,
    "vwap": 10,
    "dip_buyer": 15,
    "multi_timeframe": 15,
    "accumulate_hold": 10,
}

CATEGORY_STRATEGY_BIAS = {
    "data_quality": {"mean_reversion": 1, "momentum": 1, "rsi": 1},
    "signal_engineering": {"momentum": 3, "rsi": 2, "multi_timeframe": 3, "mean_reversion": 1},
    "execution_quality": {"vwap": 3, "momentum": 2, "multi_timeframe": 1},
    "portfolio_risk": {"accumulate_hold": 3, "dip_buyer": 2, "rsi": 1},
    "market_expansion": {"momentum": 2, "mean_reversion": 2, "vwap": 1},
    "infrastructure": {"multi_timeframe": 1, "momentum": 1},
    "ml_research": {"multi_timeframe": 2, "rsi": 2, "momentum": 1},
    "monitoring": {"accumulate_hold": 1},
    "governance": {"accumulate_hold": 1, "dip_buyer": 1},
    "agent_collaboration": {"momentum": 1, "multi_timeframe": 1},
}


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _load_json(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _save_json(path, payload):
    path.write_text(json.dumps(payload, indent=2))


def _append_log(event):
    with LOG_PATH.open("a") as f:
        f.write(json.dumps(event) + "\n")


def _load_registry():
    registry = _load_json(REGISTRY_PATH, {})
    if not isinstance(registry, dict):
        raise RuntimeError("invalid registry payload")
    items = registry.get("items", [])
    if not isinstance(items, list):
        raise RuntimeError("registry items missing")
    return registry


def _counts(items):
    out = {"planned": 0, "running": 0, "validated": 0, "blocked": 0}
    for item in items:
        status = str(item.get("status", "planned")).lower()
        if status not in out:
            continue
        out[status] += 1
    return out


def _active_batches(items):
    batches = {}
    for item in items:
        if str(item.get("status", "")).lower() != "running":
            continue
        batch_id = item.get("batch_id")
        if not batch_id:
            continue
        batches.setdefault(batch_id, []).append(item.get("id"))
    return [{"batch_id": k, "items": v, "size": len(v)} for k, v in sorted(batches.items())]


def _build_status(registry):
    items = registry.get("items", [])
    counts = _counts(items)
    active = _active_batches(items)
    payload = {
        "generated_at": _now_iso(),
        "program": registry.get("program", "strict_growth_mode_100_improvements"),
        "total_items": len(items),
        "counts": counts,
        "active_batches": active,
    }

    quant = _load_json(QUANT_RESULTS_PATH, {})
    summary = quant.get("summary", {}) if isinstance(quant, dict) else {}
    total = int(summary.get("total", 0) or 0)
    promoted = int(summary.get("promoted_warm", 0) or 0)
    payload["quant_snapshot"] = {
        "generated_at": summary.get("generated_at"),
        "total": total,
        "promoted_warm": promoted,
        "promoted_rate_pct": round((promoted / total) * 100, 2) if total > 0 else 0.0,
        "rejected_cold": int(summary.get("rejected_cold", 0) or 0),
        "no_data": int(summary.get("no_data", 0) or 0),
    }
    return payload


def _derive_strategy_count_overrides(running_items):
    counts = dict(BASE_STRATEGY_COUNTS)
    bias = {k: 0 for k in counts}
    active_categories = sorted({str(i.get("category", "")) for i in running_items if i.get("category")})

    for category in active_categories:
        weights = CATEGORY_STRATEGY_BIAS.get(category, {})
        for strategy_base, w in weights.items():
            if strategy_base in bias:
                bias[strategy_base] += int(w)

    if sum(bias.values()) <= 0:
        return counts

    total = sum(counts.values())
    shift_pool = int(total * 0.30)
    for strategy_base, b in sorted(bias.items(), key=lambda kv: kv[1], reverse=True):
        if b <= 0 or shift_pool <= 0:
            continue
        add = min(shift_pool, b * 2)
        counts[strategy_base] += add
        shift_pool -= add

    while sum(counts.values()) > total:
        for strategy_base in sorted(counts, key=lambda k: counts[k], reverse=True):
            if sum(counts.values()) <= total:
                break
            if counts[strategy_base] > 5:
                counts[strategy_base] -= 1

    while sum(counts.values()) < total:
        for strategy_base in sorted(counts, key=lambda k: bias.get(k, 0), reverse=True):
            if sum(counts.values()) >= total:
                break
            counts[strategy_base] += 1

    return counts


def _write_batch_config(registry, status):
    items = registry.get("items", [])
    running = [i for i in items if str(i.get("status", "")).lower() == "running"]
    active_batches = status.get("active_batches", [])
    active_batch = active_batches[0] if active_batches else None
    active_categories = sorted({str(i.get("category", "")) for i in running if i.get("category")})

    payload = {
        "generated_at": _now_iso(),
        "active": bool(running),
        "active_batch_id": active_batch.get("batch_id") if active_batch else None,
        "active_categories": active_categories,
        "running_item_ids": [i.get("id") for i in running],
        "strategy_count_overrides": _derive_strategy_count_overrides(running),
        "priority_pairs": ["SOL-USD", "ETH-USD", "BTC-USD"],
    }
    _save_json(BATCH_CONFIG_PATH, payload)


def cmd_status(args):
    registry = _load_registry()
    status = _build_status(registry)
    _save_json(STATUS_PATH, status)
    _write_batch_config(registry, status)
    print(json.dumps(status, indent=2))


def cmd_start(args):
    registry = _load_registry()
    items = registry.get("items", [])

    planned = [
        item
        for item in items
        if str(item.get("status", "planned")).lower() == "planned"
        and (not args.category or str(item.get("category", "")) == args.category)
    ]

    planned.sort(key=lambda x: (PRIORITY_ORDER.get(str(x.get("priority", "low")), 9), str(x.get("id", ""))))
    selected = planned[: max(1, int(args.size))]
    if not selected:
        raise RuntimeError("no planned items available for this filter")

    batch_id = args.batch_id or f"BATCH-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    for item in selected:
        item["status"] = "running"
        item["owner"] = args.owner
        item["batch_id"] = batch_id
        item["started_at"] = _now_iso()
        item["last_updated"] = _now_iso()

    registry["updated_at"] = _now_iso()
    _save_json(REGISTRY_PATH, registry)

    event = {
        "timestamp": _now_iso(),
        "event": "batch_started",
        "batch_id": batch_id,
        "owner": args.owner,
        "size": len(selected),
        "item_ids": [item.get("id") for item in selected],
        "category_filter": args.category,
    }
    _append_log(event)

    status = _build_status(registry)
    _save_json(STATUS_PATH, status)
    _write_batch_config(registry, status)
    print(json.dumps(event, indent=2))


def cmd_mark(args):
    status_target = str(args.status).lower()
    if status_target not in VALID_STATUSES:
        raise RuntimeError(f"invalid status {status_target}")

    ids = [s.strip() for s in str(args.ids).split(",") if s.strip()]
    if not ids:
        raise RuntimeError("no ids provided")

    registry = _load_registry()
    items = registry.get("items", [])
    idset = set(ids)
    touched = []

    for item in items:
        iid = str(item.get("id", ""))
        if iid not in idset:
            continue
        item["status"] = status_target
        item["last_updated"] = _now_iso()
        if status_target in {"validated", "blocked", "planned"}:
            item["completed_at"] = _now_iso()
        if args.note:
            item["note"] = args.note
        touched.append(iid)

    if not touched:
        raise RuntimeError("none of the provided ids were found")

    registry["updated_at"] = _now_iso()
    _save_json(REGISTRY_PATH, registry)

    event = {
        "timestamp": _now_iso(),
        "event": "items_marked",
        "status": status_target,
        "item_ids": touched,
        "note": args.note,
    }
    _append_log(event)

    status = _build_status(registry)
    _save_json(STATUS_PATH, status)
    _write_batch_config(registry, status)
    print(json.dumps(event, indent=2))


def cmd_close_batch(args):
    status_target = str(args.status).lower()
    if status_target not in {"validated", "blocked"}:
        raise RuntimeError("close-batch status must be validated or blocked")

    registry = _load_registry()
    items = registry.get("items", [])

    target_batch = str(args.batch_id or "").strip()
    running_items = [i for i in items if str(i.get("status", "")).lower() == "running"]
    if target_batch:
        running_items = [i for i in running_items if str(i.get("batch_id", "")) == target_batch]
    if not running_items:
        raise RuntimeError("no running items found for this batch filter")

    touched = []
    for item in running_items:
        item["status"] = status_target
        item["completed_at"] = _now_iso()
        item["last_updated"] = _now_iso()
        if args.note:
            item["note"] = args.note
        touched.append(item.get("id"))

    registry["updated_at"] = _now_iso()
    _save_json(REGISTRY_PATH, registry)

    event = {
        "timestamp": _now_iso(),
        "event": "batch_closed",
        "batch_id": target_batch or "all_running",
        "status": status_target,
        "size": len(touched),
        "item_ids": touched,
        "note": args.note,
    }
    _append_log(event)

    status = _build_status(registry)
    _save_json(STATUS_PATH, status)
    _write_batch_config(registry, status)
    print(json.dumps(event, indent=2))


def _parse_target_pct(target):
    text = str(target or "").strip()
    if not text:
        return None
    if text.startswith(">=") and text.endswith("%"):
        try:
            return float(text[2:-1].strip())
        except Exception:
            return None
    return None


def cmd_autoeval(args):
    """Auto-evaluate running items with currently available quant metrics only."""
    registry = _load_registry()
    items = registry.get("items", [])
    quant = _load_json(QUANT_RESULTS_PATH, {})
    summary = quant.get("summary", {}) if isinstance(quant, dict) else {}
    total = int(summary.get("total", 0) or 0)
    promoted = int(summary.get("promoted_warm", 0) or 0)
    promoted_rate = (promoted / total) * 100 if total > 0 else 0.0

    validated = []
    for item in items:
        if str(item.get("status", "")).lower() != "running":
            continue
        metric = str(item.get("metric", ""))
        if metric != "promoted_warm_rate":
            continue
        target = _parse_target_pct(item.get("target"))
        if target is None:
            continue
        if promoted_rate >= target:
            item["status"] = "validated"
            item["completed_at"] = _now_iso()
            item["last_updated"] = _now_iso()
            item["evidence"] = {
                "source": str(QUANT_RESULTS_PATH.name),
                "promoted_rate_pct": round(promoted_rate, 2),
                "target": item.get("target"),
            }
            validated.append(item.get("id"))

    registry["updated_at"] = _now_iso()
    _save_json(REGISTRY_PATH, registry)

    event = {
        "timestamp": _now_iso(),
        "event": "autoeval",
        "validated_ids": validated,
        "promoted_rate_pct": round(promoted_rate, 2),
    }
    _append_log(event)

    status = _build_status(registry)
    _save_json(STATUS_PATH, status)
    _write_batch_config(registry, status)
    print(json.dumps(event, indent=2))


def build_parser():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_status = sub.add_parser("status", help="write and print current program status")
    p_status.set_defaults(func=cmd_status)

    p_start = sub.add_parser("start", help="start a batch from planned items")
    p_start.add_argument("--size", type=int, default=10)
    p_start.add_argument("--owner", default="codex")
    p_start.add_argument("--category", default="")
    p_start.add_argument("--batch-id", default="")
    p_start.set_defaults(func=cmd_start)

    p_mark = sub.add_parser("mark", help="mark items by ids")
    p_mark.add_argument("--ids", required=True, help="comma-separated IDs")
    p_mark.add_argument("--status", required=True, help="planned|running|validated|blocked")
    p_mark.add_argument("--note", default="")
    p_mark.set_defaults(func=cmd_mark)

    p_close = sub.add_parser("close-batch", help="close running batch items")
    p_close.add_argument("--status", required=True, help="validated|blocked")
    p_close.add_argument("--batch-id", default="", help="optional batch ID filter")
    p_close.add_argument("--note", default="")
    p_close.set_defaults(func=cmd_close_batch)

    p_eval = sub.add_parser("autoeval", help="auto-validate running items from quant summary")
    p_eval.set_defaults(func=cmd_autoeval)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
