#!/usr/bin/env python3
"""Claude staging pipeline.

Keeps strategy context, framework snapshots, and message digests staged in a
single bundle that Claude-oriented agents can ingest each cycle.
"""

import json
import hashlib
import os
import sqlite3
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).parent
STAGING_DIR = BASE_DIR / "claude_staging"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

STRATEGY_STAGE_FILE = STAGING_DIR / "strategy_stage.json"
FRAMEWORK_STAGE_FILE = STAGING_DIR / "framework_stage.json"
MESSAGE_STAGE_FILE = STAGING_DIR / "message_stage.json"
BUNDLE_FILE = STAGING_DIR / "claude_ingest_bundle.json"
BUNDLE_SEQ_FILE = STAGING_DIR / ".bundle_sequence"
OPERATOR_MESSAGES_FILE = STAGING_DIR / "operator_messages.jsonl"
PRIORITY_CONFIG_FILE = STAGING_DIR / "priority_config.json"
MCP_CURRICULUM_FILE = STAGING_DIR / "mcp_curriculum.json"

PIPELINE_DB = BASE_DIR / "pipeline.db"
COMPUTE_POOL_DB = BASE_DIR / "compute_pool.db"
ADVANCED_BUS_FILE = BASE_DIR / "advanced_team" / "message_bus.jsonl"
QUANT100_RESULTS_FILE = BASE_DIR / "quant_100_results.json"
DASHBOARD_INSIGHTS_FILE = BASE_DIR / "advanced_team" / "dashboard_insights.json"


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _write_json(path: Path, payload):
    path.write_text(json.dumps(payload, indent=2))


def _next_bundle_sequence():
    seq = 0
    try:
        if BUNDLE_SEQ_FILE.exists():
            seq = int((BUNDLE_SEQ_FILE.read_text() or "0").strip() or "0")
    except Exception:
        seq = 0
    seq += 1
    BUNDLE_SEQ_FILE.write_text(str(seq))
    return seq


def get_priority_config():
    default = {
        "updated_at": _utc_now(),
        "priority_pairs": ["BTC-USD", "ETH-USD", "SOL-USD"],
        "framework_preferences": ["mlx", "pytorch", "mps", "numpy"],
        "hard_directives": [
            "Keep staging strategy, frameworks, and messages for Claude ingestion.",
            "Run strict profit-only pipeline gates before any promotion.",
        ],
    }
    cfg = _read_json(PRIORITY_CONFIG_FILE, default)
    if not isinstance(cfg, dict):
        cfg = default
    cfg.setdefault("priority_pairs", default["priority_pairs"])
    cfg.setdefault("framework_preferences", default["framework_preferences"])
    cfg.setdefault("hard_directives", default["hard_directives"])
    return cfg


def get_mcp_curriculum():
    default = {
        "version": 1,
        "title": "MCP Quick Curriculum",
        "goals": ["Understand MCP roles and safe tool usage"],
        "protocol_flow": [
            "Discover capabilities",
            "Execute minimal tools",
            "Validate outputs",
            "Publish duplex response",
        ],
    }
    cur = _read_json(MCP_CURRICULUM_FILE, default)
    if not isinstance(cur, dict):
        cur = default
    return cur


def stage_operator_message(text, category="operator", priority="normal", sender="user"):
    OPERATOR_MESSAGES_FILE.parent.mkdir(parents=True, exist_ok=True)
    item = {
        "timestamp": _utc_now(),
        "category": category,
        "priority": priority,
        "sender": sender,
        "message": str(text).strip(),
    }
    with open(OPERATOR_MESSAGES_FILE, "a") as f:
        f.write(json.dumps(item, separators=(",", ":")) + "\n")
    return item


def _tail_jsonl(path: Path, max_lines=200):
    if not path.exists():
        return []
    q = deque(maxlen=max_lines)
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    q.append(line)
    except Exception:
        return []

    out = []
    for line in q:
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def _snapshot_strategy_stage(priority_cfg=None):
    priority_cfg = priority_cfg or get_priority_config()
    warm = []
    hot = []
    stages = {"COLD": 0, "WARM": 0, "HOT": 0, "KILLED": 0}

    if PIPELINE_DB.exists():
        try:
            db = sqlite3.connect(str(PIPELINE_DB))
            db.row_factory = sqlite3.Row
            rows = db.execute(
                "SELECT name, pair, stage, updated_at, promoted_at FROM strategy_registry ORDER BY updated_at DESC LIMIT 400"
            ).fetchall()
            db.close()
            for r in rows:
                stage = str(r["stage"] or "")
                if stage in stages:
                    stages[stage] += 1
                item = {
                    "name": r["name"],
                    "pair": r["pair"],
                    "stage": stage,
                    "updated_at": r["updated_at"],
                    "promoted_at": r["promoted_at"],
                }
                if stage == "WARM" and len(warm) < 40:
                    warm.append(item)
                elif stage == "HOT" and len(hot) < 40:
                    hot.append(item)
        except Exception:
            pass

    quant100 = _read_json(QUANT100_RESULTS_FILE, {})
    q100_summary = quant100.get("summary", {})
    top_candidates = q100_summary.get("top_candidates", []) or []
    top_pairs = []
    for c in top_candidates[:30]:
        pair = c.get("pair")
        if pair and pair not in top_pairs:
            top_pairs.append(pair)

    dash = _read_json(DASHBOARD_INSIGHTS_FILE, {})
    hard_pairs = [str(p).upper() for p in (priority_cfg.get("priority_pairs") or []) if p]
    focus_pairs = list(dict.fromkeys(hard_pairs + (dash.get("focus_pairs") or []) + top_pairs))[:24]

    stage = {
        "updated_at": _utc_now(),
        "stage_counts": stages,
        "warm_candidates": warm,
        "hot_strategies": hot,
        "hard_priority_pairs": hard_pairs,
        "quant100_summary": {
            "total": q100_summary.get("total", 0),
            "promoted_warm": q100_summary.get("promoted_warm", 0),
            "rejected_cold": q100_summary.get("rejected_cold", 0),
            "no_data": q100_summary.get("no_data", 0),
        },
        "focus_pairs": focus_pairs,
    }
    _write_json(STRATEGY_STAGE_FILE, stage)
    return stage


def _snapshot_framework_stage():
    nodes = []
    framework_totals = {}
    online_nodes = 0
    if COMPUTE_POOL_DB.exists():
        try:
            db = sqlite3.connect(str(COMPUTE_POOL_DB))
            db.row_factory = sqlite3.Row
            rows = db.execute(
                "SELECT name, node_id, gpu_type, chip, memory_gb, frameworks, status, current_load, last_heartbeat "
                "FROM nodes ORDER BY status DESC, current_load ASC"
            ).fetchall()
            db.close()
            for r in rows:
                frameworks = []
                try:
                    frameworks = json.loads(r["frameworks"] or "[]")
                except Exception:
                    frameworks = []
                if r["status"] == "online":
                    online_nodes += 1
                for fw in frameworks:
                    framework_totals[fw] = framework_totals.get(fw, 0) + 1
                nodes.append({
                    "name": r["name"],
                    "node_id": r["node_id"],
                    "status": r["status"],
                    "gpu_type": r["gpu_type"],
                    "chip": r["chip"],
                    "memory_gb": r["memory_gb"],
                    "frameworks": frameworks,
                    "load": r["current_load"],
                    "last_heartbeat": r["last_heartbeat"],
                })
        except Exception:
            pass

    if not nodes:
        local_frameworks = ["numpy"]
        try:
            from compute_pool import detect_frameworks
            local_frameworks = detect_frameworks()
        except Exception:
            pass
        for fw in local_frameworks:
            framework_totals[fw] = framework_totals.get(fw, 0) + 1
        nodes = [{
            "name": "local-fallback",
            "node_id": "local-fallback",
            "status": "online",
            "gpu_type": "unknown",
            "chip": os.environ.get("HOSTNAME", "local"),
            "memory_gb": 0,
            "frameworks": local_frameworks,
            "load": 0.0,
            "last_heartbeat": _utc_now(),
        }]
        online_nodes = 1

    stage = {
        "updated_at": _utc_now(),
        "online_nodes": online_nodes,
        "total_nodes": len(nodes),
        "framework_totals": framework_totals,
        "nodes": nodes[:50],
    }
    _write_json(FRAMEWORK_STAGE_FILE, stage)
    return stage


def _snapshot_message_stage():
    bus_msgs = _tail_jsonl(ADVANCED_BUS_FILE, max_lines=400)
    operator_msgs = _tail_jsonl(OPERATOR_MESSAGES_FILE, max_lines=200)
    duplex = {
        "to_claude": [],
        "from_claude": [],
        "to_claude_count": 0,
        "from_claude_count": 0,
    }
    try:
        import claude_duplex as _duplex
        d = _duplex.get_duplex_snapshot(max_items=120)
        duplex = {
            "to_claude": d.get("to_claude", []),
            "from_claude": d.get("from_claude", []),
            "to_claude_count": d.get("to_claude_count", 0),
            "from_claude_count": d.get("from_claude_count", 0),
            "to_claude_last_id": d.get("to_claude_last_id", 0),
            "from_claude_last_id": d.get("from_claude_last_id", 0),
        }
    except Exception:
        pass

    wanted_types = {
        "research_memo",
        "strategy_proposal",
        "risk_verdict",
        "execution_result",
        "learning_insights",
        "algorithm_tuning",
        "quant_optimization",
        "dashboard_optimization",
    }
    digested = []
    for m in bus_msgs:
        if m.get("msg_type") not in wanted_types:
            continue
        payload = m.get("payload", {})
        digested.append({
            "id": m.get("id"),
            "timestamp": m.get("timestamp"),
            "sender": m.get("sender"),
            "recipient": m.get("recipient"),
            "type": m.get("msg_type"),
            "pair": payload.get("pair") if isinstance(payload, dict) else None,
            "summary": payload.get("summary") if isinstance(payload, dict) else None,
        })
    digested = digested[-120:]

    stage = {
        "updated_at": _utc_now(),
        "advanced_team_messages": digested,
        "operator_messages": operator_msgs[-80:],
        "duplex": duplex,
        "advanced_team_message_count": len(digested),
        "operator_message_count": len(operator_msgs),
        "duplex_to_count": int(duplex.get("to_claude_count", 0)),
        "duplex_from_count": int(duplex.get("from_claude_count", 0)),
    }
    _write_json(MESSAGE_STAGE_FILE, stage)
    return stage


def build_ingest_bundle(reason="scheduled"):
    priority_cfg = get_priority_config()
    mcp_curriculum = get_mcp_curriculum()
    generated_at = _utc_now()
    bundle_sequence = _next_bundle_sequence()

    strategy_stage = _snapshot_strategy_stage(priority_cfg=priority_cfg)
    framework_stage = _snapshot_framework_stage()
    message_stage = _snapshot_message_stage()

    instructions = [
        "Prioritize hard_priority_pairs and focus_pairs from strategy_stage for experiment allocation.",
        "Prefer frameworks in priority_config.framework_preferences when available.",
        "Treat operator_messages as high-priority constraints and intent.",
        "Use duplex.to_claude as live directives and respond via duplex.from_claude.",
        "Follow mcp_curriculum protocol_flow for MCP-style tool behavior.",
        "Do not promote strategies unless they satisfy strict profit-only gates.",
    ]

    bundle = {
        "updated_at": generated_at,
        "reason": reason,
        "priority_config": priority_cfg,
        "mcp_curriculum": mcp_curriculum,
        "strategy_stage": strategy_stage,
        "framework_stage": framework_stage,
        "message_stage": message_stage,
        "instructions_for_claude": instructions,
        "summary": {
            "focus_pairs": strategy_stage.get("focus_pairs", [])[:10],
            "hard_priority_pairs": strategy_stage.get("hard_priority_pairs", [])[:10],
            "online_nodes": framework_stage.get("online_nodes", 0),
            "frameworks": framework_stage.get("framework_totals", {}),
            "advanced_msgs": message_stage.get("advanced_team_message_count", 0),
            "operator_msgs": message_stage.get("operator_message_count", 0),
            "duplex_to": message_stage.get("duplex_to_count", 0),
            "duplex_from": message_stage.get("duplex_from_count", 0),
        },
    }
    bundle_hash = hashlib.sha256(
        json.dumps(bundle, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    bundle_id = f"bundle-{bundle_sequence}-{bundle_hash[:12]}"
    bundle["bundle_id"] = bundle_id
    bundle["bundle_hash"] = bundle_hash
    bundle["bundle_sequence"] = bundle_sequence
    bundle["schema_version"] = 1
    bundle["metadata"] = {
        "bundle_id": bundle_id,
        "bundle_hash": bundle_hash,
        "bundle_sequence": bundle_sequence,
        "generated_at": generated_at,
        "schema_version": 1,
    }
    _write_json(BUNDLE_FILE, bundle)
    return bundle


def get_latest_bundle():
    bundle = _read_json(BUNDLE_FILE, {})
    if bundle:
        return bundle
    return build_ingest_bundle(reason="bootstrap")
