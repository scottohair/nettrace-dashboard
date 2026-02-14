#!/usr/bin/env python3
"""Autonomous work manager for clawd.bot / Claude duplex collaboration.

Maintains a persistent prioritized work queue, dispatches top items to Claude
duplex as autonomous work packets, and ingests acknowledgements/completions.
"""

import argparse
import json
import logging
import os
import re
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import claude_duplex
except Exception:
    claude_duplex = None

BASE = Path(__file__).parent
QUEUE_FILE = BASE / "autonomous_work_queue.json"
STATUS_FILE = BASE / "autonomous_work_status.json"
ADVANCED_FIXES_FILE = BASE / "advanced_fixes_ranked_100.json"
WIN_TASKS_FILE = BASE / "win_1000_tasks.json"
WIN_STATUS_FILE = BASE / "win_1000_status.json"
HARDWARE_PLAN_FILE = BASE / "hardware_resource_delegation_plan.json"

INTERVAL_SECONDS = int(os.environ.get("AUTONOMOUS_WORK_INTERVAL_SECONDS", "180"))
QUEUE_TARGET_SIZE = int(os.environ.get("AUTONOMOUS_WORK_QUEUE_TARGET_SIZE", "80"))
MAX_OUTSTANDING = int(os.environ.get("AUTONOMOUS_WORK_MAX_OUTSTANDING", "12"))
DISPATCH_PER_CYCLE = int(os.environ.get("AUTONOMOUS_WORK_DISPATCH_PER_CYCLE", "4"))
DISPATCH_MAX_PER_SOURCE = int(os.environ.get("AUTONOMOUS_WORK_DISPATCH_MAX_PER_SOURCE", "2"))
MAX_DISPATCH_ATTEMPTS = int(os.environ.get("AUTONOMOUS_WORK_MAX_DISPATCH_ATTEMPTS", "6"))
STALE_SECONDS = int(os.environ.get("AUTONOMOUS_WORK_STALE_SECONDS", "3600"))
STRICT_COMPLETION_VERIFY = os.environ.get("AUTONOMOUS_WORK_STRICT_COMPLETION_VERIFY", "1").lower() not in {
    "0",
    "false",
    "no",
}
COMPLETED_RETENTION_SECONDS = int(os.environ.get("AUTONOMOUS_WORK_COMPLETED_RETENTION_SECONDS", "900"))
ADVANCED_FIXES_IMPORT_LIMIT = int(os.environ.get("AUTONOMOUS_WORK_ADVANCED_FIXES_IMPORT_LIMIT", "40"))
WIN_TASKS_IMPORT_LIMIT = int(os.environ.get("AUTONOMOUS_WORK_WIN_TASKS_IMPORT_LIMIT", "40"))
HARDWARE_PLAN_IMPORT_LIMIT = int(os.environ.get("AUTONOMOUS_WORK_HARDWARE_IMPORT_LIMIT", "24"))

DONE_HINTS = ("done", "completed", "implemented", "finished", "merged", "shipped")
TASK_ID_RE = re.compile(r"\b(AF-\d{3}|WIN-\d{4}|HWP-\d{3})\b", re.IGNORECASE)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [autonomous_work] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE / "autonomous_work_manager.log")),
    ],
)
logger = logging.getLogger("autonomous_work_manager")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _load_json(path, default):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except Exception:
        return default


def _save_json(path, payload):
    Path(path).write_text(json.dumps(payload, indent=2))


def _priority_from_score(score):
    s = float(score or 0.0)
    if s >= 90:
        return "high"
    if s >= 70:
        return "normal"
    return "low"


def _parse_iso(ts):
    raw = str(ts or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _compact_win_status(payload):
    if not isinstance(payload, dict):
        return {}
    return {
        "updated_at": str(payload.get("updated_at", "")),
        "tasks_total": int(payload.get("tasks_total", 0) or 0),
        "worked_through_count": int(payload.get("worked_through_count", 0) or 0),
        "status_counts": payload.get("status_counts", {}),
        "priority_counts": payload.get("priority_counts", {}),
    }


def _base_queue_state():
    return {
        "updated_at": _now_iso(),
        "last_from_claude_id": 0,
        "tasks": {},
        "archive": [],
    }


def _normalize_state(payload):
    if not isinstance(payload, dict):
        return _base_queue_state()
    out = _base_queue_state()
    out["updated_at"] = str(payload.get("updated_at", out["updated_at"]))
    try:
        out["last_from_claude_id"] = max(0, int(payload.get("last_from_claude_id", 0) or 0))
    except Exception:
        out["last_from_claude_id"] = 0
    raw_tasks = payload.get("tasks", {})
    if isinstance(raw_tasks, dict):
        out["tasks"] = dict(raw_tasks)
    raw_archive = payload.get("archive", [])
    if isinstance(raw_archive, list):
        out["archive"] = list(raw_archive)[-400:]
    return out


def _task_key_rank(task):
    try:
        return int(task.get("rank", 999_999) or 999_999)
    except Exception:
        return 999_999


def _task_key_priority(task):
    score = float(task.get("priority_score", 0.0) or 0.0)
    rank = _task_key_rank(task)
    return (-score, rank, str(task.get("task_id", "")))


def _import_advanced_fix_tasks(limit=ADVANCED_FIXES_IMPORT_LIMIT):
    payload = _load_json(ADVANCED_FIXES_FILE, {})
    items = payload.get("items", []) if isinstance(payload, dict) else []
    out = []
    for row in items:
        if not isinstance(row, dict):
            continue
        if bool(row.get("implemented", False)):
            continue
        rank = int(row.get("rank", 0) or 0)
        if rank <= 0:
            continue
        title = str(row.get("title", "")).strip()
        if not title:
            continue
        score = float(row.get("score", 0.0) or 0.0)
        out.append(
            {
                "task_id": f"AF-{rank:03d}",
                "source": "advanced_fixes_ranked_100",
                "rank": rank,
                "title": title,
                "description": str(row.get("description", "")).strip(),
                "acceptance": "Implement + tests green + rank item marked implemented.",
                "priority_score": score,
                "priority": _priority_from_score(score),
            }
        )
        if len(out) >= max(1, int(limit)):
            break
    return out


def _import_win_tasks(limit=WIN_TASKS_IMPORT_LIMIT):
    tasks = _load_json(WIN_TASKS_FILE, [])
    if not isinstance(tasks, list):
        return []
    out = []
    for row in tasks:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", "pending")).strip().lower()
        if status == "completed":
            continue
        tid = str(row.get("id", "")).strip().upper()
        if not tid.startswith("WIN-"):
            continue
        score = float(row.get("priority_score", 0.0) or 0.0)
        out.append(
            {
                "task_id": tid,
                "source": "win_1000",
                "rank": int(row.get("priority_rank", 0) or 0),
                "title": str(row.get("title", "")).strip(),
                "description": str(row.get("blocked_reason", "")).strip(),
                "acceptance": str(row.get("acceptance", "")).strip() or "Deterministic measurable improvement with rollback criteria.",
                "priority_score": score,
                "priority": _priority_from_score(score),
            }
        )
    out.sort(key=_task_key_priority)
    return out[: max(1, int(limit))]


def _hardware_priority_score(lane_id, cadence_seconds):
    lane = str(lane_id or "").strip().lower()
    cadence = max(1, int(cadence_seconds or 300))
    # Fast cadence lanes are more urgent because they can block runtime gates.
    cadence_boost = max(0.0, min(6.0, (300.0 / float(cadence))))
    base = 82.0
    if lane in {"exec_network_lane", "control_plane_lane"}:
        base = 94.0
    elif lane == "sim_hf_lane":
        base = 90.0
    elif lane == "compute_sql_lane":
        base = 86.0
    elif lane == "io_scan_lane":
        base = 84.0
    return round(min(99.0, base + cadence_boost), 2)


def _import_hardware_lane_tasks(limit=HARDWARE_PLAN_IMPORT_LIMIT):
    payload = _load_json(HARDWARE_PLAN_FILE, {})
    lanes = payload.get("lanes", []) if isinstance(payload, dict) else []
    out = []
    for lane in lanes:
        if not isinstance(lane, dict):
            continue
        lane_id = str(lane.get("lane_id", "")).strip()
        if not lane_id:
            continue
        resource_class = str(lane.get("resource_class", "")).strip()
        cadence_seconds = int(lane.get("cadence_seconds", 0) or 0)
        primary_agents = lane.get("primary_agents", [])
        if not isinstance(primary_agents, list):
            primary_agents = []
        tasks = lane.get("tasks", [])
        if not isinstance(tasks, list):
            tasks = []
        for row in tasks:
            if not isinstance(row, dict):
                continue
            task_id = str(row.get("task_id", "")).strip().upper()
            if not TASK_ID_RE.fullmatch(task_id):
                continue
            if not task_id.startswith("HWP-"):
                continue
            status = str(row.get("status", "")).strip().lower()
            if status in {"done", "completed"}:
                continue
            title = str(row.get("title", "")).strip()
            if not title:
                continue
            owners = row.get("owners", [])
            if not isinstance(owners, list):
                owners = []
            acceptance_list = row.get("acceptance", [])
            if not isinstance(acceptance_list, list):
                acceptance_list = []
            acceptance = " ; ".join(str(x).strip() for x in acceptance_list if str(x).strip())
            description_parts = [
                f"lane={lane_id}",
                f"resource={resource_class or 'unknown'}",
            ]
            if primary_agents:
                description_parts.append("agents=" + ",".join(str(a).strip() for a in primary_agents if str(a).strip()))
            if owners:
                description_parts.append("owners=" + ",".join(str(o).strip() for o in owners if str(o).strip()))
            out.append(
                {
                    "task_id": task_id,
                    "source": "hardware_resource_delegation_plan",
                    "rank": int("".join(ch for ch in task_id if ch.isdigit()) or 0),
                    "title": title,
                    "description": " | ".join(description_parts),
                    "acceptance": acceptance or "Deliver measurable lane improvement and provide deterministic verification output.",
                    "priority_score": _hardware_priority_score(lane_id, cadence_seconds),
                    "priority": "high",
                }
            )
    out.sort(key=_task_key_priority)
    return out[: max(1, int(limit))]


def _extract_task_ids(message, meta):
    found = set()
    text = str(message or "")
    for m in TASK_ID_RE.findall(text):
        found.add(str(m).upper())
    if isinstance(meta, dict):
        for k in ("task_id", "work_item_id"):
            val = str(meta.get(k, "")).strip().upper()
            if TASK_ID_RE.fullmatch(val):
                found.add(val)
        for k in ("task_ids", "work_item_ids"):
            vals = meta.get(k, [])
            if isinstance(vals, list):
                for v in vals:
                    val = str(v).strip().upper()
                    if TASK_ID_RE.fullmatch(val):
                        found.add(val)
    return sorted(found)


def _is_completion(msg_type, message):
    t = str(msg_type or "").strip().lower()
    body = str(message or "").strip().lower()
    if t in {"task_completed", "work_completed", "completion"}:
        return True
    return any(h in body for h in DONE_HINTS)


class AutonomousWorkManager:
    def __init__(self):
        self.running = True
        self.state = _normalize_state(_load_json(QUEUE_FILE, {}))

    def _persist(self):
        self.state["updated_at"] = _now_iso()
        _save_json(QUEUE_FILE, self.state)

    def _upsert_tasks(self, tasks):
        changed = 0
        for row in tasks:
            task_id = str(row.get("task_id", "")).strip().upper()
            if not task_id:
                continue
            cur = self.state["tasks"].get(task_id)
            if not cur:
                self.state["tasks"][task_id] = {
                    "task_id": task_id,
                    "source": str(row.get("source", "unknown")),
                    "rank": int(row.get("rank", 0) or 0),
                    "title": str(row.get("title", "")).strip(),
                    "description": str(row.get("description", "")).strip(),
                    "acceptance": str(row.get("acceptance", "")).strip(),
                    "priority_score": float(row.get("priority_score", 0.0) or 0.0),
                    "priority": str(row.get("priority", "normal")),
                    "status": "queued",
                    "dispatch_count": 0,
                    "trace_id": "",
                    "outbound_msg_id": 0,
                    "last_dispatched_at": "",
                    "last_ack_at": "",
                    "last_ack_msg_id": 0,
                    "last_ack_source": "",
                    "completion_claimed_at": "",
                    "completion_claim_msg_id": 0,
                    "completion_claim_source": "",
                    "last_completed_at": "",
                    "verification": {
                        "strict_mode": bool(STRICT_COMPLETION_VERIFY),
                        "last_checked_at": "",
                        "passed": False,
                        "reason": "not_checked",
                    },
                    "notes": [],
                    "created_at": _now_iso(),
                    "updated_at": _now_iso(),
                }
                changed += 1
                continue

            # keep metadata fresh even if task already exists.
            cur["title"] = str(row.get("title", cur.get("title", ""))).strip()
            cur["description"] = str(row.get("description", cur.get("description", ""))).strip()
            cur["acceptance"] = str(row.get("acceptance", cur.get("acceptance", ""))).strip()
            cur["priority_score"] = float(row.get("priority_score", cur.get("priority_score", 0.0)) or 0.0)
            cur["priority"] = str(row.get("priority", cur.get("priority", "normal")) or "normal")
            cur["rank"] = int(row.get("rank", cur.get("rank", 0)) or 0)
            if not isinstance(cur.get("verification"), dict):
                cur["verification"] = {
                    "strict_mode": bool(STRICT_COMPLETION_VERIFY),
                    "last_checked_at": "",
                    "passed": False,
                    "reason": "not_checked",
                }
            cur["updated_at"] = _now_iso()
        return changed

    def _queue_size(self):
        return len(self.state["tasks"])

    @staticmethod
    def _append_note(task, text, limit=12):
        notes = task.get("notes", [])
        if not isinstance(notes, list):
            notes = []
        msg = str(text or "").strip()
        if msg:
            notes.append({"ts": _now_iso(), "note": msg[:320]})
        task["notes"] = notes[-max(1, int(limit)) :]

    @staticmethod
    def _rank_from_task_id(task_id):
        m = re.fullmatch(r"AF-(\d{3})", str(task_id or "").upper().strip())
        if not m:
            return 0
        try:
            return int(m.group(1))
        except Exception:
            return 0

    def _verify_advanced_fix_completion(self, task):
        task_id = str(task.get("task_id", "")).upper().strip()
        rank = int(task.get("rank", 0) or 0) or self._rank_from_task_id(task_id)
        payload = _load_json(ADVANCED_FIXES_FILE, {})
        items = payload.get("items", []) if isinstance(payload, dict) else []
        for row in items:
            if not isinstance(row, dict):
                continue
            try:
                r = int(row.get("rank", 0) or 0)
            except Exception:
                r = 0
            if r != rank:
                continue
            if bool(row.get("implemented", False)):
                return True, "advanced_fix_marked_implemented"
            return False, "advanced_fix_not_marked_implemented"
        return False, "advanced_fix_rank_not_found"

    def _verify_win_task_completion(self, task):
        task_id = str(task.get("task_id", "")).upper().strip()
        tasks = _load_json(WIN_TASKS_FILE, [])
        if not isinstance(tasks, list):
            return False, "win_tasks_unavailable"
        for row in tasks:
            if not isinstance(row, dict):
                continue
            if str(row.get("id", "")).upper().strip() != task_id:
                continue
            status = str(row.get("status", "pending")).strip().lower()
            if status == "completed":
                return True, "win_task_status_completed"
            return False, f"win_task_not_completed:{status or 'unknown'}"
        return False, "win_task_id_not_found"

    def _verify_hardware_plan_completion(self, task):
        task_id = str(task.get("task_id", "")).upper().strip()
        payload = _load_json(HARDWARE_PLAN_FILE, {})
        lanes = payload.get("lanes", []) if isinstance(payload, dict) else []
        for lane in lanes:
            if not isinstance(lane, dict):
                continue
            tasks = lane.get("tasks", [])
            if not isinstance(tasks, list):
                continue
            for row in tasks:
                if not isinstance(row, dict):
                    continue
                if str(row.get("task_id", "")).upper().strip() != task_id:
                    continue
                status = str(row.get("status", "")).strip().lower()
                if status in {"completed", "done"}:
                    return True, "hardware_lane_task_completed"
                return False, f"hardware_lane_task_not_completed:{status or 'pending'}"
        return False, "hardware_lane_task_not_found"

    def _source_truth_completed(self, task):
        source = str(task.get("source", "unknown")).strip().lower()
        if source == "advanced_fixes_ranked_100":
            return self._verify_advanced_fix_completion(task)
        if source == "win_1000":
            return self._verify_win_task_completion(task)
        if source == "hardware_resource_delegation_plan":
            return self._verify_hardware_plan_completion(task)
        return False, "unsupported_source_for_truth_check"

    def _verify_task_completion(self, task):
        checked_at = _now_iso()
        verify = {
            "strict_mode": bool(STRICT_COMPLETION_VERIFY),
            "last_checked_at": checked_at,
            "passed": False,
            "reason": "not_checked",
        }
        if not STRICT_COMPLETION_VERIFY:
            verify["passed"] = True
            verify["reason"] = "strict_completion_verify_disabled"
            task["verification"] = verify
            return True, str(verify["reason"])

        source = str(task.get("source", "unknown")).strip().lower()
        passed = False
        reason = "unsupported_source_for_verification"
        if source == "advanced_fixes_ranked_100":
            passed, reason = self._verify_advanced_fix_completion(task)
        elif source == "win_1000":
            passed, reason = self._verify_win_task_completion(task)
        elif source == "hardware_resource_delegation_plan":
            passed, reason = self._verify_hardware_plan_completion(task)
        verify["passed"] = bool(passed)
        verify["reason"] = str(reason)
        task["verification"] = verify
        task["updated_at"] = checked_at
        return bool(passed), str(reason)

    def _reconcile_completion_claims(self):
        verified = 0
        waiting = 0
        for task in self.state["tasks"].values():
            if str(task.get("status", "")).lower() != "completion_claimed":
                continue
            passed, reason = self._verify_task_completion(task)
            if passed:
                task["status"] = "completed"
                task["last_completed_at"] = _now_iso()
                task["updated_at"] = _now_iso()
                verified += 1
            else:
                waiting += 1
                self._append_note(task, f"completion_claim_waiting_verification:{reason}")
        return {"verified": int(verified), "waiting": int(waiting)}

    def _refresh_task_truth(self):
        auto_completed = 0
        for task in self.state["tasks"].values():
            st = str(task.get("status", "")).lower()
            if st in {"completed", "completion_claimed"}:
                continue
            passed, reason = self._source_truth_completed(task)
            if passed:
                task["status"] = "completed"
                task["last_completed_at"] = _now_iso()
                task["verification"] = {
                    "strict_mode": bool(STRICT_COMPLETION_VERIFY),
                    "last_checked_at": _now_iso(),
                    "passed": True,
                    "reason": f"source_truth:{reason}",
                }
                task["updated_at"] = _now_iso()
                self._append_note(task, f"auto_completed_from_source_truth:{reason}")
                auto_completed += 1
        return {"auto_completed": int(auto_completed)}

    def _prune_completed_tasks(self):
        now = datetime.now(timezone.utc)
        to_remove = []
        for task_id, task in self.state["tasks"].items():
            if str(task.get("status", "")).lower() != "completed":
                continue
            done_at = _parse_iso(task.get("last_completed_at"))
            if done_at is None:
                done_at = _parse_iso(task.get("updated_at"))
            if done_at is None:
                to_remove.append(task_id)
                continue
            age = (now - done_at).total_seconds()
            if age >= max(0, int(COMPLETED_RETENTION_SECONDS)):
                to_remove.append(task_id)
        pruned = 0
        for task_id in to_remove:
            task = self.state["tasks"].pop(task_id, None)
            if not isinstance(task, dict):
                continue
            archive_row = {
                "task_id": str(task.get("task_id", task_id)),
                "source": str(task.get("source", "unknown")),
                "status": "completed",
                "completed_at": str(task.get("last_completed_at", "")),
                "archived_at": _now_iso(),
                "verification": task.get("verification", {}),
            }
            self.state.setdefault("archive", [])
            self.state["archive"].append(archive_row)
            self.state["archive"] = self.state["archive"][-400:]
            pruned += 1
        return {"pruned_completed": int(pruned)}

    def _ingest_claude_feedback(self):
        if claude_duplex is None:
            return {
                "available": False,
                "ingested": 0,
                "acked": 0,
                "claimed": 0,
                "completed": 0,
                "completion_waiting_verification": 0,
                "last_from_claude_id": int(self.state.get("last_from_claude_id", 0) or 0),
            }
        since_id = int(self.state.get("last_from_claude_id", 0) or 0)
        rows = claude_duplex.read_from_claude(since_id=since_id, limit=400)
        if not rows:
            return {
                "available": True,
                "ingested": 0,
                "acked": 0,
                "claimed": 0,
                "completed": 0,
                "completion_waiting_verification": 0,
                "last_from_claude_id": since_id,
            }

        max_id = since_id
        acked = 0
        claimed = 0
        completed = 0
        waiting = 0
        for row in rows:
            try:
                rid = int(row.get("id", 0) or 0)
                max_id = max(max_id, rid)
            except Exception:
                pass
            msg_type = str(row.get("msg_type", "")).strip().lower()
            source = str(row.get("source", "")).strip().lower()
            message = str(row.get("message", "")).strip()
            meta = row.get("meta", {}) if isinstance(row.get("meta"), dict) else {}
            referenced = _extract_task_ids(message, meta)
            if not referenced:
                continue
            for task_id in referenced:
                task = self.state["tasks"].get(task_id)
                if not task:
                    continue
                task["last_ack_at"] = _now_iso()
                task["last_ack_msg_id"] = int(row.get("id", 0) or 0)
                task["last_ack_source"] = source
                if _is_completion(msg_type, message):
                    claimed += 1
                    task["status"] = "completion_claimed"
                    task["completion_claimed_at"] = _now_iso()
                    task["completion_claim_msg_id"] = int(row.get("id", 0) or 0)
                    task["completion_claim_source"] = source
                    passed, reason = self._verify_task_completion(task)
                    if passed:
                        if task.get("status") != "completed":
                            completed += 1
                        task["status"] = "completed"
                        task["last_completed_at"] = _now_iso()
                    else:
                        waiting += 1
                        self._append_note(task, f"completion_claim_not_verified:{reason}")
                else:
                    if task.get("status") in {"queued", "dispatched", "stale", "completion_claimed"}:
                        task["status"] = "acknowledged"
                        acked += 1
                task["updated_at"] = _now_iso()
        self.state["last_from_claude_id"] = max_id
        return {
            "available": True,
            "ingested": len(rows),
            "acked": int(acked),
            "claimed": int(claimed),
            "completed": int(completed),
            "completion_waiting_verification": int(waiting),
            "last_from_claude_id": int(max_id),
        }

    def _mark_stale(self):
        now = datetime.now(timezone.utc)
        stale = 0
        blocked = 0
        for task in self.state["tasks"].values():
            if str(task.get("status", "")) != "dispatched":
                continue
            sent_at = _parse_iso(task.get("last_dispatched_at"))
            if not sent_at:
                continue
            age = (now - sent_at).total_seconds()
            if age >= max(60, int(STALE_SECONDS)):
                if int(task.get("dispatch_count", 0) or 0) >= int(MAX_DISPATCH_ATTEMPTS):
                    task["status"] = "blocked"
                    task["blocked_reason"] = "max_dispatch_attempts_exceeded"
                    blocked += 1
                else:
                    task["status"] = "stale"
                    stale += 1
                task["updated_at"] = _now_iso()
        return {"stale": int(stale), "blocked": int(blocked)}

    def _dispatch(self):
        if claude_duplex is None:
            return {"available": False, "dispatched": 0, "ids": []}
        outstanding = [
            t
            for t in self.state["tasks"].values()
            if str(t.get("status", "")) in {"dispatched", "acknowledged", "completion_claimed"}
        ]
        room = max(0, int(MAX_OUTSTANDING) - len(outstanding))
        if room <= 0:
            return {"available": True, "dispatched": 0, "ids": [], "room": 0}

        candidates = []
        for task in self.state["tasks"].values():
            if str(task.get("status", "")).lower() in {"queued", "stale"}:
                candidates.append(task)
        candidates.sort(key=_task_key_priority)
        send_n = min(room, max(0, int(DISPATCH_PER_CYCLE)), len(candidates))
        selected = []
        source_counts = {}
        source_cap = max(1, int(DISPATCH_MAX_PER_SOURCE))
        for task in candidates:
            src = str(task.get("source", "unknown"))
            if source_counts.get(src, 0) >= source_cap:
                continue
            selected.append(task)
            source_counts[src] = int(source_counts.get(src, 0)) + 1
            if len(selected) >= send_n:
                break
        if len(selected) < send_n:
            selected_ids = {str(t.get("task_id", "")) for t in selected}
            for task in candidates:
                tid = str(task.get("task_id", ""))
                if tid in selected_ids:
                    continue
                selected.append(task)
                if len(selected) >= send_n:
                    break
        sent_ids = []
        for task in selected[:send_n]:
            task_id = str(task.get("task_id", "")).strip().upper()
            title = str(task.get("title", "")).strip()
            acceptance = str(task.get("acceptance", "")).strip()
            message = (
                f"[{task_id}] {title}\n"
                f"Acceptance: {acceptance or 'provide deterministic measurable patch + tests + rollback criteria'}\n"
                "Reply with task_id + trace_id for ACK, then completion/progress updates."
            )
            row = claude_duplex.send_to_claude(
                message,
                msg_type="autonomous_work_item",
                priority=str(task.get("priority", "normal") or "normal"),
                source="clawdbot_autonomy",
                meta={
                    "task_id": task_id,
                    "source": str(task.get("source", "unknown")),
                    "rank": int(task.get("rank", 0) or 0),
                    "priority_score": float(task.get("priority_score", 0.0) or 0.0),
                    "acceptance": acceptance,
                },
            )
            trace_id = str(row.get("trace_id", "")).strip()
            task["status"] = "dispatched"
            task["dispatch_count"] = int(task.get("dispatch_count", 0) or 0) + 1
            task["trace_id"] = trace_id
            task["outbound_msg_id"] = int(row.get("id", 0) or 0)
            task["last_dispatched_at"] = str(row.get("timestamp", _now_iso()))
            task["updated_at"] = _now_iso()
            sent_ids.append(int(row.get("id", 0) or 0))
        return {"available": True, "dispatched": len(sent_ids), "ids": sent_ids, "room": room}

    def _compact_status(self, feedback, dispatch, imported):
        tasks = list(self.state["tasks"].values())
        counts = {
            "queued": 0,
            "dispatched": 0,
            "acknowledged": 0,
            "completion_claimed": 0,
            "completed": 0,
            "stale": 0,
            "blocked": 0,
        }
        for t in tasks:
            st = str(t.get("status", "queued")).lower()
            if st not in counts:
                counts["queued"] += 1
            else:
                counts[st] += 1

        hardware_tasks = [t for t in tasks if str(t.get("task_id", "")).upper().startswith("HWP-")]
        hardware_counts = {}
        for t in hardware_tasks:
            st = str(t.get("status", "queued")).lower()
            hardware_counts[st] = int(hardware_counts.get(st, 0) or 0) + 1
        hardware_queue = [
            {
                "task_id": t.get("task_id"),
                "title": str(t.get("title", ""))[:120],
                "status": t.get("status"),
                "dispatch_count": int(t.get("dispatch_count", 0) or 0),
                "outbound_msg_id": int(t.get("outbound_msg_id", 0) or 0),
                "trace_id": str(t.get("trace_id", "")),
                "verification_reason": str((t.get("verification", {}) or {}).get("reason", "")),
            }
            for t in sorted(hardware_tasks, key=_task_key_priority)
        ]

        top_queue = [
            {
                "task_id": t.get("task_id"),
                "title": str(t.get("title", ""))[:120],
                "status": t.get("status"),
                "priority_score": float(t.get("priority_score", 0.0) or 0.0),
                "dispatch_count": int(t.get("dispatch_count", 0) or 0),
                "trace_id": t.get("trace_id", ""),
                "verification_reason": str((t.get("verification", {}) or {}).get("reason", "")),
            }
            for t in sorted(tasks, key=_task_key_priority)[:25]
        ]
        status = {
            "updated_at": _now_iso(),
            "agent": "autonomous_work_manager",
            "duplex_available": bool(claude_duplex is not None),
            "queue_size": len(tasks),
            "queue_target_size": int(QUEUE_TARGET_SIZE),
            "max_outstanding": int(MAX_OUTSTANDING),
            "dispatch_per_cycle": int(DISPATCH_PER_CYCLE),
            "last_from_claude_id": int(self.state.get("last_from_claude_id", 0) or 0),
            "counts": counts,
            "imported": imported,
            "feedback": feedback,
            "dispatch": dispatch,
            "hardware_delegation": {
                "total": len(hardware_tasks),
                "counts": hardware_counts,
                "tasks": hardware_queue,
            },
            "top_queue": top_queue,
            "win_1000_snapshot": _compact_win_status(_load_json(WIN_STATUS_FILE, {})),
        }
        _save_json(STATUS_FILE, status)
        return status

    def run_once(self):
        imported = {
            "hardware_added": 0,
            "advanced_fixes_added": 0,
            "win_tasks_added": 0,
            "queue_size_before": self._queue_size(),
        }
        truth_refresh = self._refresh_task_truth()
        prune_result = self._prune_completed_tasks()
        current = self._queue_size()
        if current < int(QUEUE_TARGET_SIZE):
            room = int(QUEUE_TARGET_SIZE) - current
            fixes = _import_advanced_fix_tasks(limit=min(room, ADVANCED_FIXES_IMPORT_LIMIT))
            imported["advanced_fixes_added"] = self._upsert_tasks(fixes)
            current = self._queue_size()
            room = max(0, int(QUEUE_TARGET_SIZE) - current)
            win_tasks = _import_win_tasks(limit=min(room, WIN_TASKS_IMPORT_LIMIT))
            imported["win_tasks_added"] = self._upsert_tasks(win_tasks)
        current = self._queue_size()
        if current < int(QUEUE_TARGET_SIZE):
            room = int(QUEUE_TARGET_SIZE) - current
            hardware_tasks = _import_hardware_lane_tasks(limit=min(room, HARDWARE_PLAN_IMPORT_LIMIT))
            imported["hardware_added"] = self._upsert_tasks(hardware_tasks)

        stale_result = self._mark_stale()
        feedback = self._ingest_claude_feedback()
        completion_reconcile = self._reconcile_completion_claims()
        dispatch = self._dispatch()
        imported["stale_marked"] = int(stale_result.get("stale", 0) or 0)
        imported["blocked_marked"] = int(stale_result.get("blocked", 0) or 0)
        imported["completion_verified"] = int(completion_reconcile.get("verified", 0) or 0)
        imported["completion_waiting"] = int(completion_reconcile.get("waiting", 0) or 0)
        imported["auto_completed_from_source_truth"] = int(truth_refresh.get("auto_completed", 0) or 0)
        imported["pruned_completed"] = int(prune_result.get("pruned_completed", 0) or 0)
        imported["queue_size_after"] = self._queue_size()
        self._persist()
        status = self._compact_status(feedback=feedback, dispatch=dispatch, imported=imported)
        logger.info(
            "queue=%d dispatched=%d acked=%d completed=%d stale=%d",
            int(status.get("queue_size", 0) or 0),
            int((status.get("dispatch", {}) or {}).get("dispatched", 0) or 0),
            int((status.get("feedback", {}) or {}).get("acked", 0) or 0),
            int((status.get("feedback", {}) or {}).get("completed", 0) or 0),
            int((status.get("imported", {}) or {}).get("stale_marked", 0) or 0),
        )
        return status

    def run_forever(self):
        def _stop(signum, _frame):
            logger.info("received signal %d, stopping...", signum)
            self.running = False

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

        logger.info(
            "autonomous work manager started interval=%ss target=%s outstanding=%s",
            int(INTERVAL_SECONDS),
            int(QUEUE_TARGET_SIZE),
            int(MAX_OUTSTANDING),
        )
        while self.running:
            try:
                self.run_once()
            except Exception as e:
                logger.error("run_once failed: %s", e, exc_info=True)
            for _ in range(max(5, int(INTERVAL_SECONDS))):
                if not self.running:
                    break
                time.sleep(1)
        logger.info("autonomous work manager stopped")


def print_status():
    if not STATUS_FILE.exists():
        print("No autonomous work status yet.")
        return
    print(STATUS_FILE.read_text())


def main():
    parser = argparse.ArgumentParser(description="Autonomous work manager for clawd.bot collaboration")
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    parser.add_argument("--status", action="store_true", help="print current status")
    args = parser.parse_args()

    if args.status:
        print_status()
        return

    mgr = AutonomousWorkManager()
    if args.once:
        print(json.dumps(mgr.run_once(), indent=2))
    else:
        mgr.run_forever()


if __name__ == "__main__":
    main()
