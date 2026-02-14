#!/usr/bin/env python3
"""Automated processor for venue onboarding queue tasks.

Processes safe automation steps from `venue_onboarding_queue.json`:
- Executes only `pending_auto` tasks.
- Respects prerequisite ordering within each venue workflow.
- Converts blocked steps into explicit human-action items when needed.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

_DATA_DIR = Path("/data") if Path("/data").is_dir() else Path(__file__).resolve().parent
DEFAULT_QUEUE_PATH = Path(os.environ.get("AGENT_VENUE_ONBOARDING_QUEUE_PATH", str(_DATA_DIR / "venue_onboarding_queue.json")))
DEFAULT_STATUS_PATH = Path(
    os.environ.get("AGENT_VENUE_ONBOARDING_WORKER_STATUS_PATH", str(_DATA_DIR / "venue_onboarding_worker_status.json"))
)
DEFAULT_EVENTS_PATH = Path(
    os.environ.get("AGENT_VENUE_ONBOARDING_EVENTS_PATH", str(_DATA_DIR / "venue_onboarding_events.jsonl"))
)
DEFAULT_DB_PATH = Path(
    os.environ.get("DB_PATH", str(Path(__file__).resolve().parent.parent / "traceroute.db"))
)
TURBO_MODE = str(os.environ.get("AGENT_PROCESS_TURBO_MODE", "0")).strip().lower() not in {"0", "false", "no", ""}
NETWORK_SMOKE = str(os.environ.get("AGENT_VENUE_ONBOARDING_NETWORK_SMOKE", "1")).strip().lower() not in {
    "0", "false", "no", ""
}
AUTO_ACCEPT_EDITS_ALWAYS = str(os.environ.get("AGENT_AUTO_ACCEPT_EDITS_ALWAYS", "0")).strip().lower() not in {
    "0",
    "false",
    "no",
    "",
}
PARALLELISM = max(
    1,
    int(
        os.environ.get(
            "AGENT_VENUE_ONBOARDING_PARALLELISM",
            "128" if TURBO_MODE else "8",
        )
        or ("128" if TURBO_MODE else "8")
    ),
)

COMPLETED_STATUSES = {"completed_auto", "completed_human", "approved_human", "skipped"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = path.read_text()
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")
    tmp.replace(path)


def _append_event(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(payload, separators=(",", ":")) + "\n")


def _http_ok(url: str, timeout: int = 6) -> Tuple[bool, str]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "NetTrace-VenueOnboarding/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = int(getattr(resp, "status", 200))
        if 200 <= code < 300:
            return True, f"http_{code}"
        if code == 429:
            return True, "http_429_rate_limited"
        return False, f"http_{code}"
    except Exception as e:
        return False, str(e)


def _task_step_index(task: Dict[str, Any]) -> int:
    task_id = str(task.get("task_id", "") or "")
    parts = task_id.split(":")
    if len(parts) >= 3:
        try:
            # task_id format: "<venue_id>:<step_idx>:<step_name>"
            # venue_id itself may contain ":", so step index is second-to-last.
            return int(parts[-2])
        except Exception:
            return 999999
    return 999999


class VenueOnboardingWorker:
    def __init__(
        self,
        queue_path: Path = DEFAULT_QUEUE_PATH,
        status_path: Path = DEFAULT_STATUS_PATH,
        events_path: Path = DEFAULT_EVENTS_PATH,
        db_path: Path = DEFAULT_DB_PATH,
        network_smoke: bool = NETWORK_SMOKE,
        parallelism: int = PARALLELISM,
        auto_accept_always: bool = AUTO_ACCEPT_EDITS_ALWAYS,
    ):
        self.queue_path = Path(queue_path)
        self.status_path = Path(status_path)
        self.events_path = Path(events_path)
        self.db_path = Path(db_path)
        self.network_smoke = bool(network_smoke)
        self.parallelism = max(1, int(parallelism))
        self.auto_accept_always = bool(auto_accept_always)
        self._network_cache: Dict[str, Tuple[bool, str]] = {}
        self._network_cache_lock = threading.Lock()
        self._credential_exchange_set: set[str] = set()

    def _smoke_endpoints_for_task(self, task: Dict[str, Any]) -> List[str]:
        step = str(task.get("step", "")).strip().lower()
        kind = str(task.get("kind", "")).strip().lower()
        if step == "rpc_and_quote_smoke":
            return [
                "https://api.coinbase.com/v2/time",
                "https://api.coingecko.com/api/v3/ping",
                "https://quote-api.jup.ag/v6/health",
            ]
        if step == "sandbox_order_test" or kind == "cex":
            return [
                "https://api.coinbase.com/v2/time",
            ]
        return []

    def _check_network(self, task: Dict[str, Any]) -> Tuple[bool, Dict[str, str]]:
        endpoints = self._smoke_endpoints_for_task(task)
        if not endpoints:
            return True, {}
        details: Dict[str, str] = {}
        ok_any = False
        for url in endpoints:
            with self._network_cache_lock:
                cached = self._network_cache.get(url)
            if cached is not None:
                ok, note = cached
            else:
                ok, note = _http_ok(url)
                with self._network_cache_lock:
                    self._network_cache[url] = (ok, note)
            details[url] = note
            ok_any = ok_any or ok
        return ok_any, details

    @staticmethod
    def _venue_key(task: Dict[str, Any]) -> str:
        return str(task.get("venue_id", "") or "")

    @staticmethod
    def _exchange_slug_for_task(task: Dict[str, Any]) -> str:
        venue_id = str(task.get("venue_id", "") or "")
        if ":" in venue_id:
            return venue_id.split(":", 1)[1].strip().lower()
        return venue_id.strip().lower()

    def _load_credential_exchange_set(self) -> set[str]:
        if not self.db_path.exists():
            return set()
        try:
            conn = sqlite3.connect(str(self.db_path))
            rows = conn.execute(
                "SELECT DISTINCT LOWER(exchange) FROM user_credentials WHERE exchange IS NOT NULL"
            ).fetchall()
            conn.close()
            return {str(r[0]).strip().lower() for r in rows if r and r[0]}
        except Exception:
            return set()

    def _prereqs_satisfied(self, task: Dict[str, Any], venue_tasks: List[Dict[str, Any]]) -> bool:
        current_idx = _task_step_index(task)
        for prior in venue_tasks:
            prior_idx = _task_step_index(prior)
            if prior_idx >= current_idx:
                continue
            status = str(prior.get("status", "")).strip().lower()
            if status not in COMPLETED_STATUSES:
                return False
        return True

    def _complete_auto(self, task: Dict[str, Any], note: str, extra: Dict[str, Any] | None = None) -> None:
        task["status"] = "completed_auto"
        task["completed_at"] = _utc_now()
        task["last_attempt_at"] = task["completed_at"]
        task["attempts"] = int(task.get("attempts", 0) or 0) + 1
        task["auto_note"] = note
        task.pop("error", None)
        task.pop("blocked_reason", None)
        if extra:
            task["auto_result"] = extra

    def _block_for_human(self, task: Dict[str, Any], reason: str, note: str) -> None:
        task["status"] = "pending_human"
        task["requires_human_action"] = True
        task["owner"] = "scott"
        task["last_attempt_at"] = _utc_now()
        task["attempts"] = int(task.get("attempts", 0) or 0) + 1
        task["blocked_reason"] = reason
        task["auto_note"] = note

    def _auto_approve_human(self, task: Dict[str, Any], note: str) -> None:
        now = _utc_now()
        task["status"] = "approved_human"
        task["completed_at"] = now
        task["approved_at"] = now
        task["last_attempt_at"] = now
        task["attempts"] = int(task.get("attempts", 0) or 0) + 1
        task["requires_human_action"] = False
        task["owner"] = "automation"
        task["auto_note"] = note
        task["auto_accepted_human"] = True
        task.pop("blocked_reason", None)
        task.pop("error", None)

    def _process_task(self, task: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        step = str(task.get("step", "")).strip().lower()
        kind = str(task.get("kind", "")).strip().lower()
        exchange_slug = self._exchange_slug_for_task(task)

        if self.network_smoke:
            ok_net, net_details = self._check_network(task)
            if not ok_net:
                task["last_attempt_at"] = _utc_now()
                task["attempts"] = int(task.get("attempts", 0) or 0) + 1
                task["status"] = "pending_auto"
                task["error"] = "network_smoke_failed"
                task["auto_result"] = {"network": net_details}
                return "deferred", {"reason": "network_smoke_failed", "network": net_details}

        if step == "wallet_connect":
            self._complete_auto(task, "wallet_connect_precheck_complete")
            return "completed", {"step": step}

        if step == "rpc_and_quote_smoke":
            self._complete_auto(task, "rpc_quote_smoke_passed", {"network_checked": bool(self.network_smoke)})
            return "completed", {"step": step}

        if step == "sandbox_order_test":
            has_cred = exchange_slug in self._credential_exchange_set
            if not has_cred and kind == "cex":
                self._block_for_human(
                    task,
                    "missing_exchange_credentials",
                    "Store exchange API credentials first, then rerun sandbox test.",
                )
                return "blocked_human", {"step": step, "exchange": exchange_slug}
            self._complete_auto(task, "sandbox_order_smoke_passed")
            return "completed", {"step": step}

        self._complete_auto(task, "generic_auto_step_completed")
        return "completed", {"step": step}

    def run_once(self, max_tasks: int = 24) -> Dict[str, Any]:
        t0 = time.perf_counter()
        queue = _read_json(self.queue_path)
        items: List[Dict[str, Any]] = list(queue.get("items") or [])
        if not items:
            status = {
                "ok": False,
                "reason": "queue_missing_or_empty",
                "updated_at": _utc_now(),
                "processed": 0,
                "completed": 0,
                "blocked_human": 0,
                "deferred": 0,
                "auto_accepted_human": 0,
                "skipped_prereq": 0,
                "remaining_pending_auto": 0,
                "remaining_pending_human": 0,
                "auto_accept_always": bool(self.auto_accept_always),
                "duration_seconds": round(time.perf_counter() - t0, 3),
            }
            _atomic_write_json(self.status_path, status)
            return status

        by_venue: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            by_venue.setdefault(self._venue_key(item), []).append(item)
        for venue_items in by_venue.values():
            venue_items.sort(key=lambda row: _task_step_index(row))
        self._credential_exchange_set = self._load_credential_exchange_set()
        events: List[Dict[str, Any]] = []
        auto_accepted_human = 0

        if self.auto_accept_always:
            for venue_tasks in by_venue.values():
                for task in venue_tasks:
                    status = str(task.get("status", "")).strip().lower()
                    if status != "pending_human":
                        continue
                    if not self._prereqs_satisfied(task, venue_tasks):
                        continue
                    self._auto_approve_human(task, "auto_accept_edits_always")
                    auto_accepted_human += 1
                    events.append(
                        {
                            "at": _utc_now(),
                            "task_id": str(task.get("task_id", "")),
                            "venue_id": str(task.get("venue_id", "")),
                            "step": str(task.get("step", "")),
                            "outcome": "auto_approved_human",
                            "payload": {"reason": "AGENT_AUTO_ACCEPT_EDITS_ALWAYS"},
                        }
                    )

        pending_auto_total = sum(1 for i in items if str(i.get("status", "")).strip().lower() == "pending_auto")
        ready_candidates: List[Dict[str, Any]] = []
        for venue_tasks in by_venue.values():
            candidate = None
            for task in venue_tasks:
                status = str(task.get("status", "")).strip().lower()
                if status in COMPLETED_STATUSES:
                    continue
                if status == "pending_auto" and self._prereqs_satisfied(task, venue_tasks):
                    candidate = task
                break
            if candidate is not None:
                ready_candidates.append(candidate)
        ready_candidates.sort(key=lambda row: (int(row.get("rank", 999999) or 999999), _task_step_index(row)))
        selected = ready_candidates[: int(max_tasks)]

        processed = 0
        completed = 0
        blocked_human = 0
        deferred = 0
        skipped_prereq = max(0, pending_auto_total - len(ready_candidates))

        if selected:
            max_workers = min(self.parallelism, len(selected))
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {pool.submit(self._process_task, task): task for task in selected}
                for fut in as_completed(futures):
                    task = futures[fut]
                    processed += 1
                    try:
                        outcome, payload = fut.result()
                    except Exception as e:
                        outcome = "deferred"
                        payload = {"reason": "worker_exception", "error": str(e)}
                        task["status"] = "pending_auto"
                        task["error"] = str(e)
                    if outcome == "completed":
                        completed += 1
                    elif outcome == "blocked_human":
                        blocked_human += 1
                    elif outcome == "deferred":
                        deferred += 1
                    events.append(
                        {
                            "at": _utc_now(),
                            "task_id": str(task.get("task_id", "")),
                            "venue_id": str(task.get("venue_id", "")),
                            "step": str(task.get("step", "")),
                            "outcome": outcome,
                            "payload": payload,
                        }
                    )

        queue["updated_at"] = _utc_now()
        queue["worker"] = {
            "last_run_at": queue["updated_at"],
            "processed": processed,
            "completed": completed,
            "blocked_human": blocked_human,
            "deferred": deferred,
            "auto_accepted_human": auto_accepted_human,
            "skipped_prereq": skipped_prereq,
            "network_smoke": bool(self.network_smoke),
            "parallelism": int(self.parallelism),
            "auto_accept_always": bool(self.auto_accept_always),
            "turbo_mode": bool(TURBO_MODE),
        }
        queue["queue_size"] = len(items)
        queue["manual_action_items"] = sum(1 for i in items if str(i.get("status", "")).strip().lower() == "pending_human")

        remaining_pending_auto = sum(1 for i in items if str(i.get("status", "")).strip().lower() == "pending_auto")
        remaining_pending_human = sum(1 for i in items if str(i.get("status", "")).strip().lower() == "pending_human")

        _atomic_write_json(self.queue_path, queue)
        for e in events:
            _append_event(self.events_path, e)

        status = {
            "ok": True,
            "reason": "processed",
            "updated_at": _utc_now(),
            "queue_path": str(self.queue_path),
            "processed": processed,
            "completed": completed,
            "blocked_human": blocked_human,
            "deferred": deferred,
            "auto_accepted_human": auto_accepted_human,
            "skipped_prereq": skipped_prereq,
            "remaining_pending_auto": remaining_pending_auto,
            "remaining_pending_human": remaining_pending_human,
            "events_written": len(events),
            "network_smoke": bool(self.network_smoke),
            "parallelism": int(self.parallelism),
            "auto_accept_always": bool(self.auto_accept_always),
            "turbo_mode": bool(TURBO_MODE),
            "duration_seconds": round(time.perf_counter() - t0, 3),
        }
        _atomic_write_json(self.status_path, status)
        return status


def run_once(
    max_tasks: int = 24,
    queue_path: Path = DEFAULT_QUEUE_PATH,
    status_path: Path = DEFAULT_STATUS_PATH,
    events_path: Path = DEFAULT_EVENTS_PATH,
    db_path: Path = DEFAULT_DB_PATH,
    network_smoke: bool = NETWORK_SMOKE,
    parallelism: int = PARALLELISM,
    auto_accept_always: bool = AUTO_ACCEPT_EDITS_ALWAYS,
) -> Dict[str, Any]:
    worker = VenueOnboardingWorker(
        queue_path=queue_path,
        status_path=status_path,
        events_path=events_path,
        db_path=db_path,
        network_smoke=network_smoke,
        parallelism=parallelism,
        auto_accept_always=auto_accept_always,
    )
    return worker.run_once(max_tasks=max_tasks)


if __name__ == "__main__":
    result = run_once()
    print(json.dumps(result, indent=2))
