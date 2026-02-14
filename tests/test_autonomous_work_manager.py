#!/usr/bin/env python3
"""Tests for autonomous work manager queue + duplex lifecycle."""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import autonomous_work_manager as awm  # noqa: E402


class _DummyDuplex:
    def __init__(self):
        self.sent = []
        self.incoming = []
        self._id = 100

    def send_to_claude(self, message, msg_type="directive", priority="normal", source="operator", meta=None):
        self._id += 1
        row = {
            "id": self._id,
            "timestamp": "2026-02-13T00:00:00+00:00",
            "trace_id": f"trace-{self._id}",
            "msg_type": msg_type,
            "priority": priority,
            "source": source,
            "message": message,
            "meta": meta or {},
        }
        self.sent.append(row)
        return row

    def read_from_claude(self, since_id=0, limit=400):
        rows = [r for r in self.incoming if int(r.get("id", 0) or 0) > int(since_id or 0)]
        if limit and limit > 0:
            rows = rows[-limit:]
        return rows


def _setup_paths(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(awm, "QUEUE_FILE", tmp_path / "autonomous_work_queue.json")
    monkeypatch.setattr(awm, "STATUS_FILE", tmp_path / "autonomous_work_status.json")
    monkeypatch.setattr(awm, "ADVANCED_FIXES_FILE", tmp_path / "advanced_fixes_ranked_100.json")
    monkeypatch.setattr(awm, "WIN_TASKS_FILE", tmp_path / "win_1000_tasks.json")
    monkeypatch.setattr(awm, "WIN_STATUS_FILE", tmp_path / "win_1000_status.json")


def test_autonomous_work_imports_and_dispatches(monkeypatch, tmp_path):
    _setup_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(awm, "QUEUE_TARGET_SIZE", 8)
    monkeypatch.setattr(awm, "DISPATCH_PER_CYCLE", 3)
    monkeypatch.setattr(awm, "MAX_OUTSTANDING", 5)

    (tmp_path / "advanced_fixes_ranked_100.json").write_text(
        json.dumps(
            {
                "items": [
                    {"rank": 14, "title": "Walk-forward split randomization", "score": 77.0, "implemented": False},
                    {"rank": 15, "title": "Cross-venue basis classifier", "score": 75.0, "implemented": False},
                ]
            }
        )
    )
    (tmp_path / "win_1000_tasks.json").write_text(
        json.dumps(
            [
                {"id": "WIN-0001", "title": "Close velocity boost", "status": "pending", "priority_score": 93.0, "priority_rank": 1},
                {"id": "WIN-0002", "title": "Infra DNS gate", "status": "completed", "priority_score": 92.0, "priority_rank": 2},
            ]
        )
    )
    (tmp_path / "win_1000_status.json").write_text(json.dumps({"tasks_total": 1000}))

    dummy = _DummyDuplex()
    monkeypatch.setattr(awm, "claude_duplex", dummy)

    mgr = awm.AutonomousWorkManager()
    status = mgr.run_once()

    assert status["queue_size"] >= 3
    assert status["dispatch"]["dispatched"] == 3
    assert len(dummy.sent) == 3
    assert (tmp_path / "autonomous_work_queue.json").exists()
    queue_payload = json.loads((tmp_path / "autonomous_work_queue.json").read_text())
    assert "AF-014" in queue_payload["tasks"]
    assert "WIN-0001" in queue_payload["tasks"]


def test_autonomous_work_ingests_completion(monkeypatch, tmp_path):
    _setup_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(awm, "QUEUE_TARGET_SIZE", 3)
    monkeypatch.setattr(awm, "DISPATCH_PER_CYCLE", 1)
    monkeypatch.setattr(awm, "MAX_OUTSTANDING", 3)

    (tmp_path / "advanced_fixes_ranked_100.json").write_text(
        json.dumps({"items": [{"rank": 20, "title": "Latency-aware alpha half-life", "score": 74.0, "implemented": False}]})
    )
    (tmp_path / "win_1000_tasks.json").write_text(json.dumps([]))
    (tmp_path / "win_1000_status.json").write_text(json.dumps({}))

    dummy = _DummyDuplex()
    monkeypatch.setattr(awm, "claude_duplex", dummy)

    mgr = awm.AutonomousWorkManager()
    first = mgr.run_once()
    assert first["dispatch"]["dispatched"] == 1

    dummy.incoming = [
        {
            "id": 1000,
            "source": "claude_sonnet",
            "msg_type": "completion",
            "message": "AF-020 completed with tests passing.",
            "meta": {"task_id": "AF-020"},
        }
    ]
    second = mgr.run_once()
    assert second["feedback"]["completion_waiting_verification"] >= 1

    q = json.loads((tmp_path / "autonomous_work_queue.json").read_text())
    assert q["tasks"]["AF-020"]["status"] == "completion_claimed"

    # Evidence becomes true -> manager auto-verifies to completed.
    (tmp_path / "advanced_fixes_ranked_100.json").write_text(
        json.dumps({"items": [{"rank": 20, "title": "Latency-aware alpha half-life", "score": 74.0, "implemented": True}]})
    )
    third = mgr.run_once()
    assert third["imported"]["completion_verified"] >= 1
    q2 = json.loads((tmp_path / "autonomous_work_queue.json").read_text())
    assert q2["tasks"]["AF-020"]["status"] == "completed"


def test_dispatch_respects_source_cap(monkeypatch, tmp_path):
    _setup_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(awm, "QUEUE_TARGET_SIZE", 6)
    monkeypatch.setattr(awm, "DISPATCH_PER_CYCLE", 4)
    monkeypatch.setattr(awm, "MAX_OUTSTANDING", 6)
    monkeypatch.setattr(awm, "DISPATCH_MAX_PER_SOURCE", 1)

    (tmp_path / "advanced_fixes_ranked_100.json").write_text(
        json.dumps(
            {
                "items": [
                    {"rank": 21, "title": "AF a", "score": 75.0, "implemented": False},
                    {"rank": 22, "title": "AF b", "score": 74.0, "implemented": False},
                ]
            }
        )
    )
    (tmp_path / "win_1000_tasks.json").write_text(
        json.dumps(
            [
                {"id": "WIN-0003", "title": "win a", "status": "pending", "priority_score": 95.0, "priority_rank": 1},
                {"id": "WIN-0004", "title": "win b", "status": "pending", "priority_score": 94.0, "priority_rank": 2},
            ]
        )
    )
    (tmp_path / "win_1000_status.json").write_text(json.dumps({}))

    dummy = _DummyDuplex()
    monkeypatch.setattr(awm, "claude_duplex", dummy)

    mgr = awm.AutonomousWorkManager()
    status = mgr.run_once()
    assert status["dispatch"]["dispatched"] >= 2
    queue = json.loads((tmp_path / "autonomous_work_queue.json").read_text())["tasks"]
    dispatched = [v for v in queue.values() if v.get("status") == "dispatched"]
    sources = {str(v.get("source", "")) for v in dispatched}
    assert "advanced_fixes_ranked_100" in sources
    assert "win_1000" in sources


def test_auto_complete_from_source_truth_and_prune(monkeypatch, tmp_path):
    _setup_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(awm, "QUEUE_TARGET_SIZE", 2)
    monkeypatch.setattr(awm, "DISPATCH_PER_CYCLE", 0)
    monkeypatch.setattr(awm, "COMPLETED_RETENTION_SECONDS", 0)

    (tmp_path / "advanced_fixes_ranked_100.json").write_text(
        json.dumps({"items": [{"rank": 30, "title": "AF c", "score": 70.0, "implemented": True}]})
    )
    (tmp_path / "win_1000_tasks.json").write_text(json.dumps([]))
    (tmp_path / "win_1000_status.json").write_text(json.dumps({}))

    monkeypatch.setattr(awm, "claude_duplex", _DummyDuplex())
    mgr = awm.AutonomousWorkManager()
    mgr.state["tasks"]["AF-030"] = {
        "task_id": "AF-030",
        "source": "advanced_fixes_ranked_100",
        "rank": 30,
        "title": "AF c",
        "status": "queued",
        "priority_score": 70.0,
        "priority": "normal",
        "dispatch_count": 0,
        "notes": [],
        "updated_at": "2020-01-01T00:00:00+00:00",
    }

    first = mgr.run_once()
    assert first["imported"]["auto_completed_from_source_truth"] >= 1
    q = json.loads((tmp_path / "autonomous_work_queue.json").read_text())
    assert "AF-030" not in q["tasks"]
    assert len(q.get("archive", [])) >= 1
    assert first["imported"]["pruned_completed"] >= 1
