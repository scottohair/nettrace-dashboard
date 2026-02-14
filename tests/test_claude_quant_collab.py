#!/usr/bin/env python3
"""Tests for Claude Opus/Sonnet collaboration digest logic."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import claude_quant_agent as cqa  # noqa: E402


def test_directive_digest_counts_topics_and_roles():
    msgs = [
        {
            "source": "codex",
            "priority": "high",
            "message": "Strict realized pnl gate and budget escalation policy.",
            "meta": {"target_role": "opus"},
        },
        {
            "source": "flywheel_controller",
            "priority": "normal",
            "message": "Need execution latency and fill improvements for SELL close.",
            "meta": {"target_role": "sonnet"},
        },
    ]
    digest = cqa.ClaudeQuantAgent._directive_digest(msgs)
    assert digest["total"] == 2
    assert digest["from_codex"] == 1
    assert digest["high_priority"] == 1
    assert digest["target_role_counts"]["opus"] == 1
    assert digest["target_role_counts"]["sonnet"] == 1
    assert digest["topic_hits"]["realized_pnl"] >= 1
    assert digest["topic_hits"]["budget"] >= 1
    assert digest["topic_hits"]["execution"] >= 1


def test_consume_directives_returns_trace_ids(monkeypatch, tmp_path):
    class _DummyDuplex:
        @staticmethod
        def read_to_claude(since_id=0, limit=200):
            assert since_id == 0
            assert limit == 200
            return [
                {"id": 1, "message": "prioritize BTC-USD AF-014", "meta": {"trace_id": "trace-meta-1", "task_id": "AF-014"}},
                {"id": 2, "message": "focus ETH-USD", "trace_id": "trace-row-2", "meta": {"task_ids": ["WIN-0901"]}},
            ]

    monkeypatch.setattr(cqa, "claude_duplex", _DummyDuplex())
    monkeypatch.setattr(cqa, "STATUS_FILE", tmp_path / "quant_status.json")

    agent = cqa.ClaudeQuantAgent()
    msgs, pairs, traces, task_ids = agent._consume_duplex_directives()
    assert len(msgs) == 2
    assert pairs == ["BTC-USD", "ETH-USD"]
    assert traces == ["trace-meta-1", "trace-row-2"]
    assert task_ids == ["AF-014", "WIN-0901"]


def test_run_once_emits_work_ack_for_task_ids(monkeypatch, tmp_path):
    class _DummyDuplex:
        def __init__(self):
            self.sent = []

        @staticmethod
        def read_to_claude(since_id=0, limit=200):
            assert since_id == 0
            return [
                {
                    "id": 11,
                    "message": "autonomous item AF-014",
                    "meta": {"trace_id": "t1", "task_id": "AF-014"},
                    "source": "clawdbot_autonomy",
                }
            ]

        def send_from_claude(self, message, msg_type="response", priority="normal", source="claude", meta=None):
            row = {
                "message": message,
                "msg_type": msg_type,
                "priority": priority,
                "source": source,
                "meta": meta or {},
            }
            self.sent.append(row)
            return row

    dummy = _DummyDuplex()
    monkeypatch.setattr(cqa, "claude_duplex", dummy)
    monkeypatch.setattr(cqa, "claude_staging", None)
    monkeypatch.setattr(cqa, "CLAUDE_TEAM_COLLAB_ENABLED", False)
    monkeypatch.setattr(cqa, "STATUS_FILE", tmp_path / "quant_status.json")
    monkeypatch.setattr(cqa, "CLAUDE_TEAM_STATE_FILE", tmp_path / "claude_team_state.json")
    monkeypatch.setattr(cqa.q100, "PLAN_FILE", tmp_path / "quant_100_plan.json")
    monkeypatch.setattr(cqa.q100, "RESULTS_FILE", tmp_path / "quant_100_results.json")
    monkeypatch.setattr(cqa.q100, "build_100_experiments", lambda priority_pairs=None: [{"id": "x"}])
    monkeypatch.setattr(cqa.q100, "run_experiments", lambda *_args, **_kwargs: [{"status": "no_data"}])
    monkeypatch.setattr(cqa.q100, "summarize", lambda _rows: {"total": 1, "promoted_warm": 0, "rejected_cold": 0, "no_data": 1})

    agent = cqa.ClaudeQuantAgent()
    agent.run_once()

    msg_types = [row["msg_type"] for row in dummy.sent]
    assert "work_ack" in msg_types
    work_ack = next(row for row in dummy.sent if row["msg_type"] == "work_ack")
    assert work_ack["meta"]["task_ids"] == ["AF-014"]
