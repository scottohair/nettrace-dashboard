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
                {"id": 1, "message": "prioritize BTC-USD", "meta": {"trace_id": "trace-meta-1"}},
                {"id": 2, "message": "focus ETH-USD", "trace_id": "trace-row-2", "meta": {}},
            ]

    monkeypatch.setattr(cqa, "claude_duplex", _DummyDuplex())
    monkeypatch.setattr(cqa, "STATUS_FILE", tmp_path / "quant_status.json")

    agent = cqa.ClaudeQuantAgent()
    msgs, pairs, traces = agent._consume_duplex_directives()
    assert len(msgs) == 2
    assert pairs == ["BTC-USD", "ETH-USD"]
    assert traces == ["trace-meta-1", "trace-row-2"]
