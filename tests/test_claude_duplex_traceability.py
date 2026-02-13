#!/usr/bin/env python3
"""Tests for Claude duplex traceability metadata."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import claude_duplex as cd  # noqa: E402


def test_send_to_claude_includes_trace_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr(cd, "TO_CLAUDE_FILE", tmp_path / "to.jsonl")
    monkeypatch.setattr(cd, "FROM_CLAUDE_FILE", tmp_path / "from.jsonl")
    monkeypatch.setattr(cd, "COUNTER_FILE", tmp_path / "counter.txt")

    payload = cd.send_to_claude("run quant cycle", meta={"target_role": "opus"})
    assert payload["trace_id"]
    assert payload["schema_version"] == 1
    assert payload["status"] == "queued"
    assert payload["meta"]["trace_id"] == payload["trace_id"]
    assert payload["meta"]["schema_version"] == 1

    rows = cd.read_to_claude()
    assert len(rows) == 1
    assert rows[0]["trace_id"] == payload["trace_id"]


def test_read_to_claude_normalizes_legacy_rows(monkeypatch, tmp_path):
    monkeypatch.setattr(cd, "TO_CLAUDE_FILE", tmp_path / "to.jsonl")
    monkeypatch.setattr(cd, "FROM_CLAUDE_FILE", tmp_path / "from.jsonl")
    monkeypatch.setattr(cd, "COUNTER_FILE", tmp_path / "counter.txt")

    legacy_row = (
        '{"id":1,"timestamp":"2026-02-13T00:00:00+00:00","channel":"to_claude",'
        '"msg_type":"directive","priority":"normal","source":"operator","message":"legacy","meta":{}}\n'
    )
    cd.TO_CLAUDE_FILE.write_text(legacy_row)

    rows = cd.read_to_claude()
    assert len(rows) == 1
    assert rows[0]["trace_id"]
    assert rows[0]["meta"]["trace_id"] == rows[0]["trace_id"]
    assert rows[0]["schema_version"] == 1
    assert rows[0]["status"] == "queued"
