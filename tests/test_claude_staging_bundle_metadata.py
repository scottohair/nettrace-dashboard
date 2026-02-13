#!/usr/bin/env python3
"""Tests for Claude staging bundle metadata and sequence."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import claude_staging as cs  # noqa: E402


def test_build_ingest_bundle_emits_metadata_and_increments_sequence(monkeypatch, tmp_path):
    monkeypatch.setattr(cs, "STRATEGY_STAGE_FILE", tmp_path / "strategy_stage.json")
    monkeypatch.setattr(cs, "FRAMEWORK_STAGE_FILE", tmp_path / "framework_stage.json")
    monkeypatch.setattr(cs, "MESSAGE_STAGE_FILE", tmp_path / "message_stage.json")
    monkeypatch.setattr(cs, "BUNDLE_FILE", tmp_path / "bundle.json")
    monkeypatch.setattr(cs, "BUNDLE_SEQ_FILE", tmp_path / "bundle.seq")
    monkeypatch.setattr(cs, "OPERATOR_MESSAGES_FILE", tmp_path / "operator_messages.jsonl")
    monkeypatch.setattr(cs, "PRIORITY_CONFIG_FILE", tmp_path / "priority_config.json")
    monkeypatch.setattr(cs, "MCP_CURRICULUM_FILE", tmp_path / "mcp_curriculum.json")
    monkeypatch.setattr(cs, "PIPELINE_DB", tmp_path / "pipeline.db")
    monkeypatch.setattr(cs, "COMPUTE_POOL_DB", tmp_path / "compute_pool.db")
    monkeypatch.setattr(cs, "ADVANCED_BUS_FILE", tmp_path / "message_bus.jsonl")
    monkeypatch.setattr(cs, "QUANT100_RESULTS_FILE", tmp_path / "quant_100_results.json")
    monkeypatch.setattr(cs, "DASHBOARD_INSIGHTS_FILE", tmp_path / "dashboard_insights.json")

    b1 = cs.build_ingest_bundle(reason="test_one")
    b2 = cs.build_ingest_bundle(reason="test_two")

    assert b1["bundle_id"]
    assert b1["bundle_hash"]
    assert b1["bundle_sequence"] == 1
    assert b1["metadata"]["bundle_id"] == b1["bundle_id"]
    assert b1["metadata"]["bundle_hash"] == b1["bundle_hash"]
    assert b1["metadata"]["bundle_sequence"] == 1
    assert b2["bundle_sequence"] == 2
    assert b2["bundle_id"] != b1["bundle_id"]
