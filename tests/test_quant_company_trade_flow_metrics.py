#!/usr/bin/env python3
"""Tests for quant_company trade-flow metrics and close-completion gating."""

import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import quant_company_agent as qca  # noqa: E402


def test_trade_flow_metrics_blocks_when_close_completion_rate_is_low(monkeypatch, tmp_path):
    trader_db = tmp_path / "trader.db"
    conn = sqlite3.connect(str(trader_db))
    conn.execute(
        """
        CREATE TABLE agent_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            side TEXT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.executemany(
        "INSERT INTO agent_trades (side, status) VALUES (?, ?)",
        [
            ("BUY", "filled"),
            ("BUY", "filled"),
            ("SELL", "filled"),
            ("SELL", "filled"),
        ],
    )
    conn.commit()
    conn.close()

    exit_db = tmp_path / "exit_manager.db"
    econ = sqlite3.connect(str(exit_db))
    econ.execute(
        """
        CREATE TABLE exit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    econ.commit()
    econ.close()

    reconcile_status = tmp_path / "reconcile_agent_trades_status.json"
    reconcile_status.write_text(
        json.dumps(
            {
                "summary": {"close_attempts": 4, "close_completions": 1},
                "close_reconciliation": {"attempts": 4, "completions": 1},
            }
        )
    )

    monkeypatch.setattr(qca, "TRADER_DB", trader_db)
    monkeypatch.setattr(qca, "EXIT_MANAGER_DB", exit_db)
    monkeypatch.setattr(qca, "RECONCILE_STATUS_FILE", reconcile_status)
    monkeypatch.setattr(qca, "TRADE_FLOW_MAX_BUY_SELL_RATIO", 2.5)
    monkeypatch.setattr(qca, "TRADE_FLOW_MIN_CLOSE_ATTEMPTS", 2)
    monkeypatch.setattr(qca, "TRADE_FLOW_MIN_CLOSE_COMPLETION_RATE", 0.60)

    metrics = qca._trade_flow_metrics(lookback_hours=6)
    assert metrics["buy_fills"] == 2
    assert metrics["sell_fills"] == 2
    assert metrics["sell_close_attempts"] == 4
    assert metrics["sell_close_completions"] == 1
    assert metrics["close_balance_ok"] is False
    assert str(metrics["close_balance_reason"]).startswith("close_completion_rate_low:")
