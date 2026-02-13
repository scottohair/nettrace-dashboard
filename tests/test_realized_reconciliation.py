#!/usr/bin/env python3
"""Tests for realized-close reconciliation and gating filters."""

import json
import os
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import agent_tools  # noqa: E402
import quant_company_agent as qca  # noqa: E402
import reconcile_agent_trades as rat  # noqa: E402


class _FakeExchange:
    def __init__(self, snapshots):
        self._snapshots = dict(snapshots)

    def _request(self, _method, path):
        order_id = str(path).rstrip("/").split("/")[-1]
        order = self._snapshots.get(order_id, {"status": "OPEN", "filled_size": "0", "filled_value": "0"})
        return {"order": dict(order)}


@pytest.fixture()
def temp_tools(monkeypatch, tmp_path):
    db_path = tmp_path / "trader.db"
    monkeypatch.setattr(agent_tools, "TRADER_DB", str(db_path))
    tools = agent_tools.AgentTools()
    _ = tools.db
    return tools


def test_reconcile_agent_trades_updates_pending_sell_to_filled(temp_tools, monkeypatch):
    monkeypatch.setenv("AGENT_SELL_FEE_RATE", "0.0")
    tools = temp_tools
    tools.exchange = _FakeExchange(
        {
            "ord-sell-1": {
                "status": "FILLED",
                "filled_size": "1.0",
                "filled_value": "100.0",
            }
        }
    )
    tools.db.execute(
        """
        INSERT INTO agent_trades (agent, pair, side, price, quantity, total_usd, order_type, order_id, status, pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("test_agent", "BTC-USD", "BUY", 90.0, 1.0, 90.0, "limit", "ord-buy-1", "filled", None),
    )
    tools.db.execute(
        """
        INSERT INTO agent_trades (agent, pair, side, price, quantity, total_usd, order_type, order_id, status, pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("test_agent", "BTC-USD", "SELL", 95.0, 1.0, 95.0, "limit", "ord-sell-1", "pending", None),
    )
    tools.db.commit()

    summary = tools.reconcile_agent_trades(max_orders=10, lookback_hours=48)
    row = tools.db.execute(
        "SELECT status, quantity, total_usd, pnl FROM agent_trades WHERE order_id=?",
        ("ord-sell-1",),
    ).fetchone()

    assert summary["checked"] >= 1
    assert summary["updated"] == 1
    assert summary["filled"] == 1
    assert row["status"] == "filled"
    assert float(row["quantity"]) == pytest.approx(1.0, abs=1e-12)
    assert float(row["total_usd"]) == pytest.approx(100.0, abs=1e-9)
    assert float(row["pnl"]) == pytest.approx(10.0, abs=1e-9)


def test_quant_company_realized_close_evidence_ignores_pending_status(monkeypatch, tmp_path):
    db_path = tmp_path / "trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE agent_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            side TEXT,
            status TEXT,
            pnl REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("INSERT INTO agent_trades (side, status, pnl) VALUES ('SELL', 'pending', 50.0)")
    conn.execute("INSERT INTO agent_trades (side, status, pnl) VALUES ('SELL', 'filled', 5.0)")
    conn.commit()
    conn.close()

    monkeypatch.setattr(qca, "TRADER_DB", db_path)
    monkeypatch.setattr(qca, "REALIZED_LOOKBACK_HOURS", 48)
    monkeypatch.setattr(qca, "REALIZED_WINDOW_HOURS", 1)
    monkeypatch.setattr(qca, "REALIZED_MIN_CLOSES_PER_WINDOW", 1)
    monkeypatch.setattr(qca, "REALIZED_MIN_POSITIVE_WINDOWS", 1)
    monkeypatch.setattr(qca, "REALIZED_MIN_NET_PNL_USD", 0.01)

    evidence = qca._realized_close_evidence()
    assert evidence["total_closes"] == 1
    assert evidence["total_net_pnl_usd"] == pytest.approx(5.0, abs=1e-9)
    assert evidence["passed"] is True


def test_reconcile_close_first_emits_close_failure_metrics(temp_tools):
    tools = temp_tools
    tools.exchange = _FakeExchange(
        {
            "ord-sell-open-1": {
                "status": "OPEN",
                "filled_size": "0",
                "filled_value": "0",
            }
        }
    )
    tools.db.execute(
        """
        INSERT INTO agent_trades (agent, pair, side, price, quantity, total_usd, order_type, order_id, status, pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("test_agent", "BTC-USD", "SELL", 101.0, 1.0, 101.0, "limit", "ord-sell-open-1", "pending", None),
    )
    tools.db.commit()

    summary = rat.reconcile_close_first(tools, max_orders=10, lookback_hours=48)

    assert summary["close_attempts"] == 1
    assert summary["close_completions"] == 0
    assert summary["close_failures"] == 1
    assert summary["close_failure_reasons"]["not_completed_pending"] == 1
    assert summary["close_gate_passed"] is False
    assert summary["close_gate_reason"].startswith("sell_close_completion_missing:")


def test_reconcile_status_json_contains_close_metrics(temp_tools, monkeypatch, tmp_path):
    monkeypatch.setenv("AGENT_SELL_FEE_RATE", "0.0")
    tools = temp_tools
    tools.exchange = _FakeExchange(
        {
            "ord-sell-2": {
                "status": "FILLED",
                "filled_size": "1.0",
                "filled_value": "100.0",
            }
        }
    )
    tools.db.execute(
        """
        INSERT INTO agent_trades (agent, pair, side, price, quantity, total_usd, order_type, order_id, status, pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("test_agent", "BTC-USD", "BUY", 90.0, 1.0, 90.0, "limit", "ord-buy-2", "filled", None),
    )
    tools.db.execute(
        """
        INSERT INTO agent_trades (agent, pair, side, price, quantity, total_usd, order_type, order_id, status, pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("test_agent", "BTC-USD", "SELL", 95.0, 1.0, 95.0, "limit", "ord-sell-2", "pending", None),
    )
    tools.db.commit()

    status_path = tmp_path / "reconcile_status.json"
    monkeypatch.setattr(rat, "AgentTools", lambda: tools)
    monkeypatch.setattr(
        rat.sys,
        "argv",
        [
            "reconcile_agent_trades.py",
            "--max-orders",
            "20",
            "--lookback-hours",
            "48",
            "--status-file",
            str(status_path),
        ],
    )

    rat.main()
    payload = json.loads(status_path.read_text())

    close_payload = payload["close_reconciliation"]
    assert close_payload["attempts"] == 1
    assert close_payload["completions"] == 1
    assert close_payload["failures"] == 0
    assert close_payload["gate_passed"] is True
    assert close_payload["gate_reason"] == "sell_close_completion_observed"
    assert payload["summary"]["close_attempts"] == 1
    assert payload["summary"]["close_completions"] == 1
