#!/usr/bin/env python3
"""Tests for realized-close evidence aggregation in quant_company_agent."""

import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import quant_company_agent as qca  # noqa: E402


def _ts(hours_ago):
    dt = datetime.now(timezone.utc) - timedelta(hours=float(hours_ago))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _init_trader_db(path: Path):
    con = sqlite3.connect(path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT,
            pair TEXT,
            side TEXT,
            price REAL,
            quantity REAL,
            total_usd REAL,
            order_type TEXT,
            order_id TEXT,
            status TEXT,
            pnl REAL,
            created_at TEXT
        )
        """
    )
    con.commit()
    return con


def _init_exit_db(path: Path):
    con = sqlite3.connect(path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS exit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT,
            exit_type TEXT,
            reason TEXT,
            entry_price REAL,
            exit_price REAL,
            amount REAL,
            pnl_usd REAL,
            pnl_pct REAL,
            hold_duration_hours REAL,
            peak_price REAL,
            volatility REAL,
            created_at TEXT
        )
        """
    )
    con.commit()
    return con


def test_realized_evidence_uses_exit_manager_fallback_when_trader_is_test_only(monkeypatch, tmp_path):
    trader_db = tmp_path / "trader.db"
    exit_db = tmp_path / "exit_manager.db"
    tcon = _init_trader_db(trader_db)
    econ = _init_exit_db(exit_db)
    try:
        # Synthetic trader row should be filtered from realized gate evidence.
        tcon.execute(
            """
            INSERT INTO agent_trades (agent, pair, side, price, quantity, total_usd, order_id, status, pnl, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("exit_manager", "BTC-USDC", "SELL", 101000.0, 0.0003, 30.3, "test123", "filled", 0.30, _ts(1)),
        )
        tcon.commit()

        # Three positive windows in exit_manager evidence.
        for h, pnl in ((1, 0.8), (5, 0.9), (9, 0.7)):
            econ.execute(
                """
                INSERT INTO exit_events (pair, exit_type, reason, entry_price, exit_price, amount, pnl_usd, pnl_pct, hold_duration_hours, peak_price, volatility, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("ETH-USDC", "scale_out", "tp", 100.0, 101.0, 1.0, pnl, 0.01, 1.0, 101.0, 0.1, _ts(h)),
            )
        econ.commit()
    finally:
        tcon.close()
        econ.close()

    monkeypatch.setattr(qca, "TRADER_DB", trader_db)
    monkeypatch.setattr(qca, "EXIT_MANAGER_DB", exit_db)
    monkeypatch.setattr(qca, "REALIZED_LOOKBACK_HOURS", 24)
    monkeypatch.setattr(qca, "REALIZED_WINDOW_HOURS", 4)
    monkeypatch.setattr(qca, "REALIZED_MIN_POSITIVE_WINDOWS", 3)
    monkeypatch.setattr(qca, "REALIZED_MIN_CLOSES_PER_WINDOW", 1)
    monkeypatch.setattr(qca, "REALIZED_MIN_NET_PNL_USD", 0.01)

    evidence = qca._realized_close_evidence()
    assert evidence["passed"] is True
    assert evidence["source"] == "exit_manager.db.exit_events"
    assert evidence["positive_windows"] >= 3
    assert evidence["total_net_pnl_usd"] > 0


def test_realized_evidence_prefers_trader_db_when_valid(monkeypatch, tmp_path):
    trader_db = tmp_path / "trader.db"
    exit_db = tmp_path / "exit_manager.db"
    tcon = _init_trader_db(trader_db)
    econ = _init_exit_db(exit_db)
    try:
        for h, pnl in ((1, 0.4), (5, 0.5), (9, 0.6)):
            tcon.execute(
                """
                INSERT INTO agent_trades (agent, pair, side, price, quantity, total_usd, order_id, status, pnl, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("exit_manager", "SOL-USDC", "SELL", 80.0, 0.1, 8.0, f"real-{h}", "filled", pnl, _ts(h)),
            )
        tcon.commit()
    finally:
        tcon.close()
        econ.close()

    monkeypatch.setattr(qca, "TRADER_DB", trader_db)
    monkeypatch.setattr(qca, "EXIT_MANAGER_DB", exit_db)
    monkeypatch.setattr(qca, "REALIZED_LOOKBACK_HOURS", 24)
    monkeypatch.setattr(qca, "REALIZED_WINDOW_HOURS", 4)
    monkeypatch.setattr(qca, "REALIZED_MIN_POSITIVE_WINDOWS", 3)
    monkeypatch.setattr(qca, "REALIZED_MIN_CLOSES_PER_WINDOW", 1)
    monkeypatch.setattr(qca, "REALIZED_MIN_NET_PNL_USD", 0.01)

    evidence = qca._realized_close_evidence()
    assert evidence["passed"] is True
    assert evidence["source"] == "trader_db.agent_trades"
    assert evidence["positive_windows"] >= 3
