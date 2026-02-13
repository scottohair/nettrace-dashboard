#!/usr/bin/env python3
"""Tests for strict realized-close gating in rebalance_funded_budgets."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import rebalance_funded_budgets as rb  # noqa: E402


def test_realized_evidence_summary_strict_ok_when_positive():
    summary = rb._realized_evidence_summary(
        {
            "realized_close_evidence": {
                "passed": True,
                "closed_trades": 6,
                "winning_closes": 4,
                "net_pnl_usd": 2.5,
                "win_rate": 0.6667,
            }
        }
    )
    assert summary["strict_ok"] is True
    assert summary["losing_closes"] == 2


def test_realized_evidence_summary_blocks_non_positive():
    summary = rb._realized_evidence_summary(
        {
            "realized_close_evidence": {
                "passed": True,
                "closed_trades": 5,
                "winning_closes": 2,
                "net_pnl_usd": -0.01,
                "win_rate": 0.4,
            }
        }
    )
    assert summary["strict_ok"] is False
