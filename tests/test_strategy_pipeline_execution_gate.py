#!/usr/bin/env python3
"""Tests for execution-health gating in strategy budget escalation."""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

import strategy_pipeline as sp  # noqa: E402


def test_execution_health_gate_blocks_budget_increase_on_hot_promotion(monkeypatch, tmp_path):
    monkeypatch.setattr(sp, "PIPELINE_DB", str(tmp_path / "pipeline.db"))
    monkeypatch.setattr(sp, "TRADER_DB", str(tmp_path / "trader.db"))
    monkeypatch.setattr(sp, "GROWTH_MODE_ENABLED", True)
    monkeypatch.setattr(sp, "REALIZED_ESCALATION_GATE_ENABLED", False)
    monkeypatch.setattr(sp, "REALIZED_CLOSE_REQUIRED_FOR_HOT_PROMOTION", False)
    monkeypatch.setattr(sp, "EXECUTION_HEALTH_ESCALATION_GATE", True)
    monkeypatch.setattr(sp, "EXECUTION_HEALTH_REQUIRED_FOR_HOT_PROMOTION", False)
    monkeypatch.setattr(sp, "WARM_WALKFORWARD_LEAKAGE_ENFORCE", False)

    validator = sp.StrategyValidator()
    strategy = sp.MomentumStrategy()
    pair = "BTC-USD"
    validator.register_strategy(strategy, pair)
    validator.db.execute(
        "UPDATE strategy_registry SET stage='WARM' WHERE name=? AND pair=?",
        (strategy.name, pair),
    )
    validator.db.commit()
    validator._set_budget_profile(
        strategy.name,
        pair,
        {
            "tier": "standard",
            "current_budget_usd": 1.0,
            "starter_budget_usd": 1.0,
            "max_budget_usd": 5.0,
        },
    )
    monkeypatch.setattr(
        validator,
        "_execution_health_gate_status",
        lambda refresh=False: {
            "enabled": True,
            "green": False,
            "reason": "dns_unhealthy",
            "updated_at": "2026-02-13T00:00:00+00:00",
        },
    )

    paper_metrics = {
        "total_trades": 15,
        "win_rate": 0.72,
        "total_return_pct": 0.35,
        "max_drawdown_pct": 1.2,
        "runtime_seconds": 7200,
        "sharpe_ratio": 1.05,
        "losses": 0,
    }
    promoted, _msg = validator.check_warm_promotion(strategy.name, pair, paper_metrics)
    assert promoted is True

    row = validator.db.execute(
        """
        SELECT details_json
        FROM pipeline_events
        WHERE strategy_name=? AND pair=? AND event_type='promoted_to_HOT'
        ORDER BY id DESC LIMIT 1
        """,
        (strategy.name, pair),
    ).fetchone()
    assert row is not None
    details = json.loads(row["details_json"])
    budget = details.get("budget_update", {})
    assert budget.get("action") == "hold"
    assert str(budget.get("reason", "")).startswith("execution_health_gate_failed")
    validator.db.close()


def test_execution_health_required_gate_blocks_hot_promotion(monkeypatch, tmp_path):
    monkeypatch.setattr(sp, "PIPELINE_DB", str(tmp_path / "pipeline.db"))
    monkeypatch.setattr(sp, "TRADER_DB", str(tmp_path / "trader.db"))
    monkeypatch.setattr(sp, "GROWTH_MODE_ENABLED", False)
    monkeypatch.setattr(sp, "REALIZED_ESCALATION_GATE_ENABLED", False)
    monkeypatch.setattr(sp, "REALIZED_CLOSE_REQUIRED_FOR_HOT_PROMOTION", False)
    monkeypatch.setattr(sp, "EXECUTION_HEALTH_ESCALATION_GATE", True)
    monkeypatch.setattr(sp, "EXECUTION_HEALTH_REQUIRED_FOR_HOT_PROMOTION", True)
    monkeypatch.setattr(sp, "WARM_WALKFORWARD_LEAKAGE_ENFORCE", False)

    validator = sp.StrategyValidator()
    strategy = sp.MomentumStrategy()
    pair = "BTC-USD"
    validator.register_strategy(strategy, pair)
    validator.db.execute(
        "UPDATE strategy_registry SET stage='WARM' WHERE name=? AND pair=?",
        (strategy.name, pair),
    )
    validator.db.commit()
    monkeypatch.setattr(
        validator,
        "_execution_health_gate_status",
        lambda refresh=False: {
            "enabled": True,
            "green": False,
            "reason": "dns_unhealthy",
            "updated_at": "2026-02-13T00:00:00+00:00",
        },
    )

    paper_metrics = {
        "total_trades": 15,
        "win_rate": 0.72,
        "total_return_pct": 0.35,
        "max_drawdown_pct": 1.2,
        "runtime_seconds": 7200,
        "sharpe_ratio": 1.05,
        "losses": 0,
    }
    promoted, msg = validator.check_warm_promotion(strategy.name, pair, paper_metrics)
    assert promoted is False
    assert "execution_health_gate_failed:dns_unhealthy" in msg
    validator.db.close()


def test_realized_close_required_gate_blocks_hot_promotion(monkeypatch, tmp_path):
    monkeypatch.setattr(sp, "PIPELINE_DB", str(tmp_path / "pipeline.db"))
    monkeypatch.setattr(sp, "TRADER_DB", str(tmp_path / "trader.db"))
    monkeypatch.setattr(sp, "GROWTH_MODE_ENABLED", False)
    monkeypatch.setattr(sp, "REALIZED_ESCALATION_GATE_ENABLED", True)
    monkeypatch.setattr(sp, "REALIZED_CLOSE_REQUIRED_FOR_HOT_PROMOTION", True)
    monkeypatch.setattr(sp, "EXECUTION_HEALTH_ESCALATION_GATE", False)
    monkeypatch.setattr(sp, "EXECUTION_HEALTH_REQUIRED_FOR_HOT_PROMOTION", False)
    monkeypatch.setattr(sp, "WARM_WALKFORWARD_LEAKAGE_ENFORCE", False)

    validator = sp.StrategyValidator()
    strategy = sp.MomentumStrategy()
    pair = "BTC-USD"
    validator.register_strategy(strategy, pair)
    validator.db.execute(
        "UPDATE strategy_registry SET stage='WARM' WHERE name=? AND pair=?",
        (strategy.name, pair),
    )
    validator.db.commit()
    monkeypatch.setattr(
        validator,
        "_pair_realized_close_evidence",
        lambda _pair, strategy_name=None: {
            "enabled": True,
            "pair": _pair,
            "strategy_name": strategy_name,
            "passed": False,
            "reason": "closed_trades 0 < 5",
        },
    )

    paper_metrics = {
        "total_trades": 15,
        "win_rate": 0.72,
        "total_return_pct": 0.35,
        "max_drawdown_pct": 1.2,
        "runtime_seconds": 7200,
        "sharpe_ratio": 1.05,
        "losses": 0,
    }
    promoted, msg = validator.check_warm_promotion(strategy.name, pair, paper_metrics)
    assert promoted is False
    assert "realized_close_gate_failed:closed_trades 0 < 5" in msg
    validator.db.close()
