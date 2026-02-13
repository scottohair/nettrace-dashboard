#!/usr/bin/env python3
"""Build a deterministic 100-item growth improvement registry."""

import json
from datetime import datetime, timezone
from pathlib import Path

OUT = Path(__file__).parent / "growth_mode_100_improvements.json"

CATEGORIES = [
    "data_quality",
    "signal_engineering",
    "execution_quality",
    "portfolio_risk",
    "market_expansion",
    "infrastructure",
    "ml_research",
    "monitoring",
    "governance",
    "agent_collaboration",
]

CATEGORY_THEMES = {
    "data_quality": [
        "candle gap repair", "outlier clipping", "cross-venue median price", "timestamp normalization",
        "coverage SLA enforcement", "data freshness alarms", "schema drift detection", "feature null guards",
        "synthetic OHLC reconstruction", "pair-level quality scoring",
    ],
    "signal_engineering": [
        "regime-conditioned thresholds", "volatility-normalized entries", "entropy-based confidence", "multi-horizon confluence",
        "microstructure filter", "trend persistence score", "mean-reversion half-life", "adaptive stop distance",
        "drawdown-aware throttling", "signal decay model",
    ],
    "execution_quality": [
        "maker-first routing", "venue fee optimizer", "slippage predictor", "partial fill balancer",
        "latency-aware order timing", "quote staleness guard", "smart cancel/replace", "cross-venue arb sanity checks",
        "fill-quality attribution", "execution replay harness",
    ],
    "portfolio_risk": [
        "correlation cap", "sector exposure budget", "tail-risk overlay", "volatility targeting",
        "dynamic leverage ceiling", "per-strategy kill switch", "intraday drawdown governor", "scenario stress limits",
        "liquidity-weighted sizing", "capital utilization floor",
    ],
    "market_expansion": [
        "crypto majors expansion", "forex majors adapter", "metals synthetic basket", "energy proxy routing",
        "cross-asset confirmation", "session-aware activation", "regional liquidity map", "instrument onboarding checklist",
        "rollover/event calendar filters", "basis spread opportunities",
    ],
    "infrastructure": [
        "Apple MLX acceleration", "MPS inference batching", "CUDA node scheduler", "feature cache tiering",
        "distributed backtest queue", "checkpointed simulations", "fault-tolerant workers", "idempotent task execution",
        "resource autoscaling policy", "latency budget SLO",
    ],
    "ml_research": [
        "probabilistic return model", "Bayesian uncertainty bounds", "meta-labeling", "regime classifier ensemble",
        "online feature selection", "concept drift detector", "monte carlo bootstrap variants", "synthetic scenario generator",
        "model risk scoring", "counterfactual trade analysis",
    ],
    "monitoring": [
        "PnL attribution dashboard", "real-time risk heatmap", "pipeline stage telemetry", "anomaly alert routing",
        "promotion audit trail", "latency/error SLO board", "budget escalation timeline", "strategy health index",
        "market coverage panel", "incident postmortem template",
    ],
    "governance": [
        "change approval gates", "rollback playbook", "model version pinning", "compliance logging",
        "pre-trade policy checks", "post-trade reconciliation", "risk sign-off workflow", "secrets rotation",
        "dependency vulnerability scan", "kill-event drill",
    ],
    "agent_collaboration": [
        "Codex-Claude objective contracts", "duplex message schema", "artifact handoff validation", "prompt regression tests",
        "agent role specialization", "conflict resolution rules", "evidence-first directives", "feedback learning loop",
        "shared metric ontology", "cycle-level retrospectives",
    ],
}

METRIC_TARGETS = [
    ("promoted_warm_rate", ">= 20%"),
    ("rejected_false_negative_rate", "<= 10%"),
    ("walkforward_oos_return_pct", ">= 0.05%"),
    ("monte_carlo_p05_return_pct", ">= -0.10%"),
    ("max_drawdown_pct", "<= 3.0%"),
    ("execution_slippage_bps", "<= 20"),
    ("data_coverage_ratio", ">= 0.98"),
    ("latency_p95_ms", "<= 250"),
    ("daily_loss_events", "= 0"),
    ("risk_budget_adherence", ">= 99%"),
]


def build_items():
    items = []
    idx = 1
    for category in CATEGORIES:
        themes = CATEGORY_THEMES[category]
        for n, theme in enumerate(themes, start=1):
            metric, target = METRIC_TARGETS[(idx - 1) % len(METRIC_TARGETS)]
            items.append(
                {
                    "id": f"IMPR-{idx:03d}",
                    "category": category,
                    "title": f"{theme} optimization",
                    "hypothesis": f"Improving {theme} will increase risk-adjusted profitability under strict growth mode.",
                    "metric": metric,
                    "target": target,
                    "acceptance_test": "Must pass backtest + walk-forward + monte carlo + risk governor before promotion.",
                    "priority": "high" if n <= 3 else ("medium" if n <= 7 else "low"),
                    "status": "planned",
                    "owner": "codex",
                }
            )
            idx += 1

    # If category templates change, enforce exactly 100 items.
    if len(items) != 100:
        raise RuntimeError(f"expected 100 items, got {len(items)}")
    return items


def main():
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "program": "strict_growth_mode_100_improvements",
        "total_items": 100,
        "items": build_items(),
    }
    OUT.write_text(json.dumps(payload, indent=2))
    print(str(OUT))


if __name__ == "__main__":
    main()
