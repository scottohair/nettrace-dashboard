#!/usr/bin/env python3
"""Autonomous Strategy Discovery Agent for NetTrace.

Continuously monitors external sources for alpha opportunities:
  - ArXiv: Quant finance papers (q-fin.TR, q-fin.CP, stat.ML)
  - Twitter/Reddit: Social alpha discussions
  - DeFi Pulse: Protocol launches, gas prices
  - SEC EDGAR: Regulatory changes
  - Network Upgrades: MEV opportunities

Scores and ranks opportunities, generates code, and submits to validation pipeline.

Usage:
  python strategy_discovery_agent.py --mode discover
  python strategy_discovery_agent.py --mode opportunities
  python strategy_discovery_agent.py --mode score --opportunity arxiv_momentum_2026_0214
"""

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from enum import Enum
import time
import random

logger = logging.getLogger("strategy_discovery_agent")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

BASE = Path(__file__).parent
OPPORTUNITIES_DB = BASE / "strategy_opportunities.db"
OPPORTUNITIES_LOG = BASE / "strategy_opportunities.jsonl"


class OpportunitySource(Enum):
    """Sources for alpha discovery."""
    ARXIV = "arxiv"
    TWITTER = "twitter"
    REDDIT = "reddit"
    DEFI_PULSE = "defi_pulse"
    SEC_EDGAR = "sec_edgar"
    NETWORK_UPGRADE = "network_upgrade"
    MANUAL = "manual"


class OpportunityStatus(Enum):
    """Status of discovered opportunities."""
    DISCOVERED = "discovered"
    SCORED = "scored"
    SUBMITTED = "submitted"
    IN_VALIDATION = "in_validation"
    APPROVED = "approved"
    REJECTED = "rejected"
    IMPLEMENTED = "implemented"


class StrategyDiscoveryAgent:
    """Discovers and scores alpha opportunities from external sources."""

    # Scoring weights
    SCORING_WEIGHTS = {
        "novelty": 0.25,           # How new/unique is the idea
        "market_fit": 0.25,        # Does it fit current market conditions
        "implementation_complexity": 0.15,  # How hard to implement (lower = better)
        "capital_efficiency": 0.15, # How much capital needed
        "risk_profile": 0.20,      # Risk/reward ratio
    }

    # Minimum opportunity score for submission
    MIN_SUBMISSION_SCORE = 0.65

    def __init__(self):
        self._init_db()

    def _init_db(self) -> None:
        """Initialize opportunities database."""
        self.db = sqlite3.connect(str(OPPORTUNITIES_DB))
        self.db.row_factory = sqlite3.Row

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS strategy_opportunities (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                alpha_hypothesis TEXT,
                market_conditions TEXT,
                estimated_sharpe REAL,
                implementation_complexity INTEGER,  -- 1-10 scale
                capital_efficiency REAL,             -- % of portfolio needed
                risk_reward_ratio REAL,
                novelty_score REAL,
                market_fit_score REAL,
                composite_score REAL,
                status TEXT,
                discovered_at TEXT,
                submitted_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.db.execute("""
            CREATE TABLE IF NOT EXISTS discovery_sources (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL UNIQUE,
                last_checked TEXT,
                check_interval_minutes INTEGER,
                enabled BOOLEAN,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.db.commit()

        # Initialize discovery sources
        self._init_sources()

    def _init_sources(self) -> None:
        """Initialize discovery sources."""
        sources = [
            (OpportunitySource.ARXIV.value, 1440),        # Check every 24h
            (OpportunitySource.TWITTER.value, 60),        # Check every hour
            (OpportunitySource.REDDIT.value, 120),        # Check every 2 hours
            (OpportunitySource.DEFI_PULSE.value, 60),     # Check every hour
            (OpportunitySource.SEC_EDGAR.value, 1440),    # Check every 24h
            (OpportunitySource.NETWORK_UPGRADE.value, 720), # Check every 12h
        ]

        for source_name, interval in sources:
            try:
                self.db.execute("""
                    INSERT OR IGNORE INTO discovery_sources (source, check_interval_minutes, enabled)
                    VALUES (?, ?, 1)
                """, (source_name, interval))
            except:
                pass

        self.db.commit()

    def discover_opportunities(self) -> List[Dict]:
        """Discover opportunities from all enabled sources."""

        logger.info("Starting strategy discovery cycle")
        opportunities = []

        for source in OpportunitySource:
            try:
                logger.info(f"Checking {source.value}...")

                if source == OpportunitySource.ARXIV:
                    opps = self._discover_from_arxiv()
                elif source == OpportunitySource.TWITTER:
                    opps = self._discover_from_twitter()
                elif source == OpportunitySource.REDDIT:
                    opps = self._discover_from_reddit()
                elif source == OpportunitySource.DEFI_PULSE:
                    opps = self._discover_from_defi_pulse()
                elif source == OpportunitySource.SEC_EDGAR:
                    opps = self._discover_from_sec_edgar()
                elif source == OpportunitySource.NETWORK_UPGRADE:
                    opps = self._discover_from_network_upgrades()
                else:
                    continue

                opportunities.extend(opps)

                # Update last_checked
                self.db.execute("""
                    UPDATE discovery_sources SET last_checked = ?
                    WHERE source = ?
                """, (datetime.now(timezone.utc).isoformat(), source.value))

            except Exception as e:
                logger.warning(f"Discovery from {source.value} failed: {e}")

        self.db.commit()

        logger.info(f"Found {len(opportunities)} opportunities")
        return opportunities

    def _discover_from_arxiv(self) -> List[Dict]:
        """Discover from ArXiv quant papers."""
        # Mock implementation
        opportunities = []

        papers = [
            {
                "title": "Momentum Clustering in Cryptocurrency Markets",
                "alpha_hypothesis": "Buy on positive momentum clusters, exit on reversal",
                "market_conditions": "BULL/SIDEWAYS trending markets",
                "estimated_sharpe": 1.2,
            },
            {
                "title": "Mean Reversion in Micro-Cap Tokens",
                "alpha_hypothesis": "Sell when RSI > 70, buy when RSI < 30",
                "market_conditions": "SIDEWAYS/BEAR reverting markets",
                "estimated_sharpe": 0.9,
            },
        ]

        for i, paper in enumerate(papers):
            opp_id = f"arxiv_paper_{int(time.time() * 1000)}_{i}"
            opportunities.append({
                "id": opp_id,
                "source": OpportunitySource.ARXIV.value,
                "title": paper["title"],
                "alpha_hypothesis": paper["alpha_hypothesis"],
                "market_conditions": paper["market_conditions"],
                "estimated_sharpe": paper["estimated_sharpe"],
                "implementation_complexity": random.randint(3, 7),
                "capital_efficiency": random.uniform(0.05, 0.20),
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            })

        return opportunities

    def _discover_from_twitter(self) -> List[Dict]:
        """Discover from Twitter alpha discussions."""
        # Mock implementation
        return [{
            "id": f"twitter_alpha_{int(time.time() * 1000)}",
            "source": OpportunitySource.TWITTER.value,
            "title": "Latency Arbitrage on Layer-2 Rollups",
            "alpha_hypothesis": "MEV extraction using private mempools",
            "market_conditions": "All market conditions",
            "estimated_sharpe": 1.5,
            "implementation_complexity": 8,
            "capital_efficiency": 0.10,
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        }]

    def _discover_from_reddit(self) -> List[Dict]:
        """Discover from Reddit trading communities."""
        # Mock implementation
        return []

    def _discover_from_defi_pulse(self) -> List[Dict]:
        """Discover from DeFi Pulse protocol launches."""
        # Mock implementation
        return [{
            "id": f"defi_pulse_launch_{int(time.time() * 1000)}",
            "source": OpportunitySource.DEFI_PULSE.value,
            "title": "New Lending Protocol Liquidity Mining",
            "alpha_hypothesis": "Front-run liquidity mining launches",
            "market_conditions": "Requires capital on-chain",
            "estimated_sharpe": 1.1,
            "implementation_complexity": 6,
            "capital_efficiency": 0.25,
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        }]

    def _discover_from_sec_edgar(self) -> List[Dict]:
        """Discover from SEC EDGAR regulatory events."""
        # Mock implementation
        return []

    def _discover_from_network_upgrades(self) -> List[Dict]:
        """Discover from blockchain network upgrades."""
        # Mock implementation
        return []

    def score_opportunity(self, opportunity_id: str) -> Dict:
        """Score an opportunity across multiple dimensions."""

        logger.info(f"Scoring opportunity {opportunity_id}")

        cursor = self.db.execute(
            "SELECT * FROM strategy_opportunities WHERE id = ?",
            (opportunity_id,)
        )
        row = cursor.fetchone()

        if not row:
            logger.error(f"Opportunity {opportunity_id} not found")
            return {"error": "Not found"}

        opp = dict(row)

        # Score each dimension
        scores = {
            "novelty": self._score_novelty(opp),
            "market_fit": self._score_market_fit(opp),
            "implementation_complexity": self._score_complexity(opp),
            "capital_efficiency": self._score_capital_efficiency(opp),
            "risk_profile": self._score_risk(opp),
        }

        # Composite score (weighted average)
        composite = sum(
            scores.get(k, 0) * v
            for k, v in self.SCORING_WEIGHTS.items()
        )

        # Update DB
        self.db.execute("""
            UPDATE strategy_opportunities
            SET novelty_score = ?, market_fit_score = ?, composite_score = ?, status = ?
            WHERE id = ?
        """, (
            scores["novelty"],
            scores["market_fit"],
            composite,
            OpportunityStatus.SCORED.value,
            opportunity_id,
        ))
        self.db.commit()

        logger.info(f"Opportunity {opportunity_id} scored: {composite:.2f}")

        return {
            "opportunity_id": opportunity_id,
            "scores": scores,
            "composite_score": composite,
            "recommendation": "SUBMIT" if composite >= self.MIN_SUBMISSION_SCORE else "HOLD",
        }

    def _score_novelty(self, opp: Dict) -> float:
        """Score novelty of the strategy idea."""
        # Check if similar strategies exist in DB
        cursor = self.db.execute(
            "SELECT COUNT(*) as count FROM strategy_opportunities WHERE status != 'rejected'"
        )
        count = cursor.fetchone()["count"]

        # Arbitrary scoring based on implementation complexity
        # (more complex = more novel)
        return min(opp.get("implementation_complexity", 5) / 10.0, 1.0)

    def _score_market_fit(self, opp: Dict) -> float:
        """Score fit for current market conditions."""
        # Check current regime
        try:
            from regime_detector import RegimeDetector
            detector = RegimeDetector()
            regime = detector.get_current_regime()

            # Simple: if strategy mentions regime, score higher
            market_conditions = (opp.get("market_conditions") or "").upper()
            if regime in market_conditions:
                return 0.9
            elif "ALL" in market_conditions:
                return 0.7
            else:
                return 0.5
        except:
            return 0.6

    def _score_complexity(self, opp: Dict) -> float:
        """Score implementation complexity (lower = better)."""
        complexity = opp.get("implementation_complexity", 5)
        # Normalize: 10 complexity → 0.1 score, 1 complexity → 0.9 score
        return 1.0 - (complexity / 10.0)

    def _score_capital_efficiency(self, opp: Dict) -> float:
        """Score capital efficiency (lower allocation needed = higher score)."""
        efficiency = opp.get("capital_efficiency", 0.15)
        # Prefer strategies requiring less capital
        return 1.0 - min(efficiency, 0.5) * 2

    def _score_risk(self, opp: Dict) -> float:
        """Score risk/reward ratio."""
        sharpe = opp.get("estimated_sharpe", 0.5)
        # Sharpe 2.0 = max score, 0 = min score
        return min(sharpe / 2.0, 1.0)

    def get_high_scoring_opportunities(self, limit: int = 10) -> List[Dict]:
        """Get opportunities ready for code generation."""

        cursor = self.db.execute("""
            SELECT * FROM strategy_opportunities
            WHERE status IN (?, ?) AND composite_score >= ?
            ORDER BY composite_score DESC
            LIMIT ?
        """, (
            OpportunityStatus.SCORED.value,
            OpportunityStatus.DISCOVERED.value,
            self.MIN_SUBMISSION_SCORE,
            limit,
        ))

        return [dict(row) for row in cursor.fetchall()]

    def save_opportunity(self, opportunity: Dict) -> str:
        """Save discovered opportunity to DB."""

        opp_id = opportunity.get("id", f"opp_{int(time.time() * 1000)}")

        self.db.execute("""
            INSERT OR REPLACE INTO strategy_opportunities
            (id, source, title, description, alpha_hypothesis, market_conditions,
             estimated_sharpe, implementation_complexity, capital_efficiency, status, discovered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            opp_id,
            opportunity.get("source"),
            opportunity.get("title"),
            opportunity.get("description"),
            opportunity.get("alpha_hypothesis"),
            opportunity.get("market_conditions"),
            opportunity.get("estimated_sharpe", 0.5),
            opportunity.get("implementation_complexity", 5),
            opportunity.get("capital_efficiency", 0.15),
            OpportunityStatus.DISCOVERED.value,
            opportunity.get("discovered_at", datetime.now(timezone.utc).isoformat()),
        ))

        self.db.commit()

        # Log to JSONL
        with open(OPPORTUNITIES_LOG, "a") as f:
            f.write(json.dumps(opportunity) + "\n")

        logger.info(f"Saved opportunity {opp_id}")
        return opp_id

    def get_status(self) -> Dict:
        """Get discovery agent status."""

        cursor = self.db.execute("""
            SELECT status, COUNT(*) as count FROM strategy_opportunities
            GROUP BY status
        """)

        status_counts = {row["status"]: row["count"] for row in cursor.fetchall()}

        return {
            "discovery_agent_version": "1.0",
            "status_counts": status_counts,
            "total_opportunities": sum(status_counts.values()),
            "min_submission_score": self.MIN_SUBMISSION_SCORE,
            "scoring_weights": self.SCORING_WEIGHTS,
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Strategy Discovery Agent")
    parser.add_argument("--mode", choices=["discover", "score", "opportunities", "status"],
                        default="status")
    parser.add_argument("--opportunity", help="Opportunity ID to score")

    args = parser.parse_args()

    agent = StrategyDiscoveryAgent()

    if args.mode == "discover":
        opportunities = agent.discover_opportunities()
        for opp in opportunities:
            agent.save_opportunity(opp)
        print(json.dumps({"discovered": len(opportunities)}, indent=2))

    elif args.mode == "score":
        if not args.opportunity:
            print("❌ --opportunity required")
            return 1
        result = agent.score_opportunity(args.opportunity)
        print(json.dumps(result, indent=2, default=str))

    elif args.mode == "opportunities":
        opps = agent.get_high_scoring_opportunities(limit=10)
        print(json.dumps({"opportunities": opps}, indent=2, default=str))

    elif args.mode == "status":
        status = agent.get_status()
        print(json.dumps(status, indent=2, default=str))

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
