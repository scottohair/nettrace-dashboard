#!/usr/bin/env python3
"""
Deployment Pipeline - Orchestrates COLD → WARM → HOT → FLY promotion flow

Pipeline Stages:
1. COLD: New strategies, backtesting only
2. WARM: Paper trading with small capital ($2-10)
3. HOT: Live trading with full allocation
4. FLY: Auto-deployment to Fly.io for autonomous operation

Promotion Criteria:
- COLD → WARM: 20+ backtest trades, 58%+ WR, 0.3%+ avg return
- WARM → HOT: 10+ live trades, 65%+ WR, Sharpe > 1.0
- HOT → FLY: 20+ trades, 70%+ WR, Sharpe > 1.5, PnL > $50
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import json
import logging
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('deployment_pipeline')


class DeploymentPipeline:
    """
    Orchestrates strategy promotion through pipeline stages
    """

    def __init__(self):
        self.db_path = Path(__file__).parent.parent / 'data' / 'deployment_pipeline.db'
        self._init_db()

        # Import components
        self._load_components()

        logger.info('Deployment Pipeline initialized')

    def _init_db(self):
        """Initialize pipeline database"""

        self.db_path.parent.mkdir(exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT UNIQUE,
                name TEXT,
                current_stage TEXT,
                created_at TIMESTAMP,
                promoted_to_warm_at TIMESTAMP,
                promoted_to_hot_at TIMESTAMP,
                deployed_to_fly_at TIMESTAMP,
                killed_at TIMESTAMP,
                kill_reason TEXT,

                -- COLD stage metrics
                cold_backtest_trades INTEGER DEFAULT 0,
                cold_win_rate REAL DEFAULT 0,
                cold_sharpe REAL DEFAULT 0,
                cold_avg_return REAL DEFAULT 0,

                -- WARM stage metrics
                warm_trades INTEGER DEFAULT 0,
                warm_win_rate REAL DEFAULT 0,
                warm_sharpe REAL DEFAULT 0,
                warm_total_pnl REAL DEFAULT 0,

                -- HOT stage metrics
                hot_trades INTEGER DEFAULT 0,
                hot_win_rate REAL DEFAULT 0,
                hot_sharpe REAL DEFAULT 0,
                hot_total_pnl REAL DEFAULT 0,

                -- FLY stage metrics
                fly_app_name TEXT,
                fly_regions TEXT,
                fly_status TEXT,
                fly_trades INTEGER DEFAULT 0,
                fly_total_pnl REAL DEFAULT 0
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_promotions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT,
                from_stage TEXT,
                to_stage TEXT,
                timestamp TIMESTAMP,
                metrics TEXT,
                decision_reason TEXT
            )
        ''')

        conn.commit()
        conn.close()

    def _load_components(self):
        """Load pipeline components"""

        try:
            from strategy_pipeline import StrategyPipeline
            self.strategy_pipeline = StrategyPipeline()
        except Exception as e:
            logger.warning(f'Could not load strategy_pipeline: {e}')
            self.strategy_pipeline = None

        try:
            from self_deployer import SelfDeployingAgent
            self.SelfDeployingAgent = SelfDeployingAgent
        except Exception as e:
            logger.warning(f'Could not load self_deployer: {e}')
            self.SelfDeployingAgent = None

    def add_strategy(self, strategy_id: str, name: str = None):
        """
        Add new strategy to pipeline (starts in COLD stage)

        Args:
            strategy_id: Unique strategy identifier
            name: Optional human-readable name
        """

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        try:
            c.execute('''
                INSERT INTO pipeline_strategies (strategy_id, name, current_stage, created_at)
                VALUES (?, ?, 'COLD', ?)
            ''', (strategy_id, name or strategy_id, datetime.utcnow()))

            conn.commit()
            logger.info(f'Added {strategy_id} to pipeline (COLD stage)')

        except sqlite3.IntegrityError:
            logger.warning(f'Strategy {strategy_id} already in pipeline')

        conn.close()

    def update_cold_metrics(self, strategy_id: str, backtest_results: Dict):
        """Update COLD stage metrics from backtest"""

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            UPDATE pipeline_strategies
            SET cold_backtest_trades = ?,
                cold_win_rate = ?,
                cold_sharpe = ?,
                cold_avg_return = ?
            WHERE strategy_id = ?
        ''', (
            backtest_results.get('total_trades', 0),
            backtest_results.get('win_rate', 0),
            backtest_results.get('sharpe', 0),
            backtest_results.get('avg_return', 0),
            strategy_id
        ))

        conn.commit()
        conn.close()

    def update_warm_metrics(self, strategy_id: str, trade_results: Dict):
        """Update WARM stage metrics from live trades"""

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            UPDATE pipeline_strategies
            SET warm_trades = ?,
                warm_win_rate = ?,
                warm_sharpe = ?,
                warm_total_pnl = ?
            WHERE strategy_id = ?
        ''', (
            trade_results.get('total_trades', 0),
            trade_results.get('win_rate', 0),
            trade_results.get('sharpe', 0),
            trade_results.get('total_pnl', 0),
            strategy_id
        ))

        conn.commit()
        conn.close()

    def update_hot_metrics(self, strategy_id: str, trade_results: Dict):
        """Update HOT stage metrics"""

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            UPDATE pipeline_strategies
            SET hot_trades = ?,
                hot_win_rate = ?,
                hot_sharpe = ?,
                hot_total_pnl = ?
            WHERE strategy_id = ?
        ''', (
            trade_results.get('total_trades', 0),
            trade_results.get('win_rate', 0),
            trade_results.get('sharpe', 0),
            trade_results.get('total_pnl', 0),
            strategy_id
        ))

        conn.commit()
        conn.close()

    def check_cold_to_warm(self, strategy_id: str) -> bool:
        """
        Check if strategy should be promoted COLD → WARM

        Criteria:
        - 20+ backtest trades
        - 58%+ win rate
        - 0.3%+ average return

        Returns:
            bool: True if should promote
        """

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            SELECT cold_backtest_trades, cold_win_rate, cold_avg_return, current_stage
            FROM pipeline_strategies
            WHERE strategy_id = ?
        ''', (strategy_id,))

        row = c.fetchone()
        conn.close()

        if not row:
            return False

        trades, win_rate, avg_return, stage = row

        if stage != 'COLD':
            return False

        if trades >= 20 and win_rate >= 0.58 and avg_return >= 0.003:
            logger.info(f'{strategy_id}: ✅ COLD → WARM criteria met (trades={trades}, WR={win_rate:.1%}, return={avg_return:.2%})')
            return True

        return False

    def check_warm_to_hot(self, strategy_id: str) -> bool:
        """
        Check if strategy should be promoted WARM → HOT

        Criteria:
        - 10+ live trades
        - 65%+ win rate
        - Sharpe > 1.0

        Returns:
            bool: True if should promote
        """

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            SELECT warm_trades, warm_win_rate, warm_sharpe, current_stage
            FROM pipeline_strategies
            WHERE strategy_id = ?
        ''', (strategy_id,))

        row = c.fetchone()
        conn.close()

        if not row:
            return False

        trades, win_rate, sharpe, stage = row

        if stage != 'WARM':
            return False

        if trades >= 10 and win_rate >= 0.65 and sharpe >= 1.0:
            logger.info(f'{strategy_id}: ✅ WARM → HOT criteria met (trades={trades}, WR={win_rate:.1%}, Sharpe={sharpe:.2f})')
            return True

        return False

    def check_hot_to_fly(self, strategy_id: str) -> bool:
        """
        Check if strategy should be deployed HOT → FLY

        Criteria:
        - 20+ trades
        - 70%+ win rate
        - Sharpe > 1.5
        - Total PnL > $50

        Returns:
            bool: True if should deploy
        """

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            SELECT hot_trades, hot_win_rate, hot_sharpe, hot_total_pnl, current_stage
            FROM pipeline_strategies
            WHERE strategy_id = ?
        ''', (strategy_id,))

        row = c.fetchone()
        conn.close()

        if not row:
            return False

        trades, win_rate, sharpe, pnl, stage = row

        if stage != 'HOT':
            return False

        if trades >= 20 and win_rate >= 0.70 and sharpe >= 1.5 and pnl >= 50:
            logger.info(f'{strategy_id}: ✅ HOT → FLY criteria met (trades={trades}, WR={win_rate:.1%}, Sharpe={sharpe:.2f}, PnL=${pnl:.2f})')
            return True

        return False

    def promote(self, strategy_id: str, from_stage: str, to_stage: str, metrics: Dict = None):
        """
        Promote strategy to next stage

        Args:
            strategy_id: Strategy to promote
            from_stage: Current stage
            to_stage: Target stage
            metrics: Optional metrics snapshot
        """

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # Update stage
        timestamp_field = f'promoted_to_{to_stage.lower()}_at'

        c.execute(f'''
            UPDATE pipeline_strategies
            SET current_stage = ?,
                {timestamp_field} = ?
            WHERE strategy_id = ? AND current_stage = ?
        ''', (to_stage, datetime.utcnow(), strategy_id, from_stage))

        if c.rowcount == 0:
            logger.warning(f'Failed to promote {strategy_id}: not in {from_stage} stage')
            conn.close()
            return

        # Log promotion
        c.execute('''
            INSERT INTO pipeline_promotions (strategy_id, from_stage, to_stage, timestamp, metrics, decision_reason)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            strategy_id,
            from_stage,
            to_stage,
            datetime.utcnow(),
            json.dumps(metrics) if metrics else None,
            f'Automatic promotion based on performance'
        ))

        conn.commit()
        conn.close()

        logger.info(f'✅ PROMOTED: {strategy_id} from {from_stage} → {to_stage}')

    def deploy_to_fly(self, strategy_id: str):
        """
        Deploy strategy to Fly.io

        Args:
            strategy_id: Strategy to deploy
        """

        if not self.SelfDeployingAgent:
            logger.error('SelfDeployingAgent not available')
            return False

        logger.info(f'Deploying {strategy_id} to Fly.io...')

        agent = self.SelfDeployingAgent(strategy_id)

        # Set performance metrics from HOT stage
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            SELECT hot_trades, hot_win_rate, hot_sharpe, hot_total_pnl
            FROM pipeline_strategies
            WHERE strategy_id = ?
        ''', (strategy_id,))
        row = c.fetchone()
        conn.close()

        if row:
            agent.trades = row[0]
            agent.wins = int(row[0] * row[1])
            agent.sharpe_ratio = row[2]
            agent.total_pnl = row[3]

        # Deploy
        success = agent.deploy_to_fly()

        if success:
            # Update database
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('''
                UPDATE pipeline_strategies
                SET current_stage = 'FLY',
                    deployed_to_fly_at = ?,
                    fly_app_name = ?,
                    fly_regions = ?,
                    fly_status = 'deployed'
                WHERE strategy_id = ?
            ''', (datetime.utcnow(), agent.fly_app_name, agent.primary_region, strategy_id))
            conn.commit()
            conn.close()

            logger.info(f'✅ DEPLOYED: {strategy_id} to Fly.io')
            return True
        else:
            logger.error(f'❌ Deployment failed for {strategy_id}')
            return False

    def run_promotion_cycle(self):
        """
        Run one promotion cycle: check all strategies for promotion

        Returns:
            dict: Promotion statistics
        """

        logger.info('Running promotion cycle...')

        promotions = {
            'cold_to_warm': 0,
            'warm_to_hot': 0,
            'hot_to_fly': 0
        }

        # Get all strategies
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # Check COLD → WARM
        c.execute('SELECT strategy_id FROM pipeline_strategies WHERE current_stage = "COLD"')
        for row in c.fetchall():
            strategy_id = row[0]
            if self.check_cold_to_warm(strategy_id):
                self.promote(strategy_id, 'COLD', 'WARM')
                promotions['cold_to_warm'] += 1

        # Check WARM → HOT
        c.execute('SELECT strategy_id FROM pipeline_strategies WHERE current_stage = "WARM"')
        for row in c.fetchall():
            strategy_id = row[0]
            if self.check_warm_to_hot(strategy_id):
                self.promote(strategy_id, 'WARM', 'HOT')
                promotions['warm_to_hot'] += 1

        # Check HOT → FLY
        c.execute('SELECT strategy_id FROM pipeline_strategies WHERE current_stage = "HOT"')
        for row in c.fetchall():
            strategy_id = row[0]
            if self.check_hot_to_fly(strategy_id):
                self.deploy_to_fly(strategy_id)
                promotions['hot_to_fly'] += 1

        conn.close()

        logger.info(f'Promotion cycle complete: {promotions}')

        return promotions

    def get_pipeline_stats(self):
        """Get pipeline statistics"""

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        stats = {}

        for stage in ['COLD', 'WARM', 'HOT', 'FLY']:
            c.execute('SELECT COUNT(*) FROM pipeline_strategies WHERE current_stage = ?', (stage,))
            stats[stage] = c.fetchone()[0]

        c.execute('SELECT COUNT(*) FROM pipeline_promotions')
        stats['total_promotions'] = c.fetchone()[0]

        conn.close()

        return stats


if __name__ == '__main__':
    print('🚀 Deployment Pipeline - EPOCH 2')
    print('='*70)

    pipeline = DeploymentPipeline()

    # Add test strategies
    print('\n📝 Adding test strategies to pipeline...')

    pipeline.add_strategy('momentum_test_1', 'Momentum Test 1')
    pipeline.add_strategy('mean_reversion_test_2', 'Mean Reversion Test 2')

    # Simulate COLD stage backtest results
    print('\n📊 Simulating backtest results...')

    pipeline.update_cold_metrics('momentum_test_1', {
        'total_trades': 25,
        'win_rate': 0.64,
        'sharpe': 1.8,
        'avg_return': 0.005
    })

    pipeline.update_cold_metrics('mean_reversion_test_2', {
        'total_trades': 15,
        'win_rate': 0.53,
        'sharpe': 0.9,
        'avg_return': 0.002
    })

    # Run promotion cycle
    print('\n🔄 Running promotion cycle...')
    promotions = pipeline.run_promotion_cycle()

    print(f'\n📈 Promotions:')
    print(f'   COLD → WARM: {promotions["cold_to_warm"]}')
    print(f'   WARM → HOT: {promotions["warm_to_hot"]}')
    print(f'   HOT → FLY: {promotions["hot_to_fly"]}')

    # Show stats
    stats = pipeline.get_pipeline_stats()

    print(f'\n📊 Pipeline Stats:')
    print(f'   COLD: {stats["COLD"]} strategies')
    print(f'   WARM: {stats["WARM"]} strategies')
    print(f'   HOT: {stats["HOT"]} strategies')
    print(f'   FLY: {stats["FLY"]} strategies')
    print(f'   Total promotions: {stats["total_promotions"]}')

    print('\n✅ Deployment pipeline ready')
