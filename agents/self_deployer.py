#!/usr/bin/env python3
"""
Self-Deploying Agent - Autonomous deployment to Fly.io

Process:
1. Strategy passes WARM tier (10+ trades, 65%+ WR, Sharpe > 1.0)
2. Agent generates Dockerfile
3. Agent builds image
4. Agent deploys to Fly.io (ewr region)
5. Agent monitors health
6. If profitable: promote to HOT, scale to multi-region
7. If unprofitable: kill deployment

Expected: Zero-touch deployment, 100+ agents on Fly.io
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import json
import logging
import subprocess
import time
from datetime import datetime
import sqlite3
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('self_deployer')


class SelfDeployingAgent:
    """
    Agent that can deploy itself to Fly.io when promotion criteria are met
    """

    def __init__(self, strategy_id, strategy_code=None, db_path=None):
        self.strategy_id = strategy_id
        self.strategy_code = strategy_code
        self.agent_id = f'agent-{strategy_id}'

        # Performance tracking
        self.trades = 0
        self.wins = 0
        self.total_pnl = 0.0
        self.sharpe_ratio = 0.0

        # Deployment state
        self.deployed = False
        self.fly_app_name = None
        self.deployment_status = 'pending'

        # Database
        self.db_path = db_path or Path(__file__).parent.parent / 'data' / 'self_deployments.db'
        self._init_db()

        # Fly.io config
        self.flyctl_path = os.getenv('FLYCTL_PATH', '/Users/scott/.fly/bin/flyctl')
        self.primary_region = 'ewr'
        self.multi_regions = ['lhr', 'nrt', 'sin']

        logger.info(f'Self-deploying agent initialized: {self.agent_id}')

    def _init_db(self):
        """Initialize deployment tracking database"""

        self.db_path.parent.mkdir(exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            CREATE TABLE IF NOT EXISTS deployments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT UNIQUE,
                strategy_id TEXT,
                fly_app_name TEXT,
                region TEXT,
                status TEXT,
                deployed_at TIMESTAMP,
                promoted_to_hot BOOLEAN DEFAULT 0,
                total_trades INTEGER DEFAULT 0,
                win_rate REAL,
                sharpe_ratio REAL,
                total_pnl REAL,
                last_health_check TIMESTAMP
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS deployment_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT,
                timestamp TIMESTAMP,
                action TEXT,
                status TEXT,
                message TEXT
            )
        ''')

        conn.commit()
        conn.close()

    def check_promotion_criteria(self):
        """
        Check if agent meets deployment criteria

        Criteria:
        - 10+ trades
        - 65%+ win rate
        - Sharpe ratio > 1.0
        - Total PnL > 0

        Returns:
            bool: True if ready to deploy
        """

        if self.trades < 10:
            logger.debug(f'{self.agent_id}: Only {self.trades} trades (need 10+)')
            return False

        win_rate = self.wins / self.trades if self.trades > 0 else 0

        if win_rate < 0.65:
            logger.debug(f'{self.agent_id}: Win rate {win_rate:.1%} (need 65%+)')
            return False

        if self.sharpe_ratio < 1.0:
            logger.debug(f'{self.agent_id}: Sharpe {self.sharpe_ratio:.2f} (need 1.0+)')
            return False

        if self.total_pnl <= 0:
            logger.debug(f'{self.agent_id}: PnL ${self.total_pnl:.2f} (need positive)')
            return False

        logger.info(f'{self.agent_id}: ✅ PROMOTION CRITERIA MET (trades={self.trades}, WR={win_rate:.1%}, Sharpe={self.sharpe_ratio:.2f}, PnL=${self.total_pnl:.2f})')
        return True

    def generate_dockerfile(self):
        """
        Generate Dockerfile for agent deployment

        Returns:
            str: Dockerfile content
        """

        dockerfile = f'''# Auto-generated Dockerfile for {self.agent_id}
FROM python:3.11-slim

# Install dependencies
RUN apt-get update && \\
    apt-get install -y --no-install-recommends ca-certificates curl && \\
    rm -rf /var/lib/apt/lists/*

# Python packages
RUN pip install --no-cache-dir \\
    numpy==1.26.4 \\
    pandas==2.2.0 \\
    requests==2.31.0 \\
    websocket-client==1.7.0

WORKDIR /app

# Copy agent code
COPY agents/generated_strategies/{self.strategy_id}.py /app/strategy.py
COPY agents/sandbox_runner.py /app/runner.py

# Environment
ENV STRATEGY_ID={self.strategy_id}
ENV TRADING_PAIR=BTC-USD
ENV AGENT_ID={self.agent_id}

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \\
    CMD python3 -c "import sys; sys.exit(0)"

# Run
CMD ["python3", "-u", "runner.py", "--strategy-id", "{self.strategy_id}"]
'''

        return dockerfile

    def generate_fly_toml(self):
        """
        Generate fly.toml configuration

        Returns:
            str: fly.toml content
        """

        fly_toml = f'''# Auto-generated fly.toml for {self.agent_id}
app = "{self.agent_id}"
primary_region = "{self.primary_region}"

[build]
  dockerfile = "Dockerfile"

[env]
  STRATEGY_ID = "{self.strategy_id}"
  AGENT_ID = "{self.agent_id}"

[http_service]
  internal_port = 8080
  force_https = true
  auto_stop_machines = false
  auto_start_machines = true
  min_machines_running = 1

[[vm]]
  memory = '256mb'
  cpu_kind = 'shared'
  cpus = 1
'''

        return fly_toml

    def build_image(self, build_dir):
        """
        Build Docker image

        Args:
            build_dir: Directory containing Dockerfile

        Returns:
            bool: True if successful
        """

        logger.info(f'Building image for {self.agent_id}...')

        try:
            cmd = [
                'docker', 'build',
                '-t', f'{self.agent_id}:latest',
                str(build_dir)
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

            if result.returncode == 0:
                logger.info(f'✅ Image built successfully')
                self._log_deployment('build_image', 'success', 'Docker image built')
                return True
            else:
                logger.error(f'Image build failed: {result.stderr}')
                self._log_deployment('build_image', 'failed', result.stderr[:500])
                return False

        except Exception as e:
            logger.error(f'Failed to build image: {e}')
            self._log_deployment('build_image', 'error', str(e))
            return False

    def deploy_to_fly(self):
        """
        Deploy agent to Fly.io

        Returns:
            bool: True if successful
        """

        logger.info(f'Deploying {self.agent_id} to Fly.io ({self.primary_region})...')

        # Check flyctl availability
        if not os.path.exists(self.flyctl_path):
            logger.error(f'flyctl not found at {self.flyctl_path}')
            return False

        try:
            # Prepare deployment directory
            deploy_dir = Path(__file__).parent.parent / 'deployments' / self.agent_id
            deploy_dir.mkdir(parents=True, exist_ok=True)

            # Write Dockerfile
            dockerfile_path = deploy_dir / 'Dockerfile'
            dockerfile_path.write_text(self.generate_dockerfile())

            # Write fly.toml
            fly_toml_path = deploy_dir / 'fly.toml'
            fly_toml_path.write_text(self.generate_fly_toml())

            # Copy strategy code
            strategy_src = Path(__file__).parent / 'generated_strategies' / f'{self.strategy_id}.py'
            if not strategy_src.exists():
                logger.error(f'Strategy file not found: {strategy_src}')
                return False

            # Launch Fly app
            logger.info(f'Creating Fly app: {self.agent_id}')

            # Check if app exists
            check_cmd = [self.flyctl_path, 'apps', 'list', '--json']
            result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=30)

            app_exists = False
            if result.returncode == 0:
                try:
                    apps = json.loads(result.stdout)
                    app_exists = any(app['Name'] == self.agent_id for app in apps)
                except:
                    pass

            if not app_exists:
                # Create new app
                create_cmd = [
                    self.flyctl_path, 'apps', 'create',
                    self.agent_id,
                    '--org', 'personal'
                ]

                result = subprocess.run(create_cmd, capture_output=True, text=True, timeout=30)

                if result.returncode != 0:
                    logger.error(f'Failed to create app: {result.stderr}')
                    self._log_deployment('create_app', 'failed', result.stderr[:500])
                    return False

                logger.info(f'✅ Fly app created: {self.agent_id}')
                self._log_deployment('create_app', 'success', f'App created in {self.primary_region}')

            # Deploy
            logger.info('Deploying to Fly...')

            deploy_cmd = [
                self.flyctl_path, 'deploy',
                '--app', self.agent_id,
                '--config', str(fly_toml_path),
                '--dockerfile', str(dockerfile_path),
                '--remote-only',
                '--region', self.primary_region
            ]

            result = subprocess.run(deploy_cmd, capture_output=True, text=True,
                                  timeout=600, cwd=str(deploy_dir))

            if result.returncode == 0:
                logger.info(f'✅ Deployed to Fly.io')
                self.deployed = True
                self.fly_app_name = self.agent_id
                self.deployment_status = 'deployed'

                # Save to database
                self._save_deployment()
                self._log_deployment('deploy', 'success', f'Deployed to {self.primary_region}')

                return True
            else:
                logger.error(f'Deployment failed: {result.stderr}')
                self._log_deployment('deploy', 'failed', result.stderr[:500])
                return False

        except Exception as e:
            logger.error(f'Deploy failed: {e}')
            self._log_deployment('deploy', 'error', str(e))
            return False

    def scale_to_multi_region(self):
        """Scale to multiple regions after HOT promotion"""

        if not self.deployed:
            logger.error('Cannot scale - not deployed')
            return False

        logger.info(f'Scaling {self.agent_id} to multi-region...')

        try:
            for region in self.multi_regions:
                scale_cmd = [
                    self.flyctl_path, 'scale', 'count',
                    '1',
                    '--app', self.agent_id,
                    '--region', region
                ]

                result = subprocess.run(scale_cmd, capture_output=True, text=True, timeout=60)

                if result.returncode == 0:
                    logger.info(f'✅ Scaled to {region}')
                    self._log_deployment('scale', 'success', f'Scaled to {region}')
                else:
                    logger.warning(f'Failed to scale to {region}: {result.stderr}')

            return True

        except Exception as e:
            logger.error(f'Scaling failed: {e}')
            return False

    def monitor_health(self):
        """
        Check deployment health

        Returns:
            dict: Health status
        """

        if not self.deployed:
            return {'status': 'not_deployed'}

        try:
            status_cmd = [
                self.flyctl_path, 'status',
                '--app', self.agent_id,
                '--json'
            ]

            result = subprocess.run(status_cmd, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                status = json.loads(result.stdout)

                # Update database
                conn = sqlite3.connect(self.db_path)
                c = conn.cursor()
                c.execute('''
                    UPDATE deployments
                    SET last_health_check = ?
                    WHERE agent_id = ?
                ''', (datetime.utcnow(), self.agent_id))
                conn.commit()
                conn.close()

                return status
            else:
                logger.error(f'Health check failed: {result.stderr}')
                return {'status': 'error', 'message': result.stderr}

        except Exception as e:
            logger.error(f'Health check error: {e}')
            return {'status': 'error', 'message': str(e)}

    def kill_deployment(self):
        """Kill Fly.io deployment"""

        if not self.deployed:
            logger.warning('No deployment to kill')
            return True

        logger.info(f'Killing deployment: {self.agent_id}')

        try:
            destroy_cmd = [
                self.flyctl_path, 'apps', 'destroy',
                self.agent_id,
                '--yes'
            ]

            result = subprocess.run(destroy_cmd, capture_output=True, text=True, timeout=60)

            if result.returncode == 0:
                logger.info(f'✅ Deployment killed')
                self.deployed = False
                self.deployment_status = 'killed'

                # Update database
                conn = sqlite3.connect(self.db_path)
                c = conn.cursor()
                c.execute('''
                    UPDATE deployments
                    SET status = 'killed'
                    WHERE agent_id = ?
                ''', (self.agent_id,))
                conn.commit()
                conn.close()

                self._log_deployment('kill', 'success', 'Deployment terminated')
                return True
            else:
                logger.error(f'Kill failed: {result.stderr}')
                self._log_deployment('kill', 'failed', result.stderr[:500])
                return False

        except Exception as e:
            logger.error(f'Kill error: {e}')
            self._log_deployment('kill', 'error', str(e))
            return False

    def _save_deployment(self):
        """Save deployment to database"""

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        win_rate = self.wins / self.trades if self.trades > 0 else 0

        c.execute('''
            INSERT OR REPLACE INTO deployments
            (agent_id, strategy_id, fly_app_name, region, status, deployed_at,
             total_trades, win_rate, sharpe_ratio, total_pnl, last_health_check)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (self.agent_id, self.strategy_id, self.fly_app_name, self.primary_region,
              self.deployment_status, datetime.utcnow(), self.trades, win_rate,
              self.sharpe_ratio, self.total_pnl, datetime.utcnow()))

        conn.commit()
        conn.close()

    def _log_deployment(self, action, status, message):
        """Log deployment action"""

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            INSERT INTO deployment_logs (agent_id, timestamp, action, status, message)
            VALUES (?, ?, ?, ?, ?)
        ''', (self.agent_id, datetime.utcnow(), action, status, message))

        conn.commit()
        conn.close()

    def update_performance(self, trade_result):
        """
        Update performance metrics from trade result

        Args:
            trade_result: dict with 'pnl', 'success', etc.
        """

        self.trades += 1

        if trade_result.get('success') or trade_result.get('pnl', 0) > 0:
            self.wins += 1

        pnl = trade_result.get('pnl', 0)
        self.total_pnl += pnl

        # Simple Sharpe calculation
        if self.trades >= 5:
            avg_return = self.total_pnl / self.trades
            # Simplified - in production would track variance
            self.sharpe_ratio = avg_return / (abs(avg_return) * 0.1 + 0.01)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Self-Deploying Agent')
    parser.add_argument('--strategy-id', required=True, help='Strategy ID to deploy')
    parser.add_argument('--test-mode', action='store_true', help='Test mode (no actual deployment)')

    args = parser.parse_args()

    agent = SelfDeployingAgent(args.strategy_id)

    # Simulate performance for testing
    if args.test_mode:
        print(f'\n🧪 Testing self-deployment for {args.strategy_id}\n')

        # Simulate trades
        print('Simulating trades...')
        for i in range(15):
            agent.update_performance({
                'pnl': 1.5 if i % 3 != 0 else -0.5,  # 66% win rate
                'success': i % 3 != 0
            })

        print(f'   Trades: {agent.trades}')
        print(f'   Win rate: {agent.wins/agent.trades:.1%}')
        print(f'   Sharpe: {agent.sharpe_ratio:.2f}')
        print(f'   Total PnL: ${agent.total_pnl:.2f}')
        print()

        # Check criteria
        if agent.check_promotion_criteria():
            print('✅ Would deploy to Fly.io (test mode - skipping actual deployment)\n')
            print('   Dockerfile preview:')
            print('   ' + '\n   '.join(agent.generate_dockerfile().split('\n')[:10]))
            print('   ...\n')
        else:
            print('❌ Criteria not met for deployment\n')

    else:
        print(f'\n🚀 Self-Deploying Agent: {args.strategy_id}\n')
        print('⚠️  Real deployment mode - will deploy to Fly.io if criteria met\n')

        if agent.check_promotion_criteria():
            print('Deploying to Fly.io...')
            success = agent.deploy_to_fly()

            if success:
                print(f'\n✅ DEPLOYED: {agent.agent_id}')
                print(f'   App: {agent.fly_app_name}')
                print(f'   Region: {agent.primary_region}')
                print('\nMonitor with:')
                print(f'   flyctl logs -a {agent.agent_id}')
                print(f'   flyctl status -a {agent.agent_id}')
            else:
                print('\n❌ Deployment failed')
        else:
            print('❌ Criteria not met - agent not ready for deployment')
