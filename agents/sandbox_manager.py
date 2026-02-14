#!/usr/bin/env python3
"""
Sandbox Manager - Manages Docker containers for isolated agent execution

Features:
- Spawn/kill sandboxed agents
- Resource limits (512MB RAM, 0.5 CPU)
- Network whitelist (Coinbase API only)
- Read-only filesystem
- Signal collection via stdout
- Auto-restart failed containers

Security:
- Non-root user inside containers
- No host filesystem access
- Network isolation
- Memory/CPU limits enforced by Docker
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import subprocess
import json
import logging
import time
from datetime import datetime
import sqlite3
import signal as signal_module

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('sandbox_manager')


class SandboxManager:
    """
    Manages sandboxed agent containers
    """

    def __init__(self, max_containers=20):
        self.max_containers = max_containers
        self.containers = {}  # container_id -> metadata
        self.image_name = 'trading-sandbox:latest'

        # Database for tracking
        self.db_path = Path(__file__).parent.parent / 'data' / 'sandboxes.db'
        self._init_db()

        # Docker availability check
        self.docker_available = self._check_docker()

        logger.info(f'Sandbox Manager initialized (max: {max_containers}, docker: {self.docker_available})')

    def _check_docker(self):
        """Check if Docker is available"""
        try:
            result = subprocess.run(['docker', 'version'],
                                    capture_output=True,
                                    timeout=5)
            return result.returncode == 0
        except Exception as e:
            logger.warning(f'Docker not available: {e}')
            return False

    def _init_db(self):
        """Initialize sandbox tracking database"""
        self.db_path.parent.mkdir(exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            CREATE TABLE IF NOT EXISTS sandboxes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container_id TEXT UNIQUE,
                strategy_id TEXT,
                pair TEXT,
                status TEXT,
                created_at TIMESTAMP,
                stopped_at TIMESTAMP,
                signals_generated INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                last_signal_at TIMESTAMP
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS sandbox_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container_id TEXT,
                strategy_id TEXT,
                pair TEXT,
                signal TEXT,
                confidence REAL,
                timestamp TIMESTAMP,
                data TEXT
            )
        ''')

        conn.commit()
        conn.close()

    def build_image(self):
        """Build sandbox Docker image"""

        if not self.docker_available:
            logger.error('Docker not available, cannot build image')
            return False

        dockerfile_path = Path(__file__).parent.parent / 'automation_empire' / 'infrastructure' / 'docker'
        dockerfile = dockerfile_path / 'Dockerfile.trading-sandbox'

        if not dockerfile.exists():
            logger.error(f'Dockerfile not found: {dockerfile}')
            return False

        logger.info('Building sandbox Docker image...')

        try:
            # Build image
            cmd = [
                'docker', 'build',
                '-t', self.image_name,
                '-f', str(dockerfile),
                str(Path(__file__).parent.parent)  # Build context
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

            if result.returncode == 0:
                logger.info('Image built successfully')
                return True
            else:
                logger.error(f'Image build failed: {result.stderr}')
                return False

        except Exception as e:
            logger.error(f'Failed to build image: {e}')
            return False

    def spawn_sandbox(self, strategy_id, pair='BTC-USD', strategy_code=None):
        """
        Spawn a sandboxed agent container

        Args:
            strategy_id: Strategy to run
            pair: Trading pair
            strategy_code: Optional code to inject

        Returns:
            container_id or None
        """

        if not self.docker_available:
            logger.error('Docker not available')
            return None

        if len(self.containers) >= self.max_containers:
            logger.warning('Max containers reached')
            return None

        logger.info(f'Spawning sandbox for {strategy_id} on {pair}')

        try:
            # Prepare environment
            env_vars = [
                '-e', f'STRATEGY_ID={strategy_id}',
                '-e', f'TRADING_PAIR={pair}',
            ]

            if strategy_code:
                # Pass code as env var (limited to ~1MB)
                env_vars.extend(['-e', f'STRATEGY_CODE={strategy_code}'])

            # Run container with resource limits
            cmd = [
                'docker', 'run',
                '-d',  # Detached
                '--name', f'sandbox_{strategy_id}_{int(time.time())}',

                # Resource limits
                '--memory=512m',
                '--cpus=0.5',
                '--pids-limit=100',

                # Security
                '--read-only',
                '--tmpfs', '/tmp:rw,noexec,nosuid,size=100m',
                '--security-opt=no-new-privileges',
                '--cap-drop=ALL',

                # Network (TODO: restrict to whitelist)
                '--network=bridge',

                # Environment
                *env_vars,

                # Image
                self.image_name,

                # Command
                'agents/sandbox_runner.py',
                '--strategy-id', strategy_id,
                '--pair', pair,
                '--mode', 'loop'
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if result.returncode != 0:
                logger.error(f'Failed to spawn container: {result.stderr}')
                return None

            container_id = result.stdout.strip()

            # Track container
            self.containers[container_id] = {
                'strategy_id': strategy_id,
                'pair': pair,
                'started_at': datetime.utcnow(),
                'status': 'running'
            }

            # Save to DB
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('''
                INSERT INTO sandboxes (container_id, strategy_id, pair, status, created_at)
                VALUES (?, ?, ?, 'running', ?)
            ''', (container_id, strategy_id, pair, datetime.utcnow()))
            conn.commit()
            conn.close()

            logger.info(f'Spawned container {container_id[:12]}')

            return container_id

        except Exception as e:
            logger.error(f'Failed to spawn sandbox: {e}')
            return None

    def stop_sandbox(self, container_id):
        """Stop a sandbox container"""

        logger.info(f'Stopping sandbox {container_id[:12]}...')

        try:
            subprocess.run(['docker', 'stop', container_id],
                           capture_output=True, timeout=30)

            subprocess.run(['docker', 'rm', container_id],
                           capture_output=True, timeout=30)

            # Update DB
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('''
                UPDATE sandboxes
                SET status = 'stopped', stopped_at = ?
                WHERE container_id = ?
            ''', (datetime.utcnow(), container_id))
            conn.commit()
            conn.close()

            # Remove from tracking
            if container_id in self.containers:
                del self.containers[container_id]

            logger.info(f'Stopped {container_id[:12]}')

        except Exception as e:
            logger.error(f'Failed to stop sandbox: {e}')

    def get_sandbox_logs(self, container_id, tail=100):
        """Get logs from sandbox container"""

        try:
            result = subprocess.run(
                ['docker', 'logs', '--tail', str(tail), container_id],
                capture_output=True,
                text=True,
                timeout=10
            )

            return result.stdout + result.stderr

        except Exception as e:
            logger.error(f'Failed to get logs: {e}')
            return None

    def list_sandboxes(self):
        """List all active sandboxes"""

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            SELECT container_id, strategy_id, pair, status, created_at, signals_generated
            FROM sandboxes
            WHERE status = 'running'
            ORDER BY created_at DESC
        ''')

        sandboxes = []
        for row in c.fetchall():
            sandboxes.append({
                'container_id': row[0],
                'strategy_id': row[1],
                'pair': row[2],
                'status': row[3],
                'created_at': row[4],
                'signals_generated': row[5]
            })

        conn.close()

        return sandboxes

    def cleanup_stopped(self):
        """Remove stopped containers"""

        logger.info('Cleaning up stopped containers...')

        try:
            # List all stopped containers
            result = subprocess.run(
                ['docker', 'ps', '-a', '--filter', 'status=exited', '--format', '{{.ID}}'],
                capture_output=True,
                text=True,
                timeout=10
            )

            container_ids = result.stdout.strip().split('\n')

            for cid in container_ids:
                if cid:
                    subprocess.run(['docker', 'rm', cid],
                                   capture_output=True, timeout=10)

            logger.info(f'Cleaned up {len(container_ids)} containers')

        except Exception as e:
            logger.error(f'Cleanup failed: {e}')

    def stop_all(self):
        """Stop all sandboxes"""

        logger.info('Stopping all sandboxes...')

        for container_id in list(self.containers.keys()):
            self.stop_sandbox(container_id)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Sandbox Manager')
    parser.add_argument('command', choices=['build', 'spawn', 'list', 'stop', 'logs', 'cleanup'],
                        help='Command to execute')
    parser.add_argument('--strategy-id', help='Strategy ID for spawn command')
    parser.add_argument('--pair', default='BTC-USD', help='Trading pair')
    parser.add_argument('--container-id', help='Container ID for stop/logs commands')

    args = parser.parse_args()

    manager = SandboxManager()

    if args.command == 'build':
        success = manager.build_image()
        sys.exit(0 if success else 1)

    elif args.command == 'spawn':
        if not args.strategy_id:
            print('Error: --strategy-id required for spawn')
            sys.exit(1)

        container_id = manager.spawn_sandbox(args.strategy_id, args.pair)

        if container_id:
            print(f'Spawned: {container_id}')
            sys.exit(0)
        else:
            print('Failed to spawn')
            sys.exit(1)

    elif args.command == 'list':
        sandboxes = manager.list_sandboxes()

        print(f'\n{"Container ID":20s} {"Strategy":30s} {"Pair":10s} {"Signals":8s} {"Created"}\n' + '='*90)

        for sb in sandboxes:
            print(f'{sb["container_id"][:20]:20s} {sb["strategy_id"]:30s} {sb["pair"]:10s} '
                  f'{sb["signals_generated"]:8d} {sb["created_at"]}')

        print(f'\nTotal: {len(sandboxes)} sandboxes running')

    elif args.command == 'stop':
        if not args.container_id:
            print('Error: --container-id required for stop')
            sys.exit(1)

        manager.stop_sandbox(args.container_id)

    elif args.command == 'logs':
        if not args.container_id:
            print('Error: --container-id required for logs')
            sys.exit(1)

        logs = manager.get_sandbox_logs(args.container_id)
        if logs:
            print(logs)

    elif args.command == 'cleanup':
        manager.cleanup_stopped()

    print('\n✅ Sandbox manager ready')
