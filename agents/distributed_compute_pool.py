#!/usr/bin/env python3
"""
Distributed Compute Pool - EPOCH 3
Coordinate ML inference across multiple GPUs (local + cloud)

Machines:
- M3 MacBook Air: 16GB RAM, Apple Metal GPU
- M1 Max: 192.168.1.110, 64GB RAM, 32-core GPU, PyTorch 2.8
- M2 Ultra: 192.168.1.106, 128GB RAM, 76-core GPU, PyTorch 2.4

Future: AWS Spot, Vast.ai for 100+ GPU scaling

Expected: 1000+ predictions/sec, hyperparameter optimization
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import asyncio
import aiohttp
import logging
from datetime import datetime
import sqlite3
from typing import Dict, List, Optional
import json
import socket
import pickle
import struct

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('distributed_compute')


class ComputeNode:
    """Represents a compute node (local or remote GPU)"""

    def __init__(self, node_id: str, host: str, port: int, gpu_type: str, memory_gb: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.gpu_type = gpu_type
        self.memory_gb = memory_gb
        self.status = 'offline'
        self.current_job = None
        self.jobs_completed = 0
        self.avg_inference_time = 0

    async def health_check(self) -> bool:
        """Check if node is online"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f'http://{self.host}:{self.port}/health',
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        self.status = 'online'
                        return True
        except:
            pass

        self.status = 'offline'
        return False

    async def submit_job(self, job_type: str, data: Dict) -> Optional[Dict]:
        """Submit inference job to node"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f'http://{self.host}:{self.port}/infer',
                    json={'job_type': job_type, 'data': data},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        self.jobs_completed += 1
                        return result
        except Exception as e:
            logger.error(f'Job submission to {self.node_id} failed: {e}')
            return None

    def to_dict(self):
        """Convert to dict"""
        return {
            'node_id': self.node_id,
            'host': self.host,
            'port': self.port,
            'gpu_type': self.gpu_type,
            'memory_gb': self.memory_gb,
            'status': self.status,
            'jobs_completed': self.jobs_completed
        }


class DistributedComputePool:
    """
    Manages distributed compute across multiple GPUs
    """

    def __init__(self):
        self.nodes = {}
        self.job_queue = asyncio.Queue()
        self.results = {}

        # Database
        self.db_path = Path(__file__).parent.parent / 'data' / 'compute_pool.db'
        self._init_db()

        # Register default nodes
        self._register_default_nodes()

        logger.info(f'Distributed Compute Pool initialized: {len(self.nodes)} nodes')

    def _init_db(self):
        """Initialize database"""
        self.db_path.parent.mkdir(exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            CREATE TABLE IF NOT EXISTS compute_nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT UNIQUE,
                host TEXT,
                port INTEGER,
                gpu_type TEXT,
                memory_gb INTEGER,
                status TEXT,
                jobs_completed INTEGER DEFAULT 0,
                last_seen TIMESTAMP
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS compute_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT UNIQUE,
                job_type TEXT,
                node_id TEXT,
                status TEXT,
                submitted_at TIMESTAMP,
                completed_at TIMESTAMP,
                inference_time_ms REAL,
                result TEXT
            )
        ''')

        conn.commit()
        conn.close()

    def _register_default_nodes(self):
        """Register default local nodes"""

        # Local M3 (this machine)
        self.register_node(
            node_id='m3_local',
            host='localhost',
            port=9090,
            gpu_type='Apple M3 Metal',
            memory_gb=16
        )

        # M1 Max
        self.register_node(
            node_id='m1_max',
            host='192.168.1.110',
            port=9090,
            gpu_type='Apple M1 Max Metal',
            memory_gb=64
        )

        # M2 Ultra
        self.register_node(
            node_id='m2_ultra',
            host='192.168.1.106',
            port=9090,
            gpu_type='Apple M2 Ultra Metal',
            memory_gb=128
        )

    def register_node(self, node_id: str, host: str, port: int, gpu_type: str, memory_gb: int):
        """Register a compute node"""

        node = ComputeNode(node_id, host, port, gpu_type, memory_gb)
        self.nodes[node_id] = node

        # Save to database
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            INSERT OR REPLACE INTO compute_nodes
            (node_id, host, port, gpu_type, memory_gb, status, last_seen)
            VALUES (?, ?, ?, ?, ?, 'offline', ?)
        ''', (node_id, host, port, gpu_type, memory_gb, datetime.utcnow()))

        conn.commit()
        conn.close()

        logger.info(f'Registered node: {node_id} ({gpu_type}, {memory_gb}GB)')

    async def health_check_all(self):
        """Health check all nodes"""

        tasks = []
        for node in self.nodes.values():
            tasks.append(node.health_check())

        results = await asyncio.gather(*tasks)

        online = sum(results)
        logger.info(f'Health check: {online}/{len(self.nodes)} nodes online')

        return online

    async def get_best_node(self, job_type: str = None) -> Optional[ComputeNode]:
        """
        Get best available node for job

        Strategy:
        - Prefer M2 Ultra for large models
        - Prefer M1 Max for medium models
        - Use M3 for small/fast inference
        """

        # Filter online nodes
        online_nodes = [n for n in self.nodes.values() if n.status == 'online']

        if not online_nodes:
            return None

        # For large models, prefer M2 Ultra
        if job_type in ['large_model', 'hyperparameter_optimization']:
            for node in online_nodes:
                if 'M2 Ultra' in node.gpu_type:
                    return node

        # For medium models, prefer M1 Max
        if job_type in ['medium_model', 'ensemble']:
            for node in online_nodes:
                if 'M1 Max' in node.gpu_type:
                    return node

        # Default: least loaded node
        return min(online_nodes, key=lambda n: n.jobs_completed)

    async def submit_inference_job(self, model_name: str, data: Dict) -> Optional[Dict]:
        """
        Submit inference job to pool

        Args:
            model_name: Model to run (e.g., 'timesfm', 'patchtst')
            data: Input data for model

        Returns:
            Inference result or None
        """

        # Find best node
        node = await self.get_best_node(job_type='inference')

        if not node:
            logger.warning('No nodes available for inference')
            return None

        logger.info(f'Submitting {model_name} inference to {node.node_id}')

        # Submit job
        result = await node.submit_job('inference', {
            'model': model_name,
            'data': data
        })

        if result:
            logger.info(f'Inference completed on {node.node_id}')

        return result

    async def parallel_inference(self, model_name: str, data_batch: List[Dict]) -> List[Dict]:
        """
        Run inference on batch in parallel across all nodes

        Args:
            model_name: Model to run
            data_batch: List of input data

        Returns:
            List of results
        """

        # Get online nodes
        online_nodes = [n for n in self.nodes.values() if n.status == 'online']

        if not online_nodes:
            logger.warning('No nodes available')
            return []

        # Distribute work across nodes
        tasks = []
        node_idx = 0

        for data in data_batch:
            node = online_nodes[node_idx % len(online_nodes)]
            tasks.append(node.submit_job('inference', {
                'model': model_name,
                'data': data
            }))
            node_idx += 1

        # Wait for all
        results = await asyncio.gather(*tasks)

        # Filter out None results
        return [r for r in results if r is not None]

    async def hyperparameter_search(self, model_name: str, param_grid: Dict) -> Dict:
        """
        Run hyperparameter search across cluster

        Args:
            model_name: Model to optimize
            param_grid: Grid of parameters to search

        Returns:
            Best parameters
        """

        logger.info(f'Starting hyperparameter search for {model_name}')
        logger.info(f'Grid size: {len(param_grid)} combinations')

        # Generate all parameter combinations
        combinations = []
        # TODO: Implement grid expansion

        # For now, return mock result
        best_params = {
            'learning_rate': 0.001,
            'batch_size': 32,
            'epochs': 100
        }

        logger.info(f'Hyperparameter search complete')

        return best_params

    def get_pool_stats(self):
        """Get compute pool statistics"""

        total_nodes = len(self.nodes)
        online_nodes = sum(1 for n in self.nodes.values() if n.status == 'online')
        total_jobs = sum(n.jobs_completed for n in self.nodes.values())

        total_memory = sum(n.memory_gb for n in self.nodes.values())
        online_memory = sum(n.memory_gb for n in self.nodes.values() if n.status == 'online')

        return {
            'total_nodes': total_nodes,
            'online_nodes': online_nodes,
            'total_jobs_completed': total_jobs,
            'total_memory_gb': total_memory,
            'online_memory_gb': online_memory,
            'nodes': [n.to_dict() for n in self.nodes.values()]
        }


async def main():
    print('🖥️  Distributed Compute Pool - EPOCH 3')
    print('='*70)

    pool = DistributedComputePool()

    # Health check
    print('\n🏥 Health checking all nodes...\n')

    online = await pool.health_check_all()

    print(f'   {online}/{len(pool.nodes)} nodes online\n')

    # Show stats
    stats = pool.get_pool_stats()

    print('📊 Compute Pool Stats:')
    print(f'   Total nodes: {stats["total_nodes"]}')
    print(f'   Online nodes: {stats["online_nodes"]}')
    print(f'   Total memory: {stats["total_memory_gb"]}GB')
    print(f'   Online memory: {stats["online_memory_gb"]}GB')
    print(f'   Jobs completed: {stats["total_jobs_completed"]}')
    print()

    print('🖥️  Registered Nodes:')
    for node_data in stats['nodes']:
        status_icon = '✅' if node_data['status'] == 'online' else '❌'
        print(f'   {status_icon} {node_data["node_id"]:15s} | {node_data["gpu_type"]:25s} | '
              f'{node_data["memory_gb"]:3d}GB | {node_data["jobs_completed"]:4d} jobs')

    print('\n✅ Distributed Compute Pool ready')
    print('   Note: Nodes at 192.168.1.110 and 192.168.1.106 need compute_server running')


if __name__ == '__main__':
    asyncio.run(main())
