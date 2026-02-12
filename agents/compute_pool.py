#!/usr/bin/env python3
"""NetTrace Compute Pool — distributed ML inference across Apple Silicon and CUDA nodes.

Architecture:
  - Lightweight HTTP server on port 9090 (stdlib only, no dependencies)
  - Nodes register with capabilities (gpu_type, memory, frameworks)
  - Tasks dispatched to best-fit node based on requirements and current load
  - SQLite for persistent state; WAL mode for concurrent reads
  - Graceful degradation: pool works even if all remote nodes are offline

Preconfigured nodes:
  - local     : M3 Air (this machine)
  - m1max     : M1 Max @ 192.168.1.110
  - m2ultra   : M2 Ultra @ 192.168.1.106

Extensible to CUDA via POST /register from any GPU instance.
"""

import hashlib
import json
import logging
import os
import platform
import queue
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs
import urllib.request
import urllib.error

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POOL_HOST = os.environ.get("POOL_HOST", "0.0.0.0")
POOL_PORT = int(os.environ.get("POOL_PORT", "9090"))
DB_PATH = str(Path(__file__).parent / "compute_pool.db")

# Health check interval in seconds
HEALTH_INTERVAL = 30
# Node considered dead after this many seconds without heartbeat
NODE_TIMEOUT = 120
# Max queued tasks before rejecting new dispatches
MAX_QUEUE_DEPTH = 1000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [compute_pool] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "compute_pool.log")),
    ],
)
logger = logging.getLogger("compute_pool")


# ---------------------------------------------------------------------------
# Apple Silicon / GPU detection
# ---------------------------------------------------------------------------

def detect_apple_gpu() -> Dict[str, Any]:
    """Detect Apple GPU via system_profiler. Returns gpu info dict."""
    info = {
        "gpu_type": "unknown",
        "gpu_name": "",
        "metal_support": False,
        "gpu_cores": 0,
        "memory_gb": 0,
        "chip": "",
    }

    if platform.system() != "Darwin":
        return info

    try:
        result = subprocess.run(
            ["system_profiler", "SPDisplaysDataType", "-json"],
            capture_output=True, text=True, timeout=10,
        )
        data = json.loads(result.stdout)
        displays = data.get("SPDisplaysDataType", [])
        if displays:
            gpu = displays[0]
            info["gpu_name"] = gpu.get("sppci_model", "")
            info["metal_support"] = "metal" in gpu.get("spdisplays_metal", "").lower() \
                or "supported" in gpu.get("spdisplays_metal", "").lower() \
                or "spdisplays_metal-supported" in gpu.get("spdisplays_metalfeaturesetfamily", "").lower()
            cores_str = gpu.get("sppci_cores", "0")
            try:
                info["gpu_cores"] = int(cores_str)
            except (ValueError, TypeError):
                pass
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError, KeyError):
        pass

    # Detect chip type from sysctl
    try:
        result = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True, text=True, timeout=5,
        )
        info["chip"] = result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # Detect total memory
    try:
        result = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True, text=True, timeout=5,
        )
        mem_bytes = int(result.stdout.strip())
        info["memory_gb"] = round(mem_bytes / (1024 ** 3), 1)
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
        pass

    # Detect MLX availability
    info["mlx_available"] = False
    try:
        import mlx.core  # noqa: F401
        info["mlx_available"] = True
    except ImportError:
        pass

    # Metal is available on all Apple Silicon Macs
    if "apple" in info.get("chip", "").lower() or info.get("gpu_name", "").startswith("Apple"):
        info["gpu_type"] = "apple_metal"
        info["metal_support"] = True

    return info


def detect_frameworks() -> List[str]:
    """Detect which ML frameworks are available on this node."""
    frameworks = ["numpy"]  # always available (or we have bigger problems)

    try:
        import mlx.core  # noqa: F401
        frameworks.append("mlx")
    except ImportError:
        pass

    try:
        import torch  # noqa: F401
        frameworks.append("pytorch")
        if torch.cuda.is_available():
            frameworks.append("cuda")
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            frameworks.append("mps")
    except ImportError:
        pass

    try:
        import tensorflow  # noqa: F401
        frameworks.append("tensorflow")
    except ImportError:
        pass

    return frameworks


# ---------------------------------------------------------------------------
# Database layer
# ---------------------------------------------------------------------------

class PoolDB:
    """Thread-safe SQLite wrapper for compute pool state."""

    def __init__(self, db_path: str = DB_PATH):
        self._db_path = db_path
        self._local = threading.local()
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self._db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            self._local.conn = conn
        return self._local.conn

    def _init_schema(self):
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                host TEXT NOT NULL,
                port INTEGER DEFAULT 9091,
                gpu_type TEXT DEFAULT 'cpu',
                gpu_name TEXT DEFAULT '',
                gpu_cores INTEGER DEFAULT 0,
                memory_gb REAL DEFAULT 0,
                chip TEXT DEFAULT '',
                frameworks TEXT DEFAULT '[]',
                metal_support INTEGER DEFAULT 0,
                mlx_available INTEGER DEFAULT 0,
                cuda_available INTEGER DEFAULT 0,
                status TEXT DEFAULT 'online',
                current_load REAL DEFAULT 0.0,
                tasks_completed INTEGER DEFAULT 0,
                tasks_failed INTEGER DEFAULT 0,
                avg_latency_ms REAL DEFAULT 0.0,
                throughput_tps REAL DEFAULT 0.0,
                last_heartbeat TIMESTAMP,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                data TEXT NOT NULL,
                requirements TEXT DEFAULT '{}',
                assigned_node TEXT,
                status TEXT DEFAULT 'queued',
                priority INTEGER DEFAULT 5,
                result TEXT,
                error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (assigned_node) REFERENCES nodes(node_id)
            );

            CREATE TABLE IF NOT EXISTS task_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                execution_ms REAL,
                gpu_utilization REAL,
                memory_used_mb REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES tasks(task_id),
                FOREIGN KEY (node_id) REFERENCES nodes(node_id)
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
            CREATE INDEX IF NOT EXISTS idx_tasks_node ON tasks(assigned_node);
            CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
        """)
        conn.commit()

    # -- Node operations --

    def register_node(self, node: Dict) -> str:
        conn = self._get_conn()
        node_id = node.get("node_id") or str(uuid.uuid4())[:12]
        frameworks_json = json.dumps(node.get("frameworks", []))
        now = datetime.now(timezone.utc).isoformat()

        conn.execute("""
            INSERT INTO nodes (
                node_id, name, host, port, gpu_type, gpu_name, gpu_cores,
                memory_gb, chip, frameworks, metal_support, mlx_available,
                cuda_available, status, last_heartbeat
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'online', ?)
            ON CONFLICT(node_id) DO UPDATE SET
                name=excluded.name, host=excluded.host, port=excluded.port,
                gpu_type=excluded.gpu_type, gpu_name=excluded.gpu_name,
                gpu_cores=excluded.gpu_cores, memory_gb=excluded.memory_gb,
                chip=excluded.chip, frameworks=excluded.frameworks,
                metal_support=excluded.metal_support,
                mlx_available=excluded.mlx_available,
                cuda_available=excluded.cuda_available,
                status='online', last_heartbeat=excluded.last_heartbeat
        """, (
            node_id, node.get("name", "unnamed"), node.get("host", "127.0.0.1"),
            node.get("port", 9091), node.get("gpu_type", "cpu"),
            node.get("gpu_name", ""), node.get("gpu_cores", 0),
            node.get("memory_gb", 0), node.get("chip", ""),
            frameworks_json, int(node.get("metal_support", False)),
            int(node.get("mlx_available", False)),
            int(node.get("cuda_available", False)), now,
        ))
        conn.commit()
        return node_id

    def get_nodes(self, status: Optional[str] = None) -> List[Dict]:
        conn = self._get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM nodes WHERE status = ? ORDER BY current_load ASC",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM nodes ORDER BY current_load ASC"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_node(self, node_id: str) -> Optional[Dict]:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM nodes WHERE node_id = ?", (node_id,)
        ).fetchone()
        return dict(row) if row else None

    def heartbeat(self, node_id: str, load: float = 0.0) -> bool:
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "UPDATE nodes SET last_heartbeat = ?, current_load = ?, status = 'online' WHERE node_id = ?",
            (now, load, node_id),
        )
        conn.commit()
        return cur.rowcount > 0

    def mark_stale_nodes(self, timeout_seconds: int = NODE_TIMEOUT):
        """Mark nodes as offline if they haven't sent a heartbeat recently."""
        conn = self._get_conn()
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=timeout_seconds)).isoformat()
        conn.execute(
            "UPDATE nodes SET status = 'offline' WHERE last_heartbeat < ? AND status = 'online'",
            (cutoff,),
        )
        conn.commit()

    def update_node_metrics(self, node_id: str, tasks_completed: int = 0,
                            tasks_failed: int = 0, avg_latency_ms: float = 0.0,
                            throughput_tps: float = 0.0):
        conn = self._get_conn()
        conn.execute("""
            UPDATE nodes SET
                tasks_completed = tasks_completed + ?,
                tasks_failed = tasks_failed + ?,
                avg_latency_ms = ?,
                throughput_tps = ?
            WHERE node_id = ?
        """, (tasks_completed, tasks_failed, avg_latency_ms, throughput_tps, node_id))
        conn.commit()

    # -- Task operations --

    def create_task(self, task_type: str, data: Any, requirements: Dict = None,
                    priority: int = 5) -> str:
        conn = self._get_conn()
        task_id = str(uuid.uuid4())[:16]
        conn.execute(
            "INSERT INTO tasks (task_id, task_type, data, requirements, priority) VALUES (?, ?, ?, ?, ?)",
            (task_id, task_type, json.dumps(data), json.dumps(requirements or {}), priority),
        )
        conn.commit()
        return task_id

    def assign_task(self, task_id: str, node_id: str) -> bool:
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "UPDATE tasks SET assigned_node = ?, status = 'running', started_at = ? "
            "WHERE task_id = ? AND status = 'queued'",
            (node_id, now, task_id),
        )
        conn.commit()
        return cur.rowcount > 0

    def complete_task(self, task_id: str, result: Any = None, error: str = None):
        conn = self._get_conn()
        now = datetime.now(timezone.utc).isoformat()
        status = "failed" if error else "completed"
        conn.execute(
            "UPDATE tasks SET status = ?, result = ?, error = ?, completed_at = ? WHERE task_id = ?",
            (status, json.dumps(result) if result else None, error, now, task_id),
        )
        conn.commit()

    def get_task(self, task_id: str) -> Optional[Dict]:
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        return dict(row) if row else None

    def get_queued_tasks(self, limit: int = 50) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM tasks WHERE status = 'queued' ORDER BY priority ASC, created_at ASC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_running_tasks(self, node_id: str = None) -> List[Dict]:
        conn = self._get_conn()
        if node_id:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status = 'running' AND assigned_node = ?",
                (node_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status = 'running'"
            ).fetchall()
        return [dict(r) for r in rows]

    def record_task_metric(self, task_id: str, node_id: str, execution_ms: float,
                           gpu_utilization: float = 0.0, memory_used_mb: float = 0.0):
        conn = self._get_conn()
        conn.execute(
            "INSERT INTO task_metrics (task_id, node_id, execution_ms, gpu_utilization, memory_used_mb) "
            "VALUES (?, ?, ?, ?, ?)",
            (task_id, node_id, execution_ms, gpu_utilization, memory_used_mb),
        )
        conn.commit()

    def get_pool_stats(self) -> Dict:
        conn = self._get_conn()
        nodes_online = conn.execute(
            "SELECT COUNT(*) FROM nodes WHERE status = 'online'"
        ).fetchone()[0]
        nodes_total = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        tasks_queued = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'queued'"
        ).fetchone()[0]
        tasks_running = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'running'"
        ).fetchone()[0]
        tasks_completed = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'completed'"
        ).fetchone()[0]
        tasks_failed = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'failed'"
        ).fetchone()[0]

        # Aggregate GPU capacity
        gpu_rows = conn.execute(
            "SELECT gpu_type, COUNT(*) as cnt, SUM(gpu_cores) as cores, SUM(memory_gb) as mem "
            "FROM nodes WHERE status = 'online' GROUP BY gpu_type"
        ).fetchall()
        gpu_summary = [dict(r) for r in gpu_rows]

        return {
            "nodes_online": nodes_online,
            "nodes_total": nodes_total,
            "tasks_queued": tasks_queued,
            "tasks_running": tasks_running,
            "tasks_completed": tasks_completed,
            "tasks_failed": tasks_failed,
            "gpu_capacity": gpu_summary,
        }


# ---------------------------------------------------------------------------
# Task dispatcher — picks the best node for a given task
# ---------------------------------------------------------------------------

class TaskDispatcher:
    """Selects the optimal node for a task based on requirements and load."""

    def __init__(self, db: PoolDB):
        self.db = db

    def select_node(self, requirements: Dict) -> Optional[Dict]:
        """Pick the best online node matching the task requirements.

        Requirements can include:
          - gpu_type: "apple_metal", "cuda", "any"
          - min_memory_gb: minimum RAM
          - framework: "mlx", "pytorch", "cuda", "numpy"
          - prefer: "low_latency", "high_throughput", "any"
        """
        nodes = self.db.get_nodes(status="online")
        if not nodes:
            return None

        candidates = []
        req_gpu = requirements.get("gpu_type", "any")
        req_memory = requirements.get("min_memory_gb", 0)
        req_framework = requirements.get("framework", "any")

        for node in nodes:
            # Filter by GPU type
            if req_gpu != "any" and node["gpu_type"] != req_gpu:
                continue

            # Filter by memory
            if node["memory_gb"] < req_memory:
                continue

            # Filter by framework
            if req_framework != "any":
                try:
                    frameworks = json.loads(node["frameworks"])
                except (json.JSONDecodeError, TypeError):
                    frameworks = []
                if req_framework not in frameworks:
                    continue

            candidates.append(node)

        if not candidates:
            # Fallback: return any online node (graceful degradation)
            if nodes:
                logger.warning("No node matches requirements %s — falling back to least-loaded node", requirements)
                return min(nodes, key=lambda n: n["current_load"])
            return None

        # Score candidates: lower load is better, more GPU cores is better
        preference = requirements.get("prefer", "any")

        def score(node):
            s = 0.0
            # Load factor (0-1, lower is better) — weighted heavily
            s -= node["current_load"] * 10.0
            # GPU cores bonus
            s += (node.get("gpu_cores", 0) or 0) * 0.1
            # Memory bonus
            s += (node.get("memory_gb", 0) or 0) * 0.05
            # Latency preference
            if preference == "low_latency":
                avg_lat = node.get("avg_latency_ms", 999) or 999
                s -= avg_lat * 0.01
            elif preference == "high_throughput":
                tps = node.get("throughput_tps", 0) or 0
                s += tps * 0.5
            # MLX bonus for Apple tasks (native acceleration)
            if node.get("mlx_available"):
                s += 2.0
            return s

        best = max(candidates, key=score)
        return best

    def dispatch(self, task_type: str, data: Any, requirements: Dict = None,
                 priority: int = 5) -> Dict:
        """Create a task and assign it to the best available node.

        Returns dict with task_id, assigned_node, status.
        """
        requirements = requirements or {}
        task_id = self.db.create_task(task_type, data, requirements, priority)
        node = self.select_node(requirements)

        if node is None:
            logger.warning("No available nodes — task %s queued", task_id)
            return {"task_id": task_id, "assigned_node": None, "status": "queued"}

        assigned = self.db.assign_task(task_id, node["node_id"])
        if assigned:
            logger.info("Task %s assigned to node %s (%s)", task_id, node["node_id"], node["name"])
            # Bump load estimate on the node
            new_load = min(1.0, node["current_load"] + 0.1)
            self.db.heartbeat(node["node_id"], load=new_load)
            return {"task_id": task_id, "assigned_node": node["node_id"], "status": "running"}
        else:
            return {"task_id": task_id, "assigned_node": None, "status": "queued"}


# ---------------------------------------------------------------------------
# HTTP API server
# ---------------------------------------------------------------------------

class PoolHTTPHandler(BaseHTTPRequestHandler):
    """REST API for the compute pool.

    Endpoints:
      GET  /status                  — pool overview
      GET  /nodes                   — list all nodes
      GET  /nodes/{node_id}         — single node details
      POST /register                — register a new node
      POST /heartbeat               — node heartbeat
      POST /dispatch                — submit a task for execution
      GET  /results/{task_id}       — get task result
      POST /results/{task_id}       — submit task result (from worker)
      GET  /tasks                   — list queued/running tasks
      GET  /health                  — health check
    """

    # Class-level references set by the server
    db: PoolDB = None
    dispatcher: TaskDispatcher = None

    def log_message(self, format, *args):
        logger.debug("HTTP %s", format % args)

    def _send_json(self, data: Any, status: int = 200):
        body = json.dumps(data, indent=2, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> Dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}

    def _parse_path(self):
        parsed = urlparse(self.path)
        return parsed.path.rstrip("/"), parse_qs(parsed.query)

    # -- GET handlers --

    def do_GET(self):
        path, params = self._parse_path()

        if path == "/health":
            self._send_json({"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()})

        elif path == "/status":
            stats = self.db.get_pool_stats()
            stats["server_time"] = datetime.now(timezone.utc).isoformat()
            self._send_json(stats)

        elif path == "/nodes":
            status_filter = params.get("status", [None])[0]
            nodes = self.db.get_nodes(status=status_filter)
            # Parse frameworks JSON for display
            for n in nodes:
                try:
                    n["frameworks"] = json.loads(n["frameworks"])
                except (json.JSONDecodeError, TypeError):
                    n["frameworks"] = []
            self._send_json({"nodes": nodes, "count": len(nodes)})

        elif path.startswith("/nodes/"):
            node_id = path.split("/nodes/")[1]
            node = self.db.get_node(node_id)
            if node:
                try:
                    node["frameworks"] = json.loads(node["frameworks"])
                except (json.JSONDecodeError, TypeError):
                    node["frameworks"] = []
                self._send_json(node)
            else:
                self._send_json({"error": "node not found"}, 404)

        elif path.startswith("/results/"):
            task_id = path.split("/results/")[1]
            task = self.db.get_task(task_id)
            if task:
                # Parse JSON fields
                for field in ("data", "result", "requirements"):
                    if task.get(field) and isinstance(task[field], str):
                        try:
                            task[field] = json.loads(task[field])
                        except json.JSONDecodeError:
                            pass
                self._send_json(task)
            else:
                self._send_json({"error": "task not found"}, 404)

        elif path == "/tasks":
            status_filter = params.get("status", [None])[0]
            if status_filter == "queued":
                tasks = self.db.get_queued_tasks()
            elif status_filter == "running":
                tasks = self.db.get_running_tasks()
            else:
                # Return both queued and running
                tasks = self.db.get_queued_tasks() + self.db.get_running_tasks()
            self._send_json({"tasks": tasks, "count": len(tasks)})

        else:
            self._send_json({"error": "not found", "endpoints": [
                "GET /health", "GET /status", "GET /nodes", "GET /nodes/{id}",
                "POST /register", "POST /heartbeat", "POST /dispatch",
                "GET /results/{task_id}", "POST /results/{task_id}", "GET /tasks",
            ]}, 404)

    # -- POST handlers --

    def do_POST(self):
        path, _ = self._parse_path()
        body = self._read_body()

        if path == "/register":
            self._handle_register(body)
        elif path == "/heartbeat":
            self._handle_heartbeat(body)
        elif path == "/dispatch":
            self._handle_dispatch(body)
        elif path.startswith("/results/"):
            task_id = path.split("/results/")[1]
            self._handle_result_submit(task_id, body)
        else:
            self._send_json({"error": "not found"}, 404)

    # -- OPTIONS for CORS --

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _handle_register(self, body: Dict):
        """Register a compute node with the pool.

        Expected body:
        {
            "name": "my-gpu-box",
            "host": "192.168.1.200",
            "port": 9091,
            "gpu_type": "cuda",         // "apple_metal", "cuda", "cpu"
            "gpu_name": "RTX 4090",
            "gpu_cores": 16384,
            "memory_gb": 24,
            "chip": "Intel i9",
            "frameworks": ["pytorch", "cuda"],
            "metal_support": false,
            "mlx_available": false,
            "cuda_available": true
        }
        """
        required = ["name", "host"]
        missing = [f for f in required if f not in body]
        if missing:
            self._send_json({"error": f"missing required fields: {missing}"}, 400)
            return

        node_id = self.db.register_node(body)
        node = self.db.get_node(node_id)
        logger.info("Node registered: %s (%s) at %s — gpu=%s",
                     body["name"], node_id, body["host"], body.get("gpu_type", "cpu"))
        self._send_json({
            "node_id": node_id,
            "status": "registered",
            "message": f"Welcome to the compute pool, {body['name']}",
            "pool_port": POOL_PORT,
        })

    def _handle_heartbeat(self, body: Dict):
        """Update node heartbeat and load.

        Expected body: {"node_id": "...", "load": 0.3}
        """
        node_id = body.get("node_id")
        if not node_id:
            self._send_json({"error": "node_id required"}, 400)
            return

        load = float(body.get("load", 0.0))
        success = self.db.heartbeat(node_id, load)
        if success:
            # Return any queued tasks for this node
            queued = self.db.get_queued_tasks(limit=5)
            self._send_json({
                "status": "ok",
                "pending_tasks": len(queued),
            })
        else:
            self._send_json({"error": "unknown node_id"}, 404)

    def _handle_dispatch(self, body: Dict):
        """Dispatch a task to the compute pool.

        Expected body:
        {
            "task_type": "anomaly_detection",
            "data": {...},
            "requirements": {"gpu_type": "any", "framework": "mlx"},
            "priority": 5
        }
        """
        task_type = body.get("task_type")
        if not task_type:
            self._send_json({"error": "task_type required"}, 400)
            return

        data = body.get("data", {})
        requirements = body.get("requirements", {})
        priority = int(body.get("priority", 5))

        # Check queue depth
        stats = self.db.get_pool_stats()
        if stats["tasks_queued"] >= MAX_QUEUE_DEPTH:
            self._send_json({"error": "queue full", "queued": stats["tasks_queued"]}, 503)
            return

        result = self.dispatcher.dispatch(task_type, data, requirements, priority)
        self._send_json(result, 201)

    def _handle_result_submit(self, task_id: str, body: Dict):
        """Worker submits a completed task result.

        Expected body:
        {
            "result": {...},
            "error": null,
            "execution_ms": 150.5,
            "gpu_utilization": 0.7,
            "memory_used_mb": 1024
        }
        """
        task = self.db.get_task(task_id)
        if not task:
            self._send_json({"error": "task not found"}, 404)
            return

        result_data = body.get("result")
        error = body.get("error")
        self.db.complete_task(task_id, result_data, error)

        # Record metrics if provided
        node_id = task.get("assigned_node")
        execution_ms = body.get("execution_ms", 0)
        if node_id and execution_ms:
            self.db.record_task_metric(
                task_id, node_id, execution_ms,
                body.get("gpu_utilization", 0),
                body.get("memory_used_mb", 0),
            )
            # Update node aggregate metrics
            if error:
                self.db.update_node_metrics(node_id, tasks_failed=1)
            else:
                self.db.update_node_metrics(
                    node_id, tasks_completed=1,
                    avg_latency_ms=execution_ms,
                    throughput_tps=1000.0 / max(execution_ms, 1),
                )

        self._send_json({"task_id": task_id, "status": "completed" if not error else "failed"})


# ---------------------------------------------------------------------------
# Background threads
# ---------------------------------------------------------------------------

def health_monitor(db: PoolDB, stop_event: threading.Event):
    """Periodically mark stale nodes as offline."""
    while not stop_event.is_set():
        try:
            db.mark_stale_nodes(NODE_TIMEOUT)
        except Exception as e:
            logger.error("Health monitor error: %s", e)
        stop_event.wait(HEALTH_INTERVAL)


def auto_register_local(db: PoolDB):
    """Register this machine as a local compute node on startup."""
    gpu_info = detect_apple_gpu()
    frameworks = detect_frameworks()

    local_node = {
        "node_id": "local",
        "name": f"local-{platform.node()}",
        "host": "127.0.0.1",
        "port": POOL_PORT,
        "gpu_type": gpu_info.get("gpu_type", "cpu"),
        "gpu_name": gpu_info.get("gpu_name", ""),
        "gpu_cores": gpu_info.get("gpu_cores", 0),
        "memory_gb": gpu_info.get("memory_gb", 0),
        "chip": gpu_info.get("chip", ""),
        "frameworks": frameworks,
        "metal_support": gpu_info.get("metal_support", False),
        "mlx_available": gpu_info.get("mlx_available", False),
        "cuda_available": "cuda" in frameworks,
    }

    node_id = db.register_node(local_node)
    logger.info("Local node registered: %s | GPU=%s | Chip=%s | Memory=%.1fGB | Frameworks=%s",
                node_id, gpu_info.get("gpu_type"), gpu_info.get("chip"),
                gpu_info.get("memory_gb", 0), frameworks)
    return local_node


def register_known_nodes(db: PoolDB):
    """Pre-register known Apple Silicon nodes on the local network."""
    known = [
        {
            "node_id": "m1max",
            "name": "m1max-studio",
            "host": "192.168.1.110",
            "port": 9091,
            "gpu_type": "apple_metal",
            "gpu_name": "Apple M1 Max",
            "gpu_cores": 32,
            "memory_gb": 64,
            "chip": "Apple M1 Max",
            "frameworks": ["numpy", "mlx", "pytorch", "mps"],
            "metal_support": True,
            "mlx_available": True,
            "cuda_available": False,
        },
        {
            "node_id": "m2ultra",
            "name": "m2ultra-studio",
            "host": "192.168.1.106",
            "port": 9091,
            "gpu_type": "apple_metal",
            "gpu_name": "Apple M2 Ultra",
            "gpu_cores": 76,
            "memory_gb": 192,
            "chip": "Apple M2 Ultra",
            "frameworks": ["numpy", "mlx", "pytorch", "mps"],
            "metal_support": True,
            "mlx_available": True,
            "cuda_available": False,
        },
    ]

    for node in known:
        db.register_node(node)
        # Mark as offline until they send a heartbeat
        conn = db._get_conn()
        conn.execute("UPDATE nodes SET status = 'pending' WHERE node_id = ?", (node["node_id"],))
        conn.commit()
        logger.info("Known node registered (pending): %s @ %s", node["name"], node["host"])


# ---------------------------------------------------------------------------
# Compute Pool Manager (programmatic interface)
# ---------------------------------------------------------------------------

class ComputePoolManager:
    """High-level interface for interacting with the compute pool.

    Can be used both as a standalone server and as an importable library
    by other agents (e.g., ml_signal_agent.py, live_trader.py).
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db = PoolDB(db_path)
        self.dispatcher = TaskDispatcher(self.db)
        self._stop_event = threading.Event()
        self._server = None
        self._server_thread = None
        self._health_thread = None

    def start(self):
        """Start the HTTP server and background threads."""
        # Register nodes
        auto_register_local(self.db)
        register_known_nodes(self.db)

        # Start health monitor
        self._health_thread = threading.Thread(
            target=health_monitor, args=(self.db, self._stop_event),
            daemon=True, name="health-monitor",
        )
        self._health_thread.start()

        # Start HTTP server
        PoolHTTPHandler.db = self.db
        PoolHTTPHandler.dispatcher = self.dispatcher
        self._server = HTTPServer((POOL_HOST, POOL_PORT), PoolHTTPHandler)
        self._server_thread = threading.Thread(
            target=self._server.serve_forever,
            daemon=True, name="pool-http",
        )
        self._server_thread.start()
        logger.info("Compute pool HTTP server started on %s:%d", POOL_HOST, POOL_PORT)

    def stop(self):
        """Gracefully shut down the pool."""
        self._stop_event.set()
        if self._server:
            self._server.shutdown()
        logger.info("Compute pool shut down")

    def dispatch_task(self, task_type: str, data: Any, requirements: Dict = None,
                      priority: int = 5) -> Dict:
        """Submit a task to the pool (direct Python API, no HTTP needed)."""
        return self.dispatcher.dispatch(task_type, data, requirements, priority)

    def get_result(self, task_id: str) -> Optional[Dict]:
        """Get a task result."""
        return self.db.get_task(task_id)

    def wait_for_result(self, task_id: str, timeout: float = 30.0,
                        poll_interval: float = 0.5) -> Optional[Dict]:
        """Block until a task completes or timeout is reached."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            task = self.db.get_task(task_id)
            if task and task["status"] in ("completed", "failed"):
                if task.get("result") and isinstance(task["result"], str):
                    try:
                        task["result"] = json.loads(task["result"])
                    except json.JSONDecodeError:
                        pass
                return task
            time.sleep(poll_interval)
        return self.db.get_task(task_id)

    def get_online_nodes(self) -> List[Dict]:
        """List all online nodes."""
        return self.db.get_nodes(status="online")

    def get_stats(self) -> Dict:
        """Get pool statistics."""
        return self.db.get_pool_stats()


# ---------------------------------------------------------------------------
# Client helper — for remote agents to call the pool over HTTP
# ---------------------------------------------------------------------------

class ComputePoolClient:
    """HTTP client for remote agents to interact with the compute pool.

    Usage:
        client = ComputePoolClient("http://192.168.1.100:9090")
        client.register({...})
        result = client.dispatch("anomaly_detection", data, requirements)
    """

    def __init__(self, pool_url: str = f"http://127.0.0.1:{POOL_PORT}"):
        self.pool_url = pool_url.rstrip("/")
        self.timeout = 10

    def _request(self, method: str, path: str, data: Dict = None) -> Dict:
        url = f"{self.pool_url}{path}"
        body = json.dumps(data).encode("utf-8") if data else None
        req = urllib.request.Request(
            url, data=body, method=method,
            headers={"Content-Type": "application/json"} if body else {},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8", errors="replace")
            try:
                return json.loads(error_body)
            except json.JSONDecodeError:
                return {"error": str(e), "detail": error_body}
        except urllib.error.URLError as e:
            return {"error": f"connection failed: {e.reason}"}

    def status(self) -> Dict:
        return self._request("GET", "/status")

    def health(self) -> Dict:
        return self._request("GET", "/health")

    def list_nodes(self, status: str = None) -> Dict:
        path = "/nodes"
        if status:
            path += f"?status={status}"
        return self._request("GET", path)

    def register(self, node_info: Dict) -> Dict:
        return self._request("POST", "/register", node_info)

    def heartbeat(self, node_id: str, load: float = 0.0) -> Dict:
        return self._request("POST", "/heartbeat", {"node_id": node_id, "load": load})

    def dispatch(self, task_type: str, data: Any, requirements: Dict = None,
                 priority: int = 5) -> Dict:
        return self._request("POST", "/dispatch", {
            "task_type": task_type,
            "data": data,
            "requirements": requirements or {},
            "priority": priority,
        })

    def get_result(self, task_id: str) -> Dict:
        return self._request("GET", f"/results/{task_id}")

    def submit_result(self, task_id: str, result: Any = None, error: str = None,
                      execution_ms: float = 0, gpu_utilization: float = 0,
                      memory_used_mb: float = 0) -> Dict:
        return self._request("POST", f"/results/{task_id}", {
            "result": result,
            "error": error,
            "execution_ms": execution_ms,
            "gpu_utilization": gpu_utilization,
            "memory_used_mb": memory_used_mb,
        })

    def wait_for_result(self, task_id: str, timeout: float = 30.0,
                        poll_interval: float = 1.0) -> Dict:
        """Poll until task completes or timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            result = self.get_result(task_id)
            if result.get("status") in ("completed", "failed"):
                return result
            time.sleep(poll_interval)
        return self.get_result(task_id)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    """Start the compute pool manager as a standalone server."""
    import signal

    logger.info("=" * 60)
    logger.info("NetTrace Compute Pool Manager")
    logger.info("=" * 60)

    pool = ComputePoolManager()
    pool.start()

    # Print startup summary
    stats = pool.get_stats()
    logger.info("Pool ready: %d nodes registered, listening on port %d",
                stats["nodes_total"], POOL_PORT)

    # Graceful shutdown on SIGINT/SIGTERM
    def shutdown(sig, frame):
        logger.info("Shutting down compute pool...")
        pool.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pool.stop()


if __name__ == "__main__":
    main()
