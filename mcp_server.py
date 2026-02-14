#!/usr/bin/env python3
"""NetTrace MCP Server — stdio transport, WebSocket client to Fly agent, overflow logic."""

import asyncio
import json
import os
import sqlite3
import subprocess
import time
import uuid
import platform
import websocket
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

FLY_AGENT_URL = os.environ.get("FLY_AGENT_URL", "wss://nettrace-dashboard.fly.dev")
MCP_AGENT_SECRET = os.environ.get("MCP_AGENT_SECRET", "")
FLY_APP = os.environ.get("FLY_APP", "nettrace-dashboard")
LOCAL_MAX_SCANS = int(os.environ.get("LOCAL_MAX_SCANS", "3"))
RECONNECT_DELAY = 5  # seconds


def _env_flag(name: str, default: str = "0") -> bool:
    return str(os.environ.get(name, default)).strip().lower() not in {"0", "false", "no", ""}


def _parse_exec_allowlist(raw: str) -> tuple[str, ...]:
    prefixes = []
    for part in str(raw or "").split(","):
        token = part.strip()
        if token:
            prefixes.append(token)
    return tuple(prefixes)


MCP_ENABLE_REMOTE_EXEC = _env_flag("MCP_ENABLE_REMOTE_EXEC", "0")
MCP_ENABLE_FLY_MUTATIONS = _env_flag("MCP_ENABLE_FLY_MUTATIONS", "0")
MCP_EXEC_ALLOWLIST = _parse_exec_allowlist(os.environ.get("MCP_EXEC_ALLOWLIST", ""))


def _exec_command_allowed(command: str) -> bool:
    command = str(command or "").strip()
    if not command or not MCP_EXEC_ALLOWLIST:
        return False
    for prefix in MCP_EXEC_ALLOWLIST:
        if command == prefix or command.startswith(prefix + " "):
            return True
    return False

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

local_active_scans = 0
scan_lock = asyncio.Lock()
pending_requests: dict[str, asyncio.Future] = {}
ws_connection: websocket.WebSocket | None = None
ws_connected = False

# ---------------------------------------------------------------------------
# WebSocket client to Fly agent
# ---------------------------------------------------------------------------


def _ws_url():
    base = FLY_AGENT_URL.rstrip("/")
    return f"{base}/ws/agent"


def ws_connect():
    """Establish WebSocket connection to Fly agent."""
    global ws_connection, ws_connected
    if not MCP_AGENT_SECRET:
        raise RuntimeError("MCP_AGENT_SECRET is required for remote agent connection")
    try:
        ws = websocket.WebSocket()
        ws.settimeout(10)
        ws.connect(_ws_url())
        # Authenticate
        ws.send(json.dumps({"type": "auth", "secret": MCP_AGENT_SECRET}))
        resp = json.loads(ws.recv())
        if resp.get("status") != "ok":
            raise ConnectionError(f"Agent auth failed: {resp}")
        ws.settimeout(30)
        ws_connection = ws
        ws_connected = True
        return ws
    except Exception:
        ws_connected = False
        ws_connection = None
        raise


def ws_ensure():
    """Return existing connection or reconnect."""
    global ws_connection, ws_connected
    if ws_connection and ws_connected:
        try:
            ws_connection.ping()
            return ws_connection
        except Exception:
            ws_connected = False
            ws_connection = None
    return ws_connect()


def ws_send_command(cmd: str, args: dict | None = None, timeout: float = 60) -> dict:
    """Send a command to the Fly agent and wait for the response."""
    ws = ws_ensure()
    req_id = str(uuid.uuid4())
    ws.send(json.dumps({"id": req_id, "cmd": cmd, "args": args or {}}))

    deadline = time.time() + timeout
    while time.time() < deadline:
        ws.settimeout(max(1, deadline - time.time()))
        try:
            raw = ws.recv()
        except websocket.WebSocketTimeoutException:
            continue
        msg = json.loads(raw)
        if msg.get("id") == req_id:
            if msg.get("status") == "error":
                raise RuntimeError(msg.get("data", {}).get("error", "Remote error"))
            return msg.get("data", {})
    raise TimeoutError(f"Agent command '{cmd}' timed out after {timeout}s")


# ---------------------------------------------------------------------------
# Local traceroute
# ---------------------------------------------------------------------------


def run_local_traceroute(host: str, max_hops: int = 20) -> dict:
    """Run traceroute locally via subprocess."""
    try:
        result = subprocess.run(
            ["traceroute", "-m", str(max_hops), "-q", "1", "-w", "2", host],
            capture_output=True, text=True, timeout=60
        )
        return {"output": result.stdout, "error": result.stderr, "returncode": result.returncode}
    except subprocess.TimeoutExpired:
        return {"output": "", "error": "Traceroute timed out", "returncode": -1}
    except FileNotFoundError:
        return {"output": "", "error": "traceroute not installed locally", "returncode": -1}


# ---------------------------------------------------------------------------
# Fly CLI helpers
# ---------------------------------------------------------------------------


def fly_cmd(args: list[str], timeout: int = 30) -> dict:
    """Run a fly CLI command and return stdout/stderr."""
    try:
        result = subprocess.run(
            ["fly", *args, "-a", FLY_APP],
            capture_output=True, text=True, timeout=timeout
        )
        return {"stdout": result.stdout, "stderr": result.stderr, "returncode": result.returncode}
    except FileNotFoundError:
        return {"stdout": "", "stderr": "fly CLI not found", "returncode": -1}
    except subprocess.TimeoutExpired:
        return {"stdout": "", "stderr": "Command timed out", "returncode": -1}


# ---------------------------------------------------------------------------
# Meta-engine DB helpers (read-only, graceful on missing DB)
# ---------------------------------------------------------------------------

def _meta_db_path() -> str:
    """Resolve meta_engine.db path: /app/agents/ on Fly, agents/ locally."""
    fly_path = "/app/agents/meta_engine.db"
    local_path = os.path.join(os.path.dirname(__file__), "agents", "meta_engine.db")
    if os.path.exists(fly_path):
        return fly_path
    return local_path


def _meta_engine_read(query: str, params: tuple = ()) -> list[dict]:
    """Execute a read-only query against meta_engine.db. Returns list of dicts."""
    db_path = _meta_db_path()
    if not os.path.exists(db_path):
        return []
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()
        result = [dict(r) for r in rows]
        conn.close()
        return result
    except (sqlite3.OperationalError, sqlite3.DatabaseError):
        return []


def meta_engine_get_status() -> dict:
    """Full meta-engine status: agents, ideas, predictions, evolution log."""
    agents = _meta_engine_read("""
        SELECT name, strategy_type, status, sharpe_ratio, trades, wins, losses,
               total_pnl, max_drawdown, last_active, created_at
        FROM meta_agents ORDER BY sharpe_ratio DESC
    """)
    ideas = _meta_engine_read("""
        SELECT source, idea_type, description, confidence, status, created_at
        FROM meta_ideas ORDER BY id DESC LIMIT 20
    """)
    predictions = _meta_engine_read("""
        SELECT pair, direction, confidence, entry_price, current_price,
               paper_pnl, status, created_at
        FROM meta_paper_trades ORDER BY id DESC LIMIT 20
    """)
    evolution = _meta_engine_read("""
        SELECT action, details, created_at FROM meta_evolution_log
        ORDER BY id DESC LIMIT 20
    """)
    cycle_rows = _meta_engine_read("SELECT COUNT(*) as cnt FROM meta_evolution_log")
    cycle_count = cycle_rows[0]["cnt"] if cycle_rows else 0

    return {
        "agents": agents,
        "recent_ideas": ideas,
        "recent_predictions": predictions,
        "evolution_log": evolution,
        "cycle_count": cycle_count,
    }


def meta_engine_get_agents() -> list[dict]:
    """All agents with performance metrics."""
    return _meta_engine_read("""
        SELECT name, strategy_type, status, sharpe_ratio, trades, wins, losses,
               total_pnl, max_drawdown, last_active, created_at
        FROM meta_agents ORDER BY sharpe_ratio DESC
    """)


def meta_engine_get_predictions() -> list[dict]:
    """Latest ML predictions (paper trades) for all pairs."""
    return _meta_engine_read("""
        SELECT pair, direction, confidence, entry_price, current_price,
               paper_pnl, status, agent_name, created_at
        FROM meta_paper_trades ORDER BY id DESC LIMIT 50
    """)


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

server = Server("nettrace")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="scan",
            description="Run traceroute to a host. Runs locally first, overflows to Fly VM if overloaded.",
            inputSchema={
                "type": "object",
                "properties": {
                    "host": {"type": "string", "description": "Target hostname or IP"},
                    "max_hops": {"type": "integer", "description": "Max hops (default 20)", "default": 20},
                },
                "required": ["host"],
            },
        ),
        Tool(
            name="scan_remote",
            description="Force a traceroute scan on the Fly VM (remote only).",
            inputSchema={
                "type": "object",
                "properties": {
                    "host": {"type": "string", "description": "Target hostname or IP"},
                },
                "required": ["host"],
            },
        ),
        Tool(
            name="status",
            description="Get local + remote load: active scans, CPU, memory.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="machines",
            description="List Fly machines, their status and regions.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="scale",
            description="Scale Fly machines up or down (disabled unless MCP_ENABLE_FLY_MUTATIONS=1).",
            inputSchema={
                "type": "object",
                "properties": {
                    "count": {"type": "integer", "description": "Target machine count"},
                },
                "required": ["count"],
            },
        ),
        Tool(
            name="deploy",
            description="Trigger fly deploy for the app (disabled unless MCP_ENABLE_FLY_MUTATIONS=1).",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="logs",
            description="Stream recent Fly logs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "lines": {"type": "integer", "description": "Number of log lines (default 50)", "default": 50},
                },
            },
        ),
        Tool(
            name="exec",
            description="Run a command on the Fly VM via the agent (disabled unless MCP_ENABLE_REMOTE_EXEC=1 + allowlist).",
            inputSchema={
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "Shell command to execute"},
                },
                "required": ["command"],
            },
        ),
        Tool(
            name="meta_engine.get_status",
            description="Get full meta-engine status: agent pool, recent ideas, predictions, evolution log, and cycle count.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="meta_engine.get_agents",
            description="Get all meta-engine agents with performance metrics (Sharpe, trades, wins, PnL, status).",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="meta_engine.get_predictions",
            description="Get latest ML predictions (paper trades) for all pairs from the meta-engine.",
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    global local_active_scans

    # ------ scan (local first, overflow to remote) ------
    if name == "scan":
        host = arguments["host"]
        max_hops = arguments.get("max_hops", 20)

        run_remote = False
        async with scan_lock:
            if local_active_scans >= LOCAL_MAX_SCANS:
                run_remote = True
            else:
                local_active_scans += 1

        if run_remote:
            # Overflow to Fly agent
            try:
                data = ws_send_command("scan", {"host": host})
                return [TextContent(type="text", text=f"[remote scan]\n{json.dumps(data, indent=2)}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Remote scan failed: {e}")]
        else:
            try:
                result = await asyncio.to_thread(run_local_traceroute, host, max_hops)
                return [TextContent(type="text", text=f"[local scan]\n{result['output'] or result['error']}")]
            finally:
                async with scan_lock:
                    local_active_scans = max(0, local_active_scans - 1)

    # ------ scan_remote ------
    elif name == "scan_remote":
        host = arguments["host"]
        try:
            data = ws_send_command("scan", {"host": host})
            return [TextContent(type="text", text=f"[remote scan]\n{json.dumps(data, indent=2)}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Remote scan failed: {e}")]

    # ------ status ------
    elif name == "status":
        local_load = os.getloadavg()
        local_info = {
            "active_scans": local_active_scans,
            "max_scans": LOCAL_MAX_SCANS,
            "load_avg": list(local_load),
            "platform": platform.platform(),
        }
        try:
            remote_info = ws_send_command("status", timeout=10)
        except Exception as e:
            remote_info = {"error": str(e)}
        combined = {"local": local_info, "remote": remote_info}
        return [TextContent(type="text", text=json.dumps(combined, indent=2))]

    # ------ machines ------
    elif name == "machines":
        result = await asyncio.to_thread(fly_cmd, ["machine", "list"])
        output = result["stdout"] or result["stderr"]
        return [TextContent(type="text", text=output)]

    # ------ scale ------
    elif name == "scale":
        if not MCP_ENABLE_FLY_MUTATIONS:
            return [TextContent(type="text", text="Scale is disabled. Set MCP_ENABLE_FLY_MUTATIONS=1.")]
        count = arguments["count"]
        result = await asyncio.to_thread(fly_cmd, ["scale", "count", str(count)])
        output = result["stdout"] or result["stderr"]
        return [TextContent(type="text", text=output)]

    # ------ deploy ------
    elif name == "deploy":
        if not MCP_ENABLE_FLY_MUTATIONS:
            return [TextContent(type="text", text="Deploy is disabled. Set MCP_ENABLE_FLY_MUTATIONS=1.")]
        result = await asyncio.to_thread(fly_cmd, ["deploy"], timeout=300)
        output = result["stdout"] or result["stderr"]
        return [TextContent(type="text", text=output)]

    # ------ logs ------
    elif name == "logs":
        lines = arguments.get("lines", 50)
        result = await asyncio.to_thread(fly_cmd, ["logs", "--no-tail", "-n", str(lines)])
        output = result["stdout"] or result["stderr"]
        return [TextContent(type="text", text=output)]

    # ------ exec ------
    elif name == "exec":
        command = arguments["command"]
        if not MCP_ENABLE_REMOTE_EXEC:
            return [TextContent(type="text", text="Remote exec disabled. Set MCP_ENABLE_REMOTE_EXEC=1.")]
        if not _exec_command_allowed(command):
            return [TextContent(type="text", text="Command rejected by MCP_EXEC_ALLOWLIST policy.")]
        try:
            data = ws_send_command("exec", {"command": command}, timeout=30)
            return [TextContent(type="text", text=json.dumps(data, indent=2))]
        except Exception as e:
            return [TextContent(type="text", text=f"Remote exec failed: {e}")]

    # ------ meta_engine.get_status ------
    elif name == "meta_engine.get_status":
        data = await asyncio.to_thread(meta_engine_get_status)
        return [TextContent(type="text", text=json.dumps(data, indent=2, default=str))]

    # ------ meta_engine.get_agents ------
    elif name == "meta_engine.get_agents":
        data = await asyncio.to_thread(meta_engine_get_agents)
        return [TextContent(type="text", text=json.dumps(data, indent=2, default=str))]

    # ------ meta_engine.get_predictions ------
    elif name == "meta_engine.get_predictions":
        data = await asyncio.to_thread(meta_engine_get_predictions)
        return [TextContent(type="text", text=json.dumps(data, indent=2, default=str))]

    return [TextContent(type="text", text=f"Unknown tool: {name}")]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
