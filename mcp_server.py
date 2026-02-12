#!/usr/bin/env python3
"""NetTrace MCP Server — stdio transport, WebSocket client to Fly agent, overflow logic."""

import asyncio
import json
import os
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
            description="Scale Fly machines up or down.",
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
            description="Trigger fly deploy for the app.",
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
            description="Run a command on the Fly VM via the agent.",
            inputSchema={
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "Shell command to execute"},
                },
                "required": ["command"],
            },
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
        count = arguments["count"]
        result = await asyncio.to_thread(fly_cmd, ["scale", "count", str(count)])
        output = result["stdout"] or result["stderr"]
        return [TextContent(type="text", text=output)]

    # ------ deploy ------
    elif name == "deploy":
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
        try:
            data = ws_send_command("exec", {"command": command}, timeout=30)
            return [TextContent(type="text", text=json.dumps(data, indent=2))]
        except Exception as e:
            return [TextContent(type="text", text=f"Remote exec failed: {e}")]

    return [TextContent(type="text", text=f"Unknown tool: {name}")]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
