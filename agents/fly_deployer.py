#!/usr/bin/env python3
"""Fly.io Edge Deployment Manifest + Agent Assistants.

Maps trading agents to Fly.io regions for proximity-optimized execution:
  - ewr (NYC):       Coordinator, capital_allocator, advanced_team (Coinbase/US exchanges)
  - lhr (London):    exchange_scanner (LSE, EEX), forex_agent
  - nrt (Tokyo):     latency_arb (Asian exchanges), momentum_scalper
  - fra (Frankfurt): exchange_scanner backup, risk monitoring
  - sin (Singapore): latency_arb backup, Asian session coverage
  - ord (Chicago):   CME/NYMEX proximity
  - bom (Mumbai):    DGCX monitoring

Each agent gets a lightweight AgentAssistant that:
  - Monitors health (alive, crashed, error logs)
  - Tracks hourly P&L and trade metrics
  - Restarts crashed agents (respects restart limits)
  - Reports status to the message bus for coordinator visibility

Usage:
  python fly_deployer.py              # Print deployment plan
  python fly_deployer.py --region ewr # Show agents for a specific region

Import from orchestrator_v2:
  from fly_deployer import get_region, get_agents_for_region, AgentAssistant
"""

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure agents/ is on sys.path for imports
_AGENTS_DIR = str(Path(__file__).resolve().parent)
sys.path.insert(0, _AGENTS_DIR)

logger = logging.getLogger("fly_deployer")

# ---------------------------------------------------------------------------
# Region Detection
# ---------------------------------------------------------------------------

FLY_REGIONS = ["ewr", "ord", "lhr", "fra", "nrt", "sin", "bom"]

# Fly.io sets FLY_REGION env var on every machine automatically
def get_region() -> str:
    """Detect the current Fly.io region, or 'local' if not on Fly."""
    return os.environ.get("FLY_REGION", "local")


def get_machine_id() -> str:
    """Get the Fly.io machine ID, or hostname if local."""
    return os.environ.get("FLY_MACHINE_ID", os.environ.get("HOSTNAME", "local"))


# ---------------------------------------------------------------------------
# Deployment Manifest — which agents run WHERE
# ---------------------------------------------------------------------------

# Each region entry:
#   "agents": list of agent names that should run here (matches AGENT_CONFIGS in orchestrator_v2)
#   "role": human-readable purpose
#   "priority": 1 = primary (always run), 2 = secondary (run if resources allow)
#   "exchanges": nearby exchanges this region has latency advantage for
#   "notes": operational notes

DEPLOYMENT_MANIFEST = {
    "ewr": {
        "role": "Primary Coordinator (US East)",
        "agents": [
            {"name": "capital_allocator",  "priority": 1, "reason": "Treasury mgmt must be near Coinbase (US)"},
            {"name": "advanced_team",      "priority": 1, "reason": "5-agent research/strategy team, primary brain"},
            {"name": "sniper",             "priority": 1, "reason": "High-confidence execution near Coinbase API"},
            {"name": "meta_engine",        "priority": 1, "reason": "Strategy evolution, needs full market view"},
        ],
        "exchanges": ["coinbase", "gemini", "kraken_us"],
        "notes": "Primary region. Coordinator lives here. All capital allocation decisions originate from ewr.",
    },
    "ord": {
        "role": "CME/NYMEX Proximity (US Central)",
        "agents": [
            {"name": "exchange_scanner",   "priority": 1, "reason": "CME, NYMEX, CBOT futures data proximity"},
            {"name": "momentum_scalper",   "priority": 2, "reason": "Scalp CME micro-futures correlation signals"},
        ],
        "exchanges": ["cme", "nymex", "cbot", "cboe"],
        "notes": "Chicago data center proximity. Futures correlation signals fed to ewr coordinator.",
    },
    "lhr": {
        "role": "European Exchange Hub (London)",
        "agents": [
            {"name": "exchange_scanner",   "priority": 1, "reason": "LSE, EEX, Eurex monitoring"},
            {"name": "forex_arb",          "priority": 1, "reason": "London FX session (largest forex volume)"},
        ],
        "exchanges": ["lse", "eex", "eurex", "ice_europe", "bitstamp"],
        "notes": "London forex session 08:00-16:00 GMT. Highest FX volume globally.",
    },
    "fra": {
        "role": "European Backup + Risk Monitor (Frankfurt)",
        "agents": [
            {"name": "exchange_scanner",   "priority": 2, "reason": "Eurex/Deutsche Borse backup scanner"},
            {"name": "forex_arb",          "priority": 2, "reason": "Backup forex during London session"},
        ],
        "exchanges": ["eurex", "deutsche_borse", "xetra"],
        "notes": "Backup for lhr. If lhr goes down, fra takes over European scanning. Also independent risk monitor.",
    },
    "nrt": {
        "role": "Asian Exchange Primary (Tokyo)",
        "agents": [
            {"name": "latency_arb",        "priority": 1, "reason": "Asian exchange arb (bitFlyer, Liquid, GMO)"},
            {"name": "momentum_scalper",    "priority": 1, "reason": "Asian session volatility scalping"},
        ],
        "exchanges": ["bitflyer", "liquid", "gmo_coin", "tse", "ose"],
        "notes": "Tokyo/Asian session 00:00-09:00 UTC. Japanese exchanges often lead Asian price action.",
    },
    "sin": {
        "role": "Asian Exchange Backup (Singapore)",
        "agents": [
            {"name": "latency_arb",        "priority": 2, "reason": "Southeast Asian exchange coverage (SGX, Binance SG)"},
            {"name": "exchange_scanner",    "priority": 2, "reason": "SGX, regional exchange monitoring"},
        ],
        "exchanges": ["sgx", "binance", "bybit", "okx"],
        "notes": "Backup for nrt. Covers Binance (SG-proximate) and SGX. Takes over if nrt fails.",
    },
    "bom": {
        "role": "DGCX + India Market Monitor (Mumbai)",
        "agents": [
            {"name": "exchange_scanner",   "priority": 1, "reason": "DGCX (Dubai Gold/Commodity), NSE/BSE monitoring"},
            {"name": "forex_arb",          "priority": 2, "reason": "INR forex pairs, DGCX gold-crypto correlation"},
        ],
        "exchanges": ["dgcx", "nse", "bse", "mcx"],
        "notes": "Dubai/India market hours. DGCX gold futures often lead Asian gold/BTC correlation.",
    },
}


def get_agents_for_region(region: str) -> list:
    """Return list of agent configs that should run in the given region.

    Args:
        region: Fly.io region code (ewr, ord, lhr, etc.) or 'local'

    Returns:
        List of dicts with keys: name, priority, reason
        If region is 'local' or unknown, returns ALL enabled agents (dev mode).
    """
    if region in DEPLOYMENT_MANIFEST:
        return DEPLOYMENT_MANIFEST[region]["agents"]

    # Local / unknown region: return all agents (full dev mode)
    # This matches orchestrator_v2 behavior when running locally
    return []


def get_region_info(region: str) -> dict:
    """Return full region metadata."""
    return DEPLOYMENT_MANIFEST.get(region, {})


def should_agent_run_here(agent_name: str, region: str = None) -> bool:
    """Check if a specific agent should run in the current (or given) region.

    Returns True if:
      - We're running locally (not on Fly) -- run everything
      - The agent is in the manifest for this region
    """
    if region is None:
        region = get_region()

    if region == "local":
        return True  # Local dev: run everything

    agents = get_agents_for_region(region)
    return any(a["name"] == agent_name for a in agents)


# ---------------------------------------------------------------------------
# Agent Assistant — lightweight monitor per agent
# ---------------------------------------------------------------------------

class AgentAssistant:
    """Lightweight assistant that monitors a single trading agent.

    Each agent gets one assistant. The assistant:
      - Watches the agent process (alive/crashed)
      - Tracks P&L per hour from the agent's trade log
      - Logs performance metrics (trades, win_rate, uptime)
      - Can restart the agent on crash
      - Reports status to the message bus
    """

    MAX_RESTARTS = 3           # max restarts per hour
    RESTART_WINDOW = 3600      # 1 hour window for restart counting
    METRIC_INTERVAL = 60       # collect metrics every 60 seconds

    def __init__(self, agent_name: str, agent_process=None, agent_config: dict = None):
        """
        Args:
            agent_name: Name matching AGENT_CONFIGS (e.g. 'latency_arb')
            agent_process: subprocess.Popen object (or None if not yet started)
            agent_config: Config dict from AGENT_CONFIGS (script, args, etc.)
        """
        self.agent_name = agent_name
        self.process = agent_process
        self.config = agent_config or {}
        self.region = get_region()

        # Tracking state
        self.started_at = time.time() if agent_process else None
        self.restart_times = []     # timestamps of recent restarts
        self.total_restarts = 0

        # Performance metrics (accumulated)
        self.metrics = {
            "trades_total": 0,
            "trades_won": 0,
            "trades_lost": 0,
            "pnl_usd": 0.0,
            "pnl_last_hour": 0.0,
            "fees_paid": 0.0,
            "last_trade_at": None,
            "uptime_seconds": 0,
            "crashes": 0,
            "error_count": 0,
            "last_error": None,
        }

        # Hourly P&L tracking
        self._hour_start_pnl = 0.0
        self._hour_start_time = time.time()
        self._last_metric_check = 0

        # Message bus (lazy init to avoid import errors if bus not available)
        self._bus = None

        logger.info("Assistant created for agent '%s' in region '%s'",
                     agent_name, self.region)

    @property
    def bus(self):
        """Lazy-load message bus."""
        if self._bus is None:
            try:
                from advanced_team.message_bus import MessageBus
                self._bus = MessageBus()
            except ImportError:
                logger.debug("Message bus not available (advanced_team not installed)")
        return self._bus

    # ------------------------------------------------------------------
    # Core monitoring
    # ------------------------------------------------------------------

    def is_alive(self) -> bool:
        """Check if the agent process is still running."""
        if self.process is None:
            return False
        return self.process.poll() is None

    def monitor(self) -> dict:
        """Run a full health check on the agent.

        Returns:
            dict with keys: alive, pid, uptime, exit_code, errors
        """
        result = {
            "agent": self.agent_name,
            "region": self.region,
            "alive": False,
            "pid": None,
            "uptime": 0,
            "exit_code": None,
            "errors": [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if self.process is None:
            result["errors"].append("No process attached")
            return result

        result["alive"] = self.is_alive()
        result["pid"] = self.process.pid

        if self.started_at:
            result["uptime"] = int(time.time() - self.started_at)
            self.metrics["uptime_seconds"] = result["uptime"]

        if not result["alive"]:
            result["exit_code"] = self.process.returncode
            self.metrics["crashes"] += 1

        # Check agent's stdout log for recent errors
        log_errors = self._scan_log_for_errors()
        if log_errors:
            result["errors"] = log_errors
            self.metrics["error_count"] += len(log_errors)
            self.metrics["last_error"] = log_errors[-1]

        # Update hourly P&L if enough time has passed
        now = time.time()
        if now - self._hour_start_time >= 3600:
            self.metrics["pnl_last_hour"] = self.metrics["pnl_usd"] - self._hour_start_pnl
            self._hour_start_pnl = self.metrics["pnl_usd"]
            self._hour_start_time = now

        return result

    def _scan_log_for_errors(self, tail_lines: int = 50) -> list:
        """Scan the agent's stdout log for recent ERROR/CRITICAL lines."""
        log_path = os.path.join(_AGENTS_DIR, f"{self.agent_name}_stdout.log")
        errors = []

        if not os.path.exists(log_path):
            return errors

        try:
            with open(log_path, "rb") as f:
                # Seek to end, read last ~8KB for efficiency
                f.seek(0, 2)
                size = f.tell()
                read_size = min(size, 8192)
                f.seek(max(0, size - read_size))
                tail = f.read().decode("utf-8", errors="replace")

            lines = tail.strip().split("\n")
            for line in lines[-tail_lines:]:
                upper = line.upper()
                if "ERROR" in upper or "CRITICAL" in upper or "EXCEPTION" in upper:
                    errors.append(line.strip()[:200])  # cap at 200 chars

        except Exception as e:
            logger.debug("Could not scan log for %s: %s", self.agent_name, e)

        return errors[-5:]  # return at most 5 recent errors

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def record_trade(self, pnl_usd: float, fees: float = 0.0, won: bool = True):
        """Record a completed trade (called by agent or parsed from logs).

        Args:
            pnl_usd: P&L for this trade in USD (negative for loss)
            fees: Fees paid for this trade
            won: Whether the trade was profitable
        """
        self.metrics["trades_total"] += 1
        if won:
            self.metrics["trades_won"] += 1
        else:
            self.metrics["trades_lost"] += 1
        self.metrics["pnl_usd"] += pnl_usd
        self.metrics["fees_paid"] += fees
        self.metrics["last_trade_at"] = datetime.now(timezone.utc).isoformat()

    def get_metrics(self) -> dict:
        """Return current performance metrics.

        Returns:
            dict with: pnl_usd, pnl_last_hour, trades_total, win_rate,
                       uptime_seconds, crashes, error_count, last_error
        """
        m = dict(self.metrics)

        # Calculate win rate
        if m["trades_total"] > 0:
            m["win_rate"] = round(m["trades_won"] / m["trades_total"], 4)
        else:
            m["win_rate"] = 0.0

        # Update uptime
        if self.started_at and self.is_alive():
            m["uptime_seconds"] = int(time.time() - self.started_at)

        m["agent"] = self.agent_name
        m["region"] = self.region
        m["total_restarts"] = self.total_restarts
        m["timestamp"] = datetime.now(timezone.utc).isoformat()

        return m

    # ------------------------------------------------------------------
    # Restart logic
    # ------------------------------------------------------------------

    def _can_restart(self) -> bool:
        """Check if we haven't exceeded restart limit."""
        now = time.time()
        # Prune old restart timestamps
        self.restart_times = [t for t in self.restart_times
                              if now - t < self.RESTART_WINDOW]
        return len(self.restart_times) < self.MAX_RESTARTS

    def restart(self) -> bool:
        """Kill and restart the agent process.

        Returns:
            True if restart succeeded, False if limit exceeded or error
        """
        if not self._can_restart():
            logger.error("Agent '%s' exceeded restart limit (%d/%dhr). Refusing restart.",
                         self.agent_name, len(self.restart_times), self.MAX_RESTARTS)
            return False

        # Kill existing process if still running
        if self.process and self.process.poll() is None:
            logger.info("Killing agent '%s' (PID %d) for restart",
                        self.agent_name, self.process.pid)
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()

        # Start new process
        script = self.config.get("script", f"{self.agent_name}.py")
        script_path = os.path.join(_AGENTS_DIR, script)

        if not os.path.exists(script_path):
            logger.error("Cannot restart '%s': script not found at %s",
                         self.agent_name, script_path)
            return False

        cmd = [sys.executable, script_path] + self.config.get("args", [])

        try:
            log_path = os.path.join(_AGENTS_DIR, f"{self.agent_name}_stdout.log")
            log_file = open(log_path, "a")

            self.process = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=_AGENTS_DIR,
                start_new_session=True,
            )

            self.started_at = time.time()
            self.restart_times.append(time.time())
            self.total_restarts += 1

            logger.info("Restarted agent '%s' (new PID %d, restart #%d)",
                        self.agent_name, self.process.pid, self.total_restarts)
            return True

        except Exception as e:
            logger.error("Failed to restart agent '%s': %s", self.agent_name, e)
            return False

    # ------------------------------------------------------------------
    # Reporting — push status to message bus
    # ------------------------------------------------------------------

    def report(self) -> dict:
        """Push agent status to the message bus and return the report.

        Publishes an 'agent_status_report' message to the coordinator.
        """
        health = self.monitor()
        metrics = self.get_metrics()

        report_data = {
            "health": health,
            "metrics": metrics,
            "region": self.region,
            "machine_id": get_machine_id(),
        }

        # Publish to message bus if available
        if self.bus is not None:
            try:
                self.bus.publish(
                    sender=f"assistant:{self.agent_name}",
                    recipient="coordinator",
                    msg_type="agent_status_report",
                    payload=report_data,
                )
            except Exception as e:
                logger.debug("Could not publish to bus: %s", e)

        return report_data

    def __repr__(self):
        alive = self.is_alive()
        pid = self.process.pid if self.process else None
        return (f"<AgentAssistant({self.agent_name}) "
                f"region={self.region} alive={alive} pid={pid} "
                f"restarts={self.total_restarts}>")


# ---------------------------------------------------------------------------
# Deployment Plan Helpers
# ---------------------------------------------------------------------------

def get_deployment_plan() -> dict:
    """Build a structured deployment plan across all regions.

    Returns dict keyed by region with agent list, role, and resource estimates.
    """
    plan = {}
    for region, config in DEPLOYMENT_MANIFEST.items():
        agent_names = [a["name"] for a in config["agents"]]
        primary_count = sum(1 for a in config["agents"] if a["priority"] == 1)
        backup_count = sum(1 for a in config["agents"] if a["priority"] == 2)

        plan[region] = {
            "role": config["role"],
            "agents": config["agents"],
            "agent_count": len(agent_names),
            "primary_agents": primary_count,
            "backup_agents": backup_count,
            "exchanges": config.get("exchanges", []),
            "notes": config.get("notes", ""),
            # Resource estimate: each agent + assistant ~ 50MB RAM, 0.1 CPU
            "estimated_ram_mb": len(agent_names) * 50 + 64,  # +64 for OS overhead
            "vm_recommendation": "shared-cpu-1x" if len(agent_names) <= 2 else "shared-cpu-2x",
        }

    return plan


def get_full_agent_roster() -> dict:
    """Get a mapping of agent_name -> list of regions it runs in."""
    roster = {}
    for region, config in DEPLOYMENT_MANIFEST.items():
        for agent in config["agents"]:
            name = agent["name"]
            if name not in roster:
                roster[name] = []
            roster[name].append({
                "region": region,
                "priority": agent["priority"],
                "reason": agent["reason"],
            })
    return roster


def create_assistants_for_region(region: str = None, agent_processes: dict = None) -> dict:
    """Create AgentAssistant instances for all agents in the current region.

    Args:
        region: Override region (default: auto-detect from FLY_REGION)
        agent_processes: dict of agent_name -> Popen process (from orchestrator)

    Returns:
        dict of agent_name -> AgentAssistant
    """
    if region is None:
        region = get_region()

    if agent_processes is None:
        agent_processes = {}

    agents_here = get_agents_for_region(region)
    assistants = {}

    for agent_def in agents_here:
        name = agent_def["name"]
        proc = agent_processes.get(name)

        # Build config from orchestrator's AGENT_CONFIGS if available
        config = _find_agent_config(name)

        assistant = AgentAssistant(
            agent_name=name,
            agent_process=proc,
            agent_config=config,
        )
        assistants[name] = assistant

    return assistants


def _find_agent_config(agent_name: str) -> dict:
    """Look up agent config from orchestrator_v2 AGENT_CONFIGS."""
    try:
        from orchestrator_v2 import AGENT_CONFIGS
        for cfg in AGENT_CONFIGS:
            if cfg["name"] == agent_name:
                return cfg
    except ImportError:
        pass

    # Fallback: minimal config
    return {"name": agent_name, "script": f"{agent_name}.py", "args": []}


# ---------------------------------------------------------------------------
# Pretty Print
# ---------------------------------------------------------------------------

def print_deployment_plan(filter_region: str = None):
    """Print the full deployment plan to stdout."""
    plan = get_deployment_plan()
    current = get_region()

    print()
    print("=" * 76)
    print("  FLY.IO EDGE DEPLOYMENT PLAN — NETTRACE TRADING NETWORK")
    print(f"  Current region: {current} | Machine: {get_machine_id()}")
    print("=" * 76)

    regions_to_show = [filter_region] if filter_region else FLY_REGIONS

    total_agents = 0
    total_ram = 0

    for region in regions_to_show:
        if region not in plan:
            print(f"\n  {region}: (not in manifest)")
            continue

        info = plan[region]
        marker = " <-- YOU ARE HERE" if region == current else ""
        print(f"\n  [{region.upper()}] {info['role']}{marker}")
        print(f"  {'~' * 60}")
        print(f"  Exchanges: {', '.join(info['exchanges'])}")
        print(f"  VM: {info['vm_recommendation']} (~{info['estimated_ram_mb']}MB)")
        print(f"  Agents ({info['primary_agents']} primary, {info['backup_agents']} backup):")

        for agent in info["agents"]:
            pri_label = "PRI" if agent["priority"] == 1 else "BAK"
            print(f"    [{pri_label}] {agent['name']:<22} {agent['reason']}")

        if info["notes"]:
            print(f"  Note: {info['notes']}")

        total_agents += info["agent_count"]
        total_ram += info["estimated_ram_mb"]

    print(f"\n{'=' * 76}")
    print(f"  TOTALS: {total_agents} agent instances across {len(plan)} regions")
    print(f"  Estimated RAM: ~{total_ram}MB total ({total_ram // len(plan)}MB avg/region)")

    # Show agent roster (which agents are redundant across regions)
    roster = get_full_agent_roster()
    print(f"\n  AGENT REDUNDANCY MAP:")
    for name, deployments in sorted(roster.items()):
        regions_str = ", ".join(
            f"{d['region']}({'PRI' if d['priority'] == 1 else 'BAK'})"
            for d in deployments
        )
        print(f"    {name:<22} -> {regions_str}")

    print(f"\n  FAILOVER PAIRS:")
    print(f"    lhr (London)    <-> fra (Frankfurt)   European exchange scanning")
    print(f"    nrt (Tokyo)     <-> sin (Singapore)   Asian exchange arb")
    print(f"    ewr (NYC)       <-> ord (Chicago)     US exchange execution")
    print(f"{'=' * 76}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    filter_region = None
    if len(sys.argv) > 1:
        if sys.argv[1] == "--region" and len(sys.argv) > 2:
            filter_region = sys.argv[2].lower()
            if filter_region not in DEPLOYMENT_MANIFEST:
                print(f"Unknown region '{filter_region}'. Valid: {', '.join(FLY_REGIONS)}")
                sys.exit(1)
        elif sys.argv[1] == "--json":
            print(json.dumps(get_deployment_plan(), indent=2))
            sys.exit(0)
        elif sys.argv[1] == "--roster":
            print(json.dumps(get_full_agent_roster(), indent=2))
            sys.exit(0)
        elif sys.argv[1] == "--help":
            print("Usage: fly_deployer.py [--region <code>] [--json] [--roster]")
            sys.exit(0)
        else:
            print(f"Unknown argument: {sys.argv[1]}")
            print("Usage: fly_deployer.py [--region <code>] [--json] [--roster]")
            sys.exit(1)

    print_deployment_plan(filter_region)

    # Demo: create assistants for current region
    current = get_region()
    if current != "local":
        print(f"Creating assistants for region '{current}'...")
        assistants = create_assistants_for_region()
        for name, asst in assistants.items():
            print(f"  {asst}")
    else:
        print("Running locally -- assistants would be created per-region on Fly.io")
        print("Demo: creating assistant for 'sniper' agent...")
        demo = AgentAssistant("sniper", agent_config={"name": "sniper", "script": "sniper.py", "args": []})
        print(f"  {demo}")
        print(f"  Metrics: {json.dumps(demo.get_metrics(), indent=2)}")
