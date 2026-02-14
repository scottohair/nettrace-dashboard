#!/usr/bin/env python3
"""Fly Agent Runner — in-process agent execution for Fly.io containers.

Runs trading agents as daemon threads inside the Flask container. No subprocess.Popen
(unreliable in 256MB containers). Each region runs agents appropriate to its role:

  ewr (primary): sniper, meta_engine, advanced_team, capital_allocator
  lhr/nrt/sin (scouts): signal_scout (scan + anomaly detect + push signals to ewr)

The signal_scout is a lightweight agent that:
  1. Scans nearby exchanges (from DEPLOYMENT_MANIFEST)
  2. Detects latency anomalies (RTT spike > 20% = signal)
  3. Pushes signals to the central DB via /api/v1/signals/push
  4. Sleeps CRYPTO_SCAN_INTERVAL, repeats

Usage:
  # Auto-detect region from FLY_REGION env var:
  runner = FlyAgentRunner()
  runner.start()

  # Check status:
  runner.status()
"""

import json
import logging
import os
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Ensure agents/ is on sys.path
_AGENTS_DIR = str(Path(__file__).resolve().parent)
if _AGENTS_DIR not in sys.path:
    sys.path.insert(0, _AGENTS_DIR)

import sqlite3

from fly_deployer import (
    get_region,
    get_agents_for_region,
    DEPLOYMENT_MANIFEST,
    AgentAssistant,
)
from agent_goals import GoalValidator, RULES

logger = logging.getLogger("fly_agent_runner")

# Signal push config
NETTRACE_API_KEY = os.environ.get("NETTRACE_API_KEY", "")
# Prefer dedicated internal signal secret; fall back to legacy shared secret.
INTERNAL_SECRET = (
    os.environ.get("INTERNAL_SIGNAL_SECRET", "")
    or os.environ.get("SECRET_KEY", "")
)
FLY_URL = os.environ.get("APP_URL", "https://nettrace-dashboard.fly.dev")
DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).resolve().parent.parent / "traceroute.db"))
PRIMARY_REGION = os.environ.get("PRIMARY_REGION", "ewr")
SCOUT_INTERVAL = int(os.environ.get("CRYPTO_SCAN_INTERVAL", "120"))
STATUS_INTERVAL_SECONDS = max(5, int(os.environ.get("AGENT_STATUS_INTERVAL_SECONDS", "5") or 5))
TRADE_FIX_INTERVAL_SECONDS = max(
    5, int(os.environ.get("AGENT_TRADE_FIX_INTERVAL_SECONDS", "15") or 15)
)
PORTFOLIO_REFRESH_INTERVAL_SECONDS = max(
    5, int(os.environ.get("AGENT_PORTFOLIO_REFRESH_SECONDS", "20") or 20)
)
TRADE_FIX_ENABLED = os.environ.get("AGENT_TRADE_FIX_ENABLED", "1").lower() not in (
    "0",
    "false",
    "no",
)
VENUE_UNIVERSE_ENABLED = os.environ.get("AGENT_VENUE_UNIVERSE_ENABLED", "1").lower() not in (
    "0",
    "false",
    "no",
)
VENUE_UNIVERSE_REFRESH_SECONDS = max(
    60, int(os.environ.get("AGENT_VENUE_UNIVERSE_REFRESH_SECONDS", "1800") or 1800)
)
VENUE_UNIVERSE_TARGET = max(100, int(os.environ.get("AGENT_VENUE_UNIVERSE_TARGET", "1000") or 1000))
VENUE_UNIVERSE_ACTIVE_BATCH = max(
    10, int(os.environ.get("AGENT_VENUE_UNIVERSE_ACTIVE_BATCH", "120") or 120)
)
VENUE_UNIVERSE_OPERATOR_EMAIL = str(os.environ.get("AGENT_VENUE_UNIVERSE_OPERATOR_EMAIL", "") or "")
VENUE_ONBOARDING_ENABLED = os.environ.get("AGENT_VENUE_ONBOARDING_ENABLED", "1").lower() not in (
    "0",
    "false",
    "no",
)
VENUE_ONBOARDING_INTERVAL_SECONDS = max(
    10, int(os.environ.get("AGENT_VENUE_ONBOARDING_INTERVAL_SECONDS", "30") or 30)
)
VENUE_ONBOARDING_MAX_TASKS = max(
    1, int(os.environ.get("AGENT_VENUE_ONBOARDING_MAX_TASKS", "16") or 16)
)
TRADE_FIX_MAX_ORDERS = max(10, int(os.environ.get("AGENT_TRADE_FIX_MAX_ORDERS", "120") or 120))
TRADE_FIX_LOOKBACK_HOURS = max(1, int(os.environ.get("AGENT_TRADE_FIX_LOOKBACK_HOURS", "96") or 96))
TRADE_FIX_RECONCILE_STATUSES = tuple(
    s.strip().lower()
    for s in str(
        os.environ.get(
            "AGENT_TRADE_FIX_RECONCILE_STATUSES",
            "pending,placed,open,accepted,ack_ok",
        )
    ).split(",")
    if s.strip()
)
TRADE_FIX_STALE_PENDING_SECONDS = max(
    60, int(os.environ.get("AGENT_TRADE_FIX_STALE_PENDING_SECONDS", "1800") or 1800)
)
TRADER_DB_PATH = Path(__file__).resolve().parent / "trader.db"
_PERSISTENT_DIR = Path("/data") if Path("/data").is_dir() else Path(__file__).resolve().parent
STATUS_FILE = Path(
    os.environ.get(
        "AGENT_RUNNER_STATUS_FILE",
        str(_PERSISTENT_DIR / "fly_agent_runner_status.json"),
    )
)
VENUE_UNIVERSE_PATH = Path(
    os.environ.get("AGENT_VENUE_UNIVERSE_PATH", str(_PERSISTENT_DIR / "venue_universe.json"))
)
VENUE_ONBOARDING_QUEUE_PATH = Path(
    os.environ.get("AGENT_VENUE_ONBOARDING_QUEUE_PATH", str(_PERSISTENT_DIR / "venue_onboarding_queue.json"))
)
VENUE_UNIVERSE_STATUS_PATH = Path(
    os.environ.get("AGENT_VENUE_UNIVERSE_STATUS_PATH", str(_PERSISTENT_DIR / "venue_universe_status.json"))
)
VENUE_CLAUDE_BRIEF_PATH = Path(
    os.environ.get(
        "AGENT_VENUE_CLAUDE_BRIEF_PATH",
        str(_PERSISTENT_DIR / "claude_staging" / "venue_universe_brief.md"),
    )
)
VENUE_ONBOARDING_WORKER_STATUS_PATH = Path(
    os.environ.get(
        "AGENT_VENUE_ONBOARDING_WORKER_STATUS_PATH",
        str(_PERSISTENT_DIR / "venue_onboarding_worker_status.json"),
    )
)
VENUE_ONBOARDING_EVENTS_PATH = Path(
    os.environ.get(
        "AGENT_VENUE_ONBOARDING_EVENTS_PATH",
        str(_PERSISTENT_DIR / "venue_onboarding_events.jsonl"),
    )
)
_SELL_COMPLETED_STATUSES = {"filled", "closed", "executed", "settled"}
_PENDING_STATUSES = {"pending", "placed", "open", "accepted", "ack_ok"}

try:
    from reconcile_agent_trades import reconcile_close_first as _reconcile_close_first
except Exception:
    _reconcile_close_first = None


class FlyAgentRunner:
    """Lightweight in-process agent runner for Fly.io containers."""

    def __init__(self):
        self.region = get_region()
        self.is_primary = (self.region == PRIMARY_REGION or self.region == "local")
        self.agents = {}       # name -> thread
        self.assistants = {}   # name -> AgentAssistant
        self.running = False
        self.goals = GoalValidator()
        self._started_at = None
        self._lock = threading.RLock()
        self._control_thread = None
        self._tools = None
        self._status_interval_seconds = int(STATUS_INTERVAL_SECONDS)
        self._trade_fix_interval_seconds = int(TRADE_FIX_INTERVAL_SECONDS)
        self._portfolio_refresh_interval_seconds = int(PORTFOLIO_REFRESH_INTERVAL_SECONDS)
        self._trade_fix_enabled = bool(TRADE_FIX_ENABLED)
        self._venue_universe_enabled = bool(VENUE_UNIVERSE_ENABLED and self.is_primary)
        self._venue_onboarding_enabled = bool(VENUE_ONBOARDING_ENABLED and self.is_primary)
        self._last_status_at = 0.0
        self._last_trade_fix_at = 0.0
        self._last_portfolio_refresh_at = 0.0
        self._last_venue_universe_at = 0.0
        self._last_venue_onboarding_at = 0.0
        self._cached_portfolio = {
            "total_usd": 0.0,
            "available_cash": 0.0,
            "held_in_orders": 0.0,
            "holdings": {},
            "error": "not_collected",
        }
        self._last_trade_fix = {
            "ok": False,
            "reason": "not_started",
            "updated_at": "",
            "reconcile": {},
            "close_reconciliation": {},
            "trade_metrics": {},
        }
        self._last_venue_universe = {
            "ok": False,
            "reason": "not_started",
            "updated_at": "",
            "actual_count": 0,
            "manual_actions": 0,
            "active_batch_size": int(VENUE_UNIVERSE_ACTIVE_BATCH),
            "target_count": int(VENUE_UNIVERSE_TARGET),
        }
        self._last_venue_onboarding = {
            "ok": False,
            "reason": "not_started",
            "updated_at": "",
            "processed": 0,
            "completed": 0,
            "blocked_human": 0,
            "deferred": 0,
            "remaining_pending_auto": 0,
            "remaining_pending_human": 0,
        }
        self._last_status_payload = {
            "region": self.region,
            "is_primary": self.is_primary,
            "running": False,
            "uptime_seconds": 0,
            "agents": {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "trade_fix": dict(self._last_trade_fix),
            "venue_universe": dict(self._last_venue_universe),
            "venue_onboarding": dict(self._last_venue_onboarding),
            "portfolio": dict(self._cached_portfolio),
        }

        logger.info("FlyAgentRunner init: region=%s, is_primary=%s",
                     self.region, self.is_primary)

    def get_agents_for_region(self):
        """Return agent list for current region."""
        return get_agents_for_region(self.region)

    def start(self):
        """Start agents appropriate for this region."""
        with self._lock:
            if self.running:
                logger.warning("Agent runner already running")
                return

            self.running = True
            self._started_at = time.time()

            # Log the goals every agent is trained on
            logger.info("=== AGENT GOALS (encoded in every decision) ===")
            for num, rule in RULES.items():
                logger.info("  Rule #%d: %s", num, rule)
            logger.info("  Order type: %s (fee: %.1f%%)",
                        GoalValidator.optimal_order_type()["order_type"],
                        GoalValidator.optimal_order_type()["fee"] * 100)
            logger.info("================================================")

            if self.is_primary:
                self._start_primary_agents()
            else:
                self._start_scout_agent()

            self._start_control_loop()
            payload = self._build_status_payload()
            self._persist_status(payload)
            self._last_status_payload = payload
            self._last_status_at = time.time()

            agent_names = list(self.agents.keys())
            logger.info("Started %d agent(s) in region %s: %s",
                        len(agent_names), self.region, agent_names)

    def stop(self):
        """Signal all agents to stop."""
        logger.info("Stopping agent runner...")
        with self._lock:
            if not self.running:
                return
            self.running = False
            running_threads = list(self.agents.items())

        # Give cooperative loops a chance to exit cleanly.
        for name, thread in running_threads:
            thread.join(timeout=3)
            if thread.is_alive():
                logger.warning("Agent thread still alive after stop timeout: %s", name)

        control_thread = self._control_thread
        if control_thread is not None:
            control_thread.join(timeout=4)
            if control_thread.is_alive():
                logger.warning("Control thread still alive after stop timeout")

        with self._lock:
            self.agents.clear()
            self.assistants.clear()
            self._started_at = None
            self._control_thread = None
            payload = self._build_status_payload()
            self._last_status_payload = payload
            self._last_status_at = time.time()
        self._persist_status(payload)
        logger.info("Agent runner stopped")

    def _start_control_loop(self):
        with self._lock:
            existing = self._control_thread
            if existing is not None and existing.is_alive():
                return
            t = threading.Thread(
                target=self._control_loop,
                name="agent-control-loop",
                daemon=True,
            )
            t.start()
            self._control_thread = t

    def _control_loop(self):
        while self.running:
            now = time.time()
            try:
                # Emit heartbeat first so status updates remain on 5s cadence
                # even when downstream trade-fix/network calls are slow.
                if (now - self._last_status_at) >= float(self._status_interval_seconds):
                    payload = self._build_status_payload()
                    self._persist_status(payload)
                    with self._lock:
                        self._last_status_payload = payload
                        self._last_status_at = now
                    logger.info(
                        "heartbeat region=%s agents=%d pending=%s closes=%s pnl24h=%s",
                        self.region,
                        len(payload.get("agents", {}) or {}),
                        ((payload.get("trade_fix", {}) or {}).get("trade_metrics", {}) or {}).get("pending_orders"),
                        ((payload.get("trade_fix", {}) or {}).get("close_reconciliation", {}) or {}).get("completions"),
                        ((payload.get("trade_fix", {}) or {}).get("trade_metrics", {}) or {}).get("realized_pnl_24h"),
                    )

                if (
                    self._trade_fix_enabled
                    and self.is_primary
                    and (now - self._last_trade_fix_at) >= float(self._trade_fix_interval_seconds)
                ):
                    trade_fix = self._run_trade_fixes()
                    with self._lock:
                        self._last_trade_fix = trade_fix
                        self._last_trade_fix_at = now

                if (now - self._last_portfolio_refresh_at) >= float(self._portfolio_refresh_interval_seconds):
                    portfolio = self._collect_portfolio_snapshot()
                    with self._lock:
                        self._cached_portfolio = portfolio
                        self._last_portfolio_refresh_at = now

                if (
                    self._venue_universe_enabled
                    and (now - self._last_venue_universe_at) >= float(VENUE_UNIVERSE_REFRESH_SECONDS)
                ):
                    venue_universe = self._refresh_venue_universe()
                    with self._lock:
                        self._last_venue_universe = venue_universe
                        self._last_venue_universe_at = now

                if (
                    self._venue_onboarding_enabled
                    and (now - self._last_venue_onboarding_at) >= float(VENUE_ONBOARDING_INTERVAL_SECONDS)
                ):
                    venue_onboarding = self._run_venue_onboarding()
                    with self._lock:
                        self._last_venue_onboarding = venue_onboarding
                        self._last_venue_onboarding_at = now
            except Exception as e:
                logger.warning("control loop tick failed: %s", e)
            self._interruptible_sleep(1)

    def _start_primary_agents(self):
        """Start the full trading stack on the primary region (ewr)."""
        manifest_agents = get_agents_for_region(self.region)
        logger.info("Primary region %s: starting %d agents from manifest",
                     self.region, len(manifest_agents))

        for agent_def in manifest_agents:
            name = agent_def["name"]
            try:
                self._start_agent_thread(name)
            except Exception as e:
                logger.error("Failed to start agent '%s': %s", name, e)

    def _start_agent_thread(self, name):
        """Start a named agent as a daemon thread."""
        with self._lock:
            existing = self.agents.get(name)
            if existing is not None and existing.is_alive():
                logger.warning("Agent '%s' already running; skipping duplicate start", name)
                return

        target = self._get_agent_run_fn(name)
        if target is None:
            logger.warning("No run function found for agent '%s', skipping", name)
            return

        thread = threading.Thread(
            target=self._agent_wrapper,
            args=(name, target),
            name=f"agent-{name}",
            daemon=True,
        )
        thread.start()
        with self._lock:
            self.agents[name] = thread
            self.assistants[name] = AgentAssistant(name)
        logger.info("Started agent thread: %s (tid=%s)", name, thread.ident)

    def _get_agent_run_fn(self, name):
        """Import and return the run function for a named agent.

        Returns a callable that runs the agent's main loop, or None.
        """
        try:
            if name == "sniper":
                from sniper import Sniper
                instance = Sniper()
                return instance.run

            elif name == "meta_engine":
                from meta_engine import MetaEngine
                instance = MetaEngine()
                return instance.run

            elif name == "advanced_team":
                from advanced_team.coordinator import Coordinator
                instance = Coordinator()
                return instance.run

            elif name == "capital_allocator":
                from capital_allocator import CapitalAllocator
                instance = CapitalAllocator()
                return instance.run_loop

            elif name == "strike_teams":
                from strike_teams import StrikeTeamManager
                mgr = StrikeTeamManager()
                return mgr.deploy_all

            elif name == "hf_execution":
                from hf_execution_agent import main as hf_main
                return lambda: hf_main(["--live"])

            elif name in ("exchange_scanner", "latency_arb",
                          "momentum_scalper", "forex_arb"):
                # Scout-type agents — run signal_scout for non-primary regions
                # On primary, these are informational-only
                return lambda: self._run_signal_scout()

            else:
                logger.warning("Unknown agent '%s' — no loader defined", name)
                return None

        except ImportError as e:
            logger.error("Cannot import agent '%s': %s", name, e)
            return None

    def _agent_wrapper(self, name, target):
        """Wrapper that catches crashes and logs them."""
        restart_count = 0
        max_restarts = 3

        while self.running and restart_count <= max_restarts:
            try:
                logger.info("Agent '%s' starting (attempt %d)", name, restart_count + 1)
                target()
                # If target() returns normally, the agent chose to stop
                logger.info("Agent '%s' exited normally", name)
                break
            except Exception as e:
                restart_count += 1
                logger.error("Agent '%s' crashed (attempt %d/%d): %s",
                             name, restart_count, max_restarts + 1, e,
                             exc_info=True)
                if self.assistants.get(name):
                    self.assistants[name].metrics["crashes"] += 1
                    self.assistants[name].metrics["last_error"] = str(e)[:200]
                if restart_count <= max_restarts and self.running:
                    backoff = min(30 * restart_count, 120)
                    logger.info("Restarting '%s' in %ds...", name, backoff)
                    time.sleep(backoff)

        if restart_count > max_restarts:
            logger.error("Agent '%s' exceeded max restarts (%d), giving up",
                         name, max_restarts)

    # ──────────────────────────────────────────────────────────────────────
    # Signal Scout — for non-primary (scout) regions
    # ──────────────────────────────────────────────────────────────────────

    def _start_scout_agent(self):
        """Start a signal scout for non-primary regions."""
        thread = threading.Thread(
            target=self._agent_wrapper,
            args=("signal_scout", self._run_signal_scout),
            name="agent-signal_scout",
            daemon=True,
        )
        thread.start()
        self.agents["signal_scout"] = thread
        self.assistants["signal_scout"] = AgentAssistant("signal_scout")
        logger.info("Started signal scout for region %s", self.region)

    def _run_signal_scout(self):
        """Scout loop: scan nearby exchanges, detect anomalies, push signals.

        This runs on non-primary regions (lhr, nrt, sin, etc.) and provides
        the cross-region latency divergence that is our information asymmetry edge.
        """
        region_info = DEPLOYMENT_MANIFEST.get(self.region, {})
        exchanges = region_info.get("exchanges", [])
        logger.info("Signal scout starting: region=%s, exchanges=%s",
                     self.region, exchanges)

        while self.running:
            try:
                # 1. Fetch latest crypto latency data from our own API
                signals = self._fetch_local_signals()

                # 2. Detect anomalies (RTT spike > 20%)
                anomalies = [s for s in signals if s.get("is_anomaly")]

                if anomalies:
                    logger.info("Scout %s detected %d anomalies",
                                self.region, len(anomalies))

                    # 3. Push each anomaly as a signal to the central API
                    for anomaly in anomalies:
                        self._push_signal(anomaly)

                # 4. Also push a heartbeat signal so ewr knows we're alive
                self._push_heartbeat()

                # Sleep until next scan
                self._interruptible_sleep(SCOUT_INTERVAL)

            except Exception as e:
                logger.error("Scout loop error: %s", e)
                self._interruptible_sleep(30)

    def _fetch_local_signals(self):
        """Read crypto exchange latency data directly from the local SQLite DB.

        Bypasses the Flask API (no auth needed) since we're in the same container.
        Computes anomaly detection (>20% RTT deviation) the same way the API does.
        """
        try:
            db = sqlite3.connect(DB_PATH)
            db.row_factory = sqlite3.Row
            rows = db.execute("""
                SELECT target_host, target_name, total_rtt, created_at
                FROM scan_metrics
                WHERE category = 'Crypto Exchanges'
                  AND created_at >= datetime('now', '-1 hours')
                ORDER BY created_at DESC
            """).fetchall()
            db.close()

            # Aggregate per host: latest RTT + average + anomaly detection
            host_data = {}
            for row in rows:
                host = row["target_host"]
                rtt = row["total_rtt"]
                if rtt is None:
                    continue
                if host not in host_data:
                    host_data[host] = {
                        "host": host, "name": row["target_name"],
                        "latest_rtt": rtt, "rtts": [],
                    }
                host_data[host]["rtts"].append(rtt)

            results = []
            for host, data in host_data.items():
                rtts = data["rtts"]
                if len(rtts) < 2:
                    continue
                avg_rtt = sum(rtts) / len(rtts)
                deviation = (data["latest_rtt"] - avg_rtt) / avg_rtt if avg_rtt > 0 else 0
                results.append({
                    "host": host,
                    "name": data["name"],
                    "latest_rtt": data["latest_rtt"],
                    "avg_rtt": round(avg_rtt, 2),
                    "deviation_pct": round(deviation * 100, 2),
                    "is_anomaly": abs(deviation) > 0.20,
                    "samples": len(rtts),
                })
            return results

        except Exception as e:
            logger.debug("Could not read local scan_metrics: %s", e)
            return []

    def _push_signal(self, anomaly):
        """Push an anomaly signal to the central API for cross-region analysis.

        Uses internal secret auth (X-Internal-Secret header) which all Fly machines
        share via the SECRET_KEY env var. This bypasses API key auth.
        """
        if not INTERNAL_SECRET and not NETTRACE_API_KEY:
            logger.debug("No auth credentials, skipping signal push")
            return

        payload = {
            "signal_type": "latency_anomaly",
            "target_host": anomaly.get("host", "unknown"),
            "direction": "CAUTION",
            "confidence": min(abs(anomaly.get("deviation_pct", 0)) / 100, 0.99),
            "source_region": self.region,
            "details": {
                "latest_rtt": anomaly.get("latest_rtt"),
                "avg_rtt": anomaly.get("avg_rtt"),
                "deviation_pct": anomaly.get("deviation_pct"),
                "exchange_name": anomaly.get("name", ""),
            },
        }

        self._post_signal(payload)
        logger.debug("Pushed signal: %s %s (%.1f%% deviation)",
                     anomaly.get("host"), self.region,
                     anomaly.get("deviation_pct", 0))

    def _push_heartbeat(self):
        """Push a heartbeat so the primary region knows this scout is alive."""
        if not INTERNAL_SECRET and not NETTRACE_API_KEY:
            return

        payload = {
            "signal_type": "scout_heartbeat",
            "target_host": f"scout-{self.region}",
            "direction": "INFO",
            "confidence": 1.0,
            "source_region": self.region,
            "details": {
                "uptime_seconds": int(time.time() - self._started_at) if self._started_at else 0,
                "agents_running": list(self.agents.keys()),
            },
        }

        self._post_signal(payload)

    def _post_signal(self, payload):
        """POST a signal to the central API with internal or API key auth."""
        try:
            url = f"{FLY_URL}/api/v1/signals/push"
            data = json.dumps(payload).encode("utf-8")
            headers = {"Content-Type": "application/json"}

            # Prefer internal secret auth (works across all Fly machines)
            if INTERNAL_SECRET:
                headers["X-Internal-Secret"] = INTERNAL_SECRET
            elif NETTRACE_API_KEY:
                headers["Authorization"] = f"Bearer {NETTRACE_API_KEY}"

            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            resp = urllib.request.urlopen(req, timeout=10)
            logger.info("Signal pushed to %s: %s (status=%d)",
                        self.region, payload.get("signal_type", "?"), resp.status)
        except Exception as e:
            logger.warning("Signal push failed from %s: %s", self.region, e)

    def _interruptible_sleep(self, seconds):
        """Sleep in small increments so we can stop quickly."""
        end = time.time() + seconds
        while self.running and time.time() < end:
            time.sleep(min(5, end - time.time()))

    def _get_tools(self):
        if self._tools is not None:
            return self._tools
        try:
            from agent_tools import AgentTools
            self._tools = AgentTools()
            return self._tools
        except Exception as e:
            logger.warning("AgentTools unavailable for trade-fix loop: %s", e)
            return None

    @staticmethod
    def _close_reconciliation_payload(summary):
        attempts = int(summary.get("close_attempts", 0) or 0)
        completions = int(summary.get("close_completions", 0) or 0)
        failures = int(summary.get("close_failures", 0) or 0)
        completion_rate = (float(completions) / float(attempts)) if attempts > 0 else 1.0
        return {
            "attempts": attempts,
            "completions": completions,
            "failures": failures,
            "completion_rate": round(float(completion_rate), 6),
            "gate_passed": bool(summary.get("close_gate_passed", False)),
            "gate_reason": str(summary.get("close_gate_reason", "unknown")),
            "failure_reasons": dict(summary.get("close_failure_reasons", {}) or {}),
        }

    def _collect_trade_metrics(self, tools):
        metrics = {
            "total_trades": 0,
            "pending_orders": 0,
            "stale_pending_orders": 0,
            "sell_closes_24h": 0,
            "realized_pnl_24h": 0.0,
        }
        try:
            db = tools.db
            row = db.execute("SELECT COUNT(*) AS n FROM agent_trades").fetchone()
            metrics["total_trades"] = int((row["n"] if row else 0) or 0)

            pending_marks = ",".join("?" for _ in _PENDING_STATUSES)
            row = db.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM agent_trades
                WHERE LOWER(COALESCE(status, '')) IN ({pending_marks})
                """,
                tuple(sorted(_PENDING_STATUSES)),
            ).fetchone()
            metrics["pending_orders"] = int((row["n"] if row else 0) or 0)

            row = db.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM agent_trades
                WHERE LOWER(COALESCE(status, '')) IN ({pending_marks})
                  AND created_at < datetime('now', ?)
                """,
                tuple(sorted(_PENDING_STATUSES)) + (f"-{int(TRADE_FIX_STALE_PENDING_SECONDS)} seconds",),
            ).fetchone()
            metrics["stale_pending_orders"] = int((row["n"] if row else 0) or 0)

            close_marks = ",".join("?" for _ in _SELL_COMPLETED_STATUSES)
            row = db.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM agent_trades
                WHERE UPPER(COALESCE(side, ''))='SELL'
                  AND LOWER(COALESCE(status, '')) IN ({close_marks})
                  AND created_at >= datetime('now', '-24 hours')
                """,
                tuple(sorted(_SELL_COMPLETED_STATUSES)),
            ).fetchone()
            metrics["sell_closes_24h"] = int((row["n"] if row else 0) or 0)

            row = db.execute(
                """
                SELECT COALESCE(SUM(pnl), 0) AS pnl
                FROM agent_trades
                WHERE UPPER(COALESCE(side, ''))='SELL'
                  AND pnl IS NOT NULL
                  AND created_at >= datetime('now', '-24 hours')
                """
            ).fetchone()
            metrics["realized_pnl_24h"] = round(float((row["pnl"] if row else 0.0) or 0.0), 8)
        except Exception as e:
            metrics["error"] = str(e)
        return metrics

    def _collect_portfolio_snapshot(self):
        tools = self._get_tools()
        if tools is None:
            return {"error": "tools_unavailable"}
        try:
            snap = tools.get_portfolio()
            if not isinstance(snap, dict):
                return {"error": "portfolio_invalid"}
            return {
                "total_usd": float(snap.get("total_usd", 0.0) or 0.0),
                "available_cash": float(snap.get("available_cash", 0.0) or 0.0),
                "held_in_orders": float(snap.get("held_in_orders", 0.0) or 0.0),
                "holdings": dict(snap.get("holdings", {}) or {}),
            }
        except Exception as e:
            return {"error": str(e)}

    def _run_trade_fixes(self):
        payload = {
            "ok": False,
            "reason": "unknown",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "reconcile": {},
            "close_reconciliation": {},
            "trade_metrics": {},
        }
        if not self.is_primary:
            payload["ok"] = True
            payload["reason"] = "non_primary_region"
            return payload
        tools = self._get_tools()
        if tools is None:
            payload["reason"] = "tools_unavailable"
            return payload

        try:
            reconcile = tools.reconcile_agent_trades(
                max_orders=int(TRADE_FIX_MAX_ORDERS),
                lookback_hours=int(TRADE_FIX_LOOKBACK_HOURS),
                reconcile_statuses=TRADE_FIX_RECONCILE_STATUSES,
            )
            payload["reconcile"] = reconcile if isinstance(reconcile, dict) else {}
        except Exception as e:
            payload["reconcile_error"] = str(e)

        if _reconcile_close_first is not None:
            try:
                close_summary = _reconcile_close_first(
                    tools,
                    max_orders=int(TRADE_FIX_MAX_ORDERS),
                    lookback_hours=int(TRADE_FIX_LOOKBACK_HOURS),
                    reconcile_statuses=TRADE_FIX_RECONCILE_STATUSES,
                )
                if isinstance(close_summary, dict):
                    payload["close_reconciliation"] = self._close_reconciliation_payload(close_summary)
            except Exception as e:
                payload["close_reconcile_error"] = str(e)

        payload["trade_metrics"] = self._collect_trade_metrics(tools)
        payload["ok"] = True
        payload["reason"] = "trade_fix_cycle_complete"
        return payload

    def _refresh_venue_universe(self):
        payload = {
            "ok": False,
            "reason": "venue_universe_refresh_failed",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "actual_count": 0,
            "manual_actions": 0,
            "active_batch_size": int(VENUE_UNIVERSE_ACTIVE_BATCH),
            "target_count": int(VENUE_UNIVERSE_TARGET),
        }
        try:
            from venue_universe_manager import run as run_venue_universe

            result = run_venue_universe(
                target_count=int(VENUE_UNIVERSE_TARGET),
                active_batch_size=int(VENUE_UNIVERSE_ACTIVE_BATCH),
                operator_email=str(VENUE_UNIVERSE_OPERATOR_EMAIL),
                universe_path=VENUE_UNIVERSE_PATH,
                queue_path=VENUE_ONBOARDING_QUEUE_PATH,
                status_path=VENUE_UNIVERSE_STATUS_PATH,
                claude_brief_path=VENUE_CLAUDE_BRIEF_PATH,
            )
            payload.update(
                {
                    "ok": bool(result.get("ok", False)),
                    "reason": "venue_universe_refresh_complete",
                    "actual_count": int(result.get("actual_count", 0) or 0),
                    "manual_actions": int(result.get("manual_actions", 0) or 0),
                    "active_batch_size": int(
                        result.get("active_batch_size", VENUE_UNIVERSE_ACTIVE_BATCH)
                        or VENUE_UNIVERSE_ACTIVE_BATCH
                    ),
                    "target_count": int(result.get("target_count", VENUE_UNIVERSE_TARGET) or VENUE_UNIVERSE_TARGET),
                    "paths": {
                        "universe_path": str(result.get("universe_path", "")),
                        "queue_path": str(result.get("queue_path", "")),
                        "status_path": str(result.get("status_path", "")),
                        "claude_brief_path": str(result.get("claude_brief_path", "")),
                    },
                }
            )
        except Exception as e:
            payload["error"] = str(e)
            logger.warning("venue universe refresh failed: %s", e)
        return payload

    def _run_venue_onboarding(self):
        payload = {
            "ok": False,
            "reason": "venue_onboarding_failed",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "processed": 0,
            "completed": 0,
            "blocked_human": 0,
            "deferred": 0,
            "remaining_pending_auto": 0,
            "remaining_pending_human": 0,
        }
        try:
            from venue_onboarding_worker import run_once as run_onboarding_once

            result = run_onboarding_once(
                max_tasks=int(VENUE_ONBOARDING_MAX_TASKS),
                queue_path=VENUE_ONBOARDING_QUEUE_PATH,
                status_path=VENUE_ONBOARDING_WORKER_STATUS_PATH,
                events_path=VENUE_ONBOARDING_EVENTS_PATH,
                db_path=Path(DB_PATH),
            )
            if isinstance(result, dict):
                payload.update(result)
                payload["reason"] = str(result.get("reason", "processed") or "processed")
                payload["ok"] = bool(result.get("ok", False))
        except Exception as e:
            payload["error"] = str(e)
            logger.warning("venue onboarding cycle failed: %s", e)
        return payload

    def _build_status_payload(self):
        with self._lock:
            agents = list(self.agents.items())
            assistants = dict(self.assistants)
            trade_fix = dict(self._last_trade_fix) if isinstance(self._last_trade_fix, dict) else {}
            venue_universe = (
                dict(self._last_venue_universe)
                if isinstance(self._last_venue_universe, dict)
                else {}
            )
            venue_onboarding = (
                dict(self._last_venue_onboarding)
                if isinstance(self._last_venue_onboarding, dict)
                else {}
            )
            portfolio = dict(self._cached_portfolio) if isinstance(self._cached_portfolio, dict) else {}
            started_at = self._started_at
            running = bool(self.running)
            control_thread_alive = bool(self._control_thread and self._control_thread.is_alive())

        return {
            "region": self.region,
            "is_primary": self.is_primary,
            "running": running,
            "uptime_seconds": int(time.time() - started_at) if started_at else 0,
            "agents": {
                name: {
                    "alive": thread.is_alive(),
                    "metrics": assistants[name].get_metrics() if name in assistants else {},
                }
                for name, thread in agents
            },
            "control": {
                "status_interval_seconds": int(self._status_interval_seconds),
                "trade_fix_interval_seconds": int(self._trade_fix_interval_seconds),
                "portfolio_refresh_interval_seconds": int(self._portfolio_refresh_interval_seconds),
                "trade_fix_enabled": bool(self._trade_fix_enabled and self.is_primary),
                "venue_universe_enabled": bool(self._venue_universe_enabled and self.is_primary),
                "venue_onboarding_enabled": bool(self._venue_onboarding_enabled and self.is_primary),
                "control_thread_alive": control_thread_alive,
                "last_trade_fix_age_seconds": round(max(0.0, time.time() - float(self._last_trade_fix_at)), 3)
                if self._last_trade_fix_at > 0
                else None,
                "last_venue_universe_age_seconds": round(
                    max(0.0, time.time() - float(self._last_venue_universe_at)),
                    3,
                )
                if self._last_venue_universe_at > 0
                else None,
                "last_venue_onboarding_age_seconds": round(
                    max(0.0, time.time() - float(self._last_venue_onboarding_at)),
                    3,
                )
                if self._last_venue_onboarding_at > 0
                else None,
                "last_status_age_seconds": round(max(0.0, time.time() - float(self._last_status_at)), 3)
                if self._last_status_at > 0
                else None,
            },
            "trade_fix": trade_fix,
            "venue_universe": venue_universe,
            "venue_onboarding": venue_onboarding,
            "portfolio": portfolio,
            "status_file": str(STATUS_FILE),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _persist_status(self, payload):
        try:
            STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
            STATUS_FILE.write_text(json.dumps(payload, indent=2))
        except Exception as e:
            logger.warning("Failed to persist fly runner status: %s", e)

    # ──────────────────────────────────────────────────────────────────────
    # Status
    # ──────────────────────────────────────────────────────────────────────

    def status(self):
        """Return current runner status."""
        with self._lock:
            payload = (
                dict(self._last_status_payload)
                if isinstance(self._last_status_payload, dict)
                else {}
            )
        if not payload:
            payload = self._build_status_payload()
        else:
            # Always refresh liveness and timestamp on request.
            agents = payload.get("agents", {})
            if isinstance(agents, dict):
                with self._lock:
                    for name, thread in self.agents.items():
                        if name not in agents:
                            agents[name] = {}
                        agents[name]["alive"] = bool(thread.is_alive())
                        if name in self.assistants:
                            agents[name]["metrics"] = self.assistants[name].get_metrics()
            payload["running"] = bool(self.running)
            payload["timestamp"] = datetime.now(timezone.utc).isoformat()
        return payload


# ---------------------------------------------------------------------------
# Standalone entry point (for testing)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    runner = FlyAgentRunner()
    print(f"Region: {runner.region}")
    print(f"Is primary: {runner.is_primary}")
    print(f"Agents for region: {runner.get_agents_for_region()}")

    if len(sys.argv) > 1 and sys.argv[1] == "run":
        runner.start()
        try:
            while True:
                time.sleep(60)
                print(json.dumps(runner.status(), indent=2))
        except KeyboardInterrupt:
            runner.stop()
    else:
        print("\nUse 'python fly_agent_runner.py run' to start agents")
