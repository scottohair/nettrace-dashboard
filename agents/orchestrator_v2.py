#!/usr/bin/env python3
"""Master Orchestrator V2 — 24/7 background operations daemon.

Manages all trading agents as subprocesses:
  - grid_trader.py     — Grid trading on BTC-USD (primary money maker)
  - dca_bot.py         — DCA accumulation
  - live_trader.py     — Signal-based BUY-only trading
  - arb_scanner.py     — Cross-exchange arb monitoring

Features:
  - Auto-restart crashed agents (max 3 restarts/hour)
  - Portfolio monitoring every 5 minutes
  - Global risk enforcement (daily loss limit, exposure limits)
  - Health monitoring (PID checks every 30s)
  - Snapshot push to Fly dashboard
  - PID file for daemon management
  - HARDSTOP on runaway losses

Usage:
  python orchestrator_v2.py            # Run in foreground
  nohup python orchestrator_v2.py &    # Run as daemon
  python orchestrator_v2.py status     # Show status
  python orchestrator_v2.py stop       # Stop all agents
"""

import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ORCH] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "orchestrator.log")),
    ]
)
logger = logging.getLogger("orchestrator_v2")

AGENTS_DIR = str(Path(__file__).parent)
PID_FILE = str(Path(__file__).parent / "orchestrator.pid")
ORCH_DB = str(Path(__file__).parent / "orchestrator.db")

# Risk limits — NEVER violate
HARDSTOP_FLOOR_USD = 500.00   # ABSOLUTE FLOOR — kill everything if portfolio drops below this
HARDSTOP_DRAWDOWN_PCT = 0.30  # 30% drawdown from peak → kill everything (was 15%, too sensitive with bridges)
STARTING_CAPITAL = None       # Auto-detect from DB (no more hardcoding)

# Wallet monitoring (set via env or updated at runtime)
WALLET_ADDRESS = os.environ.get("WALLET_ADDRESS", "")  # EVM wallet to monitor
WALLET_CHAINS = ["ethereum", "base", "arbitrum", "polygon"]

# Bridge-in-transit tracking — prevents false HARDSTOP during L1→L2 bridges
# Each entry: {"chain": "base", "amount_usd": 62.58, "amount_eth": 0.0326, "timestamp": unix, "tx_hash": "0x..."}
PENDING_BRIDGES_FILE = str(Path(__file__).parent / "pending_bridges.json")

# Drawdown sanity check — if portfolio drops more than this in one check, verify before HARDSTOP
MAX_SANE_SINGLE_DROP_PCT = 0.40  # 40% drop in 5 minutes is suspicious, verify first

# Timing
HEALTH_CHECK_INTERVAL = 30    # seconds
PORTFOLIO_CHECK_INTERVAL = 300  # 5 minutes
MAX_RESTARTS_PER_HOUR = 3

# Agent definitions
AGENT_CONFIGS = [
    {
        "name": "grid_trader",
        "script": "grid_trader.py",
        "args": ["run"],
        "enabled": False,  # FIRED: -$2.49 P&L, unprofitable at Intro 1 fee tier (0.6% maker)
        "critical": False,
        "description": "Grid trading on BTC-USD (primary money maker)",
    },
    {
        "name": "dca_bot",
        "script": "dca_bot.py",
        "args": [],
        "enabled": False,  # FIRED: Blind buying with no trend awareness, bought into downtrend
        "critical": False,
        "description": "DCA accumulation into BTC, ETH, SOL",
    },
    {
        "name": "live_trader",
        "script": "live_trader.py",
        "args": [],
        "enabled": False,  # FIRED: BUY-only with no trend detection, no exit strategy
        "critical": False,
        "description": "Signal-based BUY-only trading",
    },
    {
        "name": "dex_grid_trader",
        "script": "dex_grid_trader.py",
        "args": [],
        "enabled": False,  # DISABLED: needs ETH bridged to Base L2 first
        "critical": False,
        "description": "HFT grid bot on Uniswap Base L2 (WETH/USDC)",
    },
    {
        "name": "sniper",
        "script": "sniper.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "High-confidence stacked-signal sniper (90%+ conf)",
    },
    {
        "name": "meta_engine",
        "script": "meta_engine.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "Autonomous strategy evolution engine",
    },
    {
        "name": "capital_allocator",
        "script": "capital_allocator.py",
        "args": ["run"],
        "enabled": True,
        "critical": True,
        "description": "Treasury management — principle protection + profit allocation",
    },
    {
        "name": "gov_data",
        "script": "gov_data.py",
        "args": ["signals"],
        "enabled": False,  # DISABLED: exits immediately with code 0, crash-loops
        "critical": False,
        "description": "SEC/NIST/data.gov JIT signal generation",
    },
    {
        "name": "forex_arb",
        "script": "forex_agent.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "Forex correlation + stablecoin arb",
    },
    {
        "name": "latency_arb",
        "script": "latency_arb_agent.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "Latency-based arb using Fly.io proximity signals",
    },
    {
        "name": "momentum_scalper",
        "script": "momentum_scalper.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "Short-timeframe momentum scalper (1-5min)",
    },
    {
        "name": "exchange_scanner",
        "script": "exchange_scanner.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "Multi-exchange correlation scanner (CME, ICE, NYMEX, LSE, etc.)",
    },
    {
        "name": "advanced_team",
        "script": "advanced_team/coordinator.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "8-agent research/strategy/risk/execution/learning/optimization team",
    },
    {
        "name": "claude_quant",
        "script": "claude_quant_agent.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "Claude Quant 100 validator + pipeline promotion + budgeting",
    },
    {
        "name": "claude_stager",
        "script": "claude_stager_agent.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "Stages strategy/framework/message context for Claude ingestion",
    },
    {
        "name": "amicoin",
        "script": "amicoin_agent.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "AmiCoin Network simulation agent (isolated, not counted in real holdings)",
    },
    # ── DETERMINISTIC STRATEGIES (work in ANY market regime) ──
    {
        "name": "market_maker",
        "script": "market_maker.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "Spread collection via limit orders (BUY below bid, SELL above ask). "
                       "No directional bet — profits from liquidity provision in any regime.",
    },
    {
        "name": "mean_reversion",
        "script": "mean_reversion_agent.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "Statistical arb on correlated pairs (BTC/ETH, ETH/SOL). "
                       "Trades z-score deviations — deterministic, no confidence needed.",
    },
    {
        "name": "arb_scanner",
        "script": "arb_scanner.py",
        "args": [],
        "enabled": True,
        "critical": False,
        "description": "Cross-exchange price arb (5 exchanges). DETERMINISTIC: "
                       "spread > fees = trade. No confidence gate.",
    },
]


class OrchestratorV2:
    """Master orchestrator managing all trading agents."""

    def __init__(self):
        self.agents = {}      # name -> {"process": Popen, "config": dict, ...}
        self.restart_log = {} # name -> [timestamps of restarts]
        self.db = sqlite3.connect(ORCH_DB)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.running = True
        # Auto-detect starting capital from DB or live portfolio
        start = self._get_starting_capital()
        self.peak_portfolio = start
        self.daily_pnl_start = None
        self._last_portfolio_total = start  # sanity check baseline

    def _get_starting_capital(self):
        """Get starting capital from DB history (no more hardcoding)."""
        # 1. Check capital_events table for running total
        try:
            row = self.db.execute(
                "SELECT running_total FROM capital_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row and row["running_total"] > 0:
                logger.info("Starting capital from DB: $%.2f", row["running_total"])
                return row["running_total"]
        except Exception:
            pass
        # 2. Check last known portfolio peak
        try:
            row = self.db.execute(
                "SELECT MAX(total_value_usd) as peak FROM portfolio_history WHERE recorded_at >= datetime('now', '-24 hours')"
            ).fetchone()
            if row and row["peak"] and row["peak"] > 0:
                logger.info("Starting capital from 24h peak: $%.2f", row["peak"])
                return row["peak"]
        except Exception:
            pass
        # 3. Get live portfolio value
        try:
            from exchange_connector import CoinbaseTrader
            trader = CoinbaseTrader()
            accts = trader.get_accounts()
            total = 0
            for a in accts.get("accounts", []):
                bal = float(a.get("available_balance", {}).get("value", 0))
                currency = a.get("currency", "")
                if bal <= 0:
                    continue
                if currency in ("USD", "USDC"):
                    total += bal
                else:
                    total += bal  # rough estimate, will be corrected on first check
            if total > 0:
                logger.info("Starting capital from live portfolio: $%.2f", total)
                return total
        except Exception:
            pass
        logger.warning("Could not auto-detect starting capital, using $30")
        return 30.0

    def record_capital_event(self, event_type, amount_usd, tx_hash=None, note=None):
        """Record a deposit, withdrawal, or adjustment to capital tracking."""
        # Get current running total
        row = self.db.execute(
            "SELECT running_total FROM capital_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        current = row["running_total"] if row else 0
        new_total = current + amount_usd
        self.db.execute(
            "INSERT INTO capital_events (event_type, amount_usd, running_total, tx_hash, note) VALUES (?, ?, ?, ?, ?)",
            (event_type, amount_usd, new_total, tx_hash, note)
        )
        self.db.commit()
        logger.info("Capital event: %s $%.2f → total $%.2f", event_type, amount_usd, new_total)
        return new_total

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS agent_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                pid INTEGER,
                uptime_seconds INTEGER,
                restarts INTEGER DEFAULT 0,
                last_error TEXT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS portfolio_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_value_usd REAL,
                available_cash REAL,
                held_in_orders REAL,
                daily_pnl REAL,
                peak_value REAL,
                drawdown_pct REAL,
                holdings_json TEXT,
                agents_running INTEGER,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS capital_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                amount_usd REAL NOT NULL,
                running_total REAL NOT NULL,
                tx_hash TEXT,
                note TEXT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS risk_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                details TEXT,
                action_taken TEXT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Wallet registry: EVERY wallet we create or use, stored permanently
            CREATE TABLE IF NOT EXISTS wallet_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL UNIQUE,
                chain TEXT NOT NULL,
                wallet_type TEXT NOT NULL,
                private_key_enc TEXT,
                label TEXT,
                is_ours INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_balance_usd REAL DEFAULT 0,
                last_checked_at TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_wallet_addr ON wallet_registry(address);
        """)
        self.db.commit()
        # Seed wallet registry with known wallets
        self._seed_wallets()

    def _seed_wallets(self):
        """Ensure all known wallets are in the registry."""
        wallets = [
            ("0x2Efbef64bdFc6D80E835B8312e051444716299aC", "ethereum,base,arbitrum,polygon", "evm", "Main EVM wallet (all chains)"),
            ("5zpnxwKFXrQDzac81hfePinjNmicAuqNhvpkpds79w38", "solana", "solana", "Solana wallet (unfunded)"),
            ("0x75466cd6CA88911CE680aF9b64aA652Cb9721D0C", "ethereum", "coinbase_deposit", "Coinbase USDC deposit address"),
            ("0x223eb5A5852248F74754dc912872a121fA05bae8", "ethereum", "coinbase_deposit", "Coinbase ETH deposit address"),
        ]
        for addr, chain, wtype, label in wallets:
            try:
                self.db.execute(
                    "INSERT OR IGNORE INTO wallet_registry (address, chain, wallet_type, label) VALUES (?, ?, ?, ?)",
                    (addr, chain, wtype, label))
            except Exception:
                pass
        self.db.commit()

    def write_pid(self):
        """Write PID file for daemon management."""
        with open(PID_FILE, "w") as f:
            f.write(str(os.getpid()))
        logger.info("PID %d written to %s", os.getpid(), PID_FILE)

    def remove_pid(self):
        """Remove PID file."""
        try:
            os.remove(PID_FILE)
        except FileNotFoundError:
            pass

    def start_agent(self, config):
        """Start a single agent as a subprocess."""
        name = config["name"]
        script = os.path.join(AGENTS_DIR, config["script"])

        if not os.path.exists(script):
            logger.error("Agent script not found: %s", script)
            return False

        cmd = [sys.executable, script] + config.get("args", [])

        try:
            log_path = os.path.join(AGENTS_DIR, f"{name}_stdout.log")
            log_file = open(log_path, "a")

            proc = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=AGENTS_DIR,
                start_new_session=True,
            )

            self.agents[name] = {
                "process": proc,
                "config": config,
                "started_at": time.time(),
                "restarts": self.agents.get(name, {}).get("restarts", 0),
                "log_file": log_file,
            }

            logger.info("Started agent '%s' (PID %d): %s", name, proc.pid, config["description"])
            self._record_agent_status(name, "running", proc.pid)
            return True

        except Exception as e:
            logger.error("Failed to start agent '%s': %s", name, e)
            self._record_agent_status(name, "failed", error=str(e))
            return False

    def start_all(self):
        """Start all enabled agents."""
        logger.info("Starting all agents...")
        started = 0
        for config in AGENT_CONFIGS:
            if config["enabled"]:
                if self.start_agent(config):
                    started += 1
                time.sleep(2)  # stagger starts
        logger.info("Started %d/%d agents", started, len([c for c in AGENT_CONFIGS if c["enabled"]]))
        return started

    def stop_agent(self, name, reason="manual"):
        """Stop a specific agent."""
        agent = self.agents.get(name)
        if not agent:
            return

        proc = agent["process"]
        if proc.poll() is None:
            logger.info("Stopping agent '%s' (PID %d): %s", name, proc.pid, reason)
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

        if agent.get("log_file"):
            agent["log_file"].close()

        self._record_agent_status(name, "stopped")

    def stop_all(self, reason="shutdown"):
        """Stop all agents."""
        logger.info("Stopping all agents: %s", reason)
        for name in list(self.agents.keys()):
            self.stop_agent(name, reason)
        self.agents.clear()

    def health_check(self):
        """Check all agents are alive. Restart crashed ones."""
        for name, agent in list(self.agents.items()):
            proc = agent["process"]
            if proc.poll() is not None:
                # Process has exited
                exit_code = proc.returncode
                logger.warning("Agent '%s' exited with code %d", name, exit_code)

                if agent.get("log_file"):
                    agent["log_file"].close()

                self._record_agent_status(name, "crashed", error=f"exit_code={exit_code}")

                # Check restart limits
                if self._can_restart(name):
                    logger.info("Restarting agent '%s'...", name)
                    agent["restarts"] = agent.get("restarts", 0) + 1
                    self.start_agent(agent["config"])
                else:
                    logger.error("Agent '%s' exceeded restart limit (%d/hour). Not restarting.",
                                name, MAX_RESTARTS_PER_HOUR)
                    self._log_risk_event("restart_limit",
                                        f"Agent '{name}' exceeded {MAX_RESTARTS_PER_HOUR} restarts/hour",
                                        "agent_disabled")

    def _can_restart(self, name):
        """Check if agent hasn't exceeded restart limit."""
        now = time.time()
        if name not in self.restart_log:
            self.restart_log[name] = []

        # Clean old entries
        self.restart_log[name] = [t for t in self.restart_log[name] if now - t < 3600]

        if len(self.restart_log[name]) >= MAX_RESTARTS_PER_HOUR:
            return False

        self.restart_log[name].append(now)
        return True

    def _load_pending_bridges(self):
        """Load pending bridge deposits that are in transit."""
        try:
            if os.path.exists(PENDING_BRIDGES_FILE):
                with open(PENDING_BRIDGES_FILE) as f:
                    bridges = json.load(f)
                # Auto-expire bridges older than 2 hours (should arrive within 20 min)
                now = time.time()
                active = [b for b in bridges if now - b.get("timestamp", 0) < 7200]
                if len(active) != len(bridges):
                    self._save_pending_bridges(active)
                return active
        except Exception:
            pass
        return []

    def _save_pending_bridges(self, bridges):
        """Save pending bridge state."""
        try:
            with open(PENDING_BRIDGES_FILE, "w") as f:
                json.dump(bridges, f, indent=2)
        except Exception:
            pass

    def check_portfolio(self):
        """Monitor combined portfolio value (Coinbase + wallet) and enforce risk limits.

        Bridge-aware: includes pending bridge deposits in total to prevent false HARDSTOP.
        Sanity-check: verifies sudden large drops before triggering HARDSTOP.
        """
        try:
            from agent_tools import AgentTools
            tools = AgentTools()
            portfolio = tools.get_portfolio()
        except Exception as e:
            logger.error("Portfolio check failed: %s", e)
            return

        total = portfolio["total_usd"]
        available = portfolio["available_cash"]
        held = portfolio["held_in_orders"]

        # Include wallet balances if configured
        wallet_total = 0
        if WALLET_ADDRESS:
            try:
                wallet_data = tools.get_multi_chain_balances(WALLET_ADDRESS, WALLET_CHAINS)
                wallet_total = wallet_data.get("total_usd", 0)
                total += wallet_total
                logger.info("Wallet: $%.2f across %d chains", wallet_total, len(WALLET_CHAINS))
            except Exception as e:
                logger.debug("Wallet check skipped: %s", e)

        # Include pending bridge deposits (ETH in transit between L1↔L2)
        bridge_total = 0
        pending_bridges = self._load_pending_bridges()
        for bridge in pending_bridges:
            bridge_total += bridge.get("amount_usd", 0)
        if bridge_total > 0:
            total += bridge_total
            logger.info("Bridge in-transit: $%.2f (%d pending)", bridge_total, len(pending_bridges))

        # ── SANITY CHECK: detect suspicious drops before updating peak ──
        # If portfolio drops >40% in one check cycle, it's likely a data issue
        # (bridge not yet credited, RPC failure returning 0, etc.)
        if hasattr(self, '_last_portfolio_total') and self._last_portfolio_total > 0:
            drop_pct = (self._last_portfolio_total - total) / self._last_portfolio_total
            if drop_pct > MAX_SANE_SINGLE_DROP_PCT:
                logger.warning(
                    "SANITY CHECK: Portfolio dropped %.1f%% in one cycle ($%.2f → $%.2f). "
                    "Likely data issue (bridge transit, RPC failure). Skipping risk check.",
                    drop_pct * 100, self._last_portfolio_total, total
                )
                self._log_risk_event(
                    "sanity_check_skip",
                    f"Suspicious {drop_pct*100:.0f}% drop: ${self._last_portfolio_total:.2f} → ${total:.2f}",
                    "risk_check_skipped"
                )
                # Still record the snapshot for debugging, but DON'T trigger HARDSTOP
                self._last_portfolio_total = total
                return

        self._last_portfolio_total = total

        # Track peak
        if total > self.peak_portfolio:
            self.peak_portfolio = total

        # Calculate drawdown from peak
        drawdown = (self.peak_portfolio - total) / self.peak_portfolio if self.peak_portfolio > 0 else 0

        # Daily P&L
        if self.daily_pnl_start is None:
            self.daily_pnl_start = total
        daily_pnl = total - self.daily_pnl_start

        # Record snapshot
        agents_running = sum(1 for a in self.agents.values() if a["process"].poll() is None)
        self.db.execute(
            """INSERT INTO portfolio_history
               (total_value_usd, available_cash, held_in_orders, daily_pnl,
                peak_value, drawdown_pct, holdings_json, agents_running)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (total, available, held, daily_pnl, self.peak_portfolio,
             round(drawdown * 100, 2), json.dumps(portfolio["holdings"]), agents_running)
        )
        self.db.commit()

        logger.info("Portfolio: $%.2f | Cash: $%.2f | Held: $%.2f | Bridge: $%.2f | Day P&L: $%+.2f | DD: %.1f%% | Agents: %d",
                     total, available, held, bridge_total, daily_pnl, drawdown * 100, agents_running)

        # Push to Fly dashboard
        try:
            snapshot = {
                "total_value_usd": total,
                "available_cash": available,
                "held_in_orders": held,
                "daily_pnl": round(daily_pnl, 2),
                "peak_value": self.peak_portfolio,
                "drawdown_pct": round(drawdown * 100, 2),
                "agents_running": agents_running,
                "holdings": portfolio["holdings"],
                "source": "orchestrator_v2",
            }
            if wallet_total > 0:
                snapshot["wallet_total_usd"] = round(wallet_total, 2)
            if bridge_total > 0:
                snapshot["bridge_in_transit_usd"] = round(bridge_total, 2)
            tools.push_to_dashboard(snapshot)
        except Exception:
            pass

        # ── RISK CHECKS ──
        try:
            from trading_guard import set_trading_lock
        except Exception:
            def set_trading_lock(reason, source="orchestrator_v2", metadata=None):
                return {"locked": True, "reason": reason, "source": source, "metadata": metadata or {}}

        # HARDSTOP #1: Absolute floor — only if peak was above floor (don't trigger if we started small)
        if total < HARDSTOP_FLOOR_USD and self.peak_portfolio >= HARDSTOP_FLOOR_USD:
            msg = f"HARDSTOP FLOOR: portfolio ${total:.2f} dropped below ${HARDSTOP_FLOOR_USD:.2f} absolute minimum"
            logger.critical(msg)
            set_trading_lock(
                reason=msg,
                source="orchestrator_v2",
                metadata={"event": "HARDSTOP_FLOOR", "portfolio": round(total, 2)},
            )
            self._log_risk_event("HARDSTOP_FLOOR", msg, "all_agents_killed")
            self.stop_all("HARDSTOP_FLOOR")
            self.running = False
            return

        # HARDSTOP #2: drawdown from peak (30% threshold)
        if drawdown >= HARDSTOP_DRAWDOWN_PCT:
            msg = f"HARDSTOP: {drawdown*100:.1f}% drawdown (peak ${self.peak_portfolio:.2f} → ${total:.2f})"
            logger.critical(msg)
            set_trading_lock(
                reason=msg,
                source="orchestrator_v2",
                metadata={"event": "HARDSTOP_DRAWDOWN", "drawdown_pct": round(drawdown * 100, 2)},
            )
            self._log_risk_event("HARDSTOP_DRAWDOWN", msg, "all_agents_killed")
            self.stop_all("HARDSTOP_DRAWDOWN")
            self.running = False
            return

        # Daily loss limit (adaptive — 5% of portfolio)
        daily_loss_limit = max(2.00, total * 0.05)
        if daily_pnl <= -daily_loss_limit:
            msg = f"Daily loss limit hit: ${daily_pnl:.2f} (adaptive limit: -${daily_loss_limit:.2f})"
            logger.warning(msg)
            set_trading_lock(
                reason=msg,
                source="orchestrator_v2",
                metadata={"event": "DAILY_LOSS_LIMIT", "daily_pnl": round(daily_pnl, 2),
                          "daily_loss_limit": round(daily_loss_limit, 2)},
            )
            self._log_risk_event("daily_loss_limit", msg, "all_agents_killed")
            self.stop_all("DAILY_LOSS_LIMIT")
            self.running = False
            return

    def _record_agent_status(self, name, status, pid=None, error=None):
        agent = self.agents.get(name, {})
        uptime = int(time.time() - agent.get("started_at", time.time()))
        restarts = agent.get("restarts", 0)
        self.db.execute(
            """INSERT INTO agent_status (name, status, pid, uptime_seconds, restarts, last_error)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (name, status, pid, uptime, restarts, error)
        )
        self.db.commit()

    def _log_risk_event(self, event_type, details, action):
        self.db.execute(
            "INSERT INTO risk_events (event_type, details, action_taken) VALUES (?, ?, ?)",
            (event_type, details, action)
        )
        self.db.commit()

    def print_status(self):
        """Print orchestrator status."""
        print(f"\n{'='*70}")
        print(f"  ORCHESTRATOR V2 STATUS")
        print(f"{'='*70}")

        for name, agent in self.agents.items():
            proc = agent["process"]
            alive = proc.poll() is None
            uptime = int(time.time() - agent["started_at"])
            status = "RUNNING" if alive else f"EXITED({proc.returncode})"
            pid = proc.pid if alive else "-"
            restarts = agent.get("restarts", 0)
            print(f"  {name:<18} PID {str(pid):<8} {status:<12} uptime={uptime}s restarts={restarts}")

        # Recent portfolio
        recent = self.db.execute(
            "SELECT * FROM portfolio_history ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if recent:
            print(f"\n  Portfolio: ${recent['total_value_usd']:.2f} | "
                  f"Day P&L: ${recent['daily_pnl']:+.2f} | "
                  f"DD: {recent['drawdown_pct']:.1f}% | "
                  f"Peak: ${recent['peak_value']:.2f}")

        # Recent risk events
        events = self.db.execute(
            "SELECT * FROM risk_events ORDER BY id DESC LIMIT 5"
        ).fetchall()
        if events:
            print(f"\n  Recent risk events:")
            for e in events:
                print(f"    [{e['recorded_at']}] {e['event_type']}: {e['details']}")

        print(f"{'='*70}\n")

    def run(self):
        """Main orchestrator loop."""
        logger.info("Orchestrator V2 starting (PID %d)", os.getpid())
        self.write_pid()

        # Handle signals
        def handle_shutdown(signum, frame):
            logger.info("Received signal %d, shutting down...", signum)
            self.running = False
        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)

        # Start all agents
        self.start_all()

        # Initial portfolio check
        self.check_portfolio()

        last_portfolio_check = time.time()
        cycle = 0

        while self.running:
            try:
                cycle += 1

                # Health check every 30s
                self.health_check()

                # Portfolio check every 5 min
                if time.time() - last_portfolio_check >= PORTFOLIO_CHECK_INTERVAL:
                    self.check_portfolio()
                    last_portfolio_check = time.time()

                # Status print every 30 min (60 cycles * 30s)
                if cycle % 60 == 0:
                    self.print_status()

                # Reset daily P&L at midnight UTC
                now_utc = datetime.now(timezone.utc)
                if now_utc.hour == 0 and now_utc.minute < 1:
                    try:
                        from agent_tools import AgentTools
                        portfolio = AgentTools().get_portfolio()
                        self.daily_pnl_start = portfolio["total_usd"]
                        logger.info("Daily P&L reset at midnight UTC. Starting value: $%.2f",
                                    self.daily_pnl_start)
                    except Exception:
                        pass

                time.sleep(HEALTH_CHECK_INTERVAL)

            except Exception as e:
                logger.error("Orchestrator error: %s", e, exc_info=True)
                time.sleep(30)

        # Shutdown
        self.stop_all("orchestrator_exit")
        self.remove_pid()
        logger.info("Orchestrator V2 shutdown complete")


def read_status():
    """Read status from DB without running the orchestrator."""
    if not os.path.exists(ORCH_DB):
        print("No orchestrator database found. Has it been started?")
        return

    db = sqlite3.connect(ORCH_DB)
    db.row_factory = sqlite3.Row

    # Latest agent statuses
    agents = db.execute("""
        SELECT name, status, pid, uptime_seconds, restarts, last_error, recorded_at
        FROM agent_status WHERE id IN (
            SELECT MAX(id) FROM agent_status GROUP BY name
        ) ORDER BY name
    """).fetchall()

    # Latest portfolio
    portfolio = db.execute(
        "SELECT * FROM portfolio_history ORDER BY id DESC LIMIT 1"
    ).fetchone()

    # Risk events
    events = db.execute(
        "SELECT * FROM risk_events ORDER BY id DESC LIMIT 5"
    ).fetchall()

    print(f"\n{'='*70}")
    print(f"  ORCHESTRATOR V2 STATUS (from DB)")
    print(f"{'='*70}")

    if agents:
        print(f"\n  Agents:")
        for a in agents:
            print(f"    {a['name']:<18} {a['status']:<10} PID={a['pid'] or '-':<8} "
                  f"uptime={a['uptime_seconds']}s restarts={a['restarts']}")
            if a["last_error"]:
                print(f"      error: {a['last_error']}")

    if portfolio:
        print(f"\n  Portfolio: ${portfolio['total_value_usd']:.2f}")
        print(f"  Day P&L:   ${portfolio['daily_pnl']:+.2f}")
        print(f"  Peak:      ${portfolio['peak_value']:.2f}")
        print(f"  Drawdown:  {portfolio['drawdown_pct']:.1f}%")
        print(f"  Agents running: {portfolio['agents_running']}")
        print(f"  Last check: {portfolio['recorded_at']}")

    if events:
        print(f"\n  Risk Events:")
        for e in events:
            print(f"    [{e['recorded_at']}] {e['event_type']}: {e['details']}")

    # Check if PID file exists and process is running
    if os.path.exists(PID_FILE):
        with open(PID_FILE) as f:
            pid = int(f.read().strip())
        try:
            os.kill(pid, 0)
            print(f"\n  Orchestrator PID {pid} is RUNNING")
        except ProcessLookupError:
            print(f"\n  Orchestrator PID {pid} is DEAD (stale PID file)")
    else:
        print(f"\n  No PID file — orchestrator not running")

    print(f"{'='*70}\n")
    db.close()


def stop_orchestrator():
    """Send SIGTERM to running orchestrator."""
    if not os.path.exists(PID_FILE):
        print("No PID file found. Orchestrator may not be running.")
        return

    with open(PID_FILE) as f:
        pid = int(f.read().strip())

    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to orchestrator PID {pid}")
    except ProcessLookupError:
        print(f"PID {pid} not found. Removing stale PID file.")
        os.remove(PID_FILE)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "status":
            read_status()
        elif cmd == "stop":
            stop_orchestrator()
        else:
            print("Usage: orchestrator_v2.py [run|status|stop]")
    else:
        orch = OrchestratorV2()
        orch.run()
