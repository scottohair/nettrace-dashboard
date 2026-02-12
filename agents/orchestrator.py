#!/usr/bin/env python3
"""NetTrace Agent Orchestrator — manages persistent worker agents.

Each agent has a role, KPIs, and reports revenue-attributed metrics.
Bottom 10% performers get replaced. Agents can propose hiring new agents.

Runs on local hardware (M1 Max / M2 Ultra) for free compute.
"""

import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

DB_PATH = os.environ.get("AGENT_DB", str(Path(__file__).parent / "agents.db"))
FLY_API_URL = "https://nettrace-dashboard.fly.dev"
MAX_AGENTS = 15
EVAL_INTERVAL = 3600  # evaluate performance every hour
FIRE_BOTTOM_PCT = 0.10  # fire bottom 10%


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db


def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS agents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            role TEXT NOT NULL,
            engine TEXT NOT NULL,  -- 'claude' or 'codex'
            status TEXT DEFAULT 'active',  -- active, fired, paused
            pid INTEGER,
            revenue_attributed REAL DEFAULT 0.0,
            tasks_completed INTEGER DEFAULT 0,
            tasks_failed INTEGER DEFAULT 0,
            api_calls_generated INTEGER DEFAULT 0,
            signups_driven INTEGER DEFAULT 0,
            content_pieces INTEGER DEFAULT 0,
            last_heartbeat TIMESTAMP,
            hired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fired_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS agent_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            result TEXT,
            revenue_delta REAL DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (agent_id) REFERENCES agents(id)
        );

        CREATE TABLE IF NOT EXISTS agent_proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposing_agent_id INTEGER,
            proposal_type TEXT,  -- 'hire', 'strategy', 'pivot'
            description TEXT,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS revenue_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            amount REAL NOT NULL,
            currency TEXT DEFAULT 'USD',
            agent_id INTEGER,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    db.commit()
    db.close()


def hire_agent(name, role, engine):
    """Register a new agent."""
    db = get_db()
    active = db.execute("SELECT COUNT(*) as cnt FROM agents WHERE status='active'").fetchone()["cnt"]
    if active >= MAX_AGENTS:
        print(f"[ORCHESTRATOR] Cannot hire {name}: max {MAX_AGENTS} agents reached")
        return False
    try:
        db.execute(
            "INSERT INTO agents (name, role, engine) VALUES (?, ?, ?)",
            (name, role, engine)
        )
        db.commit()
        print(f"[ORCHESTRATOR] Hired {name} ({engine}) as {role}")
        return True
    except sqlite3.IntegrityError:
        print(f"[ORCHESTRATOR] {name} already exists")
        return False
    finally:
        db.close()


def fire_bottom_performers():
    """Fire bottom 10% by revenue_attributed."""
    db = get_db()
    active = db.execute(
        "SELECT * FROM agents WHERE status='active' ORDER BY revenue_attributed ASC"
    ).fetchall()
    if len(active) < 3:
        db.close()
        return []

    n_fire = max(1, int(len(active) * FIRE_BOTTOM_PCT))
    fired = []
    for agent in active[:n_fire]:
        db.execute(
            "UPDATE agents SET status='fired', fired_at=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), agent["id"])
        )
        # Kill process if running
        if agent["pid"]:
            try:
                os.kill(agent["pid"], 9)
            except (ProcessLookupError, PermissionError):
                pass
        fired.append(agent["name"])
        print(f"[ORCHESTRATOR] Fired {agent['name']} (revenue: ${agent['revenue_attributed']:.2f})")

    db.commit()
    db.close()
    return fired


def log_action(agent_name, action, result=None, revenue_delta=0.0):
    """Log an agent action with optional revenue attribution."""
    db = get_db()
    agent = db.execute("SELECT id FROM agents WHERE name=?", (agent_name,)).fetchone()
    if not agent:
        db.close()
        return
    db.execute(
        "INSERT INTO agent_logs (agent_id, action, result, revenue_delta) VALUES (?, ?, ?, ?)",
        (agent["id"], action, result, revenue_delta)
    )
    if revenue_delta != 0:
        db.execute(
            "UPDATE agents SET revenue_attributed = revenue_attributed + ? WHERE id=?",
            (revenue_delta, agent["id"])
        )
    db.execute(
        "UPDATE agents SET tasks_completed = tasks_completed + 1, last_heartbeat = ? WHERE id=?",
        (datetime.now(timezone.utc).isoformat(), agent["id"])
    )
    db.commit()
    db.close()


def get_scoreboard():
    """Return ranked agent performance."""
    db = get_db()
    agents = db.execute(
        "SELECT * FROM agents WHERE status='active' ORDER BY revenue_attributed DESC"
    ).fetchall()
    db.close()
    return [dict(a) for a in agents]


def record_revenue(source, amount, currency="USD", agent_name=None, description=""):
    """Record actual revenue."""
    db = get_db()
    agent_id = None
    if agent_name:
        agent = db.execute("SELECT id FROM agents WHERE name=?", (agent_name,)).fetchone()
        if agent:
            agent_id = agent["id"]
            db.execute(
                "UPDATE agents SET revenue_attributed = revenue_attributed + ? WHERE id=?",
                (amount, agent_id)
            )
    db.execute(
        "INSERT INTO revenue_ledger (source, amount, currency, agent_id, description) VALUES (?, ?, ?, ?, ?)",
        (source, amount, currency, agent_id, description)
    )
    db.commit()
    total = db.execute("SELECT SUM(amount) as total FROM revenue_ledger").fetchone()["total"] or 0
    db.close()
    print(f"[REVENUE] +${amount:.2f} {currency} from {source} | Total: ${total:.2f}")
    return total


def print_dashboard():
    """Print agent performance dashboard."""
    db = get_db()
    agents = db.execute(
        "SELECT * FROM agents ORDER BY status ASC, revenue_attributed DESC"
    ).fetchall()
    total_rev = db.execute("SELECT SUM(amount) as t FROM revenue_ledger").fetchone()["t"] or 0
    db.close()

    print("\n" + "=" * 70)
    print(f"  NETTRACE AGENT DASHBOARD | Total Revenue: ${total_rev:.2f}")
    print("=" * 70)
    print(f"{'#':<3} {'Name':<20} {'Role':<18} {'Engine':<8} {'Revenue':<10} {'Tasks':<6} {'Status'}")
    print("-" * 70)
    for i, a in enumerate(agents, 1):
        status = a['status'].upper()
        rev = f"${a['revenue_attributed']:.2f}"
        print(f"{i:<3} {a['name']:<20} {a['role']:<18} {a['engine']:<8} {rev:<10} {a['tasks_completed']:<6} {status}")
    print("=" * 70 + "\n")


# ── Initial Agent Roster ─────────────────────────────────────────────

INITIAL_AGENTS = [
    # Claude agents
    ("scanner-alpha", "data_collection", "claude"),
    ("signal-gen", "quant_signals", "claude"),
    ("seo-writer", "content_marketing", "claude"),
    # Codex agents
    ("codex-trader", "strategy_research", "codex"),
    ("codex-builder", "feature_development", "codex"),
    ("codex-outreach", "customer_acquisition", "codex"),
]


if __name__ == "__main__":
    init_db()

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "hire":
            hire_agent(sys.argv[2], sys.argv[3], sys.argv[4])
        elif cmd == "fire":
            fire_bottom_performers()
        elif cmd == "score":
            print_dashboard()
        elif cmd == "revenue":
            record_revenue(sys.argv[2], float(sys.argv[3]),
                          agent_name=sys.argv[4] if len(sys.argv) > 4 else None)
        elif cmd == "log":
            log_action(sys.argv[2], sys.argv[3],
                      revenue_delta=float(sys.argv[4]) if len(sys.argv) > 4 else 0)
        elif cmd == "init":
            for name, role, engine in INITIAL_AGENTS:
                hire_agent(name, role, engine)
            print_dashboard()
        else:
            print(f"Usage: {sys.argv[0]} [init|score|fire|hire|revenue|log]")
    else:
        # Initialize default agents
        for name, role, engine in INITIAL_AGENTS:
            hire_agent(name, role, engine)
        print_dashboard()
