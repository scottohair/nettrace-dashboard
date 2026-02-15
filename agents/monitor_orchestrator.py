#!/usr/bin/env python3
"""Monitor and expose realtime orchestrator metrics via database."""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path

def create_metrics_table(db_path):
    """Ensure orchestrator_metrics table exists."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orchestrator_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_ms REAL,
            cycle_count INTEGER,
            total_latency_ms REAL,
            avg_latency_ms REAL,
            total_gains_usd REAL,
            gains_per_second REAL,
            gains_per_ms REAL,
            trade_count INTEGER,
            signal_count INTEGER,
            executed_count INTEGER,
            top_agents_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def update_metrics(db_path, metrics):
    """Update orchestrator metrics in database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO orchestrator_metrics (
            timestamp_ms, cycle_count, total_latency_ms, avg_latency_ms,
            total_gains_usd, gains_per_second, gains_per_ms, trade_count,
            signal_count, executed_count, top_agents_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        metrics.get('timestamp_ms'),
        metrics.get('cycle_count'),
        metrics.get('total_latency_ms'),
        metrics.get('avg_latency_ms'),
        metrics.get('total_gains_usd'),
        metrics.get('gains_per_second'),
        metrics.get('gains_per_ms'),
        metrics.get('trade_count'),
        metrics.get('signal_count'),
        metrics.get('executed_count'),
        json.dumps(metrics.get('top_agents', []))
    ))
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    db_path = Path(__file__).parent.parent / "traceroute.db"
    create_metrics_table(db_path)
    print(f"✅ Orchestrator metrics table created/verified at {db_path}")
