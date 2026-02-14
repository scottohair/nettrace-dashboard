#!/usr/bin/env python3
"""
Simple Flask server for clawd.bot website
"""
from flask import Flask, send_file, jsonify
from flask_cors import CORS
import os
from pathlib import Path

app = Flask(__name__)
CORS(app)

BASE_DIR = Path(__file__).parent

@app.route("/")
def index():
    """Serve main landing page"""
    return send_file(BASE_DIR / "index.html")

@app.route("/api/status")
def api_status():
    """API health check"""
    return jsonify({
        "status": "online",
        "platform": "clawd.bot",
        "agents_active": 95,
        "regions": 7,
        "ideas": 858,
        "uptime": "24/7"
    })

@app.route("/api/stats")
def api_stats():
    """Live stats from main platform"""
    # In production, fetch from ~/src/quant/app.py
    return jsonify({
        "portfolio_value_usd": 52.51,
        "daily_pnl_usd": -0.12,
        "active_positions": 0,
        "regions_online": 7,
        "recent_signals": [
            {"type": "latency_spike", "asset": "BTC", "region": "ewr", "confidence": 0.92},
            {"type": "rsi_oversold", "asset": "ETH", "region": "lhr", "confidence": 0.78},
            {"type": "arbitrage", "asset": "SOL", "region": "sin", "confidence": 0.85}
        ]
    })

if __name__ == "__main__":
    print("🚀 Starting Clawd.bot website on http://localhost:8000")
    print("📍 Public access via:")
    print("   - ngrok http 8000")
    print("   - cloudflared tunnel run --url http://localhost:8000 clawd-bot")
    app.run(host="0.0.0.0", port=8000, debug=True)
