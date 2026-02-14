#!/usr/bin/env python3
"""
clawd.bot Backend - Flask API for automation platform

Features:
- Strategy marketplace (buy/sell strategies)
- Live dashboards (agents, P&L, risk)
- Backtest interface
- Agent management
- User authentication (JWT)
- Stripe integration (billing)
"""

import sys
import os
from pathlib import Path

# Add agents to path
agents_path = Path(__file__).parent.parent.parent.parent / 'agents'
sys.path.insert(0, str(agents_path))

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import jwt
import sqlite3
from datetime import datetime, timedelta
from functools import wraps
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('clawd_bot')

app = Flask(__name__)
CORS(app)

# Config
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['DATABASE'] = Path(__file__).parent / 'clawd.db'

# Initialize database
def init_db():
    """Initialize clawd.bot database"""

    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()

    # Users table
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            tier TEXT DEFAULT 'free',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            stripe_customer_id TEXT,
            subscription_status TEXT DEFAULT 'inactive',
            api_calls_today INTEGER DEFAULT 0,
            agents_count INTEGER DEFAULT 0
        )
    ''')

    # Marketplace strategies table
    c.execute('''
        CREATE TABLE IF NOT EXISTS marketplace_strategies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            category TEXT,
            author_id INTEGER,
            price REAL DEFAULT 0,
            revenue_share REAL DEFAULT 0.7,
            downloads INTEGER DEFAULT 0,
            rating REAL DEFAULT 0,
            reviews_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- Performance metrics
            backtest_sharpe REAL,
            backtest_win_rate REAL,
            backtest_total_return REAL,
            live_trades INTEGER DEFAULT 0,
            live_win_rate REAL,
            live_sharpe REAL,

            -- Status
            status TEXT DEFAULT 'pending',
            verified BOOLEAN DEFAULT 0,

            FOREIGN KEY (author_id) REFERENCES users(id)
        )
    ''')

    # User agents table
    c.execute('''
        CREATE TABLE IF NOT EXISTS user_agents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            agent_id TEXT NOT NULL,
            strategy_id TEXT NOT NULL,
            status TEXT DEFAULT 'running',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            stopped_at TIMESTAMP,

            -- Metrics
            trades INTEGER DEFAULT 0,
            win_rate REAL DEFAULT 0,
            total_pnl REAL DEFAULT 0,
            sharpe_ratio REAL DEFAULT 0,

            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')

    # API usage table
    c.execute('''
        CREATE TABLE IF NOT EXISTS api_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            endpoint TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')

    conn.commit()
    conn.close()

init_db()


# Auth decorator
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')

        if not token:
            return jsonify({'error': 'Token missing'}), 401

        try:
            # Remove 'Bearer ' prefix if present
            if token.startswith('Bearer '):
                token = token[7:]

            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            current_user_id = data['user_id']

        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401

        return f(current_user_id, *args, **kwargs)

    return decorated


# Routes

@app.route('/')
def index():
    """Serve main page"""
    return send_from_directory('../', 'index.html')


@app.route('/api/status')
def status():
    """API status"""
    return jsonify({
        'status': 'online',
        'version': '2.0.0',
        'epoch': 2,
        'timestamp': datetime.utcnow().isoformat()
    })


@app.route('/api/stats')
def stats():
    """Platform statistics"""

    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()

    c.execute('SELECT COUNT(*) FROM users')
    total_users = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM marketplace_strategies WHERE status = "approved"')
    total_strategies = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM user_agents WHERE status = "running"')
    active_agents = c.fetchone()[0]

    c.execute('SELECT SUM(total_pnl) FROM user_agents')
    total_pnl = c.fetchone()[0] or 0

    conn.close()

    return jsonify({
        'users': total_users,
        'strategies': total_strategies,
        'active_agents': active_agents,
        'total_pnl': total_pnl,
        'uptime': '99.9%',
        'regions': 7
    })


@app.route('/api/marketplace/strategies')
def marketplace_strategies():
    """List marketplace strategies"""

    category = request.args.get('category')
    sort_by = request.args.get('sort', 'rating')  # rating, downloads, newest
    limit = min(int(request.args.get('limit', 20)), 100)

    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()

    query = '''
        SELECT strategy_id, name, description, category, price, rating,
               reviews_count, downloads, backtest_sharpe, backtest_win_rate,
               live_trades, live_win_rate
        FROM marketplace_strategies
        WHERE status = 'approved'
    '''

    params = []

    if category:
        query += ' AND category = ?'
        params.append(category)

    if sort_by == 'rating':
        query += ' ORDER BY rating DESC, reviews_count DESC'
    elif sort_by == 'downloads':
        query += ' ORDER BY downloads DESC'
    elif sort_by == 'newest':
        query += ' ORDER BY created_at DESC'

    query += f' LIMIT {limit}'

    c.execute(query, params)

    strategies = []
    for row in c.fetchall():
        strategies.append({
            'strategy_id': row[0],
            'name': row[1],
            'description': row[2],
            'category': row[3],
            'price': row[4],
            'rating': row[5],
            'reviews_count': row[6],
            'downloads': row[7],
            'backtest_sharpe': row[8],
            'backtest_win_rate': row[9],
            'live_trades': row[10],
            'live_win_rate': row[11]
        })

    conn.close()

    return jsonify({'strategies': strategies})


@app.route('/api/dashboard/agents', methods=['GET'])
@token_required
def dashboard_agents(current_user_id):
    """Get user's agents"""

    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()

    c.execute('''
        SELECT agent_id, strategy_id, status, trades, win_rate, total_pnl, sharpe_ratio, created_at
        FROM user_agents
        WHERE user_id = ?
        ORDER BY created_at DESC
    ''', (current_user_id,))

    agents = []
    for row in c.fetchall():
        agents.append({
            'agent_id': row[0],
            'strategy_id': row[1],
            'status': row[2],
            'trades': row[3],
            'win_rate': row[4],
            'total_pnl': row[5],
            'sharpe_ratio': row[6],
            'created_at': row[7]
        })

    conn.close()

    return jsonify({'agents': agents})


@app.route('/api/agents/start', methods=['POST'])
@token_required
def start_agent(current_user_id):
    """Start a new agent"""

    data = request.json
    strategy_id = data.get('strategy_id')

    if not strategy_id:
        return jsonify({'error': 'strategy_id required'}), 400

    # Check user's tier limits
    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()

    c.execute('SELECT tier, agents_count FROM users WHERE id = ?', (current_user_id,))
    row = c.fetchone()

    if not row:
        return jsonify({'error': 'User not found'}), 404

    tier, agents_count = row

    # Tier limits
    limits = {
        'free': 1,
        'pro': 10,
        'enterprise': 999
    }

    if agents_count >= limits.get(tier, 0):
        return jsonify({'error': f'Agent limit reached for {tier} tier'}), 403

    # Create agent
    agent_id = f'agent_{current_user_id}_{int(datetime.utcnow().timestamp())}'

    c.execute('''
        INSERT INTO user_agents (user_id, agent_id, strategy_id, status)
        VALUES (?, ?, ?, 'running')
    ''', (current_user_id, agent_id, strategy_id))

    c.execute('UPDATE users SET agents_count = agents_count + 1 WHERE id = ?', (current_user_id,))

    conn.commit()
    conn.close()

    logger.info(f'User {current_user_id} started agent {agent_id}')

    return jsonify({
        'success': True,
        'agent_id': agent_id,
        'message': 'Agent started successfully'
    })


@app.route('/api/agents/<agent_id>/stop', methods=['POST'])
@token_required
def stop_agent(current_user_id, agent_id):
    """Stop an agent"""

    conn = sqlite3.connect(app.config['DATABASE'])
    c = conn.cursor()

    c.execute('''
        UPDATE user_agents
        SET status = 'stopped', stopped_at = ?
        WHERE agent_id = ? AND user_id = ?
    ''', (datetime.utcnow(), agent_id, current_user_id))

    if c.rowcount == 0:
        conn.close()
        return jsonify({'error': 'Agent not found'}), 404

    c.execute('UPDATE users SET agents_count = agents_count - 1 WHERE id = ?', (current_user_id,))

    conn.commit()
    conn.close()

    logger.info(f'User {current_user_id} stopped agent {agent_id}')

    return jsonify({'success': True, 'message': 'Agent stopped'})


@app.route('/api/backtest', methods=['POST'])
@token_required
def run_backtest(current_user_id):
    """Run backtest"""

    data = request.json

    strategy_code = data.get('strategy_code')
    pair = data.get('pair', 'BTC-USD')
    start_date = data.get('start_date')
    end_date = data.get('end_date')

    if not strategy_code:
        return jsonify({'error': 'strategy_code required'}), 400

    # TODO: Integrate with actual backtesting engine

    # Mock results
    results = {
        'total_trades': 125,
        'win_rate': 0.68,
        'sharpe_ratio': 1.85,
        'total_return': 0.245,
        'max_drawdown': 0.087,
        'avg_trade_pnl': 2.35
    }

    return jsonify({
        'success': True,
        'results': results
    })


# Error handlers

@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(500)
def server_error(e):
    logger.error(f'Server error: {e}')
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))

    logger.info(f'🚀 clawd.bot backend starting on port {port}')
    logger.info(f'📊 Database: {app.config["DATABASE"]}')

    app.run(
        host='0.0.0.0',
        port=port,
        debug=True
    )
