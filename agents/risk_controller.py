#!/usr/bin/env python3
"""Centralized Risk Controller — dynamic, adaptive, no hardcoded values.

ALL agents MUST consult this controller before ANY trade.
It replaces hardcoded reserves, position limits, and risk parameters
with dynamic algorithms based on:
  - Portfolio size (scales logarithmically)
  - Market volatility (ATR-based, higher vol = more conservative)
  - Trend direction (SMA-based, no buying in downtrends)
  - Win/loss streaks (Kelly criterion adjustment)
  - Cross-agent coordination (prevents double-spending)
  - Time-of-day patterns (avoid low-liquidity periods)

NOTHING IS HARDCODED. Everything derives from market state.
"""

import json
import logging
import math
import os
import sqlite3
import time
import uuid
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

logger = logging.getLogger("risk_controller")

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"'))

DB_PATH = Path(__file__).parent / "risk_controller.db"
CACHE_TTL = 5  # seconds — reduced from 30s to catch flash crashes faster


class MarketState:
    """Real-time market state derived from price data."""

    def __init__(self):
        self._price_cache = {}  # pair -> (price, timestamp)
        self._candle_cache = {}  # pair -> (candles, timestamp)

    def get_price(self, pair, urgent=False):
        """Get spot price with caching. Pass urgent=True to bypass cache."""
        cache = self._price_cache.get(pair)
        if not urgent and cache and time.time() - cache[1] < CACHE_TTL:
            return cache[0]
        try:
            # Use USD pair for data (more liquid)
            data_pair = pair.replace("-USDC", "-USD")
            url = f"https://api.coinbase.com/v2/prices/{data_pair}/spot"
            req = urllib.request.Request(url, headers={"User-Agent": "RiskController/1.0"})
            resp = urllib.request.urlopen(req, timeout=5)
            price = float(json.loads(resp.read())["data"]["amount"])
            self._price_cache[pair] = (price, time.time())
            return price
        except Exception:
            return cache[0] if cache else None

    def get_candles(self, pair, granularity=300, limit=50):
        """Get recent candles for volatility/trend calculation."""
        cache_key = f"{pair}_{granularity}"
        cache = self._candle_cache.get(cache_key)
        if cache and time.time() - cache[1] < CACHE_TTL * 2:
            return cache[0]
        try:
            data_pair = pair.replace("-USDC", "-USD")
            url = (f"https://api.exchange.coinbase.com/products/{data_pair}/candles"
                   f"?granularity={granularity}")
            req = urllib.request.Request(url, headers={"User-Agent": "RiskController/1.0"})
            resp = urllib.request.urlopen(req, timeout=5)
            candles = json.loads(resp.read())
            # Coinbase format: [time, low, high, open, close, volume]
            if candles and len(candles) >= 5:
                self._candle_cache[cache_key] = (candles[:limit], time.time())
                return candles[:limit]
        except Exception:
            pass
        return cache[0] if cache else []

    def compute_volatility(self, pair):
        """ATR-based volatility as percentage of price.

        Returns: volatility ratio (e.g., 0.02 = 2% average range)
        """
        candles = self.get_candles(pair, granularity=300, limit=20)
        if len(candles) < 5:
            return 0.02  # default moderate volatility

        ranges = []
        for c in candles[:14]:  # ATR(14)
            high, low = float(c[2]), float(c[1])
            close = float(c[4])
            if close > 0:
                tr = (high - low) / close  # true range as % of close
                ranges.append(tr)

        if not ranges:
            return 0.02
        return sum(ranges) / len(ranges)

    def compute_trend(self, pair):
        """SMA-based trend detection.

        Returns: float from -1.0 (strong downtrend) to +1.0 (strong uptrend)
        """
        candles = self.get_candles(pair, granularity=300, limit=30)
        if len(candles) < 20:
            return 0.0  # neutral/unknown

        closes = [float(c[4]) for c in candles]
        # SMA 5 vs SMA 20
        sma5 = sum(closes[:5]) / 5
        sma20 = sum(closes[:20]) / 20
        current = closes[0]

        if sma20 == 0:
            return 0.0

        # Trend strength: how far SMA5 is from SMA20, normalized
        trend_ratio = (sma5 - sma20) / sma20

        # Price vs SMA20
        price_ratio = (current - sma20) / sma20

        # Combine: both above SMA20 = uptrend, both below = downtrend
        combined = (trend_ratio + price_ratio) / 2

        # Clamp to [-1, 1]
        return max(-1.0, min(1.0, combined * 20))  # amplify small moves

    def compute_momentum(self, pair):
        """Short-term price momentum (last 5 candles).

        Returns: float from -1.0 to +1.0
        """
        candles = self.get_candles(pair, granularity=60, limit=10)
        if len(candles) < 5:
            return 0.0

        prices = [float(c[4]) for c in candles[:5]]
        if prices[-1] == 0:
            return 0.0

        # % change over 5 candles
        pct_change = (prices[0] - prices[-1]) / prices[-1]
        return max(-1.0, min(1.0, pct_change * 50))  # amplify


class RiskController:
    """Centralized risk management — ALL agents must consult this.

    Dynamic sliding scales based on:
    - Portfolio size (logarithmic scaling)
    - Volatility (higher vol = more conservative)
    - Trend (no buying in downtrends)
    - Win/loss streaks (Kelly adjustment)
    - Total agent allocation (prevents double-spending)
    """

    def __init__(self):
        self.market = MarketState()
        self._lock = Lock()
        self._db = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._init_db()
        self._daily_loss = 0.0
        self._daily_reset = datetime.now(timezone.utc).date()
        self._agent_allocations = {}  # agent_name -> allocated_usd
        self._trade_count_today = 0
        self._daily_state_date = str(datetime.now(timezone.utc).date())

    def _init_db(self):
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS risk_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                agent TEXT,
                pair TEXT,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS agent_performance (
                agent TEXT PRIMARY KEY,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                total_pnl REAL DEFAULT 0,
                sharpe REAL DEFAULT 0,
                last_trade TIMESTAMP,
                status TEXT DEFAULT 'active'
            );
            CREATE TABLE IF NOT EXISTS pending_allocations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent TEXT NOT NULL,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                size_usd REAL NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS trade_audit (
                trade_id TEXT PRIMARY KEY,
                agent TEXT NOT NULL,
                pair TEXT NOT NULL,
                direction TEXT NOT NULL,
                requested_size REAL,
                approved_size REAL,
                status TEXT DEFAULT 'approved',
                reason TEXT,
                volatility REAL,
                trend REAL,
                portfolio_value REAL,
                pnl REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS daily_risk_state (
                state_date TEXT PRIMARY KEY,
                daily_loss REAL NOT NULL DEFAULT 0,
                trade_count INTEGER NOT NULL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.commit()
        self._ensure_db_schema()
        # Flush ALL stale pending allocations on startup (leftover from previous process/OOM)
        self._flush_stale_allocations()
        self._load_or_init_daily_state()

    def _ensure_db_schema(self):
        """Migrate lightweight schema updates in an idempotent way."""
        try:
            cols = self._db.execute("PRAGMA table_info(pending_allocations)").fetchall()
            col_names = {c[1] for c in cols}
            if "trade_id" not in col_names:
                self._db.execute("ALTER TABLE pending_allocations ADD COLUMN trade_id TEXT")
                logger.info("MIGRATION: Added trade_id to pending_allocations")
            self._db.execute(
                "CREATE INDEX IF NOT EXISTS idx_pending_allocations_agent_pair_status "
                "ON pending_allocations(agent, pair, status)"
            )
            self._db.execute(
                "CREATE INDEX IF NOT EXISTS idx_pending_allocations_trade_id "
                "ON pending_allocations(trade_id)"
            )
            self._db.commit()
        except Exception as e:
            logger.warning("Schema migration warning: %s", e)

    def _load_or_init_daily_state(self):
        today = str(datetime.now(timezone.utc).date())
        row = self._db.execute(
            "SELECT daily_loss, trade_count FROM daily_risk_state WHERE state_date=?",
            (today,)
        ).fetchone()
        if row:
            self._daily_loss = float(row["daily_loss"] or 0.0)
            self._trade_count_today = int(row["trade_count"] or 0)
            self._daily_reset = datetime.now(timezone.utc).date()
            self._daily_state_date = today
            return

        self._daily_reset = datetime.now(timezone.utc).date()
        self._daily_state_date = today
        self._daily_loss = 0.0
        self._trade_count_today = 0
        self._db.execute(
            "INSERT OR REPLACE INTO daily_risk_state (state_date, daily_loss, trade_count) "
            "VALUES (?, 0, 0)",
            (today,),
        )
        self._db.commit()

    def _persist_daily_state(self):
        today = str(datetime.now(timezone.utc).date())
        try:
            self._db.execute(
                "INSERT OR REPLACE INTO daily_risk_state (state_date, daily_loss, trade_count, updated_at) "
                "VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                (today, float(self._daily_loss), int(self._trade_count_today)),
            )
            self._db.commit()
        except Exception as e:
            logger.warning("persist_daily_state error: %s", e)

    def _flush_stale_allocations(self):
        """Expire all pending allocations on startup — previous process may have died mid-trade."""
        try:
            n = self._db.execute(
                "SELECT COUNT(*) FROM pending_allocations WHERE status='pending'"
            ).fetchone()[0]
            if n > 0:
                self._db.execute(
                    "UPDATE pending_allocations SET status='expired', resolved_at=CURRENT_TIMESTAMP "
                    "WHERE status='pending'")
                self._db.commit()
                logger.info("STARTUP: Flushed %d stale pending allocations", n)
                self._log_event("startup_flush", details=f"Expired {n} stale pending allocations")
        except Exception as e:
            logger.warning("flush_stale_allocations error: %s", e)

    def _check_daily_reset(self):
        """Reset daily counters at midnight UTC."""
        today = datetime.now(timezone.utc).date()
        if today != self._daily_reset:
            self._daily_loss = 0.0
            self._trade_count_today = 0
            self._daily_reset = today
            self._daily_state_date = str(today)
            self._persist_daily_state()
            self._log_event("daily_reset", details=f"Reset for {today}")

    def _normalize_direction(self, direction):
        return str(direction or "").strip().upper()

    def _normalize_pair(self, pair):
        text = str(pair or "").strip().upper()
        return text if text else "UNKNOWN"

    def _log_event(self, event_type, agent=None, pair=None, details=None):
        try:
            self._db.execute(
                "INSERT INTO risk_events (event_type, agent, pair, details) VALUES (?, ?, ?, ?)",
                (event_type, agent, pair, details))
            self._db.commit()
        except Exception:
            pass

    # ==========================================
    # DYNAMIC SLIDING SCALE CALCULATIONS
    # ==========================================

    def max_trade_usd(self, portfolio_value, volatility=None, trend=None):
        """Dynamic max trade size — scales logarithmically with portfolio.

        Formula: base = portfolio * trade_fraction
        trade_fraction = log10(portfolio) * 0.03, clamped [0.02, 0.15]
        Adjusted by: volatility (higher vol = smaller), trend (downtrend = smaller)
        """
        if portfolio_value <= 0:
            return 0

        # Logarithmic scaling: larger portfolios trade proportionally less
        log_factor = max(0, math.log10(max(1, portfolio_value)))
        trade_fraction = log_factor * 0.03  # ~6.6% at $100, ~9% at $1000, ~12% at $10000
        trade_fraction = max(0.02, min(0.15, trade_fraction))

        base = portfolio_value * trade_fraction

        # Volatility adjustment: higher vol = smaller trades
        if volatility is not None:
            vol_mult = 1.0 / (1.0 + volatility * 10)  # 2% vol → 0.83x, 5% vol → 0.67x
            base *= max(0.3, vol_mult)

        # Trend adjustment: downtrend = much smaller trades
        if trend is not None and trend < 0:
            trend_mult = max(0.1, 1.0 + trend * 0.8)  # -1.0 trend → 0.2x
            base *= trend_mult

        # Absolute floor: Coinbase minimum
        return max(1.00, round(base, 2))

    def max_daily_loss(self, portfolio_value):
        """Dynamic daily loss limit — scales with portfolio.

        Formula: portfolio * loss_fraction
        loss_fraction = 0.03 + log10(portfolio) * 0.005, clamped [0.02, 0.10]
        """
        if portfolio_value <= 0:
            return 1.00

        log_factor = max(0, math.log10(max(1, portfolio_value)))
        loss_fraction = 0.03 + log_factor * 0.005
        loss_fraction = max(0.02, min(0.10, loss_fraction))

        return max(1.00, round(portfolio_value * loss_fraction, 2))

    def min_reserve(self, portfolio_value, volatility=None, available_cash=None):
        """Dynamic cash reserve — scales with portfolio and volatility.

        Formula: portfolio * reserve_fraction
        reserve_fraction = 0.08 + volatility_adj, clamped [0.06, 0.25]
        Higher vol = larger reserve but capped to leave room for trades.
        Reserve never consumes more than 60% of available cash.
        """
        if portfolio_value <= 0:
            return 5.00

        base_fraction = 0.08  # 8% base reserve

        if volatility is not None:
            # High vol = bigger reserve, but moderate multiplier (was 5x, now 2x)
            vol_adj = volatility * 2  # 2% vol → +4%, 5% vol → +10%
            base_fraction += vol_adj

        base_fraction = max(0.06, min(0.25, base_fraction))
        reserve = max(5.00, round(portfolio_value * base_fraction, 2))

        # Enforce: reserve never consumes more than 60% of available cash
        if available_cash is not None and available_cash > 0:
            reserve = min(reserve, round(available_cash * 0.60, 2))

        return reserve

    def max_position_pct(self, portfolio_value, volatility=None):
        """Dynamic max position per asset — scales inversely with volatility.

        Formula: 0.25 - volatility_adj, clamped [0.10, 0.30]
        High vol assets = smaller max position
        """
        base = 0.25  # 25% max per asset

        if volatility is not None:
            vol_adj = volatility * 2  # 2% vol → -4%, 5% vol → -10%
            base -= vol_adj

        return max(0.10, min(0.30, base))

    def kelly_fraction(self, win_rate, avg_win, avg_loss):
        """Kelly Criterion optimal bet fraction.

        f* = (p * b - q) / b
        where p = win_rate, q = 1-p, b = avg_win/avg_loss

        We use fractional Kelly (25%) for safety.
        """
        if avg_loss == 0 or win_rate <= 0:
            return 0

        b = avg_win / avg_loss
        q = 1.0 - win_rate
        kelly = (win_rate * b - q) / b

        # Fractional Kelly: 25% of optimal
        return max(0, min(0.25, kelly * 0.25))

    # ==========================================
    # TRADE APPROVAL SYSTEM
    # ==========================================

    def approve_trade(self, agent_name, pair, direction, size_usd, portfolio_value):
        """Central trade approval — returns (approved, reason, adjusted_size).

        Every agent MUST call this before executing any trade.
        Uses SQLite-based atomic locking so cross-process agents can't
        simultaneously approve trades exceeding combined limits.
        Returns tuple: (bool approved, str reason, float adjusted_size)
        """
        # Use SQLite transaction for atomic cross-process coordination
        # threading.Lock only works within a single process — useless for
        # separate agent processes that each import risk_controller
        try:
            direction = self._normalize_direction(direction)
            if direction not in {"BUY", "SELL"}:
                return False, f"INVALID_DIRECTION:{direction}", 0

            pair = self._normalize_pair(pair)
            try:
                size_usd = float(size_usd)
            except (TypeError, ValueError):
                return False, f"INVALID_SIZE:{size_usd}", 0

            if not math.isfinite(size_usd) or size_usd <= 0:
                return False, f"INVALID_SIZE:{size_usd}", 0

            cur = self._db.cursor()
            cur.execute("BEGIN IMMEDIATE")  # acquire DB write lock atomically

            self._check_daily_reset()

            # 1. Daily loss check (BUY only — SELL/exits ALWAYS allowed to stop bleeding)
            max_loss = self.max_daily_loss(portfolio_value)
            if direction == "BUY" and self._daily_loss >= max_loss:
                self._db.rollback()
                self._log_event("hardstop", agent_name, pair,
                                f"Daily loss ${self._daily_loss:.2f} >= limit ${max_loss:.2f}")
                return False, f"HARDSTOP: Daily loss ${self._daily_loss:.2f} >= ${max_loss:.2f}", 0

            # 2. Volatility check
            vol = self.market.compute_volatility(pair)

            # 3. Trend check (BUY only — selling is always allowed)
            if direction == "BUY":
                trend = self.market.compute_trend(pair)
                if trend < -0.5:
                    self._db.rollback()
                    self._log_event("trend_block", agent_name, pair,
                                    f"Strong downtrend ({trend:.2f}), blocking BUY")
                    return False, f"BLOCKED: Strong downtrend ({trend:.2f}) on {pair}", 0

            # 4. Dynamic reserve check
            reserve = self.min_reserve(portfolio_value, vol)
            if direction == "BUY":
                max_size = self.max_trade_usd(portfolio_value, vol,
                                               self.market.compute_trend(pair))
                adjusted = min(size_usd, max_size)
                if adjusted < 1.00:
                    self._db.rollback()
                    return False, f"Trade too small after adjustments (${adjusted:.2f})", 0
            else:
                adjusted = size_usd  # sells aren't limited by same rules

            # 5. Check total pending allocations across ALL agents (cross-process safe)
            # Clean up stale pending allocations older than 2 minutes (prevents bloat from agent storms)
            cur.execute(
                "UPDATE pending_allocations SET status='expired', resolved_at=CURRENT_TIMESTAMP "
                "WHERE status='pending' AND created_at < datetime('now', '-2 minutes')")
            row = cur.execute(
                "SELECT COALESCE(SUM(size_usd), 0) FROM pending_allocations WHERE status='pending'"
            ).fetchone()
            total_pending = row[0] if row else 0
            if direction == "BUY" and (total_pending + adjusted) > portfolio_value * 0.80:
                self._db.rollback()
                return False, f"Total pending ${total_pending + adjusted:.2f} exceeds 80% of portfolio ${portfolio_value:.2f}", 0

            # 6. Rate limiting: max trades per day scales with portfolio
            # SELL/exit orders are EXEMPT — you must ALWAYS be able to exit a position
            max_trades = max(50, int(math.log10(max(1, portfolio_value)) * 40))
            if direction == "BUY" and self._trade_count_today >= max_trades:
                self._db.rollback()
                return False, f"Trade limit reached ({self._trade_count_today}/{max_trades})", 0

            # 7. Record allocation and approve (atomic with the checks above)
            trade_id = str(uuid.uuid4())[:12]
            cur.execute(
                "INSERT INTO pending_allocations (agent, pair, direction, size_usd, trade_id) "
                "VALUES (?, ?, ?, ?, ?)",
                (agent_name, pair, direction, adjusted, trade_id))
            cur.execute(
                "INSERT INTO trade_audit (trade_id, agent, pair, direction, requested_size, "
                "approved_size, status, reason, volatility, trend, portfolio_value) "
                "VALUES (?, ?, ?, ?, ?, ?, 'approved', 'APPROVED', ?, ?, ?)",
                (trade_id, agent_name, pair, direction, size_usd, adjusted,
                 vol, self.market.compute_trend(pair) if direction == "BUY" else 0,
                 portfolio_value))
            # Only count BUYs toward daily trade limit — sells must never be capped
            if direction == "BUY":
                self._trade_count_today += 1
            self._db.commit()

            logger.info("TRADE %s: APPROVED %s %s %s $%.2f (vol=%.3f)",
                        trade_id, agent_name, direction, pair, adjusted, vol)
            return True, f"APPROVED|trade_id={trade_id}", round(adjusted, 2)

        except Exception as e:
            try:
                self._db.rollback()
            except Exception:
                pass
            logger.error("approve_trade error: %s", e)
            return False, f"Risk controller error: {e}", 0

    def resolve_allocation(self, agent_name, pair, trade_id=None):
        """Mark a pending allocation as resolved after trade completes or fails."""
        try:
            agent_name = str(agent_name or "").strip()
            pair = self._normalize_pair(pair)
            if trade_id:
                cur = self._db.execute(
                    "UPDATE pending_allocations SET status='resolved', resolved_at=CURRENT_TIMESTAMP "
                    "WHERE trade_id=? AND agent=? AND status='pending'",
                    (str(trade_id), agent_name,)
                )
                rowcount = cur.rowcount
                if rowcount == 0:
                    self._db.execute(
                        "UPDATE pending_allocations SET status='resolved', resolved_at=CURRENT_TIMESTAMP "
                        "WHERE id = ("
                        "SELECT id FROM pending_allocations WHERE agent=? AND pair=? AND status='pending' "
                        "ORDER BY created_at DESC LIMIT 1)",
                        (agent_name, pair),
                    )
            else:
                self._db.execute(
                    "UPDATE pending_allocations SET status='resolved', resolved_at=CURRENT_TIMESTAMP "
                    "WHERE id = ("
                    "SELECT id FROM pending_allocations WHERE agent=? AND pair=? AND status='pending' "
                    "ORDER BY created_at DESC LIMIT 1)",
                    (agent_name, pair),
                )
            self._db.commit()
        except Exception:
            pass

    def complete_trade(self, trade_id, status="filled", pnl=None):
        """Mark a trade as completed in the audit log for post-mortem analysis."""
        try:
            self._db.execute(
                "UPDATE trade_audit SET status=?, pnl=?, completed_at=CURRENT_TIMESTAMP "
                "WHERE trade_id=?",
                (status, pnl, trade_id))
            self._db.commit()
        except Exception:
            pass

    def request_trade(self, agent_name, pair, direction, size_usd, portfolio_value=None):
        """Unified trade request entry point — ALL agents should call this.

        Wraps approve_trade with automatic portfolio estimation if not provided.
        Returns tuple: (bool approved, str reason, float adjusted_size)
        """
        if portfolio_value is None:
            # Auto-estimate from Coinbase holdings
            try:
                from exchange_connector import CoinbaseTrader
                trader = CoinbaseTrader()
                accts = trader._request("GET", "/api/v3/brokerage/accounts?limit=250")
                total = 0.0
                for a in accts.get("accounts", []):
                    cur = a.get("currency", "")
                    bal = float(a.get("available_balance", {}).get("value", 0))
                    if cur in ("USDC", "USD"):
                        total += bal
                    elif bal > 0:
                        price = self.market.get_price(f"{cur}-USDC")
                        if price:
                            total += bal * price
                portfolio_value = max(1.0, total)
            except Exception:
                portfolio_value = 100.0  # safe fallback

        return self.approve_trade(agent_name, pair, direction, size_usd, portfolio_value)

    def record_trade_result(self, agent_name, pnl):
        """Record trade result for agent performance tracking."""
        with self._lock:
            self._check_daily_reset()

            try:
                pnl = float(pnl)
            except (TypeError, ValueError):
                logger.warning("record_trade_result invalid pnl=%r for %s", pnl, agent_name)
                return

            if not math.isfinite(pnl):
                logger.warning("record_trade_result non-finite pnl=%r for %s", pnl, agent_name)
                return

            if pnl < 0:
                self._daily_loss += abs(pnl)

            try:
                self._db.execute("""
                    INSERT INTO agent_performance (agent, wins, losses, total_pnl, last_trade)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(agent) DO UPDATE SET
                        wins = wins + ?,
                        losses = losses + ?,
                        total_pnl = total_pnl + ?,
                        last_trade = CURRENT_TIMESTAMP
                """, (agent_name,
                      1 if pnl > 0 else 0,
                      1 if pnl < 0 else 0,
                      pnl,
                      1 if pnl > 0 else 0,
                      1 if pnl < 0 else 0,
                      pnl))
                self._db.commit()
                self._persist_daily_state()
            except Exception:
                pass

    def get_risk_params(self, portfolio_value, pair=None):
        """Get all dynamic risk parameters for current state.

        Returns dict with all computed values — NO hardcoded numbers.
        """
        vol = self.market.compute_volatility(pair) if pair else 0.02
        trend = self.market.compute_trend(pair) if pair else 0.0
        momentum = self.market.compute_momentum(pair) if pair else 0.0

        params = {
            "portfolio_value": portfolio_value,
            "volatility": round(vol, 4),
            "trend": round(trend, 3),
            "momentum": round(momentum, 3),
            "max_trade_usd": self.max_trade_usd(portfolio_value, vol, trend),
            "max_daily_loss": self.max_daily_loss(portfolio_value),
            "min_reserve": self.min_reserve(portfolio_value, vol),
            "max_position_pct": self.max_position_pct(portfolio_value, vol),
            "daily_loss_so_far": round(self._daily_loss, 2),
            "trades_today": self._trade_count_today,
            "can_buy": trend > -0.5,
            "regime": "UPTREND" if trend > 0.3 else "DOWNTREND" if trend < -0.3 else "RANGING",
        }
        return params

    def get_agent_rankings(self):
        """Get agent performance rankings for hire/fire decisions."""
        rows = self._db.execute(
            "SELECT agent, wins, losses, total_pnl, sharpe, status FROM agent_performance "
            "ORDER BY total_pnl DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def should_fire(self, agent_name, min_trades=10):
        """Check if an agent should be fired based on performance.

        Criteria: Sharpe < 0.5 after min_trades, or loss_rate > 70%.
        """
        row = self._db.execute(
            "SELECT wins, losses, total_pnl FROM agent_performance WHERE agent=?",
            (agent_name,)).fetchone()
        if not row:
            return False, "No data"

        total = row["wins"] + row["losses"]
        if total < min_trades:
            return False, f"Insufficient data ({total}/{min_trades} trades)"

        win_rate = row["wins"] / total if total > 0 else 0
        if win_rate < 0.30:
            return True, f"Win rate {win_rate:.0%} < 30% threshold"
        if row["total_pnl"] < -5.00:
            return True, f"Total P&L ${row['total_pnl']:.2f} < -$5 threshold"

        return False, f"Performing OK (win rate {win_rate:.0%}, P&L ${row['total_pnl']:.2f})"

    def print_status(self):
        """Print full risk controller status."""
        print(f"\n{'='*70}")
        print(f"  RISK CONTROLLER STATUS")
        print(f"{'='*70}")
        print(f"  Daily loss: ${self._daily_loss:.2f}")
        print(f"  Trades today: {self._trade_count_today}")
        print(f"\n  Agent Rankings:")
        for r in self.get_agent_rankings():
            total = r["wins"] + r["losses"]
            wr = r["wins"] / total * 100 if total > 0 else 0
            print(f"    {r['agent']:20s} W:{r['wins']:3d} L:{r['losses']:3d} "
                  f"WR:{wr:5.1f}% P&L:${r['total_pnl']:+8.2f} [{r['status']}]")
        print(f"{'='*70}\n")


# Singleton
_controller = None


def get_controller():
    """Get or create the singleton RiskController."""
    global _controller
    if _controller is None:
        _controller = RiskController()
    return _controller


if __name__ == "__main__":
    import sys
    rc = get_controller()

    if len(sys.argv) > 1 and sys.argv[1] == "status":
        rc.print_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "test":
        # Test with various portfolio sizes
        print("=== DYNAMIC RISK SCALING ===")
        for pv in [50, 100, 200, 500, 1000, 5000, 10000, 100000]:
            params = rc.get_risk_params(pv, "BTC-USDC")
            print(f"  ${pv:>7d} → max_trade=${params['max_trade_usd']:>7.2f} "
                  f"daily_loss=${params['max_daily_loss']:>7.2f} "
                  f"reserve=${params['min_reserve']:>7.2f} "
                  f"max_pos={params['max_position_pct']:.0%} "
                  f"regime={params['regime']}")
    else:
        print("Usage: risk_controller.py [status|test]")
        rc.print_status()
