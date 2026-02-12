#!/usr/bin/env python3
"""NetTrace ML Signal Agent — generates trading signals from latency scan data.

Uses the compute pool for distributed inference and supports:
  - Apple MLX  (fastest path on Apple Silicon)
  - PyTorch    (CUDA or MPS backend)
  - NumPy      (universal fallback, no GPU needed)

Signal types:
  - latency_anomaly   : Z-score spike detection on per-host RTT timeseries
  - trend_shift       : Linear regression slope change on rolling windows
  - route_change      : Hop-path divergence from baseline (not ML, but critical)
  - composite         : Weighted ensemble of the above

Designed to run on any compute pool node. Feeds signals to live_trader.py.
"""

import json
import logging
import math
import os
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Framework detection — MLX > PyTorch > NumPy
# ---------------------------------------------------------------------------

_FRAMEWORK = "numpy"
_mx = None
_torch = None
_np = None

try:
    import mlx.core as mx  # type: ignore
    _mx = mx
    _FRAMEWORK = "mlx"
except ImportError:
    pass

if _FRAMEWORK != "mlx":
    try:
        import torch
        _torch = torch
        if torch.cuda.is_available():
            _FRAMEWORK = "cuda"
        elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            _FRAMEWORK = "mps"
        else:
            _FRAMEWORK = "pytorch_cpu"
    except ImportError:
        pass

try:
    import numpy as np
    _np = np
except ImportError:
    # If even numpy is missing, we provide a minimal shim (should not happen)
    _np = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AGENTS_DIR = Path(__file__).parent
DB_PATH = os.environ.get("DB_PATH", str(AGENTS_DIR.parent / "traceroute.db"))
TRADER_DB = str(AGENTS_DIR / "trader.db")
SIGNAL_DB = str(AGENTS_DIR / "signals.db")

# Anomaly detection parameters
ZSCORE_THRESHOLD = 2.5          # Standard deviations to trigger anomaly
ZSCORE_CRITICAL = 4.0           # Extreme anomaly
LOOKBACK_WINDOW = 50            # Number of recent samples for stats
MIN_SAMPLES = 10                # Minimum samples before generating signals
TREND_WINDOW = 20               # Samples for linear regression
SLOPE_CHANGE_THRESHOLD = 0.3    # Relative slope change to trigger trend signal

# Signal confidence thresholds
MIN_CONFIDENCE = 0.40           # Below this, discard
HIGH_CONFIDENCE = 0.75          # Above this, flag as strong

# Compute pool integration
POOL_URL = os.environ.get("POOL_URL", "http://127.0.0.1:9090")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ml_signal] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(AGENTS_DIR / "ml_signal.log")),
    ],
)
logger = logging.getLogger("ml_signal")


# ---------------------------------------------------------------------------
# Math utilities — framework-agnostic
# ---------------------------------------------------------------------------

def _to_array(data: List[float]):
    """Convert list to the best available array type."""
    if _mx is not None:
        return _mx.array(data)
    if _np is not None:
        return _np.array(data, dtype=_np.float64)
    return data


def _mean(arr) -> float:
    """Compute mean, framework-aware."""
    if _mx is not None and hasattr(arr, "shape"):
        return float(_mx.mean(arr))
    if _np is not None and hasattr(arr, "shape"):
        return float(_np.mean(arr))
    if isinstance(arr, (list, tuple)) and len(arr) > 0:
        return sum(arr) / len(arr)
    return 0.0


def _std(arr) -> float:
    """Compute standard deviation, framework-aware."""
    if _mx is not None and hasattr(arr, "shape"):
        # MLX: compute std manually (ddof=0)
        m = _mx.mean(arr)
        return float(_mx.sqrt(_mx.mean((arr - m) ** 2)))
    if _np is not None and hasattr(arr, "shape"):
        return float(_np.std(arr, ddof=0))
    # Pure Python fallback
    if isinstance(arr, (list, tuple)) and len(arr) > 1:
        m = sum(arr) / len(arr)
        variance = sum((x - m) ** 2 for x in arr) / len(arr)
        return math.sqrt(variance)
    return 0.0


def _linear_regression(x_data, y_data) -> Tuple[float, float, float]:
    """Simple linear regression. Returns (slope, intercept, r_squared).

    Uses MLX or NumPy if available, pure Python otherwise.
    """
    n = len(x_data)
    if n < 2:
        return 0.0, 0.0, 0.0

    if _np is not None:
        x = _np.array(x_data, dtype=_np.float64)
        y = _np.array(y_data, dtype=_np.float64)
        x_mean = _np.mean(x)
        y_mean = _np.mean(y)
        ss_xx = _np.sum((x - x_mean) ** 2)
        ss_xy = _np.sum((x - x_mean) * (y - y_mean))
        ss_yy = _np.sum((y - y_mean) ** 2)

        if ss_xx == 0:
            return 0.0, float(y_mean), 0.0

        slope = float(ss_xy / ss_xx)
        intercept = float(y_mean - slope * x_mean)
        r_squared = float((ss_xy ** 2) / (ss_xx * ss_yy)) if ss_yy != 0 else 0.0
        return slope, intercept, r_squared

    if _mx is not None:
        x = _mx.array(x_data)
        y = _mx.array(y_data)
        x_mean = _mx.mean(x)
        y_mean = _mx.mean(y)
        ss_xx = _mx.sum((x - x_mean) ** 2)
        ss_xy = _mx.sum((x - x_mean) * (y - y_mean))
        ss_yy = _mx.sum((y - y_mean) ** 2)

        ss_xx_f = float(ss_xx)
        if ss_xx_f == 0:
            return 0.0, float(y_mean), 0.0

        slope = float(ss_xy) / ss_xx_f
        intercept = float(y_mean) - slope * float(x_mean)
        ss_yy_f = float(ss_yy)
        r_squared = (float(ss_xy) ** 2) / (ss_xx_f * ss_yy_f) if ss_yy_f != 0 else 0.0
        return slope, intercept, r_squared

    # Pure Python fallback
    x_mean = sum(x_data) / n
    y_mean = sum(y_data) / n
    ss_xx = sum((xi - x_mean) ** 2 for xi in x_data)
    ss_xy = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x_data, y_data))
    ss_yy = sum((yi - y_mean) ** 2 for yi in y_data)

    if ss_xx == 0:
        return 0.0, y_mean, 0.0

    slope = ss_xy / ss_xx
    intercept = y_mean - slope * x_mean
    r_squared = (ss_xy ** 2) / (ss_xx * ss_yy) if ss_yy != 0 else 0.0
    return slope, intercept, r_squared


# ---------------------------------------------------------------------------
# Signal database
# ---------------------------------------------------------------------------

class SignalDB:
    """Persistent storage for generated trading signals."""

    def __init__(self, db_path: str = SIGNAL_DB):
        self.db = sqlite3.connect(db_path, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS signals (
                signal_id TEXT PRIMARY KEY,
                signal_type TEXT NOT NULL,
                host TEXT NOT NULL,
                direction TEXT NOT NULL,
                confidence REAL NOT NULL,
                strength REAL DEFAULT 0.0,
                details TEXT DEFAULT '{}',
                framework_used TEXT DEFAULT 'numpy',
                node_id TEXT DEFAULT 'local',
                acted_on INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS signal_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT NOT NULL,
                pair TEXT,
                entry_price REAL,
                exit_price REAL,
                pnl REAL,
                correct INTEGER,
                evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (signal_id) REFERENCES signals(signal_id)
            );

            CREATE INDEX IF NOT EXISTS idx_signals_host ON signals(host);
            CREATE INDEX IF NOT EXISTS idx_signals_type ON signals(signal_type);
            CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at);
        """)
        self.db.commit()

    def store_signal(self, signal: Dict) -> str:
        signal_id = signal.get("signal_id") or str(uuid.uuid4())[:12]
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()

        self.db.execute("""
            INSERT OR REPLACE INTO signals
            (signal_id, signal_type, host, direction, confidence, strength,
             details, framework_used, node_id, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            signal_id, signal["signal_type"], signal["host"],
            signal["direction"], signal["confidence"], signal.get("strength", 0.0),
            json.dumps(signal.get("details", {})), signal.get("framework", _FRAMEWORK),
            signal.get("node_id", "local"), expires_at,
        ))
        self.db.commit()
        return signal_id

    def get_active_signals(self, min_confidence: float = MIN_CONFIDENCE,
                           limit: int = 20) -> List[Dict]:
        now = datetime.now(timezone.utc).isoformat()
        rows = self.db.execute("""
            SELECT * FROM signals
            WHERE confidence >= ? AND expires_at > ? AND acted_on = 0
            ORDER BY confidence DESC, created_at DESC
            LIMIT ?
        """, (min_confidence, now, limit)).fetchall()
        return [dict(r) for r in rows]

    def mark_acted(self, signal_id: str):
        self.db.execute("UPDATE signals SET acted_on = 1 WHERE signal_id = ?", (signal_id,))
        self.db.commit()

    def record_performance(self, signal_id: str, pair: str, entry_price: float,
                           exit_price: float, pnl: float, correct: bool):
        self.db.execute("""
            INSERT INTO signal_performance (signal_id, pair, entry_price, exit_price, pnl, correct)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (signal_id, pair, entry_price, exit_price, pnl, int(correct)))
        self.db.commit()

    def get_signal_accuracy(self, lookback_hours: int = 24) -> Dict:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
        row = self.db.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN correct = 1 THEN 1 ELSE 0 END) as correct_count,
                   AVG(pnl) as avg_pnl,
                   SUM(pnl) as total_pnl
            FROM signal_performance
            WHERE evaluated_at > ?
        """, (cutoff,)).fetchone()
        total = row["total"] or 0
        correct_count = row["correct_count"] or 0
        return {
            "total_signals": total,
            "correct": correct_count,
            "accuracy": correct_count / total if total > 0 else 0.0,
            "avg_pnl": row["avg_pnl"] or 0.0,
            "total_pnl": row["total_pnl"] or 0.0,
            "lookback_hours": lookback_hours,
        }


# ---------------------------------------------------------------------------
# Anomaly detectors
# ---------------------------------------------------------------------------

class LatencyAnomalyDetector:
    """Z-score based anomaly detection on latency timeseries.

    For each host, maintains a rolling window of RTT measurements.
    Flags when current measurement deviates significantly from the norm.
    """

    def __init__(self):
        # In-memory cache of recent measurements per host
        self._history: Dict[str, List[float]] = {}

    def add_sample(self, host: str, rtt_ms: float):
        if host not in self._history:
            self._history[host] = []
        self._history[host].append(rtt_ms)
        # Keep only recent samples
        if len(self._history[host]) > LOOKBACK_WINDOW * 2:
            self._history[host] = self._history[host][-LOOKBACK_WINDOW:]

    def detect(self, host: str, rtt_ms: float) -> Optional[Dict]:
        """Check if this RTT measurement is anomalous.

        Returns signal dict or None.
        """
        self.add_sample(host, rtt_ms)
        history = self._history.get(host, [])

        if len(history) < MIN_SAMPLES:
            return None

        # Use all but the latest sample for baseline
        baseline = history[:-1]
        arr = _to_array(baseline)
        mean = _mean(arr)
        std = _std(arr)

        if std < 0.001:
            # Constant latency — no anomaly possible unless value changed
            if abs(rtt_ms - mean) > 1.0:
                return {
                    "signal_type": "latency_anomaly",
                    "host": host,
                    "direction": "long" if rtt_ms > mean else "short",
                    "confidence": 0.60,
                    "strength": abs(rtt_ms - mean),
                    "details": {
                        "rtt_ms": rtt_ms,
                        "baseline_mean": round(mean, 3),
                        "baseline_std": round(std, 3),
                        "z_score": 0,
                        "note": "constant baseline broken",
                    },
                }
            return None

        z_score = (rtt_ms - mean) / std

        if abs(z_score) < ZSCORE_THRESHOLD:
            return None

        # Compute confidence based on z-score magnitude
        confidence = min(0.95, 0.50 + (abs(z_score) - ZSCORE_THRESHOLD) * 0.10)

        # Direction logic:
        # Latency spike on a crypto exchange -> exchange is degraded -> price may
        # diverge from other exchanges -> short-term arbitrage opportunity.
        # Spike = potential sell pressure (degraded service), Drop = recovery signal.
        if z_score > 0:
            direction = "short"  # Latency spike -> bearish for that exchange
        else:
            direction = "long"   # Latency drop (recovery) -> bullish

        if abs(z_score) >= ZSCORE_CRITICAL:
            confidence = min(0.95, confidence + 0.10)

        return {
            "signal_type": "latency_anomaly",
            "host": host,
            "direction": direction,
            "confidence": round(confidence, 4),
            "strength": round(abs(z_score), 4),
            "details": {
                "rtt_ms": round(rtt_ms, 3),
                "baseline_mean": round(mean, 3),
                "baseline_std": round(std, 3),
                "z_score": round(z_score, 4),
                "samples": len(baseline),
                "threshold": ZSCORE_THRESHOLD,
            },
        }


class TrendAnalyzer:
    """Detects trend shifts in latency using rolling linear regression.

    When the slope of latency changes significantly (e.g., from flat to rising),
    it signals potential network degradation or recovery.
    """

    def __init__(self):
        self._history: Dict[str, List[Tuple[float, float]]] = {}

    def add_sample(self, host: str, timestamp: float, rtt_ms: float):
        if host not in self._history:
            self._history[host] = []
        self._history[host].append((timestamp, rtt_ms))
        if len(self._history[host]) > LOOKBACK_WINDOW * 2:
            self._history[host] = self._history[host][-LOOKBACK_WINDOW:]

    def detect(self, host: str, timestamp: float, rtt_ms: float) -> Optional[Dict]:
        """Check for trend shift in latency. Returns signal dict or None."""
        self.add_sample(host, timestamp, rtt_ms)
        history = self._history.get(host, [])

        if len(history) < TREND_WINDOW + MIN_SAMPLES:
            return None

        # Compare slope of recent window vs. previous window
        recent = history[-TREND_WINDOW:]
        previous = history[-(TREND_WINDOW * 2):-TREND_WINDOW]

        if len(previous) < MIN_SAMPLES:
            return None

        r_times = [t for t, _ in recent]
        r_rtts = [r for _, r in recent]
        p_times = [t for t, _ in previous]
        p_rtts = [r for _, r in previous]

        slope_recent, _, r2_recent = _linear_regression(r_times, r_rtts)
        slope_prev, _, r2_prev = _linear_regression(p_times, p_rtts)

        # Normalize slopes to be comparable (per-second change in ms)
        if abs(slope_prev) < 0.0001:
            if abs(slope_recent) < 0.0001:
                return None
            relative_change = abs(slope_recent) * 100  # Arbitrary large multiplier
        else:
            relative_change = abs(slope_recent - slope_prev) / abs(slope_prev)

        if relative_change < SLOPE_CHANGE_THRESHOLD:
            return None

        # Confidence based on R-squared (fit quality) and change magnitude
        confidence = min(0.90, 0.40 + r2_recent * 0.25 + min(relative_change, 3.0) * 0.10)

        # Direction: slope going up = degradation = bearish, slope going down = recovery = bullish
        if slope_recent > slope_prev:
            direction = "short"
        else:
            direction = "long"

        return {
            "signal_type": "trend_shift",
            "host": host,
            "direction": direction,
            "confidence": round(confidence, 4),
            "strength": round(relative_change, 4),
            "details": {
                "slope_recent": round(slope_recent, 6),
                "slope_previous": round(slope_prev, 6),
                "r2_recent": round(r2_recent, 4),
                "r2_previous": round(r2_prev, 4),
                "relative_change": round(relative_change, 4),
                "window_size": TREND_WINDOW,
            },
        }


class RouteChangeDetector:
    """Detects changes in network routing paths.

    Not strictly ML, but a critical signal — route changes precede latency shifts.
    When hops change for a host, something is happening at the network layer.
    """

    def __init__(self):
        self._baselines: Dict[str, List[Optional[str]]] = {}  # host -> list of hop IPs

    def set_baseline(self, host: str, hop_ips: List[Optional[str]]):
        self._baselines[host] = hop_ips

    def detect(self, host: str, current_hops: List[Optional[str]]) -> Optional[Dict]:
        """Compare current route to baseline. Returns signal dict or None."""
        if host not in self._baselines:
            self._baselines[host] = current_hops
            return None

        baseline = self._baselines[host]

        # Count changed hops (ignoring None / timeout hops)
        max_len = max(len(baseline), len(current_hops))
        changed = 0
        total_comparable = 0

        for i in range(max_len):
            b_ip = baseline[i] if i < len(baseline) else None
            c_ip = current_hops[i] if i < len(current_hops) else None

            if b_ip is None and c_ip is None:
                continue

            total_comparable += 1
            if b_ip != c_ip:
                changed += 1

        if total_comparable == 0 or changed == 0:
            return None

        change_ratio = changed / total_comparable
        if change_ratio < 0.15:
            # Minor variance, not significant
            return None

        confidence = min(0.85, 0.45 + change_ratio * 0.40)
        # Hop count change
        hop_delta = len(current_hops) - len(baseline)

        # More hops usually means longer path = degradation
        direction = "short" if hop_delta > 0 else "long" if hop_delta < 0 else "short"

        # Update baseline
        self._baselines[host] = current_hops

        return {
            "signal_type": "route_change",
            "host": host,
            "direction": direction,
            "confidence": round(confidence, 4),
            "strength": round(change_ratio, 4),
            "details": {
                "hops_changed": changed,
                "total_hops": total_comparable,
                "change_ratio": round(change_ratio, 4),
                "hop_count_delta": hop_delta,
                "baseline_length": len(baseline),
                "current_length": len(current_hops),
            },
        }


# ---------------------------------------------------------------------------
# Composite signal generator
# ---------------------------------------------------------------------------

class CompositeSignalGenerator:
    """Combines multiple signal sources into a weighted composite signal.

    Weights are tuned based on historical signal accuracy.
    """

    DEFAULT_WEIGHTS = {
        "latency_anomaly": 0.45,
        "trend_shift": 0.30,
        "route_change": 0.25,
    }

    def __init__(self, weights: Dict[str, float] = None):
        self.weights = weights or self.DEFAULT_WEIGHTS.copy()

    def combine(self, signals: List[Dict]) -> Optional[Dict]:
        """Combine multiple signals for the same host into one composite signal.

        Only combines if at least 2 signal types agree on direction.
        """
        if len(signals) < 2:
            return None

        # Group by direction
        long_signals = [s for s in signals if s["direction"] == "long"]
        short_signals = [s for s in signals if s["direction"] == "short"]

        if len(long_signals) >= 2:
            chosen = long_signals
            direction = "long"
        elif len(short_signals) >= 2:
            chosen = short_signals
            direction = "short"
        else:
            return None  # No consensus

        # Weighted confidence
        total_weight = 0.0
        weighted_confidence = 0.0
        max_strength = 0.0

        for sig in chosen:
            w = self.weights.get(sig["signal_type"], 0.2)
            weighted_confidence += sig["confidence"] * w
            total_weight += w
            max_strength = max(max_strength, sig.get("strength", 0))

        if total_weight == 0:
            return None

        composite_confidence = min(0.95, weighted_confidence / total_weight)

        # Boost confidence when all signals agree
        if len(chosen) == len(signals):
            composite_confidence = min(0.95, composite_confidence * 1.10)

        return {
            "signal_type": "composite",
            "host": signals[0]["host"],
            "direction": direction,
            "confidence": round(composite_confidence, 4),
            "strength": round(max_strength, 4),
            "details": {
                "component_signals": len(chosen),
                "total_signals": len(signals),
                "signal_types": [s["signal_type"] for s in chosen],
                "weights_used": {s["signal_type"]: self.weights.get(s["signal_type"], 0.2) for s in chosen},
            },
        }


# ---------------------------------------------------------------------------
# ML Signal Agent — main class
# ---------------------------------------------------------------------------

# Mapping from exchange hosts to trading pairs
EXCHANGE_HOST_MAP = {
    "api.coinbase.com": ["BTC-USD", "ETH-USD", "SOL-USD"],
    "coinbase.com": ["BTC-USD", "ETH-USD", "SOL-USD"],
    "api.binance.com": ["BTC-USDT", "ETH-USDT"],
    "api.kraken.com": ["BTC-USD", "ETH-USD"],
    "ftx.com": ["BTC-USD", "ETH-USD"],
    "api.bybit.com": ["BTC-USDT", "ETH-USDT"],
    "api.exchange.coinbase.com": ["BTC-USD", "ETH-USD", "SOL-USD"],
}


class MLSignalAgent:
    """Generates and manages trading signals from network scan data.

    Can run standalone or be dispatched via the compute pool.
    """

    def __init__(self, db_path: str = DB_PATH, signal_db_path: str = SIGNAL_DB):
        self.db_path = db_path
        self.signal_db = SignalDB(signal_db_path)
        self.anomaly_detector = LatencyAnomalyDetector()
        self.trend_analyzer = TrendAnalyzer()
        self.route_detector = RouteChangeDetector()
        self.composite_gen = CompositeSignalGenerator()
        self.framework = _FRAMEWORK

        logger.info("ML Signal Agent initialized | framework=%s", self.framework)

    def _get_scan_db(self) -> Optional[sqlite3.Connection]:
        """Connect to the traceroute scan database (read-only)."""
        if not os.path.exists(self.db_path):
            logger.warning("Scan database not found: %s", self.db_path)
            return None
        try:
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=5)
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.Error as e:
            logger.error("Failed to open scan DB: %s", e)
            return None

    def load_recent_scans(self, hours: int = 4, limit: int = 5000) -> List[Dict]:
        """Load recent scan results from the traceroute database."""
        conn = self._get_scan_db()
        if conn is None:
            return []

        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

            # Try different table schemas (the DB might vary)
            for table in ("scan_results", "scans", "traceroutes"):
                try:
                    rows = conn.execute(f"""
                        SELECT * FROM {table}
                        WHERE created_at > ? OR timestamp > ?
                        ORDER BY COALESCE(created_at, timestamp) DESC
                        LIMIT ?
                    """, (cutoff, cutoff, limit)).fetchall()
                    return [dict(r) for r in rows]
                except sqlite3.OperationalError:
                    continue

            # Fallback: try to find any table with RTT data
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            for t in tables:
                try:
                    rows = conn.execute(
                        f"SELECT * FROM {t['name']} ORDER BY rowid DESC LIMIT ?",
                        (limit,),
                    ).fetchall()
                    if rows and any(
                        k in dict(rows[0]).keys()
                        for k in ("rtt_ms", "latency", "avg_rtt", "total_rtt")
                    ):
                        return [dict(r) for r in rows]
                except sqlite3.OperationalError:
                    continue

            return []
        finally:
            conn.close()

    def process_scan(self, host: str, rtt_ms: float, timestamp: float = None,
                     hops: List[Optional[str]] = None) -> List[Dict]:
        """Process a single scan result and generate any triggered signals.

        Returns list of signal dicts (may be empty).
        """
        timestamp = timestamp or time.time()
        signals = []

        # 1. Z-score anomaly detection
        anomaly_sig = self.anomaly_detector.detect(host, rtt_ms)
        if anomaly_sig:
            anomaly_sig["framework"] = self.framework
            signals.append(anomaly_sig)

        # 2. Trend analysis
        trend_sig = self.trend_analyzer.detect(host, timestamp, rtt_ms)
        if trend_sig:
            trend_sig["framework"] = self.framework
            signals.append(trend_sig)

        # 3. Route change detection
        if hops:
            route_sig = self.route_detector.detect(host, hops)
            if route_sig:
                route_sig["framework"] = self.framework
                signals.append(route_sig)

        # 4. Composite signal (if multiple triggers)
        if len(signals) >= 2:
            composite = self.composite_gen.combine(signals)
            if composite:
                composite["framework"] = self.framework
                signals.append(composite)

        # Store valid signals
        stored = []
        for sig in signals:
            if sig["confidence"] >= MIN_CONFIDENCE:
                sig_id = self.signal_db.store_signal(sig)
                sig["signal_id"] = sig_id
                stored.append(sig)
                logger.info("Signal: %s on %s dir=%s conf=%.2f strength=%.2f",
                            sig["signal_type"], sig["host"], sig["direction"],
                            sig["confidence"], sig.get("strength", 0))

        return stored

    def process_batch(self, scans: List[Dict]) -> List[Dict]:
        """Process a batch of scan results. Returns all generated signals."""
        all_signals = []
        t0 = time.time()

        for scan in scans:
            host = scan.get("host") or scan.get("target") or scan.get("hostname", "")
            rtt = scan.get("rtt_ms") or scan.get("latency") or scan.get("avg_rtt") or scan.get("total_rtt")

            if not host or rtt is None:
                continue

            try:
                rtt = float(rtt)
            except (ValueError, TypeError):
                continue

            ts = None
            for ts_field in ("timestamp", "created_at", "scanned_at"):
                if scan.get(ts_field):
                    try:
                        if isinstance(scan[ts_field], (int, float)):
                            ts = float(scan[ts_field])
                        else:
                            dt = datetime.fromisoformat(str(scan[ts_field]).replace("Z", "+00:00"))
                            ts = dt.timestamp()
                    except (ValueError, TypeError):
                        pass
                    break
            if ts is None:
                ts = time.time()

            # Extract hops if available
            hops = None
            if scan.get("hops"):
                try:
                    hop_data = scan["hops"]
                    if isinstance(hop_data, str):
                        hop_data = json.loads(hop_data)
                    hops = [h.get("ip") for h in hop_data if isinstance(h, dict)]
                except (json.JSONDecodeError, TypeError):
                    pass

            signals = self.process_scan(host, rtt, ts, hops)
            all_signals.extend(signals)

        elapsed = (time.time() - t0) * 1000
        logger.info("Batch processed: %d scans -> %d signals in %.1fms (framework=%s)",
                     len(scans), len(all_signals), elapsed, self.framework)
        return all_signals

    def get_trading_signals(self, min_confidence: float = 0.55) -> List[Dict]:
        """Get actionable trading signals for the live trader.

        Filters to only exchange-related hosts and maps to trading pairs.
        """
        active = self.signal_db.get_active_signals(min_confidence=min_confidence)
        trading_signals = []

        for sig in active:
            host = sig["host"]
            pairs = None

            # Check if this host is a known exchange
            for exchange_host, exchange_pairs in EXCHANGE_HOST_MAP.items():
                if exchange_host in host:
                    pairs = exchange_pairs
                    break

            if not pairs:
                continue

            # Parse details
            details = sig.get("details", "{}")
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except json.JSONDecodeError:
                    details = {}

            for pair in pairs:
                trading_signals.append({
                    "signal_id": sig["signal_id"],
                    "signal_type": sig["signal_type"],
                    "pair": pair,
                    "direction": sig["direction"],
                    "confidence": sig["confidence"],
                    "strength": sig.get("strength", 0),
                    "host": host,
                    "details": details,
                    "framework": sig.get("framework_used", "numpy"),
                    "created_at": sig["created_at"],
                    "expires_at": sig.get("expires_at"),
                })

        # Sort by confidence descending
        trading_signals.sort(key=lambda s: s["confidence"], reverse=True)
        return trading_signals

    def get_accuracy(self, hours: int = 24) -> Dict:
        """Get signal accuracy metrics."""
        return self.signal_db.get_signal_accuracy(lookback_hours=hours)

    # -- Compute pool task handlers --

    def handle_task(self, task_type: str, data: Any) -> Dict:
        """Handle a task dispatched from the compute pool.

        Supported task types:
          - anomaly_detection: {"host": "...", "rtt_ms": ..., "timestamp": ...}
          - batch_analysis:    {"scans": [...]}
          - get_signals:       {"min_confidence": 0.55}
          - get_accuracy:      {"hours": 24}
        """
        t0 = time.time()

        if task_type == "anomaly_detection":
            host = data.get("host", "")
            rtt_ms = float(data.get("rtt_ms", 0))
            timestamp = data.get("timestamp", time.time())
            hops = data.get("hops")
            signals = self.process_scan(host, rtt_ms, timestamp, hops)
            result = {"signals": signals, "count": len(signals)}

        elif task_type == "batch_analysis":
            scans = data.get("scans", [])
            signals = self.process_batch(scans)
            result = {"signals": signals, "count": len(signals)}

        elif task_type == "get_signals":
            min_conf = float(data.get("min_confidence", 0.55))
            signals = self.get_trading_signals(min_confidence=min_conf)
            result = {"signals": signals, "count": len(signals)}

        elif task_type == "get_accuracy":
            hours = int(data.get("hours", 24))
            result = self.get_accuracy(hours)

        else:
            result = {"error": f"unknown task type: {task_type}"}

        elapsed_ms = (time.time() - t0) * 1000
        result["execution_ms"] = round(elapsed_ms, 2)
        result["framework"] = self.framework
        return result


# ---------------------------------------------------------------------------
# Standalone runner — scan loop
# ---------------------------------------------------------------------------

def run_scan_loop(agent: MLSignalAgent, interval: int = 120):
    """Continuously process new scans and generate signals."""
    logger.info("Starting scan loop (interval=%ds)", interval)

    last_processed_at = None

    while True:
        try:
            scans = agent.load_recent_scans(hours=1)
            if scans:
                # Filter to only new scans since last run
                if last_processed_at:
                    new_scans = []
                    for s in scans:
                        created = s.get("created_at") or s.get("timestamp") or ""
                        if str(created) > str(last_processed_at):
                            new_scans.append(s)
                    scans = new_scans

                if scans:
                    signals = agent.process_batch(scans)
                    last_processed_at = datetime.now(timezone.utc).isoformat()

                    # Report trading signals
                    trading = agent.get_trading_signals()
                    if trading:
                        logger.info("Active trading signals: %d", len(trading))
                        for ts in trading[:3]:
                            logger.info("  -> %s %s %s conf=%.2f",
                                        ts["direction"].upper(), ts["pair"],
                                        ts["signal_type"], ts["confidence"])
            else:
                logger.debug("No recent scans found")

        except Exception as e:
            logger.error("Scan loop error: %s", e, exc_info=True)

        time.sleep(interval)


def run_as_pool_worker(agent: MLSignalAgent, pool_url: str = POOL_URL):
    """Run as a compute pool worker, pulling and processing tasks."""
    # Import here to avoid circular dependency
    from compute_pool import ComputePoolClient

    client = ComputePoolClient(pool_url)
    logger.info("Starting as compute pool worker, pool=%s", pool_url)

    # Register self
    from compute_pool import detect_apple_gpu, detect_frameworks
    gpu_info = detect_apple_gpu()
    frameworks = detect_frameworks()

    import platform
    reg = client.register({
        "name": f"ml-signal-{platform.node()}",
        "host": "127.0.0.1",
        "port": 0,
        "gpu_type": gpu_info.get("gpu_type", "cpu"),
        "gpu_name": gpu_info.get("gpu_name", ""),
        "gpu_cores": gpu_info.get("gpu_cores", 0),
        "memory_gb": gpu_info.get("memory_gb", 0),
        "chip": gpu_info.get("chip", ""),
        "frameworks": frameworks,
        "metal_support": gpu_info.get("metal_support", False),
        "mlx_available": gpu_info.get("mlx_available", False),
        "cuda_available": "cuda" in frameworks,
    })

    node_id = reg.get("node_id")
    if not node_id:
        logger.error("Failed to register with pool: %s", reg)
        return

    logger.info("Registered with pool as node %s", node_id)

    while True:
        try:
            # Send heartbeat
            client.heartbeat(node_id, load=0.1)

            # Check for assigned tasks via the tasks endpoint
            # In a full implementation, the pool would push tasks or we'd poll
            time.sleep(5)

        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error("Worker loop error: %s", e)
            time.sleep(10)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="NetTrace ML Signal Agent")
    parser.add_argument("--mode", choices=["scan", "worker", "oneshot"],
                        default="scan", help="Run mode")
    parser.add_argument("--interval", type=int, default=120,
                        help="Scan interval in seconds")
    parser.add_argument("--pool-url", default=POOL_URL,
                        help="Compute pool URL")
    parser.add_argument("--db", default=DB_PATH,
                        help="Path to traceroute scan database")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("NetTrace ML Signal Agent")
    logger.info("Framework: %s", _FRAMEWORK)
    logger.info("Mode: %s", args.mode)
    logger.info("=" * 60)

    agent = MLSignalAgent(db_path=args.db)

    if args.mode == "scan":
        run_scan_loop(agent, interval=args.interval)
    elif args.mode == "worker":
        run_as_pool_worker(agent, pool_url=args.pool_url)
    elif args.mode == "oneshot":
        scans = agent.load_recent_scans(hours=4)
        logger.info("Loaded %d recent scans", len(scans))
        if scans:
            signals = agent.process_batch(scans)
            logger.info("Generated %d signals", len(signals))
            for sig in signals:
                print(json.dumps(sig, indent=2, default=str))

        trading = agent.get_trading_signals()
        if trading:
            print("\n--- Active Trading Signals ---")
            for ts in trading:
                print(f"  {ts['direction'].upper():5s} {ts['pair']:10s} "
                      f"conf={ts['confidence']:.2f} type={ts['signal_type']} "
                      f"host={ts['host']}")

        accuracy = agent.get_accuracy()
        print(f"\nAccuracy (24h): {accuracy}")


if __name__ == "__main__":
    main()
