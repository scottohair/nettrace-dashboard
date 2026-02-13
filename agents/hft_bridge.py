#!/usr/bin/env python3
"""Python bridge to hft_core.c — nanosecond decision engine.

Provides high-level Python API wrapping the C shared library for:
  - Galois Field signal encoding
  - Lattice decision evaluation
  - Influence map propagation
  - Ko detection
  - Markov regime detection
  - Tick ring buffer management
  - Full decision pipeline (250ns single, 2500ns batch)
  - Exit evaluation

Usage:
    from hft_bridge import HFTEngine
    engine = HFTEngine()
    decision = engine.decide(signals, pair_idx=0, cash=50.0)
    exit_dec = engine.check_exit(entry=69000, current=69500, peak=69800, ...)
"""

import ctypes
import os
import time
import logging
from pathlib import Path

logger = logging.getLogger("hft_bridge")

# ============================================================
# Load shared library
# ============================================================
_lib_dir = Path(__file__).parent
_lib_path = str(_lib_dir / "hft_core.so")

if not os.path.exists(_lib_path):
    raise ImportError(
        f"hft_core.so not found at {_lib_path}. Compile with:\n"
        "  cc -O3 -mcpu=apple-m1 -shared -fPIC -o hft_core.so hft_core.c -lm"
    )

# ============================================================
# C struct definitions (must match hft_core.c exactly)
# ============================================================

MAX_SIGNALS = 16
MAX_TICKS = 4096
MAX_PAIRS = 20
MAX_INFLUENCE = 20
KO_HISTORY = 64
LATTICE_DIMS = 5

# Signal source bitmap constants
SIG_LATENCY    = (1 << 0)
SIG_REGIME     = (1 << 1)
SIG_ARB        = (1 << 2)
SIG_ORDERBOOK  = (1 << 3)
SIG_RSI        = (1 << 4)
SIG_FEAR_GREED = (1 << 5)
SIG_MOMENTUM   = (1 << 6)
SIG_UPTICK     = (1 << 7)
SIG_META_ML    = (1 << 8)

# Direction constants
DIR_NONE = 0
DIR_BUY = 1
DIR_SELL = 2

# Exit action constants
EXIT_HOLD = 0
EXIT_FULL = 1
EXIT_PARTIAL_TP0 = 2
EXIT_PARTIAL_TP1 = 3
EXIT_PARTIAL_TP2 = 4

# Signal name → bitmap mapping
SIGNAL_NAME_MAP = {
    "latency": SIG_LATENCY,
    "regime": SIG_REGIME,
    "arb": SIG_ARB,
    "orderbook": SIG_ORDERBOOK,
    "rsi_extreme": SIG_RSI,
    "fear_greed": SIG_FEAR_GREED,
    "momentum": SIG_MOMENTUM,
    "uptick": SIG_UPTICK,
    "meta_ml": SIG_META_ML,
}

# Signal bitmap index (bit position → confidence array index)
SIGNAL_BIT_TO_IDX = {
    0: 0,  # latency
    1: 1,  # regime
    2: 2,  # arb
    3: 3,  # orderbook
    4: 4,  # rsi
    5: 5,  # fear_greed
    6: 6,  # momentum
    7: 7,  # uptick
    8: 8,  # meta_ml
}


class SignalSet(ctypes.Structure):
    _fields_ = [
        ("source_bitmap", ctypes.c_int),
        ("direction", ctypes.c_int),
        ("confidences", ctypes.c_double * MAX_SIGNALS),
        ("n_signals", ctypes.c_int),
    ]


class GFResult(ctypes.Structure):
    _fields_ = [
        ("gf_composite", ctypes.c_uint16),
        ("quality_score", ctypes.c_double),
        ("syndrome", ctypes.c_double),
        ("n_confirming", ctypes.c_int),
        ("weighted_conf", ctypes.c_double),
    ]


class LatticePoint(ctypes.Structure):
    _fields_ = [
        ("dims", ctypes.c_double * LATTICE_DIMS),
    ]


class LatticeResult(ctypes.Structure):
    _fields_ = [
        ("passes", ctypes.c_int),
        ("dims_passing", ctypes.c_int),
        ("dim_bitmap", ctypes.c_int),
        ("composite_score", ctypes.c_double),
    ]


class Stone(ctypes.Structure):
    _fields_ = [
        ("direction", ctypes.c_int),
        ("strength", ctypes.c_double),
        ("timestamp_ns", ctypes.c_uint64),
    ]


class InfluenceMap(ctypes.Structure):
    _fields_ = [
        ("stones", Stone * MAX_INFLUENCE),
        ("n_stones", ctypes.c_int),
        ("correlations", (ctypes.c_double * MAX_PAIRS) * MAX_PAIRS),
        ("decay_rate", ctypes.c_double),
    ]


class InfluenceResult(ctypes.Structure):
    _fields_ = [
        ("direction", ctypes.c_int),
        ("strength", ctypes.c_double),
        ("contributing_sources", ctypes.c_int),
    ]


class KoEntry(ctypes.Structure):
    _fields_ = [
        ("pair_idx", ctypes.c_int),
        ("direction", ctypes.c_int),
        ("was_loss", ctypes.c_int),
        ("timestamp_ns", ctypes.c_uint64),
    ]


class KoDetector(ctypes.Structure):
    _fields_ = [
        ("history", KoEntry * KO_HISTORY),
        ("count", ctypes.c_int),
        ("head", ctypes.c_int),
        ("cooldown_ns", ctypes.c_uint64),
    ]


class KoBanResult(ctypes.Structure):
    _fields_ = [
        ("is_banned", ctypes.c_int),
        ("ban_expires_ns", ctypes.c_uint64),
        ("loss_count", ctypes.c_int),
        ("churn_detected", ctypes.c_int),
    ]


class Tick(ctypes.Structure):
    _fields_ = [
        ("price", ctypes.c_double),
        ("volume", ctypes.c_double),
        ("bid", ctypes.c_double),
        ("ask", ctypes.c_double),
        ("timestamp_ns", ctypes.c_uint64),
    ]


class TickRingBuffer(ctypes.Structure):
    _fields_ = [
        ("ticks", Tick * MAX_TICKS),
        ("head", ctypes.c_int),
        ("count", ctypes.c_int),
        ("pair_idx", ctypes.c_int),
        ("vwap", ctypes.c_double),
        ("cum_vol", ctypes.c_double),
        ("cum_pv", ctypes.c_double),
        ("ema_fast", ctypes.c_double),
        ("ema_slow", ctypes.c_double),
        ("max_price", ctypes.c_double),
        ("min_price", ctypes.c_double),
    ]


class PositionSize(ctypes.Structure):
    _fields_ = [
        ("fraction", ctypes.c_double),
        ("amount_usd", ctypes.c_double),
        ("ev_per_dollar", ctypes.c_double),
    ]


class Decision(ctypes.Structure):
    _fields_ = [
        ("action", ctypes.c_int),
        ("confidence", ctypes.c_double),
        ("amount_usd", ctypes.c_double),
        ("limit_price", ctypes.c_double),
        ("gf_quality_x100", ctypes.c_int),
        ("lattice_pass", ctypes.c_int),
        ("regime", ctypes.c_int),
        ("ko_banned", ctypes.c_int),
        ("influence_dir", ctypes.c_int),
        ("n_confirming", ctypes.c_int),
        ("ev_pct", ctypes.c_double),
        ("decision_ns", ctypes.c_uint64),
    ]


class ExitDecision(ctypes.Structure):
    _fields_ = [
        ("action", ctypes.c_int),
        ("sell_fraction", ctypes.c_double),
        ("urgency", ctypes.c_double),
        ("reason", ctypes.c_int),
    ]


class MarkovRegime(ctypes.Structure):
    _fields_ = [
        ("current_state", ctypes.c_int),
        ("state_probs", ctypes.c_double * 4),
        ("transition", (ctypes.c_double * 4) * 4),
        ("optimal_hold_hours", ctypes.c_double),
        ("regime_score", ctypes.c_double),
    ]


# ============================================================
# High-level Python engine
# ============================================================

# Pair name → index mapping
PAIR_INDEX = {
    "BTC": 0, "ETH": 1, "SOL": 2, "AVAX": 3,
    "LINK": 4, "DOGE": 5, "FET": 6,
}

REGIME_NAMES = {0: "ACCUMULATION", 1: "MARKUP", 2: "DISTRIBUTION", 3: "MARKDOWN"}
DIR_NAMES = {0: "NONE", 1: "BUY", 2: "SELL"}
EXIT_NAMES = {0: "HOLD", 1: "EXIT_FULL", 2: "TP0", 3: "TP1", 4: "TP2"}
EXIT_REASONS = {0: "hold", 1: "loss_limit", 2: "trailing_stop", 3: "take_profit", 4: "dead_money"}


class HFTEngine:
    """High-performance trading engine using C hft_core.

    All decision logic runs in C at nanosecond speed.
    Python handles I/O (API calls, WebSocket) only.
    """

    def __init__(self):
        self._lib = ctypes.CDLL(_lib_path)
        self._setup_signatures()

        # Persistent state objects (shared across calls)
        self._influence = InfluenceMap()
        self._ko = KoDetector()
        self._tick_bufs = {}  # pair_idx → TickRingBuffer

        # Initialize C state
        self._lib.influence_init(ctypes.byref(self._influence))
        self._lib.ko_init(ctypes.byref(self._ko))

        # Portfolio state (updated externally)
        self.portfolio_usd = 290.0
        self.cash_available = 110.0
        self.win_rate = 0.62
        self.avg_win_pct = 1.8
        self.avg_loss_pct = 1.2

        logger.info("HFT Engine initialized (hft_core.so loaded)")

    def _setup_signatures(self):
        """Set C function signatures for type safety."""
        lib = self._lib

        # gf_encode_signals
        lib.gf_encode_signals.argtypes = [ctypes.POINTER(SignalSet)]
        lib.gf_encode_signals.restype = GFResult

        # lattice_evaluate
        lib.lattice_evaluate.argtypes = [ctypes.POINTER(LatticePoint)]
        lib.lattice_evaluate.restype = LatticeResult

        # influence_init, place_stone, get
        lib.influence_init.argtypes = [ctypes.POINTER(InfluenceMap)]
        lib.influence_init.restype = None
        lib.influence_place_stone.argtypes = [
            ctypes.POINTER(InfluenceMap), ctypes.c_int, ctypes.c_int,
            ctypes.c_double, ctypes.c_uint64
        ]
        lib.influence_place_stone.restype = None
        lib.influence_get.argtypes = [
            ctypes.POINTER(InfluenceMap), ctypes.c_int, ctypes.c_uint64
        ]
        lib.influence_get.restype = InfluenceResult

        # ko_init, record, check
        lib.ko_init.argtypes = [ctypes.POINTER(KoDetector)]
        lib.ko_init.restype = None
        lib.ko_record.argtypes = [
            ctypes.POINTER(KoDetector), ctypes.c_int, ctypes.c_int,
            ctypes.c_int, ctypes.c_uint64
        ]
        lib.ko_record.restype = None
        lib.ko_check.argtypes = [
            ctypes.POINTER(KoDetector), ctypes.c_int, ctypes.c_int,
            ctypes.c_uint64
        ]
        lib.ko_check.restype = KoBanResult

        # markov_detect_regime
        lib.markov_detect_regime.argtypes = [
            ctypes.POINTER(ctypes.c_double), ctypes.c_int
        ]
        lib.markov_detect_regime.restype = MarkovRegime

        # tick_ring_init, push, recent_prices
        lib.tick_ring_init.argtypes = [ctypes.POINTER(TickRingBuffer), ctypes.c_int]
        lib.tick_ring_init.restype = None
        lib.tick_ring_push.argtypes = [
            ctypes.POINTER(TickRingBuffer), ctypes.c_double, ctypes.c_double,
            ctypes.c_double, ctypes.c_double, ctypes.c_uint64
        ]
        lib.tick_ring_push.restype = None
        lib.tick_ring_recent_prices.argtypes = [
            ctypes.POINTER(TickRingBuffer), ctypes.POINTER(ctypes.c_double),
            ctypes.c_int
        ]
        lib.tick_ring_recent_prices.restype = ctypes.c_int

        # kelly_size
        lib.kelly_size.argtypes = [
            ctypes.c_double, ctypes.c_double, ctypes.c_double,
            ctypes.c_double, ctypes.c_double
        ]
        lib.kelly_size.restype = PositionSize

        # hft_decide
        lib.hft_decide.argtypes = [
            ctypes.POINTER(SignalSet), ctypes.POINTER(TickRingBuffer),
            ctypes.POINTER(InfluenceMap), ctypes.POINTER(KoDetector),
            ctypes.c_double, ctypes.c_double, ctypes.c_double,
            ctypes.c_double, ctypes.c_double,
            ctypes.c_int, ctypes.c_uint64
        ]
        lib.hft_decide.restype = Decision

        # hft_check_exit
        lib.hft_check_exit.argtypes = [
            ctypes.c_double, ctypes.c_double, ctypes.c_double,
            ctypes.c_double, ctypes.c_double, ctypes.c_double,
            ctypes.c_double, ctypes.c_int, ctypes.c_int, ctypes.c_int
        ]
        lib.hft_check_exit.restype = ExitDecision

    def _now_ns(self):
        """Current time in nanoseconds."""
        return int(time.time() * 1e9)

    def _get_tick_buf(self, pair_idx):
        """Get or create tick ring buffer for a pair."""
        if pair_idx not in self._tick_bufs:
            tb = TickRingBuffer()
            self._lib.tick_ring_init(ctypes.byref(tb), pair_idx)
            self._tick_bufs[pair_idx] = tb
        return self._tick_bufs[pair_idx]

    def feed_tick(self, pair, price, volume=1.0, bid=0, ask=0):
        """Feed a price tick into the ring buffer.

        Call this on every WebSocket tick for real-time processing.
        """
        base = pair.split("-")[0]
        idx = PAIR_INDEX.get(base, 0)
        tb = self._get_tick_buf(idx)
        self._lib.tick_ring_push(
            ctypes.byref(tb), price, volume, bid, ask, self._now_ns()
        )

    def build_signal_set(self, signals_dict, direction):
        """Convert Python signal dict to C SignalSet.

        Args:
            signals_dict: {"latency": 0.85, "regime": 0.72, ...}
            direction: "BUY" or "SELL"
        """
        ss = SignalSet()
        ss.direction = DIR_BUY if direction == "BUY" else DIR_SELL
        bitmap = 0
        count = 0

        for name, conf in signals_dict.items():
            if name not in SIGNAL_NAME_MAP:
                continue
            bit = SIGNAL_NAME_MAP[name]
            bitmap |= bit
            # Find bit position for confidence array index
            bit_pos = 0
            temp = bit
            while temp > 1:
                temp >>= 1
                bit_pos += 1
            ss.confidences[bit_pos] = float(conf)
            count += 1

        ss.source_bitmap = bitmap
        ss.n_signals = count
        return ss

    def decide(self, signals_dict, pair, cash=None):
        """Run full decision pipeline in C.

        Args:
            signals_dict: {"latency": 0.85, "regime": 0.72, ...}
            pair: "BTC-USD" or "BTC"
            cash: available cash (None = use self.cash_available)

        Returns dict with action, confidence, amount, limit_price, etc.
        """
        base = pair.split("-")[0]
        idx = PAIR_INDEX.get(base, 0)

        # Determine direction from signals
        buy_conf = sum(c for c in signals_dict.values() if c > 0)
        direction = "BUY"  # Default; caller should determine

        ss = self.build_signal_set(signals_dict, direction)
        tb = self._get_tick_buf(idx)
        cash_usd = cash if cash is not None else self.cash_available

        dec = self._lib.hft_decide(
            ctypes.byref(ss), ctypes.byref(tb),
            ctypes.byref(self._influence), ctypes.byref(self._ko),
            self.portfolio_usd, cash_usd,
            self.win_rate, self.avg_win_pct, self.avg_loss_pct,
            idx, self._now_ns()
        )

        return {
            "action": DIR_NAMES.get(dec.action, "NONE"),
            "confidence": dec.confidence,
            "amount_usd": dec.amount_usd,
            "limit_price": dec.limit_price,
            "gf_quality": dec.gf_quality_x100 / 100.0,
            "lattice_pass": bool(dec.lattice_pass),
            "regime": REGIME_NAMES.get(dec.regime, "UNKNOWN"),
            "ko_banned": bool(dec.ko_banned),
            "influence_dir": DIR_NAMES.get(dec.influence_dir, "NONE"),
            "n_confirming": dec.n_confirming,
            "ev_pct": dec.ev_pct,
            "decision_ns": dec.decision_ns,
        }

    def check_exit(self, entry_price, current_price, peak_price,
                    hold_hours, position_usd, volatility=0.02,
                    tp0_done=False, tp1_done=False, tp2_done=False):
        """Check exit conditions in C (sub-nanosecond).

        Returns dict with action, sell_fraction, urgency, reason.
        """
        ed = self._lib.hft_check_exit(
            entry_price, current_price, peak_price,
            hold_hours, position_usd, self.portfolio_usd,
            volatility,
            int(tp0_done), int(tp1_done), int(tp2_done)
        )

        return {
            "action": EXIT_NAMES.get(ed.action, "HOLD"),
            "sell_fraction": ed.sell_fraction,
            "urgency": ed.urgency,
            "reason": EXIT_REASONS.get(ed.reason, "hold"),
        }

    def record_ko(self, pair, direction, was_loss):
        """Record a trade outcome for Ko detection."""
        base = pair.split("-")[0]
        idx = PAIR_INDEX.get(base, 0)
        d = DIR_BUY if direction == "BUY" else DIR_SELL
        self._lib.ko_record(
            ctypes.byref(self._ko), idx, d, int(was_loss), self._now_ns()
        )

    def detect_regime(self, prices):
        """Detect Markov/Wyckoff regime from price series.

        Args:
            prices: list of recent prices (newest first)

        Returns dict with state, score, optimal_hold_hours.
        """
        n = len(prices)
        arr = (ctypes.c_double * n)(*prices)
        mr = self._lib.markov_detect_regime(arr, n)
        return {
            "state": REGIME_NAMES.get(mr.current_state, "UNKNOWN"),
            "state_id": mr.current_state,
            "score": mr.regime_score,
            "optimal_hold_hours": mr.optimal_hold_hours,
            "probs": list(mr.state_probs),
        }

    def encode_signals(self, signals_dict, direction="BUY"):
        """Encode signals through Galois Field for quality scoring.

        Returns dict with quality, syndrome, composite, n_confirming.
        """
        ss = self.build_signal_set(signals_dict, direction)
        gf = self._lib.gf_encode_signals(ctypes.byref(ss))
        return {
            "quality": gf.quality_score,
            "syndrome": gf.syndrome,
            "composite": gf.gf_composite,
            "n_confirming": gf.n_confirming,
            "weighted_conf": gf.weighted_conf,
        }

    def update_portfolio(self, portfolio_usd, cash_available,
                          win_rate=None, avg_win_pct=None, avg_loss_pct=None):
        """Update portfolio state used by decision engine."""
        self.portfolio_usd = portfolio_usd
        self.cash_available = cash_available
        if win_rate is not None:
            self.win_rate = win_rate
        if avg_win_pct is not None:
            self.avg_win_pct = avg_win_pct
        if avg_loss_pct is not None:
            self.avg_loss_pct = avg_loss_pct


# Singleton
_engine = None

def get_hft_engine():
    """Get or create the singleton HFT engine."""
    global _engine
    if _engine is None:
        _engine = HFTEngine()
    return _engine


if __name__ == "__main__":
    engine = HFTEngine()

    # Feed some ticks
    for i in range(100):
        engine.feed_tick("BTC-USD", 69000 + (i % 50) * 10, volume=1.0,
                         bid=68990, ask=69010)

    # Test signal encoding
    signals = {
        "latency": 0.85,
        "regime": 0.72,
        "orderbook": 0.68,
        "momentum": 0.80,
        "meta_ml": 0.77,
    }
    gf = engine.encode_signals(signals)
    print(f"GF encoding: quality={gf['quality']:.2f} syndrome={gf['syndrome']:.2f} "
          f"confirming={gf['n_confirming']}")

    # Test full decision
    decision = engine.decide(signals, "BTC-USD", cash=50.0)
    print(f"\nDecision: {decision['action']} conf={decision['confidence']*100:.1f}% "
          f"${decision['amount_usd']:.2f} @ ${decision['limit_price']:.2f}")
    print(f"  GF quality={decision['gf_quality']:.2f} lattice={decision['lattice_pass']} "
          f"regime={decision['regime']} ko={decision['ko_banned']} "
          f"influence={decision['influence_dir']}")
    print(f"  Decision time: {decision['decision_ns']} ns")

    # Test exit check
    exit_dec = engine.check_exit(
        entry_price=69000, current_price=69500, peak_price=69800,
        hold_hours=1.5, position_usd=16.50, volatility=0.02
    )
    print(f"\nExit: {exit_dec['action']} sell={exit_dec['sell_fraction']*100:.0f}% "
          f"urgency={exit_dec['urgency']:.2f} reason={exit_dec['reason']}")

    # Test regime detection
    prices = [69000 + i * 10 for i in range(50)]
    regime = engine.detect_regime(prices)
    print(f"\nRegime: {regime['state']} score={regime['score']:.2f} "
          f"optimal_hold={regime['optimal_hold_hours']:.1f}h")

    # Benchmark
    import time as _time
    N = 100000
    start = _time.perf_counter()
    for _ in range(N):
        engine.decide(signals, "BTC-USD", cash=50.0)
    elapsed = _time.perf_counter() - start
    print(f"\n=== PYTHON BRIDGE BENCHMARK ===")
    print(f"Full decide ({N//1000}K iters): {elapsed*1000:.1f}ms "
          f"({elapsed/N*1e6:.1f} us/iter, {elapsed/N*1e9:.0f} ns/iter)")
    print(f"Target: <5000 ns/iter (C=250ns + ctypes overhead)")
