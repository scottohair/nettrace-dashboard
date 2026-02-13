#!/usr/bin/env python3
"""ctypes bridge for fast_exec.so (no-loss policy + latency penalties)."""

import ctypes
import os
from pathlib import Path

_lib_path = str(Path(__file__).parent / "fast_exec.so")


class ExecDecision(ctypes.Structure):
    _fields_ = [
        ("approved", ctypes.c_int),
        ("required_edge_pct", ctypes.c_double),
        ("penalty_pct", ctypes.c_double),
        ("score", ctypes.c_double),
    ]


class FastExec:
    def __init__(self):
        if not os.path.exists(_lib_path):
            raise ImportError(
                f"fast_exec.so not found at {_lib_path}. "
                "Compile with: cc -O3 -shared -fPIC -o fast_exec.so fast_exec.c -lm"
            )
        self._lib = ctypes.CDLL(_lib_path)
        self._lib.latency_penalty_pct.argtypes = [
            ctypes.c_double, ctypes.c_double, ctypes.c_double
        ]
        self._lib.latency_penalty_pct.restype = ctypes.c_double
        self._lib.micro_edge_pct.argtypes = [
            ctypes.c_double, ctypes.c_double, ctypes.c_double
        ]
        self._lib.micro_edge_pct.restype = ctypes.c_double
        self._lib.no_loss_gate.argtypes = [
            ctypes.c_double, ctypes.c_double, ctypes.c_double, ctypes.c_double,
            ctypes.c_double, ctypes.c_double, ctypes.c_double, ctypes.c_double,
            ctypes.c_double, ctypes.c_double, ctypes.c_double, ctypes.c_int,
        ]
        self._lib.no_loss_gate.restype = ExecDecision

    def latency_penalty_pct(self, p90_latency_ms, failure_rate, amount_usd):
        return float(self._lib.latency_penalty_pct(
            float(p90_latency_ms), float(failure_rate), float(amount_usd)
        ))

    def micro_edge_pct(self, fast_avg, slow_avg, spread_pct):
        return float(self._lib.micro_edge_pct(
            float(fast_avg), float(slow_avg), float(spread_pct)
        ))

    def no_loss_gate(
        self,
        expected_edge_pct,
        total_cost_pct,
        spread_pct,
        latency_ms,
        failure_rate,
        signal_confidence,
        min_expected_edge_pct,
        max_spread_pct,
        max_latency_ms,
        max_failure_rate,
        confidence_floor,
        buy_blocked_regime,
    ):
        r = self._lib.no_loss_gate(
            float(expected_edge_pct),
            float(total_cost_pct),
            float(spread_pct),
            float(latency_ms),
            float(failure_rate),
            float(signal_confidence),
            float(min_expected_edge_pct),
            float(max_spread_pct),
            float(max_latency_ms),
            float(max_failure_rate),
            float(confidence_floor),
            int(1 if buy_blocked_regime else 0),
        )
        return {
            "approved": bool(r.approved),
            "required_edge_pct": float(r.required_edge_pct),
            "penalty_pct": float(r.penalty_pct),
            "score": float(r.score),
        }
