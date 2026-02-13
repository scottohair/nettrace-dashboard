#!/usr/bin/env python3
"""Micro benchmark for C/C++ execution policy kernels."""

import ctypes
import time
from pathlib import Path


class _Decision(ctypes.Structure):
    _fields_ = [
        ("approved", ctypes.c_int),
        ("required_edge_pct", ctypes.c_double),
        ("penalty_pct", ctypes.c_double),
        ("score", ctypes.c_double),
    ]


def _bench(path, fn_name, iterations=300000):
    lib = ctypes.CDLL(str(path))
    fn = getattr(lib, fn_name)
    fn.argtypes = [
        ctypes.c_double, ctypes.c_double, ctypes.c_double, ctypes.c_double,
        ctypes.c_double, ctypes.c_double, ctypes.c_double, ctypes.c_double,
        ctypes.c_double, ctypes.c_double, ctypes.c_double, ctypes.c_int,
    ]
    fn.restype = _Decision
    t0 = time.perf_counter()
    out = None
    for _ in range(iterations):
        out = fn(2.2, 1.0, 0.05, 220.0, 0.03, 0.82, 0.2, 0.35, 450.0, 0.2, 0.7, 0)
    dt = time.perf_counter() - t0
    return {
        "library": path.name,
        "fn": fn_name,
        "iterations": iterations,
        "elapsed_s": round(dt, 6),
        "ns_per_call": round((dt / iterations) * 1e9, 2),
        "last": {
            "approved": bool(out.approved),
            "required_edge_pct": round(float(out.required_edge_pct), 6),
            "penalty_pct": round(float(out.penalty_pct), 6),
            "score": round(float(out.score), 6),
        },
    }


def main():
    base = Path(__file__).parent
    c_so = base / "fast_exec.so"
    cpp_so = base / "fast_exec_cpp.so"
    if not c_so.exists() or not cpp_so.exists():
        raise SystemExit("Build missing: compile fast_exec.so and fast_exec_cpp.so first")
    c_res = _bench(c_so, "no_loss_gate")
    cpp_res = _bench(cpp_so, "no_loss_gate_cpp")
    print(c_res)
    print(cpp_res)


if __name__ == "__main__":
    main()
