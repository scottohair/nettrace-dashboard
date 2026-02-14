import json
import os
import tempfile
import unittest
import sys
from pathlib import Path

try:
    import pytest
except ModuleNotFoundError:
    pytest = None
    _HAS_PYTEST = False
else:
    _HAS_PYTEST = True


if not _HAS_PYTEST:
    class _SimpleMonkeyPatch:
        def __init__(self):
            self._patches = []

        def setattr(self, target, name, value):
            self._patches.append((target, name, getattr(target, name)))
            setattr(target, name, value)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            for target, name, prior in reversed(self._patches):
                setattr(target, name, prior)
            return False

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

    import strategy_pipeline as sp  # noqa: E402
    import warm_promotion_runner as wpr  # noqa: E402
    import warm_runtime_collector as wrc  # noqa: E402

    class _StubHistoricalPrices:
        def __init__(self, rows=3, start=100.0, step=0.5):
            self._rows = self._build_rows(rows, start=start, step=step)

        @staticmethod
        def _build_rows(rows, start=100.0, step=0.5):
            out = []
            for idx in range(rows):
                price = float(start + (idx * step))
                ts = 1700000000 + (idx * 300)
                out.append(
                    {
                        "time": ts,
                        "open": price,
                        "high": price + 0.5,
                        "low": price - 0.5,
                        "close": price,
                        "volume": 1.0,
                    }
                )
            return out

        def get_5min_candles(self, pair, hours=24):
            return list(self._rows)

        def get_candles(self, pair, hours=168):
            return list(self._rows)

        def get_non_candle_rows(self, pair, hours=48, granularity_seconds=300):
            return [dict(row) for row in self._rows]

        def get_cache_meta(self, pair, granularity_seconds, hours):
            return {
                "pair": pair,
                "granularity_seconds": int(granularity_seconds),
                "hours": int(hours),
                "source": "stubbed",
                "candles": len(self._rows),
            }

    def _seed_warm_strategy(db_path, strategy_name="momentum", pair="BTC-USD"):
        sp.PIPELINE_DB = str(db_path)
        wpr.DB = db_path
        wrc.DB = db_path

        validator = sp.StrategyValidator()
        try:
            strategy = sp.MomentumStrategy()
            validator.register_strategy(strategy, pair)
            validator.db.execute(
                "UPDATE strategy_registry SET stage='WARM' WHERE name=? AND pair=?",
                (strategy_name, pair),
            )
            validator.db.commit()
        finally:
            validator.db.close()

    def _run_with_threshold(module, tmp_path, threshold, rows, symbol):
        db_path = Path(tmp_path) / f"{symbol}.db"
        out = Path(tmp_path) / f"{symbol}_report.json"

        module_rows = _StubHistoricalPrices(rows=rows)
        module.OUT = out
        with _SimpleMonkeyPatch() as monkeypatch:
            monkeypatch.setattr(module, "HistoricalPrices", lambda: module_rows)
            monkeypatch.setattr(module, "WARM_MIN_EVIDENCE_CANDLES", threshold)
            monkeypatch.setattr(module, "DB", db_path)
            _seed_warm_strategy(db_path, symbol)
            if module is wpr:
                module.run(hours=1, granularity="5min", promote=False)
            else:
                module.collect_once(hours=1, granularity="5min", promote=False)

        payload = json.loads(out.read_text())
        rows = [r for r in payload.get("results", []) if r.get("strategy_name") == symbol]
        return rows[0]

    class TestWarmEvidenceThresholdDependencies(unittest.TestCase):
        def test_promotion_skip_when_below_threshold(self):
            with tempfile.TemporaryDirectory() as tmp_path:
                row = _run_with_threshold(wpr, tmp_path, threshold=5, rows=2, symbol="momentum")
                self.assertEqual(row["status"], "skipped")
                self.assertEqual(row["reason"], "insufficient_candles:2<5")

        def test_runtime_skip_when_below_threshold(self):
            with tempfile.TemporaryDirectory() as tmp_path:
                row = _run_with_threshold(wrc, tmp_path, threshold=5, rows=3, symbol="momentum")
                self.assertEqual(row["status"], "skipped")
                self.assertEqual(row["reason"], "insufficient_candles:3<5")

        def test_evidence_threshold_snapshot(self):
            with tempfile.TemporaryDirectory() as tmp_path:
                row = _run_with_threshold(wpr, tmp_path, threshold=5, rows=8, symbol="momentum")
                self.assertNotEqual(row["status"], "skipped")

else:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))

    import strategy_pipeline as sp  # noqa: E402
    import warm_promotion_runner as wpr  # noqa: E402
    import warm_runtime_collector as wrc  # noqa: E402


    class _StubHistoricalPrices:
        def __init__(self, rows=3, start=100.0, step=0.5):
            self._rows = self._build_rows(rows, start=start, step=step)

        @staticmethod
        def _build_rows(rows, start=100.0, step=0.5):
            out = []
            for idx in range(rows):
                price = float(start + (idx * step))
                ts = 1700000000 + (idx * 300)
                out.append(
                    {
                        "time": ts,
                        "open": price,
                        "high": price + 0.5,
                        "low": price - 0.5,
                        "close": price,
                        "volume": 1.0,
                    }
                )
            return out

        def get_5min_candles(self, pair, hours=24):
            return list(self._rows)

        def get_candles(self, pair, hours=168):
            return list(self._rows)

        def get_non_candle_rows(self, pair, hours=48, granularity_seconds=300):
            return [dict(row) for row in self._rows]

        def get_cache_meta(self, pair, granularity_seconds, hours):
            return {
                "pair": pair,
                "granularity_seconds": int(granularity_seconds),
                "hours": int(hours),
                "source": "stubbed",
                "candles": len(self._rows),
            }


    def _seed_warm_strategy(monkeypatch, db_path, strategy_name="momentum", pair="BTC-USD"):
        monkeypatch.setattr(sp, "PIPELINE_DB", str(db_path))
        monkeypatch.setattr(wpr, "DB", Path(db_path))
        monkeypatch.setattr(wrc, "DB", Path(db_path))

        validator = sp.StrategyValidator()
        strategy = sp.MomentumStrategy()
        validator.register_strategy(strategy, pair)
        validator.db.execute(
            "UPDATE strategy_registry SET stage='WARM' WHERE name=? AND pair=?",
            (strategy_name, pair),
        )
        validator.db.commit()
        validator.db.close()


    def _run_with_threshold(module, monkeypatch, tmp_path, threshold, rows, symbol):
        db_path = tmp_path / f"{symbol}.db"
        out = tmp_path / f"{symbol}_report.json"

        module_rows = _StubHistoricalPrices(rows=rows)
        monkeypatch.setattr(module, "HistoricalPrices", lambda: module_rows)
        monkeypatch.setattr(module, "WARM_MIN_EVIDENCE_CANDLES", threshold)
        if module is wpr:
            module.OUT = out
            monkeypatch.setattr(module, "DB", db_path)
            _seed_warm_strategy(monkeypatch, db_path, symbol)
            module.run(hours=1, granularity="5min", promote=False)
        else:
            module.OUT = out
            monkeypatch.setattr(module, "DB", db_path)
            _seed_warm_strategy(monkeypatch, db_path, symbol)
            module.collect_once(hours=1, granularity="5min", promote=False)
        payload = json.loads(out.read_text())
        return [r for r in payload.get("results", []) if r.get("strategy_name") == symbol][0]


    def test_warm_promotion_skips_when_evidence_threshold_not_met(monkeypatch, tmp_path):
        row = _run_with_threshold(
            wpr, monkeypatch, tmp_path, threshold=5, rows=2, symbol="momentum"
        )
        assert row["status"] == "skipped"
        assert row["reason"] == "insufficient_candles:2<5"


    def test_warm_runtime_skip_when_evidence_threshold_not_met(monkeypatch, tmp_path):
        row = _run_with_threshold(
            wrc, monkeypatch, tmp_path, threshold=5, rows=3, symbol="momentum"
        )
        assert row["status"] == "skipped"
        assert row["reason"] == "insufficient_candles:3<5"


    def test_warm_threshold_snapshot_changes(monkeypatch, tmp_path):
        low_threshold_row = _run_with_threshold(
            wpr, monkeypatch, tmp_path, threshold=5, rows=8, symbol="momentum"
        )
        assert low_threshold_row["status"] != "skipped"


    def test_warm_promotion_non_candle_mode_enforces_non_candle_evidence(monkeypatch, tmp_path):
        monkeypatch.setattr(wpr, "WARM_EVIDENCE_DATA_MODE", "non_candle")
        monkeypatch.setattr(wpr, "WARM_EVIDENCE_NON_CANDLE_STRICT_MODE", True)
        monkeypatch.setattr(wpr, "WARM_EVIDENCE_NON_CANDLE_MIN_BARS", 5)
        row = _run_with_threshold(
            wpr, monkeypatch, tmp_path, threshold=5, rows=2, symbol="momentum"
        )
        assert row["status"] == "skipped"
        assert row["reason"].startswith("insufficient_non_candle_rows")
