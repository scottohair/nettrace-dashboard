#!/usr/bin/env python3
"""Always-on growth flywheel controller.

Runs strict, repeatable cycles:
  1. Growth supervision (quant + warm collection + audit)
  2. Trading lock sync from GO/NO_GO
  3. Reserve target tracking (USD/USDC/BTC/ETH + MKD/KRW/EUR/GBP)
  4. Claude duplex updates with blockers and upgrade tasks
  5. Periodic native benchmark checks
"""

import argparse
import fcntl
import importlib.util
import json
import logging
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from trading_guard import clear_trading_lock, read_trading_lock, set_trading_lock  # noqa: E402

try:
    from agent_tools import AgentTools  # noqa: E402
except Exception:
    AgentTools = None  # type: ignore

try:
    import claude_duplex  # noqa: E402
except Exception:
    claude_duplex = None

BASE = Path(__file__).parent
STATUS_FILE = BASE / "flywheel_status.json"
CYCLE_LOG = BASE / "flywheel_cycles.jsonl"
REPORT_FILE = BASE / "growth_go_no_go_report.json"
QUANT_RESULTS_FILE = BASE / "quant_100_results.json"
RESERVE_STATUS_FILE = BASE / "reserve_targets_status.json"
TRADER_DB = BASE / "trader.db"
RECONCILE_STATUS_FILE = BASE / "reconcile_agent_trades_status.json"
AUTONOMOUS_WORK_STATUS_FILE = BASE / "autonomous_work_status.json"
EXIT_MANAGER_STATUS_FILE = BASE / "exit_manager_status.json"
FLYWHEEL_LOCK_FILE = BASE / "flywheel.lock"

DEFAULT_INTERVAL_SECONDS = int(os.environ.get("FLYWHEEL_INTERVAL_SECONDS", "240"))
DEFAULT_COLLECTOR_INTERVAL_SECONDS = int(os.environ.get("FLYWHEEL_COLLECTOR_INTERVAL_SECONDS", "300"))
DEFAULT_QUANT_EVERY_CYCLES = int(os.environ.get("FLYWHEEL_QUANT_EVERY_CYCLES", "3"))
DEFAULT_BENCH_EVERY_CYCLES = int(os.environ.get("FLYWHEEL_BENCH_EVERY_CYCLES", "12"))
DEFAULT_PYTHON_BIN = os.environ.get("FLYWHEEL_PYTHON_BIN") or shutil.which("python3") or sys.executable
FLYWHEEL_SUBPROCESS_TIMEOUT_SECONDS = max(
    10, int(os.environ.get("FLYWHEEL_SUBPROCESS_TIMEOUT_SECONDS", "900"))
)
FLYWHEEL_SUPERVISOR_REPORT_MAX_AGE_SECONDS = max(
    60, int(os.environ.get("FLYWHEEL_SUPERVISOR_REPORT_MAX_AGE_SECONDS", "900"))
)
WIN_TASKS_ENABLED = os.environ.get("FLYWHEEL_WIN_TASKS_ENABLED", "1").lower() not in {"0", "false", "no"}
WIN_TASKS_EXECUTE_TOP = int(os.environ.get("FLYWHEEL_WIN_TASKS_EXECUTE_TOP", "0"))
CLAUDE_TEAM_ROLE_LOOP_ENABLED = (
    os.environ.get("FLYWHEEL_CLAUDE_TEAM_LOOP_ENABLED", "1").lower() not in {"0", "false", "no"}
)
CLAUDE_TEAM_ROLE_SOURCES = {"claude_opus", "claude_sonnet", "claude_team"}
RECONCILE_AGENT_TRADES_ENABLED = os.environ.get("FLYWHEEL_RECONCILE_AGENT_TRADES_ENABLED", "1").lower() not in {"0", "false", "no"}
RECONCILE_AGENT_TRADES_MAX_ORDERS = int(os.environ.get("FLYWHEEL_RECONCILE_AGENT_TRADES_MAX_ORDERS", "120"))
RECONCILE_AGENT_TRADES_LOOKBACK_HOURS = int(os.environ.get("FLYWHEEL_RECONCILE_AGENT_TRADES_LOOKBACK_HOURS", "96"))
CLOSE_FIRST_RECONCILE_GROWTH_GATE_ENABLED = (
    os.environ.get("FLYWHEEL_CLOSE_FIRST_RECONCILE_GROWTH_GATE_ENABLED", "1").lower() not in {"0", "false", "no"}
)
CLOSE_FIRST_RECONCILE_BOOTSTRAP_BYPASS = (
    os.environ.get("FLYWHEEL_CLOSE_FIRST_RECONCILE_BOOTSTRAP_BYPASS", "1").lower() not in {"0", "false", "no"}
)
AUTONOMOUS_WORK_ENABLED = (
    os.environ.get("FLYWHEEL_AUTONOMOUS_WORK_ENABLED", "1").lower() not in {"0", "false", "no"}
)
EXIT_MANAGER_HEALTH_GATE_ENABLED = (
    os.environ.get("FLYWHEEL_EXIT_MANAGER_HEALTH_GATE_ENABLED", "1").lower() not in {"0", "false", "no"}
)
EXIT_MANAGER_MAX_STALE_SECONDS = int(
    os.environ.get("FLYWHEEL_EXIT_MANAGER_MAX_STALE_SECONDS", "300")
)
EXIT_MANAGER_REQUIRE_RUNNING = (
    os.environ.get("FLYWHEEL_EXIT_MANAGER_REQUIRE_RUNNING", "1").lower() not in {"0", "false", "no"}
)
EXIT_MANAGER_IDLE_STALE_GRACE_SECONDS = int(
    os.environ.get("FLYWHEEL_EXIT_MANAGER_IDLE_STALE_GRACE_SECONDS", "1800")
)
EXIT_MANAGER_IDLE_MAX_CONSECUTIVE_API_FAILURES = int(
    os.environ.get("FLYWHEEL_EXIT_MANAGER_IDLE_MAX_CONSECUTIVE_API_FAILURES", "6")
)
FLYWHEEL_SINGLETON_ENFORCE = (
    os.environ.get("FLYWHEEL_SINGLETON_ENFORCE", "1").lower() not in {"0", "false", "no"}
)

WIN_OBJECTIVE_TEXT = (
    "WIN = mathematically validated, risk-governed realized gains with continuous "
    "resource optimization and treasury capture in USD/USDC."
)
WIN_STRATEGY_THEMES = [
    "multi_path_algorithmic_trading",
    "radix_feature_experiments_base10_hex",
    "network_stack_latency_tuning",
]


def _parse_float_targets(raw, fallback):
    text = str(raw or "").strip()
    if not text:
        return list(fallback)
    out = []
    for tok in text.split(","):
        t = tok.strip()
        if not t:
            continue
        try:
            val = float(t.replace("_", ""))
        except Exception:
            continue
        if val > 0:
            out.append(val)
    if not out:
        return list(fallback)
    return sorted(set(out))


DAILY_TARGETS_USD = _parse_float_targets(
    os.environ.get("FLYWHEEL_DAILY_TARGETS_USD", ""),
    [1_000.0, 1_000_000.0, 9_999_999.0],
)
RESERVE_TARGET_PCT = {
    "USDC": 35.0,
    "USD": 25.0,
    "BTC": 20.0,
    "ETH": 12.0,
    "MKD": 2.0,
    "KRW": 2.0,
    "EUR": 2.0,
    "GBP": 2.0,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [flywheel] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(BASE / "flywheel.log")),
    ],
)
logger = logging.getLogger("flywheel")


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _tail_lines(text, max_lines=20):
    lines = (text or "").splitlines()
    return "\n".join(lines[-max_lines:])


def _load_json(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _iso_age_seconds(ts_text):
    text = str(ts_text or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())
    except Exception:
        return None


class FlywheelController:
    def __init__(
        self,
        interval_seconds=DEFAULT_INTERVAL_SECONDS,
        collector_interval_seconds=DEFAULT_COLLECTOR_INTERVAL_SECONDS,
        quant_every_cycles=DEFAULT_QUANT_EVERY_CYCLES,
        bench_every_cycles=DEFAULT_BENCH_EVERY_CYCLES,
        enable_claude_updates=True,
        enable_win_tasks=WIN_TASKS_ENABLED,
    ):
        self.interval_seconds = max(30, int(interval_seconds))
        self.collector_interval_seconds = max(30, int(collector_interval_seconds))
        self.quant_every_cycles = max(1, int(quant_every_cycles))
        self.bench_every_cycles = max(2, int(bench_every_cycles))
        self.enable_claude_updates = bool(enable_claude_updates and claude_duplex is not None)
        self.enable_claude_team_loop = bool(self.enable_claude_updates and CLAUDE_TEAM_ROLE_LOOP_ENABLED)
        self.enable_win_tasks = bool(enable_win_tasks)
        self.python_bin = DEFAULT_PYTHON_BIN
        self.running = True
        self.cycle = 0
        self.last_from_claude_id = 0
        self._lock_fh = None
        self._load_runtime_state()

    def _load_runtime_state(self):
        if not STATUS_FILE.exists():
            return
        payload = _load_json(STATUS_FILE, {})
        if not isinstance(payload, dict):
            return
        try:
            self.cycle = max(0, int(payload.get("cycle", 0) or 0))
        except Exception:
            self.cycle = 0
        collab = payload.get("claude_collaboration", {}) if isinstance(payload.get("claude_collaboration"), dict) else {}
        try:
            self.last_from_claude_id = max(0, int(collab.get("last_from_claude_id", 0) or 0))
        except Exception:
            self.last_from_claude_id = 0

    def _run_py(self, script_name, *args, env_overrides=None):
        cmd = [self.python_bin, str(BASE / script_name), *list(args)]
        started = time.time()
        env = os.environ.copy()
        if isinstance(env_overrides, dict):
            for k, v in env_overrides.items():
                env[str(k)] = str(v)
        try:
            proc = subprocess.run(
                cmd,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                env=env,
                timeout=FLYWHEEL_SUBPROCESS_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "cmd": cmd,
                "returncode": 124,
                "elapsed_seconds": round(time.time() - started, 3),
                "stdout_tail": "\n".join((exc.stdout or "").strip().splitlines()[-25:]),
                "stderr_tail": (
                    f"command timed out after {FLYWHEEL_SUBPROCESS_TIMEOUT_SECONDS}s: {exc}"
                ),
                "env_overrides": dict(env_overrides or {}),
                "timed_out": True,
            }
        return {
            "cmd": cmd,
            "returncode": int(proc.returncode),
            "elapsed_seconds": round(time.time() - started, 3),
            "stdout_tail": _tail_lines(proc.stdout, max_lines=25),
            "stderr_tail": _tail_lines(proc.stderr, max_lines=25),
            "env_overrides": dict(env_overrides or {}),
        }

    def _read_growth_decision(self):
        payload = _load_json(REPORT_FILE, {})
        decision = payload.get("decision", {}) if isinstance(payload, dict) else {}
        return decision if isinstance(decision, dict) else {}

    def _growth_decision_allows_go(self):
        payload = _load_json(REPORT_FILE, {})
        generated = payload.get("generated_at", "")
        if not isinstance(payload, dict):
            return False, {}
        age_seconds = _iso_age_seconds(generated)
        if age_seconds is None or age_seconds > FLYWHEEL_SUPERVISOR_REPORT_MAX_AGE_SECONDS:
            return False, {}
        decision = payload.get("decision", {})
        if not isinstance(decision, dict):
            return False, {}
        if not bool(decision.get("go_live", False)):
            return False, decision
        if str(decision.get("decision", "")).strip().upper() != "GO":
            return False, decision
        return True, decision

    def _read_reconcile_close_gate(self, reconcile_cmd=None):
        gate = {
            "enabled": bool(CLOSE_FIRST_RECONCILE_GROWTH_GATE_ENABLED),
            "status_available": False,
            "passed": True,
            "reason": "gate_disabled",
            "attempts": 0,
            "completions": 0,
            "failures": 0,
            "failure_reasons": {},
            "updated_at": "",
        }
        if not CLOSE_FIRST_RECONCILE_GROWTH_GATE_ENABLED:
            return gate

        payload = _load_json(RECONCILE_STATUS_FILE, {})
        if not isinstance(payload, dict) or not payload:
            gate["passed"] = False
            gate["reason"] = "reconcile_status_missing"
            return gate

        summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
        close = payload.get("close_reconciliation", {}) if isinstance(payload.get("close_reconciliation"), dict) else {}
        attempts = int(close.get("attempts", summary.get("close_attempts", 0)) or 0)
        completions = int(close.get("completions", summary.get("close_completions", 0)) or 0)
        failures = int(close.get("failures", summary.get("close_failures", 0)) or 0)
        reasons = close.get("failure_reasons", summary.get("close_failure_reasons", {}))
        if not isinstance(reasons, dict):
            reasons = {}
        gate_passed = close.get("gate_passed", summary.get("close_gate_passed"))
        gate_reason = str(close.get("gate_reason", summary.get("close_gate_reason", "")) or "").strip()
        if not isinstance(gate_passed, bool):
            gate_passed = (attempts <= 0) or (completions > 0)
        if not gate_reason:
            if attempts <= 0:
                gate_reason = "no_pending_sell_closes"
            elif completions > 0:
                gate_reason = "sell_close_completion_observed"
            else:
                gate_reason = "sell_close_completion_missing"

        gate.update(
            {
                "status_available": True,
                "passed": bool(gate_passed),
                "reason": gate_reason,
                "attempts": attempts,
                "completions": completions,
                "failures": failures,
                "failure_reasons": reasons,
                "updated_at": str(payload.get("updated_at", "")),
            }
        )
        rc = None
        if isinstance(reconcile_cmd, dict):
            try:
                rc = int(reconcile_cmd.get("returncode", 0) or 0)
            except Exception:
                rc = 0
        if rc and rc != 0:
            gate["passed"] = False
            gate["reason"] = f"reconcile_command_failed_rc_{rc}"
        return gate

    def _read_exit_manager_gate(self):
        gate = {
            "enabled": bool(EXIT_MANAGER_HEALTH_GATE_ENABLED),
            "status_available": False,
            "passed": True,
            "reason": "gate_disabled",
            "running": False,
            "updated_at": "",
            "age_seconds": None,
            "max_stale_seconds": int(max(30, int(EXIT_MANAGER_MAX_STALE_SECONDS))),
            "require_running": bool(EXIT_MANAGER_REQUIRE_RUNNING),
            "idle_stale_grace_seconds": int(max(30, int(EXIT_MANAGER_IDLE_STALE_GRACE_SECONDS))),
            "idle_max_consecutive_api_failures": int(max(0, int(EXIT_MANAGER_IDLE_MAX_CONSECUTIVE_API_FAILURES))),
            "idle_stale_grace_applied": False,
            "active_positions": 0,
            "consecutive_api_failures": 0,
        }
        if not EXIT_MANAGER_HEALTH_GATE_ENABLED:
            return gate

        payload = _load_json(EXIT_MANAGER_STATUS_FILE, {})
        if not isinstance(payload, dict) or not payload:
            gate["passed"] = False
            gate["reason"] = "exit_manager_status_missing"
            return gate

        running = bool(payload.get("running", False))
        updated_at = str(payload.get("updated_at", ""))
        age = _iso_age_seconds(updated_at)
        max_stale = int(max(30, int(EXIT_MANAGER_MAX_STALE_SECONDS)))
        idle_stale_grace = int(max(max_stale, int(EXIT_MANAGER_IDLE_STALE_GRACE_SECONDS)))
        idle_api_failure_cap = int(max(0, int(EXIT_MANAGER_IDLE_MAX_CONSECUTIVE_API_FAILURES)))
        active_positions = int(payload.get("active_positions", 0) or 0)
        consecutive_api_failures = int(payload.get("consecutive_api_failures", 0) or 0)

        gate.update(
            {
                "status_available": True,
                "running": running,
                "updated_at": updated_at,
                "age_seconds": None if age is None else round(float(age), 3),
                "active_positions": active_positions,
                "consecutive_api_failures": consecutive_api_failures,
            }
        )

        has_open_positions = active_positions > 0
        excessive_idle_api_failures = consecutive_api_failures > idle_api_failure_cap

        if EXIT_MANAGER_REQUIRE_RUNNING and not running and has_open_positions:
            gate["passed"] = False
            gate["reason"] = "exit_manager_not_running"
            return gate
        if age is None:
            gate["passed"] = False
            gate["reason"] = "exit_manager_updated_at_missing"
            return gate
        if age > float(max_stale):
            if (not has_open_positions) and (age <= float(idle_stale_grace)) and (not excessive_idle_api_failures):
                gate["passed"] = True
                gate["idle_stale_grace_applied"] = True
                gate["reason"] = f"exit_manager_idle_stale_grace:{int(age)}s<={idle_stale_grace}s"
                return gate
            gate["passed"] = False
            gate["reason"] = f"exit_manager_status_stale:{int(age)}s>{max_stale}s"
            return gate

        if EXIT_MANAGER_REQUIRE_RUNNING and not running and not has_open_positions:
            gate["passed"] = True
            gate["reason"] = "exit_manager_not_running_idle_no_positions"
            return gate

        gate["reason"] = "exit_manager_healthy"
        return gate

    def _sync_trading_lock(self, decision):
        lock_state = read_trading_lock() or {}
        if not isinstance(lock_state, dict):
            lock_state = {"locked": True, "reason": "invalid_lock_state", "source": "flywheel_controller"}
        go_live = bool(decision.get("go_live", False))

        if go_live:
            if lock_state.get("locked") and lock_state.get("source") != "flywheel_controller":
                logger.warning(
                    "GO decision present, but lock is owned by '%s'; preserving external lock",
                    lock_state.get("source", "unknown"),
                )
                return lock_state
            if lock_state.get("locked") and lock_state.get("source") == "flywheel_controller":
                return clear_trading_lock(
                    source="flywheel_controller",
                    note="growth decision GO",
                )
            return lock_state

        if lock_state.get("locked") and lock_state.get("source") not in {"", "flywheel_controller"}:
            # Another risk governor already locked trading. Preserve ownership and do not overwrite.
            return lock_state

        reason = ", ".join(decision.get("reasons", [])) if isinstance(decision.get("reasons"), list) else "NO_GO"
        return set_trading_lock(
            reason=f"NO_GO: {reason or 'unknown'}",
            source="flywheel_controller",
            metadata={"decision": decision, "set_at": _utc_now()},
        )

    def _get_portfolio_snapshot(self):
        fallback = self._portfolio_snapshot_from_db()
        if AgentTools is None:
            return fallback or {
                "total_usd": 0.0,
                "available_cash": 0.0,
                "held_in_orders": 0.0,
                "holdings": {},
                "source": "unavailable",
            }
        try:
            tools = AgentTools()
            portfolio = tools.get_portfolio()
            if isinstance(portfolio, dict):
                portfolio["source"] = "agent_tools"
                if float(portfolio.get("total_usd", 0.0) or 0.0) > 0:
                    return portfolio
                if fallback:
                    return fallback
                return portfolio
        except Exception as e:
            return fallback or {
                "total_usd": 0.0,
                "available_cash": 0.0,
                "held_in_orders": 0.0,
                "holdings": {},
                "source": f"error:{e}",
            }
        return fallback or {
            "total_usd": 0.0,
            "available_cash": 0.0,
            "held_in_orders": 0.0,
            "holdings": {},
            "source": "empty",
        }

    def _portfolio_snapshot_from_db(self):
        if not TRADER_DB.exists():
            return None
        conn = sqlite3.connect(str(TRADER_DB))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                SELECT total_value_usd, available_cash, held_in_orders, holdings_json
                FROM portfolio_snapshots
                ORDER BY id DESC LIMIT 1
                """
            ).fetchone()
            if not row:
                return None
            holdings = {}
            try:
                holdings = json.loads(row["holdings_json"] or "{}")
                if not isinstance(holdings, dict):
                    holdings = {}
            except Exception:
                holdings = {}
            return {
                "total_usd": round(float(row["total_value_usd"] or 0.0), 2),
                "available_cash": round(float(row["available_cash"] or 0.0), 2),
                "held_in_orders": round(float(row["held_in_orders"] or 0.0), 2),
                "holdings": holdings,
                "source": "trader_db_snapshot_fallback",
            }
        except Exception:
            return None
        finally:
            conn.close()

    def _reserve_targets_snapshot(self, portfolio):
        holdings = portfolio.get("holdings", {}) if isinstance(portfolio.get("holdings"), dict) else {}
        total = float(portfolio.get("total_usd", 0.0) or 0.0)
        rows = []
        for asset, target_pct in RESERVE_TARGET_PCT.items():
            actual_usd = float((holdings.get(asset) or {}).get("usd_value", 0.0) or 0.0)
            target_usd = total * (float(target_pct) / 100.0)
            gap_usd = target_usd - actual_usd
            rows.append(
                {
                    "asset": asset,
                    "target_pct": round(float(target_pct), 3),
                    "target_usd": round(target_usd, 2),
                    "actual_usd": round(actual_usd, 2),
                    "gap_usd": round(gap_usd, 2),
                }
            )
        rows.sort(key=lambda x: abs(x["gap_usd"]), reverse=True)
        snapshot = {
            "updated_at": _utc_now(),
            "portfolio_total_usd": round(total, 2),
            "targets": rows,
        }
        try:
            RESERVE_STATUS_FILE.write_text(json.dumps(snapshot, indent=2))
        except Exception as e:
            logger.error("failed writing reserve status: %s", e)
        return snapshot

    def _daily_realized_pnl(self):
        if not TRADER_DB.exists():
            return 0.0
        conn = sqlite3.connect(str(TRADER_DB))
        try:
            table_rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            tables = {str(r[0]) for r in table_rows if r and r[0]}
            live = 0.0
            agent = 0.0
            if "live_trades" in tables:
                live = conn.execute(
                    "SELECT COALESCE(SUM(COALESCE(pnl, 0)), 0) FROM live_trades "
                    "WHERE date(created_at, 'utc')=date('now', 'utc')"
                ).fetchone()[0]
            if "agent_trades" in tables:
                agent = conn.execute(
                    "SELECT COALESCE(SUM(COALESCE(pnl, 0)), 0) FROM agent_trades "
                    "WHERE date(created_at, 'utc')=date('now', 'utc')"
                ).fetchone()[0]
            return round(float(live or 0.0) + float(agent or 0.0), 6)
        finally:
            conn.close()

    def _target_progress(self, realized_daily_pnl):
        now = datetime.now(timezone.utc)
        elapsed_seconds = now.hour * 3600 + now.minute * 60 + now.second
        remaining_seconds = max(1, 86400 - elapsed_seconds)
        progress = []
        for target in DAILY_TARGETS_USD:
            ratio = 0.0 if target <= 0 else (float(realized_daily_pnl) / float(target))
            remaining = max(0.0, float(target) - float(realized_daily_pnl))
            required_hourly = remaining / (remaining_seconds / 3600.0)
            progress.append(
                {
                    "target_usd_per_day": int(target),
                    "realized_usd_today": round(float(realized_daily_pnl), 6),
                    "progress_pct": round(max(0.0, ratio * 100.0), 4),
                    "remaining_usd": round(remaining, 2),
                    "required_hourly_run_rate_usd": round(required_hourly, 2),
                    "day_elapsed_pct_utc": round(elapsed_seconds / 86400.0, 4),
                }
            )
        return progress

    def _metal_runtime_snapshot(self):
        snapshot = {
            "mlx_available": bool(importlib.util.find_spec("mlx") is not None),
            "torch_available": bool(importlib.util.find_spec("torch") is not None),
            "torch_mps_available": False,
        }
        if snapshot["torch_available"]:
            try:
                import torch  # type: ignore

                snapshot["torch_mps_available"] = bool(torch.backends.mps.is_available())  # type: ignore[attr-defined]
            except Exception:
                snapshot["torch_mps_available"] = False
        return snapshot

    def _quant_blockers(self):
        payload = _load_json(QUANT_RESULTS_FILE, {})
        summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
        blockers = summary.get("rejection_reasons_top", [])
        if not isinstance(blockers, list):
            blockers = []
        compact = []
        for item in blockers[:5]:
            if isinstance(item, list) and len(item) >= 2:
                compact.append({"reason": str(item[0]), "count": int(item[1])})
        return compact

    def _maybe_send_claude_update(self, cycle_payload):
        if not self.enable_claude_updates or claude_duplex is None:
            return None
        decision = cycle_payload.get("growth_decision", {})
        pipeline = decision.get("pipeline_metrics", {}) if isinstance(decision, dict) else {}
        target_rows = cycle_payload.get("target_progress", []) if isinstance(cycle_payload.get("target_progress"), list) else []
        top_target = target_rows[0] if target_rows else {}
        win1000 = cycle_payload.get("win_1000", {}) if isinstance(cycle_payload.get("win_1000"), dict) else {}
        message = (
            "Flywheel cycle complete. "
            f"decision={decision.get('decision', 'UNKNOWN')} "
            f"hot={pipeline.get('stage_counts', {}).get('HOT', 0)} "
            f"warm={pipeline.get('stage_counts', {}).get('WARM', 0)} "
            f"funded_budget=${pipeline.get('total_funded_budget', 0)}. "
            f"run_rate_need=${float(top_target.get('required_hourly_run_rate_usd', 0.0) or 0.0):,.2f}/hr. "
            f"win1000_worked={int(win1000.get('worked_through_count', 0) or 0)}/{int(win1000.get('tasks_total', 0) or 0)}. "
            "Best-in-class mandate: reuse proven strategies first, then ship paradigm-shift upgrades only when they beat baseline."
        )
        return claude_duplex.send_to_claude(
            message,
            msg_type="pipeline_update",
            priority="high" if not bool(decision.get("go_live", False)) else "normal",
            source="flywheel_controller",
            meta={
                "cycle": self.cycle,
                "decision": decision,
                "quant_blockers": cycle_payload.get("quant_blockers", []),
                "reserve_status_file": str(RESERVE_STATUS_FILE),
                "win_objective": WIN_OBJECTIVE_TEXT,
                "strategy_themes": WIN_STRATEGY_THEMES,
                "target_progress": target_rows[:3],
                "win_1000": win1000,
                "operator_directive": "Do not reinvent solved components; prioritize composable upgrades with measurable edge.",
            },
        )

    def _send_claude_role_directives(self, cycle_payload):
        if not self.enable_claude_team_loop or claude_duplex is None:
            return {"sent_count": 0, "sent_ids": [], "priority": "none"}

        decision = cycle_payload.get("growth_decision", {}) if isinstance(cycle_payload, dict) else {}
        pipeline = decision.get("pipeline_metrics", {}) if isinstance(decision, dict) else {}
        reasons = decision.get("reasons", []) if isinstance(decision.get("reasons"), list) else []
        top_reasons = [str(r) for r in reasons[:3]]
        quant_blockers = cycle_payload.get("quant_blockers", []) if isinstance(cycle_payload.get("quant_blockers"), list) else []
        blocker_text = ", ".join(b.get("reason", "unknown") for b in quant_blockers[:3] if isinstance(b, dict)) or "none"
        target_rows = cycle_payload.get("target_progress", []) if isinstance(cycle_payload.get("target_progress"), list) else []
        top_target = target_rows[0] if target_rows else {}
        req_hourly = float(top_target.get("required_hourly_run_rate_usd", 0.0) or 0.0)
        priority = "high" if not bool(decision.get("go_live", False)) else "normal"
        meta_common = {
            "cycle": self.cycle,
            "decision": decision.get("decision", "UNKNOWN"),
            "go_live": bool(decision.get("go_live", False)),
            "decision_reasons": top_reasons,
            "quant_blockers": quant_blockers[:5],
            "run_rate_required_hourly_usd": req_hourly,
            "stage_counts": pipeline.get("stage_counts", {}) if isinstance(pipeline, dict) else {},
            "funded_budget_usd": float(pipeline.get("total_funded_budget", 0.0) or 0.0),
        }

        opus_msg = (
            f"Opus directive cycle {self.cycle}: propose top research/risk architecture upgrades "
            f"to convert NO_GO blockers into realized-close-positive evidence. "
            f"Primary blockers={top_reasons or ['none']} quant_blockers={blocker_text}."
        )
        sonnet_msg = (
            f"Sonnet directive cycle {self.cycle}: implement execution and exit-manager fixes that "
            f"increase SELL close completion and realized PnL quality. "
            f"Required run rate=${req_hourly:,.2f}/hr."
        )
        team_msg = (
            f"Team directive cycle {self.cycle}: Opus and Sonnet converge on one measurable patch set "
            f"for next cycle with deterministic acceptance checks and rollback criteria."
        )
        sent_rows = [
            claude_duplex.send_to_claude(
                opus_msg,
                msg_type="role_directive",
                priority=priority,
                source="flywheel_controller",
                meta={**meta_common, "target_role": "opus"},
            ),
            claude_duplex.send_to_claude(
                sonnet_msg,
                msg_type="role_directive",
                priority=priority,
                source="flywheel_controller",
                meta={**meta_common, "target_role": "sonnet"},
            ),
            claude_duplex.send_to_claude(
                team_msg,
                msg_type="team_directive",
                priority=priority,
                source="flywheel_controller",
                meta={**meta_common, "target_role": "team"},
            ),
        ]
        return {
            "sent_count": len(sent_rows),
            "sent_ids": [int(r.get("id", 0) or 0) for r in sent_rows],
            "priority": priority,
        }

    def _ingest_claude_role_updates(self, limit=250):
        if not self.enable_claude_team_loop or claude_duplex is None:
            return {
                "received_count_total": 0,
                "received_role_count": 0,
                "last_from_claude_id": int(self.last_from_claude_id),
                "sources": {},
                "msg_types": {},
                "latest": [],
            }

        rows = claude_duplex.read_from_claude(since_id=self.last_from_claude_id, limit=limit)
        if not rows:
            return {
                "received_count_total": 0,
                "received_role_count": 0,
                "last_from_claude_id": int(self.last_from_claude_id),
                "sources": {},
                "msg_types": {},
                "latest": [],
            }

        try:
            self.last_from_claude_id = max(int(r.get("id", 0) or 0) for r in rows)
        except Exception:
            pass

        role_rows = []
        for row in rows:
            source = str(row.get("source", "")).lower().strip()
            msg_type = str(row.get("msg_type", "")).lower().strip()
            if source in CLAUDE_TEAM_ROLE_SOURCES or msg_type in {"role_update", "team_consensus"}:
                role_rows.append(row)

        source_counts = {}
        msg_type_counts = {}
        latest_by_source = {}
        for row in role_rows:
            source = str(row.get("source", "unknown"))
            msg_type = str(row.get("msg_type", "unknown"))
            source_counts[source] = int(source_counts.get(source, 0)) + 1
            msg_type_counts[msg_type] = int(msg_type_counts.get(msg_type, 0)) + 1
            latest_by_source[source] = row

        latest = []
        for source in sorted(latest_by_source.keys()):
            row = latest_by_source[source]
            latest.append(
                {
                    "source": source,
                    "id": int(row.get("id", 0) or 0),
                    "msg_type": str(row.get("msg_type", "unknown")),
                    "timestamp": str(row.get("timestamp", "")),
                    "message": str(row.get("message", ""))[:220],
                }
            )
        return {
            "received_count_total": len(rows),
            "received_role_count": len(role_rows),
            "last_from_claude_id": int(self.last_from_claude_id),
            "sources": source_counts,
            "msg_types": msg_type_counts,
            "latest": latest,
        }

    def _run_claude_collaboration(self, cycle_payload):
        if not self.enable_claude_updates or claude_duplex is None:
            return {
                "enabled": False,
                "team_loop_enabled": False,
                "reason": "claude_duplex_unavailable",
                "last_from_claude_id": int(self.last_from_claude_id),
                "sent": {"sent_count": 0, "sent_ids": [], "priority": "none"},
                "received": {
                    "received_count_total": 0,
                    "received_role_count": 0,
                    "last_from_claude_id": int(self.last_from_claude_id),
                    "sources": {},
                    "msg_types": {},
                    "latest": [],
                },
            }

        sent = self._send_claude_role_directives(cycle_payload)
        received = self._ingest_claude_role_updates()
        return {
            "enabled": True,
            "team_loop_enabled": bool(self.enable_claude_team_loop),
            "last_from_claude_id": int(self.last_from_claude_id),
            "sent": sent,
            "received": received,
        }

    def _write_status(self, payload):
        STATUS_FILE.write_text(json.dumps(payload, indent=2))
        with CYCLE_LOG.open("a") as f:
            f.write(json.dumps(payload) + "\n")

    def _acquire_singleton_lock(self):
        if not FLYWHEEL_SINGLETON_ENFORCE:
            return True
        try:
            FLYWHEEL_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
            fh = open(FLYWHEEL_LOCK_FILE, "a+")
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                fh.seek(0)
                holder = fh.read().strip()
                logger.error("Another flywheel instance appears active (lock holder: %s)", holder or "unknown")
                fh.close()
                return False
            fh.seek(0)
            fh.truncate()
            fh.write(json.dumps({"pid": os.getpid(), "started_at": _utc_now()}))
            fh.flush()
            self._lock_fh = fh
            return True
        except Exception as e:
            logger.error("Failed acquiring flywheel singleton lock: %s", e)
            return False

    def _release_singleton_lock(self):
        if self._lock_fh is None:
            return
        try:
            fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            self._lock_fh.close()
        except Exception:
            pass
        self._lock_fh = None

    def run_cycle(self, force_quant=False):
        self.cycle += 1
        cycle_started = time.time()
        quant_run = bool(force_quant or (self.cycle % self.quant_every_cycles == 1))
        bench_run = bool(self.cycle % self.bench_every_cycles == 0)

        commands = []
        reconcile_cmd = None
        if RECONCILE_AGENT_TRADES_ENABLED:
            reconcile_cmd = self._run_py(
                "reconcile_agent_trades.py",
                "--max-orders",
                str(int(RECONCILE_AGENT_TRADES_MAX_ORDERS)),
                "--lookback-hours",
                str(int(RECONCILE_AGENT_TRADES_LOOKBACK_HOURS)),
            )
            commands.append(reconcile_cmd)
        reconcile_close_gate = self._read_reconcile_close_gate(reconcile_cmd=reconcile_cmd)
        reconcile_gate_failed = bool(
            RECONCILE_AGENT_TRADES_ENABLED
            and reconcile_close_gate.get("enabled", False)
            and not reconcile_close_gate.get("passed", False)
        )
        # In bootstrap mode, don't let reconcile gate block the growth supervisor.
        # The growth supervisor has its own close flow gate with bootstrap awareness.
        bootstrap_bypass = False
        if reconcile_gate_failed and CLOSE_FIRST_RECONCILE_BOOTSTRAP_BYPASS:
            prev_report = _load_json(REPORT_FILE, {})
            prev_decision = prev_report.get("decision", {}) if isinstance(prev_report, dict) else {}
            bootstrap_bypass = bool(prev_decision.get("hot_evidence_bootstrap", {}).get("active", False))
            if bootstrap_bypass:
                logger.info(
                    "Reconcile gate failed but bootstrap active — bypassing: %s",
                    reconcile_close_gate.get("reason", "unknown"),
                )
        close_gate_blocks_growth = reconcile_gate_failed and not bootstrap_bypass
        exit_manager_gate = self._read_exit_manager_gate()
        used_supervisor_fallback = False

        supervisor_args = ["--collector-interval-seconds", str(self.collector_interval_seconds)]
        if quant_run:
            supervisor_args.append("--quant-run")
        if close_gate_blocks_growth:
            skip_reason = f"sell_close_reconcile_gate_failed:{reconcile_close_gate.get('reason', 'unknown')}"
            supervisor_cmd = {
                "cmd": [self.python_bin, str(BASE / "growth_supervisor.py"), *supervisor_args],
                "returncode": 0,
                "elapsed_seconds": 0.0,
                "stdout_tail": "",
                "stderr_tail": "",
                "env_overrides": {},
                "skipped": True,
                "skip_reason": skip_reason,
            }
            commands.append(supervisor_cmd)
            decision = {
                "decision": "NO_GO",
                "go_live": False,
                "reasons": [skip_reason],
                "warnings": [],
                "close_reconcile_gate": {
                    "enabled": bool(reconcile_close_gate.get("enabled", False)),
                    "passed": bool(reconcile_close_gate.get("passed", False)),
                    "reason": str(reconcile_close_gate.get("reason", "unknown")),
                    "attempts": int(reconcile_close_gate.get("attempts", 0) or 0),
                    "completions": int(reconcile_close_gate.get("completions", 0) or 0),
                    "failures": int(reconcile_close_gate.get("failures", 0) or 0),
                    "failure_reasons": dict(reconcile_close_gate.get("failure_reasons", {}) or {}),
                },
            }
        else:
            supervisor_cmd = self._run_py("growth_supervisor.py", *supervisor_args)
            commands.append(supervisor_cmd)
            decision = self._read_growth_decision()
            if supervisor_cmd.get("returncode", 0) != 0 and supervisor_cmd.get("timed_out"):
                fallback_go, fallback_decision = self._growth_decision_allows_go()
                if fallback_go:
                    decision = dict(fallback_decision)
                    reasons = list(decision.get("reasons", []) or [])
                    reasons.append(
                        f"growth_supervisor_timeout_fallback_rc_{supervisor_cmd.get('returncode')}"
                    )
                    decision["reasons"] = sorted(set(str(r) for r in reasons if str(r)))
                    decision["warnings"] = sorted(
                        set(str(w) for w in (decision.get("warnings", []) or []) + ["supervisor_timeout_fallback"])
                    )
                    decision["go_live"] = True
                    decision["decision"] = "GO"
                    used_supervisor_fallback = True
        supervisor_failed = supervisor_cmd["returncode"] != 0

        if bench_run:
            commands.append(self._run_py("bench_fast_exec.py"))
        commands.append(self._run_py("claude_stager_agent.py", "--once"))
        if AUTONOMOUS_WORK_ENABLED:
            commands.append(self._run_py("autonomous_work_manager.py", "--once"))
        if self.enable_win_tasks:
            win_exec_top = WIN_TASKS_EXECUTE_TOP if self.cycle % 3 == 1 else 0
            commands.append(
                self._run_py(
                    "win_1000_runner.py",
                    "--work-through-all",
                    "--execute-top",
                    str(int(win_exec_top)),
                )
            )

        # Auto-heal concentration NO_GO by forcing stricter rebalance and re-evaluating in-cycle.
        if (
            not close_gate_blocks_growth
            and not supervisor_failed
            and isinstance(decision, dict)
            and "funding_concentration_above_cap" in (decision.get("reasons", []) or [])
        ):
            heal_env = {
                "WARM_MAX_PAIR_BUDGET_SHARE": "0.68",
                "GROWTH_MAX_PAIR_SHARE_CAP": "0.70",
            }
            commands.append(
                self._run_py(
                    "rebalance_funded_budgets.py",
                    env_overrides=heal_env,
                )
            )
            commands.append(
                self._run_py(
                    "growth_supervisor.py",
                    "--collector-interval-seconds",
                    str(self.collector_interval_seconds),
                    env_overrides=heal_env,
                )
            )
            decision = self._read_growth_decision()

        if supervisor_failed and not used_supervisor_fallback:
            decision = {
                "decision": "NO_GO",
                "go_live": False,
                "reasons": [
                    f"growth_supervisor_failed_rc_{supervisor_cmd['returncode']}"
                ],
            }

        if exit_manager_gate.get("enabled", False) and not exit_manager_gate.get("passed", False):
            reasons = decision.get("reasons", []) if isinstance(decision.get("reasons"), list) else []
            reasons = list(reasons)
            reasons.append(f"exit_manager_gate_failed:{exit_manager_gate.get('reason', 'unknown')}")
            decision["reasons"] = sorted(set(str(r) for r in reasons if str(r).strip()))
            decision["go_live"] = False
            decision["decision"] = "NO_GO"

        lock_state = self._sync_trading_lock(decision)
        portfolio = self._get_portfolio_snapshot()
        reserve_snapshot = self._reserve_targets_snapshot(portfolio)
        daily_pnl = self._daily_realized_pnl()
        target_progress = self._target_progress(daily_pnl)
        metal_runtime = self._metal_runtime_snapshot()
        quant_blockers = self._quant_blockers()

        payload = {
            "updated_at": _utc_now(),
            "cycle": self.cycle,
            "quant_run": quant_run,
            "bench_run": bench_run,
            "cycle_elapsed_seconds": round(time.time() - cycle_started, 3),
            "growth_decision": decision,
            "reconcile_close_gate": reconcile_close_gate,
            "exit_manager_gate": exit_manager_gate,
            "trading_lock": lock_state,
            "portfolio": {
                "total_usd": round(float(portfolio.get("total_usd", 0.0) or 0.0), 2),
                "available_cash": round(float(portfolio.get("available_cash", 0.0) or 0.0), 2),
                "held_in_orders": round(float(portfolio.get("held_in_orders", 0.0) or 0.0), 2),
                "source": portfolio.get("source", "unknown"),
            },
            "reserve_targets": reserve_snapshot,
            "realized_daily_pnl_usd": round(float(daily_pnl), 6),
            "target_progress": target_progress,
            "win_objective": {
                "definition": WIN_OBJECTIVE_TEXT,
                "strategy_themes": WIN_STRATEGY_THEMES,
                "treasury_assets_priority": ["USD", "USDC"],
            },
            "metal_runtime": metal_runtime,
            "quant_blockers": quant_blockers,
            "commands": commands,
        }
        win_status = _load_json(BASE / "win_1000_status.json", {})
        if isinstance(win_status, dict) and win_status:
            payload["win_1000"] = {
                "tasks_total": int(win_status.get("tasks_total", 0) or 0),
                "worked_through_count": int(win_status.get("worked_through_count", 0) or 0),
                "status_counts": win_status.get("status_counts", {}),
                "priority_counts": win_status.get("priority_counts", {}),
                "state_snapshot": win_status.get("state_snapshot", {}),
                "updated_at": win_status.get("updated_at", ""),
            }
        autonomous_status = _load_json(AUTONOMOUS_WORK_STATUS_FILE, {})
        if isinstance(autonomous_status, dict) and autonomous_status:
            payload["autonomous_work"] = {
                "queue_size": int(autonomous_status.get("queue_size", 0) or 0),
                "counts": autonomous_status.get("counts", {}),
                "dispatch": autonomous_status.get("dispatch", {}),
                "feedback": autonomous_status.get("feedback", {}),
                "updated_at": autonomous_status.get("updated_at", ""),
            }
        claude_msg = self._maybe_send_claude_update(payload)
        if claude_msg:
            payload["claude_update"] = {"id": claude_msg.get("id"), "timestamp": claude_msg.get("timestamp")}
        payload["claude_collaboration"] = self._run_claude_collaboration(payload)
        self._write_status(payload)

        logger.info(
            "cycle=%d decision=%s quant_run=%s pnl_today=%+.4f total=$%.2f lock=%s",
            self.cycle,
            decision.get("decision", "UNKNOWN"),
            quant_run,
            float(daily_pnl),
            float(portfolio.get("total_usd", 0.0) or 0.0),
            "ON" if lock_state.get("locked") else "OFF",
        )
        return payload

    def run_forever(self):
        if not self._acquire_singleton_lock():
            logger.error("flywheel singleton lock acquisition failed; exiting")
            return

        def _stop(signum, _frame):
            logger.info("received signal %d; stopping flywheel...", signum)
            self.running = False

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

        logger.info(
            "flywheel started interval=%ss quant_every=%s bench_every=%s claude_updates=%s team_loop=%s",
            self.interval_seconds,
            self.quant_every_cycles,
            self.bench_every_cycles,
            self.enable_claude_updates,
            self.enable_claude_team_loop,
        )
        try:
            while self.running:
                try:
                    self.run_cycle(force_quant=False)
                except Exception as e:
                    logger.error("flywheel cycle failed: %s", e, exc_info=True)
                for _ in range(self.interval_seconds):
                    if not self.running:
                        break
                    time.sleep(1)
            logger.info("flywheel stopped")
        finally:
            self._release_singleton_lock()


def print_status():
    if not STATUS_FILE.exists():
        print("No flywheel status file yet.")
        return
    print(STATUS_FILE.read_text())


def main():
    parser = argparse.ArgumentParser(description="Always-on growth flywheel controller")
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    parser.add_argument(
        "--once-no-force-quant",
        action="store_true",
        help="when used with --once, run a non-forced quant cycle",
    )
    parser.add_argument("--status", action="store_true", help="print current status JSON")
    parser.add_argument("--interval-seconds", type=int, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--collector-interval-seconds", type=int, default=DEFAULT_COLLECTOR_INTERVAL_SECONDS)
    parser.add_argument("--quant-every-cycles", type=int, default=DEFAULT_QUANT_EVERY_CYCLES)
    parser.add_argument("--bench-every-cycles", type=int, default=DEFAULT_BENCH_EVERY_CYCLES)
    parser.add_argument("--no-claude-updates", action="store_true")
    args = parser.parse_args()

    if args.status:
        print_status()
        return

    controller = FlywheelController(
        interval_seconds=args.interval_seconds,
        collector_interval_seconds=args.collector_interval_seconds,
        quant_every_cycles=args.quant_every_cycles,
        bench_every_cycles=args.bench_every_cycles,
        enable_claude_updates=(not args.no_claude_updates),
    )
    if args.once:
        print(json.dumps(controller.run_cycle(force_quant=(not args.once_no_force_quant)), indent=2))
        return
    controller.run_forever()


if __name__ == "__main__":
    main()
