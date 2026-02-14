## Production Cutover Runbook

### 1) Convergence Goal

Move the latest validated code (`agents/live_trader.py` and `agents/quant_company_agent.py`) into controlled production execution with minimal risk and reversible behavior.

### 2) Pre-Flight (Green Required)

Run these before any production toggle:

- `python3.11 -m py_compile agents/live_trader.py agents/quant_company_agent.py`
- `pytest -q tests/test_live_trader.py tests/test_fly_agent_runner.py tests/test_exchange_connector.py tests/test_quant_company_realized_evidence.py tests/test_realized_reconciliation.py`
- `pytest -q` (full suite)
- Ensure `agents/orchestrator_v2.py` can be parsed/runs status:
  - `python3.11 -m py_compile agents/orchestrator_v2.py`
- Confirm trading lock is clear:
  - `python3.11 -c "from pathlib import Path; print(Path('agents/trading_lock.json').exists())"`

### 3) Pre-Cutover Health Snapshot

- Capture baseline:
  - ```bash
    python3.11 tools/safe_integration/integration_guard.py --root .
    ```
  - ```bash
    python3.11 - <<'PY'
    import sys
    sys.path.insert(0, "agents")
    from quant_company_agent import _realized_close_evidence
    print(_realized_close_evidence())
    PY
    ```
- Capture open process state:
  - `python3.11 agents/orchestrator_v2.py status`
- Confirm no concurrent non-orchestrator trader processes (`ps` + script names are informational only).

### 4) Staged Rollout (Thread-Safe)

Default posture is conservative; stage in this order:

1. **Dry run in process controls**
   - Keep all agent configs unchanged.
   - Confirm one-run orchestration and stop behavior:
     - `python3.11 agents/orchestrator_v2.py status`
     - `python3.11 agents/orchestrator_v2.py stop`
2. **Enable one target agent at a time**
   - Update the target agent config in `agents/orchestrator_v2.py` from `"enabled": False` to `true` for only one trading component.
3. **Start and verify singleton lock**
   - `python3.11 agents/orchestrator_v2.py run`
   - Confirm one PID and one lock owner from status output.
4. **Watch for 15 minutes with fixed sampling**
   - Tail logs:
     - `tail -f agents/orchestrator.log`
     - `tail -f agents/live_trader.log`
   - Ensure no rapid error loops and no duplicate trade loops for same signal window.
5. **Gate-check escalation logic**
   - Re-run:
    - ```bash
      python3.11 - <<'PY'
      import sys
      sys.path.insert(0, "agents")
      from quant_company_agent import _realized_close_evidence
      print(_realized_close_evidence())
      PY
      ```
   - Ensure fallback source and thresholds remain as expected.
6. **Expand to secondary agents only after 30-60 minutes stable execution**
   - Add one additional enabled agent only if no risk flags and no duplicate instance errors.

### 5) Cutover Abort Criteria (Hard Stop)

Immediately stop all agents on any of:

- `live_trader` enters repeated route rejection loops (>3 consecutive in 5 minutes).
- DB query/persistence failure repeated across cycles (`trader_db`/`exit_manager` evidence source becomes unstable).
- Orchestrator reports duplicate external process detection for the same script.
- Any hard-risk condition from orchestrator risk module is raised.

Abort command:

- `python3.11 agents/orchestrator_v2.py stop`
- If stop fails:
  - Inspect PID and kill:
    - `cat agents/orchestrator.pid`
    - `kill -TERM <pid>`
- If lock stuck:
  - `python3.11 -c "from agents.orchestrator_v2 import _cleanup_stale_lock_file; _cleanup_stale_lock_file(expected_pid=None)"`

### 6) Rollback (Instant Reversal)

- Set target agent configs back to `"enabled": False` in orchestrator config.
- Run:
  - `python3.11 agents/orchestrator_v2.py stop`
- Confirm all related logs stop advancing.
- Validate no new fills were written during rollback window:
  - `sqlite3 agents/trader.db "SELECT id,pair,side,status,created_at FROM live_trades ORDER BY id DESC LIMIT 20;"`
- Persist incident notes and evidence path (`trader.db`/`exit_manager.db`) before any additional changes.

### 7) Post-Cutover Validation Window

After one steady cycle:

- `python3.11 agents/orchestrator_v2.py status`
- `python3.11 -m pytest -q`
- ```bash
  python3.11 - <<'PY'
  import sys
  sys.path.insert(0, "agents")
  from quant_company_agent import _realized_close_evidence
  print(_realized_close_evidence())
  PY
  ```
- Verify live_trader execution metrics are stable (filled-to-attempt ratio, queue latency, and no risk fallback spikes).

### 8) Emergency Controls

- If you need to freeze risk quickly:
  - Use existing lock mechanism (orchestrator stop).
  - Do not delete local DB files during incidents.
- If a bad config is suspected, revert config change only and keep orchestrator restarted from the known-good state.
