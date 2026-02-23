#!/usr/bin/env python3
"""Launch trading agents as proper daemon processes (double-fork)."""
import os, sys, subprocess, time, signal

AGENTS = [
    {"name": "sniper", "cmd": ["python3", "agents/sniper.py"], "log": "agents/sniper_stdout.log"},
    {"name": "orchestrator", "cmd": ["python3", "agents/realtime_orchestrator.py"], "log": "agents/realtime_orchestrator_stdout.log"},
    {"name": "exchange_scanner", "cmd": ["python3", "agents/exchange_scanner.py"], "log": "agents/exchange_scanner_stdout.log"},
    {"name": "latency_arb", "cmd": ["python3", "agents/latency_arb_agent.py"], "log": "agents/latency_arb_stdout.log"},
    {"name": "capital_allocator", "cmd": ["python3", "agents/capital_allocator.py", "run"], "log": "agents/capital_allocator_stdout.log"},
    {"name": "exit_manager", "cmd": ["python3", "agents/exit_manager.py", "monitor"], "log": "agents/exit_manager_stdout.log"},
    {"name": "derivatives_funding", "cmd": ["python3", "agents/derivatives_funding_agent.py"], "log": "agents/derivatives_funding_stdout.log"},
    {"name": "etrade_auth", "cmd": ["python3", "agents/etrade_auth_daemon.py"], "log": "agents/etrade_auth_daemon_stdout.log"},
    {"name": "kraken_signal", "cmd": ["python3", "agents/kraken_signal_agent.py"], "log": "agents/kraken_signal_stdout.log"},
    {"name": "equity_signal", "cmd": ["python3", "agents/equity_signal_agent.py"], "log": "agents/equity_signal_stdout.log"},
    {"name": "kraken_equity_signal", "cmd": ["python3", "agents/kraken_equity_signal_agent.py"], "log": "agents/kraken_equity_signal_stdout.log"},
    {"name": "nasdaqtrader_signal", "cmd": ["python3", "agents/nasdaqtrader_signal_agent.py"], "log": "agents/nasdaqtrader_signal_stdout.log"},
    {"name": "cross_venue_arb", "cmd": ["python3", "agents/cross_venue_arb_agent.py"], "log": "agents/cross_venue_arb_stdout.log"},
]

BASE = "/Users/scott/src/quant"

def launch_daemon(name, cmd, logfile):
    """Double-fork to fully detach from parent."""
    logpath = os.path.join(BASE, logfile)
    pid = os.fork()
    if pid > 0:
        # Parent - wait for first child
        os.waitpid(pid, 0)
        return
    # First child - fork again
    os.setsid()
    pid2 = os.fork()
    if pid2 > 0:
        os._exit(0)
    # Second child - this is the daemon
    os.chdir(BASE)
    with open(logpath, "a") as lf:
        proc = subprocess.Popen(
            cmd,
            stdout=lf,
            stderr=subprocess.STDOUT,
            cwd=BASE,
            start_new_session=True,
        )
        # Write PID file
        pidfile = os.path.join(BASE, "agents", f".{name}.pid")
        with open(pidfile, "w") as pf:
            pf.write(str(proc.pid))
        print(f"  {name}: PID {proc.pid}", flush=True)
    os._exit(0)

if __name__ == "__main__":
    # Kill existing agents first
    for a in AGENTS:
        name = a["name"]
        pidfile = os.path.join(BASE, "agents", f".{name}.pid")
        if os.path.exists(pidfile):
            try:
                old_pid = int(open(pidfile).read().strip())
                os.kill(old_pid, signal.SIGTERM)
                print(f"  Killed old {name} (PID {old_pid})")
            except (ProcessLookupError, ValueError):
                pass
            os.unlink(pidfile)

    time.sleep(1)
    print("Launching agents...")
    for a in AGENTS:
        launch_daemon(a["name"], a["cmd"], a["log"])

    time.sleep(3)
    # Verify
    print("\nVerification:")
    alive = 0
    for a in AGENTS:
        pidfile = os.path.join(BASE, "agents", f".{a['name']}.pid")
        if os.path.exists(pidfile):
            pid = int(open(pidfile).read().strip())
            try:
                os.kill(pid, 0)
                print(f"  {a['name']}: ALIVE (PID {pid})")
                alive += 1
            except ProcessLookupError:
                print(f"  {a['name']}: DEAD (PID {pid})")
        else:
            print(f"  {a['name']}: NO PID FILE")
    print(f"\n{alive}/{len(AGENTS)} agents running")
