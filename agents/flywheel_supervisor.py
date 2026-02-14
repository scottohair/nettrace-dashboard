#!/usr/bin/env python3
"""Supervise flywheel_controller as a single long-running service.

Commands:
  python flywheel_supervisor.py run
  python flywheel_supervisor.py start
  python flywheel_supervisor.py stop
  python flywheel_supervisor.py restart
  python flywheel_supervisor.py status
"""

import argparse
import fcntl
import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent
PID_FILE = BASE / "flywheel_supervisor.pid"
LOCK_FILE = BASE / "flywheel_supervisor.lock"
STATUS_FILE = BASE / "flywheel_supervisor_status.json"
LOG_FILE = BASE / "flywheel_supervisor.log"
STDOUT_LOG_FILE = BASE / "flywheel_supervisor_stdout.log"
CHILD_SCRIPT = BASE / "flywheel_controller.py"
CHILD_STDOUT_LOG = BASE / "flywheel_controller_stdout.log"

RESTART_BASE_SECONDS = int(os.environ.get("FLYWHEEL_SUP_RESTART_BASE_SECONDS", "3"))
RESTART_MAX_SECONDS = int(os.environ.get("FLYWHEEL_SUP_RESTART_MAX_SECONDS", "60"))
MAX_RESTARTS_PER_HOUR = int(os.environ.get("FLYWHEEL_SUP_MAX_RESTARTS_PER_HOUR", "30"))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [flywheel_supervisor] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(LOG_FILE)),
    ],
)
logger = logging.getLogger("flywheel_supervisor")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _pid_is_alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _load_json(path, default):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except Exception:
        return default


class FlywheelSupervisor:
    def __init__(self):
        self.running = True
        self.child = None
        self.restarts_total = 0
        self.restart_timestamps = []
        self.last_exit_code = None
        self._lock_fh = None

    def _acquire_lock(self):
        LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        fh = open(LOCK_FILE, "a+")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.seek(0)
            holder = fh.read().strip()
            logger.error("Another flywheel_supervisor appears active (lock holder: %s)", holder or "unknown")
            fh.close()
            return False
        fh.seek(0)
        fh.truncate()
        fh.write(json.dumps({"pid": os.getpid(), "started_at": _now_iso()}))
        fh.flush()
        self._lock_fh = fh
        return True

    def _release_lock(self):
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

    def _write_pid(self):
        PID_FILE.write_text(str(os.getpid()))

    def _remove_pid(self):
        try:
            PID_FILE.unlink()
        except FileNotFoundError:
            pass

    def _write_status(self, reason=""):
        child_pid = None
        child_running = False
        if self.child is not None:
            child_pid = int(self.child.pid)
            child_running = self.child.poll() is None
        payload = {
            "updated_at": _now_iso(),
            "running": bool(self.running),
            "pid": int(os.getpid()),
            "child_pid": child_pid,
            "child_running": bool(child_running),
            "restarts_total": int(self.restarts_total),
            "restart_window_count": int(len(self.restart_timestamps)),
            "max_restarts_per_hour": int(MAX_RESTARTS_PER_HOUR),
            "last_exit_code": self.last_exit_code,
            "reason": str(reason or ""),
        }
        try:
            STATUS_FILE.write_text(json.dumps(payload, indent=2))
        except Exception:
            pass

    def _start_child(self):
        if not CHILD_SCRIPT.exists():
            raise FileNotFoundError(f"Missing child script: {CHILD_SCRIPT}")
        child_env = os.environ.copy()
        child_env["FLYWHEEL_SINGLETON_ENFORCE"] = "1"
        child_env.setdefault("PYTHONUNBUFFERED", "1")
        out = open(CHILD_STDOUT_LOG, "a")
        self.child = subprocess.Popen(
            [sys.executable, str(CHILD_SCRIPT)],
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
            cwd=str(BASE),
            start_new_session=True,
            env=child_env,
        )
        out.close()
        logger.info("Started flywheel child PID %d", self.child.pid)
        self._write_status("child_started")

    def _stop_child(self, reason="supervisor_stop"):
        proc = self.child
        if proc is None:
            return
        if proc.poll() is None:
            logger.info("Stopping flywheel child PID %d (%s)", proc.pid, reason)
            proc.terminate()
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        self.last_exit_code = proc.returncode
        self.child = None
        self._write_status("child_stopped")

    def _trim_restart_window(self):
        now = time.time()
        self.restart_timestamps = [t for t in self.restart_timestamps if now - t < 3600]

    def run(self):
        if not self._acquire_lock():
            return 1

        self._write_pid()
        self._write_status("supervisor_started")

        def _stop(signum, _frame):
            logger.info("Received signal %d, stopping flywheel supervisor", signum)
            self.running = False

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

        try:
            while self.running:
                if self.child is None:
                    self._trim_restart_window()
                    if len(self.restart_timestamps) >= int(MAX_RESTARTS_PER_HOUR):
                        self._write_status("restart_limit_cooldown")
                        logger.error(
                            "Restart limit reached (%d/h), cooling down for 300s",
                            MAX_RESTARTS_PER_HOUR,
                        )
                        for _ in range(300):
                            if not self.running:
                                break
                            time.sleep(1)
                        continue
                    self._start_child()

                if self.child is not None and self.child.poll() is not None:
                    self.last_exit_code = int(self.child.returncode or 0)
                    self.restarts_total += 1
                    self.restart_timestamps.append(time.time())
                    delay = min(
                        int(RESTART_MAX_SECONDS),
                        int(RESTART_BASE_SECONDS) * (2 ** min(5, self.restarts_total - 1)),
                    )
                    logger.warning(
                        "Flywheel child exited rc=%s; restart #%d in %ss",
                        self.last_exit_code,
                        self.restarts_total,
                        delay,
                    )
                    self.child = None
                    self._write_status("child_exited")
                    for _ in range(max(1, delay)):
                        if not self.running:
                            break
                        time.sleep(1)
                    continue

                time.sleep(1)
        finally:
            self._stop_child("supervisor_shutdown")
            self._remove_pid()
            self._release_lock()
            self._write_status("supervisor_stopped")
            logger.info("Flywheel supervisor stopped")
        return 0


def _start_background():
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            if _pid_is_alive(pid):
                print(f"flywheel_supervisor already running (pid {pid})")
                return 0
        except Exception:
            pass

    out = open(STDOUT_LOG_FILE, "a")
    proc = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "run"],
        stdin=subprocess.DEVNULL,
        stdout=out,
        stderr=subprocess.STDOUT,
        cwd=str(BASE),
        start_new_session=True,
        env=os.environ.copy(),
    )
    out.close()
    print(f"started flywheel_supervisor launcher pid={proc.pid}")
    return 0


def _stop_background():
    if not PID_FILE.exists():
        print("flywheel_supervisor is not running (no PID file)")
        return 0
    try:
        pid = int(PID_FILE.read_text().strip())
    except Exception:
        print("invalid supervisor PID file, removing")
        try:
            PID_FILE.unlink()
        except Exception:
            pass
        return 0

    if not _pid_is_alive(pid):
        print(f"flywheel_supervisor pid {pid} is stale, cleaning up")
        try:
            PID_FILE.unlink()
        except Exception:
            pass
        return 0

    os.kill(pid, signal.SIGTERM)
    print(f"sent SIGTERM to flywheel_supervisor pid {pid}")
    return 0


def _status():
    pid = None
    alive = False
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            alive = _pid_is_alive(pid)
        except Exception:
            pid = None
            alive = False
    payload = _load_json(STATUS_FILE, {})
    report = {
        "pid_file_present": PID_FILE.exists(),
        "pid": pid,
        "alive": bool(alive),
        "status": payload if isinstance(payload, dict) else {},
    }
    print(json.dumps(report, indent=2))
    return 0


def main():
    parser = argparse.ArgumentParser(description="Supervise flywheel_controller as a singleton daemon")
    parser.add_argument("command", nargs="?", default="status", choices=["run", "start", "stop", "restart", "status"])
    args = parser.parse_args()

    if args.command == "run":
        return FlywheelSupervisor().run()
    if args.command == "start":
        return _start_background()
    if args.command == "stop":
        return _stop_background()
    if args.command == "restart":
        _stop_background()
        time.sleep(1)
        return _start_background()
    return _status()


if __name__ == "__main__":
    raise SystemExit(main())
