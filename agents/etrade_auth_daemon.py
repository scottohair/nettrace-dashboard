#!/usr/bin/env python3
"""E*Trade OAuth auto-auth daemon — keeps tokens alive 24/7.

Architecture:
  - Singleton lock via fcntl.flock (prevents double-start)
  - Token refresh every 90 minutes (E*Trade tokens go inactive after 2h)
  - Midnight ET expiry detection and automatic re-authentication
  - Three re-auth strategies tried in order:
      1. Selenium headless (if ETRADE_USERNAME/ETRADE_PASSWORD in env)
      2. Browser + local HTTP server (paste verifier in web UI)
      3. Clipboard monitor (macOS notification + pbpaste polling)
  - Health status written to agents/etrade_token_status.json
  - Graceful shutdown on SIGTERM/SIGINT

Usage:
  python3 agents/etrade_auth_daemon.py          # Run as foreground daemon
  python3 agents/etrade_auth_daemon.py --once    # Single check and exit
  python3 agents/etrade_auth_daemon.py --status  # Print current status
"""

import fcntl
import http.server
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Ensure agents/ is importable
sys.path.insert(0, str(Path(__file__).parent))

from etrade_connector import ETradeAuth, TOKEN_FILE  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE = Path(__file__).parent
LOCK_FILE = BASE / ".etrade_auth_daemon.lock"
STATUS_FILE = BASE / "etrade_token_status.json"
LOG_FILE = BASE / "etrade_auth_daemon.log"

REFRESH_INTERVAL_SECONDS = 90 * 60       # 90 minutes between token refreshes
HEALTH_CHECK_INTERVAL_SECONDS = 5 * 60   # 5 minutes between health checks
MIDNIGHT_CHECK_MARGIN_SECONDS = 300      # Check 5 min before midnight ET
VERIFIER_HTTP_PORT = 8976
CLIPBOARD_POLL_INTERVAL = 2.0            # seconds between pbpaste polls
CLIPBOARD_TIMEOUT = 300.0                # 5 min to paste verifier
VERIFIER_CODE_LENGTH = 5                 # E*Trade verifier codes are 5 chars

ET_TIMEZONE = timezone(timedelta(hours=-5))  # US Eastern (approximate, ignores DST)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [etrade_auth] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(LOG_FILE)),
    ],
)
logger = logging.getLogger("etrade_auth")


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _today_et():
    """Return today's date string in Eastern Time."""
    return datetime.now(ET_TIMEZONE).strftime("%Y-%m-%d")


def _seconds_until_midnight_et():
    """Return seconds until midnight Eastern Time."""
    now_et = datetime.now(ET_TIMEZONE)
    midnight = now_et.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return max(0, (midnight - now_et).total_seconds())


def _load_token_data():
    """Load token file data, return dict or empty dict."""
    if not TOKEN_FILE.exists():
        return {}
    try:
        return json.loads(TOKEN_FILE.read_text())
    except Exception:
        return {}


def _is_token_expired_midnight():
    """Check if token has expired due to midnight ET rollover."""
    data = _load_token_data()
    saved_date = data.get("token_date", "")
    if not saved_date:
        return True
    today = _today_et()
    return saved_date != today


# ---------------------------------------------------------------------------
# Re-auth strategies
# ---------------------------------------------------------------------------

def _attempt_selenium_auth(auth):
    """Strategy 1: Headless Selenium login.

    Requires ETRADE_USERNAME and ETRADE_PASSWORD in environment.
    Uses headless Chrome to fill the E*Trade login page, extract the
    verifier code, and complete the OAuth flow.

    Returns:
        bool: True if authentication succeeded.
    """
    username = os.environ.get("ETRADE_USERNAME", "")
    password = os.environ.get("ETRADE_PASSWORD", "")
    if not username or not password:
        logger.info("Selenium auth: ETRADE_USERNAME/ETRADE_PASSWORD not set, skipping")
        return False

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        logger.info("Selenium auth: selenium not installed, skipping")
        return False

    authorize_url = None
    try:
        authorize_url = auth.get_request_token()
    except Exception as e:
        logger.error("Selenium auth: failed to get request token: %s", e)
        return False

    driver = None
    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(30)

        logger.info("Selenium auth: navigating to authorize URL")
        driver.get(authorize_url)

        # Wait for and fill login form
        wait = WebDriverWait(driver, 15)

        # E*Trade login page has username and password fields
        user_field = wait.until(EC.presence_of_element_located((By.NAME, "USER")))
        user_field.clear()
        user_field.send_keys(username)

        pass_field = driver.find_element(By.NAME, "PASSWORD")
        pass_field.clear()
        pass_field.send_keys(password)

        # Submit login
        submit = driver.find_element(By.ID, "logon_button")
        submit.click()

        # Wait for authorize page and click Accept
        accept_btn = wait.until(EC.element_to_be_clickable((By.NAME, "submit")))
        accept_btn.click()

        # Wait for verifier code to appear on the page
        time.sleep(3)
        page_text = driver.page_source

        # Extract verifier code -- E*Trade shows it as a short alphanumeric code
        import re
        # Look for a standalone 5-character alphanumeric code
        matches = re.findall(r'\b([A-Z0-9]{5})\b', page_text)
        verifier = None
        for m in matches:
            # Filter out common HTML/CSS tokens
            if m not in ("HTTPS", "TRADE", "OAUTH", "ERROR", "INPUT", "CLICK"):
                verifier = m
                break

        if not verifier:
            logger.error("Selenium auth: could not extract verifier code from page")
            return False

        logger.info("Selenium auth: extracted verifier code: %s", verifier)
        auth.get_access_token(verifier)
        logger.info("Selenium auth: authentication successful")
        return True

    except Exception as e:
        logger.error("Selenium auth failed: %s", e)
        return False
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def _attempt_browser_auth(auth):
    """Strategy 2: Open browser + local HTTP server for verifier paste.

    Opens the authorization URL in the default browser and starts a
    local HTTP server on localhost:8976 with a simple HTML form where
    the user can paste the verifier code.

    Returns:
        bool: True if authentication succeeded.
    """
    authorize_url = None
    try:
        authorize_url = auth.get_request_token()
    except Exception as e:
        logger.error("Browser auth: failed to get request token: %s", e)
        return False

    verifier_result = {"code": None}
    server_ready = threading.Event()
    server_done = threading.Event()

    class VerifierHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/" or self.path.startswith("/?"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                html = """<!DOCTYPE html>
<html><head><title>E*Trade Auth</title>
<style>
body { font-family: -apple-system, sans-serif; max-width: 500px; margin: 60px auto; padding: 20px; }
input[type=text] { font-size: 24px; padding: 10px; width: 200px; text-align: center; letter-spacing: 4px; }
button { font-size: 18px; padding: 10px 30px; margin-top: 10px; cursor: pointer; }
.done { color: green; font-size: 20px; font-weight: bold; }
</style></head><body>
<h2>E*Trade Verifier Code</h2>
<p>Paste the 5-character verifier code from E*Trade:</p>
<form action="/verify" method="GET">
<input type="text" name="code" maxlength="10" autofocus required>
<br><button type="submit">Submit</button>
</form></body></html>"""
                self.wfile.write(html.encode())
            else:
                self.send_response(404)
                self.end_headers()

            if self.path.startswith("/verify"):
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(self.path).query)
                code = qs.get("code", [""])[0].strip()
                if code:
                    verifier_result["code"] = code

                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                if code:
                    html = """<!DOCTYPE html>
<html><head><title>E*Trade Auth</title>
<style>body { font-family: -apple-system, sans-serif; max-width: 500px; margin: 60px auto; padding: 20px; }
.done { color: green; font-size: 20px; font-weight: bold; }</style></head><body>
<p class="done">Verifier received. You can close this tab.</p>
</body></html>"""
                else:
                    html = """<!DOCTYPE html>
<html><head><title>E*Trade Auth</title></head><body>
<p>No code received. <a href="/">Try again</a></p>
</body></html>"""
                self.wfile.write(html.encode())
                if code:
                    server_done.set()

        def log_message(self, format, *args):
            # Suppress default HTTP logging
            pass

    server = None
    try:
        server = http.server.HTTPServer(("127.0.0.1", VERIFIER_HTTP_PORT), VerifierHandler)
        server.timeout = 1.0

        def run_server():
            server_ready.set()
            while not server_done.is_set():
                server.handle_request()

        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        server_ready.wait(timeout=5.0)

        # Open browser with authorize URL
        logger.info("Browser auth: opening authorize URL in browser")
        webbrowser.open(authorize_url)
        logger.info("Browser auth: local verifier server at http://127.0.0.1:%d", VERIFIER_HTTP_PORT)

        # Wait for verifier (up to 5 minutes)
        server_done.wait(timeout=CLIPBOARD_TIMEOUT)

        if not verifier_result["code"]:
            logger.warning("Browser auth: timed out waiting for verifier code")
            return False

        logger.info("Browser auth: received verifier code")
        auth.get_access_token(verifier_result["code"])
        logger.info("Browser auth: authentication successful")
        return True

    except OSError as e:
        logger.error("Browser auth: could not start HTTP server on port %d: %s",
                      VERIFIER_HTTP_PORT, e)
        return False
    except Exception as e:
        logger.error("Browser auth failed: %s", e)
        return False
    finally:
        if server:
            try:
                server.server_close()
            except Exception:
                pass


def _attempt_clipboard_auth(auth):
    """Strategy 3: macOS clipboard monitor.

    Shows a macOS notification, then polls pbpaste every 2s for a
    5-character verifier code. User copies the code from their browser
    and the daemon picks it up automatically.

    Returns:
        bool: True if authentication succeeded.
    """
    # Only works on macOS
    if sys.platform != "darwin":
        logger.info("Clipboard auth: not macOS, skipping")
        return False

    authorize_url = None
    try:
        authorize_url = auth.get_request_token()
    except Exception as e:
        logger.error("Clipboard auth: failed to get request token: %s", e)
        return False

    # Open URL in browser
    webbrowser.open(authorize_url)

    # Show macOS notification
    try:
        subprocess.run(
            [
                "osascript", "-e",
                'display notification "Authorize E*Trade in your browser, then copy the verifier code." '
                'with title "E*Trade Auth" sound name "Glass"',
            ],
            timeout=5,
            capture_output=True,
        )
    except Exception:
        pass

    logger.info("Clipboard auth: polling clipboard for verifier code (timeout %.0fs)",
                CLIPBOARD_TIMEOUT)

    # Get current clipboard to ignore it
    try:
        initial_clip = subprocess.run(
            ["pbpaste"], capture_output=True, text=True, timeout=3
        ).stdout.strip()
    except Exception:
        initial_clip = ""

    start = time.time()
    while time.time() - start < CLIPBOARD_TIMEOUT:
        time.sleep(CLIPBOARD_POLL_INTERVAL)
        try:
            clip = subprocess.run(
                ["pbpaste"], capture_output=True, text=True, timeout=3
            ).stdout.strip()
        except Exception:
            continue

        # Look for a new clipboard entry that looks like a verifier code
        if clip and clip != initial_clip and len(clip) <= 10:
            # E*Trade verifier codes are typically 5 alphanumeric chars
            clean = clip.strip()
            if len(clean) == VERIFIER_CODE_LENGTH and clean.isalnum():
                logger.info("Clipboard auth: detected verifier code from clipboard")
                try:
                    auth.get_access_token(clean)
                    logger.info("Clipboard auth: authentication successful")
                    return True
                except Exception as e:
                    logger.error("Clipboard auth: verifier rejected: %s", e)
                    # Reset initial_clip so we don't try the same code again
                    initial_clip = clip
                    continue

    logger.warning("Clipboard auth: timed out waiting for verifier in clipboard")
    return False


# ---------------------------------------------------------------------------
# Auth daemon
# ---------------------------------------------------------------------------

class ETradeAuthDaemon:
    """Background daemon that keeps E*Trade OAuth tokens alive 24/7."""

    def __init__(self):
        self.running = True
        self._lock_fh = None
        self.auth = ETradeAuth()
        self.last_refresh = 0.0
        self.last_health_check = 0.0
        self.last_auth_method = "none"
        self.errors = []

    # ------------------------------------------------------------------
    # Singleton lock
    # ------------------------------------------------------------------

    def acquire_lock(self):
        """Acquire singleton file lock. Returns True on success."""
        try:
            LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
            fh = open(LOCK_FILE, "a+")
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                fh.seek(0)
                holder = fh.read().strip()
                logger.error("Another etrade_auth_daemon instance is running (lock holder: %s)",
                             holder or "unknown")
                fh.close()
                return False
            fh.seek(0)
            fh.truncate()
            fh.write(json.dumps({"pid": os.getpid(), "started_at": _utc_now()}))
            fh.flush()
            self._lock_fh = fh
            return True
        except Exception as e:
            logger.error("Failed to acquire singleton lock: %s", e)
            return False

    def release_lock(self):
        """Release the singleton file lock."""
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

    # ------------------------------------------------------------------
    # Health status
    # ------------------------------------------------------------------

    def write_status(self, extra=None):
        """Write health status to JSON file."""
        now = time.time()
        next_refresh = self.last_refresh + REFRESH_INTERVAL_SECONDS if self.last_refresh else now
        seconds_to_midnight = _seconds_until_midnight_et()

        status = {
            "token_valid": self.auth.is_authenticated,
            "last_refresh": datetime.fromtimestamp(self.last_refresh, tz=timezone.utc).isoformat()
            if self.last_refresh else None,
            "next_refresh": datetime.fromtimestamp(next_refresh, tz=timezone.utc).isoformat()
            if self.last_refresh else None,
            "auth_method": self.last_auth_method,
            "errors": self.errors[-10:],  # Keep last 10 errors
            "pid": os.getpid(),
            "running": self.running,
            "updated_at": _utc_now(),
            "token_date": _load_token_data().get("token_date", ""),
            "today_et": _today_et(),
            "seconds_to_midnight_et": int(seconds_to_midnight),
            "sandbox": self.auth.sandbox,
        }
        if extra:
            status.update(extra)

        try:
            STATUS_FILE.write_text(json.dumps(status, indent=2))
        except Exception as e:
            logger.error("Failed to write status file: %s", e)

    # ------------------------------------------------------------------
    # Re-authentication (three strategies in order)
    # ------------------------------------------------------------------

    def perform_reauth(self):
        """Try re-authentication strategies in order.

        1. Selenium headless (if credentials available)
        2. Browser + local HTTP server
        3. Clipboard monitor (macOS only)

        Returns:
            bool: True if any strategy succeeded.
        """
        strategies = [
            ("selenium", _attempt_selenium_auth),
            ("browser_http", _attempt_browser_auth),
            ("clipboard", _attempt_clipboard_auth),
        ]

        for name, strategy_fn in strategies:
            logger.info("Trying re-auth strategy: %s", name)
            try:
                # Create a fresh auth instance for each attempt
                auth = ETradeAuth(sandbox=self.auth.sandbox)
                success = strategy_fn(auth)
                if success:
                    # Reload tokens into our auth instance
                    self.auth = ETradeAuth(sandbox=self.auth.sandbox)
                    self.last_auth_method = name
                    self.last_refresh = time.time()
                    logger.info("Re-auth succeeded via %s", name)
                    return True
            except Exception as e:
                error_msg = f"{name} failed: {e}"
                logger.error("Re-auth strategy %s raised exception: %s", name, e)
                self.errors.append({"time": _utc_now(), "error": error_msg})

        logger.error("All re-auth strategies failed")
        return False

    # ------------------------------------------------------------------
    # Token refresh
    # ------------------------------------------------------------------

    def refresh_if_needed(self):
        """Refresh the token if close to 2h inactivity timeout.

        Returns:
            bool: True if refresh succeeded or was not needed.
        """
        if not self.auth.is_authenticated:
            return False

        now = time.time()
        elapsed = now - self.last_refresh if self.last_refresh else REFRESH_INTERVAL_SECONDS + 1

        if elapsed < REFRESH_INTERVAL_SECONDS:
            return True  # Not yet time to refresh

        logger.info("Refreshing E*Trade token (%.0f min since last refresh)",
                     elapsed / 60.0)
        try:
            success = self.auth.refresh_token()
            if success:
                self.last_refresh = time.time()
                self.last_auth_method = "refresh"
                logger.info("Token refresh successful")
                return True
            else:
                error_msg = "refresh_token returned False"
                logger.warning("Token refresh failed: %s", error_msg)
                self.errors.append({"time": _utc_now(), "error": error_msg})
                return False
        except Exception as e:
            error_msg = f"refresh exception: {e}"
            logger.error("Token refresh exception: %s", e)
            self.errors.append({"time": _utc_now(), "error": error_msg})
            return False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def check_and_maintain(self):
        """Single iteration of the maintenance loop.

        Returns:
            str: Action taken ("healthy", "refreshed", "reauthed", "failed")
        """
        # 1. Check midnight expiry
        if _is_token_expired_midnight():
            logger.info("Token expired (midnight ET rollover) -- re-authenticating")
            if self.perform_reauth():
                self.write_status()
                return "reauthed"
            else:
                self.write_status()
                return "failed"

        # 2. Check if token is authenticated at all
        if not self.auth.is_authenticated:
            logger.info("No valid token -- attempting authentication")
            if self.perform_reauth():
                self.write_status()
                return "reauthed"
            else:
                self.write_status()
                return "failed"

        # 3. Refresh if near 2h inactivity
        refresh_ok = self.refresh_if_needed()
        if not refresh_ok:
            # Token refresh failed -- might be expired, try re-auth
            logger.warning("Token refresh failed -- attempting full re-auth")
            if self.perform_reauth():
                self.write_status()
                return "reauthed"
            else:
                self.write_status()
                return "failed"

        self.write_status()
        return "healthy" if self.last_refresh else "refreshed"

    def run_once(self):
        """Run a single check and exit."""
        result = self.check_and_maintain()
        logger.info("Single check result: %s", result)
        return result

    def run_forever(self):
        """Main daemon loop. Runs until SIGTERM/SIGINT."""
        if not self.acquire_lock():
            logger.error("Failed to acquire singleton lock; exiting")
            return

        # Signal handlers
        def _stop(signum, _frame):
            logger.info("Received signal %d; shutting down...", signum)
            self.running = False

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

        logger.info("E*Trade auth daemon started (PID %d)", os.getpid())
        logger.info("Refresh interval: %d min, health check interval: %d min",
                     REFRESH_INTERVAL_SECONDS // 60, HEALTH_CHECK_INTERVAL_SECONDS // 60)

        # Initial check
        try:
            result = self.check_and_maintain()
            logger.info("Initial check: %s", result)
        except Exception as e:
            logger.error("Initial check failed: %s", e, exc_info=True)

        try:
            while self.running:
                # Sleep in 1-second increments for responsive shutdown
                sleep_target = time.time() + HEALTH_CHECK_INTERVAL_SECONDS
                while self.running and time.time() < sleep_target:
                    time.sleep(1)

                if not self.running:
                    break

                try:
                    result = self.check_and_maintain()
                    logger.info("Check result: %s", result)
                except Exception as e:
                    logger.error("Check failed: %s", e, exc_info=True)
                    self.errors.append({"time": _utc_now(), "error": str(e)})
                    self.write_status()

        finally:
            self.running = False
            self.write_status(extra={"stopped_at": _utc_now()})
            self.release_lock()
            logger.info("E*Trade auth daemon stopped")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def print_status():
    """Print current daemon status."""
    if not STATUS_FILE.exists():
        print("No status file found. Daemon may not have run yet.")
        return
    data = json.loads(STATUS_FILE.read_text())
    print(json.dumps(data, indent=2))


def main():
    import argparse
    parser = argparse.ArgumentParser(description="E*Trade OAuth auto-auth daemon")
    parser.add_argument("--once", action="store_true",
                        help="Run single check and exit")
    parser.add_argument("--status", action="store_true",
                        help="Print current status JSON")
    parser.add_argument("--sandbox", action="store_true",
                        help="Use E*Trade sandbox environment")
    args = parser.parse_args()

    if args.status:
        print_status()
        return

    daemon = ETradeAuthDaemon()
    if args.sandbox:
        daemon.auth = ETradeAuth(sandbox=True)

    if args.once:
        result = daemon.run_once()
        print(f"Result: {result}")
        return

    daemon.run_forever()


if __name__ == "__main__":
    main()
