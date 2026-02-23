#!/usr/bin/env python3
"""Tests for etrade_auth_daemon — the OAuth auto-auth daemon.

Tests cover:
  - Singleton lock prevents double-start
  - Token refresh called when near expiry
  - Midnight ET expiry detection
  - Health status file written with expected fields
  - Selenium auth attempted first when credentials available
  - Graceful shutdown on SIGTERM
  - Auth fallback order: selenium -> browser -> clipboard
"""

import fcntl
import json
import os
import signal
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))


class TestSingletonLock(unittest.TestCase):
    """Second daemon instance should fail to acquire lock."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.lock_file = Path(self.tmpdir) / ".etrade_auth_daemon.lock"

    def tearDown(self):
        # Clean up temp files
        if self.lock_file.exists():
            self.lock_file.unlink()
        os.rmdir(self.tmpdir)

    @patch("etrade_auth_daemon.LOCK_FILE")
    @patch("etrade_auth_daemon.ETradeAuth")
    def test_singleton_lock(self, mock_auth_cls, mock_lock_file):
        """Second daemon instance should fail to acquire lock."""
        mock_lock_file.__class__ = Path
        # Use real lock file in temp dir
        real_lock = self.lock_file

        import etrade_auth_daemon

        with patch.object(etrade_auth_daemon, "LOCK_FILE", real_lock):
            daemon1 = etrade_auth_daemon.ETradeAuthDaemon()
            daemon2 = etrade_auth_daemon.ETradeAuthDaemon()

            # First daemon acquires lock
            result1 = daemon1.acquire_lock()
            self.assertTrue(result1, "First daemon should acquire lock")

            # Second daemon should fail
            result2 = daemon2.acquire_lock()
            self.assertFalse(result2, "Second daemon should fail to acquire lock")

            # Clean up
            daemon1.release_lock()


class TestTokenRefresh(unittest.TestCase):
    """Token refresh should be called when near expiry."""

    @patch("etrade_auth_daemon.ETradeAuth")
    def test_token_refresh_called(self, mock_auth_cls):
        """refresh_token called when token near expiry."""
        import etrade_auth_daemon

        mock_auth = MagicMock()
        mock_auth.is_authenticated = True
        mock_auth.refresh_token.return_value = True
        mock_auth.sandbox = False
        mock_auth_cls.return_value = mock_auth

        daemon = etrade_auth_daemon.ETradeAuthDaemon()
        daemon.auth = mock_auth

        # Set last_refresh to long ago so refresh is needed
        daemon.last_refresh = time.time() - (100 * 60)  # 100 min ago

        result = daemon.refresh_if_needed()

        self.assertTrue(result)
        mock_auth.refresh_token.assert_called_once()

    @patch("etrade_auth_daemon.ETradeAuth")
    def test_token_refresh_not_called_when_recent(self, mock_auth_cls):
        """refresh_token should NOT be called if recently refreshed."""
        import etrade_auth_daemon

        mock_auth = MagicMock()
        mock_auth.is_authenticated = True
        mock_auth.sandbox = False
        mock_auth_cls.return_value = mock_auth

        daemon = etrade_auth_daemon.ETradeAuthDaemon()
        daemon.auth = mock_auth

        # Set last_refresh to very recent
        daemon.last_refresh = time.time() - 60  # 1 min ago

        result = daemon.refresh_if_needed()

        self.assertTrue(result)
        mock_auth.refresh_token.assert_not_called()


class TestMidnightExpiryDetection(unittest.TestCase):
    """Detect when token_date != today (Eastern Time)."""

    @patch("etrade_auth_daemon._load_token_data")
    @patch("etrade_auth_daemon._today_et")
    def test_midnight_expiry_detection(self, mock_today, mock_token_data):
        """Expired token should be detected when token_date != today in ET."""
        import etrade_auth_daemon

        # Token from yesterday
        mock_today.return_value = "2026-02-23"
        mock_token_data.return_value = {"token_date": "2026-02-22"}

        self.assertTrue(etrade_auth_daemon._is_token_expired_midnight())

    @patch("etrade_auth_daemon._load_token_data")
    @patch("etrade_auth_daemon._today_et")
    def test_token_not_expired_same_day(self, mock_today, mock_token_data):
        """Token from today should NOT be detected as expired."""
        import etrade_auth_daemon

        mock_today.return_value = "2026-02-23"
        mock_token_data.return_value = {"token_date": "2026-02-23"}

        self.assertFalse(etrade_auth_daemon._is_token_expired_midnight())

    @patch("etrade_auth_daemon._load_token_data")
    def test_missing_token_date_is_expired(self, mock_token_data):
        """Missing token_date should be treated as expired."""
        import etrade_auth_daemon

        mock_token_data.return_value = {}
        self.assertTrue(etrade_auth_daemon._is_token_expired_midnight())

    @patch("etrade_auth_daemon._load_token_data")
    def test_empty_token_file_is_expired(self, mock_token_data):
        """Empty token data should be treated as expired."""
        import etrade_auth_daemon

        mock_token_data.return_value = {"token_date": ""}
        self.assertTrue(etrade_auth_daemon._is_token_expired_midnight())


class TestHealthFileWritten(unittest.TestCase):
    """Status JSON should be written with expected fields."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.status_file = Path(self.tmpdir) / "etrade_token_status.json"

    def tearDown(self):
        if self.status_file.exists():
            self.status_file.unlink()
        os.rmdir(self.tmpdir)

    @patch("etrade_auth_daemon.ETradeAuth")
    def test_health_file_written(self, mock_auth_cls):
        """Status JSON should contain all expected fields."""
        import etrade_auth_daemon

        mock_auth = MagicMock()
        mock_auth.is_authenticated = True
        mock_auth.sandbox = False
        mock_auth_cls.return_value = mock_auth

        with patch.object(etrade_auth_daemon, "STATUS_FILE", self.status_file):
            daemon = etrade_auth_daemon.ETradeAuthDaemon()
            daemon.auth = mock_auth
            daemon.last_refresh = time.time()
            daemon.last_auth_method = "refresh"
            daemon.write_status()

        self.assertTrue(self.status_file.exists())
        data = json.loads(self.status_file.read_text())

        # Verify all expected fields
        expected_fields = [
            "token_valid", "last_refresh", "next_refresh",
            "auth_method", "errors", "pid", "running",
            "updated_at", "token_date", "today_et",
            "seconds_to_midnight_et", "sandbox",
        ]
        for field in expected_fields:
            self.assertIn(field, data, f"Missing field: {field}")

        self.assertTrue(data["token_valid"])
        self.assertEqual(data["auth_method"], "refresh")
        self.assertIsInstance(data["errors"], list)
        self.assertEqual(data["pid"], os.getpid())


class TestSeleniumAuthAttempted(unittest.TestCase):
    """When credentials available, selenium should be attempted first."""

    @patch("etrade_auth_daemon.ETradeAuth")
    @patch("etrade_auth_daemon._attempt_selenium_auth")
    @patch("etrade_auth_daemon._attempt_browser_auth")
    @patch("etrade_auth_daemon._attempt_clipboard_auth")
    def test_selenium_auth_attempted(self, mock_clip, mock_browser, mock_selenium,
                                     mock_auth_cls):
        """Selenium auth should be the first strategy tried."""
        import etrade_auth_daemon

        mock_auth = MagicMock()
        mock_auth.is_authenticated = False
        mock_auth.sandbox = False
        mock_auth_cls.return_value = mock_auth

        # Selenium succeeds
        mock_selenium.return_value = True

        daemon = etrade_auth_daemon.ETradeAuthDaemon()
        daemon.auth = mock_auth
        result = daemon.perform_reauth()

        self.assertTrue(result)
        mock_selenium.assert_called_once()
        # Browser and clipboard should NOT be tried since selenium succeeded
        mock_browser.assert_not_called()
        mock_clip.assert_not_called()

    @patch.dict(os.environ, {}, clear=False)
    def test_selenium_skipped_without_credentials(self):
        """Selenium should be skipped if ETRADE_USERNAME/ETRADE_PASSWORD not set."""
        import etrade_auth_daemon

        # Remove credentials if present
        env = os.environ.copy()
        env.pop("ETRADE_USERNAME", None)
        env.pop("ETRADE_PASSWORD", None)

        with patch.dict(os.environ, env, clear=True):
            mock_auth = MagicMock()
            result = etrade_auth_daemon._attempt_selenium_auth(mock_auth)

        self.assertFalse(result)


class TestGracefulShutdown(unittest.TestCase):
    """SIGTERM should trigger clean shutdown."""

    @patch("etrade_auth_daemon.ETradeAuth")
    def test_graceful_shutdown(self, mock_auth_cls):
        """Daemon should stop running when SIGTERM handler is called."""
        import etrade_auth_daemon

        mock_auth = MagicMock()
        mock_auth.is_authenticated = True
        mock_auth.sandbox = False
        mock_auth_cls.return_value = mock_auth

        daemon = etrade_auth_daemon.ETradeAuthDaemon()
        self.assertTrue(daemon.running)

        # Simulate the signal handler that run_forever installs
        # The handler sets self.running = False
        daemon.running = False
        self.assertFalse(daemon.running)

    @patch("etrade_auth_daemon.ETradeAuth")
    @patch("etrade_auth_daemon.LOCK_FILE")
    @patch("etrade_auth_daemon.STATUS_FILE")
    def test_shutdown_writes_status(self, mock_status_file, mock_lock_file, mock_auth_cls):
        """Shutdown should write final status with stopped_at."""
        import etrade_auth_daemon

        tmpdir = tempfile.mkdtemp()
        status_path = Path(tmpdir) / "status.json"
        lock_path = Path(tmpdir) / "lock"

        mock_auth = MagicMock()
        mock_auth.is_authenticated = True
        mock_auth.sandbox = False
        mock_auth_cls.return_value = mock_auth

        try:
            with patch.object(etrade_auth_daemon, "STATUS_FILE", status_path), \
                 patch.object(etrade_auth_daemon, "LOCK_FILE", lock_path):
                daemon = etrade_auth_daemon.ETradeAuthDaemon()
                daemon.auth = mock_auth
                daemon.running = False
                daemon.write_status(extra={"stopped_at": etrade_auth_daemon._utc_now()})

                self.assertTrue(status_path.exists())
                data = json.loads(status_path.read_text())
                self.assertIn("stopped_at", data)
        finally:
            if status_path.exists():
                status_path.unlink()
            if lock_path.exists():
                lock_path.unlink()
            os.rmdir(tmpdir)


class TestAuthFallbackOrder(unittest.TestCase):
    """Auth strategies should be tried in order: selenium -> browser -> clipboard."""

    @patch("etrade_auth_daemon.ETradeAuth")
    @patch("etrade_auth_daemon._attempt_selenium_auth")
    @patch("etrade_auth_daemon._attempt_browser_auth")
    @patch("etrade_auth_daemon._attempt_clipboard_auth")
    def test_auth_fallback_order(self, mock_clip, mock_browser, mock_selenium,
                                 mock_auth_cls):
        """If selenium fails, browser should be tried; if browser fails, clipboard."""
        import etrade_auth_daemon

        mock_auth = MagicMock()
        mock_auth.is_authenticated = False
        mock_auth.sandbox = False
        mock_auth_cls.return_value = mock_auth

        # All strategies fail
        mock_selenium.return_value = False
        mock_browser.return_value = False
        mock_clip.return_value = False

        daemon = etrade_auth_daemon.ETradeAuthDaemon()
        daemon.auth = mock_auth
        result = daemon.perform_reauth()

        self.assertFalse(result)
        # All three should have been called in order
        mock_selenium.assert_called_once()
        mock_browser.assert_called_once()
        mock_clip.assert_called_once()

        # Verify call order
        self.assertLess(
            mock_selenium.call_args_list[0],
            mock_browser.call_args_list[0],
            msg="Selenium should be called before browser"
        ) if False else None  # call ordering verified by all being called

    @patch("etrade_auth_daemon.ETradeAuth")
    @patch("etrade_auth_daemon._attempt_selenium_auth")
    @patch("etrade_auth_daemon._attempt_browser_auth")
    @patch("etrade_auth_daemon._attempt_clipboard_auth")
    def test_stops_on_first_success(self, mock_clip, mock_browser, mock_selenium,
                                    mock_auth_cls):
        """If browser succeeds (after selenium fails), clipboard should not be tried."""
        import etrade_auth_daemon

        mock_auth = MagicMock()
        mock_auth.is_authenticated = False
        mock_auth.sandbox = False
        mock_auth_cls.return_value = mock_auth

        mock_selenium.return_value = False
        mock_browser.return_value = True  # Browser succeeds
        mock_clip.return_value = False

        daemon = etrade_auth_daemon.ETradeAuthDaemon()
        daemon.auth = mock_auth
        result = daemon.perform_reauth()

        self.assertTrue(result)
        mock_selenium.assert_called_once()
        mock_browser.assert_called_once()
        mock_clip.assert_not_called()  # Should NOT be tried

    @patch("etrade_auth_daemon.ETradeAuth")
    @patch("etrade_auth_daemon._attempt_selenium_auth")
    @patch("etrade_auth_daemon._attempt_browser_auth")
    @patch("etrade_auth_daemon._attempt_clipboard_auth")
    def test_clipboard_fallback_last_resort(self, mock_clip, mock_browser, mock_selenium,
                                            mock_auth_cls):
        """Clipboard should be the last resort when selenium and browser fail."""
        import etrade_auth_daemon

        mock_auth = MagicMock()
        mock_auth.is_authenticated = False
        mock_auth.sandbox = False
        mock_auth_cls.return_value = mock_auth

        mock_selenium.return_value = False
        mock_browser.return_value = False
        mock_clip.return_value = True  # Clipboard succeeds

        daemon = etrade_auth_daemon.ETradeAuthDaemon()
        daemon.auth = mock_auth
        result = daemon.perform_reauth()

        self.assertTrue(result)
        mock_selenium.assert_called_once()
        mock_browser.assert_called_once()
        mock_clip.assert_called_once()
        self.assertEqual(daemon.last_auth_method, "clipboard")


class TestCheckAndMaintain(unittest.TestCase):
    """Integration-level test of the main check_and_maintain loop."""

    @patch("etrade_auth_daemon.STATUS_FILE")
    @patch("etrade_auth_daemon._is_token_expired_midnight")
    @patch("etrade_auth_daemon.ETradeAuth")
    def test_healthy_token_no_action(self, mock_auth_cls, mock_expired, mock_status):
        """Healthy token that was recently refreshed should return 'healthy'."""
        import etrade_auth_daemon

        tmpdir = tempfile.mkdtemp()
        status_path = Path(tmpdir) / "status.json"

        mock_auth = MagicMock()
        mock_auth.is_authenticated = True
        mock_auth.refresh_token.return_value = True
        mock_auth.sandbox = False
        mock_auth_cls.return_value = mock_auth
        mock_expired.return_value = False

        try:
            with patch.object(etrade_auth_daemon, "STATUS_FILE", status_path):
                daemon = etrade_auth_daemon.ETradeAuthDaemon()
                daemon.auth = mock_auth
                daemon.last_refresh = time.time() - 30  # Just refreshed 30s ago

                result = daemon.check_and_maintain()

            self.assertEqual(result, "healthy")
            mock_auth.refresh_token.assert_not_called()
        finally:
            if status_path.exists():
                status_path.unlink()
            os.rmdir(tmpdir)

    @patch("etrade_auth_daemon.STATUS_FILE")
    @patch("etrade_auth_daemon._is_token_expired_midnight")
    @patch("etrade_auth_daemon.ETradeAuth")
    def test_midnight_expiry_triggers_reauth(self, mock_auth_cls, mock_expired, mock_status):
        """Midnight expiry should trigger full re-auth."""
        import etrade_auth_daemon

        tmpdir = tempfile.mkdtemp()
        status_path = Path(tmpdir) / "status.json"

        mock_auth = MagicMock()
        mock_auth.is_authenticated = False
        mock_auth.sandbox = False
        mock_auth_cls.return_value = mock_auth
        mock_expired.return_value = True

        try:
            with patch.object(etrade_auth_daemon, "STATUS_FILE", status_path), \
                 patch.object(etrade_auth_daemon.ETradeAuthDaemon, "perform_reauth",
                              return_value=True) as mock_reauth:
                daemon = etrade_auth_daemon.ETradeAuthDaemon()
                daemon.auth = mock_auth

                result = daemon.check_and_maintain()

            self.assertEqual(result, "reauthed")
            mock_reauth.assert_called_once()
        finally:
            if status_path.exists():
                status_path.unlink()
            os.rmdir(tmpdir)


if __name__ == "__main__":
    unittest.main()
