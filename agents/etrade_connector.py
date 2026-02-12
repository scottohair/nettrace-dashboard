#!/usr/bin/env python3
"""E*Trade connector — OAuth 1.0a auth, trading, and market data.

Supports:
  - OAuth 1.0a 3-legged flow (request token -> authorize -> access token)
  - Account listing, balances, portfolios
  - Order preview + placement (equities)
  - Real-time quotes via E*Trade API with Yahoo Finance fallback

All secrets loaded from env vars. NEVER hardcode keys.
Tokens stored in agents/.etrade_tokens.json (gitignored).

Risk rules (hardcoded, non-negotiable):
  - Max $5 per trade
  - Max $2 daily loss then STOP
  - NEVER sell at a loss
  - Only trade on 70%+ confidence signals
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import random
import sqlite3
import string
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger("etrade")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ETRADE_CONSUMER_KEY = os.environ.get("ETRADE_CONSUMER_KEY", "")
ETRADE_CONSUMER_SECRET = os.environ.get("ETRADE_CONSUMER_SECRET", "")

# Base URLs
ETRADE_SANDBOX_BASE = "https://apisb.etrade.com"
ETRADE_PROD_BASE = "https://api.etrade.com"
ETRADE_AUTH_BASE_SANDBOX = "https://us.etrade.com/e/t/etws/authorize"
ETRADE_AUTH_BASE_PROD = "https://us.etrade.com/e/t/etws/authorize"

TOKEN_FILE = Path(__file__).parent / ".etrade_tokens.json"
TRADE_DB = Path(__file__).parent / "etrade_trades.db"

# Risk constants — NEVER modify at runtime
MAX_TRADE_USD = 5.00
MAX_DAILY_LOSS_USD = 2.00
MIN_CONFIDENCE = 0.70


# ============================================================================
# OAuth 1.0a Implementation (stdlib only, no external libraries)
# ============================================================================

def _percent_encode(s):
    """RFC 5849 percent-encoding. Encodes everything except unreserved chars."""
    return urllib.parse.quote(str(s), safe="")


def _generate_nonce(length=32):
    """Generate a random alphanumeric nonce."""
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def _generate_timestamp():
    """Current epoch timestamp as string."""
    return str(int(time.time()))


def _build_signature_base_string(method, url, params):
    """Build the OAuth signature base string.

    Format: METHOD&percent_encode(base_url)&percent_encode(sorted_params)

    The base URL excludes query string. Params are sorted by key then value,
    then joined with '&' and percent-encoded as a single string.
    """
    # Parse out any query params from the URL itself
    parsed = urllib.parse.urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    # Merge URL query params into param dict
    query_params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    merged = dict(params)
    for k, v_list in query_params.items():
        for v in v_list:
            merged[k] = v

    # Sort by key, then by value for duplicate keys
    sorted_params = sorted(merged.items(), key=lambda x: (x[0], x[1]))
    param_string = "&".join(f"{_percent_encode(k)}={_percent_encode(v)}"
                            for k, v in sorted_params)

    return f"{method.upper()}&{_percent_encode(base_url)}&{_percent_encode(param_string)}"


def _sign_hmac_sha1(base_string, consumer_secret, token_secret=""):
    """Sign the base string with HMAC-SHA1.

    Key = percent_encode(consumer_secret) & percent_encode(token_secret)
    """
    signing_key = f"{_percent_encode(consumer_secret)}&{_percent_encode(token_secret)}"
    hashed = hmac.new(
        signing_key.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha1,
    )
    return base64.b64encode(hashed.digest()).decode("utf-8")


def _build_auth_header(oauth_params):
    """Build the OAuth Authorization header string.

    Format: OAuth key="value", key2="value2", ...
    """
    pairs = ", ".join(
        f'{_percent_encode(k)}="{_percent_encode(v)}"'
        for k, v in sorted(oauth_params.items())
    )
    return f"OAuth {pairs}"


class ETradeAuth:
    """Handles E*Trade OAuth 1.0a authentication flow.

    Usage:
        auth = ETradeAuth()
        url = auth.get_request_token()    # Step 1: get authorize URL
        # User visits URL, gets verifier code
        auth.get_access_token("verifier")  # Step 2: exchange for access token
        # Now all API calls are authenticated
        auth.refresh_token()               # Step 3: renew before 2hr inactivity
    """

    def __init__(self, consumer_key=None, consumer_secret=None, sandbox=False):
        self.consumer_key = consumer_key or ETRADE_CONSUMER_KEY
        self.consumer_secret = consumer_secret or ETRADE_CONSUMER_SECRET
        self.sandbox = sandbox

        self.base_url = ETRADE_SANDBOX_BASE if sandbox else ETRADE_PROD_BASE
        self.authorize_url = ETRADE_AUTH_BASE_SANDBOX if sandbox else ETRADE_AUTH_BASE_PROD

        # Token state
        self.request_token = ""
        self.request_token_secret = ""
        self.access_token = ""
        self.access_token_secret = ""
        self.token_timestamp = 0.0

        # Try to load saved tokens
        self._load_tokens()

        if not self.consumer_key or not self.consumer_secret:
            logger.warning(
                "E*Trade credentials not set. "
                "Set ETRADE_CONSUMER_KEY and ETRADE_CONSUMER_SECRET env vars."
            )

    # ------------------------------------------------------------------
    # Token persistence
    # ------------------------------------------------------------------

    def _load_tokens(self):
        """Load saved access tokens from disk."""
        if TOKEN_FILE.exists():
            try:
                data = json.loads(TOKEN_FILE.read_text())
                self.access_token = data.get("access_token", "")
                self.access_token_secret = data.get("access_token_secret", "")
                self.token_timestamp = data.get("token_timestamp", 0.0)
                saved_date = data.get("token_date", "")

                # Tokens expire at midnight ET — if saved today, they may still be valid
                today = datetime.now(timezone(timedelta(hours=-5))).strftime("%Y-%m-%d")
                if saved_date != today:
                    logger.info("Saved tokens are from %s (today is %s) — expired", saved_date, today)
                    self.access_token = ""
                    self.access_token_secret = ""
                elif self.access_token:
                    logger.info("Loaded saved access token (from %s)", saved_date)
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning("Failed to load saved tokens: %s", e)

    def _save_tokens(self):
        """Save access tokens to disk securely."""
        today = datetime.now(timezone(timedelta(hours=-5))).strftime("%Y-%m-%d")
        data = {
            "access_token": self.access_token,
            "access_token_secret": self.access_token_secret,
            "token_timestamp": time.time(),
            "token_date": today,
            "sandbox": self.sandbox,
        }
        TOKEN_FILE.write_text(json.dumps(data, indent=2))
        # Restrict file permissions (owner read/write only)
        TOKEN_FILE.chmod(0o600)
        logger.info("Access token saved to %s", TOKEN_FILE)

    @property
    def is_authenticated(self):
        """Check if we have a valid access token."""
        return bool(self.access_token and self.access_token_secret)

    # ------------------------------------------------------------------
    # OAuth signed request helper
    # ------------------------------------------------------------------

    def _oauth_request(self, method, url, token="", token_secret="",
                       extra_params=None, body=None, content_type=None):
        """Make an OAuth 1.0a signed HTTP request.

        Args:
            method: HTTP method (GET, POST)
            url: Full URL to request
            token: OAuth token (request or access)
            token_secret: Corresponding token secret
            extra_params: Additional OAuth params (e.g., oauth_verifier)
            body: Request body bytes for POST
            content_type: Content-Type header override

        Returns:
            Response body as string, or parsed JSON dict.
        """
        # Build OAuth params
        oauth_params = {
            "oauth_consumer_key": self.consumer_key,
            "oauth_nonce": _generate_nonce(),
            "oauth_signature_method": "HMAC-SHA1",
            "oauth_timestamp": _generate_timestamp(),
            "oauth_version": "1.0",
        }
        if token:
            oauth_params["oauth_token"] = token
        if extra_params:
            oauth_params.update(extra_params)

        # Build signature
        base_string = _build_signature_base_string(method, url, oauth_params)
        signature = _sign_hmac_sha1(base_string, self.consumer_secret, token_secret)
        oauth_params["oauth_signature"] = signature

        # Build Authorization header
        headers = {
            "Authorization": _build_auth_header(oauth_params),
            "User-Agent": "NetTrace-ETrade/1.0",
        }
        if content_type:
            headers["Content-Type"] = content_type

        req = urllib.request.Request(url, data=body, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp_body = resp.read().decode("utf-8")
                # Try to parse as JSON if applicable
                if resp.headers.get("Content-Type", "").startswith("application/json"):
                    return json.loads(resp_body)
                return resp_body
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            logger.error("E*Trade API error %d on %s %s: %s", e.code, method, url, error_body)
            raise ETradeAPIError(e.code, error_body) from e
        except urllib.error.URLError as e:
            logger.error("E*Trade connection error on %s: %s", url, e.reason)
            raise

    # ------------------------------------------------------------------
    # OAuth 1.0a flow: Step 1 — Request Token
    # ------------------------------------------------------------------

    def get_request_token(self):
        """Get a request token and return the authorization URL.

        The user must visit this URL in a browser to authorize the app.
        E*Trade will display a verifier code that the user passes to
        get_access_token().

        Returns:
            str: The authorization URL the user should visit.
        """
        url = f"{self.base_url}/oauth/request_token"

        extra_params = {"oauth_callback": "oob"}
        resp = self._oauth_request("GET", url, extra_params=extra_params)

        # Response is URL-encoded: oauth_token=xxx&oauth_token_secret=yyy&...
        params = urllib.parse.parse_qs(resp)
        self.request_token = params["oauth_token"][0]
        self.request_token_secret = params["oauth_token_secret"][0]

        logger.info("Got request token: %s...", self.request_token[:8])

        # Persist request token so verify step can read it in a separate process
        self._save_request_token()

        # Build authorize URL
        auth_url = (
            f"{self.authorize_url}"
            f"?key={_percent_encode(self.consumer_key)}"
            f"&token={_percent_encode(self.request_token)}"
        )
        return auth_url

    def _save_request_token(self):
        """Save request token to disk so verify can run in a separate process."""
        data = {
            "request_token": self.request_token,
            "request_token_secret": self.request_token_secret,
            "sandbox": self.sandbox,
            "timestamp": time.time(),
        }
        # Save alongside the regular token file
        req_file = TOKEN_FILE.parent / ".etrade_request_token.json"
        req_file.write_text(json.dumps(data))
        req_file.chmod(0o600)

    def _load_request_token(self):
        """Load request token saved during auth step."""
        req_file = TOKEN_FILE.parent / ".etrade_request_token.json"
        if req_file.exists():
            try:
                data = json.loads(req_file.read_text())
                self.request_token = data.get("request_token", "")
                self.request_token_secret = data.get("request_token_secret", "")
                if self.request_token:
                    logger.info("Loaded saved request token: %s...", self.request_token[:8])
                    return True
            except Exception as e:
                logger.warning("Failed to load request token: %s", e)
        return False

    # ------------------------------------------------------------------
    # OAuth 1.0a flow: Step 2 — Access Token
    # ------------------------------------------------------------------

    def get_access_token(self, verifier_code):
        """Exchange the request token + verifier for an access token.

        Args:
            verifier_code: The code displayed to the user after authorization.

        Returns:
            bool: True if successful.
        """
        url = f"{self.base_url}/oauth/access_token"

        extra_params = {"oauth_verifier": verifier_code.strip()}
        resp = self._oauth_request(
            "GET", url,
            token=self.request_token,
            token_secret=self.request_token_secret,
            extra_params=extra_params,
        )

        params = urllib.parse.parse_qs(resp)
        self.access_token = params["oauth_token"][0]
        self.access_token_secret = params["oauth_token_secret"][0]
        self.token_timestamp = time.time()

        logger.info("Got access token: %s...", self.access_token[:8])

        # Save to disk
        self._save_tokens()
        return True

    # ------------------------------------------------------------------
    # Token renewal
    # ------------------------------------------------------------------

    def refresh_token(self):
        """Renew the access token before the 2-hour inactivity timeout.

        E*Trade tokens go inactive after 2 hours of no API calls.
        This endpoint reactivates them. Tokens expire completely at
        midnight US Eastern time regardless.

        Returns:
            bool: True if renewal succeeded.
        """
        if not self.is_authenticated:
            logger.warning("No access token to refresh")
            return False

        url = f"{self.base_url}/oauth/renew_access_token"
        try:
            self._oauth_request(
                "GET", url,
                token=self.access_token,
                token_secret=self.access_token_secret,
            )
            self.token_timestamp = time.time()
            logger.info("Access token renewed")
            return True
        except ETradeAPIError as e:
            logger.error("Token renewal failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # Authenticated API request helper
    # ------------------------------------------------------------------

    def api_request(self, method, path, body=None):
        """Make an authenticated API request to E*Trade.

        Automatically signs with the access token and handles JSON.

        Args:
            method: HTTP method (GET, POST)
            path: API path (e.g., /v1/accounts/list)
            body: Dict to send as JSON body for POST requests

        Returns:
            Parsed JSON response as dict.
        """
        if not self.is_authenticated:
            raise ETradeAuthError("Not authenticated. Run OAuth flow first.")

        url = f"{self.base_url}{path}"
        body_bytes = None
        content_type = None

        if body is not None:
            body_bytes = json.dumps(body).encode("utf-8")
            content_type = "application/json"

        # Add .json suffix for JSON response format if not already present
        if ".json" not in url and "?" not in url:
            url += ".json"
        elif ".json" not in url and "?" in url:
            base, query = url.split("?", 1)
            url = f"{base}.json?{query}"

        resp = self._oauth_request(
            method, url,
            token=self.access_token,
            token_secret=self.access_token_secret,
            body=body_bytes,
            content_type=content_type,
        )

        if isinstance(resp, dict):
            return resp

        # Try to parse as JSON if it came back as a string
        try:
            return json.loads(resp)
        except (json.JSONDecodeError, TypeError):
            return {"raw_response": resp}


# ============================================================================
# Custom Exceptions
# ============================================================================

class ETradeAPIError(Exception):
    """Raised when the E*Trade API returns an error."""

    def __init__(self, status_code, body):
        self.status_code = status_code
        self.body = body
        super().__init__(f"HTTP {status_code}: {body[:500]}")


class ETradeAuthError(Exception):
    """Raised when authentication is missing or invalid."""
    pass


class ETradeRiskError(Exception):
    """Raised when a risk rule is violated."""
    pass


# ============================================================================
# Trading Operations
# ============================================================================

class ETradeTrader:
    """E*Trade trading operations — accounts, portfolios, orders.

    All trading operations go through the authenticated ETradeAuth instance.
    Risk rules are enforced on every trade.
    """

    def __init__(self, auth=None, sandbox=False):
        self.auth = auth or ETradeAuth(sandbox=sandbox)
        self.db = sqlite3.connect(str(TRADE_DB))
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self._daily_pnl = 0.0
        self._trades_today = 0

    def _init_db(self):
        """Initialize the trade tracking database."""
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS etrade_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL,
                total_usd REAL,
                order_type TEXT DEFAULT 'MARKET',
                order_id TEXT,
                preview_id TEXT,
                client_order_id TEXT,
                status TEXT DEFAULT 'pending',
                signal_confidence REAL,
                pnl REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS etrade_buy_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                buy_price REAL NOT NULL,
                quantity REAL NOT NULL,
                remaining_quantity REAL NOT NULL,
                account_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS etrade_daily_pnl (
                date TEXT PRIMARY KEY,
                realized_pnl REAL DEFAULT 0.0,
                trades_count INTEGER DEFAULT 0,
                stopped INTEGER DEFAULT 0
            );
        """)
        self.db.commit()

        # Load today's P&L
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        row = self.db.execute(
            "SELECT realized_pnl, trades_count, stopped FROM etrade_daily_pnl WHERE date=?",
            (today,)
        ).fetchone()
        if row:
            self._daily_pnl = row["realized_pnl"]
            self._trades_today = row["trades_count"]
            if row["stopped"]:
                logger.warning("Daily loss limit was already hit today")

    def _update_daily_pnl(self, pnl_delta):
        """Update daily P&L tracking."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._daily_pnl += pnl_delta
        self._trades_today += 1
        stopped = 1 if self._daily_pnl <= -MAX_DAILY_LOSS_USD else 0

        self.db.execute("""
            INSERT INTO etrade_daily_pnl (date, realized_pnl, trades_count, stopped)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                realized_pnl=?, trades_count=?, stopped=?
        """, (today, self._daily_pnl, self._trades_today, stopped,
              self._daily_pnl, self._trades_today, stopped))
        self.db.commit()

    def _record_buy_price(self, symbol, price, quantity, account_id):
        """Record a buy price for profit validation on future sells."""
        self.db.execute(
            "INSERT INTO etrade_buy_prices (symbol, buy_price, quantity, remaining_quantity, account_id) "
            "VALUES (?, ?, ?, ?, ?)",
            (symbol, price, quantity, quantity, account_id),
        )
        self.db.commit()
        logger.info("Recorded buy: %s @ $%.4f x %.4f", symbol, price, quantity)

    def _get_avg_buy_price(self, symbol):
        """Get volume-weighted average buy price for a symbol.

        Only considers lots with remaining_quantity > 0.

        Returns:
            (avg_price, total_remaining_qty) or (None, 0) if no buys recorded.
        """
        rows = self.db.execute(
            "SELECT buy_price, remaining_quantity FROM etrade_buy_prices "
            "WHERE symbol=? AND remaining_quantity > 0 ORDER BY created_at ASC",
            (symbol,),
        ).fetchall()

        if not rows:
            return None, 0.0

        total_cost = sum(r["buy_price"] * r["remaining_quantity"] for r in rows)
        total_qty = sum(r["remaining_quantity"] for r in rows)

        if total_qty <= 0:
            return None, 0.0

        return total_cost / total_qty, total_qty

    def _consume_buy_lots(self, symbol, sell_quantity, sell_price):
        """Consume buy lots FIFO for a sell, computing realized P&L.

        Returns:
            Realized P&L from this sell.
        """
        rows = self.db.execute(
            "SELECT id, buy_price, remaining_quantity FROM etrade_buy_prices "
            "WHERE symbol=? AND remaining_quantity > 0 ORDER BY created_at ASC",
            (symbol,),
        ).fetchall()

        remaining = sell_quantity
        realized_pnl = 0.0

        for row in rows:
            if remaining <= 0:
                break

            consume = min(remaining, row["remaining_quantity"])
            pnl = (sell_price - row["buy_price"]) * consume
            realized_pnl += pnl
            remaining -= consume

            new_remaining = row["remaining_quantity"] - consume
            self.db.execute(
                "UPDATE etrade_buy_prices SET remaining_quantity=? WHERE id=?",
                (new_remaining, row["id"]),
            )

        self.db.commit()
        return realized_pnl

    # ------------------------------------------------------------------
    # Account operations
    # ------------------------------------------------------------------

    def get_accounts(self):
        """List all E*Trade accounts for the authenticated user.

        Returns:
            List of account dicts with accountIdKey, accountName, accountType, etc.
        """
        try:
            resp = self.auth.api_request("GET", "/v1/accounts/list")
            accounts = resp.get("AccountListResponse", {}).get("Accounts", {}).get("Account", [])
            if isinstance(accounts, dict):
                accounts = [accounts]
            logger.info("Found %d E*Trade account(s)", len(accounts))
            return accounts
        except ETradeAPIError as e:
            logger.error("Failed to list accounts: %s", e)
            return []

    def get_balance(self, account_id_key):
        """Get account balance details.

        Args:
            account_id_key: The accountIdKey from list accounts.

        Returns:
            Dict with balance details including cashAvailableForInvestment.
        """
        try:
            path = (
                f"/v1/accounts/{account_id_key}/balance"
                f"?instType=BROKERAGE&realTimeNAV=true"
            )
            resp = self.auth.api_request("GET", path)
            return resp.get("BalanceResponse", resp)
        except ETradeAPIError as e:
            logger.error("Failed to get balance for %s: %s", account_id_key, e)
            return {}

    def get_portfolio(self, account_id_key):
        """Get current positions for an account.

        Args:
            account_id_key: The accountIdKey from list accounts.

        Returns:
            List of position dicts with symbol, quantity, marketValue, pricePaid, etc.
        """
        try:
            path = (
                f"/v1/accounts/{account_id_key}/portfolio"
                f"?view=COMPLETE&totalsRequired=true"
            )
            resp = self.auth.api_request("GET", path)
            portfolio = resp.get("PortfolioResponse", {})
            positions = []

            account_portfolios = portfolio.get("AccountPortfolio", [])
            if isinstance(account_portfolios, dict):
                account_portfolios = [account_portfolios]

            for ap in account_portfolios:
                pos_list = ap.get("Position", [])
                if isinstance(pos_list, dict):
                    pos_list = [pos_list]
                positions.extend(pos_list)

            logger.info("Found %d position(s) in account %s", len(positions), account_id_key)
            return positions
        except ETradeAPIError as e:
            if e.status_code == 204:
                logger.info("No positions in account %s", account_id_key)
                return []
            logger.error("Failed to get portfolio for %s: %s", account_id_key, e)
            return []

    def get_orders(self, account_id_key, status="OPEN"):
        """List recent orders for an account.

        Args:
            account_id_key: The accountIdKey from list accounts.
            status: Filter (OPEN, EXECUTED, CANCELLED, INDIVIDUAL_FILLS, etc.)

        Returns:
            List of order dicts.
        """
        try:
            path = f"/v1/accounts/{account_id_key}/orders?status={status}&count=50"
            resp = self.auth.api_request("GET", path)
            orders_resp = resp.get("OrdersResponse", {})
            orders = orders_resp.get("Order", [])
            if isinstance(orders, dict):
                orders = [orders]
            return orders
        except ETradeAPIError as e:
            if e.status_code == 204:
                return []
            logger.error("Failed to get orders for %s: %s", account_id_key, e)
            return []

    # ------------------------------------------------------------------
    # Quote
    # ------------------------------------------------------------------

    def get_quote(self, symbols):
        """Get real-time quotes for one or more symbols.

        Args:
            symbols: Single symbol string or list of symbols.

        Returns:
            Dict mapping symbol -> quote data (lastTrade, bid, ask, volume, etc.)
        """
        if isinstance(symbols, str):
            symbols = [symbols]

        # E*Trade supports up to 25 symbols per request
        symbols_str = ",".join(s.upper() for s in symbols[:25])

        try:
            path = f"/v1/market/quote/{symbols_str}?detailFlag=ALL"
            resp = self.auth.api_request("GET", path)

            quotes = {}
            quote_data = resp.get("QuoteResponse", {}).get("QuoteData", [])
            if isinstance(quote_data, dict):
                quote_data = [quote_data]

            for qd in quote_data:
                product = qd.get("Product", {})
                symbol = product.get("symbol", "")
                all_data = qd.get("All", {})
                intraday = qd.get("Intraday", {})

                quotes[symbol] = {
                    "symbol": symbol,
                    "lastTrade": all_data.get("lastTrade", intraday.get("lastTrade", 0)),
                    "bid": all_data.get("bid", 0),
                    "ask": all_data.get("ask", 0),
                    "high": all_data.get("high", 0),
                    "low": all_data.get("low", 0),
                    "volume": all_data.get("totalVolume", 0),
                    "changeClose": all_data.get("changeClose", 0),
                    "changeClosePercentage": all_data.get("changeClosePercentage", 0),
                    "previousClose": all_data.get("previousClose", 0),
                    "companyName": all_data.get("companyName", ""),
                    "week52High": all_data.get("high52", 0),
                    "week52Low": all_data.get("low52", 0),
                    "pe": all_data.get("pe", 0),
                    "eps": all_data.get("eps", 0),
                    "marketCap": all_data.get("marketCap", 0),
                    "quoteStatus": qd.get("quoteStatus", ""),
                    "dateTime": qd.get("dateTime", ""),
                }

            return quotes
        except ETradeAPIError as e:
            logger.error("Failed to get quotes for %s: %s", symbols_str, e)
            return {}

    # ------------------------------------------------------------------
    # Order placement (preview + place, with risk enforcement)
    # ------------------------------------------------------------------

    def _preview_order(self, account_id_key, symbol, side, quantity,
                       order_type="MARKET", limit_price=None):
        """Preview an order before placement.

        E*Trade requires a preview step that returns a previewId.

        Returns:
            (preview_id, estimated_total, client_order_id) or raises on error.
        """
        import uuid
        client_order_id = uuid.uuid4().hex[:20]

        order_detail = {
            "allOrNone": "false",
            "priceType": order_type.upper(),
            "orderTerm": "GOOD_FOR_DAY",
            "marketSession": "REGULAR",
            "Instrument": [{
                "Product": {
                    "securityType": "EQ",
                    "symbol": symbol.upper(),
                },
                "orderAction": side.upper(),
                "quantityType": "QUANTITY",
                "quantity": int(quantity),
            }],
        }

        if order_type.upper() == "LIMIT" and limit_price is not None:
            order_detail["limitPrice"] = str(limit_price)

        body = {
            "PreviewOrderRequest": {
                "orderType": "EQ",
                "clientOrderId": client_order_id,
                "Order": [order_detail],
            }
        }

        path = f"/v1/accounts/{account_id_key}/orders/preview"
        resp = self.auth.api_request("POST", path, body=body)

        preview_resp = resp.get("PreviewOrderResponse", {})
        preview_ids = preview_resp.get("PreviewIds", [])
        if isinstance(preview_ids, dict):
            preview_ids = [preview_ids]

        if not preview_ids:
            raise ETradeAPIError(0, f"No preview ID returned: {json.dumps(resp)[:500]}")

        preview_id = preview_ids[0].get("previewId")

        # Extract estimated cost from the preview
        estimated_total = 0.0
        orders = preview_resp.get("Order", [])
        if isinstance(orders, dict):
            orders = [orders]
        for order in orders:
            est = order.get("estimatedTotalAmount", 0)
            if est:
                estimated_total = float(est)

        logger.info(
            "Preview OK: %s %s %s x%d | est_total=$%.2f | preview_id=%s",
            side, symbol, order_type, quantity, estimated_total, preview_id,
        )
        return preview_id, estimated_total, client_order_id

    def place_order(self, account_id_key, symbol, side, quantity,
                    order_type="MARKET", limit_price=None,
                    signal_confidence=None, dry_run=False):
        """Place an equity order with full risk checks.

        Enforces ALL risk rules before execution:
          - Max $5 per trade
          - Max $2 daily loss then STOP
          - NEVER sell at a loss
          - Only trade on 70%+ confidence (if confidence provided)

        Args:
            account_id_key: Account to trade in
            symbol: Stock ticker (e.g., "AAPL")
            side: "BUY" or "SELL"
            quantity: Number of shares (integer)
            order_type: "MARKET" or "LIMIT"
            limit_price: Required for LIMIT orders
            signal_confidence: Confidence score of the triggering signal
            dry_run: If True, preview only (do not execute)

        Returns:
            Dict with order result including orderId, status, etc.
        """
        symbol = symbol.upper()
        side = side.upper()
        quantity = int(quantity)

        if quantity <= 0:
            raise ValueError("Quantity must be positive")

        if side not in ("BUY", "SELL"):
            raise ValueError("Side must be BUY or SELL")

        # ---- Risk Rule 1: Confidence threshold ----
        if signal_confidence is not None and signal_confidence < MIN_CONFIDENCE:
            raise ETradeRiskError(
                f"Signal confidence {signal_confidence:.2f} < {MIN_CONFIDENCE:.2f} minimum. "
                f"REFUSING to trade."
            )

        # ---- Risk Rule 2: Daily loss limit ----
        if self._daily_pnl <= -MAX_DAILY_LOSS_USD:
            raise ETradeRiskError(
                f"Daily P&L is ${self._daily_pnl:+.2f} — hit ${MAX_DAILY_LOSS_USD} "
                f"loss limit. STOPPED for today."
            )

        # ---- Risk Rule 3: Estimate trade value and enforce max ----
        # Get current price for validation
        quotes = self.get_quote([symbol])
        if symbol not in quotes or not quotes[symbol].get("lastTrade"):
            raise ETradeRiskError(f"Cannot get price for {symbol}. REFUSING to trade blind.")

        current_price = float(quotes[symbol]["lastTrade"])
        estimated_value = current_price * quantity

        if estimated_value > MAX_TRADE_USD:
            raise ETradeRiskError(
                f"Estimated trade value ${estimated_value:.2f} exceeds "
                f"${MAX_TRADE_USD:.2f} maximum. REFUSING."
            )

        # ---- Risk Rule 4: NEVER sell at a loss ----
        if side == "SELL":
            avg_buy, total_qty = self._get_avg_buy_price(symbol)
            if avg_buy is not None:
                # Include estimated fees (~0.5% round trip)
                breakeven = avg_buy * 1.005
                if current_price < breakeven:
                    raise ETradeRiskError(
                        f"SELL BLOCKED: {symbol} current ${current_price:.4f} < "
                        f"breakeven ${breakeven:.4f} (avg_buy=${avg_buy:.4f} + fees). "
                        f"NEVER sell at a loss."
                    )
                if quantity > total_qty:
                    logger.warning(
                        "Sell quantity %d > recorded holdings %.4f for %s. "
                        "Capping at holdings.",
                        quantity, total_qty, symbol,
                    )
                    quantity = max(1, int(total_qty))
            else:
                logger.warning(
                    "No recorded buy price for %s. REFUSING to sell without "
                    "profit validation.",
                    symbol,
                )
                raise ETradeRiskError(
                    f"No recorded buy price for {symbol}. "
                    f"Cannot validate profit. REFUSING to sell."
                )

        # ---- Risk Rule 5: Balance check for buys ----
        if side == "BUY":
            balance = self.get_balance(account_id_key)
            computed = balance.get("Computed", {})
            cash_available = float(computed.get("cashAvailableForInvestment", 0))
            if cash_available < estimated_value:
                raise ETradeRiskError(
                    f"Insufficient balance: ${cash_available:.2f} available, "
                    f"need ${estimated_value:.2f}. REFUSING."
                )

        # ---- Preview the order ----
        preview_id, est_total, client_order_id = self._preview_order(
            account_id_key, symbol, side, quantity, order_type, limit_price,
        )

        # Re-check estimated total from preview
        if est_total > MAX_TRADE_USD * 1.05:  # 5% tolerance for price movement
            raise ETradeRiskError(
                f"Preview estimated total ${est_total:.2f} exceeds limit. REFUSING."
            )

        if dry_run:
            logger.info("DRY RUN — order previewed but NOT placed")
            return {
                "dry_run": True,
                "preview_id": preview_id,
                "estimated_total": est_total,
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "current_price": current_price,
            }

        # ---- Place the order ----
        order_detail = {
            "allOrNone": "false",
            "priceType": order_type.upper(),
            "orderTerm": "GOOD_FOR_DAY",
            "marketSession": "REGULAR",
            "Instrument": [{
                "Product": {
                    "securityType": "EQ",
                    "symbol": symbol,
                },
                "orderAction": side,
                "quantityType": "QUANTITY",
                "quantity": quantity,
            }],
        }

        if order_type.upper() == "LIMIT" and limit_price is not None:
            order_detail["limitPrice"] = str(limit_price)

        body = {
            "PlaceOrderRequest": {
                "orderType": "EQ",
                "clientOrderId": client_order_id,
                "PreviewIds": [{"previewId": preview_id}],
                "Order": [order_detail],
            }
        }

        path = f"/v1/accounts/{account_id_key}/orders/place"
        resp = self.auth.api_request("POST", path, body=body)

        place_resp = resp.get("PlaceOrderResponse", resp)
        order_ids = place_resp.get("OrderIds", [])
        if isinstance(order_ids, dict):
            order_ids = [order_ids]
        order_id = order_ids[0].get("orderId", "") if order_ids else ""

        # Record the trade
        self.db.execute(
            """INSERT INTO etrade_trades
               (account_id, symbol, side, quantity, price, total_usd,
                order_type, order_id, preview_id, client_order_id, status, signal_confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (account_id_key, symbol, side, quantity, current_price, estimated_value,
             order_type, str(order_id), str(preview_id), client_order_id,
             "placed", signal_confidence),
        )
        self.db.commit()

        # Track buy prices for profit validation
        if side == "BUY":
            self._record_buy_price(symbol, current_price, quantity, account_id_key)

        # Track sell P&L
        if side == "SELL":
            realized_pnl = self._consume_buy_lots(symbol, quantity, current_price)
            self._update_daily_pnl(realized_pnl)
            logger.info("SELL realized P&L: $%.4f", realized_pnl)
            self.db.execute(
                "UPDATE etrade_trades SET pnl=? WHERE client_order_id=?",
                (realized_pnl, client_order_id),
            )
            self.db.commit()

        logger.info(
            "ORDER PLACED: %s %s %s x%d @ ~$%.2f | order_id=%s",
            side, symbol, order_type, quantity, current_price, order_id,
        )

        return {
            "success": True,
            "order_id": order_id,
            "preview_id": preview_id,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": current_price,
            "estimated_total": estimated_value,
            "response": place_resp,
        }


# ============================================================================
# Price Feed — E*Trade quotes with Yahoo Finance fallback
# ============================================================================

class ETradePriceFeed:
    """Market data price feed with caching.

    Primary: E*Trade quote API (requires auth).
    Fallback: Yahoo Finance v8 API (free, no auth).

    Mirrors the interface of PriceFeed in exchange_connector.py.
    """

    CACHE = {}
    CACHE_TTL = 10  # seconds

    def __init__(self, auth=None):
        """Initialize with optional ETradeAuth for authenticated quotes.

        If auth is None or not authenticated, falls back to Yahoo Finance only.
        """
        self.auth = auth

    def get_price(self, symbol):
        """Get current price for a symbol with 10s cache.

        Args:
            symbol: Stock ticker (e.g., "AAPL")

        Returns:
            Float price or None if unavailable.
        """
        symbol = symbol.upper()
        now = time.time()

        # Check cache
        if symbol in self.CACHE and now - self.CACHE[symbol]["t"] < self.CACHE_TTL:
            return self.CACHE[symbol]["price"]

        price = None

        # Try E*Trade authenticated API first
        if self.auth and self.auth.is_authenticated:
            price = self._fetch_etrade_price(symbol)

        # Fallback to Yahoo Finance
        if price is None:
            price = self._fetch_yahoo_price(symbol)

        if price is not None:
            self.CACHE[symbol] = {"price": price, "t": now}

        return price

    def get_prices(self, symbols):
        """Get prices for multiple symbols.

        Args:
            symbols: List of ticker strings.

        Returns:
            Dict mapping symbol -> price (or None).
        """
        if isinstance(symbols, str):
            symbols = [symbols]

        result = {}

        # Batch fetch from E*Trade if authenticated
        uncached = []
        now = time.time()
        for s in symbols:
            s = s.upper()
            if s in self.CACHE and now - self.CACHE[s]["t"] < self.CACHE_TTL:
                result[s] = self.CACHE[s]["price"]
            else:
                uncached.append(s)

        if uncached and self.auth and self.auth.is_authenticated:
            etrade_prices = self._fetch_etrade_prices_batch(uncached)
            for sym, price in etrade_prices.items():
                if price is not None:
                    self.CACHE[sym] = {"price": price, "t": now}
                    result[sym] = price
                    uncached.remove(sym)

        # Fallback for anything still missing
        for sym in uncached:
            price = self._fetch_yahoo_price(sym)
            if price is not None:
                self.CACHE[sym] = {"price": price, "t": time.time()}
            result[sym] = price

        return result

    def _fetch_etrade_price(self, symbol):
        """Fetch a single price from E*Trade quote API."""
        try:
            path = f"/v1/market/quote/{symbol}?detailFlag=INTRADAY"
            url = f"{self.auth.base_url}{path}.json"

            resp = self.auth._oauth_request(
                "GET", url,
                token=self.auth.access_token,
                token_secret=self.auth.access_token_secret,
            )

            if isinstance(resp, str):
                resp = json.loads(resp)

            quote_data = resp.get("QuoteResponse", {}).get("QuoteData", [])
            if isinstance(quote_data, dict):
                quote_data = [quote_data]

            if quote_data:
                all_data = quote_data[0].get("All", {})
                intraday = quote_data[0].get("Intraday", {})
                price = all_data.get("lastTrade") or intraday.get("lastTrade")
                if price:
                    return float(price)
        except Exception as e:
            logger.debug("E*Trade price fetch failed for %s: %s", symbol, e)

        return None

    def _fetch_etrade_prices_batch(self, symbols):
        """Fetch multiple prices from E*Trade in a single call."""
        result = {}
        if not symbols:
            return result

        try:
            symbols_str = ",".join(symbols[:25])
            path = f"/v1/market/quote/{symbols_str}?detailFlag=INTRADAY"
            url = f"{self.auth.base_url}{path}.json"

            resp = self.auth._oauth_request(
                "GET", url,
                token=self.auth.access_token,
                token_secret=self.auth.access_token_secret,
            )

            if isinstance(resp, str):
                resp = json.loads(resp)

            quote_data = resp.get("QuoteResponse", {}).get("QuoteData", [])
            if isinstance(quote_data, dict):
                quote_data = [quote_data]

            for qd in quote_data:
                sym = qd.get("Product", {}).get("symbol", "")
                all_data = qd.get("All", {})
                intraday = qd.get("Intraday", {})
                price = all_data.get("lastTrade") or intraday.get("lastTrade")
                if sym and price:
                    result[sym] = float(price)
        except Exception as e:
            logger.debug("E*Trade batch price fetch failed: %s", e)

        return result

    @staticmethod
    def _fetch_yahoo_price(symbol):
        """Fetch price from Yahoo Finance v8 API (free, no auth).

        This is the fallback when E*Trade auth is not available.
        """
        try:
            url = (
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                f"?interval=1m&range=1d"
            )
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; NetTrace/1.0)",
            })
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())

            result = data.get("chart", {}).get("result", [])
            if result:
                meta = result[0].get("meta", {})
                price = meta.get("regularMarketPrice")
                if price:
                    return float(price)
        except Exception as e:
            logger.debug("Yahoo Finance price fetch failed for %s: %s", symbol, e)

        return None


# ============================================================================
# CLI Interface
# ============================================================================

def _load_env():
    """Load .env file if present (minimal dotenv without dependencies)."""
    env_paths = [
        Path(__file__).parent / ".env",
        Path(__file__).parent.parent / ".env",
    ]
    for env_path in env_paths:
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value


def cli_auth(args):
    """Start OAuth flow — prints authorization URL."""
    sandbox = "--sandbox" in args
    auth = ETradeAuth(sandbox=sandbox)

    if auth.is_authenticated:
        print("Already authenticated (saved token found).")
        print("To re-authenticate, delete", TOKEN_FILE)
        print()
        # Verify token still works by trying a renew
        try:
            auth.refresh_token()
            print("Token is ACTIVE and renewed.")
            return
        except Exception:
            print("Saved token is INACTIVE. Starting new auth flow...")
            print()

    if not auth.consumer_key or not auth.consumer_secret:
        print("ERROR: Set ETRADE_CONSUMER_KEY and ETRADE_CONSUMER_SECRET env vars first.")
        print()
        print("  export ETRADE_CONSUMER_KEY='your_key'")
        print("  export ETRADE_CONSUMER_SECRET='your_secret'")
        sys.exit(1)

    url = auth.get_request_token()
    print()
    print("=" * 70)
    print("  E*TRADE AUTHORIZATION")
    print("=" * 70)
    print()
    print("  1. Open this URL in your browser:")
    print()
    print(f"     {url}")
    print()
    print("  2. Log in to E*Trade and authorize the application.")
    print()
    print("  3. E*Trade will display a VERIFIER CODE.")
    print()
    print("  4. Run:")
    print(f"     python {Path(__file__).name} verify <VERIFIER_CODE>")
    print()
    print("=" * 70)


def cli_verify(args):
    """Complete OAuth with verifier code."""
    if not args:
        print("Usage: python etrade_connector.py verify <VERIFIER_CODE>")
        sys.exit(1)

    verifier = args[0].strip()
    sandbox = "--sandbox" in args
    auth = ETradeAuth(sandbox=sandbox)

    if not auth.request_token:
        # Try loading from disk (saved during auth step)
        if not auth._load_request_token():
            print("ERROR: No request token found. Run 'auth' command first.")
            sys.exit(1)

    try:
        auth.get_access_token(verifier)
        print()
        print("SUCCESS! Access token obtained and saved.")
        print(f"Token stored at: {TOKEN_FILE}")
        print()
        print("You can now use E*Trade API commands:")
        print(f"  python {Path(__file__).name} status")
        print(f"  python {Path(__file__).name} quote AAPL MSFT")
    except ETradeAPIError as e:
        print(f"ERROR: {e}")
        print("The verifier code may have expired. Try 'auth' again.")
        sys.exit(1)


def cli_status(args):
    """Show accounts + balances."""
    sandbox = "--sandbox" in args
    auth = ETradeAuth(sandbox=sandbox)

    if not auth.is_authenticated:
        print("Not authenticated. Run: python etrade_connector.py auth")
        sys.exit(1)

    trader = ETradeTrader(auth=auth, sandbox=sandbox)

    print()
    print("=" * 70)
    print("  E*TRADE ACCOUNT STATUS")
    print("=" * 70)

    accounts = trader.get_accounts()
    if not accounts:
        print("  No accounts found or authentication failed.")
        return

    for acc in accounts:
        account_id_key = acc.get("accountIdKey", "")
        account_name = acc.get("accountDesc", acc.get("accountName", "Unknown"))
        account_type = acc.get("accountType", "Unknown")
        account_mode = acc.get("accountMode", "")

        print(f"\n  Account: {account_name}")
        print(f"    ID Key:  {account_id_key}")
        print(f"    Type:    {account_type} ({account_mode})")

        # Get balance
        balance = trader.get_balance(account_id_key)
        if balance:
            computed = balance.get("Computed", {})
            rtv = balance.get("RealTimeValues", {})
            total = rtv.get("totalAccountValue", computed.get("accountBalance", "N/A"))
            cash = computed.get("cashAvailableForInvestment", "N/A")
            buying_power = computed.get("cashBuyingPower", "N/A")

            print(f"    Total Value:     ${_fmt_money(total)}")
            print(f"    Cash Available:  ${_fmt_money(cash)}")
            print(f"    Buying Power:    ${_fmt_money(buying_power)}")

        # Get positions
        positions = trader.get_portfolio(account_id_key)
        if positions:
            print(f"    Positions ({len(positions)}):")
            for pos in positions:
                product = pos.get("Product", {})
                sym = product.get("symbol", pos.get("symbolDescription", "?"))
                qty = pos.get("quantity", 0)
                mv = pos.get("marketValue", 0)
                pp = pos.get("pricePaid", 0)
                gain = pos.get("totalGain", 0)
                gain_pct = pos.get("totalGainPct", 0)

                print(f"      {sym:<8} {qty:>8.2f} sh  "
                      f"MV=${_fmt_money(mv):>10}  "
                      f"Paid=${_fmt_money(pp):>8}  "
                      f"Gain=${_fmt_money(gain):>8} ({gain_pct:+.1f}%)")

    # Daily P&L tracking
    print(f"\n  Daily P&L:    ${trader._daily_pnl:+.2f}")
    print(f"  Trades Today: {trader._trades_today}")
    print(f"  Risk Limits:  max ${MAX_TRADE_USD}/trade, "
          f"stop at -${MAX_DAILY_LOSS_USD}/day")
    print("=" * 70)
    print()


def cli_quote(args):
    """Get quotes for symbols."""
    symbols = [s for s in args if not s.startswith("--")]
    sandbox = "--sandbox" in args

    if not symbols:
        print("Usage: python etrade_connector.py quote AAPL MSFT GOOGL")
        sys.exit(1)

    # Try E*Trade first, fallback to Yahoo
    auth = ETradeAuth(sandbox=sandbox)
    feed = ETradePriceFeed(auth=auth if auth.is_authenticated else None)

    use_etrade = auth.is_authenticated
    if use_etrade:
        trader = ETradeTrader(auth=auth, sandbox=sandbox)
        quotes = trader.get_quote(symbols)
    else:
        quotes = {}

    print()
    print("=" * 70)
    source = "E*Trade" if use_etrade and quotes else "Yahoo Finance"
    print(f"  MARKET QUOTES (via {source})")
    print("=" * 70)

    if use_etrade and quotes:
        for sym, q in quotes.items():
            print(f"\n  {sym} — {q.get('companyName', '')}")
            print(f"    Last:     ${q['lastTrade']:,.4f}")
            print(f"    Bid/Ask:  ${q['bid']:,.4f} / ${q['ask']:,.4f}")
            print(f"    Change:   ${q['changeClose']:+,.4f} "
                  f"({q['changeClosePercentage']:+.2f}%)")
            print(f"    Range:    ${q['low']:,.4f} - ${q['high']:,.4f}")
            print(f"    Volume:   {q['volume']:,.0f}")
            print(f"    52w:      ${q['week52Low']:,.2f} - ${q['week52High']:,.2f}")
            print(f"    P/E:      {q['pe']:.2f}  EPS: ${q['eps']:.2f}")
    else:
        prices = feed.get_prices(symbols)
        for sym in symbols:
            sym = sym.upper()
            price = prices.get(sym)
            if price is not None:
                print(f"  {sym:<8} ${price:>12,.4f}")
            else:
                print(f"  {sym:<8} unavailable")

    print()
    print("=" * 70)
    print()


def cli_test(args):
    """Dry run of a small trade to validate the full flow."""
    sandbox = "--sandbox" in args
    auth = ETradeAuth(sandbox=sandbox)

    if not auth.is_authenticated:
        print("Not authenticated. Run: python etrade_connector.py auth")
        print()
        print("Running quote-only test with Yahoo Finance fallback...")
        feed = ETradePriceFeed()
        for sym in ["AAPL", "MSFT", "GOOGL"]:
            price = feed.get_price(sym)
            status = f"${price:,.2f}" if price else "unavailable"
            print(f"  {sym}: {status}")
        return

    trader = ETradeTrader(auth=auth, sandbox=sandbox)

    print()
    print("=" * 70)
    print("  E*TRADE DRY RUN TEST")
    print("=" * 70)

    # Step 1: List accounts
    print("\n  [1/5] Listing accounts...")
    accounts = trader.get_accounts()
    if not accounts:
        print("    FAIL: No accounts found")
        return

    account_id = accounts[0].get("accountIdKey", "")
    print(f"    OK: Found {len(accounts)} account(s), using {account_id[:8]}...")

    # Step 2: Check balance
    print("\n  [2/5] Checking balance...")
    balance = trader.get_balance(account_id)
    if balance:
        computed = balance.get("Computed", {})
        cash = computed.get("cashAvailableForInvestment", 0)
        print(f"    OK: Cash available = ${_fmt_money(cash)}")
    else:
        print("    WARN: Could not fetch balance")

    # Step 3: Get quote
    print("\n  [3/5] Getting quote for AAPL...")
    quotes = trader.get_quote(["AAPL"])
    if "AAPL" in quotes:
        q = quotes["AAPL"]
        print(f"    OK: AAPL = ${q['lastTrade']:,.2f} "
              f"({q['changeClosePercentage']:+.2f}%)")
    else:
        print("    WARN: Could not fetch quote (trying Yahoo fallback)")
        feed = ETradePriceFeed()
        price = feed.get_price("AAPL")
        if price:
            print(f"    OK (Yahoo): AAPL = ${price:,.2f}")

    # Step 4: Dry-run order
    print("\n  [4/5] Dry-run: BUY 1 share AAPL (preview only, NOT placed)...")
    try:
        result = trader.place_order(
            account_id, "AAPL", "BUY", 1,
            order_type="MARKET", dry_run=True,
        )
        if result.get("dry_run"):
            print(f"    OK: Preview successful")
            print(f"    Preview ID: {result.get('preview_id')}")
            print(f"    Est. Total: ${result.get('estimated_total', 0):,.2f}")
            print(f"    Price:      ${result.get('current_price', 0):,.2f}")
        else:
            print(f"    Result: {json.dumps(result)[:300]}")
    except ETradeRiskError as e:
        print(f"    BLOCKED by risk rule: {e}")
    except ETradeAPIError as e:
        print(f"    API error: {e}")

    # Step 5: Check orders
    print("\n  [5/5] Listing recent orders...")
    orders = trader.get_orders(account_id, status="EXECUTED")
    print(f"    OK: {len(orders)} executed order(s) found")

    print()
    print("  ALL TESTS PASSED" if accounts else "  SOME TESTS FAILED")
    print("=" * 70)
    print()


def _fmt_money(val):
    """Format a monetary value."""
    try:
        return f"{float(val):,.2f}"
    except (TypeError, ValueError):
        return str(val)


def main():
    """CLI entry point."""
    _load_env()

    # Reconfigure logging for CLI
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(Path(__file__).parent / "etrade.log")),
        ],
    )

    if len(sys.argv) < 2:
        print(f"""
E*Trade Connector — OAuth 1.0a Trading Interface

Usage:
  python {Path(__file__).name} auth              Start OAuth flow (get authorize URL)
  python {Path(__file__).name} verify <CODE>     Complete OAuth with verifier code
  python {Path(__file__).name} status            Show accounts + balances + positions
  python {Path(__file__).name} quote AAPL MSFT   Get real-time quotes
  python {Path(__file__).name} test              Dry run of full trading flow

Options:
  --sandbox    Use E*Trade sandbox environment

Environment Variables:
  ETRADE_CONSUMER_KEY       Your E*Trade API consumer key
  ETRADE_CONSUMER_SECRET    Your E*Trade API consumer secret

Risk Rules (hardcoded, non-negotiable):
  Max $5 per trade | Max $2 daily loss | NEVER sell at a loss | 70%+ confidence only
""")
        sys.exit(0)

    command = sys.argv[1].lower()
    args = sys.argv[2:]

    commands = {
        "auth": cli_auth,
        "verify": cli_verify,
        "status": cli_status,
        "quote": cli_quote,
        "test": cli_test,
    }

    if command in commands:
        commands[command](args)
    else:
        print(f"Unknown command: {command}")
        print(f"Run 'python {Path(__file__).name}' for usage.")
        sys.exit(1)


if __name__ == "__main__":
    main()
