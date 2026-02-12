#!/usr/bin/env python3
"""Government Data Sources — JIT synchronized feeds from SEC, NIST, data.gov.

Just-In-Time Data: Only fetch when needed, cache with TTL, parse on demand.
RTOS-style deterministic scheduling ensures data freshness without waste.

Sources:
  1. SEC EDGAR — Corporate filings, insider trading (Forms 3/4/5), 13F holdings
  2. NIST — Time synchronization (NTP), cryptographic standards, CVE data
  3. data.gov — Treasury rates, economic indicators, FRED data

Each source has:
  - TTL (time-to-live): How long cached data is valid
  - Priority: CRITICAL (real-time), HIGH (minutes), NORMAL (hours)
  - Parser: Extracts trading-relevant signals from raw data

Usage:
    from gov_data import GovDataHub
    hub = GovDataHub()
    filings = hub.sec.get_recent_filings("AAPL")
    rates = hub.treasury.get_yield_curve()
    signals = hub.generate_signals()
"""

import json
import logging
import os
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import OrderedDict

logger = logging.getLogger("gov_data")

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

SEC_EDGAR_BASE = "https://efts.sec.gov/LATEST"
SEC_EDGAR_FILINGS = "https://efts.sec.gov/LATEST/search-index"
SEC_FULL_TEXT = "https://efts.sec.gov/LATEST/search-index"
SEC_COMPANY = "https://data.sec.gov/submissions"
DATA_GOV_TREASURY = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# User-Agent required by SEC EDGAR (they block generic UAs)
SEC_UA = os.environ.get("SEC_USER_AGENT", "NetTrace scott@nettrace.dev")


def _fetch(url, headers=None, timeout=15):
    """Fetch URL with proper headers."""
    _headers = {"User-Agent": SEC_UA, "Accept": "application/json"}
    if headers:
        _headers.update(headers)
    req = urllib.request.Request(url, headers=_headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode()


def _fetch_json(url, headers=None, timeout=15):
    """Fetch and parse JSON."""
    raw = _fetch(url, headers, timeout)
    return json.loads(raw)


class JITCache:
    """Just-In-Time cache with TTL and size limit.

    RTOS-style: Data is fetched on-demand, cached with deterministic TTL.
    Stale data is never served — either fresh or re-fetched.
    """

    def __init__(self, max_size=500):
        self._cache = OrderedDict()
        self._timestamps = {}
        self._ttls = {}
        self.max_size = max_size
        self.hits = 0
        self.misses = 0

    def get(self, key, ttl_seconds=300):
        """Get cached value if fresh, None if stale/missing."""
        if key in self._cache:
            age = time.time() - self._timestamps.get(key, 0)
            if age < ttl_seconds:
                self.hits += 1
                self._cache.move_to_end(key)
                return self._cache[key]
        self.misses += 1
        return None

    def put(self, key, value, ttl_seconds=300):
        """Store value with TTL."""
        if len(self._cache) >= self.max_size:
            self._cache.popitem(last=False)
        self._cache[key] = value
        self._timestamps[key] = time.time()
        self._ttls[key] = ttl_seconds

    @property
    def hit_rate(self):
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0


class SECDataSource:
    """SEC EDGAR data — filings, insider trades, institutional holdings."""

    # TTLs for different data types
    TTL_FILINGS = 300       # 5 min — new filings appear frequently
    TTL_INSIDER = 600       # 10 min — insider trades filed daily
    TTL_13F = 86400         # 24 hr — quarterly filings

    def __init__(self, cache):
        self.cache = cache

    def get_recent_filings(self, ticker=None, form_type=None, limit=20):
        """Get recent SEC filings, optionally filtered by ticker/form type.

        Args:
            ticker: Company ticker symbol (e.g., "AAPL")
            form_type: SEC form type (e.g., "10-K", "8-K", "4")
            limit: Max results

        Returns: List of filing dicts with date, form, company, url
        """
        cache_key = f"sec_filings:{ticker}:{form_type}:{limit}"
        cached = self.cache.get(cache_key, self.TTL_FILINGS)
        if cached is not None:
            return cached

        params = [f"dateRange=custom&startdt={(datetime.now()-timedelta(days=7)).strftime('%Y-%m-%d')}&enddt={datetime.now().strftime('%Y-%m-%d')}"]
        if ticker:
            params.append(f"q=%22{ticker}%22")
        if form_type:
            params.append(f"forms={form_type}")
        params.append(f"from=0&size={limit}")

        url = f"https://efts.sec.gov/LATEST/search-index?{('&').join(params)}"

        try:
            # SEC full-text search API
            search_url = (f"https://efts.sec.gov/LATEST/search-index?"
                         f"q={ticker or ''}&forms={form_type or ''}"
                         f"&dateRange=custom"
                         f"&startdt={(datetime.now()-timedelta(days=30)).strftime('%Y-%m-%d')}"
                         f"&enddt={datetime.now().strftime('%Y-%m-%d')}")
            data = _fetch_json(search_url)
            filings = []
            for hit in data.get("hits", {}).get("hits", [])[:limit]:
                src = hit.get("_source", {})
                filings.append({
                    "date": src.get("file_date", ""),
                    "form": src.get("form_type", ""),
                    "company": src.get("display_names", [""])[0] if src.get("display_names") else "",
                    "ticker": ticker,
                    "url": f"https://www.sec.gov/Archives/edgar/data/{src.get('entity_id', '')}",
                    "description": src.get("display_description", ""),
                })
            self.cache.put(cache_key, filings, self.TTL_FILINGS)
            return filings
        except Exception as e:
            logger.debug("SEC filings fetch failed: %s", e)
            # Fallback: use EDGAR full-text search REST API
            try:
                url = (f"https://efts.sec.gov/LATEST/search-index?"
                       f"q={ticker or ''}&forms={form_type or ''}")
                data = _fetch_json(url)
                return data.get("hits", {}).get("hits", [])[:limit]
            except Exception:
                return []

    def get_insider_trades(self, ticker, days=30):
        """Get insider trading (Form 4) filings for a company.

        Insider buys are BULLISH signals. Cluster insider buying is very bullish.
        Insider sells are less meaningful (execs sell for many reasons).
        """
        cache_key = f"sec_insider:{ticker}:{days}"
        cached = self.cache.get(cache_key, self.TTL_INSIDER)
        if cached is not None:
            return cached

        # Get CIK for the company
        try:
            tickers_url = "https://www.sec.gov/files/company_tickers.json"
            tickers_data = _fetch_json(tickers_url)
            cik = None
            for entry in tickers_data.values():
                if entry.get("ticker", "").upper() == ticker.upper():
                    cik = str(entry["cik_str"]).zfill(10)
                    break

            if not cik:
                return []

            # Get recent filings for this CIK
            url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            data = _fetch_json(url)
            recent = data.get("filings", {}).get("recent", {})

            forms = recent.get("form", [])
            dates = recent.get("filingDate", [])
            accessions = recent.get("accessionNumber", [])

            insider_trades = []
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

            for i, form in enumerate(forms):
                if form in ("3", "4", "5") and i < len(dates) and dates[i] >= cutoff:
                    insider_trades.append({
                        "form": form,
                        "date": dates[i],
                        "accession": accessions[i] if i < len(accessions) else "",
                        "type": "initial" if form == "3" else "change" if form == "4" else "annual",
                    })

            result = {
                "ticker": ticker,
                "cik": cik,
                "company": data.get("name", ""),
                "insider_filings": insider_trades[:20],
                "count": len(insider_trades),
                "signal": "BULLISH" if len(insider_trades) >= 3 else "NEUTRAL",
            }
            self.cache.put(cache_key, result, self.TTL_INSIDER)
            return result
        except Exception as e:
            logger.debug("Insider trades fetch failed for %s: %s", ticker, e)
            return {"ticker": ticker, "insider_filings": [], "count": 0, "signal": "UNKNOWN"}

    def get_13f_holdings(self, fund_cik):
        """Get 13F institutional holdings for a fund (hedge fund, mutual fund).

        13F filings show what big funds are buying/selling.
        Useful for following smart money.
        """
        cache_key = f"sec_13f:{fund_cik}"
        cached = self.cache.get(cache_key, self.TTL_13F)
        if cached is not None:
            return cached

        try:
            url = f"https://data.sec.gov/submissions/CIK{fund_cik.zfill(10)}.json"
            data = _fetch_json(url)
            recent = data.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            dates = recent.get("filingDate", [])

            holdings_filings = []
            for i, form in enumerate(forms):
                if "13F" in form and i < len(dates):
                    holdings_filings.append({
                        "form": form,
                        "date": dates[i],
                    })

            result = {
                "fund": data.get("name", ""),
                "cik": fund_cik,
                "filings_13f": holdings_filings[:4],  # Last 4 quarters
            }
            self.cache.put(cache_key, result, self.TTL_13F)
            return result
        except Exception as e:
            logger.debug("13F fetch failed: %s", e)
            return {}


class TreasuryDataSource:
    """Treasury yield curve + fiscal data from data.gov / Treasury APIs."""

    TTL_YIELDS = 3600       # 1 hr — yields update daily
    TTL_AUCTION = 86400     # 24 hr — auction results

    def __init__(self, cache):
        self.cache = cache

    def get_yield_curve(self, days=30):
        """Get Treasury yield curve (rates for 1M to 30Y).

        Yield curve inversion (2Y > 10Y) is a recession signal.
        Rapidly rising rates pressure risk assets (crypto).
        """
        cache_key = f"treasury_yields:{days}"
        cached = self.cache.get(cache_key, self.TTL_YIELDS)
        if cached is not None:
            return cached

        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        url = (f"{DATA_GOV_TREASURY}/v2/accounting/od/avg_interest_rates?"
               f"filter=record_date:gte:{start},record_date:lte:{end}"
               f"&sort=-record_date&page[size]=50"
               f"&fields=record_date,security_desc,avg_interest_rate_amt")

        try:
            data = _fetch_json(url)
            records = data.get("data", [])

            rates = {}
            for rec in records:
                date = rec.get("record_date", "")
                desc = rec.get("security_desc", "")
                rate = rec.get("avg_interest_rate_amt", "0")
                if date not in rates:
                    rates[date] = {}
                rates[date][desc] = float(rate) if rate else 0

            # Get most recent
            latest_date = max(rates.keys()) if rates else ""
            latest_rates = rates.get(latest_date, {})

            # Check for inversion
            short_rate = 0
            long_rate = 0
            for desc, rate in latest_rates.items():
                if "2-Year" in desc or "2 Year" in desc:
                    short_rate = rate
                if "10-Year" in desc or "10 Year" in desc:
                    long_rate = rate

            inverted = short_rate > long_rate and short_rate > 0 and long_rate > 0

            result = {
                "date": latest_date,
                "rates": latest_rates,
                "inverted": inverted,
                "spread_2y10y": round(long_rate - short_rate, 3) if short_rate and long_rate else None,
                "signal": "BEARISH" if inverted else "NEUTRAL",
                "history_dates": sorted(rates.keys())[-5:],
            }
            self.cache.put(cache_key, result, self.TTL_YIELDS)
            return result
        except Exception as e:
            logger.debug("Treasury yields fetch failed: %s", e)
            return {"rates": {}, "inverted": False, "signal": "UNKNOWN"}

    def get_treasury_auctions(self, security_type="Bill", limit=10):
        """Get recent Treasury auction results.

        High demand (high bid-to-cover ratio) = strong demand for safety = risk-off.
        """
        cache_key = f"treasury_auctions:{security_type}:{limit}"
        cached = self.cache.get(cache_key, self.TTL_AUCTION)
        if cached is not None:
            return cached

        url = (f"{DATA_GOV_TREASURY}/v1/accounting/od/auctions_query?"
               f"filter=security_type:eq:{security_type}"
               f"&sort=-auction_date&page[size]={limit}")

        try:
            data = _fetch_json(url)
            result = data.get("data", [])[:limit]
            self.cache.put(cache_key, result, self.TTL_AUCTION)
            return result
        except Exception as e:
            logger.debug("Treasury auctions fetch failed: %s", e)
            return []

    def get_national_debt(self):
        """Get current national debt — macro economic indicator."""
        cache_key = "treasury_debt"
        cached = self.cache.get(cache_key, 86400)
        if cached is not None:
            return cached

        url = (f"{DATA_GOV_TREASURY}/v2/accounting/od/debt_to_penny?"
               f"sort=-record_date&page[size]=5")

        try:
            data = _fetch_json(url)
            records = data.get("data", [])
            if records:
                latest = records[0]
                result = {
                    "date": latest.get("record_date", ""),
                    "total_debt": float(latest.get("tot_pub_debt_out_amt", 0)),
                    "intragov": float(latest.get("intragov_hold_amt", 0)),
                    "public": float(latest.get("debt_held_public_amt", 0)),
                }
                self.cache.put(cache_key, result, 86400)
                return result
        except Exception as e:
            logger.debug("Debt fetch failed: %s", e)
        return {}


class NISTDataSource:
    """NIST data — time sync, vulnerability data, standards.

    Primary use: CVE vulnerability data for crypto infrastructure security.
    Also useful for precise time synchronization validation.
    """

    TTL_CVE = 3600      # 1 hr — CVE database
    TTL_TIME = 60       # 1 min — time sync

    def __init__(self, cache):
        self.cache = cache

    def get_crypto_cves(self, keyword="blockchain", limit=10):
        """Get recent CVEs related to crypto/blockchain/DeFi.

        Security vulnerabilities in exchanges/wallets/protocols can cause
        sudden sell-offs. Early detection = early exit signal.
        """
        cache_key = f"nist_cve:{keyword}:{limit}"
        cached = self.cache.get(cache_key, self.TTL_CVE)
        if cached is not None:
            return cached

        # NIST NVD API v2
        url = (f"https://services.nvd.nist.gov/rest/json/cves/2.0?"
               f"keywordSearch={keyword}&resultsPerPage={limit}")

        try:
            data = _fetch_json(url, timeout=30)
            vulns = []
            for item in data.get("vulnerabilities", [])[:limit]:
                cve = item.get("cve", {})
                descriptions = cve.get("descriptions", [])
                desc = descriptions[0].get("value", "") if descriptions else ""
                metrics = cve.get("metrics", {})

                # Get CVSS score
                cvss_score = 0
                for version in ["cvssMetricV31", "cvssMetricV30", "cvssMetricV2"]:
                    if version in metrics:
                        score_data = metrics[version]
                        if score_data:
                            cvss_score = score_data[0].get("cvssData", {}).get("baseScore", 0)
                            break

                vulns.append({
                    "id": cve.get("id", ""),
                    "description": desc[:200],
                    "severity": "CRITICAL" if cvss_score >= 9 else
                               "HIGH" if cvss_score >= 7 else
                               "MEDIUM" if cvss_score >= 4 else "LOW",
                    "cvss_score": cvss_score,
                    "published": cve.get("published", ""),
                })

            result = {
                "keyword": keyword,
                "vulnerabilities": vulns,
                "count": len(vulns),
                "signal": "BEARISH" if any(v["severity"] == "CRITICAL" for v in vulns) else "NEUTRAL",
            }
            self.cache.put(cache_key, result, self.TTL_CVE)
            return result
        except Exception as e:
            logger.debug("NIST CVE fetch failed: %s", e)
            return {"keyword": keyword, "vulnerabilities": [], "count": 0, "signal": "UNKNOWN"}

    def check_time_drift(self):
        """Check local clock vs NIST time servers.

        Accurate time is critical for:
        - Order timestamp accuracy
        - Rate limit window tracking
        - Cross-exchange time sync for arb
        """
        cache_key = "nist_time"
        cached = self.cache.get(cache_key, self.TTL_TIME)
        if cached is not None:
            return cached

        local_time = time.time()

        try:
            # Use worldtimeapi as NIST NTP isn't HTTP
            url = "https://worldtimeapi.org/api/timezone/UTC"
            data = _fetch_json(url, timeout=5)
            server_epoch = data.get("unixtime", local_time)
            drift_ms = abs(local_time - server_epoch) * 1000

            result = {
                "local_epoch": local_time,
                "server_epoch": server_epoch,
                "drift_ms": round(drift_ms, 1),
                "acceptable": drift_ms < 1000,  # <1s is fine
                "signal": "WARNING" if drift_ms > 5000 else "OK",
            }
            self.cache.put(cache_key, result, self.TTL_TIME)
            return result
        except Exception as e:
            return {"drift_ms": 0, "acceptable": True, "signal": "UNKNOWN", "error": str(e)}


class GovDataHub:
    """Unified hub for all government data sources with JIT scheduling."""

    # RTOS-style priority scheduling
    SCHEDULE = {
        "sec_filings":    {"interval": 300,  "priority": "HIGH"},     # 5 min
        "insider_trades":  {"interval": 600,  "priority": "HIGH"},     # 10 min
        "yield_curve":     {"interval": 3600, "priority": "NORMAL"},   # 1 hr
        "crypto_cves":     {"interval": 3600, "priority": "NORMAL"},   # 1 hr
        "time_drift":      {"interval": 60,   "priority": "CRITICAL"}, # 1 min
        "national_debt":   {"interval": 86400, "priority": "LOW"},     # 24 hr
    }

    # Tickers to monitor for SEC filings
    WATCH_TICKERS = ["COIN", "MSTR", "RIOT", "MARA", "HOOD", "SQ", "PYPL"]

    def __init__(self):
        self.cache = JITCache(max_size=1000)
        self.sec = SECDataSource(self.cache)
        self.treasury = TreasuryDataSource(self.cache)
        self.nist = NISTDataSource(self.cache)
        self._last_run = {}

    def should_run(self, task_name):
        """RTOS-style: check if task is due based on its schedule."""
        schedule = self.SCHEDULE.get(task_name, {"interval": 3600})
        last = self._last_run.get(task_name, 0)
        return time.time() - last >= schedule["interval"]

    def mark_run(self, task_name):
        self._last_run[task_name] = time.time()

    def generate_signals(self):
        """Generate trading signals from all government data sources.

        Returns list of signal dicts, each with:
          - source, signal_type, direction (BULLISH/BEARISH/NEUTRAL),
            confidence, data, ttl
        """
        signals = []

        # SEC insider trading signals
        if self.should_run("insider_trades"):
            self.mark_run("insider_trades")
            for ticker in self.WATCH_TICKERS:
                try:
                    insider = self.sec.get_insider_trades(ticker, days=14)
                    if insider.get("count", 0) >= 3:
                        signals.append({
                            "source": "sec_edgar",
                            "signal_type": "insider_cluster_buy",
                            "ticker": ticker,
                            "direction": insider.get("signal", "NEUTRAL"),
                            "confidence": min(0.6 + insider["count"] * 0.05, 0.85),
                            "data": {"filings": insider["count"]},
                            "ttl": 600,
                        })
                except Exception:
                    pass

        # Treasury yield curve inversion
        if self.should_run("yield_curve"):
            self.mark_run("yield_curve")
            try:
                yields = self.treasury.get_yield_curve()
                if yields.get("inverted"):
                    signals.append({
                        "source": "treasury",
                        "signal_type": "yield_curve_inversion",
                        "direction": "BEARISH",
                        "confidence": 0.70,
                        "data": {"spread": yields.get("spread_2y10y")},
                        "ttl": 3600,
                    })
            except Exception:
                pass

        # Crypto CVE signals
        if self.should_run("crypto_cves"):
            self.mark_run("crypto_cves")
            for keyword in ["blockchain", "cryptocurrency", "defi"]:
                try:
                    cves = self.nist.get_crypto_cves(keyword, limit=5)
                    critical = [v for v in cves.get("vulnerabilities", [])
                               if v.get("severity") == "CRITICAL"]
                    if critical:
                        signals.append({
                            "source": "nist_nvd",
                            "signal_type": "crypto_vulnerability",
                            "direction": "BEARISH",
                            "confidence": 0.55,
                            "data": {"critical_cves": len(critical),
                                     "ids": [c["id"] for c in critical]},
                            "ttl": 3600,
                        })
                except Exception:
                    pass

        # Time drift warning
        if self.should_run("time_drift"):
            self.mark_run("time_drift")
            try:
                drift = self.nist.check_time_drift()
                if drift.get("drift_ms", 0) > 5000:
                    signals.append({
                        "source": "nist_time",
                        "signal_type": "clock_drift_warning",
                        "direction": "NEUTRAL",
                        "confidence": 1.0,
                        "data": {"drift_ms": drift["drift_ms"]},
                        "ttl": 60,
                    })
            except Exception:
                pass

        return signals

    def get_status(self):
        """Get hub status including cache stats."""
        return {
            "cache_hits": self.cache.hits,
            "cache_misses": self.cache.misses,
            "cache_hit_rate": round(self.cache.hit_rate * 100, 1),
            "cache_size": len(self.cache._cache),
            "last_runs": {k: datetime.fromtimestamp(v).isoformat()
                         for k, v in self._last_run.items()},
            "schedule": self.SCHEDULE,
        }


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [GOV] %(levelname)s %(message)s")

    hub = GovDataHub()

    if len(sys.argv) > 1 and sys.argv[1] == "signals":
        print("\nGenerating government data signals...")
        signals = hub.generate_signals()
        if signals:
            for s in signals:
                print(f"  [{s['source']}] {s['signal_type']}: {s['direction']} "
                      f"(conf={s['confidence']:.0%}) {s.get('data', {})}")
        else:
            print("  No actionable signals at this time.")
        status = hub.get_status()
        print(f"\n  Cache: {status['cache_hits']} hits, {status['cache_misses']} misses "
              f"({status['cache_hit_rate']}% hit rate)")

    elif len(sys.argv) > 1 and sys.argv[1] == "sec":
        ticker = sys.argv[2] if len(sys.argv) > 2 else "COIN"
        print(f"\nSEC insider trades for {ticker}...")
        insider = hub.sec.get_insider_trades(ticker)
        print(f"  Company: {insider.get('company', '?')}")
        print(f"  Insider filings (30d): {insider.get('count', 0)}")
        print(f"  Signal: {insider.get('signal', '?')}")
        for f in insider.get("insider_filings", [])[:5]:
            print(f"    Form {f['form']} on {f['date']} ({f['type']})")

    elif len(sys.argv) > 1 and sys.argv[1] == "yields":
        print("\nTreasury yield curve...")
        yields = hub.treasury.get_yield_curve()
        print(f"  Date: {yields.get('date', '?')}")
        print(f"  Inverted: {yields.get('inverted', '?')}")
        print(f"  2Y-10Y spread: {yields.get('spread_2y10y', '?')}")
        for desc, rate in sorted(yields.get("rates", {}).items()):
            print(f"    {desc}: {rate}%")

    elif len(sys.argv) > 1 and sys.argv[1] == "cves":
        keyword = sys.argv[2] if len(sys.argv) > 2 else "blockchain"
        print(f"\nCrypto CVEs for '{keyword}'...")
        cves = hub.nist.get_crypto_cves(keyword)
        print(f"  Found: {cves.get('count', 0)}")
        for v in cves.get("vulnerabilities", []):
            print(f"    {v['id']} [{v['severity']}] {v['description'][:80]}...")

    elif len(sys.argv) > 1 and sys.argv[1] == "time":
        print("\nTime drift check...")
        drift = hub.nist.check_time_drift()
        print(f"  Drift: {drift.get('drift_ms', '?')}ms")
        print(f"  Acceptable: {drift.get('acceptable', '?')}")

    else:
        print("Usage:")
        print("  python gov_data.py signals       # Generate trading signals")
        print("  python gov_data.py sec [TICKER]   # SEC insider trades")
        print("  python gov_data.py yields         # Treasury yield curve")
        print("  python gov_data.py cves [keyword]  # Crypto vulnerabilities")
        print("  python gov_data.py time           # Clock drift check")
