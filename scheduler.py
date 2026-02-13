"""ContinuousScanner - Background scan loop for NetTrace data engine."""

import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from quant_engine import QuantEngine

logger = logging.getLogger("nettrace.scanner")

DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent / "traceroute.db"))
TARGETS_PATH = Path(__file__).parent / "targets.json"
SCAN_INTERVAL = int(os.environ.get("SCAN_INTERVAL", "900"))  # 15 minutes default
SCAN_STAGGER = int(os.environ.get("SCAN_STAGGER", "10"))  # seconds between scans
CRYPTO_SCAN_INTERVAL = int(os.environ.get("CRYPTO_SCAN_INTERVAL", "120"))  # 2 min for crypto exchanges
SNAPSHOT_EVERY = 4  # store full snapshot every N scans per target

# Priority categories — these generate the trading edge
PRIORITY_CATEGORIES = {"Crypto Exchanges", "Forex & Brokers"}

# Region-aware exchange priorities: boost scan frequency for nearby exchanges
# Each region scans ALL targets but runs nearby exchanges MORE OFTEN
REGION_PRIORITY_EXCHANGES = {
    "ewr": {"coinbase.com", "gemini.com", "kraken.com", "api.exchange.coinbase.com"},
    "lhr": {"bitstamp.net", "api.kraken.com"},  # EU exchanges
    "nrt": {"bitflyer.com", "api.liquid.com"},   # Japanese exchanges
    "sin": {"api.binance.com", "api.bybit.com", "www.okx.com"},  # SE Asian exchanges
    "ord": {"cme.com"},
    "fra": {"eurex.com"},
    "bom": {"dgcx.ae"},
}

FLY_REGION = os.environ.get("FLY_REGION", "local")

# Geo cache shared with app.py (loaded from DB on startup)
GEO_CACHE = {}
GEO_LOCK = threading.Lock()


class ContinuousScanner:
    """Daemon thread that continuously scans all targets."""

    def __init__(self, socketio=None):
        self.socketio = socketio
        self.running = False
        self.thread = None
        self.scan_counts = {}  # target_host -> scan count since last snapshot
        self.quant_engine = QuantEngine(db_path=DB_PATH)
        self._load_geo_cache()

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True, name="scanner")
        self.thread.start()
        # Start priority crypto scanner — 3x faster for trading-critical targets
        self._crypto_thread = threading.Thread(target=self._crypto_loop, daemon=True, name="crypto_scanner")
        self._crypto_thread.start()
        logger.info("ContinuousScanner started (full=%ds, crypto=%ds, stagger=%ds)",
                     SCAN_INTERVAL, CRYPTO_SCAN_INTERVAL, SCAN_STAGGER)

    def stop(self):
        self.running = False

    def _load_geo_cache(self):
        """Load persistent geo cache from DB."""
        try:
            db = sqlite3.connect(DB_PATH)
            db.row_factory = sqlite3.Row
            rows = db.execute("SELECT ip, geo_json FROM ip_geo_cache").fetchall()
            db.close()
            with GEO_LOCK:
                for row in rows:
                    try:
                        GEO_CACHE[row["ip"]] = json.loads(row["geo_json"])
                    except (json.JSONDecodeError, KeyError):
                        pass
            logger.info("Loaded %d entries from ip_geo_cache", len(GEO_CACHE))
        except sqlite3.OperationalError:
            logger.info("ip_geo_cache table not ready yet, starting with empty cache")

    def _save_geo(self, ip, geo_data):
        """Persist a geo lookup to DB cache."""
        try:
            db = sqlite3.connect(DB_PATH)
            db.execute(
                "INSERT OR REPLACE INTO ip_geo_cache (ip, geo_json, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                (ip, json.dumps(geo_data))
            )
            db.commit()
            db.close()
        except Exception as e:
            logger.debug("Failed to persist geo for %s: %s", ip, e)

    def _load_targets(self):
        """Load targets from targets.json."""
        if not TARGETS_PATH.exists():
            logger.warning("targets.json not found at %s", TARGETS_PATH)
            return []
        with open(TARGETS_PATH) as f:
            data = json.load(f)
        targets = []
        for category, entries in data.items():
            for entry in entries:
                targets.append({
                    "name": entry["name"],
                    "host": entry["host"],
                    "category": category
                })
        return targets

    def _run_traceroute(self, host, max_hops=20):
        """Run traceroute subprocess, return parsed hops."""
        try:
            result = subprocess.run(
                ["traceroute", "-m", str(max_hops), "-q", "1", "-w", "2", host],
                capture_output=True, text=True, timeout=60
            )
            output = result.stdout
        except subprocess.TimeoutExpired:
            return []
        except FileNotFoundError:
            return []

        hops = []
        for line in output.strip().split("\n")[1:]:
            line = line.strip()
            if not line:
                continue
            m = re.match(r'\s*(\d+)\s+(\S+)\s+\((\d+\.\d+\.\d+\.\d+)\)\s+([\d.]+)\s*ms', line)
            if m:
                hops.append({"hop": int(m.group(1)), "host": m.group(2),
                             "ip": m.group(3), "rtt_ms": float(m.group(4))})
            else:
                m2 = re.match(r'\s*(\d+)\s+(\d+\.\d+\.\d+\.\d+)\s+([\d.]+)\s*ms', line)
                if m2:
                    hops.append({"hop": int(m2.group(1)), "host": m2.group(2),
                                 "ip": m2.group(2), "rtt_ms": float(m2.group(3))})
                else:
                    m3 = re.match(r'\s*(\d+)\s+\*', line)
                    if m3:
                        hops.append({"hop": int(m3.group(1)), "host": "*",
                                     "ip": None, "rtt_ms": None})
        return hops

    def _geolocate_ip(self, ip):
        """Geolocate an IP, using cache first."""
        if not ip:
            return None
        for prefix in ("10.", "192.168.", "172.16.", "172.17.", "172.18.",
                        "172.19.", "172.2", "172.30.", "172.31.", "127."):
            if ip.startswith(prefix):
                return None
        with GEO_LOCK:
            if ip in GEO_CACHE:
                return GEO_CACHE[ip]
        try:
            url = f"http://ip-api.com/json/{ip}?fields=status,country,regionName,city,lat,lon,isp,org,as"
            req = urllib.request.Request(url, headers={"User-Agent": "NetTrace/1.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
            if data.get("status") == "success":
                geo = {
                    "lat": data["lat"], "lon": data["lon"],
                    "city": data.get("city", ""), "region": data.get("regionName", ""),
                    "country": data.get("country", ""),
                    "isp": data.get("isp", ""), "org": data.get("org", ""),
                    "as": data.get("as", "")
                }
                with GEO_LOCK:
                    GEO_CACHE[ip] = geo
                self._save_geo(ip, geo)
                return geo
        except Exception:
            pass
        return None

    def _compute_route_hash(self, hops):
        """SHA256 of the IP sequence for route change detection."""
        ip_seq = "|".join(h.get("ip", "*") or "*" for h in hops)
        return hashlib.sha256(ip_seq.encode()).hexdigest()

    def _sanitize_hops(self, hops, sanitize_first_n=3):
        """Replace first N hops with sanitized values."""
        for h in hops:
            if h["hop"] <= sanitize_first_n and h.get("ip"):
                h["ip"] = f"10.0.0.{h['hop']}"
                h["host"] = f"hop-{h['hop']}.local"
                if "geo" in h:
                    h["geo"] = None
        return hops

    def _scan_target(self, target):
        """Scan a single target and record metrics."""
        host = target["host"]
        name = target["name"]
        category = target["category"]

        hops = self._run_traceroute(host)
        if not hops:
            return

        # Geolocate IPs (with rate limiting)
        for h in hops:
            if h.get("ip"):
                h["geo"] = self._geolocate_ip(h["ip"])
                time.sleep(0.1)  # rate limit geo API
            else:
                h["geo"] = None

        # Sanitize first hops
        hops = self._sanitize_hops(hops)

        # Compute metrics
        total_rtt = None
        for h in reversed(hops):
            if h.get("rtt_ms"):
                total_rtt = h["rtt_ms"]
                break

        first_hop_rtt = None
        for h in hops:
            if h.get("rtt_ms"):
                first_hop_rtt = h["rtt_ms"]
                break

        last_hop_rtt = total_rtt
        hop_count = len(hops)
        route_hash = self._compute_route_hash(hops)

        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        try:
            # Insert scan_metrics
            cur = db.execute(
                """INSERT INTO scan_metrics (target_host, target_name, category, total_rtt,
                   hop_count, first_hop_rtt, last_hop_rtt, route_hash, scan_source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'auto')""",
                (host, name, category, total_rtt, hop_count, first_hop_rtt, last_hop_rtt, route_hash)
            )
            metric_id = cur.lastrowid

            # Track scan count for snapshot decisions
            self.scan_counts[host] = self.scan_counts.get(host, 0) + 1

            # Check for route change
            prev = db.execute(
                "SELECT route_hash, id FROM scan_metrics WHERE target_host = ? AND id < ? ORDER BY id DESC LIMIT 1",
                (host, metric_id)
            ).fetchone()

            route_changed = prev and prev["route_hash"] != route_hash
            should_snapshot = route_changed or (self.scan_counts[host] % SNAPSHOT_EVERY == 0)

            if route_changed:
                rtt_delta = None
                if total_rtt and prev:
                    prev_rtt_row = db.execute(
                        "SELECT total_rtt FROM scan_metrics WHERE id = ?", (prev["id"],)
                    ).fetchone()
                    if prev_rtt_row and prev_rtt_row["total_rtt"]:
                        rtt_delta = total_rtt - prev_rtt_row["total_rtt"]

                db.execute(
                    """INSERT INTO route_changes (target_host, target_name, old_route_hash,
                       new_route_hash, new_hops_json, rtt_delta)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (host, name, prev["route_hash"], route_hash,
                     json.dumps([{"hop": h["hop"], "ip": h.get("ip"), "host": h.get("host"),
                                  "rtt_ms": h.get("rtt_ms")} for h in hops]),
                     rtt_delta)
                )
                logger.info("Route change detected for %s (%s) rtt_delta=%.1f",
                            name, host, rtt_delta or 0)

            if should_snapshot:
                db.execute(
                    "INSERT INTO scan_snapshots (metric_id, hops_json) VALUES (?, ?)",
                    (metric_id, json.dumps([{
                        "hop": h["hop"], "ip": h.get("ip"), "host": h.get("host"),
                        "rtt_ms": h.get("rtt_ms"),
                        "geo": h.get("geo")
                    } for h in hops]))
                )

            db.commit()

            # Emit real-time update via WebSocket
            if self.socketio:
                self.socketio.emit("latency_update", {
                    "host": host,
                    "name": name,
                    "category": category,
                    "total_rtt": total_rtt,
                    "hop_count": hop_count,
                    "route_hash": route_hash,
                    "route_changed": route_changed,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }, namespace="/api/v1/stream")

        except Exception as e:
            logger.error("Error recording metrics for %s: %s", host, e)
        finally:
            db.close()

    def _loop(self):
        """Main scan loop."""
        logger.info("Scanner loop starting...")
        while self.running:
            try:
                targets = self._load_targets()
                if not targets:
                    logger.warning("No targets loaded, sleeping...")
                    time.sleep(60)
                    continue

                cycle_start = time.time()
                logger.info("Starting scan cycle: %d targets", len(targets))

                for target in targets:
                    if not self.running:
                        break
                    try:
                        self._scan_target(target)
                    except Exception as e:
                        logger.error("Error scanning %s: %s", target["host"], e)
                    time.sleep(SCAN_STAGGER)

                cycle_duration = time.time() - cycle_start
                logger.info("Scan cycle complete in %.0fs", cycle_duration)

                # Run quant signal generation after each full cycle.
                self._run_quant_engine()

                # Run daily cleanup once per day
                self._maybe_cleanup()

                # Sleep until next cycle
                remaining = max(0, SCAN_INTERVAL - cycle_duration)
                if remaining > 0 and self.running:
                    logger.info("Sleeping %.0fs until next cycle", remaining)
                    # Sleep in small increments to allow clean shutdown
                    end_time = time.time() + remaining
                    while self.running and time.time() < end_time:
                        time.sleep(min(5, end_time - time.time()))

            except Exception as e:
                logger.error("Scanner loop error: %s", e)
                time.sleep(30)

    def _crypto_loop(self):
        """Fast-path scanner for trading-critical targets (crypto exchanges, forex brokers).

        Runs every CRYPTO_SCAN_INTERVAL (2 min default) — separate from the full
        15-minute scan cycle. This is the core trading edge: higher-frequency latency
        data from 7 global regions = faster anomaly detection = more money.

        Each scan from a different Fly region sees different network paths.
        Cross-region latency divergence is the private signal competitors don't have.
        """
        logger.info("Crypto priority scanner starting (interval=%ds)", CRYPTO_SCAN_INTERVAL)
        time.sleep(15)  # stagger startup from main loop

        while self.running:
            try:
                targets = self._load_targets()
                priority_targets = [t for t in targets if t.get("category") in PRIORITY_CATEGORIES]

                if not priority_targets:
                    time.sleep(60)
                    continue

                # Region-aware: sort nearby exchanges first for lower latency scans
                region_hosts = REGION_PRIORITY_EXCHANGES.get(FLY_REGION, set())
                if region_hosts:
                    nearby = [t for t in priority_targets if t["host"] in region_hosts]
                    other = [t for t in priority_targets if t["host"] not in region_hosts]
                    priority_targets = nearby + other
                    if nearby:
                        logger.info("Region %s: %d nearby exchanges prioritized",
                                    FLY_REGION, len(nearby))

                cycle_start = time.time()
                logger.info("Crypto priority scan: %d targets (region=%s)",
                            len(priority_targets), FLY_REGION)

                for target in priority_targets:
                    if not self.running:
                        break
                    try:
                        self._scan_target(target)
                    except Exception as e:
                        logger.error("Crypto scan error %s: %s", target["host"], e)
                    time.sleep(max(2, SCAN_STAGGER // 2))  # tighter stagger for priority

                cycle_duration = time.time() - cycle_start
                logger.info("Crypto priority scan complete in %.0fs (%d targets)",
                             cycle_duration, len(priority_targets))

                # Generate signals immediately after crypto scan — don't wait for full cycle
                self._run_quant_engine()

                # Sleep until next crypto cycle
                remaining = max(0, CRYPTO_SCAN_INTERVAL - cycle_duration)
                if remaining > 0 and self.running:
                    end_time = time.time() + remaining
                    while self.running and time.time() < end_time:
                        time.sleep(min(5, end_time - time.time()))

            except Exception as e:
                logger.error("Crypto scanner error: %s", e)
                time.sleep(30)

    def _run_quant_engine(self):
        """Generate and persist quant signals for the latest scan cycle."""
        try:
            signals = self.quant_engine.run(emit_stdout=False)
            if signals:
                logger.info("Quant engine generated %d signal(s)", len(signals))
        except Exception as e:
            logger.error("Quant engine error: %s", e)

    def _maybe_cleanup(self):
        """Run daily cleanup: prune old data."""
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        try:
            # Serialize guard + cleanup so concurrent schedulers cannot run it twice per UTC day.
            db.execute("BEGIN IMMEDIATE")
            db.execute("""
                CREATE TABLE IF NOT EXISTS scheduler_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            today_utc = db.execute("SELECT date('now') AS day_utc").fetchone()["day_utc"]
            marker = db.execute(
                "SELECT value FROM scheduler_metadata WHERE key = 'last_cleanup_utc_day'"
            ).fetchone()
            if marker and marker["value"] == today_utc:
                db.commit()
                return

            # Use SQLite datetime math so retention comparisons match DB timestamp format.
            r1 = db.execute("DELETE FROM scan_metrics WHERE created_at < datetime('now', '-90 days')")
            r2 = db.execute("DELETE FROM scan_snapshots WHERE created_at < datetime('now', '-30 days')")
            r3 = db.execute("DELETE FROM route_changes WHERE detected_at < datetime('now', '-180 days')")
            r4 = db.execute("DELETE FROM api_usage WHERE created_at < datetime('now', '-7 days')")

            db.execute(
                """INSERT OR REPLACE INTO scheduler_metadata (key, value, updated_at)
                   VALUES ('last_cleanup_utc_day', ?, CURRENT_TIMESTAMP)""",
                (today_utc,)
            )

            db.commit()
            logger.info(
                "Cleanup complete (day=%s): metrics=%d, snapshots=%d, routes=%d, usage=%d rows deleted",
                today_utc, r1.rowcount, r2.rowcount, r3.rowcount, r4.rowcount
            )
        except Exception as e:
            try:
                db.rollback()
            except sqlite3.Error:
                pass
            logger.error("Cleanup error: %s", e)
        finally:
            db.close()
