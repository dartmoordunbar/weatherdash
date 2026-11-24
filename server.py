#!/usr/bin/env python3
"""
WeatherDash: a live dashboard and recorder for a Davis WeatherLink Live
(or WeatherLink Console) on your local network.

  * polls /v1/current_conditions every 10 s and stores every reading in SQLite
  * listens to the 2.5 s UDP broadcast for live wind and rain
  * pushes updates to the browser with Server-Sent Events
  * finds gaps in the record (station unreachable, this program not running)
    and backfills them from the WeatherLink.com v2 API, when keys are given

Standard library only. Python 3.8+.
"""
import argparse
import csv
import io
import json
import logging
import os
import queue
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

log = logging.getLogger("weatherdash")
HERE = Path(__file__).resolve().parent

# ---------------------------------------------------------------- units
RAIN_MM = {1: 0.254, 2: 0.2, 3: 0.1, 4: 0.0254}   # Davis rain_size codes -> mm per tip


def f2c(v):
    return None if v is None else round((v - 32) * 5 / 9, 2)


def mph2kmh(v):
    return None if v is None else round(v * 1.609344, 2)


def inhg2hpa(v):
    return None if v is None else round(v * 33.8638866667, 2)


def in2mm(v):
    return None if v is None else round(v * 25.4, 3)


def same(v):
    return v


# Columns stored for every reading. Everything is metric.
COLUMNS = ["temp_c", "feels_c", "hum", "dew_c", "wind_kmh", "wind_dir", "gust_kmh",
           "rain_mm", "rain_rate_mmh", "bar_hpa", "solar", "uv", "in_temp_c", "in_hum"]


# ---------------------------------------------------------------- storage
class DB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        cols = ", ".join(f"{c} REAL" for c in COLUMNS)
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS readings(
                    ts INTEGER PRIMARY KEY, source TEXT NOT NULL, {cols});
                CREATE INDEX IF NOT EXISTS readings_source_ts ON readings(source, ts);
                CREATE TABLE IF NOT EXISTS gaps(
                    start INTEGER, end INTEGER, status TEXT, rows INTEGER DEFAULT 0,
                    detail TEXT, tried_at INTEGER, PRIMARY KEY(start, end));
                CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY, value TEXT);
            """)

    def q(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def one(self, sql, args=()):
        rows = self.q(sql, args)
        return rows[0] if rows else None

    def run(self, sql, args=()):
        with self.lock:
            return self.conn.execute(sql, args).rowcount

    def insert(self, rec, source):
        cols = ["ts", "source"] + COLUMNS
        vals = [int(rec["ts"]), source] + [rec.get(c) for c in COLUMNS]
        return self.run(f"INSERT OR IGNORE INTO readings({','.join(cols)}) "
                        f"VALUES({','.join('?' * len(cols))})", vals)

    def get_state(self, key, default=None):
        row = self.one("SELECT value FROM state WHERE key=?", (key,))
        return json.loads(row["value"]) if row else default

    def set_state(self, key, value):
        self.run("INSERT OR REPLACE INTO state(key, value) VALUES(?, ?)", (key, json.dumps(value)))


# ---------------------------------------------------------------- http helpers
def http_json(url, headers=None, timeout=8):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


# ---------------------------------------------------------------- LAN parsing
def pick_iss(conditions, txid, dst):
    iss = [c for c in conditions if c.get("data_structure_type") == dst]
    if txid:
        iss = [c for c in iss if c.get("txid") == txid] or iss
    return iss[0] if iss else None


def parse_current(doc, txid=None):
    """Turn a /v1/current_conditions document into one metric record."""
    d = doc.get("data") or {}
    conds = d.get("conditions") or []
    rec = {"ts": int(d.get("ts") or time.time())}
    for c in conds:
        t = c.get("data_structure_type")
        if t == 3:
            rec["bar_hpa"] = inhg2hpa(c.get("bar_sea_level"))
            rec["bar_trend_hpa"] = inhg2hpa(c.get("bar_trend"))
        elif t == 4:
            rec["in_temp_c"] = f2c(c.get("temp_in"))
            rec["in_hum"] = c.get("hum_in")
    iss = pick_iss(conds, txid, 1)
    if iss:
        mm = RAIN_MM.get(iss.get("rain_size"), 0.2)
        daily = iss.get("rainfall_daily")
        rec.update(
            temp_c=f2c(iss.get("temp")),
            feels_c=f2c(iss.get("thw_index") if iss.get("thw_index") is not None else iss.get("wind_chill")),
            hum=iss.get("hum"),
            dew_c=f2c(iss.get("dew_point")),
            wind_kmh=mph2kmh(iss.get("wind_speed_avg_last_1_min")),
            wind_dir=iss.get("wind_dir_scalar_avg_last_1_min"),
            gust_kmh=mph2kmh(iss.get("wind_speed_hi_last_10_min")),
            rain_rate_mmh=round((iss.get("rain_rate_last") or 0) * mm, 2),
            solar=iss.get("solar_rad"),
            uv=iss.get("uv_index"),
            # live-only fields, not stored as columns
            rain_day_mm=None if daily is None else round(daily * mm, 2),
            battery_low=bool(iss.get("trans_battery_flag")),
            rx_state=iss.get("rx_state"),
        )
    return rec


def parse_udp(doc, txid=None):
    iss = pick_iss(doc.get("conditions") or [], txid, 1)
    if not iss:
        return {}
    mm = RAIN_MM.get(iss.get("rain_size"), 0.2)
    out = {
        "live_ts": doc.get("ts") or int(time.time()),
        "live_wind_kmh": mph2kmh(iss.get("wind_speed_last")),
        "live_wind_dir": iss.get("wind_dir_last"),
        "rain_rate_mmh": round((iss.get("rain_rate_last") or 0) * mm, 2),
    }
    if iss.get("rainfall_daily") is not None:
        out["rain_day_mm"] = round(iss["rainfall_daily"] * mm, 2)
    if iss.get("wind_speed_hi_last_10_min") is not None:
        out["gust_kmh"] = mph2kmh(iss["wind_speed_hi_last_10_min"])
    return out


# ---------------------------------------------------------------- cloud (WeatherLink v2)
ISS_ARCHIVE_TYPES = {11, 24}    # WLL ISS archive, Console ISS archive
CLOUD_ISS = {
    "temp_c": [("temp_avg", f2c), ("temp_last", f2c)],
    "hum": [("hum_last", same)],
    "dew_c": [("dew_point_last", f2c)],
    "feels_c": [("thw_index_last", f2c), ("wind_chill_last", f2c)],
    "wind_kmh": [("wind_speed_avg", mph2kmh)],
    "gust_kmh": [("wind_speed_hi", mph2kmh)],
    "wind_dir": [("wind_dir_of_prevail", same), ("wind_speed_hi_dir", same)],
    "rain_mm": [("rainfall_mm", same), ("rainfall_in", in2mm)],
    "rain_rate_mmh": [("rain_rate_hi_mm", same), ("rain_rate_hi_in", in2mm)],
    "solar": [("solar_rad_avg", same)],
    "uv": [("uv_index_avg", same)],
}
CLOUD_OTHER = {
    "bar_hpa": [("bar_sea_level", inhg2hpa)],
    "in_temp_c": [("temp_in_last", f2c), ("temp_in_avg", f2c)],
    "in_hum": [("hum_in_last", same)],
}


class CloudError(Exception):
    pass


class Cloud:
    def __init__(self, key, secret, station_id=None, base=None):
        self.key, self.secret, self.station_id = key, secret, station_id
        self.base = (base or os.environ.get("WL_API_BASE") or "https://api.weatherlink.com/v2").rstrip("/")

    def get(self, path, **params):
        params["api-key"] = self.key
        url = f"{self.base}{path}?{urllib.parse.urlencode(params)}"
        try:
            return http_json(url, headers={"X-Api-Secret": self.secret}, timeout=30)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:300]
            if e.code in (401, 403):
                raise CloudError(f"WeatherLink.com refused historic data ({e.code}). "
                                 "Historic data needs a Pro or Pro+ subscription for this station. " + body)
            raise CloudError(f"WeatherLink.com error {e.code}: {body}")

    def station(self):
        if not self.station_id:
            stations = self.get("/stations").get("stations") or []
            if not stations:
                raise CloudError("No stations on this WeatherLink.com account")
            self.station_id = stations[0]["station_id"]
            log.info("cloud: using station %s (%s)", self.station_id, stations[0].get("station_name"))
        return self.station_id

    def historic(self, start, end):
        """Archive records with start < ts < end, merged across sensors, metric."""
        merged = {}
        t = start
        while t < end:
            chunk_end = min(t + 86400, end)    # the API serves at most 24 h per call
            doc = self.get(f"/historic/{self.station()}",
                           **{"start-timestamp": t, "end-timestamp": chunk_end})
            for sensor in doc.get("sensors") or []:
                is_iss = sensor.get("data_structure_type") in ISS_ARCHIVE_TYPES
                table = CLOUD_ISS if is_iss else CLOUD_OTHER
                for d in sensor.get("data") or []:
                    ts = d.get("ts")
                    if ts is None or not (start < ts < end):
                        continue
                    rec = merged.setdefault(ts, {"ts": ts})
                    for col, options in table.items():
                        if rec.get(col) is not None:
                            continue
                        for field, conv in options:
                            if d.get(field) is not None:
                                rec[col] = conv(d[field])
                                break
            t = chunk_end
        return [merged[k] for k in sorted(merged)]


# ---------------------------------------------------------------- live fan-out
class Hub:
    def __init__(self):
        self.clients, self.lock = set(), threading.Lock()

    def add(self):
        q = queue.Queue(maxsize=100)
        with self.lock:
            self.clients.add(q)
        return q

    def remove(self, q):
        with self.lock:
            self.clients.discard(q)

    def broadcast(self, msg):
        with self.lock:
            clients = list(self.clients)
        for q in clients:
            try:
                q.put_nowait(msg)
            except queue.Full:
                pass


# ---------------------------------------------------------------- the station
class Station:
    def __init__(self, cfg, db, hub, cloud):
        self.cfg, self.db, self.hub, self.cloud = cfg, db, hub, cloud
        self.current = {}
        self.today = {}
        self.online = False
        self.last_ok = None
        self.down_since = None
        self.last_error = None
        self.backfill = {"state": "idle" if cloud else "off"}
        self.lock = threading.Lock()
        self._last_pub = 0

    # ---- snapshot sent to browsers
    def snapshot(self):
        with self.lock:
            return {
                "now": int(time.time()), "name": self.cfg.name,
                "online": self.online, "last_ok": self.last_ok, "down_since": self.down_since,
                "error": self.last_error, "current": dict(self.current), "today": dict(self.today),
                "backfill": dict(self.backfill), "cloud": bool(self.cloud),
                "poll_seconds": self.cfg.poll,
            }

    def publish(self, force=True):
        if not force and time.time() - self._last_pub < 1:
            return
        self._last_pub = time.time()
        self.hub.broadcast(json.dumps(self.snapshot()))

    # ---- polling
    def poll_forever(self):
        url = f"http://{self.cfg.host}/v1/current_conditions"
        while True:
            started = time.time()
            try:
                doc = http_json(url, timeout=5)
                if doc.get("error"):
                    raise RuntimeError(doc["error"])
                rec = parse_current(doc, self.cfg.txid)
                self.store(rec)
                with self.lock:
                    if not self.online:
                        log.info("station reachable at %s", self.cfg.host)
                    self.current.update({k: v for k, v in rec.items()})
                    self.online, self.last_ok, self.down_since, self.last_error = True, time.time(), None, None
                self.refresh_today()
            except Exception as e:  # noqa: BLE001 - any failure means "offline"
                with self.lock:
                    if self.online or self.down_since is None:
                        log.warning("station not reachable: %s", e)
                        self.down_since = self.down_since or int(time.time())
                    self.online, self.last_error = False, str(e)
            self.publish()
            time.sleep(max(1.0, self.cfg.poll - (time.time() - started)))

    def store(self, rec):
        # Rain: the station gives a daily total; we store the increment since the last stored reading.
        day_mm = rec.get("rain_day_mm")
        if day_mm is not None:
            today = datetime.now().strftime("%Y-%m-%d")
            last = self.db.get_state("rain_day", None)
            if last and last["date"] == today and day_mm >= last["mm"]:
                rec["rain_mm"] = round(day_mm - last["mm"], 3)
            elif last is None:
                rec["rain_mm"] = 0.0           # first run: don't count rain from before we started
            else:
                rec["rain_mm"] = day_mm         # new day (counter reset)
        if self.db.insert(rec, "lan") and day_mm is not None:
            self.db.set_state("rain_day", {"date": datetime.now().strftime("%Y-%m-%d"), "mm": day_mm})

    def refresh_today(self):
        midnight = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        row = self.db.one("SELECT MIN(temp_c) lo, MAX(temp_c) hi, MAX(gust_kmh) gust, SUM(rain_mm) rain "
                          "FROM readings WHERE ts >= ?", (midnight,))
        bar3h = self.db.one("SELECT bar_hpa FROM readings WHERE ts BETWEEN ? AND ? AND bar_hpa IS NOT NULL "
                            "ORDER BY ts LIMIT 1", (int(time.time()) - 3 * 3600 - 600, int(time.time()) - 3 * 3600 + 600))
        with self.lock:
            self.today = row or {}
            if bar3h and self.current.get("bar_hpa") is not None and self.current.get("bar_trend_hpa") is None:
                self.current["bar_trend_hpa"] = round(self.current["bar_hpa"] - bar3h["bar_hpa"], 2)

    # ---- UDP live wind and rain
    def udp_forever(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", self.cfg.udp_port))
            sock.settimeout(5)
        except OSError as e:
            log.warning("live wind disabled: cannot listen on UDP %s (%s)", self.cfg.udp_port, e)
            return
        renew_at = 0
        station_ip = None
        while True:
            if time.time() >= renew_at:
                try:
                    station_ip = socket.gethostbyname(self.cfg.host.split(":")[0])
                    http_json(f"http://{self.cfg.host}/v1/real_time?duration=3600", timeout=5)
                    renew_at = time.time() + 3000
                except Exception as e:  # noqa: BLE001
                    log.debug("real_time request failed: %s", e)
                    renew_at = time.time() + 30
            try:
                data, addr = sock.recvfrom(8192)
            except socket.timeout:
                continue
            if station_ip and addr[0] != station_ip:
                continue
            try:
                live = parse_udp(json.loads(data.decode("utf-8")), self.cfg.txid)
            except ValueError:
                continue
            if live:
                with self.lock:
                    self.current.update(live)
                self.publish(force=False)

    # ---- gaps and backfill
    def find_gaps(self):
        since = int(time.time()) - self.cfg.backfill_days * 86400
        return self.db.q(
            "SELECT prev AS start, ts AS end FROM ("
            "  SELECT ts, LAG(ts) OVER (ORDER BY ts) AS prev FROM readings"
            "  WHERE source='lan' AND ts >= ?) "
            "WHERE prev IS NOT NULL AND ts - prev > ?", (since, self.cfg.gap_seconds))

    def backfill_forever(self):
        time.sleep(10)
        while True:
            try:
                self.backfill_once()
            except Exception:  # noqa: BLE001
                log.exception("backfill pass failed")
            time.sleep(300)

    def _set_bf(self, **kw):
        with self.lock:
            self.backfill = kw
        self.publish()

    def backfill_once(self):
        if not self.cloud:
            return
        now = int(time.time())
        jobs = []
        for g in self.find_gaps():
            known = self.db.one("SELECT status, tried_at FROM gaps WHERE start=? AND end=?", (g["start"], g["end"]))
            if known and (known["status"] in ("filled", "empty", "refused") or now - (known["tried_at"] or 0) < 3600):
                continue
            jobs.append((g["start"], g["end"], True))
        # Still offline for a while: fill the open gap up to now so the charts keep going.
        if not self.online and self.down_since and now - self.down_since > 900:
            last = self.db.one("SELECT MAX(ts) t FROM readings")
            if last and last["t"]:
                jobs.append((last["t"], now, False))

        for start, end, closed in jobs:
            self._set_bf(state="running", start=start, end=end)
            log.info("backfilling %s -> %s", datetime.fromtimestamp(start), datetime.fromtimestamp(end))
            try:
                recs = self.cloud.historic(start, end)
                n = sum(self.db.insert(r, "cloud") for r in recs)
                if closed:
                    # The LAN reading that closed the gap carries all rain since the previous one.
                    # Take off what the cloud records now account for, so rain isn't counted twice.
                    rain = self.db.one("SELECT COALESCE(SUM(rain_mm),0) r FROM readings "
                                       "WHERE source='cloud' AND ts > ? AND ts < ?", (start, end))["r"]
                    if rain:
                        self.db.run("UPDATE readings SET rain_mm = MAX(0, rain_mm - ?) "
                                    "WHERE ts=? AND source='lan'", (rain, end))
                    self.db.run("INSERT OR REPLACE INTO gaps VALUES(?,?,?,?,?,?)",
                                (start, end, "filled" if recs else "empty", n, None, now))
                log.info("backfilled %d records", n)
                if closed or n:
                    self._set_bf(state="done", start=start, end=end, rows=n, at=now)
                else:   # nothing new yet for an ongoing outage: no news
                    self._set_bf(state="idle")
            except CloudError as e:
                log.warning("backfill: %s", e)
                status = "refused" if "refused" in str(e) else "error"
                if closed:
                    self.db.run("INSERT OR REPLACE INTO gaps VALUES(?,?,?,?,?,?)", (start, end, status, 0, str(e), now))
                self._set_bf(state=status, start=start, end=end, detail=str(e), at=now)
            except Exception as e:  # noqa: BLE001 - e.g. no internet
                log.warning("backfill failed, will retry: %s", e)
                if closed:
                    self.db.run("INSERT OR REPLACE INTO gaps VALUES(?,?,?,?,?,?)", (start, end, "error", 0, str(e), now))
                self._set_bf(state="error", start=start, end=end, detail=str(e), at=now)
        self.refresh_today()


# ---------------------------------------------------------------- history queries
RANGES = {  # span seconds, bucket seconds
    "6h": (6 * 3600, 60), "24h": (86400, 300), "7d": (7 * 86400, 1800),
    "30d": (30 * 86400, 3 * 3600), "1y": (365 * 86400, 86400),
}


def history(db, rng):
    span, bucket = RANGES.get(rng, RANGES["24h"])
    since = int(time.time()) - span
    rows = db.q(f"""
        SELECT (ts / {bucket}) * {bucket} AS t,
               ROUND(AVG(temp_c),2) temp, ROUND(MIN(temp_c),2) temp_lo, ROUND(MAX(temp_c),2) temp_hi,
               ROUND(AVG(hum),1) hum, ROUND(AVG(dew_c),2) dew,
               ROUND(AVG(wind_kmh),1) wind, ROUND(MAX(gust_kmh),1) gust,
               ROUND(SUM(rain_mm),2) rain, ROUND(AVG(bar_hpa),2) bar,
               ROUND(AVG(solar),0) solar,
               SUM(source='cloud') AS cloud, COUNT(*) AS n
        FROM readings WHERE ts >= ? GROUP BY t ORDER BY t""", (since,))
    return {"range": rng, "bucket": bucket, "since": since, "points": rows}


# ---------------------------------------------------------------- web server
def make_handler(station, db, hub):
    index = HERE / "static" / "index.html"

    class Handler(BaseHTTPRequestHandler):
        server_version = "WeatherDash"

        def log_message(self, fmt, *args):
            log.debug("%s %s", self.address_string(), fmt % args)

        def send(self, code, body, ctype="application/json"):
            data = body if isinstance(body, bytes) else body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):  # noqa: N802
            u = urllib.parse.urlparse(self.path)
            qs = urllib.parse.parse_qs(u.query)
            if u.path in ("/", "/index.html"):
                return self.send(200, index.read_bytes(), "text/html; charset=utf-8")
            if u.path == "/api/now":
                return self.send(200, json.dumps(station.snapshot()))
            if u.path == "/api/history":
                return self.send(200, json.dumps(history(db, qs.get("range", ["24h"])[0])))
            if u.path == "/api/gaps":
                return self.send(200, json.dumps(db.q("SELECT * FROM gaps ORDER BY start DESC LIMIT 50")))
            if u.path == "/api/export.csv":
                return self.export(qs)
            if u.path == "/api/events":
                return self.events()
            return self.send(404, '{"error":"not found"}')

        def export(self, qs):
            days = min(int(qs.get("days", ["30"])[0]), 3660)
            rows = db.q("SELECT * FROM readings WHERE ts >= ? ORDER BY ts", (int(time.time()) - days * 86400,))
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["time_utc", "ts", "source"] + COLUMNS)
            for r in rows:
                w.writerow([datetime.fromtimestamp(r["ts"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), r["ts"], r["source"]]
                           + [r[c] for c in COLUMNS])
            self.send(200, buf.getvalue(), "text/csv; charset=utf-8")

        def events(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            q = hub.add()
            try:
                self.wfile.write(f"data: {json.dumps(station.snapshot())}\n\n".encode())
                self.wfile.flush()
                while True:
                    try:
                        msg = q.get(timeout=15)
                        self.wfile.write(f"data: {msg}\n\n".encode())
                    except queue.Empty:
                        self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            finally:
                hub.remove(q)

    return Handler


def main():
    env = os.environ.get
    p = argparse.ArgumentParser(description="Live dashboard and recorder for a Davis WeatherLink Live")
    p.add_argument("--host", default=env("WLL_HOST"), help="IP or hostname of the WeatherLink Live (e.g. 192.168.1.47)")
    p.add_argument("--name", default=env("STATION_NAME", "Home Station"))
    p.add_argument("--port", type=int, default=int(env("PORT", "8080")), help="web port (default 8080)")
    p.add_argument("--bind", default=env("BIND", "0.0.0.0"))
    p.add_argument("--db", default=env("DB_PATH", str(HERE / "weather.db")))
    p.add_argument("--poll", type=int, default=int(env("POLL_SECONDS", "10")), help="seconds between polls (min 10)")
    p.add_argument("--txid", type=int, default=int(env("ISS_TXID", "0")) or None, help="ISS transmitter ID if you have several")
    p.add_argument("--udp-port", type=int, default=22222)
    p.add_argument("--gap-seconds", type=int, default=int(env("GAP_SECONDS", "120")))
    p.add_argument("--backfill-days", type=int, default=int(env("BACKFILL_DAYS", "30")))
    p.add_argument("--api-key", default=env("WL_API_KEY"))
    p.add_argument("--api-secret", default=env("WL_API_SECRET"))
    p.add_argument("--station-id", default=env("WL_STATION_ID"))
    p.add_argument("-v", "--verbose", action="store_true")
    cfg = p.parse_args()
    if not cfg.host:
        p.error("give the station's address with --host or WLL_HOST")
    cfg.poll = max(10, cfg.poll)

    logging.basicConfig(level=logging.DEBUG if cfg.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    db = DB(cfg.db)
    hub = Hub()
    cloud = Cloud(cfg.api_key, cfg.api_secret, cfg.station_id) if cfg.api_key and cfg.api_secret else None
    if not cloud:
        log.info("no WeatherLink.com keys: gaps will be shown but not backfilled")
    station = Station(cfg, db, hub, cloud)

    for target in (station.poll_forever, station.udp_forever, station.backfill_forever):
        threading.Thread(target=target, daemon=True).start()

    srv = ThreadingHTTPServer((cfg.bind, cfg.port), make_handler(station, db, hub))
    srv.daemon_threads = True
    log.info("dashboard on http://%s:%s", "localhost" if cfg.bind == "0.0.0.0" else cfg.bind, cfg.port)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
