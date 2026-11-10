"""Read-only monitor for engines this process doesn't own: agentflow embedded in other programs (often one
short-lived engine per run), each with its own work dir. Point it at one or more roots; it finds every work dir under
them and serves the dashboard's API for the selected workflow (`?w=<id>`), live or finished.

    agentflow monitor path/to/.agentflow [more/roots ...] [--port 7070]
    cd dashboard && npm run dev        # the dashboard detects monitor mode and lists every workflow

A work dir is any folder containing engine.json or events.jsonl (written by Engine). Its id is its path relative to
the root (e.g. `acme/top-of-funnel`); with several roots the root's folder name is prefixed. Liveness comes from
engine.json (status + pid) and the freshness of state.json. Events are queried from events.jsonl, because a running
engine holds an exclusive lock on its DuckDB file.

    GET  /api/health                     {ok, mode: "monitor", roots, workflows}
    GET  /api/workflows                  every work dir: id, name, live, pid, started/stopped, runs, failures, cost, last event
    GET  /api/workflow?w=ID              topology (+ YAML) from engine.json / state.json, plus `live`
    GET  /api/metrics?w=ID               flow metrics from state.json (the engine's 60 s window; `live: false` once stopped)
    GET  /api/runs?w=ID&limit=           recent runs
    GET  /api/runs/{run_id}?w=ID         every event of one run
    POST /api/query?w=ID                 {"sql": "select ..."}  read-only SQL over that work dir's `events`
    GET  /api/queries                    preset queries
    GET  /api/steps/{name}/logs?w=ID     last lines of each replica's process log
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .events import COLUMNS, _jsonable, check_readonly

LIVE_STALE_S = 15.0           # state.json older than this = the engine is gone (crashed or killed)
MAX_DEPTH = 5
SKIP_DIRS = {"repos", "envs", "logs", "sdk", "node_modules", ".git", "__pycache__"}

_TYPES = {"ts": "DOUBLE", "replica": "INTEGER", "attempt": "INTEGER", "wait_ms": "DOUBLE", "service_ms": "DOUBLE",
          "data": "JSON"}
_JSON_COLUMNS = "{" + ", ".join(f"{c}: '{_TYPES.get(c, 'VARCHAR')}'" for c in COLUMNS) + "}"
_SELECT = ", ".join("to_timestamp(ts) AS ts" if c == "ts" else c for c in COLUMNS)
_EMPTY = ("CREATE OR REPLACE TABLE events AS SELECT " +
          ", ".join(f"CAST(NULL AS {'TIMESTAMPTZ' if c == 'ts' else _TYPES.get(c, 'VARCHAR')}) AS {c}" for c in COLUMNS)
          + " WHERE false")


def _roots() -> list[Path]:
    raw = os.environ.get("AGENTFLOW_MONITOR_ROOTS", "")
    return [Path(p).resolve() for p in raw.split(os.pathsep) if p]


def _read_json(p: Path) -> dict | None:
    try:
        return json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def _pid_alive(pid) -> bool:
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, TypeError, ValueError):
        return False


def discover(roots: list[Path]) -> dict[str, Path]:
    """{id: work_dir} for every work dir under the roots."""
    found: dict[str, Path] = {}
    for root in roots:
        if not root.is_dir():
            continue
        prefix = f"{root.name}/" if len(roots) > 1 else ""
        stack = [(root, 0)]
        while stack:
            d, depth = stack.pop()
            if (d / "engine.json").exists() or (d / "events.jsonl").exists():
                rel = d.relative_to(root).as_posix()
                found[(prefix + rel) if rel != "." else (prefix.rstrip("/") or root.name)] = d
            if depth < MAX_DEPTH:
                try:
                    subs = [c for c in d.iterdir() if c.is_dir() and c.name not in SKIP_DIRS and not c.name.startswith(".")]
                except OSError:
                    subs = []
                stack.extend((c, depth + 1) for c in subs)
    return dict(sorted(found.items()))


def liveness(work: Path) -> dict:
    eng = _read_json(work / "engine.json") or {}
    state = _read_json(work / "state.json") or {}
    fresh = bool(state.get("ts")) and time.time() - float(state["ts"]) < LIVE_STALE_S
    live = eng.get("status") == "running" and state.get("live") is True and fresh and _pid_alive(eng.get("pid"))
    status = "running" if live else ("stopped" if eng.get("status") == "stopped" else
                                     "lost" if eng.get("status") == "running" else "unknown")
    return {"live": live, "status": status, "engine": eng, "state": state}


class _EventsCache:
    """One in-memory DuckDB per work dir with events.jsonl loaded as a table; reloaded when the file changes."""

    def __init__(self):
        self.lock = threading.Lock()
        self.entries: dict[str, tuple[tuple, duckdb.DuckDBPyConnection, threading.Lock]] = {}

    def con(self, work: Path) -> tuple[duckdb.DuckDBPyConnection, threading.Lock]:
        path = work / "events.jsonl"
        try:
            st = path.stat()
            sig = (st.st_size, st.st_mtime_ns)
        except FileNotFoundError:
            sig = (0, 0)
        key = str(work)
        with self.lock:
            entry = self.entries.get(key)
            if entry and entry[0] == sig:
                return entry[1], entry[2]
            con = entry[1] if entry else duckdb.connect()
            lk = entry[2] if entry else threading.Lock()
        with lk:
            if sig[0] == 0:
                con.execute(_EMPTY)
            else:
                lit = str(path).replace("'", "''")
                con.execute(f"CREATE OR REPLACE TABLE events AS SELECT {_SELECT} FROM read_json('{lit}', "
                            f"format='newline_delimited', columns={_JSON_COLUMNS}, ignore_errors=true)")
        with self.lock:
            self.entries[key] = (sig, con, lk)
        return con, lk

    def query(self, work: Path, sql: str, params: list | None = None, limit: int = 5000) -> dict:
        con, lk = self.con(work)
        with lk:
            cur = con.cursor()
            try:
                cur.execute(sql, params or [])
                cols = [d[0] for d in cur.description] if cur.description else []
                rows = cur.fetchmany(limit + 1)
            finally:
                cur.close()
        return {"columns": cols, "rows": [[_jsonable(v) for v in r] for r in rows[:limit]], "truncated": len(rows) > limit}


CACHE = _EventsCache()
app = FastAPI(title="agentflow monitor")
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
                   allow_methods=["*"], allow_headers=["*"])


def _work(w: str | None) -> Path:
    if not w:
        raise HTTPException(400, "monitor mode: pick a workflow with ?w=<id> (GET /api/workflows lists them)")
    found = discover(_roots())
    if w not in found:
        raise HTTPException(404, f"no workflow {w!r} under the monitored roots")
    return found[w]


@app.get("/api/health")
def health():
    found = discover(_roots())
    return {"ok": True, "mode": "monitor", "workflow": None, "roots": [str(r) for r in _roots()], "workflows": len(found)}


@app.get("/api/workflows")
def workflows():
    out = []
    for wid, work in discover(_roots()).items():
        lv = liveness(work)
        eng = lv["engine"]
        try:
            stats = CACHE.query(work, """
                select count(*) filter (where event = 'run_started'),
                       count(*) filter (where event = 'run_completed' and status = 'completed'),
                       count(*) filter (where event = 'run_completed' and status <> 'completed'),
                       coalesce(sum(cast(data->>'cost_usd' as double)) filter (where event = 'usage'), 0),
                       count(*) filter (where event = 'dead_letter'),
                       max(ts)
                from events""")["rows"][0]
        except duckdb.Error:
            stats = [None] * 6
        started, ok, failed, cost, dead, last = stats
        out.append({"id": wid, "name": eng.get("workflow") or (eng.get("topology") or {}).get("name") or wid.split("/")[-1],
                    "dir": str(work), "live": lv["live"], "status": lv["status"], "pid": eng.get("pid"),
                    "started": eng.get("started"), "stopped": eng.get("stopped"),
                    "agentflow_version": eng.get("agentflow_version"),
                    "runs": started or 0, "runs_completed": ok or 0, "runs_failed": failed or 0,
                    "runs_running": max(0, (started or 0) - (ok or 0) - (failed or 0)),
                    "dead_letters": dead or 0, "cost_usd": round(float(cost or 0), 4), "last_event": last})
    out.sort(key=lambda x: x["last_event"] or "", reverse=True)       # most recent activity first
    out.sort(key=lambda x: not x["live"])                             # live engines on top (stable)
    return out


@app.get("/api/workflow")
def workflow(w: str | None = None):
    work = _work(w)
    lv = liveness(work)
    topo = dict((lv["engine"].get("topology") or {}))
    live_topo = (lv["state"].get("topology") or {}) if lv["live"] else {}
    if live_topo.get("steps"):
        topo["steps"] = live_topo["steps"]                  # current process health
    if not topo:
        raise HTTPException(404, f"{w}: no engine.json yet (written when an engine starts with agentflow >= this version)")
    if not lv["live"]:
        for s in (topo.get("steps") or {}).values():
            for p in s.get("processes", []):
                p["healthy"], p["inflight"] = False, 0
    return {**topo, "examples": [], "live": lv["live"], "status": lv["status"], "id": w}


@app.get("/api/metrics")
def metrics(w: str | None = None, window: float = 60):
    work = _work(w)
    lv = liveness(work)
    m = lv["state"].get("metrics") if lv["live"] else None
    if not m:
        m = {"window_s": 60, "bottleneck": None, "steps": {}, "edges": [],
             "workflow": {"runs_started_per_s": 0, "runs_completed_per_s": 0, "run_duration_s": None,
                          "run_duration_p95_s": None, "success_rate": None}}
    return {**m, "load": None, "live": lv["live"], "status": lv["status"]}


@app.get("/api/runs")
def runs(w: str | None = None, limit: int = 50):
    return CACHE.query(_work(w),
        "select s.run_id, s.ts as started, s.session_id, c.status, round(c.service_ms) as duration_ms,\n"
        "  cast(c.data->>'failed_items' as int) as failed_items\n"
        "from events s left join events c on c.run_id = s.run_id and c.event = 'run_completed'\n"
        "where s.event = 'run_started' order by s.ts desc limit ?", [max(1, min(limit, 500))])


@app.get("/api/runs/{run_id}")
def run_detail(run_id: str, w: str | None = None):
    res = CACHE.query(_work(w), "select ts, event, item_id, parent_id, step, replica, attempt, wait_ms, service_ms, "
                                "status, reason, error, message, data from events where run_id = ? order by ts", [run_id])
    if not res["rows"]:
        raise HTTPException(404, f"no run {run_id}")
    return res


class QueryBody(BaseModel):
    sql: str = Field(..., min_length=1, max_length=20000)
    limit: int = Field(1000, ge=1, le=5000)


@app.post("/api/query")
def query(body: QueryBody, w: str | None = Query(None)):
    work = _work(w)
    try:
        sql = check_readonly(body.sql)
    except ValueError as ex:
        raise HTTPException(400, str(ex))
    t0 = time.time()
    try:
        res = CACHE.query(work, sql, limit=body.limit)
    except Exception as ex:  # SQL errors go back verbatim
        raise HTTPException(400, str(ex).splitlines()[0][:500])
    res["elapsed_ms"] = round((time.time() - t0) * 1000, 1)
    return res


@app.get("/api/queries")
def queries():
    from .api import PRESETS
    return PRESETS


@app.get("/api/steps/{name}/logs")
def step_logs(name: str, w: str | None = None):
    work = _work(w)
    out = {}
    for f in sorted((work / "logs").glob(f"{name}-*.log")):
        idx = f.stem.rsplit("-", 1)[-1]
        try:
            lines = f.read_text(errors="replace").splitlines()
        except OSError:
            lines = []
        out[idx] = lines[-100:]
    if not out:
        raise HTTPException(404, f"no logs for step {name!r}")
    return out
