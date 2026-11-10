"""Event log: every item movement, step result, log line and LLM usage record, queryable with SQL (DuckDB).

DuckDB stores columns separately, so the dashboard's typical queries (wait time by step, failures by reason,
tokens by model over a time range) scan only the columns they need. A JSONL mirror is kept for tailing/grep.

Event types:
  run_started / run_completed        per run (status, duration)
  enqueued                           item entered a step's queue (blocked_ms = backpressure wait)
  started                            a worker picked it up (wait_ms = time in queue)
  completed / failed / retry / dead_letter   outcome (service_ms, reason, error)
  routed / unrouted / sink           where outputs went
  log / usage                        emitted by step code via the SDK
  step_source / step_process        repo commit fetched, process started/exited
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

COLUMNS = ["ts", "event", "workflow", "run_id", "item_id", "parent_id", "step", "replica", "attempt",
           "session_id", "wait_ms", "service_ms", "status", "reason", "error", "message", "data"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  ts TIMESTAMPTZ, event VARCHAR, workflow VARCHAR, run_id VARCHAR, item_id VARCHAR, parent_id VARCHAR,
  step VARCHAR, replica INTEGER, attempt INTEGER, session_id VARCHAR, wait_ms DOUBLE, service_ms DOUBLE,
  status VARCHAR, reason VARCHAR, error VARCHAR, message VARCHAR, data JSON
);
"""


class EventLog:
    def __init__(self, db_path: Path, jsonl_path: Path, workflow: str):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.workflow = workflow
        self.con = duckdb.connect(str(db_path))
        self.con.execute(SCHEMA)
        self.jsonl = jsonl_path.open("a")
        self.buf: list[tuple] = []
        self.lock = threading.Lock()
        self.db_lock = threading.Lock()

    def emit(self, event: str, **f) -> None:
        f.setdefault("ts", time.time())
        row = {"event": event, "workflow": self.workflow, **f}
        data = row.pop("data", None)
        rec = tuple(
            datetime.fromtimestamp(row["ts"], tz=timezone.utc) if c == "ts" else
            (json.dumps(data, default=str) if data is not None else None) if c == "data" else row.get(c)
            for c in COLUMNS)
        with self.lock:
            self.buf.append(rec)
            self.jsonl.write(json.dumps({**row, "data": data}, default=str) + "\n")

    def flush(self) -> int:
        with self.lock:
            rows, self.buf = self.buf, []
            self.jsonl.flush()
        if rows:
            with self.db_lock:
                self.con.executemany(f"INSERT INTO events VALUES ({', '.join('?' * len(COLUMNS))})", rows)
        return len(rows)

    def query(self, sql: str, params: list | None = None, limit: int = 5000) -> dict:
        """Read-only SQL. Runs on its own cursor so it never blocks writers for long."""
        self.flush()
        with self.db_lock:
            cur = self.con.cursor()
        try:
            cur.execute(sql, params or [])
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchmany(limit + 1)
        finally:
            cur.close()
        truncated = len(rows) > limit
        return {"columns": cols, "rows": [[_jsonable(v) for v in r] for r in rows[:limit]], "truncated": truncated}

    def close(self):
        self.flush()
        self.jsonl.close()
        self.con.close()


def _jsonable(v):
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.decode(errors="replace")
    return v


READONLY_BLOCK = ("insert", "update", "delete", "drop", "create", "alter", "attach", "detach", "copy", "export",
                  "import", "install", "load", "pragma", "set", "call", "checkpoint", "vacuum")


def check_readonly(sql: str) -> str:
    """Allow a single SELECT/WITH/DESCRIBE/SUMMARIZE statement over the events table."""
    s = sql.strip().rstrip(";").strip()
    low = " ".join(s.lower().split())
    if ";" in s:
        raise ValueError("one statement at a time")
    if not low.startswith(("select", "with", "describe", "summarize", "from")):
        raise ValueError("only read queries (SELECT / WITH / DESCRIBE / SUMMARIZE) are allowed")
    tokens = set(low.replace("(", " ").replace(")", " ").split())
    bad = tokens & set(READONLY_BLOCK)
    if bad:
        raise ValueError(f"keyword not allowed in read queries: {sorted(bad)[0]}")
    if "read_" in low or "glob(" in low or "'/" in low:
        raise ValueError("file access functions are not allowed")
    return s
