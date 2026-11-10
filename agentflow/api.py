"""HTTP API for the dashboard and CLI.

    GET  /api/health
    GET  /api/workflow                  topology from the YAML + resolved sources/processes
    GET  /api/metrics?window=60         flow metrics per step/edge + bottleneck
    POST /api/runs                      {"input": {...}, "session_id"?, "wait"?: bool} → {run_id[, status, outputs]}
    POST /api/load                      {"rate": 2, "duration_s": 60}   synthetic runs from the YAML's examples
    DELETE /api/load                    stop the load generator
    GET  /api/runs?limit=50             recent runs
    GET  /api/runs/{run_id}             every event of one run (traceback / timeline)
    POST /api/query                     {"sql": "select ..."}   read-only SQL over the `events` table
    GET  /api/queries                   preset queries
    GET  /api/steps/{name}/logs         last stdout/stderr lines of each replica
"""
from __future__ import annotations

import asyncio
import os
import random
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .engine import Engine
from .events import check_readonly
from .spec import load

PRESETS = [
    {"name": "Wait vs service time by step", "sql":
        "select step, count(*) as items, round(avg(wait_ms)) as avg_wait_ms,\n"
        "  round(quantile_cont(wait_ms, 0.95)) as p95_wait_ms, round(avg(service_ms)) as avg_service_ms,\n"
        "  round(quantile_cont(service_ms, 0.95)) as p95_service_ms\n"
        "from events where event = 'completed' group by step order by avg_wait_ms desc"},
    {"name": "Failures by step and reason", "sql":
        "select step, event, reason, count(*) as n, any_value(error) as example\n"
        "from events where event in ('retry', 'dead_letter') group by all order by n desc"},
    {"name": "Recent dead letters (traceback)", "sql":
        "select ts, run_id, item_id, step, attempt, reason, error, data->>'traceback' as traceback\n"
        "from events where event = 'dead_letter' order by ts desc limit 50"},
    {"name": "Runs by status", "sql":
        "select status, count(*) as runs, round(avg(service_ms) / 1000, 2) as avg_duration_s,\n"
        "  round(quantile_cont(service_ms, 0.95) / 1000, 2) as p95_duration_s\n"
        "from events where event = 'run_completed' group by status"},
    {"name": "Throughput per minute by step", "sql":
        "select time_bucket(interval '1 minute', ts) as minute, step, count(*) as completed\n"
        "from events where event = 'completed' group by all order by minute desc, step limit 200"},
    {"name": "LLM usage by step and model", "sql":
        "select step, message as model, count(*) as calls,\n"
        "  sum(cast(data->>'input_tokens' as bigint)) as tokens_in,\n"
        "  sum(cast(data->>'output_tokens' as bigint)) as tokens_out,\n"
        "  round(sum(cast(data->>'cost_usd' as double)), 4) as cost_usd\n"
        "from events where event = 'usage' group by all order by tokens_in desc"},
    {"name": "Step logs (warnings and errors)", "sql":
        "select ts, step, status as level, message, run_id, data\n"
        "from events where event = 'log' and status in ('warn', 'warning', 'error') order by ts desc limit 100"},
    {"name": "Backpressure: time blocked entering a queue", "sql":
        "select step, count(*) as enqueues, round(avg(cast(data->>'blocked_ms' as double)), 1) as avg_blocked_ms,\n"
        "  max(cast(data->>'queue_len' as int)) as max_queue_len\n"
        "from events where event = 'enqueued' group by step order by avg_blocked_ms desc"},
]

state: dict = {}


class RunBody(BaseModel):
    input: dict | list | str | int | float | bool | None = None
    session_id: str | None = None
    wait: bool = False
    timeout_s: float = Field(120, gt=0, le=3600)


class LoadBody(BaseModel):
    rate: float = Field(1.0, gt=0, le=200)            # runs per second
    duration_s: float = Field(60, gt=0, le=3600)


class QueryBody(BaseModel):
    sql: str = Field(..., min_length=1, max_length=20000)
    limit: int = Field(1000, ge=1, le=5000)


@asynccontextmanager
async def lifespan(_app):
    spec_path = os.environ["AGENTFLOW_SPEC"]
    wf, base, text = load(spec_path)
    work = Path(os.environ.get("AGENTFLOW_WORKDIR", base.parent / ".agentflow")).resolve()
    engine = Engine(wf, base, text, work)
    state["engine"] = engine
    await engine.start()
    yield
    task = state.pop("load", None)
    if task:
        task.cancel()
    await engine.stop()


app = FastAPI(title="agentflow", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
                   allow_methods=["*"], allow_headers=["*"])


def eng() -> Engine:
    e = state.get("engine")
    if not e or not e.ready:
        raise HTTPException(503, "runtime is starting")
    return e


@app.get("/api/health")
def health():
    e = state.get("engine")
    return {"ok": bool(e and e.ready), "workflow": e.wf.name if e else None}


@app.get("/api/workflow")
def workflow():
    return eng().topology()


@app.get("/api/metrics")
def metrics(window: float = 60):
    e = eng()
    snap = e.metrics.snapshot(e.capacity(), e.live(), window=max(5.0, min(window, 600.0)))
    snap["load"] = state.get("load_info")
    return snap


@app.post("/api/runs")
async def start_run(body: RunBody):
    e = eng()
    run_id = await e.start_run(body.input, body.session_id)
    if not body.wait:
        return {"run_id": run_id}
    run = e.runs[run_id]
    try:
        await asyncio.wait_for(run.done.wait(), body.timeout_s)
    except asyncio.TimeoutError:
        return {"run_id": run_id, "status": "running"}
    return {"run_id": run_id, "status": run.status, "outputs": run.outputs,
            "duration_s": round(time.time() - run.started, 3)}


@app.post("/api/load")
async def start_load(body: LoadBody):
    e = eng()
    if not e.wf.examples:
        raise HTTPException(400, "the workflow YAML has no `examples:` to generate load from")
    old = state.pop("load", None)
    if old:
        old.cancel()
    info = {"rate": body.rate, "duration_s": body.duration_s, "started": time.time(), "sent": 0, "running": True}
    state["load_info"] = info

    async def gen():
        end = time.time() + body.duration_s
        try:
            while time.time() < end:
                ex = dict(random.choice(e.wf.examples))
                asyncio.create_task(e.start_run(ex))
                info["sent"] += 1
                await asyncio.sleep(random.expovariate(body.rate))     # Poisson arrivals
        finally:
            info["running"] = False

    state["load"] = asyncio.create_task(gen())
    return info


@app.delete("/api/load")
def stop_load():
    task = state.pop("load", None)
    if task:
        task.cancel()
    info = state.get("load_info")
    if info:
        info["running"] = False
    return {"stopped": bool(task)}


@app.get("/api/runs")
def runs(limit: int = 50):
    e = eng()
    return e.events.query(
        "select s.run_id, s.ts as started, s.session_id, c.status, round(c.service_ms) as duration_ms,\n"
        "  cast(c.data->>'failed_items' as int) as failed_items\n"
        "from events s left join events c on c.run_id = s.run_id and c.event = 'run_completed'\n"
        "where s.event = 'run_started' order by s.ts desc limit ?", [max(1, min(limit, 500))])


@app.get("/api/runs/{run_id}")
def run_detail(run_id: str):
    e = eng()
    res = e.events.query("select ts, event, item_id, parent_id, step, replica, attempt, wait_ms, service_ms, "
                         "status, reason, error, message, data from events where run_id = ? order by ts", [run_id])
    if not res["rows"]:
        raise HTTPException(404, f"no run {run_id}")
    return res


@app.post("/api/query")
def query(body: QueryBody):
    try:
        sql = check_readonly(body.sql)
    except ValueError as ex:
        raise HTTPException(400, str(ex))
    t0 = time.time()
    try:
        res = eng().events.query(sql, limit=body.limit)
    except Exception as ex:  # SQL errors go back to the user verbatim
        raise HTTPException(400, str(ex).splitlines()[0][:500])
    res["elapsed_ms"] = round((time.time() - t0) * 1000, 1)
    return res


@app.get("/api/queries")
def queries():
    return PRESETS


@app.get("/api/steps/{name}/logs")
def step_logs(name: str):
    e = eng()
    if name not in e.procs:
        raise HTTPException(404, f"no step {name}")
    return {r.index: list(r.tail)[-100:] for r in e.procs[name].replicas}
