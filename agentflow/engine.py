"""The runtime: carries every item between steps, which is what makes waits, backpressure and traceback measurable.

Item lifecycle:  enqueued → (waits in the step's bounded queue) → started → invoke replica →
                 completed → routed to next step(s) / sink / run output
                 or failed → retry with backoff → … → dead_letter
Steps never talk to each other directly and never know the topology; the YAML routes their outputs.
"""
from __future__ import annotations

import asyncio
import json
import random
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from .events import EventLog
from .expr import compile_expr, evaluate, lookup
from .metrics import FlowMetrics
from .procs import StepProcesses
from .sources import fetch
from .spec import Workflow

PREVIEW_CHARS = 2000


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def preview(obj):
    """Small payloads are stored as-is (real JSON, queryable with ->>); big ones as a truncated string."""
    s = json.dumps(obj, default=str)
    return obj if len(s) <= PREVIEW_CHARS else s[:PREVIEW_CHARS] + f"…(+{len(s) - PREVIEW_CHARS})"


@dataclass
class Item:
    id: str
    run_id: str
    session_id: str | None
    step: str
    payload: object
    parent_id: str | None = None
    attempt: int = 1
    hops: int = 0
    enqueued_at: float = 0.0


@dataclass
class Run:
    id: str
    input: object
    session_id: str | None
    started: float
    outstanding: int = 0
    failed_items: int = 0
    outputs: list = field(default_factory=list)
    status: str = "running"
    done: asyncio.Event = field(default_factory=asyncio.Event)


class Engine:
    def __init__(self, wf: Workflow, base_dir: Path, yaml_text: str, work: Path):
        self.wf, self.base_dir, self.yaml_text, self.work = wf, base_dir, yaml_text, work
        self.events = EventLog(work / "events.duckdb", work / "events.jsonl", wf.name)
        self.metrics = FlowMetrics()
        self.procs: dict[str, StepProcesses] = {}
        self.sources: dict = {}
        self.queues: dict[str, asyncio.Queue] = {}
        self.inflight: dict[str, int] = {n: 0 for n in wf.steps}
        self.runs: dict[str, Run] = {}
        self.sessions: dict[str, dict] = {}
        self.when = {id(e): compile_expr(e.when) for e in wf.flow if e.when}
        self._tasks: list[asyncio.Task] = []
        self.client: httpx.AsyncClient | None = None
        self.ready = False

    # ------------------------------------------------------------------------------------------ lifecycle
    async def start(self, log=print):
        for name, step in self.wf.steps.items():
            src = await asyncio.to_thread(fetch, name, step, self.base_dir, self.work, log)
            self.sources[name] = src
            self.events.emit("step_source", step=name, status=src.origin,
                             message=step.repo or step.path, data={"commit": src.commit, "dir": str(src.dir),
                                                                   "ref": step.ref, "venv": src.python})
        for name, step in self.wf.steps.items():
            self.procs[name] = StepProcesses(name, step, self.sources[name], self.work, self.events)
        await asyncio.gather(*(p.start() for p in self.procs.values()))
        total = sum(s.replicas * s.concurrency for s in self.wf.steps.values())
        self.client = httpx.AsyncClient(limits=httpx.Limits(max_connections=total + 10, max_keepalive_connections=total))
        for name, step in self.wf.steps.items():
            self.queues[name] = asyncio.Queue(maxsize=step.max_queue)
            for _ in range(step.replicas * step.concurrency):
                self._tasks.append(asyncio.create_task(self._worker(name)))
        self._tasks.append(asyncio.create_task(self._background()))
        self.ready = True
        log(f"workflow {self.wf.name!r} ready: {len(self.wf.steps)} steps, {total} worker slots")

    async def stop(self):
        self.ready = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*(p.stop() for p in self.procs.values()), return_exceptions=True)
        if self.client:
            await self.client.aclose()
        self.events.close()

    async def _background(self):
        while True:
            await asyncio.sleep(1.0)
            for name, q in self.queues.items():
                self.metrics.sample(name, q.qsize(), self.inflight[name])
            await asyncio.to_thread(self.events.flush)

    # ------------------------------------------------------------------------------------------ runs
    async def start_run(self, payload, session_id: str | None = None) -> str:
        if session_id is None and self.wf.session.key:
            sid = lookup(self.wf.session.key, {"input": payload})
            session_id = str(sid) if sid is not None else None
        run = Run(id=new_id("run"), input=payload, session_id=session_id, started=time.time())
        self.runs[run.id] = run
        self.metrics.run_started()
        self.events.emit("run_started", run_id=run.id, session_id=session_id, data={"input": preview(payload)})
        targets = self._match("input", payload, payload, session_id)
        if not targets:
            self._finish_run(run, "unrouted")
            return run.id
        run.outstanding += 1          # guard: a fast first item must not complete the run before the rest start
        for edge in targets:
            await self._deliver(edge, payload, run, parent=None, hops=0, source="input")
        self._item_done(run)
        return run.id

    def _finish_run(self, run: Run, status: str | None = None):
        run.status = status or ("failed" if run.failed_items else "completed")
        dur = time.time() - run.started
        self.metrics.run_finished(dur, run.status)
        self.events.emit("run_completed", run_id=run.id, session_id=run.session_id, status=run.status,
                         service_ms=dur * 1000, data={"outputs": preview(run.outputs[-20:]),
                                                      "failed_items": run.failed_items})
        run.done.set()
        if len(self.runs) > 5000:                              # keep memory bounded; history lives in the log
            for rid in [r for r, v in self.runs.items() if v.done.is_set()][:1000]:
                self.runs.pop(rid, None)

    def _item_done(self, run: Run):
        run.outstanding -= 1
        if run.outstanding <= 0 and run.status == "running":
            self._finish_run(run)

    # ------------------------------------------------------------------------------------------ routing
    def _match(self, node: str, output, run_input, session_id):
        ctx = {"output": output, "input": run_input, "session": self.sessions.get(session_id or "", {})}
        out = []
        for e in self.wf.edges_from(node):
            if e.when is None:
                out.append(e)
                continue
            try:
                if evaluate(self.when[id(e)], ctx):
                    out.append(e)
            except Exception as ex:  # a bad condition must never kill the router
                self.events.emit("route_error", step=node, error=str(ex), message=e.when)
        return out

    async def _deliver(self, edge, payload, run: Run, parent: Item | None, hops: int, source: str):
        self.metrics.edge(source, edge.to)
        if edge.to.startswith("sink."):
            self._sink(edge, payload, run, parent)
            return
        item = Item(id=new_id("itm"), run_id=run.id, session_id=run.session_id, step=edge.to, payload=payload,
                    parent_id=parent.id if parent else None, hops=hops)
        run.outstanding += 1
        if parent is not None:
            self.events.emit("routed", run_id=run.id, item_id=item.id, parent_id=parent.id, step=source,
                             message=f"{source} → {edge.to}", data={"when": edge.when})
        await self._enqueue(item)

    def _sink(self, edge, payload, run: Run, parent: Item | None):
        rec = {"ts": time.time(), "run_id": run.id, "item_id": parent.id if parent else None,
               "step": parent.step if parent else "input", "payload": payload}
        if edge.to == "sink.jsonl":
            p = (self.base_dir / edge.with_["path"]).resolve()
            p.parent.mkdir(parents=True, exist_ok=True)
            with p.open("a") as f:
                f.write(json.dumps(rec, default=str) + "\n")
        self.events.emit("sink", run_id=run.id, item_id=parent.id if parent else None,
                         step=parent.step if parent else None, message=edge.to,
                         data={"with": edge.with_, "payload": preview(payload)})

    async def _enqueue(self, item: Item):
        t0 = time.time()
        await self.queues[item.step].put(item)            # blocks when the queue is full: backpressure
        item.enqueued_at = time.time()
        self.metrics.arrival(item.step)
        self.events.emit("enqueued", run_id=item.run_id, item_id=item.id, parent_id=item.parent_id, step=item.step,
                         attempt=item.attempt, session_id=item.session_id,
                         data={"blocked_ms": round((item.enqueued_at - t0) * 1000, 1),
                               "queue_len": self.queues[item.step].qsize(), "input": preview(item.payload)})

    # ------------------------------------------------------------------------------------------ workers
    async def _worker(self, step: str):
        q = self.queues[step]
        while True:
            item = await q.get()
            try:
                await self._process(step, item)
            except Exception as e:  # never lose a worker slot to an engine bug
                self.events.emit("engine_error", step=step, run_id=item.run_id, item_id=item.id,
                                 error=f"{type(e).__name__}: {e}")
                run = self.runs.get(item.run_id)
                if run:
                    run.failed_items += 1
                    self._item_done(run)
            finally:
                q.task_done()

    async def _process(self, step: str, item: Item):
        spec = self.wf.steps[step]
        run = self.runs.get(item.run_id)
        start = time.time()
        wait_ms = (start - item.enqueued_at) * 1000
        replica = await self._await_replica(step)
        self.inflight[step] += 1
        self.metrics.started(step, item.id, start)
        if replica:
            replica.inflight += 1
        self.events.emit("started", run_id=item.run_id, item_id=item.id, step=step, attempt=item.attempt,
                         session_id=item.session_id, replica=replica.index if replica else None, wait_ms=wait_ms)
        body, code, reason, error, retryable = None, None, None, None, True
        try:
            if replica is None:
                reason, error = "no_replica", "no healthy replica available"
            else:
                req = {"run_id": item.run_id, "item_id": item.id, "session_id": item.session_id, "step": step,
                       "attempt": item.attempt, "input": item.payload,
                       "session": self.sessions.get(item.session_id or "", {}),
                       "meta": {"workflow": self.wf.name, "parent_id": item.parent_id, "hops": item.hops}}
                resp = await self.client.post(replica.url + "/invoke", json=req, timeout=spec.timeout_s)
                code = resp.status_code
                try:
                    body = resp.json()
                except ValueError:
                    reason, error, retryable = "bad_response", resp.text[:300], False
                if body is not None and code != 200:
                    err = body.get("error") or {}
                    reason = err.get("reason", f"http_{code}")
                    error = err.get("detail") or str(err)[:500]
                    retryable = bool(err.get("retryable", code >= 500))
        except httpx.TimeoutException:
            reason, error = "timeout", f"no response within {spec.timeout_s}s"
        except httpx.HTTPError as e:
            reason, error = "step_unavailable", f"{type(e).__name__}: {e}"
        finally:
            self.inflight[step] -= 1
            if replica:
                replica.inflight -= 1
        end = time.time()
        service_ms = (end - start) * 1000
        ok = code == 200 and body is not None and reason is None

        for lg in (body or {}).get("logs", []):
            self.events.emit("log", ts=lg.get("ts", end), run_id=item.run_id, item_id=item.id, step=step,
                             session_id=item.session_id, status=lg.get("level"), message=lg.get("message"),
                             replica=replica.index if replica else None, data=lg.get("fields") or None)
        for u in (body or {}).get("usage", []):
            self.metrics.usage(step, u.get("input_tokens", 0), u.get("output_tokens", 0), u.get("cost_usd"))
            self.events.emit("usage", run_id=item.run_id, item_id=item.id, step=step, session_id=item.session_id,
                             message=u.get("model"), data=u)

        if ok:
            self.metrics.finished(step, start, end, wait_ms, True, item.id)
            outputs = body.get("outputs", [])
            patch = body.get("session_patch") or {}
            if patch and item.session_id:
                self.sessions.setdefault(item.session_id, {}).update(patch)
            self.events.emit("completed", run_id=item.run_id, item_id=item.id, step=step, attempt=item.attempt,
                             session_id=item.session_id, replica=replica.index, wait_ms=wait_ms,
                             service_ms=service_ms, status="ok",
                             data={"outputs": preview(outputs), "n_outputs": len(outputs),
                                   "session_patch": list(patch)})
            await self._route_outputs(step, item, outputs, run)
            return

        self.metrics.finished(step, start, end, wait_ms, False, item.id)
        tb = ((body or {}).get("error") or {}).get("traceback")
        if retryable and item.attempt <= spec.retries:
            self.metrics.retry(step)
            delay = min(10.0, 0.5 * 2 ** (item.attempt - 1)) * (0.8 + 0.4 * random.random())
            self.events.emit("retry", run_id=item.run_id, item_id=item.id, step=step, attempt=item.attempt,
                             session_id=item.session_id, wait_ms=wait_ms, service_ms=service_ms, status="retrying",
                             reason=reason, error=error, data={"backoff_s": round(delay, 2), "traceback": tb})
            item.attempt += 1
            asyncio.create_task(self._requeue_later(item, delay))
            return
        self.metrics.dead_letter(step)
        self.events.emit("dead_letter", run_id=item.run_id, item_id=item.id, step=step, attempt=item.attempt,
                         session_id=item.session_id, wait_ms=wait_ms, service_ms=service_ms, status="failed",
                         reason=reason, error=error,
                         data={"input": preview(item.payload), "traceback": tb, "retryable": retryable})
        if run:
            run.failed_items += 1
            self._item_done(run)

    async def _requeue_later(self, item: Item, delay: float):
        await asyncio.sleep(delay)
        await self._enqueue(item)

    async def _await_replica(self, step: str, wait_s: float = 5.0):
        deadline = time.time() + wait_s
        while True:
            r = self.procs[step].pick()
            if r or time.time() > deadline:
                return r
            await asyncio.sleep(0.1)

    async def _route_outputs(self, step: str, item: Item, outputs: list, run: Run | None):
        if run is None:
            return
        edges_out = self.wf.edges_from(step)
        for out in outputs:
            targets = self._match(step, out, run.input, item.session_id) if edges_out else []
            if not targets:
                run.outputs.append({"step": step, "output": out})
                if edges_out:
                    self.events.emit("unrouted", run_id=run.id, item_id=item.id, step=step,
                                     message="no edge condition matched", data={"output": preview(out)})
                continue
            if item.hops + 1 > self.wf.max_hops:
                self.events.emit("dead_letter", run_id=run.id, item_id=item.id, step=step, reason="max_hops",
                                 error=f"exceeded {self.wf.max_hops} hops (routing loop?)")
                run.failed_items += 1
                continue
            for edge in targets:
                await self._deliver(edge, out, run, parent=item, hops=item.hops + 1, source=step)
        self._item_done(run)

    # ------------------------------------------------------------------------------------------ views
    def capacity(self) -> dict[str, int]:
        return {n: s.replicas * s.concurrency for n, s in self.wf.steps.items()}

    def live(self) -> dict[str, tuple]:
        return {n: (q.qsize(), self.inflight[n]) for n, q in self.queues.items()}

    def topology(self) -> dict:
        steps = {}
        for n, s in self.wf.steps.items():
            src = self.sources.get(n)
            p = self.procs.get(n)
            steps[n] = {"repo": s.repo, "ref": s.ref, "path": s.path, "origin": src.origin if src else None,
                        "commit": src.commit if src else None, "replicas": s.replicas, "concurrency": s.concurrency,
                        "timeout_s": s.timeout_s, "retries": s.retries, "max_queue": s.max_queue,
                        "processes": [{"index": r.index, "port": r.port, "healthy": r.healthy,
                                       "restarts": r.restarts, "inflight": r.inflight} for r in p.replicas] if p else []}
        return {"name": self.wf.name, "steps": steps,
                "flow": [{"from": e.from_, "to": e.to, "when": e.when, "with": e.with_} for e in self.wf.flow],
                "session_key": self.wf.session.key, "examples": self.wf.examples, "yaml": self.yaml_text}
