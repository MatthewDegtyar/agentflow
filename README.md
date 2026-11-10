# agentflow

YAML-defined agent workflows where **each step is its own GitHub repo**. The runtime pulls each repo, starts the
steps, carries every message between them (steps never know the topology), and writes every movement to a
queryable event log. A Next.js dashboard shows the workflow as a live flow graph, per-run traces, and a SQL console
over the logs.

```
workflows/*.yaml ──▶ runtime (python)                                  dashboard (next.js)
                     ├─ fetch each step: git clone repo@ref (or path)   ├─ Workflow: live graph, λ/X/wait/service/ρ,
                     ├─ start N replicas per step (own venv if           │  Little's law check, bottleneck + advice,
                     │  requirements.txt), supervise, restart            │  run example / generate load
                     ├─ per-step bounded queues (backpressure),         ├─ Runs: per-run timeline (queue wait vs
                     │  retries with backoff, dead letters, routing     │  service), every event
                     │  on `when:`, fan-out, sinks, sessions            └─ Logs: read-only SQL (DuckDB) + presets
                     └─ event log: DuckDB (columnar) + JSONL
```

## Run it

```bash
# runtime
uv sync                                                                # installs deps from pyproject.toml / uv.lock
uv run python -m agentflow validate workflows/doc-triage.yaml
uv run python -m agentflow up workflows/doc-triage.yaml               # API on :7070

# dashboard (another terminal)
cd dashboard && npm install && npm run dev                             # http://localhost:3000 (proxies /api → :7070)
AGENTFLOW_API=http://127.0.0.1:7071 npm run dev                        # if the runtime is elsewhere

# drive it
uv run python -m agentflow run '{"customer_id":"acme","title":"Q3 report","text":"...","kind_hint":"report"}' --wait
uv run python -m agentflow load --rate 3 --duration 60              # watch `summarize` become the bottleneck
uv run python -m agentflow query "select step, count(*) from events where event='retry' group by step"
uv run pytest -q
```

State lives in `.agentflow/` (event log `events.duckdb` + `events.jsonl`, cloned repos, per-step venvs, process logs).

## Workflow YAML

```yaml
name: doc-triage
steps:
  classify:
    repo: github.com/your-org/agent-classify   # cloned at startup; private repos: set GITHUB_TOKEN
    ref: main                                  # branch, tag or commit SHA (recorded per event)
    path: ../examples/steps/classify           # local override while the repo doesn't exist
    run: python main.py                        # starts the step server; it must listen on $PORT
    replicas: 2                                # processes
    concurrency: 4                             # items in flight per process → capacity = 8 slots
    timeout_s: 30
    retries: 2                                 # for retryable failures (exceptions, timeouts, crashes)
    max_queue: 1000                            # bounded queue: when full, upstream waits (backpressure)
    env: { MODEL: "${MODEL:-claude-sonnet-5}" }
flow:
  - { from: input, to: intake }
  - { from: classify, to: summarize, when: "output.kind in ['report', 'contract']" }
  - { from: summarize, to: sink.jsonl, with: { path: out/summaries.jsonl } }
session:
  key: input.customer_id                       # shared state across steps for the same customer
examples: [ ... ]                              # sample inputs for `load` / the dashboard
```
`path:` wins over `repo:` until you set `AGENTFLOW_USE_REPOS=1` (with both set, `path` becomes a subdirectory of the
repo). `when:` is a safe expression over `output`, `input` (the run's original input) and `session`.

## Writing a step (the contract for each agent repo)

A step repo needs `main.py` (and optionally `requirements.txt`, installed into the step's own venv):

```python
from agentflow_step import Step        # provided by the runtime on PYTHONPATH; stdlib only

step = Step()

@step.handler
def handle(ctx, payload):
    ctx.log("working", level="info", chars=len(payload["text"]))           # → event log, tied to run/item/step
    ctx.usage(model="claude-sonnet-5", input_tokens=812, output_tokens=64, cost_usd=0.003)
    ctx.session["seen"] = ctx.session.get("seen", 0) + 1                   # shared per session key
    if not payload.get("text"):
        ctx.fail("invalid_input", "no text")                               # non-retryable → dead letter
    return {"kind": "report"}                                              # or ctx.emit(...) N times (fan-out)

step.run()                                                                 # serves POST /invoke + GET /health on $PORT
```
Raising an exception is a retryable failure: its type becomes the `reason` and the traceback is kept. Any language
works if it serves `GET /health` and `POST /invoke` with the same JSON contract (see `sdk/python/agentflow_step.py`).

## Event log

One `events` table (DuckDB, so columnar): `ts, event, workflow, run_id, item_id, parent_id, step, replica, attempt,
session_id, wait_ms, service_ms, status, reason, error, message, data (JSON)`.

Events: `run_started`, `run_completed`, `enqueued` (with `blocked_ms` = backpressure), `started` (with `wait_ms` =
time queued), `completed`, `retry`, `dead_letter`, `routed`, `unrouted`, `sink`, `log`, `usage`, `step_source`
(repo and commit), `step_process`. Every item links to its parent (`parent_id`), so any output traces back hop by
hop to the run input.

## Flow metrics (per step, sliding window)
λ arrivals/s, X throughput/s, Wq queue wait, Ws service time, L items in system, ρ utilization = busy
slot-seconds ÷ (slots × window). Little's law (L = λ·W) is shown both ways; the bottleneck is the most-utilized
step at ρ ≥ 85% or with a growing queue, with the slot count needed to run at 80%.

## Not yet
Containers (steps run as local processes today), multi-host placement, autoscaling from the metrics, auth on the
API, a TypeScript step SDK, and log retention / compaction.
