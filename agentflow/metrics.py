"""Flow metrics per step over a sliding window: the factory-floor view.

For each step:
  λ  arrival rate (items/s entering the queue)        X  throughput (items/s finishing)
  Wq mean time waiting in queue                      Ws mean service time (inside the agent)
  L  mean items in the system (queue + in service), sampled every second
  ρ  utilization = busy slot-seconds / (capacity × window), capacity = replicas × concurrency
Little's law says L ≈ λ·W (W = Wq + Ws). We report both sides: a big gap means the window hasn't reached steady
state yet (e.g. a queue that is still growing).

Bottleneck detection: the step with the highest utilization, flagged when ρ ≥ 0.85 or its queue is growing. The
hint says how many slots it needs to run at ~80% utilization: ceil(λ·Ws / 0.8).
"""
from __future__ import annotations

import math
import time
from collections import defaultdict, deque


def _pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _mean(values) -> float | None:
    values = list(values)
    return sum(values) / len(values) if values else None


class StepStats:
    def __init__(self, keep_s: float = 600):
        self.keep = keep_s
        self.arrivals: deque[float] = deque()
        self.done: deque[tuple] = deque()          # (ts, wait_ms, service_ms, ok)
        self.busy: deque[tuple] = deque()          # (start, end) of each invocation
        self.samples: deque[tuple] = deque()       # (ts, queue_len, inflight)
        self.retries: deque[float] = deque()
        self.dead: deque[float] = deque()
        self.usage: deque[tuple] = deque()         # (ts, input_tokens, output_tokens, cost)
        self.running: dict[str, float] = {}        # item_id -> start, for invocations in progress

    def _trim(self, now: float):
        for d in (self.arrivals, self.retries, self.dead):
            while d and d[0] < now - self.keep:
                d.popleft()
        for d in (self.done, self.samples, self.usage):
            while d and d[0][0] < now - self.keep:
                d.popleft()
        while self.busy and self.busy[0][1] < now - self.keep:
            self.busy.popleft()


class FlowMetrics:
    def __init__(self):
        self.steps: dict[str, StepStats] = defaultdict(StepStats)
        self.edges: dict[tuple, deque] = defaultdict(deque)       # (from, to) -> timestamps
        self.runs: deque[tuple] = deque()                          # (ts, duration_s, status)
        self.run_starts: deque[float] = deque()

    # ---- recording
    def arrival(self, step: str):
        self.steps[step].arrivals.append(time.time())

    def started(self, step: str, item_id: str, start: float):
        self.steps[step].running[item_id] = start

    def finished(self, step: str, start: float, end: float, wait_ms: float, ok: bool, item_id: str | None = None):
        st = self.steps[step]
        st.running.pop(item_id, None)
        st.done.append((end, wait_ms, (end - start) * 1000, ok))
        st.busy.append((start, end))

    def retry(self, step: str):
        self.steps[step].retries.append(time.time())

    def dead_letter(self, step: str):
        self.steps[step].dead.append(time.time())

    def usage(self, step: str, tin: int, tout: int, cost: float | None):
        self.steps[step].usage.append((time.time(), tin, tout, cost or 0.0))

    def edge(self, src: str, dst: str):
        self.edges[(src, dst)].append(time.time())

    def sample(self, step: str, queue_len: int, inflight: int):
        st = self.steps[step]
        st.samples.append((time.time(), queue_len, inflight))
        st._trim(time.time())

    def run_started(self):
        self.run_starts.append(time.time())

    def run_finished(self, duration_s: float, status: str):
        self.runs.append((time.time(), duration_s, status))

    # ---- reporting
    def snapshot(self, capacity: dict[str, int], live: dict[str, tuple], window: float = 60.0) -> dict:
        now = time.time()
        lo = now - window
        steps = {}
        for name, cap in capacity.items():
            st = self.steps[name]
            arr = [t for t in st.arrivals if t >= lo]
            done = [d for d in st.done if d[0] >= lo]
            samples = [s for s in st.samples if s[0] >= lo]
            busy = sum(max(0.0, min(e, now) - max(s, lo)) for s, e in st.busy if e >= lo)
            busy += sum(now - max(s, lo) for s in st.running.values())       # in-progress invocations
            q_now, inflight_now = live.get(name, (0, 0))
            lam = len(arr) / window
            x = len(done) / window
            wq = _mean(d[1] for d in done)
            ws = _mean(d[2] for d in done)
            w = (wq or 0) + (ws or 0) if done else None
            l_obs = _mean(s[1] + s[2] for s in samples)
            rho = min(1.0, busy / (cap * window)) if cap else None
            # queue trend: compare first vs last third of samples
            trend = 0.0
            if len(samples) >= 6:
                k = len(samples) // 3
                trend = (_mean(s[1] for s in samples[-k:]) or 0) - (_mean(s[1] for s in samples[:k]) or 0)
            use = [u for u in st.usage if u[0] >= lo]
            needed = math.ceil(lam * (ws or 0) / 1000 / 0.8) if ws else None
            steps[name] = {
                "capacity": cap, "queue": q_now, "inflight": inflight_now,
                "arrival_rate": round(lam, 3), "throughput": round(x, 3),
                "wait_ms": _r(wq), "service_ms": _r(ws),
                "wait_p95_ms": _r(_pct([d[1] for d in done], 0.95)),
                "service_p95_ms": _r(_pct([d[2] for d in done], 0.95)),
                "utilization": _r(rho, 3),
                "L_observed": _r(l_obs, 2), "L_littles": _r(lam * w / 1000, 2) if w is not None else None,
                "queue_trend": round(trend, 2),
                "success_rate": _r(sum(d[3] for d in done) / len(done), 3) if done else None,
                "retries": sum(1 for t in st.retries if t >= lo), "dead_letters": sum(1 for t in st.dead if t >= lo),
                "tokens_in": sum(u[1] for u in use), "tokens_out": sum(u[2] for u in use),
                "cost_usd": round(sum(u[3] for u in use), 6),
                "slots_needed_at_80pct": needed,
            }
        # bottleneck: highest utilization, if it's actually constrained
        status_by = {}
        ranked = sorted(steps.items(), key=lambda kv: (kv[1]["utilization"] or 0), reverse=True)
        bottleneck = None
        for name, s in ranked:
            constrained = (s["utilization"] or 0) >= 0.85 or (s["queue_trend"] > 1 and s["queue"] > 0)
            if constrained and bottleneck is None:
                bottleneck = name
            status_by[name] = "bottleneck" if name == bottleneck else (
                "busy" if (s["utilization"] or 0) >= 0.7 else "idle" if (s["utilization"] or 0) < 0.1 else "ok")
            s["status"] = status_by[name]
            s["hint"] = _hint(s)
        edges = [{"from": a, "to": b, "rate": round(sum(1 for t in ts if t >= lo) / window, 3)}
                 for (a, b), ts in self.edges.items()]
        runs = [r for r in self.runs if r[0] >= lo]
        durations = [r[1] for r in runs]
        return {
            "window_s": window, "steps": steps, "edges": edges, "bottleneck": bottleneck,
            "workflow": {
                "runs_started_per_s": round(sum(1 for t in self.run_starts if t >= lo) / window, 3),
                "runs_completed_per_s": round(len(runs) / window, 3),
                "run_duration_s": _r(_mean(durations), 2), "run_duration_p95_s": _r(_pct(durations, 0.95), 2),
                "success_rate": _r(sum(1 for r in runs if r[2] == "completed") / len(runs), 3) if runs else None,
            },
        }


def _hint(s: dict) -> str | None:
    u = s["utilization"] or 0
    if s["status"] == "bottleneck":
        need = s["slots_needed_at_80pct"]
        more = f" Needs ≈{need} slots at 80% utilization (has {s['capacity']})." if need else ""
        return f"Utilization {u:.0%}, queue {s['queue']} and {'growing' if s['queue_trend'] > 0 else 'steady'}.{more}"
    if s["status"] == "idle" and s["capacity"] > 1:
        return "Mostly idle; capacity could be reduced."
    return None


def _r(v, nd=1):
    return None if v is None else round(v, nd)
