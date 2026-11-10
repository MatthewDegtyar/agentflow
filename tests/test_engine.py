"""End to end: real step processes, real HTTP, real event log."""
import asyncio
import json
import textwrap
from pathlib import Path

from agentflow.engine import Engine
from agentflow.spec import load

STEPS = {
    "gate": """
        @step.handler
        def handle(ctx, x):
            if x["n"] < 0:
                ctx.fail("invalid_input", "n must be >= 0")        # business failure: no retry
            ctx.session["seen"] = ctx.session.get("seen", 0) + 1
            ctx.log("gate passed", n=x["n"])
            return x
    """,
    "double": """
        @step.handler
        def handle(ctx, x):
            ctx.usage(model="test-model", input_tokens=10, output_tokens=2, cost_usd=0.001)
            return {"n": x["n"] * 2}
    """,
    "split": """
        @step.handler
        def handle(ctx, x):
            for i in range(x["n"]):
                ctx.emit({"i": i, "odd": i % 2 == 1})               # fan-out
    """,
    "flaky": """
        @step.handler
        def handle(ctx, x):
            if ctx.attempt == 1:
                raise ConnectionError("first attempt always fails")  # retryable
            return {"done": x["i"]}
    """,
}

WORKFLOW = """
name: e2e
steps:
  gate:   {path: steps/gate,   run: python main.py, concurrency: 2}
  double: {path: steps/double, run: python main.py}
  split:  {path: steps/split,  run: python main.py}
  flaky:  {path: steps/flaky,  run: python main.py, retries: 2, replicas: 2, concurrency: 2}
flow:
  - {from: input, to: gate}
  - {from: gate, to: double}
  - {from: double, to: split}
  - {from: split, to: flaky, when: "output.odd == false"}
  - {from: flaky, to: sink.jsonl, with: {path: out/done.jsonl}}
session:
  key: input.customer
"""


def make(tmp_path: Path) -> Path:
    for name, body in STEPS.items():
        d = tmp_path / "steps" / name
        d.mkdir(parents=True)
        (d / "main.py").write_text("from agentflow_step import Step\nstep = Step()\n" + textwrap.dedent(body) + "\nstep.run()\n")
    wf_path = tmp_path / "wf.yaml"
    wf_path.write_text(WORKFLOW)
    return wf_path


def test_end_to_end(tmp_path):
    wf, base, text = load(make(tmp_path))

    async def main():
        eng = Engine(wf, base, text, tmp_path / ".agentflow")
        await eng.start(log=lambda *_: None)
        try:
            ok_id = await eng.start_run({"customer": "c1", "n": 2})         # 2 → 4 → split into 4, 2 even → flaky
            bad_id = await eng.start_run({"customer": "c1", "n": -1})
            await asyncio.wait_for(eng.runs[ok_id].done.wait(), 30)
            await asyncio.wait_for(eng.runs[bad_id].done.wait(), 30)
            ok, bad = eng.runs[ok_id], eng.runs[bad_id]
            snap = eng.metrics.snapshot(eng.capacity(), eng.live(), window=60)
            return ok, bad, snap, eng.sessions
        finally:
            await eng.stop()

    ok, bad, snap, sessions = asyncio.run(main())

    assert ok.status == "completed" and ok.failed_items == 0
    # odd items matched no edge out of `split`, so they end the run as outputs of `split`
    assert sorted(o["output"]["i"] for o in ok.outputs if o["step"] == "split") == [1, 3]
    lines = [json.loads(l) for l in (tmp_path / "out" / "done.jsonl").read_text().splitlines()]
    assert sorted(l["payload"]["done"] for l in lines) == [0, 2]            # only even items went to flaky
    assert bad.status == "failed" and bad.failed_items == 1
    assert sessions["c1"]["seen"] == 1                                      # only the valid run passed the gate
    assert snap["steps"]["flaky"]["retries"] == 2 and snap["steps"]["gate"]["dead_letters"] == 1
    assert snap["steps"]["double"]["tokens_in"] == 10

    # the event log tells the same story
    import duckdb
    con = duckdb.connect(str(tmp_path / ".agentflow" / "events.duckdb"), read_only=True)
    rows = dict(con.execute("select event, count(*) from events group by event").fetchall())
    assert rows["retry"] == 2 and rows["dead_letter"] == 1 and rows["run_completed"] == 2
    dl = con.execute("select step, reason, attempt from events where event = 'dead_letter'").fetchone()
    assert dl == ("gate", "invalid_input", 1)                              # business failures are not retried
    reasons = {r[0] for r in con.execute("select reason from events where event = 'retry'").fetchall()}
    assert reasons == {"ConnectionError"}
    unrouted = con.execute("select count(*) from events where event = 'unrouted' and step = 'split'").fetchone()[0]
    assert unrouted == 2                                                    # odd items matched no edge
