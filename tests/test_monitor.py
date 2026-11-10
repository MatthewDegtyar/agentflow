"""Monitor mode: watch engines embedded in other processes through their work dirs (engine.json, state.json,
events.jsonl), while they run and after they stop."""
import asyncio
import json
import os

from fastapi.testclient import TestClient

from agentflow.engine import Engine
from agentflow.spec import load
from tests.test_engine import make


def test_monitor_live_and_stopped(tmp_path, monkeypatch):
    root = tmp_path / "root"
    work = root / "acme" / "e2e"
    wf, base, text = load(make(tmp_path))
    monkeypatch.setenv("AGENTFLOW_MONITOR_ROOTS", str(root))
    from agentflow import monitor
    client = TestClient(monitor.app)
    seen_live = {}

    async def main():
        async_eng = Engine(wf, base, text, work)
        await async_eng.start(log=lambda *_: None)
        try:
            rid = await async_eng.start_run({"customer": "c1", "n": 2})
            bad = await async_eng.start_run({"customer": "c1", "n": -1})
            await asyncio.wait_for(async_eng.runs[rid].done.wait(), 30)
            await asyncio.wait_for(async_eng.runs[bad].done.wait(), 30)
            await asyncio.sleep(1.2)                     # one background tick: events flushed, state.json refreshed
            # while the engine is up (and holding its DuckDB lock), the monitor still sees everything
            seen_live["workflows"] = await asyncio.to_thread(lambda: client.get("/api/workflows").json())
            seen_live["metrics"] = await asyncio.to_thread(lambda: client.get("/api/metrics", params={"w": "acme/e2e"}).json())
            seen_live["runs"] = await asyncio.to_thread(lambda: client.get("/api/runs", params={"w": "acme/e2e"}).json())
            return rid
        finally:
            await async_eng.stop()

    rid = asyncio.run(main())

    (wl,) = seen_live["workflows"]
    assert wl["id"] == "acme/e2e" and wl["name"] == "e2e" and wl["live"] is True and wl["pid"] == os.getpid()
    assert wl["runs"] == 2 and wl["runs_completed"] == 1 and wl["runs_failed"] == 1 and wl["cost_usd"] > 0
    assert seen_live["metrics"]["live"] is True and set(seen_live["metrics"]["steps"]) == {"gate", "double", "split", "flaky"}
    assert len(seen_live["runs"]["rows"]) == 2

    # after stop: still listed, not live, history intact
    assert json.loads((work / "engine.json").read_text())["status"] == "stopped"
    (wl,) = client.get("/api/workflows").json()
    assert wl["live"] is False and wl["status"] == "stopped"
    topo = client.get("/api/workflow", params={"w": "acme/e2e"}).json()
    assert topo["name"] == "e2e" and "flaky" in topo["steps"] and topo["live"] is False and "yaml" in topo
    detail = client.get(f"/api/runs/{rid}", params={"w": "acme/e2e"}).json()
    assert {"run_started", "run_completed", "retry"} <= {r[1] for r in detail["rows"]}
    q = client.post("/api/query", params={"w": "acme/e2e"},
                    json={"sql": "select count(*) from events where event = 'dead_letter'"}).json()
    assert q["rows"][0][0] == 1
    assert client.get("/api/steps/gate/logs", params={"w": "acme/e2e"}).status_code == 200
    assert client.get("/api/health").json()["mode"] == "monitor"

    # guards
    assert client.get("/api/runs").status_code == 400                                       # needs ?w=
    assert client.get("/api/runs", params={"w": "nope"}).status_code == 404
    assert client.post("/api/query", params={"w": "acme/e2e"}, json={"sql": "drop table events"}).status_code == 400


def test_monitor_detects_lost_engine(tmp_path, monkeypatch):
    """engine.json says running but the process is gone: reported as lost, not live."""
    work = tmp_path / "root" / "w1"
    work.mkdir(parents=True)
    (work / "engine.json").write_text(json.dumps({"workflow": "w1", "status": "running", "pid": 999999,
                                                  "topology": {"name": "w1", "steps": {}, "flow": []}}))
    (work / "state.json").write_text(json.dumps({"ts": 0, "live": True}))
    (work / "events.jsonl").write_text("")
    monkeypatch.setenv("AGENTFLOW_MONITOR_ROOTS", str(tmp_path / "root"))
    from agentflow import monitor
    (wl,) = TestClient(monitor.app).get("/api/workflows").json()
    assert wl["live"] is False and wl["status"] == "lost" and wl["runs"] == 0
