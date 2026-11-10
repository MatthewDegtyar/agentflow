"""agentflow CLI.

    python -m agentflow validate workflows/doc-triage.yaml
    python -m agentflow up workflows/doc-triage.yaml [--port 7070]     # fetch steps, start them, serve the API
    python -m agentflow run '{"customer_id": "acme", "title": "...", "text": "..."}' [--wait]
    python -m agentflow load --rate 3 --duration 60
    python -m agentflow query "select step, count(*) from events group by step"
    python -m agentflow monitor path/to/.agentflow [more/roots ...] [--port 7070]   # watch embedded engines (read-only)
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import httpx


def main(argv=None):
    ap = argparse.ArgumentParser(prog="agentflow")
    ap.add_argument("--api", default=os.environ.get("AGENTFLOW_API", "http://127.0.0.1:7070"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("validate")
    v.add_argument("spec")
    u = sub.add_parser("up")
    u.add_argument("spec")
    u.add_argument("--port", type=int, default=7070)
    u.add_argument("--workdir")
    r = sub.add_parser("run")
    r.add_argument("input", help="JSON input")
    r.add_argument("--wait", action="store_true")
    ld = sub.add_parser("load")
    ld.add_argument("--rate", type=float, default=1.0)
    ld.add_argument("--duration", type=float, default=60)
    q = sub.add_parser("query")
    q.add_argument("sql")
    mo = sub.add_parser("monitor", help="read-only API over every engine work dir under the roots (for the dashboard)")
    mo.add_argument("roots", nargs="+")
    mo.add_argument("--port", type=int, default=7070)
    a = ap.parse_args(argv)

    if a.cmd == "validate":
        from .spec import SpecError, load
        try:
            wf, _, _ = load(a.spec)
        except SpecError as e:
            print(f"invalid: {e}", file=sys.stderr)
            return 1
        print(f"ok: {wf.name}: {len(wf.steps)} steps, {len(wf.flow)} edges")
        return 0
    if a.cmd == "monitor":
        import uvicorn
        from .monitor import discover
        roots = [os.path.abspath(r) for r in a.roots]
        missing = [r for r in roots if not os.path.isdir(r)]
        if missing:
            print(f"not a directory: {', '.join(missing)}", file=sys.stderr)
            return 1
        os.environ["AGENTFLOW_MONITOR_ROOTS"] = os.pathsep.join(roots)
        from pathlib import Path
        n = len(discover([Path(r) for r in roots]))
        print(f"monitoring {n} work dir(s) under {', '.join(roots)} on http://127.0.0.1:{a.port}", file=sys.stderr)
        uvicorn.run("agentflow.monitor:app", host="127.0.0.1", port=a.port, log_level="warning")
        return 0
    if a.cmd == "up":
        import uvicorn
        os.environ["AGENTFLOW_SPEC"] = os.path.abspath(a.spec)
        if a.workdir:
            os.environ["AGENTFLOW_WORKDIR"] = os.path.abspath(a.workdir)
        uvicorn.run("agentflow.api:app", host="127.0.0.1", port=a.port, log_level="warning")
        return 0
    with httpx.Client(base_url=a.api, timeout=300) as c:
        if a.cmd == "run":
            resp = c.post("/api/runs", json={"input": json.loads(a.input), "wait": a.wait})
        elif a.cmd == "load":
            resp = c.post("/api/load", json={"rate": a.rate, "duration_s": a.duration})
        else:
            resp = c.post("/api/query", json={"sql": a.sql})
        print(json.dumps(resp.json(), indent=1))
        return 0 if resp.status_code < 400 else 1


if __name__ == "__main__":
    sys.exit(main())
