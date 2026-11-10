"""agentflow step SDK: one file, standard library only. The runtime puts it on PYTHONPATH for every step.

    from agentflow_step import Step

    step = Step()

    @step.handler
    def handle(ctx, payload):
        ctx.log("classifying", chars=len(payload["text"]))
        ctx.usage(model="claude-sonnet-5", input_tokens=812, output_tokens=64)
        return {"kind": "report", "priority": "normal"}   # or ctx.emit(...) several times to fan out

    step.run()          # serves on $PORT

A step never knows where its output goes; the workflow YAML routes it. The contract with the runtime is one HTTP
call per item: POST /invoke {run_id, item_id, session_id, step, attempt, input, session} → {outputs, logs, usage,
session_patch}. Raising an exception = retryable failure. `ctx.fail(reason, detail)` = business failure: no retry.
"""
from __future__ import annotations

import json
import os
import sys
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class StepFailed(Exception):
    """Raised by ctx.fail(): a deliberate, non-retryable failure with a machine-readable reason."""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(f"{reason}: {detail}")
        self.reason, self.detail = reason, detail


class Context:
    def __init__(self, req: dict):
        self.run_id = req.get("run_id")
        self.item_id = req.get("item_id")
        self.session_id = req.get("session_id")
        self.step = req.get("step")
        self.attempt = req.get("attempt", 1)
        self.meta = req.get("meta", {})
        self.session = dict(req.get("session") or {})     # shared state for this session across steps
        self._session_in = dict(self.session)
        self._outputs: list[dict] = []
        self._logs: list[dict] = []
        self._usage: list[dict] = []

    def log(self, message: str, level: str = "info", **fields):
        self._logs.append({"ts": time.time(), "level": level, "message": str(message), "fields": fields})

    def emit(self, payload):
        """Send one output downstream. Call several times to fan out."""
        self._outputs.append(payload)

    def usage(self, model: str, input_tokens: int = 0, output_tokens: int = 0, cost_usd: float | None = None, **extra):
        self._usage.append({"model": model, "input_tokens": int(input_tokens), "output_tokens": int(output_tokens),
                            "cost_usd": cost_usd, **extra})

    def fail(self, reason: str, detail: str = ""):
        raise StepFailed(reason, detail)

    def _session_patch(self) -> dict:
        return {k: v for k, v in self.session.items() if self._session_in.get(k, object()) != v}


class Step:
    def __init__(self, name: str | None = None):
        self.name = name or os.environ.get("AGENTFLOW_STEP", "step")
        self._handler = None

    def handler(self, fn):
        self._handler = fn
        return fn

    def _invoke(self, req: dict) -> tuple[int, dict]:
        ctx = Context(req)
        try:
            result = self._handler(ctx, req.get("input"))
            if result is not None and not ctx._outputs:
                ctx.emit(result)
            return 200, {"outputs": ctx._outputs, "logs": ctx._logs, "usage": ctx._usage,
                         "session_patch": ctx._session_patch()}
        except StepFailed as e:
            return 422, {"error": {"reason": e.reason, "detail": e.detail, "retryable": False},
                         "logs": ctx._logs, "usage": ctx._usage}
        except Exception as e:  # noqa: BLE001 - any crash is reported to the runtime, which decides on retry
            tb = traceback.format_exc(limit=8)
            return 500, {"error": {"reason": type(e).__name__, "detail": f"{type(e).__name__}: {e}",
                                   "traceback": tb, "retryable": True},
                         "logs": ctx._logs, "usage": ctx._usage}

    def run(self, port: int | None = None):
        if self._handler is None:
            raise RuntimeError("no handler registered: decorate a function with @step.handler")
        step = self
        port = int(port or os.environ.get("PORT", "8000"))

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _send(self, code, body):
                data = json.dumps(body, default=str).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                if self.path == "/health":
                    self._send(200, {"ok": True, "step": step.name})
                else:
                    self._send(404, {"error": "not found"})

            def do_POST(self):
                if self.path != "/invoke":
                    return self._send(404, {"error": "not found"})
                try:
                    req = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
                except json.JSONDecodeError:
                    return self._send(400, {"error": {"reason": "bad_request", "detail": "invalid JSON"}})
                self._send(*step._invoke(req))

            def log_message(self, *args):   # keep stdout for the step's own logs
                pass

        server = ThreadingHTTPServer(("127.0.0.1", port), H)
        server.daemon_threads = True
        print(f"[agentflow] step {self.name!r} listening on :{port}", file=sys.stderr, flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
