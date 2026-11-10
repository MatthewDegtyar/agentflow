"""Start and supervise step processes: one per replica, each on its own port. Crashed replicas restart with
backoff; stdout/stderr go to .agentflow/logs/<step>-<replica>.log and the last lines are kept for the dashboard.

Step environment: `python`/`python3` at the start of `run:` means the step's own venv (when it ships
requirements.txt), else the interpreter running agentflow (sys.executable), never whatever `python` is first on PATH.
The SDK reaches steps through a private copy, <workdir>/sdk/agentflow_step.py, on PYTHONPATH. That way an installed
runtime never puts its whole site-packages in front of a step's own venv."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import shlex
import shutil
import socket
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from .sources import Source
from .spec import Step



def _sdk_dir() -> Path:
    """Directory holding agentflow_step.py: the checkout's sdk/python, else wherever the wheel installed it."""
    repo = Path(__file__).resolve().parents[1] / "sdk" / "python"
    if (repo / "agentflow_step.py").exists():
        return repo
    spec = importlib.util.find_spec("agentflow_step")
    if spec is None or spec.origin is None:
        raise RuntimeError("agentflow_step not found: reinstall agentflow or run from a checkout")
    return Path(spec.origin).resolve().parent


SDK_DIR = _sdk_dir()


def step_sdk_dir(work: Path) -> Path:
    """A directory containing only agentflow_step.py (copied from SDK_DIR), for step PYTHONPATHs."""
    dest = work / "sdk"
    src = SDK_DIR / "agentflow_step.py"
    target = dest / "agentflow_step.py"
    if not target.exists() or target.read_bytes() != src.read_bytes():
        dest.mkdir(parents=True, exist_ok=True)
        tmp = dest / f".agentflow_step.{os.getpid()}.tmp"
        shutil.copyfile(src, tmp)
        os.replace(tmp, target)
    return dest


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class Replica:
    step: str
    index: int
    port: int
    proc: asyncio.subprocess.Process | None = None
    healthy: bool = False
    inflight: int = 0
    restarts: int = 0
    started_at: float = 0.0
    tail: deque = field(default_factory=lambda: deque(maxlen=200))

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


class StepProcesses:
    def __init__(self, name: str, step: Step, source: Source, work: Path, events):
        self.name, self.step, self.source, self.work, self.events = name, step, source, work, events
        self.replicas = [Replica(name, i, free_port()) for i in range(step.replicas)]
        self._stopping = False
        self._tasks: list[asyncio.Task] = []
        self.sdk_dir = step_sdk_dir(work)

    def _cmd(self) -> list[str]:
        argv = shlex.split(self.step.run)
        if argv and argv[0] in ("python", "python3"):
            argv[0] = self.source.python or sys.executable   # the step's own venv, else the runtime's interpreter
        return argv

    async def _spawn(self, r: Replica):
        env = {**os.environ, **self.step.env, "PORT": str(r.port), "AGENTFLOW_STEP": self.name,
               "AGENTFLOW_REPLICA": str(r.index), "PYTHONUNBUFFERED": "1",
               "PYTHONPATH": os.pathsep.join(filter(None, [str(self.sdk_dir), os.environ.get("PYTHONPATH")]))}
        log_path = self.work / "logs" / f"{self.name}-{r.index}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        r.proc = await asyncio.create_subprocess_exec(*self._cmd(), cwd=self.source.dir, env=env,
                                                      stdout=asyncio.subprocess.PIPE,
                                                      stderr=asyncio.subprocess.STDOUT)
        r.started_at = time.time()
        self.events.emit("step_process", step=self.name, replica=r.index, status="started",
                         data={"pid": r.proc.pid, "port": r.port, "cmd": self._cmd(), "restarts": r.restarts})
        asyncio.create_task(self._pump(r, log_path))

    async def _pump(self, r: Replica, log_path: Path):
        with log_path.open("ab") as f:
            while r.proc and r.proc.stdout:
                line = await r.proc.stdout.readline()
                if not line:
                    break
                f.write(line)
                f.flush()
                r.tail.append(line.decode(errors="replace").rstrip())

    async def _wait_healthy(self, r: Replica, timeout: float = 30) -> None:
        deadline = time.time() + timeout
        async with httpx.AsyncClient(timeout=1.0) as c:
            while time.time() < deadline:
                if r.proc and r.proc.returncode is not None:
                    raise RuntimeError(f"{self.name}[{r.index}] exited during startup (code {r.proc.returncode}): "
                                       + " | ".join(list(r.tail)[-5:]))
                try:
                    if (await c.get(r.url + "/health")).status_code == 200:
                        r.healthy = True
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)
        raise RuntimeError(f"{self.name}[{r.index}] did not become healthy in {timeout}s: "
                           + " | ".join(list(r.tail)[-5:]))

    async def _supervise(self, r: Replica):
        while not self._stopping:
            code = await r.proc.wait()
            r.healthy = False
            if self._stopping:
                return
            r.restarts += 1
            self.events.emit("step_process", step=self.name, replica=r.index, status="exited",
                             error=f"exit code {code}", message=" | ".join(list(r.tail)[-5:]))
            await asyncio.sleep(min(30, 0.5 * 2 ** min(r.restarts, 6)))
            try:
                await self._spawn(r)
                await self._wait_healthy(r)
            except Exception as e:  # keep supervising; the next loop waits on the new process
                self.events.emit("step_process", step=self.name, replica=r.index, status="restart_failed",
                                 error=str(e)[:500])

    async def start(self):
        await asyncio.gather(*(self._spawn(r) for r in self.replicas))
        await asyncio.gather(*(self._wait_healthy(r) for r in self.replicas))
        self._tasks = [asyncio.create_task(self._supervise(r)) for r in self.replicas]

    def pick(self) -> Replica | None:
        """Least-inflight healthy replica."""
        healthy = [r for r in self.replicas if r.healthy]
        return min(healthy, key=lambda r: r.inflight) if healthy else None

    async def stop(self):
        self._stopping = True
        for r in self.replicas:
            if r.proc and r.proc.returncode is None:
                r.proc.terminate()
        for r in self.replicas:
            if r.proc:
                try:
                    await asyncio.wait_for(r.proc.wait(), 5)
                except asyncio.TimeoutError:
                    r.proc.kill()
        for t in self._tasks:
            t.cancel()
