"""Start and supervise step processes: one per replica, each on its own port. Crashed replicas restart with
backoff; stdout/stderr go to .agentflow/logs/<step>-<replica>.log and the last lines are kept for the dashboard."""
from __future__ import annotations

import asyncio
import os
import shlex
import socket
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from .sources import Source
from .spec import Step

SDK_DIR = Path(__file__).resolve().parents[1] / "sdk" / "python"


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

    def _cmd(self) -> list[str]:
        argv = shlex.split(self.step.run)
        if self.source.python and argv and argv[0] in ("python", "python3"):
            argv[0] = self.source.python          # use the step's own venv
        return argv

    async def _spawn(self, r: Replica):
        env = {**os.environ, **self.step.env, "PORT": str(r.port), "AGENTFLOW_STEP": self.name,
               "AGENTFLOW_REPLICA": str(r.index), "PYTHONUNBUFFERED": "1",
               "PYTHONPATH": os.pathsep.join(filter(None, [str(SDK_DIR), os.environ.get("PYTHONPATH")]))}
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
