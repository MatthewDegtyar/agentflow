"""Packaging: build the wheel, install it into a fresh venv, and use agentflow from outside the checkout."""
import json
import os
import shutil
import socket
import subprocess
import sys
import textwrap
import time
import zipfile
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.skipif(shutil.which("uv") is None, reason="needs uv")


@pytest.fixture(scope="module")
def installed(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("install")
    dist = tmp / "dist"
    subprocess.run(["uv", "build", "--wheel", "-q", "-o", str(dist)], cwd=ROOT, check=True)
    wheel = next(dist.glob("agentflow-*.whl"))
    venv = tmp / "venv"
    subprocess.run(["uv", "venv", "-q", "-p", f"{sys.version_info.major}.{sys.version_info.minor}", str(venv)],
                   cwd=tmp, check=True)
    py = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    subprocess.run(["uv", "pip", "install", "-q", "--python", str(py), str(wheel)], cwd=tmp, check=True)
    return wheel, py, tmp


def run_py(py: Path, cwd: Path, *args: str) -> str:
    # -I: isolated mode, so nothing from the checkout or cwd leaks onto sys.path
    r = subprocess.run([str(py), "-I", *args], cwd=cwd, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    return r.stdout


def test_wheel_contents(installed):
    wheel, _, _ = installed
    names = zipfile.ZipFile(wheel).namelist()
    assert "agentflow/__init__.py" in names and "agentflow_step.py" in names
    top = {n.split("/")[0] for n in names}
    assert not top & {"dashboard", "examples", "workflows", "tests", "sdk"}, top


def test_imports_from_install(installed):
    _, py, tmp = installed
    out = run_py(py, tmp, "-c", textwrap.dedent("""
        import json, agentflow, agentflow_step
        from agentflow import Engine, SpecError, load
        from agentflow.procs import SDK_DIR
        from agentflow_step import Step
        print(json.dumps({"pkg": agentflow.__file__, "sdk": str(SDK_DIR), "step": agentflow_step.__file__}))
    """))
    paths = json.loads(out)
    for p in paths.values():
        assert "site-packages" in p and str(ROOT) not in p, paths


def test_validate_from_install(installed):
    _, py, tmp = installed
    out = run_py(py, tmp, "-m", "agentflow", "validate", str(ROOT / "workflows" / "doc-triage.yaml"))
    assert out.startswith("ok: doc-triage")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_up_from_install(installed):
    """`up` from the installed package: step `path:` and sink paths resolve relative to the YAML, not the install
    or the cwd, and the step process imports agentflow_step from the installed wheel."""
    _, py, tmp = installed
    proj = tmp / "proj"
    (proj / "steps" / "echo").mkdir(parents=True)
    (proj / "steps" / "echo" / "main.py").write_text(textwrap.dedent("""
        from agentflow_step import Step
        step = Step()

        @step.handler
        def handle(ctx, x):
            return {"echo": x["msg"]}

        step.run()
    """))
    (proj / "flow.yaml").write_text(textwrap.dedent("""
        name: installed
        steps:
          echo: {path: steps/echo, run: python main.py}
        flow:
          - {from: input, to: echo}
          - {from: echo, to: sink.jsonl, with: {path: out/echo.jsonl}}
    """))
    elsewhere = tmp / "elsewhere"
    elsewhere.mkdir()
    port = free_port()
    env = {**os.environ, "PATH": f"{py.parent}{os.pathsep}{os.environ['PATH']}"}   # `python` → the install's venv
    env.pop("PYTHONPATH", None)
    proc = subprocess.Popen([str(py), "-I", "-m", "agentflow", "up", "../proj/flow.yaml", "--port", str(port),
                             "--workdir", str(tmp / "work")], cwd=elsewhere, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        base = f"http://127.0.0.1:{port}"
        deadline = time.time() + 60
        while True:
            assert proc.poll() is None, proc.stdout.read()
            try:
                if httpx.get(f"{base}/api/health", timeout=2).json()["ok"]:
                    break
            except httpx.HTTPError:
                pass
            assert time.time() < deadline, "runtime never became healthy"
            time.sleep(0.3)
        r = httpx.post(f"{base}/api/runs", json={"input": {"msg": "hi"}, "wait": True}, timeout=30).json()
        assert r["status"] == "completed", r
        sink = proj / "out" / "echo.jsonl"
        rec = json.loads(sink.read_text().splitlines()[0])                 # sink path is relative to flow.yaml
        assert rec["step"] == "echo" and rec["payload"] == {"echo": "hi"}, rec
        assert (tmp / "work").is_dir()
    finally:
        proc.terminate()
        proc.wait(timeout=15)
