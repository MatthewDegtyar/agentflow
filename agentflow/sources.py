"""Fetch each step's code: clone/update `repo@ref` from GitHub (or any git URL), or use a local `path`.

Private repos: set GITHUB_TOKEN (never logged). The resolved commit SHA is recorded so every event can be traced
back to exact code. `path:` wins over `repo:` unless AGENTFLOW_USE_REPOS=1, which lets the same YAML work before
and after the step repos exist.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .spec import Step


class SourceError(RuntimeError):
    pass


@dataclass
class Source:
    step: str
    dir: Path
    origin: str            # "path" | "repo"
    commit: str | None
    python: str | None     # interpreter to use if the step has its own venv


def _git_url(repo: str) -> str:
    if repo.startswith(("https://", "http://", "git@", "file://", "/")):
        return repo
    url = "https://" + repo.removesuffix(".git") + ".git"
    token = os.environ.get("GITHUB_TOKEN")
    if token and "github.com" in url:
        url = url.replace("https://", f"https://x-access-token:{token}@", 1)
    return url


def _git(args: list[str], cwd: Path | None = None) -> str:
    r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, timeout=300,
                       env={**os.environ, "GIT_TERMINAL_PROMPT": "0"})
    if r.returncode != 0:
        msg = (r.stderr or r.stdout).strip().replace(os.environ.get("GITHUB_TOKEN") or "\0", "***")
        raise SourceError(f"git {args[0]} failed: {msg[:400]}")
    return r.stdout.strip()


def _clone_or_update(repo: str, ref: str, dest: Path) -> str:
    url = _git_url(repo)
    if (dest / ".git").exists():
        _git(["fetch", "--depth", "1", "origin", ref], cwd=dest)
        _git(["checkout", "-q", "--force", "FETCH_HEAD"], cwd=dest)
    else:
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            _git(["clone", "-q", "--depth", "1", "--branch", ref, url, str(dest)])
        except SourceError:
            # ref may be a commit SHA, which --branch can't take
            _git(["init", "-q", str(dest)])
            _git(["remote", "add", "origin", url], cwd=dest)
            _git(["fetch", "--depth", "1", "origin", ref], cwd=dest)
            _git(["checkout", "-q", "FETCH_HEAD"], cwd=dest)
    return _git(["rev-parse", "HEAD"], cwd=dest)


def _local_commit(d: Path) -> str | None:
    try:
        sha = _git(["rev-parse", "HEAD"], cwd=d)
        dirty = _git(["status", "--porcelain"], cwd=d)
        return sha + ("-dirty" if dirty else "")
    except SourceError:
        return None


def _python_env(name: str, d: Path, work: Path, log) -> str | None:
    """If the step ships requirements.txt, give it its own venv (cached by requirements hash)."""
    req = d / "requirements.txt"
    if not req.exists() or not req.read_text().strip():
        return None
    digest = hashlib.sha256(req.read_bytes()).hexdigest()[:12]
    env_dir = work / "envs" / f"{name}-{digest}"
    py = env_dir / "bin" / "python"
    if py.exists():
        return str(py)
    log(f"creating venv for {name} ({digest})")
    if shutil.which("uv"):
        cmds = [["uv", "venv", "-q", str(env_dir)], ["uv", "pip", "install", "-q", "--python", str(py), "-r", str(req)]]
    else:
        cmds = [["python3", "-m", "venv", str(env_dir)], [str(py), "-m", "pip", "install", "-q", "-r", str(req)]]
    for c in cmds:
        r = subprocess.run(c, capture_output=True, text=True, timeout=900)
        if r.returncode != 0:
            raise SourceError(f"{name}: environment setup failed: {(r.stderr or r.stdout)[-400:]}")
    return str(py)


def fetch(name: str, step: Step, base_dir: Path, work: Path, log=print) -> Source:
    use_repos = os.environ.get("AGENTFLOW_USE_REPOS") == "1"
    if step.path and not (use_repos and step.repo):
        d = (base_dir / step.path).resolve()
        if not d.is_dir():
            raise SourceError(f"{name}: path {d} does not exist")
        origin, commit = "path", _local_commit(d)
    else:
        d = work / "repos" / name
        log(f"fetching {step.repo}@{step.ref} for {name}")
        commit = _clone_or_update(step.repo, step.ref, d)
        origin = "repo"
        if step.path:                      # with a repo, `path` means a subdirectory inside it
            d = d / step.path
    if step.setup:
        r = subprocess.run(step.setup, shell=True, cwd=d, capture_output=True, text=True, timeout=900)
        if r.returncode != 0:
            raise SourceError(f"{name}: setup `{step.setup}` failed: {(r.stderr or r.stdout)[-400:]}")
    python = _python_env(name, d, work, log)
    return Source(step=name, dir=d, origin=origin, commit=commit, python=python)
