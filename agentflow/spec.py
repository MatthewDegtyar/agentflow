"""Workflow YAML: schema, validation and loading. Fails fast with a precise message on any mistake."""
from __future__ import annotations

import os
import re
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .expr import ExprError, compile_expr

SINK_TYPES = {"jsonl", "log"}
_ENV = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


class SpecError(ValueError):
    pass


class Step(BaseModel):
    model_config = ConfigDict(extra="forbid")
    repo: str | None = None               # github.com/org/name (or any git URL)
    ref: str = "main"
    path: str | None = None               # local directory (dev override, or subdirectory of the repo)
    run: str                              # command that starts the step server; the SDK reads $PORT
    setup: str | None = None              # optional one-off command after fetch (default: install requirements.txt)
    replicas: int = Field(1, ge=1, le=64)
    concurrency: int = Field(4, ge=1, le=512)
    timeout_s: float = Field(60, gt=0)
    retries: int = Field(2, ge=0, le=10)
    max_queue: int = Field(1000, ge=1)
    env: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _source(self):
        if not self.repo and not self.path:
            raise ValueError("a step needs `repo` or `path`")
        return self


class Edge(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    from_: str = Field(alias="from")
    to: str
    when: str | None = None
    with_: dict = Field(default_factory=dict, alias="with")

    @field_validator("when")
    @classmethod
    def _when(cls, v):
        if v is not None:
            compile_expr(v)
        return v


class Session(BaseModel):
    model_config = ConfigDict(extra="forbid")
    key: str | None = None


class Workflow(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    steps: dict[str, Step]
    flow: list[Edge]
    session: Session = Field(default_factory=Session)
    examples: list[dict] = Field(default_factory=list)
    max_hops: int = 50                    # guards against routing loops

    @model_validator(mode="after")
    def _graph(self):
        names = set(self.steps)
        for n in names:
            if not re.fullmatch(r"[a-z][a-z0-9_-]{0,40}", n) or n == "input":
                raise ValueError(f"step name {n!r} must be lowercase letters/digits/-/_ and not 'input'")
        for e in self.flow:
            if e.from_ != "input" and e.from_ not in names:
                raise ValueError(f"edge from unknown step {e.from_!r}")
            if e.to.startswith("sink."):
                if e.to[5:] not in SINK_TYPES:
                    raise ValueError(f"unknown sink {e.to!r}; available: {sorted('sink.' + s for s in SINK_TYPES)}")
                if e.to == "sink.jsonl" and "path" not in e.with_:
                    raise ValueError("sink.jsonl needs `with: {path: ...}`")
            elif e.to not in names:
                raise ValueError(f"edge to unknown step {e.to!r}")
        if not any(e.from_ == "input" for e in self.flow):
            raise ValueError("the flow needs at least one edge `from: input`")
        reachable, frontier = set(), {"input"}
        while frontier:
            nxt = {e.to for e in self.flow if e.from_ in frontier and e.to in names} - reachable
            reachable |= nxt
            frontier = nxt
        orphans = names - reachable
        if orphans:
            raise ValueError(f"steps not reachable from input: {sorted(orphans)}")
        return self

    def edges_from(self, node: str) -> list[Edge]:
        return [e for e in self.flow if e.from_ == node]


def _interpolate(obj):
    if isinstance(obj, str):
        def sub(m):
            val = os.environ.get(m.group(1), m.group(2))
            if val is None:
                raise SpecError(f"environment variable ${{{m.group(1)}}} is not set")
            return val
        return _ENV.sub(sub, obj)
    if isinstance(obj, dict):
        return {k: _interpolate(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate(v) for v in obj]
    return obj


def load(path: str | Path) -> tuple[Workflow, Path, str]:
    """Returns (workflow, base_dir for relative paths, raw yaml text)."""
    p = Path(path).resolve()
    text = p.read_text()
    try:
        raw = _interpolate(yaml.safe_load(text))
        return Workflow.model_validate(raw), p.parent, text
    except ValidationError as e:
        msgs = "; ".join(f"{'.'.join(str(x) for x in err['loc'])}: {err['msg']}" for err in e.errors())
        raise SpecError(f"{p.name}: {msgs}") from None
    except (ExprError, yaml.YAMLError) as e:
        raise SpecError(f"{p.name}: {e}") from None
