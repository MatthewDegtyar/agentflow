from pathlib import Path

import pytest

from agentflow.events import check_readonly
from agentflow.expr import ExprError, compile_expr, evaluate, lookup
from agentflow.spec import SpecError, load

ROOT = Path(__file__).resolve().parents[1]


def write(tmp_path, text):
    p = tmp_path / "wf.yaml"
    p.write_text(text)
    (tmp_path / "s").mkdir(exist_ok=True)
    return p


BASE = """
name: t
steps:
  a: {path: s, run: python main.py}
  b: {path: s, run: python main.py}
flow:
  - {from: input, to: a}
  - {from: a, to: b}
"""


def test_demo_workflow_is_valid():
    wf, base, text = load(ROOT / "workflows" / "doc-triage.yaml")
    assert set(wf.steps) == {"intake", "classify", "summarize", "notify"} and "flow:" in text


def test_valid_minimal(tmp_path):
    wf, _, _ = load(write(tmp_path, BASE))
    assert [e.to for e in wf.edges_from("a")] == ["b"]


@pytest.mark.parametrize("patch,msg", [
    (("to: b}", "to: nope}"), "unknown step"),
    (("  - {from: a, to: b}\n", ""), "not reachable"),
    (("to: b}", "to: b, when: 'output.x ==' }"), "invalid expression"),
    (("to: b}", "to: sink.jsonl}"), "needs `with: {path"),
    (("to: b}", "to: sink.kafka}"), "unknown sink"),
    (("run: python main.py}", "run: python main.py, bogus: 1}"), "bogus"),
    (("path: s, run", "run"), "needs `repo` or `path`"),
])
def test_invalid_specs(tmp_path, patch, msg):
    text = BASE.replace(*patch, 1)
    with pytest.raises(SpecError, match=msg):
        load(write(tmp_path, text))


def test_env_interpolation(tmp_path, monkeypatch):
    text = BASE.replace("run: python main.py}", "run: python main.py, env: {TOKEN: '${MY_TOKEN}'}}", 1)
    with pytest.raises(SpecError, match="MY_TOKEN"):
        load(write(tmp_path, text))
    monkeypatch.setenv("MY_TOKEN", "abc")
    wf, _, _ = load(write(tmp_path, text))
    assert wf.steps["a"].env["TOKEN"] == "abc"


def test_expressions():
    ctx = {"output": {"kind": "report", "n": 3, "tags": ["x"]}, "input": {"customer_id": "acme"}}
    ok = lambda s: evaluate(compile_expr(s), ctx)
    assert ok("output.kind in ['report', 'contract']")
    assert ok("output.n > 2 and not output.missing")
    assert ok("output.flag == true") is False            # missing key → None, never an exception
    assert ok("output.n < 'x'") is False                  # type mismatch → no match
    assert ok("output.tags[0] == 'x'")
    assert lookup("input.customer_id", ctx) == "acme"
    for bad in ("__import__('os')", "output.__class__", "len(output)", "output.kind if 1 else 2"):
        with pytest.raises(ExprError):
            compile_expr(bad)


def test_readonly_guard():
    assert check_readonly("select * from events;").startswith("select")
    for bad in ("drop table events", "select 1; drop table events", "copy events to '/tmp/x'",
                "select * from read_csv('/etc/passwd')", "attach 'x.db'", "install httpfs"):
        with pytest.raises(ValueError):
            check_readonly(bad)
