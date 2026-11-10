"""Safe evaluator for `when:` routing conditions and dotted paths like `input.customer_id`.

Allowed: literals, names from the context (output, input, session, meta), attribute/subscript access on dicts,
comparisons (== != < <= > >= in, not in, is), and/or/not, lists/tuples. No calls, no imports, no dunder access.
Missing keys evaluate to None instead of raising, so `output.flag == true` is simply false when `flag` is absent.
"""
from __future__ import annotations

import ast
import operator

_CMP = {ast.Eq: operator.eq, ast.NotEq: operator.ne, ast.Lt: operator.lt, ast.LtE: operator.le,
        ast.Gt: operator.gt, ast.GtE: operator.ge, ast.In: lambda a, b: a in b, ast.NotIn: lambda a, b: a not in b,
        ast.Is: operator.is_, ast.IsNot: operator.is_not}
_NAMES = {"true": True, "false": False, "null": None, "none": None, "True": True, "False": False, "None": None}


class ExprError(ValueError):
    pass


def compile_expr(src: str) -> ast.Expression:
    try:
        tree = ast.parse(src.strip(), mode="eval")
    except SyntaxError as e:
        raise ExprError(f"invalid expression {src!r}: {e.msg}") from e
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.UnaryOp, ast.Not, ast.USub,
                                 ast.Compare, ast.Name, ast.Load, ast.Constant, ast.Attribute, ast.Subscript,
                                 ast.List, ast.Tuple, *_CMP)):
            raise ExprError(f"{type(node).__name__} is not allowed in {src!r}")
        if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            raise ExprError(f"private attribute access is not allowed in {src!r}")
    return tree


def _get(obj, key):
    if isinstance(obj, dict):
        return obj.get(key)
    if isinstance(obj, (list, tuple)) and isinstance(key, int) and -len(obj) <= key < len(obj):
        return obj[key]
    return None


def _eval(node, ctx):
    if isinstance(node, ast.Expression):
        return _eval(node.body, ctx)
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return ctx[node.id] if node.id in ctx else _NAMES.get(node.id)
    if isinstance(node, ast.Attribute):
        return _get(_eval(node.value, ctx), node.attr)
    if isinstance(node, ast.Subscript):
        return _get(_eval(node.value, ctx), _eval(node.slice, ctx))
    if isinstance(node, (ast.List, ast.Tuple)):
        return [_eval(e, ctx) for e in node.elts]
    if isinstance(node, ast.BoolOp):
        vals = (_eval(v, ctx) for v in node.values)
        return all(vals) if isinstance(node.op, ast.And) else any(vals)
    if isinstance(node, ast.UnaryOp):
        v = _eval(node.operand, ctx)
        return (not v) if isinstance(node.op, ast.Not) else -v
    if isinstance(node, ast.Compare):
        left = _eval(node.left, ctx)
        for op, right_node in zip(node.ops, node.comparators):
            right = _eval(right_node, ctx)
            try:
                if not _CMP[type(op)](left, right):
                    return False
            except TypeError:
                return False              # e.g. None < 3: treat as no match rather than crash the router
            left = right
        return True
    raise ExprError(f"unsupported node {type(node).__name__}")


def evaluate(tree: ast.Expression, ctx: dict):
    return _eval(tree, ctx)


def lookup(path: str, ctx: dict):
    """Resolve a dotted path like `input.customer_id` against the context."""
    return evaluate(compile_expr(path), ctx)
