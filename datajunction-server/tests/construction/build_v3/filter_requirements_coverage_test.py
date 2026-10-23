"""Every caller of build_metrics_sql must apply filter requirements."""

import ast
from pathlib import Path

import datajunction_server

ROOT = Path(datajunction_server.__file__).parent

# Defines or re-exports build_metrics_sql; does not call it for a request.
NOT_REQUEST_PATHS = {
    "construction/build_v3/builder.py",
    "construction/build_v3/__init__.py",
    # Only mentions build_metrics_sql in docstrings.
    "construction/build_v3/cube_matcher.py",
}


def _count_calls(source, name):
    """Count real calls to a function, not comments, docstrings or defs."""
    return sum(
        1
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", getattr(node.func, "attr", None)) == name
    )


def test_every_build_metrics_sql_caller_applies_requirements():
    short = {}
    callers = []
    for path in ROOT.rglob("*.py"):
        relative = str(path.relative_to(ROOT))
        if relative in NOT_REQUEST_PATHS:
            continue
        source = path.read_text()
        builds = _count_calls(source, "build_metrics_sql")
        applies = _count_calls(source, "apply_metric_filter_requirements")
        if builds:
            callers.append(relative)
        if applies < builds:
            short[relative] = {"build_metrics_sql": builds, "applies": applies}
    assert sorted(callers) == [
        "api/data.py",
        "api/djsql.py",
        "construction/build_v3/node_query.py",
        "internal/sql.py",
        "mcp/tools.py",
    ]
    assert short == {}
