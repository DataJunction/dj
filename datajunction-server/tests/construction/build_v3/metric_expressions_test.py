"""Stage boundaries and ownership contracts for derived metric expressions."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from datajunction_server.construction.build_v3.metrics import resolve_metric_expressions
from datajunction_server.construction.build_v3.types import BuildContext, MetricExprInfo
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse

from . import assert_sql_equal


def expression_context():
    definitions = {
        "v3.revenue": "SELECT SUM(amount) FROM fact",
        "v3.orders": "SELECT COUNT(DISTINCT order_id) FROM fact",
        "v3.ratio": "SELECT v3.revenue / v3.orders",
        "v3.nested": "SELECT (v3.ratio + 1) * 2",
    }
    ctx = MagicMock(spec=BuildContext)
    ctx.nodes = {
        name: SimpleNamespace(
            name=name,
            type=NodeType.METRIC,
            current=SimpleNamespace(query=query),
        )
        for name, query in definitions.items()
    }
    ctx.parent_map = {}
    ctx.get_parsed_query.side_effect = lambda node: parse(node.current.query)
    return ctx


def planned_expression(sql, name="unused", cte="computed"):
    return MetricExprInfo(
        expr_ast=parse("SELECT " + sql).select.projection[0],
        short_name=name,
        cte_alias=cte,
    )


@pytest.mark.parametrize("reverse", [False, True])
def test_shared_parents_preserve_planned_aggregates_and_tree_ownership(reverse):
    ctx = expression_context()
    available = {
        "v3.revenue": planned_expression("SUM(fact.amount)"),
        "v3.orders": planned_expression("MAX(counts.order_count)"),
    }
    names = ["v3.ratio", "v3.nested"]
    result = resolve_metric_expressions(
        ctx,
        names[::-1] if reverse else names,
        available,
    )
    expected = "SELECT (SUM(fact.amount) / MAX(counts.order_count) + 1) * 2"
    assert_sql_equal(f"SELECT {result['v3.nested']}", expected)

    # Editing one requested formula must not mutate a shared parent in another
    # formula, or the planner's authoritative input expressions.
    for col in result["v3.ratio"].find_all(ast.Column):
        col.name = ast.Name("changed")
    assert_sql_equal(f"SELECT {result['v3.nested']}", expected)
    assert_sql_equal(
        f"SELECT {available['v3.orders'].expr_ast}",
        "SELECT MAX(counts.order_count)",
    )


def test_planned_root_is_a_stage_boundary():
    ctx = expression_context()
    available = {"v3.ratio": planned_expression("weekly.ratio")}
    result = resolve_metric_expressions(ctx, ["v3.nested", "v3.ratio"], available)
    assert_sql_equal(f"SELECT {result['v3.nested']}", "SELECT (weekly.ratio + 1) * 2")
    assert_sql_equal(f"SELECT {result['v3.ratio']}", "SELECT weekly.ratio")


def test_unavailable_base_is_not_reconstructed_from_source_columns():
    ctx = expression_context()
    with pytest.raises(DJInvalidInputException, match="v3.orders.*at this SQL stage"):
        resolve_metric_expressions(
            ctx,
            ["v3.nested"],
            {"v3.revenue": planned_expression("SUM(fact.amount)")},
        )
