"""
Tests for ``datajunction.sql.inference``.
"""

from typing import Any

import pytest
from sqloxide import parse_sql

from datajunction.models.database import Column, Database, Table
from datajunction.models.node import Node
from datajunction.sql.inference import evaluate_expression, get_column_from_expression
from datajunction.sql.parse import find_nodes_by_key


def get_expression(sql: str) -> Any:
    """
    Return the first expression of the first projection.
    """
    tree = parse_sql(sql, dialect="ansi")
    projection = next(find_nodes_by_key(tree, "projection"))
    expression = projection[0]

    if "UnnamedExpr" in expression:
        return expression["UnnamedExpr"]
    if "ExprWithAlias" in expression:
        return expression["ExprWithAlias"]["expr"]
    if expression == "Wildcard":
        return expression

    raise Exception(f"Unable to evaluate expression: {expression}")


def test_evaluate_expression() -> None:
    """
    Test ``evaluate_expression``.
    """
    assert evaluate_expression([], get_expression("SELECT 1")) == 1
    assert evaluate_expression([], get_expression("SELECT 1.1")) == 1.1
    assert evaluate_expression([], get_expression("SELECT 'test'")) == "test"

    node_a = Node(
        name="A",
        tables=[
            Table(
                database=Database(name="test", URI="sqlite://"),
                table="A",
                columns=[
                    Column(name="ds", type="str"),
                    Column(name="user_id", type="int"),
                    Column(name="foo", type="float"),
                ],
            ),
        ],
    )

    assert evaluate_expression([node_a], get_expression("SELECT ds")) == Column(
        name="ds",
        type="str",
    )
    assert (
        evaluate_expression(
            [node_a],
            get_expression("SELECT MAX(foo)"),
            "bar",
        )
        == Column(name="bar", type="float")
    )
    assert (
        evaluate_expression(
            [node_a],
            get_expression("SELECT MAX(MAX(foo))"),
            "bar",
        )
        == Column(name="bar", type="float")
    )
    assert (
        evaluate_expression(
            [node_a],
            get_expression("SELECT COUNT(MAX(foo))"),
            "bar",
        )
        == Column(name="bar", type="int")
    )


def test_evaluate_expression_ambiguous() -> None:
    """
    Test ``evaluate_expression``.

    In this test we select a column without using the fully qualified notation, and it
    exists in multiple parents.
    """
    node_a = Node(
        name="A",
        tables=[
            Table(
                database=Database(name="test", URI="sqlite://"),
                table="A",
                columns=[
                    Column(name="ds", type="str"),
                    Column(name="user_id", type="int"),
                    Column(name="foo", type="float"),
                ],
            ),
        ],
    )

    node_b = Node(
        name="B",
        tables=[
            Table(
                database=Database(name="test", URI="sqlite://"),
                table="B",
                columns=[
                    Column(name="ds", type="str"),
                ],
            ),
        ],
    )

    with pytest.raises(Exception) as excinfo:
        evaluate_expression([node_a, node_b], get_expression("SELECT ds"))
    assert str(excinfo.value) == 'Unable to determine origin of column "ds"'

    # using fully qualified notation
    assert (
        evaluate_expression(
            [node_a, node_b],
            get_expression("SELECT A.ds"),
        )
        == Column(name="ds", type="str")
    )

    # invalid parent
    with pytest.raises(Exception) as excinfo:
        evaluate_expression([node_a, node_b], get_expression("SELECT C.ds"))
    assert str(excinfo.value) == 'Unable to determine origin of column "C.ds"'

    # invalid column
    with pytest.raises(Exception) as excinfo:
        evaluate_expression([node_a, node_b], get_expression("SELECT A.invalid"))
    assert str(excinfo.value) == 'Unable to find column "invalid" in node "A"'


def test_evaluate_expression_parent_no_columns() -> None:
    """
    Test ``evaluate_expression``.

    Test for when one of the parents has no columns. This should never happen.
    """
    node_a = Node(
        name="A",
        tables=[
            Table(
                database=Database(name="test", URI="sqlite://"),
                table="A",
                columns=[],
            ),
        ],
    )

    node_b = Node(
        name="B",
        tables=[
            Table(
                database=Database(name="test", URI="sqlite://"),
                table="B",
                columns=[
                    Column(name="ds", type="str"),
                    Column(name="user_id", type="int"),
                    Column(name="foo", type="float"),
                ],
            ),
        ],
    )

    assert evaluate_expression([node_a, node_b], get_expression("SELECT ds")) == Column(
        name="ds",
        type="str",
    )


def test_get_column_from_expression() -> None:
    """
    Test ``get_column_from_expression``.
    """
    assert get_column_from_expression([], get_expression("SELECT 1")) == Column(
        name=None,
        type="int",
    )
    assert get_column_from_expression([], get_expression("SELECT 1.1")) == Column(
        name=None,
        type="float",
    )
    assert get_column_from_expression([], get_expression("SELECT 'test'")) == Column(
        name=None,
        type="str",
    )

    with pytest.raises(Exception) as excinfo:
        get_column_from_expression([], get_expression("SELECT * FROM A"))
    assert str(excinfo.value) == "Invalid expression for column: Wildcard"
