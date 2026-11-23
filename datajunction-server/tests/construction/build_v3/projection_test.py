"""Input traversal and output schema invariants for measures projections."""

import pytest

from datajunction_server.construction.build_v3.projection import (
    MeasuresProjection,
    ProjectedColumn,
    column_inputs,
    input_reference,
)
from datajunction_server.construction.build_v3.types import ColumnMetadata
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.sql.parsing.backends.antlr4 import parse


@pytest.mark.parametrize("expression", ["1", "value + 1"])
def test_input_reference_rejects_non_column_expressions(expression):
    """An expression must be walked for inputs, rather than bound as a column."""
    expression_ast = parse(f"SELECT {expression}").select.projection[0]
    with pytest.raises(DJInvalidInputException, match="Expected a column reference"):
        input_reference(expression_ast, "test.parent")


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("test.dimension.key[role]", ["test.dimension.key[role]"]),
        (
            "COALESCE(test.parent.samples[0], fallback)",
            ["test.parent.samples", "test.parent.fallback"],
        ),
        (
            "CASE WHEN enabled THEN value END",
            ["test.parent.enabled", "test.parent.value"],
        ),
    ],
)
def test_column_inputs_distinguishes_roles_from_array_indexes(expression, expected):
    """Role markers and numeric array indexes are not independent source inputs."""
    expression_ast = parse(f"SELECT {expression}").select.projection[0]
    assert [
        input_reference(column, "test.parent")
        for column in column_inputs(expression_ast)
    ] == expected


def test_projection_rejects_duplicate_alias_without_changing_existing_bindings():
    """Different sources cannot silently share an output column name."""
    projection = MeasuresProjection()
    first = ProjectedColumn(
        expression=parse("SELECT fact.quantity").select.projection[0],
        metadata=ColumnMetadata("quantity", "fact.quantity", "int", "dimension"),
        source_refs=("fact.quantity",),
    )
    projection.add(first)
    conflicting = ProjectedColumn(
        expression=parse("SELECT dimension.quantity").select.projection[0],
        metadata=ColumnMetadata("quantity", "dimension.quantity", "int", "dimension"),
        source_refs=("dimension.quantity",),
    )
    with pytest.raises(
        DJInvalidInputException,
        match="Conflicting output column alias: quantity",
    ):
        projection.add(conflicting)

    assert projection.expressions == [first.expression]
    assert projection.metadata == [first.metadata]
    assert projection.input_aliases == {"fact.quantity": "quantity"}
