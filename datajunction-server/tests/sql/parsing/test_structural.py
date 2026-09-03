from dataclasses import dataclass
from decimal import Decimal

import pytest

from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.structural import (
    _serialize_number_v1,
    serialize_ast,
)


def test_serialize_ast_is_structural_and_numeric():
    integer = serialize_ast(parse("SELECT 1"))
    integral_float = serialize_ast(parse("SELECT 1.0"))
    typed = serialize_ast(
        parse("SELECT CAST(value AS DECIMAL(10, 2)) FROM source"),
    )
    decimal = serialize_ast(ast.Number(Decimal("1.50")))

    assert integer == integral_float
    assert integer["type"] == "Query"
    assert "datajunction_server." not in str(integer)
    assert typed["type"] == "Query"
    assert decimal["fields"]["value"] == {"decimal": "1.5"}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1, 1),
        (1.5, 1.5),
        (Decimal("1.0"), 1),
        (Decimal("1.50"), {"decimal": "1.5"}),
    ],
)
def test_serialize_number_v1(value, expected):
    assert _serialize_number_v1(value) == expected


def test_serialize_number_v1_rejects_booleans_and_nonfinite_values():
    with pytest.raises(TypeError, match="Boolean values are not SQL numbers"):
        _serialize_number_v1(True)
    for value in (float("inf"), Decimal("NaN")):
        with pytest.raises(ValueError, match="must be finite"):
            _serialize_number_v1(value)


def test_serialize_ast_rejects_unclassified_nodes_and_versions():
    @dataclass(eq=False)
    class FutureNode(ast.Node):
        def __str__(self) -> str:
            return "future"

    with pytest.raises(TypeError, match="Unsupported structural SQL node: FutureNode"):
        serialize_ast(FutureNode())
    with pytest.raises(TypeError, match="Unsupported structural SQL value: object"):
        serialize_ast(ast.Name(name=object()))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="Unsupported structural SQL serialization"):
        serialize_ast(parse("SELECT 1"), version=2)
