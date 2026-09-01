from dataclasses import dataclass

import pytest

from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.structural import serialize_ast


def test_serialize_ast_is_structural_and_numeric():
    integer = serialize_ast(parse("SELECT 1"))
    integral_float = serialize_ast(parse("SELECT 1.0"))
    typed = serialize_ast(
        parse("SELECT CAST(value AS DECIMAL(10, 2)) FROM source"),
    )

    assert integer == integral_float
    assert integer["type"] == "Query"
    assert "datajunction_server." not in str(integer)
    assert typed["type"] == "Query"


def test_serialize_ast_rejects_unclassified_nodes_and_versions():
    @dataclass(eq=False)
    class FutureNode(ast.Node):
        def __str__(self) -> str:
            return "future"

    with pytest.raises(TypeError, match="Unsupported structural SQL node: FutureNode"):
        serialize_ast(FutureNode())
    with pytest.raises(ValueError, match="Unsupported structural SQL serialization"):
        serialize_ast(parse("SELECT 1"), version=2)
