"""Versioned structural serialization for parsed SQL."""

import math
from collections.abc import Callable
from decimal import Decimal
from enum import Enum
from typing import Any

from datajunction_server.sql.parsing.ast import Node
from datajunction_server.sql.parsing.types import ColumnType

_AST_NODE_TAGS_V1 = frozenset(
    {
        "Alias",
        "ArithmeticUnaryOp",
        "Between",
        "BinaryOp",
        "Boolean",
        "Case",
        "Cast",
        "Column",
        "DefaultName",
        "Frame",
        "FrameBound",
        "From",
        "Function",
        "FunctionTable",
        "FunctionTableExpression",
        "Hint",
        "In",
        "InlineTable",
        "Interval",
        "IntervalUnit",
        "IsBoolean",
        "IsDistinctFrom",
        "IsNull",
        "Join",
        "JoinCriteria",
        "Lambda",
        "LateralView",
        "Like",
        "Name",
        "Null",
        "Number",
        "Organization",
        "Over",
        "Query",
        "QueryParameter",
        "Relation",
        "Rlike",
        "Select",
        "SelectExpression",
        "SetOp",
        "SortItem",
        "String",
        "Struct",
        "Subscript",
        "Table",
        "UnaryOp",
        "UnNamed",
        "Wildcard",
    },
)


def _serialize_number_v1(value: float | Decimal) -> int | float | dict[str, str]:
    if isinstance(value, bool):
        raise TypeError("Boolean values are not SQL numbers")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Structural SQL numbers must be finite")
        if value.is_integer():
            return int(value)
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Structural SQL numbers must be finite")
        if value == value.to_integral_value():
            return int(value)
        return {"decimal": format(value.normalize(), "f")}
    return value


def _serialize_ast_v1(query_ast: Node) -> Any:
    """Serialize an AST without SQL rendering or Python module-qualified names."""

    def serialize(value: Any) -> Any:
        if isinstance(value, Node):
            tag = type(value).__name__
            if tag not in _AST_NODE_TAGS_V1:
                raise TypeError(f"Unsupported structural SQL node: {tag}")
            return {
                "type": tag,
                "fields": {
                    name: serialize(field_value)
                    for name, field_value in value.fields(
                        flat=False,
                        nodes_only=False,
                        obfuscated=False,
                        nones=True,
                        named=True,
                    )
                },
            }
        if isinstance(value, ColumnType):
            return {"type": "column_type", "value": str(value)}
        if isinstance(value, Enum):
            return serialize(value.value)
        if isinstance(value, Decimal):
            return _serialize_number_v1(value)
        if isinstance(value, float):
            return _serialize_number_v1(value)
        if isinstance(value, (list, tuple)):
            return [serialize(item) for item in value]
        if value is None or isinstance(value, (str, int, bool)):
            return value
        raise TypeError(f"Unsupported structural SQL value: {type(value).__name__}")

    return serialize(query_ast)


_LATEST_VERSION = 1
_SERIALIZERS: dict[int, Callable[[Node], Any]] = {
    1: _serialize_ast_v1,
}


def serialize_ast(query_ast: Node, *, version: int | None = None) -> Any:
    """Serialize an AST using a stable structural format."""
    selected_version = _LATEST_VERSION if version is None else version
    serializer = _SERIALIZERS.get(selected_version)
    if serializer is None:
        raise ValueError(
            f"Unsupported structural SQL serialization version: {selected_version}",
        )
    return serializer(query_ast)
