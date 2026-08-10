"""
Utilities used around construction
"""

import decimal
import time
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJError, DJErrorException, ErrorCode
from datajunction_server.models.node_type import NodeType

if TYPE_CHECKING:
    from datajunction_server.sql.parsing.ast import Name, Value


NUMERIC_COLUMN_TYPES = {
    "bigint",
    "decimal",
    "double",
    "float",
    "int",
    "integer",
    "long",
    "number",
    "numeric",
    "smallint",
    "tinyint",
}
BOOLEAN_COLUMN_TYPES = {"bool", "boolean"}
STRING_COLUMN_TYPES = {"char", "string", "text", "varchar"}


def _base_column_type(column_type: str | None) -> str | None:
    """Return a simple comparable type name from a DJ column type string."""
    if not column_type:
        return None
    return column_type.lower().split("(", maxsplit=1)[0].strip()


def _string_literal(value: Any) -> "Value":
    """Build a quoted SQL string literal."""
    from datajunction_server.sql.parsing import ast

    escaped = str(value).replace("'", "''")
    return ast.String(f"'{escaped}'")


def _number_literal(value: Any) -> "Value":
    """Build a numeric SQL literal."""
    from datajunction_server.sql.parsing import ast

    if isinstance(value, bool):
        raise ValueError("Boolean values cannot be used as numeric defaults")
    return ast.Number(value)


def _boolean_literal(value: Any) -> "Value":
    """Build a boolean SQL literal."""
    from datajunction_server.sql.parsing import ast

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "false"}:
            return ast.Boolean(normalized == "true")
        raise ValueError(f"Invalid boolean default value: {value}")
    return ast.Boolean(bool(value))


def dimension_link_default_literal(
    default_value: Any,
    column_type: str | None = None,
) -> "Value":
    """
    Build an AST literal for a dimension link default value.

    When the target dimension column type is known, it wins over the Python
    value type so legacy string-authored numeric defaults like ``"0"`` render
    as numeric SQL for numeric columns.
    """
    from datajunction_server.sql.parsing import ast

    base_type = _base_column_type(column_type)
    if default_value is None:
        return ast.Null()

    if base_type in STRING_COLUMN_TYPES:
        return _string_literal(default_value)
    if base_type in NUMERIC_COLUMN_TYPES:
        return _number_literal(default_value)
    if base_type in BOOLEAN_COLUMN_TYPES:
        return _boolean_literal(default_value)

    if isinstance(default_value, bool):
        return ast.Boolean(default_value)
    if isinstance(default_value, (int, float, decimal.Decimal)):
        return ast.Number(default_value)
    return _string_literal(default_value)


async def get_dj_node(
    session: AsyncSession,
    node_name: str,
    kinds: set[NodeType] | None = None,
    current: bool = True,
) -> NodeRevision:
    """Return the DJ Node with a given name from a set of node types"""
    start = time.monotonic()
    query = select(Node).filter(Node.name == node_name)
    if kinds:
        query = query.filter(Node.type.in_(kinds))  # type: ignore
    match = None
    try:
        match = (
            (
                await session.execute(
                    query.options(
                        joinedload(Node.current).options(
                            # Load the back-reference to Node so .node is available
                            # without triggering lazy loading (avoids MissingGreenlet errors)
                            joinedload(NodeRevision.node),
                            *NodeRevision.default_load_options(),
                        ),
                    ),
                )
            )
            .unique()
            .scalar_one()
        )
    except NoResultFound as no_result_exc:
        kind_msg = " or ".join(str(k) for k in kinds) if kinds else ""
        raise DJErrorException(
            DJError(
                code=ErrorCode.UNKNOWN_NODE,
                message=f"No node `{node_name}` exists of kind {kind_msg}.",
            ),
        ) from no_result_exc
    finally:
        from datajunction_server.instrumentation.provider import (
            get_metrics_provider,
        )

        get_metrics_provider().timer(
            "dj.compile.get_dj_node_ms",
            (time.monotonic() - start) * 1000,
        )
        get_metrics_provider().counter("dj.compile.get_dj_node_count")
    return match.current if match and current else match


def to_namespaced_name(name: str) -> "Name":
    """
    Builds a namespaced name from a string
    """
    from datajunction_server.sql.parsing.ast import Name

    chunked = name.split(".")
    chunked.reverse()
    current_name = None
    full_name = None
    for chunk in chunked:
        if not current_name:
            current_name = Name(chunk)
            full_name = current_name
        else:
            current_name.namespace = Name(chunk)
            current_name = current_name.namespace
    return full_name  # type: ignore
