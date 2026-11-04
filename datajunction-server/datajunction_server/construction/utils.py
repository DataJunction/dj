"""
Utilities used around construction
"""

import decimal
import time
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJError, DJErrorException, ErrorCode
from datajunction_server.models.dimensionlink import DimensionLinkDefault
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
    "real",
    "smallint",
    "tinyint",
}
BOOLEAN_COLUMN_TYPES = {"bool", "boolean"}
STRING_COLUMN_TYPES = {"char", "string", "text", "varchar"}


def _string_literal(value: DimensionLinkDefault) -> "Value":
    """Quoted SQL string literal, with embedded quotes escaped."""
    from datajunction_server.sql.parsing import ast

    escaped = str(value).replace("'", "''")
    return ast.String(f"'{escaped}'")


def _number_literal(value: DimensionLinkDefault) -> "Value | None":
    """Numeric SQL literal, or None if the value isn't a number."""
    from datajunction_server.sql.parsing import ast

    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, decimal.Decimal)):
        return ast.Number(value)
    try:
        parsed = decimal.Decimal(value)
    except decimal.InvalidOperation:
        return None
    return ast.Number(parsed) if parsed.is_finite() else None


def _boolean_literal(value: DimensionLinkDefault) -> "Value | None":
    """Boolean SQL literal, or None if the value isn't a boolean."""
    from datajunction_server.sql.parsing import ast

    if isinstance(value, bool):
        return ast.Boolean(value)
    if str(value).strip().lower() in {"true", "false"}:
        return ast.Boolean(str(value).strip().lower() == "true")
    return None


def dimension_link_default_literal(
    default_value: DimensionLinkDefault,
    column_type: str | None = None,
) -> "Value":
    """
    Build the SQL literal for a dimension link's default value.

    The target column's type decides how the value renders, so a numeric
    column gets a bare number even when the default was authored as the
    string ``"0"``. Where the column type says nothing, the value's own
    type decides. A value that can't be rendered in the column's type
    falls back to a quoted string.
    """
    base_type = (column_type or "").lower().split("(", maxsplit=1)[0].strip()
    literal = None
    if base_type in BOOLEAN_COLUMN_TYPES:
        literal = _boolean_literal(default_value)
    elif base_type in NUMERIC_COLUMN_TYPES:
        literal = _number_literal(default_value)
    elif base_type not in STRING_COLUMN_TYPES:
        literal = (
            _boolean_literal(default_value)
            if isinstance(default_value, bool)
            else _number_literal(default_value)
            if isinstance(default_value, (int, float, decimal.Decimal))
            else None
        )
    return literal if literal is not None else _string_literal(default_value)


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
