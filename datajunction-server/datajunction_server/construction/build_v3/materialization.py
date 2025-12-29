"""
Materialization utilities for checking and using materialized tables.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from datajunction_server.database.node import Node
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import SEPARATOR

if TYPE_CHECKING:
    from datajunction_server.construction.build_v3.types import BuildContext

logger = logging.getLogger(__name__)


def amenable_name(name: str) -> str:
    """
    Convert a name to a SQL-safe identifier by replacing separators.
    """
    return name.replace(SEPARATOR, "_").replace("-", "_")


def get_physical_table_name(node: Node) -> Optional[str]:
    """
    Get the physical table name for a source node.

    For source nodes: Returns catalog.schema.table
    For other nodes: Returns None (they need CTEs)
    """
    rev = node.current
    if not rev:  # pragma: no cover
        return None

    if node.type == NodeType.SOURCE:
        parts = []
        if rev.catalog:  # pragma: no branch
            parts.append(rev.catalog.name)
        if rev.schema_:  # pragma: no branch
            parts.append(rev.schema_)
        if rev.table:  # pragma: no branch
            parts.append(rev.table)
        else:
            parts.append(node.name)  # pragma: no cover
        return SEPARATOR.join(parts)

    return None  # pragma: no cover


def has_available_materialization(node: Node) -> bool:
    """
    Check if a node has an available materialized table.

    A materialized table is available if:
    1. The node has an availability state attached
    2. The availability state indicates the data is available

    Args:
        node: The node to check

    Returns:
        True if a materialized table is available, False otherwise
    """
    if not node.current:  # pragma: no cover
        return False

    availability = node.current.availability
    if not availability:
        return False

    # The is_available method on AvailabilityState checks criteria
    # For now, we just check that availability exists
    # TODO: Add criteria checking when BuildCriteria is integrated
    return availability.is_available()


def get_materialized_table_parts(node: Node) -> list[str] | None:
    """
    Get the table reference parts for a materialized table.

    Returns [catalog, schema, table] if the node has materialization,
    or None if it doesn't.

    Args:
        node: The node to get materialization for

    Returns:
        List of table name parts, or None if no materialization
    """
    if not node.current or not node.current.availability:
        return None

    availability = node.current.availability
    parts = []

    # Note: availability.catalog is always present
    # availability.schema_ and table are required
    if availability.catalog:  # pragma: no branch
        parts.append(availability.catalog)
    if availability.schema_:  # pragma: no branch
        parts.append(availability.schema_)
    if availability.table:  # pragma: no branch
        parts.append(availability.table)

    return parts if parts else None  # pragma: no cover


def should_use_materialized_table(
    ctx: "BuildContext",
    node: Node,
) -> bool:
    """
    Determine if we should use a materialized table for a node.

    Considerations:
    1. ctx.use_materialized must be True (default)
    2. The node must have an available materialization
    3. Source nodes always use their physical table (not considered "materialization")

    Args:
        ctx: Build context
        node: Node to check

    Returns:
        True if materialization should be used, False otherwise
    """
    # Source nodes always use physical tables (not materialization)
    if node.type == NodeType.SOURCE:  # pragma: no cover
        return False

    # Check if materialization is enabled in context
    if not ctx.use_materialized:  # pragma: no cover
        return False

    # Check if node has available materialization
    return has_available_materialization(node)


def get_table_reference_parts_with_materialization(
    ctx: "BuildContext",
    node: Node,
) -> tuple[list[str], bool]:
    """
    Get table reference parts, considering materialization.

    For source nodes: Always returns physical table [catalog, schema, table]
    For other nodes:
      - If materialized and use_materialized=True: Returns [catalog, schema, table]
      - Otherwise: Returns [cte_name] for CTE reference

    Args:
        ctx: Build context
        node: Node to get reference for

    Returns:
        Tuple of (table_parts, is_materialized)
        - table_parts: List of name parts
        - is_materialized: True if using materialized table (not CTE)
    """
    rev = node.current
    if not rev:  # pragma: no cover
        raise DJInvalidInputException(f"Node {node.name} has no current revision")

    # For source nodes, always use physical table
    if node.type == NodeType.SOURCE:
        parts = []
        if rev.catalog:  # pragma: no branch
            parts.append(rev.catalog.name)
        if rev.schema_:  # pragma: no branch
            parts.append(rev.schema_)
        if rev.table:  # pragma: no branch
            parts.append(rev.table)
        else:
            parts.append(node.name)  # pragma: no cover
        return (parts, True)  # Sources are considered "materialized" (physical)

    # For non-source nodes, check for materialization
    if should_use_materialized_table(ctx, node):
        mat_parts = get_materialized_table_parts(node)
        if mat_parts:  # pragma: no branch
            logger.debug(
                f"[BuildV3] Using materialized table for {node.name}: "
                f"{'.'.join(mat_parts)}",
            )
            return (mat_parts, True)

    # Fall back to CTE reference
    return ([amenable_name(node.name)], False)
