"""
Scan estimation logic for query cost analysis.

This module provides functionality to estimate the amount of data that will be scanned
when executing queries, enabling cost warnings and optimization recommendations.
"""

import logging
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.config import Settings
from datajunction_server.database.node import Node
from datajunction_server.database.source_metadata import (
    SourceTableMetadata,
)
from datajunction_server.sql.parsing.backends.antlr4 import ast

_logger = logging.getLogger(__name__)


async def get_source_table_metadata(
    session: AsyncSession,
    node_id: int,
) -> Optional[SourceTableMetadata]:
    """
    Retrieve source table metadata for a given node.

    Args:
        session: Database session
        node_id: ID of the source node

    Returns:
        SourceTableMetadata if available, None otherwise
    """
    result = await session.execute(
        select(SourceTableMetadata).where(SourceTableMetadata.node_id == node_id),
    )
    return result.scalar_one_or_none()


async def get_source_nodes_with_metadata(
    session: AsyncSession,
    source_names: List[str],
) -> dict[str, tuple[Node, Optional[SourceTableMetadata]]]:
    """
    Batch load source nodes and their metadata.

    Avoids N+1 queries by loading all sources and their metadata in a single query.

    Args:
        session: Database session
        source_names: List of source node names to load

    Returns:
        Dict mapping source name to (Node, SourceTableMetadata) tuple
    """
    if not source_names:
        return {}

    _logger.info(
        f"[Scan Estimation] Batch loading {len(source_names)} source nodes: {source_names}",
    )

    # Single query with left join to get nodes + metadata
    from sqlalchemy.orm import joinedload

    result = await session.execute(
        select(Node, SourceTableMetadata)
        .outerjoin(SourceTableMetadata, SourceTableMetadata.node_id == Node.id)
        .where(Node.name.in_(source_names))
        .options(joinedload(Node.current)),
    )

    sources_dict = {node.name: (node, metadata) for node, metadata in result.all()}

    _logger.info(
        f"[Scan Estimation] Batch load complete: found {len(sources_dict)} nodes, "
        f"{sum(1 for _, (_, meta) in sources_dict.items() if meta is not None)} with metadata",
    )

    return sources_dict


def determine_severity(scan_bytes: int, settings: Settings) -> str:
    """
    Determine warning severity based on scan size.

    Args:
        scan_bytes: Estimated scan size in bytes
        settings: Application settings with threshold configuration

    Returns:
        Severity level: "info", "warning", or "critical"
    """
    if scan_bytes >= settings.scan_critical_threshold:
        return "critical"
    elif scan_bytes >= settings.scan_warning_threshold:
        return "warning"
    elif scan_bytes >= settings.scan_info_threshold:
        return "info"
    else:
        return "info"


def extract_source_tables(query_ast: ast.Query) -> List[ast.Table]:
    """
    Extract all source table references from a query AST.

    Args:
        query_ast: Parsed query AST

    Returns:
        List of table references
    """
    tables = []
    for table in query_ast.find_all(ast.Table):
        tables.append(table)
    return tables


def extract_where_conditions(
    query_ast: ast.Query,
    source_ref: ast.Table,
) -> List[ast.Expression]:
    """
    Extract WHERE clause conditions for a specific table reference.

    Args:
        query_ast: Parsed query AST
        source_ref: Table reference to find conditions for

    Returns:
        List of filter conditions
    """
    # This is a simplified implementation
    # In practice, would need to traverse the AST to find WHERE clauses
    # that apply to the specific table reference
    if query_ast.select.where:
        return [query_ast.select.where]
    return []


def extract_filtered_columns(where_conditions: List[ast.Expression]) -> List[str]:
    """
    Extract column names that are filtered in WHERE conditions.

    Args:
        where_conditions: List of WHERE clause expressions

    Returns:
        List of filtered column names
    """
    filtered_columns = []
    for condition in where_conditions:
        # Extract column references from the condition
        for column in condition.find_all(ast.Column):
            filtered_columns.append(column.name.name)
    return filtered_columns


def get_partition_columns(node: Node) -> list[str]:
    """
    Get partition column names for a source node.

    Args:
        node: Source node

    Returns:
        List of partition column names (e.g., ["utc_date", "region"])
    """
    if not node.current or not node.current.columns:
        return []

    partition_cols = []

    # Get temporal partition columns
    temporal_cols = node.current.temporal_partition_columns()
    partition_cols.extend([col.name for col in temporal_cols])

    # Get categorical partition columns
    categorical_cols = node.current.categorical_partition_columns()
    partition_cols.extend([col.name for col in categorical_cols])

    return partition_cols


def estimate_scan_reduction(
    filtered_columns: list[str],
    partition_columns: list[str],
    total_partitions: int | None,
    source_name: str,
) -> tuple[float, int | None]:
    """
    Estimate scan reduction when partition columns are filtered.

    This is a simplified heuristic that provides a conservative estimate.

    Args:
        filtered_columns: Columns that have filters applied
        partition_columns: Partition columns on the source
        total_partitions: Total number of partitions (if known)
        source_name: Source name for logging

    Returns:
        Tuple of (scan_percentage, estimated_scanned_partitions):
        - scan_percentage: Estimated percentage of table scanned (0.0-1.0)
        - estimated_scanned_partitions: Estimated partition count (or None if can't estimate)

    Heuristic:
    - If NO partition columns are filtered: 100% scan
    - If ANY partition column is filtered: Assume 10% scan (conservative)
    - TODO: Parse actual filter ranges for accurate estimation
    """
    if not partition_columns:
        # No partitions defined - full table scan
        _logger.info(f"[Scan Estimation] {source_name}: No partition columns, 100% scan")
        return 1.0, None

    # Check if any partition columns are filtered
    partition_cols_filtered = [
        col for col in filtered_columns if col in partition_columns
    ]

    if not partition_cols_filtered:
        # No partition columns filtered - full table scan
        _logger.info(
            f"[Scan Estimation] {source_name}: Partition columns {partition_columns} not filtered, 100% scan"
        )
        return 1.0, total_partitions

    # Conservative estimate: If partition column is filtered, assume 10% scan
    # This is a placeholder until we parse actual filter ranges
    scan_percentage = 0.1
    estimated_partitions = int(total_partitions * scan_percentage) if total_partitions else None

    _logger.info(
        f"[Scan Estimation] {source_name}: Partition column(s) {partition_cols_filtered} filtered, "
        f"estimating {scan_percentage*100:.0f}% scan ({estimated_partitions}/{total_partitions} partitions)"
    )

    return scan_percentage, estimated_partitions
