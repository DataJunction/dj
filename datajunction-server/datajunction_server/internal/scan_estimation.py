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


def analyze_sql_filters_for_sources(
    sql_ast: ast.Query,
    scanned_sources: list[str],
) -> tuple[dict[str, list[str]], Optional[ast.Expression]]:
    """
    Analyze the final generated SQL to extract filtered columns for each source.

    This function performs JOIN-aware filter analysis:
    1. Extracts WHERE filters with table aliases
    2. Extracts JOIN conditions
    3. Propagates filters through equi-joins (if t2.col is filtered and t1.col = t2.col,
       then t1.col is also filtered)
    4. Maps filtered columns back to source CTEs

    Args:
        sql_ast: Parsed AST of the final generated SQL query
        scanned_sources: List of source node names that are scanned

    Returns:
        Tuple of:
        - Dict mapping source_name -> list of filtered column names
        - WHERE condition AST (for selectivity analysis)
        Example: ({"source.sales_fact": ["utc_date"]}, <WHERE AST>)
    """
    if not hasattr(sql_ast, "select") or not sql_ast.select:
        return {source: [] for source in scanned_sources}

    # Step 1: Extract WHERE filtered columns WITH table aliases
    # Map: table_alias -> set of filtered column names
    filtered_by_table: dict[str, set[str]] = {}

    if sql_ast.select.where:
        for col in sql_ast.select.where.find_all(ast.Column):
            if col.name:
                table_alias = None
                col_name = col.name.name if hasattr(col.name, "name") else str(col.name)

                # Extract table alias if present
                if hasattr(col.name, "namespace") and col.name.namespace:
                    table_alias = (
                        col.name.namespace.name
                        if hasattr(col.name.namespace, "name")
                        else str(col.name.namespace)
                    )

                if table_alias:
                    if table_alias not in filtered_by_table:
                        filtered_by_table[table_alias] = set()
                    filtered_by_table[table_alias].add(col_name)

    _logger.info(
        f"[Scan Estimation] WHERE filtered columns by table: {filtered_by_table}",
    )

    # Step 2: Extract JOIN conditions from FROM clause
    # Map: (left_table, left_col) -> set of (right_table, right_col)
    join_conditions: dict[tuple[str, str], set[tuple[str, str]]] = {}

    if sql_ast.select.from_:
        # Find all joins in the FROM clause
        for join in sql_ast.select.from_.find_all(ast.Join):
            if join.criteria and join.criteria.on:
                # Look for equi-join conditions (t1.col = t2.col)
                for binary_op in join.criteria.on.find_all(ast.BinaryOp):
                    if binary_op.op == ast.BinaryOpKind.Eq:
                        left_expr = binary_op.left
                        right_expr = binary_op.right

                        # Extract (table, column) from both sides
                        if isinstance(left_expr, ast.Column) and isinstance(
                            right_expr,
                            ast.Column,
                        ):
                            left_table = None
                            left_col = None
                            right_table = None
                            right_col = None

                            if left_expr.name:
                                left_col = (
                                    left_expr.name.name
                                    if hasattr(left_expr.name, "name")
                                    else str(left_expr.name)
                                )
                                if (
                                    hasattr(left_expr.name, "namespace")
                                    and left_expr.name.namespace
                                ):
                                    left_table = (
                                        left_expr.name.namespace.name
                                        if hasattr(left_expr.name.namespace, "name")
                                        else str(left_expr.name.namespace)
                                    )

                            if right_expr.name:
                                right_col = (
                                    right_expr.name.name
                                    if hasattr(right_expr.name, "name")
                                    else str(right_expr.name)
                                )
                                if (
                                    hasattr(right_expr.name, "namespace")
                                    and right_expr.name.namespace
                                ):
                                    right_table = (
                                        right_expr.name.namespace.name
                                        if hasattr(right_expr.name.namespace, "name")
                                        else str(right_expr.name.namespace)
                                    )

                            # Record bidirectional join condition
                            if left_table and left_col and right_table and right_col:
                                key1 = (left_table, left_col)
                                key2 = (right_table, right_col)

                                if key1 not in join_conditions:
                                    join_conditions[key1] = set()
                                if key2 not in join_conditions:
                                    join_conditions[key2] = set()

                                join_conditions[key1].add(key2)
                                join_conditions[key2].add(key1)

    _logger.info(f"[Scan Estimation] JOIN conditions: {join_conditions}")

    # Step 3: Propagate filters through JOINs
    # If t2.dateint is filtered and t1.utc_date = t2.dateint, then t1.utc_date is also filtered
    propagated_filters: dict[str, set[str]] = {
        table: cols.copy() for table, cols in filtered_by_table.items()
    }

    changed = True
    iterations = 0
    max_iterations = 10  # Prevent infinite loops

    while changed and iterations < max_iterations:
        changed = False
        iterations += 1

        for table, cols in list(propagated_filters.items()):
            for col in list(cols):
                key = (table, col)
                if key in join_conditions:
                    # Propagate to all joined columns
                    for joined_table, joined_col in join_conditions[key]:
                        if joined_table not in propagated_filters:
                            propagated_filters[joined_table] = set()

                        if joined_col not in propagated_filters[joined_table]:
                            propagated_filters[joined_table].add(joined_col)
                            changed = True

    _logger.info(
        f"[Scan Estimation] Propagated filters after {iterations} iterations: {propagated_filters}",
    )

    # Step 4: Build CTE -> source table mapping
    # Parse FROM clause inside each CTE to find which source table it queries
    cte_to_source_table: dict[str, str] = {}

    if hasattr(sql_ast, "ctes") and sql_ast.ctes:
        _logger.info(f"[Scan Estimation] Processing {len(sql_ast.ctes)} CTEs")
        for cte in sql_ast.ctes:
            _logger.info(
                f"[Scan Estimation] CTE type: {type(cte)}, has alias: {hasattr(cte, 'alias')}, has query: {hasattr(cte, 'query')}",
            )

            if hasattr(cte, "alias") and cte.alias:
                cte_name = (
                    cte.alias.name if hasattr(cte.alias, "name") else str(cte.alias)
                )
                _logger.info(f"[Scan Estimation] Processing CTE: {cte_name}")

                # Look at the CTE's query - it might be a Query object, not Select
                cte_query = cte.query if hasattr(cte, "query") else cte
                _logger.info(f"[Scan Estimation] CTE query type: {type(cte_query)}")

                # Get the select from the query
                cte_select = None
                if hasattr(cte_query, "select"):
                    cte_select = cte_query.select
                elif isinstance(cte_query, ast.Select):
                    cte_select = cte_query

                if cte_select:
                    _logger.info(
                        f"[Scan Estimation] CTE {cte_name} has select, from_={cte_select.from_}",
                    )

                    if cte_select.from_:
                        # Try direct table reference
                        if isinstance(cte_select.from_, ast.Table):
                            table = cte_select.from_
                            _logger.info(
                                f"[Scan Estimation] Direct table in FROM: {table}",
                            )
                            if hasattr(table, "name"):
                                table_id = (
                                    table.name.identifier()
                                    if hasattr(table.name, "identifier")
                                    else str(table.name)
                                )
                                cte_to_source_table[cte_name] = table_id
                                _logger.info(
                                    f"[Scan Estimation] Mapped {cte_name} -> {table_id}",
                                )
                        else:
                            # Search for tables in FROM clause
                            for table in cte_select.from_.find_all(ast.Table):
                                _logger.info(
                                    f"[Scan Estimation] Found table in CTE {cte_name}: {table}",
                                )
                                if hasattr(table, "name"):
                                    table_id = (
                                        table.name.identifier()
                                        if hasattr(table.name, "identifier")
                                        else str(table.name)
                                    )
                                    cte_to_source_table[cte_name] = table_id
                                    _logger.info(
                                        f"[Scan Estimation] Mapped {cte_name} -> {table_id}",
                                    )
                                    break

    _logger.info(
        f"[Scan Estimation] CTE to source table mapping: {cte_to_source_table}",
    )

    # Step 5: Build table alias -> CTE name mapping from main query
    alias_to_cte: dict[str, str] = {}

    if sql_ast.select.from_:
        for table in sql_ast.select.from_.find_all(ast.Table):
            if hasattr(table, "alias") and table.alias and hasattr(table, "name"):
                alias = (
                    table.alias.name
                    if hasattr(table.alias, "name")
                    else str(table.alias)
                )
                cte_name = (
                    table.name.name if hasattr(table.name, "name") else str(table.name)
                )
                alias_to_cte[alias] = cte_name

    _logger.info(f"[Scan Estimation] Table alias to CTE: {alias_to_cte}")

    # Step 6: Map filtered columns to sources
    result: dict[str, set[str]] = {}

    for table_alias, filtered_cols in propagated_filters.items():
        # Map alias -> CTE name
        cte_name = alias_to_cte.get(table_alias, table_alias)

        # Map CTE name -> source table name
        source_table = cte_to_source_table.get(cte_name)

        if source_table:
            # Match source table against scanned_sources
            # Source table: "prodhive.dse.thumb_rating_f"
            # Scanned source: "source.prodhive.dse.thumb_rating_f"
            for source_name in scanned_sources:
                # Check if source_name ends with the source_table
                if source_name.endswith(source_table):
                    if source_name not in result:
                        result[source_name] = set()
                    result[source_name].update(filtered_cols)
                    _logger.info(
                        f"[Scan Estimation] Matched {table_alias} -> {cte_name} -> {source_table} -> {source_name}",
                    )
                    break

    _logger.info(f"[Scan Estimation] Final filtered columns by source: {result}")

    # Convert sets to lists and return with WHERE condition for selectivity analysis
    where_condition = (
        sql_ast.select.where if hasattr(sql_ast, "select") and sql_ast.select else None
    )
    return {source: list(cols) for source, cols in result.items()}, where_condition


def parse_filter_selectivity(
    where_condition: ast.Expression,
    column_name: str,
) -> Optional[int]:
    """
    Parse a WHERE condition to estimate how many partition values it matches.

    Args:
        where_condition: The WHERE clause AST
        column_name: The partition column name to analyze

    Returns:
        Estimated number of partition values matched, or None if can't determine

    Examples:
        WHERE date = '2024-01-01' → 1 partition
        WHERE date >= '2024-01-01' AND date <= '2024-01-31' → 31 partitions
        WHERE date IN ('2024-01-01', '2024-01-02') → 2 partitions
    """
    # Find all binary operations involving this column
    matching_ops = []

    for binary_op in where_condition.find_all(ast.BinaryOp):
        # Check if this operation involves our column
        left_col = None
        right_col = None

        if isinstance(binary_op.left, ast.Column):
            left_col_name = (
                binary_op.left.name.name
                if hasattr(binary_op.left.name, "name")
                else str(binary_op.left.name)
            )
            if left_col_name == column_name:
                left_col = True

        if isinstance(binary_op.right, ast.Column):
            right_col_name = (
                binary_op.right.name.name
                if hasattr(binary_op.right.name, "name")
                else str(binary_op.right.name)
            )
            if right_col_name == column_name:
                right_col = True

        if left_col or right_col:
            matching_ops.append(binary_op)

    if not matching_ops:
        return None

    # Analyze the operations
    has_equality = False
    range_bounds = {"min": None, "max": None}

    for op in matching_ops:
        if op.op == ast.BinaryOpKind.Eq:
            # Equality: single partition
            return 1

        elif op.op == ast.BinaryOpKind.Gte or op.op == ast.BinaryOpKind.Gt:
            # Greater than - sets lower bound
            # date >= '2024-01-01'
            has_equality = True

        elif op.op == ast.BinaryOpKind.Lte or op.op == ast.BinaryOpKind.Lt:
            # Less than - sets upper bound
            # date <= '2024-01-31'
            has_equality = True

    # If we have range bounds (>= and <=), we could try to parse the values
    # For now, use a heuristic: range filter = 10% of partitions
    if has_equality:
        return None  # Signal to use default heuristic

    return None


def estimate_scan_reduction(
    filtered_columns: list[str],
    partition_columns: list[str],
    total_partitions: int | None,
    source_name: str,
    where_condition: Optional[ast.Expression] = None,
) -> tuple[float, int | None]:
    """
    Estimate scan reduction when partition columns are filtered.

    Args:
        filtered_columns: Columns that have filters applied
        partition_columns: Partition columns on the source
        total_partitions: Total number of partitions (if known)
        source_name: Source name for logging
        where_condition: The WHERE clause AST for parsing filter selectivity

    Returns:
        Tuple of (scan_percentage, estimated_scanned_partitions):
        - scan_percentage: Estimated percentage of table scanned (0.0-1.0)
        - estimated_scanned_partitions: Estimated partition count (or None if can't estimate)
    """
    if not partition_columns:
        # No partitions defined - full table scan
        _logger.info(
            f"[Scan Estimation] {source_name}: No partition columns, 100% scan",
        )
        return 1.0, None

    # Check if any partition columns are filtered
    partition_cols_filtered = [
        col for col in filtered_columns if col in partition_columns
    ]

    if not partition_cols_filtered:
        # No partition columns filtered - full table scan
        _logger.info(
            f"[Scan Estimation] {source_name}: Partition columns {partition_columns} not filtered, 100% scan",
        )
        return 1.0, total_partitions

    # Try to parse filter selectivity for better estimates
    estimated_partitions = None
    if where_condition and total_partitions:
        # The WHERE clause may reference columns by different names than the source columns
        # (e.g., t2.dateint in WHERE, but utc_date is the source column)
        # Try to find ANY column in the WHERE clause that we can analyze

        # Extract all column names from the WHERE clause
        where_columns = set()
        for col in where_condition.find_all(ast.Column):
            if col.name:
                col_name = col.name.name if hasattr(col.name, "name") else str(col.name)
                where_columns.add(col_name)

        _logger.info(
            f"[Scan Estimation] {source_name}: WHERE columns: {where_columns}, "
            f"partition columns filtered: {partition_cols_filtered}",
        )

        # Try to parse selectivity for any column in the WHERE clause
        for where_col in where_columns:
            partition_count = parse_filter_selectivity(where_condition, where_col)

            if partition_count is not None:
                # We got a specific count (e.g., equality filter = 1 partition)
                estimated_partitions = partition_count
                scan_percentage = min(1.0, estimated_partitions / total_partitions)

                _logger.info(
                    f"[Scan Estimation] {source_name}: Filter selectivity analysis on '{where_col}' - "
                    f"estimated {estimated_partitions} partition(s) out of {total_partitions}, "
                    f"{scan_percentage * 100:.1f}% scan",
                )
                return scan_percentage, estimated_partitions

    # Fallback: Conservative 10% estimate
    scan_percentage = 0.1
    estimated_partitions = (
        int(total_partitions * scan_percentage) if total_partitions else None
    )

    _logger.info(
        f"[Scan Estimation] {source_name}: Partition column(s) {partition_cols_filtered} filtered, "
        f"using default estimate {scan_percentage * 100:.0f}% scan ({estimated_partitions}/{total_partitions} partitions)",
    )

    return scan_percentage, estimated_partitions
