"""
SQL Generation (V3): Measures and Metrics SQL Builders.
"""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.cube_matcher import (
    build_sql_from_cube_impl,
    find_matching_cube,
)
from datajunction_server.construction.build_v3.cte import (
    detect_window_metrics_requiring_grain_groups,
)
from datajunction_server.construction.build_v3.decomposition import (
    decompose_and_group_metrics,
)
from datajunction_server.construction.build_v3.loaders import (
    load_nodes,
    load_available_preaggs,
)
from datajunction_server.construction.build_v3.measures import (
    build_window_metric_grain_groups,
    process_metric_group,
)
from datajunction_server.construction.build_v3.metrics import (
    generate_metrics_sql,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    GeneratedMeasuresSQL,
    GeneratedSQL,
    GrainGroupSQL,
)
from datajunction_server.construction.build_v3.utils import (
    add_dimensions_from_filters,
    add_dimensions_from_metric_expressions,
)
from datajunction_server.database.partition import Partition
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.partition import PartitionType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse

logger = logging.getLogger(__name__)


async def extract_temporal_partition_columns(
    session: AsyncSession,
    metrics: list[str],
    dimensions: list[str],
) -> dict[str, Partition] | None:
    """
    Extract temporal partition columns from a matching cube.

    Finds a cube that matches the given metrics and dimensions, and extracts
    any columns marked as temporal partitions along with their partition metadata.

    Args:
        session: Database session
        metrics: List of metric node names
        dimensions: List of dimension names

    Returns:
        Dict mapping column names to partition objects, or None if no cube found or no temporal partitions
    """
    matching_cube = await find_matching_cube(
        session,
        metrics,
        dimensions,
        require_availability=False,
    )

    if not matching_cube:
        return None

    temporal_partition_columns = {}
    for column in matching_cube.columns:
        if column.partition and column.partition.type_ == PartitionType.TEMPORAL:
            temporal_partition_columns[column.name] = column.partition

    return temporal_partition_columns if temporal_partition_columns else None


def apply_orderby_limit(
    result: GeneratedSQL,
    orderby: list[str] | None,
    limit: int | None,
) -> GeneratedSQL:
    """
    Apply ORDER BY and LIMIT clauses to the generated SQL.

    Args:
        result: The GeneratedSQL object with the query AST
        orderby: List of ORDER BY expressions using semantic names
                 (e.g., ["v3.date.month DESC", "v3.total_revenue"])
        limit: Maximum number of rows to return

    Returns:
        Modified GeneratedSQL with ORDER BY and LIMIT applied
    """
    if not orderby and limit is None:
        return result

    select = result.query.select

    # Apply ORDER BY
    if orderby:
        # Build mapping from semantic_name -> output column name
        semantic_to_output: dict[str, str] = {
            col.semantic_name: col.name for col in result.columns
        }

        # Parse the orderby expressions
        orderby_str = ",".join(orderby)
        parsed = parse(f"SELECT 1 ORDER BY {orderby_str}")
        sort_items = (
            parsed.select.organization.order if parsed.select.organization else []
        )

        resolved_sort_items = []
        for sort_item in sort_items:
            # Get semantic name from the sort expression
            if isinstance(sort_item.expr, ast.Column):
                semantic_name = sort_item.expr.identifier()
            else:  # pragma: no cover
                semantic_name = str(sort_item.expr)

            if semantic_name in semantic_to_output:
                output_col_name = semantic_to_output[semantic_name]
                # Use simple column reference to output alias
                resolved_sort_items.append(
                    ast.SortItem(
                        expr=ast.Column(name=ast.Name(output_col_name)),
                        asc=sort_item.asc,
                        nulls=sort_item.nulls,
                    ),
                )
            else:
                logger.warning(
                    f"[BuildV3] ORDER BY '{semantic_name}' not found in columns, skipping",
                )

        if resolved_sort_items:
            select.organization = ast.Organization(order=resolved_sort_items)

    # Apply LIMIT
    if limit is not None:
        select.limit = ast.Number(limit)

    return result


async def setup_build_context(
    session: AsyncSession,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str] | None = None,
    dialect: Dialect = Dialect.SPARK,
    use_materialized: bool = True,
    include_temporal_filters: bool = False,
    lookback_window: str | None = None,
) -> BuildContext:
    """
    Create and initialize a BuildContext with all setup done.

    This is the single source of truth for loading nodes and decomposing metrics.
    After this returns, ctx has:
    - nodes loaded
    - metric_groups populated
    - decomposed_metrics populated
    - dimensions updated with any auto-added dims from metric expressions

    Args:
        session: Database session
        metrics: List of metric node names
        dimensions: List of dimension names
        filters: Optional list of filter expressions
        dialect: SQL dialect for output
        use_materialized: Whether to use materialized tables
        include_temporal_filters: Whether to include temporal partition filters from cube
        lookback_window: Lookback window for temporal filters

    Returns:
        Fully initialized BuildContext
    """
    logger.info("[BuildV3] setup_build_context: Starting...")

    # Extract temporal partition columns from matching cube if requested
    temporal_partition_columns = None
    if include_temporal_filters:
        logger.info(
            "[BuildV3] setup_build_context: Extracting temporal partition columns...",
        )
        temporal_partition_columns = await extract_temporal_partition_columns(
            session,
            metrics,
            dimensions,
        )
        logger.info(
            "[BuildV3] setup_build_context: Temporal partition columns extracted",
        )

    logger.info("[BuildV3] setup_build_context: Creating BuildContext...")
    ctx = BuildContext(
        session=session,
        metrics=metrics,
        dimensions=list(dimensions),
        filters=filters or [],
        dialect=dialect,
        use_materialized=use_materialized,
        temporal_partition_columns=temporal_partition_columns or {},
        lookback_window=lookback_window,
    )
    logger.info("[BuildV3] setup_build_context: BuildContext created")

    # Load all required nodes (single DB round trip)
    logger.info("[BuildV3] setup_build_context: Loading nodes...")
    await load_nodes(ctx)
    logger.info("[BuildV3] setup_build_context: Loaded %d nodes", len(ctx.nodes))

    # Validate we have at least one metric
    if not ctx.metrics:
        raise DJInvalidInputException("At least one metric is required")

    # Decompose metrics and group by parent node
    logger.info("[BuildV3] setup_build_context: Decomposing metrics...")
    ctx.metric_groups, ctx.decomposed_metrics = await decompose_and_group_metrics(ctx)
    logger.info(
        "[BuildV3] setup_build_context: Decomposed into %d metric group(s), %d decomposed metrics",
        len(ctx.metric_groups),
        len(ctx.decomposed_metrics),
    )

    # Add dimensions referenced in metric expressions (e.g., LAG ORDER BY)
    logger.info(
        "[BuildV3] setup_build_context: Adding dimensions from metric expressions...",
    )
    add_dimensions_from_metric_expressions(ctx, ctx.decomposed_metrics)
    logger.info(
        "[BuildV3] setup_build_context: Now have %d dimensions",
        len(ctx.dimensions),
    )

    # Add dimensions referenced in filters (for WHERE clause resolution)
    logger.info("[BuildV3] setup_build_context: Adding dimensions from filters...")
    add_dimensions_from_filters(ctx)
    logger.info(
        "[BuildV3] setup_build_context: Now have %d dimensions",
        len(ctx.dimensions),
    )

    # Load any missing dimension nodes (and their upstreams, including sources)
    # This is needed for dimensions discovered from metric expressions
    # load_nodes adds to ctx.nodes rather than replacing, so this is safe to call again
    logger.info("[BuildV3] setup_build_context: Loading any missing dimension nodes...")
    await load_nodes(ctx)
    logger.info("[BuildV3] setup_build_context: Final node count: %d", len(ctx.nodes))

    logger.info("[BuildV3] setup_build_context: Complete!")
    return ctx


async def build_measures_sql(
    session: AsyncSession,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str] | None = None,
    dialect: Dialect = Dialect.SPARK,
    use_materialized: bool = True,
    include_temporal_filters: bool = False,
    lookback_window: str | None = None,
) -> GeneratedMeasuresSQL:
    """
    Build measures SQL for a set of metrics, dimensions, and filters.

    Measures SQL represents the first stage of metric computation - it decomposes
    each metric into its atomic aggregation components (e.g., SUM(amount), COUNT(*)),
    groups these components by their parent fact and aggregability level, and then
    builds SQL that aggregates these components to the requested dimensional grain.

    Args:
        session: Database session
        metrics: List of metric node names
        dimensions: List of dimension names (format: "node.column" or "node.column[role]")
        filters: Optional list of filter expressions
        dialect: SQL dialect for output
        use_materialized: If True (default), use materialized tables when available.
            Set to False when generating SQL for materialization refresh to avoid
            circular references.
        include_temporal_filters: If True, finds a matching cube and applies temporal
            partition filters from that cube. Filters are pushed down to parent nodes
            that have dimension links to the temporal partition columns. Used for
            incremental materialization and partition pruning.
        lookback_window: Lookback window for temporal filters (e.g., "3 DAY").
            If not provided, filters to exactly the logical timestamp partition.

    Returns:
        GeneratedMeasuresSQL with one GrainGroupSQL per aggregation level,
        plus context and decomposed metrics for efficient reuse by build_metrics_sql
    """
    logger.info(
        "[BuildV3] Starting build_measures_sql for %d metrics, %d dimensions, %d filters",
        len(metrics),
        len(dimensions),
        len(filters) if filters else 0,
    )
    logger.debug(
        "[BuildV3] Metrics: %s, Dimensions: %s",
        metrics,
        dimensions,
    )

    # Setup context (loads nodes, decomposes metrics, adds dimensions from expressions)
    logger.info("[BuildV3] Setting up build context...")
    ctx = await setup_build_context(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        dialect=dialect,
        use_materialized=use_materialized,
        include_temporal_filters=include_temporal_filters,
        lookback_window=lookback_window,
    )
    logger.info(
        "[BuildV3] Build context ready: %d nodes loaded, %d metric groups, %d decomposed metrics",
        len(ctx.nodes),
        len(ctx.metric_groups),
        len(ctx.decomposed_metrics),
    )

    # Build grain groups from context
    logger.info("[BuildV3] Building grain groups...")
    result = await build_grain_groups(ctx, metrics)
    logger.info(
        "[BuildV3] Completed build_measures_sql with %d grain groups",
        len(result.grain_groups),
    )
    return result


async def build_grain_groups(
    ctx: BuildContext,
    metrics: list[str],
) -> GeneratedMeasuresSQL:
    """
    Build grain groups from a fully initialized BuildContext.

    This is the shared grain group building logic used by both
    build_measures_sql and build_metrics_sql (non-cube path).

    Args:
        ctx: Fully initialized BuildContext (from setup_build_context)
        metrics: Original metrics list (for sanity checks)

    Returns:
        GeneratedMeasuresSQL with grain groups
    """
    # Load available pre-aggregations (if use_materialized=True)
    logger.info("[BuildV3] Loading available pre-aggregations...")
    await load_available_preaggs(ctx)
    logger.info(
        "[BuildV3] Pre-aggregations loaded: %d available",
        len(ctx.available_preaggs),
    )

    # Process each metric group into grain group SQLs
    # Cross-fact metrics produce separate grain groups (one per parent node)
    all_grain_group_sqls: list[GrainGroupSQL] = []
    logger.info("[BuildV3] Processing %d metric groups...", len(ctx.metric_groups))
    for idx, metric_group in enumerate(ctx.metric_groups):
        logger.info(
            "[BuildV3] Processing metric group %d/%d (parent: %s, %d metrics)",
            idx + 1,
            len(ctx.metric_groups),
            metric_group.parent_node.name,
            len(metric_group.decomposed_metrics),
        )
        grain_group_sqls = process_metric_group(ctx, metric_group)
        logger.info(
            "[BuildV3] Metric group %d/%d produced %d grain group(s)",
            idx + 1,
            len(ctx.metric_groups),
            len(grain_group_sqls),
        )
        all_grain_group_sqls.extend(grain_group_sqls)

    # Sanity check: all requested metrics should already be decomposed
    for metric_name in metrics:
        if metric_name not in ctx.decomposed_metrics:  # pragma: no cover
            logger.warning(
                f"[BuildV3] Metric {metric_name} was not decomposed - this indicates a bug",
            )

    # Sanity check: all metrics in grain groups should already be decomposed
    all_grain_group_metrics = set()
    for gg in all_grain_group_sqls:
        all_grain_group_metrics.update(gg.metrics)

    for metric_name in all_grain_group_metrics:
        if metric_name not in ctx.decomposed_metrics:  # pragma: no cover
            logger.warning(
                f"[BuildV3] Grain group metric {metric_name} was not decomposed - "
                "this indicates a bug",
            )

    # Detect window metrics that require grain-level grain groups (LAG/LEAD)
    # These are period-over-period metrics that need aggregation at the ORDER BY grain
    window_metric_grains = detect_window_metrics_requiring_grain_groups(
        ctx,
        ctx.decomposed_metrics,
        all_grain_group_metrics,
    )

    # Build additional grain groups for window metrics at their ORDER BY grains
    # These are pre-aggregated at coarser grains (e.g., weekly) for LAG/LEAD to work
    # Each grain group goes through pre-agg matching, so if a pre-agg exists at that
    # grain (e.g., weekly), it will be used instead of re-scanning source tables
    if window_metric_grains:
        window_grain_groups = build_window_metric_grain_groups(
            ctx,
            window_metric_grains,
            all_grain_group_sqls,
            ctx.decomposed_metrics,
        )
        all_grain_group_sqls.extend(window_grain_groups)

    return GeneratedMeasuresSQL(
        grain_groups=all_grain_group_sqls,
        dialect=ctx.dialect,
        requested_dimensions=ctx.dimensions,
        ctx=ctx,
        decomposed_metrics=ctx.decomposed_metrics,
        window_metric_grains=window_metric_grains,
    )


async def build_metrics_sql(
    session: AsyncSession,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str] | None = None,
    orderby: list[str] | None = None,
    limit: int | None = None,
    dialect: Dialect | None = None,
    use_materialized: bool = True,
) -> GeneratedSQL:
    """
    Build metrics SQL for a set of metrics and dimensions.

    Metrics SQL applies final metric expressions on top of measures, including
    handling derived metrics. It produces a single executable query with the
    following layers:

    Layer 1: Measures
        (a) Checks if a materialized cube as the source of measures is available.
        If so, it uses the cube's availability table as the source of measures.
        (b) Otherwise, it generates measures SQL output as CTEs from either the
        pre-aggregated tables or the source tables, and joins the grain groups
        if metrics come from different facts/aggregabilities.
    Layer 2: Base Metrics
        Applies combiner expressions for multi-component metrics.
    Layer 3: Derived Metrics
        Computes derived metrics that reference other metrics.
    """
    # Default to SPARK dialect if not specified
    if dialect is None:
        dialect = Dialect.SPARK

    # Setup context (loads nodes, decomposes metrics, adds dimensions from expressions)
    ctx = await setup_build_context(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        dialect=dialect,
        use_materialized=use_materialized,
    )

    # Try cube match - if found, use cube path
    if use_materialized:
        cube = await find_matching_cube(
            session,
            metrics,
            dimensions,
            require_availability=True,
        )
        if cube:
            logger.info(f"[BuildV3] Layer 1: Using cube {cube.name}")
            result = build_sql_from_cube_impl(ctx, cube, ctx.decomposed_metrics)

            # For cubes, scan estimate would be the cube table itself (already materialized)
            # We could calculate it here if needed, but cubes are typically small compared to source tables
            # For now, we'll leave it None for cube queries
            # TODO: Add cube table size to scan estimate if cube availability has size metadata

            return apply_orderby_limit(result, orderby, limit)

    # No cube - build grain groups
    measures_result = await build_grain_groups(ctx, metrics)

    if not measures_result.grain_groups:  # pragma: no cover
        raise DJInvalidInputException("No grain groups produced from measures SQL")

    result = generate_metrics_sql(
        ctx,
        measures_result,
        ctx.decomposed_metrics,
    )
    return apply_orderby_limit(result, orderby, limit)
