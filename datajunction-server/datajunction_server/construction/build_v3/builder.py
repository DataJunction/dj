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
from datajunction_server.construction.build_v3.decomposition import (
    decompose_and_group_metrics,
)
from datajunction_server.construction.build_v3.loaders import (
    load_nodes,
    load_available_preaggs,
)
from datajunction_server.construction.build_v3.measures import (
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
    add_dimensions_from_metric_expressions,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.dialect import Dialect

logger = logging.getLogger(__name__)


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
        include_temporal_filters: Whether to add temporal filters
        lookback_window: Lookback window for temporal filters

    Returns:
        Fully initialized BuildContext
    """
    ctx = BuildContext(
        session=session,
        metrics=metrics,
        dimensions=list(dimensions),
        filters=filters or [],
        dialect=dialect,
        use_materialized=use_materialized,
        include_temporal_filters=include_temporal_filters,
        lookback_window=lookback_window,
    )

    # Load all required nodes (single DB round trip)
    await load_nodes(ctx)

    # Validate we have at least one metric
    if not ctx.metrics:
        raise DJInvalidInputException("At least one metric is required")

    # Decompose metrics and group by parent node
    ctx.metric_groups, ctx.decomposed_metrics = await decompose_and_group_metrics(ctx)

    # Add dimensions referenced in metric expressions (e.g., LAG ORDER BY)
    add_dimensions_from_metric_expressions(ctx, ctx.decomposed_metrics)

    # Load any missing dimension nodes (and their upstreams, including sources)
    # This is needed for dimensions discovered from metric expressions
    # load_nodes adds to ctx.nodes rather than replacing, so this is safe to call again
    await load_nodes(ctx)

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
        include_temporal_filters: If True, adds DJ_LOGICAL_TIMESTAMP() filters on
            temporal partition columns of source nodes. Used for incremental
            materialization to ensure partition pruning.
        lookback_window: Lookback window for temporal filters (e.g., "3 DAY").
            If not provided, filters to exactly the logical timestamp partition.

    Returns:
        GeneratedMeasuresSQL with one GrainGroupSQL per aggregation level,
        plus context and decomposed metrics for efficient reuse by build_metrics_sql
    """
    # Setup context (loads nodes, decomposes metrics, adds dimensions from expressions)
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

    # Build grain groups from context
    return await build_grain_groups(ctx, metrics)


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
    await load_available_preaggs(ctx)

    # Process each metric group into grain group SQLs
    # Cross-fact metrics produce separate grain groups (one per parent node)
    all_grain_group_sqls: list[GrainGroupSQL] = []
    for metric_group in ctx.metric_groups:
        grain_group_sqls = process_metric_group(ctx, metric_group)
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

    return GeneratedMeasuresSQL(
        grain_groups=all_grain_group_sqls,
        dialect=ctx.dialect,
        requested_dimensions=ctx.dimensions,
        ctx=ctx,
        decomposed_metrics=ctx.decomposed_metrics,
    )


async def build_metrics_sql(
    session: AsyncSession,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str] | None = None,
    dialect: Dialect = Dialect.SPARK,
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
            return build_sql_from_cube_impl(ctx, cube, ctx.decomposed_metrics)

    # No cube - build grain groups
    measures_result = await build_grain_groups(ctx, metrics)

    if not measures_result.grain_groups:  # pragma: no cover
        raise DJInvalidInputException("No grain groups produced from measures SQL")

    return generate_metrics_sql(
        ctx,
        measures_result,
        ctx.decomposed_metrics,
    )
