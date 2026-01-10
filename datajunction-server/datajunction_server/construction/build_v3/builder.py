"""
SQL Generation (V3): Measures and Metrics SQL Builders.
"""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.cube_matcher import (
    build_sql_from_cube,
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
    compute_metric_layers,
    generate_metrics_sql,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    GeneratedMeasuresSQL,
    GeneratedSQL,
    GrainGroupSQL,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.dialect import Dialect

logger = logging.getLogger(__name__)


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

    # Load available pre-aggregations (if use_materialized=True)
    await load_available_preaggs(ctx)

    # Validate we have at least one metric
    if not ctx.metrics:
        raise DJInvalidInputException("At least one metric is required")

    # Decompose metrics and group by parent node
    # Also returns the decomposed metrics to avoid redundant work
    metric_groups, decomposed_metrics = await decompose_and_group_metrics(ctx)

    # Process each metric group into grain group SQLs
    # Cross-fact metrics produce separate grain groups (one per parent node)
    all_grain_group_sqls: list[GrainGroupSQL] = []
    for metric_group in metric_groups:
        grain_group_sqls = process_metric_group(ctx, metric_group)
        all_grain_group_sqls.extend(grain_group_sqls)

    # Sanity check: all requested metrics should already be decomposed by group_metrics_by_parent
    for metric_name in metrics:
        if metric_name not in decomposed_metrics:  # pragma: no cover
            logger.warning(
                f"[BuildV3] Metric {metric_name} was not decomposed - this indicates a bug",
            )

    # Sanity check: all metrics in grain groups should already be decomposed
    all_grain_group_metrics = set()
    for gg in all_grain_group_sqls:
        all_grain_group_metrics.update(gg.metrics)

    for metric_name in all_grain_group_metrics:
        if metric_name not in decomposed_metrics:  # pragma: no cover
            logger.warning(
                f"[BuildV3] Grain group metric {metric_name} was not decomposed - "
                "this indicates a bug",
            )

    return GeneratedMeasuresSQL(
        grain_groups=all_grain_group_sqls,
        dialect=dialect,
        requested_dimensions=ctx.dimensions,  # Use ctx.dimensions which includes required dims
        ctx=ctx,
        decomposed_metrics=decomposed_metrics,
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
    # Try cube match first (early exit)
    if use_materialized:
        cube = await find_matching_cube(
            session,
            metrics,
            dimensions,
            require_availability=True,
        )
        if cube:
            logger.info(f"[BuildV3] Layer 1: Using cube {cube.name}")
            return await build_sql_from_cube(
                session=session,
                cube=cube,
                metrics=metrics,
                dimensions=dimensions,
                filters=filters,
                dialect=dialect,
            )

    # Get measures SQL with grain groups
    # This also returns context and decomposed metrics to avoid redundant work
    measures_result = await build_measures_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        dialect=dialect,
        use_materialized=use_materialized,
    )

    if not measures_result.grain_groups:  # pragma: no cover
        raise DJInvalidInputException("No grain groups produced from measures SQL")

    # Reuse context and decomposed metrics from measures SQL
    # This avoids redundant database queries and metric decomposition
    ctx = measures_result.ctx

    decomposed_metrics = measures_result.decomposed_metrics

    # Build metric dependency DAG and compute layers
    metric_layers = compute_metric_layers(ctx, decomposed_metrics)

    # Generate the combined SQL (returns GeneratedSQL with AST query)
    return generate_metrics_sql(
        ctx,
        measures_result,
        decomposed_metrics,
        metric_layers,
    )
