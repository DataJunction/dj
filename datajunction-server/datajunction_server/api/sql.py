"""
SQL related APIs.
"""

import logging
from http import HTTPStatus
from typing import List, Optional

from fastapi import BackgroundTasks, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.utils import get_current_user
from datajunction_server.construction.build_v3 import (
    build_combiner_sql,
    build_metrics_sql,
    build_measures_sql,
    resolve_dialect_and_engine_for_metrics,
)
from datajunction_server.construction.build_v3.combiners import (
    build_combiner_sql_from_preaggs,
)
from datajunction_server.models.dialect import Dialect
from datajunction_server.sql.parsing import ast

from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.internal.caching.query_cache_manager import (
    QueryCacheManager,
    QueryRequestParams,
)
from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.database import Node
from datajunction_server.database.user import User
from datajunction_server.database.queryrequest import QueryBuildType
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.metric import TranslatedSQL, V3TranslatedSQL
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.query import V3ColumnMetadata
from datajunction_server.models.sql import (
    CombinedMeasuresSQLResponse,
    ComponentResponse,
    GrainGroupResponse,
    MeasuresSQLResponse,
    MetricFormulaResponse,
)
from datajunction_server.models.sql import GeneratedSQL
from datajunction_server.utils import (
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["sql"])


@router.get(
    "/sql/measures/v2/",
    response_model=List[GeneratedSQL],
    name="Get Measures SQL",
)
async def get_measures_sql_for_cube_v2(
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    preaggregate: bool = Query(
        False,
        description=(
            "Whether to pre-aggregate to the requested dimensions so that "
            "subsequent queries are more efficient."
        ),
    ),
    query_params: str = Query("{}", description="Query parameters"),
    *,
    include_all_columns: bool = Query(
        False,
        description=(
            "Whether to include all columns or only those necessary "
            "for the metrics and dimensions in the cube"
        ),
    ),
    cache: Cache = Depends(get_cache),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    use_materialized: bool = True,
    background_tasks: BackgroundTasks,
    request: Request,
) -> List[GeneratedSQL]:
    """
    Return measures SQL for a set of metrics with dimensions and filters.

    The measures query can be used to produce intermediate table(s) with all the measures
    and dimensions needed prior to applying specific metric aggregations.

    This endpoint returns one SQL query per upstream node of the requested metrics.
    For example, if some of your metrics are aggregations on measures in parent node A
    and others are aggregations on measures in parent node B, this endpoint will generate
    two measures queries, one for A and one for B.
    """
    query_cache_manager = QueryCacheManager(
        cache=cache,
        query_type=QueryBuildType.MEASURES,
    )
    return await query_cache_manager.get_or_load(
        background_tasks,
        request,
        QueryRequestParams(
            nodes=metrics,
            dimensions=dimensions,
            filters=filters,
            engine_name=engine_name,
            engine_version=engine_version,
            orderby=orderby,
            query_params=query_params,
            include_all_columns=include_all_columns,
            preaggregate=preaggregate,
            use_materialized=use_materialized,
        ),
    )


@router.get(
    "/sql/{node_name}/",
    response_model=TranslatedSQL,
    name="Get SQL For A Node",
)
async def get_sql(
    node_name: str,
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    query_params: str = Query("{}", description="Query parameters"),
    *,
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    background_tasks: BackgroundTasks,
    ignore_errors: Optional[bool] = True,
    use_materialized: Optional[bool] = True,
    cache: Cache = Depends(get_cache),
    request: Request,
) -> TranslatedSQL:
    """
    Return SQL for a node.
    """
    query_cache_manager = QueryCacheManager(
        cache=cache,
        query_type=QueryBuildType.NODE,
    )
    return await query_cache_manager.get_or_load(
        background_tasks,
        request,
        QueryRequestParams(
            nodes=[node_name],
            dimensions=dimensions,
            filters=filters,
            orderby=orderby,
            limit=limit,
            query_params=query_params,
            engine_name=engine_name,
            engine_version=engine_version,
            use_materialized=use_materialized,
            ignore_errors=ignore_errors,
        ),
    )


@router.get(
    "/sql/measures/v3/",
    response_model=MeasuresSQLResponse,
    name="Get Measures SQL V3",
    tags=["sql", "v3"],
)
async def get_measures_sql_v3(
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    use_materialized: bool = Query(True),
    dialect: Dialect = Query(
        Dialect.SPARK,
        description="SQL dialect for the generated query.",
    ),
    include_temporal_filters: bool = Query(
        False,
        description=(
            "Whether to include temporal partition filters. Only applies if "
            "the metrics and dimensions resolve to a cube with temporal partitions."
        ),
    ),
    lookback_window: Optional[str] = Query(
        None,
        description=(
            "Lookback window for temporal filters (e.g., '3 DAY', '1 WEEK'). "
            "Only applicable when include_temporal_filters is True."
        ),
    ),
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> MeasuresSQLResponse:
    """
    Generate pre-aggregated measures SQL for the requested metrics.

    Measures SQL represents the first stage of metric computation - it decomposes
    each metric into its atomic aggregation components (e.g., SUM(amount), COUNT(*))
    and produces SQL that computes these components at the requested dimensional grain.

    Metrics are separated into grain groups, which represent sets of metrics that can be
    computed together at a common grain. Each grain group produces its own SQL query, which
    can be materialized independently to produce intermediate tables that are then queried
    to compute final metric values.

    Returns:
        One or more `GrainGroupSQL` objects, each containing:
        - SQL query computing metric components at the specified grain
        - Column metadata with semantic types
        - Component details for downstream re-aggregation

    Args:
        use_materialized: If True (default), use materialized tables when available.
            Set to False when generating SQL for materialization refresh to avoid
            circular references.
        include_temporal_filters: If True, checks if metrics+dimensions resolve to
            a cube with temporal partitions, and applies partition filters if so.
        lookback_window: Lookback window for temporal filters when applicable.

    See also: `/sql/metrics/v3/` for the final combined query with metric expressions.
    """
    result = await build_measures_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        dialect=dialect,
        use_materialized=use_materialized,
        include_temporal_filters=include_temporal_filters,
        lookback_window=lookback_window,
    )

    # Build a unified component_aliases map from all grain groups
    # This maps component hash names -> actual SQL column aliases
    all_component_aliases: dict[str, str] = {}
    for gg in result.grain_groups:
        all_component_aliases.update(gg.component_aliases)

    # Build metric formulas from decomposed metrics
    metric_formulas = []
    for metric_name, decomposed in result.decomposed_metrics.items():
        # Get the combiner expression and rewrite component names to actual SQL aliases
        from copy import deepcopy

        combiner_ast = deepcopy(decomposed.derived_ast.select.projection[0])

        # Replace component hash names with actual SQL aliases in the combiner
        for col in combiner_ast.find_all(ast.Column):
            col_name = col.name.name if col.name else None
            if col_name and col_name in all_component_aliases:
                col.name = ast.Name(all_component_aliases[col_name])
                col._table = None

        combiner_str = str(combiner_ast)

        # Determine parent node name from the first grain group that contains this metric
        parent_name = None
        for gg in result.grain_groups:
            if metric_name in gg.metrics:
                parent_name = gg.parent_name
                break

        # Check if this is a derived metric (references other metrics)
        parent_names = result.ctx.parent_map.get(metric_name, [])
        is_derived = decomposed.is_derived_for_parents(
            parent_names,
            result.ctx.nodes,
        )

        # Get component column names as they appear in SQL
        # Use the unified component_aliases to resolve hash names -> actual aliases
        component_names = [
            all_component_aliases.get(comp.name, comp.name)
            for comp in decomposed.components
        ]

        metric_formulas.append(
            MetricFormulaResponse(
                name=metric_name,
                short_name=metric_name.split(".")[-1],
                query=decomposed.metric_node.current.query,
                combiner=combiner_str,
                components=component_names,
                is_derived=is_derived,
                parent_name=parent_name,
            ),
        )

    # Calculate scan estimates for each grain group
    grain_group_responses = []
    for gg in result.grain_groups:
        grain_group_responses.append(
            GrainGroupResponse(
                sql=gg.sql,
                columns=[
                    V3ColumnMetadata(
                        name=col.name,
                        type=str(col.type),  # Ensure string even if ColumnType object
                        semantic_name=col.semantic_name,
                        semantic_type=col.semantic_type,
                    )
                    for col in gg.columns
                ],
                grain=gg.grain,
                aggregability=gg.aggregability.value
                if hasattr(gg.aggregability, "value")
                else str(gg.aggregability),
                metrics=gg.metrics,
                components=[
                    ComponentResponse(
                        # Use actual SQL alias (metric short name for single-component, hash for multi)
                        name=gg.component_aliases.get(comp.name, comp.name),
                        expression=comp.expression,
                        aggregation=comp.aggregation,
                        merge=comp.merge,
                        aggregability=comp.rule.type.value
                        if hasattr(comp.rule.type, "value")
                        else str(comp.rule.type),
                    )
                    for comp in gg.components
                ],
                parent_name=gg.parent_name,
                scan_estimate=gg.scan_estimate,
            ),
        )

    return MeasuresSQLResponse(
        grain_groups=grain_group_responses,
        metric_formulas=metric_formulas,
        dialect=str(result.dialect) if result.dialect else None,
        requested_dimensions=result.requested_dimensions,
    )


@router.get(
    "/sql/measures/v3/combined",
    response_model=CombinedMeasuresSQLResponse,
    name="Get Combined Measures SQL V3",
    tags=["sql", "v3"],
)
async def get_combined_measures_sql_v3(
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    use_preagg_tables: bool = Query(
        False,
        description=(
            "If False (default), compute from scratch using source tables. "
            "If True, compute from pre-aggregation tables"
        ),
    ),
    dialect: Dialect = Query(
        Dialect.SPARK,
        description="SQL dialect for the generated query.",
    ),
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> CombinedMeasuresSQLResponse:
    """
    Generate combined pre-aggregated measures SQL for the requested metrics.

    This endpoint combines multiple grain groups into a single SQL query using
    FULL OUTER JOIN on shared dimensions. Dimension columns are wrapped with
    COALESCE to handle NULLs from non-matching rows.

    This is useful for:
    - Druid cube materialization where a single combined table is needed
    - Simplifying downstream queries that need data from multiple fact tables
    - Pre-computing joined aggregations for dashboards

    The combined SQL contains:
    - CTEs for each grain group's pre-aggregated data
    - FULL OUTER JOIN between grain groups on shared dimensions
    - COALESCE on dimension columns to handle NULL values
    - All measure columns from all grain groups

    Args:
        metrics: List of metric names to include
        dimensions: List of dimensions to group by (the grain)
        filters: Optional filters to apply
        use_preagg_tables: If False (default), compute from scratch using source tables.
            If True, read from pre-aggregation tables.

    Returns:
        Combined SQL query with column metadata and grain information.

    See also:
        - `/sql/measures/v3/` for individual grain group queries
        - `/sql/metrics/v3/` for final metric computations with combiner expressions
    """
    if use_preagg_tables:
        # Generate SQL that reads from pre-agg tables (deterministic names)
        (
            combined_result,
            source_tables,
            _,
        ) = await build_combiner_sql_from_preaggs(  # pragma: no cover
            session=session,
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            dialect=dialect,
        )
    else:
        # Build the measures SQL to get grain groups (compute from scratch)
        result = await build_measures_sql(
            session=session,
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            dialect=dialect,
            use_materialized=False,  # Don't use materialized - compute from source
        )

        if not result.grain_groups:
            raise DJInvalidInputException(  # pragma: no cover
                message="No grain groups generated. Ensure metrics and dimensions are valid.",
                http_status_code=HTTPStatus.BAD_REQUEST,
            )

        # Combine the grain groups using the combiner logic
        combined_result = build_combiner_sql(result.grain_groups)

        # Collect source tables from grain groups
        # These are the upstream tables used in the queries
        source_tables = []
        for gg in result.grain_groups:
            # Extract table references from the query
            source_tables.append(gg.parent_name)

    return CombinedMeasuresSQLResponse(
        sql=combined_result.sql,
        columns=[
            V3ColumnMetadata(
                name=col.name,
                type=col.type,
                semantic_name=col.semantic_name,
                semantic_type=col.semantic_type,
            )
            for col in combined_result.columns
        ],
        grain=combined_result.shared_dimensions,
        grain_groups_combined=combined_result.grain_groups_combined,
        dialect=str(dialect),
        use_preagg_tables=use_preagg_tables,
        source_tables=source_tables,
    )


@router.get(
    "/sql/metrics/v3/",
    response_model=V3TranslatedSQL,
    name="Get Metrics SQL V3",
    tags=["sql", "v3"],
)
async def get_metrics_sql_v3(
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query(
        [],
        description="ORDER BY clauses using semantic names (e.g., 'v3.total_revenue DESC', 'v3.date.month')",
    ),
    limit: Optional[int] = Query(
        None,
        description="Maximum number of rows to return",
    ),
    use_materialized: bool = Query(True),
    dialect: Optional[Dialect] = Query(
        None,
        description="SQL dialect for the generated query. If not specified, "
        "auto-resolves based on cube availability.",
    ),
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> V3TranslatedSQL:
    """
    Generate final metrics SQL with fully computed metric expressions for the
    requested metrics, dimensions, and filters using the specified dialect.

    Metrics SQL is the second (and final) stage of metric computation - it takes
    the pre-aggregated components from Measures SQL and applies combiner expressions
    to produce the actual metric values requested.

    - Metric components are re-aggregated as needed to match the requested
    dimensional grain.

    - Derived metrics (defined as expressions over other metrics)
    (e.g., `conversion_rate = order_count / visitor_count`) are computed by
    substituting component references with their re-aggregated expressions.

    - When metrics come from different fact tables, their
    grain groups are FULL OUTER JOINed on the common dimensions, with COALESCE
    for dimension columns to handle NULLs from non-matching rows.

    - Dimension references in metric expressions are resolved to their
    final column aliases.

    Args:
        metrics: List of metric names to include
        dimensions: List of dimensions to group by (the grain)
        filters: Optional filters to apply
        dialect: SQL dialect for the generated query. If not specified, auto-resolves
            based on cube availability (uses Druid if cube exists, else metric's catalog).
        use_materialized: If True (default), use materialized tables when available.
            Set to False when generating SQL for materialization refresh to avoid
            circular references.
    """
    # Auto-resolve dialect if not explicitly provided
    resolved_dialect = dialect
    if resolved_dialect is None:  # pragma: no branch
        execution_ctx = await resolve_dialect_and_engine_for_metrics(
            session=session,
            metrics=metrics,
            dimensions=dimensions,
            use_materialized=use_materialized,
        )
        resolved_dialect = execution_ctx.dialect

    result = await build_metrics_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        orderby=orderby if orderby else None,
        limit=limit,
        dialect=resolved_dialect,
        use_materialized=use_materialized,
    )

    return V3TranslatedSQL(
        sql=result.sql,
        columns=[
            V3ColumnMetadata(
                name=col.name,
                type=str(col.type),  # Ensure string even if ColumnType object
                semantic_name=col.semantic_name,
                semantic_type=col.semantic_type,
            )
            for col in result.columns
        ],
        dialect=result.dialect,
        cube_name=result.cube_name,
        scan_estimate=result.scan_estimate,
    )


@router.get("/sql/", response_model=TranslatedSQL, name="Get SQL For Metrics")
async def get_sql_for_metrics(
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    query_params: str = Query("{}", description="Query parameters"),
    *,
    session: AsyncSession = Depends(get_session),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    ignore_errors: Optional[bool] = True,
    use_materialized: Optional[bool] = True,
    background_tasks: BackgroundTasks,
    cache: Cache = Depends(get_cache),
    request: Request,
) -> TranslatedSQL:
    """
    Return SQL for a set of metrics with dimensions and filters
    """
    # make sure all metrics exist and have correct node type
    nodes = [
        await Node.get_by_name(session, node, raise_if_not_exists=True)
        for node in metrics
    ]
    non_metric_nodes = [node for node in nodes if node and node.type != NodeType.METRIC]

    if non_metric_nodes:
        raise DJInvalidInputException(
            message="All nodes must be of metric type, but some are not: "
            f"{', '.join([f'{n.name} ({n.type})' for n in non_metric_nodes])} .",
            http_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        )

    query_cache_manager = QueryCacheManager(
        cache=cache,
        query_type=QueryBuildType.METRICS,
    )
    return await query_cache_manager.get_or_load(
        background_tasks,
        request,
        QueryRequestParams(
            nodes=metrics,
            dimensions=dimensions,
            filters=filters,
            limit=limit,
            orderby=orderby,
            query_params=query_params,
            engine_name=engine_name,
            engine_version=engine_version,
            use_materialized=use_materialized,
            ignore_errors=ignore_errors,
        ),
    )
