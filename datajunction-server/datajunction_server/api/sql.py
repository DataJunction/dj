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
    build_metrics_sql,
    build_measures_sql,
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

    See also: `/sql/metrics/v3/` for the final combined query with metric expressions.
    """
    result = await build_measures_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        dialect=Dialect.SPARK,
        use_materialized=use_materialized,
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
                combiner=combiner_str,
                components=component_names,
                is_derived=is_derived,
                parent_name=parent_name,
            ),
        )

    return MeasuresSQLResponse(
        grain_groups=[
            GrainGroupResponse(
                sql=gg.sql,
                columns=[
                    V3ColumnMetadata(
                        name=col.name,
                        type=str(col.type),  # Ensure string even if ColumnType object
                        semantic_entity=col.semantic_name,
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
            )
            for gg in result.grain_groups
        ],
        metric_formulas=metric_formulas,
        dialect=str(result.dialect) if result.dialect else None,
        requested_dimensions=result.requested_dimensions,
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
    use_materialized: bool = Query(True),
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> V3TranslatedSQL:
    """
    Generate final metrics SQL with fully computed metric expressions.

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
        use_materialized: If True (default), use materialized tables when available.
            Set to False when generating SQL for materialization refresh to avoid
            circular references.

    Returns:
        A single SQL query that:
        - Defines CTEs for each grain group (pre-aggregated component data) or
        uses materialized pre-agg tables when available
        - Joins grain groups on shared dimensions (if multiple)
        - Builds dimensions with coalesce and metrics with combiner expressions
        - Groups by dimensions to finalize re-aggregation

    See also: `/sql/measures/v3/` for the underlying pre-aggregated components.
    """

    result = await build_metrics_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        dialect=Dialect.SPARK,
        use_materialized=use_materialized,
    )

    return V3TranslatedSQL(
        sql=result.sql,
        columns=[
            V3ColumnMetadata(
                name=col.name,
                type=str(col.type),  # Ensure string even if ColumnType object
                semantic_entity=col.semantic_name,
                semantic_type=col.semantic_type,
            )
            for col in result.columns
        ],
        dialect=result.dialect,
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
