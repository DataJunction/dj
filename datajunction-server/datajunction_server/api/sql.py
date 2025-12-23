"""
SQL related APIs.
"""

import logging
from http import HTTPStatus
from typing import List, Optional

from fastapi import BackgroundTasks, Depends, Query, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3 import (
    build_metrics_sql,
    build_measures_sql,
)
from datajunction_server.models.dialect import Dialect

from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.internal.caching.query_cache_manager import (
    QueryCacheManager,
    QueryRequestParams,
)
from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.database import Node
from datajunction_server.database.queryrequest import QueryBuildType
from datajunction_server.database.user import User
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access
from datajunction_server.models.metric import TranslatedSQL, V3TranslatedSQL
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.query import V3ColumnMetadata
from datajunction_server.models.sql import GeneratedSQL
from datajunction_server.utils import (
    get_current_user,
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
    session: AsyncSession = Depends(get_session),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
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
            current_user=current_user,
            validate_access=validate_access,
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
    session: AsyncSession = Depends(get_session),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
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
            current_user=current_user,
            validate_access=validate_access,
        ),
    )


class ComponentResponse(BaseModel):
    """Response model for a metric component in measures SQL."""

    name: str  # Component name (e.g., "unit_price_sum")
    expression: str  # The raw SQL expression (e.g., "unit_price")
    aggregation: Optional[str] = None  # Phase 1: "SUM", "COUNT", etc.
    merge: Optional[str] = (
        None  # Phase 2 (re-aggregation): "SUM", "COUNT_DISTINCT", etc.
    )
    aggregability: str  # "FULL", "LIMITED", or "NONE"


class MetricFormulaResponse(BaseModel):
    """Response model for a metric's combiner formula."""

    name: str  # Full metric name (e.g., "v3.avg_unit_price")
    short_name: str  # Short name (e.g., "avg_unit_price")
    combiner: str  # Formula combining components (e.g., "SUM(unit_price_sum) / SUM(unit_price_count)")
    components: List[str]  # Component names used in this metric
    is_derived: bool  # True if metric is derived from other metrics
    parent_name: Optional[str] = None  # Source fact/transform node name


class GrainGroupResponse(BaseModel):
    """Response model for a single grain group in measures SQL."""

    sql: str
    columns: List[V3ColumnMetadata]  # Clean V3 column metadata
    grain: List[str]
    aggregability: str
    metrics: List[str]
    components: List[
        ComponentResponse
    ]  # Metric components for materialization planning
    parent_name: str  # Source fact/transform node name


class MeasuresSQLResponse(BaseModel):
    """Response model for V3 measures SQL with multiple grain groups."""

    grain_groups: List[GrainGroupResponse]
    metric_formulas: List[MetricFormulaResponse]  # How metrics combine components
    dialect: Optional[str] = None
    requested_dimensions: List[str]


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
    V3 Measures SQL generation (for testing/development).

    Returns SQL with metric expressions aggregated to dimensional grain,
    with one SQL query per grain group. Different aggregability levels
    (FULL, LIMITED, NONE) produce different grain groups.

    Args:
        use_materialized: If True (default), use materialized tables when available.
            Set to False when generating SQL for materialization refresh to avoid
            circular references.

    Use cases:
    - Materialization: Each grain group can be materialized separately
    - Live queries: Pass to /sql/metrics/v3/ to get a single combined query
    """
    result = await build_measures_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        dialect=Dialect.SPARK,
        use_materialized=use_materialized,
    )

    # Build metric formulas from decomposed metrics
    metric_formulas = []
    if result.decomposed_metrics:
        for metric_name, decomposed in result.decomposed_metrics.items():
            # Get the combiner expression (the first projection from the derived AST)
            combiner_str = decomposed.combiner
            if decomposed.derived_ast and decomposed.derived_ast.select.projection:
                # Use the AST to get a cleaner representation
                combiner_str = str(decomposed.derived_ast.select.projection[0])

            # Determine parent node name from the first grain group that contains this metric
            parent_name = None
            for gg in result.grain_groups:
                if metric_name in gg.metrics:
                    parent_name = gg.parent_name
                    break

            # Check if this is a derived metric (references other metrics)
            is_derived = False
            if result.ctx:
                parent_names = result.ctx.parent_map.get(metric_name, [])
                is_derived = decomposed.is_derived_for_parents(
                    parent_names,
                    result.ctx.nodes,
                )

            metric_formulas.append(
                MetricFormulaResponse(
                    name=metric_name,
                    short_name=metric_name.split(".")[-1],
                    combiner=combiner_str,
                    components=[comp.name for comp in decomposed.components],
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
                        type=col.type,
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
                        name=comp.name,
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
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> V3TranslatedSQL:
    """
    V3 Metrics SQL generation (for testing/development).

    Returns SQL with final metric expressions applied, including
    handling for derived metrics.
    """

    result = await build_metrics_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        dialect=Dialect.SPARK,
    )

    return V3TranslatedSQL(
        sql=result.sql,
        columns=[
            V3ColumnMetadata(
                name=col.name,
                type=col.type,
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
    current_user: User = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
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
            current_user=current_user,
            validate_access=validate_access,
        ),
    )
