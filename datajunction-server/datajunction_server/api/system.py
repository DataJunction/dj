"""
Router for various system overview metrics
"""

import logging
from fastapi import BackgroundTasks, Depends, Query, Request
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.internal.access.authorization import (
    validate_access,
)
from datajunction_server.models import access
from datajunction_server.models.system import DimensionStats, RowOutput
from datajunction_server.sql.dag import (
    get_cubes_using_dimensions,
    get_dimension_dag_indegree,
)
from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.database.user import User
from datajunction_server.database.node import Node
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import (
    get_current_user,
    get_session,
    get_settings,
)
from datajunction_server.internal.caching.query_cache_manager import (
    QueryCacheManager,
    QueryRequestParams,
    QueryBuildType,
)
from datajunction_server.models.sql import GeneratedSQL

logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["System"])


@router.get("/system/metrics")
async def list_system_metrics(
    session: AsyncSession = Depends(get_session),
):
    """
    Returns a list of DJ system metrics (available as metric nodes in DJ).
    """
    metrics = await Node.find_by(
        session=session,
        namespace=settings.seed_setup.system_namespace,
        node_types=[NodeType.METRIC],
    )
    return [m.name for m in metrics]


@router.get("/system/data/{metric_name}")
async def get_data_for_system_metric(
    metric_name: str,
    dimensions: list[str] = Query([]),
    filters: list[str] = Query([]),
    orderby: list[str] = Query([]),
    limit: int | None = None,
    session: AsyncSession = Depends(get_session),
    *,
    current_user: User = Depends(get_current_user),
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
    cache: Cache = Depends(get_cache),
    request: Request,
) -> list[list[RowOutput]]:
    """
    This is not a generic data for metrics endpoint, but rather a specific endpoint for
    system overview metrics that are automatically defined by DJ, such as the number of nodes.
    This endpoint will return data for any system metric, cut by their available dimensions
    and filters.

    This setup circumvents going to the query service to get metric data, since all system
    metrics can be computed directly from the database.

    For a list of available system metrics, see the `/system/metrics` endpoint. All dimensions
    for the metric can be discovered through the usual endpoints.
    """
    query_cache_manager = QueryCacheManager(cache=cache, query_type=QueryBuildType.NODE)
    # e.g., "system.dj.number_of_nodes"
    translated_sql: GeneratedSQL = await query_cache_manager.get_or_load(
        background_tasks,
        request,
        QueryRequestParams(
            nodes=[metric_name],
            dimensions=dimensions,
            filters=filters,
            orderby=orderby,
            limit=limit,
            current_user=current_user,
            validate_access=validate_access,
        ),
    )
    results = await session.execute(text(translated_sql.sql))
    output = [
        [
            RowOutput(
                value=value,
                col=col.semantic_entity
                if col.semantic_type == "dimension"
                else col.node,
            )
            for value, col in zip(row, translated_sql.columns)  # type: ignore
        ]
        for row in results
    ]
    return output


@router.get("/system/dimensions", response_model=list[DimensionStats])
async def get_dimensions_stats(
    session: AsyncSession = Depends(get_session),
) -> list[DimensionStats]:
    """
    List dimensions statistics, including the indegree of the dimension in the DAG
    and the number of cubes that use the dimension.
    """
    find_available_dimensions = select(Node.name).where(
        Node.type == NodeType.DIMENSION,
        Node.deactivated_at.is_(None),
    )
    dimension_node_names = [
        row.name for row in await session.execute(find_available_dimensions)
    ]

    node_indegrees = await get_dimension_dag_indegree(session, dimension_node_names)
    cubes_using_dims = await get_cubes_using_dimensions(session, dimension_node_names)
    return sorted(
        [
            DimensionStats(
                name=dim,
                indegree=node_indegrees.get(dim, 0),
                cube_count=cubes_using_dims.get(dim, 0),
            )
            for dim in dimension_node_names
        ],
        key=lambda stats: -stats.indegree,
    )
