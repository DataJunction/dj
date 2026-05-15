"""
Router for various system overview metrics
"""

import logging
from fastapi import BackgroundTasks, Depends, Query, Request
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from datajunction_server.models.system import DimensionStats, SystemMetricData
from datajunction_server.sql.dag import (
    get_cubes_using_dimensions,
    get_dimension_dag_indegree,
)
from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.database.node import Node
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import (
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
    Returns the DJ system metrics (available as metric nodes in DJ),
    each as ``{name, display_name}``.
    """
    metrics = await Node.find_by(
        session=session,
        namespace=settings.seed_setup.system_namespace,
        node_types=[NodeType.METRIC],
        options=[joinedload(Node.current)],
    )
    return [
        {
            "name": m.name,
            "display_name": (m.current.display_name if m.current else None) or m.name,
            "description": (m.current.description if m.current else None) or "",
            "custom_metadata": (m.current.custom_metadata if m.current else None) or {},
        }
        for m in metrics
    ]


@router.get("/system/data/{metric_name}")
async def get_data_for_system_metric(
    metric_name: str,
    dimensions: list[str] = Query([]),
    filters: list[str] = Query([]),
    orderby: list[str] = Query([]),
    limit: int | None = None,
    session: AsyncSession = Depends(get_session),
    *,
    background_tasks: BackgroundTasks,
    cache: Cache = Depends(get_cache),
    request: Request,
) -> SystemMetricData:
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
            engine_name=settings.seed_setup.system_engine_name,
        ),
    )
    # The /system/data endpoint executes the SQL directly against the DJ
    # metadata database (Postgres), which doesn't support 3-part
    # ``catalog.schema.table`` references. v3 emits the system catalog
    # name (``dj_metadata``) as a prefix on physical-table references —
    # strip it here so Postgres can resolve the remaining ``schema.table``.
    sql_to_run = translated_sql.sql.replace(
        f"{settings.seed_setup.system_catalog_name}.",
        "",
    )
    columns = [
        col.semantic_entity if col.semantic_type == "dimension" else col.node
        for col in translated_sql.columns  # type: ignore
    ]
    results = await session.execute(text(sql_to_run))
    return SystemMetricData(
        columns=columns,
        rows=[list(row) for row in results],
    )


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
