"""
SQL related APIs.
"""

import logging
from http import HTTPStatus
from typing import List, Optional

from fastapi import BackgroundTasks, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

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
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node_type import NodeType
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
