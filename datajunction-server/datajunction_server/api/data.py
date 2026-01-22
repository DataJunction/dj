"""
Data related APIs.
"""

import logging
from dataclasses import asdict
from typing import Callable, Dict, List, Optional, cast

from fastapi import BackgroundTasks, Depends, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload
from sse_starlette.sse import EventSourceResponse

from datajunction_server.api.helpers import (
    resolve_engine,
    query_event_stream,
)
from datajunction_server.construction.build_v3.builder import build_metrics_sql
from datajunction_server.models.dialect import Dialect
from datajunction_server.transpilation import transpile_sql
from datajunction_server.api.helpers import get_save_history
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.history import History
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJInvalidInputException,
    DJQueryServiceClientException,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    AccessChecker,
    AccessDenialMode,
    get_access_checker,
)
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.models import access
from datajunction_server.models.node import AvailabilityStateBase
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.utils import (
    get_current_user,
    get_query_service_client,
    get_session,
    get_settings,
)
from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.internal.caching.query_cache_manager import (
    QueryCacheManager,
    QueryRequestParams,
    QueryBuildType,
)
from datajunction_server.models.sql import GeneratedSQL

_logger = logging.getLogger(__name__)

settings = get_settings()
router = SecureAPIRouter(tags=["data"])


@router.post("/data/{node_name}/availability/", name="Add Availability State to Node")
async def add_availability_state(
    node_name: str,
    data: AvailabilityStateBase,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Add an availability state to a node.
    """
    _logger.info("Storing availability for node=%s", node_name)

    node = await Node.get_by_name(
        session,
        node_name,
        options=[
            joinedload(Node.current).options(
                selectinload(NodeRevision.catalog),
                selectinload(NodeRevision.availability),
            ),
        ],
        raise_if_not_exists=True,
    )

    # Source nodes require that any availability states set are for one of the defined tables
    node_revision = node.current  # type: ignore
    access_checker.add_request(
        access.ResourceRequest(
            verb=access.ResourceAction.WRITE,
            access_object=access.Resource.from_node(node_revision),
        ),
    )
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    if node.current.type == NodeType.SOURCE:  # type: ignore
        if (
            data.catalog != node_revision.catalog.name
            or data.schema_ != node_revision.schema_
            or data.table != node_revision.table
        ):
            raise DJInvalidInputException(
                message=(
                    "Cannot set availability state, "
                    "source nodes require availability "
                    "states to match the set table: "
                    f"{data.catalog}."
                    f"{data.schema_}."
                    f"{data.table} "
                    "does not match "
                    f"{node_revision.catalog.name}."
                    f"{node_revision.schema_}."
                    f"{node_revision.table} "
                ),
            )

    # Merge the new availability state with the current availability state if one exists
    old_availability = node_revision.availability
    if (
        old_availability
        and old_availability.catalog == data.catalog
        and old_availability.schema_ == data.schema_
        and old_availability.table == data.table
    ):
        data.merge(node_revision.availability)

    # Update the node with the new availability state
    node_revision.availability = AvailabilityState(
        catalog=data.catalog,
        schema_=data.schema_,
        table=data.table,
        valid_through_ts=data.valid_through_ts,
        url=data.url,
        min_temporal_partition=[
            str(part) for part in data.min_temporal_partition or []
        ],
        max_temporal_partition=[
            str(part) for part in data.max_temporal_partition or []
        ],
        partitions=[
            partition.model_dump() if not isinstance(partition, Dict) else partition
            for partition in (data.partitions or [])
        ],
        categorical_partitions=data.categorical_partitions,
        temporal_partitions=data.temporal_partitions,
        links=data.links,
    )
    if node_revision.availability and not node_revision.availability.partitions:
        node_revision.availability.partitions = []
    session.add(node_revision)
    await save_history(
        event=History(
            entity_type=EntityType.AVAILABILITY,
            node=node.name,  # type: ignore
            activity_type=ActivityType.CREATE,
            pre=AvailabilityStateBase.model_validate(
                old_availability,
            ).model_dump()
            if old_availability
            else {},
            post=AvailabilityStateBase.model_validate(
                node_revision.availability,
            ).model_dump(),
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    return JSONResponse(
        status_code=200,
        content={"message": "Availability state successfully posted"},
    )


@router.delete(
    "/data/{node_name}/availability/",
    name="Remove Availability State from Node",
)
async def remove_availability_state(
    node_name: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Remove an availability state from a node.
    """
    _logger.info("Removing availability for node=%s", node_name)

    node = cast(
        Node,
        await Node.get_by_name(
            session,
            node_name,
            options=[
                joinedload(Node.current).options(
                    selectinload(NodeRevision.catalog),
                    selectinload(NodeRevision.availability),
                ),
            ],
            raise_if_not_exists=True,
        ),
    )

    access_checker.add_request(
        access.ResourceRequest(
            verb=access.ResourceAction.WRITE,
            access_object=access.Resource.from_node(node.current),  # type: ignore
        ),
    )
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Save the old availability state for history record
    old_availability = (
        AvailabilityStateBase.from_orm(node.current.availability).dict()
        if node.current.availability
        else {}
    )
    node.current.availability = None
    await save_history(
        event=History(
            entity_type=EntityType.AVAILABILITY,
            node=node.name,  # type: ignore
            activity_type=ActivityType.DELETE,
            pre=old_availability,
            post={},
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    return JSONResponse(
        status_code=201,
        content={"message": "Availability state successfully removed"},
    )


@router.get("/data/{node_name}/", name="Get Data for a Node")
async def get_data(
    node_name: str,
    *,
    dimensions: List[str] = Query([], description="Dimensional attributes to group by"),
    filters: List[str] = Query([], description="Filters on dimensional attributes"),
    orderby: List[str] = Query([], description="Expression to order by"),
    limit: Optional[int] = Query(
        None,
        description="Number of rows to limit the data retrieved to",
    ),
    async_: bool = Query(
        default=False,
        description="Whether to run the query async or wait for results from the query engine",
    ),
    use_materialized: bool = Query(
        default=True,
        description="Whether to use materialized nodes when available",
    ),
    ignore_errors: bool = Query(
        default=False,
        description="Whether to ignore errors when building the query",
    ),
    query_params: str = Query("{}", description="Query parameters"),
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    background_tasks: BackgroundTasks,
    cache: Cache = Depends(get_cache),
) -> QueryWithResults:
    """
    Gets data for a node
    """
    request_headers = dict(request.headers)
    query_cache_manager = QueryCacheManager(
        cache=cache,
        query_type=QueryBuildType.NODE,
    )
    generated_sql: GeneratedSQL = await query_cache_manager.get_or_load(
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

    node = cast(
        Node,
        await Node.get_by_name(session, node_name, raise_if_not_exists=True),
    )
    engine = await resolve_engine(
        session=session,
        node=node,
        engine_name=engine_name,
        engine_version=engine_version,
        dialect=generated_sql.dialect,
    )

    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=node.current.catalog.name,  # type: ignore
        engine_version=engine.version,
        submitted_query=generated_sql.sql,
        async_=async_,
    )
    result = query_service_client.submit_query(
        query_create,
        request_headers=request_headers,
    )

    # Inject column info if there are results
    if result.results.root:  # pragma: no cover
        result.results.root[0].columns = generated_sql.columns  # type: ignore
    return result


@router.get("/stream/{node_name}", response_model=QueryWithResults)
async def get_data_stream_for_node(
    node_name: str,
    *,
    dimensions: List[str] = Query([], description="Dimensional attributes to group by"),
    filters: List[str] = Query([], description="Filters on dimensional attributes"),
    orderby: List[str] = Query([], description="Expression to order by"),
    limit: Optional[int] = Query(
        None,
        description="Number of rows to limit the data retrieved to",
    ),
    query_params: str = Query("{}", description="Query parameters"),
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    background_tasks: BackgroundTasks,
    cache: Cache = Depends(get_cache),
) -> QueryWithResults:
    """
    Return data for a node using server side events
    """
    request_headers = dict(request.headers)
    node = cast(
        Node,
        await Node.get_by_name(session, node_name, raise_if_not_exists=True),
    )
    engine = await resolve_engine(
        session=session,
        node=node,
        engine_name=engine_name,
        engine_version=engine_version,
    )
    query_cache_manager = QueryCacheManager(
        cache=cache,
        query_type=QueryBuildType.NODE,
    )
    translated_sql = await query_cache_manager.get_or_load(
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
            use_materialized=True,
            ignore_errors=False,
        ),
    )
    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=node.current.catalog.name,  # type: ignore
        engine_version=engine.version,
        submitted_query=translated_sql.sql,
        async_=True,
    )
    initial_query_info = query_service_client.submit_query(
        query_create,
        request_headers=request_headers,
    )
    return EventSourceResponse(  # pragma: no cover
        query_event_stream(
            query=initial_query_info,
            query_service_client=query_service_client,
            request_headers=request_headers,
            columns=translated_sql.columns,  # type: ignore
            request=request,
        ),
    )


@router.get(
    "/data/query/{query_id}",
    response_model=QueryWithResults,
    name="Get Data For Query ID",
)
def get_data_for_query(
    query_id: str,
    *,
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> QueryWithResults:
    """
    Return data for a specific query ID.
    """
    request_headers = dict(request.headers)
    try:
        return query_service_client.get_query(
            query_id=query_id,
            request_headers=request_headers,
        )
    except DJQueryServiceClientException as exc:
        raise DJQueryServiceClientException(  # pragma: no cover
            f"DJ Query Service Error: {exc.message}",
        ) from exc


@router.get("/data/", response_model=QueryWithResults, name="Get Data For Metrics")
async def get_data_for_metrics(
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    async_: bool = False,
    use_materialized: bool = Query(
        default=True,
        description="Whether to use materialized tables when available",
    ),
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
) -> QueryWithResults:
    """
    Return data for a set of metrics with dimensions and filters.

    Uses v3 SQL builder which supports:
    - Derived metrics (multi-level)
    - Cube matching for materialized tables
    - Grain group joins for metrics from different facts
    """
    request_headers = dict(request.headers)

    # Build SQL using v3 builder (generates Spark dialect by default)
    generated_sql = await build_metrics_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters if filters else None,
        orderby=orderby if orderby else None,
        limit=limit,
        dialect=Dialect.SPARK,
        use_materialized=use_materialized,
    )

    # Get the first metric node to resolve engine and catalog
    node = cast(
        Node,
        await Node.get_by_name(session, metrics[0], raise_if_not_exists=True),
    )
    engine = await resolve_engine(
        session=session,
        node=node,
        engine_name=engine_name,
        engine_version=engine_version,
        dialect=generated_sql.dialect,
    )

    # Transpile SQL to the engine's dialect
    final_sql = transpile_sql(generated_sql.sql, engine.dialect)

    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=node.current.catalog.name,
        engine_version=engine.version,
        submitted_query=final_sql,
        async_=async_,
    )
    result = query_service_client.submit_query(
        query_create,
        request_headers=request_headers,
    )

    # Inject column info if there are results
    if result.results.root:  # pragma: no cover
        # Convert dataclass columns to dicts for proper serialization
        result.results.root[0].columns = (
            [asdict(col) for col in generated_sql.columns]  # type: ignore
            if generated_sql.columns
            else []
        )
    return result


@router.get("/stream/", response_model=QueryWithResults)
async def get_data_stream_for_metrics(
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    use_materialized: bool = Query(
        default=True,
        description="Whether to use materialized tables when available",
    ),
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: User = Depends(get_current_user),
) -> QueryWithResults:
    """
    Return data for a set of metrics with dimensions and filters using server sent events.

    Uses v3 SQL builder which supports:
    - Derived metrics (multi-level)
    - Cube matching for materialized tables
    - Grain group joins for metrics from different facts
    """
    request_headers = dict(request.headers)

    # Build SQL using v3 builder (generates Spark dialect by default)
    generated_sql = await build_metrics_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters if filters else None,
        orderby=orderby if orderby else None,
        limit=limit,
        dialect=Dialect.SPARK,
        use_materialized=use_materialized,
    )

    # Get the first metric node to resolve engine and catalog
    node = cast(
        Node,
        await Node.get_by_name(session, metrics[0], raise_if_not_exists=True),
    )
    engine = await resolve_engine(
        session=session,
        node=node,
        engine_name=engine_name,
        engine_version=engine_version,
        dialect=generated_sql.dialect,
    )

    # Transpile SQL to the engine's dialect
    final_sql = transpile_sql(generated_sql.sql, engine.dialect)

    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=node.current.catalog.name,
        engine_version=engine.version,
        submitted_query=final_sql,
        async_=True,
    )
    # Submits the query, equivalent to calling POST /data/ directly
    initial_query_info = query_service_client.submit_query(
        query_create,
        request_headers=request_headers,
    )
    return EventSourceResponse(
        query_event_stream(
            query=initial_query_info,
            request_headers=request_headers,
            query_service_client=query_service_client,
            columns=[asdict(col) for col in generated_sql.columns]  # type: ignore
            if generated_sql.columns
            else [],
            request=request,
        ),
    )
