# pylint: disable=too-many-arguments
"""
Data related APIs.
"""
from http import HTTPStatus
from typing import Annotated, Dict, List, Optional

from fastapi import BackgroundTasks, Depends, Header, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload
from sse_starlette.sse import EventSourceResponse

from datajunction_server.api.helpers import (
    build_sql_for_multiple_metrics,
    query_event_stream,
)
from datajunction_server.api.sql import get_node_sql
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.history import ActivityType, EntityType, History
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJException,
    DJInvalidInputException,
    DJQueryServiceClientException,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    validate_access,
    validate_access_requests,
)
from datajunction_server.internal.engines import get_engine
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

settings = get_settings()
router = SecureAPIRouter(tags=["data"])


@router.post("/data/{node_name}/availability/", name="Add Availability State to Node")
async def add_availability_state(
    node_name: str,
    data: AvailabilityStateBase,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> JSONResponse:
    """
    Add an availability state to a node.
    """

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
    validate_access_requests(
        validate_access,
        current_user,
        [
            access.ResourceRequest(
                verb=access.ResourceRequestVerb.WRITE,
                access_object=access.Resource.from_node(node_revision),
            ),
        ],
        True,
    )

    if node.current.type == NodeType.SOURCE:  # type: ignore
        if (
            data.catalog != node_revision.catalog.name
            or data.schema_ != node_revision.schema_
            or data.table != node_revision.table
        ):
            raise DJException(
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
        min_temporal_partition=data.min_temporal_partition,
        max_temporal_partition=data.max_temporal_partition,
        partitions=[
            partition.dict() if not isinstance(partition, Dict) else partition
            for partition in data.partitions  # type: ignore
        ],
        categorical_partitions=data.categorical_partitions,
        temporal_partitions=data.temporal_partitions,
    )
    if node_revision.availability and not node_revision.availability.partitions:
        node_revision.availability.partitions = []
    session.add(node_revision)
    session.add(
        History(
            entity_type=EntityType.AVAILABILITY,
            node=node.name,  # type: ignore
            activity_type=ActivityType.CREATE,
            pre=AvailabilityStateBase.from_orm(old_availability).dict()
            if old_availability
            else {},
            post=AvailabilityStateBase.from_orm(node_revision.availability).dict(),
            user=current_user.username if current_user else None,
        ),
    )
    await session.commit()
    return JSONResponse(
        status_code=200,
        content={"message": "Availability state successfully posted"},
    )


@router.get("/data/{node_name}/", name="Get Data for a Node")
async def get_data(  # pylint: disable=too-many-locals
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
    cache_control: Annotated[str, Header()] = "",
    session: AsyncSession = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
    background_tasks: BackgroundTasks,
) -> QueryWithResults:
    """
    Gets data for a node
    """
    query, query_request = await get_node_sql(
        node_name,
        dimensions,
        filters,
        orderby,
        limit,
        session=session,
        engine_name=engine_name,
        engine_version=engine_version,
        current_user=current_user,
        validate_access=validate_access,
        background_tasks=background_tasks,
    )
    node = await Node.get_by_name(session, node_name, raise_if_not_exists=True)
    available_engines = node.current.catalog.engines  # type: ignore
    engine = (
        await get_engine(session, engine_name, engine_version)  # type: ignore
        if engine_name
        else available_engines[0]
    )
    if engine not in available_engines:
        raise DJInvalidInputException(  # pragma: no cover
            f"The selected engine is not available for the node {node_name}. "
            f"Available engines include: {', '.join(engine.name for engine in available_engines)}",
        )

    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=node.current.catalog.name,  # type: ignore
        engine_version=engine.version,
        submitted_query=query.sql,
        async_=async_,
    )
    result = query_service_client.submit_query(
        query_create,
        headers={"Cache-Control": cache_control},
    )
    query_request.query_id = result.id

    # Inject column info if there are results
    if result.results.__root__:  # pragma: no cover
        result.results.__root__[0].columns = query.columns  # type: ignore
    return result


@router.get("/stream/{node_name}", response_model=QueryWithResults)
async def get_data_stream_for_node(  # pylint: disable=R0914, R0913
    node_name: str,
    *,
    dimensions: List[str] = Query([], description="Dimensional attributes to group by"),
    filters: List[str] = Query([], description="Filters on dimensional attributes"),
    orderby: List[str] = Query([], description="Expression to order by"),
    limit: Optional[int] = Query(
        None,
        description="Number of rows to limit the data retrieved to",
    ),
    cache_control: Annotated[str, Header()] = "",
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
    background_tasks: BackgroundTasks,
) -> QueryWithResults:
    """
    Return data for a node using server side events
    """
    query, query_request = await get_node_sql(
        node_name,
        dimensions,
        [filter_ for filter_ in filters if filter_],
        orderby,
        limit,
        session=session,
        engine_name=engine_name,
        engine_version=engine_version,
        current_user=current_user,
        validate_access=validate_access,
        background_tasks=background_tasks,
    )
    if query_request and query_request.query_id:
        return EventSourceResponse(
            query_event_stream(
                query=QueryWithResults(
                    id=query_request.query_id,
                    submitted_query=query_request.query,
                    results=[],
                    errors=[],
                ),
                query_service_client=query_service_client,
                columns=query.columns,  # type: ignore
                request=request,
            ),
        )

    node = await Node.get_by_name(session, node_name, raise_if_not_exists=True)
    available_engines = node.current.catalog.engines  # type: ignore
    engine = (
        await get_engine(session, engine_name, engine_version)  # type: ignore
        if engine_name
        else available_engines[0]
    )
    if engine not in available_engines:
        raise DJInvalidInputException(  # pragma: no cover
            f"The selected engine is not available for the node {node_name}. "
            f"Available engines include: {', '.join(engine.name for engine in available_engines)}",
        )

    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=node.current.catalog.name,  # type: ignore
        engine_version=engine.version,
        submitted_query=query.sql,
        async_=True,
    )
    initial_query_info = query_service_client.submit_query(
        query_create,
        headers={"Cache-Control": cache_control or "max-age=86400"},
    )

    # Save the external query id reference
    query_request.query_id = initial_query_info.id
    session.add(query_request)
    await session.commit()

    return EventSourceResponse(
        query_event_stream(
            query=initial_query_info,
            query_service_client=query_service_client,
            columns=query.columns,  # type: ignore
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
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> QueryWithResults:
    """
    Return data for a specific query ID.
    """
    try:
        return query_service_client.get_query(query_id=query_id)
    except DJQueryServiceClientException as exc:
        raise DJException(
            message=str(exc.message),
            http_status_code=HTTPStatus.NOT_FOUND,
        ) from exc


@router.get("/data/", response_model=QueryWithResults, name="Get Data For Metrics")
async def get_data_for_metrics(  # pylint: disable=R0914, R0913
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    async_: bool = False,
    *,
    cache_control: Annotated[str, Header()] = "",
    session: AsyncSession = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> QueryWithResults:
    """
    Return data for a set of metrics with dimensions and filters
    """
    access_control = access.AccessControlStore(
        validate_access=validate_access,
        user=current_user,
        base_verb=access.ResourceRequestVerb.READ,
    )

    translated_sql, engine, catalog = await build_sql_for_multiple_metrics(
        session,
        metrics,
        dimensions,
        filters,
        orderby,
        limit,
        engine_name,
        engine_version,
        access_control,
    )

    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=catalog.name,
        engine_version=engine.version,
        submitted_query=translated_sql.sql,
        async_=async_,
    )
    result = query_service_client.submit_query(
        query_create,
        headers={"Cache-Control": cache_control},
    )

    # Inject column info if there are results
    if result.results.__root__:  # pragma: no cover
        result.results.__root__[0].columns = translated_sql.columns or []
    return result


@router.get("/stream/", response_model=QueryWithResults)
async def get_data_stream_for_metrics(  # pylint: disable=R0914, R0913
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    *,
    cache_control: Annotated[str, Header()] = "",
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
) -> QueryWithResults:
    """
    Return data for a set of metrics with dimensions and filters using server side events
    """
    translated_sql, engine, catalog = await build_sql_for_multiple_metrics(
        session,
        metrics,
        dimensions,
        filters,
        orderby,
        limit,
        engine_name,
        engine_version,
    )

    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=catalog.name,
        engine_version=engine.version,
        submitted_query=translated_sql.sql,
        async_=True,
    )
    # Submits the query, equivalent to calling POST /data/ directly
    initial_query_info = query_service_client.submit_query(
        query_create,
        headers={"Cache-Control": cache_control},
    )
    return EventSourceResponse(
        query_event_stream(
            query=initial_query_info,
            query_service_client=query_service_client,
            columns=translated_sql.columns,  # type: ignore
            request=request,
        ),
    )
