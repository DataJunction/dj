"""
Data related APIs.
"""
from http import HTTPStatus
from typing import List, Optional

from fastapi import Depends, Query, Request
from fastapi.responses import JSONResponse
from sqlmodel import Session
from sse_starlette.sse import EventSourceResponse

from datajunction_server.api.helpers import (
    build_sql_for_multiple_metrics,
    get_engine,
    get_node_by_name,
    get_query,
    query_event_stream,
    validate_orderby,
)
from datajunction_server.errors import (
    DJException,
    DJInvalidInputException,
    DJQueryServiceClientException,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    validate_access,
    validate_access_nodes,
)
from datajunction_server.models import History, User, access
from datajunction_server.models.history import ActivityType, EntityType
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node import (
    AvailabilityState,
    AvailabilityStateBase,
    NodeType,
)
from datajunction_server.models.query import (
    ColumnMetadata,
    QueryCreate,
    QueryWithResults,
)
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
def add_availability_state(
    node_name: str,
    data: AvailabilityStateBase,
    *,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> JSONResponse:
    """
    Add an availability state to a node.
    """
    node = get_node_by_name(session, node_name)

    # Source nodes require that any availability states set are for one of the defined tables
    node_revision = node.current
    validate_access_nodes(
        validate_access,
        access.ResourceRequestVerb.WRITE,
        current_user,
        [node_revision],
        True,
    )

    if node.current.type == NodeType.SOURCE:
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
        node_revision.availability
        and node_revision.availability.catalog == node.current.catalog.name
        and node_revision.availability.schema_ == data.schema_
        and node_revision.availability.table == data.table
    ):
        data.merge(node_revision.availability)

    # Update the node with the new availability state
    node_revision.availability = AvailabilityState.from_orm(data)
    if node_revision.availability and not node_revision.availability.partitions:
        node_revision.availability.partitions = []
    session.add(node_revision)
    session.add(
        History(
            entity_type=EntityType.AVAILABILITY,
            node=node.name,
            activity_type=ActivityType.CREATE,
            pre=AvailabilityStateBase.parse_obj(old_availability).dict()
            if old_availability
            else {},
            post=AvailabilityStateBase.parse_obj(node_revision.availability).dict(),
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    return JSONResponse(
        status_code=200,
        content={"message": "Availability state successfully posted"},
    )


@router.get("/data/{node_name}/", name="Get Data for a Node")
def get_data(  # pylint: disable=too-many-locals
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
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> QueryWithResults:
    """
    Gets data for a node
    """

    node = get_node_by_name(session, node_name)

    available_engines = node.current.catalog.engines
    engine = (
        get_engine(session, engine_name, engine_version)  # type: ignore
        if engine_name
        else available_engines[0]
    )
    if engine not in available_engines:
        raise DJInvalidInputException(  # pragma: no cover
            f"The selected engine is not available for the node {node_name}. "
            f"Available engines include: {', '.join(engine.name for engine in available_engines)}",
        )
    validate_orderby(orderby, [node_name], dimensions)

    access_control = access.AccessControlStore(
        validate_access=validate_access,
        user=current_user,
        base_verb=access.ResourceRequestVerb.EXECUTE,
    )

    query_ast = get_query(
        session=session,
        node_name=node_name,
        dimensions=dimensions,
        filters=filters,
        orderby=orderby,
        limit=limit,
        engine=engine,
        access_control=access_control,
    )

    columns = [
        ColumnMetadata(name=col.alias_or_name.name, type=str(col.type))  # type: ignore
        for col in query_ast.select.projection
    ]

    query = TranslatedSQL(
        sql=str(query_ast),
        columns=columns,
    )

    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=node.current.catalog.name,
        engine_version=engine.version,
        submitted_query=query.sql,
        async_=async_,
    )
    result = query_service_client.submit_query(query_create)
    # Inject column info if there are results
    if result.results.__root__:  # pragma: no cover
        result.results.__root__[0].columns = columns
    return result


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
def get_data_for_metrics(  # pylint: disable=R0914, R0913
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    async_: bool = False,
    *,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
) -> QueryWithResults:
    """
    Return data for a set of metrics with dimensions and filters
    """
    translated_sql, engine, catalog = build_sql_for_multiple_metrics(
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
        async_=async_,
    )
    result = query_service_client.submit_query(query_create)

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
    session: Session = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
) -> QueryWithResults:
    """
    Return data for a set of metrics with dimensions and filters using server side events
    """
    translated_sql, engine, catalog = build_sql_for_multiple_metrics(
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
    initial_query_info = query_service_client.submit_query(query_create)
    return EventSourceResponse(
        query_event_stream(
            query=initial_query_info,
            query_service_client=query_service_client,
            columns=translated_sql.columns,  # type: ignore
            request=request,
        ),
    )
