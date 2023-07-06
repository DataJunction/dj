"""
Data related APIs.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlmodel import Session

from datajunction_server.api.helpers import (
    build_sql_for_multiple_metrics,
    get_engine,
    get_node_by_name,
    get_query,
    validate_orderby,
)
from datajunction_server.errors import DJException, DJInvalidInputException
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
from datajunction_server.utils import get_query_service_client, get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/data/{node_name}/availability/")
def add_an_availability_state(
    node_name: str,
    data: AvailabilityStateBase,
    *,
    session: Session = Depends(get_session),
) -> JSONResponse:
    """
    Add an availability state to a node
    """
    node = get_node_by_name(session, node_name)

    # Source nodes require that any availability states set are for one of the defined tables
    node_revision = node.current
    if node.current.type == NodeType.SOURCE:
        if (
            data.catalog != node_revision.catalog.name
            or node_revision.schema_ != data.schema_
            or node_revision.table != data.table
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
    session.commit()
    return JSONResponse(
        status_code=200,
        content={"message": "Availability state successfully posted"},
    )


@router.get("/data/{node_name}/")
def get_data(  # pylint: disable=too-many-locals
    node_name: str,
    *,
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    async_: bool = False,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
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
    query_ast = get_query(
        session=session,
        node_name=node_name,
        dimensions=dimensions,
        filters=filters,
        orderby=orderby,
        limit=limit,
        engine=engine,
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


@router.get("/data/", response_model=QueryWithResults)
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
