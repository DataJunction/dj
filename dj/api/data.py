"""
Data related APIs.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlmodel import Session

from dj.api.helpers import get_engine, get_node_by_name, get_query, validate_cube
from dj.construction.build import build_metric_nodes
from dj.errors import DJException, DJInvalidInputException
from dj.models.metric import TranslatedSQL
from dj.models.node import AvailabilityState, AvailabilityStateBase, NodeType
from dj.models.query import ColumnMetadata, QueryCreate, QueryWithResults
from dj.service_clients import QueryServiceClient
from dj.utils import get_query_service_client, get_session

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
    if data.catalog != node_revision.catalog.name:
        raise DJException(
            "Cannot set availability state in different catalog: "
            f"{data.catalog}, {node_revision.catalog}",
        )
    if node.current.type == NodeType.SOURCE:
        if node_revision.schema_ != data.schema_ or node_revision.table != data.table:
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
        # Currently, we do not consider type information. We should eventually check the type of
        # the partition values in order to cast them before sorting.
        data.max_partition = max(
            (
                node_revision.availability.max_partition,
                data.max_partition,
            ),
        )
        data.min_partition = min(
            (
                node_revision.availability.min_partition,
                data.min_partition,
            ),
        )

    db_new_availability = AvailabilityState.from_orm(data)
    node_revision.availability = db_new_availability
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

    query_ast = get_query(
        session=session,
        node_name=node_name,
        dimensions=dimensions,
        filters=filters,
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
def get_data_for_metrics(  # pylint: disable=R0914
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
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
    leading_metric_node = get_node_by_name(session, metrics[0])
    available_engines = leading_metric_node.current.catalog.engines
    engine = (
        get_engine(session, engine_name, engine_version)  # type: ignore
        if engine_name
        else available_engines[0]
    )
    if engine not in available_engines:
        raise DJInvalidInputException(  # pragma: no cover
            f"The selected engine is not available for the node {metrics[0]}. "
            f"Available engines include: {', '.join(engine.name for engine in available_engines)}",
        )

    _, metric_nodes, _, _ = validate_cube(
        session,
        metrics,
        dimensions,
    )
    query_ast = build_metric_nodes(
        session,
        metric_nodes,
        filters=filters or [],
        dimensions=dimensions or [],
    )
    columns = [
        ColumnMetadata(name=col.alias_or_name.name, type=str(col.type))  # type: ignore
        for col in query_ast.select.projection
    ]
    query = TranslatedSQL(
        sql=str(query_ast),
        columns=columns,
        dialect=engine.dialect if engine else None,
    )

    query_create = QueryCreate(
        engine_name=engine.name,
        catalog_name=leading_metric_node.current.catalog.name,
        engine_version=engine.version,
        submitted_query=query.sql,
        async_=async_,
    )
    result = query_service_client.submit_query(query_create)
    # Inject column info if there are results
    if result.results.__root__:  # pragma: no cover
        result.results.__root__[0].columns = columns
    return result
