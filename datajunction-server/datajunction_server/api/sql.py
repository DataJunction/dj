"""
SQL related APIs.
"""

import json
import logging
from collections import OrderedDict
from http import HTTPStatus
from typing import Any, List, Optional, Tuple, cast

from fastapi import BackgroundTasks, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.api.helpers import (
    assemble_column_metadata,
    build_sql_for_multiple_metrics,
    get_query,
    validate_orderby,
)
from datajunction_server.database import Engine, Node
from datajunction_server.database.queryrequest import QueryBuildType, QueryRequest
from datajunction_server.database.user import User
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.internal.engines import get_engine
from datajunction_server.models import access
from datajunction_server.models.access import AccessControlStore
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.sql import GeneratedSQL
from datajunction_server.models.user import UserOutput
from datajunction_server.utils import (
    Settings,
    get_and_update_current_user,
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
    settings: Settings = Depends(get_settings),
    session: AsyncSession = Depends(get_session),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: Optional[User] = Depends(get_and_update_current_user),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
    use_materialized: bool = True,
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
    from datajunction_server.construction.build_v2 import (
        get_measures_query,
    )

    metrics = list(OrderedDict.fromkeys(metrics))
    return await get_measures_query(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        orderby=orderby,
        engine_name=engine_name,
        engine_version=engine_version,
        current_user=current_user,
        validate_access=validate_access,
        include_all_columns=include_all_columns,
        use_materialized=use_materialized,
        preagg_requested=preaggregate,
        query_parameters=json.loads(query_params),
    )


async def build_and_save_node_sql(
    node_name: str,
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    *,
    session: AsyncSession = Depends(get_session),
    engine: Engine,
    access_control: AccessControlStore,
    ignore_errors: bool = True,
    use_materialized: bool = True,
    query_parameters: dict[str, Any] | None = None,
    save: bool = False,
) -> QueryRequest:
    """
    Build node SQL and save it to query requests
    """
    node = cast(
        Node,
        await Node.get_by_name(session, node_name, raise_if_not_exists=True),
    )

    # If it's a cube, we'll build SQL for the metrics in the cube, along with any additional
    # dimensions or filters provided in the arguments
    if node.type == NodeType.CUBE:
        node = cast(
            Node,
            await Node.get_cube_by_name(session, node_name),
        )
        dimensions = list(
            OrderedDict.fromkeys(node.current.cube_node_dimensions + dimensions),
        )
        translated_sql, engine, _ = await build_sql_for_multiple_metrics(
            session=session,
            metrics=node.current.cube_node_metrics,
            dimensions=dimensions,
            filters=filters,
            orderby=orderby,
            limit=limit,
            engine_name=engine.name if engine else None,
            engine_version=engine.version if engine else None,
            access_control=access_control,
            use_materialized=use_materialized,
            query_parameters=query_parameters,
        )
        # We save the request for both the cube and the metrics, so that if someone makes either
        # of these types of requests, they'll go to the cached query
        requests_to_save = (
            [
                (node.current.cube_node_metrics, QueryBuildType.METRICS),
                ([node_name], QueryBuildType.NODE),
            ]
            if save
            else []
        )
        for nodes, query_type in requests_to_save:
            if query_parameters:
                continue  # pragma: no cover
            request = await QueryRequest.save_query_request(
                session=session,
                nodes=nodes,
                dimensions=dimensions,
                filters=filters,
                orderby=orderby,
                limit=limit,
                engine_name=engine.name if engine else None,
                engine_version=engine.version if engine else None,
                query_type=query_type,
                query=translated_sql.sql,
                columns=[col.dict() for col in translated_sql.columns],  # type: ignore
            )
        return request

    # For all other nodes, build the node query
    node = await Node.get_by_name(session, node_name, raise_if_not_exists=True)  # type: ignore
    if node.type == NodeType.METRIC:
        translated_sql, engine, _ = await build_sql_for_multiple_metrics(
            session,
            [node_name],
            dimensions,
            filters,
            orderby,
            limit,
            engine.name if engine else None,
            engine.version if engine else None,
            access_control=access_control,
            ignore_errors=ignore_errors,
            use_materialized=use_materialized,
            query_parameters=query_parameters,
        )
        query = translated_sql.sql
        columns = translated_sql.columns
    else:
        query_ast = await get_query(
            session=session,
            node_name=node_name,
            dimensions=dimensions,
            filters=filters,
            orderby=orderby,
            limit=limit,
            engine=engine,
            access_control=access_control,
            use_materialized=use_materialized,
            query_parameters=query_parameters,
            ignore_errors=ignore_errors,
        )
        columns = [
            assemble_column_metadata(col, use_semantic_metadata=True)  # type: ignore
            for col in query_ast.select.projection
        ]
        query = str(query_ast)

    query_request = await QueryRequest.save_query_request(
        session=session,
        nodes=[node_name],
        dimensions=dimensions,
        filters=filters,
        orderby=orderby,
        limit=limit,
        engine_name=engine.name if engine else None,
        engine_version=engine.version if engine else None,
        query_type=QueryBuildType.NODE,
        query=query,
        columns=[col.dict() for col in columns or []],
        save=False,
    )
    return query_request


async def get_node_sql(
    node_name: str,
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    *,
    session: AsyncSession = Depends(get_session),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: User,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks,
    ignore_errors: bool = True,
    use_materialized: bool = True,
    query_parameters: dict[str, Any] | None = None,
    cache: Cache = None,
) -> Tuple[TranslatedSQL, QueryRequest]:
    """
    Return SQL for a node.
    """
    dimensions = [dim for dim in dimensions if dim and dim != ""]
    access_control = access.AccessControlStore(
        validate_access=validate_access,
        user=UserOutput.from_orm(current_user),
        base_verb=access.ResourceRequestVerb.READ,
    )

    engine = (
        await get_engine(session, engine_name, engine_version)  # type: ignore
        if engine_name
        else None
    )
    validate_orderby(orderby, [node_name], dimensions)

    if query_request := await QueryRequest.get_query_request(
        session,
        nodes=[node_name],
        dimensions=dimensions,
        filters=filters,
        orderby=orderby,
        limit=limit,
        engine_name=engine.name if engine else None,
        engine_version=engine.version if engine else None,
        query_type=QueryBuildType.NODE,
    ):
        # Update the node SQL in a background task to keep it up-to-date
        background_tasks.add_task(
            build_and_save_node_sql,
            node_name=node_name,
            dimensions=dimensions,
            filters=filters,
            orderby=orderby,
            limit=limit,
            session=session,
            engine=engine,
            access_control=access_control,
            use_materialized=use_materialized,
            query_parameters=query_parameters,
            save=False,
        )
        return (
            TranslatedSQL.create(
                sql=query_request.query,
                columns=query_request.columns,
                dialect=engine.dialect if engine else None,
            ),
            query_request,
        )

    query_request = await build_and_save_node_sql(
        node_name=node_name,
        dimensions=dimensions,
        filters=filters,
        orderby=orderby,
        limit=limit,
        session=session,
        engine=engine,  # type: ignore
        access_control=access_control,
        ignore_errors=ignore_errors,
        use_materialized=use_materialized,
        query_parameters=query_parameters,
        save=False,
    )
    return (
        TranslatedSQL.create(
            sql=query_request.query,
            columns=query_request.columns,
            dialect=engine.dialect if engine else None,
        ),
        query_request,
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
) -> TranslatedSQL:
    """
    Return SQL for a node.
    """
    translated_sql, _ = await get_node_sql(
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
        ignore_errors=ignore_errors,  # type: ignore
        use_materialized=use_materialized,  # type: ignore
        query_parameters=json.loads(query_params),
        cache=cache,
    )
    return translated_sql


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
) -> TranslatedSQL:
    """
    Return SQL for a set of metrics with dimensions and filters
    """

    access_control = access.AccessControlStore(
        validate_access=validate_access,
        user=current_user,
        base_verb=access.ResourceRequestVerb.READ,
    )

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

    if (
        query_request := await QueryRequest.get_query_request(
            session,
            nodes=metrics,
            dimensions=dimensions,
            filters=filters,
            orderby=orderby,
            limit=limit,
            engine_name=engine_name,
            engine_version=engine_version,
            query_type=QueryBuildType.METRICS,
        )
    ) and not query_params:
        # Update the node SQL in a background task to keep it up-to-date
        background_tasks.add_task(  # pragma: no cover
            build_and_save_sql_for_metrics,
            session=session,
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            orderby=orderby,
            limit=limit,
            engine_name=engine_name,
            engine_version=engine_version,
            access_control=access_control,
            ignore_errors=ignore_errors,
            use_materialized=use_materialized,
            query_parameters=json.loads(query_params),
            save=False,
        )
        engine = (  # pragma: no cover
            await get_engine(session, engine_name, engine_version)  # type: ignore
            if engine_name
            else None
        )
        return TranslatedSQL.create(  # pragma: no cover
            sql=query_request.query,
            columns=query_request.columns,
            dialect=engine.dialect if engine else None,
        )

    return await build_and_save_sql_for_metrics(
        session,
        metrics,
        dimensions,
        filters,
        orderby,
        limit,
        engine_name,
        engine_version,
        access_control,
        ignore_errors=ignore_errors,  # type: ignore
        use_materialized=use_materialized,  # type: ignore
        query_parameters=json.loads(query_params),
        save=False,
    )


async def build_and_save_sql_for_metrics(
    session: AsyncSession,
    metrics: List[str],
    dimensions: List[str],
    filters: List[str] = None,
    orderby: List[str] = None,
    limit: Optional[int] = None,
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    access_control: Optional[access.AccessControlStore] = None,
    ignore_errors: bool = True,
    use_materialized: bool = True,
    query_parameters: dict[str, Any] | None = None,
    save: bool = True,
):
    """
    Builds and saves SQL for metrics.
    """
    translated_sql, _, _ = await build_sql_for_multiple_metrics(
        session,
        metrics,
        dimensions,
        filters,
        orderby,
        limit,
        engine_name,
        engine_version,
        access_control,
        ignore_errors=ignore_errors,  # type: ignore
        use_materialized=use_materialized,  # type: ignore
        query_parameters=query_parameters,
    )
    if save:
        await QueryRequest.save_query_request(
            session=session,
            nodes=metrics,
            dimensions=dimensions,
            filters=filters,  # type: ignore
            orderby=orderby,  # type: ignore
            limit=limit,
            engine_name=engine_name,
            engine_version=engine_version,
            query_type=QueryBuildType.METRICS,
            query=translated_sql.sql,
            columns=[col.dict() for col in translated_sql.columns],  # type: ignore
        )
    return translated_sql
