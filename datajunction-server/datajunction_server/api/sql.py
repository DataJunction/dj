# pylint: disable=too-many-arguments
"""
SQL related APIs.
"""
import logging
from typing import List, Optional

from fastapi import Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import (
    assemble_column_metadata,
    build_sql_for_multiple_metrics,
    get_query,
    validate_orderby,
)
from datajunction_server.construction.build import get_measures_query
from datajunction_server.database.queryrequest import QueryBuildType, QueryRequest
from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.internal.engines import get_engine
from datajunction_server.models import access
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.user import UserOutput
from datajunction_server.utils import get_current_user, get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["sql"])


@router.get("/sql/measures/", response_model=TranslatedSQL, name="Get Measures SQL")
async def get_measures_sql_for_cube(
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    *,
    include_all_columns: bool = Query(
        False,
        description=(
            "Whether to include all columns or only those necessary "
            "for the metrics and dimensions in the cube"
        ),
    ),
    session: AsyncSession = Depends(get_session),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> TranslatedSQL:
    """
    Return the measures SQL for a set of metrics with dimensions and filters.
    This SQL can be used to produce an intermediate table with all the measures
    and dimensions needed for an analytics database (e.g., Druid).
    """
    if query_request := await QueryRequest.get_query_request(
        session,
        nodes=metrics,
        dimensions=dimensions,
        filters=filters,
        orderby=[],
        limit=None,
        engine_name=engine_name,
        engine_version=engine_version,
        query_type=QueryBuildType.MEASURES,
        other_args={"include_all_columns": include_all_columns},
    ):
        engine = (
            await get_engine(session, engine_name, engine_version)  # type: ignore
            if engine_name
            else None
        )
        return TranslatedSQL(
            sql=query_request.query,
            columns=query_request.columns,
            dialect=engine.dialect if engine else None,
        )

    measures_query = await get_measures_query(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        engine_name=engine_name,
        engine_version=engine_version,
        current_user=current_user,
        validate_access=validate_access,
        include_all_columns=include_all_columns,
    )

    await QueryRequest.save_query_request(
        session=session,
        nodes=metrics,
        dimensions=dimensions,
        filters=filters,
        orderby=[],
        limit=None,
        engine_name=engine_name,
        engine_version=engine_version,
        query_type=QueryBuildType.MEASURES,
        query=measures_query.sql,
        columns=[col.dict() for col in measures_query.columns],  # type: ignore
        other_args={"include_all_columns": include_all_columns},
    )
    return measures_query


@router.get(
    "/sql/{node_name}/",
    response_model=TranslatedSQL,
    name="Get SQL For A Node",
)
async def get_sql(  # pylint: disable=too-many-locals
    node_name: str,
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    *,
    session: AsyncSession = Depends(get_session),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> TranslatedSQL:
    """
    Return SQL for a node.
    """
    dimensions = [dim for dim in dimensions if dim and dim != ""]
    access_control = access.AccessControlStore(
        validate_access=validate_access,
        user=UserOutput.from_orm(current_user) if current_user else None,
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
        return TranslatedSQL(
            sql=query_request.query,
            columns=query_request.columns,
            dialect=engine.dialect if engine else None,
        )

    query_ast = await get_query(
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
        assemble_column_metadata(col)  # type: ignore
        for col in query_ast.select.projection
    ]
    query = str(query_ast)
    await QueryRequest.save_query_request(
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
        columns=[col.dict() for col in columns],
    )
    return TranslatedSQL(
        sql=query,
        columns=columns,
        dialect=engine.dialect if engine else None,
    )


@router.get("/sql/", response_model=TranslatedSQL, name="Get SQL For Metrics")
async def get_sql_for_metrics(
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    *,
    session: AsyncSession = Depends(get_session),
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> TranslatedSQL:
    """
    Return SQL for a set of metrics with dimensions and filters
    """

    access_control = access.AccessControlStore(
        validate_access=validate_access,
        user=current_user,
        base_verb=access.ResourceRequestVerb.READ,
    )

    if query_request := await QueryRequest.get_query_request(
        session,
        nodes=metrics,
        dimensions=dimensions,
        filters=filters,
        orderby=orderby,
        limit=limit,
        engine_name=engine_name,
        engine_version=engine_version,
        query_type=QueryBuildType.METRICS,
    ):
        engine = (
            await get_engine(session, engine_name, engine_version)  # type: ignore
            if engine_name
            else None
        )
        return TranslatedSQL(
            sql=query_request.query,
            columns=query_request.columns,
            dialect=engine.dialect if engine else None,
        )

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
    )

    await QueryRequest.save_query_request(
        session=session,
        nodes=metrics,
        dimensions=dimensions,
        filters=filters,
        orderby=orderby,
        limit=limit,
        engine_name=engine_name,
        engine_version=engine_version,
        query_type=QueryBuildType.METRICS,
        query=translated_sql.sql,
        columns=[col.dict() for col in translated_sql.columns],  # type: ignore
    )
    return translated_sql
