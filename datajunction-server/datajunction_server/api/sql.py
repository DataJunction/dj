# pylint: disable=too-many-arguments
"""
SQL related APIs.
"""
import logging
from typing import List, Optional

from fastapi import Depends, Query
from sqlalchemy.orm import Session

from datajunction_server.api.helpers import (
    assemble_column_metadata,
    build_sql_for_multiple_metrics,
    get_query,
    validate_orderby,
)
from datajunction_server.construction.build import get_measures_query
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
def get_measures_sql_for_cube(
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    *,
    session: Session = Depends(get_session),
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
    return get_measures_query(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        engine_name=engine_name,
        engine_version=engine_version,
        current_user=current_user,
        validate_access=validate_access,
    )


@router.get(
    "/sql/{node_name}/",
    response_model=TranslatedSQL,
    name="Get SQL For A Node",
)
def get_sql(
    node_name: str,
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    *,
    session: Session = Depends(get_session),
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
    access_control = access.AccessControlStore(
        validate_access=validate_access,
        user=UserOutput.from_orm(current_user) if current_user else None,
        base_verb=access.ResourceRequestVerb.READ,
    )

    engine = (
        get_engine(session, engine_name, engine_version)  # type: ignore
        if engine_name
        else None
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
        access_control=access_control,
    )
    columns = [
        assemble_column_metadata(col)  # type: ignore
        for col in query_ast.select.projection
    ]
    return TranslatedSQL(
        sql=str(query_ast),
        columns=columns,
        dialect=engine.dialect if engine else None,
    )


@router.get("/sql/", response_model=TranslatedSQL, name="Get SQL For Metrics")
def get_sql_for_metrics(
    metrics: List[str] = Query([]),
    dimensions: List[str] = Query([]),
    filters: List[str] = Query([]),
    orderby: List[str] = Query([]),
    limit: Optional[int] = None,
    *,
    session: Session = Depends(get_session),
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

    translated_sql, _, _ = build_sql_for_multiple_metrics(
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
    return translated_sql
