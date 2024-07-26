# pylint: disable=too-many-arguments
"""
Cube related APIs.
"""
import logging
from typing import List, Optional

from fastapi import Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import get_catalog_by_name
from datajunction_server.construction.dimensions import build_dimensions_from_cube_query
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.internal.nodes import get_cube_revision_metadata
from datajunction_server.models import access
from datajunction_server.models.cube import (
    CubeRevisionMetadata,
    DimensionValue,
    DimensionValues,
)
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.query import QueryCreate
from datajunction_server.naming import from_amenable_name
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.utils import (
    get_and_update_current_user,
    get_query_service_client,
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["cubes"])


@router.get("/cubes/{name}/", name="Get a Cube")
async def get_cube(
    name: str, *, session: AsyncSession = Depends(get_session)
) -> CubeRevisionMetadata:
    """
    Get information on a cube
    """
    return await get_cube_revision_metadata(session, name)


@router.get("/cubes/{name}/dimensions/sql", name="Dimensions SQL for Cube")
async def get_cube_dimension_sql(
    name: str,
    *,
    dimensions: List[str] = Query([], description="Dimensions to get values for"),
    filters: Optional[str] = Query(
        None,
        description="Filters on dimensional attributes",
    ),
    limit: Optional[int] = Query(
        None,
        description="Number of rows to limit the data retrieved to",
    ),
    include_counts: bool = False,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=redefined-outer-name
        validate_access,
    ),
) -> TranslatedSQL:
    """
    Generates SQL to retrieve all unique values of a dimension for the cube
    """
    node = await Node.get_cube_by_name(session, name)
    node_revision = node.current  # type: ignore
    return await build_dimensions_from_cube_query(
        session,
        node_revision,
        dimensions,
        current_user,
        validate_access,
        filters,
        limit,
        include_counts,
    )


@router.get(
    "/cubes/{name}/dimensions/data",
    name="Dimensions Values for Cube",
)
async def get_cube_dimension_values(  # pylint: disable=too-many-locals
    name: str,
    *,
    dimensions: List[str] = Query([], description="Dimensions to get values for"),
    filters: Optional[str] = Query(
        None,
        description="Filters on dimensional attributes",
    ),
    limit: Optional[int] = Query(
        None,
        description="Number of rows to limit the data retrieved to",
    ),
    include_counts: bool = False,
    async_: bool = False,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: User = Depends(get_and_update_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=redefined-outer-name
        validate_access,
    ),
) -> DimensionValues:
    """
    All unique values of a dimension from the cube
    """
    request_headers = dict(request.headers)
    node = await Node.get_cube_by_name(session, name)
    cube = node.current  # type: ignore
    translated_sql = await build_dimensions_from_cube_query(
        session,
        cube,
        dimensions,
        current_user,
        validate_access,
        filters,
        limit,
        include_counts,
    )
    if cube.availability:
        catalog = await get_catalog_by_name(  # pragma: no cover
            session,
            cube.availability.catalog,  # type: ignore
        )
    else:
        catalog = cube.catalog
    query_create = QueryCreate(
        engine_name=catalog.engines[0].name,
        catalog_name=catalog.name,
        engine_version=catalog.engines[0].version,
        submitted_query=translated_sql.sql,
        async_=async_,
    )
    result = query_service_client.submit_query(
        query_create,
        request_headers=request_headers,
    )
    count_column = [
        idx
        for idx, col in enumerate(translated_sql.columns)  # type: ignore
        if col.name == "count"
    ]
    dimension_values = [  # pragma: no cover
        DimensionValue(
            value=row[0 : count_column[0]] if count_column else row,
            count=row[count_column[0]] if count_column else None,
        )
        for row in result.results.__root__[0].rows
    ]
    return DimensionValues(  # pragma: no cover
        dimensions=[
            from_amenable_name(col.name)
            for col in translated_sql.columns  # type: ignore # pylint: disable=not-an-iterable
            if col.name != "count"
        ],
        values=dimension_values,
        cardinality=len(dimension_values),
    )
