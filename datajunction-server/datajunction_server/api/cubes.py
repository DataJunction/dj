"""
Cube related APIs.
"""
import logging

from fastapi import Depends
from sqlmodel import Session

from datajunction_server.api.helpers import get_catalog_by_name, get_node_by_name
from datajunction_server.construction.dimensions import build_dimensions_from_cube_query
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access
from datajunction_server.models.cube import (
    CubeRevisionMetadata,
    DimensionValue,
    DimensionValues,
)
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node import NodeType
from datajunction_server.models.query import QueryCreate
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.utils import (
    get_query_service_client,
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["cubes"])


@router.get("/cubes/{name}/", response_model=CubeRevisionMetadata, name="Get a Cube")
def get_cube(
    name: str, *, session: Session = Depends(get_session)
) -> CubeRevisionMetadata:
    """
    Get information on a cube
    """
    node = get_node_by_name(session=session, name=name, node_type=NodeType.CUBE)
    return node.current


@router.get("/cubes/{name}/dimensions/{dimension}/sql", name="Dimensions SQL for Cube")
def get_cube_dimension_sql(
    name: str,
    dimension: str,
    *,
    include_counts: bool = False,
    session: Session = Depends(get_session),
    validate_access: access.ValidateAccessFn = Depends(validate_access),
) -> TranslatedSQL:
    """
    Generates SQL to retrieve all unique values of a dimension for the cube
    """
    node = get_node_by_name(session=session, name=name, node_type=NodeType.CUBE)
    cube = node.current
    return build_dimensions_from_cube_query(
        session,
        cube,
        dimension,
        include_counts,
        validate_access=validate_access,
    )


@router.get(
    "/cubes/{name}/dimensions/{dimension}/values",
    name="Dimension Values From Cube",
)
def get_cube_dimension_values(
    name: str,
    dimension: str,
    *,
    include_counts: bool = False,
    async_: bool = False,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    validate_access: access.ValidateAccessFn = Depends(validate_access),
) -> DimensionValues:
    """
    All unique values of a dimension from the cube
    """
    node = get_node_by_name(session=session, name=name, node_type=NodeType.CUBE)
    cube = node.current
    translated_sql = build_dimensions_from_cube_query(
        session,
        cube,
        dimension,
        include_counts,
        validate_access,
    )
    if cube.availability:
        catalog = get_catalog_by_name(session, cube.availability.catalog)  # type: ignore
    else:
        catalog = cube.catalog
    query_create = QueryCreate(
        engine_name=catalog.engines[0].name,
        catalog_name=catalog.name,
        engine_version=catalog.engines[0].version,
        submitted_query=translated_sql.sql,
        async_=async_,
    )
    result = query_service_client.submit_query(query_create)
    dimension_values = [
        DimensionValue(value=row[0], count=row[1] if len(row) > 1 else None)
        for row in result.results.__root__[0].rows
    ]
    return DimensionValues(
        values=dimension_values,
        cardinality=len(dimension_values),
    )
