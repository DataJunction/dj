"""
Cube related APIs.
"""
import logging
from typing import List, Optional

from fastapi import Depends, Query
from fastapi.responses import JSONResponse
from sqlmodel import Session

from datajunction_server.api.helpers import get_catalog_by_name, get_node_by_name
from datajunction_server.construction.dimensions import build_dimensions_from_cube_query
from datajunction_server.errors import DJAlreadyExistsException, DJInvalidInputException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access
from datajunction_server.models.cube import (
    CubeRevisionMetadata,
    DimensionValue,
    DimensionValues,
)
from datajunction_server.models.filterset import Filterset, FiltersetBase
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node import NodeType
from datajunction_server.models.query import QueryCreate
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import (
    SEPARATOR,
    from_amenable_name,
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


@router.get("/cubes/{name}/dimensions/sql", name="Dimensions SQL for Cube")
def get_cube_dimension_sql(
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
    session: Session = Depends(get_session),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=redefined-outer-name
        validate_access,
    ),
) -> TranslatedSQL:
    """
    Generates SQL to retrieve all unique values of a dimension for the cube
    """
    node = get_node_by_name(session=session, name=name, node_type=NodeType.CUBE)
    cube = node.current
    return build_dimensions_from_cube_query(
        session,
        cube,
        dimensions,
        filters,
        limit,
        include_counts,
        validate_access=validate_access,
    )


@router.get(
    "/cubes/{name}/dimensions/data",
    name="Dimensions Values for Cube",
)
def get_cube_dimension_values(  # pylint: disable=too-many-locals
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
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=redefined-outer-name
        validate_access,
    ),
) -> DimensionValues:
    """
    All unique values of a dimension from the cube
    """
    node = get_node_by_name(session=session, name=name, node_type=NodeType.CUBE)
    cube = node.current
    translated_sql = build_dimensions_from_cube_query(
        session,
        cube,
        dimensions,
        filters,
        limit,
        include_counts,
        validate_access,
    )
    if cube.availability:
        catalog = get_catalog_by_name(  # pragma: no cover
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
    result = query_service_client.submit_query(query_create)
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


@router.post(
    "/cubes/{name}/filtersets",
    name="Add filterset to cube",
)
def add_filterset_to_cube(  # pylint: disable=too-many-locals
    name: str,
    *,
    add_filterset: FiltersetBase,
    session: Session = Depends(get_session),
):
    node = get_node_by_name(session=session, name=name, node_type=NodeType.CUBE)
    cube = node.current
    if add_filterset.name in {filterset.name for filterset in cube.filtersets}:
        raise DJAlreadyExistsException(
            f"A filterset with name {add_filterset.name} already exists for `{name}`",
        )

    # Check if all elements referenced by the filterset exist on the cube
    filters_ast = parse(f"SELECT * WHERE {add_filterset.filters}")
    cube_elements = {element for element in cube.cube_dimensions()}
    cube_elements.update({element.split(SEPARATOR)[-1] for element in cube_elements})
    referenced_columns = {
        col.alias_or_name.identifier()
        for col in filters_ast.select.where.find_all(ast.Column)
    }
    if referenced_columns - cube_elements:
        raise DJInvalidInputException(
            "Not all elements referenced by the filterset SQL "
            f"(`{add_filterset.filters}`) are available on the cube. Unavailable elements: {referenced_columns}",
        )

    # Add the filterset
    filterset = Filterset.from_orm(add_filterset)
    filterset.node_revision = cube
    session.add(filterset)
    session.commit()
    return JSONResponse(
        status_code=200,
        content={
            "message": f"Filterset `{filterset.name}` successfully added to `{name}`",
        },
    )
