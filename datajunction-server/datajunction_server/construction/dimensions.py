"""
Dimensions-related query building
"""
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import get_catalog_by_name
from datajunction_server.construction.build_v2 import get_measures_query
from datajunction_server.database.node import NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models import access
from datajunction_server.models.column import SemanticType
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.naming import amenable_name, from_amenable_name
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.types import IntegerType
from datajunction_server.utils import SEPARATOR


async def build_dimensions_from_cube_query(  # pylint: disable=too-many-arguments,too-many-locals
    session: AsyncSession,
    cube: NodeRevision,
    dimensions: List[str],
    current_user: User,
    validate_access: access.ValidateAccessFn,
    filters: Optional[str] = None,
    limit: Optional[int] = 50000,
    include_counts: bool = False,
) -> TranslatedSQL:
    """
    Builds a query for retrieving unique values of a dimension for the given cube.
    The filters provided here are additional filters layered on top of any existing cube filters.
    Setting `include_counts` to true will also provide associated counts for each dimension value.
    """
    unavailable_dimensions = set(dimensions) - set(cube.cube_dimensions())
    if unavailable_dimensions:
        raise DJInvalidInputException(
            f"The following dimensions {unavailable_dimensions} are not "
            f"available in the cube {cube.name}.",
        )
    query_ast: ast.Query = ast.Query(
        select=ast.Select(from_=ast.From(relations=[])),
        ctes=[],
    )
    for dimension in dimensions or cube.cube_dimensions():
        dimension_column = ast.Column(
            name=ast.Name(amenable_name(dimension)),
        )
        query_ast.select.projection.append(dimension_column)
        query_ast.select.group_by.append(dimension_column)
    if include_counts:
        query_ast.select.projection.append(
            ast.Function(
                name=ast.Name("COUNT"),
                args=[ast.Column(name=ast.Name("*"))],
            ),
        )
        query_ast.select.organization = ast.Organization(
            order=[
                ast.SortItem(
                    expr=ast.Column(
                        name=ast.Name(str(len(query_ast.select.projection))),
                    ),
                    asc="DESC",
                    nulls="",
                ),
            ],
        )
        query_ast.select.where = None

    if limit is not None:
        query_ast.select.limit = ast.Number(limit)

    # Build the FROM clause. The source table on the FROM clause depends on whether
    # the cube is available as a materialized datasource or if it needs to be built up
    # from the measures query.
    if cube.availability:
        catalog = await get_catalog_by_name(session, cube.availability.catalog)  # type: ignore
        query_ast.select.from_.relations.append(  # type: ignore
            ast.Relation(primary=ast.Table(ast.Name(cube.availability.table))),  # type: ignore
        )
        if filters:
            temp_filters_select = parse(f"select * where {filters}")
            for col in temp_filters_select.find_all(ast.Column):
                if (  # pragma: no cover
                    col.alias_or_name.identifier() in cube.cube_dimensions()
                ):
                    col.name = ast.Name(
                        name=amenable_name(col.alias_or_name.identifier()),
                    )
            query_ast.select.where = temp_filters_select.select.where
    else:
        catalog = cube.catalog
        measures_query = await get_measures_query(
            session=session,
            metrics=[metric.name for metric in cube.cube_metrics()],
            dimensions=dimensions,
            filters=[filters] if filters else [],
            current_user=current_user,
            validate_access=validate_access,
        )
        measures_query_ast = parse(measures_query[0].sql)
        measures_query_ast.bake_ctes()
        measures_query_ast.parenthesized = True
        query_ast.select.from_.relations.append(  # type: ignore
            ast.Relation(primary=measures_query_ast),
        )
    types_lookup = {
        amenable_name(elem.node_revision().name + SEPARATOR + elem.name): elem.type  # type: ignore
        for elem in cube.cube_elements
    }
    return TranslatedSQL(
        sql=str(query_ast),
        columns=[
            ColumnMetadata(
                name=col.name.name,  # type: ignore
                type=str(types_lookup.get(col.name.name)),  # type: ignore
                semantic_entity=from_amenable_name(col.name.name),  # type: ignore
                semantic_type=SemanticType.DIMENSION,
            )
            if col.name.name in types_lookup  # type: ignore
            else ColumnMetadata(name="count", type=str(IntegerType()))
            for col in query_ast.select.projection
        ],
        dialect=catalog.engines[0].dialect if catalog else None,
    )
