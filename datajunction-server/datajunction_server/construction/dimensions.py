from sqlmodel import Session

from datajunction_server.api.helpers import get_catalog_by_name
from datajunction_server.construction.build import get_measures_query
from datajunction_server.models import NodeRevision, access
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import amenable_name, from_amenable_name, SEPARATOR


def build_dimensions_from_cube_query(
    session: Session,
    cube: NodeRevision,
    dimension: str,
    include_counts: bool = False,
    validate_access: access.ValidateAccessFn = None,
) -> TranslatedSQL:
    """
    Builds a query for retrieving unique values of a dimension for the given cube.
    """
    query_ast: ast.Query = ast.Query(
        select=ast.Select(from_=ast.From(relations=[])),
        ctes=[],
    )
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
                    expr=ast.Column(name=ast.Name("2")),
                    asc="DESC",
                    nulls="",
                ),
            ],
        )
    if cube.availability and dimension in cube.cube_dimensions():
        query_ast.select.from_.relations.append(  # type: ignore
            ast.Relation(primary=ast.Table(ast.Name(cube.availability.table))),  # type: ignore
        )
        catalog = get_catalog_by_name(session, cube.availability.catalog)  # type: ignore
    else:
        catalog = cube.catalog
        measures_query = get_measures_query(
            session=session,
            metrics=[metric.name for metric in cube.cube_metrics()],
            dimensions=[dimension],
            filters=[],
            validate_access=validate_access,
        )
        measures_query_ast = parse(measures_query.sql)
        measures_query_ast.bake_ctes()
        measures_query_ast.parenthesized = True
        query_ast.select.from_.relations.append(  # type: ignore
            ast.Relation(primary=measures_query_ast),
        )
    dimension_type = [
        elem.type for elem in cube.cube_elements if (elem.node_revision().name + SEPARATOR + elem.name) == dimension
    ][0]
    return TranslatedSQL(
        sql=str(query_ast),
        columns=[
            ColumnMetadata(
                name=dimension_column.name.name,
                type=str(dimension_type),
                semantic_entity=from_amenable_name(dimension_column.name.name),
                semantic_type="dimension",
            ),
        ],
        dialect=catalog.engines[0].dialect,
    )
