"""Functions to add to an ast DJ node queries"""
# pylint: disable=too-many-arguments,too-many-locals,too-many-nested-blocks,too-many-branches,R0401
from typing import Dict, List, Optional, Union, cast

from sqlmodel import Session

from dj.construction.utils import amenable_name
from dj.errors import DJException
from dj.models.column import Column
from dj.models.node import BuildCriteria, NodeRevision, NodeType
from dj.sql.parsing import ast, parse


def _get_tables_from_select(select: ast.Select) -> Dict[NodeRevision, List[ast.Table]]:
    """
    Extract all tables (source, transform, dimensions)
    """
    tables: Dict[NodeRevision, List[ast.Table]] = {}

    for table in select.find_all(ast.Table):
        if node := table.dj_node:  # pragma: no cover
            tables[node] = tables.get(node, [])
            tables[node].append(table)
    return tables


def _get_dim_cols_from_select(
    select: ast.Select,
) -> Dict[NodeRevision, List[ast.Column]]:
    """
    Extract all dimension nodes referenced as columns
    """
    dimension_columns: Dict[NodeRevision, List[ast.Column]] = {}

    for col in select.find_all(ast.Column):
        if isinstance(col.table, ast.Table):
            if node := col.table.dj_node:  # pragma: no cover
                if node.type == NodeType.DIMENSION:
                    dimension_columns[node] = dimension_columns.get(node, [])
                    dimension_columns[node].append(col)
    return dimension_columns


def _build_dimensions_on_select(
    session: Session,
    dimension_columns: Dict[NodeRevision, List[ast.Column]],
    dialect: Optional[str] = None,
    build_criteria: Optional[BuildCriteria] = None,
):
    """
    Add all filter and agg dimensions to a select
    """

    for dim_node, dim_cols in dimension_columns.items():
        selects_map = {}
        for dim_col in dim_cols:
            select = cast(ast.Select, dim_col.get_nearest_parent_of_type(ast.Select))
            selects_map[id(select)] = select
        for select in selects_map.values():
            tables = _get_tables_from_select(select)
            if dim_node not in tables:  # need to join dimension
                join_info: Dict[NodeRevision, List[Column]] = {}
                for table_node in tables:
                    join_dim_cols = []
                    for col in table_node.columns:
                        if col.dimension and col.dimension.current == dim_node:
                            if col.dimension_column is None and not any(
                                dim_col.name == "id" for dim_col in dim_node.columns
                            ):
                                raise DJException(
                                    f"Node {table_node.node.name} specifiying dimension "
                                    f"{dim_node.node.name} on column {col.name} does not"
                                    f" specify a dimension column, but {dim_node.node.name} "
                                    f"does not have the default key `id`.",
                                )
                            join_dim_cols.append(col)

                    join_info[table_node] = join_dim_cols
                dim_table = cast(
                    Optional[ast.Table],
                    _get_node_table(dim_node, build_criteria),
                )  # got a materialization
                if (
                    dim_table is None
                ):  # no materialization - recurse to build dimension first  # pragma: no cover
                    dim_query = parse(cast(str, dim_node.query), dialect)
                    dim_table = build_ast(  # type: ignore  # noqa
                        session,
                        dim_query,
                        build_criteria,
                    ).to_select()

                alias = amenable_name(dim_node.node.name)
                dim_ast = ast.Alias(  # type: ignore
                    ast.Name(alias),
                    child=dim_table,
                )

                for (
                    dim_col
                ) in dim_cols:  # replace column table references to this dimension
                    dim_col.add_table(dim_ast)  # type: ignore

                for table_node, cols in join_info.items():
                    ast_tables = tables[table_node]
                    join_on = []
                    for table in ast_tables:
                        for col in cols:
                            join_on.append(
                                ast.BinaryOp.Eq(
                                    ast.Column(ast.Name(col.name), _table=table),
                                    ast.Column(
                                        ast.Name(col.dimension_column or "id"),
                                        _table=dim_ast,  # type: ignore
                                    ),
                                ),
                            )

                    if join_on:  # table had stuff to join
                        select.from_.joins.append(  # pragma: no cover
                            ast.Join(
                                ast.JoinKind.LeftOuter,
                                dim_ast,  # type: ignore
                                ast.BinaryOp.And(*join_on),  # pylint: disable=E1120
                            ),
                        )


def _build_tables_on_select(
    session: Session,
    select: ast.Select,
    tables: Dict[NodeRevision, List[ast.Table]],
    dialect: Optional[str] = None,
    build_criteria: Optional[BuildCriteria] = None,
):
    """
    Add all nodes not agg or filter dimensions to the select
    """
    for node, tbls in tables.items():

        node_table = cast(
            Optional[ast.Table],
            _get_node_table(node, build_criteria),
        )  # got a materialization
        if node_table is None:  # no materialization - recurse to node first
            node_query = parse(cast(str, node.query), dialect)
            node_table = build_ast(  # type: ignore
                session,
                node_query,
                build_criteria,
            ).to_select()  # pylint: disable=W0212

        alias = amenable_name(node.node.name)
        node_ast = ast.Alias(ast.Name(alias), child=node_table)  # type: ignore
        for tbl in tbls:
            select.replace(tbl, node_ast)


# flake8: noqa: C901
def _build_select_ast(
    session: Session,
    select: ast.Select,
    dialect: Optional[str] = None,
    build_criteria: Optional[BuildCriteria] = None,
):
    """
    Transforms a select ast by replacing dj node references with their asts
    """

    dimension_columns = _get_dim_cols_from_select(select)
    tables = _get_tables_from_select(select)

    _build_dimensions_on_select(session, dimension_columns, dialect, build_criteria)

    _build_tables_on_select(session, select, tables, dialect, build_criteria)


def add_filters_and_dimensions_to_query_ast(
    query: ast.Query,
    dialect: Optional[str] = None,
    filters: Optional[List[str]] = None,
    dimensions: Optional[List[str]] = None,
):
    """
    Add filters and dimensions to a query ast
    """
    projection_addition = []
    if filters:
        filter_asts = (  # pylint: disable=consider-using-ternary
            query.select.where and [query.select.where] or []
        )

        for filter_ in filters:
            temp_select = parse(f"select * where {filter_}", dialect).select
            filter_asts.append(
                # use parse to get the asts from the strings we got
                temp_select.where,  # type:ignore
            )
            projection_addition += list(temp_select.find_all(ast.Column))
        query.select.where = ast.BinaryOp.And(*filter_asts)

    if dimensions:
        for agg in dimensions:
            temp_select = parse(
                f"select * group by {agg}",
                dialect,
            ).select
            query.select.group_by += temp_select.group_by  # type:ignore
            projection_addition += list(temp_select.find_all(ast.Column))
    query.select.projection += [
        col.set_api_column(True).copy() for col in set(projection_addition)
    ]


def _get_node_table(
    node: NodeRevision,
    build_criteria: Optional[BuildCriteria],
    as_select: bool = False,
) -> Optional[Union[ast.Select, ast.Table]]:
    """
    If a node is available, return a table for the available state
    """
    table = None
    if node.type == NodeType.SOURCE:
        namespace = None
        if node.table:
            name = ast.Name(node.table, '"')
            namespace = (
                ast.Namespace([ast.Name(node.schema_, '"')])
                if node.schema_
                else ast.Namespace([])
            )
        else:
            name = ast.Name(node.name, '"')
        table = ast.Table(name, namespace, _dj_node=node)
    elif node.availability and node.availability.is_available(
        criteria=build_criteria,
    ):  # pragma: no cover
        namespace = (
            ast.Namespace([ast.Name(node.availability.schema_, '"')])
            if node.availability.schema_
            else ast.Namespace([])
        )
        table = ast.Table(
            ast.Name(node.availability.table, '"'),
            namespace,
            _dj_node=node,
        )
    if table and as_select:  # pragma: no cover
        return ast.Select(projection=[ast.Wildcard()], from_=ast.From(tables=[table]))
    return table


def build_node(  # pylint: disable=too-many-arguments
    session: Session,
    node: NodeRevision,
    dialect: Optional[str] = None,
    filters: Optional[List[str]] = None,
    dimensions: Optional[List[str]] = None,
    build_criteria: Optional[BuildCriteria] = None,
) -> ast.Query:
    """
    Determines the optimal way to build the Node and does so
    """
    if node.query is None:  # pragma: no cover
        raise Exception(
            "Node has no query. Cannot build a node without a query.",
        )
    # if no dimensions need to be added then we can see if the node is directly materialized
    if not (filters or dimensions):
        if select := cast(
            ast.Select,
            _get_node_table(node, build_criteria, as_select=True),
        ):
            return ast.Query(select=select)  # pragma: no cover

    query = parse(node.query, dialect)
    add_filters_and_dimensions_to_query_ast(query, dialect, filters, dimensions)

    return build_ast(session, query, build_criteria)


def build_ast(  # pylint: disable=too-many-arguments
    session: Session,
    query: ast.Query,
    build_criteria: Optional[BuildCriteria] = None,
) -> ast.Query:
    """
    Determines the optimal way to build the query AST and does so
    """
    query.compile(session)
    query.build(session, build_criteria)
    return query
