"""Functions to add to an ast DJ node queries"""

from functools import reduce
from typing import Dict, List, Optional, Set, Tuple

from sqlmodel import Session

from dj.construction.build_planning import (
    BuildPlan,
    generate_build_plan_from_query,
    optimize_level_by_cost,
    optimize_level_by_database_id,
)
from dj.construction.utils import amenable_name
from dj.errors import DJException
from dj.models.column import Column
from dj.models.database import Database
from dj.models.node import Node, NodeType
from dj.sql.parsing import ast
from dj.sql.parsing.backends.sqloxide import parse


# flake8: noqa: C901
def _build_select(  # pylint: disable=too-many-arguments,too-many-locals,too-many-nested-blocks,too-many-branches
    session: Session,
    select: ast.Select,
    build_plan: BuildPlan,
    build_plan_depth: int,
    database: Database,
    dialect: Optional[str] = None,
) -> ast.Select:
    """
    Transforms a select ast by replacing dj node references with their asts
    """

    _, build_plan_lookup = build_plan

    dimension_columns: Dict[Node, List[ast.Column]] = {}
    tables: Dict[Node, List[ast.Table]] = {}

    for table in select.find_all(ast.Table):
        if node := table.dj_node:  # pragma: no cover
            tables[node] = tables.get(node, [])
            tables[node].append(table)

    for col in select.find_all(ast.Column):
        if isinstance(col.table, ast.Table):
            if node := col.table.dj_node:  # pragma: no cover
                if node.type == NodeType.DIMENSION:
                    dimension_columns[node] = dimension_columns.get(node, [])
                    dimension_columns[node].append(col)

    for dim_node, dim_cols in dimension_columns.items():
        if dim_node not in tables:  # need to join dimension
            join_info: Dict[Node, List[Column]] = {}
            for table_node in tables:
                join_dim_cols = []
                for col in table_node.columns:
                    if col.dimension == dim_node:
                        if col.dimension_column is None and not any(
                            dim_col.name == "id" for dim_col in dim_node.columns
                        ):
                            raise DJException(
                                f"Node {table_node.name} specifiying dimension {dim_node.name}"
                                f" on column {col.name} does not specify a dimension column, "
                                f"but {dim_node.name} does not have the default key `id`.",
                            )
                        join_dim_cols.append(col)

                join_info[table_node] = join_dim_cols
            if build_plan_depth > 0:  # continue following build plan
                alias = amenable_name(dim_node.name)

                _, dim_build_plan = build_plan_lookup[dim_node]
                dim_ast = dim_build_plan[0]
                dim_query: ast.Query = _build_query(
                    session,
                    dim_ast,
                    dim_build_plan,
                    build_plan_depth - 1,
                    database,
                    dialect,
                )

                dim_select = dim_query.select
                dim_ast = ast.Alias(ast.Name(alias), child=dim_select)
            else:  # pragma: no cover
                dim_table = [
                    table
                    for table in dim_node.tables
                    if table.database.id == database.id
                ][0]
                dim_ast = ast.Table(
                    ast.Name(dim_table.table),
                    namespace=ast.Namespace(
                        [
                            ast.Name(s)
                            for s in (dim_table.catalog, dim_table.schema_)
                            if s
                        ],
                    ),
                )

            for (
                dim_col
            ) in dim_cols:  # replace column table references to this dimension
                dim_col.add_table(dim_ast)

            for table_node, cols in join_info.items():
                ast_tables = tables[table_node]
                join_on = []
                for table in ast_tables:
                    for col in cols:
                        join_on.append(
                            ast.BinaryOp(
                                ast.BinaryOpKind.Eq,
                                ast.Column(ast.Name(col.name), _table=table),
                                ast.Column(
                                    ast.Name(col.dimension_column or "id"),
                                    _table=dim_ast,
                                ),
                            ),
                        )
                select.from_.joins.append(  # pragma: no cover
                    ast.Join(
                        ast.JoinKind.LeftOuter,
                        dim_ast,
                        reduce(
                            lambda left, right: ast.BinaryOp(
                                ast.BinaryOpKind.And,
                                left,
                                right,
                            ),
                            join_on,
                        ),
                    ),
                )

    for node, tbls in tables.items():

        if (
            node.type != NodeType.SOURCE and build_plan_depth > 0
        ):  # continue following build plan
            _, node_build_plan = build_plan_lookup[node]
            node_ast = node_build_plan[0]
            node_query = _build_query(
                session,
                node_ast,
                node_build_plan,
                build_plan_depth - 1,
                database,
                dialect,
            )
            alias = amenable_name(node.name)
            node_select = node_query.select
            node_ast = ast.Alias(ast.Name(alias), child=node_select)
            for tbl in tbls:
                select.replace(tbl, node_ast)
        else:
            node_table = [
                table for table in node.tables if table.database.id == database.id
            ][0]
            node_ast = ast.Table(
                ast.Name(node_table.table),
                namespace=ast.Namespace(
                    [
                        ast.Name(s)
                        for s in (node_table.catalog, node_table.schema_)
                        if s
                    ],
                ),
            )

        for tbl in tbls:
            select.replace(tbl, node_ast)

    return select


def _build_query(  # pylint: disable=too-many-arguments
    session: Session,
    query: ast.Query,
    build_plan: BuildPlan,
    build_plan_depth: int,
    database: Database,
    dialect: Optional[str] = None,
) -> ast.Query:
    """
    Transforms a query ast by replacing dj node references with their asts
    """
    select = query._to_select()
    _build_select(session, select, build_plan, build_plan_depth, database, dialect)
    for i, exp in enumerate(select.projection):
        if not isinstance(exp, ast.Named):
            name = f"_col{i}"
            aliased = ast.Alias(ast.Name(name), child=exp)
            # only replace those that are identical in memory
            select.replace(exp, aliased, lambda a, b: id(a) == id(b))
    return query


def add_filters_aggs(
    query: ast.Query,
    dialect: Optional[str] = None,
    filters: Optional[List[str]] = None,
    aggs: Optional[List[str]] = None,
) -> ast.Query:
    """
    Add filters and aggs to a query ast
    """
    if filters:
        filter_asts = (  # pylint: disable=consider-using-ternary
            query.select.where and [query.select.where] or []
        )

        for filter_ in filters:
            filter_asts.append(
                # use parse to get the asts from the strings we got
                parse(f"select * where {filter_}", dialect).select.where,  # type:ignore
            )
        query.select.where = reduce(
            lambda left, right: ast.BinaryOp(
                ast.BinaryOpKind.And,
                left,
                right,
            ),
            filter_asts,
        )
    if aggs:
        for agg in aggs:
            query.select.group_by += parse(
                f"select * group by {agg}",
                dialect,
            ).select.group_by  # type:ignore

    return query


async def build_node(  # pylint: disable=too-many-arguments
    session: Session,
    node: Node,
    dialect: Optional[str] = None,
    database_id: Optional[int] = None,
    filters: Optional[List[str]] = None,
    aggs: Optional[List[str]] = None,
) -> Tuple[ast.Query, Database]:
    """
    Determines the optimal database to run the query in and builds the query AST appropriately
    """
    if node.query is None:
        raise Exception(
            "Node has no query. Cannot generate a build plan without a query.",
        )
    query = parse(node.query, dialect)
    top_dbs: Set[Database] = set()
    if filters or aggs:
        query = add_filters_aggs(query, dialect, filters, aggs)
    else:
        top_dbs = {table.database for table in node.tables}
    build_plan = generate_build_plan_from_query(session, query, dialect)
    if database_id is not None:
        for top_db in top_dbs:
            if top_db.id == database_id:  # pragma: no cover
                return query, top_db

        build_plan_depth, database = await optimize_level_by_database_id(
            build_plan,
            database_id,
        )

    else:
        build_plan_depth, database = await optimize_level_by_cost(build_plan)
        for top_db in top_dbs:
            if top_db.cost <= database.cost:  # pragma: no cover
                return query, top_db

    query = build_plan[0]
    query.build(session, build_plan, build_plan_depth, database, dialect)
    return (
        query,
        database,
    )
