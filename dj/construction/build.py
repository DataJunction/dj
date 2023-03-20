"""Functions to add to an ast DJ node queries"""
import collections
# pylint: disable=too-many-arguments,too-many-locals,too-many-nested-blocks,too-many-branches,R0401
from typing import Dict, List, Optional, Union, cast, Set, DefaultDict

from sqlmodel import Session

from dj.construction.utils import amenable_name
from dj.errors import DJException
from dj.models.column import Column
from dj.models.node import BuildCriteria, NodeRevision, NodeType
from dj.sql.parsing import ast, parse


def _get_tables_from_select(select: ast.Select) -> DefaultDict[NodeRevision, List[ast.Table]]:
    """
    Extract all tables (source, transform, dimensions)
    """
    tables: DefaultDict[NodeRevision, List[ast.Table]] = collections.defaultdict(list)

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


def join_path(dimension_node: NodeRevision, nodes: Set[NodeRevision]):
    """
    Assuming the two nodes are joinable, finds a join path between them.
    Needs all the intermediary join nodes or else it won't know how to do the join.
    """
    processed = set()

    to_process = collections.deque([])
    join_info: Dict[NodeRevision, List[Column]] = {}
    to_process.extend([(node, join_info.copy()) for node in nodes])

    while to_process:
        current_node, path = to_process.popleft()
        processed.add(current_node)
        dimensions_to_columns = collections.defaultdict(list)

        for col in current_node.columns:
            if col.dimension:
                dimensions_to_columns[col.dimension.current].append(col)
        for joinable_dim, join_cols in dimensions_to_columns.items():
            # print("joinable_dim", joinable_dim.name, [c.name for c in join_cols])
            next_path = {**path, **{(current_node, joinable_dim): join_cols}}
            next_item = (joinable_dim, next_path)
            if joinable_dim == dimension_node:
                for col in join_cols:
                    if col.dimension_column is None and not any(
                        dim_col.name == "id" for dim_col in dimension_node.columns
                    ):
                        raise DJException(
                            f"Node {current_node.name} specifying dimension "
                            f"{joinable_dim.name} on column {col.name} does not"
                            f" specify a dimension column, but {dimension_node.name} "
                            f"does not have the default key `id`.",
                        )
                return next_item
            if joinable_dim not in processed:
                to_process.append(next_item)
                for parent in joinable_dim.parents:
                    to_process.append((parent.current, next_path))
    return None


def _build_dimensions_on_select(
    session: Session,
    dimension_columns: Dict[NodeRevision, List[ast.Column]],
    tables: DefaultDict[NodeRevision, List[ast.Table]],
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
            initial_tables = _get_tables_from_select(select)
            if dim_node not in initial_tables:  # need to join dimension
                node, paths = join_path(dim_node, set(initial_tables.keys()))
                join_info: Dict[NodeRevision, List[Column]] = paths
                for (start_node, table_node), cols in join_info.items():
                    join_on = []
                    start_node_alias = amenable_name(start_node.name)
                    tables[start_node].append(ast.Table(ast.Name(start_node.name.split(".")[-1]),
                                                        ast.Namespace(start_node.name.split(".")[-1]),
                                                        _dj_node=start_node))
                    table_node_alias = amenable_name(table_node.name)
                    tables[table_node].append(ast.Table(ast.Name(table_node.name.split(".")[-1]),
                                                        ast.Namespace(table_node.name.split(".")[-1]),
                                                        _dj_node=table_node))

                    for col in cols:
                        join_on.append(
                            ast.BinaryOp.Eq(
                                ast.Column(
                                    ast.Name(col.name),
                                    _table=tables[start_node][0],
                                ),
                                ast.Column(
                                    ast.Name(col.dimension_column or "id"),
                                    _table=tables[table_node][0],
                                ),
                            ),
                        )
                    join_table = cast(
                        Optional[ast.Table],
                        _get_node_table(table_node, build_criteria),
                    )  # got a materialization
                    if (
                        join_table is None
                    ):  # no materialization - recurse to build dimension first  # pragma: no cover
                        join_query = parse(cast(str, table_node.query), dialect)
                        join_table = build_ast(session, join_query)

                    join_ast = ast.Alias(  # type: ignore
                        ast.Name(table_node_alias),
                        child=join_table,
                    )

                    if join_on:  # table had stuff to join
                        select.from_.joins.append(  # pragma: no cover
                            ast.Join(
                                ast.JoinKind.LeftOuter,
                                join_ast,  # type: ignore
                                ast.BinaryOp.And(*join_on),  # pylint: disable=E1120
                            ),
                        )


def _build_tables_on_select(
    session: Session,
    select: ast.Select,
    tables: DefaultDict[NodeRevision, List[ast.Table]],
    dialect: Optional[str] = None,
    build_criteria: Optional[BuildCriteria] = None,
):
    """
    Add all nodes not agg or filter dimensions to the select
    """
    # print("tables (nodes)", [(n.name, amenable_name(n.name)) for n in tables.keys()], select.where)
    for node, tbls in tables.items():
        print("tables: ", node.name, [t.name for t in tbls])

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

        alias = amenable_name(node.name)
        node_ast = ast.Alias(ast.Name(alias), child=node_table)  # type: ignore
        for tbl in tbls:
            # old_table = ast.Table(ast.Name(node.name.split(".")[-1]),
            #                       ast.Namespace(node.name.split(".")[:-1]),
            #                       _dj_node=node)
            if node.name == "basic.dimension.users":
                print("replacing", tbl.name, "with", node_ast.name)
            select.replace(tbl, node_ast)
    print("select `WHERE` after replacing tables", select.where)


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

    _build_dimensions_on_select(session, dimension_columns, tables, dialect, build_criteria)

    _build_tables_on_select(session, select, tables, dialect, build_criteria)
    select


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

    # Cannot select for columns that aren't in GROUP BY and aren't aggregations
    if query.select.group_by:
        query.select.projection = [
            col
            for col in query.select.projection
            if col.is_aggregation()
            or col.name.name in {gc.name.name for gc in query.select.group_by}  # type: ignore
        ]


def _get_node_table(
    node: NodeRevision,
    build_criteria: Optional[BuildCriteria] = None,
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
    # if no dimensions need to be added then we can see if the node is directly materialized
    if not (filters or dimensions):
        if select := cast(
            ast.Select,
            _get_node_table(node, build_criteria, as_select=True),
        ):
            return ast.Query(select=select)  # pragma: no cover

    if node.query:
        query = parse(node.query, dialect)
    else:
        query = build_source_node_query(node)

    add_filters_and_dimensions_to_query_ast(
        query,
        dialect,
        filters,
        dimensions,
    )

    return build_ast(session, query, build_criteria)


def build_source_node_query(node: NodeRevision):
    """
    Returns a query that selects each column explicitly in the source node.
    """
    name = ast.Name(node.name, '"')
    table = ast.Table(name, None, _dj_node=node)
    select = ast.Select(
        projection=[
            ast.Column(ast.Name(tbl_col.name), _table=table.alias_or_self())
            for tbl_col in node.columns
        ],
        from_=ast.From(tables=[table]),
    )
    return ast.Query(select=select)


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
