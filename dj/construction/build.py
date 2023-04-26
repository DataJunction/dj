"""Functions to add to an ast DJ node queries"""
import collections

# pylint: disable=too-many-arguments,too-many-locals,too-many-nested-blocks,too-many-branches,R0401
from typing import DefaultDict, Deque, Dict, List, Optional, Set, Tuple, Union, cast

from sqlmodel import Session

from dj.construction.utils import amenable_name, to_namespaced_name
from dj.errors import DJException, DJInvalidInputException
from dj.models.column import Column
from dj.models.engine import Dialect
from dj.models.node import BuildCriteria, Node, NodeRevision, NodeType
from dj.sql.dag import get_shared_dimensions
from dj.sql.parsing.ast import CompileContext
from dj.sql.parsing.backends.antlr4 import ast, parse


def _get_tables_from_select(
    select: ast.SelectExpression,
) -> DefaultDict[NodeRevision, List[ast.Table]]:
    """
    Extract all tables (source, transform, dimensions)
    directly on the select that have an attached DJ node
    """
    tables: DefaultDict[NodeRevision, List[ast.Table]] = collections.defaultdict(list)

    for table in select.find_all(ast.Table):
        if node := table.dj_node:  # pragma: no cover
            tables[node].append(table)
    return tables


def _join_path(
    dimension_node: NodeRevision,
    initial_nodes: Set[NodeRevision],
) -> Tuple[NodeRevision, Dict[Tuple[NodeRevision, NodeRevision], List[Column]]]:
    """
    For a dimension node, we want to find a possible join path between it
    and any of the nodes that are directly referenced in the original query. If
    no join path exists, returns an empty dict.
    """
    processed = set()

    to_process: Deque[
        Tuple[NodeRevision, Dict[Tuple[NodeRevision], List[Column]]]
    ] = collections.deque([])
    join_info: Dict[Tuple[NodeRevision], List[Column]] = {}
    to_process.extend([(node, join_info.copy()) for node in initial_nodes])
    possible_join_paths = []

    while to_process:
        current_node, path = to_process.popleft()
        processed.add(current_node)
        dimensions_to_columns = collections.defaultdict(list)

        # From the columns on the current node, find the next layer of
        # dimension nodes that can be joined in
        for col in current_node.columns:
            if col.dimension and col.dimension.type == NodeType.DIMENSION:
                dimensions_to_columns[col.dimension.current].append(col)

        # Go through all potential dimensions and their join columns
        for joinable_dim, join_cols in dimensions_to_columns.items():
            next_join_path = {**path, **{(current_node, joinable_dim): join_cols}}
            full_join_path = (joinable_dim, next_join_path)
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
                possible_join_paths.append(full_join_path)  # type: ignore
            if joinable_dim not in processed:  # pragma: no cover
                to_process.append(full_join_path)
                for parent in joinable_dim.parents:
                    to_process.append((parent.current, next_join_path))
    return min(possible_join_paths, key=len)  # type: ignore


def _get_or_build_join_table(
    session: Session,
    table_node: NodeRevision,
    build_criteria: Optional[BuildCriteria],
):
    """
    Build the join table from a materialization if one is available, or recurse
    to build it from the dimension node's query if not
    """
    table_node_alias = amenable_name(table_node.name)
    join_table = cast(
        Optional[ast.TableExpression],
        _get_node_table(table_node, build_criteria),
    )
    if not join_table:  # pragma: no cover
        join_query = parse(cast(str, table_node.query))
        join_table = build_ast(session, join_query)  # type: ignore
        join_table.parenthesized = True  # type: ignore

    for col in join_table.columns:
        col._table = join_table  # pylint: disable=protected-access

    join_table = cast(ast.TableExpression, join_table)  # type: ignore
    right_alias = ast.Name(table_node_alias)
    join_right = ast.Alias(  # type: ignore
        right_alias,
        child=join_table,
        as_=True,
    )
    join_table.set_alias(right_alias)  # type: ignore
    return join_right


def _build_joins_for_dimension(
    session: Session,
    dim_node: NodeRevision,
    initial_nodes: Set[NodeRevision],
    tables: DefaultDict[NodeRevision, List[ast.Table]],
    build_criteria: Optional[BuildCriteria],
    required_dimension_columns: List[ast.Column],
) -> List[ast.Join]:
    """
    Returns the join ASTs needed to bring in the dimension node from
    the set of initial nodes.
    """
    _, paths = _join_path(dim_node, initial_nodes)
    asts = []
    for connecting_nodes, join_columns in paths.items():
        start_node, table_node = connecting_nodes  # type: ignore
        join_on = []

        # Assemble table on left of join
        left_table = (
            tables[start_node][0].child  # type: ignore
            if isinstance(tables[start_node][0], ast.Alias)
            else tables[start_node][0]
        )
        join_left_columns = {
            col.alias_or_name.name: col for col in left_table.columns  # type: ignore
        }

        # Assemble table on right of join
        join_right = _get_or_build_join_table(
            session,
            table_node,
            build_criteria,
        )

        # Optimize query by filtering down to only the necessary columns
        selected_columns = {col.name.name for col in required_dimension_columns}
        available_join_columns = {
            col.dimension_column for col in join_columns if col.dimension_column
        }
        primary_key_columns = {col.name for col in table_node.primary_key()}
        joinable_dim_columns = {
            col.name for col in table_node.columns if col.dimension_id
        }
        required_mapping = (
            selected_columns.union(available_join_columns)
            .union(primary_key_columns)
            .union(joinable_dim_columns)
        )
        join_right.child.select.projection = [
            col
            for col in join_right.child.select.projection
            if col.alias_or_name.name in required_mapping
        ]

        initial_nodes.add(table_node)
        tables[table_node].append(join_right)  # type: ignore
        join_right_columns = {
            col.alias_or_name.name: col  # type: ignore
            for col in join_right.child.columns
        }

        # Assemble join ON clause
        for join_col in join_columns:
            join_table_pk = table_node.primary_key()
            if join_col.name in join_left_columns and (
                join_col.dimension_column in join_right_columns
                or join_table_pk[0].name in join_right_columns
            ):
                left_table.add_ref_column(
                    cast(ast.Column, join_left_columns[join_col.name]),
                )
                join_on.append(
                    ast.BinaryOp.Eq(
                        join_left_columns[join_col.name],
                        join_right_columns[
                            join_col.dimension_column or join_table_pk[0].name
                        ],
                        use_alias_as_name=True,
                    ),
                )
            else:
                raise DJInvalidInputException(  # pragma: no cover
                    f"The specified join column {join_col.dimension_column} "
                    f"does not exist on {table_node.name}",
                )
            for dim_col in required_dimension_columns:
                join_right.child.add_ref_column(dim_col)

        if join_on:  # pragma: no cover
            asts.append(
                ast.Join(
                    "LEFT OUTER",
                    join_right,  # type: ignore
                    ast.JoinCriteria(
                        on=ast.BinaryOp.And(*join_on),  # pylint: disable=E1120
                    ),
                ),
            )
    return asts


def join_tables_for_dimensions(
    session: Session,
    dimension_nodes_to_columns: Dict[NodeRevision, List[ast.Column]],
    tables: DefaultDict[NodeRevision, List[ast.Table]],
    build_criteria: Optional[BuildCriteria] = None,
):
    """
    Joins the tables necessary for a set of filter and group by dimensions
    onto the select expression.

    In some cases, the necessary tables will already be on the select and
    no additional joins will be needed. However, if the tables are not in
    the select, it will traverse through available linked tables (via dimension
    nodes) and join them in.
    """
    for dim_node, required_dimension_columns in sorted(
        dimension_nodes_to_columns.items(),
        key=lambda x: x[0].name,
    ):
        # Find all the selects that contain the different dimension columns
        selects_map = {
            cast(ast.Select, dim_col.get_nearest_parent_of_type(ast.Select))
            for dim_col in required_dimension_columns
        }

        # Join the source tables (if necessary) for these dimension columns
        # onto each select clause
        for select in selects_map:
            initial_nodes = set(tables)
            if dim_node not in initial_nodes:  # need to join dimension
                join_asts = _build_joins_for_dimension(
                    session,
                    dim_node,
                    initial_nodes,
                    tables,
                    build_criteria,
                    required_dimension_columns,
                )
                if join_asts and select.from_:
                    select.from_.relations[-1].extensions.extend(  # pragma: no cover
                        join_asts,
                    )


def _build_tables_on_select(
    session: Session,
    select: ast.SelectExpression,
    tables: Dict[NodeRevision, List[ast.Table]],
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
            node_query = parse(cast(str, node.query))
            node_table = build_ast(  # type: ignore
                session,
                node_query,
                build_criteria,
            ).select  # pylint: disable=W0212
            node_table.parenthesized = True  # type: ignore

        alias = amenable_name(node.node.name)
        context = CompileContext(session=session, exception=DJException())

        node_ast = ast.Alias(ast.Name(alias), child=node_table, as_=True)  # type: ignore
        for tbl in tbls:
            if isinstance(node_ast.child, ast.Select) and isinstance(tbl, ast.Alias):
                node_ast.child.projection = [
                    col
                    for col in node_ast.child.projection
                    if col in set(tbl.child.select.projection)
                ]
            node_ast.compile(context)
            select.replace(tbl, node_ast)


def dimension_columns_mapping(
    select: ast.SelectExpression,
) -> Dict[NodeRevision, List[ast.Column]]:
    """
    Extract all dimension nodes referenced by columns
    """
    dimension_nodes_to_columns: Dict[NodeRevision, List[ast.Column]] = {}

    for col in select.find_all(ast.Column):
        if isinstance(col.table, ast.Table):
            if node := col.table.dj_node:  # pragma: no cover
                if node.type == NodeType.DIMENSION:
                    dimension_nodes_to_columns[node] = dimension_nodes_to_columns.get(
                        node,
                        [],
                    )
                    dimension_nodes_to_columns[node].append(col)
    return dimension_nodes_to_columns


# flake8: noqa: C901
def _build_select_ast(
    session: Session,
    select: ast.SelectExpression,
    build_criteria: Optional[BuildCriteria] = None,
):
    """
    Transforms a select ast by replacing dj node references with their asts
    Starts by extracting all dimensions-backed columns from filters + group bys.
    Some of them can be sourced directly from tables on the select, others cannot
    For the ones that cannot be sourced directly, attempt to join them via dimension links.
    """
    tables = _get_tables_from_select(select)
    dimension_columns = dimension_columns_mapping(select)
    join_tables_for_dimensions(session, dimension_columns, tables, build_criteria)
    _build_tables_on_select(session, select, tables, build_criteria)


def add_filters_and_dimensions_to_query_ast(
    query: ast.Query,
    dialect: Optional[str] = None,  # pylint: disable=unused-argument
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
            temp_select = parse(f"select * where {filter_}").select
            filter_asts.append(
                # use parse to get the asts from the strings we got
                temp_select.where,  # type:ignore
            )
        query.select.where = ast.BinaryOp.And(*filter_asts)

    if dimensions:
        for agg in dimensions:
            temp_select = parse(
                f"select * group by {agg}",
            ).select
            query.select.group_by += temp_select.group_by  # type:ignore
            projection_addition += list(temp_select.find_all(ast.Column))
    query.select.projection += list(projection_addition)

    # Cannot select for columns that aren't in GROUP BY and aren't aggregations
    if query.select.group_by:
        query.select.projection = [
            col
            for col in query.select.projection
            if col.is_aggregation()  # type: ignore
            or col.name.name in {gc.name.name for gc in query.select.group_by}  # type: ignore
        ]


def _get_node_table(
    node: NodeRevision,
    build_criteria: Optional[BuildCriteria] = None,
    as_select: bool = False,
) -> Optional[Union[ast.Select, ast.Table]]:
    """
    If a node has a materialization available, return the materialized table
    """
    table = None
    if node.type == NodeType.SOURCE:
        if node.table:
            name = ast.Name(
                node.table,
                namespace=ast.Name(node.schema_) if node.schema_ else None,
            )
        else:
            name = to_namespaced_name(node.name)
        table = ast.Table(name, _dj_node=node)
    elif node.availability and node.availability.is_available(
        criteria=build_criteria,
    ):  # pragma: no cover
        table = ast.Table(
            ast.Name(
                node.availability.table,
                namespace=(
                    ast.Name(node.availability.schema_)
                    if node.availability.schema_
                    else None
                ),
            ),
            _dj_node=node,
        )
    if table and as_select:  # pragma: no cover
        return ast.Select(
            projection=[ast.Wildcard()],
            from_=ast.From(relations=[ast.Relation(table)]),
        )
    return table


def build_node(  # pylint: disable=too-many-arguments
    session: Session,
    node: NodeRevision,
    filters: Optional[List[str]] = None,
    dimensions: Optional[List[str]] = None,
    build_criteria: Optional[BuildCriteria] = None,
) -> ast.Query:
    """
    Determines the optimal way to build the Node and does so
    """
    # Set the dialect by finding available engines for this node, or default to Spark
    if not build_criteria:
        build_criteria = BuildCriteria(
            dialect=(
                node.catalog.engines[0].dialect
                if node.catalog
                and node.catalog.engines
                and node.catalog.engines[0].dialect
                else Dialect.SPARK
            ),
        )

    # if no dimensions need to be added then we can see if the node is directly materialized
    if not (filters or dimensions):
        if select := cast(
            ast.Select,
            _get_node_table(node, build_criteria, as_select=True),
        ):
            return ast.Query(select=select)  # pragma: no cover

    if node.query:
        query = parse(node.query)
    else:
        query = build_source_node_query(node)

    add_filters_and_dimensions_to_query_ast(
        query,
        build_criteria.dialect,
        filters,
        dimensions,
    )

    return build_ast(session, query, build_criteria)


def build_metric_nodes(
    session: Session,
    metric_nodes: List[Node],
    filters: List[str],
    dimensions: List[str],
    build_criteria: Optional[BuildCriteria] = None,
):
    """
    Build a single query for all metrics in the list, including the
    specified group bys (dimensions) and filters.
    """
    shared_dimensions = get_shared_dimensions(metric_nodes)
    for dimension_attribute in dimensions:
        if dimension_attribute not in shared_dimensions:
            raise DJInvalidInputException(
                f"The dimension attribute `{dimension_attribute}` is not "
                "available on every metric and thus cannot be included.",
            )

    combined_ast = build_node(
        session,
        metric_nodes[0].current,
        filters,
        dimensions,
        build_criteria,
    )
    metric_dependencies: Set[str] = set()
    for metric_node in metric_nodes[1:]:
        if metric_node.type != NodeType.METRIC:  # pragma: no cover
            raise DJInvalidInputException(
                "Cannot build a query for multiple nodes if one or more "
                f"of them aren't metric nodes. ({metric_node.name} is not"
                "a metric node)",
            )
        metric_ast = build_node(
            session,
            metric_node.current,
            filters,
            dimensions,
            build_criteria,
        )
        metric_dependencies = metric_dependencies.union(
            {tbl.alias_or_name.name for tbl in metric_ast.find_all(ast.Table)},
        )
        built_source_tables = {
            tbl.alias_or_name.name for tbl in metric_ast.find_all(ast.Table)
        }
        diff_columns = set(combined_ast.select.projection).difference(
            metric_ast.select.projection,
        )
        if all(tbl in built_source_tables for tbl in metric_dependencies):
            metric_ast.select.projection.extend(diff_columns)
            combined_ast = metric_ast
        else:
            combined_ast.select.projection.extend(diff_columns)  # pragma: no cover

    built_source_tables = {
        tbl.alias_or_name.name for tbl in combined_ast.find_all(ast.Table)
    }
    if not all(tbl in built_source_tables for tbl in metric_dependencies):
        raise DJInvalidInputException(  # pragma: no cover
            "We cannot build these metrics together as they aren't "
            "querying from the same sources. Metric dependencies include "
            ", ".join(metric_dependencies),
        )
    return combined_ast


def build_source_node_query(node: NodeRevision):
    """
    Returns a query that selects each column explicitly in the source node.
    """
    table = ast.Table(to_namespaced_name(node.name), None, _dj_node=node)
    select = ast.Select(
        projection=[
            ast.Column(ast.Name(tbl_col.name), _table=table) for tbl_col in node.columns
        ],
        from_=ast.From(relations=[ast.Relation(table)]),
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
    context = CompileContext(session=session, exception=DJException())
    query.compile(context)
    query.build(session, build_criteria)
    return query
