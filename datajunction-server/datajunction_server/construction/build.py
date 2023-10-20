"""Functions to add to an ast DJ node queries"""
import collections
import logging
import time

# pylint: disable=too-many-arguments,too-many-locals,too-many-nested-blocks,too-many-branches,R0401
from typing import DefaultDict, Deque, Dict, List, Optional, Set, Tuple, Union, cast

from sqlmodel import Session

from datajunction_server.construction.utils import to_namespaced_name
from datajunction_server.errors import DJException, DJInvalidInputException
from datajunction_server.models import access
from datajunction_server.models.column import Column
from datajunction_server.models.engine import Dialect
from datajunction_server.models.materialization import GenericCubeConfig
from datajunction_server.models.node import BuildCriteria, Node, NodeRevision, NodeType
from datajunction_server.sql.dag import get_shared_dimensions
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse
from datajunction_server.sql.parsing.types import ColumnType
from datajunction_server.utils import LOOKUP_CHARS, SEPARATOR, amenable_name

_logger = logging.getLogger(__name__)


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
                    dim_pk = dimension_node.primary_key()
                    if not col.dimension_column:
                        if len(dim_pk) != 1:
                            raise DJException(  # pragma: no cover
                                f"Node {current_node.name} specifying dimension "
                                f"{joinable_dim.name} on column {col.name} does not"
                                f" specify a dimension column, and {dimension_node.name} "
                                f"has a compound primary key.",
                            )
                        col.dimension_column = dim_pk[0].name

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
    initial_nodes = set(tables)
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
    memoized_queries: Dict[int, ast.Query],
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
            if hash(node_query) in memoized_queries:  # pragma: no cover
                node_table = memoized_queries[hash(node_query)].select  # type: ignore
            else:
                query_ast = build_ast(  # type: ignore
                    session,
                    node_query,
                    memoized_queries,
                    build_criteria,
                )
                node_table = query_ast.select  # type: ignore
                node_table.parenthesized = True  # type: ignore
                memoized_queries[hash(node_query)] = query_ast

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
            select.replace(
                tbl,
                node_ast,
                copy=False,
            )


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
    memoized_queries: Dict[int, ast.Query],
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
    _build_tables_on_select(session, select, tables, memoized_queries, build_criteria)


# pylint: disable=R0915
def add_filters_dimensions_orderby_limit_to_query_ast(
    session: Session,
    query: ast.Query,
    dialect: Optional[str] = None,  # pylint: disable=unused-argument
    filters: Optional[List[str]] = None,
    dimensions: Optional[List[str]] = None,
    orderby: Optional[List[str]] = None,
    limit: Optional[int] = None,
    include_dimensions_in_groupby: bool = True,
    access_control: Optional[access.AccessControlStore] = None,
):
    """
    Add filters and dimensions to a query ast
    """
    projection_addition = {}

    if dimensions:
        for agg in dimensions:
            temp_select = parse(
                f"select * group by {agg}",
            ).select
            if include_dimensions_in_groupby:
                query.select.group_by += temp_select.group_by  # type:ignore
            for col in temp_select.find_all(ast.Column):
                projection_addition[col.identifier(False)] = col

                if access_control:
                    access_control.add_request_by_node_name(
                        session,
                        col,
                    )

    if filters:
        filter_asts = (  # pylint: disable=consider-using-ternary
            query.select.where and [query.select.where] or []
        )

        for filter_ in filters:
            # use parse to get the asts from the strings we got
            temp_select = parse(f"select * where {filter_}").select
            filter_asts.append(
                temp_select.where,  # type:ignore
            )
            for col in temp_select.find_all(ast.Column):
                if not dimensions:
                    projection_addition[col.identifier(False)] = col
                if access_control:
                    access_control.add_request_by_node_name(
                        session,
                        col,
                    )

        query.select.where = ast.BinaryOp.And(*filter_asts)

    if not query.select.organization:
        query.select.organization = ast.Organization([])

    if orderby:
        for order in orderby:
            temp_query = parse(
                f"select * order by {order}",
            )
            query.select.organization.order += (  # type:ignore
                temp_query.select.organization.order  # type:ignore
            )
            for col in temp_query.find_all(ast.Column):
                if access_control:  # pragma: no cover
                    access_control.add_request_by_node_name(
                        session,
                        col,
                    )

    # add all used dimension columns to the projection without duplicates
    projection_update = []
    for exp in query.select.projection:
        if not isinstance(exp, ast.Column):
            projection_update.append(exp)
        else:
            ident = exp.identifier(False)
            added = None
            for exist_idents, exist_cols in projection_addition.items():
                if exist_idents.endswith(ident) or ident.endswith(exist_idents):
                    projection_update.append(exist_cols)
                    added = exist_idents
                    break

            if added is None:
                projection_update.append(exp)
            else:
                del projection_addition[added]

    projection_update += list(projection_addition.values())

    query.select.projection = projection_update

    if limit is not None:
        query.select.limit = ast.Number(limit)


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
    orderby: Optional[List[str]] = None,
    limit: Optional[int] = None,
    build_criteria: Optional[BuildCriteria] = None,
    access_control: Optional[access.AccessControlStore] = None,
    include_dimensions_in_groupby: bool = None,
) -> ast.Query:
    """
    Determines the optimal way to build the Node and does so
    """
    if access_control:
        access_control.add_request_by_node(node)

    if include_dimensions_in_groupby is None:
        include_dimensions_in_groupby = node.type == NodeType.METRIC

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

    # get dimension columns which are required
    # in the stated bound dimensions on the metric node
    dimensions = dimensions or []
    dimensions = [
        col.name for col in node.required_dimensions if col.name not in dimensions
    ] + dimensions

    # if no dimensions need to be added then we can see if the node is directly materialized
    if not (filters or dimensions):
        if select := cast(
            ast.Select,
            _get_node_table(node, build_criteria, as_select=True),
        ):
            return ast.Query(select=select)  # pragma: no cover

    if node.query and node.type == NodeType.METRIC:
        query = parse(NodeRevision.format_metric_alias(node.query, node.name))
    elif node.query and node.type != NodeType.METRIC:
        node_query = parse(node.query)
        if node_query.ctes:
            node_query = node_query.bake_ctes()  # pragma: no cover
        node_query.select.add_aliases_to_unnamed_columns()
        query = parse(f"select * from {node.name}")
        query.select.projection = []
        for expr in node_query.select.projection:
            query.select.projection.append(
                ast.Column(ast.Name(expr.alias_or_name.name), _table=node_query),  # type: ignore
            )
    else:
        query = build_source_node_query(node)

    add_filters_dimensions_orderby_limit_to_query_ast(
        session,
        query,
        build_criteria.dialect,
        filters,
        dimensions,
        orderby,
        limit,
        include_dimensions_in_groupby=include_dimensions_in_groupby,
        access_control=access_control,
    )

    if access_control:
        access_control.validate_and_raise()
        access_control.state = access.AccessControlState.INDIRECT

    memoized_queries: Dict[int, ast.Query] = {}
    _logger.info("Calling build_ast on %s", node.name)
    built_ast = build_ast(session, query, memoized_queries, build_criteria)
    if access_control:
        access_control.add_request_by_nodes(
            [
                cast(NodeRevision, cast(ast.Table, tbl).dj_node)
                for tbl in built_ast.filter(
                    lambda ast_node: isinstance(ast_node, ast.Table)
                    and ast_node.dj_node is not None,
                )
            ],
        )
        access_control.validate_and_raise()
    _logger.info("Finished build_ast on %s", node.name)
    return built_ast


def group_metrics_by_parent(
    metric_nodes: List[Node],
) -> DefaultDict[Node, List[NodeRevision]]:
    """
    Group metrics by their parent node
    """
    common_parents = collections.defaultdict(list)
    for metric_node in metric_nodes:
        immediate_parent = metric_node.current.parents[0]
        common_parents[immediate_parent].append(metric_node.current)
    return common_parents


def build_metric_nodes(
    session: Session,
    metric_nodes: List[Node],
    filters: List[str],
    dimensions: List[str],
    orderby: List[str],
    limit: Optional[int] = None,
    build_criteria: Optional[BuildCriteria] = None,
    access_control: Optional[access.AccessControlStore] = None,
):
    """
    Build a single query for all metrics in the list, including the specified
    group bys (dimensions) and filters. The metric nodes should share the same set
    of dimensional attributes. Then we can:
    (a) Group metrics by their parent nodes.
    (b) Build a query for each parent node with the dimensional attributes referenced joined in
    (c) For all metrics that reference the parent node, insert the metric expression
        into the parent node's select and build the parent query
    (d) Set the rest of the parent query's attributes (filters, orderby, limit etc)
    (e) Join together the transforms on the shared dimensions
    (f) Select all the requested metrics and dimensions in the final SELECT
    """
    if any(metric_node.type != NodeType.METRIC for metric_node in metric_nodes):
        raise DJInvalidInputException(  # pragma: no cover
            "Cannot build a query for multiple nodes if one or more "
            "of them aren't metric nodes.",
        )

    shared_dimensions = [dim.name for dim in get_shared_dimensions(metric_nodes)]
    for dimension_attribute in dimensions:
        if dimension_attribute not in shared_dimensions:
            raise DJInvalidInputException(
                f"The dimension attribute `{dimension_attribute}` is not "
                "available on every metric and thus cannot be included.",
            )

    for filter_ in filters:
        temp_select = parse(f"select * where {filter_}").select
        columns_in_filter = temp_select.where.find_all(ast.Column)  # type: ignore
        dims_without_prefix = {dim.split(".")[-1]: dim for dim in shared_dimensions}
        for col in columns_in_filter:
            if str(col) not in shared_dimensions:
                potential_dimension_match = (
                    f" Did you mean `{dims_without_prefix[str(col)]}`?"
                    if str(col) in dims_without_prefix
                    else ""
                )
                raise DJInvalidInputException(
                    f"The filter `{filter_}` references the dimension attribute "
                    f"`{col}`, which is not available on every"
                    f" metric and thus cannot be included.{potential_dimension_match}",
                )

    combined_ast: ast.Query = ast.Query(
        select=ast.Select(from_=ast.From(relations=[])),
        ctes=[],
    )
    final_mapping = {}
    initial_dimension_columns = []
    all_dimension_columns = []

    orderby_sort_items: List[ast.SortItem] = []
    orderby = orderby or []
    orderby_mapping = {}
    for order in orderby:
        orderby_metric = None
        for metric_node in metric_nodes:
            if metric_node.name.lower() in order.lower():
                orderby_metric = metric_node.name
                break
        orderby_mapping[order] = orderby_metric

    context = CompileContext(session=session, exception=DJException())
    common_parents = group_metrics_by_parent(metric_nodes)

    for parent_node, metrics in common_parents.items():
        parent_ast = build_node(
            session=session,
            node=parent_node.current,
            dimensions=dimensions,
            filters=filters,
            build_criteria=build_criteria,
            include_dimensions_in_groupby=True,
            access_control=access_control,
        )

        # Select only columns that were one of the chosen dimensions
        parent_ast.select.projection = [
            expr
            for expr in parent_ast.select.projection
            if str(expr).replace(f"_{LOOKUP_CHARS.get('.')}_", SEPARATOR) in dimensions
        ]
        # Re-alias the dimensions with better names, but keep the group by alias-free
        parent_ast.select.group_by = [
            expr.copy() for expr in parent_ast.select.group_by
        ]
        for expr in parent_ast.select.projection:
            expr.set_alias(ast.Name(amenable_name(str(expr))))
        parent_ast.compile(context)

        # Add the metric expression into the parent node query
        for metric_node in metrics:
            metric_query = parse(
                NodeRevision.format_metric_alias(
                    metric_node.query,  # type: ignore
                    metric_node.name,
                ),
            )
            metric_query.compile(context)
            metric_query.build(session, {})
            parent_ast.select.projection.extend(metric_query.select.projection)

        # Add the WITH statements to the combined query
        parent_ast_alias = ast.Name(amenable_name(parent_node.name))
        parent_ast.alias = parent_ast_alias
        parent_ast.parenthesized = True
        parent_ast.as_ = True
        combined_ast.ctes += [parent_ast]

        # Add the metric and dimensions to the final query layer's SELECT
        current_cte_as_table = ast.Table(parent_ast_alias)

        # If the parent node contains an orderby, parse and add it to the order items
        # bind the table for this built metric to all columns in the
        organization = cast(ast.Organization, parent_ast.select.organization)
        parent_ast.select.organization = None
        for col in organization.find_all(ast.Column):
            col.add_table(current_cte_as_table)  # pragma: no cover
        orderby_sort_items += organization.order  # type: ignore

        final_select_columns = [
            ast.Column(
                name=col.alias_or_name,  # type: ignore
                _table=current_cte_as_table,
                _type=col.type,  # type: ignore
            )
            for col in parent_ast.select.projection
        ]

        metric_columns = []
        dimension_columns = []
        metric_column_identifiers = {amenable_name(metric.name) for metric in metrics}
        for col in final_select_columns:
            if col.name.name in metric_column_identifiers:
                for metric_node in metrics:
                    if amenable_name(metric_node.name) in col.alias_or_name.name:
                        final_mapping[metric_node.name] = col
                metric_columns.append(col)
            else:
                dimension_columns.append(col)

        all_dimension_columns += dimension_columns
        combined_ast.select.projection.extend(metric_columns)

        # Each time we build another parent node CTE clause, add it
        # to the join clause with the current CTEs on the selected dimensions
        if not combined_ast.select.from_.relations:  # type: ignore
            initial_dimension_columns = dimension_columns
            combined_ast.select.from_.relations.append(  # type: ignore
                ast.Relation(current_cte_as_table),
            )
        else:
            join_parents_on = [
                ast.BinaryOp.Eq(initial_dim_col, current_dim_col)
                for initial_dim_col, current_dim_col in zip(
                    initial_dimension_columns,
                    dimension_columns,
                )
            ]
            combined_ast.select.from_.relations[0].extensions.append(  # type: ignore
                ast.Join(
                    "FULL OUTER",
                    ast.Table(parent_ast_alias),
                    ast.JoinCriteria(
                        on=ast.BinaryOp.And(*join_parents_on),
                    ),
                ),
            )

    # Include dimensions in the final select: COALESCE the dimensions across
    # all parent nodes, which will be used as the joins
    dimension_grouping: Dict[str, List] = {}
    for col in all_dimension_columns:
        dimension_grouping.setdefault(col.identifier(), []).append(col)
    dimension_columns = [
        ast.Function(name=ast.Name("COALESCE"), args=list(columns)).set_alias(
            ast.Name(col_name),
        )
        if len(columns) > 1
        else columns[0]
        for col_name, columns in dimension_grouping.items()
    ]
    for dim_col in dimension_columns:
        dimension_name = dim_col.alias_or_name.name.replace(
            f"_{LOOKUP_CHARS.get('.')}_",
            SEPARATOR,
        )
        final_mapping[dimension_name] = dim_col

    combined_ast.select.projection.extend(dimension_columns)

    # go through the orderby items and make sure we put them in the order the user requested them in
    for idx, sort_item in enumerate(orderby_mapping):
        if isinstance(sort_item, ast.SortItem):
            orderby_sort_items.insert(idx, sort_item)  # pragma: no cover
        else:
            sort_expr_list = sort_item.split(" ")
            if sort_item in final_mapping:  # pragma: no cover
                orderby_sort_items.insert(
                    idx,
                    ast.SortItem(
                        expr=final_mapping[sort_expr_list[0]].alias_or_name,  # type: ignore
                        asc=sort_expr_list[1] if len(sort_expr_list) >= 2 else "",
                        nulls=sort_expr_list[2] if len(sort_expr_list) == 3 else "",
                    ),
                )

    combined_ast.select.organization = ast.Organization(orderby_sort_items)

    if limit is not None:
        combined_ast.select.limit = ast.Number(limit)
    return combined_ast


def build_temp_select(temp_query: str):
    """
    Builds an intermediate select ast used to build cube queries
    """
    temp_select = parse(temp_query).select

    for col in temp_select.find_all(ast.Column):
        dimension_column = col.identifier(False).replace(
            SEPARATOR,
            f"_{LOOKUP_CHARS.get(SEPARATOR)}_",
        )
        col.name = ast.Name(dimension_column)
    return temp_select


def build_materialized_cube_node(
    selected_metrics: List[Column],
    selected_dimensions: List[Column],
    cube: NodeRevision,
    filters: List[str] = None,
    orderby: List[str] = None,
    limit: Optional[int] = None,
) -> ast.Query:
    """
    Build query for a materialized cube node
    """
    combined_ast: ast.Query = ast.Query(
        select=ast.Select(from_=ast.From(relations=[])),
        ctes=[],
    )
    default_materialization_config = [
        materialization
        for materialization in cube.materializations
        if materialization.name == "default"
    ][0].config
    cube_config = GenericCubeConfig.parse_obj(default_materialization_config)

    selected_metric_keys = [col.name for col in selected_metrics]

    # Assemble query for materialized cube based on the previously saved measures
    # combiner expression for each metric
    for metric_key in selected_metric_keys:
        if metric_key in cube_config.measures:  # pragma: no cover
            metric_measures = cube_config.measures[metric_key]
            measures_combiner_ast = parse(f"SELECT {metric_measures.combiner}")
            measures_type_lookup = {
                measure.name: measure.type for measure in metric_measures.measures
            }
            for col in measures_combiner_ast.find_all(ast.Column):
                col.add_type(
                    ColumnType(
                        measures_type_lookup[col.alias_or_name.name],  # type: ignore
                    ),
                )
            combined_ast.select.projection.extend(
                [
                    proj.set_alias(ast.Name(metric_key))
                    for proj in measures_combiner_ast.select.projection
                ],
            )

    # Add in selected dimension attributes to the query
    for selected_dim in selected_dimensions:
        dimension_column = ast.Column(
            name=ast.Name(
                (
                    selected_dim.node_revision().name  # type: ignore
                    + SEPARATOR
                    + selected_dim.name
                ).replace(SEPARATOR, f"_{LOOKUP_CHARS.get(SEPARATOR)}_"),
            ),
        )
        combined_ast.select.projection.append(dimension_column)
        combined_ast.select.group_by.append(dimension_column)

    # Add in filters to the query
    filter_asts = []
    for filter_ in filters:  # type: ignore
        temp_select = build_temp_select(
            f"select * where {filter_}",
        )
        filter_asts.append(temp_select.where)

    if filter_asts:
        # pylint: disable=no-value-for-parameter
        combined_ast.select.where = ast.BinaryOp.And(*filter_asts)

    # Add orderby
    if orderby:
        temp_select = build_temp_select(
            f"select * order by {','.join(orderby)}",
        )
        combined_ast.select.organization = temp_select.organization

    # Add limit
    if limit:
        combined_ast.select.limit = ast.Number(value=limit)

    # Set up FROM clause
    combined_ast.select.from_.relations.append(  # type: ignore
        ast.Relation(primary=ast.Table(ast.Name(cube.availability.table))),  # type: ignore
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
    memoized_queries: Dict[int, ast.Query] = None,
    build_criteria: Optional[BuildCriteria] = None,
) -> ast.Query:
    """
    Determines the optimal way to build the query AST and does so
    """
    memoized_queries = memoized_queries or {}

    start = time.time()
    context = CompileContext(session=session, exception=DJException())
    if hash(query) in memoized_queries:
        query = memoized_queries[hash(query)]  # pragma: no cover
    else:
        query.compile(context)
        memoized_queries[hash(query)] = query
    end = time.time()
    _logger.info("Finished compiling query %s in %s", str(query)[-100:], end - start)

    start = time.time()
    query.build(session, memoized_queries, build_criteria)
    end = time.time()
    _logger.info("Finished building query in %s", end - start)
    return query
