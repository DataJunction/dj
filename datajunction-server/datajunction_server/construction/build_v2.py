import collections
import time
from dataclasses import dataclass
from typing import DefaultDict, Dict, List, Optional, Union, cast

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from datajunction_server.construction.utils import to_namespaced_name
from datajunction_server.database import Engine
from datajunction_server.database.column import Column
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJError,
    DJException,
    DJInvalidInputException,
    ErrorCode,
)
from datajunction_server.internal.engines import get_engine
from datajunction_server.models import access
from datajunction_server.models.column import SemanticType
from datajunction_server.models.engine import Dialect
from datajunction_server.models.materialization import GenericCubeConfig
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node import BuildCriteria
from datajunction_server.models.node_type import NodeType
from datajunction_server.naming import LOOKUP_CHARS, amenable_name, from_amenable_name
from datajunction_server.sql.dag import get_dimensions, get_shared_dimensions
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse
from datajunction_server.sql.parsing.types import (
    ColumnType,
    DoubleType,
    LongType,
    TimestampType,
)
from datajunction_server.utils import SEPARATOR


@dataclass
class FullColumnName:
    """
    A fully qualified column name with the node name and the column.
    """

    name: str

    @property
    def node_name(self):
        return SEPARATOR.join(self.name.split(SEPARATOR)[:-1])

    @property
    def column_name(self):
        return self.name.split(SEPARATOR)[-1]


async def dimension_join_path(
    session: AsyncSession,
    node: NodeRevision,
    dimension: str,
) -> Optional[List[DimensionLink]]:
    """
    Find a join path between this node and the dimension attribute.
    * If there is no possible join path, returns None
    * If it is a local dimension on this node, return []
    * If it is in one of the dimension nodes on the dimensions graph, return a
    list of dimension links that represent the join path
    """
    # Check if it is a local dimension
    if dimension.startswith(node.name):
        for col in node.columns:
            if f"{node.name}.{col.name}" == dimension and col.is_dimensional():
                return []

    dimension_attr = FullColumnName(dimension)

    # If it's not a local dimension, traverse the node's dimensions graph
    # This queue tracks the dimension link being processed and the path to that link
    await session.refresh(node, ["dimension_links"])

    # Start with first layer of linked dims
    processing_queue = collections.deque(
        [(link, [link]) for link in node.dimension_links],
    )
    while processing_queue:
        current_link, join_path = processing_queue.pop()
        await session.refresh(current_link, ["dimension"])
        if current_link.dimension.name == dimension_attr.node_name:
            return join_path
        await session.refresh(current_link.dimension, ["current"])
        await session.refresh(current_link.dimension.current, ["dimension_links"])
        processing_queue.extend(
            [
                (link, join_path + [link])
                for link in current_link.dimension.current.dimension_links
            ],
        )
    return None


@dataclass
class DimensionJoin:
    join_path: List[ast.Join]
    requested_dimensions: List[str]
    node_query: Optional[ast.Query] = None


async def compile_node_ast(session, node_revision: NodeRevision) -> ast.Query:
    """
    Parses the node's query into an AST and compiles it.
    """
    node_ast = parse(node_revision.query)
    ctx = CompileContext(session, DJException())
    await node_ast.compile(ctx)
    return node_ast


def update_filter_column_with_foreign_key(
    dimension_attr: str,  # this is the canonical dimension
    filter_dim: ast.Column,  # this is the referenced dimension column from a filter expression
    node_ast: ast.Query,
    link: "DimensionLink",
):
    """
    Modifies the referenced dimension in the filter expression with the foreign key column from the node.
    """
    foreign_key = link.foreign_keys_reversed[dimension_attr]
    for node_col in node_ast.select.projection:
        fk_column = FullColumnName(foreign_key)
        if node_col.alias_or_name.identifier() == fk_column.column_name:
            filter_dim.parent.replace(
                filter_dim,
                node_col.child if isinstance(node_col, ast.Alias) else node_col,
            )
            filter_dim.dimension_ref = dimension_attr


async def needs_dimension_join(
    session: AsyncSession,
    dimension_attribute: str,
    join_path: List["DimensionLink"],
) -> bool:
    if len(join_path) == 1:
        link = join_path[0]
        await session.refresh(link.dimension, ["current"])
        await session.refresh(link.dimension.current, ["columns"])
        if dimension_attribute in link.foreign_keys_reversed:
            return False
    return True


def combine_filter_conditions(
    existing_condition, *new_conditions
) -> Optional[ast.BinaryOp]:
    """Combines the existing where clause with"""
    if not existing_condition and not new_conditions:
        return None
    if not existing_condition:
        return ast.BinaryOp.And(*new_conditions)
    return ast.BinaryOp.And(existing_condition, *new_conditions)


def build_join_for_link(
    link: "DimensionLink",
    join_left: ast.Query,
    join_right: ast.Query,
):
    """
    Build a join for the dimension link using the provided query table expression
    on the left and the provided query table expression on the right.
    """
    join_ast = link.joins()[0]
    join_ast.right = join_right.alias
    dimension_node_columns = join_right.select.column_mapping
    node_columns = join_left.select.column_mapping
    for col in join_ast.criteria.find_all(ast.Column):
        full_column = FullColumnName(col.identifier())
        is_dimension_node = full_column.node_name == link.dimension.name
        col.parent.replace(
            col,
            ast.Column(
                name=ast.Name(full_column.column_name),
                _table=join_right if is_dimension_node else join_left,
                _type=(dimension_node_columns if is_dimension_node else node_columns)
                .get(full_column.column_name)
                .type,
            ),
        )
    return join_ast


def get_default_criteria(
    node: NodeRevision,
    engine: Optional[Engine] = None,
) -> BuildCriteria:
    """
    Get the default build criteria for a node.
    """
    # Set the dialect by using the provided engine, if any. If no engine is specified,
    # set the dialect by finding available engines for this node, or default to Spark
    dialect = (
        engine.dialect
        if engine
        else (
            node.catalog.engines[0].dialect
            if node.catalog and node.catalog.engines and node.catalog.engines[0].dialect
            else Dialect.SPARK
        )
    )
    return BuildCriteria(
        dialect=dialect,
        target_node_name=node.name,
    )


async def process_filters(
    session: AsyncSession,
    node: NodeRevision,
    node_ast: ast.Query,
    filters: List[str],
    dimension_node_joins: Optional[Dict[str, DimensionJoin]] = None,
):
    filter_asts = [
        parse(f"select * where {filter_}").select.where for filter_ in filters or []
    ]
    pushdown_filters, join_filters = [], []
    if not dimension_node_joins:
        dimension_node_joins = {}
    for filter_ast in filter_asts:
        # This mapping keeps track of each referenced dimension in the filter expression
        # and whether it needs a join to pull it in or if it can reuse a column on the node
        join_required = {}

        for filter_dim in filter_ast.find_all(ast.Column):
            dimension_attr = FullColumnName(filter_dim.identifier())
            join_required[dimension_attr.name] = True
            if dimension_attr.node_name not in dimension_node_joins:
                join_path = await dimension_join_path(
                    session,
                    node,
                    dimension_attr.name,
                )
                # print("links!", node.name, dimension_node, join_path)
                if join_path:
                    join_required[dimension_attr.name] = await needs_dimension_join(
                        session,
                        dimension_attr.name,
                        join_path,
                    )
                    # Only add it to the dimension node joins tracker if a join is needed
                    if join_required[dimension_attr.name]:
                        dimension_node_joins[dimension_attr.node_name] = DimensionJoin(
                            join_path=await dimension_join_path(
                                session,
                                node,
                                dimension_attr.name,
                            ),
                            requested_dimensions=[dimension_attr.name],
                        )
            else:
                if (
                    dimension_attr.name
                    not in dimension_node_joins[
                        dimension_attr.node_name
                    ].requested_dimensions
                ):
                    dimension_node_joins[
                        dimension_attr.node_name
                    ].requested_dimensions.append(
                        dimension_attr.name,
                    )

            # If it matches FK then update
            if not join_required[dimension_attr.name] and join_path:
                update_filter_column_with_foreign_key(
                    dimension_attr.name,
                    filter_dim,
                    node_ast,
                    join_path[0],
                )

        # If the entire filter expression does not need any joins, add it to the node
        if not any(join_required[req] for req in join_required):
            pushdown_filters.append(filter_ast)
        else:
            join_filters.append(filter_ast)
        print("pushdown", [str(f) for f in pushdown_filters])
        print("join", [str(f) for f in pushdown_filters])
    return pushdown_filters, join_filters, dimension_node_joins


async def build_node(  # pylint: disable=too-many-arguments
    session: AsyncSession,
    node: NodeRevision,
    filters: Optional[List[str]] = None,
    dimensions: Optional[List[str]] = None,
    orderby: Optional[List[str]] = None,
    limit: Optional[int] = None,
    build_criteria: Optional[BuildCriteria] = None,
    access_control: Optional[access.AccessControlStore] = None,
    include_dimensions_in_groupby: bool = None,
    top_level: bool = False,
) -> ast.Query:
    """
    Determines the optimal way to build the Node and does so
    """
    start = time.time()
    print("1. Building node", node.name, "with dimensions", dimensions)
    if access_control:
        access_control.add_request_by_node(node)

    if include_dimensions_in_groupby is None:
        include_dimensions_in_groupby = node.type == NodeType.METRIC

    # Set the dialect by finding available engines for this node, or default to Spark
    build_criteria = build_criteria or get_default_criteria(node)

    physical_table = _get_node_table(node, build_criteria, as_select=True)
    if physical_table:
        return physical_table
    node_ast = await compile_node_ast(session, node)

    # Keep track of all the dimension nodes that need to be joined based on the
    # requested dimensions and filters
    dimension_node_joins: Dict[str, DimensionJoin] = {}
    await session.refresh(node, ["required_dimensions"])
    requested_dimensions = list(
        set(
            dimensions or [] + [required.name for required in node.required_dimensions],
        ),
    )
    for dim in requested_dimensions:
        dimension_attr = FullColumnName(dim)
        dim_node = dimension_attr.node_name
        if dim_node not in dimension_node_joins:
            join_path = await dimension_join_path(session, node, dimension_attr.name)
            if await needs_dimension_join(session, dimension_attr.name, join_path):
                dimension_node_joins[dim_node] = DimensionJoin(
                    join_path=join_path,
                    requested_dimensions=[dimension_attr.name],
                )
        else:
            if dim not in dimension_node_joins[dim_node].requested_dimensions:
                dimension_node_joins[dim_node].requested_dimensions.append(dim)

    pushdown_filters, join_filters, dimension_node_joins_ = await process_filters(
        session,
        node,
        node_ast,
        filters,
        dimension_node_joins,
    )
    dimension_node_joins.update(dimension_node_joins_)
    node_ast.select.where = combine_filter_conditions(
        node_ast.select.where,
        *pushdown_filters,
    )
    print("2. node_ast after adding filters and dims that dont need join", node_ast)

    # Collect all necessary CTEs: the node query and the dimension node joins
    node_alias = ast.Name(amenable_name(node.name))
    node_ast = await build_ast(
        session,
        node_ast,
        build_criteria=build_criteria,
    )
    node_ast = node_ast.to_cte(node_alias)

    print("3. node_ast after calling build_ast", node_ast)
    final_ast = ast.Query(
        select=ast.Select(
            projection=[
                ast.Column(
                    ast.Name(col.alias_or_name.name),
                    _table=node_ast,
                    _type=col.type,
                )
                for col in node_ast.select.projection
            ],
            from_=ast.From(relations=[ast.Relation(node_alias)]),
        ),
        ctes=[node_ast],
    )
    print("4. final_ast with node CTE", final_ast)

    # Start building the joins using the CTEs
    cte_mapping = {}  # Track dimension node CTEs that have been added
    join_left = node_ast
    for dimension_node, dimension_join in dimension_node_joins.items():
        join_path = dimension_join.join_path
        requested_dimensions = list(dict.fromkeys(dimension_join.requested_dimensions))

        for link in join_path:
            if all(dim in link.foreign_keys_reversed for dim in requested_dimensions):
                continue

            if link.dimension.name in cte_mapping:
                dimension_join.node_query = cte_mapping[link.dimension.name]
                continue

            await session.refresh(link.dimension, ["current"])
            await session.refresh(link.dimension.current, ["columns"])
            # 1. Build the dimension node
            dimension_node_ast = await compile_node_ast(session, link.dimension.current)
            pushdown_filters, join_filters, _ = await process_filters(
                session,
                link.dimension.current,
                dimension_node_ast,
                join_filters,
            )
            dimension_node_ast.select.where = combine_filter_conditions(
                dimension_node_ast.select.where,
                *pushdown_filters,
            )
            dimension_node_query = await build_ast(
                session,
                dimension_node_ast,
                build_criteria=build_criteria,
            )
            # 1a. Convert it to a CTE
            dimension_node_query = dimension_node_query.to_cte(
                ast.Name(amenable_name(link.dimension.name)),
                final_ast,
            )
            dimension_join.node_query = dimension_node_query

            # 2. Add it to the list of CTEs
            cte_mapping[link.dimension.name] = dimension_node_query
            final_ast.ctes.append(dimension_node_query)

            # 3. Build the join statement
            join_ast = build_join_for_link(
                link,
                join_left,
                dimension_node_query,
            )
            final_ast.select.from_.relations[-1].extensions.append(join_ast)
            print("5b. Building CTE for dimension join", final_ast)
            join_left = dimension_node_query

        # Add the requested dimensions to the final SELECT
        for dim in requested_dimensions:
            full_dimension_attr = FullColumnName(dim)
            dim_node = full_dimension_attr.node_name
            # Check if it's a foreign key reference on one of the dimensions
            foreign_key_column_name = None
            if dim in link.foreign_keys_reversed:
                foreign_key_column_name = FullColumnName(
                    link.foreign_keys_reversed[dim],
                ).column_name

            if dim_node in dimension_node_joins:
                for col in dimension_node_joins[dim_node].node_query.select.projection:
                    if col.alias_or_name.name == full_dimension_attr.column_name or (
                        foreign_key_column_name
                        and col.alias_or_name.identifier() == foreign_key_column_name
                    ):
                        final_ast.select.projection.append(
                            ast.Column(
                                name=ast.Name(col.alias_or_name.name),
                                # **({"alias": ast.Name(amenable_name(dim))} if full_dimension_attr.column_name in final_columns else {}),
                                alias=ast.Name(amenable_name(dim)),
                                _table=dimension_node_joins[dim_node].node_query,
                                _type=col.type,
                            ),
                        )
    # Add remaining join filters to the where clause
    for filter_ast in join_filters:
        for filter_dim in filter_ast.find_all(ast.Column):
            dimension_attr = FullColumnName(filter_dim.identifier())
            dim_node = dimension_attr.node_name
            for col in dimension_node_joins[dim_node].node_query.select.projection:
                if col.alias_or_name.name == dimension_attr.column_name:
                    filter_dim.parent.replace(
                        filter_dim,
                        ast.Column(
                            name=ast.Name(col.alias_or_name.name),
                            # alias=ast.Name(amenable_name(filter_dim.identifier())),
                            _table=dimension_node_joins[dim_node].node_query,
                            _type=col.type,
                        ),
                    )
        final_ast.select.where = combine_filter_conditions(
            final_ast.select.where,
            filter_ast,
        )
    print("5. final_ast after joins", final_ast)
    print("Elapse", time.time() - start)
    return final_ast


async def build_ast(  # pylint: disable=too-many-arguments
    session: AsyncSession,
    query: ast.Query,
    memoized_queries: Dict[int, ast.Query] = None,
    build_criteria: Optional[BuildCriteria] = None,
    access_control=None,
) -> ast.Query:
    """
    Determines the optimal way to build the query AST and does so.
    """
    memoized_queries = memoized_queries or {}

    start = time.time()
    context = CompileContext(session=session, exception=DJException())
    hashed_query = hash(query)
    if hashed_query in memoized_queries:
        query = memoized_queries[hashed_query]  # pragma: no cover
    else:
        await query.compile(context)
        memoized_queries[hashed_query] = query
    end = time.time()
    # _logger.info("Finished compiling query %s in %s", str(query)[-100:], end - start)

    start = time.time()

    query.bake_ctes()  # pylint: disable=W0212
    await _build_select_ast(
        session,
        query.select,
        memoized_queries,
        build_criteria,
        access_control,
    )
    query.select.add_aliases_to_unnamed_columns()

    end = time.time()
    # _logger.info("Finished building query in %s", end - start)
    return query


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


# flake8: noqa: C901
async def _build_select_ast(
    session: AsyncSession,
    select: ast.SelectExpression,
    memoized_queries: Dict[int, ast.Query],
    build_criteria: Optional[BuildCriteria] = None,
    access_control=None,
):
    """
    Transforms a select ast by replacing dj node references with their asts
    Starts by extracting all dimensions-backed columns from filters + group bys.
    Some of them can be sourced directly from tables on the select, others cannot
    For the ones that cannot be sourced directly, attempt to join them via dimension links.
    """
    tables = _get_tables_from_select(select)
    await _build_tables_on_select(
        session,
        select,
        tables,
        memoized_queries,
        build_criteria,
        access_control,
    )


def _get_node_table(
    node: NodeRevision,
    build_criteria: Optional[BuildCriteria] = None,
    as_select: bool = False,
) -> Optional[Union[ast.Select, ast.Table]]:
    """
    If a node has a materialization available, return the materialized table
    """
    table = None
    can_use_materialization = (
        build_criteria and node.name != build_criteria.target_node_name
    )
    if node.type == NodeType.SOURCE:
        if node.table:
            name = ast.Name(
                node.table,
                namespace=ast.Name(
                    node.schema_,
                    namespace=ast.Name(node.catalog.name)
                    if node.schema_ == "iceberg"
                    else None,
                )
                if node.schema_
                else None,
            )
        else:
            name = to_namespaced_name(node.name)
        table = ast.Table(
            name,
            _columns=[
                ast.Column(name=ast.Name(col.name), _type=col.type)
                for col in node.columns
            ],
            _dj_node=node,
        )
    elif (
        can_use_materialization
        and node.availability
        and node.availability.is_available(
            criteria=build_criteria,
        )
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
            _columns=[
                ast.Column(name=ast.Name(col.name), _type=col.type)
                for col in node.columns
            ],
            _dj_node=node,
        )
    if table and as_select:  # pragma: no cover
        return ast.Select(
            projection=table.columns,  # type: ignore
            from_=ast.From(relations=[ast.Relation(table)]),
        )
    return table


async def _build_tables_on_select(
    session: AsyncSession,
    select: ast.SelectExpression,
    tables: Dict[NodeRevision, List[ast.Table]],
    memoized_queries: Dict[int, ast.Query],
    build_criteria: Optional[BuildCriteria] = None,
    access_control=None,
):
    """
    Add all nodes referenced in the SELECT expression
    """
    context = CompileContext(session=session, exception=DJException())

    # `tables` is a mapping between DJ nodes and table expressions on the query AST
    # If there is more than one table expression on the AST, this means the same DJ
    # node is referenced multiple times in the query, likely through being joined
    # more than once.
    for referenced_node, tbls in tables.items():
        await session.refresh(referenced_node, ["dimension_links"])

        # Try to find a physical table attached to this node, if one exists.
        physical_table = cast(
            Optional[ast.Table],
            _get_node_table(referenced_node, build_criteria),
        )

        for tbl in tbls:
            # If no attached physical table was found, recursively build the node
            if physical_table is None:
                node_query = parse(cast(str, referenced_node.query))
                if hash(node_query) in memoized_queries:  # pragma: no cover
                    query_ast = memoized_queries[hash(node_query)]  # type: ignore
                else:
                    query_ast = await build_ast(  # type: ignore
                        session,
                        node_query,
                        memoized_queries,
                        build_criteria,
                        access_control,
                    )
                    memoized_queries[hash(node_query)] = query_ast

                alias = amenable_name(referenced_node.name)
                node_ast = ast.Alias(ast.Name(alias), child=query_ast, as_=True)  # type: ignore
            else:
                alias = amenable_name(referenced_node.name)
                node_ast = ast.Alias(ast.Name(alias), child=physical_table, as_=True)  # type: ignore

            select.replace(
                tbl,
                node_ast,
                copy=False,
            )
            await select.compile(context)
            for col in select.find_all(ast.Column):
                if (
                    col._table
                    and isinstance(col._table, ast.Table)
                    and col._table.dj_node
                    and col._table.dj_node.name == referenced_node.name
                ):
                    col._table = node_ast
