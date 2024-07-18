import collections
import logging
import re
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

logger = logging.getLogger(__name__)


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


@dataclass
class DimensionJoin:
    """
    Info on a dimension join
    """

    join_path: List[ast.Join]
    requested_dimensions: List[str]
    node_query: Optional[ast.Query] = None


async def get_measures_query(
    session: AsyncSession,
    metrics: List[str],
    dimensions: List[str],
    filters: List[str],
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: Optional[User] = None,
    validate_access: access.ValidateAccessFn = None,
    cast_timestamp_to_ms: bool = False,
    include_all_columns: bool = False,
) -> Dict[str, TranslatedSQL]:
    """
    Builds the measures SQL for a set of metrics with dimensions and filters.
    This SQL can be used to produce an intermediate table with all the measures
    and dimensions needed for an analytics database (e.g., Druid).
    """
    from datajunction_server.api.helpers import (  # pylint: disable=import-outside-toplevel
        assemble_column_metadata,
        validate_cube,
    )

    from datajunction_server.construction.build import (  # pylint: disable=import-outside-toplevel
        metrics_to_measures,
        group_metrics_by_parent,
        rename_columns
    )

    engine = (
        await get_engine(session, engine_name, engine_version)
        if engine_name and engine_version
        else None
    )
    build_criteria = BuildCriteria(
        dialect=engine.dialect if engine and engine.dialect else Dialect.SPARK,
    )
    access_control = access.AccessControlStore(
        validate_access=validate_access,
        user=current_user,
        base_verb=access.ResourceRequestVerb.READ,
    )

    if not filters:
        filters = []

    (_, metric_nodes, _, _, _) = await validate_cube(
        session,
        metrics,
        dimensions,
    )
    context = CompileContext(session=session, exception=DJException())
    common_parents = group_metrics_by_parent(metric_nodes)

    # Mapping between each metric node and its measures
    parents_to_measures, _ = await metrics_to_measures(
        session,
        metric_nodes,
    )

    column_name_regex = r"([A-Za-z0-9_\.]+)(\[[A-Za-z0-9_]+\])?"
    matcher = re.compile(column_name_regex)
    dimensions_without_roles = [matcher.findall(dim)[0][0] for dim in dimensions]

    measures_sql_mapping = {}
    for parent_node, _ in common_parents.items():  # type: ignore
        measure_columns, dimensional_columns = [], []
        parent_ast = await build_node(
            session=session,
            node=parent_node.current,
            dimensions=dimensions,
            filters=filters,
            build_criteria=build_criteria,
            include_dimensions_in_groupby=False,
            access_control=access_control,
        )
        print("parent_ast", parent_ast)

        # Select only columns that were one of the necessary measures
        if not include_all_columns:
            parent_ast.select.projection = [
                expr
                for expr in parent_ast.select.projection
                if from_amenable_name(expr.alias_or_name.identifier(False)).split(  # type: ignore
                    SEPARATOR,
                )[
                    -1
                ]
                in parents_to_measures[parent_node.name]
                or from_amenable_name(expr.alias_or_name.identifier(False))  # type: ignore
                in dimensions_without_roles
            ]
        await session.refresh(parent_node.current, ["columns"])
        parent_ast = rename_columns(parent_ast, parent_node.current)

        # Sort the selected columns into dimension vs measure columns and
        # generate identifiers for them
        for expr in parent_ast.select.projection:
            column_identifier = expr.alias_or_name.identifier(False)  # type: ignore
            if from_amenable_name(column_identifier) in dimensions_without_roles:
                dimensional_columns.append(expr)
                expr.set_semantic_type(SemanticType.DIMENSION)  # type: ignore
            else:
                measure_columns.append(expr)
                expr.set_semantic_type(SemanticType.MEASURE)  # type: ignore
        await parent_ast.compile(context)

        # Build translated SQL object
        columns_metadata = [
            assemble_column_metadata(  # pragma: no cover
                cast(ast.Column, col),
            )
            for col in parent_ast.select.projection
        ]
        dependencies, _ = await parent_ast.extract_dependencies(
            CompileContext(session, DJException()),
        )
        measures_sql_mapping[parent_node.name] = TranslatedSQL(
            sql=str(parent_ast),
            columns=columns_metadata,
            dialect=build_criteria.dialect,
            upstream_tables=[
                f"{dep.catalog.name}.{dep.schema_}.{dep.table}"
                for dep in dependencies
                if dep.type == NodeType.SOURCE
            ],
        )
    return measures_sql_mapping


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
) -> ast.Query:
    """
    Determines the optimal way to build the node with the requested set of
    dimensions, filter expressions, order by, and limit clauses.
    """
    logger.info(
        "Building node %s with dimensions %s, filters %s, order by %s, limit %s",
        node.name,
        dimensions,
        filters,
        orderby,
        limit,
    )
    start = time.time()
    if access_control:
        access_control.add_request_by_node(node)

    # Set the dialect by finding available engines for this node, or default to Spark
    build_criteria = build_criteria or get_default_criteria(node)

    if include_dimensions_in_groupby is None:
        include_dimensions_in_groupby = node.type == NodeType.METRIC

    physical_table = get_table_for_node(node, build_criteria, as_query=True)
    if physical_table and not filters and not dimensions:
        return physical_table
    node_ast = (
        await compile_node_ast(session, node) if not physical_table else physical_table
    )

    ##################################
    ## Gather join metadata
    ##################################

    # Keep track of all the dimension nodes that need to be joined based on the
    # requested dimensions and filters
    requested_dimensions = await get_necessary_dimensions(session, node, dimensions)
    dimension_node_joins = await find_dimension_node_joins(
        session,
        node,
        requested_dimensions,
    )
    pushdown_filters, join_filters, dimension_node_joins_ = await process_filters(
        session,
        node,
        node_ast,
        filters,
        dimension_node_joins,
    )

    # Push down any filters and dimensions that don't need joins
    dimension_node_joins.update(dimension_node_joins_)
    node_ast.select.where = combine_filter_conditions(
        node_ast.select.where,
        *pushdown_filters,
    )

    ###################
    ## Build Node CTEs
    ###################

    # Start tracking node query CTEs as they get built
    cte_mapping = {}  # Maps node name to its CTE

    # Build node AST into a CTE
    node_alias = ast.Name(amenable_name(node.name))
    node_ast = await build_ast(
        session,
        node_ast,
        build_criteria=build_criteria,
    )
    node_ast = node_ast.to_cte(node_alias)
    cte_mapping[node.name] = node_ast

    # Initialize the final query AST structure
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

    # Start building the dimension joins and adding them to the CTEs
    for dimension_node, dimension_join in dimension_node_joins.items():
        join_path = dimension_join.join_path
        requested_dimensions = list(dict.fromkeys(dimension_join.requested_dimensions))
        print("joinnig dimension_node", dimension_node)

        for link in join_path:
            if all(dim in link.foreign_keys_reversed for dim in requested_dimensions):
                continue

            if link.dimension.name in cte_mapping:
                dimension_join.node_query = cte_mapping[link.dimension.name]
                continue

            dimension_node_query = await build_dimension_node_query(
                session,
                build_criteria,
                link,
                join_filters,
                cte_mapping,
            )
            dimension_join.node_query = convert_to_cte(
                dimension_node_query,
                final_ast,
                link.dimension.name,
            )
            # 5c. Add it to the list of CTEs
            cte_mapping[link.dimension.name] = dimension_join.node_query
            final_ast.ctes.append(dimension_join.node_query)

            # 5d. Build the join statement
            join_ast = build_join_for_link(
                link,
                cte_mapping,
                dimension_node_query,
            )
            final_ast.select.from_.relations[-1].extensions.append(join_ast)
            print("5. Building CTE for dimension join", final_ast)

        # Add the requested dimensions to the final SELECT
        dimensions_columns = build_requested_dimensions_columns(
            requested_dimensions,
            link,
            dimension_node_joins,
        )
        final_ast.select.projection.extend(dimensions_columns)

    # Add remaining join filters to the where clause
    for filter_ast in join_filters:
        filter_ast = replace_filter_dimension_refs(filter_ast, dimension_node_joins)
    final_ast.select.where = combine_filter_conditions(
        final_ast.select.where,
        *join_filters,
    )
    print("Elapsed", time.time() - start)
    return final_ast


async def get_necessary_dimensions(
    session: AsyncSession,
    node: NodeRevision,
    dimensions: List[str],
):
    """
    Returns the necessary dimensions for this node, which are a combination of the requested
    dimensions and the node's required dimensions
    """
    await session.refresh(node, ["required_dimensions"])
    return list(
        dict.fromkeys(
            dimensions or [] + [required.name for required in node.required_dimensions],
        ),
    )


async def find_dimension_node_joins(
    session: AsyncSession,
    node: NodeRevision,
    requested_dimensions: List[str],
) -> Dict[str, DimensionJoin]:
    """
    Returns a list of dimension node joins that are necessary based on
    the requested dimensions.
    """
    dimension_node_joins = {}
    for dim in requested_dimensions:
        dimension_attr = FullColumnName(dim)
        dim_node = dimension_attr.node_name
        if dim_node not in dimension_node_joins:
            join_path = await dimension_join_path(session, node, dimension_attr.name)
            if not join_path:
                raise DJException(
                    f"This dimension attribute cannot be joined in: {dimension_attr.name}. "
                    f"Please make sure that {dimension_attr.node_name} is linked to {node.name}",
                )
            if await needs_dimension_join(session, dimension_attr.name, join_path):
                dimension_node_joins[dim_node] = DimensionJoin(
                    join_path=join_path,
                    requested_dimensions=[dimension_attr.name],
                )
        else:
            if dim not in dimension_node_joins[dim_node].requested_dimensions:
                dimension_node_joins[dim_node].requested_dimensions.append(dim)
    return dimension_node_joins


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


async def build_dimension_node_query(
    session: AsyncSession,
    build_criteria: BuildCriteria,
    link: DimensionLink,
    join_filters: List[str],
    cte_mapping: Dict[str, ast.Query],
):
    """
    Builds a dimension node query with the requested filters
    """
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
    print("1a. Building CTE for", link.dimension.name)
    dimension_node_query = await build_ast(
        session,
        dimension_node_ast,
        build_criteria=build_criteria,
        ctes_mapping=cte_mapping,
    )
    return dimension_node_query


def convert_to_cte(
    inner_query: ast.Query,
    outer_query: ast.Query,
    cte_name: str,
):
    """
    Convert the query to a CTE that can be used by the outer query
    """
    # Move all the CTEs used by the inner query to the outer query
    for cte in inner_query.ctes:
        cte.set_parent(outer_query, parent_key="ctes")
    outer_query.ctes.extend(inner_query.ctes)
    inner_query.ctes = []

    # Convert the dimension node query to a CTE
    inner_query = inner_query.to_cte(
        ast.Name(amenable_name(cte_name)),
        outer_query,
    )
    return inner_query


def build_requested_dimensions_columns(
    requested_dimensions,
    link,
    dimension_node_joins,
):
    dimensions_columns = []
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
                    dimensions_columns.append(
                        ast.Column(
                            name=ast.Name(col.alias_or_name.name),
                            # **({"alias": ast.Name(amenable_name(dim))} if full_dimension_attr.column_name in final_columns else {}),
                            alias=ast.Name(amenable_name(dim)),
                            _table=dimension_node_joins[dim_node].node_query,
                            _type=col.type,
                        ),
                    )
    return dimensions_columns


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


def replace_filter_dimension_refs(
    filter_ast: ast.Expression,
    dimension_node_joins: Dict[str, ast.Query],
):
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
    return filter_ast


async def needs_dimension_join(
    session: AsyncSession,
    dimension_attribute: str,
    join_path: List["DimensionLink"],
) -> bool:
    """
    Checks if the requested dimension attribute needs a dimension join or
    if it can be pulled from an existing column on the node.
    """
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
    """
    Combines the existing where clause with new filter conditions.
    """
    if not existing_condition and not new_conditions:
        return None
    if not existing_condition:
        return ast.BinaryOp.And(*new_conditions)
    return ast.BinaryOp.And(existing_condition, *new_conditions)


def build_join_for_link(
    link: "DimensionLink",
    cte_mapping: Dict[str, ast.Query],
    join_right: ast.Query,
):
    """
    Build a join for the dimension link using the provided query table expression
    on the left and the provided query table expression on the right.
    """
    join_ast = link.joins()[0]
    join_ast.right = join_right.alias
    dimension_node_columns = join_right.select.column_mapping
    join_left = cte_mapping.get(link.node_revision.name)
    node_columns = join_left.select.column_mapping
    for col in join_ast.criteria.find_all(ast.Column):
        full_column = FullColumnName(col.identifier())
        is_dimension_node = full_column.node_name == link.dimension.name
        replacement = ast.Column(
            name=ast.Name(full_column.column_name),
            _table=join_right if is_dimension_node else join_left,
            _type=(dimension_node_columns if is_dimension_node else node_columns)
            .get(full_column.column_name)
            .type,
        )
        col.parent.replace(col, replacement)
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
    """
    Sort filter expressions into:
    * pushdown filters: these filter expressions reference dimensions that do not
      require a join and therefore can be "pushed" down
    * join filters: these filters require one or more dimension node joins
    Also returns the necessary dimension node joins
    """
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


async def build_ast(  # pylint: disable=too-many-arguments
    session: AsyncSession,
    query: ast.Query,
    memoized_queries: Dict[int, ast.Query] = None,
    build_criteria: Optional[BuildCriteria] = None,
    access_control=None,
    ctes_mapping: Dict[str, ast.Query] = None,
) -> ast.Query:
    """
    Builds the query AST, which involves replacing each of the DJ node references in
    the query with their ASTs (either the referenced node's query or a materialized table).
    """
    context = CompileContext(session=session, exception=DJException())
    await query.compile(context)
    query.bake_ctes()  # pylint: disable=W0212

    new_cte_mapping = {}

    # Transforms a select expression by replacing dj node references with their ASTs
    node_to_tables_mapping = _get_tables_from_select(query.select)
    for referenced_node, table_expressions in node_to_tables_mapping.items():
        await session.refresh(referenced_node, ["dimension_links"])

        # Try to find a materialized table attached to this node, if one exists.
        table_reference = cast(
            Optional[ast.Table],
            get_table_for_node(referenced_node, build_criteria),
        )
        if not table_reference:
            if referenced_node.name not in ctes_mapping:
                # Build a new CTE with the query AST otherwise
                node_query = parse(cast(str, referenced_node.query))
                query_ast = await build_ast(  # type: ignore
                    session,
                    node_query,
                    memoized_queries,
                    build_criteria,
                    access_control,
                )
                cte_name = ast.Name(amenable_name(referenced_node.name))
                query_ast = query_ast.to_cte(cte_name, parent_ast=query)
                new_cte_mapping[referenced_node.name] = query_ast

            reference_cte = (
                ctes_mapping[referenced_node.name]
                if referenced_node.name in ctes_mapping
                else new_cte_mapping[referenced_node.name]
            )
            query_ast = ast.Table(
                reference_cte.alias,
                _columns=reference_cte._columns,
                _dj_node=referenced_node,
            )
        else:
            alias = amenable_name(referenced_node.name)
            query_ast = ast.Alias(ast.Name(alias), child=table_reference, as_=True)  # type: ignore

        for table in table_expressions:
            query.select.replace(
                table,
                query_ast,
                copy=False,
            )
            await query.select.compile(context)
            for col in query.select.find_all(ast.Column):
                if (
                    col._table
                    and isinstance(col._table, ast.Table)
                    and col._table.dj_node
                    and col._table.dj_node.name == referenced_node.name
                ):
                    col._table = query_ast
    query.ctes.extend(new_cte_mapping.values())
    query.select.add_aliases_to_unnamed_columns()
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


def get_table_for_node(
    node: NodeRevision,
    build_criteria: Optional[BuildCriteria] = None,
    as_query: bool = False,
) -> Optional[Union[ast.Select, ast.Table]]:
    """
    If a node has a materialized table available, return the materialized table.
    Source nodes should always have an associated table, whereas for all other nodes
    we can check the materialization type.
    """
    table = None
    can_use_materialization = (
        build_criteria and node.name != build_criteria.target_node_name
    )
    if node.type == NodeType.SOURCE:
        table_name = (
            f"{node.catalog.name}.{node.schema_}.{node.table}"
            if node.schema_ == "iceberg"
            else f"{node.schema_}.{node.table}"
        )
        name = to_namespaced_name(table_name if node.table else node.name)
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
    if table and as_query:  # pragma: no cover
        return ast.Query(
            select=ast.Select(
                projection=table.columns,  # type: ignore
                from_=ast.From(relations=[ast.Relation(table)]),
            ),
        )
    return table
