"""Building node SQL functions"""
import collections
import logging
import re
from dataclasses import dataclass
from typing import DefaultDict, Dict, List, Optional, Union, cast

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.utils import to_namespaced_name
from datajunction_server.database import Engine
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import DJException
from datajunction_server.internal.engines import get_engine
from datajunction_server.models import access
from datajunction_server.models.column import SemanticType
from datajunction_server.models.engine import Dialect
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node import BuildCriteria
from datajunction_server.models.node_type import NodeType
from datajunction_server.naming import amenable_name, from_amenable_name
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse
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
        """
        Gets the node name part of the full column name.
        """
        return SEPARATOR.join(self.name.split(SEPARATOR)[:-1])

    @property
    def column_name(self):
        """
        Gets the column name part of the full column name.
        """
        return self.name.split(SEPARATOR)[-1]


@dataclass
class DimensionJoin:
    """
    Info on a dimension join
    """

    join_path: List[DimensionLink]
    requested_dimensions: List[str]
    node_query: Optional[ast.Query] = None


async def get_measures_query(  # pylint: disable=too-many-locals
    session: AsyncSession,
    metrics: List[str],
    dimensions: List[str],
    filters: List[str],
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    current_user: Optional[User] = None,
    validate_access: access.ValidateAccessFn = None,
    cast_timestamp_to_ms: bool = False,  # pylint: disable=unused-argument
    include_all_columns: bool = False,
) -> Dict[str, TranslatedSQL]:
    """
    Builds the measures SQL for a set of metrics with dimensions and filters.

    Measures queries are generated at the grain of each of the metrics' upstream nodes.
    For example, if some of your metrics are aggregations on measures in parent node A
    and others are aggregations on measures in parent node B, this function will return a
    dictionary that maps A to the measures query for A, and B to the measures query for B.
    """
    from datajunction_server.api.helpers import (  # pylint: disable=import-outside-toplevel
        assemble_column_metadata,
        validate_cube,
    )
    from datajunction_server.construction.build import (  # pylint: disable=import-outside-toplevel
        group_metrics_by_parent,
        metrics_to_measures,
        rename_columns,
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


async def build_node(  # pylint: disable=too-many-arguments,too-many-locals
    session: AsyncSession,
    node: NodeRevision,
    filters: Optional[List[str]] = None,
    dimensions: Optional[List[str]] = None,
    orderby: Optional[List[str]] = None,  # pylint: disable=unused-argument
    limit: Optional[int] = None,  # pylint: disable=unused-argument
    build_criteria: Optional[BuildCriteria] = None,
    access_control: Optional[access.AccessControlStore] = None,
    include_dimensions_in_groupby: bool = None,
) -> ast.Query:
    """
    Builds the node SQL with the requested set of dimensions, filter expressions,
    order by, and limit clauses.

    Build Strategy
    ---------------
    1. Recursively turn node references into query ASTs + apply any filters that can
    be pushed down. If the node query has CTEs, unwind them into the query.
    2. Initialize the final query with the node's query AST added to it as a CTE.
    3. For any dimensions or filters requested for the node, determine if a join is
    needed to bring in the dimension or filter. Keep track of all the necessary dimension
    joins in a dict that maps dimension nodes to join paths.
    4. For each of the necessary dimension joins, build the dimension node's query in the
    same manner as above, recursively replacing any node references and pushing down requested
    filters where possible.
    5. Add each dimension node's query AST to the final query as a CTE.
    6. Build the final query using the various CTEs. This does all the joins between the node
    query AST and the dimension nodes' ASTs using the join logic from the dimension links.
    7. Add all requested dimensions to the final select.
    8. Add order by and limit to the final select (TODO)
    """
    if access_control:
        access_control.add_request_by_node(node)

    build_criteria = build_criteria or get_default_criteria(node)

    physical_table = get_table_for_node(
        node,
        build_criteria=build_criteria,
    )
    if physical_table and not filters and not dimensions:
        physical_table.parenthesized = False
        return ast.Query(
            select=ast.Select(
                projection=physical_table.columns,  # type: ignore
                from_=ast.From(relations=[ast.Relation(physical_table)]),
            ),
        )

    if include_dimensions_in_groupby is None:
        include_dimensions_in_groupby = node.type == NodeType.METRIC

    node_ast = (
        await compile_node_ast(session, node)
        if not physical_table
        else ast.Query(
            select=ast.Select(
                projection=physical_table.columns,  # type: ignore
                from_=ast.From(relations=[ast.Relation(physical_table)]),
            ),
        )
    )

    ##################################
    ## Gather join metadata
    ##################################

    # Augment the dimensions to request based on the node's required dimensions
    # and the user-requested dimensions
    requested_dimensions = await get_necessary_dimensions(
        session,
        node,
        dimensions or [],
    )

    # Find all dimension node joins necessary for the requested dimensions and filters
    dimension_node_joins = await find_dimension_node_joins(
        session,
        node,
        requested_dimensions,
        filters,
    )

    ###################
    ## Build Node CTEs
    ###################

    # Start tracking node query CTEs as they get built
    cte_mapping: Dict[str, ast.Query] = {}  # Maps node name to its CTE

    # Build node AST into a CTE
    ctx = CompileContext(session, DJException())
    await node_ast.compile(ctx)
    node_alias = ast.Name(amenable_name(node.name))
    node_ast = await build_ast(
        session,
        node,
        node_ast,
        filters=filters,
        build_criteria=build_criteria,
        ctes_mapping=cte_mapping,
    )

    # Initialize the final query AST structure
    final_ast = ast.Query(
        select=ast.Select(
            projection=[
                ast.Column(
                    ast.Name(col.alias_or_name.name),  # type: ignore
                    _table=node_ast,
                    _type=col.type,  # type: ignore
                )
                for col in node_ast.select.projection
            ],
            from_=ast.From(relations=[ast.Relation(node_alias)]),  # type: ignore
        ),
        ctes=[*node_ast.ctes, node_ast],
    )
    node_ast.ctes = []
    node_ast = node_ast.to_cte(node_alias)
    cte_mapping[node.name] = node_ast

    # Start building the dimension joins and adding them to the CTEs
    for _, dimension_join in dimension_node_joins.items():
        join_path = dimension_join.join_path
        requested_dimensions = list(dict.fromkeys(dimension_join.requested_dimensions))

        for link in join_path:
            link = cast(DimensionLink, link)
            if all(
                dim in link.foreign_keys_reversed for dim in requested_dimensions
            ):  # pylint: disable=line-too-long # pragma: no cover
                continue  # pragma: no cover

            if link.dimension.name in cte_mapping:
                dimension_join.node_query = cte_mapping[link.dimension.name]
                continue

            dimension_node_query = await build_dimension_node_query(
                session,
                build_criteria,
                link,
                to_filter_asts(filters),
                cte_mapping,
            )
            dimension_join.node_query = convert_to_cte(
                dimension_node_query,
                final_ast,
                link.dimension.name,
            )
            # Add it to the list of CTEs
            cte_mapping[link.dimension.name] = dimension_join.node_query  # type: ignore
            final_ast.ctes.append(dimension_join.node_query)  # type: ignore

            # Build the join statement
            join_ast = build_join_for_link(
                link,
                cte_mapping,
                dimension_node_query,
            )
            final_ast.select.from_.relations[-1].extensions.append(join_ast)  # type: ignore

        # Add the requested dimensions to the final SELECT
        if join_path:  # pragma: no cover
            dimensions_columns = build_requested_dimensions_columns(
                requested_dimensions,
                join_path[-1],
                dimension_node_joins,
            )
            final_ast.select.projection.extend(dimensions_columns)

    # Mark any remaining requested dimensions that don't need a join with
    # their canonical dimension names
    for dim_name in requested_dimensions:
        column_name = get_column_from_canonical_dimension(dim_name, node)
        node_col = (
            final_ast.select.column_mapping.get(column_name) if column_name else None
        )
        # Realias based on canonical dimension name
        if node_col:
            node_col.set_alias(ast.Name(amenable_name(dim_name)))
    return final_ast


def get_column_from_canonical_dimension(
    dimension_name: str,
    node: NodeRevision,
) -> Optional[str]:
    """
    Gets a column based on a dimension request on a node.
    """
    column_name = None
    dimension_attr = FullColumnName(dimension_name)
    # Dimension requested was on node
    if dimension_attr.node_name == node.name:
        column_name = dimension_attr.column_name

    # Dimension referenced was foreign key of dimension link
    link = next(
        (
            link
            for link in node.dimension_links
            if dimension_attr.name in link.foreign_keys_reversed
        ),
        None,
    )
    if link:
        column_name = FullColumnName(
            link.foreign_keys_reversed[dimension_attr.name],
        ).column_name
    return column_name


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


def to_filter_asts(filters: Optional[List[str]] = None):
    """
    Converts a list of filter expresisons to ASTs
    """
    return [
        parse(f"select * where {filter_}").select.where for filter_ in filters or []
    ]


async def find_dimension_node_joins(
    session: AsyncSession,
    node: NodeRevision,
    requested_dimensions: List[str],
    filters: Optional[List[str]] = None,
) -> Dict[str, DimensionJoin]:
    """
    Returns a list of dimension node joins that are necessary based on
    the requested dimensions and filters
    """
    dimension_node_joins = {}

    # Combine necessary dimensions from filters and requested dimensions
    necessary_dimensions = requested_dimensions.copy()
    for filter_ast in to_filter_asts(filters):
        for filter_dim in filter_ast.find_all(ast.Column):
            necessary_dimensions.append(filter_dim.identifier())

    # For dimensions that need a join, build metadata on the join path
    for dim in necessary_dimensions:
        dimension_attr = FullColumnName(dim)
        dim_node = dimension_attr.node_name
        if dim_node == node.name:
            continue
        if dim_node not in dimension_node_joins:
            join_path = await dimension_join_path(session, node, dimension_attr.name)
            if not join_path:
                raise DJException(
                    f"This dimension attribute cannot be joined in: {dimension_attr.name}. "
                    f"Please make sure that {dimension_attr.node_name} is linked to {node.name}",
                )
            if await needs_dimension_join(session, dimension_attr.name, join_path):
                dimension_node_joins[dim_node] = DimensionJoin(
                    join_path=join_path,  # type: ignore
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
        for col in node.columns:  # pragma: no cover
            # Decide if we should restrict this to only columns marked as dimensional
            # await session.refresh(col, ["attributes"]) TODO
            # if col.is_dimensional():
            #     ...
            if f"{node.name}.{col.name}" == dimension:
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
    filters: List[str],
    cte_mapping: Dict[str, ast.Query],
):
    """
    Builds a dimension node query with the requested filters
    """
    await session.refresh(link.dimension, ["current"])
    await session.refresh(link.dimension.current, ["columns"])
    dimension_node_ast = await compile_node_ast(session, link.dimension.current)
    dimension_node_query = await build_ast(
        session,
        link.dimension.current,
        dimension_node_ast,
        filters=filters,  # type: ignore
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
    """
    Builds the requested dimension columns for the final select layer.
    """
    dimensions_columns = []
    for dim in requested_dimensions:
        replacement = build_dimension_attribute(
            dim,
            dimension_node_joins,
            link,
            alias=amenable_name(dim),
        )
        if replacement:  # pragma: no cover
            dimensions_columns.append(replacement)
    return dimensions_columns


async def compile_node_ast(session, node_revision: NodeRevision) -> ast.Query:
    """
    Parses the node's query into an AST and compiles it.
    """
    node_ast = parse(node_revision.query)
    ctx = CompileContext(session, DJException())
    await node_ast.compile(ctx)
    return node_ast


def build_dimension_attribute(
    full_column_name: str,
    dimension_node_joins: Dict[str, DimensionJoin],
    link: DimensionLink,
    alias: Optional[str] = None,
) -> Optional[ast.Column]:
    """
    Turn the canonical dimension attribute into a column on the query AST
    """
    dimension_attr = FullColumnName(full_column_name)
    dim_node = dimension_attr.node_name
    node_query = (
        dimension_node_joins[dim_node].node_query
        if dim_node in dimension_node_joins
        else None
    )

    if node_query:
        foreign_key_column_name = (
            FullColumnName(
                link.foreign_keys_reversed.get(dimension_attr.name),
            ).column_name
            if dimension_attr.name in link.foreign_keys_reversed
            else None
        )
        for col in node_query.select.projection:
            if col.alias_or_name.name == dimension_attr.column_name or (  # type: ignore
                foreign_key_column_name
                and col.alias_or_name.identifier() == foreign_key_column_name  # type: ignore
            ):
                return ast.Column(
                    name=ast.Name(col.alias_or_name.name),  # type: ignore
                    alias=ast.Name(alias) if alias else None,
                    _table=node_query,
                    _type=col.type,  # type: ignore
                )
    return None  # pragma: no cover


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
) -> Optional[Union[ast.BinaryOp, ast.Expression]]:
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
    join_ast.right = join_right.alias  # type: ignore
    dimension_node_columns = join_right.select.column_mapping
    join_left = cte_mapping.get(link.node_revision.name)
    node_columns = join_left.select.column_mapping  # type: ignore
    for col in join_ast.criteria.find_all(ast.Column):  # type: ignore
        full_column = FullColumnName(col.identifier())
        is_dimension_node = full_column.node_name == link.dimension.name
        replacement = ast.Column(
            name=ast.Name(full_column.column_name),
            _table=join_right if is_dimension_node else join_left,
            _type=(dimension_node_columns if is_dimension_node else node_columns)  # type: ignore
            .get(full_column.column_name)
            .type,
        )
        col.parent.replace(col, replacement)  # type: ignore
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


async def build_ast(  # pylint: disable=too-many-arguments,too-many-locals
    session: AsyncSession,
    node: NodeRevision,
    query: ast.Query,
    filters: Optional[List[str]],
    memoized_queries: Dict[int, ast.Query] = None,
    build_criteria: Optional[BuildCriteria] = None,
    access_control=None,
    ctes_mapping: Dict[str, ast.Query] = None,
) -> ast.Query:
    """
    Recursively replaces DJ node references with query ASTs. These are replaced with
    materialized tables where possible (i.e., source nodes will always be replaced with a
    materialized table), but otherwise we generate the SQL of each upstream node reference.

    This function will apply any filters that can be pushed down to each referenced node's AST
    (filters are only applied if they don't require dimension node joins).
    """
    await session.refresh(node, ["dimension_links"])
    context = CompileContext(session=session, exception=DJException())
    await query.compile(context)
    query.bake_ctes()  # pylint: disable=W0212

    new_cte_mapping: Dict[str, ast.Query] = {}
    if not ctes_mapping:
        ctes_mapping = new_cte_mapping

    node_to_tables_mapping = get_dj_node_references_from_select(query.select)
    for referenced_node, table_expressions in node_to_tables_mapping.items():
        await session.refresh(referenced_node, ["dimension_links"])

        # Try to find a materialized table attached to this node, if one exists.
        table_reference = cast(
            Optional[ast.Table],
            get_table_for_node(
                referenced_node,
                build_criteria=build_criteria,
            ),
        )
        if not table_reference:
            # Build a new CTE with the query AST if there is no materialized table
            if referenced_node.name not in ctes_mapping:
                node_query = parse(cast(str, referenced_node.query))
                query_ast = await build_ast(  # type: ignore
                    session,
                    referenced_node,
                    node_query,
                    filters=filters,
                    memoized_queries=memoized_queries,
                    build_criteria=build_criteria,
                    access_control=access_control,
                )
                cte_name = ast.Name(amenable_name(referenced_node.name))
                query_ast = query_ast.to_cte(cte_name, parent_ast=query)
                new_cte_mapping[referenced_node.name] = query_ast

            reference_cte = (
                ctes_mapping[referenced_node.name]
                if referenced_node.name in ctes_mapping
                else new_cte_mapping[referenced_node.name]
            )
            query_ast = ast.Table(  # type: ignore
                reference_cte.alias,  # type: ignore
                _columns=reference_cte._columns,  # pylint: disable=protected-access
                _dj_node=referenced_node,
            )
        else:
            # Otherwise use the materialized table and apply filters where possible
            alias = amenable_name(referenced_node.name)
            query_ast = ast.Query(
                select=ast.Select(
                    projection=table_reference.columns,  # type: ignore
                    from_=ast.From(relations=[ast.Relation(table_reference)]),
                ),
                alias=ast.Name(alias),
            )
            query_ast.parenthesized = True
            apply_filters_to_node(referenced_node, query_ast, to_filter_asts(filters))
            if not query_ast.select.where:
                query_ast = ast.Alias(  # type: ignore
                    ast.Name(alias),
                    child=table_reference,
                    as_=True,
                )

        for table in table_expressions:
            query.select.replace(
                table,
                query_ast,
                copy=False,
            )
            await query.select.compile(context)
            for col in query.select.find_all(ast.Column):
                if (
                    col.table
                    and isinstance(col.table, ast.Table)
                    and col.table.dj_node
                    and col.table.dj_node.name == referenced_node.name
                ):
                    col._table = query_ast  # pylint: disable=protected-access

    # Apply pushdown filters if possible
    apply_filters_to_node(node, query, to_filter_asts(filters))

    query.ctes.extend(new_cte_mapping.values())
    query.select.add_aliases_to_unnamed_columns()
    return query


def apply_filters_to_node(
    node: NodeRevision,
    query: ast.Query,
    filters: List[ast.Expression],
):
    """
    Apply pushdown filters if possible to the node's query AST.

    A pushdown filter is defined as a filter with references to dimensions that
    already exist on the node query without any additional dimension joins. We can
    apply these filters directly to the query AST by renaming the dimension ref in
    the filter expression.
    """
    for filter_ast in filters:
        all_referenced_dimensions_can_pushdown = True
        for filter_dim in filter_ast.find_all(ast.Column):
            column_name = get_column_from_canonical_dimension(
                filter_dim.identifier(),
                node,
            )
            node_col = (
                query.select.column_mapping.get(column_name) if column_name else None
            )
            if node_col:
                replacement = (
                    node_col.child if isinstance(node_col, ast.Alias) else node_col  # type: ignore
                )
                filter_ast.replace(filter_dim, replacement)
            else:
                all_referenced_dimensions_can_pushdown = False

        if all_referenced_dimensions_can_pushdown:
            query.select.where = combine_filter_conditions(
                query.select.where,
                filter_ast,
            )
    return query


def get_dj_node_references_from_select(
    select: ast.SelectExpression,
) -> DefaultDict[NodeRevision, List[ast.Table]]:
    """
    Extract all DJ node references (source, transform, dimensions) from the select
    expression. DJ node references are represented in the AST as table expressions
    and have an attached DJ node.
    """
    tables: DefaultDict[NodeRevision, List[ast.Table]] = collections.defaultdict(list)

    for table in select.find_all(ast.Table):
        if node := table.dj_node:  # pragma: no cover
            tables[node].append(table)
    return tables


def get_table_for_node(
    node: NodeRevision,
    build_criteria: Optional[BuildCriteria] = None,
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
    return table
