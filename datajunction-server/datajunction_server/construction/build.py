# pylint: disable=too-many-arguments,too-many-locals,too-many-nested-blocks,too-many-branches,R0401,too-many-lines,protected-access,line-too-long
"""Functions for building DJ node queries"""
import collections
import logging
from typing import DefaultDict, List, Optional, Set, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database import Engine
from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import (
    DJError,
    DJException,
    DJInvalidInputException,
    ErrorCode,
)
from datajunction_server.internal.engines import get_engine
from datajunction_server.models import access
from datajunction_server.models.engine import Dialect
from datajunction_server.models.materialization import GenericCubeConfig
from datajunction_server.models.node import BuildCriteria
from datajunction_server.naming import LOOKUP_CHARS, amenable_name, from_amenable_name
from datajunction_server.sql.dag import get_shared_dimensions
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse
from datajunction_server.sql.parsing.types import ColumnType
from datajunction_server.utils import SEPARATOR

_logger = logging.getLogger(__name__)


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


def rename_columns(built_ast: ast.Query, node: NodeRevision):
    """
    Rename columns in the built ast to fully qualified column names.
    """
    projection = []
    node_columns = {col.name for col in node.columns}
    for expression in built_ast.select.projection:
        if (
            not isinstance(expression, ast.Alias)
            and not isinstance(
                expression,
                ast.Wildcard,
            )
            and not (hasattr(expression, "alias") and expression.alias)  # type: ignore
        ):
            alias_name = expression.alias_or_name.identifier(False)  # type: ignore
            if expression.alias_or_name.name in node_columns:  # type: ignore  # pragma: no cover
                alias_name = node.name + SEPARATOR + expression.alias_or_name.name  # type: ignore
            expression = expression.copy()
            expression.set_semantic_entity(alias_name)  # type: ignore
            expression.set_alias(ast.Name(amenable_name(alias_name)))
            projection.append(expression)
        else:
            expression = expression.copy()
            if isinstance(
                expression,
                ast.Aliasable,
            ) and not isinstance(  # pragma: no cover
                expression,
                ast.Wildcard,
            ):
                column_ref = expression.alias_or_name.identifier()
                if column_ref in node_columns:  # type: ignore
                    alias_name = f"{node.name}{SEPARATOR}{column_ref}"  # type: ignore  # pragma: no cover  # pylint: disable=line-too-long
                    expression.set_semantic_entity(alias_name)  # pragma: no cover
                else:
                    expression.set_semantic_entity(from_amenable_name(column_ref))
            projection.append(expression)  # type: ignore
    built_ast.select.projection = projection

    if built_ast.select.where:
        for col in built_ast.select.where.find_all(ast.Column):  # pragma: no cover
            if hasattr(col, "alias"):  # pragma: no cover
                col.alias = None

    if built_ast.select.group_by:
        for i in range(  # pylint: disable=consider-using-enumerate  # pragma: no cover
            len(built_ast.select.group_by),
        ):
            if hasattr(built_ast.select.group_by[i], "alias"):  # pragma: no cover
                built_ast.select.group_by[i] = ast.Column(
                    name=built_ast.select.group_by[i].name,  # type: ignore
                    # pylint:disable=protected-access
                    _type=built_ast.select.group_by[i].type,  # type: ignore
                    # pylint:disable=protected-access
                    _table=built_ast.select.group_by[i]._table,  # type: ignore
                )
                built_ast.select.group_by[i].alias = None
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


async def validate_shared_dimensions(
    session: AsyncSession,
    metric_nodes: List[Node],
    dimensions: List[str],
):
    """
    Determine if dimensions are shared.
    """
    shared_dimensions = [
        dim.name for dim in await get_shared_dimensions(session, metric_nodes)
    ]
    for dimension_attribute in dimensions:
        if dimension_attribute not in shared_dimensions:
            message = (
                f"The dimension attribute `{dimension_attribute}` is not "
                "available on every metric and thus cannot be included."
            )
            raise DJInvalidInputException(
                message,
                errors=[DJError(code=ErrorCode.INVALID_DIMENSION, message=message)],
            )


async def build_metric_nodes(  # pylint: disable=too-many-statements
    session: AsyncSession,
    metric_nodes: List[Node],
    filters: List[str],
    dimensions: List[str],
    orderby: List[str],  # pylint: disable=unused-argument
    limit: Optional[int] = None,  # pylint: disable=unused-argument
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    build_criteria: Optional[BuildCriteria] = None,
    access_control: Optional[access.AccessControlStore] = None,
    ignore_errors: bool = True,
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
    from datajunction_server.construction.build_v2 import (  # pylint: disable=import-outside-toplevel
        CubeQueryBuilder,
    )

    engine = (
        await get_engine(session, engine_name, engine_version)
        if engine_name and engine_version
        else None
    )
    build_criteria = build_criteria or BuildCriteria(
        dialect=engine.dialect if engine and engine.dialect else Dialect.SPARK,
    )
    builder = await CubeQueryBuilder.create(
        session,
        metric_nodes,
        use_materialized=False,
    )
    builder = (
        builder.add_filters(filters)
        .add_dimensions(dimensions)
        .order_by(orderby)
        .limit(limit)
        .with_build_criteria(build_criteria)
        .with_access_control(access_control)
    )
    if ignore_errors:
        builder = builder.ignore_errors()
    return await builder.build()


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
    materialization_config = cube.materializations[0]
    cube_config = GenericCubeConfig.parse_obj(materialization_config.config)

    if materialization_config.name == "default":
        # TODO: remove after we migrate old Druid materializations  # pylint: disable=fixme
        selected_metric_keys = [
            col.name for col in selected_metrics
        ]  # pragma: no cover
    else:
        selected_metric_keys = [
            col.node_revision().name for col in selected_metrics  # type: ignore
        ]

    # Assemble query for materialized cube based on the previously saved measures
    # combiner expression for each metric
    for metric_key in selected_metric_keys:
        if (
            cube_config.measures and metric_key in cube_config.measures
        ):  # pragma: no cover
            metric_measures = cube_config.measures[metric_key]
            measures_combiner_ast = parse(f"SELECT {metric_measures.combiner}")
            measures_type_lookup = {
                (
                    measure.name
                    if materialization_config.name == "default"
                    else measure.field_name
                ): measure.type
                for measure in metric_measures.measures
            }
            for col in measures_combiner_ast.find_all(ast.Column):
                col.add_type(
                    ColumnType(
                        measures_type_lookup[col.alias_or_name.name],  # type: ignore
                    ),
                )
            combined_ast.select.projection.extend(
                [
                    proj.set_alias(ast.Name(amenable_name(metric_key)))
                    for proj in measures_combiner_ast.select.projection
                ],
            )
        else:
            # This is the materialized metrics table case. We choose SUM for now,
            # since there shouldn't be any other possible aggregation types on a
            # metric (maybe MAX or MIN in some special cases).
            combined_ast.select.projection.append(
                ast.Function(  # pragma: no cover
                    name=ast.Name("SUM"),
                    args=[ast.Column(name=ast.Name(amenable_name(metric_key)))],
                ),
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

    if filter_asts:  # pragma: no cover
        # pylint: disable=no-value-for-parameter
        combined_ast.select.where = ast.BinaryOp.And(*filter_asts)

    # Add orderby
    if orderby:  # pragma: no cover
        temp_select = build_temp_select(
            f"select * order by {','.join(orderby)}",
        )
        combined_ast.select.organization = temp_select.organization

    # Add limit
    if limit:  # pragma: no cover
        combined_ast.select.limit = ast.Number(value=limit)

    # Set up FROM clause
    combined_ast.select.from_.relations.append(  # type: ignore
        ast.Relation(primary=ast.Table(ast.Name(cube.availability.table))),  # type: ignore
    )
    return combined_ast


async def metrics_to_measures(
    session: AsyncSession,
    metric_nodes: List[Node],
) -> Tuple[DefaultDict[str, Set[str]], DefaultDict[str, Set[str]]]:
    """
    For the given metric nodes, returns a mapping between the metrics' referenced parent nodes
    and the list of necessary measures to extract from the parent node.
    The structure is:
    {
        "parent_node_name1": ["measure_columnA", "measure_columnB"],
        "parent_node_name2": ["measure_columnX"],
    }
    """
    ctx = CompileContext(session, DJException())
    metric_to_measures = collections.defaultdict(set)
    parents_to_measures = collections.defaultdict(set)
    for metric_node in metric_nodes:
        metric_ast = parse(metric_node.current.query)
        await metric_ast.compile(ctx)
        for col in metric_ast.find_all(ast.Column):
            if col.table:  # pragma: no cover
                parents_to_measures[col.table.dj_node.name].add(  # type: ignore
                    col.alias_or_name.name,
                )
                metric_to_measures[metric_node.name].add(
                    col.alias_or_name.name,
                )
    return parents_to_measures, metric_to_measures
