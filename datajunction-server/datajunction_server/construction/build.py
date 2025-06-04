"""Functions for building DJ node queries"""

import collections
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, DefaultDict, List, Optional, Set, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database import Engine
from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJError, DJInvalidInputException, ErrorCode
from datajunction_server.internal.engines import get_engine
from datajunction_server.models import access
from datajunction_server.models.cube_materialization import MetricComponent
from datajunction_server.models.engine import Dialect
from datajunction_server.models.materialization import GenericCubeConfig
from datajunction_server.models.node import BuildCriteria
from datajunction_server.naming import LOOKUP_CHARS, amenable_name, from_amenable_name
from datajunction_server.sql.dag import get_shared_dimensions
from datajunction_server.sql.decompose import MetricComponentExtractor
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


def rename_columns(
    built_ast: ast.Query,
    node: NodeRevision,
    preaggregate: bool = False,
):
    """
    Rename columns in the built ast to fully qualified column names.
    """
    projection = []
    node_columns = {col.name for col in node.columns}
    for expression in built_ast.select.projection:
        if hasattr(expression, "semantic_entity") and expression.semantic_entity:  # type: ignore
            # If the expression already has a semantic entity, we assume it is already
            # fully qualified and skip renaming.
            projection.append(expression)
            expression.set_alias(ast.Name(amenable_name(expression.semantic_entity)))  # type: ignore
            continue
        if (
            not isinstance(expression, ast.Alias)
            and not isinstance(
                expression,
                ast.Wildcard,
            )
            and not (hasattr(expression, "alias") and expression.alias)  # type: ignore
        ):
            alias_name = expression.alias_or_name.identifier(False)  # type: ignore
            if expression.alias_or_name.name.lower() in node_columns:  # type: ignore  # pragma: no cover
                alias_name = node.name + SEPARATOR + expression.alias_or_name.name  # type: ignore
            expression = expression.copy()
            expression.set_semantic_entity(alias_name)  # type: ignore
            if not preaggregate:
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
                    alias_name = f"{node.name}{SEPARATOR}{column_ref}"  # type: ignore  # pragma: no cover
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
        for i in range(  # pragma: no cover
            len(built_ast.select.group_by),
        ):
            if hasattr(built_ast.select.group_by[i], "alias"):  # pragma: no cover
                built_ast.select.group_by[i] = ast.Column(
                    name=built_ast.select.group_by[i].name,  # type: ignore
                    _type=built_ast.select.group_by[i].type,  # type: ignore
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


async def build_metric_nodes(
    session: AsyncSession,
    metric_nodes: List[Node],
    filters: List[str],
    dimensions: List[str],
    orderby: List[str],
    limit: Optional[int] = None,
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
    build_criteria: Optional[BuildCriteria] = None,
    access_control: Optional[access.AccessControlStore] = None,
    ignore_errors: bool = True,
    query_parameters: Optional[dict[str, Any]] = None,
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
    from datajunction_server.construction.build_v2 import CubeQueryBuilder

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
        .add_query_parameters(query_parameters)
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
        # TODO: remove after we migrate old Druid materializations
        selected_metric_keys = [
            col.name for col in selected_metrics
        ]  # pragma: no cover
    else:
        selected_metric_keys = [
            col.node_revision().name  # type: ignore
            for col in selected_metrics
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


def extract_components_and_parent_columns(
    metric_nodes: list[Node],
) -> Tuple[
    DefaultDict[str, Set[str]],
    dict[str, tuple[list[MetricComponent], ast.Query]],
]:
    """
    Given a list of metric nodes, returns:
    1. A mapping of parent node names to sets of column names (as referenced in metric queries).
    2. A mapping from metric node names to their extracted MetricComponents and full AST.

    Returns:
    (
        {
            "parent_node_1": {"column_a", "column_b"},
            "parent_node_2": {"column_x"},
        },
        {
            "metric_node_name": ([MetricComponent, ...], ast.Query),
            ...
        }
    )
    """
    components_by_metric: dict[str, tuple[list[MetricComponent], ast.Query]] = {}
    parent_columns = collections.defaultdict(set)

    def extract_components_and_columns(metric_query: str):
        extractor = MetricComponentExtractor.from_query_string(metric_query)
        metric_ast = parse(metric_query)
        return extractor.extract(), list(metric_ast.find_all(ast.Column))

    max_workers = min(max(1, len(metric_nodes)), os.cpu_count())  # type: ignore
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        extracted_measures = executor.map(
            extract_components_and_columns,
            [metric_node.current.query for metric_node in metric_nodes],
        )

    for metric_node, (measures, columns) in zip(metric_nodes, extracted_measures):
        components_by_metric[metric_node.name] = measures
        for col in columns:
            parent_columns[metric_node.current.parents[0].name].add(  # type: ignore
                col.alias_or_name.name,
            )
    return parent_columns, components_by_metric
