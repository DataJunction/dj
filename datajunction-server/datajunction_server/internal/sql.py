import logging
from typing import Any, Tuple, OrderedDict, cast
import re

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from datajunction_server.api.helpers import (
    assemble_column_metadata,
    find_existing_cube,
    get_catalog_by_name,
    get_query,
    validate_orderby,
    validate_cube,
    check_dimension_attributes_exist,
    check_metrics_exist,
)
from datajunction_server.construction.build import (
    build_materialized_cube_node,
    build_metric_nodes,
    group_metrics_by_parent,
    extract_components_and_parent_columns,
    rename_columns,
)
from datajunction_server.construction.build_v2 import (
    QueryBuilder,
    build_preaggregate_query,
    get_dimensions_referenced_in_metrics,
)
from datajunction_server.database import Engine
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.database.catalog import Catalog
from datajunction_server.errors import DJInvalidInputException, DJException
from datajunction_server.internal.engines import get_engine
from datajunction_server.models import access
from datajunction_server.models.column import SemanticType
from datajunction_server.models.cube_materialization import Aggregability
from datajunction_server.models.engine import Dialect
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node import BuildCriteria
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.models.sql import GeneratedSQL
from datajunction_server.naming import LOOKUP_CHARS, from_amenable_name
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.utils import SEPARATOR, refresh_if_needed

logger = logging.getLogger(__name__)


async def build_node_sql(
    session: AsyncSession,
    node_name: str,
    dimensions: list[str] | None = None,
    filters: list[str] | None = None,
    orderby: list[str] | None = None,
    limit: int | None = None,
    engine: Engine | None = None,
    access_control: access.AccessControlStore | None = None,
    ignore_errors: bool = True,
    use_materialized: bool = True,
    query_parameters: dict[str, Any] | None = None,
) -> TranslatedSQL:
    """
    Build node SQL and save it to query requests
    """
    if orderby:
        validate_orderby(orderby, [node_name], dimensions or [])

    node = cast(
        Node,
        await Node.get_by_name(session, node_name, raise_if_not_exists=True),
    )
    if not engine:  # pragma: no cover
        engine = node.current.catalog.engines[0]

    # If it's a cube, we'll build SQL for the metrics in the cube, along with any additional
    # dimensions or filters provided in the arguments
    if node.type == NodeType.CUBE:
        node = cast(
            Node,
            await Node.get_cube_by_name(session, node_name),
        )
        dimensions = list(
            OrderedDict.fromkeys(node.current.cube_node_dimensions + dimensions),
        )
        translated_sql, engine, _ = await build_sql_for_multiple_metrics(
            session=session,
            metrics=node.current.cube_node_metrics,
            dimensions=dimensions,
            filters=filters,
            orderby=orderby,
            limit=limit,
            engine_name=engine.name if engine else None,
            engine_version=engine.version if engine else None,
            access_control=access_control,
            use_materialized=use_materialized,
            query_parameters=query_parameters,
        )
        return translated_sql

    # For all other nodes, build the node query
    node = await Node.get_by_name(session, node_name, raise_if_not_exists=True)  # type: ignore
    if node.type == NodeType.METRIC:
        translated_sql, engine, _ = await build_sql_for_multiple_metrics(
            session,
            [node_name],
            dimensions or [],
            filters or [],
            orderby or [],
            limit,
            engine.name if engine else None,
            engine.version if engine else None,
            access_control=access_control,
            ignore_errors=ignore_errors,
            use_materialized=use_materialized,
            query_parameters=query_parameters,
        )
        query = translated_sql.sql
        columns = translated_sql.columns
    else:
        query_ast = await get_query(
            session=session,
            node_name=node_name,
            dimensions=dimensions or [],
            filters=filters or [],
            orderby=orderby or [],
            limit=limit,
            engine=engine,
            access_control=access_control,
            use_materialized=use_materialized,
            query_parameters=query_parameters,
            ignore_errors=ignore_errors,
        )
        columns = [
            assemble_column_metadata(col, use_semantic_metadata=True)  # type: ignore
            for col in query_ast.select.projection
        ]
        query = str(query_ast)

    return TranslatedSQL.create(
        sql=query,
        columns=columns,
        dialect=engine.dialect if engine else None,
    )


async def build_sql_for_multiple_metrics(
    session: AsyncSession,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str] = None,
    orderby: list[str] = None,
    limit: int | None = None,
    engine_name: str | None = None,
    engine_version: str | None = None,
    access_control: access.AccessControlStore | None = None,
    ignore_errors: bool = True,
    use_materialized: bool = True,
    query_parameters: dict[str, str] | None = None,
) -> Tuple[TranslatedSQL, Engine, Catalog]:
    """
    Build SQL for multiple metrics. Used by both /sql and /data endpoints
    """
    if not filters:
        filters = []
    if not orderby:
        orderby = []

    metric_columns, metric_nodes, _, dimension_columns, _ = await validate_cube(
        session,
        metrics,
        dimensions,
        require_dimensions=False,
    )
    leading_metric_node = await Node.get_by_name(
        session,
        metrics[0],
        options=[
            joinedload(Node.current).options(
                joinedload(NodeRevision.catalog).options(joinedload(Catalog.engines)),
            ),
        ],
    )
    if access_control:
        access_control.add_request_by_node(leading_metric_node.current)  # type: ignore
    available_engines = (
        leading_metric_node.current.catalog.engines  # type: ignore
        if leading_metric_node.current.catalog  # type: ignore
        else []
    )

    # Try to find a built cube that already has the given metrics and dimensions
    # The cube needs to have a materialization configured and an availability state
    # posted in order for us to use the materialized datasource
    cube = await find_existing_cube(
        session,
        metric_columns,
        dimension_columns,
        materialized=True,
    )
    materialized_cube_catalog = None
    if cube:
        materialized_cube_catalog = await get_catalog_by_name(
            session,
            cube.availability.catalog,  # type: ignore
        )

    # Check if selected engine is available
    engine = (
        await get_engine(session, engine_name, engine_version)  # type: ignore
        if engine_name
        else available_engines[0]
    )
    if engine not in available_engines:
        raise DJInvalidInputException(  # pragma: no cover
            f"The selected engine is not available for the node {metrics[0]}. "
            f"Available engines include: {', '.join(engine.name for engine in available_engines)}",
        )

    # Do not use the materialized cube if the chosen engine is not available for
    # the materialized cube's catalog
    if (
        cube
        and materialized_cube_catalog
        and engine.name not in [eng.name for eng in materialized_cube_catalog.engines]
    ):
        cube = None

    if orderby:
        validate_orderby(orderby, metrics, dimensions)

    if cube and cube.availability and use_materialized and materialized_cube_catalog:
        if access_control:  # pragma: no cover
            access_control.add_request_by_node(cube)
            access_control.state = access.AccessControlState.INDIRECT
            access_control.raise_if_invalid_requests()
        query_ast = build_materialized_cube_node(
            metric_columns,
            dimension_columns,
            cube,
            filters,
            orderby,
            limit,
        )
        query_metric_columns = [
            ColumnMetadata(
                name=col.name,
                type=str(col.type),
                column=col.name,
                node=col.node_revision.name,  # type: ignore
            )
            for col in metric_columns
        ]
        query_dimension_columns = [
            ColumnMetadata(
                name=(col.node_revision.name + SEPARATOR + col.name).replace(  # type: ignore
                    SEPARATOR,
                    f"_{LOOKUP_CHARS.get(SEPARATOR)}_",
                ),
                type=str(col.type),
                node=col.node_revision.name,  # type: ignore
                column=col.name,  # type: ignore
            )
            for col in dimension_columns
        ]
        engine = materialized_cube_catalog.engines[0]
        return (
            TranslatedSQL.create(
                sql=str(query_ast),
                columns=query_metric_columns + query_dimension_columns,
                dialect=materialized_cube_catalog.engines[0].dialect,
            ),
            engine,
            cube.catalog,
        )

    query_ast = await build_metric_nodes(
        session,
        metric_nodes,
        filters=filters or [],
        dimensions=dimensions or [],
        orderby=orderby or [],
        limit=limit,
        access_control=access_control,
        ignore_errors=ignore_errors,
        query_parameters=query_parameters,
    )
    columns = [
        assemble_column_metadata(col, use_semantic_metadata=True)  # type: ignore
        for col in query_ast.select.projection
    ]
    upstream_tables = [tbl for tbl in query_ast.find_all(ast.Table) if tbl.dj_node]
    for tbl in upstream_tables:
        await refresh_if_needed(session, tbl.dj_node, ["availability"])
    return (
        TranslatedSQL.create(
            sql=str(query_ast),
            columns=columns,
            dialect=engine.dialect if engine else None,
            upstream_tables=[
                f"{leading_metric_node.current.catalog.name}.{tbl.identifier()}"  # type: ignore
                for tbl in upstream_tables
                # If a table has a corresponding node with an associated physical table (either
                # a source node or a node with a materialized table).
                if cast(NodeRevision, tbl.dj_node).type == NodeType.SOURCE
                or cast(NodeRevision, tbl.dj_node).availability is not None
            ],
        ),
        engine,
        leading_metric_node.current.catalog,  # type: ignore
    )


async def get_measures_query(
    session: AsyncSession,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str],
    orderby: list[str] = None,
    engine_name: str | None = None,
    engine_version: str | None = None,
    current_user: User | None = None,
    validate_access: access.ValidateAccessFn = None,
    include_all_columns: bool = False,
    use_materialized: bool = True,
    preagg_requested: bool = False,
    query_parameters: dict[str, Any] = None,
) -> list[GeneratedSQL]:
    """
    Builds the measures SQL for a set of metrics with dimensions and filters.

    Measures queries are generated at the grain of each of the metrics' upstream nodes.
    For example, if some of your metrics are aggregations on measures in parent node A
    and others are aggregations on measures in parent node B, this function will return a
    dictionary that maps A to the measures query for A, and B to the measures query for B.
    """
    engine = (
        await get_engine(session, engine_name, engine_version) if engine_name else None  # type: ignore
    )
    build_criteria = BuildCriteria(
        dialect=engine.dialect if engine and engine.dialect else Dialect.SPARK,
    )
    access_control = (
        access.AccessControlStore(
            validate_access=validate_access,
            user=current_user,
            base_verb=access.ResourceRequestVerb.READ,
        )
        if validate_access
        else None
    )

    if not filters:
        filters = []

    metrics_sorting_order = {val: idx for idx, val in enumerate(metrics)}
    metric_nodes = await check_metrics_exist(session, metrics)
    await check_dimension_attributes_exist(session, dimensions)

    common_parents = group_metrics_by_parent(metric_nodes)
    parent_columns, metric_components = extract_components_and_parent_columns(
        metric_nodes,
    )

    column_name_regex = r"([A-Za-z0-9_\.]+)(\[[A-Za-z0-9_]+\])?"
    matcher = re.compile(column_name_regex)

    # Find any dimensions referenced in the metric definitions and add to requested dimensions
    dimensions.extend(
        [
            dim
            for dim in get_dimensions_referenced_in_metrics(metric_nodes)
            if dim not in dimensions
        ],
    )

    dimensions_without_roles = [matcher.findall(dim)[0][0] for dim in dimensions]

    measures_queries = []
    context = CompileContext(session=session, exception=DJException())
    for parent_node, children in common_parents.items():  # type: ignore
        children = sorted(children, key=lambda x: metrics_sorting_order.get(x.name, 0))

        # Determine whether to pre-aggregate to the requested dimensions so that subsequent
        # queries are more efficient by checking the measures on the requested metrics
        preaggregate = preagg_requested and all(
            len(metric_components[metric.name][0]) > 0
            and all(
                measure.rule.type in (Aggregability.FULL, Aggregability.LIMITED)
                for measure in metric_components[metric.name][0]
            )
            for metric in children
        )

        measure_columns, dimensional_columns = [], []
        await refresh_if_needed(session, parent_node, ["current"])
        query_builder = await QueryBuilder.create(
            session,
            parent_node.current,
            use_materialized=use_materialized,
        )
        parent_ast = await (
            query_builder.ignore_errors()
            .with_access_control(access_control)
            .with_build_criteria(build_criteria)
            .add_dimensions(dimensions)
            .add_filters(filters)
            .add_query_parameters(query_parameters)
            .order_by(orderby)
            .build()
        )

        # Select only columns that were one of the necessary measures
        if not include_all_columns:
            parent_ast.select.projection = [
                expr
                for expr in parent_ast.select.projection
                if (
                    (identifier := expr.alias_or_name.identifier(False))
                    and (
                        from_amenable_name(identifier).split(SEPARATOR)[-1]
                        in parent_columns[parent_node.name]
                        or identifier in parent_columns[parent_node.name]
                        or expr.semantic_entity in dimensions_without_roles
                        or from_amenable_name(identifier) in dimensions_without_roles
                    )
                )
            ]

        await refresh_if_needed(session, parent_node.current, ["columns"])
        parent_ast = rename_columns(parent_ast, parent_node.current, preaggregate)

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
        dependencies, _ = await parent_ast.extract_dependencies(
            CompileContext(session, DJException()),
        )

        final_query = (
            build_preaggregate_query(
                parent_ast,
                parent_node,
                dimensional_columns,
                children,
                metric_components,
            )
            if preaggregate
            else parent_ast
        )

        # Build translated SQL object
        columns_metadata = [
            assemble_column_metadata(  # pragma: no cover
                cast(ast.Column, col),
                preaggregate,
            )
            for col in final_query.select.projection
        ]
        measures_queries.append(
            GeneratedSQL.create(
                node=parent_node.current,
                sql=str(final_query),
                columns=columns_metadata,
                dialect=build_criteria.dialect,
                upstream_tables=[
                    f"{dep.catalog.name}.{dep.schema_}.{dep.table}"
                    for dep in dependencies
                    if dep.type == NodeType.SOURCE
                ],
                grain=(
                    [
                        col.name
                        for col in columns_metadata
                        if col.semantic_type == SemanticType.DIMENSION
                    ]
                    if preaggregate
                    else [pk_col.name for pk_col in parent_node.current.primary_key()]
                ),
                errors=query_builder.errors,
                metrics={
                    metric.name: (
                        metric_components[metric.name][0],
                        str(metric_components[metric.name][1]).replace("\n", "")
                        if preaggregate
                        else metric.query,
                    )
                    for metric in children
                },
            ),
        )
    return measures_queries
