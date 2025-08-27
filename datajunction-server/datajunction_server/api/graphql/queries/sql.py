"""Generate SQL-related GraphQL queries."""

from typing import Annotated, Optional

import strawberry
from strawberry.types import Info
from strawberry.scalars import JSON
from datajunction_server.internal.caching.query_cache_manager import (
    QueryCacheManager,
    QueryRequestParams,
)
from datajunction_server.database.queryrequest import QueryBuildType

from datajunction_server.api.graphql.resolvers.nodes import (
    get_metrics,
    resolve_metrics_and_dimensions,
    find_nodes_by,
)
from datajunction_server.utils import SEPARATOR
from datajunction_server.sql.parsing.backends.antlr4 import parse, ast
from datajunction_server.models.cube_materialization import Aggregability
from datajunction_server.api.graphql.scalars.sql import (
    GeneratedSQL,
    CubeDefinition,
    EngineSettings,
    MaterializationPlan,
    MaterializationUnit,
    VersionedRef,
    MetricComponent,
)
from datajunction_server.construction.build import group_metrics_by_parent


async def measures_sql(
    cube: CubeDefinition,
    engine: Optional[EngineSettings] = None,
    use_materialized: Annotated[
        bool,
        strawberry.argument(
            description="Whether to use materialized nodes where applicable",
        ),
    ] = True,
    include_all_columns: Annotated[
        bool,
        strawberry.argument(
            description="Whether to include all columns or only those necessary "
            "for the metrics and dimensions in the cube",
        ),
    ] = False,
    preaggregate: Annotated[
        bool,
        strawberry.argument(
            description="Whether to pre-aggregate to the requested dimensions so that "
            "subsequent queries are more efficient.",
        ),
    ] = False,
    query_parameters: Annotated[
        JSON | None,
        strawberry.argument(
            description="Query parameters to include in the SQL",
        ),
    ] = None,
    *,
    info: Info,
) -> list[GeneratedSQL]:
    """
    Get measures SQL for a set of metrics with dimensions and filters
    """
    session = info.context["session"]
    metrics, dimensions = await resolve_metrics_and_dimensions(session, cube)
    query_cache_manager = QueryCacheManager(
        cache=info.context["cache"],
        query_type=QueryBuildType.MEASURES,
    )
    queries = await query_cache_manager.get_or_load(
        info.context["background_tasks"],
        info.context["request"],
        QueryRequestParams(
            nodes=metrics,
            dimensions=dimensions,
            filters=cube.filters,
            engine_name=engine.name if engine else None,
            engine_version=engine.version if engine else None,
            orderby=cube.orderby,
            query_params=query_parameters,
            include_all_columns=include_all_columns,
            preaggregate=preaggregate,
            use_materialized=use_materialized,
        ),
    )
    return [
        await GeneratedSQL.from_pydantic(info, measures_query)
        for measures_query in queries
    ]


async def materialization_plan(
    cube: CubeDefinition,
    *,
    info: Info,
) -> MaterializationPlan:
    """
    This constructs a `MaterializationPlan` by computing all the versioned entities (metrics,
    measures, dimensions, filters) required to materialize the cube.
    """
    session = info.context["session"]
    metrics, dimensions = await resolve_metrics_and_dimensions(session, cube)

    metric_nodes = await get_metrics(session, metrics=metrics)

    # Extract dimension references from filters
    filter_refs = [
        filter_dim.identifier()
        for filter_expr in cube.filters or []
        for filter_dim in parse(f"SELECT 1 WHERE {filter_expr}").find_all(ast.Column)
    ]

    # Resolve nodes for dimensions and filter references
    all_ref_nodes = {dim.rsplit(SEPARATOR, 1)[0] for dim in dimensions + filter_refs}
    nodes_lookup = {
        node.name: node for node in await find_nodes_by(info, list(all_ref_nodes))
    }

    # Group the metrics by upstream node
    grouped_metrics = group_metrics_by_parent(metric_nodes)
    units = []
    for upstream_node, metrics_in_group in grouped_metrics.items():
        # Ensure frozen measures are loaded
        for metric in metrics_in_group:
            await session.refresh(metric, ["frozen_measures"])

        # Deduplicate and collect all frozen measures
        measures = {
            fm.name: MetricComponent(  # type: ignore
                name=fm.name,
                expression=fm.expression,
                rule=fm.rule,
                aggregation=fm.aggregation,
            )
            for metric in metrics_in_group
            for fm in metric.frozen_measures
        }.values()

        # Determine grain dimensions based on aggregability
        limited_agg_measures = [
            m
            for m in measures
            if m.rule.type == Aggregability.LIMITED  # type: ignore
        ]
        non_agg_measures = [
            m
            for m in measures
            if m.rule.type == Aggregability.NONE  # type: ignore
        ]

        if non_agg_measures:
            grain_dimensions = []  # pragma: no cover
        else:
            grain_from_rules = [
                dim
                for m in limited_agg_measures
                for dim in m.rule.level  # type: ignore
            ]
            grain_from_dims = [
                nodes_lookup[dim.rsplit(SEPARATOR, 1)[0]] for dim in dimensions
            ]
            grain_dimensions = grain_from_rules + grain_from_dims

        # Construct materialization unit
        unit = MaterializationUnit(  # type: ignore
            upstream=VersionedRef(  # type: ignore
                name=upstream_node.name,
                version=upstream_node.current_version,
            ),
            grain_dimensions=[
                VersionedRef(name=dim.name, version=dim.current_version)  # type: ignore
                for dim in grain_dimensions
            ],
            measures=list(measures),
            filter_refs=[
                VersionedRef(  # type: ignore
                    name=ref,
                    version=nodes_lookup[ref.rsplit(SEPARATOR, 1)[0]].current_version,
                )
                for ref in filter_refs
            ],
            filters=cube.filters,
        )
        units.append(unit)

    return MaterializationPlan(units=units)  # type: ignore
