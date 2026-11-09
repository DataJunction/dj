"""Generate SQL-related GraphQL queries."""

from strawberry.types import Info

from datajunction_server.api.graphql.resolvers.nodes import (
    find_nodes_by,
    get_metrics,
    resolve_metrics_and_dimensions,
)
from datajunction_server.api.graphql.scalars.sql import (
    CubeDefinition,
    MaterializationPlan,
    MaterializationUnit,
    MetricComponent,
    VersionedRef,
)
from datajunction_server.api.graphql.utils import resolver_session
from datajunction_server.construction.build import group_metrics_by_parent
from datajunction_server.models.cube_materialization import Aggregability
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse
from datajunction_server.utils import SEPARATOR


async def materialization_plan(
    cube: CubeDefinition,
    *,
    info: Info,
) -> MaterializationPlan:
    """
    This constructs a `MaterializationPlan` by computing all the versioned entities (metrics,
    measures, dimensions, filters) required to materialize the cube.
    """
    async with resolver_session(info) as session:
        metrics, dimensions = await resolve_metrics_and_dimensions(session, cube)

        metric_nodes = await get_metrics(session, metrics=metrics)

        # Extract dimension references from filters
        filter_refs = [
            filter_dim.identifier()
            for filter_expr in cube.filters or []
            for filter_dim in parse(f"SELECT 1 WHERE {filter_expr}").find_all(
                ast.Column,
            )
        ]

        # Resolve nodes for dimensions and filter references
        all_ref_nodes = {
            dim.rsplit(SEPARATOR, 1)[0] for dim in dimensions + filter_refs
        }
        nodes_lookup = {
            node.name: node for node in await find_nodes_by(info, list(all_ref_nodes))
        }

        # Group the metrics by upstream node
        grouped_metrics = await group_metrics_by_parent(session, metric_nodes)
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
                    nodes_lookup[dim.rsplit(SEPARATOR, 1)[0]]
                    for dim in dimensions
                    if dim.rsplit(SEPARATOR, 1)[0] in nodes_lookup
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
                        version=nodes_lookup[
                            ref.rsplit(SEPARATOR, 1)[0]
                        ].current_version,
                    )
                    for ref in filter_refs
                    if ref.rsplit(SEPARATOR, 1)[0] in nodes_lookup
                ],
                filters=cube.filters,
            )
            units.append(unit)

        return MaterializationPlan(units=units)  # type: ignore
