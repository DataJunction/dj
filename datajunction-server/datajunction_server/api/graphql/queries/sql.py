"""Generate SQL-related GraphQL queries."""

from typing import Annotated, Optional, OrderedDict

import strawberry
from strawberry.types import Info

from datajunction_server.models.node import NodeType
from datajunction_server.errors import DJNodeNotFound
from datajunction_server.database.node import Node as DBNode
from datajunction_server.api.graphql.resolvers.nodes import find_nodes_by
from datajunction_server.api.graphql.scalars.sql import GeneratedSQL
from datajunction_server.construction.build_v2 import get_measures_query


@strawberry.input
class CubeDefinition:
    """
    The cube definition for the query
    """
    cube: Annotated[
        str | None,
        strawberry.argument(
            description="The name of the cube to query",
        ),
    ] = None  # type: ignore
    metrics: Annotated[
        list[str] | None,
        strawberry.argument(
            description="A list of metric node names",
        ),
    ] = None  # type: ignore
    dimensions: Annotated[
        list[str] | None,
        strawberry.argument(
            description="A list of dimension attribute names",
        ),
    ] = None
    filters: Annotated[
        list[str] | None,
        strawberry.argument(
            description="A list of filter SQL clauses",
        ),
    ] = None
    orderby: Annotated[
        list[str] | None,
        strawberry.argument(
            description="A list of order by clauses",
        ),
    ] = None


@strawberry.input
class EngineSettings:
    """
    The engine settings for the query
    """

    name: str = strawberry.field(
        description="The name of the engine used by the generated SQL",
    )
    version: str | None = strawberry.field(
        description="The version of the engine used by the generated SQL",
    )


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
    *,
    info: Info,
) -> list[GeneratedSQL]:
    """
    Get measures SQL for a set of metrics with dimensions and filters
    """
    session = info.context["session"]
    metrics = cube.metrics or []
    dimensions = cube.dimensions or []
    if cube.cube:
        cube_node = await DBNode.get_cube_by_name(session, cube.cube)
        if not cube_node:
            raise DJNodeNotFound(f"Cube '{cube.cube}' not found.")
        current_revision = cube_node.current
        cube_node_metrics = set(current_revision.cube_node_metrics)
        cube_node_dimensions = set(current_revision.cube_node_dimensions)
        metrics = current_revision.cube_node_metrics + [m for m in metrics if m not in cube_node_metrics]
        dimensions = current_revision.cube_node_dimensions + [dim for dim in dimensions if dim not in cube_node_dimensions]

    queries = await get_measures_query(
        session=session,
        metrics=list(OrderedDict.fromkeys(metrics)),  # type: ignore
        dimensions=dimensions,  # type: ignore
        filters=cube.filters,  # type: ignore
        orderby=cube.orderby,
        engine_name=engine.name if engine else None,
        engine_version=engine.version if engine else None,
        include_all_columns=include_all_columns,
        use_materialized=use_materialized,
        preagg_requested=preaggregate,
    )
    return [
        await GeneratedSQL.from_pydantic(info, measures_query)
        for measures_query in queries
    ]
