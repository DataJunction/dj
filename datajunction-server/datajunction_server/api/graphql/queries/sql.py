"""Generate SQL-related GraphQL queries."""

from typing import Annotated, Optional, OrderedDict

import strawberry
from strawberry.types import Info

from datajunction_server.api.graphql.scalars.sql import GeneratedSQL
from datajunction_server.construction.build_v2 import get_measures_query


@strawberry.input
class CubeDefinition:
    """
    The cube definition for the query
    """

    metrics: Annotated[
        list[str],
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
    queries = await get_measures_query(
        session=session,
        metrics=list(OrderedDict.fromkeys(cube.metrics)),  # type: ignore
        dimensions=cube.dimensions,  # type: ignore
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
