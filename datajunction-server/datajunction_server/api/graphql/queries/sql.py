"""Generate SQL-related GraphQL queries."""
from typing import Annotated, Optional, OrderedDict

import strawberry
from strawberry.types import Info

from datajunction_server.api.graphql.scalars.sql import (
    CubeDefinition,
    EngineSettings,
    GeneratedSQL,
)
from datajunction_server.construction.build_v2 import get_measures_query


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
    session, settings = info.context["session"], info.context["settings"]
    queries = await get_measures_query(
        session=session,
        metrics=list(OrderedDict.fromkeys(cube.metrics)),  # type: ignore
        dimensions=cube.dimensions,  # type: ignore
        filters=cube.filters,  # type: ignore
        orderby=cube.orderby,
        engine_name=engine.name if engine else None,
        engine_version=engine.version if engine else None,
        include_all_columns=include_all_columns,
        sql_transpilation_library=settings.sql_transpilation_library,
        use_materialized=use_materialized,
        preaggregate=preaggregate,
    )
    return [
        await GeneratedSQL.from_pydantic(info, measures_query)
        for measures_query in queries
    ]
