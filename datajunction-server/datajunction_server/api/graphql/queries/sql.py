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
from datajunction_server.construction.build_v3 import (
    build_metrics_sql,
    build_measures_sql,
    resolve_dialect_and_engine_for_metrics,
)
from datajunction_server.models.dialect import Dialect as Dialect_
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
    Dialect,
    QueryBuildType as QueryBuildType_GQL,
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
        node_version_loader=info.context.get("node_version_loader"),  # Use DataLoader!
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


async def sql(
    query_type: Annotated[
        QueryBuildType_GQL,
        strawberry.argument(
            description="Query type: MEASURES or METRICS (NODE not supported)",
        ),
    ],
    cube: CubeDefinition,
    use_materialized: Annotated[
        bool,
        strawberry.argument(
            description="Whether to use materialized nodes where applicable",
        ),
    ] = True,
    limit: Annotated[
        int | None,
        strawberry.argument(
            description="Maximum number of rows to return (METRICS queries only)",
        ),
    ] = None,
    dialect: Annotated[
        Dialect | None,
        strawberry.argument(
            description="SQL dialect for the generated query. Auto-resolves if not specified.",
        ),
    ] = None,
    include_temporal_filters: Annotated[
        bool,
        strawberry.argument(
            description="Whether to include temporal partition filters (MEASURES only)",
        ),
    ] = False,
    lookback_window: Annotated[
        str | None,
        strawberry.argument(
            description="Lookback window for temporal filters (MEASURES only, e.g., '3 DAY', '1 WEEK')",
        ),
    ] = None,
    *,
    info: Info,
) -> list[GeneratedSQL]:
    """
    SQL generation endpoint using V3 builder (recommended).

    Supports MEASURES and METRICS queries (NODE not supported).
    Always returns a list of GeneratedSQL:
    - MEASURES: Multiple queries (one per grain group)
    - METRICS: Single-item list

    Uses DataLoader for efficient cache key building to minimize database load.
    """
    session = info.context["session"]
    metrics, dimensions = await resolve_metrics_and_dimensions(session, cube)

    # Validate query type
    if query_type == QueryBuildType.NODE:
        raise ValueError("NODE queries are not supported in this endpoint")

    # Resolve dialect (required for V3)
    resolved_dialect = Dialect_.SPARK  # Default
    if dialect:
        resolved_dialect = Dialect_(dialect.value)
    else:
        # Auto-resolve dialect based on cube availability
        execution_ctx = await resolve_dialect_and_engine_for_metrics(
            session=session,
            metrics=metrics,
            dimensions=dimensions,
        )
        if execution_ctx:
            resolved_dialect = execution_ctx.dialect

    if query_type == QueryBuildType.MEASURES:
        # Build measures SQL (V3)
        from datajunction_server.construction.build_v3.types import (
            GeneratedMeasuresSQL,
        )
        from datajunction_server.models.sql import GeneratedSQL as GeneratedSQL_Pydantic
        from datajunction_server.models.node_type import NodeNameVersion
        from datajunction_server.models.query import (
            ColumnMetadata as ColumnMetadata_Pydantic,
        )

        measures_result: GeneratedMeasuresSQL = await build_measures_sql(
            session=session,
            metrics=metrics,
            dimensions=dimensions,
            filters=cube.filters or [],
            dialect=resolved_dialect,
            use_materialized=use_materialized,
            include_temporal_filters=include_temporal_filters,
            lookback_window=lookback_window,
        )

        # Convert GeneratedMeasuresSQL to List[GeneratedSQL]
        # Each grain group becomes a separate GeneratedSQL
        generated_sqls = []
        for grain_group in measures_result.grain_groups:
            # Convert V3ColumnMetadata to ColumnMetadata
            converted_columns = [
                ColumnMetadata_Pydantic(
                    name=col.name,
                    type=col.type,
                    semantic_entity=col.semantic_name,
                    semantic_type=col.semantic_type,
                )
                for col in grain_group.columns
            ]

            gen_sql = GeneratedSQL_Pydantic(
                node=NodeNameVersion(name=grain_group.parent_name, version=""),
                sql=grain_group.sql,
                columns=converted_columns,
                grain=grain_group.grain,
                dialect=measures_result.dialect,
                upstream_tables=[],
                errors=[],
                scan_estimate=grain_group.scan_estimate,
            )
            generated_sqls.append(await GeneratedSQL.from_pydantic(info, gen_sql))
        return generated_sqls

    else:  # METRICS
        # Build metrics SQL (V3) - returns build_v3.GeneratedSQL (not pydantic model)
        from datajunction_server.construction.build_v3.types import (
            GeneratedSQL as GeneratedSQL_V3,
        )
        from datajunction_server.models.sql import GeneratedSQL as GeneratedSQL_Pydantic
        from datajunction_server.models.node_type import NodeNameVersion
        from datajunction_server.models.query import (
            ColumnMetadata as ColumnMetadata_Pydantic,
        )

        metrics_result: GeneratedSQL_V3 = await build_metrics_sql(
            session=session,
            metrics=metrics,
            dimensions=dimensions,
            filters=cube.filters or [],
            orderby=cube.orderby or [],
            limit=limit,
            dialect=resolved_dialect,
            use_materialized=use_materialized,
        )

        # Convert build_v3 GeneratedSQL to pydantic GeneratedSQL

        # Use cube name if available, otherwise first metric name
        node_name = metrics_result.cube_name if metrics_result.cube_name else metrics[0]

        # Convert columns (build_v3 ColumnMetadata -> pydantic ColumnMetadata)
        # build_v3: semantic_name -> pydantic: semantic_entity
        converted_columns = [
            ColumnMetadata_Pydantic(
                name=col.name,
                type=col.type,
                semantic_entity=col.semantic_name,
                semantic_type=col.semantic_type,
            )
            for col in metrics_result.columns
        ]

        gen_sql = GeneratedSQL_Pydantic(
            node=NodeNameVersion(name=node_name, version=""),
            sql=metrics_result.sql,  # This is a property that renders the AST
            columns=converted_columns,
            dialect=metrics_result.dialect,
            upstream_tables=[],
            errors=[],
            scan_estimate=metrics_result.scan_estimate,
        )
        return [await GeneratedSQL.from_pydantic(info, gen_sql)]


async def sql_v2(
    query_type: Annotated[
        QueryBuildType_GQL,
        strawberry.argument(
            description="Query type: MEASURES or METRICS (NODE not supported)",
        ),
    ],
    cube: CubeDefinition,
    use_materialized: Annotated[
        bool,
        strawberry.argument(
            description="Whether to use materialized nodes where applicable",
        ),
    ] = True,
    limit: Annotated[
        int | None,
        strawberry.argument(
            description="Maximum number of rows to return (METRICS queries only)",
        ),
    ] = None,
    engine: Annotated[
        EngineSettings | None,
        strawberry.argument(
            description="Engine settings",
        ),
    ] = None,
    include_all_columns: Annotated[
        bool,
        strawberry.argument(
            description="Whether to include all columns (MEASURES only)",
        ),
    ] = False,
    preaggregate: Annotated[
        bool,
        strawberry.argument(
            description="Whether to pre-aggregate to requested dimensions (MEASURES only)",
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
    SQL generation endpoint using V2 builder (legacy, deprecated).

    Use the `sql` query instead for V3 builder with improved performance.

    Supports MEASURES and METRICS queries (NODE not supported).
    Always returns a list of GeneratedSQL:
    - MEASURES: Multiple queries (one per grain group)
    - METRICS: Single-item list

    Uses DataLoader for efficient cache key building to minimize database load.
    """
    session = info.context["session"]
    metrics, dimensions = await resolve_metrics_and_dimensions(session, cube)

    # Validate query type
    if query_type == QueryBuildType.NODE:
        raise ValueError("NODE queries are not supported in this endpoint")

    # Use V2 builder with QueryCacheManager
    query_cache_manager = QueryCacheManager(
        cache=info.context["cache"],
        query_type=query_type,
        node_version_loader=info.context.get("node_version_loader"),
    )
    result = await query_cache_manager.get_or_load(
        info.context["background_tasks"],
        info.context["request"],
        QueryRequestParams(
            nodes=metrics,
            dimensions=dimensions,
            filters=cube.filters or [],
            engine_name=engine.name if engine else None,
            engine_version=engine.version if engine else None,
            orderby=cube.orderby or [],
            limit=limit,
            query_params=str(query_parameters) if query_parameters else None,
            include_all_columns=include_all_columns,
            preaggregate=preaggregate,
            use_materialized=use_materialized,
        ),
    )

    # Handle different return types
    # MEASURES returns list[GeneratedSQL], METRICS returns single TranslatedSQL
    if query_type == QueryBuildType.MEASURES:
        return [await GeneratedSQL.from_pydantic(info, query) for query in result]
    else:  # METRICS
        # TranslatedSQL doesn't have 'node' field, need to convert to GeneratedSQL
        from datajunction_server.models.sql import GeneratedSQL as GeneratedSQL_Pydantic
        from datajunction_server.models.node_type import NodeNameVersion

        # Use first metric name as the node name
        gen_sql = GeneratedSQL_Pydantic(
            node=NodeNameVersion(name=metrics[0], version=""),
            sql=result.sql,
            columns=result.columns or [],
            dialect=result.dialect,
            upstream_tables=result.upstream_tables or [],
            errors=[],
            scan_estimate=result.scan_estimate,
        )
        return [await GeneratedSQL.from_pydantic(info, gen_sql)]
