"""Helper functions related to cube materializations."""

import itertools
from types import SimpleNamespace

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.construction.build_v3.builder import build_measures_sql
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    DecomposedMetricInfo,
    GrainGroupSQL,
)
from datajunction_server.database.node import Column, NodeRevision
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.column import SemanticType
from datajunction_server.models.cube_materialization import (
    CombineMaterialization,
    CubeMetric,
    DruidCubeConfig,
    MeasureKey,
    MeasuresMaterialization,
    UpsertCubeMaterialization,
)
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.partition import Granularity
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.sql.parsing import ast
from datajunction_server.utils import SEPARATOR


def generate_partition_filter_sql(
    temporal_partition: Column,
    lookback_window: str,
) -> str:
    """
    Generate filter SQL on partitions
    """
    logical_ts = "CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP)"

    def _partition_sql(timestamp_expression, column_type):
        return f"CAST(DATE_FORMAT({timestamp_expression}, 'yyyyMMdd') AS {column_type})"

    partition_sql = _partition_sql(logical_ts, str(temporal_partition.type))
    if temporal_partition.partition.granularity == Granularity.DAY and (
        lookback_window == "1 DAY" or not lookback_window
    ):
        return f"{temporal_partition.name} = {partition_sql}"
    lookback_timestamp = (
        f"{logical_ts} - INTERVAL {lookback_window}"  # pragma: no cover
    )
    partition_start = _partition_sql(  # pragma: no cover
        lookback_timestamp,
        str(temporal_partition.type),
    )
    return (  # pragma: no cover
        f"{temporal_partition.name} BETWEEN {partition_start} AND {partition_sql}"
    )


def combine_measures_on_shared_grain(
    measures_materializations: list[MeasuresMaterialization],
    query_grain: list[str],
) -> ast.Query:
    """
    Generate a query that combines measures datasets on their shared grain.
    An example:
      SELECT
        COALESCE(measureA.grain1, measureB.grain1) grain1,
        COALESCE(measureA.grain2, measureB.grain2) grain2,
        measureA.one,
        measureA.two,
        measureB.three,
        measureB.four
      FROM measureA
      JOIN measureB ON
        measureA.grain1 = measureB.grain1 AND
        measureA.grain2 = measureB.grain2
    """
    measures_tables = {
        mat.output_table_name: mat.table_ast() for mat in measures_materializations
    }
    initial_mat = measures_materializations[0]

    # Coalesce grain fields
    grain_fields = [
        ast.Function(
            name=ast.Name("COALESCE"),
            args=[
                ast.Column(
                    name=ast.Name(grain),
                    _table=measures_tables.get(mat.output_table_name),
                )
                for mat in measures_materializations
            ],
        ).set_alias(alias=ast.Name(grain))
        for grain in query_grain
    ]
    measures_fields = [
        ast.Column(
            name=ast.Name(measure.name),
            _table=measures_tables.get(mat.output_table_name),
            semantic_type=SemanticType.MEASURE,
        )
        for mat in measures_materializations
        for measure in mat.measures
    ]
    from_relation = ast.Relation(
        primary=measures_tables.get(initial_mat.output_table_name),  # type: ignore
        extensions=[
            ast.Join(
                join_type="FULL OUTER",
                right=ast.Table(name=ast.Name(measures_tables[mat.output_table_name])),
                criteria=ast.JoinCriteria(
                    on=_combine_measures_join_criteria(
                        measures_tables[initial_mat.output_table_name],
                        measures_tables[mat.output_table_name],
                        query_grain,
                    ),
                ),
            )
            for mat in measures_materializations[1:]
        ],
    )
    return ast.Query(
        select=ast.Select(
            projection=grain_fields + measures_fields,  # type: ignore
            from_=ast.From(relations=[from_relation]),
        ),
    )


def _combine_measures_join_criteria(left_table, right_table, query_grain):
    """
    Generate the join condition across tables for shared grains.
    """
    return ast.BinaryOp.And(
        *[
            ast.BinaryOp.Eq(
                ast.Column(name=ast.Name(grain), _table=left_table),
                ast.Column(name=ast.Name(grain), _table=right_table),
            )
            for grain in query_grain
        ],
    )


def _extract_expression(metric_query: str) -> str:
    """
    Extract only the derived metric expression from a metric query.
    """
    expression = parse(metric_query).select.projection[0]
    return str(
        expression.child
        if isinstance(expression, ast.Alias)
        else expression.without_aliases()
        if isinstance(expression, ast.Expression)
        else expression,
    )


def _v3_col_to_model_column(col) -> ColumnMetadata:
    """
    Convert a v3 ``ColumnMetadata`` (dataclass keyed on ``semantic_name``) into
    the persisted ``models.query.ColumnMetadata`` (BaseModel keyed on
    ``semantic_entity``) shape that ``MeasuresMaterialization.from_measures_query``
    consumes. v3's ``"metric"`` / ``"metric_component"`` / ``"metric_input"``
    semantic types collapse to v2's ``"measure"`` so the downstream
    ``SemanticType.DIMENSION`` / ``"measure"`` branching keeps working.
    """
    semantic_entity = col.semantic_name
    column_name = None
    node_name = None
    if semantic_entity and SEPARATOR in semantic_entity:
        column_name = semantic_entity.rsplit(SEPARATOR, 1)[-1]
        node_name = semantic_entity.rsplit(SEPARATOR, 1)[0]
    v3_type = col.semantic_type
    semantic_type = (
        "measure"
        if v3_type in ("metric", "metric_component", "metric_input")
        else v3_type
    )
    return ColumnMetadata(
        name=col.name,
        type=col.type,
        column=column_name,
        node=node_name,
        semantic_entity=semantic_entity,
        semantic_type=semantic_type,
    )


async def _v3_grain_group_to_measures_query(
    session: AsyncSession,
    gg: GrainGroupSQL,
    ctx: BuildContext,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
):
    """
    Adapt a v3 ``GrainGroupSQL`` into the v2 measures-query shape so the
    rest of ``build_cube_materialization`` (and
    ``MeasuresMaterialization.from_measures_query``) can stay unchanged.
    Async because we may need to refresh expired ORM attributes on the parent
    fact / source nodes that v3 left lazily-loaded.
    """
    from datajunction_server.models.node_type import NodeType  # noqa: PLC0415
    from datajunction_server.utils import refresh_if_needed  # noqa: PLC0415

    parent_node = ctx.nodes.get(gg.parent_name)
    if not parent_node or not parent_node.current:  # pragma: no cover
        raise DJInvalidInputException(
            f"Parent node `{gg.parent_name}` not found in build context",
        )
    rev = parent_node.current
    # v3's ``ctx.nodes`` may carry NodeRevisions whose scalar attrs got
    # expired by an earlier ``session.commit()`` in the calling endpoint.
    # Pull what we need before reading.
    await refresh_if_needed(session, rev, ["name", "version", "display_name"])

    # v3 doesn't expose the per-grain-group source set on the public
    # ``GrainGroupSQL`` (it's tracked internally for scan-estimation only).
    # Walk ``ctx.parent_map`` from this grain group's parent fact and
    # collect any ``SOURCE`` ancestors so the cube workflow gets the same
    # ``catalog.schema.table`` upstream list v2 produced.
    upstream_tables: list[str] = []
    seen_sources: set[str] = set()
    visited: set[str] = set()

    async def _walk(node_name: str) -> None:
        if node_name in visited:
            return
        visited.add(node_name)
        node = ctx.nodes.get(node_name)
        if not node or not node.current:
            return
        if node.type == NodeType.SOURCE:
            srev = node.current
            await refresh_if_needed(
                session,
                srev,
                ["catalog", "schema_", "table"],
            )
            if (
                node.name not in seen_sources
                and srev.catalog
                and srev.schema_
                and srev.table
            ):
                seen_sources.add(node.name)
                upstream_tables.append(
                    f"{srev.catalog.name}.{srev.schema_}.{srev.table}",
                )
            return
        for parent_name in ctx.parent_map.get(node_name, []):
            await _walk(parent_name)

    await _walk(gg.parent_name)
    # Dimension columns are joined in via dimension links rather than parent
    # edges, so their source tables don't appear under ``gg.parent_name``'s
    # ``parent_map``. Walk each dimension column's owning node too so the
    # upstream-table list reflects everything the materialization SQL reads.
    for col in gg.columns:
        if col.semantic_type != "dimension":
            continue
        sem = col.semantic_name
        if not sem or SEPARATOR not in sem:
            continue
        # Strip optional ``[role]`` suffix to get the bare ``node.column``.
        bare_sem = sem.split("[", 1)[0]
        dim_node_name = bare_sem.rsplit(SEPARATOR, 1)[0]
        await _walk(dim_node_name)

    metrics: dict[str, tuple[list, str]] = {}
    for metric_name in gg.metrics:
        info = decomposed_metrics.get(metric_name)
        if info is None:  # pragma: no cover
            continue
        # v2's "combiner" is the full derived SELECT statement (e.g. ``SELECT
        # SUM(...) FROM ...``); ``_extract_expression`` and the V1 cube config's
        # ``derived_expression`` field both rely on that shape. v3 exposes the
        # full query AST separately on ``derived_ast``; stringify it so the
        # downstream parser-based extraction stays happy.
        metrics[metric_name] = (info.components, str(info.derived_ast))

    return SimpleNamespace(
        node=NodeNameVersion(
            name=rev.name,
            version=rev.version,
            display_name=rev.display_name,
        ),
        grain=list(gg.grain),
        columns=[_v3_col_to_model_column(c) for c in gg.columns],
        metrics=metrics,
        sql=gg.sql,
        spark_conf=None,
        upstream_tables=upstream_tables,
    )


async def build_cube_materialization(
    session: AsyncSession,
    current_revision: NodeRevision,
    upsert_input: UpsertCubeMaterialization,
) -> DruidCubeConfig:
    """
    Build the full config needed for a Druid cube materialization
    """
    temporal_partitions = current_revision.temporal_partition_columns()
    temporal_partition = temporal_partitions[0]
    incremental = upsert_input.strategy == MaterializationStrategy.INCREMENTAL_TIME
    extra_filters = (
        [
            generate_partition_filter_sql(
                temporal_partition,
                upsert_input.lookback_window,  # type: ignore
            ),
        ]
        if incremental
        else []
    )
    result = await build_measures_sql(
        session=session,
        metrics=current_revision.cube_node_metrics,
        dimensions=current_revision.cube_node_dimensions,
        filters=(current_revision.cube_filters or []) + extra_filters,
        dialect=Dialect.SPARK,
        use_materialized=True,
    )
    measures_queries = [
        await _v3_grain_group_to_measures_query(
            session,
            gg,
            result.ctx,
            result.decomposed_metrics,
        )
        for gg in result.grain_groups
    ]
    query_grains = {
        k: [q.node.name for q in queries]
        for k, queries in itertools.groupby(
            measures_queries,
            lambda query: tuple(query.grain),  # type: ignore
        )
    }
    if len(query_grains) > 1:
        raise DJInvalidInputException(  # pragma: no cover
            "DJ cannot manage materializations for cubes that have underlying "
            "measures queries at different grains: "
            + " vs ".join(
                f"{', '.join(query_nodes)} at [{', '.join(grain)}]"
                for grain, query_nodes in query_grains.items()
            ),
        )
    measures_materializations = [
        MeasuresMaterialization.from_measures_query(measures_query, temporal_partition)
        for measures_query in measures_queries
    ]

    # Combine the queries on the shared query grain
    query_grain = next(iter(query_grains))
    if len(measures_materializations) == 1:
        measures_materialization = measures_materializations[0]
        combiners = [
            CombineMaterialization(
                node=current_revision,
                output_table_name=measures_materialization.output_table_name,
                columns=measures_materialization.columns,
                grain=measures_materialization.grain,
                measures=measures_materialization.measures,
                dimensions=measures_materialization.dimensions,
                timestamp_column=measures_materialization.timestamp_column,
                timestamp_format=measures_materialization.timestamp_format,
                granularity=measures_materialization.granularity,
                upstream_tables=[measures_materialization.output_table_name],
            ),
        ]
    if len(measures_materializations) > 1:
        measures_materialization = measures_materializations[0]
        combiner_query = combine_measures_on_shared_grain(
            measures_materializations,
            query_grain,  # type: ignore
        )
        columns_metadata_lookup = {
            col.name: col
            for mat in measures_materializations
            for col in mat.columns
            if col.name in combiner_query.select.column_mapping
        }
        measures_lookup = {
            measure.name: measure
            for mat in measures_materializations
            for measure in mat.measures
        }
        combiners = [
            CombineMaterialization(
                node=current_revision,
                query=str(combiner_query),
                columns=[
                    columns_metadata_lookup.get(col.alias_or_name.name)  # type: ignore
                    for col in combiner_query.select.projection
                ],
                grain=query_grain,
                measures=[
                    measures_lookup.get(col.alias_or_name.name)  # type: ignore
                    for col in combiner_query.select.projection
                    if col.semantic_type == SemanticType.MEASURE  # type: ignore
                ],
                dimensions=query_grain,
                timestamp_column=measures_materialization.timestamp_column,
                timestamp_format=measures_materialization.timestamp_format,
                granularity=measures_materialization.granularity,
            ),
        ]

    metrics_mapping = {
        metric: (measures_query.node, measures)
        for measures_query in measures_queries
        for metric, measures in measures_query.metrics.items()  # type: ignore
    }
    config = DruidCubeConfig(
        cube=NodeNameVersion(
            name=current_revision.name,
            version=current_revision.version,
            display_name=current_revision.display_name,
        ),
        metrics=[
            CubeMetric(
                metric=NodeNameVersion(
                    name=metric.name,
                    version=metric.current_version,
                    display_name=metric.current.display_name,
                ),
                required_measures=[
                    MeasureKey(
                        node=metrics_mapping.get(metric.name)[0],  # type: ignore
                        measure_name=measure.name,
                    )
                    for measure in metrics_mapping.get(metric.name)[1][0]  # type: ignore
                ],
                derived_expression=metrics_mapping.get(metric.name)[1][1],  # type: ignore
                metric_expression=_extract_expression(
                    metrics_mapping.get(metric.name)[1][1],  # type: ignore
                ),
            )
            for metric in current_revision.cube_metrics()
        ],
        dimensions=current_revision.cube_node_dimensions,
        measures_materializations=measures_materializations,
        combiners=combiners,
    )
    return config
