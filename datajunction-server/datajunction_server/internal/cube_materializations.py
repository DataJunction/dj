"""Helper functions related to cube materializations."""

import itertools

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.construction.build_v2 import get_measures_query
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
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.partition import Granularity
from datajunction_server.sql.parsing import ast


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
    measures_queries = await get_measures_query(
        session=session,
        metrics=current_revision.cube_node_metrics,
        dimensions=current_revision.cube_node_dimensions,
        filters=[
            generate_partition_filter_sql(
                temporal_partition,
                upsert_input.lookback_window,  # type: ignore
            ),
        ],
        include_all_columns=False,
        use_materialized=True,
        preagg_requested=True,
    )
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
                node=measures_materialization.node,
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
