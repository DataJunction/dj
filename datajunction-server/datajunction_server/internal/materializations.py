"""Node materialization helper functions"""
import zlib
from typing import Dict, List, Optional, Tuple, Union

from pydantic import ValidationError
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import build_sql_for_multiple_metrics
from datajunction_server.construction.build import get_default_criteria
from datajunction_server.construction.build_v2 import QueryBuilder, get_measures_query
from datajunction_server.database.materialization import Materialization
from datajunction_server.database.node import NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import DJException, DJInvalidInputException
from datajunction_server.materialization.jobs import MaterializationJob
from datajunction_server.models import access
from datajunction_server.models.column import SemanticType
from datajunction_server.models.materialization import (
    DruidMeasuresCubeConfig,
    DruidMetricsCubeConfig,
    GenericMaterializationConfig,
    MaterializationInfo,
    MaterializationJobTypeEnum,
    Measure,
    MetricMeasures,
    UpsertMaterialization,
)
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.types import TimestampType
from datajunction_server.utils import SEPARATOR

MAX_COLUMN_NAME_LENGTH = 128


async def rewrite_metrics_expressions(
    session: AsyncSession,
    current_revision: NodeRevision,
    measures_query: TranslatedSQL,
) -> Dict[str, MetricMeasures]:
    """
    Map each metric to a rewritten version of the metric expression with the measures from
    the materialized measures table.
    """
    context = CompileContext(session, DJException())
    metrics_expressions = {}
    measures_to_output_columns_lookup = {
        column.semantic_entity: column.name
        for column in measures_query.columns  # type: ignore # pylint: disable=not-an-iterable
    }
    for metric in current_revision.cube_metrics():
        measures_for_metric = []
        metric_ast = parse(metric.current.query)
        await metric_ast.compile(context)
        for col in metric_ast.select.find_all(ast.Column):
            full_column_name = (
                col.table.dj_node.name + SEPARATOR + col.alias_or_name.name  # type: ignore
            )
            if (
                full_column_name in measures_to_output_columns_lookup
            ):  # pragma: no cover
                measures_for_metric.append(
                    Measure(
                        name=full_column_name,
                        field_name=measures_to_output_columns_lookup[full_column_name],
                        type=str(col.type),
                        agg="sum",
                    ),
                )

                col._table = None  # pylint: disable=protected-access
                col.name = ast.Name(
                    measures_to_output_columns_lookup[full_column_name],
                )
        if (
            hasattr(metric_ast.select.projection[0], "alias")
            and metric_ast.select.projection[0].alias  # type: ignore
        ):
            metric_ast.select.projection[0].alias.name = ""  # type: ignore
        if measures_for_metric:
            metrics_expressions[metric.name] = MetricMeasures(
                metric=metric.name,
                measures=measures_for_metric,
                combiner=str(metric_ast.select.projection[0]),
            )
    return metrics_expressions


async def build_cube_materialization_config(
    session: AsyncSession,
    current_revision: NodeRevision,
    upsert_input: UpsertMaterialization,
    validate_access: access.ValidateAccessFn,
    current_user: User,
) -> DruidMeasuresCubeConfig:
    """
    Builds the materialization config for a cube.

    If the job type is DRUID_METRICS_CUBE, we build an aggregation query with all metric
    aggregations and ingest this aggregated table to Druid.

    Alternatively, we build a measures query where we ingest the referenced measures for all
    selected metrics at the level of dimensions provided. This query is used to create
    an intermediate table for ingestion into an OLAP database like Druid.

    We additionally provide a metric to measures mapping that tells us both which measures
    in the query map to each selected metric and how to rewrite each metric expression
    based on the materialized measures table.
    """
    try:
        # Druid Metrics Cube (Post-Agg)
        if upsert_input.job == MaterializationJobTypeEnum.DRUID_METRICS_CUBE:
            metrics_query, _, _ = await build_sql_for_multiple_metrics(
                session=session,
                metrics=[node.name for node in current_revision.cube_metrics()],
                dimensions=current_revision.cube_dimensions(),
                use_materialized=False,
            )
            generic_config = DruidMetricsCubeConfig(
                lookback_window=upsert_input.config.lookback_window,
                node_name=current_revision.name,
                query=metrics_query.sql,
                dimensions=[
                    col.name
                    for col in metrics_query.columns  # type: ignore # pylint: disable=not-an-iterable
                    if col.semantic_type != SemanticType.METRIC
                ],
                metrics=[
                    col
                    for col in metrics_query.columns  # type: ignore # pylint: disable=not-an-iterable
                    if col.semantic_type == SemanticType.METRIC
                ],
                spark=upsert_input.config.spark,
                upstream_tables=metrics_query.upstream_tables,
                columns=metrics_query.columns,
            )
            return generic_config

        # Druid Measures Cube (Pre-Agg)
        measures_queries = await get_measures_query(
            session=session,
            metrics=[node.name for node in current_revision.cube_metrics()],
            dimensions=current_revision.cube_dimensions(),
            filters=[],
            current_user=current_user,
            validate_access=validate_access,
            cast_timestamp_to_ms=True,
        )
        for measures_query in measures_queries:
            metrics_expressions = await rewrite_metrics_expressions(
                session,
                current_revision,
                measures_query,
            )
            generic_config = DruidMeasuresCubeConfig(
                node_name=current_revision.name,
                query=measures_query.sql,
                dimensions=[
                    col.name
                    for col in measures_query.columns  # type: ignore # pylint: disable=not-an-iterable
                    if col.semantic_type == SemanticType.DIMENSION
                ],
                measures=metrics_expressions,
                spark=upsert_input.config.spark,
                upstream_tables=measures_query.upstream_tables,
                columns=measures_query.columns,
            )
        return generic_config
    except (KeyError, ValidationError, AttributeError) as exc:  # pragma: no cover
        raise DJInvalidInputException(  # pragma: no cover
            message=(
                "No change has been made to the materialization config for "
                f"node `{current_revision.name}` and job `{upsert_input.job.name}` as"
                " the config does not have valid configuration for "
                f"engine `{upsert_input.job.name}`."
            ),
        ) from exc


async def build_non_cube_materialization_config(
    session: AsyncSession,
    current_revision: NodeRevision,
    upsert: UpsertMaterialization,
) -> GenericMaterializationConfig:
    """
    Build materialization config for non-cube nodes (transforms and dimensions).
    """
    build_criteria = get_default_criteria(
        node=current_revision,
    )
    query_builder = await QueryBuilder.create(session, current_revision)
    materialization_ast = await (
        query_builder.ignore_errors().with_build_criteria(build_criteria).build()
    )
    generic_config = GenericMaterializationConfig(
        lookback_window=upsert.config.lookback_window,
        query=str(materialization_ast),
        spark=upsert.config.spark if upsert.config.spark else {},
        upstream_tables=[
            f"{current_revision.catalog.name}.{tbl.identifier()}"
            for tbl in materialization_ast.find_all(ast.Table)
        ],
        columns=[
            ColumnMetadata(name=col.name, type=str(col.type))
            for col in current_revision.columns
        ],
    )
    return generic_config


async def create_new_materialization(
    session: AsyncSession,
    current_revision: NodeRevision,
    upsert: UpsertMaterialization,
    validate_access: access.ValidateAccessFn,
    current_user: User,
) -> Materialization:
    """
    Create a new materialization based on the input values.
    """
    generic_config = None
    try:
        await session.refresh(current_revision, ["columns"])
    except InvalidRequestError:
        pass
    temporal_partition = current_revision.temporal_partition_columns()
    timestamp_columns = [
        col for col in current_revision.columns if col.type == TimestampType()
    ]
    if current_revision.type in (
        NodeType.DIMENSION,
        NodeType.TRANSFORM,
    ):
        generic_config = await build_non_cube_materialization_config(
            session,
            current_revision,
            upsert,
        )

    categorical_partitions = current_revision.categorical_partition_columns()
    if current_revision.type == NodeType.CUBE:
        if not temporal_partition and not timestamp_columns:
            raise DJInvalidInputException(
                "The cube materialization cannot be configured if there is no "
                "temporal partition specified on the cube. Please make sure at "
                "least one cube element has a temporal partition defined",
            )
        generic_config = await build_cube_materialization_config(
            session,
            current_revision,
            upsert,
            validate_access,
            current_user=current_user,
        )
    materialization_name = (
        f"{upsert.job.name.lower()}__{upsert.strategy.name.lower()}"
        + (f"__{temporal_partition[0].name}" if temporal_partition else "")
        + ("__" if categorical_partitions else "")
        + ("__".join([partition.name for partition in categorical_partitions]))
    )
    return Materialization(
        name=materialization_name,
        node_revision=current_revision,
        config=generic_config.dict(),  # type: ignore
        schedule=upsert.schedule or "@daily",
        strategy=upsert.strategy,
        job=upsert.job.value.job_class,  # type: ignore
    )


async def schedule_materialization_jobs(
    session: AsyncSession,
    node_revision_id: int,
    materialization_names: List[str],
    query_service_client: QueryServiceClient,
    request_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, MaterializationInfo]:
    """
    Schedule recurring materialization jobs
    """
    materializations = await Materialization.get_by_names(
        session,
        node_revision_id,
        materialization_names,
    )
    materialization_jobs = {
        cls.__name__: cls for cls in MaterializationJob.__subclasses__()
    }
    materialization_to_output = {}
    for materialization in materializations:
        clazz = materialization_jobs.get(materialization.job)
        if clazz and materialization.name:  # pragma: no cover
            materialization_to_output[materialization.name] = clazz().schedule(  # type: ignore
                materialization,
                query_service_client,
                request_headers=request_headers,
            )
    return materialization_to_output


def _get_readable_name(expr):
    """
    Returns a readable name based on the columns in the expression. This is used
    if we want to represent the expression as a single measure, which needs a name
    """
    columns = [col for arg in expr.args for col in arg.find_all(ast.Column)]
    readable_name = "_".join(
        str(col.alias_or_name).rsplit(".", maxsplit=1)[-1] for col in columns
    )
    return (
        readable_name[: MAX_COLUMN_NAME_LENGTH - 28]
        + str(zlib.crc32(readable_name.encode("utf-8")))
        if columns
        else "placeholder"
    )


def decompose_expression(  # pylint: disable=too-many-return-statements
    expr: Union[ast.Aliasable, ast.Expression],
) -> Tuple[ast.Expression, List[ast.Alias]]:
    """
    Takes a metric expression and (a) determines the measures needed to evaluate
    the metric and (b) includes the query expression needed to recombine these
    measures into the metric, given a materialized cube.

    Simple aggregations are operations that can be computed incrementally as new
    data is ingested, without relying on the results of other aggregations.
    Examples include SUM, COUNT, MIN, MAX.

    Some complex aggregations can be decomposed to simple aggregations: i.e., AVG(x) can
    be decomposed to SUM(x)/COUNT(x).
    """
    if isinstance(expr, ast.Alias):
        expr = expr.child  # pragma: no cover

    if isinstance(expr, ast.Number):
        return expr, []  # type: ignore

    if not expr.is_aggregation():  # type: ignore  # pragma: no cover
        return expr, [expr]  # type: ignore

    simple_aggregations = {"sum", "count", "min", "max"}
    if isinstance(expr, ast.Function):
        function_name = expr.alias_or_name.name.lower()
        readable_name = _get_readable_name(expr)

        if function_name in simple_aggregations:
            measure_name = ast.Name(f"{readable_name}_{function_name}")
            if not expr.args[0].is_aggregation():
                combiner: ast.Expression = ast.Function(
                    name=ast.Name(function_name),
                    args=[ast.Column(name=measure_name)],
                )
                return combiner, [expr.set_alias(measure_name)]

            combiner, measures = decompose_expression(expr.args[0])
            return (
                ast.Function(
                    name=ast.Name(function_name),
                    args=[combiner],
                ),
                measures,
            )

        if function_name == "avg":  # pragma: no cover
            numerator_measure_name = ast.Name(f"{readable_name}_sum")
            denominator_measure_name = ast.Name(f"{readable_name}_count")
            combiner = ast.BinaryOp(
                left=ast.Function(
                    ast.Name("sum"),
                    args=[ast.Column(name=numerator_measure_name)],
                ),
                right=ast.Function(
                    ast.Name("count"),
                    args=[ast.Column(name=denominator_measure_name)],
                ),
                op=ast.BinaryOpKind.Divide,
            )
            return combiner, [
                (
                    ast.Function(ast.Name("sum"), args=expr.args).set_alias(
                        numerator_measure_name,
                    )
                ),
                (
                    ast.Function(ast.Name("count"), args=expr.args).set_alias(
                        denominator_measure_name,
                    )
                ),
            ]
    acceptable_binary_ops = {
        ast.BinaryOpKind.Plus,
        ast.BinaryOpKind.Minus,
        ast.BinaryOpKind.Multiply,
        ast.BinaryOpKind.Divide,
    }
    if isinstance(expr, ast.BinaryOp):
        if expr.op in acceptable_binary_ops:  # pragma: no cover
            measures_combiner_left, measures_left = decompose_expression(expr.left)
            measures_combiner_right, measures_right = decompose_expression(expr.right)
            combiner = ast.BinaryOp(
                left=measures_combiner_left,
                right=measures_combiner_right,
                op=expr.op,
            )
            return combiner, measures_left + measures_right

    if isinstance(expr, ast.Cast):
        return decompose_expression(expr.expression)

    raise DJInvalidInputException(  # pragma: no cover
        f"Metric expression {expr} cannot be decomposed into its constituent measures",
    )
