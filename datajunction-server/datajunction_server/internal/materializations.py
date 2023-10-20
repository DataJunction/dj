"""Node materialization helper functions"""
import zlib
from typing import Dict, List, Set, Tuple, Union

from pydantic import ValidationError
from sqlmodel import Session

from datajunction_server.api.helpers import get_engine
from datajunction_server.construction.build import build_node
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.materialization.jobs import (
    DefaultCubeMaterialization,
    DruidCubeMaterializationJob,
    MaterializationJob,
    SparkSqlMaterializationJob,
    TrinoMaterializationJob,
)
from datajunction_server.models import Engine, NodeRevision
from datajunction_server.models.engine import Dialect
from datajunction_server.models.materialization import (
    DruidCubeConfig,
    GenericCubeConfig,
    GenericMaterializationConfig,
    Materialization,
    MaterializationInfo,
    Measure,
    MetricMeasures,
    UpsertMaterialization,
)
from datajunction_server.models.node import NodeType
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing import ast

MAX_COLUMN_NAME_LENGTH = 128


def build_cube_config(  # pylint: disable=too-many-locals
    cube_node: NodeRevision,
    combined_ast: ast.Query,
) -> Union[DruidCubeConfig, GenericCubeConfig]:
    """
    Builds the materialization config for a cube. This includes two parts:
    (a) building a query that decomposes each of the cube's metrics into their
    constituent measures that have simple aggregations
    (b) adding a metric to measures mapping that tells us which measures
    in the query map to each selected metric

    The query in the materialization config is different from the one stored
    on the cube node itself in that this one is meant to create a temporary
    table in preparation for ingestion into an OLAP database like Druid. The
    metric to measures mapping then provides information on how to assemble
    metrics from measures based on this query's output.

    The query directly on the cube node is meant for direct querying of the cube
    without materialization to an OLAP database.
    """
    dimensions_set = {
        dim.name for dim in cube_node.columns if dim.has_dimension_attribute()
    }
    metrics_to_measures = {}
    measures_tracker = {}

    # Track columns to return later as a part of the default config
    intermediate_columns = {
        ColumnMetadata(name=dim.name, type=str(dim.type))
        for dim in cube_node.columns
        if dim.has_dimension_attribute()
    }
    for cte in combined_ast.ctes:
        metrics_to_measures.update(decompose_metrics(cte, dimensions_set))
        new_select_projection: Set[Union[ast.Aliasable, ast.Expression]] = set()
        for expr in cte.select.projection:
            if expr in metrics_to_measures:
                combiner, measures = metrics_to_measures[expr]  # type: ignore
                new_select_projection = set(new_select_projection).union(
                    measures,
                )
                metric_key = expr.alias_or_name.name  # type: ignore
                if metric_key not in measures_tracker:  # pragma: no cover
                    measures_tracker[metric_key] = MetricMeasures(
                        metric=metric_key,
                        measures=[],
                        combiner=str(combiner),
                    )
                for measure in measures:
                    intermediate_columns.add(
                        ColumnMetadata(
                            name=str(
                                f"{cte.alias_or_name}_{measure.alias_or_name}",
                            ),
                            type=str(measure.type),
                        ),
                    )
                    measures_tracker[metric_key].measures.append(  # type: ignore
                        Measure(
                            name=measure.alias_or_name.name,
                            field_name=str(
                                f"{cte.alias_or_name}_{measure.alias_or_name}",
                            ),
                            agg=str(measure.child.name),
                            type=str(measure.type),
                        ),
                    )
            else:
                new_select_projection.add(expr)
        cte.select.projection = list(new_select_projection)
    for _, metric_measures in measures_tracker.items():
        metric_measures.measures = sorted(
            metric_measures.measures,
            key=lambda x: x.name,
        )
    combined_ast.select.projection = [
        (
            ast.Column(name=col.alias_or_name, _table=cte).set_alias(  # type: ignore
                ast.Name(f"{cte.alias_or_name}_{col.alias_or_name}"),  # type: ignore
            )
        )
        for cte in combined_ast.ctes
        for col in cte.select.projection
        if col.alias_or_name.name not in dimensions_set  # type: ignore
    ]
    dimension_grouping: Dict[str, List[ast.Column]] = {}
    for col in [
        ast.Column(name=col.alias_or_name, _table=cte)  # type: ignore
        for cte in combined_ast.ctes
        for col in cte.select.projection
        if col.alias_or_name.name in dimensions_set  # type: ignore
    ]:
        dimension_grouping.setdefault(str(col.alias_or_name.name), []).append(col)

    for col_name, columns in dimension_grouping.items():
        combined_ast.select.projection.append(
            ast.Function(name=ast.Name("COALESCE"), args=list(columns)).set_alias(
                ast.Name(col_name),
            ),
        )

    upstream_tables = sorted(
        list(
            {
                f"{tbl.dj_node.catalog.name}.{tbl.identifier()}"
                for tbl in combined_ast.find_all(ast.Table)
                if tbl.dj_node
            },
        ),
    )
    ordering = {
        col.alias_or_name.name: idx  # type: ignore
        for idx, col in enumerate(combined_ast.select.projection)
    }
    return GenericCubeConfig(
        query=str(combined_ast),
        columns=sorted(list(intermediate_columns), key=lambda x: ordering[x.name]),
        dimensions=sorted(list(dimensions_set)),
        measures=measures_tracker,
        partitions=[],
        upstream_tables=upstream_tables,
    )


def materialization_job_from_engine(engine: Engine) -> MaterializationJob:
    """
    Finds the appropriate materialization job based on the choice of engine.
    """
    engine_to_job_mapping = {
        Dialect.SPARK: SparkSqlMaterializationJob,
        Dialect.TRINO: TrinoMaterializationJob,
        Dialect.DRUID: DruidCubeMaterializationJob,
        None: SparkSqlMaterializationJob,
    }
    if engine.dialect not in engine_to_job_mapping:
        raise DJInvalidInputException(  # pragma: no cover
            f"The engine used for materialization ({engine.name}) "
            "must have a dialect configured.",
        )
    return engine_to_job_mapping[engine.dialect]  # type: ignore


def create_new_materialization(
    session: Session,
    current_revision: NodeRevision,
    upsert: UpsertMaterialization,
) -> Materialization:
    """
    Create a new materialization based on the input values.
    """
    generic_config = None
    engine = get_engine(session, upsert.engine.name, upsert.engine.version)
    temporal_partition = current_revision.temporal_partition_columns()
    if current_revision.type in (
        NodeType.DIMENSION,
        NodeType.TRANSFORM,
        NodeType.METRIC,
    ):
        materialization_ast = build_node(
            session=session,
            node=current_revision,
            dimensions=[],
            orderby=[],
        )
        generic_config = GenericMaterializationConfig(
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

    if current_revision.type == NodeType.CUBE:
        # Check to see if a default materialization was already configured, so that we
        # can copy over the default cube setup and layer on specific config as needed
        if not temporal_partition:
            raise DJInvalidInputException(
                "The cube materialization cannot be configured if there is no "
                "temporal partition specified on the cube. Please make sure at "
                "least one cube element has a temporal partition defined",
            )
        default_job = [
            conf
            for conf in current_revision.materializations
            if conf.job == DefaultCubeMaterialization.__name__
        ][0]
        default_job_config = GenericCubeConfig.parse_obj(default_job.config)
        try:
            generic_config = DruidCubeConfig(
                node_name=current_revision.name,
                query=default_job_config.query,
                dimensions=default_job_config.dimensions,
                measures=default_job_config.measures,
                spark=upsert.config.spark,
                upstream_tables=default_job_config.upstream_tables,
                columns=default_job_config.columns,
            )
        except (KeyError, ValidationError, AttributeError) as exc:  # pragma: no cover
            raise DJInvalidInputException(  # pragma: no cover
                message=(
                    "No change has been made to the materialization config for "
                    f"node `{current_revision.name}` and engine `{engine.name}` as"
                    " the config does not have valid configuration for "
                    f"engine `{engine.name}`."
                ),
            ) from exc
    materialization_name = (
        f"{temporal_partition[0].name}_{engine.name}"
        if temporal_partition
        else engine.name
    )
    return Materialization(
        name=materialization_name,
        node_revision=current_revision,
        engine=engine,
        config=generic_config,
        schedule=upsert.schedule or "@daily",
        job=materialization_job_from_engine(engine).__name__,  # type: ignore
    )


def schedule_materialization_jobs(
    materializations: List[Materialization],
    query_service_client: QueryServiceClient,
) -> Dict[str, MaterializationInfo]:
    """
    Schedule recurring materialization jobs
    """
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
        expr = expr.child

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


def decompose_metrics(
    combined_ast: ast.Query,
    dimensions_set: Set[str],
) -> Dict[Union[ast.Aliasable, ast.Expression], Tuple[ast.Expression, List[ast.Alias]]]:
    """
    Decompose each metric into simple constituent measures and return a dict
    that maps each metric to its measures.
    """
    metrics_to_measures = {}
    for expr in combined_ast.select.projection:
        if expr.alias_or_name.name not in dimensions_set:  # type: ignore
            metrics_to_measures[expr] = decompose_expression(expr)
    return metrics_to_measures
