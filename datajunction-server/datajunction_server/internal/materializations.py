"""Node materialization helper functions"""

import logging
import zlib
from dataclasses import dataclass
from datetime import UTC

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build import get_default_criteria
from datajunction_server.construction.build_v2 import QueryBuilder
from datajunction_server.database.history import (
    ActivityType,
    EntityType,
    History,
)
from datajunction_server.database.materialization import Materialization
from datajunction_server.database.node import NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import DJException, DJInvalidInputException
from datajunction_server.internal.access.authorization import AccessChecker
from datajunction_server.internal.cube_materializations import (
    build_cube_materialization,
)
from datajunction_server.internal.sql import (
    build_sql_for_multiple_metrics,
    get_measures_query,
)
from datajunction_server.materialization.jobs import MaterializationJob
from datajunction_server.models.column import SemanticType
from datajunction_server.models.cube_materialization import UpsertCubeMaterialization
from datajunction_server.models.deployment import MaterializationSpec
from datajunction_server.models.materialization import (
    DruidCubeConfigInput,
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
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import SEPARATOR, session_context

MAX_COLUMN_NAME_LENGTH = 128
_logger = logging.getLogger(__name__)


async def rewrite_metrics_expressions(
    session: AsyncSession,
    current_revision: NodeRevision,
    measures_query: TranslatedSQL,
) -> dict[str, MetricMeasures]:
    """
    Map each metric to a rewritten version of the metric expression with the measures from
    the materialized measures table.
    """
    context = CompileContext(session, DJException())
    metrics_expressions = {}
    measures_to_output_columns_lookup = {
        column.semantic_entity: column.name
        for column in measures_query.columns  # type: ignore
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

                col._table = None
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
    access_checker: AccessChecker,
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
                access_checker=access_checker,
            )
            generic_config = DruidMetricsCubeConfig(
                lookback_window=upsert_input.config.lookback_window,
                node_name=current_revision.name,
                query=metrics_query.sql,
                dimensions=[
                    col.name
                    for col in metrics_query.columns  # type: ignore
                    if col.semantic_type != SemanticType.METRIC
                ],
                metrics=[
                    col
                    for col in metrics_query.columns  # type: ignore
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
            access_checker=access_checker,
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
                    for col in measures_query.columns  # type: ignore
                    if col.semantic_type == SemanticType.DIMENSION
                ],
                measures=metrics_expressions,
                spark=(
                    getattr(getattr(upsert_input.config, "spark", None), "root", {})
                    if upsert_input.config
                    else {}
                ),
                upstream_tables=measures_query.upstream_tables,
                columns=measures_query.columns,
                lookback_window=getattr(upsert_input, "lookback_window", None),
            )
        return generic_config
    except (KeyError, ValidationError, AttributeError) as exc:  # pragma: no cover
        _logger.exception(exc)
        raise DJInvalidInputException(  # pragma: no cover
            message=(
                "No change has been made to the materialization config for "
                f"node `{current_revision.name}` and job "
                f"`{upsert_input.job.name}` as"  # type: ignore
                " the config does not have valid configuration for "
                f"engine `{upsert_input.job.name}`."  # type: ignore
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
    _logger.info(
        "Building materialization config for node=%s node_type=%s %s",
        current_revision.name,
        current_revision.type,
        upsert,
    )
    build_criteria = get_default_criteria(
        node=current_revision,
    )
    query_builder = await QueryBuilder.create(session, current_revision)
    materialization_ast = await (
        query_builder.ignore_errors().with_build_criteria(build_criteria).build()
    )
    generic_config = GenericMaterializationConfig(
        lookback_window=getattr(upsert.config, "lookback_window", None),
        query=str(materialization_ast),
        spark=getattr(upsert.config, "spark", None) if upsert.config else {},
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
    upsert: UpsertCubeMaterialization | UpsertMaterialization,
    access_checker: AccessChecker,
    current_user: User,
) -> Materialization:
    """
    Create a new materialization based on the input values.
    """
    generic_config = None
    try:
        await session.refresh(current_revision, ["columns"])
    except InvalidRequestError:  # pragma: no cover
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

        # Druid Cube (this job will subsume all existing cube materialization types)
        if upsert.job == MaterializationJobTypeEnum.DRUID_CUBE:
            generic_config = await build_cube_materialization(
                session=session,
                current_revision=current_revision,
                upsert_input=upsert,
            )
        else:
            generic_config = await build_cube_materialization_config(
                session,
                current_revision,
                upsert,
                access_checker,
                current_user=current_user,
            )
    materialization_name = _materialization_name(
        upsert,
        current_revision,
        temporal_partition,
        categorical_partitions,
    )
    return Materialization(
        name=materialization_name,
        node_revision=current_revision,
        config=generic_config.model_dump(),  # type: ignore
        schedule=upsert.schedule or "@daily",
        strategy=upsert.strategy,
        job=upsert.job.value.job_class,  # type: ignore
    )


def _materialization_name(
    upsert: UpsertCubeMaterialization | UpsertMaterialization,
    current_revision: NodeRevision,
    temporal_partitions: list,
    categorical_partitions: list,
) -> str:
    """Build a materialization identity that preserves cube partition roles."""
    partition_names = [
        partition.cube_element_name
        if current_revision.type == NodeType.CUBE
        else partition.name
        for partition in temporal_partitions + categorical_partitions
    ]
    return (
        f"{upsert.job.name.lower()}__{upsert.strategy.name.lower()}"  # type: ignore
        + (f"__{partition_names[0]}" if temporal_partitions else "")
        + ("__" if categorical_partitions else "")
        + ("__".join(partition_names[len(temporal_partitions) :]))
    )


def _upsert_from_materialization(
    materialization: Materialization,
) -> UpsertCubeMaterialization | UpsertMaterialization:
    """
    Recover the user's materialization intent from a persisted materialization.

    Only the intent -- job type, strategy, schedule and lookback window -- is
    carried over; everything else in a stored config is generated content derived
    from the revision it was built against.
    """
    config = materialization.config if isinstance(materialization.config, dict) else {}
    lookback_window = config.get("lookback_window")
    job_type = MaterializationJobTypeEnum.find_match(materialization.job)
    if job_type == MaterializationJobTypeEnum.DRUID_CUBE:
        return UpsertCubeMaterialization(
            job=job_type.value.name,  # type: ignore
            strategy=materialization.strategy,
            schedule=materialization.schedule,
            lookback_window=lookback_window,
        )
    return UpsertMaterialization(
        job=job_type.value.name,  # type: ignore
        strategy=materialization.strategy,
        schedule=materialization.schedule,
        config=DruidCubeConfigInput(
            spark=config.get("spark") or {},
            lookback_window=lookback_window,
        ),
    )


@dataclass
class CubeMaterializationSwap:
    """
    The query service work a cube materialization swap still owes, once DJ's own
    record of the swap is durable.
    """

    cube_name: str
    previous_version: str
    new_revision_id: int
    new_version: str
    rebuilt_names: list[str]
    superseded: list[Materialization]


def _apply_declared_spec(
    upsert: UpsertCubeMaterialization | UpsertMaterialization,
    declared: MaterializationSpec,
) -> UpsertCubeMaterialization | UpsertMaterialization:
    """
    Let a cube's declared YAML block win over the intent recovered from the
    persisted materialization.

    A cube that declares `materialization:` has its config in the repo, so a rebuild
    triggered by the same deploy must build what the YAML now says rather than what
    the superseded revision happened to be built with -- otherwise editing a metric
    and the schedule in one push would rebuild with the old schedule. Only cube
    materializations can be declared; anything else keeps its recovered intent.
    """
    if isinstance(upsert, UpsertCubeMaterialization):
        return upsert.model_copy(
            update={
                "schedule": declared.schedule,
                "strategy": declared.strategy,
                "lookback_window": declared.lookback_window,
            },
        )
    return upsert  # pragma: no cover


async def reconcile_declared_materialization(
    session: AsyncSession,
    revision: NodeRevision,
    declared: MaterializationSpec,
    *,
    access_checker: AccessChecker,
    current_user: User,
) -> tuple[Materialization, bool]:
    """
    Bring a cube revision's materialization in line with its declared block.

    Returns the materialization and whether anything actually changed, so a deploy
    that re-declares what is already configured can skip the query service entirely.

    Mirrors what `POST /nodes/{name}/materialization/` does -- build the config, then
    update the row of the same name in place rather than inserting a second one,
    since `(name, node_revision_id)` is unique -- with one deliberate difference. The
    endpoint decides "unchanged" on `config` alone, but `schedule` and `strategy` are
    columns rather than config keys, so by that test a schedule-only edit compares
    equal and is silently dropped. Rescheduling is the whole point of a declared
    block, so all three are compared here.
    """
    # Snapshotted before the build, which sets the new materialization's backref and
    # so appends it to this very collection -- searching afterwards would find the
    # build itself, call it unchanged, and then detach it.
    before = list(revision.materializations)
    built = await create_new_materialization(
        session,
        revision,
        UpsertCubeMaterialization(
            job=MaterializationJobTypeEnum.DRUID_CUBE.value.name,  # type: ignore
            strategy=declared.strategy,
            schedule=declared.schedule,
            lookback_window=declared.lookback_window,
        ),
        access_checker,
        current_user=current_user,
    )
    existing = next(
        (mat for mat in before if mat.name == built.name),
        None,
    )
    if existing is None:
        session.add(built)
        return built, True

    # `create_new_materialization` sets the backref, which would insert a duplicate
    # row alongside the one being updated.
    built.node_revision = None  # type: ignore
    unchanged = (
        existing.config == built.config
        and existing.schedule == built.schedule
        and existing.strategy == built.strategy
        and existing.deactivated_at is None
    )
    if unchanged:
        return existing, False
    existing.config = built.config
    existing.schedule = built.schedule
    existing.strategy = built.strategy
    existing.deactivated_at = None
    return existing, True


async def swap_cube_materializations(
    session: AsyncSession,
    old_revision: NodeRevision,
    new_revision: NodeRevision,
    *,
    access_checker: AccessChecker,
    current_user: User,
    previous_table_usable: bool,
    declared: MaterializationSpec | None = None,
) -> CubeMaterializationSwap | None:
    """
    Rebuild a cube's materializations against a new revision and retire the old ones.

    Materializations belong to a single `NodeRevision` and availability is scoped to
    the revision encoded in the materialized table name, so without this a new cube
    revision -- including a metadata-only one -- has neither: the cube silently falls
    back to live queries while the superseded revision's workflow keeps posting
    availability for a table built from the old definition. Every new revision
    therefore swaps, no matter how insignificant the change was.

    Rebuilt rather than copied: a stored config embeds the cube version plus combiner
    SQL and a Druid spec derived from the old definition, so the new revision goes
    back through `create_new_materialization`. The old materialization is the default
    source of the user's intent because it is often the only record of it -- a cube
    materialized through the UI has no YAML to read it from. A cube that does declare
    `materialization:` passes it as `declared`, which wins, so a push that edits both
    a metric and the schedule rebuilds with the new schedule rather than the old.

    `previous_table_usable` is the caller's answer to whether the superseded
    revision's materialized table is still valid (`is_non_trivial_cube_change`
    inverted), recorded on the history event so an operator can tell whether the
    rebuild can adopt the existing data or needs a fresh build and backfill.

    Touches only DJ-side state, and returns the query service work still owed --
    `None` when the cube had nothing materialized and there is no work at all. The
    caller commits and then hands the result to `apply_cube_materialization_swap`, so
    a query service that is slow or unreachable can neither hold the transaction open
    nor leave DJ's record of what is active disagreeing with what was requested.
    """
    superseded = [
        mat for mat in old_revision.materializations if not mat.deactivated_at
    ]
    if not superseded:
        return None

    # The rebuild reads the new revision's columns, partitions and cube elements.
    # Neither caller is guaranteed to have those loaded -- the API path has just
    # committed the revision -- and a lazy load from async code would fail, so pull
    # them in up front.
    await session.execute(
        select(NodeRevision)
        .where(NodeRevision.id == new_revision.id)
        .options(*NodeRevision.cube_load_options()),
    )

    rebuilt: list[Materialization] = []
    for materialization in superseded:
        try:
            upsert = _upsert_from_materialization(materialization)
            if declared:
                upsert = _apply_declared_spec(upsert, declared)
            new_materialization = await create_new_materialization(
                session,
                new_revision,
                upsert,
                access_checker,
                current_user=current_user,
            )
            # `create_new_materialization` only sets the backref, and `new_revision`
            # is already persisted here, so nothing cascades the new row in for us.
            session.add(new_materialization)
            rebuilt.append(new_materialization)
        except Exception as exc:
            # A rebuild can become impossible -- e.g. the new revision no longer has
            # the temporal partition a cube materialization needs. The superseded
            # workflow still has to be stopped, and the cube edit that triggered the
            # swap must not fail because its materialization could not be carried
            # forward, so nothing here is allowed to propagate.
            _logger.warning(
                "Cannot rebuild materialization %s for cube=%s version=%s: %s",
                materialization.name,
                new_revision.name,
                new_revision.version,
                str(exc),
            )
    for materialization in superseded:
        materialization.deactivated_at = UTCDatetime.now(UTC)  # type: ignore
    session.add(
        History(
            entity_type=EntityType.MATERIALIZATION,
            entity_name=old_revision.name,
            node=old_revision.name,
            activity_type=ActivityType.STATUS_CHANGE,
            details={
                "message": (
                    f"Cube updated to {new_revision.version}. Materializations were "
                    "rebuilt against the new version and the previous version's "
                    "workflows were stopped."
                ),
                "previous_version": old_revision.version,
                "new_version": new_revision.version,
                "stopped_materializations": [mat.name for mat in superseded],
                "rebuilt_materializations": [mat.name for mat in rebuilt],
                "previous_table_usable": previous_table_usable,
            },
            user=current_user.username,
        ),
    )
    await session.flush()
    return CubeMaterializationSwap(
        cube_name=old_revision.name,
        previous_version=old_revision.version,
        new_revision_id=new_revision.id,
        new_version=new_revision.version,
        rebuilt_names=[mat.name for mat in rebuilt],
        superseded=superseded,
    )


async def apply_cube_materialization_swap(
    session: AsyncSession,
    swap: CubeMaterializationSwap,
    query_service_client: QueryServiceClient | None,
    request_headers: dict[str, str] | None = None,
) -> None:
    """
    Hand a committed cube materialization swap to the query service.

    The replacement is scheduled before the superseded workflow is stopped, so the
    cube is never knowingly left with nothing running. The stop happens either way:
    DJ has already recorded the superseded materializations as inactive, and leaving
    their workflow running would keep posting availability for a table the current
    revision cannot use.

    Never raises. With no query service configured there is nothing to do at all.
    """
    if not query_service_client:
        return
    if swap.rebuilt_names:
        try:
            await schedule_materialization_jobs(
                session,
                node_revision_id=swap.new_revision_id,
                materialization_names=swap.rebuilt_names,
                query_service_client=query_service_client,
                request_headers=request_headers,
            )
        except Exception as exc:
            _logger.warning(
                "Failed to schedule rebuilt materializations for cube=%s version=%s: "
                "%s (continuing)",
                swap.cube_name,
                swap.new_version,
                str(exc),
            )
    stop_cube_materialization_workflows(
        query_service_client=query_service_client,
        cube_name=swap.cube_name,
        cube_version=swap.previous_version,
        materializations=swap.superseded,
        request_headers=request_headers,
    )


def stop_cube_materialization_workflows(
    query_service_client: QueryServiceClient,
    cube_name: str,
    cube_version: str,
    materializations: list[Materialization],
    request_headers: dict[str, str] | None = None,
) -> None:
    """
    Ask the query service to stop the workflows behind cube materializations.

    Workflow names recorded on the materialization configs are stopped together in
    one call. Materializations predating persisted workflow names fall back to the
    cube name + version endpoint, whose arguments are identical for every
    materialization on a revision, so it is called at most once.

    Never raises: a query service that is unreachable, or that has already forgotten
    the workflow, must not block the DJ-side operation that triggered the teardown.
    """
    workflow_names: list[str] = []
    needs_legacy_stop = False
    for materialization in materializations:
        config = (
            materialization.config if isinstance(materialization.config, dict) else {}
        )
        names = config.get("workflow_names", [])
        if names:
            workflow_names.extend(names)
        else:
            needs_legacy_stop = True
    try:
        if workflow_names:
            query_service_client.deactivate_workflows(
                workflow_names=workflow_names,
                request_headers=request_headers,
            )
            _logger.info(
                "Deactivated workflows for cube=%s: %s",
                cube_name,
                workflow_names,
            )
        if needs_legacy_stop:
            query_service_client.deactivate_cube_workflow(
                cube_name,
                version=cube_version,
                request_headers=request_headers,
            )
            _logger.info(
                "Deactivated workflow for cube=%s version=%s (legacy)",
                cube_name,
                cube_version,
            )
    except Exception as exc:
        _logger.warning(
            "Failed to deactivate workflows for cube=%s version=%s: %s (continuing)",
            cube_name,
            cube_version,
            str(exc),
        )


async def schedule_materialization_jobs(
    session: AsyncSession,
    node_revision_id: int,
    materialization_names: list[str],
    query_service_client: QueryServiceClient,
    request_headers: dict[str, str] | None = None,
) -> dict[str, MaterializationInfo]:
    """
    Schedule recurring materialization jobs
    """
    if not query_service_client:
        return {}  # No query service configured, skip scheduling

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


async def schedule_materialization_jobs_bg(
    node_revision_id: int,
    materialization_names: list[str],
    query_service_client: QueryServiceClient,
    request_headers: dict[str, str] | None = None,
) -> None:
    """
    Schedule a materialization job in the background.
    """
    try:
        async with session_context() as session:
            await schedule_materialization_jobs(
                session=session,
                node_revision_id=node_revision_id,
                materialization_names=materialization_names,
                query_service_client=query_service_client,
                request_headers=request_headers,
            )
    except Exception:
        _logger.exception(
            "Error scheduling materialization jobs for node revision %s",
            node_revision_id,
        )


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


def decompose_expression(
    expr: ast.Aliasable | ast.Expression,
) -> tuple[ast.Expression, list[ast.Alias]]:
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
                    ast.Name("NULLIF"),
                    args=[
                        ast.Function(
                            ast.Name("count"),
                            args=[ast.Column(name=denominator_measure_name)],
                        ),
                        ast.Number(value=0),
                    ],
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
            if expr.op == ast.BinaryOpKind.Divide and not (
                isinstance(measures_combiner_right, ast.Function)
                and measures_combiner_right.alias_or_name.name.lower() == "nullif"
            ):
                measures_combiner_right = ast.Function(
                    ast.Name("NULLIF"),
                    args=[measures_combiner_right, ast.Number(value=0)],
                )
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
