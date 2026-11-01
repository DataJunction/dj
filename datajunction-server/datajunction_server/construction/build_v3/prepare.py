"""Get a metrics query ready to build: check its filters, pick its engine."""

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.cube_matcher import (
    resolve_dialect_and_engine_for_metrics,
    validate_pinned_cube_covers_filters,
)
from datajunction_server.construction.build_v3.filter_requirements import (
    CheckedFilters,
    check_filter_requirements,
)
from datajunction_server.construction.build_v3.types import ResolvedExecutionContext
from datajunction_server.database.node import NodeRevision
from datajunction_server.models.dialect import Dialect


@dataclass
class PreparedMetricsQuery:
    """
    What build_metrics_sql needs, once the steps before the build have run.
    """

    filters: CheckedFilters
    # The dialect to build for; None leaves the choice to the build.
    dialect: Dialect | None
    # Set only when the engine was resolved.
    execution: ResolvedExecutionContext | None = None


async def prepare_metrics_query(
    session: AsyncSession,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str] | None,
    *,
    matched_cube: NodeRevision | None = None,
    use_materialized: bool = True,
    dialect: Dialect | None = None,
    engine_name: str | None = None,
    engine_version: str | None = None,
    resolve_execution: bool = False,
    enforce_filter_requirements: bool = True,
    inherit: bool = False,
) -> PreparedMetricsQuery:
    """
    The steps every metrics query takes before it is built:

    1. Check the filters against the metrics' filter requirements, unless
       ``enforce_filter_requirements`` is False. Distinct-value lookups skip it,
       because their metrics only scope the query to a cube.
    2. For an explicitly chosen cube that will back the build, check that it
       covers every filtered dimension.
    3. With ``resolve_execution``, pick the dialect and engine together, so
       engine selection sees the same filters as the build.

    Callers pass the final metrics, dimensions and filters, including any a
    cube adds. Pass the result's ``filters`` and ``dialect`` to build_metrics_sql.
    """
    checked = (
        await check_filter_requirements(session, metrics, filters, inherit)
        if enforce_filter_requirements
        else CheckedFilters(list(filters or []))
    )

    if (
        matched_cube is not None
        and metrics
        and use_materialized
        and dialect in (None, Dialect.DRUID)
    ):
        # A pinned cube bypasses find_matching_cube's coverage check. When that
        # cube will back the build, fail here instead of emitting Druid SQL
        # that references a column the cube table lacks.
        await validate_pinned_cube_covers_filters(
            session,
            matched_cube,
            dimensions,
            checked.filters,
            metrics,
        )

    if not resolve_execution:
        return PreparedMetricsQuery(checked, dialect)

    execution = await resolve_dialect_and_engine_for_metrics(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        use_materialized=use_materialized,
        engine_name=engine_name,
        engine_version=engine_version,
        dialect_override=dialect,
        matched_cube=matched_cube if metrics else None,
        filters=checked.filters or None,
    )
    return PreparedMetricsQuery(checked, execution.dialect, execution)
