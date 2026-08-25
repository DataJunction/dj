"""
Tests for node materialization helper functions.
"""

from datetime import date
from unittest import mock

import pytest
from sqlalchemy import select

from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.backfill import Backfill
from datajunction_server.database.column import Column
from datajunction_server.database.materialization import Materialization
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.partition import Partition
from datajunction_server.internal.materializations import (
    CoverageBackfill,
    CubeMaterializationSwap,
    CubeMaterializationSwapOutcome,
    NodeMaterializationTeardown,
    apply_cube_materialization_swap,
    backfill_recorded,
    coverage_backfill,
    coverage_gap,
    coverage_partition,
    record_backfill,
    stop_cube_materialization_workflows,
    stop_materialization_workflows,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import Granularity, PartitionType
from datajunction_server.models.preaggregation import CubeBackfillInput
from datajunction_server.sql.parsing.types import StringType


def test_stop_cube_workflows_prefers_stored_workflow_names():
    """
    Recorded workflow names identify the workflows exactly, so they win over the
    cube name + version fallback.
    """
    query_service_client = mock.MagicMock()
    materialization = Materialization(
        name="druid_cube_v3",
        config={"workflow_names": ["cube_workflow_1", "cube_workflow_2"]},
    )

    stop_cube_materialization_workflows(
        query_service_client=query_service_client,
        cube_name="default.a_cube",
        cube_version="v1.0",
        materializations=[materialization],
        request_headers={"cookie": "a-cookie"},
    )

    assert query_service_client.deactivate_workflows.call_args_list == [
        mock.call(
            workflow_names=["cube_workflow_1", "cube_workflow_2"],
            request_headers={"cookie": "a-cookie"},
        ),
    ]
    assert query_service_client.deactivate_cube_workflow.call_args_list == []


def test_stop_cube_workflows_swallows_workflow_names_failure():
    """
    A query service that cannot stop the named workflows must not break the caller.
    """
    query_service_client = mock.MagicMock()
    query_service_client.deactivate_workflows.side_effect = Exception("unreachable")
    materialization = Materialization(
        name="druid_cube_v3",
        config={"workflow_names": ["cube_workflow_1"]},
    )

    stop_cube_materialization_workflows(
        query_service_client=query_service_client,
        cube_name="default.a_cube",
        cube_version="v1.0",
        materializations=[materialization],
    )

    assert query_service_client.deactivate_workflows.call_args_list == [
        mock.call(workflow_names=["cube_workflow_1"], request_headers=None),
    ]
    assert query_service_client.deactivate_cube_workflow.call_args_list == []


def test_stop_cube_workflows_batches_across_materializations():
    """
    Several materializations are stopped with one call, not one call each: these are
    blocking HTTP requests, and the legacy fallback's arguments are identical for
    every materialization on a revision, so it is issued at most once.
    """
    query_service_client = mock.MagicMock()
    materializations = [
        Materialization(
            name="druid_cube_v3",
            config={"workflow_names": ["cube_workflow_1"]},
        ),
        Materialization(
            name="druid_cube_v3_backfill",
            config={"workflow_names": ["cube_workflow_2", "cube_workflow_3"]},
        ),
        Materialization(name="default", config={}),
    ]

    stop_cube_materialization_workflows(
        query_service_client=query_service_client,
        cube_name="default.a_cube",
        cube_version="v1.0",
        materializations=materializations,
    )

    assert query_service_client.deactivate_workflows.call_args_list == [
        mock.call(
            workflow_names=["cube_workflow_1", "cube_workflow_2", "cube_workflow_3"],
            request_headers=None,
        ),
    ]
    assert query_service_client.deactivate_cube_workflow.call_args_list == [
        mock.call("default.a_cube", version="v1.0", request_headers=None),
    ]


def test_stop_cube_workflows_without_a_config():
    """
    A materialization with no config at all falls back to cube name + version.
    """
    query_service_client = mock.MagicMock()
    materialization = Materialization(name="druid_cube_v3", config=None)

    stop_cube_materialization_workflows(
        query_service_client=query_service_client,
        cube_name="default.a_cube",
        cube_version="v2.0",
        materializations=[materialization],
    )

    assert query_service_client.deactivate_cube_workflow.call_args_list == [
        mock.call("default.a_cube", version="v2.0", request_headers=None),
    ]
    assert query_service_client.deactivate_workflows.call_args_list == []


def test_stop_materialization_workflows_without_a_query_service():
    """
    With no query service configured there is nothing to stop and nobody to tell.
    """
    assert (
        stop_materialization_workflows(
            None,
            [
                NodeMaterializationTeardown(
                    node_name="default.a_cube",
                    node_version="v1.0",
                    node_type=NodeType.CUBE,
                    materializations=[Materialization(name="druid_cube_v3", config={})],
                ),
            ],
        )
        == []
    )


def test_stop_materialization_workflows_for_a_non_cube_node():
    """
    A materialized transform's workflows are stopped by node and materialization
    name -- the same call node deactivation makes -- since only cubes record
    workflow names on their config.
    """
    query_service_client = mock.MagicMock()

    failures = stop_materialization_workflows(
        query_service_client,
        [
            NodeMaterializationTeardown(
                node_name="default.a_transform",
                node_version="v1.1",
                node_type=NodeType.TRANSFORM,
                materializations=[
                    Materialization(name="spark_sql__full__birth_date", config={}),
                    Materialization(name="spark_sql__full__hire_date", config={}),
                ],
            ),
        ],
        request_headers={"cookie": "a-cookie"},
    )

    assert failures == []
    assert query_service_client.deactivate_materialization.call_args_list == [
        mock.call(
            "default.a_transform",
            "spark_sql__full__birth_date",
            node_version="v1.1",
            request_headers={"cookie": "a-cookie"},
        ),
        mock.call(
            "default.a_transform",
            "spark_sql__full__hire_date",
            node_version="v1.1",
            request_headers={"cookie": "a-cookie"},
        ),
    ]
    assert query_service_client.deactivate_workflows.call_args_list == []
    assert query_service_client.deactivate_cube_workflow.call_args_list == []


def test_stop_materialization_workflows_reports_every_failure():
    """
    A query service that refuses is reported, not raised: the nodes are already
    deleted. Each node that failed gets its own message, and one failure does not
    stop the rest from being tried.
    """
    query_service_client = mock.MagicMock()
    query_service_client.deactivate_materialization.side_effect = Exception(
        "unreachable",
    )
    query_service_client.deactivate_cube_workflow.side_effect = Exception(
        "unreachable",
    )

    failures = stop_materialization_workflows(
        query_service_client,
        [
            NodeMaterializationTeardown(
                node_name="default.a_transform",
                node_version="v1.1",
                node_type=NodeType.TRANSFORM,
                materializations=[
                    Materialization(name="spark_sql__full__birth_date", config={}),
                ],
            ),
            NodeMaterializationTeardown(
                node_name="default.a_cube",
                node_version="v1.0",
                node_type=NodeType.CUBE,
                materializations=[Materialization(name="druid_cube_v3", config={})],
            ),
        ],
    )

    assert failures == [
        "Node `default.a_transform`: DJ deleted the materialization "
        "`spark_sql__full__birth_date` at version v1.1, but the query service did "
        "not stop its workflow: unreachable. The workflow may still be running.",
        "Cube `default.a_cube`: DJ retired the materialization `druid_cube_v3` at "
        "version v1.0, but the query service did not stop its workflow: unreachable. "
        "The workflow may still be running.",
    ]


def _swap(
    rebuilt_names: list[str],
    backfill: CoverageBackfill | None = None,
) -> CubeMaterializationSwap:
    """A swap with one superseded materialization and the given rebuilt names."""
    return CubeMaterializationSwap(
        cube_name="default.a_cube",
        previous_version="v1.0",
        new_revision_id=42,
        new_version="v1.1",
        rebuilt_names=rebuilt_names,
        superseded=[
            Materialization(
                name="druid_cube_v3",
                config={"workflow_names": ["cube_workflow_1"]},
            ),
        ],
        backfill=backfill,
    )


@pytest.mark.asyncio
async def test_apply_cube_swap_reports_a_scheduled_push():
    """
    A push the query service accepted reports the cube scheduled, with no error to
    carry back to the deployment.
    """
    query_service_client = mock.MagicMock()
    session = mock.MagicMock()

    with mock.patch(
        "datajunction_server.internal.materializations.schedule_materialization_jobs",
        new=mock.AsyncMock(),
    ) as schedule:
        outcome = await apply_cube_materialization_swap(
            session,
            _swap(["druid_cube_v3"]),
            query_service_client,
            request_headers={"cookie": "a-cookie"},
        )

    assert outcome == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=["druid_cube_v3"],
        scheduled=True,
        error=None,
    )
    assert schedule.call_args_list == [
        mock.call(
            session,
            node_revision_id=42,
            materialization_names=["druid_cube_v3"],
            query_service_client=query_service_client,
            request_headers={"cookie": "a-cookie"},
        ),
    ]
    assert query_service_client.deactivate_workflows.call_args_list == [
        mock.call(
            workflow_names=["cube_workflow_1"],
            request_headers={"cookie": "a-cookie"},
        ),
    ]


@pytest.mark.asyncio
async def test_apply_cube_swap_reports_a_rejected_push():
    """
    A query service that refuses the push still does not raise -- the deploy has
    committed -- but the refusal comes back verbatim, and the superseded workflow is
    stopped regardless: DJ has already recorded it as inactive.
    """
    query_service_client = mock.MagicMock()
    rejection = (
        "User `dj` is not an owner of data project `analytics` and cannot write to "
        "`prodhive.analytics.a_cube`"
    )

    with mock.patch(
        "datajunction_server.internal.materializations.schedule_materialization_jobs",
        new=mock.AsyncMock(side_effect=Exception(rejection)),
    ):
        outcome = await apply_cube_materialization_swap(
            mock.MagicMock(),
            _swap(["druid_cube_v3"]),
            query_service_client,
        )

    assert outcome == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=["druid_cube_v3"],
        scheduled=False,
        error=rejection,
    )
    assert query_service_client.deactivate_workflows.call_args_list == [
        mock.call(workflow_names=["cube_workflow_1"], request_headers=None),
    ]


@pytest.mark.asyncio
async def test_apply_cube_swap_of_a_teardown_scheduled_nothing_and_failed_nothing():
    """
    A swap that rebuilt nothing is a teardown. There is no scheduling to fail, so it
    reports success with no names -- a workflow the query service could not stop is
    `stop_cube_materialization_workflows`'s to report, not a materialization failure
    to hang on the cube.
    """
    query_service_client = mock.MagicMock()
    query_service_client.deactivate_workflows.side_effect = Exception("unreachable")

    outcome = await apply_cube_materialization_swap(
        mock.MagicMock(),
        _swap([]),
        query_service_client,
    )

    assert outcome == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=[],
        scheduled=True,
        error=None,
    )


@pytest.mark.asyncio
async def test_apply_cube_swap_without_a_query_service():
    """
    With no query service configured there is nothing to push and nothing to report.
    """
    assert await apply_cube_materialization_swap(
        mock.MagicMock(),
        _swap(["druid_cube_v3"]),
        None,
    ) == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=["druid_cube_v3"],
        scheduled=True,
        error=None,
    )


async def _apply_with_backfill(
    session,
    query_service_client,
    backfill: CoverageBackfill,
    rebuilt_names: list[str] | None = None,
) -> CubeMaterializationSwapOutcome:
    """Apply a swap that carries a span to backfill."""
    with mock.patch(
        "datajunction_server.internal.materializations.schedule_materialization_jobs",
        new=mock.AsyncMock(),
    ):
        return await apply_cube_materialization_swap(
            session,
            _swap(
                rebuilt_names if rebuilt_names is not None else ["druid_cube_v3"],
                backfill=backfill,
            ),
            query_service_client,
        )


@pytest.mark.asyncio
async def test_apply_cube_swap_backfills_the_span(session, current_user):
    """
    A swap carrying a span asks for exactly those days, against the new version --
    which is what names the datasource being filled -- and writes the span down
    with the job the query service handed back.
    """
    materialization = await _stored_materialization(session, current_user)
    query_service_client = mock.MagicMock()
    query_service_client.run_cube_backfill.return_value = {"job_url": "http://job1"}

    outcome = await _apply_with_backfill(
        session,
        query_service_client,
        coverage_backfill(
            materialization,
            _cube_revision().columns[0],
            (date(2024, 1, 1), date(2024, 6, 30)),
        ),
    )

    assert outcome == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=["druid_cube_v3"],
        scheduled=True,
        error=None,
        backfill_span=(date(2024, 1, 1), date(2024, 6, 30)),
        backfill_error=None,
        backfill_record_error=None,
    )
    assert query_service_client.run_cube_backfill.call_args_list == [
        mock.call(
            CubeBackfillInput(
                cube_name="default.a_cube",
                cube_version="v1.1",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 6, 30),
            ),
            request_headers=None,
        ),
    ]
    assert await _recorded(session, materialization) == [
        (
            [
                {
                    "column_name": "date_0",
                    "values": None,
                    "range": ["2024-01-01", "2024-06-30"],
                },
            ],
            ["http://job1"],
        ),
    ]


@pytest.mark.asyncio
async def test_apply_cube_swap_backfills_without_rebuilding(session, current_user):
    """
    A gap fill rides on the workflow the cube already has, so its swap rebuilds
    nothing and schedules nothing -- it only backfills. A query service that
    names no job still gets the span written down.
    """
    materialization = await _stored_materialization(session, current_user)
    query_service_client = mock.MagicMock()
    query_service_client.run_cube_backfill.return_value = {}

    outcome = await _apply_with_backfill(
        session,
        query_service_client,
        coverage_backfill(
            materialization,
            _cube_revision().columns[0],
            (date(2024, 1, 1), date(2024, 1, 3)),
        ),
        rebuilt_names=[],
    )

    assert outcome == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=[],
        scheduled=True,
        error=None,
        backfill_span=(date(2024, 1, 1), date(2024, 1, 3)),
        backfill_error=None,
        backfill_record_error=None,
    )
    assert await _recorded(session, materialization) == [
        (
            [
                {
                    "column_name": "date_0",
                    "values": None,
                    "range": ["2024-01-01", "2024-01-03"],
                },
            ],
            [],
        ),
    ]


@pytest.mark.asyncio
async def test_apply_cube_swap_reports_a_refused_backfill(session, current_user):
    """
    A backfill the query service refuses does not raise -- the materialization is
    scheduled and the deploy has committed -- but the refusal comes back verbatim
    and nothing is written down, so the next deploy asks for the same days.
    """
    materialization = await _stored_materialization(session, current_user)
    query_service_client = mock.MagicMock()
    query_service_client.run_cube_backfill.side_effect = Exception("429 Too Many")

    outcome = await _apply_with_backfill(
        session,
        query_service_client,
        coverage_backfill(
            materialization,
            _cube_revision().columns[0],
            (date(2024, 1, 1), date(2024, 1, 2)),
        ),
    )

    assert outcome == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=["druid_cube_v3"],
        scheduled=True,
        error=None,
        backfill_span=None,
        backfill_error="429 Too Many",
        backfill_record_error=None,
    )
    assert await _recorded(session, materialization) == []
    assert await backfill_recorded(session, materialization, date(2024, 1, 1)) is False


@pytest.mark.asyncio
async def test_apply_cube_swap_survives_a_record_failure(session, current_user):
    """
    The backfill is already running by the time DJ writes it down, so a write that
    fails is reported rather than raised at a deploy that has committed.
    """
    materialization = await _stored_materialization(session, current_user)
    query_service_client = mock.MagicMock()
    query_service_client.run_cube_backfill.return_value = {"job_url": "http://job1"}

    with mock.patch(
        "datajunction_server.internal.materializations.record_backfill",
        new=mock.AsyncMock(side_effect=Exception("connection reset")),
    ):
        outcome = await _apply_with_backfill(
            session,
            query_service_client,
            coverage_backfill(
                materialization,
                _cube_revision().columns[0],
                (date(2024, 1, 1), date(2024, 1, 2)),
            ),
        )

    assert outcome == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=["druid_cube_v3"],
        scheduled=True,
        error=None,
        backfill_span=(date(2024, 1, 1), date(2024, 1, 2)),
        backfill_error=None,
        backfill_record_error="connection reset",
    )
    assert await _recorded(session, materialization) == []


@pytest.mark.asyncio
async def test_apply_cube_swap_backfills_nothing_unscheduled(session, current_user):
    """
    A backfill needs the workflow the scheduling would have created, so a rejected
    push takes the backfill with it.
    """
    materialization = await _stored_materialization(session, current_user)
    query_service_client = mock.MagicMock()

    with mock.patch(
        "datajunction_server.internal.materializations.schedule_materialization_jobs",
        new=mock.AsyncMock(side_effect=Exception("403 Forbidden")),
    ):
        outcome = await apply_cube_materialization_swap(
            session,
            _swap(
                ["druid_cube_v3"],
                backfill=coverage_backfill(
                    materialization,
                    _cube_revision().columns[0],
                    (date(2024, 1, 1), date(2024, 1, 2)),
                ),
            ),
            query_service_client,
        )

    assert outcome == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=["druid_cube_v3"],
        scheduled=False,
        error="403 Forbidden",
        backfill_span=None,
        backfill_error=None,
        backfill_record_error=None,
    )
    assert query_service_client.run_cube_backfill.call_args_list == []
    assert await _recorded(session, materialization) == []


def _cube_revision(
    min_temporal_partition: list[str] | None = None,
    partitions: int = 1,
) -> NodeRevision:
    """A day-partitioned cube revision, optionally with availability."""
    columns = [
        Column(
            name=f"date_{index}",
            display_name=f"date_{index}",
            type=StringType(),
            partition=Partition(
                type_=PartitionType.TEMPORAL,
                granularity=Granularity.DAY,
                format="yyyyMMdd",
            ),
        )
        for index in range(partitions)
    ]
    return NodeRevision(
        name="default.a_cube",
        type=NodeType.CUBE,
        columns=columns,
        availability=(
            AvailabilityState(
                catalog="default",
                table="a_cube",
                valid_through_ts=0,
                min_temporal_partition=min_temporal_partition,
            )
            if min_temporal_partition is not None
            else None
        ),
    )


def test_coverage_partition_needs_one_axis():
    """
    Coverage is a span of days on a single axis, so a cube partitioned on two
    temporal columns, or none, has no axis to run one over.
    """
    assert coverage_partition(_cube_revision(partitions=0)) is None
    assert coverage_partition(_cube_revision(partitions=2)) is None
    assert coverage_partition(_cube_revision()).name == "date_0"


def test_coverage_gap_without_availability():
    """A cube that has posted no availability holds nothing at all."""
    span = (date(2024, 1, 1), date(2024, 6, 30))

    assert coverage_gap(span, _cube_revision()) == span


@pytest.mark.parametrize("covered", ["20250301", 20250301])
def test_coverage_gap_before_the_covered_start(covered):
    """
    The gap runs from the declared start to the day before the cube's earliest,
    read out of availability in the partition's own format, written either way.
    """
    assert coverage_gap(
        (date(2024, 1, 1), date(2026, 8, 1)),
        _cube_revision(min_temporal_partition=[covered]),
    ) == (date(2024, 1, 1), date(2025, 2, 28))


def test_coverage_gap_stops_at_the_declared_end():
    """
    A span that ends before the cube's earliest day is missing in full, rather
    than reported as reaching up to a day the cube never declared.
    """
    assert coverage_gap(
        (date(2024, 1, 1), date(2024, 6, 30)),
        _cube_revision(min_temporal_partition=["20250301"]),
    ) == (date(2024, 1, 1), date(2024, 6, 30))


@pytest.mark.parametrize(
    "revision",
    [
        # The cube already reaches back further than it declares.
        _cube_revision(min_temporal_partition=["20230101"]),
        # It reaches back to exactly the declared start.
        _cube_revision(min_temporal_partition=["20240101"]),
        # Its availability does not read against the partition format.
        _cube_revision(min_temporal_partition=["2024-01-01"]),
        # It reports a bound per axis, and there is more than one axis.
        _cube_revision(min_temporal_partition=["20240101", "01"], partitions=2),
    ],
)
def test_coverage_gap_leaves_nothing_to_fill(revision):
    """Nothing knowably missing, and nothing guessed at."""
    assert coverage_gap((date(2024, 1, 1), date(2026, 8, 1)), revision) is None


async def _stored_materialization(session, current_user) -> Materialization:
    """A stored cube materialization to hang backfill records on."""
    revision = _cube_revision()
    revision.version = "v1.0"
    revision.created_by_id = current_user.id
    revision.node = Node(
        name=revision.name,
        type=NodeType.CUBE,
        current_version="v1.0",
        created_by_id=current_user.id,
    )
    materialization = Materialization(
        node_revision=revision,
        name="druid_cube_v3",
        schedule="@daily",
        config={},
    )
    session.add(materialization)
    await session.commit()
    return materialization


async def _recorded(session, materialization: Materialization) -> list[tuple]:
    """The backfill specs and urls stored against a materialization."""
    return [
        tuple(row)
        for row in (
            await session.execute(
                select(Backfill.spec, Backfill.urls).where(
                    Backfill.materialization_id == materialization.id,
                ),
            )
        ).all()
    ]


@pytest.mark.asyncio
async def test_record_backfill_is_read_back(session, current_user):
    """
    A recorded span stops the next deploy asking for the same days again, and any
    span it already reaches back over.
    """
    materialization = await _stored_materialization(session, current_user)

    await record_backfill(
        session,
        coverage_backfill(
            materialization,
            _cube_revision().columns[0],
            (date(2024, 1, 1), date(2024, 6, 30)),
        ),
    )

    assert await _recorded(session, materialization) == [
        (
            [
                {
                    "column_name": "date_0",
                    "values": None,
                    "range": ["2024-01-01", "2024-06-30"],
                },
            ],
            [],
        ),
    ]
    assert await backfill_recorded(session, materialization, date(2024, 1, 1)) is True
    assert await backfill_recorded(session, materialization, date(2024, 6, 1)) is True
    assert (
        await backfill_recorded(session, materialization, date(2023, 12, 31)) is False
    )


@pytest.mark.parametrize(
    "spec",
    [
        # A backfill run from the API, in partition format rather than ISO.
        [{"column_name": "date_0", "range": [20240101, 20240630]}],
        # One naming values rather than a range.
        [{"column_name": "date_0", "values": [20240101], "range": None}],
        # Nothing at all.
        [],
    ],
)
@pytest.mark.asyncio
async def test_backfill_recorded_reads_djs_own(session, current_user, spec):
    """
    Only DJ's own records count. A backfill posted through the API says nothing
    about the span a cube declares.
    """
    materialization = await _stored_materialization(session, current_user)
    session.add(Backfill(materialization_id=materialization.id, spec=spec))

    assert await backfill_recorded(session, materialization, date(2024, 1, 1)) is False
