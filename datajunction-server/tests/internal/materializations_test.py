"""
Tests for node materialization helper functions.
"""

from datetime import date, timedelta
from unittest import mock

import pytest

from datajunction_server.database.materialization import Materialization
from datajunction_server.internal.materializations import (
    CubeMaterializationSwap,
    CubeMaterializationSwapOutcome,
    NodeMaterializationTeardown,
    apply_cube_materialization_swap,
    stop_cube_materialization_workflows,
    stop_materialization_workflows,
)
from datajunction_server.models.materialization import CoverageSpec
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.preaggregation import CubeBackfillInput


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
    backfill: CoverageSpec | None = None,
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
    query_service_client,
    coverage: CoverageSpec,
) -> CubeMaterializationSwapOutcome:
    """Apply a swap that mints a new datasource over the given coverage."""
    with mock.patch(
        "datajunction_server.internal.materializations.schedule_materialization_jobs",
        new=mock.AsyncMock(),
    ):
        return await apply_cube_materialization_swap(
            mock.MagicMock(),
            _swap(["druid_cube_v3"], backfill=coverage),
            query_service_client,
        )


@pytest.mark.asyncio
async def test_apply_cube_swap_backfills_a_fixed_span():
    """
    A declared span with both ends is backfilled exactly as written, against the
    new version -- which is what names the empty datasource.
    """
    query_service_client = mock.MagicMock()

    outcome = await _apply_with_backfill(
        query_service_client,
        CoverageSpec(from_=date(2024, 1, 1), to=date(2024, 6, 30)),
    )

    assert outcome == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=["druid_cube_v3"],
        scheduled=True,
        error=None,
        backfill_span=(date(2024, 1, 1), date(2024, 6, 30)),
        backfill_error=None,
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


@pytest.mark.asyncio
async def test_apply_cube_swap_backfills_an_ongoing_span():
    """A span with no `to` runs up to yesterday, the last full day."""
    yesterday = date.today() - timedelta(days=1)

    outcome = await _apply_with_backfill(
        mock.MagicMock(),
        CoverageSpec(from_=date(2024, 1, 1)),
    )

    assert outcome.backfill_span == (date(2024, 1, 1), yesterday)


@pytest.mark.asyncio
async def test_apply_cube_swap_backfills_a_rolling_window():
    """A rolling window is counted back from yesterday, inclusive of both ends."""
    yesterday = date.today() - timedelta(days=1)

    outcome = await _apply_with_backfill(
        mock.MagicMock(),
        CoverageSpec(window="800 DAYS"),
    )

    assert outcome.backfill_span == (yesterday - timedelta(days=799), yesterday)


@pytest.mark.asyncio
@pytest.mark.parametrize("window", ["6 MONTHS", "a while"])
async def test_apply_cube_swap_cannot_count_the_window(window):
    """
    A window DJ cannot turn into days -- an unknown unit, or a duration that reads
    as nothing at all -- launches nothing, and says so rather than leaving the
    datasource quietly empty.
    """
    query_service_client = mock.MagicMock()

    outcome = await _apply_with_backfill(
        query_service_client,
        CoverageSpec(window=window),
    )

    assert outcome == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=["druid_cube_v3"],
        scheduled=True,
        error=None,
        backfill_span=None,
        backfill_error="DJ cannot count this coverage window in days.",
    )
    assert query_service_client.run_cube_backfill.call_args_list == []


@pytest.mark.asyncio
async def test_apply_cube_swap_reports_a_refused_backfill():
    """
    A backfill the query service refuses does not raise -- the materialization is
    scheduled and the deploy has committed -- but the refusal comes back verbatim.
    """
    query_service_client = mock.MagicMock()
    query_service_client.run_cube_backfill.side_effect = Exception("429 Too Many")

    outcome = await _apply_with_backfill(
        query_service_client,
        CoverageSpec(from_=date(2024, 1, 1), to=date(2024, 1, 2)),
    )

    assert outcome == CubeMaterializationSwapOutcome(
        cube_name="default.a_cube",
        materialization_names=["druid_cube_v3"],
        scheduled=True,
        error=None,
        backfill_span=None,
        backfill_error="429 Too Many",
    )


@pytest.mark.asyncio
async def test_apply_cube_swap_backfills_nothing_unscheduled():
    """
    A backfill needs the workflow the scheduling would have created, so a rejected
    push takes the backfill with it.
    """
    query_service_client = mock.MagicMock()

    with mock.patch(
        "datajunction_server.internal.materializations.schedule_materialization_jobs",
        new=mock.AsyncMock(side_effect=Exception("403 Forbidden")),
    ):
        outcome = await apply_cube_materialization_swap(
            mock.MagicMock(),
            _swap(
                ["druid_cube_v3"],
                backfill=CoverageSpec(from_=date(2024, 1, 1)),
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
    )
    assert query_service_client.run_cube_backfill.call_args_list == []
