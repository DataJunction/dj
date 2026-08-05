"""
Tests for node materialization helper functions.
"""

from unittest import mock

from datajunction_server.database.materialization import Materialization
from datajunction_server.internal.materializations import (
    stop_cube_materialization_workflows,
)


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
