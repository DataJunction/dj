"""Materialization plan fingerprints and produced-data receipts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datajunction_server.api.data import add_availability_state
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.node import NodeRevision
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.materializations import (
    capture_materialization_fingerprint,
    materialization_reuse_decision,
)
from datajunction_server.materialization.jobs.cube_materialization import (
    DruidCubeMaterializationJob,
)
from datajunction_server.models.cube_materialization import DruidCubeConfig
from datajunction_server.models.node import AvailabilityStateBase
from datajunction_server.models.node_type import NodeNameVersion, NodeType
from datajunction_server.models.semantic_fingerprint import SemanticFingerprint
from datajunction_server.semantic_fingerprints.materialization import (
    MaterializationContract,
    MaterializationFingerprint,
)


def _manifest(digest="a"):
    return MaterializationFingerprint(
        SemanticFingerprint(version=2, digest=digest * 64),
        MaterializationContract(version=1, planner_epoch=1, digest="b" * 64),
    ).to_dict()


@pytest.mark.asyncio
async def test_capture_and_schedule_pass_the_same_planned_manifest():
    revision = SimpleNamespace(
        name="ns.cube",
        cube_node_metrics=["ns.metric"],
        cube_node_dimensions=["ns.date.day"],
        cube_filters=["ns.date.day > 0"],
        columns=[],
        node=SimpleNamespace(owners=[]),
        custom_metadata={"description": "ignored"},
    )
    config = DruidCubeConfig(
        cube=NodeNameVersion(name=revision.name, version="v1.0"),
        planned_node_versions={"ns.cube": "v1.0", "ns.metric": "v2.0"},
        metrics=[],
        dimensions=revision.cube_node_dimensions,
        measures_materializations=[],
        combiners=[],
    )
    manifest = _manifest()
    session = AsyncMock()
    query_result = MagicMock()
    query_result.all.return_value = list(config.planned_node_versions.items())
    session.execute.return_value = query_result
    with (
        patch(
            "datajunction_server.internal.deployment.fingerprints.build_deployment_fingerprints",
            new_callable=AsyncMock,
            return_value=(
                {},
                {revision.name: SemanticFingerprint(**manifest["semantic"])},
            ),
        ) as build,
        patch(
            "datajunction_server.internal.materializations.materialization_contract",
            return_value=MaterializationContract.from_value(manifest["storage"]),
        ),
    ):
        config.fingerprint_v2 = await capture_materialization_fingerprint(
            session,
            revision,
            config,
        )
    assert build.call_args.kwargs == {"version": 2}
    cube = build.call_args.args[2][0]
    assert cube.metrics == revision.cube_node_metrics
    assert cube.filters == revision.cube_filters
    assert config.fingerprint_v2 == manifest
    assert "planned_node_versions" not in config.model_dump()
    materialization = SimpleNamespace(
        config=config.model_dump(),
        node_revision=revision,
        name="cube",
        strategy="full",
        schedule="@daily",
        job="DruidCubeMaterializationJob",
    )
    client = MagicMock()
    DruidCubeMaterializationJob().schedule(materialization, client)
    assert (
        client.materialize_cube.call_args.kwargs["materialization_input"].fingerprint_v2
        == manifest
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "planned_versions,current_versions",
    [
        ({}, []),
        ({"ns.cube": "v1.0"}, [("ns.cube", "v2.0")]),
    ],
)
async def test_capture_returns_unknown_without_a_matching_version_snapshot(
    planned_versions,
    current_versions,
):
    revision = SimpleNamespace(
        name="ns.cube",
        cube_node_metrics=["ns.metric"],
        cube_node_dimensions=[],
        cube_filters=[],
        columns=[],
    )
    config = DruidCubeConfig(
        cube=NodeNameVersion(name=revision.name, version="v1.0"),
        planned_node_versions=planned_versions,
        dimensions=[],
        metrics=[],
        measures_materializations=[],
        combiners=[],
    )
    session = AsyncMock()
    query_result = MagicMock()
    query_result.all.return_value = current_versions
    session.execute.return_value = query_result
    semantic = SemanticFingerprint(version=2, digest="a" * 64)
    contract = MaterializationContract(version=1, planner_epoch=1, digest="b" * 64)
    with (
        patch(
            "datajunction_server.internal.deployment.fingerprints.build_deployment_fingerprints",
            new_callable=AsyncMock,
            return_value=({}, {revision.name: semantic}),
        ),
        patch(
            "datajunction_server.internal.materializations.materialization_contract",
            return_value=contract,
        ),
    ):
        manifest = await capture_materialization_fingerprint(
            session,
            revision,
            config,
        )
    assert manifest is None


def test_reuse_reads_produced_receipt_and_requires_explicit_operational_gates():
    expected = {"fingerprint_v2": _manifest()}
    availability = AvailabilityState(links=expected)
    assert materialization_reuse_decision(
        availability,
        expected,
        fresh=True,
        execution_compatible=True,
    ).reusable
    for receipt in (
        None,
        {},
        {"fingerprint_v2": "unknown"},
        {"fingerprint_v2": _manifest("c")},
    ):
        availability.links = receipt
        assert not materialization_reuse_decision(
            availability,
            expected,
            fresh=True,
            execution_compatible=True,
        ).reusable
    assert not materialization_reuse_decision(
        None,
        expected,
        fresh=True,
        execution_compatible=True,
    ).reusable


@pytest.mark.asyncio
async def test_availability_receipt_is_bound_to_plan_and_does_not_inherit_other_generations():
    manifest = _manifest()
    revision = NodeRevision(id=1, name="ns.cube", version="v1.0", type=NodeType.CUBE)
    revision.availability = AvailabilityState(
        catalog="druid",
        table="cube",
        valid_through_ts=100,
        min_temporal_partition=["old"],
        max_temporal_partition=["old"],
        links={"fingerprint_v2": _manifest("c")},
    )
    session = AsyncMock()
    session.add = MagicMock()
    session.scalars.return_value = [
        {"fingerprint_v2": manifest, "version": "v3", "druid_datasource": "cube"},
    ]
    access = SimpleNamespace(add_request=MagicMock(), check=AsyncMock())
    data = AvailabilityStateBase(
        catalog="druid",
        table="cube",
        valid_through_ts=1,
        node_version="v1.0",
        min_temporal_partition=["new"],
        max_temporal_partition=["new"],
        links={"fingerprint_v2": manifest},
    )
    with patch(
        "datajunction_server.api.data.Node.get_by_name",
        AsyncMock(return_value=SimpleNamespace(current=revision, name=revision.name)),
    ):
        await add_availability_state(
            revision.name,
            data,
            session=session,
            current_user=SimpleNamespace(username="dj"),
            access_checker=access,
            save_history=AsyncMock(),
        )
        assert revision.availability.valid_through_ts == 1
        assert revision.availability.min_temporal_partition == ["new"]
        data.links = {"fingerprint_v2": _manifest("c")}
        with pytest.raises(DJInvalidInputException, match="planned materialization"):
            await add_availability_state(
                revision.name,
                data,
                session=session,
                current_user=SimpleNamespace(username="dj"),
                access_checker=access,
                save_history=AsyncMock(),
            )
        data.links = {"fingerprint_v2": manifest}
        data.table = "another_table"
        with pytest.raises(DJInvalidInputException, match="planned materialization"):
            await add_availability_state(
                revision.name,
                data,
                session=session,
                current_user=SimpleNamespace(username="dj"),
                access_checker=access,
                save_history=AsyncMock(),
            )
        data.table = "cube"
        data.node_version = None
        with pytest.raises(DJInvalidInputException, match="explicit node_version"):
            await add_availability_state(
                revision.name,
                data,
                session=session,
                current_user=SimpleNamespace(username="dj"),
                access_checker=access,
                save_history=AsyncMock(),
            )
