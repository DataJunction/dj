from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datajunction_server.internal.deployment.fingerprints import (
    DeploymentFingerprintBuildResult,
)
from datajunction_server.internal.deployment.orchestrator import (
    DeploymentOrchestrator,
)
from datajunction_server.models.deployment import DeploymentInfo, DeploymentSpec
from datajunction_server.models.impact import DownstreamImpact, ImpactType
from datajunction_server.models.node import NodeStatus, NodeType
from datajunction_server.models.semantic_fingerprint import (
    SemanticFingerprintComparison,
)


def test_deployment_spec_defaults_to_v1_and_allows_v2():
    assert DeploymentSpec(namespace="ns").semantic_fingerprint_version == 1
    assert (
        DeploymentSpec(
            namespace="ns",
            semantic_fingerprint_version=2,
        ).semantic_fingerprint_version
        == 2
    )


def test_downstream_impact_only_serializes_computed_comparison():
    impact = DownstreamImpact(
        name="ns.transform",
        node_type=NodeType.TRANSFORM,
        current_status=NodeStatus.VALID,
        predicted_status=NodeStatus.VALID,
        impact_type=ImpactType.MAY_AFFECT,
        impact_reason="changed upstream",
        depth=1,
    )

    assert "semantic_comparison" not in impact.model_dump()
    impact.semantic_comparison = SemanticFingerprintComparison(
        status="indeterminate",
        precision="unknown",
        reasons=["unsupported_query_shape"],
    )
    assert impact.model_dump()["semantic_comparison"] == {
        "status": "indeterminate",
        "precision": "unknown",
        "reasons": ["unsupported_query_shape"],
    }


def test_deployment_info_deduplicates_only_confirmed_changed_owners():
    changed = DownstreamImpact(
        name="ns.changed",
        node_type=NodeType.TRANSFORM,
        current_status=NodeStatus.VALID,
        predicted_status=NodeStatus.VALID,
        impact_type=ImpactType.MAY_AFFECT,
        impact_reason="changed upstream",
        depth=1,
        owners=["owner-b", "owner-a"],
        semantic_comparison=SemanticFingerprintComparison(
            status="changed",
            precision="precise",
        ),
    )
    indeterminate = DownstreamImpact(
        name="ns.unknown",
        node_type=NodeType.TRANSFORM,
        current_status=NodeStatus.VALID,
        predicted_status=NodeStatus.VALID,
        impact_type=ImpactType.MAY_AFFECT,
        impact_reason="changed upstream",
        depth=1,
        owners=["owner-a", "owner-c"],
        semantic_comparison=SemanticFingerprintComparison(
            status="indeterminate",
            precision="unknown",
            reasons=["unsupported_query_shape"],
        ),
    )

    info = DeploymentInfo(
        uuid="deployment-id",
        namespace="ns",
        status="success",
        downstream_impacts=[changed, indeterminate],
    )

    assert info.semantic_review_recipients == ["owner-a", "owner-b"]
    assert info.model_dump()["semantic_review_recipients"] == [
        "owner-a",
        "owner-b",
    ]


@pytest.mark.asyncio
async def test_orchestrator_passes_version_and_inferred_columns_to_builder():
    source = "ns.source"
    inferred_columns = {source: []}
    comparison = SemanticFingerprintComparison(
        status="unchanged",
        precision="precise",
    )
    build_result = DeploymentFingerprintBuildResult(
        current={},
        proposed={},
        comparisons={"ns.transform": comparison},
    )
    orchestrator = object.__new__(DeploymentOrchestrator)
    orchestrator.session = MagicMock()
    orchestrator.deployment_spec = DeploymentSpec(
        namespace="ns",
        semantic_fingerprint_version=2,
    )
    orchestrator._proposed_inferred_columns = inferred_columns
    orchestrator._current_semantic_fingerprints = {}
    orchestrator._proposed_semantic_fingerprints = {}
    orchestrator._apply_semantic_fingerprints = MagicMock()

    plan = MagicMock()
    plan.existing_specs = {}
    plan.deletable_specs = []
    plan.to_deploy = []
    plan.to_delete = []
    impact = DownstreamImpact(
        name="ns.transform",
        node_type=NodeType.TRANSFORM,
        current_status=NodeStatus.VALID,
        predicted_status=NodeStatus.VALID,
        impact_type=ImpactType.MAY_AFFECT,
        impact_reason="changed upstream",
        depth=1,
    )

    with patch(
        "datajunction_server.internal.deployment.orchestrator.build_deployment_fingerprints",
        new=AsyncMock(return_value=build_result),
    ) as build:
        await orchestrator._build_and_apply_semantic_fingerprints(plan, [impact])

    assert build.await_args.kwargs["version"] == 2
    assert build.await_args.kwargs["proposed_inferred_columns"] == inferred_columns
    assert build.await_args.kwargs["comparison_target_names"] == {
        "ns.transform",
    }
    assert impact.semantic_comparison == comparison


@pytest.mark.asyncio
async def test_orchestrator_preserves_v1_default_response():
    orchestrator = object.__new__(DeploymentOrchestrator)
    orchestrator.session = MagicMock()
    orchestrator.deployment_spec = DeploymentSpec(namespace="ns")
    orchestrator._proposed_inferred_columns = {}
    orchestrator._current_semantic_fingerprints = {}
    orchestrator._proposed_semantic_fingerprints = {}
    orchestrator._apply_semantic_fingerprints = MagicMock()
    plan = MagicMock()
    plan.existing_specs = {}
    plan.deletable_specs = []
    plan.to_deploy = []
    plan.to_delete = []
    impact = DownstreamImpact(
        name="ns.transform",
        node_type=NodeType.TRANSFORM,
        current_status=NodeStatus.VALID,
        predicted_status=NodeStatus.VALID,
        impact_type=ImpactType.MAY_AFFECT,
        impact_reason="changed upstream",
        depth=1,
    )

    with patch(
        "datajunction_server.internal.deployment.orchestrator.build_deployment_fingerprints",
        new=AsyncMock(
            return_value=DeploymentFingerprintBuildResult(current={}, proposed={}),
        ),
    ) as build:
        await orchestrator._build_and_apply_semantic_fingerprints(plan, [impact])

    assert build.await_args.kwargs["version"] == 1
    assert build.await_args.kwargs["comparison_target_names"] == ()
    assert impact.semantic_comparison is None
