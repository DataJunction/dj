import asyncio

import pytest

from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)


async def _wait_for_deployment(client, deployment_id: str, timeout: int = 30):
    for _ in range(timeout):
        response = await client.get(f"/deployments/{deployment_id}")
        deployment = response.json()
        if deployment["status"] in ("success", "failed"):
            return deployment
        await asyncio.sleep(0.1)
    return (await client.get(f"/deployments/{deployment_id}")).json()


async def _deploy(client, spec: DeploymentSpec):
    response = await client.post(
        "/deployments",
        json=spec.model_dump(by_alias=True),
    )
    assert response.status_code == 200
    deployment = await _wait_for_deployment(client, response.json()["uuid"])
    assert deployment["status"] == "success", deployment
    return deployment


def _project(namespace: str, transform_query: str, description: str) -> DeploymentSpec:
    return DeploymentSpec(
        namespace=namespace,
        semantic_fingerprint_version=2,
        nodes=[
            SourceSpec(
                name="typed_source",
                catalog="default",
                schema_="test",
                table="typed_source",
                columns=[
                    ColumnSpec(name="id", type="int"),
                    ColumnSpec(name="value", type="int"),
                ],
            ),
            TransformSpec(
                name="prepared",
                query=transform_query,
                description=description,
            ),
            *[
                MetricSpec(
                    name=f"value_sum_{index:02d}",
                    query="SELECT SUM(value) FROM ${prefix}prepared",
                    owners=["dj"],
                )
                for index in range(12)
            ],
        ],
    )


class TestDeploymentV2FingerprintImpact:
    @pytest.mark.asyncio
    async def test_v2_uses_validated_output_types_and_deduplicates_changed_owners(
        self,
        client_with_roads,
    ):
        namespace = "fingerprint_v2_impact_test"
        original_query = "SELECT id, value FROM ${prefix}typed_source"
        initial = _project(namespace, original_query, "Initial description")
        await _deploy(client_with_roads, initial)
        assert initial.nodes[1].columns is None

        metadata_only = _project(namespace, original_query, "Updated description")
        metadata_result = await _deploy(client_with_roads, metadata_only)
        metadata_impacts = {
            impact["name"]: impact for impact in metadata_result["downstream_impacts"]
        }
        metric_names = {f"{namespace}.value_sum_{index:02d}" for index in range(12)}
        assert metric_names <= metadata_impacts.keys()
        metadata_statuses = {
            name: metadata_impacts[name]["semantic_comparison"] for name in metric_names
        }
        assert all(
            comparison["status"] == "unchanged"
            for comparison in metadata_statuses.values()
        ), metadata_statuses
        assert metadata_result["semantic_review_recipients"] == []

        changed_type = _project(
            namespace,
            "SELECT id, CAST(value AS DOUBLE) AS value FROM ${prefix}typed_source",
            "Updated description",
        )
        type_result = await _deploy(client_with_roads, changed_type)
        type_impacts = {
            impact["name"]: impact for impact in type_result["downstream_impacts"]
        }
        assert metric_names <= type_impacts.keys()
        assert all(
            type_impacts[name]["semantic_comparison"]["status"] == "changed"
            for name in metric_names
        )
        assert type_result["semantic_review_recipients"] == ["dj"]

        missing_value = _project(
            namespace,
            "SELECT id FROM ${prefix}typed_source",
            "Updated description",
        )
        invalidation_result = await _deploy(client_with_roads, missing_value)
        invalidation_impacts = {
            impact["name"]: impact
            for impact in invalidation_result["downstream_impacts"]
        }
        assert metric_names <= invalidation_impacts.keys()
        for name in metric_names:
            impact = invalidation_impacts[name]
            assert impact["semantic_comparison"] == {
                "status": "indeterminate",
                "precision": "unknown",
                "reasons": ["proposed_schema_unavailable"],
            }
            assert impact["semantic_fingerprint"] is None
        assert invalidation_result["semantic_review_recipients"] == []
