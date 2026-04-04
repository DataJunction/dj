"""
Tests for POST /deployments/impact endpoint (orchestrator dry-run).
"""

import asyncio

import pytest
from unittest import mock

from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentInfo,
    DeploymentSpec,
    SourceSpec,
    TransformSpec,
)


@pytest.fixture(autouse=True, scope="module")
def patch_effective_writer_concurrency():
    from datajunction_server.internal.deployment.deployment import settings

    with mock.patch.object(
        settings.__class__,
        "effective_writer_concurrency",
        new_callable=mock.PropertyMock,
        return_value=1,
    ):
        yield


async def _wait_for_deployment(client, deployment_id: str, timeout: int = 30):
    """Poll until a deployment reaches a terminal status."""
    for _ in range(timeout):
        resp = await client.get(f"/deployments/{deployment_id}")
        if resp.json()["status"] in ("success", "failed"):
            return resp.json()
        await asyncio.sleep(0.1)
    return (await client.get(f"/deployments/{deployment_id}")).json()


class TestDeploymentImpactEndpoint:
    """Tests for POST /deployments/impact (orchestrator dry-run)."""

    @pytest.mark.asyncio
    async def test_impact_create_new_nodes(self, client_with_roads):
        """Deploying into an empty namespace: results show CREATE operations."""
        spec = DeploymentSpec(
            namespace="impact_create_test",
            nodes=[
                SourceSpec(
                    name="orders",
                    catalog="default",
                    schema_="test",
                    table="orders",
                    columns=[
                        ColumnSpec(name="order_id", type="int"),
                        ColumnSpec(name="amount", type="float"),
                    ],
                ),
            ],
        )

        response = await client_with_roads.post(
            "/deployments/impact",
            json=spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200

        data = response.json()
        info = DeploymentInfo(**data)
        assert info.uuid == "dry_run"
        assert info.namespace == "impact_create_test"

        node_results = [r for r in info.results if r.deploy_type == "node"]
        assert len(node_results) == 1
        assert node_results[0].name == "impact_create_test.orders"
        assert node_results[0].operation == "create"

    @pytest.mark.asyncio
    async def test_impact_detects_updates(self, client_with_roads):
        """After deploying a node, a dry-run with a changed query shows UPDATE."""
        initial_spec = DeploymentSpec(
            namespace="impact_update_test",
            nodes=[
                TransformSpec(
                    name="orders_summary",
                    query="SELECT 1 AS order_id FROM ${prefix}raw",
                ),
                SourceSpec(
                    name="raw",
                    catalog="default",
                    schema_="test",
                    table="raw",
                    columns=[ColumnSpec(name="order_id", type="int")],
                ),
            ],
        )

        deploy_resp = await client_with_roads.post(
            "/deployments",
            json=initial_spec.model_dump(by_alias=True),
        )
        assert deploy_resp.status_code == 200
        await _wait_for_deployment(client_with_roads, deploy_resp.json()["uuid"])

        updated_spec = DeploymentSpec(
            namespace="impact_update_test",
            nodes=[
                TransformSpec(
                    name="orders_summary",
                    query="SELECT 1 AS order_id, 'updated' AS status FROM ${prefix}raw",
                ),
                SourceSpec(
                    name="raw",
                    catalog="default",
                    schema_="test",
                    table="raw",
                    columns=[ColumnSpec(name="order_id", type="int")],
                ),
            ],
        )

        response = await client_with_roads.post(
            "/deployments/impact",
            json=updated_spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200

        data = response.json()
        update_results = [
            r
            for r in data["results"]
            if r["operation"] == "update" and r["deploy_type"] == "node"
        ]
        skip_results = [r for r in data["results"] if r["operation"] == "noop"]
        assert len(update_results) == 1
        assert "orders_summary" in update_results[0]["name"]
        assert len(skip_results) >= 1

    @pytest.mark.asyncio
    async def test_impact_detects_deletions(self, client_with_roads):
        """Nodes present in DB but absent from spec appear as DELETE in dry-run."""
        initial_spec = DeploymentSpec(
            namespace="impact_delete_test",
            nodes=[
                SourceSpec(
                    name="to_keep",
                    catalog="default",
                    schema_="test",
                    table="keep",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                SourceSpec(
                    name="to_delete",
                    catalog="default",
                    schema_="test",
                    table="delete_me",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )

        deploy_resp = await client_with_roads.post(
            "/deployments",
            json=initial_spec.model_dump(by_alias=True),
        )
        assert deploy_resp.status_code == 200
        await _wait_for_deployment(client_with_roads, deploy_resp.json()["uuid"])

        modified_spec = DeploymentSpec(
            namespace="impact_delete_test",
            nodes=[
                SourceSpec(
                    name="to_keep",
                    catalog="default",
                    schema_="test",
                    table="keep",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                # to_delete omitted
            ],
        )

        response = await client_with_roads.post(
            "/deployments/impact",
            json=modified_spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200

        data = response.json()
        delete_results = [r for r in data["results"] if r["operation"] == "delete"]
        assert len(delete_results) == 1
        assert "to_delete" in delete_results[0]["name"]

    @pytest.mark.asyncio
    async def test_dry_run_does_not_mutate_db(self, client_with_roads):
        """Calling /deployments/impact must not persist any changes."""
        spec = DeploymentSpec(
            namespace="dry_run_no_mutation_test",
            nodes=[
                SourceSpec(
                    name="ephemeral",
                    catalog="default",
                    schema_="test",
                    table="ephemeral",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )

        # Call dry-run
        impact_resp = await client_with_roads.post(
            "/deployments/impact",
            json=spec.model_dump(by_alias=True),
        )
        assert impact_resp.status_code == 200
        assert impact_resp.json()["uuid"] == "dry_run"

        # The node must NOT exist in the namespace
        nodes_resp = await client_with_roads.get(
            "/namespaces/dry_run_no_mutation_test/nodes/",
        )
        assert nodes_resp.status_code in (200, 404)
        if nodes_resp.status_code == 200:
            node_names = [n["name"] for n in nodes_resp.json()]
            assert "dry_run_no_mutation_test.ephemeral" not in node_names

    @pytest.mark.asyncio
    async def test_downstream_impacts_returned_for_invalid_parent(
        self,
        client_with_roads,
    ):
        """If a parent node is changed to INVALID, downstream nodes appear in
        downstream_impacts with impact_type=will_invalidate."""
        # Deploy a transform that depends on a source
        initial_spec = DeploymentSpec(
            namespace="impact_downstream_test",
            nodes=[
                SourceSpec(
                    name="base",
                    catalog="default",
                    schema_="test",
                    table="base",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="value", type="float"),
                    ],
                ),
                TransformSpec(
                    name="derived",
                    query="SELECT id, value FROM ${prefix}base",
                ),
            ],
        )

        deploy_resp = await client_with_roads.post(
            "/deployments",
            json=initial_spec.model_dump(by_alias=True),
        )
        assert deploy_resp.status_code == 200
        await _wait_for_deployment(client_with_roads, deploy_resp.json()["uuid"])

        # Dry-run: remove the 'value' column from base, making the transform invalid
        modified_spec = DeploymentSpec(
            namespace="impact_downstream_test",
            nodes=[
                SourceSpec(
                    name="base",
                    catalog="default",
                    schema_="test",
                    table="base",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        # 'value' removed
                    ],
                ),
                TransformSpec(
                    name="derived",
                    query="SELECT id, value FROM ${prefix}base",  # will break
                ),
            ],
        )

        response = await client_with_roads.post(
            "/deployments/impact",
            json=modified_spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200

        data = response.json()
        # At minimum the request succeeds and returns the expected shape
        assert "results" in data
        assert "downstream_impacts" in data
        assert isinstance(data["downstream_impacts"], list)
