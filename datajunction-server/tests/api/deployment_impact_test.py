"""
Tests for POST /deployments/impact endpoint (orchestrator dry-run).
"""

import asyncio
from unittest import mock

import pytest

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DeploymentInfo,
    DeploymentSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
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


def _source(
    name: str,
    *,
    description: str | None = None,
    dimension_links: list[DimensionJoinLinkSpec] | None = None,
) -> SourceSpec:
    return SourceSpec(
        name=name,
        catalog="default",
        schema_="test",
        table=name,
        columns=[ColumnSpec(name="id", type="int")],
        description=description,
        dimension_links=dimension_links or [],
    )


def _dimension_project(namespace: str) -> DeploymentSpec:
    return DeploymentSpec(
        namespace=namespace,
        nodes=[
            _source("raw"),
            DimensionSpec(
                name="dimension",
                query="SELECT id FROM ${prefix}raw",
                primary_key=["id"],
            ),
        ],
    )


def _updated_dimension_project(project: DeploymentSpec) -> DeploymentSpec:
    updated = project.model_copy(deep=True)
    dimension = updated.nodes[1]
    assert isinstance(dimension, DimensionSpec)
    dimension.query = "SELECT id FROM ${prefix}raw WHERE id IS NOT NULL"
    return updated


def _dimension_fingerprint(project: DeploymentSpec):
    return project.nodes[1].semantic_fingerprint(
        parent_fingerprints=[project.nodes[0].semantic_fingerprint()],
    )


async def _deploy(client, *specs: DeploymentSpec):
    for spec in specs:
        response = await client.post(
            "/deployments",
            json=spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200
        await _wait_for_deployment(client, response.json()["uuid"])


async def _impact(client, spec: DeploymentSpec):
    response = await client.post(
        "/deployments/impact",
        json=spec.model_dump(by_alias=True),
    )
    assert response.status_code == 200
    return response.json()


async def _impact_nodes(client, spec):
    data = await _impact(client, spec)
    return {
        result["name"]: result
        for result in data["results"]
        if result["deploy_type"] == "node"
    }


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
        assert node_results[0].change_tier == "major"
        assert (
            node_results[0].semantic_fingerprint == spec.nodes[0].semantic_fingerprint()
        )

    @pytest.mark.asyncio
    async def test_impact_detects_updates(self, client_with_roads):
        """A source change updates its own result and descendant fingerprint."""
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
                    query="SELECT 1 AS order_id FROM ${prefix}raw",
                ),
                SourceSpec(
                    name="raw",
                    catalog="default",
                    schema_="test",
                    table="raw_v2",
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
        assert update_results[0]["name"] == "impact_update_test.raw"
        assert len(skip_results) >= 1
        assert update_results[0]["change_tier"] == "major"
        assert (
            update_results[0]["semantic_fingerprint"]
            == updated_spec.nodes[1].semantic_fingerprint().model_dump()
        )
        updated_fingerprint = updated_spec.nodes[0].semantic_fingerprint(
            parent_fingerprints=[updated_spec.nodes[1].semantic_fingerprint()],
        )
        initial_fingerprint = initial_spec.nodes[0].semantic_fingerprint(
            parent_fingerprints=[initial_spec.nodes[1].semantic_fingerprint()],
        )
        unchanged_nodes = [
            result for result in skip_results if result["deploy_type"] == "node"
        ]
        assert all(result["change_tier"] == "none" for result in unchanged_nodes)
        transform_result = next(
            result
            for result in unchanged_nodes
            if result["name"] == "impact_update_test.orders_summary"
        )
        assert (
            transform_result["semantic_fingerprint"] == updated_fingerprint.model_dump()
        )
        assert (
            transform_result["semantic_fingerprint"] != initial_fingerprint.model_dump()
        )

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
        assert delete_results[0]["change_tier"] == "major"
        assert (
            delete_results[0]["semantic_fingerprint"]
            == initial_spec.nodes[1].semantic_fingerprint().model_dump()
        )

    @pytest.mark.asyncio
    async def test_impact_minor_full_noop_and_forced_revalidation(
        self,
        client_with_roads,
    ):
        initial = DeploymentSpec(
            namespace="impact_tiers_test",
            nodes=[
                _source("one", description="Before"),
                _source("two"),
            ],
        )
        await _deploy(client_with_roads, initial)

        minor = initial.model_copy(deep=True)
        minor.nodes[0].description = "After"
        by_name = await _impact_nodes(client_with_roads, minor)
        assert by_name["impact_tiers_test.one"]["change_tier"] == "minor"
        assert (
            by_name["impact_tiers_test.one"]["semantic_fingerprint"]
            == initial.nodes[0].semantic_fingerprint().model_dump()
        )
        assert by_name["impact_tiers_test.two"]["change_tier"] == "none"

        equivalent = initial.model_copy(deep=True)
        equivalent.nodes[1].columns = None
        noop_nodes = await _impact_nodes(client_with_roads, equivalent)
        assert set(noop_nodes) == {
            "impact_tiers_test.one",
            "impact_tiers_test.two",
        }
        assert all(result["change_tier"] == "none" for result in noop_nodes.values())
        assert all(result["semantic_fingerprint"] for result in noop_nodes.values())
        assert (
            noop_nodes["impact_tiers_test.two"]["semantic_fingerprint"]
            == initial.nodes[1].semantic_fingerprint().model_dump()
        )

        forced = equivalent.model_copy(update={"force": True})
        forced_nodes = await _impact_nodes(client_with_roads, forced)
        assert all(result["operation"] == "update" for result in forced_nodes.values())
        assert all(result["change_tier"] == "none" for result in forced_nodes.values())
        assert all(result["semantic_fingerprint"] for result in forced_nodes.values())
        assert (
            forced_nodes["impact_tiers_test.two"]["semantic_fingerprint"]
            == initial.nodes[1].semantic_fingerprint().model_dump()
        )

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
                    owners=["dj"],
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
                    owners=["dj"],
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

        # Every impact carries the owners of the affected node
        derived = [
            impact
            for impact in data["downstream_impacts"]
            if impact["name"] == "impact_downstream_test.derived"
        ]
        assert derived == [
            {
                "name": "impact_downstream_test.derived",
                "node_type": "transform",
                "current_status": "valid",
                "predicted_status": "invalid",
                "impact_type": "will_invalidate",
                "impact_reason": (
                    "Revalidation failed: Column `value` not found in any table. "
                    "Available tables: ['impact_downstream_test.base']"
                ),
                "depth": 1,
                "caused_by": ["impact_downstream_test.base"],
                "is_external": False,
                "owners": ["dj"],
                "semantic_fingerprint": modified_spec.nodes[1]
                .semantic_fingerprint(
                    parent_fingerprints=[
                        modified_spec.nodes[0].semantic_fingerprint(),
                    ],
                )
                .model_dump(),
            },
        ]

    @pytest.mark.asyncio
    async def test_external_downstream_impacts_include_changed_fingerprint(
        self,
        client_with_roads,
    ):
        parent = DeploymentSpec(
            namespace="impact_external_parent",
            nodes=[_source("base")],
        )
        consumer = DeploymentSpec(
            namespace="impact_external_consumer",
            nodes=[
                TransformSpec(
                    name="derived",
                    query="SELECT id FROM impact_external_parent.base",
                ),
            ],
        )
        await _deploy(client_with_roads, parent, consumer)

        updated_parent = parent.model_copy(deep=True)
        assert isinstance(updated_parent.nodes[0], SourceSpec)
        updated_parent.nodes[0].table = "base_v2"
        data = await _impact(client_with_roads, updated_parent)

        external = next(
            impact
            for impact in data["downstream_impacts"]
            if impact["name"] == "impact_external_consumer.derived"
        )
        expected = consumer.nodes[0].semantic_fingerprint(
            parent_fingerprints=[
                updated_parent.nodes[0].semantic_fingerprint(),
            ],
        )
        assert external["is_external"] is True
        assert external["semantic_fingerprint"] == expected.model_dump()

        metadata_only = parent.model_copy(deep=True)
        metadata_only.nodes[0].description = "Updated description"
        data = await _impact(client_with_roads, metadata_only)
        external = next(
            impact
            for impact in data["downstream_impacts"]
            if impact["name"] == "impact_external_consumer.derived"
        )
        assert external["semantic_fingerprint"] is None

    @pytest.mark.asyncio
    async def test_unparseable_node_and_dependents_return_unknown_fingerprints(
        self,
        client_with_roads,
    ):
        initial = DeploymentSpec(
            namespace="impact_unknown",
            nodes=[
                _source("base"),
                TransformSpec(
                    name="broken",
                    query="SELECT id FROM ${prefix}base",
                ),
                TransformSpec(
                    name="dependent",
                    query="SELECT id FROM ${prefix}broken",
                ),
            ],
        )
        await _deploy(client_with_roads, initial)

        proposed = DeploymentSpec(
            namespace="impact_unknown",
            nodes=[
                _source("base"),
                TransformSpec(name="broken", query="SELECT ("),
                TransformSpec(
                    name="dependent",
                    query="SELECT id FROM ${prefix}broken",
                ),
            ],
        )
        data = await _impact(client_with_roads, proposed)
        results = {
            result["name"]: result
            for result in data["results"]
            if result["deploy_type"] == "node"
        }

        assert results["impact_unknown.broken"]["semantic_fingerprint"] == "unknown"
        assert results["impact_unknown.dependent"]["semantic_fingerprint"] == "unknown"

    @pytest.mark.asyncio
    async def test_blocked_delete_keeps_current_fingerprint_state(
        self,
        client_with_roads,
    ):
        parent = DeploymentSpec(
            namespace="impact_blocked_delete",
            nodes=[_source("keep"), _source("base")],
        )
        consumer = DeploymentSpec(
            namespace="impact_blocked_consumer",
            nodes=[
                TransformSpec(
                    name="derived",
                    query="SELECT id FROM impact_blocked_delete.base",
                ),
            ],
        )
        await _deploy(client_with_roads, parent, consumer)

        without_parent = parent.model_copy(
            deep=True,
            update={"nodes": [parent.nodes[0]]},
        )
        data = await _impact(client_with_roads, without_parent)

        delete_result = next(
            result
            for result in data["results"]
            if result["name"] == "impact_blocked_delete.base"
        )
        assert delete_result["status"] == "failed"
        assert (
            delete_result["semantic_fingerprint"]
            == parent.nodes[1].semantic_fingerprint().model_dump()
        )
        assert data["downstream_impacts"] == []

    @pytest.mark.asyncio
    async def test_dimension_link_descendants_are_discovered(
        self,
        client_with_roads,
    ):
        parent = _dimension_project("impact_link_parent")
        consumer = DeploymentSpec(
            namespace="impact_link_consumer",
            nodes=[
                _source(
                    "fact",
                    dimension_links=[
                        DimensionJoinLinkSpec(
                            dimension_node="impact_link_parent.dimension",
                            join_on=(
                                "impact_link_consumer.fact.id = "
                                "impact_link_parent.dimension.id"
                            ),
                        ),
                    ],
                ),
            ],
        )
        await _deploy(client_with_roads, parent, consumer)

        updated_parent = _updated_dimension_project(parent)
        data = await _impact(client_with_roads, updated_parent)

        external = next(
            impact
            for impact in data["downstream_impacts"]
            if impact["name"] == "impact_link_consumer.fact"
        )
        expected = consumer.nodes[0].semantic_fingerprint(
            parent_fingerprints=[_dimension_fingerprint(updated_parent)],
        )
        assert external["semantic_fingerprint"] == expected.model_dump()

    @pytest.mark.asyncio
    async def test_cube_filter_descendants_are_discovered(
        self,
        client_with_roads,
    ):
        parent = _dimension_project("impact_filter_parent")
        consumer = DeploymentSpec(
            namespace="impact_filter_consumer",
            nodes=[
                _source("fact"),
                MetricSpec(
                    name="metric",
                    query="SELECT COUNT(*) FROM ${prefix}fact",
                ),
                CubeSpec(
                    name="cube",
                    metrics=["${prefix}metric"],
                    filters=["impact_filter_parent.dimension.id > 0"],
                ),
            ],
        )
        await _deploy(client_with_roads, parent, consumer)

        updated_parent = _updated_dimension_project(parent)
        data = await _impact(client_with_roads, updated_parent)

        external = next(
            impact
            for impact in data["downstream_impacts"]
            if impact["name"] == "impact_filter_consumer.cube"
        )
        fact_fingerprint = consumer.nodes[0].semantic_fingerprint()
        metric_fingerprint = consumer.nodes[1].semantic_fingerprint(
            parent_fingerprints=[fact_fingerprint],
        )
        expected = consumer.nodes[2].semantic_fingerprint(
            parent_fingerprints=[
                _dimension_fingerprint(updated_parent),
                metric_fingerprint,
            ],
        )
        assert external["semantic_fingerprint"] == expected.model_dump()
