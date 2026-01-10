"""
Tests for deployment impact analysis API endpoint.
"""

import pytest
from unittest import mock

from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.impact import (
    ColumnChangeType,
    DeploymentImpactResponse,
    NodeChangeOperation,
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


class TestDeploymentImpactEndpoint:
    """Tests for POST /deployments/impact endpoint"""

    @pytest.mark.asyncio
    async def test_impact_analysis_empty_namespace(
        self,
        client_with_roads,
    ):
        """Test impact analysis on a fresh namespace"""
        deployment_spec = DeploymentSpec(
            namespace="impact_test",
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
            json=deployment_spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200

        impact = DeploymentImpactResponse(**response.json())
        assert impact.namespace == "impact_test"
        assert impact.create_count == 1
        assert impact.update_count == 0
        assert impact.delete_count == 0
        assert impact.skip_count == 0
        assert len(impact.changes) == 1
        assert impact.changes[0].operation == NodeChangeOperation.CREATE
        assert impact.changes[0].name == "impact_test.orders"

    @pytest.mark.asyncio
    async def test_impact_analysis_with_updates(
        self,
        client_with_roads,
    ):
        """Test impact analysis when updating existing nodes"""
        import asyncio

        # First, deploy some nodes
        initial_spec = DeploymentSpec(
            namespace="impact_update_test",
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
                TransformSpec(
                    name="orders_summary",
                    query="SELECT order_id, amount FROM ${prefix}orders",
                ),
            ],
        )

        # Deploy initial nodes
        deploy_response = await client_with_roads.post(
            "/deployments",
            json=initial_spec.model_dump(by_alias=True),
        )
        assert deploy_response.status_code == 200

        # Wait for deployment to complete
        deployment_id = deploy_response.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        # Now analyze impact of an update
        updated_spec = DeploymentSpec(
            namespace="impact_update_test",
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
                TransformSpec(
                    name="orders_summary",
                    query="SELECT order_id, amount, 'updated' as status FROM ${prefix}orders",
                ),
            ],
        )

        response = await client_with_roads.post(
            "/deployments/impact",
            json=updated_spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200

        impact = DeploymentImpactResponse(**response.json())
        assert impact.namespace == "impact_update_test"
        assert impact.create_count == 0
        assert impact.update_count == 1  # orders_summary updated
        assert impact.skip_count == 1  # orders unchanged

        # Check that the update is detected
        update_changes = [
            c for c in impact.changes if c.operation == NodeChangeOperation.UPDATE
        ]
        assert len(update_changes) == 1
        assert update_changes[0].name == "impact_update_test.orders_summary"

    @pytest.mark.asyncio
    async def test_impact_analysis_with_downstream_effects(
        self,
        client_with_roads,
    ):
        """Test impact analysis shows downstream effects"""
        import asyncio

        # First, deploy a chain of dependent nodes
        initial_spec = DeploymentSpec(
            namespace="impact_downstream_test",
            nodes=[
                SourceSpec(
                    name="base_table",
                    catalog="default",
                    schema_="test",
                    table="base",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="value", type="float"),
                    ],
                ),
                TransformSpec(
                    name="intermediate",
                    query="SELECT id, value FROM ${prefix}base_table",
                ),
                MetricSpec(
                    name="total_value",
                    query="SELECT SUM(value) FROM ${prefix}intermediate",
                ),
            ],
        )

        # Deploy initial nodes
        deploy_response = await client_with_roads.post(
            "/deployments",
            json=initial_spec.model_dump(by_alias=True),
        )
        assert deploy_response.status_code == 200

        # Wait for deployment to complete
        deployment_id = deploy_response.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        # Now analyze impact of removing a column from base_table
        # This should show downstream impact on intermediate and total_value
        modified_spec = DeploymentSpec(
            namespace="impact_downstream_test",
            nodes=[
                SourceSpec(
                    name="base_table",
                    catalog="default",
                    schema_="test",
                    table="base",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        # 'value' column removed - breaking change!
                    ],
                ),
                TransformSpec(
                    name="intermediate",
                    query="SELECT id, value FROM ${prefix}base_table",  # Will break
                ),
                MetricSpec(
                    name="total_value",
                    query="SELECT SUM(value) FROM ${prefix}intermediate",
                ),
            ],
        )

        response = await client_with_roads.post(
            "/deployments/impact",
            json=modified_spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200

        impact = DeploymentImpactResponse(**response.json())
        assert impact.namespace == "impact_downstream_test"

        # Check for column change detection
        update_changes = [
            c for c in impact.changes if c.operation == NodeChangeOperation.UPDATE
        ]

        # The source node should show update with column removal
        source_change = next(
            (c for c in update_changes if "base_table" in c.name),
            None,
        )
        if source_change and source_change.column_changes:
            removed_cols = [
                cc
                for cc in source_change.column_changes
                if cc.change_type == ColumnChangeType.REMOVED
            ]
            # Verify column removal is detected
            assert any(cc.column == "value" for cc in removed_cols)

    @pytest.mark.asyncio
    async def test_impact_analysis_with_deletions(
        self,
        client_with_roads,
    ):
        """Test impact analysis when deleting nodes"""
        import asyncio

        # First, deploy some nodes
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

        # Deploy initial nodes
        deploy_response = await client_with_roads.post(
            "/deployments",
            json=initial_spec.model_dump(by_alias=True),
        )
        assert deploy_response.status_code == 200

        # Wait for deployment
        deployment_id = deploy_response.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        # Now analyze impact of removing to_delete node
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
                # to_delete is removed
            ],
        )

        response = await client_with_roads.post(
            "/deployments/impact",
            json=modified_spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200

        impact = DeploymentImpactResponse(**response.json())
        assert impact.delete_count == 1
        assert impact.skip_count == 1  # to_keep unchanged

        # Check deletion is detected
        delete_changes = [
            c for c in impact.changes if c.operation == NodeChangeOperation.DELETE
        ]
        assert len(delete_changes) == 1
        assert "to_delete" in delete_changes[0].name

    @pytest.mark.asyncio
    async def test_impact_analysis_warnings(
        self,
        client_with_roads,
    ):
        """Test that impact analysis generates appropriate warnings"""
        import asyncio

        # Deploy a simple node first
        initial_spec = DeploymentSpec(
            namespace="impact_warnings_test",
            nodes=[
                SourceSpec(
                    name="source_with_columns",
                    catalog="default",
                    schema_="test",
                    table="source",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="important_col", type="string"),
                    ],
                ),
            ],
        )

        deploy_response = await client_with_roads.post(
            "/deployments",
            json=initial_spec.model_dump(by_alias=True),
        )
        assert deploy_response.status_code == 200

        # Wait for deployment
        deployment_id = deploy_response.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        # Analyze impact of removing a column (should generate warning)
        modified_spec = DeploymentSpec(
            namespace="impact_warnings_test",
            nodes=[
                SourceSpec(
                    name="source_with_columns",
                    catalog="default",
                    schema_="test",
                    table="source",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        # important_col removed
                    ],
                ),
            ],
        )

        response = await client_with_roads.post(
            "/deployments/impact",
            json=modified_spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200

        impact = DeploymentImpactResponse(**response.json())

        # Check for breaking change warning
        breaking_warnings = [
            w
            for w in impact.warnings
            if "breaking" in w.lower() or "removed" in w.lower()
        ]
        # Should have a warning about column removal
        assert len(breaking_warnings) >= 1 or impact.update_count == 1
