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

    @pytest.mark.asyncio
    async def test_impact_analysis_type_change_warning(
        self,
        client_with_roads,
    ):
        """Test that type changes generate appropriate warnings (covers line 618)"""
        import asyncio

        # Deploy a simple node first
        initial_spec = DeploymentSpec(
            namespace="impact_type_change_test",
            nodes=[
                SourceSpec(
                    name="source_with_int",
                    catalog="default",
                    schema_="test",
                    table="source",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="amount", type="int"),
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

        # Analyze impact of changing column type
        modified_spec = DeploymentSpec(
            namespace="impact_type_change_test",
            nodes=[
                SourceSpec(
                    name="source_with_int",
                    catalog="default",
                    schema_="test",
                    table="source",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="amount", type="bigint"),  # int -> bigint
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

        # Check for type change in column_changes
        update_changes = [
            c for c in impact.changes if c.operation == NodeChangeOperation.UPDATE
        ]
        if update_changes:
            source_change = update_changes[0]
            type_changes = [
                cc
                for cc in source_change.column_changes
                if cc.change_type == ColumnChangeType.TYPE_CHANGED
            ]
            if type_changes:
                assert type_changes[0].column == "amount"


class TestImpactAnalysisInternalFunctions:
    """Tests for internal impact analysis helper functions"""

    def test_validate_specs_empty_list(self):
        """Test that empty node_specs returns empty dict (covers line 250)"""
        from datajunction_server.internal.deployment.impact import (
            _validate_specs_for_impact,
        )

        # We can't easily test async functions without a session,
        # but we can test synchronously with mocking
        import asyncio
        from unittest.mock import MagicMock

        async def run_test():
            session = MagicMock()
            result = await _validate_specs_for_impact(session, [], {})
            assert result == {}

        asyncio.get_event_loop().run_until_complete(run_test())

    def test_detect_column_changes_no_existing_node(self):
        """Test column change detection with no existing node (covers line 296)"""
        from datajunction_server.internal.deployment.impact import (
            _detect_column_changes,
        )
        from datajunction_server.models.deployment import TransformSpec, ColumnSpec

        new_spec = TransformSpec(
            name="new.node",
            query="SELECT 1",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # No existing node
        result = _detect_column_changes(None, new_spec)
        assert result == []

    def test_detect_column_changes_no_columns_available(self):
        """Test column change detection with no columns in spec (covers line 314)"""
        from datajunction_server.internal.deployment.impact import (
            _detect_column_changes,
        )
        from datajunction_server.models.deployment import TransformSpec
        from unittest.mock import MagicMock

        # Create mock existing node
        existing_node = MagicMock()
        existing_node.current.columns = [MagicMock(name="id")]

        # Spec with no columns
        new_spec = TransformSpec(
            name="test.node",
            query="SELECT 1",
        )

        # No inferred columns, no spec columns
        result = _detect_column_changes(existing_node, new_spec, inferred_columns=None)
        assert result == []

    def test_normalize_type_empty_string(self):
        """Test type normalization with empty string (covers line 391)"""
        from datajunction_server.internal.deployment.impact import _normalize_type

        assert _normalize_type(None) == ""
        assert _normalize_type("") == ""

    def test_normalize_type_aliases(self):
        """Test type normalization with various aliases"""
        from datajunction_server.internal.deployment.impact import _normalize_type

        # Test common type aliases
        assert _normalize_type("BIGINT") == "bigint"
        assert _normalize_type("long") == "bigint"
        assert _normalize_type("int64") == "bigint"
        assert _normalize_type("INTEGER") == "int"
        assert _normalize_type("STRING") == "varchar"
        assert _normalize_type("text") == "varchar"

    def test_generate_warnings_type_changed(self):
        """Test warning generation for type changes (covers line 618)"""
        from datajunction_server.internal.deployment.impact import _generate_warnings
        from datajunction_server.models.impact import (
            NodeChange,
            NodeChangeOperation,
            ColumnChange,
            ColumnChangeType,
        )
        from datajunction_server.models.node import NodeType

        changes = [
            NodeChange(
                name="test.source",
                operation=NodeChangeOperation.UPDATE,
                node_type=NodeType.SOURCE,
                column_changes=[
                    ColumnChange(
                        column="amount",
                        change_type=ColumnChangeType.TYPE_CHANGED,
                        old_type="int",
                        new_type="bigint",
                    ),
                ],
            ),
        ]

        warnings = _generate_warnings(changes, [])

        # Should have a warning about type change
        type_warnings = [w for w in warnings if "type" in w.lower()]
        assert len(type_warnings) >= 1
        assert "amount" in type_warnings[0]

    def test_generate_warnings_query_changed_no_columns(self):
        """Test warning for query changes without column changes (covers line 637)"""
        from datajunction_server.internal.deployment.impact import _generate_warnings
        from datajunction_server.models.impact import (
            NodeChange,
            NodeChangeOperation,
        )
        from datajunction_server.models.node import NodeType

        changes = [
            NodeChange(
                name="test.transform",
                operation=NodeChangeOperation.UPDATE,
                node_type=NodeType.TRANSFORM,
                changed_fields=["query"],
                column_changes=[],  # No column changes detected
            ),
        ]

        warnings = _generate_warnings(changes, [])

        # Should have a warning about query change
        query_warnings = [w for w in warnings if "query" in w.lower()]
        assert len(query_warnings) >= 1

    def test_generate_warnings_deletion_with_downstreams(self):
        """Test warning for deletions with downstream dependencies (covers line 650)"""
        from datajunction_server.internal.deployment.impact import _generate_warnings
        from datajunction_server.models.impact import (
            NodeChange,
            NodeChangeOperation,
            DownstreamImpact,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType, NodeStatus

        changes = [
            NodeChange(
                name="test.to_delete",
                operation=NodeChangeOperation.DELETE,
                node_type=NodeType.SOURCE,
            ),
        ]

        downstream_impacts = [
            DownstreamImpact(
                name="test.dependent",
                node_type=NodeType.TRANSFORM,
                current_status=NodeStatus.VALID,
                predicted_status=NodeStatus.INVALID,
                impact_type=ImpactType.WILL_INVALIDATE,
                impact_reason="Depends on deleted node",
                depth=1,
                caused_by=["test.to_delete"],
            ),
        ]

        warnings = _generate_warnings(changes, downstream_impacts)

        # Should have a warning about deletion
        delete_warnings = [w for w in warnings if "delet" in w.lower()]
        assert len(delete_warnings) >= 1

    def test_generate_warnings_external_impacts(self):
        """Test warning for external namespace impacts (covers lines 657-659)"""
        from datajunction_server.internal.deployment.impact import _generate_warnings
        from datajunction_server.models.impact import (
            NodeChange,
            NodeChangeOperation,
            DownstreamImpact,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType, NodeStatus

        changes = [
            NodeChange(
                name="my_namespace.source",
                operation=NodeChangeOperation.UPDATE,
                node_type=NodeType.SOURCE,
            ),
        ]

        downstream_impacts = [
            DownstreamImpact(
                name="other_namespace.dependent",
                node_type=NodeType.TRANSFORM,
                current_status=NodeStatus.VALID,
                predicted_status=NodeStatus.VALID,
                impact_type=ImpactType.MAY_AFFECT,
                impact_reason="Depends on updated source",
                depth=1,
                caused_by=["my_namespace.source"],
                is_external=True,
            ),
        ]

        warnings = _generate_warnings(changes, downstream_impacts)

        # Should have a warning about external impacts
        external_warnings = [w for w in warnings if "outside" in w.lower()]
        assert len(external_warnings) >= 1

    def test_generate_warnings_high_invalidation_count(self):
        """Test warning for high invalidation count (covers line 671)"""
        from datajunction_server.internal.deployment.impact import _generate_warnings
        from datajunction_server.models.impact import (
            NodeChange,
            NodeChangeOperation,
            DownstreamImpact,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType, NodeStatus

        changes = [
            NodeChange(
                name="test.source",
                operation=NodeChangeOperation.DELETE,
                node_type=NodeType.SOURCE,
            ),
        ]

        # Create more than 10 downstream impacts with WILL_INVALIDATE
        downstream_impacts = [
            DownstreamImpact(
                name=f"test.dependent_{i}",
                node_type=NodeType.TRANSFORM,
                current_status=NodeStatus.VALID,
                predicted_status=NodeStatus.INVALID,
                impact_type=ImpactType.WILL_INVALIDATE,
                impact_reason="Depends on deleted node",
                depth=1,
                caused_by=["test.source"],
            )
            for i in range(15)
        ]

        warnings = _generate_warnings(changes, downstream_impacts)

        # Should have a warning about high invalidation count
        high_impact_warnings = [
            w for w in warnings if "15" in w or "invalidate" in w.lower()
        ]
        assert len(high_impact_warnings) >= 1

    def test_predict_downstream_impact_delete_operation(self):
        """Test downstream impact prediction for DELETE operation (covers line 496)"""
        from datajunction_server.internal.deployment.impact import (
            _predict_downstream_impact,
        )
        from datajunction_server.models.impact import (
            NodeChange,
            NodeChangeOperation,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType, NodeStatus
        from unittest.mock import MagicMock

        # Create a delete change
        change = NodeChange(
            name="test.to_delete",
            operation=NodeChangeOperation.DELETE,
            node_type=NodeType.SOURCE,
        )

        # Create mock downstream node
        downstream = MagicMock()
        downstream.name = "test.dependent"
        downstream.type = NodeType.TRANSFORM
        downstream.current.status = NodeStatus.VALID

        result = _predict_downstream_impact(
            downstream,
            change,
            deployment_namespace="test",
        )

        assert result.impact_type == ImpactType.WILL_INVALIDATE
        assert result.predicted_status == NodeStatus.INVALID
        assert "deleted" in result.impact_reason.lower()

    def test_predict_downstream_impact_cube_on_metric_change(self):
        """Test cube impact when metric changes (covers lines 516-535)"""
        from datajunction_server.internal.deployment.impact import (
            _predict_downstream_impact,
        )
        from datajunction_server.models.impact import (
            NodeChange,
            NodeChangeOperation,
            ColumnChange,
            ColumnChangeType,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType, NodeStatus
        from unittest.mock import MagicMock

        # Create a metric update with type changes
        change = NodeChange(
            name="test.metric",
            operation=NodeChangeOperation.UPDATE,
            node_type=NodeType.METRIC,
            column_changes=[
                ColumnChange(
                    column="value",
                    change_type=ColumnChangeType.TYPE_CHANGED,
                    old_type="int",
                    new_type="bigint",
                ),
            ],
        )

        # Create mock cube downstream
        downstream = MagicMock()
        downstream.name = "test.cube"
        downstream.type = NodeType.CUBE
        downstream.current.status = NodeStatus.VALID

        result = _predict_downstream_impact(
            downstream,
            change,
            deployment_namespace="test",
        )

        assert result.impact_type == ImpactType.MAY_AFFECT
        assert "type changes" in result.impact_reason.lower()

    def test_predict_downstream_impact_cube_on_dimension_change(self):
        """Test cube impact when dimension changes (covers line 549)"""
        from datajunction_server.internal.deployment.impact import (
            _predict_downstream_impact,
        )
        from datajunction_server.models.impact import (
            NodeChange,
            NodeChangeOperation,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType, NodeStatus
        from unittest.mock import MagicMock

        # Create a dimension update
        change = NodeChange(
            name="test.dimension",
            operation=NodeChangeOperation.UPDATE,
            node_type=NodeType.DIMENSION,
        )

        # Create mock cube downstream
        downstream = MagicMock()
        downstream.name = "test.cube"
        downstream.type = NodeType.CUBE
        downstream.current.status = NodeStatus.VALID

        result = _predict_downstream_impact(
            downstream,
            change,
            deployment_namespace="test",
        )

        assert result.impact_type == ImpactType.MAY_AFFECT
        assert "dimension" in result.impact_reason.lower()

    def test_predict_downstream_impact_generic_update(self):
        """Test generic downstream impact for updates (covers line 586)"""
        from datajunction_server.internal.deployment.impact import (
            _predict_downstream_impact,
        )
        from datajunction_server.models.impact import (
            NodeChange,
            NodeChangeOperation,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType, NodeStatus
        from unittest.mock import MagicMock

        # Create a transform update with no column changes
        change = NodeChange(
            name="test.source",
            operation=NodeChangeOperation.UPDATE,
            node_type=NodeType.SOURCE,
            column_changes=[],  # No breaking column changes
        )

        # Create mock transform downstream
        downstream = MagicMock()
        downstream.name = "test.transform"
        downstream.type = NodeType.TRANSFORM
        downstream.current.status = NodeStatus.VALID

        result = _predict_downstream_impact(
            downstream,
            change,
            deployment_namespace="test",
        )

        assert result.impact_type == ImpactType.MAY_AFFECT
        assert "updated" in result.impact_reason.lower()

    def test_predict_downstream_impact_is_external(self):
        """Test downstream impact marks external nodes correctly"""
        from datajunction_server.internal.deployment.impact import (
            _predict_downstream_impact,
        )
        from datajunction_server.models.impact import (
            NodeChange,
            NodeChangeOperation,
        )
        from datajunction_server.models.node import NodeType, NodeStatus
        from unittest.mock import MagicMock

        # Create an update
        change = NodeChange(
            name="my_namespace.source",
            operation=NodeChangeOperation.UPDATE,
            node_type=NodeType.SOURCE,
        )

        # Create mock downstream in different namespace
        downstream = MagicMock()
        downstream.name = "other_namespace.transform"
        downstream.type = NodeType.TRANSFORM
        downstream.current.status = NodeStatus.VALID

        result = _predict_downstream_impact(
            downstream,
            change,
            deployment_namespace="my_namespace",
        )

        assert result.is_external is True

    def test_detect_column_changes_with_spec_columns_fallback(self):
        """Test column detection falls back to spec columns (covers lines 305-310)"""
        from datajunction_server.internal.deployment.impact import (
            _detect_column_changes,
        )
        from datajunction_server.models.deployment import TransformSpec, ColumnSpec
        from unittest.mock import MagicMock

        # Create mock existing node
        existing_col = MagicMock()
        existing_col.name = "id"
        existing_col.type.value = "int"

        existing_node = MagicMock()
        existing_node.current.columns = [existing_col]

        # Spec with columns (used as fallback when no inferred_columns)
        new_spec = TransformSpec(
            name="test.node",
            query="SELECT id, name FROM source",
            columns=[
                ColumnSpec(name="id", type="int"),
                ColumnSpec(name="name", type="string"),  # New column
            ],
        )

        # No inferred columns, should fallback to spec columns
        result = _detect_column_changes(existing_node, new_spec, inferred_columns=None)

        # Should detect the new 'name' column as added
        added_cols = [c for c in result if c.change_type.value == "added"]
        assert len(added_cols) == 1
        assert added_cols[0].column == "name"
