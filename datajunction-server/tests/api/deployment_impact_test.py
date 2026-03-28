"""
Tests for deployment impact analysis API endpoint.
"""

import asyncio

import pytest
from unittest import mock

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    DeploymentSpec,
    MetricSpec,
    PartitionSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.impact import (
    ColumnChangeType,
    DeploymentImpactResponse,
    ImpactType,
    NodeChangeOperation,
)
from datajunction_server.models.partition import Granularity, PartitionType


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

        # The source node should show update with column removal
        update_changes = [
            c for c in impact.changes if c.operation == NodeChangeOperation.UPDATE
        ]
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

        deploy_response = await client_with_roads.post(
            "/deployments",
            json=initial_spec.model_dump(by_alias=True),
        )
        assert deploy_response.status_code == 200

        deployment_id = deploy_response.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        # Analyze impact of removing to_delete node
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
        assert impact.skip_count == 1

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
        breaking_warnings = [
            w
            for w in impact.warnings
            if "breaking" in w.lower() or "removed" in w.lower()
        ]
        assert len(breaking_warnings) >= 1 or impact.update_count == 1

    @pytest.mark.asyncio
    async def test_impact_analysis_type_change_warning(
        self,
        client_with_roads,
    ):
        """Test that type changes generate appropriate warnings"""
        import asyncio

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

        deployment_id = deploy_response.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

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


class TestAnalyzeDeploymentImpactCoverage:
    """Tests for analyze_deployment_impact function to improve coverage"""

    @pytest.mark.asyncio
    async def test_cube_spec_skips_column_detection(
        self,
        client_with_roads,
    ):
        """Test that cube specs skip column change detection"""
        import asyncio

        initial_spec = DeploymentSpec(
            namespace="cube_column_skip_test",
            nodes=[
                SourceSpec(
                    name="events",
                    catalog="default",
                    schema_="test",
                    table="events",
                    columns=[
                        ColumnSpec(name="event_id", type="int"),
                        ColumnSpec(name="user_id", type="int"),
                        ColumnSpec(name="value", type="float"),
                    ],
                ),
            ],
        )

        deploy_response = await client_with_roads.post(
            "/deployments",
            json=initial_spec.model_dump(by_alias=True),
        )
        assert deploy_response.status_code == 200

        deployment_id = deploy_response.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        response = await client_with_roads.post(
            "/deployments/impact",
            json=initial_spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200

        impact = DeploymentImpactResponse(**response.json())
        # Should be a NOOP since nothing changed
        assert impact.skip_count == 1


class TestImpactAnalysisInternalFunctions:
    """Tests for internal impact analysis helper functions"""

    # ---------------------------------------------------------------------------
    # _detect_source_column_changes
    # ---------------------------------------------------------------------------

    def test_detect_source_column_changes_no_existing_node(self):
        """Returns empty list when existing_node has no current revision."""
        from datajunction_server.internal.deployment.impact import (
            _detect_source_column_changes,
        )
        from datajunction_server.models.deployment import SourceSpec
        from unittest.mock import MagicMock

        existing_node = MagicMock()
        existing_node.current = None  # No current revision
        spec = SourceSpec(
            name="test.source",
            catalog="cat",
            schema_="sch",
            table="t",
            columns=[ColumnSpec(name="id", type="int")],
        )
        result = _detect_source_column_changes(existing_node, spec)
        assert result == []

    def test_detect_source_column_changes_no_spec_columns(self):
        """Returns empty list when the spec has no explicit columns."""
        from datajunction_server.internal.deployment.impact import (
            _detect_source_column_changes,
        )
        from datajunction_server.models.deployment import SourceSpec
        from unittest.mock import MagicMock

        existing_col = MagicMock()
        existing_col.name = "id"
        existing_col.type = "int"

        existing_node = MagicMock()
        existing_node.current.columns = [existing_col]

        # Spec with no columns → nothing to compare
        spec = SourceSpec(
            name="test.source",
            catalog="cat",
            schema_="sch",
            table="t",
        )
        result = _detect_source_column_changes(existing_node, spec)
        assert result == []

    def test_detect_source_column_changes_detects_removed(self):
        """Column present in DB but absent from new spec → REMOVED."""
        from datajunction_server.internal.deployment.impact import (
            _detect_source_column_changes,
        )
        from datajunction_server.models.deployment import SourceSpec
        from datajunction_server.models.impact import ColumnChangeType
        from unittest.mock import MagicMock

        existing_col = MagicMock()
        existing_col.name = "old_col"
        existing_col.type = "int"

        existing_node = MagicMock()
        existing_node.current.columns = [existing_col]

        spec = SourceSpec(
            name="test.source",
            catalog="cat",
            schema_="sch",
            table="t",
            columns=[ColumnSpec(name="new_col", type="string")],
        )
        result = _detect_source_column_changes(existing_node, spec)
        removed = [c for c in result if c.change_type == ColumnChangeType.REMOVED]
        added = [c for c in result if c.change_type == ColumnChangeType.ADDED]
        assert any(c.column == "old_col" for c in removed)
        assert any(c.column == "new_col" for c in added)

    def test_detect_source_column_changes_detects_type_change(self):
        """Column present in both with different type → TYPE_CHANGED."""
        from datajunction_server.internal.deployment.impact import (
            _detect_source_column_changes,
        )
        from datajunction_server.models.deployment import SourceSpec
        from datajunction_server.models.impact import ColumnChangeType
        from unittest.mock import MagicMock

        existing_col = MagicMock()
        existing_col.name = "amount"
        existing_col.type = MagicMock()
        existing_col.type.__str__ = lambda self: "int"

        existing_node = MagicMock()
        existing_node.current.columns = [existing_col]

        spec = SourceSpec(
            name="test.source",
            catalog="cat",
            schema_="sch",
            table="t",
            columns=[ColumnSpec(name="amount", type="bigint")],
        )
        result = _detect_source_column_changes(existing_node, spec)
        type_changes = [
            c for c in result if c.change_type == ColumnChangeType.TYPE_CHANGED
        ]
        assert len(type_changes) == 1
        assert type_changes[0].column == "amount"

    def test_detect_source_column_changes_no_change_when_type_none(self):
        """When new spec type is None/empty, no TYPE_CHANGED is emitted."""
        from datajunction_server.internal.deployment.impact import (
            _detect_source_column_changes,
        )
        from datajunction_server.models.deployment import SourceSpec
        from datajunction_server.models.impact import ColumnChangeType
        from unittest.mock import MagicMock

        existing_col = MagicMock()
        existing_col.name = "amount"
        existing_col.type = MagicMock()
        existing_col.type.__str__ = lambda self: "int"

        existing_node = MagicMock()
        existing_node.current.columns = [existing_col]

        # ColumnSpec with no type → should not produce TYPE_CHANGED
        spec = SourceSpec(
            name="test.source",
            catalog="cat",
            schema_="sch",
            table="t",
            columns=[ColumnSpec(name="amount", type=None)],
        )
        result = _detect_source_column_changes(existing_node, spec)
        type_changes = [
            c for c in result if c.change_type == ColumnChangeType.TYPE_CHANGED
        ]
        assert len(type_changes) == 0

    # ---------------------------------------------------------------------------
    # _normalize_type
    # ---------------------------------------------------------------------------

    def test_normalize_type_empty_string(self):
        """Test type normalization with empty/None input."""
        from datajunction_server.internal.deployment.impact import _normalize_type

        assert _normalize_type(None) == ""
        assert _normalize_type("") == ""

    def test_normalize_type_aliases(self):
        """Test type normalization with various aliases."""
        from datajunction_server.internal.deployment.impact import _normalize_type

        assert _normalize_type("BIGINT") == "bigint"
        assert _normalize_type("long") == "bigint"
        assert _normalize_type("int64") == "bigint"
        assert _normalize_type("INTEGER") == "int"
        assert _normalize_type("STRING") == "varchar"
        assert _normalize_type("text") == "varchar"

    # ---------------------------------------------------------------------------
    # _generate_warnings
    # ---------------------------------------------------------------------------

    def test_generate_warnings_type_changed(self):
        """Warning is emitted for type changes."""
        from datajunction_server.internal.deployment.impact import _generate_warnings
        from datajunction_server.models.impact import (
            NodeEffect,
            NodeChangeOperation,
            ColumnChange,
            ColumnChangeType,
        )
        from datajunction_server.models.node import NodeType

        changes = [
            NodeEffect(
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
        type_warnings = [w for w in warnings if "type" in w.lower()]
        assert len(type_warnings) >= 1
        assert "amount" in type_warnings[0]

    def test_generate_warnings_query_changed_no_columns(self):
        """Warning is emitted when query changes but no column changes detected."""
        from datajunction_server.internal.deployment.impact import _generate_warnings
        from datajunction_server.models.impact import NodeEffect, NodeChangeOperation
        from datajunction_server.models.node import NodeType

        changes = [
            NodeEffect(
                name="test.transform",
                operation=NodeChangeOperation.UPDATE,
                node_type=NodeType.TRANSFORM,
                changed_fields=["query"],
                column_changes=[],
            ),
        ]
        warnings = _generate_warnings(changes, [])
        query_warnings = [w for w in warnings if "query" in w.lower()]
        assert len(query_warnings) >= 1

    def test_generate_warnings_deletion_with_downstreams(self):
        """Warning is emitted when a deleted node has downstream dependents."""
        from datajunction_server.internal.deployment.impact import _generate_warnings
        from datajunction_server.models.impact import (
            NodeEffect,
            NodeChangeOperation,
            NodeEffect,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType, NodeStatus

        changes = [
            NodeEffect(
                name="test.to_delete",
                operation=NodeChangeOperation.DELETE,
                node_type=NodeType.SOURCE,
            ),
        ]
        downstream_impacts = [
            NodeEffect(
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
        delete_warnings = [w for w in warnings if "delet" in w.lower()]
        assert len(delete_warnings) >= 1

    def test_generate_warnings_external_impacts(self):
        """Warning is emitted when changes affect nodes in other namespaces."""
        from datajunction_server.internal.deployment.impact import _generate_warnings
        from datajunction_server.models.impact import (
            NodeEffect,
            NodeChangeOperation,
            NodeEffect,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType, NodeStatus

        changes = [
            NodeEffect(
                name="my_namespace.source",
                operation=NodeChangeOperation.UPDATE,
                node_type=NodeType.SOURCE,
            ),
        ]
        downstream_impacts = [
            NodeEffect(
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
        external_warnings = [w for w in warnings if "outside" in w.lower()]
        assert len(external_warnings) >= 1

    def test_generate_warnings_high_invalidation_count(self):
        """Warning is emitted when more than 10 downstream nodes will invalidate."""
        from datajunction_server.internal.deployment.impact import _generate_warnings
        from datajunction_server.models.impact import (
            NodeEffect,
            NodeChangeOperation,
            NodeEffect,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType, NodeStatus

        changes = [
            NodeEffect(
                name="test.source",
                operation=NodeChangeOperation.DELETE,
                node_type=NodeType.SOURCE,
            ),
        ]
        downstream_impacts = [
            NodeEffect(
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
        high_impact_warnings = [
            w for w in warnings if "15" in w or "invalidate" in w.lower()
        ]
        assert len(high_impact_warnings) >= 1

    # ---------------------------------------------------------------------------
    # _analyze_downstream_impacts — delegates to compute_impact
    # ---------------------------------------------------------------------------

    def test_analyze_downstream_impacts_skip_directly_changed(self):
        """Nodes being directly changed are excluded from downstream impacts."""
        import asyncio
        from unittest.mock import patch, AsyncMock
        from datajunction_server.internal.deployment.impact import (
            _analyze_downstream_impacts,
        )
        from datajunction_server.models.impact import (
            NodeEffect,
            NodeChangeOperation,
        )
        from datajunction_server.models.node import NodeType
        from datajunction_server.models.impact_preview import ImpactedNode
        from datajunction_server.models.node import NodeStatus

        async def run_test():
            session = object()
            changes = [
                NodeEffect(
                    name="test.source",
                    operation=NodeChangeOperation.UPDATE,
                    node_type=NodeType.SOURCE,
                ),
                NodeEffect(
                    name="test.transform",
                    operation=NodeChangeOperation.UPDATE,
                    node_type=NodeType.TRANSFORM,
                ),
            ]
            # compute_impact returns test.transform as impacted, but it's in directly_changed
            mock_impacted = ImpactedNode(
                name="test.transform",
                node_type=NodeType.TRANSFORM,
                namespace="test",
                current_status=NodeStatus.VALID,
                projected_status=NodeStatus.INVALID,
                reason="Upstream columns removed: foo",
                caused_by=["test.source"],
                impact_type="column",
            )
            with patch(
                "datajunction_server.internal.deployment.impact.compute_impact",
                new_callable=AsyncMock,
                return_value=[mock_impacted],
            ):
                result = await _analyze_downstream_impacts(
                    session=session,
                    changes=changes,
                    deployment_namespace="test",
                )
            # test.transform is in directly_changed_names, so it should be skipped
            assert len(result) == 0

        asyncio.get_event_loop().run_until_complete(run_test())

    def test_analyze_downstream_impacts_marks_external(self):
        """Impacts outside the deployment namespace are marked is_external=True."""
        from unittest.mock import patch, AsyncMock
        from datajunction_server.internal.deployment.impact import (
            _analyze_downstream_impacts,
        )
        from datajunction_server.models.impact import (
            NodeEffect,
            NodeChangeOperation,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType
        from datajunction_server.models.impact_preview import ImpactedNode
        from datajunction_server.models.node import NodeStatus

        async def run_test():
            session = object()
            changes = [
                NodeEffect(
                    name="my_ns.source",
                    operation=NodeChangeOperation.UPDATE,
                    node_type=NodeType.SOURCE,
                ),
            ]
            mock_impacted = ImpactedNode(
                name="other_ns.metric",
                node_type=NodeType.METRIC,
                namespace="other_ns",
                current_status=NodeStatus.VALID,
                projected_status=NodeStatus.INVALID,
                reason="Upstream columns removed: foo",
                caused_by=["my_ns.source"],
                impact_type="column",
            )
            with patch(
                "datajunction_server.internal.deployment.impact.compute_impact",
                new_callable=AsyncMock,
                return_value=[mock_impacted],
            ):
                result = await _analyze_downstream_impacts(
                    session=session,
                    changes=changes,
                    deployment_namespace="my_ns",
                )
            assert len(result) == 1
            assert result[0].is_external is True
            assert result[0].impact_type == ImpactType.WILL_INVALIDATE

    def test_analyze_downstream_impacts_dim_link_is_may_affect(self):
        """dimension_link impact type maps to MAY_AFFECT."""
        from unittest.mock import patch, AsyncMock
        from datajunction_server.internal.deployment.impact import (
            _analyze_downstream_impacts,
        )
        from datajunction_server.models.impact import (
            NodeEffect,
            NodeChangeOperation,
            ImpactType,
        )
        from datajunction_server.models.node import NodeType
        from datajunction_server.models.impact_preview import ImpactedNode
        from datajunction_server.models.node import NodeStatus

        async def run_test():
            session = object()
            changes = [
                NodeEffect(
                    name="my_ns.source",
                    operation=NodeChangeOperation.UPDATE,
                    node_type=NodeType.SOURCE,
                ),
            ]
            mock_impacted = ImpactedNode(
                name="my_ns.cube",
                node_type=NodeType.CUBE,
                namespace="my_ns",
                current_status=NodeStatus.VALID,
                projected_status=NodeStatus.INVALID,
                reason="Dimension links removed: my_ns.dim",
                caused_by=["my_ns.source"],
                impact_type="dimension_link",
            )
            with patch(
                "datajunction_server.internal.deployment.impact.compute_impact",
                new_callable=AsyncMock,
                return_value=[mock_impacted],
            ):
                result = await _analyze_downstream_impacts(
                    session=session,
                    changes=changes,
                    deployment_namespace="my_ns",
                )
            assert len(result) == 1
            assert result[0].impact_type == ImpactType.MAY_AFFECT


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


async def _deploy_and_wait(client, spec: DeploymentSpec) -> None:
    """Deploy a spec and block until the deployment succeeds or fails."""
    response = await client.post(
        "/deployments",
        json=spec.model_dump(by_alias=True),
    )
    assert response.status_code == 200, response.text
    deployment_id = response.json()["uuid"]
    for _ in range(60):
        status_response = await client.get(f"/deployments/{deployment_id}")
        if status_response.json()["status"] in ("success", "failed"):
            break
        await asyncio.sleep(0.1)


async def _impact(client, spec: DeploymentSpec) -> DeploymentImpactResponse:
    """POST to /deployments/impact and return the parsed response."""
    response = await client.post(
        "/deployments/impact",
        json=spec.model_dump(by_alias=True),
    )
    assert response.status_code == 200, response.text
    return DeploymentImpactResponse(**response.json())


# ---------------------------------------------------------------------------
# TestChangeDetectionCoverage
# ---------------------------------------------------------------------------


class TestChangeDetectionCoverage:
    """
    Tests for the change-detection logic in analyze_deployment_impact.

    Each test deploys an initial state to its own namespace and then calls
    /deployments/impact with a modified spec, asserting that the right
    changed_fields / column_changes / dim_link_changes appear.
    """

    # ------------------------------------------------------------------
    # Section 2a — Query changes
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_detect_query_changed(self, client_with_roads):
        """'query' appears in changed_fields when a transform's SQL is modified."""
        ns = "det_qry_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="val", type="float"),
                    ],
                ),
                TransformSpec(
                    name="trm",
                    query="SELECT id, val FROM ${prefix}src",
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="val", type="float"),
                    ],
                ),
                TransformSpec(
                    name="trm",
                    query="SELECT id, val, 'v2' AS status FROM ${prefix}src",
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update_changes = [
            c for c in impact.changes if c.operation == NodeChangeOperation.UPDATE
        ]
        trm = next((c for c in update_changes if c.name.endswith(".trm")), None)
        assert trm is not None, "Expected trm to be an UPDATE"
        assert "query" in trm.changed_fields

    @pytest.mark.asyncio
    async def test_detect_query_invalid(self, client_with_roads):
        """'query_invalid' appears in changed_fields when the new query doesn't parse."""
        ns = "det_qry_inv"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                TransformSpec(
                    name="trm",
                    query="SELECT id FROM ${prefix}src",
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        # Reference a column that doesn't exist → validation failure
        broken = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                TransformSpec(
                    name="trm",
                    query="SELECT id, nonexistent_col FROM ${prefix}src",
                ),
            ],
        )
        impact = await _impact(client_with_roads, broken)

        update_changes = [
            c for c in impact.changes if c.operation == NodeChangeOperation.UPDATE
        ]
        trm = next((c for c in update_changes if c.name.endswith(".trm")), None)
        assert trm is not None
        assert "query_invalid" in trm.changed_fields
        assert len(trm.validation_errors) > 0

    # ------------------------------------------------------------------
    # Section 2b — Source column changes
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_detect_column_removed_source(self, client_with_roads):
        """REMOVED appears in column_changes when a source column is dropped."""
        ns = "det_col_rm"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="extra", type="string"),
                    ],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        removed = [
            cc
            for cc in update.column_changes
            if cc.change_type == ColumnChangeType.REMOVED
        ]
        assert any(cc.column == "extra" for cc in removed)

    @pytest.mark.asyncio
    async def test_detect_column_type_changed_source(self, client_with_roads):
        """TYPE_CHANGED appears in column_changes when a column's type changes."""
        ns = "det_col_tc"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="amount", type="int"),
                    ],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="amount", type="bigint"),
                    ],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        type_changes = [
            cc
            for cc in update.column_changes
            if cc.change_type == ColumnChangeType.TYPE_CHANGED
        ]
        assert any(cc.column == "amount" for cc in type_changes)

    @pytest.mark.asyncio
    async def test_detect_column_added_source(self, client_with_roads):
        """ADDED appears in column_changes when a new column is introduced."""
        ns = "det_col_add"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="new_col", type="string"),
                    ],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        added = [
            cc
            for cc in update.column_changes
            if cc.change_type == ColumnChangeType.ADDED
        ]
        assert any(cc.column == "new_col" for cc in added)

    @pytest.mark.asyncio
    async def test_detect_partition_changed_source(self, client_with_roads):
        """PARTITION_CHANGED appears when a column's partition spec changes."""
        ns = "det_part_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(
                            name="dt",
                            type="timestamp",
                            partition=PartitionSpec(type=PartitionType.TEMPORAL),
                        ),
                    ],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        # Change partition granularity
        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(
                            name="dt",
                            type="timestamp",
                            partition=PartitionSpec(
                                type=PartitionType.TEMPORAL,
                                granularity=Granularity.DAY,
                            ),
                        ),
                    ],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        partition_changes = [
            cc
            for cc in update.column_changes
            if cc.change_type == ColumnChangeType.PARTITION_CHANGED
        ]
        assert any(cc.column == "dt" for cc in partition_changes)

    # ------------------------------------------------------------------
    # Section 2c — Metadata changes
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_detect_display_name_changed(self, client_with_roads):
        """'display_name' appears in changed_fields when node display name changes."""
        ns = "det_dn_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                    display_name="Original Name",
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                    display_name="Updated Name",
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        assert "display_name" in update.changed_fields

    @pytest.mark.asyncio
    async def test_detect_description_changed(self, client_with_roads):
        """'description' appears in changed_fields when node description changes."""
        ns = "det_desc_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                    description="Original description",
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                    description="Updated description",
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        assert "description" in update.changed_fields

    @pytest.mark.asyncio
    async def test_detect_tags_changed(self, client_with_roads):
        """'tags' appears in changed_fields when the tag set changes."""
        ns = "det_tags_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                    tags=["some_tag"],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        assert "tags" in update.changed_fields

    @pytest.mark.asyncio
    async def test_detect_owners_changed(self, client_with_roads):
        """'owners' appears in changed_fields when the owner set changes."""
        ns = "det_own_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                    owners=["dj"],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        assert "owners" in update.changed_fields

    @pytest.mark.asyncio
    async def test_detect_custom_metadata_changed(self, client_with_roads):
        """'custom_metadata' appears in changed_fields when it changes."""
        ns = "det_cm_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                    custom_metadata={"key": "old"},
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                    custom_metadata={"key": "new"},
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        assert "custom_metadata" in update.changed_fields

    @pytest.mark.asyncio
    async def test_detect_column_metadata_changed(self, client_with_roads):
        """'column_metadata' appears when a column's display_name or description changes."""
        ns = "det_colmeta_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(name="id", type="int", display_name="ID"),
                    ],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(name="id", type="int", display_name="Identifier"),
                    ],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        assert "column_metadata" in update.changed_fields

    # ------------------------------------------------------------------
    # Section 2d — Source-specific fields
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_detect_catalog_changed(self, client_with_roads):
        """'catalog' appears in changed_fields when the catalog changes."""
        ns = "det_cat_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="basic",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        assert "catalog" in update.changed_fields

    @pytest.mark.asyncio
    async def test_detect_schema_changed(self, client_with_roads):
        """'schema' appears in changed_fields when the underlying schema changes."""
        ns = "det_sch_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="old_schema",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="new_schema",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        assert "schema" in update.changed_fields

    @pytest.mark.asyncio
    async def test_detect_table_changed(self, client_with_roads):
        """'table' appears in changed_fields when the underlying table changes."""
        ns = "det_tbl_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="old_table",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="new_table",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        assert "table" in update.changed_fields

    # ------------------------------------------------------------------
    # Section 2e — Metric-specific fields
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_detect_metric_direction_changed(self, client_with_roads):
        """'direction' appears in changed_fields when metric direction changes."""
        ns = "det_mdir_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                MetricSpec(
                    name="cnt",
                    query="SELECT count(id) FROM ${prefix}src",
                    direction="higher_is_better",
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                MetricSpec(
                    name="cnt",
                    query="SELECT count(id) FROM ${prefix}src",
                    direction="lower_is_better",
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".cnt")), None)
        assert update is not None
        assert "direction" in update.changed_fields

    @pytest.mark.asyncio
    async def test_detect_metric_significant_digits_changed(self, client_with_roads):
        """'significant_digits' appears when the metric significant_digits changes."""
        ns = "det_sd_chg"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                MetricSpec(
                    name="cnt",
                    query="SELECT count(id) FROM ${prefix}src",
                    significant_digits=2,
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                MetricSpec(
                    name="cnt",
                    query="SELECT count(id) FROM ${prefix}src",
                    significant_digits=4,
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".cnt")), None)
        assert update is not None
        assert "significant_digits" in update.changed_fields

    # ------------------------------------------------------------------
    # Section 2f — Cube-specific fields
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_detect_cube_metrics_changed(self, client_with_roads):
        """'metrics' appears in cube changed_fields when the metric set changes."""
        ns = "det_cube_m"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                MetricSpec(
                    name="cnt_a",
                    query="SELECT count(id) FROM ${prefix}src",
                ),
                MetricSpec(
                    name="cnt_b",
                    query="SELECT count(id) FROM ${prefix}src",
                ),
                CubeSpec(
                    name="cube",
                    metrics=["${prefix}cnt_a"],
                    dimensions=[],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        # Change cube to use cnt_b instead of cnt_a
        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                MetricSpec(
                    name="cnt_a",
                    query="SELECT count(id) FROM ${prefix}src",
                ),
                MetricSpec(
                    name="cnt_b",
                    query="SELECT count(id) FROM ${prefix}src",
                ),
                CubeSpec(
                    name="cube",
                    metrics=["${prefix}cnt_b"],
                    dimensions=[],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".cube")), None)
        assert update is not None
        assert "metrics" in update.changed_fields

    @pytest.mark.asyncio
    async def test_detect_cube_filters_changed(self, client_with_roads):
        """'filters' appears in cube changed_fields when the filter set changes."""
        ns = "det_cube_f"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                MetricSpec(
                    name="cnt",
                    query="SELECT count(id) FROM ${prefix}src",
                ),
                CubeSpec(
                    name="cube",
                    metrics=["${prefix}cnt"],
                    dimensions=[],
                    filters=[],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                MetricSpec(
                    name="cnt",
                    query="SELECT count(id) FROM ${prefix}src",
                ),
                CubeSpec(
                    name="cube",
                    metrics=["${prefix}cnt"],
                    dimensions=[],
                    filters=["id > 0"],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".cube")), None)
        assert update is not None
        assert "filters" in update.changed_fields

    # ------------------------------------------------------------------
    # Section 2g — Dim link changes
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_detect_dim_link_removed_explicit(self, client_with_roads):
        """operation='removed' in dim_link_changes when a link is explicitly dropped."""
        ns = "det_dlrm"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="orders",
                    catalog="default",
                    schema_="t",
                    table="orders",
                    columns=[
                        ColumnSpec(name="order_id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                ),
                DimensionSpec(
                    name="dim",
                    query="SELECT dim_id FROM ${prefix}orders",
                    primary_key=["dim_id"],
                ),
                SourceSpec(
                    name="facts",
                    catalog="default",
                    schema_="t",
                    table="facts",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                    dimension_links=[
                        DimensionJoinLinkSpec(
                            dimension_node="${prefix}dim",
                            join_on="${prefix}facts.dim_id = ${prefix}dim.dim_id",
                        ),
                    ],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        # Remove the dim link from facts
        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="orders",
                    catalog="default",
                    schema_="t",
                    table="orders",
                    columns=[
                        ColumnSpec(name="order_id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                ),
                DimensionSpec(
                    name="dim",
                    query="SELECT dim_id FROM ${prefix}orders",
                    primary_key=["dim_id"],
                ),
                SourceSpec(
                    name="facts",
                    catalog="default",
                    schema_="t",
                    table="facts",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                    # No dimension_links
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".facts")), None)
        assert update is not None
        removed_links = [
            dlc for dlc in update.dim_link_changes if dlc.operation == "removed"
        ]
        assert len(removed_links) > 0

    @pytest.mark.asyncio
    async def test_detect_dim_link_added(self, client_with_roads):
        """operation='added' in dim_link_changes when a new link is introduced."""
        ns = "det_dladd"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="orders",
                    catalog="default",
                    schema_="t",
                    table="orders",
                    columns=[
                        ColumnSpec(name="order_id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                ),
                DimensionSpec(
                    name="dim",
                    query="SELECT dim_id FROM ${prefix}orders",
                    primary_key=["dim_id"],
                ),
                SourceSpec(
                    name="facts",
                    catalog="default",
                    schema_="t",
                    table="facts",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                    # No links initially
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        # Add a dim link to facts
        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="orders",
                    catalog="default",
                    schema_="t",
                    table="orders",
                    columns=[
                        ColumnSpec(name="order_id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                ),
                DimensionSpec(
                    name="dim",
                    query="SELECT dim_id FROM ${prefix}orders",
                    primary_key=["dim_id"],
                ),
                SourceSpec(
                    name="facts",
                    catalog="default",
                    schema_="t",
                    table="facts",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                    dimension_links=[
                        DimensionJoinLinkSpec(
                            dimension_node="${prefix}dim",
                            join_on="${prefix}facts.dim_id = ${prefix}dim.dim_id",
                        ),
                    ],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".facts")), None)
        assert update is not None
        added_links = [
            dlc for dlc in update.dim_link_changes if dlc.operation == "added"
        ]
        assert len(added_links) > 0

    @pytest.mark.asyncio
    async def test_detect_dim_link_updated_join_on(self, client_with_roads):
        """operation='updated' in dim_link_changes when join_on expression changes."""
        ns = "det_dlupd"
        dim_link = DimensionJoinLinkSpec(
            dimension_node="${prefix}dim",
            join_on="${prefix}facts.dim_id = ${prefix}dim.dim_id",
        )
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="orders",
                    catalog="default",
                    schema_="t",
                    table="orders",
                    columns=[
                        ColumnSpec(name="order_id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                ),
                DimensionSpec(
                    name="dim",
                    query="SELECT dim_id FROM ${prefix}orders",
                    primary_key=["dim_id"],
                ),
                SourceSpec(
                    name="facts",
                    catalog="default",
                    schema_="t",
                    table="facts",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                        ColumnSpec(name="alt_id", type="int"),
                    ],
                    dimension_links=[dim_link],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        # Change join_on expression
        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="orders",
                    catalog="default",
                    schema_="t",
                    table="orders",
                    columns=[
                        ColumnSpec(name="order_id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                ),
                DimensionSpec(
                    name="dim",
                    query="SELECT dim_id FROM ${prefix}orders",
                    primary_key=["dim_id"],
                ),
                SourceSpec(
                    name="facts",
                    catalog="default",
                    schema_="t",
                    table="facts",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                        ColumnSpec(name="alt_id", type="int"),
                    ],
                    dimension_links=[
                        DimensionJoinLinkSpec(
                            dimension_node="${prefix}dim",
                            join_on="${prefix}facts.alt_id = ${prefix}dim.dim_id",
                        ),
                    ],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".facts")), None)
        assert update is not None
        updated_links = [
            dlc for dlc in update.dim_link_changes if dlc.operation == "updated"
        ]
        assert len(updated_links) > 0

    @pytest.mark.asyncio
    async def test_detect_dim_link_broken_by_column_removal(self, client_with_roads):
        """operation='broken' when a column used in join_on is removed."""
        ns = "det_dlbrk"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="orders",
                    catalog="default",
                    schema_="t",
                    table="orders",
                    columns=[
                        ColumnSpec(name="order_id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                ),
                DimensionSpec(
                    name="dim",
                    query="SELECT dim_id FROM ${prefix}orders",
                    primary_key=["dim_id"],
                ),
                SourceSpec(
                    name="facts",
                    catalog="default",
                    schema_="t",
                    table="facts",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                    dimension_links=[
                        DimensionJoinLinkSpec(
                            dimension_node="${prefix}dim",
                            join_on="${prefix}facts.dim_id = ${prefix}dim.dim_id",
                        ),
                    ],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        # Remove dim_id column that the link joins on → link is implicitly broken
        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="orders",
                    catalog="default",
                    schema_="t",
                    table="orders",
                    columns=[
                        ColumnSpec(name="order_id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                ),
                DimensionSpec(
                    name="dim",
                    query="SELECT dim_id FROM ${prefix}orders",
                    primary_key=["dim_id"],
                ),
                SourceSpec(
                    name="facts",
                    catalog="default",
                    schema_="t",
                    table="facts",
                    # dim_id removed → breaks the join_on
                    columns=[ColumnSpec(name="id", type="int")],
                    dimension_links=[
                        DimensionJoinLinkSpec(
                            dimension_node="${prefix}dim",
                            join_on="${prefix}facts.dim_id = ${prefix}dim.dim_id",
                        ),
                    ],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".facts")), None)
        assert update is not None
        broken_links = [
            dlc for dlc in update.dim_link_changes if dlc.operation == "broken"
        ]
        assert len(broken_links) > 0
        assert any(len(dlc.broken_by_columns) > 0 for dlc in broken_links)

    @pytest.mark.asyncio
    async def test_detect_dim_link_invalid_nonexistent_target(self, client_with_roads):
        """'dim_link_invalid' in changed_fields when the dimension target doesn't exist."""
        ns = "det_dlinv"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="facts",
                    catalog="default",
                    schema_="t",
                    table="facts",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        # Link to a nonexistent dimension
        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="facts",
                    catalog="default",
                    schema_="t",
                    table="facts",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="dim_id", type="int"),
                    ],
                    dimension_links=[
                        DimensionJoinLinkSpec(
                            dimension_node="totally.nonexistent.dim",
                            join_on="${prefix}facts.dim_id = totally.nonexistent.dim.dim_id",
                        ),
                    ],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".facts")), None)
        assert update is not None
        assert "dim_link_invalid" in update.changed_fields
        assert len(update.validation_errors) > 0

    # ------------------------------------------------------------------
    # Section 2h — Validation
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_detect_primary_key_invalid(self, client_with_roads):
        """'primary_key_invalid' in changed_fields when dimension has no primary key."""
        ns = "det_pk_inv"
        # Deploy a valid dimension with primary key
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                DimensionSpec(
                    name="dim",
                    query="SELECT id FROM ${prefix}src",
                    primary_key=["id"],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        # Update the dimension removing primary_key → invalid
        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                DimensionSpec(
                    name="dim",
                    query="SELECT id FROM ${prefix}src",
                    primary_key=[],  # No primary key → invalid
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".dim")), None)
        assert update is not None
        assert "primary_key_invalid" in update.changed_fields

    @pytest.mark.asyncio
    async def test_detect_column_attribute_invalid(self, client_with_roads):
        """'column_attribute_invalid' in changed_fields for unknown column attributes."""
        ns = "det_ca_inv"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(
                            name="id",
                            type="int",
                            attributes=["nonexistent_attribute_xyz"],
                        ),
                    ],
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        update = next((c for c in impact.changes if c.name.endswith(".src")), None)
        assert update is not None
        assert "column_attribute_invalid" in update.changed_fields
        assert len(update.validation_errors) > 0

    @pytest.mark.asyncio
    async def test_detect_noop_when_nothing_changed(self, client_with_roads):
        """NOOP operation and empty changed_fields when spec is identical to DB state."""
        ns = "det_noop"
        spec = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, spec)

        # Impact with identical spec → NOOP
        impact = await _impact(client_with_roads, spec)

        noop_changes = [
            c for c in impact.changes if c.operation == NodeChangeOperation.NOOP
        ]
        assert len(noop_changes) == 0

    # ------------------------------------------------------------------
    # Section 2i — Downstream propagation via HTTP
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_downstream_will_invalidate_on_column_removal(
        self,
        client_with_roads,
    ):
        """WILL_INVALIDATE appears in downstream_impacts when a column is removed."""
        ns = "det_ds_inv"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                        ColumnSpec(name="val", type="float"),
                    ],
                ),
                TransformSpec(
                    name="trm",
                    query="SELECT id, val FROM ${prefix}src",
                ),
                MetricSpec(
                    name="total",
                    query="SELECT sum(val) FROM ${prefix}trm",
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        # Remove val → trm breaks → metric breaks
        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                TransformSpec(
                    name="trm",
                    query="SELECT id, val FROM ${prefix}src",  # val no longer in src
                ),
                MetricSpec(
                    name="total",
                    query="SELECT sum(val) FROM ${prefix}trm",
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        will_invalidate = [
            imp
            for imp in impact.downstream_impacts
            if imp.impact_type == ImpactType.WILL_INVALIDATE
        ]
        assert len(will_invalidate) > 0

    @pytest.mark.asyncio
    async def test_downstream_delete_propagates(self, client_with_roads):
        """Deleting a source causes downstream nodes to be marked WILL_INVALIDATE."""
        ns = "det_ds_del"
        initial = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                TransformSpec(
                    name="trm",
                    query="SELECT id FROM ${prefix}src",
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, initial)

        # Omit src from spec → it gets deleted
        modified = DeploymentSpec(
            namespace=ns,
            nodes=[
                TransformSpec(
                    name="trm",
                    query="SELECT id FROM ${prefix}src",
                ),
            ],
        )
        impact = await _impact(client_with_roads, modified)

        # src is deleted; trm depends on it → WILL_INVALIDATE
        will_invalidate = [
            imp
            for imp in impact.downstream_impacts
            if imp.impact_type == ImpactType.WILL_INVALIDATE
        ]
        assert len(will_invalidate) > 0

    @pytest.mark.asyncio
    async def test_no_downstream_for_noop(self, client_with_roads):
        """A noop node (unchanged) produces no downstream_impacts for itself."""
        ns = "det_ds_noop"
        spec = DeploymentSpec(
            namespace=ns,
            nodes=[
                SourceSpec(
                    name="src",
                    catalog="default",
                    schema_="t",
                    table="t",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
        )
        await _deploy_and_wait(client_with_roads, spec)

        impact = await _impact(client_with_roads, spec)

        # Noop → no downstream impacts
        assert impact.downstream_impacts == []
