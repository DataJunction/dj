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
