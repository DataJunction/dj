"""
Unit tests for DeploymentOrchestrator
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock

from datajunction_server.internal.deployment.orchestrator import (
    DeploymentOrchestrator,
)
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.internal.deployment.validation import (
    CubeValidationData,
)
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.column import Column
from datajunction_server.models.partition import Granularity

from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.internal.deployment.orchestrator import (
    DeploymentOrchestrator,
    DeploymentPlan,
    ResourceRegistry,
    column_changed,
)
from datajunction_server.internal.deployment.validation import (
    NodeValidationResult,
)
from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentSpec,
    PartitionType,
    PartitionSpec,
    TagSpec,
)
from datajunction_server.models.deployment import (
    SourceSpec,
    TransformSpec,
    MetricSpec,
    CubeSpec,
    DeploymentResult,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
)
from datajunction_server.models.node import NodeStatus
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.database.tag import Tag
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.dimensionlink import (
    JoinCardinality,
    JoinType,
)
from datajunction_server.models.dimensionlink import JoinLinkInput
from datajunction_server.models.history import ActivityType
from datajunction_server.errors import DJError, DJInvalidDeploymentConfig, ErrorCode


@pytest.fixture
def mock_deployment_context(current_user: User):
    """Mock DeploymentContext with sensible defaults"""
    context = Mock()
    context.current_user = current_user
    context.request = Mock()
    context.query_service_client = Mock()
    context.background_tasks = Mock()
    context.save_history = AsyncMock()
    context.cache = Mock()
    return context


@pytest.fixture
def sample_deployment_spec():
    """Sample deployment specification for testing"""
    return DeploymentSpec(
        namespace="test",
        nodes=[
            SourceSpec(
                name="source_node",
                display_name="Test Source",
                table="table",
                catalog="catalog",
                schema="schema",
                columns=[],
                owners=["admin"],
                tags=["example"],
            ),
            TransformSpec(
                name="transform_node",
                display_name="Test Transform",
                query="SELECT * FROM source_node",
                owners=["you"],
            ),
            MetricSpec(
                name="metric_node",
                display_name="Test Metric",
                query="SELECT COUNT(*) FROM transform_node",
                owners=["me"],
            ),
        ],
        tags=[
            TagSpec(
                name="example",
                display_name="Example Tag",
                description="An example tag",
            ),
        ],
    )


@pytest.fixture
def orchestrator(
    sample_deployment_spec,
    session,
    current_user,
    mock_deployment_context,
):
    """Create DeploymentOrchestrator instance for testing"""
    mock_context = MagicMock(spec=DeploymentContext)
    mock_context.current_user = current_user
    return DeploymentOrchestrator(
        deployment_spec=sample_deployment_spec,
        deployment_id="test-deployment-123",
        session=session,
        context=mock_context,
    )


@pytest.fixture
def mock_registry():
    """Mock ResourceRegistry with sample data"""
    registry = ResourceRegistry()

    # Add sample tags
    registry.add_tags(
        {
            "analytics": Mock(spec=Tag, name="analytics", id=1),
            "production": Mock(spec=Tag, name="production", id=2),
        },
    )

    # Add sample users/owners
    registry.add_owners(
        {
            "admin": Mock(spec=User, username="admin", id=1),
            "analyst": Mock(spec=User, username="analyst", id=2),
        },
    )

    # Add sample catalogs
    registry.add_catalogs(
        {
            "catalog": Mock(spec=Catalog, name="catalog", id=1),
        },
    )

    return registry


class TestResourceSetup:
    """Test resource setup methods"""

    @pytest.mark.asyncio
    async def test_setup_deployment_resources_success(self, orchestrator):
        """Test successful deployment resource setup"""
        await orchestrator._setup_deployment_resources()
        assert orchestrator.registry.tags.keys() == {"example"}
        assert orchestrator.registry.owners.keys() == {"you", "me", "admin"}
        assert orchestrator.registry.attributes.keys() == {
            "primary_key",
            "dimension",
            "hidden",
        }

    @pytest.mark.asyncio
    async def test_validate_deployment_resources_with_errors(self, orchestrator):
        """Test validation fails when errors exist"""

        # Add some errors to orchestrator
        orchestrator.errors.append(
            DJError(code=ErrorCode.TAG_NOT_FOUND, message="Tag 'missing' not found"),
        )

        # Should raise DJInvalidDeploymentConfig
        with pytest.raises(DJInvalidDeploymentConfig) as exc_info:
            await orchestrator._validate_deployment_resources()

        assert "Invalid deployment configuration" in str(exc_info.value)


class TestDeploymentPlanning:
    """Test deployment planning methods"""

    @pytest.mark.asyncio
    async def test_find_namespaces_to_create(self, session, current_user):
        """
        Test _find_namespaces_to_create with various node namespaces
        """
        deployment_spec = DeploymentSpec(
            namespace="some.namespace",
            nodes=[
                # Valid node since the namespace prefix will be added
                SourceSpec(
                    name="simple_node",
                    display_name="Simple Node",
                    catalog="catalog",
                    schema="schema",
                    table="table1",
                ),
                # Node not under deployment namespace - should add error
                SourceSpec(
                    name="other_team.${prefix}marketing.campaigns",
                    display_name="Marketing Node",
                    catalog="catalog",
                    schema="schema",
                    table="table2",
                ),
                # Valid node under deployment namespace - should work normally
                SourceSpec(
                    name="${prefix}random.users.active_users",
                    display_name="Active Users",
                    catalog="catalog",
                    schema="schema",
                    table="table3",
                ),
            ],
            tags=[],
        )

        context = MagicMock(autospec=DeploymentContext)
        context.current_user = current_user
        context.save_history = AsyncMock()
        orchestrator = DeploymentOrchestrator(
            deployment_spec=deployment_spec,
            deployment_id="test-deployment",
            session=session,
            context=context,
        )
        result_namespaces = await orchestrator._find_namespaces_to_create()

        # Verify that invalid nodes are skipped
        namespace_strings = {ns for ns in result_namespaces}
        assert namespace_strings == {
            "some.namespace",
            "some.namespace.random",
            "some.namespace.random.users",
        }

        # Verify error added for invalid namespace
        assert len(orchestrator.errors) == 1
        error = orchestrator.errors[0]

        assert error.code == ErrorCode.INVALID_NAMESPACE
        assert "other_team.some.namespace.marketing.campaigns" in error.message
        assert "is not under deployment namespace 'some.namespace'" in error.message
        assert error.context == "namespace validation"

    @pytest.mark.asyncio
    async def test_setup_tags_missing(self, session, current_user):
        """
        Test running _setup_tags with missing tags
        """
        # Pre-create one tag in the database
        session.add_all(
            [
                Tag(
                    name="updated_existing_tag",
                    tag_type="system",
                    display_name="Existing Tag",
                    description="Existing tag description",
                    created_by_id=current_user.id,
                ),
                Tag(
                    name="not_updated_tag",
                    tag_type="system",
                    display_name="Another Tag",
                    description="Another tag",
                    created_by_id=current_user.id,
                ),
            ],
        )
        await session.commit()

        deployment_spec = DeploymentSpec(
            namespace="some.namespace",
            nodes=[
                SourceSpec(
                    name="simple_node",
                    display_name="Simple Node",
                    catalog="catalog",
                    schema="schema",
                    table="table1",
                    tags=["updated_existing_tag", "missing_tag"],
                ),
            ],
            tags=[
                TagSpec(
                    name="new_tag",
                    display_name="New Tag",
                    description="A new tag",
                ),
                TagSpec(
                    name="updated_existing_tag",
                    display_name="Existing Tag",
                    description="An existing tag",
                ),
                TagSpec(
                    name="not_updated_tag",
                    display_name="Another Tag",
                    description="Another tag",
                    tag_type="system",
                ),
            ],
        )
        context = MagicMock(autospec=DeploymentContext)
        context.current_user = current_user
        context.save_history = AsyncMock()
        orchestrator = DeploymentOrchestrator(
            deployment_spec=deployment_spec,
            deployment_id="test-deployment",
            session=session,
            context=context,
        )
        result_tags = await orchestrator._setup_tags()
        assert result_tags.keys() == {
            "updated_existing_tag",
            "not_updated_tag",
            "new_tag",
        }
        assert len(orchestrator.errors) == 1
        error = orchestrator.errors[0]
        assert error.code == ErrorCode.TAG_NOT_FOUND
        assert "Tags used by nodes but not defined: missing_tag" in error.message

    @pytest.mark.asyncio
    async def test_setup_tags_valid(self, session, current_user):
        """
        Test _setup_tags with all valid tags
        """
        valid_deployment_spec = DeploymentSpec(
            namespace="some.namespace",
            nodes=[
                SourceSpec(
                    name="simple_node",
                    display_name="Simple Node",
                    catalog="catalog",
                    schema="schema",
                    table="table1",
                    tags=["new_tag"],
                ),
            ],
            tags=[
                TagSpec(
                    name="new_tag",
                    display_name="New Tag",
                    description="A new tag",
                ),
            ],
        )
        context = MagicMock(autospec=DeploymentContext)
        context.current_user = current_user
        context.save_history = AsyncMock()
        orchestrator = DeploymentOrchestrator(
            deployment_spec=valid_deployment_spec,
            deployment_id="test-deployment",
            session=session,
            context=context,
        )
        result_tags = await orchestrator._setup_tags()
        assert result_tags.keys() == {"new_tag"}
        assert len(orchestrator.errors) == 0

    @pytest.mark.asyncio
    async def test_setup_owners_missing(self, session, current_user):
        """
        Test _setup_owners with missing owners
        """
        valid_deployment_spec = DeploymentSpec(
            namespace="some.namespace",
            nodes=[
                SourceSpec(
                    name="simple_node",
                    display_name="Simple Node",
                    catalog="catalog",
                    schema="schema",
                    table="table1",
                    owners=["new_owner"],
                ),
            ],
        )
        context = MagicMock(autospec=DeploymentContext)
        context.current_user = current_user
        context.save_history = AsyncMock()
        orchestrator = DeploymentOrchestrator(
            deployment_spec=valid_deployment_spec,
            deployment_id="test-deployment",
            session=session,
            context=context,
        )
        result_owners = await orchestrator._setup_owners()
        assert result_owners.keys() == {"new_owner"}
        assert len(orchestrator.errors) == 0

    @pytest.mark.asyncio
    async def test_setup_owners_valid(self, session, current_user):
        """
        Test setup owners with existing owners
        """
        session.add(
            User(
                username="new_owner",
                email="new_owner@example.com",
                oauth_provider=OAuthProvider.BASIC,
            ),
        )
        await session.commit()
        deployment_spec = DeploymentSpec(
            namespace="some.namespace",
            nodes=[
                SourceSpec(
                    name="simple_node",
                    display_name="Simple Node",
                    catalog="catalog",
                    schema="schema",
                    table="table1",
                    owners=["new_owner"],
                ),
            ],
        )
        context = MagicMock(autospec=DeploymentContext)
        context.current_user = current_user
        context.save_history = AsyncMock()
        orchestrator = DeploymentOrchestrator(
            deployment_spec=deployment_spec,
            deployment_id="test-deployment",
            session=session,
            context=context,
        )
        result_owners = await orchestrator._setup_owners()
        assert result_owners.keys() == {"new_owner"}
        assert len(orchestrator.errors) == 0

    @pytest.mark.asyncio
    async def test_setup_catalogs_returns_all_db_catalogs(self, session, current_user):
        """
        _setup_catalogs should return ALL catalogs in the DB, not only those
        explicitly referenced by the deployment spec.  Catalogs outside the spec
        are needed so the registry has full catalog context for nodes that
        indirectly depend on them (e.g. transforms referencing pre-existing sources).
        """
        # Two catalogs in the DB; only one is mentioned in the spec
        catalog_in_spec = Catalog(name="spec_catalog")
        catalog_not_in_spec = Catalog(name="other_catalog")
        session.add_all([catalog_in_spec, catalog_not_in_spec])
        await session.commit()

        deployment_spec = DeploymentSpec(
            namespace="some.namespace",
            nodes=[
                SourceSpec(
                    name="simple_node",
                    display_name="Simple Node",
                    catalog="spec_catalog",
                    schema="schema",
                    table="table1",
                ),
            ],
        )
        context = MagicMock(autospec=DeploymentContext)
        context.current_user = current_user
        context.save_history = AsyncMock()
        orchestrator = DeploymentOrchestrator(
            deployment_spec=deployment_spec,
            deployment_id="test-deployment",
            session=session,
            context=context,
        )
        result_catalogs = await orchestrator._setup_catalogs()

        # Both catalogs are returned even though only one was in the spec
        assert set(result_catalogs.keys()) >= {"spec_catalog", "other_catalog"}
        assert len(orchestrator.errors) == 0

    @pytest.mark.asyncio
    async def test_setup_catalogs_missing(self, session, current_user):
        """
        Test setup catalogs with missing catalogs
        """
        deployment_spec = DeploymentSpec(
            namespace="some.namespace",
            nodes=[
                SourceSpec(
                    name="simple_node",
                    display_name="Simple Node",
                    catalog="catalog",
                    schema="schema",
                    table="table1",
                ),
            ],
        )
        context = MagicMock(autospec=DeploymentContext)
        context.current_user = current_user
        context.save_history = AsyncMock()
        orchestrator = DeploymentOrchestrator(
            deployment_spec=deployment_spec,
            deployment_id="test-deployment",
            session=session,
            context=context,
        )
        result_catalogs = await orchestrator._setup_catalogs()
        # The missing spec catalog is not in the returned map but all other DB
        # catalogs are still returned (new behavior: always return full catalog map)
        assert "catalog" not in result_catalogs
        assert len(orchestrator.errors) == 1

    @pytest.mark.asyncio
    async def test_setup_catalogs_validates_missing_server_default_catalog(
        self,
        session,
        current_user,
    ):
        """
        _setup_catalogs should report an error when the server-configured
        default_catalog_name refers to a catalog that doesn't exist in the DB.
        """
        from unittest.mock import patch

        deployment_spec = DeploymentSpec(
            namespace="some.namespace",
            nodes=[],
        )
        context = MagicMock(autospec=DeploymentContext)
        context.current_user = current_user
        context.save_history = AsyncMock()
        orchestrator = DeploymentOrchestrator(
            deployment_spec=deployment_spec,
            deployment_id="test-deployment",
            session=session,
            context=context,
        )
        with patch(
            "datajunction_server.internal.deployment.orchestrator.get_settings",
        ) as mock_settings:
            mock_settings.return_value.seed_setup.default_catalog_name = (
                "nonexistent_catalog"
            )
            result_catalogs = await orchestrator._setup_catalogs()
        assert "nonexistent_catalog" not in result_catalogs
        assert len(orchestrator.errors) == 1
        assert "nonexistent_catalog" in orchestrator.errors[0].message

    @pytest.mark.asyncio
    async def test_setup_catalogs_validates_missing_spec_default_catalog(
        self,
        session,
        current_user,
    ):
        """
        _setup_catalogs should report an error when the deployment spec's
        default_catalog refers to a catalog that doesn't exist in the DB.
        """
        from unittest.mock import patch

        deployment_spec = DeploymentSpec(
            namespace="some.namespace",
            nodes=[],
            default_catalog="nonexistent_spec_catalog",
        )
        context = MagicMock(autospec=DeploymentContext)
        context.current_user = current_user
        context.save_history = AsyncMock()
        orchestrator = DeploymentOrchestrator(
            deployment_spec=deployment_spec,
            deployment_id="test-deployment",
            session=session,
            context=context,
        )
        with patch(
            "datajunction_server.internal.deployment.orchestrator.get_settings",
        ) as mock_settings:
            mock_settings.return_value.seed_setup.default_catalog_name = None
            result_catalogs = await orchestrator._setup_catalogs()
        assert "nonexistent_spec_catalog" not in result_catalogs
        assert len(orchestrator.errors) == 1
        assert "nonexistent_spec_catalog" in orchestrator.errors[0].message

    def test_filter_nodes_to_deploy_without_force(
        self,
        orchestrator,
        sample_deployment_spec,
    ):
        """Unchanged nodes go to to_skip when force=False (default)."""
        # Build an existing_nodes_map where every node matches the spec exactly
        existing_specs = {
            node.rendered_name: node for node in sample_deployment_spec.nodes
        }
        to_deploy, to_skip, to_delete = orchestrator.filter_nodes_to_deploy(
            existing_specs,
        )
        assert to_deploy == []
        assert len(to_skip) == len(sample_deployment_spec.nodes)
        assert to_delete == []

    def test_filter_nodes_to_deploy_with_force(
        self,
        session,
        current_user,
        sample_deployment_spec,
    ):
        """With force=True, unchanged nodes are moved to to_update instead of to_skip."""
        force_spec = DeploymentSpec(
            namespace=sample_deployment_spec.namespace,
            nodes=sample_deployment_spec.nodes,
            tags=sample_deployment_spec.tags,
            force=True,
        )
        context = MagicMock(autospec=DeploymentContext)
        context.current_user = current_user
        context.save_history = AsyncMock()
        orchestrator = DeploymentOrchestrator(
            deployment_spec=force_spec,
            deployment_id="force-deployment",
            session=session,
            context=context,
        )
        existing_specs = {
            node.rendered_name: node for node in sample_deployment_spec.nodes
        }
        to_deploy, to_skip, to_delete = orchestrator.filter_nodes_to_deploy(
            existing_specs,
        )
        # All existing nodes should be re-deployed, none skipped
        assert len(to_deploy) == len(sample_deployment_spec.nodes)
        assert to_skip == []
        assert to_delete == []


class TestOrchestrationFlow:
    """Test end-to-end orchestration flow"""

    @pytest.mark.asyncio
    async def test_execute_full_deployment_success(self, orchestrator):
        """Test successful end-to-end deployment execution"""

        with (
            patch.object(orchestrator, "_setup_deployment_resources") as mock_setup,
            patch.object(
                orchestrator,
                "_validate_deployment_resources",
            ) as mock_validate_resources,
            patch.object(orchestrator, "_create_deployment_plan") as mock_create_plan,
            patch.object(
                orchestrator,
                "_execute_deployment_plan",
                return_value=[],
            ) as mock_execute_plan,
        ):
            # Configure deployment plan
            mock_plan = Mock(spec=DeploymentPlan)
            mock_plan.is_empty.return_value = False
            mock_plan.to_deploy = []
            mock_plan.to_delete = []
            mock_create_plan.return_value = (mock_plan, [])

            # Execute
            await orchestrator.execute()

            # Verify all phases were called in order
            mock_setup.assert_called_once()
            mock_validate_resources.assert_called_once()
            mock_create_plan.assert_called_once()
            mock_execute_plan.assert_called_once_with(mock_plan)

    @pytest.mark.asyncio
    async def test_execute_empty_deployment(self, orchestrator):
        """Test execution with empty deployment (no changes)"""

        with (
            patch.object(orchestrator, "_setup_deployment_resources"),
            patch.object(orchestrator, "_validate_deployment_resources"),
            patch.object(orchestrator, "_create_deployment_plan") as mock_create_plan,
            patch.object(orchestrator, "_handle_no_changes") as mock_handle_no_changes,
        ):
            # Configure empty deployment plan
            mock_plan = Mock(spec=DeploymentPlan)
            mock_plan.is_empty.return_value = True
            mock_plan.to_deploy = []
            mock_plan.to_delete = []
            mock_create_plan.return_value = (mock_plan, [])

            mock_handle_no_changes.return_value = []

            # Execute
            await orchestrator.execute()

            # Should handle no changes
            mock_handle_no_changes.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_with_validation_errors(self, orchestrator):
        """Test execution when validation errors exist"""

        with (
            patch.object(orchestrator, "_setup_deployment_resources"),
            patch.object(
                orchestrator,
                "_validate_deployment_resources",
            ) as mock_validate,
        ):
            # Configure validation to raise error
            mock_validate.side_effect = DJInvalidDeploymentConfig(
                message="Invalid config",
                errors=[],
                warnings=[],
            )

            # Should raise DJInvalidDeploymentConfig
            with pytest.raises(DJInvalidDeploymentConfig):
                await orchestrator.execute()


class TestColumnChanged:
    """Test suite for column_changed function"""

    def test_column_unchanged_identical_fields(self):
        """Test when column has no changes"""
        from datajunction_server.database.column import Column

        # Test mismatch description
        existing_col = Column(
            name="test_col",
            display_name="Test Column",
            description="A test column",
            type="int",
        )
        desired_spec = ColumnSpec(
            name="test_col",
            type="int",
            display_name="Test Column",
            description="A test column11",
            attributes=["dimension"],
        )
        result = column_changed(desired_spec, existing_col)
        assert result is True

        # Test mismatch partition
        existing_col = Column(
            name="test_col",
            display_name="Test Column",
            description="A test column",
            type="int",
        )
        desired_spec = ColumnSpec(
            name="test_col",
            type="int",
            display_name="Test Column",
            description="A test column",
            partition=PartitionSpec(type=PartitionType.TEMPORAL),
        )
        result = column_changed(desired_spec, existing_col)
        assert result is True

        # Test mismatch attributes
        existing_col = Column(
            name="test_col",
            display_name="Test Column",
            description="A test column",
            type="int",
        )
        desired_spec = ColumnSpec(
            name="test_col",
            type="int",
            display_name="Test Column",
            description="A test column",
            attributes=["dimension"],
        )
        result = column_changed(desired_spec, existing_col)
        assert result is True

        # Test match
        existing_col = Column(
            name="test_col",
            display_name="Test Column",
            description="A test column",
            type="int",
        )
        desired_spec = ColumnSpec(
            name="test_col",
            display_name="Test Column",
            description="A test column",
            type="int",
        )
        result = column_changed(desired_spec, existing_col)
        assert result is False


class TestCubeDeployment:
    """Test suite for cube deployment functionality"""

    @pytest.fixture
    def sample_cube_specs(self):
        """Sample cube specifications for testing"""
        return [
            CubeSpec(
                name="test_cube_1",
                node_type="cube",
                metrics=["metric1", "metric2"],
                dimensions=["dim1.attr1", "dim2.attr2"],
                namespace="test",
            ),
            CubeSpec(
                name="test_cube_2",
                node_type="cube",
                metrics=["metric2", "metric3"],
                dimensions=["dim1.attr1", "dim3.attr3"],
                namespace="test",
            ),
        ]

    @pytest.fixture
    def cube_deployment_plan(self, sample_cube_specs):
        """Deployment plan with cube specs"""
        return DeploymentPlan(
            to_deploy=sample_cube_specs,
            to_delete=[],
            to_skip=[],
            existing_specs={},
            node_graph={},
            external_deps=set(),
        )

    @pytest.mark.asyncio
    async def test_deploy_cubes_filters_non_cubes(self, orchestrator):
        """Test that _deploy_cubes only processes CubeSpec nodes"""
        plan = DeploymentPlan(
            to_deploy=[
                MetricSpec(name="metric1", node_type="metric", query="SELECT 1"),
                # No cube specs
            ],
            to_delete=[],
            to_skip=[],
            existing_specs={},
            node_graph={},
            external_deps=set(),
        )

        result = await orchestrator._deploy_cubes(plan)

        assert result == []

    @pytest.mark.asyncio
    async def test_bulk_validate_cubes_all_valid(
        self,
        orchestrator,
        sample_cube_specs,
    ):
        """Test bulk validation with all dependencies available"""
        result = await orchestrator._bulk_validate_cubes(sample_cube_specs)

        # Verify all cubes are processed (valid or invalid depending on implementation)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_bulk_validate_cubes_missing_dependencies(
        self,
        orchestrator,
        sample_cube_specs,
    ):
        """Test bulk validation with missing dependencies"""
        result = await orchestrator._bulk_validate_cubes(sample_cube_specs)

        # Should still return results for all cubes (some will be invalid)
        assert len(result) == 2

        # At least one cube should be invalid due to missing deps
        invalid_results = [r for r in result if r.status == "invalid"]
        assert len(invalid_results) > 0

        # Invalid cubes should have error messages about missing dependencies
        for invalid_result in invalid_results:
            assert len(invalid_result.errors) > 0
            error_messages = " ".join(err.message for err in invalid_result.errors)
            assert "One or more metrics not found for cube" in error_messages
            assert "One or more dimensions not found for cube" in error_messages

    @pytest.mark.asyncio
    async def test_create_cubes_from_validation_invalid_cubes(self, orchestrator):
        """Test creating cubes when validation results contain invalid cubes.
        Invalid cubes now go through the normal path with _cube_validation_data."""
        invalid_results = [
            NodeValidationResult(
                spec=CubeSpec(
                    name="test.invalid_cube",
                    node_type="cube",
                    metrics=[],
                    dimensions=[],
                ),
                status="invalid",
                inferred_columns=[],
                errors=[DJError(code=ErrorCode.INVALID_CUBE, message="Invalid cube")],
                dependencies=[],
                _cube_validation_data=CubeValidationData(
                    metric_columns=[],
                    metric_nodes=[],
                    dimension_nodes=[],
                    dimension_columns=[],
                    catalog=None,
                ),
            ),
        ]

        # Mock _create_cube_node_revision_from_validation_data since it
        # needs full DB setup
        mock_revision = Mock()
        mock_revision.version = "v1.0"
        orchestrator._create_cube_node_revision_from_validation_data = AsyncMock(
            return_value=mock_revision,
        )
        orchestrator._generate_changelog = AsyncMock(return_value=([], []))

        with patch(
            "datajunction_server.internal.deployment.orchestrator.get_node_namespace",
            new_callable=AsyncMock,
        ):
            (
                nodes,
                revisions,
                results,
            ) = await orchestrator._create_cubes_from_validation(
                invalid_results,
            )

        assert len(nodes) == 1
        assert len(revisions) == 1
        assert len(results) == 1
        assert results[0].status == "invalid"

    @pytest.mark.asyncio
    async def test_cube_column_partition_applied_from_spec(
        self,
        session,
        current_user,
    ):
        """Test that partitions from cube spec columns are applied during deployment"""
        # Create a cube spec with column partition
        cube_spec = CubeSpec(
            name="test.sales_cube",
            node_type="cube",
            metrics=["test.revenue"],
            dimensions=["test.date.dateint"],
            namespace="test",
            columns=[
                ColumnSpec(
                    name="test.date.dateint",
                    display_name="Date",
                    attributes=["primary_key"],
                    partition=PartitionSpec(
                        type=PartitionType.TEMPORAL,
                        granularity=Granularity.DAY,
                        format="yyyyMMdd",
                    ),
                ),
            ],
        )

        # Create actual catalog in the database
        catalog = Catalog(name="test_catalog")
        session.add(catalog)

        # Create actual date dimension node
        date_node = Node(
            name="test.date",
            type="dimension",
            current_version="v1.0",
            created_by_id=current_user.id,
        )
        session.add(date_node)

        date_revision = NodeRevision(
            name="test.date",
            display_name="Date",
            type="dimension",
            node=date_node,
            version="v1.0",
            query="SELECT dateint FROM dates",
            created_by_id=current_user.id,
        )
        session.add(date_revision)

        # Create actual revenue metric node
        revenue_node = Node(
            name="test.revenue",
            type="metric",
            current_version="v1.0",
            created_by_id=current_user.id,
        )
        session.add(revenue_node)

        revenue_revision = NodeRevision(
            name="test.revenue",
            display_name="Revenue",
            type="metric",
            node=revenue_node,
            version="v1.0",
            query="SELECT SUM(amount) FROM sales",
            created_by_id=current_user.id,
        )
        session.add(revenue_revision)

        await session.commit()

        # Create columns on the node revisions
        date_column = Column(
            name="dateint",
            type="int",
            node_revision_id=date_revision.id,
            node_revision=date_revision,
            attributes=[],  # Empty attributes list
        )
        session.add(date_column)

        revenue_column = Column(
            name="revenue",
            type="bigint",
            node_revision_id=revenue_revision.id,
            node_revision=revenue_revision,
            attributes=[],  # Empty attributes list
        )
        session.add(revenue_column)
        await session.commit()

        # Refresh to load relationships
        await session.refresh(date_column, ["node_revision", "attributes"])
        await session.refresh(revenue_column, ["node_revision", "attributes"])

        # Create validation data
        validation_data = CubeValidationData(
            metric_columns=[revenue_column],
            dimension_columns=[date_column],
            metric_nodes=[revenue_node],
            dimension_nodes=[date_node],
            catalog=catalog,
        )

        # Create new cube node
        new_node = Node(
            name="test.sales_cube",
            type="cube",
            current_version="v1.0",
            created_by_id=current_user.id,
        )
        session.add(new_node)
        await session.commit()

        # Create orchestrator
        context = DeploymentContext(
            current_user=current_user,
            request=Mock(),
            query_service_client=Mock(),
            background_tasks=Mock(),
            cache=Mock(),
        )
        orchestrator = DeploymentOrchestrator(
            deployment_id="test-deployment",
            deployment_spec=DeploymentSpec(namespace="test", nodes=[]),
            session=session,
            context=context,
        )

        # Call the method that creates the node revision
        node_revision = (
            await orchestrator._create_cube_node_revision_from_validation_data(
                cube_spec=cube_spec,
                validation_data=validation_data,
                new_node=new_node,
            )
        )

        session.add(node_revision)
        await session.commit()

        # Verify partition was applied
        assert len(node_revision.columns) == 2  # revenue + date
        date_cube_column = [
            col for col in node_revision.columns if "dateint" in col.name
        ][0]

        # Check that partition was added to the session
        # Note: We need to refresh to get the partition relationship
        await session.refresh(date_cube_column, ["partition"])
        assert date_cube_column.partition is not None
        assert date_cube_column.partition.type_ == PartitionType.TEMPORAL
        assert date_cube_column.partition.granularity == Granularity.DAY
        assert date_cube_column.partition.format == "yyyyMMdd"

    @pytest.mark.asyncio
    async def test_multiple_cubes_with_partition_no_autoflush_error(
        self,
        session,
        current_user,
    ):
        """
        Regression test: deploying multiple cubes where one has a partitioned column
        must not raise an IntegrityError due to a premature autoflush.

        Previously, self.session.add(partition) inside
        _create_cube_node_revision_from_validation_data caused SQLAlchemy to silently
        add the associated node_column to the session. After the no_autoflush block
        exited, those columns were stranded in the session with node_revision_id=null.
        The next DB query (triggered by processing the second cube) would autoflush
        them, hitting the NOT NULL constraint on node_revision_id.
        """
        catalog = Catalog(name="test_catalog")
        session.add(catalog)

        date_node = Node(
            name="test.date",
            type="dimension",
            current_version="v1.0",
            created_by_id=current_user.id,
        )
        revenue_node = Node(
            name="test.revenue",
            type="metric",
            current_version="v1.0",
            created_by_id=current_user.id,
        )
        session.add_all([date_node, revenue_node])

        date_revision = NodeRevision(
            name="test.date",
            display_name="Date",
            type="dimension",
            node=date_node,
            version="v1.0",
            query="SELECT dateint FROM dates",
            created_by_id=current_user.id,
        )
        revenue_revision = NodeRevision(
            name="test.revenue",
            display_name="Revenue",
            type="metric",
            node=revenue_node,
            version="v1.0",
            query="SELECT SUM(amount) FROM sales",
            created_by_id=current_user.id,
        )
        session.add_all([date_revision, revenue_revision])
        await session.commit()

        date_column = Column(
            name="dateint",
            type="int",
            node_revision_id=date_revision.id,
            node_revision=date_revision,
            attributes=[],
        )
        revenue_column = Column(
            name="revenue",
            type="bigint",
            node_revision_id=revenue_revision.id,
            node_revision=revenue_revision,
            attributes=[],
        )
        session.add_all([date_column, revenue_column])
        await session.commit()

        await session.refresh(date_column, ["node_revision", "attributes"])
        await session.refresh(revenue_column, ["node_revision", "attributes"])

        validation_data = CubeValidationData(
            metric_columns=[revenue_column],
            dimension_columns=[date_column],
            metric_nodes=[revenue_node],
            dimension_nodes=[date_node],
            catalog=catalog,
        )

        # Two cube nodes — the second cube forces a DB query after the first
        # cube's no_autoflush block exits, which triggers the bug.
        cube_node_1 = Node(
            name="test.cube_1",
            type="cube",
            current_version="v1.0",
            created_by_id=current_user.id,
        )
        cube_node_2 = Node(
            name="test.cube_2",
            type="cube",
            current_version="v1.0",
            created_by_id=current_user.id,
        )
        session.add_all([cube_node_1, cube_node_2])
        await session.commit()

        context = DeploymentContext(
            current_user=current_user,
            request=Mock(),
            query_service_client=Mock(),
            background_tasks=Mock(),
            cache=Mock(),
        )
        orchestrator = DeploymentOrchestrator(
            deployment_id="test-deployment",
            deployment_spec=DeploymentSpec(namespace="test", nodes=[]),
            session=session,
            context=context,
        )

        cube_spec_with_partition = CubeSpec(
            name="test.cube_1",
            node_type="cube",
            metrics=["test.revenue"],
            dimensions=["test.date.dateint"],
            namespace="test",
            columns=[
                ColumnSpec(
                    name="test.date.dateint",
                    partition=PartitionSpec(
                        type=PartitionType.TEMPORAL,
                        granularity=Granularity.DAY,
                        format="yyyyMMdd",
                    ),
                ),
            ],
        )
        cube_spec_plain = CubeSpec(
            name="test.cube_2",
            node_type="cube",
            metrics=["test.revenue"],
            dimensions=["test.date.dateint"],
            namespace="test",
        )

        # Creating both revisions back-to-back reproduces the autoflush bug:
        # after the first no_autoflush block exits, the second cube's session.refresh
        # call triggers a flush that would expose the orphaned column.
        revision_1 = await orchestrator._create_cube_node_revision_from_validation_data(
            cube_spec=cube_spec_with_partition,
            validation_data=validation_data,
            new_node=cube_node_1,
        )
        revision_2 = await orchestrator._create_cube_node_revision_from_validation_data(
            cube_spec=cube_spec_plain,
            validation_data=validation_data,
            new_node=cube_node_2,
        )

        session.add_all([revision_1, revision_2])
        await session.commit()  # Would raise IntegrityError before the fix

        await session.refresh(revision_1, ["columns"])
        date_col = next(c for c in revision_1.columns if "dateint" in c.name)
        await session.refresh(date_col, ["partition"])
        assert date_col.partition is not None
        assert date_col.partition.type_ == PartitionType.TEMPORAL

        await session.refresh(revision_2, ["columns"])
        assert len(revision_2.columns) == 2


@pytest.mark.asyncio
async def test_auto_register_sources_disabled(
    session,
    current_user: User,
):
    """Test auto-registration when disabled."""
    context = DeploymentContext(
        current_user=current_user,
        request=Mock(),
        query_service_client=Mock(),
        background_tasks=Mock(),
        cache=Mock(),
    )

    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=False,  # Disabled
    )

    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )

    # Should return empty list when disabled
    result = await orchestrator._auto_register_sources(["catalog.schema.table"])
    assert result == []


@pytest.mark.asyncio
async def test_auto_register_sources_no_matching_pattern(
    session,
    current_user: User,
):
    """Test auto-registration when missing nodes don't match catalog.schema.table pattern."""
    context = DeploymentContext(
        current_user=current_user,
        request=Mock(),
        query_service_client=Mock(),
        background_tasks=Mock(),
        cache=Mock(),
    )

    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )

    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )

    # Missing nodes that don't match catalog.schema.table pattern (only 2 parts)
    result = await orchestrator._auto_register_sources(["foo.bar"])
    assert result == []


@pytest.mark.asyncio
async def test_auto_register_sources_no_query_service_client(
    session,
    current_user: User,
):
    """Test auto-registration when query service client is not available."""
    context = DeploymentContext(
        current_user=current_user,
        request=Mock(),
        query_service_client=None,  # No query service client
        background_tasks=Mock(),
        cache=Mock(),
    )

    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )

    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )

    # Should return empty list and log warning
    result = await orchestrator._auto_register_sources(["catalog.schema.table"])
    assert result == []


@pytest.mark.asyncio
async def test_auto_register_sources_with_empty_columns(
    session,
    current_user: User,
):
    """Test auto-registration when tables have no columns."""
    # Create catalog
    catalog = Catalog(name="test_catalog")
    session.add(catalog)
    await session.commit()

    # Create mock column
    mock_col = Mock()
    mock_col.name = "col1"

    mock_query_client = Mock()
    # Mock batch column fetch returning empty columns for one table
    mock_query_client.get_columns_for_tables_batch.return_value = {
        ("test_catalog", "schema1", "table1"): [],  # Empty columns
        ("test_catalog", "schema1", "table2"): [mock_col],
    }

    context = DeploymentContext(
        current_user=current_user,
        request=Mock(headers={}),
        query_service_client=mock_query_client,
        background_tasks=Mock(),
        cache=Mock(),
    )

    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )

    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )

    # Should only register table2 (table1 has no columns)
    result = await orchestrator._auto_register_sources(
        [
            "test_catalog.schema1.table1",
            "test_catalog.schema1.table2",
        ],
    )

    # Only table2 should be registered
    assert len(result) == 1
    assert result[0].name == "test_catalog.schema1.table2"


@pytest.mark.asyncio
async def test_auto_register_sources_batch_introspection_failure(
    session,
    current_user: User,
):
    """Test auto-registration when batch introspection fails."""
    # Create catalog
    catalog = Catalog(name="test_catalog")
    session.add(catalog)
    await session.commit()

    mock_query_client = Mock()
    # Mock batch column fetch to raise exception
    mock_query_client.get_columns_for_tables_batch.side_effect = Exception(
        "Connection timeout",
    )

    context = DeploymentContext(
        current_user=current_user,
        request=Mock(headers={}),
        query_service_client=mock_query_client,
        background_tasks=Mock(),
        cache=Mock(),
    )

    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )

    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )

    # Should return empty list and add errors
    result = await orchestrator._auto_register_sources(
        [
            "test_catalog.schema1.table1",
        ],
    )

    assert result == []
    # Should have error for failed table
    assert len(orchestrator.errors) > 0
    assert "Failed to auto-register source" in str(orchestrator.errors[0])


@pytest.mark.asyncio
async def test_auto_register_sources_invalid_node_name(
    session,
    current_user: User,
):
    """Unknown catalogs are silently skipped — no error, just returns empty."""
    context = DeploymentContext(
        current_user=current_user,
        request=Mock(headers={}),
        query_service_client=Mock(),
        background_tasks=Mock(),
        cache=Mock(),
    )

    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )

    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )

    result = await orchestrator._auto_register_sources(["invalid_catalog.schema.table"])

    assert result == []
    assert len(orchestrator.errors) == 0


@pytest.mark.asyncio
async def test_auto_register_sources_success(
    session,
    current_user: User,
):
    """Test successful auto-registration of sources."""

    # Create catalog
    catalog = Catalog(name="test_catalog")
    session.add(catalog)
    await session.commit()

    mock_query_client = Mock()
    # Mock successful batch column fetch
    mock_query_client.get_columns_for_tables_batch.return_value = {
        ("test_catalog", "schema1", "table1"): [
            Column(name="id", type="int", order=0),
            Column(name="name", type="str", order=1),
        ],
        ("test_catalog", "schema1", "table2"): [
            Column(name="value", type="bigint", order=0),
        ],
    }

    context = DeploymentContext(
        current_user=current_user,
        request=Mock(headers={}),
        query_service_client=mock_query_client,
        background_tasks=Mock(),
        cache=Mock(),
    )

    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )

    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )

    # Should successfully register both sources
    result = await orchestrator._auto_register_sources(
        [
            "test_catalog.schema1.table1",
            "test_catalog.schema1.table2",
        ],
    )

    assert len(result) == 2
    assert result[0].name == "test_catalog.schema1.table1"
    assert result[1].name == "test_catalog.schema1.table2"
    assert result[0].columns is not None and len(result[0].columns) == 2
    assert result[1].columns is not None and len(result[1].columns) == 1


@pytest.mark.asyncio
async def test_auto_register_sources_empty_list(
    session,
    current_user: User,
):
    """Lines 266-267: return early when missing_nodes is empty but auto-register is enabled."""
    context = DeploymentContext(
        current_user=current_user,
        request=Mock(),
        query_service_client=Mock(),
        background_tasks=Mock(),
        cache=Mock(),
    )
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )
    result = await orchestrator._auto_register_sources([])
    assert result == []


@pytest.mark.asyncio
async def test_auto_register_sources_multiple_catalogs_error(
    session,
    current_user: User,
):
    """Test that auto-registration fails when missing nodes span multiple catalogs,
    since a single SQL query cannot reference tables from different engines."""
    catalog_a = Catalog(name="iceberg_catalog")
    catalog_b = Catalog(name="druid_catalog")
    session.add_all([catalog_a, catalog_b])
    await session.commit()

    context = DeploymentContext(
        current_user=current_user,
        request=Mock(headers={}),
        query_service_client=Mock(),
        background_tasks=Mock(),
        cache=Mock(),
    )
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )

    result = await orchestrator._auto_register_sources(
        [
            "iceberg_catalog.schema.table_a",
            "druid_catalog.schema.table_b",
        ],
    )

    assert result == []
    assert len(orchestrator.errors) == 1
    assert "iceberg_catalog" in orchestrator.errors[0].message
    assert "druid_catalog" in orchestrator.errors[0].message
    assert "multiple catalogs" in orchestrator.errors[0].message


@pytest.mark.asyncio
async def test_auto_register_sources_strips_source_prefix(
    session,
    current_user: User,
):
    """Lines 280-282: the configured source namespace prefix is stripped before matching.
    Lines 313-314: no nodes remain after catalog filter when catalog is unknown."""
    context = DeploymentContext(
        current_user=current_user,
        request=Mock(headers={}),
        query_service_client=Mock(),
        background_tasks=Mock(),
        cache=Mock(),
    )
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )
    # "source.catalog.schema.table" has the default source prefix → stripped to
    # "catalog.schema.table". But "catalog" doesn't exist in DB → filtered out → [].
    result = await orchestrator._auto_register_sources(["source.catalog.schema.table"])
    assert result == []
    assert len(orchestrator.errors) == 0


@pytest.mark.asyncio
async def test_auto_register_sources_unknown_catalog(
    session,
    current_user: User,
):
    """Unknown catalogs are silently skipped — no error, just returns empty."""
    context = DeploymentContext(
        current_user=current_user,
        request=Mock(headers={}),
        query_service_client=Mock(),
        background_tasks=Mock(),
        cache=Mock(),
    )
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )
    result = await orchestrator._auto_register_sources(["unknown_catalog.schema.table"])
    assert result == []
    assert len(orchestrator.errors) == 0


@pytest.mark.asyncio
async def test_auto_register_sources_preserves_source_prefix_in_name(
    session,
    current_user: User,
):
    """When a table is referenced as source.catalog.schema.table, the registered
    source node name should preserve the full 'source.' prefix, not strip it."""
    catalog = Catalog(name="test_catalog")
    session.add(catalog)
    await session.commit()

    mock_col = Mock()
    mock_col.name = "id"
    mock_col.type = "int"

    mock_query_client = Mock()
    mock_query_client.get_columns_for_tables_batch.return_value = {
        ("test_catalog", "schema1", "table1"): [mock_col],
    }

    context = DeploymentContext(
        current_user=current_user,
        request=Mock(headers={}),
        query_service_client=mock_query_client,
        background_tasks=Mock(),
        cache=Mock(),
    )
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )

    # Reference uses the "source." prefix — the registered node name should keep it
    result = await orchestrator._auto_register_sources(
        ["source.test_catalog.schema1.table1"],
    )

    assert len(result) == 1
    assert result[0].name == "source.test_catalog.schema1.table1"
    assert result[0].catalog == "test_catalog"
    assert result[0].schema_ == "schema1"
    assert result[0].table == "table1"


@pytest.mark.asyncio
async def test_find_namespaces_to_create_root_level_node(session, current_user: User):
    """Line 444: a node whose rendered_name has no separator returns True immediately."""
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[
            SourceSpec(
                name="simple_node",
                catalog="catalog",
                schema="schema",
                table="table",
            ),
        ],
    )
    # Force namespace to None so rendered_name = "simple_node" (no separator)
    deployment_spec.nodes[0].namespace = None

    context = MagicMock(autospec=DeploymentContext)
    context.current_user = current_user
    context.save_history = AsyncMock()
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )
    result = await orchestrator._find_namespaces_to_create()
    # The deployment namespace is always present; root-level node adds no extra namespace
    assert "test" in result


@pytest.mark.asyncio
async def test_check_external_deps_duplicate_source_forms(session, current_user: User):
    """Line 1807->1804: when both 'source.X' and 'X' are in deps_not_in_deployment,
    the normalized form is already in names_to_search so the append is skipped."""
    catalog = Catalog(name="ext")
    session.add(catalog)
    await session.commit()

    ext_node = Node(
        name="ext.schema.tbl",
        type="source",
        current_version="v1.0",
        created_by_id=current_user.id,
    )
    session.add(ext_node)
    ext_rev = NodeRevision(
        name="ext.schema.tbl",
        type="source",
        node=ext_node,
        version="v1.0",
        created_by_id=current_user.id,
        schema_="schema",
        table="tbl",
        catalog_id=catalog.id,
    )
    session.add(ext_rev)
    await session.commit()

    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )
    context = MagicMock(autospec=DeploymentContext)
    context.current_user = current_user
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )

    # Both forms are external deps; "ext.schema.tbl" is already in names_to_search
    # when we encounter "source.ext.schema.tbl", so the append at line 1807 is skipped.
    node_graph = {"test.transform": ["source.ext.schema.tbl", "ext.schema.tbl"]}

    external_deps, auto_registered, missing = await orchestrator.check_external_deps(
        node_graph,
    )
    assert auto_registered == []
    assert missing == []
    assert len(orchestrator.errors) == 0


@pytest.mark.asyncio
async def test_check_external_deps_auto_register_fails_with_errors(
    session,
    current_user: User,
):
    """Line 1853: when _auto_register_sources populates self.errors,
    DJInvalidDeploymentConfig is raised with 'Failed to auto-register sources'."""
    catalog = Catalog(name="test_catalog")
    session.add(catalog)
    await session.commit()

    mock_client = Mock()
    mock_client.get_columns_for_tables_batch.side_effect = Exception(
        "Connection timeout",
    )

    context = DeploymentContext(
        current_user=current_user,
        request=Mock(headers={}),
        query_service_client=mock_client,
        background_tasks=Mock(),
        cache=Mock(),
    )
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
        auto_register_sources=True,
    )
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )
    node_graph = {"test.transform": ["test_catalog.ext_schema.ext_table"]}

    with pytest.raises(DJInvalidDeploymentConfig) as exc_info:
        await orchestrator.check_external_deps(node_graph)

    assert "Failed to auto-register sources" in exc_info.value.message


@pytest.mark.asyncio
async def test_create_deployment_plan_all_auto_sources_already_exist(
    session,
    current_user: User,
):
    """Lines 737-747: when all auto-registered sources already exist in DB,
    sources_to_create is empty (else branch at 737) and existing sources are
    added to existing_specs (lines 743-747)."""
    session.add(NodeNamespace(namespace="test"))
    catalog = Catalog(name="ext_cat")
    session.add(catalog)
    await session.commit()  # flush catalog first so its ID is available

    existing_source = Node(
        name="ext_cat.s.t",
        type="source",
        current_version="v1.0",
        created_by_id=current_user.id,
    )
    session.add(existing_source)
    existing_rev = NodeRevision(
        name="ext_cat.s.t",
        type="source",
        node=existing_source,
        version="v1.0",
        created_by_id=current_user.id,
        schema_="s",
        table="t",
        catalog_id=catalog.id,
    )
    session.add(existing_rev)
    await session.commit()

    # Include a transform so to_deploy is non-empty, triggering the check_external_deps path
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[TransformSpec(name="my_transform", query="SELECT 1")],
        auto_register_sources=True,
    )
    context = MagicMock(autospec=DeploymentContext)
    context.current_user = current_user
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        deployment_spec=deployment_spec,
        session=session,
        context=context,
    )

    auto_source = SourceSpec(
        name="ext_cat.s.t",
        catalog="ext_cat",
        schema="s",
        table="t",
        columns=[],
    )
    auto_source.namespace = None  # rendered_name = "ext_cat.s.t" (matches the DB node)

    # First call: return the auto source; second call (re-check after rebuild): return nothing
    with patch.object(
        orchestrator,
        "check_external_deps",
        side_effect=[(set(), [auto_source], []), (set(), [], [])],
    ):
        plan, _ = await orchestrator._create_deployment_plan()

    # The existing source was found in DB and added to existing_specs (lines 743-747)
    assert "ext_cat.s.t" in plan.existing_specs


@pytest.mark.asyncio
async def test_validate_single_cube_with_invalid_metric(
    orchestrator,
):
    """When a cube references a metric that is in the registry but INVALID,
    _validate_single_cube returns INVALID (covers line 1738)."""
    invalid_metric = MagicMock()
    invalid_metric.name = "test.broken_metric"
    invalid_metric.current.status = NodeStatus.INVALID

    cube_spec = CubeSpec(
        name="test_cube_bad",
        namespace="test",
        metrics=["broken_metric"],
        dimensions=[],
    )

    from datajunction_server.internal.deployment.dimension_reachability import (
        DimensionReachability,
    )

    result = orchestrator._validate_single_cube(
        cube_spec=cube_spec,
        metric_nodes_map={"broken_metric": invalid_metric},
        missing_metrics=set(),
        missing_dimensions=set(),
        dimension_mapping={},
        reachability=DimensionReachability({}),
        metric_to_parents={},
    )

    assert result.status == NodeStatus.INVALID
    assert any("INVALID" in err.message for err in result.errors)
    # Validation data should still be built so the cube revision preserves
    # its metrics/dimensions definition even when INVALID.
    assert result._cube_validation_data is not None
    # The invalid metric should still appear in metric_nodes so the cube
    # knows which metrics it references (even if they can't be resolved).
    assert invalid_metric in result._cube_validation_data.metric_nodes
    # No valid metric columns could be extracted (all metrics were INVALID)
    assert result._cube_validation_data.metric_columns == []


@pytest.mark.asyncio
async def test_deploy_reference_link_on_invalid_node(
    session,
    current_user,
    mock_deployment_context,
):
    """When a node is INVALID and the link spec is a REFERENCE (not JOIN),
    _deploy_link_spec returns a FAILED result (covers line 1381)."""
    invalid_node = MagicMock()
    invalid_node.name = "test.invalid_transform"
    invalid_node.current.status = NodeStatus.INVALID
    invalid_node.current.columns = []

    dimension_node = MagicMock()
    dimension_node.name = "test.some_dim"

    ref_link = DimensionReferenceLinkSpec(
        dimension="test.some_dim.id",
        node_column="fk_id",
    )
    ref_link.namespace = "test"

    transform_spec = TransformSpec(
        name="invalid_transform",
        namespace="test",
        query="SELECT bad_col FROM nonexistent",
    )

    orch = DeploymentOrchestrator(
        deployment_spec=DeploymentSpec(namespace="test", nodes=[]),
        deployment_id="ref-link-test",
        session=session,
        context=mock_deployment_context,
        dry_run=True,
    )
    orch.registry.nodes["test.invalid_transform"] = invalid_node
    orch.registry.nodes["test.some_dim"] = dimension_node

    result = await orch._process_node_dimension_link(
        node_spec=transform_spec,
        link_spec=ref_link,
    )

    assert result.status == DeploymentResult.Status.FAILED
    assert "INVALID" in result.message


@pytest.mark.asyncio
async def test_deploy_delete_node_calls_add_history(
    session,
    current_user,
    mock_deployment_context,
):
    """_deploy_delete_node with a real existing node calls hard_delete_node which
    invokes the nested add_history callback (covers line 2196)."""
    orch = DeploymentOrchestrator(
        deployment_spec=DeploymentSpec(namespace="default", nodes=[]),
        deployment_id="delete-test",
        session=session,
        context=mock_deployment_context,
        dry_run=False,
    )

    # "default.hard_hat" is present in the pre-loaded roads example DB
    result = await orch._deploy_delete_node("default.hard_hat")

    # The node was found and deleted successfully
    assert result.status == DeploymentResult.Status.SUCCESS
    assert result.operation == DeploymentResult.Operation.DELETE


class TestGenerateChangelog:
    """Unit tests for DeploymentOrchestrator._generate_changelog."""

    @pytest.mark.asyncio
    async def test_no_changed_fields_with_dimension_links(
        self,
        session,
        mock_deployment_context,
    ):
        """When an existing node has the same spec but non-empty dimension_links,
        _generate_changelog covers all three false branches:
          3012->3017  (col_change_notes is empty)
          3017->3033  (changed_fields is empty)
          3038        (dimension_links non-empty → appends 'Updated dimension_links')
        """
        dim_link = DimensionJoinLinkSpec(dimension_node="default.some_dim")
        transform_spec = TransformSpec(
            name="test_node",
            namespace="default",
            query="SELECT id FROM default.source_table",
            dimension_links=[dim_link],
        )
        # existing_spec is identical — diff() will return []
        existing_spec = TransformSpec(
            name="test_node",
            namespace="default",
            query="SELECT id FROM default.source_table",
            dimension_links=[dim_link],
        )

        # Mock the existing Node object
        mock_existing = MagicMock()
        mock_existing.current.columns = []
        mock_existing.to_spec = AsyncMock(return_value=existing_spec)

        orchestrator = DeploymentOrchestrator(
            deployment_spec=DeploymentSpec(namespace="default", nodes=[]),
            deployment_id="changelog-test",
            session=session,
            context=mock_deployment_context,
            dry_run=True,
        )
        orchestrator.registry.nodes["default.test_node"] = mock_existing

        result = NodeValidationResult(
            spec=transform_spec,
            status=None,
            inferred_columns=[],
            errors=[],
            dependencies=[],
        )

        changelog, changed_fields = await orchestrator._generate_changelog(result)

        assert changed_fields == []
        assert changelog == ["└─ Updated dimension_links"]


@pytest.mark.asyncio
async def test_execute_deployment_plan_wet_run_commits(
    session,
    current_user: User,
):
    """When dry_run=False and the plan is empty (nothing to deploy/delete),
    _execute_deployment_plan exits the SAVEPOINT cleanly and commits the
    outer transaction (line 1201 — the wet-run commit path).
    """
    context = MagicMock(autospec=DeploymentContext)
    context.current_user = current_user
    context.save_history = AsyncMock()

    orchestrator = DeploymentOrchestrator(
        deployment_spec=DeploymentSpec(namespace="test", nodes=[]),
        deployment_id="wet-run-test",
        session=session,
        context=context,
        dry_run=False,  # Wet-run mode — line 1193 branch is False
    )

    plan = DeploymentPlan(
        to_deploy=[],
        to_skip=[],
        to_delete=[],
        existing_specs={},
        node_graph={},
        external_deps=set(),
    )

    # Should succeed without raising — the SAVEPOINT is committed and then
    # session.commit() at line 1201 runs (wet-run path).
    await orchestrator._execute_deployment_plan(plan)


@pytest.mark.asyncio
async def test_update_deployment_status_dry_run_noop(
    session,
    current_user: User,
    mock_deployment_context,
):
    """_update_deployment_status returns immediately when dry_run=True.

    Covers orchestrator.py line 298: if self.dry_run: return
    """
    orchestrator = DeploymentOrchestrator(
        deployment_spec=DeploymentSpec(namespace="test", nodes=[]),
        deployment_id="dry-run-status-test",
        session=session,
        context=mock_deployment_context,
        dry_run=True,
    )
    # Should return without calling InProcessExecutor (no error = success)
    await orchestrator._update_deployment_status()


@pytest.mark.asyncio
async def test_execute_deployment_plan_dry_run_savepoint_rollback(
    session,
    current_user: User,
    mock_deployment_context,
):
    """When dry_run=True, _execute_deployment_plan raises _DryRunRollback inside
    the SAVEPOINT, which rolls it back.

    Covers orchestrator.py lines 967-970: except _DryRunRollback: pass
    """
    orchestrator = DeploymentOrchestrator(
        deployment_spec=DeploymentSpec(namespace="test", nodes=[]),
        deployment_id="dry-run-rollback-test",
        session=session,
        context=mock_deployment_context,
        dry_run=True,
    )
    plan = DeploymentPlan(
        to_deploy=[],
        to_skip=[],
        to_delete=[],
        existing_specs={},
        node_graph={},
        external_deps=set(),
    )
    # Patch _deploy_nodes / _deploy_links / _deploy_cubes to avoid real DB work
    with (
        patch.object(
            orchestrator,
            "_deploy_nodes",
            new=AsyncMock(return_value=([], [])),
        ),
        patch.object(orchestrator, "_deploy_links", new=AsyncMock(return_value=[])),
        patch.object(orchestrator, "_deploy_cubes", new=AsyncMock(return_value=[])),
        patch.object(orchestrator, "_delete_nodes", new=AsyncMock(return_value=[])),
    ):
        downstream = await orchestrator._execute_deployment_plan(plan)

    # Dry-run should roll back the SAVEPOINT and return empty downstream
    assert downstream == []


class TestClassifyParents:
    """Unit tests for DeploymentOrchestrator._classify_parents.

    Covers parity gaps with the single-node validation path:
      - derived metrics drop non-metric resolutions and never emit MissingParent
      - regular queries emit MissingParent for unresolved ast.Table names
      - regular queries keep all resolved Node objects as parents regardless of type
    """

    @staticmethod
    def _make_node(name: str, node_type):
        from datajunction_server.models.node_type import NodeType as _NT

        node = MagicMock()
        node.name = name
        node.type = node_type if isinstance(node_type, _NT) else _NT(node_type)
        return node

    def test_regular_query_unresolved_names_become_missing_parents(self):
        """Transform whose ast.Table ref is absent from dependency_nodes emits a
        MissingParent entry, preserving the single-node path's behavior."""
        spec = TransformSpec(
            name="t",
            namespace="default",
            query="SELECT a FROM default.missing_src",
        )
        resolved, missing = DeploymentOrchestrator._classify_parents(
            spec,
            dep_names=["default.missing_src"],
            dependency_nodes={},
        )
        assert resolved == []
        assert missing == ["default.missing_src"]

    def test_regular_query_resolved_parents_kept_regardless_of_type(self):
        """A transform that references a source returns the source Node as a parent."""
        from datajunction_server.models.node_type import NodeType as _NT

        src = self._make_node("default.src", _NT.SOURCE)
        spec = TransformSpec(
            name="t",
            namespace="default",
            query="SELECT a FROM default.src",
        )
        resolved, missing = DeploymentOrchestrator._classify_parents(
            spec,
            dep_names=["default.src"],
            dependency_nodes={"default.src": src},
        )
        assert resolved == [src]
        assert missing == []

    def test_derived_metric_drops_non_metric_resolutions(self):
        """Derived metric that references a dim-attribute like `ns.dim.col` must
        not store the dim as a parent — dim-attribute refs aren't SQL parents."""
        from datajunction_server.models.node_type import NodeType as _NT

        metric_a = self._make_node("default.metric_a", _NT.METRIC)
        dim_x = self._make_node("default.dim_x", _NT.DIMENSION)
        spec = MetricSpec(
            name="derived",
            namespace="default",
            query="SELECT default.metric_a * 2 + default.dim_x.year",
        )
        # extract_node_graph emits both full id and parent-path candidates
        dep_names = [
            "default.metric_a",
            "default.dim_x.year",
            "default.dim_x",
        ]
        dependency_nodes = {
            "default.metric_a": metric_a,
            "default.dim_x": dim_x,
        }
        resolved, missing = DeploymentOrchestrator._classify_parents(
            spec,
            dep_names,
            dependency_nodes,
        )
        assert resolved == [metric_a]
        # Derived metrics never emit MissingParent rows — speculative prefix
        # candidates aren't genuine references.
        assert missing == []

    def test_derived_metric_never_emits_missing_parents(self):
        """Even when a derived metric's candidate resolves to nothing, no
        MissingParent is recorded (candidates are speculative prefix expansions)."""
        spec = MetricSpec(
            name="derived",
            namespace="default",
            query="SELECT default.metric_a / default.metric_b",
        )
        resolved, missing = DeploymentOrchestrator._classify_parents(
            spec,
            dep_names=["default.metric_a", "default.metric_b"],
            dependency_nodes={},
        )
        assert resolved == []
        assert missing == []

    def test_metric_with_from_clause_treated_as_regular(self):
        """A metric whose query has a FROM clause is NOT a derived metric and
        follows the regular-query path (emit MissingParent for unresolved refs)."""
        spec = MetricSpec(
            name="std_metric",
            namespace="default",
            query="SELECT SUM(amount) FROM default.missing_fact",
        )
        resolved, missing = DeploymentOrchestrator._classify_parents(
            spec,
            dep_names=["default.missing_fact"],
            dependency_nodes={},
        )
        assert resolved == []
        assert missing == ["default.missing_fact"]


class TestCreateOrUpdateDimensionJoinLink:
    """Tests for create_or_update_dimension_join_link idempotency."""

    @pytest.mark.asyncio
    async def test_exact_match_returns_refresh(
        self,
        session,
        mock_deployment_context,
    ):
        """When a dimension link already exists with identical fields,
        create_or_update_dimension_join_link should return REFRESH (noop)
        without modifying the link."""
        orchestrator = DeploymentOrchestrator(
            deployment_spec=DeploymentSpec(namespace="default", nodes=[]),
            deployment_id="idempotency-test",
            session=session,
            context=mock_deployment_context,
        )

        # Create mock dimension node
        dim_node = MagicMock()
        dim_node.name = "default.date_dim"
        dim_node.id = 42

        # Create a mock node revision with an existing dimension link
        existing_link = MagicMock()
        existing_link.dimension = dim_node
        existing_link.role = None
        existing_link.join_sql = "t.date_id = default.date_dim.dateint"
        existing_link.join_type = JoinType.LEFT
        existing_link.join_cardinality = JoinCardinality.MANY_TO_ONE
        existing_link.default_value = None
        existing_link.spark_hints = None

        node_revision = MagicMock()
        node_revision.dimension_links = [existing_link]
        node_revision.name = "default.my_transform"

        link_input = JoinLinkInput(
            dimension_node="default.date_dim",
            join_on="t.date_id = default.date_dim.dateint",
            join_type=JoinType.LEFT,
        )

        result_link, activity = await orchestrator.create_or_update_dimension_join_link(
            node_revision=node_revision,
            dimension_node=dim_node,
            link_input=link_input,
            join_type=JoinType.LEFT,
        )

        assert activity == ActivityType.REFRESH
        assert result_link is existing_link

    @pytest.mark.asyncio
    async def test_multiple_links_same_dimension_no_role_idempotent(
        self,
        session,
        mock_deployment_context,
    ):
        """Two links to the same dimension with role=None but different join_sql
        should both return REFRESH on re-deploy (no reshuffling)."""
        orchestrator = DeploymentOrchestrator(
            deployment_spec=DeploymentSpec(namespace="default", nodes=[]),
            deployment_id="multi-link-test",
            session=session,
            context=mock_deployment_context,
        )

        dim_node = MagicMock()
        dim_node.name = "default.date_dim"
        dim_node.id = 42

        # Two links to the same dimension, different join_sql, both role=None
        link_epoch = MagicMock()
        link_epoch.dimension = dim_node
        link_epoch.role = None
        link_epoch.join_sql = "t.epoch_date = default.date_dim.dateint"
        link_epoch.join_type = JoinType.INNER
        link_epoch.join_cardinality = JoinCardinality.MANY_TO_ONE
        link_epoch.default_value = None
        link_epoch.spark_hints = None

        link_region = MagicMock()
        link_region.dimension = dim_node
        link_region.role = None
        link_region.join_sql = "t.region_date = default.date_dim.dateint"
        link_region.join_type = JoinType.INNER
        link_region.join_cardinality = JoinCardinality.MANY_TO_ONE
        link_region.default_value = None
        link_region.spark_hints = None

        node_revision = MagicMock()
        node_revision.dimension_links = [link_epoch, link_region]
        node_revision.name = "default.my_transform"

        # Re-deploy first link (region_date)
        link_input_region = JoinLinkInput(
            dimension_node="default.date_dim",
            join_on="t.region_date = default.date_dim.dateint",
            join_type=JoinType.INNER,
        )
        result1, activity1 = await orchestrator.create_or_update_dimension_join_link(
            node_revision=node_revision,
            dimension_node=dim_node,
            link_input=link_input_region,
            join_type=JoinType.INNER,
        )
        assert activity1 == ActivityType.REFRESH
        assert result1 is link_region

        # Re-deploy second link (epoch_date)
        link_input_epoch = JoinLinkInput(
            dimension_node="default.date_dim",
            join_on="t.epoch_date = default.date_dim.dateint",
            join_type=JoinType.INNER,
        )
        result2, activity2 = await orchestrator.create_or_update_dimension_join_link(
            node_revision=node_revision,
            dimension_node=dim_node,
            link_input=link_input_epoch,
            join_type=JoinType.INNER,
        )
        assert activity2 == ActivityType.REFRESH
        assert result2 is link_epoch

    @pytest.mark.asyncio
    async def test_single_candidate_gets_updated(
        self,
        session,
        mock_deployment_context,
    ):
        """When there's exactly one link matching (dimension, role) but with
        different join_sql, it should be updated (not duplicated)."""
        orchestrator = DeploymentOrchestrator(
            deployment_spec=DeploymentSpec(namespace="default", nodes=[]),
            deployment_id="update-test",
            session=session,
            context=mock_deployment_context,
        )

        dim_node = MagicMock()
        dim_node.name = "default.date_dim"
        dim_node.id = 42

        existing_link = MagicMock()
        existing_link.dimension = dim_node
        existing_link.role = None
        existing_link.join_sql = "t.old_date = default.date_dim.dateint"
        existing_link.join_type = JoinType.LEFT
        existing_link.join_cardinality = JoinCardinality.MANY_TO_ONE
        existing_link.default_value = None
        existing_link.spark_hints = None

        node_revision = MagicMock()
        node_revision.dimension_links = [existing_link]
        node_revision.name = "default.my_transform"

        link_input = JoinLinkInput(
            dimension_node="default.date_dim",
            join_on="t.new_date = default.date_dim.dateint",
            join_type=JoinType.LEFT,
        )

        result_link, activity = await orchestrator.create_or_update_dimension_join_link(
            node_revision=node_revision,
            dimension_node=dim_node,
            link_input=link_input,
            join_type=JoinType.LEFT,
        )

        assert activity == ActivityType.UPDATE
        assert result_link is existing_link
        assert existing_link.join_sql == "t.new_date = default.date_dim.dateint"
