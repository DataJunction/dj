"""
Unit tests for DeploymentOrchestrator
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock

from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.internal.deployment.orchestrator import (
    DeploymentOrchestrator,
    DeploymentPlan,
    ResourceRegistry,
    column_changed,
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
)
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.database.tag import Tag
from datajunction_server.database.catalog import Catalog
from datajunction_server.errors import DJError, DJInvalidDeploymentConfig, ErrorCode


@pytest.fixture
def mock_deployment_context(current_user: User):
    """Mock DeploymentContext with sensible defaults"""
    context = Mock()
    context.current_user = current_user
    context.request = Mock()
    context.query_service_client = Mock()
    context.validate_access = AsyncMock(return_value=True)
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
        assert result_catalogs == {}
        assert len(orchestrator.errors) == 1


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
            patch.object(orchestrator, "_execute_deployment_plan") as mock_execute_plan,
        ):
            # Configure deployment plan
            mock_plan = Mock(spec=DeploymentPlan)
            mock_plan.is_empty.return_value = False
            mock_create_plan.return_value = mock_plan

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
            mock_create_plan.return_value = mock_plan

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
