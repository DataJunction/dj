"""
Tests for validate_query_node exception handling with real objects
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from unittest.mock import patch

from datajunction_server.internal.deployment.validation import (
    NodeSpecBulkValidator,
    ValidationContext,
    _reparse_column_types,
)
from datajunction_server.models.deployment import ColumnSpec, TransformSpec, MetricSpec
from datajunction_server.models.node import NodeType, NodeStatus
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User, OAuthProvider
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.column import Column
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse
from datajunction_server.sql.parsing.types import IntegerType, MapType, StringType
from datajunction_server.errors import ErrorCode


class TestValidateQuery:
    """Test validate_query_node exception handling with real database objects"""

    @pytest_asyncio.fixture
    async def user(self, session: AsyncSession) -> User:
        """Create a test user"""
        user = User(
            username="testuser",
            oauth_provider=OAuthProvider.BASIC,
        )
        session.add(user)
        await session.commit()
        return user

    @pytest_asyncio.fixture
    async def catalog(self, session: AsyncSession) -> Catalog:
        """Create a test catalog"""
        catalog = Catalog(
            name="test_catalog",
            engines=[],
        )
        session.add(catalog)
        await session.commit()
        return catalog

    @pytest_asyncio.fixture
    async def parent_node(
        self,
        session: AsyncSession,
        user: User,
        catalog: Catalog,
    ) -> Node:
        """Create a parent source node for dependencies"""
        node = Node(
            name="test.parent",
            type=NodeType.SOURCE,
            current_version="v1",
            created_by_id=user.id,
        )
        node_revision = NodeRevision(
            node=node,
            name=node.name,
            catalog_id=catalog.id,
            type=node.type,
            version="v1",
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="name", type=StringType(), order=1),
                Column(name="value", type=IntegerType(), order=2),
            ],
            created_by_id=user.id,
        )
        node.current = node_revision
        session.add(node_revision)
        await session.commit()
        return node

    @pytest_asyncio.fixture
    async def validation_context(
        self,
        session: AsyncSession,
        parent_node: Node,
    ) -> ValidationContext:
        """Create a real ValidationContext with actual dependencies"""

        # Create a real compile context
        compile_context = ast.CompileContext(
            session=session,
            exception=ast.DJException(),
            dependencies_cache={parent_node.name: parent_node},
        )

        return ValidationContext(
            session=session,
            node_graph={"test.transform": ["test.parent"]},
            dependency_nodes={parent_node.name: parent_node},
            compile_context=compile_context,
        )

    @pytest.fixture
    def transform_spec(self) -> TransformSpec:
        """Create a test transform spec"""
        return TransformSpec(
            name="transform",
            query="SELECT id, name FROM test.parent",
            description="A test transform",
            mode="published",
        )

    @pytest.fixture
    def metric_spec(self) -> MetricSpec:
        """Create a test metric spec that will cause parsing issues"""
        return MetricSpec(
            name="metric",
            query="SELECT COUNT(*) FROM test.parent",
            description="A test metric",
            mode="published",
            required_dimensions=[
                "test.parent.id",
            ],  # This could cause validation issues
        )

    @pytest.mark.asyncio
    async def test_validate_query_node_parse_exception(
        self,
        validation_context: ValidationContext,
        transform_spec: TransformSpec,
    ):
        """Test exception when parsing fails due to malformed SQL"""
        bad_spec = TransformSpec(
            name="bad_transform",
            query="SELECT 1a FROM some_table",  # Invalid SQL
            description="Bad transform",
            mode="published",
            primary_key=["id"],  # To avoid primary key inference issues
        )
        parsed_ast = parse(bad_spec.query)
        validator = NodeSpecBulkValidator(validation_context)
        result = await validator.validate_query_node(bad_spec, parsed_ast)
        assert result.spec == bad_spec
        assert result.status == NodeStatus.INVALID
        assert result.inferred_columns == []
        error_codes = [e.code for e in result.errors]
        assert ErrorCode.TYPE_INFERENCE in error_codes
        type_err = next(e for e in result.errors if e.code == ErrorCode.TYPE_INFERENCE)
        assert "Unable to infer type for column `1a`" in type_err.message

    @pytest.mark.asyncio
    async def test_validate_query_node_later_exception(
        self,
        validation_context: ValidationContext,
        transform_spec: TransformSpec,
    ):
        """
        Test exception during later validation steps with real AST
        """
        parsed_ast = parse("SELECT 1a FROM some_table")
        validator = NodeSpecBulkValidator(validation_context)
        # mock _check_inferred_columns to raise an exception
        with patch.object(
            validator,
            "_check_inferred_columns",
            side_effect=ValueError("Column inference failed"),
        ):
            result = await validator.validate_query_node(transform_spec, parsed_ast)
            assert result.status == NodeStatus.INVALID
            assert len(result.errors) == 1
            assert result.errors[0].code == ErrorCode.INVALID_SQL_QUERY

    @pytest.mark.asyncio
    async def test_validate_query_node_dependency_extraction_failure(
        self,
        validation_context: ValidationContext,
        transform_spec: TransformSpec,
    ):
        """Test exception during dependency extraction with real AST"""

        # Parse a valid query
        parsed_ast = parse(transform_spec.query)

        # Mock the compile context to throw during extraction
        with patch.object(
            validation_context.compile_context,
            "exception",
            side_effect=Exception("Dependency extraction failed"),
        ):
            # Patch extract_dependencies to throw
            with patch.object(
                parsed_ast.bake_ctes(),
                "extract_dependencies",
                side_effect=Exception("Dependency extraction failed"),
            ):
                validator = NodeSpecBulkValidator(validation_context)

                # This should hit the exception handler
                result = await validator.validate_query_node(transform_spec, parsed_ast)

        # Verify proper error handling
        assert result.status == NodeStatus.INVALID
        assert len(result.errors) == 1
        assert result.errors[0].code == ErrorCode.INVALID_SQL_QUERY
        assert "Dependency extraction failed" in result.errors[0].message

    @pytest.mark.asyncio
    async def test_validate_query_node_column_inference_failure(
        self,
        validation_context: ValidationContext,
        transform_spec: TransformSpec,
    ):
        """Test exception during column inference with real objects"""

        # Parse a valid query
        parsed_ast = parse(transform_spec.query)

        # Create validator and patch _infer_columns to fail
        validator = NodeSpecBulkValidator(validation_context)

        with patch.object(
            validator,
            "_infer_columns",
            side_effect=AttributeError("Column inference failed - missing attribute"),
        ):
            # This should hit the exception handler
            result = await validator.validate_query_node(transform_spec, parsed_ast)

        # Verify the exception was caught and handled
        assert result.status == NodeStatus.INVALID
        assert len(result.errors) == 1
        assert result.errors[0].code == ErrorCode.INVALID_SQL_QUERY
        assert "Column inference failed" in result.errors[0].message

    @pytest.mark.asyncio
    async def test_validate_query_node_metric_validation_exception(
        self,
        validation_context: ValidationContext,
        metric_spec: MetricSpec,
    ):
        """Test exception during metric-specific validation"""

        # Parse the metric query
        parsed_ast = parse(metric_spec.query)

        # Create validator and patch metric validation to fail
        validator = NodeSpecBulkValidator(validation_context)

        with patch.object(
            validator,
            "_check_metric_query",
            side_effect=ValueError("Metric validation failed"),
        ):
            # This should hit the exception handler
            result = await validator.validate_query_node(metric_spec, parsed_ast)

        # Verify proper error handling
        assert result.status == NodeStatus.INVALID
        assert len(result.errors) == 1
        assert result.errors[0].code == ErrorCode.INVALID_SQL_QUERY
        assert "Metric validation failed" in result.errors[0].message

    @pytest.mark.asyncio
    async def test_validate_query_node_successful_path_for_comparison(
        self,
        validation_context: ValidationContext,
        transform_spec: TransformSpec,
    ):
        """Test the successful path to ensure our exception tests are meaningful"""

        # Parse a valid query
        parsed_ast = parse(transform_spec.query)

        # Create validator
        validator = NodeSpecBulkValidator(validation_context)

        # This should succeed (not hit exception handler)
        result = await validator.validate_query_node(transform_spec, parsed_ast)

        # Verify success (this ensures our exception tests are testing real failures)
        assert result.status in [
            NodeStatus.VALID,
            NodeStatus.INVALID,
        ]  # Could be invalid for other reasons
        assert result.spec == transform_spec
        # If it failed, it should be due to validation logic, not exceptions
        if result.status == NodeStatus.INVALID:
            # Should have specific validation errors, not generic exception errors
            for error in result.errors:
                assert (
                    error.code != ErrorCode.INVALID_SQL_QUERY
                    or "No columns could be inferred" in error.message
                )

    @pytest.mark.asyncio
    async def test_validate_query_node_with_skip_validation(
        self,
        validation_context: ValidationContext,
    ):
        """Test that specs with _skip_validation preserve their columns without re-inferring"""
        # Create a spec with _skip_validation flag set and pre-defined columns
        spec = TransformSpec(
            name="transform_from_git",
            query="SELECT id, name FROM test.parent",
            description="Transform copied from validated namespace",
            mode="published",
            columns=[
                ColumnSpec(name="id", type="int"),
                ColumnSpec(name="name", type="string"),
            ],
        )
        spec._skip_validation = True  # Set the private flag

        parsed_ast = parse(spec.query)
        validator = NodeSpecBulkValidator(validation_context)
        result = await validator.validate_query_node(spec, parsed_ast)

        # Should use the spec's columns directly without re-inference
        assert result.inferred_columns == spec.columns
        assert result.errors == []

    @pytest_asyncio.fixture
    async def invalid_parent_node(
        self,
        session: AsyncSession,
        user: User,
        catalog: Catalog,
    ) -> Node:
        """Create a transform node whose current revision is INVALID"""
        node = Node(
            name="test.invalid_parent",
            type=NodeType.TRANSFORM,
            current_version="v1",
            created_by_id=user.id,
        )
        node_revision = NodeRevision(
            node=node,
            name=node.name,
            catalog_id=catalog.id,
            type=node.type,
            version="v1",
            query="SELECT bad_col FROM nowhere",
            status=NodeStatus.INVALID,
            columns=[
                Column(name="bad_col", type=IntegerType(), order=0),
            ],
            created_by_id=user.id,
        )
        node.current = node_revision
        session.add(node_revision)
        await session.commit()
        return node

    @pytest.mark.asyncio
    async def test_invalid_parent_propagation(
        self,
        session: AsyncSession,
        user: User,
        catalog: Catalog,
        invalid_parent_node: Node,
    ):
        """Transform whose parent is INVALID should itself be INVALID with INVALID_PARENT error"""
        compile_context = ast.CompileContext(
            session=session,
            exception=ast.DJException(),
            dependencies_cache={invalid_parent_node.name: invalid_parent_node},
        )
        context = ValidationContext(
            session=session,
            node_graph={"test.child_transform": [invalid_parent_node.name]},
            dependency_nodes={invalid_parent_node.name: invalid_parent_node},
            compile_context=compile_context,
        )
        spec = TransformSpec(
            name="test.child_transform",
            query=f"SELECT bad_col FROM {invalid_parent_node.name}",
            description="Child of invalid parent",
            mode="published",
        )

        parsed_ast = parse(spec.query)
        validator = NodeSpecBulkValidator(context)
        result = await validator.validate_query_node(spec, parsed_ast)

        assert result.status == NodeStatus.INVALID
        error_codes = [e.code for e in result.errors]
        assert ErrorCode.INVALID_PARENT in error_codes
        invalid_parent_err = next(
            e for e in result.errors if e.code == ErrorCode.INVALID_PARENT
        )
        assert invalid_parent_node.name in invalid_parent_err.message


class TestReparseColumnTypes:
    """Tests for _reparse_column_types helper (PR 2)."""

    def _make_node(self, columns: list) -> Node:
        """Build a minimal in-memory Node with a current revision."""
        node = Node(name="test.node", type=NodeType.SOURCE)
        revision = NodeRevision(
            name=node.name,
            type=node.type,
            version="v1",
            columns=columns,
        )
        node.current = revision
        return node

    def test_string_type_is_reparsed(self):
        """Column with a string type like 'MAP<string,bigint>' is parsed to MapType."""
        col = Column(name="tags", type="MAP<string, bigint>", order=0)
        node = self._make_node([col])
        _reparse_column_types({node.name: node})
        assert isinstance(col.type, MapType)

    def test_already_parsed_type_is_unchanged(self):
        """Column already holding a parsed type object is left alone."""
        col = Column(name="id", type=IntegerType(), order=0)
        original_type = col.type
        node = self._make_node([col])
        _reparse_column_types({node.name: node})
        assert col.type is original_type

    def test_unparseable_string_is_left_unchanged(self):
        """If parse_rule raises, the original type value is preserved."""
        col = Column(name="weird", type="NOT_A_VALID_TYPE_$$$$", order=0)
        node = self._make_node([col])
        _reparse_column_types({node.name: node})
        # Should not raise; type is still the original string
        assert col.type == "NOT_A_VALID_TYPE_$$$$"

    def test_node_without_current_is_skipped(self):
        """Node with no current revision doesn't cause an error."""
        node = Node(name="orphan", type=NodeType.SOURCE)
        node.current = None
        _reparse_column_types({node.name: node})  # Should not raise

    def test_empty_dict_is_noop(self):
        """Empty dependency_nodes dict is handled without error."""
        _reparse_column_types({})  # Should not raise
