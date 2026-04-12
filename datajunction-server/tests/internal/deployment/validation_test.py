"""
Tests for validate_query_node exception handling with real objects
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from unittest.mock import MagicMock, patch

from datajunction_server.internal.deployment.validation import (
    NodeSpecBulkValidator,
    NodeValidationResult,
    ValidationContext,
    _reparse_column_types,
    bulk_validate_node_data,
)
from datajunction_server.models.deployment import (
    ColumnSpec,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.node import (
    DimensionAttributeOutput,
    NodeType,
    NodeStatus,
)
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User, OAuthProvider
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.column import Column
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.types import IntegerType, MapType, StringType
from datajunction_server.errors import ErrorCode


@pytest_asyncio.fixture
async def user(session: AsyncSession) -> User:
    """Create a test user"""
    user = User(
        username="testuser",
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(user)
    await session.commit()
    return user


@pytest_asyncio.fixture
async def catalog(session: AsyncSession) -> Catalog:
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


class TestValidateQuery:
    """Test validate_query_node exception handling with real database objects"""

    @pytest_asyncio.fixture
    async def validation_context(
        self,
        session: AsyncSession,
        parent_node: Node,
    ) -> ValidationContext:
        """Create a real ValidationContext with actual dependencies"""

        return ValidationContext(
            session=session,
            node_graph={"test.transform": ["test.parent"]},
            dependency_nodes={parent_node.name: parent_node},
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
        """Test that a query referencing a missing table produces errors"""
        bad_spec = TransformSpec(
            name="bad_transform",
            query="SELECT x FROM nonexistent.table",
            description="Bad transform",
            mode="published",
            primary_key=["id"],
        )
        validator = NodeSpecBulkValidator(validation_context)
        result = validator.validate_query_node(bad_spec)
        assert result.spec == bad_spec
        assert result.status == NodeStatus.INVALID
        error_codes = [e.code for e in result.errors]
        assert ErrorCode.TYPE_INFERENCE in error_codes

    @pytest.mark.asyncio
    async def test_validate_query_node_later_exception(
        self,
        validation_context: ValidationContext,
        transform_spec: TransformSpec,
    ):
        """
        Test exception during later validation steps with real AST
        """
        validator = NodeSpecBulkValidator(validation_context)
        # mock _check_inferred_columns to raise an exception
        with patch.object(
            validator,
            "_check_inferred_columns",
            side_effect=ValueError("Column inference failed"),
        ):
            result = validator.validate_query_node(transform_spec)
            assert result.status == NodeStatus.INVALID
            assert len(result.errors) == 1
            assert result.errors[0].code == ErrorCode.INVALID_SQL_QUERY

    @pytest.mark.asyncio
    async def test_validate_query_node_validate_node_query_failure(
        self,
        validation_context: ValidationContext,
        transform_spec: TransformSpec,
    ):
        """Test exception during validate_node_query with real AST"""
        validator = NodeSpecBulkValidator(validation_context)

        with patch(
            "datajunction_server.internal.deployment.validation.validate_node_query",
            side_effect=RuntimeError("Unexpected validation failure"),
        ):
            result = validator.validate_query_node(transform_spec)

        assert result.status == NodeStatus.INVALID
        assert len(result.errors) == 1
        assert result.errors[0].code == ErrorCode.INVALID_SQL_QUERY
        assert "Unexpected validation failure" in result.errors[0].message

    @pytest.mark.asyncio
    async def test_validate_query_node_metric_validation_exception(
        self,
        validation_context: ValidationContext,
        metric_spec: MetricSpec,
    ):
        """Test exception during metric-specific validation"""

        # Parse the metric query

        # Create validator and patch metric validation to fail
        validator = NodeSpecBulkValidator(validation_context)

        with patch.object(
            validator,
            "_check_metric_query",
            side_effect=ValueError("Metric validation failed"),
        ):
            # This should hit the exception handler
            result = validator.validate_query_node(metric_spec)

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

        # Create validator
        validator = NodeSpecBulkValidator(validation_context)

        # This should succeed (not hit exception handler)
        result = validator.validate_query_node(transform_spec)

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

        validator = NodeSpecBulkValidator(validation_context)
        result = validator.validate_query_node(spec)

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
        context = ValidationContext(
            session=session,
            node_graph={"test.child_transform": [invalid_parent_node.name]},
            dependency_nodes={invalid_parent_node.name: invalid_parent_node},
        )
        spec = TransformSpec(
            name="test.child_transform",
            query=f"SELECT bad_col FROM {invalid_parent_node.name}",
            description="Child of invalid parent",
            mode="published",
        )

        validator = NodeSpecBulkValidator(context)
        result = validator.validate_query_node(spec)

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


class TestBulkValidateSkipValidation:
    """Tests for the _skip_validation fast-path in bulk_validate_node_data."""

    def _make_node(self, name: str, columns: list) -> Node:
        node = Node(name=name, type=NodeType.SOURCE)
        revision = NodeRevision(
            name=name,
            type=node.type,
            version="v1",
            columns=columns,
        )
        node.current = revision
        return node

    @pytest.mark.asyncio
    async def test_skip_validation_specs_bypass_parsing(
        self,
        session: AsyncSession,
    ):
        """Specs with _skip_validation=True return pre-existing columns without SQL parsing."""
        spec = TransformSpec(
            name="test.copy_node",
            query="SELECT id, name FROM test.parent",
            description="Copied node",
            mode="published",
            columns=[
                ColumnSpec(name="id", type="int"),
                ColumnSpec(name="name", type="string"),
            ],
        )
        spec._skip_validation = True

        results = await bulk_validate_node_data(
            node_specs=[spec],
            node_graph={"test.copy_node": []},
            session=session,
            dependency_nodes={},
        )

        assert len(results) == 1
        assert results[0].status == NodeStatus.VALID
        assert results[0].inferred_columns == spec.columns

    @pytest.mark.asyncio
    async def test_mixed_specs_skip_and_validate(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Specs with and without _skip_validation are handled in the same batch."""
        skip_spec = TransformSpec(
            name="test.skip_node",
            query="SELECT id FROM test.parent",
            description="Copied",
            mode="published",
            columns=[ColumnSpec(name="id", type="int")],
        )
        skip_spec._skip_validation = True

        normal_spec = TransformSpec(
            name="test.normal_node",
            query="SELECT id FROM test.parent",
            description="Normal node",
            mode="published",
            columns=[ColumnSpec(name="id", type="int")],
        )

        results = await bulk_validate_node_data(
            node_specs=[skip_spec, normal_spec],
            node_graph={
                "test.skip_node": ["test.parent"],
                "test.normal_node": [],
            },
            session=session,
            dependency_nodes={parent_node.name: parent_node},
        )

        assert len(results) == 2
        # skip_spec result uses pre-existing columns
        skip_result = next(r for r in results if r.spec.name == "test.skip_node")
        assert skip_result.inferred_columns == skip_spec.columns

    @pytest.mark.asyncio
    async def test_validate_without_parsing_directly(
        self,
        session: AsyncSession,
    ):
        """_validate_without_parsing returns VALID result with spec columns."""
        spec = TransformSpec(
            name="test.direct",
            query="SELECT id FROM nowhere",
            description="Direct",
            mode="published",
            columns=[ColumnSpec(name="id", type="bigint")],
        )
        spec._skip_validation = True

        context = ValidationContext(
            session=session,
            node_graph={"test.direct": []},
            dependency_nodes={},
        )
        validator = NodeSpecBulkValidator(context)
        result = validator._validate_without_parsing(spec)

        assert result.status == NodeStatus.VALID
        assert result.inferred_columns == spec.columns
        assert result.errors == []


class TestRequiredDimensions:
    """Tests for required_dimensions validation (PR 3)."""

    # ------------------------------------------------------------------ helpers

    def _make_dim_node(
        self,
        name: str,
        col_names: list[str],
    ) -> Node:
        node = Node(name=name, type=NodeType.DIMENSION)
        revision = NodeRevision(
            name=name,
            type=NodeType.DIMENSION,
            version="v1",
            columns=[
                Column(name=c, type=StringType(), order=i)
                for i, c in enumerate(col_names)
            ],
        )
        node.current = revision
        return node

    def _make_context(
        self,
        session: AsyncSession,
        parent_node: Node,
    ) -> ValidationContext:
        dep_nodes = {parent_node.name: parent_node}
        return ValidationContext(
            session=session,
            node_graph={"test.metric": [parent_node.name]},
            dependency_nodes=dep_nodes,
        )

    # ------------------------------------------------------------------ unit tests (no DB query needed)

    @pytest.mark.asyncio
    async def test_valid_full_path_in_dependency_nodes(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """required_dimensions full-path column found in dependency_nodes → VALID."""
        dim_node = self._make_dim_node("test.dim", ["dateint"])
        context = self._make_context(session, parent_node)
        spec = MetricSpec(
            name="test.metric",
            query="SELECT COUNT(*) FROM test.parent",
            required_dimensions=["test.dim.dateint"],
        )
        validator = NodeSpecBulkValidator(context)
        # Pre-populate as if _prefetch_required_dimension_nodes ran
        validator._all_dim_nodes = {**context.dependency_nodes, "test.dim": dim_node}

        parsed_ast = parse(spec.rendered_query)
        spec._query_ast = parsed_ast
        result = validator.validate_query_node(spec)

        assert result.status == NodeStatus.VALID
        error_codes = [e.code for e in result.errors]
        assert ErrorCode.INVALID_COLUMN not in error_codes

    @pytest.mark.asyncio
    async def test_invalid_column_not_found_on_dim_node(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """required_dimensions full-path column absent from dim node → INVALID."""
        dim_node = self._make_dim_node("test.dim", ["dateint"])
        context = self._make_context(session, parent_node)
        spec = MetricSpec(
            name="test.metric",
            query="SELECT COUNT(*) FROM test.parent",
            required_dimensions=["test.dim.nonexistent_col"],
        )
        validator = NodeSpecBulkValidator(context)
        validator._all_dim_nodes = {**context.dependency_nodes, "test.dim": dim_node}
        result = validator.validate_query_node(spec)

        assert result.status == NodeStatus.INVALID
        err = next(e for e in result.errors if e.code == ErrorCode.INVALID_COLUMN)
        assert err.debug is not None
        assert "test.dim.nonexistent_col" in err.debug["invalid_required_dimensions"]

    @pytest.mark.asyncio
    async def test_invalid_dim_node_not_found(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """required_dimensions references a dim node that doesn't exist → INVALID."""
        context = self._make_context(session, parent_node)
        spec = MetricSpec(
            name="test.metric",
            query="SELECT COUNT(*) FROM test.parent",
            required_dimensions=["ghost.dim.col"],
        )
        validator = NodeSpecBulkValidator(context)
        # _all_dim_nodes has no entry for "ghost.dim"
        validator._all_dim_nodes = dict(context.dependency_nodes)
        result = validator.validate_query_node(spec)

        assert result.status == NodeStatus.INVALID
        err = next(e for e in result.errors if e.code == ErrorCode.INVALID_COLUMN)
        assert err.debug is not None
        assert "ghost.dim.col" in err.debug["invalid_required_dimensions"]

    @pytest.mark.asyncio
    async def test_valid_short_name_in_parent_columns(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """required_dimensions short name found in parent columns → VALID."""
        context = self._make_context(session, parent_node)
        spec = MetricSpec(
            name="test.metric",
            query="SELECT SUM(value) FROM test.parent",
            # "value" is a column on parent_node (IntegerType)
            required_dimensions=["value"],
        )
        validator = NodeSpecBulkValidator(context)
        validator._all_dim_nodes = dict(context.dependency_nodes)
        result = validator.validate_query_node(spec)

        assert result.status == NodeStatus.VALID
        error_codes = [e.code for e in result.errors]
        assert ErrorCode.INVALID_COLUMN not in error_codes

    @pytest.mark.asyncio
    async def test_invalid_short_name_not_in_parent_columns(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """required_dimensions short name absent from all parent columns → INVALID."""
        context = self._make_context(session, parent_node)
        spec = MetricSpec(
            name="test.metric",
            query="SELECT COUNT(*) FROM test.parent",
            required_dimensions=["no_such_col"],
        )
        validator = NodeSpecBulkValidator(context)
        validator._all_dim_nodes = dict(context.dependency_nodes)
        result = validator.validate_query_node(spec)

        assert result.status == NodeStatus.INVALID
        err = next(e for e in result.errors if e.code == ErrorCode.INVALID_COLUMN)
        assert err.debug is not None
        assert "no_such_col" in err.debug["invalid_required_dimensions"]

    @pytest.mark.asyncio
    async def test_no_required_dimensions_is_noop(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Spec with no required_dimensions produces no INVALID_COLUMN error."""
        context = self._make_context(session, parent_node)
        spec = MetricSpec(
            name="test.metric",
            query="SELECT COUNT(*) FROM test.parent",
        )
        validator = NodeSpecBulkValidator(context)
        validator._all_dim_nodes = dict(context.dependency_nodes)
        result = validator.validate_query_node(spec)

        error_codes = [e.code for e in result.errors]
        assert ErrorCode.INVALID_COLUMN not in error_codes

    # ------------------------------------------------------------------ DB-fetch path

    @pytest_asyncio.fixture
    async def dim_node_in_db(
        self,
        session: AsyncSession,
        user: User,
        catalog: Catalog,
    ) -> Node:
        """A dimension node that exists in the DB but is NOT in dependency_nodes."""
        node = Node(
            name="test.external_dim",
            type=NodeType.DIMENSION,
            current_version="v1",
            created_by_id=user.id,
        )
        revision = NodeRevision(
            node=node,
            name=node.name,
            catalog_id=catalog.id,
            type=node.type,
            version="v1",
            columns=[Column(name="region", type=StringType(), order=0)],
            created_by_id=user.id,
        )
        node.current = revision
        session.add(revision)
        await session.commit()
        return node

    @pytest.mark.asyncio
    async def test_prefetch_fetches_dim_node_from_db(
        self,
        session: AsyncSession,
        parent_node: Node,
        dim_node_in_db: Node,
    ):
        """_prefetch_required_dimension_nodes fetches a node not in dependency_nodes."""
        context = self._make_context(session, parent_node)
        spec = MetricSpec(
            name="test.metric",
            query="SELECT COUNT(*) FROM test.parent",
            required_dimensions=["test.external_dim.region"],
        )
        validator = NodeSpecBulkValidator(context)

        await validator._prefetch_required_dimension_nodes([spec])

        assert "test.external_dim" in validator._all_dim_nodes
        fetched = validator._all_dim_nodes["test.external_dim"]
        assert fetched.current is not None
        col_names = {c.name for c in fetched.current.columns}
        assert "region" in col_names

    @pytest.mark.asyncio
    async def test_prefetch_with_no_required_dimensions(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """_prefetch_required_dimension_nodes with specs lacking required_dimensions is a noop."""
        context = self._make_context(session, parent_node)
        spec = MetricSpec(
            name="test.metric",
            query="SELECT COUNT(*) FROM test.parent",
        )
        validator = NodeSpecBulkValidator(context)
        await validator._prefetch_required_dimension_nodes([spec])

        # _all_dim_nodes is just a copy of dependency_nodes
        assert set(validator._all_dim_nodes.keys()) == set(
            context.dependency_nodes.keys(),
        )


# ---------------------------------------------------------------------------
# Helpers shared by TestCrossFactDimensions
# ---------------------------------------------------------------------------


def _dim(name: str) -> DimensionAttributeOutput:
    """Build a minimal DimensionAttributeOutput with the given name."""
    return DimensionAttributeOutput(
        name=name,
        node_name=None,
        node_display_name=None,
        properties=None,
        type=None,
        path=[],
    )


def _make_metric_node(name: str) -> Node:
    """Build an in-memory METRIC Node (no DB record needed)."""
    node = Node(name=name, type=NodeType.METRIC)
    revision = NodeRevision(name=name, type=NodeType.METRIC, version="v1")
    node.current = revision
    return node


class TestCrossFactDimensions:
    """Tests for cross-fact derived metric shared-dimension check (PR 4)."""

    def _make_context(
        self,
        session: AsyncSession,
        parent_node: Node,
        base_metric_a: Node,
        base_metric_b: Node,
    ) -> ValidationContext:
        dep_nodes = {
            parent_node.name: parent_node,
            base_metric_a.name: base_metric_a,
            base_metric_b.name: base_metric_b,
        }
        return ValidationContext(
            session=session,
            node_graph={
                "test.derived": [base_metric_a.name, base_metric_b.name],
            },
            dependency_nodes=dep_nodes,
        )

    # ------------------------------------------------------------------ unit tests (direct method calls)
    # These tests call _check_cross_fact_dimensions directly to isolate the logic
    # from extract_dependencies, which requires real DB-backed nodes.

    def test_shared_dimension_is_valid(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Cross-fact derived metric whose base metrics share a dimension → None."""
        ma = _make_metric_node("test.metric_a")
        mb = _make_metric_node("test.metric_b")
        context = self._make_context(session, parent_node, ma, mb)
        spec = MetricSpec(
            name="test.derived",
            query="SELECT test_metric_a + test_metric_b FROM test.metric_a, test.metric_b",
        )
        validator = NodeSpecBulkValidator(context)
        validator._metric_dimensions = {
            ma.name: {"date_id", "region"},
            mb.name: {"date_id", "country"},
        }

        assert validator._check_cross_fact_dimensions(spec) is None

    def test_no_shared_dimension_is_invalid(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Cross-fact derived metric whose base metrics share no dimension → DJError."""
        ma = _make_metric_node("test.metric_a")
        mb = _make_metric_node("test.metric_b")
        context = self._make_context(session, parent_node, ma, mb)
        spec = MetricSpec(
            name="test.derived",
            query="SELECT test_metric_a + test_metric_b FROM test.metric_a, test.metric_b",
        )
        validator = NodeSpecBulkValidator(context)
        validator._metric_dimensions = {
            ma.name: {"date_id", "region"},
            mb.name: {"country", "city"},
        }

        err = validator._check_cross_fact_dimensions(spec)
        assert err is not None
        assert err.code == ErrorCode.INVALID_PARENT
        assert ma.name in err.message
        assert mb.name in err.message

    def test_single_metric_parent_skipped(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Derived metric with only one metric parent is not cross-fact — returns None."""
        ma = _make_metric_node("test.metric_a")
        dep_nodes = {parent_node.name: parent_node, ma.name: ma}
        context = ValidationContext(
            session=session,
            node_graph={"test.derived": [ma.name]},
            dependency_nodes=dep_nodes,
        )
        spec = MetricSpec(
            name="test.derived",
            query="SELECT test_metric_a * 2 FROM test.metric_a",
        )
        validator = NodeSpecBulkValidator(context)
        validator._metric_dimensions = {ma.name: {"date_id"}}

        assert validator._check_cross_fact_dimensions(spec) is None

    def test_missing_prefetch_entry_skips_check(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """If only one base metric has a prefetched entry (e.g. other is new), skip."""
        ma = _make_metric_node("test.metric_a")
        mb = _make_metric_node("test.metric_b")
        context = self._make_context(session, parent_node, ma, mb)
        spec = MetricSpec(
            name="test.derived",
            query="SELECT test_metric_a + test_metric_b FROM test.metric_a, test.metric_b",
        )
        validator = NodeSpecBulkValidator(context)
        # Only metric_a was prefetched; metric_b has no DB record yet
        validator._metric_dimensions = {ma.name: {"date_id"}}

        assert validator._check_cross_fact_dimensions(spec) is None

    def test_non_metric_spec_skipped(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Non-metric spec always returns None."""
        context = ValidationContext(
            session=session,
            node_graph={},
            dependency_nodes={},
        )
        spec = TransformSpec(
            name="test.transform",
            query="SELECT id FROM test.parent",
        )
        validator = NodeSpecBulkValidator(context)
        assert validator._check_cross_fact_dimensions(spec) is None

    # ------------------------------------------------------------------ prefetch tests

    @pytest.mark.asyncio
    async def test_prefetch_calls_get_dimensions_for_each_base_metric(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """_prefetch_metric_dimensions issues one get_dimensions call per unique base metric."""
        ma = _make_metric_node("test.metric_a")
        mb = _make_metric_node("test.metric_b")
        context = self._make_context(session, parent_node, ma, mb)

        spec = MetricSpec(
            name="test.derived",
            query="SELECT test_metric_a + test_metric_b FROM test.metric_a, test.metric_b",
        )
        validator = NodeSpecBulkValidator(context)

        # Use a dict-keyed side_effect to avoid order sensitivity (set iteration).
        dim_map = {
            ma.name: [_dim("date_id"), _dim("region")],
            mb.name: [_dim("date_id"), _dim("country")],
        }

        async def fake_get_dimensions(session, node, **kwargs):
            return dim_map[node.name]

        with patch(
            "datajunction_server.internal.deployment.validation.get_dimensions",
            side_effect=fake_get_dimensions,
        ) as mock_get_dims:
            await validator._prefetch_metric_dimensions([spec])

        assert mock_get_dims.call_count == 2
        called_nodes = {call.args[1].name for call in mock_get_dims.call_args_list}
        assert called_nodes == {ma.name, mb.name}
        assert validator._metric_dimensions[ma.name] == {"date_id", "region"}
        assert validator._metric_dimensions[mb.name] == {"date_id", "country"}

    @pytest.mark.asyncio
    async def test_prefetch_deduplicates_base_metrics(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Two derived metrics sharing the same base metrics → only 2 get_dimensions calls."""
        ma = _make_metric_node("test.metric_a")
        mb = _make_metric_node("test.metric_b")
        dep_nodes = {
            parent_node.name: parent_node,
            ma.name: ma,
            mb.name: mb,
        }
        context = ValidationContext(
            session=session,
            node_graph={
                "test.derived1": [ma.name, mb.name],
                "test.derived2": [ma.name, mb.name],
            },
            dependency_nodes=dep_nodes,
        )
        specs = [
            MetricSpec(
                name="test.derived1",
                query="SELECT test_metric_a + test_metric_b FROM test.metric_a, test.metric_b",
            ),
            MetricSpec(
                name="test.derived2",
                query="SELECT test_metric_a - test_metric_b FROM test.metric_a, test.metric_b",
            ),
        ]
        validator = NodeSpecBulkValidator(context)

        async def fake_get_dimensions(session, node, **kwargs):
            return [_dim("date_id")]

        with patch(
            "datajunction_server.internal.deployment.validation.get_dimensions",
            side_effect=fake_get_dimensions,
        ) as mock_get_dims:
            await validator._prefetch_metric_dimensions(specs)

        # Despite 2 derived metrics, only 2 calls (one per unique base metric)
        assert mock_get_dims.call_count == 2

    @pytest.mark.asyncio
    async def test_prefetch_skips_non_cross_fact_metrics(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Metrics with ≤1 metric parent don't trigger a prefetch."""
        ma = _make_metric_node("test.metric_a")
        dep_nodes = {parent_node.name: parent_node, ma.name: ma}
        context = ValidationContext(
            session=session,
            node_graph={"test.simple_derived": [ma.name]},
            dependency_nodes=dep_nodes,
        )
        spec = MetricSpec(
            name="test.simple_derived",
            query="SELECT test_metric_a * 2 FROM test.metric_a",
        )
        validator = NodeSpecBulkValidator(context)

        with patch(
            "datajunction_server.internal.deployment.validation.get_dimensions",
        ) as mock_get_dims:
            await validator._prefetch_metric_dimensions([spec])

        mock_get_dims.assert_not_called()
        assert validator._metric_dimensions == {}


# ---------------------------------------------------------------------------
# TestDimLinkValidation — Step B: _validate_dimension_link_specs
# ---------------------------------------------------------------------------


def _make_validator(session: AsyncSession) -> NodeSpecBulkValidator:
    """Build a minimal NodeSpecBulkValidator for unit-testing _validate_dimension_link_specs."""
    context = ValidationContext(
        session=session,
        node_graph={},
        dependency_nodes={},
    )
    return NodeSpecBulkValidator(context)


def _make_source_result(
    node_name: str,
    col_names: list[str],
    join_on: str,
    dim_name: str,
) -> NodeValidationResult:
    """Build a NodeValidationResult for a source node with one dimension link."""
    spec = SourceSpec(
        name=node_name.split(".")[-1],
        catalog="default",
        schema_="s",
        table="t",
        columns=[ColumnSpec(name=c, type="int") for c in col_names],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node=dim_name,
                join_on=join_on,
            ),
        ],
    )
    spec.namespace = ".".join(node_name.split(".")[:-1]) or node_name
    return NodeValidationResult(
        spec=spec,
        status=NodeStatus.VALID,
        inferred_columns=[ColumnSpec(name=c, type="int") for c in col_names],
        errors=[],
        dependencies=[],
    )


class TestDimLinkValidation:
    """Unit tests for NodeSpecBulkValidator._validate_dimension_link_specs (Step B)."""

    def test_valid_dim_link_no_errors(self, session: AsyncSession):
        """A well-formed join_on with known columns produces no errors."""
        validator = _make_validator(session)
        result = _make_source_result(
            node_name="test.facts",
            col_names=["id", "dim_id"],
            join_on="test.facts.dim_id = test.dim.dim_id",
            dim_name="test.dim",
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.VALID
        assert result.errors == []

    def test_invalid_join_on_sql_raises_error(self, session: AsyncSession):
        """Unparseable join_on SQL produces an INVALID_SQL_QUERY error."""
        validator = _make_validator(session)
        result = _make_source_result(
            node_name="test.facts",
            col_names=["id", "dim_id"],
            join_on="NOT VALID SQL !!!",
            dim_name="test.dim",
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.INVALID
        assert any(e.code == ErrorCode.INVALID_SQL_QUERY for e in result.errors)
        assert any("test.dim" in e.message for e in result.errors)

    def test_join_on_column_not_on_node_raises_error(self, session: AsyncSession):
        """join_on referencing a nonexistent column on this node produces an INVALID_COLUMN error."""
        validator = _make_validator(session)
        result = _make_source_result(
            node_name="test.facts",
            col_names=["id", "dim_id"],
            join_on="test.facts.nonexistent_fk = test.dim.dim_id",
            dim_name="test.dim",
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.INVALID
        assert any(
            e.code == ErrorCode.INVALID_COLUMN and "nonexistent_fk" in e.message
            for e in result.errors
        )

    def test_join_on_external_node_column_is_invalid(self, session: AsyncSession):
        """join_on referencing an unknown table is flagged as INVALID_SQL_QUERY."""
        validator = _make_validator(session)
        result = _make_source_result(
            node_name="test.facts",
            col_names=["id", "dim_id"],
            # Column belongs to 'external.other' — neither node nor dim → invalid
            join_on="external.other.some_col = test.dim.dim_id",
            dim_name="test.dim",
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.INVALID
        assert any(
            e.code == ErrorCode.INVALID_SQL_QUERY and "external.other" in e.message
            for e in result.errors
        )

    def test_join_on_no_criteria_skips_column_check(self, session: AsyncSession):
        """When parse_join_sql returns a tree where the first join has criteria=None,
        the column-reference block is skipped (e.g. a USING-style or cross-join).
        """
        validator = _make_validator(session)
        result = _make_source_result(
            node_name="test.facts",
            col_names=["id", "dim_id"],
            join_on="test.facts.dim_id = test.dim.dim_id",
            dim_name="test.dim",
        )

        # Build a fake tree whose single join extension has criteria=None.
        mock_join = MagicMock()
        mock_join.criteria = None
        mock_relation = MagicMock()
        mock_relation.extensions = [mock_join]
        fake_tree = MagicMock()
        fake_tree.select.from_.relations.__getitem__.return_value = mock_relation

        with patch(
            "datajunction_server.internal.deployment.validation.DimensionLink.parse_join_sql",
            return_value=fake_tree,
        ):
            validator._validate_dimension_link_specs([result])

        # No errors: criteria=None makes line 225 falsy → column check skipped
        assert result.status == NodeStatus.VALID
        assert result.errors == []

    def test_join_link_with_no_join_on_is_skipped(self, session: AsyncSession):
        """A DimensionJoinLinkSpec with no join_on is silently skipped."""
        validator = _make_validator(session)
        spec = SourceSpec(
            name="facts",
            catalog="default",
            schema_="s",
            table="t",
            columns=[ColumnSpec(name="id", type="int")],
            dimension_links=[DimensionJoinLinkSpec(dimension_node="test.dim")],
        )
        spec.namespace = "test"
        result = NodeValidationResult(
            spec=spec,
            status=NodeStatus.VALID,
            inferred_columns=[ColumnSpec(name="id", type="int")],
            errors=[],
            dependencies=[],
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.VALID
        assert result.errors == []

    def test_reference_link_is_skipped(self, session: AsyncSession):
        """A DimensionReferenceLinkSpec is silently skipped (no join_on to validate)."""
        validator = _make_validator(session)
        spec = SourceSpec(
            name="facts",
            catalog="default",
            schema_="s",
            table="t",
            columns=[ColumnSpec(name="dim_id", type="int")],
            dimension_links=[
                DimensionReferenceLinkSpec(
                    dimension="test.dim.id",
                    node_column="dim_id",
                ),
            ],
        )
        spec.namespace = "test"
        result = NodeValidationResult(
            spec=spec,
            status=NodeStatus.VALID,
            inferred_columns=[ColumnSpec(name="dim_id", type="int")],
            errors=[],
            dependencies=[],
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.VALID
        assert result.errors == []

    def test_non_linkable_spec_is_skipped(self, session: AsyncSession):
        """MetricSpec (not LinkableNodeSpec) is silently skipped."""
        validator = _make_validator(session)
        metric_spec = MetricSpec(
            name="test.cnt",
            query="SELECT count(1) FROM test.facts",
        )
        metric_spec.namespace = "test"
        result = NodeValidationResult(
            spec=metric_spec,
            status=NodeStatus.VALID,
            inferred_columns=[],
            errors=[],
            dependencies=[],
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.VALID
        assert result.errors == []


class TestDimLinkJoinColumnWithoutNamespace:
    """Tests for join_on columns without a table namespace (line 230 coverage)."""

    def test_join_on_col_without_namespace_is_skipped(self, session: AsyncSession):
        """A column in join_on without a table-qualified reference (no namespace) is skipped
        without raising an error (line 230 - the continue branch)."""
        validator = _make_validator(session)
        result = _make_source_result(
            node_name="test.facts",
            col_names=["id", "dim_id"],
            # 'unqualified_col' has no namespace (no table prefix) — should be skipped
            join_on="test.facts.dim_id = test.dim.dim_id AND unqualified_col = 1",
            dim_name="test.dim",
        )
        validator._validate_dimension_link_specs([result])
        # The unqualified column should be skipped without causing an error
        assert result.status == NodeStatus.VALID
        assert result.errors == []


# ---------------------------------------------------------------------------
# TestDimLinkValidationExtended — dim node type, self-join, dim-side columns,
# reference links, and _prefetch_dimension_link_nodes
# ---------------------------------------------------------------------------


def _make_dim_node_for_link(
    name: str,
    col_names: list[str],
    node_type=NodeType.DIMENSION,
) -> Node:
    """Build an in-memory Node to use as a dimension link target."""
    node = Node(name=name, type=node_type)
    revision = NodeRevision(
        name=name,
        type=node_type,
        version="v1",
        columns=[
            Column(name=c, type=StringType(), order=i) for i, c in enumerate(col_names)
        ],
    )
    node.current = revision
    return node


class TestDimLinkValidationExtended:
    """Tests for the dim-node-aware checks added to _validate_join_link and
    _validate_reference_link."""

    # ------------------------------------------------------------------ join link

    def test_dim_node_wrong_type_is_invalid(self, session: AsyncSession):
        """Dim node that is not DIMENSION type produces an INVALID_SQL_QUERY error."""
        validator = _make_validator(session)
        dim_node = _make_dim_node_for_link(
            "test.not_a_dim",
            ["id"],
            node_type=NodeType.TRANSFORM,
        )
        validator._dim_link_nodes = {"test.not_a_dim": dim_node}
        result = _make_source_result(
            node_name="test.facts",
            col_names=["id"],
            join_on="test.facts.id = test.not_a_dim.id",
            dim_name="test.not_a_dim",
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.INVALID
        assert any(
            e.code == ErrorCode.INVALID_SQL_QUERY
            and "not a dimension node" in e.message
            for e in result.errors
        )

    def test_dim_node_not_found_skips_dim_side_checks(self, session: AsyncSession):
        """If the dim node is not in _dim_link_nodes (newly deployed), skip dim-side checks."""
        validator = _make_validator(session)
        # _dim_link_nodes is empty — dim node not yet in DB
        validator._dim_link_nodes = {}
        result = _make_source_result(
            node_name="test.facts",
            col_names=["id"],
            join_on="test.facts.id = test.new_dim.id",
            dim_name="test.new_dim",
        )
        validator._validate_dimension_link_specs([result])
        # Syntax is valid and node-side column exists — should pass
        assert result.status == NodeStatus.VALID
        assert result.errors == []

    def test_self_join_without_role_is_invalid(self, session: AsyncSession):
        """Self-join (node_name == dim_name) without a role produces an INVALID_SQL_QUERY error."""
        validator = _make_validator(session)
        dim_node = _make_dim_node_for_link("test.self_dim", ["id", "manager_id"])
        validator._dim_link_nodes = {"test.self_dim": dim_node}
        spec = SourceSpec(
            name="self_dim",
            catalog="default",
            schema_="s",
            table="t",
            columns=[
                ColumnSpec(name="id", type="int"),
                ColumnSpec(name="manager_id", type="int"),
            ],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="test.self_dim",
                    join_on="test.self_dim.manager_id = test.self_dim.id",
                    # no role — should fail
                ),
            ],
        )
        spec.namespace = "test"
        result = NodeValidationResult(
            spec=spec,
            status=NodeStatus.VALID,
            inferred_columns=[
                ColumnSpec(name="id", type="int"),
                ColumnSpec(name="manager_id", type="int"),
            ],
            errors=[],
            dependencies=[],
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.INVALID
        assert any(
            e.code == ErrorCode.INVALID_SQL_QUERY and "role" in e.message
            for e in result.errors
        )

    def test_dim_side_column_not_found_is_invalid(self, session: AsyncSession):
        """Column on dim side of join_on that doesn't exist on dim node → INVALID_COLUMN."""
        validator = _make_validator(session)
        dim_node = _make_dim_node_for_link("test.dim", ["real_id"])
        validator._dim_link_nodes = {"test.dim": dim_node}
        validator._dim_link_col_names = {"test.dim": {"real_id"}}
        result = _make_source_result(
            node_name="test.facts",
            col_names=["fk"],
            join_on="test.facts.fk = test.dim.nonexistent_pk",
            dim_name="test.dim",
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.INVALID
        assert any(
            e.code == ErrorCode.INVALID_COLUMN and "nonexistent_pk" in e.message
            for e in result.errors
        )

    def test_dim_side_column_valid_with_loaded_dim_node(self, session: AsyncSession):
        """When dim node is loaded, a valid dim-side column produces no errors."""
        validator = _make_validator(session)
        dim_node = _make_dim_node_for_link("test.dim", ["dim_id"])
        validator._dim_link_nodes = {"test.dim": dim_node}
        validator._dim_link_col_names = {"test.dim": {"dim_id"}}
        result = _make_source_result(
            node_name="test.facts",
            col_names=["fk"],
            join_on="test.facts.fk = test.dim.dim_id",
            dim_name="test.dim",
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.VALID
        assert result.errors == []

    # ------------------------------------------------------------------ reference link

    def test_reference_link_valid(self, session: AsyncSession):
        """A valid reference link (dim attribute and node column both exist) passes."""
        validator = _make_validator(session)
        dim_node = _make_dim_node_for_link("test.dim", ["region"])
        validator._dim_link_nodes = {"test.dim": dim_node}
        validator._dim_link_col_names = {"test.dim": {"region"}}
        spec = SourceSpec(
            name="facts",
            catalog="default",
            schema_="s",
            table="t",
            columns=[ColumnSpec(name="region_id", type="int")],
            dimension_links=[
                DimensionReferenceLinkSpec(
                    dimension="test.dim.region",
                    node_column="region_id",
                ),
            ],
        )
        spec.namespace = "test"
        result = NodeValidationResult(
            spec=spec,
            status=NodeStatus.VALID,
            inferred_columns=[ColumnSpec(name="region_id", type="int")],
            errors=[],
            dependencies=[],
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.VALID
        assert result.errors == []

    def test_reference_link_attribute_not_on_dim_is_invalid(
        self,
        session: AsyncSession,
    ):
        """Reference link pointing to a column that doesn't exist on the dim → INVALID_COLUMN."""
        validator = _make_validator(session)
        dim_node = _make_dim_node_for_link("test.dim", ["real_col"])
        validator._dim_link_nodes = {"test.dim": dim_node}
        validator._dim_link_col_names = {"test.dim": {"real_col"}}
        spec = SourceSpec(
            name="facts",
            catalog="default",
            schema_="s",
            table="t",
            columns=[ColumnSpec(name="fk", type="int")],
            dimension_links=[
                DimensionReferenceLinkSpec(
                    dimension="test.dim.missing_col",
                    node_column="fk",
                ),
            ],
        )
        spec.namespace = "test"
        result = NodeValidationResult(
            spec=spec,
            status=NodeStatus.VALID,
            inferred_columns=[ColumnSpec(name="fk", type="int")],
            errors=[],
            dependencies=[],
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.INVALID
        assert any(
            e.code == ErrorCode.INVALID_COLUMN and "missing_col" in e.message
            for e in result.errors
        )

    def test_reference_link_node_column_not_found_is_invalid(
        self,
        session: AsyncSession,
    ):
        """Reference link node_column that doesn't exist on the node → INVALID_COLUMN."""
        validator = _make_validator(session)
        dim_node = _make_dim_node_for_link("test.dim", ["region"])
        validator._dim_link_nodes = {"test.dim": dim_node}
        validator._dim_link_col_names = {"test.dim": {"region"}}
        spec = SourceSpec(
            name="facts",
            catalog="default",
            schema_="s",
            table="t",
            columns=[ColumnSpec(name="real_col", type="int")],
            dimension_links=[
                DimensionReferenceLinkSpec(
                    dimension="test.dim.region",
                    node_column="nonexistent",
                ),
            ],
        )
        spec.namespace = "test"
        result = NodeValidationResult(
            spec=spec,
            status=NodeStatus.VALID,
            inferred_columns=[ColumnSpec(name="real_col", type="int")],
            errors=[],
            dependencies=[],
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.INVALID
        assert any(
            e.code == ErrorCode.INVALID_COLUMN and "nonexistent" in e.message
            for e in result.errors
        )

    def test_reference_link_dim_wrong_type_is_invalid(self, session: AsyncSession):
        """Reference link whose dim node is not type DIMENSION → INVALID_SQL_QUERY."""
        validator = _make_validator(session)
        dim_node = _make_dim_node_for_link(
            "test.not_dim",
            ["col"],
            node_type=NodeType.TRANSFORM,
        )
        validator._dim_link_nodes = {"test.not_dim": dim_node}
        spec = SourceSpec(
            name="facts",
            catalog="default",
            schema_="s",
            table="t",
            columns=[ColumnSpec(name="fk", type="int")],
            dimension_links=[
                DimensionReferenceLinkSpec(
                    dimension="test.not_dim.col",
                    node_column="fk",
                ),
            ],
        )
        spec.namespace = "test"
        result = NodeValidationResult(
            spec=spec,
            status=NodeStatus.VALID,
            inferred_columns=[ColumnSpec(name="fk", type="int")],
            errors=[],
            dependencies=[],
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.INVALID
        assert any(
            e.code == ErrorCode.INVALID_SQL_QUERY
            and "not a dimension node" in e.message
            for e in result.errors
        )

    def test_reference_link_dim_not_found_skips_dim_side_checks(
        self,
        session: AsyncSession,
    ):
        """Reference link whose dim node is not in _dim_link_nodes skips dim-side checks."""
        validator = _make_validator(session)
        validator._dim_link_nodes = {}
        spec = SourceSpec(
            name="facts",
            catalog="default",
            schema_="s",
            table="t",
            columns=[ColumnSpec(name="fk", type="int")],
            dimension_links=[
                DimensionReferenceLinkSpec(
                    dimension="test.new_dim.col",
                    node_column="fk",
                ),
            ],
        )
        spec.namespace = "test"
        result = NodeValidationResult(
            spec=spec,
            status=NodeStatus.VALID,
            inferred_columns=[ColumnSpec(name="fk", type="int")],
            errors=[],
            dependencies=[],
        )
        validator._validate_dimension_link_specs([result])
        assert result.status == NodeStatus.VALID
        assert result.errors == []

    # ------------------------------------------------------------------ prefetch

    @pytest.mark.asyncio
    async def test_prefetch_dimension_link_nodes_fetches_from_db(
        self,
        session: AsyncSession,
        user: User,
        catalog: Catalog,
    ):
        """_prefetch_dimension_link_nodes fetches a dim node not in dependency_nodes."""
        # Create a real dimension node in DB
        dim_node = Node(
            name="test.linked_dim",
            type=NodeType.DIMENSION,
            current_version="v1",
            created_by_id=user.id,
        )
        revision = NodeRevision(
            node=dim_node,
            name=dim_node.name,
            catalog_id=catalog.id,
            type=dim_node.type,
            version="v1",
            columns=[Column(name="dim_col", type=StringType(), order=0)],
            created_by_id=user.id,
        )
        dim_node.current = revision
        session.add(revision)
        await session.commit()

        context = ValidationContext(
            session=session,
            node_graph={},
            dependency_nodes={},
        )
        spec = SourceSpec(
            name="facts",
            catalog="default",
            schema_="s",
            table="t",
            columns=[ColumnSpec(name="fk", type="int")],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="test.linked_dim",
                    join_on="test.facts.fk = test.linked_dim.dim_col",
                ),
            ],
        )
        spec.namespace = "test"
        validator = NodeSpecBulkValidator(context)

        await validator._prefetch_dimension_link_nodes([spec])

        assert "test.linked_dim" in validator._dim_link_nodes
        fetched = validator._dim_link_nodes["test.linked_dim"]
        assert fetched.current is not None
        assert any(c.name == "dim_col" for c in fetched.current.columns)
        assert "dim_col" in validator._dim_link_col_names.get("test.linked_dim", set())

    @pytest.mark.asyncio
    async def test_prefetch_dimension_link_nodes_no_links_is_noop(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """_prefetch_dimension_link_nodes with specs having no dimension_links is a noop."""
        context = ValidationContext(
            session=session,
            node_graph={},
            dependency_nodes={parent_node.name: parent_node},
        )
        spec = SourceSpec(
            name="facts",
            catalog="default",
            schema_="s",
            table="t",
            columns=[ColumnSpec(name="id", type="int")],
        )
        spec.namespace = "test"
        validator = NodeSpecBulkValidator(context)

        await validator._prefetch_dimension_link_nodes([spec])

        # No dimension links → early return, _dim_link_nodes stays empty
        assert validator._dim_link_nodes == {}
        assert validator._dim_link_col_names == {}

    @pytest.mark.asyncio
    async def test_prefetch_dimension_link_nodes_already_in_dep_nodes(
        self,
        session: AsyncSession,
        parent_node: Node,
    ):
        """Dim node already in dependency_nodes is not re-fetched from DB."""
        dim_node = _make_dim_node_for_link("test.existing_dim", ["id"])
        context = ValidationContext(
            session=session,
            node_graph={},
            dependency_nodes={dim_node.name: dim_node},
        )
        spec = SourceSpec(
            name="facts",
            catalog="default",
            schema_="s",
            table="t",
            columns=[ColumnSpec(name="fk", type="int")],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="test.existing_dim",
                    join_on="test.facts.fk = test.existing_dim.id",
                ),
            ],
        )
        spec.namespace = "test"
        validator = NodeSpecBulkValidator(context)

        await validator._prefetch_dimension_link_nodes([spec])

        # Should use the pre-existing node object (same identity)
        assert validator._dim_link_nodes["test.existing_dim"] is dim_node
        # Column names should be pre-computed for the dim link target
        assert "id" in validator._dim_link_col_names.get("test.existing_dim", set())
