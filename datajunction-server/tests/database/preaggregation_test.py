"""Tests for PreAggregation database schema."""

import pytest
import pytest_asyncio
from unittest.mock import MagicMock

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.preaggregation import (
    PreAggregation,
    VALID_PREAGG_STRATEGIES,
    compute_grain_group_hash,
    compute_expression_hash,
    get_measure_expr_hashes,
    compute_preagg_hash,
)
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.models.decompose import AggregationRule, PreAggMeasure
from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.models.node_type import NodeType


def make_measure(
    name: str,
    expression: str,
    aggregation: str = "SUM",
    merge: str = "SUM",
):
    """Helper to create a measure dict with all required fields."""
    return PreAggMeasure(
        name=name,
        expression=expression,
        expr_hash=compute_expression_hash(expression),
        aggregation=aggregation,
        merge=merge,
        rule=AggregationRule(type="full", level=None),
    )


class TestComputeExpressionHash:
    """Tests for compute_expression_hash function."""

    def test_basic_hash(self):
        """Test basic expression hash computation."""
        hash1 = compute_expression_hash("price * quantity")
        assert isinstance(hash1, str)
        assert len(hash1) == 12  # Truncated MD5

    def test_same_expression_same_hash(self):
        """Test that same expression produces same hash."""
        hash1 = compute_expression_hash("price * quantity")
        hash2 = compute_expression_hash("price * quantity")
        assert hash1 == hash2

    def test_different_expression_different_hash(self):
        """Test that different expressions produce different hashes."""
        hash1 = compute_expression_hash("price * quantity")
        hash2 = compute_expression_hash("price * quantity * (1 - discount)")
        assert hash1 != hash2

    def test_whitespace_normalized(self):
        """Test that whitespace is normalized for consistent hashing."""
        hash1 = compute_expression_hash("price * quantity")
        hash2 = compute_expression_hash("price  *  quantity")
        hash3 = compute_expression_hash("price\n*\nquantity")
        assert hash1 == hash2 == hash3


class TestGetMeasureExprHashes:
    """Tests for get_measure_expr_hashes function."""

    def test_extracts_hashes(self):
        """Test extracting expr_hashes from measures."""
        measures = [
            PreAggMeasure(
                name="sum_revenue",
                expression="revenue",
                aggregation="SUM",
                rule=AggregationRule(type="full"),
                expr_hash="abc123",
            ),
            PreAggMeasure(
                name="count_orders",
                expression="1",
                aggregation="COUNT",
                rule=AggregationRule(type="full"),
                expr_hash="def456",
            ),
        ]
        hashes = get_measure_expr_hashes(measures)
        assert hashes == {"abc123", "def456"}

    def test_handles_missing_hash(self):
        """Test that missing expr_hash is handled."""
        measures = [
            PreAggMeasure(
                name="sum_revenue",
                expression="revenue",
                aggregation="SUM",
                rule=AggregationRule(type="full"),
                expr_hash="abc123",
            ),
            PreAggMeasure(
                name="count_orders",
                expression="1",
                aggregation="COUNT",
                rule=AggregationRule(type="full"),
                expr_hash=None,  # No expr_hash
            ),
        ]
        hashes = get_measure_expr_hashes(measures)
        assert hashes == {"abc123"}

    def test_empty_list(self):
        """Test with empty measures list."""
        hashes = get_measure_expr_hashes([])
        assert hashes == set()


class TestComputeGrainGroupHash:
    """Tests for compute_grain_group_hash function."""

    def test_basic_hash(self):
        """Test basic hash computation."""
        hash1 = compute_grain_group_hash(
            node_revision_id=123,
            grain_columns=[
                "default.date_dim.date_id",
                "default.customer_dim.customer_id",
            ],
        )
        assert isinstance(hash1, str)
        assert len(hash1) == 32  # MD5 hex digest length

    def test_same_inputs_same_hash(self):
        """Test that same inputs produce same hash."""
        hash1 = compute_grain_group_hash(
            node_revision_id=123,
            grain_columns=[
                "default.date_dim.date_id",
                "default.customer_dim.customer_id",
            ],
        )
        hash2 = compute_grain_group_hash(
            node_revision_id=123,
            grain_columns=[
                "default.date_dim.date_id",
                "default.customer_dim.customer_id",
            ],
        )
        assert hash1 == hash2

    def test_different_node_revision_different_hash(self):
        """Test that different node_revision_id produces different hash."""
        hash1 = compute_grain_group_hash(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
        )
        hash2 = compute_grain_group_hash(
            node_revision_id=456,
            grain_columns=["default.date_dim.date_id"],
        )
        assert hash1 != hash2

    def test_different_grain_different_hash(self):
        """Test that different grain_columns produces different hash."""
        hash1 = compute_grain_group_hash(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
        )
        hash2 = compute_grain_group_hash(
            node_revision_id=123,
            grain_columns=[
                "default.date_dim.date_id",
                "default.customer_dim.customer_id",
            ],
        )
        assert hash1 != hash2

    def test_grain_order_does_not_matter(self):
        """Test that grain column order doesn't affect hash (sorted internally)."""
        hash1 = compute_grain_group_hash(
            node_revision_id=123,
            grain_columns=[
                "default.date_dim.date_id",
                "default.customer_dim.customer_id",
            ],
        )
        hash2 = compute_grain_group_hash(
            node_revision_id=123,
            grain_columns=[
                "default.customer_dim.customer_id",
                "default.date_dim.date_id",
            ],
        )
        assert hash1 == hash2

    def test_empty_grain_columns(self):
        """Test with empty grain columns."""
        hash1 = compute_grain_group_hash(
            node_revision_id=123,
            grain_columns=[],
        )
        assert isinstance(hash1, str)
        assert len(hash1) == 32


class TestValidPreAggStrategies:
    """Tests for VALID_PREAGG_STRATEGIES constant."""

    def test_valid_strategies(self):
        """Test that VALID_PREAGG_STRATEGIES contains expected values."""
        assert MaterializationStrategy.FULL in VALID_PREAGG_STRATEGIES
        assert MaterializationStrategy.INCREMENTAL_TIME in VALID_PREAGG_STRATEGIES

    def test_invalid_strategies_excluded(self):
        """Test that invalid strategies are not in VALID_PREAGG_STRATEGIES."""
        assert MaterializationStrategy.SNAPSHOT not in VALID_PREAGG_STRATEGIES
        assert MaterializationStrategy.SNAPSHOT_PARTITION not in VALID_PREAGG_STRATEGIES
        assert MaterializationStrategy.VIEW not in VALID_PREAGG_STRATEGIES


class TestPreAggregationProperties:
    """Tests for PreAggregation model properties."""

    def test_status_pending_when_no_availability(self):
        """Test that status is 'pending' when no availability."""
        measures = [make_measure("sum_revenue", "revenue")]
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=measures,
            grain_group_hash="abc123",
            preagg_hash=compute_preagg_hash(
                123,
                ["default.date_dim.date_id"],
                measures,
            ),
        )
        pre_agg.availability = None
        assert pre_agg.status == "pending"

    def test_status_active_when_has_availability(self):
        """Test that status is 'active' when has availability."""
        measures = [make_measure("sum_revenue", "revenue")]
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=measures,
            grain_group_hash="abc123",
            preagg_hash=compute_preagg_hash(
                123,
                ["default.date_dim.date_id"],
                measures,
            ),
        )
        # Mock availability
        mock_availability = MagicMock()
        mock_availability.catalog = "analytics"
        mock_availability.schema_ = "materialized"
        mock_availability.table = "preagg_123"
        mock_availability.max_temporal_partition = ["2024", "01", "15"]
        pre_agg.availability = mock_availability

        assert pre_agg.status == "active"

    def test_materialized_table_ref_none_when_no_availability(self):
        """Test materialized_table_ref is None when no availability."""
        measures = [make_measure("sum_revenue", "revenue")]
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=measures,
            grain_group_hash="abc123",
            preagg_hash=compute_preagg_hash(
                123,
                ["default.date_dim.date_id"],
                measures,
            ),
        )
        pre_agg.availability = None
        assert pre_agg.materialized_table_ref is None

    def test_materialized_table_ref_with_all_parts(self):
        """Test materialized_table_ref with catalog, schema, table."""
        measures = [make_measure("sum_revenue", "revenue")]
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=measures,
            grain_group_hash="abc123",
            preagg_hash=compute_preagg_hash(
                123,
                ["default.date_dim.date_id"],
                measures,
            ),
        )
        mock_availability = MagicMock()
        mock_availability.catalog = "analytics"
        mock_availability.schema_ = "materialized"
        mock_availability.table = "preagg_123"
        pre_agg.availability = mock_availability

        assert pre_agg.materialized_table_ref == "analytics.materialized.preagg_123"

    def test_materialized_table_ref_without_catalog(self):
        """Test materialized_table_ref without catalog."""
        measures = [make_measure("sum_revenue", "revenue")]
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=measures,
            grain_group_hash="abc123",
            preagg_hash=compute_preagg_hash(
                123,
                ["default.date_dim.date_id"],
                measures,
            ),
        )
        mock_availability = MagicMock()
        mock_availability.catalog = None
        mock_availability.schema_ = "materialized"
        mock_availability.table = "preagg_123"
        pre_agg.availability = mock_availability

        assert pre_agg.materialized_table_ref == "materialized.preagg_123"

    def test_max_partition_none_when_no_availability(self):
        """Test max_partition is None when no availability."""
        measures = [make_measure("sum_revenue", "revenue")]
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=measures,
            grain_group_hash="abc123",
            preagg_hash=compute_preagg_hash(
                123,
                ["default.date_dim.date_id"],
                measures,
            ),
        )
        pre_agg.availability = None
        assert pre_agg.max_partition is None

    def test_max_partition_from_availability(self):
        """Test max_partition returns availability's max_temporal_partition."""
        measures = [make_measure("sum_revenue", "revenue")]
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=measures,
            grain_group_hash="abc123",
            preagg_hash=compute_preagg_hash(
                123,
                ["default.date_dim.date_id"],
                measures,
            ),
        )
        mock_availability = MagicMock()
        mock_availability.max_temporal_partition = ["2024", "01", "15"]
        pre_agg.availability = mock_availability

        assert pre_agg.max_partition == ["2024", "01", "15"]


class TestComputePreAggHash:
    """Tests for compute_preagg_hash function."""

    def test_basic_hash(self):
        """Test basic hash computation."""
        measures = [make_measure("sum_revenue", "revenue")]
        hash1 = compute_preagg_hash(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=measures,
        )
        assert isinstance(hash1, str)
        assert len(hash1) == 8  # Truncated to 8 chars

    def test_same_inputs_same_hash(self):
        """Test that same inputs produce same hash."""
        measures = [make_measure("sum_revenue", "revenue")]
        hash1 = compute_preagg_hash(123, ["default.date_dim.date_id"], measures)
        hash2 = compute_preagg_hash(123, ["default.date_dim.date_id"], measures)
        assert hash1 == hash2

    def test_different_node_revision_different_hash(self):
        """Test that different node_revision_id produces different hash."""
        measures = [make_measure("sum_revenue", "revenue")]
        hash1 = compute_preagg_hash(123, ["default.date_dim.date_id"], measures)
        hash2 = compute_preagg_hash(456, ["default.date_dim.date_id"], measures)
        assert hash1 != hash2

    def test_different_measures_different_hash(self):
        """Test that different measures produce different hash."""
        measures1 = [make_measure("sum_revenue", "revenue")]
        measures2 = [make_measure("count_orders", "1", "COUNT")]
        hash1 = compute_preagg_hash(123, ["default.date_dim.date_id"], measures1)
        hash2 = compute_preagg_hash(123, ["default.date_dim.date_id"], measures2)
        assert hash1 != hash2

    def test_grain_order_does_not_matter(self):
        """Test that grain column order doesn't affect hash."""
        measures = [make_measure("sum_revenue", "revenue")]
        hash1 = compute_preagg_hash(123, ["a.b.c", "d.e.f"], measures)
        hash2 = compute_preagg_hash(123, ["d.e.f", "a.b.c"], measures)
        assert hash1 == hash2


@pytest_asyncio.fixture
async def minimal_node_revision(session):
    """Create a minimal node revision for testing PreAggregation."""
    # Create user
    user = User(
        username="test_preagg_user",
        email="test_preagg@test.com",
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(user)
    await session.commit()

    # Create node
    node = Node(
        name="test.preagg.source_node",
        type=NodeType.SOURCE,
        created_by_id=user.id,
        namespace="test.preagg",
    )
    session.add(node)
    await session.commit()

    # Create node revision
    node_revision = NodeRevision(
        name="test.preagg.source_node",
        type=NodeType.SOURCE,
        node_id=node.id,
        created_by_id=user.id,
        version="v1.0",
    )
    session.add(node_revision)
    await session.commit()

    return node_revision


@pytest.mark.asyncio
class TestPreAggregationDBMethods:
    """Integration tests for PreAggregation async database methods."""

    async def test_get_by_grain_group_hash(
        self,
        session,
        minimal_node_revision,
    ):
        """Test get_by_grain_group_hash returns matching pre-aggs."""
        grain_columns = ["test.dim.col1"]
        grain_hash = compute_grain_group_hash(minimal_node_revision.id, grain_columns)

        # Create 2 pre-aggs with same grain hash
        measures1 = [make_measure("sum_a", "a")]
        preagg1 = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=grain_columns,
            measures=measures1,
            columns=[],
            sql="SELECT a FROM t GROUP BY col1",
            grain_group_hash=grain_hash,
            preagg_hash=compute_preagg_hash(
                minimal_node_revision.id,
                grain_columns,
                measures1,
            ),
        )
        measures2 = [make_measure("sum_b", "b")]
        preagg2 = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=grain_columns,
            measures=measures2,
            columns=[],
            sql="SELECT b FROM t GROUP BY col1",
            grain_group_hash=grain_hash,
            preagg_hash=compute_preagg_hash(
                minimal_node_revision.id,
                grain_columns,
                measures2,
            ),
        )
        # Different grain = different hash
        other_hash = compute_grain_group_hash(minimal_node_revision.id, ["other.col"])
        measures3 = [make_measure("sum_c", "c")]
        preagg3 = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=["other.col"],
            measures=measures3,
            columns=[],
            sql="SELECT c FROM t GROUP BY other",
            grain_group_hash=other_hash,
            preagg_hash=compute_preagg_hash(
                minimal_node_revision.id,
                ["other.col"],
                measures3,
            ),
        )

        session.add_all([preagg1, preagg2, preagg3])
        await session.flush()

        # Should find exactly 2 with matching hash
        results = await PreAggregation.get_by_grain_group_hash(
            session,
            grain_hash,
        )
        assert len(results) == 2

        # Should find 1 with other hash
        results = await PreAggregation.get_by_grain_group_hash(
            session,
            other_hash,
        )
        assert len(results) == 1

        # Should find 0 with non-existent hash
        results = await PreAggregation.get_by_grain_group_hash(
            session,
            "nonexistent",
        )
        assert len(results) == 0

    async def test_get_by_id(self, session, minimal_node_revision):
        """Test get_by_id returns pre-agg when found, None when not."""
        measures = [make_measure("sum_x", "x")]
        preagg = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=["test.col"],
            measures=measures,
            columns=[],
            sql="SELECT x FROM t",
            grain_group_hash="hash123",
            preagg_hash=compute_preagg_hash(
                minimal_node_revision.id,
                ["test.col"],
                measures,
            ),
        )
        session.add(preagg)
        await session.flush()

        # Found
        result = await PreAggregation.get_by_id(session, preagg.id)
        assert result is not None
        assert result.id == preagg.id

        # Not found
        result = await PreAggregation.get_by_id(session, 999999)
        assert result is None

    async def test_find_matching_with_superset(
        self,
        session,
        minimal_node_revision,
    ):
        """Test find_matching returns pre-agg with superset of measures."""
        grain_columns = ["test.dim.col"]

        measures = [
            make_measure("sum_price", "price"),
            make_measure("count_orders", "1", "COUNT"),
        ]
        preagg = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=grain_columns,
            measures=measures,
            columns=[],
            sql="SELECT price, count(1) FROM t GROUP BY col",
            grain_group_hash=compute_grain_group_hash(
                minimal_node_revision.id,
                grain_columns,
            ),
            preagg_hash=compute_preagg_hash(
                minimal_node_revision.id,
                grain_columns,
                measures,
            ),
        )
        session.add(preagg)
        await session.flush()

        # Request subset - should match
        result = await PreAggregation.find_matching(
            session,
            node_revision_id=minimal_node_revision.id,
            grain_columns=grain_columns,
            measure_expr_hashes={compute_expression_hash("price")},
        )
        assert result is not None
        assert result.id == preagg.id

    async def test_find_matching_no_match(self, session, minimal_node_revision):
        """Test find_matching returns None when no candidate has superset."""
        grain_columns = ["test.dim.col"]

        measures = [make_measure("sum_price", "price")]
        preagg = PreAggregation(
            node_revision_id=minimal_node_revision.id,
            grain_columns=grain_columns,
            measures=measures,
            columns=[],
            sql="SELECT price FROM t GROUP BY col",
            grain_group_hash=compute_grain_group_hash(
                minimal_node_revision.id,
                grain_columns,
            ),
            preagg_hash=compute_preagg_hash(
                minimal_node_revision.id,
                grain_columns,
                measures,
            ),
        )
        session.add(preagg)
        await session.flush()

        # Request non-existent measure - should not match
        result = await PreAggregation.find_matching(
            session,
            node_revision_id=minimal_node_revision.id,
            grain_columns=grain_columns,
            measure_expr_hashes={compute_expression_hash("nonexistent")},
        )
        assert result is None

    async def test_find_matching_no_candidates(
        self,
        session,
        minimal_node_revision,
    ):
        """Test find_matching returns None when no candidates exist."""
        result = await PreAggregation.find_matching(
            session,
            node_revision_id=minimal_node_revision.id,
            grain_columns=["completely.different.grain"],
            measure_expr_hashes={compute_expression_hash("anything")},
        )
        assert result is None
