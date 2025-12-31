"""Tests for PreAggregation database schema."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from datajunction_server.database.preaggregation import (
    PreAggregation,
    VALID_PREAGG_STRATEGIES,
    compute_grain_group_hash,
    compute_expression_hash,
    get_measure_expr_hashes,
)
from datajunction_server.models.materialization import MaterializationStrategy


def make_measure(
    name: str,
    expression: str,
    aggregation: str = "SUM",
    merge: str = "SUM",
):
    """Helper to create a measure dict with all required fields."""
    return {
        "name": name,
        "expression": expression,
        "expr_hash": compute_expression_hash(expression),
        "aggregation": aggregation,
        "merge": merge,
        "rule": {"type": "full", "level": None},
    }


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
            {"name": "sum_revenue", "expr_hash": "abc123"},
            {"name": "count_orders", "expr_hash": "def456"},
        ]
        hashes = get_measure_expr_hashes(measures)
        assert hashes == {"abc123", "def456"}

    def test_handles_missing_hash(self):
        """Test that missing expr_hash is handled."""
        measures = [
            {"name": "sum_revenue", "expr_hash": "abc123"},
            {"name": "count_orders"},  # No expr_hash
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
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=[make_measure("sum_revenue", "revenue")],
            grain_group_hash="abc123",
        )
        pre_agg.availability = None
        assert pre_agg.status == "pending"

    def test_status_active_when_has_availability(self):
        """Test that status is 'active' when has availability."""
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=[make_measure("sum_revenue", "revenue")],
            grain_group_hash="abc123",
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
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=[make_measure("sum_revenue", "revenue")],
            grain_group_hash="abc123",
        )
        pre_agg.availability = None
        assert pre_agg.materialized_table_ref is None

    def test_materialized_table_ref_with_all_parts(self):
        """Test materialized_table_ref with catalog, schema, table."""
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=[make_measure("sum_revenue", "revenue")],
            grain_group_hash="abc123",
        )
        mock_availability = MagicMock()
        mock_availability.catalog = "analytics"
        mock_availability.schema_ = "materialized"
        mock_availability.table = "preagg_123"
        pre_agg.availability = mock_availability

        assert pre_agg.materialized_table_ref == "analytics.materialized.preagg_123"

    def test_materialized_table_ref_without_catalog(self):
        """Test materialized_table_ref without catalog."""
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=[make_measure("sum_revenue", "revenue")],
            grain_group_hash="abc123",
        )
        mock_availability = MagicMock()
        mock_availability.catalog = None
        mock_availability.schema_ = "materialized"
        mock_availability.table = "preagg_123"
        pre_agg.availability = mock_availability

        assert pre_agg.materialized_table_ref == "materialized.preagg_123"

    def test_max_partition_none_when_no_availability(self):
        """Test max_partition is None when no availability."""
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=[make_measure("sum_revenue", "revenue")],
            grain_group_hash="abc123",
        )
        pre_agg.availability = None
        assert pre_agg.max_partition is None

    def test_max_partition_from_availability(self):
        """Test max_partition returns availability's max_temporal_partition."""
        pre_agg = PreAggregation(
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=[make_measure("sum_revenue", "revenue")],
            grain_group_hash="abc123",
        )
        mock_availability = MagicMock()
        mock_availability.max_temporal_partition = ["2024", "01", "15"]
        pre_agg.availability = mock_availability

        assert pre_agg.max_partition == ["2024", "01", "15"]


@pytest.mark.asyncio
class TestPreAggregationGetOrCreate:
    """Tests for PreAggregation.get_or_create class method."""

    async def test_create_new_when_no_candidates(self):
        """Test creating new pre-agg when no candidates exist."""
        mock_session = AsyncMock()
        measures = [
            make_measure("sum_revenue", "revenue"),
            make_measure("count_orders", "1", "COUNT", "SUM"),
        ]

        # Mock get_by_grain_group_hash to return empty list
        with patch.object(
            PreAggregation,
            "get_by_grain_group_hash",
            new_callable=AsyncMock,
            return_value=[],
        ):
            result = await PreAggregation.get_or_create(
                session=mock_session,
                node_revision_id=123,
                grain_columns=["default.date_dim.date_id"],
                measures=measures,
                strategy=MaterializationStrategy.FULL,
            )

            # Should create a new pre-agg
            assert result.node_revision_id == 123
            assert result.grain_columns == ["default.date_dim.date_id"]
            assert len(result.measures) == 2
            assert result.strategy == MaterializationStrategy.FULL
            mock_session.add.assert_called_once()

    async def test_reuse_existing_with_superset_measures(self):
        """Test reusing existing pre-agg when it has superset of needed measures (by expr_hash)."""
        mock_session = AsyncMock()

        # Create existing pre-agg with superset of measures
        existing_measures = [
            make_measure("sum_revenue", "revenue"),
            make_measure("count_orders", "1", "COUNT", "SUM"),
            make_measure("avg_price", "price"),
        ]
        existing_pre_agg = PreAggregation(
            id=42,
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=existing_measures,
            grain_group_hash="abc123",
        )
        existing_pre_agg.availability = MagicMock()  # Has availability (materialized)

        # Request subset of measures
        requested_measures = [
            make_measure("sum_revenue", "revenue"),
            make_measure("count_orders", "1", "COUNT", "SUM"),
        ]

        with patch.object(
            PreAggregation,
            "get_by_grain_group_hash",
            new_callable=AsyncMock,
            return_value=[existing_pre_agg],
        ):
            result = await PreAggregation.get_or_create(
                session=mock_session,
                node_revision_id=123,
                grain_columns=["default.date_dim.date_id"],
                measures=requested_measures,
            )

            # Should return existing pre-agg
            assert result.id == 42
            assert len(result.measures) == 3  # All 3 measures
            mock_session.add.assert_not_called()

    async def test_extend_pending_preagg_measures(self):
        """Test extending measures on pending (not materialized) pre-agg."""
        mock_session = AsyncMock()

        # Create existing pre-agg without availability (pending)
        existing_measures = [make_measure("sum_revenue", "revenue")]
        existing_pre_agg = PreAggregation(
            id=42,
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=existing_measures,
            grain_group_hash="abc123",
        )
        existing_pre_agg.availability = None  # Not materialized yet

        # Request additional measure
        requested_measures = [
            make_measure("sum_revenue", "revenue"),
            make_measure("count_orders", "1", "COUNT", "SUM"),
        ]

        with patch.object(
            PreAggregation,
            "get_by_grain_group_hash",
            new_callable=AsyncMock,
            return_value=[existing_pre_agg],
        ):
            result = await PreAggregation.get_or_create(
                session=mock_session,
                node_revision_id=123,
                grain_columns=["default.date_dim.date_id"],
                measures=requested_measures,
            )

            # Should extend existing pre-agg's measures
            assert result.id == 42
            assert len(result.measures) == 2  # Both measures now
            measure_names = {m["name"] for m in result.measures}
            assert measure_names == {"sum_revenue", "count_orders"}
            mock_session.add.assert_not_called()

    async def test_create_new_when_materialized_lacks_measures(self):
        """Test creating new pre-agg when materialized one lacks needed measures."""
        mock_session = AsyncMock()

        # Create existing pre-agg that's materialized but lacks measures
        existing_measures = [make_measure("sum_revenue", "revenue")]
        existing_pre_agg = PreAggregation(
            id=42,
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=existing_measures,
            grain_group_hash="abc123",
        )
        existing_pre_agg.availability = MagicMock()  # Has availability (materialized)

        # Request additional measure that doesn't exist
        requested_measures = [
            make_measure("sum_revenue", "revenue"),
            make_measure("count_orders", "1", "COUNT", "SUM"),  # Not in existing
        ]

        with patch.object(
            PreAggregation,
            "get_by_grain_group_hash",
            new_callable=AsyncMock,
            return_value=[existing_pre_agg],
        ):
            result = await PreAggregation.get_or_create(
                session=mock_session,
                node_revision_id=123,
                grain_columns=["default.date_dim.date_id"],
                measures=requested_measures,
            )

            # Should create new pre-agg (can't extend materialized one)
            assert result.id is None  # New, not persisted yet
            assert len(result.measures) == 2
            mock_session.add.assert_called_once()

    async def test_picks_first_superset_match(self):
        """Test that first pre-agg with superset measures is returned."""
        mock_session = AsyncMock()

        # Create multiple candidates
        candidate1_measures = [make_measure("sum_revenue", "revenue")]
        candidate1 = PreAggregation(
            id=1,
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=candidate1_measures,  # Not a superset
            grain_group_hash="abc123",
        )
        candidate1.availability = MagicMock()

        candidate2_measures = [
            make_measure("sum_revenue", "revenue"),
            make_measure("count_orders", "1", "COUNT", "SUM"),
            make_measure("extra", "extra_col"),
        ]
        candidate2 = PreAggregation(
            id=2,
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=candidate2_measures,  # Superset!
            grain_group_hash="abc123",
        )
        candidate2.availability = MagicMock()

        requested_measures = [
            make_measure("sum_revenue", "revenue"),
            make_measure("count_orders", "1", "COUNT", "SUM"),
        ]

        with patch.object(
            PreAggregation,
            "get_by_grain_group_hash",
            new_callable=AsyncMock,
            return_value=[candidate1, candidate2],
        ):
            result = await PreAggregation.get_or_create(
                session=mock_session,
                node_revision_id=123,
                grain_columns=["default.date_dim.date_id"],
                measures=requested_measures,
            )

            # Should return candidate2 (first superset match)
            assert result.id == 2

    async def test_matching_by_expr_hash_not_name(self):
        """Test that matching is done by expr_hash, not by name."""
        mock_session = AsyncMock()

        # Existing pre-agg has "sum_revenue" with expression "revenue"
        existing_measures = [
            {
                "name": "sum_revenue",
                "expression": "revenue",
                "expr_hash": compute_expression_hash("revenue"),
                "aggregation": "SUM",
                "merge": "SUM",
                "rule": {"type": "full"},
            },
        ]
        existing_pre_agg = PreAggregation(
            id=42,
            node_revision_id=123,
            grain_columns=["default.date_dim.date_id"],
            measures=existing_measures,
            grain_group_hash="abc123",
        )
        existing_pre_agg.availability = MagicMock()

        # Request same name but DIFFERENT expression - should NOT match!
        requested_measures = [
            {
                "name": "sum_revenue",
                "expression": "revenue * (1 - discount)",
                "expr_hash": compute_expression_hash("revenue * (1 - discount)"),
                "aggregation": "SUM",
                "merge": "SUM",
                "rule": {"type": "full"},
            },
        ]

        with patch.object(
            PreAggregation,
            "get_by_grain_group_hash",
            new_callable=AsyncMock,
            return_value=[existing_pre_agg],
        ):
            result = await PreAggregation.get_or_create(
                session=mock_session,
                node_revision_id=123,
                grain_columns=["default.date_dim.date_id"],
                measures=requested_measures,
            )

            # Should create new pre-agg because expr_hash is different
            assert result.id is None  # New, not persisted
            mock_session.add.assert_called_once()
