"""
Unit tests for namespace diff helper functions.

These test the internal comparison functions directly without going through
the full API, which makes it easier to cover edge cases.
"""

from unittest.mock import MagicMock

from datajunction_server.internal.namespaces import (
    _get_propagation_reason,
    _compare_queries_for_diff,
    _compare_columns_for_diff,
    _compare_cube_columns_for_diff,
    _compare_dimension_links_for_diff,
    _detect_column_changes_for_diff,
)
from datajunction_server.models.node import NodeStatus


class TestGetPropagationReason:
    """Tests for _get_propagation_reason function."""

    def test_version_changed(self):
        """Test when only version changed."""
        reason = _get_propagation_reason(
            base_version="v1.0",
            compare_version="v2.0",
            base_status=NodeStatus.VALID,
            compare_status=NodeStatus.VALID,
        )
        assert "version changed from v1.0 to v2.0" in reason

    def test_status_changed(self):
        """Test when only status changed."""
        reason = _get_propagation_reason(
            base_version="v1.0",
            compare_version="v1.0",
            base_status=NodeStatus.VALID,
            compare_status=NodeStatus.INVALID,
        )
        assert "status changed from valid to invalid" in reason

    def test_both_changed(self):
        """Test when both version and status changed."""
        reason = _get_propagation_reason(
            base_version="v1.0",
            compare_version="v2.0",
            base_status=NodeStatus.VALID,
            compare_status=NodeStatus.INVALID,
        )
        assert "version changed" in reason
        assert "status changed" in reason

    def test_none_status(self):
        """Test with None status values."""
        reason = _get_propagation_reason(
            base_version="v1.0",
            compare_version="v1.0",
            base_status=None,
            compare_status=NodeStatus.VALID,
        )
        assert "status changed from unknown to valid" in reason

    def test_no_changes_fallback(self):
        """Test fallback message when called with identical values."""
        reason = _get_propagation_reason(
            base_version="v1.0",
            compare_version="v1.0",
            base_status=NodeStatus.VALID,
            compare_status=NodeStatus.VALID,
        )
        assert reason == "system-derived fields changed"


class TestCompareQueriesForDiff:
    """Tests for _compare_queries_for_diff function."""

    def test_both_none(self):
        """Test when both queries are None."""
        result = _compare_queries_for_diff(None, None, "ns1", "ns2")
        assert result is True

    def test_one_none(self):
        """Test when one query is None."""
        result = _compare_queries_for_diff(
            "SELECT * FROM table",
            None,
            "ns1",
            "ns2",
        )
        assert result is False

        result = _compare_queries_for_diff(
            None,
            "SELECT * FROM table",
            "ns1",
            "ns2",
        )
        assert result is False

    def test_identical_queries(self):
        """Test identical queries."""
        result = _compare_queries_for_diff(
            "SELECT * FROM ns1.table",
            "SELECT * FROM ns2.table",
            "ns1",
            "ns2",
        )
        assert result is True

    def test_different_queries(self):
        """Test different queries."""
        result = _compare_queries_for_diff(
            "SELECT * FROM ns1.table WHERE x = 1",
            "SELECT * FROM ns2.table WHERE x = 2",
            "ns1",
            "ns2",
        )
        assert result is False

    def test_whitespace_normalization(self):
        """Test whitespace is normalized."""
        result = _compare_queries_for_diff(
            "SELECT   *   FROM   ns1.table",
            "SELECT * FROM ns2.table",
            "ns1",
            "ns2",
        )
        assert result is True

    def test_case_insensitive(self):
        """Test case is normalized."""
        result = _compare_queries_for_diff(
            "SELECT * FROM NS1.TABLE",
            "select * from ns2.table",
            "ns1",
            "ns2",
        )
        assert result is True


class TestCompareColumnsForDiff:
    """Tests for _compare_columns_for_diff function."""

    def test_both_empty(self):
        """Test when both column lists are empty."""
        result = _compare_columns_for_diff([], [], "ns1", "ns2", compare_types=False)
        assert result is True

    def test_both_none(self):
        """Test when both column lists are None."""
        result = _compare_columns_for_diff(
            None,
            None,
            "ns1",
            "ns2",
            compare_types=False,
        )
        assert result is True

    def test_one_empty(self):
        """Test when one column list is empty."""
        col = MagicMock()
        col.name = "col1"
        result = _compare_columns_for_diff([col], [], "ns1", "ns2", compare_types=False)
        assert result is False

    def test_different_lengths(self):
        """Test when column lists have different lengths."""
        col1 = MagicMock()
        col1.name = "col1"
        col2 = MagicMock()
        col2.name = "col2"
        result = _compare_columns_for_diff(
            [col1],
            [col1, col2],
            "ns1",
            "ns2",
            compare_types=False,
        )
        assert result is False

    def test_different_column_names(self):
        """Test when column names don't match."""
        col1 = MagicMock()
        col1.name = "col1"
        col2 = MagicMock()
        col2.name = "col2"
        result = _compare_columns_for_diff(
            [col1],
            [col2],
            "ns1",
            "ns2",
            compare_types=False,
        )
        assert result is False

    def test_type_comparison(self):
        """Test column type comparison when enabled."""
        col1 = MagicMock()
        col1.name = "col1"
        col1.type = "int"
        col1.display_name = None
        col1.description = None
        col1.attributes = []

        col2 = MagicMock()
        col2.name = "col1"
        col2.type = "bigint"
        col2.display_name = None
        col2.description = None
        col2.attributes = []

        result = _compare_columns_for_diff(
            [col1],
            [col2],
            "ns1",
            "ns2",
            compare_types=True,
        )
        assert result is False

    def test_display_name_difference(self):
        """Test display_name comparison."""
        col1 = MagicMock()
        col1.name = "col1"
        col1.type = "int"
        col1.display_name = "Column One"
        col1.description = None
        col1.attributes = []

        col2 = MagicMock()
        col2.name = "col1"
        col2.type = "int"
        col2.display_name = "Column 1"
        col2.description = None
        col2.attributes = []

        result = _compare_columns_for_diff(
            [col1],
            [col2],
            "ns1",
            "ns2",
            compare_types=False,
        )
        assert result is False

    def test_description_difference(self):
        """Test description comparison."""
        col1 = MagicMock()
        col1.name = "col1"
        col1.type = "int"
        col1.display_name = None
        col1.description = "Description 1"
        col1.attributes = []

        col2 = MagicMock()
        col2.name = "col1"
        col2.type = "int"
        col2.display_name = None
        col2.description = "Description 2"
        col2.attributes = []

        result = _compare_columns_for_diff(
            [col1],
            [col2],
            "ns1",
            "ns2",
            compare_types=False,
        )
        assert result is False

    def test_attributes_difference(self):
        """Test attributes comparison."""
        col1 = MagicMock()
        col1.name = "col1"
        col1.type = "int"
        col1.display_name = None
        col1.description = None
        col1.attributes = ["attr1"]

        col2 = MagicMock()
        col2.name = "col1"
        col2.type = "int"
        col2.display_name = None
        col2.description = None
        col2.attributes = ["attr2"]

        result = _compare_columns_for_diff(
            [col1],
            [col2],
            "ns1",
            "ns2",
            compare_types=False,
        )
        assert result is False


class TestCompareCubeColumnsForDiff:
    """Tests for _compare_cube_columns_for_diff function."""

    def test_both_empty(self):
        """Test when both column lists are empty."""
        result = _compare_cube_columns_for_diff([], [], "ns1", "ns2")
        assert result is True

    def test_both_none(self):
        """Test when both column lists are None."""
        result = _compare_cube_columns_for_diff(None, None, "ns1", "ns2")
        assert result is True

    def test_one_empty(self):
        """Test when one column list is empty."""
        col = MagicMock()
        col.name = "ns1.metric"
        result = _compare_cube_columns_for_diff([col], None, "ns1", "ns2")
        assert result is False

    def test_different_lengths(self):
        """Test when column lists have different lengths."""
        col1 = MagicMock()
        col1.name = "ns1.metric1"
        col2 = MagicMock()
        col2.name = "ns1.metric2"
        result = _compare_cube_columns_for_diff(
            [col1],
            [col1, col2],
            "ns1",
            "ns2",
        )
        assert result is False

    def test_different_column_names(self):
        """Test when column names don't match after namespace normalization."""
        col1 = MagicMock()
        col1.name = "ns1.metric1"
        col1.display_name = None
        col1.description = None

        col2 = MagicMock()
        col2.name = "ns2.metric2"
        col2.display_name = None
        col2.description = None

        result = _compare_cube_columns_for_diff(
            [col1],
            [col2],
            "ns1",
            "ns2",
        )
        assert result is False

    def test_matching_columns_with_namespace(self):
        """Test columns match after namespace normalization."""
        col1 = MagicMock()
        col1.name = "ns1.metric1"
        col1.display_name = None
        col1.description = None

        col2 = MagicMock()
        col2.name = "ns2.metric1"
        col2.display_name = None
        col2.description = None

        # Mock partition attribute
        col1.partition = None
        col2.partition = None

        result = _compare_cube_columns_for_diff(
            [col1],
            [col2],
            "ns1",
            "ns2",
        )
        assert result is True

    def test_display_name_difference(self):
        """Test display_name comparison in cube columns."""
        col1 = MagicMock()
        col1.name = "ns1.metric1"
        col1.display_name = "Display 1"
        col1.description = None

        col2 = MagicMock()
        col2.name = "ns2.metric1"
        col2.display_name = "Display 2"
        col2.description = None

        result = _compare_cube_columns_for_diff(
            [col1],
            [col2],
            "ns1",
            "ns2",
        )
        assert result is False

    def test_description_difference(self):
        """Test description comparison in cube columns."""
        col1 = MagicMock()
        col1.name = "ns1.metric1"
        col1.display_name = None
        col1.description = "Desc 1"

        col2 = MagicMock()
        col2.name = "ns2.metric1"
        col2.display_name = None
        col2.description = "Desc 2"

        result = _compare_cube_columns_for_diff(
            [col1],
            [col2],
            "ns1",
            "ns2",
        )
        assert result is False

    def test_partition_difference(self):
        """Test partition config comparison."""
        col1 = MagicMock()
        col1.name = "ns1.metric1"
        col1.display_name = None
        col1.description = None
        col1.partition = {"type": "temporal"}

        col2 = MagicMock()
        col2.name = "ns2.metric1"
        col2.display_name = None
        col2.description = None
        col2.partition = {"type": "categorical"}

        result = _compare_cube_columns_for_diff(
            [col1],
            [col2],
            "ns1",
            "ns2",
        )
        assert result is False


class TestCompareDimensionLinksForDiff:
    """Tests for _compare_dimension_links_for_diff function."""

    def test_both_empty(self):
        """Test when both link lists are empty."""
        result = _compare_dimension_links_for_diff([], [], "ns1", "ns2")
        assert result is True

    def test_different_lengths(self):
        """Test when link lists have different lengths."""
        link1 = MagicMock()
        link1.type = "reference"
        link1.dimension = "ns1.dim"
        link1.role = None

        result = _compare_dimension_links_for_diff(
            [link1],
            [],
            "ns1",
            "ns2",
        )
        assert result is False

    def test_different_types(self):
        """Test when link types differ."""
        link1 = MagicMock()
        link1.type = "reference"
        link1.dimension = "ns1.dim"
        link1.role = None

        link2 = MagicMock()
        link2.type = "join"
        link2.dimension_node = "ns2.dim"
        link2.role = None
        link2.join_type = "left"
        link2.join_on = ""

        result = _compare_dimension_links_for_diff(
            [link1],
            [link2],
            "ns1",
            "ns2",
        )
        assert result is False

    def test_different_roles(self):
        """Test when link roles differ."""
        link1 = MagicMock()
        link1.type = "reference"
        link1.dimension = "ns1.dim"
        link1.role = "role1"

        link2 = MagicMock()
        link2.type = "reference"
        link2.dimension = "ns2.dim"
        link2.role = "role2"

        result = _compare_dimension_links_for_diff(
            [link1],
            [link2],
            "ns1",
            "ns2",
        )
        assert result is False

    def test_matching_reference_links(self):
        """Test matching reference links with namespace normalization."""
        link1 = MagicMock()
        link1.type = "reference"
        link1.dimension = "ns1.dim"
        link1.role = None
        # Make hasattr return False for dimension_node
        del link1.dimension_node

        link2 = MagicMock()
        link2.type = "reference"
        link2.dimension = "ns2.dim"
        link2.role = None
        del link2.dimension_node

        result = _compare_dimension_links_for_diff(
            [link1],
            [link2],
            "ns1",
            "ns2",
        )
        assert result is True

    def test_join_link_different_join_type(self):
        """Test join links with different join types."""
        link1 = MagicMock()
        link1.type = "join"
        link1.dimension_node = "ns1.dim"
        link1.role = None
        link1.join_type = "left"
        link1.join_on = "ns1.table.col = ns1.dim.col"

        link2 = MagicMock()
        link2.type = "join"
        link2.dimension_node = "ns2.dim"
        link2.role = None
        link2.join_type = "inner"
        link2.join_on = "ns2.table.col = ns2.dim.col"

        result = _compare_dimension_links_for_diff(
            [link1],
            [link2],
            "ns1",
            "ns2",
        )
        assert result is False

    def test_join_link_different_join_on(self):
        """Test join links with different join_on conditions."""
        link1 = MagicMock()
        link1.type = "join"
        link1.dimension_node = "ns1.dim"
        link1.role = None
        link1.join_type = "left"
        link1.join_on = "ns1.table.col = ns1.dim.col"

        link2 = MagicMock()
        link2.type = "join"
        link2.dimension_node = "ns2.dim"
        link2.role = None
        link2.join_type = "left"
        link2.join_on = "ns2.table.other_col = ns2.dim.col"

        result = _compare_dimension_links_for_diff(
            [link1],
            [link2],
            "ns1",
            "ns2",
        )
        assert result is False

    def test_reference_link_different_dimensions(self):
        """Test reference links with different dimension references."""
        link1 = MagicMock()
        link1.type = "reference"
        link1.dimension = "ns1.dim1"
        link1.role = None
        del link1.dimension_node

        link2 = MagicMock()
        link2.type = "reference"
        link2.dimension = "ns2.dim2"
        link2.role = None
        del link2.dimension_node

        result = _compare_dimension_links_for_diff(
            [link1],
            [link2],
            "ns1",
            "ns2",
        )
        assert result is False


class TestDetectColumnChangesForDiff:
    """Tests for _detect_column_changes_for_diff function."""

    def test_base_node_none(self):
        """Test when base node is None."""
        result = _detect_column_changes_for_diff(None, MagicMock(), "ns1", "ns2")
        assert result == []

    def test_compare_node_none(self):
        """Test when compare node is None."""
        result = _detect_column_changes_for_diff(MagicMock(), None, "ns1", "ns2")
        assert result == []

    def test_base_current_none(self):
        """Test when base node.current is None."""
        base_node = MagicMock()
        base_node.current = None
        result = _detect_column_changes_for_diff(base_node, MagicMock(), "ns1", "ns2")
        assert result == []

    def test_compare_current_none(self):
        """Test when compare node.current is None."""
        base_node = MagicMock()
        base_node.current = MagicMock()
        compare_node = MagicMock()
        compare_node.current = None
        result = _detect_column_changes_for_diff(base_node, compare_node, "ns1", "ns2")
        assert result == []
