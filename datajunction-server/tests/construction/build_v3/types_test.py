"""
Tests for types.py in build_v3.
"""

from unittest.mock import MagicMock


from datajunction_server.construction.build_v3.types import (
    DecomposedMetricInfo,
    GrainGroup,
    MetricGroup,
)
from datajunction_server.models.decompose import Aggregability


class TestDecomposedMetricInfo:
    """Tests for DecomposedMetricInfo."""

    def test_is_fully_decomposable_all_full(self):
        """Test is_fully_decomposable when all components are FULL."""
        # Create mock components with FULL aggregability
        component1 = MagicMock()
        component1.rule = MagicMock()
        component1.rule.type = Aggregability.FULL

        component2 = MagicMock()
        component2.rule = MagicMock()
        component2.rule.type = Aggregability.FULL

        # Create DecomposedMetricInfo
        metric_node = MagicMock()
        derived_ast = MagicMock()
        derived_ast.select.projection = [MagicMock()]

        info = DecomposedMetricInfo(
            metric_node=metric_node,
            components=[component1, component2],
            aggregability=Aggregability.FULL,
            combiner="SUM(x) + SUM(y)",
            derived_ast=derived_ast,
        )

        assert info.is_fully_decomposable is True

    def test_is_fully_decomposable_with_limited(self):
        """Test is_fully_decomposable when one component is LIMITED."""
        # Create mock components with mixed aggregability
        component1 = MagicMock()
        component1.rule = MagicMock()
        component1.rule.type = Aggregability.FULL

        component2 = MagicMock()
        component2.rule = MagicMock()
        component2.rule.type = Aggregability.LIMITED

        metric_node = MagicMock()
        derived_ast = MagicMock()
        derived_ast.select.projection = [MagicMock()]

        info = DecomposedMetricInfo(
            metric_node=metric_node,
            components=[component1, component2],
            aggregability=Aggregability.LIMITED,
            combiner="SUM(x) / COUNT(DISTINCT y)",
            derived_ast=derived_ast,
        )

        assert info.is_fully_decomposable is False

    def test_is_fully_decomposable_with_none(self):
        """Test is_fully_decomposable when one component is NONE."""
        component1 = MagicMock()
        component1.rule = MagicMock()
        component1.rule.type = Aggregability.NONE

        metric_node = MagicMock()
        derived_ast = MagicMock()
        derived_ast.select.projection = [MagicMock()]

        info = DecomposedMetricInfo(
            metric_node=metric_node,
            components=[component1],
            aggregability=Aggregability.NONE,
            combiner="RANK()",
            derived_ast=derived_ast,
        )

        assert info.is_fully_decomposable is False


class TestMetricGroup:
    """Tests for MetricGroup."""

    def test_overall_aggregability_all_full(self):
        """Test overall_aggregability when all metrics are FULL."""
        decomposed1 = MagicMock()
        decomposed1.aggregability = Aggregability.FULL

        decomposed2 = MagicMock()
        decomposed2.aggregability = Aggregability.FULL

        parent_node = MagicMock()
        group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed1, decomposed2],
        )

        assert group.overall_aggregability == Aggregability.FULL

    def test_overall_aggregability_with_limited(self):
        """Test overall_aggregability when one metric is LIMITED."""
        decomposed1 = MagicMock()
        decomposed1.aggregability = Aggregability.FULL

        decomposed2 = MagicMock()
        decomposed2.aggregability = Aggregability.LIMITED

        parent_node = MagicMock()
        group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed1, decomposed2],
        )

        assert group.overall_aggregability == Aggregability.LIMITED

    def test_overall_aggregability_with_none(self):
        """Test overall_aggregability when one metric is NONE."""
        decomposed1 = MagicMock()
        decomposed1.aggregability = Aggregability.FULL

        decomposed2 = MagicMock()
        decomposed2.aggregability = Aggregability.NONE

        parent_node = MagicMock()
        group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed1, decomposed2],
        )

        # NONE is the worst, so it should return NONE
        assert group.overall_aggregability == Aggregability.NONE


class TestGrainGroup:
    """Tests for GrainGroup."""

    def test_grain_key(self):
        """Test grain_key property returns correct tuple."""
        parent_node = MagicMock()
        parent_node.name = "v3.order_details"

        component = MagicMock()
        metric_node = MagicMock()

        group = GrainGroup(
            parent_node=parent_node,
            aggregability=Aggregability.LIMITED,
            grain_columns=["customer_id", "product_id"],  # Unsorted on purpose
            components=[(metric_node, component)],
        )

        key = group.grain_key
        assert key == (
            "v3.order_details",
            Aggregability.LIMITED,
            ("customer_id", "product_id"),  # Should be sorted
        )

    def test_grain_key_with_full_aggregability(self):
        """Test grain_key with FULL aggregability (empty grain)."""
        parent_node = MagicMock()
        parent_node.name = "v3.page_views"

        component = MagicMock()
        metric_node = MagicMock()

        group = GrainGroup(
            parent_node=parent_node,
            aggregability=Aggregability.FULL,
            grain_columns=[],  # FULL means no extra grain columns
            components=[(metric_node, component)],
        )

        key = group.grain_key
        assert key == ("v3.page_views", Aggregability.FULL, ())

    def test_grain_key_sorting(self):
        """Test that grain_key sorts grain_columns."""
        parent_node = MagicMock()
        parent_node.name = "v3.facts"

        component = MagicMock()
        metric_node = MagicMock()

        group = GrainGroup(
            parent_node=parent_node,
            aggregability=Aggregability.LIMITED,
            grain_columns=["z_col", "a_col", "m_col"],
            components=[(metric_node, component)],
        )

        key = group.grain_key
        # Grain columns should be sorted alphabetically
        assert key[2] == ("a_col", "m_col", "z_col")
