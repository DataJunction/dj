"""Tests for decomposition module."""

from unittest.mock import MagicMock

import pytest

from datajunction_server.construction.build_v3.decomposition import (
    get_base_metrics_for_derived,
    is_derived_metric,
)
from datajunction_server.construction.build_v3.types import BuildContext
from datajunction_server.models.node_type import NodeType


class TestGetBaseMetricsForDerived:
    """Tests for get_base_metrics_for_derived function."""

    def test_skips_dimension_parents(self):
        """
        Test that a required-dimension link on the derived metric doesn't derail
        base-metric detection.

        When a derived metric has a dimension as a parent (e.g., for required_dimensions
        in window functions), that dimension link should be ignored: it is not a metric
        (so it isn't recursed into) and it must not cause the derived metric itself to be
        treated as a base metric. Only the real metric parent's base is collected.
        """
        # Create mock nodes
        dimension_node = MagicMock()
        dimension_node.name = "v3.date"
        dimension_node.type = NodeType.DIMENSION

        base_metric_node = MagicMock()
        base_metric_node.name = "v3.total_revenue"
        base_metric_node.type = NodeType.METRIC

        fact_node = MagicMock()
        fact_node.name = "v3.order_details"
        fact_node.type = NodeType.TRANSFORM

        derived_metric_node = MagicMock()
        derived_metric_node.name = "v3.trailing_wow_revenue_change"
        derived_metric_node.type = NodeType.METRIC

        # Create mock context
        ctx = MagicMock(spec=BuildContext)
        ctx.nodes = {
            "v3.date": dimension_node,
            "v3.total_revenue": base_metric_node,
            "v3.order_details": fact_node,
            "v3.trailing_wow_revenue_change": derived_metric_node,
        }
        # Parent map: derived_metric -> [dimension, base_metric]
        # base_metric -> [fact]
        ctx.parent_map = {
            "v3.trailing_wow_revenue_change": ["v3.date", "v3.total_revenue"],
            "v3.total_revenue": ["v3.order_details"],
        }

        # Execute
        result = get_base_metrics_for_derived(ctx, derived_metric_node)

        # Verify: Should find base_metric_node, not treat derived as base due to dimension
        assert len(result) == 1
        assert result[0].name == "v3.total_revenue"

    def test_handles_dimension_only_parent(self):
        """
        Test handling when a metric only has a dimension parent in one path.

        The function should continue checking other parents when it hits a dimension.
        """
        dimension_node = MagicMock()
        dimension_node.name = "v3.date"
        dimension_node.type = NodeType.DIMENSION

        metric_node = MagicMock()
        metric_node.name = "v3.some_metric"
        metric_node.type = NodeType.METRIC

        fact_node = MagicMock()
        fact_node.name = "v3.fact_table"
        fact_node.type = NodeType.TRANSFORM

        # Metric has dimension first, then fact
        ctx = MagicMock(spec=BuildContext)
        ctx.nodes = {
            "v3.date": dimension_node,
            "v3.some_metric": metric_node,
            "v3.fact_table": fact_node,
        }
        ctx.parent_map = {
            "v3.some_metric": ["v3.date", "v3.fact_table"],
        }

        result = get_base_metrics_for_derived(ctx, metric_node)

        # Should find the metric as a base metric (via fact parent)
        assert len(result) == 1
        assert result[0].name == "v3.some_metric"

    def test_collects_base_metric_defined_on_dimension_source(self):
        """
        A derived ratio whose two base metrics have DIFFERENT parents -- one on a
        fact/transform, the other defined directly on a dimension node -- must
        collect BOTH base metrics.

        Regression for the cross-parent ratio bug: a base metric whose data source
        is a dimension node was mistaken for a bare required-dimension reference and
        dropped, so its grain group was never built and the generated SQL referenced
        an aggregate column that no CTE computed.
        """
        fact_node = MagicMock()
        fact_node.name = "v3.visits_fact"
        fact_node.type = NodeType.TRANSFORM

        dim_source = MagicMock()
        dim_source.name = "v3.accounts_dim"
        dim_source.type = NodeType.DIMENSION

        date_dim = MagicMock()
        date_dim.name = "v3.date"
        date_dim.type = NodeType.DIMENSION

        numerator = MagicMock()
        numerator.name = "v3.visited_accounts"
        numerator.type = NodeType.METRIC

        denominator = MagicMock()
        denominator.name = "v3.eligible_accounts"
        denominator.type = NodeType.METRIC

        derived = MagicMock()
        derived.name = "v3.visit_rate"
        derived.type = NodeType.METRIC

        ctx = MagicMock(spec=BuildContext)
        ctx.nodes = {
            n.name: n
            for n in [fact_node, dim_source, date_dim, numerator, denominator, derived]
        }
        # date is a required-dimension link on each metric; the numerator's data
        # source is a fact, the denominator's data source is a dimension node.
        ctx.parent_map = {
            "v3.visit_rate": [
                "v3.visited_accounts",
                "v3.eligible_accounts",
                "v3.date",
            ],
            "v3.visited_accounts": ["v3.visits_fact", "v3.date"],
            "v3.eligible_accounts": ["v3.accounts_dim", "v3.date"],
        }

        result = get_base_metrics_for_derived(ctx, derived)

        assert {n.name for n in result} == {
            "v3.visited_accounts",
            "v3.eligible_accounts",
        }


class TestIsDerivedMetric:
    """Tests for is_derived_metric function."""

    def test_metric_with_dimension_and_metric_parents_is_derived(self):
        """
        Test that a metric with both dimension and metric parents is derived.

        This tests the logic that checks if ANY parent is a metric.
        """
        dimension_node = MagicMock()
        dimension_node.name = "v3.date"
        dimension_node.type = NodeType.DIMENSION

        parent_metric = MagicMock()
        parent_metric.name = "v3.total_revenue"
        parent_metric.type = NodeType.METRIC

        derived_metric = MagicMock()
        derived_metric.name = "v3.wow_change"
        derived_metric.type = NodeType.METRIC

        ctx = MagicMock(spec=BuildContext)
        ctx.nodes = {
            "v3.date": dimension_node,
            "v3.total_revenue": parent_metric,
            "v3.wow_change": derived_metric,
        }
        # Dimension appears BEFORE the metric in parent list
        ctx.parent_map = {
            "v3.wow_change": ["v3.date", "v3.total_revenue"],
        }

        result = is_derived_metric(ctx, derived_metric)

        # Should be derived because it has a metric parent
        assert result is True

    def test_metric_with_only_dimension_parent_not_derived(self):
        """
        Test that a metric with only dimension parents is not derived.
        """
        dimension_node = MagicMock()
        dimension_node.name = "v3.date"
        dimension_node.type = NodeType.DIMENSION

        metric_node = MagicMock()
        metric_node.name = "v3.some_metric"
        metric_node.type = NodeType.METRIC

        ctx = MagicMock(spec=BuildContext)
        ctx.nodes = {
            "v3.date": dimension_node,
            "v3.some_metric": metric_node,
        }
        ctx.parent_map = {
            "v3.some_metric": ["v3.date"],
        }

        result = is_derived_metric(ctx, metric_node)

        # Only dimension parent - not a derived metric
        assert result is False


@pytest.mark.asyncio
async def test_decomposition_with_dimension_parent_integration(
    module__client_with_build_v3,
):
    """
    Integration test: verify that metrics with dimension parents work correctly.

    The v3.trailing_wow_revenue_change metric has:
    - required_dimensions: ["v3.date.date_id[order]"] - creates dimension parent
    - References v3.total_revenue - creates metric parent

    This exercises the required-dimension-link path in a real scenario: the
    dimension parent must not be mistaken for a data source.
    """
    client = module__client_with_build_v3

    # Query the trailing metric - this exercises the decomposition code path
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.trailing_wow_revenue_change"],
            "dimensions": ["v3.product.category"],
        },
    )

    # Should succeed - if dimension parent wasn't skipped, it might fail
    # or produce incorrect results
    assert response.status_code == 200, response.json()
    result = response.json()

    # Verify the SQL was generated successfully
    assert "sql" in result
    assert result["sql"] is not None

    # Verify the metric appears in columns
    column_names = [col["name"] for col in result["columns"]]
    assert "trailing_wow_revenue_change" in column_names
