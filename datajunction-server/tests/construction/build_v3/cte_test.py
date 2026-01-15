"""Tests for cte.py - CTE building and AST transformation utilities."""

from datajunction_server.construction.build_v3.cte import (
    get_column_full_name,
    inject_partition_by_into_windows,
    replace_metric_refs_in_ast,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse


class TestGetColumnFullName:
    """Tests for get_column_full_name function."""

    def test_simple_table_column(self):
        """Column with table prefix returns full dotted name."""
        query = parse("SELECT t.column_name FROM t")
        col = list(query.find_all(ast.Column))[0]

        result = get_column_full_name(col)

        assert result == "t.column_name"

    def test_namespaced_table_column(self):
        """Column with namespaced table returns full path."""
        query = parse("SELECT v3.order_details.status FROM v3.order_details")
        col = list(query.find_all(ast.Column))[0]

        result = get_column_full_name(col)

        assert result == "v3.order_details.status"

    def test_metric_reference_style(self):
        """Metric-style reference (namespace.metric) returns full name."""
        query = parse("SELECT v3.total_revenue / v3.order_count")
        cols = list(query.find_all(ast.Column))

        # Should find both metric references
        names = [get_column_full_name(col) for col in cols]

        assert "v3.total_revenue" in names
        assert "v3.order_count" in names


class TestReplaceMetricRefsInAst:
    """Tests for replace_metric_refs_in_ast function."""

    def test_replaces_matching_metric_references(self):
        """Should replace metric name references with CTE column references."""
        # Parse a query that looks like a derived metric combiner expression
        query = parse("SELECT v3.total_revenue / NULLIF(v3.order_count, 0)")
        expr = query.select.projection[0]

        metric_aliases = {
            "v3.total_revenue": ("order_details_0", "line_total_sum_e1f61696"),
            "v3.order_count": ("order_details_0", "order_id_count_78d2e5eb"),
        }

        replace_metric_refs_in_ast(expr, metric_aliases)

        # Check that columns were replaced with CTE references
        result_sql = str(expr)
        assert "order_details_0.line_total_sum_e1f61696" in result_sql
        assert "order_details_0.order_id_count_78d2e5eb" in result_sql
        # Original references should be gone
        assert "v3.total_revenue" not in result_sql
        assert "v3.order_count" not in result_sql

    def test_only_replaces_matching_columns(self):
        """Should only replace columns that match metric_aliases keys."""
        query = parse("SELECT v3.total_revenue + some_other_column")
        expr = query.select.projection[0]

        metric_aliases = {
            "v3.total_revenue": ("cte_0", "revenue"),
        }

        replace_metric_refs_in_ast(expr, metric_aliases)

        result_sql = str(expr)
        # Matching column replaced
        assert "cte_0.revenue" in result_sql
        # Non-matching column unchanged
        assert "some_other_column" in result_sql

    def test_empty_metric_aliases_no_changes(self):
        """Empty metric_aliases should not modify the AST."""
        query = parse("SELECT v3.total_revenue / v3.order_count")
        expr = query.select.projection[0]
        original_sql = str(expr)

        replace_metric_refs_in_ast(expr, {})

        assert str(expr) == original_sql

    def test_handles_nested_expressions(self):
        """Should find and replace columns in nested expressions."""
        # Complex expression with nested functions
        query = parse(
            "SELECT CAST(v3.order_count AS DOUBLE) / NULLIF(v3.visitor_count, 0)",
        )
        expr = query.select.projection[0]

        metric_aliases = {
            "v3.order_count": ("orders_0", "order_id_count_78d2e5eb"),
            "v3.visitor_count": ("visitors_0", "visitor_id_hll_1c4f5e47"),
        }

        replace_metric_refs_in_ast(expr, metric_aliases)

        result_sql = str(expr)
        assert "orders_0.order_id_count_78d2e5eb" in result_sql
        assert "visitors_0.visitor_id_hll_1c4f5e47" in result_sql

    def test_handles_multiple_references_same_metric(self):
        """Should replace all occurrences of the same metric reference."""
        query = parse("SELECT v3.revenue + v3.revenue * 0.1")
        expr = query.select.projection[0]

        metric_aliases = {
            "v3.revenue": ("cte_0", "line_total_sum_e1f61696"),
        }

        replace_metric_refs_in_ast(expr, metric_aliases)

        result_sql = str(expr)
        # Both occurrences should be replaced
        assert result_sql.count("cte_0.line_total_sum_e1f61696") == 2
        assert "v3.revenue" not in result_sql


class TestInjectPartitionByIntoWindows:
    """Tests for inject_partition_by_into_windows function."""

    def test_injects_partition_by_for_lag_window(self):
        """LAG window function gets PARTITION BY with non-ORDER-BY dimensions."""
        # WoW metric expression: LAG(revenue, 1) OVER (ORDER BY week)
        query = parse("SELECT LAG(revenue, 1) OVER (ORDER BY week)")
        expr = query.select.projection[0]

        # Requested dimensions: category, country, week
        # week is in ORDER BY, so PARTITION BY should have: category, country
        inject_partition_by_into_windows(expr, ["category", "country", "week"])

        result_sql = str(expr)
        assert "PARTITION BY category, country" in result_sql
        assert "ORDER BY week" in result_sql

    def test_injects_partition_by_excludes_order_by_dimension(self):
        """ORDER BY dimension should not be in PARTITION BY."""
        query = parse("SELECT LAG(total, 1) OVER (ORDER BY month)")
        expr = query.select.projection[0]

        inject_partition_by_into_windows(expr, ["category", "month", "year"])

        result_sql = str(expr)
        # month is in ORDER BY, so should NOT be in PARTITION BY
        assert "PARTITION BY category, year" in result_sql
        assert "ORDER BY month" in result_sql

    def test_does_not_modify_window_with_existing_partition_by(self):
        """Should not override existing PARTITION BY clause."""
        query = parse(
            "SELECT LAG(revenue, 1) OVER (PARTITION BY region ORDER BY week)",
        )
        expr = query.select.projection[0]

        inject_partition_by_into_windows(expr, ["category", "country", "week"])

        result_sql = str(expr)
        # Original PARTITION BY should be preserved
        assert "PARTITION BY region" in result_sql
        # Should NOT have the injected dimensions
        assert "category" not in result_sql

    def test_handles_multiple_window_functions(self):
        """Should inject PARTITION BY into multiple window functions."""
        # MoM and WoW in the same expression
        query = parse(
            "SELECT "
            "(revenue - LAG(revenue, 1) OVER (ORDER BY week)) / "
            "NULLIF(LAG(revenue, 1) OVER (ORDER BY week), 0)",
        )
        expr = query.select.projection[0]

        inject_partition_by_into_windows(expr, ["category", "week"])

        result_sql = str(expr)
        # Both LAG functions should get PARTITION BY category
        assert result_sql.count("PARTITION BY category") == 2

    def test_handles_no_window_functions(self):
        """Expression without window functions should not be modified."""
        query = parse("SELECT revenue / NULLIF(orders, 0)")
        expr = query.select.projection[0]
        original_sql = str(expr)

        inject_partition_by_into_windows(expr, ["category", "week"])

        assert str(expr) == original_sql

    def test_handles_empty_dimension_list(self):
        """Empty dimension list should not add PARTITION BY."""
        query = parse("SELECT LAG(revenue, 1) OVER (ORDER BY week)")
        expr = query.select.projection[0]

        inject_partition_by_into_windows(expr, [])

        result_sql = str(expr)
        # No PARTITION BY should be added
        assert "PARTITION BY" not in result_sql
        assert "ORDER BY week" in result_sql

    def test_handles_all_dimensions_in_order_by(self):
        """If all dimensions are in ORDER BY, no PARTITION BY added."""
        query = parse("SELECT LAG(revenue, 1) OVER (ORDER BY week)")
        expr = query.select.projection[0]

        # Only dimension is week, which is in ORDER BY
        inject_partition_by_into_windows(expr, ["week"])

        result_sql = str(expr)
        # No PARTITION BY because all dimensions are in ORDER BY
        assert "PARTITION BY" not in result_sql

    def test_real_wow_metric_expression(self):
        """Test with realistic week-over-week metric expression."""
        query = parse(
            "SELECT "
            "(SUM(total_revenue) - LAG(SUM(total_revenue), 1) OVER (ORDER BY week)) "
            "/ NULLIF(LAG(SUM(total_revenue), 1) OVER (ORDER BY week), 0) * 100",
        )
        expr = query.select.projection[0]

        # Typical dimensions for WoW: category (non-time), week (time)
        inject_partition_by_into_windows(expr, ["category", "week"])

        result_sql = str(expr)
        # Both LAG windows should get PARTITION BY category
        assert result_sql.count("PARTITION BY category") == 2
        # week should remain in ORDER BY
        assert "ORDER BY week" in result_sql

    def test_aggregate_window_not_modified(self):
        """Aggregate window functions (SUM OVER) should NOT get PARTITION BY injected."""
        # This is the weighted CPM pattern - grand total for weighting
        query = parse("SELECT impressions / NULLIF(SUM(impressions) OVER (), 0)")
        expr = query.select.projection[0]

        inject_partition_by_into_windows(expr, ["category", "country"])

        result_sql = str(expr)
        # SUM OVER () should remain empty - no PARTITION BY injection
        assert "PARTITION BY" not in result_sql
        assert "SUM(impressions) OVER ()" in result_sql

    def test_weighted_cpm_pattern(self):
        """Test weighted CPM pattern with grand total weight."""
        # Weighted CPM = (revenue / impressions) * (impressions / SUM(impressions) OVER ())
        query = parse(
            "SELECT "
            "(revenue / NULLIF(impressions / 1000.0, 0)) "
            "* (impressions / NULLIF(SUM(impressions) OVER (), 0))",
        )
        expr = query.select.projection[0]

        inject_partition_by_into_windows(expr, ["category", "country"])

        result_sql = str(expr)
        # SUM OVER () should NOT get PARTITION BY - it's computing grand total
        assert "SUM(impressions) OVER ()" in result_sql
        assert "PARTITION BY" not in result_sql

    def test_mixed_lag_and_aggregate_window(self):
        """LAG should get PARTITION BY, but SUM OVER () should not."""
        query = parse(
            "SELECT LAG(revenue, 1) OVER (ORDER BY week) + SUM(revenue) OVER ()",
        )
        expr = query.select.projection[0]

        inject_partition_by_into_windows(expr, ["category", "week"])

        result_sql = str(expr)
        # LAG should get PARTITION BY category (week is in ORDER BY)
        assert "LAG(revenue, 1) OVER (" in result_sql
        assert "PARTITION BY category" in result_sql
        # SUM OVER () should remain empty
        assert "SUM(revenue) OVER ()" in result_sql

    def test_rank_gets_partition_by(self):
        """RANK window function should get PARTITION BY injection."""
        query = parse("SELECT RANK() OVER (ORDER BY revenue DESC)")
        expr = query.select.projection[0]

        inject_partition_by_into_windows(expr, ["category", "country"])

        result_sql = str(expr)
        # RANK should get PARTITION BY
        assert "PARTITION BY" in result_sql
        assert "category" in result_sql
        assert "country" in result_sql

    def test_row_number_gets_partition_by(self):
        """ROW_NUMBER window function should get PARTITION BY injection."""
        query = parse("SELECT ROW_NUMBER() OVER (ORDER BY created_at)")
        expr = query.select.projection[0]

        inject_partition_by_into_windows(expr, ["user_id"])

        result_sql = str(expr)
        # ROW_NUMBER should get PARTITION BY
        assert "PARTITION BY user_id" in result_sql

    def test_avg_over_not_modified(self):
        """AVG OVER () should not get PARTITION BY injected."""
        query = parse("SELECT value / NULLIF(AVG(value) OVER (), 0)")
        expr = query.select.projection[0]

        inject_partition_by_into_windows(expr, ["category"])

        result_sql = str(expr)
        # AVG OVER () should remain empty
        assert "AVG(value) OVER ()" in result_sql
        assert "PARTITION BY" not in result_sql
