"""Tests for cte.py - CTE building and AST transformation utilities."""

from datajunction_server.construction.build_v3.cte import (
    get_column_full_name,
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
            "v3.total_revenue": ("order_details_0", "total_revenue"),
            "v3.order_count": ("order_details_0", "order_count"),
        }

        replace_metric_refs_in_ast(expr, metric_aliases)

        # Check that columns were replaced with CTE references
        result_sql = str(expr)
        assert "order_details_0.total_revenue" in result_sql
        assert "order_details_0.order_count" in result_sql
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
            "v3.order_count": ("orders_0", "order_count"),
            "v3.visitor_count": ("visitors_0", "visitor_count"),
        }

        replace_metric_refs_in_ast(expr, metric_aliases)

        result_sql = str(expr)
        assert "orders_0.order_count" in result_sql
        assert "visitors_0.visitor_count" in result_sql

    def test_handles_multiple_references_same_metric(self):
        """Should replace all occurrences of the same metric reference."""
        query = parse("SELECT v3.revenue + v3.revenue * 0.1")
        expr = query.select.projection[0]

        metric_aliases = {
            "v3.revenue": ("cte_0", "total_revenue"),
        }

        replace_metric_refs_in_ast(expr, metric_aliases)

        result_sql = str(expr)
        # Both occurrences should be replaced
        assert result_sql.count("cte_0.total_revenue") == 2
        assert "v3.revenue" not in result_sql
