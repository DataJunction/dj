"""Tests for cte.py - CTE building and AST transformation utilities."""

import pytest

from datajunction_server.construction.build_v3.cte import (
    _build_cte_projection_map,
    _inject_filter_into_where,
    _rewrite_filter_for_cte,
    get_column_full_name,
    inject_partition_by_into_windows,
    process_metric_combiner_expression,
    replace_metric_refs_in_ast,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse

from . import assert_sql_equal


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


class TestProcessMetricCombinerExpression:
    """Tests for process_metric_combiner_expression."""

    def test_no_partition_dimensions_skips_partition_by_injection(self):
        """
        When partition_dimensions is None the function skips the PARTITION BY
        injection block.
        """
        query = parse("SELECT SUM(revenue) / NULLIF(COUNT(DISTINCT order_id), 0)")
        combiner_ast = query.select.projection[0]

        dimension_refs = {
            "v3.order_details.status": ("order_details_0", "status"),
        }

        result = process_metric_combiner_expression(
            combiner_ast=combiner_ast,
            dimension_refs=dimension_refs,
            partition_dimensions=None,
        )

        # Function should return without error; no PARTITION BY injected
        result_sql = str(result)
        assert "PARTITION BY" not in result_sql


class TestBuildCteProjectionMap:
    """Tests for _build_cte_projection_map — output-name → underlying ref."""

    def test_unaliased_qualified_column(self):
        """A table-qualified column maps its bare name to the qualified form."""
        query = parse("SELECT t.test_id FROM some.table t")
        assert _build_cte_projection_map(query) == {"test_id": "t.test_id"}

    def test_unaliased_bare_column(self):
        """A bare column maps its name to itself."""
        query = parse("SELECT test_id FROM some.table")
        assert _build_cte_projection_map(query) == {"test_id": "test_id"}

    def test_aliased_column_uses_alias_as_key(self):
        """An aliased column keys by the alias; the value is the underlying ref.

        This is the load-bearing case for pushing filters into CTEs whose
        output columns rename an upstream source column — the filter must
        reference ``o.placed_on`` (valid in WHERE) rather than the alias
        ``order_date`` (which Spark SQL rejects).
        """
        query = parse(
            "SELECT o.placed_on AS order_date FROM src o",
        )
        assert _build_cte_projection_map(query) == {
            "order_date": "o.placed_on",
        }

    def test_non_column_projection_maps_to_none(self):
        """Aggregations and other non-column projections map to None so
        callers can distinguish "unsafe to inline" from "not projected".
        """
        query = parse(
            "SELECT SUM(x) AS total, t.bare_col FROM t",
        )
        assert _build_cte_projection_map(query) == {
            "total": None,
            "bare_col": "t.bare_col",
        }

    def test_unaliased_non_column_projection_is_omitted(self):
        """A non-column projection without an alias has no addressable output
        name, so it's simply skipped — not added to the map."""
        query = parse(
            "SELECT SUM(x), t.bare_col FROM t",
        )
        assert _build_cte_projection_map(query) == {
            "bare_col": "t.bare_col",
        }

    def test_cast_around_column_is_passthrough(self):
        """``CAST(col AS T) AS col`` maps the alias to the unwrapped column,
        not None.  CAST is value-equivalent for equality/IN/range filters
        against literals of the cast target type, so filters can safely
        push into the CTE referring to the underlying column.

        This covers the LATERAL-VIEW-EXPLODE'd column pattern where the
        modeler CASTs the exploded value for type normalization
        (``CAST(max_observation_end AS INT) AS max_observation_end``).
        """
        query = parse(
            "SELECT CAST(max_observation_end AS INT) AS max_observation_end FROM t",
        )
        assert _build_cte_projection_map(query) == {
            "max_observation_end": "max_observation_end",
        }

    def test_cast_around_qualified_column_preserves_qualifier(self):
        """``CAST(t.col AS T) AS alias`` unwraps to ``t.col``, retaining the
        table qualifier in the WHERE-safe form."""
        query = parse(
            "SELECT CAST(t.placed_on AS DATE) AS order_date FROM src t",
        )
        assert _build_cte_projection_map(query) == {
            "order_date": "t.placed_on",
        }

    def test_cast_around_non_column_marks_unsafe(self):
        """``CAST`` around a non-column expression (e.g.
        ``CAST(SUM(x) AS BIGINT) AS total``) is still unsafe — only
        column-arg CASTs are passthroughs.
        """
        query = parse(
            "SELECT CAST(SUM(x) AS BIGINT) AS total FROM t",
        )
        assert _build_cte_projection_map(query) == {
            "total": None,
        }


class TestRewriteFilterForCte:
    """Tests for _rewrite_filter_for_cte."""

    def test_rewrites_alias_to_underlying_reference(self):
        """Filter on an aliased output column is rewritten to the qualified source.

        Regression: previously produced ``WHERE order_date BETWEEN ...``,
        which Spark SQL and other engines reject because SELECT-list aliases
        aren't visible in the same query's WHERE.
        """
        cte_query = parse(
            "SELECT o.placed_on AS order_date FROM src o",
        )
        rewritten = _rewrite_filter_for_cte(
            "fact.orders.order_date BETWEEN 20260216 AND 20260402",
            filter_column_aliases={
                "fact.orders.order_date": "order_date",
            },
            cte_output_cols={"order_date"},
            cte_query=cte_query,
        )
        assert str(rewritten) == "o.placed_on BETWEEN 20260216 AND 20260402"

    def test_skips_non_column_projection(self):
        """Pushdown is skipped when the target column is projected via a function."""
        cte_query = parse(
            "SELECT SUM(x) AS total FROM t",
        )
        rewritten = _rewrite_filter_for_cte(
            "v3.metric.total > 0",
            filter_column_aliases={"v3.metric.total": "total"},
            cte_output_cols={"total"},
            cte_query=cte_query,
        )
        assert rewritten is None

    def test_returns_none_when_column_absent(self):
        """Filter targeting a column the CTE doesn't output returns None."""
        cte_query = parse("SELECT foo FROM t")
        rewritten = _rewrite_filter_for_cte(
            "v3.dim.bar = 'x'",
            filter_column_aliases={"v3.dim.bar": "bar"},
            cte_output_cols={"foo"},
            cte_query=cte_query,
        )
        assert rewritten is None

    def test_falls_through_to_bare_name_when_column_pruned(self):
        """A column that's exposed by the DJ node but pruned from the CTE's
        SELECT still has a WHERE-safe reference — the bare column name, which
        the underlying source table exposes.  Column pruning trims what the
        CTE returns to the outer query, it doesn't remove the column from the
        underlying scan.
        """
        cte_query = parse("SELECT hard_hat_id, hire_date FROM src")
        rewritten = _rewrite_filter_for_cte(
            "default.hard_hat.state = 'AZ'",
            filter_column_aliases={"default.hard_hat.state": "state"},
            # The node exposes `state` even though this CTE pruned it out.
            cte_output_cols={"hard_hat_id", "hire_date", "state"},
            cte_query=cte_query,
        )
        assert str(rewritten) == "state = 'AZ'"

    def test_does_not_corrupt_substring_collisions(self):
        """A shorter dim_ref must not rewrite inside a longer similarly-prefixed
        reference.  ``fact.orders.order_date`` is in scope; an unrelated
        ``fact.orders.order_date_extended`` elsewhere in the filter must be
        left untouched.
        """
        cte_query = parse(
            "SELECT o.placed_on AS order_date FROM src o",
        )
        rewritten = _rewrite_filter_for_cte(
            "fact.orders.order_date_extended > 0 OR fact.orders.order_date < 1",
            filter_column_aliases={
                "fact.orders.order_date": "order_date",
            },
            cte_output_cols={"order_date"},
            cte_query=cte_query,
        )
        assert str(rewritten) == (
            "fact.orders.order_date_extended > 0\nOR o.placed_on < 1"
        )


# ---------------------------------------------------------------------------
# Pushdown edge cases.  Kept-xfail entries document desired behavior for
# future work; non-xfail entries assert currently-supported behavior.
# ---------------------------------------------------------------------------


class TestPushdownEdgeCases:
    """Edge cases in filter pushdown."""

    def test_multiple_dim_refs_in_one_predicate_same_cte(self):
        """A single filter referencing two dim refs both exposed by this CTE
        rewrites both — we don't stop after the first match.
        """
        cte_query = parse(
            "SELECT o.placed_on AS order_date, o.status AS state FROM src o",
        )
        rewritten = _rewrite_filter_for_cte(
            "fact.orders.order_date >= 20260101 OR fact.orders.state = 'OPEN'",
            filter_column_aliases={
                "fact.orders.order_date": "order_date",
                "fact.orders.state": "state",
            },
            cte_output_cols={"order_date", "state"},
            cte_query=cte_query,
        )
        assert str(rewritten) == ("o.placed_on >= 20260101\nOR o.status = 'OPEN'")

    def test_multiple_dim_refs_crossing_ctes_skip_entirely(self):
        """If an OR-combined predicate references a column this CTE doesn't
        expose, the whole filter must stay on the outer query.  A partial
        rewrite would leave an unresolved dim_ref inside the CTE and
        produce invalid SQL.
        """
        cte_query = parse(
            "SELECT o.placed_on AS order_date FROM src o",
        )
        rewritten = _rewrite_filter_for_cte(
            "fact.orders.order_date >= 20260101 OR fact.products.category = 'X'",
            filter_column_aliases={
                "fact.orders.order_date": "order_date",
                "fact.products.category": "category",
            },
            cte_output_cols={"order_date"},
            cte_query=cte_query,
        )
        assert rewritten is None

    def test_union_all_cte_is_handled_safely(self):
        """A CTE whose body is a UNION / UNION ALL has arms whose projections
        may differ; ``_build_cte_projection_map`` only inspects the first.
        Skip pushdown entirely for these rather than emit a rewrite valid
        for only one arm.
        """
        cte_query = parse(
            """
            SELECT a.placed_on AS order_date FROM src_a a
            UNION ALL
            SELECT b.order_date FROM src_b b
            """,
        )
        rewritten = _rewrite_filter_for_cte(
            "fact.orders.order_date > 0",
            filter_column_aliases={
                "fact.orders.order_date": "order_date",
            },
            cte_output_cols={"order_date"},
            cte_query=cte_query,
        )
        assert rewritten is None

    def test_role_suffix_on_aliased_column(self):
        """A role-suffixed dim ref targeting an aliased column rewrites to
        the underlying qualified reference and the role suffix is consumed
        with the rest of the matched dim_ref.

        Upstream ``build_filter_column_aliases`` populates both the
        role-suffixed key and a bare fallback; longest-first iteration
        ensures the role-suffixed form is matched wholesale and replaced
        before the bare key gets a chance.
        """
        cte_query = parse(
            "SELECT o.placed_on AS order_date FROM src o",
        )
        rewritten = _rewrite_filter_for_cte(
            "fact.orders.order_date[order] >= 20260101",
            filter_column_aliases={
                "fact.orders.order_date[order]": "order_date",
                "fact.orders.order_date": "order_date",
            },
            cte_output_cols={"order_date"},
            cte_query=cte_query,
        )
        assert str(rewritten) == "o.placed_on >= 20260101"

    @pytest.mark.xfail(
        strict=True,
        reason="SQL identifiers are case-insensitive for unquoted names but "
        "the projection map and alias lookup are case-sensitive dicts. "
        "Filter rewritten against a different-cased column name should still "
        "work but doesn't today.",
    )
    def test_case_insensitive_column_match(self):
        """Unquoted identifiers in SQL are case-insensitive; rewrites should follow."""
        cte_query = parse(
            "SELECT o.placed_on AS order_date FROM src o",
        )
        rewritten = _rewrite_filter_for_cte(
            "fact.orders.ORDER_DATE > 0",
            filter_column_aliases={
                "fact.orders.order_date": "order_date",
            },
            cte_output_cols={"order_date"},
            cte_query=cte_query,
        )
        assert str(rewritten) == "o.placed_on > 0"


def _normalize_sql(sql: str) -> str:
    """Collapse whitespace for stable equality on emitted SQL."""
    return " ".join(sql.split())


class TestInjectFilterIntoWhereOuterJoinSafe:
    """`_inject_filter_into_where` must not turn OUTER JOINs into INNER JOINs.

    Filters whose columns resolve entirely to the non-preserved side of an
    OUTER JOIN are wrapped around that inner-side relation as a subquery,
    not ANDed into the surrounding WHERE.
    """

    def test_right_join_filter_on_left_wraps_left_side(self):
        """`orders RIGHT JOIN customers` with filter on `orders.placed_on`
        must wrap the orders relation, not WHERE the post-join row set.
        """
        query = parse(
            "SELECT * FROM orders RIGHT JOIN customers "
            "ON orders.customer_id = customers.id",
        )
        filt = parse("SELECT 1 WHERE orders.placed_on BETWEEN 1 AND 2").select.where
        _inject_filter_into_where(query, filt)
        assert query.select.where is None
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM (
                SELECT * FROM orders WHERE orders.placed_on BETWEEN 1 AND 2
            ) orders
            RIGHT JOIN customers ON orders.customer_id = customers.id
            """,
        )

    def test_left_join_filter_on_right_wraps_right_side(self):
        """`users LEFT JOIN logins` with filter on `logins.success` must wrap
        the logins relation.
        """
        query = parse(
            "SELECT * FROM users LEFT JOIN logins ON users.id = logins.user_id",
        )
        filt = parse("SELECT 1 WHERE logins.success = 1").select.where
        _inject_filter_into_where(query, filt)
        assert query.select.where is None
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM users
            LEFT JOIN (
                SELECT * FROM logins WHERE logins.success = 1
            ) logins ON users.id = logins.user_id
            """,
        )

    def test_full_outer_join_filter_on_left_wraps_left_side(self):
        """`stocks FULL OUTER JOIN sales` makes both sides non-preserved.
        A filter on `stocks.sku` wraps the stocks relation.
        """
        query = parse(
            "SELECT * FROM stocks FULL OUTER JOIN sales ON stocks.sku = sales.sku",
        )
        filt = parse("SELECT 1 WHERE stocks.sku > 0").select.where
        _inject_filter_into_where(query, filt)
        assert query.select.where is None
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM (
                SELECT * FROM stocks WHERE stocks.sku > 0
            ) stocks
            FULL OUTER JOIN sales ON stocks.sku = sales.sku
            """,
        )

    def test_full_outer_join_filter_on_right_wraps_right_side(self):
        """Symmetric case: filter on `sales.qty` wraps the sales relation."""
        query = parse(
            "SELECT * FROM stocks FULL OUTER JOIN sales ON stocks.sku = sales.sku",
        )
        filt = parse("SELECT 1 WHERE sales.qty > 0").select.where
        _inject_filter_into_where(query, filt)
        assert query.select.where is None
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM stocks
            FULL OUTER JOIN (
                SELECT * FROM sales WHERE sales.qty > 0
            ) sales ON stocks.sku = sales.sku
            """,
        )

    def test_inner_join_filter_lands_in_where(self):
        """INNER JOIN has no preserved/non-preserved asymmetry — WHERE is safe."""
        query = parse(
            "SELECT * FROM authors INNER JOIN books ON authors.id = books.author_id",
        )
        filt = parse("SELECT 1 WHERE authors.age > 0").select.where
        _inject_filter_into_where(query, filt)
        assert query.select.where is not None
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM authors
            INNER JOIN books ON authors.id = books.author_id
            WHERE authors.age > 0
            """,
        )

    def test_filter_on_preserved_side_lands_in_where(self):
        """Filter on the preserved side of a LEFT JOIN is safe in WHERE — it
        just narrows the preserved row set, which is the user's intent.
        """
        query = parse(
            "SELECT * FROM orders LEFT JOIN refunds ON orders.id = refunds.order_id",
        )
        filt = parse("SELECT 1 WHERE orders.region = 'US'").select.where
        _inject_filter_into_where(query, filt)
        assert query.select.where is not None
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM orders
            LEFT JOIN refunds ON orders.id = refunds.order_id
            WHERE orders.region = 'US'
            """,
        )

    def test_filter_spanning_two_non_preserved_sides_falls_through_to_where(self):
        """A filter that touches columns from two distinct non-preserved sides
        can't be wrapped on a single relation — single-side wrapping wouldn't
        preserve the cross-side semantics.  Falls through to WHERE.

        ``a LEFT JOIN b LEFT JOIN c`` makes both ``b`` and ``c`` non-preserved
        against a post-WHERE filter; a predicate touching ``b.x AND c.y``
        spans both, so the classifier returns ``None`` and the filter lands
        in WHERE — covering the ``len(targets) != 1`` branch.
        """
        query = parse(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id LEFT JOIN c ON a.id = c.a_id",
        )
        # The AND-tree split routes each atom independently, so combine the
        # cross-side references into one atom (a non-AND op) to exercise the
        # multi-namespace classification path.
        single_atom = parse(
            "SELECT 1 WHERE COALESCE(b.x, 0) + COALESCE(c.y, 0) > 0",
        ).select.where
        _inject_filter_into_where(query, single_atom)
        assert query.select.where is not None
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM a
            LEFT JOIN b ON a.id = b.a_id
            LEFT JOIN c ON a.id = c.a_id
            WHERE COALESCE(b.x, 0) + COALESCE(c.y, 0) > 0
            """,
        )

    def test_filter_spanning_both_sides_splits_via_and_atoms(self):
        """A top-level AND between a non-preserved-side atom and a
        preserved-side atom splits via the AND-atom router: the
        non-preserved atom wraps its side; the preserved atom lands in
        the outer WHERE.  Together they replicate the original AND
        semantics without defeating the OUTER JOIN.
        """
        query = parse(
            "SELECT * FROM orders RIGHT JOIN customers "
            "ON orders.customer_id = customers.id",
        )
        filt = parse(
            "SELECT 1 WHERE orders.placed_on > 1 AND customers.tier = 'A'",
        ).select.where
        _inject_filter_into_where(query, filt)
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM (
                SELECT * FROM orders WHERE orders.placed_on > 1
            ) orders
            RIGHT JOIN customers ON orders.customer_id = customers.id
            WHERE customers.tier = 'A'
            """,
        )

    def test_no_join_lands_in_where(self):
        """Single-table FROM has nothing to wrap — WHERE is the only landing spot."""
        query = parse("SELECT * FROM orders")
        filt = parse("SELECT 1 WHERE orders.placed_on > 0").select.where
        _inject_filter_into_where(query, filt)
        assert query.select.where is not None
        assert_sql_equal(
            str(query),
            "SELECT * FROM orders WHERE orders.placed_on > 0",
        )

    def test_unqualified_filter_falls_through_to_where(self):
        """Without a namespace on the column ref we can't determine which side
        the filter belongs to; default to WHERE so behavior is unchanged from
        the pre-fix code path.
        """
        query = parse(
            "SELECT * FROM orders RIGHT JOIN customers "
            "ON orders.customer_id = customers.id",
        )
        filt = parse("SELECT 1 WHERE placed_on > 0").select.where
        _inject_filter_into_where(query, filt)
        assert query.select.where is not None
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM orders
            RIGHT JOIN customers ON orders.customer_id = customers.id
            WHERE placed_on > 0
            """,
        )

    def test_filter_existing_where_is_preserved(self):
        """When wrapping happens the original WHERE on the outer query is
        untouched (no double-application).
        """
        query = parse(
            "SELECT * FROM orders RIGHT JOIN customers "
            "ON orders.customer_id = customers.id "
            "WHERE customers.tier = 'A'",
        )
        filt = parse("SELECT 1 WHERE orders.placed_on > 0").select.where
        _inject_filter_into_where(query, filt)
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM (
                SELECT * FROM orders WHERE orders.placed_on > 0
            ) orders
            RIGHT JOIN customers ON orders.customer_id = customers.id
            WHERE customers.tier = 'A'
            """,
        )

    def test_consecutive_filters_flatten_into_single_wrap(self):
        """Two filters on the same non-preserved side flatten into a single
        wrap — the second call detects the existing wrap and ANDs into its
        WHERE rather than nesting another subquery layer.
        """
        query = parse(
            "SELECT * FROM orders RIGHT JOIN customers "
            "ON orders.customer_id = customers.id",
        )
        _inject_filter_into_where(
            query,
            parse("SELECT 1 WHERE orders.placed_on > 0").select.where,
        )
        _inject_filter_into_where(
            query,
            parse("SELECT 1 WHERE orders.region = 'US'").select.where,
        )
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM (
                SELECT * FROM orders
                WHERE orders.placed_on > 0 AND orders.region = 'US'
            ) orders
            RIGHT JOIN customers ON orders.customer_id = customers.id
            """,
        )

    def test_existing_subquery_target_merges_filter_into_inner_where(self):
        """When the OUTER JOIN's non-preserved side is already a subquery
        (from projection pruning in the user's transform, an explicit inline
        subquery, etc.), AND the filter atom into that subquery's WHERE
        instead of wrapping a second layer. The OUTER JOIN's NULL-fill
        semantics are preserved because the filter still runs before the
        join.
        """
        query = parse(
            "SELECT * FROM users LEFT JOIN "
            "(SELECT user_id, success FROM logins) l "
            "ON users.id = l.user_id",
        )
        filt = parse("SELECT 1 WHERE l.success = 1").select.where
        _inject_filter_into_where(query, filt)
        assert query.select.where is None
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM users
            LEFT JOIN (
                SELECT user_id, success FROM logins WHERE l.success = 1
            ) l ON users.id = l.user_id
            """,
        )

    def test_existing_subquery_with_where_ands_into_inner_where(self):
        """When the inline subquery already has its own WHERE, the injected
        filter is ANDed into it rather than wrapping or replacing.
        """
        query = parse(
            "SELECT * FROM users LEFT JOIN "
            "(SELECT user_id, success FROM logins WHERE country = 'US') l "
            "ON users.id = l.user_id",
        )
        filt = parse("SELECT 1 WHERE l.success = 1").select.where
        _inject_filter_into_where(query, filt)
        assert query.select.where is None
        assert_sql_equal(
            str(query),
            """
            SELECT * FROM users
            LEFT JOIN (
                SELECT user_id, success FROM logins
                WHERE country = 'US' AND l.success = 1
            ) l ON users.id = l.user_id
            """,
        )
