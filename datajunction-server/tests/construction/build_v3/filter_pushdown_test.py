"""
Tests for filter pushdown into CTEs.

Verifies that user-supplied dimension filters are pushed down into the
appropriate CTE WHERE clauses, not just applied on the outer query.
"""

import pytest

from tests.construction.build_v3 import assert_sql_equal, get_first_grain_group


class TestFilterPushdownToParentCTE:
    """Filters on dimensions linked via FK on the parent node push into the parent CTE."""

    @pytest.mark.asyncio
    async def test_date_filter_pushed_to_parent_cte(
        self,
        client_with_build_v3,
    ):
        """
        Filter on v3.date.date_id[order] should push order_date filter into
        the v3_order_details CTE (parent node) via the FK dimension link.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.date.date_id[order] >= 20240101"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.order_date >= 20240101
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )


class TestFilterPushdownToDimensionCTE:
    """Filters on dimension columns push into the dimension node's CTE."""

    @pytest.mark.asyncio
    async def test_product_filter_pushed_to_dimension_cte(
        self,
        client_with_build_v3,
    ):
        """
        Filter on v3.product.category should push into the v3_product CTE.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.product.category = 'Electronics'"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
              WHERE category = 'Electronics'
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics'
            GROUP BY t2.category
            """,
        )


class TestFilterPushdownMultiple:
    """Multiple filters push into their respective CTEs."""

    @pytest.mark.asyncio
    async def test_multiple_filters_pushed_to_different_ctes(
        self,
        client_with_build_v3,
    ):
        """
        Date filter → parent CTE (via FK), product filter → product dim CTE.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.date.date_id[order] >= 20240101",
                    "v3.product.category = 'Electronics'",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.order_date >= 20240101
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
              WHERE category = 'Electronics'
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics'
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_two_filters_on_same_parent_cte_compose_as_and(
        self,
        client_with_build_v3,
    ):
        """Two user filters on distinct columns of the same parent compose
        as AND in both the CTE's WHERE and the outer WHERE — no duplicates,
        no dropped predicate.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.date.date_id[order] >= 20240101",
                    "v3.order_details.status = 'completed'",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                o.status,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.order_date >= 20240101 AND o.status = 'completed'
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )


class TestFilterPushdownMultiRef:
    """A single filter predicate can reference multiple dim refs (OR-combined)."""

    @pytest.mark.asyncio
    async def test_or_predicate_both_refs_in_same_cte_pushed_down(
        self,
        client_with_build_v3,
    ):
        """OR-combined refs that both resolve to the same CTE push down as
        one predicate.  Both refs are fully qualified — bare column refs in
        filters are ambiguous across multi-parent builds and aren't a
        supported input shape.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.date.date_id[order] >= 20240101 "
                    "OR v3.order_details.status = 'completed'",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                o.status,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.order_date >= 20240101 OR o.status = 'completed'
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_or_predicate_crossing_ctes_stays_on_outer_query(
        self,
        client_with_build_v3,
    ):
        """An OR that mixes columns from different CTEs must not push into
        either CTE — a partial rewrite would reference an unresolved name
        inside a CTE.  The whole predicate stays on the outer WHERE.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.product.category = 'Electronics' "
                    "OR v3.date.date_id[order] >= 20240101",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics' OR t1.order_date >= 20240101
            GROUP BY t2.category
            """,
        )


class TestFilterPushdownEdgeCases:
    """Edge cases for filter pushdown."""

    @pytest.mark.asyncio
    async def test_no_filters_no_pushdown(
        self,
        client_with_build_v3,
    ):
        """No filters → CTEs should not have injected WHERE clauses."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_on_local_dimension(
        self,
        client_with_build_v3,
    ):
        """Filter on a local dimension (column directly on parent) pushes to parent CTE."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status = 'completed'"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.status,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.status = 'completed'
            )
            SELECT t1.status,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )
