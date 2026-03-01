"""
Tests for HAVING clause support (metric filters).

These tests verify that filters on aggregated metrics are correctly
translated to HAVING clauses in the generated SQL.
"""

import pytest

from tests.construction.build_v3 import assert_sql_equal


class TestHavingClauseBasic:
    """Basic HAVING clause tests with single metric filters."""

    @pytest.mark.asyncio
    async def test_single_metric_filter_basic(
        self,
        client_with_build_v3,
    ):
        """
        Test basic metric filter generates HAVING clause.

        Query: Show categories where total_revenue > 10000
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.total_revenue > 10000"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Verify HAVING clause is present
        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            order_details_0 AS (
                SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                GROUP BY t2.category
            )
            SELECT order_details_0.category AS category,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            GROUP BY order_details_0.category
            HAVING SUM(order_details_0.line_total_sum_e1f61696) > 10000
            """,
        )

    @pytest.mark.asyncio
    async def test_metric_filter_with_less_than(
        self,
        client_with_build_v3,
    ):
        """Test metric filter with < operator."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.order_count"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.order_count < 100"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH v3_order_details AS (
              SELECT
                o.order_id,
                oi.product_id
              FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT
                product_id,
                category
              FROM default.v3.products
            ),
            order_details_0 AS (
              SELECT
                t2.category,
                t1.order_id
              FROM v3_order_details t1
              LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              GROUP BY  t2.category, t1.order_id
            )
            SELECT
              order_details_0.category AS category,
              COUNT( DISTINCT order_details_0.order_id) AS order_count
            FROM order_details_0
            GROUP BY  order_details_0.category
            HAVING  COUNT( DISTINCT order_details_0.order_id) < 100
            """,
        )

    @pytest.mark.asyncio
    async def test_metric_filter_with_greater_equal(
        self,
        client_with_build_v3,
    ):
        """Test metric filter with >= operator."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_quantity"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.total_quantity >= 500"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT oi.product_id, oi.quantity
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            order_details_0 AS (
                SELECT t2.category, SUM(t1.quantity) quantity_sum_06b64d2e
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                GROUP BY t2.category
            )
            SELECT order_details_0.category AS category,
                   SUM(order_details_0.quantity_sum_06b64d2e) AS total_quantity
            FROM order_details_0
            GROUP BY order_details_0.category
            HAVING SUM(order_details_0.quantity_sum_06b64d2e) >= 500
            """,
        )

    @pytest.mark.asyncio
    async def test_metric_filter_with_equality(
        self,
        client_with_build_v3,
    ):
        """Test metric filter with = operator."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.order_count"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.order_count = 50"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH v3_order_details AS (
              SELECT
                o.order_id,
                oi.product_id
              FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT
                product_id,
                category
              FROM default.v3.products
            ),
            order_details_0 AS (
              SELECT
                t2.category,
                t1.order_id
              FROM v3_order_details t1
              LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              GROUP BY  t2.category, t1.order_id
            )
            SELECT
              order_details_0.category AS category,
              COUNT( DISTINCT order_details_0.order_id) AS order_count
            FROM order_details_0
            GROUP BY  order_details_0.category
            HAVING  COUNT(DISTINCT order_details_0.order_id) = 50
            """,
        )


class TestHavingClauseMultipleFilters:
    """Tests for multiple metric filters (compound HAVING clauses)."""

    @pytest.mark.asyncio
    async def test_multiple_metric_filters_and(
        self,
        client_with_build_v3,
    ):
        """
        Test multiple metric filters combined with AND.

        Query: Show categories with revenue > 5000 AND order_count > 20
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.order_count"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.total_revenue > 5000",
                    "v3.order_count > 20",
                ],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH v3_order_details AS (
              SELECT
                o.order_id,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT
                product_id,
                category
              FROM default.v3.products
            ),
            order_details_0 AS (
              SELECT
                t2.category,
                t1.order_id,
                SUM(t1.line_total) line_total_sum_e1f61696
              FROM v3_order_details t1
              LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              GROUP BY  t2.category, t1.order_id
            )
            SELECT
              order_details_0.category AS category,
              SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue,
              COUNT( DISTINCT order_details_0.order_id) AS order_count
            FROM order_details_0
            GROUP BY  order_details_0.category
            HAVING
              SUM(order_details_0.line_total_sum_e1f61696) > 5000
              AND COUNT( DISTINCT order_details_0.order_id) > 20
            """,
        )

    @pytest.mark.asyncio
    async def test_metric_filter_between(
        self,
        client_with_build_v3,
    ):
        """Test metric filter with BETWEEN operator."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.total_revenue BETWEEN 1000 AND 10000"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            order_details_0 AS (
                SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                GROUP BY t2.category
            )
            SELECT order_details_0.category AS category,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            GROUP BY order_details_0.category
            HAVING SUM(order_details_0.line_total_sum_e1f61696) BETWEEN 1000 AND 10000
            """,
        )


class TestHavingClauseMixedFilters:
    """Tests for queries with both dimension filters (WHERE) and metric filters (HAVING)."""

    @pytest.mark.asyncio
    async def test_dimension_and_metric_filters(
        self,
        client_with_build_v3,
    ):
        """
        Test query with both dimension and metric filters.

        Query: Show Electronics category products with revenue > 5000
        - Dimension filter: category = 'Electronics' → WHERE
        - Metric filter: total_revenue > 5000 → HAVING
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.product.category = 'Electronics'",  # dimension filter
                    "v3.total_revenue > 5000",  # metric filter
                ],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Verify both WHERE and HAVING are present
        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            order_details_0 AS (
                SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                WHERE  t2.category = 'Electronics'
                GROUP BY t2.category
            )
            SELECT order_details_0.category AS category,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            WHERE order_details_0.category = 'Electronics'
            GROUP BY order_details_0.category
            HAVING SUM(order_details_0.line_total_sum_e1f61696) > 5000
            """,
        )

    @pytest.mark.asyncio
    async def test_complex_mixed_filters(
        self,
        client_with_build_v3,
    ):
        """
        Test complex query with multiple dimension and metric filters.

        Filters:
        - Dimension: category IN ('Electronics', 'Clothing')
        - Dimension: status = 'completed'
        - Metric: total_revenue > 10000
        - Metric: order_count >= 50
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.order_count"],
                "dimensions": ["v3.product.category", "v3.order_details.status"],
                "filters": [
                    "v3.product.category IN ('Electronics', 'Clothing')",
                    "v3.order_details.status = 'completed'",
                    "v3.total_revenue > 10000",
                    "v3.order_count >= 50",
                ],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH v3_order_details AS (
              SELECT
                o.order_id,
                o.status,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT
                product_id,
                category
              FROM default.v3.products
            ),
            order_details_0 AS (
              SELECT
                t2.category,
                t1.status,
                t1.order_id,
                SUM(t1.line_total) line_total_sum_e1f61696
              FROM v3_order_details t1
              LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              WHERE  t2.category IN ('Electronics', 'Clothing') AND t1.status = 'completed'
              GROUP BY  t2.category, t1.status, t1.order_id
            )
            SELECT
              order_details_0.category AS category,
              order_details_0.status AS status,
              SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue,
              COUNT(DISTINCT order_details_0.order_id) AS order_count
            FROM order_details_0
            WHERE  order_details_0.category IN ('Electronics', 'Clothing') AND order_details_0.status = 'completed'
            GROUP BY  order_details_0.category, order_details_0.status
            HAVING  SUM(order_details_0.line_total_sum_e1f61696) > 10000 AND COUNT( DISTINCT order_details_0.order_id) >= 50
            """,
        )


class TestHavingClauseWithDerivedMetrics:
    """Tests for HAVING clauses with derived metrics (ratios, percentages)."""

    @pytest.mark.asyncio
    async def test_derived_metric_filter(
        self,
        client_with_build_v3,
    ):
        """
        Test filter on a derived metric (ratio).

        Query: Show categories where avg_order_value > 100
        (avg_order_value = total_revenue / order_count)
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.avg_order_value"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.avg_order_value > 100"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH v3_order_details AS (
              SELECT
                o.order_id,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT
                product_id,
                category
              FROM default.v3.products
            ),
            order_details_0 AS (
              SELECT
                t2.category,
                t1.order_id,
                SUM(t1.line_total) line_total_sum_e1f61696
              FROM v3_order_details t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              GROUP BY  t2.category, t1.order_id
            )
            SELECT
              order_details_0.category AS category,
              SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT( DISTINCT order_details_0.order_id), 0) AS avg_order_value
            FROM order_details_0
            GROUP BY  order_details_0.category
            HAVING  SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT( DISTINCT order_details_0.order_id), 0) > 100
            """,
        )


class TestHavingClauseEdgeCases:
    """Edge case tests for HAVING clause functionality."""

    @pytest.mark.asyncio
    async def test_metric_filter_no_dimensions(
        self,
        client_with_build_v3,
    ):
        """
        Test metric filter with no dimensions (global aggregation).

        Query: Total revenue across all products, filtered to > 100000
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [],
                "filters": ["v3.total_revenue > 100000"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            order_details_0 AS (
                SELECT SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
            )
            SELECT SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            HAVING SUM(order_details_0.line_total_sum_e1f61696) > 100000
            """,
        )

    @pytest.mark.asyncio
    async def test_only_dimension_filters_no_having(
        self,
        client_with_build_v3,
    ):
        """
        Test that queries with only dimension filters don't generate HAVING.

        This verifies backwards compatibility - existing queries still work.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.product.category = 'Electronics'"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Verify WHERE present, HAVING absent
        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            order_details_0 AS (
                SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                WHERE  t2.category = 'Electronics'
                GROUP BY t2.category
            )
            SELECT order_details_0.category AS category,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            WHERE order_details_0.category = 'Electronics'
            GROUP BY order_details_0.category
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_only_dimension_with_metric_filter(
        self,
        client_with_build_v3,
    ):
        """
        Test metric filter with a filter-only dimension.

        Dimensions in output: [category]
        Filters:
        - subcategory = 'Smartphones' (filter-only dimension, not in output)
        - total_revenue > 5000 (metric filter)
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.product.subcategory = 'Smartphones'",
                    "v3.total_revenue > 5000",
                ],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category, subcategory
                FROM default.v3.products
            ),
            order_details_0 AS (
                SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                WHERE t2.subcategory = 'Smartphones'
                GROUP BY t2.category
            )
            SELECT order_details_0.category AS category,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            GROUP BY order_details_0.category
            HAVING SUM(order_details_0.line_total_sum_e1f61696) > 5000
            """,
        )

        # Output should only have category, not subcategory
        column_names = [col["name"] for col in result["columns"]]
        assert "category" in column_names
        assert "subcategory" not in column_names
