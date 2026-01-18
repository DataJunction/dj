"""
Tests for Build V3 SQL generation.

These tests cover:
- Chunk 1: Minimal Measures SQL (no joins)
- Chunk 2: Dimension Joins
- Chunk 3: Multiple Metrics
"""

import pytest
from datajunction_server.construction.build_v3.builder import (
    setup_build_context,
)

from . import assert_sql_equal, get_first_grain_group


class TestInnerCTEFlattening:
    """
    Tests for flattening inner CTEs within transforms.

    When a transform has its own WITH clause (inner CTEs), those CTEs need to be
    extracted and prefixed to avoid name collisions in the final query.
    """

    @pytest.mark.asyncio
    async def test_transform_with_inner_cte(self, client_with_build_v3):
        """
        Test that transforms with inner CTEs have those CTEs flattened and prefixed.

        Creates:
        - A transform with an inner CTE: WITH order_totals AS (...) SELECT ...
        - A metric on that transform

        The generated SQL should have:
        - v3_transform_with_cte__order_totals AS (...)  -- prefixed inner CTE
        - v3_transform_with_cte AS (SELECT ... FROM v3_transform_with_cte__order_totals)
        """
        # Create a transform that has an inner CTE
        transform_response = await client_with_build_v3.post(
            "/nodes/transform",
            json={
                "name": "v3.transform_with_cte",
                "type": "transform",
                "description": "Transform with inner CTE for testing CTE flattening",
                "query": """
                    WITH order_totals AS (
                        SELECT
                            o.order_id,
                            o.customer_id,
                            SUM(oi.quantity * oi.unit_price) AS total_amount
                        FROM v3.src_orders o
                        JOIN v3.src_order_items oi ON o.order_id = oi.order_id
                        GROUP BY o.order_id, o.customer_id
                    )
                    SELECT
                        customer_id,
                        COUNT(*) AS order_count,
                        SUM(total_amount) AS total_spent
                    FROM order_totals
                    GROUP BY customer_id
                """,
                "mode": "published",
            },
        )
        assert transform_response.status_code == 201, transform_response.json()

        # Create a metric on the transform
        metric_response = await client_with_build_v3.post(
            "/nodes/metric",
            json={
                "name": "v3.total_customer_spend",
                "type": "metric",
                "description": "Total spend across all customers",
                "query": "SELECT SUM(total_spent) FROM v3.transform_with_cte",
                "mode": "published",
            },
        )
        assert metric_response.status_code == 201, metric_response.json()

        # Request the measures SQL
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_customer_spend"],
                "dimensions": [],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())
        sql = data["sql"]
        assert_sql_equal(
            sql,
            """
            WITH v3_transform_with_cte__order_totals AS (
              SELECT
                o.order_id,
                o.customer_id,
                SUM(oi.quantity * oi.unit_price) AS total_amount
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              GROUP BY  o.order_id, o.customer_id
            ),
            v3_transform_with_cte AS (
              SELECT
                SUM(total_amount) AS total_spent
              FROM v3_transform_with_cte__order_totals order_totals
              GROUP BY  customer_id
            )
            SELECT  SUM(t1.total_spent) total_spent_sum_c9824762
            FROM v3_transform_with_cte t1
            """,
        )

    @pytest.mark.asyncio
    async def test_transform_with_multiple_inner_ctes(self, client_with_build_v3):
        """
        Test that transforms with multiple inner CTEs have all of them flattened.
        """
        # Create a transform with multiple inner CTEs
        # Note: Use full CTE names as table aliases to avoid DJ validation issues
        transform_response = await client_with_build_v3.post(
            "/nodes/transform",
            json={
                "name": "v3.transform_multi_cte",
                "type": "transform",
                "description": "Transform with multiple inner CTEs",
                "query": """
                    WITH
                        order_counts AS (
                            SELECT src.customer_id, COUNT(*) AS num_orders
                            FROM v3.src_orders src
                            GROUP BY src.customer_id
                        ),
                        item_totals AS (
                            SELECT items.order_id, SUM(items.quantity) AS total_items
                            FROM v3.src_order_items items
                            GROUP BY items.order_id
                        )
                    SELECT
                        order_counts.customer_id,
                        order_counts.num_orders,
                        SUM(item_totals.total_items) AS total_items_purchased
                    FROM order_counts
                    JOIN v3.src_orders o ON order_counts.customer_id = o.customer_id
                    JOIN item_totals ON o.order_id = item_totals.order_id
                    GROUP BY order_counts.customer_id, order_counts.num_orders
                """,
                "mode": "published",
            },
        )
        assert transform_response.status_code == 201, transform_response.json()

        # Create a metric
        metric_response = await client_with_build_v3.post(
            "/nodes/metric",
            json={
                "name": "v3.avg_items_per_customer",
                "type": "metric",
                "description": "Average items purchased per customer",
                "query": "SELECT AVG(total_items_purchased) FROM v3.transform_multi_cte",
                "mode": "published",
            },
        )
        assert metric_response.status_code == 201, metric_response.json()

        # Request the measures SQL
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.avg_items_per_customer"],
                "dimensions": [],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())
        sql = data["sql"]

        # Verify both inner CTEs are flattened with prefix
        assert "v3_transform_multi_cte__order_counts" in sql, (
            f"First inner CTE should be prefixed. Got:\n{sql}"
        )
        assert "v3_transform_multi_cte__item_totals" in sql, (
            f"Second inner CTE should be prefixed. Got:\n{sql}"
        )

        # Verify only one WITH clause
        with_count = sql.upper().count("WITH")
        assert with_count == 1, (
            f"Should have only one WITH clause, found {with_count}. Got:\n{sql}"
        )


class TestMaterialization:
    """Tests for materialization support - using physical tables instead of CTEs."""

    @pytest.mark.asyncio
    async def test_materialized_transform_uses_physical_table(
        self,
        client_with_build_v3,
    ):
        """
        Test that a materialized transform uses the physical table instead of CTE.

        Setup:
        1. Add availability state to v3.order_details transform
        2. Generate SQL for a metric using that transform
        3. Verify the SQL references the materialized table, not a CTE
        """
        # Add availability state to the order_details transform
        response = await client_with_build_v3.post(
            "/data/v3.order_details/availability/",
            json={
                "catalog": "analytics",
                "schema_": "warehouse",
                "table": "order_details_materialized",
                "valid_through_ts": 9999999999,
            },
        )
        assert response.status_code == 200, response.json()

        # Now generate SQL - should use materialized table
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()

        # Get the SQL
        assert len(data["grain_groups"]) == 1
        sql = data["grain_groups"][0]["sql"]

        # The SQL should reference the materialized table directly
        # NOT have a CTE for v3_order_details
        assert "analytics.warehouse.order_details_materialized" in sql
        assert "v3_order_details AS" not in sql, (
            "Should not have CTE for materialized transform"
        )

    @pytest.mark.asyncio
    async def test_non_materialized_transform_uses_cte(
        self,
        client_with_build_v3,
    ):
        """
        Test that a non-materialized transform uses CTE as usual.

        v3.page_views_enriched has no materialization, so it should
        generate a CTE.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.page_view_count"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()

        # Get the SQL
        assert len(data["grain_groups"]) == 1
        sql = data["grain_groups"][0]["sql"]

        # Should have CTE for page_views_enriched
        assert "v3_page_views_enriched AS" in sql

    @pytest.mark.asyncio
    async def test_materialized_dimension_uses_physical_table(
        self,
        client_with_build_v3,
    ):
        """
        Test that a materialized dimension uses physical table in joins.
        """
        # Add availability to the product dimension
        response = await client_with_build_v3.post(
            "/data/v3.product/availability/",
            json={
                "catalog": "analytics",
                "schema_": "dim",
                "table": "product_dim",
                "valid_through_ts": 9999999999,
            },
        )
        assert response.status_code == 200, response.json()

        # Generate SQL that joins to product dimension
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()

        sql = data["grain_groups"][0]["sql"]
        assert_sql_equal(
            sql,
            """
            WITH v3_order_details AS (
              SELECT
                oi.product_id,
            	oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT
              t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN analytics.dim.product_dim t2 ON t1.product_id = t2.product_id
            GROUP BY
              t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_use_materialized_false_ignores_materialization(
        self,
        client_with_build_v3,
    ):
        """
        Test that when building SQL for materialization itself,
        we don't use the materialized table (would cause circular reference).

        This tests the use_materialized=False flag.
        """
        # First materialize order_details
        response = await client_with_build_v3.post(
            "/data/v3.order_details/availability/",
            json={
                "catalog": "analytics",
                "schema_": "warehouse",
                "table": "order_details_mat",
                "valid_through_ts": 9999999999,
            },
        )
        assert response.status_code == 200

        # Generate SQL with use_materialized=False
        # This would be used when generating SQL to refresh the materialization
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "use_materialized": "false",  # Query param as string
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()

        sql = data["grain_groups"][0]["sql"]

        # Should have CTE for order_details (not use materialized table)
        assert "v3_order_details AS" in sql
        # Should NOT reference the materialized table
        assert "order_details_mat" not in sql


@pytest.mark.asyncio
class TestAddDimensionsFromMetricExpressions:
    """Test auto-detection of dimensions from metric expressions."""

    async def test_adds_dimension_from_metric_expression_when_not_requested(
        self,
        client_with_build_v3,
        session,
    ):
        """
        When a metric uses a dimension in ORDER BY (e.g., LAG) but the user
        doesn't request it and it's not in required_dimensions, the dimension
        should be auto-added to ctx.dimensions.
        """
        # Create a derived metric without required_dimensions set
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.test_wow_no_required_dims",
                "description": "WoW metric without required_dimensions",
                "query": """
                    SELECT
                        (v3.total_revenue - LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.week_code))
                        / NULLIF(LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.week_code), 0) * 100
                """,
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()

        # Query the metric without requesting the week dimension
        # The dimension should be auto-added from the metric expression
        ctx = await setup_build_context(
            session=session,
            metrics=["v3.test_wow_no_required_dims"],
            dimensions=["v3.product.category"],  # Only request category, not week
        )

        # The week dimension should have been auto-added
        assert any("week_code" in dim for dim in ctx.dimensions), (
            f"Expected week_code to be auto-added, got: {ctx.dimensions}"
        )

    async def test_dimension_not_added_when_already_covered(
        self,
        client_with_build_v3,
        session,
    ):
        """
        When a metric uses a dimension that's already covered by a user-requested
        dimension (same node.column), it should NOT be added again.
        """
        # Create a metric that uses v3.date.week in ORDER BY
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.test_wow_week_plain",
                "description": "WoW metric using plain week reference",
                "query": """
                    SELECT
                        (v3.total_revenue - LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.week))
                        / NULLIF(LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.week), 0) * 100
                """,
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()

        # Request the same dimension with a role - v3.date.week[order]
        # The metric expression uses v3.date.week (no role)
        # These should be treated as "covered" (same node.column)
        ctx = await setup_build_context(
            session=session,
            metrics=["v3.test_wow_week_plain"],
            dimensions=["v3.date.week[order]"],  # Requested WITH role
        )

        # Should only have one week dimension, not two
        week_dims = [d for d in ctx.dimensions if "week" in d]
        assert len(week_dims) == 1, (
            f"Expected only one week dimension, got: {week_dims}"
        )
