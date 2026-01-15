"""
Tests for pre-aggregation substitution in SQL generation.

These tests focus on the `/sql/metrics/v3` endpoint which is the primary user-facing API.
Each test validates both:
1. Measures SQL (grain group SQL) - intermediate computation
2. Metrics SQL - final query with combiner expressions applied

Key scenarios:
1. Pre-agg exists with availability -> use materialized table
2. Pre-agg exists without availability -> compute from source
3. No matching pre-agg -> compute from source
4. Grain matching (finer/coarser grain compatibility)
5. Cross-fact metrics with partial pre-agg coverage
6. use_materialized=False -> always compute from source
"""

import pytest
from . import assert_sql_equal, get_first_grain_group


class TestMetricsSQLWithPreAggregation:
    """
    Tests for metrics SQL generation with pre-aggregation substitution.

    Each test runs both measures and metrics SQL to validate the full flow:
    - Measures SQL produces grain-level data (with pre-agg substitution if available)
    - Metrics SQL applies combiner expressions to produce final metric values
    """

    @pytest.mark.asyncio
    async def test_simple_metric_no_preagg(self, client_with_build_v3):
        """
        Simple FULL aggregability metric without pre-aggregation.

        total_revenue = SUM(line_total) - FULL aggregability

        Measures SQL: Applies SUM at requested grain
        Metrics SQL: Wraps in CTE and selects (no additional aggregation needed)
        """
        # Measures SQL
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert measures_response.status_code == 200
        measures_data = get_first_grain_group(measures_response.json())

        assert_sql_equal(
            measures_data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

        # Metrics SQL
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert metrics_response.status_code == 200
        metrics_data = metrics_response.json()

        # For a simple metric, metrics SQL wraps the grain group in a CTE
        # and applies the combiner (SUM for FULL aggregability)
        assert_sql_equal(
            metrics_data["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            order_details_0 AS (
                SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                GROUP BY t1.status
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_limited_metric_no_preagg(self, client_with_build_v3):
        """
        LIMITED aggregability metric (COUNT DISTINCT) without pre-aggregation.

        order_count = COUNT(DISTINCT order_id) - LIMITED aggregability

        Measures SQL: Outputs grain column (order_id) at finest grain
        Metrics SQL: Applies COUNT(DISTINCT) combiner
        """
        # Measures SQL - outputs grain column, no aggregation
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.order_count"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert measures_response.status_code == 200
        measures_data = get_first_grain_group(measures_response.json())

        # LIMITED aggregability: grain includes order_id, no metric expression
        assert measures_data["aggregability"] == "limited"
        assert "order_id" in measures_data["grain"]
        assert_sql_equal(
            measures_data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.order_id, o.status
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, t1.order_id
            FROM v3_order_details t1
            GROUP BY t1.status, t1.order_id
            """,
        )

        # Metrics SQL - applies COUNT(DISTINCT) combiner
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.order_count"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert metrics_response.status_code == 200
        metrics_data = metrics_response.json()

        # The combiner COUNT(DISTINCT order_id) is applied here
        assert_sql_equal(
            metrics_data["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.order_id, o.status
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            order_details_0 AS (
                SELECT t1.status, t1.order_id
                FROM v3_order_details t1
                GROUP BY t1.status, t1.order_id
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   COUNT(DISTINCT order_details_0.order_id) AS order_count
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_derived_metric_no_preagg(self, client_with_build_v3):
        """
        Derived metric (ratio of two metrics) without pre-aggregation.

        avg_order_value = total_revenue / order_count
        - total_revenue: FULL aggregability (SUM)
        - order_count: LIMITED aggregability (COUNT DISTINCT)

        Measures SQL: Merged grain group with both components
        Metrics SQL: Applies combiners and divides
        """
        # Measures SQL
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.avg_order_value"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert measures_response.status_code == 200
        measures_data = get_first_grain_group(measures_response.json())

        # Merged grain group at finest grain (LIMITED dominates)
        assert measures_data["aggregability"] == "limited"
        assert "order_id" in measures_data["grain"]
        assert_sql_equal(
            measures_data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.order_id, o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, t1.order_id, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status, t1.order_id
            """,
        )

        # Metrics SQL - applies both combiners and divides
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.avg_order_value"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert metrics_response.status_code == 200
        metrics_data = metrics_response.json()

        # avg_order_value = SUM(total_revenue) / COUNT(DISTINCT order_id)
        assert_sql_equal(
            metrics_data["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.order_id, o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            order_details_0 AS (
                SELECT t1.status, t1.order_id, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                GROUP BY t1.status, t1.order_id
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT(DISTINCT order_details_0.order_id), 0) AS avg_order_value
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_simple_metric_with_preagg(self, client_with_build_v3):
        """
        Simple FULL aggregability metric WITH pre-aggregation available.

        Both measures and metrics SQL should use the pre-agg table.
        """
        # Create pre-agg
        plan_response = await client_with_build_v3.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert plan_response.status_code == 201
        preagg = plan_response.json()["preaggs"][0]

        # Set availability
        await client_with_build_v3.post(
            f"/preaggs/{preagg['id']}/availability/",
            json={
                "catalog": "warehouse",
                "schema_": "preaggs",
                "table": "v3_revenue_by_status",
                "valid_through_ts": 20250103,
            },
        )

        # Measures SQL - should use pre-agg table
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert measures_response.status_code == 200
        measures_data = get_first_grain_group(measures_response.json())

        # Pre-agg table has hashed column name for the measure
        assert_sql_equal(
            measures_data["sql"],
            """
            SELECT status, SUM(line_total_sum_e1f61696) line_total_sum_e1f61696
            FROM warehouse.preaggs.v3_revenue_by_status
            GROUP BY status
            """,
        )

        # Metrics SQL - should also use pre-agg table
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert metrics_response.status_code == 200
        metrics_data = metrics_response.json()

        assert_sql_equal(
            metrics_data["sql"],
            """
            WITH order_details_0 AS (
              SELECT
                status,
                SUM(line_total_sum_e1f61696) line_total_sum_e1f61696
              FROM warehouse.preaggs.v3_revenue_by_status
              GROUP BY  status
            )
            SELECT
              COALESCE(order_details_0.status) AS status,
              SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            GROUP BY  order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_derived_metric_with_preagg(self, client_with_build_v3):
        """
        Derived metric with pre-aggregation available for underlying metrics.

        avg_order_value = total_revenue / order_count

        Pre-agg has both total_revenue and order_count at status grain.
        """
        # Create pre-agg with both underlying metrics
        plan_response = await client_with_build_v3.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.total_revenue", "v3.order_count"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert plan_response.status_code == 201
        preagg = plan_response.json()["preaggs"][0]

        await client_with_build_v3.post(
            f"/preaggs/{preagg['id']}/availability/",
            json={
                "catalog": "warehouse",
                "schema_": "preaggs",
                "table": "v3_order_metrics",
                "valid_through_ts": 20250103,
            },
        )

        # Metrics SQL for derived metric
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.avg_order_value"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert metrics_response.status_code == 200
        metrics_data = metrics_response.json()

        # Uses pre-agg table, applies combiners
        # Pre-agg has: total_revenue (aggregated) and order_id grain column (for order_count)
        # Note: aggregated columns appear before non-aggregated grain columns
        assert_sql_equal(
            metrics_data["sql"],
            """
            WITH order_details_0 AS (
                SELECT status,
                       SUM(line_total_sum_e1f61696) line_total_sum_e1f61696,
                       order_id
                FROM warehouse.preaggs.v3_order_metrics
                GROUP BY status, order_id
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT(DISTINCT order_details_0.order_id), 0) AS avg_order_value
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )


class TestPreAggGrainMatching:
    """Tests for grain compatibility when using pre-aggregations."""

    @pytest.mark.asyncio
    async def test_preagg_at_finer_grain_rolls_up(self, client_with_build_v3):
        """
        Pre-agg at finer grain than requested can be used with roll-up.

        Pre-agg: status + customer_id grain
        Request: status grain only
        Result: SELECT from pre-agg, GROUP BY status (rolls up customer_id)
        """
        # Create pre-agg at finer grain
        plan_response = await client_with_build_v3.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.order_details.status",
                    "v3.order_details.customer_id",
                ],
            },
        )
        assert plan_response.status_code == 201
        preagg = plan_response.json()["preaggs"][0]

        await client_with_build_v3.post(
            f"/preaggs/{preagg['id']}/availability/",
            json={
                "catalog": "warehouse",
                "schema_": "preaggs",
                "table": "v3_revenue_by_status_customer",
                "valid_through_ts": 20250103,
            },
        )

        # Request at coarser grain
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert metrics_response.status_code == 200
        metrics_data = metrics_response.json()

        # Uses pre-agg and rolls up customer_id
        assert_sql_equal(
            metrics_data["sql"],
            """
            WITH order_details_0 AS (
                SELECT status, SUM(line_total_sum_e1f61696) line_total_sum_e1f61696
                FROM warehouse.preaggs.v3_revenue_by_status_customer
                GROUP BY status
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_preagg_at_coarser_grain_not_used(self, client_with_build_v3):
        """
        Pre-agg at coarser grain than requested CANNOT be used.

        Pre-agg: status grain only
        Request: status + customer_id grain
        Result: Must compute from source (can't invent customer_id values)
        """
        # Create pre-agg at coarser grain
        plan_response = await client_with_build_v3.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert plan_response.status_code == 201
        preagg = plan_response.json()["preaggs"][0]

        await client_with_build_v3.post(
            f"/preaggs/{preagg['id']}/availability/",
            json={
                "catalog": "warehouse",
                "schema_": "preaggs",
                "table": "v3_revenue_by_status_only",
                "valid_through_ts": 20250103,
            },
        )

        # Request at finer grain - must compute from source
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.order_details.status",
                    "v3.order_details.customer_id",
                ],
            },
        )
        assert metrics_response.status_code == 200
        metrics_data = metrics_response.json()

        # Should NOT use pre-agg, computes from source
        assert "warehouse.preaggs" not in metrics_data["sql"]
        assert (
            "v3_order_details" in metrics_data["sql"]
            or "v3.orders" in metrics_data["sql"]
        )


class TestCrossFactMetrics:
    """Tests for cross-fact derived metrics with pre-aggregation."""

    @pytest.mark.asyncio
    async def test_cross_fact_with_partial_preagg(self, client_with_build_v3):
        """
        Cross-fact metric where one fact has pre-agg, other doesn't.

        revenue_per_visitor = total_revenue (order_details) / visitor_count (page_views)

        Pre-agg exists for total_revenue at customer_id grain.
        No pre-agg for visitor_count.
        """
        # Create pre-agg for order_details only
        plan_response = await client_with_build_v3.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.customer.customer_id"],
            },
        )
        assert plan_response.status_code == 201
        preagg = plan_response.json()["preaggs"][0]

        await client_with_build_v3.post(
            f"/preaggs/{preagg['id']}/availability/",
            json={
                "catalog": "warehouse",
                "schema_": "preaggs",
                "table": "v3_revenue_by_customer",
                "valid_through_ts": 20250103,
            },
        )

        # Request cross-fact metric
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.revenue_per_visitor"],
                "dimensions": ["v3.customer.customer_id"],
            },
        )
        assert metrics_response.status_code == 200
        metrics_data = metrics_response.json()

        # order_details CTE uses pre-agg, page_views CTE computes from source
        assert_sql_equal(
            metrics_data["sql"],
            """
            WITH
            v3_page_views_enriched AS (
            SELECT  customer_id
            FROM default.v3.page_views
            ),
            order_details_0 AS (
            SELECT  customer_id,
                SUM(line_total_sum_e1f61696) line_total_sum_e1f61696
            FROM warehouse.preaggs.v3_revenue_by_customer
            GROUP BY  customer_id
            ),
            page_views_enriched_0 AS (
            SELECT  t1.customer_id,
                t1.customer_id
            FROM v3_page_views_enriched t1
            GROUP BY  t1.customer_id, t1.customer_id
            )

            SELECT  COALESCE(order_details_0.customer_id, page_views_enriched_0.customer_id) AS customer_id,
                SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT( DISTINCT page_views_enriched_0.customer_id), 0) AS revenue_per_visitor
            FROM order_details_0 FULL OUTER JOIN page_views_enriched_0 ON order_details_0.customer_id = page_views_enriched_0.customer_id
            GROUP BY  order_details_0.customer_id
            """,
        )

    async def test_cross_fact_with_full_preagg_coverage(self, client_with_build_v3):
        """
        Cross-fact metric where both facts have materialized pre-aggs.

        Both grain groups should read from their respective pre-agg tables,
        then FULL OUTER JOIN on the shared dimension.
        """
        # Create pre-agg for order_details (revenue)
        plan1 = await client_with_build_v3.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.customer.customer_id"],
            },
        )
        preagg1 = plan1.json()["preaggs"][0]
        await client_with_build_v3.post(
            f"/preaggs/{preagg1['id']}/availability/",
            json={
                "catalog": "warehouse",
                "schema_": "preaggs",
                "table": "v3_revenue_by_customer",
                "valid_through_ts": 20250103,
            },
        )

        # Create pre-agg for page_views (visitor count)
        plan2 = await client_with_build_v3.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.page_view_count"],  # or similar metric from page_views
                "dimensions": ["v3.customer.customer_id"],
            },
        )
        preagg2 = plan2.json()["preaggs"][0]
        await client_with_build_v3.post(
            f"/preaggs/{preagg2['id']}/availability/",
            json={
                "catalog": "warehouse",
                "schema_": "preaggs",
                "table": "v3_visitors_by_customer",
                "valid_through_ts": 20250103,
            },
        )

        # Request cross-fact metric - should use BOTH pre-aggs
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.revenue_per_visitor"],
                "dimensions": ["v3.customer.customer_id"],
            },
        )
        # Assert: Both CTEs read from pre-agg tables, FULL OUTER JOINed
        sql = response.json()["sql"]
        assert_sql_equal(
            sql,
            """
            WITH v3_page_views_enriched AS (
            SELECT  customer_id
            FROM default.v3.page_views
            ),
            order_details_0 AS (
            SELECT  customer_id,
                SUM(line_total_sum_e1f61696) line_total_sum_e1f61696
            FROM warehouse.preaggs.v3_revenue_by_customer
            GROUP BY  customer_id
            ),
            page_views_enriched_0 AS (
            SELECT  t1.customer_id,
                t1.customer_id
            FROM v3_page_views_enriched t1
            GROUP BY  t1.customer_id, t1.customer_id
            )
            SELECT  COALESCE(order_details_0.customer_id, page_views_enriched_0.customer_id) AS customer_id,
                SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT( DISTINCT page_views_enriched_0.customer_id), 0) AS revenue_per_visitor
            FROM order_details_0 FULL OUTER JOIN page_views_enriched_0 ON order_details_0.customer_id = page_views_enriched_0.customer_id
            GROUP BY  order_details_0.customer_id
            """,
        )

    @pytest.mark.asyncio
    async def test_cross_fact_without_shared_dimension_errors(
        self,
        client_with_build_v3,
    ):
        """
        Cross-fact metrics require at least one shared dimension for joining.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.revenue_per_visitor"],
                "dimensions": [],  # No shared dimension!
            },
        )

        assert response.status_code == 422
        result = response.json()
        assert "require at least one shared dimension" in result["message"]


class TestUseMaterializedFlag:
    """Tests for the use_materialized flag behavior."""

    @pytest.mark.asyncio
    async def test_use_materialized_false_ignores_preagg(self, client_with_build_v3):
        """
        When use_materialized=False, always compute from source.

        This is used when generating SQL for materialization refresh
        to avoid circular references.
        """
        # Create pre-agg with availability
        plan_response = await client_with_build_v3.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.total_quantity"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert plan_response.status_code == 201
        preagg = plan_response.json()["preaggs"][0]

        await client_with_build_v3.post(
            f"/preaggs/{preagg['id']}/availability/",
            json={
                "catalog": "warehouse",
                "schema_": "preaggs",
                "table": "v3_quantity_preagg",
                "valid_through_ts": 20250103,
            },
        )

        # Request with use_materialized=False
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_quantity"],
                "dimensions": ["v3.order_details.status"],
                "use_materialized": "false",
            },
        )
        assert metrics_response.status_code == 200
        metrics_data = metrics_response.json()

        # Should compute from source, not use pre-agg
        assert "warehouse.preaggs" not in metrics_data["sql"]
        assert (
            "v3_order_details" in metrics_data["sql"]
            or "v3.orders" in metrics_data["sql"]
        )

    @pytest.mark.asyncio
    async def test_preagg_without_availability_computes_from_source(
        self,
        client_with_build_v3,
    ):
        """
        Pre-agg exists but has no availability -> compute from source.
        """
        # Create pre-agg but don't set availability
        plan_response = await client_with_build_v3.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert plan_response.status_code == 201
        # Do NOT set availability

        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert metrics_response.status_code == 200
        metrics_data = metrics_response.json()

        # Should compute from source
        assert "warehouse.preaggs" not in metrics_data["sql"]
