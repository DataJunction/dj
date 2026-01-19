import pytest
from . import assert_sql_equal


class TestMetricsSQLBasic:
    @pytest.mark.asyncio
    async def test_basic_metrics_sql(self, client_with_build_v3):
        """Test that metrics SQL endpoint returns valid SQL."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        # Should return 200 OK with SQL
        assert response.status_code == 200
        result = response.json()
        assert "sql" in result
        assert result["sql"]
        assert "SELECT" in result["sql"].upper()

    @pytest.mark.asyncio
    async def test_simple_single_metric(self, client_with_build_v3):
        """
        Test metrics SQL for a single simple metric (SUM).

        Even for single grain groups, the unified generate_metrics_sql
        wraps the result in a grain group CTE (e.g., order_details_0) for consistency.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Should have SQL output with shared CTEs, grain group wrapper,
        # and re-aggregation in final SELECT (always applied for consistency)
        assert_sql_equal(
            result["sql"],
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

        # Should have columns (names match SQL AS aliases)
        assert result["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "total_revenue",
                "type": "double",
                "semantic_entity": "v3.total_revenue",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_multiple_metrics_same_grain(self, client_with_build_v3):
        """
        Test metrics SQL for multiple metrics from the same parent.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
            SELECT  o.status,
                oi.quantity,
                oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            order_details_0 AS (
            SELECT  t1.status,
                SUM(t1.line_total) line_total_sum_e1f61696,
                SUM(t1.quantity) quantity_sum_06b64d2e
            FROM v3_order_details t1
            GROUP BY  t1.status
            )
            SELECT  COALESCE(order_details_0.status) AS status,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue,
                SUM(order_details_0.quantity_sum_06b64d2e) AS total_quantity
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )
        assert result["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "total_revenue",
                "type": "double",
                "semantic_entity": "v3.total_revenue",
                "semantic_type": "metric",
            },
            {
                "name": "total_quantity",
                "type": "bigint",
                "semantic_entity": "v3.total_quantity",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_multi_component_metric(self, client_with_build_v3):
        """
        Test metrics SQL for a multi-component metric (AVG).

        AVG decomposes into SUM and COUNT, and the combiner expression
        should be applied: SUM(x) / COUNT(x).
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.avg_unit_price"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Should have SQL output with flattened CTEs and GROUP BY for consistency
        sql = result["sql"]
        assert_sql_equal(
            sql,
            """
            WITH
            v3_order_details AS (
            SELECT  o.status,
                oi.unit_price
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            order_details_0 AS (
            SELECT  t1.status,
                COUNT(t1.unit_price) unit_price_count_55cff00f,
                SUM(t1.unit_price) unit_price_sum_55cff00f
            FROM v3_order_details t1
            GROUP BY  t1.status
            )
            SELECT  COALESCE(order_details_0.status) AS status,
                SUM(order_details_0.unit_price_sum_55cff00f) / SUM(order_details_0.unit_price_count_55cff00f) AS avg_unit_price
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )
        assert result["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "avg_unit_price",
                "type": "double",
                "semantic_entity": "v3.avg_unit_price",
                "semantic_type": "metric",
            },
        ]


class TestMetricsSQLDerived:
    @pytest.mark.asyncio
    async def test_derived_metric_ratio(self, client_with_build_v3):
        """
        Test metrics SQL for a derived metric (conversion_rate = order_count / visitor_count).

        This is a cross-fact derived metric that requires:
        1. Computing order_count from order_details (COUNT DISTINCT order_id)
        2. Computing visitor_count from page_views (COUNT DISTINCT customer_id)
        3. Dividing them to get conversion_rate

        Only the requested metric (conversion_rate) is in the output - base metrics
        are computed internally but not exposed.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.conversion_rate"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.order_id, oi.product_id
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            v3_page_views_enriched AS (
                SELECT customer_id, product_id
                FROM default.v3.page_views
            ),
            order_details_0 AS (
                SELECT t2.category, t1.order_id
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                GROUP BY t2.category, t1.order_id
            ),
            page_views_enriched_0 AS (
                SELECT t2.category, t1.customer_id
                FROM v3_page_views_enriched t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                GROUP BY t2.category, t1.customer_id
            )
            SELECT COALESCE(order_details_0.category, page_views_enriched_0.category) AS category,
                   CAST(COUNT(DISTINCT order_details_0.order_id) AS DOUBLE) / NULLIF(COUNT(DISTINCT page_views_enriched_0.customer_id), 0) AS conversion_rate
            FROM order_details_0
            FULL OUTER JOIN page_views_enriched_0 ON order_details_0.category = page_views_enriched_0.category
            GROUP BY order_details_0.category
            """,
        )
        # Only the derived metric appears in output (not base metrics)
        assert result["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",  # Full dimension reference
                "semantic_type": "dimension",
            },
            {
                "name": "conversion_rate",
                "type": "double",
                "semantic_entity": "v3.conversion_rate",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_multiple_derived_metrics_same_fact(self, client_with_build_v3):
        """
        Test metrics SQL for a derived metric from the same fact.

        avg_order_value = total_revenue / order_count (both from order_details)

        This uses different aggregabilities:
        - total_revenue: FULL (SUM)
        - order_count: LIMITED (COUNT DISTINCT order_id)

        Only the requested metric (avg_order_value) is in the output.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.avg_order_value", "v3.avg_items_per_order"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # With merged grain groups:
        # - CTE aggregates FULL components at finest grain (order_id level)
        # - Final SELECT re-aggregates to requested grain (status level) with GROUP BY
        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.order_id, o.status, oi.quantity, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            order_details_0 AS (
                SELECT t1.status, t1.order_id, SUM(t1.line_total) line_total_sum_e1f61696, SUM(t1.quantity) quantity_sum_06b64d2e
                FROM v3_order_details t1
                GROUP BY t1.status, t1.order_id
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT(DISTINCT order_details_0.order_id), 0) AS avg_order_value,
                   SUM(order_details_0.quantity_sum_06b64d2e) / NULLIF(COUNT(DISTINCT order_details_0.order_id), 0) AS avg_items_per_order
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )
        # Only the derived metrics appear in output (not base metrics)
        assert result["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",  # Full dimension reference
                "semantic_type": "dimension",
            },
            {
                "name": "avg_order_value",
                "type": "double",
                "semantic_entity": "v3.avg_order_value",
                "semantic_type": "metric",
            },
            {
                "name": "avg_items_per_order",
                "type": "bigint",
                "semantic_entity": "v3.avg_items_per_order",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_derived_metrics_cross_fact(self, client_with_build_v3):
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": [
                    "v3.conversion_rate",
                    "v3.revenue_per_visitor",
                    "v3.revenue_per_page_view",
                ],
                "dimensions": [
                    "v3.product.category",
                    "v3.customer.name[customer]",
                ],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # With merged grain groups, we get one CTE per parent
        # with raw values and aggregations applied in the final SELECT
        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_customer AS (
            SELECT  customer_id,
                name
            FROM default.v3.customers
            ),
            v3_order_details AS (
            SELECT  o.order_id,
                o.customer_id,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
            SELECT  product_id,
                category
            FROM default.v3.products
            ),
            v3_page_views_enriched AS (
            SELECT  view_id,
                customer_id,
                product_id
            FROM default.v3.page_views
            ),
            order_details_0 AS (
            SELECT  t2.category,
                t3.name name_customer,
                t1.order_id,
                SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            LEFT OUTER JOIN v3_customer t3 ON t1.customer_id = t3.customer_id
            GROUP BY  t2.category, t3.name, t1.order_id
            ),
            page_views_enriched_0 AS (
            SELECT  t2.category,
                t3.name name_customer,
                t1.customer_id,
                COUNT(t1.view_id) view_id_count_f41e2db4
            FROM v3_page_views_enriched t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            LEFT OUTER JOIN v3_customer t3 ON t1.customer_id = t3.customer_id
            GROUP BY  t2.category, t3.name, t1.customer_id
            )

            SELECT  COALESCE(order_details_0.category, page_views_enriched_0.category) AS category,
                COALESCE(order_details_0.name_customer, page_views_enriched_0.name_customer) AS name_customer,
                CAST(COUNT( DISTINCT order_details_0.order_id) AS DOUBLE) / NULLIF(COUNT( DISTINCT page_views_enriched_0.customer_id), 0) AS conversion_rate,
                SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT( DISTINCT page_views_enriched_0.customer_id), 0) AS revenue_per_visitor,
                SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(SUM(page_views_enriched_0.view_id_count_f41e2db4), 0) AS revenue_per_page_view
            FROM order_details_0 FULL OUTER JOIN page_views_enriched_0 ON order_details_0.category = page_views_enriched_0.category AND order_details_0.name_customer = page_views_enriched_0.name_customer
            GROUP BY  order_details_0.category, order_details_0.name_customer
            """,
        )
        assert result["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",  # Full dimension reference
                "semantic_type": "dimension",
            },
            {
                "name": "name_customer",
                "type": "string",
                "semantic_entity": "v3.customer.name[customer]",  # Full with role
                "semantic_type": "dimension",
            },
            {
                "name": "conversion_rate",
                "type": "double",
                "semantic_entity": "v3.conversion_rate",
                "semantic_type": "metric",
            },
            {
                "name": "revenue_per_visitor",
                "type": "double",
                "semantic_entity": "v3.revenue_per_visitor",
                "semantic_type": "metric",
            },
            {
                "name": "revenue_per_page_view",
                "type": "double",
                "semantic_entity": "v3.revenue_per_page_view",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_pages_per_session_same_fact_derived(self, client_with_build_v3):
        """
        Test same-fact derived metric.

        v3.pages_per_session = v3.page_view_count / v3.session_count
        Both base metrics are from page_views_enriched.

        With grain group merging, there's one CTE with raw values
        and aggregations are applied in the final SELECT.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.pages_per_session"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # With merged grain groups, we get a single CTE with raw values
        # and aggregations in the final SELECT
        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_page_views_enriched AS (
            SELECT  view_id,
                session_id,
                product_id
            FROM default.v3.page_views
            ),
            v3_product AS (
            SELECT  product_id,
                category
            FROM default.v3.products
            ),
            page_views_enriched_0 AS (
            SELECT  t2.category,
                t1.session_id,
                COUNT(t1.view_id) view_id_count_f41e2db4
            FROM v3_page_views_enriched t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY  t2.category, t1.session_id
            )

            SELECT  COALESCE(page_views_enriched_0.category) AS category,
                SUM(page_views_enriched_0.view_id_count_f41e2db4) / NULLIF(COUNT( DISTINCT page_views_enriched_0.session_id), 0) AS pages_per_session
            FROM page_views_enriched_0
            GROUP BY  page_views_enriched_0.category
            """,
        )

        assert result["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "pages_per_session",
                "type": "bigint",
                "semantic_entity": "v3.pages_per_session",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_mom_revenue_change_metrics_sql(self, client_with_build_v3):
        """
        Test month-over-month revenue change through metrics SQL.

        v3.mom_revenue_change uses LAG() window function to compare
        current month revenue with previous month.

        Window function metrics use a base_metrics CTE to pre-compute
        base metrics before applying the window function.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.mom_revenue_change"],
                "dimensions": ["v3.date.month[order]"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_date AS (
                SELECT date_id, month
                FROM default.v3.dates
            ),
            v3_order_details AS (
                SELECT o.order_date, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            order_details_0 AS (
                SELECT t2.month month_order, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_date t2 ON t1.order_date = t2.date_id
                GROUP BY t2.month
            ),
            base_metrics AS (
                SELECT
                    COALESCE(order_details_0.month_order) AS month_order,
                    SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
                FROM order_details_0
                GROUP BY order_details_0.month_order
            )
            SELECT
                base_metrics.month_order AS month_order,
                (base_metrics.total_revenue - LAG(base_metrics.total_revenue, 1) OVER (ORDER BY base_metrics.month_order))
                    / NULLIF(LAG(base_metrics.total_revenue, 1) OVER (ORDER BY base_metrics.month_order), 0) * 100
                    AS mom_revenue_change
            FROM base_metrics
            """,
        )

        assert result["columns"] == [
            {
                "name": "month_order",
                "type": "int",
                "semantic_entity": "v3.date.month[order]",
                "semantic_type": "dimension",
            },
            {
                "name": "mom_revenue_change",
                "type": "double",
                "semantic_entity": "v3.mom_revenue_change",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_mom_without_order_role(self, client_with_build_v3):
        """
        Test MoM metric when dimension is requested without [order] role.

        This tests the case where neither the metric's ORDER BY nor the
        requested dimension have role suffixes.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.mom_revenue_change"],
                "dimensions": ["v3.date.month"],  # No [order] suffix
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Should still generate correct SQL with grain-level CTE
        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_date AS (
                SELECT date_id, month
                FROM default.v3.dates
            ),
            v3_order_details AS (
                SELECT o.order_date, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            order_details_0 AS (
                SELECT t2.month, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_date t2 ON t1.order_date = t2.date_id
                GROUP BY t2.month
            ),
            base_metrics AS (
                SELECT
                    COALESCE(order_details_0.month) AS month,
                    SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
                FROM order_details_0
                GROUP BY order_details_0.month
            )
            SELECT
                base_metrics.month AS month,
                (base_metrics.total_revenue - LAG(base_metrics.total_revenue, 1) OVER (ORDER BY base_metrics.month))
                    / NULLIF(LAG(base_metrics.total_revenue, 1) OVER (ORDER BY base_metrics.month), 0) * 100
                    AS mom_revenue_change
            FROM base_metrics
            """,
        )

        assert result["columns"] == [
            {
                "name": "month",
                "type": "int",
                "semantic_entity": "v3.date.month",
                "semantic_type": "dimension",
            },
            {
                "name": "mom_revenue_change",
                "type": "double",
                "semantic_entity": "v3.mom_revenue_change",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_wow_and_mom_without_order_role(self, client_with_build_v3):
        """
        Test WoW and MoM metrics when neither uses [order] role suffix.

        Both metrics and dimensions have no role suffix, testing the
        pure no-suffix case for the grain-level CTE logic.
        """
        # Create the metric locally for this test
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.wow_revenue_change_no_role",
                "description": "Week-over-week revenue change (%) - without [order] role suffix",
                "query": """
                    SELECT
                        (v3.total_revenue - LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.week))
                        / NULLIF(LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.week), 0) * 100
                """,
                "mode": "published",
                "required_dimensions": ["v3.date.week"],
            },
        )
        assert response.status_code in (200, 201), response.json()

        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": [
                    "v3.wow_revenue_change_no_role",
                    "v3.mom_revenue_change",
                ],
                "dimensions": [
                    "v3.product.category",
                    "v3.date.week",
                    "v3.date.month",
                ],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Both WoW and MoM should get grain-level CTEs since we're requesting
        # category + week + month, but each metric operates at a different grain
        assert_sql_equal(
            result["sql"],
            """
            WITH v3_date AS (
              SELECT date_id, week, month
              FROM default.v3.dates
            ),
            v3_order_details AS (
              SELECT
                o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            ),
            order_details_0 AS (
              SELECT
                t2.category,
                t3.week,
                t3.month,
                SUM(t1.line_total) line_total_sum_e1f61696
              FROM v3_order_details t1
              LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
              GROUP BY t2.category, t3.week, t3.month
            ),
            base_metrics AS (
              SELECT
                COALESCE(order_details_0.category) AS category,
                COALESCE(order_details_0.week) AS week,
                COALESCE(order_details_0.month) AS month,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.week, order_details_0.month
            ),
            order_details_week_agg AS (
              SELECT
                order_details_0.category AS category,
                order_details_0.week AS week,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.week
            ),
            order_details_week AS (
              SELECT
                order_details_week_agg.category AS category,
                order_details_week_agg.week AS week,
                (order_details_week_agg.total_revenue - LAG(order_details_week_agg.total_revenue, 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week))
                  / NULLIF(LAG(order_details_week_agg.total_revenue, 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week), 0) * 100
                  AS wow_revenue_change_no_role
              FROM order_details_week_agg
            ),
            order_details_month_agg AS (
              SELECT
                order_details_0.category AS category,
                order_details_0.month AS month,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.month
            ),
            order_details_month AS (
              SELECT
                order_details_month_agg.category AS category,
                order_details_month_agg.month AS month,
                (order_details_month_agg.total_revenue - LAG(order_details_month_agg.total_revenue, 1) OVER (PARTITION BY order_details_month_agg.category ORDER BY order_details_month_agg.month))
                  / NULLIF(LAG(order_details_month_agg.total_revenue, 1) OVER (PARTITION BY order_details_month_agg.category ORDER BY order_details_month_agg.month), 0) * 100
                  AS mom_revenue_change
              FROM order_details_month_agg
            )
            SELECT
              base_metrics.category AS category,
              base_metrics.week AS week,
              base_metrics.month AS month,
              order_details_week.wow_revenue_change_no_role AS wow_revenue_change_no_role,
              order_details_month.mom_revenue_change AS mom_revenue_change
            FROM base_metrics
            LEFT OUTER JOIN order_details_week ON base_metrics.category = order_details_week.category AND base_metrics.week = order_details_week.week
            LEFT OUTER JOIN order_details_month ON base_metrics.category = order_details_month.category AND base_metrics.month = order_details_month.month
            """,
        )

        assert result["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "week",
                "type": "int",
                "semantic_entity": "v3.date.week",
                "semantic_type": "dimension",
            },
            {
                "name": "month",
                "type": "int",
                "semantic_entity": "v3.date.month",
                "semantic_type": "dimension",
            },
            {
                "name": "wow_revenue_change_no_role",
                "type": "double",
                "semantic_entity": "v3.wow_revenue_change_no_role",
                "semantic_type": "metric",
            },
            {
                "name": "mom_revenue_change",
                "type": "double",
                "semantic_entity": "v3.mom_revenue_change",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_filter_in_metrics_sql(self, client_with_build_v3):
        """Test that filters are applied in the metrics SQL endpoint."""
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

        # Verify exact SQL structure with WHERE clause in grain group CTE and final SELECT
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
                WHERE t2.category = 'Electronics'
                GROUP BY t2.category
            )
            SELECT COALESCE(order_details_0.category) AS category,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            WHERE order_details_0.category = 'Electronics'
            GROUP BY order_details_0.category
            """,
        )

        assert result["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "total_revenue",
                "type": "double",
                "semantic_entity": "v3.total_revenue",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_filter_cross_fact_metrics(self, client_with_build_v3):
        """Test that filters are applied to cross-fact metrics SQL."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.page_view_count"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.product.category = 'Electronics'"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Verify WHERE clause is applied to both grain group CTEs and final SELECT
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
            v3_page_views_enriched AS (
                SELECT view_id, product_id
                FROM default.v3.page_views
            ),
            order_details_0 AS (
                SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                WHERE t2.category = 'Electronics'
                GROUP BY t2.category
            ),
            page_views_enriched_0 AS (
                SELECT t2.category, COUNT(t1.view_id) view_id_count_f41e2db4
                FROM v3_page_views_enriched t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                WHERE t2.category = 'Electronics'
                GROUP BY t2.category
            )
            SELECT COALESCE(order_details_0.category, page_views_enriched_0.category) AS category,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue,
                   SUM(page_views_enriched_0.view_id_count_f41e2db4) AS page_view_count
            FROM order_details_0
            FULL OUTER JOIN page_views_enriched_0 ON order_details_0.category = page_views_enriched_0.category
            WHERE order_details_0.category = 'Electronics'
            GROUP BY order_details_0.category
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_on_different_dimension(self, client_with_build_v3):
        """Test filter on a dimension different from the GROUP BY dimension."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status = 'completed'"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Verify WHERE clause on status dimension in both CTE and final SELECT
        assert_sql_equal(
            result["sql"],
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
                WHERE t1.status = 'completed'
                GROUP BY t1.status
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            WHERE order_details_0.status = 'completed'
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_with_in_operator(self, client_with_build_v3):
        """Test filter with IN operator."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status IN ('active', 'completed')"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Verify WHERE clause with IN operator in both CTE and final SELECT
        assert_sql_equal(
            result["sql"],
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
                WHERE t1.status IN ('active', 'completed')
                GROUP BY t1.status
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            WHERE order_details_0.status IN ('active', 'completed')
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_with_not_equals(self, client_with_build_v3):
        """Test filter with != operator."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status != 'cancelled'"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Verify WHERE clause with != operator in both CTE and final SELECT
        assert_sql_equal(
            result["sql"],
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
                WHERE t1.status != 'cancelled'
                GROUP BY t1.status
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            WHERE order_details_0.status != 'cancelled'
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_with_like_operator(self, client_with_build_v3):
        """Test filter with LIKE operator."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status LIKE 'act%'"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Verify WHERE clause with LIKE operator in both CTE and final SELECT
        assert_sql_equal(
            result["sql"],
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
                WHERE t1.status LIKE 'act%'
                GROUP BY t1.status
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            WHERE order_details_0.status LIKE 'act%'
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_with_multiple_conditions(self, client_with_build_v3):
        """Test filter with multiple AND conditions."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status", "v3.product.category"],
                "filters": [
                    "v3.order_details.status = 'active'",
                    "v3.product.category = 'Electronics'",
                ],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Verify multiple filter conditions are combined with AND in both CTE and final SELECT
        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.status, oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            order_details_0 AS (
                SELECT t1.status, t2.category, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                WHERE t1.status = 'active' AND t2.category = 'Electronics'
                GROUP BY t1.status, t2.category
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   COALESCE(order_details_0.category) AS category,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            WHERE order_details_0.status = 'active' AND order_details_0.category = 'Electronics'
            GROUP BY order_details_0.status, order_details_0.category
            """,
        )


class TestMetricsSQLCrossFact:
    @pytest.mark.asyncio
    async def test_cross_fact_metrics(self, client_with_build_v3):
        """
        Test metrics SQL for metrics from different facts.

        This should JOIN the grain groups together.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.page_view_count"],
                "dimensions": ["v3.product.category"],
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
            v3_page_views_enriched AS (
                SELECT view_id, product_id
                FROM default.v3.page_views
            ),
            order_details_0 AS (
                SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                GROUP BY t2.category
            ),
            page_views_enriched_0 AS (
                SELECT t2.category, COUNT(t1.view_id) view_id_count_f41e2db4
                FROM v3_page_views_enriched t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                GROUP BY t2.category
            )
            SELECT COALESCE(order_details_0.category, page_views_enriched_0.category) AS category,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue,
                   SUM(page_views_enriched_0.view_id_count_f41e2db4) AS page_view_count
            FROM order_details_0
            FULL OUTER JOIN page_views_enriched_0 ON order_details_0.category = page_views_enriched_0.category
            GROUP BY order_details_0.category
            """,
        )
        assert result["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",  # Full dimension reference
                "semantic_type": "dimension",
            },
            {
                "name": "total_revenue",
                "type": "double",
                "semantic_entity": "v3.total_revenue",
                "semantic_type": "metric",
            },
            {
                "name": "page_view_count",
                "type": "bigint",
                "semantic_entity": "v3.page_view_count",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_all_additional_metrics_metrics_sql(self, client_with_build_v3):
        """
        Test all additional metrics through the metrics SQL endpoint.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": [
                    "v3.max_unit_price",
                    "v3.min_unit_price",
                    "v3.completed_order_revenue",
                    "v3.total_revenue",
                    "v3.price_spread",
                    "v3.price_spread_pct",
                ],
                "dimensions": [
                    "v3.order_details.status",
                    "v3.product.category",
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
            SELECT  o.status,
                oi.product_id,
                oi.unit_price,
                oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
            SELECT  product_id,
                category
            FROM default.v3.products
            ),
            order_details_0 AS (
            SELECT  t1.status,
                t2.category,
                MAX(t1.unit_price) unit_price_max_55cff00f,
                MIN(t1.unit_price) unit_price_min_55cff00f,
                SUM(CASE WHEN t1.status = 'completed' THEN t1.line_total ELSE 0 END) status_line_total_sum_43004dae,
                SUM(t1.line_total) line_total_sum_e1f61696,
                COUNT(t1.unit_price) unit_price_count_55cff00f,
                SUM(t1.unit_price) unit_price_sum_55cff00f
            FROM v3_order_details t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY  t1.status, t2.category
            )

            SELECT  COALESCE(order_details_0.status) AS status,
                COALESCE(order_details_0.category) AS category,
                MAX(order_details_0.unit_price_max_55cff00f) AS max_unit_price,
                MIN(order_details_0.unit_price_min_55cff00f) AS min_unit_price,
                SUM(order_details_0.status_line_total_sum_43004dae) AS completed_order_revenue,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue,
                MAX(order_details_0.unit_price_max_55cff00f) - MIN(order_details_0.unit_price_min_55cff00f) AS price_spread,
                (MAX(order_details_0.unit_price_max_55cff00f) - MIN(order_details_0.unit_price_min_55cff00f)) / NULLIF(SUM(order_details_0.unit_price_sum_55cff00f) / SUM(order_details_0.unit_price_count_55cff00f), 0) * 100 AS price_spread_pct
            FROM order_details_0
            GROUP BY order_details_0.status, order_details_0.category
            """,
        )

        assert result["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "max_unit_price",
                "type": "float",
                "semantic_entity": "v3.max_unit_price",
                "semantic_type": "metric",
            },
            {
                "name": "min_unit_price",
                "type": "float",
                "semantic_entity": "v3.min_unit_price",
                "semantic_type": "metric",
            },
            {
                "name": "completed_order_revenue",
                "type": "double",
                "semantic_entity": "v3.completed_order_revenue",
                "semantic_type": "metric",
            },
            {
                "name": "total_revenue",
                "type": "double",
                "semantic_entity": "v3.total_revenue",
                "semantic_type": "metric",
            },
            {
                "name": "price_spread",
                "semantic_entity": "v3.price_spread",
                "semantic_type": "metric",
                "type": "float",
            },
            {
                "name": "price_spread_pct",
                "semantic_entity": "v3.price_spread_pct",
                "semantic_type": "metric",
                "type": "double",
            },
        ]

    @pytest.mark.asyncio
    async def test_period_over_period_metrics(self, client_with_build_v3):
        """
        Test period-over-period metrics (WoW, MoM) through metrics SQL.

        These use LAG() window functions and require grain-level CTEs
        to properly aggregate to weekly/monthly grains before applying
        window functions. This ensures COUNT DISTINCT metrics are correctly
        re-computed at each grain level.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": [
                    "v3.wow_revenue_change",
                    "v3.wow_order_growth",
                    "v3.mom_revenue_change",
                ],
                "dimensions": [
                    "v3.product.category",
                ],
            },
        )
        result = response.json()
        assert_sql_equal(
            result["sql"],
            """
            WITH v3_date AS (
              SELECT
                date_id,
                week,
                month
              FROM default.v3.dates
            ),
            v3_order_details AS (
              SELECT
                o.order_id,
                o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
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
                t3.month,
                t3.week,
                t1.order_id,
                SUM(t1.line_total) line_total_sum_e1f61696
              FROM v3_order_details t1
              LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
              GROUP BY t2.category, t3.month, t3.week, t1.order_id
            ),
            base_metrics AS (
              SELECT
                COALESCE(order_details_0.category) AS category,
                COALESCE(order_details_0.month) AS month,
                COALESCE(order_details_0.week) AS week,
                COUNT(DISTINCT order_details_0.order_id) AS order_count,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.month, order_details_0.week
            ),
            order_details_week_agg AS (
              SELECT
                order_details_0.category AS category,
                order_details_0.week AS week,
                COUNT(DISTINCT order_details_0.order_id) AS order_count,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.week
            ),
            order_details_week AS (
              SELECT
                order_details_week_agg.category AS category,
                order_details_week_agg.week AS week,
                (order_details_week_agg.total_revenue - LAG(order_details_week_agg.total_revenue, 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week))
                  / NULLIF(LAG(order_details_week_agg.total_revenue, 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week), 0) * 100
                  AS wow_revenue_change,
                (CAST(order_details_week_agg.order_count AS DOUBLE) - LAG(CAST(order_details_week_agg.order_count AS DOUBLE), 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week))
                  / NULLIF(LAG(CAST(order_details_week_agg.order_count AS DOUBLE), 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week), 0) * 100
                  AS wow_order_growth
              FROM order_details_week_agg
            ),
            order_details_month_agg AS (
              SELECT
                order_details_0.category AS category,
                order_details_0.month AS month,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.month
            ),
            order_details_month AS (
              SELECT
                order_details_month_agg.category AS category,
                order_details_month_agg.month AS month,
                (order_details_month_agg.total_revenue - LAG(order_details_month_agg.total_revenue, 1) OVER (PARTITION BY order_details_month_agg.category ORDER BY order_details_month_agg.month))
                  / NULLIF(LAG(order_details_month_agg.total_revenue, 1) OVER (PARTITION BY order_details_month_agg.category ORDER BY order_details_month_agg.month), 0) * 100
                  AS mom_revenue_change
              FROM order_details_month_agg
            )
            SELECT
              base_metrics.category AS category,
              base_metrics.month AS month,
              base_metrics.week AS week,
              order_details_week.wow_revenue_change AS wow_revenue_change,
              order_details_week.wow_order_growth AS wow_order_growth,
              order_details_month.mom_revenue_change AS mom_revenue_change
            FROM base_metrics
            LEFT OUTER JOIN order_details_week ON base_metrics.category = order_details_week.category AND base_metrics.week = order_details_week.week
            LEFT OUTER JOIN order_details_month ON base_metrics.category = order_details_month.category AND base_metrics.month = order_details_month.month
            """,
        )
        assert result["columns"] == [
            {
                "name": "category",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
                "type": "string",
            },
            {
                "name": "month",
                "semantic_entity": "v3.date.month",
                "semantic_type": "dimension",
                "type": "int",
            },
            {
                "name": "week",
                "semantic_entity": "v3.date.week",
                "semantic_type": "dimension",
                "type": "int",
            },
            {
                "name": "wow_revenue_change",
                "semantic_entity": "v3.wow_revenue_change",
                "semantic_type": "metric",
                "type": "double",
            },
            {
                "name": "wow_order_growth",
                "semantic_entity": "v3.wow_order_growth",
                "semantic_type": "metric",
                "type": "double",
            },
            {
                "name": "mom_revenue_change",
                "semantic_entity": "v3.mom_revenue_change",
                "semantic_type": "metric",
                "type": "double",
            },
        ]

    @pytest.mark.asyncio
    async def test_cross_fact_metrics_without_shared_dimensions_raises_error(
        self,
        client_with_build_v3,
    ):
        """
        Test that cross-fact metrics without shared dimensions raises an error.

        When requesting metrics from different parent nodes (e.g., order_details
        and page_views) without any dimensions, the system cannot join the results
        because there are no shared columns. This would produce a CROSS JOIN which
        is semantically meaningless.

        This test covers metrics.py lines 428-434.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                # These metrics come from different parent nodes:
                # - total_revenue: from v3.order_details
                # - page_view_count: from v3.page_views_enriched
                "metrics": ["v3.total_revenue", "v3.page_view_count"],
                # No dimensions means no columns to join on
                "dimensions": [],
            },
        )

        # Should return an error because cross-fact requires shared dimensions
        assert response.status_code == 422 or response.status_code == 400
        error_detail = response.json()
        assert (
            "Cross-fact metrics" in str(error_detail)
            or "shared dimension" in str(error_detail).lower()
        )


class TestNonDecomposableMetrics:
    """Tests for metrics that cannot be decomposed (Aggregability.NONE)."""

    @pytest.mark.asyncio
    async def test_non_decomposable_metric_max_by(self, client_with_build_v3):
        """
        Test that non-decomposable metrics like MAX_BY are handled in metrics SQL.

        MAX_BY cannot be pre-aggregated because it needs access to the full
        dataset to determine which row has the maximum value. Since it has
        Aggregability.NONE, the measures CTE outputs raw rows at native grain
        (PK columns), and the final metrics SQL applies MAX_BY over those rows.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.top_product_by_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # The final SQL should apply MAX_BY to get the product with highest revenue
        # Since NONE aggregability, measures CTE has raw rows, then final SELECT
        # applies the actual aggregation
        assert_sql_equal(
            result["sql"],
            """
            WITH v3_order_details AS (
              SELECT
                o.order_id,
                oi.line_number,
                o.status,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            order_details_0 AS (
              SELECT
                t1.status,
                t1.order_id,
                t1.line_number,
                t1.product_id,
                t1.line_total
              FROM v3_order_details t1
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   MAX_BY(order_details_0.product_id, order_details_0.line_total) AS top_product_by_revenue
            FROM order_details_0
            GROUP BY  order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_trailing_wow_metrics(self, client_with_build_v3):
        """
        Test trailing/rolling week-over-week metrics.

        These metrics use frame clauses (ROWS BETWEEN) to compare:
        - Last 7 days (ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
        - Previous 7 days (ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING)

        Window function metrics use a base_metrics CTE to pre-compute
        base metrics before applying the window function.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.trailing_wow_revenue_change"],
                "dimensions": ["v3.product.category"],
            },
        )
        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH v3_order_details AS (
              SELECT
                o.order_date,
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
                t1.order_date date_id,
                SUM(t1.line_total) line_total_sum_e1f61696
              FROM v3_order_details t1
              LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              GROUP BY  t2.category, t1.order_date
            ),
            base_metrics AS (
              SELECT
                COALESCE(order_details_0.category) AS category,
                COALESCE(order_details_0.date_id) AS date_id,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY  order_details_0.category, order_details_0.date_id
            )
            SELECT
              base_metrics.category AS category,
              base_metrics.date_id AS date_id,
              (SUM(base_metrics.total_revenue) OVER (PARTITION BY base_metrics.category ORDER BY base_metrics.date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  - SUM(base_metrics.total_revenue) OVER (PARTITION BY base_metrics.category ORDER BY base_metrics.date_id ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) ) / NULLIF(SUM(base_metrics.total_revenue) OVER (PARTITION BY base_metrics.category ORDER BY base_metrics.date_id ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) , 0) * 100 AS trailing_wow_revenue_change
            FROM base_metrics
            """,
        )

        assert result["columns"] == [
            {
                "name": "category",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
                "type": "string",
            },
            {
                "name": "date_id",
                "semantic_entity": "v3.date.date_id",
                "semantic_type": "dimension",
                "type": "int",
            },
            {
                "name": "trailing_wow_revenue_change",
                "semantic_entity": "v3.trailing_wow_revenue_change",
                "semantic_type": "metric",
                "type": "double",
            },
        ]

    @pytest.mark.asyncio
    async def test_trailing_7d_revenue(self, client_with_build_v3):
        """
        Test trailing 7-day rolling sum metric.

        This metric computes a rolling 7-day sum using a frame clause.
        Window function metrics use a base_metrics CTE to pre-compute
        base metrics before applying the window function.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.trailing_7d_revenue"],
                "dimensions": ["v3.product.category"],
            },
        )
        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH v3_order_details AS (
              SELECT
                o.order_date,
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
                t1.order_date date_id,
                SUM(t1.line_total) line_total_sum_e1f61696
              FROM v3_order_details t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              GROUP BY  t2.category, t1.order_date
            ),
            base_metrics AS (
              SELECT
                COALESCE(order_details_0.category) AS category,
                COALESCE(order_details_0.date_id) AS date_id,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY  order_details_0.category, order_details_0.date_id
            )
            SELECT
              base_metrics.category AS category,
              base_metrics.date_id AS date_id,
              SUM(base_metrics.total_revenue) OVER (PARTITION BY base_metrics.category ORDER BY base_metrics.date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS trailing_7d_revenue
            FROM base_metrics
            """,
        )

        assert result["columns"] == [
            {
                "name": "category",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
                "type": "string",
            },
            {
                "name": "date_id",
                "semantic_entity": "v3.date.date_id",
                "semantic_type": "dimension",
                "type": "int",
            },
            {
                "name": "trailing_7d_revenue",
                "semantic_entity": "v3.trailing_7d_revenue",
                "semantic_type": "metric",
                "type": "double",
            },
        ]


class TestMetricsSQLNestedDerived:
    """
    Test nested derived metrics - metrics that reference other derived metrics.

    These test the inline expansion of intermediate derived metrics during
    SQL generation.
    """

    @pytest.mark.asyncio
    async def test_nested_derived_metric_simple(self, client_with_build_v3):
        """
        Test a simple nested derived metric.

        v3.aov_growth_index = v3.avg_order_value / 50.0 * 100
        where v3.avg_order_value = v3.total_revenue / v3.order_count

        The intermediate derived metric (avg_order_value) should be expanded inline
        to its component expressions.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.aov_growth_index"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # The nested derived metric should expand avg_order_value inline
        # avg_order_value = total_revenue / order_count
        # aov_growth_index = avg_order_value / 50.0 * 100
        # = (total_revenue / order_count) / 50.0 * 100
        assert_sql_equal(
            result["sql"],
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
                   SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT(DISTINCT order_details_0.order_id), 0) / 50.0 * 100 AS aov_growth_index
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )

        # Verify output columns
        assert len(result["columns"]) == 2
        assert result["columns"][0]["semantic_entity"] == "v3.order_details.status"
        assert result["columns"][1]["semantic_entity"] == "v3.aov_growth_index"

    @pytest.mark.asyncio
    async def test_nested_derived_metric_with_window_function(
        self,
        client_with_build_v3,
    ):
        """
        Test a nested derived metric with window function.

        v3.wow_aov_change uses LAG() on v3.avg_order_value, which is itself
        a derived metric (v3.total_revenue / v3.order_count).

        This requires:
        1. Computing base metrics (total_revenue, order_count) in grain groups
        2. Computing the intermediate derived metric (avg_order_value) in base_metrics CTE
        3. Applying the window function to reference avg_order_value from base_metrics CTE
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.wow_aov_change"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Window function metric requires base_metrics CTE with:
        # - Base metrics (total_revenue, order_count)
        # - Intermediate derived metric (avg_order_value) pre-computed
        # Final SELECT applies LAG on base_metrics.avg_order_value
        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_date AS (
                SELECT date_id, week
                FROM default.v3.dates
            ),
            v3_order_details AS (
                SELECT o.order_id, o.order_date, oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            order_details_0 AS (
                SELECT t2.category, t3.week, t1.order_id, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
                GROUP BY t2.category, t3.week, t1.order_id
            ),
            base_metrics AS (
                SELECT
                    COALESCE(order_details_0.category) AS category,
                    COALESCE(order_details_0.week) AS week,
                    COUNT(DISTINCT order_details_0.order_id) AS order_count,
                    SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue,
                    SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT( DISTINCT order_details_0.order_id), 0) AS avg_order_value
                FROM order_details_0
                GROUP BY order_details_0.category, order_details_0.week
            )
            SELECT
                base_metrics.category AS category,
                base_metrics.week AS week,
                (base_metrics.avg_order_value - LAG(base_metrics.avg_order_value, 1) OVER (PARTITION BY base_metrics.category ORDER BY base_metrics.week))
                    / NULLIF(LAG(base_metrics.avg_order_value, 1) OVER (PARTITION BY base_metrics.category ORDER BY base_metrics.week), 0) * 100
                    AS wow_aov_change
            FROM base_metrics
            """,
        )

        assert result["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "week",
                "type": "int",
                "semantic_entity": "v3.date.week",
                "semantic_type": "dimension",
            },
            {
                "name": "wow_aov_change",
                "type": "double",
                "semantic_entity": "v3.wow_aov_change",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_nested_derived_metric_cross_fact(self, client_with_build_v3):
        """
        Test a nested derived metric that references derived metrics from different facts.

        v3.efficiency_ratio = v3.avg_order_value / v3.pages_per_session
        where:
        - v3.avg_order_value = v3.total_revenue / v3.order_count (from order_details)
        - v3.pages_per_session = v3.page_view_count / v3.session_count (from page_views)

        Both intermediate derived metrics should be expanded inline.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.efficiency_ratio"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Cross-fact nested derived metric:
        # - Grain group from order_details for total_revenue/order_count components
        # - Grain group from page_views for page_view_count/session_count components
        # - Final SELECT computes both intermediate metrics and divides
        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.order_id, oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            v3_page_views_enriched AS (
                SELECT view_id, session_id, product_id
                FROM default.v3.page_views
            ),
            order_details_0 AS (
                SELECT t2.category, t1.order_id, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                GROUP BY t2.category, t1.order_id
            ),
            page_views_enriched_0 AS (
                SELECT t2.category, t1.session_id, COUNT(t1.view_id) view_id_count_f41e2db4
                FROM v3_page_views_enriched t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                GROUP BY t2.category, t1.session_id
            )
            SELECT COALESCE(order_details_0.category, page_views_enriched_0.category) AS category,
                   SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT(DISTINCT order_details_0.order_id), 0)
                   / NULLIF(SUM(page_views_enriched_0.view_id_count_f41e2db4) / NULLIF(COUNT(DISTINCT page_views_enriched_0.session_id), 0), 0) AS efficiency_ratio
            FROM order_details_0
            FULL OUTER JOIN page_views_enriched_0 ON order_details_0.category = page_views_enriched_0.category
            GROUP BY order_details_0.category
            """,
        )

        assert result["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "efficiency_ratio",
                "type": "double",
                "semantic_entity": "v3.efficiency_ratio",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_dod_at_daily_grain(self, client_with_build_v3):
        """
        Test day-over-day metric at daily grain.

        DoD uses ORDER BY date_id, so when requesting daily grain (date_id),
        no grain-level CTEs are needed - the LAG operates directly at the
        requested grain.
        """
        # Create the metric locally for this test
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.dod_revenue_change",
                "description": "Day-over-day revenue change (%)",
                "query": """
                    SELECT
                        (v3.total_revenue - LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.date_id[order]))
                        / NULLIF(LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.date_id[order]), 0) * 100
                """,
                "mode": "published",
            },
        )
        assert response.status_code in (200, 201), response.json()

        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.dod_revenue_change"],
                "dimensions": ["v3.product.category"],
            },
        )
        assert response.status_code == 200, response.json()
        result = response.json()

        # DoD at daily grain: LAG operates on base_metrics directly
        # PARTITION BY category ensures comparison within each category
        assert_sql_equal(
            result["sql"],
            """
            WITH v3_order_details AS (
              SELECT
                o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            ),
            order_details_0 AS (
              SELECT
                t2.category,
                t1.order_date date_id,
                SUM(t1.line_total) line_total_sum_e1f61696
              FROM v3_order_details t1
              LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              GROUP BY t2.category, t1.order_date
            ),
            base_metrics AS (
              SELECT
                COALESCE(order_details_0.category) AS category,
                COALESCE(order_details_0.date_id) AS date_id,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.date_id
            )
            SELECT
              base_metrics.category AS category,
              base_metrics.date_id AS date_id,
              (base_metrics.total_revenue - LAG(base_metrics.total_revenue, 1) OVER (PARTITION BY base_metrics.category ORDER BY base_metrics.date_id))
                / NULLIF(LAG(base_metrics.total_revenue, 1) OVER (PARTITION BY base_metrics.category ORDER BY base_metrics.date_id), 0) * 100
                AS dod_revenue_change
            FROM base_metrics
            """,
        )

    @pytest.mark.asyncio
    async def test_wow_at_daily_grain(self, client_with_build_v3):
        """
        Test week-over-week metric when requesting daily grain.

        WoW uses ORDER BY week, but user requests date_id (daily grain).
        This requires grain-level CTEs:
        1. base_metrics: at daily grain (date_id, week, category)
        2. week_metrics_agg: aggregates to weekly grain (week, category)
        3. week_metrics: applies LAG at weekly grain
        4. Final SELECT: joins base_metrics with week_metrics
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.wow_revenue_change"],
                "dimensions": [
                    "v3.date.date_id[order]",
                    "v3.product.category",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        result = response.json()

        # WoW at daily grain requires grain-level CTEs:
        # - week_metrics_agg: aggregates to weekly grain
        # - week_metrics: applies LAG at weekly grain
        # - Final SELECT joins base_metrics with week_metrics
        # Note: date_id_order is used (not date_id) due to the [order] role suffix
        assert_sql_equal(
            result["sql"],
            """
            WITH v3_date AS (
              SELECT date_id, week
              FROM default.v3.dates
            ),
            v3_order_details AS (
              SELECT
                o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            ),
            order_details_0 AS (
              SELECT
                t1.order_date date_id_order,
                t2.category,
                t3.week,
                SUM(t1.line_total) line_total_sum_e1f61696
              FROM v3_order_details t1
              LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
              GROUP BY t1.order_date, t2.category, t3.week
            ),
            base_metrics AS (
              SELECT
                COALESCE(order_details_0.date_id_order) AS date_id_order,
                COALESCE(order_details_0.category) AS category,
                COALESCE(order_details_0.week) AS week,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.date_id_order, order_details_0.category, order_details_0.week
            ),
            order_details_week_agg AS (
              SELECT
                order_details_0.category AS category,
                order_details_0.week AS week,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.week
            ),
            order_details_week AS (
              SELECT
                order_details_week_agg.category AS category,
                order_details_week_agg.week AS week,
                (order_details_week_agg.total_revenue - LAG(order_details_week_agg.total_revenue, 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week))
                  / NULLIF(LAG(order_details_week_agg.total_revenue, 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week), 0) * 100
                  AS wow_revenue_change
              FROM order_details_week_agg
            )
            SELECT
              base_metrics.date_id_order AS date_id_order,
              base_metrics.category AS category,
              base_metrics.week AS week,
              order_details_week.wow_revenue_change AS wow_revenue_change
            FROM base_metrics
            LEFT OUTER JOIN order_details_week ON base_metrics.category = order_details_week.category AND base_metrics.week = order_details_week.week
            """,
        )

    @pytest.mark.asyncio
    async def test_wow_and_mom_at_daily_grain(self, client_with_build_v3):
        """
        Test WoW and MoM metrics together when requesting daily grain.

        Both WoW (ORDER BY week) and MoM (ORDER BY month) require separate
        grain-level CTEs since they operate at different grains.
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": [
                    "v3.wow_revenue_change",
                    "v3.mom_revenue_change",
                ],
                "dimensions": [
                    "v3.date.date_id[order]",
                    "v3.product.category",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        result = response.json()

        # Multiple grain-level CTEs for different period comparisons
        # Note: date_id_order is used (not date_id) due to the [order] role suffix
        assert_sql_equal(
            result["sql"],
            """
            WITH v3_date AS (
              SELECT date_id, week, month
              FROM default.v3.dates
            ),
            v3_order_details AS (
              SELECT
                o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            ),
            order_details_0 AS (
              SELECT
                t1.order_date date_id_order,
                t2.category,
                t3.month,
                t3.week,
                SUM(t1.line_total) line_total_sum_e1f61696
              FROM v3_order_details t1
              LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
              GROUP BY t1.order_date, t2.category, t3.month, t3.week
            ),
            base_metrics AS (
              SELECT
                COALESCE(order_details_0.date_id_order) AS date_id_order,
                COALESCE(order_details_0.category) AS category,
                COALESCE(order_details_0.month) AS month,
                COALESCE(order_details_0.week) AS week,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.date_id_order, order_details_0.category, order_details_0.month, order_details_0.week
            ),
            order_details_week_agg AS (
              SELECT
                order_details_0.category AS category,
                order_details_0.week AS week,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.week
            ),
            order_details_week AS (
              SELECT
                order_details_week_agg.category AS category,
                order_details_week_agg.week AS week,
                (order_details_week_agg.total_revenue - LAG(order_details_week_agg.total_revenue, 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week))
                  / NULLIF(LAG(order_details_week_agg.total_revenue, 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week), 0) * 100
                  AS wow_revenue_change
              FROM order_details_week_agg
            ),
            order_details_month_agg AS (
              SELECT
                order_details_0.category AS category,
                order_details_0.month AS month,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.month
            ),
            order_details_month AS (
              SELECT
                order_details_month_agg.category AS category,
                order_details_month_agg.month AS month,
                (order_details_month_agg.total_revenue - LAG(order_details_month_agg.total_revenue, 1) OVER (PARTITION BY order_details_month_agg.category ORDER BY order_details_month_agg.month))
                  / NULLIF(LAG(order_details_month_agg.total_revenue, 1) OVER (PARTITION BY order_details_month_agg.category ORDER BY order_details_month_agg.month), 0) * 100
                  AS mom_revenue_change
              FROM order_details_month_agg
            )
            SELECT
              base_metrics.date_id_order AS date_id_order,
              base_metrics.category AS category,
              base_metrics.month AS month,
              base_metrics.week AS week,
              order_details_week.wow_revenue_change AS wow_revenue_change,
              order_details_month.mom_revenue_change AS mom_revenue_change
            FROM base_metrics
            LEFT OUTER JOIN order_details_week ON base_metrics.category = order_details_week.category AND base_metrics.week = order_details_week.week
            LEFT OUTER JOIN order_details_month ON base_metrics.category = order_details_month.category AND base_metrics.month = order_details_month.month
            """,
        )

    @pytest.mark.asyncio
    async def test_wow_with_count_distinct_at_daily_grain(self, client_with_build_v3):
        """
        Test WoW metrics with COUNT DISTINCT at daily grain.

        This tests the critical case where:
        1. order_count uses COUNT(DISTINCT order_id) - non-additive
        2. total_revenue uses SUM(line_total) - additive
        3. User requests daily grain (date_id[order])

        The grain-level CTE (week_metrics_agg) must:
        - NOT include order_id in GROUP BY (would make COUNT DISTINCT = 1)
        - GROUP BY only user-requested dimensions + the ORDER BY dimension
        - Re-compute COUNT(DISTINCT order_id) at weekly grain from order_details_0
        """
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": [
                    "v3.wow_order_growth",  # Uses COUNT(DISTINCT order_id)
                    "v3.wow_revenue_change",  # Uses SUM(line_total)
                ],
                "dimensions": [
                    "v3.date.date_id[order]",
                    "v3.product.category",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        result = response.json()

        # Key verification: week_metrics_agg should NOT have order_id in GROUP BY
        # It should re-compute COUNT(DISTINCT order_id) at weekly grain
        assert_sql_equal(
            result["sql"],
            """
            WITH v3_date AS (
              SELECT date_id, week
              FROM default.v3.dates
            ),
            v3_order_details AS (
              SELECT
                o.order_id,
                o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            ),
            order_details_0 AS (
              SELECT
                t1.order_date date_id_order,
                t2.category,
                t3.week,
                t1.order_id,
                SUM(t1.line_total) line_total_sum_e1f61696
              FROM v3_order_details t1
              LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
              LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
              GROUP BY t1.order_date, t2.category, t3.week, t1.order_id
            ),
            base_metrics AS (
              SELECT
                COALESCE(order_details_0.date_id_order) AS date_id_order,
                COALESCE(order_details_0.category) AS category,
                COALESCE(order_details_0.week) AS week,
                COUNT(DISTINCT order_details_0.order_id) AS order_count,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.date_id_order, order_details_0.category, order_details_0.week
            ),
            order_details_week_agg AS (
              SELECT
                order_details_0.category AS category,
                order_details_0.week AS week,
                COUNT(DISTINCT order_details_0.order_id) AS order_count,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.week
            ),
            order_details_week AS (
              SELECT
                order_details_week_agg.category AS category,
                order_details_week_agg.week AS week,
                (CAST(order_details_week_agg.order_count AS DOUBLE) - LAG(CAST(order_details_week_agg.order_count AS DOUBLE), 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week))
                  / NULLIF(LAG(CAST(order_details_week_agg.order_count AS DOUBLE), 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week), 0) * 100
                  AS wow_order_growth,
                (order_details_week_agg.total_revenue - LAG(order_details_week_agg.total_revenue, 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week))
                  / NULLIF(LAG(order_details_week_agg.total_revenue, 1) OVER (PARTITION BY order_details_week_agg.category ORDER BY order_details_week_agg.week), 0) * 100
                  AS wow_revenue_change
              FROM order_details_week_agg
            )
            SELECT
              base_metrics.date_id_order AS date_id_order,
              base_metrics.category AS category,
              base_metrics.week AS week,
              order_details_week.wow_order_growth AS wow_order_growth,
              order_details_week.wow_revenue_change AS wow_revenue_change
            FROM base_metrics
            LEFT OUTER JOIN order_details_week ON base_metrics.category = order_details_week.category AND base_metrics.week = order_details_week.week
            """,
        )


class TestMetricsSQLCrossFactWindow:
    """Tests for window metrics that span multiple facts."""

    @pytest.mark.asyncio
    async def test_cross_fact_wow_conversion_rate(self, client_with_build_v3):
        """
        Test cross-fact window metric: week-over-week conversion rate change.

        v3.wow_conversion_rate_change uses LAG() on v3.conversion_rate, which
        is itself a cross-fact derived metric:
            conversion_rate = order_count (order_details) / visitor_count (page_views)

        This tests that:
        1. Metrics from both facts are computed in their own grain group CTEs
        2. The base_metrics CTE has FULL OUTER JOIN to combine them
        3. conversion_rate is computed as an intermediate derived metric
        4. The window aggregation CTE uses base_metrics as source (not individual CTEs)
        5. The final SELECT applies LAG on the cross-fact derived metric
        """
        # Create the metric locally for this test
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.wow_conversion_rate_change",
                "description": (
                    "Week-over-week conversion rate change (%). "
                    "Cross-fact window metric: conversion_rate = order_count (order_details) / visitor_count (page_views). "
                    "Tests window functions on metrics spanning multiple facts."
                ),
                "query": """
                    SELECT
                        (v3.conversion_rate - LAG(v3.conversion_rate, 1) OVER (ORDER BY v3.date.week[order]))
                        / NULLIF(LAG(v3.conversion_rate, 1) OVER (ORDER BY v3.date.week[order]), 0) * 100
                """,
                "mode": "published",
                "required_dimensions": ["v3.date.week[order]"],
            },
        )
        assert response.status_code in (200, 201), response.json()

        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.wow_conversion_rate_change"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # For cross-fact window metrics, base_metrics already contains the week
        # dimension in its GROUP BY, so the LAG window function can be applied
        # directly without additional aggregation CTEs.
        #
        # Expected structure:
        # 1. order_details_0: grain group CTE with order_count components (includes week)
        # 2. page_views_enriched_0: grain group CTE with visitor_count components (includes week)
        # 3. base_metrics: FULL OUTER JOIN + computes conversion_rate (at week grain)
        # 4. Final SELECT: applies LAG directly on base_metrics.conversion_rate
        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_date AS (
                SELECT date_id, week
                FROM default.v3.dates
            ),
            v3_order_details AS (
                SELECT o.order_id, o.order_date, oi.product_id
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            v3_page_views_enriched AS (
                SELECT customer_id, page_date, product_id
                FROM default.v3.page_views
            ),
            order_details_0 AS (
                SELECT t2.category, t3.week, t1.order_id
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
                GROUP BY t2.category, t3.week, t1.order_id
            ),
            page_views_enriched_0 AS (
                SELECT t2.category, t3.week, t1.customer_id
                FROM v3_page_views_enriched t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                LEFT OUTER JOIN v3_date t3 ON t1.page_date = t3.date_id
                GROUP BY t2.category, t3.week, t1.customer_id
            ),
            base_metrics AS (
                SELECT
                    COALESCE(order_details_0.category, page_views_enriched_0.category) AS category,
                    COALESCE(order_details_0.week, page_views_enriched_0.week) AS week,
                    COUNT(DISTINCT order_details_0.order_id) AS order_count,
                    COUNT(DISTINCT page_views_enriched_0.customer_id) AS visitor_count,
                    CAST(COUNT(DISTINCT order_details_0.order_id) AS DOUBLE) / NULLIF(COUNT(DISTINCT page_views_enriched_0.customer_id), 0) AS conversion_rate
                FROM order_details_0
                FULL OUTER JOIN page_views_enriched_0 ON order_details_0.category = page_views_enriched_0.category AND order_details_0.week = page_views_enriched_0.week
                GROUP BY 1, 2
            )
            SELECT
                base_metrics.category AS category,
                base_metrics.week AS week,
                (base_metrics.conversion_rate - LAG(base_metrics.conversion_rate, 1) OVER (PARTITION BY base_metrics.category ORDER BY base_metrics.week))
                    / NULLIF(LAG(base_metrics.conversion_rate, 1) OVER (PARTITION BY base_metrics.category ORDER BY base_metrics.week), 0) * 100
                    AS wow_conversion_rate_change
            FROM base_metrics
            """,
        )

        assert result["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "week",
                "type": "int",
                "semantic_entity": "v3.date.week",
                "semantic_type": "dimension",
            },
            {
                "name": "wow_conversion_rate_change",
                "type": "double",
                "semantic_entity": "v3.wow_conversion_rate_change",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_cross_fact_window_metric_with_finer_grain(
        self,
        client_with_build_v3,
    ):
        """
        Test cross-fact window metric when user requests finer grain than ORDER BY.

        This tests the build_window_agg_cte_from_base_metrics code path:
        - User requests daily grain (v3.date.date_id)
        - Window metric orders by weekly grain (v3.date.week)
        - Metric is cross-fact (conversion_rate = order_count / visitor_count)

        Expected behavior:
        1. Grain groups are built at daily grain (include date_id AND week)
        2. base_metrics CTE combines facts with FULL OUTER JOIN at daily grain
        3. A window aggregation CTE reaggregates base_metrics to weekly grain
        4. Window CTE applies LAG on the weekly-aggregated data
        5. Final SELECT joins daily base_metrics with weekly window results
        """
        # Create the metric locally for this test
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.wow_conversion_rate_change",
                "description": (
                    "Week-over-week conversion rate change (%). "
                    "Cross-fact window metric: conversion_rate = order_count (order_details) / visitor_count (page_views). "
                    "Tests window functions on metrics spanning multiple facts."
                ),
                "query": """
                    SELECT
                        (v3.conversion_rate - LAG(v3.conversion_rate, 1) OVER (ORDER BY v3.date.week[order]))
                        / NULLIF(LAG(v3.conversion_rate, 1) OVER (ORDER BY v3.date.week[order]), 0) * 100
                """,
                "mode": "published",
                "required_dimensions": ["v3.date.week[order]"],
            },
        )
        assert response.status_code in (200, 201), response.json()

        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.wow_conversion_rate_change"],
                "dimensions": ["v3.date.date_id", "v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Verify the SQL has the expected structure with reaggregation CTE
        # The key is that there should be a CTE that aggregates from base_metrics
        # to weekly grain before applying the LAG window function
        sql = result["sql"]
        assert_sql_equal(
            sql,
            """
            WITH
            v3_date AS (
            SELECT  date_id,
                week
            FROM default.v3.dates
            ),
            v3_order_details AS (
            SELECT  o.order_id,
                o.order_date,
                oi.product_id
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
            SELECT  product_id,
                category
            FROM default.v3.products
            ),
            v3_page_views_enriched AS (
            SELECT  customer_id,
                page_date,
                product_id
            FROM default.v3.page_views
            ),
            order_details_0 AS (
            SELECT  t1.order_date date_id,
                t2.category,
                t3.week,
                t1.order_id
            FROM v3_order_details t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
            GROUP BY  t1.order_date, t2.category, t3.week, t1.order_id
            ),
            page_views_enriched_0 AS (
            SELECT  t1.page_date date_id,
                t2.category,
                t3.week,
                t1.customer_id
            FROM v3_page_views_enriched t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            LEFT OUTER JOIN v3_date t3 ON t1.page_date = t3.date_id
            GROUP BY  t1.page_date, t2.category, t3.week, t1.customer_id
            ),
            base_metrics AS (
            SELECT  COALESCE(order_details_0.date_id, page_views_enriched_0.date_id) AS date_id,
                COALESCE(order_details_0.category, page_views_enriched_0.category) AS category,
                COALESCE(order_details_0.week, page_views_enriched_0.week) AS week,
                COUNT( DISTINCT order_details_0.order_id) AS order_count,
                COUNT( DISTINCT page_views_enriched_0.customer_id) AS visitor_count,
                CAST(COUNT( DISTINCT order_details_0.order_id) AS DOUBLE) / NULLIF(COUNT( DISTINCT page_views_enriched_0.customer_id), 0) AS conversion_rate
            FROM order_details_0 FULL OUTER JOIN page_views_enriched_0 ON order_details_0.date_id = page_views_enriched_0.date_id AND order_details_0.category = page_views_enriched_0.category AND order_details_0.week = page_views_enriched_0.week
            GROUP BY  1, 2, 3
            )

            SELECT  base_metrics.date_id AS date_id,
                base_metrics.category AS category,
                base_metrics.week AS week,
                (base_metrics.conversion_rate - LAG(base_metrics.conversion_rate, 1) OVER ( PARTITION BY base_metrics.category
            ORDER BY base_metrics.week) ) / NULLIF(LAG(base_metrics.conversion_rate, 1) OVER ( PARTITION BY base_metrics.category
            ORDER BY base_metrics.week) , 0) * 100 AS wow_conversion_rate_change
            FROM base_metrics
            """,
        )

    @pytest.mark.asyncio
    async def test_cross_fact_window_on_derived_metric(self, client_with_build_v3):
        """
        Test cross-fact window metric that references a DERIVED metric.

        This tests the derived metric expansion code in build_window_agg_cte_from_base_metrics:
        - efficiency_ratio = avg_order_value / pages_per_session
        - avg_order_value = total_revenue / order_count (derived from orders)
        - pages_per_session = page_view_count / visitor_count (derived from page_views)
        - wow_efficiency_ratio_change = LAG(efficiency_ratio, 1) OVER (ORDER BY week)

        This hits lines 1029-1055 in metrics.py where derived metrics are expanded
        by replacing column references with parent metric expressions.
        """
        # Create the metric locally for this test
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.wow_efficiency_ratio_change",
                "description": "Week-over-week efficiency ratio change (%)",
                "query": """
                    SELECT
                        (v3.efficiency_ratio - LAG(v3.efficiency_ratio, 1) OVER (ORDER BY v3.date.week[order]))
                        / NULLIF(LAG(v3.efficiency_ratio, 1) OVER (ORDER BY v3.date.week[order]), 0) * 100
                """,
                "mode": "published",
            },
        )
        assert response.status_code in (200, 201), response.json()

        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.wow_efficiency_ratio_change"],
                "dimensions": ["v3.date.date_id", "v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()
        sql = result["sql"]
        assert_sql_equal(
            sql,
            """
            WITH
            v3_date AS (
            SELECT  date_id,
                week
            FROM default.v3.dates
            ),
            v3_order_details AS (
            SELECT  o.order_id,
                o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
            SELECT  product_id,
                category
            FROM default.v3.products
            ),
            v3_page_views_enriched AS (
            SELECT  view_id,
                session_id,
                page_date,
                product_id
            FROM default.v3.page_views
            ),
            order_details_0 AS (
            SELECT  t1.order_date date_id,
                t2.category,
                t3.week,
                t1.order_id,
                SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
            GROUP BY  t1.order_date, t2.category, t3.week, t1.order_id
            ),
            page_views_enriched_0 AS (
            SELECT  t1.page_date date_id,
                t2.category,
                t3.week,
                t1.session_id,
                COUNT(t1.view_id) view_id_count_f41e2db4
            FROM v3_page_views_enriched t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            LEFT OUTER JOIN v3_date t3 ON t1.page_date = t3.date_id
            GROUP BY  t1.page_date, t2.category, t3.week, t1.session_id
            ),
            base_metrics AS (
            SELECT  COALESCE(order_details_0.date_id, page_views_enriched_0.date_id) AS date_id,
                COALESCE(order_details_0.category, page_views_enriched_0.category) AS category,
                COALESCE(order_details_0.week, page_views_enriched_0.week) AS week,
                COUNT( DISTINCT order_details_0.order_id) AS order_count,
                SUM(page_views_enriched_0.view_id_count_f41e2db4) AS page_view_count,
                COUNT( DISTINCT page_views_enriched_0.session_id) AS session_count,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue,
                SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT( DISTINCT order_details_0.order_id), 0) AS avg_order_value,
                SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT( DISTINCT order_details_0.order_id), 0) / NULLIF(SUM(page_views_enriched_0.view_id_count_f41e2db4) / NULLIF(COUNT( DISTINCT page_views_enriched_0.session_id), 0), 0) AS efficiency_ratio,
                SUM(page_views_enriched_0.view_id_count_f41e2db4) / NULLIF(COUNT( DISTINCT page_views_enriched_0.session_id), 0) AS pages_per_session
            FROM order_details_0 FULL OUTER JOIN page_views_enriched_0 ON order_details_0.date_id = page_views_enriched_0.date_id AND order_details_0.category = page_views_enriched_0.category AND order_details_0.week = page_views_enriched_0.week
            GROUP BY  1, 2, 3
            )

            SELECT  base_metrics.date_id AS date_id,
                base_metrics.category AS category,
                base_metrics.week AS week,
                (base_metrics.efficiency_ratio - LAG(base_metrics.efficiency_ratio, 1) OVER ( PARTITION BY base_metrics.category
            ORDER BY base_metrics.week) ) / NULLIF(LAG(base_metrics.efficiency_ratio, 1) OVER ( PARTITION BY base_metrics.category
            ORDER BY base_metrics.week) , 0) * 100 AS wow_efficiency_ratio_change
            FROM base_metrics""",
        )

    @pytest.mark.asyncio
    async def test_cross_fact_window_on_base_metrics(self, client_with_build_v3):
        """
        Test cross-fact window metric that directly references BASE metrics.

        This tests the build_window_agg_cte_from_base_metrics code path:
        - wow_order_and_visitor_change = LAG(order_count + visitor_count, 1)
        - order_count is a base metric from order_details
        - visitor_count is a base metric from page_views_enriched
        - Both are in grain groups (not derived metrics)

        This should trigger build_window_agg_cte_from_base_metrics because:
        1. It's cross-fact (order_count + visitor_count span multiple facts)
        2. The base metrics ARE in grain groups (unlike derived metrics)
        3. Window ORDER BY grain (week) is coarser than requested grain (date_id)
        """
        # Create the metric locally for this test
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.wow_order_and_visitor_change",
                "description": "Week-over-week change in orders + visitors",
                "query": """
                    SELECT
                        (v3.order_count + v3.visitor_count)
                        - LAG(v3.order_count + v3.visitor_count, 1)
                            OVER (ORDER BY v3.date.week[order])
                """,
                "mode": "published",
            },
        )
        assert response.status_code in (200, 201), response.json()

        # Request with finer grain (date_id) than ORDER BY grain (week)
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.wow_order_and_visitor_change"],
                "dimensions": ["v3.date.date_id", "v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()
        sql = result["sql"]
        assert_sql_equal(
            sql,
            """
            WITH
            v3_date AS (
            SELECT  date_id,
                week
            FROM default.v3.dates
            ),
            v3_order_details AS (
            SELECT  o.order_id,
                o.order_date,
                oi.product_id
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
            SELECT  product_id,
                category
            FROM default.v3.products
            ),
            v3_page_views_enriched AS (
            SELECT  customer_id,
                page_date,
                product_id
            FROM default.v3.page_views
            ),
            order_details_0 AS (
            SELECT  t1.order_date date_id,
                t2.category,
                t3.week,
                t1.order_id
            FROM v3_order_details t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
            GROUP BY  t1.order_date, t2.category, t3.week, t1.order_id
            ),
            page_views_enriched_0 AS (
            SELECT  t1.page_date date_id,
                t2.category,
                t3.week,
                t1.customer_id
            FROM v3_page_views_enriched t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            LEFT OUTER JOIN v3_date t3 ON t1.page_date = t3.date_id
            GROUP BY  t1.page_date, t2.category, t3.week, t1.customer_id
            ),
            base_metrics AS (
            SELECT  COALESCE(order_details_0.date_id, page_views_enriched_0.date_id) AS date_id,
                COALESCE(order_details_0.category, page_views_enriched_0.category) AS category,
                COALESCE(order_details_0.week, page_views_enriched_0.week) AS week,
                COUNT( DISTINCT order_details_0.order_id) AS order_count,
                COUNT( DISTINCT page_views_enriched_0.customer_id) AS visitor_count
            FROM order_details_0 FULL OUTER JOIN page_views_enriched_0 ON order_details_0.date_id = page_views_enriched_0.date_id AND order_details_0.category = page_views_enriched_0.category AND order_details_0.week = page_views_enriched_0.week
            GROUP BY  1, 2, 3
            ),
            order_details_week_agg AS (
            SELECT  base_metrics.category AS category,
                base_metrics.week AS week,
                COUNT( DISTINCT base_metrics.order_id) AS order_count,
                COUNT( DISTINCT base_metrics.customer_id) AS visitor_count
            FROM base_metrics
            GROUP BY  base_metrics.category, base_metrics.week
            ),
            order_details_week AS (
            SELECT  order_details_week_agg.category AS category,
                order_details_week_agg.week AS week,
                (order_details_week_agg.order_count + order_details_week_agg.visitor_count) - LAG(order_details_week_agg.order_count + order_details_week_agg.visitor_count, 1) OVER ( PARTITION BY order_details_week_agg.category
            ORDER BY order_details_week_agg.week)  AS wow_order_and_visitor_change
            FROM order_details_week_agg
            )
            SELECT  base_metrics.date_id AS date_id,
                base_metrics.category AS category,
                base_metrics.week AS week,
                order_details_week.wow_order_and_visitor_change AS wow_order_and_visitor_change
            FROM base_metrics LEFT OUTER JOIN order_details_week ON base_metrics.category = order_details_week.category AND base_metrics.week = order_details_week.week
            """,
        )

    @pytest.mark.asyncio
    async def test_multi_fact_window_metrics_same_grain(self, client_with_build_v3):
        """
        Test window metrics from different facts with same ORDER BY grain.

        This tests the critical scenario where:
        1. wow_revenue_change (order_details) uses ORDER BY week
        2. wow_pages_per_session_change (page_views_enriched) uses ORDER BY week
        3. Both have the same ORDER BY grain but come from DIFFERENT facts

        Expected behavior:
        - Each fact gets its own window aggregation CTE
        - order_details metrics use order_details_0 as source
        - page_views metrics use page_views_enriched_0 as source
        - They are NOT mixed together

        This is similar to the demo case with thumbs_up_rate_wow_change (thumb_rating)
        and avg_download_time_wow_change (download_session).
        """
        # Create the metric locally for this test
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.wow_pages_per_session_change",
                "description": (
                    "Week-over-week pages per session change (%). "
                    "Single-fact window metric from page_views_enriched. "
                    "Tests that this is separated from order_details window metrics."
                ),
                "query": """
                    SELECT
                        (v3.pages_per_session - LAG(v3.pages_per_session, 1) OVER (ORDER BY v3.date.week[order]))
                        / NULLIF(LAG(v3.pages_per_session, 1) OVER (ORDER BY v3.date.week[order]), 0) * 100
                """,
                "mode": "published",
                "required_dimensions": ["v3.date.week[order]"],
            },
        )
        assert response.status_code in (200, 201), response.json()

        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": [
                    "v3.wow_revenue_change",  # From order_details
                    "v3.wow_pages_per_session_change",  # From page_views_enriched
                ],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()
        sql = result["sql"]
        assert_sql_equal(
            sql,
            """
            WITH
            v3_date AS (
            SELECT  date_id,
                week
            FROM default.v3.dates
            ),
            v3_order_details AS (
            SELECT  o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
            SELECT  product_id,
                category
            FROM default.v3.products
            ),
            v3_page_views_enriched AS (
            SELECT  view_id,
                session_id,
                page_date,
                product_id
            FROM default.v3.page_views
            ),
            order_details_0 AS (
            SELECT  t2.category,
                t3.week,
                SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
            GROUP BY  t2.category, t3.week
            ),
            page_views_enriched_0 AS (
            SELECT  t2.category,
                t3.week,
                t1.session_id,
                COUNT(t1.view_id) view_id_count_f41e2db4
            FROM v3_page_views_enriched t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            LEFT OUTER JOIN v3_date t3 ON t1.page_date = t3.date_id
            GROUP BY  t2.category, t3.week, t1.session_id
            ),
            base_metrics AS (
            SELECT  COALESCE(order_details_0.category, page_views_enriched_0.category) AS category,
                COALESCE(order_details_0.week, page_views_enriched_0.week) AS week,
                SUM(page_views_enriched_0.view_id_count_f41e2db4) AS page_view_count,
                COUNT( DISTINCT page_views_enriched_0.session_id) AS session_count,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue,
                SUM(page_views_enriched_0.view_id_count_f41e2db4) / NULLIF(COUNT( DISTINCT page_views_enriched_0.session_id), 0) AS pages_per_session
            FROM order_details_0 FULL OUTER JOIN page_views_enriched_0 ON order_details_0.category = page_views_enriched_0.category AND order_details_0.week = page_views_enriched_0.week
            GROUP BY  1, 2
            )

            SELECT  base_metrics.category AS category,
                base_metrics.week AS week,
                (base_metrics.total_revenue - LAG(base_metrics.total_revenue, 1) OVER ( PARTITION BY base_metrics.category
            ORDER BY base_metrics.week) ) / NULLIF(LAG(base_metrics.total_revenue, 1) OVER ( PARTITION BY base_metrics.category
            ORDER BY base_metrics.week) , 0) * 100 AS wow_revenue_change,
                (base_metrics.pages_per_session - LAG(base_metrics.pages_per_session, 1) OVER ( PARTITION BY base_metrics.category
            ORDER BY base_metrics.week) ) / NULLIF(LAG(base_metrics.pages_per_session, 1) OVER ( PARTITION BY base_metrics.category
            ORDER BY base_metrics.week) , 0) * 100 AS wow_pages_per_session_change
            FROM base_metrics
            """,
        )
