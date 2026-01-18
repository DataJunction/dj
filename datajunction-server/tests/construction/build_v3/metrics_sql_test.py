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
        data = response.json()
        sql = data["sql"]

        # Should have WHERE clause in the final SQL
        assert "WHERE" in sql
        assert "category" in sql
        assert "'Electronics'" in sql


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

        These use LAG() window functions and require a base_metrics CTE
        to pre-compute the base metrics before applying window functions.
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
                COUNT( DISTINCT order_details_0.order_id) AS order_count,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
              FROM order_details_0
              GROUP BY order_details_0.category, order_details_0.month, order_details_0.week
            )
            SELECT
              base_metrics.category AS category,
              base_metrics.month AS month,
              base_metrics.week AS week,
              (base_metrics.total_revenue - LAG(base_metrics.total_revenue, 1) OVER (PARTITION BY category, month ORDER BY base_metrics.week))
                / NULLIF(LAG(base_metrics.total_revenue, 1) OVER (PARTITION BY category, month ORDER BY base_metrics.week), 0) * 100
                AS wow_revenue_change,
              (CAST(base_metrics.order_count AS DOUBLE) - LAG(CAST(base_metrics.order_count AS DOUBLE), 1) OVER (PARTITION BY category, month ORDER BY base_metrics.week))
                / NULLIF(LAG(CAST(base_metrics.order_count AS DOUBLE), 1) OVER (PARTITION BY category, month ORDER BY base_metrics.week), 0) * 100
                AS wow_order_growth,
              (base_metrics.total_revenue - LAG(base_metrics.total_revenue, 1) OVER (PARTITION BY category, week ORDER BY base_metrics.month))
                / NULLIF(LAG(base_metrics.total_revenue, 1) OVER (PARTITION BY category, week ORDER BY base_metrics.month), 0) * 100
                AS mom_revenue_change
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
              (SUM(base_metrics.total_revenue) OVER ( ORDER BY base_metrics.date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  - SUM(base_metrics.total_revenue) OVER ( ORDER BY base_metrics.date_id ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) ) / NULLIF(SUM(base_metrics.total_revenue) OVER ( ORDER BY base_metrics.date_id ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) , 0) * 100 AS trailing_wow_revenue_change
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
              SUM(base_metrics.total_revenue) OVER ( ORDER BY base_metrics.date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS trailing_7d_revenue
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
