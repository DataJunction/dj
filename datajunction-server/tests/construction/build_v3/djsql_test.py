"""
Tests for the /djsql/ endpoint using the v3 builder.

These tests validate that DJ SQL queries are correctly parsed and translated
to executable SQL using the build_v3 metrics SQL builder.
"""

import pytest
from . import assert_sql_equal


class TestDJSQLBasic:
    """Basic tests for DJ SQL parsing and translation."""

    @pytest.mark.asyncio
    async def test_simple_single_metric(self, client_with_build_v3):
        """
        Test DJ SQL with a single metric and dimension.
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.total_revenue, v3.order_details.status
                    FROM metrics
                    GROUP BY v3.order_details.status
                """,
                "dialect": "spark",
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

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

        # Verify columns
        assert len(result["columns"]) == 2
        assert result["columns"][0]["name"] == "status"
        assert result["columns"][0]["semantic_type"] == "dimension"
        assert result["columns"][1]["name"] == "total_revenue"
        assert result["columns"][1]["semantic_type"] == "metric"

    @pytest.mark.asyncio
    async def test_multiple_metrics_same_grain(self, client_with_build_v3):
        """
        Test DJ SQL with multiple metrics from the same parent.
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.total_revenue, v3.total_quantity, v3.order_details.status
                    FROM metrics
                    GROUP BY v3.order_details.status
                """,
                "dialect": "spark",
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert_sql_equal(
            result["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.status,
                    oi.quantity,
                    oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            order_details_0 AS (
                SELECT t1.status,
                    SUM(t1.line_total) line_total_sum_e1f61696,
                    SUM(t1.quantity) quantity_sum_06b64d2e
                FROM v3_order_details t1
                GROUP BY t1.status
            )
            SELECT COALESCE(order_details_0.status) AS status,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue,
                SUM(order_details_0.quantity_sum_06b64d2e) AS total_quantity
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )

        # Verify columns
        assert len(result["columns"]) == 3
        column_names = [c["name"] for c in result["columns"]]
        assert "status" in column_names
        assert "total_revenue" in column_names
        assert "total_quantity" in column_names

    @pytest.mark.asyncio
    async def test_derived_metric(self, client_with_build_v3):
        """
        Test DJ SQL with a derived metric (ratio of two metrics).
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.avg_order_value, v3.order_details.status
                    FROM metrics
                    GROUP BY v3.order_details.status
                """,
                "dialect": "spark",
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # avg_order_value = total_revenue / order_count
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
                   SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT(DISTINCT order_details_0.order_id), 0) AS avg_order_value
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_with_filter(self, client_with_build_v3):
        """
        Test DJ SQL with a WHERE filter.
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.total_revenue, v3.order_details.status
                    FROM metrics
                    WHERE v3.order_details.status = 'completed'
                    GROUP BY v3.order_details.status
                """,
                "dialect": "spark",
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # The filter should be applied in the SQL
        assert "completed" in result["sql"].lower() or "WHERE" in result["sql"].upper()


class TestDJSQLWithPreAggregation:
    """Tests for DJ SQL with pre-aggregation availability."""

    @pytest.mark.asyncio
    async def test_metric_with_preagg_available(self, client_with_build_v3):
        """
        Test DJ SQL uses pre-aggregated table when availability is set.
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

        # Now query via DJ SQL - should use pre-agg
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.total_revenue, v3.order_details.status
                    FROM metrics
                    GROUP BY v3.order_details.status
                """,
                "dialect": "spark",
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Should use the pre-agg table
        assert_sql_equal(
            result["sql"],
            """
            WITH order_details_0 AS (
                SELECT status, SUM(line_total_sum_e1f61696) line_total_sum_e1f61696
                FROM warehouse.preaggs.v3_revenue_by_status
                GROUP BY status
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_metric_without_preagg(self, client_with_build_v3):
        """
        Test DJ SQL computes from source when no pre-agg is available.

        This is essentially the same as test_simple_single_metric but
        explicitly documents the "no preagg" path.
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.total_quantity, v3.order_details.status
                    FROM metrics
                    GROUP BY v3.order_details.status
                """,
                "dialect": "spark",
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Should compute from source tables (v3_order_details CTE)
        assert "v3_order_details" in result["sql"]
        assert "default.v3.orders" in result["sql"]
        assert "default.v3.order_items" in result["sql"]


class TestDJSQLValidation:
    """Tests for DJ SQL validation and error handling."""

    @pytest.mark.asyncio
    async def test_missing_from_metrics(self, client_with_build_v3):
        """
        Test that DJ SQL requires FROM metrics.
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.total_revenue
                    FROM some_table
                """,
                "dialect": "spark",
            },
        )

        assert response.status_code == 422  # Validation error
        assert "metrics" in response.json()["message"].lower()

    @pytest.mark.asyncio
    async def test_no_metrics_in_select(self, client_with_build_v3):
        """
        Test that DJ SQL requires at least one metric.
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.order_details.status
                    FROM metrics
                    GROUP BY v3.order_details.status
                """,
                "dialect": "spark",
            },
        )

        assert response.status_code == 422  # Validation error - no metrics
        assert "metric" in response.json()["message"].lower()

    @pytest.mark.asyncio
    async def test_column_not_in_group_by(self, client_with_build_v3):
        """
        Test that non-metric columns must be in GROUP BY.
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.total_revenue, v3.order_details.status
                    FROM metrics
                """,
                "dialect": "spark",
            },
        )

        # Should fail - status not in GROUP BY
        assert response.status_code == 422  # Validation error
        assert "group by" in response.json()["message"].lower()


class TestDJSQLFilterOnlyDimensions:
    """Tests for filter-only dimensions (dimensions in WHERE but not GROUP BY)."""

    @pytest.mark.asyncio
    async def test_filter_on_local_column(self, client_with_build_v3):
        """
        Test filtering on a local column (column on the parent node).

        This should work without needing to join to any dimension tables.
        The filter dimension (status) is already in GROUP BY, so it's not filter-only.
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.total_revenue, v3.order_details.status
                    FROM metrics
                    WHERE v3.order_details.status = 'completed'
                    GROUP BY v3.order_details.status
                """,
                "dialect": "spark",
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # The filter should be applied in the grain group CTE
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
            GROUP BY order_details_0.status
            """,
        )

        # Verify only 2 output columns (status and total_revenue)
        assert result["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_name": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "total_revenue",
                "type": "double",
                "semantic_name": "v3.total_revenue",
                "semantic_type": "metric",
            },
        ]

    @pytest.mark.asyncio
    async def test_filter_only_dimension_resolved(self, client_with_build_v3):
        """
        Test that a dimension referenced only in the filter (not in GROUP BY)
        is properly resolved (JOINed) but not included in output columns.
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.total_revenue, v3.order_details.status
                    FROM metrics
                    WHERE v3.product.category = 'Electronics'
                    GROUP BY v3.order_details.status
                """,
                "dialect": "spark",
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # The filter dimension should be JOINed but not in output
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
                SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                WHERE t2.category = 'Electronics'
                GROUP BY t1.status
            )
            SELECT COALESCE(order_details_0.status) AS status,
                   SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )

        # Verify only 2 output columns (status and total_revenue)
        # The filter dimension (category) should NOT be in the output
        assert result["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_name": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "total_revenue",
                "type": "double",
                "semantic_name": "v3.total_revenue",
                "semantic_type": "metric",
            },
        ]


class TestDJSQLDialects:
    """Tests for different SQL dialects."""

    @pytest.mark.asyncio
    async def test_trino_dialect(self, client_with_build_v3):
        """
        Test DJ SQL with Trino dialect.
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.total_revenue, v3.order_details.status
                    FROM metrics
                    GROUP BY v3.order_details.status
                """,
                "dialect": "trino",
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()
        assert result["dialect"] == "trino"
        assert "SELECT" in result["sql"]

    @pytest.mark.asyncio
    async def test_default_dialect_is_spark(self, client_with_build_v3):
        """
        Test that default dialect is Spark.
        """
        response = await client_with_build_v3.get(
            "/djsql/",
            params={
                "query": """
                    SELECT v3.total_revenue, v3.order_details.status
                    FROM metrics
                    GROUP BY v3.order_details.status
                """,
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()
        assert result["dialect"] == "spark"
