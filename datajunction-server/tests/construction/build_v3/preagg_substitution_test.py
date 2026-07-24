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

from datajunction_server.construction.build_v3.measures import (
    build_grain_group_from_preagg,
)
from datajunction_server.construction.build_v3.types import BuildContext, GrainGroup
from datajunction_server.database.preaggregation import (
    PreAggregation,
    compute_expression_hash,
)
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    MetricComponent,
    PreAggMeasure,
)
from types import SimpleNamespace
from unittest.mock import MagicMock

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.utils import get_query_service_client
from . import assert_sql_equal, get_first_grain_group


async def _register_external_preagg(
    client,
    *,
    metrics,
    dimensions,
    table_ref,
    measure_columns,
    table_columns,
    dimension_columns=None,
    expected_status=201,
):
    """
    Register an externally-built pre-aggregation via /preaggs/register with a
    mocked query service that reports ``table_columns`` for the external table.
    Returns the response (asserts ``expected_status``).
    """

    items = (
        table_columns.items()
        if isinstance(table_columns, dict)
        else [(name, "double") for name in table_columns]
    )

    async def _fake_columns(*args, **kwargs):
        return [SimpleNamespace(name=name, type=type_str) for name, type_str in items]

    mock_qs = MagicMock()
    mock_qs.get_columns_for_table = _fake_columns
    client.app.dependency_overrides[get_query_service_client] = lambda: mock_qs
    try:
        payload = {
            "metrics": metrics,
            "dimensions": dimensions,
            "table": table_ref,
            "measure_columns": measure_columns,
        }
        if dimension_columns is not None:
            payload["dimension_columns"] = dimension_columns
        response = await client.post("/preaggs/register", json=payload)
        assert response.status_code == expected_status, response.text
        return response
    finally:
        del client.app.dependency_overrides[get_query_service_client]


class TestExternalPreAggRouting:
    """Queries route to externally-registered pre-agg tables via source_column."""

    @pytest.mark.asyncio
    async def test_external_preagg_used_at_exact_grain(self, client_with_build_v3):
        """An exact-grain query reads the external table's physical source column."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_by_status",
                "valid_through_ts": 20250101,
            },
            measure_columns={"v3.total_revenue": "revenue_sum"},
            table_columns=["status", "revenue_sum"],
        )
        # Measures SQL: reads the external table, applying SUM over the
        # user-supplied physical column (revenue_sum) aliased to the measure.
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert measures_response.status_code == 200
        measures_sql = get_first_grain_group(measures_response.json())["sql"]
        assert_sql_equal(
            measures_sql,
            """
            SELECT status, SUM(revenue_sum) revenue_sum
            FROM default.analytics.revenue_by_status
            GROUP BY status
            """,
        )

        # Metrics SQL: wraps the pre-agg read in a CTE and applies the combiner.
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert metrics_response.status_code == 200
        assert_sql_equal(
            metrics_response.json()["sql"],
            """
            WITH order_details_0 AS (
                SELECT status, SUM(revenue_sum) revenue_sum
                FROM default.analytics.revenue_by_status
                GROUP BY status
            )
            SELECT order_details_0.status AS status,
                   SUM(order_details_0.revenue_sum) AS total_revenue
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_external_preagg_rolls_up_additive(self, client_with_build_v3):
        """An additive measure rolls up from an external pre-agg at a coarser grain."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_by_status",
                "valid_through_ts": 20250101,
            },
            measure_columns={"v3.total_revenue": "revenue_sum"},
            table_columns=["status", "revenue_sum"],
        )
        # Query at a coarser grain (no dimensions) -> roll up the additive sum
        # straight off the external table.
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={"metrics": ["v3.total_revenue"]},
        )
        assert measures_response.status_code == 200
        measures_sql = get_first_grain_group(measures_response.json())["sql"]
        assert_sql_equal(
            measures_sql,
            """
            SELECT SUM(revenue_sum) revenue_sum
            FROM default.analytics.revenue_by_status
            """,
        )

        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={"metrics": ["v3.total_revenue"]},
        )
        assert metrics_response.status_code == 200
        assert_sql_equal(
            metrics_response.json()["sql"],
            """
            WITH order_details_0 AS (
                SELECT SUM(revenue_sum) revenue_sum
                FROM default.analytics.revenue_by_status
            )
            SELECT SUM(order_details_0.revenue_sum) AS total_revenue
            FROM order_details_0
            """,
        )

    @pytest.mark.asyncio
    async def test_external_non_additive_not_rolled_up(self, client_with_build_v3):
        """A non-additive measure (COUNT DISTINCT) does not roll up to a coarser
        grain; the query falls back to raw sources."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.order_count"],
            dimensions=["v3.order_details.status"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "orders_by_status",
                "valid_through_ts": 20250101,
            },
            measure_columns={"v3.order_count": "order_cnt"},
            table_columns=["status", "order_cnt"],
        )
        # Coarser grain than the pre-agg -> a distinct count cannot be summed.
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={"metrics": ["v3.order_count"]},
        )
        assert response.status_code == 200
        sql = get_first_grain_group(response.json())["sql"]
        assert "default.analytics.orders_by_status" not in sql

    @pytest.mark.asyncio
    async def test_external_preagg_pending_not_used(self, client_with_build_v3):
        """A registered pre-agg with no availability (no valid_through_ts) is not
        used to answer queries."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_pending",
            },
            measure_columns={"v3.total_revenue": "revenue_sum"},
            table_columns=["status", "revenue_sum"],
        )
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert response.status_code == 200
        sql = get_first_grain_group(response.json())["sql"]
        assert "default.analytics.revenue_pending" not in sql

    @pytest.mark.asyncio
    async def test_external_preagg_multiple_measures_covered(
        self,
        client_with_build_v3,
    ):
        """Two additive measures registered on one external table are both read
        from it at the exact grain."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue", "v3.total_quantity"],
            dimensions=["v3.order_details.status"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_qty_by_status",
                "valid_through_ts": 20250101,
            },
            measure_columns={
                "v3.total_revenue": "revenue_sum",
                "v3.total_quantity": "qty_sum",
            },
            table_columns=["status", "revenue_sum", "qty_sum"],
        )
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert measures_response.status_code == 200
        measures_sql = get_first_grain_group(measures_response.json())["sql"]
        assert_sql_equal(
            measures_sql,
            """
            SELECT status,
                   SUM(revenue_sum) revenue_sum,
                   SUM(qty_sum) qty_sum
            FROM default.analytics.revenue_qty_by_status
            GROUP BY status
            """,
        )

        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert metrics_response.status_code == 200
        assert_sql_equal(
            metrics_response.json()["sql"],
            """
            WITH order_details_0 AS (
                SELECT status,
                       SUM(revenue_sum) revenue_sum,
                       SUM(qty_sum) qty_sum
                FROM default.analytics.revenue_qty_by_status
                GROUP BY status
            )
            SELECT order_details_0.status AS status,
                   SUM(order_details_0.revenue_sum) AS total_revenue,
                   SUM(order_details_0.qty_sum) AS total_quantity
            FROM order_details_0
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_external_preagg_partial_metric_coverage_falls_back(
        self,
        client_with_build_v3,
    ):
        """Pre-agg substitution is all-or-nothing per grain group: if the table
        covers only some of the requested measures at that grain, the whole
        group is computed from source (here total_unit_price is uncovered, so
        even the covered total_revenue/total_quantity come from source)."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue", "v3.total_quantity"],
            dimensions=["v3.order_details.status"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_qty_by_status",
                "valid_through_ts": 20250101,
            },
            measure_columns={
                "v3.total_revenue": "revenue_sum",
                "v3.total_quantity": "qty_sum",
            },
            table_columns=["status", "revenue_sum", "qty_sum"],
        )
        # total_unit_price is NOT covered by the pre-agg.
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": [
                    "v3.total_revenue",
                    "v3.total_quantity",
                    "v3.total_unit_price",
                ],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert measures_response.status_code == 200
        grain_groups = measures_response.json()["grain_groups"]
        # All three FULL-additive measures share one grain group, computed from
        # source -- the pre-agg is not referenced.
        assert len(grain_groups) == 1
        measures_sql = grain_groups[0]["sql"]
        assert "default.analytics.revenue_qty_by_status" not in measures_sql
        assert_sql_equal(
            measures_sql,
            """
            WITH v3_order_details AS (
                SELECT o.status,
                       oi.quantity,
                       oi.unit_price,
                       oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status,
                   SUM(t1.line_total) line_total_sum_e1f61696,
                   SUM(t1.quantity) quantity_sum_06b64d2e,
                   SUM(t1.unit_price) unit_price_sum_55cff00f
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

    @pytest.mark.asyncio
    async def test_external_preagg_rollup_over_covered_dimension(
        self,
        client_with_build_v3,
    ):
        """A pre-agg at [status, product_id] answers a coarser [status] query by
        rolling the additive measure up over the dropped dimension."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=[
                "v3.order_details.status",
                "v3.order_details.product_id",
            ],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_by_status_product",
                "valid_through_ts": 20250101,
            },
            measure_columns={"v3.total_revenue": "revenue_sum"},
            table_columns=["status", "product_id", "revenue_sum"],
        )
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert measures_response.status_code == 200
        measures_sql = get_first_grain_group(measures_response.json())["sql"]
        assert_sql_equal(
            measures_sql,
            """
            SELECT status, SUM(revenue_sum) revenue_sum
            FROM default.analytics.revenue_by_status_product
            GROUP BY status
            """,
        )

    @pytest.mark.asyncio
    async def test_external_preagg_extra_dimension_falls_back(
        self,
        client_with_build_v3,
    ):
        """A query needing a dimension the pre-agg lacks cannot use it (a rollup
        cannot add a grain), so it computes from source."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_by_status",
                "valid_through_ts": 20250101,
            },
            measure_columns={"v3.total_revenue": "revenue_sum"},
            table_columns=["status", "revenue_sum"],
        )
        # product_id is not in the pre-agg grain -> cannot be served by it.
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.order_details.status",
                    "v3.order_details.product_id",
                ],
            },
        )
        assert measures_response.status_code == 200
        measures_sql = get_first_grain_group(measures_response.json())["sql"]
        assert "default.analytics.revenue_by_status" not in measures_sql
        assert_sql_equal(
            measures_sql,
            """
            WITH v3_order_details AS (
                SELECT o.status,
                       oi.product_id,
                       oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status,
                   t1.product_id,
                   SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status, t1.product_id
            """,
        )

    @pytest.mark.asyncio
    async def test_external_preagg_filter_on_covered_dimension(
        self,
        client_with_build_v3,
    ):
        """A filter on a dimension in the pre-agg grain is pushed onto the
        pre-agg read."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_by_status",
                "valid_through_ts": 20250101,
            },
            measure_columns={"v3.total_revenue": "revenue_sum"},
            table_columns=["status", "revenue_sum"],
        )
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status = 'completed'"],
            },
        )
        assert measures_response.status_code == 200
        # The grain-group read stays a clean pre-agg scan (no filter inlined).
        measures_sql = get_first_grain_group(measures_response.json())["sql"]
        assert_sql_equal(
            measures_sql,
            """
            SELECT status, SUM(revenue_sum) revenue_sum
            FROM default.analytics.revenue_by_status
            GROUP BY status
            """,
        )
        # Since status is in the grain, the filter is correctly applied at the
        # metrics layer over the pre-agg-derived CTE (post-aggregation is
        # equivalent to pre-aggregation for a grain column).
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status = 'completed'"],
            },
        )
        assert metrics_response.status_code == 200
        assert_sql_equal(
            metrics_response.json()["sql"],
            """
            WITH order_details_0 AS (
                SELECT status, SUM(revenue_sum) revenue_sum
                FROM default.analytics.revenue_by_status
                GROUP BY status
            )
            SELECT order_details_0.status AS status,
                   SUM(order_details_0.revenue_sum) AS total_revenue
            FROM order_details_0
            WHERE order_details_0.status = 'completed'
            GROUP BY order_details_0.status
            """,
        )

    @pytest.mark.asyncio
    async def test_external_preagg_filter_on_uncovered_column(
        self,
        client_with_build_v3,
    ):
        """A filter on a column absent from the pre-agg grain forces a fallback
        to source (the pre-agg has already aggregated that column away)."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_by_status",
                "valid_through_ts": 20250101,
            },
            measure_columns={"v3.total_revenue": "revenue_sum"},
            table_columns=["status", "revenue_sum"],
        )
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.product_id = 5"],
            },
        )
        assert measures_response.status_code == 200
        measures_sql = get_first_grain_group(measures_response.json())["sql"]
        assert "default.analytics.revenue_by_status" not in measures_sql
        # product_id must be filtered before aggregation, so it is inlined into
        # the source CTE rather than applied over the pre-agg.
        assert_sql_equal(
            measures_sql,
            """
            WITH v3_order_details AS (
                SELECT o.status,
                       oi.product_id,
                       oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE oi.product_id = 5
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

    @pytest.mark.asyncio
    async def test_external_preagg_cross_fact_partial_substitution(
        self,
        client_with_build_v3,
    ):
        """Substitution is per grain group: for a cross-fact metric, the fact
        with an external pre-agg reads it while the other fact computes from
        source, then the two are FULL OUTER JOINed on the shared dimension.

        revenue_per_visitor = total_revenue (order_details) / visitor_count
        (page_views_enriched); only total_revenue is pre-aggregated.
        """
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.customer.customer_id"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_by_customer",
                "valid_through_ts": 20250101,
            },
            measure_columns={"v3.total_revenue": "revenue_sum"},
            table_columns=["customer_id", "revenue_sum"],
        )
        metrics_response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.revenue_per_visitor"],
                "dimensions": ["v3.customer.customer_id"],
            },
        )
        assert metrics_response.status_code == 200
        assert_sql_equal(
            metrics_response.json()["sql"],
            """
            WITH
            v3_page_views_enriched AS (
                SELECT customer_id
                FROM default.v3.page_views
            ),
            order_details_0 AS (
                SELECT customer_id, SUM(revenue_sum) revenue_sum
                FROM default.analytics.revenue_by_customer
                GROUP BY customer_id
            ),
            page_views_enriched_0 AS (
                SELECT t1.customer_id
                FROM v3_page_views_enriched t1
                GROUP BY t1.customer_id
            )
            SELECT COALESCE(order_details_0.customer_id,
                            page_views_enriched_0.customer_id) AS customer_id,
                   SUM(order_details_0.revenue_sum)
                   / NULLIF(COUNT(DISTINCT page_views_enriched_0.customer_id), 0)
                   AS revenue_per_visitor
            FROM order_details_0
            FULL OUTER JOIN page_views_enriched_0
                ON order_details_0.customer_id = page_views_enriched_0.customer_id
            GROUP BY 1
            """,
        )

    @pytest.mark.asyncio
    async def test_external_preagg_renamed_dimension_column(
        self,
        client_with_build_v3,
    ):
        """A grain dimension stored under a different physical column name is read
        via dimension_columns and aliased back to the DJ name."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_by_status",
                "valid_through_ts": 20250101,
            },
            measure_columns={"v3.total_revenue": "revenue_sum"},
            dimension_columns={"v3.order_details.status": "order_status"},
            table_columns={"order_status": "string", "revenue_sum": "double"},
        )
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert measures_response.status_code == 200
        measures_sql = get_first_grain_group(measures_response.json())["sql"]
        assert_sql_equal(
            measures_sql,
            """
            SELECT order_status status, SUM(revenue_sum) revenue_sum
            FROM default.analytics.revenue_by_status
            GROUP BY order_status
            """,
        )

    @pytest.mark.asyncio
    async def test_external_preagg_dimension_column_must_exist(
        self,
        client_with_build_v3,
    ):
        """A dimension_columns mapping to a column absent from the table is
        rejected; an unknown dimension key is rejected too."""
        missing = await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            table_ref={"catalog": "default", "schema": "analytics", "table": "t"},
            measure_columns={"v3.total_revenue": "revenue_sum"},
            dimension_columns={"v3.order_details.status": "nope"},
            table_columns=["order_status", "revenue_sum"],
            expected_status=422,
        )
        assert "not found in table" in missing.json()["message"]

        unknown = await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            table_ref={"catalog": "default", "schema": "analytics", "table": "t"},
            measure_columns={"v3.total_revenue": "revenue_sum"},
            dimension_columns={"v3.order_details.not_a_dim": "x"},
            table_columns=["order_status", "revenue_sum", "x"],
            expected_status=422,
        )
        assert "not in the pre-aggregation's dimensions" in unknown.json()["message"]

        # A string dimension bound to a numeric column is type-incompatible.
        bad_type = await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            table_ref={"catalog": "default", "schema": "analytics", "table": "t"},
            measure_columns={"v3.total_revenue": "revenue_sum"},
            dimension_columns={"v3.order_details.status": "status_num"},
            table_columns={"status_num": "bigint", "revenue_sum": "double"},
            expected_status=422,
        )
        assert (
            "not type-compatible with dimension 'v3.order_details.status'"
            in bad_type.json()["message"]
        )

    @pytest.mark.asyncio
    async def test_external_preagg_joined_attribute_read_directly(
        self,
        client_with_build_v3,
    ):
        """A joined dimension attribute stored (denormalized) in the external
        table is read directly from it via dimension_columns -- no join back to
        the dimension node."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_by_category",
                "valid_through_ts": 20250101,
            },
            measure_columns={"v3.total_revenue": "rev_sum"},
            dimension_columns={"v3.product.category": "cat"},
            table_columns={"cat": "string", "rev_sum": "double"},
        )
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
            },
        )
        assert measures_response.status_code == 200
        measures_sql = get_first_grain_group(measures_response.json())["sql"]
        # No JOIN to the product dimension -- the attribute is read from the agg.
        assert "join" not in measures_sql.lower()
        assert_sql_equal(
            measures_sql,
            """
            SELECT cat category, SUM(rev_sum) rev_sum
            FROM default.analytics.revenue_by_category
            GROUP BY cat
            """,
        )

    @pytest.mark.asyncio
    async def test_external_preagg_renamed_measure_and_dimensions(
        self,
        client_with_build_v3,
    ):
        """Renamed measure and multiple renamed dimensions (a local column and a
        joined attribute) coexist on one external table."""
        await _register_external_preagg(
            client_with_build_v3,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status", "v3.product.category"],
            table_ref={
                "catalog": "default",
                "schema": "analytics",
                "table": "revenue_by_status_category",
                "valid_through_ts": 20250101,
            },
            measure_columns={"v3.total_revenue": "rev_sum"},
            dimension_columns={
                "v3.order_details.status": "st",
                "v3.product.category": "cat",
            },
            table_columns={"st": "string", "cat": "string", "rev_sum": "double"},
        )
        measures_response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status", "v3.product.category"],
            },
        )
        assert measures_response.status_code == 200
        measures_sql = get_first_grain_group(measures_response.json())["sql"]
        assert_sql_equal(
            measures_sql,
            """
            SELECT st status, cat category, SUM(rev_sum) rev_sum
            FROM default.analytics.revenue_by_status_category
            GROUP BY st, cat
            """,
        )


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
            SELECT order_details_0.status AS status,
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
            SELECT order_details_0.status AS status,
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

        # avg_order_value = SUM(total_revenue) / NULLIF(COUNT(DISTINCT order_id), 0)
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
            SELECT order_details_0.status AS status,
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
              order_details_0.status AS status,
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
            SELECT order_details_0.status AS status,
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
            SELECT order_details_0.status AS status,
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
            SELECT t1.customer_id
            FROM v3_page_views_enriched t1
            GROUP BY  t1.customer_id
            )

            SELECT  COALESCE(order_details_0.customer_id, page_views_enriched_0.customer_id) AS customer_id,
                SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT( DISTINCT page_views_enriched_0.customer_id), 0) AS revenue_per_visitor
            FROM order_details_0 FULL OUTER JOIN page_views_enriched_0 ON order_details_0.customer_id = page_views_enriched_0.customer_id
            GROUP BY  1
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
            SELECT  t1.customer_id
            FROM v3_page_views_enriched t1
            GROUP BY  t1.customer_id
            )
            SELECT  COALESCE(order_details_0.customer_id, page_views_enriched_0.customer_id) AS customer_id,
                SUM(order_details_0.line_total_sum_e1f61696) / NULLIF(COUNT( DISTINCT page_views_enriched_0.customer_id), 0) AS revenue_per_visitor
            FROM order_details_0 FULL OUTER JOIN page_views_enriched_0 ON order_details_0.customer_id = page_views_enriched_0.customer_id
            GROUP BY  1
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


class TestBuildGrainGroupFromPreaggErrorPaths:
    """Unit tests for error guard paths in build_grain_group_from_preagg."""

    def _make_ctx(self) -> BuildContext:
        return BuildContext(
            session=None,
            metrics=[],
            dimensions=[],
            use_materialized=True,
        )  # type: ignore[arg-type]

    def _make_grain_group(self, node: Node, components: list) -> GrainGroup:
        return GrainGroup(
            parent_node=node,
            aggregability=Aggregability.FULL,
            grain_columns=[],
            components=components,
        )

    def _make_component(self, name: str, expression: str) -> MetricComponent:
        return MetricComponent(
            name=name,
            expression=expression,
            aggregation="SUM",
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        )

    def _make_node(self) -> Node:
        revision = NodeRevision(
            name="test_node",
            display_name="Test Node",
            version="v1.0",
        )
        node = Node(name="test_node", type="transform")
        node.current = revision
        return node

    def test_raises_when_preagg_has_no_availability(self):
        """build_grain_group_from_preagg raises ValueError if preagg.availability is None."""
        node = self._make_node()
        component = self._make_component("rev_sum", "revenue")
        grain_group = self._make_grain_group(node, [(node, component)])

        preagg = PreAggregation(
            node_revision_id=1,
            grain_columns=[],
            measures=[],
            sql="SELECT 1",
            grain_group_hash="abc",
            preagg_hash="def",
            availability=None,
        )

        with pytest.raises(ValueError, match="has no availability"):
            build_grain_group_from_preagg(
                self._make_ctx(),
                grain_group,
                preagg,
                resolved_dimensions=[],
                components_per_metric={},
            )

    def test_raises_when_component_not_in_preagg_measures(self):
        """build_grain_group_from_preagg raises ValueError if a component has no matching measure."""
        from datajunction_server.database.availabilitystate import AvailabilityState

        node = self._make_node()
        component = self._make_component("rev_sum", "revenue")
        grain_group = self._make_grain_group(node, [(node, component)])

        avail = AvailabilityState(
            catalog="wh",
            schema_="preaggs",
            table="tbl",
            valid_through_ts=99999,
        )
        preagg = PreAggregation(
            node_revision_id=1,
            grain_columns=[],
            measures=[],  # empty — no matching measure
            sql="SELECT 1",
            grain_group_hash="abc",
            preagg_hash="def",
            availability=avail,
        )

        with pytest.raises(ValueError, match="not found in pre-agg"):
            build_grain_group_from_preagg(
                self._make_ctx(),
                grain_group,
                preagg,
                resolved_dimensions=[],
                components_per_metric={},
            )

    def test_deduplicates_repeated_components(self):
        """build_grain_group_from_preagg skips duplicate components (same name)."""
        from datajunction_server.database.availabilitystate import AvailabilityState

        node = self._make_node()
        component = self._make_component("rev_sum", "revenue")
        # Same component twice — second should be skipped
        grain_group = self._make_grain_group(
            node,
            [(node, component), (node, component)],
        )

        expr_hash = compute_expression_hash("revenue")
        measure = PreAggMeasure(
            name="rev_col",
            expression="revenue",
            aggregation="SUM",
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
            expr_hash=expr_hash,
        )
        avail = AvailabilityState(
            catalog="wh",
            schema_="preaggs",
            table="tbl",
            valid_through_ts=99999,
        )
        preagg = PreAggregation(
            node_revision_id=1,
            grain_columns=[],
            measures=[measure],
            sql="SELECT 1",
            grain_group_hash="abc",
            preagg_hash="def",
            availability=avail,
        )

        result = build_grain_group_from_preagg(
            self._make_ctx(),
            grain_group,
            preagg,
            resolved_dimensions=[],
            components_per_metric={},
        )
        # Only one component should appear in output despite two in input
        assert len(result.components) == 1
