"""Tests for an explicit WHERE clause in a metric definition."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datajunction_server.construction.build_v3.decomposition import (
    analyze_grain_groups,
    merge_grain_groups,
)
from datajunction_server.construction.build_v3.metrics import _union_cte_columns
from datajunction_server.construction.build_v3.preagg_matcher import (
    preagg_measure_matches_component,
)
from datajunction_server.construction.build_v3.types import (
    DecomposedMetricInfo,
    MetricGroup,
)
from datajunction_server.database.preaggregation import (
    compute_expression_hash,
    measure_identity_token,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    MetricComponent,
    PreAggMeasure,
)
from datajunction_server.internal.sql import METRIC_WHERE_MESSAGE
from datajunction_server.sql.decompose import (
    MetricComponentExtractor,
    MetricData,
    validate_where_shape,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import get_query_service_client

from . import assert_sql_equal, get_first_grain_group

COMPLETED_REVENUE = (
    "SELECT SUM(line_total) FROM v3.order_details WHERE status = 'completed'"
)


async def create_metric(client, name, query, status_code=201, **extra):
    """Create a metric and return the response."""
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": name,
            "description": "Metric with a WHERE clause",
            "query": query,
            "mode": "published",
            **extra,
        },
    )
    assert response.status_code == status_code, response.json()
    return response


def make_component(name, where, rule_type=Aggregability.FULL, level=None):
    return MetricComponent(
        name=name,
        expression="line_total",
        aggregation="SUM",
        merge="SUM",
        rule=AggregationRule(type=rule_type, level=level),
        where=where,
    )


def make_decomposed(name, components):
    return DecomposedMetricInfo(
        metric_node=SimpleNamespace(name=name),
        components=components,
        aggregability=Aggregability.FULL,
        combiner="",
        derived_ast=parse("SELECT 1"),
    )


@pytest.mark.asyncio
async def test_metric_where_filters_the_parent_scan(client_with_build_v3):
    """The metric's own WHERE is applied before aggregation."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.completed_revenue"],
            "dimensions": ["v3.product.category"],
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH v3_order_details AS (
            SELECT o.status, oi.product_id, oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o
            JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        v3_product AS (
            SELECT product_id, category FROM default.v3.products
        ),
        order_details_0 AS (
            SELECT t2.category, SUM(t1.line_total) line_total_sum_217f4197
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t1.status = 'completed'
            GROUP BY t2.category
        )
        SELECT order_details_0.category AS category,
               SUM(order_details_0.line_total_sum_217f4197) AS completed_revenue
        FROM order_details_0
        GROUP BY order_details_0.category
        """,
    )


@pytest.mark.asyncio
async def test_measures_sql_carries_the_filtered_component(client_with_build_v3):
    """The measures SQL filters the scan and names the component by its filter."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)

    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.completed_revenue"],
            "dimensions": ["v3.product.category"],
        },
    )
    assert response.status_code == 200, response.json()
    grain_group = response.json()["grain_groups"][0]
    assert_sql_equal(
        grain_group["sql"],
        """
        WITH v3_order_details AS (
            SELECT o.status, oi.product_id, oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o
            JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        v3_product AS (
            SELECT product_id, category FROM default.v3.products
        )
        SELECT t2.category, SUM(t1.line_total) line_total_sum_217f4197
        FROM v3_order_details t1
        LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
        WHERE t1.status = 'completed'
        GROUP BY t2.category
        """,
    )
    assert grain_group["components"] == [
        {
            "name": "line_total_sum_217f4197",
            "expression": "line_total",
            "aggregation": "SUM",
            "merge": "SUM",
            "aggregability": "full",
        },
    ]


@pytest.mark.asyncio
async def test_different_where_clauses_do_not_share_a_scan(client_with_build_v3):
    """Metrics on one parent with different WHERE clauses scan separately."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)
    await create_metric(
        client_with_build_v3,
        "v3.pending_revenue",
        "SELECT SUM(line_total) FROM v3.order_details WHERE status = 'pending'",
    )

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.completed_revenue", "v3.pending_revenue"],
            "dimensions": ["v3.order_details.status"],
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH v3_order_details AS (
            SELECT o.status, oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o
            JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        order_details_0 AS (
            SELECT t1.status, SUM(t1.line_total) line_total_sum_217f4197
            FROM v3_order_details t1
            WHERE t1.status = 'completed'
            GROUP BY t1.status
        ),
        order_details_1 AS (
            SELECT t1.status, SUM(t1.line_total) line_total_sum_5cb242ec
            FROM v3_order_details t1
            WHERE t1.status = 'pending'
            GROUP BY t1.status
        )
        SELECT COALESCE(order_details_0.status, order_details_1.status) AS status,
               SUM(order_details_0.line_total_sum_217f4197) AS completed_revenue,
               SUM(order_details_1.line_total_sum_5cb242ec) AS pending_revenue
        FROM order_details_0
        FULL OUTER JOIN order_details_1
            ON order_details_0.status = order_details_1.status
        GROUP BY 1
        """,
    )


@pytest.mark.asyncio
async def test_caller_filter_does_not_leak_the_metric_where(client_with_build_v3):
    """A caller filter reaches every scan; the metric WHERE only its own."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.completed_revenue", "v3.total_revenue"],
            "dimensions": ["v3.product.category"],
            "filters": ["v3.product.category = 'Electronics'"],
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH v3_order_details AS (
            SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total, o.status
            FROM default.v3.orders o
            JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        v3_product AS (
            SELECT product_id, category
            FROM default.v3.products
            WHERE category = 'Electronics'
        ),
        order_details_0 AS (
            SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics'
            GROUP BY t2.category
        ),
        order_details_1 AS (
            SELECT t2.category, SUM(t1.line_total) line_total_sum_217f4197
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics' AND t1.status = 'completed'
            GROUP BY t2.category
        )
        SELECT COALESCE(order_details_0.category, order_details_1.category) AS category,
               SUM(order_details_1.line_total_sum_217f4197) AS completed_revenue,
               SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
        FROM order_details_0
        FULL OUTER JOIN order_details_1
            ON order_details_0.category = order_details_1.category
        WHERE order_details_0.category = 'Electronics'
        GROUP BY 1
        """,
    )


@pytest.mark.asyncio
async def test_derived_metric_components_keep_their_own_where(client_with_build_v3):
    """Each base component carries its filter; they combine after aggregation."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)
    await create_metric(
        client_with_build_v3,
        "v3.completed_share",
        "SELECT v3.completed_revenue / v3.total_revenue",
    )

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.completed_share"],
            "dimensions": ["v3.product.category"],
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH v3_order_details AS (
            SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total, o.status
            FROM default.v3.orders o
            JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        v3_product AS (
            SELECT product_id, category FROM default.v3.products
        ),
        order_details_0 AS (
            SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
        ),
        order_details_1 AS (
            SELECT t2.category, SUM(t1.line_total) line_total_sum_217f4197
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t1.status = 'completed'
            GROUP BY t2.category
        )
        SELECT COALESCE(order_details_0.category, order_details_1.category) AS category,
               SUM(order_details_1.line_total_sum_217f4197)
                   / NULLIF(SUM(order_details_0.line_total_sum_e1f61696), 0)
                   AS completed_share
        FROM order_details_0
        FULL OUTER JOIN order_details_1
            ON order_details_0.category = order_details_1.category
        GROUP BY 1
        """,
    )


@pytest.mark.asyncio
async def test_count_distinct_where_filters_before_the_distinct(
    client_with_build_v3,
):
    """A limited-aggregability metric filters its scan and keeps a unique name."""
    await create_metric(
        client_with_build_v3,
        "v3.completed_customers",
        "SELECT COUNT(DISTINCT customer_id) FROM v3.order_details "
        "WHERE status = 'completed'",
    )

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.completed_customers", "v3.total_revenue"],
            "dimensions": ["v3.product.category"],
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH v3_order_details AS (
            SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total,
                   o.customer_id, o.status
            FROM default.v3.orders o
            JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        v3_product AS (
            SELECT product_id, category FROM default.v3.products
        ),
        order_details_0 AS (
            SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
        ),
        order_details_1 AS (
            SELECT t2.category, t1.customer_id
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t1.status = 'completed'
            GROUP BY t2.category, t1.customer_id
        ),
        order_details_1_agg AS (
            SELECT category,
                   COUNT(DISTINCT customer_id) customer_id_distinct_d9046706
            FROM order_details_1
            GROUP BY category
        )
        SELECT COALESCE(order_details_0.category, order_details_1_agg.category)
                   AS category,
               MAX(order_details_1_agg.customer_id_distinct_d9046706)
                   AS completed_customers,
               SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
        FROM order_details_0
        FULL OUTER JOIN order_details_1_agg
            ON order_details_0.category = order_details_1_agg.category
        GROUP BY 1
        """,
    )


@pytest.mark.asyncio
async def test_where_applies_before_a_fixed_grain(client_with_build_v3):
    """A fixed-grain metric filters before its first aggregation."""
    await create_metric(
        client_with_build_v3,
        "v3.completed_global",
        COMPLETED_REVENUE,
        fixed_grain=[],
    )

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.completed_global"],
            "dimensions": ["v3.order_details.status"],
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH v3_order_details AS (
            SELECT o.status, oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o
            JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        order_details_0 AS (
            SELECT t1.status, SUM(t1.line_total) line_total_sum_217f4197
            FROM v3_order_details t1
            WHERE t1.status = 'completed'
            GROUP BY t1.status
        )
        SELECT order_details_0.status AS status,
               SUM(SUM(order_details_0.line_total_sum_217f4197)) OVER () AS completed_global
        FROM order_details_0
        GROUP BY order_details_0.status
        """,
    )


@pytest.mark.asyncio
async def test_where_is_rejected_with_window_metrics(client_with_build_v3):
    """Window metrics at a coarser grain do not support a metric WHERE yet."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)
    await create_metric(
        client_with_build_v3,
        "v3.wow_completed",
        "SELECT v3.completed_revenue - LAG(v3.completed_revenue, 1) "
        "OVER (ORDER BY v3.date.week[order])",
        required_dimensions=["v3.date.week[order]"],
    )

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.wow_completed"],
            "dimensions": ["v3.date.week[order]", "v3.product.category"],
        },
    )
    assert response.status_code == 422, response.json()
    assert response.json()["message"] == (
        "A metric WHERE is not supported with window metrics."
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("query", "status_code", "message"),
    [
        (
            "SELECT v3.completed_revenue / v3.total_revenue WHERE status = 'x'",
            400,
            "A WHERE clause is only supported on base metrics.",
        ),
        (
            "SELECT SUM(line_total) FROM v3.order_details "
            "WHERE v3.product.category = 'x'",
            400,
            "A metric WHERE can only use parent columns, not v3.product.",
        ),
        (
            "SELECT SUM(line_total) FROM v3.order_details WHERE nope = 'x'",
            422,
            "Metric definition references missing columns: nope",
        ),
        (
            "SELECT MAX_BY(product_id, line_total) FROM v3.order_details "
            "WHERE status = 'x'",
            422,
            "A metric WHERE is not supported with this aggregate.",
        ),
    ],
    ids=["derived", "dimension-attribute", "missing-column", "not-decomposable"],
)
async def test_invalid_where_is_rejected_on_create(
    client_with_build_v3,
    query,
    status_code,
    message,
):
    """Unsupported WHERE clauses fail at write time with a clear message."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)
    response = await create_metric(
        client_with_build_v3,
        "v3.bad_where",
        query,
        status_code=status_code,
    )
    assert response.json()["message"] == message


@pytest.mark.asyncio
async def test_qualified_where_columns_are_accepted(client_with_build_v3):
    """Columns qualified by the parent name or alias are parent columns."""
    await create_metric(
        client_with_build_v3,
        "v3.aliased_where",
        "SELECT SUM(od.line_total) FROM v3.order_details od WHERE od.status = 'q'",
    )
    await create_metric(
        client_with_build_v3,
        "v3.named_where",
        "SELECT SUM(line_total) FROM v3.order_details "
        "WHERE v3.order_details.status = 'q'",
    )

    for name in ("v3.aliased_where", "v3.named_where"):
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={"metrics": [name], "dimensions": ["v3.order_details.status"]},
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_HASH
            FROM v3_order_details t1
            WHERE t1.status = 'q'
            GROUP BY t1.status
            """,
            normalize_aliases=True,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    ["/sql/", "/sql/measures/v2/"],
)
async def test_legacy_sql_rejects_a_metric_where(client_with_build_v3, path):
    """The legacy builder would ignore the WHERE, so it refuses instead."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)

    response = await client_with_build_v3.get(
        path,
        params={
            "metrics": ["v3.completed_revenue"],
            "dimensions": ["v3.product.category"],
        },
    )
    assert response.status_code == 422, response.json()
    assert response.json()["message"] == (
        "Metrics with a WHERE clause need the v3 endpoints."
    )


async def register_preagg(client, metric, column):
    """Register an external pre-aggregation table for one metric."""

    async def fake_columns(*args, **kwargs):
        return [
            SimpleNamespace(name="status", type="string"),
            SimpleNamespace(name=column, type="double"),
        ]

    query_service = MagicMock()
    query_service.get_columns_for_table = fake_columns
    client.app.dependency_overrides[get_query_service_client] = lambda: query_service
    try:
        return await client.post(
            "/preaggs/register",
            json={
                "metrics": [metric],
                "dimensions": ["v3.order_details.status"],
                "table": {
                    "catalog": "default",
                    "schema": "analytics",
                    "table": "by_status",
                    "valid_through_ts": 20250101,
                },
                "measure_columns": {metric: column},
            },
        )
    finally:
        del client.app.dependency_overrides[get_query_service_client]


@pytest.mark.asyncio
async def test_unfiltered_preagg_never_serves_a_filtered_metric(
    client_with_build_v3,
):
    """A pre-aggregation built without the WHERE is not read for the metric."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)
    response = await register_preagg(client_with_build_v3, "v3.total_revenue", "rev")
    assert response.status_code == 201, response.text

    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.completed_revenue"],
            "dimensions": ["v3.order_details.status"],
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        get_first_grain_group(response.json())["sql"],
        """
        WITH v3_order_details AS (
            SELECT o.status, oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o
            JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        )
        SELECT t1.status, SUM(t1.line_total) line_total_sum_217f4197
        FROM v3_order_details t1
        WHERE t1.status = 'completed'
        GROUP BY t1.status
        """,
    )

    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.order_details.status"],
        },
    )
    assert_sql_equal(
        get_first_grain_group(response.json())["sql"],
        "SELECT status, SUM(rev) rev FROM default.analytics.by_status GROUP BY status",
    )


@pytest.mark.asyncio
async def test_filtered_preagg_serves_only_its_metric(client_with_build_v3):
    """A pre-aggregation built with the WHERE serves that metric alone."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)
    response = await register_preagg(
        client_with_build_v3,
        "v3.completed_revenue",
        "completed_rev",
    )
    assert response.status_code == 201, response.text

    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.completed_revenue"],
            "dimensions": ["v3.order_details.status"],
        },
    )
    assert_sql_equal(
        get_first_grain_group(response.json())["sql"],
        "SELECT status, SUM(completed_rev) completed_rev "
        "FROM default.analytics.by_status GROUP BY status",
    )

    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.order_details.status"],
        },
    )
    assert_sql_equal(
        get_first_grain_group(response.json())["sql"],
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


@pytest.mark.asyncio
async def test_registering_a_column_for_the_wrong_filter_fails(
    client_with_build_v3,
):
    """A measure column named for another metric's filter is not accepted."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)
    await create_metric(
        client_with_build_v3,
        "v3.pending_revenue",
        "SELECT SUM(line_total) FROM v3.order_details WHERE status = 'pending'",
    )

    async def fake_columns(*args, **kwargs):
        return [
            SimpleNamespace(name="status", type="string"),
            SimpleNamespace(name="rev", type="double"),
        ]

    query_service = MagicMock()
    query_service.get_columns_for_table = fake_columns
    client = client_with_build_v3
    client.app.dependency_overrides[get_query_service_client] = lambda: query_service
    try:
        response = await client.post(
            "/preaggs/register",
            json={
                "metrics": ["v3.completed_revenue"],
                "dimensions": ["v3.order_details.status"],
                "table": {
                    "catalog": "default",
                    "schema": "analytics",
                    "table": "by_status",
                    "valid_through_ts": 20250101,
                },
                "measure_columns": {"v3.pending_revenue": "rev"},
            },
        )
    finally:
        del client.app.dependency_overrides[get_query_service_client]
    assert response.status_code == 422, response.json()
    assert response.json()["message"] == (
        "Measure 'line_total_sum_217f4197' (SUM over 'line_total') required by "
        "the requested metrics is not covered by measure_columns. Add the "
        "is_measure metric it corresponds to."
    )


def test_extraction_records_the_where_on_each_component():
    """The WHERE is normalized onto components and changes their names."""
    extractor = MetricComponentExtractor(0)
    plain, _ = extractor._extract_base(parse("SELECT SUM(x) FROM t"))
    filtered, derived = extractor._extract_base(
        parse("SELECT SUM(x) FROM t a WHERE a.k = 1 AND t.j > 2"),
    )
    other, _ = extractor._extract_base(parse("SELECT SUM(x) FROM t WHERE k = 2"))

    assert [(c.name, c.where) for c in plain] == [("x_sum_68d2fbf3", None)]
    assert [(c.name, c.where) for c in filtered] == [
        ("x_sum_347dccf1", "k = 1 AND j > 2"),
    ]
    assert [(c.name, c.where) for c in other] == [("x_sum_ad2a80e3", "k = 2")]
    assert derived.select.where is None


def test_where_is_left_out_of_dumps_when_unset():
    """Components without a WHERE serialize exactly as before."""
    assert make_component("a", None).model_dump()["name"] == "a"
    assert "where" not in make_component("a", None).model_dump()
    assert make_component("a", "k = 1").model_dump()["where"] == "k = 1"


def test_where_is_part_of_measure_identity():
    """Only filtered measures get a WHERE segment in their token."""
    assert measure_identity_token("abc", "sum") == "abc:SUM"
    assert measure_identity_token("abc", "sum", None, "k = 1") == (
        "abc:SUM|where=k = 1"
    )
    assert measure_identity_token("abc", "sum", {"p": 1}, "k = 1") == (
        'abc:SUM:{"p":1}|where=k = 1'
    )


def test_preagg_measure_matching_requires_equal_where():
    """A stored measure matches only a component with the same WHERE."""
    expr_hash = compute_expression_hash("line_total")

    def stored(where):
        return PreAggMeasure(
            **make_component("a", where).model_dump(),
            expr_hash=expr_hash,
        )

    assert preagg_measure_matches_component(stored(None), make_component("b", None))
    assert preagg_measure_matches_component(
        stored("k = 1"),
        make_component("b", "k = 1"),
    )
    assert not preagg_measure_matches_component(
        stored(None),
        make_component("b", "k = 1"),
    )
    assert not preagg_measure_matches_component(
        stored("k = 1"),
        make_component("b", None),
    )
    assert not preagg_measure_matches_component(
        stored("k = 1"),
        make_component("b", "k = 2"),
    )


def test_grain_groups_split_and_stay_split_by_where():
    """Components with different WHERE clauses never share a grain group."""
    parent = SimpleNamespace(
        name="v3.order_details",
        current=SimpleNamespace(columns=[]),
    )
    group = MetricGroup(
        parent_node=parent,
        decomposed_metrics=[
            make_decomposed("m1", [make_component("a", None)]),
            make_decomposed("m2", [make_component("b", "k = 1")]),
            make_decomposed("m3", [make_component("c", "k = 1")]),
            make_decomposed(
                "m4",
                [
                    make_component(
                        "d",
                        "k = 1",
                        rule_type=Aggregability.LIMITED,
                        level=["u"],
                    ),
                ],
            ),
        ],
    )
    grain_groups = merge_grain_groups(analyze_grain_groups(group, []))

    assert [
        (
            gg.where,
            gg.is_merged,
            [component.name for _, component in gg.components],
        )
        for gg in grain_groups
    ] == [
        (None, False, ["a"]),
        ("k = 1", True, ["b", "c", "d"]),
    ]


def test_where_needs_a_decomposable_aggregate():
    """An aggregate that cannot split into measures cannot carry a WHERE."""
    validate_where_shape("SELECT SUM(x) FROM t WHERE k = 1")
    with pytest.raises(DJInvalidInputException) as exc_info:
        validate_where_shape("SELECT MAX_BY(a, b) FROM t WHERE k = 1")
    assert str(exc_info.value) == (
        "A metric WHERE is not supported with this aggregate."
    )


def test_shared_cte_keeps_columns_either_copy_needs():
    """Duplicate copies of a CTE pruned differently are merged."""
    kept = parse("SELECT a, b FROM t")
    other = parse("SELECT b, c AS d FROM t")
    _union_cte_columns(kept, other)
    assert str(kept) == str(parse("SELECT a, b, c AS d FROM t"))

    unioned = parse("SELECT a FROM t UNION ALL SELECT a FROM u")
    _union_cte_columns(unioned, other)
    assert str(unioned) == str(parse("SELECT a FROM t UNION ALL SELECT a FROM u"))


@pytest.mark.asyncio
async def test_combined_measures_keep_each_where(client_with_build_v3):
    """Combined measures SQL joins the filtered and unfiltered scans."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)
    response = await client_with_build_v3.get(
        "/sql/measures/v3/combined",
        params={
            "metrics": ["v3.completed_revenue", "v3.total_revenue"],
            "dimensions": ["v3.order_details.status"],
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH v3_order_details AS (
            SELECT o.status, oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o
            JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        gg1 AS (
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
        ),
        gg2 AS (
            SELECT t1.status, SUM(t1.line_total) line_total_sum_217f4197
            FROM v3_order_details t1
            WHERE t1.status = 'completed'
            GROUP BY t1.status
        )
        SELECT COALESCE(gg1.status, gg2.status) status,
               gg1.line_total_sum_e1f61696,
               gg2.line_total_sum_217f4197
        FROM gg1 FULL OUTER JOIN gg2 ON gg1.status = gg2.status
        """,
    )


@pytest.mark.asyncio
async def test_legacy_sql_rejects_a_derived_metric_over_where(client_with_build_v3):
    """The legacy builder also refuses a metric built on a filtered one."""
    await create_metric(client_with_build_v3, "v3.completed_revenue", COMPLETED_REVENUE)
    await create_metric(
        client_with_build_v3,
        "v3.completed_share",
        "SELECT v3.completed_revenue / v3.total_revenue",
    )
    response = await client_with_build_v3.get(
        "/sql/",
        params={
            "metrics": ["v3.completed_share"],
            "dimensions": ["v3.product.category"],
        },
    )
    assert response.status_code == 422, response.json()
    assert response.json()["message"] == METRIC_WHERE_MESSAGE


@pytest.mark.asyncio
async def test_extraction_rejects_a_derived_metric_where():
    """Metrics that predate validation still cannot extract with a WHERE."""
    extractor = MetricComponentExtractor(0)
    data = MetricData(
        query="SELECT a / b WHERE x = 1",
        is_derived=True,
        base_metrics=[],
    )
    with patch.object(extractor, "_load_metric_data", AsyncMock(return_value=data)):
        with pytest.raises(DJInvalidInputException) as exc_info:
            await extractor.extract(None)
    assert str(exc_info.value) == "A WHERE clause is only supported on base metrics."
