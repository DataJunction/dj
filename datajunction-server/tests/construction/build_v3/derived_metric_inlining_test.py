"""Derived metrics inlined into other derived metrics keep their meaning."""

import pytest
import pytest_asyncio
from httpx import AsyncClient

from . import assert_sql_equal


async def _create_metrics(client: AsyncClient, metrics: dict[str, str]) -> None:
    for name, query in metrics.items():
        response = await client.post(
            "/nodes/metric/",
            json={
                "name": name,
                "description": name,
                "query": query,
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()


@pytest_asyncio.fixture
async def client_with_sums(client_with_build_v3: AsyncClient):
    await _create_metrics(
        client_with_build_v3,
        {
            "v3.sum_a": "SELECT COALESCE(v3.total_revenue, 0) + COALESCE(v3.total_quantity, 0)",
            "v3.sum_b": "SELECT COALESCE(v3.total_quantity, 0) + COALESCE(v3.order_count, 0)",
            "v3.sum_ratio": "SELECT v3.sum_a / NULLIF(v3.sum_b, 0)",
        },
    )
    return client_with_build_v3


@pytest_asyncio.fixture
async def client_with_diamonds(client_with_build_v3: AsyncClient):
    await _create_metrics(
        client_with_build_v3,
        {
            "v3.hours": "SELECT COALESCE(v3.total_revenue, 0) + COALESCE(v3.total_quantity, 0)",
            "v3.left_term": "SELECT v3.hours * 2",
            "v3.right_term": "SELECT v3.hours + 1",
            "v3.diamond": "SELECT v3.left_term / NULLIF(v3.right_term, 0)",
            "v3.term_1": "SELECT v3.hours * 1",
            "v3.term_2": "SELECT v3.hours * 2",
            "v3.term_3": "SELECT v3.hours * 3",
            "v3.term_4": "SELECT v3.hours * 4",
            "v3.term_5": "SELECT v3.hours * 5",
            "v3.five_terms": (
                "SELECT v3.term_1 + v3.term_2 + v3.term_3 + v3.term_4 + v3.term_5"
            ),
        },
    )
    return client_with_build_v3


async def _metrics_sql(client: AsyncClient, metrics: list[str]) -> str:
    response = await client.get(
        "/sql/metrics/v3/",
        params={"metrics": metrics, "dimensions": ["v3.order_details.status"]},
    )
    assert response.status_code == 200, response.json()
    return response.json()["sql"]


STATUS_CTES = """
WITH
v3_order_details AS (
    SELECT o.status, oi.quantity, oi.quantity * oi.unit_price AS line_total
    FROM default.v3.orders o
    JOIN default.v3.order_items oi ON o.order_id = oi.order_id
),
order_details_0 AS (
    SELECT t1.status,
           SUM(t1.quantity) quantity_sum_06b64d2e,
           SUM(t1.line_total) line_total_sum_e1f61696
    FROM v3_order_details t1
    GROUP BY t1.status
)
"""

REVENUE = "SUM(order_details_0.line_total_sum_e1f61696)"
QUANTITY = "SUM(order_details_0.quantity_sum_06b64d2e)"
HOURS = f"COALESCE({REVENUE}, 0) + COALESCE({QUANTITY}, 0)"
FIVE_TERMS = " + ".join(f"({HOURS}) * {n}" for n in range(1, 6))
DIAMOND = f"({HOURS}) * 2 / NULLIF({HOURS} + 1, 0)"


def status_query(*projections: str) -> str:
    return (
        f"{STATUS_CTES} SELECT order_details_0.status AS status, "
        f"{', '.join(projections)} "
        "FROM order_details_0 GROUP BY order_details_0.status"
    )


@pytest.mark.asyncio
async def test_sum_read_by_a_ratio_is_bracketed(client_with_sums):
    """A sum used as a dividend keeps its grouping."""
    sql = await _metrics_sql(client_with_sums, ["v3.sum_ratio"])
    assert_sql_equal(
        sql,
        """
        WITH
        v3_order_details AS (
            SELECT o.order_id, o.status, oi.quantity, oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o
            JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        order_details_0 AS (
            SELECT t1.status, t1.order_id,
                   SUM(t1.quantity) quantity_sum_06b64d2e,
                   SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status, t1.order_id
        )
        SELECT order_details_0.status AS status,
               (COALESCE(SUM(order_details_0.line_total_sum_e1f61696), 0)
                + COALESCE(SUM(order_details_0.quantity_sum_06b64d2e), 0))
               / NULLIF(COALESCE(SUM(order_details_0.quantity_sum_06b64d2e), 0)
                        + COALESCE(COUNT(DISTINCT order_details_0.order_id), 0), 0) AS sum_ratio
        FROM order_details_0
        GROUP BY order_details_0.status
        """,
    )


@pytest.mark.asyncio
async def test_base_metric_operand_is_bracketed(client_with_build_v3):
    """A base metric that is itself a division is bracketed as an operand."""
    await _create_metrics(
        client_with_build_v3,
        {
            "v3.revenue_thousands": "SELECT SUM(line_total) / 1000.0 FROM v3.order_details",
            "v3.quantity_times_thousands": "SELECT v3.total_quantity * v3.revenue_thousands",
        },
    )
    sql = await _metrics_sql(client_with_build_v3, ["v3.quantity_times_thousands"])
    assert_sql_equal(
        sql,
        STATUS_CTES.replace(
            "SUM(t1.quantity) quantity_sum_06b64d2e,\n           "
            "SUM(t1.line_total) line_total_sum_e1f61696",
            "SUM(t1.line_total) line_total_sum_e1f61696,\n           "
            "SUM(t1.quantity) quantity_sum_06b64d2e",
        )
        + f"""
        SELECT order_details_0.status AS status,
               {QUANTITY} * ({REVENUE} / 1000.0) AS quantity_times_thousands
        FROM order_details_0
        GROUP BY order_details_0.status
        """,
    )


@pytest.mark.asyncio
async def test_negated_sum_is_bracketed(client_with_sums):
    """A sum read by a unary minus keeps its grouping."""
    await _create_metrics(
        client_with_sums,
        {"v3.negated_sum": "SELECT -v3.sum_a"},
    )
    sql = await _metrics_sql(client_with_sums, ["v3.negated_sum"])
    assert_sql_equal(
        sql,
        status_query(f"-({HOURS}) AS negated_sum"),
    )


@pytest.mark.asyncio
async def test_atoms_are_not_bracketed(client_with_build_v3):
    """Aggregates inlined into an operator get no brackets."""
    await _create_metrics(
        client_with_build_v3,
        {"v3.revenue_times_quantity": "SELECT v3.total_revenue * v3.total_quantity"},
    )
    sql = await _metrics_sql(client_with_build_v3, ["v3.revenue_times_quantity"])
    assert_sql_equal(
        sql,
        status_query(f"{REVENUE} * {QUANTITY} AS revenue_times_quantity"),
    )


@pytest.mark.asyncio
async def test_diamond_of_derived_metrics(client_with_diamonds):
    """Two derived metrics reading the same derived metric are not a cycle."""
    sql = await _metrics_sql(client_with_diamonds, ["v3.diamond"])
    assert_sql_equal(sql, status_query(f"{DIAMOND} AS diamond"))


@pytest.mark.asyncio
async def test_five_terms_reading_one_metric(client_with_diamonds):
    """Five terms that each read the same metric are not a cycle."""
    sql = await _metrics_sql(client_with_diamonds, ["v3.five_terms"])
    assert_sql_equal(sql, status_query(f"{FIVE_TERMS} AS five_terms"))


@pytest.mark.asyncio
async def test_diamonds_requested_with_other_metrics(client_with_diamonds):
    """Diamonds still resolve beside a base metric."""
    sql = await _metrics_sql(
        client_with_diamonds,
        ["v3.diamond", "v3.five_terms", "v3.total_revenue"],
    )
    assert_sql_equal(
        sql,
        status_query(
            f"{DIAMOND} AS diamond",
            f"{FIVE_TERMS} AS five_terms",
            f"{REVENUE} AS total_revenue",
        ),
    )


@pytest_asyncio.fixture
async def client_with_aliased_parents(client_with_build_v3: AsyncClient):
    """Metrics that name their own result (`... AS name`) and are read by others."""
    await _create_metrics(
        client_with_build_v3,
        {
            "v3.aliased_base": "SELECT COUNT(DISTINCT order_id) AS order_total FROM v3.order_details",
            "v3.aliased_sum": (
                "SELECT COALESCE(v3.total_revenue, 0) "
                "+ COALESCE(v3.total_quantity, 0) AS hours"
            ),
            "v3.ratio_of_aliased": "SELECT v3.aliased_sum / NULLIF(v3.aliased_base, 0)",
            "v3.double_aliased": "SELECT v3.aliased_sum * 2 AS twice",
        },
    )
    return client_with_build_v3


@pytest.mark.asyncio
async def test_a_parent_that_aliases_its_result_is_inlined_without_the_alias(
    client_with_aliased_parents,
):
    """The alias names the parent's own column, so it does not follow the
    expression into the metrics that read it."""
    sql = await _metrics_sql(client_with_aliased_parents, ["v3.ratio_of_aliased"])

    assert "AS hours" not in sql and "AS order_total" not in sql
    assert_sql_equal(
        sql,
        """
        WITH
        v3_order_details AS (
            SELECT o.order_id, o.status, oi.quantity, oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o
            JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        order_details_0 AS (
            SELECT t1.status, t1.order_id,
                   SUM(t1.quantity) quantity_sum_06b64d2e,
                   SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status, t1.order_id
        )
        SELECT order_details_0.status AS status,
               (COALESCE(SUM(order_details_0.line_total_sum_e1f61696), 0)
                + COALESCE(SUM(order_details_0.quantity_sum_06b64d2e), 0))
               / NULLIF(COUNT(DISTINCT order_details_0.order_id), 0) AS ratio_of_aliased
        FROM order_details_0
        GROUP BY order_details_0.status
        """,
    )


@pytest.mark.asyncio
async def test_a_derived_metric_over_an_aliased_parent_still_builds(
    client_with_aliased_parents,
):
    response = await client_with_aliased_parents.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.double_aliased", "v3.aliased_base"],
            "dimensions": ["v3.order_details.status"],
        },
    )
    assert response.status_code == 200, response.json()
