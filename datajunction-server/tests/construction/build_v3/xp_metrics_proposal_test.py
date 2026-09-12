"""Experimentation-metric shapes, from the XP metrics proposal.

An experimentation metric needs three things: what each allocated unit
contributes, which units are included, and how the contributions combine. DJ
metrics capture the first and third today; the population is supplied by the
analysis tool. The proposal writes the population down as a metric too, so an
experimentation metric becomes a ratio of two unit-grain metrics:

    allocated_units  = COUNT(DISTINCT unit) at unit grain
    activity         = SUM(measure)         at unit grain
    xp_metric        = activity / allocated_units

These tests pin what this branch does with that shape. The fixtures stand in
for the real tables:

    account_id      -> v3.customer.customer_id (linked to both facts)
    allocation_day  -> v3.page_views_enriched  (the allocated population)
    some_fact       -> v3.order_details        (the activity)
"""

import pytest

from tests.construction.build_v3 import assert_sql_equal

UNIT = "v3.customer.customer_id"
BY_MONTH = {"dimensions": ["v3.date.month"]}


async def create_metric(client, name, query, **declarations):
    """Create a metric and return the response."""
    return await client.post(
        "/nodes/metric/",
        json={
            "name": name,
            "description": name,
            "query": query,
            "mode": "published",
            **declarations,
        },
    )


async def build_sql(client, metric):
    """Ask for a metric's SQL, grouped by month."""
    return await client.get(
        "/sql/metrics/v3/",
        params={"metrics": [metric], **BY_MONTH},
    )


@pytest.mark.asyncio
async def test_population_metric_at_unit_grain_is_refused(client_with_build_v3):
    """The allocated population cannot be declared at unit grain.

    `COUNT(DISTINCT unit)` is one per unit at unit grain, so summing it gives
    the population -- but the shape check rejects the distinct aggregate
    before noticing that its key is the declared grain. The shape check now
    runs at write time, so the refusal arrives on creation, not on query.
    """
    client = client_with_build_v3
    created = await create_metric(
        client,
        "v3.xp_allocated_units",
        "SELECT COUNT(DISTINCT customer_id) FROM v3.page_views_enriched",
        fixed_grain=[UNIT],
        reaggregate={"fn": "sum"},
    )
    assert created.status_code == 422
    assert created.json()["message"] == (
        "Unsupported fixed_grain metric shape: a fixed grain requires one "
        "non-distinct fully-aggregatable component, because broadcasting "
        "re-applies the merge."
    )


@pytest.mark.asyncio
async def test_cross_fact_ratio_at_unit_grain_is_refused(client_with_build_v3):
    """An experimentation metric is a ratio across two facts, which is refused.

    The population comes from the allocation table and the activity from the
    fact table, so the ratio always spans two grain groups. Any non-empty
    `fixed_grain` on either side refuses the combination, which puts this
    constraint on the proposal's central shape rather than on an edge case.

    The unit dimension is requested here alongside month so the query reaches
    this refusal at all -- an unrequested `fixed_grain` dimension is refused
    on its own terms first (see `fixed_grain_test.py`'s
    `test_grain_absent_from_the_query_is_refused`).
    """
    client = client_with_build_v3
    await create_metric(
        client,
        "v3.xp_activity",
        "SELECT SUM(line_total) FROM v3.order_details",
        fixed_grain=[UNIT],
        reaggregate={"fn": "sum"},
    )
    created = await create_metric(
        client,
        "v3.xp_avg_per_allocated",
        "SELECT v3.xp_activity / v3.visitor_count",
    )
    assert created.status_code == 201

    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.xp_avg_per_allocated"],
            "dimensions": ["v3.date.month", UNIT],
        },
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Metric `v3.xp_activity` declares a non-empty `fixed_grain` and cannot "
        "be combined with metrics from another fact table in one query: the "
        "joined result groups over a `COALESCE` of the shared dimensions, so a "
        "partition naming one side's column is not grouped and the engine "
        "rejects it. Query it on its own, or use a global grain "
        "(`fixed_grain: []`), which partitions by nothing."
    )


@pytest.mark.asyncio
async def test_global_grain_ratio_builds_but_is_a_different_number(
    client_with_build_v3,
):
    """`fixed_grain: []` is the suggested escape, and answers another question.

    The refusal above points at a global grain. That builds, but `OVER ()`
    broadcasts the total across every month, so the result is all-time revenue
    divided by one month's visitors -- not revenue per allocated unit. Pinned
    here so the difference is not mistaken for a workaround.
    """
    client = client_with_build_v3
    await create_metric(
        client,
        "v3.xp_activity_global",
        "SELECT SUM(line_total) FROM v3.order_details",
        fixed_grain=[],
    )
    await create_metric(
        client,
        "v3.xp_avg_global",
        "SELECT v3.xp_activity_global / v3.visitor_count",
    )

    response = await build_sql(client, "v3.xp_avg_global")
    assert response.status_code == 200
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH
        v3_date AS (
        SELECT  date_id,
        	month 
         FROM default.v3.dates
        ),
        v3_order_details AS (
        SELECT  o.order_date,
        	oi.quantity * oi.unit_price AS line_total 
         FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        v3_page_views_enriched AS (
        SELECT  customer_id,
        	page_date 
         FROM default.v3.page_views
        ),
        order_details_0 AS (
        SELECT  t2.month,
        	SUM(t1.line_total) line_total_sum_e1f61696 
         FROM v3_order_details t1 LEFT OUTER JOIN v3_date t2 ON t1.order_date = t2.date_id 
         GROUP BY  t2.month
        ),
        page_views_enriched_0 AS (
        SELECT  t2.month,
        	t1.customer_id 
         FROM v3_page_views_enriched t1 LEFT OUTER JOIN v3_date t2 ON t1.page_date = t2.date_id 
         GROUP BY  t2.month, t1.customer_id
        ),
        base_metrics AS (
        SELECT  COALESCE(order_details_0.month, page_views_enriched_0.month) AS month,
        	COUNT( DISTINCT page_views_enriched_0.customer_id) AS visitor_count,
        	SUM(SUM(order_details_0.line_total_sum_e1f61696)) OVER ()  AS xp_activity_global 
         FROM order_details_0 FULL OUTER JOIN page_views_enriched_0 ON order_details_0.month = page_views_enriched_0.month 
         GROUP BY  1
        )

        SELECT  base_metrics.month AS month,
        	base_metrics.xp_activity_global / base_metrics.visitor_count AS xp_avg_global 
         FROM base_metrics
        """,
        normalize_aliases=True,
    )


@pytest.mark.asyncio
async def test_activity_at_unit_grain_without_the_unit_is_refused(
    client_with_build_v3,
):
    """The unit grain has to be part of the query, one way or another.

    `customer_id` is not a requested dimension here, so it is an unrequested
    `fixed_grain` dimension: refused outright, rather than silently
    generating a window that partitions by a column no CTE projects (the
    shape this used to produce, and no engine accepted).
    """
    client = client_with_build_v3
    await create_metric(
        client,
        "v3.xp_activity_private",
        "SELECT SUM(line_total) FROM v3.order_details",
        fixed_grain=[UNIT],
        reaggregate={"fn": "sum"},
    )

    response = await build_sql(client, "v3.xp_activity_private")
    assert response.status_code == 422
    assert response.json()["message"] == (
        f"Metric `v3.xp_activity_private` declares `fixed_grain` on `{UNIT}`, "
        "which is not a requested output dimension. Add it to the query's "
        "dimensions before requesting this metric."
    )
