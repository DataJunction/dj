"""
Filters on a role that only an upstream node links push down on the plain
column of the link with that role.
"""

import pytest
import pytest_asyncio

from tests.construction.build_v3 import assert_sql_equal, get_first_grain_group


async def post(client, path, body, params=None):
    response = await client.post(path, json=body, params=params)
    assert response.status_code in (200, 201), response.text


@pytest_asyncio.fixture
async def pushdown_client(client_with_build_v3):
    """An event source linking v3.date as `measure` and `snapshot`."""
    client = client_with_build_v3
    await post(
        client,
        "/nodes/source/",
        {
            "name": "v3.pd_events",
            "description": "d",
            "catalog": "default",
            "schema_": "v3",
            "table": "pd_events",
            "mode": "published",
            "columns": [
                {"name": "event_id", "type": "int"},
                {"name": "amount", "type": "int"},
                {"name": "event_date", "type": "int"},
                {"name": "snapshot_date", "type": "int"},
            ],
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.pd_fact",
            "description": "d",
            "query": "SELECT event_id, amount FROM v3.pd_events",
            "mode": "published",
            "primary_key": ["event_id"],
        },
    )
    for role, column in [("measure", "event_date"), ("snapshot", "snapshot_date")]:
        await post(
            client,
            "/nodes/v3.pd_events/link",
            {
                "dimension_node": "v3.date",
                "join_type": "left",
                "join_on": f"v3.pd_events.{column} = v3.date.date_id",
                "role": role,
            },
        )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.pd_total",
            "description": "d",
            "query": "SELECT SUM(amount) FROM v3.pd_fact",
        },
    )
    return client


async def build(client, endpoint, filters):
    return await client.get(
        f"/sql/{endpoint}/v3/",
        params={"metrics": ["v3.pd_total"], "filters": filters},
    )


@pytest.mark.asyncio
async def test_measures_each_role_filters_its_own_column(pushdown_client):
    """Each role binds to its link's column, with no `[role]` left in the SQL."""
    response = await build(
        pushdown_client,
        "measures",
        [
            "v3.date.date_id[measure] BETWEEN 1 AND 5",
            "v3.date.date_id[snapshot] = 9",
        ],
    )
    assert response.status_code == 200, response.text
    assert_sql_equal(
        get_first_grain_group(response.json())["sql"],
        """
        WITH v3_pd_fact AS (
          SELECT amount
          FROM default.v3.pd_events
          WHERE pd_events.event_date BETWEEN 1 AND 5
            AND pd_events.snapshot_date = 9
        )
        SELECT SUM(t1.amount) amount_sum_72f59fda
        FROM v3_pd_fact t1
        """,
    )


@pytest.mark.asyncio
async def test_metrics_each_role_filters_its_own_column(pushdown_client):
    response = await build(
        pushdown_client,
        "metrics",
        [
            "v3.date.date_id[measure] BETWEEN 1 AND 5",
            "v3.date.date_id[snapshot] = 9",
        ],
    )
    assert response.status_code == 200, response.text
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH v3_pd_fact AS (
          SELECT amount
          FROM default.v3.pd_events
          WHERE pd_events.event_date BETWEEN 1 AND 5
            AND pd_events.snapshot_date = 9
        ),
        pd_fact_0 AS (
          SELECT SUM(t1.amount) amount_sum_72f59fda
          FROM v3_pd_fact t1
        )
        SELECT SUM(pd_fact_0.amount_sum_72f59fda) AS pd_total
        FROM pd_fact_0
        """,
    )


@pytest.mark.asyncio
async def test_one_role_filter_leaves_the_other_column_alone(pushdown_client):
    response = await build(
        pushdown_client,
        "measures",
        ["v3.date.date_id[snapshot] = 9"],
    )
    assert response.status_code == 200, response.text
    assert_sql_equal(
        get_first_grain_group(response.json())["sql"],
        """
        WITH v3_pd_fact AS (
          SELECT amount
          FROM default.v3.pd_events
          WHERE pd_events.snapshot_date = 9
        )
        SELECT SUM(t1.amount) amount_sum_72f59fda
        FROM v3_pd_fact t1
        """,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["measures", "metrics"])
async def test_role_no_upstream_link_has_is_rejected(pushdown_client, endpoint):
    """A role no upstream link has is not bound to the links that exist."""
    response = await build(
        pushdown_client,
        endpoint,
        ["v3.date.date_id[nope] = 9"],
    )
    assert response.status_code == 422, response.text
    assert response.json()["message"] == (
        "Cannot find join path from v3.pd_fact to dimension v3.date. "
        "Please create a dimension link between these nodes."
    )


@pytest.mark.asyncio
async def test_metric_expression_joins_dimension_linked_only_by_role(
    client_with_build_v3,
):
    """A dimension a metric reads, linked only under a role, joins through it."""
    client = client_with_build_v3
    await post(
        client,
        "/nodes/source/",
        {
            "name": "v3.re_events",
            "description": "d",
            "catalog": "default",
            "schema_": "v3",
            "table": "re_events",
            "mode": "published",
            "columns": [
                {"name": "event_id", "type": "int"},
                {"name": "amount", "type": "int"},
                {"name": "event_date", "type": "int"},
            ],
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.re_fact",
            "description": "d",
            "query": "SELECT event_id, amount, event_date FROM v3.re_events",
            "mode": "published",
            "primary_key": ["event_id"],
        },
    )
    await post(
        client,
        "/nodes/v3.re_fact/link",
        {
            "dimension_node": "v3.date",
            "join_type": "left",
            "join_on": "v3.re_fact.event_date = v3.date.date_id",
            "role": "measure",
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.re_late_total",
            "description": "d",
            "query": (
                "SELECT SUM(CASE WHEN v3.date.week >= 5 THEN amount ELSE 0 END) "
                "FROM v3.re_fact"
            ),
        },
    )
    response = await client.get(
        "/sql/measures/v3/",
        params={"metrics": ["v3.re_late_total"], "dimensions": []},
    )
    assert response.status_code == 200, response.text
    assert_sql_equal(
        get_first_grain_group(response.json())["sql"],
        """
        WITH v3_date AS (
          SELECT date_id, week
          FROM default.v3.dates
        ),
        v3_re_fact AS (
          SELECT amount, event_date
          FROM default.v3.re_events
        )
        SELECT SUM(CASE WHEN t2.week >= 5 THEN t1.amount ELSE 0 END)
          v3_DOT_date_DOT_week_amount_sum_decd5461
        FROM v3_re_fact t1
        LEFT OUTER JOIN v3_date t2 ON t1.event_date = t2.date_id
        """,
    )
