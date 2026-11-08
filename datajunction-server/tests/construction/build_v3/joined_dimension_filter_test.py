"""
Filters on an allocation dimension reached from an event fact apply only
on the allocation side of the join.
"""

import pytest
import pytest_asyncio

from tests.construction.build_v3 import assert_sql_equal, get_first_grain_group


async def post(client, path, body):
    response = await client.post(path, json=body)
    assert response.status_code in (200, 201), response.text


@pytest_asyncio.fixture
async def allocation_client(client_with_build_v3):
    """Events by account, linked to an allocation dimension and to v3.date."""
    client = client_with_build_v3
    await post(
        client,
        "/nodes/source/",
        {
            "name": "v3.al_events",
            "description": "d",
            "catalog": "default",
            "schema_": "v3",
            "table": "al_events",
            "mode": "published",
            "columns": [
                {"name": "event_id", "type": "int"},
                {"name": "account_id", "type": "int"},
                {"name": "view_secs", "type": "int"},
                {"name": "event_date", "type": "int"},
            ],
        },
    )
    await post(
        client,
        "/nodes/source/",
        {
            "name": "v3.al_allocations",
            "description": "d",
            "catalog": "default",
            "schema_": "v3",
            "table": "al_allocations",
            "mode": "published",
            "columns": [
                {"name": "account_id", "type": "int"},
                {"name": "cell", "type": "string"},
                {"name": "allocation_date", "type": "int"},
            ],
        },
    )
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "v3.al_allocation",
            "description": "d",
            "query": "SELECT account_id, cell, allocation_date FROM v3.al_allocations",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.al_fact",
            "description": "d",
            "query": "SELECT event_id, account_id, view_secs, event_date FROM v3.al_events",
            "mode": "published",
            "primary_key": ["event_id"],
        },
    )
    await post(
        client,
        "/nodes/v3.al_fact/link",
        {
            "dimension_node": "v3.al_allocation",
            "join_type": "left",
            "join_on": "v3.al_fact.account_id = v3.al_allocation.account_id",
        },
    )
    await post(
        client,
        "/nodes/v3.al_fact/link",
        {
            "dimension_node": "v3.date",
            "join_type": "left",
            "join_on": "v3.al_fact.event_date = v3.date.date_id",
            "role": "event",
        },
    )
    await post(
        client,
        "/nodes/v3.al_allocation/link",
        {
            "dimension_node": "v3.date",
            "join_type": "left",
            "join_on": "v3.al_allocation.allocation_date = v3.date.date_id",
            "role": "allocation",
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.al_view_secs",
            "description": "d",
            "query": "SELECT SUM(view_secs) FROM v3.al_fact",
        },
    )
    return client


async def measures_sql(client, filters):
    response = await client.get(
        "/sql/measures/v3/",
        params={"metrics": ["v3.al_view_secs"], "filters": filters},
    )
    assert response.status_code == 200, response.text
    return get_first_grain_group(response.json())["sql"]


@pytest.mark.asyncio
async def test_allocation_attribute_filter_stays_on_allocation_side(
    allocation_client,
):
    """The event CTE is untouched; the filter is on the allocation CTE and join."""
    assert_sql_equal(
        await measures_sql(allocation_client, ["v3.al_allocation.cell = 'T1'"]),
        """
        WITH v3_al_allocation AS (
          SELECT account_id, cell
          FROM default.v3.al_allocations
          WHERE cell = 'T1'
        ),
        v3_al_fact AS (
          SELECT account_id, view_secs
          FROM default.v3.al_events
        )
        SELECT SUM(t1.view_secs) view_secs_sum_c7895340
        FROM v3_al_fact t1
        LEFT OUTER JOIN v3_al_allocation t2 ON t1.account_id = t2.account_id
        WHERE t2.cell = 'T1'
        """,
    )


@pytest.mark.asyncio
async def test_allocation_role_date_filter_stays_on_allocation_side(
    allocation_client,
):
    """A date filter on the allocation role uses the allocation date only."""
    assert_sql_equal(
        await measures_sql(
            allocation_client,
            ["v3.date.date_id[allocation] BETWEEN 1 AND 5"],
        ),
        """
        WITH v3_al_allocation AS (
          SELECT account_id, allocation_date
          FROM default.v3.al_allocations
          WHERE allocation_date BETWEEN 1 AND 5
        ),
        v3_al_fact AS (
          SELECT account_id, view_secs
          FROM default.v3.al_events
        )
        SELECT SUM(t1.view_secs) view_secs_sum_c7895340
        FROM v3_al_fact t1
        LEFT OUTER JOIN v3_al_allocation t2 ON t1.account_id = t2.account_id
        WHERE t2.allocation_date BETWEEN 1 AND 5
        """,
    )


@pytest.mark.asyncio
async def test_allocation_and_event_filters_each_apply_to_their_side(
    allocation_client,
):
    """Allocation filters go to the allocation side and the event date to the events."""
    assert_sql_equal(
        await measures_sql(
            allocation_client,
            [
                "v3.al_allocation.cell = 'T1'",
                "v3.date.date_id[allocation] BETWEEN 1 AND 5",
                "v3.date.date_id[event] BETWEEN 3 AND 9",
            ],
        ),
        """
        WITH v3_al_allocation AS (
          SELECT account_id, cell, allocation_date
          FROM default.v3.al_allocations
          WHERE cell = 'T1' AND allocation_date BETWEEN 1 AND 5
        ),
        v3_al_fact AS (
          SELECT account_id, view_secs
          FROM default.v3.al_events
          WHERE event_date BETWEEN 3 AND 9
        )
        SELECT SUM(t1.view_secs) view_secs_sum_c7895340
        FROM v3_al_fact t1
        LEFT OUTER JOIN v3_al_allocation t2 ON t1.account_id = t2.account_id
        WHERE t2.cell = 'T1' AND t2.allocation_date BETWEEN 1 AND 5
        """,
    )


@pytest.mark.asyncio
async def test_metrics_sql_keeps_the_same_sides(allocation_client):
    response = await allocation_client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.al_view_secs"],
            "filters": [
                "v3.al_allocation.cell = 'T1'",
                "v3.date.date_id[event] BETWEEN 3 AND 9",
            ],
        },
    )
    assert response.status_code == 200, response.text
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH v3_al_allocation AS (
          SELECT account_id, cell
          FROM default.v3.al_allocations
          WHERE cell = 'T1'
        ),
        v3_al_fact AS (
          SELECT account_id, view_secs
          FROM default.v3.al_events
          WHERE event_date BETWEEN 3 AND 9
        ),
        al_fact_0 AS (
          SELECT SUM(t1.view_secs) view_secs_sum_c7895340
          FROM v3_al_fact t1
          LEFT OUTER JOIN v3_al_allocation t2 ON t1.account_id = t2.account_id
          WHERE t2.cell = 'T1'
        )
        SELECT SUM(al_fact_0.view_secs_sum_c7895340) AS al_view_secs
        FROM al_fact_0
        """,
    )
