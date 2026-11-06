"""
A date filter on one role is still pushed into the fact's scan when another
filter uses a different role of the same date dimension, reached through a
RIGHT OUTER JOIN to a dimension that links to the date dimension itself.

Pushdown refused every filter on `v3.date.date_id` once any role of it was
joined: the guard against binding a joined dimension's attribute to a
same-named foreign-key column was keyed on the role-less name, so it blocked
`date_id[measure]` (a column the fact scan owns) along with the joined
`date_id[allocation_snapshot]`. The fact was then scanned unbounded.
"""

import pytest

from tests.construction.build_v3 import assert_sql_equal, get_first_grain_group

MEASURE_FILTER = "v3.date.date_id[measure] BETWEEN 20260923 AND 20261009"
SNAPSHOT_FILTER = "v3.date.date_id[allocation_snapshot] = 20261025"


async def build_touch_events_with_allocation_days(client):
    """`touch_events` links the date as `measure` and RIGHT JOINs `alloc_day`,
    which links the same date dimension as `allocation_snapshot`."""
    for name, columns, table in [
        (
            "v3.src_touch_events",
            [
                {"name": "account_id", "type": "int"},
                {"name": "event_date", "type": "int"},
                {"name": "amount", "type": "double"},
            ],
            "touch_events",
        ),
        (
            "v3.src_alloc_days",
            [
                {"name": "account_id", "type": "int"},
                {"name": "snapshot_date", "type": "int"},
            ],
            "alloc_days",
        ),
    ]:
        resp = await client.post(
            "/nodes/source/",
            json={
                "name": name,
                "columns": columns,
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": table,
            },
        )
        assert resp.status_code in (200, 201), resp.json()
    resp = await client.post(
        "/nodes/transform/",
        json={
            "name": "v3.touch_events",
            "query": "SELECT account_id, event_date, amount FROM v3.src_touch_events",
            "mode": "published",
            "primary_key": ["account_id", "event_date"],
        },
    )
    assert resp.status_code == 201, resp.json()
    resp = await client.post(
        "/nodes/dimension/",
        json={
            "name": "v3.alloc_day",
            "query": "SELECT account_id, snapshot_date FROM v3.src_alloc_days",
            "mode": "published",
            "primary_key": ["account_id", "snapshot_date"],
        },
    )
    assert resp.status_code == 201, resp.json()
    for node, link in [
        (
            "v3.touch_events",
            {
                "dimension_node": "v3.date",
                "join_type": "left",
                "join_on": "v3.touch_events.event_date = v3.date.date_id",
                "role": "measure",
            },
        ),
        (
            "v3.alloc_day",
            {
                "dimension_node": "v3.date",
                "join_type": "left",
                "join_on": "v3.alloc_day.snapshot_date = v3.date.date_id",
                "role": "allocation_snapshot",
            },
        ),
        (
            "v3.touch_events",
            {
                "dimension_node": "v3.alloc_day",
                "join_type": "right",
                "join_on": "v3.touch_events.account_id = v3.alloc_day.account_id",
            },
        ),
    ]:
        resp = await client.post(f"/nodes/{node}/link/", json=link)
        assert resp.status_code in (200, 201), resp.json()
    resp = await client.post(
        "/nodes/metric/",
        json={
            "name": "v3.touch_amount",
            "query": "SELECT SUM(amount) FROM v3.touch_events",
            "mode": "published",
        },
    )
    assert resp.status_code == 201, resp.json()


async def measures_sql(client, filters: list[str]) -> str:
    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.touch_amount"],
            "dimensions": ["v3.date.date_id[measure]"],
            "filters": filters,
        },
    )
    assert response.status_code == 200, response.json()
    return get_first_grain_group(response.json())["sql"]


@pytest.mark.asyncio
async def test_measure_filter_alone_is_pushed_into_the_fact_scan(
    client_with_build_v3,
):
    await build_touch_events_with_allocation_days(client_with_build_v3)
    assert_sql_equal(
        await measures_sql(client_with_build_v3, [MEASURE_FILTER]),
        """
        WITH v3_touch_events AS (
          SELECT event_date, amount
          FROM default.v3.touch_events
          WHERE event_date BETWEEN 20260923 AND 20261009
        )
        SELECT t1.event_date date_id_measure,
          SUM(t1.amount) amount_sum_HASH
        FROM v3_touch_events t1
        GROUP BY t1.event_date
        """,
        normalize_aliases=True,
    )


@pytest.mark.asyncio
async def test_measure_filter_is_still_pushed_when_another_role_is_joined(
    client_with_build_v3,
):
    await build_touch_events_with_allocation_days(client_with_build_v3)
    assert_sql_equal(
        await measures_sql(
            client_with_build_v3,
            [MEASURE_FILTER, SNAPSHOT_FILTER],
        ),
        """
        WITH v3_alloc_day AS (
          SELECT account_id, snapshot_date
          FROM default.v3.alloc_days
          WHERE snapshot_date = 20261025
        ),
        v3_touch_events AS (
          SELECT account_id, event_date, amount
          FROM default.v3.touch_events
          WHERE event_date BETWEEN 20260923 AND 20261009
        )
        SELECT t1.event_date date_id_measure,
          SUM(t1.amount) amount_sum_HASH
        FROM v3_touch_events t1
        RIGHT OUTER JOIN v3_alloc_day t2 ON t1.account_id = t2.account_id
        WHERE t2.snapshot_date = 20261025
        GROUP BY t1.event_date
        """,
        normalize_aliases=True,
    )


@pytest.mark.asyncio
async def test_role_less_label_filter_stays_blocked_beside_a_roled_one(
    client_with_build_v3,
):
    """A label filter whose name matches the fact's int foreign key must not be
    pushed onto that key, whether or not the same dimension is also requested
    under a role."""
    client = client_with_build_v3
    resp = await client.post(
        "/nodes/source/",
        json={
            "name": "v3.src_bot_events",
            "columns": [
                {"name": "account_id", "type": "int"},
                {"name": "is_bot", "type": "int"},
                {"name": "amount", "type": "double"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "bot_events",
        },
    )
    assert resp.status_code in (200, 201), resp.json()
    resp = await client.post(
        "/nodes/transform/",
        json={
            "name": "v3.bot_events",
            "query": "SELECT account_id, is_bot, amount FROM v3.src_bot_events",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    assert resp.status_code == 201, resp.json()
    resp = await client.post(
        "/nodes/dimension/",
        json={
            "name": "v3.bot_label",
            "query": (
                "SELECT t.is_bot_key, t.is_bot "
                "FROM (SELECT 0 AS is_bot_key, 'false' AS is_bot "
                "UNION ALL SELECT 1, 'true') t"
            ),
            "mode": "published",
            "primary_key": ["is_bot_key"],
        },
    )
    assert resp.status_code == 201, resp.json()
    for link in [
        {"role": None},
        {"role": "other"},
    ]:
        resp = await client.post(
            "/nodes/v3.bot_events/link/",
            json={
                "dimension_node": "v3.bot_label",
                "join_type": "left",
                "join_on": "v3.bot_events.is_bot = v3.bot_label.is_bot_key",
                **({"role": link["role"]} if link["role"] else {}),
            },
        )
        assert resp.status_code in (200, 201), resp.json()
    resp = await client.post(
        "/nodes/metric/",
        json={
            "name": "v3.bot_amount",
            "query": "SELECT SUM(amount) FROM v3.bot_events",
            "mode": "published",
        },
    )
    assert resp.status_code == 201, resp.json()

    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.bot_amount"],
            "dimensions": ["v3.bot_label.is_bot", "v3.bot_label.is_bot[other]"],
            "filters": [
                "v3.bot_label.is_bot = 'false'",
                "v3.bot_label.is_bot[other] = 'false'",
            ],
        },
    )
    assert response.status_code == 200, response.json()
    sql = get_first_grain_group(response.json())["sql"]
    print("\n#### SQL\n", sql)
    events_cte = sql.split("v3_bot_events AS (")[1].split(")")[0]
    assert "'false'" not in events_cte
