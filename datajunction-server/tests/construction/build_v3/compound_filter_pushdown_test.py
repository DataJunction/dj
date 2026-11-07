"""
A compound filter pushes down only the AND conditions that fully resolve on
the upstream node; everything else stays in the outer query.
"""

import pytest

from datajunction_server.construction.build_v3.dimensions import (
    _split_pushable_conjuncts,
)
from tests.construction.build_v3 import assert_sql_equal


async def post(client, path, body):
    resp = await client.post(path, json=body)
    assert resp.status_code in (200, 201), resp.json()


async def build_hidden_key_fact(client):
    """`v3.hk_fact` reads `v3.hk_stage`, which reads a source linking `v3.date`;
    the date column is not projected by `v3.hk_fact`."""
    await post(
        client,
        "/nodes/source/",
        {
            "name": "v3.hk_source",
            "columns": [
                {"name": "account_id", "type": "int"},
                {"name": "event_date", "type": "int"},
                {"name": "amount", "type": "double"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "hk_source",
        },
    )
    await post(
        client,
        "/nodes/v3.hk_source/link/",
        {
            "dimension_node": "v3.date",
            "join_type": "left",
            "join_on": "v3.hk_source.event_date = v3.date.date_id",
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.hk_stage",
            "query": "SELECT account_id, amount FROM v3.hk_source",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.hk_fact",
            "query": "SELECT account_id, amount, amount * 2 AS total FROM v3.hk_stage",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.hk_amount",
            "query": "SELECT SUM(amount) FROM v3.hk_fact",
            "mode": "published",
        },
    )


async def get_response(client, filter_str):
    return await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.hk_amount"],
            "filters": [filter_str],
            "dialect": "trino",
        },
    )


@pytest.mark.asyncio
async def test_and_pushes_only_the_resolvable_condition(client_with_build_v3):
    await build_hidden_key_fact(client_with_build_v3)
    response = await get_response(
        client_with_build_v3,
        "v3.date.date_id = 20240101 AND v3.hk_fact.total > 1",
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH
        v3_hk_stage AS (
          SELECT amount
          FROM default.v3.hk_source
          WHERE hk_source.event_date = 20240101
        ),
        v3_hk_fact AS (
          SELECT amount, amount * 2 AS total FROM v3_hk_stage
        ),
        hk_fact_0 AS (
          SELECT SUM(t1.amount) amount_sum_HASH
          FROM v3_hk_fact t1
          WHERE t1.total > 1
        )
        SELECT SUM(hk_fact_0.amount_sum_HASH) AS hk_amount FROM hk_fact_0
        """,
        normalize_aliases=True,
    )


@pytest.mark.asyncio
async def test_and_pushes_every_condition_on_the_pushed_dimension(
    client_with_build_v3,
):
    await build_hidden_key_fact(client_with_build_v3)
    response = await get_response(
        client_with_build_v3,
        "v3.date.date_id >= 20240101 AND v3.date.date_id < 20240201 "
        "AND v3.hk_fact.total > 1",
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH
        v3_hk_stage AS (
          SELECT amount
          FROM default.v3.hk_source
          WHERE hk_source.event_date >= 20240101
            AND hk_source.event_date < 20240201
        ),
        v3_hk_fact AS (
          SELECT amount, amount * 2 AS total FROM v3_hk_stage
        ),
        hk_fact_0 AS (
          SELECT SUM(t1.amount) amount_sum_HASH
          FROM v3_hk_fact t1
          WHERE t1.total > 1
        )
        SELECT SUM(hk_fact_0.amount_sum_HASH) AS hk_amount FROM hk_fact_0
        """,
        normalize_aliases=True,
    )


@pytest.mark.asyncio
async def test_or_across_nodes_is_not_pushed_upstream(client_with_build_v3):
    await build_hidden_key_fact(client_with_build_v3)
    response = await get_response(
        client_with_build_v3,
        "v3.date.date_id = 20240101 OR v3.hk_fact.total > 1",
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Cannot find join path from v3.hk_fact to dimension v3.date. "
        "Please create a dimension link between these nodes."
    )


@pytest.mark.parametrize(
    ("filter_str", "expected"),
    [
        (
            "v3.d.c[sold] > 1 AND v3.d.c[sold] < 5",
            ("v3.d.c[sold] > 1 AND v3.d.c[sold] < 5", None),
        ),
        ("v3.d.c[a->b] > 1 AND v3.x.t > 5", ("v3.d.c[a->b] > 1", "v3.x.t > 5")),
        ("v3.d.c = 1 OR v3.x.t > 5", (None, None)),
        ("v3.d.c = v3.x.t AND v3.d.c > 2", ("v3.d.c > 2", "v3.d.c = v3.x.t")),
    ],
)
def test_split_pushable_conjuncts(filter_str, expected):
    assert _split_pushable_conjuncts(filter_str, "v3.d.c") == expected
