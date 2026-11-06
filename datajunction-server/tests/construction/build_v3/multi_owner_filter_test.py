"""
A filter on a dimension linked by several upstream nodes applies to each one.
"""

import pytest

from tests.construction.build_v3 import assert_sql_equal


async def post(client, path, body):
    resp = await client.post(path, json=body)
    assert resp.status_code in (200, 201), resp.json()


async def build_fact_over_two_linked_sources(client):
    """`fact` joins `stage_a` and `stage_b`; each reads a source that links
    `v3.date`, and `fact` does not project either date column."""
    for suffix in ("a", "b"):
        await post(
            client,
            "/nodes/source/",
            {
                "name": f"v3.src_{suffix}",
                "columns": [
                    {"name": "account_id", "type": "int"},
                    {"name": "snapshot_date", "type": "int"},
                    {"name": "amount", "type": "double"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": f"source_{suffix}",
            },
        )
        await post(
            client,
            f"/nodes/v3.src_{suffix}/link/",
            {
                "dimension_node": "v3.date",
                "join_type": "left",
                "join_on": f"v3.src_{suffix}.snapshot_date = v3.date.date_id",
            },
        )
        await post(
            client,
            "/nodes/transform/",
            {
                "name": f"v3.stage_{suffix}",
                "query": f"SELECT account_id, amount FROM v3.src_{suffix}",
                "mode": "published",
                "primary_key": ["account_id"],
            },
        )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.fact",
            "query": "SELECT a.account_id, a.amount FROM v3.stage_a a "
            "JOIN v3.stage_b b ON a.account_id = b.account_id",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.fact_amount",
            "query": "SELECT SUM(amount) FROM v3.fact",
            "mode": "published",
        },
    )


@pytest.mark.asyncio
async def test_filter_applies_to_every_upstream_node_linking_the_dimension(
    client_with_build_v3,
):
    await build_fact_over_two_linked_sources(client_with_build_v3)
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.fact_amount"],
            "filters": ["v3.date.date_id = 20261104"],
            "dialect": "trino",
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH
        v3_stage_a AS (
          SELECT account_id, amount
          FROM default.v3.source_a src_a
          WHERE src_a.snapshot_date = 20261104
        ),
        v3_stage_b AS (
          SELECT account_id
          FROM default.v3.source_b src_b
          WHERE src_b.snapshot_date = 20261104
        ),
        v3_fact AS (
          SELECT a.amount
          FROM v3_stage_a a
          JOIN v3_stage_b b ON a.account_id = b.account_id
        ),
        fact_0 AS (
          SELECT SUM(t1.amount) amount_sum_HASH FROM v3_fact t1
        )
        SELECT SUM(fact_0.amount_sum_HASH) AS fact_amount FROM fact_0
        """,
        normalize_aliases=True,
    )
