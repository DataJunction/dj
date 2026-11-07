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


async def build_linked_stages(client):
    """`outer_stage` reads `inner_stage`, and both link `v3.date`."""
    await post(
        client,
        "/nodes/source/",
        {
            "name": "v3.src_inner",
            "columns": [
                {"name": "account_id", "type": "int"},
                {"name": "inner_date", "type": "int"},
                {"name": "amount", "type": "double"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "source_inner",
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.inner_stage",
            "query": "SELECT account_id, inner_date, amount FROM v3.src_inner",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.outer_stage",
            "query": "SELECT account_id, inner_date AS outer_date, amount "
            "FROM v3.inner_stage",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    for node, column in (("inner_stage", "inner_date"), ("outer_stage", "outer_date")):
        await post(
            client,
            f"/nodes/v3.{node}/link/",
            {
                "dimension_node": "v3.date",
                "join_type": "left",
                "join_on": f"v3.{node}.{column} = v3.date.date_id",
            },
        )


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    reason="the old pushdown filters a column the outer stage does not expose",
)
async def test_filter_reaches_a_linked_node_under_another_linked_node(
    client_with_build_v3,
):
    client = client_with_build_v3
    await build_linked_stages(client)
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.nested_fact",
            "query": "SELECT account_id, amount FROM v3.outer_stage",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.nested_amount",
            "query": "SELECT SUM(amount) FROM v3.nested_fact",
            "mode": "published",
        },
    )
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.nested_amount"],
            "filters": ["v3.date.date_id = 20261104"],
            "dialect": "trino",
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH
        v3_inner_stage AS (
          SELECT inner_date, amount FROM default.v3.source_inner
        ),
        v3_outer_stage AS (
          SELECT inner_date AS outer_date, amount
          FROM v3_inner_stage inner_stage
          WHERE inner_stage.inner_date = 20261104
        ),
        v3_nested_fact AS (
          SELECT amount
          FROM v3_outer_stage outer_stage
          WHERE outer_stage.outer_date = 20261104
        ),
        nested_fact_0 AS (
          SELECT SUM(t1.amount) amount_sum_HASH FROM v3_nested_fact t1
        )
        SELECT SUM(nested_fact_0.amount_sum_HASH) AS nested_amount FROM nested_fact_0
        """,
        normalize_aliases=True,
    )


@pytest.mark.asyncio
async def test_filter_reaches_a_linked_node_read_by_a_linking_fact(
    client_with_build_v3,
):
    """The fact links the dimension itself and reads a node that links it too."""
    client = client_with_build_v3
    await build_linked_stages(client)
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.outer_amount",
            "query": "SELECT SUM(amount) FROM v3.outer_stage",
            "mode": "published",
        },
    )
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.outer_amount"],
            "filters": ["v3.date.date_id = 20261104"],
            "dialect": "trino",
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH
        v3_inner_stage AS (
          SELECT inner_date, amount
          FROM default.v3.source_inner
          WHERE inner_date = 20261104
        ),
        v3_outer_stage AS (
          SELECT amount
          FROM v3_inner_stage
          WHERE inner_date = 20261104
        ),
        outer_stage_0 AS (
          SELECT SUM(t1.amount) amount_sum_HASH
          FROM v3_outer_stage t1
        )
        SELECT SUM(outer_stage_0.amount_sum_HASH) outer_amount
        FROM outer_stage_0
        """,
        normalize_aliases=True,
    )


async def build_nested_fact(client):
    await build_linked_stages(client)
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.nested_fact",
            "query": "SELECT account_id, amount FROM v3.outer_stage",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.nested_amount",
            "query": "SELECT SUM(amount) FROM v3.nested_fact",
            "mode": "published",
        },
    )


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    reason="the old pushdown sends an unresolved reference into the upstream node",
)
async def test_compound_filter_uses_the_nearest_linked_node(client_with_build_v3):
    client = client_with_build_v3
    await build_nested_fact(client)
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.nested_amount"],
            "filters": ["v3.date.date_id = 20261104 OR v3.nested_fact.amount > 5"],
            "dialect": "trino",
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH
        v3_inner_stage AS (
          SELECT inner_date, amount FROM default.v3.source_inner
        ),
        v3_outer_stage AS (
          SELECT inner_date AS outer_date, amount FROM v3_inner_stage
        ),
        v3_nested_fact AS (
          SELECT amount
          FROM v3_outer_stage outer_stage
          WHERE outer_stage.outer_date = 20261104 OR amount > 5
        ),
        nested_fact_0 AS (
          SELECT SUM(t1.amount) amount_sum_HASH FROM v3_nested_fact t1
        )
        SELECT SUM(nested_fact_0.amount_sum_HASH) AS nested_amount FROM nested_fact_0
        """,
        normalize_aliases=True,
    )


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    reason="the old pushdown repeats the predicate on a shared definition",
)
async def test_shared_definition_is_filtered_once(client_with_build_v3):
    """`double_stage` reads `inner_stage` twice, and `inner_stage` reads a
    linked source: that source scan gets one predicate, not two."""
    client = client_with_build_v3
    await build_linked_stages(client)
    await post(
        client,
        "/nodes/v3.src_inner/link/",
        {
            "dimension_node": "v3.date",
            "join_type": "left",
            "join_on": "v3.src_inner.inner_date = v3.date.date_id",
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.double_stage",
            "query": "SELECT a.account_id, a.inner_date AS double_date, a.amount "
            "FROM v3.inner_stage a JOIN v3.inner_stage b ON a.account_id = b.account_id",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/v3.double_stage/link/",
        {
            "dimension_node": "v3.date",
            "join_type": "left",
            "join_on": "v3.double_stage.double_date = v3.date.date_id",
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.double_fact",
            "query": "SELECT account_id, amount FROM v3.double_stage",
            "mode": "published",
            "primary_key": ["account_id"],
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.double_amount",
            "query": "SELECT SUM(amount) FROM v3.double_fact",
            "mode": "published",
        },
    )
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.double_amount"],
            "filters": ["v3.date.date_id = 20261104"],
            "dialect": "trino",
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH
        v3_inner_stage AS (
          SELECT account_id, inner_date, amount
          FROM default.v3.source_inner src_inner
          WHERE src_inner.inner_date = 20261104
        ),
        v3_double_stage AS (
          SELECT a.inner_date AS double_date, a.amount
          FROM v3_inner_stage a
          JOIN v3_inner_stage b ON a.account_id = b.account_id
          WHERE a.inner_date = 20261104 AND b.inner_date = 20261104
        ),
        v3_double_fact AS (
          SELECT amount
          FROM v3_double_stage double_stage
          WHERE double_stage.double_date = 20261104
        ),
        double_fact_0 AS (
          SELECT SUM(t1.amount) amount_sum_HASH FROM v3_double_fact t1
        )
        SELECT SUM(double_fact_0.amount_sum_HASH) AS double_amount FROM double_fact_0
        """,
        normalize_aliases=True,
    )


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    reason="the old pushdown filters inside a left-joined input, which unmatches rows",
)
async def test_null_accepting_filter_stays_out_of_a_left_joined_node(
    client_with_build_v3,
):
    """`IS NULL` inside the left-joined input would unmatch rows that have a
    value, so only the fact's own column is filtered."""
    client = client_with_build_v3
    await post(
        client,
        "/nodes/source/",
        {
            "name": "v3.src_shoppers",
            "columns": [
                {"name": "customer_id", "type": "int"},
                {"name": "registration_date", "type": "int"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "shoppers",
        },
    )
    await post(
        client,
        "/nodes/v3.src_shoppers/link/",
        {
            "dimension_node": "v3.date",
            "join_type": "left",
            "join_on": "v3.src_shoppers.registration_date = v3.date.date_id",
        },
    )
    await post(
        client,
        "/nodes/source/",
        {
            "name": "v3.src_purchases",
            "columns": [
                {"name": "sale_id", "type": "int"},
                {"name": "customer_id", "type": "int"},
                {"name": "amount", "type": "double"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "purchases",
        },
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.purchase_fact",
            "query": "SELECT s.sale_id, s.amount, c.registration_date "
            "FROM v3.src_purchases s LEFT JOIN "
            "(SELECT customer_id, registration_date FROM v3.src_shoppers) c "
            "ON s.customer_id = c.customer_id",
            "mode": "published",
            "primary_key": ["sale_id"],
        },
    )
    await post(
        client,
        "/nodes/v3.purchase_fact/link/",
        {
            "dimension_node": "v3.date",
            "join_type": "left",
            "join_on": "v3.purchase_fact.registration_date = v3.date.date_id",
        },
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.purchase_amount",
            "query": "SELECT SUM(amount) FROM v3.purchase_fact",
            "mode": "published",
        },
    )
    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.purchase_amount"],
            "dimensions": ["v3.purchase_fact.sale_id"],
            "filters": ["v3.date.date_id IS NULL"],
        },
    )
    assert response.status_code == 200, response.json()
    assert_sql_equal(
        response.json()["grain_groups"][0]["sql"],
        """
        WITH v3_purchase_fact AS (
          SELECT s.sale_id, s.amount
          FROM default.v3.purchases s
          LEFT JOIN (
            SELECT customer_id, registration_date FROM default.v3.shoppers
          ) c ON s.customer_id = c.customer_id
          WHERE c.registration_date IS NULL
        )
        SELECT t1.sale_id, SUM(t1.amount) amount_sum_HASH
        FROM v3_purchase_fact t1
        GROUP BY t1.sale_id
        """,
        normalize_aliases=True,
    )
