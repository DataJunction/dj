"""
Tests for ``build_v3.node_query.build_node_sql_v3``.

Covers /data/{node} and /sql/{node} for non-metric / non-cube nodes —
specifically the cases that the v2 ``QueryBuilder`` mishandles, plus the
standard dim-with-parents and source paths. The v3 path emits the node's
compiled query directly (no pointless ``WITH starting AS (...) SELECT *
FROM starting`` wrapper); upstream non-source parents become CTEs only when
they actually exist and need defining.
"""

import pytest
from httpx import AsyncClient

from . import assert_sql_equal


@pytest.mark.asyncio
async def test_sql_for_parent_less_dimension_node(
    client_with_roads: AsyncClient,
):
    """
    A dim node whose query has no real upstream tables (the ``xp.measure_hour``
    shape) renders as just the node's own SELECT plus the requested LIMIT —
    no CTE wrapper. Reproduces the original failure where v2 emitted only the
    outer SELECT with an undefined alias.
    """
    create = await client_with_roads.post(
        "/nodes/dimension/",
        json={
            "name": "default.literal_hours",
            "description": "24 hour-of-day rows from a literal sequence",
            "query": "SELECT CAST(hour AS INT) AS hour FROM (SELECT EXPLODE(SEQUENCE(0, 23)) AS hour) t",
            "primary_key": ["hour"],
            "mode": "published",
        },
    )
    assert create.status_code == 201, create.json()

    response = await client_with_roads.get(
        "/sql/default.literal_hours/",
        params={"limit": 10},
    )
    assert response.status_code == 200, response.json()

    assert_sql_equal(
        response.json()["sql"],
        """
        SELECT CAST(hour AS INT) AS hour
        FROM (SELECT EXPLODE(SEQUENCE(0, 23)) AS hour) t
        LIMIT 10
        """,
    )


@pytest.mark.asyncio
async def test_sql_for_source_node(
    client_with_roads: AsyncClient,
):
    """
    Source nodes are emitted as ``SELECT cols FROM <catalog>.<schema>.<table>``
    directly — no CTE wrapper, since v3 treats sources as physical-table refs.
    """
    response = await client_with_roads.get(
        "/sql/default.repair_orders/",
        params={"limit": 5},
    )
    assert response.status_code == 200, response.json()

    assert_sql_equal(
        response.json()["sql"],
        """
        SELECT
          repair_order_id,
          municipality_id,
          hard_hat_id,
          order_date,
          required_date,
          dispatched_date,
          dispatcher_id
        FROM default.roads.repair_orders
        LIMIT 5
        """,
    )


@pytest.mark.asyncio
async def test_sql_for_transform_with_source_parents(
    client_with_roads: AsyncClient,
):
    """
    A transform whose only upstream nodes are sources renders as just the
    transform's own query body — sources inline as physical-table refs, and
    no CTE wrapper appears.
    """
    response = await client_with_roads.get(
        "/sql/default.repair_orders_fact/",
        params={"limit": 7},
    )
    assert response.status_code == 200, response.json()

    assert_sql_equal(
        response.json()["sql"],
        """
        SELECT
          repair_orders.repair_order_id,
          repair_orders.municipality_id,
          repair_orders.hard_hat_id,
          repair_orders.dispatcher_id,
          repair_orders.order_date,
          repair_orders.dispatched_date,
          repair_orders.required_date,
          repair_order_details.discount,
          repair_order_details.price,
          repair_order_details.quantity,
          repair_order_details.repair_type_id,
          repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
          repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
          repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
        FROM default.roads.repair_orders repair_orders
        JOIN default.roads.repair_order_details repair_order_details
          ON repair_orders.repair_order_id = repair_order_details.repair_order_id
        LIMIT 7
        """,
    )


@pytest.mark.asyncio
async def test_sql_for_dimension_with_source_parent(
    client_with_roads: AsyncClient,
):
    """A dimension that selects from a source renders as the dim's own body."""
    response = await client_with_roads.get(
        "/sql/default.hard_hat/",
        params={"limit": 3},
    )
    assert response.status_code == 200, response.json()

    assert_sql_equal(
        response.json()["sql"],
        """
        SELECT
          hard_hat_id,
          last_name,
          first_name,
          title,
          birth_date,
          hire_date,
          address,
          city,
          state,
          postal_code,
          country,
          manager,
          contractor_id
        FROM default.roads.hard_hats
        LIMIT 3
        """,
    )


@pytest.mark.asyncio
async def test_sql_for_multi_hop_transform_chain(
    client_with_roads: AsyncClient,
):
    """
    A transform whose upstream is *another transform* (not just sources).
    The inner transform becomes a real CTE — that's the case where the WITH
    clause is genuinely needed because the outer body has to reference an
    inlined sub-query body somewhere.
    """
    inner = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": "default.test_filtered_orders",
            "description": "Repair orders filtered to high-id rows",
            "query": (
                "SELECT repair_order_id, hard_hat_id "
                "FROM default.repair_orders "
                "WHERE repair_order_id > 100"
            ),
            "mode": "published",
        },
    )
    assert inner.status_code == 201, inner.json()

    outer = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": "default.test_filtered_order_ids",
            "description": "Just the order ids of the filtered set",
            "query": "SELECT repair_order_id FROM default.test_filtered_orders",
            "mode": "published",
        },
    )
    assert outer.status_code == 201, outer.json()

    response = await client_with_roads.get(
        "/sql/default.test_filtered_order_ids/",
        params={"limit": 4},
    )
    assert response.status_code == 200, response.json()

    assert_sql_equal(
        response.json()["sql"],
        """
        WITH default_test_filtered_orders AS (
          SELECT repair_order_id, hard_hat_id
          FROM default.roads.repair_orders
          WHERE repair_order_id > 100
        )
        SELECT repair_order_id
        FROM default_test_filtered_orders
        LIMIT 4
        """,
    )


@pytest.mark.asyncio
async def test_sql_for_transform_with_window_and_case(
    client_with_roads: AsyncClient,
):
    """
    A transform with non-trivial SQL features (window function, CASE WHEN,
    arithmetic in projection) round-trips as written, with sources inlined.
    """
    create = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": "default.test_orders_with_rank",
            "description": "Repair orders ranked by id within hard hat",
            "query": (
                "SELECT "
                "  repair_order_id, "
                "  hard_hat_id, "
                "  CASE WHEN repair_order_id > 1000 THEN 'large' ELSE 'small' END AS bucket, "
                "  ROW_NUMBER() OVER (PARTITION BY hard_hat_id ORDER BY repair_order_id) AS rk "
                "FROM default.repair_orders"
            ),
            "mode": "published",
        },
    )
    assert create.status_code == 201, create.json()

    response = await client_with_roads.get(
        "/sql/default.test_orders_with_rank/",
        params={"limit": 5},
    )
    assert response.status_code == 200, response.json()

    assert_sql_equal(
        response.json()["sql"],
        """
        SELECT
          repair_order_id,
          hard_hat_id,
          CASE WHEN repair_order_id > 1000 THEN 'large' ELSE 'small' END AS bucket,
          ROW_NUMBER() OVER (PARTITION BY hard_hat_id ORDER BY repair_order_id) AS rk
        FROM default.roads.repair_orders
        LIMIT 5
        """,
    )


@pytest.mark.asyncio
async def test_sql_for_dimension_with_union_all(
    client_with_roads: AsyncClient,
):
    """
    A dimension whose query has a UNION ALL of literal-only SELECTs (no real
    upstream tables) renders as the union as written.
    """
    create = await client_with_roads.post(
        "/nodes/dimension/",
        json={
            "name": "default.test_priorities",
            "description": "Priority codes",
            "query": (
                "SELECT 1 AS code, 'high' AS label "
                "UNION ALL SELECT 2, 'medium' "
                "UNION ALL SELECT 3, 'low'"
            ),
            "primary_key": ["code"],
            "mode": "published",
        },
    )
    assert create.status_code == 201, create.json()

    response = await client_with_roads.get(
        "/sql/default.test_priorities/",
        params={"limit": 10},
    )
    assert response.status_code == 200, response.json()

    assert_sql_equal(
        response.json()["sql"],
        """
        SELECT 1 AS code, 'high' AS label
        UNION ALL SELECT 2, 'medium'
        UNION ALL SELECT 3, 'low'
        LIMIT 10
        """,
    )


@pytest.mark.asyncio
async def test_sql_for_transform_with_inner_cte(
    client_with_roads: AsyncClient,
):
    """
    A transform whose own query has a WITH clause: ``flatten_inner_ctes``
    extracts and prefixes the inner CTE so it doesn't collide with anything
    else, then the body references the prefixed name. This is the same
    flattening v3 metrics relies on; we just inherit it via ``collect_node_ctes``.
    """
    create = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": "default.test_orders_with_inner_cte",
            "description": "Transform that itself uses a WITH clause",
            "query": (
                "WITH high_id_orders AS ("
                "  SELECT repair_order_id, hard_hat_id "
                "  FROM default.repair_orders "
                "  WHERE repair_order_id > 100"
                ") "
                "SELECT repair_order_id, hard_hat_id "
                "FROM high_id_orders"
            ),
            "mode": "published",
        },
    )
    assert create.status_code == 201, create.json()

    response = await client_with_roads.get(
        "/sql/default.test_orders_with_inner_cte/",
        params={"limit": 6},
    )
    assert response.status_code == 200, response.json()

    # The inner ``high_id_orders`` CTE gets prefixed with the outer node's
    # CTE name to avoid collisions across the wider query — but here the
    # outer node *is* the starting node, so the prefix shape is the
    # canonical one ``flatten_inner_ctes`` produces. The body then references
    # the prefixed name, aliased back to the original.
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH default_test_orders_with_inner_cte__high_id_orders AS (
          SELECT repair_order_id, hard_hat_id
          FROM default.roads.repair_orders
          WHERE repair_order_id > 100
        )
        SELECT repair_order_id, hard_hat_id
        FROM default_test_orders_with_inner_cte__high_id_orders high_id_orders
        LIMIT 6
        """,
    )


@pytest.mark.asyncio
async def test_with_dimensions_falls_through_to_v2(
    client_with_roads: AsyncClient,
):
    """
    Phase 1 only handles the no-dims/no-filters case in v3. A request with
    dimensions still goes through the v2 path until Phase 2 ports dim-link
    join resolution. This test pins that contract — the request succeeds and
    returns SQL — without pinning the exact v2 output (which we don't want
    to lock in given v2 is on the way out).
    """
    response = await client_with_roads.get(
        "/sql/default.repair_orders_fact/",
        params={
            "dimensions": ["default.hard_hat.state"],
            "limit": 2,
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    # v2 always wraps the starting node as a CTE; the new v3 path doesn't.
    # The presence of the v2-style dot-mangled CTE alias is the cheap signal
    # that we did *not* go through ``build_node_sql_v3`` for this request.
    assert "default_DOT_repair_orders_fact" in sql
