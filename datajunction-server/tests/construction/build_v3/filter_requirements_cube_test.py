"""Filter requirements on queries that can use a cube."""

import time

import pytest

from . import assert_sql_equal


async def _make_metric(client, name, **fields):
    query = (await client.get("/nodes/v3.total_revenue/")).json()["query"]
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": name,
            "description": "d",
            "query": query,
            "mode": "published",
            **fields,
        },
    )
    assert response.status_code == 201, response.text


async def _make_cube(client, name, metric, dimensions, **fields):
    response = await client.post(
        "/nodes/cube/",
        json={
            "name": name,
            "metrics": [metric],
            "dimensions": dimensions,
            "mode": "published",
            "description": "d",
            **fields,
        },
    )
    assert response.status_code == 201, response.text
    table = name.split(".")[-1]
    response = await client.post(
        f"/data/{name}/availability/",
        json={
            "catalog": "default",
            "schema_": "analytics",
            "table": table,
            "valid_through_ts": int(time.time() * 1000),
        },
    )
    assert response.status_code == 200, response.text


DEFAULT = {
    "dimension": "v3.date.date_id[order]",
    "default": "v3.date.date_id[order] = 20250101",
}


CUBE_SQL = """
WITH cube_with_date_0 AS (
  SELECT category, date_id_order, line_total_sum_e1f61696
  FROM cube_with_date
  WHERE date_id_order = {date}
)
SELECT cube_with_date_0.category AS category,
  SUM(cube_with_date_0.line_total_sum_e1f61696) AS cube_default
FROM cube_with_date_0
GROUP BY cube_with_date_0.category
"""

# The node SQL cache is shared between tests, so these tests use their own cube
# names.
# The cube node's own SQL is built from the source tables.
CUBE_NODE_SQL = """
WITH
v3_order_details AS (
  SELECT o.order_date, oi.product_id, oi.quantity * oi.unit_price AS line_total
  FROM default.v3.orders o
  JOIN default.v3.order_items oi ON o.order_id = oi.order_id
  WHERE o.order_date = {date}
),
v3_product AS (
  SELECT product_id, category FROM default.v3.products
),
order_details_0 AS (
  SELECT t1.order_date date_id_order, t2.category,
    SUM(t1.line_total) line_total_sum_e1f61696
  FROM v3_order_details t1
  LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
  GROUP BY t1.order_date, t2.category
)
SELECT order_details_0.date_id_order AS date_id_order,
  order_details_0.category AS category,
  SUM(order_details_0.line_total_sum_e1f61696) AS cube_default
FROM order_details_0
WHERE order_details_0.date_id_order = {date}
GROUP BY order_details_0.date_id_order, order_details_0.category
"""

# Source-table SQL for the cube query when the cube is not used.
SOURCE_SQL = """
WITH
v3_order_details AS (
  SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total
  FROM default.v3.orders o
  JOIN default.v3.order_items oi ON o.order_id = oi.order_id
  WHERE o.order_date = 20250101
),
v3_product AS (
  SELECT product_id, category FROM default.v3.products
),
order_details_0 AS (
  SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
  FROM v3_order_details t1
  LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
  GROUP BY t2.category
)
SELECT order_details_0.category AS category,
  SUM(order_details_0.line_total_sum_e1f61696) AS cube_default
FROM order_details_0
GROUP BY order_details_0.category
"""

APPLIED = [
    (
        "DEFAULT_FILTER_APPLIED",
        "Applied default filter v3.date.date_id[order] = 20250101 "
        "(from metric v3.cube_default)",
    ),
]

QUERY = {
    "metrics": ["v3.cube_default"],
    "dimensions": ["v3.product.category"],
    "dialect": "druid",
}


async def _setup(client, cube_name="v3.cube_with_date", dimensions=None, **fields):
    await _make_metric(client, "v3.cube_default", required_filters=[DEFAULT])
    await _make_cube(
        client,
        cube_name,
        "v3.cube_default",
        dimensions or ["v3.date.date_id[order]", "v3.product.category"],
        **fields,
    )


@pytest.mark.asyncio
async def test_default_applied_when_cube_has_dimension(client_with_build_v3):
    await _setup(client_with_build_v3)
    response = await client_with_build_v3.get("/sql/metrics/v3/", params=QUERY)
    assert response.status_code == 200
    body = response.json()
    assert body["cube_name"] == "v3.cube_with_date"
    assert_sql_equal(body["sql"], CUBE_SQL.format(date=20250101))
    assert [(w["code"], w["message"]) for w in body["warnings"]] == APPLIED


@pytest.mark.asyncio
async def test_falls_back_to_sources_when_cube_lacks_dimension(client_with_build_v3):
    await _setup(client_with_build_v3, dimensions=["v3.product.category"])
    response = await client_with_build_v3.get("/sql/metrics/v3/", params=QUERY)
    assert response.status_code == 200
    body = response.json()
    assert body["cube_name"] is None
    assert_sql_equal(body["sql"], SOURCE_SQL)
    assert [(w["code"], w["message"]) for w in body["warnings"]] == APPLIED


@pytest.mark.asyncio
async def test_pinned_cube_without_dimension_is_rejected(client_with_build_v3):
    await _setup(client_with_build_v3, dimensions=["v3.product.category"])
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={**QUERY, "cube": "v3.cube_with_date"},
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Pinned cube `v3.cube_with_date` does not cover dimension(s) "
        "['v3.date.date_id[order]'] referenced by the query. "
        "Remove the filter/dimension, or pick a cube that materializes it."
    )


@pytest.mark.asyncio
async def test_pinned_cube_with_dimension_gets_default(client_with_build_v3):
    await _setup(client_with_build_v3)
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={**QUERY, "cube": "v3.cube_with_date"},
    )
    assert response.status_code == 200
    body = response.json()
    assert_sql_equal(body["sql"], CUBE_SQL.format(date=20250101))
    assert [(w["code"], w["message"]) for w in body["warnings"]] == APPLIED


@pytest.mark.asyncio
async def test_pinned_cube_filter_replaces_default(client_with_build_v3):
    await _setup(client_with_build_v3, filters=["v3.date.date_id[order] = 20240101"])
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={**QUERY, "cube": "v3.cube_with_date"},
    )
    assert response.status_code == 200
    body = response.json()
    assert_sql_equal(body["sql"], CUBE_SQL.format(date=20240101))
    assert body["warnings"] == []


@pytest.mark.asyncio
async def test_cube_node_sql_gets_default(client_with_build_v3):
    await _setup(client_with_build_v3, "v3.cube_node_default")
    response = await client_with_build_v3.get(
        "/sql/v3.cube_node_default/",
        params={"dialect": "druid"},
    )
    assert response.status_code == 200
    body = response.json()
    assert_sql_equal(body["sql"], CUBE_NODE_SQL.format(date=20250101))
    assert [(w["code"], w["message"]) for w in body["warnings"]] == APPLIED


@pytest.mark.asyncio
async def test_cube_node_sql_with_stored_filter_skips_default(client_with_build_v3):
    await _setup(
        client_with_build_v3,
        "v3.cube_node_stored",
        filters=["v3.date.date_id[order] = 20240101"],
    )
    response = await client_with_build_v3.get(
        "/sql/v3.cube_node_stored/",
        params={"dialect": "druid"},
    )
    assert response.status_code == 200
    body = response.json()
    assert_sql_equal(body["sql"], CUBE_NODE_SQL.format(date=20240101))
    assert body["warnings"] == []


@pytest.mark.asyncio
async def test_pinned_cube_rejects_roleless_default(client_with_build_v3):
    await _make_metric(
        client_with_build_v3,
        "v3.cube_default",
        required_filters=[
            {
                "dimension": "v3.date.date_id",
                "default": "v3.date.date_id = 20250101",
            },
        ],
    )
    await _make_cube(
        client_with_build_v3,
        "v3.cube_with_date",
        "v3.cube_default",
        ["v3.date.date_id[order]", "v3.product.category"],
    )
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={**QUERY, "cube": "v3.cube_with_date"},
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Pinned cube `v3.cube_with_date` does not cover dimension(s) "
        "['v3.date.date_id'] referenced by the query. "
        "Remove the filter/dimension, or pick a cube that materializes it."
    )


DRUID = {"engine_name": "druid", "engine_version": ""}

# A cube node on Druid reads its cube table, grouped by the cube's dimensions.
CUBE_NODE_DRUID_SQL = """
WITH cube_node_druid_0 AS (
  SELECT date_id_order, category, line_total_sum_e1f61696
  FROM cube_node_druid
  WHERE date_id_order = 20250101
)
SELECT cube_node_druid_0.date_id_order AS date_id_order,
  cube_node_druid_0.category AS category,
  SUM(cube_node_druid_0.line_total_sum_e1f61696) AS cube_default
FROM cube_node_druid_0
WHERE cube_node_druid_0.date_id_order = 20250101
GROUP BY cube_node_druid_0.date_id_order, cube_node_druid_0.category
"""


@pytest.mark.asyncio
async def test_druid_cube_node_sql_reads_cube_with_default(client_with_build_v3):
    await _setup(client_with_build_v3, "v3.cube_node_druid")
    response = await client_with_build_v3.get(
        "/sql/v3.cube_node_druid/",
        params=DRUID,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert_sql_equal(body["sql"], CUBE_NODE_DRUID_SQL)
    assert [(w["code"], w["message"]) for w in body["warnings"]] == APPLIED


@pytest.mark.asyncio
async def test_druid_cube_node_without_default_dimension_is_rejected(
    client_with_build_v3,
):
    await _setup(
        client_with_build_v3,
        "v3.cube_node_no_date",
        dimensions=["v3.product.category"],
    )
    response = await client_with_build_v3.get(
        "/sql/v3.cube_node_no_date/",
        params=DRUID,
    )
    assert response.status_code == 422, response.text
    assert response.json()["message"] == (
        "Pinned cube `v3.cube_node_no_date` does not cover dimension(s) "
        "['v3.date.date_id[order]'] referenced by the query. "
        "Remove the filter/dimension, or pick a cube that materializes it."
    )
