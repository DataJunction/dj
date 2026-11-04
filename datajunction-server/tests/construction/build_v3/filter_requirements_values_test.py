"""Distinct-value lookups ignore metric filter requirements."""

import pytest

from . import assert_sql_equal


async def _setup(client, cube_name, requirement):
    query = (await client.get("/nodes/v3.total_revenue/")).json()["query"]
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": f"{cube_name}_metric",
            "description": "d",
            "query": query,
            "mode": "published",
            "required_filters": [requirement],
        },
    )
    assert response.status_code == 201, response.text
    response = await client.post(
        "/nodes/cube/",
        json={
            "name": cube_name,
            "metrics": [f"{cube_name}_metric"],
            "dimensions": ["v3.date.date_id[order]", "v3.product.category"],
            "mode": "published",
            "description": "d",
        },
    )
    assert response.status_code == 201, response.text


VALUES_SQL = """
SELECT DISTINCT dimension_values.category
FROM (
  WITH v3_order_details AS (
    SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total
    FROM default.v3.orders o
    JOIN default.v3.order_items oi ON o.order_id = oi.order_id
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
    SUM(order_details_0.line_total_sum_e1f61696) AS {metric}
  FROM order_details_0
  GROUP BY order_details_0.category
) AS dimension_values
"""

REQUIREMENTS = [
    pytest.param(
        "v3.values_required",
        {"dimension": "v3.date.date_id[order]"},
        id="required-without-default",
    ),
    pytest.param(
        "v3.values_default",
        {
            "dimension": "v3.date.date_id[order]",
            "default": "v3.date.date_id[order] = 20250101",
        },
        id="required-with-default",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("cube_name,requirement", REQUIREMENTS)
async def test_dimension_values_skip_requirements(
    client_with_build_v3,
    cube_name,
    requirement,
):
    await _setup(client_with_build_v3, cube_name, requirement)
    response = await client_with_build_v3.get(
        "/sql/dimensions/v3/",
        params={"dimensions": ["v3.product.category"], "cube": cube_name},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    metric = cube_name.split(".")[-1] + "_metric"
    assert_sql_equal(body["sql"], VALUES_SQL.format(metric=metric))
    assert body["warnings"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("cube_name,requirement", REQUIREMENTS)
async def test_semantic_values_skip_requirements(
    client_with_build_v3,
    cube_name,
    requirement,
):
    await _setup(client_with_build_v3, cube_name, requirement)
    response = await client_with_build_v3.post(
        f"/semantic/views/{cube_name}/values",
        json={"dimension": "v3.product.category"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    metric = cube_name.split(".")[-1] + "_metric"
    assert_sql_equal(body["sql"], VALUES_SQL.format(metric=metric) + "LIMIT 10000")
    assert {key: value for key, value in body.items() if key != "sql"} == {
        "dialect": "spark",
        "columns": [{"name": "category", "type": "utf8"}],
        "cube_name": None,
    }
