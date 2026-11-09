"""Tests for /sql endpoints that use node SQL build v2"""

from unittest import mock

import duckdb
import pytest
from httpx import AsyncClient

from datajunction_server.sql.parsing.backends.antlr4 import parse


async def fix_dimension_links(module__client_with_roads: AsyncClient):
    """
    Override some dimension links with inner join instead of left join.
    """
    await module__client_with_roads.post(
        "/nodes/default.repair_orders_fact/link",
        json={
            "dimension_node": "default.hard_hat",
            "join_type": "inner",
            "join_on": (
                "default.repair_orders_fact.hard_hat_id = default.hard_hat.hard_hat_id"
            ),
        },
    )
    await module__client_with_roads.post(
        "/nodes/default.repair_orders_fact/link",
        json={
            "dimension_node": "default.dispatcher",
            "join_type": "left",
            "join_on": (
                "default.repair_orders_fact.dispatcher_id = default.dispatcher.dispatcher_id"
            ),
        },
    )
    await module__client_with_roads.post(
        "/nodes/default.hard_hat/link",
        json={
            "dimension_node": "default.us_state",
            "join_type": "inner",
            "join_on": ("default.hard_hat.state = default.us_state.state_short"),
        },
    )


@pytest.mark.asyncio
async def test_metrics_sql_different_parents(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
):
    """
    Test ``GET /sql`` for metrics from different parents.
    """
    await fix_dimension_links(module__client_with_roads)

    response = await module__client_with_roads.get(
        "/sql",
        params={
            "metrics": [
                "default.avg_length_of_employment",
                "default.total_repair_cost",
            ],
            "dimensions": [
                "default.hard_hat.first_name",
                "default.hard_hat.last_name",
            ],
            "filters": [
                # "default.hard_hat.first_name like '%a%'",
            ],
            "orderby": ["default.hard_hat.last_name"],
            "limit": 5,
        },
    )
    data = response.json()
    expected_sql = """WITH
default_DOT_hard_hat AS (
SELECT  default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.last_name,
    default_DOT_hard_hats.first_name,
    default_DOT_hard_hats.title,
    default_DOT_hard_hats.birth_date,
    default_DOT_hard_hats.hire_date,
    default_DOT_hard_hats.address,
    default_DOT_hard_hats.city,
    default_DOT_hard_hats.state,
    default_DOT_hard_hats.postal_code,
    default_DOT_hard_hats.country,
    default_DOT_hard_hats.manager,
    default_DOT_hard_hats.contractor_id
 FROM roads.hard_hats AS default_DOT_hard_hats
),
default_DOT_repair_orders_fact AS (
SELECT  repair_orders.repair_order_id,
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
 FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id
),
default_DOT_hard_hat_metrics AS (
SELECT  default_DOT_hard_hat.last_name default_DOT_hard_hat_DOT_last_name,
    default_DOT_hard_hat.first_name default_DOT_hard_hat_DOT_first_name,
    avg(CAST(NOW() AS DATE) - default_DOT_hard_hat.hire_date) default_DOT_avg_length_of_employment
 FROM default_DOT_hard_hat
 GROUP BY  default_DOT_hard_hat.last_name, default_DOT_hard_hat.first_name
),
default_DOT_repair_orders_fact_metrics AS (
SELECT  default_DOT_hard_hat.first_name default_DOT_hard_hat_DOT_first_name,
    default_DOT_hard_hat.last_name default_DOT_hard_hat_DOT_last_name,
    sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
 FROM default_DOT_repair_orders_fact INNER JOIN default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
 GROUP BY  default_DOT_hard_hat.first_name, default_DOT_hard_hat.last_name
)
SELECT  default_DOT_hard_hat_metrics.default_DOT_hard_hat_DOT_last_name,
    default_DOT_hard_hat_metrics.default_DOT_hard_hat_DOT_first_name,
    default_DOT_hard_hat_metrics.default_DOT_avg_length_of_employment,
    default_DOT_repair_orders_fact_metrics.default_DOT_total_repair_cost
 FROM default_DOT_hard_hat_metrics FULL JOIN default_DOT_repair_orders_fact_metrics ON default_DOT_hard_hat_metrics.default_DOT_hard_hat_DOT_first_name = default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_first_name AND default_DOT_hard_hat_metrics.default_DOT_hard_hat_DOT_last_name = default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_last_name
 ORDER BY default_DOT_hard_hat_metrics.default_DOT_hard_hat_DOT_last_name
 LIMIT 5"""
    assert str(parse(str(data["sql"]))) == str(parse(expected_sql))

    response = await module__client_with_roads.get(
        "/sql",
        params={
            "metrics": [
                "default.avg_length_of_employment",
                "default.total_repair_cost",
            ],
            "dimensions": [
                "default.hard_hat.first_name",
                "default.hard_hat.last_name",
            ],
            "filters": [],
            "orderby": "default.hard_hat.last_name",
            "limit": 5,
        },
    )
    data = response.json()
    assert str(parse(str(data["sql"]))) == str(parse(expected_sql))

    result = duckdb_conn.sql(data["sql"])
    assert result.fetchall() == [
        ("Alfred", "Clarke", mock.ANY, 196787.0),
        ("Brian", "Perkins", mock.ANY, 218691.0),
        ("Cathy", "Best", mock.ANY, 229666.0),
        ("Donna", "Riley", mock.ANY, 320953.0),
        ("Luka", "Henderson", mock.ANY, 131364.0),
    ]


@pytest.mark.asyncio
async def test_sql_metric_definition_with_multiple_joinable_dimensions(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
):
    """
    Test metrics SQL for metric definitions that reference joinable dimensions
    """
    await fix_dimension_links(module__client_with_roads)

    metric_name = "default.unique_hard_hat_names_in_ny2"
    response = await module__client_with_roads.post(
        "/nodes/metric",
        json={
            "description": "An example metric with a definition that references a joinable dimension",
            "query": "SELECT COUNT(DISTINCT IF(default.hard_hat.state = 'NY', default.hard_hat.first_name, NULL)) FROM default.repair_orders_fact",
            "mode": "published",
            "name": metric_name,
            "display_name": "Number of Unique Hard Hat Names in NY",
        },
    )
    response = await module__client_with_roads.get(
        "/sql",
        params={
            "metrics": [metric_name],
            "dimensions": [],
            "filters": [],
        },
    )
    data = response.json()
    expected_sql = """
    WITH default_DOT_repair_orders_fact AS (
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
      FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id
    ),
    default_DOT_hard_hat AS (
      SELECT
        default_DOT_hard_hats.hard_hat_id,
        default_DOT_hard_hats.last_name,
        default_DOT_hard_hats.first_name,
        default_DOT_hard_hats.title,
        default_DOT_hard_hats.birth_date,
        default_DOT_hard_hats.hire_date,
        default_DOT_hard_hats.address,
        default_DOT_hard_hats.city,
        default_DOT_hard_hats.state,
        default_DOT_hard_hats.postal_code,
        default_DOT_hard_hats.country,
        default_DOT_hard_hats.manager,
        default_DOT_hard_hats.contractor_id
      FROM roads.hard_hats AS default_DOT_hard_hats
    ),
    default_DOT_repair_orders_fact_metrics AS (
      SELECT
        default_DOT_hard_hat.first_name default_DOT_hard_hat_DOT_first_name,
        default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
        COUNT( DISTINCT IF(default_DOT_hard_hat.state = 'NY', default_DOT_hard_hat.first_name, NULL)) default_DOT_unique_hard_hat_names_in_ny2
      FROM default_DOT_repair_orders_fact INNER JOIN default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
      GROUP BY
        default_DOT_hard_hat.first_name,
        default_DOT_hard_hat.state
    )
    SELECT
      default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_first_name,
      default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_state,
      default_DOT_repair_orders_fact_metrics.default_DOT_unique_hard_hat_names_in_ny2
    FROM default_DOT_repair_orders_fact_metrics
    """
    assert str(parse(data["sql"])) == str(parse(expected_sql))
    result = duckdb_conn.sql(data["sql"])
    assert sorted(result.fetchall()) == sorted(
        [
            ("Perkins", "NJ", 0),
            ("Best", "MA", 0),
            ("Riley", "MI", 0),
            ("Henderson", "AZ", 0),
            ("Stafford", "PA", 0),
            ("Clarke", "GA", 0),
            ("Massey", "CT", 0),
            ("Ziegler", "OK", 0),
            ("Boone", "NY", 1),
        ],
    )


@pytest.mark.asyncio
async def test_approx_count_distinct_metric_sql(
    module__client_with_simple_hll: AsyncClient,
):
    """
    Test SQL generation for APPROX_COUNT_DISTINCT metric using a minimal fixture.

    This verifies that:
    1. The metric query generates valid SQL with APPROX_COUNT_DISTINCT
    2. The metric decomposes to a Spark HLL combiner

    Uses the SIMPLE_HLL fixture which has just:
    - hll.events (source table with user_id, category)
    - hll.category_dim (dimension)
    - hll.unique_users (APPROX_COUNT_DISTINCT metric)
    """
    client = module__client_with_simple_hll

    response = await client.get(
        "/sql",
        params={
            "metrics": ["hll.unique_users"],
            "dimensions": ["hll.category_dim.category"],
            "filters": [],
        },
    )
    assert response.status_code == 200
    data = response.json()

    # The SQL should contain APPROX_COUNT_DISTINCT
    assert "APPROX_COUNT_DISTINCT" in data["sql"].upper()

    response = await client.get("/metrics/hll.unique_users")
    assert str(parse(response.json()["derived_query"])) == str(
        parse(
            "SELECT hll_sketch_estimate(hll_union_agg(user_id_hll_7a744b96)) FROM hll.events",
        ),
    )


@pytest.mark.asyncio
async def test_sql_non_metric_node_returns_422(
    module__client_with_roads: AsyncClient,
):
    """
    Requesting metrics SQL for a node that is not a metric should return a 422.
    """
    response = await module__client_with_roads.get(
        "/sql/",
        params={"metrics": ["default.repair_orders_fact"]},
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "All nodes must be of metric type, but some are not: "
        "default.repair_orders_fact (transform) ."
    )


@pytest.mark.asyncio
async def test_sql_local_dimension_in_metric(
    module__client_with_roads: AsyncClient,
):
    """
    Test metrics SQL for a metric that references a local dimension
    """
    await fix_dimension_links(module__client_with_roads)

    response = await module__client_with_roads.get(
        "/sql",
        params={
            "metrics": ["default.avg_length_of_employment"],
            "dimensions": ["default.hard_hat.hire_date"],
        },
    )
    assert response.status_code == 200
    expected_sql = """
    WITH default_DOT_hard_hat AS (
      SELECT
        default_DOT_hard_hats.hard_hat_id,
        default_DOT_hard_hats.last_name,
        default_DOT_hard_hats.first_name,
        default_DOT_hard_hats.title,
        default_DOT_hard_hats.birth_date,
        default_DOT_hard_hats.hire_date,
        default_DOT_hard_hats.address,
        default_DOT_hard_hats.city,
        default_DOT_hard_hats.state,
        default_DOT_hard_hats.postal_code,
        default_DOT_hard_hats.country,
        default_DOT_hard_hats.manager,
        default_DOT_hard_hats.contractor_id
      FROM roads.hard_hats AS default_DOT_hard_hats
    ),
    default_DOT_hard_hat_metrics AS (
      SELECT
        default_DOT_hard_hat.hire_date default_DOT_hard_hat_DOT_hire_date,
        avg(CAST(NOW() AS DATE) - default_DOT_hard_hat.hire_date) default_DOT_avg_length_of_employment
      FROM default_DOT_hard_hat
      GROUP BY default_DOT_hard_hat.hire_date
    )
    SELECT
      default_DOT_hard_hat_metrics.default_DOT_hard_hat_DOT_hire_date,
      default_DOT_hard_hat_metrics.default_DOT_avg_length_of_employment
    FROM default_DOT_hard_hat_metrics
    """
    assert str(parse(response.json()["sql"])) == str(parse(expected_sql))


@pytest.mark.asyncio
async def test_sql_nonexistent_metric_returns_404(
    module__client_with_roads: AsyncClient,
):
    """
    Requesting SQL for a metric that does not exist should return a 404.
    """
    response = await module__client_with_roads.get(
        "/sql/",
        params={"metrics": ["default.this_metric_does_not_exist"]},
    )
    assert response.status_code == 404
    assert "default.this_metric_does_not_exist" in response.json()["message"]
