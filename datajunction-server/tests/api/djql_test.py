"""
Tests for the djql API.
"""
# pylint: disable=too-many-lines

from fastapi.testclient import TestClient

from tests.sql.utils import compare_query_strings


def test_get_djql_data_only_nodes_query(
    client_with_query_service: TestClient,
) -> None:
    """
    Test djql with just some non-metric nodes
    """

    query = """
SELECT default.hard_hat.country,
  default.hard_hat.city
FROM default.hard_hat
    """

    response = client_with_query_service.get(
        "/djql/data/",
        params={"query": query},
    )
    query = response.json()["results"][0]["sql"]
    expected_query = """WITH
node_query_0 AS (SELECT  default_DOT_hard_hats.address,
    default_DOT_hard_hats.birth_date,
    default_DOT_hard_hats.city,
    default_DOT_hard_hats.contractor_id,
    default_DOT_hard_hats.country,
    default_DOT_hard_hats.first_name,
    default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.hire_date,
    default_DOT_hard_hats.last_name,
    default_DOT_hard_hats.manager,
    default_DOT_hard_hats.postal_code,
    default_DOT_hard_hats.state,
    default_DOT_hard_hats.title
 FROM roads.hard_hats AS default_DOT_hard_hats

)

SELECT  node_query_0.country,
    node_query_0.city
 FROM node_query_0"""

    assert compare_query_strings(query, expected_query)


def test_get_djql_data_only_nested_metrics(
    client_with_query_service: TestClient,
) -> None:
    """
    Test djql with metric subquery
    """

    query = """
    SELECT
    Sum(avg_repair_price),
    city
    FROM
    (
        SELECT
        default.avg_repair_price avg_repair_price,
        default.hard_hat.country,
        default.hard_hat.city
        FROM
        metrics
        GROUP BY
        default.hard_hat.country,
        default.hard_hat.city
        LIMIT
        5
    )
    GROUP BY city
    """

    response = client_with_query_service.get(
        "/djql/data/",
        params={"query": query},
    )
    query = response.json()["results"][0]["sql"]
    expected_query = """WITH
metric_query_0 AS (SELECT  m0_default_DOT_avg_repair_price.default_DOT_avg_repair_price,
    m0_default_DOT_avg_repair_price.city,
    m0_default_DOT_avg_repair_price.country
 FROM (SELECT  default_DOT_hard_hat.city,
    default_DOT_hard_hat.country,
    avg(default_DOT_repair_order_details.price) default_DOT_avg_repair_price
 FROM roads.repair_order_details AS default_DOT_repair_order_details LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
    default_DOT_repair_orders.hard_hat_id,
    default_DOT_repair_orders.municipality_id,
    default_DOT_repair_orders.repair_order_id
 FROM roads.repair_orders AS default_DOT_repair_orders)
 AS default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.birth_date,
    default_DOT_hard_hats.city,
    default_DOT_hard_hats.country,
    default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.hire_date,
    default_DOT_hard_hats.state
 FROM roads.hard_hats AS default_DOT_hard_hats)
 AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
 GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.city
) AS m0_default_DOT_avg_repair_price
LIMIT 5
)

SELECT  Sum(avg_repair_price),
    city
 FROM (SELECT  metric_query_0.default_DOT_avg_repair_price AS avg_repair_price,
    metric_query_0.country,
    metric_query_0.city
 FROM metric_query_0
)
 GROUP BY  city"""

    assert compare_query_strings(query, expected_query)
