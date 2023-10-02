"""
Tests for the djsql API.
"""
# pylint: disable=too-many-lines,C0301

from fastapi.testclient import TestClient

from tests.sql.utils import compare_query_strings


def test_get_djsql_data_only_nodes_query(
    client_with_query_service: TestClient,
) -> None:
    """
    Test djsql with just some non-metric nodes
    """

    query = """
SELECT default.hard_hat.country,
  default.hard_hat.city
FROM default.hard_hat
    """

    response = client_with_query_service.get(
        "/djsql/data/",
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


def test_get_djsql_data_only_nested_metrics(
    client_with_query_service: TestClient,
) -> None:
    """
    Test djsql with metric subquery
    """

    query = """
    SELECT
    Sum(avg_repair_price),
    city
    FROM
    (
        SELECT
        default.avg_repair_price avg_repair_price,
        default.hard_hat.country country,
        default.hard_hat.city city
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
        "/djsql/data/",
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


def test_get_djsql_data_only_multiple_metrics(
    client_with_query_service: TestClient,
) -> None:
    """
    Test djsql with metric subquery
    """

    query = """
    SELECT
        default.avg_repair_price avg_repair_price,
        default.total_repair_cost total_cost,
        default.hard_hat.country,
        default.hard_hat.city
    FROM
        metrics
    GROUP BY
        default.hard_hat.country,
        default.hard_hat.city
    """

    response = client_with_query_service.get(
        "/djsql/data/",
        params={"query": query},
    )
    query = response.json()["results"][0]["sql"]
    expected_query = """WITH
metric_query_0 AS (SELECT  m0_default_DOT_avg_repair_price.default_DOT_avg_repair_price,
        m1_default_DOT_total_repair_cost.default_DOT_total_repair_cost,
        COALESCE(m0_default_DOT_avg_repair_price.city, m1_default_DOT_total_repair_cost.city) city,
        COALESCE(m0_default_DOT_avg_repair_price.country, m1_default_DOT_total_repair_cost.country) country
 FROM (SELECT  default_DOT_hard_hat.city,
        default_DOT_hard_hat.country,
        avg(default_DOT_repair_order_details.price) AS default_DOT_avg_repair_price
 FROM roads.repair_order_details AS default_DOT_repair_order_details LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
        default_DOT_repair_orders.hard_hat_id,
        default_DOT_repair_orders.municipality_id,
        default_DOT_repair_orders.repair_order_id
 FROM roads.repair_orders AS default_DOT_repair_orders)
 AS default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.city,
        default_DOT_hard_hats.country,
        default_DOT_hard_hats.hard_hat_id,
        default_DOT_hard_hats.state
 FROM roads.hard_hats AS default_DOT_hard_hats)
 AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
 GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.city
) AS m0_default_DOT_avg_repair_price FULL OUTER JOIN (SELECT  default_DOT_hard_hat.city,
        default_DOT_hard_hat.country,
        sum(default_DOT_repair_order_details.price) default_DOT_total_repair_cost
 FROM roads.repair_order_details AS default_DOT_repair_order_details LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
        default_DOT_repair_orders.hard_hat_id,
        default_DOT_repair_orders.municipality_id,
        default_DOT_repair_orders.repair_order_id
 FROM roads.repair_orders AS default_DOT_repair_orders)
 AS default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.city,
        default_DOT_hard_hats.country,
        default_DOT_hard_hats.hard_hat_id,
        default_DOT_hard_hats.state
 FROM roads.hard_hats AS default_DOT_hard_hats)
 AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
 GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.city
) AS m1_default_DOT_total_repair_cost ON m0_default_DOT_avg_repair_price.city = m1_default_DOT_total_repair_cost.city AND m0_default_DOT_avg_repair_price.country = m1_default_DOT_total_repair_cost.country

)

SELECT  metric_query_0.default_DOT_avg_repair_price AS avg_repair_price,
        metric_query_0.default_DOT_total_repair_cost AS total_cost,
        metric_query_0.country,
        metric_query_0.city
 FROM metric_query_0"""

    assert compare_query_strings(query, expected_query)


def test_get_djsql_metric_table_exception(
    client_with_query_service: TestClient,
) -> None:
    """
    Test djsql with metric subquery from non `metrics`
    """

    query = """
        SELECT
        default.avg_repair_price avg_repair_price,
        default.hard_hat.country,
        default.hard_hat.city
        FROM
        oops
        GROUP BY
        default.hard_hat.country,
        default.hard_hat.city

    """

    response = client_with_query_service.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert (
        response.json()["message"]
        == "Any SELECT referencing a Metric must source from a single unaliased Table named `metrics`."
    )


def test_get_djsql_illegal_clause_metric_query(
    client_with_query_service: TestClient,
) -> None:
    """
    Test djsql with metric subquery from non `metrics`
    """

    query = """
        SELECT
        default.avg_repair_price avg_repair_price,
        default.hard_hat.country,
        default.hard_hat.city
        FROM
        metrics
        GROUP BY
        default.hard_hat.country,
        default.hard_hat.city
        HAVING 5
    """

    response = client_with_query_service.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert (
        response.json()["message"]
        == "HAVING, LATERAL VIEWS, and SET OPERATIONS are not allowed on `metrics` queries."
    )


def test_get_djsql_illegal_column_expression(
    client_with_query_service: TestClient,
) -> None:
    """
    Test djsql with non col exp in projection
    """

    query = """
        SELECT
        default.avg_repair_price avg_repair_price,
        default.hard_hat.country,
        default.hard_hat.id+5
        FROM
        metrics
        GROUP BY
        default.hard_hat.country,
        default.hard_hat.city
    """

    response = client_with_query_service.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert (
        response.json()["message"]
        == "Only direct Columns are allowed in `metrics` queries, found `default.hard_hat.id + 5`."
    )


def test_get_djsql_illegal_column(
    client_with_query_service: TestClient,
) -> None:
    """
    Test djsql with bad col in projection
    """

    query = """
        SELECT
        default.avg_repair_price avg_repair_price,
        default.hard_hat.country,
        default.repair_orders.id
        FROM
        metrics
        GROUP BY
        default.hard_hat.country,
        default.hard_hat.city
    """

    response = client_with_query_service.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert (
        response.json()["message"]
        == "You can only select direct METRIC nodes or a column from your GROUP BY on `metrics` queries, found `default.repair_orders.id`"
    )


def test_get_djsql_illegal_limit(
    client_with_query_service: TestClient,
) -> None:
    """
    Test djsql with bad limit
    """

    query = """
        SELECT
        default.avg_repair_price avg_repair_price
        FROM
        metrics
        GROUP BY
        default.hard_hat.country
        LIMIT 1+2
    """

    response = client_with_query_service.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert (
        response.json()["message"]
        == "LIMITs on `metrics` queries can only be integers not `1 + 2`."
    )


def test_get_djsql_no_nodes(
    client_with_query_service: TestClient,
) -> None:
    """
    Test djsql without dj node refs
    """

    query = """
        SELECT 1
    """

    response = client_with_query_service.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert response.json()["message"].startswith("Found no dj nodes in query")


# def test_djsql_stream(
#     client_with_query_service: TestClient,
# ) -> None:
#     """
#     Test streaming djsql
#     """
#     query = """
#     SELECT default.hard_hat.country,
#     default.hard_hat.city
#     FROM default.hard_hat
#         """

#     response = client_with_query_service.get(
#         "/djsql/stream/",
#         params={"query": query},
#         headers={
#             "Accept": "text/event-stream",
#         },
#         stream=True,
#     )
#     assert response.status_code == 200
