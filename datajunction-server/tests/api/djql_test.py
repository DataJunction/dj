"""
Tests for the djsql API.
"""
import pytest
from httpx import AsyncClient

from tests.sql.utils import assert_query_strings_equal, compare_query_strings

# pylint: disable=too-many-lines,C0301


@pytest.mark.asyncio
@pytest.mark.skip(reason="Will move djsql to new sql build later")
async def test_get_djsql_data_only_nodes_query(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test djsql with just some non-metric nodes
    """

    query = """
SELECT default.hard_hat.country,
  default.hard_hat.city
FROM default.hard_hat
    """

    response = await module__client_with_roads.get(
        "/djsql/data/",
        params={"query": query},
    )
    data = response.json()["results"][0]["rows"]
    assert data == [
        ["USA", "Jersey City"],
        ["USA", "Middletown"],
        ["USA", "Billerica"],
        ["USA", "Southampton"],
        ["USA", "Southgate"],
        ["USA", "Powder Springs"],
        ["USA", "Niagara Falls"],
        ["USA", "Phoenix"],
        ["USA", "Muskogee"],
    ]
    query = response.json()["results"][0]["sql"]
    expected_query = """WITH
    node_query_0 AS (SELECT  default_DOT_hard_hat.hard_hat_id,
        default_DOT_hard_hat.last_name,
        default_DOT_hard_hat.first_name,
        default_DOT_hard_hat.title,
        default_DOT_hard_hat.birth_date,
        default_DOT_hard_hat.hire_date,
        default_DOT_hard_hat.address,
        default_DOT_hard_hat.city,
        default_DOT_hard_hat.state,
        default_DOT_hard_hat.postal_code,
        default_DOT_hard_hat.country,
        default_DOT_hard_hat.manager,
        default_DOT_hard_hat.contractor_id
     FROM (SELECT  default_DOT_hard_hats.hard_hat_id,
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
     FROM roads.hard_hats AS default_DOT_hard_hats)
     AS default_DOT_hard_hat

    )

    SELECT  node_query_0.country,
        node_query_0.city
     FROM node_query_0"""
    assert compare_query_strings(query, expected_query)


@pytest.mark.asyncio
@pytest.mark.skip(reason="Will move djsql to new sql build later")
async def test_get_djsql_data_only_nested_metrics(
    module__client_with_roads: AsyncClient,
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

    response = await module__client_with_roads.get(
        "/djsql/data/",
        params={"query": query},
    )
    rows = response.json()["results"][0]["rows"]
    assert rows == [
        [54672.75, "Jersey City"],
        [76555.33333333333, "Billerica"],
        [64190.6, "Southgate"],
        [65682.0, "Phoenix"],
        [54083.5, "Southampton"],
    ]

    query = response.json()["results"][0]["sql"]
    expected_query = """WITH
    metric_query_0 AS (SELECT  default_DOT_repair_orders_fact.default_DOT_avg_repair_price,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_country,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_city
     FROM (SELECT  default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
        default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
        avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price
     FROM (SELECT  repair_orders.repair_order_id,
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
     FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
     AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
        default_DOT_hard_hats.city,
        default_DOT_hard_hats.state,
        default_DOT_hard_hats.country
     FROM roads.hard_hats AS default_DOT_hard_hats)
     AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
    GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.city) AS default_DOT_repair_orders_fact

    LIMIT 5)

    SELECT  Sum(avg_repair_price),
        city
     FROM (SELECT  metric_query_0.default_DOT_avg_repair_price AS avg_repair_price,
        metric_query_0.default_DOT_hard_hat_DOT_country AS country,
        metric_query_0.default_DOT_hard_hat_DOT_city AS city
     FROM metric_query_0)
     GROUP BY  city"""
    assert_query_strings_equal(query, expected_query)


@pytest.mark.asyncio
@pytest.mark.skip(reason="Will move djsql to new sql build later")
async def test_get_djsql_data_only_multiple_metrics(
    module__client_with_roads: AsyncClient,
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

    response = await module__client_with_roads.get(
        "/djsql/data/",
        params={"query": query},
    )
    data = response.json()["results"][0]["rows"]
    assert data == [
        [54672.75, 218691.0, "USA", "Jersey City"],
        [76555.33333333333, 229666.0, "USA", "Billerica"],
        [64190.6, 320953.0, "USA", "Southgate"],
        [65682.0, 131364.0, "USA", "Phoenix"],
        [54083.5, 216334.0, "USA", "Southampton"],
        [65595.66666666667, 196787.0, "USA", "Powder Springs"],
        [39301.5, 78603.0, "USA", "Middletown"],
        [70418.0, 70418.0, "USA", "Muskogee"],
        [53374.0, 53374.0, "USA", "Niagara Falls"],
    ]
    query = response.json()["results"][0]["sql"]
    expected_query = """WITH
    metric_query_0 AS (SELECT  default_DOT_repair_orders_fact.default_DOT_avg_repair_price,
        default_DOT_repair_orders_fact.default_DOT_total_repair_cost,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_country,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_city
     FROM (SELECT  default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
        default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
        avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
        sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
     FROM (SELECT  repair_orders.repair_order_id,
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
     FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
     AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
        default_DOT_hard_hats.city,
        default_DOT_hard_hats.state,
        default_DOT_hard_hats.country
     FROM roads.hard_hats AS default_DOT_hard_hats)
     AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
     GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.city) AS default_DOT_repair_orders_fact)

    SELECT  metric_query_0.default_DOT_avg_repair_price AS avg_repair_price,
        metric_query_0.default_DOT_total_repair_cost AS total_cost,
        metric_query_0.default_DOT_hard_hat_DOT_country,
        metric_query_0.default_DOT_hard_hat_DOT_city
     FROM metric_query_0"""
    assert compare_query_strings(query, expected_query)


@pytest.mark.asyncio
async def test_get_djsql_metric_table_exception(
    module__client_with_roads: AsyncClient,
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

    response = await module__client_with_roads.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert (
        response.json()["message"]
        == "Any SELECT referencing a Metric must source from a single unaliased Table named `metrics`."
    )


@pytest.mark.asyncio
async def test_get_djsql_illegal_clause_metric_query(
    module__client_with_roads: AsyncClient,
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

    response = await module__client_with_roads.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert (
        response.json()["message"]
        == "HAVING, LATERAL VIEWS, and SET OPERATIONS are not allowed on `metrics` queries."
    )


@pytest.mark.asyncio
async def test_get_djsql_illegal_column_expression(
    module__client_with_roads: AsyncClient,
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

    response = await module__client_with_roads.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert (
        response.json()["message"]
        == "Only direct Columns are allowed in `metrics` queries, found `default.hard_hat.id + 5`."
    )


@pytest.mark.asyncio
async def test_get_djsql_illegal_column(
    module__client_with_roads: AsyncClient,
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

    response = await module__client_with_roads.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert (
        response.json()["message"]
        == "You can only select direct METRIC nodes or a column from your GROUP BY on `metrics` queries, found `default.repair_orders.id`"
    )


@pytest.mark.asyncio
async def test_get_djsql_illegal_limit(
    module__client_with_roads: AsyncClient,
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

    response = await module__client_with_roads.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert (
        response.json()["message"]
        == "LIMITs on `metrics` queries can only be integers not `1 + 2`."
    )


@pytest.mark.asyncio
async def test_get_djsql_no_nodes(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test djsql without dj node refs
    """

    query = """
        SELECT 1
    """

    response = await module__client_with_roads.get(
        "/djsql/data/",
        params={"query": query},
    )
    assert response.json()["message"].startswith("Found no dj nodes in query")


@pytest.mark.asyncio
async def test_djsql_stream(
    module__client: AsyncClient,
) -> None:
    """
    Test streaming djsql
    """
    query = """
    SELECT 1
        """

    response = await module__client.get(
        "/djsql/stream/",
        params={"query": query},
    )
    assert response.status_code == 500
    assert response.json()["message"].startswith("Found no dj nodes in query")
