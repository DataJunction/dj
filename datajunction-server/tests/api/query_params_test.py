import pytest
from httpx import AsyncClient
import pytest_asyncio
from datajunction_server.sql.parsing.backends.antlr4 import parse


@pytest_asyncio.fixture(scope="module")
async def module__client_with_query_params(
    module__client_with_roads: AsyncClient,
) -> AsyncClient:
    """
    Fixture to create a transform with query parameters
    """
    response = await module__client_with_roads.patch(
        "/nodes/default.repair_orders_fact",
        json={
            "query": "SELECT CAST(:`default.hard_hat.hard_hat_id` AS INT) AS hh_id,"
            "hard_hat_id, repair_order_id FROM default.repair_orders repair_orders",
        },
    )
    assert response.status_code == 200
    return module__client_with_roads


@pytest.mark.asyncio
async def test_query_parameters_node_sql(
    module__client_with_query_params: AsyncClient,
):
    """
    Test using query parameters in the SQL query
    """
    response = await module__client_with_query_params.get(
        "/sql/default.repair_orders_fact",
        params={
            "dimensions": ["default.hard_hat.hard_hat_id"],
            "query_params": '{"default.hard_hat.hard_hat_id": 123}',
        },
    )
    assert str(parse(response.json()["sql"])) == str(
        parse(
            """
            WITH default_DOT_repair_orders_fact AS (
              SELECT
                CAST(123 AS INT) AS hh_id,
                repair_orders.hard_hat_id,
                repair_orders.repair_order_id
              FROM roads.repair_orders AS repair_orders
            )
            SELECT
              default_DOT_repair_orders_fact.hh_id default_DOT_repair_orders_fact_DOT_hh_id,
              default_DOT_repair_orders_fact.hard_hat_id default_DOT_hard_hat_DOT_hard_hat_id,
              default_DOT_repair_orders_fact.repair_order_id default_DOT_repair_orders_fact_DOT_repair_order_id
            FROM default_DOT_repair_orders_fact
            """,
        ),
    )

    response = await module__client_with_query_params.get(
        "/sql/default.repair_orders_fact",
        params={
            "dimensions": ["default.hard_hat.hard_hat_id"],
            "filters": [
                "default.hard_hat.hard_hat_id = 123",
            ],
            "ignore_errors": False,
        },
    )
    assert (
        response.json()["message"]
        == "Missing value for parameter: default.hard_hat.hard_hat_id"
    )


@pytest.mark.asyncio
async def test_query_parameters_measures_sql(
    module__client_with_query_params: AsyncClient,
):
    """
    Test using query parameters in the SQL query
    """
    response = await module__client_with_query_params.get(
        "/sql/measures/v2",
        params={
            "metrics": ["default.num_repair_orders"],
            "dimensions": ["default.hard_hat.hard_hat_id"],
            "query_params": '{"default.hard_hat.hard_hat_id": 123}',
        },
    )
    assert str(parse(response.json()[0]["sql"])) == str(
        parse(
            """
            WITH default_DOT_repair_orders_fact AS (
              SELECT
                CAST(123 AS INT) AS hh_id,
                repair_orders.hard_hat_id,
                repair_orders.repair_order_id
              FROM roads.repair_orders AS repair_orders
            )
            SELECT
              default_DOT_repair_orders_fact.hard_hat_id default_DOT_hard_hat_DOT_hard_hat_id,
              default_DOT_repair_orders_fact.repair_order_id default_DOT_repair_orders_fact_DOT_repair_order_id
            FROM default_DOT_repair_orders_fact
            """,
        ),
    )

    response = await module__client_with_query_params.get(
        "/sql/measures/v2",
        params={
            "metrics": ["default.num_repair_orders"],
            "dimensions": ["default.hard_hat.hard_hat_id"],
        },
    )
    assert response.json()[0]["errors"] == [
        {
            "code": 303,
            "message": "Missing value for parameter: default.hard_hat.hard_hat_id",
            "debug": None,
            "context": "",
        },
    ]


@pytest.mark.asyncio
async def test_query_parameters_metrics_sql(
    module__client_with_query_params: AsyncClient,
):
    """
    Test using query parameters in the SQL query
    """
    response = await module__client_with_query_params.get(
        "/sql",
        params={
            "metrics": ["default.num_repair_orders"],
            "dimensions": ["default.hard_hat.hard_hat_id"],
            "query_params": '{"default.hard_hat.hard_hat_id": 123}',
        },
    )
    assert str(parse(response.json()["sql"])) == str(
        parse(
            """
            WITH default_DOT_repair_orders_fact AS (
              SELECT
                CAST(123 AS INT) AS hh_id,
                repair_orders.hard_hat_id,
                repair_orders.repair_order_id
              FROM roads.repair_orders AS repair_orders
            ),
            default_DOT_repair_orders_fact_metrics AS (
              SELECT
                default_DOT_repair_orders_fact.hard_hat_id default_DOT_hard_hat_DOT_hard_hat_id,
                count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders
              FROM default_DOT_repair_orders_fact
              GROUP BY  default_DOT_repair_orders_fact.hard_hat_id
            )
            SELECT
              default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_hard_hat_id,
              default_DOT_repair_orders_fact_metrics.default_DOT_num_repair_orders
            FROM default_DOT_repair_orders_fact_metrics
            """,
        ),
    )

    response = await module__client_with_query_params.get(
        "/sql",
        params={
            "metrics": ["default.num_repair_orders"],
            "dimensions": ["default.hard_hat.hard_hat_id"],
            "ignore_errors": False,
        },
    )
    assert response.json()["errors"] == [
        {
            "code": 303,
            "message": "Missing value for parameter: default.hard_hat.hard_hat_id",
            "debug": None,
            "context": "",
        },
    ]
