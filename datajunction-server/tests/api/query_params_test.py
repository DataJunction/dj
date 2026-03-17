import pytest
from httpx import AsyncClient
import pytest_asyncio
from datajunction_server.sql.parsing.backends.antlr4 import parse
from tests.construction.build_v3 import assert_sql_equal


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


@pytest_asyncio.fixture(scope="module")
async def module__client_with_query_params_v3(
    module__client_with_query_params: AsyncClient,
) -> AsyncClient:
    """
    Fixture that adds a metric using the parameterized hh_id column so that
    v3 SQL generation actually projects it (and thus substitutes the param).
    """
    response = await module__client_with_query_params.post(
        "/nodes/metric/",
        json={
            "name": "default.sum_hh_id",
            "description": "Sum of hh_id (parameterized column)",
            "query": "SELECT SUM(hh_id) FROM default.repair_orders_fact",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), response.json()
    return module__client_with_query_params


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


@pytest.mark.asyncio
async def test_query_parameters_measures_sql_v3(
    module__client_with_query_params_v3: AsyncClient,
):
    """
    Test using query parameters in /sql/measures/v3/ - parameter substitution
    should replace :param_name placeholders in grain group SQL.

    Uses default.sum_hh_id which aggregates the hh_id column (the parameterized
    column), ensuring it is projected in the v3 CTE and the substitution is visible.
    """
    response = await module__client_with_query_params_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["default.sum_hh_id"],
            "dimensions": ["default.hard_hat.hard_hat_id"],
            "query_params": '{"default.hard_hat.hard_hat_id": 123}',
        },
    )
    assert response.status_code == 200, response.json()
    grain_group_sql = response.json()["grain_groups"][0]["sql"]
    assert_sql_equal(
        grain_group_sql,
        """
        WITH default_repair_orders_fact AS (
          SELECT
            CAST(123 AS INT) AS hh_id,
            hard_hat_id
          FROM default.roads.repair_orders repair_orders
        )
        SELECT
          t1.hard_hat_id,
          SUM(t1.hh_id) hh_id_sum_HASH
        FROM default_repair_orders_fact t1
        GROUP BY t1.hard_hat_id
        """,
        normalize_aliases=True,
    )

    # Without query_params, missing param raises a 422 error
    response = await module__client_with_query_params_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["default.sum_hh_id"],
            "dimensions": ["default.hard_hat.hard_hat_id"],
        },
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Missing value for query parameter: default.hard_hat.hard_hat_id"
    )


@pytest.mark.asyncio
async def test_query_parameters_metrics_sql_v3(
    module__client_with_query_params_v3: AsyncClient,
):
    """
    Test using query parameters in /sql/metrics/v3/ - parameter substitution
    should replace :param_name placeholders in the final combined SQL.
    """
    response = await module__client_with_query_params_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["default.sum_hh_id"],
            "dimensions": ["default.hard_hat.hard_hat_id"],
            "query_params": '{"default.hard_hat.hard_hat_id": 123}',
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    assert_sql_equal(
        sql,
        """
        WITH default_repair_orders_fact AS (
          SELECT
            CAST(123 AS INT) AS hh_id,
            hard_hat_id
          FROM default.roads.repair_orders repair_orders
        ),
        repair_orders_fact_0 AS (
          SELECT
            t1.hard_hat_id,
            SUM(t1.hh_id) hh_id_sum_HASH
          FROM default_repair_orders_fact t1
          GROUP BY t1.hard_hat_id
        )
        SELECT
          repair_orders_fact_0.hard_hat_id AS hard_hat_id,
          SUM(repair_orders_fact_0.hh_id_sum_HASH) AS sum_hh_id
        FROM repair_orders_fact_0
        GROUP BY repair_orders_fact_0.hard_hat_id
        """,
        normalize_aliases=True,
    )

    # Without query_params, missing param raises a 422 error
    response = await module__client_with_query_params_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["default.sum_hh_id"],
            "dimensions": ["default.hard_hat.hard_hat_id"],
        },
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Missing value for query parameter: default.hard_hat.hard_hat_id"
    )
