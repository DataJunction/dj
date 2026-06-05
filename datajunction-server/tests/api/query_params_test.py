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
    assert_sql_equal(
        response.json()["sql"],
        """
        WITH default_repair_orders_fact AS (
          SELECT  CAST(123 AS INT) AS hh_id,
            hard_hat_id,
            repair_order_id
          FROM default.roads.repair_orders repair_orders
        )
        SELECT  t1.hh_id,
            t1.repair_order_id,
            t1.hard_hat_id
        FROM default_repair_orders_fact t1
        """,
    )

    # v3 leaves missing parameter references as ``:`name``` placeholders
    # in the rendered SQL rather than raising "Missing value for parameter"
    # — the placeholder is preserved so downstream engines that do their
    # own param binding can still receive the rendered SQL. (v2 raised
    # eagerly; the behavior change is intentional in the unification.)
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
    assert response.status_code == 200
    assert "`default.hard_hat.hard_hat_id`" in response.json()["sql"]


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
            "code": "MISSING_PARAMETER",
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
            "code": "MISSING_PARAMETER",
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


@pytest.mark.asyncio
async def test_query_parameter_in_dimension_link_join_condition(
    client_with_service_setup,
):
    """
    Test that query parameters are substituted in dimension link join conditions.

    The FX conversion use case: the destination currency is a query-time parameter
    so the same metric can be reported in USD, EUR, etc. without separate metrics.
    The dimension link join_on contains :`reporting_currency` which should be
    substituted with the supplied value before SQL generation.
    """
    client = client_with_service_setup

    # FX rate source
    r = await client.post(
        "/nodes/source/",
        json={
            "name": "default.fx_test_rates_source",
            "catalog": "default",
            "schema_": "finance",
            "table": "fx_rates",
            "columns": [
                {"name": "from_currency", "type": "string"},
                {"name": "to_currency", "type": "string"},
                {"name": "rate", "type": "double"},
                {"name": "rate_date", "type": "date"},
            ],
            "mode": "published",
        },
    )
    assert r.status_code == 200, r.json()

    # FX rate dimension
    r = await client.post(
        "/nodes/dimension/",
        json={
            "name": "default.fx_test_rates",
            "query": "SELECT from_currency, to_currency, rate, rate_date FROM default.fx_test_rates_source",
            "primary_key": ["from_currency", "to_currency", "rate_date"],
            "mode": "published",
        },
    )
    assert r.status_code == 201, r.json()

    # Revenue source
    r = await client.post(
        "/nodes/source/",
        json={
            "name": "default.fx_test_rev_source",
            "catalog": "default",
            "schema_": "finance",
            "table": "revenue",
            "columns": [
                {"name": "amount", "type": "double"},
                {"name": "transaction_currency", "type": "string"},
                {"name": "txn_date", "type": "date"},
            ],
            "mode": "published",
        },
    )
    assert r.status_code == 200, r.json()

    # Revenue transform
    r = await client.post(
        "/nodes/transform/",
        json={
            "name": "default.fx_test_revenue",
            "query": "SELECT amount, transaction_currency, txn_date FROM default.fx_test_rev_source",
            "mode": "published",
        },
    )
    assert r.status_code == 201, r.json()

    # Dimension link with parameterized destination currency in join_on
    r = await client.post(
        "/nodes/default.revenue/link/",
        json={
            "dimension_node": "default.fx_test_rates",
            "join_type": "left",
            "join_on": (
                "default.revenue.transaction_currency = default.fx_rates.from_currency "
                "AND default.revenue.txn_date = default.fx_rates.rate_date "
                "AND default.fx_rates.to_currency = :`reporting_currency`"
            ),
        },
    )
    assert r.status_code == 201, r.json()

    # Metric: revenue converted to the reporting currency
    r = await client.post(
        "/nodes/metric/",
        json={
            "name": "default.fx_test_revenue_converted",
            "description": "Revenue converted to reporting currency via FX rates",
            "query": "SELECT SUM(default.revenue.amount * default.fx_rates.rate)",
            "mode": "published",
        },
    )
    assert r.status_code == 201, r.json()

    # With query_params: :`reporting_currency` should be substituted with 'USD'
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["default.fx_test_revenue_converted"],
            "dimensions": ["default.fx_rates.to_currency"],
            "query_params": '{"reporting_currency": "USD"}',
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    assert "'USD'" in sql or '"USD"' in sql, f"Expected 'USD' in SQL:\n{sql}"
    assert "`reporting_currency`" not in sql

    # Without query_params: should error on the missing parameter
    response_no_param = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["default.fx_test_revenue_converted"],
            "dimensions": ["default.fx_rates.to_currency"],
        },
    )
    assert response_no_param.status_code == 422
    assert "reporting_currency" in response_no_param.json()["message"]


@pytest.mark.asyncio
async def test_query_parameter_in_dimension_link_join_condition(
    client_with_service_setup,
):
    """
    Test that query parameters are substituted in dimension link join conditions.

    The FX conversion use case: the destination currency is a query-time parameter
    so the same metric can be reported in USD, EUR, etc. without separate metrics.
    The dimension link join_on contains : which should be
    substituted with the supplied value before SQL generation.
    """
    client = client_with_service_setup

    for r in [
        await client.post(
            "/nodes/source/",
            json={
                "name": "default.fx_src",
                "catalog": "default",
                "schema_": "finance",
                "table": "fx_rates",
                "columns": [
                    {"name": "from_currency", "type": "string"},
                    {"name": "to_currency", "type": "string"},
                    {"name": "rate", "type": "double"},
                    {"name": "rate_date", "type": "date"},
                ],
                "mode": "published",
            },
        ),
        await client.post(
            "/nodes/dimension/",
            json={
                "name": "default.fx_dim",
                "query": "SELECT from_currency, to_currency, rate, rate_date FROM default.fx_src",
                "primary_key": ["from_currency", "to_currency", "rate_date"],
                "mode": "published",
            },
        ),
        await client.post(
            "/nodes/source/",
            json={
                "name": "default.rev_src",
                "catalog": "default",
                "schema_": "finance",
                "table": "revenue",
                "columns": [
                    {"name": "amount", "type": "double"},
                    {"name": "transaction_currency", "type": "string"},
                    {"name": "txn_date", "type": "date"},
                ],
                "mode": "published",
            },
        ),
        await client.post(
            "/nodes/transform/",
            json={
                "name": "default.rev_transform",
                "query": "SELECT amount, transaction_currency, txn_date FROM default.rev_src",
                "mode": "published",
            },
        ),
        await client.post(
            "/nodes/default.rev_transform/link/",
            json={
                "dimension_node": "default.fx_dim",
                "join_type": "left",
                "join_on": (
                    "default.rev_transform.transaction_currency = default.fx_dim.from_currency "
                    "AND default.rev_transform.txn_date = default.fx_dim.rate_date "
                    "AND default.fx_dim.to_currency = :"
                ),
            },
        ),
        await client.post(
            "/nodes/metric/",
            json={
                "name": "default.rev_converted",
                "description": "Revenue converted to reporting currency via FX rates",
                "query": "SELECT SUM(default.rev_transform.amount * default.fx_dim.rate)",
                "mode": "published",
            },
        ),
    ]:
        assert r.status_code in (200, 201), r.json()

    # With query_params: : should be substituted with 'USD'
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["default.rev_converted"],
            "dimensions": ["default.fx_dim.to_currency"],
            "query_params": '{"reporting_currency": "USD"}',
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    assert "'USD'" in sql or '"USD"' in sql, sql
    assert "`reporting_currency`" not in sql

    # Without query_params: should error on missing parameter
    response_no_param = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["default.rev_converted"],
            "dimensions": ["default.fx_dim.to_currency"],
        },
    )
    assert response_no_param.status_code == 422
    assert "reporting_currency" in response_no_param.json()["message"]
