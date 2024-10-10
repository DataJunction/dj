"""Tests for all /sql endpoints that use node SQL build v2"""
# pylint: disable=line-too-long,too-many-lines
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


@pytest.mark.parametrize(
    "metrics, dimensions, filters, orderby, sql, columns, rows",
    [
        # One metric with two measures + one local dimension. Both referenced measures should
        # show up in the generated measures SQL
        (
            ["default.total_repair_order_discounts"],
            ["default.dispatcher.dispatcher_id"],
            [],
            [],
            """
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
                repair_order_details.price *
                  repair_order_details.quantity AS total_repair_cost,
                repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
                repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
              FROM roads.repair_orders AS repair_orders
              JOIN roads.repair_order_details AS repair_order_details
                ON repair_orders.repair_order_id =
                  repair_order_details.repair_order_id
            )
            SELECT
              default_DOT_repair_orders_fact.dispatcher_id
                default_DOT_dispatcher_DOT_dispatcher_id,
              default_DOT_repair_orders_fact.discount
                default_DOT_repair_orders_fact_DOT_discount,
              default_DOT_repair_orders_fact.price
                default_DOT_repair_orders_fact_DOT_price
            FROM default_DOT_repair_orders_fact
            """,
            [
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_dispatcher_DOT_dispatcher_id",
                    "node": "default.dispatcher",
                    "semantic_entity": "default.dispatcher.dispatcher_id",
                    "semantic_type": "dimension",
                    "type": "int",
                },
                {
                    "column": "discount",
                    "name": "default_DOT_repair_orders_fact_DOT_discount",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.discount",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "price",
                    "name": "default_DOT_repair_orders_fact_DOT_price",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price",
                    "semantic_type": "measure",
                    "type": "float",
                },
            ],
            [
                (3, 0.05000000074505806, 63708.0),
                (1, 0.05000000074505806, 67253.0),
                (2, 0.05000000074505806, 66808.0),
                (1, 0.05000000074505806, 18497.0),
                (2, 0.05000000074505806, 76463.0),
                (2, 0.05000000074505806, 87858.0),
                (2, 0.05000000074505806, 63918.0),
                (3, 0.05000000074505806, 21083.0),
                (2, 0.05000000074505806, 74555.0),
                (3, 0.05000000074505806, 27222.0),
                (1, 0.05000000074505806, 73600.0),
                (3, 0.009999999776482582, 54901.0),
                (1, 0.009999999776482582, 51594.0),
                (2, 0.009999999776482582, 65114.0),
                (3, 0.009999999776482582, 48919.0),
                (3, 0.009999999776482582, 70418.0),
                (3, 0.009999999776482582, 29684.0),
                (1, 0.009999999776482582, 62928.0),
                (3, 0.009999999776482582, 97916.0),
                (1, 0.009999999776482582, 44120.0),
                (3, 0.009999999776482582, 53374.0),
                (1, 0.009999999776482582, 87289.0),
                (1, 0.009999999776482582, 92366.0),
                (2, 0.009999999776482582, 47857.0),
                (2, 0.009999999776482582, 68745.0),
            ],
        ),
        # # Two metrics with overlapping measures + one joinable dimension
        (
            [
                "default.total_repair_order_discounts",
                "default.avg_repair_order_discounts",
            ],
            ["default.dispatcher.dispatcher_id"],
            [],
            [],
            """
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
              FROM roads.repair_orders AS repair_orders
              JOIN roads.repair_order_details AS repair_order_details
                ON repair_orders.repair_order_id = repair_order_details.repair_order_id
            )
            SELECT
              default_DOT_repair_orders_fact.dispatcher_id
                default_DOT_dispatcher_DOT_dispatcher_id,
              default_DOT_repair_orders_fact.discount
                default_DOT_repair_orders_fact_DOT_discount,
              default_DOT_repair_orders_fact.price
                default_DOT_repair_orders_fact_DOT_price
            FROM default_DOT_repair_orders_fact
            """,
            [
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_dispatcher_DOT_dispatcher_id",
                    "node": "default.dispatcher",
                    "semantic_entity": "default.dispatcher.dispatcher_id",
                    "semantic_type": "dimension",
                    "type": "int",
                },
                {
                    "column": "discount",
                    "name": "default_DOT_repair_orders_fact_DOT_discount",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.discount",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "price",
                    "name": "default_DOT_repair_orders_fact_DOT_price",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price",
                    "semantic_type": "measure",
                    "type": "float",
                },
            ],
            [
                (3, 0.05000000074505806, 63708.0),
                (1, 0.05000000074505806, 67253.0),
                (2, 0.05000000074505806, 66808.0),
                (1, 0.05000000074505806, 18497.0),
                (2, 0.05000000074505806, 76463.0),
                (2, 0.05000000074505806, 87858.0),
                (2, 0.05000000074505806, 63918.0),
                (3, 0.05000000074505806, 21083.0),
                (2, 0.05000000074505806, 74555.0),
                (3, 0.05000000074505806, 27222.0),
                (1, 0.05000000074505806, 73600.0),
                (3, 0.009999999776482582, 54901.0),
                (1, 0.009999999776482582, 51594.0),
                (2, 0.009999999776482582, 65114.0),
                (3, 0.009999999776482582, 48919.0),
                (3, 0.009999999776482582, 70418.0),
                (3, 0.009999999776482582, 29684.0),
                (1, 0.009999999776482582, 62928.0),
                (3, 0.009999999776482582, 97916.0),
                (1, 0.009999999776482582, 44120.0),
                (3, 0.009999999776482582, 53374.0),
                (1, 0.009999999776482582, 87289.0),
                (1, 0.009999999776482582, 92366.0),
                (2, 0.009999999776482582, 47857.0),
                (2, 0.009999999776482582, 68745.0),
            ],
        ),
        # Two metrics with different measures + two dimensions from different sources
        (
            ["default.avg_time_to_dispatch", "default.total_repair_cost"],
            [
                "default.us_state.state_name",
                "default.dispatcher.company_name",
                "default.hard_hat.last_name",
            ],
            [
                "default.us_state.state_name = 'New Jersey'",
                "default.hard_hat.last_name IN ('Brian')",
            ],
            [],
            """
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
                repair_order_details.price * repair_order_details.quantity
                  AS total_repair_cost,
                repair_orders.dispatched_date - repair_orders.order_date
                  AS time_to_dispatch,
                repair_orders.dispatched_date - repair_orders.required_date
                  AS dispatch_delay
              FROM roads.repair_orders AS repair_orders
              JOIN roads.repair_order_details AS repair_order_details
              ON repair_orders.repair_order_id = repair_order_details.repair_order_id
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
              WHERE default_DOT_hard_hats.last_name IN ('Brian')
            ),
            default_DOT_us_state AS (
              SELECT
                s.state_id,
                s.state_name,
                s.state_abbr AS state_short,
                s.state_region
              FROM roads.us_states AS s
              WHERE  s.state_name = 'New Jersey'
            ),
            default_DOT_dispatcher AS (
              SELECT
                default_DOT_dispatchers.dispatcher_id,
                default_DOT_dispatchers.company_name,
                default_DOT_dispatchers.phone
              FROM roads.dispatchers AS default_DOT_dispatchers
            )
            SELECT
              default_DOT_repair_orders_fact.total_repair_cost
                default_DOT_repair_orders_fact_DOT_total_repair_cost,
              default_DOT_repair_orders_fact.time_to_dispatch
                default_DOT_repair_orders_fact_DOT_time_to_dispatch,
              default_DOT_us_state.state_name
                default_DOT_us_state_DOT_state_name,
              default_DOT_dispatcher.company_name
                default_DOT_dispatcher_DOT_company_name,
              default_DOT_hard_hat.last_name
                default_DOT_hard_hat_DOT_last_name
            FROM default_DOT_repair_orders_fact
            INNER JOIN default_DOT_hard_hat
              ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
            INNER JOIN default_DOT_us_state
              ON default_DOT_hard_hat.state = default_DOT_us_state.state_short
            LEFT JOIN default_DOT_dispatcher
              ON default_DOT_repair_orders_fact.dispatcher_id =default_DOT_dispatcher.dispatcher_id
            """,
            [
                {
                    "column": "total_repair_cost",
                    "name": "default_DOT_repair_orders_fact_DOT_total_repair_cost",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.total_repair_cost",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "time_to_dispatch",
                    "name": "default_DOT_repair_orders_fact_DOT_time_to_dispatch",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.time_to_dispatch",
                    "semantic_type": "measure",
                    "type": "timestamp",
                },
                {
                    "column": "state_name",
                    "name": "default_DOT_us_state_DOT_state_name",
                    "node": "default.us_state",
                    "semantic_entity": "default.us_state.state_name",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "company_name",
                    "name": "default_DOT_dispatcher_DOT_company_name",
                    "node": "default.dispatcher",
                    "semantic_entity": "default.dispatcher.company_name",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "last_name",
                    "name": "default_DOT_hard_hat_DOT_last_name",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.last_name",
                    "semantic_type": "dimension",
                    "type": "string",
                },
            ],
            [
                (92366.0, 204, "New Jersey", "Pothole Pete", "Brian"),
                (44120.0, 196, "New Jersey", "Pothole Pete", "Brian"),
                (18497.0, 146, "New Jersey", "Pothole Pete", "Brian"),
                (63708.0, 150, "New Jersey", "Federal Roads Group", "Brian"),
            ],
        ),
        (
            ["default.avg_time_to_dispatch"],
            ["default.dispatcher.company_name", "default.hard_hat.last_name"],
            ["default.hard_hat.last_name IN ('Brian')"],
            ["default.dispatcher.company_name"],
            """
            WITH default_DOT_repair_orders_fact AS (
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
            default_DOT_dispatcher AS (
            SELECT  default_DOT_dispatchers.dispatcher_id,
                default_DOT_dispatchers.company_name,
                default_DOT_dispatchers.phone
            FROM roads.dispatchers AS default_DOT_dispatchers
            ),
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
            WHERE  default_DOT_hard_hats.last_name IN ('Brian')
            )
            SELECT
              default_DOT_repair_orders_fact.time_to_dispatch default_DOT_repair_orders_fact_DOT_time_to_dispatch,
              default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
              default_DOT_hard_hat.last_name default_DOT_hard_hat_DOT_last_name
            FROM default_DOT_repair_orders_fact
            LEFT JOIN default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
            INNER JOIN default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
            ORDER BY default_DOT_dispatcher.company_name
            """,
            [],
            [
                (150, "Federal Roads Group", "Brian"),
                (146, "Pothole Pete", "Brian"),
                (196, "Pothole Pete", "Brian"),
                (204, "Pothole Pete", "Brian"),
            ],
        ),
        (
            ["default.avg_time_to_dispatch"],
            ["default.dispatcher.company_name", "default.hard_hat.last_name"],
            ["default.hard_hat.last_name IN ('Brian')"],
            ["default.dispatcher.company_name DESC"],
            """
            WITH default_DOT_repair_orders_fact AS (
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
            default_DOT_dispatcher AS (
            SELECT  default_DOT_dispatchers.dispatcher_id,
                default_DOT_dispatchers.company_name,
                default_DOT_dispatchers.phone
            FROM roads.dispatchers AS default_DOT_dispatchers
            ),
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
            WHERE  default_DOT_hard_hats.last_name IN ('Brian')
            )
            SELECT
              default_DOT_repair_orders_fact.time_to_dispatch default_DOT_repair_orders_fact_DOT_time_to_dispatch,
              default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
              default_DOT_hard_hat.last_name default_DOT_hard_hat_DOT_last_name
            FROM default_DOT_repair_orders_fact
            LEFT JOIN default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
            INNER JOIN default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
            ORDER BY default_DOT_dispatcher.company_name DESC
            """,
            [],
            [
                (146, "Pothole Pete", "Brian"),
                (196, "Pothole Pete", "Brian"),
                (204, "Pothole Pete", "Brian"),
                (150, "Federal Roads Group", "Brian"),
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_measures_sql_with_filters__v2(  # pylint: disable=too-many-arguments
    metrics,
    dimensions,
    filters,
    orderby,
    sql,
    columns,
    rows,
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,  # pylint: disable=c-extension-no-member
):
    """
    Test ``GET /sql/measures`` with various metrics, filters, and dimensions.
    """
    await fix_dimension_links(module__client_with_roads)
    sql_params = {
        "metrics": metrics,
        "dimensions": dimensions,
        "filters": filters,
        **({"orderby": orderby} if orderby else {}),
    }
    response = await module__client_with_roads.get(
        "/sql/measures/v2",
        params=sql_params,
    )
    data = response.json()
    translated_sql = data[0]
    assert str(parse(str(sql))) == str(parse(str(translated_sql["sql"])))
    result = duckdb_conn.sql(translated_sql["sql"])
    assert set(result.fetchall()) == set(rows)
    if columns:
        assert translated_sql["columns"] == columns


@pytest.mark.asyncio
async def test_measures_sql_include_all_columns(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,  # pylint: disable=c-extension-no-member
):
    """
    Test ``GET /sql/measures/v2`` with include_all_columns set to true.
    """
    await fix_dimension_links(module__client_with_roads)

    response = await module__client_with_roads.get(
        "/sql/measures/v2",
        params={
            "metrics": ["default.avg_time_to_dispatch"],
            "dimensions": [
                "default.us_state.state_name",
                "default.dispatcher.company_name",
                "default.hard_hat.last_name",
            ],
            "filters": [
                "default.us_state.state_name = 'New Jersey'",
                "default.hard_hat.last_name IN ('Brian')",
            ],
            "include_all_columns": True,
        },
    )
    data = response.json()
    translated_sql = data[0]

    expected_sql = """
    WITH default_DOT_repair_orders_fact AS (
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
     WHERE  default_DOT_hard_hats.last_name IN ('Brian')
    ),
    default_DOT_us_state AS (
    SELECT  s.state_id,
        s.state_name,
        s.state_abbr AS state_short,
        s.state_region
     FROM roads.us_states AS s
     WHERE  s.state_name = 'New Jersey'
    ),
    default_DOT_dispatcher AS (
    SELECT  default_DOT_dispatchers.dispatcher_id,
        default_DOT_dispatchers.company_name,
        default_DOT_dispatchers.phone
     FROM roads.dispatchers AS default_DOT_dispatchers
    )
    SELECT  default_DOT_repair_orders_fact.repair_order_id default_DOT_repair_orders_fact_DOT_repair_order_id,
        default_DOT_repair_orders_fact.municipality_id default_DOT_repair_orders_fact_DOT_municipality_id,
        default_DOT_repair_orders_fact.hard_hat_id default_DOT_repair_orders_fact_DOT_hard_hat_id,
        default_DOT_repair_orders_fact.dispatcher_id default_DOT_repair_orders_fact_DOT_dispatcher_id,
        default_DOT_repair_orders_fact.order_date default_DOT_repair_orders_fact_DOT_order_date,
        default_DOT_repair_orders_fact.dispatched_date default_DOT_repair_orders_fact_DOT_dispatched_date,
        default_DOT_repair_orders_fact.required_date default_DOT_repair_orders_fact_DOT_required_date,
        default_DOT_repair_orders_fact.discount default_DOT_repair_orders_fact_DOT_discount,
        default_DOT_repair_orders_fact.price default_DOT_repair_orders_fact_DOT_price,
        default_DOT_repair_orders_fact.quantity default_DOT_repair_orders_fact_DOT_quantity,
        default_DOT_repair_orders_fact.repair_type_id default_DOT_repair_orders_fact_DOT_repair_type_id,
        default_DOT_repair_orders_fact.total_repair_cost default_DOT_repair_orders_fact_DOT_total_repair_cost,
        default_DOT_repair_orders_fact.time_to_dispatch default_DOT_repair_orders_fact_DOT_time_to_dispatch,
        default_DOT_repair_orders_fact.dispatch_delay default_DOT_repair_orders_fact_DOT_dispatch_delay,
        default_DOT_us_state.state_name default_DOT_us_state_DOT_state_name,
        default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
        default_DOT_hard_hat.last_name default_DOT_hard_hat_DOT_last_name
     FROM default_DOT_repair_orders_fact INNER JOIN default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
    INNER JOIN default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_short
    LEFT JOIN default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
    """
    assert str(parse(str(expected_sql))) == str(parse(str(translated_sql["sql"])))
    result = duckdb_conn.sql(translated_sql["sql"])
    assert len(result.fetchall()) == 4


@pytest.mark.asyncio
async def test_measures_sql_errors(
    module__client_with_roads: AsyncClient,
):
    """
    Test ``GET /sql/measures/v2`` with include_all_columns set to true.
    """
    await fix_dimension_links(module__client_with_roads)

    response = await module__client_with_roads.get(
        "/sql/measures/v2",
        params={
            "metrics": ["default.avg_time_to_dispatch"],
            "dimensions": [
                "default.hard_hat.last_name",
            ],
            "filters": [
                "default.us_state.state_name = 'New Jersey'",
                "default.hard_hat.last_name IN ('Brian')",
            ],
            "orderby": ["default.dispatcher.company_name"],
        },
    )
    data = response.json()
    assert data[0]["errors"] == [
        {
            "code": 208,
            "message": "['default.dispatcher.company_name'] is not a valid ORDER BY request",
            "debug": {
                "node_revision": "default.repair_orders_fact",
                "filters": [
                    "default.us_state.state_name = 'New Jersey'",
                    "default.hard_hat.last_name IN ('Brian')",
                ],
                "required_dimensions": [],
                "dimensions": ["default.hard_hat.last_name"],
                "orderby": ["default.dispatcher.company_name"],
                "limit": None,
                "ignore_errors": True,
                "build_criteria": {
                    "timestamp": None,
                    "dialect": "spark",
                    "target_node_name": None,
                },
            },
            "context": "",
        },
    ]


@pytest.mark.asyncio
async def test_metrics_sql_different_parents(
    module__client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,  # pylint: disable=c-extension-no-member
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
