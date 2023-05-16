"""
Tests for the cubes API.
"""

from fastapi.testclient import TestClient

from tests.sql.utils import compare_query_strings


def test_read_cube(client_with_examples: TestClient) -> None:
    """
    Test ``GET /cubes/{name}``.
    """
    # Create a cube
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.number_of_account_types"],
            "dimensions": ["default.account_type.account_type_name"],
            "filters": [],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "default.number_of_accounts_by_account_type",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["version"] == "v1.0"
    assert data["type"] == "cube"
    assert data["name"] == "default.number_of_accounts_by_account_type"
    assert data["display_name"] == "Default: Number Of Accounts By Account Type"

    # Read the cube
    response = client_with_examples.get(
        "/cubes/default.number_of_accounts_by_account_type",
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "cube"
    assert data["name"] == "default.number_of_accounts_by_account_type"
    assert data["display_name"] == "Default: Number Of Accounts By Account Type"
    assert data["version"] == "v1.0"
    assert data["description"] == "A cube of number of accounts grouped by account type"
    assert compare_query_strings(
        data["query"],
        """
        SELECT
          default_DOT_account_type.account_type_name,
          count(default_DOT_account_type.id) default_DOT_number_of_account_types
        FROM (
          SELECT
            default_DOT_account_type_table.account_type_classification,
            default_DOT_account_type_table.account_type_name,
            default_DOT_account_type_table.id
          FROM accounting.account_type_table AS default_DOT_account_type_table)
          AS default_DOT_account_type
          GROUP BY
            default_DOT_account_type.account_type_name
        """,
    )


def test_create_invalid_cube(client_with_examples: TestClient):
    """
    Check that creating a cube with a query fails appropriately
    """
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "query": "SELECT 1",
            "cube_elements": [
                "default.number_of_account_types",
                "default.account_type",
            ],
            "name": "default.cubes_shouldnt_have_queries",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data["detail"] == [
        {
            "loc": ["body", "metrics"],
            "msg": "field required",
            "type": "value_error.missing",
        },
        {
            "loc": ["body", "dimensions"],
            "msg": "field required",
            "type": "value_error.missing",
        },
    ]

    # Check that creating a cube with no cube elements fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.account_type"],
            "dimensions": ["default.account_type.account_type_name"],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "default.cubes_must_have_elements",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "Node default.account_type of type dimension cannot be added to a cube. "
        "Did you mean to add a dimension attribute?",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with incompatible nodes fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.number_of_account_types"],
            "dimensions": ["default.payment_type.payment_type_name"],
            "description": "",
            "mode": "published",
            "name": "default.cubes_cant_use_source_nodes",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "The dimension attribute `default.payment_type.payment_type_name` is not "
        "available on every metric and thus cannot be included.",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with no metric nodes fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": [],
            "dimensions": ["default.account_type.account_type_name"],
            "description": "",
            "mode": "published",
            "name": "default.cubes_must_have_metrics",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "At least one metric is required",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with no dimension nodes fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.number_of_account_types"],
            "dimensions": [],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "default.cubes_must_have_dimensions",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "At least one dimension is required",
        "errors": [],
        "warnings": [],
    }


def test_raise_on_cube_with_multiple_catalogs(
    client_with_examples: TestClient,
) -> None:
    """
    Test raising when creating a cube with multiple catalogs
    """
    # Create a cube
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.number_of_account_types", "basic.num_comments"],
            "dimensions": ["default.account_type.account_type_name"],
            "description": "multicatalog cube's raise an error",
            "mode": "published",
            "name": "default.multicatalog",
        },
    )
    assert not response.ok
    data = response.json()
    assert "Metrics and dimensions cannot be from multiple catalogs" in data["message"]


def test_cube_sql(client_with_examples: TestClient):
    """
    Test that the generated cube materialization SQL makes sense
    """
    metrics_list = [
        "default.discounted_orders_rate",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.total_repair_order_discounts",
    ]
    # Should fail because dimension attribute isn't available
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": metrics_list,
            "dimensions": [
                "default.contractor.company_name",
            ],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.repairs_cube",
        },
    )
    assert response.json()["message"] == (
        "The dimension attribute `default.contractor.company_name` is not available "
        "on every metric and thus cannot be included."
    )

    # Metric that doubles the total repair cost to test the sum(x) + sum(y) scenario
    response = client_with_examples.post(
        "/nodes/metric/",
        json={
            "description": "Double total repair cost",
            "query": (
                "SELECT sum(price) + sum(price) as default_DOT_double_total_repair_cost "
                "FROM default.repair_order_details"
            ),
            "mode": "published",
            "name": "default.double_total_repair_cost",
        },
    )
    assert response.ok
    # Should succeed
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": metrics_list + ["default.double_total_repair_cost"],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.postal_code",
                "default.hard_hat.city",
                "default.hard_hat.state",
                "default.dispatcher.company_name",
                "default.municipality_dim.local_region",
            ],
            "filters": ["default.hard_hat.state='AZ'"],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.repairs_cube",
        },
    )
    results = response.json()

    assert results["name"] == "default.repairs_cube"
    assert results["display_name"] == "Default: Repairs Cube"
    assert results["description"] == "Cube of various metrics related to repairs"
    expected_query = """
        SELECT  default_DOT_dispatcher.company_name,
                default_DOT_hard_hat.city,
                default_DOT_hard_hat.country,
                default_DOT_hard_hat.postal_code,
                default_DOT_hard_hat.state,
                default_DOT_municipality_dim.local_region,
                sum(default_DOT_repair_order_details.price) + sum(default_DOT_repair_order_details.price) AS default_DOT_double_total_repair_cost,
                count(default_DOT_repair_orders.repair_order_id) default_DOT_num_repair_orders,
                CAST(sum(if(default_DOT_repair_order_details.discount > 0.0, 1, 0)) AS DOUBLE) / count(*) AS default_DOT_discounted_orders_rate,
                sum(default_DOT_repair_order_details.price * default_DOT_repair_order_details.discount) default_DOT_total_repair_order_discounts,
                avg(default_DOT_repair_order_details.price) AS default_DOT_avg_repair_price,
                sum(default_DOT_repair_order_details.price) default_DOT_total_repair_cost
        FROM roads.repair_order_details AS default_DOT_repair_order_details LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.repair_order_id
        FROM roads.repair_orders AS default_DOT_repair_orders) AS default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
        LEFT OUTER JOIN (SELECT  default_DOT_dispatchers.company_name,
                default_DOT_dispatchers.dispatcher_id
        FROM roads.dispatchers AS default_DOT_dispatchers) AS default_DOT_dispatcher ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
        LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.city,
                default_DOT_hard_hats.country,
                default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.postal_code,
                default_DOT_hard_hats.state
        FROM roads.hard_hats AS default_DOT_hard_hats) AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
        LEFT OUTER JOIN (SELECT  default_DOT_municipality.local_region,
                default_DOT_municipality.municipality_id
        FROM roads.municipality AS default_DOT_municipality LEFT  JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
        LEFT  JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc) AS default_DOT_municipality_dim ON default_DOT_repair_order.municipality_id = default_DOT_municipality_dim.municipality_id
        WHERE  default_DOT_hard_hat.state = 'AZ'
        GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.postal_code, default_DOT_hard_hat.city, default_DOT_hard_hat.state, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region
    """
    assert compare_query_strings(results["query"], expected_query)

    response = client_with_examples.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "engine_name": "druid",
            "engine_version": "",
            "config": {},
            "schedule": "",
        },
    )
    assert response.json() == {
        "message": "Successfully updated materialization config "
        "for node `default.repairs_cube` and engine `druid`.",
    }

    response = client_with_examples.get("/cubes/default.repairs_cube/")
    data = response.json()
    assert data["cube_elements"] == [
        {
            "name": "default_DOT_discounted_orders_rate",
            "node_name": "default.discounted_orders_rate",
            "type": "metric",
        },
        {
            "name": "default_DOT_num_repair_orders",
            "node_name": "default.num_repair_orders",
            "type": "metric",
        },
        {
            "name": "default_DOT_avg_repair_price",
            "node_name": "default.avg_repair_price",
            "type": "metric",
        },
        {
            "name": "default_DOT_total_repair_cost",
            "node_name": "default.total_repair_cost",
            "type": "metric",
        },
        {
            "name": "default_DOT_total_repair_order_discounts",
            "node_name": "default.total_repair_order_discounts",
            "type": "metric",
        },
        {
            "name": "default_DOT_double_total_repair_cost",
            "node_name": "default.double_total_repair_cost",
            "type": "metric",
        },
        {"name": "country", "node_name": "default.hard_hat", "type": "dimension"},
        {"name": "postal_code", "node_name": "default.hard_hat", "type": "dimension"},
        {"name": "city", "node_name": "default.hard_hat", "type": "dimension"},
        {"name": "state", "node_name": "default.hard_hat", "type": "dimension"},
        {
            "name": "company_name",
            "node_name": "default.dispatcher",
            "type": "dimension",
        },
        {
            "name": "local_region",
            "node_name": "default.municipality_dim",
            "type": "dimension",
        },
    ]
    expected_materialization_query = """
        SELECT  count(*) placeholder_count,
                sum(if(default_DOT_repair_order_details.discount > 0.0, 1, 0)) discount_sum,
                default_DOT_hard_hat.city,
                sum(default_DOT_repair_order_details.price * default_DOT_repair_order_details.discount) price_discount_sum,
                default_DOT_hard_hat.country,
                count(default_DOT_repair_order_details.price) price_count,
                default_DOT_hard_hat.state,
                default_DOT_dispatcher.company_name,
                default_DOT_hard_hat.postal_code,
                default_DOT_municipality_dim.local_region,
                count(default_DOT_repair_orders.repair_order_id) repair_order_id_count,
                sum(default_DOT_repair_order_details.price) price_sum
        FROM roads.repair_order_details AS default_DOT_repair_order_details LEFT OUTER  JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.repair_order_id
        FROM roads.repair_orders AS default_DOT_repair_orders
        ) AS default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
        LEFT OUTER  JOIN (SELECT  default_DOT_dispatchers.company_name,
                default_DOT_dispatchers.dispatcher_id
        FROM roads.dispatchers AS default_DOT_dispatchers
        ) AS default_DOT_dispatcher ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
        LEFT OUTER  JOIN (SELECT  default_DOT_hard_hats.city,
                default_DOT_hard_hats.country,
                default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.postal_code,
                default_DOT_hard_hats.state
        FROM roads.hard_hats AS default_DOT_hard_hats
        ) AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
        LEFT OUTER  JOIN (SELECT  default_DOT_municipality.local_region,
                default_DOT_municipality.municipality_id
        FROM roads.municipality AS default_DOT_municipality LEFT  JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
        LEFT  JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc
        ) AS default_DOT_municipality_dim ON default_DOT_repair_order.municipality_id = default_DOT_municipality_dim.municipality_id
        WHERE  default_DOT_hard_hat.state = 'AZ'
        GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.postal_code, default_DOT_hard_hat.city, default_DOT_hard_hat.state, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region
    """
    assert compare_query_strings(
        data["materialization_configs"][0]["config"]["query"],
        expected_materialization_query,
    )
    assert data["materialization_configs"][0]["config"]["measures"] == {
        "default_DOT_double_total_repair_cost": [
            {
                "name": "price_sum",
                "agg": "sum",
                "expr": "sum(default_DOT_repair_order_details.price)",
            },
        ],
        "default_DOT_num_repair_orders": [
            {
                "name": "repair_order_id_count",
                "agg": "count",
                "expr": "count(default_DOT_repair_orders.repair_order_id)",
            },
        ],
        "default_DOT_total_repair_order_discounts": [
            {
                "name": "price_discount_sum",
                "agg": "sum",
                "expr": (
                    "sum(default_DOT_repair_order_details.price * "
                    "default_DOT_repair_order_details.discount)"
                ),
            },
        ],
        "default_DOT_discounted_orders_rate": [
            {
                "name": "discount_sum",
                "agg": "sum",
                "expr": "sum(if(default_DOT_repair_order_details.discount > 0.0, 1, 0))",
            },
            {"name": "placeholder_count", "agg": "count", "expr": "count(*)"},
        ],
        "default_DOT_total_repair_cost": [
            {
                "name": "price_sum",
                "agg": "sum",
                "expr": "sum(default_DOT_repair_order_details.price)",
            },
        ],
        "default_DOT_avg_repair_price": [
            {
                "name": "price_count",
                "agg": "count",
                "expr": "count(default_DOT_repair_order_details.price)",
            },
            {
                "name": "price_sum",
                "agg": "sum",
                "expr": "sum(default_DOT_repair_order_details.price)",
            },
        ],
    }
