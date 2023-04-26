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
            "metrics": ["number_of_account_types"],
            "dimensions": ["account_type.account_type_name"],
            "filters": [],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "number_of_accounts_by_account_type",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["version"] == "v1.0"
    assert data["type"] == "cube"
    assert data["name"] == "number_of_accounts_by_account_type"
    assert data["display_name"] == "Number Of Accounts By Account Type"

    # Read the cube
    response = client_with_examples.get("/cubes/number_of_accounts_by_account_type")
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "cube"
    assert data["name"] == "number_of_accounts_by_account_type"
    assert data["display_name"] == "Number Of Accounts By Account Type"
    assert data["version"] == "v1.0"
    assert data["description"] == "A cube of number of accounts grouped by account type"
    assert compare_query_strings(
        data["query"],
        """
        SELECT
          account_type.account_type_name,
          count(account_type.id) AS num_accounts
        FROM (
          SELECT
            account_type_table.account_type_classification,
            account_type_table.account_type_name,
            account_type_table.id
          FROM accounting.account_type_table AS account_type_table) AS account_type
          GROUP BY
            account_type.account_type_name
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
            "cube_elements": ["number_of_account_types", "account_type"],
            "name": "cubes_shouldnt_have_queries",
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
            "metrics": ["account_type"],
            "dimensions": ["account_type.account_type_name"],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "cubes_must_have_elements",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "Node account_type of type dimension cannot be added to a cube. "
        "Did you mean to add a dimension attribute?",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with incompatible nodes fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": ["number_of_account_types"],
            "dimensions": ["payment_type.payment_type_name"],
            "description": "",
            "mode": "published",
            "name": "cubes_cant_use_source_nodes",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "The dimension attribute `payment_type.payment_type_name` is not "
        "available on every metric and thus cannot be included.",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with no metric nodes fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": [],
            "dimensions": ["account_type.account_type_name"],
            "description": "",
            "mode": "published",
            "name": "cubes_must_have_metrics",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "At least one metric is required to create a cube node",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with no dimension nodes fails appropriately
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": ["number_of_account_types"],
            "dimensions": [],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "cubes_must_have_dimensions",
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "At least one dimension is required to create a cube node",
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
            "metrics": ["number_of_account_types", "basic.num_comments"],
            "dimensions": ["account_type.account_type_name"],
            "description": "multicatalog cube's raise an error",
            "mode": "published",
            "name": "multicatalog",
        },
    )
    assert not response.ok
    data = response.json()
    assert "Cannot create cube using nodes from multiple catalogs" in data["message"]


def test_cube_sql(client_with_examples: TestClient):
    """
    Test that the generated cube materialization SQL makes sense
    """
    metrics_list = [
        "discounted_orders_rate",
        "num_repair_orders",
        "avg_repair_price",
        "total_repair_cost",
        "total_repair_order_discounts",
    ]
    # Should fail because dimension attribute isn't available
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": metrics_list,
            "dimensions": [
                "contractor.company_name",
            ],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "repairs_cube",
        },
    )
    assert response.json()["message"] == (
        "The dimension attribute `contractor.company_name` is not available "
        "on every metric and thus cannot be included."
    )

    # Metric that doubles the total repair cost to test the sum(x) + sum(y) scenario
    client_with_examples.post(
        "/nodes/metric/",
        json={
            "description": "Double total repair cost",
            "query": (
                "SELECT sum(price) + sum(price) as double_total_repair_cost "
                "FROM repair_order_details"
            ),
            "mode": "published",
            "name": "double_total_repair_cost",
        },
    )
    # Should succeed
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "metrics": metrics_list + ["double_total_repair_cost"],
            "dimensions": [
                "hard_hat.country",
                "hard_hat.postal_code",
                "hard_hat.city",
                "hard_hat.state",
                "dispatcher.company_name",
                "municipality_dim.local_region",
            ],
            "filters": ["hard_hat.state='AZ'"],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "repairs_cube",
        },
    )
    results = response.json()

    assert results["name"] == "repairs_cube"
    assert results["display_name"] == "Repairs Cube"
    assert results["description"] == "Cube of various metrics related to repairs"
    expected_query = """
        SELECT
          dispatcher.company_name,
          hard_hat.city,
          hard_hat.country,
          hard_hat.postal_code,
          hard_hat.state,
          municipality_dim.local_region,
          sum(repair_order_details.price) + sum(repair_order_details.price) AS double_total_repair_cost,
          sum(repair_order_details.price * repair_order_details.discount) AS total_discount,
          sum(repair_order_details.price) AS total_repair_cost,
          avg(repair_order_details.price) AS avg_repair_price,
          count(repair_orders.repair_order_id) AS num_repair_orders,
          CAST(sum(if(repair_order_details.discount > 0.0, 1, 0)) AS DOUBLE) / count(*)
            AS discounted_orders_rate
        FROM roads.repair_order_details AS repair_order_details
        LEFT OUTER JOIN (
          SELECT
            repair_orders.dispatcher_id,
            repair_orders.hard_hat_id,
            repair_orders.municipality_id,
            repair_orders.repair_order_id
          FROM roads.repair_orders AS repair_orders
        ) AS repair_order
        ON repair_order_details.repair_order_id = repair_order.repair_order_id
        LEFT OUTER JOIN (
          SELECT
            dispatchers.company_name,
            dispatchers.dispatcher_id
          FROM roads.dispatchers AS dispatchers
        ) AS dispatcher ON repair_order.dispatcher_id = dispatcher.dispatcher_id
        LEFT OUTER JOIN (
          SELECT
            hard_hats.city,
            hard_hats.country,
            hard_hats.hard_hat_id,
            hard_hats.postal_code,
            hard_hats.state
          FROM roads.hard_hats AS hard_hats
        ) AS hard_hat ON repair_order.hard_hat_id = hard_hat.hard_hat_id
        LEFT OUTER JOIN (
          SELECT
            municipality.local_region,
            municipality.municipality_id
          FROM roads.municipality AS municipality
          LEFT JOIN roads.municipality_municipality_type AS municipality_municipality_type
          ON municipality.municipality_id = municipality_municipality_type.municipality_id
          LEFT JOIN roads.municipality_type AS municipality_type
          ON municipality_municipality_type.municipality_type_id
             = municipality_type.municipality_type_desc
        ) AS municipality_dim
        ON repair_order.municipality_id = municipality_dim.municipality_id
        WHERE hard_hat.state = 'AZ'
        GROUP BY
          hard_hat.country,
          hard_hat.postal_code,
          hard_hat.city,
          hard_hat.state,
          dispatcher.company_name,
          municipality_dim.local_region
    """
    print(results["query"])
    assert compare_query_strings(results["query"], expected_query)

    response = client_with_examples.post(
        "/nodes/repairs_cube/materialization/",
        json={
            "engine_name": "druid",
            "engine_version": "",
            "config": {},
            "schedule": "",
        },
    )
    assert response.json() == {
        "message": "Successfully updated materialization config "
        "for node `repairs_cube` and engine `druid`.",
    }

    response = client_with_examples.get("/cubes/repairs_cube/")
    data = response.json()
    assert data["cube_elements"] == [
        {
            "name": "discounted_orders_rate",
            "node_name": "discounted_orders_rate",
            "type": "metric",
        },
        {
            "name": "num_repair_orders",
            "node_name": "num_repair_orders",
            "type": "metric",
        },
        {"name": "avg_repair_price", "node_name": "avg_repair_price", "type": "metric"},
        {
            "name": "total_repair_cost",
            "node_name": "total_repair_cost",
            "type": "metric",
        },
        {
            "name": "total_discount",
            "node_name": "total_repair_order_discounts",
            "type": "metric",
        },
        {
            "name": "double_total_repair_cost",
            "node_name": "double_total_repair_cost",
            "type": "metric",
        },
        {"name": "country", "node_name": "hard_hat", "type": "dimension"},
        {"name": "postal_code", "node_name": "hard_hat", "type": "dimension"},
        {"name": "city", "node_name": "hard_hat", "type": "dimension"},
        {"name": "state", "node_name": "hard_hat", "type": "dimension"},
        {"name": "company_name", "node_name": "dispatcher", "type": "dimension"},
        {"name": "local_region", "node_name": "municipality_dim", "type": "dimension"},
    ]
    expected_materialization_query = """
        SELECT
          count(repair_order_details.price) price_count,
          dispatcher.company_name,
          count(repair_orders.repair_order_id) repair_order_id_count,
          sum(repair_order_details.price * repair_order_details.discount) price_discount_sum,
          hard_hat.city,
          count(*) placeholder_count,
          sum(if(repair_order_details.discount > 0.0, 1, 0)) discount_sum,
          hard_hat.state,
          municipality_dim.local_region,
          hard_hat.postal_code,
          sum(repair_order_details.price) price_sum,
          hard_hat.country
        FROM roads.repair_order_details AS repair_order_details
        LEFT OUTER JOIN (
          SELECT
            repair_orders.dispatcher_id,
            repair_orders.hard_hat_id,
            repair_orders.municipality_id,
            repair_orders.repair_order_id
          FROM roads.repair_orders AS repair_orders
        ) AS repair_order
        ON repair_order_details.repair_order_id = repair_order.repair_order_id
        LEFT OUTER JOIN (
          SELECT
            dispatchers.company_name,
            dispatchers.dispatcher_id
          FROM roads.dispatchers AS dispatchers
        ) AS dispatcher ON repair_order.dispatcher_id = dispatcher.dispatcher_id
        LEFT OUTER JOIN (
          SELECT
            hard_hats.city,
            hard_hats.country,
            hard_hats.hard_hat_id,
            hard_hats.postal_code,
            hard_hats.state
          FROM roads.hard_hats AS hard_hats
        ) AS hard_hat ON repair_order.hard_hat_id = hard_hat.hard_hat_id
        LEFT OUTER JOIN (
          SELECT
            municipality.local_region,
            municipality.municipality_id
          FROM roads.municipality AS municipality
          LEFT JOIN roads.municipality_municipality_type AS municipality_municipality_type
          ON municipality.municipality_id = municipality_municipality_type.municipality_id
          LEFT JOIN roads.municipality_type AS municipality_type
          ON municipality_municipality_type.municipality_type_id
             = municipality_type.municipality_type_desc
        ) AS municipality_dim
        ON repair_order.municipality_id = municipality_dim.municipality_id
        WHERE hard_hat.state = 'AZ'
        GROUP BY
          hard_hat.country,
          hard_hat.postal_code,
          hard_hat.city,
          hard_hat.state,
          dispatcher.company_name,
          municipality_dim.local_region
    """
    assert compare_query_strings(
        data["materialization_configs"][0]["config"]["query"],
        expected_materialization_query,
    )
    assert data["materialization_configs"][0]["config"]["measures"] == {
        "avg_repair_price": [
            {
                "name": "price_count",
                "agg": "count",
                "expr": "count(repair_order_details.price)",
            },
            {
                "name": "price_sum",
                "agg": "sum",
                "expr": "sum(repair_order_details.price)",
            },
        ],
        "double_total_repair_cost": [
            {
                "agg": "sum",
                "expr": "sum(repair_order_details.price)",
                "name": "price_sum",
            },
        ],
        "discounted_orders_rate": [
            {
                "agg": "sum",
                "expr": "sum(if(repair_order_details.discount > " "0.0, 1, 0))",
                "name": "discount_sum",
            },
            {"agg": "count", "expr": "count(*)", "name": "placeholder_count"},
        ],
        "num_repair_orders": [
            {
                "name": "repair_order_id_count",
                "agg": "count",
                "expr": "count(repair_orders.repair_order_id)",
            },
        ],
        "total_discount": [
            {
                "agg": "sum",
                "expr": "sum(repair_order_details.price * repair_order_details.discount)",
                "name": "price_discount_sum",
            },
        ],
        "total_repair_cost": [
            {
                "name": "price_sum",
                "agg": "sum",
                "expr": "sum(repair_order_details.price)",
            },
        ],
    }
