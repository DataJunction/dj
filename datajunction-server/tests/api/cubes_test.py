# pylint: disable=too-many-lines
"""
Tests for the cubes API.
"""
from typing import Callable, Dict, Iterator, List, Optional
from unittest import mock

import pytest
import requests
from fastapi.testclient import TestClient

from datajunction_server.models.query import ColumnMetadata
from datajunction_server.service_clients import QueryServiceClient
from tests.sql.utils import compare_query_strings


def set_temporal_partition_cube(client: TestClient):
    """
    Sets the temporal partition column for a cube
    """
    return client.post(
        "/nodes/default.repairs_cube/columns/default_DOT_hard_hat_DOT_hire_date/partition",
        json={
            "type_": "temporal",
            "granularity": "day",
            "format": "yyyyMMdd",
        },
    )


def test_read_cube(client_with_account_revenue: TestClient) -> None:
    """
    Test ``GET /cubes/{name}``.
    """
    # Create a cube
    response = client_with_account_revenue.post(
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
    response = client_with_account_revenue.get(
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
        WITH default_DOT_account_type AS (
          SELECT
            default_DOT_account_type.account_type_name
              default_DOT_account_type_DOT_account_type_name,
            count(default_DOT_account_type.id)
              default_DOT_number_of_account_types
          FROM (SELECT  default_DOT_account_type_table.account_type_classification,
              default_DOT_account_type_table.account_type_name,
              default_DOT_account_type_table.id
          FROM accounting.account_type_table AS default_DOT_account_type_table)
          AS default_DOT_account_type
          GROUP BY  default_DOT_account_type.account_type_name
        )
        SELECT  default_DOT_account_type.default_DOT_number_of_account_types,
            default_DOT_account_type.default_DOT_account_type_DOT_account_type_name
         FROM default_DOT_account_type
        """,
    )


def test_create_invalid_cube(client_with_account_revenue: TestClient):
    """
    Check that creating a cube with a query fails appropriately
    """
    response = client_with_account_revenue.post(
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
    response = client_with_account_revenue.post(
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
    response = client_with_account_revenue.post(
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
        "message": "The dimension attribute `default.payment_type.payment_type_name` "
        "is not available on every metric and thus cannot be included.",
        "errors": [],
        "warnings": [],
    }

    # Check that creating a cube with no metric nodes fails appropriately
    response = client_with_account_revenue.post(
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
    response = client_with_account_revenue.post(
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
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
) -> None:
    """
    Test raising when creating a cube with multiple catalogs
    """
    # Create a cube
    custom_client = client_example_loader(["BASIC", "ACCOUNT_REVENUE"])
    response = custom_client.post(
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


@pytest.fixture
def client_with_repairs_cube(
    client_with_query_service_example_loader,
):
    """
    Adds a repairs cube with a new double total repair cost metric to the test client
    """
    custom_client = client_with_query_service_example_loader(["ROADS"])
    metrics_list = [
        "default.discounted_orders_rate",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.total_repair_order_discounts",
    ]

    # Metric that doubles the total repair cost to test the sum(x) + sum(y) scenario
    custom_client.post(
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
    # Should succeed
    response = custom_client.post(
        "/nodes/cube/",
        json={
            "metrics": metrics_list + ["default.double_total_repair_cost"],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.postal_code",
                "default.hard_hat.city",
                "default.hard_hat.hire_date",
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
    assert response.status_code == 201
    assert response.json()["version"] == "v1.0"
    return custom_client


@pytest.fixture
def repair_orders_cube_measures() -> Dict:
    """
    Fixture for repair orders cube metrics to measures mapping.
    """
    return {
        "default_DOT_avg_repair_price": {
            "combiner": "sum(price3402113753_sum) / " "count(price3402113753_count)",
            "measures": [
                {
                    "agg": "count",
                    "field_name": "default_DOT_repair_order_details_price3402113753_count",
                    "name": "price3402113753_count",
                    "type": "bigint",
                },
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_price3402113753_sum",
                    "name": "price3402113753_sum",
                    "type": "double",
                },
            ],
            "metric": "default_DOT_avg_repair_price",
        },
        "default_DOT_discounted_orders_rate": {
            "combiner": "sum(discount3789599758_sum) " "/ " "count(placeholder_count)",
            "measures": [
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_discount3789599758_sum",
                    "name": "discount3789599758_sum",
                    "type": "bigint",
                },
                {
                    "agg": "count",
                    "field_name": "default_DOT_repair_order_details_placeholder_count",
                    "name": "placeholder_count",
                    "type": "bigint",
                },
            ],
            "metric": "default_DOT_discounted_orders_rate",
        },
        "default_DOT_double_total_repair_cost": {
            "combiner": "sum(price3402113753_sum) " "+ " "sum(price3402113753_sum)",
            "measures": [
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_price3402113753_sum",
                    "name": "price3402113753_sum",
                    "type": "double",
                },
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_price3402113753_sum",
                    "name": "price3402113753_sum",
                    "type": "double",
                },
            ],
            "metric": "default_DOT_double_total_repair_cost",
        },
        "default_DOT_num_repair_orders": {
            "combiner": "count(repair_order_id3825669267_count)",
            "measures": [
                {
                    "agg": "count",
                    "field_name": "default_DOT_repair_orders_repair_order_id3825669267_count",
                    "name": "repair_order_id3825669267_count",
                    "type": "bigint",
                },
            ],
            "metric": "default_DOT_num_repair_orders",
        },
        "default_DOT_total_repair_cost": {
            "combiner": "sum(price3402113753_sum)",
            "measures": [
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_price3402113753_sum",
                    "name": "price3402113753_sum",
                    "type": "double",
                },
            ],
            "metric": "default_DOT_total_repair_cost",
        },
        "default_DOT_total_repair_order_discounts": {
            "combiner": "sum(price_discount2203488025_sum)",
            "measures": [
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_price_discount2203488025_sum",
                    "name": "price_discount2203488025_sum",
                    "type": "double",
                },
            ],
            "metric": "default_DOT_total_repair_order_discounts",
        },
    }


@pytest.fixture
def repairs_cube_elements():
    """
    Fixture of repairs cube elements
    """
    return sorted(
        [
            {
                "display_name": "City",
                "name": "city",
                "node_name": "default.hard_hat",
                "type": "dimension",
                "partition": None,
            },
            {
                "display_name": "Company Name",
                "name": "company_name",
                "node_name": "default.dispatcher",
                "type": "dimension",
                "partition": None,
            },
            {
                "display_name": "Country",
                "name": "country",
                "node_name": "default.hard_hat",
                "type": "dimension",
                "partition": None,
            },
            {
                "display_name": "Hire Date",
                "name": "hire_date",
                "node_name": "default.hard_hat",
                "partition": None,
                "type": "dimension",
            },
            {
                "display_name": "Default: Avg Repair Price",
                "name": "default_DOT_avg_repair_price",
                "node_name": "default.avg_repair_price",
                "type": "metric",
                "partition": None,
            },
            {
                "display_name": "Default: Discounted Orders Rate",
                "name": "default_DOT_discounted_orders_rate",
                "node_name": "default.discounted_orders_rate",
                "type": "metric",
                "partition": None,
            },
            {
                "display_name": "Default: Double Total Repair Cost",
                "name": "default_DOT_double_total_repair_cost",
                "node_name": "default.double_total_repair_cost",
                "type": "metric",
                "partition": None,
            },
            {
                "display_name": "Default: Num Repair Orders",
                "name": "default_DOT_num_repair_orders",
                "node_name": "default.num_repair_orders",
                "type": "metric",
                "partition": None,
            },
            {
                "display_name": "Default: Total Repair Cost",
                "name": "default_DOT_total_repair_cost",
                "node_name": "default.total_repair_cost",
                "type": "metric",
                "partition": None,
            },
            {
                "display_name": "Default: Total Repair Order Discounts",
                "name": "default_DOT_total_repair_order_discounts",
                "node_name": "default.total_repair_order_discounts",
                "type": "metric",
                "partition": None,
            },
            {
                "display_name": "Local Region",
                "name": "local_region",
                "node_name": "default.municipality_dim",
                "type": "dimension",
                "partition": None,
            },
            {
                "display_name": "Postal Code",
                "name": "postal_code",
                "node_name": "default.hard_hat",
                "type": "dimension",
                "partition": None,
            },
            {
                "display_name": "State",
                "name": "state",
                "node_name": "default.hard_hat",
                "type": "dimension",
                "partition": None,
            },
        ],
        key=lambda x: x["name"],
    )


def test_invalid_cube(client_with_roads: TestClient):
    """
    Test that creating a cube without valid dimensions fails
    """
    metrics_list = [
        "default.discounted_orders_rate",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.total_repair_order_discounts",
    ]
    # Should fail because dimension attribute isn't available
    response = client_with_roads.post(
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
        "The dimension attribute `default.contractor.company_name` "
        "is not available on every metric and thus cannot be included."
    )


def test_create_cube(  # pylint: disable=redefined-outer-name
    client_with_repairs_cube: TestClient,
    repair_orders_cube_measures,
):
    """
    Tests cube creation and the generated cube SQL
    """
    response = client_with_repairs_cube.get("/nodes/default.repairs_cube/")
    results = response.json()

    assert results["name"] == "default.repairs_cube"
    assert results["display_name"] == "Default: Repairs Cube"
    assert results["description"] == "Cube of various metrics related to repairs"

    default_materialization = response.json()["materializations"][0]
    assert default_materialization["job"] == "DefaultCubeMaterialization"
    assert default_materialization["name"] == "default"
    assert default_materialization["schedule"] == "@daily"
    assert sorted(
        default_materialization["config"]["columns"],
        key=lambda x: x["name"],
    ) == sorted(
        [
            {"name": "default_DOT_dispatcher_DOT_company_name", "type": "string"},
            {"name": "default_DOT_hard_hat_DOT_city", "type": "string"},
            {"name": "default_DOT_hard_hat_DOT_country", "type": "string"},
            {"name": "default_DOT_hard_hat_DOT_hire_date", "type": "timestamp"},
            {"name": "default_DOT_hard_hat_DOT_postal_code", "type": "string"},
            {"name": "default_DOT_hard_hat_DOT_state", "type": "string"},
            {"name": "default_DOT_municipality_dim_DOT_local_region", "type": "string"},
            {
                "name": "default_DOT_repair_order_details_discount3789599758_sum",
                "type": "bigint",
            },
            {
                "name": "default_DOT_repair_order_details_placeholder_count",
                "type": "bigint",
            },
            {
                "name": "default_DOT_repair_order_details_price3402113753_count",
                "type": "bigint",
            },
            {
                "name": "default_DOT_repair_order_details_price3402113753_sum",
                "type": "double",
            },
            {
                "name": "default_DOT_repair_order_details_price_discount2203488025_sum",
                "type": "double",
            },
            {
                "name": "default_DOT_repair_orders_repair_order_id3825669267_count",
                "type": "bigint",
            },
        ],
        key=lambda x: x["name"],
    )
    assert default_materialization["config"]["upstream_tables"] == [
        "default.roads.dispatchers",
        "default.roads.hard_hats",
        "default.roads.municipality",
        "default.roads.municipality_municipality_type",
        "default.roads.municipality_type",
        "default.roads.repair_order_details",
        "default.roads.repair_orders",
    ]
    assert default_materialization["config"]["dimensions"] == [
        "default_DOT_dispatcher_DOT_company_name",
        "default_DOT_hard_hat_DOT_city",
        "default_DOT_hard_hat_DOT_country",
        "default_DOT_hard_hat_DOT_hire_date",
        "default_DOT_hard_hat_DOT_postal_code",
        "default_DOT_hard_hat_DOT_state",
        "default_DOT_municipality_dim_DOT_local_region",
    ]
    assert default_materialization["config"]["measures"] == repair_orders_cube_measures

    expected_query = """
    WITH
    default_DOT_repair_order_details AS (SELECT  default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
        default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
        default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
        default_DOT_hard_hat.hire_date default_DOT_hard_hat_DOT_hire_date,
        default_DOT_hard_hat.postal_code default_DOT_hard_hat_DOT_postal_code,
        default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
        default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region,
        CAST(sum(if(default_DOT_repair_order_details.discount > 0.0, 1, 0)) AS DOUBLE) / count(*) AS default_DOT_discounted_orders_rate,
        avg(default_DOT_repair_order_details.price) AS default_DOT_avg_repair_price,
        sum(default_DOT_repair_order_details.price) default_DOT_total_repair_cost,
        sum(default_DOT_repair_order_details.price * default_DOT_repair_order_details.discount) default_DOT_total_repair_order_discounts,
        sum(default_DOT_repair_order_details.price) + sum(default_DOT_repair_order_details.price) AS default_DOT_double_total_repair_cost
    FROM roads.repair_order_details AS default_DOT_repair_order_details LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
            default_DOT_repair_orders.hard_hat_id,
            default_DOT_repair_orders.municipality_id,
            default_DOT_repair_orders.repair_order_id
    FROM roads.repair_orders AS default_DOT_repair_orders)
    AS default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
    LEFT OUTER JOIN (SELECT  default_DOT_dispatchers.company_name,
            default_DOT_dispatchers.dispatcher_id
    FROM roads.dispatchers AS default_DOT_dispatchers)
    AS default_DOT_dispatcher ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
    LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.city,
            default_DOT_hard_hats.country,
            default_DOT_hard_hats.hard_hat_id,
            default_DOT_hard_hats.hire_date,
            default_DOT_hard_hats.postal_code,
            default_DOT_hard_hats.state
    FROM roads.hard_hats AS default_DOT_hard_hats)
    AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
    LEFT OUTER JOIN (SELECT  default_DOT_municipality.local_region,
            default_DOT_municipality.municipality_id AS municipality_id
    FROM roads.municipality AS default_DOT_municipality LEFT  JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
    LEFT  JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc)
    AS default_DOT_municipality_dim ON default_DOT_repair_order.municipality_id = default_DOT_municipality_dim.municipality_id
    WHERE  default_DOT_hard_hat.state = 'AZ'
    GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.postal_code, default_DOT_hard_hat.city, default_DOT_hard_hat.hire_date, default_DOT_hard_hat.state, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region
    ),
    default_DOT_repair_orders AS (SELECT  default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
            default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
            default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
            default_DOT_hard_hat.hire_date default_DOT_hard_hat_DOT_hire_date,
            default_DOT_hard_hat.postal_code default_DOT_hard_hat_DOT_postal_code,
            default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
            default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region,
            count(default_DOT_repair_orders.repair_order_id) default_DOT_num_repair_orders
         FROM roads.repair_orders AS default_DOT_repair_orders LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
            default_DOT_repair_orders.hard_hat_id,
            default_DOT_repair_orders.municipality_id,
            default_DOT_repair_orders.repair_order_id
         FROM roads.repair_orders AS default_DOT_repair_orders)
         AS default_DOT_repair_order ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order.repair_order_id
        LEFT OUTER JOIN (SELECT  default_DOT_dispatchers.company_name,
            default_DOT_dispatchers.dispatcher_id
         FROM roads.dispatchers AS default_DOT_dispatchers)
         AS default_DOT_dispatcher ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
        LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.city,
            default_DOT_hard_hats.country,
            default_DOT_hard_hats.hard_hat_id,
            default_DOT_hard_hats.hire_date,
            default_DOT_hard_hats.postal_code,
            default_DOT_hard_hats.state
         FROM roads.hard_hats AS default_DOT_hard_hats)
         AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
        LEFT OUTER JOIN (SELECT  default_DOT_municipality.local_region,
            default_DOT_municipality.municipality_id AS municipality_id
         FROM roads.municipality AS default_DOT_municipality LEFT  JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
        LEFT  JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc)
         AS default_DOT_municipality_dim ON default_DOT_repair_order.municipality_id = default_DOT_municipality_dim.municipality_id
         WHERE  default_DOT_hard_hat.state = 'AZ'
     GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.postal_code, default_DOT_hard_hat.city, default_DOT_hard_hat.hire_date, default_DOT_hard_hat.state, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region
    )

    SELECT  default_DOT_repair_order_details.default_DOT_discounted_orders_rate,
        default_DOT_repair_order_details.default_DOT_avg_repair_price,
        default_DOT_repair_order_details.default_DOT_total_repair_cost,
        default_DOT_repair_order_details.default_DOT_total_repair_order_discounts,
        default_DOT_repair_order_details.default_DOT_double_total_repair_cost,
        default_DOT_repair_orders.default_DOT_num_repair_orders,
        COALESCE(default_DOT_repair_order_details.default_DOT_dispatcher_DOT_company_name, default_DOT_repair_orders.default_DOT_dispatcher_DOT_company_name) default_DOT_dispatcher_DOT_company_name,
        COALESCE(default_DOT_repair_order_details.default_DOT_hard_hat_DOT_city, default_DOT_repair_orders.default_DOT_hard_hat_DOT_city) default_DOT_hard_hat_DOT_city,
        COALESCE(default_DOT_repair_order_details.default_DOT_hard_hat_DOT_country, default_DOT_repair_orders.default_DOT_hard_hat_DOT_country) default_DOT_hard_hat_DOT_country,
        COALESCE(default_DOT_repair_order_details.default_DOT_hard_hat_DOT_hire_date, default_DOT_repair_orders.default_DOT_hard_hat_DOT_hire_date) default_DOT_hard_hat_DOT_hire_date,
        COALESCE(default_DOT_repair_order_details.default_DOT_hard_hat_DOT_postal_code, default_DOT_repair_orders.default_DOT_hard_hat_DOT_postal_code) default_DOT_hard_hat_DOT_postal_code,
        COALESCE(default_DOT_repair_order_details.default_DOT_hard_hat_DOT_state, default_DOT_repair_orders.default_DOT_hard_hat_DOT_state) default_DOT_hard_hat_DOT_state,
        COALESCE(default_DOT_repair_order_details.default_DOT_municipality_dim_DOT_local_region, default_DOT_repair_orders.default_DOT_municipality_dim_DOT_local_region) default_DOT_municipality_dim_DOT_local_region
    FROM default_DOT_repair_order_details FULL OUTER JOIN default_DOT_repair_orders ON default_DOT_repair_order_details.default_DOT_dispatcher_DOT_company_name = default_DOT_repair_orders.default_DOT_dispatcher_DOT_company_name AND default_DOT_repair_order_details.default_DOT_hard_hat_DOT_city = default_DOT_repair_orders.default_DOT_hard_hat_DOT_city AND default_DOT_repair_order_details.default_DOT_hard_hat_DOT_country = default_DOT_repair_orders.default_DOT_hard_hat_DOT_country AND default_DOT_repair_order_details.default_DOT_hard_hat_DOT_hire_date = default_DOT_repair_orders.default_DOT_hard_hat_DOT_hire_date AND default_DOT_repair_order_details.default_DOT_hard_hat_DOT_postal_code = default_DOT_repair_orders.default_DOT_hard_hat_DOT_postal_code AND default_DOT_repair_order_details.default_DOT_hard_hat_DOT_state = default_DOT_repair_orders.default_DOT_hard_hat_DOT_state AND default_DOT_repair_order_details.default_DOT_municipality_dim_DOT_local_region = default_DOT_repair_orders.default_DOT_municipality_dim_DOT_local_region
    """
    assert compare_query_strings(results["query"], expected_query)


def test_cube_materialization_sql_and_measures(
    client_with_repairs_cube: TestClient,  # pylint: disable=redefined-outer-name
    repair_orders_cube_measures,  # pylint: disable=redefined-outer-name
    repairs_cube_elements,  # pylint: disable=redefined-outer-name
):
    """
    Verifies a cube's materialization SQL + measures
    """
    response = client_with_repairs_cube.get("/cubes/default.repairs_cube/")
    data = response.json()
    assert (
        sorted(data["cube_elements"], key=lambda x: x["name"]) == repairs_cube_elements
    )
    expected_materialization_query = """
    WITH
    default_DOT_repair_order_details AS (SELECT  sum(default_DOT_repair_order_details.price) price3402113753_sum,
            count(*) placeholder_count,
            default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region,
            sum(default_DOT_repair_order_details.price * default_DOT_repair_order_details.discount) price_discount2203488025_sum,
            default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
            default_DOT_hard_hat.postal_code default_DOT_hard_hat_DOT_postal_code,
            default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
            sum(if(default_DOT_repair_order_details.discount > 0.0, 1, 0)) discount3789599758_sum,
            default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
            default_DOT_hard_hat.hire_date default_DOT_hard_hat_DOT_hire_date,
            default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
            count(default_DOT_repair_order_details.price) price3402113753_count
    FROM roads.repair_order_details AS default_DOT_repair_order_details LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
            default_DOT_repair_orders.hard_hat_id,
            default_DOT_repair_orders.municipality_id,
            default_DOT_repair_orders.repair_order_id
    FROM roads.repair_orders AS default_DOT_repair_orders)
    AS default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
    LEFT OUTER JOIN (SELECT  default_DOT_dispatchers.company_name,
            default_DOT_dispatchers.dispatcher_id
    FROM roads.dispatchers AS default_DOT_dispatchers)
    AS default_DOT_dispatcher ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
    LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.city,
            default_DOT_hard_hats.country,
            default_DOT_hard_hats.hard_hat_id,
            default_DOT_hard_hats.hire_date,
            default_DOT_hard_hats.postal_code,
            default_DOT_hard_hats.state
    FROM roads.hard_hats AS default_DOT_hard_hats)
    AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
    LEFT OUTER JOIN (SELECT  default_DOT_municipality.local_region,
            default_DOT_municipality.municipality_id AS municipality_id
    FROM roads.municipality AS default_DOT_municipality LEFT  JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
    LEFT  JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc)
    AS default_DOT_municipality_dim ON default_DOT_repair_order.municipality_id = default_DOT_municipality_dim.municipality_id
    WHERE  default_DOT_hard_hat.state = 'AZ'
    GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.postal_code, default_DOT_hard_hat.city, default_DOT_hard_hat.hire_date, default_DOT_hard_hat.state, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region
    ),
    default_DOT_repair_orders AS (SELECT  count(default_DOT_repair_orders.repair_order_id) repair_order_id3825669267_count,
        default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
        default_DOT_hard_hat.hire_date default_DOT_hard_hat_DOT_hire_date,
        default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
        default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region,
        default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
        default_DOT_hard_hat.postal_code default_DOT_hard_hat_DOT_postal_code,
        default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name
    FROM roads.repair_orders AS default_DOT_repair_orders LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
        default_DOT_repair_orders.hard_hat_id,
        default_DOT_repair_orders.municipality_id,
        default_DOT_repair_orders.repair_order_id
    FROM roads.repair_orders AS default_DOT_repair_orders)
         AS default_DOT_repair_order ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order.repair_order_id
        LEFT OUTER JOIN (SELECT  default_DOT_dispatchers.company_name,
            default_DOT_dispatchers.dispatcher_id
         FROM roads.dispatchers AS default_DOT_dispatchers)
         AS default_DOT_dispatcher ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
        LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.city,
            default_DOT_hard_hats.country,
            default_DOT_hard_hats.hard_hat_id,
        default_DOT_hard_hats.hire_date,
            default_DOT_hard_hats.postal_code,
            default_DOT_hard_hats.state
         FROM roads.hard_hats AS default_DOT_hard_hats)
         AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
        LEFT OUTER JOIN (SELECT  default_DOT_municipality.local_region,
            default_DOT_municipality.municipality_id AS municipality_id
         FROM roads.municipality AS default_DOT_municipality LEFT  JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
        LEFT  JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc)
         AS default_DOT_municipality_dim ON default_DOT_repair_order.municipality_id = default_DOT_municipality_dim.municipality_id
         WHERE  default_DOT_hard_hat.state = 'AZ'
     GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.postal_code, default_DOT_hard_hat.city, default_DOT_hard_hat.hire_date, default_DOT_hard_hat.state, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region
    )

    SELECT  default_DOT_repair_order_details.price3402113753_sum default_DOT_repair_order_details_price3402113753_sum,
        default_DOT_repair_order_details.placeholder_count default_DOT_repair_order_details_placeholder_count,
        default_DOT_repair_order_details.price_discount2203488025_sum default_DOT_repair_order_details_price_discount2203488025_sum,
        default_DOT_repair_order_details.discount3789599758_sum default_DOT_repair_order_details_discount3789599758_sum,
        default_DOT_repair_order_details.price3402113753_count default_DOT_repair_order_details_price3402113753_count,
        default_DOT_repair_orders.repair_order_id3825669267_count default_DOT_repair_orders_repair_order_id3825669267_count,
        COALESCE(default_DOT_repair_order_details.default_DOT_municipality_dim_DOT_local_region, default_DOT_repair_orders.default_DOT_municipality_dim_DOT_local_region) default_DOT_municipality_dim_DOT_local_region,
        COALESCE(default_DOT_repair_order_details.default_DOT_hard_hat_DOT_country, default_DOT_repair_orders.default_DOT_hard_hat_DOT_country) default_DOT_hard_hat_DOT_country,
        COALESCE(default_DOT_repair_order_details.default_DOT_hard_hat_DOT_postal_code, default_DOT_repair_orders.default_DOT_hard_hat_DOT_postal_code) default_DOT_hard_hat_DOT_postal_code,
        COALESCE(default_DOT_repair_order_details.default_DOT_dispatcher_DOT_company_name, default_DOT_repair_orders.default_DOT_dispatcher_DOT_company_name) default_DOT_dispatcher_DOT_company_name,
        COALESCE(default_DOT_repair_order_details.default_DOT_hard_hat_DOT_city, default_DOT_repair_orders.default_DOT_hard_hat_DOT_city) default_DOT_hard_hat_DOT_city,
        COALESCE(default_DOT_repair_order_details.default_DOT_hard_hat_DOT_hire_date, default_DOT_repair_orders.default_DOT_hard_hat_DOT_hire_date) default_DOT_hard_hat_DOT_hire_date,
        COALESCE(default_DOT_repair_order_details.default_DOT_hard_hat_DOT_state, default_DOT_repair_orders.default_DOT_hard_hat_DOT_state) default_DOT_hard_hat_DOT_state
    FROM default_DOT_repair_order_details FULL OUTER JOIN default_DOT_repair_orders
        ON default_DOT_repair_order_details.default_DOT_dispatcher_DOT_company_name =
            default_DOT_repair_orders.default_DOT_dispatcher_DOT_company_name
        AND default_DOT_repair_order_details.default_DOT_hard_hat_DOT_city =
            default_DOT_repair_orders.default_DOT_hard_hat_DOT_city
        AND default_DOT_repair_order_details.default_DOT_hard_hat_DOT_country
            = default_DOT_repair_orders.default_DOT_hard_hat_DOT_country
        AND default_DOT_repair_order_details.default_DOT_hard_hat_DOT_hire_date
            = default_DOT_repair_orders.default_DOT_hard_hat_DOT_hire_date
        AND default_DOT_repair_order_details.default_DOT_hard_hat_DOT_postal_code
            = default_DOT_repair_orders.default_DOT_hard_hat_DOT_postal_code
        AND default_DOT_repair_order_details.default_DOT_hard_hat_DOT_state
            = default_DOT_repair_orders.default_DOT_hard_hat_DOT_state
        AND default_DOT_repair_order_details.default_DOT_municipality_dim_DOT_local_region
            = default_DOT_repair_orders.default_DOT_municipality_dim_DOT_local_region"""
    assert compare_query_strings(
        data["materializations"][0]["config"]["query"],
        expected_materialization_query,
    )
    assert data["materializations"][0]["job"] == "DefaultCubeMaterialization"
    assert (
        data["materializations"][0]["config"]["measures"] == repair_orders_cube_measures
    )


def test_add_materialization_cube_failures(
    client_with_repairs_cube: TestClient,  # pylint: disable=redefined-outer-name
    query_service_client: QueryServiceClient,
):
    """
    Verifies failure modes when adding materialization config to cube nodes
    """
    response = client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "engine": {"name": "druid", "version": ""},
            "config": {},
            "schedule": "@daily",
        },
    )
    assert response.json()["message"] == (
        "The cube materialization cannot be configured if there is no temporal partition specified"
        " on the cube. Please make sure at least one cube element has a temporal partition defined"
    )

    set_temporal_partition_cube(client_with_repairs_cube)

    response = client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "engine": {"name": "druid", "version": ""},
            "config": {
                "spark": {},
            },
            "schedule": "",
        },
    )
    assert (
        response.json()["message"]
        == "Successfully updated materialization config named "
        "`default_DOT_hard_hat_DOT_hire_date_druid` "
        "for node `default.repairs_cube`"
    )
    args, _ = query_service_client.materialize.call_args_list[0]  # type: ignore
    assert (
        "WHERE  default_DOT_hard_hat_DOT_hire_date = CAST(DATE_FORMAT("
        "CAST(${dj_logical_timestamp} AS TIMESTAMP), 'yyyyMMdd') AS TIMESTAMP)"
        in args[0].query
    )

    response = client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "engine": {"name": "druid", "version": ""},
            "config": {
                "druid": {"a": "b"},
                "spark": {},
            },
            "schedule": "",
        },
    )
    assert response.json()["message"] == (
        "The same materialization config with name "
        "`default_DOT_hard_hat_DOT_hire_date_druid` already exists for node "
        "`default.repairs_cube` so no update was performed."
    )


@pytest.fixture
def repairs_cube_with_materialization(
    client_with_repairs_cube: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Repairs cube with a configured materialization
    """
    set_temporal_partition_cube(client_with_repairs_cube)
    return client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "engine": {"name": "druid", "version": ""},
            "config": {
                "spark": {},
            },
            "schedule": "",
        },
    )


def test_add_materialization_config_to_cube(
    client_with_repairs_cube: TestClient,  # pylint: disable=redefined-outer-name
    repairs_cube_with_materialization: requests.Response,  # pylint: disable=redefined-outer-name
    query_service_client: Iterator[QueryServiceClient],
):
    """
    Verifies adding materialization config to a cube
    """
    assert repairs_cube_with_materialization.json() == {
        "message": "Successfully updated materialization config named "
        "`default_DOT_hard_hat_DOT_hire_date_druid` "
        "for node `default.repairs_cube`",
        "urls": [["http://fake.url/job"]],
    }
    called_kwargs = [
        call_[0]
        for call_ in query_service_client.materialize.call_args_list  # type: ignore
    ][0][0]
    assert called_kwargs.name == "default_DOT_hard_hat_DOT_hire_date_druid"
    assert called_kwargs.node_name == "default.repairs_cube"
    assert called_kwargs.node_type == "cube"
    assert called_kwargs.schedule == "@daily"
    assert called_kwargs.spark_conf == {}
    assert len(called_kwargs.columns) > 0
    dimensions_sorted = sorted(
        called_kwargs.druid_spec["dataSchema"]["parser"]["parseSpec"]["dimensionsSpec"][
            "dimensions"
        ],
    )
    called_kwargs.druid_spec["dataSchema"]["parser"]["parseSpec"]["dimensionsSpec"][
        "dimensions"
    ] = dimensions_sorted
    called_kwargs.druid_spec["dataSchema"]["metricsSpec"] = sorted(
        called_kwargs.druid_spec["dataSchema"]["metricsSpec"],
        key=lambda x: x["fieldName"],
    )
    assert sorted(called_kwargs.columns, key=lambda x: x.name) == sorted(
        [
            ColumnMetadata(
                name="default_DOT_dispatcher_DOT_company_name",
                type="string",
            ),
            ColumnMetadata(name="default_DOT_hard_hat_DOT_city", type="string"),
            ColumnMetadata(name="default_DOT_hard_hat_DOT_country", type="string"),
            ColumnMetadata(name="default_DOT_hard_hat_DOT_hire_date", type="timestamp"),
            ColumnMetadata(name="default_DOT_hard_hat_DOT_postal_code", type="string"),
            ColumnMetadata(name="default_DOT_hard_hat_DOT_state", type="string"),
            ColumnMetadata(
                name="default_DOT_municipality_dim_DOT_local_region",
                type="string",
            ),
            ColumnMetadata(
                name="default_DOT_repair_order_details_discount3789599758_sum",
                type="bigint",
            ),
            ColumnMetadata(
                name="default_DOT_repair_order_details_placeholder_count",
                type="bigint",
            ),
            ColumnMetadata(
                name="default_DOT_repair_order_details_price3402113753_count",
                type="bigint",
            ),
            ColumnMetadata(
                name="default_DOT_repair_order_details_price3402113753_sum",
                type="double",
            ),
            ColumnMetadata(
                name="default_DOT_repair_order_details_price_discount2203488025_sum",
                type="double",
            ),
            ColumnMetadata(
                name="default_DOT_repair_orders_repair_order_id3825669267_count",
                type="bigint",
            ),
        ],
        key=lambda x: x.name,
    )
    assert called_kwargs.druid_spec == {
        "dataSchema": {
            "dataSource": "default_DOT_repairs_cube",
            "granularitySpec": {
                "intervals": [],
                "segmentGranularity": "day",
                "type": "uniform",
            },
            "metricsSpec": [
                {
                    "fieldName": "default_DOT_repair_order_details_discount3789599758_sum",
                    "name": "discount3789599758_sum",
                    "type": "longSum",
                },
                {
                    "fieldName": "default_DOT_repair_order_details_placeholder_count",
                    "name": "placeholder_count",
                    "type": "longSum",
                },
                {
                    "fieldName": "default_DOT_repair_order_details_price3402113753_count",
                    "name": "price3402113753_count",
                    "type": "longSum",
                },
                {
                    "fieldName": "default_DOT_repair_order_details_price3402113753_sum",
                    "name": "price3402113753_sum",
                    "type": "doubleSum",
                },
                {
                    "fieldName": "default_DOT_repair_order_details_price_discount2203488025_sum",
                    "name": "price_discount2203488025_sum",
                    "type": "doubleSum",
                },
                {
                    "fieldName": "default_DOT_repair_orders_repair_order_id3825669267_count",
                    "name": "repair_order_id3825669267_count",
                    "type": "longSum",
                },
            ],
            "parser": {
                "parseSpec": {
                    "dimensionsSpec": {
                        "dimensions": [
                            "default_DOT_dispatcher_DOT_company_name",
                            "default_DOT_hard_hat_DOT_city",
                            "default_DOT_hard_hat_DOT_country",
                            "default_DOT_hard_hat_DOT_hire_date",
                            "default_DOT_hard_hat_DOT_postal_code",
                            "default_DOT_hard_hat_DOT_state",
                            "default_DOT_municipality_dim_DOT_local_region",
                        ],
                    },
                    "format": "parquet",
                    "timestampSpec": {
                        "column": "default_DOT_hard_hat_DOT_hire_date",
                        "format": "yyyyMMdd",
                    },
                },
            },
        },
    }
    response = client_with_repairs_cube.get("/nodes/default.repairs_cube/")
    materializations = response.json()["materializations"]
    assert len(materializations) == 2
    druid_materialization = [
        materialization
        for materialization in materializations
        if materialization["engine"]["name"] == "druid"
    ][0]
    assert druid_materialization["engine"] == {
        "name": "druid",
        "version": "",
        "uri": None,
        "dialect": "druid",
    }
    assert set(druid_materialization["config"]["dimensions"]) == {
        "default_DOT_dispatcher_DOT_company_name",
        "default_DOT_hard_hat_DOT_city",
        "default_DOT_hard_hat_DOT_country",
        "default_DOT_hard_hat_DOT_hire_date",
        "default_DOT_hard_hat_DOT_postal_code",
        "default_DOT_hard_hat_DOT_state",
        "default_DOT_municipality_dim_DOT_local_region",
    }
    assert druid_materialization["config"]["measures"] == {
        "default_DOT_avg_repair_price": {
            "combiner": "sum(price3402113753_sum) / " "count(price3402113753_count)",
            "measures": [
                {
                    "agg": "count",
                    "field_name": "default_DOT_repair_order_details_price3402113753_count",
                    "name": "price3402113753_count",
                    "type": "bigint",
                },
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_price3402113753_sum",
                    "name": "price3402113753_sum",
                    "type": "double",
                },
            ],
            "metric": "default_DOT_avg_repair_price",
        },
        "default_DOT_discounted_orders_rate": {
            "combiner": "sum(discount3789599758_sum) " "/ " "count(placeholder_count)",
            "measures": [
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_discount3789599758_sum",
                    "name": "discount3789599758_sum",
                    "type": "bigint",
                },
                {
                    "agg": "count",
                    "field_name": "default_DOT_repair_order_details_placeholder_count",
                    "name": "placeholder_count",
                    "type": "bigint",
                },
            ],
            "metric": "default_DOT_discounted_orders_rate",
        },
        "default_DOT_double_total_repair_cost": {
            "combiner": "sum(price3402113753_sum) " "+ " "sum(price3402113753_sum)",
            "measures": [
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_price3402113753_sum",
                    "name": "price3402113753_sum",
                    "type": "double",
                },
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_price3402113753_sum",
                    "name": "price3402113753_sum",
                    "type": "double",
                },
            ],
            "metric": "default_DOT_double_total_repair_cost",
        },
        "default_DOT_num_repair_orders": {
            "combiner": "count(repair_order_id3825669267_count)",
            "measures": [
                {
                    "agg": "count",
                    "field_name": "default_DOT_repair_orders_repair_order_id3825669267_count",
                    "name": "repair_order_id3825669267_count",
                    "type": "bigint",
                },
            ],
            "metric": "default_DOT_num_repair_orders",
        },
        "default_DOT_total_repair_cost": {
            "combiner": "sum(price3402113753_sum)",
            "measures": [
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_price3402113753_sum",
                    "name": "price3402113753_sum",
                    "type": "double",
                },
            ],
            "metric": "default_DOT_total_repair_cost",
        },
        "default_DOT_total_repair_order_discounts": {
            "combiner": "sum(price_discount2203488025_sum)",
            "measures": [
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_price_discount2203488025_sum",
                    "name": "price_discount2203488025_sum",
                    "type": "double",
                },
            ],
            "metric": "default_DOT_total_repair_order_discounts",
        },
    }
    assert druid_materialization["schedule"] == "@daily"


def test_cube_sql_generation_with_availability(
    client_with_repairs_cube: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Test generating SQL for metrics + dimensions in a cube after adding a cube materialization
    """
    client_with_repairs_cube.post(
        "/data/default.repairs_cube/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": "repairs_cube",
            "valid_through_ts": 1010129120,
        },
    )

    # Ask for SQL with metrics, dimensions, filters, order by, and limit
    response = client_with_repairs_cube.get(
        "/sql/",
        params={
            "metrics": [
                "default.discounted_orders_rate",
                "default.num_repair_orders",
                "default.avg_repair_price",
            ],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.postal_code",
                "default.hard_hat.hire_date",
            ],
            "filters": ["default.hard_hat.country='NZ'"],
            "orderby": ["default.hard_hat.country ASC"],
            "limit": 100,
        },
    )
    data = response.json()
    assert data["columns"] == [
        {"name": "default_DOT_discounted_orders_rate", "type": "double"},
        {"name": "default_DOT_num_repair_orders", "type": "bigint"},
        {"name": "default_DOT_avg_repair_price", "type": "double"},
        {"name": "default_DOT_hard_hat_DOT_country", "type": "string"},
        {"name": "default_DOT_hard_hat_DOT_postal_code", "type": "string"},
        {"name": "default_DOT_hard_hat_DOT_hire_date", "type": "timestamp"},
    ]
    assert compare_query_strings(
        data["sql"],
        """SELECT
  sum(discount3789599758_sum) / count(placeholder_count) default_DOT_discounted_orders_rate,
  count(repair_order_id3825669267_count) default_DOT_num_repair_orders,
  sum(price3402113753_sum) / count(price3402113753_count) default_DOT_avg_repair_price,
  default_DOT_hard_hat_DOT_country,
  default_DOT_hard_hat_DOT_postal_code,
  default_DOT_hard_hat_DOT_hire_date
FROM repairs_cube
WHERE
  default_DOT_hard_hat_DOT_country = 'NZ'
GROUP BY
  default_DOT_hard_hat_DOT_country,
  default_DOT_hard_hat_DOT_postal_code,
  default_DOT_hard_hat_DOT_hire_date
ORDER BY default_DOT_hard_hat_DOT_country ASC
LIMIT 100""",
    )

    # Ask for SQL with only metrics and dimensions
    response = client_with_repairs_cube.get(
        "/sql/",
        params={
            "metrics": [
                "default.discounted_orders_rate",
                "default.num_repair_orders",
                "default.avg_repair_price",
            ],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.postal_code",
                "default.hard_hat.hire_date",
            ],
        },
    )
    data = response.json()
    assert data["columns"] == [
        {"name": "default_DOT_discounted_orders_rate", "type": "double"},
        {"name": "default_DOT_num_repair_orders", "type": "bigint"},
        {"name": "default_DOT_avg_repair_price", "type": "double"},
        {"name": "default_DOT_hard_hat_DOT_country", "type": "string"},
        {"name": "default_DOT_hard_hat_DOT_postal_code", "type": "string"},
        {"name": "default_DOT_hard_hat_DOT_hire_date", "type": "timestamp"},
    ]
    print("datasql", data["sql"])
    assert compare_query_strings(
        data["sql"],
        """SELECT
  sum(discount3789599758_sum) / count(placeholder_count) default_DOT_discounted_orders_rate,
  count(repair_order_id3825669267_count) default_DOT_num_repair_orders,
  sum(price3402113753_sum) / count(price3402113753_count) default_DOT_avg_repair_price,
  default_DOT_hard_hat_DOT_country,
  default_DOT_hard_hat_DOT_postal_code,
  default_DOT_hard_hat_DOT_hire_date
FROM repairs_cube
GROUP BY
  default_DOT_hard_hat_DOT_country,
  default_DOT_hard_hat_DOT_postal_code,
  default_DOT_hard_hat_DOT_hire_date""",
    )


def test_unlink_node_column_dimension(
    client_with_repairs_cube: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    When a node column link to a dimension is removed, the cube should be invalidated
    """
    response = client_with_repairs_cube.delete(
        "/nodes/default.repair_order/columns/hard_hat_id/"
        "?dimension=default.hard_hat&dimension_column=hard_hat_id",
    )
    assert response.json() == {
        "message": "The dimension link on the node default.repair_order's hard_hat_id "
        "to default.hard_hat has been successfully removed.",
    }
    response = client_with_repairs_cube.get("/nodes/default.repairs_cube")
    data = response.json()
    assert data["status"] == "invalid"


def test_changing_node_upstream_from_cube(
    client_with_repairs_cube: TestClient,  # pylint: disable=redefined-outer-name
    repairs_cube_elements: List[Dict],  # pylint: disable=redefined-outer-name
):
    """
    Verify changing nodes upstream from a cube
    """
    # Verify effects on cube after deactivating a node upstream from the cube
    client_with_repairs_cube.delete("/nodes/default.repair_orders/")
    response = client_with_repairs_cube.get("/nodes/default.repairs_cube/")
    data = response.json()
    assert data["status"] == "invalid"

    # Verify effects on cube after restoring a node upstream from the cube
    client_with_repairs_cube.post("/nodes/default.repair_orders/restore/")
    response = client_with_repairs_cube.get("/nodes/default.repairs_cube/")
    data = response.json()
    assert data["status"] == "valid"

    response = client_with_repairs_cube.get("/cubes/default.repairs_cube/")
    data = response.json()
    assert (
        sorted(data["cube_elements"], key=lambda x: x["name"]) == repairs_cube_elements
    )

    # Verify effects on cube after updating a node upstream from the cube
    client_with_repairs_cube.patch(
        "/nodes/default.discounted_orders_rate/",
        json={
            "query": """SELECT
  cast(sum(if(discount > 0.0, 1, 0)) as double)
FROM default.repair_order_details""",
        },
    )
    response = client_with_repairs_cube.get("/nodes/default.repairs_cube/")
    data = response.json()
    assert data["status"] == "valid"

    response = client_with_repairs_cube.get("/cubes/default.repairs_cube/")
    data = response.json()
    assert (
        sorted(data["cube_elements"], key=lambda x: x["name"]) == repairs_cube_elements
    )


def assert_updated_repairs_cube(data):
    """
    Asserts that the updated repairs cube has the right cube elements and default materialization
    """
    assert sorted(data["cube_elements"], key=lambda x: x["name"]) == [
        {
            "display_name": "City",
            "name": "city",
            "node_name": "default.hard_hat",
            "type": "dimension",
            "partition": None,
        },
        {
            "display_name": "Default: Discounted Orders Rate",
            "name": "default_DOT_discounted_orders_rate",
            "node_name": "default.discounted_orders_rate",
            "type": "metric",
            "partition": None,
        },
        {
            "display_name": "Hire Date",
            "name": "hire_date",
            "node_name": "default.hard_hat",
            "partition": None,
            "type": "dimension",
        },
    ]

    assert data["materializations"][0]["config"]["dimensions"] == [
        "default_DOT_hard_hat_DOT_city",
        "default_DOT_hard_hat_DOT_hire_date",
    ]
    assert data["materializations"][0]["config"]["measures"] == {
        "default_DOT_discounted_orders_rate": {
            "combiner": "sum(discount3789599758_sum) " "/ " "count(placeholder_count)",
            "measures": [
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_discount3789599758_sum",
                    "name": "discount3789599758_sum",
                    "type": "bigint",
                },
                {
                    "agg": "count",
                    "field_name": "default_DOT_repair_order_details_placeholder_count",
                    "name": "placeholder_count",
                    "type": "bigint",
                },
            ],
            "metric": "default_DOT_discounted_orders_rate",
        },
    }
    assert "discount3789599758_sum" in data["materializations"][0]["config"]["query"]
    assert data["materializations"][0]["config"]["upstream_tables"] == [
        "default.roads.hard_hats",
        "default.roads.repair_order_details",
        "default.roads.repair_orders",
    ]
    assert data["materializations"][0]["job"] == "DefaultCubeMaterialization"
    assert data["materializations"][0]["name"] == "default"


def test_updating_cube(
    client_with_repairs_cube: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Verify updating a cube
    """
    # Check a minor update to the cube
    response = client_with_repairs_cube.patch(
        "/nodes/default.repairs_cube",
        json={
            "description": "This cube has a new description",
        },
    )
    data = response.json()
    assert data["version"] == "v1.1"
    assert data["description"] == "This cube has a new description"

    # Check a major update to the cube
    response = client_with_repairs_cube.patch(
        "/nodes/default.repairs_cube",
        json={
            "metrics": ["default.discounted_orders_rate"],
            "dimensions": ["default.hard_hat.city", "default.hard_hat.hire_date"],
        },
    )
    result = response.json()
    assert result["version"] == "v2.0"
    assert sorted(result["columns"], key=lambda x: x["name"]) == sorted(
        [
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Default: Discounted Orders Rate",
                "name": "default_DOT_discounted_orders_rate",
                "type": "double",
                "partition": None,
            },
            {
                "attributes": [
                    {"attribute_type": {"name": "dimension", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "City",
                "name": "default_DOT_hard_hat_DOT_city",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [
                    {"attribute_type": {"name": "dimension", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Hire Date",
                "name": "default_DOT_hard_hat_DOT_hire_date",
                "partition": None,
                "type": "timestamp",
            },
        ],
        key=lambda x: x["name"],  # type: ignore
    )

    response = client_with_repairs_cube.get("/cubes/default.repairs_cube/")
    data = response.json()
    assert_updated_repairs_cube(data)

    response = client_with_repairs_cube.get("/history?node=default.repairs_cube")
    assert [
        event for event in response.json() if event["activity_type"] == "update"
    ] == [
        {
            "activity_type": "update",
            "created_at": mock.ANY,
            "details": {"version": "v1.1"},
            "entity_name": "default.repairs_cube",
            "entity_type": "node",
            "id": mock.ANY,
            "node": "default.repairs_cube",
            "post": {},
            "pre": {},
            "user": "dj",
        },
        {
            "activity_type": "update",
            "created_at": mock.ANY,
            "details": {"version": "v2.0"},
            "entity_name": "default.repairs_cube",
            "entity_type": "node",
            "id": mock.ANY,
            "node": "default.repairs_cube",
            "post": {},
            "pre": {},
            "user": "dj",
        },
    ]


def test_updating_cube_with_existing_materialization(
    client_with_repairs_cube: TestClient,  # pylint: disable=redefined-outer-name
    query_service_client: QueryServiceClient,
    repairs_cube_with_materialization: requests.Response,  # pylint: disable=redefined-outer-name
):
    """
    Verify updating a cube with existing materialization
    """
    assert repairs_cube_with_materialization.ok

    # Make sure that the cube already has an additional materialization configured
    response = client_with_repairs_cube.get("/cubes/default.repairs_cube/")
    data = response.json()
    assert len(data["materializations"]) == 2

    # Update the existing materialization config
    response = client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization",
        json={
            "engine": {"name": "druid", "version": ""},
            "config": {"spark": {"spark.executor.memory": "6g"}},
            "schedule": "@daily",
        },
    )
    data = response.json()
    assert data == {
        "message": "Successfully updated materialization config named "
        "`default_DOT_hard_hat_DOT_hire_date_druid` for node "
        "`default.repairs_cube`",
        "urls": [["http://fake.url/job"]],
    }

    # Check that the configured materialization was updated
    response = client_with_repairs_cube.get("/cubes/default.repairs_cube/")
    data = response.json()
    assert data["materializations"][1]["config"]["spark"] == {
        "spark.executor.memory": "6g",
    }

    # Update the cube, but keep the temporal partition column. This should succeed
    response = client_with_repairs_cube.patch(
        "/nodes/default.repairs_cube",
        json={
            "metrics": ["default.discounted_orders_rate"],
            "dimensions": ["default.hard_hat.city", "default.hard_hat.hire_date"],
        },
    )
    result = response.json()
    assert result["version"] == "v2.0"

    # Check that the query service was called to materialize
    last_call_args = (
        query_service_client.materialize.call_args_list[-1].args[0].dict()  # type: ignore
    )
    assert last_call_args["name"] == "default_DOT_hard_hat_DOT_hire_date_druid"
    assert last_call_args["node_name"] == "default.repairs_cube"
    assert last_call_args["node_version"] == "v2.0"
    assert last_call_args["node_type"] == "cube"
    assert last_call_args["schedule"] == "@daily"
    assert last_call_args["druid_spec"]["dataSchema"]["parser"]["parseSpec"][
        "timestampSpec"
    ] == {
        "column": "default_DOT_hard_hat_DOT_hire_date",
        "format": "yyyyMMdd",
    }

    # Check that the cube was updated
    response = client_with_repairs_cube.get("/cubes/default.repairs_cube/")
    data = response.json()
    assert_updated_repairs_cube(data)

    # Check that the existing materialization was updated
    assert data["materializations"][1] == {
        "backfills": [],
        "config": {
            "columns": [
                {"name": mock.ANY, "type": mock.ANY},
                {"name": mock.ANY, "type": mock.ANY},
                {"name": mock.ANY, "type": mock.ANY},
                {"name": mock.ANY, "type": mock.ANY},
            ],
            "dimensions": [
                "default_DOT_hard_hat_DOT_city",
                "default_DOT_hard_hat_DOT_hire_date",
            ],
            "druid": None,
            "measures": {
                "default_DOT_discounted_orders_rate": {
                    "combiner": "sum(discount3789599758_sum) "
                    "/ "
                    "count(placeholder_count)",
                    "measures": [
                        {
                            "agg": "sum",
                            "field_name": "default_DOT_repair_order_details_discount3789599758_sum",
                            "name": "discount3789599758_sum",
                            "type": "bigint",
                        },
                        {
                            "agg": "count",
                            "field_name": "default_DOT_repair_order_details_placeholder_count",
                            "name": "placeholder_count",
                            "type": "bigint",
                        },
                    ],
                    "metric": "default_DOT_discounted_orders_rate",
                },
            },
            "prefix": "",
            "query": mock.ANY,
            "spark": {"spark.executor.memory": "6g"},
            "suffix": "",
            "upstream_tables": [
                "default.roads.hard_hats",
                "default.roads.repair_order_details",
                "default.roads.repair_orders",
            ],
        },
        "engine": {"dialect": "druid", "name": "druid", "uri": None, "version": ""},
        "job": "DruidCubeMaterializationJob",
        "name": "default_DOT_hard_hat_DOT_hire_date_druid",
        "schedule": "@daily",
    }

    response = client_with_repairs_cube.get("/history?node=default.repairs_cube")
    assert [
        event for event in response.json() if event["activity_type"] == "update"
    ] == [
        {
            "activity_type": "update",
            "created_at": mock.ANY,
            "details": {
                "materialization": "default_DOT_hard_hat_DOT_hire_date_druid",
                "node": "default.repairs_cube",
            },
            "entity_name": "default_DOT_hard_hat_DOT_hire_date_druid",
            "entity_type": "materialization",
            "id": mock.ANY,
            "node": "default.repairs_cube",
            "post": {},
            "pre": {},
            "user": "dj",
        },
        {
            "activity_type": "update",
            "created_at": mock.ANY,
            "details": {"version": "v2.0"},
            "entity_name": "default.repairs_cube",
            "entity_type": "node",
            "id": mock.ANY,
            "node": "default.repairs_cube",
            "post": {},
            "pre": {},
            "user": "dj",
        },
        {
            "activity_type": "update",
            "created_at": mock.ANY,
            "details": {},
            "entity_name": "default_DOT_hard_hat_DOT_hire_date_druid",
            "entity_type": "materialization",
            "id": mock.ANY,
            "node": "default.repairs_cube",
            "post": {},
            "pre": {},
            "user": "dj",
        },
    ]

    # Update the cube, but remove the temporal partition column. This should fail when
    # trying to update the cube's materialization config
    response = client_with_repairs_cube.patch(
        "/nodes/default.repairs_cube",
        json={
            "metrics": ["default.discounted_orders_rate"],
            "dimensions": ["default.hard_hat.city"],
        },
    )
    result = response.json()
    assert result["message"] == (
        "The cube materialization cannot be configured if there is no temporal partition "
        "specified on the cube. Please make sure at least one cube element has a temporal "
        "partition defined"
    )
