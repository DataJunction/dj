"""
Tests for the cubes API.
"""

from typing import Dict, Iterator
from unittest import mock

import pytest
import pytest_asyncio
from httpx import AsyncClient

from datajunction_server.internal.nodes import derive_sql_column
from datajunction_server.models.cube import CubeElementMetadata
from datajunction_server.models.node import ColumnOutput
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing.backends.antlr4 import parse
from tests.sql.utils import compare_query_strings


async def make_a_test_cube(
    client: AsyncClient,
    cube_name: str,
    with_materialization: bool = True,
    metrics_or_measures: str = "metrics",
):
    """
    Make a new cube with a temporal partition.
    """
    # Make an isolated cube
    metrics_list = [
        "default.discounted_orders_rate",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.total_repair_order_discounts",
    ]
    await client.post(
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
            "name": f"{cube_name}",
        },
    )
    if with_materialization:
        # add materialization to the cube
        await client.post(
            f"/nodes/{cube_name}/columns/default.hard_hat.hire_date/partition",
            json={
                "type_": "temporal",
                "granularity": "day",
                "format": "yyyyMMdd",
            },
        )
        await client.post(
            f"/nodes/{cube_name}/materialization/",
            json={
                "job": "druid_metrics_cube"
                if metrics_or_measures == "metrics"
                else "druid_measures_cube",
                "strategy": "incremental_time",
                "config": {
                    "spark": {},
                },
                "schedule": "",
            },
        )


@pytest.mark.asyncio
async def test_read_cube(module__client_with_account_revenue: AsyncClient) -> None:
    """
    Test ``GET /cubes/{name}``.
    """
    # Create a cube
    response = await module__client_with_account_revenue.post(
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
    assert data["display_name"] == "Number Of Accounts By Account Type"
    assert data["mode"] == "published"
    assert data["tags"] == []

    # Read the cube
    response = await module__client_with_account_revenue.get(
        "/cubes/default.number_of_accounts_by_account_type",
    )
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "cube"
    assert data["name"] == "default.number_of_accounts_by_account_type"
    assert data["display_name"] == "Number Of Accounts By Account Type"
    assert data["version"] == "v1.0"
    assert data["description"] == "A cube of number of accounts grouped by account type"


@pytest.mark.asyncio
async def test_create_invalid_cube(module__client_with_account_revenue: AsyncClient):
    """
    Check that creating a cube with a query fails appropriately
    """
    response = await module__client_with_account_revenue.post(
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
    response = await module__client_with_account_revenue.post(
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
        "errors": [
            {
                "code": 204,
                "context": "",
                "debug": None,
                "message": "Node default.account_type of type dimension cannot be added to a "
                "cube. Did you mean to add a dimension attribute?",
            },
        ],
        "warnings": [],
    }

    # Check that creating a cube with incompatible nodes fails appropriately
    response = await module__client_with_account_revenue.post(
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
        "errors": [
            {
                "code": 601,
                "context": "",
                "debug": None,
                "message": "The dimension attribute `default.payment_type.payment_type_name` "
                "is not available on every metric and thus cannot be included.",
            },
        ],
        "warnings": [],
    }

    # Check that creating a cube with no metric nodes fails appropriately
    response = await module__client_with_account_revenue.post(
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


@pytest.mark.asyncio
async def test_create_cube_no_dimensions(
    module__client_with_account_revenue: AsyncClient,
):
    """
    Check that creating a cube with no dimension attributes works
    """
    response = await module__client_with_account_revenue.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.number_of_account_types"],
            "dimensions": [],
            "description": "A cube of number of accounts grouped by account type",
            "mode": "published",
            "name": "default.cubes_can_have_no_dimensions",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["parents"] == [{"name": "default.number_of_account_types"}]


@pytest.mark.asyncio
async def test_raise_on_cube_with_multiple_catalogs(
    module__client_with_both_basics,
) -> None:
    """
    Test raising when creating a cube with multiple catalogs
    """
    # Create a cube
    response = await module__client_with_both_basics.post(
        "/nodes/cube/",
        json={
            "metrics": ["basic.num_users", "different.basic.num_users"],
            "dimensions": ["basic.dimension.users.country"],
            "description": "multicatalog cube's raise an error",
            "mode": "published",
            "name": "default.multicatalog",
        },
    )
    assert response.status_code >= 400
    data = response.json()
    assert "Metrics and dimensions cannot be from multiple catalogs" in data["message"]


@pytest_asyncio.fixture(scope="module")
async def client_with_repairs_cube(
    module__client_with_roads: AsyncClient,
):
    """
    Adds a repairs cube with a new double total repair cost metric to the test client
    """
    metrics_list = [
        "default.discounted_orders_rate",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.total_repair_order_discounts",
    ]

    # Metric that doubles the total repair cost to test the sum(x) + sum(y) scenario
    await module__client_with_roads.post(
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
    response = await module__client_with_roads.post(
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
                "default.hard_hat_to_delete.hire_date",
            ],
            "filters": ["default.hard_hat.state='AZ'"],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.repairs_cube",
        },
    )
    assert response.status_code == 201
    assert response.json()["version"] == "v1.0"
    return module__client_with_roads


@pytest.fixture
def repair_orders_cube_measures() -> Dict:
    """
    Fixture for repair orders cube metrics to measures mapping.
    """
    return {
        "default.double_total_repair_cost": {
            "combiner": "sum(default_DOT_repair_order_details_DOT_price) + "
            "sum(default_DOT_repair_order_details_DOT_price) AS ",
            "measures": [
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_DOT_price",
                    "name": "default.repair_order_details.price",
                    "type": "float",
                },
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_order_details_DOT_price",
                    "name": "default.repair_order_details.price",
                    "type": "float",
                },
            ],
            "metric": "default.double_total_repair_cost",
        },
    }


@pytest.fixture
def repairs_cube_elements():
    """
    Fixture of repairs cube elements
    """
    return [
        {
            "display_name": "Discounted Orders Rate",
            "name": "default_DOT_discounted_orders_rate",
            "node_name": "default.discounted_orders_rate",
            "partition": None,
            "type": "metric",
        },
        {
            "display_name": "Company Name",
            "name": "company_name",
            "node_name": "default.dispatcher",
            "partition": None,
            "type": "dimension",
        },
        {
            "display_name": "Local Region",
            "name": "local_region",
            "node_name": "default.municipality_dim",
            "partition": None,
            "type": "dimension",
        },
        {
            "display_name": "Hire Date",
            "name": "hire_date",
            "node_name": "default.hard_hat",
            "partition": None,
            "type": "dimension",
        },
        {
            "display_name": "City",
            "name": "city",
            "node_name": "default.hard_hat",
            "partition": None,
            "type": "dimension",
        },
        {
            "display_name": "State",
            "name": "state",
            "node_name": "default.hard_hat",
            "partition": None,
            "type": "dimension",
        },
        {
            "display_name": "Postal Code",
            "name": "postal_code",
            "node_name": "default.hard_hat",
            "partition": None,
            "type": "dimension",
        },
        {
            "display_name": "Country",
            "name": "country",
            "node_name": "default.hard_hat",
            "partition": None,
            "type": "dimension",
        },
        {
            "display_name": "Num Repair Orders",
            "name": "default_DOT_num_repair_orders",
            "node_name": "default.num_repair_orders",
            "partition": None,
            "type": "metric",
        },
        {
            "display_name": "Avg Repair Price",
            "name": "default_DOT_avg_repair_price",
            "node_name": "default.avg_repair_price",
            "partition": None,
            "type": "metric",
        },
        {
            "display_name": "Total Repair Cost",
            "name": "default_DOT_total_repair_cost",
            "node_name": "default.total_repair_cost",
            "partition": None,
            "type": "metric",
        },
        {
            "display_name": "Total Repair Order Discounts",
            "name": "default_DOT_total_repair_order_discounts",
            "node_name": "default.total_repair_order_discounts",
            "partition": None,
            "type": "metric",
        },
        {
            "display_name": "Double Total Repair Cost",
            "name": "default_DOT_double_total_repair_cost",
            "node_name": "default.double_total_repair_cost",
            "partition": None,
            "type": "metric",
        },
    ]


@pytest.mark.asyncio
async def test_invalid_cube(module__client_with_roads: AsyncClient):
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
    response = await module__client_with_roads.post(
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


@pytest.mark.asyncio
async def test_create_cube_failures(
    module__client_with_roads: AsyncClient,
):
    """
    Test create cube failure cases
    """
    # Creating a cube with a metric that doesn't exist should fail
    response = await module__client_with_roads.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.metric_that_doesnt_exist"],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.postal_code",
            ],
            "description": "Cube with metric that doesn't exist",
            "mode": "published",
            "name": "default.bad_cube",
        },
    )
    assert response.status_code == 404
    assert response.json() == {
        "message": "The following metric nodes were not found: default.metric_that_doesnt_exist",
        "errors": [
            {
                "code": 203,
                "context": "",
                "debug": None,
                "message": "The following metric nodes were not found: "
                "default.metric_that_doesnt_exist",
            },
        ],
        "warnings": [],
    }


@pytest.mark.asyncio
async def test_create_cube_similar_dimensions(
    module__client_with_roads: AsyncClient,
):
    """
    Tests cube creation for dimension attributes with the same name
    but from different dimension nodes.
    """

    metrics_list = [
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
    ]

    await module__client_with_roads.post("/nodes/default.repair_order_fact/")
    # Should succeed
    response = await module__client_with_roads.post(
        "/nodes/cube/",
        json={
            "metrics": metrics_list,
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.postal_code",
                "default.hard_hat_to_delete.postal_code",
            ],
            "filters": ["default.hard_hat.state='AZ'"],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.repairs_cube2",
        },
    )
    assert response.status_code == 201
    assert response.json()["version"] == "v1.0"


@pytest.mark.asyncio
async def test_create_cube(
    client_with_repairs_cube: AsyncClient,
):
    """
    Tests cube creation and the generated cube SQL
    """
    response = await client_with_repairs_cube.get("/nodes/default.repairs_cube/")
    results = response.json()

    assert results["name"] == "default.repairs_cube"
    assert results["display_name"] == "Repairs Cube"
    assert results["description"] == "Cube of various metrics related to repairs"

    response = await client_with_repairs_cube.get("/cubes/default.repairs_cube")
    cube = response.json()

    # Make sure it matches the original metrics order
    metrics = [
        "default.discounted_orders_rate",
        "default.num_repair_orders",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.total_repair_order_discounts",
        "default.double_total_repair_cost",
    ]
    assert cube["cube_node_metrics"] == metrics
    assert [
        elem["node_name"] for elem in cube["cube_elements"] if elem["type"] == "metric"
    ] == metrics
    dimensions = [
        "default.hard_hat.country",
        "default.hard_hat.postal_code",
        "default.hard_hat.city",
        "default.hard_hat.state",
        "default.dispatcher.company_name",
        "default.municipality_dim.local_region",
        "default.hard_hat_to_delete.hire_date",
    ]
    assert cube["cube_node_dimensions"] == dimensions

    metrics_query = "&".join([f"metrics={metric}" for metric in metrics])
    dimensions_query = "&".join([f"dimensions={dim}" for dim in dimensions])
    response = await client_with_repairs_cube.get(
        f"/sql?{metrics_query}&{dimensions_query}&filters=default.hard_hat.state='AZ'",
    )
    metrics_sql_results = response.json()
    response = await client_with_repairs_cube.get(
        "/sql/default.repairs_cube?filters=default.hard_hat.state='AZ'",
    )
    cube_sql_results = response.json()
    expected_query = """
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
), default_DOT_hard_hat AS (
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
  WHERE
    default_DOT_hard_hats.state = 'AZ'
), default_DOT_dispatcher AS (
  SELECT
    default_DOT_dispatchers.dispatcher_id,
    default_DOT_dispatchers.company_name,
    default_DOT_dispatchers.phone
  FROM roads.dispatchers AS default_DOT_dispatchers
), default_DOT_municipality_dim AS (
  SELECT
    m.municipality_id AS municipality_id,
    m.contact_name,
    m.contact_title,
    m.local_region,
    m.state_id,
    mmt.municipality_type_id AS municipality_type_id,
    mt.municipality_type_desc AS municipality_type_desc
  FROM roads.municipality AS m
  LEFT JOIN roads.municipality_municipality_type AS mmt
    ON m.municipality_id = mmt.municipality_id
  LEFT JOIN roads.municipality_type AS mt
    ON mmt.municipality_type_id = mt.municipality_type_desc
), default_DOT_hard_hat_to_delete AS (
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
), default_DOT_repair_order_details AS (
  SELECT
    default_DOT_repair_order_details.repair_order_id,
    default_DOT_repair_order_details.repair_type_id,
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.quantity,
    default_DOT_repair_order_details.discount
  FROM roads.repair_order_details AS default_DOT_repair_order_details
), default_DOT_repair_order AS (
  SELECT
    default_DOT_repair_orders.repair_order_id,
    default_DOT_repair_orders.municipality_id,
    default_DOT_repair_orders.hard_hat_id,
    default_DOT_repair_orders.order_date,
    default_DOT_repair_orders.required_date,
    default_DOT_repair_orders.dispatched_date,
    default_DOT_repair_orders.dispatcher_id
  FROM roads.repair_orders AS default_DOT_repair_orders
), default_DOT_repair_orders_fact_metrics AS (
  SELECT
    default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
    default_DOT_hard_hat.postal_code default_DOT_hard_hat_DOT_postal_code,
    default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
    default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
    default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
    default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region,
    default_DOT_hard_hat_to_delete.hire_date default_DOT_hard_hat_to_delete_DOT_hire_date,
    CAST(sum(if(default_DOT_repair_orders_fact.discount > 0.0, 1, 0)) AS DOUBLE) / count(*)
      AS default_DOT_discounted_orders_rate,
    count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders,
    avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
    sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost,
    sum(default_DOT_repair_orders_fact.price * default_DOT_repair_orders_fact.discount)
      default_DOT_total_repair_order_discounts
  FROM default_DOT_repair_orders_fact
  INNER JOIN default_DOT_hard_hat
    ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
  INNER JOIN default_DOT_dispatcher
    ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
  INNER JOIN default_DOT_municipality_dim
    ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
  LEFT JOIN default_DOT_hard_hat_to_delete
    ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat_to_delete.hard_hat_id
  GROUP BY
    default_DOT_hard_hat.country,
    default_DOT_hard_hat.postal_code,
    default_DOT_hard_hat.city,
    default_DOT_hard_hat.state,
    default_DOT_dispatcher.company_name,
    default_DOT_municipality_dim.local_region,
    default_DOT_hard_hat_to_delete.hire_date
), default_DOT_repair_order_details_metrics AS (
  SELECT
    default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
    default_DOT_hard_hat.postal_code default_DOT_hard_hat_DOT_postal_code,
    default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
    default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
    default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
    default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region,
    default_DOT_hard_hat_to_delete.hire_date default_DOT_hard_hat_to_delete_DOT_hire_date,
    sum(default_DOT_repair_order_details.price) + sum(default_DOT_repair_order_details.price)
      AS default_DOT_double_total_repair_cost
  FROM default_DOT_repair_order_details
  INNER JOIN default_DOT_repair_order
    ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
  INNER JOIN default_DOT_hard_hat
    ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
  INNER JOIN default_DOT_dispatcher
    ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
  INNER JOIN default_DOT_municipality_dim
    ON default_DOT_repair_order.municipality_id = default_DOT_municipality_dim.municipality_id
  LEFT JOIN default_DOT_hard_hat_to_delete
    ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat_to_delete.hard_hat_id
  GROUP BY
    default_DOT_hard_hat.country,
    default_DOT_hard_hat.postal_code,
    default_DOT_hard_hat.city,
    default_DOT_hard_hat.state,
    default_DOT_dispatcher.company_name,
    default_DOT_municipality_dim.local_region,
    default_DOT_hard_hat_to_delete.hire_date
)
SELECT
  default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_country,
  default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_postal_code,
  default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_city,
  default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_state,
  default_DOT_repair_orders_fact_metrics.default_DOT_dispatcher_DOT_company_name,
  default_DOT_repair_orders_fact_metrics.default_DOT_municipality_dim_DOT_local_region,
  default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_to_delete_DOT_hire_date,
  default_DOT_repair_orders_fact_metrics.default_DOT_discounted_orders_rate,
  default_DOT_repair_orders_fact_metrics.default_DOT_num_repair_orders,
  default_DOT_repair_orders_fact_metrics.default_DOT_avg_repair_price,
  default_DOT_repair_orders_fact_metrics.default_DOT_total_repair_cost,
  default_DOT_repair_orders_fact_metrics.default_DOT_total_repair_order_discounts,
  default_DOT_repair_order_details_metrics.default_DOT_double_total_repair_cost
FROM default_DOT_repair_orders_fact_metrics
FULL JOIN default_DOT_repair_order_details_metrics
   ON default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_country =
   default_DOT_repair_order_details_metrics.default_DOT_hard_hat_DOT_country
   AND default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_postal_code =
   default_DOT_repair_order_details_metrics.default_DOT_hard_hat_DOT_postal_code
   AND default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_city =
   default_DOT_repair_order_details_metrics.default_DOT_hard_hat_DOT_city
   AND default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_state =
   default_DOT_repair_order_details_metrics.default_DOT_hard_hat_DOT_state
   AND default_DOT_repair_orders_fact_metrics.default_DOT_dispatcher_DOT_company_name =
   default_DOT_repair_order_details_metrics.default_DOT_dispatcher_DOT_company_name
   AND default_DOT_repair_orders_fact_metrics.default_DOT_municipality_dim_DOT_local_region =
   default_DOT_repair_order_details_metrics.default_DOT_municipality_dim_DOT_local_region
   AND default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_to_delete_DOT_hire_date =
   default_DOT_repair_order_details_metrics.default_DOT_hard_hat_to_delete_DOT_hire_date"""
    assert str(parse(metrics_sql_results["sql"])) == str(parse(expected_query))
    assert str(parse(cube_sql_results["sql"])) == str(parse(expected_query))


@pytest.mark.asyncio
async def test_cube_materialization_sql_and_measures(
    client_with_repairs_cube: AsyncClient,
    repair_orders_cube_measures,
    repairs_cube_elements,
):
    """
    Verifies a cube's materialization SQL + measures
    """
    await make_a_test_cube(
        client_with_repairs_cube,
        "default.repairs_cube_3",
        metrics_or_measures="measures",
    )

    response = await client_with_repairs_cube.get("/cubes/default.repairs_cube_3/")
    data = response.json()
    assert data["cube_elements"] == repairs_cube_elements
    expected_materialization_query = """
    WITH
    default_DOT_repair_order_details AS (
    SELECT  default_DOT_repair_order_details.repair_order_id,
        default_DOT_repair_order_details.repair_type_id,
        default_DOT_repair_order_details.price,
        default_DOT_repair_order_details.quantity,
        default_DOT_repair_order_details.discount
    FROM roads.repair_order_details AS default_DOT_repair_order_details
    ),
    default_DOT_repair_order AS (
    SELECT  default_DOT_repair_orders.repair_order_id,
        default_DOT_repair_orders.municipality_id,
        default_DOT_repair_orders.hard_hat_id,
        default_DOT_repair_orders.order_date,
        default_DOT_repair_orders.required_date,
        default_DOT_repair_orders.dispatched_date,
        default_DOT_repair_orders.dispatcher_id
    FROM roads.repair_orders AS default_DOT_repair_orders
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
    ),
    default_DOT_dispatcher AS (
    SELECT  default_DOT_dispatchers.dispatcher_id,
        default_DOT_dispatchers.company_name,
        default_DOT_dispatchers.phone
    FROM roads.dispatchers AS default_DOT_dispatchers
    ),
    default_DOT_municipality_dim AS (
    SELECT  m.municipality_id AS municipality_id,
        m.contact_name,
        m.contact_title,
        m.local_region,
        m.state_id,
        mmt.municipality_type_id AS municipality_type_id,
        mt.municipality_type_desc AS municipality_type_desc
    FROM roads.municipality AS m LEFT JOIN roads.municipality_municipality_type AS mmt ON m.municipality_id = mmt.municipality_id
    LEFT JOIN roads.municipality_type AS mt ON mmt.municipality_type_id = mt.municipality_type_desc
    )
    SELECT  default_DOT_repair_order_details.price default_DOT_repair_order_details_DOT_price,
        default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
        default_DOT_hard_hat.postal_code default_DOT_hard_hat_DOT_postal_code,
        default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
        default_DOT_hard_hat.hire_date default_DOT_hard_hat_DOT_hire_date,
        default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
        default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
        default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region
    FROM default_DOT_repair_order_details INNER JOIN default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
    INNER JOIN default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
    INNER JOIN default_DOT_dispatcher ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
    INNER JOIN default_DOT_municipality_dim ON default_DOT_repair_order.municipality_id = default_DOT_municipality_dim.municipality_id
    """
    assert str(parse(data["materializations"][0]["config"]["query"])) == str(
        parse(expected_materialization_query),
    )
    assert data["materializations"][0]["job"] == "DruidMeasuresCubeMaterializationJob"
    assert (
        data["materializations"][0]["config"]["measures"] == repair_orders_cube_measures
    )


@pytest.mark.asyncio
async def test_druid_cube_agg_materialization(
    client_with_repairs_cube: AsyncClient,
    module__query_service_client: Iterator[QueryServiceClient],
):
    """
    Verifies scheduling a materialized aggregate cube
    """
    await make_a_test_cube(
        client_with_repairs_cube,
        "default.repairs_cube_4",
    )
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube_4/materialization/",
        json={
            "job": "druid_metrics_cube",
            "strategy": "incremental_time",
            "config": {
                "spark": {},
            },
            "schedule": "@daily",
        },
    )
    assert response.json() == {
        "message": "The same materialization config with name "
        "`druid_metrics_cube__incremental_time__default.hard_hat.hire_date` "
        "already exists for node `default.repairs_cube_4` so no update was performed.",
        "info": {
            "output_tables": ["common.a", "common.b"],
            "urls": ["http://fake.url/job"],
        },
    }
    called_kwargs_all = [
        call_[0][0]
        for call_ in module__query_service_client.materialize.call_args_list  # type: ignore
    ]
    called_kwargs_for_cube_4 = [
        call_
        for call_ in called_kwargs_all
        if call_.node_name == "default.repairs_cube_4"
    ]
    called_kwargs = called_kwargs_for_cube_4[0]
    assert (
        called_kwargs.name
        == "druid_metrics_cube__incremental_time__default.hard_hat.hire_date"
    )
    assert called_kwargs.node_name == "default.repairs_cube_4"
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
                name="default_DOT_avg_repair_price",
                type="double",
                column="default_DOT_avg_repair_price",
                node="default.avg_repair_price",
                semantic_entity="default.avg_repair_price.default_DOT_avg_repair_price",
                semantic_type="metric",
            ),
            ColumnMetadata(
                name="default_DOT_discounted_orders_rate",
                type="double",
                column="default_DOT_discounted_orders_rate",
                node="default.discounted_orders_rate",
                semantic_entity="default.discounted_orders_rate.default_DOT_discounted_orders_rate",
                semantic_type="metric",
            ),
            ColumnMetadata(
                name="default_DOT_dispatcher_DOT_company_name",
                type="string",
                column="company_name",
                node="default.dispatcher",
                semantic_entity="default.dispatcher.company_name",
                semantic_type="dimension",
            ),
            ColumnMetadata(
                name="default_DOT_double_total_repair_cost",
                type="double",
                column="default_DOT_double_total_repair_cost",
                node="default.double_total_repair_cost",
                semantic_entity="default.double_total_repair_cost.default_DOT_double_total_repair_cost",
                semantic_type="metric",
            ),
            ColumnMetadata(
                name="default_DOT_hard_hat_DOT_city",
                type="string",
                column="city",
                node="default.hard_hat",
                semantic_entity="default.hard_hat.city",
                semantic_type="dimension",
            ),
            ColumnMetadata(
                name="default_DOT_hard_hat_DOT_country",
                type="string",
                column="country",
                node="default.hard_hat",
                semantic_entity="default.hard_hat.country",
                semantic_type="dimension",
            ),
            ColumnMetadata(
                name="default_DOT_hard_hat_DOT_hire_date",
                type="timestamp",
                column="hire_date",
                node="default.hard_hat",
                semantic_entity="default.hard_hat.hire_date",
                semantic_type="dimension",
            ),
            ColumnMetadata(
                name="default_DOT_hard_hat_DOT_postal_code",
                type="string",
                column="postal_code",
                node="default.hard_hat",
                semantic_entity="default.hard_hat.postal_code",
                semantic_type="dimension",
            ),
            ColumnMetadata(
                name="default_DOT_hard_hat_DOT_state",
                type="string",
                column="state",
                node="default.hard_hat",
                semantic_entity="default.hard_hat.state",
                semantic_type="dimension",
            ),
            ColumnMetadata(
                name="default_DOT_municipality_dim_DOT_local_region",
                type="string",
                column="local_region",
                node="default.municipality_dim",
                semantic_entity="default.municipality_dim.local_region",
                semantic_type="dimension",
            ),
            ColumnMetadata(
                name="default_DOT_num_repair_orders",
                type="bigint",
                column="default_DOT_num_repair_orders",
                node="default.num_repair_orders",
                semantic_entity="default.num_repair_orders.default_DOT_num_repair_orders",
                semantic_type="metric",
            ),
            ColumnMetadata(
                name="default_DOT_total_repair_cost",
                type="double",
                column="default_DOT_total_repair_cost",
                node="default.total_repair_cost",
                semantic_entity="default.total_repair_cost.default_DOT_total_repair_cost",
                semantic_type="metric",
            ),
            ColumnMetadata(
                name="default_DOT_total_repair_order_discounts",
                type="double",
                column="default_DOT_total_repair_order_discounts",
                node="default.total_repair_order_discounts",
                semantic_entity="default.total_repair_order_discounts.default_DOT_total_repair_order_discounts",
                semantic_type="metric",
            ),
        ],
        key=lambda x: x.name,
    )
    assert called_kwargs.druid_spec == {
        "dataSchema": {
            "dataSource": "default_DOT_repairs_cube_4",
            "parser": {
                "parseSpec": {
                    "format": "parquet",
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
                    "timestampSpec": {
                        "column": "default_DOT_hard_hat_DOT_hire_date",
                        "format": "yyyyMMdd",
                    },
                },
            },
            "metricsSpec": [
                {
                    "fieldName": "default_DOT_avg_repair_price",
                    "name": "default_DOT_avg_repair_price",
                    "type": "doubleSum",
                },
                {
                    "fieldName": "default_DOT_discounted_orders_rate",
                    "name": "default_DOT_discounted_orders_rate",
                    "type": "doubleSum",
                },
                {
                    "fieldName": "default_DOT_double_total_repair_cost",
                    "name": "default_DOT_double_total_repair_cost",
                    "type": "doubleSum",
                },
                {
                    "fieldName": "default_DOT_num_repair_orders",
                    "name": "default_DOT_num_repair_orders",
                    "type": "longSum",
                },
                {
                    "fieldName": "default_DOT_total_repair_cost",
                    "name": "default_DOT_total_repair_cost",
                    "type": "doubleSum",
                },
                {
                    "fieldName": "default_DOT_total_repair_order_discounts",
                    "name": "default_DOT_total_repair_order_discounts",
                    "type": "doubleSum",
                },
            ],
            "granularitySpec": {
                "type": "uniform",
                "segmentGranularity": "DAY",
                "intervals": [],
            },
        },
        "tuningConfig": {
            "partitionsSpec": {"targetPartitionSize": 5000000, "type": "hashed"},
            "useCombiner": True,
            "type": "hadoop",
        },
    }
    response = await client_with_repairs_cube.get("/nodes/default.repairs_cube_4/")
    materializations = response.json()["materializations"]
    assert len(materializations) == 1
    druid_materialization = [
        materialization
        for materialization in materializations
        if materialization["job"] == "DruidMetricsCubeMaterializationJob"
    ][0]
    assert set(druid_materialization["config"]["dimensions"]) == {
        "default_DOT_hard_hat_DOT_country",
        "default_DOT_hard_hat_DOT_postal_code",
        "default_DOT_hard_hat_DOT_city",
        "default_DOT_hard_hat_DOT_hire_date",
        "default_DOT_hard_hat_DOT_state",
        "default_DOT_dispatcher_DOT_company_name",
        "default_DOT_municipality_dim_DOT_local_region",
    }
    assert druid_materialization["config"]["metrics"] == [
        {
            "name": "default_DOT_discounted_orders_rate",
            "type": "double",
            "column": "default_DOT_discounted_orders_rate",
            "node": "default.discounted_orders_rate",
            "semantic_entity": "default.discounted_orders_rate.default_DOT_discounted_orders_rate",
            "semantic_type": "metric",
        },
        {
            "name": "default_DOT_num_repair_orders",
            "type": "bigint",
            "column": "default_DOT_num_repair_orders",
            "node": "default.num_repair_orders",
            "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
            "semantic_type": "metric",
        },
        {
            "name": "default_DOT_avg_repair_price",
            "type": "double",
            "column": "default_DOT_avg_repair_price",
            "node": "default.avg_repair_price",
            "semantic_entity": "default.avg_repair_price.default_DOT_avg_repair_price",
            "semantic_type": "metric",
        },
        {
            "name": "default_DOT_total_repair_cost",
            "type": "double",
            "column": "default_DOT_total_repair_cost",
            "node": "default.total_repair_cost",
            "semantic_entity": "default.total_repair_cost.default_DOT_total_repair_cost",
            "semantic_type": "metric",
        },
        {
            "name": "default_DOT_total_repair_order_discounts",
            "type": "double",
            "column": "default_DOT_total_repair_order_discounts",
            "node": "default.total_repair_order_discounts",
            "semantic_entity": "default.total_repair_order_discounts."
            "default_DOT_total_repair_order_discounts",
            "semantic_type": "metric",
        },
        {
            "name": "default_DOT_double_total_repair_cost",
            "type": "double",
            "column": "default_DOT_double_total_repair_cost",
            "node": "default.double_total_repair_cost",
            "semantic_entity": "default.double_total_repair_cost."
            "default_DOT_double_total_repair_cost",
            "semantic_type": "metric",
        },
    ]
    assert druid_materialization["schedule"] == "@daily"


@pytest.mark.asyncio
async def test_materialized_cube_sql(
    client_with_repairs_cube: AsyncClient,
):
    """
    Test generating SQL for a materialized cube with two cases:
    (1) the materialized table's catalog is compatible with the desired engine.
    (2) the materialized table's catalog is not compatible with the desired engine.
    """
    await make_a_test_cube(
        client_with_repairs_cube,
        "default.mini_repairs_cube",
        with_materialization=True,
    )
    await client_with_repairs_cube.post(
        "/data/default.mini_repairs_cube/availability/",
        json={
            "catalog": "draft",
            "schema_": "roads",
            "table": "mini_repairs_cube",
            "valid_through_ts": 1010129120,
        },
    )
    # Ask for SQL with metrics, dimensions, filters, order by, and limit
    response = await client_with_repairs_cube.get(
        "/sql/",
        params={
            "metrics": ["default.avg_repair_price", "default.num_repair_orders"],
            "dimensions": ["default.hard_hat.state", "default.dispatcher.company_name"],
            "limit": 100,
        },
    )
    results = response.json()
    assert "default_DOT_hard_hat AS (" in results["sql"]

    response = await client_with_repairs_cube.post(
        "/data/default.mini_repairs_cube/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": "mini_repairs_cube",
            "valid_through_ts": 1010129120,
        },
    )
    assert response.status_code == 200

    response = await client_with_repairs_cube.get(
        "/sql/",
        params={
            "metrics": ["default.avg_repair_price", "default.num_repair_orders"],
            "dimensions": ["default.hard_hat.state", "default.dispatcher.company_name"],
            "limit": 100,
        },
    )
    response = await client_with_repairs_cube.get(
        "/sql/",
        params={
            "metrics": ["default.avg_repair_price", "default.num_repair_orders"],
            "dimensions": ["default.hard_hat.state", "default.dispatcher.company_name"],
            "limit": 100,
        },
    )
    results = response.json()
    expected_sql = """
    SELECT
      SUM(default_DOT_avg_repair_price),
      SUM(default_DOT_num_repair_orders),
      default_DOT_hard_hat_DOT_state,
      default_DOT_dispatcher_DOT_company_name
    FROM mini_repairs_cube
    GROUP BY  default_DOT_hard_hat_DOT_state, default_DOT_dispatcher_DOT_company_name
    LIMIT 100"""
    assert str(parse(results["sql"])) == str(parse(expected_sql))


@pytest.mark.asyncio
async def test_remove_dimension_link_invalidate_cube(
    client_with_repairs_cube: AsyncClient,
):
    """
    Verify that removing a dimension link can invalidate a cube.
    """
    # Delete an irrelevant dimension link
    response = await client_with_repairs_cube.request(
        "DELETE",
        "/nodes/default.repair_order/link/",
        json={
            "dimension_node": "default.hard_hat_to_delete",
            "role": None,
        },
    )
    assert response.json() == {
        "message": "Dimension link default.hard_hat_to_delete to node default.repair_order has "
        "been removed.",
    }
    # The cube remains valid
    response = await client_with_repairs_cube.get("/nodes/default.repairs_cube")
    data = response.json()
    assert data["status"] == "valid"

    # Add an unaffected cube
    await client_with_repairs_cube.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.num_repair_orders"],
            "dimensions": [
                "default.dispatcher.company_name",
                "default.municipality_dim.local_region",
            ],
            "filters": ["default.dispatcher.company_name='Pothole Pete'"],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.repairs_cube_unaffected",
        },
    )

    # Delete the link between default.repair_orders_fact and default.hard_hat_to_delete
    response = await client_with_repairs_cube.request(
        "DELETE",
        "/nodes/default.repair_orders_fact/link",
        json={
            "dimension_node": "default.hard_hat_to_delete",
        },
    )
    assert response.status_code == 201

    # The cube that has default.hard_hat_to_delete dimensions should be invalid
    response = await client_with_repairs_cube.get("/nodes/default.repairs_cube")
    assert response.json()["status"] == "invalid"

    # The cube without default.hard_hat_to_delete dimensions should remain valid
    response = await client_with_repairs_cube.get(
        "/nodes/default.repairs_cube_unaffected",
    )
    assert response.json()["status"] == "valid"


@pytest.mark.asyncio
async def test_changing_node_upstream_from_cube(
    client_with_repairs_cube: AsyncClient,
):
    """
    Verify changing nodes upstream from a cube
    """
    # Prepare the objects
    await client_with_repairs_cube.post(
        "/nodes/source/",
        json={
            "columns": [
                {"name": "repair_order_id", "type": "int"},
                {"name": "municipality_id", "type": "string"},
                {"name": "hard_hat_id", "type": "int"},
                {"name": "order_date", "type": "timestamp"},
                {"name": "required_date", "type": "timestamp"},
                {"name": "dispatched_date", "type": "timestamp"},
                {"name": "dispatcher_id", "type": "int"},
            ],
            "description": "All repair orders",
            "mode": "published",
            "name": "default.repair_orders__one",
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_orders__one",
        },
    )
    await client_with_repairs_cube.post(
        "/nodes/dimension/",
        json={
            "description": "Repair order dimension",
            "query": """
                        SELECT
                        repair_order_id,
                        municipality_id,
                        hard_hat_id,
                        order_date,
                        required_date,
                        dispatched_date,
                        dispatcher_id
                        FROM default.repair_orders__one
                    """,
            "mode": "published",
            "name": "default.repair_order_dim__one",
            "primary_key": ["repair_order_id"],
        },
    )
    await client_with_repairs_cube.post(
        "/nodes/metric/",
        json={
            "description": "Repair order date count",
            "query": """
                        SELECT
                        COUNT(DISTINCT order_date) AS order_date_count
                        FROM default.repair_order_dim__one
                    """,
            "mode": "published",
            "name": "default.order_date_count__one",
        },
    )
    response = await client_with_repairs_cube.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.order_date_count__one"],
            "dimensions": [
                "default.repair_order_dim__one.hard_hat_id",
                "default.repair_order_dim__one.municipality_id",
            ],
            "filters": ["default.repair_order_dim__one.hard_hat_id > 10"],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.repairs_cube_1",
        },
    )
    assert response.status_code == 201
    assert response.json()["version"] == "v1.0"

    # Verify effects on cube after deactivating a node upstream from the cube
    await client_with_repairs_cube.request(
        "DELETE",
        "/nodes/default.repair_orders__one/",
    )
    response = await client_with_repairs_cube.get("/nodes/default.repairs_cube_1/")
    data = response.json()
    assert data["status"] == "invalid"

    # Verify effects on cube after restoring a node upstream from the cube
    await client_with_repairs_cube.post("/nodes/default.repair_orders__one/restore/")
    response = await client_with_repairs_cube.get("/nodes/default.repairs_cube_1/")
    data = response.json()
    assert data["status"] == "valid"

    response = await client_with_repairs_cube.get("/cubes/default.repairs_cube_1/")
    data = response.json()
    expected_elements = [
        {
            "name": "default_DOT_order_date_count__one",
            "display_name": "Order Date Count  One",
            "node_name": "default.order_date_count__one",
            "type": "metric",
            "partition": None,
        },
        {
            "name": "municipality_id",
            "display_name": "Municipality Id",
            "node_name": "default.repair_order_dim__one",
            "type": "dimension",
            "partition": None,
        },
        {
            "name": "hard_hat_id",
            "display_name": "Hard Hat Id",
            "node_name": "default.repair_order_dim__one",
            "type": "dimension",
            "partition": None,
        },
    ]
    assert data["cube_elements"] == expected_elements

    # Verify effects on cube after updating a node upstream from the cube
    await client_with_repairs_cube.patch(
        "/nodes/default.order_date_count__one/",
        json={
            "query": """SELECT COUNT(DISTINCT order_date) + 42 AS order_date_count "
            "FROM default.repair_order_dim__one""",
        },
    )
    response = await client_with_repairs_cube.get("/nodes/default.repairs_cube_1/")
    data = response.json()
    assert data["status"] == "valid"

    response = await client_with_repairs_cube.get("/cubes/default.repairs_cube_1/")
    data = response.json()
    assert data["cube_elements"] == expected_elements


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
            "display_name": "Discounted Orders Rate",
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


@pytest.mark.asyncio
async def test_updating_cube(
    client_with_repairs_cube: AsyncClient,
):
    """
    Verify updating a cube
    """
    await make_a_test_cube(
        client_with_repairs_cube,
        "default.repairs_cube_6",
    )
    # Check a minor update to the cube
    response = await client_with_repairs_cube.patch(
        "/nodes/default.repairs_cube_6",
        json={
            "description": "This cube has a new description",
        },
    )
    data = response.json()
    assert data["version"] == "v1.1"
    assert data["description"] == "This cube has a new description"

    # Check a major update to the cube
    response = await client_with_repairs_cube.patch(
        "/nodes/default.repairs_cube_6",
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
                "name": "default.discounted_orders_rate",
                "display_name": "Discounted Orders Rate",
                "type": "double",
                "attributes": [],
                "description": None,
                "dimension": None,
                "partition": None,
            },
            {
                "name": "default.hard_hat.city",
                "display_name": "City",
                "type": "string",
                "attributes": [],
                "description": None,
                "dimension": None,
                "partition": None,
            },
            {
                "name": "default.hard_hat.hire_date",
                "display_name": "Hire Date",
                "type": "timestamp",
                "attributes": [],
                "description": None,
                "dimension": None,
                "partition": {
                    "type_": "temporal",
                    "format": "yyyyMMdd",
                    "granularity": "day",
                    "expression": None,
                },
            },
        ],
        key=lambda x: x["name"],  # type: ignore
    )

    response = await client_with_repairs_cube.get("/cubes/default.repairs_cube_6/")
    data = response.json()
    assert_updated_repairs_cube(data)

    response = await client_with_repairs_cube.get(
        "/history?node=default.repairs_cube_6",
    )
    assert [
        event for event in response.json() if event["activity_type"] == "update"
    ] == [
        {
            "activity_type": "update",
            "created_at": mock.ANY,
            "details": {"version": "v2.0"},
            "entity_name": "default.repairs_cube_6",
            "entity_type": "node",
            "id": mock.ANY,
            "node": "default.repairs_cube_6",
            "post": {
                "dimensions": [
                    "default.hard_hat.city",
                    "default.hard_hat.hire_date",
                ],
                "metrics": [
                    "default.discounted_orders_rate",
                ],
            },
            "pre": {
                "dimensions": [
                    "default.hard_hat.country",
                    "default.hard_hat.postal_code",
                    "default.hard_hat.city",
                    "default.hard_hat.hire_date",
                    "default.hard_hat.state",
                    "default.dispatcher.company_name",
                    "default.municipality_dim.local_region",
                ],
                "metrics": [
                    "default.discounted_orders_rate",
                    "default.num_repair_orders",
                    "default.avg_repair_price",
                    "default.total_repair_cost",
                    "default.total_repair_order_discounts",
                    "default.double_total_repair_cost",
                ],
            },
            "user": mock.ANY,
        },
        {
            "activity_type": "update",
            "created_at": mock.ANY,
            "details": {"version": "v1.1"},
            "entity_name": "default.repairs_cube_6",
            "entity_type": "node",
            "id": mock.ANY,
            "node": "default.repairs_cube_6",
            "post": {
                "dimensions": [
                    "default.hard_hat.country",
                    "default.hard_hat.postal_code",
                    "default.hard_hat.city",
                    "default.hard_hat.hire_date",
                    "default.hard_hat.state",
                    "default.dispatcher.company_name",
                    "default.municipality_dim.local_region",
                ],
                "metrics": [
                    "default.discounted_orders_rate",
                    "default.num_repair_orders",
                    "default.avg_repair_price",
                    "default.total_repair_cost",
                    "default.total_repair_order_discounts",
                    "default.double_total_repair_cost",
                ],
            },
            "pre": {
                "dimensions": [
                    "default.hard_hat.country",
                    "default.hard_hat.postal_code",
                    "default.hard_hat.city",
                    "default.hard_hat.hire_date",
                    "default.hard_hat.state",
                    "default.dispatcher.company_name",
                    "default.municipality_dim.local_region",
                ],
                "metrics": [
                    "default.discounted_orders_rate",
                    "default.num_repair_orders",
                    "default.avg_repair_price",
                    "default.total_repair_cost",
                    "default.total_repair_order_discounts",
                    "default.double_total_repair_cost",
                ],
            },
            "user": mock.ANY,
        },
    ]


@pytest.mark.asyncio
async def test_updating_cube_with_existing_cube_materialization(
    client_with_repairs_cube: AsyncClient,
    module__query_service_client: QueryServiceClient,
):
    """
    Verify updating a cube with an existing new-style cube materialization
    """
    cube_name = "default.repairs_cube__default_incremental_11"
    await make_a_test_cube(
        client_with_repairs_cube,
        cube_name,
    )
    response = await client_with_repairs_cube.post(
        f"/nodes/{cube_name}/columns/default.hard_hat.hire_date/partition",
        json={
            "type_": "temporal",
            "granularity": "day",
            "format": "yyyyMMdd",
        },
    )
    assert response.status_code in (200, 201)
    response = await client_with_repairs_cube.post(
        f"/nodes/{cube_name}/materialization/",
        json={
            "job": "druid_cube",
            "strategy": "incremental_time",
            "schedule": "@daily",
            "lookback_window": "1 DAY",
        },
    )
    # Update the cube, but keep the temporal partition column. This should succeed
    response = await client_with_repairs_cube.patch(
        f"/nodes/{cube_name}",
        json={
            "metrics": ["default.discounted_orders_rate"],
            "dimensions": ["default.hard_hat.city", "default.hard_hat.hire_date"],
        },
    )
    result = response.json()
    assert result["version"] == "v2.0"

    # Check that the configured materialization was updated
    response = await client_with_repairs_cube.get(f"/cubes/{cube_name}/")
    data = response.json()
    assert [
        col["semantic_entity"]
        for col in data["materializations"][0]["config"]["columns"]
    ] == [
        "default.hard_hat.city",
        "default.hard_hat.hire_date",
        "default.discounted_orders_rate.default_DOT_discounted_orders_rate",
    ]
    assert data["materializations"][0]["job"] == "DruidMetricsCubeMaterializationJob"
    assert (
        data["materializations"][0]["name"]
        == "druid_metrics_cube__incremental_time__default.hard_hat.hire_date"
    )
    assert data["materializations"][0]["strategy"] == "incremental_time"
    assert data["materializations"][0]["schedule"] == "@daily"


@pytest.mark.asyncio
async def test_updating_cube_with_existing_materialization(
    client_with_repairs_cube: AsyncClient,
    module__query_service_client: QueryServiceClient,
):
    """
    Verify updating a cube with existing materialization
    """
    # Make an isolated cube and add materialization to the cube
    await make_a_test_cube(
        client_with_repairs_cube,
        "default.repairs_cube_2",
        metrics_or_measures="measures",
    )
    # Make sure that the cube already has an additional materialization configured
    response = await client_with_repairs_cube.get("/cubes/default.repairs_cube_2/")
    data = response.json()
    assert len(data["materializations"]) == 1

    # Update the existing materialization config
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube_2/materialization",
        json={
            "job": "druid_measures_cube",
            "strategy": "incremental_time",
            "config": {"spark": {"spark.executor.memory": "6g"}},
            "schedule": "@daily",
        },
    )
    data = response.json()
    assert data == {
        "message": "Successfully updated materialization config named "
        "`druid_measures_cube__incremental_time__default.hard_hat.hire_date` for node "
        "`default.repairs_cube_2`",
        "urls": [["http://fake.url/job"]],
    }

    # Check that the configured materialization was updated
    response = await client_with_repairs_cube.get("/cubes/default.repairs_cube_2/")
    data = response.json()
    assert data["materializations"][0]["config"]["spark"] == {
        "spark.executor.memory": "6g",
    }

    # Update the cube, but keep the temporal partition column. This should succeed
    response = await client_with_repairs_cube.patch(
        "/nodes/default.repairs_cube_2",
        json={
            "metrics": ["default.discounted_orders_rate"],
            "dimensions": ["default.hard_hat.city", "default.hard_hat.hire_date"],
        },
    )
    result = response.json()
    assert result["version"] == "v2.0"

    # Check that the query service was called to materialize
    assert len(module__query_service_client.materialize.call_args_list) >= 1  # type: ignore
    last_call_args = (
        module__query_service_client.materialize.call_args_list[-1].args[0].dict()  # type: ignore
    )
    assert (
        last_call_args["name"]
        == "druid_measures_cube__incremental_time__default.hard_hat.hire_date"
    )
    assert last_call_args["node_name"] == "default.repairs_cube_2"
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
    response = await client_with_repairs_cube.get("/cubes/default.repairs_cube_2/")
    data = response.json()
    assert_updated_repairs_cube(data)

    # Check that the existing materialization was updated
    assert data["materializations"][0]["backfills"] == []
    assert sorted(
        data["materializations"][0]["config"]["columns"],
        key=lambda x: x["name"],
    ) == sorted(
        [
            {
                "column": "city",
                "name": "default_DOT_hard_hat_DOT_city",
                "node": "default.hard_hat",
                "semantic_entity": "default.hard_hat.city",
                "semantic_type": "dimension",
                "type": "string",
            },
            {
                "column": "hire_date",
                "name": "default_DOT_hard_hat_DOT_hire_date",
                "node": "default.hard_hat",
                "semantic_entity": "default.hard_hat.hire_date",
                "semantic_type": "dimension",
                "type": "timestamp",
            },
            {
                "column": "discount",
                "name": "default_DOT_repair_orders_fact_DOT_discount",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.discount",
                "semantic_type": "measure",
                "type": "float",
            },
        ],
        key=lambda x: x["name"],
    )
    assert data["materializations"][0]["config"]["dimensions"] == [
        "default_DOT_hard_hat_DOT_city",
        "default_DOT_hard_hat_DOT_hire_date",
    ]
    assert data["materializations"][0]["config"]["druid"] is None
    assert data["materializations"][0]["config"]["measures"] == {
        "default.discounted_orders_rate": {
            "combiner": "CAST(sum(if(default_DOT_repair_orders_fact_DOT_discount "
            "> 0.0, 1, 0)) AS DOUBLE) / "
            "count(*) AS ",
            "measures": [
                {
                    "agg": "sum",
                    "field_name": "default_DOT_repair_orders_fact_DOT_discount",
                    "name": "default.repair_orders_fact.discount",
                    "type": "float",
                },
            ],
            "metric": "default.discounted_orders_rate",
        },
    }
    assert data["materializations"][0]["config"]["prefix"] == ""
    assert data["materializations"][0]["config"]["suffix"] == ""
    assert data["materializations"][0]["config"]["spark"] == {
        "spark.executor.memory": "6g",
    }
    assert set(data["materializations"][0]["config"]["upstream_tables"]) == {
        "default.roads.repair_order_details",
        "default.roads.repair_orders",
        "default.roads.hard_hats",
    }
    assert data["materializations"][0]["strategy"] == "incremental_time"
    assert data["materializations"][0]["job"] == "DruidMeasuresCubeMaterializationJob"
    assert (
        data["materializations"][0]["name"]
        == "druid_measures_cube__incremental_time__default.hard_hat.hire_date"
    )
    assert data["materializations"][0]["schedule"] == "@daily"

    response = await client_with_repairs_cube.get(
        "/history?node=default.repairs_cube_2",
    )
    assert [
        event for event in response.json() if event["activity_type"] == "update"
    ] == [
        {
            "activity_type": "update",
            "created_at": mock.ANY,
            "details": {},
            "entity_name": "druid_measures_cube__incremental_time__default.hard_hat.hire_date",
            "entity_type": "materialization",
            "id": mock.ANY,
            "node": "default.repairs_cube_2",
            "post": {},
            "pre": {},
            "user": "dj",
        },
        {
            "activity_type": "update",
            "created_at": mock.ANY,
            "details": {"version": "v2.0"},
            "entity_name": "default.repairs_cube_2",
            "entity_type": "node",
            "id": mock.ANY,
            "node": "default.repairs_cube_2",
            "post": {
                "dimensions": [
                    "default.hard_hat.city",
                    "default.hard_hat.hire_date",
                ],
                "metrics": [
                    "default.discounted_orders_rate",
                ],
            },
            "pre": {
                "dimensions": [
                    "default.hard_hat.country",
                    "default.hard_hat.postal_code",
                    "default.hard_hat.city",
                    "default.hard_hat.hire_date",
                    "default.hard_hat.state",
                    "default.dispatcher.company_name",
                    "default.municipality_dim.local_region",
                ],
                "metrics": [
                    "default.discounted_orders_rate",
                    "default.num_repair_orders",
                    "default.avg_repair_price",
                    "default.total_repair_cost",
                    "default.total_repair_order_discounts",
                    "default.double_total_repair_cost",
                ],
            },
            "user": "dj",
        },
        {
            "activity_type": "update",
            "created_at": mock.ANY,
            "details": {
                "materialization": "druid_measures_cube__incremental_time__"
                "default.hard_hat.hire_date",
                "node": "default.repairs_cube_2",
            },
            "entity_name": "druid_measures_cube__incremental_time__default.hard_hat.hire_date",
            "entity_type": "materialization",
            "id": mock.ANY,
            "node": "default.repairs_cube_2",
            "post": {},
            "pre": {},
            "user": "dj",
        },
    ]

    # Update the cube, but remove the temporal partition column. This should fail when
    # trying to update the cube's materialization config
    response = await client_with_repairs_cube.patch(
        "/nodes/default.repairs_cube_2",
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


@pytest.mark.asyncio
async def test_get_materialized_cube_dimension_sql(
    client_with_repairs_cube: AsyncClient,
):
    """
    Test building SQL to get unique dimension values for a materialized cube
    """
    await make_a_test_cube(
        client_with_repairs_cube,
        "default.repairs_cube_7",
    )
    await client_with_repairs_cube.post(
        "/data/default.repairs_cube_7/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": "repairs_cube",
            "valid_through_ts": 1010129120,
        },
    )

    # Asking for an unavailable dimension should fail
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_7/dimensions/sql",
        params={
            "dimensions": ["default.hard_hat.city", "default.contractor.company_name"],
        },
    )
    assert response.json()["message"] == (
        "The following dimensions {'default.contractor.company_name'} are "
        "not available in the cube default.repairs_cube_7."
    )

    # Ask for single dimension
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_7/dimensions/sql",
        params={
            "dimensions": ["default.hard_hat.city"],
        },
    )
    results = response.json()
    assert results["columns"] == [
        {
            "column": None,
            "name": "default_DOT_hard_hat_DOT_city",
            "node": None,
            "semantic_entity": "default.hard_hat.city",
            "semantic_type": "dimension",
            "type": "string",
        },
    ]
    assert compare_query_strings(
        results["sql"],
        "SELECT default_DOT_hard_hat_DOT_city FROM repairs_cube "
        "GROUP BY default_DOT_hard_hat_DOT_city",
    )

    # Ask for single dimension with counts
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_7/dimensions/sql",
        params={
            "dimensions": ["default.hard_hat.city"],
            "include_counts": True,
        },
    )
    results = response.json()
    assert results["columns"] == [
        {
            "column": None,
            "name": "default_DOT_hard_hat_DOT_city",
            "node": None,
            "semantic_entity": "default.hard_hat.city",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": None,
            "name": "count",
            "node": None,
            "semantic_entity": None,
            "semantic_type": None,
            "type": "int",
        },
    ]
    assert compare_query_strings(
        results["sql"],
        "SELECT default_DOT_hard_hat_DOT_city, COUNT(*) FROM repairs_cube "
        "GROUP BY  default_DOT_hard_hat_DOT_city "
        "ORDER BY 2 DESC",
    )

    # Ask for multiple dimensions with counts
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_7/dimensions/sql",
        params={
            "dimensions": ["default.hard_hat.city", "default.dispatcher.company_name"],
            "include_counts": True,
        },
    )
    results = response.json()
    assert results["columns"] == [
        {
            "column": None,
            "name": "default_DOT_hard_hat_DOT_city",
            "node": None,
            "semantic_entity": "default.hard_hat.city",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": None,
            "name": "default_DOT_dispatcher_DOT_company_name",
            "node": None,
            "semantic_entity": "default.dispatcher.company_name",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": None,
            "name": "count",
            "node": None,
            "semantic_entity": None,
            "semantic_type": None,
            "type": "int",
        },
    ]
    assert compare_query_strings(
        results["sql"],
        "SELECT default_DOT_hard_hat_DOT_city, default_DOT_dispatcher_DOT_company_name, COUNT(*) "
        "FROM repairs_cube "
        "GROUP BY default_DOT_hard_hat_DOT_city, default_DOT_dispatcher_DOT_company_name "
        "ORDER BY 3 DESC",
    )

    # Ask for multiple dimensions with filters and limit
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_7/dimensions/sql",
        params={
            "dimensions": ["default.hard_hat.city", "default.dispatcher.company_name"],
            "filters": "default.dispatcher.company_name = 'Pothole Pete'",
            "limit": 4,
            "include_counts": True,
        },
    )
    results = response.json()
    assert compare_query_strings(
        results["sql"],
        "SELECT default_DOT_hard_hat_DOT_city, default_DOT_dispatcher_DOT_company_name, COUNT(*) "
        "FROM repairs_cube "
        "WHERE default_DOT_dispatcher_DOT_company_name = 'Pothole Pete' "
        "GROUP BY default_DOT_hard_hat_DOT_city, default_DOT_dispatcher_DOT_company_name "
        "ORDER BY 3 DESC LIMIT 4",
    )


@pytest.mark.asyncio
async def test_get_unmaterialized_cube_dimensions_values(
    client_with_repairs_cube: AsyncClient,
):
    """
    Test building SQL + getting data for dimension values for an unmaterialized cube
    """
    await make_a_test_cube(
        client_with_repairs_cube,
        "default.repairs_cube_8",
        with_materialization=False,
    )
    # Get SQL for single dimension
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_8/dimensions/sql",
        params={
            "dimensions": ["default.hard_hat.city"],
        },
    )
    results = response.json()
    assert "SELECT  default_DOT_hard_hat_DOT_city" in results["sql"]
    assert "GROUP BY  default_DOT_hard_hat_DOT_city" in results["sql"]

    # Get data for single dimension
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_8/dimensions/data",
        params={
            "dimensions": ["default.hard_hat.city"],
        },
    )
    results = response.json()
    assert results == {
        "cardinality": 9,
        "dimensions": ["default.hard_hat.city"],
        "values": [
            {"count": None, "value": ["Jersey City"]},
            {"count": None, "value": ["Billerica"]},
            {"count": None, "value": ["Southgate"]},
            {"count": None, "value": ["Phoenix"]},
            {"count": None, "value": ["Southampton"]},
            {"count": None, "value": ["Powder Springs"]},
            {"count": None, "value": ["Middletown"]},
            {"count": None, "value": ["Muskogee"]},
            {"count": None, "value": ["Niagara Falls"]},
        ],
    }

    # Ask for single dimension with counts
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_8/dimensions/sql",
        params={
            "dimensions": ["default.hard_hat.city"],
            "include_counts": True,
        },
    )
    results = response.json()
    assert "SELECT  default_DOT_hard_hat_DOT_city,\n\tCOUNT(*)" in results["sql"]
    assert "GROUP BY  default_DOT_hard_hat_DOT_city" in results["sql"]
    assert "ORDER BY 2 DESC" in results["sql"]

    # Get data for single dimension with counts
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_8/dimensions/data",
        params={
            "dimensions": ["default.hard_hat.city"],
            "include_counts": True,
        },
    )
    assert response.json() == {
        "cardinality": 9,
        "dimensions": ["default.hard_hat.city"],
        "values": [
            {"count": 5, "value": ["Southgate"]},
            {"count": 4, "value": ["Jersey City"]},
            {"count": 4, "value": ["Southampton"]},
            {"count": 3, "value": ["Billerica"]},
            {"count": 3, "value": ["Powder Springs"]},
            {"count": 2, "value": ["Phoenix"]},
            {"count": 2, "value": ["Middletown"]},
            {"count": 1, "value": ["Muskogee"]},
            {"count": 1, "value": ["Niagara Falls"]},
        ],
    }

    # Get data for multiple dimensions with counts
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_8/dimensions/data",
        params={
            "dimensions": ["default.hard_hat.city", "default.dispatcher.company_name"],
            "include_counts": True,
        },
    )
    assert response.json() == {
        "cardinality": 17,
        "dimensions": ["default.hard_hat.city", "default.dispatcher.company_name"],
        "values": [
            {"count": 3, "value": ["Jersey City", "Pothole Pete"]},
            {"count": 2, "value": ["Southgate", "Asphalts R Us"]},
            {"count": 2, "value": ["Billerica", "Asphalts R Us"]},
            {"count": 2, "value": ["Southgate", "Federal Roads Group"]},
            {"count": 2, "value": ["Southampton", "Pothole Pete"]},
            {"count": 2, "value": ["Powder Springs", "Asphalts R Us"]},
            {"count": 2, "value": ["Middletown", "Federal Roads Group"]},
            {"count": 1, "value": ["Jersey City", "Federal Roads Group"]},
            {"count": 1, "value": ["Billerica", "Pothole Pete"]},
            {"count": 1, "value": ["Phoenix", "Asphalts R Us"]},
            {"count": 1, "value": ["Southampton", "Asphalts R Us"]},
            {"count": 1, "value": ["Southampton", "Federal Roads Group"]},
            {"count": 1, "value": ["Phoenix", "Federal Roads Group"]},
            {"count": 1, "value": ["Muskogee", "Federal Roads Group"]},
            {"count": 1, "value": ["Powder Springs", "Pothole Pete"]},
            {"count": 1, "value": ["Niagara Falls", "Federal Roads Group"]},
            {"count": 1, "value": ["Southgate", "Pothole Pete"]},
        ],
    }

    # Get data for multiple dimensions with filters
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_8/dimensions/data",
        params={
            "dimensions": ["default.hard_hat.city", "default.dispatcher.company_name"],
            "filters": "default.dispatcher.company_name = 'Pothole Pete'",
            "include_counts": True,
        },
    )
    assert response.json() == {
        "cardinality": 5,
        "dimensions": ["default.hard_hat.city", "default.dispatcher.company_name"],
        "values": [
            {"count": 3, "value": ["Jersey City", "Pothole Pete"]},
            {"count": 2, "value": ["Southampton", "Pothole Pete"]},
            {"count": 1, "value": ["Billerica", "Pothole Pete"]},
            {"count": 1, "value": ["Southgate", "Pothole Pete"]},
            {"count": 1, "value": ["Powder Springs", "Pothole Pete"]},
        ],
    }

    # Get data for multiple dimensions with filters and limit
    response = await client_with_repairs_cube.get(
        "/cubes/default.repairs_cube_8/dimensions/data",
        params={
            "dimensions": ["default.hard_hat.city", "default.dispatcher.company_name"],
            "filters": "default.dispatcher.company_name = 'Pothole Pete'",
            "limit": 4,
            "include_counts": True,
        },
    )
    assert response.json() == {
        "cardinality": 4,
        "dimensions": ["default.hard_hat.city", "default.dispatcher.company_name"],
        "values": [
            {"count": 3, "value": ["Jersey City", "Pothole Pete"]},
            {"count": 2, "value": ["Southampton", "Pothole Pete"]},
            {"count": 1, "value": ["Billerica", "Pothole Pete"]},
            {"count": 1, "value": ["Southgate", "Pothole Pete"]},
        ],
    }


@pytest.mark.asyncio
async def test_derive_sql_column():
    """
    Test that SQL column name are properly derived from cube elements
    """
    sql_column = await derive_sql_column(
        CubeElementMetadata(
            name="foo_DOT_bar_DOT_baz_DOT_revenue",
            display_name="Revenue",
            node_name="foo.bar.baz",
            type="metric",
        ),
    )
    expected_sql_column = ColumnOutput(
        name="foo_DOT_bar_DOT_baz_DOT_revenue",
        display_name="Revenue",
        type="metric",
    )
    assert sql_column.name == expected_sql_column.name
    assert sql_column.display_name == expected_sql_column.display_name
    assert sql_column.type == expected_sql_column.type
    sql_column = await derive_sql_column(
        CubeElementMetadata(
            name="owner",
            display_name="Owner",
            node_name="foo.bar.baz",
            type="dimension",
        ),
    )
    expected_sql_column = ColumnOutput(
        name="foo_DOT_bar_DOT_baz_DOT_owner",
        display_name="Owner",
        type="dimension",
    )
    assert sql_column.name == expected_sql_column.name
    assert sql_column.display_name == expected_sql_column.display_name
    assert sql_column.type == expected_sql_column.type


@pytest.mark.asyncio
async def test_cube_materialization_metadata(
    client_with_repairs_cube: AsyncClient,
):
    """
    Test building cube materialization metadata
    """
    await make_a_test_cube(
        client_with_repairs_cube,
        "default.example_repairs_cube",
        with_materialization=False,
    )
    response = await client_with_repairs_cube.get(
        "/cubes/default.example_repairs_cube/materialization",
    )
    results = response.json()
    assert results["message"] == (
        "The cube must have a single temporal partition column set"
        " in order for it to be materialized."
    )

    await client_with_repairs_cube.post(
        "/nodes/default.example_repairs_cube/columns/default.hard_hat.hire_date/partition",
        json={
            "type_": "temporal",
            "granularity": "day",
            "format": "yyyyMMdd",
        },
    )
    response = await client_with_repairs_cube.get(
        "/cubes/default.example_repairs_cube/materialization",
    )
    results = response.json()
    assert results["cube"] == {
        "name": "default.example_repairs_cube",
        "version": "v1.0",
        "display_name": "Example Repairs Cube",
    }
    assert results["job"] == "DRUID_CUBE"
    assert results["lookback_window"] == "1 DAY"
    assert results["schedule"] == "0 0 * * *"
    assert results["strategy"] == "incremental_time"
    assert results["dimensions"] == [
        "default.hard_hat.country",
        "default.hard_hat.postal_code",
        "default.hard_hat.city",
        "default.hard_hat.hire_date",
        "default.hard_hat.state",
        "default.dispatcher.company_name",
        "default.municipality_dim.local_region",
    ]
    assert results["metrics"] == [
        {
            "derived_expression": "SELECT  CAST(sum(discount_sum_62846f49) AS DOUBLE) / "
            "SUM(count_3389dae3) AS default_DOT_discounted_orders_rate  FROM "
            "default.repair_orders_fact",
            "metric_expression": "CAST(sum(discount_sum_62846f49) AS DOUBLE) / "
            "SUM(count_3389dae3)",
            "metric": {
                "name": "default.discounted_orders_rate",
                "version": "v1.0",
                "display_name": "Discounted Orders Rate",
            },
            "required_measures": [
                {
                    "measure_name": "discount_sum_62846f49",
                    "node": {
                        "name": "default.repair_orders_fact",
                        "version": "v1.0",
                        "display_name": "Repair Orders Fact",
                    },
                },
                {
                    "measure_name": "count_3389dae3",
                    "node": {
                        "name": "default.repair_orders_fact",
                        "version": "v1.0",
                        "display_name": "Repair Orders Fact",
                    },
                },
            ],
        },
        {
            "derived_expression": "SELECT  SUM(repair_order_id_count_0b7dfba0)  FROM "
            "default.repair_orders_fact",
            "metric_expression": "SUM(repair_order_id_count_0b7dfba0)",
            "metric": {
                "name": "default.num_repair_orders",
                "version": "v1.0",
                "display_name": "Num Repair Orders",
            },
            "required_measures": [
                {
                    "measure_name": "repair_order_id_count_0b7dfba0",
                    "node": {
                        "name": "default.repair_orders_fact",
                        "version": "v1.0",
                        "display_name": "Repair Orders Fact",
                    },
                },
            ],
        },
        {
            "derived_expression": "SELECT  SUM(price_sum_78a5eb43) / SUM(price_count_78a5eb43)  FROM "
            "default.repair_orders_fact",
            "metric_expression": "SUM(price_sum_78a5eb43) / SUM(price_count_78a5eb43)",
            "metric": {
                "name": "default.avg_repair_price",
                "version": "v1.0",
                "display_name": "Avg Repair Price",
            },
            "required_measures": [
                {
                    "measure_name": "price_count_78a5eb43",
                    "node": {
                        "name": "default.repair_orders_fact",
                        "version": "v1.0",
                        "display_name": "Repair Orders Fact",
                    },
                },
                {
                    "measure_name": "price_sum_78a5eb43",
                    "node": {
                        "name": "default.repair_orders_fact",
                        "version": "v1.0",
                        "display_name": "Repair Orders Fact",
                    },
                },
            ],
        },
        {
            "derived_expression": "SELECT  sum(total_repair_cost_sum_9bdaf803)  FROM "
            "default.repair_orders_fact",
            "metric_expression": "sum(total_repair_cost_sum_9bdaf803)",
            "metric": {
                "name": "default.total_repair_cost",
                "version": "v1.0",
                "display_name": "Total Repair Cost",
            },
            "required_measures": [
                {
                    "measure_name": "total_repair_cost_sum_9bdaf803",
                    "node": {
                        "name": "default.repair_orders_fact",
                        "version": "v1.0",
                        "display_name": "Repair Orders Fact",
                    },
                },
            ],
        },
        {
            "derived_expression": "SELECT  sum(price_discount_sum_017d55a8)  FROM "
            "default.repair_orders_fact",
            "metric_expression": "sum(price_discount_sum_017d55a8)",
            "metric": {
                "name": "default.total_repair_order_discounts",
                "version": "v1.0",
                "display_name": "Total Repair Order Discounts",
            },
            "required_measures": [
                {
                    "measure_name": "price_discount_sum_017d55a8",
                    "node": {
                        "name": "default.repair_orders_fact",
                        "version": "v1.0",
                        "display_name": "Repair Orders Fact",
                    },
                },
            ],
        },
        {
            "derived_expression": "SELECT  sum(price_sum_78a5eb43) + sum(price_sum_78a5eb43) AS "
            "default_DOT_double_total_repair_cost  FROM "
            "default.repair_order_details",
            "metric_expression": "sum(price_sum_78a5eb43) + sum(price_sum_78a5eb43)",
            "metric": {
                "name": "default.double_total_repair_cost",
                "version": "v1.0",
                "display_name": "Double Total Repair Cost",
            },
            "required_measures": [
                {
                    "measure_name": "price_sum_78a5eb43",
                    "node": {
                        "name": "default.repair_order_details",
                        "version": "v1.0",
                        "display_name": "default.roads.repair_order_details",
                    },
                },
            ],
        },
    ]
    assert results["measures_materializations"] == [
        {
            "columns": [
                {
                    "column": "country",
                    "name": "default_DOT_hard_hat_DOT_country",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.country",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "postal_code",
                    "name": "default_DOT_hard_hat_DOT_postal_code",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.postal_code",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "city",
                    "name": "default_DOT_hard_hat_DOT_city",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.city",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "hire_date",
                    "name": "default_DOT_hard_hat_DOT_hire_date",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.hire_date",
                    "semantic_type": "dimension",
                    "type": "timestamp",
                },
                {
                    "column": "state",
                    "name": "default_DOT_hard_hat_DOT_state",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.state",
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
                    "column": "local_region",
                    "name": "default_DOT_municipality_dim_DOT_local_region",
                    "node": "default.municipality_dim",
                    "semantic_entity": "default.municipality_dim.local_region",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "discount_sum_62846f49",
                    "name": "discount_sum_62846f49",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.discount_sum_62846f49",
                    "semantic_type": "measure",
                    "type": "bigint",
                },
                {
                    "column": "count_3389dae3",
                    "name": "count_3389dae3",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.count_3389dae3",
                    "semantic_type": "measure",
                    "type": "bigint",
                },
                {
                    "column": "repair_order_id_count_0b7dfba0",
                    "name": "repair_order_id_count_0b7dfba0",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.repair_order_id_count_0b7dfba0",
                    "semantic_type": "measure",
                    "type": "bigint",
                },
                {
                    "column": "price_count_78a5eb43",
                    "name": "price_count_78a5eb43",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price_count_78a5eb43",
                    "semantic_type": "measure",
                    "type": "bigint",
                },
                {
                    "column": "price_sum_78a5eb43",
                    "name": "price_sum_78a5eb43",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price_sum_78a5eb43",
                    "semantic_type": "measure",
                    "type": "double",
                },
                {
                    "column": "total_repair_cost_sum_9bdaf803",
                    "name": "total_repair_cost_sum_9bdaf803",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.total_repair_cost_sum_9bdaf803",
                    "semantic_type": "measure",
                    "type": "double",
                },
                {
                    "column": "price_discount_sum_017d55a8",
                    "name": "price_discount_sum_017d55a8",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price_discount_sum_017d55a8",
                    "semantic_type": "measure",
                    "type": "double",
                },
            ],
            "dimensions": [
                "default_DOT_hard_hat_DOT_country",
                "default_DOT_hard_hat_DOT_postal_code",
                "default_DOT_hard_hat_DOT_city",
                "default_DOT_hard_hat_DOT_hire_date",
                "default_DOT_hard_hat_DOT_state",
                "default_DOT_dispatcher_DOT_company_name",
                "default_DOT_municipality_dim_DOT_local_region",
            ],
            "grain": [
                "default_DOT_hard_hat_DOT_country",
                "default_DOT_hard_hat_DOT_postal_code",
                "default_DOT_hard_hat_DOT_city",
                "default_DOT_hard_hat_DOT_hire_date",
                "default_DOT_hard_hat_DOT_state",
                "default_DOT_dispatcher_DOT_company_name",
                "default_DOT_municipality_dim_DOT_local_region",
            ],
            "granularity": "day",
            "measures": [
                {
                    "aggregation": "SUM",
                    "expression": "if(discount > 0.0, 1, 0)",
                    "name": "discount_sum_62846f49",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "COUNT",
                    "expression": "*",
                    "name": "count_3389dae3",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "COUNT",
                    "expression": "repair_order_id",
                    "name": "repair_order_id_count_0b7dfba0",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "COUNT",
                    "expression": "price",
                    "name": "price_count_78a5eb43",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "SUM",
                    "expression": "price",
                    "name": "price_sum_78a5eb43",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "SUM",
                    "expression": "total_repair_cost",
                    "name": "total_repair_cost_sum_9bdaf803",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "SUM",
                    "expression": "price * discount",
                    "name": "price_discount_sum_017d55a8",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
            ],
            "node": {
                "name": "default.repair_orders_fact",
                "version": "v1.0",
                "display_name": "Repair Orders Fact",
            },
            "output_table_name": "default_repair_orders_fact_v1_0_c9390406463b348e",
            "query": mock.ANY,
            "spark_conf": None,
            "timestamp_column": "default_DOT_hard_hat_DOT_hire_date",
            "timestamp_format": "yyyyMMdd",
            "upstream_tables": [
                "default.roads.repair_orders",
                "default.roads.repair_order_details",
                "default.roads.hard_hats",
                "default.roads.dispatchers",
                "default.roads.municipality",
                "default.roads.municipality_municipality_type",
                "default.roads.municipality_type",
            ],
        },
        {
            "columns": [
                {
                    "column": "country",
                    "name": "default_DOT_hard_hat_DOT_country",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.country",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "postal_code",
                    "name": "default_DOT_hard_hat_DOT_postal_code",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.postal_code",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "city",
                    "name": "default_DOT_hard_hat_DOT_city",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.city",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "hire_date",
                    "name": "default_DOT_hard_hat_DOT_hire_date",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.hire_date",
                    "semantic_type": "dimension",
                    "type": "timestamp",
                },
                {
                    "column": "state",
                    "name": "default_DOT_hard_hat_DOT_state",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.state",
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
                    "column": "local_region",
                    "name": "default_DOT_municipality_dim_DOT_local_region",
                    "node": "default.municipality_dim",
                    "semantic_entity": "default.municipality_dim.local_region",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "price_sum_78a5eb43",
                    "name": "price_sum_78a5eb43",
                    "node": "default.repair_order_details",
                    "semantic_entity": "default.repair_order_details.price_sum_78a5eb43",
                    "semantic_type": "measure",
                    "type": "double",
                },
            ],
            "dimensions": [
                "default_DOT_hard_hat_DOT_country",
                "default_DOT_hard_hat_DOT_postal_code",
                "default_DOT_hard_hat_DOT_city",
                "default_DOT_hard_hat_DOT_hire_date",
                "default_DOT_hard_hat_DOT_state",
                "default_DOT_dispatcher_DOT_company_name",
                "default_DOT_municipality_dim_DOT_local_region",
            ],
            "grain": [
                "default_DOT_hard_hat_DOT_country",
                "default_DOT_hard_hat_DOT_postal_code",
                "default_DOT_hard_hat_DOT_city",
                "default_DOT_hard_hat_DOT_hire_date",
                "default_DOT_hard_hat_DOT_state",
                "default_DOT_dispatcher_DOT_company_name",
                "default_DOT_municipality_dim_DOT_local_region",
            ],
            "granularity": "day",
            "measures": [
                {
                    "aggregation": "SUM",
                    "expression": "price",
                    "name": "price_sum_78a5eb43",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
            ],
            "node": {
                "name": "default.repair_order_details",
                "version": "v1.0",
                "display_name": "default.roads.repair_order_details",
            },
            "output_table_name": "default_repair_order_details_v1_0_5bf367d2fc7c255d",
            "query": mock.ANY,
            "spark_conf": None,
            "timestamp_column": "default_DOT_hard_hat_DOT_hire_date",
            "timestamp_format": "yyyyMMdd",
            "upstream_tables": [
                "default.roads.repair_order_details",
                "default.roads.repair_orders",
                "default.roads.hard_hats",
                "default.roads.dispatchers",
                "default.roads.municipality",
                "default.roads.municipality_municipality_type",
                "default.roads.municipality_type",
            ],
        },
    ]
    measures_sql = """
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
      FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id
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
      WHERE  default_DOT_hard_hats.hire_date = CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS TIMESTAMP)
    ),
    default_DOT_dispatcher AS (
      SELECT
        default_DOT_dispatchers.dispatcher_id,
        default_DOT_dispatchers.company_name,
        default_DOT_dispatchers.phone
      FROM roads.dispatchers AS default_DOT_dispatchers
    ),
    default_DOT_municipality_dim AS (
      SELECT
        m.municipality_id AS municipality_id,
        m.contact_name,
        m.contact_title,
        m.local_region,
        m.state_id,
        mmt.municipality_type_id AS municipality_type_id,
        mt.municipality_type_desc AS municipality_type_desc
      FROM roads.municipality AS m LEFT JOIN roads.municipality_municipality_type AS mmt ON m.municipality_id = mmt.municipality_id
      LEFT JOIN roads.municipality_type AS mt ON mmt.municipality_type_id = mt.municipality_type_desc
    ),
    default_DOT_repair_orders_fact_built AS (
      SELECT
        default_DOT_repair_orders_fact.repair_order_id,
        default_DOT_repair_orders_fact.discount,
        default_DOT_repair_orders_fact.price,
        default_DOT_repair_orders_fact.total_repair_cost,
        default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
        default_DOT_hard_hat.postal_code default_DOT_hard_hat_DOT_postal_code,
        default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
        default_DOT_hard_hat.hire_date default_DOT_hard_hat_DOT_hire_date,
        default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
        default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
        default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region
      FROM default_DOT_repair_orders_fact INNER JOIN default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
      INNER JOIN default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
      INNER JOIN default_DOT_municipality_dim ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
    )

    SELECT  default_DOT_repair_orders_fact_built.default_DOT_hard_hat_DOT_country,
      default_DOT_repair_orders_fact_built.default_DOT_hard_hat_DOT_postal_code,
      default_DOT_repair_orders_fact_built.default_DOT_hard_hat_DOT_city,
      default_DOT_repair_orders_fact_built.default_DOT_hard_hat_DOT_hire_date,
      default_DOT_repair_orders_fact_built.default_DOT_hard_hat_DOT_state,
      default_DOT_repair_orders_fact_built.default_DOT_dispatcher_DOT_company_name,
      default_DOT_repair_orders_fact_built.default_DOT_municipality_dim_DOT_local_region,
      SUM(if(discount > 0.0, 1, 0)) AS discount_sum_62846f49,
      COUNT(*) AS count_3389dae3,
      COUNT(repair_order_id) AS repair_order_id_count_0b7dfba0,
      COUNT(price) AS price_count_78a5eb43,
      SUM(price) AS price_sum_78a5eb43,
      SUM(total_repair_cost) AS total_repair_cost_sum_9bdaf803,
      SUM(price * discount) AS price_discount_sum_017d55a8
    FROM default_DOT_repair_orders_fact_built
    GROUP BY  default_DOT_repair_orders_fact_built.default_DOT_hard_hat_DOT_country, default_DOT_repair_orders_fact_built.default_DOT_hard_hat_DOT_postal_code, default_DOT_repair_orders_fact_built.default_DOT_hard_hat_DOT_city, default_DOT_repair_orders_fact_built.default_DOT_hard_hat_DOT_hire_date, default_DOT_repair_orders_fact_built.default_DOT_hard_hat_DOT_state, default_DOT_repair_orders_fact_built.default_DOT_dispatcher_DOT_company_name, default_DOT_repair_orders_fact_built.default_DOT_municipality_dim_DOT_local_region
    """
    assert str(
        parse(
            results["measures_materializations"][0]["query"].replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            ),
        ),
    ) == str(parse(measures_sql))

    assert results["combiners"] == [
        {
            "columns": [
                {
                    "column": "country",
                    "name": "default_DOT_hard_hat_DOT_country",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.country",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "postal_code",
                    "name": "default_DOT_hard_hat_DOT_postal_code",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.postal_code",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "city",
                    "name": "default_DOT_hard_hat_DOT_city",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.city",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "hire_date",
                    "name": "default_DOT_hard_hat_DOT_hire_date",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.hire_date",
                    "semantic_type": "dimension",
                    "type": "timestamp",
                },
                {
                    "column": "state",
                    "name": "default_DOT_hard_hat_DOT_state",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.state",
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
                    "column": "local_region",
                    "name": "default_DOT_municipality_dim_DOT_local_region",
                    "node": "default.municipality_dim",
                    "semantic_entity": "default.municipality_dim.local_region",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "discount_sum_62846f49",
                    "name": "discount_sum_62846f49",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.discount_sum_62846f49",
                    "semantic_type": "measure",
                    "type": "bigint",
                },
                {
                    "column": "count_3389dae3",
                    "name": "count_3389dae3",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.count_3389dae3",
                    "semantic_type": "measure",
                    "type": "bigint",
                },
                {
                    "column": "repair_order_id_count_0b7dfba0",
                    "name": "repair_order_id_count_0b7dfba0",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.repair_order_id_count_0b7dfba0",
                    "semantic_type": "measure",
                    "type": "bigint",
                },
                {
                    "column": "price_count_78a5eb43",
                    "name": "price_count_78a5eb43",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price_count_78a5eb43",
                    "semantic_type": "measure",
                    "type": "bigint",
                },
                {
                    "column": "price_sum_78a5eb43",
                    "name": "price_sum_78a5eb43",
                    "node": "default.repair_order_details",
                    "semantic_entity": "default.repair_order_details.price_sum_78a5eb43",
                    "semantic_type": "measure",
                    "type": "double",
                },
                {
                    "column": "total_repair_cost_sum_9bdaf803",
                    "name": "total_repair_cost_sum_9bdaf803",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.total_repair_cost_sum_9bdaf803",
                    "semantic_type": "measure",
                    "type": "double",
                },
                {
                    "column": "price_discount_sum_017d55a8",
                    "name": "price_discount_sum_017d55a8",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price_discount_sum_017d55a8",
                    "semantic_type": "measure",
                    "type": "double",
                },
                {
                    "column": "price_sum_78a5eb43",
                    "name": "price_sum_78a5eb43",
                    "node": "default.repair_order_details",
                    "semantic_entity": "default.repair_order_details.price_sum_78a5eb43",
                    "semantic_type": "measure",
                    "type": "double",
                },
            ],
            "dimensions": [
                "default_DOT_hard_hat_DOT_country",
                "default_DOT_hard_hat_DOT_postal_code",
                "default_DOT_hard_hat_DOT_city",
                "default_DOT_hard_hat_DOT_hire_date",
                "default_DOT_hard_hat_DOT_state",
                "default_DOT_dispatcher_DOT_company_name",
                "default_DOT_municipality_dim_DOT_local_region",
            ],
            "druid_spec": {
                "dataSchema": {
                    "dataSource": "dj__default_example_repairs_cube_v1_0_41f1198b6e6032dd",
                    "granularitySpec": {
                        "intervals": [],
                        "segmentGranularity": "DAY",
                        "type": "uniform",
                    },
                    "metricsSpec": [
                        {
                            "fieldName": "discount_sum_62846f49",
                            "name": "discount_sum_62846f49",
                            "type": "longSum",
                        },
                        {
                            "fieldName": "count_3389dae3",
                            "name": "count_3389dae3",
                            "type": "longSum",
                        },
                        {
                            "fieldName": "repair_order_id_count_0b7dfba0",
                            "name": "repair_order_id_count_0b7dfba0",
                            "type": "longSum",
                        },
                        {
                            "fieldName": "price_count_78a5eb43",
                            "name": "price_count_78a5eb43",
                            "type": "longSum",
                        },
                        {
                            "fieldName": "price_sum_78a5eb43",
                            "name": "price_sum_78a5eb43",
                            "type": "doubleSum",
                        },
                        {
                            "fieldName": "total_repair_cost_sum_9bdaf803",
                            "name": "total_repair_cost_sum_9bdaf803",
                            "type": "doubleSum",
                        },
                        {
                            "fieldName": "price_discount_sum_017d55a8",
                            "name": "price_discount_sum_017d55a8",
                            "type": "doubleSum",
                        },
                        {
                            "fieldName": "price_sum_78a5eb43",
                            "name": "price_sum_78a5eb43",
                            "type": "doubleSum",
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
                "tuningConfig": {
                    "partitionsSpec": {
                        "targetPartitionSize": 5000000,
                        "type": "hashed",
                    },
                    "type": "hadoop",
                    "useCombiner": True,
                },
            },
            "grain": [
                "default_DOT_hard_hat_DOT_country",
                "default_DOT_hard_hat_DOT_postal_code",
                "default_DOT_hard_hat_DOT_city",
                "default_DOT_hard_hat_DOT_hire_date",
                "default_DOT_hard_hat_DOT_state",
                "default_DOT_dispatcher_DOT_company_name",
                "default_DOT_municipality_dim_DOT_local_region",
            ],
            "granularity": "day",
            "measures": [
                {
                    "aggregation": "SUM",
                    "expression": "if(discount > 0.0, 1, 0)",
                    "name": "discount_sum_62846f49",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "COUNT",
                    "expression": "*",
                    "name": "count_3389dae3",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "COUNT",
                    "expression": "repair_order_id",
                    "name": "repair_order_id_count_0b7dfba0",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "COUNT",
                    "expression": "price",
                    "name": "price_count_78a5eb43",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "SUM",
                    "expression": "price",
                    "name": "price_sum_78a5eb43",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "SUM",
                    "expression": "total_repair_cost",
                    "name": "total_repair_cost_sum_9bdaf803",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "SUM",
                    "expression": "price * discount",
                    "name": "price_discount_sum_017d55a8",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
                {
                    "aggregation": "SUM",
                    "expression": "price",
                    "name": "price_sum_78a5eb43",
                    "rule": {
                        "level": None,
                        "type": "full",
                    },
                },
            ],
            "node": {
                "display_name": "Example Repairs Cube",
                "name": "default.example_repairs_cube",
                "version": "v1.0",
            },
            "output_table_name": "default_example_repairs_cube_v1_0_41f1198b6e6032dd",
            "query": mock.ANY,
            "timestamp_column": "default_DOT_hard_hat_DOT_hire_date",
            "timestamp_format": "yyyyMMdd",
            "upstream_tables": [],
        },
    ]

    expected_combiner = """
    SELECT
      COALESCE(
        default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_hard_hat_DOT_country,
        default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_hard_hat_DOT_country
      ) default_DOT_hard_hat_DOT_country,
      COALESCE(
        default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_hard_hat_DOT_postal_code,
        default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_hard_hat_DOT_postal_code
      ) default_DOT_hard_hat_DOT_postal_code,
      COALESCE(
        default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_hard_hat_DOT_city,
        default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_hard_hat_DOT_city
      ) default_DOT_hard_hat_DOT_city,
      COALESCE(
        default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_hard_hat_DOT_hire_date,
        default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_hard_hat_DOT_hire_date
      ) default_DOT_hard_hat_DOT_hire_date,
      COALESCE(
        default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_hard_hat_DOT_state,
        default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_hard_hat_DOT_state
      ) default_DOT_hard_hat_DOT_state,
      COALESCE(
        default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_dispatcher_DOT_company_name,
        default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_dispatcher_DOT_company_name
      ) default_DOT_dispatcher_DOT_company_name,
      COALESCE(
        default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_municipality_dim_DOT_local_region,
        default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_municipality_dim_DOT_local_region
      ) default_DOT_municipality_dim_DOT_local_region,
      default_repair_orders_fact_v1_0_c9390406463b348e.discount_sum_62846f49,
      default_repair_orders_fact_v1_0_c9390406463b348e.count_3389dae3,
      default_repair_orders_fact_v1_0_c9390406463b348e.repair_order_id_count_0b7dfba0,
      default_repair_orders_fact_v1_0_c9390406463b348e.price_count_78a5eb43,
      default_repair_orders_fact_v1_0_c9390406463b348e.price_sum_78a5eb43,
      default_repair_orders_fact_v1_0_c9390406463b348e.total_repair_cost_sum_9bdaf803,
      default_repair_orders_fact_v1_0_c9390406463b348e.price_discount_sum_017d55a8,
      default_repair_order_details_v1_0_5bf367d2fc7c255d.price_sum_78a5eb43
    FROM default_repair_orders_fact_v1_0_c9390406463b348e
    FULL OUTER JOIN default_repair_order_details_v1_0_5bf367d2fc7c255d
      ON default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_hard_hat_DOT_country =
        default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_hard_hat_DOT_country
        AND default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_hard_hat_DOT_postal_code =
          default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_hard_hat_DOT_postal_code
        AND default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_hard_hat_DOT_city =
          default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_hard_hat_DOT_city
        AND default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_hard_hat_DOT_hire_date =
          default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_hard_hat_DOT_hire_date
        AND default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_hard_hat_DOT_state =
          default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_hard_hat_DOT_state
        AND default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_dispatcher_DOT_company_name =
          default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_dispatcher_DOT_company_name
        AND default_repair_orders_fact_v1_0_c9390406463b348e.default_DOT_municipality_dim_DOT_local_region =
          default_repair_order_details_v1_0_5bf367d2fc7c255d.default_DOT_municipality_dim_DOT_local_region
    """
    assert str(parse(results["combiners"][0]["query"])) == str(parse(expected_combiner))


@pytest.mark.asyncio
async def test_get_all_cubes(
    client_with_repairs_cube: AsyncClient,
):
    """
    Test getting all cubes and limiting to available cubes
    """
    await make_a_test_cube(
        client_with_repairs_cube,
        "default.repairs_cube_9",
    )

    # Get all cubes
    response = await client_with_repairs_cube.get("/cubes")
    assert response.is_success
    data = response.json()
    assert len([cube for cube in data if cube["name"] == "default.repairs_cube_9"]) == 1

    # Get cubes available in default and test that this cube is excluded
    response = await client_with_repairs_cube.get("/cubes?catalog=default")
    assert response.is_success
    data = response.json()
    assert len([cube for cube in data if cube["name"] == "default.repairs_cube_9"]) == 0

    # Set an avialability for the cube
    await client_with_repairs_cube.post(
        "/data/default.repairs_cube_9/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": "repairs_cube",
            "valid_through_ts": 1010129120,
        },
    )

    # Get only cubes available in default and test that this cube is now included
    response = await client_with_repairs_cube.get("/cubes?catalog=default")
    assert response.is_success
    data = response.json()
    assert len([cube for cube in data if cube["name"] == "default.repairs_cube_9"]) == 1
