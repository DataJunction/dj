# pylint: disable=too-many-lines
"""Tests for /materialization api"""
import json
import os
from pathlib import Path
from unittest.mock import call

import pytest
import pytest_asyncio
from httpx import AsyncClient

from datajunction_server.models.partition import PartitionBackfill
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing.backends.antlr4 import parse

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def load_expected_file():
    """
    Loads expected fixture file
    """

    def _load(filename: str):
        expected_path = TEST_DIR / Path("files/materializations_test")
        with open(expected_path / filename, encoding="utf-8") as fe:
            if filename.endswith(".json"):
                return json.loads(fe.read().strip())
            return fe.read().strip()

    return _load


@pytest_asyncio.fixture
async def client_with_repairs_cube(
    client_with_query_service_example_loader,
):
    """
    Adds a repairs cube to the test client
    """
    custom_client = await client_with_query_service_example_loader(["ROADS"])
    response = await custom_client.post(
        "/nodes/default.repair_orders_fact/columns/order_date/attributes/",
        json=[{"name": "dimension"}],
    )
    assert response.status_code in (200, 201)
    response = await custom_client.post(
        "/nodes/cube/",
        json={
            "metrics": [
                "default.num_repair_orders",
                "default.total_repair_cost",
            ],
            "dimensions": [
                "default.repair_orders_fact.order_date",
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
def set_temporal_column(
    client_with_repairs_cube,
):  # pylint: disable=redefined-outer-name
    """
    Sets the given column as a temporal partition on the specified node.
    """

    async def _set_temporal_column(node_name: str, column: str):
        response = await client_with_repairs_cube.post(
            f"/nodes/{node_name}/columns/{column}/partition",
            json={
                "type_": "temporal",
                "granularity": "day",
                "format": "yyyyMMdd",
            },
        )
        assert response.status_code in (200, 201)

    return _set_temporal_column


@pytest.fixture
def set_categorical_partition(
    client_with_repairs_cube,
):  # pylint: disable=redefined-outer-name
    """
    Sets the given column as a categorical partition on the specified node.
    """

    async def _set_categorical_partition(node_name: str, column: str):
        response = await client_with_repairs_cube.post(
            f"/nodes/{node_name}/columns/{column}/partition",
            json={
                "type_": "categorical",
            },
        )
        assert response.status_code in (200, 201)

    return _set_categorical_partition


@pytest.mark.asyncio
async def test_materialization_info(client: AsyncClient) -> None:
    """
    Test ``GET /materialization/info``.
    """
    response = await client.get("/materialization/info")
    data = response.json()

    assert response.status_code == 200
    assert data == {
        "job_types": [
            {
                "allowed_node_types": ["transform", "dimension", "cube"],
                "description": "Spark SQL materialization job",
                "job_class": "SparkSqlMaterializationJob",
                "label": "Spark SQL",
                "name": "spark_sql",
            },
            {
                "allowed_node_types": ["cube"],
                "description": "Used to materialize a cube's measures to Druid for "
                "low-latency access to a set of metrics and "
                "dimensions. While the logical cube definition "
                "is at the level of metrics and dimensions, this "
                "materialized Druid cube will contain "
                "measures and dimensions, with rollup "
                "configured on the measures where appropriate.",
                "job_class": "DruidMeasuresCubeMaterializationJob",
                "label": "Druid Measures Cube (Pre-Agg Cube)",
                "name": "druid_measures_cube",
            },
            {
                "allowed_node_types": ["cube"],
                "description": "Used to materialize a cube of metrics and "
                "dimensions to Druid for low-latency access. "
                "The materialized cube is at the metric level, "
                "meaning that all metrics will be aggregated to "
                "the level of the cube's dimensions.",
                "job_class": "DruidMetricsCubeMaterializationJob",
                "label": "Druid Metrics Cube (Post-Agg Cube)",
                "name": "druid_metrics_cube",
            },
        ],
        "strategies": [
            {"label": "Full", "name": "full"},
            {"label": "Snapshot", "name": "snapshot"},
            {"label": "Snapshot Partition", "name": "snapshot_partition"},
            {"label": "Incremental Time", "name": "incremental_time"},
            {"label": "View", "name": "view"},
        ],
    }


@pytest.mark.asyncio
async def test_crud_materialization(client_with_query_service: AsyncClient):
    """
    Verifies the CRUD endpoints for adding/updating/deleting materialization and backfill
    """
    # Create the engine and check the existing transform node
    await client_with_query_service.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "2.4.4",
            "dialect": "spark",
        },
    )

    response = await client_with_query_service.get(
        "/nodes/basic.transform.country_agg/",
    )
    old_node_data = response.json()
    assert old_node_data["version"] == "v1.0"
    assert old_node_data["materializations"] == []

    # Setting the materialization config should succeed
    response = await client_with_query_service.post(
        "/nodes/basic.transform.country_agg/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "full",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    data = response.json()
    assert (
        data["message"] == "Successfully updated materialization config named "
        "`spark_sql__full` for node `basic.transform.country_agg`"
    )

    # Check history of the node with materialization
    response = await client_with_query_service.get(
        "/history?node=basic.transform.country_agg",
    )
    history = response.json()
    assert [
        (activity["activity_type"], activity["entity_type"]) for activity in history
    ] == [("create", "materialization"), ("create", "node")]

    # Setting it again should inform that it already exists
    response = await client_with_query_service.post(
        "/nodes/basic.transform.country_agg/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "full",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    assert response.json() == {
        "info": {
            "output_tables": ["common.a", "common.b"],
            "urls": ["http://fake.url/job"],
        },
        "message": "The same materialization config with name "
        "`spark_sql__full` already exists for node "
        "`basic.transform.country_agg` so no update was performed.",
    }

    # Deactivating it should work
    response = await client_with_query_service.delete(
        "/nodes/basic.transform.country_agg/materializations/"
        "?materialization_name=spark_sql__full",
    )
    assert response.json() == {
        "message": "The materialization named `spark_sql__full` on node "
        "`basic.transform.country_agg` has been successfully deactivated",
    }

    # Setting it again should inform that it already exists but was reactivated
    response = await client_with_query_service.post(
        "/nodes/basic.transform.country_agg/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "full",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    assert response.json()["message"] == (
        "The same materialization config with name `spark_sql__full` already "
        "exists for node `basic.transform.country_agg` but was deactivated. It has "
        "now been restored."
    )
    response = await client_with_query_service.get(
        "/history?node=basic.transform.country_agg",
    )
    assert [
        (
            activity["activity_type"],
            activity["entity_type"],
            activity["entity_name"],
        )
        for activity in response.json()
    ] == [
        ("restore", "materialization", "spark_sql__full"),
        ("delete", "materialization", "spark_sql__full"),
        ("create", "materialization", "spark_sql__full"),
        ("create", "node", "basic.transform.country_agg"),
    ]


@pytest.mark.asyncio
async def test_druid_measures_cube_full(
    client_with_repairs_cube: AsyncClient,  # pylint: disable=redefined-outer-name
    query_service_client: QueryServiceClient,
    load_expected_file,  # pylint: disable=redefined-outer-name
    set_temporal_column,  # pylint: disable=redefined-outer-name
):
    """
    Verifying this materialization setup:
    - Job Type: druid_measures_cube
    - Strategy: full
    Cases to check:
    - [success] When there is a column on the cube with type `timestamp`
    - [success] When there is a column on the cube with the partition label
    - [failure] When there are no columns on the cube with type `timestamp` and no partition labels
    - [failure] If nothing has changed, will not update the existing materialization
    """
    # [success] When there is a column on the cube with type `timestamp`:
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "job": "druid_measures_cube",
            "strategy": "full",
            "config": {},
            "schedule": "@daily",
        },
    )
    assert response.json()["message"] == (
        "Successfully updated materialization config named "
        "`druid_measures_cube__full` for node `default.repairs_cube`"
    )
    args, _ = query_service_client.materialize.call_args_list[0]  # type: ignore
    assert str(parse(args[0].query)) == str(
        parse(load_expected_file("druid_measures_cube.full.query.sql")),
    )
    assert args[0].druid_spec == load_expected_file(
        "druid_measures_cube.full.druid_spec.json",
    )

    # Reset by deleting the materialization
    response = await client_with_repairs_cube.delete(
        "/nodes/default.repairs_cube/materializations/",
        params={
            "materialization_name": "druid_measures_cube__full",
        },
    )
    assert response.status_code in (200, 201)

    # [success] When there is a column on the cube with a temporal partition label:
    await set_temporal_column(
        "default.repairs_cube",
        "default.repair_orders_fact.order_date",
    )
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "job": "druid_measures_cube",
            "strategy": "full",
            "config": {
                "spark": {},
            },
            "schedule": "",
        },
    )
    assert (
        response.json()["message"]
        == "Successfully updated materialization config named "
        "`druid_measures_cube__full__default.repair_orders_fact.order_date` "
        "for node `default.repairs_cube`"
    )
    args, _ = query_service_client.materialize.call_args_list[0]  # type: ignore
    print("args[0].query", args[0].query)
    assert str(parse(args[0].query)) == str(
        parse(load_expected_file("druid_measures_cube.full.query.sql")),
    )
    assert args[0].druid_spec == load_expected_file(
        "druid_measures_cube.full.druid_spec.json",
    )

    # [failure] When there are no columns on the cube with type `timestamp` and no partition labels
    response = await client_with_repairs_cube.post(
        "/nodes/cube/",
        json={
            "metrics": [
                "default.num_repair_orders",
                "default.total_repair_cost",
            ],
            "dimensions": [
                "default.hard_hat.state",
                "default.dispatcher.company_name",
                "default.municipality_dim.local_region",
            ],
            "filters": ["default.hard_hat.state='AZ'"],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.bad_repairs_cube",
        },
    )
    assert response.status_code in (200, 201)
    response = await client_with_repairs_cube.post(
        "/nodes/default.bad_repairs_cube/materialization/",
        json={
            "job": "druid_measures_cube",
            "strategy": "full",
            "config": {
                "spark": {},
            },
            "schedule": "",
        },
    )
    assert response.json()["message"] == (
        "The cube materialization cannot be configured if there is no "
        "temporal partition specified on the cube. Please make sure at "
        "least one cube element has a temporal partition defined"
    )

    # [failure] If nothing has changed, will not update the existing materialization
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "job": "druid_measures_cube",
            "strategy": "full",
            "config": {
                "druid": {"a": "b"},
                "spark": {},
            },
            "schedule": "",
        },
    )
    assert response.json()["message"] == (
        "The same materialization config with name "
        "`druid_measures_cube__full__default.repair_orders_fact.order_date` already "
        "exists for node `default.repairs_cube` so no update was performed."
    )


@pytest.mark.asyncio
async def test_druid_measures_cube_incremental(
    client_with_repairs_cube: AsyncClient,  # pylint: disable=redefined-outer-name
    query_service_client: QueryServiceClient,
    load_expected_file,  # pylint: disable=redefined-outer-name
    set_temporal_column,  # pylint: disable=redefined-outer-name
    set_categorical_partition,  # pylint: disable=redefined-outer-name,
):
    """
    Verifying this materialization setup:
    - Job Type: druid_measures_cube
    - Strategy: incremental_time
    Cases to check:
    - [failure] If there is no time partition column configured, fail. This is because without the
                time partition we don't know the granularity for incremental materialization.
    - [success] When there is a column on the cube with the partition label
    - [success] When the underlying measures node contains DJ_LOGICAL_TIMESTAMP
    """
    # [failure] If there is no time partition column configured
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "job": "druid_measures_cube",
            "strategy": "incremental_time",
            "config": {},
            "schedule": "@daily",
        },
    )
    assert response.json()["message"] == (
        "Cannot create materialization with strategy `incremental_time` "
        "without specifying a time partition column!"
    )

    # [success] When there is a column on the cube with the partition label, should succeed.
    await set_temporal_column(
        "default.repairs_cube",
        "default.repair_orders_fact.order_date",
    )
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "job": "druid_measures_cube",
            "strategy": "incremental_time",
            "config": {},
            "schedule": "@daily",
        },
    )
    assert response.status_code in (200, 201)
    assert response.json()["message"] == (
        "Successfully updated materialization config named "
        "`druid_measures_cube__incremental_time__default.repair_orders_fact.order_date` "
        "for node `default.repairs_cube`"
    )
    args, _ = query_service_client.materialize.call_args_list[0]  # type: ignore
    assert str(
        parse(
            args[0].query.replace("${dj_logical_timestamp}", "DJ_LOGICAL_TIMESTAMP()"),
        ),
    ) == str(
        parse(
            load_expected_file("druid_measures_cube.incremental.query.sql").replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            ),
        ),
    )
    assert args[0].druid_spec == load_expected_file(
        "druid_measures_cube.incremental.druid_spec.json",
    )

    # [success] When the node itself contains DJ_LOGICAL_TIMESTAMP
    response = await client_with_repairs_cube.patch(
        "/nodes/default.repair_orders_fact",
        json={
            "query": """SELECT
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
FROM
  default.repair_orders repair_orders
JOIN
  default.repair_order_details repair_order_details
ON repair_orders.repair_order_id = repair_order_details.repair_order_id
WHERE repair_orders.order_date = DJ_LOGICAL_TIMESTAMP()""",
        },
    )
    assert response.status_code in (200, 201)
    response = await client_with_repairs_cube.get("/nodes/default.repair_orders_fact")

    # Delete previous
    response = await client_with_repairs_cube.delete(
        "/nodes/default.repairs_cube/materializations/",
        params={
            "materialization_name": "druid_measures_cube__incremental_time__default."
            "repair_orders_fact.order_date",
        },
    )
    assert response.status_code in (200, 201)
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "job": "druid_measures_cube",
            "strategy": "incremental_time",
            "config": {},
            "schedule": "@daily",
        },
    )
    assert response.status_code in (200, 201)
    args, _ = query_service_client.materialize.call_args_list[1]  # type: ignore
    assert str(
        parse(
            args[0].query.replace("${dj_logical_timestamp}", "DJ_LOGICAL_TIMESTAMP()"),
        ),
    ) == str(
        parse(
            load_expected_file(
                "druid_measures_cube.incremental.patched.query.sql",
            ).replace("${dj_logical_timestamp}", "DJ_LOGICAL_TIMESTAMP()"),
        ),
    )
    assert args[0].druid_spec == load_expected_file(
        "druid_measures_cube.incremental.druid_spec.json",
    )

    # [success] When there is a column on the cube with the partition label, should succeed.
    await set_temporal_column(
        "default.repairs_cube",
        "default.repair_orders_fact.order_date",
    )
    await set_categorical_partition(
        "default.repairs_cube",
        "default.dispatcher.company_name",
    )
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "job": "druid_measures_cube",
            "strategy": "incremental_time",
            "config": {},
            "schedule": "@daily",
        },
    )
    assert response.status_code in (200, 201)
    assert response.json()["message"] == (
        "Successfully updated materialization config named "
        "`druid_measures_cube__incremental_time__default.repair_"
        "orders_fact.order_date__default.dispatcher.company_name` "
        "for node `default.repairs_cube`"
    )
    args, _ = query_service_client.materialize.call_args_list[2]  # type: ignore
    print("args[0]", args[0].query)
    assert str(
        parse(
            args[0]
            .query.replace("${dj_logical_timestamp}", "DJ_LOGICAL_TIMESTAMP()")
            .replace("${default_DOT_dispatcher_DOT_company_name}", "DJ_COMPANY_NAME()"),
        ),
    ) == str(
        parse(
            load_expected_file("druid_measures_cube.incremental.categorical.query.sql")
            .replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            )
            .replace("${default_DOT_dispatcher_DOT_company_name}", "DJ_COMPANY_NAME()"),
        ),
    )
    assert args[0].druid_spec == load_expected_file(
        "druid_measures_cube.incremental.druid_spec.json",
    )


@pytest.mark.asyncio
async def test_druid_metrics_cube_incremental(
    client_with_repairs_cube: AsyncClient,  # pylint: disable=redefined-outer-name
    query_service_client: QueryServiceClient,
    load_expected_file,  # pylint: disable=redefined-outer-name
    set_temporal_column,  # pylint: disable=redefined-outer-name,
    set_categorical_partition,  # pylint: disable=redefined-outer-name,
):
    """
    Verifying this materialization setup:
    - Job Type: druid_metrics_cube
    - Strategy: incremental_time
    Cases to check:
    - [failure] If there is no time partition column configured, fail. This is because without the
                time partition we don't know the granularity for incremental materialization.
    - [success] When there is a column on the cube with the partition label
    - [success] When the underlying measures node contains DJ_LOGICAL_TIMESTAMP
    """
    # [failure] If there is no time partition column configured
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "job": "druid_metrics_cube",
            "strategy": "incremental_time",
            "config": {},
            "schedule": "@daily",
        },
    )
    assert response.json()["message"] == (
        "Cannot create materialization with strategy `incremental_time` "
        "without specifying a time partition column!"
    )

    # [success] When there is a temporal partition column on the cube, should succeed.
    await set_temporal_column(
        "default.repairs_cube",
        "default.repair_orders_fact.order_date",
    )
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "job": "druid_metrics_cube",
            "strategy": "incremental_time",
            "config": {},
            "schedule": "@daily",
        },
    )
    assert response.status_code in (200, 201)
    assert response.json()["message"] == (
        "Successfully updated materialization config named "
        "`druid_metrics_cube__incremental_time__default.repair_orders_fact.order_date` "
        "for node `default.repairs_cube`"
    )
    args, _ = query_service_client.materialize.call_args_list[0]  # type: ignore
    assert str(
        parse(
            args[0].query.replace("${dj_logical_timestamp}", "DJ_LOGICAL_TIMESTAMP()"),
        ),
    ) == str(
        parse(
            load_expected_file("druid_metrics_cube.incremental.query.sql").replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            ),
        ),
    )
    assert args[0].druid_spec == load_expected_file(
        "druid_metrics_cube.incremental.druid_spec.json",
    )

    # [success] When there is both a temporal and categorical partition column on the cube
    await set_temporal_column(
        "default.repairs_cube",
        "default.repair_orders_fact.order_date",
    )
    await set_categorical_partition(
        "default.repairs_cube",
        "default.hard_hat.state",
    )
    response = await client_with_repairs_cube.post(
        "/nodes/default.repairs_cube/materialization/",
        json={
            "job": "druid_metrics_cube",
            "strategy": "incremental_time",
            "config": {},
            "schedule": "@daily",
        },
    )
    assert response.status_code in (200, 201)
    assert response.json()["message"] == (
        "Successfully updated materialization config named "
        "`druid_metrics_cube__incremental_time__default.repair_"
        "orders_fact.order_date__default.hard_hat.state` "
        "for node `default.repairs_cube`"
    )
    args, _ = query_service_client.materialize.call_args_list[1]  # type: ignore
    assert str(
        parse(
            args[0]
            .query.replace("${dj_logical_timestamp}", "DJ_LOGICAL_TIMESTAMP()")
            .replace("${default_DOT_hard_hat_DOT_state}", "DJ_STATE()"),
        ),
    ) == str(
        parse(
            load_expected_file("druid_metrics_cube.incremental.categorical.query.sql")
            .replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            )
            .replace("${default_DOT_hard_hat_DOT_state}", "DJ_STATE()"),
        ),
    )
    assert args[0].druid_spec == load_expected_file(
        "druid_metrics_cube.incremental.druid_spec.json",
    )


@pytest.mark.asyncio
async def test_spark_sql_full(
    client_with_repairs_cube: AsyncClient,  # pylint: disable=redefined-outer-name
    query_service_client: QueryServiceClient,
    load_expected_file,  # pylint: disable=redefined-outer-name
):
    """
    Verifying this materialization setup:
    - Job Type: SPARK_SQL
    - Strategy: FULL
    Cases to check:
    - [failure] If the node SQL uses DJ_LOGICAL_TIMESTAMP(), the FULL strategy is not allowed
    - [success] A transform/dimension with no partitions should work
    - [success] A transform/dimension with partitions but no DJ_LOGICAL_TIMESTAMP() should work
                This just means that the output table will be partitioned by the partition cols
    """
    # [success] A transform/dimension with no partitions should work
    response = await client_with_repairs_cube.post(
        "/nodes/default.hard_hat/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "full",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    data = response.json()
    assert (
        data["message"]
        == "Successfully updated materialization config named `spark_sql__full` for node "
        "`default.hard_hat`"
    )

    # Reading the node should yield the materialization config
    response = await client_with_repairs_cube.get("/nodes/default.hard_hat/")
    data = response.json()
    assert data["version"] == "v1.0"
    materialization_query = data["materializations"][0]["config"]["query"]
    assert str(parse(materialization_query)) == str(
        parse(load_expected_file("spark_sql.full.query.sql")),
    )
    del data["materializations"][0]["config"]["query"]
    assert data["materializations"] == load_expected_file("spark_sql.full.config.json")

    # Set both temporal and categorical partitions on node
    response = await client_with_repairs_cube.post(
        "/nodes/default.hard_hat/columns/birth_date/partition",
        json={
            "type_": "temporal",
            "granularity": "day",
            "format": "yyyyMMdd",
        },
    )
    assert response.status_code in (200, 201)

    response = await client_with_repairs_cube.post(
        "/nodes/default.hard_hat/columns/country/partition",
        json={
            "type_": "categorical",
        },
    )
    assert response.status_code in (200, 201)

    # Setting the materialization config should succeed and it should reschedule
    # the materialization with the temporal partition
    response = await client_with_repairs_cube.post(
        "/nodes/default.hard_hat/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "full",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    data = response.json()
    assert (
        data["message"] == "Successfully updated materialization config named "
        "`spark_sql__full__birth_date__country` for node `default.hard_hat`"
    )
    expected_query = load_expected_file("spark_sql.full.query.sql")
    args, _ = query_service_client.materialize.call_args_list[1]  # type: ignore
    assert str(parse(args[0].query)) == str(parse(expected_query))

    # Check that the temporal partition is appended onto the list of partitions in the
    # materialization config but is not included directly in the materialization query
    response = await client_with_repairs_cube.get("/nodes/default.hard_hat/")
    data = response.json()
    assert data["version"] == "v1.0"
    assert len(data["materializations"]) == 2

    expected_query = load_expected_file("spark_sql.full.partition.query.sql")
    args, _ = query_service_client.materialize.call_args_list[1]  # type: ignore
    assert str(parse(args[0].query)) == str(parse(expected_query))
    materialization_with_partitions = data["materializations"][1]
    del materialization_with_partitions["config"]["query"]
    expected_config = load_expected_file("spark_sql.full.partition.config.json")
    assert materialization_with_partitions == expected_config

    # Check listing materializations of the node
    response = await client_with_repairs_cube.get(
        "/nodes/default.hard_hat/materializations/",
    )
    materializations = response.json()
    assert materializations[0] == load_expected_file(
        "spark_sql.full.materializations.json",
    )
    materializations = response.json()
    assert materializations[1] == load_expected_file(
        "spark_sql.full.partition.materializations.json",
    )

    # Kick off backfill for this materialization
    response = await client_with_repairs_cube.post(
        "/nodes/default.hard_hat/materializations/spark_sql__full__birth_date__country/backfill",
        json=[
            {
                "column_name": "birth_date",
                "range": ["20230101", "20230201"],
            },
        ],
    )
    assert query_service_client.run_backfill.call_args_list == [  # type: ignore
        call(
            "default.hard_hat",
            "spark_sql__full__birth_date__country",
            [
                PartitionBackfill(
                    column_name="birth_date",
                    values=None,
                    range=["20230101", "20230201"],
                ),
            ],
        ),
    ]
    assert response.json() == {"output_tables": [], "urls": ["http://fake.url/job"]}


@pytest.mark.asyncio
async def test_spark_sql_incremental(
    client_with_repairs_cube: AsyncClient,  # pylint: disable=redefined-outer-name
    query_service_client: QueryServiceClient,
    set_temporal_column,  # pylint: disable=redefined-outer-name
    set_categorical_partition,  # pylint: disable=redefined-outer-name
    load_expected_file,  # pylint: disable=redefined-outer-name
):
    """
    Verifying this materialization setup:
    - Job Type: SPARK_SQL
    - Strategy: INCREMENTAL
    Cases to check:
    - [failure] If the node SQL uses DJ_LOGICAL_TIMESTAMP(), the FULL strategy is not allowed
    - [success] A transform/dimension with a time partition should work
    - [success] A transform/dimension with a time partition and additional usage of
                DJ_LOGICAL_TIMESTAMP() should work
    - [success] A transform/dimension with a time partition and a categorical partition should work
    """
    # [failure] No time partitions
    response = await client_with_repairs_cube.post(
        "/nodes/default.hard_hat/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "incremental_time",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    data = response.json()
    assert (
        data["message"]
        == "Cannot create materialization with strategy `incremental_time` without "
        "specifying a time partition column!"
    )

    # [success] A transform/dimension with a time partition should work
    await set_temporal_column("default.hard_hat", "birth_date")
    response = await client_with_repairs_cube.post(
        "/nodes/default.hard_hat/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "incremental_time",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    assert response.json()["message"] == (
        "Successfully updated materialization config named "
        "`spark_sql__incremental_time__birth_date` for node `default.hard_hat`"
    )

    # The materialization query contains a filter on the time partition column
    # to the DJ_LOGICAL_TIMESTAMP
    args, _ = query_service_client.materialize.call_args_list[0]  # type: ignore
    materialization_query = args[0].query
    assert str(
        parse(
            materialization_query.replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            ),
        ),
    ) == str(
        parse(
            load_expected_file("spark_sql.incremental.query.sql").replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            ),
        ),
    )

    # Reading the node should yield the materialization config
    response = await client_with_repairs_cube.get("/nodes/default.hard_hat/")
    data = response.json()
    assert data["version"] == "v1.0"
    del data["materializations"][0]["config"]["query"]
    assert data["materializations"] == load_expected_file(
        "spark_sql.incremental.config.json",
    )

    # Kick off backfill for this materialization
    response = await client_with_repairs_cube.post(
        "/nodes/default.hard_hat/materializations/spark_sql__incremental_time__birth_date/backfill",
        json=[
            {
                "column_name": "birth_date",
                "range": ["20230101", "20230201"],
            },
        ],
    )
    assert query_service_client.run_backfill.call_args_list == [  # type: ignore
        call(
            "default.hard_hat",
            "spark_sql__incremental_time__birth_date",
            [
                PartitionBackfill(
                    column_name="birth_date",
                    values=None,
                    range=["20230101", "20230201"],
                ),
            ],
        ),
    ]
    assert response.json() == {"output_tables": [], "urls": ["http://fake.url/job"]}

    # [success] A transform/dimension with a time partition and additional usage of
    # DJ_LOGICAL_TIMESTAMP() should work
    response = await client_with_repairs_cube.patch(
        "/nodes/default.hard_hat",
        json={
            "query": "SELECT last_name, first_name, birth_date, country FROM default.hard_hats"
            " WHERE DATE_FORMAT(birth_date, 'yyyyMMdd') = "
            "DATE_FORMAT(DJ_LOGICAL_TIMESTAMP(), 'yyyyMMdd')",
        },
    )
    assert response.status_code in (200, 201)

    # The materialization query contains a filter on the time partition column
    # to the DJ_LOGICAL_TIMESTAMP
    args, _ = query_service_client.materialize.call_args_list[1]  # type: ignore
    materialization_query = args[0].query
    assert str(
        parse(
            materialization_query.replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            ),
        ),
    ) == str(
        parse(
            load_expected_file("spark_sql.incremental.additional.query.sql").replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            ),
        ),
    )

    # [success] A transform/dimension with a time partition and a categorical partition should work
    await set_temporal_column("default.hard_hat", "birth_date")
    await set_categorical_partition("default.hard_hat", "country")
    response = await client_with_repairs_cube.post(
        "/nodes/default.hard_hat/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "incremental_time",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    assert response.status_code in (200, 201)

    # The materialization query contains a filter on the time partition column
    # and the categorical partition columns
    args, _ = query_service_client.materialize.call_args_list[2]  # type: ignore
    materialization_query = args[0].query

    assert str(
        parse(
            materialization_query.replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            ).replace("${country}", "DJ_COUNTRY()"),
        ),
    ) == str(
        parse(
            load_expected_file("spark_sql.incremental.categorical.query.sql")
            .replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            )
            .replace("${country}", "DJ_COUNTRY()"),
        ),
    )

    # [success] A transform/dimension with a time partition and a categorical partition,
    # along with a lookback window configured
    await set_temporal_column("default.hard_hat", "birth_date")
    await set_categorical_partition("default.hard_hat", "country")
    response = await client_with_repairs_cube.post(
        "/nodes/default.hard_hat/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "incremental_time",
            "config": {
                "lookback_window": "100 DAYS",
            },
            "schedule": "0 * * * *",
        },
    )
    assert response.status_code in (200, 201)

    # The materialization query's temporal partition filter includes the lookback window
    args, _ = query_service_client.materialize.call_args_list[3]  # type: ignore
    materialization_query = args[0].query

    assert str(
        parse(
            materialization_query.replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            ).replace("${country}", "DJ_COUNTRY()"),
        ),
    ) == str(
        parse(
            load_expected_file("spark_sql.incremental.lookback.query.sql")
            .replace(
                "${dj_logical_timestamp}",
                "DJ_LOGICAL_TIMESTAMP()",
            )
            .replace("${country}", "DJ_COUNTRY()"),
        ),
    )


@pytest.mark.asyncio
async def test_spark_with_availablity(
    client_with_repairs_cube: AsyncClient,  # pylint: disable=redefined-outer-name
):
    """
    Verify that we build the Query correctly with or without the materialization availability.
    """
    # add one transform node
    response = await client_with_repairs_cube.post(
        "/nodes/transform/",
        json={
            "query": ("SELECT first_name, birth_date FROM default.hard_hats"),
            "description": "test transform",
            "mode": "published",
            "name": "default.test_transform",
        },
    )
    assert response.status_code in (200, 201)

    # add another transform node on top of the 1st one
    response = await client_with_repairs_cube.post(
        "/nodes/transform/",
        json={
            "query": ("SELECT first_name, birth_date FROM default.test_transform"),
            "description": "test transform two",
            "mode": "published",
            "name": "default.test_transform_two",
        },
    )
    assert response.status_code in (200, 201)

    # create a materialization on the 1st node (w/o availability)
    response = await client_with_repairs_cube.post(
        "/nodes/default.test_transform/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "full",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    assert response.status_code in (200, 201)

    # check the materialization on 1st node
    query_one = (
        "SELECT  default_DOT_test_transform.first_name,"
        "\n\tdefault_DOT_test_transform.birth_date "
        "\n FROM (SELECT  default_DOT_hard_hats.first_name,"
        "\n\tdefault_DOT_hard_hats.birth_date "
        "\n FROM roads.hard_hats AS default_DOT_hard_hats)"
        "\n AS default_DOT_test_transform\n\n"
    )
    response = await client_with_repairs_cube.get(
        "/nodes/default.test_transform/materializations/",
    )
    assert response.status_code in (200, 201)
    assert response.json()[0]["config"]["query"] == query_one

    # create a materialization on the 2nd node (w/o availability)
    response = await client_with_repairs_cube.post(
        "/nodes/default.test_transform_two/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "full",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    assert response.status_code in (200, 201)

    # check the materialization on 2nd node
    query_two = (
        "SELECT  default_DOT_test_transform_two.first_name,"
        "\n\tdefault_DOT_test_transform_two.birth_date "
        "\n FROM (SELECT  default_DOT_test_transform.first_name,"
        "\n\tdefault_DOT_test_transform.birth_date "
        "\n FROM (SELECT  default_DOT_hard_hats.first_name,"
        "\n\tdefault_DOT_hard_hats.birth_date "
        "\n FROM roads.hard_hats AS default_DOT_hard_hats)"
        "\n AS default_DOT_test_transform)"
        "\n AS default_DOT_test_transform_two\n\n"
    )
    response = await client_with_repairs_cube.get(
        "/nodes/default.test_transform_two/materializations/",
    )
    assert response.status_code in (200, 201)
    assert response.json()[0]["config"]["query"] == query_two

    # add some availability to the 1st node
    response = await client_with_repairs_cube.post(
        "/data/default.test_transform/availability/",
        json={
            "catalog": "default",
            "schema_": "accounting",
            "table": "test_transform_materialized",
            "valid_through_ts": 20230125,
            "max_temporal_partition": ["2023", "01", "25"],
            "min_temporal_partition": ["2022", "01", "01"],
            "url": "http://some.catalog.com/default.accounting.pmts",
        },
    )
    assert response.status_code in (200, 201)

    # refresh the materialization on 1st node now with availability (query should be the same)
    response = await client_with_repairs_cube.post(
        "/nodes/default.test_transform/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "full",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    assert response.status_code in (200, 201)

    # check the materialization query again (query should be the same)
    response = await client_with_repairs_cube.get(
        "/nodes/default.test_transform/materializations/",
    )
    assert response.status_code in (200, 201)
    assert response.json()[0]["config"]["query"] == query_one

    # refresh the materialization on 2nd node now with availability
    # (query should now include availability of the 1st node)
    response = await client_with_repairs_cube.post(
        "/nodes/default.test_transform_two/materialization/",
        json={
            "job": "spark_sql",
            "strategy": "full",
            "config": {},
            "schedule": "0 * * * *",
        },
    )
    assert response.status_code in (200, 201)

    # check the materialization query again (query should be the same)
    response = await client_with_repairs_cube.get(
        "/nodes/default.test_transform_two/materializations/",
    )
    assert response.status_code in (200, 201)
    assert response.json()[0]["config"]["query"] != query_two
    assert (
        "FROM accounting.test_transform_materialized "
        in response.json()[0]["config"]["query"]
    )
