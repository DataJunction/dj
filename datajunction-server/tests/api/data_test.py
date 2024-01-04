# pylint: disable=too-many-lines
"""
Tests for the data API.
"""
# pylint: disable=too-many-lines,C0302
from typing import Dict, List, Optional
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from datajunction_server.database.node import Node
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access
from datajunction_server.models.node import AvailabilityStateBase


class TestDataForNode:
    """
    Test ``POST /data/{node_name}/``.
    """

    def test_get_dimension_data_failed(
        self,
        client_with_account_revenue: TestClient,
    ) -> None:
        """
        Test trying to get dimensions data while setting dimensions
        """
        response = client_with_account_revenue.get(
            "/data/default.payment_type/",
            params={
                "dimensions": ["something"],
                "filters": [],
            },
        )
        data = response.json()
        assert response.status_code == 500
        assert "Cannot resolve type of column something" in data["message"]

    def test_get_dimension_data(
        self,
        client_with_query_service_example_loader: TestClient,
    ) -> None:
        """
        Test trying to get dimensions data while setting dimensions
        """
        custom_client = client_with_query_service_example_loader(["ACCOUNT_REVENUE"])
        response = custom_client.get(
            "/data/default.payment_type/",
        )
        data = response.json()
        assert response.status_code == 200
        assert data == {
            "engine_name": None,
            "engine_version": None,
            "errors": [],
            "executed_query": None,
            "finished": None,
            "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            "links": None,
            "next": None,
            "output_table": None,
            "previous": None,
            "progress": 0.0,
            "results": [
                {
                    "columns": [
                        {
                            "column": "id",
                            "name": "default_DOT_payment_type_DOT_id",
                            "node": "default.payment_type",
                            "semantic_type": None,
                            "semantic_entity": "default.payment_type.id",
                            "type": "int",
                        },
                        {
                            "column": "payment_type_name",
                            "name": "default_DOT_payment_type_DOT_payment_type_name",
                            "node": "default.payment_type",
                            "semantic_type": None,
                            "semantic_entity": "default.payment_type.payment_type_name",
                            "type": "string",
                        },
                        {
                            "column": "payment_type_classification",
                            "name": "default_DOT_payment_type_DOT_payment_type_classification",
                            "node": "default.payment_type",
                            "semantic_type": None,
                            "semantic_entity": "default.payment_type.payment_type_classification",
                            "type": "string",
                        },
                    ],
                    "row_count": 0,
                    "rows": [[1, "VISA", "CARD"], [2, "MASTERCARD", "CARD"]],
                    "sql": mock.ANY,
                },
            ],
            "scheduled": None,
            "started": None,
            "state": "FINISHED",
            "submitted_query": mock.ANY,
        }

    def test_get_source_data(
        self,
        client_with_query_service_example_loader: TestClient,
    ) -> None:
        """
        Test retrieving data for a source node
        """
        custom_client = client_with_query_service_example_loader(["ACCOUNT_REVENUE"])
        response = custom_client.get("/data/default.revenue/")
        data = response.json()
        assert response.status_code == 200
        assert data == {
            "engine_name": None,
            "engine_version": None,
            "errors": [],
            "executed_query": None,
            "finished": None,
            "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            "links": None,
            "next": None,
            "output_table": None,
            "previous": None,
            "progress": 0.0,
            "results": [
                {
                    "columns": [
                        {
                            "column": "payment_id",
                            "name": "default_DOT_revenue_DOT_payment_id",
                            "node": "default.revenue",
                            "semantic_entity": "default.revenue.payment_id",
                            "semantic_type": None,
                            "type": "int",
                        },
                        {
                            "column": "payment_amount",
                            "name": "default_DOT_revenue_DOT_payment_amount",
                            "node": "default.revenue",
                            "semantic_entity": "default.revenue.payment_amount",
                            "semantic_type": None,
                            "type": "float",
                        },
                        {
                            "column": "payment_type",
                            "name": "default_DOT_revenue_DOT_payment_type",
                            "node": "default.revenue",
                            "semantic_entity": "default.revenue.payment_type",
                            "semantic_type": None,
                            "type": "int",
                        },
                        {
                            "column": "customer_id",
                            "name": "default_DOT_revenue_DOT_customer_id",
                            "node": "default.revenue",
                            "semantic_entity": "default.revenue.customer_id",
                            "semantic_type": None,
                            "type": "int",
                        },
                        {
                            "column": "account_type",
                            "name": "default_DOT_revenue_DOT_account_type",
                            "node": "default.revenue",
                            "semantic_entity": "default.revenue.account_type",
                            "semantic_type": None,
                            "type": "string",
                        },
                    ],
                    "row_count": 0,
                    "rows": [
                        [1, 25.5, 1, 2, "ACTIVE"],
                        [2, 12.5, 2, 2, "INACTIVE"],
                        [3, 89.0, 1, 3, "ACTIVE"],
                        [4, 1293.199951171875, 2, 2, "ACTIVE"],
                        [5, 23.0, 1, 4, "INACTIVE"],
                        [6, 398.1300048828125, 2, 3, "ACTIVE"],
                        [7, 239.6999969482422, 2, 4, "ACTIVE"],
                    ],
                    "sql": "SELECT  payment_id default_DOT_revenue_DOT_payment_id,\n"
                    "\tpayment_amount "
                    "default_DOT_revenue_DOT_payment_amount,\n"
                    "\tpayment_type default_DOT_revenue_DOT_payment_type,\n"
                    "\tcustomer_id default_DOT_revenue_DOT_customer_id,\n"
                    "\taccount_type default_DOT_revenue_DOT_account_type \n"
                    " FROM accounting.revenue\n",
                },
            ],
            "scheduled": None,
            "started": None,
            "state": "FINISHED",
            "submitted_query": "SELECT  payment_id default_DOT_revenue_DOT_payment_id,\n"
            "\tpayment_amount default_DOT_revenue_DOT_payment_amount,\n"
            "\tpayment_type default_DOT_revenue_DOT_payment_type,\n"
            "\tcustomer_id default_DOT_revenue_DOT_customer_id,\n"
            "\taccount_type default_DOT_revenue_DOT_account_type \n"
            " FROM accounting.revenue\n",
        }

    def test_get_transform_data(
        self,
        client_with_query_service_example_loader: TestClient,
    ) -> None:
        """
        Test retrieving data for a transform node
        """
        custom_client = client_with_query_service_example_loader(["ROADS"])
        response = custom_client.get(
            "/data/default.repair_orders_fact/?limit=2",
        )
        data = response.json()
        assert response.status_code == 200
        assert data == {
            "engine_name": None,
            "engine_version": None,
            "errors": [],
            "executed_query": None,
            "finished": None,
            "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            "links": None,
            "next": None,
            "output_table": None,
            "previous": None,
            "progress": 0.0,
            "results": [
                {
                    "columns": [
                        {
                            "column": "repair_order_id",
                            "name": "default_DOT_repair_orders_fact_DOT_repair_order_id",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.repair_order_id",
                            "semantic_type": None,
                            "type": "int",
                        },
                        {
                            "column": "municipality_id",
                            "name": "default_DOT_repair_orders_fact_DOT_municipality_id",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.municipality_id",
                            "semantic_type": None,
                            "type": "string",
                        },
                        {
                            "column": "hard_hat_id",
                            "name": "default_DOT_repair_orders_fact_DOT_hard_hat_id",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.hard_hat_id",
                            "semantic_type": None,
                            "type": "int",
                        },
                        {
                            "column": "dispatcher_id",
                            "name": "default_DOT_repair_orders_fact_DOT_dispatcher_id",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.dispatcher_id",
                            "semantic_type": None,
                            "type": "int",
                        },
                        {
                            "column": "order_date",
                            "name": "default_DOT_repair_orders_fact_DOT_order_date",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.order_date",
                            "semantic_type": None,
                            "type": "timestamp",
                        },
                        {
                            "column": "dispatched_date",
                            "name": "default_DOT_repair_orders_fact_DOT_dispatched_date",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.dispatched_date",
                            "semantic_type": None,
                            "type": "timestamp",
                        },
                        {
                            "column": "required_date",
                            "name": "default_DOT_repair_orders_fact_DOT_required_date",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.required_date",
                            "semantic_type": None,
                            "type": "timestamp",
                        },
                        {
                            "column": "discount",
                            "name": "default_DOT_repair_orders_fact_DOT_discount",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.discount",
                            "semantic_type": None,
                            "type": "float",
                        },
                        {
                            "column": "price",
                            "name": "default_DOT_repair_orders_fact_DOT_price",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.price",
                            "semantic_type": None,
                            "type": "float",
                        },
                        {
                            "column": "quantity",
                            "name": "default_DOT_repair_orders_fact_DOT_quantity",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.quantity",
                            "semantic_type": None,
                            "type": "int",
                        },
                        {
                            "column": "repair_type_id",
                            "name": "default_DOT_repair_orders_fact_DOT_repair_type_id",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.repair_type_id",
                            "semantic_type": None,
                            "type": "int",
                        },
                        {
                            "column": "total_repair_cost",
                            "name": "default_DOT_repair_orders_fact_DOT_total_repair_cost",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.total_repair_cost",
                            "semantic_type": None,
                            "type": "float",
                        },
                        {
                            "column": "time_to_dispatch",
                            "name": "default_DOT_repair_orders_fact_DOT_time_to_dispatch",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.time_to_dispatch",
                            "semantic_type": None,
                            "type": "timestamp",
                        },
                        {
                            "column": "dispatch_delay",
                            "name": "default_DOT_repair_orders_fact_DOT_dispatch_delay",
                            "node": "default.repair_orders_fact",
                            "semantic_entity": "default.repair_orders_fact.dispatch_delay",
                            "semantic_type": None,
                            "type": "timestamp",
                        },
                    ],
                    "row_count": 0,
                    "rows": [
                        [
                            10001,
                            "New York",
                            1,
                            3,
                            "2007-07-04",
                            "2007-12-01",
                            "2009-07-18",
                            0.05000000074505806,
                            63708.0,
                            1,
                            1,
                            63708.0,
                            150,
                            -595,
                        ],
                        [
                            10002,
                            "New York",
                            3,
                            1,
                            "2007-07-05",
                            "2007-12-01",
                            "2009-08-28",
                            0.05000000074505806,
                            67253.0,
                            1,
                            4,
                            67253.0,
                            149,
                            -636,
                        ],
                    ],
                    "sql": mock.ANY,
                },
            ],
            "scheduled": None,
            "started": None,
            "state": "FINISHED",
            "submitted_query": mock.ANY,
        }

    def test_get_metric_data(
        self,
        client_with_query_service_example_loader: TestClient,
    ) -> None:
        """
        Test retrieving data for a metric
        """
        custom_client = client_with_query_service_example_loader(["ROADS"])

        response = custom_client.get("/data/default.num_repair_orders/")
        data = response.json()
        assert response.status_code == 200
        assert data == {
            "engine_name": None,
            "engine_version": None,
            "errors": [],
            "executed_query": None,
            "finished": None,
            "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            "links": None,
            "next": None,
            "output_table": None,
            "previous": None,
            "progress": 0.0,
            "results": [
                {
                    "columns": [
                        {
                            "column": "default_DOT_num_repair_orders",
                            "name": "default_DOT_num_repair_orders",
                            "node": "default.num_repair_orders",
                            "semantic_entity": (
                                "default.num_repair_orders.default_DOT_num_repair_orders"
                            ),
                            "semantic_type": None,
                            "type": "bigint",
                        },
                    ],
                    "row_count": 0,
                    "rows": [[25]],
                    "sql": mock.ANY,
                },
            ],
            "scheduled": None,
            "started": None,
            "state": "FINISHED",
            "submitted_query": mock.ANY,
        }

    def test_get_metric_data_unauthorized(
        self,
        client_with_query_service_example_loader: TestClient,
    ) -> None:
        """
        Test retrieving data for a metric
        """
        custom_client = client_with_query_service_example_loader(["BASIC"])

        def validate_access_override():
            def _validate_access(access_control: access.AccessControl):
                access_control.deny_all()

            return _validate_access

        app = custom_client.app
        app.dependency_overrides[validate_access] = validate_access_override

        response = custom_client.get("/data/basic.num_comments/")
        app.dependency_overrides.clear()
        data = response.json()
        assert data["message"] == (
            "Authorization of User `dj` for this request failed."
            "\nThe following requests were denied:\nexecute:node/basic.num_comments."
        )
        assert response.status_code == 403

    def test_get_multiple_metrics_and_dimensions_data(
        self,
        client_with_query_service_example_loader: TestClient,
    ) -> None:
        """
        Test getting multiple metrics and dimensions
        """
        custom_client = client_with_query_service_example_loader(["ROADS"])
        response = custom_client.get(
            "/data?metrics=default.num_repair_orders&metrics="
            "default.avg_repair_price&dimensions=default.dispatcher.company_name&limit=10",
        )
        data = response.json()
        assert response.status_code == 200
        assert data == {
            "engine_name": None,
            "engine_version": None,
            "errors": [],
            "executed_query": None,
            "finished": None,
            "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            "links": None,
            "next": None,
            "output_table": None,
            "previous": None,
            "progress": 0.0,
            "results": [
                {
                    "columns": [
                        {
                            "column": "default_DOT_num_repair_orders",
                            "name": "default_DOT_num_repair_orders",
                            "node": "default.num_repair_orders",
                            "semantic_entity": (
                                "default.num_repair_orders.default_DOT_num_repair_orders"
                            ),
                            "semantic_type": "metric",
                            "type": "bigint",
                        },
                        {
                            "column": "default_DOT_avg_repair_price",
                            "name": "default_DOT_avg_repair_price",
                            "node": "default.avg_repair_price",
                            "semantic_entity": (
                                "default.avg_repair_price.default_DOT_avg_repair_price"
                            ),
                            "semantic_type": "metric",
                            "type": "double",
                        },
                        {
                            "column": "company_name",
                            "name": "default_DOT_dispatcher_DOT_company_name",
                            "node": "default.dispatcher",
                            "semantic_entity": "default.dispatcher.company_name",
                            "semantic_type": "dimension",
                            "type": "string",
                        },
                    ],
                    "row_count": 0,
                    "rows": [
                        [9, 51913.88888888889, "Federal Roads Group"],
                        [8, 62205.875, "Pothole Pete"],
                        [8, 68914.75, "Asphalts R Us"],
                    ],
                    "sql": mock.ANY,
                },
            ],
            "scheduled": None,
            "started": None,
            "state": "FINISHED",
            "submitted_query": mock.ANY,
        }

    def test_stream_multiple_metrics_and_dimensions_data(
        self,
        client_with_query_service_example_loader: TestClient,
    ) -> None:
        """
        Test streaming query status for multiple metrics and dimensions
        """
        custom_client = client_with_query_service_example_loader(["ROADS"])
        response = custom_client.get(
            "/stream?metrics=default.num_repair_orders&metrics="
            "default.avg_repair_price&dimensions=default.dispatcher.company_name&limit=10",
            headers={
                "Accept": "text/event-stream",
            },
            stream=True,
        )
        assert response.status_code == 200

    def test_get_data_for_query_id(
        self,
        client_with_query_service_example_loader: TestClient,
    ) -> None:
        """
        Test retrieving data for a query ID
        """
        # run some query
        custom_client = client_with_query_service_example_loader(["ROADS"])
        response = custom_client.get("/data/default.num_repair_orders/")
        data = response.json()
        assert response.status_code == 200
        assert data["id"] == "bd98d6be-e2d2-413e-94c7-96d9411ddee2"

        # and try to get the results by the query id only
        new_response = custom_client.get(f"/data/query/{data['id']}/")
        new_data = response.json()
        assert new_response.status_code == 200
        assert new_data["results"] == [
            {
                "columns": [
                    {
                        "column": "default_DOT_num_repair_orders",
                        "name": "default_DOT_num_repair_orders",
                        "node": "default.num_repair_orders",
                        "semantic_entity": (
                            "default.num_repair_orders.default_DOT_num_repair_orders"
                        ),
                        "semantic_type": None,
                        "type": "bigint",
                    },
                ],
                "row_count": 0,
                "rows": [[25]],
                "sql": "SELECT  count(default_DOT_repair_orders_fact.repair_order_id) "
                "default_DOT_num_repair_orders \n"
                " FROM (SELECT  default_DOT_repair_orders.repair_order_id,\n"
                "\tdefault_DOT_repair_orders.municipality_id,\n"
                "\tdefault_DOT_repair_orders.hard_hat_id,\n"
                "\tdefault_DOT_repair_orders.dispatcher_id,\n"
                "\tdefault_DOT_repair_orders.order_date,\n"
                "\tdefault_DOT_repair_orders.dispatched_date,\n"
                "\tdefault_DOT_repair_orders.required_date,\n"
                "\tdefault_DOT_repair_order_details.discount,\n"
                "\tdefault_DOT_repair_order_details.price,\n"
                "\tdefault_DOT_repair_order_details.quantity,\n"
                "\tdefault_DOT_repair_order_details.repair_type_id,\n"
                "\tdefault_DOT_repair_order_details.price * "
                "default_DOT_repair_order_details.quantity AS total_repair_cost,\n"
                "\tdefault_DOT_repair_orders.dispatched_date - "
                "default_DOT_repair_orders.order_date AS time_to_dispatch,\n"
                "\tdefault_DOT_repair_orders.dispatched_date - "
                "default_DOT_repair_orders.required_date AS dispatch_delay \n"
                " FROM roads.repair_orders AS default_DOT_repair_orders JOIN "
                "roads.repair_order_details AS default_DOT_repair_order_details ON "
                "default_DOT_repair_orders.repair_order_id = "
                "default_DOT_repair_order_details.repair_order_id)\n"
                " AS default_DOT_repair_orders_fact\n"
                "\n",
            },
        ]

        # and repeat for a bogus query id
        yet_another_response = custom_client.get("/data/query/foo-bar-baz/")
        assert yet_another_response.status_code == 404
        assert "Query foo-bar-baz not found." in yet_another_response.text


class TestAvailabilityState:  # pylint: disable=too-many-public-methods
    """
    Test ``POST /data/{node_name}/availability/``.
    """

    def test_setting_availability_state(
        self,
        session: Session,
        client_with_query_service_example_loader: TestClient,
    ) -> None:
        """
        Test adding an availability state
        """
        custom_client = client_with_query_service_example_loader(["ACCOUNT_REVENUE"])
        response = custom_client.post(
            "/data/default.large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_temporal_partition": ["2023", "01", "25"],
                "min_temporal_partition": ["2022", "01", "01"],
                "url": "http://some.catalog.com/default.accounting.pmts",
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        # Check that the history tracker has been updated
        response = custom_client.get(
            "/history?node=default.large_revenue_payments_and_business_only",
        )
        data = response.json()
        availability_activities = [
            activity for activity in data if activity["entity_type"] == "availability"
        ]
        assert availability_activities == [
            {
                "activity_type": "create",
                "created_at": mock.ANY,
                "details": {},
                "entity_name": None,
                "node": "default.large_revenue_payments_and_business_only",
                "entity_type": "availability",
                "id": mock.ANY,
                "post": {
                    "catalog": "default",
                    "categorical_partitions": [],
                    "max_temporal_partition": ["2023", "01", "25"],
                    "min_temporal_partition": ["2022", "01", "01"],
                    "partitions": [],
                    "schema_": "accounting",
                    "table": "pmts",
                    "temporal_partitions": [],
                    "valid_through_ts": 20230125,
                    "url": "http://some.catalog.com/default.accounting.pmts",
                },
                "pre": {},
                "user": "dj",
            },
        ]

        statement = select(Node).where(
            Node.name == "default.large_revenue_payments_and_business_only",
        )
        large_revenue_payments_and_business_only = session.execute(
            statement,
        ).scalar_one()
        node_dict = AvailabilityStateBase.from_orm(
            large_revenue_payments_and_business_only.current.availability,
        ).dict()
        assert node_dict == {
            "valid_through_ts": 20230125,
            "catalog": "default",
            "min_temporal_partition": ["2022", "01", "01"],
            "table": "pmts",
            "max_temporal_partition": ["2023", "01", "25"],
            "partitions": [],
            "schema_": "accounting",
            "categorical_partitions": [],
            "temporal_partitions": [],
            "url": "http://some.catalog.com/default.accounting.pmts",
        }

    def test_availability_catalog_mismatch(
        self,
        client_with_account_revenue: TestClient,
    ) -> None:
        """
        Test that setting availability works even when the catalogs do not match
        """
        response = client_with_account_revenue.post(
            "/data/default.large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "public",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_temporal_partition": ["2023", "01", "25"],
                "min_temporal_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data["message"] == "Availability state successfully posted"

    def test_setting_availability_state_multiple_times(
        self,
        session: Session,
        client_with_account_revenue: TestClient,
    ) -> None:
        """
        Test adding multiple availability states
        """
        response = client_with_account_revenue.post(
            "/data/default.large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_temporal_partition": ["2023", "01", "25"],
                "min_temporal_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        response = client_with_account_revenue.post(
            "/data/default.large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_temporal_partition": ["2023", "01", "25"],
                "min_temporal_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        response = client_with_account_revenue.post(
            "/data/default.large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "new_accounting",
                "table": "new_payments_table",
                "valid_through_ts": 20230125,
                "max_temporal_partition": ["2023", "01", "25"],
                "min_temporal_partition": ["2022", "01", "01"],
                "categorical_partitions": [],
                "temporal_partitions": [],
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        # Check that the history tracker has been updated
        response = client_with_account_revenue.get(
            "/history/?node=default.large_revenue_payments_and_business_only",
        )
        data = response.json()
        availability_activities = [
            activity for activity in data if activity["entity_type"] == "availability"
        ]
        assert availability_activities == [
            {
                "activity_type": "create",
                "created_at": mock.ANY,
                "details": {},
                "entity_name": None,
                "node": "default.large_revenue_payments_and_business_only",
                "entity_type": "availability",
                "id": mock.ANY,
                "post": {
                    "catalog": "default",
                    "categorical_partitions": [],
                    "max_temporal_partition": ["2023", "01", "25"],
                    "min_temporal_partition": ["2022", "01", "01"],
                    "partitions": [],
                    "schema_": "accounting",
                    "table": "pmts",
                    "temporal_partitions": [],
                    "valid_through_ts": 20230125,
                    "url": None,
                },
                "pre": {},
                "user": "dj",
            },
            {
                "activity_type": "create",
                "created_at": mock.ANY,
                "details": {},
                "entity_name": None,
                "node": "default.large_revenue_payments_and_business_only",
                "entity_type": "availability",
                "id": mock.ANY,
                "post": {
                    "catalog": "default",
                    "categorical_partitions": [],
                    "max_temporal_partition": ["2023", "01", "25"],
                    "min_temporal_partition": ["2022", "01", "01"],
                    "partitions": [],
                    "schema_": "accounting",
                    "table": "pmts",
                    "temporal_partitions": [],
                    "valid_through_ts": 20230125,
                    "url": None,
                },
                "pre": {
                    "catalog": "default",
                    "categorical_partitions": [],
                    "max_temporal_partition": ["2023", "01", "25"],
                    "min_temporal_partition": ["2022", "01", "01"],
                    "partitions": [],
                    "schema_": "accounting",
                    "table": "pmts",
                    "temporal_partitions": [],
                    "valid_through_ts": 20230125,
                    "url": None,
                },
                "user": "dj",
            },
            {
                "activity_type": "create",
                "created_at": mock.ANY,
                "details": {},
                "entity_name": None,
                "node": "default.large_revenue_payments_and_business_only",
                "entity_type": "availability",
                "id": mock.ANY,
                "post": {
                    "catalog": "default",
                    "categorical_partitions": [],
                    "max_temporal_partition": ["2023", "01", "25"],
                    "min_temporal_partition": ["2022", "01", "01"],
                    "partitions": [],
                    "schema_": "new_accounting",
                    "table": "new_payments_table",
                    "temporal_partitions": [],
                    "valid_through_ts": 20230125,
                    "url": None,
                },
                "pre": {
                    "catalog": "default",
                    "categorical_partitions": [],
                    "max_temporal_partition": ["2023", "01", "25"],
                    "min_temporal_partition": ["2022", "01", "01"],
                    "partitions": [],
                    "schema_": "accounting",
                    "table": "pmts",
                    "temporal_partitions": [],
                    "valid_through_ts": 20230125,
                    "url": None,
                },
                "user": "dj",
            },
        ]

        statement = select(Node).where(
            Node.name == "default.large_revenue_payments_and_business_only",
        )
        large_revenue_payments_and_business_only = session.execute(
            statement,
        ).scalar_one()
        node_dict = AvailabilityStateBase.from_orm(
            large_revenue_payments_and_business_only.current.availability,
        ).dict()
        assert node_dict == {
            "valid_through_ts": 20230125,
            "catalog": "default",
            "min_temporal_partition": ["2022", "01", "01"],
            "table": "new_payments_table",
            "max_temporal_partition": ["2023", "01", "25"],
            "partitions": [],
            "schema_": "new_accounting",
            "categorical_partitions": [],
            "temporal_partitions": [],
            "url": None,
        }

    def test_that_update_at_timestamp_is_being_updated(
        self,
        session: Session,
        client_with_account_revenue: TestClient,
    ) -> None:
        """
        Test that the `updated_at` attribute is being updated
        """
        response = client_with_account_revenue.post(
            "/data/default.large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_temporal_partition": ["2023", "01", "25"],
                "min_temporal_partition": ["2022", "01", "01"],
            },
        )
        assert response.status_code == 200
        statement = select(Node).where(
            Node.name == "default.large_revenue_payments_and_business_only",
        )
        large_revenue_payments_and_business_only = session.execute(
            statement,
        ).scalar_one()
        updated_at_1 = (
            large_revenue_payments_and_business_only.current.availability.updated_at
        )

        response = client_with_account_revenue.post(
            "/data/default.large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_temporal_partition": ["2023", "01", "25"],
                "min_temporal_partition": ["2022", "01", "01"],
            },
        )
        assert response.status_code == 200

        session.refresh(large_revenue_payments_and_business_only)
        updated_at_2 = (
            large_revenue_payments_and_business_only.current.availability.updated_at
        )

        assert updated_at_2 > updated_at_1

    def test_raising_when_node_does_not_exist(
        self,
        client_with_service_setup: TestClient,
    ) -> None:
        """
        Test raising when setting availability state on non-existent node
        """
        response = client_with_service_setup.post(
            "/data/default.nonexistentnode/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_temporal_partition": ["2023", "01", "25"],
                "min_temporal_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 404
        assert data == {
            "message": "A node with name `default.nonexistentnode` does not exist.",
            "errors": [],
            "warnings": [],
        }

    def test_merging_in_a_higher_max_partition(
        self,
        session: Session,
        client_with_account_revenue: TestClient,
    ) -> None:
        """
        Test that the higher max_partition value is used when merging in an availability state
        """
        client_with_account_revenue.post(
            "/data/default.large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230101,
                "max_temporal_partition": ["2023", "01", "01"],
                "min_temporal_partition": ["2022", "01", "01"],
            },
        )
        response = client_with_account_revenue.post(
            "/data/default.large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230102,
                "max_temporal_partition": [
                    "2023",
                    "01",
                    "02",
                ],  # should be used since it's a higher max_temporal_partition
                "min_temporal_partition": [
                    "2023",
                    "01",
                    "02",
                ],  # should be ignored since it's a higher min_temporal_partition
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        statement = select(Node).where(
            Node.name == "default.large_revenue_payments_only",
        )
        large_revenue_payments_only = session.execute(statement).scalar_one()
        node_dict = AvailabilityStateBase.from_orm(
            large_revenue_payments_only.current.availability,
        ).dict()
        assert node_dict == {
            "valid_through_ts": 20230102,
            "catalog": "default",
            "min_temporal_partition": ["2022", "01", "01"],
            "table": "large_pmts",
            "max_temporal_partition": ["2023", "01", "02"],
            "schema_": "accounting",
            "partitions": [],
            "categorical_partitions": [],
            "temporal_partitions": [],
            "url": None,
        }

    @pytest.fixture
    def post_local_hard_hats_availability(self, client_with_roads: TestClient):
        """
        Fixture for posting availability for local_hard_hats
        """

        def _post(
            min_temporal_partition: Optional[List[str]] = None,
            max_temporal_partition: Optional[List[str]] = None,
            partitions: List[Dict] = None,
            categorical_partitions: List[str] = None,
        ):
            if categorical_partitions is None:
                categorical_partitions = ["country", "postal_code"]
            return client_with_roads.post(
                "/data/default.local_hard_hats/availability/",
                json={
                    "catalog": "default",
                    "schema_": "dimensions",
                    "table": "local_hard_hats",
                    "valid_through_ts": 20230101,
                    "categorical_partitions": categorical_partitions,
                    "temporal_partitions": ["birth_date"],
                    "min_temporal_partition": min_temporal_partition,
                    "max_temporal_partition": max_temporal_partition,
                    "partitions": partitions,
                },
            )

        return _post

    def test_set_temporal_only_availability(
        self,
        client_with_roads: TestClient,
        post_local_hard_hats_availability,
    ):
        """
        Test setting availability on a node where it only has temporal partitions and
        no categorical partitions.
        """
        post_local_hard_hats_availability(
            min_temporal_partition=["20230101"],
            max_temporal_partition=["20230105"],
            partitions=[],
            categorical_partitions=[],
        )
        post_local_hard_hats_availability(
            min_temporal_partition=["20230101"],
            max_temporal_partition=["20230110"],
            partitions=[],
            categorical_partitions=[],
        )

        response = client_with_roads.get(
            "/nodes/default.local_hard_hats/",
        )
        assert response.json()["availability"] == {
            "catalog": "default",
            "min_temporal_partition": ["20230101"],
            "max_temporal_partition": ["20230110"],
            "categorical_partitions": [],
            "temporal_partitions": ["birth_date"],
            "partitions": [],
            "schema_": "dimensions",
            "table": "local_hard_hats",
            "valid_through_ts": 20230101,
            "url": None,
        }

    def test_set_node_level_availability_wider_time_range(
        self,
        client_with_roads: TestClient,
        post_local_hard_hats_availability,
    ):
        """
        The node starts off with partition-level availability with a specific time range.
        We add in a node-level availability with a wider time range than at the partition
        level. We expect this new availability state to overwrite the partition-level
        availability.
        """
        # Set initial availability state
        post_local_hard_hats_availability(
            partitions=[
                {
                    "value": ["DE", "ABC123D"],
                    "min_temporal_partition": ["20230101"],
                    "max_temporal_partition": ["20230105"],
                    "valid_through_ts": 20230101,
                },
            ],
        )
        # Post wider availability
        post_local_hard_hats_availability(
            min_temporal_partition=["20230101"],
            max_temporal_partition=["20230110"],
            partitions=[],
        )

        response = client_with_roads.get(
            "/nodes/default.local_hard_hats/",
        )
        assert response.json()["availability"] == {
            "catalog": "default",
            "min_temporal_partition": ["20230101"],
            "max_temporal_partition": ["20230110"],
            "categorical_partitions": ["country", "postal_code"],
            "temporal_partitions": ["birth_date"],
            "partitions": [],
            "schema_": "dimensions",
            "table": "local_hard_hats",
            "valid_through_ts": 20230101,
            "url": None,
        }

    def test_set_node_level_availability_smaller_time_range(
        self,
        client_with_roads: TestClient,
        post_local_hard_hats_availability,
    ):
        """
        Set a node level availability with a smaller time range than the existing
        one will result in no change to the merged availability state
        """
        post_local_hard_hats_availability(
            min_temporal_partition=["20230101"],
            max_temporal_partition=["20230110"],
            partitions=[],
        )

        post_local_hard_hats_availability(
            min_temporal_partition=["20230103"],
            max_temporal_partition=["20230105"],
            partitions=[],
        )

        response = client_with_roads.get(
            "/nodes/default.local_hard_hats/",
        )
        availability = response.json()["availability"]
        assert availability["min_temporal_partition"] == ["20230101"]
        assert availability["max_temporal_partition"] == ["20230110"]
        assert availability["partitions"] == []

    def test_set_partition_level_availability_smaller_time_range(
        self,
        client_with_roads: TestClient,
        post_local_hard_hats_availability,
    ):
        """
        Set a partition-level availability with a smaller time range than
        the existing node-level time range will result in no change to the
        merged availability state
        """
        post_local_hard_hats_availability(
            min_temporal_partition=["20230101"],
            max_temporal_partition=["20230110"],
            partitions=[],
        )

        post_local_hard_hats_availability(
            partitions=[
                {
                    "value": ["DE", None],
                    "min_temporal_partition": ["20230102"],
                    "max_temporal_partition": ["20230107"],
                    "valid_through_ts": 20230101,
                },
            ],
        )

        response = client_with_roads.get(
            "/nodes/default.local_hard_hats/",
        )
        availability = response.json()["availability"]
        assert availability["min_temporal_partition"] == ["20230101"]
        assert availability["max_temporal_partition"] == ["20230110"]
        assert availability["partitions"] == []

    def test_set_partition_level_availability_larger_time_range(
        self,
        client_with_roads: TestClient,
        post_local_hard_hats_availability,
    ):
        """
        Set a partition-level availability with a larger time range than
        the existing node-level time range will result in the partition with
        the larger range being recorded
        """
        post_local_hard_hats_availability(
            min_temporal_partition=["20230101"],
            max_temporal_partition=["20230110"],
            partitions=[],
        )

        post_local_hard_hats_availability(
            partitions=[
                {
                    "value": ["DE", None],
                    "min_temporal_partition": ["20230102"],
                    "max_temporal_partition": ["20230115"],
                    "valid_through_ts": 20230101,
                },
            ],
        )

        response = client_with_roads.get(
            "/nodes/default.local_hard_hats/",
        )
        availability = response.json()["availability"]
        assert availability["min_temporal_partition"] == ["20230101"]
        assert availability["max_temporal_partition"] == ["20230110"]
        assert availability["partitions"] == [
            {
                "value": ["DE", None],
                "min_temporal_partition": ["20230102"],
                "max_temporal_partition": ["20230115"],
                "valid_through_ts": 20230101,
            },
        ]

    def test_set_orthogonal_partition_level_availability(
        self,
        client_with_roads: TestClient,
        post_local_hard_hats_availability,
    ):
        """
        Test setting an orthogonal partition-level availability.
        """
        post_local_hard_hats_availability(
            min_temporal_partition=["20230101"],
            max_temporal_partition=["20230110"],
            partitions=[
                {
                    "value": ["DE", None],
                    "min_temporal_partition": ["20230102"],
                    "max_temporal_partition": ["20230115"],
                    "valid_through_ts": 20230101,
                },
            ],
        )

        post_local_hard_hats_availability(
            partitions=[
                {
                    "value": ["MY", None],
                    "min_temporal_partition": ["20230102"],
                    "max_temporal_partition": ["20230115"],
                    "valid_through_ts": 20230101,
                },
            ],
        )

        response = client_with_roads.get(
            "/nodes/default.local_hard_hats/",
        )
        availability = response.json()["availability"]
        assert availability["min_temporal_partition"] == ["20230101"]
        assert availability["max_temporal_partition"] == ["20230110"]
        assert availability["partitions"] == [
            {
                "value": ["DE", None],
                "min_temporal_partition": ["20230102"],
                "max_temporal_partition": ["20230115"],
                "valid_through_ts": 20230101,
            },
            {
                "value": ["MY", None],
                "min_temporal_partition": ["20230102"],
                "max_temporal_partition": ["20230115"],
                "valid_through_ts": 20230101,
            },
        ]

    def test_set_overlap_partition_level_availability(
        self,
        client_with_roads: TestClient,
        post_local_hard_hats_availability,
    ):
        """
        Test setting an overlapping partition-level availability.
        """
        post_local_hard_hats_availability(
            min_temporal_partition=["20230101"],
            max_temporal_partition=["20230110"],
            partitions=[
                {
                    "value": ["DE", None],
                    "min_temporal_partition": ["20230102"],
                    "max_temporal_partition": ["20230115"],
                    "valid_through_ts": 20230101,
                },
            ],
        )

        post_local_hard_hats_availability(
            partitions=[
                {
                    "value": ["DE", None],
                    "min_temporal_partition": ["20230105"],
                    "max_temporal_partition": ["20230215"],
                    "valid_through_ts": 20230101,
                },
            ],
        )

        response = client_with_roads.get(
            "/nodes/default.local_hard_hats/",
        )
        availability = response.json()["availability"]
        assert availability["min_temporal_partition"] == ["20230101"]
        assert availability["max_temporal_partition"] == ["20230110"]
        assert availability["partitions"] == [
            {
                "value": ["DE", None],
                "min_temporal_partition": ["20230102"],
                "max_temporal_partition": ["20230215"],
                "valid_through_ts": 20230101,
            },
        ]

    def test_set_semioverlap_partition_level_availability(
        self,
        client_with_roads: TestClient,
        post_local_hard_hats_availability,
    ):
        """
        Test setting a semi-overlapping partition-level availability.
        """
        post_local_hard_hats_availability(
            min_temporal_partition=["20230101"],
            max_temporal_partition=["20230110"],
            partitions=[
                {
                    "value": ["DE", None],
                    "min_temporal_partition": ["20230102"],
                    "max_temporal_partition": ["20230115"],
                    "valid_through_ts": 20230101,
                },
                {
                    "value": ["DE", "abc-def"],
                    "min_temporal_partition": ["20230202"],
                    "max_temporal_partition": ["20230215"],
                    "valid_through_ts": 20230101,
                },
            ],
        )

        post_local_hard_hats_availability(
            partitions=[
                {
                    "value": ["DE", None],
                    "min_temporal_partition": ["20230102"],
                    "max_temporal_partition": ["20230115"],
                    "valid_through_ts": 20230101,
                },
                {
                    "value": ["DE", "abc-def"],
                    "min_temporal_partition": ["20230102"],
                    "max_temporal_partition": ["20230215"],
                    "valid_through_ts": 20230101,
                },
            ],
        )

        response = client_with_roads.get(
            "/nodes/default.local_hard_hats/",
        )
        availability = response.json()["availability"]
        assert availability["min_temporal_partition"] == ["20230101"]
        assert availability["max_temporal_partition"] == ["20230110"]
        assert availability["partitions"] == [
            {
                "value": ["DE", "abc-def"],
                "min_temporal_partition": ["20230102"],
                "max_temporal_partition": ["20230215"],
                "valid_through_ts": 20230101,
            },
            {
                "value": ["DE", None],
                "min_temporal_partition": ["20230102"],
                "max_temporal_partition": ["20230115"],
                "valid_through_ts": 20230101,
            },
        ]

    def test_merging_in_a_lower_min_partition(
        self,
        session: Session,
        client_with_account_revenue: TestClient,
    ) -> None:
        """
        Test that the lower min_partition value is used when merging in an availability state
        """
        client_with_account_revenue.post(
            "/data/default.large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230101,
                "max_temporal_partition": ["2023", "01", "01"],
                "min_temporal_partition": ["2022", "01", "01"],
            },
        )
        response = client_with_account_revenue.post(
            "/data/default.large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230101,
                "max_temporal_partition": [
                    "2021",
                    "12",
                    "31",
                ],  # should be ignored since it's a lower max_temporal_partition
                "min_temporal_partition": [
                    "2021",
                    "12",
                    "31",
                ],  # should be used since it's a lower min_partition
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        statement = select(Node).where(
            Node.name == "default.large_revenue_payments_only",
        )
        large_revenue_payments_only = session.execute(statement).scalar_one()
        node_dict = AvailabilityStateBase.from_orm(
            large_revenue_payments_only.current.availability,
        ).dict()
        assert node_dict == {
            "valid_through_ts": 20230101,
            "catalog": "default",
            "min_temporal_partition": ["2021", "12", "31"],
            "categorical_partitions": [],
            "temporal_partitions": [],
            "table": "large_pmts",
            "max_temporal_partition": ["2023", "01", "01"],
            "schema_": "accounting",
            "partitions": [],
            "url": None,
        }

    def test_moving_back_valid_through_ts(
        self,
        session: Session,
        client_with_account_revenue: TestClient,
    ) -> None:
        """
        Test that the valid through timestamp can be moved backwards
        """
        client_with_account_revenue.post(
            "/data/default.large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230101,
                "max_temporal_partition": ["2023", "01", "01"],
                "min_temporal_partition": ["2022", "01", "01"],
            },
        )
        response = client_with_account_revenue.post(
            "/data/default.large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20221231,
                "max_temporal_partition": [
                    "2023",
                    "01",
                    "01",
                ],  # should be ignored since it's a lower max_temporal_partition
                "min_temporal_partition": [
                    "2022",
                    "01",
                    "01",
                ],  # should be used since it's a lower min_temporal_partition
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        statement = select(Node).where(
            Node.name == "default.large_revenue_payments_only",
        )
        large_revenue_payments_only = session.execute(statement).scalar_one()
        node_dict = AvailabilityStateBase.from_orm(
            large_revenue_payments_only.current.availability,
        ).dict()
        assert node_dict == {
            "valid_through_ts": 20230101,
            "catalog": "default",
            "min_temporal_partition": ["2022", "01", "01"],
            "table": "large_pmts",
            "max_temporal_partition": ["2023", "01", "01"],
            "schema_": "accounting",
            "partitions": [],
            "categorical_partitions": [],
            "temporal_partitions": [],
            "url": None,
        }

    def test_setting_availablity_state_on_a_source_node(
        self,
        session: Session,
        client_with_account_revenue: TestClient,
    ) -> None:
        """
        Test setting the availability state on a source node
        """
        response = client_with_account_revenue.post(
            "/data/default.revenue/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "revenue",
                "valid_through_ts": 20230101,
                "max_temporal_partition": ["2023", "01", "01"],
                "min_temporal_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        statement = select(Node).where(
            Node.name == "default.revenue",
        )
        revenue = session.execute(statement).scalar_one()
        node_dict = AvailabilityStateBase.from_orm(revenue.current.availability).dict()
        assert node_dict == {
            "valid_through_ts": 20230101,
            "catalog": "default",
            "min_temporal_partition": ["2022", "01", "01"],
            "table": "revenue",
            "max_temporal_partition": ["2023", "01", "01"],
            "schema_": "accounting",
            "partitions": [],
            "categorical_partitions": [],
            "temporal_partitions": [],
            "url": None,
        }

    def test_raise_on_setting_invalid_availability_state_on_a_source_node(
        self,
        client_with_account_revenue: TestClient,
    ) -> None:
        """
        Test raising availability state doesn't match existing source node table
        """
        response = client_with_account_revenue.post(
            "/data/default.revenue/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230101,
                "max_temporal_partition": ["2023", "01", "01"],
                "min_temporal_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 500
        assert data == {
            "message": (
                "Cannot set availability state, source nodes require availability states "
                "to match the set table: default.accounting.large_pmts does not match "
                "default.accounting.revenue "
            ),
            "errors": [],
            "warnings": [],
        }
