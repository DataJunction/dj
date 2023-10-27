# pylint: disable=too-many-lines
"""
Tests for the data API.
"""
# pylint: disable=too-many-lines,C0302
from typing import Dict, List, Optional
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access
from datajunction_server.models.node import Node


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
            "id": "0cb5478c-fd7d-4159-a414-68c50f4b9914",
            "engine_name": None,
            "engine_version": None,
            "submitted_query": (
                "SELECT  payment_type_table.id,\n\t"
                "payment_type_table.payment_type_classification,"
                "\n\tpayment_type_table.payment_type_name \n FROM "
                '"accounting"."payment_type_table" '
                "AS payment_type_table"
            ),
            "executed_query": None,
            "scheduled": None,
            "started": None,
            "finished": None,
            "state": "FINISHED",
            "progress": 0.0,
            "output_table": None,
            "results": [
                {
                    "sql": "",
                    "columns": [
                        {
                            "column": "id",
                            "name": "default_DOT_payment_type_DOT_id",
                            "node": "default.payment_type",
                            "type": "int",
                        },
                        {
                            "column": "payment_type_classification",
                            "name": "default_DOT_payment_type_DOT_payment_type_classification",
                            "node": "default.payment_type",
                            "type": "string",
                        },
                        {
                            "column": "payment_type_name",
                            "name": "default_DOT_payment_type_DOT_payment_type_name",
                            "node": "default.payment_type",
                            "type": "string",
                        },
                    ],
                    "rows": [[1, "CARD", "VISA"], [2, "CARD", "MASTERCARD"]],
                    "row_count": 0,
                },
            ],
            "next": None,
            "previous": None,
            "errors": [],
            "links": None,
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
            "id": "8a8bb03a-74c8-448a-8630-e9439bd5a01b",
            "engine_name": None,
            "engine_version": None,
            "submitted_query": 'SELECT  * \n FROM "accounting"."revenue"',
            "executed_query": None,
            "scheduled": None,
            "started": None,
            "finished": None,
            "state": "FINISHED",
            "progress": 0.0,
            "output_table": None,
            "results": [
                {
                    "sql": "",
                    "columns": [
                        {"name": "*", "type": "wildcard", "node": "", "column": "*"},
                    ],
                    "rows": [[129.19]],
                    "row_count": 0,
                },
            ],
            "next": None,
            "previous": None,
            "errors": [],
            "links": None,
        }

    def test_get_transform_data(
        self,
        client_with_query_service_example_loader: TestClient,
    ) -> None:
        """
        Test retrieving data for a transform node
        """
        custom_client = client_with_query_service_example_loader(["ACCOUNT_REVENUE"])
        response = custom_client.get(
            "/data/default.large_revenue_payments_only/",
        )
        data = response.json()
        assert response.status_code == 200
        assert data == {
            "id": "1b049fb1-652e-458a-ba9d-3669412b34bd",
            "engine_name": None,
            "engine_version": None,
            "submitted_query": mock.ANY,
            "executed_query": None,
            "scheduled": None,
            "started": None,
            "finished": None,
            "state": "FINISHED",
            "progress": 0.0,
            "output_table": None,
            "results": [
                {
                    "sql": "",
                    "columns": [
                        {
                            "column": "account_type",
                            "name": "default_DOT_large_revenue_payments_only_DOT_account_type",
                            "node": "default.large_revenue_payments_only",
                            "type": "string",
                        },
                        {
                            "column": "customer_id",
                            "name": "default_DOT_large_revenue_payments_only_DOT_customer_id",
                            "node": "default.large_revenue_payments_only",
                            "type": "int",
                        },
                        {
                            "column": "payment_amount",
                            "name": "default_DOT_large_revenue_payments_only_DOT_payment_amount",
                            "node": "default.large_revenue_payments_only",
                            "type": "float",
                        },
                        {
                            "column": "payment_id",
                            "name": "default_DOT_large_revenue_payments_only_DOT_payment_id",
                            "node": "default.large_revenue_payments_only",
                            "type": "int",
                        },
                    ],
                    "rows": [
                        ["CHECKING", 2, "22.50", 1],
                        ["SAVINGS", 2, "100.50", 1],
                        ["CREDIT", 1, "11.50", 1],
                        ["CHECKING", 2, "2.50", 1],
                    ],
                    "row_count": 0,
                },
            ],
            "next": None,
            "previous": None,
            "errors": [],
            "links": None,
        }

    def test_get_metric_data(
        self,
        client_with_query_service_example_loader: TestClient,
    ) -> None:
        """
        Test retrieving data for a metric
        """
        custom_client = client_with_query_service_example_loader(["BASIC"])

        response = custom_client.get("/data/basic.num_comments/")
        data = response.json()
        assert response.status_code == 200
        assert data == {
            "id": "ee41ea6c-2303-4fe1-8bf0-f0ce3d6a35ca",
            "engine_name": None,
            "engine_version": None,
            "submitted_query": (
                'SELECT  COUNT(1) basic_DOT_num_comments \n FROM "basic"."comments" '
                "AS basic_DOT_source_DOT_comments"
            ),
            "executed_query": None,
            "scheduled": None,
            "started": None,
            "finished": None,
            "state": "FINISHED",
            "progress": 0.0,
            "output_table": None,
            "results": [
                {
                    "sql": "",
                    "columns": [
                        {
                            "column": "basic_DOT_num_comments",
                            "name": "basic_DOT_num_comments",
                            "node": "basic.num_comments",
                            "type": "bigint",
                        },
                    ],
                    "rows": [[1]],
                    "row_count": 0,
                },
            ],
            "next": None,
            "previous": None,
            "errors": [],
            "links": None,
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
            "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            "engine_name": None,
            "engine_version": None,
            "submitted_query": (
                "SELECT  avg(repair_order_details.price) AS default_DOT_avg_repair_price,"
                "\\n\\tdispatcher.company_name,\\n\\tcount(repair_orders.repair_order_id) "
                "AS default_DOT_num_repair_ordersdefault_DOT_num_repair_orders \\n FROM "
                "roads.repair_order_details AS repair_order_details LEFT OUTER JOIN (SELECT  "
                "repair_orders.dispatcher_id,\\n\\trepair_orders.hard_hat_id,\\n\\t"
                "repair_orders.municipality_id,\\n\\trepair_orders.repair_order_id "
                "\\n FROM roads.repair_orders AS repair_orders) AS repair_order ON "
                "repair_order_details.repair_order_id = repair_order.repair_order_id"
                "\\nLEFT OUTER JOIN (SELECT  dispatchers.company_name,\\n\\t"
                "dispatchers.dispatcher_id \\n FROM roads.dispatchers AS dispatchers) "
                "AS dispatcher ON repair_order.dispatcher_id = dispatcher.dispatcher_id "
                "\\n GROUP BY  dispatcher.company_name\\nLIMIT 10"
            ),
            "executed_query": None,
            "scheduled": None,
            "started": None,
            "finished": None,
            "state": "FINISHED",
            "progress": 0.0,
            "output_table": None,
            "results": [
                {
                    "sql": "",
                    "columns": [
                        {
                            "column": "default_DOT_num_repair_orders",
                            "name": "default_DOT_num_repair_orders",
                            "node": "default.num_repair_orders",
                            "type": "bigint",
                        },
                        {
                            "column": "default_DOT_avg_repair_price",
                            "name": "default_DOT_avg_repair_price",
                            "node": "default.avg_repair_price",
                            "type": "double",
                        },
                        {
                            "column": "company_name",
                            "name": "default_DOT_dispatcher_DOT_company_name",
                            "node": "default.dispatcher",
                            "type": "string",
                        },
                    ],
                    "rows": [[1.0, "Foo", 100], [2.0, "Bar", 200]],
                    "row_count": 0,
                },
            ],
            "next": None,
            "previous": None,
            "errors": [],
            "links": None,
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
        custom_client = client_with_query_service_example_loader(["BASIC"])
        response = custom_client.get("/data/basic.num_comments/")
        data = response.json()
        assert response.status_code == 200
        assert data["id"] == "ee41ea6c-2303-4fe1-8bf0-f0ce3d6a35ca"

        # and try to get the results by the query id only
        new_response = custom_client.get(f"/data/query/{data['id']}/")
        new_data = response.json()
        assert new_response.status_code == 200
        assert new_data["results"] == [
            {
                "sql": "",
                "columns": [
                    {
                        "column": "basic_DOT_num_comments",
                        "name": "basic_DOT_num_comments",
                        "node": "basic.num_comments",
                        "type": "bigint",
                    },
                ],
                "rows": [[1]],
                "row_count": 0,
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
        large_revenue_payments_and_business_only = session.exec(statement).one()
        node_dict = large_revenue_payments_and_business_only.current.availability.dict()
        node_dict.pop("updated_at")
        assert node_dict == {
            "valid_through_ts": 20230125,
            "catalog": "default",
            "min_temporal_partition": ["2022", "01", "01"],
            "table": "pmts",
            "max_temporal_partition": ["2023", "01", "25"],
            "partitions": [],
            "schema_": "accounting",
            "id": 1,
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
        large_revenue_payments_and_business_only = session.exec(statement).one()
        node_dict = large_revenue_payments_and_business_only.current.availability.dict()
        node_dict.pop("updated_at")
        assert node_dict == {
            "valid_through_ts": 20230125,
            "catalog": "default",
            "min_temporal_partition": ["2022", "01", "01"],
            "table": "new_payments_table",
            "max_temporal_partition": ["2023", "01", "25"],
            "partitions": [],
            "schema_": "new_accounting",
            "id": 3,
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
        large_revenue_payments_and_business_only = session.exec(statement).one()
        updated_at_1 = (
            large_revenue_payments_and_business_only.current.availability.dict()[
                "updated_at"
            ]
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
            large_revenue_payments_and_business_only.current.availability.dict()[
                "updated_at"
            ]
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
        large_revenue_payments_only = session.exec(statement).one()
        node_dict = large_revenue_payments_only.current.availability.dict()
        node_dict.pop("updated_at")
        assert node_dict == {
            "valid_through_ts": 20230102,
            "catalog": "default",
            "min_temporal_partition": ["2022", "01", "01"],
            "table": "large_pmts",
            "max_temporal_partition": ["2023", "01", "02"],
            "schema_": "accounting",
            "partitions": [],
            "id": 2,
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
            "id": mock.ANY,
            "min_temporal_partition": ["20230101"],
            "max_temporal_partition": ["20230110"],
            "categorical_partitions": [],
            "temporal_partitions": ["birth_date"],
            "partitions": [],
            "schema_": "dimensions",
            "table": "local_hard_hats",
            "updated_at": mock.ANY,
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
            "id": mock.ANY,
            "min_temporal_partition": ["20230101"],
            "max_temporal_partition": ["20230110"],
            "categorical_partitions": ["country", "postal_code"],
            "temporal_partitions": ["birth_date"],
            "partitions": [],
            "schema_": "dimensions",
            "table": "local_hard_hats",
            "updated_at": mock.ANY,
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
        large_revenue_payments_only = session.exec(statement).one()
        node_dict = large_revenue_payments_only.current.availability.dict()
        node_dict.pop("updated_at")
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
            "id": 2,
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
        large_revenue_payments_only = session.exec(statement).one()
        node_dict = large_revenue_payments_only.current.availability.dict()
        node_dict.pop("updated_at")
        assert node_dict == {
            "valid_through_ts": 20230101,
            "catalog": "default",
            "min_temporal_partition": ["2022", "01", "01"],
            "table": "large_pmts",
            "max_temporal_partition": ["2023", "01", "01"],
            "schema_": "accounting",
            "partitions": [],
            "id": 2,
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
        revenue = session.exec(statement).one()
        node_dict = revenue.current.availability.dict()
        node_dict.pop("updated_at")
        assert node_dict == {
            "valid_through_ts": 20230101,
            "catalog": "default",
            "min_temporal_partition": ["2022", "01", "01"],
            "table": "revenue",
            "max_temporal_partition": ["2023", "01", "01"],
            "schema_": "accounting",
            "partitions": [],
            "id": 1,
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
