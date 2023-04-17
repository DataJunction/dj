"""
Tests for the data API.
"""

from fastapi.testclient import TestClient
from sqlmodel import Session, select

from dj.models.node import Node


class TestDataForNode:
    """
    Test ``POST /data/{node_name}/``.
    """

    def test_get_dimension_data_failed(
        self,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test trying to get dimensions data while setting dimensions
        """
        response = client_with_examples.get(
            "/data/payment_type/",
            params={
                "dimensions": ["something"],
                "filters": [],
            },
        )
        data = response.json()
        assert response.status_code == 422
        assert data["message"] == "Cannot set dimensions for node type dimension!"

    def test_get_dimension_data(
        self,
        client_with_query_service: TestClient,
    ) -> None:
        """
        Test trying to get dimensions data while setting dimensions
        """
        response = client_with_query_service.get(
            "/data/payment_type/",
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
                        {"name": "id", "type": "int"},
                        {"name": "payment_type_classification", "type": "string"},
                        {"name": "payment_type_name", "type": "string"},
                    ],
                    "rows": [[1, "CARD", "VISA"], [2, "CARD", "MASTERCARD"]],
                    "row_count": 0,
                },
            ],
            "next": None,
            "previous": None,
            "errors": [],
        }

    def test_get_source_data(
        self,
        client_with_query_service: TestClient,
    ) -> None:
        """
        Test retrieving data for a source node
        """
        response = client_with_query_service.get("/data/revenue/")
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
                    "columns": [{"name": "*", "type": "wildcard"}],
                    "rows": [[129.19]],
                    "row_count": 0,
                },
            ],
            "next": None,
            "previous": None,
            "errors": [],
        }

    def test_get_transform_data(
        self,
        client_with_query_service: TestClient,
    ) -> None:
        """
        Test retrieving data for a transform node
        """
        response = client_with_query_service.get("/data/large_revenue_payments_only/")
        data = response.json()
        assert response.status_code == 200
        assert data == {
            "id": "1b049fb1-652e-458a-ba9d-3669412b34bd",
            "engine_name": None,
            "engine_version": None,
            "submitted_query": (
                "SELECT  revenue.account_type,\n\trevenue.customer_id,\n\trevenue.payment_amount,"
                '\n\trevenue.payment_id \n FROM "accounting"."revenue" AS revenue\n \n WHERE  '
                "revenue.payment_amount > 1000000"
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
                        {"name": "account_type", "type": "string"},
                        {"name": "customer_id", "type": "int"},
                        {"name": "payment_amount", "type": "float"},
                        {"name": "payment_id", "type": "int"},
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
        }

    def test_get_metric_data(
        self,
        client_with_query_service: TestClient,
    ) -> None:
        """
        Trying to get transform or source data should fail
        """
        response = client_with_query_service.get("/data/basic.num_comments/")
        data = response.json()
        assert response.status_code == 200
        assert data == {
            "id": "ee41ea6c-2303-4fe1-8bf0-f0ce3d6a35ca",
            "engine_name": None,
            "engine_version": None,
            "submitted_query": (
                'SELECT  COUNT(1) AS cnt \n FROM "basic"."comments" '
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
                    "columns": [{"name": "cnt", "type": "bigint"}],
                    "rows": [[1]],
                    "row_count": 0,
                },
            ],
            "next": None,
            "previous": None,
            "errors": [],
        }


class TestAvailabilityState:  # pylint: disable=too-many-public-methods
    """
    Test ``POST /data/{node_name}/availability/``.
    """

    def test_setting_availability_state(
        self,
        session: Session,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test adding an availability state
        """
        response = client_with_examples.post(
            "/data/large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_partition": ["2023", "01", "25"],
                "min_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        statement = select(Node).where(
            Node.name == "large_revenue_payments_and_business_only",
        )
        large_revenue_payments_and_business_only = session.exec(statement).one()
        node_dict = large_revenue_payments_and_business_only.current.availability.dict()
        node_dict.pop("updated_at")
        assert node_dict == {
            "valid_through_ts": 20230125,
            "catalog": "default",
            "min_partition": ["2022", "01", "01"],
            "table": "pmts",
            "max_partition": ["2023", "01", "25"],
            "schema_": "accounting",
            "id": 1,
        }

    def test_raising_if_availability_catalog_mismatch(
        self,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test raising when the catalog does not match
        """
        response = client_with_examples.post(
            "/data/large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "public",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_partition": ["2023", "01", "25"],
                "min_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 500
        assert data["message"] == (
            "Cannot set availability state in different " "catalog: public, default"
        )

    def test_setting_availability_state_multiple_times(
        self,
        session: Session,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test adding multiple availability states
        """
        response = client_with_examples.post(
            "/data/large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_partition": ["2023", "01", "25"],
                "min_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        response = client_with_examples.post(
            "/data/large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_partition": ["2023", "01", "25"],
                "min_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        response = client_with_examples.post(
            "/data/large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "new_accounting",
                "table": "new_payments_table",
                "valid_through_ts": 20230125,
                "max_partition": ["2023", "01", "25"],
                "min_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        statement = select(Node).where(
            Node.name == "large_revenue_payments_and_business_only",
        )
        large_revenue_payments_and_business_only = session.exec(statement).one()
        node_dict = large_revenue_payments_and_business_only.current.availability.dict()
        node_dict.pop("updated_at")
        assert node_dict == {
            "valid_through_ts": 20230125,
            "catalog": "default",
            "min_partition": ["2022", "01", "01"],
            "table": "new_payments_table",
            "max_partition": ["2023", "01", "25"],
            "schema_": "new_accounting",
            "id": 3,
        }

    def test_that_update_at_timestamp_is_being_updated(
        self,
        session: Session,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test that the `updated_at` attribute is being updated
        """
        response = client_with_examples.post(
            "/data/large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_partition": ["2023", "01", "25"],
                "min_partition": ["2022", "01", "01"],
            },
        )
        assert response.status_code == 200
        statement = select(Node).where(
            Node.name == "large_revenue_payments_and_business_only",
        )
        large_revenue_payments_and_business_only = session.exec(statement).one()
        updated_at_1 = (
            large_revenue_payments_and_business_only.current.availability.dict()[
                "updated_at"
            ]
        )

        response = client_with_examples.post(
            "/data/large_revenue_payments_and_business_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_partition": ["2023", "01", "25"],
                "min_partition": ["2022", "01", "01"],
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
        client_with_examples: TestClient,
    ) -> None:
        """
        Test raising when setting availability state on non-existent node
        """
        response = client_with_examples.post(
            "/data/nonexistentnode/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "pmts",
                "valid_through_ts": 20230125,
                "max_partition": ["2023", "01", "25"],
                "min_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 404
        assert data == {
            "message": "A node with name `nonexistentnode` does not exist.",
            "errors": [],
            "warnings": [],
        }

    def test_merging_in_a_higher_max_partition(
        self,
        session: Session,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test that the higher max_partition value is used when merging in an availability state
        """
        client_with_examples.post(
            "/data/large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230101,
                "max_partition": ["2023", "01", "01"],
                "min_partition": ["2022", "01", "01"],
            },
        )
        response = client_with_examples.post(
            "/data/large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230102,
                "max_partition": [
                    "2023",
                    "01",
                    "02",
                ],  # should be used since it's a higher max_partition
                "min_partition": [
                    "2023",
                    "01",
                    "02",
                ],  # should be ignored since it's a higher min_partition
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        statement = select(Node).where(
            Node.name == "large_revenue_payments_only",
        )
        large_revenue_payments_only = session.exec(statement).one()
        node_dict = large_revenue_payments_only.current.availability.dict()
        node_dict.pop("updated_at")
        assert node_dict == {
            "valid_through_ts": 20230102,
            "catalog": "default",
            "min_partition": ["2022", "01", "01"],
            "table": "large_pmts",
            "max_partition": ["2023", "01", "02"],
            "schema_": "accounting",
            "id": 2,
        }

    def test_merging_in_a_lower_min_partition(
        self,
        session: Session,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test that the lower min_partition value is used when merging in an availability state
        """
        client_with_examples.post(
            "/data/large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230101,
                "max_partition": ["2023", "01", "01"],
                "min_partition": ["2022", "01", "01"],
            },
        )
        response = client_with_examples.post(
            "/data/large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230101,
                "max_partition": [
                    "2021",
                    "12",
                    "31",
                ],  # should be ignored since it's a lower max_partition
                "min_partition": [
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
            Node.name == "large_revenue_payments_only",
        )
        large_revenue_payments_only = session.exec(statement).one()
        node_dict = large_revenue_payments_only.current.availability.dict()
        node_dict.pop("updated_at")
        assert node_dict == {
            "valid_through_ts": 20230101,
            "catalog": "default",
            "min_partition": ["2021", "12", "31"],
            "table": "large_pmts",
            "max_partition": ["2023", "01", "01"],
            "schema_": "accounting",
            "id": 2,
        }

    def test_moving_back_valid_through_ts(
        self,
        session: Session,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test that the valid through timestamp can be moved backwards
        """
        client_with_examples.post(
            "/data/large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230101,
                "max_partition": ["2023", "01", "01"],
                "min_partition": ["2022", "01", "01"],
            },
        )
        response = client_with_examples.post(
            "/data/large_revenue_payments_only/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20221231,
                "max_partition": [
                    "2023",
                    "01",
                    "01",
                ],  # should be ignored since it's a lower max_partition
                "min_partition": [
                    "2022",
                    "01",
                    "01",
                ],  # should be used since it's a lower min_partition
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        statement = select(Node).where(
            Node.name == "large_revenue_payments_only",
        )
        large_revenue_payments_only = session.exec(statement).one()
        node_dict = large_revenue_payments_only.current.availability.dict()
        node_dict.pop("updated_at")
        assert node_dict == {
            "valid_through_ts": 20221231,
            "catalog": "default",
            "min_partition": ["2022", "01", "01"],
            "table": "large_pmts",
            "max_partition": ["2023", "01", "01"],
            "schema_": "accounting",
            "id": 2,
        }

    def test_setting_availablity_state_on_a_source_node(
        self,
        session: Session,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test setting the availability state on a source node
        """
        response = client_with_examples.post(
            "/data/revenue/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "revenue",
                "valid_through_ts": 20230101,
                "max_partition": ["2023", "01", "01"],
                "min_partition": ["2022", "01", "01"],
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert data == {"message": "Availability state successfully posted"}

        statement = select(Node).where(
            Node.name == "revenue",
        )
        revenue = session.exec(statement).one()
        node_dict = revenue.current.availability.dict()
        node_dict.pop("updated_at")
        assert node_dict == {
            "valid_through_ts": 20230101,
            "catalog": "default",
            "min_partition": ["2022", "01", "01"],
            "table": "revenue",
            "max_partition": ["2023", "01", "01"],
            "schema_": "accounting",
            "id": 1,
        }

    def test_raise_on_setting_invalid_availability_state_on_a_source_node(
        self,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test raising availability state doesn't match existing source node table
        """
        response = client_with_examples.post(
            "/data/revenue/availability/",
            json={
                "catalog": "default",
                "schema_": "accounting",
                "table": "large_pmts",
                "valid_through_ts": 20230101,
                "max_partition": ["2023", "01", "01"],
                "min_partition": ["2022", "01", "01"],
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
