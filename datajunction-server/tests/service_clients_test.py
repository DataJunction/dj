"""
Tests for ``datajunction_server.service_clients``.
"""
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from requests import Request

from datajunction_server.errors import DJQueryServiceClientException
from datajunction_server.models import Engine
from datajunction_server.models.materialization import GenericMaterializationInput
from datajunction_server.models.node import NodeType
from datajunction_server.models.partition import PartitionBackfill
from datajunction_server.models.query import QueryCreate
from datajunction_server.service_clients import (
    QueryServiceClient,
    RequestsSessionWithEndpoint,
)


class TestRequestsSessionWithEndpoint:
    """
    Test using requests session with endpoint.
    """

    example_endpoint = "http://pieservice:7020"

    @pytest.fixture
    def requests_session(self) -> RequestsSessionWithEndpoint:
        """
        Create a requests session.
        """
        return RequestsSessionWithEndpoint(endpoint=self.example_endpoint)

    def test_prepare_request(
        self,
        requests_session: RequestsSessionWithEndpoint,
    ) -> None:
        """
        Test preparing request
        """
        req = Request(
            "GET",
            f"{self.example_endpoint}/pies/?flavor=blueberry",
            data=None,
            headers=None,
        )
        prepped = requests_session.prepare_request(req)
        assert prepped.headers["Connection"] == "keep-alive"

    def test_make_requests(
        self,
        mocker: MockerFixture,
        requests_session: RequestsSessionWithEndpoint,
    ):
        """
        Test making requests.
        """
        mock_request = mocker.patch("requests.Session.request")

        requests_session.get("/pies/")
        mock_request.assert_called_with(
            "GET",
            f"{self.example_endpoint}/pies/",
            allow_redirects=True,
        )

        requests_session.post("/pies/", json={"flavor": "blueberry", "diameter": 10})
        mock_request.assert_called_with(
            "POST",
            f"{self.example_endpoint}/pies/",
            data=None,
            json={"flavor": "blueberry", "diameter": 10},
        )


class TestQueryServiceClient:  # pylint: disable=too-few-public-methods
    """
    Test using the query service client.
    """

    endpoint = "http://queryservice:8001"

    def test_query_service_client_get_columns_for_table(
        self,
        mocker: MockerFixture,
    ) -> None:
        """
        Test the query service client.
        """

        mock_request = mocker.patch("requests.Session.request")
        query_service_client = QueryServiceClient(uri=self.endpoint)
        query_service_client.get_columns_for_table("hive", "test", "pies")
        mock_request.assert_called_with(
            "GET",
            "http://queryservice:8001/table/hive.test.pies/columns/",
            params={},
            allow_redirects=True,
        )

        query_service_client.get_columns_for_table(
            "hive",
            "test",
            "pies",
            engine=Engine(name="spark", version="2.4.4"),
        )
        mock_request.assert_called_with(
            "GET",
            "http://queryservice:8001/table/hive.test.pies/columns/",
            params={"engine": "spark", "engine_version": "2.4.4"},
            allow_redirects=True,
        )

    def test_query_service_client_submit_query(self, mocker: MockerFixture) -> None:
        """
        Test submitting a query to a query service client.
        """

        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {
            "catalog_name": "public",
            "engine_name": "postgres",
            "engine_version": "15.2",
            "id": "ef209eef-c31a-4089-aae6-833259a08e22",
            "submitted_query": "SELECT 1 as num",
            "executed_query": "SELECT 1 as num",
            "scheduled": "2023-01-01T00:00:00.000000",
            "started": "2023-01-01T00:00:00.000000",
            "finished": "2023-01-01T00:00:00.000001",
            "state": "FINISHED",
            "progress": 1,
            "results": [
                {
                    "sql": "SELECT 1 as num",
                    "columns": [{"name": "num", "type": "STR"}],
                    "rows": [[1]],
                    "row_count": 1,
                },
            ],
            "next": None,
            "previous": None,
            "errors": [],
        }

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        query_create = QueryCreate(
            catalog_name="default",
            engine_name="postgres",
            engine_version="15.2",
            submitted_query="SELECT 1",
            async_=False,
        )
        query_service_client.submit_query(query_create)

        mock_request.assert_called_with(
            "/queries/",
            json={
                "catalog_name": "default",
                "engine_name": "postgres",
                "engine_version": "15.2",
                "submitted_query": "SELECT 1",
                "async_": False,
            },
        )

    def test_query_service_client_get_query(self, mocker: MockerFixture) -> None:
        """
        Test getting a previously submitted query from a query service client.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "catalog_name": "public",
            "engine_name": "postgres",
            "engine_version": "15.2",
            "id": "ef209eef-c31a-4089-aae6-833259a08e22",
            "submitted_query": "SELECT 1 as num",
            "executed_query": "SELECT 1 as num",
            "scheduled": "2023-01-01T00:00:00.000000",
            "started": "2023-01-01T00:00:00.000000",
            "finished": "2023-01-01T00:00:00.000001",
            "state": "FINISHED",
            "progress": 1,
            "results": [
                {
                    "sql": "SELECT 1 as num",
                    "columns": [{"name": "num", "type": "STR"}],
                    "rows": [[1]],
                    "row_count": 1,
                },
            ],
            "next": None,
            "previous": None,
            "errors": [],
            "database_id": 1,  # Will be deprecated soon in favor of catalog
        }

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.get",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        query_service_client.get_query(
            "ef209eef-c31a-4089-aae6-833259a08e22",
        )

        mock_request.assert_called_with(
            "/queries/ef209eef-c31a-4089-aae6-833259a08e22/",
        )

    def test_query_service_client_materialize(self, mocker: MockerFixture) -> None:
        """
        Test materialize from a query service client.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "urls": ["http://fake.url/job"],
            "output_tables": ["common.a", "common.b"],
        }

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        query_service_client.materialize(
            GenericMaterializationInput(
                name="default",
                node_name="default.hard_hat",
                node_version="v1",
                node_type=NodeType.DIMENSION,
                schedule="0 * * * *",
                query="",
                spark_conf={},
                upstream_tables=["default.hard_hats"],
                partitions=[],
                columns=[],
            ),
        )

        mock_request.assert_called_with(
            "/materialization/",
            json={
                "name": "default",
                "node_name": "default.hard_hat",
                "node_version": "v1",
                "node_type": "dimension",
                "partitions": [],
                "query": "",
                "schedule": "0 * * * *",
                "spark_conf": {},
                "upstream_tables": ["default.hard_hats"],
                "columns": [],
            },
        )

    def test_query_service_client_deactivate_materialization(
        self,
        mocker: MockerFixture,
    ) -> None:
        """
        Test deactivate materialization from a query service client.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "urls": ["http://fake.url/job"],
            "output_tables": ["common.a", "common.b"],
        }

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.delete",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        query_service_client.deactivate_materialization(
            node_name="default.hard_hat",
            materialization_name="default",
        )

        mock_request.assert_called_with(
            "/materialization/",
            params={
                "node_name": "default.hard_hat",
                "materialization_name": "default",
            },
        )

    def test_query_service_client_raising_error(self, mocker: MockerFixture) -> None:
        """
        Test handling an error response from the query service client
        """
        mock_response = MagicMock()
        mock_response.ok = False

        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.get",
            return_value=mock_response,
        )
        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)

        with pytest.raises(DJQueryServiceClientException) as exc_info:
            query_service_client.get_query(
                "ef209eef-c31a-4089-aae6-833259a08e22",
            )
        assert "Error response from query service" in str(exc_info.value)
        query_create = QueryCreate(
            catalog_name="hive",
            engine_name="postgres",
            engine_version="15.2",
            submitted_query="SELECT 1",
            async_=False,
        )

        with pytest.raises(DJQueryServiceClientException) as exc_info:
            query_service_client.submit_query(query_create)
        assert "Error response from query service" in str(exc_info.value)

    def test_materialize(self, mocker: MockerFixture) -> None:
        """
        Test get materialization urls for a given node materialization
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "urls": ["http://fake.url/job"],
            "output_tables": ["common.a", "common.b"],
        }

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        response = query_service_client.materialize(
            GenericMaterializationInput(
                name="default",
                node_name="default.hard_hat",
                node_version="v1",
                node_type=NodeType.DIMENSION,
                schedule="0 * * * *",
                query="",
                spark_conf={},
                upstream_tables=["default.hard_hats"],
                partitions=[],
                columns=[],
            ),
        )
        mock_request.assert_called_with(
            "/materialization/",
            json={
                "name": "default",
                "node_name": "default.hard_hat",
                "node_version": "v1",
                "node_type": "dimension",
                "schedule": "0 * * * *",
                "query": "",
                "upstream_tables": ["default.hard_hats"],
                "spark_conf": {},
                "partitions": [],
                "columns": [],
            },
        )
        assert response == {
            "urls": ["http://fake.url/job"],
            "output_tables": ["common.a", "common.b"],
        }

    def test_get_materialization_info(self, mocker: MockerFixture) -> None:
        """
        Test get materialization urls for a given node materialization
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "urls": ["http://fake.url/job"],
            "output_tables": ["common.a", "common.b"],
        }

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.get",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        response = query_service_client.get_materialization_info(
            node_name="default.hard_hat",
            node_version="v3.1",
            materialization_name="default",
        )
        mock_request.assert_called_with(
            "/materialization/default.hard_hat/v3.1/default/",
            timeout=3,
        )
        assert response == {
            "urls": ["http://fake.url/job"],
            "output_tables": ["common.a", "common.b"],
        }

    def test_get_materialization_info_error(self, mocker: MockerFixture) -> None:
        """
        Test get materialization info with errors
        """
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.ok = False
        mock_response.json.return_value = {"message": "An error has occurred"}

        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.get",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        response = query_service_client.get_materialization_info(
            node_name="default.hard_hat",
            node_version="v3.1",
            materialization_name="default",
        )
        assert response == {
            "urls": [],
            "output_tables": [],
        }

    def test_run_backfill(self, mocker: MockerFixture) -> None:
        """
        Test get materialization info with errors
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "urls": ["http://fake.url/job"],
            "output_tables": [],
        }

        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        response = query_service_client.run_backfill(
            node_name="default.hard_hat",
            backfill=PartitionBackfill(
                column_name="hire_date",
                range=["20230101", "20230201"],
            ),
            materialization_name="default",
        )
        assert response == {
            "urls": ["http://fake.url/job"],
            "output_tables": [],
        }
