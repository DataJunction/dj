"""
Tests for ``datajunction_server.service_clients``.
"""

from datetime import date
from unittest.mock import ANY, MagicMock

import pytest
from pytest_mock import MockerFixture
from requests import Request

from datajunction_server.database.engine import Engine
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJQueryServiceClientEntityNotFound,
    DJQueryServiceClientException,
)
from datajunction_server.models.cube_materialization import (
    CubeMetric,
    CubeMaterializationV2Input,
    DruidCubeMaterializationInput,
    MeasureKey,
    NodeNameVersion,
)
from datajunction_server.models.materialization import (
    GenericMaterializationInput,
    MaterializationInfo,
    MaterializationStrategy,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import PartitionBackfill
from datajunction_server.models.preaggregation import CubeBackfillInput
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


class TestQueryServiceClient:
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
        mock_request.return_value = MagicMock(status_code=200, text="Unknown")
        query_service_client = QueryServiceClient(uri=self.endpoint)
        query_service_client.get_columns_for_table("hive", "test", "pies")
        mock_request.assert_called_with(
            "GET",
            "http://queryservice:8001/table/hive.test.pies/columns/",
            params={},
            allow_redirects=True,
            headers=ANY,
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
            headers=ANY,
        )

        # failed request with unknown reason
        mock_request = mocker.patch("requests.Session.request")
        mock_request.return_value = MagicMock(status_code=400, text="Unknown")
        query_service_client = QueryServiceClient(uri=self.endpoint)
        with pytest.raises(DJQueryServiceClientException) as exc_info:
            query_service_client.get_columns_for_table("hive", "test", "pies")
        assert "Error response from query service" in str(exc_info.value)

        # failed request with table not found
        mock_request = mocker.patch("requests.Session.request")
        mock_request.return_value = MagicMock(status_code=404, text="Table not found")
        with pytest.raises(DJDoesNotExistException) as exc_info:
            query_service_client.get_columns_for_table("hive", "test", "pies")
        assert "Table not found" in str(exc_info.value)

        # no columns returned
        mock_request = mocker.patch("requests.Session.request")
        mock_request.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"columns": []}),
        )
        with pytest.raises(DJDoesNotExistException) as exc_info:
            query_service_client.get_columns_for_table("hive", "test", "pies")
        assert "No columns found" in str(exc_info.value)

    def test_query_service_client_create_view(self, mocker: MockerFixture) -> None:
        """
        Test creating a view using the query service client.
        """

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "catalog_name": "public",
            "engine_name": "postgres",
            "engine_version": "15.2",
            "id": "ef209eef-c31a-4089-aae6-833259a08e22",
            "submitted_query": "CREATE OR REPLACE VIEW foo SELECT 1 as num",
            "executed_query": "CREATE OR REPLACE VIEW foo SELECT 1 as num",
            "scheduled": "2023-01-01T00:00:00.000000",
            "started": "2023-01-01T00:00:00.000000",
            "finished": "2023-01-01T00:00:00.000001",
            "state": "FINISHED",
            "progress": 1,
            "results": [],
            "next": None,
            "previous": None,
            "errors": [],
        }

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        # successful request
        query_service_client = QueryServiceClient(uri=self.endpoint)
        query_create = QueryCreate(
            catalog_name="default",
            engine_name="postgres",
            engine_version="15.2",
            submitted_query="CREATE OR REPLACE VIEW foo SELECT 1 as num",
            async_=False,
        )
        query_service_client.create_view(
            view_name="foo",
            query_create=query_create,
        )

        mock_request.assert_called_with(
            "/queries/",
            headers=ANY,
            json={
                "catalog_name": "default",
                "engine_name": "postgres",
                "engine_version": "15.2",
                "submitted_query": "CREATE OR REPLACE VIEW foo SELECT 1 as num",
                "async_": False,
            },
        )

    def test_query_service_client_create_view_with_failure(
        self,
        mocker: MockerFixture,
    ) -> None:
        """
        Test creating a view using the query service client with a filed response.
        """

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"
        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        # successful request
        query_service_client = QueryServiceClient(uri=self.endpoint)
        query_create = QueryCreate(
            catalog_name="default",
            engine_name="postgres",
            engine_version="15.2",
            submitted_query="CREATE OR REPLACE VIEW foo SELECT 1 as num",
            async_=False,
        )

        with pytest.raises(DJQueryServiceClientException) as exc_info:
            query_service_client.create_view(
                view_name="foo",
                query_create=query_create,
            )
        assert "Error response from query service: Internal server error" in str(
            exc_info.value,
        )

        mock_request.assert_called_with(
            "/queries/",
            headers=ANY,
            json={
                "catalog_name": "default",
                "engine_name": "postgres",
                "engine_version": "15.2",
                "submitted_query": "CREATE OR REPLACE VIEW foo SELECT 1 as num",
                "async_": False,
            },
        )

    def test_query_service_client_submit_query(self, mocker: MockerFixture) -> None:
        """
        Test submitting a query to a query service client.
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
        query_service_client.submit_query(
            query_create,
        )

        mock_request.assert_called_with(
            "/queries/",
            headers=ANY,
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
            headers=ANY,
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
                job="SparkSqlMaterializationJob",
                strategy=MaterializationStrategy.FULL,
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
                "job": "SparkSqlMaterializationJob",
                "strategy": "full",
                "node_name": "default.hard_hat",
                "node_version": "v1",
                "node_type": "dimension",
                "partitions": [],
                "query": "",
                "schedule": "0 * * * *",
                "spark_conf": {},
                "upstream_tables": ["default.hard_hats"],
                "columns": [],
                "lookback_window": "1 DAY",
            },
            headers=ANY,
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
            "/materialization/default.hard_hat/default/",
            headers=ANY,
            json={},
        )

    def test_query_service_client_raising_error(self, mocker: MockerFixture) -> None:
        """
        Test handling an error response from the query service client
        """
        mock_400_response = MagicMock()
        mock_400_response.status_code = 400
        mock_400_response.text = "Bad request error"

        mock_404_response = MagicMock()
        mock_404_response.status_code = 404
        mock_404_response.text = "Query not found"

        query_service_client = QueryServiceClient(uri=self.endpoint)

        with mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.get",
            return_value=mock_400_response,
        ):
            with pytest.raises(DJQueryServiceClientException) as exc_info:
                query_service_client.get_query(
                    "ef209eef-c31a-4089-aae6-833259a08e22",
                )
        assert "Bad request error" in str(exc_info.value)

        with mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.get",
            return_value=mock_404_response,
        ):
            with pytest.raises(DJQueryServiceClientEntityNotFound) as exc_info:
                query_service_client.get_query(
                    "ef209eef-c31a-4089-aae6-833259a08e22",
                )
        assert "Query not found" in str(exc_info.value)

        query_create = QueryCreate(
            catalog_name="hive",
            engine_name="postgres",
            engine_version="15.2",
            submitted_query="SELECT 1",
            async_=False,
        )

        with mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_400_response,
        ):
            with pytest.raises(DJQueryServiceClientException) as exc_info:
                query_service_client.submit_query(
                    query_create,
                )
        assert "Bad request error" in str(exc_info.value)

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
                job="SparkSqlMaterializationJob",
                strategy=MaterializationStrategy.FULL,
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
                "job": "SparkSqlMaterializationJob",
                "lookback_window": "1 DAY",
                "strategy": "full",
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
            headers=ANY,
        )
        assert response == MaterializationInfo(
            urls=["http://fake.url/job"],
            output_tables=["common.a", "common.b"],
        )

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
            node_type=NodeType.DIMENSION,
            materialization_name="default",
        )
        mock_request.assert_called_with(
            "/materialization/default.hard_hat/v3.1/default/?node_type=dimension",
            timeout=3,
            headers=ANY,
        )
        assert response == MaterializationInfo(
            urls=["http://fake.url/job"],
            output_tables=["common.a", "common.b"],
        )

    def test_get_materialization_info_error(self, mocker: MockerFixture) -> None:
        """
        Test get materialization info with errors
        """
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"message": "An error has occurred"}

        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.get",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        response = query_service_client.get_materialization_info(
            node_name="default.hard_hat",
            node_version="v3.1",
            node_type=NodeType.DIMENSION,
            materialization_name="default",
        )
        assert response == MaterializationInfo(
            urls=[],
            output_tables=[],
        )

    def test_run_backfill(self, mocker: MockerFixture) -> None:
        """
        Test running backfill on temporal partitions and categorical partitions
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "urls": ["http://fake.url/job"],
            "output_tables": [],
        }

        mocked_call = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        response = query_service_client.run_backfill(
            node_name="default.hard_hat",
            node_version="v1",
            node_type=NodeType.DIMENSION,
            partitions=[
                PartitionBackfill(
                    column_name="hire_date",
                    range=["20230101", "20230201"],
                ),
            ],
            materialization_name="default",
        )
        assert response == MaterializationInfo(
            urls=["http://fake.url/job"],
            output_tables=[],
        )
        mocked_call.assert_called_with(
            "/materialization/run/default.hard_hat/default/?node_version=v1&node_type=dimension",
            json=[
                {
                    "column_name": "hire_date",
                    "range": ["20230101", "20230201"],
                    "values": None,
                },
            ],
            timeout=20,
            headers=ANY,
        )

        mocked_call.reset_mock()

        query_service_client = QueryServiceClient(uri=self.endpoint)
        response = query_service_client.run_backfill(
            node_name="default.hard_hat",
            node_version="v1",
            node_type=NodeType.DIMENSION,
            partitions=[
                PartitionBackfill(
                    column_name="hire_date",
                    range=["20230101", "20230201"],
                ),
                PartitionBackfill(
                    column_name="state",
                    values=["CA", "DE"],
                ),
            ],
            materialization_name="default",
        )
        assert response == MaterializationInfo(
            urls=["http://fake.url/job"],
            output_tables=[],
        )
        mocked_call.assert_called_with(
            "/materialization/run/default.hard_hat/default/?node_version=v1&node_type=dimension",
            json=[
                {
                    "column_name": "hire_date",
                    "range": ["20230101", "20230201"],
                    "values": None,
                },
                {
                    "column_name": "state",
                    "range": None,
                    "values": ["CA", "DE"],
                },
            ],
            timeout=20,
            headers=ANY,
        )

    def test_filtered_headers(self):
        """
        By default, no headers are forwarded from the original request.
        Auth should be handled via session headers set during client initialization.
        """
        client = QueryServiceClient(uri="http://localhost:8000")
        assert (
            client.filtered_headers(
                {
                    "User-Agent": "python-requests/2.29.0",
                    "Accept-Encoding": "gzip, deflate",
                    "Accept": "*/*",
                },
            )
            == {}
        )
        assert (
            client.filtered_headers(
                {
                    "Authorization": "Bearer token",
                    "X-Custom-Header": "value",
                },
            )
            == {}
        )

    def test_materialize_cube(self, mocker: MockerFixture) -> None:
        """
        Test materialize cube via query service client
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
        materialization_input = DruidCubeMaterializationInput(
            name="default",
            strategy=MaterializationStrategy.INCREMENTAL_TIME,
            schedule="@daily",
            job="DruidCubeMaterialization",
            cube=NodeNameVersion(name="default.repairs_cube", version="v1.0"),
            dimensions=["default.hard_hat.first_name", "default.hard_hat.last_name"],
            metrics=[
                CubeMetric(
                    metric=NodeNameVersion(
                        name="default.num_repair_orders",
                        version="v1.0",
                        display_name=None,
                    ),
                    required_measures=[
                        MeasureKey(
                            node=NodeNameVersion(
                                name="default.repair_orders",
                                version="v1.0",
                            ),
                            measure_name="count",
                        ),
                    ],
                    derived_expression="SELECT SUM(count) FROM default.repair_orders",
                    metric_expression="SUM(count)",
                ),
                CubeMetric(
                    metric=NodeNameVersion(
                        name="default.avg_repair_price",
                        version="v1.0",
                        display_name=None,
                    ),
                    required_measures=[
                        MeasureKey(
                            node=NodeNameVersion(
                                name="default.repair_orders",
                                version="v1.0",
                            ),
                            measure_name="sum_price_123abc",
                        ),
                    ],
                    derived_expression="SELECT SUM(sum_price_123abc) FROM default.repair_orders",
                    metric_expression="SUM(sum_price_123abc)",
                ),
            ],
            measures_materializations=[],
            combiners=[],
        )
        response = query_service_client.materialize_cube(materialization_input)
        mock_request.assert_called_with(
            "/cubes/materialize",
            json=materialization_input.model_dump(),
            timeout=20,
            headers=ANY,
        )
        assert response == MaterializationInfo(
            urls=["http://fake.url/job"],
            output_tables=["common.a", "common.b"],
        )

    def test_materialize_preagg(self, mocker: MockerFixture) -> None:
        """
        Test materialize_preagg via query service client.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "workflow_url": "http://fake.url/workflow/123",
            "status": "SCHEDULED",
            "output_tables": ["common.preagg_table"],
        }

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        # Create a mock materialization input
        mock_input = MagicMock()
        mock_input.preagg_id = 123
        mock_input.output_table = "common.preagg_table"
        mock_input.model_dump.return_value = {
            "preagg_id": 123,
            "output_table": "common.preagg_table",
            "query": "SELECT * FROM test",
            "schedule": "0 * * * *",
        }

        query_service_client = QueryServiceClient(uri=self.endpoint)
        response = query_service_client.materialize_preagg(mock_input)

        mock_request.assert_called_with(
            "/preaggs/materialize",
            json={
                "preagg_id": 123,
                "output_table": "common.preagg_table",
                "query": "SELECT * FROM test",
                "schedule": "0 * * * *",
            },
            headers=ANY,
            timeout=30,
        )
        assert response == {
            "workflow_url": "http://fake.url/workflow/123",
            "status": "SCHEDULED",
            "output_tables": ["common.preagg_table"],
        }

    def test_materialize_preagg_with_error(self, mocker: MockerFixture) -> None:
        """
        Test materialize_preagg error handling.
        """
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"

        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        mock_input = MagicMock()
        mock_input.preagg_id = 123
        mock_input.output_table = "common.preagg_table"
        mock_input.model_dump.return_value = {"preagg_id": 123}

        query_service_client = QueryServiceClient(uri=self.endpoint)
        with pytest.raises(Exception) as exc_info:
            query_service_client.materialize_preagg(mock_input)
        assert "Query service error" in str(exc_info.value)

    def test_deactivate_preagg_workflow(self, mocker: MockerFixture) -> None:
        """
        Test deactivate_preagg_workflow via query service client.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "DEACTIVATED"}'
        mock_response.json.return_value = {"status": "DEACTIVATED"}

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.delete",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        response = query_service_client.deactivate_preagg_workflow(
            output_table="test_preagg_table",
        )

        mock_request.assert_called_with(
            "/preaggs/test_preagg_table/workflow",
            headers=ANY,
            timeout=20,
        )
        assert response == {"status": "DEACTIVATED"}

    def test_deactivate_preagg_workflow_empty_response(
        self,
        mocker: MockerFixture,
    ) -> None:
        """
        Test deactivate_preagg_workflow with empty response body (204).
        """
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_response.text = ""

        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.delete",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        response = query_service_client.deactivate_preagg_workflow(
            output_table="another_preagg_table",
        )

        assert response == {}

    def test_deactivate_preagg_workflow_with_error(
        self,
        mocker: MockerFixture,
    ) -> None:
        """
        Test deactivate_preagg_workflow error handling.
        """
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Workflow not found"

        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.delete",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        with pytest.raises(Exception) as exc_info:
            query_service_client.deactivate_preagg_workflow(
                output_table="nonexistent_preagg",
            )
        assert "Query service error" in str(exc_info.value)

    def test_run_preagg_backfill(self, mocker: MockerFixture) -> None:
        """
        Test run_preagg_backfill via query service client.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "job_url": "http://fake.url/job/backfill/123",
            "status": "RUNNING",
        }

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        mock_input = MagicMock()
        mock_input.preagg_id = 123
        mock_input.model_dump.return_value = {
            "preagg_id": 123,
            "partitions": [{"column_name": "date", "range": ["20230101", "20230201"]}],
        }

        query_service_client = QueryServiceClient(uri=self.endpoint)
        response = query_service_client.run_preagg_backfill(mock_input)

        mock_request.assert_called_with(
            "/preaggs/backfill",
            json={
                "preagg_id": 123,
                "partitions": [
                    {"column_name": "date", "range": ["20230101", "20230201"]},
                ],
            },
            headers=ANY,
            timeout=30,
        )
        assert response == {
            "job_url": "http://fake.url/job/backfill/123",
            "status": "RUNNING",
        }

    def test_run_preagg_backfill_with_error(self, mocker: MockerFixture) -> None:
        """
        Test run_preagg_backfill error handling.
        """
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Invalid partition range"

        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        mock_input = MagicMock()
        mock_input.preagg_id = 123
        mock_input.model_dump.return_value = {"preagg_id": 123}

        query_service_client = QueryServiceClient(uri=self.endpoint)
        with pytest.raises(Exception) as exc_info:
            query_service_client.run_preagg_backfill(mock_input)
        assert "Query service error" in str(exc_info.value)

    def test_materialize_cube_v2_success(self, mocker: MockerFixture) -> None:
        """
        Test successful v2 cube materialization.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "urls": ["http://workflow1"],
            "output_tables": ["druid_ds"],
            "status": "created",
        }

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        input_data = CubeMaterializationV2Input(
            cube_name="test.cube",
            cube_version="v1",
            preagg_tables=[],
            combined_sql="SELECT 1",
            combined_columns=[],
            combined_grain=[],
            druid_datasource="test_ds",
            druid_spec={},
            timestamp_column="date_id",
            timestamp_format="yyyyMMdd",
            strategy=MaterializationStrategy.FULL,
            schedule="0 0 * * *",
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        result = query_service_client.materialize_cube_v2(input_data)

        mock_request.assert_called_once()
        assert result.urls == ["http://workflow1"]

    def test_materialize_cube_v2_failure(self, mocker: MockerFixture) -> None:
        """
        Test v2 cube materialization failure raises exception.
        """
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        input_data = CubeMaterializationV2Input(
            cube_name="test.cube",
            cube_version="v1",
            preagg_tables=[],
            combined_sql="SELECT 1",
            combined_columns=[],
            combined_grain=[],
            druid_datasource="test_ds",
            druid_spec={},
            timestamp_column="date_id",
            timestamp_format="yyyyMMdd",
            strategy=MaterializationStrategy.FULL,
            schedule="0 0 * * *",
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        with pytest.raises(Exception) as exc_info:
            query_service_client.materialize_cube_v2(input_data)
        assert "Query service error" in str(exc_info.value)

    def test_deactivate_cube_workflow_success(self, mocker: MockerFixture) -> None:
        """
        Test successful cube workflow deactivation.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "deactivated"}'
        mock_response.json.return_value = {"status": "deactivated"}

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.delete",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        result = query_service_client.deactivate_cube_workflow("test.cube")

        mock_request.assert_called_once()
        # Verify URL doesn't have version parameter
        assert mock_request.call_args[0][0] == "/cubes/test.cube/workflow"
        assert result["status"] == "deactivated"

    def test_deactivate_cube_workflow_with_version(self, mocker: MockerFixture) -> None:
        """
        Test cube workflow deactivation with version parameter.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"status": "deactivated"}'
        mock_response.json.return_value = {"status": "deactivated"}

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.delete",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        result = query_service_client.deactivate_cube_workflow(
            "test.cube",
            version="v3",
        )

        mock_request.assert_called_once()
        # Verify URL has version parameter
        assert mock_request.call_args[0][0] == "/cubes/test.cube/workflow?version=v3"
        assert result["status"] == "deactivated"

    def test_deactivate_cube_workflow_failure_returns_failed_status(
        self,
        mocker: MockerFixture,
    ) -> None:
        """
        Test cube workflow deactivation failure returns failed status (doesn't raise).
        """
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"

        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.delete",
            return_value=mock_response,
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        result = query_service_client.deactivate_cube_workflow("test.cube")
        # Should NOT raise, should return failed status
        assert result["status"] == "failed"

    def test_run_cube_backfill_success(self, mocker: MockerFixture) -> None:
        """
        Test successful cube backfill.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"job_url": "http://backfill-job"}

        mock_request = mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        backfill_input = CubeBackfillInput(
            cube_name="test.cube",
            cube_version="v1.0",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        result = query_service_client.run_cube_backfill(backfill_input)

        mock_request.assert_called_once()
        assert result["job_url"] == "http://backfill-job"

    def test_run_cube_backfill_failure(self, mocker: MockerFixture) -> None:
        """
        Test cube backfill failure raises exception.
        """
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        mocker.patch(
            "datajunction_server.service_clients.RequestsSessionWithEndpoint.post",
            return_value=mock_response,
        )

        backfill_input = CubeBackfillInput(
            cube_name="test.cube",
            cube_version="v1.0",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
        )

        query_service_client = QueryServiceClient(uri=self.endpoint)
        with pytest.raises(Exception) as exc_info:
            query_service_client.run_cube_backfill(backfill_input)
        assert "Query service error" in str(exc_info.value)
