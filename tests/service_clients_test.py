"""
Tests for ``dj.service_clients``.
"""
import pytest
from pytest_mock import MockerFixture
from requests import Request

from dj.service_clients import QueryServiceClient, RequestsSessionWithEndpoint


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

    def test_get_columns_for_table(self, mocker: MockerFixture) -> None:
        """
        Test the query service client.
        """

        mock_request = mocker.patch("requests.Session.request")
        query_service_client = QueryServiceClient(uri=self.endpoint)
        query_service_client.get_columns_for_table("hive", "test", "pies")

        mock_request.assert_called_with(
            "GET",
            "http://queryservice:8001/table/hive.test.pies/columns/",
            allow_redirects=True,
        )
