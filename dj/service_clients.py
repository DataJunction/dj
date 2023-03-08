"""Clients for various configurable services."""
from typing import List, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from dj.errors import DJException
from dj.models.column import Column
from dj.models.query import QueryWithResults
from dj.typing import ColumnType


class RequestsSessionWithEndpoint(requests.Session):
    """
    Creates a requests session that comes with an endpoint that all
    subsequent requests will use as a prefix.
    """

    def __init__(self, endpoint: str = None, retry_strategy: Retry = None):
        super().__init__()
        self.endpoint = endpoint
        self.mount("http://", HTTPAdapter(max_retries=retry_strategy))
        self.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    def request(self, method, url, *args, **kwargs):
        """
        Make the request with the full URL.
        """
        url = self.construct_url(url)
        return super().request(method, url, *args, **kwargs)

    def prepare_request(self, request, *args, **kwargs):
        """
        Prepare the request with the full URL.
        """
        request.url = self.construct_url(request.url)
        return super().prepare_request(
            request,
            *args,
            **kwargs,
        )

    def construct_url(self, url):
        """
        Construct full URL based off the endpoint.
        """
        return urljoin(self.endpoint, url)


class QueryServiceClient:  # pylint: disable=too-few-public-methods
    """
    Client for the query service.
    """

    def __init__(self, uri: str, retries: int = 2):
        self.uri = uri
        retry_strategy = Retry(
            total=retries,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "PATCH"],
        )
        self.requests_session = RequestsSessionWithEndpoint(
            endpoint=self.uri,
            retry_strategy=retry_strategy,
        )

    def get_columns_for_table(
        self,
        catalog: str,
        schema: str,
        table: str,
    ) -> List[Column]:
        """
        Retrieves columns for a table.
        """
        response = self.requests_session.get(
            f"/table/{catalog}.{schema}.{table}/columns/",
        )
        table_columns = response.json()["columns"]
        return [
            Column(name=column["name"], type=ColumnType(column["type"]))
            for column in table_columns
        ]

    def submit_query(  # pylint: disable=too-many-arguments
        self,
        engine: str,
        engine_version: str,
        submitted_query: str,
        catalog: Optional[str] = "default",
        async_: Optional[bool] = False,
    ) -> QueryWithResults:
        """
        Submit a query to the query service
        """
        response = self.requests_session.post(
            "/queries/",
            json={
                "engine": engine,
                "engine_version": engine_version,
                "submitted_query": submitted_query,
                "catalog_name": catalog,
                "async_": async_,
            },
        )
        if response.status_code >= 400:
            raise DJException(
                message=f"Error response from query service: {response.text}",
            )
        query_info = response.json()
        return QueryWithResults(**query_info)

    def get_query(
        self,
        query_id: str,
    ) -> QueryWithResults:
        """
        Get a previously submitted query
        """
        response = self.requests_session.get(f"/queries/{query_id}/")
        if response.status_code >= 400:
            raise DJException(
                message=f"Error response from query service: {response.text}",
            )
        query_info = response.json()
        return QueryWithResults(**query_info)
