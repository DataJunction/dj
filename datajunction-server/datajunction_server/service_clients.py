"""Clients for various configurable services."""
from http import HTTPStatus
from typing import TYPE_CHECKING, Dict, List, Optional, Union
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from datajunction_server.database.column import Column
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJError,
    DJQueryServiceClientException,
    ErrorCode,
)
from datajunction_server.models.materialization import (
    DruidMaterializationInput,
    GenericMaterializationInput,
    MaterializationInfo,
)
from datajunction_server.models.partition import PartitionBackfill
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.sql.parsing.types import ColumnType

if TYPE_CHECKING:
    from datajunction_server.database.engine import Engine


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

    HEADERS_TO_IGNORE = ("accept-encoding",)

    def __init__(self, uri: str, retries: int = 0):
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

    @staticmethod
    def filtered_headers(request_headers: Dict[str, str]):
        """
        The request headers with the headers to ignore filtered out.
        """
        return {
            key: value
            for key, value in request_headers.items()
            if key.lower() not in QueryServiceClient.HEADERS_TO_IGNORE
        }

    def get_columns_for_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        request_headers: Optional[Dict[str, str]] = None,
        engine: Optional["Engine"] = None,
    ) -> List[Column]:
        """
        Retrieves columns for a table.
        """
        response = self.requests_session.get(
            f"/table/{catalog}.{schema}.{table}/columns/",
            params={
                "engine": engine.name,
                "engine_version": engine.version,
            }
            if engine
            else {},
            headers={
                **self.requests_session.headers,
                **QueryServiceClient.filtered_headers(request_headers),
            }
            if request_headers
            else self.requests_session.headers,
        )
        if response.status_code not in (200, 201):
            if response.status_code == HTTPStatus.NOT_FOUND:
                raise DJDoesNotExistException(
                    message=f"Table not found: {response.text}",
                )
            raise DJQueryServiceClientException(
                message=f"Error response from query service: {response.text}",
            )
        table_columns = response.json()["columns"]
        if not table_columns:
            raise DJQueryServiceClientException(
                message=f"No columns found: {response.text}",
            )
        return [
            Column(name=column["name"], type=ColumnType(column["type"]), order=idx)
            for idx, column in enumerate(table_columns)
        ]

    def create_view(  # pylint: disable=too-many-arguments
        self,
        view_name: str,
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Re-create a view using the query service.
        """
        response = self.requests_session.post(
            "/queries/",
            headers={
                **self.requests_session.headers,
                **QueryServiceClient.filtered_headers(request_headers),
            }
            if request_headers
            else self.requests_session.headers,
            json=query_create.dict(),
        )
        response_data = response.json()
        if response.status_code not in (200, 201):
            raise DJQueryServiceClientException(
                message=f"Error response from query service: {response_data['message']}",
                errors=[
                    DJError(code=ErrorCode.QUERY_SERVICE_ERROR, message=error)
                    for error in response_data["errors"]
                ],
                http_status_code=response.status_code,
            )
        return f"View '{view_name}' created successfully."

    def submit_query(  # pylint: disable=too-many-arguments
        self,
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> QueryWithResults:
        """
        Submit a query to the query service
        """
        response = self.requests_session.post(
            "/queries/",
            headers={
                **self.requests_session.headers,
                **QueryServiceClient.filtered_headers(request_headers),
            }
            if request_headers
            else self.requests_session.headers,
            json=query_create.dict(),
        )
        response_data = response.json()
        if response.status_code not in (200, 201):
            raise DJQueryServiceClientException(
                message=f"Error response from query service: {response_data['message']}",
                errors=[
                    DJError(code=ErrorCode.QUERY_SERVICE_ERROR, message=error)
                    for error in response_data["errors"]
                ],
                http_status_code=response.status_code,
            )
        query_info = response.json()
        return QueryWithResults(**query_info)

    def get_query(
        self,
        query_id: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> QueryWithResults:
        """
        Get a previously submitted query
        """
        response = self.requests_session.get(
            f"/queries/{query_id}/",
            headers={**self.requests_session.headers, **request_headers}
            if request_headers
            else self.requests_session.headers,
        )
        if response.status_code not in (200, 201):
            raise DJQueryServiceClientException(
                message=f"Error response from query service: {response.text}",
            )
        query_info = response.json()
        return QueryWithResults(**query_info)

    def materialize(
        self,
        materialization_input: Union[
            GenericMaterializationInput,
            DruidMaterializationInput,
        ],
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Post a request to the query service asking it to set up a scheduled materialization
        for the node. The query service is expected to manage all reruns of this job. Note
        that this functionality may be moved to the materialization service at a later point.
        """
        response = self.requests_session.post(
            "/materialization/",
            json=materialization_input.dict(),
            headers={
                **self.requests_session.headers,
                **QueryServiceClient.filtered_headers(request_headers),
            }
            if request_headers
            else self.requests_session.headers,
        )
        if response.status_code not in (200, 201):  # pragma: no cover
            return MaterializationInfo(urls=[], output_tables=[])
        result = response.json()
        return MaterializationInfo(**result)

    def deactivate_materialization(
        self,
        node_name: str,
        materialization_name: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Deactivates the specified node materialization
        """
        response = self.requests_session.delete(
            f"/materialization/{node_name}/{materialization_name}/",
            headers={
                **self.requests_session.headers,
                **QueryServiceClient.filtered_headers(request_headers),
            }
            if request_headers
            else self.requests_session.headers,
        )
        if response.status_code not in (200, 201):  # pragma: no cover
            return MaterializationInfo(urls=[], output_tables=[])
        result = response.json()
        return MaterializationInfo(**result)

    def get_materialization_info(
        self,
        node_name: str,
        node_version: str,
        materialization_name: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Gets materialization info for the node and materialization config name.
        """
        response = self.requests_session.get(
            f"/materialization/{node_name}/{node_version}/{materialization_name}/",
            timeout=3,
            headers={
                **self.requests_session.headers,
                **QueryServiceClient.filtered_headers(request_headers),
            }
            if request_headers
            else self.requests_session.headers,
        )
        if response.status_code not in (200, 201):
            return MaterializationInfo(output_tables=[], urls=[])
        return MaterializationInfo(**response.json())

    def run_backfill(
        self,
        node_name: str,
        materialization_name: str,
        partitions: List[PartitionBackfill],
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """Kicks off a backfill with the given backfill spec"""
        response = self.requests_session.post(
            f"/materialization/run/{node_name}/{materialization_name}/",
            json=[partition.dict() for partition in partitions],
            headers={
                **self.requests_session.headers,
                **QueryServiceClient.filtered_headers(request_headers),
            }
            if request_headers
            else self.requests_session.headers,
            timeout=20,
        )
        if response.status_code not in (200, 201):
            return MaterializationInfo(output_tables=[], urls=[])  # pragma: no cover
        return MaterializationInfo(**response.json())
