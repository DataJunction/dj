"""Clients for various configurable services."""
from typing import TYPE_CHECKING, List, Optional, Union
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from datajunction_server.errors import DJQueryServiceClientException
from datajunction_server.models.column import Column
from datajunction_server.models.materialization import (
    DruidMaterializationInput,
    GenericMaterializationInput,
    MaterializationInfo,
)
from datajunction_server.models.partition import PartitionBackfill
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.sql.parsing.types import ColumnType

if TYPE_CHECKING:
    from datajunction_server.models.engine import Engine


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

    def get_columns_for_table(
        self,
        catalog: str,
        schema: str,
        table: str,
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
        )
        table_columns = response.json()["columns"]
        return [
            Column(name=column["name"], type=ColumnType(column["type"]))
            for column in table_columns
        ]

    def submit_query(  # pylint: disable=too-many-arguments
        self,
        query_create: QueryCreate,
    ) -> QueryWithResults:
        """
        Submit a query to the query service
        """
        response = self.requests_session.post(
            "/queries/",
            json=query_create.dict(),
        )
        response_data = response.json()
        if not response.ok:
            raise DJQueryServiceClientException(
                message=f"Error response from query service: {response_data['message']}",
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
        if not response.ok:
            raise DJQueryServiceClientException(
                message=f"Error response from query service: {response.text}",
            )
        query_info = response.json()
        return QueryWithResults(**query_info)

    def materialize(  # pylint: disable=too-many-arguments
        self,
        materialization_input: Union[
            GenericMaterializationInput,
            DruidMaterializationInput,
        ],
    ) -> MaterializationInfo:
        """
        Post a request to the query service asking it to set up a scheduled materialization
        for the node. The query service is expected to manage all reruns of this job. Note
        that this functionality may be moved to the materialization service at a later point.
        """
        response = self.requests_session.post(
            "/materialization/",
            json=materialization_input.dict(),
        )
        if not response.ok:  # pragma: no cover
            return MaterializationInfo(urls=[], output_tables=[])
        result = response.json()
        return MaterializationInfo(**result)

    def deactivate_materialization(
        self,
        node_name: str,
        materialization_name: str,
    ) -> MaterializationInfo:
        """
        Deactivates the specified node materialization
        """
        response = self.requests_session.delete(
            "/materialization/",
            params={
                "node_name": node_name,
                "materialization_name": materialization_name,
            },
        )
        if not response.ok:  # pragma: no cover
            return MaterializationInfo(urls=[], output_tables=[])
        result = response.json()
        return MaterializationInfo(**result)

    def get_materialization_info(
        self,
        node_name: str,
        node_version: str,
        materialization_name: str,
    ) -> MaterializationInfo:
        """
        Gets materialization info for the node and materialization config name.
        """
        response = self.requests_session.get(
            f"/materialization/{node_name}/{node_version}/{materialization_name}/",
            timeout=3,
        )
        if not response.ok:
            return MaterializationInfo(output_tables=[], urls=[])
        return MaterializationInfo(**response.json())

    def run_backfill(
        self,
        node_name: str,
        materialization_name: str,
        backfill: PartitionBackfill,
    ) -> MaterializationInfo:
        """Kicks off a backfill with the given backfill spec"""
        response = self.requests_session.post(
            f"/materialization/run/{node_name}/{materialization_name}/",
            json=backfill.dict(),
            timeout=20,
        )
        if not response.ok:
            return MaterializationInfo(output_tables=[], urls=[])  # pragma: no cover
        return MaterializationInfo(**response.json())
