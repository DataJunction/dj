"""HTTP query service client - wrapper around the original QueryServiceClient."""

from typing import TYPE_CHECKING, Dict, List, Optional, Union

from datajunction_server.database.column import Column
from datajunction_server.models.cube_materialization import (
    DruidCubeMaterializationInput,
)
from datajunction_server.models.materialization import (
    DruidMaterializationInput,
    GenericMaterializationInput,
    MaterializationInfo,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import PartitionBackfill
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.query_clients.base import BaseQueryServiceClient
from datajunction_server.service_clients import QueryServiceClient

if TYPE_CHECKING:
    from datajunction_server.database.engine import Engine


class HttpQueryServiceClient(BaseQueryServiceClient):
    """
    HTTP-based query service client that wraps the original QueryServiceClient.

    This maintains backward compatibility with the existing HTTP query service pattern.
    All methods are simple pass-throughs to the underlying QueryServiceClient.
    """

    def __init__(self, uri: str, retries: int = 0):
        """
        Initialize the HTTP query service client.

        Args:
            uri: URI of the query service
            retries: Number of retries for failed requests
        """
        self.uri = uri
        self._client = QueryServiceClient(uri=uri, retries=retries)

    def get_columns_for_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        request_headers: Optional[Dict[str, str]] = None,
        engine: Optional["Engine"] = None,
    ) -> List[Column]:
        """Retrieves columns for a table via HTTP query service."""
        return self._client.get_columns_for_table(
            catalog=catalog,
            schema=schema,
            table=table,
            request_headers=request_headers,
            engine=engine,
        )

    def create_view(
        self,
        view_name: str,
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> str:
        """Re-create a view using the HTTP query service."""
        return self._client.create_view(
            view_name=view_name,
            query_create=query_create,
            request_headers=request_headers,
        )

    def submit_query(
        self,
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> QueryWithResults:
        """Submit a query to the HTTP query service."""
        return self._client.submit_query(
            query_create=query_create,
            request_headers=request_headers,
        )

    def get_query(
        self,
        query_id: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> QueryWithResults:
        """Get a previously submitted query from the HTTP query service."""
        return self._client.get_query(
            query_id=query_id,
            request_headers=request_headers,
        )

    def materialize(
        self,
        materialization_input: Union[
            GenericMaterializationInput,
            DruidMaterializationInput,
        ],
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """Set up a scheduled materialization via HTTP query service."""
        return self._client.materialize(
            materialization_input=materialization_input,
            request_headers=request_headers,
        )

    def materialize_cube(
        self,
        materialization_input: DruidCubeMaterializationInput,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """Set up a scheduled cube materialization via HTTP query service."""
        return self._client.materialize_cube(
            materialization_input=materialization_input,
            request_headers=request_headers,
        )

    def deactivate_materialization(
        self,
        node_name: str,
        materialization_name: str,
        node_version: str | None = None,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """Deactivate materialization via HTTP query service."""
        return self._client.deactivate_materialization(
            node_name=node_name,
            materialization_name=materialization_name,
            node_version=node_version,
            request_headers=request_headers,
        )

    def get_materialization_info(
        self,
        node_name: str,
        node_version: str,
        node_type: NodeType,
        materialization_name: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """Get materialization info via HTTP query service."""
        return self._client.get_materialization_info(
            node_name=node_name,
            node_version=node_version,
            node_type=node_type,
            materialization_name=materialization_name,
            request_headers=request_headers,
        )

    def run_backfill(
        self,
        node_name: str,
        node_version: str,
        node_type: NodeType,
        materialization_name: str,
        partitions: List[PartitionBackfill],
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """Run backfill via HTTP query service."""
        return self._client.run_backfill(
            node_name=node_name,
            node_version=node_version,
            node_type=node_type,
            materialization_name=materialization_name,
            partitions=partitions,
            request_headers=request_headers,
        )
