"""Base abstract class for query service clients."""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from datajunction_server.database.column import Column
from datajunction_server.models.cube_materialization import (
    DruidCubeMaterializationInput,
)
from datajunction_server.models.materialization import (
    DruidMaterializationInput,
    GenericMaterializationInput,
    MaterializationInfo,
)
from datajunction_server.models.preaggregation import PreAggMaterializationInput
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import PartitionBackfill
from datajunction_server.models.query import QueryCreate, QueryWithResults

if TYPE_CHECKING:
    from datajunction_server.database.engine import Engine
    from datajunction_server.models.preaggregation import (
        BackfillInput,
        CubeBackfillInput,
    )

_logger = logging.getLogger(__name__)


class BaseQueryServiceClient(ABC):
    """
    Abstract base class for query service clients.

    This class defines the interface that all query service clients must implement.
    Custom implementations can selectively implement only the methods they need.
    """

    @abstractmethod
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

        Args:
            catalog: The catalog name
            schema: The schema name
            table: The table name
            request_headers: Optional HTTP headers
            engine: Optional engine for context

        Returns:
            List of Column objects
        """
        pass

    def create_view(
        self,
        view_name: str,
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Re-create a view using the query service.

        Default implementation raises NotImplementedError.
        Override in subclasses that support view creation.

        Args:
            view_name: Name of the view to create
            query_create: Query creation parameters
            request_headers: Optional HTTP headers

        Returns:
            Success message string
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support view creation",
        )

    def submit_query(
        self,
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> QueryWithResults:
        """
        Submit a query to the query service.

        Default implementation raises NotImplementedError.
        Override in subclasses that support query submission.

        Args:
            query_create: Query creation parameters
            request_headers: Optional HTTP headers

        Returns:
            QueryWithResults containing query results
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support query submission",
        )

    def get_query(
        self,
        query_id: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> QueryWithResults:
        """
        Get a previously submitted query.

        Default implementation raises NotImplementedError.
        Override in subclasses that support query retrieval.

        Args:
            query_id: ID of the query to retrieve
            request_headers: Optional HTTP headers

        Returns:
            QueryWithResults containing query results
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support query retrieval",
        )

    def materialize(
        self,
        materialization_input: Union[
            GenericMaterializationInput,
            DruidMaterializationInput,
        ],
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Post a request to set up a scheduled materialization.

        Default implementation raises NotImplementedError.
        Override in subclasses that support materialization.

        Args:
            materialization_input: Materialization configuration
            request_headers: Optional HTTP headers

        Returns:
            MaterializationInfo with materialization details
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support materialization",
        )

    def materialize_cube(
        self,
        materialization_input: DruidCubeMaterializationInput,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Post a request to set up a scheduled cube materialization.

        Default implementation raises NotImplementedError.
        Override in subclasses that support cube materialization.

        Args:
            materialization_input: Cube materialization configuration
            request_headers: Optional HTTP headers

        Returns:
            MaterializationInfo with materialization details
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support cube materialization",
        )

    def materialize_preagg(
        self,
        materialization_input: PreAggMaterializationInput,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Create/update a scheduled workflow for a pre-aggregation materialization.

        This creates or updates the recurring workflow that materializes the pre-agg
        on the configured schedule.

        Default implementation raises NotImplementedError.
        Override in subclasses that support pre-aggregation materialization.

        Args:
            materialization_input: Pre-aggregation materialization configuration
            request_headers: Optional HTTP headers

        Returns:
            Dict with 'workflow_url', 'status', and optionally other details
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support pre-aggregation materialization",
        )

    def deactivate_preagg_workflow(
        self,
        output_table: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Deactivate a pre-aggregation's workflows by output table name."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support pre-aggregation workflows",
        )

    def run_preagg_backfill(
        self,
        backfill_input: "BackfillInput",
        request_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Run a backfill for a pre-aggregation."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support pre-aggregation backfill",
        )

    def run_cube_backfill(
        self,
        backfill_input: "CubeBackfillInput",
        request_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Run a backfill for a cube."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support cube backfill",
        )

    def refresh_cube_materialization(
        self,
        cube_name: str,
        cube_version: Optional[str] = None,
        materializations: Optional[List[Dict[str, Any]]] = None,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Refresh/rebuild materialization workflows for a cube without creating a new version.

        Default implementation raises NotImplementedError.
        Override in subclasses that support cube materialization refresh.

        Args:
            cube_name: Name of the cube node
            cube_version: Optional cube version
            materializations: List of active materialization dicts to rebuild
            request_headers: Optional HTTP headers

        Returns:
            MaterializationInfo with details of refreshed workflows
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support cube materialization refresh",
        )

    def deactivate_materialization(
        self,
        node_name: str,
        materialization_name: str,
        node_version: str | None = None,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Deactivates the specified node materialization.

        Default implementation raises NotImplementedError.
        Override in subclasses that support materialization deactivation.

        Args:
            node_name: Name of the node
            materialization_name: Name of the materialization
            node_version: Optional version of the node
            request_headers: Optional HTTP headers

        Returns:
            MaterializationInfo with deactivation details
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support materialization deactivation",
        )

    def get_materialization_info(
        self,
        node_name: str,
        node_version: str,
        node_type: NodeType,
        materialization_name: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Gets materialization info for the node and materialization config name.

        Default implementation raises NotImplementedError.
        Override in subclasses that support materialization info retrieval.

        Args:
            node_name: Name of the node
            node_version: Version of the node
            node_type: Type of the node
            materialization_name: Name of the materialization
            request_headers: Optional HTTP headers

        Returns:
            MaterializationInfo with materialization details
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support materialization info retrieval",
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
        """
        Kicks off a backfill with the given backfill spec.

        Default implementation raises NotImplementedError.
        Override in subclasses that support backfill operations.

        Args:
            node_name: Name of the node
            node_version: Version of the node
            node_type: Type of the node
            materialization_name: Name of the materialization
            partitions: List of partition backfill specifications
            request_headers: Optional HTTP headers

        Returns:
            MaterializationInfo with backfill details
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support backfill operations",
        )
