"""DataJunction base client setup."""
import abc

# pylint: disable=redefined-outer-name, import-outside-toplevel, too-many-lines, protected-access
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import requests

from datajunction import models
from datajunction._internal import ClientEntity
from datajunction.exceptions import DJClientException
from datajunction.tags import Tag

if TYPE_CHECKING:  # pragma: no cover
    from datajunction.client import DJClient


@dataclass
class Namespace(ClientEntity):  # pylint: disable=protected-access
    """
    Represents a namespace
    """

    namespace: str

    #
    # List
    #
    def nodes(self):
        """
        Retrieves all nodes under this namespace.
        """
        return self.dj_client._get_nodes_in_namespace(
            self.namespace,
        )

    def sources(self):
        """
        Retrieves source nodes under this namespace.
        """
        return self.dj_client._get_nodes_in_namespace(
            self.namespace,
            type_=models.NodeType.SOURCE,
        )

    def transforms(self):
        """
        Retrieves transform nodes under this namespace.
        """
        return self.dj_client._get_nodes_in_namespace(
            self.namespace,
            type_=models.NodeType.TRANSFORM,
        )

    def cubes(self):
        """
        Retrieves cubes under this namespace.
        """
        return self.dj_client._get_nodes_in_namespace(
            self.namespace,
            type_=models.NodeType.CUBE,
        )


@dataclass
class Node(ClientEntity):  # pylint: disable=too-many-instance-attributes
    """
    Represents a DJ node object
    """

    name: str
    type: str
    description: Optional[str] = None
    mode: Optional[models.NodeMode] = None
    status: Optional[str] = None
    display_name: Optional[str] = None
    availability: Optional[models.AvailabilityState] = None
    tags: Optional[List[Tag]] = None
    primary_key: Optional[List[str]] = None
    materializations: Optional[List[Dict[str, Any]]] = None
    version: Optional[str] = None
    deactivated_at: Optional[int] = None
    current_version: Optional[str] = None
    columns: Optional[List[models.Column]] = None
    query: Optional[str] = None

    def to_dict(self, exclude: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Convert the source node to a dictionary. We need to make this method because
        the default asdict() method from dataclasses does not handle nested dataclasses.
        """
        dict_ = {
            "name": self.name,
            "type": self.type,
            "description": self.description,
            "mode": self.mode,
            "status": self.status,
            "display_name": self.display_name,
            "availability": self.availability,
            "tags": [
                # asdict() is not used to avoid dataclasses circular serialization error
                tag.to_dict() if isinstance(tag, Tag) else tag
                for tag in self.tags
            ]
            if self.tags
            else None,
            "primary_key": self.primary_key,
            "materializations": self.materializations,
            "version": self.version,
            "deactivated_at": self.deactivated_at,
            "current_version": self.current_version,
            "columns": [
                asdict(col) if isinstance(col, models.Column) else col
                for col in self.columns
            ]
            if self.columns
            else None,
            "query": self.query if hasattr(self, "query") else None,
        }
        exclude = exclude + self.exclude if exclude else self.exclude
        dict_ = {k: v for k, v in dict_.items() if k not in exclude}
        return dict_

    #
    # Node level actions
    #
    def list_revisions(self) -> List[dict]:
        """
        List all revisions of this node
        """
        return self.dj_client._get_node_revisions(self.name)

    @abc.abstractmethod
    def _update(self) -> requests.Response:
        """
        Update the node for fields that have changed.
        """

    def _update_tags(self) -> None:
        """
        Update the tags on a node
        """
        response = self.dj_client._update_node_tags(
            node_name=self.name,
            tags=[
                tag.name
                if isinstance(tag, Tag)
                else tag["name"]
                if isinstance(tag, dict)
                else None
                for tag in self.tags
            ]
            if self.tags
            else [],
        )
        if not response.status_code < 400:  # pragma: no cover
            raise DJClientException(
                f"Error updating tags for node {self.name}, {self.tags}",
            )

    def save(self, mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED) -> dict:
        """
        Saves the node to DJ, whether it existed before or not.
        """
        existing_node = self.dj_client._get_node(node_name=self.name)
        if "name" in existing_node:
            # update
            self._update_tags()
            response = self._update()
            if not response.status_code < 400:  # pragma: no cover
                raise DJClientException(
                    f"Error updating node `{self.name}`: {response.text}",
                )
            self.refresh()
        else:
            # create
            response = self.dj_client._create_node(
                node=self,
                mode=mode,
            )  # pragma: no cover
            if not response.status_code < 400:  # pragma: no cover
                raise DJClientException(
                    f"Error creating new node `{self.name}`: {response.text}",
                )
            self._update_tags()
            self.refresh()

        return response.json()

    def refresh(self):
        """
        Refreshes a node with its latest version from the database.
        """
        refreshed_node = self.dj_client._get_node(self.name)
        for key, value in refreshed_node.items():
            if hasattr(self, key):
                setattr(self, key, value)
        return self

    def delete(self):
        """
        Deletes the node (softly). We still keep it in an inactive state.
        """
        return self.dj_client.delete_node(self.name)

    def restore(self):
        """
        Restores (aka reactivates) the node.
        """
        return self.dj_client.restore_node(self.name)

    #
    # Node attributes level actions
    #
    def link_dimension(
        self,
        column: str,
        dimension: str,
        dimension_column: Optional[str] = None,
    ):
        """
        Links the dimension to this node via the node's `column` and the dimension's
        `dimension_column`. If no `dimension_column` is provided, the dimension's
        primary key will be used automatically.
        """
        link_response = self.dj_client._link_dimension_to_node(
            self.name,
            column,
            dimension,
            dimension_column,
        )
        self.refresh()
        return link_response

    def link_complex_dimension(  # pylint: disable=too-many-arguments
        self,
        dimension_node: str,
        join_type: Optional[str] = None,
        *,
        join_on: str,
        join_cardinality: Optional[str] = None,
        role: Optional[str] = None,
    ):
        """
        Links the dimension to this node via the specified join SQL.
        """
        link_response = self.dj_client._link_complex_dimension_to_node(
            node_name=self.name,
            dimension_node=dimension_node,
            join_type=join_type,
            join_on=join_on,
            join_cardinality=join_cardinality,
            role=role,
        )
        self.refresh()
        return link_response

    def unlink_dimension(
        self,
        column: str,
        dimension: str,
        dimension_column: Optional[str],
    ):
        """
        Removes the dimension link on the node's `column` to the dimension.
        """
        link_response = self.dj_client._unlink_dimension_from_node(  # pylint: disable=protected-access
            self.name,
            column,
            dimension,
            dimension_column,
        )
        self.refresh()
        return link_response

    def remove_complex_dimension_link(
        self,
        dimension_node: str,
        role: Optional[str] = None,
    ):
        """
        Removes a complex dimension link from this node
        """
        unlink_response = self.dj_client._remove_complex_dimension_link(
            node_name=self.name,
            dimension_node=dimension_node,
            role=role,
        )
        self.refresh()
        return unlink_response

    def add_materialization(self, config: models.Materialization):
        """
        Adds a materialization for the node. This will not work for source nodes
        as they don't need to be materialized.
        """
        upsert_response = self.dj_client._upsert_materialization(
            self.name,
            config,
        )
        self.refresh()
        return upsert_response

    def deactivate_materialization(self, materialization_name: str):
        """
        Deactivate a materialization for the node.
        """
        response = self.dj_client._deactivate_materialization(
            self.name,
            materialization_name,
        )
        self.refresh()
        return response

    def add_availability(self, availability: models.AvailabilityState):
        """
        Adds an availability state to the node
        """
        return self.dj_client._add_availability_state(self.name, availability)

    def set_column_attributes(
        self,
        column_name,
        attributes: List[models.ColumnAttribute],
    ):
        """
        Sets attributes for columns on the node
        """
        return self.dj_client._set_column_attributes(self.name, column_name, attributes)


@dataclass
class Source(Node):
    """
    DJ source node
    """

    type: str = "source"
    catalog: Optional[str] = None
    schema_: Optional[str] = None
    table: Optional[str] = None

    def to_dict(self, exclude: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Convert the source node to a dictionary
        """
        dict_ = super().to_dict(exclude=exclude)
        dict_["catalog"] = self.catalog
        dict_["schema_"] = self.schema_
        dict_["table"] = self.table
        return dict_

    @classmethod
    def from_dict(cls, dj_client: "DJClient", data: Dict[str, Any]) -> "Source":
        """
        Create a new source node from a dictionary
        """
        return cls(
            dj_client=dj_client,
            name=data["name"],
            description=data.get("description"),
            mode=data.get("mode"),
            status=data.get("status"),
            display_name=data.get("display_name"),
            availability=data.get("availability"),
            tags=data.get("tags"),
            primary_key=data.get("primary_key"),
            materializations=data.get("materializations"),
            version=data.get("version"),
            deactivated_at=data.get("deactivated_at"),
            current_version=data.get("current_version"),
            catalog=data.get("catalog"),
            schema_=data.get("schema_"),
            table=data.get("table"),
            columns=data.get("columns"),
        )

    def __post_init__(self):
        """
        When `catalog` is a dictionary, parse out the catalog's
        name, otherwise just return the string.
        """
        if self.catalog:
            if isinstance(self.catalog, dict):
                self.catalog = self.catalog["name"]

    def _update(self) -> requests.Response:
        """
        Update the node for fields that have changed
        """
        update_node = models.UpdateNode(
            display_name=self.display_name,
            description=self.description,
            mode=self.mode,
            primary_key=self.primary_key,
            columns=self.columns,
        )
        return self.dj_client._update_node(self.name, update_node)

    def validate(self) -> str:
        """
        This method is only for Source nodes.

        It will compare the source node metadata and create a new revision if necessary.
        """
        response = self.dj_client._refresh_source_node(self.name)
        self.refresh()
        return response["status"]


@dataclass
class NodeWithQuery(Node):
    """
    Nodes with query attribute
    """

    query: str = ""

    def to_dict(self, exclude: Optional[List[str]] = None) -> Dict[str, Any]:
        dict_ = super().to_dict(exclude=exclude)
        dict_["query"] = self.query
        return dict_

    def _update(self) -> requests.Response:
        """
        Update the node for fields that have changed.
        """
        update_node = models.UpdateNode(
            display_name=self.display_name,
            description=self.description,
            mode=self.mode,
            primary_key=self.primary_key,
            query=self.query,
        )
        return self.dj_client._update_node(self.name, update_node)

    def validate(self) -> str:
        """
        Check if the node is valid by calling the /validate endpoint.

        For source nodes, see the Source.validate() method.
        """
        validation = self.dj_client._validate_node(self)
        return validation["status"]

    def publish(self) -> bool:
        """
        Change a node's mode to published
        """
        self.dj_client._publish_node(
            self.name,
            models.UpdateNode(mode=models.NodeMode.PUBLISHED),
        )
        return True

    def get_upstreams(self) -> List[str]:
        """
        Lists the upstream nodes of this node
        """
        return [node["name"] for node in self.dj_client._get_node_upstreams(self.name)]

    def get_downstreams(self) -> List[str]:
        """
        Lists the downstream nodes of this node
        """
        return [
            node["name"] for node in self.dj_client._get_node_downstreams(self.name)
        ]

    def get_dimensions(self) -> List[str]:
        """
        Lists dimensions available for the node
        """
        return self.dj_client._get_node_dimensions(self.name)


@dataclass
class Transform(NodeWithQuery):
    """
    DJ transform node
    """

    type: str = "transform"
    # columns: Optional[List[models.Column]] = None

    @classmethod
    def from_dict(cls, dj_client: "DJClient", data: Dict[str, Any]) -> "Transform":
        """
        Create a new transform node from a dictionary
        """
        return cls(
            dj_client=dj_client,
            name=data["name"],
            description=data.get("description"),
            mode=data.get("mode"),
            status=data.get("status"),
            display_name=data.get("display_name"),
            availability=data.get("availability"),
            tags=data.get("tags"),
            primary_key=data.get("primary_key"),
            materializations=data.get("materializations"),
            version=data.get("version"),
            deactivated_at=data.get("deactivated_at"),
            current_version=data.get("current_version"),
            query=data["query"],
            columns=data.get("columns"),
        )


@dataclass
class Metric(NodeWithQuery):
    """
    DJ metric node
    """

    type: str = "metric"
    required_dimensions: Optional[List[str]] = None
    metric_metadata: Optional[models.MetricMetadata] = None

    def to_dict(self, exclude: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Convert the source node to a dictionary
        """
        dict_ = super().to_dict(exclude=exclude)
        dict_["required_dimensions"] = self.required_dimensions
        dict_["metric_metadata"] = (
            asdict(self.metric_metadata) if self.metric_metadata else None
        )
        return dict_

    @classmethod
    def from_dict(cls, dj_client: "DJClient", data: Dict[str, Any]) -> "Metric":
        """
        Create a new metric node from a dictionary
        """
        return cls(
            dj_client=dj_client,
            name=data["name"],
            description=data.get("description"),
            mode=data.get("mode"),
            status=data.get("status"),
            display_name=data.get("display_name"),
            availability=data.get("availability"),
            tags=data.get("tags"),
            primary_key=data.get("primary_key"),
            materializations=data.get("materializations"),
            version=data.get("version"),
            deactivated_at=data.get("deactivated_at"),
            current_version=data.get("current_version"),
            query=data["query"],
            required_dimensions=data.get("required_dimensions"),
            metric_metadata=data.get("metric_metadata"),
        )

    def dimensions(self):
        """
        Returns the available dimensions for this metric.
        """
        metric = self.dj_client.get_metric(self.name)
        return metric["dimensions"]

    def _update(self) -> requests.Response:
        """
        Update the node for fields that have changed.
        """
        update_node = models.UpdateNode(
            display_name=self.display_name,
            description=self.description,
            mode=self.mode,
            primary_key=self.primary_key,
            query=self.query,
            required_dimensions=self.required_dimensions,
            metric_metadata=self.metric_metadata,
        )
        return self.dj_client._update_node(self.name, update_node)


@dataclass
class Dimension(NodeWithQuery):
    """
    DJ dimension node
    """

    type: str = "dimension"
    # columns: Optional[List[models.Column]] = None

    @classmethod
    def from_dict(cls, dj_client: "DJClient", data: Dict[str, Any]) -> "Dimension":
        """
        Create a new dimension node from a dictionary
        """
        return cls(
            dj_client=dj_client,
            name=data["name"],
            description=data.get("description"),
            mode=data.get("mode"),
            status=data.get("status"),
            display_name=data.get("display_name"),
            availability=data.get("availability"),
            tags=data.get("tags"),
            primary_key=data.get("primary_key"),
            materializations=data.get("materializations"),
            version=data.get("version"),
            deactivated_at=data.get("deactivated_at"),
            current_version=data.get("current_version"),
            query=data["query"],
            columns=data.get("columns"),
        )

    def linked_nodes(self):
        """
        Find all nodes linked to this dimension
        """
        return [
            node["name"]
            for node in self.dj_client._find_nodes_with_dimension(self.name)
        ]


@dataclass
class Cube(Node):  # pylint: disable=abstract-method
    """
    DJ cube node
    """

    type: str = "cube"
    metrics: Optional[List[str]] = None
    dimensions: Optional[List[str]] = None
    filters: Optional[List[str]] = None

    def to_dict(self, exclude: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Convert the source node to a dictionary
        """
        dict_ = super().to_dict(exclude=exclude)
        dict_["metrics"] = self.metrics
        dict_["dimensions"] = self.dimensions
        dict_["filters"] = self.filters
        return dict_

    @classmethod
    def from_dict(cls, dj_client: "DJClient", data: Dict[str, Any]) -> "Cube":
        """
        Create a new cube node from a dictionary
        """
        return cls(
            dj_client=dj_client,
            name=data["name"],
            description=data.get("description"),
            mode=data.get("mode"),
            status=data.get("status"),
            display_name=data.get("display_name"),
            availability=data.get("availability"),
            tags=data.get("tags"),
            primary_key=data.get("primary_key"),
            materializations=data.get("materializations"),
            version=data.get("version"),
            deactivated_at=data.get("deactivated_at"),
            current_version=data.get("current_version"),
            query=data.get("query"),
            metrics=data.get("metrics"),
            dimensions=data.get("dimensions"),
            filters=data.get("filters"),
            columns=data.get("columns"),
        )

    def _update(self):  # pragma: no cover
        update_node = models.UpdateNode(
            description=self.description,
            mode=self.mode,
            metrics=self.metrics,
            dimensions=self.dimensions,
            filters=self.filters,
        )
        return self.dj_client._update_node(self.name, update_node)
