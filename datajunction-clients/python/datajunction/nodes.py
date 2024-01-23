"""DataJunction base client setup."""
import abc

# pylint: disable=redefined-outer-name, import-outside-toplevel, too-many-lines, protected-access
from typing import Any, Dict, List, Optional, Union

from pydantic import validator

from datajunction import models
from datajunction._internal import ClientEntity
from datajunction.exceptions import DJClientException
from datajunction.tags import TagInfo


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


class Node(ClientEntity):  # pylint: disable=protected-access
    """
    Represents a DJ node object
    """

    name: str
    description: Optional[str]
    type: str
    mode: Optional[models.NodeMode]
    status: Optional[str] = None
    display_name: Optional[str]
    availability: Optional[models.AvailabilityState]
    tags: Optional[List[TagInfo]] = None
    primary_key: Optional[List[str]]
    materializations: Optional[List[Dict[str, Any]]]
    version: Optional[str]
    deactivated_at: Optional[int]
    current_version: Optional[str]

    #
    # Node level actions
    #
    def list_revisions(self) -> List[dict]:
        """
        List all revisions of this node
        """
        return self.dj_client._get_node_revisions(self.name)

    @abc.abstractmethod
    def _update(self) -> "Node":
        """
        Update the node for fields that have changed.
        """

    def _update_tags(self) -> None:
        """
        Update the tags on a node
        """
        response = self.dj_client._update_node_tags(
            node_name=self.name,
            tags=[tag.name for tag in self.tags] if self.tags else None,
        )
        if not response.ok:  # pragma: no cover
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
            if not response.ok:  # pragma: no cover
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
            if not response.ok:  # pragma: no cover
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


class Source(Node):
    """
    DJ source node
    """

    type: str = "source"
    catalog: str
    schema_: str
    table: str
    columns: Optional[List[models.Column]]

    @validator("catalog", pre=True)
    def parse_cls(  # pylint: disable=no-self-argument
        cls,
        value: Union[str, Dict[str, Any]],
    ) -> str:
        """
        When `catalog` is a dictionary, parse out the catalog's
        name, otherwise just return the string.
        """
        if isinstance(value, str):
            return value
        return value["name"]

    def _update(self) -> "Node":
        """
        Update the node for fields that have changed
        """
        update_node = models.UpdateNode(
            display_name=self.display_name,
            description=self.description,
            mode=self.mode,
            primary_key=self.primary_key,
            catalog=self.catalog,
            schema_=self.schema_,
            table=self.table,
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


class NodeWithQuery(Node):
    """
    Nodes with query attribute
    """

    query: str

    def _update(self) -> "Node":
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


class Transform(NodeWithQuery):
    """
    DJ transform node
    """

    type: str = "transform"
    columns: Optional[List[models.Column]]


class Metric(NodeWithQuery):
    """
    DJ metric node
    """

    type: str = "metric"
    columns: Optional[List[models.Column]]

    def dimensions(self):
        """
        Returns the available dimensions for this metric.
        """
        metric = self.dj_client.get_metric(self.name)
        return metric["dimensions"]


class Dimension(NodeWithQuery):
    """
    DJ dimension node
    """

    type: str = "dimension"
    query: str
    columns: Optional[List[models.Column]]

    def linked_nodes(self):
        """
        Find all nodes linked to this dimension
        """
        return [
            node["name"]
            for node in self.dj_client._find_nodes_with_dimension(self.name)
        ]


class Cube(Node):  # pylint: disable=abstract-method
    """
    DJ cube node
    """

    type: str = "cube"
    query: Optional[str] = None
    metrics: List[str]
    dimensions: List[str]
    filters: Optional[List[str]]
    columns: Optional[List[models.Column]]

    def _update(self):  # pragma: no cover
        update_node = models.UpdateNode(
            description=self.description,
            mode=self.mode,
            metrics=self.metrics,
            dimensions=self.dimensions,
            filters=self.filters,
            columns=self.columns,
        )
        return self.dj_client._update_node(self.name, update_node)
