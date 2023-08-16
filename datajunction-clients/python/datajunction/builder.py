"""DataJunction main client module."""

from http import HTTPStatus
from typing import List, Optional

from datajunction import models
from datajunction.client import DJClient
from datajunction.exceptions import DJClientException, DJNamespaceAlreadyExists
from datajunction.nodes import Cube, Dimension, Metric, Namespace, Source, Transform


class DJBuilder(DJClient):  # pylint: disable=too-many-public-methods
    """
    Client class for DJ dag and node modifications.
    """

    #
    # Namespace
    #
    def namespace(self, namespace: str) -> "Namespace":
        """
        Returns the specified node namespace.
        """
        namespaces = self.list_namespaces(prefix=namespace)
        if namespace not in namespaces:
            raise DJClientException(f"Namespace `{namespace}` does not exist.")
        return Namespace(namespace=namespace, dj_client=self)

    def create_namespace(self, namespace: str) -> "Namespace":
        """
        Create a namespace with a given name.
        """
        response = self._session.post(
            f"/namespaces/{namespace}/",
            timeout=self._timeout,
        )
        json_response = response.json()
        if response.status_code == 409:
            raise DJNamespaceAlreadyExists(json_response["message"])
        return Namespace(namespace=namespace, dj_client=self)

    def delete_namespace(self, namespace: str, cascade: bool = False) -> None:
        """
        Delete a namespace by name.
        """
        response = self._session.delete(
            f"/namespaces/{namespace}/",
            timeout=self._timeout,
            params={
                "cascade": cascade,
            },
        )
        if response.status_code != HTTPStatus.OK:
            raise DJClientException(response.json()["message"])

    def restore_namespace(self, namespace: str, cascade: bool = False) -> None:
        """
        Restore a namespace by name.
        """
        response = self._session.post(
            f"/namespaces/{namespace}/restore/",
            timeout=self._timeout,
            params={
                "cascade": cascade,
            },
        )
        if response.status_code != HTTPStatus.CREATED:
            raise DJClientException(response.json()["message"])

    #
    # Nodes: all
    #
    def delete_node(self, node_name: str) -> None:
        """
        Delete (aka deactivate) this node.
        """
        response = self._session.delete(
            f"/nodes/{node_name}/",
            timeout=self._timeout,
        )
        json_response = response.json()
        if not response.ok:
            raise DJClientException(
                f"Deleting node `{node_name}` failed: {json_response}",
            )  # pragma: no cover

    def restore_node(self, node_name: str) -> None:
        """
        Restore (aka reactivate) this node.
        """
        response = self._session.post(
            f"/nodes/{node_name}/restore/",
            timeout=self._timeout,
        )
        json_response = response.json()
        if not response.ok:
            raise DJClientException(
                f"Restoring node `{node_name}` failed: {json_response}",
            )  # pragma: no cover

    #
    # Nodes: SOURCE
    #
    def source(self, node_name: str) -> "Source":
        """
        Retrieves a source node with that name if one exists.
        """
        node_dict = self._verify_node_exists(
            node_name,
            type_=models.NodeType.SOURCE.value,
        )
        node = Source(
            **node_dict,
            dj_client=self,
        )
        node.primary_key = self._primary_key_from_columns(node_dict["columns"])
        return node

    def create_source(  # pylint: disable=too-many-arguments
        self,
        name: str,
        catalog: str,
        schema_: str,
        table: str,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        columns: Optional[List[models.Column]] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List[models.Tag]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
    ) -> "Source":
        """
        Creates a new Source node with given parameters.
        """
        new_node = Source(
            dj_client=self,
            name=name,
            description=description,
            display_name=display_name,
            tags=tags,
            primary_key=primary_key,
            catalog=catalog,
            schema_=schema_,
            table=table,
            columns=columns,
        )
        self._create_node(node=new_node, mode=mode)
        return new_node

    def register_table(self, catalog: str, schema: str, table: str) -> Source:
        """
        Register a table as a source node. This will create a source node under the configured
        `source_node_namespace` (a server-side setting), which defaults to the `source` namespace.
        """
        response = self._session.post(f"/register/table/{catalog}/{schema}/{table}/")
        new_node = Source(
            **response.json(),
            dj_client=self,
        )
        return new_node

    #
    # Nodes: TRANSFORM
    #
    def transform(self, node_name: str) -> "Transform":
        """
        Retrieves a transform node with that name if one exists.
        """
        node_dict = self._verify_node_exists(
            node_name,
            type_=models.NodeType.TRANSFORM.value,
        )
        node = Transform(
            **node_dict,
            dj_client=self,
        )
        node.primary_key = self._primary_key_from_columns(node_dict["columns"])
        return node

    def create_transform(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: str,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List[models.Tag]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
    ) -> "Transform":
        """
        Creates a new Transform node with given parameters.
        """
        new_node = Transform(
            dj_client=self,
            name=name,
            description=description,
            display_name=display_name,
            tags=tags,
            primary_key=primary_key,
            query=query,
        )
        self._create_node(node=new_node, mode=mode)
        return new_node

    #
    # Nodes: DIMENSION
    #
    def dimension(self, node_name: str) -> "Dimension":
        """
        Retrieves a Dimension node with that name if one exists.
        """
        node_dict = self._verify_node_exists(
            node_name,
            type_=models.NodeType.DIMENSION.value,
        )
        node = Dimension(
            **node_dict,
            dj_client=self,
        )
        node.primary_key = self._primary_key_from_columns(node_dict["columns"])
        return node

    def create_dimension(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: str,
        primary_key: Optional[List[str]],
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        tags: Optional[List[models.Tag]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
    ) -> "Transform":
        """
        Creates a new Dimension node with given parameters.
        """
        new_node = Dimension(
            dj_client=self,
            name=name,
            description=description,
            display_name=display_name,
            tags=tags,
            primary_key=primary_key,
            query=query,
        )
        self._create_node(node=new_node, mode=mode)
        return new_node

    #
    # Nodes: METRIC
    #
    def metric(self, node_name: str) -> "Metric":
        """
        Retrieves a Metric node with that name if one exists.
        """
        node_dict = self._verify_node_exists(
            node_name,
            type_=models.NodeType.METRIC.value,
        )
        node = Metric(
            **node_dict,
            dj_client=self,
        )
        node.primary_key = self._primary_key_from_columns(node_dict["columns"])
        return node

    def create_metric(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: str,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List[models.Tag]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
    ) -> "Transform":
        """
        Creates a new Metric node with given parameters.
        """
        new_node = Metric(
            dj_client=self,
            name=name,
            description=description,
            display_name=display_name,
            tags=tags,
            primary_key=primary_key,
            query=query,
        )
        self._create_node(node=new_node, mode=mode)
        return new_node

    #
    # Nodes: CUBE
    #
    def cube(self, node_name: str) -> "Cube":  # pragma: no cover
        """
        Retrieves a Cube node with that name if one exists.
        """
        node_dict = self._get_cube(node_name)
        if "name" not in node_dict:
            raise DJClientException(f"Cube `{node_name}` does not exist")
        dimensions = [
            f'{col["node_name"]}.{col["name"]}'
            for col in node_dict["cube_elements"]
            if col["type"] != "metric"
        ]
        metrics = [
            f'{col["node_name"]}.{col["name"]}'
            for col in node_dict["cube_elements"]
            if col["type"] == "metric"
        ]
        return Cube(
            **node_dict,
            metrics=metrics,
            dimensions=dimensions,
            dj_client=self,
        )

    def create_cube(  # pylint: disable=too-many-arguments
        self,
        name: str,
        metrics: List[str],
        dimensions: List[str],
        filters: Optional[List[str]] = None,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
    ) -> "Cube":
        """
        Instantiates a new cube with the given parameters.
        """
        new_node = Cube(  # pragma: no cover
            dj_client=self,
            name=name,
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            description=description,
            display_name=display_name,
        )
        self._create_node(node=new_node, mode=mode)  # pragma: no cover
        return new_node  # pragma: no cover
