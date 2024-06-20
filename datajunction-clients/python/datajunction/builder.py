"""DataJunction builder client module."""
# pylint: disable=protected-access
import re
from http import HTTPStatus
from typing import Dict, List, Optional, Type

from datajunction import models
from datajunction.client import DJClient
from datajunction.exceptions import (
    DJClientException,
    DJNamespaceAlreadyExists,
    DJTableAlreadyRegistered,
)
from datajunction.nodes import (
    Cube,
    Dimension,
    Metric,
    Namespace,
    Node,
    Source,
    Transform,
)
from datajunction.tags import Tag


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
        response = self._session.request(
            "DELETE",
            f"/namespaces/{namespace}/",
            timeout=self._timeout,
            params={
                "cascade": cascade,
            },
        )
        if not response.status_code < 400:
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
    def create_node(
        self,
        type_: models.NodeType,
        name: str,
        data: Dict,
        update_if_exists: bool = True,
    ) -> "Node":
        """
        Create or update a new node
        """

        node_type_to_class: dict[models.NodeType, Type[Node]] = {
            models.NodeType.METRIC: Metric,
            models.NodeType.CUBE: Cube,
            models.NodeType.TRANSFORM: Transform,
            models.NodeType.SOURCE: Source,
            models.NodeType.DIMENSION: Dimension,
        }

        data = {
            k: v
            for k, v in data.items()
            if k not in ["dj_client", "type"] and v is not None
        }

        try:
            existing_node_dict = self._get_node(name)
            if type_ == models.NodeType.CUBE.value:
                cube_dict = self._get_cube(name)
                # This check is for the unit tests, which don't raise an exception
                # for >= 400 status codes
                if "name" in cube_dict:
                    existing_node_dict["metrics"] = cube_dict["cube_node_metrics"]
                    existing_node_dict["dimensions"] = cube_dict["cube_node_dimensions"]
        except DJClientException as e:  # pragma: no cover # pytest fixture doesn't raise
            if re.search(r"node .* does not exist", str(e)):
                existing_node_dict = None
            else:
                raise

        node_cls = node_type_to_class[type_]
        # pylint: disable=fixme
        # TODO: checking for "name" in existing_node_dict is a workaround
        #   to accommodate pytest mock client, which return a error message dict (no "name")
        #   instead of raising like the real client.
        if existing_node_dict and "name" in existing_node_dict:
            # update
            if update_if_exists:
                existing_node = node_cls(dj_client=self, **existing_node_dict)
                new_node = existing_node.copy(update=data)
                # dj_client is an Pydantic-excluded field and doesn't survive .copy()
                #   so we need to set it again
                new_node.dj_client = self
                new_node._update_tags()
                new_node._update()
            else:
                raise DJClientException(
                    f"Node `{name}` already exists. "
                    f"Use `update_if_exists=True` to update the node.",
                )
        else:
            # create
            new_node = node_cls(dj_client=self, **data)
            self._create_node(node=new_node, mode=data.get("mode"))
        new_node.refresh()
        return new_node

    def delete_node(self, node_name: str) -> None:
        """
        Delete (aka deactivate) this node.
        """
        response = self._session.delete(
            f"/nodes/{node_name}/",
            timeout=self._timeout,
        )
        json_response = response.json()
        if not response.status_code < 400:
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
        if not response.status_code < 400:
            raise DJClientException(
                f"Restoring node `{node_name}` failed: {json_response}",
            )  # pragma: no cover

    #
    # Nodes: SOURCE
    #

    def create_source(  # pylint: disable=too-many-arguments
        self,
        name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        columns: Optional[List[models.Column]] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List[Tag]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
        update_if_exists: bool = True,
    ) -> "Source":
        """
        Creates a new Source node with given parameters.
        """

        return self.create_node(
            type_=models.NodeType.SOURCE,
            name=name,
            data={
                "name": name,
                "catalog": catalog,
                "schema_": schema,
                "table": table,
                "display_name": display_name,
                "description": description,
                "columns": columns,
                "primary_key": primary_key,
                "tags": tags,
                "mode": mode,
            },
            update_if_exists=update_if_exists,
        )

    def register_table(self, catalog: str, schema: str, table: str) -> Source:
        """
        Register a table as a source node. This will create a source node under the configured
        `source_node_namespace` (a server-side setting), which defaults to the `source` namespace.
        """
        try:
            response = self._session.post(
                f"/register/table/{catalog}/{schema}/{table}/",
            )
        except Exception as exc:
            if "409 Client Error" in str(exc):
                raise DJTableAlreadyRegistered(catalog, schema, table) from exc
            raise DJClientException(
                f"Failed to register table `{catalog}.{schema}.{table}`: {exc}",
            ) from exc
        source_node = Source(
            **response.json(),
            dj_client=self,
        )
        return source_node

    #
    # Nodes: TRANSFORM
    #
    def create_transform(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: Optional[str] = None,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List[Tag]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
        update_if_exists: bool = True,
    ) -> "Transform":
        """
        Creates or update a Transform node with given parameters.
        """
        return self.create_node(
            type_=models.NodeType.TRANSFORM,
            name=name,
            data={
                "name": name,
                "query": query,
                "description": description,
                "display_name": display_name,
                "primary_key": primary_key,
                "tags": tags,
                "mode": mode,
            },
            update_if_exists=update_if_exists,
        )

    #
    # Nodes: DIMENSION
    #
    def create_dimension(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        tags: Optional[List[Tag]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
        update_if_exists: bool = True,
    ) -> "Dimension":
        """
        Creates or update a Dimension node with given parameters.
        """
        return self.create_node(
            type_=models.NodeType.DIMENSION,
            name=name,
            data={
                "name": name,
                "query": query,
                "description": description,
                "display_name": display_name,
                "primary_key": primary_key,
                "tags": tags,
                "mode": mode,
            },
            update_if_exists=update_if_exists,
        )

    #
    # Nodes: METRIC
    #
    def create_metric(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: Optional[str] = None,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List[Tag]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
        update_if_exists: bool = True,
    ) -> "Metric":
        """
        Creates or update a Metric node with given parameters.
        """
        return self.create_node(
            type_=models.NodeType.METRIC,
            name=name,
            data={
                "name": name,
                "query": query,
                "description": description,
                "display_name": display_name,
                "primary_key": primary_key,
                "tags": tags,
                "mode": mode,
            },
            update_if_exists=update_if_exists,
        )

    #
    # Nodes: CUBE
    #
    def create_cube(  # pylint: disable=too-many-arguments
        self,
        name: str,
        metrics: Optional[List[str]] = None,
        dimensions: Optional[List[str]] = None,
        filters: Optional[List[str]] = None,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
        tags: Optional[List[Tag]] = None,
        update_if_exists: bool = True,
    ) -> "Cube":
        """
        Create or update a cube with the given parameters.
        """
        return self.create_node(
            type_=models.NodeType.CUBE,
            name=name,
            data={
                "name": name,
                "metrics": metrics,
                "dimensions": dimensions,
                "filters": filters or [],
                "description": description,
                "display_name": display_name,
                "mode": mode,
                "tags": tags,
            },
            update_if_exists=update_if_exists,
        )

    #
    # Tag
    #
    def create_tag(
        self,
        name: str,
        description: Optional[str],
        tag_metadata: Dict,
        tag_type: str,
    ) -> Tag:
        """
        Create a tag with a given name.
        """
        new_tag = Tag(
            dj_client=self,
            name=name,
            description=description,
            tag_type=tag_type,
            tag_metadata=tag_metadata,
        )
        self._create_tag(tag=new_tag)
        new_tag.refresh()
        return new_tag
