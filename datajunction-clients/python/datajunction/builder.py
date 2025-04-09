"""DataJunction builder client module."""

# pylint: disable=protected-access
import re
from dataclasses import fields
from http import HTTPStatus
from typing import TYPE_CHECKING, Dict, List, Optional

from datajunction import models
from datajunction.client import DJClient
from datajunction.exceptions import (
    DJClientException,
    DJNamespaceAlreadyExists,
    DJTableAlreadyRegistered,
    DJTagAlreadyExists,
    DJViewAlreadyRegistered,
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

if TYPE_CHECKING:
    from datajunction.compile import ColumnYAML  # pragma: no cover


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

    def create_namespace(
        self,
        namespace: str,
        skip_if_exists: bool = False,
    ) -> "Namespace":
        """
        Create a namespace with a given name.
        """
        response = self._session.post(
            f"/namespaces/{namespace}/",
            timeout=self._timeout,
        )
        json_response = response.json()
        if response.status_code == 409 and not skip_if_exists:
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
    def make_node_of_type(
        self,
        type_: models.NodeType,
        data: Dict,
    ):
        """
        Make a new node of the given type.
        """
        # common arguments
        common_args = [
            field.name
            for field in fields(Node)
            if field.name not in ["dj_client", "type"]
        ]

        def type_to_class(type_: str):
            if type_ == models.NodeType.SOURCE:
                return Source
            if type_ == models.NodeType.METRIC:
                return Metric
            if type_ == models.NodeType.DIMENSION:
                return Dimension
            if type_ == models.NodeType.TRANSFORM:
                return Transform
            if type_ == models.NodeType.CUBE:
                return Cube
            raise DJClientException(f"Unknown node type: {type_}")  # pragma: no cover

        class_ = type_to_class(type_)

        args = common_args + [
            field.name for field in fields(class_) if field.name not in common_args
        ]
        data_ = {k: v for k, v in data.items() if k in args}
        return class_(dj_client=self, **data_)

    def create_node(
        self,
        type_: models.NodeType,
        name: str,
        data: Dict,
        update_if_exists: bool = False,
    ):
        """
        Create or update a new node
        """

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
        except (
            DJClientException
        ) as e:  # pragma: no cover # pytest fixture doesn't raise
            if re.search(r"node .* does not exist", str(e)):
                existing_node_dict = None
            else:
                raise

        # Checking for "name" in existing_node_dict is a workaround
        #   to accommodate pytest mock client, which return a error message dict (no "name")
        #   instead of raising like the real client.
        if existing_node_dict and "name" in existing_node_dict:
            # update
            if update_if_exists:
                existing_node_dict.update(data)
                new_node = self.make_node_of_type(
                    type_=type_,
                    data=existing_node_dict,
                )
                new_node._update_tags()
                new_node._update()
            else:
                raise DJClientException(
                    f"Node `{name}` already exists. "
                    f"Use `update_if_exists=True` to update the node.",
                )
        else:
            # create
            new_node = self.make_node_of_type(type_=type_, data=data)
            response = self._create_node(node=new_node, mode=data.get("mode"))
            if not response.status_code < 400:
                raise DJClientException(
                    f"Creating node `{name}` failed: {response.json()}",
                )  # pragma: no cover
            new_node._update_tags()
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
        columns: Optional[List["ColumnYAML"]] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
        update_if_exists: bool = False,
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
                "columns": [
                    {"name": column.name, "type": column.type} for column in columns
                ]
                if columns
                else [],
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
        data = response.json()
        source_node = Source(
            dj_client=self,
            name=data["name"],
            catalog=data["catalog"],
            schema_=data["schema_"],
            table=data["table"],
            columns=data["columns"],
            description=data["description"],
            mode=data["mode"],
            status=data["status"],
            display_name=data["display_name"],
            availability=data["availability"],
            tags=data["tags"],
            materializations=data["materializations"],
            version=data["version"],
            current_version=data["current_version"],
        )
        return source_node

    def register_view(  # pylint: disable=too-many-arguments
        self,
        catalog: str,
        schema: str,
        view: str,
        query: str,
        replace: bool = False,
    ) -> Source:
        """
        Register a table as a source node. This will create a source node under the configured
        `source_node_namespace` (a server-side setting), which defaults to the `source` namespace.
        """
        try:
            response = self._session.post(
                f"/register/view/{catalog}/{schema}/{view}/",
                params={"query": query, "replace": str(replace)},
            )
        except Exception as exc:
            if "409 Client Error" in str(exc):
                raise DJViewAlreadyRegistered(catalog, schema, view) from exc
            raise DJClientException(
                f"Failed to register view `{catalog}.{schema}.{view}`: {exc}",
            ) from exc
        data = response.json()
        source_node = Source(
            dj_client=self,
            name=data["name"],
            catalog=data["catalog"],
            schema_=data["schema_"],
            table=data["table"],
            columns=data["columns"],
            description=data["description"],
            mode=data["mode"],
            status=data["status"],
            display_name=data["display_name"],
            availability=data["availability"],
            tags=data["tags"],
            materializations=data["materializations"],
            version=data["version"],
            current_version=data["current_version"],
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
        tags: Optional[List[str]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
        custom_metadata: Optional[Dict] = None,
        update_if_exists: bool = False,
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
                "custom_metadata": custom_metadata,
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
        tags: Optional[List[str]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
        update_if_exists: bool = False,
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
        required_dimensions: Optional[List[str]] = None,
        direction: Optional[models.MetricDirection] = None,
        unit: Optional[models.MetricUnit] = None,
        significant_digits: int | None = None,
        min_decimal_exponent: int | None = None,
        max_decimal_exponent: int | None = None,
        tags: Optional[List[str]] = None,
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
        update_if_exists: bool = False,
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
                "required_dimensions": required_dimensions,
                **(
                    {
                        "metric_metadata": models.MetricMetadata(
                            direction=direction,
                            unit=unit,
                            significant_digits=significant_digits,
                            min_decimal_exponent=min_decimal_exponent,
                            max_decimal_exponent=max_decimal_exponent,
                        ),
                    }
                    if direction or unit
                    else {}
                ),
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
        tags: Optional[List[str]] = None,
        update_if_exists: bool = False,
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
    def create_tag(  # pylint: disable=too-many-arguments
        self,
        name: str,
        description: Optional[str],
        tag_metadata: Dict,
        tag_type: str,
        update_if_exists: bool = False,
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
        try:
            self._create_tag(tag=new_tag)
        except DJTagAlreadyExists as exc:
            if update_if_exists:
                new_tag._update()
            else:
                raise exc
        new_tag.refresh()
        return new_tag
