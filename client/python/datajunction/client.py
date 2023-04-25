"""DataJunction client setup."""
import enum
import logging
import platform
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import pandas as pd
import requests
from pydantic import BaseModel, Field, validator
from requests.adapters import CaseInsensitiveDict, HTTPAdapter

from datajunction.exceptions import DJClientException

DEFAULT_NAMESPACE = "default"
_logger = logging.getLogger(__name__)


class RequestsSessionWithEndpoint(requests.Session):  # pragma: no cover
    """
    Creates a requests session that comes with an endpoint that all
    subsequent requests will use as a prefix.
    """

    def __init__(self, endpoint: str = None):
        super().__init__()
        self.endpoint = endpoint
        self.mount("http://", HTTPAdapter())
        self.mount("https://", HTTPAdapter())

        self.headers = CaseInsensitiveDict(
            {
                "User-Agent": (
                    f"datajunction;;N/A;"
                    f"{platform.processor() or platform.machine()};"
                    f"{platform.system()};"
                    f"{platform.release()} {platform.version()}"
                ),
            },
        )

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


class DJClient:  # pylint: disable=too-many-public-methods
    """
    Client for access to the DJ core service
    """

    def __init__(
        self,
        uri: str = "http://localhost:8000",
        requests_session: RequestsSessionWithEndpoint = None,
        target_namespace: str = DEFAULT_NAMESPACE,
        timeout=2 * 60,
    ):
        self.target_namespace = target_namespace
        self.uri = uri
        if not requests_session:  # pragma: no cover
            self._session = RequestsSessionWithEndpoint(
                endpoint=self.uri,
            )
        else:  # pragma: no cover
            self._session = requests_session
        self._timeout = timeout

    def source(self, node_name: str) -> "Source":
        """
        Retrieves a source node with that name if one exists.
        """
        node_dict = self.verify_node_exists(node_name, "source")
        return Source(
            **node_dict,
            dj_client=self,
        )

    def new_source(  # pylint: disable=too-many-arguments
        self,
        name: str,
        catalog: str,
        schema_: str,
        table: str,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        columns: Optional[List["Column"]] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List["Tag"]] = None,
    ) -> "Source":
        """
        Instantiates a new Source object with the given parameters.
        """
        return Source(
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

    def transform(self, node_name: str) -> "Transform":
        """
        Retrieves a transform node with that name if one exists.
        """
        node_dict = self.verify_node_exists(node_name, "transform")
        return Transform(
            **node_dict,
            dj_client=self,
        )

    def new_transform(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: str,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List["Tag"]] = None,
    ) -> "Transform":
        """
        Instantiates a new Transform object with the given parameters.
        """
        return Transform(
            dj_client=self,
            name=name,
            description=description,
            display_name=display_name,
            tags=tags,
            primary_key=primary_key,
            query=query,
        )

    def dimension(self, node_name: str) -> "Dimension":
        """
        Retrieves a dimension node with that name if one exists.
        """
        node_dict = self.verify_node_exists(node_name, "dimension")
        return Dimension(
            **node_dict,
            dj_client=self,
        )

    def new_dimension(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: str,
        primary_key: Optional[List[str]],
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        tags: Optional[List["Tag"]] = None,
    ) -> "Dimension":
        """
        Instantiates a new Dimension object with the given parameters.
        """
        return Dimension(
            dj_client=self,
            name=name,
            description=description,
            display_name=display_name,
            tags=tags,
            primary_key=primary_key,
            query=query,
        )

    def metric(self, node_name: str) -> "Metric":
        """
        Retrieves a metric node with that name if one exists.
        """
        node_dict = self.verify_node_exists(node_name, "metric")
        return Metric(
            **node_dict,
            dj_client=self,
        )

    def new_metric(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: str,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List["Tag"]] = None,
    ) -> "Metric":
        """
        Instantiates a new Metric object with the given parameters.
        """
        return Metric(
            dj_client=self,
            name=name,
            description=description,
            display_name=display_name,
            tags=tags,
            primary_key=primary_key,
            query=query,
        )

    def cube(self, node_name: str) -> "Cube":  # pragma: no cover
        """
        Retrieves a cube node with that name if one exists.
        """
        node_dict = self.get_cube(node_name)
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

    def new_cube(  # pylint: disable=too-many-arguments
        self,
        name: str,
        metrics: List[str],
        dimensions: List[str],
        filters: Optional[List[str]] = None,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
    ) -> "Cube":
        """
        Instantiates a new cube with the given parameters.
        """
        return Cube(  # pragma: no cover
            dj_client=self,
            name=name,
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            description=description,
            display_name=display_name,
        )

    def verify_node_exists(self, node_name: str, type_: str) -> Dict[str, Any]:
        """
        Retrieves a node and verifies that it exists and has the expected node type.
        """
        node = self.get_node(node_name)
        if "name" not in node:
            raise DJClientException(f"No node with name {node_name} exists!")
        if "name" in node and node["type"] != type_:
            raise DJClientException(
                f"A node with name {node_name} exists, but it is not a {type_} node!",
            )
        return node

    def catalogs(self):
        """
        Gets all catalogs.
        """
        response = self._session.get("/catalogs/", timeout=self._timeout)
        return response.json()

    def engines(self):
        """
        Gets all engines.
        """
        response = self._session.get("/engines/", timeout=self._timeout)
        return response.json()

    def namespaces(self) -> List["Namespace"]:
        """
        Returns all node namespaces.
        """
        return [
            Namespace.parse_obj({**namespace, **{"dj_client": self}})
            for namespace in self.get_node_namespaces()
        ]

    def namespace(self, _namespace):
        """
        Returns the specified node namespace.
        """
        return Namespace(namespace=_namespace, dj_client=self)

    def delete_node(self, node: "Node"):
        """
        Delete this node
        """
        response = self._session.delete(f"/nodes/{node.name}/", timeout=self._timeout)
        return response

    def create_node(self, node: "Node", mode: "NodeMode"):
        """
        Helper function to create a node.
        """
        node.mode = mode
        response = self._session.post(
            f"/nodes/{node.type}/",
            timeout=self._timeout,
            json=node.dict(exclude_none=True, exclude={"type"}),
        )
        return response.json()

    def get_node_namespaces(self):
        """
        Retrieves all node namespaces
        """
        response = self._session.get("/namespaces/")
        return response.json()

    def get_nodes_in_namespace(
        self,
        namespace: str,
        type_: Optional[str] = None,
    ):
        """
        Retrieves all nodes in the namespace
        """
        response = self._session.get(
            f"/namespaces/{namespace}/" + (f"?type_={type_}" if type_ else ""),
        )
        return response.json()

    def get_node(self, node_name: str):
        """
        Retrieves a node.
        """
        response = self._session.get(f"/nodes/{node_name}/")
        return response.json()

    def get_cube(self, node_name: str):
        """
        Retrieves a node.
        """
        response = self._session.get(f"/cubes/{node_name}/")
        return response.json()

    def link_dimension_to_node(
        self,
        node_name: str,
        column_name: str,
        dimension_name: str,
        dimension_column: Optional[str] = None,
    ):
        """
        Helper function to link a dimension to the node.
        """
        response = self._session.post(
            f"/nodes/{node_name}/columns/{column_name}/"
            f"?dimension={dimension_name}&dimension_column={dimension_column}",
            timeout=self._timeout,
        )
        return response.json()

    def get_metric(self, node_name: str):
        """
        Helper function to retrieve metadata for the given metric node.
        """
        response = self._session.get(f"/metrics/{node_name}/")
        return response.json()

    def sql(  # pylint: disable=too-many-arguments
        self,
        node_name: str,
        dimensions: List[str],
        filters: List[str],
        engine_name: Optional[str] = "TRINO_DIRECT",
        engine_version: Optional[str] = "",
    ):
        """
        Retrieves the SQL query built for the node with the provided dimensions and filters.
        """
        response = self._session.get(
            f"/sql/{node_name}/",
            params={
                "dimensions": dimensions,
                "filters": filters,
                "engine_name": engine_name,
                "engine_version": engine_version,
            },
        )
        if response.status_code == 200:
            return response.json()["sql"]
        return response.json()

    def data(  # pylint: disable=too-many-arguments
        self,
        node_name: str,
        dimensions: List[str],
        filters: List[str],
        engine_name: Optional[str] = "TRINO_DIRECT",
        engine_version: Optional[str] = "",
    ):  # pragma: no cover
        """
        Retrieves the data for the node with the provided dimensions and filters.
        """
        response = self._session.get(
            f"/data/{node_name}/",
            params={
                "dimensions": dimensions,
                "filters": filters,
                "engine_name": engine_name,
                "engine_version": engine_version,
            },
        )
        results = response.json()
        columns = results["results"][0]["columns"]
        rows = results["results"][0]["rows"]
        return pd.DataFrame(rows, columns=[col["name"] for col in columns])

    def upsert_materialization_config(
        self,
        node_name: str,
        config: "MaterializationConfig",
    ):
        """
        Upserts a materialization config for the node.
        """
        response = self._session.post(
            f"/nodes/{node_name}/materialization/",
            json=config.dict(),
        )
        return response.json()


class Column(BaseModel):
    """
    Represents a column
    """

    name: str
    type: str


class NodeMode(str, enum.Enum):
    """
    DJ node's mode
    """

    DRAFT = "draft"
    PUBLISHED = "published"


class Tag(BaseModel):
    """
    Node tags
    """

    name: str
    display_name: str
    tag_type: str


class ClientEntity(BaseModel):
    """
    Any entity that uses the DJ client
    """

    dj_client: DJClient = Field(exclude=True)

    class Config:  # pylint: disable=too-few-public-methods
        """
        Allow arbitrary types to support DJClient but exclude
        it from the output.
        """

        arbitrary_types_allowed = True
        exclude = {"dj_client"}


class Node(ClientEntity):
    """
    Represents a DJ node object
    """

    name: str
    description: Optional[str]
    type: str
    mode: Optional[NodeMode]
    display_name: Optional[str]
    availability: Optional[Dict]
    tags: Optional[List[Tag]]
    primary_key: Optional[List[str]]
    materialization_configs: Optional[List[Dict[str, Any]]]

    def save(self, mode: NodeMode = NodeMode.PUBLISHED):
        """
        Sets the node's mode to PUBLISHED and pushes it to the server.
        """
        create_response = self.dj_client.create_node(self, mode)
        self.sync()
        return create_response

    def sync(self):
        """
        Refreshes a node with its latest version from the database.
        """
        refreshed_node = self.dj_client.get_node(self.name)
        for key, value in refreshed_node.items():
            if hasattr(self, key):
                setattr(self, key, value)
        return self

    def link_dimension(
        self,
        column: str,
        dimension: str,
        dimension_column: Optional[str],
    ):
        """
        Links the dimension to this node via the node's `column` and the dimension's
        `dimension_column`. If no `dimension_column` is provided, the dimension's
        primary key will be used automatically.
        """
        link_response = self.dj_client.link_dimension_to_node(
            self.name,
            column,
            dimension,
            dimension_column,
        )
        self.sync()
        return link_response

    def add_materialization_config(self, config: "MaterializationConfig"):
        """
        Adds a materialization config for the node. This will not work for source nodes
        as they don't need to be materialized.
        """
        upsert_response = self.dj_client.upsert_materialization_config(
            self.name,
            config,
        )
        self.sync()
        return upsert_response

    def sql(
        self,
        dimensions: List[str],
        filters: List[str],
        engine_name: Optional[str] = None,
        engine_version: Optional[str] = None,
    ):
        """
        Builds the SQL for this node, given the provided dimensions and filters.
        """
        return self.dj_client.sql(
            self.name,
            dimensions,
            filters,
            engine_name,
            engine_version,
        )

    def data(
        self,
        dimensions: List[str],
        filters: List[str],
        engine_name: Optional[str] = None,
        engine_version: Optional[str] = None,
    ):
        """
        Gets data for this node, given the provided dimensions and filters.
        """
        return self.dj_client.data(  # pragma: no cover
            self.name,
            dimensions,
            filters,
            engine_name,
            engine_version,
        )

    def delete(self):
        """
        Deletes the node
        """
        response = self.dj_client.delete_node(self)
        assert response.status_code == 204
        return f"Successfully deleted `{self.name}`"


class MaterializationConfig(BaseModel):
    """
    A node's materialization config
    """

    engine_name: str
    engine_version: Optional[str]
    schedule: str
    config: Dict


class Source(Node):
    """
    DJ source node
    """

    type: str = "source"
    catalog: str
    schema_: str
    table: str
    columns: Optional[List[Column]]

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


class Transform(Node):
    """
    DJ transform node
    """

    type: str = "transform"
    query: str
    columns: Optional[List[Column]]


class Metric(Node):
    """
    DJ metric node
    """

    type: str = "metric"
    query: str
    columns: Optional[List[Column]]

    def dimensions(self):
        """
        Returns the available dimensions for this metric.
        """
        metric = self.dj_client.get_metric(self.name)
        return metric["dimensions"]


class Dimension(Node):
    """
    DJ dimension node
    """

    type: str = "dimension"
    query: str
    columns: Optional[List[Column]]


class Cube(Node):
    """
    DJ cube node
    """

    type: str = "cube"
    query: Optional[str] = None
    metrics: List[str]
    dimensions: List[str]
    filters: Optional[List[str]]
    columns: Optional[List[Column]]


class Namespace(ClientEntity):
    """
    Represents a namespace
    """

    namespace: str

    def nodes(self):
        """
        Retrieves all nodes under this namespace.
        """
        return self.dj_client.get_nodes_in_namespace(
            self.namespace,
        )

    def metrics(self):
        """
        Retrieves metric nodes under this namespace.
        """
        return self.dj_client.get_nodes_in_namespace(
            self.namespace,
            type_="metric",
        )

    def sources(self):
        """
        Retrieves source nodes under this namespace.
        """
        return self.dj_client.get_nodes_in_namespace(
            self.namespace,
            type_="source",
        )

    def transforms(self):
        """
        Retrieves transform nodes under this namespace.
        """
        return self.dj_client.get_nodes_in_namespace(
            self.namespace,
            type_="transform",
        )

    def cubes(self):
        """
        Retrieves cubes under this namespace.
        """
        return self.dj_client.get_nodes_in_namespace(
            self.namespace,
            type_="cube",
        )

    def dimensions(self):
        """
        Retrieves dimension nodes under this namespace.
        """
        return self.dj_client.get_nodes_in_namespace(
            self.namespace,
            type_="dimension",
        )
