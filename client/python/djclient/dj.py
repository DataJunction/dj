"""DataJunction client setup."""
import enum
import platform
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import pandas as pd
import requests
from pydantic import BaseModel
from requests.adapters import CaseInsensitiveDict, HTTPAdapter

from djclient.exceptions import DJClientException

DEFAULT_NAMESPACE = "default"
REGISTRY = {}


class RequestsSessionWithEndpoint(requests.Session):
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
                    f"djclient;;N/A;"
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


class DJClient:
    """
    Client for access to the DJ core service
    """

    def __init__(
        self,
        uri: str = "http://localhost:8000",
        namespace: str = DEFAULT_NAMESPACE,
        timeout=2 * 60,
    ):
        self.namespace = namespace
        self.uri = uri
        self._session = RequestsSessionWithEndpoint(
            endpoint=self.uri,
        )
        self._timeout = timeout
        REGISTRY["_client"] = self

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

    def namespaces(self, names_only: bool = False):
        """
        Returns all node namespaces.
        """
        return self.get_node_namespaces()

    def nodes(self, names_only: bool = False):
        """
        Returns all source nodes. If `names_only` is set to true,
        it will return a list of node names.
        """
        return self._nodes_by_type(names_only=names_only)

    def sources(self, names_only: bool = False):
        """
        Returns all source nodes. If `names_only` is set to true,
        it will return a list of node names.
        """
        return self._nodes_by_type(type_="source", names_only=names_only)

    def dimensions(self, names_only: bool = False):
        """
        Returns all dimension nodes. If `names_only` is set to true,
        it will return a list of node names.
        """
        return self._nodes_by_type(type_="dimension", names_only=names_only)

    def transforms(self, names_only: bool = False):
        """
        Returns all transform nodes. If `names_only` is set to true,
        it will return a list of node names.
        """
        return self._nodes_by_type(type_="transform", names_only=names_only)

    def metrics(self, names_only: bool = False):
        """
        Returns all metric nodes. If `names_only` is set to true,
        it will return a list of node names.
        """
        return self._nodes_by_type(type_="metric", names_only=names_only)

    def cubes(self, names_only: bool = False):
        """
        Returns all cube nodes. If `names_only` is set to true,
        it will return a list of node names.
        """
        return self._nodes_by_type(type_="cube", names_only=names_only)

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
        response = self._session.get(f"/nodes/namespace/all/")
        return response.json()

    def get_node(self, node_name: str):
        """
        Retrieves a node.
        """
        response = self._session.get(f"/nodes/{node_name}/")
        return response.json()

    def _nodes_by_type(
        self,
        type_: Optional[str] = None,
        names_only: bool = False,
    ) -> Union[List[str], List[Dict[str, Any]]]:
        """
        Helper function to retrieve DJ nodes by its type.
        """
        response = self._session.get("/nodes/", timeout=self._timeout)
        if names_only:
            return [
                node["name"]
                for node in response.json()
                if not type_ or node["type"] == type_
            ]
        return [node for node in response.json() if not type_ or node["type"] == type_]

    def link_dimension_to_node(
        self,
        node_name: str,
        column_name: str,
        dimension_name: str,
        dimension_column: str,
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

    def sql(self, node_name: str, dimensions: List[str], filters: List[str]):
        """
        Retrieves the SQL query built for the node with the provided dimensions and filters.
        """
        response = self._session.get(
            f"/sql/{node_name}/",
            params={
                "dimensions": dimensions,
                "filters": filters,
            },
        )
        return response.json()["sql"]

    def data(self, node_name: str, dimensions: List[str], filters: List[str]):
        """
        Retrieves the data for the node with the provided dimensions and filters.
        """
        response = self._session.get(
            f"/data/{node_name}/",
            params={
                "dimensions": dimensions,
                "filters": filters,
            },
        )
        results = response.json()
        columns = results["results"][0]["columns"]
        rows = results["results"][0]["rows"]
        return pd.DataFrame(rows, columns=[col["name"] for col in columns])


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

    def _get_initialized_client(self):
        """
        Retrieves the saved DJ client if one has been initialized.
        """
        client = REGISTRY.get("_client")
        if not client:
            raise DJClientException(
                "DJ client not initialized! Please initialize "
                "a client with the endpoint uri:"
                "    client = DJClient(uri='...')",
            )
        return client


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

    def publish(self):
        """
        Sets the node's mode to PUBLISHED and pushes it to the server.
        """
        client = self._get_initialized_client()
        create_response = client.create_node(self, NodeMode.PUBLISHED)
        self.refresh()
        return create_response

    def refresh(self):
        """
        Refreshes a node with its latest version from the database.
        """
        client = self._get_initialized_client()
        refreshed_node = client.get_node(self.name)
        for key, value in refreshed_node.items():
            if hasattr(self, key):
                setattr(self, key, value)
        return self

    def draft(self):
        """
        Sets the node's mode to DRAFT and pushes it to the server.
        """
        client = self._get_initialized_client()
        create_response = client.create_node(self, NodeMode.DRAFT)
        self.refresh()
        return create_response

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
        client = self._get_initialized_client()
        return client.link_dimension_to_node(
            self.name,
            column,
            dimension,
            dimension_column,
        )

    def sql(self, dimensions: List[str], filters: List[str]):
        """
        Builds the SQL for this node, given the provided dimensions and filters.
        """
        client = self._get_initialized_client()
        return client.sql(self.name, dimensions, filters)

    def data(self, dimensions: List[str], filters: List[str]):
        """
        Gets data for this node, given the provided dimensions and filters.
        """
        client = self._get_initialized_client()
        return client.data(self.name, dimensions, filters)

    def delete(self):
        """
        Deletes the node
        """
        session = self._get_initialized_client()
        response = session.delete_node(self)
        assert response.status_code == 204
        return f"Successfully deleted `{self.name}`"


class Source(Node):
    """
    DJ source node
    """

    type: str = "source"
    catalog: str
    schema_: str
    table: str
    columns: Optional[List[Column]]


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
        client = self._get_initialized_client()
        metric = client.get_metric(self.name)
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

    metrics: List[str]
    dimensions: List[str]
    filters: List[str]
