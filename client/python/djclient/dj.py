"""DataJunction client setup."""
import enum
import platform
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import requests
from pydantic import BaseModel
from requests.adapters import CaseInsensitiveDict, HTTPAdapter

from djclient import __version__
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
                    f"djclient;{__version__};N/A;"
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


class Node(BaseModel):
    """
    Represents a DJ node object
    """

    name: str
    description: str
    type: str
    mode: Optional[NodeMode]
    display_name: Optional[str]
    availability: Optional[Dict]
    tags: Optional[List[Tag]]

    def _get_initialized_client(self):
        """
        Retrieves the saved DJ client if one has been initialized.
        """
        session = REGISTRY.get("_client")
        if not session:
            raise DJClientException(
                "DJ client not initialized! Please initialize "
                "a client with the endpoint uri:"
                "    client = DJClient(uri='...')",
            )
        return session

    def publish(self):
        """
        Sets the node's mode to PUBLISHED and pushes it to the server.
        """
        session = self._get_initialized_client()
        session.create_node(self, NodeMode.PUBLISHED)

    def draft(self):
        """
        Sets the node's mode to DRAFT and pushes it to the server.
        """
        session = self._get_initialized_client()
        session.create_node(self, NodeMode.DRAFT)

    def delete(self):
        """
        Sets the node's mode to DRAFT and pushes it to the server.
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
    columns: List[Column]


class Metric(Node):
    """
    DJ metric node
    """

    type: str = "metric"
    query: str
    columns: List[Column]


class Dimension(Node):
    """
    DJ dimension node
    """

    type: str = "dimension"
    query: str
    columns: List[Column]


class Cube(Node):
    """
    DJ cube node
    """

    metrics: List[str]
    dimensions: List[str]
    filters: List[str]
