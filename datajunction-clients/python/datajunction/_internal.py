"""DataJunction base client setup."""

# pylint: disable=redefined-outer-name, import-outside-toplevel, too-many-lines
import logging
import os
import platform
import warnings
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, TypedDict, Union
from urllib.parse import urljoin

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    warnings.warn(
        (
            "Optional dependency `pandas` not found, data retrieval"
            "disabled. You can install pandas by running `pip install pandas`."
        ),
        ImportWarning,
    )
import requests
from requests.adapters import CaseInsensitiveDict, HTTPAdapter

from datajunction import models
from datajunction._base import SerializableMixin
from datajunction.exceptions import (
    DJClientException,
    DJTagAlreadyExists,
    DJTagDoesNotExist,
)

if TYPE_CHECKING:  # pragma: no cover
    from datajunction.admin import DJAdmin
    from datajunction.builder import DJBuilder
    from datajunction.nodes import Node
    from datajunction.tags import Tag

DEFAULT_NAMESPACE = "default"
_logger = logging.getLogger(__name__)


#
# Helpers
#
def from_jupyter() -> bool:  # pragma: no cover
    """
    Checks whether we're running from an IPython interactive console
    """
    try:
        from IPython import get_ipython
    except ImportError:
        return False
    return get_ipython() is not None


class Results(TypedDict):
    """
    Results in a completed DJ Query
    """

    columns: Tuple[str]
    data: Tuple[Tuple]


class RequestsSessionWithEndpoint(requests.Session):  # pragma: no cover
    """
    Creates a requests session that comes with an endpoint that all
    subsequent requests will use as a prefix.
    """

    def __init__(self, endpoint: str = None, show_traceback: bool = False):
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

        self._show_traceback = show_traceback

        if from_jupyter() and not self._show_traceback:
            from IPython import get_ipython  # pylint: disable=import-error

            def shortened_error(*args, **kwargs):  # pylint: disable=unused-argument
                import sys

                etype, value, _ = sys.exc_info()
                _logger.error("[%s]: %s", etype.__name__, value)

            get_ipython().showtraceback = shortened_error

    def request(self, method, url, *args, **kwargs):
        """
        Make the request with the full URL.
        """
        url = self.construct_url(url)
        try:
            response = super().request(method, url, *args, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as exc:
            if exc.response.headers.get("Content-Type") == "application/json":
                error_message = exc.response.json()
            else:
                error_message = f"Request failed {exc.response.status_code}: {str(exc)}"
            raise DJClientException(error_message) from exc

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


#
# Main DJClient (internal)
#
class DJClient:
    """
    Internal client class with non-user facing methods.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        uri: str = "http://localhost:8000",
        engine_name: str = None,
        engine_version: str = None,
        requests_session: RequestsSessionWithEndpoint = None,
        target_namespace: str = DEFAULT_NAMESPACE,
        timeout: int = 2 * 60,
        debug: bool = False,
    ):
        self.target_namespace = target_namespace
        self.uri = uri
        self.engine_name = engine_name
        self.engine_version = engine_version
        self._debug = debug

        if not requests_session:  # pragma: no cover
            self._session = RequestsSessionWithEndpoint(
                endpoint=self.uri,
                show_traceback=self._debug,
            )
        else:  # pragma: no cover
            self._session = requests_session
        self._timeout = timeout

    #
    # Authentication
    #
    def create_user(self, email: str, username: str, password: str):
        """
        Create basic user.
        """
        response = self._session.post(
            "/basic/user/",
            data={"email": email, "username": username, "password": password},
        )
        return response.json()

    def basic_login(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        Login with basic authentication.
        """
        response = self._session.post(
            "/basic/login/",
            data={
                "username": username or os.getenv("DJ_USER"),
                "password": password or os.getenv("DJ_PWD"),
            },
        )
        return response

    @staticmethod
    def _primary_key_from_columns(columns) -> List[str]:
        """
        Extracts the primary key from the columns
        """
        return [
            column["name"]
            for column in columns
            if any(
                attr["attribute_type"]["name"] == "primary_key"
                for attr in column["attributes"]
                if attr
            )
        ]

    @staticmethod
    def process_results(results) -> "pd.DataFrame":
        """
        Return a pandas dataframe of the results if pandas is installed
        """
        if "results" in results and results["results"]:
            columns = results["results"][0]["columns"]
            rows = results["results"][0]["rows"]
            try:
                return pd.DataFrame(
                    rows,
                    columns=[col["name"] for col in columns],
                )
            except NameError:  # pragma: no cover
                return Results(
                    data=rows,
                    columns=tuple(col["name"] for col in columns),  # type: ignore
                )
        raise DJClientException("No data for query!")

    #
    # Node methods
    #
    def _get_nodes_in_namespace(
        self,
        namespace: str,
        type_: Optional[models.NodeType] = None,
    ):
        """
        Retrieves all nodes in given namespace.
        """
        response = self._session.get(
            f"/namespaces/{namespace}/" + (f"?type_={type_.value}" if type_ else ""),
        )
        node_details_list = response.json()
        nodes = [n["name"] for n in node_details_list]
        return nodes

    def _get_all_nodes(
        self,
        type_: Optional[models.NodeType] = None,
    ):
        """
        Retrieve all nodes of a given type.
        """
        response = self._session.get(
            "/nodes/" + (f"?node_type={type_.value}" if type_ else ""),
        )
        return response.json()

    def _verify_node_exists(
        self,
        node_name: str,
        type_: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Retrieves a node and verifies that it exists and has the expected node type.
        """
        node = self._get_node(node_name)
        if "name" not in node:
            raise DJClientException(f"No node with name {node_name} exists!")
        if type_ and "name" in node and node["type"] != type_:
            raise DJClientException(
                f"A node with name {node_name} exists, but it is not a {type_} node!",
            )
        return node

    def _validate_node(self, node: "Node"):
        """
        Check if a locally defined node is valid.
        """
        node_copy = node.to_dict()
        node_copy["mode"] = models.NodeMode.PUBLISHED
        response = self._session.post(
            "/nodes/validate/",
            json=node_copy,
            timeout=self._timeout,
        )
        return response.json()

    def _create_node(
        self,
        node: "Node",
        mode: Optional[models.NodeMode] = models.NodeMode.PUBLISHED,
    ):
        """
        Helper function to create a node.
        Raises an error if node already exists and is active.
        """
        node.mode = mode
        response = self._session.post(
            f"/nodes/{node.type}/",
            timeout=self._timeout,
            json=node.to_dict(exclude=["type"]),
        )
        return response

    def _update_node(
        self,
        node_name: str,
        update_input: models.UpdateNode,
    ) -> requests.Response:
        """
        Call node update API with attributes to update.
        """
        return self._session.patch(f"/nodes/{node_name}/", json=asdict(update_input))

    def _publish_node(self, node_name: str, update_input: models.UpdateNode):
        """
        Retrieves a node.
        """
        response = self._session.patch(
            f"/nodes/{node_name}/",
            json=asdict(update_input),
        )
        return response.json()

    def _get_node(self, node_name: str):
        """
        Retrieves a node.
        """
        response = self._session.get(f"/nodes/{node_name}/")
        return response.json()

    def _get_node_upstreams(self, node_name: str):
        """
        Retrieves a node's upstreams
        """
        response = self._session.get(f"/nodes/{node_name}/upstream")
        return response.json()

    def _get_node_downstreams(self, node_name: str):
        """
        Retrieves a node's downstreams
        """
        response = self._session.get(f"/nodes/{node_name}/downstream")
        return response.json()

    def _get_node_dimensions(self, node_name: str):
        """
        Retrieves a node's dimensions
        """
        response = self._session.get(f"/nodes/{node_name}/dimensions")
        return response.json()

    def _get_cube(self, node_name: str):
        """
        Retrieves a Cube node.
        """
        response = self._session.get(f"/cubes/{node_name}/")
        return response.json()

    def get_metric(self, node_name: str):
        """
        Helper function to retrieve metadata for the given metric node.
        """
        response = self._session.get(f"/metrics/{node_name}/")
        return response.json()

    def _get_node_revisions(self, node_name: str):
        """
        Retrieve all revisions of the node
        """
        response = self._session.get(f"/nodes/{node_name}/revisions")
        return response.json()

    def _link_dimension_to_node(
        self,
        node_name: str,
        column_name: str,
        dimension_name: str,
        dimension_column: Optional[str],
    ):
        """
        Helper function to link a dimension to the node.
        """
        params = {"dimension": dimension_name}
        if dimension_column:
            params["dimension_column"] = dimension_column
        response = self._session.post(
            f"/nodes/{node_name}/columns/{column_name}/",
            timeout=self._timeout,
            params=params,
        )
        return response.json()

    def _add_reference_dimension_link(  # pylint: disable=too-many-arguments
        self,
        node_name: str,
        node_column: str,
        dimension_node: str,
        dimension_column: str,
        role: Optional[str] = None,
    ):
        """
        Helper function to link a dimension to the node.
        """
        params = {
            "dimension_node": dimension_node,
            "dimension_column": dimension_column,
            **({"role": role} if role else {}),
        }
        response = self._session.post(
            f"/nodes/{node_name}/columns/{node_column}/link",
            timeout=self._timeout,
            params=params,
        )
        return response.json()

    def _link_complex_dimension_to_node(  # pylint: disable=too-many-arguments
        self,
        node_name: str,
        dimension_node: str,
        join_type: Optional[str] = None,
        *,
        join_on: str,
        join_cardinality: Optional[str] = None,
        role: Optional[str] = None,
    ):
        """
        Helper function to link a complex dimension to the node.
        """
        params = {
            "dimension_node": dimension_node,
            "join_type": join_type or "LEFT",
            "join_on": join_on,
            "join_cardinality": join_cardinality or "one_to_many",
            "role": role,
        }
        response = self._session.post(
            f"/nodes/{node_name}/link/",
            timeout=self._timeout,
            json=params,
        )
        return response.json()

    def _unlink_dimension_from_node(
        self,
        node_name: str,
        column_name: str,
        dimension_name: str,
        dimension_column: Optional[str] = None,
    ):
        """
        Helper function to un-link a dimension to the node.
        """
        response = self._session.delete(
            f"/nodes/{node_name}/columns/{column_name}/"
            f"?dimension={dimension_name}&dimension_column={dimension_column}",
            timeout=self._timeout,
        )
        return response.json()

    def _remove_complex_dimension_link(
        self,
        node_name: str,
        dimension_node: str,
        role: Optional[str] = None,
    ):
        """
        Helper function to remove a complex dimension link.
        """
        response = self._session.request(
            "DELETE",
            f"/nodes/{node_name}/link/",
            timeout=self._timeout,
            json={
                "dimension_node": dimension_node,
                "role": role,
            },
        )
        return response.json()

    def _remove_reference_dimension_link(
        self,
        node_name: str,
        node_column: str,
    ):
        """
        Helper function to remove a reference dimension link from a node column.
        """
        response = self._session.delete(
            f"/nodes/{node_name}/columns/{node_column}/link",
            timeout=self._timeout,
        )
        return response.json()

    def _upsert_materialization(
        self,
        node_name: str,
        config: models.Materialization,
    ):
        """
        Upserts a materialization config for the node.
        """
        response = self._session.post(
            f"/nodes/{node_name}/materialization/",
            json=config.to_dict(),
        )
        return response.json()

    def _deactivate_materialization(
        self,
        node_name: str,
        materialization_name: str,
    ):
        """
        Upserts a materialization config for the node.
        """
        response = self._session.delete(
            f"/nodes/{node_name}/materializations/",
            params={
                "materialization_name": materialization_name,
            },
        )
        return response.json()

    def _add_availability_state(
        self,
        node_name: str,
        availability: models.AvailabilityState,
    ):
        """
        Adds an availability state for the node
        """
        response = self._session.post(
            f"/data/{node_name}/availability/",
            json=asdict(availability),
        )
        return response.json()

    def _set_column_attributes(
        self,
        node_name,
        column_name,
        attributes: List[models.ColumnAttribute],
    ):
        """
        Sets attributes for columns on the node
        """
        response = self._session.post(
            f"/nodes/{node_name}/columns/{column_name}/attributes/",
            json=[asdict(attribute) for attribute in attributes],
        )
        return response.json()

    def _set_column_display_name(
        self,
        node_name,
        column_name,
        display_name: str,
    ):
        """
        Sets display name for the column on the node
        """
        response = self._session.patch(
            f"/nodes/{node_name}/columns/{column_name}/",
            params={"display_name": display_name},
        )
        return response.json()

    def _set_column_description(
        self,
        node_name,
        column_name,
        description: str,
    ):
        """
        Sets description for the column on the node
        """
        response = self._session.patch(
            f"/nodes/{node_name}/columns/{column_name}/description/",
            params={"description": description},
        )
        return response.json()

    def _find_nodes_with_dimension(
        self,
        node_name,
    ):
        """
        Find all nodes with this dimension
        """
        response = self._session.get(f"/dimensions/{node_name}/nodes/")
        return response.json()

    def _refresh_source_node(
        self,
        node_name,
    ):
        """
        Find all nodes with this dimension
        """
        response = self._session.post(f"/nodes/{node_name}/refresh/")
        return response.json()

    def _export_namespace(self, namespace):
        """
        Export an array of definitions contained within a namespace
        """
        response = self._session.get(f"/namespaces/{namespace}/export/")
        return response.json()

    #
    # Methods for Tags
    #
    def _update_tag(self, tag_name: str, update_input: models.UpdateTag):
        """
        Call tag update API with attributes to update.
        """
        return self._session.patch(f"/tags/{tag_name}/", json=asdict(update_input))

    def _update_node_tags(self, node_name: str, tags: Optional[List[str]]):
        """
        Update tags on a node
        """
        return self._session.post(
            f"/nodes/{node_name}/tags/",
            params={"tag_names": tags} if tags else None,
        )

    def _get_tag(self, tag_name: str):
        """
        Retrieves a tag.
        """
        try:
            response = self._session.get(f"/tags/{tag_name}/")
            return response.json()
        except DJClientException as exc:  # pragma: no cover
            return exc.__dict__

    def _create_tag(
        self,
        tag: "Tag",
    ):
        """
        Helper function to create a tag.
        Raises an error if tag already exists.
        """
        existing_tag = self._get_tag(tag_name=tag.name)
        if "name" in existing_tag:
            raise DJTagAlreadyExists(tag_name=tag.name)
        response = self._session.post(
            "/tags/",
            timeout=self._timeout,
            json=tag.to_dict(),
        )
        return response

    def _list_nodes_with_tag(
        self,
        tag_name: str,
        node_type: Optional[models.NodeType] = None,
    ):
        """
        Retrieves all nodes with a given tag.
        """
        response = self._session.get(
            f"/tags/{tag_name}/nodes"
            + (f"?node_type={node_type.value}" if node_type else ""),
        )
        if response.status_code == 404:
            raise DJTagDoesNotExist(tag_name)
        return [n["name"] for n in response.json()]


@dataclass
class ClientEntity(SerializableMixin):
    """
    Any entity that uses the DJ client.
    """

    dj_client: Union[DJClient, "DJBuilder", "DJAdmin"]
    exclude = ["dj_client"]
