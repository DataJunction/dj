"""DataJunction client setup."""
import abc

# pylint: disable=redefined-outer-name, import-outside-toplevel, too-many-lines
import logging
import platform
import time
import warnings
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Union
from urllib.parse import urlencode, urljoin

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
from alive_progress import alive_bar
from pydantic import BaseModel, Field, validator
from requests.adapters import CaseInsensitiveDict, HTTPAdapter

from datajunction import models
from datajunction.exceptions import DJClientException, DJNamespaceAlreadyExists

DEFAULT_NAMESPACE = "default"
_logger = logging.getLogger(__name__)


class Results(TypedDict):
    """
    Results in a completed DJ Query
    """

    data: Tuple[Tuple]
    columns: Tuple[str]


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
        try:
            response = super().request(method, url, *args, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as exc:
            error_message = None
            if not exc.response:
                raise DJClientException(exc) from exc
            if exc.response.headers.get("Content-Type") == "application/json":
                error_message = exc.response.json().get("message")
            if not error_message:
                error_message = (
                    f"Request failed with status code {exc.response.status_code}"
                )
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


class DJClient:  # pylint: disable=too-many-public-methods
    """
    Client for access to the DJ core service
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        uri: str = "http://localhost:8000",
        engine_name: str = None,
        engine_version: str = None,
        requests_session: RequestsSessionWithEndpoint = None,
        target_namespace: str = DEFAULT_NAMESPACE,
        timeout=2 * 60,
    ):
        self.target_namespace = target_namespace
        self.uri = uri
        self.engine_name = engine_name
        self.engine_version = engine_version
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
        node = Source(
            **node_dict,
            dj_client=self,
        )
        node.primary_key = self.primary_key_from_columns(node_dict["columns"])
        return node

    def new_source(  # pylint: disable=too-many-arguments
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
        node = Transform(
            **node_dict,
            dj_client=self,
        )
        node.primary_key = self.primary_key_from_columns(node_dict["columns"])
        return node

    def new_transform(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: str,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List[models.Tag]] = None,
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
        node = Dimension(
            **node_dict,
            dj_client=self,
        )
        node.primary_key = self.primary_key_from_columns(node_dict["columns"])
        return node

    def new_dimension(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: str,
        primary_key: Optional[List[str]],
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        tags: Optional[List[models.Tag]] = None,
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
        node = Metric(
            **node_dict,
            dj_client=self,
        )
        node.primary_key = self.primary_key_from_columns(node_dict["columns"])
        return node

    def new_metric(  # pylint: disable=too-many-arguments
        self,
        name: str,
        query: str,
        description: Optional[str] = None,
        display_name: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        tags: Optional[List[models.Tag]] = None,
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

    @staticmethod
    def primary_key_from_columns(columns) -> List[str]:
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
        # materialization_configs: Optional[List[MaterializationConfig]] = None,
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

    def new_namespace(
        self,
        namespace: str,
    ) -> "Namespace":
        """
        Creates a new namespace on the server
        """
        _namespace = Namespace(dj_client=self, namespace=namespace)
        self.create_namespace(_namespace)
        return _namespace

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

    def metrics(self):
        """
        Returns all metrics
        """
        return self.get_metrics()

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

    def create_namespace(self, namespace: "Namespace"):
        """
        Helper function to create a namespace.
        """
        response = self._session.post(
            f"/namespaces/{namespace.namespace}/",
            timeout=self._timeout,
        )
        json_response = response.json()
        if response.status_code == 409:
            raise DJNamespaceAlreadyExists(json_response["message"])
        return json_response

    def deactivate_node(self, node: "Node"):
        """
        Deactivate this node
        """
        response = self._session.post(
            f"/nodes/{node.name}/deactivate/",
            timeout=self._timeout,
        )
        return response

    def activate_node(self, node: "Node"):
        """
        Activate this node
        """
        response = self._session.post(
            f"/nodes/{node.name}/activate/",
            timeout=self._timeout,
        )
        return response

    def validate_node(self, node: "Node"):
        """
        Check if a locally defined node is valid
        """
        node_copy = node.dict().copy()
        node_copy["mode"] = models.NodeMode.PUBLISHED
        response = self._session.post(
            "/nodes/validate/",
            json=node_copy,
            timeout=self._timeout,
        )
        return response.json()

    def create_node(self, node: "Node", mode: models.NodeMode):
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

    def get_metrics(self):
        """
        Retrieves all metrics
        """
        response = self._session.get("/metrics/")
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

    def update_node(self, node_name: str, update_input: models.UpdateNode):
        """
        Retrieves a node.
        """
        response = self._session.patch(f"/nodes/{node_name}/", json=update_input.dict())
        return response.json()

    def publish_node(self, node_name: str, update_input: models.UpdateNode):
        """
        Retrieves a node.
        """
        response = self._session.patch(f"/nodes/{node_name}/", json=update_input.dict())
        return response.json()

    def get_node(self, node_name: str):
        """
        Retrieves a node.
        """
        try:
            response = self._session.get(f"/nodes/{node_name}/")
            return response.json()
        except DJClientException as exc:  # pragma: no cover
            return exc.__dict__

    def get_cube(self, node_name: str):
        """
        Retrieves a node.
        """
        response = self._session.get(f"/cubes/{node_name}/")
        return response.json()

    def get_node_revisions(self, node_name: str):
        """
        Retrieve all revisions of the node
        """
        response = self._session.get(f"/nodes/{node_name}/revisions")
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

    def common_dimensions(
        self,
        metrics: List[str],
    ):  # pragma: no cover # Tested in integration tests
        """
        Return common dimensions for a set of metrics
        """
        query_params = []
        for metric in metrics:
            query_params.append(("metric", metric))
        response = self._session.get(
            f"/metrics/common/dimensions/?{urlencode(query_params)}",
        )
        return response.json()

    def sql_for_metric(  # pylint: disable=too-many-arguments
        self,
        node_name: str,
        dimensions: List[str],
        filters: List[str],
        engine_name: Optional[str] = None,
        engine_version: Optional[str] = None,
    ):
        """
        Retrieves the SQL query built for the node with the provided dimensions and filters.
        """
        response = self._session.get(
            f"/sql/{node_name}/",
            params={
                "dimensions": dimensions,
                "filters": filters,
                "engine_name": engine_name or self.engine_name,
                "engine_version": engine_version or self.engine_version,
            },
        )
        if response.status_code == 200:
            return response.json()["sql"]
        return response.json()

    def sql(  # pylint: disable=too-many-arguments
        self,
        metrics: List[str],
        dimensions: List[str],
        filters: List[str],
        engine_name: Optional[str] = None,
        engine_version: Optional[str] = None,
    ):
        """
        Builds SQL for multiple metrics with the provided dimensions and filters.
        """
        response = self._session.get(
            "/sql/",
            params={
                "metrics": metrics,
                "dimensions": dimensions,
                "filters": filters,
                "engine_name": engine_name or self.engine_name,
                "engine_version": engine_version or self.engine_version,
            },
        )
        if response.status_code == 200:
            return response.json()["sql"]
        return response.json()

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

    def data(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        metrics: List[str],
        dimensions: List[str],
        filters: List[str],
        engine_name: Optional[str] = None,
        engine_version: Optional[str] = None,
        async_: bool = True,
    ):
        """
        Retrieves the data for the node with the provided dimensions and filters.
        """
        printed_links = False
        with alive_bar(
            title="Processing",
            length=20,
            bar="smooth",
            force_tty=True,
            calibrate=5e40,
        ) as progress_bar:
            poll_interval = 1  # Initial polling interval in seconds
            job_state = models.QueryState.UNKNOWN
            results = None
            while job_state not in models.END_JOB_STATES:
                progress_bar()  # pylint: disable=not-callable
                response = self._session.get(
                    "/data/",
                    params={
                        "metrics": metrics,
                        "dimensions": dimensions,
                        "filters": filters,
                        "engine_name": engine_name or self.engine_name,
                        "engine_version": engine_version or self.engine_version,
                        "async_": async_,
                    },
                )
                results = response.json()

                # Raise errors if any
                if not response.ok:
                    raise DJClientException(f"Error retrieving data: {response.text}")
                if results["state"] not in models.QueryState.list():
                    raise DJClientException(  # pragma: no cover
                        f"Query state {results['state']} is not a DJ-parseable query state!"
                        " Please reach out to your server admin to make sure DJ is configured"
                        " correctly.",
                    )

                # Update the query state and print links if any
                job_state = models.QueryState(results["state"])
                if not printed_links and results["links"]:  # pragma: no cover
                    print(
                        "Links:\n"
                        + "\n".join([f"\t* {link}" for link in results["links"]]),
                    )
                    printed_links = True
                progress_bar.title = f"Status: {job_state.value}"

                # Update the polling interval
                time.sleep(poll_interval)
                poll_interval *= 2

            # Return results if the job has finished
            if job_state == models.QueryState.FINISHED:
                return self.process_results(results)
            if job_state == models.QueryState.CANCELED:  # pragma: no cover
                raise DJClientException("Query execution was canceled!")
            raise DJClientException(  # pragma: no cover
                f"Error retrieving data: {response.text}",
            )

    def upsert_materialization(
        self,
        node_name: str,
        config: models.MaterializationConfig,
    ):
        """
        Upserts a materialization config for the node.
        """
        response = self._session.post(
            f"/nodes/{node_name}/materialization/",
            json=config.dict(),
        )
        return response.json()

    def add_availability_state(
        self,
        node_name: str,
        availability: models.AvailabilityState,
    ):
        """
        Adds an availability state for the node
        """
        response = self._session.post(
            f"/data/{node_name}/availability/",
            json=availability.dict(),
        )
        return response.json()

    def set_column_attributes(
        self,
        node_name,
        attributes: List[models.ColumnAttribute],
    ):
        """
        Sets attributes for columns on the node
        """
        response = self._session.post(
            f"/nodes/{node_name}/attributes/",
            json=[attribute.dict() for attribute in attributes],
        )
        return response.json()


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
    mode: Optional[models.NodeMode]
    status: Optional[str] = None
    display_name: Optional[str]
    availability: Optional[models.AvailabilityState]
    tags: Optional[List[models.Tag]]
    primary_key: Optional[List[str]]
    materializations: Optional[List[Dict[str, Any]]]
    version: Optional[str]
    deactivated_at: Optional[int]

    def save(self, mode: models.NodeMode = models.NodeMode.PUBLISHED):
        """
        Sets the node's mode to PUBLISHED and pushes it to the server.
        """
        existing_node = self.dj_client.get_node(node_name=self.name)
        if "name" in existing_node:
            response = self.update()
        else:
            response = self.dj_client.create_node(self, mode)
        self.sync()
        return response

    @abc.abstractmethod
    def update(self) -> Dict:
        """
        Update the node for fields that have changed
        """

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

    def add_materialization(self, config: models.MaterializationConfig):
        """
        Adds a materialization for the node. This will not work for source nodes
        as they don't need to be materialized.
        """
        upsert_response = self.dj_client.upsert_materialization(
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
        engine_version: Optional[str] = "",
    ):
        """
        Builds the SQL for this node, given the provided dimensions and filters.
        """
        return self.dj_client.sql_for_metric(
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
        Retrieves data for this node, given the provided dimensions and filters.
        """
        return self.dj_client.data(  # pragma: no cover
            [self.name],
            dimensions,
            filters,
            engine_name,
            engine_version,
        )

    def deactivate(self) -> str:
        """
        Deactivates the node
        """
        response = self.dj_client.deactivate_node(self)
        if not response.ok:  # pragma: no cover
            raise DJClientException(
                f"Error deactivating node `{self.name}`: {response.text}",
            )
        return f"Successfully deactivated `{self.name}`"

    def activate(self) -> str:
        """
        Activates the node
        """
        response = self.dj_client.activate_node(self)
        if not response.ok:  # pragma: no cover
            raise DJClientException(
                f"Error activating node `{self.name}`: {response.text}",
            )
        return f"Successfully activated `{self.name}`"

    def revisions(self):
        """
        List all revisions of this node
        """
        return self.dj_client.get_node_revisions(self.name)

    def add_availability(self, availability: models.AvailabilityState):
        """
        Adds an availability state to the node
        """
        return self.dj_client.add_availability_state(self.name, availability)

    def set_column_attributes(self, attributes: List[models.ColumnAttribute]):
        """
        Sets attributes for columns on the node
        """
        return self.dj_client.set_column_attributes(self.name, attributes)


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

    def update(self) -> Dict:
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
        return self.dj_client.update_node(self.name, update_node)


class NodeWithQuery(Node):
    """
    Nodes with query attribute
    """

    query: str

    def update(self) -> Dict:
        """
        Update the node for fields that have changed
        """
        update_node = models.UpdateNode(
            display_name=self.display_name,
            description=self.description,
            mode=self.mode,
            primary_key=self.primary_key,
            query=self.query,
        )
        return self.dj_client.update_node(self.name, update_node)

    def check(self) -> str:
        """
        Check if the node is valid by calling the /validate endpoint
        """
        validation = self.dj_client.validate_node(self)
        return validation["status"]

    def publish(self) -> bool:
        """
        Change a node's mode to published
        """
        self.dj_client.publish_node(
            self.name,
            models.UpdateNode(mode=models.NodeMode.PUBLISHED),
        )
        return True


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

    def update(self):  # pragma: no cover
        pass


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
