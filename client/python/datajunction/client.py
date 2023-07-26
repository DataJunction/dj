"""DataJunction main client module."""

import time
from typing import List, Optional
from urllib.parse import urlencode

from alive_progress import alive_bar

from datajunction import _internal, models
from datajunction.exceptions import DJClientException, DJNamespaceAlreadyExists
from datajunction.nodes import Cube, Dimension, Metric, Namespace, Source, Transform


class DJClient(_internal.DJClient):
    """
    Client class for basic DJ dag and data access.
    """

    #
    # List
    #
    def list_namespaces(self, prefix: Optional[str] = None) -> List[str]:
        """
        List namespaces starting with a given prefix.
        """
        namespaces = self._session.get("/namespaces/").json()
        if prefix:
            namespaces = [n for n in namespaces if n.startswith(prefix)]
        return namespaces

    def list_dimensions(self, namespace: str) -> List[str]:
        """
        List dimension nodes under given namespace.
        TODO: make namespace optional and return all dimension nodes, similar to get("/metrics/").
        """
        return self._get_nodes_in_namespace(
            namespace=namespace,
            type_=models.NodeType.DIMENSION.value,
        )

    def list_metrics(self, namespace: Optional[str] = None) -> List[str]:
        """
        List metric nodes for given namespace or all.
        """
        if namespace:
            return self._get_nodes_in_namespace(
                namespace=namespace,
                type_=models.NodeType.METRIC.value,
            )
        return self._session.get("/metrics/").json()

    #
    # Get common metrics and dimensions
    #
    def common_dimensions_with_details(
        self,
        metrics: List[str],
    ) -> List[dict]:  # pragma: no cover # Tested in integration tests
        """
        Return common dimensions for a set of metrics.
        """
        query_params = []
        for metric in metrics:
            query_params.append((models.NodeType.METRIC.value, metric))
        response = self._session.get(
            f"/metrics/common/dimensions/?{urlencode(query_params)}",
        )
        return response.json()

    def common_dimensions(
        self,
        metrics: List[str],
    ) -> List[str]:  # pragma: no cover # Tested in integration tests
        """
        Return common dimensions (names only) for a set of metrics.
        """
        dimensions = self.common_dimensions_with_details(metrics=metrics)
        return [dimension["name"] for dimension in dimensions]

    def common_metrics_with_details(
        self,
        dimensions: List[str],
    ) -> List[dict]:  # pragma: no cover # Tested in integration tests
        """
        Return common metrics for a set of dimensions.
        """
        query_params = [("node_type", models.NodeType.METRIC.value)]
        for dim in dimensions:
            query_params.append((models.NodeType.DIMENSION.value, dim))
        response = self._session.get(
            f"/dimensions/common/?{urlencode(query_params)}",
        )
        return response.json()

    def common_metrics(
        self,
        dimensions: List[str],
    ) -> List[str]:  # pragma: no cover # Tested in integration tests
        """
        Return common metrics (names only) for a set of dimensions.
        """
        metrics = self.common_metrics_with_details(dimensions=dimensions)
        return [metric["name"] for metric in metrics]

    #
    # Get SQL
    #
    def sql(  # pylint: disable=too-many-arguments
        self,
        metrics: List[str],
        dimensions: Optional[List[str]] = None,
        filters: Optional[List[str]] = None,
        engine_name: Optional[str] = None,
        engine_version: Optional[str] = None,
    ):
        """
        Builds SQL for one (or multiple) metrics with the provided dimensions and filters.
        """
        if len(metrics) == 1:
            response = self._session.get(
                f"/sql/{metrics[0]}/",
                params={
                    "dimensions": dimensions or [],
                    "filters": filters or [],
                    "engine_name": engine_name or self.engine_name,
                    "engine_version": engine_version or self.engine_version,
                },
            )
        else:
            response = self._session.get(
                "/sql/",
                params={
                    "metrics": metrics,
                    "dimensions": dimensions or [],
                    "filters": filters or [],
                    "engine_name": engine_name or self.engine_name,
                    "engine_version": engine_version or self.engine_version,
                },
            )
        if response.status_code == 200:
            return response.json()["sql"]
        return response.json()

    #
    # Get data
    #
    def data(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        metrics: List[str],
        dimensions: Optional[List[str]] = None,
        filters: Optional[List[str]] = None,
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
                        "dimensions": dimensions or [],
                        "filters": filters or [],
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


class DJBuilder(DJClient):
    """
    Client class for DJ node manipulation.
    """

    #
    # Catalogs and Engines
    #
    def list_catalogs(self):
        """
        Gets all catalogs.
        """
        response = self._session.get("/catalogs/", timeout=self._timeout)
        return response.json()

    def list_engines(self):
        """
        Gets all engines.
        """
        response = self._session.get("/engines/", timeout=self._timeout)
        return response.json()

    #
    # Namespace
    #
    # TODO: add delete namespace (with warnings) # pylint: disable=fixme
    def namespace(self, namespace: str) -> "Namespace":
        """
        Returns the specified node namespace.
        """
        try:
            return self.create_namespace(namespace=namespace)
        except DJNamespaceAlreadyExists:
            pass
        return Namespace(namespace=namespace, dj_client=self)

    def create_namespace(self, namespace: str) -> "Namespace":
        """
        Helper function to create a namespace.
        """
        response = self._session.post(
            f"/namespaces/{namespace}/",
            timeout=self._timeout,
        )
        json_response = response.json()
        if response.status_code == 409:
            raise DJNamespaceAlreadyExists(json_response["message"])
        return Namespace(namespace=namespace, dj_client=self)

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
        new_source = Source(
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
        new_source.save(mode=mode)
        return new_source

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
        new_node.save(mode=mode)
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
        new_node.save(mode=mode)
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
        new_node.save(mode=mode)
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
        new_node.save(mode=mode)  # pragma: no cover
        return new_node  # pragma: no cover
