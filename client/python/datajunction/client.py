"""DataJunction main client module."""

import time
from typing import List, Optional
from urllib.parse import urlencode

from alive_progress import alive_bar

from datajunction import _internal, models
from datajunction.exceptions import DJClientException


class DJReader(_internal.DJClient):
    """
    Client class to consume basic DJ services: metrics and dimensions.
    """

    #
    # List namespace, metrics and dimensions
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
        return self.get_nodes_in_namespace(
            namespace=namespace,
            type_="dimension",
        )

    def list_metrics(self, namespace: Optional[str] = None) -> List[str]:
        """
        List metric nodes for given namespace or all.
        """
        if namespace:
            return self.get_nodes_in_namespace(
                namespace=namespace,
                type_="metric",
            )
        return self._session.get("/metrics/").json()

    #
    # Get common metrics and dimensions
    #
    def common_dimensions(
        self,
        metrics: List[str],
    ) -> List:  # pragma: no cover # Tested in integration tests
        """
        Return common dimensions for a set of metrics.
        """
        query_params = []
        for metric in metrics:
            query_params.append(("metric", metric))
        response = self._session.get(
            f"/metrics/common/dimensions/?{urlencode(query_params)}",
        )
        return response.json()

    def common_metrics(
        self,
        dimensions: List[str],
    ) -> List[str]:  # pragma: no cover # Tested in integration tests
        """
        Return common metrics for a set of dimensions.
        """
        query_params = [("node_type", models.NodeType.METRIC.value)]
        for dim in dimensions:
            query_params.append(("dimension", dim))
        response = self._session.get(
            f"/dimensions/common/?{urlencode(query_params)}",
        )
        return [metric["name"] for metric in response.json()]

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


class DJWriter(DJReader):
    """
    Client class to manage all your DJ dag: nodes, attributes, namespaces, tags.
    """
