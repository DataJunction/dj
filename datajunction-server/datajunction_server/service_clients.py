"""Clients for various configurable services."""

import logging
from http import HTTPStatus
from typing import TYPE_CHECKING, Dict, List, Optional, Union, Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from datajunction_server.database.column import Column
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJQueryServiceClientEntityNotFound,
    DJQueryServiceClientException,
)
from datajunction_server.models.cube_materialization import (
    CubeMaterializationV2Input,
    DruidCubeMaterializationInput,
)
from datajunction_server.models.materialization import (
    DruidMaterializationInput,
    GenericMaterializationInput,
    MaterializationInfo,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import PartitionBackfill
from datajunction_server.models.query import QueryCreate, QueryWithResults
from datajunction_server.sql.parsing.types import ColumnType

if TYPE_CHECKING:
    from datajunction_server.models.preaggregation import (
        BackfillInput,
        CubeBackfillInput,
        PreAggMaterializationInput,
    )
    from datajunction_server.database.engine import Engine

_logger = logging.getLogger(__name__)


class RequestsSessionWithEndpoint(requests.Session):
    """
    Creates a requests session that comes with an endpoint that all
    subsequent requests will use as a prefix.
    """

    def __init__(self, endpoint: str = None, retry_strategy: Retry = None):
        super().__init__()
        self.endpoint = endpoint
        self.mount("http://", HTTPAdapter(max_retries=retry_strategy))
        self.mount("https://", HTTPAdapter(max_retries=retry_strategy))

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


class QueryServiceClient:
    """
    Client for the query service.
    """

    def __init__(self, uri: str, retries: int = 0):
        self.uri = uri
        retry_strategy = Retry(
            total=retries,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "PATCH"],
        )
        self.requests_session = RequestsSessionWithEndpoint(
            endpoint=self.uri,
            retry_strategy=retry_strategy,
        )

    def get_columns_for_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        request_headers: Optional[Dict[str, str]] = None,
        engine: Optional["Engine"] = None,
    ) -> List[Column]:
        """
        Retrieves columns for a table.
        """
        response = self.requests_session.get(
            f"/table/{catalog}.{schema}.{table}/columns/",
            params={
                "engine": engine.name,
                "engine_version": engine.version,
            }
            if engine
            else {},
            headers=self.requests_session.headers,
        )
        if response.status_code not in (200, 201):
            if response.status_code == HTTPStatus.NOT_FOUND:
                raise DJDoesNotExistException(
                    message=f"Table not found: {response.text}",
                )
            raise DJQueryServiceClientException(
                message=f"Error response from query service: {response.text}",
            )
        table_columns = response.json()["columns"]
        if not table_columns:
            raise DJDoesNotExistException(
                message=f"No columns found: {response.text}",
            )
        return [
            Column(name=column["name"], type=ColumnType(column["type"]), order=idx)
            for idx, column in enumerate(table_columns)
        ]

    def create_view(
        self,
        view_name: str,
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Re-create a view using the query service.
        """
        response = self.requests_session.post(
            "/queries/",
            headers=self.requests_session.headers,
            json=query_create.model_dump(),
        )
        if response.status_code not in (200, 201):
            raise DJQueryServiceClientException(
                message=f"Error response from query service: {response.text}",
                http_status_code=response.status_code,
            )
        return f"View '{view_name}' created successfully."

    def submit_query(
        self,
        query_create: QueryCreate,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> QueryWithResults:
        """
        Submit a query to the query service
        """
        response = self.requests_session.post(
            "/queries/",
            headers={
                **self.requests_session.headers,
                "accept": "application/json",
            },
            json=query_create.model_dump(),
        )
        if response.status_code not in (200, 201):
            raise DJQueryServiceClientException(
                message=f"Error response from query service: {response.text}",
                http_status_code=response.status_code,
            )
        query_info = response.json()
        return QueryWithResults(**query_info)

    def get_query(
        self,
        query_id: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> QueryWithResults:
        """
        Get a previously submitted query
        """
        get_query_endpoint = f"/queries/{query_id}/"
        response = self.requests_session.get(
            get_query_endpoint,
            headers=self.requests_session.headers,
        )
        if response.status_code == 404:
            _logger.exception(
                "[DJQS] Failed to get query_id=%s with `GET %s`",
                query_id,
                get_query_endpoint,
                exc_info=True,
            )
            raise DJQueryServiceClientEntityNotFound(  # pragma: no cover
                message=f"Error response from query service: {response.text}",
            )
        if response.status_code not in (200, 201):
            raise DJQueryServiceClientException(
                message=f"Error response from query service: {response.text}",
            )
        query_info = response.json()
        _logger.info(
            "[DJQS] Retrieved query_id=%s with `GET %s`",
            query_id,
            get_query_endpoint,
        )
        return QueryWithResults(**query_info)

    def materialize(
        self,
        materialization_input: Union[
            GenericMaterializationInput,
            DruidMaterializationInput,
        ],
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Post a request to the query service asking it to set up a scheduled materialization
        for the node. The query service is expected to manage all reruns of this job. Note
        that this functionality may be moved to the materialization service at a later point.
        """
        response = self.requests_session.post(
            "/materialization/",
            json=materialization_input.model_dump(),
            headers=self.requests_session.headers,
        )
        if response.status_code not in (200, 201):  # pragma: no cover
            _logger.exception(
                "[DJQS] Failed to materialize node=%s with `POST /materialization/`: %s",
                materialization_input.node_name,
                materialization_input.model_dump(),
                exc_info=True,
            )
            return MaterializationInfo(urls=[], output_tables=[])
        result = response.json()
        _logger.info(
            "[DJQS] Scheduled materialization for node=%s with `POST /materialization/`",
            materialization_input.node_name,
        )
        return MaterializationInfo(**result)

    def materialize_cube(
        self,
        materialization_input: DruidCubeMaterializationInput,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Post a request to the query service asking it to set up a scheduled materialization
        for the node. The query service is expected to manage all reruns of this job. Note
        that this functionality may be moved to the materialization service at a later point.
        """
        response = self.requests_session.post(
            "/cubes/materialize",
            json=materialization_input.model_dump(),
            headers=self.requests_session.headers,
            timeout=20,
        )
        if response.status_code not in (200, 201):  # pragma: no cover
            _logger.exception(
                "[DJQS] Failed to schedule cube materialization for"
                " node=%s with `POST /cubes/materialize`: %s",
                materialization_input.cube,
                response.text,
                exc_info=True,
            )
            return MaterializationInfo(urls=[], output_tables=[])  # pragma: no cover
        result = response.json()  # pragma: no cover
        _logger.info(
            "[DJQS] Scheduled cube materialization for node=%s with `POST /cubes/materialize`",
            materialization_input.cube,
        )
        return MaterializationInfo(**result)  # pragma: no cover

    def materialize_cube_v2(
        self,
        materialization_input: CubeMaterializationV2Input,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Create a v2 cube materialization workflow (pre-agg based).

        This calls the query service's POST /cubes/materialize/v2 endpoint which creates
        a workflow that:
        1. Waits for pre-agg tables to be available (via VTTS)
        2. Runs the combined SQL that joins/coalesces pre-agg data
        3. Ingests the combined result to Druid

        Unlike materialize_cube(), this does NOT create measures materialization
        workflows inline - it expects pre-aggs to already exist.
        """
        response = self.requests_session.post(
            "/cubes/materialize/v2",
            json=materialization_input.model_dump(),
            headers=self.requests_session.headers,
            timeout=30,
        )
        if response.status_code not in (200, 201):
            _logger.exception(
                "[DJQS] Failed to schedule v2 cube materialization for"
                " cube=%s with `POST /cubes/materialize/v2`: %s",
                materialization_input.cube_name,
                response.text,
                exc_info=True,
            )
            raise Exception(f"Query service error: {response.text}")
        result = response.json()
        _logger.info(
            "[DJQS] Scheduled v2 cube materialization for cube=%s with "
            "`POST /cubes/materialize/v2`, urls=%s",
            materialization_input.cube_name,
            result.get("urls"),
        )
        return MaterializationInfo(**result)

    def materialize_preagg(
        self,
        materialization_input: "PreAggMaterializationInput",
        request_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Create a scheduled workflow for a pre-aggregation.

        This creates/updates the recurring workflow that materializes the pre-agg
        on the configured schedule. The query service will:
        1. Create/update the scheduled workflow
        2. Execute on schedule (or immediately if triggered)
        3. Callback to DJ's POST /preaggs/{preagg_id}/availability/ when done

        Returns:
            Dict with 'workflow_url', 'status', and optionally 'urls', 'output_tables'
        """
        response = self.requests_session.post(
            "/preaggs/materialize",
            json=materialization_input.model_dump(mode="json"),
            headers=self.requests_session.headers,
            timeout=30,
        )
        if response.status_code not in (200, 201):
            _logger.exception(
                "[DJQS] Failed to create workflow for preagg_id=%s: %s",
                materialization_input.preagg_id,
                response.text,
                exc_info=True,
            )
            raise Exception(f"Query service error: {response.text}")
        result = response.json()
        _logger.info(
            "[DJQS] Created workflow for preagg_id=%s, output_table=%s, "
            "workflow_url=%s, status=%s",
            materialization_input.preagg_id,
            materialization_input.output_table,
            result.get("workflow_url"),
            result.get("status"),
        )
        return result

    def deactivate_preagg_workflow(
        self,
        output_table: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Deactivate a pre-aggregation's workflows by output table name.

        Args:
            output_table: The pre-aggregation's output table name (resource identifier).
                         Query Service owns the workflow naming pattern and reconstructs
                         workflow names from this identifier.

        Returns:
            Dict with 'status'
        """
        response = self.requests_session.delete(
            f"/preaggs/{output_table}/workflow",
            headers=self.requests_session.headers,
            timeout=20,
        )
        if response.status_code not in (200, 201, 204):
            _logger.exception(
                "[DJQS] Failed to deactivate preagg workflow for output_table=%s: %s",
                output_table,
                response.text,
                exc_info=True,
            )
            raise Exception(f"Query service error: {response.text}")
        result = response.json() if response.text else {}
        _logger.info(
            "[DJQS] Deactivated preagg workflows for output_table=%s",
            output_table,
        )
        return result

    def deactivate_cube_workflow(
        self,
        cube_name: str,
        version: Optional[str] = None,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Deactivate a cube's Druid materialization workflows by cube name and version.

        Args:
            cube_name: Full cube name (e.g., 'default.my_cube').
            version: Optional cube version. If provided, deactivates version-specific
                     workflows. If not provided, falls back to legacy naming.

        Returns:
            Dict with 'status'
        """
        url = f"/cubes/{cube_name}/workflow"
        if version:
            url = f"{url}?version={version}"

        response = self.requests_session.delete(
            url,
            headers=self.requests_session.headers,
            timeout=20,
        )
        if response.status_code not in (200, 201, 204):
            _logger.warning(
                "[DJQS] Failed to deactivate cube workflow for cube=%s version=%s: %s",
                cube_name,
                version,
                response.text,
            )
            # Don't raise - the query service endpoint may not exist yet
            return {"status": "failed", "message": response.text}
        result = response.json() if response.text else {}
        _logger.info(
            "[DJQS] Deactivated cube workflows for cube=%s version=%s",
            cube_name,
            version,
        )
        return result

    def run_preagg_backfill(
        self,
        backfill_input: "BackfillInput",
        request_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Run a backfill for a pre-aggregation.

        Returns:
            Dict with 'job_url'
        """
        response = self.requests_session.post(
            "/preaggs/backfill",
            json=backfill_input.model_dump(mode="json"),
            headers=self.requests_session.headers,
            timeout=30,
        )
        if response.status_code not in (200, 201):
            _logger.exception(
                "[DJQS] Failed to run backfill for preagg_id=%s: %s",
                backfill_input.preagg_id,
                response.text,
                exc_info=True,
            )
            raise Exception(f"Query service error: {response.text}")
        result = response.json()
        _logger.info(
            "[DJQS] Started backfill for preagg_id=%s, job_url=%s",
            backfill_input.preagg_id,
            result.get("job_url"),
        )
        return result

    def run_cube_backfill(
        self,
        backfill_input: "CubeBackfillInput",
        request_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Run a backfill for a cube.

        Returns:
            Dict with 'job_url'
        """
        response = self.requests_session.post(
            "/cubes/backfill",
            json=backfill_input.model_dump(mode="json"),
            headers=self.requests_session.headers,
            timeout=30,
        )
        if response.status_code not in (200, 201):
            _logger.exception(
                "[DJQS] Failed to run backfill for cube=%s: %s",
                backfill_input.cube_name,
                response.text,
                exc_info=True,
            )
            raise Exception(f"Query service error: {response.text}")
        result = response.json()
        _logger.info(
            "[DJQS] Started backfill for cube=%s, job_url=%s",
            backfill_input.cube_name,
            result.get("job_url"),
        )
        return result

    def deactivate_materialization(
        self,
        node_name: str,
        materialization_name: str,
        node_version: str | None = None,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Deactivates the specified node materialization
        """
        deactivate_endpoint = f"/materialization/{node_name}/{materialization_name}/"
        response = self.requests_session.delete(
            deactivate_endpoint,
            headers=self.requests_session.headers,
            json={"node_version": node_version} if node_version else {},
        )
        if response.status_code not in (200, 201):  # pragma: no cover
            _logger.exception(
                "[DJQS] Failed to deactivate materialization for node=%s, version=%s with `DELETE %s`",
                node_name,
                node_version,
                deactivate_endpoint,
                exc_info=True,
            )
            return MaterializationInfo(urls=[], output_tables=[])
        result = response.json()
        _logger.info(
            "[DJQS] Deactivated materialization for node=%s, version=%s with `DELETE %s`",
            node_name,
            node_version,
            deactivate_endpoint,
        )
        return MaterializationInfo(**result)

    def refresh_cube_materialization(
        self,
        cube_name: str,
        cube_version: Optional[str] = None,
        materializations: Optional[List[Dict]] = None,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Refresh/rebuild materialization workflows for a cube without creating a new version.

        This calls the query service to recreate the cube's materialization workflows
        with the same names, effectively overwriting them in the scheduler.

        Args:
            cube_name: Name of the cube node
            cube_version: Optional cube version (defaults to latest if not specified)
            materializations: List of active materialization dicts to rebuild
            request_headers: Optional HTTP headers

        Returns:
            MaterializationInfo with URLs of recreated workflows
        """
        refresh_endpoint = f"/cubes/{cube_name}/refresh-materialization"
        params = {}
        if cube_version:
            params["cube_version"] = cube_version

        _logger.info(
            "[DJQS] Refreshing materializations for cube=%s version=%s",
            cube_name,
            cube_version or "latest",
        )

        # Merge request headers with session headers
        headers = dict(self.requests_session.headers)
        if request_headers:
            headers.update(request_headers)

        # Build request body with materialization data
        body = {}
        if materializations is not None:
            body["materializations"] = materializations

        response = self.requests_session.post(
            refresh_endpoint,
            headers=headers,
            params=params,
            json=body,
        )

        if response.status_code not in (200, 201):  # pragma: no cover
            _logger.exception(
                "[DJQS] Failed to refresh materializations for cube=%s with `POST %s`: %s",
                cube_name,
                refresh_endpoint,
                response.text,
                exc_info=True,
            )
            return MaterializationInfo(urls=[], output_tables=[])

        result = response.json()
        _logger.info(
            "[DJQS] Successfully refreshed materializations for cube=%s with `POST %s`",
            cube_name,
            refresh_endpoint,
        )
        return MaterializationInfo(**result)

    def get_materialization_info(
        self,
        node_name: str,
        node_version: str,
        node_type: NodeType,
        materialization_name: str,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Gets materialization info for the node and materialization config name.
        """
        info_endpoint = (
            f"/materialization/{node_name}/{node_version}/{materialization_name}/"
            f"?node_type={node_type}"
        )
        response = self.requests_session.get(
            info_endpoint,
            timeout=3,
            headers=self.requests_session.headers,
        )
        if response.status_code not in (200, 201):
            _logger.exception(
                "[DJQS] Failed to get materialization info for node=%s with `GET %s`",
                node_name,
                info_endpoint,
                exc_info=True,
            )
            return MaterializationInfo(output_tables=[], urls=[])

        _logger.info(
            "[DJQS] Retrieved materialization info for node=%s with `GET %s`",
            node_name,
            info_endpoint,
        )
        return MaterializationInfo(**response.json())

    def run_backfill(
        self,
        node_name: str,
        node_version: str,
        node_type: NodeType,
        materialization_name: str,
        partitions: List[PartitionBackfill],
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """Kicks off a backfill with the given backfill spec"""
        backfill_endpoint = (
            f"/materialization/run/{node_name}/{materialization_name}"
            f"/?node_version={node_version}&node_type={node_type}"
        )
        response = self.requests_session.post(
            backfill_endpoint,
            json=[partition.model_dump() for partition in partitions],
            headers=self.requests_session.headers,
            timeout=20,
        )
        if response.status_code not in (200, 201):
            _logger.exception(  # pragma: no cover
                "[DJQS] Failed to run backfill for node=%s with `POST %s`",
                node_name,
                backfill_endpoint,
                exc_info=True,
            )
            return MaterializationInfo(output_tables=[], urls=[])  # pragma: no cover

        _logger.info(
            "[DJQS] Ran backfill for node=%s with `POST %s`",
            node_name,
            backfill_endpoint,
        )
        return MaterializationInfo(**response.json())
