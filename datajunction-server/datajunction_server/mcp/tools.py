"""
MCP Tool implementations that call DJ GraphQL API via HTTP
"""
import logging
from typing import Any, Dict, List, Optional

import httpx

from datajunction_server.mcp.config import get_mcp_settings
from datajunction_server.mcp.formatters import (
    format_dimensions_compatibility,
    format_error,
    format_node_details,
    format_nodes_list,
    format_sql_response,
)

logger = logging.getLogger(__name__)


class DJGraphQLClient:
    """Client for making GraphQL requests to DJ API"""

    def __init__(self):
        self.settings = get_mcp_settings()
        self.graphql_url = f"{self.settings.dj_api_url.rstrip('/')}/graphql"
        self._token: Optional[str] = None
        self._token_initialized = False

    async def _ensure_token(self):
        """Ensure we have a valid JWT token for authentication"""
        if self._token_initialized:
            return

        # If API token is explicitly provided, use it
        if self.settings.dj_api_token:
            self._token = self.settings.dj_api_token
            self._token_initialized = True
            return

        # Otherwise, try to login with username/password to get a JWT token
        if self.settings.dj_username and self.settings.dj_password:
            try:
                async with httpx.AsyncClient(timeout=self.settings.request_timeout) as client:
                    response = await client.post(
                        f"{self.settings.dj_api_url.rstrip('/')}/basic/login/",
                        data={
                            "username": self.settings.dj_username,
                            "password": self.settings.dj_password,
                        },
                    )
                    response.raise_for_status()

                    # Extract JWT token from cookie
                    cookies = response.cookies
                    if "dj_auth" in cookies:
                        self._token = cookies["dj_auth"]
                        logger.info("Successfully obtained JWT token via login")
                    else:
                        logger.warning("Login successful but no JWT token in response")
            except Exception as e:
                logger.error(f"Failed to login and obtain JWT token: {str(e)}")

        self._token_initialized = True

    def _get_headers(self) -> Dict[str, str]:
        """Build request headers with authentication"""
        headers = {"Content-Type": "application/json"}

        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        return headers

    def _get_auth(self) -> Optional[tuple]:
        """Get basic auth - not used anymore, we use JWT tokens"""
        return None

    async def query(self, query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Execute a GraphQL query

        Args:
            query: GraphQL query string
            variables: Query variables

        Returns:
            GraphQL response data

        Raises:
            Exception: If request fails
        """
        await self._ensure_token()

        try:
            async with httpx.AsyncClient(timeout=self.settings.request_timeout) as client:
                response = await client.post(
                    self.graphql_url,
                    json={"query": query, "variables": variables or {}},
                    headers=self._get_headers(),
                )
                response.raise_for_status()
                result = response.json()

                if "errors" in result:
                    error_messages = [e.get("message", "Unknown error") for e in result["errors"]]
                    raise Exception(f"GraphQL errors: {'; '.join(error_messages)}")

                return result.get("data", {})

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
            raise Exception(f"API request failed: {e.response.status_code}")
        except httpx.RequestError as e:
            logger.error(f"Request error: {str(e)}")
            raise Exception(f"Failed to connect to DJ API: {str(e)}")


# Global client instance
_client = None


def get_client() -> DJGraphQLClient:
    """Get or create GraphQL client singleton"""
    global _client
    if _client is None:
        _client = DJGraphQLClient()
    return _client


async def list_namespaces() -> str:
    """
    List all available namespaces in the DataJunction instance.
    Namespaces are the primary organizational structure for nodes.

    Returns:
        Formatted list of namespaces with node counts
    """
    graphql_query = """
    query ListNamespaces {
        findNodes(limit: 1000) {
            namespace
        }
    }
    """

    try:
        client = get_client()
        data = await client.query(graphql_query)

        nodes = data.get("findNodes", [])
        if not nodes:
            return "No namespaces found."

        # Count nodes per namespace
        from collections import Counter
        namespace_counts = Counter(node["namespace"] for node in nodes if node.get("namespace"))

        # Format output
        lines = ["Available Namespaces:", ""]
        for namespace, count in sorted(namespace_counts.items()):
            lines.append(f"  • {namespace} ({count} nodes)")

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"Error listing namespaces: {str(e)}")
        return f"Error: {str(e)}"


async def search_nodes(
    query: str,
    node_type: Optional[str] = None,
    namespace: Optional[str] = None,
    limit: int = 100,
) -> str:
    """
    Search for nodes (metrics, dimensions, cubes, etc.)

    Args:
        query: Search term (fragment of node name)
        node_type: Optional filter by type (metric, dimension, cube, source, transform)
        namespace: Optional filter by namespace (highly recommended for narrowing results)
        limit: Maximum number of results (default: 100, max: 1000)

    Returns:
        Formatted list of matching nodes
    """
    graphql_query = """
    query SearchNodes(
        $fragment: String,
        $nodeTypes: [NodeType!],
        $namespace: String,
        $limit: Int
    ) {
        findNodes(
            fragment: $fragment,
            nodeTypes: $nodeTypes,
            namespace: $namespace,
            limit: $limit
        ) {
            name
            type
            createdAt
            current {
                displayName
                description
                status
                mode
            }
            tags {
                name
                tagType
            }
            owners {
                username
                email
            }
        }
    }
    """

    try:
        client = get_client()
        data = await client.query(
            graphql_query,
            {
                "fragment": query,
                "nodeTypes": [node_type.upper()] if node_type else None,
                "namespace": namespace,
                "limit": limit,
            },
        )

        nodes = data.get("findNodes", [])
        return format_nodes_list(nodes)

    except Exception as e:
        logger.error(f"Error searching nodes: {str(e)}")
        return format_error(str(e), f"Searching for nodes matching '{query}'")


async def get_node_details(name: str) -> str:
    """
    Get detailed information about a specific node

    Args:
        name: Full node name (e.g., 'finance.daily_revenue')

    Returns:
        Formatted detailed node information
    """
    graphql_query = """
    query GetNodeDetails($names: [String!]!) {
        findNodes(names: $names) {
            name
            type
            createdAt
            current {
                displayName
                description
                status
                mode
                query
                metricMetadata {
                    direction
                    unit {
                        name
                        label
                    }
                }
                columns {
                    name
                    type
                    displayName
                }
                parents {
                    name
                    type
                }
            }
            tags {
                name
                tagType
                description
            }
            owners {
                username
                email
                name
            }
        }
        commonDimensions(nodes: $names) {
            name
            type
            dimensionNode {
                name
                current {
                    description
                    displayName
                }
            }
        }
    }
    """

    try:
        client = get_client()
        data = await client.query(graphql_query, {"names": [name]})

        nodes = data.get("findNodes", [])
        if not nodes:
            return f"Node '{name}' not found."

        node = nodes[0]
        dimensions = data.get("commonDimensions", [])

        return format_node_details(node, dimensions)

    except Exception as e:
        logger.error(f"Error getting node details: {str(e)}")
        return format_error(str(e), f"Fetching details for node '{name}'")


async def get_common_dimensions(metric_names: List[str]) -> str:
    """
    Find dimensions that are common across multiple metrics

    Args:
        metric_names: List of metric node names

    Returns:
        Formatted dimension compatibility report
    """
    graphql_query = """
    query GetCommonDimensions($nodes: [String!]) {
        commonDimensions(nodes: $nodes) {
            name
            type
            dimensionNode {
                name
                current {
                    description
                    displayName
                }
            }
        }
    }
    """

    try:
        client = get_client()
        data = await client.query(graphql_query, {"nodes": metric_names})

        dimensions = data.get("commonDimensions", [])
        return format_dimensions_compatibility(metric_names, dimensions)

    except Exception as e:
        logger.error(f"Error getting common dimensions: {str(e)}")
        return format_error(
            str(e),
            f"Finding common dimensions for metrics: {', '.join(metric_names)}",
        )


async def build_metric_sql(
    metrics: List[str],
    dimensions: Optional[List[str]] = None,
    filters: Optional[List[str]] = None,
    orderby: Optional[List[str]] = None,
    limit: Optional[int] = None,
    dialect: Optional[str] = None,
    use_materialized: bool = True,
) -> str:
    """
    Generate SQL for querying metrics with dimensions and filters using v3 SQL builder

    Args:
        metrics: List of metric node names to query
        dimensions: Optional list of dimensions to group by
        filters: Optional list of SQL filter conditions
        orderby: Optional list of columns to order by
        limit: Optional row limit
        dialect: Optional SQL dialect (e.g., 'spark', 'trino', 'postgres')
        use_materialized: Whether to use materialized tables when available (default: True)

    Returns:
        Formatted SQL with metadata
    """
    try:
        client = get_client()
        await client._ensure_token()

        # Build query parameters for v3 SQL endpoint
        params = {
            "metrics": metrics,
            "dimensions": dimensions or [],
            "filters": filters or [],
            "orderby": orderby or [],
            "use_materialized": use_materialized,
        }
        if limit:
            params["limit"] = limit
        if dialect:
            params["dialect"] = dialect

        # Call v3 SQL metrics endpoint via REST
        async with httpx.AsyncClient(timeout=client.settings.request_timeout) as http_client:
            response = await http_client.get(
                f"{client.settings.dj_api_url.rstrip('/')}/sql/metrics/v3/",
                params=params,
                headers=client._get_headers(),
            )
            response.raise_for_status()
            result = response.json()

        # Format the response
        lines = [
            "Generated SQL Query:",
            "=" * 60,
            "",
            f"Dialect: {result.get('dialect', 'N/A')}",
            f"Cube: {result.get('cube_name', 'N/A')}",
            "",
            "SQL:",
            "-" * 60,
            result.get('sql', 'No SQL generated'),
            "",
            "Output Columns:",
            "-" * 60,
        ]

        for col in result.get('columns', []):
            semantic_info = ""
            if col.get('semantic_name'):
                semantic_info = f" (semantic: {col['semantic_name']})"
            lines.append(f"  • {col['name']}: {col['type']}{semantic_info}")

        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        return format_error(
            f"API request failed: {e.response.status_code} - {e.response.text}",
            f"Building SQL for metrics: {', '.join(metrics)}",
        )
    except Exception as e:
        logger.error(f"Error building SQL: {str(e)}")
        return format_error(
            str(e),
            f"Building SQL for metrics: {', '.join(metrics)}",
        )


async def get_metric_data(
    metrics: List[str],
    dimensions: Optional[List[str]] = None,
    filters: Optional[List[str]] = None,
    orderby: Optional[List[str]] = None,
    limit: Optional[int] = None,
    use_materialized: bool = True,
) -> str:
    """
    Execute a query to get actual data for metrics with dimensions and filters

    Args:
        metrics: List of metric node names to query
        dimensions: Optional list of dimensions to group by
        filters: Optional list of SQL filter conditions
        orderby: Optional list of columns to order by
        limit: Optional row limit (recommended to set a reasonable limit)
        use_materialized: Whether to use materialized tables when available (default: True)

    Returns:
        Formatted data results with rows
    """
    try:
        client = get_client()
        await client._ensure_token()

        # Build query parameters for /data/ endpoint
        params = {
            "metrics": metrics,
            "dimensions": dimensions or [],
            "filters": filters or [],
            "orderby": orderby or [],
            "use_materialized": use_materialized,
            "async_": False,  # Get results synchronously
        }
        if limit:
            params["limit"] = limit

        # Call /data/ endpoint via REST
        async with httpx.AsyncClient(timeout=client.settings.request_timeout) as http_client:
            response = await http_client.get(
                f"{client.settings.dj_api_url.rstrip('/')}/data/",
                params=params,
                headers=client._get_headers(),
            )
            response.raise_for_status()
            result = response.json()

        # Format the response
        lines = [
            "Query Results:",
            "=" * 60,
            "",
        ]

        # Add query state info
        state = result.get('state', 'unknown')
        lines.append(f"Query State: {state}")

        if result.get('id'):
            lines.append(f"Query ID: {result['id']}")

        # Add result info
        results_data = result.get('results', [])
        if results_data:
            lines.append(f"Row Count: {len(results_data)}")
            lines.append("")
            lines.append("Data:")
            lines.append("-" * 60)

            # Format as table (show first 10 rows)
            max_rows = min(len(results_data), 10)
            for i, row in enumerate(results_data[:max_rows]):
                lines.append(f"Row {i+1}:")
                for key, value in row.items():
                    lines.append(f"  {key}: {value}")
                lines.append("")

            if len(results_data) > 10:
                lines.append(f"... and {len(results_data) - 10} more rows")
        else:
            lines.append("No results returned")

        # Add any errors
        if result.get('errors'):
            lines.append("")
            lines.append("Errors:")
            for error in result['errors']:
                lines.append(f"  • {error}")

        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        return format_error(
            f"API request failed: {e.response.status_code} - {e.response.text}",
            f"Getting data for metrics: {', '.join(metrics)}",
        )
    except Exception as e:
        logger.error(f"Error getting data: {str(e)}")
        return format_error(
            str(e),
            f"Getting data for metrics: {', '.join(metrics)}",
        )

