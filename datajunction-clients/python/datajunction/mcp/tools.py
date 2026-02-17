"""
MCP Tool implementations that call DJ GraphQL API via HTTP
"""

import logging
import os
from typing import Any, Dict, List, Optional

import httpx

from datajunction.mcp.config import get_mcp_settings
from datajunction.mcp.formatters import (
    format_dimensions_compatibility,
    format_error,
    format_node_details,
    format_nodes_list,
)

logger = logging.getLogger(__name__)

# Add file handler for debugging
_log_file = os.path.expanduser("~/.dj_mcp_debug.log")
_file_handler = logging.FileHandler(_log_file)
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
)
logger.addHandler(_file_handler)
logger.setLevel(logging.DEBUG)
logger.info(f"DJ MCP logging to: {_log_file}")


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
            logger.debug(
                f"Token already initialized. Has token: {self._token is not None}",
            )
            return

        logger.info("=== Initializing DJ API authentication ===")
        logger.info(f"API URL: {self.settings.dj_api_url}")
        logger.info(f"Has API token: {bool(self.settings.dj_api_token)}")
        logger.info(f"Has username: {bool(self.settings.dj_username)}")
        logger.info(f"Has password: {bool(self.settings.dj_password)}")

        # If API token is explicitly provided, use it
        if self.settings.dj_api_token:
            self._token = self.settings.dj_api_token
            self._token_initialized = True
            logger.info("✓ Using provided API token")
            return

        # Otherwise, try to login with username/password to get a JWT token
        if self.settings.dj_username and self.settings.dj_password:
            logger.info(f"Attempting login for user: {self.settings.dj_username}")
            try:
                login_url = f"{self.settings.dj_api_url.rstrip('/')}/basic/login/"
                logger.info(f"Login URL: {login_url}")

                async with httpx.AsyncClient(
                    timeout=self.settings.request_timeout,
                    follow_redirects=True,
                ) as client:
                    # Use form-encoded data as expected by OAuth2PasswordRequestForm
                    response = await client.post(
                        login_url,
                        data={
                            "username": self.settings.dj_username,
                            "password": self.settings.dj_password,
                        },
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                    )
                    logger.info(f"Login response status: {response.status_code}")
                    response.raise_for_status()

                    # Extract JWT token from cookie
                    cookies = response.cookies
                    logger.info(f"Available cookies: {list(cookies.keys())}")

                    if "__dj" in cookies:
                        self._token = cookies["__dj"]
                        logger.info(
                            f"✓ Successfully obtained JWT token (length: {len(self._token)})",
                        )
                    else:
                        logger.error(
                            "✗ Login successful but no JWT token in '__dj' cookie",
                        )
                        logger.error(f"Response body: {response.text[:500]}")
            except httpx.HTTPStatusError as e:
                logger.error(f"✗ Login failed with HTTP {e.response.status_code}")
                logger.error(f"Response: {e.response.text[:500]}")
            except Exception as e:
                logger.error(f"✗ Login exception: {type(e).__name__}: {str(e)}")

        self._token_initialized = True
        logger.info(
            f"=== Authentication initialization complete. Has token: {self._token is not None} ===",
        )

    def _get_headers(self) -> Dict[str, str]:
        """Build request headers with authentication"""
        headers = {"Content-Type": "application/json"}

        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
            logger.debug(
                f"Added Authorization header with token (length: {len(self._token)})",
            )
        else:
            logger.warning("No token available - request will be unauthenticated")

        return headers

    def _get_auth(self) -> Optional[tuple]:
        """Get basic auth - not used anymore, we use JWT tokens"""
        return None

    async def query(
        self,
        query: str,
        variables: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
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
            async with httpx.AsyncClient(
                timeout=self.settings.request_timeout,
            ) as client:
                response = await client.post(
                    self.graphql_url,
                    json={"query": query, "variables": variables or {}},
                    headers=self._get_headers(),
                )
                response.raise_for_status()
                result = response.json()

                if "errors" in result:
                    error_messages = [
                        e.get("message", "Unknown error") for e in result["errors"]
                    ]
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

    Note on Git-Backed Namespaces:
        Some namespaces may be backed by a git repository, following the pattern: {prefix}.{branch}

        Examples:
            - "xyz.main" = prefix "xyz" with branch "main" (production)
            - "xyz.feature1" = prefix "xyz" with branch "feature1" (development)

        When you see multiple namespaces sharing a prefix (e.g., xyz.main, xyz.feature1, xyz.feature2),
        they represent different git branches of the same repository. The ".main" branch typically
        contains production-grade nodes, while other branches contain development or experimental versions.
    """
    graphql_query = """
    query ListNamespaces {
        findNodes(limit: 1000) {
            name
        }
    }
    """

    try:
        client = get_client()
        data = await client.query(graphql_query)

        nodes = data.get("findNodes", [])
        if not nodes:
            return "No namespaces found."

        # Extract namespace from node name (everything before the last dot)
        from collections import Counter

        namespaces = []
        for node in nodes:
            if node.get("name"):
                parts = node["name"].rsplit(".", 1)
                if len(parts) > 1:
                    namespaces.append(parts[0])

        namespace_counts = Counter(namespaces)

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
    prefer_main_branch: bool = True,
) -> str:
    """
    Search for nodes (metrics, dimensions, cubes, etc.)

    Args:
        query: Search term (fragment of node name)
        node_type: Optional filter by type (metric, dimension, cube, source, transform)
        namespace: Optional filter by namespace (highly recommended for narrowing results)
        limit: Maximum number of results (default: 100, max: 1000)
        prefer_main_branch: If True and namespace provided, automatically uses .main branch (default: True)

    Returns:
        Formatted list of matching nodes

    Note on Git-Backed Namespaces:
        Certain namespaces may be backed by a git repository. In these cases, the namespace
        follows a pattern of: {prefix}.{branch}

        Examples:
            - For prefix "xyz" with branch "main":
              namespace = "xyz.main"

            - For prefix "xyz" with feature branch "feature1":
              namespace = "xyz.feature1"

        When prefer_main_branch=True (default), searching with namespace="finance" will automatically
        query "finance.main" if that namespace exists. To query all branches, set prefer_main_branch=False.
    """
    # Resolve namespace to .main branch if needed
    actual_namespace = namespace
    if (
        prefer_main_branch
        and namespace
        and namespace.split(".")[-1]
        not in (
            "main",
            "dev",
            "prod",
            "master",
        )
    ):
        # Get all namespaces to find the .main version
        try:
            all_namespaces_str = await list_namespaces()
            # Parse namespace names from the output (format: "namespace (N nodes)")
            import re

            namespace_pattern = r"^(.+?)\s+\(\d+ nodes\)$"
            all_namespaces = []
            for line in all_namespaces_str.split("\n"):
                match = re.match(namespace_pattern, line.strip())
                if match:
                    all_namespaces.append(match.group(1))

            # Look for namespace.main
            main_namespace = f"{namespace}.main"
            if main_namespace in all_namespaces:
                actual_namespace = main_namespace
                logger.info(
                    f"Resolved namespace '{namespace}' to '{actual_namespace}'",
                )
        except Exception as e:
            logger.warning(f"Failed to resolve namespace to main branch: {e}")
            # Continue with original namespace if resolution fails

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
            gitInfo {
                repo
                branch
                defaultBranch
            }
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
                "namespace": actual_namespace,
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
        async with httpx.AsyncClient(
            timeout=client.settings.request_timeout,
        ) as http_client:
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
            result.get("sql", "No SQL generated"),
            "",
            "Output Columns:",
            "-" * 60,
        ]

        for col in result.get("columns", []):
            semantic_info = ""
            if col.get("semantic_name"):
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
        async with httpx.AsyncClient(
            timeout=client.settings.request_timeout,
        ) as http_client:
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
        state = result.get("state", "unknown")
        lines.append(f"Query State: {state}")

        if result.get("id"):
            lines.append(f"Query ID: {result['id']}")

        # Add result info
        results_data = result.get("results", [])
        if results_data:
            lines.append(f"Row Count: {len(results_data)}")
            lines.append("")
            lines.append("Data:")
            lines.append("-" * 60)

            # Format as table (show first 10 rows)
            max_rows = min(len(results_data), 10)
            for i, row in enumerate(results_data[:max_rows]):
                lines.append(f"Row {i + 1}:")
                for key, value in row.items():
                    lines.append(f"  {key}: {value}")
                lines.append("")

            if len(results_data) > 10:
                lines.append(f"... and {len(results_data) - 10} more rows")
        else:
            lines.append("No results returned")

        # Add any errors
        if result.get("errors"):
            lines.append("")
            lines.append("Errors:")
            for error in result["errors"]:
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


async def get_node_lineage(
    node_name: str,
    direction: str = "both",
    max_depth: Optional[int] = None,
) -> str:
    """
    Get lineage information for a node (upstream/downstream dependencies)

    Args:
        node_name: Full node name (e.g., 'default.avg_repair_price')
        direction: Lineage direction - "upstream", "downstream", or "both" (default)
        max_depth: Maximum depth to traverse (None = unlimited, -1 = all)

    Returns:
        Formatted lineage information with node dependencies
    """
    try:
        client = get_client()
        await client._ensure_token()

        lines = [f"Lineage for: {node_name}", "=" * 60, ""]

        # Fetch lineage based on direction
        async with httpx.AsyncClient(
            timeout=client.settings.request_timeout,
        ) as http_client:
            if direction in ("upstream", "both"):
                params = {}
                if max_depth is not None:
                    params["max_depth"] = max_depth

                response = await http_client.get(
                    f"{client.settings.dj_api_url.rstrip('/')}/nodes/{node_name}/upstream/",
                    params=params,
                    headers=client._get_headers(),
                )
                response.raise_for_status()
                upstream_nodes = response.json()

                lines.append(f"Upstream Dependencies ({len(upstream_nodes)} nodes):")
                lines.append("-" * 60)
                if upstream_nodes:
                    for node in upstream_nodes:
                        lines.append(
                            f"  • {node['name']} ({node['type']}) - {node.get('status', 'N/A')}",
                        )
                else:
                    lines.append("  (none)")
                lines.append("")

            if direction in ("downstream", "both"):
                params = {}
                if max_depth is not None:
                    params["max_depth"] = max_depth

                response = await http_client.get(
                    f"{client.settings.dj_api_url.rstrip('/')}/nodes/{node_name}/downstream/",
                    params=params,
                    headers=client._get_headers(),
                )
                response.raise_for_status()
                downstream_nodes = response.json()

                lines.append(
                    f"Downstream Dependencies ({len(downstream_nodes)} nodes):",
                )
                lines.append("-" * 60)
                if downstream_nodes:
                    for node in downstream_nodes:
                        lines.append(
                            f"  • {node['name']} ({node['type']}) - {node.get('status', 'N/A')}",
                        )
                else:
                    lines.append("  (none)")

        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        return format_error(
            f"API request failed: {e.response.status_code} - {e.response.text}",
            f"Getting lineage for node: {node_name}",
        )
    except Exception as e:
        logger.error(f"Error getting lineage: {str(e)}")
        return format_error(
            str(e),
            f"Getting lineage for node: {node_name}",
        )


async def get_node_dimensions(node_name: str) -> str:
    """
    Get all available dimensions for a specific node

    Args:
        node_name: Full node name (e.g., 'default.avg_repair_price')

    Returns:
        Formatted list of dimensions available for the node
    """
    try:
        client = get_client()
        await client._ensure_token()

        # Call the REST API to get dimensions
        async with httpx.AsyncClient(
            timeout=client.settings.request_timeout,
        ) as http_client:
            response = await http_client.get(
                f"{client.settings.dj_api_url.rstrip('/')}/nodes/{node_name}/dimensions/",
                headers=client._get_headers(),
            )
            response.raise_for_status()
            dimensions = response.json()

        # Format the response
        lines = [
            f"Dimensions for: {node_name}",
            "=" * 60,
            "",
            f"Total: {len(dimensions)} dimensions",
            "",
            "Available Dimensions:",
            "-" * 60,
        ]

        if dimensions:
            # Group by dimension type if available
            for dim in dimensions:
                dim_info = f"  • {dim['name']}"
                if dim.get("type"):
                    dim_info += f" ({dim['type']})"
                if dim.get("path"):
                    dim_info += f" - via: {' → '.join(dim['path'])}"
                lines.append(dim_info)
        else:
            lines.append("  (no dimensions available)")

        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        return format_error(
            f"API request failed: {e.response.status_code} - {e.response.text}",
            f"Getting dimensions for node: {node_name}",
        )
    except Exception as e:
        logger.error(f"Error getting dimensions: {str(e)}")
        return format_error(
            str(e),
            f"Getting dimensions for node: {node_name}",
        )
