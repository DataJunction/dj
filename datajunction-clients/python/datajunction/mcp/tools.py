"""
MCP Tool implementations that call DJ GraphQL API via HTTP
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
import mcp.types as types

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
) -> str:
    """
    Execute a query to get actual data for metrics with dimensions and filters.

    IMPORTANT: This function ONLY works with materialized cubes and will refuse
    to execute expensive ad-hoc queries.

    Args:
        metrics: List of metric node names to query
        dimensions: Optional list of dimensions to group by
        filters: Optional list of SQL filter conditions
        orderby: Optional list of columns to order by
        limit: Optional row limit (recommended to set a reasonable limit)

    Returns:
        Formatted data results with rows
    """
    try:
        client = get_client()
        await client._ensure_token()

        # Build query parameters
        params: dict[str, Any] = {
            "metrics": metrics,
            "dimensions": dimensions or [],
            "filters": filters or [],
            "orderby": orderby or [],
        }
        if limit:
            params["limit"] = limit

        async with httpx.AsyncClient(
            timeout=client.settings.request_timeout,
        ) as http_client:
            # STEP 1: Check for materialization first (ALWAYS)
            logger.info("Checking for materialized cube availability...")
            sql_response = await http_client.get(
                f"{client.settings.dj_api_url.rstrip('/')}/sql/metrics/v3",
                params=params,
                headers=client._get_headers(),
            )
            sql_response.raise_for_status()
            sql_result = sql_response.json()

            # Check if materialized tables are used
            generated_sql = sql_result.get("sql", "")
            is_materialized = any(
                indicator in generated_sql.lower()
                for indicator in ["preagg", "materialized", "_cube_", "druid"]
            )

            if not is_materialized:
                return format_error(
                    "⚠️  No materialized cube available for this query.\n\n"
                    "This query would require an expensive ad-hoc computation.\n"
                    "Please ensure:\n"
                    "  • A cube exists for these metrics\n"
                    "  • The cube has been materialized\n"
                    "  • The query matches the cube's grain\n\n"
                    f"Attempted query:\n  Metrics: {', '.join(metrics)}\n"
                    f"  Dimensions: {', '.join(dimensions or ['none'])}",
                )

            logger.info("✓ Materialized cube found, executing query...")

            # STEP 2: Call /data/ endpoint via REST (always with use_materialized=True)
            data_params = {**params, "use_materialized": True, "async_": False}
            response = await http_client.get(
                f"{client.settings.dj_api_url.rstrip('/')}/data/",
                params=data_params,
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


async def visualize_metrics(
    metrics: List[str],
    dimensions: Optional[List[str]] = None,
    filters: Optional[List[str]] = None,
    orderby: Optional[List[str]] = None,
    limit: int = 100,
    chart_type: str = "line",
    title: Optional[str] = None,
    y_min: Optional[float] = None,
) -> list[types.TextContent]:
    """
    Query metrics and generate a text-based terminal visualization

    Args:
        metrics: List of metric node names to visualize (e.g., ["default.revenue", "default.orders"])
        dimensions: Optional list of dimensions to group by (first used for x-axis)
                   (e.g., ["default.customer.country", "default.date_dim.dateint"])
        filters: Optional list of SQL filter conditions (e.g., ["country = 'US'", "revenue > 1000"])
        orderby: Optional list of columns to order by. Must use FULL node names:
                 - For metrics: "default.revenue" or "default.revenue DESC"
                 - For dimensions: "default.customer.country" or "default.date_dim.dateint ASC"
                 - NOT short names like "revenue" or "country"
        limit: Maximum number of data points (default: 100)
        chart_type: Type of chart - 'line', 'bar', or 'scatter' (default: 'line')
                   Note: Automatically switches to 'bar' for categorical x-axis data
        title: Optional chart title
        y_min: Optional minimum value for y-axis (default: auto-scale). Set to 0 to start at zero.

    Returns:
        List containing TextContent with ASCII chart and metadata

    Note:
        This tool requires a materialized cube for performance. It will check if
        materialized tables are available before executing the query. If no
        materialized cube exists for the requested metrics/dimensions, it will
        return an error instead of running an expensive ad-hoc query.
    """
    try:
        # Import plotext here to avoid requiring it for other tools
        import plotext as plt

        logger.info(
            f"visualize_metrics called with y_min={y_min} (type: {type(y_min)})",
        )

        client = get_client()
        await client._ensure_token()

        # Build query parameters
        params: dict[str, Any] = {
            "metrics": metrics,
            "dimensions": dimensions or [],
            "filters": filters or [],
            "orderby": orderby or [],
            "limit": limit,
        }

        async with httpx.AsyncClient(
            timeout=client.settings.request_timeout,
        ) as http_client:
            # STEP 1: Call /sql/metrics/v3 to generate SQL WITHOUT executing
            logger.info("Checking for materialized cube availability...")
            sql_response = await http_client.get(
                f"{client.settings.dj_api_url.rstrip('/')}/sql/metrics/v3",
                params=params,
                headers=client._get_headers(),
            )
            sql_response.raise_for_status()
            sql_result = sql_response.json()

            # Check if materialized tables are used
            generated_sql = sql_result.get("sql", "")
            is_materialized = any(
                indicator in generated_sql.lower()
                for indicator in ["preagg", "materialized", "_cube_", "druid"]
            )

            if not is_materialized:
                return [
                    types.TextContent(
                        type="text",
                        text=format_error(
                            "⚠️  No materialized cube available for this query.\n\n"
                            "Visualization requires a materialized cube for performance.\n"
                            "Please ensure:\n"
                            "  • A cube exists for these metrics\n"
                            "  • The cube has been materialized\n"
                            "  • The query matches the cube's grain\n\n"
                            f"Attempted query:\n  Metrics: {', '.join(metrics)}\n"
                            f"  Dimensions: {', '.join(dimensions or ['none'])}",
                        ),
                    ),
                ]

            logger.info("✓ Materialized cube found, executing query...")

            # STEP 2: Now safe to execute - call /data/ endpoint
            data_params = {**params, "use_materialized": True, "async_": False}
            response = await http_client.get(
                f"{client.settings.dj_api_url.rstrip('/')}/data/",
                params=data_params,
                headers=client._get_headers(),
            )
            response.raise_for_status()
            result = response.json()

        # Extract data - DJ API returns results as list with one query result object
        results_list = result.get("results", [])

        if not results_list:
            return [
                types.TextContent(
                    type="text",
                    text="No results returned from query. Cannot generate visualization.",
                ),
            ]

        # Get the first (and typically only) query result
        query_result = results_list[0]

        # Extract rows and columns from the query result
        rows_data = query_result.get("rows", [])
        columns_info = query_result.get("columns", [])

        logger.info(f"Found {len(rows_data)} data rows")
        logger.info(f"Columns: {[col['name'] for col in columns_info]}")

        # Convert array rows to dictionaries
        if not rows_data:
            return [
                types.TextContent(
                    type="text",
                    text="No data rows returned from query. Cannot generate visualization.",
                ),
            ]

        # Convert rows (arrays) to dictionaries
        column_names = [col["name"] for col in columns_info]
        results_data = []
        for row in rows_data:
            row_dict = dict(zip(column_names, row))
            results_data.append(row_dict)

        logger.info(f"Converted {len(results_data)} rows to dict format")
        logger.info(f"First row: {results_data[0]}")

        # Prepare data for plotting
        # Try to find x-axis (first dimension or first column)
        if dimensions and len(dimensions) > 0:
            x_key = dimensions[0].split(".")[-1]  # Get just the dimension name
        else:
            # Use first non-metric column as x-axis
            metric_names = [m.split(".")[-1] for m in metrics]
            x_key = next(
                (k for k in results_data[0].keys() if k not in metric_names),
                list(results_data[0].keys())[0],
            )

        # Extract x and y values
        x_values = []
        for i, row in enumerate(results_data):
            x_val = row.get(x_key, i)
            # Handle different x-axis value types
            if x_val is None:
                x_val = i
            x_values.append(x_val)

        # Convert x_values to datetime objects, then to day offsets
        x_dates: List[Optional[datetime]] = []
        x_numeric: List[float] = []

        for i, x in enumerate(x_values):
            try:
                if isinstance(x, str) and len(x) == 8:
                    # Parse dateint format YYYYMMDD
                    date_obj = datetime.strptime(x, "%Y%m%d")
                    x_dates.append(date_obj)
                elif isinstance(x, (int, float)) and x > 19000000:
                    # Looks like a dateint as number
                    date_obj = datetime.strptime(str(int(x)), "%Y%m%d")
                    x_dates.append(date_obj)
                else:
                    # Not a date, use None as placeholder
                    x_dates.append(None)
            except (ValueError, TypeError):
                x_dates.append(None)
                logger.warning(f"Could not parse date: {x}")

        # Detect data type and handle accordingly
        x_labels: List[str] = []
        is_categorical = False

        # Convert dates to day offsets from first date
        if x_dates and any(d is not None for d in x_dates):
            first_date: datetime = next(d for d in x_dates if d is not None)  # type: ignore[assignment]
            for i, date_obj in enumerate(x_dates):  # type: ignore[assignment]
                if date_obj is not None:
                    days_offset = (date_obj - first_date).days
                    x_numeric.append(days_offset)
                    x_labels.append(date_obj.strftime("%Y-%m-%d"))
                else:
                    # Fallback to index if no date
                    x_numeric.append(float(i))
                    x_labels.append(str(x_values[i]))
        else:
            # No valid dates - check if numeric or categorical
            numeric_conversion_failed = False
            temp_numeric = []
            for i, x in enumerate(x_values):
                try:
                    temp_numeric.append(float(x))
                except (ValueError, TypeError):
                    # Not numeric - this is categorical data
                    numeric_conversion_failed = True
                    break

            if numeric_conversion_failed:
                # Categorical data - use indices and create labels
                is_categorical = True
                x_numeric = list(range(len(x_values)))
                # Truncate long category names to 15 chars
                x_labels = [
                    str(x)[:15] + ("..." if len(str(x)) > 15 else "") for x in x_values
                ]

                # Auto-switch to bar chart for categorical data
                if chart_type == "line":
                    chart_type = "bar"
                    logger.info(
                        "Auto-switched to bar chart for categorical x-axis data",
                    )
            else:
                # Numeric data
                x_numeric = temp_numeric
                x_labels = [str(x) for x in x_values]

        logger.info(f"Parsed {len([d for d in x_dates if d])} dates")
        logger.info(f"X numeric values (first 5): {x_numeric[:5]}")
        logger.info(f"X numeric values (last 5): {x_numeric[-5:]}")
        logger.info(f"X range: {min(x_numeric)} to {max(x_numeric)}")

        # Clear any previous plot
        plt.clear_figure()

        # Debug: log data ranges
        logger.info(f"X values range: {len(x_numeric)} points")
        logger.info(f"X numeric sample: {x_numeric[:5]}")

        # Plot each metric
        for metric in metrics:
            metric_name = metric.split(".")[-1]
            y_values = [row.get(metric_name) for row in results_data]

            # Convert to float, preserving None for gaps
            y_numeric: List[Optional[float]] = []
            has_valid_data = False
            for y in y_values:
                if y is None:
                    y_numeric.append(None)  # Keep as None to create gap in plot
                elif isinstance(y, (int, float)):
                    y_numeric.append(float(y))
                    has_valid_data = True
                else:
                    try:
                        y_numeric.append(float(y))
                        has_valid_data = True
                    except (ValueError, TypeError):
                        y_numeric.append(None)

            # Debug: log y values
            valid_y = [y for y in y_numeric if y is not None]
            logger.info(
                f"Metric {metric_name}: {len(y_numeric)} total, {len(valid_y)} valid values",
            )
            logger.info(f"Y numeric sample: {y_numeric[:5]}")
            logger.info(
                f"Y range: {min(valid_y) if valid_y else 'N/A'} to {max(valid_y) if valid_y else 'N/A'}",
            )

            # Only plot if we have valid data
            if not has_valid_data:
                logger.warning(f"No valid data for metric {metric_name}")
                continue

            if chart_type == "line":
                plt.plot(x_numeric, y_numeric, label=metric_name, marker="dot")
            elif chart_type == "bar":
                plt.bar(x_numeric, y_numeric, label=metric_name)
            elif chart_type == "scatter":  # pragma: no branch
                plt.scatter(x_numeric, y_numeric, label=metric_name, marker="dot")

        # Set title and labels
        if title:
            plt.title(title)
        else:
            plt.title(f"{', '.join([m.split('.')[-1] for m in metrics])}")

        plt.xlabel(x_key)
        plt.ylabel("Value")

        # Set y-axis minimum if specified
        if y_min is not None:
            plt.ylim(y_min, None)  # Set min, let max auto-scale

        # Set custom x-axis labels
        if is_categorical:
            # For categorical data, show all labels (already truncated)
            plt.xticks(x_numeric, x_labels)
        elif len(x_numeric) > 10 and x_dates and any(d is not None for d in x_dates):
            # For dates, show every Nth value to avoid crowding
            label_frequency = max(1, len(x_numeric) // 10)  # Show ~10 labels
            x_tick_positions = [
                x_numeric[i] for i in range(0, len(x_numeric), label_frequency)
            ]
            # Format dates nicely (MM/DD)
            x_tick_labels = []
            for i in range(0, len(x_dates), label_frequency):
                date_val = x_dates[i]
                if date_val is not None:
                    x_tick_labels.append(date_val.strftime("%m/%d"))
                else:
                    x_tick_labels.append(str(x_values[i]))
            plt.xticks(x_tick_positions, x_tick_labels)

        # Show legend if multiple metrics
        if len(metrics) > 1:
            plt.legend()

        # Set plot size for better readability
        plt.plot_size(100, 30)

        # Build the chart as a string
        chart_output = plt.build()

        # Clear the figure
        plt.clear_figure()

        # Create output - chart first, then compact metadata
        output_lines = [
            chart_output,
            "",
            f"📊 {', '.join([m.split('.')[-1] for m in metrics])} | {len(results_data)} points | {chart_type}",
        ]

        return [types.TextContent(type="text", text="\n".join(output_lines))]

    except ImportError:
        error_msg = (
            "plotext is not installed. Please install it with:\n"
            "pip install 'datajunction[mcp]' or pip install plotext"
        )
        logger.error(error_msg)
        return [types.TextContent(type="text", text=f"Error: {error_msg}")]

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        return [
            types.TextContent(
                type="text",
                text=format_error(
                    f"API request failed: {e.response.status_code} - {e.response.text}",
                    f"Visualizing metrics: {', '.join(metrics)}",
                ),
            ),
        ]
    except Exception as e:
        logger.error(f"Error generating visualization: {str(e)}", exc_info=True)
        return [
            types.TextContent(
                type="text",
                text=format_error(
                    str(e),
                    f"Generating visualization for metrics: {', '.join(metrics)}",
                ),
            ),
        ]
