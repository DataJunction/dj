"""
DataJunction MCP Server

Main MCP server implementation using the official MCP Python SDK.
Exposes DJ semantic layer to AI agents.
"""

import logging

import mcp.types as types
from mcp.server import Server

from datajunction_server.mcp import tools

logger = logging.getLogger(__name__)

# Create MCP server instance
app = Server("datajunction")


@app.list_tools()
async def list_tools() -> list[types.Tool]:
    """
    List available MCP tools for interacting with DataJunction

    Returns:
        List of tool definitions
    """
    return [
        types.Tool(
            name="list_namespaces",
            description=(
                "List all available namespaces in the DataJunction instance. "
                "Namespaces are the primary way nodes are organized (e.g., 'demo.metrics', 'common.dimensions'). "
                "Use this to discover what organizational structures exist before searching for specific nodes."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        types.Tool(
            name="search_nodes",
            description=(
                "Search for DataJunction nodes (metrics, dimensions, cubes, sources, transforms) "
                "by name fragment or other properties. Returns a list of matching nodes with "
                "their basic information including status, tags, and owners. "
                "TIP: Use the 'namespace' parameter to narrow searches - namespaces are the primary "
                "organizational structure in DJ (e.g., 'demo.metrics', 'common.dimensions')."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search term - fragment of node name to search for (e.g., 'revenue', 'user')",
                    },
                    "node_type": {
                        "type": "string",
                        "enum": ["metric", "dimension", "cube", "source", "transform"],
                        "description": "Optional: Filter results to specific node type",
                    },
                    "namespace": {
                        "type": "string",
                        "description": (
                            "Optional: Filter results to specific namespace (e.g., 'demo.metrics', 'common.dimensions'). "
                            "HIGHLY RECOMMENDED - namespaces are the primary way to organize nodes in DJ. "
                            "Use this to narrow search results to a specific domain or area."
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 1000,
                        "description": "Maximum number of results to return (default: 100, max: 1000)",
                    },
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="get_node_details",
            description=(
                "Get comprehensive details about a specific DataJunction node. "
                "Returns full information including description, SQL definition, available dimensions, "
                "columns, tags, ownership, and lineage information."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Full node name including namespace (e.g., 'finance.daily_revenue', 'core.users')",
                    },
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="get_common_dimensions",
            description=(
                "Find dimensions that are available across multiple metrics. "
                "Use this to determine which dimensions you can use when querying multiple metrics together. "
                "Returns the list of common dimensions that work across all specified metrics."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "metric_names": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of metric node names to analyze (e.g., ['finance.revenue', 'growth.users'])",
                    },
                },
                "required": ["metric_names"],
            },
        ),
        types.Tool(
            name="build_metric_sql",
            description=(
                "Generate executable SQL for querying metrics with specified dimensions and filters using v3 SQL builder. "
                "Returns the SQL query, output columns, dialect, and cube name. "
                "Use this to get the actual SQL needed to query DataJunction metrics."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of metric node names to query (e.g., ['finance.daily_revenue'])",
                    },
                    "dimensions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional: List of dimensions to group by (e.g., ['core.date', 'core.region'])",
                    },
                    "filters": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional: SQL filter conditions (e.g., ['date >= \\'2024-01-01\\'', 'region = \\'US\\''])",
                    },
                    "orderby": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional: Columns to order by (e.g., ['date DESC', 'revenue DESC'])",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Optional: Maximum number of rows to return",
                    },
                    "dialect": {
                        "type": "string",
                        "description": "Optional: Target SQL dialect (e.g., 'spark', 'trino', 'postgres')",
                    },
                    "use_materialized": {
                        "type": "boolean",
                        "default": True,
                        "description": "Optional: Whether to use materialized tables when available (default: true)",
                    },
                },
                "required": ["metrics"],
            },
        ),
        types.Tool(
            name="get_metric_data",
            description=(
                "Execute a query and get actual data for metrics with specified dimensions and filters. "
                "Returns query results with data rows. Recommend setting a limit to avoid large result sets. "
                "Use this when you want to see actual data values, not just the SQL."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of metric node names to query (e.g., ['finance.daily_revenue'])",
                    },
                    "dimensions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional: List of dimensions to group by (e.g., ['core.date', 'core.region'])",
                    },
                    "filters": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional: SQL filter conditions (e.g., ['date >= \\'2024-01-01\\'', 'region = \\'US\\''])",
                    },
                    "orderby": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional: Columns to order by (e.g., ['date DESC', 'revenue DESC'])",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "RECOMMENDED: Maximum number of rows to return (default: unlimited, use with caution)",
                    },
                    "use_materialized": {
                        "type": "boolean",
                        "default": True,
                        "description": "Optional: Whether to use materialized tables when available (default: true)",
                    },
                },
                "required": ["metrics"],
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """
    Handle tool execution requests from AI agents

    Args:
        name: Tool name to execute
        arguments: Tool arguments

    Returns:
        Tool execution results as text content
    """
    logger.info(f"Executing tool: {name} with args: {arguments}")

    try:
        if name == "list_namespaces":
            result = await tools.list_namespaces()

        elif name == "search_nodes":
            result = await tools.search_nodes(
                query=arguments["query"],
                node_type=arguments.get("node_type"),
                namespace=arguments.get("namespace"),
                limit=arguments.get("limit", 100),
            )

        elif name == "get_node_details":
            result = await tools.get_node_details(
                name=arguments["name"],
            )

        elif name == "get_common_dimensions":
            result = await tools.get_common_dimensions(
                metric_names=arguments["metric_names"],
            )

        elif name == "build_metric_sql":
            result = await tools.build_metric_sql(
                metrics=arguments["metrics"],
                dimensions=arguments.get("dimensions"),
                filters=arguments.get("filters"),
                orderby=arguments.get("orderby"),
                limit=arguments.get("limit"),
                dialect=arguments.get("dialect"),
                use_materialized=arguments.get("use_materialized", True),
            )

        elif name == "get_metric_data":
            result = await tools.get_metric_data(
                metrics=arguments["metrics"],
                dimensions=arguments.get("dimensions"),
                filters=arguments.get("filters"),
                orderby=arguments.get("orderby"),
                limit=arguments.get("limit"),
                use_materialized=arguments.get("use_materialized", True),
            )

        else:
            result = f"Unknown tool: {name}"

        return [types.TextContent(type="text", text=result)]

    except Exception as e:
        logger.error(f"Error executing tool {name}: {str(e)}", exc_info=True)
        error_msg = f"Error executing {name}: {str(e)}"
        return [types.TextContent(type="text", text=error_msg)]


@app.list_resources()
async def list_resources() -> list[types.Resource]:
    """
    List available resources

    Currently not implemented - could be added later to expose:
    - dj://catalog - Browse all nodes by namespace
    - dj://metrics - List all metrics
    - dj://cubes - List all cubes

    Returns:
        List of available resources
    """
    return []


@app.read_resource()
async def read_resource(uri: str) -> str:
    """
    Read a specific resource

    Args:
        uri: Resource URI to read

    Returns:
        Resource content
    """
    # Not implemented yet - could add catalog browsing here
    return f"Resource not found: {uri}"
