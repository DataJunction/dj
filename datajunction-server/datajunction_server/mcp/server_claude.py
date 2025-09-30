"""
DataJunction MCP Server for Claude Desktop Integration

This module provides a proper MCP server using stdio communication
that Claude Desktop can connect to directly.
"""

import asyncio
import json
import logging
from typing import Any, Optional, Dict, List

import httpx
from mcp import stdio_server, types
from mcp.server import Server
from mcp.types import Tool, Resource

logger = logging.getLogger(__name__)


class DataJunctionMCPServer:
    """MCP Server for DataJunction with focused tools using GraphQL backend"""

    def __init__(self, dj_base_url: str = "http://localhost:8000"):
        self.dj_base_url = dj_base_url
        self.graphql_url = f"{dj_base_url}/graphql"
        self.server = Server("datajunction")
        self._setup_handlers()

    def _setup_handlers(self):
        """Setup MCP handlers for tools and resources"""

        # List available tools
        @self.server.list_tools()
        async def handle_list_tools() -> List[Tool]:
            """Return list of available tools"""
            return [
                Tool(
                    name="list_nodes_in_namespace",
                    description="List nodes in a specific namespace with optional filtering by node type",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "namespace": {
                                "type": "string",
                                "description": "The namespace to list nodes from",
                            },
                            "node_type": {
                                "type": "string",
                                "description": "Optional filter by node type (source, transform, metric, dimension, cube)",
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Maximum number of nodes to return",
                                "default": 100,
                            },
                        },
                        "required": ["namespace"],
                    },
                ),
                Tool(
                    name="get_node_details",
                    description="Get detailed information about a specific node by name",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "The full name of the node (e.g., 'namespace.node_name')",
                            },
                        },
                        "required": ["name"],
                    },
                ),
                Tool(
                    name="get_metric_details",
                    description="Get detailed information about a specific metric node including metadata and measures",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "The full name of the metric node",
                            },
                        },
                        "required": ["name"],
                    },
                ),
                Tool(
                    name="get_cube_details",
                    description="Get detailed information about a specific cube node including its metrics and dimensions",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "The full name of the cube node",
                            },
                        },
                        "required": ["name"],
                    },
                ),
                Tool(
                    name="list_dimensions_for_metrics",
                    description="Get common dimensions available for a set of metrics",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "metric_names": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of metric node names",
                            },
                        },
                        "required": ["metric_names"],
                    },
                ),
            ]

        # Handle tool calls
        @self.server.call_tool()
        async def handle_call_tool(
            name: str,
            arguments: Dict[str, Any],
        ) -> List[types.TextContent]:
            """Handle tool execution"""
            try:
                if name == "list_nodes_in_namespace":
                    result = await self._list_nodes_in_namespace(**arguments)
                elif name == "get_node_details":
                    result = await self._get_node_details(**arguments)
                elif name == "get_metric_details":
                    result = await self._get_metric_details(**arguments)
                elif name == "get_cube_details":
                    result = await self._get_cube_details(**arguments)
                elif name == "list_dimensions_for_metrics":
                    result = await self._list_dimensions_for_metrics(**arguments)
                else:
                    result = json.dumps({"error": f"Unknown tool: {name}"})

                return [types.TextContent(type="text", text=result)]

            except Exception as e:
                logger.error(f"Error executing tool {name}: {e}")
                error_result = json.dumps({"error": str(e)})
                return [types.TextContent(type="text", text=error_result)]

        # List available resources
        @self.server.list_resources()
        async def handle_list_resources() -> List[Resource]:
            """Return list of available resources"""
            return [
                Resource(
                    uri="dj://nodes",
                    name="DataJunction Nodes",
                    description="Overview of all nodes in DataJunction",
                    mimeType="application/json",
                ),
                Resource(
                    uri="dj://catalogs",
                    name="DataJunction Catalogs",
                    description="All available catalogs and their engines",
                    mimeType="application/json",
                ),
            ]

        # Handle resource reads
        @self.server.read_resource()
        async def handle_read_resource(uri: str) -> str:
            """Handle resource read requests"""
            if uri == "dj://nodes":
                return await self._get_nodes_resource()
            elif uri == "dj://catalogs":
                return await self._get_catalogs_resource()
            else:
                raise ValueError(f"Unknown resource: {uri}")

    # GraphQL query methods (same as before)
    async def _list_nodes_in_namespace(
        self,
        namespace: str,
        node_type: Optional[str] = None,
        limit: int = 100,
    ) -> str:
        """List nodes in a specific namespace with optional filtering by node type"""
        query = """
        query FindNodes($namespace: String!, $nodeTypes: [NodeType!], $limit: Int!) {
          findNodesPaginated(namespace: $namespace, nodeTypes: $nodeTypes, limit: $limit) {
            edges {
              node {
                name
                type
                current {
                  displayName
                  description
                  status
                  mode
                  updatedAt
                }
                createdAt
                tags {
                  name
                  tagType
                }
              }
            }
            pageInfo {
              hasNextPage
              hasPrevPage
            }
          }
        }
        """

        variables = {"namespace": namespace, "limit": limit}

        if node_type:
            variables["nodeTypes"] = [node_type.upper()]

        return await self._execute_graphql_query(query, variables)

    async def _get_node_details(self, name: str) -> str:
        """Get detailed information about a specific node by name"""
        query = """
        query FindNodes($names: [String!]!) {
          findNodes(names: $names, limit: 1) {
            name
            type
            currentVersion
            createdAt
            current {
              displayName
              description
              status
              mode
              updatedAt
              query
              schema_
              table
              catalog {
                name
              }
              columns {
                name
                displayName
                type
                dimension {
                  name
                }
              }
              parents {
                name
                currentVersion
              }
              dimensionLinks {
                dimension {
                  name
                }
                joinType
                joinCardinality
                role
              }
              availability {
                catalog
                schema_
                table
              }
            }
            tags {
              name
              tagType
              description
            }
          }
        }
        """

        variables = {"names": [name]}
        return await self._execute_graphql_query(query, variables)

    async def _get_metric_details(self, name: str) -> str:
        """Get detailed information about a specific metric node"""
        query = """
        query FindNodes($names: [String!]!) {
          findNodes(names: $names, limit: 1, nodeTypes: [METRIC]) {
            name
            type
            currentVersion
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
                  abbreviation
                }
                expression
                significantDigits
                minDecimalExponent
                maxDecimalExponent
              }
              requiredDimensions {
                name
                type
                dimension {
                  name
                }
              }
              extractedMeasures {
                derivedQuery
                derivedExpression
                components {
                  node
                  alias
                  field_name
                }
              }
              parents {
                name
                currentVersion
              }
            }
            tags {
              name
              tagType
            }
          }
        }
        """

        variables = {"names": [name]}
        return await self._execute_graphql_query(query, variables)

    async def _get_cube_details(self, name: str) -> str:
        """Get detailed information about a specific cube node"""
        query = """
        query FindNodes($names: [String!]!) {
          findNodes(names: $names, limit: 1, nodeTypes: [CUBE]) {
            name
            type
            currentVersion
            current {
              displayName
              description
              status
              mode
              cubeMetrics {
                name
                type
                displayName
                description
                metricMetadata {
                  direction
                  unit {
                    name
                    abbreviation
                  }
                  expression
                }
              }
              cubeDimensions {
                name
                attribute
                role
                type
                properties
                dimensionNode {
                  name
                  displayName
                  description
                }
              }
            }
            tags {
              name
              tagType
            }
          }
        }
        """

        variables = {"names": [name]}
        return await self._execute_graphql_query(query, variables)

    async def _list_dimensions_for_metrics(self, metric_names: List[str]) -> str:
        """Get common dimensions available for a set of metrics"""
        query = """
        query CommonDimensions($nodes: [String!]!) {
          commonDimensions(nodes: $nodes) {
            name
            attribute
            role
            type
            properties
            dimensionNode {
              name
              displayName
              description
              current {
                columns {
                  name
                  type
                  displayName
                }
              }
            }
          }
        }
        """

        variables = {"nodes": metric_names}
        return await self._execute_graphql_query(query, variables)

    async def _get_nodes_resource(self) -> str:
        """Resource providing all nodes overview"""
        query = """
        query {
          findNodes(limit: 1000) {
            name
            type
            current {
              displayName
              description
              status
            }
            createdAt
          }
        }
        """

        return await self._execute_graphql_query(query, {})

    async def _get_catalogs_resource(self) -> str:
        """Resource providing all catalogs"""
        query = """
        query {
          listCatalogs {
            name
            engines {
              name
              version
              dialect
            }
          }
        }
        """

        return await self._execute_graphql_query(query, {})

    async def _execute_graphql_query(
        self,
        query: str,
        variables: Dict[str, Any],
    ) -> str:
        """Execute a GraphQL query against the DJ server"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.graphql_url,
                    json={"query": query, "variables": variables},
                    headers={"Content-Type": "application/json"},
                    timeout=30.0,
                )
                response.raise_for_status()
                result = response.json()

                if "errors" in result:
                    logger.error(f"GraphQL errors: {result['errors']}")
                    return json.dumps(
                        {"error": "GraphQL query failed", "details": result["errors"]},
                    )

                return json.dumps(result["data"], indent=2)

        except httpx.RequestError as e:
            logger.error(f"Request error: {e}")
            return json.dumps(
                {
                    "error": "Failed to connect to DataJunction server",
                    "details": str(e),
                },
            )
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return json.dumps({"error": "Unexpected error occurred", "details": str(e)})


async def main():
    """Main entry point for Claude Desktop integration"""
    import os

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Get DJ server URL from environment
    dj_url = os.getenv("DJ_BASE_URL", "http://localhost:8000")

    # Create the MCP server
    dj_server = DataJunctionMCPServer(dj_url)

    # Run with stdio for Claude Desktop communication
    async with stdio_server() as (read_stream, write_stream):
        await dj_server.server.run(
            read_stream,
            write_stream,
            dj_server.server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
