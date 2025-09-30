"""
DataJunction MCP Server Implementation

This module provides a focused MCP server for DataJunction with essential tools
for working with nodes, metrics, cubes, and dimensions using GraphQL backend.
"""

import asyncio
import json
import logging
from typing import Any, Optional

# For now, let's use a simple MCP server implementation
# We'll add proper MCP SDK imports once we resolve the package issues
import httpx


logger = logging.getLogger(__name__)


class SimpleMCPServer:
    """DataJunction MCP Server with focused tools using GraphQL backend"""

    def __init__(self, dj_base_url: str = "http://localhost:8000"):
        self.dj_base_url = dj_base_url
        self.graphql_url = f"{dj_base_url}/graphql"

    async def list_nodes_in_namespace(
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

    async def get_node_details(self, name: str) -> str:
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

    async def get_metric_details(self, name: str) -> str:
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

    async def get_cube_details(self, name: str) -> str:
        """Get detailed information about a specific cube node including its metrics and dimensions"""
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

    async def list_dimensions_for_metrics(self, metric_names: list[str]) -> str:
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

    async def get_nodes_resource(self) -> str:
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

        result = await self._execute_graphql_query(query, {})
        return result

    async def get_catalogs_resource(self) -> str:
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

        result = await self._execute_graphql_query(query, {})
        return result

    async def _execute_graphql_query(
        self,
        query: str,
        variables: dict[str, Any],
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


class MCPServerRunner:
    """Simple MCP server that stays running and handles requests"""

    def __init__(self, dj_server: SimpleMCPServer):
        self.dj_server = dj_server
        self.running = True

    async def handle_request(self, method: str, params: dict):
        """Handle incoming MCP requests"""
        try:
            if method == "list_nodes_in_namespace":
                return await self.dj_server.list_nodes_in_namespace(**params)
            elif method == "get_node_details":
                return await self.dj_server.get_node_details(**params)
            elif method == "get_metric_details":
                return await self.dj_server.get_metric_details(**params)
            elif method == "get_cube_details":
                return await self.dj_server.get_cube_details(**params)
            elif method == "list_dimensions_for_metrics":
                return await self.dj_server.list_dimensions_for_metrics(**params)
            elif method == "get_catalogs":
                return await self.dj_server.get_catalogs_resource()
            elif method == "get_nodes":
                return await self.dj_server.get_nodes_resource()
            else:
                return json.dumps({"error": f"Unknown method: {method}"})
        except Exception as e:
            logger.error(f"Error handling request {method}: {e}")
            return json.dumps({"error": str(e)})

    async def run_interactive(self):
        """Run in interactive mode for testing"""
        print("\n🚀 DataJunction MCP Server Interactive Mode")
        print("Available commands:")
        print("  - catalogs: List all catalogs")
        print("  - nodes: List all nodes")
        print("  - nodes <namespace>: List nodes in namespace")
        print("  - node <name>: Get node details")
        print("  - metric <name>: Get metric details")
        print("  - cube <name>: Get cube details")
        print("  - dims <metric1,metric2,...>: Get common dimensions")
        print("  - quit: Exit")
        print()

        while self.running:
            try:
                user_input = input("dj-mcp> ").strip()
                if not user_input:
                    continue

                parts = user_input.split(" ", 1)
                command = parts[0].lower()
                args = parts[1] if len(parts) > 1 else ""

                if command == "quit":
                    break
                elif command == "catalogs":
                    result = await self.handle_request("get_catalogs", {})
                    print(result)
                elif command == "nodes" and not args:
                    result = await self.handle_request("get_nodes", {})
                    print(result)
                elif command == "nodes" and args:
                    result = await self.handle_request(
                        "list_nodes_in_namespace",
                        {"namespace": args},
                    )
                    print(result)
                elif command == "node" and args:
                    result = await self.handle_request(
                        "get_node_details",
                        {"name": args},
                    )
                    print(result)
                elif command == "metric" and args:
                    result = await self.handle_request(
                        "get_metric_details",
                        {"name": args},
                    )
                    print(result)
                elif command == "cube" and args:
                    result = await self.handle_request(
                        "get_cube_details",
                        {"name": args},
                    )
                    print(result)
                elif command == "dims" and args:
                    metric_names = [name.strip() for name in args.split(",")]
                    result = await self.handle_request(
                        "list_dimensions_for_metrics",
                        {"metric_names": metric_names},
                    )
                    print(result)
                else:
                    print("Unknown command. Type 'quit' to exit.")

            except KeyboardInterrupt:
                break
            except EOFError:
                break
            except Exception as e:
                print(f"Error: {e}")

        print("\n👋 MCP Server stopped")

    def stop(self):
        self.running = False


async def test_server():
    """Test the server functionality"""
    server = SimpleMCPServer()

    # Test basic GraphQL query
    result = await server.get_catalogs_resource()
    print("✅ Connection test successful!")
    print("Catalogs found:", json.loads(result)["listCatalogs"])
    return server


async def main():
    """Main entry point - run persistent MCP server"""
    import os

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Get DJ server URL from environment
    dj_url = os.getenv("DJ_BASE_URL", "http://localhost:8000")

    print(f"🔗 Connecting to DataJunction server at {dj_url}")
    print("📡 Starting MCP Server (simplified version for testing)")

    try:
        # Test connection first
        server = await test_server()

        # Run interactive server
        runner = MCPServerRunner(server)
        await runner.run_interactive()

    except Exception as e:
        logger.error(f"Failed to start MCP server: {e}")
        print(f"❌ Error: {e}")
        print("Make sure your DataJunction server is running on http://localhost:8000")


if __name__ == "__main__":
    asyncio.run(main())
