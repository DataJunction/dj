from fastapi import Request, BackgroundTasks

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.database.user import User
from datajunction_server.models import access
from datajunction_server.models.deployment import (
    NodeSpec,
    CubeSpec,
    DimensionSpec,
    MetricSpec,
    TransformSpec,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.errors import DJGraphCycleException
import logging

logger = logging.getLogger(__name__)


def extract_node_graph(nodes: list[NodeSpec]) -> dict[str, list[str]]:
    """
    Extract the node graph from a list of nodes
    """
    logger.info("Extracting node graph for %d nodes", len(nodes))
    dependencies_map: dict[str, list[str]] = {}
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(_find_upstreams_for_node, node) for node in nodes]
        for future in as_completed(futures):
            name, deps = future.result()
            dependencies_map[name] = deps

    logger.info("Extracted node graph with %d entries", len(dependencies_map))
    return dependencies_map


def _find_upstreams_for_node(node: NodeSpec) -> tuple[str, list[str]]:
    """
    Find the upstream dependencies for a given node.
    """
    if (
        isinstance(node, (TransformSpec, DimensionSpec, MetricSpec))
        and node.rendered_query
    ):
        query_ast = parse(node.rendered_query)
        cte_names = [cte.alias_or_name.identifier() for cte in query_ast.ctes]
        tables = {
            t.name.identifier()
            for t in query_ast.find_all(ast.Table)
            if t.name.identifier() not in cte_names
        }
        return node.rendered_name, sorted(list(tables))
    if isinstance(node, CubeSpec):
        dimension_nodes = [dim.rsplit(".", 1)[0] for dim in node.rendered_dimensions]
        return node.rendered_name, node.rendered_metrics + dimension_nodes
    return node.rendered_name, []


def topological_levels(
    graph: dict[str, list[str]],
    ascending: bool = True,
) -> list[list[str]]:
    """
    Perform a topological sort on a directed acyclic graph (DAG) and
    return the nodes based on their levels.

    Args:
        graph (dict): A dictionary representing the DAG where keys are node names
                      and values are lists of upstream node names.

    Returns:
        list: A list of node names sorted in topological order.

    Raises:
        ValueError: If the graph contains a cycle.
    """
    # If there are any external dependencies, add them to the adjacency list
    for deps in list(graph.values()):
        for dep in deps:
            if dep not in graph:
                graph[dep] = []

    in_degree = defaultdict(int)
    for node in graph:
        in_degree[node] = 0
    for deps in graph.values():
        for dep in deps:
            in_degree[dep] += 1

    levels = []
    current = [n for n, d in in_degree.items() if d == 0]
    while current:
        levels.append(sorted(current))
        next_level = []
        for node in current:
            for dep in graph.get(node, []):
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    next_level.append(dep)
        current = next_level

    if sum(in_degree.values()) != 0:
        raise DJGraphCycleException("The graph contains a cycle!")

    return levels if ascending else levels[::-1]


@dataclass
class DeploymentContext:
    current_user: User
    request: Request
    query_service_client: QueryServiceClient
    validate_access: access.ValidateAccessFn
    background_tasks: BackgroundTasks
    cache: Cache
