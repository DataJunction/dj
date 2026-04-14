from fastapi import Request, BackgroundTasks

from collections import defaultdict
from dataclasses import dataclass
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.database.user import User
from datajunction_server.database.node import NodeRevision
from datajunction_server.models.deployment import (
    NodeSpec,
    CubeSpec,
    DimensionSpec,
    MetricSpec,
    TransformSpec,
)
from datajunction_server.utils import SEPARATOR
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.ast import fast_parse_mode
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.errors import DJGraphCycleException
import logging

logger = logging.getLogger(__name__)


def extract_node_graph(nodes: list[NodeSpec]) -> dict[str, list[str]]:
    """
    Extract the node graph from a list of nodes.

    Parsed ASTs are cached on each spec for reuse by downstream validation.

    Returns:
        dependencies_map: node name → list of upstream dependency names.
    """
    logger.info("Extracting node graph for %d nodes", len(nodes))
    dependencies_map: dict[str, list[str]] = {}
    for node in nodes:
        with fast_parse_mode():
            name, deps, parsed = _find_upstreams_for_node(node)
        dependencies_map[name] = deps
        if parsed is not None:
            node._query_ast = parsed

    logger.info("Extracted node graph with %d entries", len(dependencies_map))
    return dependencies_map


def _find_upstreams_for_node(node: NodeSpec) -> tuple[str, list[str], ast.Query | None]:
    """
    Find the upstream dependencies for a given node.
    Returns (name, deps, parsed_ast_or_none).
    """
    if (
        isinstance(node, (TransformSpec, DimensionSpec, MetricSpec))
        and node.rendered_query
    ):
        # For metrics, parse the aliased version so the AST can be reused
        # by validation (which expects the metric-aliased form).
        query_str = node.rendered_query
        if isinstance(node, MetricSpec):
            query_str = NodeRevision.format_metric_alias(
                query_str,
                node.rendered_name,
            )
        query_ast = parse(query_str)
        cte_names = [cte.alias_or_name.identifier() for cte in query_ast.ctes]
        tables = {
            t.name.identifier()
            for t in query_ast.find_all(ast.Table)
            if t.name.identifier() not in cte_names
        }

        # For derived metrics (no FROM clause), look for metric references
        # in Column nodes. E.g., SELECT default.metric_a / default.metric_b
        if (
            isinstance(node, MetricSpec)
            and not tables
            and query_ast.select.from_ is None
        ):
            for col in query_ast.find_all(ast.Column):
                col_identifier = col.identifier()
                if SEPARATOR in col_identifier:  # pragma: no branch
                    # Add full identifier (might be a metric node)
                    tables.add(col_identifier)
                    # Also add parent path (might be dimension.column)
                    parent_path = col_identifier.rsplit(SEPARATOR, 1)[0]
                    if SEPARATOR in parent_path:  # Only if there's still a namespace
                        tables.add(parent_path)

        return node.rendered_name, sorted(list(tables)), query_ast
    if isinstance(node, CubeSpec):
        dimension_nodes = [dim.rsplit(".", 1)[0] for dim in node.rendered_dimensions]
        return node.rendered_name, node.rendered_metrics + dimension_nodes, None
    return node.rendered_name, [], None


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
    request: Request | None = None
    query_service_client: QueryServiceClient | None = None
    background_tasks: BackgroundTasks | None = None
    cache: Cache | None = None
