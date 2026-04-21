from fastapi import Request, BackgroundTasks

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.database.user import User
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.models.deployment import (
    NodeSpec,
    CubeSpec,
    DimensionSpec,
    MetricSpec,
    TransformSpec,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import SEPARATOR
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.ast import fast_parse_mode
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.errors import DJGraphCycleException
import logging

logger = logging.getLogger(__name__)


def extract_upstream_candidates(
    query_ast: ast.Query,
    is_metric: bool,
) -> set[str]:
    """Scan a parsed query for upstream node name candidates.

    Returns ast.Table references (excluding CTE names). For derived metrics
    (MetricSpec/MetricRevision with no FROM clause), also returns namespaced
    column identifiers plus their parent paths — these are speculative prefix
    candidates that callers must resolve against the DB to distinguish real
    parents from dim-attribute refs that aren't SQL parents.

    Shared between the deployment path (extract_node_graph) and the
    single-node path (validate_node_data) so both see identical candidates.
    """
    cte_names = {cte.alias_or_name.identifier() for cte in query_ast.ctes}
    tables = {
        t.name.identifier()
        for t in query_ast.find_all(ast.Table)
        if t.name.identifier() not in cte_names
    }

    if is_metric and not tables and query_ast.select.from_ is None:
        for col in query_ast.find_all(ast.Column):
            col_identifier = col.identifier()
            if SEPARATOR in col_identifier:
                tables.add(col_identifier)
                parent_path = col_identifier.rsplit(SEPARATOR, 1)[0]
                if SEPARATOR in parent_path:
                    tables.add(parent_path)

    return tables


def classify_parents(
    is_derived_metric: bool,
    dep_names: Iterable[str],
    dependency_nodes: dict[str, Node],
) -> tuple[list[Node], list[str]]:
    """Split a node's dependency names into resolved parents and missing names.

    For derived metrics the dep name set includes speculative namespace-prefix
    candidates for dim-attribute refs like `ns.dim.col`, which aren't real
    parents. Keep only METRIC-typed resolutions, and skip MissingParent
    emission since unresolved prefixes aren't genuine missing references.
    """
    resolved: list[Node] = []
    missing: list[str] = []
    for name in dep_names:
        node = dependency_nodes.get(name)
        if node is None:
            if not is_derived_metric:
                missing.append(name)
            continue
        if is_derived_metric and node.type != NodeType.METRIC:
            continue
        resolved.append(node)
    return resolved, missing


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
        candidates = extract_upstream_candidates(
            query_ast,
            is_metric=isinstance(node, MetricSpec),
        )
        return node.rendered_name, sorted(candidates), query_ast
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
