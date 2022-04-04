"""
DAG related functions.
"""

from io import StringIO
from typing import Any, Dict, Set

import asciidag.graph
import asciidag.node

from datajunction.models.database import Database
from datajunction.models.node import Node


def render_dag(dependencies: Dict[str, Set[str]], **kwargs: Any) -> str:
    """
    Render the DAG of dependencies.
    """
    out = StringIO()
    graph = asciidag.graph.Graph(out, **kwargs)

    asciidag_nodes: Dict[str, asciidag.node.Node] = {}
    tips = sorted(
        [build_asciidag(name, dependencies, asciidag_nodes) for name in dependencies],
        key=lambda n: n.item,
    )

    graph.show_nodes(tips)
    out.seek(0)
    return out.getvalue()


def build_asciidag(
    name: str,
    dependencies: Dict[str, Set[str]],
    asciidag_nodes: Dict[str, asciidag.node.Node],
) -> asciidag.node.Node:
    """
    Build the nodes for ``asciidag``.
    """
    if name in asciidag_nodes:
        asciidag_node = asciidag_nodes[name]
    else:
        asciidag_node = asciidag.node.Node(name)
        asciidag_nodes[name] = asciidag_node

    asciidag_node.parents = sorted(
        [
            build_asciidag(child, dependencies, asciidag_nodes)
            for child in dependencies[name]
        ],
        key=lambda n: n.item,
    )

    return asciidag_node


def get_computable_databases(node: Node) -> Set[Database]:
    """
    Return all the databases where a given node can be computed.

    TODO (betodealmeida): this should also take into consideration the node expression,
    since some of the columns might not be present in all databases.
    """
    # add all the databases where the node is explicitly materialized
    databases = {table.database for table in node.tables}

    # add all the databases that are common between the parents
    if node.parents:
        parent_databases = [get_computable_databases(parent) for parent in node.parents]
        databases |= set.intersection(*parent_databases)

    return databases
