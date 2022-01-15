"""
DAG functions.
"""

from io import StringIO
from typing import Any, Dict, Set

import asciidag.graph
import asciidag.node


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
