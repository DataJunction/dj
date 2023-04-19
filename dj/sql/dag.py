"""
DAG related functions.
"""
import collections
from typing import List, Set

from dj.models.node import Node, NodeType
from dj.utils import get_settings

settings = get_settings()


def get_dimensions(node: Node) -> List[str]:
    """
    Return all available dimensions for a given node.
    """
    dimensions = []

    # Start with the node itself or the node's immediate parent if it's a metric node
    to_process = collections.deque(
        [node, *(node.current.parents if node.type == NodeType.METRIC else [])],
    )
    processed = set()

    while to_process:
        current_node = to_process.popleft()
        processed.add(current_node)

        for column in current_node.current.columns:
            # Include the dimension if it's a column belonging to a dimension node
            # or if it's tagged with the dimension column attribute
            if current_node.type == NodeType.DIMENSION or any(
                attr.attribute_type.name == "dimension" for attr in column.attributes
            ):
                dimensions.append(f"{current_node.name}.{column.name}")
            if column.dimension and column.dimension not in processed:
                to_process.append(column.dimension)
    return sorted(dimensions)


def get_shared_dimensions(metric_nodes: List[Node]) -> Set[str]:
    """
    Return a list of dimensions that are common between the nodes.
    """
    common = set(get_dimensions(metric_nodes[0]))
    for node in set(metric_nodes[1:]):
        common.intersection_update(get_dimensions(node))
    return common
