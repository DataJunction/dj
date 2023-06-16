"""
DAG related functions.
"""
import collections
from typing import List

from datajunction_server.models.node import DimensionAttributeOutput, Node, NodeType
from datajunction_server.utils import get_settings

settings = get_settings()


def get_dimensions(node: Node) -> List[DimensionAttributeOutput]:
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

        # Don't include attributes from deactivated dimensions
        if current_node.deactivated_at:
            continue
        processed.add(current_node)

        for column in current_node.current.columns:
            # Include the dimension if it's a column belonging to a dimension node
            # or if it's tagged with the dimension column attribute
            if (
                current_node.type == NodeType.DIMENSION
                or any(
                    attr.attribute_type.name == "dimension"
                    for attr in column.attributes
                )
                or column.dimension
            ):
                dimensions.append(
                    DimensionAttributeOutput(
                        name=f"{current_node.name}.{column.name}",
                        type=column.type,
                    ),
                )
            if column.dimension and column.dimension not in processed:
                to_process.append(column.dimension)
    return sorted(dimensions, key=lambda x: x.name)


def get_shared_dimensions(
    metric_nodes: List[Node],
) -> List[DimensionAttributeOutput]:
    """
    Return a list of dimensions that are common between the nodes.
    """
    common = {dim.name: dim for dim in get_dimensions(metric_nodes[0])}
    for node in set(metric_nodes[1:]):
        node_dimensions = {dim.name: dim for dim in get_dimensions(node)}
        common_dim_keys = common.keys() & node_dimensions.keys()
        common = {dim: node_dimensions[dim] for dim in common_dim_keys}
    return sorted(common.values(), key=lambda x: x.name)
