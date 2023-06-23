"""
DAG related functions.
"""
import collections
import itertools
from typing import Deque, List, Set, Tuple

from datajunction_server.models import Column
from datajunction_server.models.node import DimensionAttributeOutput, Node, NodeType
from datajunction_server.utils import get_settings

settings = get_settings()


def get_dimensions(node: Node) -> List[DimensionAttributeOutput]:
    """
    Return all available dimensions for a given node.
    """
    dimensions = []

    # Start with the node itself or the node's immediate parent if it's a metric node
    StateTrackingType = Tuple[Node, List[Column]]
    node_starting_state: StateTrackingType = (node, [])
    immediate_parent_starting_state: List[StateTrackingType] = [
        (parent, [])
        for parent in (node.current.parents if node.type == NodeType.METRIC else [])
    ]
    to_process: Deque[StateTrackingType] = collections.deque(
        [node_starting_state, *immediate_parent_starting_state],
    )
    processed: Set[Node] = set()

    while to_process:
        current_node, join_path = to_process.popleft()

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
                join_path_str = [
                    (
                        (
                            link_column.node_revisions[0].name + "."
                            if link_column.node_revisions
                            else ""
                        )
                        + link_column.name
                    )
                    for link_column in join_path
                    if link_column is not None and link_column.dimension
                ]
                dimensions.append(
                    DimensionAttributeOutput(
                        name=f"{current_node.name}.{column.name}",
                        type=column.type,
                        path=join_path_str,
                    ),
                )
            if column.dimension and column.dimension not in processed:
                to_process.append((column.dimension, join_path + [column]))
    return sorted(dimensions, key=lambda x: x.name)


def get_shared_dimensions(
    metric_nodes: List[Node],
) -> List[DimensionAttributeOutput]:
    """
    Return a list of dimensions that are common between the nodes.
    """
    common = {
        k: list(v)
        for k, v in itertools.groupby(
            get_dimensions(metric_nodes[0]),
            key=lambda dim: dim.name,
        )
    }
    for node in set(metric_nodes[1:]):
        node_dimensions = {
            k: list(v)
            for k, v in itertools.groupby(
                get_dimensions(node),
                key=lambda dim: dim.name,
            )
        }
        common_dim_keys = common.keys() & list(node_dimensions.keys())
        common = {dim: common[dim] + node_dimensions[dim] for dim in common_dim_keys}
    return sorted(
        [y for x in common.values() for y in x],
        key=lambda x: (x.name, x.path),
    )
