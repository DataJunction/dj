"""
DAG related functions.
"""
import collections
import itertools
from typing import Deque, Dict, List, Optional, Set, Tuple, Union

from sqlmodel import Session, select

from datajunction_server.models import Column
from datajunction_server.models.base import NodeColumns
from datajunction_server.models.node import (
    DimensionAttributeOutput,
    Node,
    NodeRevision,
    NodeType,
)
from datajunction_server.utils import SEPARATOR, get_settings

settings = get_settings()


def get_dimensions(
    node: Node,
    attributes: bool = True,
) -> List[Union[DimensionAttributeOutput, Node]]:
    """
    Return all available dimensions for a given node.
    * Setting `attributes` to True will return a list of dimension attributes,
    * Setting `attributes` to False will return a list of dimension nodes
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
            # or if it's tagged with the dimension column attribute (but not
            # additionally linked to a dimension)
            if current_node.type == NodeType.DIMENSION or (
                column.is_dimensional() and not column.dimension_id
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
    if attributes:
        return sorted(dimensions, key=lambda x: x.name)
    return sorted(list(processed), key=lambda x: x.name)


def check_convergence(path1: List[str], path2: List[str]) -> bool:
    """
    Determines whether two join paths converge before we reach the
    final element, the dimension attribute.
    """
    if path1 == path2:
        return True
    len1 = len(path1)
    len2 = len(path2)
    min_len = min(len1, len2)

    for i in range(min_len):
        partial1 = path1[len1 - i - 1 :]
        partial2 = path2[len2 - i - 1 :]
        if partial1 == partial2:
            return True

    # TODO: Once we introduce dimension roles, we can remove this.  # pylint: disable=fixme
    # To workaround this for now, we're using column names as the effective role of the dimension
    if (
        path1
        and path2
        and path1[-1].split(SEPARATOR)[-1] == path2[-1].split(SEPARATOR)[-1]
    ):
        return True
    return False  # pragma: no cover


def group_dimensions_by_name(node: Node) -> Dict[str, List[DimensionAttributeOutput]]:
    """
    Group the dimensions for the node by the dimension attribute name
    """
    return {
        k: list(v)
        for k, v in itertools.groupby(
            get_dimensions(node),
            key=lambda dim: dim.name,
        )
    }


def get_shared_dimensions(
    metric_nodes: List[Node],
) -> List[DimensionAttributeOutput]:
    """
    Return a list of dimensions that are common between the nodes.
    """
    common = group_dimensions_by_name(metric_nodes[0])
    for node in set(metric_nodes[1:]):
        node_dimensions = group_dimensions_by_name(node)

        # Merge each set of dimensions based on the name and path
        to_delete = set(common.keys() - node_dimensions.keys())
        common_dim_keys = common.keys() & list(node_dimensions.keys())
        if not common_dim_keys:
            return []
        for common_dim in common_dim_keys:
            for existing_attr in common[common_dim]:
                for new_attr in node_dimensions[common_dim]:
                    converged = check_convergence(existing_attr.path, new_attr.path)
                    if not converged:
                        to_delete.add(common_dim)  # pragma: no cover

        for dim_key in to_delete:
            del common[dim_key]  # pragma: no cover

    return sorted(
        [y for x in common.values() for y in x],
        key=lambda x: (x.name, x.path),
    )


def get_nodes_with_dimension(
    session: Session,
    dimension_node: Node,
    node_types: Optional[List[NodeType]] = None,
) -> List[NodeRevision]:
    """
    Find all nodes that can be joined to a given dimension
    """
    to_process = [dimension_node]
    processed: Set[str] = set()
    final_set: Set[NodeRevision] = set()
    while to_process:
        current_node = to_process.pop()
        processed.add(current_node.name)

        # Dimension nodes are used to expand the searchable graph by finding
        # the next layer of nodes that are linked to this dimension
        if current_node.type == NodeType.DIMENSION:
            statement = (
                select(NodeRevision)
                .join(
                    Node,
                    onclause=(
                        (NodeRevision.node_id == Node.id)
                        & (Node.current_version == NodeRevision.version)
                    ),  # pylint: disable=superfluous-parens
                )
                .join(
                    NodeColumns,
                    onclause=(
                        NodeRevision.id == NodeColumns.node_id
                    ),  # pylint: disable=superfluous-parens
                )
                .join(
                    Column,
                    onclause=(
                        NodeColumns.column_id == Column.id
                    ),  # pylint: disable=superfluous-parens
                )
                .where(
                    Column.dimension_id.in_(  # type: ignore  # pylint: disable=no-member
                        [current_node.id],
                    ),
                )
            )
            node_revisions = session.exec(statement).unique().all()
            for node_rev in node_revisions:
                to_process.append(node_rev.node)
        else:
            # All other nodes are added to the result set
            final_set.add(current_node.current)
            for child in current_node.children:
                if child.name not in processed:
                    to_process.append(child.node)
    if node_types:
        return [node for node in final_set if node.type in node_types]
    return list(final_set)


def get_nodes_with_common_dimensions(
    session: Session,
    common_dimensions: List[Node],
    node_types: Optional[List[NodeType]] = None,
) -> List[NodeRevision]:
    """
    Find all nodes that share a list of common dimensions
    """
    nodes_that_share_dimensions = set()
    first = True
    for dimension in common_dimensions:
        new_nodes = get_nodes_with_dimension(session, dimension, node_types)
        if first:
            nodes_that_share_dimensions = set(new_nodes)
            first = False
        else:
            nodes_that_share_dimensions = nodes_that_share_dimensions.intersection(
                set(new_nodes),
            )
            if not nodes_that_share_dimensions:
                break
    return list(nodes_that_share_dimensions)
