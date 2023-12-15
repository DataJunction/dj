"""
DAG related functions.
"""
import itertools
from typing import Dict, List, Optional, Set, Union

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import aliased
from sqlmodel import Session, join, select

from datajunction_server.models import AttributeType, Column, ColumnAttribute
from datajunction_server.models.base import NodeColumns
from datajunction_server.models.node import (
    DimensionAttributeOutput,
    Node,
    NodeRelationship,
    NodeRevision,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import SEPARATOR, get_settings

settings = get_settings()


def get_dimensions_dag(
    session: Session,
    node_revision: NodeRevision,
    attributes: bool = True,
) -> List[Node]:
    """
    Gets the dimensions graph of the given node revision.
    Uses a recursive CTE query to build out all available dimensions.
    """

    initial_node = aliased(NodeRevision, name="initial_node")
    dimension_node = aliased(Node, name="dimension_node")
    dimension_rev = aliased(NodeRevision, name="dimension_rev")
    current_node = aliased(Node, name="current_node")
    current_rev = aliased(NodeRevision, name="current_rev")
    next_node = aliased(Node, name="next_node")
    next_rev = aliased(NodeRevision, name="next_rev")
    c = aliased(Column, name="c")

    # Recursive CTE
    dimensions_graph = (
        select(
            [
                initial_node.id.label("path_start"),
                c.name.label("col_name"),
                dimension_node.id.label("path_end"),
                (initial_node.name + "." + c.name + "," + dimension_node.name).label(
                    "join_path",
                ),
                dimension_node.name.label("node_name"),
                dimension_rev.id.label("node_revision_id"),
                dimension_rev.display_name.label("node_display_name"),
            ],
        )
        .select_from(initial_node)
        .join(NodeColumns, initial_node.id == NodeColumns.node_id)
        .join(c, NodeColumns.column_id == c.id)
        .join(dimension_node, c.dimension_id == dimension_node.id)
        .join(
            dimension_rev,
            and_(
                dimension_rev.version == dimension_node.current_version,
                dimension_rev.node_id == dimension_node.id,
            ),
        )
        .where(initial_node.id == node_revision.id)
    ).cte("dimensions_graph", recursive=True)

    ng = dimensions_graph.alias("ng")
    paths = dimensions_graph.union_all(
        select(
            [
                ng.c.path_start,
                c.name.label("col_name"),
                next_node.id.label("path_end"),
                (ng.c.join_path + "." + c.name + "," + next_node.name).label(
                    "join_path",
                ),
                next_node.name.label("node_name"),
                next_rev.id.label("node_revision_id"),
                next_rev.display_name.label("node_display_name"),
            ],
        ).select_from(
            ng.join(current_node, ng.c.path_end == current_node.id)
            .join(
                current_rev,
                and_(
                    current_rev.version == current_node.current_version,
                    current_rev.node_id == current_node.id,
                ),
            )
            .join(NodeColumns, current_rev.id == NodeColumns.node_id)
            .join(c, NodeColumns.column_id == c.id)
            .join(next_node, c.dimension_id == next_node.id)
            .join(
                next_rev,
                and_(
                    next_rev.version == next_node.current_version,
                    next_rev.node_id == next_node.id,
                ),
            ),
        ),
    )

    # Final SELECT statement
    final_query = (
        select(
            [
                paths.c.node_name,
                paths.c.node_revision_id,
                paths.c.node_display_name,
                c.name,
                c.type,
                AttributeType.name.label("column_attribute_type_name"),
                paths.c.join_path,
            ],
        )
        .select_from(paths)
        .join(NodeColumns, NodeColumns.node_id == paths.c.node_revision_id)
        .join(c, NodeColumns.column_id == c.id)
        .join(ColumnAttribute, c.id == ColumnAttribute.column_id)
        .join(AttributeType, ColumnAttribute.attribute_type_id == AttributeType.id)
    )

    # Execute the query
    result = session.exec(final_query).all()
    return result


def get_dimensions(
    session: Session,
    node: Node,
    attributes: bool = True,
) -> List[Union[DimensionAttributeOutput, Node]]:
    """
    Return all available dimensions for a given node.
    * Setting `attributes` to True will return a list of dimension attributes,
    * Setting `attributes` to False will return a list of dimension nodes
    """
    dimensions = get_dimensions_dag(session, node.current, attributes)
    processed: Set[Node] = set()

    if attributes:
        return sorted(
            [
                DimensionAttributeOutput(
                    name=f"{dim[0]}.{dim[3]}",
                    node_name=dim[0],
                    node_display_name=dim[2],
                    is_primary_key=dim[5] == "primary_key",
                    type=str(dim[4]),
                    path=dim[-1].split(","),
                )
                for dim in dimensions
            ],
            key=lambda x: x.name,
        )
    return sorted(list(processed), key=lambda x: x.name)


def check_convergence(path1: List[str], path2: List[str]) -> bool:
    """
    Determines whether two join paths converge before we reach the
    final element, the dimension attribute.
    """
    if path1 == path2:
        return True  # pragma: no cover
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


def group_dimensions_by_name(
    session: Session, node: Node,
) -> Dict[str, List[DimensionAttributeOutput]]:
    """
    Group the dimensions for the node by the dimension attribute name
    """
    return {
        k: list(v)
        for k, v in itertools.groupby(
            get_dimensions(session, node),
            key=lambda dim: dim.name,
        )
    }


def get_shared_dimensions(
    session: Session,
    metric_nodes: List[Node],
) -> List[DimensionAttributeOutput]:
    """
    Return a list of dimensions that are common between the nodes.
    """
    find_latest_node_revisions = [
        and_(
            NodeRevision.name == metric_node.name,
            NodeRevision.version == metric_node.current_version,
        )
        for metric_node in metric_nodes
    ]
    statement = (
        select(Node)
        .where(or_(*find_latest_node_revisions))
        .select_from(
            join(
                join(
                    NodeRevision,
                    NodeRelationship,
                ),
                Node,
                NodeRelationship.parent_id == Node.id,
            ),
        )
    )
    common_parents = set(session.exec(statement).all())

    parents = list(common_parents)
    common = group_dimensions_by_name(session, parents[0][0])
    for node in parents[1:]:
        node_dimensions = group_dimensions_by_name(session, node[0])

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
