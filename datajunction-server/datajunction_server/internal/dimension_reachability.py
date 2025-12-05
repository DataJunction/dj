"""
Functions for computing and maintaining node-to-dimension reachability.

This module pre-computes which nodes can reach which dimensions, enabling
O(1) lookups for queries like "find all nodes that can filter by dimension X".
"""

import logging
from collections import defaultdict, deque
from typing import Dict, Set, Tuple

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.column import Column
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.dimensionreachability import DimensionReachability
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.models.node_type import NodeType

logger = logging.getLogger(__name__)


async def rebuild_dimension_reachability(session: AsyncSession) -> int:
    """
    Rebuild the entire dimension_reachability table from scratch.

    Computes which nodes (of any type) can reach which dimensions.
    Uses BFS through the dimension graph to find transitive reachability.

    Returns the number of rows inserted.
    """
    logger.info("Rebuilding dimension reachability table...")
    table_name = DimensionReachability.__tablename__

    # Get all active nodes
    all_nodes_query = select(Node.id, Node.type).where(
        Node.deactivated_at.is_(None),
    )
    all_nodes = {
        row[0]: row[1] for row in (await session.execute(all_nodes_query)).all()
    }

    if not all_nodes:
        await session.execute(text(f"TRUNCATE TABLE {table_name}"))
        await session.commit()
        logger.info("No nodes found, cleared reachability table")
        return 0

    dimension_node_ids = {
        node_id
        for node_id, node_type in all_nodes.items()
        if node_type == NodeType.DIMENSION
    }

    # Build dimension-to-dimension adjacency list
    dim_edges = await _get_dimension_edges(session, dimension_node_ids)
    dim_adj = _build_adjacency_list(dim_edges)

    # Build node-to-dimension direct links (columns + dimension_links)
    node_to_dims = await _get_node_to_dimension_links(session)

    # For each node, find all dimensions it can reach
    total_inserted = 0
    for source_node_id in all_nodes:
        # Delete old rows for this source
        await session.execute(
            text(f"DELETE FROM {table_name} WHERE source_node_id = :source_id"),
            {"source_id": source_node_id},
        )

        # Get dimensions directly linked to this node
        direct_dims = node_to_dims.get(source_node_id, set())

        # BFS through dimension graph to find all reachable dimensions
        all_reachable_dims: Set[int] = set()
        for dim_id in direct_dims:
            if dim_id in dimension_node_ids:
                reachable = _bfs_reachable(dim_id, dim_adj)
                all_reachable_dims.update(reachable)

        # Insert new rows
        if all_reachable_dims:
            values_parts = [
                f"({source_node_id}, {dim_id})" for dim_id in all_reachable_dims
            ]
            values_sql = ", ".join(values_parts)
            await session.execute(
                text(
                    f"INSERT INTO {table_name} "
                    f"(source_node_id, target_dimension_node_id) "
                    f"VALUES {values_sql}",
                ),
            )
            total_inserted += len(all_reachable_dims)

    await session.commit()
    logger.info(
        "Rebuilt dimension reachability: %d nodes, %d dimensions, %d reachable pairs",
        len(all_nodes),
        len(dimension_node_ids),
        total_inserted,
    )
    return total_inserted


async def _get_node_to_dimension_links(
    session: AsyncSession,
) -> Dict[int, Set[int]]:
    """
    Get all direct node-to-dimension links (via columns and dimension_links).

    Returns a dict mapping node_id -> set of dimension_node_ids.
    """
    node_to_dims: Dict[int, Set[int]] = defaultdict(set)

    # From columns
    column_links_query = (
        select(Node.id, Column.dimension_id)
        .select_from(Column)
        .join(NodeRevision, Column.node_revision_id == NodeRevision.id)
        .join(
            Node,
            (NodeRevision.node_id == Node.id)
            & (Node.current_version == NodeRevision.version),
        )
        .where(
            Node.deactivated_at.is_(None),
            Column.dimension_id.isnot(None),
        )
    )
    for node_id, dim_id in await session.execute(column_links_query):
        node_to_dims[node_id].add(dim_id)

    # From dimension links
    dimlink_query = (
        select(Node.id, DimensionLink.dimension_id)
        .select_from(DimensionLink)
        .join(NodeRevision, DimensionLink.node_revision_id == NodeRevision.id)
        .join(
            Node,
            (NodeRevision.node_id == Node.id)
            & (Node.current_version == NodeRevision.version),
        )
        .where(Node.deactivated_at.is_(None))
    )
    for node_id, dim_id in await session.execute(dimlink_query):
        node_to_dims[node_id].add(dim_id)

    return node_to_dims


def _build_adjacency_list(edges: Set[Tuple[int, int]]) -> Dict[int, Set[int]]:
    """Build adjacency list from edge set. O(E) memory."""
    adj: Dict[int, Set[int]] = defaultdict(set)
    for source, target in edges:
        adj[source].add(target)
    return adj


def _bfs_reachable(source: int, adj: Dict[int, Set[int]]) -> Set[int]:
    """
    BFS to find all nodes reachable from source.
    Returns set including source itself (self-loop).

    Memory: O(V) for visited set
    Time: O(V + E) for this source
    """
    visited = {source}
    queue = deque([source])

    while queue:
        node = queue.popleft()
        for neighbor in adj.get(node, set()):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)

    return visited


async def _get_dimension_edges(
    session: AsyncSession,
    dimension_node_ids: Set[int],
) -> Set[Tuple[int, int]]:
    """
    Get all direct edges between dimension nodes (from columns and dimension links).

    An edge (A, B) means dimension node A has a column or link pointing to dimension node B.
    """
    edges: Set[Tuple[int, int]] = set()

    # From column references (column.dimension_id points to a dimension node)
    column_edges_query = (
        select(Node.id, Column.dimension_id)
        .select_from(Column)
        .join(NodeRevision, Column.node_revision_id == NodeRevision.id)
        .join(
            Node,
            (NodeRevision.node_id == Node.id)
            & (Node.current_version == NodeRevision.version),
        )
        .where(
            Node.type == NodeType.DIMENSION,
            Node.deactivated_at.is_(None),
            Column.dimension_id.isnot(None),
            Column.dimension_id.in_(dimension_node_ids),
        )
    )
    for source_node_id, target_node_id in await session.execute(column_edges_query):
        if source_node_id in dimension_node_ids:
            edges.add((source_node_id, target_node_id))

    # From dimension links on dimension nodes
    dimlink_edges_query = (
        select(Node.id, DimensionLink.dimension_id)
        .select_from(DimensionLink)
        .join(NodeRevision, DimensionLink.node_revision_id == NodeRevision.id)
        .join(
            Node,
            (NodeRevision.node_id == Node.id)
            & (Node.current_version == NodeRevision.version),
        )
        .where(
            Node.type == NodeType.DIMENSION,
            Node.deactivated_at.is_(None),
            DimensionLink.dimension_id.in_(dimension_node_ids),
        )
    )
    for source_node_id, target_node_id in await session.execute(dimlink_edges_query):
        if source_node_id in dimension_node_ids:
            edges.add((source_node_id, target_node_id))

    return edges


async def ensure_reachability_table_populated(session: AsyncSession) -> bool:
    """
    Ensure the reachability table is populated. If empty, rebuild it.

    Call this on application startup.

    Returns True if a rebuild was performed, False if table was already populated.
    """
    count_query = select(DimensionReachability.id).limit(1)
    result = await session.execute(count_query)

    if result.scalar() is None:
        logger.info(
            "Dimension reachability table is empty, performing initial build...",
        )
        await rebuild_dimension_reachability(session)
        return True

    return False


async def get_reachability_stats(session: AsyncSession) -> dict:
    """
    Get statistics about the reachability table for monitoring/debugging.
    """
    total_query = select(func.count(DimensionReachability.id))
    total = (await session.execute(total_query)).scalar() or 0

    nodes_query = select(
        func.count(func.distinct(DimensionReachability.source_node_id)),
    )
    nodes = (await session.execute(nodes_query)).scalar() or 0

    dimensions_query = select(
        func.count(func.distinct(DimensionReachability.target_dimension_node_id)),
    )
    dimensions = (await session.execute(dimensions_query)).scalar() or 0

    return {
        "total_reachability_pairs": total,
        "nodes_in_graph": nodes,
        "dimensions_in_graph": dimensions,
    }
