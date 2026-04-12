"""
Downstream impact propagation for deployments.
"""

import logging
import time

from sqlalchemy import select
from sqlalchemy.sql.operators import is_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from datajunction_server.database.node import Node, NodeRevision, NodeRelationship
from datajunction_server.instrumentation.provider import get_metrics_provider
from datajunction_server.models.impact import DownstreamImpact, ImpactType
from datajunction_server.models.node import NodeStatus

logger = logging.getLogger(__name__)


async def propagate_impact(
    session: AsyncSession,
    namespace: str,
    changed_node_names: set[str],
    deleted_node_names: frozenset[str] = frozenset(),
) -> list[DownstreamImpact]:
    """BFS downstream impact analysis with INVALID propagation.

    Must be called inside the caller's active transaction (inside a SAVEPOINT for
    dry-runs).  For dry-runs the caller rolls back the SAVEPOINT, undoing both the
    node changes and any INVALID markers written here.  For wet-runs the caller
    commits, persisting everything.

    Args:
        session: Active async session (inside a SAVEPOINT for dry-runs).
        namespace: Deployment namespace used to flag external impacts.
        changed_node_names: Names of nodes that were created or updated.
        deleted_node_names: Names of nodes about to be deleted (still in DB at
            call time — caller must invoke this before hard_delete_node).

    Returns:
        List of DownstreamImpact describing each affected downstream node.
    """
    start = time.perf_counter()
    all_root_names = changed_node_names | deleted_node_names
    if not all_root_names:
        return []

    changed_nodes = (
        await Node.get_by_names(
            session,
            list(changed_node_names),
            options=[joinedload(Node.current)],
        )
        if changed_node_names
        else []
    )
    deleted_nodes = (
        await Node.get_by_names(
            session,
            list(deleted_node_names),
            options=[joinedload(Node.current)],
        )
        if deleted_node_names
        else []
    )

    changed_by_id: dict[int, Node] = {n.id: n for n in changed_nodes}
    deleted_by_id: dict[int, Node] = {n.id: n for n in deleted_nodes}
    all_root_ids: set[int] = set(changed_by_id) | set(deleted_by_id)

    # IDs whose invalidity propagates downstream:
    # - Changed nodes that are now INVALID after deployment
    # - All deleted nodes (their children lose a required parent)
    invalidating_ids: set[int] = {
        nid
        for nid, n in changed_by_id.items()
        if n.current and n.current.status == NodeStatus.INVALID
    } | set(deleted_by_id)

    # Causality tracking: node_id → set of root node IDs responsible
    cause_map: dict[int, set[int]] = {nid: {nid} for nid in all_root_ids}
    root_id_to_name: dict[int, str] = {
        **{n.id: n.name for n in changed_nodes},
        **{n.id: n.name for n in deleted_nodes},
    }

    frontier_ids: set[int] = set(all_root_ids)
    visited_node_ids: set[int] = set(frontier_ids)
    results: list[DownstreamImpact] = []
    depth = 1

    while frontier_ids:
        # Each row: (child_node_id, parent_node_id) for all frontier parents
        rows = (
            await session.execute(
                select(NodeRevision.node_id, NodeRelationship.parent_id)
                .join(NodeRelationship, NodeRelationship.child_id == NodeRevision.id)
                .where(NodeRelationship.parent_id.in_(frontier_ids)),
            )
        ).all()

        # Group: child_node_id → set of frontier parent node IDs that triggered it
        child_to_parents: dict[int, set[int]] = {}
        for child_node_id, parent_id in rows:
            child_to_parents.setdefault(child_node_id, set()).add(parent_id)

        unvisited = [nid for nid in child_to_parents if nid not in visited_node_ids]
        if not unvisited:
            break

        child_nodes = (
            (
                await session.execute(
                    select(Node)
                    .where(Node.id.in_(unvisited))
                    .where(is_(Node.deactivated_at, None))
                    .options(joinedload(Node.current)),
                )
            )
            .unique()
            .scalars()
            .all()
        )

        next_frontier: set[int] = set()
        for node in child_nodes:
            visited_node_ids.add(node.id)
            parent_ids = child_to_parents.get(node.id, set())

            # Propagate causality: union of causes from all triggering parents
            node_causes: set[int] = set()
            for pid in parent_ids:
                node_causes |= cause_map.get(pid, {pid})
            cause_map[node.id] = node_causes

            will_invalidate = bool(parent_ids & invalidating_ids)
            impact_type = (
                ImpactType.WILL_INVALIDATE if will_invalidate else ImpactType.MAY_AFFECT
            )
            current_status = node.current.status if node.current else NodeStatus.INVALID
            predicted_status = NodeStatus.INVALID if will_invalidate else current_status

            if will_invalidate and current_status != NodeStatus.INVALID:
                node.current.status = NodeStatus.INVALID
                session.add(node.current)
                invalidating_ids.add(node.id)

            cause_names = sorted(
                root_id_to_name[cid] for cid in node_causes if cid in root_id_to_name
            )
            impact_reason = (
                f"Dependent on invalid/deleted node(s): {', '.join(cause_names)}"
                if will_invalidate
                else f"Upstream node(s) changed: {', '.join(cause_names)}"
            )
            results.append(
                DownstreamImpact(
                    name=node.name,
                    node_type=node.type,
                    current_status=current_status,
                    predicted_status=predicted_status,
                    impact_type=impact_type,
                    impact_reason=impact_reason,
                    depth=depth,
                    caused_by=cause_names,
                    is_external=not node.name.startswith(namespace + "."),
                ),
            )
            next_frontier.add(node.id)

        frontier_ids = next_frontier
        depth += 1

    elapsed_ms = (time.perf_counter() - start) * 1000
    will_invalidate_count = sum(
        1 for r in results if r.impact_type == ImpactType.WILL_INVALIDATE
    )
    logger.info(
        "Impact analysis: %d downstream nodes (%d will_invalidate)",
        len(results),
        will_invalidate_count,
    )
    get_metrics_provider().timer(
        "dj.deployment.propagate_impact_ms",
        elapsed_ms,
    )
    get_metrics_provider().gauge(
        "dj.deployment.propagate_impact.nodes_affected",
        len(results),
    )
    get_metrics_provider().gauge(
        "dj.deployment.propagate_impact.will_invalidate",
        will_invalidate_count,
    )
    return results
