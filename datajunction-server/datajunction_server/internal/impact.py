"""
Downstream impact propagation for deployments.
"""

import asyncio
import logging
import time
from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.sql.operators import is_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.database.node import Node, NodeRevision, NodeRelationship
from datajunction_server.instrumentation.provider import get_metrics_provider
from datajunction_server.internal.validation import validate_node_data
from datajunction_server.models.impact import DownstreamImpact, ImpactType
from datajunction_server.models.node import NodeStatus
from datajunction_server.models.node_type import NodeType

logger = logging.getLogger(__name__)


async def propagate_impact(
    session: AsyncSession,
    namespace: str,
    changed_node_names: set[str],
    deleted_node_names: frozenset[str] = frozenset(),
) -> list[DownstreamImpact]:
    """BFS downstream impact analysis with INVALID propagation and validity recovery.

    Must be called inside the caller's active transaction (inside a SAVEPOINT for
    dry-runs).  For dry-runs the caller rolls back the SAVEPOINT, undoing both the
    node changes and any status changes written here.  For wet-runs the caller
    commits, persisting everything.

    Validity recovery: If a downstream node was INVALID and none of its changed
    parents are invalidating (INVALID or deleted), we check if ALL of its parents
    are now VALID. If so, the node is marked VALID (recovered).

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

    # Track root nodes that became VALID (for validity recovery)
    validating_root_ids: set[int] = {
        nid
        for nid, n in changed_by_id.items()
        if n.current and n.current.status == NodeStatus.VALID
    }

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

    # Track recovery candidates: (node_id, node, depth, result_index)
    # These are INVALID nodes with no invalidating parents in the traversal
    recovery_candidates: list[tuple[int, Node, int, int]] = []

    # Track all visited nodes by ID for later parent status lookup
    visited_nodes_by_id: dict[int, Node] = {**changed_by_id, **deleted_by_id}

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
            visited_nodes_by_id[node.id] = node
            parent_ids = child_to_parents.get(node.id, set())

            # Propagate causality: union of causes from all triggering parents
            node_causes: set[int] = set()
            for pid in parent_ids:
                node_causes |= cause_map.get(pid, {pid})
            cause_map[node.id] = node_causes

            will_invalidate = bool(parent_ids & invalidating_ids)
            current_status = node.current.status if node.current else NodeStatus.INVALID

            # Check if this is a recovery candidate:
            # - Currently INVALID
            # - No invalidating parents in this traversal
            # - Has at least one validating root in its causes
            is_recovery_candidate = (
                not will_invalidate
                and current_status == NodeStatus.INVALID
                and bool(node_causes & validating_root_ids)
            )

            if will_invalidate:
                impact_type = ImpactType.WILL_INVALIDATE
                predicted_status = NodeStatus.INVALID
                if current_status != NodeStatus.INVALID:
                    node.current.status = NodeStatus.INVALID
                    session.add(node.current)
                invalidating_ids.add(node.id)
            elif is_recovery_candidate:
                # Tentatively mark as MAY_AFFECT; will update after batch parent check
                impact_type = ImpactType.MAY_AFFECT
                predicted_status = current_status
            else:
                impact_type = ImpactType.MAY_AFFECT
                predicted_status = current_status

            cause_names = sorted(
                root_id_to_name[cid] for cid in node_causes if cid in root_id_to_name
            )
            impact_reason = (
                f"Dependent on invalid/deleted node(s): {', '.join(cause_names)}"
                if will_invalidate
                else f"Upstream node(s) changed: {', '.join(cause_names)}"
            )

            result_index = len(results)
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

            if is_recovery_candidate:
                recovery_candidates.append((node.id, node, depth, result_index))

            next_frontier.add(node.id)

        frontier_ids = next_frontier
        depth += 1

    # Phase 2: Validity recovery via batched parent check
    if recovery_candidates:
        recovered_count = await _process_validity_recovery(
            session=session,
            recovery_candidates=recovery_candidates,
            visited_nodes_by_id=visited_nodes_by_id,
            results=results,
        )
    else:
        recovered_count = 0

    elapsed_ms = (time.perf_counter() - start) * 1000
    will_invalidate_count = sum(
        1 for r in results if r.impact_type == ImpactType.WILL_INVALIDATE
    )
    logger.info(
        "Impact analysis: %d downstream nodes (%d will_invalidate, %d will_recover)",
        len(results),
        will_invalidate_count,
        recovered_count,
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


async def _process_validity_recovery(
    session: AsyncSession,
    recovery_candidates: list[tuple[int, Node, int, int]],
    visited_nodes_by_id: dict[int, Node],
    results: list[DownstreamImpact],
) -> int:
    """
    Process validity recovery for candidate nodes.

    For each candidate:
    1. Check if ALL parents are VALID
    2. If so, validate the node's query to confirm it's actually valid
    3. Only mark as recovered if validation passes

    Args:
        session: Active async session.
        recovery_candidates: List of (node_id, node, depth, result_index).
        visited_nodes_by_id: Map of node_id → Node for nodes in the traversal.
        results: The results list to update in place.

    Returns:
        Number of nodes that were recovered.
    """
    candidate_ids = [c[0] for c in recovery_candidates]

    # Batch query: get all parent node IDs for each candidate
    parent_query = (
        select(
            NodeRevision.node_id.label("child_node_id"),
            NodeRelationship.parent_id.label("parent_node_id"),
        )
        .select_from(NodeRelationship)
        .join(NodeRevision, NodeRelationship.child_id == NodeRevision.id)
        .where(NodeRevision.node_id.in_(candidate_ids))
    )
    parent_rows = (await session.execute(parent_query)).all()

    # Group by child: child_node_id → set of parent_node_ids
    parents_by_child: dict[int, set[int]] = defaultdict(set)
    all_parent_ids: set[int] = set()
    for child_node_id, parent_node_id in parent_rows:
        parents_by_child[child_node_id].add(parent_node_id)
        all_parent_ids.add(parent_node_id)

    # Load parent nodes that aren't already in visited_nodes_by_id
    missing_parent_ids = all_parent_ids - set(visited_nodes_by_id.keys())
    if missing_parent_ids:
        missing_parents = (
            (
                await session.execute(
                    select(Node)
                    .where(Node.id.in_(missing_parent_ids))
                    .options(joinedload(Node.current)),
                )
            )
            .unique()
            .scalars()
            .all()
        )
        for p in missing_parents:
            visited_nodes_by_id[p.id] = p

    # Sort candidates by depth (ascending) so cascading recovery works
    recovery_candidates_sorted = sorted(recovery_candidates, key=lambda c: c[2])

    # Track which nodes we've recovered (they become VALID for later checks)
    recovered_ids: set[int] = set()
    recovered_count = 0

    # Process candidates level by level for cascading recovery
    # Group by depth so we can validate each level in parallel
    candidates_by_depth: dict[int, list[tuple[int, Node, int, int]]] = defaultdict(list)
    for candidate in recovery_candidates_sorted:
        _, _, depth, _ = candidate
        candidates_by_depth[depth].append(candidate)

    for depth in sorted(candidates_by_depth.keys()):
        level_candidates = candidates_by_depth[depth]

        # First pass: filter to candidates with all parents VALID
        valid_parent_candidates: list[tuple[int, Node, int, int]] = []
        for node_id, node, d, result_index in level_candidates:
            parent_ids = parents_by_child.get(node_id, set())
            if not parent_ids:
                continue

            all_parents_valid = True
            for pid in parent_ids:
                parent_node = visited_nodes_by_id.get(pid)
                if parent_node is None:
                    all_parents_valid = False
                    break
                parent_status = (
                    parent_node.current.status
                    if parent_node.current
                    else NodeStatus.INVALID
                )
                if pid in recovered_ids:
                    parent_status = NodeStatus.VALID
                if parent_status != NodeStatus.VALID:
                    all_parents_valid = False
                    break

            if all_parents_valid:
                valid_parent_candidates.append((node_id, node, d, result_index))

        if not valid_parent_candidates:
            continue

        # Second pass: validate candidates in parallel
        # Source and cube nodes don't need SQL validation - they just need valid parents
        to_validate: list[tuple[int, Node, int, int]] = []
        auto_recover: list[tuple[int, Node, int, int]] = []

        for candidate in valid_parent_candidates:
            _, node, _, _ = candidate
            if node.type == NodeType.SOURCE:
                # Source nodes have no SQL to validate
                auto_recover.append(candidate)
            elif node.type == NodeType.CUBE:
                # Cubes are VALID if all their metric/dimension elements are VALID
                # Since we've already verified all parents are VALID, auto-recover
                auto_recover.append(candidate)
            else:
                to_validate.append(candidate)

        # Auto-recover source nodes (they don't have queries to validate)
        for node_id, node, d, result_index in auto_recover:
            node.current.status = NodeStatus.VALID
            session.add(node.current)
            recovered_ids.add(node_id)
            recovered_count += 1
            _update_result_to_recovered(results, result_index)

        # Validate query nodes in parallel
        if to_validate:
            # Ensure nodes have their revisions loaded with necessary relationships
            node_ids_to_load = [c[0] for c in to_validate]
            loaded_nodes = (
                (
                    await session.execute(
                        select(Node)
                        .where(Node.id.in_(node_ids_to_load))
                        .options(
                            selectinload(Node.current).options(
                                selectinload(NodeRevision.columns),
                                selectinload(NodeRevision.parents),
                            ),
                        ),
                    )
                )
                .unique()
                .scalars()
                .all()
            )
            loaded_by_id = {n.id: n for n in loaded_nodes}

            # Validate in parallel
            validation_tasks = [
                validate_node_data(loaded_by_id[node_id].current, session)
                for node_id, _, _, _ in to_validate
            ]
            validation_results = await asyncio.gather(
                *validation_tasks,
                return_exceptions=True,
            )

            # Process validation results
            for (node_id, node, d, result_index), validation_result in zip(
                to_validate,
                validation_results,
            ):
                if isinstance(validation_result, Exception):
                    logger.warning(
                        "Validation failed for recovery candidate %s: %s",
                        node.name,
                        validation_result,
                    )
                    continue

                if validation_result.status == NodeStatus.VALID:
                    node.current.status = NodeStatus.VALID
                    session.add(node.current)
                    recovered_ids.add(node_id)
                    recovered_count += 1
                    _update_result_to_recovered(results, result_index)
                else:
                    logger.info(
                        "Recovery candidate %s failed validation: %s",
                        node.name,
                        [str(e) for e in validation_result.errors],
                    )

    return recovered_count


def _update_result_to_recovered(results: list[DownstreamImpact], result_index: int):
    """Update a result entry to reflect successful recovery."""
    old_result = results[result_index]
    results[result_index] = DownstreamImpact(
        name=old_result.name,
        node_type=old_result.node_type,
        current_status=old_result.current_status,
        predicted_status=NodeStatus.VALID,
        impact_type=ImpactType.WILL_RECOVER,
        impact_reason=f"Validated and recovered - upstream nodes now valid: {', '.join(old_result.caused_by)}",
        depth=old_result.depth,
        caused_by=old_result.caused_by,
        is_external=old_result.is_external,
    )
