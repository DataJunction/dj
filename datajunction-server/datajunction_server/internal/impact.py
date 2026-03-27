"""
Impact preview — compute the blast radius of proposed node changes.

Entry point:
- ``compute_impact``: given a dict of {node_name: NodeChange}, BFS downstream and return
  a topo-sorted list of ImpactedNode objects.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload, selectinload
from sqlalchemy.sql.expression import bindparam, text

from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJException
from datajunction_server.internal.validation import validate_node_data
from datajunction_server.models.impact_preview import (
    ImpactedNode,
    NodeChange,
)
from datajunction_server.models.node import NodeStatus
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import (
    _node_output_options,
    get_dimension_inbound_bfs,
    get_downstream_nodes,
    topological_sort,
)
from datajunction_server.sql.parsing import ast

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lightweight reference checks (kept for dim-link and fallback cases)
# ---------------------------------------------------------------------------


def references_changed_columns(node: Node, removed_cols: set[str]) -> bool:
    """Return True if any of the node's output columns overlap with removed_cols."""
    node_col_names = {c.name for c in node.current.columns}
    return bool(node_col_names & removed_cols)


def references_removed_dim(node: Node, removed_dim_names: set[str]) -> bool:
    """Return True if a metric or cube references any attribute of the removed dimensions.

    For metrics: checks node.current.columns where column.dimension has a name in
                 removed_dim_names.
    For cubes: checks node.current.parents (which include all metric/dimension nodes
               the cube was built from) for membership in removed_dim_names.
    """
    if node.type == NodeType.METRIC:
        for col in node.current.columns:
            if col.dimension and col.dimension.name in removed_dim_names:
                return True
        # Also check parents — some metrics explicitly list dimension parents
        for parent in node.current.parents:
            if parent.name in removed_dim_names:
                return True
        return False

    if node.type == NodeType.CUBE:
        # Cube parents include all dimension nodes used by the cube
        for parent in node.current.parents:
            if parent.name in removed_dim_names:
                return True
        return False

    return False


def _cube_dim_output_options() -> list:
    """Load options for cubes that need cube_elements with their source dimension nodes.

    Each cube element's ``node_revision.node.name`` is the dimension (or metric) node
    it was drawn from.  Loading this chain lets us check whether any element comes from
    a dimension node whose link was removed.
    """
    return [
        *_node_output_options(),
        selectinload(Node.current).options(
            selectinload(NodeRevision.cube_elements)
            .selectinload(Column.node_revision)
            .options(
                selectinload(NodeRevision.node),
                noload(NodeRevision.created_by),
            ),
        ),
    ]


async def _check_cube_dim_link_impacts(
    session: AsyncSession,
    node_name: str,
    dim_links_removed: set[str],
    caused_by: list[str],
    impacted: dict[str, ImpactedNode],
    node_cache: dict[str, Node],
) -> None:
    """Find all downstream cubes that reference any of the removed dimension links.

    Traverses the full DAG downstream from ``node_name``, collects every cube,
    then checks each cube's ``cube_elements`` for elements whose ``dimension.name``
    is in ``dim_links_removed``.  Matching cubes are recorded in ``impacted``.
    """
    _logger.info(
        "[cube-dim-impact] checking cubes downstream of %r for dim_links_removed=%r",
        node_name,
        dim_links_removed,
    )
    downstream_cubes = await get_downstream_nodes(
        session,
        node_name,
        node_type=NodeType.CUBE,
        depth=-1,
        options=_cube_dim_output_options(),
    )
    _logger.info(
        "[cube-dim-impact] found %d downstream cube(s) from %r: %r",
        len(downstream_cubes),
        node_name,
        [c.name for c in downstream_cubes],
    )
    reason = f"Dimension links removed: {', '.join(sorted(dim_links_removed))}"
    for cube in downstream_cubes:
        node_cache[cube.name] = cube
        elem_source_nodes = [
            (
                e.name,
                e.node_revision.node.name
                if e.node_revision and e.node_revision.node
                else None,
            )
            for e in cube.current.cube_elements
        ]
        _logger.info(
            "[cube-dim-impact] cube %r has cube_elements (name, source_node): %r",
            cube.name,
            elem_source_nodes,
        )
        for element in cube.current.cube_elements:
            source_node_name = (
                element.node_revision.node.name
                if element.node_revision and element.node_revision.node
                else None
            )
            if source_node_name and source_node_name in dim_links_removed:
                _logger.info(
                    "[cube-dim-impact] HIT: cube %r element %r comes from removed dim %r",
                    cube.name,
                    element.name,
                    source_node_name,
                )
                _record_impact(
                    impacted,
                    cube,
                    impact_type="dimension_link",
                    caused_by=caused_by,
                    reason=reason,
                )
                break  # one match is enough per cube
        else:
            _logger.info(
                "[cube-dim-impact] MISS: cube %r has no elements from %r",
                cube.name,
                dim_links_removed,
            )


# ---------------------------------------------------------------------------
# BFS propagation state
# ---------------------------------------------------------------------------


@dataclass
class _PropagatedChange:
    """Accumulated change state as it propagates through the DAG."""

    columns_removed: set[str] = field(default_factory=set)
    dim_links_removed: set[str] = field(default_factory=set)
    is_deleted: bool = False
    caused_by: list[str] = field(default_factory=list)
    # Proposed column state for the node that owns this change entry.
    # When set, downstream validation uses this list as the upstream column state
    # instead of loading from DB — giving accurate impact detection without a DB write.
    proposed_columns: list | None = None  # objects with .name and .type


# ---------------------------------------------------------------------------
# Per-node validation against proposed upstream state
# ---------------------------------------------------------------------------


async def _validate_downstream_node(
    session: AsyncSession,
    child: Node,
    upstream_proposed: dict[str, list],  # upstream_name → proposed columns
) -> tuple[bool, list]:
    """Validate ``child`` as if its upstream nodes had the given proposed columns.

    Returns
    -------
    (is_impacted, new_output_columns)
        is_impacted     — True if the child would break or lose output columns
        new_output_cols — columns the child would expose after the change
                          (empty list when validation fails entirely)
    """
    if child.current.query is None:
        # SOURCE nodes have no query; treat as not impacted by column changes
        return False, list(child.current.columns)

    ctx = ast.CompileContext(
        session=session,
        exception=DJException(),
        column_overrides=upstream_proposed,
    )
    try:
        validator = await validate_node_data(
            child.current,
            session,
            compile_context=ctx,
        )
    except Exception:
        return True, []

    if validator.status == NodeStatus.INVALID or validator.errors:
        return True, []

    current_col_names = {c.name for c in child.current.columns}
    new_col_names = {c.name for c in validator.columns}
    cols_lost = current_col_names - new_col_names
    is_impacted = bool(cols_lost)
    return is_impacted, list(validator.columns)


# ---------------------------------------------------------------------------
# compute_impact
# ---------------------------------------------------------------------------


async def _batch_get_children(
    session: AsyncSession,
    parent_node_ids: list[int],
    options: list,
) -> dict[int, list[Node]]:
    """Fetch direct DAG children for multiple parent nodes in two queries.

    Returns a dict mapping parent_node_id → list of child Node objects.
    ``noderelationship.parent_id`` is a ``node.id`` FK, so we can pass all
    frontier node IDs in a single ``IN`` clause.
    """
    if not parent_node_ids:
        return {}

    rows = (
        await session.execute(
            text("""
                SELECT DISTINCT n.id AS child_id, rel.parent_id
                FROM noderelationship rel
                JOIN noderevision nr ON rel.child_id = nr.id
                JOIN node n ON n.id = nr.node_id
                    AND n.current_version = nr.version
                    AND n.deactivated_at IS NULL
                WHERE rel.parent_id IN :parent_ids
            """).bindparams(bindparam("parent_ids", expanding=True)),
            {"parent_ids": parent_node_ids},
        )
    ).fetchall()

    if not rows:
        return {}

    child_ids = list({r.child_id for r in rows})
    child_nodes = (
        (
            await session.execute(
                select(Node).where(Node.id.in_(child_ids)).options(*options),
            )
        )
        .unique()
        .scalars()
        .all()
    )
    child_by_id: dict[int, Node] = {n.id: n for n in child_nodes}

    result: dict[int, list[Node]] = defaultdict(list)
    for row in rows:
        if row.child_id in child_by_id:
            result[row.parent_id].append(child_by_id[row.child_id])
    return result


async def compute_impact(
    session: AsyncSession,
    changed_nodes: dict[str, NodeChange],
) -> list[ImpactedNode]:
    """BFS over the DAG to find all downstream nodes affected by the given changes.

    Returns a topo-sorted list of ImpactedNode objects.

    Parameters
    ----------
    session:
        Async DB session.
    changed_nodes:
        Mapping of node_name → NodeChange describing what changed on each node.
    """
    if not changed_nodes:
        return []

    # propagated_change[node_name] accumulates the change as it propagates forward
    propagated_change: dict[str, _PropagatedChange] = {}
    for name, change in changed_nodes.items():
        propagated_change[name] = _PropagatedChange(
            columns_removed=set(change.columns_removed),
            dim_links_removed=set(change.dim_links_removed),
            is_deleted=change.is_deleted,
            caused_by=[name],
        )

    # Seed the BFS frontier with the directly changed nodes
    frontier: set[str] = set(changed_nodes.keys())
    visited: set[str] = set(changed_nodes.keys())
    impacted: dict[str, ImpactedNode] = {}

    # Batch-load all seed nodes in one query
    node_cache: dict[str, Node] = {}
    seed_nodes = await Node.get_by_names(
        session,
        list(changed_nodes.keys()),
        options=list(_node_output_options()),
    )
    for node in seed_nodes:
        node_cache[node.name] = node
        pc = propagated_change[node.name]
        if pc.columns_removed and not pc.is_deleted:
            current_col_names = {c.name for c in node.current.columns}
            actually_removed = pc.columns_removed & current_col_names
            pc.columns_removed = actually_removed
            if actually_removed:
                pc.proposed_columns = [
                    c for c in node.current.columns if c.name not in actually_removed
                ]

    # ---------------------------------------------------------------------------
    # Cube dim-link impact pass: for each node that has dim_links_removed,
    # find all downstream cubes that reference those dimensions via cube_elements.
    # This is done upfront because cubes declare dimensions in cube_elements
    # (not via SQL parents), so the BFS column checks can't detect them.
    # ---------------------------------------------------------------------------
    _logger.info(
        "[compute_impact] changed_nodes: %r",
        {
            n: {
                "dim_links_removed": list(c.dim_links_removed),
                "columns_removed": list(c.columns_removed),
                "is_deleted": c.is_deleted,
            }
            for n, c in changed_nodes.items()
        },
    )
    for name, change in changed_nodes.items():
        if change.dim_links_removed:
            _logger.info(
                "[compute_impact] node %r has dim_links_removed=%r — running cube pass",
                name,
                change.dim_links_removed,
            )
            await _check_cube_dim_link_impacts(
                session=session,
                node_name=name,
                dim_links_removed=set(change.dim_links_removed),
                caused_by=propagated_change[name].caused_by,
                impacted=impacted,
                node_cache=node_cache,
            )

    # ---------------------------------------------------------------------------
    # Case 3 seed: if a dimension node loses columns, find all nodes that expose
    # it via a DimensionLink (using get_dimension_inbound_bfs at depth=1), seed
    # them into the frontier, and run the cube pass from each.
    # ---------------------------------------------------------------------------
    dim_nodes_losing_cols = [
        node_cache[name]
        for name, change in changed_nodes.items()
        if change.columns_removed
        and node_cache.get(name) is not None
        and node_cache[name].type == NodeType.DIMENSION
    ]
    if dim_nodes_losing_cols:
        holder_names: set[str] = set()
        for dim_node in dim_nodes_losing_cols:
            holder_displays, _ = await get_dimension_inbound_bfs(
                session, dim_node, depth=1,
            )
            holder_names.update(d.name for d in holder_displays)

        holders = await Node.get_by_names(
            session,
            list(holder_names),
            options=list(_node_output_options()),
        )
        dim_node_names = [n.name for n in dim_nodes_losing_cols]
        for holder in holders:
            if holder.name not in propagated_change:
                propagated_change[holder.name] = _PropagatedChange(
                    caused_by=dim_node_names,
                )
            propagated_change[holder.name].dim_links_removed.update(dim_node_names)
            node_cache[holder.name] = holder
            frontier.add(holder.name)
            visited.add(holder.name)
            await _check_cube_dim_link_impacts(
                session=session,
                node_name=holder.name,
                dim_links_removed=set(dim_node_names),
                caused_by=dim_node_names,
                impacted=impacted,
                node_cache=node_cache,
            )

    # ---------------------------------------------------------------------------
    # BFS main loop — one batch query per level instead of one per node
    # ---------------------------------------------------------------------------
    while frontier:
        next_frontier: set[str] = set()

        # Collect the node.id for every node currently in the frontier
        parent_ids = [node_cache[name].id for name in frontier if name in node_cache]
        # Single round-trip: get all direct children for the entire frontier
        children_by_parent = await _batch_get_children(
            session, parent_ids, list(_node_output_options()),
        )
        # Map node.id → node_name so we can look up propagated_change
        id_to_name = {node_cache[n].id: n for n in frontier if n in node_cache}

        for parent_node_id, children in children_by_parent.items():
            node_name = id_to_name.get(parent_node_id)
            if node_name is None:
                continue
            prop: _PropagatedChange | None = propagated_change.get(node_name)
            if not prop:
                continue

            upstream_proposed: dict[str, list] = {}
            if prop.proposed_columns is not None:
                upstream_proposed[node_name] = prop.proposed_columns

            for child in children:
                child_name = child.name
                node_cache[child_name] = child

                # --- Case 1: column removed or node deleted ---
                if prop.is_deleted or prop.columns_removed:
                    if prop.is_deleted:
                        _record_impact(
                            impacted,
                            child,
                            impact_type="deleted_parent",
                            caused_by=prop.caused_by,
                            reason="Upstream node was deleted",
                        )
                        _merge_propagated(
                            propagated_change,
                            child_name,
                            columns_removed=set(),
                            dim_links_removed=set(),
                            is_deleted=False,
                            caused_by=[node_name],
                        )
                        if child_name not in visited:
                            next_frontier.add(child_name)
                            visited.add(child_name)
                    elif upstream_proposed:
                        is_impacted, new_cols = await _validate_downstream_node(
                            session,
                            child,
                            upstream_proposed,
                        )
                        if is_impacted:
                            _record_impact(
                                impacted,
                                child,
                                impact_type="column",
                                caused_by=prop.caused_by,
                                reason=_column_reason(prop),
                            )
                            _merge_propagated(
                                propagated_change,
                                child_name,
                                columns_removed=prop.columns_removed,
                                dim_links_removed=set(),
                                is_deleted=False,
                                caused_by=[node_name],
                                proposed_columns=new_cols if new_cols else None,
                            )
                            if child_name not in visited:
                                next_frontier.add(child_name)
                                visited.add(child_name)
                    else:
                        # Fallback heuristic when no proposed column state available
                        is_terminal = child.type in (NodeType.METRIC, NodeType.CUBE)
                        if is_terminal or references_changed_columns(
                            child, prop.columns_removed,
                        ):
                            _record_impact(
                                impacted,
                                child,
                                impact_type="column",
                                caused_by=prop.caused_by,
                                reason=_column_reason(prop),
                            )
                            _merge_propagated(
                                propagated_change,
                                child_name,
                                columns_removed=prop.columns_removed,
                                dim_links_removed=set(),
                                is_deleted=False,
                                caused_by=[node_name],
                            )
                            if child_name not in visited:
                                next_frontier.add(child_name)
                                visited.add(child_name)

                # --- Case 2: dimension link removed — metrics only ---
                # Cubes are handled by the upfront _check_cube_dim_link_impacts pass.
                if prop.dim_links_removed:
                    if child.type == NodeType.METRIC:
                        if references_removed_dim(child, prop.dim_links_removed):
                            _record_impact(
                                impacted,
                                child,
                                impact_type="dimension_link",
                                caused_by=prop.caused_by,
                                reason=_dim_link_reason(prop),
                            )
                    elif child.type != NodeType.CUBE:
                        # Propagate through intermediate nodes so downstream metrics
                        # can be reached.
                        _merge_propagated(
                            propagated_change,
                            child_name,
                            columns_removed=set(),
                            dim_links_removed=prop.dim_links_removed,
                            is_deleted=False,
                            caused_by=prop.caused_by,
                        )
                        if child_name not in visited:
                            next_frontier.add(child_name)
                            visited.add(child_name)

        frontier = next_frontier

    if not impacted:
        return []

    # Topo-sort the impacted nodes using the cached Node objects
    impacted_node_objs = [node_cache[n] for n in impacted if n in node_cache]
    try:
        sorted_nodes = topological_sort(impacted_node_objs)
    except Exception:  # cycle guard — fall back to unsorted
        sorted_nodes = impacted_node_objs

    return [impacted[n.name] for n in sorted_nodes if n.name in impacted]


def _record_impact(
    impacted: dict[str, ImpactedNode],
    node: Node,
    *,
    impact_type: str,
    caused_by: list[str],
    reason: str,
) -> None:
    """Insert or update an ImpactedNode entry."""
    if node.name in impacted:
        # Merge caused_by lists if already recorded
        existing = impacted[node.name]
        merged_causes = list(dict.fromkeys(existing.caused_by + caused_by))
        impacted[node.name] = existing.model_copy(update={"caused_by": merged_causes})
    else:
        impacted[node.name] = ImpactedNode(
            name=node.name,
            node_type=node.type,
            namespace=node.namespace,
            current_status=node.current.status,
            projected_status=NodeStatus.INVALID,
            reason=reason,
            caused_by=caused_by,
            impact_type=impact_type,
        )


def _merge_propagated(
    propagated_change: dict[str, _PropagatedChange],
    name: str,
    *,
    columns_removed: set[str],
    dim_links_removed: set[str],
    is_deleted: bool,
    caused_by: list[str],
    proposed_columns: list | None = None,
) -> None:
    """Merge propagated change state into the dict entry for `name`."""
    if name not in propagated_change:
        propagated_change[name] = _PropagatedChange(caused_by=list(caused_by))
    pc = propagated_change[name]
    pc.columns_removed |= columns_removed
    pc.dim_links_removed |= dim_links_removed
    pc.is_deleted = pc.is_deleted or is_deleted
    for c in caused_by:
        if c not in pc.caused_by:
            pc.caused_by.append(c)
    if proposed_columns is not None:
        pc.proposed_columns = proposed_columns


def _column_reason(change: _PropagatedChange) -> str:
    if change.is_deleted:
        return "Upstream node was deleted"
    cols = sorted(change.columns_removed)
    return f"Upstream columns removed: {', '.join(cols)}"


def _dim_link_reason(change: _PropagatedChange) -> str:
    dims = sorted(change.dim_links_removed)
    return f"Dimension links removed: {', '.join(dims)}"
