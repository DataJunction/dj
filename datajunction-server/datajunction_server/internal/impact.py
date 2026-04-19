"""
Downstream impact propagation for deployments.

Structured in three phases:
  Phase 1: BFS via SQL parent graph (NodeRelationship) — discovers all downstream nodes
  Phase 2: BFS via dimension link graph (DimensionLink) — discovers additional affected nodes
  Phase 3: Revalidate all downstream nodes using lightweight type inference
"""

import asyncio
import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.sql.operators import is_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.database.node import Node, NodeRevision, NodeRelationship
from datajunction_server.instrumentation.provider import get_metrics_provider
from datajunction_server.internal.deployment.dimension_reachability import (
    DimensionReachability,
)
from datajunction_server.internal.deployment.type_inference import (
    columns_signature_changed,
    parse_query,
    validate_node_query,
)
from datajunction_server.models.impact import DownstreamImpact, ImpactType
from datajunction_server.models.node import NodeStatus
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import get_metric_parents_map
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.types import ColumnType
from datajunction_server.utils import SEPARATOR

logger = logging.getLogger(__name__)


@dataclass
class PropagationContext:
    """Shared state across all propagation phases."""

    namespace: str
    changed_by_id: dict[int, Node] = field(default_factory=dict)
    deleted_by_id: dict[int, Node] = field(default_factory=dict)
    # Nodes whose dimension links changed (separate from query/column changes)
    link_changed_by_id: dict[int, Node] = field(default_factory=dict)

    # Causality: node_id → set of root node IDs that caused it
    cause_map: dict[int, set[int]] = field(default_factory=dict)
    root_id_to_name: dict[int, str] = field(default_factory=dict)

    # All nodes visited during BFS (for parent column lookups in Phase 3)
    visited_nodes_by_id: dict[int, Node] = field(default_factory=dict)


async def propagate_impact(
    session: AsyncSession,
    namespace: str,
    changed_node_names: set[str],
    deleted_node_names: frozenset[str] = frozenset(),
    changed_link_node_names: set[str] | None = None,
) -> list[DownstreamImpact]:
    """BFS downstream impact analysis with revalidation.

    Must be called inside the caller's active transaction (inside a SAVEPOINT for
    dry-runs).  For dry-runs the caller rolls back the SAVEPOINT, undoing both the
    node changes and any status changes written here.  For wet-runs the caller
    commits, persisting everything.

    Args:
        session: Active async session (inside a SAVEPOINT for dry-runs).
        namespace: Deployment namespace used to flag external impacts.
        changed_node_names: Names of nodes that were created or updated.
        deleted_node_names: Names of nodes about to be deleted (still in DB at
            call time — caller must invoke this before hard_delete_node).
        changed_link_node_names: Names of nodes whose dimension links changed.

    Returns:
        List of DownstreamImpact describing each affected downstream node.
    """
    start = time.perf_counter()
    all_root_names = changed_node_names | deleted_node_names
    if not all_root_names and not changed_link_node_names:
        return []

    ctx = await _build_propagation_context(
        session,
        namespace,
        changed_node_names,
        deleted_node_names,
        changed_link_node_names,
    )

    # Phase 1: discover affected nodes via SQL parent graph
    parent_graph_impacts = await _propagate_via_parent_graph(session, ctx)

    # Phase 2: discover affected nodes via dimension link graph
    link_impacts = await _propagate_via_dimension_links(
        session,
        ctx,
        changed_link_node_names or set(),
    )

    # Merge discoveries
    all_impacts = _merge_impacts(parent_graph_impacts, link_impacts)

    # Phase 3: revalidate all downstream nodes and apply status changes
    results = await _revalidate_and_apply(session, ctx, all_impacts)

    _emit_metrics(start, results)
    return results


# ---------------------------------------------------------------------------
# Context building
# ---------------------------------------------------------------------------


async def _build_propagation_context(
    session: AsyncSession,
    namespace: str,
    changed_node_names: set[str],
    deleted_node_names: frozenset[str],
    changed_link_node_names: set[str] | None = None,
) -> PropagationContext:
    """Load root nodes and build the shared propagation context."""
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
    # Load nodes whose dimension links changed but that aren't already
    # in changed_node_names (they may only have link changes, no query changes).
    link_only_names = (
        (changed_link_node_names or set()) - changed_node_names - deleted_node_names
    )
    link_changed_nodes = (
        await Node.get_by_names(
            session,
            list(link_only_names),
            options=[joinedload(Node.current)],
        )
        if link_only_names
        else []
    )

    changed_by_id = {n.id: n for n in changed_nodes}
    deleted_by_id = {n.id: n for n in deleted_nodes}
    link_changed_by_id = {n.id: n for n in link_changed_nodes}
    all_root_ids = set(changed_by_id) | set(deleted_by_id) | set(link_changed_by_id)

    cause_map = {nid: {nid} for nid in all_root_ids}
    root_id_to_name = {
        **{n.id: n.name for n in changed_nodes},
        **{n.id: n.name for n in deleted_nodes},
        **{n.id: n.name for n in link_changed_nodes},
    }

    return PropagationContext(
        namespace=namespace,
        changed_by_id=changed_by_id,
        deleted_by_id=deleted_by_id,
        link_changed_by_id=link_changed_by_id,
        cause_map=cause_map,
        root_id_to_name=root_id_to_name,
        visited_nodes_by_id={**changed_by_id, **deleted_by_id, **link_changed_by_id},
    )


# ---------------------------------------------------------------------------
# Phase 1: SQL parent graph BFS
# ---------------------------------------------------------------------------


async def _propagate_via_parent_graph(
    session: AsyncSession,
    ctx: PropagationContext,
) -> list[DownstreamImpact]:
    """BFS through NodeRelationship to find all downstream nodes.

    Returns impacts without mutating DB state — Phase 3 determines the actual
    impact type via revalidation.
    """
    all_root_ids = (
        set(ctx.changed_by_id) | set(ctx.deleted_by_id) | set(ctx.link_changed_by_id)
    )
    frontier_ids = set(all_root_ids)
    visited_node_ids = set(frontier_ids)
    results: list[DownstreamImpact] = []
    depth = 1

    while frontier_ids:
        rows = (
            await session.execute(
                select(NodeRevision.node_id, NodeRelationship.parent_id)
                .join(NodeRelationship, NodeRelationship.child_id == NodeRevision.id)
                .where(NodeRelationship.parent_id.in_(frontier_ids)),
            )
        ).all()

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
            ctx.visited_nodes_by_id[node.id] = node
            parent_ids = child_to_parents.get(node.id, set())

            # Propagate causality
            node_causes: set[int] = set()
            for pid in parent_ids:
                node_causes |= ctx.cause_map.get(pid, {pid})
            ctx.cause_map[node.id] = node_causes

            current_status = node.current.status if node.current else NodeStatus.INVALID

            cause_names = sorted(
                ctx.root_id_to_name[cid]
                for cid in node_causes
                if cid in ctx.root_id_to_name
            )

            results.append(
                DownstreamImpact(
                    name=node.name,
                    node_type=node.type,
                    current_status=current_status,
                    predicted_status=current_status,
                    impact_type=ImpactType.MAY_AFFECT,
                    impact_reason=f"Upstream node(s) changed: {', '.join(cause_names)}",
                    depth=depth,
                    caused_by=cause_names,
                    is_external=not node.name.startswith(ctx.namespace + "."),
                ),
            )
            next_frontier.add(node.id)

        frontier_ids = next_frontier
        depth += 1

    return results


# ---------------------------------------------------------------------------
# Phase 2: Dimension link graph
# ---------------------------------------------------------------------------


async def _propagate_via_dimension_links(
    session: AsyncSession,
    ctx: PropagationContext,
    changed_link_node_names: set[str],
) -> list[DownstreamImpact]:
    """Find additional nodes affected by dimension link changes.

    Link-changed nodes are already included in the Phase 1 BFS roots
    (via ctx.link_changed_by_id), so downstream cubes are discovered
    through the SQL parent graph.  Dimension reachability checking for
    those cubes happens in the Phase 3 post-pass
    (_check_cube_dimension_reachability).

    This method handles any ADDITIONAL impacts not discoverable via
    NodeRelationship — currently none, but reserved for future cases
    like dimension-link-only dependencies.
    """
    return []


# ---------------------------------------------------------------------------
# Merge impacts
# ---------------------------------------------------------------------------


def _merge_impacts(
    parent_impacts: list[DownstreamImpact],
    link_impacts: list[DownstreamImpact],
) -> list[DownstreamImpact]:
    """Combine Phase 1 and Phase 2 results.

    If the same node appears in both, keep the higher-severity impact
    (WILL_INVALIDATE > MAY_AFFECT).
    """
    if not link_impacts:
        return parent_impacts

    by_name: dict[str, DownstreamImpact] = {}
    for impact in parent_impacts:
        by_name[impact.name] = impact

    severity = {
        ImpactType.WILL_INVALIDATE: 2,
        ImpactType.MAY_AFFECT: 1,
        ImpactType.UNCHANGED: 0,
    }
    for impact in link_impacts:
        existing = by_name.get(impact.name)
        if existing is None:
            by_name[impact.name] = impact
        elif severity.get(impact.impact_type, 0) > severity.get(
            existing.impact_type,
            0,
        ):
            by_name[impact.name] = impact

    return list(by_name.values())


# ---------------------------------------------------------------------------
# Phase 3: Revalidate and apply
# ---------------------------------------------------------------------------


async def _revalidate_and_apply(
    session: AsyncSession,
    ctx: PropagationContext,
    impacts: list[DownstreamImpact],
) -> list[DownstreamImpact]:
    """Revalidate all downstream nodes and apply status changes.

    Every downstream node is revalidated using validate_node_query.
    Nodes are processed level-by-level so that failures at depth N
    cascade to children at depth N+1 (failed parents are excluded
    from the parent_columns_map).
    """
    results: list[DownstreamImpact] = []
    nodes_by_name = _build_name_index(ctx)

    by_depth: dict[int, list[DownstreamImpact]] = defaultdict(list)
    for impact in impacts:
        by_depth[impact.depth].append(impact)

    if not by_depth:
        return results

    # Collect queries to pre-parse from nodes we already visited in Phase 1
    visited_by_name = {n.name: n for n in ctx.visited_nodes_by_id.values()}
    queries_to_parse: dict[str, str] = {}
    for impact in impacts:
        node = visited_by_name.get(impact.name)
        if (
            node
            and node.current
            and node.current.query
            and node.type != NodeType.SOURCE
        ):
            queries_to_parse[impact.name] = node.current.query

    # Run DB load and ANTLR parsing concurrently:
    # - DB load is async IO (awaitable)
    # - ANTLR parsing is CPU-bound (threadpool)
    all_names = [impact.name for impact in impacts]

    async def _parse_all_queries() -> dict[str, ast.Query | Exception]:
        """Parse all queries in a threadpool."""
        loop = asyncio.get_running_loop()
        parsed: dict[str, ast.Query | Exception] = {}
        with ThreadPoolExecutor(max_workers=min(8, len(queries_to_parse) or 1)) as pool:
            futures = {
                name: loop.run_in_executor(pool, parse_query, query_str)
                for name, query_str in queries_to_parse.items()
            }
            for name, future in futures.items():
                try:
                    parsed[name] = await future
                except Exception as exc:
                    parsed[name] = exc
        return parsed

    # Run DB load and ANTLR parsing concurrently
    loaded_nodes, pre_parsed = await asyncio.gather(
        _batch_load_nodes_for_revalidation(session, all_names),
        _parse_all_queries(),
    )

    # Seed with deleted and invalid root nodes — their children should
    # not see their columns during revalidation.
    failed_names: set[str] = {n.name for n in ctx.deleted_by_id.values()} | {
        n.name
        for n in ctx.changed_by_id.values()
        if n.current and n.current.status == NodeStatus.INVALID
    }

    for depth in sorted(by_depth.keys()):
        for impact in by_depth[depth]:
            # Use pre-parsed AST if available (parsed in threadpool)
            parsed_ast = pre_parsed.get(impact.name)
            if isinstance(parsed_ast, Exception):
                parsed_ast = None  # Fall back to inline parse
            revalidated = _revalidate_single_node(
                impact,
                loaded_nodes,
                nodes_by_name,
                session,
                failed_names,
                pre_parsed_query=parsed_ast,
            )
            if revalidated.impact_type == ImpactType.WILL_INVALIDATE:
                failed_names.add(impact.name)
            results.append(revalidated)

    # Post-pass: check dimension reachability for any cubes in the results.
    # A cube's query revalidation always passes (cubes have no query), but
    # its dimension accessibility may have changed due to upstream link changes.
    if ctx.link_changed_by_id:  # pragma: no cover
        results = await _check_cube_dimension_reachability(
            session,
            ctx,
            results,
            loaded_nodes,
        )

    return results


async def _check_cube_dimension_reachability(
    session: AsyncSession,
    ctx: PropagationContext,
    results: list[DownstreamImpact],
    loaded_nodes: dict[str, Node],
) -> list[DownstreamImpact]:
    """Check dimension reachability for cubes discovered in the impact set.

    When upstream dimension links change, a cube's query still validates
    (cubes have no query) but its dimensions may become unreachable.
    This post-pass uses DimensionReachability to detect that.

    Prefers already-loaded nodes (from Phase 3 batch load and BFS visited
    nodes) to avoid redundant DB queries.
    """
    # Find cube impacts that currently show MAY_AFFECT (passthrough from Phase 3)
    cube_impacts = [
        (i, r)
        for i, r in enumerate(results)
        if r.node_type == NodeType.CUBE and r.impact_type == ImpactType.MAY_AFFECT
    ]
    if not cube_impacts:
        return results

    # All nodes already in memory from Phase 1 BFS + Phase 3 batch load
    known_nodes = {
        **{n.name: n for n in ctx.visited_nodes_by_id.values()},
        **loaded_nodes,
    }

    # Get cube nodes — prefer already-loaded, fall back to DB
    cube_names = [r.name for _, r in cube_impacts]
    cube_map: dict[str, Node] = {}
    missing_cube_names: list[str] = []
    for name in cube_names:
        if name in known_nodes and known_nodes[name].current:
            cube_map[name] = known_nodes[name]
        else:
            missing_cube_names.append(name)  # pragma: no cover
    if missing_cube_names:  # pragma: no cover
        db_cubes = await Node.get_by_names(
            session,
            missing_cube_names,
            options=[
                joinedload(Node.current).options(selectinload(NodeRevision.columns)),
            ],
        )
        cube_map.update({n.name: n for n in db_cubes})

    # Collect metric names from cubes, load from known nodes first
    all_metric_names: set[str] = set()
    for cube_node in cube_map.values():
        if cube_node.current:  # pragma: no branch
            all_metric_names.update(cube_node.current.cube_node_metrics)

    metric_nodes: list[Node] = []
    missing_metric_names: list[str] = []
    for name in all_metric_names:
        if name in known_nodes and known_nodes[name].current:
            metric_nodes.append(known_nodes[name])
        else:
            missing_metric_names.append(name)  # pragma: no cover
    if missing_metric_names:  # pragma: no cover
        db_metrics = await Node.get_by_names(
            session,
            missing_metric_names,
            options=[joinedload(Node.current)],
        )
        metric_nodes.extend(db_metrics)

    # Resolve metric → non-metric parents (batched DB query)
    metric_to_parents = await get_metric_parents_map(session, metric_nodes)

    # Build reachability from the post-deployment state
    local_names: dict[int, str] = {}
    all_parent_rev_ids: set[int] = set()
    for parents in metric_to_parents.values():
        for p in parents:
            if p.current:  # pragma: no branch
                all_parent_rev_ids.add(p.current.id)
                local_names[p.current.id] = p.name

    all_dim_node_names: set[str] = set()
    for cube_node in cube_map.values():
        if cube_node.current:  # pragma: no branch
            for dim in cube_node.current.cube_node_dimensions:
                all_dim_node_names.add(dim.rsplit(SEPARATOR, 1)[0])

    reachability = await DimensionReachability.build(
        session,
        all_parent_rev_ids,
        all_dim_node_names,
        local_names=local_names,
    )

    # Check each cube
    updated_results = list(results)
    for idx, impact in cube_impacts:
        cube_node = cube_map.get(impact.name)  # type: ignore[assignment]
        if not cube_node or not cube_node.current:  # pragma: no cover
            continue

        cube_parent_rev_ids = {
            p.current.id
            for metric_name in cube_node.current.cube_node_metrics
            for p in metric_to_parents.get(metric_name, [])
            if p.current
        }
        requested_dims = {
            dim.rsplit(SEPARATOR, 1)[0]
            for dim in cube_node.current.cube_node_dimensions
        }
        unreachable = reachability.unreachable_dimensions(
            cube_parent_rev_ids,
            requested_dims,
        )
        if unreachable:
            dim_list = ", ".join(sorted(unreachable.keys()))
            updated_results[idx] = DownstreamImpact(
                name=impact.name,
                node_type=impact.node_type,
                current_status=impact.current_status,
                predicted_status=NodeStatus.INVALID,
                impact_type=ImpactType.WILL_INVALIDATE,
                impact_reason=(
                    f"Dimension(s) no longer reachable after link change: {dim_list}"
                ),
                depth=impact.depth,
                caused_by=impact.caused_by,
                is_external=impact.is_external,
            )

    return updated_results


def _build_name_index(ctx: PropagationContext) -> dict[str, Node]:
    """Build a name → Node lookup from visited nodes."""
    return {node.name: node for node in ctx.visited_nodes_by_id.values()}


async def _batch_load_nodes_for_revalidation(
    session: AsyncSession,
    node_names: list[str],
) -> dict[str, Node]:
    """Load nodes with their parents and columns for revalidation."""
    if not node_names:  # pragma: no cover
        return {}

    nodes = await Node.get_by_names(
        session,
        node_names,
        options=[
            joinedload(Node.current).options(
                selectinload(NodeRevision.columns),
                selectinload(NodeRevision.parents).options(
                    joinedload(Node.current).options(
                        selectinload(NodeRevision.columns),
                    ),
                ),
            ),
        ],
    )
    return {n.name: n for n in nodes}


def _revalidate_single_node(
    impact: DownstreamImpact,
    loaded_nodes: dict[str, Node],
    nodes_by_name: dict[str, Node],
    session: AsyncSession,
    failed_names: set[str],
    pre_parsed_query: ast.Query | None = None,
) -> DownstreamImpact:
    """Revalidate a single downstream node using validate_node_query.

    Returns an updated DownstreamImpact with the correct predicted_status.
    """
    node = loaded_nodes.get(impact.name)
    if not node or not node.current:  # pragma: no cover
        return impact

    # Source nodes don't have queries — they're always valid if their parent is
    if node.type == NodeType.SOURCE:
        return impact

    query = node.current.query
    if not query:  # pragma: no cover
        return impact

    # Build parent_columns_map from the node's parents (excluding failed ones)
    parent_columns_map = _build_parent_columns_map(
        node,
        loaded_nodes,
        nodes_by_name,
        failed_names,
    )

    # Validate the query and resolve output columns
    validation = validate_node_query(
        query,
        parent_columns_map,
        pre_parsed=pre_parsed_query,
    )

    if validation.errors:
        # Node's query has errors — it's now INVALID
        logger.info(
            "Node %s failed revalidation: %s",
            impact.name,
            "; ".join(validation.errors),
        )
        if node.current.status != NodeStatus.INVALID:
            node.current.status = NodeStatus.INVALID
            session.add(node.current)
        return DownstreamImpact(
            name=impact.name,
            node_type=impact.node_type,
            current_status=impact.current_status,
            predicted_status=NodeStatus.INVALID,
            impact_type=ImpactType.WILL_INVALIDATE,
            impact_reason=f"Revalidation failed: {'; '.join(validation.errors)}",
            depth=impact.depth,
            caused_by=impact.caused_by,
            is_external=impact.is_external,
        )

    new_columns = validation.output_columns

    # Compare old columns with new
    old_columns = [(col.name, col.type) for col in (node.current.columns or [])]

    if not columns_signature_changed(old_columns, new_columns):
        # Columns unchanged — node is fine
        if impact.current_status == NodeStatus.INVALID and _all_parents_valid(
            node,
            loaded_nodes,
            nodes_by_name,
        ):
            # Was INVALID, parents now VALID, columns resolve → recover
            node.current.status = NodeStatus.VALID
            session.add(node.current)
            return DownstreamImpact(
                name=impact.name,
                node_type=impact.node_type,
                current_status=impact.current_status,
                predicted_status=NodeStatus.VALID,
                impact_type=ImpactType.WILL_RECOVER,
                impact_reason=f"Revalidated — upstream nodes now valid: {', '.join(impact.caused_by)}",
                depth=impact.depth,
                caused_by=impact.caused_by,
                is_external=impact.is_external,
            )
        return impact

    # Columns changed — update the node's columns
    logger.info(
        "Node %s columns changed during revalidation",
        impact.name,
    )
    _update_node_columns(node, new_columns)
    session.add(node.current)

    if impact.current_status == NodeStatus.INVALID:
        node.current.status = NodeStatus.VALID
        return DownstreamImpact(
            name=impact.name,
            node_type=impact.node_type,
            current_status=impact.current_status,
            predicted_status=NodeStatus.VALID,
            impact_type=ImpactType.WILL_RECOVER,
            impact_reason=f"Revalidated with updated columns — upstream nodes changed: {', '.join(impact.caused_by)}",
            depth=impact.depth,
            caused_by=impact.caused_by,
            is_external=impact.is_external,
        )

    return DownstreamImpact(
        name=impact.name,
        node_type=impact.node_type,
        current_status=impact.current_status,
        predicted_status=impact.current_status,
        impact_type=ImpactType.MAY_AFFECT,
        impact_reason=f"Column types changed — upstream nodes changed: {', '.join(impact.caused_by)}",
        depth=impact.depth,
        caused_by=impact.caused_by,
        is_external=impact.is_external,
    )


def _build_parent_columns_map(
    node: Node,
    loaded_nodes: dict[str, Node],
    nodes_by_name: dict[str, Node],
    failed_names: set[str],
) -> dict[str, dict[str, ColumnType]]:
    """Build the parent_columns_map for validate_node_query.

    Looks up parents from the loaded nodes first, then falls back to
    the broader visited nodes index.  Parents that failed revalidation
    are excluded so that their children fail naturally.
    """
    parent_map: dict[str, dict[str, ColumnType]] = {}
    for parent in node.current.parents or []:
        if parent.name in failed_names:
            continue
        parent_node = loaded_nodes.get(parent.name) or nodes_by_name.get(parent.name)
        if parent_node and parent_node.current and parent_node.current.columns:
            parent_map[parent.name] = {
                col.name: col.type for col in parent_node.current.columns
            }
    return parent_map


def _all_parents_valid(
    node: Node,
    loaded_nodes: dict[str, Node],
    nodes_by_name: dict[str, Node],
) -> bool:
    """Check if all of a node's parents are VALID."""
    for parent in node.current.parents or []:
        parent_node = loaded_nodes.get(parent.name) or nodes_by_name.get(parent.name)
        if not parent_node or not parent_node.current:  # pragma: no cover
            return False
        if parent_node.current.status != NodeStatus.VALID:  # pragma: no cover
            return False
    return True


def _update_node_columns(
    node: Node,
    new_columns: list[tuple[str, ColumnType]],
):
    """Update a node's column types from validate_node_query results."""
    col_type_map = {name: col_type for name, col_type in new_columns}
    for col in node.current.columns or []:
        if col.name in col_type_map:  # pragma: no branch
            col.type = col_type_map[col.name]


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def _emit_metrics(start: float, results: list[DownstreamImpact]):
    """Emit timing and count metrics."""
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
