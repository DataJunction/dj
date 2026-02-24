"""
Database loading functions
"""

from __future__ import annotations

import logging

from sqlalchemy import select, text, bindparam
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, joinedload, load_only

from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Node, NodeRevision, Column
from datajunction_server.database.preaggregation import PreAggregation
from datajunction_server.models.node_type import NodeType

from datajunction_server.construction.build_v3.dimensions import parse_dimension_ref
from datajunction_server.construction.build_v3.types import BuildContext
from datajunction_server.construction.build_v3.utils import collect_required_dimensions

logger = logging.getLogger(__name__)


async def find_upstream_node_names(
    session: AsyncSession,
    starting_node_names: list[str],
) -> tuple[set[str], dict[str, list[str]]]:
    """
    Find all upstream node names using a lightweight recursive CTE.

    This returns just node names (not full nodes) to minimize query overhead.
    Uses NodeRelationship to traverse the parent-child relationships.

    Returns:
        Tuple of:
        - Set of all upstream node names (including the starting nodes)
        - Dict mapping child_name -> list of parent_names (for metrics to find their parents)
    """
    if not starting_node_names:  # pragma: no cover
        return set(), {}

    # Lightweight recursive CTE - returns node names AND parent-child relationships
    recursive_query = text("""
        WITH RECURSIVE upstream AS (
            -- Base case: get the starting nodes' current revision IDs
            SELECT
                nr.id as revision_id,
                n.name as node_name,
                CAST(NULL AS TEXT) as child_name
            FROM node n
            JOIN noderevision nr ON n.id = nr.node_id AND n.current_version = nr.version
            WHERE n.name IN :starting_names
            AND n.deactivated_at IS NULL

            UNION

            -- Recursive case: find parents of current nodes
            SELECT
                parent_nr.id as revision_id,
                parent_n.name as node_name,
                u.node_name as child_name
            FROM upstream u
            JOIN noderelationship nrel ON u.revision_id = nrel.child_id
            JOIN node parent_n ON nrel.parent_id = parent_n.id
            JOIN noderevision parent_nr ON parent_n.id = parent_nr.node_id
                AND parent_n.current_version = parent_nr.version
            WHERE parent_n.deactivated_at IS NULL
        )
        SELECT DISTINCT node_name, child_name FROM upstream
    """).bindparams(bindparam("starting_names", expanding=True))

    result = await session.execute(
        recursive_query,
        {"starting_names": list(starting_node_names)},
    )
    rows = result.fetchall()

    # Collect all node names and parent relationships
    all_names: set[str] = set()
    # child_name -> list of parent_names
    parent_map: dict[str, list[str]] = {}

    for node_name, child_name in rows:
        all_names.add(node_name)
        if child_name:
            # node_name is the parent of child_name
            if child_name not in parent_map:
                parent_map[child_name] = []
            parent_map[child_name].append(node_name)

    return all_names, parent_map


async def find_join_paths_batch(
    session: AsyncSession,
    source_revision_ids: set[int],
    target_dimension_names: set[str],
    max_depth: int = 100,  # High limit is safe with cycle prevention; iterative deepening keeps it fast
) -> dict[tuple[int, str, str], list[int]]:
    """
    Find join paths from multiple source nodes to all target dimension nodes
    using a single recursive CTE query with early termination.

    Args:
        source_revision_ids: Set of source node revision IDs to find paths from
        target_dimension_names: Set of dimension node names to find paths to
        max_depth: Safety limit (default 100, but early termination kicks in first)

    Returns a dict mapping (source_revision_id, dimension_node_name, role_path)
    to the list of DimensionLink IDs forming the path.

    The role_path is a "->" separated string of roles at each step.
    Empty roles are represented as empty strings.

    Key optimization: Once a path reaches a target dimension, it stops exploring
    from that node (no point continuing past a target).
    """

    if not target_dimension_names or not source_revision_ids:  # pragma: no cover
        return {}

    # Track found paths: (source_rev_id, target_name, role) -> [link_ids]
    found_paths: dict[tuple[int, str, str], list[int]] = {}

    # Initialize frontier: each source node starts exploration
    # Frontier item: (source_rev_id, current_node_id, path_so_far, role_path, visited_node_ids)
    frontier: list[tuple[int, int, list[int], str, list[int]]] = []

    # Get the node_id for each source revision to start BFS
    init_query = text("""
        SELECT nr.id as rev_id, nr.node_id
        FROM noderevision nr
        WHERE nr.id IN :source_revision_ids
    """).bindparams(bindparam("source_revision_ids", expanding=True))

    init_result = await session.execute(
        init_query,
        {"source_revision_ids": list(source_revision_ids)},
    )

    for rev_id, node_id in init_result:
        frontier.append((rev_id, node_id, [], "", []))

    # BFS: explore depth by depth
    # We explore up to a practical depth limit (typically 5-10 hops is more than enough)
    practical_max_depth = min(max_depth, 10)

    for depth in range(1, practical_max_depth + 1):
        if not frontier:
            break

        # Expand frontier by one hop - BATCH query for all frontier nodes
        new_frontier: list[tuple[int, int, list[int], str, list[int]]] = []
        targets_found_at_depth = set()

        # Build a map: node_id -> list of frontier items with that node
        frontier_by_node: dict[
            int,
            list[tuple[int, int, list[int], str, list[int]]],
        ] = {}
        for item in frontier:
            node_id = item[1]
            if node_id not in frontier_by_node:
                frontier_by_node[node_id] = []
            frontier_by_node[node_id].append(item)

        # Single batched query for ALL frontier nodes at this depth
        frontier_node_ids = list(frontier_by_node.keys())

        expand_query = text("""
            SELECT
                nr.node_id,
                dl.id as link_id,
                dl.dimension_id as to_node_id,
                n.name as dim_name,
                COALESCE(dl.role, '') as link_role
            FROM noderevision nr
            JOIN dimensionlink dl ON dl.node_revision_id = nr.id
            JOIN node n ON dl.dimension_id = n.id
            WHERE nr.node_id IN :node_ids
                AND nr.version = (SELECT current_version FROM node WHERE id = nr.node_id)
        """).bindparams(bindparam("node_ids", expanding=True))

        try:
            expand_result = await session.execute(
                expand_query,
                {"node_ids": frontier_node_ids},
            )
            rows = expand_result.fetchall()
        except Exception as e:
            logger.error(
                "[BuildV3] find_join_paths_batch: Query failed at depth %d: %s",
                depth,
                str(e),
            )
            break

        # Process results: for each link found, apply to all frontier items with that source node
        for row in rows:
            from_node_id = row.node_id
            link_id = row.link_id
            to_node_id = row.to_node_id
            dim_name = row.dim_name
            link_role = row.link_role

            # Apply this link to all frontier items that came from this node
            for (
                source_rev_id,
                node_id,
                path_so_far,
                role_path,
                visited,
            ) in frontier_by_node[from_node_id]:
                # Cycle prevention: skip if we've already visited this node
                if to_node_id in visited:
                    continue

                # Build new path and role
                new_path = path_so_far + [link_id]
                new_role_path = (
                    (role_path + "->" + link_role) if role_path else link_role
                )
                new_visited = visited + [to_node_id]

                # Check if this reaches a target
                if dim_name in target_dimension_names:
                    key = (source_rev_id, dim_name, new_role_path)
                    if key not in found_paths:
                        found_paths[key] = new_path
                        targets_found_at_depth.add(dim_name)

                # Add to new frontier (explore further regardless of whether it's a target)
                # This allows finding multi-hop paths through intermediate targets
                new_frontier.append(
                    (source_rev_id, to_node_id, new_path, new_role_path, new_visited),
                )

        # Update frontier for next iteration
        frontier = new_frontier

        # Continue exploring even if we found some targets, because:
        # 1. We might need the same dimension with different roles (multi-hop paths)
        # 2. BFS naturally finds shortest paths first, so we'll get those before longer ones
        # 3. We stop when frontier is empty (no more nodes to explore)

    return found_paths


async def load_dimension_links_batch(
    session: AsyncSession,
    link_ids: set[int],
) -> dict[int, DimensionLink]:
    """
    Batch load DimensionLinks with minimal data needed for join building.
    Returns a dict mapping link_id to DimensionLink object.

    Note: Most dimension nodes should already be in ctx.nodes from query2.
    We load current+query for pre-parsing cache, and columns for type lookups.
    """
    if not link_ids:
        return {}

    # Load dimension links with eager loading for columns (needed for type lookups)
    stmt = (
        select(DimensionLink)
        .where(DimensionLink.id.in_(link_ids))
        .options(
            joinedload(DimensionLink.dimension).options(
                joinedload(Node.current).options(
                    # Load what's needed for table references, parsing, and type lookups
                    joinedload(NodeRevision.catalog),
                    joinedload(NodeRevision.availability),
                    selectinload(NodeRevision.columns).options(
                        load_only(Column.name, Column.type),
                    ),
                ),
            ),
        )
    )
    result = await session.execute(stmt)
    links = result.scalars().unique().all()

    return {link.id: link for link in links}


async def preload_join_paths(
    ctx: BuildContext,
    source_revision_ids: set[int],
    target_dimension_names: set[str],
) -> None:
    """
    Preload all join paths from multiple source nodes to target dimensions.

    Uses BFS to find paths from ALL sources to all targets, then a single
    batch load for DimensionLink objects. Results are stored in ctx.join_paths.
    """

    if not target_dimension_names or not source_revision_ids:
        logger.info("[BuildV3] preload_join_paths: Nothing to preload, returning early")
        return

    # Find all paths from all sources using recursive CTE (single query)
    logger.info("[BuildV3] preload_join_paths: Finding join paths via recursive CTE...")
    path_ids = await find_join_paths_batch(
        ctx.session,
        source_revision_ids,
        target_dimension_names,
    )
    logger.info("[BuildV3] preload_join_paths: Found %d join path(s)", len(path_ids))

    # Collect all link IDs we need to load
    logger.info("[BuildV3] preload_join_paths: Collecting link IDs...")
    all_link_ids: set[int] = set()
    for link_id_list in path_ids.values():
        all_link_ids.update(link_id_list)
    logger.info(
        "[BuildV3] preload_join_paths: Need to load %d dimension link(s)",
        len(all_link_ids),
    )

    # Batch load all DimensionLinks (single query)
    logger.info("[BuildV3] preload_join_paths: Batch loading dimension links...")
    link_dict = await load_dimension_links_batch(ctx.session, all_link_ids)
    logger.info(
        "[BuildV3] preload_join_paths: Loaded %d dimension link(s)",
        len(link_dict),
    )

    # Store in context, keyed by (source_revision_id, dim_name, role_path)
    logger.info("[BuildV3] preload_join_paths: Storing paths in context...")
    for (source_rev_id, dim_name, role_path), link_id_list in path_ids.items():
        links = [link_dict[lid] for lid in link_id_list if lid in link_dict]
        ctx.join_paths[(source_rev_id, dim_name, role_path)] = links
        # Also cache dimension nodes
        for link in links:
            if link.dimension and link.dimension.name not in ctx.nodes:
                ctx.nodes[link.dimension.name] = link.dimension

    # Log which dimensions we found paths to (for debugging)
    unique_targets = set()
    for src_id, dim_name, role_path in ctx.join_paths.keys():
        unique_targets.add(dim_name)


async def load_nodes(ctx: BuildContext) -> None:
    """
    Load all nodes needed for SQL generation

    Query 1: Find all upstream node names using lightweight recursive CTE
    Query 2: Batch load all those nodes with eager loading
    Query 3-4: Find join paths and batch load dimension links
    """
    # Collect initial node names (metrics + explicit dimension nodes)
    initial_node_names = set(ctx.metrics)

    # Parse dimension references to get target dimension node names
    logger.info("[BuildV3] load_nodes: Parsing dimension references...")
    target_dim_names: set[str] = set()
    for dim in ctx.dimensions:
        dim_ref = parse_dimension_ref(dim)
        if dim_ref.node_name:  # pragma: no branch
            initial_node_names.add(dim_ref.node_name)
            target_dim_names.add(dim_ref.node_name)
    logger.info(
        "[BuildV3] load_nodes: Initial node names: %d (metrics: %d, dimensions: %d)",
        len(initial_node_names),
        len(ctx.metrics),
        len(target_dim_names),
    )

    # Find all upstream nodes using a recursive CTE query
    logger.info("[BuildV3] load_nodes: Finding upstream nodes...")
    all_node_names, parent_map = await find_upstream_node_names(
        ctx.session,
        list(initial_node_names),
    )
    logger.info("[BuildV3] load_nodes: Found %d upstream nodes", len(all_node_names))

    # Store parent map in context for later use (e.g., get_parent_node)
    ctx.parent_map = parent_map

    # Also include the initial nodes themselves
    all_node_names.update(initial_node_names)

    logger.info(f"[BuildV3] load_nodes: Total nodes to load: {len(all_node_names)}")

    # Query 2: Batch load all nodes with appropriate eager loading
    logger.info("[BuildV3] load_nodes: Building batch load query...")
    stmt = (
        select(Node)
        .where(Node.name.in_(all_node_names))
        .where(Node.deactivated_at.is_(None))
        .options(
            load_only(
                Node.name,
                Node.type,
                Node.current_version,
            ),
            joinedload(Node.current).options(
                load_only(
                    NodeRevision.name,
                    NodeRevision.query,
                    NodeRevision.schema_,
                    NodeRevision.table,
                ),
                selectinload(NodeRevision.columns).options(
                    load_only(
                        Column.name,
                        Column.type,
                    ),
                ),
                joinedload(NodeRevision.catalog),
                selectinload(NodeRevision.required_dimensions).options(
                    # Load the node_revision and node to reconstruct full dimension path
                    joinedload(Column.node_revision).options(
                        joinedload(NodeRevision.node),
                    ),
                ),
                joinedload(NodeRevision.availability),  # For materialization support
                selectinload(NodeRevision.dimension_links).options(
                    # Load dimension node for link matching in temporal filters
                    joinedload(DimensionLink.dimension),
                ),
            ),
        )
    )

    logger.info("[BuildV3] load_nodes: Executing batch load query...")
    result = await ctx.session.execute(stmt)
    logger.info("[BuildV3] load_nodes: Processing query results...")
    nodes = result.scalars().unique().all()
    logger.info("[BuildV3] load_nodes: Loaded %d nodes from database", len(nodes))

    # Cache all loaded nodes
    logger.info("[BuildV3] load_nodes: Caching loaded nodes...")
    for node in nodes:
        ctx.nodes[node.name] = node
    logger.info("[BuildV3] load_nodes: Cached %d nodes", len(ctx.nodes))

    # Collect required dimensions from metrics and add to context
    # Required dimensions are stored as Column objects, so they don't have role info.
    # We need to check if a user-requested dimension already covers the same (node, column).
    logger.info("[BuildV3] load_nodes: Collecting required dimensions...")
    required_dims = collect_required_dimensions(ctx.nodes, ctx.metrics)
    logger.info(
        "[BuildV3] load_nodes: Found %d required dimensions",
        len(required_dims),
    )
    for req_dim in required_dims:
        dim_ref = parse_dimension_ref(req_dim)
        if dim_ref.node_name:  # pragma: no branch
            target_dim_names.add(dim_ref.node_name)

            # Check if any existing dimension already covers this (node, column)
            # This handles cases like: user requests v3.date.week[order],
            # required dim is v3.date.week (no role) - they're the same column
            is_covered = False
            for existing_dim in ctx.dimensions:
                existing_ref = parse_dimension_ref(existing_dim)
                if (
                    existing_ref.node_name == dim_ref.node_name
                    and existing_ref.column_name == dim_ref.column_name
                ):
                    is_covered = True
                    logger.debug(
                        f"[BuildV3] Required dimension {req_dim} already covered by {existing_dim}",
                    )
                    break

            if not is_covered:
                logger.info(
                    f"[BuildV3] Auto-adding required dimension {req_dim} from metric",
                )
                ctx.dimensions.append(req_dim)

    # Collect parent revision IDs for join path lookup (using parent_map from Query 1)
    # For derived metrics, we need to recursively find fact parents through the metric chain
    parent_revision_ids: set[int] = set()

    def collect_fact_parents(metric_name: str, visited: set[str]) -> None:
        """Recursively collect fact/transform parent revision IDs from metrics."""
        if metric_name in visited:  # pragma: no cover
            return
        visited.add(metric_name)

        metric_node = ctx.nodes.get(metric_name)
        if not metric_node:  # pragma: no cover
            return

        parent_names = ctx.parent_map.get(metric_name, [])
        for parent_name in parent_names:
            parent_node = ctx.nodes.get(parent_name)
            if not parent_node:  # pragma: no cover
                continue

            if parent_node.type == NodeType.METRIC:
                # Parent is another metric - recurse to find its fact parents
                collect_fact_parents(parent_name, visited)
            elif parent_node.current:  # pragma: no branch
                # Parent is a fact/transform - collect its revision ID
                parent_revision_ids.add(parent_node.current.id)

    logger.info("[BuildV3] load_nodes: Collecting fact parents...")
    for metric_name in ctx.metrics:
        collect_fact_parents(metric_name, set())
    logger.info(
        "[BuildV3] load_nodes: Collected %d parent revision IDs",
        len(parent_revision_ids),
    )

    # Log which nodes these revision IDs correspond to for debugging
    parent_nodes_info = []
    for rev_id in parent_revision_ids:
        for node in ctx.nodes.values():
            if node.current and node.current.id == rev_id:
                parent_nodes_info.append(f"{node.name} (rev={rev_id})")
                break
    if parent_nodes_info:
        logger.info(
            "[BuildV3] load_nodes: Parent nodes to search from: %s",
            ", ".join(parent_nodes_info),
        )

    # Preload join paths for ALL parent nodes in a single batch
    logger.info(
        "[BuildV3] load_nodes: Preloading join paths (%d parents, %d target dimensions)...",
        len(parent_revision_ids),
        len(target_dim_names),
    )
    await preload_join_paths(ctx, parent_revision_ids, target_dim_names)
    logger.info("[BuildV3] load_nodes: Join paths preloaded")

    # Store parent_revision_ids for pre-agg loading (if needed)
    ctx._parent_revision_ids = parent_revision_ids


async def load_available_preaggs(ctx: BuildContext) -> None:
    """
    Load pre-aggregations that could satisfy the current query.

    Queries for pre-aggs that:
    1. Have availability state (materialized)
    2. Match the parent nodes of requested metrics

    Results are indexed by node_revision_id for fast lookup during grain group
    processing. The grain and measure matching is done at query time since
    we need to compare against the specific grain group requirements.

    This function is only called when ctx.use_materialized=True.
    """
    if not ctx.use_materialized:
        return

    # Get parent revision IDs from the load_nodes step
    parent_revision_ids: set[int] = ctx._parent_revision_ids
    if not parent_revision_ids:
        return

    # Query for available pre-aggs with their availability state
    stmt = (
        select(PreAggregation)
        .options(joinedload(PreAggregation.availability))
        .where(
            PreAggregation.node_revision_id.in_(parent_revision_ids),
            PreAggregation.availability_id.isnot(None),  # Has availability
        )
    )
    result = await ctx.session.execute(stmt)
    preaggs = result.scalars().unique().all()

    # Index by node_revision_id for fast lookup
    for preagg in preaggs:
        if preagg.availability and preagg.availability.is_available():
            if preagg.node_revision_id not in ctx.available_preaggs:
                ctx.available_preaggs[preagg.node_revision_id] = []
            ctx.available_preaggs[preagg.node_revision_id].append(preagg)

    logger.debug(
        f"[BuildV3] Loaded {sum(len(v) for v in ctx.available_preaggs.values())} "
        f"available pre-aggs for {len(ctx.available_preaggs)} parent nodes",
    )
