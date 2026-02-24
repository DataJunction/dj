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
    logger.info(
        "[BuildV3] find_join_paths_batch: Starting recursive CTE query (max_depth=%d)...",
        max_depth,
    )
    logger.info(
        "[BuildV3] find_join_paths_batch: Searching for paths FROM revision_ids=%s TO dimensions=%s",
        list(source_revision_ids),
        list(target_dimension_names),
    )

    if not target_dimension_names or not source_revision_ids:  # pragma: no cover
        logger.info("[BuildV3] find_join_paths_batch: Empty input, returning early")
        return {}

    # Breadth-first search with true early termination
    # Tries each depth level incrementally and stops at the first level where we find paths
    # This prevents exploring deeper paths when shorter ones exist
    recursive_query = text("""
        WITH RECURSIVE paths AS (
            -- Base case: first level dimension links from any source node
            SELECT
                dl.node_revision_id as source_rev_id,
                dl.id as link_id,
                n.name as dim_name,
                CAST(dl.id AS TEXT) as path,
                COALESCE(dl.role, '') as role_path,
                1 as depth,
                CASE WHEN n.name IN :target_names THEN 1 ELSE 0 END as is_target,
                ARRAY[dl.dimension_id] as visited_nodes
            FROM dimensionlink dl
            JOIN node n ON dl.dimension_id = n.id
            WHERE dl.node_revision_id IN :source_revision_ids

            UNION

            -- Recursive case: only explore if we haven't found ALL targets yet at shallower depth
            -- This is true breadth-first: explore all paths at depth N before moving to N+1
            SELECT
                paths.source_rev_id,
                dl2.id as link_id,
                n2.name as dim_name,
                paths.path || ',' || CAST(dl2.id AS TEXT) as path,
                paths.role_path || '->' || COALESCE(dl2.role, '') as role_path,
                paths.depth + 1 as depth,
                CASE WHEN n2.name IN :target_names THEN 1 ELSE 0 END as is_target,
                paths.visited_nodes || dl2.dimension_id
            FROM paths
            JOIN node prev_node ON paths.dim_name = prev_node.name
            JOIN noderevision nr ON prev_node.current_version = nr.version AND nr.node_id = prev_node.id
            JOIN dimensionlink dl2 ON dl2.node_revision_id = nr.id
            JOIN node n2 ON dl2.dimension_id = n2.id
            WHERE paths.depth < :max_depth
              AND NOT (dl2.dimension_id = ANY(paths.visited_nodes))  -- Cycle prevention
              -- Allow exploring through targets to reach other targets (multi-hop paths)
              -- Cycle prevention handles infinite loops
        ),
        -- Find the minimum depth where we found each (source, target, role) combination
        min_depths AS (
            SELECT source_rev_id, dim_name, role_path, MIN(depth) as min_depth
            FROM paths
            WHERE is_target = 1
            GROUP BY source_rev_id, dim_name, role_path
        )
        -- Only return paths at their minimum depth (shortest paths)
        SELECT p.source_rev_id, p.dim_name, p.path, p.role_path, p.depth
        FROM paths p
        JOIN min_depths md ON
            p.source_rev_id = md.source_rev_id
            AND p.dim_name = md.dim_name
            AND p.role_path = md.role_path
            AND p.depth = md.min_depth
        WHERE p.is_target = 1
        ORDER BY p.depth ASC
    """).bindparams(
        bindparam("source_revision_ids", expanding=True),
        bindparam("target_names", expanding=True),
    )

    # First, let's check what dimension links actually exist from these sources
    # This helps debug why we might not find expected paths
    debug_query = text("""
        SELECT
            n.name as source_node,
            nr.id as source_rev_id,
            dim.name as target_dim,
            dl.role,
            dl.join_type
        FROM dimensionlink dl
        JOIN noderevision nr ON dl.node_revision_id = nr.id
        JOIN node n ON nr.node_id = n.id
        JOIN node dim ON dl.dimension_id = dim.id
        WHERE nr.id IN :source_revision_ids
        ORDER BY n.name, dim.name
    """).bindparams(bindparam("source_revision_ids", expanding=True))

    debug_result = await session.execute(
        debug_query,
        {"source_revision_ids": list(source_revision_ids)},
    )
    debug_rows = debug_result.fetchall()

    if debug_rows:
        logger.info(
            "[BuildV3] find_join_paths_batch: Found %d direct dimension link(s) from source nodes:",
            len(debug_rows),
        )
        for source_node, source_rev_id, target_dim, role, join_type in debug_rows:
            logger.info(
                "  - %s (rev=%d) -> %s [role=%s, type=%s]",
                source_node,
                source_rev_id,
                target_dim,
                role or "null",
                join_type,
            )
    else:
        logger.warning(
            "[BuildV3] find_join_paths_batch: No direct dimension links found from source revision IDs: %s",
            list(source_revision_ids),
        )

    # Iterative deepening: try shallow depths first (fast), go deeper only if needed
    # Most paths are 1-3 hops, so this finds them quickly without exploring deep
    paths: dict[tuple[int, str, str], list[int]] = {}

    # Try depths incrementally: 1, 2, 3, 5, 10, 20, 30
    # Increasing gaps since deeper searches are exponentially slower
    depth_sequence = [1, 2, 3, 5, 10, 20, 30]

    for current_max_depth in depth_sequence:
        if current_max_depth > max_depth:
            break

        logger.info(
            "[BuildV3] find_join_paths_batch: Trying depth %d (found %d paths so far)...",
            current_max_depth,
            len(paths),
        )

        # Set a statement timeout for each iteration (5 seconds per depth level)
        await session.execute(text("SET LOCAL statement_timeout = '5s'"))

        try:
            result = await session.execute(
                recursive_query,
                {
                    "source_revision_ids": list(source_revision_ids),
                    "max_depth": current_max_depth,
                    "target_names": list(target_dimension_names),
                },
            )
        except Exception as e:
            logger.warning(
                "[BuildV3] find_join_paths_batch: Depth %d timed out or failed: %s",
                current_max_depth,
                str(e),
            )
            # Reset timeout and try next depth
            await session.execute(text("SET LOCAL statement_timeout = DEFAULT"))
            continue
        logger.info(
            "[BuildV3] find_join_paths_batch: Query executed, fetching results...",
        )
        rows = result.fetchall()
        logger.info(
            "[BuildV3] find_join_paths_batch: Got %d row(s) at depth %d",
            len(rows),
            current_max_depth,
        )

        # Log which dimensions were found at this depth
        dims_at_depth = set()
        for source_rev_id, dim_name, path_str, role_path, depth in rows:
            dims_at_depth.add(dim_name)
        if dims_at_depth:
            logger.info(
                "[BuildV3] find_join_paths_batch: Dimensions found at depth %d: %s",
                current_max_depth,
                sorted(dims_at_depth),
            )

        # Build paths dict for this depth level
        new_paths_found = 0
        for source_rev_id, dim_name, path_str, role_path, depth in rows:
            key = (source_rev_id, dim_name, role_path or "")
            if key not in paths:  # Only add if we haven't found this path yet
                paths[key] = [int(x) for x in path_str.split(",")]
                new_paths_found += 1

        logger.info(
            "[BuildV3] find_join_paths_batch: Added %d new path(s) at depth %d",
            new_paths_found,
            current_max_depth,
        )

        # Early exit: if we didn't find any new paths at this depth, likely won't find any deeper
        # This is more reliable than counting total paths (which can vary with roles)
        if new_paths_found == 0 and len(paths) > 0:
            logger.info(
                "[BuildV3] find_join_paths_batch: No new paths at depth %d, stopping search",
                current_max_depth,
            )
            break

    logger.info(
        "[BuildV3] find_join_paths_batch: COMPLETE - found %d path(s) total",
        len(paths),
    )
    return paths


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

    Uses a single recursive CTE query to find paths from ALL sources at once,
    then a single batch load for DimensionLink objects. Results are stored
    in ctx.join_paths.

    This is O(2) queries regardless of how many source nodes we have.
    """
    logger.info(
        "[BuildV3] preload_join_paths: Starting with %d source revisions, %d target dimensions",
        len(source_revision_ids),
        len(target_dimension_names),
    )

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
    logger.info(
        "[BuildV3] preload_join_paths: Found paths to %d unique dimension(s): %s",
        len(unique_targets),
        sorted(unique_targets),
    )

    logger.info(
        "[BuildV3] preload_join_paths: COMPLETE - Preloaded %d join paths for %d sources",
        len(path_ids),
        len(source_revision_ids),
    )


async def load_nodes(ctx: BuildContext) -> None:
    """
    Load all nodes needed for SQL generation

    Query 1: Find all upstream node names using lightweight recursive CTE
    Query 2: Batch load all those nodes with eager loading
    Query 3-4: Find join paths and batch load dimension links
    """
    logger.info("[BuildV3] load_nodes: Starting...")
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

    logger.info("[BuildV3] load_nodes: COMPLETE - loaded %d nodes", len(ctx.nodes))


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
