"""
DAG related functions.
"""

import functools
import itertools
import logging
import time
from collections import namedtuple
from typing import Dict, List, Union, cast

from sqlalchemy import and_, func, join, or_, select, text, bindparam
from sqlalchemy.sql.base import ExecutableOption
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy.sql.operators import is_
from sqlalchemy.dialects.postgresql import array

from datajunction_server.database.attributetype import ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import (
    CubeRelationship,
    Node,
    NodeRelationship,
    NodeRevision,
)
from datajunction_server.errors import DJGraphCycleException
from datajunction_server.models.attribute import ColumnAttributes
from datajunction_server.models.node import DimensionAttributeOutput
from datajunction_server.models.node_type import NodeType, NodeNameVersion
from datajunction_server.utils import SEPARATOR, get_settings, refresh_if_needed

logger = logging.getLogger(__name__)
settings = get_settings()


def _dag_timed(operation: str):
    """
    Local timing decorator for DAG traversal functions.

    Uses a lazy import of ``get_metrics_provider`` to avoid the circular
    dependency that arises when ``dag.py`` imports ``instrumentation.provider``
    at module level (database → models → dag creates a cycle).
    """

    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            _start = time.monotonic()
            try:
                return await fn(*args, **kwargs)
            finally:
                from datajunction_server.instrumentation.provider import (  # noqa: PLC0415
                    get_metrics_provider,
                )

                get_metrics_provider().timer(
                    "dj.dag.traversal_ms",
                    (time.monotonic() - _start) * 1000,
                    {"operation": operation},
                )

        return wrapper

    return decorator


# State during BFS traversal of the dimensions DAG in _get_dimensions_dag_bfs.
# node_id:    ID of the current Node (not NodeRevision)
# node_name:  name of the current node (used to build edge labels)
# path_edges: list of edge labels like "A.b_id" or "A.role_name" accumulated so far
# role_path:  accumulated "role_a->role_b" string for the name suffix (empty if no roles)
# visited:    frozenset of node_ids already seen (cycle prevention)
DimBFSState = namedtuple(
    "DimBFSState",
    ["node_id", "node_name", "path_edges", "role_path", "visited"],
)


def _node_output_options():
    """
    Statement options to retrieve all NodeOutput objects in one query
    """
    return [
        selectinload(Node.current).options(
            selectinload(NodeRevision.columns).options(
                selectinload(Column.attributes).joinedload(
                    ColumnAttribute.attribute_type,
                ),
                selectinload(Column.dimension),
                selectinload(Column.partition),
            ),
            selectinload(NodeRevision.catalog),
            selectinload(NodeRevision.parents),
            selectinload(NodeRevision.dimension_links).options(
                selectinload(DimensionLink.dimension).options(
                    selectinload(Node.current),
                ),
            ),
        ),
        selectinload(Node.tags),
        selectinload(Node.owners),
    ]


@_dag_timed("get_downstream_nodes")
async def get_downstream_nodes(
    session: AsyncSession,
    node_name: str,
    node_type: NodeType = None,
    include_deactivated: bool = True,
    include_cubes: bool = True,
    depth: int = -1,
    options: list[ExecutableOption] = None,
) -> list[Node]:
    """
    Gets all downstream children of the given node, filterable by node type.

    Uses layered BFS over the noderelationship table — one raw SQL query per depth level —
    collecting node IDs cheaply, then loading all matched nodes in a single batched query
    with the caller-supplied options. This avoids the Postgres recursive CTE materialization
    overhead on wide/deep graphs and the per-node ORM queries of the old BFS fallback.
    """
    result_options = options if options is not None else _node_output_options()

    node = await Node.get_by_name(
        session,
        node_name,
        options=[joinedload(Node.current)],
    )
    if not node:
        return []

    visited: set[int] = {node.id}
    frontier: list[int] = [node.id]
    current_depth = 0

    deactivated_filter = (
        "AND n.deactivated_at IS NULL" if not include_deactivated else ""
    )
    cube_filter = "AND nr.type != 'CUBE'::nodetype" if not include_cubes else ""

    while frontier and (depth == -1 or current_depth < depth):
        rows = await session.execute(
            text(f"""
                SELECT DISTINCT n.id
                FROM noderelationship rel
                JOIN noderevision nr ON rel.child_id = nr.id
                JOIN node n ON n.id = nr.node_id
                    AND n.current_version = nr.version
                WHERE rel.parent_id IN :parent_ids
                  {deactivated_filter}
                  {cube_filter}
            """).bindparams(bindparam("parent_ids", expanding=True)),
            {"parent_ids": frontier},
        )
        child_ids = [r[0] for r in rows if r[0] not in visited]
        if not child_ids:
            break
        visited.update(child_ids)
        frontier = child_ids
        current_depth += 1

        if len(visited) >= settings.node_list_max:
            logger.warning(
                "get_downstream_nodes: hit node_list_max (%d) at depth %d for %s",
                settings.node_list_max,
                current_depth,
                node_name,
            )
            break

    all_downstream_ids = visited - {node.id}
    if not all_downstream_ids:
        return []

    results = (
        (
            await session.execute(
                select(Node)
                .where(Node.id.in_(all_downstream_ids))
                .options(*result_options),
            )
        )
        .unique()
        .scalars()
        .all()
    )

    filtered = [
        n
        for n in results
        if (node_type is None or n.type == node_type)
        and (include_deactivated or n.deactivated_at is None)
    ]
    return filtered[: settings.node_list_max]


@_dag_timed("get_upstream_nodes")
async def get_upstream_nodes(
    session: AsyncSession,
    node_name: Union[str, List[str]],
    node_type: NodeType = None,
    include_deactivated: bool = True,
    options: List = None,
) -> List[Node]:
    """
    Gets all upstreams of the given node(s), filterable by node type.

    Uses layered BFS over noderelationship — one raw SQL query per depth level —
    collecting parent node IDs cheaply, then loading all matched nodes in a single
    batched query with the caller-supplied options.
    """
    node_names = [node_name] if isinstance(node_name, str) else node_name
    result_options = options if options is not None else _node_output_options()

    nodes = await Node.get_by_names(
        session,
        node_names,
        options=[joinedload(Node.current)],
    )
    if not nodes:
        return []  # pragma: no cover

    starting_node_ids = {n.id for n in nodes}
    visited: set[int] = set(starting_node_ids)
    frontier: list[int] = list(starting_node_ids)

    deactivated_filter = (
        "AND parent_n.deactivated_at IS NULL" if not include_deactivated else ""
    )

    while frontier:  # pragma: no branch
        rows = await session.execute(
            text(f"""
                SELECT DISTINCT parent_n.id
                FROM node child_n
                JOIN noderevision child_nr
                    ON child_n.id = child_nr.node_id
                    AND child_n.current_version = child_nr.version
                JOIN noderelationship rel ON rel.child_id = child_nr.id
                JOIN node parent_n ON parent_n.id = rel.parent_id
                WHERE child_n.id IN :frontier_ids
                {deactivated_filter}
            """).bindparams(bindparam("frontier_ids", expanding=True)),
            {"frontier_ids": frontier},
        )
        parent_ids = [r[0] for r in rows if r[0] not in visited]
        if not parent_ids:
            break
        visited.update(parent_ids)
        frontier = parent_ids

    all_upstream_ids = visited - starting_node_ids
    if not all_upstream_ids:
        return []

    results = (
        (
            await session.execute(
                select(Node)
                .where(Node.id.in_(all_upstream_ids))
                .order_by(Node.name)
                .options(*result_options),
            )
        )
        .unique()
        .scalars()
        .all()
    )

    return [n for n in results if node_type is None or n.type == node_type]


async def build_reference_link(
    session: AsyncSession,
    col: Column,
    path: list[str],
    role: list[str] | None = None,
) -> DimensionAttributeOutput | None:
    """
    Builds a reference link dimension attribute output for a column.
    """
    if not (col.dimension_id and col.dimension_column):
        return None  # pragma: no cover
    await session.refresh(col, ["dimension"])
    await session.refresh(col.dimension, ["current"])
    await session.refresh(col.dimension.current, ["columns"])

    dim_cols = col.dimension.current.columns
    if dim_col := next(
        (dc for dc in dim_cols if dc.name == col.dimension_column),
        None,
    ):
        return DimensionAttributeOutput(
            name=f"{col.dimension.name}.{col.dimension_column}"
            + (f"[{'->'.join(role)}]" if role else ""),
            node_name=col.dimension.name,
            node_display_name=col.dimension.current.display_name,
            properties=dim_col.attribute_names(),
            type=str(col.type),
            path=path,
        )
    return None  # pragma: no cover


async def get_dimension_attributes(
    session: AsyncSession,
    node_name: str,
    include_deactivated: bool = True,
):
    """
    Get all dimension attributes for a given node.
    """
    node = cast(
        Node,
        await Node.get_by_name(
            session,
            node_name,
            options=[joinedload(Node.current)],
        ),
    )
    if node.type == NodeType.METRIC:
        # For metrics, check if it's a base metric (parent is non-metric) or
        # derived metric (parent is another metric)
        await refresh_if_needed(session, node.current, ["parents"])

        # Find metric parents (not dimension parents)
        metric_parents = [p for p in node.current.parents if p.type == NodeType.METRIC]
        non_metric_parents = [
            p
            for p in node.current.parents
            if p.type not in (NodeType.METRIC, NodeType.DIMENSION)
        ]

        if metric_parents:
            # Derived metric - use get_dimensions which handles intersection
            dimensions = cast(
                list[DimensionAttributeOutput],
                await get_dimensions(session, node, with_attributes=True),
            )
            # Prepend the metric's name to each dimension's path
            for dim in dimensions:
                dim.path = [node_name] + dim.path
            return dimensions
        elif non_metric_parents:
            # Base metric - use the first non-metric parent (fact/transform)
            await refresh_if_needed(session, non_metric_parents[0], ["current"])
            node = non_metric_parents[0]
        else:
            # No valid parents found
            return []  # pragma: no cover

    # Discover all dimension nodes in the given node's dimensions graph
    dimension_nodes_and_paths = await get_dimension_nodes(
        session,
        node,
        include_deactivated,
    )
    dimensions_map = {dim.id: dim for dim, _, _ in dimension_nodes_and_paths}

    # Add all reference links to the list of dimension attributes
    reference_links = []
    await refresh_if_needed(session, node.current, ["columns"])
    for col in node.current.columns:
        await refresh_if_needed(session, col, ["dimension_id", "dimension_column"])
        if col.dimension_id and col.dimension_column:
            await session.refresh(col, ["dimension"])
            if ref_link := await build_reference_link(  # pragma: no cover
                session,
                col,
                path=[f"{node.name}.{col.name}"],
            ):
                reference_links.append(ref_link)
    for dimension_node, path, role in dimension_nodes_and_paths:
        await refresh_if_needed(session, dimension_node.current, ["columns"])
        for col in dimension_node.current.columns:
            await refresh_if_needed(session, col, ["dimension_id", "dimension_column"])
            if col.dimension_id and col.dimension_column:
                join_path = (
                    [node.name] if dimension_node.name != node.name else []
                ) + [dimensions_map[int(node_id)].name for node_id in path]
                if ref_link := await build_reference_link(  # pragma: no cover
                    session,
                    col,
                    join_path,
                    role,
                ):
                    reference_links.append(ref_link)

    # Build all dimension attributes from the dimension nodes in the graph
    graph_dimensions = [
        DimensionAttributeOutput(
            name=f"{dim.name}.{col.name}" + (f"[{'->'.join(role)}]" if role else ""),
            node_name=dim.name,
            node_display_name=dim.current.display_name,
            properties=col.attribute_names(),
            type=str(col.type),
            path=[node_name] + [dimensions_map[int(node_id)].name for node_id in path],
        )
        for dim, path, role in dimension_nodes_and_paths
        for col in dim.current.columns
    ]

    # Build all local dimension attributes from the original node
    local_dimensions = [
        DimensionAttributeOutput(
            name=f"{node.name}.{col.name}",
            node_name=node.name,
            node_display_name=node.current.display_name,
            properties=col.attribute_names(),
            type=str(col.type),
            path=[],
        )
        for col in node.current.columns
    ]
    local_dimensions = [
        dim
        for dim in local_dimensions
        if "primary_key" in (dim.properties or [])
        or "dimension" in (dim.properties or [])
        or node.type == NodeType.DIMENSION
    ]
    return reference_links + graph_dimensions + local_dimensions


async def get_dimension_nodes(
    session: AsyncSession,
    node: Node,
    include_deactivated: bool = True,
) -> list[tuple[Node, list[str], list[str] | None]]:
    """
    Discovers all dimension nodes in the given node's dimensions graph using a recursive
    CTE query to build out the dimension links.
    """
    dag = (
        (
            select(
                DimensionLink.node_revision_id,
                NodeRevision.id,
                NodeRevision.node_id,
                array([NodeRevision.node_id]).label("join_path"),  # start path
                array([DimensionLink.role]).label("role"),
            )
            .where(DimensionLink.node_revision_id == node.current.id)
            .join(Node, DimensionLink.dimension_id == Node.id)
            .join(
                NodeRevision,
                (Node.id == NodeRevision.node_id)
                & (Node.current_version == NodeRevision.version),
            )
        )
        .cte("dimensions", recursive=True)
        .suffix_with(
            "CYCLE node_id SET is_cycle USING path",
        )
    )

    paths = dag.union_all(
        select(
            dag.c.node_revision_id,
            NodeRevision.id,
            NodeRevision.node_id,
            func.array_cat(dag.c.join_path, array([NodeRevision.node_id])).label(
                "join_path",
            ),
            func.array_cat(dag.c.role, array([DimensionLink.role])).label("role"),
        )
        .join(DimensionLink, dag.c.id == DimensionLink.node_revision_id)
        .join(Node, DimensionLink.dimension_id == Node.id)
        .join(
            NodeRevision,
            (Node.id == NodeRevision.node_id)
            & (Node.current_version == NodeRevision.version),
        ),
    )

    node_selector = select(Node, paths.c.join_path, paths.c.role)
    if not include_deactivated:
        node_selector = node_selector.where(  # pragma: no cover
            is_(Node.deactivated_at, None),
        )
    statement = (
        node_selector.join(paths, paths.c.node_id == Node.id)
        .join(
            NodeRevision,
            (Node.current_version == NodeRevision.version)
            & (Node.id == NodeRevision.node_id),
        )
        .options(*_node_output_options())
    )
    return [
        (node, path, [r for r in role if r])
        for node, path, role in (await session.execute(statement)).all()
    ]


@_dag_timed("get_dimensions_dag")
async def get_dimensions_dag(
    session: AsyncSession,
    node_revision: NodeRevision,
    with_attributes: bool = True,
    depth: int = 30,
) -> List[Union[DimensionAttributeOutput, Node]]:
    """
    Gets the dimensions graph of the given node revision using layered BFS raw SQL queries.
    This graph is split out into dimension attributes or dimension nodes depending on the
    `with_attributes` flag.

    Replaces the former recursive CTE approach with an iterative BFS that issues one
    batched query per graph depth, avoiding Postgres recursive-CTE materialization overhead
    on wide/deep graphs.
    """
    return await _get_dimensions_dag_bfs(
        session,
        node_revision,
        with_attributes,
        depth,
    )


async def _get_dimensions_dag_bfs(
    session: AsyncSession,
    node_revision: NodeRevision,
    with_attributes: bool = True,
    depth: int = 30,
) -> List[Union[DimensionAttributeOutput, Node]]:
    """
    BFS implementation of get_dimensions_dag.

    Phase A: seed the frontier from the starting node revision.
    Phase B: expand one layer per iteration using raw SQL.
             - Depth 0: follows BOTH DimensionLink edges AND Column→dimension edges.
             - Depth 1+: follows DimensionLink edges only (same as old CTE).
    Phase C: batch-fetch column info for all discovered dimension nodes.
    Phase D: assemble DimensionAttributeOutput objects.

    The ``path`` on each output mirrors the old CTE: each element is an edge label
    ``"from_node_name"`` (DimensionLink, no role), ``"from_node_name.role"``
    (DimensionLink with role), or ``"from_node_name.col_name"`` (Column→dim edge).
    """
    is_sqlite = session.bind.dialect.name == "sqlite"

    # -------------------------------------------------------------------------
    # Phase A – seed frontier
    # -------------------------------------------------------------------------
    # Maps (node_id, role_path) -> (node_name, path_edges)
    # Using (node_id, role_path) as key so that the same node reached via different
    # role paths produces separate DimensionAttributeOutput entries (matching old CTE).
    discovered: dict[tuple[int, str], tuple[str, list[str]]] = {}

    frontier: list[DimBFSState] = [
        DimBFSState(
            node_id=node_revision.node_id,
            node_name=node_revision.name,
            path_edges=[],
            role_path="",
            visited=frozenset([node_revision.node_id]),
        ),
    ]

    # -------------------------------------------------------------------------
    # Phase B – BFS expansion
    # -------------------------------------------------------------------------
    for current_depth in range(depth + 1):
        if not frontier:
            break

        # Group frontier items by node_id so we issue one query per layer
        frontier_by_node: dict[int, list[DimBFSState]] = {}
        for item in frontier:
            frontier_by_node.setdefault(item.node_id, []).append(item)

        frontier_node_ids = list(frontier_by_node.keys())

        if current_depth == 0:
            # Depth 0: expand via BOTH DimensionLink edges AND Column→dimension edges.
            # col_name is NULL for DimensionLink rows, non-NULL for Column→dim rows.
            expand_query = text("""
                SELECT from_node_id, to_node_id, to_node_name, link_role, col_name FROM (
                    SELECT
                        nr.node_id AS from_node_id,
                        n.id       AS to_node_id,
                        n.name     AS to_node_name,
                        COALESCE(dl.role, '') AS link_role,
                        NULL       AS col_name
                    FROM noderevision nr
                    JOIN dimensionlink dl ON dl.node_revision_id = nr.id
                    JOIN node n ON dl.dimension_id = n.id AND n.deactivated_at IS NULL
                    WHERE nr.id = :node_revision_id
                    UNION ALL
                    SELECT
                        nr2.node_id AS from_node_id,
                        n2.id       AS to_node_id,
                        n2.name     AS to_node_name,
                        ''          AS link_role,
                        c.name      AS col_name
                    FROM noderevision nr2
                    JOIN "column" c ON c.node_revision_id = nr2.id
                    JOIN node n2 ON c.dimension_id = n2.id AND n2.deactivated_at IS NULL
                    WHERE nr2.id = :node_revision_id
                      AND c.dimension_id IS NOT NULL
                ) edges
            """)
            rows_result = await session.execute(
                expand_query,
                {"node_revision_id": node_revision.id},
            )
        else:
            # Depth 1+: expand via DimensionLink only (col_name always NULL)
            expand_query = text("""
                SELECT
                    nr.node_id   AS from_node_id,
                    n.id         AS to_node_id,
                    n.name       AS to_node_name,
                    COALESCE(dl.role, '') AS link_role,
                    NULL         AS col_name
                FROM noderevision nr
                JOIN dimensionlink dl ON dl.node_revision_id = nr.id
                JOIN node n ON dl.dimension_id = n.id AND n.deactivated_at IS NULL
                WHERE nr.node_id IN :node_ids
                  AND nr.version = (
                      SELECT current_version FROM node WHERE id = nr.node_id
                  )
            """).bindparams(bindparam("node_ids", expanding=True))
            rows_result = await session.execute(
                expand_query,
                {"node_ids": frontier_node_ids},
            )

        rows = rows_result.fetchall()

        new_frontier: list[DimBFSState] = []
        for row in rows:
            for item in frontier_by_node.get(row.from_node_id, []):
                if row.to_node_id in item.visited:
                    continue  # cycle prevention

                # Build role_path (same logic as find_join_paths_batch)
                link_role = row.link_role or ""
                if not item.role_path and not link_role:
                    new_role_path = ""
                elif not item.role_path:
                    new_role_path = link_role
                elif not link_role:
                    new_role_path = item.role_path
                else:
                    new_role_path = item.role_path + "->" + link_role

                # Build edge label matching the old CTE's join_path format:
                #   - Column→dim: "from_node.col_name"
                #   - DimensionLink with role: "from_node.role_name"
                #   - DimensionLink without role: "from_node"
                col_name = row.col_name
                if col_name is not None:
                    edge_label = f"{item.node_name}.{col_name}"
                elif link_role:
                    edge_label = f"{item.node_name}.{link_role}"
                else:
                    edge_label = item.node_name

                new_path_edges = item.path_edges + [edge_label]
                new_visited = item.visited | frozenset([row.to_node_id])
                disc_key = (row.to_node_id, new_role_path)

                # Record path for this (node, role_path) combination.
                # Skip if already recorded to avoid re-exploring identical states.
                if disc_key not in discovered:
                    discovered[disc_key] = (row.to_node_name, new_path_edges)
                    new_frontier.append(
                        DimBFSState(
                            node_id=row.to_node_id,
                            node_name=row.to_node_name,
                            path_edges=new_path_edges,
                            role_path=new_role_path,
                            visited=new_visited,
                        ),
                    )

        frontier = new_frontier

    if not discovered:
        # No dimension nodes reachable — still need to handle local dimension attributes
        # on the starting node itself (e.g., when the node IS a dimension node).
        if not with_attributes:
            return []
        # Fall through to Phase C/D with empty discovered set; local columns handled below.

    # -------------------------------------------------------------------------
    # If with_attributes=False, return dimension Node objects via ORM
    # -------------------------------------------------------------------------
    if not with_attributes:
        discovered_names = list({name for (name, _) in discovered.values()})
        result = await session.execute(
            select(Node)
            .where(Node.name.in_(discovered_names))
            .options(*_node_output_options()),
        )
        return result.unique().scalars().all()

    # -------------------------------------------------------------------------
    # Phase C – batch fetch columns for discovered dimension nodes
    # -------------------------------------------------------------------------
    agg_fn = "group_concat(at.name, ',')" if is_sqlite else "string_agg(at.name, ',')"

    dim_columns: dict[str, list[tuple[str, str, str | None]]] = {}
    # Maps node_name -> list of (col_name, col_type, attribute_types_csv)
    dim_display_names: dict[str, str] = {}

    discovered_names = list({name for (name, _) in discovered.values()})
    if discovered_names:
        dim_col_query = text(f"""
            SELECT
                n.name          AS node_name,
                nr.display_name AS node_display_name,
                c.name          AS col_name,
                CAST(c.type AS TEXT) AS col_type,
                {agg_fn}        AS attribute_types
            FROM node n
            JOIN noderevision nr
                ON nr.node_id = n.id AND nr.version = n.current_version
            JOIN "column" c ON c.node_revision_id = nr.id
            LEFT JOIN columnattribute ca ON ca.column_id = c.id
            LEFT JOIN attributetype at ON at.id = ca.attribute_type_id
            WHERE n.name IN :dim_names
            GROUP BY n.name, nr.display_name, c.name, c.type
        """).bindparams(bindparam("dim_names", expanding=True))

        dim_col_result = await session.execute(
            dim_col_query,
            {"dim_names": discovered_names},
        )
        for row in dim_col_result.fetchall():
            dim_columns.setdefault(row.node_name, []).append(
                (row.col_name, row.col_type, row.attribute_types),
            )
            dim_display_names[row.node_name] = row.node_display_name

    # Also fetch local columns on the starting node revision (for local dimension attrs)
    local_col_query = text(f"""
        SELECT
            nr.name         AS node_name,
            nr.display_name AS node_display_name,
            c.name          AS col_name,
            CAST(c.type AS TEXT) AS col_type,
            {agg_fn}        AS attribute_types
        FROM noderevision nr
        JOIN "column" c ON c.node_revision_id = nr.id
        LEFT JOIN columnattribute ca ON ca.column_id = c.id
        LEFT JOIN attributetype at ON at.id = ca.attribute_type_id
        WHERE nr.id = :node_revision_id
        GROUP BY nr.name, nr.display_name, c.name, c.type
    """)
    local_col_result = await session.execute(
        local_col_query,
        {"node_revision_id": node_revision.id},
    )
    local_node_display_name = node_revision.name
    local_columns: list[tuple[str, str, str | None]] = []
    for row in local_col_result.fetchall():
        local_node_display_name = row.node_display_name
        local_columns.append((row.col_name, row.col_type, row.attribute_types))

    # -------------------------------------------------------------------------
    # Phase D – build DimensionAttributeOutput objects
    # -------------------------------------------------------------------------
    results: list[DimensionAttributeOutput] = []

    # Columns from discovered dimension nodes
    for (_, role_path), (node_name, path_edges) in discovered.items():
        role_suffix = f"[{role_path}]" if role_path else ""
        node_display_name = dim_display_names.get(node_name, node_name)
        for col_name, col_type, attribute_types in dim_columns.get(node_name, []):
            attr_list = attribute_types.split(",") if attribute_types else []
            results.append(
                DimensionAttributeOutput(
                    name=f"{node_name}.{col_name}{role_suffix}",
                    node_name=node_name,
                    node_display_name=node_display_name,
                    properties=attr_list,
                    type=col_type,
                    path=list(path_edges),
                ),
            )

    # Local columns on starting node: only include if tagged dimension or primary_key,
    # or if the starting node itself is a dimension
    for col_name, col_type, attribute_types in local_columns:
        attr_list = attribute_types.split(",") if attribute_types else []
        is_dim_attr = (
            ColumnAttributes.DIMENSION.value in attr_list
            or ColumnAttributes.PRIMARY_KEY.value in attr_list
        )
        if is_dim_attr or node_revision.type == NodeType.DIMENSION:
            results.append(
                DimensionAttributeOutput(
                    name=f"{node_revision.name}.{col_name}",
                    node_name=node_revision.name,
                    node_display_name=local_node_display_name,
                    properties=attr_list,
                    type=col_type,
                    path=[],
                ),
            )

    return sorted(results, key=lambda x: (x.name, ",".join(x.path)))


async def get_dimensions(
    session: AsyncSession,
    node: Node,
    with_attributes: bool = True,
    depth: int = 30,
) -> List[Union[DimensionAttributeOutput, Node]]:
    """
    Return all available dimensions for a given node.
    * Setting `attributes` to True will return a list of dimension attributes,
    * Setting `attributes` to False will return a list of dimension nodes

    For metrics, returns the intersection of dimensions available from
    all non-metric parents.
    """
    if node.type != NodeType.METRIC:
        node_revision = (
            (
                await session.execute(
                    select(NodeRevision).where(
                        NodeRevision.node_id == node.id,
                        NodeRevision.version == node.current_version,
                    ),
                )
            )
            .scalars()
            .first()
        )
        return await get_dimensions_dag(
            session,
            node_revision,
            with_attributes,
            depth=depth,
        )

    # For metrics, get ultimate non-metric parents
    # This handles both base metrics (returns immediate parents) and
    # derived metrics (returns base metrics' parents)
    ultimate_parents = await get_metric_parents(session, [node])

    if not ultimate_parents:
        return []  # pragma: no cover

    # Get dimensions for all ultimate parents
    all_dimensions: List[List[DimensionAttributeOutput]] = []
    for parent in ultimate_parents:
        node_revision = (
            (
                await session.execute(
                    select(NodeRevision).where(
                        NodeRevision.node_id == parent.id,
                        NodeRevision.version == parent.current_version,
                    ),
                )
            )
            .scalars()
            .first()
        )
        parent_dims = await get_dimensions_dag(
            session,
            node_revision,
            with_attributes,
            depth=depth,
        )
        all_dimensions.append(parent_dims)

    if not all_dimensions:
        return []  # pragma: no cover

    if len(all_dimensions) == 1:
        return all_dimensions[0]

    # Find intersection by dimension name
    common_names = set(d.name for d in all_dimensions[0])
    for dims in all_dimensions[1:]:
        common_names &= set(d.name for d in dims)

    # Return dimensions from first parent that are in the intersection
    return sorted(
        [d for d in all_dimensions[0] if d.name in common_names],
        key=lambda x: (x.name, ",".join(x.path)),
    )


async def get_filter_only_dimensions(
    session: AsyncSession,
    node_name: str,
):
    """
    Get dimensions for this node that can only be filtered by and cannot be grouped by
    or retrieved as a part of the node's SELECT clause.
    """
    filter_only_dimensions = []
    upstreams = await get_upstream_nodes(session, node_name, node_type=NodeType.SOURCE)
    for upstream in upstreams:
        await session.refresh(upstream.current, ["dimension_links"])
        for link in upstream.current.dimension_links:
            await session.refresh(link.dimension, ["current"])
            await session.refresh(link.dimension.current)
            column_mapping = {col.name: col for col in link.dimension.current.columns}
            filter_only_dimensions.extend(
                [
                    DimensionAttributeOutput(
                        name=dim,
                        node_name=link.dimension.name,
                        node_display_name=link.dimension.current.display_name,
                        type=str(column_mapping[dim.split(SEPARATOR)[-1]].type),
                        path=[upstream.name],
                        filter_only=True,
                        properties=column_mapping[
                            dim.split(SEPARATOR)[-1]
                        ].attribute_names(),
                    )
                    for dim in link.foreign_keys.values()
                ],
            )
    return filter_only_dimensions


async def group_dimensions_by_name(
    session: AsyncSession,
    node: Node,
) -> Dict[str, List[DimensionAttributeOutput]]:
    """
    Group the dimensions for the node by the dimension attribute name
    """
    return {
        k: list(v)
        for k, v in itertools.groupby(
            await get_dimensions(session, node),
            key=lambda dim: dim.name,
        )
    }


async def get_shared_dimensions(
    session: AsyncSession,
    metric_nodes: List[Node],
) -> List[DimensionAttributeOutput]:
    """
    Return a list of dimensions that are common between the metric nodes.

    For each individual metric:
    - If it has multiple parents (e.g., derived metric referencing multiple base metrics),
      returns the union of dimensions from all its parents.

    Across multiple metrics:
    - Returns the intersection of dimensions available for each metric.

    This allows derived metrics to use dimensions from any of their base metrics,
    while still ensuring compatibility when querying multiple metrics together.
    """
    if not metric_nodes:
        return []

    # Get the per-metric parent mapping (batched for efficiency)
    metric_to_parents = await get_metric_parents_map(session, metric_nodes)

    # Collect all unique parent nodes across all metrics
    unique_parents: Dict[int, Node] = {}
    for parents in metric_to_parents.values():
        for parent in parents:
            if parent.id not in unique_parents:
                unique_parents[parent.id] = parent

    # Compute dimensions once per unique parent
    parent_dims_cache: Dict[int, Dict[str, List[DimensionAttributeOutput]]] = {}
    for parent_id, parent in unique_parents.items():
        parent_dims = await group_dimensions_by_name(session, parent)
        parent_dims_cache[parent_id] = parent_dims

    # Map cached results back to each metric
    per_metric_dimensions: List[Dict[str, List[DimensionAttributeOutput]]] = []
    for metric_node in metric_nodes:
        parents = metric_to_parents.get(metric_node.name, [])
        if not parents:
            continue  # pragma: no cover

        # Compute union of dimensions from all parents (using cached results)
        dims_by_name: Dict[str, List[DimensionAttributeOutput]] = {}
        for parent in parents:
            parent_dims = parent_dims_cache[parent.id]
            for dim_name, dim_list in parent_dims.items():
                if dim_name not in dims_by_name:
                    dims_by_name[dim_name] = dim_list
                # If already present, keep existing (they should be equivalent)

        per_metric_dimensions.append(dims_by_name)

    if not per_metric_dimensions:
        return []  # pragma: no cover

    if len(per_metric_dimensions) == 1:
        # Single metric - return all its dimensions
        return sorted(
            [dim for dims in per_metric_dimensions[0].values() for dim in dims],
            key=lambda x: (x.name, x.path),
        )

    # Multiple metrics - find intersection across metrics
    common_names = set(per_metric_dimensions[0].keys())
    for dims_by_name in per_metric_dimensions[1:]:
        common_names &= set(dims_by_name.keys())

    if not common_names:
        return []

    # Return dimensions from first metric that are in the intersection
    return sorted(
        [
            dim
            for name, dims in per_metric_dimensions[0].items()
            if name in common_names
            for dim in dims
        ],
        key=lambda x: (x.name, x.path),
    )


async def get_metric_parents_map(
    session: AsyncSession,
    metric_nodes: list[Node],
) -> Dict[str, List[Node]]:
    """
    Return a mapping from metric name to its non-metric parent nodes.

    For derived metrics (metrics that reference base metrics), returns the
    non-metric parents of those base metrics.

    This batched version maintains the relationship between metrics and their
    parents, which is needed to compute per-metric dimension unions.

    Supports nested derived metrics - will recursively resolve until reaching
    non-metric parents (facts/transforms).
    """
    if not metric_nodes:
        return {}

    metric_names = {m.name for m in metric_nodes}
    result: Dict[str, List[Node]] = {name: [] for name in metric_names}

    # Get all immediate parents for the input metrics WITH the child metric name
    find_latest_node_revisions = [
        and_(
            NodeRevision.name == metric_node.name,
            NodeRevision.version == metric_node.current_version,
        )
        for metric_node in metric_nodes
    ]
    statement = (
        select(NodeRevision.name.label("metric_name"), Node)
        .where(or_(*find_latest_node_revisions))
        .select_from(
            join(
                join(
                    NodeRevision,
                    NodeRelationship,
                    NodeRevision.id == NodeRelationship.child_id,
                ),
                Node,
                NodeRelationship.parent_id == Node.id,
            ),
        )
    )
    rows = (await session.execute(statement)).all()

    # Build mapping and track metric parents that need further resolution
    # Maps: parent_metric_name -> [original_metric_names that need this parent resolved]
    metric_parents_to_resolve: Dict[str, List[str]] = {}

    # Group parents by metric name first
    parents_by_metric: Dict[str, List[Node]] = {}
    for metric_name, parent_node in rows:
        if metric_name not in parents_by_metric:
            parents_by_metric[metric_name] = []
        parents_by_metric[metric_name].append(parent_node)

    # Process each metric's parents
    for metric_name, parents in parents_by_metric.items():
        metric_parents = [p for p in parents if p.type == NodeType.METRIC]
        dimension_parents = [p for p in parents if p.type == NodeType.DIMENSION]
        other_parents = [
            p for p in parents if p.type not in (NodeType.METRIC, NodeType.DIMENSION)
        ]

        # Add metric parents to resolve list
        for parent_node in metric_parents:
            if parent_node.name not in metric_parents_to_resolve:
                metric_parents_to_resolve[parent_node.name] = []
            metric_parents_to_resolve[parent_node.name].append(metric_name)

        # Add fact/transform parents directly
        for parent_node in other_parents:
            result[metric_name].append(parent_node)

        # Only add dimension parents if there are no other parents
        # (i.e., the metric is defined directly on a dimension node)
        if not metric_parents and not other_parents:
            for parent_node in dimension_parents:
                result[metric_name].append(parent_node)

    # Recursively resolve metric parents until we reach non-metric nodes
    visited_metrics: set[str] = set()

    while metric_parents_to_resolve:
        base_metric_names = [
            name
            for name in metric_parents_to_resolve.keys()
            if name not in visited_metrics
        ]

        if not base_metric_names:
            break  # pragma: no cover

        for name in base_metric_names:
            visited_metrics.add(name)

        # Get the base metrics' current versions
        base_metrics_stmt = (
            select(Node)
            .where(Node.name.in_(base_metric_names))
            .where(is_(Node.deactivated_at, None))
        )
        base_metrics = list((await session.execute(base_metrics_stmt)).scalars().all())

        if not base_metrics:
            break  # pragma: no cover

        find_base_metric_revisions = [
            and_(
                NodeRevision.name == m.name,
                NodeRevision.version == m.current_version,
            )
            for m in base_metrics
        ]
        statement = (
            select(NodeRevision.name.label("base_metric_name"), Node)
            .where(or_(*find_base_metric_revisions))
            .select_from(
                join(
                    join(
                        NodeRevision,
                        NodeRelationship,
                        NodeRevision.id == NodeRelationship.child_id,
                    ),
                    Node,
                    NodeRelationship.parent_id == Node.id,
                ),
            )
        )
        base_rows = (await session.execute(statement)).all()

        # Group parents by base metric name first
        base_parents_by_metric: Dict[str, List[Node]] = {}
        for base_metric_name, parent_node in base_rows:
            if base_metric_name not in base_parents_by_metric:
                base_parents_by_metric[base_metric_name] = []
            base_parents_by_metric[base_metric_name].append(parent_node)

        # Process results and track new metric parents for next iteration
        next_metric_parents_to_resolve: Dict[str, List[str]] = {}

        for base_metric_name, parents in base_parents_by_metric.items():
            original_metrics = metric_parents_to_resolve.get(base_metric_name, [])

            metric_parents = [p for p in parents if p.type == NodeType.METRIC]
            dimension_parents = [p for p in parents if p.type == NodeType.DIMENSION]
            other_parents = [
                p
                for p in parents
                if p.type not in (NodeType.METRIC, NodeType.DIMENSION)
            ]

            # Add metric parents to next resolve list (multi-level derived metrics)
            for parent_node in metric_parents:  # pragma: no cover
                if parent_node.name not in next_metric_parents_to_resolve:
                    next_metric_parents_to_resolve[parent_node.name] = []
                next_metric_parents_to_resolve[parent_node.name].extend(
                    original_metrics,
                )

            # Add fact/transform parents to results
            for parent_node in other_parents:
                for derived_metric_name in original_metrics:
                    result[derived_metric_name].append(parent_node)

            # Only add dimension parents if there are no other parents
            # (i.e., the metric is defined directly on a dimension node)
            if not metric_parents and not other_parents:  # pragma: no cover
                for parent_node in dimension_parents:
                    for derived_metric_name in original_metrics:
                        result[derived_metric_name].append(parent_node)

        metric_parents_to_resolve = next_metric_parents_to_resolve

    # Deduplicate parents for each metric
    return {name: list(set(parents)) for name, parents in result.items()}


async def get_metric_parents(
    session: AsyncSession,
    metric_nodes: list[Node],
) -> list[Node]:
    """
    Return a flat list of non-metric parent nodes of the metrics.

    For derived metrics (metrics that reference base metrics), returns the
    non-metric parents of those base metrics.

    Note: Only 1 level of metric nesting is supported. Derived metrics can
    reference base metrics, but not other derived metrics.
    """
    metric_to_parents = await get_metric_parents_map(session, metric_nodes)
    all_parents = []
    for parents in metric_to_parents.values():
        all_parents.extend(parents)
    return list(set(all_parents))


async def get_common_dimensions(session: AsyncSession, nodes: list[Node]):
    """
    Return a list of dimensions that are common between the nodes.
    """
    metric_nodes = [node for node in nodes if node.type == NodeType.METRIC]
    other_nodes = [node for node in nodes if node.type != NodeType.METRIC]
    if metric_nodes:  # pragma: no branch
        nodes = list(set(other_nodes + await get_metric_parents(session, metric_nodes)))

    common = await group_dimensions_by_name(session, nodes[0])
    for node in nodes[1:]:
        node_dimensions = await group_dimensions_by_name(session, node)

        # Merge each set of dimensions based on the name and path
        to_delete = set(common.keys() - node_dimensions.keys())
        common_dim_keys = common.keys() & list(node_dimensions.keys())
        if not common_dim_keys:
            return []
        for dim_key in to_delete:  # pragma: no cover
            del common[dim_key]  # pragma: no cover
    return sorted(
        [y for x in common.values() for y in x],
        key=lambda x: (x.name, x.path),
    )


async def get_nodes_with_common_dimensions(
    session: AsyncSession,
    common_dimensions: list[Node],
    node_types: list[NodeType] | None = None,
) -> list[NodeNameVersion]:
    """
    Find all nodes that share a list of common dimensions.

    This traverses the dimension graph recursively to find:
    1. Nodes directly linked to any dimension in the graph that leads to the target dimensions
    2. Metrics that inherit those dimensions from their parent nodes

    For example, if dim A -> dim B -> dim C, searching for dim C will find nodes
    linked to dim C, dim B, or dim A (since they all lead to dim C).

    Args:
        common_dimensions: List of dimension nodes to find common nodes for
        node_types: Optional list of node types to filter by

    Returns:
        List of NodeNameVersion objects containing node name and version
    """
    if not common_dimensions:
        return []

    dimension_ids = [d.id for d in common_dimensions]
    num_dimensions = len(dimension_ids)

    # Build a CTE that merges column -> dimension and dimension link -> dimension relationships
    # These are the "branches" of the dimensions graph
    graph_branches = (
        select(
            Column.node_revision_id.label("node_revision_id"),
            Column.dimension_id.label("dimension_id"),
        )
        .where(Column.dimension_id.isnot(None))
        .union_all(
            select(
                DimensionLink.node_revision_id.label("node_revision_id"),
                DimensionLink.dimension_id.label("dimension_id"),
            ),
        )
    ).cte("graph_branches")

    # Recursive CTE: find all dimensions that lead to each target dimension
    # Base case: the target dimensions themselves
    dimension_graph = (
        select(
            Node.id.label("target_dim_id"),  # The original target dimension
            Node.id.label("reachable_dim_id"),  # Dimensions that can reach the target
        )
        .where(Node.id.in_(dimension_ids))
        .where(Node.deactivated_at.is_(None))
    ).cte("dimension_graph", recursive=True)

    # Add cycle detection for PostgreSQL
    dimension_graph = dimension_graph.suffix_with(
        "CYCLE reachable_dim_id SET is_cycle USING path",
    )

    # Recursive case: find dimensions that link to dimensions already in our set
    dimension_graph = dimension_graph.union_all(
        select(
            dimension_graph.c.target_dim_id,
            Node.id.label("reachable_dim_id"),
        )
        .select_from(graph_branches)
        .join(
            NodeRevision,
            graph_branches.c.node_revision_id == NodeRevision.id,
        )
        .join(
            Node,
            (NodeRevision.node_id == Node.id)
            & (Node.current_version == NodeRevision.version),
        )
        .join(
            dimension_graph,
            graph_branches.c.dimension_id == dimension_graph.c.reachable_dim_id,
        )
        .where(Node.type == NodeType.DIMENSION)
        .where(Node.deactivated_at.is_(None)),
    )

    # Find all node revisions that link to any dimension in the expanded graph
    nodes_linked_to_expanded_dims = (
        select(
            graph_branches.c.node_revision_id,
            dimension_graph.c.target_dim_id,
        )
        .select_from(graph_branches)
        .join(
            dimension_graph,
            graph_branches.c.dimension_id == dimension_graph.c.reachable_dim_id,
        )
    ).subquery()

    # Find node_revisions linked to all target dimensions
    nodes_with_all_dims = (
        select(nodes_linked_to_expanded_dims.c.node_revision_id)
        .group_by(nodes_linked_to_expanded_dims.c.node_revision_id)
        .having(
            func.count(func.distinct(nodes_linked_to_expanded_dims.c.target_dim_id))
            >= num_dimensions,
        )
    ).subquery()

    # Find parent node IDs that have all dimensions
    parents_with_all_dims = (
        select(Node.id.label("parent_node_id"))
        .select_from(nodes_with_all_dims)
        .join(
            NodeRevision,
            nodes_with_all_dims.c.node_revision_id == NodeRevision.id,
        )
        .join(
            Node,
            (NodeRevision.node_id == Node.id)
            & (Node.current_version == NodeRevision.version),
        )
        .where(Node.deactivated_at.is_(None))
    ).subquery()

    # Find metrics whose parents have all dimensions
    metrics_via_parents = (
        select(NodeRevision.id.label("node_revision_id"))
        .select_from(NodeRelationship)
        .join(
            NodeRevision,
            NodeRelationship.child_id == NodeRevision.id,
        )
        .join(
            Node,
            (NodeRevision.node_id == Node.id)
            & (Node.current_version == NodeRevision.version),
        )
        .join(
            parents_with_all_dims,
            NodeRelationship.parent_id == parents_with_all_dims.c.parent_node_id,
        )
        .where(NodeRevision.type == NodeType.METRIC)
        .where(Node.deactivated_at.is_(None))
    )

    # Combine: nodes directly linked + metrics via parents
    all_matching_nodes = (
        select(nodes_with_all_dims.c.node_revision_id)
        .union(metrics_via_parents)
        .subquery()
    )

    # Final query to get only node name and version (lightweight)
    statement = (
        select(Node.name, Node.current_version)
        .select_from(all_matching_nodes)
        .join(
            NodeRevision,
            all_matching_nodes.c.node_revision_id == NodeRevision.id,
        )
        .join(
            Node,
            (NodeRevision.node_id == Node.id)
            & (Node.current_version == NodeRevision.version),
        )
        .where(Node.deactivated_at.is_(None))
    )

    if node_types:
        statement = statement.where(NodeRevision.type.in_(node_types))

    results = await session.execute(statement)
    return [NodeNameVersion(name=row[0], version=row[1]) for row in results.all()]


def topological_sort(nodes: List[Node]) -> List[Node]:
    """
    Sort a list of nodes into topological order so that the nodes with the most dependencies
    are later in the list, and the nodes with the fewest dependencies are earlier.
    """
    all_nodes = {node.name: node for node in nodes}

    # Build adjacency list and calculate in-degrees
    adjacency_list: Dict[str, List[Node]] = {}
    in_degrees: Dict[str, int] = {}
    for node in nodes:
        adjacency_list[node.name] = [
            parent for parent in node.current.parents if parent.name in all_nodes
        ]
        in_degrees[node.name] = 0

    # Build reverse mapping: parent -> children, and set in-degrees
    children_to_parents = adjacency_list.copy()  # Save for in-degree calc
    adjacency_list = {}  # Reset to build parent->children mapping

    for node in nodes:
        adjacency_list[node.name] = []
        in_degrees[node.name] = len(children_to_parents[node.name])

    # Populate parent->children mapping
    for node_name, parents in children_to_parents.items():
        for parent in parents:
            adjacency_list[parent.name].append(all_nodes[node_name])

    # Initialize queue with nodes having in-degree 0
    queue: List[Node] = [
        all_nodes[name] for name, degree in in_degrees.items() if degree == 0
    ]

    # Perform topological sort using Kahn's algorithm
    sorted_nodes: List[Node] = []
    while queue:
        current_node = queue.pop(0)
        sorted_nodes.append(current_node)
        for child in adjacency_list.get(current_node.name, []):
            in_degrees[child.name] -= 1
            if in_degrees[child.name] == 0:
                queue.append(child)

    # Check for cycles
    if len(sorted_nodes) != len(in_degrees):
        raise DJGraphCycleException("Graph has at least one cycle")

    return sorted_nodes


async def get_dimension_dag_indegree(session, node_names: List[str]) -> Dict[str, int]:
    """
    For a given node, calculate the indegrees for its dimensions graph by finding the number
    of dimension links that reference this node. Non-dimension nodes will always have an
    indegree of 0.
    """
    nodes = await Node.get_by_names(session, node_names)
    dimension_ids = [node.id for node in nodes]
    statement = (
        select(
            DimensionLink.dimension_id,
            func.count(DimensionLink.id),
        )
        .where(DimensionLink.dimension_id.in_(dimension_ids))
        .join(NodeRevision, DimensionLink.node_revision_id == NodeRevision.id)
        .join(
            Node,
            and_(
                Node.id == NodeRevision.node_id,
                Node.current_version == NodeRevision.version,
            ),
        )
        .group_by(DimensionLink.dimension_id)
    )
    result = await session.execute(statement)
    link_counts = {link[0]: link[1] for link in result.unique().all()}
    dimension_dag_indegree = {node.name: link_counts.get(node.id, 0) for node in nodes}
    return dimension_dag_indegree


async def get_cubes_using_dimensions(
    session: AsyncSession,
    dimension_names: list[str],
) -> dict[str, int]:
    """
    Find cube revisions that use all the specified dimension node
    """
    cubes_subquery = (
        select(NodeRevision.id, Node.name)
        .select_from(Node)
        .join(
            NodeRevision,
            and_(
                NodeRevision.node_id == Node.id,
                Node.current_version == NodeRevision.version,
            ),
        )
        .where(
            Node.type == NodeType.CUBE,
            Node.deactivated_at.is_(None),
        )
        .subquery()
    )

    dimensions_subquery = (
        select(NodeRevision.id, Node.name)
        .select_from(Node)
        .join(
            NodeRevision,
            and_(
                NodeRevision.node_id == Node.id,
                Node.current_version == NodeRevision.version,
            ),
        )
        .where(
            Node.type == NodeType.DIMENSION,
            Node.deactivated_at.is_(None),
        )
        .subquery()
    )

    find_statement = (
        select(
            dimensions_subquery.c.name,
            func.count(func.distinct(CubeRelationship.cube_id)).label(
                "cubes_using_dim",
            ),
        )
        .select_from(cubes_subquery)
        .join(CubeRelationship, cubes_subquery.c.id == CubeRelationship.cube_id)
        .join(Column, CubeRelationship.cube_element_id == Column.id)
        .join(dimensions_subquery, Column.node_revision_id == dimensions_subquery.c.id)
        .where(dimensions_subquery.c.name.in_(dimension_names))
        .group_by(dimensions_subquery.c.name)
    )
    result = await session.execute(find_statement)
    return {res[0]: res[1] for res in result.fetchall()}
