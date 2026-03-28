"""
Impact preview — compute the blast radius of proposed node changes.

Entry point:
- ``compute_impact``: given a dict of {node_name: NodeChange}, BFS downstream and return
  a topo-sorted list of ImpactedNode objects.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import NamedTuple

from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload, selectinload
from sqlalchemy.sql.expression import bindparam, text

from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJException
from datajunction_server.internal.validation import validate_node_data
from datajunction_server.models.deployment import (
    ColumnSpec,
    DimensionJoinLinkSpec,
    LinkableNodeSpec,
    NodeSpec,
)
from datajunction_server.models.impact import (
    ColumnChange,
    ColumnChangeType,
)
from datajunction_server.models.node import NodeRevisionBase
from datajunction_server.models.impact_preview import ImpactedNode
from datajunction_server.models.node import NodeStatus
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import (
    _node_output_options,
    get_dimension_inbound_bfs,
    get_downstream_nodes,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.types import PRIMITIVE_TYPES

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lightweight column proxy for proposed state (avoids mutating ORM objects)
# ---------------------------------------------------------------------------


class _ProposedColumn(NamedTuple):
    """Minimal column-like object accepted by ast.CompileContext.column_overrides."""

    name: str
    type: object  # ColumnType instance


def _build_proposed_columns(
    current_columns: list,
    columns_removed: set[str],
    columns_changed: list[tuple[str, str, str]],
) -> list[_ProposedColumn]:
    """Return the proposed column list after applying removals and type changes.

    ``columns_changed`` is a list of ``(name, old_type, new_type)`` triples.
    The new type is parsed via ``PRIMITIVE_TYPES``; unrecognised type strings
    leave the column's current type unchanged (safe fallback).
    """
    type_overrides: dict[str, object] = {}
    for col_name, _old, new_type_str in columns_changed:
        parsed = PRIMITIVE_TYPES.get(new_type_str.lower().strip())
        if parsed is not None:
            type_overrides[col_name] = parsed

    result: list[_ProposedColumn] = []
    for col in current_columns:
        if col.name in columns_removed:
            continue
        new_type = type_overrides.get(col.name, col.type)
        result.append(_ProposedColumn(name=col.name, type=new_type))
    return result


# ---------------------------------------------------------------------------
# Lightweight reference checks (kept for dim-link and fallback cases)
# ---------------------------------------------------------------------------


def references_changed_columns(node: Node, removed_cols: set[str]) -> bool:
    """Return True if any of the node's output columns overlap with removed_cols."""
    node_col_names = {c.name for c in node.current.columns}
    return bool(node_col_names & removed_cols)


def references_removed_dim(node: Node, removed_dim_names: set[str]) -> bool:
    """Return True if a metric has a removed dim as a required dimension.

    A metric is only affected if the removed dimension is in its
    ``required_dimensions`` — i.e. the dim is mandatory for the metric to be
    queried at all.  Losing a dim link doesn't invalidate a metric by itself;
    it just removes a slicing option, which is a cube-level concern handled
    separately by the cube element checks.
    """
    if node.type == NodeType.METRIC:
        return any(
            dim.name in removed_dim_names for dim in node.current.required_dimensions
        )

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


async def _check_immediate_metric_cube_impacts(
    session: AsyncSession,
    changed_node: Node,
    dim_links_removed: set[str],
    caused_by: list[str],
    impacted: dict[str, ImpactedNode],
    node_cache: dict[str, Node],
) -> None:
    """Check cubes of immediate downstream metrics for dim link removal impacts.

    Walk one hop from ``changed_node`` to find metric children, then one more
    hop to find each metric's cubes.  Any cube whose ``cube_elements`` contain
    an element sourced from one of the ``dim_links_removed`` nodes is flagged
    WILL_INVALIDATE — the cube declared that dimension attribute, but the link
    that exposed it has now been removed.

    Path: changed_node → immediate metric children → their cubes → cube elements.
    """
    _logger.info(
        "[metric-cube-impact] checking cubes of immediate metrics downstream of %r "
        "for dim_links_removed=%r",
        changed_node.name,
        dim_links_removed,
    )

    # Step 1: immediate children of changed_node
    children_by_parent = await _batch_get_children(
        session,
        [changed_node.id],
        list(_node_output_options()),
    )
    immediate_metrics = [
        c
        for c in children_by_parent.get(changed_node.id, [])
        if c.type == NodeType.METRIC
    ]
    if not immediate_metrics:
        _logger.info(
            "[metric-cube-impact] no immediate metric children of %r",
            changed_node.name,
        )
        return

    _logger.info(
        "[metric-cube-impact] immediate metric children of %r: %r",
        changed_node.name,
        [m.name for m in immediate_metrics],
    )

    # Step 2: cubes of those metrics (loaded with cube_elements)
    metric_ids = [m.id for m in immediate_metrics]
    cubes_by_metric_id = await _batch_get_children(
        session,
        metric_ids,
        list(_cube_dim_output_options()),
    )

    reason = f"Dimension links removed: {', '.join(sorted(dim_links_removed))}"
    seen_cubes: set[str] = set()

    for metric in immediate_metrics:
        cubes = cubes_by_metric_id.get(metric.id, [])
        for cube in cubes:
            if cube.type != NodeType.CUBE:
                continue
            if cube.name in seen_cubes:
                continue
            seen_cubes.add(cube.name)
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
                "[metric-cube-impact] cube %r (via metric %r) elements (name, source): %r",
                cube.name,
                metric.name,
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
                        "[metric-cube-impact] HIT: cube %r element %r comes from "
                        "removed dim %r (via metric %r)",
                        cube.name,
                        element.name,
                        source_node_name,
                        metric.name,
                    )
                    _record_impact(
                        impacted,
                        cube,
                        impact_type="dimension_link",
                        caused_by=caused_by,
                        reason=reason,
                    )
                    break


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
    # (name, old_type, new_type) triples for type-changed columns
    columns_changed: list[tuple[str, str, str]] = field(default_factory=list)
    dim_links_removed: set[str] = field(default_factory=set)
    is_deleted: bool = False
    # Node still exists but its SQL is now broken (e.g. upstream column removed).
    # Propagated like deletion — all noderelationship children are unconditionally
    # marked impacted, bypassing column_overrides which cannot intercept
    # metric-on-metric or other value-level references.
    is_invalid: bool = False
    caused_by: list[str] = field(default_factory=list)
    # Proposed column state for the node that owns this change entry.
    # When set, downstream validation uses this list as the upstream column state
    # instead of loading from DB — giving accurate impact detection without a DB write.
    proposed_columns: list | None = None  # _ProposedColumn or Column objects


# ---------------------------------------------------------------------------
# Helpers for computing column / dim-link diffs from (NodeSpec, Node) pairs
# ---------------------------------------------------------------------------


def _detect_source_column_changes(
    existing_node: Node,
    new_spec: "NodeSpec",
) -> list[ColumnChange]:
    """Detect column changes for SOURCE nodes (explicit YAML column types)."""
    changes: list[ColumnChange] = []
    if not existing_node.current:
        return changes

    existing_columns = {col.name: col for col in existing_node.current.columns}
    spec_columns: dict[str, ColumnSpec] = {}
    if isinstance(new_spec, LinkableNodeSpec) and new_spec.columns:
        spec_columns = {col.name: col for col in new_spec.columns}

    if not spec_columns:
        return changes

    for col_name in existing_columns.keys() - spec_columns.keys():
        changes.append(
            ColumnChange(
                column=col_name,
                change_type=ColumnChangeType.REMOVED,
                old_type=str(existing_columns[col_name].type),
            ),
        )
    for col_name in spec_columns.keys() - existing_columns.keys():
        changes.append(
            ColumnChange(
                column=col_name,
                change_type=ColumnChangeType.ADDED,
                new_type=spec_columns[col_name].type,
            ),
        )
    for col_name in existing_columns.keys() & spec_columns.keys():
        existing_col = existing_columns[col_name]
        spec_col = spec_columns[col_name]
        old_type = str(existing_col.type)
        new_type = spec_col.type
        if new_type and _normalize_type(old_type) != _normalize_type(new_type):
            changes.append(
                ColumnChange(
                    column=col_name,
                    change_type=ColumnChangeType.TYPE_CHANGED,
                    old_type=old_type,
                    new_type=new_type,
                ),
            )
        # Partition change: compare type/granularity/format between spec and DB
        existing_partition = existing_col.partition
        spec_partition = spec_col.partition
        existing_p = (
            (
                existing_partition.type_.value if existing_partition.type_ else None,
                existing_partition.granularity.value
                if existing_partition.granularity
                else None,
                existing_partition.format,
            )
            if existing_partition
            else None
        )
        spec_p = (
            (
                spec_partition.type.value if spec_partition.type else None,
                spec_partition.granularity.value
                if spec_partition.granularity
                else None,
                spec_partition.format,
            )
            if spec_partition
            else None
        )
        if existing_p != spec_p:
            changes.append(
                ColumnChange(
                    column=col_name,
                    change_type=ColumnChangeType.PARTITION_CHANGED,
                ),
            )
    return changes


def _detect_dim_link_removals(
    existing_node: Node,
    new_spec: LinkableNodeSpec,
) -> list[str]:
    """Return dimension node names whose links exist in the DB but are absent from new_spec."""
    if not existing_node.current:
        return []
    existing_dim_names: set[str] = {
        dl.dimension.name for dl in existing_node.current.dimension_links
    }
    spec_dim_names: set[str] = {
        link.rendered_dimension_node
        for link in new_spec.dimension_links
        if isinstance(link, DimensionJoinLinkSpec)
    }
    return sorted(existing_dim_names - spec_dim_names)


# Type aliases for column type normalisation (used by _detect_source_column_changes)
_TYPE_ALIASES: dict[str, str] = {
    "long": "bigint",
    "int64": "bigint",
    "integer": "int",
    "int32": "int",
    "short": "smallint",
    "int16": "smallint",
    "byte": "tinyint",
    "int8": "tinyint",
    "double": "double",
    "float64": "double",
    "float": "float",
    "float32": "float",
    "real": "float",
    "string": "varchar",
    "text": "varchar",
    "bool": "boolean",
}


def _normalize_type(type_str: str | None) -> str:
    if not type_str:
        return ""
    normalized = type_str.lower().strip()
    return _TYPE_ALIASES.get(normalized, normalized)


# ---------------------------------------------------------------------------
# Explicit diff spec — temporary bridge for the single-node preview endpoint
# until NodeChange is massaged into a proper NodeSpec.
# ---------------------------------------------------------------------------


@dataclass
class _ExplicitDiffSpec:
    """Captures an explicit user-supplied diff (columns_removed, new_query, etc.).

    Used by the single-node impact-preview endpoint as a stand-in for a real
    NodeSpec until we build the full NodeChange → NodeSpec conversion.
    """

    node_type: NodeType
    new_query: str | None = None
    explicit_columns_removed: set[str] = field(default_factory=set)
    explicit_columns_changed: list[tuple[str, str, str]] = field(default_factory=list)
    dim_links_removed: set[str] = field(default_factory=set)


# ---------------------------------------------------------------------------
# Query-based column inference (Option C: parse new_query inside compute_impact)
# ---------------------------------------------------------------------------


async def _infer_column_diff(
    session: AsyncSession,
    node: Node,
    new_query: str,
) -> tuple[
    list[_ProposedColumn],
    list[str],
    list[tuple[str, str, str]],
    bool,
    list[str],
]:
    """Parse ``new_query`` through the validator to derive the actual column diff.

    Returns ``(proposed_columns, columns_removed, columns_changed, is_valid, errors)`` where:
    - ``proposed_columns`` is what the node would expose after the change
    - ``columns_removed`` are column names present now but absent in new output
    - ``columns_changed`` are ``(name, old_type_str, new_type_str)`` triples
    - ``is_valid`` is False when the new query fails validation (query is broken)
    - ``errors`` are human-readable validation error messages (only set when is_valid=False)

    When ``is_valid`` is False the other lists should be ignored by callers —
    they reflect a failed parse, not a genuine schema change.
    """
    revision_stub = NodeRevisionBase(
        name=node.name,
        type=node.type,
        query=new_query,
    )
    _logger.info(
        "[_infer_column_diff] node=%r type=%r — running validator on new_query: %r",
        node.name,
        node.type,
        new_query[:200] if len(new_query) > 200 else new_query,
    )
    try:
        validator = await validate_node_data(revision_stub, session)
    except Exception as exc:
        _logger.warning(
            "[_infer_column_diff] node=%r validator raised %s: %s — query is invalid",
            node.name,
            type(exc).__name__,
            exc,
        )
        if isinstance(exc, AttributeError):
            msg = "Type resolution failed — a subscript key or field reference in the SELECT output could not be resolved"
        else:
            msg = f"SQL validation failed: {type(exc).__name__}: {exc}"
        return [], [], [], False, [msg]

    if validator.status != NodeStatus.VALID or not validator.columns:
        _logger.info(
            "[_infer_column_diff] node=%r new_query validation failed: status=%r — query is invalid",
            node.name,
            validator.status,
        )
        errors = [e.message for e in (validator.errors or [])]
        return [], [], [], False, errors

    current_col_map = {c.name: c for c in node.current.columns}
    new_col_map = {c.name: c for c in validator.columns}

    columns_removed = [name for name in current_col_map if name not in new_col_map]
    columns_changed: list[tuple[str, str, str]] = [
        (name, str(current_col_map[name].type), str(new_col_map[name].type))
        for name in current_col_map
        if name in new_col_map
        and str(current_col_map[name].type) != str(new_col_map[name].type)
    ]
    proposed_columns = [
        _ProposedColumn(name=c.name, type=c.type) for c in validator.columns
    ]
    _logger.info(
        "[_infer_column_diff] node=%r: inferred %d new cols, %d removed, %d type-changed. "
        "new_cols=%r  removed=%r  changed=%r",
        node.name,
        len(proposed_columns),
        len(columns_removed),
        len(columns_changed),
        [c.name for c in proposed_columns],
        columns_removed,
        columns_changed,
    )
    return proposed_columns, columns_removed, columns_changed, True, []


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
    if child.current.query is None or not child.current.query.strip():
        # SOURCE and CUBE nodes have no meaningful SQL query; skip column validation.
        # Cubes are affected via MAY_AFFECT (dim links / metrics), not SQL compilation.
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
    except Exception as exc:
        _logger.warning(
            "[_validate_downstream_node] exception for %r: %r",
            child.name,
            exc,
        )
        return True, []

    _logger.info(
        "[_validate_downstream_node] child=%r status=%r errors=%r",
        child.name,
        validator.status,
        validator.errors,
    )
    if validator.status != NodeStatus.VALID or validator.errors:
        return True, []

    return False, list(validator.columns) if validator.columns else list(
        child.current.columns,
    )


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
    changed_nodes: dict[
        str,
        tuple[NodeSpec | _ExplicitDiffSpec | None, Node | None, set[str]],
    ],
) -> AsyncGenerator[ImpactedNode, None]:
    """BFS over the DAG to find all downstream nodes affected by the given changes.

    Yields ImpactedNode objects in BFS level order (direct casualties first, then
    their dependents, etc.).

    Parameters
    ----------
    session:
        Async DB session.
    changed_nodes:
        Mapping of node_name → (proposed_spec, existing_node).

        - ``proposed_spec=None`` means the node is being deleted.
        - ``existing_node=None`` means the node is new (no downstream impact).
        - ``proposed_spec`` is either a real ``NodeSpec`` (deployment path) or an
          ``_ExplicitDiffSpec`` (single-node preview path, temporary bridge).
    """
    if not changed_nodes:
        return

    propagated_change: dict[str, _PropagatedChange] = {}
    node_cache: dict[str, Node] = {}
    frontier: set[str] = set()
    visited: set[str] = set()
    impacted: dict[str, ImpactedNode] = {}

    # ---------------------------------------------------------------------------
    # Seed phase — compute the diff for each directly changed node
    # ---------------------------------------------------------------------------
    for node_name, (
        proposed_spec,
        existing_node,
        pre_computed_dim_links,
    ) in changed_nodes.items():
        if existing_node is None:
            # New node — no downstream to affect
            continue

        node_cache[node_name] = existing_node
        visited.add(node_name)

        if proposed_spec is None:
            # Deletion — all direct consumers are unconditionally impacted
            propagated_change[node_name] = _PropagatedChange(
                is_deleted=True,
                caused_by=[node_name],
            )
            frontier.add(node_name)
            continue

        prop = _PropagatedChange(caused_by=[node_name])

        if isinstance(proposed_spec, _ExplicitDiffSpec):
            # Single-node preview: explicit diff supplied by the caller
            prop.dim_links_removed = proposed_spec.dim_links_removed
            if proposed_spec.new_query:
                _logger.info(
                    "[seed] %r: query changed, inferring column diff",
                    node_name,
                )
                proposed_cols, removed, cols_changed, _, _ = await _infer_column_diff(
                    session,
                    existing_node,
                    proposed_spec.new_query,
                )
                prop.proposed_columns = proposed_cols
                prop.columns_removed = set(removed)
                prop.columns_changed = cols_changed
                _logger.info(
                    "[seed] %r: inferred columns_removed=%r columns_changed=%r",
                    node_name,
                    prop.columns_removed,
                    prop.columns_changed,
                )
            elif (
                proposed_spec.explicit_columns_removed
                or proposed_spec.explicit_columns_changed
            ):
                current_col_names = {c.name for c in existing_node.current.columns}
                actually_removed = (
                    proposed_spec.explicit_columns_removed & current_col_names
                )
                prop.columns_removed = actually_removed
                prop.columns_changed = list(proposed_spec.explicit_columns_changed)
                if actually_removed or proposed_spec.explicit_columns_changed:
                    prop.proposed_columns = _build_proposed_columns(
                        existing_node.current.columns,
                        actually_removed,
                        list(proposed_spec.explicit_columns_changed),
                    )
        else:
            # Deployment path: use pre-computed dim link removals from the caller.
            prop.dim_links_removed = pre_computed_dim_links

            if proposed_spec.node_type == NodeType.SOURCE:
                col_changes = _detect_source_column_changes(
                    existing_node,
                    proposed_spec,
                )
                src_removed: set[str] = {
                    cc.column
                    for cc in col_changes
                    if cc.change_type == ColumnChangeType.REMOVED
                }
                src_cols_changed = [
                    (cc.column, cc.old_type or "", cc.new_type or "")
                    for cc in col_changes
                    if cc.change_type == ColumnChangeType.TYPE_CHANGED
                ]
                prop.columns_removed = src_removed
                prop.columns_changed = src_cols_changed
                if src_removed or src_cols_changed:
                    prop.proposed_columns = _build_proposed_columns(
                        existing_node.current.columns,
                        src_removed,
                        src_cols_changed,
                    )
            elif (
                hasattr(proposed_spec, "rendered_query")
                and proposed_spec.rendered_query
                and proposed_spec.rendered_query != existing_node.current.query
            ):
                _logger.info(
                    "[seed] %r: query changed, inferring column diff",
                    node_name,
                )
                (
                    proposed_cols,
                    removed_list,
                    cols_changed,
                    _,
                    _,
                ) = await _infer_column_diff(
                    session,
                    existing_node,
                    proposed_spec.rendered_query,
                )
                prop.proposed_columns = proposed_cols
                prop.columns_removed = set(removed_list)
                prop.columns_changed = cols_changed
                _logger.info(
                    "[seed] %r: inferred columns_removed=%r columns_changed=%r",
                    node_name,
                    prop.columns_removed,
                    prop.columns_changed,
                )

        # If any output columns were removed, check whether any of the node's own
        # DimensionLinks join on those columns — those links are now implicitly broken.
        if prop.columns_removed:
            for link in existing_node.current.dimension_links:
                if link.foreign_key_column_names & prop.columns_removed:
                    _logger.info(
                        "[seed] %r: dim link to %r implicitly broken — "
                        "join columns %r removed",
                        node_name,
                        link.dimension.name,
                        link.foreign_key_column_names & prop.columns_removed,
                    )
                    prop.dim_links_removed.add(link.dimension.name)

        propagated_change[node_name] = prop
        frontier.add(node_name)

    # ---------------------------------------------------------------------------
    # Cube dim-link impact pass: for each node that has dim_links_removed,
    # find all downstream cubes that reference those dimensions via cube_elements.
    # This is done upfront because cubes declare dimensions in cube_elements
    # (not via SQL parents), so the BFS column checks can't detect them.
    # ---------------------------------------------------------------------------
    _logger.info(
        "[compute_impact] propagated_change: %r",
        {
            n: {
                "dim_links_removed": list(c.dim_links_removed),
                "columns_removed": list(c.columns_removed),
                "is_deleted": c.is_deleted,
            }
            for n, c in propagated_change.items()
        },
    )
    for name, prop in propagated_change.items():
        if prop.dim_links_removed:
            _logger.info(
                "[compute_impact] node %r has dim_links_removed=%r — running cube pass",
                name,
                prop.dim_links_removed,
            )
            await _check_cube_dim_link_impacts(
                session=session,
                node_name=name,
                dim_links_removed=prop.dim_links_removed,
                caused_by=prop.caused_by,
                impacted=impacted,
                node_cache=node_cache,
            )
            # Also check cubes of immediate downstream metrics explicitly:
            # N → immediate metric children → their cubes → cube elements.
            # This catches cubes that reference dim_links_removed as elements
            # but may not be found by the full-DAG traversal above.
            if name in node_cache:
                await _check_immediate_metric_cube_impacts(
                    session=session,
                    changed_node=node_cache[name],
                    dim_links_removed=prop.dim_links_removed,
                    caused_by=prop.caused_by,
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
        for name, prop in propagated_change.items()
        if prop.columns_removed
        and node_cache.get(name) is not None
        and node_cache[name].type == NodeType.DIMENSION
    ]
    if dim_nodes_losing_cols:
        holder_names: set[str] = set()
        for dim_node in dim_nodes_losing_cols:
            holder_displays, _ = await get_dimension_inbound_bfs(
                session,
                dim_node,
                depth=1,
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

    # Yield any impacts already recorded by the pre-BFS passes (cube dim-link and
    # case-3 dimension-node-loses-columns).  These are "level 0" results.
    for node in list(impacted.values()):
        yield node

    # ---------------------------------------------------------------------------
    # BFS main loop — one batch query per level instead of one per node
    # ---------------------------------------------------------------------------
    while frontier:
        next_frontier: set[str] = set()
        known_before_level: set[str] = set(impacted.keys())

        # Collect the node.id for every node currently in the frontier
        parent_ids = [node_cache[name].id for name in frontier if name in node_cache]
        # Single round-trip: get all direct children for the entire frontier
        children_by_parent = await _batch_get_children(
            session,
            parent_ids,
            list(_node_output_options()),
        )
        # Map node.id → node_name so we can look up propagated_change
        id_to_name = {node_cache[n].id: n for n in frontier if n in node_cache}

        for parent_node_id, children in children_by_parent.items():
            bfs_node_name = id_to_name.get(parent_node_id)
            if bfs_node_name is None:
                continue
            node_name = bfs_node_name
            node_prop = propagated_change.get(node_name)
            if not node_prop:
                continue
            prop = node_prop

            for child in children:
                child_name = child.name
                node_cache[child_name] = child

                # --- Case 1: column removed/type-changed, node deleted, or invalid ---
                if (
                    prop.is_deleted
                    or prop.is_invalid
                    or prop.proposed_columns is not None
                ):
                    if prop.is_deleted or prop.is_invalid:
                        # Deleted or invalid upstream: all noderelationship children are
                        # unconditionally impacted.  We skip the validator because
                        # column_overrides cannot intercept value-level references such as
                        # metric-on-metric (the compiler inlines parent metric SQL rather
                        # than resolving columns via the override dict).
                        reason = (
                            "Upstream node was deleted"
                            if prop.is_deleted
                            else f"Upstream node '{node_name}' is invalid"
                        )
                        _record_impact(
                            impacted,
                            child,
                            impact_type="deleted_parent",
                            caused_by=[node_name],
                            reason=reason,
                        )
                        _merge_propagated(
                            propagated_change,
                            child_name,
                            dim_links_removed=set(),
                            is_deleted=False,
                            is_invalid=True,
                            caused_by=[node_name],
                        )
                        if child_name not in visited:
                            next_frontier.add(child_name)
                            visited.add(child_name)
                    elif prop.proposed_columns is not None:
                        # Column schema changed: validate the child's SQL against the
                        # proposed upstream state.  Pass all known-impacted nodes in
                        # column_overrides so siblings that also changed are included.
                        upstream_proposed = {
                            name: pc.proposed_columns
                            for name, pc in propagated_change.items()
                            if pc.proposed_columns is not None
                        }
                        _logger.info(
                            "[BFS] validating child=%r against proposed upstream %r (%d cols)",
                            child_name,
                            node_name,
                            len(prop.proposed_columns),
                        )
                        is_impacted, new_cols = await _validate_downstream_node(
                            session,
                            child,
                            upstream_proposed,
                        )
                        _logger.info(
                            "[BFS] child=%r is_impacted=%r new_cols=%d",
                            child_name,
                            is_impacted,
                            len(new_cols),
                        )
                        if is_impacted:
                            _record_impact(
                                impacted,
                                child,
                                impact_type="column",
                                caused_by=[node_name],
                                reason=_column_reason(prop),
                            )
                            _merge_propagated(
                                propagated_change,
                                child_name,
                                dim_links_removed=set(),
                                is_deleted=False,
                                is_invalid=len(new_cols) == 0,
                                caused_by=[node_name],
                                proposed_columns=new_cols if new_cols else None,
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
                                caused_by=[node_name],
                                reason=_dim_link_reason(prop),
                            )
                    elif child.type != NodeType.CUBE:
                        # Propagate through intermediate nodes so downstream metrics
                        # can be reached.
                        _merge_propagated(
                            propagated_change,
                            child_name,
                            dim_links_removed=prop.dim_links_removed,
                            is_deleted=False,
                            caused_by=[node_name],
                        )
                        if child_name not in visited:
                            next_frontier.add(child_name)
                            visited.add(child_name)

        # Yield nodes newly discovered in this BFS level
        for name in set(impacted.keys()) - known_before_level:
            yield impacted[name]

        frontier = next_frontier


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
    dim_links_removed: set[str],
    is_deleted: bool,
    caused_by: list[str],
    is_invalid: bool = False,
    proposed_columns: list | None = None,
) -> None:
    """Merge propagated change state into the dict entry for `name`."""
    if name not in propagated_change:
        propagated_change[name] = _PropagatedChange(caused_by=list(caused_by))
    pc = propagated_change[name]
    pc.dim_links_removed |= dim_links_removed
    pc.is_deleted = pc.is_deleted or is_deleted
    pc.is_invalid = pc.is_invalid or is_invalid
    for c in caused_by:
        if c not in pc.caused_by:
            pc.caused_by.append(c)
    if proposed_columns is not None:
        pc.proposed_columns = proposed_columns


def _column_reason(change: _PropagatedChange) -> str:
    if change.is_deleted:
        return "Upstream node was deleted"
    parts: list[str] = []
    if change.columns_removed:
        parts.append(
            f"Upstream columns removed: {', '.join(sorted(change.columns_removed))}",
        )
    if change.columns_changed:
        changed_names = sorted({n for n, _, _ in change.columns_changed})
        parts.append(f"Upstream column types changed: {', '.join(changed_names)}")
    return "; ".join(parts) if parts else "Upstream column change"


def _dim_link_reason(change: _PropagatedChange) -> str:
    dims = sorted(change.dim_links_removed)
    return f"Dimension links removed: {', '.join(dims)}"
