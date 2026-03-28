"""
Deployment impact analysis - predicts effects of a deployment without executing it.
"""

import logging
import re
import time
from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.attributetype import AttributeType
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node
from datajunction_server.internal.impact import (
    compute_impact,
    _detect_dim_link_removals,
    _detect_source_column_changes,
    _infer_column_diff,
)
from datajunction_server.models.deployment import (
    CubeSpec,
    DeploymentSpec,
    LinkableNodeSpec,
    MetricSpec,
    NodeSpec,
    SourceSpec,
)
from datajunction_server.models.impact import (
    ColumnChange,
    ColumnChangeType,
    DeploymentImpactResponse,
    DimLinkChange,
    ImpactType,
    NodeChangeOperation,
    NodeEffect,
)
from datajunction_server.models.node import NodeStatus, NodeType
from datajunction_server.sql.dag import _node_output_options

logger = logging.getLogger(__name__)

# Matches sequences of dot-separated identifiers, e.g. "a.b.c.d"
_DOTTED_IDENT_RE = re.compile(r"[\w$]+(?:\.[\w$]+)+")


def _extract_join_on_candidates(join_on: str) -> set[str]:
    """Extract all plausible node-name candidates from a join_on SQL expression.

    Each dotted identifier ``a.b.c.d`` contributes prefix candidates
    ``a.b.c``, ``a.b``, ``a`` (all but the last segment, which is the column).
    Identifiers with any digit-starting segment are skipped — they are numeric
    literals, not node names.
    """
    candidates: set[str] = set()
    for match in _DOTTED_IDENT_RE.finditer(join_on):
        segments = match.group().split(".")
        if len(segments) < 2:
            continue
        if any(seg[0].isdigit() for seg in segments):
            continue
        for i in range(len(segments) - 1, 0, -1):
            candidates.add(".".join(segments[:i]))
    return candidates


def _validate_dim_link_join_on(
    join_on: str,
    known_node_names: set[str],
) -> list[str]:
    """Check all dotted identifiers in ``join_on`` against ``known_node_names``.

    Call ``_bulk_resolve_join_on_candidates`` first to populate ``known_node_names``
    with DB results so this function needs no DB access.

    Returns a list of human-readable error strings (empty when everything resolves).
    """
    errors: list[str] = []
    seen: set[str] = set()
    for match in _DOTTED_IDENT_RE.finditer(join_on):
        segments = match.group().split(".")
        if len(segments) < 2:
            continue
        if any(seg[0].isdigit() for seg in segments):
            continue
        resolved = any(
            ".".join(segments[:i]) in known_node_names
            for i in range(len(segments) - 1, 0, -1)
        )
        if not resolved:
            candidate = ".".join(segments[:-1])
            if candidate not in seen:
                errors.append(
                    f"Node '{candidate}' referenced in join_on does not exist",
                )
                seen.add(candidate)
    return errors


# ---------------------------------------------------------------------------
# Context — all DB data loaded upfront, shared across per-node comparisons
# ---------------------------------------------------------------------------


@dataclass
class _ImpactContext:
    """Pre-loaded DB data required for diffing every node in the deployment spec."""

    existing_nodes_map: dict[str, Node]
    valid_attribute_names: set[str]
    known_node_names: set[str]


async def _load_impact_context(
    session: AsyncSession,
    deployment_spec: DeploymentSpec,
) -> _ImpactContext:
    """Load all DB state needed for diffing: existing nodes, attribute types, join_on targets."""
    existing_nodes: list[Node] = []
    try:
        existing_nodes = await NodeNamespace.list_all_nodes(
            session,
            deployment_spec.namespace,
            options=list(_node_output_options()),
        )
    except Exception as e:
        logger.info(
            "Namespace %s does not exist, all nodes will be creates: %s",
            deployment_spec.namespace,
            e,
        )

    existing_nodes_map = {node.name: node for node in existing_nodes}

    # Cubes need a separate reload to expose cube_node_metrics / cube_node_dimensions
    # (those properties are backed by cube_elements which requires its own load option).
    cube_node_names = [n.name for n in existing_nodes if n.type == NodeType.CUBE]
    if cube_node_names:
        cube_nodes_full = await Node.get_by_names(
            session,
            cube_node_names,
            options=list(Node.cube_load_options()),
        )
        for cube_node in cube_nodes_full:
            existing_nodes_map[cube_node.name] = cube_node

    valid_attribute_names: set[str] = {
        at.name for at in await AttributeType.get_all(session)
    }

    # Seed known_node_names with existing and proposed nodes, then bulk-fetch any
    # additional nodes referenced in join_on expressions so per-link validation
    # below needs no further DB queries.
    known_node_names: set[str] = set(existing_nodes_map.keys()) | {
        n.rendered_name for n in deployment_spec.nodes
    }
    all_candidates: set[str] = set()
    for node_spec in deployment_spec.nodes:
        if isinstance(node_spec, LinkableNodeSpec):
            for link in node_spec.dimension_links:
                all_candidates.add(link.rendered_dimension_node)
                if link.rendered_join_on:
                    all_candidates.update(
                        _extract_join_on_candidates(link.rendered_join_on),
                    )
    unknown_candidates = all_candidates - known_node_names
    if unknown_candidates:
        found_nodes = await Node.get_by_names(
            session,
            list(unknown_candidates),
            options=[],
        )
        known_node_names.update(n.name for n in found_nodes)

    return _ImpactContext(
        existing_nodes_map=existing_nodes_map,
        valid_attribute_names=valid_attribute_names,
        known_node_names=known_node_names,
    )


# ---------------------------------------------------------------------------
# Diff result — accumulates per-node diff state across all sub-steps
# ---------------------------------------------------------------------------


@dataclass
class _NodeDiffResult:
    """Accumulated diff for a single node being re-deployed."""

    changed_fields: list[str] = field(default_factory=list)
    column_changes: list[ColumnChange] = field(default_factory=list)
    dim_link_changes: list[DimLinkChange] = field(default_factory=list)
    new_query: str | None = None
    predicted_status: NodeStatus | None = None
    validation_errors: list[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(
            self.changed_fields
            or self.column_changes
            or self.dim_link_changes
            or self.new_query,
        )


# ---------------------------------------------------------------------------
# Per-node diff sub-steps — each owns exactly one concern
# ---------------------------------------------------------------------------


def _validate_schema(
    result: _NodeDiffResult,
    node_spec: NodeSpec,
    valid_attribute_names: set[str],
) -> None:
    """Check primary key requirement (dimensions) and column attribute names."""
    if (
        node_spec.node_type == NodeType.DIMENSION
        and isinstance(node_spec, LinkableNodeSpec)
        and not node_spec.primary_key
    ):
        result.validation_errors.append("Dimension nodes must define a primary key")
        result.predicted_status = NodeStatus.INVALID
        result.changed_fields.append("primary_key_invalid")

    if isinstance(node_spec, LinkableNodeSpec) and node_spec.columns:
        for col_spec in node_spec.columns:
            invalid_attrs = [
                a for a in col_spec.attributes if a not in valid_attribute_names
            ]
            if invalid_attrs:
                for attr in invalid_attrs:
                    result.validation_errors.append(
                        f"Column '{col_spec.name}' has unknown attribute '{attr}'",
                    )
                result.predicted_status = NodeStatus.INVALID
                if "column_attribute_invalid" not in result.changed_fields:
                    result.changed_fields.append("column_attribute_invalid")


async def _compute_column_diff(
    result: _NodeDiffResult,
    session: AsyncSession,
    existing_node: Node,
    node_spec: NodeSpec,
) -> None:
    """Populate column_changes and new_query from source YAML diff or SQL inference."""
    if node_spec.node_type == NodeType.SOURCE:
        column_changes = _detect_source_column_changes(existing_node, node_spec)
        result.column_changes.extend(column_changes)
        if column_changes:
            result.changed_fields.append("columns")
        return

    rendered_query = getattr(node_spec, "rendered_query", None)
    if not rendered_query or rendered_query == existing_node.current.query:
        return

    logger.info(
        "Query changed for %s\n  existing: %s\n  proposed: %s",
        node_spec.rendered_name,
        existing_node.current.query,
        rendered_query,
    )
    (
        _,
        removed_names,
        cols_changed,
        query_is_valid,
        errors,
    ) = await _infer_column_diff(session, existing_node, rendered_query)

    if not query_is_valid:
        # Invalid query — don't set new_query so compute_impact skips column inference;
        # downstream is handled via the deleted_parent path instead.
        result.changed_fields.append("query_invalid")
        result.predicted_status = NodeStatus.INVALID
        result.validation_errors = errors
    else:
        result.new_query = rendered_query
        result.changed_fields.append("query")
        result.predicted_status = NodeStatus.VALID
        result.column_changes.extend(
            ColumnChange(column=col, change_type=ColumnChangeType.REMOVED)
            for col in removed_names
        )
        result.column_changes.extend(
            ColumnChange(
                column=col,
                change_type=ColumnChangeType.TYPE_CHANGED,
                old_type=old,
                new_type=new,
            )
            for col, old, new in cols_changed
        )


def _compute_dim_link_diff(
    result: _NodeDiffResult,
    existing_node: Node,
    node_spec: NodeSpec,
    known_node_names: set[str],
) -> None:
    """Populate dim_link_changes: explicit removals, implicit breaks, added/updated, join_on validation."""
    if not isinstance(node_spec, LinkableNodeSpec):
        return

    explicitly_removed = _detect_dim_link_removals(existing_node, node_spec)
    removed_cols = {
        cc.column
        for cc in result.column_changes
        if cc.change_type == ColumnChangeType.REMOVED
    }

    # Explicit removals
    for dim_name in explicitly_removed:
        result.dim_link_changes.append(
            DimLinkChange(dim_name=dim_name, operation="removed"),
        )

    # Implicit breaks: a join column was removed so the link no longer works
    for link in existing_node.current.dimension_links:
        dim_name = link.dimension.name  # type: ignore[union-attr]
        broken_cols = list(link.foreign_key_column_names & removed_cols)
        if broken_cols and dim_name not in explicitly_removed:
            logger.info(
                "Implicit dim link break for %s: link to %s broken "
                "because join columns %s were removed",
                node_spec.rendered_name,
                dim_name,
                broken_cols,
            )
            result.dim_link_changes.append(
                DimLinkChange(
                    dim_name=dim_name,
                    operation="broken",
                    broken_by_columns=broken_cols,
                ),
            )

    # Added and updated links
    existing_links_by_dim = {
        dl.dimension.name: dl for dl in existing_node.current.dimension_links
    }
    for link in node_spec.dimension_links:
        target_dim = link.rendered_dimension_node
        if target_dim not in existing_links_by_dim:
            result.dim_link_changes.append(
                DimLinkChange(dim_name=target_dim, operation="added"),
            )
        else:
            existing_dl = existing_links_by_dim[target_dim]
            existing_join_on = existing_dl.join_sql or ""
            spec_join_on = link.rendered_join_on or ""
            existing_join_type = existing_dl.join_type or "LEFT"
            spec_join_type = link.join_type if hasattr(link, "join_type") else "LEFT"
            existing_role = existing_dl.role or ""
            spec_role = link.role or "" if hasattr(link, "role") else ""
            existing_default = existing_dl.default_value or ""
            spec_default = (
                link.default_value or "" if hasattr(link, "default_value") else ""
            )
            if (
                existing_join_on != spec_join_on
                or str(existing_join_type) != str(spec_join_type)
                or existing_role != spec_role
                or existing_default != spec_default
            ):
                result.dim_link_changes.append(
                    DimLinkChange(dim_name=target_dim, operation="updated"),
                )

    if result.dim_link_changes:
        logger.info(
            "Dim link changes for %s: %s",
            node_spec.rendered_name,
            [(c.dim_name, c.operation) for c in result.dim_link_changes],
        )

    # Validate join_on expressions — no additional DB access (known_node_names pre-populated)
    existing_dim_names = {
        dl.dimension.name for dl in existing_node.current.dimension_links
    }
    for link in node_spec.dimension_links:
        target_dim = link.rendered_dimension_node
        if target_dim not in known_node_names:
            logger.info(
                "Dimension node does not exist: %s → %s",
                node_spec.rendered_name,
                target_dim,
            )
            result.validation_errors.append(
                f"Dimension node '{target_dim}' does not exist",
            )
            result.predicted_status = NodeStatus.INVALID
            if "dim_link_invalid" not in result.changed_fields:
                result.changed_fields.append("dim_link_invalid")

        join_on = link.rendered_join_on
        if join_on:
            link_errors = _validate_dim_link_join_on(join_on, known_node_names)
            if link_errors:
                logger.info(
                    "Invalid join_on for %s → %s: %s",
                    node_spec.rendered_name,
                    link.rendered_dimension_node,
                    link_errors,
                )
                result.validation_errors.extend(link_errors)
                result.predicted_status = NodeStatus.INVALID
                if "dim_link_invalid" not in result.changed_fields:
                    result.changed_fields.append("dim_link_invalid")
                # If this link previously existed (same target dim), treat it as "broken"
                # so the cube impact check fires.  Metrics downstream are not invalidated
                # — they just lose the dim.  New links with a broken join_on have no
                # existing cube references, so they need no cube check.
                if target_dim in existing_dim_names and not any(
                    dlc.dim_name == target_dim for dlc in result.dim_link_changes
                ):
                    result.dim_link_changes.append(
                        DimLinkChange(
                            dim_name=target_dim,
                            operation="broken",
                            broken_by_columns=[],
                        ),
                    )


def _compute_metadata_diff(
    result: _NodeDiffResult,
    existing_node: Node,
    node_spec: NodeSpec,
) -> None:
    """Append changed field names for all metadata-only changes (no query, no columns)."""
    # Node-level metadata
    if node_spec.display_name is not None and (node_spec.display_name or "") != (
        existing_node.current.display_name or ""
    ):
        logger.info(
            "display_name changed for %s: %r -> %r",
            node_spec.rendered_name,
            existing_node.current.display_name,
            node_spec.display_name,
        )
        result.changed_fields.append("display_name")

    if (node_spec.description or "") != (existing_node.current.description or ""):
        logger.info(
            "description changed for %s: %r -> %r",
            node_spec.rendered_name,
            existing_node.current.description,
            node_spec.description,
        )
        result.changed_fields.append("description")

    if set(getattr(node_spec, "tags", [])) != {t.name for t in existing_node.tags}:
        logger.info(
            "tags changed for %s: %s -> %s",
            node_spec.rendered_name,
            {t.name for t in existing_node.tags},
            set(getattr(node_spec, "tags", [])),
        )
        result.changed_fields.append("tags")

    if hasattr(node_spec, "mode") and node_spec.mode != existing_node.current.mode:
        logger.info(
            "mode changed for %s: %r -> %r",
            node_spec.rendered_name,
            existing_node.current.mode,
            node_spec.mode,
        )
        result.changed_fields.append("mode")

    if set(getattr(node_spec, "owners", [])) != {
        o.username for o in existing_node.owners
    }:
        result.changed_fields.append("owners")

    if (node_spec.custom_metadata or {}) != (
        existing_node.current.custom_metadata or {}
    ):
        result.changed_fields.append("custom_metadata")

    # Source-specific: underlying table location
    if node_spec.node_type == NodeType.SOURCE and isinstance(node_spec, SourceSpec):
        existing_catalog = (
            existing_node.current.catalog.name
            if existing_node.current.catalog
            else None
        )
        if node_spec.catalog != existing_catalog:
            result.changed_fields.append("catalog")
        if (node_spec.schema_ or "") != (existing_node.current.schema_ or ""):
            result.changed_fields.append("schema")
        if (node_spec.table or "") != (existing_node.current.table or ""):
            result.changed_fields.append("table")

    # Metric-specific: semantic metadata
    if node_spec.node_type == NodeType.METRIC and isinstance(node_spec, MetricSpec):
        mm = existing_node.current.metric_metadata
        if node_spec.direction != (mm.direction if mm else None):
            result.changed_fields.append("direction")
        existing_unit = mm.unit.value.name.lower() if mm and mm.unit else None
        if (node_spec.unit or None) != existing_unit:
            result.changed_fields.append("unit")
        if node_spec.significant_digits != (mm.significant_digits if mm else None):
            result.changed_fields.append("significant_digits")
        if node_spec.min_decimal_exponent != (mm.min_decimal_exponent if mm else None):
            result.changed_fields.append("min_decimal_exponent")
        if node_spec.max_decimal_exponent != (mm.max_decimal_exponent if mm else None):
            result.changed_fields.append("max_decimal_exponent")

    # Cube-specific: metrics, dimensions, filters
    if node_spec.node_type == NodeType.CUBE and isinstance(node_spec, CubeSpec):
        if set(node_spec.rendered_metrics) != set(
            existing_node.current.cube_node_metrics,
        ):
            result.changed_fields.append("metrics")
        if set(node_spec.rendered_dimensions) != set(
            existing_node.current.cube_node_dimensions,
        ):
            result.changed_fields.append("dimensions")
        if set(node_spec.rendered_filters) != set(
            existing_node.current.cube_filters or [],
        ):
            result.changed_fields.append("filters")

    # Column-level metadata (display_name, description) — applies to all node types
    if isinstance(node_spec, LinkableNodeSpec) and node_spec.columns:
        existing_col_map = {col.name: col for col in existing_node.current.columns}
        for col_spec in node_spec.columns:
            existing_col = existing_col_map.get(col_spec.name)
            if existing_col is None:
                continue
            if col_spec.display_name is not None and (col_spec.display_name or "") != (
                existing_col.display_name or ""
            ):
                logger.info(
                    "Column display_name changed for %s.%s: %r -> %r",
                    node_spec.rendered_name,
                    col_spec.name,
                    existing_col.display_name,
                    col_spec.display_name,
                )
                if "column_metadata" not in result.changed_fields:
                    result.changed_fields.append("column_metadata")
            if col_spec.description is not None and (col_spec.description or "") != (
                existing_col.description or ""
            ):
                logger.info(
                    "Column description changed for %s.%s",
                    node_spec.rendered_name,
                    col_spec.name,
                )
                if "column_metadata" not in result.changed_fields:
                    result.changed_fields.append("column_metadata")


async def _diff_existing_node(
    session: AsyncSession,
    existing_node: Node,
    node_spec: NodeSpec,
    ctx: _ImpactContext,
) -> _NodeDiffResult:
    """Compute the full diff for a node that already exists in the namespace."""
    result = _NodeDiffResult()
    _validate_schema(result, node_spec, ctx.valid_attribute_names)
    await _compute_column_diff(result, session, existing_node, node_spec)
    _compute_dim_link_diff(result, existing_node, node_spec, ctx.known_node_names)
    _compute_metadata_diff(result, existing_node, node_spec)
    return result


# ---------------------------------------------------------------------------
# NodeEffect constructors — one per operation type
# ---------------------------------------------------------------------------


def _create_effect(node_spec: NodeSpec) -> NodeEffect:
    return NodeEffect(
        name=node_spec.rendered_name,
        operation=NodeChangeOperation.CREATE,
        node_type=node_spec.node_type,
        display_name=node_spec.display_name,
        description=node_spec.description,
        current_status=None,
    )


def _delete_effect(node: Node) -> NodeEffect:
    return NodeEffect(
        name=node.name,
        operation=NodeChangeOperation.DELETE,
        node_type=node.type,
        display_name=node.current.display_name,
        description=node.current.description,
        current_status=node.current.status,
    )


def _update_or_noop_effect(
    existing_node: Node,
    node_spec: NodeSpec,
    diff: _NodeDiffResult,
) -> NodeEffect:
    if diff.has_changes:
        return NodeEffect(
            name=node_spec.rendered_name,
            operation=NodeChangeOperation.UPDATE,
            node_type=node_spec.node_type,
            display_name=node_spec.display_name,
            description=node_spec.description,
            current_status=existing_node.current.status,
            predicted_status=diff.predicted_status,
            changed_fields=diff.changed_fields,
            column_changes=diff.column_changes,
            dim_link_changes=diff.dim_link_changes,
            validation_errors=diff.validation_errors,
            new_query=diff.new_query,
        )
    logger.info("No changes detected for %s", node_spec.rendered_name)
    return NodeEffect(
        name=node_spec.rendered_name,
        operation=NodeChangeOperation.NOOP,
        node_type=node_spec.node_type,
        display_name=node_spec.display_name,
        description=node_spec.description,
        current_status=existing_node.current.status,
    )


# ---------------------------------------------------------------------------
# Classification — CREATE / UPDATE / NOOP / DELETE for every node in the spec
# ---------------------------------------------------------------------------


async def _classify_all_nodes(
    session: AsyncSession,
    deployment_spec: DeploymentSpec,
    ctx: _ImpactContext,
) -> list[NodeEffect]:
    """Emit one NodeEffect per node: CREATE for new, UPDATE/NOOP for existing, DELETE for absent."""
    changes: list[NodeEffect] = []

    for node_spec in deployment_spec.nodes:
        existing_node = ctx.existing_nodes_map.get(node_spec.rendered_name)
        if not existing_node:
            changes.append(_create_effect(node_spec))
        else:
            diff = await _diff_existing_node(session, existing_node, node_spec, ctx)
            changes.append(_update_or_noop_effect(existing_node, node_spec, diff))

    # Nodes present in the namespace but absent from the spec are being deleted.
    desired_names = {n.rendered_name for n in deployment_spec.nodes}
    for name, node in ctx.existing_nodes_map.items():
        if name not in desired_names:
            changes.append(_delete_effect(node))

    return changes


# ---------------------------------------------------------------------------
# Response builder
# ---------------------------------------------------------------------------


def _build_response(
    deployment_spec: DeploymentSpec,
    changes: list[NodeEffect],
    downstream_impacts: list[NodeEffect],
    warnings: list[str],
    start_time: float,
) -> DeploymentImpactResponse:
    create_count = sum(1 for c in changes if c.operation == NodeChangeOperation.CREATE)
    update_count = sum(1 for c in changes if c.operation == NodeChangeOperation.UPDATE)
    delete_count = sum(1 for c in changes if c.operation == NodeChangeOperation.DELETE)
    skip_count = sum(1 for c in changes if c.operation == NodeChangeOperation.NOOP)
    will_invalidate_count = sum(
        1 for imp in downstream_impacts if imp.impact_type == ImpactType.WILL_INVALIDATE
    )
    may_affect_count = sum(
        1 for imp in downstream_impacts if imp.impact_type == ImpactType.MAY_AFFECT
    )
    logger.info(
        "Impact analysis completed in %.3fs: %d creates, %d updates, %d deletes, "
        "%d skips, %d downstream impacts",
        time.perf_counter() - start_time,
        create_count,
        update_count,
        delete_count,
        skip_count,
        len(downstream_impacts),
    )
    return DeploymentImpactResponse(
        namespace=deployment_spec.namespace,
        changes=changes,
        create_count=create_count,
        update_count=update_count,
        delete_count=delete_count,
        skip_count=skip_count,
        downstream_impacts=downstream_impacts,
        will_invalidate_count=will_invalidate_count,
        may_affect_count=may_affect_count,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def analyze_deployment_impact(
    session: AsyncSession,
    deployment_spec: DeploymentSpec,
) -> DeploymentImpactResponse:
    """
    Analyze the impact of a deployment WITHOUT actually deploying.

    Returns:
        DeploymentImpactResponse with:
        - Direct changes (CREATE/UPDATE/DELETE/NOOP)
        - Predicted downstream impacts
        - Warnings about potential issues
    """
    start_time = time.perf_counter()
    logger.info(
        "Analyzing impact for deployment of %d nodes to namespace %s",
        len(deployment_spec.nodes),
        deployment_spec.namespace,
    )

    ctx = await _load_impact_context(session, deployment_spec)
    changes = await _classify_all_nodes(session, deployment_spec, ctx)
    downstream_impacts = await _analyze_downstream_impacts(
        session=session,
        changes=changes,
        deployment_namespace=deployment_spec.namespace,
        existing_nodes_map=ctx.existing_nodes_map,
        proposed_specs_map={n.rendered_name: n for n in deployment_spec.nodes},
    )
    warnings = _generate_warnings(changes, downstream_impacts)
    return _build_response(
        deployment_spec,
        changes,
        downstream_impacts,
        warnings,
        start_time,
    )


# ---------------------------------------------------------------------------
# Downstream impact propagation — delegates to compute_impact BFS engine
# ---------------------------------------------------------------------------


async def _analyze_downstream_impacts(
    session: AsyncSession,
    changes: list[NodeEffect],
    deployment_namespace: str,
    existing_nodes_map: dict[str, Node] | None = None,
    proposed_specs_map: dict[str, NodeSpec] | None = None,
) -> list[NodeEffect]:
    """Analyze downstream impact by passing (proposed_spec, existing_node) pairs to compute_impact."""
    directly_changed_names = {
        c.name for c in changes if c.operation != NodeChangeOperation.NOOP
    }

    existing_nodes_map = existing_nodes_map or {}
    proposed_specs_map = proposed_specs_map or {}

    node_pairs: dict[str, tuple[NodeSpec | None, Node | None, set[str]]] = {}
    for change in changes:
        dim_links_removed = {dlc.dim_name for dlc in change.dim_link_changes}
        if change.operation == NodeChangeOperation.DELETE:
            node_pairs[change.name] = (
                None,
                existing_nodes_map.get(change.name),
                dim_links_removed,
            )
        elif change.operation == NodeChangeOperation.UPDATE:
            # Invalid query: treat like a deletion so compute_impact uses the
            # deleted_parent path instead of re-running column inference.
            if "query_invalid" in change.changed_fields:
                node_pairs[change.name] = (
                    None,
                    existing_nodes_map.get(change.name),
                    dim_links_removed,
                )
            else:
                node_pairs[change.name] = (
                    proposed_specs_map.get(change.name),
                    existing_nodes_map.get(change.name),
                    dim_links_removed,
                )

    impacted = [node async for node in compute_impact(session, node_pairs)]  # type: ignore[arg-type]

    impacts: list[NodeEffect] = []
    for node in impacted:
        if node.name in directly_changed_names:
            continue
        is_external = not node.name.startswith(deployment_namespace + ".")
        impact_type = (
            ImpactType.WILL_INVALIDATE
            if node.impact_type in ("column", "deleted_parent")
            else ImpactType.MAY_AFFECT
        )
        impacts.append(
            NodeEffect(
                name=node.name,
                node_type=node.node_type,
                current_status=node.current_status,
                predicted_status=node.projected_status,
                impact_type=impact_type,
                impact_reason=node.reason,
                depth=1,
                caused_by=node.caused_by,
                is_external=is_external,
            ),
        )

    # Upgrade MAY_AFFECT → WILL_INVALIDATE when at least one caused_by node is
    # already WILL_INVALIDATE (e.g. a cube whose constituent metrics are invalid).
    will_invalidate_names = {
        imp.name for imp in impacts if imp.impact_type == ImpactType.WILL_INVALIDATE
    }
    for imp in impacts:
        if imp.impact_type == ImpactType.MAY_AFFECT and any(
            cause in will_invalidate_names for cause in imp.caused_by
        ):
            imp.impact_type = ImpactType.WILL_INVALIDATE

    return impacts


# ---------------------------------------------------------------------------
# Warning generation
# ---------------------------------------------------------------------------


def _generate_warnings(
    changes: list[NodeEffect],
    downstream_impacts: list[NodeEffect],
) -> list[str]:
    """
    Generate warnings about potential issues with the deployment.
    """
    warnings = []

    # Warn about breaking column changes and removed/broken dimension links
    for change in changes:
        if change.operation == NodeChangeOperation.UPDATE:
            for cc in change.column_changes:
                if cc.change_type == ColumnChangeType.REMOVED:
                    warnings.append(
                        f"Breaking change: Column '{cc.column}' is being removed from "
                        f"'{change.name}'",
                    )
                elif cc.change_type == ColumnChangeType.TYPE_CHANGED:
                    warnings.append(
                        f"Potential breaking change: Column '{cc.column}' in '{change.name}' "
                        f"is changing type from {cc.old_type} to {cc.new_type}",
                    )
            for dlc in change.dim_link_changes:
                if dlc.operation == "removed":
                    warnings.append(
                        f"Breaking change: Dimension link to '{dlc.dim_name}' is being removed from "
                        f"'{change.name}'",
                    )
                elif dlc.operation == "broken":
                    warnings.append(
                        f"Breaking change: Dimension link to '{dlc.dim_name}' is implicitly broken "
                        f"in '{change.name}' (join columns removed: {', '.join(dlc.broken_by_columns)})",
                    )

    # Warn about query changes where we couldn't detect column changes
    # This happens when query parsing fails or columns are unchanged
    # Note: We now try to infer columns from queries, so this warning only
    # triggers when inference failed or no column changes were detected
    # Cubes are excluded since we don't compare columns for them
    query_changes_no_column_changes = [
        change.name
        for change in changes
        if change.operation == NodeChangeOperation.UPDATE
        and "query" in change.changed_fields
        and not change.column_changes  # No column changes detected/inferred
        and change.node_type != NodeType.CUBE  # Cubes don't have column comparison
    ]
    if query_changes_no_column_changes:
        warnings.append(
            f"Query changed for: {', '.join(query_changes_no_column_changes)}. "
            f"No column changes detected (columns may be unchanged, or parsing failed).",
        )

    # Warn about deletions with downstream dependencies
    deletions_with_downstreams = [
        change.name
        for change in changes
        if change.operation == NodeChangeOperation.DELETE
        and any(change.name in impact.caused_by for impact in downstream_impacts)
    ]
    if deletions_with_downstreams:
        warnings.append(
            f"Deleting nodes with downstream dependencies: {', '.join(deletions_with_downstreams)}",
        )

    # Warn about external impacts
    external_impacts = [imp for imp in downstream_impacts if imp.is_external]
    if external_impacts:
        external_names = [imp.name for imp in external_impacts[:5]]
        more = len(external_impacts) - 5 if len(external_impacts) > 5 else 0
        warnings.append(
            f"Changes will affect nodes outside this namespace: {', '.join(external_names)}"
            + (f" and {more} more" if more else ""),
        )

    # Warn about high impact count
    will_invalidate = [
        imp
        for imp in downstream_impacts
        if imp.impact_type == ImpactType.WILL_INVALIDATE
    ]
    if len(will_invalidate) > 10:
        warnings.append(
            f"This deployment will invalidate {len(will_invalidate)} downstream nodes",
        )

    return warnings
