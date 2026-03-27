"""
Deployment impact analysis - predicts effects of a deployment without executing it.
"""

import logging
import time

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node
from datajunction_server.models.deployment import (
    ColumnSpec,
    DimensionJoinLinkSpec,
    DeploymentSpec,
    LinkableNodeSpec,
    NodeSpec,
)
from datajunction_server.models.impact import (
    ColumnChange,
    ColumnChangeType,
    DeploymentImpactResponse,
    DownstreamImpact,
    ImpactType,
    NodeChange,
    NodeChangeOperation,
)
from datajunction_server.models.node import NodeType
from datajunction_server.internal.impact import compute_impact
from datajunction_server.models.impact_preview import NodeChange as ImpactNodeChange

logger = logging.getLogger(__name__)


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

    # Load existing nodes in the namespace (may not exist yet)
    existing_nodes = []
    try:
        existing_nodes = await NodeNamespace.list_all_nodes(
            session,
            deployment_spec.namespace,
            options=Node.cube_load_options(),
        )
    except Exception as e:
        # Namespace doesn't exist - all nodes will be creates
        logger.info(
            "Namespace %s does not exist, all nodes will be creates: %s",
            deployment_spec.namespace,
            e,
        )

    existing_nodes_map = {node.name: node for node in existing_nodes}

    # Convert existing nodes to specs for comparison
    existing_specs = {node.name: await node.to_spec(session) for node in existing_nodes}

    # Analyze direct changes
    changes = []
    to_create = []
    to_update = []
    to_skip = []

    for node_spec in deployment_spec.nodes:
        existing_spec = existing_specs.get(node_spec.rendered_name)
        existing_node = existing_nodes_map.get(node_spec.rendered_name)

        if not existing_spec:
            # New node
            changes.append(
                NodeChange(
                    name=node_spec.rendered_name,
                    operation=NodeChangeOperation.CREATE,
                    node_type=node_spec.node_type,
                    display_name=node_spec.display_name,
                    description=node_spec.description,
                    current_status=None,
                ),
            )
            to_create.append(node_spec)
        else:
            # Check for spec changes
            spec_changed = node_spec != existing_spec
            changed_fields = existing_spec.diff(node_spec) if spec_changed else []

            # Column diff: only for SOURCE nodes, which always have explicit column types
            # in their YAML. For transform/dimension/metric nodes, column types come from
            # SQL inference — BFS validation in compute_impact handles those accurately.
            column_changes = []
            if node_spec.node_type == NodeType.SOURCE and existing_node:
                column_changes = _detect_source_column_changes(existing_node, node_spec)

            # Dimension link diff: detect removed links for any linkable node type
            dim_links_removed: list[str] = []
            if existing_node and isinstance(node_spec, LinkableNodeSpec):
                dim_links_removed = _detect_dim_link_removals(existing_node, node_spec)

            # If column changes or dim link changes detected, this is an update
            # even if spec __eq__ returned True
            if spec_changed or column_changes or dim_links_removed:
                changes.append(
                    NodeChange(
                        name=node_spec.rendered_name,
                        operation=NodeChangeOperation.UPDATE,
                        node_type=node_spec.node_type,
                        display_name=node_spec.display_name,
                        description=node_spec.description,
                        current_status=existing_node.current.status
                        if existing_node
                        else None,
                        changed_fields=changed_fields,
                        column_changes=column_changes,
                        dim_links_removed=dim_links_removed,
                    ),
                )
                to_update.append(node_spec)
            else:
                # Unchanged node
                changes.append(
                    NodeChange(
                        name=node_spec.rendered_name,
                        operation=NodeChangeOperation.NOOP,
                        node_type=node_spec.node_type,
                        display_name=node_spec.display_name,
                        description=node_spec.description,
                        current_status=existing_node.current.status
                        if existing_node
                        else None,
                    ),
                )
                to_skip.append(node_spec)

    # Detect nodes to delete (exist in namespace but not in deployment)
    desired_names = {n.rendered_name for n in deployment_spec.nodes}
    to_delete = [
        existing_spec
        for name, existing_spec in existing_specs.items()
        if name not in desired_names
    ]

    for deleted_spec in to_delete:
        existing_node = existing_nodes_map.get(deleted_spec.rendered_name)
        changes.append(
            NodeChange(
                name=deleted_spec.rendered_name,
                operation=NodeChangeOperation.DELETE,
                node_type=deleted_spec.node_type,
                display_name=deleted_spec.display_name,
                description=deleted_spec.description,
                current_status=existing_node.current.status if existing_node else None,
            ),
        )

    # Analyze downstream impact for changed nodes
    changed_node_names = [
        c.name
        for c in changes
        if c.operation in (NodeChangeOperation.UPDATE, NodeChangeOperation.DELETE)
    ]

    downstream_impacts = []
    if changed_node_names:
        downstream_impacts = await _analyze_downstream_impacts(
            session=session,
            changes=changes,
            deployment_namespace=deployment_spec.namespace,
        )

    # Generate warnings
    warnings = _generate_warnings(changes, downstream_impacts)

    # Calculate counts
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
        len(to_create),
        len(to_update),
        len(to_delete),
        len(to_skip),
        len(downstream_impacts),
    )

    return DeploymentImpactResponse(
        namespace=deployment_spec.namespace,
        changes=changes,
        create_count=len(to_create),
        update_count=len(to_update),
        delete_count=len(to_delete),
        skip_count=len(to_skip),
        downstream_impacts=downstream_impacts,
        will_invalidate_count=will_invalidate_count,
        may_affect_count=may_affect_count,
        warnings=warnings,
    )


def _detect_source_column_changes(
    existing_node: Node,
    new_spec: NodeSpec,
) -> list[ColumnChange]:
    """Detect column changes for SOURCE nodes, which always have explicit types in YAML.

    For transform/dimension/metric nodes, column types come from SQL inference.
    Those are handled by BFS validation in ``compute_impact`` rather than upfront diff.
    """
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
        old_type = str(existing_columns[col_name].type)
        new_type = spec_columns[col_name].type
        if new_type and _normalize_type(old_type) != _normalize_type(new_type):
            changes.append(
                ColumnChange(
                    column=col_name,
                    change_type=ColumnChangeType.TYPE_CHANGED,
                    old_type=old_type,
                    new_type=new_type,
                ),
            )

    return changes


def _detect_dim_link_removals(
    existing_node: Node,
    new_spec: LinkableNodeSpec,
) -> list[str]:
    """Return dimension node names whose links exist in the DB but are absent from new_spec.

    Only JOIN-type links carry dimension availability downstream (reference links are
    column-level annotations). We compare by rendered dimension node name.
    """
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


# Type aliases - map alternative names to canonical names
_TYPE_ALIASES: dict[str, str] = {
    # Integer types
    "long": "bigint",
    "int64": "bigint",
    "integer": "int",
    "int32": "int",
    "short": "smallint",
    "int16": "smallint",
    "byte": "tinyint",
    "int8": "tinyint",
    # Floating point types
    "double": "double",
    "float64": "double",
    "float": "float",
    "float32": "float",
    "real": "float",
    # String types
    "string": "varchar",
    "text": "varchar",
    # Boolean
    "bool": "boolean",
}


def _normalize_type(type_str: str | None) -> str:
    """
    Normalize a type string to a canonical form for comparison.

    This handles aliases like:
    - bigint / long / int64 → bigint
    - int / integer / int32 → int
    - double / float64 → double
    - string / varchar / text → varchar
    """
    if not type_str:
        return ""

    # Lowercase and strip whitespace
    normalized = type_str.lower().strip()

    # Check for aliases
    return _TYPE_ALIASES.get(normalized, normalized)


async def _analyze_downstream_impacts(
    session: AsyncSession,
    changes: list[NodeChange],
    deployment_namespace: str,
) -> list[DownstreamImpact]:
    """
    Analyze how downstream nodes will be affected by the changes.

    Delegates to compute_impact for BFS traversal with column-level pruning and
    dimension-link awareness, then maps the results back to DownstreamImpact objects.
    """
    # Build the impact_preview NodeChange dict from deployment NodeChange objects
    directly_changed_names = {
        c.name for c in changes if c.operation != NodeChangeOperation.NOOP
    }
    impact_changes: dict[str, ImpactNodeChange] = {}
    for change in changes:
        if change.operation == NodeChangeOperation.DELETE:
            impact_changes[change.name] = ImpactNodeChange(is_deleted=True)
        elif change.operation == NodeChangeOperation.UPDATE:
            impact_changes[change.name] = ImpactNodeChange(
                columns_removed=[
                    cc.column
                    for cc in change.column_changes
                    if cc.change_type == ColumnChangeType.REMOVED
                ],
                columns_changed=[
                    (cc.column, cc.old_type or "", cc.new_type or "")
                    for cc in change.column_changes
                    if cc.change_type == ColumnChangeType.TYPE_CHANGED
                ],
                columns_added=[
                    cc.column
                    for cc in change.column_changes
                    if cc.change_type == ColumnChangeType.ADDED
                ],
                dim_links_removed=list(change.dim_links_removed),
            )

    impacted = await compute_impact(session, impact_changes)

    impacts: list[DownstreamImpact] = []
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
            DownstreamImpact(
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

    return impacts


def _generate_warnings(
    changes: list[NodeChange],
    downstream_impacts: list[DownstreamImpact],
) -> list[str]:
    """
    Generate warnings about potential issues with the deployment.
    """
    warnings = []

    # Warn about breaking column changes and removed dimension links
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
            for dim_name in change.dim_links_removed:
                warnings.append(
                    f"Breaking change: Dimension link to '{dim_name}' is being removed from "
                    f"'{change.name}'",
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
