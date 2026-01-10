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
    CubeSpec,
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
from datajunction_server.models.node import NodeStatus
from datajunction_server.sql.dag import get_downstream_nodes

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

            # Always check for column changes (may detect changes not in spec equality)
            column_changes = _detect_column_changes(existing_node, node_spec)

            # If column changes detected, this is an update even if spec __eq__ returned True
            if spec_changed or column_changes:
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


def _detect_column_changes(
    existing_node: Node | None,
    new_spec: NodeSpec,
) -> list[ColumnChange]:
    """
    Compare existing node columns to new spec and detect breaking changes.
    """
    changes: list[ColumnChange] = []

    if not existing_node or not existing_node.current:
        return changes

    existing_columns = {col.name: col for col in existing_node.current.columns}

    # Get new columns from spec
    new_columns: dict[str, ColumnSpec] = {}
    if isinstance(new_spec, LinkableNodeSpec) and new_spec.columns:
        new_columns = {col.name: col for col in new_spec.columns}
    elif isinstance(new_spec, CubeSpec) and new_spec.columns:
        new_columns = {col.name: col for col in new_spec.columns}

    # If no columns specified in spec, we can't detect column changes
    # (columns will be inferred during deployment)
    if not new_columns:
        return changes

    # Detect removed columns (breaking change)
    for col_name in existing_columns.keys() - new_columns.keys():
        changes.append(
            ColumnChange(
                column=col_name,
                change_type=ColumnChangeType.REMOVED,
                old_type=str(existing_columns[col_name].type),
            ),
        )

    # Detect added columns (non-breaking)
    for col_name in new_columns.keys() - existing_columns.keys():
        changes.append(
            ColumnChange(
                column=col_name,
                change_type=ColumnChangeType.ADDED,
                new_type=new_columns[col_name].type,
            ),
        )

    # Detect type changes (potentially breaking)
    for col_name in existing_columns.keys() & new_columns.keys():
        old_type = str(existing_columns[col_name].type)
        new_type = new_columns[col_name].type
        if old_type != new_type:
            changes.append(
                ColumnChange(
                    column=col_name,
                    change_type=ColumnChangeType.TYPE_CHANGED,
                    old_type=old_type,
                    new_type=new_type,
                ),
            )

    return changes


async def _analyze_downstream_impacts(
    session: AsyncSession,
    changes: list[NodeChange],
    deployment_namespace: str,
) -> list[DownstreamImpact]:
    """
    Analyze how downstream nodes will be affected by the changes.
    """
    impacts: list[DownstreamImpact] = []
    seen_downstreams: set[str] = set()

    # Get the names of nodes being directly changed
    directly_changed_names = {
        c.name for c in changes if c.operation != NodeChangeOperation.NOOP
    }

    for change in changes:
        # Only analyze impact for updates and deletes
        if change.operation not in (
            NodeChangeOperation.UPDATE,
            NodeChangeOperation.DELETE,
        ):
            continue

        # Get all downstream nodes
        try:
            downstreams = await get_downstream_nodes(
                session,
                change.name,
                include_deactivated=False,
                include_cubes=True,
            )
        except Exception as e:
            logger.warning(
                "Failed to get downstreams for %s: %s",
                change.name,
                e,
            )
            continue

        for downstream in downstreams:
            # Skip if this downstream is being directly changed in this deployment
            if downstream.name in directly_changed_names:
                continue

            # Skip if we've already analyzed this downstream
            if downstream.name in seen_downstreams:
                # But we should add this change to the caused_by list
                for impact in impacts:
                    if (
                        impact.name == downstream.name
                        and change.name not in impact.caused_by
                    ):
                        impact.caused_by.append(change.name)
                continue

            seen_downstreams.add(downstream.name)

            # Predict impact based on change type
            impact = _predict_downstream_impact(
                downstream=downstream,
                change=change,
                deployment_namespace=deployment_namespace,
            )
            impacts.append(impact)

    # Sort by severity and depth
    impacts.sort(
        key=lambda x: (
            0 if x.impact_type == ImpactType.WILL_INVALIDATE else 1,
            x.depth,
            x.name,
        ),
    )

    return impacts


def _predict_downstream_impact(
    downstream: Node,
    change: NodeChange,
    deployment_namespace: str,
) -> DownstreamImpact:
    """
    Predict how a single downstream node will be affected by a change.
    """
    current_status = (
        downstream.current.status if downstream.current else NodeStatus.INVALID
    )

    # Determine if the downstream is external to the deployment namespace
    is_external = not downstream.name.startswith(deployment_namespace + ".")

    # Predict impact based on change type
    if change.operation == NodeChangeOperation.DELETE:
        # Deleting a node will definitely invalidate downstreams
        return DownstreamImpact(
            name=downstream.name,
            node_type=downstream.type,
            current_status=current_status,
            predicted_status=NodeStatus.INVALID,
            impact_type=ImpactType.WILL_INVALIDATE,
            impact_reason=f"Depends on {change.name} which will be deleted",
            depth=1,
            caused_by=[change.name],
            is_external=is_external,
        )

    # For updates, check if there are breaking column changes
    breaking_column_changes = [
        cc
        for cc in change.column_changes
        if cc.change_type in (ColumnChangeType.REMOVED, ColumnChangeType.TYPE_CHANGED)
    ]

    if breaking_column_changes:
        # Check if downstream might reference the changed columns
        # This is a heuristic - we'd need to parse the query to be certain
        breaking_columns = [cc.column for cc in breaking_column_changes]

        return DownstreamImpact(
            name=downstream.name,
            node_type=downstream.type,
            current_status=current_status,
            predicted_status=NodeStatus.INVALID,
            impact_type=ImpactType.WILL_INVALIDATE,
            impact_reason=f"May reference columns {breaking_columns} which changed in {change.name}",
            depth=1,
            caused_by=[change.name],
            is_external=is_external,
        )

    # For other updates, the downstream may need revalidation
    return DownstreamImpact(
        name=downstream.name,
        node_type=downstream.type,
        current_status=current_status,
        predicted_status=current_status,  # Status likely unchanged
        impact_type=ImpactType.MAY_AFFECT,
        impact_reason=f"Depends on {change.name} which is being updated",
        depth=1,
        caused_by=[change.name],
        is_external=is_external,
    )


def _generate_warnings(
    changes: list[NodeChange],
    downstream_impacts: list[DownstreamImpact],
) -> list[str]:
    """
    Generate warnings about potential issues with the deployment.
    """
    warnings = []

    # Warn about breaking column changes
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
