"""
Handles metric decomposition and grain analysis

TODO: Add validation to reject derived metrics that aggregate other metrics
(e.g., AVG(other_metric) without OVER clause). Derived metrics should only do
arithmetic or window functions on base metrics, not introduce new aggregation
layers. This check should happen during metric creation/update validation.
"""

from __future__ import annotations

from typing import cast

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.types import (
    BuildContext,
    DecomposedMetricInfo,
    GrainGroup,
    MetricGroup,
)
from datajunction_server.database.node import Node
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import Aggregability, MetricComponent
from datajunction_server.sql.decompose import MetricComponentExtractor
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse


async def decompose_and_group_metrics(
    ctx: BuildContext,
) -> tuple[list[MetricGroup], dict[str, DecomposedMetricInfo]]:
    """
    Decompose metrics and group them by parent node (fact/transform).

    For base metrics: groups by direct parent
    For derived metrics: decomposes into base metrics and groups by their parents

    This enables cross-fact derived metrics by producing separate grain groups
    for each underlying fact.

    Returns:
        Tuple of:
        - List of MetricGroup, one per unique parent node, with decomposed metrics.
        - Dict of metric_name -> DecomposedMetricInfo for reuse by callers.
    """
    # Map parent node name -> list of DecomposedMetricInfo
    parent_groups: dict[str, list[DecomposedMetricInfo]] = {}
    parent_nodes: dict[str, Node] = {}
    # Cache of all decomposed metrics to avoid redundant work
    all_decomposed: dict[str, DecomposedMetricInfo] = {}

    for metric_name in ctx.metrics:
        metric_node = ctx.get_metric_node(metric_name)

        if is_derived_metric(ctx, metric_node):
            # Derived metric - get base metrics and decompose each
            base_metric_nodes = get_base_metrics_for_derived(ctx, metric_node)

            for base_metric in base_metric_nodes:
                # Get the fact/transform parent of the base metric
                parent_node = ctx.get_parent_node(base_metric)
                parent_name = parent_node.name

                # Check if already decomposed (e.g., shared base metric)
                if base_metric.name in all_decomposed:
                    decomposed = all_decomposed[base_metric.name]
                else:
                    # Decompose the BASE metric (not the derived one) - use cache!
                    decomposed = await decompose_metric(
                        ctx.session,
                        base_metric,
                        nodes_cache=ctx.nodes,
                        parent_map=ctx.parent_map,
                    )
                    all_decomposed[base_metric.name] = decomposed

                # Always ensure parent group exists and add if not already present
                if parent_name not in parent_groups:
                    parent_groups[parent_name] = []
                    parent_nodes[parent_name] = parent_node

                # Only add to parent group if not already there
                if decomposed not in parent_groups[parent_name]:
                    parent_groups[parent_name].append(decomposed)

            # Also decompose the derived metric itself and cache it
            if metric_name not in all_decomposed:  # pragma: no branch
                derived_decomposed = await decompose_metric(
                    ctx.session,
                    metric_node,
                    nodes_cache=ctx.nodes,
                    parent_map=ctx.parent_map,
                )
                all_decomposed[metric_name] = derived_decomposed
        else:
            # Base metric - use direct parent
            parent_node = ctx.get_parent_node(metric_node)
            parent_name = parent_node.name

            if metric_name in all_decomposed:
                # Already decomposed - just ensure it's in the parent group
                decomposed = all_decomposed[metric_name]  # pragma: no branch
                if parent_name not in parent_groups:  # pragma: no cover
                    parent_groups[parent_name] = []
                    parent_nodes[parent_name] = parent_node
                if decomposed not in parent_groups[parent_name]:
                    parent_groups[parent_name].append(decomposed)  # pragma: no cover
                continue

            # Use cache to avoid DB queries!
            decomposed = await decompose_metric(
                ctx.session,
                metric_node,
                nodes_cache=ctx.nodes,
                parent_map=ctx.parent_map,
            )
            all_decomposed[metric_node.name] = decomposed

            if parent_name not in parent_groups:
                parent_groups[parent_name] = []
                parent_nodes[parent_name] = parent_node

            parent_groups[parent_name].append(decomposed)

    # Build MetricGroup objects
    metric_groups = [
        MetricGroup(parent_node=parent_nodes[name], decomposed_metrics=metrics)
        for name, metrics in parent_groups.items()
    ]
    # Sort by parent node name for deterministic ordering in generated SQL
    metric_groups.sort(key=lambda g: g.parent_node.name)
    return metric_groups, all_decomposed


async def decompose_metric(
    session: AsyncSession,
    metric_node: Node,
    *,
    nodes_cache: dict[str, Node] | None = None,
    parent_map: dict[str, list[str]] | None = None,
) -> DecomposedMetricInfo:
    """
    Decompose a metric into its constituent components.

    Uses MetricComponentExtractor to break down aggregations like:
    - SUM(x) -> [sum_x component]
    - AVG(x) -> [sum_x component, count_x component]
    - COUNT(DISTINCT x) -> [distinct_x component with LIMITED aggregability]

    Args:
        session: Database session
        metric_node: The metric node to decompose
        nodes_cache: Optional dict of node_name -> Node. If provided along with
            parent_map, avoids database queries by using cached data.
        parent_map: Optional dict of child_name -> list of parent_names.
            Required if nodes_cache is provided.

    Returns:
        DecomposedMetricInfo with components, combiner expression, and aggregability
    """
    if not metric_node.current:  # pragma: no cover
        raise DJInvalidInputException(
            f"Metric {metric_node.name} has no current revision",
        )

    # Use the MetricComponentExtractor with optional cache
    extractor = MetricComponentExtractor(metric_node.current.id)
    components, derived_ast = await extractor.extract(
        session,
        nodes_cache=nodes_cache,
        parent_map=parent_map,
        metric_node=metric_node,
    )

    # Extract combiner expression from the derived query AST
    # The first projection element is the metric expression with component references
    combiner = (
        str(derived_ast.select.projection[0]) if derived_ast.select.projection else ""
    )

    # Determine overall aggregability (worst case among components)
    if not components:  # pragma: no cover
        # No decomposable aggregations found - treat as NONE
        aggregability = Aggregability.NONE
    elif any(c.rule.type == Aggregability.NONE for c in components):  # pragma: no cover
        aggregability = Aggregability.NONE
    elif any(c.rule.type == Aggregability.LIMITED for c in components):
        aggregability = Aggregability.LIMITED
    else:
        aggregability = Aggregability.FULL

    return DecomposedMetricInfo(
        metric_node=metric_node,
        components=components,
        aggregability=aggregability,
        combiner=combiner,
        derived_ast=derived_ast,
    )


def build_component_expression(component: MetricComponent) -> ast.Expression:
    """
    Build the accumulate expression AST for a metric component.

    For simple aggregations like SUM, this is: SUM(expression)
    For templates like "SUM(POWER({}, 2))", expands to: SUM(POWER(expression, 2))

    Note: Templates may be pre-expanded (e.g., "SUM(POWER(match_score, 2))")
    by the decomposition phase, so we detect this by checking for parentheses
    without template placeholders.
    """
    if not component.aggregation:  # pragma: no cover
        # No aggregation - just return the expression as a column
        return ast.Column(name=ast.Name(component.expression))

    # Check if it's an unexpanded template with {}
    if "{" in component.aggregation:  # pragma: no cover
        # Template like "SUM(POWER({}, 2))" - expand it
        expanded = component.aggregation.replace("{}", component.expression)
        # Parse as expression
        expr_ast = parse(f"SELECT {expanded}").select.projection[0]
        if isinstance(expr_ast, ast.Alias):
            expr_ast = expr_ast.child
        expr_ast.clear_parent()
        return cast(ast.Expression, expr_ast)

    # Check if it's a pre-expanded template (contains parentheses, like "SUM(POWER(x, 2))")
    # vs a simple function name (like "SUM")
    if "(" in component.aggregation:
        # Pre-expanded template - parse it directly as a complete expression
        expr_ast = parse(f"SELECT {component.aggregation}").select.projection[0]
        if isinstance(expr_ast, ast.Alias):
            expr_ast = expr_ast.child  # pragma: no cover
        expr_ast.clear_parent()
        return cast(ast.Expression, expr_ast)
    else:
        # Simple function name like "SUM" - build SUM(expression)
        arg_expr = parse(f"SELECT {component.expression}").select.projection[0]
        func = ast.Function(
            name=ast.Name(component.aggregation),
            args=[cast(ast.Expression, arg_expr)],
        )
        return func


def get_base_metrics_for_derived(ctx: BuildContext, metric_node: Node) -> list[Node]:
    """
    For a derived metric, get all the base metrics it depends on.

    Returns list of base metric nodes (metrics that SELECT FROM a fact/transform, not other metrics).
    """
    base_metrics = []
    visited = set()

    def collect_bases(node: Node):
        if node.name in visited:  # pragma: no cover
            return
        visited.add(node.name)

        parent_names = ctx.parent_map.get(node.name, [])
        for parent_name in parent_names:
            parent = ctx.nodes.get(parent_name)
            if not parent:  # pragma: no cover
                continue

            if parent.type == NodeType.METRIC:
                # Parent is also a metric - recurse
                collect_bases(parent)
            elif parent.type == NodeType.DIMENSION:
                # Skip dimension nodes - they're for required dimensions (e.g., in window
                # functions), not the actual data source. Don't treat the derived metric
                # as a base metric just because it references a dimension.
                continue  # pragma: no cover
            else:
                # Parent is a fact/transform - this is a base metric
                base_metrics.append(node)
                break  # Found the base, don't check other parents

    collect_bases(metric_node)
    return base_metrics


def is_derived_metric(ctx: BuildContext, metric_node: Node) -> bool:
    """Check if a metric is derived (references other metrics) vs base (references fact/transform)."""
    parent_names = ctx.parent_map.get(metric_node.name, [])
    if not parent_names:  # pragma: no cover
        return False

    # Check if ANY parent is a metric (not just the first one)
    # This handles cases where a dimension (for required dimensions in window functions)
    # appears before the metric parent in the parent list
    for parent_name in parent_names:
        parent = ctx.nodes.get(parent_name)
        if parent is not None and parent.type == NodeType.METRIC:
            return True
    return False


def get_native_grain(node: Node) -> list[str]:
    """
    Get the native grain (primary key columns) of a node.

    For transforms/dimensions, this is their primary key columns.
    If no PK is defined, returns all columns (every column together forms
    the unique identity of a row).
    """
    if not node.current:  # pragma: no cover
        return []

    pk_columns = []
    for col in node.current.columns:
        # Check if this column is part of the primary key
        if col.has_primary_key_attribute():
            pk_columns.append(col.name)

    # If no PK is defined, use all columns as the grain
    # This ensures we don't accidentally aggregate non-decomposable metrics
    if not pk_columns:
        pk_columns = [col.name for col in node.current.columns]  # pragma: no cover

    return pk_columns


def analyze_grain_groups(
    metric_group: MetricGroup,
    requested_dimensions: list[str],
) -> list[GrainGroup]:
    """
    Analyze a MetricGroup and split it into GrainGroups based on aggregability.

    Each GrainGroup contains components that can be computed at the same grain.

    Rules:
    - FULL aggregability: grain = requested dimensions
    - LIMITED aggregability: grain = requested dimensions + level columns
    - NONE aggregability: grain = native grain (PK of parent)

    Non-decomposable metrics (like MAX_BY) have no components but still need
    a grain group at native grain to pass through raw rows.

    Args:
        metric_group: MetricGroup with decomposed metrics
        requested_dimensions: Dimensions requested by user (column names only)

    Returns:
        List of GrainGroups, one per unique grain
    """
    parent_node = metric_group.parent_node

    # Group components by their effective grain
    # Key: (aggregability, tuple of additional grain columns)
    grain_buckets: dict[
        tuple[Aggregability, tuple[str, ...]],
        list[tuple[Node, MetricComponent]],
    ] = {}

    # Track non-decomposable metrics (those with no components)
    non_decomposable: list[DecomposedMetricInfo] = []

    for decomposed in metric_group.decomposed_metrics:
        if not decomposed.components:
            # Non-decomposable metric (like MAX_BY) - track it separately
            non_decomposable.append(decomposed)
            continue

        # Process decomposable components
        for component in decomposed.components:
            agg_type = component.rule.type

            # Explicitly type the key to satisfy mypy
            key: tuple[Aggregability, tuple[str, ...]]
            if agg_type == Aggregability.FULL:
                # FULL: no additional grain columns needed
                key = (Aggregability.FULL, ())
            elif agg_type == Aggregability.LIMITED:
                # LIMITED: add level columns to grain
                level_cols = tuple(sorted(component.rule.level or []))
                key = (Aggregability.LIMITED, level_cols)
            else:  # NONE
                # NONE: use native grain (PK columns)
                native_grain = get_native_grain(parent_node)
                key = (
                    Aggregability.NONE,
                    tuple(sorted(native_grain)),
                )  # pragma: no cover

            if key not in grain_buckets:
                grain_buckets[key] = []
            grain_buckets[key].append((decomposed.metric_node, component))

    # Convert buckets to GrainGroup objects
    grain_groups = []
    for (agg_type, grain_cols), components in grain_buckets.items():
        grain_groups.append(
            GrainGroup(
                parent_node=parent_node,
                aggregability=agg_type,
                grain_columns=list(grain_cols),
                components=components,
            ),
        )

    # Handle non-decomposable metrics - create a NONE grain group at native grain
    if non_decomposable:
        native_grain = get_native_grain(parent_node)
        none_key = (Aggregability.NONE, tuple(sorted(native_grain)))

        # Check if we already have a NONE grain group at this grain
        existing_none = next(
            (g for g in grain_groups if g.grain_key[1:] == none_key),
            None,
        )

        if existing_none:
            # Add non-decomposable metrics to existing NONE group
            existing_none.non_decomposable_metrics.extend(
                non_decomposable,
            )  # pragma: no cover
        else:
            # Create new NONE grain group for non-decomposable metrics
            grain_groups.append(
                GrainGroup(
                    parent_node=parent_node,
                    aggregability=Aggregability.NONE,
                    grain_columns=list(native_grain),
                    components=[],  # No components
                    non_decomposable_metrics=non_decomposable,
                ),
            )

    # Sort groups: FULL first, then LIMITED, then NONE (for consistent output)
    agg_order = {Aggregability.FULL: 0, Aggregability.LIMITED: 1, Aggregability.NONE: 2}
    grain_groups.sort(
        key=lambda g: (agg_order.get(g.aggregability, 3), g.grain_columns),
    )

    return grain_groups


def merge_grain_groups(grain_groups: list[GrainGroup]) -> list[GrainGroup]:
    """
    Merge compatible grain groups from the same parent into single CTEs.

    Grain groups can be merged if they share the same parent node. The merged
    group uses the finest grain (union of all grain columns) and outputs raw
    columns instead of pre-aggregated values. Aggregations are then applied
    in the final SELECT.

    This optimization reduces duplicate CTEs and JOINs when multiple metrics
    with different aggregabilities come from the same parent.

    Args:
        grain_groups: List of grain groups to potentially merge

    Returns:
        List of grain groups with compatible groups merged
    """
    from collections import defaultdict

    # Group by parent node name
    by_parent: dict[str, list[GrainGroup]] = defaultdict(list)
    for gg in grain_groups:
        by_parent[gg.parent_node.name].append(gg)

    merged_groups: list[GrainGroup] = []

    for parent_name, parent_groups in by_parent.items():
        if len(parent_groups) == 1:
            # Only one group for this parent - no merge needed
            merged_groups.append(parent_groups[0])
        else:
            # Multiple groups for same parent - merge them
            merged = _merge_parent_grain_groups(parent_groups)
            merged_groups.append(merged)

    # Sort for deterministic output
    agg_order = {Aggregability.FULL: 0, Aggregability.LIMITED: 1, Aggregability.NONE: 2}
    merged_groups.sort(
        key=lambda g: (
            g.parent_node.name,
            agg_order.get(g.aggregability, 3),
            g.grain_columns,
        ),
    )

    return merged_groups


def _merge_parent_grain_groups(groups: list[GrainGroup]) -> GrainGroup:
    """
    Merge multiple grain groups from the same parent into one.

    The merged group:
    - Uses the finest grain (union of all grain columns)
    - Has aggregability = worst case (NONE > LIMITED > FULL)
    - Contains all components from all groups
    - Has is_merged=True to signal that aggregations happen in final SELECT
    - Tracks original component aggregabilities for proper final aggregation
    """
    if not groups:  # pragma: no cover
        raise ValueError("Cannot merge empty list of grain groups")

    parent_node = groups[0].parent_node

    # Collect all components and track their original aggregabilities
    all_components: list[tuple[Node, MetricComponent]] = []
    component_aggregabilities: dict[str, Aggregability] = {}

    for gg in groups:
        for metric_node, component in gg.components:
            all_components.append((metric_node, component))
            # Track original aggregability for each component
            component_aggregabilities[component.name] = gg.aggregability

    # Compute finest grain (union of all grain columns)
    finest_grain_set: set[str] = set()
    for gg in groups:
        finest_grain_set.update(gg.grain_columns)
    finest_grain = sorted(finest_grain_set)

    # Determine worst-case aggregability
    # NONE > LIMITED > FULL (NONE is worst, forces finest grain)
    agg_order = {Aggregability.FULL: 0, Aggregability.LIMITED: 1, Aggregability.NONE: 2}
    worst_agg = max(
        groups,
        key=lambda g: agg_order.get(g.aggregability, 0),
    ).aggregability

    return GrainGroup(
        parent_node=parent_node,
        aggregability=worst_agg,
        grain_columns=finest_grain,
        components=all_components,
        is_merged=True,
        component_aggregabilities=component_aggregabilities,
    )
