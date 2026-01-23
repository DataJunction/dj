"""
Cube matching logic for SQL generation (V3).

This module provides functions to find cubes that can satisfy a metrics query,
enabling direct querying from materialized cube tables instead of computing
from scratch.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.construction.build_v3.decomposition import is_derived_metric
from datajunction_server.models.dialect import Dialect
from datajunction_server.construction.build_v3.dimensions import parse_dimension_ref
from datajunction_server.construction.build_v3.metrics import (
    generate_metrics_sql,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    ColumnMetadata,
    GeneratedMeasuresSQL,
    GeneratedSQL,
    GrainGroupSQL,
    DecomposedMetricInfo,
)
from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.models.decompose import Aggregability
from datajunction_server.models.node_type import NodeType
from datajunction_server.naming import amenable_name
from datajunction_server.sql.parsing import ast

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


async def find_matching_cube(
    session: AsyncSession,
    metrics: list[str],
    dimensions: list[str],
    require_availability: bool = True,
) -> Optional[NodeRevision]:
    """
    Find a cube that covers all requested metrics and dimensions.

    A cube matches if:
    1. It contains all requested metrics (by node name)
    2. It contains all requested dimensions
    3. Cube has availability state (materialized) - configurable with require_availability

    Args:
        session: Database session
        metrics: List of metric node names
        dimensions: List of dimension references (e.g., "default.date_dim.date_id")
        require_availability: If True, only consider cubes with availability defined

    Returns:
        Matching cube NodeRevision if found, None otherwise
    """
    if not metrics:
        return None

    # Build query for cubes
    statement = (
        select(Node)
        .where(Node.type == NodeType.CUBE)
        .where(Node.deactivated_at.is_(None))
        .join(
            NodeRevision,
            and_(
                Node.id == NodeRevision.node_id,
                Node.current_version == NodeRevision.version,
            ),
        )
        .options(
            joinedload(Node.current).options(
                selectinload(NodeRevision.cube_elements).selectinload(
                    Column.node_revision,
                ),
                joinedload(NodeRevision.availability),
                selectinload(NodeRevision.materializations),
                selectinload(NodeRevision.columns),
            ),
        )
    )

    # Filter: cube must contain all requested metrics
    # Cube elements use amenable name format (e.g., "default_DOT_metric" instead of "default.metric")
    for metric_name in metrics:
        amenable_metric_name = amenable_name(metric_name)
        statement = statement.filter(
            NodeRevision.cube_elements.any(Column.name == amenable_metric_name),
        )

    result = await session.execute(statement)
    candidate_cubes = result.unique().scalars().all()

    # Find the best matching cube (smallest grain that covers all dimensions)
    best_match: Optional[NodeRevision] = None
    best_grain_size = float("inf")

    for cube_node in candidate_cubes:
        cube_rev = cube_node.current
        if not cube_rev:
            continue  # pragma: no cover

        # Check availability if required
        if require_availability and not cube_rev.availability:
            logger.debug(
                f"[BuildV3] Cube {cube_rev.name} skipped: no availability",
            )
            continue

        # Check dimension coverage: requested dims must be subset of cube dims
        cube_dims = set(cube_rev.cube_dimensions())
        requested_dims = set(dimensions)

        if not requested_dims.issubset(cube_dims):
            logger.debug(
                f"[BuildV3] Cube {cube_rev.name} dims {cube_dims} "
                f"don't cover requested {requested_dims}",
            )
            continue

        # Found a match - prefer smallest grain (less roll-up work)
        if len(cube_dims) < best_grain_size:
            best_match = cube_rev
            best_grain_size = len(cube_dims)
            logger.debug(
                f"[BuildV3] Found matching cube {cube_rev.name} "
                f"(grain_size={len(cube_dims)})",
            )

    if best_match:
        logger.info(
            f"[BuildV3] Using cube {best_match.name} for "
            f"metrics={metrics}, dims={dimensions}",
        )

    return best_match


def build_sql_from_cube_impl(
    ctx: BuildContext,
    cube: NodeRevision,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
) -> GeneratedSQL:
    """
    Internal: Build SQL from cube with pre-computed context and decomposed metrics.

    This is the core implementation used by both:
    - build_sql_from_cube() for direct calls
    - build_metrics_sql() when a matching cube is found

    Args:
        ctx: BuildContext with nodes loaded and dimensions updated
        cube: The cube NodeRevision to query from
        decomposed_metrics: Pre-computed decomposed metrics

    Returns:
        GeneratedSQL with the query and column metadata.
    """
    # Build synthetic GrainGroupSQL for cube table
    synthetic_grain_group = build_synthetic_grain_group(
        ctx,
        decomposed_metrics,
        cube,
    )

    # Create GeneratedMeasuresSQL and call generate_metrics_sql
    measures_result = GeneratedMeasuresSQL(
        grain_groups=[synthetic_grain_group],
        dialect=ctx.dialect,
        requested_dimensions=ctx.dimensions,
        ctx=ctx,
        decomposed_metrics=decomposed_metrics,
    )

    result = generate_metrics_sql(
        ctx,
        measures_result,
        decomposed_metrics,
    )

    # Set cube_name so /data/ endpoint knows to use Druid engine
    # cube is a NodeRevision, use its name directly
    result.cube_name = cube.name

    return result


async def build_sql_from_cube(
    session: AsyncSession,
    cube: NodeRevision,
    metrics: list[str],
    dimensions: list[str],
    filters: list[str] | None,
    dialect: Dialect,
) -> GeneratedSQL:
    """
    Build final metrics SQL by querying directly from a cube's availability table.

    This is the public API for direct calls (e.g., from tests).
    For the internal path from build_metrics_sql(), use build_sql_from_cube_impl().

    Args:
        session: Database session
        cube: The cube NodeRevision to query from
        metrics: List of metric node names (full paths like "default.total_revenue")
        dimensions: List of dimension references (full paths like "default.date_dim.date_id")
        filters: Optional filter expressions
        dialect: SQL dialect for output

    Returns:
        GeneratedSQL with the query and column metadata.
    """
    # Import here to avoid circular dependency
    from datajunction_server.construction.build_v3.builder import setup_build_context

    # Setup context (loads nodes, decomposes metrics, adds dimensions from expressions)
    ctx = await setup_build_context(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=filters,
        dialect=dialect,
        use_materialized=False,
    )

    # Use shared implementation
    return build_sql_from_cube_impl(ctx, cube, ctx.decomposed_metrics)


def build_synthetic_grain_group(
    ctx: BuildContext,
    decomposed_metrics: dict[str, DecomposedMetricInfo],
    cube: NodeRevision,
) -> GrainGroupSQL:
    """
    Collect components from base metrics only (not derived).
    V3 cube column naming always uses component.name (the hashed name) for consistency.
    """
    all_components = []
    component_aliases: dict[str, str] = {}

    avail = cube.availability
    if not avail:  # pragma: no cover
        raise ValueError(f"Cube {cube.name} has no availability")
    table_parts = [p for p in [avail.catalog, avail.schema_, avail.table] if p]
    table_name = ".".join(table_parts)

    for metric_name, decomposed in decomposed_metrics.items():
        # Only process BASE metrics for component alias mapping
        # Derived metrics don't define cube columns - they reference base metric columns
        metric_node = ctx.nodes.get(metric_name)
        if metric_node and is_derived_metric(ctx, metric_node):
            continue

        for comp in decomposed.components:
            if comp.name not in component_aliases:  # pragma: no branch
                # Always use component.name for consistency - no special case for single-component
                cube_col_name = comp.name

                component_aliases[comp.name] = cube_col_name
                all_components.append(comp)

    # Build column metadata for the synthetic grain group
    grain_group_columns: list[ColumnMetadata] = []

    # Add dimension columns (short names with role suffix if present)
    dim_short_names = []
    for dim_ref in ctx.dimensions:
        parsed_dim = parse_dimension_ref(dim_ref)
        col_name = parsed_dim.column_name
        if parsed_dim.role:
            col_name = f"{col_name}_{parsed_dim.role}"
        dim_short_names.append(col_name)
        grain_group_columns.append(
            ColumnMetadata(
                name=col_name,
                semantic_name=dim_ref,
                type="string",  # Will be refined by generate_metrics_sql
                semantic_type="dimension",
            ),
        )

    # Add component columns (using cube column names from component_aliases)
    for comp in all_components:
        cube_col_name = component_aliases[comp.name]
        grain_group_columns.append(
            ColumnMetadata(
                name=cube_col_name,
                semantic_name=comp.name,
                type="double",
                semantic_type="metric_component",
            ),
        )

    # Build the synthetic query: SELECT dims, components FROM cube_table
    # No GROUP BY here - generate_metrics_sql will add that
    projection: list[ast.Column] = []

    # Add dimension columns
    for dim_col in dim_short_names:
        projection.append(ast.Column(name=ast.Name(dim_col)))

    # Add component columns (using cube column names)
    for comp in all_components:
        cube_col_name = component_aliases[comp.name]
        projection.append(ast.Column(name=ast.Name(cube_col_name)))

    # Build SELECT ... FROM cube_table
    synthetic_query = ast.Query(
        select=ast.Select(
            projection=projection,  # type: ignore
            from_=ast.From(
                relations=[ast.Relation(primary=ast.Table(ast.Name(table_name)))],
            ),
        ),
        ctes=[],
    )

    # Identify all base metrics (not derived) for the grain group. This includes
    # both directly requested base metrics and base metrics that derived metrics
    # depend on.
    #
    # Note: Derived metrics should not be in grain_group.metrics so they get processed
    # by the derived metrics loop in generate_metrics_sql, which handles PARTITION BY
    # injection for window functions
    base_metrics = []
    for metric_name in decomposed_metrics.keys():
        metric_node = ctx.nodes.get(metric_name)
        if metric_node and not is_derived_metric(ctx, metric_node):
            base_metrics.append(metric_name)

    # Create the synthetic GrainGroupSQL
    # Note: We use a placeholder parent_name since the cube combines multiple parents
    return GrainGroupSQL(
        query=synthetic_query,
        columns=grain_group_columns,
        grain=dim_short_names,
        aggregability=Aggregability.FULL,  # Cube components are pre-aggregated
        metrics=base_metrics,  # Only base metrics, not derived
        parent_name=cube.name,  # Use cube name as parent
        component_aliases=component_aliases,
        is_merged=False,
        components=all_components,
        dialect=ctx.dialect,
    )
