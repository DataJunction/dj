"""
Build V3: Clean SQL Generation

This module provides a clean-slate reimplementation of SQL generation that:
- Separates measures (pre-aggregated) from metrics (fully computed)
- Properly handles metric decomposition and derived metrics
- Supports explicit hierarchies and inferred dimension link chains
- Respects aggregability rules and required dimensions

Materialization optimization hierarchy:
1. Find cube with availability -> query directly (most optimal)
2. Find pre-agg tables -> roll up with merge functions
3. Compute from source tables (fallback)
"""

from datajunction_server.construction.build_v3.builder import (
    build_measures_sql,
    build_metrics_sql,
)
from datajunction_server.construction.build_v3.cube_matcher import (
    build_sql_from_cube,
    find_matching_cube,
    resolve_dialect_and_engine_for_metrics,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    ColumnMetadata,
    GeneratedMeasuresSQL,
    GeneratedSQL,
    GrainGroupSQL,
    ResolvedExecutionContext,
)
from datajunction_server.construction.build_v3.alias_registry import AliasRegistry
from datajunction_server.construction.build_v3.combiners import (
    build_combiner_sql,
    CombinedGrainGroupResult,
    validate_grain_groups_compatible,
)

__all__ = [
    # Main entry points
    "build_measures_sql",
    "build_metrics_sql",
    # Cube matching (Layer 1)
    "find_matching_cube",
    "build_sql_from_cube",
    "resolve_dialect_and_engine_for_metrics",
    # Combiners
    "build_combiner_sql",
    "CombinedGrainGroupResult",
    "validate_grain_groups_compatible",
    # Context and types
    "BuildContext",
    "GeneratedSQL",
    "GeneratedMeasuresSQL",
    "GrainGroupSQL",
    "ColumnMetadata",
    "ResolvedExecutionContext",
    # Alias registry
    "AliasRegistry",
]
