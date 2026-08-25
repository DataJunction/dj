"""
Pre-aggregation matching logic for SQL generation.

This module provides functions to find pre-aggregations that can satisfy
a grain group's requirements, enabling SQL substitution when materialized
tables are available.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datajunction_server.construction.build_v3.dimensions import (
    parse_dimension_ref,
    roles_reaching_dimension,
)
from datajunction_server.database.preaggregation import (
    PreAggregation,
    compute_expression_hash,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import Aggregability, MetricComponent
from datajunction_server.models.dimensionlink import JoinType
from datajunction_server.models.preaggregation import TemporalPartitionColumn
from datajunction_server.naming import SEPARATOR

if TYPE_CHECKING:
    from datajunction_server.construction.build_v3.types import (
        BuildContext,
        GrainGroup,
        JoinPath,
        ResolvedDimension,
    )
    from datajunction_server.database.column import Column
    from datajunction_server.database.dimensionlink import DimensionLink
    from datajunction_server.database.node import Node

logger = logging.getLogger(__name__)


def get_required_measure_identities(
    grain_group: GrainGroup,
) -> set[tuple[str, str]]:
    """
    Identity of each measure a grain group needs: (expression hash, Phase-1
    aggregation).

    Matching is name-independent, so ``expr_hash`` stands in for identity -- but
    it covers the expression alone, making ``SUM(x)`` and ``MAX(x)`` hash alike
    and letting a MAX metric read a SUM column. Pairing it with the aggregation
    restores the distinction: a partial is reusable only by a metric that
    accumulates the same way.

    Phase-1 ``aggregation`` rather than ``merge``, because merge is not unique --
    COUNT accumulates with COUNT but merges with SUM.
    """
    return {
        (
            compute_expression_hash(component.expression),
            component.normalized_aggregation,
        )
        for _, component in grain_group.components
    }


def required_semi_additive_grain(
    ctx: BuildContext,
    node_rev_id: int,
    grain_group: GrainGroup,
) -> set[str]:
    """
    Protected dimensions a pre-agg must retain for final semi-additive collapse.

    ``semi_additive_component_dimensions`` is populated only when the requested
    output grain omitted the protected dimension. In that case a pre-agg at the
    output grain would have already collapsed away the key needed by MAX_BY /
    MIN_BY / min / max in the metrics layer, so it is not substitutable.
    """
    return {
        canonical_dimension_ref(ctx, node_rev_id, dimension_ref)
        for dimension_ref in grain_group.semi_additive_component_dimensions.values()
    }


def canonical_dimension_ref(
    ctx: BuildContext,
    node_rev_id: int,
    ref: str,
) -> str:
    """
    Canonical form of a dimension reference, so that a pre-agg registered with one
    spelling still matches a query written with the other.

    A bare reference is qualified only when exactly one named role reaches the
    dimension and no role-free link does -- mirroring how the reference resolves,
    which prefers the role-free link. Anything else is returned unchanged.
    """
    try:
        dim_ref = parse_dimension_ref(ref)
    except DJInvalidInputException:
        # Not a node.column reference, so there is no dimension node to look up
        # roles for. Compared as-is rather than failing the whole build.
        return ref
    if dim_ref.role:
        return ref
    roles = roles_reaching_dimension(ctx, node_rev_id, dim_ref.node_name)
    if "" in roles or len(roles) != 1:
        return ref
    return f"{ref}[{roles.pop()}]"


@dataclass(frozen=True)
class JoinBackCoverage:
    """
    A requested dimension reachable by joining a dimension the pre-agg keyed on.

    The pre-agg's grain holds a dimension's key rather than the attribute asked
    for, so the attribute is one join away: read the pre-agg, join the dimension
    on the retained key, and group by the attribute.

    ``key_refs`` are the canonical grain entries holding that key -- the columns
    the join reads on the pre-agg side.
    """

    dimension: ResolvedDimension
    key_refs: tuple[str, ...]
    # The one link reaching the dimension; is_join_back_safe established there is
    # exactly one, so consumers don't re-derive it or re-assert the invariant.
    link: DimensionLink


@dataclass(frozen=True)
class PreaggMatch:
    """
    A pre-aggregation chosen for a grain group, with the joins it needs.

    ``join_back`` is empty for the common case where the stored grain covers the
    request outright.
    """

    preagg: PreAggregation
    join_back: tuple[JoinBackCoverage, ...] = ()


def is_join_back_safe(join_path: JoinPath | None) -> bool:
    """
    Whether joining a dimension onto a retained key is safe to re-aggregate over.

    A single link whose join is on the dimension's primary key is many-to-one, so
    it cannot multiply the measures. Multi-hop paths are refused for now: each
    additional link is only safe if it too cannot fan out, and nothing checks
    that yet.

    The join type matters too: a RIGHT or FULL link makes the *dimension* the
    preserved side, so a predicate on the pre-agg's own columns would have to be
    absorbed into the join rather than applied after it -- machinery the source
    path has and this one doesn't.

    This is the seam for asking each link on the path whether it can fan out --
    ``DimensionLink.join_cardinality`` already records that, but it defaults to
    ``MANY_TO_ONE`` for every link nobody annotated, so trusting it today would
    assume safety rather than establish it. Once it is reliably populated this
    should test the path's links rather than count them, which also lifts the
    single-hop restriction.
    """
    if join_path is None or len(join_path.links) != 1:
        return False
    return join_path.links[0].join_type in (None, JoinType.LEFT, JoinType.INNER)


def join_back_coverage(
    ctx: BuildContext,
    node_rev_id: int,
    resolved_dimensions: list[ResolvedDimension],
    preagg_grain_set: set[str],
) -> list[JoinBackCoverage] | None:
    """
    How each requested dimension the pre-agg lacks could be reached by a join.

    Returns one entry per dimension that needs a join, or ``None`` when any
    requested dimension can be reached no other way -- in which case the pre-agg
    cannot serve the query at all.

    A dimension qualifies only when the grain retains its *whole* primary key at
    the same role. Joining on part of a composite key, or on a column that merely
    happens to be present, can match several dimension rows per pre-aggregated
    row and multiply the measures.
    """
    coverage: list[JoinBackCoverage] = []
    for rdim in resolved_dimensions:
        if (
            canonical_dimension_ref(ctx, node_rev_id, rdim.original_ref)
            in preagg_grain_set
        ):
            continue

        # A locally-owned column has no dimension node to join, and neither does
        # a reference that named no node at all.
        dim_node = ctx.nodes.get(rdim.node_name)
        if not dim_node or not dim_node.current:
            return None

        primary_key = [col.name for col in dim_node.current.primary_key()]
        if not primary_key or rdim.column_name in primary_key:
            # No key to join on, or the request *is* the key -- which would have
            # matched directly, so reaching here means the grain lacks it.
            return None

        suffix = f"[{rdim.role}]" if rdim.role else ""
        key_refs = tuple(
            canonical_dimension_ref(
                ctx,
                node_rev_id,
                f"{rdim.node_name}{SEPARATOR}{key_col}{suffix}",
            )
            for key_col in primary_key
        )
        if not set(key_refs).issubset(preagg_grain_set):
            return None

        if not is_join_back_safe(rdim.join_path):
            return None
        assert rdim.join_path is not None  # narrowed by is_join_back_safe

        coverage.append(
            JoinBackCoverage(
                dimension=rdim,
                key_refs=key_refs,
                link=rdim.join_path.links[0],
            ),
        )
    return coverage


def find_matching_preagg(
    ctx: BuildContext,
    parent_node: Node,
    resolved_dimensions: list[ResolvedDimension],
    grain_group: GrainGroup,
) -> PreaggMatch | None:
    """
    Find a pre-aggregation that can satisfy the grain group requirements.

    Matching rules:
    1. Pre-agg must be for the same parent node (same node_revision_id)
    2. Pre-agg grain must be a SUPERSET of requested grain (can roll up but not drill down)
    3. Pre-agg measures must be a SUPERSET of required measures (by expr_hash)
    4. Pre-agg must have availability (already checked in load_available_preaggs)

    Args:
        ctx: Build context with available_preaggs cache
        parent_node: The parent node for this grain group
        resolved_dimensions: The requested dimensions, resolved to join paths
        grain_group: The grain group with required components

    Returns:
        The chosen ``PreaggMatch`` if one was found, None otherwise
    """
    if not ctx.use_materialized:
        return None

    if not parent_node.current:
        return None

    node_rev_id = parent_node.current.id

    # Get available pre-aggs for this parent node
    available = ctx.available_preaggs.get(node_rev_id, [])
    if not available:
        return None

    required_measures = get_required_measure_identities(grain_group)
    if not required_measures:
        return None

    semi_additive_grain = required_semi_additive_grain(
        ctx,
        node_rev_id,
        grain_group,
    )

    requested_grain_set = {
        canonical_dimension_ref(ctx, node_rev_id, rdim.original_ref)
        for rdim in resolved_dimensions
    }

    # Non-additive measures (e.g. COUNT(DISTINCT ...), raw AVG components) cannot
    # be rolled up to a coarser grain without producing wrong results. When any
    # required component is not fully additive, a pre-agg can only satisfy the
    # query if its grain EXACTLY matches the requested grain (no roll-up). Fully
    # additive measures may still be rolled up via a superset (subset) match.
    requires_exact_grain = any(
        component.rule.type != Aggregability.FULL
        for _, component in grain_group.components
    )

    # Find a matching pre-agg. Ranked by joins first, then grain size: a pre-agg
    # that covers the request directly always beats one needing a join back, even
    # when the joining one is at a coarser grain.
    candidates: list[tuple[tuple[int, int], PreaggMatch]] = []

    for preagg in available:
        preagg_grain_set = {
            canonical_dimension_ref(ctx, node_rev_id, ref)
            for ref in (preagg.grain_columns or [])
        }

        if not semi_additive_grain.issubset(preagg_grain_set):
            logger.debug(
                f"[BuildV3] Pre-agg {preagg.id} grain {preagg_grain_set} "
                f"doesn't retain semi-additive protected grain "
                f"{semi_additive_grain}",
            )
            continue

        # Check grain compatibility. For additive measures the pre-agg may be at
        # the same or a finer grain (roll-up allowed → subset match). For
        # non-additive measures the pre-agg grain must exactly match the request.
        if requires_exact_grain and requested_grain_set != preagg_grain_set:
            logger.debug(
                f"[BuildV3] Pre-agg {preagg.id} grain {preagg_grain_set} "
                f"must exactly match requested grain {requested_grain_set} "
                f"because the grain group has non-additive measures",
            )
            continue

        # Non-additive measures never reach a join back: exact grain demands set
        # equality, and needing a join means the request isn't in the grain.
        join_back: list[JoinBackCoverage] = []
        if not requires_exact_grain and not requested_grain_set.issubset(
            preagg_grain_set,
        ):
            # The grain may still reach the request by joining a dimension whose
            # key it retained.
            coverage = join_back_coverage(
                ctx,
                node_rev_id,
                resolved_dimensions,
                preagg_grain_set,
            )
            if coverage is None:
                logger.debug(
                    f"[BuildV3] Pre-agg {preagg.id} grain {preagg_grain_set} "
                    f"doesn't cover requested grain {requested_grain_set}",
                )
                continue
            join_back = coverage

        # Coverage check on (expression hash, Phase-1 aggregation) -- see
        # get_required_measure_identities for why the aggregation is part of it.
        preagg_measures = {
            (measure.expr_hash, measure.normalized_aggregation)
            for measure in preagg.measures
            if measure.expr_hash
        }
        if not required_measures.issubset(preagg_measures):
            logger.debug(
                f"[BuildV3] Pre-agg {preagg.id} measures {preagg_measures} "
                f"don't cover required measures {required_measures}",
            )
            continue

        logger.debug(
            f"[BuildV3] Pre-agg {preagg.id} is a candidate "
            f"(grain={preagg_grain_set}, measures={len(preagg_measures)}, "
            f"joins={len(join_back)})",
        )
        candidates.append(
            (
                (len(join_back), len(preagg_grain_set)),
                PreaggMatch(preagg=preagg, join_back=tuple(join_back)),
            ),
        )

    # Fewest joins first, then smallest grain: a pre-agg covering the request
    # outright always beats one needing a join, and among equals the one closest
    # to the requested grain is the least roll-up work.
    best_match = min(candidates, key=lambda c: c[0])[1] if candidates else None
    if best_match:
        logger.info(
            f"[BuildV3] Using pre-agg {best_match.preagg.id} for "
            f"parent={parent_node.name} grain={requested_grain_set}",
        )

    return best_match


def get_preagg_measure_column(
    preagg: PreAggregation,
    component: MetricComponent,
) -> str | None:
    """
    Find the column name in the pre-agg that corresponds to a metric component.

    Matches on the full identity -- expression hash and Phase-1 aggregation -- so
    the right column is found even when names differ. One pre-agg can hold several
    measures over the same expression (SUM(x) and MAX(x)), where matching on the
    hash alone would return whichever came first.

    Args:
        preagg: The pre-aggregation to search
        component: The metric component to find

    Returns:
        Column name in the pre-agg, or None if not found
    """
    target = (
        compute_expression_hash(component.expression),
        component.normalized_aggregation,
    )

    for measure in preagg.measures:
        if (measure.expr_hash, measure.normalized_aggregation) == target:
            # Externally-registered pre-aggs bind the measure to a physical
            # column name that may differ from the DJ component name.
            return measure.source_column or measure.name

    return None


def get_preagg_dimension_column(
    ctx: BuildContext,
    node_rev_id: int,
    preagg: PreAggregation,
    dimension_ref: str,
    default_column: str,
) -> str:
    """
    Physical column in the pre-agg backing a resolved grain dimension.

    External pre-aggs may store a dimension under a different column name (kept as
    source_column). Matched by canonical dimension reference, so a bare and a
    role-qualified spelling of the same dimension still match, falling back to the
    DJ column name.
    """
    target = canonical_dimension_ref(ctx, node_rev_id, dimension_ref)
    for col in preagg.columns or []:
        if col.semantic_type == "dimension" and target == canonical_dimension_ref(
            ctx,
            node_rev_id,
            col.semantic_name,
        ):
            return col.source_column or col.name
    return default_column


def match_temporal_columns_to_grain(
    preagg: PreAggregation,
) -> list[tuple[Column, str | None]]:
    """
    Each temporal partition column of a pre-agg's parent, paired with the grain
    entry it corresponds to (``None`` when the column isn't part of the grain).

    A temporal column reaches the grain three ways: named directly by the parent,
    reached through a dimension link the column feeds, or through the column's own
    dimension reference. Callers derive what they need from the matched ref --
    ``get_temporal_partitions`` an output column name, freshness gating the axis
    ``valid_through_ts`` describes.
    """
    node_revision = preagg.node_revision
    if not node_revision:  # pragma: no cover - defensive
        return []

    grain_columns = preagg.grain_columns or []
    temporal_columns = node_revision.temporal_partition_columns()
    if not temporal_columns:
        return []

    def grain_ref_under(dim_node: str, dim_attr: str | None = None) -> str | None:
        """First grain entry belonging to a dimension node."""
        return next(
            (
                grain_col
                for grain_col in grain_columns
                if grain_col == dim_attr or grain_col.startswith(dim_node + SEPARATOR)
            ),
            None,
        )

    # A source column can feed several dimension links, so this is multi-valued:
    # keeping only one would miss the attribute actually named in the grain. Built
    # lazily -- it parses each link's join SQL, which is wasted whenever the grain
    # names the parent's own column.
    col_to_dims: dict[str, list[str]] | None = None

    matches: list[tuple[Column, str | None]] = []
    for temporal_col in temporal_columns:
        direct_ref = f"{node_revision.name}{SEPARATOR}{temporal_col.name}"
        if direct_ref in grain_columns:
            matches.append((temporal_col, direct_ref))
            continue

        if col_to_dims is None:
            col_to_dims = {}
            for (
                dim_attr,
                source_col,
            ) in node_revision.dimensions_to_columns_map().items():
                source_name = source_col.identifier().split(SEPARATOR)[-1]
                col_to_dims.setdefault(source_name, []).append(dim_attr)

        matched_ref = next(
            (
                ref
                for dim_attr in col_to_dims.get(temporal_col.name, [])
                if (ref := grain_ref_under(dim_attr.rsplit(SEPARATOR, 1)[0], dim_attr))
            ),
            None,
        )
        if matched_ref is None and temporal_col.dimension:
            matched_ref = grain_ref_under(temporal_col.dimension.name)
        matches.append((temporal_col, matched_ref))
    return matches


def temporal_output_name(temporal_col: Column, grain_ref: str | None) -> str:
    """
    Output column name for a temporal partition column.

    Named from the grain entry it matched -- ``v3.date.week[order]`` becomes
    ``week_order`` -- falling back to the source column name when the column
    isn't in the grain.
    """
    if grain_ref is None:
        return temporal_col.name
    parsed = parse_dimension_ref(grain_ref)
    if parsed.role:
        return f"{parsed.column_name}_{parsed.role}"
    return parsed.column_name


def get_temporal_partitions(preagg: PreAggregation) -> list[TemporalPartitionColumn]:
    """
    Get temporal partition columns for a pre-aggregation.
    """
    temporal_partitions: list[TemporalPartitionColumn] = []
    for temporal_col, grain_ref in match_temporal_columns_to_grain(preagg):
        source_type = str(temporal_col.type) if temporal_col.type else "int"
        output_name = temporal_output_name(temporal_col, grain_ref)
        logger.info(
            "Temporal partition: source=%s -> output=%s (type=%s)",
            temporal_col.name,
            output_name,
            source_type,
        )
        partition = temporal_col.partition
        temporal_partitions.append(
            TemporalPartitionColumn(
                column_name=output_name,
                column_type=source_type,
                format=partition.format if partition else None,
                granularity=(
                    str(partition.granularity.value)
                    if partition and partition.granularity
                    else None
                ),
                expression=(
                    str(partition.temporal_expression()) if partition else None
                ),
            ),
        )
    return temporal_partitions
