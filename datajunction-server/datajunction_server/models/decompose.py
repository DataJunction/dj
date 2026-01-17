"""
Models for metric decomposition.

This module defines the data models used when decomposing metrics into their
constituent components for pre-aggregation and rollup.

Key concepts:
- MetricComponent: A single measure with accumulate/merge phases
- DecomposedMetric: A metric broken into components + combiner expression
"""

from typing import List

from pydantic import BaseModel

from datajunction_server.enum import StrEnum


class Aggregability(StrEnum):
    """
    Type of allowed aggregation for a given metric component.
    """

    FULL = "full"
    LIMITED = "limited"
    NONE = "none"


class MetricRef(BaseModel):
    """Reference to a metric with name and display name."""

    name: str
    display_name: str | None = None


class AggregationRule(BaseModel):
    """
    The aggregation rule for the metric component.

    If the Aggregability type is LIMITED, the `level` should be specified to
    highlight the level at which the metric component needs to be aggregated
    in order to support the specified aggregation function.

    Example for COUNT(DISTINCT user_id):
        It can be decomposed into a single metric component with LIMITED
        aggregability, i.e., it is only aggregatable if the component is
        calculated at the `user_id` level:

        MetricComponent(
            name="num_users",
            expression="DISTINCT user_id",
            aggregation="COUNT",
            rule=AggregationRule(type=LIMITED, level=["user_id"])
        )
    """

    type: Aggregability = Aggregability.NONE
    level: list[str] | None = None


class MetricComponent(BaseModel):
    """
    A reusable, named building block of a metric definition.

    A MetricComponent represents a SQL expression that can serve as an input
    to building a metric. It supports a two-phase aggregation model:

    - Phase 1 (Accumulate): Build from raw data using `aggregation`
      Can be a function name ("SUM") or a template ("SUM(POWER({}, 2))")

    - Phase 2 (Merge): Combine pre-aggregated values using `merge` function
      Examples: SUM, SUM (for COUNT), hll_union_agg

    For most aggregations, accumulate and merge use the same function (SUM → SUM).
    For COUNT, merge is SUM (sum up the counts).
    For HLL sketches, they differ: hll_sketch_estimate vs hll_union_agg.

    The final expression combining merged components is specified in
    DecomposedMetric.combiner.

    Attributes:
        name: A unique name for the component, derived from its expression.
        expression: The raw SQL expression (column/value) being aggregated.
        aggregation: Function name or template for Phase 1. Simple cases use
                     just the name ("SUM"), complex cases use templates with
                     {} placeholder ("SUM(POWER({}, 2))").
        merge: The function name for combining pre-aggregated values (Phase 2).
        rule: Aggregation rules defining how/when the component can be aggregated.
    """

    name: str
    expression: str
    aggregation: str | None  # Phase 1: function name or template
    merge: str | None = None  # Phase 2: merge function name
    rule: AggregationRule


class PreAggMeasure(MetricComponent):
    """
    A metric component stored in a pre-aggregation.

    Extends MetricComponent with an expression hash for identity matching.
    This allows finding pre-aggs that contain the same measure even if
    the component name differs.
    """

    expr_hash: str | None = None  # Hash of expression for identity matching
    used_by_metrics: list[MetricRef] | None = None  # Metrics that use this measure


class DecomposedMetric(BaseModel):
    """
    A metric decomposed into its constituent components with a combining expression.

    This is the result of decomposing a metric query. It specifies:
    - components: The measures needed for pre-aggregation
    - combiner: How to combine merged components into the final metric value
    - derived_query: The full SQL query using the combiner

    Examples:
        SUM metric:
            components: [{name: "revenue_sum", aggregation: "SUM", merge: "SUM"}]
            combiner: "SUM(revenue_sum)"

        AVG metric:
            components: [
                {name: "revenue_sum", aggregation: "SUM", merge: "SUM"},
                {name: "revenue_count", aggregation: "COUNT", merge: "SUM"}
            ]
            combiner: "SUM(revenue_sum) / SUM(revenue_count)"

        APPROX_COUNT_DISTINCT metric (uses Spark function names):
            components: [{name: "user_hll", aggregation: "hll_sketch_agg", merge: "hll_union"}]
            combiner: "hll_sketch_estimate(hll_union(user_hll))"
    """

    components: List[MetricComponent]
    combiner: str  # Expression combining merged components into final value
    derived_query: str | None = None  # The full derived query as string
