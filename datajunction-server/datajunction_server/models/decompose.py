"""
Models for metric decomposition.

This module defines the data models used when decomposing metrics into their
constituent components for pre-aggregation and rollup.

Key concepts:
- MetricComponent: A single measure with accumulate/merge phases
- DecomposedMetric: A metric broken into components + combiner expression
"""

from typing import Any

from pydantic import BaseModel, Field

from datajunction_server.enum import StrEnum
from datajunction_server.models.materialization import MaterializationTarget
from datajunction_server.models.reaggregate import DimensionReaggregateRule


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
    reaggregate: DimensionReaggregateRule | None = None


class MetricComponent(BaseModel):
    """A reusable, named building block of a metric definition."""

    name: str
    expression: str
    aggregation: str | None  # Phase 1: function name or template
    merge: str | None = None  # Phase 2: merge function name
    rule: AggregationRule
    # SQL alias to use for the grain column in measures SQL (LIMITED components only).
    # For simple column grains (e.g. COUNT(DISTINCT order_id)) this is the column name
    # ("order_id"); for complex expressions it is component.name so the identifier stays
    # valid and consistent with what decompose.py computed.
    grain_alias: str | None = None
    # Tuning parameters for `aggregation`/`merge`, for sketch-backed components
    # whose function name alone doesn't pin down the accumulate (a t-digest still
    # needs a compression, a KLL a `k`). Part of measure identity -- see
    # `measure_identity_token` -- because a sketch built at one accuracy must not
    # satisfy a query asking for another.
    params: dict[str, Any] | None = None
    # Fixed arguments appended after the column in the Phase 2 merge call, as SQL
    # literals: `nflx_tdigest_agg(col, 200.0)` is merge="nflx_tdigest_agg" plus
    # merge_args=["200.0"]. `merge` stays a bare function name because two things
    # match on it -- the Druid aggregator lookup key and the semi-additive rewrite
    # in `_replace_reaggregate_merge_expression` -- so the tuning cannot be folded
    # into it as a template. Rendered from `params` by the decomposition; `params`
    # remains the identity record, this is the emission form.
    merge_args: list[str] = Field(default_factory=list)

    # Conversion applied when writing this component to a materialized table for
    # a target listed in `serialize_targets` -- see `ComponentDef.serialize`. The
    # value is a template expanded against the accumulated expression.
    serialize: str | None = None
    # A list rather than a set: this model is dumped straight to JSON in cube
    # materialization configs, and `json.dumps` cannot encode a set. StrEnum
    # members are fine -- they subclass `str`.
    serialize_targets: list[MaterializationTarget] = Field(default_factory=list)
    # Column type `serialize` produces. See `ComponentDef.serialize_type`.
    serialize_type: str | None = None

    def serializes_for(self, materialization_target: Any | None) -> bool:
        """
        Whether this component converts its accumulated value for `target`.

        Two things key off this and they must agree: the SQL that writes the
        column, and the type recorded for it. If they disagree the measures
        table holds one representation while the catalog claims another, and the
        Druid aggregator lookup -- which keys on the column type -- silently
        finds nothing and drops the measure.
        """
        return bool(
            self.serialize
            and materialization_target is not None
            and materialization_target in self.serialize_targets,
        )

    @property
    def normalized_aggregation(self) -> str:
        """
        Phase-1 aggregation function, normalized for identity comparison.

        A measure is identified by (expression, aggregation), not expression
        alone. Matching, column binding and registration all compare this, so
        normalization lives with the model rather than at each call site.
        """
        return (self.aggregation or "").strip().upper()


class PreAggMeasure(MetricComponent):
    """
    A metric component stored in a pre-aggregation.

    Extends MetricComponent with an expression hash for identity matching.
    This allows finding pre-aggs that contain the same measure even if
    the component name differs.
    """

    expr_hash: str | None = None  # Hash of expression for identity matching
    used_by_metrics: list[MetricRef] | None = None  # Metrics that use this measure
    # Physical column holding this measure in an externally-registered pre-agg
    # table. None for DJ-managed pre-aggs (the component name IS the column).
    source_column: str | None = None


class DecomposedMetric(BaseModel):
    """A metric decomposed into its constituent components with a combining expression."""

    components: list[MetricComponent]
    combiner: str  # Expression combining merged components into final value
    derived_query: str | None = None  # The full derived query as string
