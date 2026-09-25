"""Models for metric reaggregation declarations."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from datajunction_server.enum import StrEnum


class ReaggregationFunction(StrEnum):
    """
    Supported functions for metric reaggregation.
    """

    AUTO = "auto"
    NONE = "none"
    SUM = "sum"
    AVG = "avg"
    LAST_VALUE = "last_value"
    FIRST_VALUE = "first_value"
    MIN = "min"
    MAX = "max"
    # Quantile sketch family. Unlike the functions above it does not describe a
    # rollup arithmetic; it selects how a quantile metric is accumulated, merged
    # and read back, which is what makes percentiles pre-aggregatable at all.
    TDIGEST = "tdigest"


DIMENSION_REAGGREGATE_FUNCTIONS = frozenset(
    {
        ReaggregationFunction.LAST_VALUE,
        ReaggregationFunction.FIRST_VALUE,
        ReaggregationFunction.MIN,
        ReaggregationFunction.MAX,
    },
)


PARAMETERIZED_REAGGREGATE_FUNCTIONS: frozenset[ReaggregationFunction] = frozenset(
    {
        # compression: centroids retained, trading sketch size for tail accuracy.
        ReaggregationFunction.TDIGEST,
    },
)


def is_parameterized_reaggregate_function(
    function: ReaggregationFunction | None,
) -> bool:
    """
    Return whether a function accepts tuning parameters in `params`.
    """
    return function in PARAMETERIZED_REAGGREGATE_FUNCTIONS


def is_supported_dimension_reaggregate_function(
    function: ReaggregationFunction,
) -> bool:
    """
    Return whether a function can collapse across a protected dimension.
    """
    return function in DIMENSION_REAGGREGATE_FUNCTIONS


def unsupported_dimension_reaggregate_functions(
    spec: "ReaggregateSpec | dict | None",
) -> list[str]:
    """
    Unsupported dimension-specific collapse functions in a reaggregate spec.
    """
    reaggregate = parse_reaggregate_spec(spec)
    if not reaggregate:
        return []
    return sorted(
        {
            rule.fn.value
            for rule in reaggregate.rules
            if not is_supported_dimension_reaggregate_function(rule.fn)
        },
    )


class DimensionReaggregateRule(BaseModel):
    """
    Dimension-specific metric reaggregation rule.
    """

    model_config = ConfigDict(extra="forbid")

    dimension: str
    fn: ReaggregationFunction


class ReaggregateSpec(BaseModel):
    """
    Declaration for how a metric rolls up from its accumulation grain.
    """

    model_config = ConfigDict(extra="forbid")

    # A sketch family selects an alternate decomposition for the metric's
    # aggregate expression. Ordinary dimension-specific behavior remains in
    # `rules` below.
    fn: ReaggregationFunction | None = None
    rules: list[DimensionReaggregateRule] = Field(default_factory=list)

    # Tuning parameters for sketch-backed aggregation/merge functions. The
    # materialization adapter validates the supported keys for its aggregator.
    params: dict[str, Any] | None = None


def dump_reaggregate_spec(
    spec: ReaggregateSpec | dict | None,
) -> dict | None:
    """
    Return a JSON-serializable reaggregate spec dictionary.
    """
    if spec is None:
        return None
    if isinstance(spec, ReaggregateSpec):
        return spec.model_dump(mode="json")
    return ReaggregateSpec.model_validate(spec).model_dump(mode="json")


def parse_reaggregate_spec(
    spec: ReaggregateSpec | dict | None,
) -> ReaggregateSpec | None:
    """
    Return a validated reaggregate spec model.
    """
    if spec is None:
        return None
    if isinstance(spec, ReaggregateSpec):
        return spec
    return ReaggregateSpec.model_validate(spec)


def dimension_reaggregate_rules(
    spec: ReaggregateSpec | dict | None,
) -> list[DimensionReaggregateRule]:
    """
    Return the dimension-specific reaggregation rules from a spec.
    """
    reaggregate = parse_reaggregate_spec(spec)
    return reaggregate.rules if reaggregate else []
