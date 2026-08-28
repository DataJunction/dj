"""Models for metric reaggregation declarations."""

from pydantic import BaseModel, Field

from datajunction_server.enum import StrEnum


class ReaggregationFunction(StrEnum):
    """
    Supported functions for metric reaggregation.
    """

    AUTO = "auto"
    NONE = "none"
    SUM = "sum"
    AVG = "avg"
    WEIGHTED_AVG = "weighted_avg"
    LAST_VALUE = "last_value"
    FIRST_VALUE = "first_value"
    MIN = "min"
    MAX = "max"


class DimensionReaggregateRule(BaseModel):
    """
    Dimension-specific metric reaggregation rule.
    """

    dimension: str
    fn: ReaggregationFunction


class ReaggregateSpec(BaseModel):
    """
    Declaration for how a metric rolls up from its accumulation grain.
    """

    fn: ReaggregationFunction | None = None
    weight: str | None = None
    rules: list[DimensionReaggregateRule] = Field(default_factory=list)


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
