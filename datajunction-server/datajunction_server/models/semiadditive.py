"""Models for semi-additive metric declarations."""

from pydantic import BaseModel

from datajunction_server.enum import StrEnum


class SemiAdditiveCollapseFunction(StrEnum):
    """
    Supported collapse functions for semi-additive metrics.
    """

    LAST_VALUE = "last_value"
    FIRST_VALUE = "first_value"
    MIN = "min"
    MAX = "max"


class SemiAdditiveSpec(BaseModel):
    """
    Declaration that a metric must collapse, rather than sum, across a dimension.
    """

    dimension: str
    function: SemiAdditiveCollapseFunction


def dump_semi_additive_spec(
    spec: SemiAdditiveSpec | dict | None,
) -> dict | None:
    """
    Return a JSON-serializable semi-additive spec dictionary.
    """
    if spec is None:
        return None
    if isinstance(spec, SemiAdditiveSpec):
        return spec.model_dump(mode="json")
    return SemiAdditiveSpec.model_validate(spec).model_dump(mode="json")


def parse_semi_additive_spec(
    spec: SemiAdditiveSpec | dict | None,
) -> SemiAdditiveSpec | None:
    """
    Return a validated semi-additive spec model.
    """
    if spec is None:
        return None
    if isinstance(spec, SemiAdditiveSpec):
        return spec
    return SemiAdditiveSpec.model_validate(spec)
