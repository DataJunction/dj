"""Models for metric filter requirements."""

from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class FilterRequirement(BaseModel):
    """
    One entry in a metric's required or recommended filters.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    # A filter on any listed attribute satisfies the entry.
    dimension: tuple[str, ...]
    # A filter on one of the attributes, applied when the caller sends none.
    default: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _string_to_list(cls, data: Any) -> Any:
        if isinstance(data, dict) and isinstance(data.get("dimension"), str):
            return {**data, "dimension": [data["dimension"]]}
        return data

    @field_validator("dimension")
    @classmethod
    def _not_empty(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("dimension needs at least one attribute")
        return value


def parse_filter_requirements(
    value: "Sequence[FilterRequirement | dict] | None",
) -> list[FilterRequirement] | None:
    """
    Return validated entries, or None when the field is unset.
    """
    if value is None:
        return None
    return [
        item
        if isinstance(item, FilterRequirement)
        else FilterRequirement.model_validate(item)
        for item in value
    ]


def dump_filter_requirements(
    value: "Sequence[FilterRequirement | dict] | None",
) -> list[dict] | None:
    """
    Return JSON-ready entries, leaving out an unset default.
    """
    entries = parse_filter_requirements(value)
    if entries is None:
        return None
    return [entry.model_dump(mode="json", exclude_none=True) for entry in entries]


def filter_requirements_identity(
    value: "Sequence[FilterRequirement | dict] | None",
) -> frozenset[tuple[frozenset[str], str | None]]:
    """
    Order-insensitive form used to compare two lists of entries.
    """
    return frozenset(
        (frozenset(entry.dimension), entry.default)
        for entry in parse_filter_requirements(value) or []
    )


def attribute_node_name(attribute: str) -> str:
    """
    Node name of `node.column` or `node.column[role]`.
    """
    return attribute.split("[", 1)[0].rsplit(".", 1)[0]
