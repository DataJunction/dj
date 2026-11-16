"""Models for dimension links"""

import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

from datajunction_server.enum import StrEnum
from datajunction_server.models.node_type import NodeNameOutput

# Fallback value for NULLs from an outer join. Renders as a SQL literal
# matching the type of whichever dimension column it wraps.
DimensionLinkDefault = str | int | float | bool


def default_value_key(value: DimensionLinkDefault | None) -> tuple[str, Any]:
    """
    Comparison key for a default value, carrying its type.

    Python counts ``1``, ``1.0`` and ``True`` as equal, but each renders as
    a different SQL literal, so comparing on value alone would miss the
    change and leave a stale default in place.
    """
    return (type(value).__name__, value)


# Type categories a link can set a default for; each covers several column types.
DEFAULT_VALUE_CATEGORIES = (
    "string",
    "integer",
    "decimal",
    "boolean",
    "date",
    "timestamp",
)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_date(value: Any) -> bool:
    try:
        datetime.datetime.strptime(value, "%Y-%m-%d")
    except (TypeError, ValueError):
        return False
    return True


def _is_timestamp(value: Any) -> bool:
    try:
        datetime.datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return False
    return True


_CATEGORY_CHECKS = {
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "decimal": _is_number,
    "boolean": lambda v: isinstance(v, bool),
    "date": _is_date,
    "timestamp": _is_timestamp,
}


def validate_default_values(
    default_value: DimensionLinkDefault | None,
    default_values: dict[str, DimensionLinkDefault] | None,
) -> None:
    """
    Check a link's defaults: the two options are exclusive, keys must be known
    type categories, and each value must fit its category.
    """
    if default_values is None:
        return
    if default_value is not None:
        raise ValueError(
            "default_value and default_values cannot both be set on a dimension "
            "link. Use default_values to set a default per column type.",
        )
    for category, value in default_values.items():
        if category not in _CATEGORY_CHECKS:
            raise ValueError(
                f"Unknown default_values type '{category}'. Must be one of: "
                f"{', '.join(DEFAULT_VALUE_CATEGORIES)}.",
            )
        if not _CATEGORY_CHECKS[category](value):
            raise ValueError(
                f"Invalid default_values entry for '{category}': {value!r} "
                f"is not a valid {category} value.",
            )


def default_values_key(
    values: dict[str, DimensionLinkDefault] | None,
) -> tuple[Any, ...] | None:
    """Order-independent comparison key for default_values, keeping value types."""
    if values is None:
        return None
    return tuple(
        sorted(
            (category, default_value_key(value)) for category, value in values.items()
        ),
    )


class JoinCardinality(StrEnum):
    """
    The version upgrade type
    """

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class SparkJoinStrategy(StrEnum):
    """
    Spark SQL join strategy hint
    """

    BROADCAST = "broadcast"
    MERGE = "merge"
    SHUFFLE_HASH = "shuffle_hash"
    SHUFFLE_REPLICATE_NL = "shuffle_replicate_nl"


class JoinType(StrEnum):
    """
    Join type
    """

    LEFT = "left"
    RIGHT = "right"
    INNER = "inner"
    FULL = "full"
    CROSS = "cross"


class LinkType(StrEnum):
    """
    There are two types of dimensions links supported: join links or reference links
    """

    JOIN = "join"
    REFERENCE = "reference"


def missing_join_on_message(node_name: str, dimension_node: str) -> str:
    """Error text for a join link with no join_on."""
    return (
        f"Dimension link from {node_name} to {dimension_node} has no join_on "
        "clause. Set join_on to the equality between this node's foreign key "
        "column(s) and the dimension's primary key."
    )


def misplaced_node_column_message(node_name: str, dimension_node: str) -> str:
    """Error text for node_column on a join link."""
    return (
        f"Dimension link from {node_name} to {dimension_node} sets node_column, "
        "which only applies to reference links. Express the join in join_on instead."
    )


class LinkDimensionIdentifier(BaseModel):
    """
    Input for linking a dimension to a node
    """

    dimension_node: str
    role: str | None = None


class JoinLinkInput(BaseModel):
    """
    Input for creating a join link between a dimension node and node
    """

    dimension_node: str
    join_type: JoinType | None = JoinType.LEFT
    join_on: str | None = None
    join_cardinality: JoinCardinality | None = JoinCardinality.MANY_TO_ONE
    role: str | None = None
    default_value: DimensionLinkDefault | None = None
    default_values: dict[str, DimensionLinkDefault] | None = None
    spark_hints: SparkJoinStrategy | None = None

    @model_validator(mode="after")
    def check_default_values(self):
        validate_default_values(self.default_value, self.default_values)
        return self


class LinkDimensionOutput(BaseModel):
    """
    Input for linking a dimension to a node
    """

    dimension: NodeNameOutput
    join_type: JoinType
    join_sql: str
    join_cardinality: JoinCardinality | None = None
    role: str | None = None
    foreign_keys: dict[str, str | None]
    default_value: DimensionLinkDefault | None = None
    default_values: dict[str, DimensionLinkDefault] | None = None
    spark_hints: SparkJoinStrategy | None = None

    model_config = ConfigDict(from_attributes=True)
