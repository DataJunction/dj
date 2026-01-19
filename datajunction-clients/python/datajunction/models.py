"""Models used by the DJ client."""

import enum
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from datajunction._base import SerializableMixin

if TYPE_CHECKING:  # pragma: no cover
    from datajunction.client import DJClient


@dataclass
class Engine(SerializableMixin):
    """
    Represents an engine
    """

    name: str
    version: Optional[str]


class MetricDirection(str, enum.Enum):
    """
    The direction of the metric that's considered good, i.e., higher is better
    """

    HIGHER_IS_BETTER = "higher_is_better"
    LOWER_IS_BETTER = "lower_is_better"
    NEUTRAL = "neutral"


class MetricUnit(str, enum.Enum):
    """
    Unit
    """

    UNKNOWN = "unknown"
    UNITLESS = "unitless"
    PERCENTAGE = "percentage"
    PROPORTION = "proportion"
    DOLLAR = "dollar"
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"


@dataclass
class MetricMetadata(SerializableMixin):
    """
    Metric metadata output
    """

    direction: MetricDirection | None
    unit: MetricUnit | None
    significant_digits: int | None
    min_decimal_exponent: int | None
    max_decimal_exponent: int | None

    @classmethod
    def from_dict(
        cls,
        dj_client: Optional["DJClient"],
        data: Dict[str, Any],
    ) -> "MetricMetadata":
        """
        Create an instance of the given dataclass `cls` from a dictionary `data`.
        This will handle nested dataclasses and optional types.
        """
        return cls(
            direction=MetricDirection(data["direction"].lower()),
            unit=MetricUnit(data["unit"]["name"].lower()),
            significant_digits=data["significant_digits"],
            min_decimal_exponent=data["min_decimal_exponent"],
            max_decimal_exponent=data["max_decimal_exponent"],
        )


class MaterializationJobType(str, enum.Enum):
    """
    Materialization job types
    """

    SPARK_SQL = "spark_sql"
    DRUID_CUBE = "druid_cube"


class MaterializationStrategy(str, enum.Enum):
    """
    Materialization strategies
    """

    FULL = "full"
    SNAPSHOT = "snapshot"
    INCREMENTAL_TIME = "incremental_time"
    VIEW = "view"


@dataclass
class Materialization(SerializableMixin):
    """
    A node's materialization config
    """

    job: MaterializationJobType
    strategy: MaterializationStrategy
    schedule: str
    config: Dict

    def to_dict(self) -> Dict:
        """
        Convert to a dict
        """
        return {
            "job": self.job.value,
            "strategy": self.strategy.value,
            "schedule": self.schedule,
            "config": self.config,
        }


class NodeMode(str, enum.Enum):
    """
    DJ node's mode
    """

    DRAFT = "draft"
    PUBLISHED = "published"


class NodeStatus(str, enum.Enum):
    """
    DJ node's status
    """

    VALID = "valid"
    INVALID = "invalid"


class NodeType(str, enum.Enum):
    """
    DJ node types
    """

    METRIC = "metric"
    DIMENSION = "dimension"
    SOURCE = "source"
    TRANSFORM = "transform"
    CUBE = "cube"


@dataclass
class ColumnAttribute(SerializableMixin):
    """
    Represents a column attribute
    """

    name: str
    namespace: Optional[str] = "system"

    @classmethod
    def from_dict(
        cls,
        dj_client: Optional["DJClient"],
        data: Dict[str, Any],
    ) -> "ColumnAttribute":
        """
        Create an instance of the given dataclass `cls` from a dictionary `data`.
        This will handle nested dataclasses and optional types.
        """
        return ColumnAttribute(**data["attribute_type"])


@dataclass
class Column(SerializableMixin):
    """
    Represents a column
    """

    name: str
    type: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    attributes: Optional[List[ColumnAttribute]] = None
    dimension: Optional[str] = None
    dimension_column: Optional[str] = None


@dataclass
class UpdateNode(SerializableMixin):  # pylint: disable=too-many-instance-attributes
    """
    Fields for updating a node
    """

    display_name: Optional[str] = None
    description: Optional[str] = None
    mode: Optional[NodeMode] = None
    primary_key: Optional[List[str]] = None
    query: Optional[str] = None
    # this is a problem .... fails many tests
    custom_metadata: Optional[Dict] = None

    # source nodes only
    catalog: Optional[str] = None
    schema_: Optional[str] = None
    table: Optional[str] = None
    columns: Optional[List[Column]] = field(default_factory=list[Column])

    # cube nodes only
    metrics: Optional[List[str]] = None
    dimensions: Optional[List[str]] = None
    filters: Optional[List[str]] = None
    orderby: Optional[List[str]] = None
    limit: Optional[int] = None

    # metric nodes only
    required_dimensions: Optional[List[str]] = None
    metric_metadata: Optional[MetricMetadata] = None


@dataclass
class UpdateTag(SerializableMixin):
    """
    Model for a tag update
    """

    description: Optional[str]
    tag_metadata: Optional[Dict]


class QueryState(str, enum.Enum):
    """
    Different states of a query.
    """

    UNKNOWN = "UNKNOWN"
    ACCEPTED = "ACCEPTED"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    CANCELED = "CANCELED"
    FAILED = "FAILED"

    @classmethod
    def list(cls) -> List[str]:
        """
        List of available query states as strings
        """
        return list(map(lambda c: c.value, cls))  # type: ignore


@dataclass
class AvailabilityState(SerializableMixin):
    """
    Represents the availability state for a node.
    """

    catalog: str
    schema_: Optional[str]
    table: str
    valid_through_ts: int

    min_temporal_partition: Optional[List[str]] = None
    max_temporal_partition: Optional[List[str]] = None


END_JOB_STATES = [QueryState.FINISHED, QueryState.CANCELED, QueryState.FAILED]


# =============================================================================
# Namespace Diff Models
# =============================================================================


class NamespaceDiffChangeType(str, enum.Enum):
    """Type of change in namespace diff"""

    DIRECT = "direct"
    PROPAGATED = "propagated"


@dataclass
class ColumnChange(SerializableMixin):
    """Represents a column change"""

    column: str
    change_type: str  # "added", "removed", "type_changed"
    old_type: Optional[str] = None
    new_type: Optional[str] = None


@dataclass
class NamespaceDiffNodeChange(SerializableMixin):
    """Represents a changed node in namespace diff"""

    name: str
    full_name: str
    node_type: str
    change_type: NamespaceDiffChangeType
    base_version: Optional[str] = None
    compare_version: Optional[str] = None
    base_status: Optional[str] = None
    compare_status: Optional[str] = None
    changed_fields: Optional[List[str]] = None
    column_changes: Optional[List[ColumnChange]] = None
    caused_by: Optional[List[str]] = None
    propagation_reason: Optional[str] = None

    @classmethod
    def from_dict(
        cls,
        dj_client: Optional["DJClient"],
        data: Dict[str, Any],
    ) -> "NamespaceDiffNodeChange":
        column_changes = None
        if data.get("column_changes"):
            column_changes = [
                ColumnChange(
                    column=c["column"],
                    change_type=c["change_type"],
                    old_type=c.get("old_type"),
                    new_type=c.get("new_type"),
                )
                for c in data["column_changes"]
            ]
        return cls(
            name=data["name"],
            full_name=data["full_name"],
            node_type=data["node_type"],
            change_type=NamespaceDiffChangeType(data["change_type"]),
            base_version=data.get("base_version"),
            compare_version=data.get("compare_version"),
            base_status=data.get("base_status"),
            compare_status=data.get("compare_status"),
            changed_fields=data.get("changed_fields", []),
            column_changes=column_changes,
            caused_by=data.get("caused_by", []),
            propagation_reason=data.get("propagation_reason"),
        )


@dataclass
class NamespaceDiffAddedNode(SerializableMixin):
    """Represents an added node"""

    name: str
    full_name: str
    node_type: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    version: Optional[str] = None

    @classmethod
    def from_dict(
        cls,
        dj_client: Optional["DJClient"],
        data: Dict[str, Any],
    ) -> "NamespaceDiffAddedNode":
        return cls(
            name=data["name"],
            full_name=data["full_name"],
            node_type=data["node_type"],
            display_name=data.get("display_name"),
            description=data.get("description"),
            status=data.get("status"),
            version=data.get("version"),
        )


@dataclass
class NamespaceDiffRemovedNode(SerializableMixin):
    """Represents a removed node"""

    name: str
    full_name: str
    node_type: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    version: Optional[str] = None

    @classmethod
    def from_dict(
        cls,
        dj_client: Optional["DJClient"],
        data: Dict[str, Any],
    ) -> "NamespaceDiffRemovedNode":
        return cls(
            name=data["name"],
            full_name=data["full_name"],
            node_type=data["node_type"],
            display_name=data.get("display_name"),
            description=data.get("description"),
            status=data.get("status"),
            version=data.get("version"),
        )


@dataclass
class NamespaceDiff(SerializableMixin):
    """
    Result of comparing two namespaces.

    Provides methods to format the diff for various outputs (markdown, terminal, etc.)
    """

    base_namespace: str
    compare_namespace: str
    added: List[NamespaceDiffAddedNode]
    removed: List[NamespaceDiffRemovedNode]
    direct_changes: List[NamespaceDiffNodeChange]
    propagated_changes: List[NamespaceDiffNodeChange]
    unchanged_count: int
    added_count: int
    removed_count: int
    direct_change_count: int
    propagated_change_count: int

    @classmethod
    def from_dict(
        cls,
        dj_client: Optional["DJClient"],
        data: Dict[str, Any],
    ) -> "NamespaceDiff":
        return cls(
            base_namespace=data["base_namespace"],
            compare_namespace=data["compare_namespace"],
            added=[
                NamespaceDiffAddedNode.from_dict(None, a) for a in data.get("added", [])
            ],
            removed=[
                NamespaceDiffRemovedNode.from_dict(None, r)
                for r in data.get("removed", [])
            ],
            direct_changes=[
                NamespaceDiffNodeChange.from_dict(None, d)
                for d in data.get("direct_changes", [])
            ],
            propagated_changes=[
                NamespaceDiffNodeChange.from_dict(None, p)
                for p in data.get("propagated_changes", [])
            ],
            unchanged_count=data.get("unchanged_count", 0),
            added_count=data.get("added_count", 0),
            removed_count=data.get("removed_count", 0),
            direct_change_count=data.get("direct_change_count", 0),
            propagated_change_count=data.get("propagated_change_count", 0),
        )

    def has_changes(self) -> bool:
        """Returns True if there are any changes between namespaces."""
        return (
            self.added_count > 0
            or self.removed_count > 0
            or self.direct_change_count > 0
            or self.propagated_change_count > 0
        )

    def to_markdown(self) -> str:
        """
        Format the diff as GitHub-flavored markdown.

        Suitable for posting as a PR comment or GitHub Actions summary.
        """
        lines = []

        # Header
        lines.append(
            f"## Namespace Diff: `{self.compare_namespace}` vs `{self.base_namespace}`",
        )
        lines.append("")

        # Summary table
        lines.append("### Summary")
        lines.append("| Category | Count |")
        lines.append("|----------|-------|")
        lines.append(f"| Added | {self.added_count} |")
        lines.append(f"| Removed | {self.removed_count} |")
        lines.append(f"| Direct Changes | {self.direct_change_count} |")
        lines.append(f"| Propagated Changes | {self.propagated_change_count} |")
        lines.append(f"| Unchanged | {self.unchanged_count} |")
        lines.append("")

        # Added nodes
        if self.added:
            lines.append("### Added Nodes")
            lines.append("| Node | Type |")
            lines.append("|------|------|")
            for added_node in self.added:
                lines.append(f"| `{added_node.name}` | {added_node.node_type} |")
            lines.append("")

        # Removed nodes
        if self.removed:
            lines.append("### Removed Nodes")
            lines.append("| Node | Type |")
            lines.append("|------|------|")
            for removed_node in self.removed:
                lines.append(f"| `{removed_node.name}` | {removed_node.node_type} |")
            lines.append("")

        # Direct changes
        if self.direct_changes:
            lines.append("### Direct Changes")
            lines.append("| Node | Type | Changed Fields |")
            lines.append("|------|------|----------------|")
            for change in self.direct_changes:
                fields = ", ".join(change.changed_fields or [])
                lines.append(f"| `{change.name}` | {change.node_type} | {fields} |")
            lines.append("")

            # Column changes detail
            changes_with_columns = [c for c in self.direct_changes if c.column_changes]
            if changes_with_columns:
                lines.append("#### Column Changes")
                for change in changes_with_columns:
                    lines.append(f"**{change.name}**:")
                    for col in change.column_changes or []:  # pragma: no branch
                        if col.change_type == "added":
                            lines.append(f"  - Added: `{col.column}` ({col.new_type})")
                        elif col.change_type == "removed":
                            lines.append(
                                f"  - Removed: `{col.column}` ({col.old_type})",
                            )
                        elif col.change_type == "type_changed":
                            lines.append(
                                f"  - Type changed: `{col.column}` "
                                f"({col.old_type} -> {col.new_type})",
                            )
                lines.append("")

        # Propagated changes
        if self.propagated_changes:
            lines.append("### Propagated Changes")
            lines.append("| Node | Type | Status Change | Caused By |")
            lines.append("|------|------|---------------|-----------|")
            for change in self.propagated_changes:
                status = ""
                if change.base_status and change.compare_status:  # pragma: no branch
                    status = f"{change.base_status} -> {change.compare_status}"
                caused_by = ", ".join(f"`{c}`" for c in (change.caused_by or []))
                lines.append(
                    f"| `{change.name}` | {change.node_type} | {status} | {caused_by} |",
                )
            lines.append("")

        # No changes message
        if not self.has_changes():
            lines.append("*No changes detected between namespaces.*")
            lines.append("")

        return "\n".join(lines)

    def summary(self) -> str:
        """
        Return a brief one-line summary of the diff.
        """
        parts = []
        if self.added_count:
            parts.append(f"+{self.added_count} added")
        if self.removed_count:
            parts.append(f"-{self.removed_count} removed")
        if self.direct_change_count:
            parts.append(f"~{self.direct_change_count} direct changes")
        if self.propagated_change_count:
            parts.append(f"~{self.propagated_change_count} propagated")

        if not parts:
            return "No changes"
        return ", ".join(parts)
