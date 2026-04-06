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


# ---------------------------------------------------------------------------
# Deployment response models
#
# TODO: replace with generated models from OpenAPI spec once client codegen
# is set up. Canonical definitions live in datajunction_server/models/deployment.py.
# ---------------------------------------------------------------------------


@dataclass
class DeploymentResult:
    """A single node-level result within a deployment."""

    name: str
    operation: str
    status: str
    message: str = ""
    changed_fields: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> "DeploymentResult":
        return cls(
            name=d.get("name", ""),
            operation=d.get("operation", ""),
            status=d.get("status", ""),
            message=d.get("message", ""),
            changed_fields=d.get("changed_fields") or [],
        )


@dataclass
class DownstreamImpact:
    """A downstream node that will be affected by a deployment."""

    name: str
    node_type: str
    predicted_status: str
    caused_by: List[str] = field(default_factory=list)
    depth: int = 0
    impact_type: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> "DownstreamImpact":
        return cls(
            name=d.get("name", ""),
            node_type=d.get("node_type", ""),
            predicted_status=d.get("predicted_status", ""),
            caused_by=d.get("caused_by") or [],
            depth=d.get("depth", 0),
            impact_type=d.get("impact_type", ""),
        )


@dataclass
class DeploymentInfo:
    """The full response from a deployment or dry-run impact call."""

    uuid: str
    namespace: str
    status: str
    results: List[DeploymentResult] = field(default_factory=list)
    downstream_impacts: List[DownstreamImpact] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> "DeploymentInfo":
        return cls(
            uuid=d.get("uuid", ""),
            namespace=d.get("namespace", ""),
            status=d.get("status", ""),
            results=[DeploymentResult.from_dict(r) for r in d.get("results", [])],
            downstream_impacts=[
                DownstreamImpact.from_dict(i) for i in d.get("downstream_impacts", [])
            ],
        )
