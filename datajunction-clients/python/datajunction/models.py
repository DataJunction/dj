"""Models used by the DJ client."""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypeAlias

from datajunction._base import SerializableMixin

if TYPE_CHECKING:  # pragma: no cover
    from datajunction.client import DJClient


@dataclass
class Engine(SerializableMixin):
    """
    Represents an engine
    """

    name: str
    version: str | None


@dataclass
class ColumnYAML(SerializableMixin):
    """
    Represents a column
    """

    name: str
    type: str
    display_name: str | None = None
    description: str | None = None
    attributes: list[str] | None = None


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
        dj_client: DJClient | None,
        data: dict[str, Any],
    ) -> MetricMetadata:
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
    config: dict

    def to_dict(self) -> dict:
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
    namespace: str | None = "system"

    @classmethod
    def from_dict(
        cls,
        dj_client: DJClient | None,
        data: dict[str, Any],
    ) -> ColumnAttribute:
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
    display_name: str | None = None
    description: str | None = None
    attributes: list[ColumnAttribute] | None = None
    dimension: str | None = None
    dimension_column: str | None = None


@dataclass
class UpdateNode(SerializableMixin):  # pylint: disable=too-many-instance-attributes
    """
    Fields for updating a node
    """

    display_name: str | None = None
    description: str | None = None
    mode: NodeMode | None = None
    primary_key: list[str] | None = None
    query: str | None = None
    # this is a problem .... fails many tests
    custom_metadata: dict | None = None

    # source nodes only
    catalog: str | None = None
    schema_: str | None = None
    table: str | None = None
    columns: list[Column] | None = field(default_factory=list[Column])

    # cube nodes only
    metrics: list[str] | None = None
    dimensions: list[str] | None = None
    filters: list[str] | None = None
    orderby: list[str] | None = None
    limit: int | None = None

    # metric nodes only
    required_dimensions: list[str] | None = None
    metric_metadata: MetricMetadata | None = None


@dataclass
class UpdateTag(SerializableMixin):
    """
    Model for a tag update
    """

    description: str | None
    tag_metadata: dict | None


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
    def list(cls) -> list[str]:
        """
        List of available query states as strings
        """
        return [c.value for c in cls]  # type: ignore


@dataclass
class AvailabilityState(SerializableMixin):
    """
    Represents the availability state for a node.
    """

    catalog: str
    schema_: str | None
    table: str
    valid_through_ts: int

    min_temporal_partition: list[str] | None = None
    max_temporal_partition: list[str] | None = None


END_JOB_STATES = [QueryState.FINISHED, QueryState.CANCELED, QueryState.FAILED]


# ---------------------------------------------------------------------------
# Git config and branch models
# ---------------------------------------------------------------------------


@dataclass
class GitConfig(SerializableMixin):
    """
    Git configuration for a namespace, enabling git-backed branch management.
    """

    github_repo_path: str | None = None
    git_branch: str | None = None
    git_path: str | None = None
    default_branch: str | None = None
    parent_namespace: str | None = None
    git_only: bool | None = None

    @classmethod
    def from_dict(
        cls,
        dj_client: DJClient | None,
        data: dict[str, Any],
    ) -> GitConfig:
        """Create a GitConfig from a dictionary."""
        return cls(
            github_repo_path=data.get("github_repo_path"),
            git_branch=data.get("git_branch"),
            git_path=data.get("git_path"),
            default_branch=data.get("default_branch"),
            parent_namespace=data.get("parent_namespace"),
            git_only=data.get("git_only"),
        )


@dataclass
class BranchInfo(SerializableMixin):
    """Information about a branch namespace."""

    namespace: str
    git_branch: str
    parent_namespace: str
    github_repo_path: str
    num_nodes: int = 0
    invalid_node_count: int = 0
    git_only: bool = False
    last_updated_at: str | None = None

    @classmethod
    def from_dict(
        cls,
        dj_client: DJClient | None,
        data: dict[str, Any],
    ) -> BranchInfo:
        """Create a BranchInfo from a dictionary."""
        return cls(
            namespace=data["namespace"],
            git_branch=data["git_branch"],
            parent_namespace=data["parent_namespace"],
            github_repo_path=data["github_repo_path"],
            num_nodes=data.get("num_nodes", 0),
            invalid_node_count=data.get("invalid_node_count", 0),
            git_only=data.get("git_only", False),
            last_updated_at=data.get("last_updated_at"),
        )


# ---------------------------------------------------------------------------
# Deployment response models
#
# TODO: replace with generated models from OpenAPI spec once client codegen
# is set up. Canonical definitions live in datajunction_server/models/deployment.py.
# ---------------------------------------------------------------------------


@dataclass
class SemanticFingerprint:
    """A semantic node digest returned by the server."""

    digest: str
    version: Literal[1] = 1

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError(
                f"Unsupported semantic fingerprint version: {self.version}",
            )
        if re.fullmatch(r"[0-9a-f]{64}", self.digest) is None:
            raise ValueError(
                "Semantic fingerprint digest must be 64 lowercase hexadecimal characters",
            )

    @classmethod
    def from_dict(cls, d: dict) -> SemanticFingerprint:
        return cls(
            version=d.get("version", 1),
            digest=d.get("digest", ""),
        )


SemanticFingerprintValue: TypeAlias = SemanticFingerprint | Literal["unknown"]


def _parse_semantic_fingerprint(value: Any) -> SemanticFingerprintValue | None:
    if value is None or value == "unknown":
        return value
    if not isinstance(value, dict):
        raise ValueError("Semantic fingerprint must be an object or 'unknown'")
    return SemanticFingerprint.from_dict(value)


@dataclass
class DeploymentResult:
    """A single node-level result within a deployment."""

    name: str
    operation: str
    status: str
    message: str = ""
    changed_fields: list[str] = field(default_factory=list)
    deploy_type: str = ""
    change_tier: Literal["none", "minor", "major"] | None = None
    semantic_fingerprint: SemanticFingerprintValue | None = None

    @classmethod
    def from_dict(cls, d: dict) -> DeploymentResult:
        fingerprint = d.get("semantic_fingerprint")
        return cls(
            name=d.get("name", ""),
            operation=d.get("operation", ""),
            status=d.get("status", ""),
            message=d.get("message", ""),
            changed_fields=d.get("changed_fields") or [],
            deploy_type=d.get("deploy_type", ""),
            change_tier=d.get("change_tier"),
            semantic_fingerprint=_parse_semantic_fingerprint(fingerprint),
        )


@dataclass
class DownstreamImpact:
    """A downstream node that will be affected by a deployment."""

    name: str
    node_type: str
    predicted_status: str
    caused_by: list[str] = field(default_factory=list)
    depth: int = 0
    impact_type: str = ""
    semantic_fingerprint: SemanticFingerprintValue | None = None

    @classmethod
    def from_dict(cls, d: dict) -> DownstreamImpact:
        fingerprint = d.get("semantic_fingerprint")
        return cls(
            name=d.get("name", ""),
            node_type=d.get("node_type", ""),
            predicted_status=d.get("predicted_status", ""),
            caused_by=d.get("caused_by") or [],
            depth=d.get("depth", 0),
            impact_type=d.get("impact_type", ""),
            semantic_fingerprint=_parse_semantic_fingerprint(fingerprint),
        )


@dataclass
class DeploymentInfo:
    """The full response from a deployment or dry-run impact call."""

    uuid: str
    namespace: str
    status: str
    results: list[DeploymentResult] = field(default_factory=list)
    downstream_impacts: list[DownstreamImpact] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> DeploymentInfo:
        return cls(
            uuid=d.get("uuid", ""),
            namespace=d.get("namespace", ""),
            status=d.get("status", ""),
            results=[DeploymentResult.from_dict(r) for r in d.get("results", [])],
            downstream_impacts=[
                DownstreamImpact.from_dict(i) for i in d.get("downstream_impacts", [])
            ],
        )
