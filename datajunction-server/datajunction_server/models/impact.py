"""
Models for deployment impact analysis.
"""

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

from datajunction_server.models.node import NodeStatus, NodeType


class ColumnChangeType(str, Enum):
    """Types of column changes"""

    ADDED = "added"
    REMOVED = "removed"
    TYPE_CHANGED = "type_changed"
    PARTITION_CHANGED = "partition_changed"


class ColumnChange(BaseModel):
    """Represents a change to a column"""

    column: str
    change_type: ColumnChangeType
    old_type: str | None = None
    new_type: str | None = None


class NodeChangeOperation(str, Enum):
    """Operation being performed on a node"""

    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    NOOP = "noop"


class DimLinkChange(BaseModel):
    """Represents a change to a dimension link"""

    dim_name: str
    operation: Literal["removed", "broken", "added", "updated"]
    # "removed" = user explicitly deleted from YAML
    # "broken"  = implicitly broken; join columns were removed
    # "added"   = new link not previously in the DB
    # "updated" = existing link with changed join_on/join_type/role/default_value
    broken_by_columns: list[str] = Field(default_factory=list)  # only for "broken"


class ImpactType(str, Enum):
    """Type of impact on a downstream node"""

    WILL_INVALIDATE = "will_invalidate"  # Certain to break
    MAY_AFFECT = "may_affect"  # Might need revalidation
    UNCHANGED = "unchanged"  # No predicted impact


class NodeEffect(BaseModel):
    """Unified model for both direct changes and downstream impacts."""

    name: str
    node_type: NodeType
    operation: NodeChangeOperation | None = None  # set for direct; None for downstream
    display_name: str | None = None
    description: str | None = None
    current_status: NodeStatus | None = None  # None if CREATE

    # Level 0 — user intent (direct nodes only)
    changed_fields: list[str] = Field(default_factory=list)

    # Level 1 — derived effects on this node (direct nodes only)
    column_changes: list[ColumnChange] = Field(default_factory=list)
    dim_link_changes: list[DimLinkChange] = Field(default_factory=list)
    validation_errors: list[str] = Field(default_factory=list)

    # Downstream context (downstream nodes only)
    caused_by: list[str] = Field(default_factory=list)
    impact_type: ImpactType | None = None
    predicted_status: NodeStatus | None = None
    impact_reason: str = ""
    depth: int = 0
    is_external: bool = False

    # Internal only — not in API response
    new_query: str | None = Field(default=None, exclude=True)


class DeploymentImpactResponse(BaseModel):
    """Full response for deployment impact analysis"""

    namespace: str

    # Direct changes in this deployment
    changes: list[NodeEffect] = Field(default_factory=list)

    # Summary counts for direct changes
    create_count: int = 0
    update_count: int = 0
    delete_count: int = 0
    skip_count: int = 0

    # Downstream impact (second/third-order effects)
    downstream_impacts: list[NodeEffect] = Field(default_factory=list)

    # Impact summary counts
    will_invalidate_count: int = 0
    may_affect_count: int = 0

    # Warnings about potential issues
    warnings: list[str] = Field(default_factory=list)
