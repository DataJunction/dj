"""
Models for deployment impact analysis.
"""

from enum import Enum

from pydantic import BaseModel, Field

from datajunction_server.models.node import NodeStatus, NodeType


class ColumnChangeType(str, Enum):
    """Types of column changes"""

    ADDED = "added"
    REMOVED = "removed"
    TYPE_CHANGED = "type_changed"


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


class NodeChange(BaseModel):
    """Represents a direct change to a node in the deployment"""

    name: str
    operation: NodeChangeOperation
    node_type: NodeType
    display_name: str | None = None
    description: str | None = None
    current_status: NodeStatus | None = None  # None if CREATE

    # For UPDATEs: what changed
    changed_fields: list[str] = Field(default_factory=list)
    column_changes: list[ColumnChange] = Field(default_factory=list)


class ImpactType(str, Enum):
    """Type of impact on a downstream node"""

    WILL_INVALIDATE = "will_invalidate"  # Certain to break
    MAY_AFFECT = "may_affect"  # Might need revalidation
    UNCHANGED = "unchanged"  # No predicted impact


class DownstreamImpact(BaseModel):
    """Predicted impact on a downstream node"""

    name: str
    node_type: NodeType
    current_status: NodeStatus
    predicted_status: NodeStatus
    impact_type: ImpactType
    impact_reason: str  # Human-readable explanation
    depth: int  # Hops from the changed node
    caused_by: list[str] = Field(default_factory=list)  # Which changed nodes cause this
    is_external: bool = False  # True if outside the deployment namespace


class DeploymentImpactResponse(BaseModel):
    """Full response for deployment impact analysis"""

    namespace: str

    # Direct changes in this deployment
    changes: list[NodeChange] = Field(default_factory=list)

    # Summary counts for direct changes
    create_count: int = 0
    update_count: int = 0
    delete_count: int = 0
    skip_count: int = 0

    # Downstream impact (second/third-order effects)
    downstream_impacts: list[DownstreamImpact] = Field(default_factory=list)

    # Impact summary counts
    will_invalidate_count: int = 0
    may_affect_count: int = 0

    # Warnings about potential issues
    warnings: list[str] = Field(default_factory=list)


# =============================================================================
# Namespace Diff Models
# =============================================================================


class NamespaceDiffChangeType(str, Enum):
    """Type of change detected in namespace diff"""

    DIRECT = "direct"  # User-provided fields changed
    PROPAGATED = "propagated"  # Only system-derived fields changed (status, version)


class NamespaceDiffNodeChange(BaseModel):
    """Represents a changed node in namespace diff"""

    name: str  # Node name without namespace prefix
    full_name: str  # Full node name with namespace
    node_type: NodeType
    change_type: NamespaceDiffChangeType

    # Version info
    base_version: str | None = None
    compare_version: str | None = None

    # Status info
    base_status: NodeStatus | None = None
    compare_status: NodeStatus | None = None

    # For direct changes: which user-provided fields changed
    changed_fields: list[str] = Field(default_factory=list)
    column_changes: list[ColumnChange] = Field(default_factory=list)

    # For propagated changes: what caused it
    caused_by: list[str] = Field(default_factory=list)
    propagation_reason: str | None = None


class NamespaceDiffAddedNode(BaseModel):
    """Represents a node that exists only in the compare namespace"""

    name: str  # Node name without namespace prefix
    full_name: str  # Full node name with namespace
    node_type: NodeType
    display_name: str | None = None
    description: str | None = None
    status: NodeStatus | None = None
    version: str | None = None


class NamespaceDiffRemovedNode(BaseModel):
    """Represents a node that exists only in the base namespace"""

    name: str  # Node name without namespace prefix
    full_name: str  # Full node name with namespace
    node_type: NodeType
    display_name: str | None = None
    description: str | None = None
    status: NodeStatus | None = None
    version: str | None = None


class NamespaceDiffResponse(BaseModel):
    """Response for namespace diff comparison"""

    base_namespace: str
    compare_namespace: str

    # Nodes that exist only in compare namespace (added in compare)
    added: list[NamespaceDiffAddedNode] = Field(default_factory=list)

    # Nodes that exist only in base namespace (removed in compare)
    removed: list[NamespaceDiffRemovedNode] = Field(default_factory=list)

    # Nodes with direct changes (user-provided fields differ)
    direct_changes: list[NamespaceDiffNodeChange] = Field(default_factory=list)

    # Nodes with propagated changes (only system-derived fields differ)
    propagated_changes: list[NamespaceDiffNodeChange] = Field(default_factory=list)

    # Nodes that are identical in both namespaces
    unchanged_count: int = 0

    # Summary counts
    added_count: int = 0
    removed_count: int = 0
    direct_change_count: int = 0
    propagated_change_count: int = 0
