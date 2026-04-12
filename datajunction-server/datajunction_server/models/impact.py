"""
Models for deployment downstream impact analysis.
"""

from enum import Enum

from pydantic import BaseModel, Field

from datajunction_server.models.node import NodeStatus, NodeType


class ImpactType(str, Enum):
    """Type of impact on a downstream node"""

    WILL_INVALIDATE = "will_invalidate"  # Certain to break
    WILL_RECOVER = "will_recover"  # Was INVALID, will become VALID
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
