"""
Models for the impact preview feature.
"""

from typing import Literal

from pydantic import BaseModel, Field

from datajunction_server.models.node import NodeStatus
from datajunction_server.models.node_type import NodeType


class ImpactedNode(BaseModel):
    """A downstream node that would be affected by the proposed changes."""

    name: str
    node_type: NodeType
    namespace: str
    current_status: NodeStatus
    projected_status: NodeStatus
    reason: str
    caused_by: list[str] = Field(default_factory=list)
    impact_type: Literal["column", "dimension_link", "deleted_parent"]
