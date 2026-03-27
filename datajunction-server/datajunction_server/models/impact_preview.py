"""
Models for the impact preview feature.

Used by both the single-node preview endpoint and the branch merge preview endpoint.
"""

from typing import Literal

from pydantic import BaseModel, Field

from datajunction_server.models.node import NodeStatus
from datajunction_server.models.node_type import NodeType


class NodeChange(BaseModel):
    """What changed on a node — input to compute_impact and output of diff_namespaces."""

    is_deleted: bool = False
    columns_added: list[str] = Field(default_factory=list)
    columns_removed: list[str] = Field(default_factory=list)
    columns_changed: list[tuple[str, str, str]] = Field(
        default_factory=list,
    )  # (name, from_type, to_type)
    dim_links_removed: list[str] = Field(
        default_factory=list,
    )  # dimension node names whose links were removed
    dim_links_added: list[str] = Field(
        default_factory=list,
    )  # dimension node names newly linked (no downstream impact)


class NodeDiff(BaseModel):
    """Describes how a single node changed between branch and main."""

    name: str
    node_type: NodeType
    change_type: Literal["modified", "deleted", "added"]
    diff: NodeChange


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


class SingleNodePreviewResponse(BaseModel):
    """Response for POST /nodes/{name}/impact-preview."""

    node_diff: NodeDiff
    downstream_impact: list[ImpactedNode]  # topo-sorted
