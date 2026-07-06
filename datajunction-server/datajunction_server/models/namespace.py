from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel


class BranchInfo(BaseModel):
    """Information about a branch namespace."""

    namespace: str  # e.g., "myproject.feature_x"
    git_branch: str  # e.g., "feature-x"
    parent_namespace: str  # e.g., "myproject.main"
    github_repo_path: str  # e.g., "owner/repo"
    num_nodes: int = 0
    invalid_node_count: int = 0
    git_only: bool = False
    last_updated_at: Optional[datetime] = None


class ImpactedNode(BaseModel):
    name: str
    caused_by: List[str]


class ImpactedNodes(BaseModel):
    downstreams: List[ImpactedNode]
    links: List[ImpactedNode]


class HardDeleteResponse(BaseModel):
    deleted_nodes: list[str]
    deleted_namespaces: list[str]
    impacted: ImpactedNodes
