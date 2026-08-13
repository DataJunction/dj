from datetime import datetime

from pydantic import BaseModel, Field

from datajunction_server.typing import StrEnum


class BranchInfo(BaseModel):
    """Information about a branch namespace."""

    namespace: str  # e.g., "myproject.feature_x"
    git_branch: str  # e.g., "feature-x"
    parent_namespace: str  # e.g., "myproject.main"
    github_repo_path: str  # e.g., "owner/repo"
    num_nodes: int = 0
    invalid_node_count: int = 0
    git_only: bool = False
    last_updated_at: datetime | None = None


class ImpactedNode(BaseModel):
    name: str
    caused_by: list[str]


class ImpactedNodes(BaseModel):
    downstreams: list[ImpactedNode]
    links: list[ImpactedNode]


class HardDeleteResponse(BaseModel):
    deleted_nodes: list[str]
    deleted_namespaces: list[str]
    impacted: ImpactedNodes

    # Materialization workflows the query service refused to stop. The nodes are
    # gone either way -- an unreachable query service does not undo a delete -- but
    # DJ no longer holds what those workflows are named, so a failure here is the
    # last chance anyone has to hear about it.
    materialization_failures: list[str] = []


class NamespaceWriteStatus(StrEnum):
    """What a create-or-reactivate namespace request ended up doing."""

    CREATED = "created"
    REACTIVATED = "reactivated"
    ALREADY_EXISTS = "already_exists"


class NamespaceWriteResult(BaseModel):
    """
    Outcome of creating or reactivating a namespace.

    ``namespaces`` holds the namespaces created (parents included when
    requested), or the single namespace that was reactivated / already existed.
    """

    status: NamespaceWriteStatus
    namespaces: list[str]


class NamespaceProvisionRequest(BaseModel):
    """Principals that receive roles when a namespace boundary is provisioned."""

    owner_group: str = Field(min_length=1)
    deployer_service_accounts: list[str] = Field(default_factory=list)


class NamespaceProvisionResponse(BaseModel):
    """Managed roles for a provisioned namespace boundary."""

    namespace: str
    owner_role: str
    deployer_role: str
