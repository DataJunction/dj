"""
Branch management API endpoints.

Enables creating, listing, and deleting branch namespaces that are linked
to git branches for the git-backed workflow.
"""

import logging
from http import HTTPStatus
from typing import List

from fastapi import Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import get_node_namespace
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJAlreadyExistsException,
    DJInvalidInputException,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    AccessChecker,
    AccessDenialMode,
    get_access_checker,
)
from datajunction_server.internal.git import GitHubService
from datajunction_server.internal.git.github_service import GitHubServiceError
from datajunction_server.internal.nodes import copy_nodes_to_namespace
from datajunction_server.models.access import ResourceAction
from datajunction_server.models.deployment import DeploymentResult
from datajunction_server.utils import get_current_user, get_session

_logger = logging.getLogger(__name__)
router = SecureAPIRouter(tags=["branches"])


class CreateBranchRequest(BaseModel):
    """Request to create a new branch namespace."""

    branch_name: str  # e.g., "feature-x"


class BranchInfo(BaseModel):
    """Information about a branch namespace."""

    namespace: str  # e.g., "myproject.feature_x"
    git_branch: str  # e.g., "feature-x"
    parent_namespace: str  # e.g., "myproject.main"
    github_repo_path: str  # e.g., "owner/repo"


class CreateBranchResult(BaseModel):
    """Result of creating a branch."""

    branch: BranchInfo
    deployment_results: List[DeploymentResult]


@router.post(
    "/namespaces/{namespace}/branches",
    response_model=CreateBranchResult,
    status_code=HTTPStatus.CREATED,
    name="Create a branch namespace",
)
async def create_branch(
    namespace: str,
    request: CreateBranchRequest,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> CreateBranchResult:
    """
    Create a new branch namespace from a parent namespace.

    This creates both:
    1. A git branch in the configured repository
    2. A DJ namespace linked to that git branch

    Preconditions:
    - Parent namespace must have github_repo_path configured
    - GITHUB_SERVICE_TOKEN must be set in environment

    The new namespace will inherit:
    - github_repo_path from parent
    - git_path from parent
    - git_branch = the new branch name
    - parent_namespace = the parent's namespace name
    """
    access_checker.add_namespace(namespace, ResourceAction.WRITE)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Get parent namespace and validate it has git config
    parent_ns = await get_node_namespace(session, namespace)

    if not parent_ns.github_repo_path:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' does not have git configured. "
            "Set github_repo_path first.",
        )

    if not parent_ns.git_branch:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' does not have a git branch configured. "
            "Set git_branch first.",
        )

    # Validate branch name
    branch_name = request.branch_name.strip()
    if not branch_name:
        raise DJInvalidInputException(message="Branch name cannot be empty.")

    # Convert branch name to namespace: feature-x -> feature_x
    branch_namespace_suffix = branch_name.replace("-", "_").replace("/", "_")

    # Construct new namespace name
    # If parent is "myproject.main", new namespace is "myproject.feature_x"
    parent_parts = namespace.rsplit(".", 1)
    if len(parent_parts) > 1:
        new_namespace = f"{parent_parts[0]}.{branch_namespace_suffix}"
    else:
        new_namespace = f"{namespace}.{branch_namespace_suffix}"

    # Check if namespace already exists
    existing = await NodeNamespace.get(
        session,
        new_namespace,
        raise_if_not_exists=False,
    )
    if existing:
        raise DJAlreadyExistsException(
            message=f"Namespace '{new_namespace}' already exists.",
        )

    # Create git branch via GitHub API
    try:
        github = GitHubService()
        await github.create_branch(
            repo_path=parent_ns.github_repo_path,
            branch=branch_name,
            from_ref=parent_ns.git_branch,
        )
        _logger.info(
            "Created git branch '%s' from '%s' in repo '%s'",
            branch_name,
            parent_ns.git_branch,
            parent_ns.github_repo_path,
        )
    except GitHubServiceError as e:
        _logger.error("Failed to create git branch: %s", e)
        raise DJInvalidInputException(
            message=f"Failed to create git branch '{branch_name}': {e.message}",
        ) from e

    # Create DJ namespace
    new_ns = NodeNamespace(
        namespace=new_namespace,
        github_repo_path=parent_ns.github_repo_path,
        git_branch=branch_name,
        git_path=parent_ns.git_path,
        parent_namespace=namespace,
    )
    session.add(new_ns)
    await session.commit()

    _logger.info(
        "Created branch namespace '%s' linked to git branch '%s'",
        new_namespace,
        branch_name,
    )

    # Copy all nodes from parent namespace to new branch namespace
    deployment_results = await copy_nodes_to_namespace(
        session=session,
        source_namespace=namespace,
        target_namespace=new_namespace,
        current_user=current_user,
    )

    return CreateBranchResult(
        branch=BranchInfo(
            namespace=new_namespace,
            git_branch=branch_name,
            parent_namespace=namespace,
            github_repo_path=parent_ns.github_repo_path,
        ),
        deployment_results=deployment_results,
    )


@router.get(
    "/namespaces/{namespace}/branches",
    response_model=List[BranchInfo],
    name="List branch namespaces",
)
async def list_branches(
    namespace: str,
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> List[BranchInfo]:
    """
    List all branch namespaces that were created from this namespace.

    Returns namespaces where parent_namespace equals the given namespace.
    """
    access_checker.add_namespace(namespace, ResourceAction.READ)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Verify parent namespace exists
    parent_ns = await get_node_namespace(session, namespace)

    # Query child namespaces
    stmt = select(NodeNamespace).where(NodeNamespace.parent_namespace == namespace)
    result = await session.execute(stmt)
    child_namespaces = result.scalars().all()

    return [
        BranchInfo(
            namespace=ns.namespace,
            git_branch=ns.git_branch or "",
            parent_namespace=namespace,
            github_repo_path=ns.github_repo_path or parent_ns.github_repo_path or "",
        )
        for ns in child_namespaces
    ]


@router.delete(
    "/namespaces/{namespace}/branches/{branch_namespace}",
    name="Delete a branch namespace",
)
async def delete_branch(
    namespace: str,
    branch_namespace: str,
    delete_git_branch: bool = True,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> JSONResponse:
    """
    Delete a branch namespace.

    This will:
    1. Delete all nodes in the branch namespace
    2. Delete the namespace record
    3. Delete the git branch (unless delete_git_branch=False)

    Args:
        namespace: Parent namespace
        branch_namespace: The branch namespace to delete
        delete_git_branch: If True (default), also delete the git branch in GitHub
    """
    access_checker.add_namespace(namespace, ResourceAction.WRITE)
    access_checker.add_namespace(branch_namespace, ResourceAction.WRITE)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Verify parent namespace exists
    await get_node_namespace(session, namespace)

    # Get the branch namespace
    branch_ns = await get_node_namespace(session, branch_namespace)

    # Verify it's actually a child of the parent
    if branch_ns.parent_namespace != namespace:
        raise DJInvalidInputException(
            message=f"Namespace '{branch_namespace}' is not a branch of '{namespace}'.",
        )

    # Delete the git branch
    git_branch_deleted = False
    if delete_git_branch and branch_ns.github_repo_path and branch_ns.git_branch:
        try:
            github = GitHubService()
            await github.delete_branch(
                repo_path=branch_ns.github_repo_path,
                branch=branch_ns.git_branch,
            )
            git_branch_deleted = True
            _logger.info(
                "Deleted git branch '%s' in repo '%s'",
                branch_ns.git_branch,
                branch_ns.github_repo_path,
            )
        except GitHubServiceError as e:
            _logger.warning("Failed to delete git branch: %s", e)
            # Don't fail the request - the branch might already be deleted

    # Delete all nodes in the branch namespace
    from datajunction_server.database.node import Node

    nodes_query = select(Node).where(
        or_(
            Node.namespace == branch_namespace,
            Node.namespace.like(f"{branch_namespace}.%"),
        ),
    )
    result = await session.execute(nodes_query)
    nodes_to_delete = result.scalars().all()
    nodes_deleted = len(nodes_to_delete)

    for node in nodes_to_delete:
        await session.delete(node)

    # Delete the namespace record
    await session.delete(branch_ns)

    # Also delete any child namespaces
    child_ns_query = select(NodeNamespace).where(
        or_(
            NodeNamespace.namespace == branch_namespace,
            NodeNamespace.namespace.like(f"{branch_namespace}.%"),
        ),
    )
    child_result = await session.execute(child_ns_query)
    for child_ns in child_result.scalars().all():
        if child_ns.namespace != branch_namespace:  # Already deleted above
            await session.delete(child_ns)

    await session.commit()

    _logger.info(
        "Deleted branch namespace '%s' (parent: '%s', nodes: %d, git_branch: %s)",
        branch_namespace,
        namespace,
        nodes_deleted,
        git_branch_deleted,
    )

    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={
            "message": f"Branch namespace '{branch_namespace}' deleted",
            "nodes_deleted": nodes_deleted,
            "git_branch_deleted": git_branch_deleted,
        },
    )
