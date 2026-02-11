"""
Branch management API endpoints.

Enables creating, listing, and deleting branch namespaces that are linked
to git branches for the git-backed workflow.
"""

import asyncio
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
from datajunction_server.database.node import Node
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
from datajunction_server.internal.git.github_service import GitHubService
from datajunction_server.internal.git.github_service import GitHubServiceError
from datajunction_server.internal.namespaces import (
    resolve_git_config,
    validate_sibling_relationship,
)
from datajunction_server.internal.nodes import copy_nodes_to_namespace
from datajunction_server.models.access import ResourceAction
from datajunction_server.models.deployment import DeploymentResult
from datajunction_server.utils import SEPARATOR, get_current_user, get_session

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


async def _create_git_branch(
    repo_path: str,
    branch_name: str,
    from_ref: str,
) -> None:
    """Create a git branch via GitHub API."""
    github = GitHubService()
    await github.create_branch(
        repo_path=repo_path,
        branch=branch_name,
        from_ref=from_ref,
    )
    _logger.info(
        "Created git branch '%s' from '%s' in repo '%s'",
        branch_name,
        from_ref,
        repo_path,
    )


async def _create_namespace_and_copy_nodes(
    session: AsyncSession,
    new_namespace: str,
    branch_name: str,
    root_namespace: NodeNamespace,
    current_user: User,
) -> List[DeploymentResult]:
    """Create DJ namespace and copy nodes from appropriate source.

    Args:
        root_namespace: The git root namespace that the branch is being created from.

    If creating from a git root (has default_branch but no git_branch), copies from
    the default_branch namespace and links to the git root as parent.
    Otherwise copies from the branch namespace and links to the sibling parent.
    """
    # Determine parent namespace for the link (used for PR targeting)
    # - Git root: link to the git root itself (e.g., "demo.metrics")
    # - Branch: link to shared parent by removing last segment (e.g., "demo.main" -> "demo")
    if root_namespace.default_branch and not root_namespace.git_branch:
        # Creating from git root - new branch is a child
        parent_namespace = root_namespace.namespace
    else:
        # Creating from branch - new branch is a sibling
        parent_namespace = (
            root_namespace.namespace.rsplit(SEPARATOR, 1)[0]
            if SEPARATOR in root_namespace.namespace
            else root_namespace.namespace
        )

    # Determine source namespace to copy from
    if root_namespace.default_branch and not root_namespace.git_branch:
        # Creating from git root - copy from parent.default_branch
        source_namespace = f"{root_namespace.namespace}{SEPARATOR}{root_namespace.default_branch.replace('-', '_').replace('/', '_')}"
        _logger.info(
            "Copying from default branch namespace '%s' (creating from git root)",
            source_namespace,
        )
    else:
        # Creating from branch namespace - copy from it directly
        source_namespace = root_namespace.namespace
        _logger.info(
            "Copying from source namespace '%s' (creating from branch)",
            source_namespace,
        )

    # Validate sibling relationship
    validate_sibling_relationship(new_namespace, parent_namespace)

    # Ensure parent namespace exists before creating child
    # This prevents foreign key violations
    parent_ns = await NodeNamespace.get(
        session,
        parent_namespace,
        raise_if_not_exists=False,
    )
    if not parent_ns:
        # Parent doesn't exist - create it as a git root placeholder
        # Copy git config from the source namespace and set default_branch
        # to the source's git_branch so PRs can resolve correctly
        github_repo_path, git_path, _ = await resolve_git_config(
            session,
            root_namespace.namespace,
        )
        parent_ns = NodeNamespace(
            namespace=parent_namespace,
            github_repo_path=github_repo_path,
            git_path=git_path,
            default_branch=root_namespace.git_branch,  # Use source's branch as default
        )
        session.add(parent_ns)
        await session.flush()  # Ensure it's in DB before creating child

    # If the source namespace doesn't have this parent set, link it
    # This ensures the source and its branches all belong to the same family
    if (
        root_namespace.git_branch
        and root_namespace.parent_namespace != parent_namespace
    ):
        root_namespace.parent_namespace = parent_namespace
        session.add(root_namespace)
        await session.flush()

    # Create DJ namespace
    # Note: We don't copy github_repo_path or git_path - those are inherited
    # from parent via resolve_git_config(). Only set branch and parent link.
    new_ns = NodeNamespace(
        namespace=new_namespace,
        git_branch=branch_name,
        parent_namespace=parent_namespace,
    )
    session.add(new_ns)
    await session.commit()

    _logger.info(
        "Created branch namespace '%s' linked to git branch '%s'",
        new_namespace,
        branch_name,
    )

    # Copy all nodes from source namespace to new branch namespace
    deployment_results = await copy_nodes_to_namespace(
        session=session,
        source_namespace=source_namespace,
        target_namespace=new_namespace,
        current_user=current_user,
    )
    return deployment_results


async def _cleanup_git_branch(repo_path: str, branch_name: str) -> None:
    """Delete a git branch via GitHub API (best effort)."""
    try:
        github = GitHubService()
        await github.delete_branch(repo_path=repo_path, branch=branch_name)
        _logger.info("Cleaned up git branch '%s' in repo '%s'", branch_name, repo_path)
    except Exception as e:
        _logger.warning(
            "Failed to cleanup git branch '%s': %s (may need manual cleanup)",
            branch_name,
            e,
        )


async def _cleanup_namespace_and_nodes(
    session: AsyncSession,
    namespace: str,
) -> None:
    """Delete namespace and all its nodes (best effort)."""
    try:
        # Delete all nodes
        nodes_query = select(Node).where(
            or_(
                Node.namespace == namespace,
                Node.namespace.like(f"{namespace}.%"),
            ),
        )
        result = await session.execute(nodes_query)
        nodes_to_delete = result.scalars().all()
        for node in nodes_to_delete:
            await session.delete(node)

        # Delete namespace
        ns = await NodeNamespace.get(session, namespace, raise_if_not_exists=False)
        if ns:  # pragma: no branch
            await session.delete(ns)

        await session.commit()
        _logger.info(
            "Cleaned up namespace '%s' and %d nodes",
            namespace,
            len(nodes_to_delete),
        )
    except Exception as e:  # pragma: no cover
        _logger.warning(
            "Failed to cleanup namespace '%s': %s (may need manual cleanup)",
            namespace,
            e,
        )
        await session.rollback()


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
    1. A git branch in the configured repository (via GitHub API)
    2. A DJ namespace with copied nodes

    These operations run in parallel for optimal performance.

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

    # Get parent namespace and validate it has git config (resolved)
    parent_ns = await get_node_namespace(session, namespace)

    # Resolve git config - repo/path may be inherited from ancestors
    github_repo_path, git_path, git_branch = await resolve_git_config(
        session,
        namespace,
    )

    if not github_repo_path:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' does not have git configured. "
            "Set github_repo_path on this namespace or a parent namespace.",
        )

    # For git root namespaces (no git_branch), use default_branch
    # For branch namespaces, use the existing git_branch
    source_branch = git_branch
    if not source_branch:
        # This is a git root namespace - use default_branch
        if not parent_ns.default_branch:
            raise DJInvalidInputException(
                message=f"Namespace '{namespace}' is a git root without a default_branch configured. "
                "Set default_branch to specify which branch to create new branches from.",
            )
        source_branch = parent_ns.default_branch

    # Validate branch name
    branch_name = request.branch_name.strip()
    if not branch_name:
        raise DJInvalidInputException(message="Branch name cannot be empty.")

    # Convert branch name to namespace: feature-x -> feature_x
    branch_namespace_suffix = branch_name.replace("-", "_").replace("/", "_")

    # Construct new namespace name
    # If creating from git root (e.g., "demo.metrics"), use full namespace: "demo.metrics.feature_x"
    # If creating from branch namespace (e.g., "demo.main"), use parent prefix: "demo.feature_x"
    if git_branch:
        # This is a branch namespace - siblings share parent prefix
        # e.g., "demo.main" -> "demo.feature_x"
        parent_parts = namespace.rsplit(SEPARATOR, 1)
        if len(parent_parts) > 1:
            new_namespace = f"{parent_parts[0]}{SEPARATOR}{branch_namespace_suffix}"
        else:
            new_namespace = f"{namespace}{SEPARATOR}{branch_namespace_suffix}"
    else:
        # This is a git root namespace - new branch is a child
        # e.g., "demo.metrics" -> "demo.metrics.feature_x"
        new_namespace = f"{namespace}{SEPARATOR}{branch_namespace_suffix}"

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

    # Run git branch creation and namespace/node copying in parallel
    _logger.info(
        "Starting parallel creation of git branch and namespace for '%s'",
        new_namespace,
    )

    git_task = asyncio.create_task(
        _create_git_branch(
            repo_path=github_repo_path,
            branch_name=branch_name,
            from_ref=source_branch,  # type: ignore
        ),
    )

    namespace_task = asyncio.create_task(
        _create_namespace_and_copy_nodes(
            session=session,
            new_namespace=new_namespace,
            branch_name=branch_name,
            root_namespace=parent_ns,
            current_user=current_user,
        ),
    )

    # Wait for both operations with proper error handling
    git_result, namespace_result = await asyncio.gather(
        git_task,
        namespace_task,
        return_exceptions=True,
    )

    # Handle failures and cleanup
    git_succeeded = not isinstance(git_result, Exception)
    namespace_succeeded = not isinstance(namespace_result, Exception)

    if not git_succeeded and not namespace_succeeded:
        # Both failed - report both errors for debugging
        _logger.error(
            "Both git branch and namespace creation failed for '%s'",
            new_namespace,
        )
        raise DJInvalidInputException(
            message=f"Failed to create branch: Git error: {git_result}, "
            f"Namespace error: {namespace_result}",
        )

    if not git_succeeded:
        # Git failed but namespace succeeded - cleanup namespace
        _logger.error("Git branch creation failed, cleaning up namespace")
        try:
            await _cleanup_namespace_and_nodes(session, new_namespace)
        except Exception as cleanup_error:  # pragma: no cover
            _logger.warning(
                "Failed to cleanup namespace after git failure: %s",
                cleanup_error,
            )
        raise DJInvalidInputException(
            message=f"Failed to create git branch '{branch_name}': "
            f"{getattr(git_result, 'message', str(git_result))}",
        )

    if not namespace_succeeded:
        # Namespace failed but git succeeded - cleanup git branch
        _logger.error("Namespace creation failed, cleaning up git branch")
        try:
            await _cleanup_git_branch(github_repo_path, branch_name)
        except Exception as cleanup_error:  # pragma: no cover
            _logger.warning(
                "Failed to cleanup git branch after namespace failure: %s",
                cleanup_error,
            )
        raise DJInvalidInputException(
            message=f"Failed to create namespace '{new_namespace}': {namespace_result}",
        )

    # Both succeeded!
    deployment_results = namespace_result

    return CreateBranchResult(
        branch=BranchInfo(
            namespace=new_namespace,
            git_branch=branch_name,
            parent_namespace=namespace,
            github_repo_path=github_repo_path,
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

    Returns:
    - Direct children: where parent_namespace equals the given namespace (git root branches)
    - Siblings: where parent_namespace equals this namespace's parent (branch-from-branch)
    """
    access_checker.add_namespace(namespace, ResourceAction.READ)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Verify namespace exists
    source_ns = await get_node_namespace(session, namespace)

    # Build query to find related branches
    if source_ns.parent_namespace:
        # This is a branch namespace - find siblings that share the same parent
        # (excluding self)
        stmt = (
            select(NodeNamespace)
            .where(NodeNamespace.parent_namespace == source_ns.parent_namespace)
            .where(NodeNamespace.namespace != namespace)
        )
    else:
        # This is a root namespace - find direct children
        stmt = select(NodeNamespace).where(NodeNamespace.parent_namespace == namespace)

    result = await session.execute(stmt)
    child_namespaces = result.scalars().all()

    # Resolve github_repo_path for response
    github_repo_path, _, _ = await resolve_git_config(session, namespace)

    return [
        BranchInfo(
            namespace=ns.namespace,
            git_branch=ns.git_branch or "",
            parent_namespace=ns.parent_namespace or "",
            github_repo_path=github_repo_path or "",
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

    # Verify it's actually related to the parent namespace
    # Two cases are valid:
    # 1. Direct child: branch_ns.parent_namespace == namespace (git root branches)
    # 2. Sibling: branch_ns.parent_namespace == namespace's parent (branch-from-branch)
    parent_ns = await get_node_namespace(session, namespace)
    is_direct_child = branch_ns.parent_namespace == namespace
    is_sibling = (
        parent_ns.parent_namespace
        and branch_ns.parent_namespace == parent_ns.parent_namespace
    )

    if not (is_direct_child or is_sibling):
        raise DJInvalidInputException(
            message=f"Namespace '{branch_namespace}' is not a branch of '{namespace}'.",
        )

    # Delete the git branch
    git_branch_deleted = False
    if delete_git_branch and branch_ns.git_branch:
        # Resolve git config from branch namespace (may be inherited from parent)
        github_repo_path, _, _ = await resolve_git_config(session, branch_namespace)

        if github_repo_path:  # pragma: no branch
            try:
                github = GitHubService()
                await github.delete_branch(
                    repo_path=github_repo_path,
                    branch=branch_ns.git_branch,
                )
                git_branch_deleted = True
                _logger.info(
                    "Deleted git branch '%s' in repo '%s'",
                    branch_ns.git_branch,
                    github_repo_path,
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
        if child_ns.namespace != branch_namespace:  # pragma: no branch
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
