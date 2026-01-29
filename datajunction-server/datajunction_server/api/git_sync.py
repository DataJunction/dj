"""
Git sync API endpoints.

Enables syncing node definitions to git and creating pull requests.
"""

import logging
from typing import List, Optional

from fastapi import Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import get_node_namespace
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.errors import DJDoesNotExistException, DJInvalidInputException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    AccessChecker,
    AccessDenialMode,
    get_access_checker,
)
from datajunction_server.internal.git import GitHubService
from datajunction_server.internal.git.github_service import GitHubServiceError
from datajunction_server.internal.namespaces import (
    get_node_specs_for_export,
    inject_prefixes,
    node_spec_to_yaml,
)
from datajunction_server.models.access import ResourceAction
from datajunction_server.utils import get_current_user, get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["git-sync"])


class SyncToGitRequest(BaseModel):
    """Request to sync node(s) to git."""

    commit_message: Optional[str] = None  # Auto-generate if not provided


class SyncResult(BaseModel):
    """Result of syncing a node to git."""

    node_name: str
    file_path: str
    commit_sha: str
    commit_url: str
    created: bool  # True if file was created, False if updated


class SyncNamespaceResult(BaseModel):
    """Result of syncing a namespace to git."""

    namespace: str
    files_synced: int
    commit_sha: str
    commit_url: str
    results: List[SyncResult]


class CreatePRRequest(BaseModel):
    """Request to create a pull request."""

    title: str
    body: Optional[str] = None


class PRResult(BaseModel):
    """Result of creating a pull request."""

    pr_number: int
    pr_url: str
    head_branch: str
    base_branch: str


@router.post(
    "/nodes/{node_name}/sync-to-git",
    response_model=SyncResult,
    name="Sync a node to git",
)
async def sync_node_to_git(
    node_name: str,
    request: SyncToGitRequest,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> SyncResult:
    """
    Sync a single node to its namespace's git branch.

    1. Gets node and its namespace's git config
    2. Serializes node to YAML (reuses existing export logic)
    3. Commits to git branch via GitHub API

    Commits are made by the bot/service account, with the current user
    attributed via Co-authored-by trailer.
    """
    # Get the node
    node = await Node.get_by_name(session, node_name, options=Node.cube_load_options())
    if not node:
        raise DJDoesNotExistException(
            message=f"Node '{node_name}' does not exist.",
            http_status_code=404,
        )

    access_checker.add_namespace(node.namespace, ResourceAction.WRITE)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Get namespace git config
    namespace_obj = await get_node_namespace(session, node.namespace)

    if not namespace_obj.github_repo_path:
        raise DJInvalidInputException(
            message=f"Namespace '{node.namespace}' does not have git configured.",
        )
    if not namespace_obj.git_branch:
        raise DJInvalidInputException(
            message=f"Namespace '{node.namespace}' does not have a git branch configured.",
        )

    # Convert node to spec with ${prefix} injection (same format as export)
    node_spec = await node.to_spec(session)

    # Inject ${prefix} into name and query (like export does)
    node_spec.name = inject_prefixes(node_spec.name, node.namespace)
    if hasattr(node_spec, "query") and node_spec.query:
        node_spec.query = inject_prefixes(node_spec.query, node.namespace)

    yaml_content = node_spec_to_yaml(node_spec)

    # File path uses short name (strip namespace prefix)
    # e.g., "demo.main.orders" with namespace "demo.main" -> "orders.yaml"
    if node_name.startswith(node.namespace + "."):
        short_name = node_name[len(node.namespace) + 1 :]
    else:
        short_name = node_name  # pragma: no cover

    parts = short_name.split(".")
    file_path = "/".join(parts) + ".yaml"
    if namespace_obj.git_path:
        git_path = namespace_obj.git_path.strip("/")
        file_path = f"{git_path}/{file_path}"

    _logger.info("Syncing node to git: %s -> %s", node_name, file_path)

    # Commit message
    commit_message = request.commit_message or f"Update {node_name}"

    # Sync to git
    try:
        github = GitHubService()

        # Check if file exists to get SHA for update
        existing_file = await github.get_file(
            repo_path=namespace_obj.github_repo_path,
            path=file_path,
            branch=namespace_obj.git_branch,
        )

        result = await github.commit_file(
            repo_path=namespace_obj.github_repo_path,
            path=file_path,
            content=yaml_content,
            message=commit_message,
            branch=namespace_obj.git_branch,
            sha=existing_file["sha"] if existing_file else None,
            co_author_name=current_user.username,
            co_author_email=current_user.email
            or f"{current_user.username}@users.noreply",
        )

        commit_sha = result["commit"]["sha"]
        commit_url = result["commit"]["html_url"]
        created = existing_file is None

        _logger.info(
            "Synced node '%s' to git: %s (sha: %s)",
            node_name,
            file_path,
            commit_sha[:8],
        )

        return SyncResult(
            node_name=node_name,
            file_path=file_path,
            commit_sha=commit_sha,
            commit_url=commit_url,
            created=created,
        )

    except GitHubServiceError as e:
        _logger.error("Failed to sync node to git: %s", e)
        raise DJInvalidInputException(
            message=f"Failed to sync to git: {e.message}",
        ) from e


@router.post(
    "/namespaces/{namespace}/sync-to-git",
    response_model=SyncNamespaceResult,
    name="Sync namespace to git",
)
async def sync_namespace_to_git(
    namespace: str,
    request: SyncToGitRequest,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> SyncNamespaceResult:
    """
    Sync all nodes in a namespace to git.

    This exports all nodes as YAML files and commits them to the configured
    git branch. Each node becomes a separate file.

    Commits are made by the bot/service account, with the current user
    attributed via Co-authored-by trailer.
    """
    access_checker.add_namespace(namespace, ResourceAction.WRITE)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Get namespace git config
    namespace_obj = await get_node_namespace(session, namespace)

    if not namespace_obj.github_repo_path:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' does not have git configured.",
        )
    if not namespace_obj.git_branch:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' does not have a git branch configured.",
        )

    # Get all node specs with ${prefix} injection (same as export)
    node_specs = await get_node_specs_for_export(session, namespace)

    if not node_specs:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' has no nodes to sync.",
        )

    # Prepare YAML content for each node
    files_to_commit: List[dict] = []
    results: List[SyncResult] = []

    for node_spec in node_specs:
        # The spec name has ${prefix} injected (e.g., "${prefix}orders")
        # Strip ${prefix} to get the short name for file path
        spec_name = node_spec.name
        if spec_name.startswith("${prefix}"):
            short_name = spec_name[len("${prefix}") :]
        else:
            short_name = spec_name  # pragma: no cover

        # Convert to YAML using the export format (with ${prefix})
        yaml_content = node_spec_to_yaml(node_spec)

        # File path uses short name (no namespace prefix, no ${prefix})
        # e.g., "orders" -> "nodes/orders.yaml" (with git_path="nodes")
        parts = short_name.split(".")
        file_path = "/".join(parts) + ".yaml"
        if namespace_obj.git_path:
            git_path = namespace_obj.git_path.strip("/")
            file_path = f"{git_path}/{file_path}"

        files_to_commit.append(
            {
                "path": file_path,
                "content": yaml_content,
                "node_name": spec_name,
            },
        )
        _logger.info(
            "Preparing file for git sync: %s (spec name: %s)",
            file_path,
            spec_name,
        )

    try:
        github = GitHubService()
        commit_message = request.commit_message or f"Sync {namespace}"

        # Batch commit all files in a single commit
        commit_result = await github.commit_files(
            repo_path=namespace_obj.github_repo_path,
            files=[
                {"path": f["path"], "content": f["content"]} for f in files_to_commit
            ],
            message=commit_message,
            branch=namespace_obj.git_branch,
            co_author_name=current_user.username,
            co_author_email=current_user.email
            or f"{current_user.username}@users.noreply",
        )

        commit_sha = commit_result["sha"]
        commit_url = commit_result["html_url"]

        # Build results for each file
        for file_info in files_to_commit:
            results.append(
                SyncResult(
                    node_name=file_info["node_name"],
                    file_path=file_info["path"],
                    commit_sha=commit_sha,
                    commit_url=commit_url,
                    created=True,  # We don't track individual file status in batch mode
                ),
            )

        _logger.info(
            "Synced namespace '%s' to git: %d files in single commit (sha: %s)",
            namespace,
            len(results),
            commit_sha[:8] if commit_sha else "none",
        )

        return SyncNamespaceResult(
            namespace=namespace,
            files_synced=len(results),
            commit_sha=commit_sha,
            commit_url=commit_url,
            results=results,
        )

    except GitHubServiceError as e:
        _logger.error("Failed to sync namespace to git: %s", e)
        raise DJInvalidInputException(
            message=f"Failed to sync to git: {e.message}",
        ) from e


@router.get(
    "/namespaces/{namespace}/pull-request",
    response_model=Optional[PRResult],
    name="Get existing pull request",
)
async def get_pull_request(
    namespace: str,
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> Optional[PRResult]:
    """
    Check if a pull request exists for this branch namespace.

    Returns the PR info if one exists, or null if no PR exists.
    """
    access_checker.add_namespace(namespace, ResourceAction.READ)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    namespace_obj = await get_node_namespace(session, namespace)

    if not namespace_obj.parent_namespace:
        return None  # Not a branch namespace, no PR possible

    if not namespace_obj.github_repo_path or not namespace_obj.git_branch:
        return None  # No git configured

    parent_ns = await get_node_namespace(session, namespace_obj.parent_namespace)
    if not parent_ns.git_branch:
        return None  # Parent has no git branch

    try:
        github = GitHubService()
        existing_pr = await github.get_pull_request(
            repo_path=namespace_obj.github_repo_path,
            head=namespace_obj.git_branch,
            base=parent_ns.git_branch,
        )

        if existing_pr:
            return PRResult(
                pr_number=existing_pr["number"],
                pr_url=existing_pr["html_url"],
                head_branch=namespace_obj.git_branch,
                base_branch=parent_ns.git_branch,
            )
        return None

    except GitHubServiceError:
        return None  # If GitHub API fails, just return no PR


@router.post(
    "/namespaces/{namespace}/pull-request",
    response_model=PRResult,
    name="Create a pull request",
)
async def create_pull_request(
    namespace: str,
    request: CreatePRRequest,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> PRResult:
    """
    Create a pull request from this branch namespace to its parent.

    Preconditions:
    - Namespace must have parent_namespace set (is a branch namespace)
    - Both namespaces must have git configured

    The PR is created from git_branch -> parent's git_branch.
    """
    access_checker.add_namespace(namespace, ResourceAction.WRITE)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Get namespace
    namespace_obj = await get_node_namespace(session, namespace)

    if not namespace_obj.parent_namespace:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' is not a branch namespace. "
            "Only branch namespaces (with parent_namespace) can create PRs.",
        )

    if not namespace_obj.github_repo_path or not namespace_obj.git_branch:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' does not have git configured.",
        )

    # Get parent namespace
    parent_ns = await get_node_namespace(session, namespace_obj.parent_namespace)

    if not parent_ns.git_branch:
        raise DJInvalidInputException(
            message=f"Parent namespace '{namespace_obj.parent_namespace}' does not have a git branch configured.",
        )

    try:
        github = GitHubService()

        # Check if PR already exists
        existing_pr = await github.get_pull_request(
            repo_path=namespace_obj.github_repo_path,
            head=namespace_obj.git_branch,
            base=parent_ns.git_branch,
        )

        if existing_pr:
            return PRResult(
                pr_number=existing_pr["number"],
                pr_url=existing_pr["html_url"],
                head_branch=namespace_obj.git_branch,
                base_branch=parent_ns.git_branch,
            )

        # Create PR
        pr_body = request.body or f"Changes from DJ namespace `{namespace}`"

        result = await github.create_pull_request(
            repo_path=namespace_obj.github_repo_path,
            head=namespace_obj.git_branch,
            base=parent_ns.git_branch,
            title=request.title,
            body=pr_body,
        )

        _logger.info(
            "Created PR #%d: %s -> %s",
            result["number"],
            namespace_obj.git_branch,
            parent_ns.git_branch,
        )

        return PRResult(
            pr_number=result["number"],
            pr_url=result["html_url"],
            head_branch=namespace_obj.git_branch,
            base_branch=parent_ns.git_branch,
        )

    except GitHubServiceError as e:
        _logger.error("Failed to create pull request: %s", e)
        raise DJInvalidInputException(
            message=str(e),
        ) from e
