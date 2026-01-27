"""
Git sync API endpoints.

Enables syncing node definitions to git and creating pull requests.
"""

import logging
from typing import List, Optional

import yaml
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
    _get_yaml_dumper,
    _node_spec_to_yaml_dict,
    get_node_specs_for_export,
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


def _get_node_file_path(node_name: str, namespace: str, git_path: Optional[str]) -> str:
    """
    Get the file path for a node in the git repository.

    Converts node name to file path:
    - namespace.sub.node_name -> sub/node_name.yaml (relative to namespace root)
    - With git_path: definitions/sub/node_name.yaml
    """
    # Remove namespace prefix from node name
    if node_name.startswith(namespace + "."):
        relative_name = node_name[len(namespace) + 1 :]
    else:
        relative_name = node_name

    # Convert dots to directory separators
    parts = relative_name.split(".")
    file_path = "/".join(parts) + ".yaml"

    # Prepend git_path if configured
    if git_path:
        git_path = git_path.strip("/")
        file_path = f"{git_path}/{file_path}"

    return file_path


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

    The commit is attributed to the current user via author metadata.
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

    # Convert node to spec and then to YAML
    node_spec = await node.to_spec(session)
    yaml_dict = _node_spec_to_yaml_dict(node_spec)
    yaml_dumper = _get_yaml_dumper()
    yaml_content = yaml.dump(
        yaml_dict,
        Dumper=yaml_dumper,
        sort_keys=False,
        default_flow_style=False,
    )

    # Determine file path
    file_path = _get_node_file_path(
        node_name,
        node.namespace,
        namespace_obj.git_path,
    )

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
            author_name=current_user.username,
            author_email=current_user.email or f"{current_user.username}@users.noreply",
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

    # Get all node specs
    node_specs = await get_node_specs_for_export(session, namespace)

    if not node_specs:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' has no nodes to sync.",
        )

    # Prepare YAML content for each node
    yaml_dumper = _get_yaml_dumper()
    results: List[SyncResult] = []
    last_commit_sha = ""
    last_commit_url = ""

    try:
        github = GitHubService()

        for node_spec in node_specs:
            # Get the original node name (before ${prefix} injection)
            node_name = node_spec.rendered_name

            yaml_dict = _node_spec_to_yaml_dict(node_spec)
            yaml_content = yaml.dump(
                yaml_dict,
                Dumper=yaml_dumper,
                sort_keys=False,
                default_flow_style=False,
            )

            file_path = _get_node_file_path(
                node_name,
                namespace,
                namespace_obj.git_path,
            )

            commit_message = request.commit_message or f"Sync {namespace}"

            # Check if file exists
            existing_file = await github.get_file(
                repo_path=namespace_obj.github_repo_path,
                path=file_path,
                branch=namespace_obj.git_branch,
            )

            result = await github.commit_file(
                repo_path=namespace_obj.github_repo_path,
                path=file_path,
                content=yaml_content,
                message=f"{commit_message}: {node_name}",
                branch=namespace_obj.git_branch,
                sha=existing_file["sha"] if existing_file else None,
                author_name=current_user.username,
                author_email=current_user.email
                or f"{current_user.username}@users.noreply",
            )

            last_commit_sha = result["commit"]["sha"]
            last_commit_url = result["commit"]["html_url"]

            results.append(
                SyncResult(
                    node_name=node_name,
                    file_path=file_path,
                    commit_sha=last_commit_sha,
                    commit_url=last_commit_url,
                    created=existing_file is None,
                ),
            )

        _logger.info(
            "Synced namespace '%s' to git: %d files (last sha: %s)",
            namespace,
            len(results),
            last_commit_sha[:8] if last_commit_sha else "none",
        )

        return SyncNamespaceResult(
            namespace=namespace,
            files_synced=len(results),
            commit_sha=last_commit_sha,
            commit_url=last_commit_url,
            results=results,
        )

    except GitHubServiceError as e:
        _logger.error("Failed to sync namespace to git: %s", e)
        raise DJInvalidInputException(
            message=f"Failed to sync to git: {e.message}",
        ) from e


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
            message=f"Failed to create pull request: {e.message}",
        ) from e
