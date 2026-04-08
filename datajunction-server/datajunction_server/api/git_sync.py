"""
Git sync API endpoints.

Enables syncing node definitions to git and creating pull requests.
Also provides sync-from-git endpoint for DJ-pull model deployments.
"""

import base64
import io
import logging
import tarfile
import tempfile
import uuid
from pathlib import Path
from typing import List, Optional

import yaml
from fastapi import BackgroundTasks, Depends, Request
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
from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.internal.deployment.orchestrator import DeploymentOrchestrator
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.internal.git import GitHubService
from datajunction_server.internal.git.github_service import GitHubServiceError
from pydantic import TypeAdapter

from datajunction_server.internal.namespaces import (
    get_node_specs_for_export,
    inject_prefixes,
    node_spec_to_yaml,
    resolve_git_config,
    _get_yaml_handler,
)
from datajunction_server.models.access import ResourceAction
from datajunction_server.models.deployment import (
    DeploymentInfo,
    DeploymentSpec,
    DeploymentStatus,
    GitDeploymentSource,
    NodeUnion,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.utils import (
    SEPARATOR,
    get_current_user,
    get_query_service_client,
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["git-sync"])

# TypeAdapter for parsing YAML dicts into the correct NodeSpec subclass
_node_spec_adapter = TypeAdapter(NodeUnion)


def _specs_are_equivalent(existing_yaml: str, new_spec: NodeUnion) -> bool:
    """
    Compare a YAML spec from git with a NodeSpec using the existing
    semantic comparison logic from the deployment orchestrator.

    Parses the existing YAML into a NodeSpec and uses the NodeSpec.__eq__
    method which handles:
    - AST-based query comparison (whitespace/formatting agnostic)
    - Sorted dimension link comparison
    - Column comparison with type inference handling
    - Metric metadata comparison with defaults

    Args:
        existing_yaml: YAML string from git
        new_spec: NodeSpec from current node (with ${prefix} placeholders and namespace set)

    Returns:
        True if the specs are semantically equivalent, False otherwise.
    """
    try:
        yaml_handler = _get_yaml_handler()
        existing_data = yaml_handler.load(existing_yaml)

        if not existing_data:
            return False

        # Convert ruamel.yaml CommentedMap to regular dict for Pydantic
        existing_dict = dict(existing_data)

        # Parse the existing YAML into the appropriate NodeSpec subclass
        # The discriminator field (node_type) determines which class to use
        existing_spec = _node_spec_adapter.validate_python(existing_dict)

        # Inject the namespace into the parsed spec so rendered_name matches
        # (NodeSpec.__eq__ compares rendered_name which depends on namespace)
        existing_spec.namespace = new_spec.namespace

        # Use the NodeSpec's __eq__ which does semantic comparison
        # (AST comparison for queries, sorted dimension links, etc.)
        result = new_spec == existing_spec
        if not result:
            _logger.debug(
                "Specs differ for %s: new=%s, existing=%s",
                new_spec.rendered_name,
                new_spec.model_dump(exclude_none=True),
                existing_spec.model_dump(exclude_none=True),
            )
        return result
    except Exception as e:
        # If we can't parse, assume they're different (safer)
        _logger.debug("Failed to compare specs: %s", e)
        return False


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
    commit_sha: Optional[str] = None  # None if no changes detected
    commit_url: Optional[str] = None  # None if no changes detected
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


async def _fetch_deployment_spec_from_git(
    github: GitHubService,
    repo_path: str,
    ref: str,
    git_path: Optional[str],
    namespace: str,
) -> dict:
    """
    Download repo archive at ref, extract, and parse YAML files into a DeploymentSpec dict.

    This implements the DJ-pull model: server fetches content directly from GitHub,
    so user-submitted content is never trusted.

    Args:
        github: GitHubService instance
        repo_path: Repository path (e.g., "owner/repo")
        ref: Git ref (commit SHA or branch name)
        git_path: Subdirectory within repo for node definitions (optional)
        namespace: Target namespace for deployment

    Returns:
        DeploymentSpec-compatible dict with nodes, namespace, etc.
    """
    _logger.info(
        "Fetching deployment spec from git: %s @ %s (path: %s)",
        repo_path,
        ref[:12] if len(ref) > 12 else ref,
        git_path or "/",
    )

    # Download archive at specific ref (works with commit SHAs and branch names)
    archive_bytes = await github.download_archive(
        repo_path=repo_path,
        branch=ref,
        format="tarball",
    )

    # Extract and parse YAML files
    nodes: List[dict] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
            tar.extractall(tmpdir)

        # Find extracted directory (GitHub adds prefix like "owner-repo-sha")
        extracted_dirs = list(Path(tmpdir).iterdir())
        if not extracted_dirs:
            raise DJInvalidInputException(
                message="Failed to extract archive - no directories found",
            )
        repo_dir = extracted_dirs[0]

        # Determine files directory
        if git_path:
            files_dir = repo_dir / git_path.strip("/")
        else:
            files_dir = repo_dir

        if not files_dir.exists():
            raise DJInvalidInputException(
                message=f"Path '{git_path}' not found in repository at ref '{ref}'",
            )

        # Parse all YAML files (skip dj.yaml project file)
        for yaml_file in files_dir.rglob("*.yaml"):
            if yaml_file.name == "dj.yaml":
                continue
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    node = yaml.safe_load(f)
                if isinstance(node, dict) and "name" in node:
                    nodes.append(node)
                    _logger.debug("Loaded node spec: %s", node.get("name"))
            except Exception as e:
                _logger.warning(
                    "Skipping invalid YAML file %s: %s",
                    yaml_file,
                    e,
                )
                continue

    _logger.info(
        "Fetched %d node specs from git: %s @ %s",
        len(nodes),
        repo_path,
        ref[:12] if len(ref) > 12 else ref,
    )

    return {
        "namespace": namespace,
        "nodes": nodes,
        "tags": [],
    }


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

    # Resolve git config (may be inherited from parent)
    github_repo_path, git_path, git_branch = await resolve_git_config(
        session,
        node.namespace,
    )

    if not github_repo_path:
        raise DJInvalidInputException(
            message=f"Namespace '{node.namespace}' does not have git configured. "
            "Set github_repo_path on this namespace or a parent namespace.",
        )
    if not git_branch:
        raise DJInvalidInputException(
            message=f"Namespace '{node.namespace}' does not have a git branch configured.",
        )

    # Convert node to spec with ${prefix} injection (same format as export)
    node_spec = await node.to_spec(session)

    # Inject ${prefix} into name and query (like export does)
    node_spec.name = inject_prefixes(node_spec.name, node.namespace)
    if hasattr(node_spec, "query") and node_spec.query:
        node_spec.query = inject_prefixes(node_spec.query, node.namespace)

    # For cubes, also inject ${prefix} into metrics and dimensions
    if node.type == NodeType.CUBE and hasattr(node_spec, "metrics"):
        node_spec.metrics = [
            inject_prefixes(metric, node.namespace) for metric in node_spec.metrics
        ]
    if node.type == NodeType.CUBE and hasattr(node_spec, "dimensions"):
        node_spec.dimensions = [
            inject_prefixes(dim, node.namespace) for dim in node_spec.dimensions
        ]

    # File path uses short name (strip namespace prefix)
    # e.g., "demo.main.orders" with namespace "demo.main" -> "orders.yaml"
    if node_name.startswith(node.namespace + SEPARATOR):
        short_name = node_name[len(node.namespace) + len(SEPARATOR) :]
    else:
        short_name = node_name  # pragma: no cover

    parts = short_name.split(".")
    file_path = "/".join(parts) + ".yaml"
    if git_path:
        git_path_stripped = git_path.strip("/")
        file_path = f"{git_path_stripped}/{file_path}"

    # Sync to git
    try:
        github = GitHubService()

        # Try to read existing YAML content to preserve comments
        # Also get the file SHA if it exists for the commit
        existing_yaml = None
        existing_file = None
        try:
            existing_file = await github.get_file(
                repo_path=github_repo_path,
                path=file_path,
                branch=git_branch,
            )
            if existing_file and "content" in existing_file:
                existing_yaml = base64.b64decode(existing_file["content"]).decode(
                    "utf-8",
                )
        except Exception:
            # File doesn't exist yet or couldn't be read - that's ok
            pass

        yaml_content = node_spec_to_yaml(node_spec, existing_yaml=existing_yaml)

        _logger.info("Syncing node to git: %s -> %s", node_name, file_path)

        # Commit message
        commit_message = request.commit_message or f"Update {node_name}"

        result = await github.commit_file(
            repo_path=github_repo_path,
            path=file_path,
            content=yaml_content,
            message=commit_message,
            branch=git_branch,
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

    # Resolve git config (may be inherited from parent)
    github_repo_path, git_path, git_branch = await resolve_git_config(
        session,
        namespace,
    )

    if not github_repo_path:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' does not have git configured. "
            "Set github_repo_path on this namespace or a parent namespace.",
        )
    if not git_branch:
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

    try:
        github = GitHubService()

        # Download entire repo archive once (much faster than N get_file calls)
        _logger.info(
            "Downloading archive for %s branch '%s'",
            github_repo_path,
            git_branch,
        )
        archive_bytes = await github.download_archive(
            repo_path=github_repo_path,
            branch=git_branch,
            format="tarball",
        )

        # Extract archive to temp directory
        with tempfile.TemporaryDirectory() as tmpdir:
            with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
                tar.extractall(tmpdir)

            # Find the extracted repo directory (GitHub adds a prefix like "owner-repo-sha")
            extracted_dirs = list(Path(tmpdir).iterdir())
            if not extracted_dirs:
                raise GitHubServiceError(  # pragma: no cover
                    message="Failed to extract archive - no directories found",
                )
            repo_dir = extracted_dirs[0]

            # Build a map of existing files for quick lookup
            existing_files_map = {}
            if git_path:
                git_path_stripped = git_path.strip("/")
                files_dir = repo_dir / git_path_stripped
            else:
                files_dir = repo_dir

            if files_dir.exists():
                for yaml_file in files_dir.rglob("*.yaml"):
                    rel_path = yaml_file.relative_to(repo_dir)
                    existing_files_map[str(rel_path)] = yaml_file

            # Process each node spec
            skipped_unchanged = 0
            for node_spec in node_specs:
                # The spec name has ${prefix} injected (e.g., "${prefix}orders")
                # Strip ${prefix} to get the short name for file path
                spec_name = node_spec.name
                if spec_name.startswith("${prefix}"):
                    short_name = spec_name[len("${prefix}") :]
                else:
                    short_name = spec_name  # pragma: no cover

                # File path uses short name (no namespace prefix, no ${prefix})
                # e.g., "orders" -> "nodes/orders.yaml" (with git_path="nodes")
                parts = short_name.split(".")
                file_path = "/".join(parts) + ".yaml"
                if git_path:
                    git_path_stripped = git_path.strip("/")
                    file_path = f"{git_path_stripped}/{file_path}"

                # Try to read existing YAML content from extracted archive
                existing_yaml = None
                if file_path in existing_files_map:
                    try:
                        existing_yaml = existing_files_map[file_path].read_text()
                    except Exception:  # pragma: no cover
                        # File couldn't be read - that's ok
                        pass

                # Convert to YAML using the export format (with ${prefix})
                yaml_content = node_spec_to_yaml(node_spec, existing_yaml=existing_yaml)

                # Only include files that have actually changed (semantic comparison)
                # Uses NodeSpec.__eq__ which does AST-based query comparison, etc.
                if existing_yaml is not None and _specs_are_equivalent(
                    existing_yaml,
                    node_spec,
                ):
                    skipped_unchanged += 1
                    continue

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

            if skipped_unchanged > 0:
                _logger.info(
                    "Skipped %d unchanged files for namespace '%s'",
                    skipped_unchanged,
                    namespace,
                )

        # If no files have changed, return early without making a commit
        if not files_to_commit:
            _logger.info(
                "No changes detected for namespace '%s' - skipping commit",
                namespace,
            )
            return SyncNamespaceResult(
                namespace=namespace,
                files_synced=0,
                commit_sha=None,
                commit_url=None,
                results=[],
            )

        commit_message = request.commit_message or f"Sync {namespace}"

        # Batch commit all files in a single commit
        commit_result = await github.commit_files(
            repo_path=github_repo_path,
            files=[
                {"path": f["path"], "content": f["content"]} for f in files_to_commit
            ],
            message=commit_message,
            branch=git_branch,
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

    If the parent is a git root (has default_branch), searches for a PR targeting
    the parent's default_branch namespace (e.g., parent.main).

    Returns the PR info if one exists, or null if no PR exists.
    """
    access_checker.add_namespace(namespace, ResourceAction.READ)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    namespace_obj = await get_node_namespace(session, namespace)

    if not namespace_obj.parent_namespace:
        return None  # Not a branch namespace, no PR possible

    # Resolve git config for this namespace
    github_repo_path, _, git_branch = await resolve_git_config(session, namespace)
    if not github_repo_path or not git_branch:
        return None  # No git configured

    # Resolve parent's git branch
    # If parent is a git root (has default_branch but no git_branch), target the default_branch namespace
    parent_ns_obj = await get_node_namespace(session, namespace_obj.parent_namespace)
    target_namespace = namespace_obj.parent_namespace

    if (
        parent_ns_obj.default_branch and not parent_ns_obj.git_branch
    ):  # pragma: no cover
        # Parent is a git root - target the default_branch namespace (e.g., "demo.metrics.main")
        target_namespace = f"{namespace_obj.parent_namespace}{SEPARATOR}{parent_ns_obj.default_branch.replace('-', '_').replace('/', '_')}"

    _, _, parent_git_branch = await resolve_git_config(
        session,
        target_namespace,
    )
    if not parent_git_branch:  # Target namespace has no git branch
        return None  # pragma: no cover

    try:
        github = GitHubService()
        existing_pr = await github.get_pull_request(
            repo_path=github_repo_path,
            head=git_branch,
            base=parent_git_branch,
        )

        if existing_pr:
            return PRResult(
                pr_number=existing_pr["number"],
                pr_url=existing_pr["html_url"],
                head_branch=git_branch,
                base_branch=parent_git_branch,
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

    # Resolve git config for this namespace
    github_repo_path, _, git_branch = await resolve_git_config(session, namespace)
    if not github_repo_path or not git_branch:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' does not have git configured. "
            "Set github_repo_path and git_branch on this namespace or a parent.",
        )

    # Resolve parent's git branch
    # If parent is a git root (has default_branch but no git_branch), target the default_branch namespace
    parent_ns_obj = await get_node_namespace(session, namespace_obj.parent_namespace)
    target_namespace = namespace_obj.parent_namespace

    if (
        parent_ns_obj.default_branch and not parent_ns_obj.git_branch
    ):  # pragma: no cover
        # Parent is a git root - target the default_branch namespace (e.g., "demo.metrics.main")
        target_namespace = f"{namespace_obj.parent_namespace}{SEPARATOR}{parent_ns_obj.default_branch.replace('-', '_').replace('/', '_')}"

    _, _, parent_git_branch = await resolve_git_config(
        session,
        target_namespace,
    )
    if not parent_git_branch:
        raise DJInvalidInputException(  # pragma: no cover
            message=f"Target namespace '{target_namespace}' does not have a git branch configured.",
        )

    try:
        github = GitHubService()

        # Check if PR already exists
        existing_pr = await github.get_pull_request(
            repo_path=github_repo_path,
            head=git_branch,
            base=parent_git_branch,
        )

        if existing_pr:
            return PRResult(
                pr_number=existing_pr["number"],
                pr_url=existing_pr["html_url"],
                head_branch=git_branch,
                base_branch=parent_git_branch,
            )

        # Create PR
        pr_body = request.body or f"Changes from DJ namespace `{namespace}`"

        result = await github.create_pull_request(
            repo_path=github_repo_path,
            head=git_branch,
            base=parent_git_branch,
            title=request.title,
            body=pr_body,
        )

        _logger.info(
            "Created PR #%d: %s -> %s",
            result["number"],
            git_branch,
            parent_git_branch,
        )

        return PRResult(
            pr_number=result["number"],
            pr_url=result["html_url"],
            head_branch=git_branch,
            base_branch=parent_git_branch,
        )

    except GitHubServiceError as e:
        _logger.error("Failed to create pull request: %s", e)
        raise DJInvalidInputException(
            message=str(e),
        ) from e


@router.post(
    "/namespaces/{namespace}/sync-from-git",
    response_model=DeploymentInfo,
    name="Sync namespace from git",
)
async def sync_namespace_from_git(
    namespace: str,
    http_request: Request,
    background_tasks: BackgroundTasks,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    cache: Cache = Depends(get_cache),
) -> DeploymentInfo:
    """
    Sync a namespace by pulling node specs directly from the HEAD of its configured
    git branch (DJ-pull model).

    The server fetches content from GitHub — no ref is accepted from the caller.
    This ensures only the latest commit on the configured branch can be deployed,
    preventing users from pinning to an older (potentially vulnerable) commit.

    Returns:
        DeploymentInfo with deployment results

    Raises:
        400: If namespace has no github_repo_path configured
        400: If namespace has no git_branch configured
    """
    access_checker.add_namespace(namespace, ResourceAction.WRITE)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # 1. Resolve git config (validates namespace exists via resolve_git_config)
    github_repo_path, git_path, git_branch = await resolve_git_config(
        session,
        namespace,
    )

    if not github_repo_path:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' does not have github_repo_path configured. "
            "Cannot sync from git. Configure git settings on this namespace or a parent.",
        )

    if not git_branch:
        raise DJInvalidInputException(
            message=f"Namespace '{namespace}' does not have git_branch configured. "
            "Cannot sync from git without a branch to track.",
        )

    # 2. Resolve branch to current HEAD commit SHA
    github = GitHubService()
    try:
        commit_sha = await github.resolve_ref_to_sha(github_repo_path, git_branch)
    except GitHubServiceError as e:
        raise DJInvalidInputException(message=str(e)) from e

    _logger.info(
        "Syncing namespace '%s' from git: %s @ %s (resolved to %s)",
        namespace,
        github_repo_path,
        git_branch,
        commit_sha[:12],
    )

    # 3. Fetch commit author so history records the person who changed the YAML,
    #    not the CI caller. Failures are non-fatal — we proceed without author info.
    commit_author_name, commit_author_email = None, None
    try:
        commit_author_name, commit_author_email = await github.get_commit_author(
            github_repo_path,
            commit_sha,
        )
    except GitHubServiceError:
        _logger.warning(
            "Could not fetch commit author for %s @ %s; history will use API caller",
            github_repo_path,
            commit_sha[:12],
        )

    # 4. Download and parse YAML files from git at this commit
    try:
        deployment_spec_dict = await _fetch_deployment_spec_from_git(
            github=github,
            repo_path=github_repo_path,
            ref=commit_sha,
            git_path=git_path,
            namespace=namespace,
        )
    except GitHubServiceError as e:
        raise DJInvalidInputException(
            message=f"Failed to fetch from git: {e.message}",
        ) from e

    if not deployment_spec_dict.get("nodes"):
        raise DJInvalidInputException(
            message=f"No node definitions found in repository '{github_repo_path}' "
            f"at branch '{git_branch}'"
            + (f" in path '{git_path}'" if git_path else ""),
        )

    # 5. Set source metadata (proves content came from verified commit)
    deployment_spec_dict["source"] = {
        "type": "git",
        "repository": github_repo_path,
        "branch": git_branch,
        "commit_sha": commit_sha,
        "commit_author_name": commit_author_name,
        "commit_author_email": commit_author_email,
    }

    # 6. Deploy using existing orchestrator
    deployment_id = str(uuid.uuid4())
    deployment_spec = DeploymentSpec(**deployment_spec_dict)

    orchestrator = DeploymentOrchestrator(
        deployment_id=deployment_id,
        deployment_spec=deployment_spec,
        session=session,
        context=DeploymentContext(
            current_user=current_user,
            request=http_request,
            query_service_client=query_service_client,
            background_tasks=background_tasks,
            cache=cache,
        ),
        dry_run=False,
    )

    try:
        execute_result = await orchestrator.execute()
    except Exception as e:
        _logger.error("Deployment failed for sync-from-git: %s", e)
        raise

    _logger.info(
        "Sync-from-git completed for namespace '%s': %d results",
        namespace,
        len(execute_result.results),
    )

    return DeploymentInfo(
        uuid=deployment_id,
        namespace=namespace,
        status=DeploymentStatus.SUCCESS,
        results=execute_result.results,
        downstream_impacts=execute_result.downstream_impacts,
        source=GitDeploymentSource(
            type="git",
            repository=github_repo_path,
            branch=git_branch,
            commit_sha=commit_sha,
            commit_author_name=commit_author_name,
            commit_author_email=commit_author_email,
        ),
    )
