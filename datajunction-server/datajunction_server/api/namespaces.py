"""
Node namespace related APIs.
"""

import io
import logging
import zipfile
from http import HTTPStatus
from typing import Callable, Dict, List, Optional

import yaml
from fastapi import Depends, Query, BackgroundTasks, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.api.helpers import get_node_namespace, get_save_history
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.user import User
from datajunction_server.errors import DJAlreadyExistsException, DJInvalidInputException
from datajunction_server.models.access import ResourceAction
from datajunction_server.models.deployment import (
    BulkNamespaceSourcesRequest,
    BulkNamespaceSourcesResponse,
    DeploymentSpec,
    NamespaceGitConfig,
    NamespaceSourcesResponse,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    AccessChecker,
    get_access_checker,
    AccessDenialMode,
)
from datajunction_server.internal.namespaces import (
    create_namespace,
    get_nodes_in_namespace,
    get_nodes_in_namespace_detailed,
    get_project_config,
    hard_delete_namespace,
    mark_namespace_deactivated,
    mark_namespace_restored,
    get_sources_for_namespace,
    get_sources_for_namespaces_bulk,
    get_node_specs_for_export,
    node_spec_to_yaml,
    detect_parent_cycle,
    resolve_git_config,
    validate_sibling_relationship,
    validate_git_path,
)
from datajunction_server.internal.nodes import activate_node, deactivate_node
from datajunction_server.models import access
from datajunction_server.models.node import NamespaceOutput, NodeMinimumDetail
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import (
    get_current_user,
    get_query_service_client,
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["namespaces"])


# Response model is the same as the base model, but we return resolved values
# If parent_namespace is set, clients can GET the parent to understand inheritance


@router.post("/namespaces/{namespace}/", status_code=HTTPStatus.CREATED)
async def create_node_namespace(
    namespace: str,
    include_parents: Optional[bool] = False,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    *,
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Create a node namespace
    """
    if node_namespace := await NodeNamespace.get(
        session,
        namespace,
        raise_if_not_exists=False,
    ):  # pragma: no cover
        if node_namespace.deactivated_at:
            node_namespace.deactivated_at = None
            session.add(node_namespace)
            await session.commit()
            return JSONResponse(
                status_code=HTTPStatus.CREATED,
                content={
                    "message": (
                        "The following node namespace has been successfully reactivated: "
                        + namespace
                    ),
                },
            )
        return JSONResponse(
            status_code=409,
            content={
                "message": f"Node namespace `{namespace}` already exists",
            },
        )
    created_namespaces = await create_namespace(
        session=session,
        namespace=namespace,
        include_parents=include_parents,  # type: ignore
        current_user=current_user,
        save_history=save_history,
    )
    return JSONResponse(
        status_code=HTTPStatus.CREATED,
        content={
            "message": (
                "The following node namespaces have been successfully created: "
                + ", ".join(created_namespaces)
            ),
        },
    )


@router.get(
    "/namespaces/",
    response_model=List[NamespaceOutput],
    status_code=200,
)
async def list_namespaces(
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> List[NamespaceOutput]:
    """
    List namespaces with the number of nodes contained in them
    """
    results = await NodeNamespace.get_all_with_node_count(session)
    access_checker.add_namespaces(
        [record.namespace for record in results],
        access.ResourceAction.READ,
    )
    approved_namespaces = await access_checker.approved_resource_names()
    return [
        NamespaceOutput(namespace=record.namespace, num_nodes=record.num_nodes)
        for record in results
        if record.namespace in approved_namespaces
    ]


@router.get(
    "/namespaces/{namespace}/",
    response_model=List[NodeMinimumDetail],
    status_code=HTTPStatus.OK,
)
async def list_nodes_in_namespace(
    namespace: str,
    type_: Optional[NodeType] = Query(
        default=None,
        description="Filter the list of nodes to this type",
    ),
    with_edited_by: bool = Query(
        default=False,
        description="Whether to include a list of users who edited each node",
    ),
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> List[NodeMinimumDetail]:
    """
    List node names in namespace, filterable to a given type if desired.
    """
    # Check that the user has namespace-level READ access
    access_checker.add_namespace(namespace, access.ResourceAction.READ)
    namespace_decisions = await access_checker.check(
        on_denied=AccessDenialMode.FILTER,
    )
    if not namespace_decisions:
        # User has no access to this namespace at all
        return []  # pragma: no cover

    # Get all nodes in namespace
    nodes = await NodeNamespace.list_nodes(
        session,
        namespace,
        type_,
        with_edited_by=with_edited_by,
    )

    # Filter to nodes the user has READ access to
    access_checker.add_nodes(nodes=nodes, action=access.ResourceAction.READ)
    node_decisions = await access_checker.check(on_denied=AccessDenialMode.RETURN)
    approved_names = {
        decision.request.access_object.name
        for decision in node_decisions
        if decision.approved
    }
    return [node for node in nodes if node.name in approved_names]


@router.delete("/namespaces/{namespace}/", status_code=HTTPStatus.OK)
async def deactivate_a_namespace(
    namespace: str,
    cascade: bool = Query(
        default=False,
        description="Cascade the deletion down to the nodes in the namespace",
    ),
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    save_history: Callable = Depends(get_save_history),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    background_tasks: BackgroundTasks,
    request: Request,
    access_checker: AccessChecker = Depends(get_access_checker),
) -> JSONResponse:
    """
    Deactivates a node namespace
    """
    access_checker.add_namespace(namespace, ResourceAction.WRITE)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    node_namespace = await NodeNamespace.get(
        session,
        namespace,
        raise_if_not_exists=True,
    )

    if node_namespace.deactivated_at:  # type: ignore
        raise DJAlreadyExistsException(
            message=f"Namespace `{namespace}` is already deactivated.",
        )

    # If there are no active nodes in the namespace, we can safely deactivate this namespace
    node_list = await NodeNamespace.list_nodes(session, namespace)
    node_names = [node.name for node in node_list]
    if len(node_names) == 0:
        message = f"Namespace `{namespace}` has been deactivated."
        await mark_namespace_deactivated(
            session=session,
            namespace=node_namespace,  # type: ignore
            message=message,
            current_user=current_user,
            save_history=save_history,
        )
        return JSONResponse(
            status_code=HTTPStatus.OK,
            content={"message": message},
        )

    # If cascade=true is set, we'll deactivate all nodes in this namespace and then
    # subsequently deactivate this namespace
    if cascade:
        for node_name in node_names:
            await deactivate_node(
                session=session,
                name=node_name,
                message=f"Cascaded from deactivating namespace `{namespace}`",
                current_user=current_user,
                save_history=save_history,
                query_service_client=query_service_client,
                background_tasks=background_tasks,
                request_headers=dict(request.headers),
            )
        message = (
            f"Namespace `{namespace}` has been deactivated. The following nodes"
            f" have also been deactivated: {','.join(node_names)}"
        )
        await mark_namespace_deactivated(
            session=session,
            namespace=node_namespace,  # type: ignore
            message=message,
            current_user=current_user,
            save_history=save_history,
        )

        return JSONResponse(
            status_code=HTTPStatus.OK,
            content={
                "message": message,
            },
        )

    return JSONResponse(
        status_code=405,
        content={
            "message": f"Cannot deactivate node namespace `{namespace}` as there are "
            "still active nodes under that namespace.",
        },
    )


@router.post("/namespaces/{namespace}/restore/", status_code=HTTPStatus.CREATED)
async def restore_a_namespace(
    namespace: str,
    cascade: bool = Query(
        default=False,
        description="Cascade the restore down to the nodes in the namespace",
    ),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    save_history: Callable = Depends(get_save_history),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> JSONResponse:
    """
    Restores a node namespace
    """
    access_checker.add_namespace(namespace, ResourceAction.WRITE)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    node_namespace = await get_node_namespace(
        session=session,
        namespace=namespace,
        raise_if_not_exists=True,
    )
    if not node_namespace.deactivated_at:
        raise DJAlreadyExistsException(
            message=f"Node namespace `{namespace}` already exists and is active.",
        )

    node_list = await get_nodes_in_namespace(
        session,
        namespace,
        include_deactivated=True,
    )
    node_names = [node.name for node in node_list]
    # If cascade=true is set, we'll restore all nodes in this namespace and then
    # subsequently restore this namespace
    if cascade:
        for node_name in node_names:
            await activate_node(
                name=node_name,
                session=session,
                message=f"Cascaded from restoring namespace `{namespace}`",
                current_user=current_user,
                save_history=save_history,
            )

        message = (
            f"Namespace `{namespace}` has been restored. The following nodes"
            f" have also been restored: {','.join(node_names)}"
        )
        await mark_namespace_restored(
            session=session,
            namespace=node_namespace,
            message=message,
            current_user=current_user,
            save_history=save_history,
        )

        return JSONResponse(
            status_code=HTTPStatus.CREATED,
            content={
                "message": message,
            },
        )

    # Otherwise just restore this namespace
    message = f"Namespace `{namespace}` has been restored."
    await mark_namespace_restored(
        session=session,
        namespace=node_namespace,
        message=message,
        current_user=current_user,
        save_history=save_history,
    )
    return JSONResponse(
        status_code=HTTPStatus.CREATED,
        content={"message": message},
    )


@router.delete("/namespaces/{namespace}/hard/", name="Hard Delete a DJ Namespace")
async def hard_delete_node_namespace(
    namespace: str,
    *,
    cascade: bool = False,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    save_history: Callable = Depends(get_save_history),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> JSONResponse:
    """
    Hard delete a namespace, which will completely remove the namespace. Additionally,
    if any nodes are saved under this namespace, we'll hard delete the nodes if cascade
    is set to true. If cascade is set to false, we'll raise an error. This should be used
    with caution, as the impact may be large.
    """
    access_checker.add_namespace(namespace, ResourceAction.DELETE)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    impacts = await hard_delete_namespace(
        session=session,
        namespace=namespace,
        cascade=cascade,
        current_user=current_user,
        save_history=save_history,
    )
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={
            "message": f"The namespace `{namespace}` has been completely removed.",
            "impact": impacts.model_dump(),
        },
    )


@router.get(
    "/namespaces/{namespace}/export/",
    name="Export a namespace as a single project's metadata",
)
async def export_a_namespace(
    namespace: str,
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> List[Dict]:
    """
    Generates a zip of YAML files for the contents of the given namespace
    as well as a project definition file.
    """
    access_checker.add_namespace(namespace, ResourceAction.READ)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    return await get_project_config(
        session=session,
        nodes=await get_nodes_in_namespace_detailed(session, namespace),
        namespace_requested=namespace,
    )


@router.get(
    "/namespaces/{namespace}/export/spec",
    name="Export namespace as a deployment specification",
    response_model_exclude_none=True,
)
async def export_namespace_spec(
    namespace: str,
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> DeploymentSpec:
    """
    Generates a deployment spec for a namespace
    """
    access_checker.add_namespace(namespace, ResourceAction.READ)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    node_specs = await get_node_specs_for_export(session, namespace)
    return DeploymentSpec(
        namespace=namespace,
        nodes=node_specs,
    )


@router.get(
    "/namespaces/{namespace}/export/yaml",
    name="Export namespace as downloadable YAML ZIP",
    response_class=StreamingResponse,
)
async def export_namespace_yaml(
    namespace: str,
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> StreamingResponse:
    """
    Export a namespace as a downloadable ZIP file containing YAML files.

    The ZIP structure matches the expected layout for `dj push`:
    - dj.yaml (project manifest)
    - <namespace>/<node>.yaml (one file per node)

    This makes it easy to start managing nodes via Git/CI-CD.
    """
    access_checker.add_namespace(namespace, ResourceAction.READ)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Get node specs with ${prefix} injection applied
    node_specs = await get_node_specs_for_export(session, namespace)

    # Create ZIP in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        # Add dj.yaml project manifest
        project_manifest = {
            "name": f"Project {namespace} (Exported)",
            "description": f"Exported project for namespace {namespace}",
            "namespace": namespace,
        }

        zf.writestr(
            "dj.yaml",
            yaml.dump(
                project_manifest,
                sort_keys=False,
                default_flow_style=False,
            ),
        )

        # Add each node as a YAML file
        for node_spec in node_specs:
            # Convert name to file path: foo.bar.baz -> foo/bar/baz.yaml
            node_name = node_spec.name.replace("${prefix}", "").lstrip(".")
            parts = node_name.split(".")
            file_path = "/".join(parts) + ".yaml"

            zf.writestr(
                file_path,
                node_spec_to_yaml(node_spec),
            )

    zip_buffer.seek(0)

    # Return as downloadable ZIP
    safe_namespace = namespace.replace(".", "_")
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_namespace}_export.zip"',
        },
    )


@router.get(
    "/namespaces/{namespace}/sources",
    response_model=NamespaceSourcesResponse,
    name="Get deployment sources for a namespace",
)
async def get_namespace_sources(
    namespace: str,
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> NamespaceSourcesResponse:
    """
    Get all deployment sources that have deployed to this namespace.

    This helps teams understand:
    - Whether a namespace is managed by CI/CD
    - Which repositories have deployed to this namespace
    - If there are multiple sources (potential conflict indicator)
    """
    access_checker.add_namespace(namespace, ResourceAction.READ)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    return await get_sources_for_namespace(session, namespace)


@router.post(
    "/namespaces/sources/bulk",
    response_model=BulkNamespaceSourcesResponse,
    name="Get deployment sources for multiple namespaces",
)
async def get_bulk_namespace_sources(
    request: BulkNamespaceSourcesRequest,
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> BulkNamespaceSourcesResponse:
    """
    Get deployment sources for multiple namespaces in a single request.

    This is useful for displaying CI/CD badges in the UI for all visible namespaces.
    Returns a map of namespace name -> source info for each requested namespace.
    """
    # Add access checks for all requested namespaces
    for namespace in request.namespaces:
        access_checker.add_namespace(namespace, ResourceAction.READ)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Fetch sources for all namespaces in optimized bulk query
    sources = await get_sources_for_namespaces_bulk(session, request.namespaces)

    return BulkNamespaceSourcesResponse(sources=sources)


# =============================================================================
# Git Configuration Endpoints
# =============================================================================


@router.get(
    "/namespaces/{namespace}/git",
    response_model=NamespaceGitConfig,
    name="Get namespace git configuration",
)
async def get_namespace_git_config(
    namespace: str,
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> NamespaceGitConfig:
    """
    Get the git configuration for a namespace.

    Returns the effective git configuration (resolved, including inherited values).
    If parent_namespace is set, the github_repo_path and git_path may be
    inherited from the parent. Clients can GET the parent namespace to
    understand the inheritance hierarchy.
    """
    access_checker.add_namespace(namespace, ResourceAction.READ)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    node_namespace = await get_node_namespace(session, namespace)

    # Resolve the effective git config (including inherited values)
    resolved_repo, resolved_path, resolved_branch = await resolve_git_config(
        session,
        namespace,
    )

    return NamespaceGitConfig(
        # Return resolved values (effective configuration)
        github_repo_path=resolved_repo,
        git_path=resolved_path,
        git_branch=resolved_branch,
        parent_namespace=node_namespace.parent_namespace,
        git_only=node_namespace.git_only,
    )


@router.patch(
    "/namespaces/{namespace}/git",
    response_model=NamespaceGitConfig,
    name="Update namespace git configuration",
)
async def update_namespace_git_config(
    namespace: str,
    config: NamespaceGitConfig,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> NamespaceGitConfig:
    """
    Update the git configuration for a namespace.

    This enables git-backed branch management for the namespace, allowing users
    to create branches, sync changes to git, and create pull requests from the UI.

    Fields:
    - github_repo_path: Repository path (e.g., "owner/repo")
    - git_branch: Branch name (e.g., "main")
    - git_path: Subdirectory in repo for node definitions (e.g., "definitions/")
    - parent_namespace: Parent namespace for branch namespaces (for PR targeting)
    - git_only: If True, UI edits are blocked; must edit via git deployments
    """
    access_checker.add_namespace(namespace, ResourceAction.WRITE)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Get or create the namespace - auto-create if it doesn't exist
    # This allows retroactive configuration of parent namespaces when children already exist
    node_namespace = await NodeNamespace.get(
        session,
        namespace,
        raise_if_not_exists=False,
    )
    if not node_namespace:
        node_namespace = NodeNamespace(namespace=namespace)
        session.add(node_namespace)
        await session.commit()
        await session.refresh(node_namespace)

    # Compute the effective values after update
    new_repo = (
        config.github_repo_path
        if config.github_repo_path is not None
        else node_namespace.github_repo_path
    )
    new_branch = (
        config.git_branch
        if config.git_branch is not None
        else node_namespace.git_branch
    )
    new_path = (
        config.git_path if config.git_path is not None else node_namespace.git_path
    )
    new_parent = (
        config.parent_namespace
        if config.parent_namespace is not None
        else node_namespace.parent_namespace
    )
    new_git_only = (
        config.git_only if config.git_only is not None else node_namespace.git_only
    )

    # Early validations (independent of parent relationship)
    validate_git_path(new_path)

    # Validate hierarchical config rules: child namespaces cannot set repo/path
    # This enforces the model: parent has repo/path, children have branch+parent_namespace
    if new_parent:
        # If setting parent_namespace, cannot also set github_repo_path or git_path
        # (they must be inherited from parent)
        if config.github_repo_path is not None and config.github_repo_path != "":
            raise DJInvalidInputException(
                message="Cannot set github_repo_path on a branch namespace. "
                "Git repository configuration is inherited from parent_namespace. "
                "Remove parent_namespace if you want to configure this as a git root.",
            )
        if config.git_path is not None and config.git_path != "":
            raise DJInvalidInputException(
                message="Cannot set git_path on a branch namespace. "
                "Git path configuration is inherited from parent_namespace. "
                "Remove parent_namespace if you want to configure this as a git root.",
            )

    # Validate git_only requirement (must have git config, either direct or inherited)
    # Validate git_only - only makes sense for branch namespaces
    if new_git_only:
        # git_only requires a branch namespace (parent + branch)
        # Git roots just store configuration and don't have deployable content
        is_branch_namespace = new_parent and new_branch

        if not is_branch_namespace:
            raise DJInvalidInputException(
                message=(
                    "Cannot enable git_only on a git root namespace. "
                    "git_only is only applicable to branch namespaces that have "
                    "parent_namespace and git_branch configured."
                ),
            )

    # Validate parent_namespace if provided
    if new_parent:
        # Check for self-reference
        if new_parent == namespace:
            raise DJInvalidInputException(
                message="A namespace cannot be its own parent.",
            )

        # Check parent exists
        parent_ns_obj = await NodeNamespace.get(
            session,
            new_parent,
            raise_if_not_exists=False,
        )
        if not parent_ns_obj:
            raise DJInvalidInputException(
                message=f"Parent namespace '{new_parent}' does not exist.",
            )

        # Validate sibling relationship (same prefix)
        validate_sibling_relationship(namespace, new_parent)

        # Detect circular parent references
        await detect_parent_cycle(session, namespace, new_parent)

        # Note: No need to check repo matches parent anymore since we prevent
        # child namespaces from setting github_repo_path/git_path entirely.
        # They inherit these from parent via resolve_git_config().

    # Check for duplicate repo+branch+path (excluding this namespace)
    if new_repo and new_branch:
        stmt = select(NodeNamespace).where(
            NodeNamespace.github_repo_path == new_repo,
            NodeNamespace.git_branch == new_branch,
            NodeNamespace.namespace != namespace,
        )
        # Also match on git_path (treating None and "" as equivalent)
        if new_path:
            stmt = stmt.where(NodeNamespace.git_path == new_path)
        else:
            stmt = stmt.where(
                or_(NodeNamespace.git_path.is_(None), NodeNamespace.git_path == ""),
            )

        result = await session.execute(stmt)
        conflict = result.scalar_one_or_none()
        if conflict:
            raise DJInvalidInputException(
                message=f"Git location conflict: namespace '{conflict.namespace}' "
                f"already uses repo '{new_repo}', branch '{new_branch}', "
                f"path '{new_path or '(root)'}'. Each namespace must have a unique "
                "git location to avoid overwriting files.",
            )

    # Update only provided fields (None means no change)
    if config.github_repo_path is not None:
        node_namespace.github_repo_path = config.github_repo_path or None
    if config.git_branch is not None:
        node_namespace.git_branch = config.git_branch or None
    if config.git_path is not None:
        node_namespace.git_path = config.git_path or None
    if config.parent_namespace is not None:
        node_namespace.parent_namespace = config.parent_namespace or None
    if config.git_only is not None:
        node_namespace.git_only = config.git_only

    await session.commit()
    await session.refresh(node_namespace)

    _logger.info(
        "Updated git config for namespace %s: repo=%s, branch=%s, git_only=%s",
        namespace,
        node_namespace.github_repo_path,
        node_namespace.git_branch,
        node_namespace.git_only,
    )

    # Resolve and return the effective git config (including inherited values)
    resolved_repo, resolved_path, resolved_branch = await resolve_git_config(
        session,
        namespace,
    )

    return NamespaceGitConfig(
        # Return resolved values (effective configuration)
        github_repo_path=resolved_repo,
        git_path=resolved_path,
        git_branch=resolved_branch,
        parent_namespace=node_namespace.parent_namespace,
        git_only=node_namespace.git_only,
    )


@router.delete(
    "/namespaces/{namespace}/git",
    name="Remove namespace git configuration",
    status_code=HTTPStatus.NO_CONTENT,
)
async def delete_namespace_git_config(
    namespace: str,
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
):
    access_checker.add_namespace(namespace, ResourceAction.WRITE)
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    node_namespace = await get_node_namespace(session, namespace)
    node_namespace.git_branch = None
    node_namespace.github_repo_path = None
    node_namespace.git_path = None
    node_namespace.parent_namespace = None
    node_namespace.git_only = False

    session.add(node_namespace)
    await session.commit()
    await session.refresh(node_namespace)

    return Response(status_code=HTTPStatus.NO_CONTENT)
