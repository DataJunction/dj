"""
Bulk deployment APIs.
"""

import asyncio
import logging
import time
import uuid
from typing import Optional

from fastapi import Depends, BackgroundTasks, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datajunction_server.database.user import User
from datajunction_server.database.deployment import Deployment
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.errors import DJDoesNotExistException, DJInvalidInputException
from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.internal.git.github_service import (
    GitHubServiceError,
    GitHubService,
)
from datajunction_server.internal.namespaces import resolve_git_config
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.models.deployment import (
    DeploymentResult,
    DeploymentSpec,
    DeploymentInfo,
    DeploymentSourceType,
    GitDeploymentSource,
    LocalDeploymentSource,
)
from datajunction_server.instrumentation.provider import get_metrics_provider
from datajunction_server.internal.deployment.deployment import deploy
from datajunction_server.internal.deployment.orchestrator import DeploymentOrchestrator
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    AccessChecker,
    AccessDenialMode,
    get_access_checker,
)
from datajunction_server.models import access
from datajunction_server.models.deployment import DeploymentStatus
from datajunction_server.utils import (
    get_current_user,
    get_query_service_client,
    get_session,
    get_settings,
    session_context,
)
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["deployments"])


async def _verify_git_deployment(
    session: AsyncSession,
    deployment_spec: DeploymentSpec,
    namespace_obj: Optional[NodeNamespace],
) -> None:
    """
    Verify that a deployment to a git_only namespace meets requirements:
    1. Must have source.type = git
    2. Must include commit_sha
    3. Commit must exist in the configured repository

    Raises DJInvalidInputException if requirements not met.
    """
    # Check source type
    if not deployment_spec.source:
        raise DJInvalidInputException(
            message=f"Namespace '{deployment_spec.namespace}' is git-only. "
            "Deployments must include a git source with commit_sha.",
        )

    if deployment_spec.source.type != DeploymentSourceType.GIT:
        raise DJInvalidInputException(
            message=f"Namespace '{deployment_spec.namespace}' is git-only. "
            "Deployments must have source.type='git', not "
            f"'{deployment_spec.source.type}'.",
        )

    # Check commit_sha is provided
    if not deployment_spec.source.commit_sha:
        raise DJInvalidInputException(
            message=f"Namespace '{deployment_spec.namespace}' is git-only. "
            "Deployments must include source.commit_sha.",
        )

    # Resolve git config (may be inherited from parent for branch namespaces)
    github_repo_path, _, _ = await resolve_git_config(
        session,
        deployment_spec.namespace,
    )

    # Verify commit exists in the configured repository
    if not github_repo_path:
        raise DJInvalidInputException(  # pragma: no cover
            message=f"Namespace '{deployment_spec.namespace}' is git-only but has no "
            "github_repo_path configured. Cannot verify commit.",
        )

    try:
        github = GitHubService()
        commit_exists = await github.verify_commit(
            repo_path=github_repo_path,
            commit_sha=deployment_spec.source.commit_sha,
        )
        if not commit_exists:
            raise DJInvalidInputException(
                message=f"Commit '{deployment_spec.source.commit_sha}' not found in "
                f"repository '{github_repo_path}'. "
                "Deployments to git-only namespaces must reference valid commits.",
            )
    except GitHubServiceError as e:
        raise DJInvalidInputException(
            message=f"Failed to verify commit: {e.message}",
        ) from e

    logger.info(
        "Verified git deployment to %s: commit %s exists in %s",
        deployment_spec.namespace,
        deployment_spec.source.commit_sha[:8],
        namespace_obj.github_repo_path if namespace_obj else github_repo_path,
    )


class DeploymentExecutor(ABC):
    @abstractmethod
    async def submit(self, spec: DeploymentSpec, context: DeploymentContext) -> str:
        """
        Kick off a deployment job asynchronously.
        Should not block. Should update deployment status externally.
        """
        ...  # pragma: no cover


class InProcessExecutor(DeploymentExecutor):
    def __init__(self):
        self.statuses: dict[str, DeploymentStatus] = {}

    async def submit(self, spec: DeploymentSpec, context: DeploymentContext) -> str:
        deployment_uuid = str(uuid.uuid4())
        async with session_context() as session:
            deployment = Deployment(
                uuid=deployment_uuid,
                namespace=spec.namespace,
                spec=spec.model_dump(),
                status=DeploymentStatus.PENDING,
                created_by_id=context.current_user.id,
            )
            session.add(deployment)
            await session.commit()

        asyncio.create_task(
            self._run_deployment(
                deployment_id=deployment_uuid,
                deployment_spec=spec,
                context=context,
            ),
        )
        return deployment_uuid

    @staticmethod
    async def update_status(
        deployment_uuid: str,
        status: DeploymentStatus,
        results: list[DeploymentResult] | None = None,
        downstream_impacts: list | None = None,
    ):
        async with session_context() as session:
            deployment = await session.get(Deployment, deployment_uuid)
            if deployment is None:
                # Deployment record doesn't exist (e.g., internal deployment)
                return
            deployment.status = status
            if results is not None:
                deployment.results = [r.model_dump() for r in results]
            if downstream_impacts is not None:
                deployment.downstream_impacts = [
                    d.model_dump() for d in downstream_impacts
                ]
            await session.commit()

    async def _run_deployment(
        self,
        deployment_id: str,
        deployment_spec: DeploymentSpec,
        context: DeploymentContext,
    ):
        await InProcessExecutor.update_status(deployment_id, DeploymentStatus.RUNNING)

        try:
            async with session_context() as session:
                execute_result = await deploy(
                    session=session,
                    deployment_id=deployment_id,
                    deployment=deployment_spec,
                    context=context,
                )
                results = execute_result.results
                final_status = (
                    DeploymentStatus.SUCCESS
                    if all(
                        r.status
                        in (
                            DeploymentResult.Status.SUCCESS,
                            DeploymentResult.Status.SKIPPED,
                            DeploymentResult.Status.INVALID,
                        )
                        for r in results
                    )
                    else DeploymentStatus.FAILED
                )
                await InProcessExecutor.update_status(
                    deployment_id,
                    final_status,
                    results,
                    downstream_impacts=execute_result.downstream_impacts,
                )
        except Exception as exc:
            logger.error("Deployment %s failed: %s", deployment_id, exc, exc_info=True)
            await InProcessExecutor.update_status(
                deployment_id,
                DeploymentStatus.FAILED,
                [
                    DeploymentResult(
                        name=exc.__class__.__name__,
                        deploy_type=DeploymentResult.Type.GENERAL,
                        message=str(exc),
                        status=DeploymentResult.Status.FAILED,
                        operation=DeploymentResult.Operation.UNKNOWN,
                    ),
                ],
            )


executor = InProcessExecutor()


@router.post(
    "/deployments",
    name="Creates a bulk deployment",
    response_model=DeploymentInfo,
)
async def create_deployment(
    deployment_spec: DeploymentSpec,
    background_tasks: BackgroundTasks,
    request: Request,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    cache: Cache = Depends(get_cache),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> DeploymentInfo:
    """
    This endpoint takes a deployment specification (namespace, nodes, tags), topologically
    sorts and validates the deployable objects, and deploys the nodes in parallel where
    possible. It returns a summary of the deployment.
    """
    access_checker.add_request(
        access.ResourceRequest(
            verb=access.ResourceAction.WRITE,
            access_object=access.Resource(
                resource_type=access.ResourceType.NAMESPACE,
                name=deployment_spec.namespace,
            ),
        ),
    )
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    # Check git_only enforcement
    namespace_obj = await NodeNamespace.get(
        session,
        deployment_spec.namespace,
        raise_if_not_exists=False,
    )
    is_git_root = (
        namespace_obj is not None
        and namespace_obj.github_repo_path is not None
        and namespace_obj.git_branch is None
    )
    if namespace_obj and (namespace_obj.git_only or is_git_root):
        await _verify_git_deployment(session, deployment_spec, namespace_obj)

    _t0 = time.monotonic()
    source_type = deployment_spec.source.type if deployment_spec.source else "local"
    _metrics_tags = {
        "namespace": deployment_spec.namespace,
        "source_type": str(source_type),
    }

    deployment_id = await executor.submit(
        spec=deployment_spec,
        context=DeploymentContext(
            current_user=current_user,
            request=request,
            query_service_client=query_service_client,
            background_tasks=background_tasks,
            cache=cache,
        ),
    )
    deployment = await session.get(Deployment, deployment_id)
    status = deployment.status.value if deployment else "unknown"
    get_metrics_provider().timer(
        "dj.deployments.create_latency_ms",
        (time.monotonic() - _t0) * 1000,
        _metrics_tags,
    )
    get_metrics_provider().counter(
        "dj.deployments.create",
        tags={**_metrics_tags, "status": status},
    )
    return DeploymentInfo(
        uuid=deployment_id,
        namespace=deployment.namespace,
        status=deployment.status.value,
        results=deployment.deployment_results,
        downstream_impacts=deployment.deployment_downstream_impacts,
    )


@router.get("/deployments/{deployment_id}", response_model=DeploymentInfo)
async def get_deployment_status(
    deployment_id: str,
    session: AsyncSession = Depends(get_session),
) -> DeploymentInfo:
    deployment = await session.get(Deployment, deployment_id)
    if not deployment:
        raise DJDoesNotExistException(
            message=f"Deployment {deployment_id} not found",
        )  # pragma: no cover
    return DeploymentInfo(
        uuid=deployment_id,
        namespace=deployment.namespace,
        status=deployment.status.value,
        results=deployment.deployment_results,
        downstream_impacts=deployment.deployment_downstream_impacts,
    )


@router.get("/deployments", response_model=list[DeploymentInfo])
async def list_deployments(  # pragma: no cover
    namespace: str | None = None,
    limit: int = 50,
    session: AsyncSession = Depends(get_session),
) -> list[DeploymentInfo]:
    statement = select(Deployment).order_by(Deployment.created_at.desc())
    if namespace:
        statement = statement.where(Deployment.namespace == namespace)
    statement = statement.limit(limit)
    deployments = (await session.execute(statement)).scalars().all()

    results = []
    for deployment in deployments:
        # Parse source from spec
        source_data = deployment.spec.get("source") if deployment.spec else None
        source = None
        if source_data and source_data.get("type") == DeploymentSourceType.GIT:
            source = GitDeploymentSource(**source_data)
        elif source_data and source_data.get("type") == DeploymentSourceType.LOCAL:
            source = LocalDeploymentSource(**source_data)

        results.append(
            DeploymentInfo(
                uuid=str(deployment.uuid),
                namespace=deployment.namespace,
                status=deployment.status,
                results=deployment.deployment_results,
                created_at=deployment.created_at.isoformat()
                if deployment.created_at
                else None,
                created_by=deployment.created_by.username
                if deployment.created_by
                else None,
                source=source,
            ),
        )
    return results


@router.post(
    "/deployments/impact",
    name="Preview deployment impact",
    response_model=DeploymentInfo,
)
async def preview_deployment_impact(
    deployment_spec: DeploymentSpec,
    request: Request,
    background_tasks: BackgroundTasks,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    cache: Cache = Depends(get_cache),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> DeploymentInfo:
    """
    Analyze the impact of a deployment WITHOUT actually deploying.

    Runs a full dry-run through the deployment orchestrator: nodes are validated
    and deployed into a database SAVEPOINT, downstream impact is computed via BFS,
    then the SAVEPOINT is rolled back so no changes are persisted.

    Returns the same ``DeploymentInfo`` shape as ``POST /deployments``, with
    ``results`` showing what would change and ``downstream_impacts`` showing
    which downstream nodes would be affected.
    """
    access_checker.add_request(
        access.ResourceRequest(
            verb=access.ResourceAction.READ,
            access_object=access.Resource(
                resource_type=access.ResourceType.NAMESPACE,
                name=deployment_spec.namespace,
            ),
        ),
    )
    await access_checker.check(on_denied=AccessDenialMode.RAISE)

    orchestrator = DeploymentOrchestrator(
        deployment_id="dry_run",
        deployment_spec=deployment_spec,
        session=session,
        context=DeploymentContext(
            current_user=current_user,
            request=request,
            query_service_client=query_service_client,
            background_tasks=background_tasks,
            cache=cache,
        ),
        dry_run=True,
    )
    execute_result = await orchestrator.execute()
    return DeploymentInfo(
        uuid="dry_run",
        namespace=deployment_spec.namespace,
        status=DeploymentStatus.SUCCESS,
        results=execute_result.results,
        downstream_impacts=execute_result.downstream_impacts,
    )
