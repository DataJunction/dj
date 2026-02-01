"""
Bulk deployment APIs.
"""

import asyncio
import logging
import uuid

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
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.models.deployment import (
    DeploymentResult,
    DeploymentSpec,
    DeploymentInfo,
    DeploymentSourceType,
    GitDeploymentSource,
    LocalDeploymentSource,
)
from datajunction_server.models.impact import DeploymentImpactResponse
from datajunction_server.internal.deployment.deployment import deploy
from datajunction_server.internal.deployment.impact import analyze_deployment_impact
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
    deployment_spec: DeploymentSpec,
    namespace_obj: NodeNamespace,
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

    # Verify commit exists in the configured repository
    if not namespace_obj.github_repo_path:
        raise DJInvalidInputException(  # pragma: no cover
            message=f"Namespace '{deployment_spec.namespace}' is git-only but has no "
            "github_repo_path configured. Cannot verify commit.",
        )

    try:
        github = GitHubService()
        commit_exists = await github.verify_commit(
            repo_path=namespace_obj.github_repo_path,
            commit_sha=deployment_spec.source.commit_sha,
        )
        if not commit_exists:
            raise DJInvalidInputException(
                message=f"Commit '{deployment_spec.source.commit_sha}' not found in "
                f"repository '{namespace_obj.github_repo_path}'. "
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
        namespace_obj.github_repo_path,
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
    ):
        async with session_context() as session:
            deployment = await session.get(Deployment, deployment_uuid)
            if deployment is None:
                # Deployment record doesn't exist (e.g., internal deployment)
                return
            deployment.status = status
            if results is not None:
                deployment.results = [r.model_dump() for r in results]
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
                results = await deploy(
                    session=session,
                    deployment_id=deployment_id,
                    deployment=deployment_spec,
                    context=context,
                )
                final_status = (
                    DeploymentStatus.SUCCESS
                    if all(
                        r.status
                        in (
                            DeploymentResult.Status.SUCCESS,
                            DeploymentResult.Status.SKIPPED,
                        )
                        for r in results
                    )
                    else DeploymentStatus.FAILED
                )
                await InProcessExecutor.update_status(
                    deployment_id,
                    final_status,
                    results,
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
    if namespace_obj and namespace_obj.git_only:
        await _verify_git_deployment(deployment_spec, namespace_obj)

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
    return DeploymentInfo(
        uuid=deployment_id,
        namespace=deployment.namespace,
        status=deployment.status.value,
        results=deployment.deployment_results,
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
    response_model=DeploymentImpactResponse,
)
async def preview_deployment_impact(
    deployment_spec: DeploymentSpec,
    *,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> DeploymentImpactResponse:
    """
    Analyze the impact of a deployment WITHOUT actually deploying.

    This endpoint takes a deployment specification and returns:
    - Direct changes: What nodes will be created, updated, deleted, or skipped
    - Downstream impacts: What existing nodes will be affected by these changes
    - Warnings: Potential issues like breaking column changes or external impacts

    Use this endpoint to preview changes before deploying, similar to a dry-run
    but with more detailed impact analysis including second and third-order effects.
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

    return await analyze_deployment_impact(
        session=session,
        deployment_spec=deployment_spec,
    )
