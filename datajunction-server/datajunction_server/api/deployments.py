"""
Bulk deployment APIs.
"""

import asyncio
from dataclasses import dataclass
import logging
from typing import Callable
import uuid

from fastapi import Depends, BackgroundTasks, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datajunction_server.database.user import User
from datajunction_server.database.deployment import Deployment
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.api.helpers import get_save_history
from datajunction_server.models.deployment import (
    DeploymentResult,
    DeploymentSpec,
    DeploymentInfo,
)
from datajunction_server.internal.deployment import (
    deploy,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    validate_access,
)
from datajunction_server.models import access
from datajunction_server.models.deployment import DeploymentStatus
from datajunction_server.utils import (
    get_and_update_current_user,
    get_query_service_client,
    get_session,
    get_settings,
    session_context,
)
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["deployments"])


@dataclass
class DeploymentContext:
    current_user: User
    request: Request
    query_service_client: QueryServiceClient
    save_history: Callable
    validate_access: access.ValidateAccessFn
    background_tasks: BackgroundTasks
    cache: Cache


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
                current_user=context.current_user,
                request=context.request,
                query_service_client=context.query_service_client,
                save_history=context.save_history,
                validate_access=context.validate_access,
                background_tasks=context.background_tasks,
                cache=context.cache,
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
            deployment.status = status
            if results is not None:
                deployment.results = [r.model_dump() for r in results]
            await session.commit()

    async def _run_deployment(
        self,
        deployment_id: str,
        deployment_spec: DeploymentSpec,
        current_user: User,
        request: Request,
        query_service_client: QueryServiceClient,
        save_history: Callable,
        validate_access: access.ValidateAccessFn,
        background_tasks: BackgroundTasks,
        cache: Cache,
    ):
        await InProcessExecutor.update_status(deployment_id, DeploymentStatus.RUNNING)

        try:
            results = await deploy(
                deployment_id=deployment_id,
                deployment=deployment_spec,
                current_username=current_user.username,
                request=request,
                query_service_client=query_service_client,
                save_history=save_history,
                validate_access=validate_access,
                background_tasks=background_tasks,
                cache=cache,
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
            await InProcessExecutor.update_status(deployment_id, final_status, results)
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
    current_user: User = Depends(get_and_update_current_user),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    save_history: Callable = Depends(get_save_history),
    cache: Cache = Depends(get_cache),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
) -> DeploymentInfo:
    """
    This endpoint takes a deployment specification (namespace, nodes, tags), topologically
    sorts and validates the deployable objects, and deploys the nodes in parallel where
    possible. It returns a summary of the deployment.
    """
    deployment_id = await executor.submit(
        spec=deployment_spec,
        context=DeploymentContext(
            current_user=current_user,
            request=request,
            query_service_client=query_service_client,
            save_history=save_history,
            validate_access=validate_access,
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
    session: AsyncSession = Depends(get_session),
) -> list[DeploymentInfo]:
    statement = select(Deployment)
    if namespace:
        statement = statement.where(Deployment.namespace == namespace)
    deployments = (await session.execute(statement)).scalars().all()
    return [
        DeploymentInfo(
            uuid=str(deployment.uuid),
            namespace=deployment.namespace,
            status=deployment.status,
            results=deployment.deployment_results,
        )
        for deployment in deployments
    ]
