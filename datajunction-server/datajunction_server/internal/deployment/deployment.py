import logging
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.internal.deployment.orchestrator import DeploymentOrchestrator
from datajunction_server.models.deployment import (
    DeploymentResult,
    DeploymentSpec,
)
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.utils import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


async def deploy(
    session: AsyncSession,
    deployment_id: str,
    deployment: DeploymentSpec,
    context: DeploymentContext,
) -> list[DeploymentResult]:
    """
    Deploy to a namespace based on the given deployment specification.
    """
    orchestrator = DeploymentOrchestrator(
        deployment_id=deployment_id,
        deployment_spec=deployment,
        session=session,
        context=context,
    )
    return await orchestrator.execute()
