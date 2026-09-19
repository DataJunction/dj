import logging

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.internal.deployment.orchestrator import (
    DeploymentExecuteResult,
    DeploymentOrchestrator,
)
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.models.deployment import (
    DeploymentSpec,
)
from datajunction_server.utils import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


async def deploy(
    session: AsyncSession,
    deployment_id: str,
    deployment: DeploymentSpec,
    context: DeploymentContext,
    dry_run: bool = False,
) -> DeploymentExecuteResult:
    """
    Deploy to a namespace based on the given deployment specification.
    """
    orchestrator = DeploymentOrchestrator(
        deployment_id=deployment_id,
        deployment_spec=deployment,
        session=session,
        context=context,
        dry_run=dry_run,
    )
    return await orchestrator.execute()
