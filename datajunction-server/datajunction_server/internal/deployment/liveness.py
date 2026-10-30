"""
Tracks which deployments are alive and marks dead ones as interrupted.
"""

import asyncio
import logging
import os
import socket
import threading
import uuid
from datetime import UTC, datetime, timedelta
from functools import cache

from sqlalchemy import and_, create_engine, func, or_, update
from sqlalchemy.pool import NullPool

from datajunction_server.database.deployment import Deployment
from datajunction_server.models.deployment import DeploymentResult, DeploymentStatus
from datajunction_server.utils import get_settings, session_context

logger = logging.getLogger(__name__)

INTERRUPTED_MESSAGE = "Deployment interrupted: its server stopped before it finished"


@cache
def get_worker_id() -> str:
    """
    Name of this server process, created on first use.
    """
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


class Heartbeat:
    """
    Refreshes a running deployment's heartbeat from a separate thread,
    so a busy event loop cannot stall it.
    """

    def __init__(self, deployment_id: str, interval: float):
        self.deployment_id = deployment_id
        self.interval = interval
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stopped.set()

    def _run(self) -> None:
        while not self._stopped.wait(self.interval):
            self.beat()

    def beat(self) -> None:
        """
        Write one heartbeat over a short-lived connection.
        """
        try:
            engine = create_engine(get_settings().writer_db.uri, poolclass=NullPool)
            try:
                with engine.begin() as connection:
                    connection.execute(
                        update(Deployment)
                        .where(
                            Deployment.uuid == self.deployment_id,
                            Deployment.status == DeploymentStatus.RUNNING,
                            Deployment.worker_id == get_worker_id(),
                        )
                        .values(heartbeat_at=datetime.now(UTC)),
                    )
            finally:
                engine.dispose()
        except Exception as exc:
            logger.warning("Heartbeat for %s failed: %s", self.deployment_id, exc)


async def interrupt_deployments(condition) -> list[str]:
    """
    Atomically fail the unfinished deployments matching a condition.
    Returns the ids this call changed.
    """
    result = DeploymentResult(
        name="interrupted",
        deploy_type=DeploymentResult.Type.GENERAL,
        message=INTERRUPTED_MESSAGE,
        status=DeploymentResult.Status.FAILED,
        operation=DeploymentResult.Operation.UNKNOWN,
    )
    async with session_context() as session:
        rows = await session.execute(
            update(Deployment)
            .where(
                Deployment.status.in_(
                    [DeploymentStatus.PENDING, DeploymentStatus.RUNNING],
                ),
                condition,
            )
            .values(status=DeploymentStatus.FAILED, results=[result.model_dump()])
            .returning(Deployment.uuid)
            .execution_options(synchronize_session=False),
        )
        interrupted = [str(row.uuid) for row in rows]
        await session.commit()
    return interrupted


async def sweep_stale(stale_after: float) -> list[str]:
    """
    Interrupt deployments with no recent heartbeat, and pending
    ones that no server ever started.
    """
    cutoff = datetime.now(UTC) - timedelta(seconds=stale_after)
    return await interrupt_deployments(
        or_(
            and_(
                Deployment.status == DeploymentStatus.RUNNING,
                func.coalesce(Deployment.heartbeat_at, Deployment.updated_at) < cutoff,
            ),
            and_(
                Deployment.status == DeploymentStatus.PENDING,
                Deployment.worker_id.is_(None),
                Deployment.created_at < cutoff,
            ),
        ),
    )


async def sweep_forever() -> None:
    """
    Run the stale deployment sweep until cancelled.
    """
    settings = get_settings()
    while True:
        try:
            interrupted = await sweep_stale(settings.deployment_stale_after)
            if interrupted:
                logger.warning("Interrupted stale deployments: %s", interrupted)
        except Exception as exc:
            logger.error("Deployment sweep failed: %s", exc)
        await asyncio.sleep(settings.deployment_sweep_interval)
