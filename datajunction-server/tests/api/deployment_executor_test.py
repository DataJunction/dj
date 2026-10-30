"""
Tests for InProcessExecutor tracking, draining, and interruption handling.
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from datajunction_server.api.deployments import InProcessExecutor
from datajunction_server.database.deployment import Deployment
from datajunction_server.internal.deployment.liveness import get_worker_id
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.models.deployment import (
    DeploymentResult,
    DeploymentSpec,
    DeploymentStatus,
)

INTERRUPTED_RESULTS = [
    DeploymentResult(
        name="interrupted",
        deploy_type=DeploymentResult.Type.GENERAL,
        message="Deployment interrupted: its server stopped before it finished",
        status=DeploymentResult.Status.FAILED,
        operation=DeploymentResult.Operation.UNKNOWN,
    ).model_dump(),
]

DEPLOYMENTS = "datajunction_server.api.deployments"
LIVENESS = "datajunction_server.internal.deployment.liveness"


def _context(session):
    @asynccontextmanager
    async def fake_session_context():
        yield session

    return fake_session_context


@pytest.fixture
def patched_context(session):
    with (
        patch(f"{DEPLOYMENTS}.session_context", _context(session)),
        patch(f"{LIVENESS}.session_context", _context(session)),
    ):
        yield


@pytest.fixture
def spec():
    return DeploymentSpec(namespace="test", nodes=[])


@pytest.fixture
def deployment_context(current_user):
    return MagicMock(spec=DeploymentContext, current_user=current_user)


async def _row(session, deployment_id) -> Deployment:
    row = (
        await session.execute(
            select(Deployment).where(Deployment.uuid == deployment_id),
        )
    ).scalar_one()
    await session.refresh(row)
    return row


async def _pending(session, current_user, **fields) -> str:
    deployment_id = str(uuid.uuid4())
    session.add(
        Deployment(
            uuid=deployment_id,
            namespace="test",
            status=fields.pop("status", DeploymentStatus.PENDING),
            created_by_id=current_user.id,
            **fields,
        ),
    )
    await session.commit()
    return deployment_id


def _deploy_result(results=None):
    return SimpleNamespace(
        results=results or [],
        downstream_impacts=[],
        warnings=[],
        check_results=[],
    )


async def test_submit_tracks_task_until_done(
    session,
    patched_context,
    spec,
    deployment_context,
):
    executor = InProcessExecutor()
    release = asyncio.Event()

    async def fake_run(**kwargs):
        await release.wait()

    with patch.object(executor, "_run_deployment", side_effect=fake_run):
        deployment_id = await executor.submit(spec, deployment_context)

    assert [task.get_name() for task in executor._tasks] == [deployment_id]
    release.set()
    await asyncio.gather(*executor._tasks)
    await asyncio.sleep(0)
    assert executor._tasks == set()
    assert (await _row(session, deployment_id)).status == DeploymentStatus.PENDING


async def test_submit_refused_while_draining(spec, deployment_context):
    executor = InProcessExecutor()
    executor.draining = True
    with pytest.raises(HTTPException) as exc_info:
        await executor.submit(spec, deployment_context)
    assert (
        exc_info.value.status_code,
        exc_info.value.detail,
        exc_info.value.headers,
    ) == (
        503,
        "Server is shutting down, retry the deployment",
        {"Retry-After": "30"},
    )
    assert executor._tasks == set()


async def test_drain_without_tasks():
    executor = InProcessExecutor()
    await executor.drain(1)
    assert executor.draining is True


async def test_drain_waits_for_running_deployment(
    session,
    patched_context,
    spec,
    deployment_context,
):
    executor = InProcessExecutor()

    async def fake_run(**kwargs):
        await asyncio.sleep(0.05)

    with patch.object(executor, "_run_deployment", side_effect=fake_run):
        deployment_id = await executor.submit(spec, deployment_context)
    await executor.drain(10)

    assert executor._tasks == set()
    assert (await _row(session, deployment_id)).status == DeploymentStatus.PENDING


async def test_drain_timeout_marks_interrupted(
    session,
    patched_context,
    spec,
    deployment_context,
):
    executor = InProcessExecutor()

    async def fake_run(**kwargs):
        await InProcessExecutor.update_status(
            kwargs["deployment_id"],
            DeploymentStatus.RUNNING,
        )
        await asyncio.sleep(60)

    with patch.object(executor, "_run_deployment", side_effect=fake_run):
        deployment_id = await executor.submit(spec, deployment_context)
        await asyncio.sleep(0.05)
        await executor.drain(0.05)
        await asyncio.sleep(0.05)

    row = await _row(session, deployment_id)
    assert (row.status, row.results) == (DeploymentStatus.FAILED, INTERRUPTED_RESULTS)
    assert executor._tasks == set()


async def test_run_records_worker_and_heartbeat(
    session,
    current_user,
    patched_context,
    spec,
    deployment_context,
):
    deployment_id = await _pending(session, current_user)
    before = datetime.now(UTC)
    with (
        patch(f"{DEPLOYMENTS}.Heartbeat") as heartbeat_class,
        patch(
            f"{DEPLOYMENTS}.deploy",
            AsyncMock(return_value=_deploy_result()),
        ) as deploy,
    ):
        # The row is RUNNING with a worker while deploy executes.
        async def check_running(**kwargs):
            running = await _row(session, deployment_id)
            assert (running.status, running.worker_id) == (
                DeploymentStatus.RUNNING,
                get_worker_id(),
            )
            assert before <= running.heartbeat_at <= datetime.now(UTC)
            return _deploy_result()

        deploy.side_effect = check_running
        await InProcessExecutor()._run_deployment(
            deployment_id,
            spec,
            deployment_context,
        )

    heartbeat_class.return_value.start.assert_called_once_with()
    heartbeat_class.return_value.stop.assert_called_once_with()
    row = await _row(session, deployment_id)
    assert (row.status, row.results) == (DeploymentStatus.SUCCESS, [])


async def test_late_success_keeps_interrupted_row(
    session,
    current_user,
    patched_context,
    spec,
    deployment_context,
):
    deployment_id = await _pending(session, current_user)

    async def sweep_then_succeed(**kwargs):
        row = await _row(session, deployment_id)
        row.status = DeploymentStatus.FAILED
        row.results = INTERRUPTED_RESULTS
        await session.commit()
        return _deploy_result()

    with (
        patch(f"{DEPLOYMENTS}.Heartbeat"),
        patch(f"{DEPLOYMENTS}.deploy", side_effect=sweep_then_succeed),
        patch(f"{DEPLOYMENTS}._maybe_autolock_git_namespace") as autolock,
    ):
        await InProcessExecutor()._run_deployment(
            deployment_id,
            spec,
            deployment_context,
        )

    autolock.assert_not_called()
    row = await _row(session, deployment_id)
    assert (row.status, row.results) == (DeploymentStatus.FAILED, INTERRUPTED_RESULTS)


async def test_late_error_keeps_interrupted_row(
    session,
    current_user,
    patched_context,
    spec,
    deployment_context,
):
    deployment_id = await _pending(session, current_user)

    async def sweep_then_fail(**kwargs):
        row = await _row(session, deployment_id)
        row.status = DeploymentStatus.FAILED
        row.results = INTERRUPTED_RESULTS
        await session.commit()
        raise RuntimeError("late failure")

    with (
        patch(f"{DEPLOYMENTS}.Heartbeat"),
        patch(f"{DEPLOYMENTS}.deploy", side_effect=sweep_then_fail),
    ):
        await InProcessExecutor()._run_deployment(
            deployment_id,
            spec,
            deployment_context,
        )

    row = await _row(session, deployment_id)
    assert (row.status, row.results) == (DeploymentStatus.FAILED, INTERRUPTED_RESULTS)


async def test_interrupted_before_start_does_not_run(
    session,
    current_user,
    patched_context,
    spec,
    deployment_context,
):
    deployment_id = await _pending(
        session,
        current_user,
        status=DeploymentStatus.FAILED,
        results=INTERRUPTED_RESULTS,
    )
    with (
        patch(f"{DEPLOYMENTS}.Heartbeat") as heartbeat_class,
        patch(f"{DEPLOYMENTS}.deploy") as deploy,
    ):
        await InProcessExecutor()._run_deployment(
            deployment_id,
            spec,
            deployment_context,
        )

    deploy.assert_not_called()
    heartbeat_class.assert_not_called()
    row = await _row(session, deployment_id)
    assert (row.status, row.worker_id, row.results) == (
        DeploymentStatus.FAILED,
        None,
        INTERRUPTED_RESULTS,
    )


async def test_update_status_keep_failed(session, current_user, patched_context):
    deployment_id = await _pending(session, current_user)
    skipped = await InProcessExecutor.update_status(
        deployment_id,
        DeploymentStatus.FAILED,
        keep_failed=True,
    )
    assert skipped is False
    skipped = await InProcessExecutor.update_status(
        deployment_id,
        DeploymentStatus.SUCCESS,
        keep_failed=True,
    )
    assert skipped is True
    assert (await _row(session, deployment_id)).status == DeploymentStatus.FAILED
    assert (
        await InProcessExecutor.update_status(
            str(uuid.uuid4()),
            DeploymentStatus.RUNNING,
            keep_failed=True,
        )
        is False
    )
