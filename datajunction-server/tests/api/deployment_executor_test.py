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
from sqlalchemy import select, text

from datajunction_server.api.deployments import InProcessExecutor
from datajunction_server.database.deployment import Deployment
from datajunction_server.internal.deployment.liveness import (
    get_worker_id,
    interrupt_deployments,
)
from datajunction_server.internal.deployment.orchestrator import DeploymentOrchestrator
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
    session_factory,
    patched_context,
    spec,
    deployment_context,
):
    executor = InProcessExecutor()
    started = asyncio.Event()

    async def fake_run(**kwargs):
        await InProcessExecutor.update_status(
            kwargs["deployment_id"],
            DeploymentStatus.RUNNING,
        )
        started.set()
        await asyncio.sleep(3600)

    # The drain uses its own sessions, apart from the task's session.
    @asynccontextmanager
    async def drain_context():
        drain_session = await session_factory()
        try:
            yield drain_session
        finally:
            await drain_session.close()

    with (
        patch.object(executor, "_run_deployment", side_effect=fake_run),
        patch(f"{LIVENESS}.session_context", drain_context),
    ):
        deployment_id = await executor.submit(spec, deployment_context)
        task = next(iter(executor._tasks))
        await started.wait()
        await executor.drain(0.01)
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)

    row = await _row(session, deployment_id)
    assert (row.status, row.results) == (DeploymentStatus.FAILED, INTERRUPTED_RESULTS)
    assert (task.cancelled(), executor._tasks) == (True, set())


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


async def test_update_status_keeps_failed(session, current_user, patched_context):
    deployment_id = await _pending(session, current_user)
    skipped = await InProcessExecutor.update_status(
        deployment_id,
        DeploymentStatus.FAILED,
    )
    assert skipped is False
    skipped = await InProcessExecutor.update_status(
        deployment_id,
        DeploymentStatus.SUCCESS,
    )
    assert skipped is True
    assert (await _row(session, deployment_id)).status == DeploymentStatus.FAILED
    assert (
        await InProcessExecutor.update_status(
            str(uuid.uuid4()),
            DeploymentStatus.RUNNING,
        )
        is False
    )


async def test_orchestrator_progress_keeps_interrupted(
    session,
    current_user,
    patched_context,
):
    deployment_id = await _pending(
        session,
        current_user,
        status=DeploymentStatus.FAILED,
        results=INTERRUPTED_RESULTS,
    )
    fake_orchestrator = SimpleNamespace(
        dry_run=False,
        deployment_id=deployment_id,
        deployed_results=[],
    )
    await DeploymentOrchestrator._update_deployment_status(fake_orchestrator)

    row = await _row(session, deployment_id)
    assert (row.status, row.results) == (DeploymentStatus.FAILED, INTERRUPTED_RESULTS)


@pytest.mark.parametrize(
    "start, write",
    [
        (DeploymentStatus.PENDING, DeploymentStatus.RUNNING),
        (DeploymentStatus.RUNNING, DeploymentStatus.SUCCESS),
        (DeploymentStatus.RUNNING, DeploymentStatus.RUNNING),
    ],
)
async def test_sweep_between_read_and_write(
    session,
    session_factory,
    current_user,
    start,
    write,
):
    deployment_id = await _pending(session, current_user, status=start)
    # A session that already read the row as unfinished.
    stale_session = await session_factory()
    seen = await stale_session.get(Deployment, deployment_id)
    assert seen.status == start

    @asynccontextmanager
    async def fresh_context():
        other = await session_factory()
        try:
            yield other
        finally:
            await other.close()

    with patch(f"{LIVENESS}.session_context", fresh_context):
        interrupted = await interrupt_deployments(Deployment.uuid == deployment_id)
    assert interrupted == [deployment_id]

    with patch(f"{DEPLOYMENTS}.session_context", _context(stale_session)):
        skipped = await InProcessExecutor.update_status(deployment_id, write)
    await stale_session.close()

    row = await _row(session, deployment_id)
    assert (skipped, row.status, row.results) == (
        True,
        DeploymentStatus.FAILED,
        INTERRUPTED_RESULTS,
    )


async def test_sweep_can_use_unfinished_index(session):
    await session.execute(text("SET enable_seqscan = off"))
    plan = await session.execute(
        text(
            "EXPLAIN SELECT uuid FROM deployments "
            "WHERE status IN ('PENDING', 'RUNNING') AND created_at < now()",
        ),
    )
    uses_index = any("ix_deployments_unfinished" in row[0] for row in plan)
    assert uses_index is True
